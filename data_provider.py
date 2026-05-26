"""
data_provider.py — Asynchronous data ingestion engine.

Defines an abstract broker interface and concrete implementations.
Swap broker by changing BrokerCredentials.provider — nothing else changes.

Supported providers:
  • mock    — Synthetic tick generator for testing/backtesting
  • shoonya — Finvasia/Shoonya NorenAPI (free, widely used)
  • dhan    — Dhan HQ (websocket v2)
  • fyers   — Fyers API v3

All providers emit normalized DataTick objects into a shared asyncio.Queue,
decoupling ingestion from strategy processing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

from config import SystemConfig, BrokerCredentials

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalized Data Structures
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class IndexTick:
    """Normalized spot/futures price tick."""
    symbol: str
    ltp: float
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime
    is_index: bool = True          # True = spot index, False = futures contract


@dataclass(slots=True)
class OptionTick:
    """Normalized option quote tick."""
    symbol: str                    # e.g., "NIFTY24JAN23000CE"
    underlying: str                # e.g., "NIFTY"
    strike: float
    option_type: str               # "CE" or "PE"
    expiry: date
    ltp: float
    bid: float
    ask: float
    oi: int
    change_oi: int
    volume: int
    iv: float
    delta: float
    timestamp: datetime


@dataclass
class OHLCV:
    """Single candle bar."""
    symbol: str
    timeframe: int                 # Minutes
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime


@dataclass
class InstrumentInfo:
    """Broker-agnostic instrument descriptor."""
    token: str                     # Broker-specific numeric/string token
    symbol: str
    exchange: str                  # NSE / BSE / NFO / BFO
    instrument_type: str           # INDEX, FUTIDX, OPTIDX
    strike: Optional[float] = None
    option_type: Optional[str] = None   # CE / PE
    expiry: Optional[date] = None
    lot_size: int = 1


# ---------------------------------------------------------------------------
# Abstract Broker Interface
# ---------------------------------------------------------------------------

class BaseBroker(ABC):
    """
    Contract that every concrete broker implementation must satisfy.

    The engine calls authenticate() once at startup, then calls
    stream_ticks() which yields DataTick objects indefinitely. All order
    methods are awaitable so they are non-blocking in the async loop.
    """

    def __init__(self, credentials: BrokerCredentials, config: SystemConfig) -> None:
        self.credentials = credentials
        self.config = config
        self._subscribed_tokens: List[str] = []
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    @abstractmethod
    async def authenticate(self) -> bool:
        """Return True if login succeeded."""

    @abstractmethod
    async def logout(self) -> None:
        """Gracefully close the session."""

    # ------------------------------------------------------------------
    # Instrument Discovery
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_instrument_master(self, exchange: str = "NFO") -> pd.DataFrame:
        """
        Download and return the broker's full instrument master as a
        DataFrame with columns: token, symbol, exchange, instrument_type,
        strike, option_type, expiry, lot_size.
        """

    @abstractmethod
    async def find_option_contracts(
        self,
        underlying: str,
        expiry: date,
        strikes: List[float],
    ) -> List[InstrumentInfo]:
        """Return InstrumentInfo list for the given strikes/expiry."""

    @abstractmethod
    async def get_atm_strike(self, underlying: str) -> Tuple[float, float]:
        """Return (spot_price, atm_strike) for the underlying."""

    # ------------------------------------------------------------------
    # Historical Data
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_historical_candles(
        self,
        token: str,
        exchange: str,
        interval: str,           # "1", "5", "15", "60", "D"
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Return DataFrame with columns: datetime, open, high, low, close, volume."""

    # ------------------------------------------------------------------
    # Real-Time Streaming
    # ------------------------------------------------------------------

    @abstractmethod
    async def subscribe(self, tokens: List[str]) -> None:
        """Subscribe to live ticks for the given broker tokens."""

    @abstractmethod
    async def unsubscribe(self, tokens: List[str]) -> None:
        """Unsubscribe from tick feed."""

    @abstractmethod
    def stream_ticks(self) -> AsyncIterator[IndexTick | OptionTick]:
        """Async generator that yields normalized ticks indefinitely."""

    # ------------------------------------------------------------------
    # Order Management
    # ------------------------------------------------------------------

    @abstractmethod
    async def place_order(
        self,
        token: str,
        exchange: str,
        symbol: str,
        transaction_type: str,   # "BUY" or "SELL"
        quantity: int,
        order_type: str,         # "MARKET" | "LIMIT" | "SL" | "SL-M"
        price: float = 0.0,
        trigger_price: float = 0.0,
        product_type: str = "INTRADAY",
        tag: str = "",
    ) -> str:
        """Submit an order. Returns broker order_id string."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True on success."""

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Return order status dict with keys: status, filled_qty, avg_price."""

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Return current open positions."""

    @abstractmethod
    async def get_funds(self) -> Dict[str, float]:
        """Return available margin: {'available': ..., 'used': ...}."""

    async def __aenter__(self) -> "BaseBroker":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.logout()
        if self._session:
            await self._session.close()


# ---------------------------------------------------------------------------
# Mock Broker — Synthetic data for paper trading and unit tests
# ---------------------------------------------------------------------------

class MockBroker(BaseBroker):
    """
    Generates synthetic but realistic NIFTY/BANKNIFTY price action and
    option chain data without any external dependencies. Useful for local
    development and CI.
    """

    # Realistic index base prices
    _BASE_PRICES: Dict[str, float] = {
        "NIFTY": 24_500.0,
        "BANKNIFTY": 52_000.0,
        "FINNIFTY": 23_000.0,
        "SENSEX": 80_000.0,
        "MIDCPNIFTY": 12_000.0,
    }

    # Index lot sizes
    _LOT_SIZES: Dict[str, int] = {
        "NIFTY": 25, "BANKNIFTY": 15, "FINNIFTY": 40,
        "SENSEX": 10, "MIDCPNIFTY": 75,
    }

    def __init__(self, credentials: BrokerCredentials, config: SystemConfig) -> None:
        super().__init__(credentials, config)
        self._prices: Dict[str, float] = dict(self._BASE_PRICES)
        self._tick_queue: asyncio.Queue[IndexTick | OptionTick] = asyncio.Queue(maxsize=1000)
        self._streaming = False
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._order_counter = 0
        self._positions: List[Dict[str, Any]] = []
        self._funds = {"available": config.risk.capital, "used": 0.0}
        self._instrument_master: Optional[pd.DataFrame] = None

    async def authenticate(self) -> bool:
        logger.info("MockBroker: Authentication successful (no-op).")
        return True

    async def logout(self) -> None:
        self._streaming = False
        logger.info("MockBroker: Logged out.")

    async def fetch_instrument_master(self, exchange: str = "NFO") -> pd.DataFrame:
        if self._instrument_master is not None:
            return self._instrument_master

        records = []
        today = date.today()
        # Simulate weekly expiry (next Thursday)
        days_until_thursday = (3 - today.weekday()) % 7
        if days_until_thursday == 0:
            days_until_thursday = 7
        from datetime import timedelta
        expiry = today + timedelta(days=days_until_thursday)

        token_base = 10000
        for underlying, base_price in self._BASE_PRICES.items():
            lot = self._LOT_SIZES[underlying]
            strike_step = 50 if underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100
            if underlying == "SENSEX":
                strike_step = 100
            atm = round(base_price / strike_step) * strike_step

            # Spot token
            records.append({
                "token": str(token_base),
                "symbol": underlying,
                "exchange": "NSE",
                "instrument_type": "INDEX",
                "strike": None,
                "option_type": None,
                "expiry": None,
                "lot_size": lot,
            })
            token_base += 1

            for i in range(-10, 11):
                strike = atm + i * strike_step
                for opt in ("CE", "PE"):
                    sym = f"{underlying}{expiry.strftime('%d%b%y').upper()}{int(strike)}{opt}"
                    records.append({
                        "token": str(token_base),
                        "symbol": sym,
                        "exchange": "NFO",
                        "instrument_type": "OPTIDX",
                        "strike": float(strike),
                        "option_type": opt,
                        "expiry": expiry,
                        "lot_size": lot,
                    })
                    token_base += 1

        self._instrument_master = pd.DataFrame(records)
        return self._instrument_master

    async def find_option_contracts(
        self,
        underlying: str,
        expiry: date,
        strikes: List[float],
    ) -> List[InstrumentInfo]:
        master = await self.fetch_instrument_master()
        mask = (
            master["instrument_type"] == "OPTIDX"
        ) & (
            master["symbol"].str.startswith(underlying)
        ) & (
            master["expiry"] == expiry
        ) & (
            master["strike"].isin(strikes)
        )
        result = []
        for _, row in master[mask].iterrows():
            result.append(InstrumentInfo(
                token=row["token"],
                symbol=row["symbol"],
                exchange=row["exchange"],
                instrument_type=row["instrument_type"],
                strike=row["strike"],
                option_type=row["option_type"],
                expiry=row["expiry"],
                lot_size=row["lot_size"],
            ))
        return result

    async def get_atm_strike(self, underlying: str) -> Tuple[float, float]:
        spot = self._prices.get(underlying, self._BASE_PRICES[underlying])
        step = 50 if underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100
        if underlying == "SENSEX":
            step = 100
        atm = round(spot / step) * step
        return spot, float(atm)

    async def fetch_historical_candles(
        self,
        token: str,
        exchange: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Generate synthetic OHLCV candles for the token."""
        minutes = int(interval) if interval.isdigit() else 5
        timestamps = pd.date_range(from_dt, to_dt, freq=f"{minutes}min")
        n = len(timestamps)

        # Random walk
        rng = random.Random(hash(token))
        base = 24_500.0
        closes = [base]
        for _ in range(n - 1):
            closes.append(closes[-1] * (1 + rng.gauss(0, 0.001)))
        opens = [closes[max(0, i - 1)] for i in range(n)]
        highs = [max(o, c) * (1 + abs(rng.gauss(0, 0.0005))) for o, c in zip(opens, closes)]
        lows = [min(o, c) * (1 - abs(rng.gauss(0, 0.0005))) for o, c in zip(opens, closes)]
        volumes = [rng.randint(50_000, 500_000) for _ in range(n)]

        return pd.DataFrame({
            "datetime": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }).set_index("datetime")

    async def subscribe(self, tokens: List[str]) -> None:
        self._subscribed_tokens.extend(t for t in tokens if t not in self._subscribed_tokens)
        logger.debug("MockBroker: Subscribed to %d tokens.", len(self._subscribed_tokens))

    async def unsubscribe(self, tokens: List[str]) -> None:
        self._subscribed_tokens = [t for t in self._subscribed_tokens if t not in tokens]

    async def stream_ticks(self) -> AsyncIterator[IndexTick | OptionTick]:  # type: ignore[override]
        self._streaming = True
        asyncio.create_task(self._generate_ticks())
        while self._streaming:
            try:
                tick = await asyncio.wait_for(self._tick_queue.get(), timeout=1.0)
                yield tick
            except asyncio.TimeoutError:
                continue

    async def _generate_ticks(self) -> None:
        """Simulate live market data every 100ms."""
        while self._streaming:
            now = datetime.now()
            for underlying, price in list(self._prices.items()):
                # Random walk
                change = price * random.gauss(0, 0.0002)
                self._prices[underlying] = max(price + change, 1.0)
                new_price = self._prices[underlying]

                tick = IndexTick(
                    symbol=underlying,
                    ltp=round(new_price, 2),
                    open=round(new_price * 0.9995, 2),
                    high=round(new_price * 1.001, 2),
                    low=round(new_price * 0.999, 2),
                    close=round(new_price, 2),
                    volume=random.randint(1000, 50000),
                    timestamp=now,
                    is_index=True,
                )
                if not self._tick_queue.full():
                    await self._tick_queue.put(tick)

                # Generate synthetic option ticks for ATM ± 2
                step = 50 if underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100
                atm = round(new_price / step) * step
                for offset in range(-2, 3):
                    strike = atm + offset * step
                    for opt_type in ("CE", "PE"):
                        intrinsic = max(
                            (new_price - strike) if opt_type == "CE" else (strike - new_price), 0
                        )
                        ltp = max(intrinsic + random.gauss(50, 15), 1.0)
                        oi = random.randint(100_000, 10_000_000)
                        opt_tick = OptionTick(
                            symbol=f"{underlying}OPT{strike}{opt_type}",
                            underlying=underlying,
                            strike=float(strike),
                            option_type=opt_type,
                            expiry=date.today(),
                            ltp=round(ltp, 2),
                            bid=round(ltp - 0.5, 2),
                            ask=round(ltp + 0.5, 2),
                            oi=oi,
                            change_oi=random.randint(-50_000, 100_000),
                            volume=random.randint(1_000, 500_000),
                            iv=round(random.gauss(15, 3), 2),
                            delta=round(0.5 + offset * 0.1, 4),
                            timestamp=now,
                        )
                        if not self._tick_queue.full():
                            await self._tick_queue.put(opt_tick)

            await asyncio.sleep(0.1)

    async def place_order(
        self,
        token: str,
        exchange: str,
        symbol: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        price: float = 0.0,
        trigger_price: float = 0.0,
        product_type: str = "INTRADAY",
        tag: str = "",
    ) -> str:
        self._order_counter += 1
        order_id = f"MOCK-{self._order_counter:06d}"
        fill_price = price if order_type == "LIMIT" else (price or 100.0)
        self._orders[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "price": fill_price,
            "status": "COMPLETE",
            "filled_qty": quantity,
            "avg_price": fill_price,
            "tag": tag,
            "timestamp": datetime.now(),
        }
        cost = fill_price * quantity
        if transaction_type == "BUY":
            self._funds["available"] -= cost
            self._funds["used"] += cost
        else:
            self._funds["available"] += cost
            self._funds["used"] = max(0.0, self._funds["used"] - cost)

        logger.info(
            "MockBroker ORDER: %s %s %s qty=%d @ %.2f → %s",
            transaction_type, symbol, order_type, quantity, fill_price, order_id,
        )
        return order_id

    async def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id]["status"] = "CANCELLED"
            return True
        return False

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        return self._orders.get(order_id, {"status": "NOT_FOUND"})

    async def get_positions(self) -> List[Dict[str, Any]]:
        return self._positions

    async def get_funds(self) -> Dict[str, float]:
        return dict(self._funds)


# ---------------------------------------------------------------------------
# Shoonya (Finvasia) Broker Implementation
# ---------------------------------------------------------------------------

class ShoonyaBroker(BaseBroker):
    """
    Concrete implementation for the Shoonya/Finvasia NorenAPI.

    Requires: pip install NorenRestApiPy websocket-client
    The NorenAPI uses callbacks, so we bridge it into asyncio via
    an asyncio.Queue fed by the websocket callback thread.
    """

    _BASE_URL = "https://api.shoonya.com/NorenWClientTP"

    def __init__(self, credentials: BrokerCredentials, config: SystemConfig) -> None:
        super().__init__(credentials, config)
        self._api: Any = None                      # NorenApi instance
        self._tick_queue: asyncio.Queue[IndexTick | OptionTick] = asyncio.Queue(maxsize=5000)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._token_symbol_map: Dict[str, str] = {}

    async def authenticate(self) -> bool:
        try:
            from NorenRestApiPy.NorenApi import NorenApi  # type: ignore[import]

            class _ShoonyaApi(NorenApi):
                def __init__(self_inner) -> None:
                    super().__init__(
                        host=self._BASE_URL,
                        websocket="wss://api.shoonya.com/NorenWSTP/",
                    )

            self._api = _ShoonyaApi()
            self._loop = asyncio.get_event_loop()

            ret = await asyncio.to_thread(
                self._api.login,
                userid=self.credentials.user_id,
                password=self.credentials.password,
                twoFA=self.credentials.totp_secret,
                vendor_code=self.credentials.vendor_code,
                api_secret=self.credentials.api_secret,
                imei=self.credentials.imei,
            )
            if ret and ret.get("stat") == "Ok":
                logger.info("Shoonya: Authenticated successfully for user %s.", self.credentials.user_id)
                return True
            logger.error("Shoonya auth failed: %s", ret)
            return False
        except ImportError:
            logger.error("NorenRestApiPy not installed. Run: pip install NorenRestApiPy")
            return False

    async def logout(self) -> None:
        if self._api:
            await asyncio.to_thread(self._api.logout)

    async def fetch_instrument_master(self, exchange: str = "NFO") -> pd.DataFrame:
        import io
        import zipfile

        url = f"https://api.shoonya.com/{exchange}_symbols.txt.zip"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                raw = await resp.read()

        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                df = pd.read_csv(f)

        # Normalize column names
        df.columns = [c.lower().strip() for c in df.columns]
        rename_map = {
            "token": "token", "tradingsymbol": "symbol",
            "exch_seg": "exchange", "instrument": "instrument_type",
            "strikeprice": "strike", "optiontype": "option_type",
            "expiry": "expiry", "lotsize": "lot_size",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        if "expiry" in df.columns:
            df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date

        return df

    async def find_option_contracts(
        self,
        underlying: str,
        expiry: date,
        strikes: List[float],
    ) -> List[InstrumentInfo]:
        master = await self.fetch_instrument_master()
        mask = (
            master.get("instrument_type", pd.Series()).str.contains("OPT", na=False)
        ) & (
            master.get("symbol", pd.Series()).str.startswith(underlying)
        ) & (
            master.get("expiry") == expiry
        ) & (
            master.get("strike").isin(strikes)
        )
        result = []
        for _, row in master[mask].iterrows():
            result.append(InstrumentInfo(
                token=str(row.get("token", "")),
                symbol=str(row.get("symbol", "")),
                exchange=str(row.get("exchange", "NFO")),
                instrument_type=str(row.get("instrument_type", "OPTIDX")),
                strike=float(row.get("strike", 0)),
                option_type=str(row.get("option_type", "")),
                expiry=row.get("expiry"),
                lot_size=int(row.get("lot_size", 25)),
            ))
        return result

    async def get_atm_strike(self, underlying: str) -> Tuple[float, float]:
        if not self._api:
            raise RuntimeError("Not authenticated.")
        quote = await asyncio.to_thread(
            self._api.get_quotes, exchange="NSE", token=underlying
        )
        spot = float(quote.get("lp", 0))
        step = 50 if underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100
        atm = round(spot / step) * step
        return spot, float(atm)

    async def fetch_historical_candles(
        self,
        token: str,
        exchange: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        if not self._api:
            raise RuntimeError("Not authenticated.")
        data = await asyncio.to_thread(
            self._api.get_time_price_series,
            exchange=exchange,
            token=token,
            starttime=int(from_dt.timestamp()),
            endtime=int(to_dt.timestamp()),
            interval=interval,
        )
        if not data:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["time"], unit="s") if "time" in df else pd.to_datetime(df["ssboe"], unit="s")
        df = df.rename(columns={"into": "open", "inth": "high", "intl": "low", "intc": "close", "intv": "volume"})
        df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.set_index("datetime").sort_index()

    async def subscribe(self, tokens: List[str]) -> None:
        if not self._api:
            raise RuntimeError("Not authenticated.")
        self._subscribed_tokens.extend(tokens)
        # Open websocket with Shoonya callback bridge
        def _on_open() -> None:
            logger.info("Shoonya WS opened.")

        def _on_message(msg: Dict[str, Any]) -> None:
            if self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._handle_shoonya_message(msg), self._loop
                )

        def _on_error(msg: str) -> None:
            logger.error("Shoonya WS error: %s", msg)

        def _on_close() -> None:
            logger.warning("Shoonya WS closed.")

        await asyncio.to_thread(
            self._api.start_websocket,
            subscribe_callback=_on_message,
            order_update_callback=None,
            socket_open_callback=_on_open,
            socket_close_callback=_on_close,
            socket_error_callback=_on_error,
        )

        # Subscribe tokens
        channel_list = [f"NFO|{t}" for t in tokens]
        await asyncio.to_thread(self._api.subscribe, channel_list)

    async def _handle_shoonya_message(self, msg: Dict[str, Any]) -> None:
        """Normalize a raw Shoonya websocket message into a typed tick."""
        try:
            token = str(msg.get("tk", ""))
            sym = self._token_symbol_map.get(token, token)
            ltp = float(msg.get("lp", 0) or 0)
            ts = datetime.fromtimestamp(int(msg.get("ft", time.time())))

            if msg.get("e") in ("NSE", "BSE"):
                tick: IndexTick | OptionTick = IndexTick(
                    symbol=sym,
                    ltp=ltp,
                    open=float(msg.get("op", ltp) or ltp),
                    high=float(msg.get("h", ltp) or ltp),
                    low=float(msg.get("l", ltp) or ltp),
                    close=ltp,
                    volume=int(msg.get("v", 0) or 0),
                    timestamp=ts,
                )
            else:
                tick = OptionTick(
                    symbol=sym,
                    underlying=sym[:len(sym) - 15] if len(sym) > 15 else sym,
                    strike=0.0,
                    option_type="CE" if sym.endswith("CE") else "PE",
                    expiry=date.today(),
                    ltp=ltp,
                    bid=float(msg.get("bp1", ltp - 0.5) or ltp),
                    ask=float(msg.get("sp1", ltp + 0.5) or ltp),
                    oi=int(msg.get("oi", 0) or 0),
                    change_oi=int(msg.get("doi", 0) or 0),
                    volume=int(msg.get("v", 0) or 0),
                    iv=float(msg.get("iv", 0) or 0),
                    delta=float(msg.get("delta", 0) or 0),
                    timestamp=ts,
                )
            if not self._tick_queue.full():
                await self._tick_queue.put(tick)
        except Exception as exc:
            logger.debug("Shoonya message parse error: %s | raw: %s", exc, msg)

    async def unsubscribe(self, tokens: List[str]) -> None:
        if self._api:
            channel_list = [f"NFO|{t}" for t in tokens]
            await asyncio.to_thread(self._api.unsubscribe, channel_list)

    async def stream_ticks(self) -> AsyncIterator[IndexTick | OptionTick]:  # type: ignore[override]
        while True:
            try:
                tick = await asyncio.wait_for(self._tick_queue.get(), timeout=5.0)
                yield tick
            except asyncio.TimeoutError:
                logger.debug("Shoonya: No tick for 5s — heartbeat check.")
                continue

    async def place_order(
        self,
        token: str,
        exchange: str,
        symbol: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        price: float = 0.0,
        trigger_price: float = 0.0,
        product_type: str = "I",
        tag: str = "",
    ) -> str:
        if not self._api:
            raise RuntimeError("Not authenticated.")
        type_map = {"MARKET": "MKT", "LIMIT": "LMT", "SL": "SL-LMT", "SL-M": "SL-MKT"}
        ret = await asyncio.to_thread(
            self._api.place_order,
            buy_or_sell="B" if transaction_type == "BUY" else "S",
            product_type=product_type,
            exchange=exchange,
            tradingsymbol=symbol,
            quantity=quantity,
            discloseqty=0,
            price_type=type_map.get(order_type, "MKT"),
            price=price,
            trigger_price=trigger_price if trigger_price > 0 else None,
            retention="DAY",
            remarks=tag,
        )
        if ret and ret.get("stat") == "Ok":
            return ret["norenordno"]
        raise RuntimeError(f"Shoonya order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        ret = await asyncio.to_thread(self._api.cancel_order, orderno=order_id)
        return bool(ret and ret.get("stat") == "Ok")

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        orders = await asyncio.to_thread(self._api.get_orderbook)
        for o in (orders or []):
            if o.get("norenordno") == order_id:
                return {
                    "status": o.get("status", "UNKNOWN"),
                    "filled_qty": int(o.get("fillshares", 0) or 0),
                    "avg_price": float(o.get("avgprc", 0) or 0),
                }
        return {"status": "NOT_FOUND", "filled_qty": 0, "avg_price": 0.0}

    async def get_positions(self) -> List[Dict[str, Any]]:
        raw = await asyncio.to_thread(self._api.get_positions)
        return raw or []

    async def get_funds(self) -> Dict[str, float]:
        raw = await asyncio.to_thread(self._api.get_limits)
        if raw:
            return {
                "available": float(raw.get("cash", 0) or 0),
                "used": float(raw.get("marginused", 0) or 0),
            }
        return {"available": 0.0, "used": 0.0}


# ---------------------------------------------------------------------------
# Factory — resolve provider string → concrete class
# ---------------------------------------------------------------------------

_PROVIDER_MAP: Dict[str, type] = {
    "mock": MockBroker,
    "shoonya": ShoonyaBroker,
    # "angelone": AngelOneBroker,  # Extend here
    # "dhan": DhanBroker,
    # "fyers": FyersBroker,
}


def create_broker(config: SystemConfig) -> BaseBroker:
    """Return the appropriate broker instance based on config.broker.provider."""
    provider = config.broker.provider.lower()
    cls = _PROVIDER_MAP.get(provider)
    if cls is None:
        raise ValueError(
            f"Unknown broker provider '{provider}'. Available: {list(_PROVIDER_MAP)}"
        )
    return cls(config.broker, config)
