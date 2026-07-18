#!/usr/bin/env python3
"""
scripts/run_production_cascade.py

Production forward-test runner for the V4 macro-to-micro spot cascade.

Features:
  - Reads the Upstox access token from the environment (UPSTOX_ACCESS_TOKEN).
  - Loads the live option instrument registry via data_layer.instrument_registry.
  - Polls the Upstox MarketQuote / History API for the selected index spot.
  - Builds rolling 1m / 5m / 15m / 75m OHLC bars anchored at 09:15 IST.
  - Detects V4 macro traps (75m/150m/225m) and waits for zone re-entry + MTF/LTF
    rejection before triggering a 1m close-mode entry.
  - Places LIMIT entry orders at the current option premium, then manages:
      * dual-tranche (50/50) split
      * 1R break-even move on both tranches
      * 2R fixed target for Tranche 1
      * structural trailing stop for Tranche 2 (configurable 4x5m or 2x15m)

Usage (paper mode, default):
    set UPSTOX_ACCESS_TOKEN=...
    python scripts/run_production_cascade.py --index NIFTY

Usage (real money — requires explicit --live and a working instrument map):
    set UPSTOX_ACCESS_TOKEN=...
    python scripts/run_production_cascade.py --index NIFTY --live --delta 0.55

WARNING: This is a production scaffold. Review all risk parameters and the
option-premium mapping before running with real money. Paper mode is default.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

import pandas as pd
import pytz
import upstox_client

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.client_profiles import BrokerBinding
from data_layer.instrument_registry import REGISTRY as INSTRUMENT_REGISTRY
from data_layer.symbol_translator import InternalSymbol
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from execution_bridge.broker_upstox import UpstoxBroker
from strategies.trap_scanner import v4_spot_cascade as v4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
ENTRY_END = time(15, 15)
POLL_INTERVAL_SECONDS = 5

# Spot index strike step used to pick the ATM option strike.
STRIKE_STEP: Dict[str, int] = {
    "NIFTY": 50,
    "SENSEX": 100,
    "BANKNIFTY": 100,
}

# Default delta multiplier for spot → option premium approximation.
DEFAULT_DELTA = 0.55


# ---------------------------------------------------------------------------
# Helper data classes
# ---------------------------------------------------------------------------

@dataclass
class EntrySignal:
    setup_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    direction: str  # "LONG" or "SHORT"
    entry_price: float  # spot trigger price
    sl: float           # spot SL
    target: float       # spot target
    initial_risk: float
    multiplier: str
    macro_confirm_ts: pd.Timestamp


@dataclass
class TrancheOrderIds:
    t1_target: Optional[str] = None
    t1_sl: Optional[str] = None
    t2_sl: Optional[str] = None


@dataclass
class PositionState:
    active: bool = False
    side: str = ""  # "LONG" or "SHORT"
    entry_price: float = 0.0
    entry_premium: float = 0.0
    initial_risk: float = 0.0
    initial_risk_premium: float = 0.0
    total_qty: int = 0
    tranche_qty: int = 0
    running_best_spot: float = 0.0
    break_even_done: bool = False
    trailing_active: bool = False
    current_sl_spot: float = 0.0
    current_sl_premium: float = 0.0
    entry_order_id: Optional[str] = None
    order_ids: TrancheOrderIds = field(default_factory=TrancheOrderIds)
    setup_ts: Optional[pd.Timestamp] = None


# ---------------------------------------------------------------------------
# Candle builder
# ---------------------------------------------------------------------------

class CandleBuilder:
    """Build 1m OHLC bars from a stream of LTP ticks."""

    def __init__(self, market_open: time = MARKET_OPEN) -> None:
        self.market_open = market_open
        self.current: Optional[Dict] = None
        self.closed: List[Dict] = []

    def on_tick(self, price: float, ts: pd.Timestamp) -> None:
        minute = ts.replace(second=0, microsecond=0)
        if self.current is None or self.current["datetime"] != minute:
            if self.current is not None:
                self.closed.append(self.current)
            self.current = {
                "datetime": minute,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0,
            }
        else:
            self.current["high"] = max(self.current["high"], price)
            self.current["low"] = min(self.current["low"], price)
            self.current["close"] = price

    def close_final_candle(self) -> None:
        if self.current is not None:
            self.closed.append(self.current)
            self.current = None

    def df(self) -> pd.DataFrame:
        rows = list(self.closed)
        if self.current is not None:
            rows.append(self.current)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("datetime").reset_index(drop=True)
        return df


# ---------------------------------------------------------------------------
# Market data client
# ---------------------------------------------------------------------------

class UpstoxMarketDataClient:
    """Thin wrapper around the Upstox MarketQuote and History APIs."""

    def __init__(self, api_client: upstox_client.ApiClient) -> None:
        self._quote_api = upstox_client.MarketQuoteApi(api_client)
        self._hist_api = upstox_client.HistoryApi(api_client)

    async def get_full_quote(self, instrument_key: str) -> Optional[Dict]:
        try:
            ret = await asyncio.to_thread(
                self._quote_api.get_full_market_quote,
                symbol=instrument_key,
                api_version="2.0",
            )
            if ret and ret.status == "success" and ret.data:
                # ret.data is a dict keyed by instrument_key
                return dict(ret.data)
            return None
        except Exception as exc:
            logger.warning("Market quote failed for %s: %s", instrument_key, exc)
            return None

    async def ltp(self, instrument_key: str) -> Optional[float]:
        q = await self.get_full_quote(instrument_key)
        if not q:
            return None
        data = q.get(instrument_key) or q.get(instrument_key.upper()) or list(q.values())[0]
        if data is None:
            return None
        # depth/ltp structure varies slightly; try common paths
        if isinstance(data, dict):
            if "last_price" in data:
                return float(data["last_price"])
            if "ltp" in data:
                return float(data["ltp"])
            if "ohlc" in data and isinstance(data["ohlc"], dict):
                return float(data["ohlc"].get("close", 0))
        return None

    async def fetch_intraday_1m(self, instrument_key: str) -> pd.DataFrame:
        """Load today's 1m candles so far via the History API."""
        try:
            ret = await asyncio.to_thread(
                self._hist_api.get_intra_day_candle_data,
                instrument_key=instrument_key,
                interval="1minute",
                api_version="2.0",
            )
            if not ret or not ret.data:
                return pd.DataFrame()
            rows = []
            for c in ret.data:
                rows.append({
                    "datetime": pd.to_datetime(c.timestamp, utc=True).tz_convert("Asia/Kolkata")
                    if c.timestamp else None,
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "volume": int(c.volume) if c.volume else 0,
                })
            df = pd.DataFrame(rows).dropna(subset=["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
            return df
        except Exception as exc:
            logger.warning("Intraday history fetch failed: %s", exc)
            return pd.DataFrame()


# ---------------------------------------------------------------------------
# Production cascade runner
# ---------------------------------------------------------------------------

class ProductionCascade:
    def __init__(
        self,
        index_name: str,
        access_token: str,
        live: bool = False,
        delta: float = DEFAULT_DELTA,
        trailing_profile: str = "dual_2R_4x5m",
        client_id: str = "prod_cascade",
    ) -> None:
        self.index_name = index_name.upper()
        self.access_token = access_token
        self.live = live
        self.delta = delta
        self.trailing_profile = trailing_profile
        self.client_id = client_id
        self.ist = IST

        self._spot_key = INSTRUMENT_REGISTRY.get_upstox_index_key(self.index_name)
        self._candle_builder = CandleBuilder()
        self._last_signal_entry_ts: Optional[pd.Timestamp] = None
        self._position = PositionState()
        self._shutdown = False

        self._trailing_cfg = self._parse_trailing_profile(trailing_profile)
        self._tranche_units = v4.INDEX_LOT_CONFIG.get(self.index_name, 75)
        self._total_units = self._tranche_units * 2  # 2 lots, 1 lot per tranche
        self._strike_step = STRIKE_STEP.get(self.index_name, 50)

        self._api_client: Optional[upstox_client.ApiClient] = None
        self._market: Optional[UpstoxMarketDataClient] = None
        self._broker: Optional[UpstoxBroker] = None

    def _parse_trailing_profile(self, profile: str) -> Dict:
        mapping = {
            "single_tranche": {"dual": False, "activation_r": 2.0, "tf": "5m", "lookback": 4},
            "dual_1.5R_2x5m": {"dual": True, "activation_r": 1.5, "tf": "5m", "lookback": 2},
            "dual_2R_4x5m": {"dual": True, "activation_r": 2.0, "tf": "5m", "lookback": 4},
            "dual_2R_2x15m": {"dual": True, "activation_r": 2.0, "tf": "15m", "lookback": 2},
        }
        return mapping.get(profile, mapping["dual_2R_4x5m"])

    async def initialize(self) -> bool:
        """Authenticate broker and load the instrument registry."""
        # Upstox API client (also used for market data)
        config = upstox_client.Configuration()
        config.access_token = self.access_token
        self._api_client = upstox_client.ApiClient(configuration=config)
        self._market = UpstoxMarketDataClient(self._api_client)

        # Broker binding
        binding = BrokerBinding(
            binding_id="upstox_cascade",
            provider="upstox",
            access_token=self.access_token,
            client_code=os.environ.get("UPSTOX_CLIENT_ID", ""),
            api_key=os.environ.get("UPSTOX_API_KEY", ""),
            api_secret=os.environ.get("UPSTOX_API_SECRET", ""),
            trading_mode="live" if self.live else "paper",
            product_type="MIS",
        )
        self._broker = UpstoxBroker(binding, self.client_id)
        ok = await self._broker.authenticate()
        if not ok:
            logger.error("Broker authentication failed.")
            return False

        # Load option contracts for the underlying
        logger.info("Loading instrument registry for %s ...", self.index_name)
        await asyncio.to_thread(INSTRUMENT_REGISTRY.load_sync, self.index_name, self.access_token)
        if not INSTRUMENT_REGISTRY.is_loaded(self.index_name):
            logger.error("Instrument registry failed to load for %s.", self.index_name)
            return False

        instrument_map = INSTRUMENT_REGISTRY.build_instrument_map(self.index_name)
        logger.info("Instrument map loaded: %d contracts.", len(instrument_map))
        self._broker.inject_instrument_map(instrument_map)
        return True

    def _active_expiry(self, as_of: date) -> Optional[date]:
        return INSTRUMENT_REGISTRY.get_active_expiry(self.index_name, as_of)

    def _atm_strike(self, spot: float, expiry: date) -> int:
        step = self._strike_step
        return int(round(spot / step) * step)

    def _option_key(self, spot: float, direction: str, expiry: date) -> Optional[str]:
        strike = self._atm_strike(spot, expiry)
        opt_type = "CE" if direction == "LONG" else "PE"
        return INSTRUMENT_REGISTRY.get_upstox_key(self.index_name, expiry, strike, opt_type)

    def _canonical_symbol(self, spot: float, direction: str, expiry: date) -> str:
        strike = self._atm_strike(spot, expiry)
        opt_type = "CE" if direction == "LONG" else "PE"
        return str(InternalSymbol(
            underlying=self.index_name,
            strike=float(strike),
            option_type=opt_type,
            expiry=expiry,
        ))

    def _minutes_to_close(self, ts: pd.Timestamp) -> pd.Timestamp:
        return ts + pd.Timedelta(minutes=1)

    async def _seed_candles(self) -> None:
        """Pre-load today's 1m candles so the engine has context at startup."""
        now = datetime.now(self.ist)
        if now.time() < MARKET_OPEN:
            logger.info("Market not open yet; no historical seed needed.")
            return
        df = await self._market.fetch_intraday_1m(self._spot_key)
        if df.empty:
            logger.warning("No intraday history available; will build from live ticks.")
            return
        for _, row in df.iterrows():
            self._candle_builder.on_tick(float(row["close"]), row["datetime"])
        logger.info("Seeded %d 1m candles from history.", len(df))

    def _find_entry_signal(self, df_1m: pd.DataFrame) -> Optional[EntrySignal]:
        """Run the V4 backtest on the current day's bars and extract the latest entry signal."""
        if df_1m.empty or len(df_1m) < 10:
            return None

        # Only evaluate today
        today = df_1m["datetime"].iloc[-1].date()
        df_today = df_1m[df_1m["datetime"].dt.date == today].copy()
        if df_today.empty:
            return None

        cfg = self._trailing_cfg
        _, trades = v4.backtest_macro_to_micro(
            df_today,
            multipliers=[75, 150, 225],
            lookback=3,
            use_filters=False,
            require_zone_reentry=True,
            require_mtf_ltf_rejection=True,
            entry_mode="close",
            dual_tranche=False,
            trailing_activation_r=cfg["activation_r"],
            trailing_tf=cfg["tf"],
            trailing_lookback=cfg["lookback"],
            index_name=self.index_name,
        )
        if trades.empty:
            return None

        # Most recent entry signal on the current (latest) 1m candle
        latest_ts = df_today["datetime"].iloc[-1]
        latest = trades[trades["entry_ts"] == latest_ts]
        if latest.empty:
            return None
        r = latest.sort_values("entry_ts").iloc[-1]
        if self._last_signal_entry_ts is not None and r["entry_ts"] <= self._last_signal_entry_ts:
            return None

        return EntrySignal(
            setup_ts=r["setup_ts"],
            entry_ts=r["entry_ts"],
            direction=r["direction"],
            entry_price=float(r["entry_price"]),
            sl=float(r["sl"]),
            target=float(r["target"]),
            initial_risk=abs(float(r["entry_price"]) - float(r["sl"])),
            multiplier=str(r.get("multiplier", "75m")),
            macro_confirm_ts=r.get("macro_confirm_ts"),
        )

    async def _wait_for_entry_fill(self, timeout: int = 60) -> bool:
        """Poll the entry order status for up to timeout seconds."""
        if self._position.entry_order_id is None:
            return False
        deadline = datetime.now(self.ist) + timedelta(seconds=timeout)
        while datetime.now(self.ist) < deadline:
            fill = await self._broker.get_order_status(self._position.entry_order_id)
            if fill.status.name == "COMPLETE":
                return True
            if fill.status.name in ("REJECTED", "CANCELLED"):
                return False
            await asyncio.sleep(2)
        return False

    async def _enter_position(self, signal: EntrySignal, spot_ltp: float) -> bool:
        """Place the option entry order, then the SL and target orders."""
        today = date.today()
        expiry = self._active_expiry(today)
        if expiry is None:
            logger.error("No active expiry found for %s; cannot place option order.", self.index_name)
            return False

        canonical = self._canonical_symbol(spot_ltp, signal.direction, expiry)
        option_key = self._option_key(spot_ltp, signal.direction, expiry)
        if not option_key:
            logger.error("No option instrument key found for %s; cannot place order.", canonical)
            return False

        # Use current option premium as the limit entry price
        entry_premium = await self._market.ltp(option_key)
        if entry_premium is None:
            logger.error("Failed to fetch option LTP for %s; aborting entry.", canonical)
            return False

        initial_risk_premium = signal.initial_risk * self.delta
        if signal.direction == "LONG":
            sl_premium = entry_premium - initial_risk_premium
            target_premium = entry_premium + 2 * initial_risk_premium
        else:
            sl_premium = entry_premium + initial_risk_premium
            target_premium = entry_premium - 2 * initial_risk_premium

        sl_premium = round(sl_premium, 2)
        target_premium = round(target_premium, 2)

        tag = f"v4_{self.index_name}_{signal.setup_ts.strftime('%H%M')}"
        logger.info(
            "[ENTRY] %s %s | spot entry=%.2f SL=%.2f target=%.2f | option=%s premium=%.2f SL=%.2f T=%.2f",
            signal.direction, self.index_name, signal.entry_price, signal.sl, signal.target,
            canonical, entry_premium, sl_premium, target_premium,
        )

        if not self.live:
            logger.info("[PAPER] Simulated entry fill for %s @ %.2f", canonical, entry_premium)
            self._position.active = True
            self._position.side = signal.direction
            self._position.entry_price = signal.entry_price
            self._position.entry_premium = entry_premium
            self._position.initial_risk = signal.initial_risk
            self._position.initial_risk_premium = initial_risk_premium
            self._position.total_qty = self._total_units
            self._position.tranche_qty = self._tranche_units
            self._position.running_best_spot = signal.entry_price
            self._position.current_sl_spot = signal.sl
            self._position.current_sl_premium = sl_premium
            self._position.setup_ts = signal.setup_ts
            return True

        # Live entry order (2 lots combined)
        req = OrderRequest(
            broker_symbol=canonical,
            exchange="NFO",
            side=OrderSide.BUY,
            qty=self._total_units,
            order_type=OrderType.LIMIT,
            price=round(entry_premium, 2),
            trigger_price=0,
            product="INTRADAY",
            tag=tag,
            client_id=self.client_id,
        )
        try:
            entry_id = await self._broker.place_order(req)
        except Exception as exc:
            logger.error("Entry order failed: %s", exc)
            return False

        self._position.entry_order_id = entry_id
        logger.info("Entry order placed: %s", entry_id)

        filled = await self._wait_for_entry_fill(timeout=60)
        if not filled:
            logger.warning("Entry order not filled in time; cancelling.")
            await self._broker.cancel_order(entry_id)
            self._position = PositionState()
            return False

        # Place T1 target + T1 SL, and T2 SL (each tranche = 1 lot)
        def _sl_req(qty: int, t: str) -> OrderRequest:
            return OrderRequest(
                broker_symbol=canonical,
                exchange="NFO",
                side=OrderSide.SELL if signal.direction == "LONG" else OrderSide.BUY,
                qty=qty,
                order_type=OrderType.SL_L,
                price=sl_premium,
                trigger_price=sl_premium,
                product="INTRADAY",
                tag=t,
                client_id=self.client_id,
            )

        def _target_req(qty: int, t: str) -> OrderRequest:
            return OrderRequest(
                broker_symbol=canonical,
                exchange="NFO",
                side=OrderSide.SELL if signal.direction == "LONG" else OrderSide.BUY,
                qty=qty,
                order_type=OrderType.LIMIT,
                price=target_premium,
                trigger_price=0,
                product="INTRADAY",
                tag=t,
                client_id=self.client_id,
            )

        try:
            t1_sl_id = await self._broker.place_order(_sl_req(self._tranche_units, f"{tag}_t1sl"))
            t2_sl_id = await self._broker.place_order(_sl_req(self._tranche_units, f"{tag}_t2sl"))
            t1_tgt_id = await self._broker.place_order(_target_req(self._tranche_units, f"{tag}_t1tgt"))
        except Exception as exc:
            logger.error("SL/target order failed: %s", exc)
            return False

        self._position.order_ids.t1_sl = t1_sl_id
        self._position.order_ids.t2_sl = t2_sl_id
        self._position.order_ids.t1_target = t1_tgt_id
        self._position.active = True
        self._position.side = signal.direction
        self._position.entry_price = signal.entry_price
        self._position.entry_premium = entry_premium
        self._position.initial_risk = signal.initial_risk
        self._position.initial_risk_premium = initial_risk_premium
        self._position.total_qty = self._total_units
        self._position.tranche_qty = self._tranche_units
        self._position.running_best_spot = signal.entry_price
        self._position.current_sl_spot = signal.sl
        self._position.current_sl_premium = sl_premium
        self._position.setup_ts = signal.setup_ts
        logger.info("T1 SL %s, T2 SL %s, and T1 target %s placed.", t1_sl_id, t2_sl_id, t1_tgt_id)
        return True

    async def _update_trailing_sl(self, current_1m_ts: pd.Timestamp) -> None:
        """Recompute and update the Tranche 2 SL if it has tightened."""
        if not self._position.active or not self._position.order_ids.t2_sl:
            return

        pos = self._position
        df_1m = self._candle_builder.df()
        df_5m = v4._resample_per_day(df_1m, 5) if not df_1m.empty else pd.DataFrame()
        df_15m = v4._resample_per_day(df_1m, 15) if not df_1m.empty else pd.DataFrame()

        cfg = self._trailing_cfg
        new_spot_sl = v4._compute_trailing_sl(
            kind="BEAR" if pos.side == "LONG" else "BULL",
            current_1m_ts=current_1m_ts,
            df_5m_full=df_5m,
            df_15m_full=df_15m,
            current_sl=pos.current_sl_spot,
            entry=pos.entry_price,
            initial_risk=pos.initial_risk,
            running_best=pos.running_best_spot,
            trailing_activation_r=cfg["activation_r"],
            trailing_tf=cfg["tf"],
            trailing_lookback=cfg["lookback"],
        )

        # Convert spot SL movement to option premium movement
        sl_spot_delta = new_spot_sl - pos.current_sl_spot
        sl_premium_delta = sl_spot_delta * self.delta
        new_sl_premium = pos.current_sl_premium + sl_premium_delta
        new_sl_premium = round(new_sl_premium, 2)

        # Only tighten (for long: raise SL; for short: lower SL)
        if pos.side == "LONG" and new_sl_premium <= pos.current_sl_premium:
            return
        if pos.side == "SHORT" and new_sl_premium >= pos.current_sl_premium:
            return

        logger.info("[TRAIL] Updating T2 SL for %s: spot %.2f -> %.2f | premium %.2f -> %.2f",
                    pos.side, pos.current_sl_spot, new_spot_sl,
                    pos.current_sl_premium, new_sl_premium)

        if self.live:
            await self._broker.cancel_order(pos.order_ids.t2_sl)
            canonical = self._canonical_symbol(0, pos.side, self._active_expiry(date.today()))
            sl_req = OrderRequest(
                broker_symbol=canonical,
                exchange="NFO",
                side=OrderSide.SELL if pos.side == "LONG" else OrderSide.BUY,
                qty=self._tranche_units,
                order_type=OrderType.SL_L,
                price=new_sl_premium,
                trigger_price=new_sl_premium,
                product="INTRADAY",
                tag=f"v4_{self.index_name}_t2sl_trail",
                client_id=self.client_id,
            )
            try:
                new_id = await self._broker.place_order(sl_req)
                pos.order_ids.t2_sl = new_id
            except Exception as exc:
                logger.error("Failed to place trailing SL: %s", exc)
                return

        pos.current_sl_spot = new_spot_sl
        pos.current_sl_premium = new_sl_premium
    async def _manage_position(self, spot_ltp: float, ts: pd.Timestamp) -> None:
        """Break-even and trailing logic."""
        if not self._position.active:
            return

        pos = self._position
        # Update running best
        if pos.side == "LONG":
            pos.running_best_spot = max(pos.running_best_spot, spot_ltp)
        else:
            pos.running_best_spot = min(pos.running_best_spot, spot_ltp)

        # 1R break-even: move both T1 SL and T2 SL to entry
        profit = pos.running_best_spot - pos.entry_price if pos.side == "LONG" else pos.entry_price - pos.running_best_spot
        if not pos.break_even_done and profit >= pos.initial_risk:
            logger.info("[BE] 1R reached for %s; moving SLs to entry.", self.index_name)
            if self.live:
                for sl_oid in (pos.order_ids.t1_sl, pos.order_ids.t2_sl):
                    if sl_oid:
                        await self._broker.cancel_order(sl_oid)
                canonical = self._canonical_symbol(0, pos.side, self._active_expiry(date.today()))
                be_sl_req = OrderRequest(
                    broker_symbol=canonical,
                    exchange="NFO",
                    side=OrderSide.SELL if pos.side == "LONG" else OrderSide.BUY,
                    qty=self._tranche_units,
                    order_type=OrderType.SL_L,
                    price=round(pos.entry_premium, 2),
                    trigger_price=round(pos.entry_premium, 2),
                    product="INTRADAY",
                    tag=f"v4_{self.index_name}_sl_be",
                    client_id=self.client_id,
                )
                try:
                    t1_be = await self._broker.place_order(be_sl_req)
                    t2_be = await self._broker.place_order(be_sl_req)
                    pos.order_ids.t1_sl = t1_be
                    pos.order_ids.t2_sl = t2_be
                except Exception as exc:
                    logger.error("Failed to place break-even SLs: %s", exc)
            pos.current_sl_spot = pos.entry_price
            pos.current_sl_premium = pos.entry_premium
            pos.break_even_done = True

        # If T1 target has filled, cancel its dedicated SL (T2 SL remains for tranche 2)
        if self.live and pos.order_ids.t1_target and pos.order_ids.t1_sl:
            try:
                fill = await self._broker.get_order_status(pos.order_ids.t1_target)
                if fill.status.name == "COMPLETE":
                    await self._broker.cancel_order(pos.order_ids.t1_sl)
                    pos.order_ids.t1_sl = None
            except Exception as exc:
                logger.warning("T1 target status check failed: %s", exc)

        # Trailing for tranche 2 (only when profit >= activation_r * R)
        if profit >= pos.initial_risk * self._trailing_cfg["activation_r"]:
            await self._update_trailing_sl(ts)

        # In paper mode, simulate exits from spot levels
        if not self.live:
            target_spot = pos.entry_price + 2 * pos.initial_risk if pos.side == "LONG" else pos.entry_price - 2 * pos.initial_risk
            sl_spot = pos.current_sl_spot
            if pos.side == "LONG":
                if spot_ltp >= target_spot:
                    logger.info("[PAPER] T1 target hit for %s.", self.index_name)
                    pos.active = False
                    return
                if spot_ltp <= sl_spot:
                    logger.info("[PAPER] SL hit for %s.", self.index_name)
                    pos.active = False
                    return
            else:
                if spot_ltp <= target_spot:
                    logger.info("[PAPER] T1 target hit for %s.", self.index_name)
                    pos.active = False
                    return
                if spot_ltp >= sl_spot:
                    logger.info("[PAPER] SL hit for %s.", self.index_name)
                    pos.active = False
                    return

    async def _flatten_all(self, reason: str) -> None:
        """Cancel all open orders and clear state at EOD or shutdown."""
        if not self._position.active:
            return
        logger.info("[FLATTEN] %s for %s", reason, self.index_name)
        if self.live:
            for oid in (
                self._position.entry_order_id,
                self._position.order_ids.t1_sl,
                self._position.order_ids.t2_sl,
                self._position.order_ids.t1_target,
            ):
                if oid:
                    try:
                        await self._broker.cancel_order(oid)
                    except Exception as exc:
                        logger.warning("Cancel %s failed: %s", oid, exc)
        self._position = PositionState()

    async def run(self) -> None:
        if not await self.initialize():
            return

        await self._seed_candles()
        last_minute: Optional[pd.Timestamp] = None

        while not self._shutdown:
            now = datetime.now(self.ist)
            if now.time() < MARKET_OPEN:
                wait = (datetime.combine(now.date(), MARKET_OPEN) - now).total_seconds()
                logger.info("Market opens in %.0f seconds; waiting.", wait)
                await asyncio.sleep(max(1, wait))
                continue
            if now.time() >= MARKET_CLOSE:
                await self._flatten_all("EOD")
                logger.info("Market closed. Exiting.")
                break

            # Fetch spot LTP
            spot_ltp = await self._market.ltp(self._spot_key)
            if spot_ltp is None:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue
            ts = pd.Timestamp(now, tz="Asia/Kolkata")
            self._candle_builder.on_tick(spot_ltp, ts)

            # Manage an existing position every tick
            await self._manage_position(spot_ltp, ts)

            # On a new completed 1m candle, evaluate entry signals
            current_minute = ts.replace(second=0, microsecond=0)
            if last_minute is None or current_minute > last_minute:
                last_minute = current_minute
                df_1m = self._candle_builder.df()
                if not self._position.active and now.time() < ENTRY_END:
                    signal = self._find_entry_signal(df_1m)
                    if signal:
                        self._last_signal_entry_ts = signal.entry_ts
                        await self._enter_position(signal, spot_ltp)

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._shutdown = True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="V4 Production Cascade Forward-Test Runner")
    parser.add_argument("--index", type=str, default="NIFTY", choices=["NIFTY", "SENSEX", "BANKNIFTY"])
    parser.add_argument("--live", action="store_true", help="Send real orders. Default is paper logging.")
    parser.add_argument("--delta", type=float, default=DEFAULT_DELTA, help="Spot-to-option delta approximation.")
    parser.add_argument(
        "--trailing-profile",
        type=str,
        default="dual_2R_4x5m",
        choices=["single_tranche", "dual_1.5R_2x5m", "dual_2R_4x5m", "dual_2R_2x15m"],
    )
    parser.add_argument("--client-id", type=str, default="prod_cascade")
    args = parser.parse_args()

    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        print("ERROR: Set UPSTOX_ACCESS_TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    if args.live:
        print("\n" + "=" * 70)
        print("WARNING: --live WILL SEND REAL ORDERS TO UPSTOX.")
        print("Ensure the instrument registry is loaded and all risk limits are correct.")
        print("=" * 70 + "\n")

    runner = ProductionCascade(
        index_name=args.index,
        access_token=token,
        live=args.live,
        delta=args.delta,
        trailing_profile=args.trailing_profile,
        client_id=args.client_id,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, runner.stop)
    except NotImplementedError:
        logger.warning("Signal handlers not supported on this platform; use Ctrl-C/Ctrl-Break to stop.")

    try:
        loop.run_until_complete(runner.run())
    finally:
        loop.run_until_complete(runner._flatten_all("SHUTDOWN"))
        loop.close()


if __name__ == "__main__":
    main()
