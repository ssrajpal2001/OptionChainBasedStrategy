"""
data_layer/global_feeder.py — Admin-managed global data feed.

The GlobalFeeder is the single canonical data source for all
downstream components. It wraps one (or a failover pair of) broker
websocket connections, normalizes every raw frame, and fans it out
across the EventBus.

Key design rules:
  • Only ONE GlobalFeeder instance runs at a time (managed by AdminConsole).
  • No strategy or execution logic lives here — pure data normalization.
  • Connection health is monitored via a heartbeat task; reconnect is
    automatic without blocking any other coroutine.
  • No time.sleep — all waits use asyncio.sleep or asyncio.Event.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import (
    BaseFeeder, CandleEvent, EventBus, IndexTick, OptionTick, SystemEvent,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Mock Feeder  (no external dependencies — default provider)
# ─────────────────────────────────────────────────────────────────────────────

class MockFeeder(BaseFeeder):
    """
    Generates synthetic IST-timestamped ticks for all monitored indices.
    Used in paper-trading and testing modes.

    Implements the two-stage BaseFeeder contract:
      _ws_loop()    — generates synthetic "raw" frames and enqueues them
      _parse_frame() — converts each frame into IndexTick/OptionTick and publishes
    """

    _BASE: Dict[str, float] = {
        "NIFTY": 24_500.0, "BANKNIFTY": 52_000.0,
        "FINNIFTY": 23_000.0, "SENSEX": 80_000.0, "MIDCPNIFTY": 12_000.0,
    }

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        super().__init__(bus)
        self._cfg = cfg
        self._prices: Dict[str, float] = dict(self._BASE)
        self._tick_interval = 0.1          # 100 ms per tick batch
        self._rng = random.Random()

    async def connect(self) -> bool:
        self._connected = True
        logger.info("MockFeeder: connected (synthetic mode).")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        logger.debug("MockFeeder: subscribed to %d tokens.", len(tokens))

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        pass

    async def _ws_loop(self) -> None:
        """
        Simulate a WebSocket receive loop.  Each 'frame' is a pre-built dict
        that _parse_frame() will convert into typed ticks.  No heavy computation
        happens here — the frame dict is created cheaply and enqueued immediately.
        """
        from data_layer.instrument_registry import next_expiry as _nexp
        while self._running:
            now = datetime.now(IST)

            for underlying in self._cfg.monitored_indices:
                expiry = _nexp(underlying) or (now.date() + __import__("datetime").timedelta(days=7))
                # Commodities/unknowns have no synthetic base — skip in mock mode
                # (the live dual feed provides their futures/ATM ticks).
                if underlying not in self._prices:
                    continue
                p = self._prices[underlying]
                p = max(p * (1 + self._rng.gauss(0, 0.0003)), 1.0)
                self._prices[underlying] = p
                step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
                atm = round(p / step) * step

                # Enqueue one raw frame per underlying (cheap dict, no computation)
                self._enqueue_raw({
                    "type": "batch",
                    "underlying": underlying,
                    "ltp": round(p, 2),
                    "atm": atm,
                    "step": step,
                    "expiry": expiry,
                    "timestamp": now,
                })

            await asyncio.sleep(self._tick_interval)

    async def _parse_frame(self, raw: Any) -> None:
        """
        Expand one raw batch frame into IndexTick + OptionTicks and publish.
        All CPU work (option pricing math, random draws) happens here,
        isolated from the WS receive path.
        """
        underlying = raw["underlying"]
        p          = raw["ltp"]
        atm        = raw["atm"]
        step       = raw["step"]
        expiry     = raw["expiry"]
        now        = raw["timestamp"]

        tick = IndexTick(
            symbol=underlying, ltp=p,
            open=round(p * 0.9998, 2), high=round(p * 1.001, 2),
            low=round(p * 0.999, 2), close=p,
            volume=self._rng.randint(1_000, 50_000), timestamp=now,
        )
        await self._publish_index(tick)

        for i in range(-3, 4):
            strike = atm + i * step
            for opt_type in ("CE", "PE"):
                intrinsic = max((p - strike) if opt_type == "CE" else (strike - p), 0)
                ltp = max(intrinsic + abs(self._rng.gauss(50, 15)), 0.5)
                opt = OptionTick(
                    symbol=f"{underlying}OPT{strike}{opt_type}",
                    underlying=underlying, strike=strike,
                    option_type=opt_type, expiry=expiry,
                    ltp=round(ltp, 2), bid=round(ltp - 0.5, 2),
                    ask=round(ltp + 0.5, 2),
                    oi=self._rng.randint(100_000, 10_000_000),
                    change_oi=self._rng.randint(-100_000, 200_000),
                    volume=self._rng.randint(1_000, 300_000),
                    iv=round(abs(self._rng.gauss(15, 3)), 2),
                    delta=round(0.5 - i * 0.08, 4),
                    timestamp=now,
                )
                await self._publish_option(opt)



# ─────────────────────────────────────────────────────────────────────────────
# DedupBuffer — per-symbol tick deduplication for dual-feed setups
# ─────────────────────────────────────────────────────────────────────────────

class DedupBuffer:
    """
    Tracks the last accepted (monotonic_ts, ltp) per symbol.

    accept() returns True (and updates state) when:
      • first tick for the symbol, OR
      • >= 100 ms elapsed since last accepted tick, OR
      • price moved more than 1e-4 (absolute).

    All other ticks are silently dropped → returns False.
    """

    def __init__(self) -> None:
        self._last: Dict[str, Tuple[float, float]] = {}   # symbol → (ts, ltp)
        # Active-PASSIVE failover: when a primary provider is set, ONLY the primary
        # feeder's ticks drive prices; the secondary is used only when the primary
        # goes stale (down). This prevents two feeds disagreeing on the same
        # contract (the price flip-flop, e.g. 711 vs 365). None → legacy active-active.
        self._primary: Optional[str] = None
        self._stale_sec: float = 3.0
        self._last_primary_ts: float = 0.0

    def set_primary(self, provider: Optional[str], stale_sec: float = 3.0) -> None:
        self._primary = (provider or "").lower() or None
        self._stale_sec = stale_sec
        # Treat the primary as "just ticked" at startup so the secondary is NOT used
        # in the boot window before the primary's first tick (which would capture a
        # stale secondary price at entry). The secondary only takes over after the
        # primary actually goes stale_sec without a tick.
        self._last_primary_ts = time.monotonic()

    def accept(self, symbol: str, ltp: float, provider: Optional[str] = None) -> bool:
        now = time.monotonic()
        # Active-passive gate
        if self._primary is not None and provider is not None:
            if provider.lower() == self._primary:
                self._last_primary_ts = now
            elif (now - self._last_primary_ts) < self._stale_sec:
                return False   # secondary dropped while primary is healthy
        entry = self._last.get(symbol)
        if entry is None:
            self._last[symbol] = (now, ltp)
            return True
        prev_ts, prev_ltp = entry
        if (now - prev_ts) >= 0.100 or abs(ltp - prev_ltp) > 1e-4:
            self._last[symbol] = (now, ltp)
            return True
        return False


# ─────────────────────────────────────────────────────────────────────────────
# UpstoxFeeder — stub for Upstox API v2 WebSocket feed
# ─────────────────────────────────────────────────────────────────────────────

_UPSTOX_INDEX_KEY_TO_INTERNAL: Dict[str, str] = {
    "NSE_INDEX|Nifty 50":        "NIFTY",
    "NSE_INDEX|Nifty Bank":      "BANKNIFTY",
    "NSE_INDEX|Nifty Fin Service": "FINNIFTY",
    "NSE_INDEX|NIFTY MID SELECT": "MIDCPNIFTY",
    "BSE_INDEX|SENSEX":          "SENSEX",
}


class UpstoxFeeder(BaseFeeder):
    """
    Live Upstox API v3 data feeder using MarketDataStreamerV3.

    Streams real-time INDEX_TICK and OPTION_TICK events for all configured
    monitored indices. The WebSocket runs in a thread via asyncio.to_thread;
    the on_message callback uses run_coroutine_threadsafe to safely enqueue
    frames into the asyncio raw queue.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        self._streamer = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed_keys: List[str] = []   # all currently subscribed instrument keys
        try:
            import upstox_client  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    def _index_instrument_keys(self) -> List[str]:
        """
        Upstox instrument keys for monitored instruments. MCX commodities use the
        near-month FUTURES instrument_key from the registry as the ATM source.
        """
        from data_layer.symbol_translator import SymbolTranslator
        from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_UPSTOX_INDEX_KEY_TO_INTERNAL.values())
        )
        keys: List[str] = []
        for i in indices:
            if i.upper() in _MCX_UNDERLYINGS:
                fk = REGISTRY.get_futures_upstox(i.upper())
                if fk:
                    keys.append(fk)
            else:
                keys.append(SymbolTranslator.to_upstox_index(i))
        return keys

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "UpstoxFeeder: upstox_client SDK not installed — "
                "pip install upstox-client.  Feeder will not connect."
            )
            return False
        access_token = self._creds.get("access_token", "")
        if not access_token:
            logger.warning("UpstoxFeeder: no access_token in credentials — cannot connect.")
            return False

        self._loop = asyncio.get_running_loop()

        import upstox_client

        cfg_obj = upstox_client.Configuration()
        cfg_obj.access_token = access_token
        api_client_obj = upstox_client.ApiClient(cfg_obj)

        # Combine index keys + cached option keys into initial subscription list
        index_keys = self._index_instrument_keys()
        all_keys = list(index_keys)
        for k in self._subscribed_keys:
            if k not in all_keys:
                all_keys.append(k)
        self._subscribed_keys = all_keys

        def _on_open() -> None:
            self._connected = True
            logger.info(
                "UpstoxFeeder: WebSocket connected — subscribed to %d keys (%d index, %d option).",
                len(self._subscribed_keys),
                len(index_keys),
                len(self._subscribed_keys) - len(index_keys),
            )

        def _on_message(message: bytes) -> None:
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self._parse_frame(message), self._loop
                )

        def _on_error(error) -> None:
            logger.warning("UpstoxFeeder: WS error: %s", error)

        def _on_close() -> None:
            logger.info("UpstoxFeeder: WebSocket closed.")
            self._connected = False

        self._streamer = upstox_client.MarketDataStreamerV3(
            api_client=api_client_obj,
            instrumentKeys=self._subscribed_keys,
            mode="full",
        )
        self._streamer.on("open", _on_open)
        self._streamer.on("message", _on_message)
        self._streamer.on("error", _on_error)
        self._streamer.on("close", _on_close)

        logger.info("UpstoxFeeder: streamer created — will connect in _ws_loop.")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._streamer:
            try:
                self._streamer.disconnect()
            except Exception:
                pass
            self._streamer = None

    @staticmethod
    def _is_upstox_key(token: str) -> bool:
        """Upstox instrument_keys contain a pipe (e.g. NSE_FO|...). Fyers symbols don't."""
        return "|" in token

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        # In dual mode the rebalancer sends BOTH Upstox + Fyers tokens; take only ours.
        mine = [t for t in tokens if self._is_upstox_key(t)]
        new_keys = [t for t in mine if t not in self._subscribed_keys]
        if not new_keys:
            return
        for k in new_keys:
            self._subscribed_keys.append(k)
        if self._streamer:   # don't gate on possibly-stale _connected flag
            try:
                # SDK signature: subscribe(instrumentKeys, mode='ltpc') — positional.
                # Some SDK builds accept only keys; fall back if mode is rejected.
                try:
                    self._streamer.subscribe(new_keys, "full")
                except TypeError:
                    self._streamer.subscribe(new_keys)
                logger.info("UpstoxFeeder: subscribed %d option keys.", len(new_keys))
            except Exception as exc:
                logger.warning("UpstoxFeeder: subscribe error: %s", exc)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        mine = [t for t in tokens if self._is_upstox_key(t)]
        for t in mine:
            if t in self._subscribed_keys:
                self._subscribed_keys.remove(t)
        if self._streamer and self._connected and mine:
            try:
                self._streamer.unsubscribe(mine)
            except Exception as exc:
                logger.debug("UpstoxFeeder: unsubscribe error: %s", exc)

    async def _ws_loop(self) -> None:
        if not self._streamer:
            return
        self._running = True
        try:
            await asyncio.to_thread(self._streamer.connect)
        except Exception as exc:
            logger.error("UpstoxFeeder: _ws_loop ended with error: %s", exc)
        finally:
            self._connected = False
            self._running = False

    def _get_option_meta(self, inst_key: str):
        """
        Return (underlying, strike, opt_type, expiry) for an Upstox instrument_key,
        using a lazily-built reverse lookup cache. Rebuilds when registry grows.
        """
        from data_layer.instrument_registry import REGISTRY
        from datetime import date as _date

        total = sum(len(v) for v in REGISTRY._upstox_keys.values())
        if not hasattr(self, "_rev_map") or total != getattr(self, "_rev_map_size", -1):
            rev: Dict[str, tuple] = {}
            for underlying, kmap in REGISTRY._upstox_keys.items():
                for (exp_str, strike, opt_type), stored_key in kmap.items():
                    try:
                        expiry = _date.fromisoformat(exp_str)
                    except ValueError:
                        continue
                    rev[stored_key] = (underlying, strike, opt_type, expiry)
            self._rev_map: Dict[str, tuple] = rev
            self._rev_map_size: int = total
        return self._rev_map.get(inst_key)

    @staticmethod
    def _extract_ltp(feed_data) -> Optional[float]:
        """
        Extract LTP from a decoded Upstox feed entry (dict form from MarketDataStreamerV3).
        Handles full mode (fullFeed.marketFF / fullFeed.indexFF) and ltpc mode.
        """
        if not isinstance(feed_data, dict):
            return None
        # Full mode
        ff = feed_data.get("fullFeed") or feed_data.get("ff")
        if isinstance(ff, dict):
            for sub in ("marketFF", "indexFF"):
                blk = ff.get(sub)
                if isinstance(blk, dict):
                    ltp = (blk.get("ltpc") or {}).get("ltp")
                    if ltp:
                        return float(ltp)
        # ltpc mode
        ltpc = feed_data.get("ltpc")
        if isinstance(ltpc, dict) and ltpc.get("ltp"):
            return float(ltpc["ltp"])
        return None

    @staticmethod
    def _extract_extras(feed_data) -> Dict[str, float]:
        """Extract OI, volume, and ATP (broker VWAP) from a full-mode dict feed entry."""
        result: Dict[str, float] = {"oi": 0, "volume": 0, "atp": 0.0}
        if not isinstance(feed_data, dict):
            return result
        ff = feed_data.get("fullFeed") or feed_data.get("ff") or {}
        mff = ff.get("marketFF") if isinstance(ff, dict) else None
        if isinstance(mff, dict):
            try:
                result["oi"] = int(float(mff.get("oi") or 0))
            except (TypeError, ValueError):
                pass
            try:
                result["volume"] = int(float(mff.get("vtt") or 0))
            except (TypeError, ValueError):
                pass
            try:
                # ATP = exchange average traded price = broker VWAP for this contract
                result["atp"] = float((mff.get("eFeedDetails") or {}).get("atp") or mff.get("atp") or 0.0)
            except (TypeError, ValueError):
                pass
        return result

    async def _parse_frame(self, raw: Any) -> None:
        # MarketDataStreamerV3 on("message") delivers an already-decoded dict in
        # recent SDKs; older builds emit raw protobuf bytes. Handle both.
        decoded = raw
        if not isinstance(raw, dict):
            try:
                import upstox_client
                obj = upstox_client.MarketDataStreamerV3.decode_protobuf(raw)
                # Convert protobuf to dict if helper available
                decoded = obj if isinstance(obj, dict) else getattr(obj, "__dict__", {}) or {}
            except Exception as exc:
                if not hasattr(self, "_logged_raw_type"):
                    self._logged_raw_type = True
                    logger.warning("UpstoxFeeder: cannot decode message type=%s err=%s sample=%r",
                                   type(raw).__name__, exc, str(raw)[:200])
                return

        if not hasattr(self, "_logged_raw_type"):
            self._logged_raw_type = True
            logger.info("UpstoxFeeder: first raw message type=%s keys=%s",
                        type(raw).__name__,
                        list(decoded.keys())[:6] if isinstance(decoded, dict) else "n/a")

        feeds = decoded.get("feeds") if isinstance(decoded, dict) else None
        if not isinstance(feeds, dict):
            return

        if not hasattr(self, "_logged_first_tick"):
            self._logged_first_tick = True
            logger.info("UpstoxFeeder: first decoded frame keys sample: %s", list(feeds.keys())[:3])

        now = datetime.now(IST)

        for inst_key, feed_data in feeds.items():
            ltp = self._extract_ltp(feed_data)
            if ltp is None or ltp == 0.0:
                continue

            # ── Index tick ──────────────────────────────────────────────────
            internal_name = _UPSTOX_INDEX_KEY_TO_INTERNAL.get(inst_key) or _mcx_upstox_fut_to_internal(inst_key)
            if internal_name:
                tick = IndexTick(
                    symbol=internal_name,
                    ltp=ltp,
                    open=ltp, high=ltp, low=ltp, close=ltp,
                    volume=0,
                    timestamp=now,
                )
                await self._publish_index(tick)
                continue

            # ── Option tick — look up via cached reverse map ──────────────
            meta = self._get_option_meta(inst_key)
            if meta:
                underlying, strike, opt_type, expiry = meta
                extras = self._extract_extras(feed_data)
                opt_tick = OptionTick(
                    symbol=inst_key,
                    underlying=underlying,
                    strike=float(strike),
                    option_type=opt_type,
                    expiry=expiry,
                    ltp=ltp,
                    bid=ltp,
                    ask=ltp,
                    oi=extras["oi"],
                    change_oi=0,
                    volume=extras["volume"],
                    iv=0.0,
                    delta=0.0,
                    timestamp=now,
                    atp=float(extras.get("atp") or 0.0),  # broker VWAP
                )
                await self._publish_option(opt_tick)


# ─────────────────────────────────────────────────────────────────────────────
# FyersFeeder — stub for Fyers API v3 WebSocket feed
# ─────────────────────────────────────────────────────────────────────────────

_FYERS_INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY":      "NSE:NIFTY50-INDEX",
    "BANKNIFTY":  "NSE:NIFTYBANK-INDEX",
    "FINNIFTY":   "NSE:FINNIFTY-INDEX",
    "SENSEX":     "BSE:SENSEX-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
}
_FYERS_TO_INTERNAL: Dict[str, str] = {v: k for k, v in _FYERS_INDEX_SYMBOLS.items()}


def _mcx_fyers_fut_to_internal(symbol: str) -> Optional[str]:
    """Map an MCX futures Fyers symbol (e.g. MCX:CRUDEOIL26JUNFUT) -> 'CRUDEOIL'."""
    from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
    for u in _MCX_UNDERLYINGS:
        if symbol and REGISTRY.get_futures_fyers(u) == symbol:
            return u
    return None


def _mcx_upstox_fut_to_internal(ikey: str) -> Optional[str]:
    """Map an MCX futures Upstox key (e.g. MCX_FO|499095) -> 'CRUDEOIL'."""
    from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
    for u in _MCX_UNDERLYINGS:
        if ikey and REGISTRY.get_futures_upstox(u) == ikey:
            return u
    return None


import re as _re
_MCX_FY_OPT_RE = _re.compile(r"^MCX:([A-Z]+?)(\d{2})([A-Z]{3})(\d+)(CE|PE)$")


def _parse_mcx_fyers_option(symbol: str):
    """Parse 'MCX:CRUDEOIL26JUN8850CE' -> (underlying, strike, opt_type, expiry)."""
    m = _MCX_FY_OPT_RE.match(symbol or "")
    if not m:
        return None
    underlying, _yy, _mon, strike, ot = m.groups()
    from data_layer.instrument_registry import REGISTRY
    exp = REGISTRY.get_active_expiry(underlying)
    return (underlying, float(strike), ot, exp)


class FyersFeeder(BaseFeeder):
    """
    Live Fyers API v3 data feeder using FyersDataSocket.

    Streams real-time INDEX_TICK events for all configured monitored indices.
    The WebSocket runs in a thread via asyncio.to_thread; the on_message callback
    uses call_soon_threadsafe to safely enqueue frames into the asyncio raw queue.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        self._socket = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed_tokens: List[str] = []  # option tokens to re-subscribe on reconnect
        try:
            import fyers_apiv3  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "FyersFeeder: fyers_apiv3 SDK not installed — "
                "pip install fyers-apiv3.  Feeder will not connect."
            )
            return False
        access_token = self._creds.get("access_token", "")
        if not access_token:
            logger.warning("FyersFeeder: no access_token in credentials — cannot connect.")
            return False

        self._loop = asyncio.get_running_loop()

        from fyers_apiv3.FyersWebsocket import data_ws

        def _on_message(msg: dict) -> None:
            # Use run_coroutine_threadsafe so _parse_frame runs even if _parse_task is cancelled
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._parse_frame(msg), self._loop)

        def _on_error(msg: dict) -> None:
            logger.warning("FyersFeeder: WS error: %s", msg)

        def _on_connect() -> None:
            # Subscribe all symbols from WS thread on every connect/reconnect
            self._connected = True
            symbols = self._index_symbols()
            all_symbols = list(symbols) + list(self._subscribed_tokens)
            if all_symbols and self._socket:
                self._socket.subscribe(symbols=all_symbols, data_type="SymbolUpdate")
                logger.info("FyersFeeder: connected and subscribed — %d index + %d option tokens",
                            len(symbols), len(self._subscribed_tokens))

        def _on_close(msg: dict) -> None:
            logger.info("FyersFeeder: WebSocket closed: %s", msg)
            self._connected = False

        self._socket = data_ws.FyersDataSocket(
            access_token=access_token,
            write_to_file=False,
            litemode=False,  # full mode needed for continuous option tick streaming
            reconnect=True,
            on_message=_on_message,
            on_error=_on_error,
            on_connect=_on_connect,
            on_close=_on_close,
            reconnect_retry=5,
        )
        logger.info("FyersFeeder: socket created — will connect in _ws_loop.")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._socket:
            try:
                self._socket.close_connection()
            except Exception:
                pass
            self._socket = None

    def _index_symbols(self) -> List[str]:
        """
        Fyers-format 'index' symbols for all monitored instruments. For MCX
        commodities (CRUDEOIL) the ATM source is the near-month FUTURES symbol
        from the registry (e.g. MCX:CRUDEOIL26JUNFUT), not a spot index.
        """
        from data_layer.instrument_registry import REGISTRY, _MCX_UNDERLYINGS
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_FYERS_INDEX_SYMBOLS.keys())
        )
        syms: List[str] = []
        for i in indices:
            if i.upper() in _MCX_UNDERLYINGS:
                fut = REGISTRY.get_futures_fyers(i.upper())
                if fut:
                    syms.append(fut)
            elif i in _FYERS_INDEX_SYMBOLS:
                syms.append(_FYERS_INDEX_SYMBOLS[i])
        return syms

    @staticmethod
    def _is_fyers_symbol(token: str) -> bool:
        """
        Fyers symbols start with an exchange prefix: NSE:NIFTY... / BSE:SENSEX...
        / MCX:CRUDEOIL... (commodities). Excludes the internal canonical format
        (NIFTY:02JUN26:...) which has no exchange prefix, and Upstox keys (...|...).
        """
        return token.startswith(("NSE:", "BSE:", "MCX:")) and "|" not in token

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        # In dual mode the rebalancer sends BOTH Upstox + Fyers tokens; take only ours.
        mine = [t for t in tokens if self._is_fyers_symbol(t)]
        # Diagnostic: reveal received vs matched so we can see why options may be 0.
        logger.info(
            "FyersFeeder.subscribe_tokens: received=%d matched_fyers=%d connected=%s sample_in=%r sample_mine=%r",
            len(tokens), len(mine), self._connected,
            tokens[:2], mine[:2],
        )
        # Remember tokens so they are re-subscribed on every reconnect
        for t in mine:
            if t not in self._subscribed_tokens:
                self._subscribed_tokens.append(t)
        # Subscribe whenever the socket exists — do NOT gate on the _connected
        # flag, which can be stale (e.g. options arrive after _on_connect already
        # fired, or during DualFeeder churn) and would silently drop the tokens.
        # The FyersDataSocket dedups duplicate subscriptions, so this is safe.
        if self._socket and mine:
            try:
                self._socket.subscribe(symbols=mine, data_type="SymbolUpdate")
                logger.info("FyersFeeder: subscribed to %d option tokens (connected=%s).",
                            len(mine), self._connected)
            except Exception as exc:
                logger.warning("FyersFeeder: subscribe_tokens error: %s", exc)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        mine = [t for t in tokens if self._is_fyers_symbol(t)]
        for t in mine:
            if t in self._subscribed_tokens:
                self._subscribed_tokens.remove(t)
        if self._socket and self._connected and mine:
            try:
                self._socket.unsubscribe(symbols=mine, data_type="SymbolUpdate")
            except Exception as exc:
                logger.debug("FyersFeeder: unsubscribe_tokens error: %s", exc)

    async def _ws_loop(self) -> None:
        if not self._socket:
            return
        self._running = True
        # socket.connect() is a blocking call that runs until closed
        try:
            await asyncio.to_thread(self._socket.connect)
        except Exception as exc:
            logger.error("FyersFeeder: _ws_loop ended with error: %s", exc)
        finally:
            self._connected = False
            self._running = False

    def set_latency_tracker(self, provider: str, latency_dict: Dict[str, float]) -> None:
        """Called by DualFeeder so this feeder can record its own tick latency."""
        self._latency_provider = provider
        self._latency_dict = latency_dict

    async def _parse_frame(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            logger.info("FyersFeeder: raw frame is not dict — type=%s val=%r", type(raw).__name__, str(raw)[:200])
            return
        symbol_fyers = raw.get("symbol", "")
        ltp = raw.get("ltp")
        if not symbol_fyers or ltp is None:
            return
        if not hasattr(self, "_logged_first_tick"):
            self._logged_first_tick = True
            logger.info("FyersFeeder: first TICK frame keys=%s sample=%r", list(raw.keys()), str(raw)[:400])

        internal = _FYERS_TO_INTERNAL.get(symbol_fyers) or _mcx_fyers_fut_to_internal(symbol_fyers)
        if internal:
            t0 = time.monotonic()
            tick = IndexTick(
                symbol=internal,
                ltp=float(ltp),
                open=float(raw.get("open_price") or ltp),
                high=float(raw.get("high_price") or ltp),
                low=float(raw.get("low_price")  or ltp),
                close=float(raw.get("prev_close_price") or ltp),
                volume=int(raw.get("vol_traded_today") or 0),
                timestamp=datetime.now(IST),
            )
            await self._publish_index(tick)
            if hasattr(self, "_latency_dict"):
                self._latency_dict[self._latency_provider] = (time.monotonic() - t0) * 1000.0
        else:
            # Option tick — parse Fyers symbol (NSE or MCX) and publish OptionTick
            try:
                from data_layer.symbol_translator import SymbolTranslator
                _u = _s = _ot = _exp = None
                sym = SymbolTranslator.from_fyers(symbol_fyers)
                if sym is not None:
                    _u, _s, _ot, _exp = sym.underlying, sym.strike, sym.option_type, sym.expiry
                else:
                    mcx = _parse_mcx_fyers_option(symbol_fyers)
                    if mcx is not None:
                        _u, _s, _ot, _exp = mcx
                if _u is not None and _exp is not None:
                    from data_layer.base_feeder import OptionTick
                    opt_tick = OptionTick(
                        symbol      = symbol_fyers,
                        underlying  = _u,
                        strike      = _s,
                        option_type = _ot,
                        expiry      = _exp,
                        ltp         = float(ltp),
                        bid         = float(raw.get("bid_price") or ltp),
                        ask         = float(raw.get("ask_price") or ltp),
                        oi          = int(raw.get("oi") or 0),
                        change_oi   = int(raw.get("chng_oi") or 0),
                        volume      = int(raw.get("vol_traded_today") or 0),
                        iv          = float(raw.get("iv") or 0.0),
                        delta       = 0.0,
                        timestamp   = datetime.now(IST),
                        atp         = float(raw.get("avg_trade_price") or 0.0),  # broker VWAP
                    )
                    await self._publish_option(opt_tick)
            except Exception as _exc:
                logger.debug("FyersFeeder: option tick parse error for %s: %s", symbol_fyers, _exc)


# ─────────────────────────────────────────────────────────────────────────────
# DualFeeder — concurrent active-active dual-provider feed manager
# ─────────────────────────────────────────────────────────────────────────────

class DualFeeder:
    """
    Manages Upstox + Fyers feeders concurrently (active-active).

    Each provider runs in its own asyncio Task. A crash in one does NOT
    affect the other. Exponential back-off reconnect is per-provider.
    All ticks pass through DedupBuffer before being published so
    duplicate ticks from the trailing provider are silently discarded.
    Per-provider latency (ms) is tracked in _latency.
    """

    MAX_RECONNECT_DELAY = 60

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._running = False
        self._dedup = DedupBuffer()
        self._latency: Dict[str, float] = {}
        self._tasks: List[asyncio.Task] = []
        self._feeders: Dict[str, BaseFeeder] = {}

    async def start(self, upstox_creds: Dict[str, str], fyers_creds: Dict[str, str]) -> None:
        self._running = True
        upstox = UpstoxFeeder(self._bus, self._cfg)
        upstox.set_credentials(upstox_creds)
        fyers = FyersFeeder(self._bus, self._cfg)
        fyers.set_credentials(fyers_creds)

        # Active-PASSIVE: the primary provider drives all prices; the secondary is
        # used only when the primary goes stale (down). Avoids the two feeds
        # disagreeing on a contract (price flip-flop). Primary from config
        # (primary_feeder_provider), default upstox.
        _primary = (getattr(self._cfg, "primary_feeder_provider", "upstox") or "upstox").lower()
        if _primary not in ("upstox", "fyers"):
            _primary = "upstox"
        self._dedup.set_primary(_primary, float(getattr(self._cfg, "feeder_failover_stale_sec", 3.0)))
        logger.info("DualFeeder: active-passive — primary=%s (secondary used only when primary stale).", _primary)

        for provider, feeder in (("upstox", upstox), ("fyers", fyers)):
            feeder.set_provider_name(provider)
            if hasattr(feeder, "set_latency_tracker"):
                feeder.set_latency_tracker(provider, self._latency)
            # Shared gate across both feeders → active-passive failover.
            feeder.set_dedup_buffer(self._dedup)
            try:
                ok = await feeder.connect()
            except Exception as exc:
                logger.warning("DualFeeder: %s connect raised: %s — continuing.", provider, exc)
                ok = False
            if ok:
                self._feeders[provider] = feeder
                task = asyncio.create_task(
                    self._run_stream(provider, feeder),
                    name=f"dual_feeder_{provider}",
                )
                self._tasks.append(task)
                logger.info("DualFeeder: %s stream task started.", provider)
            else:
                logger.warning("DualFeeder: %s failed to connect — stream not started.", provider)

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._tasks.clear()
        for provider, feeder in self._feeders.items():
            try:
                feeder.stop()
                await feeder.disconnect()
            except Exception as exc:
                logger.debug("DualFeeder: %s disconnect raised: %s", provider, exc)
        self._feeders.clear()
        logger.info("DualFeeder: stopped.")

    async def _run_stream(self, provider: str, feeder: BaseFeeder) -> None:
        reconnect_attempts = 0
        while self._running:
            try:
                await feeder.run()
                break  # clean exit
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = min(2 ** reconnect_attempts, self.MAX_RECONNECT_DELAY)
                logger.error(
                    "DualFeeder: %s stream error: %s — reconnecting in %.0fs (attempt %d).",
                    provider, exc, delay, reconnect_attempts + 1,
                )
                await asyncio.sleep(delay)
                reconnect_attempts += 1
                try:
                    await feeder.disconnect()
                    ok = await feeder.connect()
                except Exception as reconnect_exc:
                    logger.warning("DualFeeder: %s reconnect raised: %s", provider, reconnect_exc)
                    ok = False
                if ok:
                    reconnect_attempts = 0
                    logger.info("DualFeeder: %s reconnected successfully.", provider)

    async def _wrap_publish_index(self, provider: str, feeder: BaseFeeder, tick: IndexTick) -> None:
        t0 = time.monotonic()
        if self._dedup.accept(tick.symbol, tick.ltp):
            await feeder._publish_index(tick)
            self._latency[provider] = (time.monotonic() - t0) * 1000.0

    @property
    def is_running(self) -> bool:
        return self._running and any(f.is_connected for f in self._feeders.values())

    @property
    def latency(self) -> Dict[str, float]:
        return dict(self._latency)

    @property
    def provider_connected(self) -> Dict[str, bool]:
        """Returns per-provider connected state based on active feeder tasks."""
        return {p: f.is_connected for p, f in self._feeders.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Feeder Registry — maps provider string → feeder class
# ─────────────────────────────────────────────────────────────────────────────

def _load_shared_client():
    """Lazy import so shared_feed_client.py doesn't create a circular dep at module load."""
    from data_layer.shared_feed_client import SharedFeedClient
    return SharedFeedClient


_FEEDER_REGISTRY: Dict[str, type] = {
    "mock":    MockFeeder,
    "upstox":  UpstoxFeeder,
    "fyers":   FyersFeeder,
    "shared":  None,   # populated on first access via register_feeder("shared", ...)
    # "shoonya": ShoonyaFeeder,
    # "dhan": DhanFeeder,
    # "angelone": AngelOneFeeder,
}


def register_feeder(provider: str, cls: type) -> None:
    """Called by broker feeder modules to self-register."""
    _FEEDER_REGISTRY[provider.lower()] = cls


# Auto-register SharedFeedClient for "shared" provider
try:
    from data_layer.shared_feed_client import SharedFeedClient as _SFC
    _FEEDER_REGISTRY["shared"] = _SFC
except ImportError:
    pass


# ─────────────────────────────────────────────────────────────────────────────
# GlobalFeeder — admin-managed wrapper with heartbeat + auto-reconnect
# ─────────────────────────────────────────────────────────────────────────────

class GlobalFeeder:
    """
    Lifecycle wrapper around one BaseFeeder instance.

    AdminConsole creates one GlobalFeeder, configures the provider,
    calls start() at 09:00 IST, and stop() at 15:30 IST.

    The heartbeat task checks connection health every 30 s and
    triggers a reconnect cycle if the feeder goes silent.
    """

    HEARTBEAT_INTERVAL = 30         # seconds
    MAX_RECONNECT_DELAY = 60        # seconds cap for exponential backoff

    def __init__(self, bus: EventBus, cfg: GlobalConfig, client_db=None) -> None:
        self._bus = bus
        self._cfg = cfg
        self._client_db = client_db   # Optional[ClientDB]; None in demo/paper mode
        self._feeder: Optional[BaseFeeder] = None
        self._feeder_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._tick_listener_task: Optional[asyncio.Task] = None
        self._candle_persist_task: Optional[asyncio.Task] = None
        self._last_tick_ts: float = 0.0
        self._reconnect_delay: float = 2.0
        self._running = False
        self._dual_feeder: Optional[DualFeeder] = None
        self._active_provider: str = "mock"
        self._cached_tokens: List[str] = []  # option tokens to re-apply on every reconnect

    async def start(self) -> None:
        """Create feeder, connect, and launch the run + heartbeat tasks."""
        self._running = True
        provider = self._cfg.primary_feeder_provider.lower()
        cls = _FEEDER_REGISTRY.get(provider)
        if cls is None:
            raise ValueError(f"GlobalFeeder: Unknown provider '{provider}'. Available: {list(_FEEDER_REGISTRY)}")

        # Pass cfg only if the constructor accepts it (MockFeeder does; others may vary)
        try:
            self._feeder = cls(self._bus, self._cfg)
        except TypeError:
            self._feeder = cls(self._bus)

        if not await self._feeder.connect():
            raise ConnectionError(f"GlobalFeeder: Failed to connect via '{provider}'.")

        self._last_tick_ts = time.monotonic()
        self._feeder_task = asyncio.create_task(self._run_feeder(), name="global_feeder_run")
        self._heartbeat_task = asyncio.create_task(self._heartbeat(), name="global_feeder_hb")
        self._tick_listener_task = asyncio.create_task(self._tick_listener(), name="global_feeder_tick_listener")
        if self._client_db is not None:
            self._candle_persist_task = asyncio.create_task(
                self._candle_persist_loop(), name="candle_persist_1m"
            )

        self._active_provider = provider
        await self._bus.publish(Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_RESTORED, provider))
        logger.info("GlobalFeeder: Started with provider='%s'.", provider)

    async def stop(self) -> None:
        self._running = False
        if self._feeder:
            self._feeder.stop()
            await self._feeder.disconnect()
        for task in (self._feeder_task, self._heartbeat_task, self._tick_listener_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        if self._candle_persist_task and not self._candle_persist_task.done():
            self._candle_persist_task.cancel()
            try:
                await self._candle_persist_task
            except asyncio.CancelledError:
                pass
        logger.info("GlobalFeeder: Stopped.")

    async def subscribe_tokens(self, tokens: list) -> None:
        """Proxy to active feeder — DualFeeder takes priority over initial feeder."""
        for t in tokens:
            if t not in self._cached_tokens:
                self._cached_tokens.append(t)
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.subscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: list) -> None:
        """Proxy to active feeder — DualFeeder takes priority over initial feeder."""
        for t in tokens:
            if t in self._cached_tokens:
                self._cached_tokens.remove(t)
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.unsubscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.unsubscribe_tokens(tokens)

    async def _reapply_cached_tokens(self) -> None:
        """Re-subscribe cached option tokens after a DualFeeder reconnect."""
        if not self._cached_tokens:
            return
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.subscribe_tokens(self._cached_tokens)
            logger.info("GlobalFeeder: re-applied %d cached option tokens after reconnect.",
                        len(self._cached_tokens))

    async def _stop_initial_feeder(self) -> None:
        """Stop and discard the initial (mock) feeder when switching to a real provider."""
        if self._feeder is not None:
            try:
                self._feeder.stop()
                await self._feeder.disconnect()
            except Exception as exc:
                logger.debug("GlobalFeeder: initial feeder stop raised: %s", exc)
            self._feeder = None
        for task in (self._feeder_task, self._heartbeat_task, self._tick_listener_task):
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        self._feeder_task = None
        self._heartbeat_task = None
        self._tick_listener_task = None
        logger.info("GlobalFeeder: initial feeder stopped — switching to live provider.")

    async def start_dual(self, upstox_creds: Dict[str, str], fyers_creds: Dict[str, str]) -> None:
        """Bootstrap active-active DualFeeder. Stops MockFeeder and any prior DualFeeder."""
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        await self._stop_initial_feeder()
        dual = DualFeeder(self._bus, self._cfg)
        await dual.start(upstox_creds, fyers_creds)
        self._dual_feeder = dual
        self._active_provider = "dual"
        await self._bus.publish(
            Topic.SYSTEM_EVENT,
            SystemEvent(SysEvent.FEEDER_RESTORED, "dual_active_active"),
        )
        logger.info("GlobalFeeder: DualFeeder (active-active) started.")
        await self._reapply_cached_tokens()

    async def start_single(self, provider: str, creds: Dict[str, str]) -> None:
        """Bootstrap single-provider DualFeeder. Stops MockFeeder and any prior DualFeeder."""
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        await self._stop_initial_feeder()
        dual = DualFeeder(self._bus, self._cfg)
        upstox_creds = creds if provider == "upstox" else {}
        fyers_creds  = creds if provider == "fyers"  else {}
        await dual.start(upstox_creds, fyers_creds)
        self._dual_feeder = dual
        self._active_provider = provider
        await self._bus.publish(
            Topic.SYSTEM_EVENT,
            SystemEvent(SysEvent.FEEDER_RESTORED, f"single_{provider}"),
        )
        logger.info("GlobalFeeder: single-provider '%s' feeder started.", provider)
        await self._reapply_cached_tokens()

    @property
    def dual_latency(self) -> Dict[str, float]:
        """Per-provider latency dict from the DualFeeder, or {} if inactive."""
        return self._dual_feeder.latency if self._dual_feeder is not None else {}

    async def _run_feeder(self) -> None:
        while self._running:
            try:
                if self._feeder:
                    await self._feeder.run()
            except Exception as exc:
                logger.error("GlobalFeeder: Feeder crashed: %s. Reconnecting in %.0fs.", exc, self._reconnect_delay)
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self.MAX_RECONNECT_DELAY)
                await self._reconnect()
            else:
                break    # Clean exit

    async def _reconnect(self) -> None:
        if self._feeder:
            try:
                await self._feeder.disconnect()
            except Exception:
                pass
            if not await self._feeder.connect():
                logger.error("GlobalFeeder: Reconnect failed.")
                await self._bus.publish(
                    Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_DOWN, "reconnect_failed")
                )
            else:
                self._reconnect_delay = 2.0
                self._last_tick_ts = time.monotonic()
                # disconnect() set _running=False which exits _ws_loop → run() returns → task done.
                # Must restart the feeder run loop so ticks resume.
                if not self._feeder_task or self._feeder_task.done():
                    self._feeder_task = asyncio.create_task(
                        self._run_feeder(), name="global_feeder_run"
                    )
                logger.info("GlobalFeeder: Reconnected.")
                await self._bus.publish(
                    Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_RESTORED, "reconnect_ok")
                )

    async def _heartbeat(self) -> None:
        """
        Detect silent feed (no ticks for > HEARTBEAT_INTERVAL seconds)
        and trigger a reconnect.
        """
        while self._running:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            silence = time.monotonic() - self._last_tick_ts
            if silence > self.HEARTBEAT_INTERVAL * 2:
                logger.warning("GlobalFeeder: No ticks for %.0f seconds — triggering reconnect.", silence)
                await self._reconnect()

    async def _tick_listener(self) -> None:
        """
        Subscribes to INDEX_TICK and refreshes _last_tick_ts on every arrival.
        This is the only correct way to drive the heartbeat — avoids the need
        for any external caller to invoke record_tick().
        """
        q = self._bus.subscribe(Topic.INDEX_TICK)
        while self._running:
            try:
                await asyncio.wait_for(q.get(), timeout=1.0)
                self._last_tick_ts = time.monotonic()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @property
    def active_provider(self) -> str:
        """Currently active provider name: 'mock', 'upstox', 'fyers', or 'dual'."""
        return self._active_provider

    @property
    def is_running(self) -> bool:
        primary_ok = self._running and (self._feeder is not None and self._feeder.is_connected)
        dual_ok = self._dual_feeder is not None and self._dual_feeder.is_running
        return primary_ok or dual_ok

    async def _candle_persist_loop(self) -> None:
        """Persist every 1-minute CandleEvent to option_1m_bar_repository."""
        q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        try:
            while self._running:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(ev, CandleEvent):
                    continue
                if ev.timeframe != 1:
                    continue
                try:
                    await self._client_db.upsert_1m_bar(
                        symbol    = ev.symbol,
                        timestamp = ev.timestamp,
                        open_     = ev.open,
                        high      = ev.high,
                        low       = ev.low,
                        close     = ev.close,
                        volume    = float(ev.volume) if ev.volume else 0.0,
                    )
                except Exception as exc:
                    logger.warning("1m bar persist failed [%s]: %s", ev.symbol, exc)
        finally:
            try:
                self._bus._subs[Topic.CANDLE_CLOSE].remove(q)
            except (ValueError, KeyError, AttributeError):
                pass
