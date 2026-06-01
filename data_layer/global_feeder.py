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

    def accept(self, symbol: str, ltp: float) -> bool:
        now = time.monotonic()
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

class UpstoxFeeder(BaseFeeder):
    """
    Stub feeder for Upstox API v2.

    If the `upstox_client` SDK is not installed, connect() logs a warning and
    returns False. _ws_loop idles — replace with real SDK WebSocket wiring when
    the SDK is available.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
        try:
            import upstox_client  # noqa: F401
            self._sdk_available = True
        except ImportError:
            self._sdk_available = False

    def set_credentials(self, creds: Dict[str, str]) -> None:
        self._creds = creds

    async def connect(self) -> bool:
        if not self._sdk_available:
            logger.warning(
                "UpstoxFeeder: upstox_client SDK not installed — "
                "pip install upstox-client.  Feeder will not connect."
            )
            return False
        self._connected = True
        logger.info("UpstoxFeeder: connected (stub).")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        logger.debug("UpstoxFeeder: subscribe_tokens %d tokens (stub).", len(tokens))

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        pass

    async def _ws_loop(self) -> None:
        while self._running:
            await asyncio.sleep(1)

    async def _parse_frame(self, raw: Any) -> None:
        pass


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
            # After "Full Mode On", subscribe via a separate thread (deferred)
            # so we don't call socket.subscribe() re-entrantly from within the callback
            if isinstance(msg, dict) and msg.get("type") == "ful" and msg.get("code") == 200:
                import threading
                symbols = list(self._index_symbols())
                sock = self._socket
                loop = self._loop
                def _do_subscribe():
                    import time as _time
                    _time.sleep(0.3)   # let _on_message return first
                    if sock:
                        sock.subscribe(symbols=symbols, data_type="SymbolUpdate")
                    if loop and not loop.is_closed():
                        loop.call_soon_threadsafe(
                            lambda: logger.info("FyersFeeder: subscribed after Full Mode On — %s", symbols)
                        )
                threading.Thread(target=_do_subscribe, daemon=True).start()
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._enqueue_raw, msg)

        def _on_error(msg: dict) -> None:
            logger.warning("FyersFeeder: WS error: %s", msg)

        def _on_connect() -> None:
            logger.info("FyersFeeder: WebSocket authenticated — waiting for Full Mode On before subscribing.")
            self._connected = True

        def _on_close(msg: dict) -> None:
            logger.info("FyersFeeder: WebSocket closed: %s", msg)
            self._connected = False

        self._socket = data_ws.FyersDataSocket(
            access_token=access_token,
            write_to_file=False,
            litemode=False,
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
        """Return Fyers-format symbols for all configured monitored indices."""
        indices = (
            self._cfg.monitored_indices
            if self._cfg and hasattr(self._cfg, "monitored_indices")
            else list(_FYERS_INDEX_SYMBOLS.keys())
        )
        return [_FYERS_INDEX_SYMBOLS[i] for i in indices if i in _FYERS_INDEX_SYMBOLS]

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        if self._socket and self._connected:
            # tokens are Fyers-format strings from the rebalancer
            try:
                self._socket.subscribe(symbols=tokens, data_type="SymbolUpdate")
                logger.debug("FyersFeeder: subscribed to %d tokens.", len(tokens))
            except Exception as exc:
                logger.warning("FyersFeeder: subscribe_tokens error: %s", exc)

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        if self._socket and self._connected:
            try:
                self._socket.unsubscribe(symbols=tokens, data_type="SymbolUpdate")
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

        internal = _FYERS_TO_INTERNAL.get(symbol_fyers)
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
            # Option tick — parse Fyers symbol and publish OptionTick
            try:
                from data_layer.symbol_translator import SymbolTranslator
                sym = SymbolTranslator.from_fyers(symbol_fyers)
                if sym is not None:
                    from data_layer.base_feeder import OptionTick
                    from datetime import date as _date
                    opt_tick = OptionTick(
                        symbol      = symbol_fyers,
                        underlying  = sym.underlying,
                        strike      = sym.strike,
                        option_type = sym.option_type,
                        expiry      = sym.expiry,
                        ltp         = float(ltp),
                        bid         = float(raw.get("bid_price") or ltp),
                        ask         = float(raw.get("ask_price") or ltp),
                        oi          = int(raw.get("oi") or 0),
                        change_oi   = int(raw.get("chng_oi") or 0),
                        volume      = int(raw.get("vol_traded_today") or 0),
                        iv          = float(raw.get("iv") or 0.0),
                        delta       = 0.0,
                        timestamp   = datetime.now(IST),
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

        for provider, feeder in (("upstox", upstox), ("fyers", fyers)):
            if hasattr(feeder, "set_latency_tracker"):
                feeder.set_latency_tracker(provider, self._latency)
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
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.subscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: list) -> None:
        """Proxy to active feeder — DualFeeder takes priority over initial feeder."""
        if self._dual_feeder is not None:
            for feeder in self._dual_feeder._feeders.values():
                await feeder.unsubscribe_tokens(tokens)
        elif self._feeder is not None:
            await self._feeder.unsubscribe_tokens(tokens)

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
