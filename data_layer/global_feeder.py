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
    BaseFeeder, EventBus, IndexTick, OptionTick, SystemEvent,
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
        while self._running:
            now = datetime.now(IST)
            expiry = self._next_thursday()

            for underlying in self._cfg.monitored_indices:
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

    @staticmethod
    def _next_thursday() -> date:
        today = datetime.now(IST).date()
        days = (3 - today.weekday()) % 7 or 7
        return today + timedelta(days=days)


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

class FyersFeeder(BaseFeeder):
    """
    Stub feeder for Fyers API v3.

    If `fyers_apiv3` is not installed, connect() logs a warning and returns
    False. _ws_loop idles — replace with real FyersDataSocket wiring when
    the SDK is available.
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig = None) -> None:  # type: ignore[assignment]
        super().__init__(bus)
        self._cfg = cfg
        self._creds: Dict[str, str] = {}
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
        self._connected = True
        logger.info("FyersFeeder: connected (stub).")
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        logger.debug("FyersFeeder: subscribe_tokens %d tokens (stub).", len(tokens))

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        pass

    async def _ws_loop(self) -> None:
        while self._running:
            await asyncio.sleep(1)

    async def _parse_frame(self, raw: Any) -> None:
        pass


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


# ─────────────────────────────────────────────────────────────────────────────
# Feeder Registry — maps provider string → feeder class
# ─────────────────────────────────────────────────────────────────────────────

_FEEDER_REGISTRY: Dict[str, type] = {
    "mock":   MockFeeder,
    "upstox": UpstoxFeeder,
    "fyers":  FyersFeeder,
    # "shoonya": ShoonyaFeeder,
    # "dhan": DhanFeeder,
    # "angelone": AngelOneFeeder,
}


def register_feeder(provider: str, cls: type) -> None:
    """Called by broker feeder modules to self-register."""
    _FEEDER_REGISTRY[provider.lower()] = cls


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

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._feeder: Optional[BaseFeeder] = None
        self._feeder_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._tick_listener_task: Optional[asyncio.Task] = None
        self._last_tick_ts: float = 0.0
        self._reconnect_delay: float = 2.0
        self._running = False
        self._dual_feeder: Optional[DualFeeder] = None

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
        logger.info("GlobalFeeder: Stopped.")

    async def subscribe_tokens(self, tokens: list) -> None:
        """Proxy token subscription to the active inner feeder."""
        if self._feeder is not None:
            await self._feeder.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: list) -> None:
        """Proxy token unsubscription to the active inner feeder."""
        if self._feeder is not None:
            await self._feeder.unsubscribe_tokens(tokens)

    async def start_dual(self, upstox_creds: Dict[str, str], fyers_creds: Dict[str, str]) -> None:
        """
        Bootstrap an active-active DualFeeder. Stops any existing DualFeeder first.
        The single-provider feeder continues running alongside.
        """
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        dual = DualFeeder(self._bus, self._cfg)
        await dual.start(upstox_creds, fyers_creds)
        self._dual_feeder = dual
        await self._bus.publish(
            Topic.SYSTEM_EVENT,
            SystemEvent(SysEvent.FEEDER_RESTORED, "dual_active_active"),
        )
        logger.info("GlobalFeeder: DualFeeder (active-active) started.")

    async def start_single(self, provider: str, creds: Dict[str, str]) -> None:
        """
        Bootstrap a single-provider stream via DualFeeder.
        The other provider slot receives empty creds and idles gracefully.
        Replaces any existing DualFeeder instance.
        """
        if self._dual_feeder is not None:
            await self._dual_feeder.stop()
            self._dual_feeder = None
        dual = DualFeeder(self._bus, self._cfg)
        upstox_creds = creds if provider == "upstox" else {}
        fyers_creds  = creds if provider == "fyers"  else {}
        await dual.start(upstox_creds, fyers_creds)
        self._dual_feeder = dual
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
        from config.global_config import Topic
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
    def is_running(self) -> bool:
        return self._running and (self._feeder is not None and self._feeder.is_connected)
