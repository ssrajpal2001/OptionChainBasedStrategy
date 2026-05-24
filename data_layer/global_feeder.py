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
from typing import Dict, List, Optional, Tuple

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
# Feeder Registry — maps provider string → feeder class
# ─────────────────────────────────────────────────────────────────────────────

_FEEDER_REGISTRY: Dict[str, type] = {
    "mock": MockFeeder,
    # "shoonya": ShoonyaFeeder,   ← registered when broker module is imported
    # "dhan": DhanFeeder,
    # "fyers": FyersFeeder,
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
        self._last_tick_ts: float = 0.0
        self._reconnect_delay: float = 2.0
        self._running = False

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

        await self._bus.publish(Topic.SYSTEM_EVENT, SystemEvent(SysEvent.FEEDER_RESTORED, provider))
        logger.info("GlobalFeeder: Started with provider='%s'.", provider)

    async def stop(self) -> None:
        self._running = False
        if self._feeder:
            self._feeder.stop()
            await self._feeder.disconnect()
        for task in (self._feeder_task, self._heartbeat_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("GlobalFeeder: Stopped.")

    async def subscribe_tokens(self, tokens: list) -> None:
        """Proxy token subscription to the active inner feeder."""
        if self._feeder is not None:
            await self._feeder.subscribe_tokens(tokens)

    async def unsubscribe_tokens(self, tokens: list) -> None:
        """Proxy token unsubscription to the active inner feeder."""
        if self._feeder is not None:
            await self._feeder.unsubscribe_tokens(tokens)

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

    def record_tick(self) -> None:
        """Called by the EventBus interceptor to update heartbeat timestamp."""
        self._last_tick_ts = time.monotonic()

    @property
    def is_running(self) -> bool:
        return self._running and (self._feeder is not None and self._feeder.is_connected)
