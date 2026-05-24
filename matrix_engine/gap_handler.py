"""
matrix_engine/gap_handler.py — Market-open gap detector and system reset coordinator.

Problem this solves:
  On days with overnight news or pre-market circuit moves the NSE pre-open
  session can print a reference price 1-3% away from the prior close.  If the
  system boots with ATM and indicator windows built from yesterday's close, the
  first 5-15 minute candles carry a phantom baseline that poisons RSI, VWAP,
  and ADX for the entire morning session.

How it works:
  Phase 1 — Pre-open capture (09:08:00–09:14:59 IST):
    On the first INDEX_TICK at or after 09:08:00 IST, record the spot price as
    the pre-open reference for that underlying.  This is the equilibrium price
    printed during the NSE call-auction window.

  Phase 2 — Opening validation (first tick at or after 09:15:01 IST):
    Compare the opening spot to the pre-open reference.
    If |opening - reference| / reference > GAP_THRESHOLD (default 1%), a gap
    has been detected.

  Phase 3 — Reset cascade (on gap detected, async, non-blocking):
    1. Publish SYSTEM_EVENT GAP_OPEN so all subscribers can react.
    2. Call every registered async reset callback with (underlying, opening_spot).
       Callbacks are registered by the application layer:
         gap_handler.register_reset_callback(candle_cache.reset_symbol_async)
         gap_handler.register_reset_callback(rebalancer.reset_atm)
         gap_handler.register_reset_callback(strategy_a.reset_state)
         ...
    3. Log the gap size so the operator is alerted via structured logs.

  After the reset, the rebalancer re-derives ATM from the new opening price,
  candle buffers accumulate fresh bars, and indicators reach valid readings
  within their warmup periods (RSI after 15 bars, ADX after ~42 bars, VWAP
  after 500 bars).

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from typing import Awaitable, Callable, Dict, List, Optional

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick, SystemEvent

logger = logging.getLogger(__name__)

# 09:08 IST — first tick after pre-open call-auction
_PRE_OPEN_CAPTURE = time(9, 8, 0)

# 09:15:01 IST — first tick after market opens
_MARKET_OPEN_CHECK = time(9, 15, 1)

# Gap threshold: 1% drift triggers full reset
GAP_THRESHOLD = 0.01

# Callback signature: async (underlying: str, opening_spot: float) -> None
ResetCallback = Callable[[str, float], Awaitable[None]]


# ─────────────────────────────────────────────────────────────────────────────
# Per-Underlying Gap State
# ─────────────────────────────────────────────────────────────────────────────

class _GapState:
    __slots__ = ("pre_open_ref", "gap_checked", "gap_fired")

    def __init__(self) -> None:
        self.pre_open_ref: Optional[float] = None   # Spot captured at 09:08
        self.gap_checked: bool = False               # 09:15:01 check done
        self.gap_fired: bool = False                 # Reset cascade triggered


# ─────────────────────────────────────────────────────────────────────────────
# Gap Handler
# ─────────────────────────────────────────────────────────────────────────────

class GapHandler:
    """
    Monitors INDEX_TICK events, captures the pre-open reference price at
    09:08 IST, and at 09:15:01 IST checks whether the opening spot has gapped
    beyond GAP_THRESHOLD.  On a detected gap, fires all registered async reset
    callbacks and publishes a GAP_OPEN system event.

    Registration example (in main.py or admin_console.py):
        gap_handler.register_reset_callback(candle_cache.reset_symbol_async)
        gap_handler.register_reset_callback(rebalancer.reset_atm)
        gap_handler.register_reset_callback(strategy_a.reset_state_async)
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus = bus
        self._cfg = cfg
        self._tick_queue = bus.subscribe(Topic.INDEX_TICK)
        self._running = False
        self._state: Dict[str, _GapState] = {
            u: _GapState() for u in cfg.monitored_indices
        }
        self._callbacks: List[ResetCallback] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def register_reset_callback(self, cb: ResetCallback) -> None:
        """
        Register an async callback to invoke when a gap-open is detected.
        Signature: async (underlying: str, opening_spot: float) -> None
        """
        self._callbacks.append(cb)
        logger.debug("GapHandler: registered reset callback %s.", getattr(cb, "__qualname__", repr(cb)))

    def gap_reference(self, underlying: str) -> Optional[float]:
        """Return the captured pre-open reference price, or None if not yet captured."""
        st = self._state.get(underlying)
        return st.pre_open_ref if st else None

    def gap_was_fired(self, underlying: str) -> bool:
        """True if a gap-open reset was triggered for this underlying today."""
        st = self._state.get(underlying)
        return st.gap_fired if st else False

    def daily_reset(self) -> None:
        """
        Clear all per-underlying state at EOD so tomorrow's session starts fresh.
        Call this from the DAILY_RESET system event handler.
        """
        for st in self._state.values():
            st.pre_open_ref = None
            st.gap_checked = False
            st.gap_fired = False
        logger.info("GapHandler: daily state reset complete.")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("GapHandler: started (threshold=%.1f%%).", GAP_THRESHOLD * 100)
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(
                    self._tick_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                await self._on_tick(tick)
            except Exception as exc:
                logger.debug("GapHandler: _on_tick error: %s", exc)

    def stop(self) -> None:
        self._running = False

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def _on_tick(self, tick: IndexTick) -> None:
        underlying = tick.symbol
        if underlying not in self._state:
            return

        state = self._state[underlying]
        tick_time = tick.timestamp.astimezone(IST).time()

        # Phase 1: capture pre-open reference at first tick >= 09:08:00
        if (
            state.pre_open_ref is None
            and tick_time >= _PRE_OPEN_CAPTURE
            and tick_time < _MARKET_OPEN_CHECK
        ):
            state.pre_open_ref = tick.ltp
            logger.info(
                "GapHandler: [%s] pre-open reference captured = %.2f (at %s).",
                underlying, tick.ltp,
                tick.timestamp.astimezone(IST).strftime("%H:%M:%S"),
            )
            return

        # Phase 2: opening validation on first tick >= 09:15:01
        if (
            not state.gap_checked
            and state.pre_open_ref is not None
            and tick_time >= _MARKET_OPEN_CHECK
        ):
            state.gap_checked = True
            drift = abs(tick.ltp - state.pre_open_ref) / state.pre_open_ref
            logger.info(
                "GapHandler: [%s] opening check — ref=%.2f open=%.2f drift=%.2f%%.",
                underlying, state.pre_open_ref, tick.ltp, drift * 100,
            )
            if drift > GAP_THRESHOLD:
                await self._fire_gap_reset(underlying, tick.ltp, drift, state)

    # ── Gap reset cascade ─────────────────────────────────────────────────────

    async def _fire_gap_reset(
        self,
        underlying: str,
        opening_spot: float,
        drift: float,
        state: _GapState,
    ) -> None:
        """
        Orchestrate the full reset cascade when a gap is detected.
        All callbacks run concurrently via asyncio.gather so no single slow
        callback delays the others.
        """
        state.gap_fired = True

        logger.warning(
            "GapHandler: [%s] GAP OPEN DETECTED — drift=%.2f%% "
            "(ref=%.2f → open=%.2f). Triggering full reset cascade.",
            underlying, drift * 100, state.pre_open_ref, opening_spot,
        )

        # Publish GAP_OPEN system event — strategies/monitors react independently
        evt = SystemEvent(
            code=SysEvent.GAP_OPEN,
            message=(
                f"{underlying}:ref={state.pre_open_ref:.2f}:"
                f"open={opening_spot:.2f}:drift={drift*100:.2f}pct"
            ),
        )
        await self._bus.publish(Topic.SYSTEM_EVENT, evt)

        # Fire all registered reset callbacks concurrently
        if self._callbacks:
            results = await asyncio.gather(
                *[cb(underlying, opening_spot) for cb in self._callbacks],
                return_exceptions=True,
            )
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                    cb_name = getattr(self._callbacks[i], "__qualname__", f"callback[{i}]")
                    logger.warning(
                        "GapHandler: [%s] reset callback %s raised: %s",
                        underlying, cb_name, res,
                    )

        logger.info(
            "GapHandler: [%s] reset cascade complete (%d callbacks invoked).",
            underlying, len(self._callbacks),
        )
