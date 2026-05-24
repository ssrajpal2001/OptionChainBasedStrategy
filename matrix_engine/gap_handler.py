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

  Phase 3 — Reset cascade (on gap detected, strictly sequenced):
    Step A — Candle buffer clear + restore (must complete first):
      1. CandleCache.reset_symbol(underlying) — wipe all stale ring-buffers.
      2. asyncio.to_thread(state_persistence.restore_candle_history(...))
         — re-populate last 500 OHLCV rows from SQLite so that RSI(14),
         ADX(20), and VWAP(500) are at full analytical parity before any
         strategy logic runs.  This step MUST precede strategy resets.
    Step B — Strategy + infrastructure callbacks (run concurrently):
      3. All registered async callbacks: rebalancer.reset_atm, strategy
         state machine resets, etc.  These receive (underlying, opening_spot).
    Step C — Bus notification:
      4. Publish SYSTEM_EVENT GAP_OPEN so downstream monitors can react.

  Strict ordering of Steps A → B is critical: if strategies reset before the
  candle buffers are re-populated, their first evaluation fires against empty
  indicator arrays and produces either errors or trivially-incorrect signals.

Constructor args:
  candle_cache       (CandleCache, optional)       — enables internal clear+restore
  state_persistence  (StatePersistence, optional)  — provides restore_candle_history()

  If candle_cache is not provided, the clear+restore step is skipped entirely and
  the caller is responsible for registering a callback that handles it.
  If candle_cache IS provided but state_persistence is NOT, buffers are cleared but
  not restored — a WARNING is logged, and indicators need natural warmup.

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
    beyond GAP_THRESHOLD.

    On a detected gap, the reset cascade is strictly sequenced:
      1. Clear CandleCache ring-buffers for the underlying.
      2. Re-populate from SQLite (asyncio.to_thread) — indicators reach full
         parity BEFORE strategy callbacks fire.
      3. Fire all registered async strategy/infrastructure reset callbacks.
      4. Publish GAP_OPEN system event.

    Wiring in main.py:
        gap_handler = GapHandler(bus, cfg,
                                 candle_cache=cache,
                                 state_persistence=persist)
        gap_handler.register_reset_callback(rebalancer.reset_atm)
        gap_handler.register_reset_callback(strategy_a.reset_state_async)
        gap_handler.register_reset_callback(strategy_b.reset_state_async)
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        candle_cache=None,       # CandleCache — if set, manages clear+restore internally
        state_persistence=None,  # StatePersistence — source for restore_candle_history()
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._candle_cache = candle_cache
        self._state_persistence = state_persistence
        self._tick_queue = bus.subscribe(Topic.INDEX_TICK)
        self._running = False
        self._state: Dict[str, _GapState] = {
            u: _GapState() for u in cfg.monitored_indices
        }
        self._callbacks: List[ResetCallback] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def register_reset_callback(self, cb: ResetCallback) -> None:
        """
        Register an async callback invoked after candle buffers are restored.
        Signature: async (underlying: str, opening_spot: float) -> None

        These callbacks fire AFTER the candle clear+restore completes, so
        strategy state machines evaluate against repopulated indicator arrays.
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
        Strictly sequenced reset cascade:
          Step A — clear stale candle buffers and re-populate from DB (awaited).
          Step B — fire all registered strategy/infra callbacks concurrently.
          Step C — publish GAP_OPEN on the bus.

        The await on Step A guarantees that when strategy callbacks in Step B
        call evaluate(), they are reading against DB-restored indicator arrays,
        not empty deques.
        """
        state.gap_fired = True

        logger.warning(
            "GapHandler: [%s] GAP OPEN DETECTED — drift=%.2f%% "
            "(ref=%.2f → open=%.2f). Triggering full reset cascade.",
            underlying, drift * 100, state.pre_open_ref, opening_spot,
        )

        # Step A: clear + restore candle buffers BEFORE any callback fires
        await self._clear_and_restore_candles(underlying)

        # Step B: fire strategy reset callbacks concurrently
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

        # Step C: publish system event (after callbacks so subscribers see consistent state)
        evt = SystemEvent(
            code=SysEvent.GAP_OPEN,
            message=(
                f"{underlying}:ref={state.pre_open_ref:.2f}:"
                f"open={opening_spot:.2f}:drift={drift*100:.2f}pct"
            ),
        )
        await self._bus.publish(Topic.SYSTEM_EVENT, evt)

        logger.info(
            "GapHandler: [%s] reset cascade complete (%d callbacks invoked).",
            underlying, len(self._callbacks),
        )

    async def _clear_and_restore_candles(self, underlying: str) -> None:
        """
        Clear the stale candle ring-buffers for this underlying, then
        immediately re-populate from SQLite so RSI(14), VWAP(500), and ADX(20)
        are at full analytical parity before any strategy evaluation runs.

        All SQLite I/O runs in asyncio.to_thread() — event loop never stalled.
        """
        if self._candle_cache is None:
            # Caller manages candle reset via registered callbacks
            return

        # Wipe stale ring-buffers across all timeframes for this underlying
        self._candle_cache.reset_symbol(underlying)

        if self._state_persistence is None:
            logger.warning(
                "GapHandler: [%s] candle buffers cleared but no StatePersistence "
                "provided — RSI/VWAP/ADX need natural warmup (15/42/500 bars).",
                underlying,
            )
            return

        # Re-populate from SQLite: last 500 bars per symbol×timeframe
        try:
            history = await asyncio.to_thread(
                self._state_persistence.restore_candle_history,
                [underlying],
                self._cfg.candle_timeframes,
                500,
            )
            loaded_rows = 0
            for (sym, tf), df in history.items():
                if not df.empty:
                    self._candle_cache.load_history(sym, tf, df)
                    loaded_rows += len(df)
            logger.info(
                "GapHandler: [%s] candle buffers cleared and restored — "
                "%d rows reloaded across %d symbol×timeframe pair(s). "
                "RSI/VWAP/ADX at full parity.",
                underlying, loaded_rows, len(history),
            )
        except Exception as exc:
            logger.warning(
                "GapHandler: [%s] DB restore failed after gap clear: %s — "
                "indicators will need natural warmup.",
                underlying, exc,
            )
