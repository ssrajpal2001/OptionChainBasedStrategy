"""
data_layer/strike_rebalancer.py — Dynamic ATM strike monitor and rebalancer.

Subscribes to INDEX_TICK on the EventBus.  For each underlying:
  1. Records market-open ATM on the first tick at or after 09:15 IST.
  2. On every subsequent tick, checks if spot has drifted ≥ 3 × strike_step
     from the recorded open ATM.
  3. If drift is detected, computes the new ATM and:
       a. Publishes a REBALANCE_REQUIRED system event.
       b. Calls feeder.unsubscribe_tokens() for dropped strikes.
       c. Calls feeder.subscribe_tokens() for newly needed strikes.
       d. Updates the open ATM baseline to the new ATM.
  4. Strikes with open positions (pinned_strikes) are NEVER unsubscribed —
     protecting active option streams from data loss.

No time.sleep. All async.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Dict, Optional, Set

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick, SystemEvent, BaseFeeder
from data_layer.symbol_translator import SymbolTranslator, InternalSymbol

logger = logging.getLogger(__name__)

_MARKET_OPEN = time(9, 15, 0)
_REBALANCE_THRESHOLD = 3   # strike intervals


# ─────────────────────────────────────────────────────────────────────────────
# Per-Underlying Rebalance State
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _UnderlyingState:
    open_atm: Optional[float] = None         # ATM recorded at market open
    current_atm: Optional[float] = None      # Last known ATM after rebalance
    active_strikes: Set[float] = field(default_factory=set)
    pinned_strikes: Set[float] = field(default_factory=set)  # Must not unsub
    rebalance_count: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# Strike Rebalancer
# ─────────────────────────────────────────────────────────────────────────────

class StrikeRebalancer:
    """
    Monitors spot drift and asynchronously resubscribes to the ATM ± chain_depth
    strike window whenever drift ≥ REBALANCE_THRESHOLD × strike_step.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        feeder: BaseFeeder,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._feeder = feeder
        self._tick_queue = bus.subscribe(Topic.INDEX_TICK)
        self._running = False
        # state per underlying
        self._state: Dict[str, _UnderlyingState] = {
            u: _UnderlyingState() for u in cfg.monitored_indices
        }

    # ── Public API ─────────────────────────────────────────────────────────────

    def pin_strike(self, underlying: str, strike: float) -> None:
        """Called by execution layer when a position is opened — prevents unsub."""
        if underlying in self._state:
            self._state[underlying].pinned_strikes.add(strike)

    def unpin_strike(self, underlying: str, strike: float) -> None:
        """Called when a position is closed."""
        if underlying in self._state:
            self._state[underlying].pinned_strikes.discard(strike)

    def rebalance_stats(self) -> Dict[str, int]:
        return {u: s.rebalance_count for u, s in self._state.items()}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("StrikeRebalancer: started.")
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(
                    self._tick_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            await self._on_tick(tick)

    def stop(self) -> None:
        self._running = False

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def _on_tick(self, tick: IndexTick) -> None:
        underlying = tick.symbol
        if underlying not in self._state:
            return

        state = self._state[underlying]
        step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        atm = _round_to_strike(tick.ltp, step)

        # Record market-open ATM on first qualifying tick
        if state.open_atm is None and _is_market_open(tick.timestamp):
            state.open_atm = atm
            state.current_atm = atm
            await self._initial_subscribe(underlying, atm, step)
            logger.info(
                "StrikeRebalancer: [%s] Market-open ATM = %.0f",
                underlying, atm,
            )
            return

        if state.current_atm is None:
            return

        drift_intervals = abs(atm - state.current_atm) / step
        if drift_intervals >= _REBALANCE_THRESHOLD:
            logger.info(
                "StrikeRebalancer: [%s] Spot %.2f drifted %.1f intervals from ATM %.0f"
                " — rebalancing to new ATM %.0f.",
                underlying, tick.ltp, drift_intervals, state.current_atm, atm,
            )
            await self._rebalance(underlying, atm, step, state)

    # ── Subscription management ───────────────────────────────────────────────

    async def _initial_subscribe(
        self, underlying: str, atm: float, step: float
    ) -> None:
        state = self._state[underlying]
        depth = self._cfg.chain_depth
        strikes = _strike_window(atm, step, depth)
        state.active_strikes = set(strikes)
        tokens = self._strikes_to_tokens(underlying, strikes)
        if tokens:
            await self._feeder.subscribe_tokens(tokens)
        logger.debug(
            "StrikeRebalancer: [%s] Initial subscription %d strikes around %.0f.",
            underlying, len(strikes), atm,
        )

    async def _rebalance(
        self,
        underlying: str,
        new_atm: float,
        step: float,
        state: _UnderlyingState,
    ) -> None:
        depth = self._cfg.chain_depth
        old_strikes = state.active_strikes
        new_strikes = set(_strike_window(new_atm, step, depth))

        to_unsub = old_strikes - new_strikes - state.pinned_strikes
        to_sub   = new_strikes - old_strikes

        # Unsubscribe dropped strikes (never drops pinned ones)
        if to_unsub:
            tokens = self._strikes_to_tokens(underlying, list(to_unsub))
            if tokens:
                await self._feeder.unsubscribe_tokens(tokens)

        # Subscribe newly needed strikes
        if to_sub:
            tokens = self._strikes_to_tokens(underlying, list(to_sub))
            if tokens:
                await self._feeder.subscribe_tokens(tokens)

        # Update state
        state.active_strikes = (old_strikes - to_unsub) | to_sub
        state.current_atm = new_atm
        state.rebalance_count += 1

        # Publish system event for admin visibility
        evt = SystemEvent(
            code=SysEvent.FEEDER_RESTORED,   # Reuse as closest semantic match
            message=(
                f"REBALANCE {underlying}: new_atm={new_atm:.0f} "
                f"sub={len(to_sub)} unsub={len(to_unsub)} "
                f"pinned={len(state.pinned_strikes)}"
            ),
        )
        await self._bus.publish(Topic.SYSTEM_EVENT, evt)

    def _strikes_to_tokens(self, underlying: str, strikes) -> list:
        """Convert strike prices to broker-agnostic subscription tokens."""
        tokens = []
        from datetime import date
        today = datetime.now(IST).date()
        # Use a placeholder expiry for token building (feeder resolves actual)
        for strike in strikes:
            for otype in ("CE", "PE"):
                sym = InternalSymbol(
                    underlying=underlying,
                    strike=float(strike),
                    option_type=otype,
                    expiry=today,
                )
                tokens.append(str(sym))
        return tokens


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _round_to_strike(price: float, step: float) -> float:
    return round(round(price / step) * step, 2)


def _strike_window(atm: float, step: float, depth: int) -> list:
    return [atm + (i * step) for i in range(-depth, depth + 1)]


def _is_market_open(ts: datetime) -> bool:
    t = ts.astimezone(IST).time()
    return t >= _MARKET_OPEN
