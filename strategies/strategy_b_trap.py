"""
strategies/strategy_b_trap.py — Institutional Liquidity Trap Engine.

Implements:
  • Rolling Base: any candle closing below its predecessor becomes the
    new base_level (never static, always dynamic).
  • Void Lift: if price accelerates past void_atr_mult × ATR beyond the
    trap level, the setup enters a VOID state and is suspended. The void
    is LIFTED only when candle.low <= htf_entry_level (retest condition).

State machine per underlying:
  IDLE → TRAP_DETECTED → STALLING → [VOID | REVERSAL_CONFIRMED]
                  ↑                         │
                  └─────────────────────────┘ (reset on fill or invalidation)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional

from config.global_config import GlobalConfig, IST
from matrix_engine.indicators import TechSnapshot
from matrix_engine.option_matrix import ChainSnapshot
from strategies.base_strategy import BaseStrategy, Direction, SignalPackage, StrategyID

logger = logging.getLogger(__name__)


class _Phase(Enum):
    IDLE             = auto()
    BEARISH_TRAP     = auto()    # Price broke above call resistance
    BULLISH_TRAP     = auto()    # Price broke below put support
    VOID             = auto()    # Price ran away; setup suspended
    CONFIRMED        = auto()    # Reversal confirmed — fire signal


@dataclass
class _TrapState:
    phase: _Phase = _Phase.IDLE
    trap_level: float = 0.0
    trap_high: float = 0.0
    trap_low: float = 0.0
    oi_at_trap: int = 0
    stall_count: int = 0
    trap_type: str = ""            # "bearish" | "bullish"
    rolling_base: float = 0.0     # Dynamic support/resistance (rolling-base mechanism)
    htf_entry_level: float = 0.0  # High-timeframe structural entry for void lift
    void_since: Optional[datetime] = None


class StrategyB_Trap(BaseStrategy):

    def __init__(self, cfg: GlobalConfig) -> None:
        super().__init__(cfg)
        self._states: Dict[str, _TrapState] = {}

    def evaluate(
        self,
        tech: TechSnapshot,
        chain: ChainSnapshot,
        all_tf: List[Optional[TechSnapshot]],
    ) -> Optional[SignalPackage]:
        und = chain.underlying
        sp  = self._sp
        spot = chain.spot
        state = self._states.setdefault(und, _TrapState())

        call_s   = chain.max_call_oi_strike
        put_s    = chain.max_put_oi_strike
        call_row = chain.row(call_s)
        put_row  = chain.row(put_s)
        step     = chain._step()

        # ── Rolling Base Update ───────────────────────────────────────────────
        # Any candle close below the previous candle sets a new dynamic base.
        if tech.c_close < tech.p_close:
            new_base = tech.c_low
            if state.rolling_base == 0.0 or new_base < state.rolling_base:
                state.rolling_base = new_base

        # ── Phase: IDLE — scan for trap setup ────────────────────────────────
        if state.phase == _Phase.IDLE:
            if call_row and spot > call_s + step * 0.3:
                # Bearish trap candidate: price broke above call resistance with OI/vol spike
                if call_row.call_doi_spike(sp.trap_oi_spike_mult) and tech.is_vol_spike:
                    state.phase      = _Phase.BEARISH_TRAP
                    state.trap_level = call_s
                    state.trap_high  = tech.c_high
                    state.trap_low   = tech.c_low
                    state.trap_type  = "bearish"
                    state.oi_at_trap = call_row.call_doi
                    state.stall_count = 0
                    state.htf_entry_level = call_s    # Structural level for void lift
                    state.rolling_base = tech.c_low
                    logger.debug("StratB: BEARISH_TRAP detected %s @ %.0f", und, call_s)

            elif put_row and spot < put_s - step * 0.3:
                # Bullish trap candidate: price broke below put support with OI/vol spike
                if put_row.put_doi_spike(sp.trap_oi_spike_mult) and tech.is_vol_spike:
                    state.phase      = _Phase.BULLISH_TRAP
                    state.trap_level = put_s
                    state.trap_high  = tech.c_high
                    state.trap_low   = tech.c_low
                    state.trap_type  = "bullish"
                    state.oi_at_trap = put_row.put_doi
                    state.stall_count = 0
                    state.htf_entry_level = put_s
                    state.rolling_base = tech.c_high
                    logger.debug("StratB: BULLISH_TRAP detected %s @ %.0f", und, put_s)

        # ── Phase: TRAP WATCHING — look for stall and unwinding ──────────────
        elif state.phase in (_Phase.BEARISH_TRAP, _Phase.BULLISH_TRAP):
            void_band = tech.atr_val * sp.void_atr_mult

            # Void state: price accelerated way past trap level
            if state.phase == _Phase.BEARISH_TRAP:
                ran_away = spot > state.trap_high + void_band
            else:
                ran_away = spot < state.trap_low - void_band

            if ran_away:
                state.phase = _Phase.VOID
                state.void_since = datetime.now(IST)
                logger.debug("StratB: VOID entered for %s (price ran %.1f beyond band)", und, void_band)
                return None

            # Count stall candles (price not making new extremes)
            state.stall_count += 1

            # Check OI unwinding — smart money reversing
            if state.phase == _Phase.BEARISH_TRAP and call_row:
                unwinding = call_row.call_doi < 0 and abs(call_row.call_doi) > 20_000
            elif state.phase == _Phase.BULLISH_TRAP and put_row:
                unwinding = put_row.put_doi < 0 and abs(put_row.put_doi) > 20_000
            else:
                unwinding = False

            if state.stall_count >= sp.trap_stall_candles and unwinding:
                state.phase = _Phase.CONFIRMED

        # ── Phase: VOID — wait for void lift condition ────────────────────────
        elif state.phase == _Phase.VOID:
            # Guard: htf_entry_level must be set (trap must have been detected first)
            if state.htf_entry_level == 0.0:
                state.phase = _Phase.IDLE
                return None

            tol = state.htf_entry_level * sp.void_lift_retest_tolerance / 100
            # Void lifts ONLY when candle.low retests back to the HTF structural level.
            # rolling_base must also be below htf_entry_level to confirm bearish pressure
            # unwound (bullish trap) or price pulled back (bearish trap).
            retest_hit = tech.c_low <= state.htf_entry_level + tol
            # For bearish traps: rolling_base confirms downtrend is intact
            # For bullish traps: rolling_base (which tracks highs) stays above level
            if retest_hit:
                logger.debug(
                    "StratB: VOID LIFTED for %s — retest of HTF level %.0f "
                    "(rolling_base=%.0f tol=%.2f)",
                    und, state.htf_entry_level, state.rolling_base, tol,
                )
                state.phase = _Phase.CONFIRMED
            else:
                return None    # Still in void

        # ── Phase: CONFIRMED — generate signal ───────────────────────────────
        if state.phase == _Phase.CONFIRMED:
            state.phase = _Phase.IDLE    # Reset immediately

            if state.trap_type == "bearish":
                # Counter-trend SHORT: failed breakout above resistance
                sl  = state.trap_high + tech.atr_val * 0.3
                tgt = spot - (sl - spot) * self._sp.min_risk_reward
                strike = self._execution_strike(chain, Direction.SHORT)
                conf = self._trap_conf(tech, chain, state)
                notes = (f"BearTrap@{state.trap_level:.0f} "
                         f"OI={state.oi_at_trap:,} stall={state.stall_count} "
                         f"RollingBase={state.rolling_base:.0f}")
                logger.info("StratB SHORT (bear-trap) | %s | conf=%.2f | %s", und, conf, notes)
                return SignalPackage(
                    source=StrategyID.B_TRAP, direction=Direction.SHORT,
                    underlying=und, option_type="PE",
                    target_strike=strike, entry_spot=spot,
                    stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
                )

            elif state.trap_type == "bullish":
                # Counter-trend LONG: failed breakdown below support
                sl  = state.trap_low - tech.atr_val * 0.3
                tgt = spot + (spot - sl) * self._sp.min_risk_reward
                strike = self._execution_strike(chain, Direction.LONG)
                conf = self._trap_conf(tech, chain, state)
                notes = (f"BullTrap@{state.trap_level:.0f} "
                         f"OI={state.oi_at_trap:,} stall={state.stall_count} "
                         f"RollingBase={state.rolling_base:.0f}")
                logger.info("StratB LONG (bull-trap) | %s | conf=%.2f | %s", und, conf, notes)
                return SignalPackage(
                    source=StrategyID.B_TRAP, direction=Direction.LONG,
                    underlying=und, option_type="CE",
                    target_strike=strike, entry_spot=spot,
                    stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
                )
        return None

    def _trap_conf(self, tech: TechSnapshot, chain: ChainSnapshot, state: _TrapState) -> float:
        s = 0.40
        if tech.is_vol_spike:              s += 0.15
        if tech.adx_val > 20:             s += 0.10
        if state.stall_count >= 3:        s += 0.10
        pcr = chain.pcr_smooth()
        if state.trap_type == "bearish" and pcr < 0.8: s += 0.10
        if state.trap_type == "bullish" and pcr > 1.2: s += 0.10
        if state.void_since is not None:  s += 0.05   # Void-lift trades get extra conf
        return min(s, 1.0)

    def reset(self, underlying: str) -> None:
        self._states.pop(underlying, None)
