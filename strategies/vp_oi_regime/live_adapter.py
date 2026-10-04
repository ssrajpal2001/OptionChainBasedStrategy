"""Per-book live adapter wrapping the pure vp_oi_regime classes
(SessionVolumeProfile/OiRegimeTracker/FuturesRolloverTracker/decide/
NineEmaTrailingStop) for use by a live (or backtest-replayed) SellStraddle
book. This module owns NO I/O and NO bus/broker calls -- it only tracks
state and returns decisions; the caller (strategies/sell_straddle/exits.py)
is responsible for acting on them (closing legs, buying hedges).

NOTE (2026-10-04): live OI is NOT currently captured anywhere in
strategies/sell_straddle/engine.py -- self._strike_prem only ever stores
{"ltp": ..., "atp": ...} per strike, never "oi". Until that capture is added
(a separate, small engine.py change reading OptionTick.oi), on_option_oi_tick
below simply never gets called in production and OiRegimeTracker.classify()
returns None, so `evaluate()` degrades safely to "no decision yet" rather
than silently trading on stale/zero OI. The adapter and its tests are correct
and ready; the missing OI capture is a separate, explicitly flagged gap.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from strategies.vp_oi_regime.decision_matrix import DecisionResult, NineEmaTrailingStop, decide
from strategies.vp_oi_regime.futures_rollover import FuturesRolloverTracker
from strategies.vp_oi_regime.oi_regime import OiRegimeTracker
from strategies.vp_oi_regime.volume_profile import SessionVolumeProfile

# NONE = flat/normal straddle running; NAKED_CE/NAKED_PE = one leg was exited
# per a Highly Bearish/Bullish regime and a hedge (if any) was bought;
# NAKED_BOTH = both legs exited per a Volatile regime.
NakedState = str  # "NONE" | "NAKED_CE" | "NAKED_PE" | "NAKED_BOTH"


@dataclass
class VpOiRegimeAdapter:
    strike_step: float = 50.0
    change_window_min: int = 5
    is_expiry_week: bool = False

    _vp: SessionVolumeProfile = field(init=False)
    _oi: OiRegimeTracker = field(init=False)
    _rollover: FuturesRolloverTracker = field(init=False)
    _ema: Dict[str, NineEmaTrailingStop] = field(init=False)

    naked_state: NakedState = field(default="NONE", init=False)
    # Entry premium of each naked long (needed for the Hedge Exit Rule's own
    # 9-EMA ride and for re-entry sizing); keyed "CE"/"PE".
    naked_entry_premium: Dict[str, float] = field(default_factory=dict, init=False)
    # True once a naked leg's stop has fired and the Re-entry Rule is now
    # watching for a POC/Value-Area re-entry to re-sell a fresh short.
    awaiting_reentry: Dict[str, bool] = field(default_factory=dict, init=False)

    last_decision: Optional[DecisionResult] = field(default=None, init=False)
    last_spot: float = field(default=0.0, init=False)
    last_poc: Optional[float] = field(default=None, init=False)
    last_vah: Optional[float] = field(default=None, init=False)
    last_val: Optional[float] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._vp = SessionVolumeProfile(rows=80, value_area_pct=0.70)
        self._oi = OiRegimeTracker(strike_step=self.strike_step, change_window_min=self.change_window_min)
        self._rollover = FuturesRolloverTracker(change_window_min=self.change_window_min)
        self._ema = {"CE": NineEmaTrailingStop(period=9), "PE": NineEmaTrailingStop(period=9)}

    def reset(self) -> None:
        """New trading day -- fresh everything, same lifecycle as the
        engine's other per-session trackers."""
        self._vp = SessionVolumeProfile(rows=80, value_area_pct=0.70)
        self._oi.reset()
        self._rollover.reset()
        for ema in self._ema.values():
            ema.reset()
        self.naked_state = "NONE"
        self.naked_entry_premium = {}
        self.awaiting_reentry = {}
        self.last_decision = None
        self.last_poc = self.last_vah = self.last_val = None

    # ── Feed methods (called by the engine as ticks/bars arrive) ──────────
    def on_futures_bar(self, high: float, low: float, volume: float, oi: float, ts: float) -> None:
        self._vp.add_bar(high, low, volume)
        self._rollover.update_near(ts, oi or 0.0, volume or 0.0)

    def on_futures_next_month_bar(self, volume: float, oi: float, ts: float) -> None:
        self._rollover.update_next(ts, oi or 0.0, volume or 0.0)

    def on_option_oi_tick(self, strike: int, side: str, oi: float, ts: float) -> None:
        self._oi.update_tick(strike, side, oi, ts)

    def on_naked_leg_price(self, side: str, ltp: float) -> None:
        """Feed the naked long's own live LTP into its 9-EMA trailing stop,
        every tick -- only meaningful while that side is actually naked."""
        if ltp and ltp > 0:
            self._ema[side].update(ltp)

    # ── Evaluation ──────────────────────────────────────────────────────
    def evaluate(self, spot: float, now_ts: float) -> Optional[DecisionResult]:
        """Call once per 1-min close (same cadence the matrix is defined
        against). Returns the current DecisionResult, or None if the
        OI tracker isn't warm yet (no option OI ticks fed this session --
        the honest, safe degrade described in this module's docstring)."""
        self.last_spot = spot
        snap = self._vp.snapshot()
        if snap:
            self.last_poc, self.last_vah, self.last_val = snap.poc, snap.vah, snap.val

        roll_state = self._rollover.classify(now_ts=now_ts, is_expiry_week=self.is_expiry_week)
        oi_res = self._oi.classify(spot=spot, minute_ts=now_ts, rollover_active=roll_state.rollover_detected)
        if oi_res is None:
            self.last_decision = None
            return None

        self.last_decision = decide(roll_state.future_oi_trend, oi_res.put_trend, oi_res.call_trend)
        return self.last_decision

    # ── Naked-leg lifecycle (state only -- caller executes the real orders) ──
    def mark_naked(self, state: NakedState, entry_premiums: Dict[str, float]) -> None:
        self.naked_state = state
        self.naked_entry_premium = dict(entry_premiums)
        self.awaiting_reentry = {}

    def stop_hit(self, side: str, ltp: float) -> bool:
        """direction='up' for a naked long CALL (exits on a close below its
        own 9-EMA), 'down' for a naked long PUT (exits on a close above)."""
        direction = "up" if side == "CE" else "down"
        return self._ema[side].stop_hit(ltp, direction)

    def on_naked_stopped(self, side: str) -> None:
        """Naked leg's 9-EMA stop fired -- arm the Re-entry Rule watch for
        this side; caller is responsible for actually closing the naked
        long + its hedge (Hedge Exit Rule) before/alongside this call."""
        self.awaiting_reentry[side] = True
        if self.naked_state == "NAKED_BOTH":
            self.naked_state = "NAKED_PE" if side == "CE" else "NAKED_CE"
        else:
            self.naked_state = "NONE"
        self._ema[side].reset()

    def check_reentry(self, side: str, close_price: float) -> bool:
        """Re-entry Rule: price closes back on the 'fakeout' side of the
        matrix row's own reference level. Per the authoritative source
        (CLAUDE.md Part 3), the reference is POC for the Rise-Future
        'Buildup' rows and -- for every other row -- a Value-Area boundary.
        This adapter checks against POC, which is the shared, always-
        available level (VAH/VAL-specific nuance is left to the caller,
        which has the full DecisionResult.reentry_rule text to inspect)."""
        if not self.awaiting_reentry.get(side):
            return False
        if self.last_poc is None:
            return False
        fired = (close_price > self.last_poc) if side == "PE" else (close_price < self.last_poc)
        if fired:
            self.awaiting_reentry[side] = False
        return fired

    def monitoring_state(self) -> dict:
        """UI-facing snapshot -- same role as every other strategy's own
        monitoring_state()/GET status endpoint in this codebase."""
        dr = self.last_decision
        return {
            "spot": self.last_spot,
            "poc": self.last_poc, "vah": self.last_vah, "val": self.last_val,
            "regime": dr.regime if dr else None,
            "call_action": dr.call_action if dr else None,
            "put_action": dr.put_action if dr else None,
            "call_hedge": {"enabled": dr.call_hedge.enabled, "pct": dr.call_hedge.pct_of_premium} if dr else None,
            "put_hedge": {"enabled": dr.put_hedge.enabled, "pct": dr.put_hedge.pct_of_premium} if dr else None,
            "naked_state": self.naked_state,
            "naked_entry_premium": dict(self.naked_entry_premium),
            "awaiting_reentry": dict(self.awaiting_reentry),
        }
