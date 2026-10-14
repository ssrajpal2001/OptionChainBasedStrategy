"""Per-book live adapter wrapping the pure vp_oi_regime classes
(SessionVolumeProfile/OiRegimeTracker/FuturesRolloverTracker/decide/
NineEmaTrailingStop) for use by a live (or backtest-replayed) SellStraddle
book. This module owns NO I/O and NO bus/broker calls -- it only tracks
state and returns decisions; the caller (strategies/sell_straddle/exits.py)
is responsible for acting on them (closing legs, buying hedges).

NOTE (2026-10-04, UPDATED 2026-10-09): engine.py now DOES capture live
option OI (self._strike_prem stores an "oi" key per strike, see
engine.py's _option_loop) and feeds it to on_option_oi_tick below -- this
module's own docstring was stale, written before that capture was added.
The adapter and its tests are correct and ready, and are now genuinely
live (confirmed via real production logs 2026-10-07/08: futures_oi and
regime classification both updating off real data).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

from strategies.vp_oi_regime.decision_matrix import DecisionResult, NineEmaTrailingStop, decide
from strategies.vp_oi_regime.futures_rollover import FuturesRolloverTracker
from strategies.vp_oi_regime.oi_regime import OiRegimeTracker
from strategies.vp_oi_regime.volume_profile import SessionVolumeProfile, VolumeProfileSnapshot

# NONE = flat/normal straddle running; NAKED_CE/NAKED_PE = one leg was exited
# per a Highly Bearish/Bullish regime and a hedge (if any) was bought;
# NAKED_BOTH = both legs exited per a Volatile regime.
NakedState = str  # "NONE" | "NAKED_CE" | "NAKED_PE" | "NAKED_BOTH"


@dataclass
class VpOiRegimeAdapter:
    strike_step: float = 50.0
    change_window_min: int = 5
    is_expiry_week: bool = False
    # 2026-10-10, direct user ask: exposed as an admin-tunable setting
    # (SellStraddleConfig.vp_oi_noise_floor_pct) instead of a hardcoded
    # constant, since the user wants to optimize this value against real
    # forward data.
    noise_floor_pct: float = 15.0
    # 2026-10-10, same ask: the wall-hold override threshold (how much the
    # dominant-OI strike's own OI must move, while spot sits near it, before
    # its reading overrides the band-wide trend).
    override_pct: float = 30.0

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
    # 2026-10-09, direct user spec (price-location/LVN confirmation gate):
    # the full snapshot (needed for .in_lvn()/.location(), not just the
    # three POC/VAH/VAL floats above) so a caller can check "volume
    # acceptance in LVN below VAL/above VAH" before acting on a Highly
    # Bearish/Bullish regime, matching the Algo S/R Trigger text already in
    # decision_matrix.py's _BEARISH_SR/_BULLISH_SR constants.
    last_snapshot: Optional[VolumeProfileSnapshot] = field(default=None, init=False)
    # 2026-10-09, direct user ask: expose the raw Rise/Fall/No Change inputs
    # the regime label is built from, not just the conclusion -- previously
    # computed in evaluate() and discarded right after feeding decide().
    last_future_trend: Optional[str] = field(default=None, init=False)
    last_call_trend: Optional[str] = field(default=None, init=False)
    last_put_trend: Optional[str] = field(default=None, init=False)
    # 2026-10-09, direct user ask: previous (anchor) OI, current OI, and the
    # %-move between them that the Rise/Fall/No Change call above is based
    # on -- all three already real fields on RolloverState/OiRegimeResult,
    # just never carried past evaluate() before.
    last_future_oi_now: Optional[float] = field(default=None, init=False)
    last_future_oi_anchor: Optional[float] = field(default=None, init=False)
    last_future_oi_pct: Optional[float] = field(default=None, init=False)
    last_call_oi_now: Optional[float] = field(default=None, init=False)
    last_call_oi_anchor: Optional[float] = field(default=None, init=False)
    last_call_oi_pct: Optional[float] = field(default=None, init=False)
    last_put_oi_now: Optional[float] = field(default=None, init=False)
    last_put_oi_anchor: Optional[float] = field(default=None, init=False)
    last_put_oi_pct: Optional[float] = field(default=None, init=False)
    # Per-side OTM-shift state for the "shift to OTM while not yet LVN-
    # confirmed" interim action -- tracks the side's CURRENT strike after a
    # shift, so _check_vp_oi_regime only shifts once per regime episode
    # (not every cycle) and can tell a fresh regime flip from a still-active
    # one. Cleared on reset() and whenever the regime leaves Highly
    # Bearish/Bullish for that side.
    shifted_strike: Dict[str, int] = field(default_factory=dict, init=False)
    # 2026-10-09, direct user spec: client-visible tick-by-tick trail of
    # VP/OI decision-point reasoning (LVN check, POC-reversal check, which
    # of the 3 outcomes fired) -- same "recent remarks" pattern OI-Flow's
    # own telemetry already uses, surfaced via monitoring_state() ->
    # GET /api/sellstraddle/vpoi_status -> monitor.html's VP/OI panel.
    remarks: Deque[str] = field(default_factory=lambda: deque(maxlen=30), init=False)

    # 2026-10-11, direct user spec (PDF decision-matrix rows 9/18/27, the
    # three Volatile rows): hedge buys on a Volatile full-close are
    # CONDITIONAL -- "Buy a Call hedge IF VAH broken in 5 mins" / "Buy a Put
    # hedge IF VAL broken in 5 mins" -- not immediate. Armed by
    # arm_volatile_watch() right after the straddle closes; each side's
    # entry removed once its 5-minute window resolves (break -> hedge
    # bought by the caller, or timeout -> no hedge, per spec). Keyed "CE"
    # (watches VAH, hedge is a Call) / "PE" (watches VAL, hedge is a Put).
    volatile_watch: Dict[str, dict] = field(default_factory=dict, init=False)
    # Re-entry Rule for all three Volatile rows: "Wait for a new POC
    # formation before re-entering. Enter as per entry logic." -- the POC
    # value AT THE MOMENT of the Volatile exit; normal re-entry stays
    # blocked while the live POC hasn't yet moved off this anchor.
    reentry_poc_anchor: Optional[float] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._vp = SessionVolumeProfile(rows=80, value_area_pct=0.70)
        self._oi = OiRegimeTracker(strike_step=self.strike_step, change_window_min=self.change_window_min,
                                    noise_floor_pct=self.noise_floor_pct, override_pct=self.override_pct)
        self._rollover = FuturesRolloverTracker(change_window_min=self.change_window_min)
        self._ema = {"CE": NineEmaTrailingStop(period=9), "PE": NineEmaTrailingStop(period=9)}

    def freeze_atm_for_day(self, atm: int) -> None:
        """2026-10-14, direct user spec: freeze the OI tracker's ATM±band to
        a fixed strike set for the whole trading day (off the real 9:15
        session-open price), instead of re-centering on live spot every
        evaluation -- see OiRegimeTracker.freeze_atm's own docstring for the
        full rationale. Call once, at session start or right after a
        mid-day restart."""
        self._oi.freeze_atm(atm)

    def seed_anchors_from_recorder(self, peaks: dict, now_ts: float) -> None:
        """2026-10-13, direct user spec: call this ONCE, right after
        construction on a mid-day restart (before any real update/evaluate),
        with the dict returned by recorder.load_today_peaks(). Seeds each
        latch's anchor to today's REAL peak OI so far, instead of letting it
        reset to whatever's live at the exact restart moment -- avoids a
        fake, huge-looking "move" off a wrong baseline right after restart.
        No-op for any side with no real history yet today (None)."""
        self._oi.seed_anchors(peaks.get("call_oi_peak"), peaks.get("put_oi_peak"), now_ts)
        fut_peak = peaks.get("future_oi_peak")
        if fut_peak is not None:
            self._rollover.seed_anchor(fut_peak, now_ts)

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
        self.last_snapshot = None
        self.last_future_trend = self.last_call_trend = self.last_put_trend = None
        self.last_future_oi_now = self.last_future_oi_anchor = self.last_future_oi_pct = None
        self.last_call_oi_now = self.last_call_oi_anchor = self.last_call_oi_pct = None
        self.last_put_oi_now = self.last_put_oi_anchor = self.last_put_oi_pct = None
        self.shifted_strike = {}
        self.remarks = deque(maxlen=30)
        self.volatile_watch = {}
        self.reentry_poc_anchor = None

    def add_remark(self, text: str, ts: float = 0.0) -> None:
        """Append one human-readable line to the client-visible trail."""
        import time as _t
        self.remarks.append({"ts": ts or _t.time(), "text": text})

    # ── Volatile-regime conditional hedge watch + POC re-entry gate ───────
    # (PDF decision-matrix rows 9/18/27 -- the three Volatile rows, 2026-10-11)
    def arm_volatile_watch(self, ce_entry_premium: float, pe_entry_premium: float,
                            ce_hedge, pe_hedge, now_ts: float, window_sec: float = 300.0) -> None:
        """Called right after a Volatile-regime full close. Arms a 5-minute
        watch per side (CE watches for a VAH break -> Call hedge; PE watches
        for a VAL break -> Put hedge) instead of buying either hedge
        immediately, and records the POC at this moment as the re-entry
        gate's anchor -- normal re-entry stays blocked until the POC
        genuinely moves off this value (a "new POC formation"). ce_hedge/
        pe_hedge are the DecisionResult's own HedgeSpend objects, carried
        through since `dr` itself is out of scope by the time a window
        resolves (could be several minutes later)."""
        self.volatile_watch = {
            "CE": {"level": self.last_vah, "deadline_ts": now_ts + window_sec,
                   "entry_premium": ce_entry_premium, "hedge": ce_hedge},
            "PE": {"level": self.last_val, "deadline_ts": now_ts + window_sec,
                   "entry_premium": pe_entry_premium, "hedge": pe_hedge},
        }
        self.reentry_poc_anchor = self.last_poc

    def check_volatile_hedge_triggers(self, spot: float, now_ts: float) -> list:
        """Call every cycle while volatile_watch is non-empty. Returns a list
        of (side, "break" | "timeout", entry_premium, hedge_spec) for every
        side whose window just resolved this call -- "break" means the
        caller should buy that side's hedge now (VAH/VAL genuinely broken
        within the 5-minute window); "timeout" means the window elapsed
        with no break, so per spec NO hedge is bought for that side. Each
        resolved side is removed from volatile_watch so it's only reported
        once."""
        if spot <= 0:
            return []
        resolved = []
        for side in list(self.volatile_watch.keys()):
            w = self.volatile_watch[side]
            level = w.get("level")
            broke = (level is not None) and (
                (spot > level) if side == "CE" else (spot < level)
            )
            if broke:
                resolved.append((side, "break", w["entry_premium"], w["hedge"]))
                del self.volatile_watch[side]
            elif now_ts >= w["deadline_ts"]:
                resolved.append((side, "timeout", w["entry_premium"], w["hedge"]))
                del self.volatile_watch[side]
        return resolved

    def reentry_blocked_for_new_poc(self) -> bool:
        """True while normal re-entry must stay blocked per the Volatile
        rows' own Re-entry Rule ("wait for a new POC formation"). Clears
        itself (returns False from then on) the first time the live POC is
        observed to differ from the anchor recorded at exit time -- a
        genuinely NEW POC has formed, not just a reading of the old one."""
        if self.reentry_poc_anchor is None:
            return False
        if self.last_poc is not None and self.last_poc != self.reentry_poc_anchor:
            self.reentry_poc_anchor = None
            return False
        return True

    def in_value_area(self, price: float) -> bool:
        """"Exit immediately if price re-enters the Value Area" -- the
        Volatile rows' own fakeout-exit clause for a naked long bought off
        the conditional hedge watch above."""
        if self.last_snapshot is None or price <= 0:
            return False
        return self.last_snapshot.location(price) == "inside_va"

    # ── Feed methods (called by the engine as ticks/bars arrive) ──────────
    def on_futures_bar(self, high: float, low: float, volume: float, oi: float, ts: float) -> None:
        self._vp.add_bar(high, low, volume)
        self._rollover.update_near(ts, oi or 0.0, volume or 0.0)

    def on_futures_next_month_bar(self, volume: float, oi: float, ts: float) -> None:
        self._rollover.update_next(ts, oi or 0.0, volume or 0.0)

    def on_option_oi_tick(self, strike: int, side: str, oi: float, ts: float) -> None:
        self._oi.update_tick(strike, side, oi, ts)

    def seed_volume_profile_bar(self, high: float, low: float, volume: float) -> None:
        """REST-seed ONLY the Volume Profile from today's real historical
        high/low/volume (e.g. on a mid-day restart) -- deliberately does
        NOT touch the futures rollover tracker via on_futures_bar, since
        Upstox's historical 1-min candle API hardcodes OI to 0 at this
        granularity; feeding fake zero-OI readings into the rollover
        tracker would corrupt its state, not just leave it unwarmed. The
        OI regime tracker (option strike OI) has no historical source at
        all and always rebuilds from live ticks forward -- same structural
        gap already documented for OI-Flow/Liquidity Trap in this codebase."""
        self._vp.add_bar(high, low, volume)

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
        self.last_snapshot = snap
        if snap:
            self.last_poc, self.last_vah, self.last_val = snap.poc, snap.vah, snap.val

        roll_state = self._rollover.classify(now_ts=now_ts, is_expiry_week=self.is_expiry_week)
        oi_res = self._oi.classify(spot=spot, minute_ts=now_ts, rollover_active=roll_state.rollover_detected)
        # 2026-10-09, direct user ask: expose the raw Rise/Fall/No Change
        # inputs the regime label is built from -- previously computed here
        # and discarded right after feeding decide(), so the UI could only
        # ever show the final conclusion, never "why" (the log heartbeat in
        # exits.py was the only place these ever became visible, and only
        # as text in a log file, not in the dashboard). Current/anchor/%-vs-
        # anchor were already real fields on RolloverState/OiRegimeResult
        # (not computed here) -- just never threaded past evaluate() before.
        self.last_future_trend = roll_state.future_oi_trend
        self.last_future_oi_now = roll_state.near_oi
        self.last_future_oi_anchor = roll_state.future_oi_anchor
        self.last_future_oi_pct = roll_state.future_oi_pct_vs_anchor
        if oi_res is None:
            self.last_decision = None
            self.last_call_trend = self.last_put_trend = None
            self.last_call_oi_now = self.last_put_oi_now = None
            self.last_call_oi_anchor = self.last_put_oi_anchor = None
            self.last_call_oi_pct = self.last_put_oi_pct = None
            return None

        self.last_call_trend = oi_res.call_trend
        self.last_put_trend = oi_res.put_trend
        self.last_call_oi_now = oi_res.call_total_now
        self.last_put_oi_now = oi_res.put_total_now
        self.last_call_oi_anchor = oi_res.call_anchor_oi
        self.last_put_oi_anchor = oi_res.put_anchor_oi
        self.last_call_oi_pct = oi_res.call_pct_vs_anchor
        self.last_put_oi_pct = oi_res.put_pct_vs_anchor
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
            "future_oi_trend": self.last_future_trend,
            "call_oi_trend": self.last_call_trend,
            "put_oi_trend": self.last_put_trend,
            # 2026-10-09, direct user ask: previous (anchor) OI, current OI,
            # the %-move between them, and the threshold that %-move is
            # judged against -- the "why" behind the trend labels above.
            # 2026-10-13 fix, real user-found gap: this used to be a hardcoded
            # literal 3.0, disconnected from futures_rollover.py's own real
            # latch -- it never reflected the actual threshold in use (stayed
            # "3.00%" in the UI even after the 2026-10-12 change to 10%). Now
            # reads the real live value directly.
            "future_oi_trend_threshold_pct": self._rollover._future_oi_latch.trend_pct,
            "oi_trend_threshold_pct": self._oi.trend_pct,
            "future_oi_now": self.last_future_oi_now,
            "future_oi_anchor": self.last_future_oi_anchor,
            "future_oi_pct": self.last_future_oi_pct,
            "call_oi_now": self.last_call_oi_now,
            "call_oi_anchor": self.last_call_oi_anchor,
            "call_oi_pct": self.last_call_oi_pct,
            "put_oi_now": self.last_put_oi_now,
            "put_oi_anchor": self.last_put_oi_anchor,
            "put_oi_pct": self.last_put_oi_pct,
            "regime": dr.regime if dr else None,
            "call_action": dr.call_action if dr else None,
            "put_action": dr.put_action if dr else None,
            "call_hedge": {"enabled": dr.call_hedge.enabled, "pct": dr.call_hedge.pct_of_premium} if dr else None,
            "put_hedge": {"enabled": dr.put_hedge.enabled, "pct": dr.put_hedge.pct_of_premium} if dr else None,
            "naked_state": self.naked_state,
            "naked_entry_premium": dict(self.naked_entry_premium),
            "awaiting_reentry": dict(self.awaiting_reentry),
            "volatile_watch": {s: dict(w) for s, w in self.volatile_watch.items()},
            "reentry_poc_anchor": self.reentry_poc_anchor,
            "remarks": list(self.remarks),
        }
