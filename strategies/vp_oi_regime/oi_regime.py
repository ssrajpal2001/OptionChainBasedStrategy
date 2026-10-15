"""OI regime classifier -- spec rows 2-5 (OI timeframe/strike-band params +
Absolute-vs-Change-OI relevance Rules 1/2/3). Generalizes the scratchpad
prototype (oi_regime_phase1.py, validated this session against the real
2026-10-01 incident) into a reusable, live-capable class.

Live use: feed every real OPTION_TICK's (strike, side, oi) via update_tick();
classify() can be called at any later instant. Backtest use: the same class,
fed historical 1-min closes in a loop (see the backtest harness) -- batch and
live share one implementation, per this codebase's own
feedback_backtest_drive_real_class lesson.

Does NOT depend on matrix_engine.option_matrix.ChainSnapshot -- that engine's
own recompute() is an unsmoothed instantaneous max() that is documented to
jitter between near-tied strikes (the same wall-jitter bug that starved the
old OI-Flow strategy for days). This tracker keeps its own per-strike OI
history instead.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from strategies.vp_oi_regime.sticky_latch import StickyTrendLatch

Side = str  # "CE" or "PE"


@dataclass
class OiRegimeResult:
    minute_ts: float
    spot: float
    atm: int
    strikes: List[int]
    call_wall: int
    put_wall: int
    call_peak_oi: float
    put_peak_oi: float
    call_total_now: float
    put_total_now: float
    call_trend: str   # "Rise" | "Fall" | "No Change"
    put_trend: str
    wall_notes: List[str]
    rollover_suppressed: bool = False
    # 2026-10-03, REVISED 2026-10-15 for the dual high/low-anchor design:
    # the sticky-latch calculation shown for audit/chart-cross-check
    # purposes -- the HIGH anchor (session-wide running peak) each side's
    # Fall check is measured against, the LOW anchor (session-wide running
    # trough) the Rise check is measured against, and the raw %-move off
    # each that produced (or failed to produce) the displayed trend.
    call_anchor_oi: float = 0.0        # HIGH anchor
    put_anchor_oi: float = 0.0         # HIGH anchor
    call_pct_vs_anchor: float = 0.0    # % vs HIGH anchor (Fall check)
    put_pct_vs_anchor: float = 0.0     # % vs HIGH anchor (Fall check)
    call_low_anchor_oi: float = 0.0
    put_low_anchor_oi: float = 0.0
    call_pct_vs_low_anchor: float = 0.0    # % vs LOW anchor (Rise check)
    put_pct_vs_low_anchor: float = 0.0     # % vs LOW anchor (Rise check)


class OiRegimeTracker:
    """Per-(strike, side) OI time series over an ATM±band window, with the
    noise-floor / hard-wall / momentum-override relevance rules applied
    before computing a band-wide change-in-OI trend for each side."""

    def __init__(
        self,
        strike_step: float = 50.0,
        band: int = 5,                 # ATM +/- 5 strikes, confirmed fixed
        change_window_min: int = 5,    # 5 or 15, per spec row 2
        rise_pct: float = 20.0,
        fall_pct: float = 10.0,
        noise_floor_pct: float = 15.0,
        hard_wall_pts: float = 50.0,
        override_pct: float = 30.0,
    ) -> None:
        if change_window_min not in (5, 15):
            raise ValueError(f"change_window_min must be 5 or 15, got {change_window_min}")
        self.strike_step = strike_step
        self.band = band
        self.change_window_min = change_window_min
        self.rise_pct = rise_pct
        self.fall_pct = fall_pct
        self.noise_floor_pct = noise_floor_pct
        self.hard_wall_pts = hard_wall_pts
        self.override_pct = override_pct

        # (strike, side) -> deque[(minute_ts, oi)], enough history to cover
        # change_window_min plus slack for irregular tick timing.
        self._series: Dict[Tuple[int, Side], Deque[Tuple[float, float]]] = {}
        self._session_start_oi: Dict[Tuple[int, Side], float] = {}
        self._maxlen = 240  # ~4h of 1-min bars, plenty for a 15-min window
        # 2026-10-14, direct user spec: ATM±band frozen off the real 9:15
        # session-open price (see freeze_atm's own docstring) -- None means
        # "not frozen yet," falls back to live-spot-recentering until frozen.
        self._frozen_atm: Optional[int] = None

        # 2026-10-03, REVISED 2026-10-15 direct user spec (dual high/low
        # ratcheting-anchor design, see StickyTrendLatch's own module
        # docstring for the full mechanic): a session-wide HIGH anchor and
        # LOW anchor are kept per side. Fall fires when the current reading
        # is >= fall_pct below the high anchor; Rise fires when it's
        # >= rise_pct above the low anchor (Fall checked first, wins any
        # tie). This also makes the trend self-paced on its OWN
        # change_window_min cadence regardless of how often classify()
        # itself is called (e.g. every real tick in production) -- a new
        # evaluation only actually happens once that many minutes have
        # elapsed since the side's own last evaluation.
        self._latches: Dict[Side, StickyTrendLatch] = {
            "CE": StickyTrendLatch(rise_pct=rise_pct, fall_pct=fall_pct, eval_window_min=change_window_min),
            "PE": StickyTrendLatch(rise_pct=rise_pct, fall_pct=fall_pct, eval_window_min=change_window_min),
        }

    def reset(self) -> None:
        self._series.clear()
        self._session_start_oi.clear()
        self._frozen_atm = None
        for latch in self._latches.values():
            latch.reset()

    def seed_anchors(self, call_anchor: Optional[float], put_anchor: Optional[float], ts: float) -> None:
        """2026-10-13 direct user spec: seed the CE/PE latches' anchors from
        real recorded history (e.g. today's peak OI so far, reconstructed
        from recorder.py's DB) right after a mid-day restart, instead of
        letting them reset to whatever's live at that moment. Either side
        may be None (no usable history for it) -- left untouched."""
        if call_anchor is not None:
            self._latches["CE"].seed_anchor(call_anchor, ts)
        if put_anchor is not None:
            self._latches["PE"].seed_anchor(put_anchor, ts)

    def _update_latch(self, side: Side, now_total: float, minute_ts: float) -> Tuple[str, float, float, float, float]:
        """Returns (trend, pct_vs_high, pct_vs_low, high_anchor, low_anchor)."""
        return self._latches[side].update(now_total, minute_ts)

    def update_tick(self, strike: int, side: Side, oi: float, minute_ts: float) -> None:
        if oi < 0:
            return
        key = (int(strike), side)
        dq = self._series.setdefault(key, deque(maxlen=self._maxlen))
        if dq and dq[-1][0] == minute_ts:
            dq[-1] = (minute_ts, oi)  # same-minute update, overwrite not append
        else:
            dq.append((minute_ts, oi))
        if key not in self._session_start_oi:
            self._session_start_oi[key] = oi

    def freeze_atm(self, atm: int) -> None:
        """2026-10-14, direct user spec: the ATM±band strike SET used to
        re-center on live spot every single classify() call -- as spot
        drifted, a strike could enter/leave the band purely from that
        drift, making the Call/Put OI totals jump for a reason that has
        nothing to do with any real OI change at those strikes. Freezing
        the band once, off the real 9:15 session-open price, means every
        later OI change genuinely reflects writers acting at the SAME
        strikes all day -- never a band-shift artifact. Call once, at
        session start (fresh day) or right after a mid-day restart (seeded
        from the real 9:15 open via REST, see the caller's own docstring).
        """
        self._frozen_atm = int(atm)

    def _atm(self, spot: float) -> int:
        frozen = getattr(self, "_frozen_atm", None)
        if frozen is not None:
            return frozen
        return int(round(spot / self.strike_step) * self.strike_step)

    def _band_strikes(self, atm: int) -> List[int]:
        return [int(atm + i * self.strike_step) for i in range(-self.band, self.band + 1)]

    def _oi_now(self, strike: int, side: Side) -> Optional[float]:
        dq = self._series.get((int(strike), side))
        return dq[-1][1] if dq else None

    def _oi_at_or_before(self, strike: int, side: Side, minute_ts: float) -> Optional[float]:
        dq = self._series.get((int(strike), side))
        if not dq:
            return None
        val = None
        for ts, oi in dq:
            if ts > minute_ts:
                break
            val = oi
        return val

    def classify(
        self, spot: float, minute_ts: float, rollover_active: bool = False,
    ) -> Optional[OiRegimeResult]:
        """``rollover_active`` -- set True when FuturesRolloverTracker has
        flagged a confirmed near-month/next-month rollover in progress this
        window; suppresses a directional trend call (both sides forced to
        'No Change') per spec row 6's false-signal filter, since a rollover's
        OI shuffling must never be read as Long Unwinding/Short Covering.

        2026-10-15 CRITICAL FIX, real incident: before freeze_atm() has
        succeeded, _atm() recenters on live spot every call -- so the band
        of strikes being summed can shift strike-by-strike between two
        calls purely from spot drifting a few points, with ZERO real OI
        change. That band-shift artifact looks exactly like a huge genuine
        OI move (confirmed live: PUT OI total jumped +151% session-start,
        before any freeze had fired) and can falsely feed/poison the
        high/low anchors. Refuse to classify at all -- same as "not warm
        yet" -- until the band is genuinely frozen; the caller's own
        _seed_vp_oi_volume_profile() retries every tick until it succeeds."""
        if self._frozen_atm is None:
            return None
        atm = self._atm(spot)
        strikes = self._band_strikes(atm)

        ce_now: Dict[int, float] = {}
        pe_now: Dict[int, float] = {}
        for s in strikes:
            v = self._oi_now(s, "CE")
            if v is not None:
                ce_now[s] = v
            v = self._oi_now(s, "PE")
            if v is not None:
                pe_now[s] = v
        if not ce_now or not pe_now:
            return None

        call_wall = max(ce_now, key=ce_now.get)
        put_wall = max(pe_now, key=pe_now.get)
        call_peak = ce_now[call_wall]
        put_peak = pe_now[put_wall]

        # Rule 1: noise filter -- drop strikes < noise_floor_pct of peak
        # absolute OI before summing the band's change-in-OI.
        ce_now_f = {s: v for s, v in ce_now.items() if v >= self.noise_floor_pct / 100.0 * call_peak}
        pe_now_f = {s: v for s, v in pe_now.items() if v >= self.noise_floor_pct / 100.0 * put_peak}

        call_total_now = sum(ce_now_f.values())
        put_total_now = sum(pe_now_f.values())

        # Sticky-latch trend (2026-10-03 fix, see _update_latch's own
        # docstring) replaces the old stateless N-minutes-ago comparison.
        call_trend, call_pct_hi, call_pct_lo, call_anchor, call_low_anchor = \
            self._update_latch("CE", call_total_now, minute_ts)
        put_trend, put_pct_hi, put_pct_lo, put_anchor, put_low_anchor = \
            self._update_latch("PE", put_total_now, minute_ts)

        # Rule 2 / Rule 3: hard wall + momentum override, evaluated per side
        # for whichever wall spot sits within hard_wall_pts of. These still
        # compare the WALL STRIKE's own absolute OI against its value
        # change_window_min minutes ago (a single-strike structural check,
        # deliberately independent of the band-wide sticky latch above).
        prev_ts = minute_ts - self.change_window_min * 60.0
        ce_prev = {s: self._oi_at_or_before(s, "CE", prev_ts) for s in strikes}
        pe_prev = {s: self._oi_at_or_before(s, "PE", prev_ts) for s in strikes}
        wall_notes: List[str] = []
        for side, wall_strike, now_map, prev_map, trend_name in (
            ("CALL", call_wall, ce_now, ce_prev, "call_trend"),
            ("PUT", put_wall, pe_now, pe_prev, "put_trend"),
        ):
            if abs(spot - wall_strike) > self.hard_wall_pts:
                continue
            prev_wall_oi = prev_map.get(wall_strike)
            now_wall_oi = now_map.get(wall_strike, 0.0)
            if prev_wall_oi and prev_wall_oi > 0:
                wall_chg_pct = (now_wall_oi - prev_wall_oi) / prev_wall_oi * 100.0
            else:
                wall_chg_pct = 0.0
            if abs(wall_chg_pct) < self.override_pct:
                wall_notes.append(
                    f"near {side} wall {wall_strike} (spot={spot:.0f}), "
                    f"change={wall_chg_pct:+.1f}% < {self.override_pct}% -> WALL HOLDS "
                    f"(raw change-in-OI trend suppressed, structural wall dictates)"
                )
                if side == "CALL":
                    call_trend = "No Change"
                else:
                    put_trend = "No Change"
            else:
                wall_notes.append(
                    f"near {side} wall {wall_strike}, change={wall_chg_pct:+.1f}% "
                    f">= {self.override_pct}% -> WALL BREACH OVERRIDE (momentum wins)"
                )

        rollover_suppressed = False
        if rollover_active:
            call_trend = "No Change"
            put_trend = "No Change"
            rollover_suppressed = True
            wall_notes.append(
                "ROLLOVER ACTIVE -- near/next-month futures OI shift in progress, "
                "directional trend suppressed per spec row 6 false-signal filter."
            )

        return OiRegimeResult(
            minute_ts=minute_ts, spot=spot, atm=atm, strikes=strikes,
            call_wall=call_wall, put_wall=put_wall,
            call_peak_oi=call_peak, put_peak_oi=put_peak,
            call_total_now=call_total_now, put_total_now=put_total_now,
            call_trend=call_trend, put_trend=put_trend,
            wall_notes=wall_notes, rollover_suppressed=rollover_suppressed,
            call_anchor_oi=call_anchor, put_anchor_oi=put_anchor,
            call_pct_vs_anchor=call_pct_hi, put_pct_vs_anchor=put_pct_hi,
            call_low_anchor_oi=call_low_anchor, put_low_anchor_oi=put_low_anchor,
            call_pct_vs_low_anchor=call_pct_lo, put_pct_vs_low_anchor=put_pct_lo,
        )
