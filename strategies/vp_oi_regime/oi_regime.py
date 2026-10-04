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
    # 2026-10-03, direct user spec: the sticky-latch calculation shown for
    # audit/chart-cross-check purposes -- the anchor OI each side's trend is
    # currently measured against, and the raw %-move off that anchor that
    # produced (or failed to produce) the displayed trend this tick.
    call_anchor_oi: float = 0.0
    put_anchor_oi: float = 0.0
    call_pct_vs_anchor: float = 0.0
    put_pct_vs_anchor: float = 0.0


class OiRegimeTracker:
    """Per-(strike, side) OI time series over an ATM±band window, with the
    noise-floor / hard-wall / momentum-override relevance rules applied
    before computing a band-wide change-in-OI trend for each side."""

    def __init__(
        self,
        strike_step: float = 50.0,
        band: int = 5,                 # ATM +/- 5 strikes, confirmed fixed
        change_window_min: int = 5,    # 5 or 15, per spec row 2
        trend_pct: float = 3.0,
        noise_floor_pct: float = 15.0,
        hard_wall_pts: float = 50.0,
        override_pct: float = 30.0,
    ) -> None:
        if change_window_min not in (5, 15):
            raise ValueError(f"change_window_min must be 5 or 15, got {change_window_min}")
        self.strike_step = strike_step
        self.band = band
        self.change_window_min = change_window_min
        self.trend_pct = trend_pct
        self.noise_floor_pct = noise_floor_pct
        self.hard_wall_pts = hard_wall_pts
        self.override_pct = override_pct

        # (strike, side) -> deque[(minute_ts, oi)], enough history to cover
        # change_window_min plus slack for irregular tick timing.
        self._series: Dict[Tuple[int, Side], Deque[Tuple[float, float]]] = {}
        self._session_start_oi: Dict[Tuple[int, Side], float] = {}
        self._maxlen = 240  # ~4h of 1-min bars, plenty for a 15-min window

        # 2026-10-03 CRITICAL FIX, direct user spec: sticky/latched trend
        # state machine (classic hysteresis), replacing the old stateless
        # "compare to N-minutes-ago" calc. A fixed reference ANCHOR is kept
        # per side; the trend only updates (and the anchor only moves) when
        # the current band total crosses +/-trend_pct away from that anchor.
        # A reading that falls back inside the band does NOT reset the
        # trend to "No Change" -- it holds whatever was last confirmed
        # (Rise/Fall/No Change), against the SAME anchor, until a genuine
        # new crossing happens. This also makes the trend self-paced on its
        # OWN change_window_min cadence regardless of how often classify()
        # itself is called (e.g. every real tick in production) -- a new
        # evaluation only actually happens once that many minutes have
        # elapsed since the side's own last evaluation.
        self._latches: Dict[Side, StickyTrendLatch] = {
            "CE": StickyTrendLatch(trend_pct=trend_pct, eval_window_min=change_window_min),
            "PE": StickyTrendLatch(trend_pct=trend_pct, eval_window_min=change_window_min),
        }

    def reset(self) -> None:
        self._series.clear()
        self._session_start_oi.clear()
        for latch in self._latches.values():
            latch.reset()

    def _update_latch(self, side: Side, now_total: float, minute_ts: float) -> Tuple[str, float, float]:
        """Returns (trend, pct_vs_anchor, anchor_used_for_this_reading)."""
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

    def _atm(self, spot: float) -> int:
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
        OI shuffling must never be read as Long Unwinding/Short Covering."""
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
        call_trend, call_pct, call_anchor = self._update_latch("CE", call_total_now, minute_ts)
        put_trend, put_pct, put_anchor = self._update_latch("PE", put_total_now, minute_ts)

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
            call_pct_vs_anchor=call_pct, put_pct_vs_anchor=put_pct,
        )
