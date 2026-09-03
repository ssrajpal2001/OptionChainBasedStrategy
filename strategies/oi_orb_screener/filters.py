"""
strategies/oi_orb_screener/filters.py — five independently-toggleable,
independently-logged signal filters, additive to the existing OI-Spurt +
price-move shortlist and ORB/regime entry logic (that core logic is
UNCHANGED by this module).

2026-08-25, direct user spec: real incident where SAIL fired a CALL
breakout right underneath a large Call-OI wall (writers defending a strike
just above entry) -- a classic false-breakout setup the existing shortlist/
ORB logic has no visibility into (it only ever looks at aggregate OI-Spurt%
and price move, never WHERE OI actually sits across the strike chain).

Design, per direct user instruction: build all five, run them in parallel
on every real signal, each with its OWN dedicated log file so a scenario
can be reviewed filter-by-filter after the fact. Each filter's own
enable-flag controls whether its verdict actually BLOCKS a trade -- when
disabled (default), it still evaluates and logs on every signal, it just
never stops entry. This mirrors the same soft/telemetry-first discipline
already used elsewhere in this codebase (OI-Flow's own PCR/volume-spike
signals) for a filter with zero forward validation yet.

Every function here is pure -- no I/O, no logging, no strategy state --
so each can be unit-tested against hand-built inputs exactly like
screener.py's own functions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass
class FilterVerdict:
    """Uniform result shape for every filter below -- what the admin config
    panel/log line needs regardless of which specific filter produced it."""
    name: str
    available: bool          # False = insufficient data to evaluate at all (never a block)
    passed: bool              # True = filter has no objection; only meaningful when available
    reason: str
    numbers: dict = field(default_factory=dict)   # filter-specific values for the log line

    def blocks(self, enabled: bool) -> bool:
        """Whether this filter should actually stop the trade, given its own
        enable flag. Never blocks when data was unavailable -- an untested/
        cold filter must fail OPEN, not silently kill a real signal."""
        return bool(enabled) and self.available and not self.passed


# ── 1. OI-wall presence ──────────────────────────────────────────────────

def evaluate_oi_wall(snap, side: str, entry_strike: float,
                      dominance_ratio: float = 1.5) -> FilterVerdict:
    """Is there a meaningfully dominant OI concentration on the side that
    would resist this trade (Call OI above a CALL entry, Put OI below a PUT
    entry)? `snap` is a matrix_engine.option_matrix.ChainSnapshot (or
    anything exposing the same .rows/.strikes()/.call_oi_at()/.put_oi_at()
    interface). dominance_ratio: the wall strike's OI must be at least this
    many times the AVERAGE OI across the rest of the tracked strikes to
    count as a genuine wall, not just noise."""
    name = "oi_wall"
    if snap is None or not getattr(snap, "rows", None):
        return FilterVerdict(name, available=False, passed=True, reason="no chain data yet")

    strikes = snap.strikes()
    unfavorable_strikes = [s for s in strikes if s > entry_strike] if side == "CALL" \
        else [s for s in strikes if s < entry_strike]
    if not unfavorable_strikes:
        return FilterVerdict(name, available=False, passed=True,
                              reason="no strikes tracked on the resistance side yet")

    oi_at = snap.call_oi_at if side == "CALL" else snap.put_oi_at
    oi_by_strike = {s: oi_at(s) for s in unfavorable_strikes}
    wall_strike = max(oi_by_strike, key=oi_by_strike.get)
    wall_oi = oi_by_strike[wall_strike]
    others = [v for s, v in oi_by_strike.items() if s != wall_strike]
    avg_other = (sum(others) / len(others)) if others else 0.0

    if wall_oi <= 0:
        return FilterVerdict(name, available=False, passed=True, reason="no OI data on tracked strikes yet")

    is_wall = avg_other <= 0 or (wall_oi / max(avg_other, 1.0)) >= dominance_ratio
    reason = (f"{side} wall at {wall_strike:.0f} (OI={wall_oi:,}, {wall_oi / max(avg_other, 1.0):.1f}x avg)"
              if is_wall else f"no dominant wall found (max {wall_strike:.0f} OI={wall_oi:,})")
    return FilterVerdict(
        name, available=True, passed=not is_wall, reason=reason,
        numbers={"wall_strike": wall_strike, "wall_oi": wall_oi, "avg_other_oi": round(avg_other, 1)},
    )


# ── 2. Distance to wall ──────────────────────────────────────────────────

def evaluate_distance_to_wall(snap, side: str, entry_strike: float,
                               min_distance_pct: float = 1.5) -> FilterVerdict:
    """Quantified companion to evaluate_oi_wall: regardless of whether the
    OI concentration counts as a 'dominant wall', how close is the single
    largest-OI strike on the resistance side to the entry strike, as a %
    of entry_strike? Blocks if that distance is under min_distance_pct --
    a wall right on top of the entry is dangerous even if it's not hugely
    dominant vs. the rest of the chain."""
    name = "distance_to_wall"
    if snap is None or not getattr(snap, "rows", None):
        return FilterVerdict(name, available=False, passed=True, reason="no chain data yet")

    strikes = snap.strikes()
    unfavorable_strikes = [s for s in strikes if s > entry_strike] if side == "CALL" \
        else [s for s in strikes if s < entry_strike]
    if not unfavorable_strikes:
        return FilterVerdict(name, available=False, passed=True,
                              reason="no strikes tracked on the resistance side yet")

    oi_at = snap.call_oi_at if side == "CALL" else snap.put_oi_at
    oi_by_strike = {s: oi_at(s) for s in unfavorable_strikes}
    if not any(oi_by_strike.values()):
        return FilterVerdict(name, available=False, passed=True, reason="no OI data on tracked strikes yet")

    wall_strike = max(oi_by_strike, key=oi_by_strike.get)
    if entry_strike <= 0:
        return FilterVerdict(name, available=False, passed=True, reason="invalid entry_strike")
    distance_pct = abs(wall_strike - entry_strike) / entry_strike * 100.0
    too_close = distance_pct < min_distance_pct
    reason = f"nearest OI wall {wall_strike:.0f} is {distance_pct:.2f}% from entry {entry_strike:.0f}"
    return FilterVerdict(
        name, available=True, passed=not too_close, reason=reason,
        numbers={"wall_strike": wall_strike, "distance_pct": round(distance_pct, 3)},
    )


# ── 3. PCR (Put-Call Ratio) gate ─────────────────────────────────────────

def evaluate_pcr(pcr: Optional[float], side: str,
                  max_pcr_for_call: float = 1.2, min_pcr_for_put: float = 0.8) -> FilterVerdict:
    """A CALL entry wants the chain NOT already put-heavy (pcr below
    max_pcr_for_call); a PUT entry wants it NOT already call-heavy (pcr
    above min_pcr_for_put). Mirrors OI-Flow's own PCR-band gate concept,
    reimplemented fresh here per this strategy's standalone mandate."""
    name = "pcr"
    if pcr is None or pcr <= 0:
        return FilterVerdict(name, available=False, passed=True, reason="no PCR data yet")

    if side == "CALL":
        ok = pcr <= max_pcr_for_call
        reason = f"PCR {pcr:.2f} {'<=' if ok else '>'} {max_pcr_for_call} (CALL threshold)"
    else:
        ok = pcr >= min_pcr_for_put
        reason = f"PCR {pcr:.2f} {'>=' if ok else '<'} {min_pcr_for_put} (PUT threshold)"
    return FilterVerdict(name, available=True, passed=ok, reason=reason, numbers={"pcr": round(pcr, 3)})


# ── 4. Volume confirmation ───────────────────────────────────────────────

def evaluate_volume_confirmation(recent_volume: Optional[float], trailing_avg_volume: Optional[float],
                                  min_ratio: float = 1.5) -> FilterVerdict:
    """Does the breakout candle's own volume actually confirm the move, or
    is this a thin/false push? recent_volume/trailing_avg_volume are both
    VOLUME DELTAS over comparable windows (not cumulative session totals --
    caller is responsible for that subtraction, same technique OI-Flow's
    own BarAccumulator already uses for change-in-cumulative-volume)."""
    name = "volume_confirmation"
    if recent_volume is None or trailing_avg_volume is None or trailing_avg_volume <= 0:
        return FilterVerdict(name, available=False, passed=True, reason="insufficient volume history yet")

    ratio = recent_volume / trailing_avg_volume
    ok = ratio >= min_ratio
    reason = f"volume ratio {ratio:.2f}x trailing avg ({'>=' if ok else '<'} {min_ratio}x required)"
    return FilterVerdict(name, available=True, passed=ok, reason=reason,
                          numbers={"recent_volume": recent_volume, "trailing_avg_volume": trailing_avg_volume,
                                   "ratio": round(ratio, 3)})


# ── 5. OI rate-of-change ─────────────────────────────────────────────────

def evaluate_oi_roc(oi_history: List[Tuple[float, float]], min_roc_pct: float = 3.0,
                     lookback_sec: float = 300.0, now_ts: Optional[float] = None) -> FilterVerdict:
    """ADDITIVE to the existing static daily OI-Spurt% filter, never a
    replacement -- that filter stays exactly as-is. This measures whether
    OI is building RIGHT NOW (fresh, over the last lookback_sec) rather than
    having accumulated gradually all week and simply crossing the static
    daily threshold on a stale trend. oi_history: [(unix_ts, oi_value), ...]
    ordered oldest-first, same shape every other rolling-history structure
    in this codebase already uses."""
    name = "oi_roc"
    if not oi_history or len(oi_history) < 2:
        return FilterVerdict(name, available=False, passed=True, reason="insufficient OI history yet")

    now_ts = now_ts if now_ts is not None else oi_history[-1][0]
    cutoff = now_ts - lookback_sec
    window = [(t, v) for t, v in oi_history if t >= cutoff]
    if len(window) < 2:
        return FilterVerdict(name, available=False, passed=True,
                              reason=f"fewer than 2 OI samples in the last {lookback_sec:.0f}s")

    start_oi = window[0][1]
    end_oi = window[-1][1]
    if start_oi <= 0:
        return FilterVerdict(name, available=False, passed=True, reason="starting OI reading was zero")

    roc_pct = (end_oi - start_oi) / start_oi * 100.0
    ok = roc_pct >= min_roc_pct
    reason = f"OI ROC {roc_pct:+.2f}% over last {lookback_sec:.0f}s ({'>=' if ok else '<'} {min_roc_pct}% required)"
    return FilterVerdict(name, available=True, passed=ok, reason=reason,
                          numbers={"start_oi": start_oi, "end_oi": end_oi, "roc_pct": round(roc_pct, 3)})
