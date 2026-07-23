"""
strategies/v4_cascade/rolling_base.py — pure trap zone detection.

CORRECTED mechanics (per direct chart validation, 2026-07-18 session):

  ref candle:      entry_line = ref.low   (bears'/sellers' entry — a support
                                            break level)
                    sl_level   = ref.high  (bears' stop-loss level)

  SELLERS_IN:       the first later candle whose low < entry_line confirms
                     sellers are in (a support break). Not itself reported as
                     a separate event — we only ever report confirmed traps.

  TRAPPED (confirmed trap — the only state this module reports):
                     scanning FORWARD from the sellers-in candle, with NO
                     bar-count cap — the wait is bounded only by however much
                     history is available at this multiplier (the ladder's
                     lookback window) — the FIRST later candle whose
                     high > sl_level confirms the trap. The zone is then
                     [entry_line, sweep_low] where sweep_low is the lowest low
                     reached across the whole sellers-in -> trapped-
                     confirmation span. If the SL never clears anywhere in
                     that multiplier's resampled history, the ladder climbs
                     to the next multiplier (150m, 225m, ...) and
                     re-evaluates the identical 3-part sequence there. Until
                     TRAPPED confirms, the ref/sellers-in pair is not
                     reported as any kind of "pending" state — this module
                     only ever surfaces confirmed traps.

  Only after TRAPPED confirms does entry_line become live as the retest
  trigger level (a further candle's low piercing entry_line, checked
  elsewhere in zone_state.py's check_pierce/entries.py).

  Symmetric "bull" version (buyers trapped — bearish read, PE side):
    ref.high = entry_line (buyers' entry, a resistance break)
    ref.low  = sl_level    (buyers' stop-loss level)
    BUYERS_IN: first later candle whose high > entry_line
    TRAPPED: first later candle (no cap) whose low < sl_level

Strict first-touch / mitigation rule: a confirmed TRAPPED zone is only a live
opportunity if entry_line has NOT already been re-touched by any bar strictly
between the TRAPPED-confirmation candle and the most recent bar in the series
(the most recent bar itself is excluded — a touch there IS the live trigger,
not disqualifying history). A mitigated zone is rejected and the scan
continues backward for an OLDER, still-unmitigated ref/trap pair before
concluding nothing valid exists at this timeframe multiplier.

No bus/broker/DB/asyncio dependency — pure functions over plain bar objects
exposing .low/.high/.close/.timestamp (works with CandleEvent or any duck-typed
bar with those four attributes).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Protocol, Tuple

from strategies.v4_cascade.dataclasses import RollingBaseZone


class _Bar(Protocol):
    low: float
    high: float
    close: float
    timestamp: datetime


@dataclass(frozen=True)
class LadderMatch:
    """A confirmed TRAPPED zone found at a specific timeframe multiplier."""
    multiplier: int
    zone: RollingBaseZone
    ref_ts: datetime
    reclaim_ts: datetime   # the TRAPPED-confirmation candle's timestamp


def _is_mitigated_bear(bars: List[_Bar], entry_line: float, sweep_low: float, trapped_idx: int) -> bool:
    """True if any bar strictly after the TRAPPED-confirmation candle, up to
    (but NOT including) the most recent bar in ``bars``, CLOSED through the
    FAR edge of the zone (min(entry_line, sweep_low) -- i.e. back past the
    original sweep extreme) again.

    Checks the zone's LOW, not entry_line alone: entry_line is the TOP of
    the zone (the level that got reclaimed) -- a close back down near/below
    entry_line is a completely normal, expected RETEST, not a failure; it's
    the exact behavior "waiting for zone entry" is watching for. Only a
    close all the way through the zone's actual bottom (the original sweep
    low) means the reclaim has structurally failed. Confirmed against a
    real PE 24400 example: 07-20 13:00 closed at 173.90 -- well below
    entry_line (210.05, a normal retest) but still comfortably above the
    true zone low (153.85, the original sweep) -- so this must NOT mitigate.

    Also checks .close, not .low: a wick alone is a liquidity sweep (the
    SIGNAL this whole strategy trades), not a confirmed breakdown."""
    zone_low = min(entry_line, sweep_low)
    for j in range(trapped_idx + 1, len(bars) - 1):
        if bars[j].close <= zone_low:
            return True
    return False


def _is_mitigated_bull(bars: List[_Bar], entry_line: float, sweep_high: float, trapped_idx: int) -> bool:
    zone_high = max(entry_line, sweep_high)
    for j in range(trapped_idx + 1, len(bars) - 1):
        if bars[j].close >= zone_high:
            return True
    return False


def find_bear_zone(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> Optional[RollingBaseZone]:
    """Scan ``bars`` (ascending by time) for a confirmed BEAR-side trap
    (bears/sellers trapped — bullish read). Tries ref candidates NEWEST
    FIRST; for each, scans forward for sellers-in then a later SL-clear
    (TRAPPED) with NO bar-count cap — the wait is bounded only by however
    much history is in ``bars`` (i.e. the ladder multiplier's lookback
    window). Until the SL clears, the ref/sellers-in pair is not reported as
    any kind of state — we only ever report a CONFIRMED (SL-hit) trap.
    Rejects the match if already mitigated and keeps walking backward for an
    older, still-unmitigated ref/trap pair. ``skip_before_ts``: ignore any
    TRAPPED confirmation at or before this timestamp — used to avoid
    re-triggering on an already-consumed setup.

    2026-07-22: this is a genuine THREE-candle pattern (ref, sweep, reclaim)
    -- the reclaim (TRAPPED) must land on a candle STRICTLY AFTER the sweep
    candle, never the sweep candle itself. Confirmed live: a single unusually
    wide candle that both broke below ref.low AND above ref.high within its
    own range was being accepted as a valid trap -- collapsing the pattern
    to 2 candles and producing a trap off one violent, low-quality bar
    instead of a genuine multi-candle structure."""
    n = len(bars)
    for i in range(n - 2, -1, -1):
        ref = bars[i]

        sellers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].low < ref.low:
                sellers_in_idx = j
                break
        if sellers_in_idx is None:
            continue

        trapped_idx: Optional[int] = None
        sweep_low = bars[sellers_in_idx].low
        sweep_started_ts = bars[sellers_in_idx].timestamp
        for k in range(sellers_in_idx + 1, n):
            sweep_low = min(sweep_low, bars[k].low)
            if bars[k].high > ref.high:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue  # SL not (yet) cleared anywhere in the available history

        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue

        entry_line = ref.low
        if _is_mitigated_bear(bars, entry_line, sweep_low, trapped_idx=trapped_idx):
            continue

        return RollingBaseZone(
            reference_low=ref.low, reference_low_ts=ref.timestamp,
            prev_close=ref.close,
            swept=True, sweep_low=sweep_low, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - sellers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.high,
        )
    return None


def find_bull_zone(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> Optional[RollingBaseZone]:
    """Symmetric BULL-side trap (buyers trapped — bearish read). ref.high is
    the entry_line (resistance break), ref.low is the sl_level (buyers' SL);
    TRAPPED confirms on a later candle's low clearing sl_level, with NO
    bar-count cap (bounded only by the available history).

    2026-07-22: three-candle pattern (ref, sweep, reclaim) -- see
    find_bear_zone's docstring; the reclaim must land strictly after the
    sweep candle, never on it."""
    n = len(bars)
    for i in range(n - 2, -1, -1):
        ref = bars[i]

        buyers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].high > ref.high:
                buyers_in_idx = j
                break
        if buyers_in_idx is None:
            continue

        trapped_idx: Optional[int] = None
        sweep_high = bars[buyers_in_idx].high
        sweep_started_ts = bars[buyers_in_idx].timestamp
        for k in range(buyers_in_idx + 1, n):
            sweep_high = max(sweep_high, bars[k].high)
            if bars[k].low < ref.low:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue

        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue

        entry_line = ref.high
        if _is_mitigated_bull(bars, entry_line, sweep_high, trapped_idx=trapped_idx):
            continue

        return RollingBaseZone(
            reference_low=ref.high, reference_low_ts=ref.timestamp,
            prev_close=ref.close,
            swept=True, sweep_low=sweep_high, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - buyers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.low,
        )
    return None


def find_all_bear_zones(
    bars: List[_Bar], known_ref_ts: Optional[set] = None,
) -> List[RollingBaseZone]:
    """Enumerate EVERY confirmed bear-side (sweep+reclaim) zone in ``bars``,
    not just the newest one returned by find_bear_zone (that function's
    newest-first, return-on-first-match design is for the single-zone use
    case; this is for a multi-zone POOL, where several candidate zones may
    be concurrently valid). Same 3-candle rule (ref/sweep/reclaim strictly
    distinct candles) and the same mitigation check as find_bear_zone.
    ``known_ref_ts``: ref timestamps already handled (added to a pool, or
    already removed from it) -- never re-considered, so a zone that ages
    out or breaks is gone for good, not rediscovered next bar."""
    known_ref_ts = known_ref_ts or set()
    n = len(bars)
    found: List[RollingBaseZone] = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        if ref.timestamp in known_ref_ts:
            continue
        sellers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].low < ref.low:
                sellers_in_idx = j
                break
        if sellers_in_idx is None:
            continue
        trapped_idx: Optional[int] = None
        sweep_low = bars[sellers_in_idx].low
        sweep_started_ts = bars[sellers_in_idx].timestamp
        for k in range(sellers_in_idx + 1, n):
            sweep_low = min(sweep_low, bars[k].low)
            if bars[k].high > ref.high:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        entry_line = ref.low
        if _is_mitigated_bear(bars, entry_line, sweep_low, trapped_idx=trapped_idx):
            continue
        found.append(RollingBaseZone(
            reference_low=ref.low, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=sweep_low, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - sellers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.high,
        ))
    return found


def find_all_bull_zones(
    bars: List[_Bar], known_ref_ts: Optional[set] = None,
) -> List[RollingBaseZone]:
    """Symmetric to find_all_bear_zones -- buyers trapped (bearish read)."""
    known_ref_ts = known_ref_ts or set()
    n = len(bars)
    found: List[RollingBaseZone] = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        if ref.timestamp in known_ref_ts:
            continue
        buyers_in_idx: Optional[int] = None
        for j in range(i + 1, n):
            if bars[j].high > ref.high:
                buyers_in_idx = j
                break
        if buyers_in_idx is None:
            continue
        trapped_idx: Optional[int] = None
        sweep_high = bars[buyers_in_idx].high
        sweep_started_ts = bars[buyers_in_idx].timestamp
        for k in range(buyers_in_idx + 1, n):
            sweep_high = max(sweep_high, bars[k].high)
            if bars[k].low < ref.low:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        entry_line = ref.high
        if _is_mitigated_bull(bars, entry_line, sweep_high, trapped_idx=trapped_idx):
            continue
        found.append(RollingBaseZone(
            reference_low=ref.high, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=sweep_high, sweep_started_ts=sweep_started_ts,
            bars_since_sweep=trapped_idx - buyers_in_idx,
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.low,
        ))
    return found


def find_bear_trap_2candle(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> Optional[RollingBaseZone]:
    """2026-07-19 3-gate premium funnel variant: same ref/sellers-in/TRAPPED
    mechanics as ``find_bear_zone`` (TRAPPED = unbounded-lookback SL-clear,
    mitigation-checked, newest-ref-first backward search) EXCEPT the zone
    WIDTH (``sweep_low``) is pinned to the immediate next candle's low only —
    NOT the minimum low across the whole sellers-in-to-TRAPPED span. Used for
    BOTH Gate 1 (75m HTF lock) and Gate 2 (5m/15m Inner Zone lock) in
    zone_state.py's ``PremiumGateScanner`` — same function, different bar
    series fed in. Both CE and PE tracking-contract premium charts use ONLY
    this bear-trap pattern (option short-sellers trapped as premium spikes
    above their structural high) — bull traps are never scanned on premium
    charts; ``find_bull_zone`` stays reserved for spot_confirm.py's
    dual-direction bias classification on the NIFTY spot chart."""
    n = len(bars)
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        next_bar = bars[i + 1]
        if next_bar.low >= ref.low:
            continue  # no sweep on the immediate next candle

        trapped_idx: Optional[int] = None
        for k in range(i + 1, n):
            if bars[k].high > ref.high:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue  # SL not (yet) cleared anywhere in the available history

        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue

        entry_line = ref.low
        if _is_mitigated_bear(bars, entry_line, next_bar.low, trapped_idx=trapped_idx):
            continue

        return RollingBaseZone(
            reference_low=ref.low, reference_low_ts=ref.timestamp,
            prev_close=ref.close,
            swept=True, sweep_low=next_bar.low, sweep_started_ts=next_bar.timestamp,
            bars_since_sweep=trapped_idx - (i + 1),
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.high,
        )
    return None


def find_all_bear_traps_2candle(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> List[RollingBaseZone]:
    """Enumerate ALL confirmed, price-level-independent 2-candle bear-trap
    zones in ``bars`` — not just the single newest one like
    ``find_bear_trap_2candle``. Validated 2026-07-19 against real chart data
    (the manual multi-zone cross-check earlier this session): real markets
    have multiple genuinely concurrent trap structures at different price
    levels, and a single-zone-per-side scanner misses almost all of them.

    A ref is excluded as redundant ONLY if it falls inside an already-kept,
    OLDER (smaller ref index) zone's ref-to-next-candle formation span AT A
    LESS-EXTREME-OR-EQUAL price — i.e. it's genuinely an interior candle of
    the SAME structure, not an independent zone at a different price level
    that merely overlaps in time with another. This is the price-level-aware
    dedup that fixed the "many real zones silently hidden" bug found this
    session — a naive time-window-only dedup wrongly swallows independent
    zones whenever an older ref takes many bars to sweep."""
    n = len(bars)
    raw = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        next_bar = bars[i + 1]
        if next_bar.low >= ref.low:
            continue
        trapped_idx: Optional[int] = None
        for k in range(i + 1, n):
            if bars[k].high > ref.high:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue
        entry_line = ref.low
        if _is_mitigated_bear(bars, entry_line, next_bar.low, trapped_idx=trapped_idx):
            continue
        raw.append(dict(ref_idx=i, trig_idx=i + 1, trapped_idx=trapped_idx))

    raw_sorted = sorted(raw, key=lambda z: z["ref_idx"])
    kept: List[dict] = []
    for z in raw_sorted:
        entry = bars[z["ref_idx"]].low
        redundant = False
        for k in kept:
            kentry = bars[k["ref_idx"]].low
            if k["trig_idx"] >= z["trig_idx"] and k["ref_idx"] <= z["ref_idx"] <= k["trig_idx"]:
                if entry >= kentry:
                    redundant = True
                    break
        if not redundant:
            kept.append(z)

    zones: List[RollingBaseZone] = []
    for z in kept:
        ref = bars[z["ref_idx"]]
        next_bar = bars[z["ref_idx"] + 1]
        trapped = bars[z["trapped_idx"]]
        zones.append(RollingBaseZone(
            reference_low=ref.low, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=next_bar.low, sweep_started_ts=next_bar.timestamp,
            bars_since_sweep=z["trapped_idx"] - z["trig_idx"],
            locked=True, lock_ts=trapped.timestamp,
            entry_line=ref.low, sl_level=ref.high,
        ))
    zones.sort(key=lambda z: z.reference_low_ts)
    return zones


def find_bull_trap_2candle(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> Optional[RollingBaseZone]:
    """2026-07-19 — symmetric bull-trap counterpart to ``find_bear_trap_2candle``,
    added for the crypto spot-only test path (Delta BTC/ETH): unlike a real
    option premium chart (where PE premium rising after a sweep-down already
    means bearish spot), raw spot has no such inversion, so PE must scan
    spot for genuine BULL traps (buyers trapped, price reverses down) to
    produce an actual bearish signal — scanning bear-only on spot for both
    CE and PE would just find the same bullish patterns twice. ref.high =
    entry_line (buyers' entry), ref.low = sl_level (buyers' SL); zone width
    (``sweep_low``, reused field name for the sweep HIGH — same convention
    as the original find_bull_zone) pinned to the immediate next candle's
    high only. Not used anywhere in the NIFTY path — PremiumGateScanner
    defaults to bear-only exactly as before."""
    n = len(bars)
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        next_bar = bars[i + 1]
        if next_bar.high <= ref.high:
            continue  # no sweep on the immediate next candle

        trapped_idx: Optional[int] = None
        for k in range(i + 1, n):
            if bars[k].low < ref.low:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue

        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue

        entry_line = ref.high
        if _is_mitigated_bull(bars, entry_line, next_bar.high, trapped_idx=trapped_idx):
            continue

        return RollingBaseZone(
            reference_low=ref.high, reference_low_ts=ref.timestamp,
            prev_close=ref.close,
            swept=True, sweep_low=next_bar.high, sweep_started_ts=next_bar.timestamp,
            bars_since_sweep=trapped_idx - (i + 1),
            locked=True, lock_ts=trapped_ts,
            entry_line=entry_line, sl_level=ref.low,
        )
    return None


def find_all_bull_traps_2candle(
    bars: List[_Bar], skip_before_ts: Optional[datetime] = None,
) -> List[RollingBaseZone]:
    """Enumerate ALL confirmed, price-level-independent bull-trap zones —
    symmetric counterpart to ``find_all_bear_traps_2candle``."""
    n = len(bars)
    raw = []
    for i in range(n - 2, -1, -1):
        ref = bars[i]
        next_bar = bars[i + 1]
        if next_bar.high <= ref.high:
            continue
        trapped_idx: Optional[int] = None
        for k in range(i + 1, n):
            if bars[k].low < ref.low:
                trapped_idx = k
                break
        if trapped_idx is None:
            continue
        trapped_ts = bars[trapped_idx].timestamp
        if skip_before_ts is not None and trapped_ts <= skip_before_ts:
            continue
        entry_line = ref.high
        if _is_mitigated_bull(bars, entry_line, next_bar.high, trapped_idx=trapped_idx):
            continue
        raw.append(dict(ref_idx=i, trig_idx=i + 1, trapped_idx=trapped_idx))

    raw_sorted = sorted(raw, key=lambda z: z["ref_idx"])
    kept: List[dict] = []
    for z in raw_sorted:
        entry = bars[z["ref_idx"]].high
        redundant = False
        for k in kept:
            kentry = bars[k["ref_idx"]].high
            if k["trig_idx"] >= z["trig_idx"] and k["ref_idx"] <= z["ref_idx"] <= k["trig_idx"]:
                if entry <= kentry:
                    redundant = True
                    break
        if not redundant:
            kept.append(z)

    zones: List[RollingBaseZone] = []
    for z in kept:
        ref = bars[z["ref_idx"]]
        next_bar = bars[z["ref_idx"] + 1]
        trapped = bars[z["trapped_idx"]]
        zones.append(RollingBaseZone(
            reference_low=ref.high, reference_low_ts=ref.timestamp, prev_close=ref.close,
            swept=True, sweep_low=next_bar.high, sweep_started_ts=next_bar.timestamp,
            bars_since_sweep=z["trapped_idx"] - z["trig_idx"],
            locked=True, lock_ts=trapped.timestamp,
            entry_line=ref.high, sl_level=ref.low,
        ))
    zones.sort(key=lambda z: z.reference_low_ts)
    return zones


def resample_bars(
    bars_5m: List[_Bar], multiplier: int, session_open: Tuple[int, int] = (9, 15),
) -> List["_ResampledBar"]:
    """Resample a 5-minute bar sequence into ``multiplier``-minute bars,
    grouping each bar by (calendar day, minutes-since-that-day's-session-open
    // multiplier). This is CLOCK-ANCHORED per calendar day — every bar's own
    timestamp determines its bucket, independent of its position in the input
    list. This makes grouping robust to buffer eviction (e.g. a deque(maxlen=N)
    upstream dropping older bars), data gaps, or the input starting mid-day —
    none of which can misalign a purely positional "every 15th bar" grouping
    (which is what this function used to do, and which silently drifted off
    the 09:15 session boundary once an upstream deque eviction removed a
    partial day's worth of bars — see project memory / 2026-07-18 bugfix).

    ``session_open``: (hour, minute) of the exchange's own session open —
    defaults to NSE/NIFTY's 09:15. MCX underlyings (CRUDEOIL etc.) open at
    09:00 and must pass (9, 0) so bucket boundaries land on the real session
    start instead of NIFTY's.

    EVERY input bar is grouped into exactly one output bucket, unconditionally
    — including bars whose timestamp falls before ``session_open`` (these get
    a negative ``bucket_idx``, but that's still a valid, distinct bucket key;
    they are NOT dropped). This matters for 24/7 callers like crypto (BTC/ETH),
    where there is no real "pre-session" — bars before the default 09:15
    anchor are ordinary trading data, not noise, and must not be filtered
    out. Callers who genuinely want only the post-session-open bars must
    filter the input (or output) themselves; this function never does it for
    them."""
    if multiplier % 5 != 0 or multiplier < 5:
        raise ValueError(f"multiplier must be a positive multiple of 5, got {multiplier}")

    open_hour, open_minute = session_open
    buckets: dict = {}
    order: list = []
    for b in bars_5m:
        day = b.timestamp.date()
        open_dt = b.timestamp.replace(
            hour=open_hour, minute=open_minute, second=0, microsecond=0,
        )
        minutes_since_open = int((b.timestamp - open_dt).total_seconds() // 60)
        bucket_idx = minutes_since_open // multiplier
        key = (day, bucket_idx)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)

    out: List[_ResampledBar] = []
    for key in order:
        chunk = buckets[key]
        out.append(_ResampledBar(
            timestamp=chunk[0].timestamp,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
        ))
    return out


@dataclass(frozen=True)
class _ResampledBar:
    timestamp: datetime
    high: float
    low: float
    close: float


def build_ladder(step: int, max_minutes: int) -> List[int]:
    """[step, 2*step, 3*step, ...] capped at max_minutes (inclusive)."""
    if step <= 0 or max_minutes <= 0:
        return []
    out = []
    m = step
    while m <= max_minutes:
        out.append(m)
        m += step
    return out or [step]


def scan_ladder(
    bars_5m: List[_Bar], ladder: List[int], bear: bool,
    skip_before_ts: Optional[datetime] = None,
) -> Optional[LadderMatch]:
    """Scan the multiplier ladder in ascending order; return the FIRST
    multiplier that shows a confirmed TRAPPED pattern anywhere within that
    multiplier's resampled lookback (no bar-count cap on the SL-clear wait —
    only the overall lookback window, via ``ladder``'s ceiling, bounds it).
    If a multiplier's WHOLE resampled history shows no confirmed trap, the
    scan climbs to the next (coarser) multiplier and re-evaluates the
    identical 3-part sequence there. ``bear=True`` scans for bears-trapped
    (bullish read); ``bear=False`` scans buyers-trapped (bearish read)."""
    finder = find_bear_zone if bear else find_bull_zone
    for m in ladder:
        resampled = resample_bars(bars_5m, m)
        if len(resampled) < 3:
            continue
        zone = finder(resampled, skip_before_ts=skip_before_ts)
        if zone is not None:
            return LadderMatch(multiplier=m, zone=zone, ref_ts=zone.reference_low_ts, reclaim_ts=zone.lock_ts)
    return None
