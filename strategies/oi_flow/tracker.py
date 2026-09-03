"""
strategies/oi_flow/tracker.py — OIFlowTracker.

The missing piece confirmed absent from this codebase before this strategy
existed: a rolling, wall-clock-anchored, per-(strike, side) Open Interest
time series. matrix_engine/option_matrix.py's ChainRow keeps a 30-TICK
internal deque (_call_oi_hist/_put_oi_hist) used only for its own spike
z-score check -- not time-anchored (30 ticks can span seconds or many
minutes depending on activity) and not exposed to callers. This class is
the real thing: "OI change over the last N seconds," computed from
absolute OI LEVELS, never a broker-supplied delta field.

Why never trust a feed's own change_oi: confirmed by direct inspection of
data_layer/global_feeder.py that Upstox's live WebSocket feed hardcodes
change_oi=0 (no ready delta in Upstox's payload) -- only Fyers populates a
genuine per-tick delta. Computing rate-of-change ourselves from the
absolute `oi` level (which Upstox DOES populate correctly) is the only
approach that's correct regardless of which broker feed is active.

Pure class -- no bus/asyncio/broker dependency, mirrors the "pure data +
pure functions" style already used by strategies/v4_cascade/rolling_base.py
so this is trivially unit-testable with hand-built tick objects.

Standalone by design (see strategies/oi_flow/__init__.py docstring) -- does
not import anything from any other strategy module. `on_option_tick`
accepts any duck-typed object with .strike/.option_type/.oi/.timestamp
(data_layer.base_feeder.OptionTick already matches this shape; the
platform's own tick type, not another strategy's).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

_Key = Tuple[float, str]   # (strike, "CE"|"PE")


class OIFlowTracker:
    """One instance per book (BANKNIFTY, per side, or however the caller
    wants to scope it -- this class itself is underlying-agnostic, just a
    keyed rolling OI-level history)."""

    def __init__(self, max_history_sec: int = 360) -> None:
        self._max_history_sec = max_history_sec
        self._watched: Dict[_Key, bool] = {}
        # key -> list of (timestamp, oi) samples, oldest first.
        self._samples: Dict[_Key, List[Tuple[datetime, int]]] = {}

    def watch_strikes(self, strikes: Dict[_Key, bool]) -> None:
        """Wholesale-replace the watch list. Keys no longer watched have
        their history dropped immediately -- called on every strike-
        selection change (e.g. daily OI-wall re-pick) so memory never
        accumulates history for strikes that stopped mattering."""
        self._watched = dict(strikes)
        self._samples = {k: v for k, v in self._samples.items() if k in self._watched}

    def on_option_tick(self, tick) -> None:
        """O(1): appends (ts, oi) for tick.strike/tick.option_type if
        watched, then prunes samples older than max_history_sec. Ticks for
        unwatched (strike, side) pairs are ignored (cheap early-out)."""
        key: _Key = (float(tick.strike), str(tick.option_type).upper())
        if key not in self._watched:
            return
        ts = tick.timestamp
        oi = int(tick.oi)
        bucket = self._samples.setdefault(key, [])
        bucket.append((ts, oi))
        cutoff = ts - timedelta(seconds=self._max_history_sec)
        # Prune from the front -- samples are appended in non-decreasing
        # timestamp order in live use (real tick stream), so this stays O(1)
        # amortized; a single out-of-order tick can't corrupt correctness,
        # only (harmlessly) skip pruning that pass.
        i = 0
        while i < len(bucket) and bucket[i][0] < cutoff:
            i += 1
        if i:
            del bucket[:i]

    def oi_now(self, strike: float, option_type: str) -> Optional[int]:
        bucket = self._samples.get((float(strike), option_type.upper()))
        if not bucket:
            return None
        return bucket[-1][1]

    def oi_roc(self, strike: float, option_type: str, window_sec: int,
               now: Optional[datetime] = None) -> Optional[int]:
        """latest OI minus the OI at the OLDEST sample <= (now - window_sec).

        Returns None -- never 0 -- when no sample is yet old enough to
        anchor the requested window. A caller MUST be able to tell "no
        signal yet" (insufficient history, e.g. right after startup or a
        fresh watch_strikes() call) apart from "genuinely flat" (real zero
        change over a fully-populated window) -- collapsing these to the
        same value would make the tracker silently lie every single
        morning during its own warm-up period, exactly the kind of bug
        this codebase's own confirm-then-finalize discipline elsewhere
        exists to prevent (an absent signal must never look identical to
        a negative/zero confirmation)."""
        bucket = self._samples.get((float(strike), option_type.upper()))
        if not bucket:
            return None
        latest_ts, latest_oi = bucket[-1]
        anchor_ts = (now or latest_ts) - timedelta(seconds=window_sec)
        anchor_oi: Optional[int] = None
        for ts, oi in bucket:
            if ts <= anchor_ts:
                anchor_oi = oi
            else:
                break
        if anchor_oi is None:
            return None
        return latest_oi - anchor_oi

    # ── persistence (2026-08-21) ─────────────────────────────────────────────
    # OI has no historical REST source (Upstox hardcodes OI=0 on every
    # historical candle, confirmed by direct inspection) -- there is no way to
    # backfill a mid-day restart's lost window the way every other strategy's
    # own warmup does. But the live samples this tracker already collected
    # before a restart are real, genuinely-observed data; persisting them to
    # disk and reloading on startup means a restart only ever costs the
    # samples collected in the gap between the last periodic save and the
    # restart itself (a few seconds), not the full max_history_sec window.

    def to_dict(self) -> Dict[str, list]:
        """Serializable snapshot of every watched key's current sample
        history. Key format "strike|SIDE" (dict keys must be strings for
        JSON)."""
        return {
            f"{strike}|{side}": [[ts.isoformat(), oi] for ts, oi in samples]
            for (strike, side), samples in self._samples.items()
        }

    def load_dict(self, data: Dict[str, list], now: datetime) -> None:
        """Restore from a to_dict() snapshot, pruning anything older than
        max_history_sec relative to the REAL current time (`now`), not the
        samples' own timestamps -- restoring genuinely stale samples without
        this would let oi_roc() silently compute a "valid-looking" result
        entirely from pre-restart data (several of this tracker's own
        engine-side callers don't pass their own `now=`, defaulting to the
        bucket's latest sample timestamp -- exactly the "silently lies"
        failure mode this tracker's own oi_roc() docstring already commits to
        never doing). Does NOT filter by self._watched -- a restart may not
        know the current wall strikes yet (that itself depends on live
        ticks); the existing, already-correct watch_strikes() call that
        follows shortly after startup will naturally drop anything no longer
        relevant, same as it already does for any watch-list change."""
        cutoff = now - timedelta(seconds=self._max_history_sec)
        restored: Dict[_Key, List[Tuple[datetime, int]]] = {}
        for key_str, rows in (data or {}).items():
            try:
                strike_str, side = key_str.rsplit("|", 1)
                key: _Key = (float(strike_str), side)
                samples = [(datetime.fromisoformat(ts), int(oi)) for ts, oi in rows]
            except Exception:
                continue
            kept = [(ts, oi) for ts, oi in samples if ts >= cutoff]
            if kept:
                restored[key] = kept
        self._samples = restored
