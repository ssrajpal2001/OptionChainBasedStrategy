"""
2026-08-12: unit tests for the two opt-in selectivity filters added to
SRPingPongTracker (strategies/d1_trap_option/support_resistance.py) for the
high win-rate/PF-weighted D1 Trap optimization pass -- min_breach_buffer_pct
and min_r2_bucket_count. Both default to 0 (byte-identical to the previously
validated behavior); these tests isolate ONLY the new gate logic, not the
underlying S&R phase machine itself (SupportResistanceCalculator), which is
already the proven, live-validated ground truth this whole mechanic sits on.

State is injected directly into SRPingPongTracker.active_sr[...]["calc"]
rather than replayed through a full multi-phase candle journey -- the phase
machine's own correctness is out of scope here; what's under test is whether
the new buffer/bucket-count gates correctly allow or block an entry given a
phase transition the calculator has already confirmed.
"""
from datetime import datetime

from config.global_config import IST
from strategies.d1_trap_option.bear_only_book import _Bar
from strategies.d1_trap_option.support_resistance import SRPingPongTracker


def _bar(ts, o, h, l, c):
    return _Bar(timestamp=ts, open=o, high=h, low=l, close=c)


def _zone(lock_ts, lo=90.0, hi=100.0):
    return {"lock_ts": lock_ts, "zone_lo": lo, "zone_hi": hi}


def _inject_r2_tracking(tracker, zone_ts, r1_high=100.0, s1_low=92.0, touch_ts=None):
    """Fast-forward one zone's calc straight into R2_TRACKING with a known R1
    high, bypassing the full Phase0->R1_TRACKING->S2_TRACKING->R2_TRACKING
    journey (already covered by the mechanic's own live/backtest track
    record). Clears the touch bar's pending bucket first so it can't get
    double-processed against the injected state on the next bar."""
    entry = tracker.active_sr[zone_ts]
    entry["calc"].states["OPT"] = {
        "current_phase": "R2_TRACKING",
        "last_candle": {"high": r1_high, "low": 95.0, "timestamp": touch_ts, "duration": 1},
        "sr_levels": {
            "S1": {"low": s1_low, "high": r1_high, "timestamp": touch_ts, "is_established": True},
            "R1": {"high": r1_high, "low": s1_low, "timestamp": touch_ts, "is_established": True},
            "S2": None,
            "R2": {"high": 98.0, "low": 96.0, "breakout_level": 96.0, "timestamp": touch_ts, "is_established": False},
        },
    }
    entry["bucket"] = []
    entry["bucket_open"] = touch_ts.replace(second=0, microsecond=0)


def test_default_params_fire_entry_on_bare_breach():
    """buffer=0, min_r2=0 (defaults) -- must reproduce the pre-2026-08-12
    behavior exactly: any breach above R1, however marginal, fires."""
    zone_ts = datetime(2026, 7, 1, 9, 20, tzinfo=IST)
    zone = _zone(zone_ts)
    tracker = SRPingPongTracker([zone], tf_minutes=1, lot_size=30)

    t0 = datetime(2026, 7, 1, 9, 25, tzinfo=IST)
    tracker.on_bar(_bar(t0, 95, 96, 94, 95))
    _inject_r2_tracking(tracker, zone_ts, touch_ts=t0)
    tracker._r2_bucket_counts[zone_ts] = 1

    t1 = datetime(2026, 7, 1, 9, 26, tzinfo=IST)
    tracker.on_bar(_bar(t1, 100, 100.1, 99.5, 100.05))   # clears R1(100.0) by 0.1
    t2 = datetime(2026, 7, 1, 9, 27, tzinfo=IST)
    ev = tracker.on_bar(_bar(t2, 100.1, 100.2, 100.0, 100.1))

    assert ev is not None and ev["type"] == "entry"
    assert tracker.position is not None


def _one_zone_breach(min_breach_buffer_pct, min_r2_bucket_count, breach_high, r1_high=100.0):
    """Build a fresh single-zone tracker, fast-forward it into R2_TRACKING,
    then feed one breach bucket. Returns (tracker, zone_ts, ev). Each call
    uses its own tracker/bar-stream -- on_bar() checks EVERY zone against the
    SAME incoming bar each call, so two zones sharing one call can't each
    have their own independent price level (real premium is one series);
    isolating scenarios this way avoids that conflation."""
    zone_ts = datetime(2026, 7, 1, 9, 20, tzinfo=IST)
    zone = _zone(zone_ts, lo=r1_high - 20.0, hi=r1_high)
    tracker = SRPingPongTracker([zone], tf_minutes=1, lot_size=30,
                                 min_breach_buffer_pct=min_breach_buffer_pct,
                                 min_r2_bucket_count=min_r2_bucket_count)

    t0 = datetime(2026, 7, 1, 9, 25, tzinfo=IST)
    tracker.on_bar(_bar(t0, r1_high - 8, r1_high - 4, r1_high - 10, r1_high - 5))
    _inject_r2_tracking(tracker, zone_ts, r1_high=r1_high, s1_low=r1_high - 8.0, touch_ts=t0)
    tracker._r2_bucket_counts[zone_ts] = 5  # plenty, unless the test overrides it below

    t1 = datetime(2026, 7, 1, 9, 26, tzinfo=IST)
    tracker.on_bar(_bar(t1, r1_high, breach_high, r1_high - 0.5, breach_high - 0.2))
    t2 = datetime(2026, 7, 1, 9, 27, tzinfo=IST)
    ev = tracker.on_bar(_bar(t2, breach_high - 0.2, breach_high - 0.1, breach_high - 0.3, breach_high - 0.2))
    return tracker, zone_ts, ev


def test_breach_buffer_rejects_marginal_breakout():
    # +0.5% over R1(100) -- below a 5% buffer requirement.
    tracker, zone_ts, ev = _one_zone_breach(min_breach_buffer_pct=0.05, min_r2_bucket_count=0, breach_high=100.5)
    assert ev is None or ev.get("type") != "entry"
    assert tracker.position is None
    assert tracker.active_sr[zone_ts].get("void") is not True, "rejected breach must not void the zone"


def test_breach_buffer_accepts_strong_breakout():
    # +10% over R1(100) -- clears a 5% buffer easily.
    tracker, zone_ts, ev = _one_zone_breach(min_breach_buffer_pct=0.05, min_r2_bucket_count=0, breach_high=110.0)
    assert ev is not None and ev["type"] == "entry"
    assert tracker.position is not None and tracker.position["zone_ts"] == zone_ts


def test_min_r2_bucket_count_blocks_entry_when_too_few_buckets_seen():
    zone_ts = datetime(2026, 7, 1, 9, 20, tzinfo=IST)
    zone = _zone(zone_ts, lo=80.0, hi=100.0)
    tracker = SRPingPongTracker([zone], tf_minutes=1, lot_size=30, min_r2_bucket_count=3)

    t0 = datetime(2026, 7, 1, 9, 25, tzinfo=IST)
    tracker.on_bar(_bar(t0, 92, 96, 90, 95))
    _inject_r2_tracking(tracker, zone_ts, r1_high=100.0, s1_low=92.0, touch_ts=t0)
    tracker._r2_bucket_counts[zone_ts] = 1   # not enough

    t1 = datetime(2026, 7, 1, 9, 26, tzinfo=IST)
    tracker.on_bar(_bar(t1, 100, 105.0, 99.5, 104.0))
    t2 = datetime(2026, 7, 1, 9, 27, tzinfo=IST)
    ev = tracker.on_bar(_bar(t2, 104.0, 104.2, 103.8, 104.0))

    assert ev is None or ev.get("type") != "entry"
    assert tracker.position is None


def test_min_r2_bucket_count_allows_entry_once_enough_buckets_seen():
    zone_ts = datetime(2026, 7, 1, 9, 20, tzinfo=IST)
    zone = _zone(zone_ts, lo=80.0, hi=100.0)
    tracker = SRPingPongTracker([zone], tf_minutes=1, lot_size=30, min_r2_bucket_count=3)

    t0 = datetime(2026, 7, 1, 9, 25, tzinfo=IST)
    tracker.on_bar(_bar(t0, 92, 96, 90, 95))
    _inject_r2_tracking(tracker, zone_ts, r1_high=100.0, s1_low=92.0, touch_ts=t0)
    tracker._r2_bucket_counts[zone_ts] = 3   # exactly enough

    t1 = datetime(2026, 7, 1, 9, 26, tzinfo=IST)
    tracker.on_bar(_bar(t1, 100, 105.0, 99.5, 104.0))
    t2 = datetime(2026, 7, 1, 9, 27, tzinfo=IST)
    ev = tracker.on_bar(_bar(t2, 104.0, 104.2, 103.8, 104.0))

    assert ev is not None and ev["type"] == "entry"
    assert tracker.position["zone_ts"] == zone_ts


def test_r2_bucket_count_increments_while_phase_stays_r2_tracking():
    zone_ts = datetime(2026, 7, 1, 9, 20, tzinfo=IST)
    zone = _zone(zone_ts)
    tracker = SRPingPongTracker([zone], tf_minutes=1, lot_size=30)

    t0 = datetime(2026, 7, 1, 9, 25, tzinfo=IST)
    tracker.on_bar(_bar(t0, 95, 96, 94, 95))
    _inject_r2_tracking(tracker, zone_ts, r1_high=100.0, s1_low=92.0, touch_ts=t0)
    assert tracker._r2_bucket_counts.get(zone_ts, 0) == 0

    # A bar that stays inside R2 (doesn't break R1, doesn't break S1) keeps the
    # calculator in R2_TRACKING -- e.g. high=98.5 (below r1=100), low=96.5
    # (above r2's low=96) just nudges R2's own high up (see process_straddle_
    # candle's R2_TRACKING branch: "elif high > r2['high']: r2['high'] = high").
    t1 = datetime(2026, 7, 1, 9, 26, tzinfo=IST)
    tracker.on_bar(_bar(t1, 97, 98.5, 96.5, 98))
    t2 = datetime(2026, 7, 1, 9, 27, tzinfo=IST)
    tracker.on_bar(_bar(t2, 98, 98.2, 97.9, 98.1))

    st = tracker.active_sr[zone_ts]["calc"].get_calculated_sr_state("OPT")
    assert st["current_phase"] == "R2_TRACKING"
    assert tracker._r2_bucket_counts[zone_ts] == 1
