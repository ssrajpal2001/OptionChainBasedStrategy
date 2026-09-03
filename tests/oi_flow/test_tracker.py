"""
2026-08-12: unit tests for strategies/oi_flow/tracker.py (OIFlowTracker).
Hand-built tick objects with controlled oi/timestamp -- no real feed, no
network, no dependency on any other strategy module (this whole package is
built standalone by explicit user direction).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

from config.global_config import IST
from strategies.oi_flow.tracker import OIFlowTracker


@dataclass
class _FakeTick:
    strike: float
    option_type: str
    oi: int
    timestamp: datetime


def _t(base: datetime, seconds: int) -> datetime:
    return base + timedelta(seconds=seconds)


def test_oi_roc_returns_none_before_window_fills():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_500, timestamp=_t(base, 30)))
    # Only 30s of history exists -- a 180s window has nothing old enough to anchor on.
    assert tracker.oi_roc(57700, "CE", window_sec=180, now=_t(base, 30)) is None


def test_oi_roc_returns_correct_delta_once_window_is_filled():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=99_000, timestamp=_t(base, 90)))
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=98_200, timestamp=_t(base, 190)))
    # 190s later, a 180s window anchors at/before t=10s -- the oldest sample <= t=10 is t=0 (oi=100000).
    roc = tracker.oi_roc(57700, "CE", window_sec=180, now=_t(base, 190))
    assert roc == 98_200 - 100_000 == -1_800


def test_oi_now_returns_latest_watched_value():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker()
    tracker.watch_strikes({(57700, "PE"): True})
    tracker.on_option_tick(_FakeTick(57700, "PE", oi=200_000, timestamp=_t(base, 0)))
    tracker.on_option_tick(_FakeTick(57700, "PE", oi=205_000, timestamp=_t(base, 60)))
    assert tracker.oi_now(57700, "PE") == 205_000


def test_unwatched_strikes_are_ignored():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker()
    tracker.watch_strikes({(57700, "CE"): True})   # only 57700 CE watched
    tracker.on_option_tick(_FakeTick(58000, "CE", oi=999_999, timestamp=_t(base, 0)))  # different strike
    tracker.on_option_tick(_FakeTick(57700, "PE", oi=888_888, timestamp=_t(base, 0)))  # different side
    assert tracker.oi_now(58000, "CE") is None
    assert tracker.oi_now(57700, "PE") is None


def test_stale_samples_are_evicted_past_max_history_sec():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=120)
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    # A tick 200s later prunes anything older than (200-120)=80s -- the t=0 sample must be evicted.
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=101_000, timestamp=_t(base, 200)))
    # A 180s-window ROC request now has no sample old enough to anchor on (the t=0 one was evicted).
    assert tracker.oi_roc(57700, "CE", window_sec=180, now=_t(base, 200)) is None


def test_watch_strikes_rescoping_drops_old_keys_history():
    base = datetime(2026, 8, 12, 9, 20, tzinfo=IST)
    tracker = OIFlowTracker()
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    assert tracker.oi_now(57700, "CE") == 100_000

    # Strike selection changes for a new day/period -- old key must be dropped entirely.
    tracker.watch_strikes({(58200, "CE"): True})
    assert tracker.oi_now(57700, "CE") is None, "history for a no-longer-watched key must not leak"
    # And a stray tick for the old, now-unwatched key must not silently resurrect it.
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=999_000, timestamp=_t(base, 10)))
    assert tracker.oi_now(57700, "CE") is None


def test_oi_roc_none_when_strike_never_seen_at_all():
    tracker = OIFlowTracker()
    tracker.watch_strikes({(57700, "CE"): True})
    assert tracker.oi_roc(57700, "CE", window_sec=180) is None
    assert tracker.oi_now(57700, "CE") is None


# ── persistence (2026-08-21) -- restart-proofing since REST backfill is
# structurally impossible for OI (Upstox historical candles hardcode oi=0) ──

def test_to_dict_and_load_dict_round_trip():
    base = datetime(2026, 8, 21, 10, 0, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.watch_strikes({(57700, "CE"): True, (57800, "PE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_500, timestamp=_t(base, 30)))
    tracker.on_option_tick(_FakeTick(57800, "PE", oi=50_000, timestamp=_t(base, 10)))

    snapshot = tracker.to_dict()

    restored = OIFlowTracker(max_history_sec=360)
    restored.load_dict(snapshot, now=_t(base, 30))
    assert restored.oi_now(57700, "CE") == 100_500
    assert restored.oi_now(57800, "PE") == 50_000
    assert restored.oi_roc(57700, "CE", window_sec=30, now=_t(base, 30)) == 500


def test_load_dict_prunes_samples_stale_relative_to_real_now():
    """The core correctness requirement: a sample must be judged stale
    against the REAL current time passed to load_dict(), not its own
    timestamp -- otherwise oi_roc() (several of whose engine callers don't
    pass their own now=) could silently compute a result entirely from
    pre-restart data, indistinguishable from a genuinely fresh reading."""
    base = datetime(2026, 8, 21, 10, 0, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    snapshot = tracker.to_dict()

    # Restore as if 20 minutes (1200s) have passed in real wall-clock time --
    # well past max_history_sec=360 -- the persisted sample must NOT survive.
    restored = OIFlowTracker(max_history_sec=360)
    restored.load_dict(snapshot, now=_t(base, 1200))
    assert restored.oi_now(57700, "CE") is None
    assert restored.oi_roc(57700, "CE", window_sec=180, now=_t(base, 1200)) is None


def test_load_dict_keeps_samples_still_within_max_history_sec():
    base = datetime(2026, 8, 21, 10, 0, tzinfo=IST)
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.watch_strikes({(57700, "CE"): True})
    tracker.on_option_tick(_FakeTick(57700, "CE", oi=100_000, timestamp=_t(base, 0)))
    snapshot = tracker.to_dict()

    # Only 60s have passed -- well within the 360s window -- must survive.
    restored = OIFlowTracker(max_history_sec=360)
    restored.load_dict(snapshot, now=_t(base, 60))
    assert restored.oi_now(57700, "CE") == 100_000


def test_load_dict_tolerates_malformed_entries():
    tracker = OIFlowTracker(max_history_sec=360)
    now = datetime(2026, 8, 21, 10, 0, tzinfo=IST)
    tracker.load_dict({"not-a-valid-key-format": [["bad-ts", "bad-oi"]]}, now=now)   # must not raise
    assert tracker.oi_now(57700, "CE") is None


def test_load_dict_empty_snapshot_is_a_noop():
    tracker = OIFlowTracker(max_history_sec=360)
    tracker.load_dict({}, now=datetime(2026, 8, 21, 10, 0, tzinfo=IST))
    assert tracker.oi_now(57700, "CE") is None
