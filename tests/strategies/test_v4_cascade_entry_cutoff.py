"""strategies/v4_cascade/engine.py's entry_cutoff_hour_min -- 2026-07-21 fix.

V4 Cascade had NO entry cutoff at all, unlike sell_straddle (which has a
distinct EntryEnd separate from SquareOff). Confirmed live on 0DTE NIFTY
expiry: EOD force square-off correctly closed the open position at 15:20,
but Gate 3 kept firing brand-new positions AFTER that, each immediately
force-closed again on the very next bar -- a repeating loop (3 spurious
re-entries between 15:25 and 15:44) that only stopped when a human manually
stopped the deployment, because decayed near-zero premium noise kept
re-triggering the pierce. _may_fire now also gates on entry_cutoff_hour_min,
reusing the exact same "leave the setup pending, don't pop it" mechanics
already used for the unarmed-side case."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.engine import V4CascadeEngine

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 15, 0, tzinfo=IST)


def _bar5(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _index_bear_confirm_bars():
    return [
        _Bar(_BASE - timedelta(hours=6), 200, 210, 200, 205, tf=75),
        _Bar(_BASE - timedelta(hours=4, minutes=45), 105, 110, 100, 105, tf=75),
        _Bar(_BASE - timedelta(hours=3, minutes=30), 95, 115, 90, 112, tf=75),
    ]


def _premium_trap_pattern(offset0):
    """entry_line=100, sweep_low=95, limit_entry_price=98.33."""
    return [
        _bar5(offset0, 105, 110, 100, 105),
        _bar5(offset0 + 1, 98, 105, 95, 100),
        _bar5(offset0 + 2, 110, 115, 105, 112),
    ]


def _armed_engine(entry_cutoff_hour_min=None):
    eng = V4CascadeEngine(entry_cutoff_hour_min=entry_cutoff_hour_min)
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    assert eng._scanners["CE"].armed is True
    return eng


def test_pierce_before_cutoff_fires_normally():
    eng = _armed_engine(entry_cutoff_hour_min=(23, 59))   # nowhere near these bars
    events = []
    for b in _premium_trap_pattern(0):
        events += eng.update(ce_bar=b)
    events += eng.update(ce_bar=_bar5(3, 99, 100, 99, 99.5))   # -> limit_armed
    events += eng.update(ce_bar=_bar5(4, 98, 99, 97, 97.5))    # pierce -> fires
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 1
    assert eng.position is not None


def test_pierce_at_or_after_cutoff_does_not_fire():
    """The exact live bug: a fully pierced setup must NOT open a new
    position once past the configured square-off time -- it stays pending,
    un-popped, for the rest of the (already-over) session."""
    eng = _armed_engine(entry_cutoff_hour_min=(15, 20))   # _BASE=15:00, bars run past 15:15
    events = []
    for b in _premium_trap_pattern(0):
        events += eng.update(ce_bar=b)
    events += eng.update(ce_bar=_bar5(3, 99, 100, 99, 99.5))   # -> limit_armed, still before 15:20
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"

    # This pierce bar lands at 15:00 + 4*5min = 15:20 -- exactly at cutoff.
    events += eng.update(ce_bar=_bar5(4, 98, 99, 97, 97.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 0
    assert eng.position is None
    assert len(eng._scanners["CE"].setups) == 1   # left pending, not popped
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"


def test_no_cutoff_configured_preserves_old_behavior():
    """entry_cutoff_hour_min=None (the default) must behave exactly as
    before this fix -- no regression for callers that don't pass it."""
    eng = _armed_engine(entry_cutoff_hour_min=None)
    events = []
    for b in _premium_trap_pattern(0):
        events += eng.update(ce_bar=b)
    events += eng.update(ce_bar=_bar5(3, 99, 100, 99, 99.5))
    events += eng.update(ce_bar=_bar5(4, 98, 99, 97, 97.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 1
