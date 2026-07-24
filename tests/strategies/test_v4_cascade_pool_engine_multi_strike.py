"""strategies/v4_cascade/pool_engine.py -- multi-candidate (side, strike)
pooling, added 2026-07-24 so V4Cascade can scan several strikes per side
instead of committing blindly to one fixed ATM-offset (real chart-confirmed
bug: a fixed offset landed CE in a zone-less region of that strike's own
premium chart while a different, untracked strike had 8 zones)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.pool_engine import PoolCascadeEngine

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar75(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def _bar5(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=5)


def _engine():
    cfg = V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=65)
    return PoolCascadeEngine(cfg, entry_offset=5.0, session_open=(9, 15))


def _arm_zone(eng, side, strike):
    """ref@0 (low=100,high=110), sweep@1 (low=90), reclaim+reentry@2-3 --
    same geometry the existing single-candidate tests already use, just
    routed through a specific (side, strike) candidate."""
    eng.on_75m_bar(side, strike, _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar(side, strike, _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar(side, strike, _bar75(2, 96, 115, 95, 112))
    eng.on_75m_bar(side, strike, _bar75(3, 105, 108, 92, 96))  # re-entry


def test_two_ce_candidates_pool_independently():
    """CE 24000 finds a zone; CE 23900 (a different candidate, same side)
    sees completely different bars and finds nothing -- proves the pools
    are keyed independently per strike, not merged/shared by side."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    eng.on_75m_bar("CE", 23900.0, _bar75(0, 500, 500, 500, 500))
    eng.on_75m_bar("CE", 23900.0, _bar75(1, 500, 500, 500, 500))

    assert len(eng._pool[("CE", 24000.0)]) == 1
    assert eng._pool[("CE", 24000.0)][0].tracking is True
    assert eng._pool.get(("CE", 23900.0), []) == []


def test_only_first_pierce_opens_second_candidate_ignored():
    """Two CE candidates both reach pending_entry; whichever pierces FIRST
    opens the position, and the other candidate's later pierce is skipped
    -- exactly one position total, same-side siblings frozen once one side
    is open."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)

    base = _BASE + timedelta(minutes=75 * 4)
    # 24000 triggers and pierces over three 5m bars -- the first bar of a
    # session only ever establishes prev_5m_bar (no legitimate predecessor
    # yet, per the "intraday-only trigger" rule), the second arms the
    # break-of-structure trigger (close > prev.high), and the third pierces
    # the entry limit (low <= zone_low + entry_offset); the arming bar
    # itself never checks for a pierce (see on_5m_bar's `continue` right
    # after setting pending_entry), so this cannot collapse to two bars.
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 0, 96, 97, 95, 96))
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 5, 98, 99, 97, 99))    # arms (99 > prev.high 97)
    events = eng.on_5m_bar("CE", 24000.0, _bar5(base, 10, 99, 100, 89, 92))  # pierces (low 89 <= limit 95)
    assert eng.is_open()
    assert eng.position.side == "CE"
    assert eng.position.tracking_strike == 24000.0
    opened_events = [e for e in events if e.event_type.name.startswith("OPEN_")]
    assert len(opened_events) == 1

    # 23900's own bars keep arriving -- it must be frozen (no new events,
    # no state change) while CE is open, even though it independently has
    # a pending trigger of its own.
    before = list(eng._pool.get(("CE", 23900.0), []))
    more_events = eng.on_5m_bar("CE", 23900.0, _bar5(base, 15, 99, 100, 89, 92))
    assert more_events == []
    assert eng._pool.get(("CE", 23900.0), []) == before
    assert eng.position.tracking_strike == 24000.0  # unchanged -- no flip within the same side


def test_cross_side_structural_flip_still_works_with_multiple_candidates():
    """Opposite-side pierce still force-closes + flips into the new
    position -- the existing structural-flip behavior (shipped the commit
    immediately before this multi-strike work) must not regress."""
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "PE", 23900.0)

    base = _BASE + timedelta(minutes=75 * 4)
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 0, 96, 97, 95, 96))
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 5, 98, 99, 97, 99))
    eng.on_5m_bar("CE", 24000.0, _bar5(base, 10, 99, 100, 89, 92))
    assert eng.is_open() and eng.position.side == "CE"

    eng.on_5m_bar("PE", 23900.0, _bar5(base, 15, 96, 97, 95, 96))
    eng.on_5m_bar("PE", 23900.0, _bar5(base, 20, 98, 99, 97, 99))
    events = eng.on_5m_bar("PE", 23900.0, _bar5(base, 25, 99, 100, 89, 92))
    # _close_for_structural_flip's close events carry reason="structural_flip"
    # (see _close_event's call site) -- the presence of at least one proves
    # the CE leg was genuinely force-closed, not silently dropped.
    assert any(e.reason == "structural_flip" for e in events)
    assert eng.is_open() and eng.position.side == "PE"
    assert eng.position.tracking_strike == 23900.0


def test_open_position_carries_real_strike_not_placeholder():
    """_open_position must set CascadePosition.tracking_strike/.execution_strike
    and CascadeEvent.execution_strike to the real triggering strike -- these
    were hardcoded 0.0/None before this task."""
    eng = _engine()
    _arm_zone(eng, "PE", 24500.0)
    base = _BASE + timedelta(minutes=75 * 4)
    eng.on_5m_bar("PE", 24500.0, _bar5(base, 0, 96, 97, 95, 96))
    eng.on_5m_bar("PE", 24500.0, _bar5(base, 5, 98, 99, 97, 99))
    events = eng.on_5m_bar("PE", 24500.0, _bar5(base, 10, 99, 100, 89, 92))
    open_ev = [e for e in events if e.event_type.name.startswith("OPEN_")][0]
    assert open_ev.execution_strike == 24500.0
    assert eng.position.tracking_strike == 24500.0
    assert eng.position.execution_strike == 24500.0
    assert eng.position.t1.strike == 24500.0
    assert eng.position.t2.strike == 24500.0


def test_reset_candidate_clears_only_that_strike():
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)
    assert len(eng._pool[("CE", 24000.0)]) == 1
    assert len(eng._pool[("CE", 23900.0)]) == 1

    eng.reset_candidate("CE", 24000.0)
    assert eng._pool[("CE", 24000.0)] == []
    assert len(eng._pool[("CE", 23900.0)]) == 1  # untouched


def test_reset_side_clears_every_candidate_on_that_side():
    eng = _engine()
    _arm_zone(eng, "CE", 24000.0)
    _arm_zone(eng, "CE", 23900.0)
    eng.reset_side("CE")
    assert eng._pool[("CE", 24000.0)] == []
    assert eng._pool[("CE", 23900.0)] == []
