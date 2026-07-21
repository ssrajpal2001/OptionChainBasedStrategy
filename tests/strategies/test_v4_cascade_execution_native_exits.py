"""V4CascadeEngine.check_exits_execution_native -- 2026-07-22: for a position
with risk_basis=="execution_native", T1/T2 exit-checks run against the
EXECUTION contract's own bars directly (no tracking-to-execution scale
mapping at all -- current_stop, sl_price, target_price and the bar are all
on the SAME scale). Also: _update_side must skip the OLD tracking-bar-driven
_check_exits entirely for such a position, since its exits are now driven
exclusively by this new, separate execution-bar clock."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.exits import TrailingBaseTracker

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 12, 0, tzinfo=IST)


def _bar(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _execution_native_position(entry_price=21.0, sl_price=18.0, target_price=25.0):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, target_price=target_price, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, status="open")
    return CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="execution_native",
    ), t1, t2


def test_t1_target_hit_on_execution_bar_directly_no_scale_mapping():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)

    events = eng.check_exits_execution_native("CE", _bar(0, 24, 26, 23, 25))
    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_target_2r"
    assert t1.close_price == 25.0   # the EXECUTION bar's own level, no scale conversion


def test_t2_trail_stop_hit_on_execution_bar_directly():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    trail = TrailingBaseTracker(bear=True, initial_stop=18.0)
    eng._trackers["CE"] = trail

    events = eng.check_exits_execution_native("CE", _bar(0, 19, 20, 17, 17.5))
    fired = [e for e in events if e.tranche == "T2"]
    assert len(fired) == 1
    assert fired[0].reason == "t2_trailing_base_stop"
    assert t2.close_price == 18.0


def test_no_position_is_a_noop():
    eng = V4CascadeEngine()
    eng.position = None
    events = eng.check_exits_execution_native("CE", _bar(0, 24, 26, 23, 25))
    assert events == []


def test_update_side_skips_tracking_check_exits_for_execution_native_position():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)
    # A TRACKING bar whose (tracking-scale) numbers would trivially "hit" T1's
    # execution-scale target (25.0) if scale mapping were mistakenly still
    # applied -- must NOT close anything via the tracking-bar path.
    tracking_bar = _bar(0, 1000, 1100, 900, 1050)
    events = eng.update(ce_bar=tracking_bar)
    assert events == []
    assert t1.status == "open"
    assert t2.status == "open"
