"""V4CascadeEngine.check_exits_tick -- 2026-07-22: tick-driven SL/target/
trailing-stop enforcement, fired on every live tick instead of waiting for
the next 5m bar close. The critical property under test: t1.sl_price/
target_price are TRACKING-contract scale for a risk_basis=="tracking"
position but EXECUTION-contract scale for risk_basis=="execution_native" --
passing the wrong-scale LTP must never trigger a close (that was the exact
bug caught and fixed before this shipped: an execution-scale tick was
originally being compared against a tracking-scale SL/target for the
default, far more common "tracking" risk_basis case)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.exits import TrailingBaseTracker

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 22, 10, 0, tzinfo=IST)


def _tracking_position(tracking_sl=154.3, tracking_target=201.9, exec_entry=178.10):
    """risk_basis defaults to 'tracking'. t1.sl_price/target_price are
    TRACKING-contract scale (per engine._open_position's collapsed-scale
    compute_risk_mapping call); t1.entry_price/t2.entry_price hold the real
    EXECUTION fill (book.py's _on_fill overwrites these post-entry)."""
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24000.0, qty=65,
                     entry_price=exec_entry, sl_price=tracking_sl, target_price=tracking_target,
                     status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24000.0, qty=65,
                     entry_price=exec_entry, sl_price=tracking_sl, status="open",
                     trail_stop_price=160.0)  # execution-scale, as map_trailing_stop_to_execution would set it
    pos = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24000.0,
        atm_at_trigger=24150.0, entry_spot=24150.0, t1=t1, t2=t2, open_time=_BASE,
        tracking_entry_price=140.0,
    )
    return pos, t1, t2


def _execution_native_position(entry_price=21.0, sl_price=18.0, target_price=25.0):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, target_price=target_price, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, status="open", trail_stop_price=18.0)
    pos = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, open_time=_BASE,
        risk_basis="execution_native",
    )
    return pos, t1, t2


def test_tracking_position_t1_sl_hit_on_tracking_ltp():
    eng = V4CascadeEngine()
    pos, t1, t2 = _tracking_position()
    eng.position = pos
    eng._tracking_entry_price["CE"] = 140.0
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=154.3)

    events = eng.check_exits_tick("CE", _BASE, tracking_ltp=147.0)
    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_sl_structural_floor"
    assert t1.close_price == 147.0


def test_tracking_position_ignores_execution_scale_ltp_for_t1():
    """The exact bug this feature caught pre-ship: an EXECUTION-scale tick
    (e.g. the real 178.10-scale option LTP) must NOT be compared against a
    TRACKING-scale sl_price (154.3) -- here execution_ltp=147.0 would
    trivially "hit" 154.3 if scale were ignored, but must be silently
    skipped since this position's risk_basis is 'tracking'."""
    eng = V4CascadeEngine()
    pos, t1, t2 = _tracking_position()
    t2.status = "closed"  # isolate T1 -- T2's trail_stop_price (execution-scale) would legitimately fire otherwise
    eng.position = pos
    eng._tracking_entry_price["CE"] = 140.0
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=154.3)

    events = eng.check_exits_tick("CE", _BASE, execution_ltp=147.0)
    assert events == []
    assert t1.status == "open"


def test_tracking_position_t2_trail_checked_on_execution_ltp():
    """t2.trail_stop_price is always execution-scale regardless of
    risk_basis -- so T2 IS checked off execution_ltp even for a 'tracking'
    position."""
    eng = V4CascadeEngine()
    pos, t1, t2 = _tracking_position()
    t1.status = "closed"  # isolate T2
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=160.0)

    events = eng.check_exits_tick("CE", _BASE, execution_ltp=159.0)
    fired = [e for e in events if e.tranche == "T2"]
    assert len(fired) == 1
    assert fired[0].reason == "t2_trailing_base_stop"
    assert t2.close_price == 159.0


def test_execution_native_position_t1_checked_on_execution_ltp_only():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)

    # tracking_ltp must be ignored for an execution_native position.
    events = eng.check_exits_tick("CE", _BASE, tracking_ltp=17.0)
    assert events == []
    assert t1.status == "open"

    events = eng.check_exits_tick("CE", _BASE, execution_ltp=17.5)
    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_sl_structural_floor"
    assert t1.close_price == 17.5


def test_t1_target_hit_ratchets_t2_breakeven_immediately():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)

    events = eng.check_exits_tick("CE", _BASE, execution_ltp=25.5)
    fired = [e for e in events if e.tranche == "T1"]
    assert fired[0].reason == "t1_target_2r"
    # breakeven ratchet: entry(21.0) + sl_buffer(default 10.0 NIFTY) = 31.0
    assert t2.trail_stop_price == 31.0


def test_no_position_is_noop():
    eng = V4CascadeEngine()
    eng.position = None
    events = eng.check_exits_tick("CE", _BASE, tracking_ltp=100.0, execution_ltp=100.0)
    assert events == []


def test_already_closed_t1_not_reclosed():
    eng = V4CascadeEngine()
    pos, t1, t2 = _execution_native_position()
    t1.status = "closed"
    eng.position = pos
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=18.0)

    events = eng.check_exits_tick("CE", _BASE, execution_ltp=10.0)
    assert [e for e in events if e.tranche == "T1"] == []
