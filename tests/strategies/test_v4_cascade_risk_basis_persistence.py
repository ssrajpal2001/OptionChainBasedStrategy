"""CascadePosition.risk_basis -- 2026-07-21, records which price scale a
trade's SL/target/trailing-stop were computed on ("tracking" = today's
scaled-from-tracking-contract approach, "execution_native" = the new
lookback-on-execution-strike approach). Needed so a restart can rebuild T2's
tracker on the SAME scale the live trade was actually using -- without this,
_restore_tracker_state_for_open_position has no way to know which
reconstruction path to take."""
from datetime import datetime
from zoneinfo import ZoneInfo

from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _position(risk_basis="tracking"):
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0)
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65, entry_price=20.0)
    return CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2,
        open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST), risk_basis=risk_basis,
    )


def test_defaults_to_tracking():
    pos = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05,
    )
    assert pos.risk_basis == "tracking"


def test_round_trips_execution_native_through_to_dict_from_dict():
    pos = _position(risk_basis="execution_native")
    restored = CascadePosition.from_dict(pos.to_dict())
    assert restored.risk_basis == "execution_native"


def test_round_trips_tracking_through_to_dict_from_dict():
    pos = _position(risk_basis="tracking")
    restored = CascadePosition.from_dict(pos.to_dict())
    assert restored.risk_basis == "tracking"
