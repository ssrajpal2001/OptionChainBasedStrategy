from datetime import datetime, timezone
from strategies.bear_trap_oi.models import (
    Bar, TrapZoneState, TrapZone, OiSnapshot, OiFilterResult, Position,
    MtmSnapshot,
)


def _ts(minute):
    return datetime(2026, 10, 1, 9, minute, tzinfo=timezone.utc)


def test_bar_is_a_plain_dataclass():
    bar = Bar(ts=_ts(15), open=100.0, high=105.0, low=98.0, close=102.0)
    assert bar.high == 105.0
    assert bar.low == 98.0


def test_trap_zone_defaults_to_waiting_with_no_candles():
    zone = TrapZone(state=TrapZoneState.WAITING, c1=None, c2=None,
                     zone_lo=None, zone_hi=None, confirmed_ts=None)
    assert zone.state == TrapZoneState.WAITING
    assert zone.c1 is None


def test_oi_filter_result_carries_pass_and_detail():
    result = OiFilterResult(call_oi_total=120000, put_oi_total_same_band=90000,
                             call_oi_trend="FALLING", put_oi_trend="RISING",
                             passed=True, detail="call falling, put rising")
    assert result.passed is True
    assert "falling" in result.detail


def test_position_tracks_side_and_status():
    pos = Position(side="CE", strike=24500, qty=75, entry_price=120.5,
                    entry_ts=_ts(20), status="open")
    assert pos.status == "open"
    assert pos.side == "CE"


def test_mtm_snapshot_carries_running_mtm_and_trigger():
    snap = MtmSnapshot(side="CE", observed_entry_price=120.5, observed_qty=75,
                        observed_strike=24500, current_ltp=95.0,
                        running_mtm=(95.0 - 120.5) * 75, elapsed_sec=1800,
                        trigger="concurrent_entry", ts=_ts(45),
                        other_side_event="PE entry fired @ 60.25")
    assert snap.running_mtm == -1912.5
    assert snap.trigger == "concurrent_entry"
