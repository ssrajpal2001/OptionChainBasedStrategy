"""strategies/v4_cascade/pool_engine.py's PoolCascadeEngine -- live-
incremental adaptation of the validated multi-zone-pool HTF/LTF cascade
(backtest/v4_cascade/htf_ltf_backtest.py). Trades the tracking contract
directly (strike=0.0 here -- book.py fills in the real tracking strike at
_emit_order time, same convention the pure V4CascadeEngine already uses)."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import CascadeEventType
from strategies.v4_cascade.pool_engine import PoolCascadeEngine

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 1, 9, 15, tzinfo=IST)


def _bar75(offset, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset), o, h, l, c, tf=75)


def _bar15(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=15)


def _bar5(base, offset_min, o, h, l, c):
    return _Bar(base + timedelta(minutes=offset_min), o, h, l, c, tf=5)


def _engine():
    cfg = V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=65)
    return PoolCascadeEngine(cfg, entry_offset=5.0, session_open=(9, 15))


def test_htf_zone_added_to_pool_on_reentry():
    eng = _engine()
    # ref@0 (low=100,high=110), sweep@1 (low=90), reclaim@2 (high=115).
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    assert len(eng._pool["CE"]) == 1
    slot = eng._pool["CE"][0]
    assert slot.zone_low == 90 and slot.zone_high == 100
    assert slot.tracking is False
    # A later bar re-enters [90, 100].
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))
    assert eng._pool["CE"][0].tracking is True


def test_full_chain_produces_open_event():
    eng = _engine()
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))  # re-entry
    assert eng._pool["CE"][0].tracking is True

    ltf_base = _BASE + timedelta(minutes=75 * 4)
    # 15m nested pattern: ref(low=93,high=97), sweep(low=91), reclaim(high=99).
    eng.on_15m_bar("CE", _bar15(ltf_base, 0, 95, 97, 93, 95))
    eng.on_15m_bar("CE", _bar15(ltf_base, 15, 92, 94, 91, 92))
    eng.on_15m_bar("CE", _bar15(ltf_base, 30, 93, 99, 92, 97))
    assert eng._pool["CE"][0].ltf_zone is not None
    assert eng._pool["CE"][0].ltf_zone.sl_level == 97  # ref.high

    # 5m trigger: candle closes above the previous candle's high.
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 30, 93, 94, 92, 93))
    assert events == []
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 35, 93, 95, 92, 94.5))
    assert events == []  # trigger armed (94.5 > 94 -- prev bar's high), not pierced yet
    # limit = zone_low(90) + offset(5) = 95 -- a bar whose low pierces down to it fills.
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 40, 95, 96, 94, 95.5))
    assert len(events) == 1
    assert events[0].event_type == CascadeEventType.OPEN_LONG_CE
    assert eng.position is not None
    assert eng.position.t1.entry_price == 95.0
    assert eng.position.t1.sl_price == 85.0  # zone_low(90) - offset(5)
    assert eng.position.t1.target_price == 97.0  # ltf_zone.sl_level
    assert eng.position.t2.target_price is None  # T2 has no fixed target field set at open (matches V4CascadeEngine convention)
    assert eng._pool["CE"] == []  # pool cleared on fill


def test_intraday_trigger_reset_skips_cross_day_comparison():
    eng = _engine()
    day1 = datetime(2026, 7, 1, 14, 45, tzinfo=IST)
    day2 = datetime(2026, 7, 2, 9, 15, tzinfo=IST)
    slot_bar = _Bar(day1, 100, 101, 99, 100, tf=5)
    # Manually seed a tracking, ltf-ready pool slot (bypassing the full
    # 75m/15m chain, which is exercised by the other tests).
    from strategies.v4_cascade.pool_engine import _ZoneSlot
    from strategies.v4_cascade.dataclasses import RollingBaseZone
    zone = RollingBaseZone(entry_line=100.0, sweep_low=90.0, sl_level=110.0,
                            reference_low_ts=day1, lock_ts=day1, locked=True)
    slot = _ZoneSlot(zone)
    slot.tracking = True
    slot.ltf_zone = RollingBaseZone(entry_line=95.0, sweep_low=92.0, sl_level=98.0,
                                     reference_low_ts=day1, lock_ts=day1, locked=True)
    slot.prev_5m_bar = slot_bar  # yesterday's last 5m bar
    eng._pool["CE"] = [slot]
    eng._last_5m_date["CE"] = day1.date()

    # First 5m bar of the NEW day -- even though its close (150) is way
    # above yesterday's bar's high (101), it must NOT trigger, since the
    # "previous candle" pointer resets across the day boundary.
    events = eng.on_5m_bar("CE", _Bar(day2, 140, 150, 139, 150, tf=5))
    assert events == []
    assert eng._pool["CE"][0].pending_entry is False
