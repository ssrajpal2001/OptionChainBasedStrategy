"""strategies/v4_cascade/pool_engine.py's PoolCascadeEngine -- live-
incremental adaptation of the validated multi-zone-pool HTF/LTF cascade
(backtest/v4_cascade/htf_ltf_backtest.py). Trades the tracking contract
directly (strike=0.0 here -- book.py fills in the real tracking strike at
_emit_order time, same convention the pure V4CascadeEngine already uses).

2026-07-23: this engine is ALWAYS a buyer -- both CE and PE look for the
SAME bear-trap pattern (demand-zone sweep+reclaim) on their OWN premium
chart, never a bull-trap/short-style mirror. Every test below exercises
this directly: the PE tests use the identical zone-shape/geometry as the
CE tests, proving there is no side-flipped branch left anywhere."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import (
    CascadeEventType, CascadePosition, RollingBaseZone, TrancheLeg,
)
from strategies.v4_cascade.exits import TrailingBaseTracker
from strategies.v4_cascade.pool_engine import PoolCascadeEngine, _ZoneSlot

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


def test_htf_zone_added_to_pool_on_reentry_pe_same_bear_trap_shape():
    """2026-07-23: PE must find the SAME bear-trap pattern (demand-zone
    sweep+reclaim) on its OWN premium chart -- there is no bull-trap branch
    for PE anymore. Identical bars/assertions to the CE test above, just
    fed to the "PE" side, proving on_75m_bar no longer branches on side."""
    eng = _engine()
    eng.on_75m_bar("PE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("PE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("PE", _bar75(2, 96, 115, 95, 112))
    assert len(eng._pool["PE"]) == 1
    slot = eng._pool["PE"][0]
    assert slot.zone_low == 90 and slot.zone_high == 100
    assert slot.tracking is False
    eng.on_75m_bar("PE", _bar75(3, 105, 108, 92, 96))
    assert eng._pool["PE"][0].tracking is True
    # CE's pool is completely untouched by feeding PE bars.
    assert eng._pool["CE"] == []


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
    assert eng.position.t1.target_price == 97.0  # ltf_zone.sl_level (locked before fill)
    assert eng.position.t2.target_price is None  # T2 has no fixed target field set at open (matches V4CascadeEngine convention)
    assert eng._pool["CE"] == []  # pool cleared on fill


def test_5m_trigger_fires_without_any_15m_zone_ever_forming():
    """2026-07-23: the 5m trigger must NOT wait for slot.ltf_zone to exist --
    entry is driven by the 75m zone re-entry + 5m break-of-structure alone.
    Here on_15m_bar is never called at all, yet the trade still fires."""
    eng = _engine()
    eng.on_75m_bar("CE", _bar75(0, 105, 110, 100, 105))
    eng.on_75m_bar("CE", _bar75(1, 95, 100, 90, 95))
    eng.on_75m_bar("CE", _bar75(2, 96, 115, 95, 112))
    eng.on_75m_bar("CE", _bar75(3, 105, 108, 92, 96))  # re-entry
    assert eng._pool["CE"][0].tracking is True
    assert eng._pool["CE"][0].ltf_zone is None  # no 15m data fed at all

    ltf_base = _BASE + timedelta(minutes=75 * 4)
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 30, 93, 94, 92, 93))
    assert events == []
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 35, 93, 95, 92, 94.5))  # arms (94.5 > 94)
    assert events == []
    events = eng.on_5m_bar("CE", _bar5(ltf_base, 40, 95, 96, 94, 95.5))  # pierces limit(95)
    assert len(events) == 1
    assert events[0].event_type == CascadeEventType.OPEN_LONG_CE
    assert events[0].audit["ltf_found_at_fill"] is False
    assert eng.position is not None
    # No 15m trap ever locked -> T1 falls back to T2's target (htf.sl_level =
    # 110, the 75m ref candle bar75(0)'s own high).
    assert eng.position.t1.target_price == 110.0
    assert eng._pool["CE"] == []


def test_intraday_trigger_reset_skips_cross_day_comparison():
    eng = _engine()
    day1 = datetime(2026, 7, 1, 14, 45, tzinfo=IST)
    day2 = datetime(2026, 7, 2, 9, 15, tzinfo=IST)
    slot_bar = _Bar(day1, 100, 101, 99, 100, tf=5)
    # Manually seed a tracking, ltf-ready pool slot (bypassing the full
    # 75m/15m chain, which is exercised by the other tests).
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


def test_pending_entry_pierce_fires_on_first_bar_of_new_day():
    """Reviewer's Finding 1 repro: a slot already `pending_entry=True` going
    into a day boundary must still get its limit-pierce checked on the VERY
    FIRST 5m bar of the new day -- the day-boundary reset only clears
    prev_5m_bar (the trigger-arm pointer), never pending_entry itself, which
    is exactly the kind of mid-tracking state the design spec says carries
    across days unchanged. Before the fix, the unconditional
    `if prev is None: continue` ran ahead of the pending_entry check and
    swallowed the whole per-slot body (pierce check included) on this bar,
    silently delaying the fill by one whole 5m bar."""
    eng = _engine()
    day1 = datetime(2026, 7, 1, 14, 45, tzinfo=IST)
    day2 = datetime(2026, 7, 2, 9, 15, tzinfo=IST)
    zone = RollingBaseZone(entry_line=100.0, sweep_low=90.0, sl_level=110.0,
                            reference_low_ts=day1, lock_ts=day1, locked=True)
    slot = _ZoneSlot(zone)
    slot.tracking = True
    slot.ltf_zone = RollingBaseZone(entry_line=95.0, sweep_low=92.0, sl_level=98.0,
                                     reference_low_ts=day1, lock_ts=day1, locked=True)
    slot.pending_entry = True
    slot.trigger_ts = day1
    slot.prev_5m_bar = _Bar(day1, 100, 101, 99, 100, tf=5)  # yesterday's last 5m bar
    eng._pool["CE"] = [slot]
    eng._last_5m_date["CE"] = day1.date()

    # First 5m bar of the new day: limit price = zone_low(90) + offset(5) =
    # 95; this bar's low (93) genuinely pierces it. The fill must fire on
    # THIS bar, not the next one.
    events = eng.on_5m_bar("CE", _Bar(day2, 97, 98, 93, 96, tf=5))
    assert len(events) == 1
    assert events[0].event_type == CascadeEventType.OPEN_LONG_CE
    assert eng.position is not None
    assert eng.position.t1.entry_price == 95.0  # limit price = zone_low(90) + offset(5)


def _open_position(eng, side):
    """Builds a long position (always the bear-trap/long geometry: SL below
    entry, target above entry) on either side, mirroring _open_position's
    own construction, for exercising _check_exits/force_eod_close without
    needing the full 75m/15m/5m discovery chain."""
    ts0 = _BASE
    entry, sl, t1_target = 100.0, 90.0, 106.0
    qty = eng._cfg.tranche_qty
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=0.0, qty=qty,
                     entry_price=entry, entry_time=ts0, entry_reason="test",
                     sl_price=sl, target_price=t1_target)
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=0.0, qty=qty,
                     entry_price=entry, entry_time=ts0, entry_reason="test",
                     sl_price=sl, target_price=None, tracking_current_stop=sl)
    eng.position = CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=0.0, execution_strike=0.0,
        atm_at_trigger=0.0, entry_spot=0.0, t1=t1, t2=t2, open_time=ts0,
        tracking_entry_price=entry,
    )
    eng._trail[side] = TrailingBaseTracker(bear=True, initial_stop=sl)
    return t1, t2


def test_check_exits_ce_t1_target_then_breakeven_ratchet_then_t2_trailing_stop():
    eng = _engine()
    t1, t2 = _open_position(eng, "CE")

    # Bar 1: T1's fixed 2R target (106) hit on the bar's HIGH (long/not-short
    # branch of check_t1). Bar's low (105) stays above the post-ratchet
    # breakeven stop (100) so T2 does not also close on this same bar.
    bar1 = _bar5(_BASE, 5, 104, 107, 105, 106)
    events1 = eng._check_exits("CE", bar1)
    assert len(events1) == 1
    assert events1[0].tranche == "T1"
    assert events1[0].reason == "t1_target_2r"
    assert t1.status == "closed"
    assert t2.status == "open"
    # Breakeven ratchet moved T2's trailing stop up from the original
    # structural SL (90) to entry (100).
    assert eng._trail["CE"].current_stop == 100.0
    assert t2.trail_stop_price == 100.0
    assert t2.tracking_current_stop == 100.0
    assert eng.position.status == "open"

    # Bar 2: T2's ratcheted trailing stop (100) is hit on the bar's LOW.
    bar2 = _bar5(_BASE, 10, 99, 100, 95, 98)
    events2 = eng._check_exits("CE", bar2)
    assert len(events2) == 1
    assert events2[0].tranche == "T2"
    assert events2[0].reason == "t2_trailing_base_stop"
    assert t2.status == "closed"
    assert eng.position.status == "closed"


def test_check_exits_pe_same_long_geometry_as_ce():
    """2026-07-23: PE is ALSO a plain long buy (never short-style) -- SL
    below entry, target above entry, breakeven ratchet moves UP, exactly
    like CE. Identical bars/assertions to the CE test above, on "PE"."""
    eng = _engine()
    t1, t2 = _open_position(eng, "PE")

    bar1 = _bar5(_BASE, 5, 104, 107, 105, 106)
    events1 = eng._check_exits("PE", bar1)
    assert len(events1) == 1
    assert events1[0].tranche == "T1"
    assert events1[0].reason == "t1_target_2r"
    assert t1.status == "closed"
    assert t2.status == "open"
    assert eng._trail["PE"].current_stop == 100.0
    assert t2.trail_stop_price == 100.0
    assert t2.tracking_current_stop == 100.0
    assert eng.position.status == "open"

    bar2 = _bar5(_BASE, 10, 99, 100, 95, 98)
    events2 = eng._check_exits("PE", bar2)
    assert len(events2) == 1
    assert events2[0].tranche == "T2"
    assert events2[0].reason == "t2_trailing_base_stop"
    assert t2.status == "closed"
    assert eng.position.status == "closed"


def test_force_eod_close_closes_both_open_legs():
    eng = _engine()
    t1, t2 = _open_position(eng, "CE")
    assert t1.status == "open" and t2.status == "open"

    eod_ts = _BASE + timedelta(hours=6)
    events = eng.force_eod_close("CE", eod_ts, 101.5)
    assert len(events) == 2
    assert {e.tranche for e in events} == {"T1", "T2"}
    assert all(e.reason == "eod_force_close" for e in events)
    assert t1.status == "closed" and t1.close_reason == "eod_force_close"
    assert t2.status == "closed" and t2.close_reason == "eod_force_close"
    assert eng.position.status == "closed"
    assert eng._trail["CE"] is None
