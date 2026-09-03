"""
2026-08-12: unit tests for the strike_mode="fixed_period" / execute_strike_
mode="daily_itm1" features added to D1TrapSRBook (strategies/d1_trap_option/
sr_book.py) for tomorrow's paper deployment. No prior tests existed for this
class at all (confirmed via repo search before writing these), so these
also cover the pre-existing default (daily_atm/same) behavior to guard
against a regression in the untouched path.

D1TrapSRBook is constructed via __new__ + manual attribute assignment
(bypasses the real __init__, which needs a live bus/cfg) -- same pattern
already used in tests/execution/test_d1trap_bridge_fail_loud.py for
D1TrapExecutionBridge.
"""
from datetime import date, datetime

import pytest

from config.global_config import IST
from strategies.d1_trap_option.sr_book import D1TrapSRBook


def _make_book(**overrides) -> D1TrapSRBook:
    book = D1TrapSRBook.__new__(D1TrapSRBook)
    book._underlying = overrides.get("underlying", "BANKNIFTY")
    book._client_id = "ssrajpal2001"
    book._binding_id = "SA5770"
    book._itm_offset_pts = overrides.get("itm_offset_pts", 300)
    book._strike_step = overrides.get("strike_step", 100)
    book._lot_size = 30
    book._lot_multiplier = 1
    book._product_type = "MIS"
    book._strike_mode = overrides.get("strike_mode", "daily_atm")
    book._execute_strike_mode = overrides.get("execute_strike_mode", "same")
    book._today = overrides.get("today", date(2026, 8, 12))
    book._ce_strike = overrides.get("ce_strike")
    book._pe_strike = overrides.get("pe_strike")
    book._exec_ce_strike = overrides.get("exec_ce_strike")
    book._exec_pe_strike = overrides.get("exec_pe_strike")
    book._execute_ltp = overrides.get("execute_ltp", {})
    book._period_strike_cache = overrides.get("period_strike_cache", {})
    book._positions = []
    book._stop_for_day = False
    book._event_counter = 0
    book._bus = None
    book._persist_key = "test_persist_key"
    book._persist_positions = lambda: None
    return book


# ── _period_key ──────────────────────────────────────────────────────────────

def test_period_key_monthly_for_banknifty():
    book = _make_book(underlying="BANKNIFTY")
    assert book._period_key(date(2026, 8, 12)) == (2026, 8)
    assert book._period_key(date(2026, 7, 1)) == (2026, 7)


def test_period_key_weekly_iso_for_nifty():
    book = _make_book(underlying="NIFTY")
    # 2026-08-12 is a Wednesday in ISO week 33.
    y, w, _ = date(2026, 8, 12).isocalendar()
    assert book._period_key(date(2026, 8, 12)) == (y, w)
    # A Monday and the Sunday ending that same week must share a period_key.
    monday = date(2026, 8, 10)
    sunday = date(2026, 8, 16)
    assert book._period_key(monday) == book._period_key(sunday)
    # The following Monday must be a DIFFERENT period_key.
    next_monday = date(2026, 8, 17)
    assert book._period_key(monday) != book._period_key(next_monday)


# ── _fixed_period_strikes ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fixed_period_strikes_computes_from_prev_period_hilo():
    book = _make_book(underlying="BANKNIFTY", itm_offset_pts=300)

    async def _fake_hilo(today):
        return 56023.60, 58596.85   # real numbers from this session's own sweep

    book._prev_period_hilo = _fake_hilo
    ce, pe = await book._fixed_period_strikes(spot_open=57000.0)
    assert ce == 55700   # round(56023.60/100)*100 - 300 = 56000-300
    assert pe == 58900   # round(58596.85/100)*100 + 300 = 58600+300


@pytest.mark.asyncio
async def test_fixed_period_strikes_caches_per_period_reusing_without_refetch():
    book = _make_book(underlying="BANKNIFTY", itm_offset_pts=300)
    calls = {"n": 0}

    async def _fake_hilo(today):
        calls["n"] += 1
        return 56000.0, 58600.0

    book._prev_period_hilo = _fake_hilo
    ce1, pe1 = await book._fixed_period_strikes(spot_open=57000.0)
    ce2, pe2 = await book._fixed_period_strikes(spot_open=57500.0)   # different spot, same period
    assert (ce1, pe1) == (ce2, pe2)
    assert calls["n"] == 1, "second call within the same period must hit the cache, not re-fetch"


@pytest.mark.asyncio
async def test_fixed_period_strikes_falls_back_to_daily_atm_when_hilo_unresolvable():
    book = _make_book(underlying="BANKNIFTY", itm_offset_pts=300)

    async def _fake_hilo(today):
        return None, None   # no token / no data

    book._prev_period_hilo = _fake_hilo
    ce, pe = await book._fixed_period_strikes(spot_open=57654.0)
    atm = round(57654.0 / 100) * 100
    assert (ce, pe) == (int(atm - 300), int(atm + 300))


# ── _enter_leg: execute_strike_mode ──────────────────────────────────────────

def test_enter_leg_same_mode_uses_scan_strike_and_scan_premium():
    book = _make_book(ce_strike=55700, pe_strike=58900, execute_strike_mode="same")
    ev = dict(entry_premium=1200.50, initial_sl=1100.0, entry_ts=datetime(2026, 8, 12, 10, 0, tzinfo=IST))
    book._enter_leg("PE", ev)
    assert len(book._positions) == 1
    pos = book._positions[0]
    assert pos["strike"] == 58900
    assert pos["entry_price"] == 1200.50


def test_enter_leg_daily_itm1_uses_execute_strike_and_live_ltp():
    book = _make_book(ce_strike=55700, pe_strike=58900, execute_strike_mode="daily_itm1",
                       exec_ce_strike=57000, exec_pe_strike=57200,
                       execute_ltp={"PE": 480.25})
    ev = dict(entry_premium=1200.50, initial_sl=1100.0, entry_ts=datetime(2026, 8, 12, 10, 0, tzinfo=IST))
    book._enter_leg("PE", ev)
    assert len(book._positions) == 1
    pos = book._positions[0]
    assert pos["strike"] == 57200, "must trade the EXECUTE strike, not the scan strike"
    assert pos["entry_price"] == 480.25, "must book the EXECUTE strike's real live LTP, not the scan premium"
    assert pos["scan_strike"] == 58900


def test_enter_leg_daily_itm1_skips_when_live_ltp_not_ready():
    book = _make_book(ce_strike=55700, pe_strike=58900, execute_strike_mode="daily_itm1",
                       exec_ce_strike=57000, exec_pe_strike=57200,
                       execute_ltp={})   # no tick arrived yet for PE
    ev = dict(entry_premium=1200.50, initial_sl=1100.0, entry_ts=datetime(2026, 8, 12, 10, 0, tzinfo=IST))
    book._enter_leg("PE", ev)
    assert book._positions == [], "must not fabricate a fill against a stale/wrong price"


# ── _restore_positions: strike-match validation ──────────────────────────────

def test_restore_positions_validates_against_execute_strike_when_daily_itm1():
    book = _make_book(ce_strike=55700, pe_strike=58900, execute_strike_mode="daily_itm1",
                       exec_ce_strike=57000, exec_pe_strike=57200)
    stored = {"legs": [{"side": "PE", "strike": 57200, "entry_price": 480.25,
                         "initial_sl": 450.0, "qty": 30, "entry_ts": "2026-08-12T10:00:00+05:30"}]}

    import strategies.d1_trap_option.sr_book as m
    orig = m.position_store.load
    m.position_store.load = lambda key: stored
    try:
        book._restore_positions()
    finally:
        m.position_store.load = orig
    assert len(book._positions) == 1, "a leg on the correct EXECUTE strike must be restored, not discarded"


def test_restore_positions_discards_stale_scan_strike_leg_when_daily_itm1():
    book = _make_book(ce_strike=55700, pe_strike=58900, execute_strike_mode="daily_itm1",
                       exec_ce_strike=57000, exec_pe_strike=57200)
    # A leg stored on the SCAN strike (58900) must NOT be restored once
    # execute_strike_mode is active -- real orders trade on 57200, not 58900.
    stored = {"legs": [{"side": "PE", "strike": 58900, "entry_price": 1200.50,
                         "initial_sl": 1100.0, "qty": 30, "entry_ts": "2026-08-12T10:00:00+05:30"}]}

    import strategies.d1_trap_option.sr_book as m
    orig = m.position_store.load
    m.position_store.load = lambda key: stored
    try:
        book._restore_positions()
    finally:
        m.position_store.load = orig
    assert book._positions == []


# ── _square_off_from_event: execute-price re-lookup ─────────────────────────

@pytest.mark.asyncio
async def test_square_off_uses_execute_strike_live_ltp():
    book = _make_book(execute_strike_mode="daily_itm1", execute_ltp={"PE": 460.10})
    pos = dict(side="PE", strike=57200, scan_strike=58900, entry_price=480.25,
               initial_sl=450.0, entry_ts=datetime(2026, 8, 12, 10, 0, tzinfo=IST), qty=30)
    book._positions = [pos]

    captured = {}

    async def _fake_square_off_leg(p, reason, exit_price):
        captured["reason"] = reason
        captured["exit_price"] = exit_price

    book._square_off_leg = _fake_square_off_leg
    ev = dict(reason="sl_raw@1150.00", exit_price=1150.00)   # scan-strike exit price
    await book._square_off_from_event("PE", ev)
    assert captured["exit_price"] == 460.10, "must re-price the exit on the EXECUTE strike's live LTP"


@pytest.mark.asyncio
async def test_square_off_falls_back_to_scan_price_when_execute_ltp_missing():
    book = _make_book(execute_strike_mode="daily_itm1", execute_ltp={})
    pos = dict(side="PE", strike=57200, scan_strike=58900, entry_price=480.25,
               initial_sl=450.0, entry_ts=datetime(2026, 8, 12, 10, 0, tzinfo=IST), qty=30)
    book._positions = [pos]

    captured = {}

    async def _fake_square_off_leg(p, reason, exit_price):
        captured["exit_price"] = exit_price

    book._square_off_leg = _fake_square_off_leg
    ev = dict(reason="eod", exit_price=1150.00)
    await book._square_off_from_event("PE", ev)
    assert captured["exit_price"] == 1150.00, "must still close the leg (never leave it stuck open)"
