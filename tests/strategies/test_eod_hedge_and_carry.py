"""Regression tests for the 2026-08-20 EOD hedge-and-carry feature (user spec).

Covers: find_hedge_strike (pure), the T-1-from-expiry / both-legs-in-loss
checks, StraddlePosition hedge-field persistence round-trip, and the full
EOD decision (_eod_close_or_hedge) — hedge trigger, T-1 override, degenerate
same-strike-at-construction fallback, and the ongoing same-strike collision
guard. `_dispatch_hedge_order` is stubbed directly (same pattern
test_itm_pair_gate.py uses for `_emit_order`) so these tests don't need to
drive the real bus/bridge machinery.
"""
import asyncio
import datetime
from types import SimpleNamespace

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from strategies.sell_straddle.selection import find_hedge_strike


# ── find_hedge_strike (pure) ─────────────────────────────────────────────────

def test_find_hedge_strike_ce_scans_upward_for_half_price():
    strike_prem = {
        (24400, "CE"): {"ltp": 100.0},
        (24450, "CE"): {"ltp": 70.0},
        (24500, "CE"): {"ltp": 48.0},   # first <= 50% of 100
        (24550, "CE"): {"ltp": 30.0},
    }
    result = find_hedge_strike(strike_prem, "CE", 24400, 100.0, step=50.0)
    assert result == (24500, 48.0)


def test_find_hedge_strike_pe_scans_downward_for_half_price():
    strike_prem = {
        (24000, "PE"): {"ltp": 90.0},
        (23950, "PE"): {"ltp": 60.0},
        (23900, "PE"): {"ltp": 44.0},   # first <= 50% of 90
    }
    result = find_hedge_strike(strike_prem, "PE", 24000, 90.0, step=50.0)
    assert result == (23900, 44.0)


def test_find_hedge_strike_returns_none_when_nothing_qualifies():
    strike_prem = {(24450, "CE"): {"ltp": 90.0}}   # never drops to <=50%
    assert find_hedge_strike(strike_prem, "CE", 24400, 100.0, step=50.0, max_offset_steps=2) is None


def test_find_hedge_strike_returns_none_on_zero_ltp():
    assert find_hedge_strike({}, "CE", 24400, 0.0, step=50.0) is None


# ── strategy harness ──────────────────────────────────────────────────────

def _make(bus=None, expiry_offset_days: int = 5) -> SellStraddleStrategy:
    bus = bus or EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._client_id, s._binding_id = "C", "B"
    today = datetime.datetime.now(IST).date()
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, 100.0, 130.0, open_time=datetime.datetime.now(IST)),  # in loss
        pe_leg=StraddleLeg("PE", 24000, 100.0, 120.0, open_time=datetime.datetime.now(IST)),  # in loss
        net_credit=200.0, status="open",
        expiry_date=today + datetime.timedelta(days=expiry_offset_days),
    )
    s._strike_prem = {
        (24500, "CE"): {"ltp": 60.0},   # <=50% of 130
        (23500, "PE"): {"ltp": 55.0},   # <=50% of 120
    }
    return s


def _stub_dispatch(s, fills: dict) -> list:
    """Replace _dispatch_hedge_order with a canned responder. `fills` maps
    (action, side, strike) -> a fill-like object (or None for a failed/timed-out
    dispatch). Returns the list of calls made, for assertions."""
    calls = []

    async def _fake(action, side, strike, price, entry_price, expiry, reason):
        calls.append((action, side, int(strike)))
        return fills.get((action, side, int(strike)))

    s._dispatch_hedge_order = _fake
    return calls


def _fill(action: str, side: str, strike: int, price: float) -> SimpleNamespace:
    return SimpleNamespace(
        action=action, option_type=side, strike=strike, fill_price=price,
        entry_aborted=False, routing_failed=False, exit_failed=False,
    )


# ── _both_legs_in_loss / _is_t1_from_expiry ─────────────────────────────────

def test_both_legs_in_loss_true_when_both_above_entry():
    s = _make()
    assert s._both_legs_in_loss(s._position) is True


def test_both_legs_in_loss_false_when_one_leg_profitable():
    s = _make()
    s._position.pe_leg.ltp = 50.0   # now profitable (entry 100 -> ltp 50)
    assert s._both_legs_in_loss(s._position) is False


def test_is_t1_from_expiry_true_when_expiry_is_tomorrow():
    s = _make(expiry_offset_days=1)
    now = datetime.datetime.now(IST)
    assert s._is_t1_from_expiry(s._position, now) is True


def test_is_t1_from_expiry_false_when_expiry_is_far_out():
    s = _make(expiry_offset_days=5)
    now = datetime.datetime.now(IST)
    assert s._is_t1_from_expiry(s._position, now) is False


# ── StraddlePosition hedge-field persistence round-trip ─────────────────────

def test_straddle_position_hedge_fields_roundtrip():
    s = _make()
    s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 60.0)
    s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 55.0)
    s._position.is_hedged_positional = True
    d = s._position.to_dict()
    restored = StraddlePosition.from_dict(d)
    assert restored.is_hedged_positional is True
    assert restored.hedge_ce_leg.strike == 24500
    assert restored.hedge_pe_leg.strike == 23500


def test_straddle_position_hedge_fields_default_none():
    d = StraddlePosition(underlying="NIFTY", atm_at_entry=0, entry_spot=0).to_dict()
    restored = StraddlePosition.from_dict(d)
    assert restored.hedge_ce_leg is None
    assert restored.hedge_pe_leg is None
    assert restored.is_hedged_positional is False


# ── _eod_close_or_hedge: full decision ladder ────────────────────────────────

def test_eod_builds_hedge_when_both_legs_in_loss_and_enabled():
    async def run():
        s = _make()
        s._hedge_carry_enabled = True
        calls = _stub_dispatch(s, {
            ("BUY", "CE", 24500): _fill("BUY", "CE", 24500, 60.0),
            ("BUY", "PE", 23500): _fill("BUY", "PE", 23500, 55.0),
        })
        closed = []
        s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == []   # normal close must NOT have run
        assert s._stop_for_day is True
        assert s._position.is_hedged_positional is True
        assert s._position.hedge_ce_leg.strike == 24500
        assert s._position.hedge_pe_leg.strike == 23500
        assert ("BUY", "CE", 24500) in calls
        assert ("BUY", "PE", 23500) in calls
    asyncio.run(run())


def test_eod_normal_close_when_hedge_disabled():
    async def run():
        s = _make()
        s._hedge_carry_enabled = False
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
        assert s._stop_for_day is True
    asyncio.run(run())


def test_eod_normal_close_when_only_one_leg_in_loss():
    async def run():
        s = _make()
        s._hedge_carry_enabled = True
        s._position.pe_leg.ltp = 50.0   # PE now profitable
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
    asyncio.run(run())


def test_eod_falls_back_to_close_when_no_valid_hedge_strike_found():
    async def run():
        s = _make()
        s._hedge_carry_enabled = True
        s._strike_prem = {}   # nothing quoted -- find_hedge_strike returns None for both sides
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
        assert s._position.is_hedged_positional is False
    asyncio.run(run())


def test_eod_degenerate_same_strike_at_construction_falls_back_to_close():
    """The computed hedge strike happens to equal the sold leg's own strike --
    not a real hedge, must fall back to a normal close instead of buying back
    the identical contract just sold."""
    async def run():
        s = _make()
        s._hedge_carry_enabled = True
        # CE hedge candidate search lands exactly on the sold CE's own strike.
        s._strike_prem = {
            (24000, "CE"): {"ltp": 60.0},   # == pos.ce_leg.strike, <=50% of 130
            (23500, "PE"): {"ltp": 55.0},
        }
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        calls = _stub_dispatch(s, {})   # should never be called

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
        assert calls == []
    asyncio.run(run())


def test_eod_t1_from_expiry_forces_normal_close_even_if_both_legs_in_loss():
    async def run():
        s = _make(expiry_offset_days=1)   # T-1
        s._hedge_carry_enabled = True
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        calls = _stub_dispatch(s, {})   # hedge must never even be attempted

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
        assert calls == []
    asyncio.run(run())


def test_eod_t1_from_expiry_closes_standing_hedge_legs_first():
    async def run():
        s = _make(expiry_offset_days=1)   # T-1
        s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 40.0)
        s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 35.0)
        s._position.is_hedged_positional = True
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        calls = _stub_dispatch(s, {
            ("SELL", "CE", 24500): _fill("SELL", "CE", 24500, 40.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 35.0),
        })

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert ("SELL", "CE", 24500) in calls
        assert ("SELL", "PE", 23500) in calls
        assert s._position.hedge_ce_leg is None
        assert s._position.hedge_pe_leg is None
        assert closed == ["eod_squareoff"]
    asyncio.run(run())


def test_eod_already_hedged_not_t1_does_nothing():
    async def run():
        s = _make(expiry_offset_days=5)   # not T-1
        s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 40.0)
        s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 35.0)
        s._position.is_hedged_positional = True
        closed = []
        s._close_position = lambda reason: closed.append(reason)
        calls = _stub_dispatch(s, {})

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == []
        assert calls == []
        assert s._position.status == "open"
    asyncio.run(run())


# ── same-strike collision guard (in _check_exits) ────────────────────────────

def test_same_strike_collision_closes_everything():
    async def run():
        s = _make()
        s._force_exit = datetime.time(23, 59)   # never past EOD in this test
        s._ltp_decay_enabled = False
        s._tsl_enabled = False
        s._vwap_rise_enabled = False
        s._exit_rules = []
        s._day_profit_target_pct = 0.0
        s._day_loss_sl_pct = 0.0
        s._ratio_threshold = 999.0
        s._itm_pair_gate_enabled = False
        s._day_low_exit_enabled = False

        # Hedge CE leg happens to sit at the exact same strike as the (freshly
        # rolled) sold CE leg -- the collision this guard exists to catch.
        s._position.hedge_ce_leg = StraddleLeg("CE", 24000, 60.0, 45.0)
        s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 40.0)
        s._position.is_hedged_positional = True

        hedge_calls = _stub_dispatch(s, {
            ("SELL", "CE", 24000): _fill("SELL", "CE", 24000, 45.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 40.0),
        })
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close

        await s._check_exits()

        assert closed == ["hedge_strike_collision"]
        assert ("SELL", "CE", 24000) in hedge_calls
        assert ("SELL", "PE", 23500) in hedge_calls
    asyncio.run(run())


def test_no_collision_when_strikes_differ():
    async def run():
        s = _make()
        s._force_exit = datetime.time(23, 59)
        s._ltp_decay_enabled = False
        s._tsl_enabled = False
        s._vwap_rise_enabled = False
        s._exit_rules = []
        s._day_profit_target_pct = 0.0
        s._day_loss_sl_pct = 0.0
        s._ratio_threshold = 999.0
        s._itm_pair_gate_enabled = False
        s._day_low_exit_enabled = False

        s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 45.0)   # different strike
        s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 40.0)   # different strike
        s._position.is_hedged_positional = True

        closed = []
        s._close_position = lambda reason: closed.append(reason)

        await s._check_exits()

        assert closed == []
        assert s._position.status == "open"
    asyncio.run(run())
