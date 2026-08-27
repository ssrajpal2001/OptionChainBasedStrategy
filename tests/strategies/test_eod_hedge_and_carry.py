"""Regression tests for the 2026-08-20 EOD hedge-and-carry feature (user spec),
the 2026-08-24 correction (user spec: cumulative P&L trigger, tick-by-tick
profit close, reset_session day-boundary fix), and the SAME-DAY follow-up
correction (user spec): T-1-from-expiry no longer force-closes a hedge or
blocks building one -- it ROLLS the position to next week's expiry instead,
since NSE cash-settles the current week's contracts at expiry regardless of
what this code does.

Covers: find_hedge_strike (pure), _cumulative_hedge_pnl, T-1-from-expiry,
StraddlePosition hedge-field persistence round-trip (incl. hedge_unrealized_pnl),
the full EOD decision (_eod_close_or_hedge) — hedge trigger, T-1-triggers-roll
(not close), degenerate same-strike-at-construction fallback, the ongoing
same-strike collision guard, the tick-by-tick _check_hedge_cumulative_profit_close,
_start_hedge_roll / _try_complete_hedge_roll, and reset_session's day-boundary
hedge-preservation guard. `_dispatch_hedge_order` is stubbed directly (same
pattern test_itm_pair_gate.py uses for `_emit_order`) so these tests don't need
to drive the real bus/bridge machinery.
"""
import asyncio
import datetime
from types import SimpleNamespace

import pytest

from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from strategies.sell_straddle.selection import find_hedge_strike


@pytest.fixture(autouse=True)
def _restore_registry_expiries():
    """REGISTRY is a process-wide singleton -- tests here overwrite
    REGISTRY._expiries["NIFTY"] to control what _start_hedge_roll finds as
    the "next expiry". Restore afterward so this file can't leak state into
    other test files running later in the same session (a real bug found and
    fixed earlier this session in a different test file)."""
    original = REGISTRY._expiries.get("NIFTY")
    yield
    if original is None:
        REGISTRY._expiries.pop("NIFTY", None)
    else:
        REGISTRY._expiries["NIFTY"] = original


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


# ── _cumulative_hedge_pnl / _is_t1_from_expiry ──────────────────────────────
# 2026-08-24 user spec correction: the hedge-build trigger is the OVERALL
# cumulative P&L (booked + running sold legs), not "both legs individually
# in loss" -- CE +50 / PE -80 (net -30) now qualifies, where the old per-leg
# check would have skipped it since CE alone was "in profit".

def test_cumulative_hedge_pnl_negative_when_both_legs_in_loss():
    s = _make()   # ce_leg entry=100 ltp=130 (-30), pe_leg entry=100 ltp=120 (-20)
    assert s._cumulative_hedge_pnl(s._position) == -50.0


def test_cumulative_hedge_pnl_still_negative_with_one_leg_profitable():
    """The example from the correction: CE +50, PE -80, net -30 overall --
    must still qualify for a hedge even though CE alone is profitable."""
    s = _make()
    s._position.ce_leg.entry_price, s._position.ce_leg.ltp = 100.0, 50.0    # CE +50
    s._position.pe_leg.entry_price, s._position.pe_leg.ltp = 100.0, 180.0   # PE -80
    s._position.net_credit = 200.0
    assert s._cumulative_hedge_pnl(s._position) == -30.0


def test_cumulative_hedge_pnl_positive_when_net_profitable():
    s = _make()
    s._position.ce_leg.ltp = 40.0   # CE +60
    s._position.pe_leg.ltp = 30.0   # PE +70
    assert s._cumulative_hedge_pnl(s._position) == 130.0


def test_cumulative_hedge_pnl_includes_booked_session_pnl():
    s = _make()
    s._session_realized_pnl_pts = 45.0
    assert s._cumulative_hedge_pnl(s._position) == -50.0 + 45.0


def test_cumulative_hedge_pnl_include_hedge_nets_in_hedge_legs():
    s = _make()
    s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 90.0)   # bought 60, now 90: +30
    s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 40.0)   # bought 55, now 40: -15
    # sold legs net -50 (as in test_cumulative_hedge_pnl_negative_when_both_legs_in_loss)
    # + hedge legs net +15 (=+30-15) -> total -35
    assert s._cumulative_hedge_pnl(s._position, include_hedge=True) == -35.0
    # include_hedge=False must NOT be affected by the hedge legs
    assert s._cumulative_hedge_pnl(s._position, include_hedge=False) == -50.0


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


def _seed_next_expiry(pos_expiry: datetime.date, weeks_out: int = 1):
    """Give REGISTRY a later expiry than pos_expiry so _start_hedge_roll has
    somewhere to roll onto."""
    REGISTRY._expiries["NIFTY"] = [pos_expiry, pos_expiry + datetime.timedelta(days=7 * weeks_out)]


def test_eod_t1_from_expiry_rolls_instead_of_closing_when_cumulative_negative():
    """2026-08-24 correction: T-1 no longer forces a plain close -- it rolls
    to next week's expiry, same as any other day's hedge trigger would, just
    onto fresh (non-expiring) contracts instead of the current dying ones."""
    async def run():
        s = _make(expiry_offset_days=1)   # T-1, sold legs net -50 (cumulative < 0)
        s._hedge_carry_enabled = True
        _seed_next_expiry(s._position.expiry_date)
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        calls = _stub_dispatch(s, {})   # no hedge legs stood yet -- nothing to dispatch here

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["t1_new_hedge_roll"]
        assert calls == []
        assert s._hedge_roll_pending is True
        assert s._hedge_roll_reason == "t1_new_hedge_roll"
        assert s._entry_expiry_date == s._position.expiry_date + datetime.timedelta(days=7)
        assert s._expiry_shifted_low_anchor_ltp is True
        assert s._strike_prem == {}
    asyncio.run(run())


def test_eod_t1_no_roll_when_cumulative_not_negative():
    """T-1 with a normal (non-hedge-candidate) position still just closes
    plainly -- the roll only replaces the OLD "always force-close" behavior
    for the specific case that would otherwise have been hedged."""
    async def run():
        s = _make(expiry_offset_days=1)   # T-1
        s._hedge_carry_enabled = True
        s._position.ce_leg.ltp = 40.0   # CE +60
        s._position.pe_leg.ltp = 30.0   # PE +70 -- cumulative +130, not negative
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        calls = _stub_dispatch(s, {})

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["eod_squareoff"]
        assert calls == []
        assert s._hedge_roll_pending is False
    asyncio.run(run())


def test_eod_t1_already_hedged_rolls_closing_old_hedge_legs_for_real():
    """A hedge carried from an earlier day, reaching T-1 on its own sold
    legs' expiry: the standing hedge legs are closed for real (never
    stashed -- they're on the same expiring contract as the sold legs, so
    carrying them onto a next-week sold pair would mismatch expiries)."""
    async def run():
        s = _make(expiry_offset_days=1)   # T-1
        s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, 40.0)
        s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, 35.0)
        s._position.is_hedged_positional = True
        _seed_next_expiry(s._position.expiry_date)
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
        assert closed == ["t1_hedge_roll"]
        assert s._hedge_roll_pending is True
        assert s._hedge_roll_reason == "t1_hedge_roll"
    asyncio.run(run())


def test_eod_t1_roll_logs_critical_and_gives_up_when_no_next_expiry():
    """REGISTRY has no expiry past the current one -- the roll can't happen.
    Position is still closed (never left dangling), but no roll is pending."""
    async def run():
        s = _make(expiry_offset_days=1)
        s._hedge_carry_enabled = True
        REGISTRY._expiries["NIFTY"] = [s._position.expiry_date]   # nothing later
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close

        await s._eod_close_or_hedge(s._position, datetime.datetime.now(IST))

        assert closed == ["t1_new_hedge_roll"]
        assert s._hedge_roll_pending is False
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


# ── _check_hedge_cumulative_profit_close (2026-08-24, tick-by-tick) ─────────

def _hedged(s, ce_hedge_ltp=90.0, pe_hedge_ltp=40.0):
    """Attach a standing hedge to s._position: bought CE@60 (now ce_hedge_ltp),
    bought PE@55 (now pe_hedge_ltp)."""
    s._position.hedge_ce_leg = StraddleLeg("CE", 24500, 60.0, ce_hedge_ltp)
    s._position.hedge_pe_leg = StraddleLeg("PE", 23500, 55.0, pe_hedge_ltp)
    s._position.is_hedged_positional = True
    return s._position


def test_hedge_cumulative_profit_close_fires_and_closes_all_four_legs():
    async def run():
        s = _make()   # sold legs net -50 (ce -30, pe -20)
        pos = _hedged(s, ce_hedge_ltp=200.0, pe_hedge_ltp=40.0)
        # hedge: CE 60->200 (+140), PE 55->40 (-15) => hedge net +125
        # total = -50 (sold) + 0 (booked) + 125 (hedge) = +75 -> should fire
        hedge_calls = _stub_dispatch(s, {
            ("SELL", "CE", 24500): _fill("SELL", "CE", 24500, 200.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 40.0),
        })
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        cooldowns = []
        s._apply_sl_cooldown = lambda rule_key="entry_rules_reentry": cooldowns.append(rule_key)

        fired = await s._check_hedge_cumulative_profit_close(pos, datetime.datetime.now(IST))

        assert fired is True
        assert ("SELL", "CE", 24500) in hedge_calls
        assert ("SELL", "PE", 23500) in hedge_calls
        assert pos.hedge_ce_leg is None and pos.hedge_pe_leg is None
        assert closed == ["hedge_cumulative_profit"]
        # Next entry must use BEGINNING rules, not re-entry -- the whole point
        # of "start fresh" (user spec).
        assert cooldowns == ["entry_rules_beginning"]
    asyncio.run(run())


def test_hedge_cumulative_profit_close_does_not_fire_when_still_negative():
    async def run():
        s = _make()   # sold legs net -50
        pos = _hedged(s, ce_hedge_ltp=61.0, pe_hedge_ltp=56.0)
        # hedge: CE 60->61 (+1), PE 55->56 (+1) => hedge net +2
        # total = -50 + 0 + 2 = -48 -> must NOT fire
        hedge_calls = _stub_dispatch(s, {})
        closed = []
        s._close_position = lambda reason: closed.append(reason)

        fired = await s._check_hedge_cumulative_profit_close(pos, datetime.datetime.now(IST))

        assert fired is False
        assert hedge_calls == []
        assert closed == []
        assert pos.is_hedged_positional is True
    asyncio.run(run())


def test_hedge_cumulative_profit_close_includes_booked_session_pnl():
    async def run():
        s = _make()   # sold legs net -50
        s._session_realized_pnl_pts = 60.0   # booked earlier today
        pos = _hedged(s, ce_hedge_ltp=61.0, pe_hedge_ltp=56.0)   # hedge net +2
        # total = -50 + 60 + 2 = +12 -> should fire purely because of the booked P&L
        hedge_calls = _stub_dispatch(s, {
            ("SELL", "CE", 24500): _fill("SELL", "CE", 24500, 61.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 56.0),
        })
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        s._apply_sl_cooldown = lambda rule_key="entry_rules_reentry": None

        fired = await s._check_hedge_cumulative_profit_close(pos, datetime.datetime.now(IST))

        assert fired is True
        assert closed == ["hedge_cumulative_profit"]
    asyncio.run(run())


def test_hedge_cumulative_profit_close_requires_rs500_not_just_breakeven():
    """2026-08-27, direct user correction: a net that merely nets to ~0 must
    NOT close the trade -- the trigger is a genuine Rs500 of real cumulative
    profit, not breakeven."""
    async def run():
        s = _make()   # sold legs net -50 pts
        s._session_realized_pnl_pts = 55.0   # booked earlier today
        # hedge net = 0 (ltp == entry_price on both hedge legs)
        # total = -50 + 55 + 0 = +5 pts -> Rs375 (lot=75) -- below the Rs500 bar
        pos = _hedged(s, ce_hedge_ltp=60.0, pe_hedge_ltp=55.0)
        hedge_calls = _stub_dispatch(s, {})
        closed = []
        s._close_position = lambda reason: closed.append(reason)

        fired = await s._check_hedge_cumulative_profit_close(pos, datetime.datetime.now(IST))

        assert fired is False
        assert hedge_calls == []
        assert closed == []
        assert pos.is_hedged_positional is True
    asyncio.run(run())


def test_hedge_cumulative_profit_close_fires_once_rs500_reached():
    async def run():
        s = _make()   # sold legs net -50 pts
        s._session_realized_pnl_pts = 57.0
        # total = -50 + 57 + 0 = +7 pts -> Rs525 (lot=75) -- clears the Rs500 bar
        pos = _hedged(s, ce_hedge_ltp=60.0, pe_hedge_ltp=55.0)
        hedge_calls = _stub_dispatch(s, {
            ("SELL", "CE", 24500): _fill("SELL", "CE", 24500, 60.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 55.0),
        })
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        s._apply_sl_cooldown = lambda rule_key="entry_rules_reentry": None

        fired = await s._check_hedge_cumulative_profit_close(pos, datetime.datetime.now(IST))

        assert fired is True
        assert closed == ["hedge_cumulative_profit"]
    asyncio.run(run())


def test_check_exits_hedge_profit_close_runs_before_other_exit_checks():
    """Wired into _check_exits ahead of the same-strike-collision guard and
    every normal sold-leg exit -- while hedged and cumulatively profitable,
    it must fire even if other exit thresholds would also technically match."""
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

        pos = _hedged(s, ce_hedge_ltp=200.0, pe_hedge_ltp=40.0)   # hedge net +125, total +75
        _stub_dispatch(s, {
            ("SELL", "CE", 24500): _fill("SELL", "CE", 24500, 200.0),
            ("SELL", "PE", 23500): _fill("SELL", "PE", 23500, 40.0),
        })
        closed = []
        async def _fake_close(reason):
            closed.append(reason)
            s._position.status = "closed"
        s._close_position = _fake_close
        s._apply_sl_cooldown = lambda rule_key="entry_rules_reentry": None

        await s._check_exits()

        assert closed == ["hedge_cumulative_profit"]
    asyncio.run(run())


# ── reset_session(): standing hedge survives a day-boundary transition ─────

def test_reset_session_preserves_position_when_hedged():
    s = _make()
    _hedged(s)
    s._trades_today = 3
    s.reset_session()
    assert s._position is not None
    assert s._position.is_hedged_positional is True
    assert s._position.hedge_ce_leg is not None
    # Per-day counters still reset normally -- only the position itself is preserved.
    assert s._trades_today == 0


def test_reset_session_still_clears_position_when_not_hedged():
    s = _make()   # is_hedged_positional defaults False
    s.reset_session()
    assert s._position is None


# ── 2026-08-25 real incident: dual EOD trigger race (candle-close vs tick
# loop both independently deciding EOD, one bypassing the hedge decision
# entirely) — pre-squareoff precheck + reentrancy guard ────────────────────

def _disable_other_exits(s):
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0
    s._itm_pair_gate_enabled = False
    s._day_low_exit_enabled = False


def test_hedge_precheck_time_true_only_in_lead_window_before_force_exit():
    s = _make()
    s._force_exit = datetime.time(15, 20)
    today = datetime.datetime.now(IST).date()
    _at = lambda hh, mm: datetime.datetime.combine(today, datetime.time(hh, mm)).replace(tzinfo=IST)
    # 1 minute before force_exit -- inside the precheck window.
    assert s._hedge_precheck_time(_at(15, 19)) is True
    # exactly at force_exit -- precheck window has closed (real squareoff owns this).
    assert s._hedge_precheck_time(_at(15, 20)) is False
    # well before -- not yet in the window.
    assert s._hedge_precheck_time(_at(15, 0)) is False


def test_check_exits_prehedge_builds_hedge_before_squareoff_deadline():
    """The core fix for the 2026-08-25 incident: the hedge decision now runs
    _HEDGE_PRECHECK_LEAD_MIN minutes BEFORE the hard squareoff deadline (here
    simulated via _hedge_precheck_time directly, since real wall-clock time in
    CI is not controllable), so by the time the real deadline hits, a hedge
    (if eligible) is already standing and the real EOD close path leaves it
    running instead of closing the sold legs out from under an in-flight
    hedge build."""
    async def run():
        s = _make()
        s._force_exit = datetime.time(23, 59)   # never past real EOD in this test
        s._hedge_carry_enabled = True
        _disable_other_exits(s)
        s._hedge_precheck_time = lambda now: True
        _stub_dispatch(s, {
            ("BUY", "CE", 24500): _fill("BUY", "CE", 24500, 60.0),
            ("BUY", "PE", 23500): _fill("BUY", "PE", 23500, 55.0),
        })
        closed = []
        s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)

        await s._check_exits()

        assert closed == []
        assert s._position.is_hedged_positional is True
        assert s._prehedge_attempted_today is True
    asyncio.run(run())


def test_check_exits_prehedge_only_fires_once_per_day():
    async def run():
        s = _make()
        s._force_exit = datetime.time(23, 59)
        s._hedge_carry_enabled = True
        _disable_other_exits(s)
        s._hedge_precheck_time = lambda now: True
        s._prehedge_attempted_today = True   # already fired earlier this session
        calls = _stub_dispatch(s, {})
        closed = []
        s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)

        await s._check_exits()

        assert calls == []
        assert closed == []
        assert s._position.is_hedged_positional is False
    asyncio.run(run())


def test_check_exits_real_squareoff_leaves_prehedged_position_running():
    """Once the precheck has already built the hedge, the real squareoff
    tick (_past_squareoff true) must find is_hedged_positional already set
    and leave it running -- never re-close the sold legs."""
    async def run():
        s = _make(expiry_offset_days=5)   # not T-1
        _disable_other_exits(s)
        _hedged(s)   # simulates a hedge already built by the precheck
        s._prehedge_attempted_today = True
        s._past_squareoff = lambda now: True
        closed = []
        s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)
        calls = _stub_dispatch(s, {})

        await s._check_exits()

        assert closed == []
        assert calls == []
        assert s._position.status == "open"
        assert s._position.is_hedged_positional is True
    asyncio.run(run())


def test_check_exits_eod_decision_in_progress_guard_blocks_reentry():
    """Simulates _eod_backstop_loop calling _check_exits() again for the same
    position while a FIRST call (e.g. from _tick_loop) is still mid-hedge-build
    -- the exact shape of the 2026-08-25 incident, just between the two
    surviving loops instead of the now-removed candle-close duplicate. The
    reentrancy guard must make the second call a pure no-op."""
    async def run():
        s = _make()
        s._hedge_carry_enabled = True
        _disable_other_exits(s)
        s._past_squareoff = lambda now: True
        s._eod_decision_in_progress = True   # simulates a first call already in flight
        calls = _stub_dispatch(s, {
            ("BUY", "CE", 24500): _fill("BUY", "CE", 24500, 60.0),
            ("BUY", "PE", 23500): _fill("BUY", "PE", 23500, 55.0),
        })
        closed = []
        s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)

        await s._check_exits()

        assert calls == []
        assert closed == []
        assert s._position.status == "open"
    asyncio.run(run())


# ── _start_hedge_roll / _try_complete_hedge_roll ────────────────────────────

def _auto_confirm_entries(s):
    """Auto-confirm any ENTRY order dispatched via _emit_order, through the
    real _on_fill path -- same pattern test_itm_pair_gate.py uses for EXIT
    fills. Needed because _try_complete_hedge_roll opens the fresh sold pair
    via the real _open_position -> _emit_order path."""
    from execution_bridge.straddle_bridge import StraddleFillEvent

    def _confirm(ev):
        if ev.action == "ENTRY":
            s._on_fill(StraddleFillEvent(
                action="ENTRY", underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id="C", binding_id="B", event_id=ev.event_id,
            ))
    async def _emit(ev):
        _confirm(ev)
    s._emit_order = _emit


def test_try_complete_hedge_roll_waits_for_live_atm_data():
    async def run():
        s = _make()
        s._hedge_roll_pending = True
        s._hedge_roll_reason = "t1_new_hedge_roll"
        s._entry_expiry_date = datetime.date.today() + datetime.timedelta(days=8)
        s._strike_prem = {}   # nothing quoted yet for the new expiry's ATM

        await s._try_complete_hedge_roll(datetime.datetime.now(IST))

        assert s._hedge_roll_pending is True   # still waiting
    asyncio.run(run())


def test_try_complete_hedge_roll_opens_fresh_pair_and_hedge_once_data_arrives():
    async def run():
        s = _make()
        _auto_confirm_entries(s)
        s._hedge_roll_pending = True
        s._hedge_roll_reason = "t1_new_hedge_roll"
        next_expiry = datetime.date.today() + datetime.timedelta(days=8)
        s._entry_expiry_date = next_expiry
        # ATM (spot=24000, step=50) = 24000. Both live now.
        s._strike_prem = {
            (24000, "CE"): {"ltp": 120.0},
            (24000, "PE"): {"ltp": 110.0},
            (24500, "CE"): {"ltp": 55.0},   # <=50% of 120, hedge candidate
            (23500, "PE"): {"ltp": 50.0},   # <=50% of 110, hedge candidate
        }
        hedge_calls = _stub_dispatch(s, {
            ("BUY", "CE", 24500): _fill("BUY", "CE", 24500, 55.0),
            ("BUY", "PE", 23500): _fill("BUY", "PE", 23500, 50.0),
        })

        await s._try_complete_hedge_roll(datetime.datetime.now(IST))

        assert s._hedge_roll_pending is False
        assert s._position is not None
        assert s._position.status == "open"
        assert s._position.ce_leg.strike == 24000
        assert s._position.pe_leg.strike == 24000
        assert s._position.expiry_date == next_expiry
        assert s._position.is_hedged_positional is True
        assert s._position.hedge_ce_leg.strike == 24500
        assert s._position.hedge_pe_leg.strike == 23500
        assert ("BUY", "CE", 24500) in hedge_calls
        assert ("BUY", "PE", 23500) in hedge_calls
    asyncio.run(run())


def test_try_complete_hedge_roll_noop_when_not_pending():
    async def run():
        s = _make()
        s._hedge_roll_pending = False
        called = []
        s._open_position = lambda *a, **k: called.append(1)

        await s._try_complete_hedge_roll(datetime.datetime.now(IST))

        assert called == []
    asyncio.run(run())


def test_maybe_try_entry_routes_to_hedge_roll_completion_when_pending():
    """Wired into the real entry loop -- a pending roll takes priority over
    normal entry-rule evaluation."""
    async def run():
        s = _make()
        s._position = None   # roll already closed the old position
        s._hedge_roll_pending = True
        called = []
        async def _fake_complete(now):
            called.append(now)
        s._try_complete_hedge_roll = _fake_complete
        s._any_active_terminal = lambda: True

        await s._maybe_try_entry(datetime.datetime.now(IST))

        assert len(called) == 1
    asyncio.run(run())
