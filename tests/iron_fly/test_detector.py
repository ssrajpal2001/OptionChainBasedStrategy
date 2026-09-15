"""Regression tests for strategies/iron_fly/detector.py -- the pure Iron
Condor -> Iron Fly logic (strike search, +/-100 roll, gap-pending, ATM
conversion + leg reconciliation, cycle profit math). See CLAUDE.md's own
"CAG Long Straddle" precedent for the package-layout convention this
follows; the plan approved 2026-09-13 is the source of truth for the
mechanic being locked in here.
"""
from strategies.iron_fly.detector import (
    round_to_atm,
    find_short_strike,
    find_long_strike,
    classify_move,
    PendingAdjustment,
    check_pending_fire,
    check_atm_conversion,
    reconcile_protective_leg,
    expected_max_profit,
    leg_pnl,
    cycle_pnl,
    profit_target_hit,
    Leg,
    should_use_next_week_expiry,
)
from datetime import date


# ── round_to_atm ─────────────────────────────────────────────────────────────

def test_round_to_atm_rounds_to_nearest_strike_step():
    assert round_to_atm(25032, 50) == 25050
    assert round_to_atm(25018, 50) == 25000


# ── find_short_strike / find_long_strike ────────────────────────────────────

def test_find_short_strike_picks_farthest_strike_still_above_threshold():
    # Strikes ordered nearest-ATM-first, premiums decreasing with distance.
    strikes = [25000, 25050, 25100, 25150, 25200]
    premiums = {25000: 120.0, 25050: 65.0, 25100: 28.0, 25150: 15.0, 25200: 8.0}
    assert find_short_strike(strikes, premiums, threshold=20.0) == 25100


def test_find_short_strike_returns_none_if_even_nearest_is_below_threshold():
    strikes = [25000, 25050]
    premiums = {25000: 18.0, 25050: 10.0}
    assert find_short_strike(strikes, premiums, threshold=20.0) is None


def test_find_short_strike_stops_at_missing_data():
    strikes = [25000, 25050, 25100]
    premiums = {25000: 120.0}  # 25050/25100 never ticked yet
    assert find_short_strike(strikes, premiums, threshold=20.0) == 25000


def test_find_long_strike_picks_first_strike_below_threshold():
    strikes = [25150, 25200, 25250]  # continuing outward past the short strike
    premiums = {25150: 28.0, 25200: 15.0, 25250: 8.0}
    assert find_long_strike(strikes, premiums, threshold=20.0) == 25200


def test_find_long_strike_returns_none_on_missing_data_before_a_hit():
    strikes = [25150, 25200]
    premiums = {25150: 28.0}
    assert find_long_strike(strikes, premiums, threshold=20.0) is None


# ── classify_move (±100 roll + gap detection) ───────────────────────────────

def test_classify_move_no_trigger_within_band():
    assert classify_move(25050, reference=25000, distance=100.0) == (None, False)


def test_classify_move_ordinary_fall_triggers_call_no_gap():
    # First tick to cross the boundary is right at/just past it -- ordinary.
    assert classify_move(24900, reference=25000, distance=100.0) == ("CALL", False)


def test_classify_move_gap_down_flags_gap_and_still_names_call():
    # Doc's own worked example: reference=25000, call level=24900, gaps to 24700.
    assert classify_move(24700, reference=25000, distance=100.0) == ("CALL", True)


def test_classify_move_ordinary_rise_triggers_put_no_gap():
    assert classify_move(25100, reference=25000, distance=100.0) == ("PUT", False)


def test_classify_move_gap_up_flags_gap_and_still_names_put():
    assert classify_move(25300, reference=25000, distance=100.0) == ("PUT", True)


# ── PendingAdjustment / check_pending_fire ──────────────────────────────────

def test_pending_call_adjustment_fires_when_price_rises_back_to_trigger():
    pending = PendingAdjustment(side="CALL", trigger_price=24900)
    assert check_pending_fire(pending, 24750) is False
    assert check_pending_fire(pending, 24900) is True
    assert check_pending_fire(pending, 24950) is True


def test_pending_put_adjustment_fires_when_price_falls_back_to_trigger():
    pending = PendingAdjustment(side="PUT", trigger_price=25100)
    assert check_pending_fire(pending, 25250) is False
    assert check_pending_fire(pending, 25100) is True
    assert check_pending_fire(pending, 25050) is True


# ── check_atm_conversion ─────────────────────────────────────────────────────

def test_short_pe_becomes_atm_flags_put_side_conversion():
    # NIFTY falls to exactly the short PE strike.
    assert check_atm_conversion(
        nifty_price=24500, short_ce_strike=25150, short_pe_strike=24500,
    ) == "PUT_BECOMES_ATM"


def test_short_ce_becomes_atm_flags_call_side_conversion():
    assert check_atm_conversion(
        nifty_price=25500, short_ce_strike=25500, short_pe_strike=24850,
    ) == "CALL_BECOMES_ATM"


def test_no_conversion_when_price_is_between_both_short_strikes():
    assert check_atm_conversion(
        nifty_price=25020, short_ce_strike=25150, short_pe_strike=24850,
    ) is None


def test_gap_up_through_sold_call_flags_call_side_conversion():
    # Direct user spec (2026-09-14): a GAP that skips straight past the sold
    # call strike (never landing exactly on it) must still convert
    # immediately -- NIFTY opens at 25,080 with sold call = 25,000 (doc's
    # own worked example). The strike itself (25,000) is kept and becomes
    # the fly's short leg, per direct user confirmation -- NOT the literal
    # rounded ATM (25,100) the gap actually landed on.
    assert check_atm_conversion(
        nifty_price=25080, short_ce_strike=25000, short_pe_strike=24900,
    ) == "CALL_BECOMES_ATM"


def test_gap_down_through_sold_put_flags_put_side_conversion():
    # Doc's own gap-down worked example: sold put = 24,900, NIFTY opens at
    # 24,800 -- skips straight past 24,900 without ever landing on it.
    assert check_atm_conversion(
        nifty_price=24800, short_ce_strike=25000, short_pe_strike=24900,
    ) == "PUT_BECOMES_ATM"


# ── reconcile_protective_leg ─────────────────────────────────────────────────

def test_reconcile_protective_leg_keeps_existing_otm1_long():
    # Doc Point 8: short 24,500 PE / existing long 24,450 PE (already OTM1 below) -> keep.
    needs_replace, correct = reconcile_protective_leg(
        short_strike=24500, existing_long_strike=24450, otm1=50, direction=-1,
    )
    assert needs_replace is False
    assert correct == 24450


def test_reconcile_protective_leg_replaces_a_farther_long():
    # Doc Point 8: short 24,500 PE / existing long 24,400 PE (100 away) -> replace with 24,450.
    needs_replace, correct = reconcile_protective_leg(
        short_strike=24500, existing_long_strike=24400, otm1=50, direction=-1,
    )
    assert needs_replace is True
    assert correct == 24450


def test_reconcile_protective_leg_call_side_direction_is_positive():
    # Doc Point 10: short 25,500 CE / existing long 25,600 CE -> replace with 25,550.
    needs_replace, correct = reconcile_protective_leg(
        short_strike=25500, existing_long_strike=25600, otm1=50, direction=1,
    )
    assert needs_replace is True
    assert correct == 25550


# ── profit / P&L math ────────────────────────────────────────────────────────

def test_expected_max_profit_is_net_credit_times_qty():
    # Sell CE 65, buy CE 28 (credit 37) + sell PE 60, buy PE 25 (credit 35) = 72/lot.
    assert expected_max_profit(
        short_ce_premium=65.0, long_ce_premium=28.0,
        short_pe_premium=60.0, long_pe_premium=25.0,
        qty=75,
    ) == 72.0 * 75


def test_leg_pnl_short_leg_profits_as_premium_falls():
    leg = Leg(strike=25100, entry_price=28.0, qty=75, is_short=True, side="CE")
    assert leg_pnl(leg, live_price=10.0) == (28.0 - 10.0) * 75


def test_leg_pnl_long_leg_profits_as_premium_rises():
    leg = Leg(strike=25150, entry_price=15.0, qty=75, is_short=False, side="CE")
    assert leg_pnl(leg, live_price=22.0) == (22.0 - 15.0) * 75


def test_cycle_pnl_sums_realized_and_open_legs():
    legs = [
        Leg(strike=25100, entry_price=28.0, qty=75, is_short=True, side="CE"),
        Leg(strike=25150, entry_price=15.0, qty=75, is_short=False, side="CE"),
    ]
    live = {(25100, "CE"): 20.0, (25150, "CE"): 12.0}
    # realized 500 + short-leg open pnl (28-20)*75=600 + long-leg open pnl (12-15)*75=-225
    assert cycle_pnl(realized_pnl=500.0, open_legs=legs, live_premiums=live) == 500.0 + 600.0 - 225.0


def test_cycle_pnl_does_not_collide_when_ce_and_pe_share_a_strike():
    # Real bug found via the first live backtest run (2026-09-13): once a
    # position converts to an Iron Fly, the short CE and short PE sit at the
    # SAME strike. A live_premiums dict keyed by strike alone silently
    # collides -- the CE leg's lookup would return the PE leg's premium (or
    # vice versa), corrupting the 65%-profit-target decision. Keying by
    # (strike, side) is the fix this test locks in.
    legs = [
        Leg(strike=23350, entry_price=100.0, qty=75, is_short=True, side="CE"),
        Leg(strike=23350, entry_price=25.0, qty=75, is_short=True, side="PE"),
    ]
    live = {(23350, "CE"): 120.0, (23350, "PE"): 90.0}
    # CE short pnl = (100-120)*75 = -1500; PE short pnl = (25-90)*75 = -4875
    assert cycle_pnl(realized_pnl=0.0, open_legs=legs, live_premiums=live) == -1500.0 - 4875.0


def test_profit_target_hit_true_at_or_above_65_pct():
    assert profit_target_hit(current_cycle_pnl=3900.0, expected_max_profit=6000.0, target_pct=0.65) is True
    assert profit_target_hit(current_cycle_pnl=3899.0, expected_max_profit=6000.0, target_pct=0.65) is False


# ── should_use_next_week_expiry (real incident 2026-09-15, simplified 2026-09-16) ──
#
# Direct user spec, final form: "we will NEVER take trade of same week
# expiry on expiry day" -- unconditional, no time-of-day check at all. Same
# for the day before expiry ("if 1 day before ... 65% is achieved then also
# it will jump to next week"). Only DTE (days to expiry) matters now -- the
# original 15:00 cutoff-time concept was explicitly dropped as too narrow
# once the user saw DTE=0 needed the SAME unconditional treatment as DTE=1.

def test_expiry_day_itself_always_jumps_even_in_the_morning():
    # This would have changed the real 2026-09-15 09:21 first entry of the
    # day to use Sep 22 instead of Sep 15 -- direct user confirmation this
    # is now the intended behavior, overriding the earlier cutoff-gated design.
    assert should_use_next_week_expiry(today=date(2026, 9, 15), active_expiry=date(2026, 9, 15)) is True


def test_day_before_expiry_always_jumps_even_in_the_morning():
    assert should_use_next_week_expiry(today=date(2026, 9, 14), active_expiry=date(2026, 9, 15)) is True


def test_two_days_before_expiry_does_not_jump():
    # DTE >= 2 -- plenty of runway, use the current contract as normal.
    assert should_use_next_week_expiry(today=date(2026, 9, 13), active_expiry=date(2026, 9, 15)) is False


def test_non_expiry_week_never_triggers():
    assert should_use_next_week_expiry(today=date(2026, 9, 11), active_expiry=date(2026, 9, 15)) is False
