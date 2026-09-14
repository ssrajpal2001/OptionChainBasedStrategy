"""Regression tests for strategies/iron_fly/engine.py -- the IronFlyEngine
state machine that drives strategies/iron_fly/detector.py's pure functions
against a spot price + a pluggable premium lookup. Deliberately synchronous
(no EventBus/asyncio) for Phase 1 -- this is the class scripts/
iron_fly_backtest.py drives directly against replayed real historical data;
a live async wrapper is Phase 2+ scope per the approved plan.
"""
from strategies.iron_fly.engine import IronFlyEngine, snapshot_legs, diff_legs_for_orders


def _premium_fn(table):
    """table: {(strike, side): price}. Returns a get_premium(strike, side)
    callable a test can mutate between ticks by editing `table` in place."""
    def _get(strike, side):
        return table.get((strike, side))
    return _get


def _entry_table():
    # Short strikes deliberately land 150pts from ATM (not 100) so they never
    # coincide with the +/-100 roll trigger distance in these tests -- that
    # coincidence is real-world possible (ATM conversion legitimately takes
    # priority over a same-tick roll, see engine.py), but these tests target
    # the roll/gap mechanics in isolation.
    return {
        (25000, "CE"): 150.0, (25050, "CE"): 90.0, (25100, "CE"): 55.0, (25150, "CE"): 22.0, (25200, "CE"): 8.0,
        (25000, "PE"): 148.0, (24950, "PE"): 88.0, (24900, "PE"): 53.0, (24850, "PE"): 21.0, (24800, "PE"): 8.0,
    }


# ── entry ────────────────────────────────────────────────────────────────────

def test_try_enter_selects_expected_legs_and_records_reference():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    ok = eng.try_enter(25000, _premium_fn(table))
    assert ok is True
    assert eng.short_ce.strike == 25150 and eng.long_ce.strike == 25200
    assert eng.short_pe.strike == 24850 and eng.long_pe.strike == 24800
    assert eng.reference_price == 25000
    # Net credit = (short_ce - long_ce) + (short_pe - long_pe) = (22-8) + (21-8) = 27/lot.
    assert eng.cycle_expected_max_profit == ((22.0 - 8.0) + (21.0 - 8.0)) * 75
    assert eng.is_flat() is False


def test_try_enter_fails_when_no_strike_clears_short_threshold():
    eng = IronFlyEngine(qty=75)
    table = {(25000, "CE"): 15.0, (25000, "PE"): 15.0}  # already below Rs20 at ATM
    assert eng.try_enter(25000, _premium_fn(table)) is False
    assert eng.is_flat() is True


# ── ±100 roll ────────────────────────────────────────────────────────────────

def test_fall_100_rolls_call_side_only():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    eng.try_enter(25000, _premium_fn(table))
    old_short_pe, old_long_pe = eng.short_pe, eng.long_pe

    # NIFTY falls to 24900 (reference - 100). New ATM=24900 call search table:
    table.update({
        (24900, "CE"): 130.0, (24950, "CE"): 70.0, (25000, "CE"): 30.0, (25050, "CE"): 16.0, (25100, "CE"): 8.0,
    })
    eng.on_spot_tick(24900, _premium_fn(table))

    assert eng.short_ce.strike == 25000 and eng.long_ce.strike == 25050  # rolled
    assert eng.short_pe is old_short_pe and eng.long_pe is old_long_pe    # put side untouched
    assert eng.reference_price == 24900


def test_rise_100_rolls_put_side_only():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    eng.try_enter(25000, _premium_fn(table))
    old_short_ce, old_long_ce = eng.short_ce, eng.long_ce

    table.update({
        (25100, "PE"): 125.0, (25050, "PE"): 68.0, (25000, "PE"): 29.0, (24950, "PE"): 16.0, (24900, "PE"): 8.0,
    })
    eng.on_spot_tick(25100, _premium_fn(table))

    assert eng.short_pe.strike == 25000 and eng.long_pe.strike == 24950
    assert eng.short_ce is old_short_ce and eng.long_ce is old_long_ce
    assert eng.reference_price == 25100


# ── gap handling ─────────────────────────────────────────────────────────────

def test_gap_down_arms_pending_then_fires_on_retrace_to_original_trigger():
    # Put side deliberately placed FAR from this excursion (23800, vs. the
    # 24700-24900 range being tested) so it can never satisfy the gap-
    # THROUGH-a-sold-strike condition (which would otherwise correctly take
    # priority per the 2026-09-14 spec, tested separately) -- this test
    # targets the ORDINARY gap-pending mechanic (doc Section 6) in isolation.
    eng = IronFlyEngine(qty=75)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25150, entry_price=22.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25200, entry_price=8.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=23800, entry_price=21.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=23750, entry_price=8.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 25000
    eng.cycle_expected_max_profit = 4875.0

    table = _entry_table()

    # Gaps straight to 24700 (skips the ordinary 24900 trigger by a full increment).
    eng.on_spot_tick(24700, _premium_fn(table))
    assert eng.pending_adjustment is not None
    assert eng.pending_adjustment.side == "CALL"
    assert eng.pending_adjustment.trigger_price == 24900
    assert eng.short_ce.strike == 25150  # NOT rolled yet

    # Still below trigger -- must not fire.
    eng.on_spot_tick(24800, _premium_fn(table))
    assert eng.pending_adjustment is not None
    assert eng.short_ce.strike == 25150

    # Retraces up to the original trigger (24900) -- fires now.
    table.update({
        (24900, "CE"): 130.0, (24950, "CE"): 70.0, (25000, "CE"): 30.0, (25050, "CE"): 16.0, (25100, "CE"): 8.0,
    })
    eng.on_spot_tick(24900, _premium_fn(table))
    assert eng.pending_adjustment is None
    assert eng.short_ce.strike == 25000 and eng.long_ce.strike == 25050
    assert eng.reference_price == 24900


# ── gap-through a sold strike (direct user spec, 2026-09-14) ────────────────

def test_gap_through_sold_call_converts_immediately_at_the_original_strike():
    # Doc's own worked example: sold call=25,000, NIFTY opens at 25,080 --
    # skips straight past 25,000 without a tick ever landing on it. Must
    # convert IMMEDIATELY (no waiting for a retrace, no waiting for another
    # +/-100 move) and the fly's short/long CE legs must anchor on the
    # ORIGINAL sold strike (25,000), NOT the literal rounded ATM (25,100) --
    # direct user confirmation, "same 25000 will be sold as iron fly".
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25000, entry_price=30.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25050, entry_price=10.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=24900, entry_price=26.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24850, entry_price=14.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 24950  # so the ordinary +100 PUT-roll trigger (25050) would
                                  # ALSO fire on this same tick -- proves gap-through wins
    eng.cycle_expected_max_profit = 1.0

    table = {(25000, "PE"): 65.0, (24950, "PE"): 20.0}
    eng.on_spot_tick(25080, _premium_fn(table))

    assert eng.is_flied is True
    assert eng.short_pe.strike == 25000 and eng.short_pe.entry_price == 65.0   # new fly PE, at the ORIGINAL sold CE strike
    assert eng.long_pe.strike == 24950 and eng.long_pe.entry_price == 20.0    # OTM1 below
    assert eng.short_ce.strike == 25000 and eng.short_ce.entry_price == 30.0  # kept exactly as-is


def test_gap_through_sold_put_converts_immediately_at_the_original_strike():
    # Mirror of the above: doc's gap-down example, sold put=24,900, NIFTY
    # opens at 24,800.
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25000, entry_price=30.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25050, entry_price=10.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=24900, entry_price=26.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24850, entry_price=14.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 24950
    eng.cycle_expected_max_profit = 1.0

    table = {(24900, "CE"): 65.0, (24950, "CE"): 20.0}
    eng.on_spot_tick(24800, _premium_fn(table))

    assert eng.is_flied is True
    assert eng.short_ce.strike == 24900 and eng.short_ce.entry_price == 65.0
    assert eng.long_ce.strike == 24950 and eng.long_ce.entry_price == 20.0
    assert eng.short_pe.strike == 24900 and eng.short_pe.entry_price == 26.0  # kept


def test_gap_through_detection_blocks_routine_adjustment_even_if_conversion_cant_execute_yet():
    # Direct user spec: gap-through detection must outrank routine
    # adjustments even when the conversion itself can't complete THIS tick
    # (missing premium data for the new leg) -- it must not fall through to
    # an ordinary roll/gap-pending-arm on the same tick; it should simply
    # retry the conversion next tick.
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25000, entry_price=30.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25050, entry_price=10.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=24900, entry_price=26.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24850, entry_price=14.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 24950  # ordinary +100 PUT-roll trigger (25050) also satisfied
    eng.cycle_expected_max_profit = 1.0

    table = {}  # no premium data at all for the new PE legs -- conversion can't complete
    eng.on_spot_tick(25080, _premium_fn(table))

    assert eng.is_flied is False              # conversion didn't complete
    assert eng.short_pe.strike == 24900        # NOT rolled by the ordinary +100 rule
    assert eng.pending_adjustment is None      # NOT armed as an ordinary gap-pending either


# ── Iron Fly conversion ──────────────────────────────────────────────────────

def test_short_pe_becomes_atm_converts_call_side_and_keeps_otm1_long_pe():
    # Doc Point 8's own worked example: short PE 24,500 becomes ATM while the
    # existing long PE (24,450) is already the correct OTM1 strike -- kept,
    # no replace. Position set up directly since the conversion mechanic
    # itself, not how the position got there, is what's under test.
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_pe = Leg(strike=24500, entry_price=90.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24450, entry_price=40.0, qty=75, is_short=False, side="PE")
    eng.short_ce = None
    eng.long_ce = None
    eng.reference_price = 24500
    eng.cycle_expected_max_profit = 1.0  # nonzero, irrelevant to this test

    table = {(24500, "CE"): 55.0, (24550, "CE"): 12.0}
    eng.on_spot_tick(24500, _premium_fn(table))

    assert eng.is_flied is True
    assert eng.short_ce.strike == 24500 and eng.long_ce.strike == 24550
    assert eng.short_pe.strike == 24500 and eng.short_pe.entry_price == 90.0  # untouched, kept at a loss
    assert eng.long_pe.strike == 24450  # already OTM1 -- kept, no replace


def test_existing_protective_leg_farther_than_otm1_gets_replaced_on_conversion():
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_pe = Leg(strike=24500, entry_price=90.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24400, entry_price=20.0, qty=75, is_short=False, side="PE")  # 100 away, not OTM1
    eng.short_ce = None
    eng.long_ce = None
    eng.reference_price = 24500
    eng.cycle_expected_max_profit = 1.0  # nonzero, irrelevant to this test

    table = {
        (24500, "CE"): 55.0, (24550, "CE"): 12.0,
        (24450, "PE"): 40.0, (24400, "PE"): 20.0,
    }
    eng.on_spot_tick(24500, _premium_fn(table))

    assert eng.is_flied is True
    assert eng.long_pe.strike == 24450  # replaced to the correct OTM1


def test_put_becomes_atm_also_locks_the_put_side_from_further_rolls():
    # Real bug found via the first live backtest run (2026-09-13): after
    # PUT_BECOMES_ATM, only the newly-built CALL side was being locked
    # (is_call_flied) -- the ORIGINAL short PE that triggered the
    # conversion (which the doc says to "keep" / "do NOT exit") stayed
    # eligible for an ordinary +/-100 roll on a later tick, which is wrong:
    # once ANY conversion fires, the whole position is a fixed Iron Fly for
    # the rest of the cycle (per the approved plan), not just one side.
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_pe = Leg(strike=24500, entry_price=90.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24450, entry_price=40.0, qty=75, is_short=False, side="PE")
    eng.short_ce = None
    eng.long_ce = None
    eng.reference_price = 24500
    eng.cycle_expected_max_profit = 1.0

    table = {(24500, "CE"): 55.0, (24550, "CE"): 12.0}
    eng.on_spot_tick(24500, _premium_fn(table))
    assert eng.is_flied is True

    # NIFTY rises back past reference+100 -- would ordinarily roll the PUT
    # side, but the whole position is fly-locked now.
    table.update({(24600, "PE"): 5.0, (24650, "PE"): 2.0})
    eng.on_spot_tick(24600, _premium_fn(table))
    assert eng.short_pe.strike == 24500  # NOT rolled


def test_once_flied_side_no_longer_rolls_on_further_moves():
    eng = IronFlyEngine(qty=75, otm1=50)
    from strategies.iron_fly.detector import Leg
    eng.short_pe = Leg(strike=24500, entry_price=90.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24450, entry_price=40.0, qty=75, is_short=False, side="PE")
    eng.short_ce = Leg(strike=24500, entry_price=55.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=24550, entry_price=12.0, qty=75, is_short=False, side="CE")
    eng.reference_price = 24500
    eng.is_flied = True
    eng.cycle_expected_max_profit = 1.0

    # NIFTY keeps falling past reference-100 -- would ordinarily roll the call
    # side, but that side is permanently fly-locked now.
    table = {(24500, "CE"): 200.0, (24450, "CE"): 240.0}
    eng.on_spot_tick(24400, _premium_fn(table))

    assert eng.short_ce.strike == 24500  # NOT rolled -- fly conversion is permanent


# ── order_count (for brokerage/charge reporting in the backtest script) ──────

def test_order_count_tracks_every_leg_opened_and_closed():
    # Direct user request (2026-09-14): every buy or sell incurs a flat
    # brokerage+charges fee (Rs60/order in their account) -- the backtest
    # report needs an accurate order count, not a manual guess, to compute
    # total costs. order_count is a lifetime counter across cycles (real
    # brokerage doesn't reset at a cycle boundary), incremented once per
    # leg opened AND once per leg closed.
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    eng.try_enter(25000, _premium_fn(table))
    assert eng.order_count == 4  # 4 legs opened, nothing closed yet

    table.update({
        (24900, "CE"): 130.0, (24950, "CE"): 70.0, (25000, "CE"): 30.0, (25050, "CE"): 16.0, (25100, "CE"): 8.0,
    })
    eng.on_spot_tick(24900, _premium_fn(table))  # rolls the call side
    assert eng.order_count == 8  # +2 closed, +2 opened


def test_lifetime_realized_pnl_survives_a_cycle_reset_unlike_cycle_realized_pnl():
    # cycle_realized_pnl deliberately resets to 0 on a fresh try_enter (the
    # 65%-target math needs that). lifetime_realized_pnl must NOT reset --
    # it's the running total the backtest script uses for a cost-adjusted
    # cumulative P&L ledger across cycle boundaries.
    eng = IronFlyEngine(qty=75, profit_target_pct=0.65)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25100, entry_price=28.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25150, entry_price=15.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=24900, entry_price=26.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24850, entry_price=14.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 25000
    eng.cycle_expected_max_profit = ((28.0 - 15.0) + (26.0 - 14.0)) * 75
    eng.cycle_realized_pnl = 0.0
    eng.order_count = 4  # as if these 4 legs were freshly entered

    # Base table supplies enough depth for the forced re-entry to succeed
    # afterward; override the CLOSING legs' own strikes with decayed
    # premiums LAST so they aren't clobbered back by the base values.
    table = _entry_table()
    table.update({(25100, "CE"): 5.0, (25150, "CE"): 2.0, (24900, "PE"): 5.0, (24850, "PE"): 2.0})
    eng.on_spot_tick(25000, _premium_fn(table))  # profit target hits -> close all + re-enter

    assert eng.cycle_realized_pnl == 0.0          # reset for the new cycle
    assert eng.lifetime_realized_pnl != 0.0        # NOT reset -- carries the closed cycle's P&L forward


# ── profit target ────────────────────────────────────────────────────────────

def test_profit_target_hit_closes_all_and_reenters_fresh_cycle():
    eng = IronFlyEngine(qty=75, profit_target_pct=0.65)
    from strategies.iron_fly.detector import Leg
    eng.short_ce = Leg(strike=25100, entry_price=28.0, qty=75, is_short=True, side="CE")
    eng.long_ce = Leg(strike=25150, entry_price=15.0, qty=75, is_short=False, side="CE")
    eng.short_pe = Leg(strike=24900, entry_price=26.0, qty=75, is_short=True, side="PE")
    eng.long_pe = Leg(strike=24850, entry_price=14.0, qty=75, is_short=False, side="PE")
    eng.reference_price = 25000
    eng.cycle_expected_max_profit = ((28.0 - 15.0) + (26.0 - 14.0)) * 75  # 1875
    eng.cycle_realized_pnl = 0.0
    old_cycle_number = eng.cycle_number

    # Base table supplies enough depth for a fresh re-entry search to succeed
    # afterward; then override the CLOSING legs' own strikes with decayed
    # premiums so mark-to-market P&L clears 65% of 1875 (~1219).
    table = _entry_table()
    table.update({
        (25100, "CE"): 5.0, (25150, "CE"): 2.0, (24900, "PE"): 5.0, (24850, "PE"): 2.0,
    })
    eng.on_spot_tick(25000, _premium_fn(table))

    assert eng.cycle_number == old_cycle_number + 1  # closed + re-entered
    assert eng.is_flat() is False  # a fresh 4-leg position was found and opened
    assert eng.cycle_realized_pnl == 0.0  # reset for the new cycle
    assert eng.is_flied is False


# ── diff_legs_for_orders (feeds the live async wrapper's order translation) ─

def test_diff_legs_for_orders_on_fresh_entry_reports_four_opens_no_closes():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    prev = snapshot_legs(eng)  # all 4 slots None before entry
    old_len = len(eng.trade_log)
    eng.try_enter(25000, _premium_fn(table))
    closed, opened = diff_legs_for_orders(prev, eng, eng.trade_log[old_len:])

    assert closed == []
    assert {o["slot"] for o in opened} == {"short_ce", "long_ce", "short_pe", "long_pe"}
    short_ce_entry = next(o for o in opened if o["slot"] == "short_ce")
    assert short_ce_entry["side"] == "CE"
    assert short_ce_entry["leg"].strike == 25150
    assert short_ce_entry["leg"].entry_price == 22.0


def test_diff_legs_for_orders_on_call_roll_reports_only_ce_slots():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    eng.try_enter(25000, _premium_fn(table))

    table.update({
        (25150, "CE"): 5.0,   # the OLD short_ce's premium has since decayed -- proves close_price
                              # comes from the real CLOSE log line, not a fallback to entry_price (22.0)
        (24900, "CE"): 130.0, (24950, "CE"): 70.0, (25000, "CE"): 30.0, (25050, "CE"): 16.0, (25100, "CE"): 8.0,
    })
    prev = snapshot_legs(eng)
    old_len = len(eng.trade_log)
    eng.on_spot_tick(24900, _premium_fn(table))
    closed, opened = diff_legs_for_orders(prev, eng, eng.trade_log[old_len:])

    assert {c["slot"] for c in closed} == {"short_ce", "long_ce"}
    assert {o["slot"] for o in opened} == {"short_ce", "long_ce"}
    old_short = next(c for c in closed if c["slot"] == "short_ce")
    assert old_short["leg"].strike == 25150
    assert old_short["close_price"] == 5.0  # the real CLOSE line's price, not entry_price (22.0)
    new_short = next(o for o in opened if o["slot"] == "short_ce")
    assert new_short["leg"].strike == 25000


def test_diff_legs_for_orders_no_change_reports_nothing():
    eng = IronFlyEngine(qty=75)
    table = _entry_table()
    eng.try_enter(25000, _premium_fn(table))
    prev = snapshot_legs(eng)
    old_len = len(eng.trade_log)
    eng.on_spot_tick(25010, _premium_fn(table))  # inside the band, nothing should happen
    closed, opened = diff_legs_for_orders(prev, eng, eng.trade_log[old_len:])
    assert closed == [] and opened == []
