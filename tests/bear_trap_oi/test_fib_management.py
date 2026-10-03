from datetime import datetime, timedelta, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import (
    detect_first_local_high, fib_extension_levels, compute_itm_strike,
    compute_roll_schedule, manage_fib_trade, run_dynamic_side_backtest,
)


def _bar(minute, o, h, l, c):
    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    return Bar(ts=base + timedelta(minutes=minute), open=o, high=h, low=l, close=c)


def test_detect_first_local_high_finds_first_pullback_after_impulse():
    bars = [
        _bar(0, 100, 105, 100, 104),
        _bar(5, 104, 115, 103, 112),   # impulse high 115
        _bar(10, 112, 110, 105, 108),  # pullback: high 110 < 115 -> 115 is the swing high
        _bar(15, 108, 130, 107, 125),  # later higher high -- irrelevant, first one already found
    ]
    result = detect_first_local_high(bars)
    assert result == (115.0, bars[1].ts)


def test_detect_first_local_high_returns_none_when_strictly_ascending():
    bars = [
        _bar(0, 100, 105, 100, 104),
        _bar(5, 104, 110, 103, 108),
        _bar(10, 108, 120, 107, 118),
    ]
    assert detect_first_local_high(bars) is None


def test_fib_extension_levels_computes_1272_and_1618():
    levels = fib_extension_levels(entry_price=100.0, swing_high=150.0)
    assert levels["1.272"] == 100.0 + (150.0 - 100.0) * 1.272
    assert levels["1.618"] == 100.0 + (150.0 - 100.0) * 1.618


def test_compute_itm_strike_ce_is_below_spot_pe_is_above_spot():
    assert compute_itm_strike(spot=24530.0, points=100, step=50, side="CE") == 24450
    assert compute_itm_strike(spot=24530.0, points=100, step=50, side="PE") == 24650


def test_compute_roll_schedule_emits_event_on_100pt_move_and_resets_anchor():
    spot_bars = [
        _bar(0, 24500, 24510, 24490, 24500),
        _bar(5, 24500, 24520, 24495, 24510),   # only +10 from anchor, no roll
        _bar(10, 24510, 24620, 24505, 24610),  # +110 from anchor -> roll here
        _bar(15, 24610, 24625, 24600, 24615),  # +5 from NEW anchor (24610), no roll
    ]
    schedule = compute_roll_schedule(spot_bars, initial_spot=24500.0, step=50,
                                      roll_points=100)
    assert len(schedule) == 1
    ts, ce_strike, pe_strike = schedule[0]
    assert ts == spot_bars[2].ts
    assert ce_strike == compute_itm_strike(24610.0, 100, 50, "CE")
    assert pe_strike == compute_itm_strike(24610.0, 100, 50, "PE")


def test_manage_fib_trade_variant_a_locks_breakeven_after_lot1_target():
    # swing high forms early (120 then pullback), fib 1.272 and 1.618 off [100,120]
    entry_price = 100.0
    bars = [
        _bar(0, 100, 120, 99, 118),    # impulse high 120
        _bar(5, 118, 119, 110, 112),   # pullback -> swing high = 120 @ bar0
        _bar(10, 112, 125.44, 111, 120),  # touches 1.272 = 100+20*1.272=125.44 -> lot1 exits here
        _bar(15, 120, 121, 99, 100),   # drops to 99 -> breaches breakeven TSL (100) -> lot2 exits @ 100
        _bar(315, 100, 105, 98, 102),  # EOD (irrelevant, lot2 already closed)
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, lot_qty_each=75)
    assert result["swing_high"] == 120.0
    fib_1272 = 100.0 + (120.0 - 100.0) * 1.272
    fib_1618 = 100.0 + (120.0 - 100.0) * 1.618
    assert result["fib_1272"] == fib_1272
    assert result["fib_1618"] == fib_1618
    assert result["lot1_exit_price"] == fib_1272
    assert result["lot1_exit_ts"] == bars[2].ts
    assert result["variant_a"]["lot2_exit_price"] == entry_price
    assert result["variant_a"]["lot2_exit_reason"] == "tsl_breakeven"
    assert result["variant_a"]["lot2_exit_ts"] == bars[3].ts


def _mk(hour, minute, o, h, l, c):
    return Bar(ts=datetime(2026, 10, 1, hour, minute, tzinfo=timezone.utc),
                open=o, high=h, low=l, close=c)


def test_run_dynamic_side_backtest_rolls_strike_while_flat_and_while_in_position():
    t0 = datetime(2026, 10, 1, 9, 15, tzinfo=timezone.utc)
    t1 = datetime(2026, 10, 1, 9, 20, tzinfo=timezone.utc)
    t2 = datetime(2026, 10, 1, 9, 25, tzinfo=timezone.utc)
    t3 = datetime(2026, 10, 1, 9, 30, tzinfo=timezone.utc)
    t4 = datetime(2026, 10, 1, 9, 35, tzinfo=timezone.utc)
    t5 = datetime(2026, 10, 1, 9, 40, tzinfo=timezone.utc)
    t6 = datetime(2026, 10, 1, 9, 45, tzinfo=timezone.utc)
    t7 = datetime(2026, 10, 1, 9, 50, tzinfo=timezone.utc)
    t8 = datetime(2026, 10, 1, 9, 55, tzinfo=timezone.utc)
    t9 = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 10, 1, 10, 5, tzinfo=timezone.utc)

    bars_by_strike = {
        100: {
            t0: Bar(ts=t0, open=50, high=55, low=48, close=52),
            t1: Bar(ts=t1, open=52, high=53, low=40, close=45),
        },
        200: {
            t2: Bar(ts=t2, open=100, high=105, low=98, close=102),   # new C1 after roll
            t3: Bar(ts=t3, open=102, high=104, low=90, close=95),    # C2 breakdown, zone[90,102]
            t4: Bar(ts=t4, open=95, high=110, low=94, close=108),    # trap confirmed
            t5: Bar(ts=t5, open=108, high=112, low=96, close=97),    # re-entry @97
        },
        300: {
            t6: Bar(ts=t6, open=97, high=140, low=96, close=130),    # post-entry impulse high=140
            t7: Bar(ts=t7, open=130, high=135, low=125, close=128),  # pullback -> swing_high=140
            t8: Bar(ts=t8, open=128, high=152, low=127, close=150),  # clears fib_1272 (151.696)
            t9: Bar(ts=t9, open=150, high=155, low=90, close=95),    # low<=entry(97) -> TSL (A)
            t10: Bar(ts=t10, open=95, high=170, low=94, close=165),  # clears fib_1618 (166.574) (B)
        },
    }
    master_ts = [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10]
    # Roll at t2 while FLAT (still detecting) -> switches to strike 200 and
    # resets the detector fresh. Roll at t6 while IN A POSITION -> keeps the
    # original entry_price/strike but switches post-entry bar SOURCE to 300.
    side_roll_schedule = [(t2, 200), (t6, 300)]

    trade = run_dynamic_side_backtest(bars_by_strike, master_ts,
                                       initial_strike=100,
                                       side_roll_schedule=side_roll_schedule,
                                       side="CE", lot_qty_each=75)

    assert trade is not None
    assert trade["entry_strike"] == 200       # entry happened on the ROLLED strike, not 100
    assert trade["entry_price"] == 97.0
    assert trade["entry_ts"] == t5
    assert trade["swing_high"] == 140.0        # detected on strike 300's post-entry bars
    fib_1272 = 97.0 + (140.0 - 97.0) * 1.272
    fib_1618 = 97.0 + (140.0 - 97.0) * 1.618
    assert trade["fib_1272"] == fib_1272
    assert trade["lot1_exit_price"] == fib_1272
    assert trade["lot1_exit_ts"] == t8
    assert trade["variant_a"]["lot2_exit_reason"] == "tsl_breakeven"
    assert trade["variant_a"]["lot2_exit_ts"] == t9
    assert trade["variant_b"]["lot2_exit_reason"] == "target_1618"
    assert trade["variant_b"]["lot2_exit_price"] == fib_1618
    assert trade["variant_b"]["lot2_exit_ts"] == t10


def test_manage_fib_trade_variant_b_lets_lot2_run_to_1618_with_no_tsl():
    entry_price = 100.0
    fib_1618 = 100.0 + (120.0 - 100.0) * 1.618  # 132.36
    bars = [
        _bar(0, 100, 120, 99, 118),
        _bar(5, 118, 119, 110, 112),
        _bar(10, 112, 125.44, 111, 120),   # lot1 @ 1.272 (125.44)
        _bar(15, 120, 90, 89, 95),          # variant B has NO tsl -- stays open despite the dip
        _bar(20, 95, 133.0, 94, 128),       # clears fib_1618 (132.36) -> lot2 target hit here
        _bar(315, 128, 130, 126, 129),
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, lot_qty_each=75)
    assert result["variant_b"]["lot2_exit_reason"] == "target_1618"
    assert result["variant_b"]["lot2_exit_price"] == fib_1618
    assert result["variant_b"]["lot2_exit_ts"] == bars[4].ts
    assert result["variant_b"]["lot2_pnl"] == (fib_1618 - entry_price) * 75
    assert result["variant_b"]["total_pnl"] == result["lot1_pnl"] + result["variant_b"]["lot2_pnl"]
    # Variant A, same bars: TSL breaches at bar3 (low=89 <= entry_price=100) before 1618 ever hits
    assert result["variant_a"]["lot2_exit_reason"] == "tsl_breakeven"
    assert result["variant_a"]["lot2_exit_price"] == entry_price
    assert result["variant_a"]["lot2_exit_ts"] == bars[3].ts
