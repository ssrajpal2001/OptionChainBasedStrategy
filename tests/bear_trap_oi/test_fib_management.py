from datetime import datetime, timedelta, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import (
    fib_extension_levels, compute_itm_strike, compute_roll_schedule,
    manage_fib_trade, run_dynamic_side_backtest, compute_running_excursions,
    format_trade_audit,
)


def _bar(minute, o, h, l, c):
    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    return Bar(ts=base + timedelta(minutes=minute), open=o, high=h, low=l, close=c)


def test_fib_extension_levels_computes_widened_1618_and_2618_by_default():
    target1, target2 = fib_extension_levels(base_price=100.0, top_price=150.0)
    assert target1 == 100.0 + (150.0 - 100.0) * 1.618
    assert target2 == 100.0 + (150.0 - 100.0) * 2.618


def test_fib_extension_levels_accepts_custom_ratios():
    target1, target2 = fib_extension_levels(base_price=100.0, top_price=150.0,
                                             ratio1=1.272, ratio2=1.618)
    assert target1 == 100.0 + (150.0 - 100.0) * 1.272
    assert target2 == 100.0 + (150.0 - 100.0) * 1.618


def test_compute_itm_strike_ce_is_below_spot_pe_is_above_spot():
    assert compute_itm_strike(spot=24530.0, points=100, step=50, side="CE") == 24450
    assert compute_itm_strike(spot=24530.0, points=100, step=50, side="PE") == 24650


def test_compute_roll_schedule_emits_event_on_100pt_move_and_resets_anchor():
    spot_bars = [
        _bar(0, 24500, 24510, 24490, 24500),
        _bar(5, 24500, 24520, 24495, 24510),
        _bar(10, 24510, 24620, 24505, 24610),
        _bar(15, 24610, 24625, 24600, 24615),
    ]
    schedule = compute_roll_schedule(spot_bars, initial_spot=24500.0, step=50,
                                      roll_points=100)
    assert len(schedule) == 1
    ts, ce_strike, pe_strike = schedule[0]
    assert ts == spot_bars[2].ts
    assert ce_strike == compute_itm_strike(24610.0, 100, 50, "CE")
    assert pe_strike == compute_itm_strike(24610.0, 100, 50, "PE")


def test_manage_fib_trade_uses_fallback_fixed_r_when_impulse_too_small():
    # Peak never clears 2% above entry (max peak = 101, only +1%), so the
    # dynamic zone_lo->peak Fib span is never trusted; lot1 uses the fixed
    # 2R fallback off (entry - zone_lo), lot2 the fixed 3R fallback
    # (widened 2026-10-03, direct user fix -- was 1.5R/2.0R).
    entry_price, zone_lo = 100.0, 95.0  # risk = 5
    bars = [
        _bar(0, 100, 101, 99, 100),
        _bar(5, 100, 100.5, 99, 100),
        _bar(315, 100, 100.8, 99, 99.5),  # EOD, target1 (110) never reached
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["mode"] == "fallback_fixed_R"
    assert result["target1"] == entry_price + 2.0 * (entry_price - zone_lo)  # 110.0
    assert result["target2"] == entry_price + 3.0 * (entry_price - zone_lo)  # 115.0
    assert result["lot1_exit_reason"] == "eod_target1_not_reached"
    assert result["lot1_exit_price"] == bars[-1].close


def test_manage_fib_trade_floors_risk_to_1pct_for_a_razor_thin_zone():
    # Real incident: CE 22600 (2026-10-01) had zone_lo=113.25, entry=113.3
    # -- a 0.05-point trap zone -- producing a near-useless 113.375 target.
    # Floored risk = max(113.3-113.25, 113.3*0.01) = max(0.05, 1.133) = 1.133.
    entry_price, zone_lo = 113.3, 113.25
    floored_risk = max(entry_price - zone_lo, entry_price * 0.01)
    expected_target1 = entry_price + 2.0 * floored_risk
    expected_target2 = entry_price + 3.0 * floored_risk
    bars = [
        _bar(0, entry_price, entry_price + 0.5, entry_price - 0.5, entry_price),
        _bar(5, entry_price, expected_target1 + 1, entry_price - 1, expected_target1),
        _bar(10, expected_target1, expected_target2 + 1, expected_target1 - 1, expected_target2),
        _bar(315, expected_target2, expected_target2 + 0.5, expected_target2 - 0.5, expected_target2),
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["target1"] == expected_target1
    assert result["target2"] == expected_target2
    # the floored target must be well clear of the razor-thin raw-zone target
    # (113.375) that caused the original real-money-relevant complaint
    assert result["target1"] > 113.375 + 1.0


def test_manage_fib_trade_switches_to_dynamic_peak_mode_once_impulse_qualifies():
    # entry=100, zone_lo=90 -> risk=10. First bar's high (103) clears the
    # 2% impulse gate ((103-100)/100=3%), so from the FOLLOWING bar onward
    # target1 is the zone_lo->peak Fib 1.618 extension, not the fallback.
    entry_price, zone_lo = 100.0, 90.0
    bars = [
        _bar(0, 100, 103, 99, 102),     # peak becomes 103 after this bar (still fallback THIS bar)
        _bar(5, 102, 112, 101, 106),    # target1 now = 90+(103-90)*1.618=111.034; high=112 clears it
        _bar(10, 106, 115, 95, 110),    # EOD
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["mode"] == "fib_dynamic_peak"
    target1 = zone_lo + (103.0 - zone_lo) * 1.618
    assert result["target1"] == target1
    assert result["lot1_exit_reason"] == "target1_hit"
    assert result["lot1_exit_price"] == target1
    assert result["lot1_exit_ts"] == bars[1].ts


def test_manage_fib_trade_variant_a_3bar_low_tsl_survives_a_dip_flat_breakeven_would_not():
    # Real incident this reproduces: Variant A's OLD flat-breakeven stop
    # got kicked out by an ordinary dip minutes before a real rally
    # (CE 22550, 2026-09-30). Entry=100, zone_lo=90: bar0/bar1 both dip
    # well below entry (lows 85 and 90), so the rolling 3-bar low sits at
    # 85 -- a dip to 92 on the very next bar would have breached a flat
    # breakeven(100) stop, but does NOT breach the 3-bar-low(85) stop.
    entry_price, zone_lo = 100.0, 90.0
    bars = [
        _bar(0, 100, 103, 85, 102),     # peak->103 after check; low=85 (feeds the 3-bar window)
        _bar(5, 102, 112, 90, 108),     # target1=90+(103-90)*1.618=111.034 -> lot1 fires here
        _bar(10, 108, 109, 92, 95),     # a dip to 92: ABOVE the 3-bar-low(85) stop -> no exit
        _bar(15, 95, 130, 93, 128),     # rally continues: high=130 clears target2
    ]
    target1 = zone_lo + (103.0 - zone_lo) * 1.618
    target2 = zone_lo + (103.0 - zone_lo) * 2.618
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["lot1_exit_ts"] == bars[1].ts
    assert result["target2"] == target2
    # Variant A was NOT stopped out on the dip at bars[2] -- it rides the
    # rally to target2, same bar as Variant B.
    assert result["variant_a"]["lot2_exit_reason"] == "target2_hit"
    assert result["variant_a"]["lot2_exit_price"] == target2
    assert result["variant_a"]["lot2_exit_ts"] == bars[3].ts
    assert result["variant_b"]["lot2_exit_reason"] == "target2_hit"
    assert result["variant_b"]["lot2_exit_ts"] == bars[3].ts


def test_manage_fib_trade_variant_a_3bar_low_tsl_still_exits_on_a_genuine_breach():
    entry_price, zone_lo = 100.0, 90.0
    bars = [
        _bar(0, 100, 103, 98, 102),     # 3-bar window feeds low=98
        _bar(5, 102, 112, 99, 108),     # target1 hit here; window low so far min(98,99)
        _bar(10, 108, 109, 80, 95),     # genuine breach: 80 is below the rolling 3-bar low
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["variant_a"]["lot2_exit_reason"] == "tsl_3bar_low"
    # stop level = min low of the 3 bars ending at (and including) the
    # lot1-firing bar = min(98, 99) = 98 (only 2 bars exist that far back)
    assert result["variant_a"]["lot2_exit_price"] == 98.0
    assert result["variant_a"]["lot2_exit_ts"] == bars[2].ts


def test_run_dynamic_side_backtest_blocks_new_entries_after_1445():
    t_c1 = datetime(2026, 10, 1, 14, 30, tzinfo=timezone.utc)
    t_c2 = datetime(2026, 10, 1, 14, 35, tzinfo=timezone.utc)
    t_confirm = datetime(2026, 10, 1, 14, 40, tzinfo=timezone.utc)
    t_reentry = datetime(2026, 10, 1, 14, 46, tzinfo=timezone.utc)  # after 14:45 cutoff
    bars_100 = {
        t_c1: Bar(ts=t_c1, open=100, high=105, low=98, close=102),
        t_c2: Bar(ts=t_c2, open=97, high=99, low=90, close=94),
        t_confirm: Bar(ts=t_confirm, open=95, high=110, low=95, close=108),
        t_reentry: Bar(ts=t_reentry, open=108, high=112, low=96, close=97),
    }
    master_ts = [t_c1, t_c2, t_confirm, t_reentry]
    trade = run_dynamic_side_backtest({100: bars_100}, master_ts,
                                       initial_strike=100, side_roll_schedule=[],
                                       side="CE", lot_qty_each=75)
    assert trade is None  # armed correctly, but re-entry fired after the 14:45 cutoff -> blocked


def test_compute_running_excursions_tracks_mfe_mae_per_bar():
    entry_price = 100.0
    bars = [
        _bar(0, 100, 105, 98, 102),   # running MFE=105, MAE=98
        _bar(5, 102, 103, 90, 95),    # MFE stays 105, MAE drops to 90
        _bar(10, 95, 120, 94, 110),   # MFE jumps to 120, MAE stays 90
    ]
    rows = compute_running_excursions(bars, entry_price)
    assert len(rows) == 3
    assert rows[0]["mfe"] == 105.0 and rows[0]["mae"] == 98.0
    assert rows[1]["mfe"] == 105.0 and rows[1]["mae"] == 90.0
    assert rows[2]["mfe"] == 120.0 and rows[2]["mae"] == 90.0
    assert rows[2]["open"] == 95 and rows[2]["close"] == 110


def test_format_trade_audit_includes_entry_candle_table_and_fib_block():
    entry_price, zone_lo = 100.0, 90.0
    bars = [
        _bar(0, 100, 103, 99, 102),
        _bar(5, 102, 108, 101, 106),
        _bar(10, 106, 115, 95, 110),
        _bar(315, 110, 112, 108, 111),
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    trade = {"side": "CE", "entry_strike": 24500, "entry_price": entry_price,
              "entry_ts": _bar(-5, 0, 0, 0, 0).ts, **result}
    text = format_trade_audit(trade, bars)
    assert "CE" in text and "24500" in text
    assert "MFE" in text and "MAE" in text
    assert "Target1" in text or "target1" in text.lower()
    assert "Variant A" in text and "Variant B" in text
    # every bar's own OHLC values must appear somewhere in the rendered table
    for b in bars:
        assert str(b.high) in text


def test_run_dynamic_side_backtest_allows_entry_before_1445_cutoff():
    t0 = datetime(2026, 10, 1, 9, 15, tzinfo=timezone.utc)
    t1 = datetime(2026, 10, 1, 9, 20, tzinfo=timezone.utc)
    t2 = datetime(2026, 10, 1, 9, 25, tzinfo=timezone.utc)
    t3 = datetime(2026, 10, 1, 9, 30, tzinfo=timezone.utc)
    bars_100 = {
        t0: Bar(ts=t0, open=100, high=105, low=98, close=102),
        t1: Bar(ts=t1, open=97, high=99, low=90, close=94),
        t2: Bar(ts=t2, open=95, high=110, low=95, close=108),
        t3: Bar(ts=t3, open=108, high=112, low=96, close=97),
    }
    master_ts = [t0, t1, t2, t3]
    trade = run_dynamic_side_backtest({100: bars_100}, master_ts,
                                       initial_strike=100, side_roll_schedule=[],
                                       side="CE", lot_qty_each=75)
    assert trade is not None
    assert trade["entry_price"] == 97.0
    assert trade["post_entry_bars"] == []  # entry was on the last bar in master_ts
