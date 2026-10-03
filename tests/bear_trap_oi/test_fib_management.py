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


def test_fib_extension_levels_computes_1272_and_1618():
    levels = fib_extension_levels(base_price=100.0, top_price=150.0)
    assert levels["1.272"] == 100.0 + (150.0 - 100.0) * 1.272
    assert levels["1.618"] == 100.0 + (150.0 - 100.0) * 1.618


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
    # 1.5R fallback off (entry - zone_lo), lot2 the fixed 2R fallback.
    entry_price, zone_lo = 100.0, 95.0  # risk = 5
    bars = [
        _bar(0, 100, 101, 99, 100),
        _bar(5, 100, 100.5, 99, 100),
        _bar(315, 100, 100.8, 99, 99.5),  # EOD, target1 (107.5) never reached
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["mode"] == "fallback_fixed_R"
    assert result["target1"] == entry_price + 1.5 * (entry_price - zone_lo)  # 107.5
    assert result["target2"] == entry_price + 2.0 * (entry_price - zone_lo)  # 110.0
    assert result["lot1_exit_reason"] == "eod_target1_not_reached"
    assert result["lot1_exit_price"] == bars[-1].close


def test_manage_fib_trade_switches_to_dynamic_peak_mode_once_impulse_qualifies():
    # entry=100, zone_lo=90 -> risk=10. First bar's high (103) clears the
    # 2% impulse gate ((103-100)/100=3%), so from the FOLLOWING bar onward
    # target1 is the zone_lo->peak Fib 1.272 extension, not the fallback.
    entry_price, zone_lo = 100.0, 90.0
    bars = [
        _bar(0, 100, 103, 99, 102),     # peak becomes 103 after this bar (still fallback THIS bar)
        _bar(5, 102, 108, 101, 106),    # target1 now = 90+(103-90)*1.272=106.536; high=108 clears it
        _bar(10, 106, 115, 95, 110),    # variant A: low=95<=entry(100) -> TSL; variant B: checked below
        _bar(315, 110, 112, 108, 111),  # EOD
    ]
    result = manage_fib_trade(bars, entry_price=entry_price, zone_lo=zone_lo,
                               lot_qty_each=75)
    assert result["mode"] == "fib_dynamic_peak"
    target1 = zone_lo + (103.0 - zone_lo) * 1.272
    assert result["target1"] == target1
    assert result["lot1_exit_reason"] == "target1_hit"
    assert result["lot1_exit_price"] == target1
    assert result["lot1_exit_ts"] == bars[1].ts
    assert result["variant_a"]["lot2_exit_reason"] == "tsl_breakeven"
    assert result["variant_a"]["lot2_exit_price"] == entry_price
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
