from datetime import datetime, timedelta, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import (
    fib_extension_levels, compute_itm_strike, compute_roll_schedule,
    manage_fib_trade, run_dynamic_side_backtest, compute_running_excursions,
    format_trade_audit, compute_performance_metrics, apply_execution_costs,
    compute_oi_trend, sum_oi_band, check_oi_filter,
    VolBar, compute_volume_profile, classify_rollover,
    is_strike_oi_significant, check_hard_wall, check_directional_matrix,
    check_price_acceptance, svp_ready, compute_ema, check_momentum_acceptance,
    check_buyer_gate, compute_roc, check_velocity_gate, diagnose_trap_funnel,
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


def test_compute_performance_metrics_profit_factor_expectancy_max_drawdown():
    # Sequence chosen so cumulative P&L has a real peak-to-trough:
    # +100, +200 (cum 300, peak 300), -150 (cum 150, dd=150), +50 (cum 200).
    pnls = [100.0, 200.0, -150.0, 50.0]
    m = compute_performance_metrics(pnls)
    assert m["gross_win"] == 350.0
    assert m["gross_loss"] == 150.0
    assert m["profit_factor"] == 350.0 / 150.0
    assert m["expectancy"] == sum(pnls) / 4
    assert m["max_drawdown"] == 150.0
    assert m["total_pnl"] == 200.0
    assert m["win_count"] == 3 and m["loss_count"] == 1


def test_compute_performance_metrics_handles_no_losses():
    m = compute_performance_metrics([100.0, 50.0])
    assert m["gross_loss"] == 0.0
    assert m["profit_factor"] == float("inf")
    assert m["max_drawdown"] == 0.0  # cumulative never dips below its own peak


def test_apply_execution_costs_deducts_slippage_and_flat_fees():
    # entry=100, lot1 exits @110, lot2 exits @120, qty=75/lot.
    # Slippage 0.05% works AGAINST us on every fill (pay more to buy,
    # receive less to sell). Flat fee: 30/lot/leg, 2 legs per lot
    # (its own entry + its own exit) -- 4 lot-legs total across both lots.
    result = apply_execution_costs(entry_price=100.0, lot1_exit_price=110.0,
                                    lot2_exit_price=120.0, lot_qty_each=75,
                                    cost_per_lot_leg=30.0, slippage_pct=0.0005)
    eff_entry = 100.0 * 1.0005
    eff_lot1_exit = 110.0 * 0.9995
    eff_lot2_exit = 120.0 * 0.9995
    lot1_net = (eff_lot1_exit - eff_entry) * 75 - 2 * 30.0
    lot2_net = (eff_lot2_exit - eff_entry) * 75 - 2 * 30.0
    assert abs(result["lot1_net_pnl"] - lot1_net) < 1e-9
    assert abs(result["lot2_net_pnl"] - lot2_net) < 1e-9
    assert abs(result["total_net_pnl"] - (lot1_net + lot2_net)) < 1e-9
    assert result["total_net_pnl"] < (110.0 - 100.0 + 120.0 - 100.0) * 75  # strictly worse than gross


def test_compute_oi_trend_rising_falling_flat():
    assert compute_oi_trend(now=100, baseline=80) == "RISING"
    assert compute_oi_trend(now=80, baseline=100) == "FALLING"
    assert compute_oi_trend(now=100, baseline=100) == "FLAT"


def test_sum_oi_band_ce_is_atm_plus_4_strikes_below():
    oi_by_strike = {22400: 10, 22450: 20, 22500: 30, 22550: 40, 22600: 50,
                     22650: 999}  # above ATM, must NOT be included for CE
    total = sum_oi_band(oi_by_strike, atm_strike=22600, step=50, side="CE", depth=5)
    assert total == 10 + 20 + 30 + 40 + 50  # 22400,22450,22500,22550,22600


def test_sum_oi_band_pe_is_atm_plus_4_strikes_above():
    oi_by_strike = {22600: 50, 22650: 40, 22700: 30, 22750: 20, 22800: 10,
                     22550: 999}  # below ATM, must NOT be included for PE
    total = sum_oi_band(oi_by_strike, atm_strike=22600, step=50, side="PE", depth=5)
    assert total == 50 + 40 + 30 + 20 + 10


def test_check_oi_filter_passes_only_when_target_falling_and_opposing_rising():
    passing = check_oi_filter(target_oi_now=80, target_oi_base=100,
                               opposing_oi_now=120, opposing_oi_base=100)
    assert passing["passed"] is True
    assert passing["target_trend"] == "FALLING"
    assert passing["opposing_trend"] == "RISING"

    both_rising = check_oi_filter(target_oi_now=120, target_oi_base=100,
                                   opposing_oi_now=120, opposing_oi_base=100)
    assert both_rising["passed"] is False

    both_falling = check_oi_filter(target_oi_now=80, target_oi_base=100,
                                    opposing_oi_now=80, opposing_oi_base=100)
    assert both_falling["passed"] is False


def _volbar(minute, o, h, l, c, v):
    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    return VolBar(ts=base + timedelta(minutes=minute), open=o, high=h, low=l,
                  close=c, volume=v)


def test_compute_volume_profile_finds_poc_vah_val_and_lvns():
    # Price oscillates 100-104 in bin 102 heavily (POC), thin volume at
    # 106-108 (an LVN gap) before a second, smaller cluster at 110.
    bars = [
        _volbar(0, 100, 102, 99, 101, 500),
        _volbar(5, 101, 103, 100, 102, 900),   # heavy volume around 102 -> POC
        _volbar(10, 102, 104, 101, 103, 800),
        _volbar(15, 103, 107, 102, 106, 50),   # thin -- LVN zone
        _volbar(20, 106, 109, 105, 108, 40),   # thin -- LVN zone
        _volbar(25, 108, 111, 107, 110, 300),
    ]
    profile = compute_volume_profile(bars, price_bin_size=1.0, value_area_pct=0.70)
    assert 101 <= profile["poc"] <= 103
    assert profile["val"] <= profile["poc"] <= profile["vah"]
    assert any(106 <= lvn <= 108 for lvn in profile["lvns"])


def test_classify_rollover_detects_similar_magnitude_opposite_moves():
    # Current month OI falls ~20%, next month OI rises by a similar
    # absolute amount -> rollover, not a genuine directional signal.
    result = classify_rollover(curr_oi_now=800_000, curr_oi_base=1_000_000,
                                 next_oi_now=420_000, next_oi_base=200_000,
                                 similarity_tol=0.3)
    assert result is True


def test_classify_rollover_rejects_dissimilar_magnitude_moves():
    # Current month barely moves, next month surges -- not a rollover
    # pattern (no offsetting unwind on the current contract).
    result = classify_rollover(curr_oi_now=990_000, curr_oi_base=1_000_000,
                                 next_oi_now=500_000, next_oi_base=200_000,
                                 similarity_tol=0.3)
    assert result is False


def test_is_strike_oi_significant_below_15pct_of_peak_is_noise():
    assert is_strike_oi_significant(abs_oi_at_strike=10_000, peak_oi=100_000,
                                     min_pct=0.15) is False
    assert is_strike_oi_significant(abs_oi_at_strike=20_000, peak_oi=100_000,
                                     min_pct=0.15) is True


def test_check_hard_wall_blocks_minor_moves_near_a_wall_without_momentum_override():
    # Price 50pts from the wall, OI change only 10% -- blocked (no override).
    blocked = check_hard_wall(price=22550, wall_strike=22600, distance_pts=50,
                               oi_change_pct=0.10, override_pct=0.30)
    assert blocked is True
    # Same distance, but OI change of 35% clears the momentum override.
    allowed = check_hard_wall(price=22550, wall_strike=22600, distance_pts=50,
                               oi_change_pct=0.35, override_pct=0.30)
    assert allowed is False
    # Far from any wall -- never blocked regardless of OI change.
    far = check_hard_wall(price=22300, wall_strike=22600, distance_pts=50,
                           oi_change_pct=0.0, override_pct=0.30)
    assert far is False


def test_check_directional_matrix_pe_and_ce_configurations():
    # PE: Future RISING + Put FALLING + Call RISING -> high-conviction bearish
    assert check_directional_matrix("RISING", "FALLING", "RISING", side="PE") is True
    # PE: Future FALLING + Put FALLING + Call RISING -> also valid
    assert check_directional_matrix("FALLING", "FALLING", "RISING", side="PE") is True
    # PE: anything else fails
    assert check_directional_matrix("RISING", "RISING", "RISING", side="PE") is False
    # CE: Future RISING + Put RISING + Call FALLING -> high-conviction bullish
    assert check_directional_matrix("RISING", "RISING", "FALLING", side="CE") is True
    # CE: Future FALLING + Put RISING + Call FALLING -> also valid
    assert check_directional_matrix("FALLING", "RISING", "FALLING", side="CE") is True
    assert check_directional_matrix("RISING", "RISING", "RISING", side="CE") is False


def test_check_price_acceptance_requires_breakout_beyond_value_area_and_lvn_touch():
    # Bullish breakout: close above VAH, AND the bar's own range overlapped
    # an LVN zone (volume "accepted" through the thin zone).
    accepted = check_price_acceptance(close=109.0, val=101.0, vah=103.0,
                                       lvns=[106.5, 107.5], bar_low=105.5,
                                       bar_high=109.5, direction="bullish")
    assert accepted is True
    # Close beyond VAH but never touched any LVN zone -- not accepted.
    not_accepted = check_price_acceptance(close=109.0, val=101.0, vah=103.0,
                                            lvns=[106.5, 107.5], bar_low=108.8,
                                            bar_high=109.5, direction="bullish")
    assert not_accepted is False


def test_svp_ready_false_before_0945_true_at_or_after():
    before = datetime(2026, 10, 1, 9, 40, tzinfo=timezone.utc)
    at = datetime(2026, 10, 1, 9, 45, tzinfo=timezone.utc)
    after = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
    assert svp_ready(before) is False
    assert svp_ready(at) is True
    assert svp_ready(after) is True


def test_compute_ema_matches_standard_formula():
    values = [10, 11, 12, 13, 14, 15, 16, 17, 18, 20]
    ema = compute_ema(values, period=9)
    assert len(ema) == len(values)
    k = 2 / (9 + 1)
    expected_last = ema[-2] + k * (values[-1] - ema[-2])
    assert abs(ema[-1] - expected_last) < 1e-9


def test_check_momentum_acceptance_breakout_plus_volume_surge():
    accepted = check_momentum_acceptance(
        close=110.0, val=95.0, vah=105.0, direction="bullish",
        bar_volume=2000, avg_volume=1000, ema_prev=100.0, ema_curr=100.5,
        volume_surge_mult=1.5,
    )
    assert accepted is True  # breakout beyond VAH + volume surge, EMA irrelevant


def test_check_momentum_acceptance_breakout_plus_ema_slope_no_volume():
    accepted = check_momentum_acceptance(
        close=110.0, val=95.0, vah=105.0, direction="bullish",
        bar_volume=900, avg_volume=1000, ema_prev=100.0, ema_curr=102.0,
        volume_surge_mult=1.5,
    )
    assert accepted is True  # no volume surge, but EMA sloping up confirms


def test_check_momentum_acceptance_rejects_no_breakout():
    accepted = check_momentum_acceptance(
        close=102.0, val=95.0, vah=105.0, direction="bullish",
        bar_volume=5000, avg_volume=1000, ema_prev=100.0, ema_curr=102.0,
    )
    assert accepted is False  # close never cleared VAH -- no breakout at all


def test_check_momentum_acceptance_rejects_breakout_with_neither_confirmation():
    accepted = check_momentum_acceptance(
        close=110.0, val=95.0, vah=105.0, direction="bullish",
        bar_volume=900, avg_volume=1000, ema_prev=100.0, ema_curr=99.0,
        volume_surge_mult=1.5,
    )
    assert accepted is False  # breakout but no volume surge and EMA sloping DOWN


def test_check_buyer_gate_combines_directional_matrix_and_momentum():
    # Directional matrix passes (PE config) AND momentum breakout+volume -- gate open.
    gate_open = check_buyer_gate(
        fut_trend="RISING", put_trend="FALLING", call_trend="RISING", side="PE",
        close=90.0, val=95.0, vah=105.0, bar_volume=2000, avg_volume=1000,
        ema_prev=100.0, ema_curr=99.0,
    )
    assert gate_open is True
    # Directional matrix fails -- gate closed regardless of momentum.
    gate_closed = check_buyer_gate(
        fut_trend="RISING", put_trend="RISING", call_trend="RISING", side="PE",
        close=90.0, val=95.0, vah=105.0, bar_volume=2000, avg_volume=1000,
        ema_prev=100.0, ema_curr=99.0,
    )
    assert gate_closed is False


def test_compute_roc_over_lookback_bars():
    closes = [100.0, 100.5, 101.0, 102.0]  # entry bar is the LAST close
    roc = compute_roc(closes, lookback=3)
    assert roc == (102.0 - 100.0) / 100.0


def test_check_velocity_gate_requires_matrix_velocity_and_momentum():
    # CE: matrix passes (Fut RISING+Put RISING+Call FALLING), velocity
    # clears threshold upward, volume confirms -- gate open.
    open_gate = check_velocity_gate(
        fut_trend="RISING", put_trend="RISING", call_trend="FALLING", side="CE",
        roc_pct=0.006, roc_threshold=0.003, bar_volume=2000, avg_volume=1000,
        ema_prev=100.0, ema_curr=100.1,
    )
    assert open_gate is True


def test_check_velocity_gate_rejects_weak_velocity():
    closed = check_velocity_gate(
        fut_trend="RISING", put_trend="RISING", call_trend="FALLING", side="CE",
        roc_pct=0.001, roc_threshold=0.003, bar_volume=2000, avg_volume=1000,
        ema_prev=100.0, ema_curr=100.1,
    )
    assert closed is False


def test_check_velocity_gate_rejects_wrong_direction_velocity():
    # CE needs upward velocity -- a negative roc fails even if matrix passes.
    closed = check_velocity_gate(
        fut_trend="RISING", put_trend="RISING", call_trend="FALLING", side="CE",
        roc_pct=-0.006, roc_threshold=0.003, bar_volume=2000, avg_volume=1000,
        ema_prev=100.0, ema_curr=100.1,
    )
    assert closed is False


def test_diagnose_trap_funnel_reports_each_terminal_stage():
    entered = [
        _bar(0, 100, 105, 98, 102), _bar(5, 97, 99, 90, 94),
        _bar(10, 95, 110, 95, 108), _bar(15, 108, 112, 96, 97),
    ]
    assert diagnose_trap_funnel(entered) == "ENTERED"

    armed_no_reentry = [
        _bar(0, 100, 105, 98, 102), _bar(5, 97, 99, 90, 94),
        _bar(10, 95, 110, 95, 108), _bar(15, 108, 160, 150, 155),
    ]
    assert diagnose_trap_funnel(armed_no_reentry) == "ARMED_WAIT_REENTRY"

    trap_watch_no_confirm = [
        _bar(0, 100, 105, 98, 102), _bar(5, 97, 99, 90, 94),
        _bar(10, 94, 99, 92, 95),
    ]
    assert diagnose_trap_funnel(trap_watch_no_confirm) == "TRAP_WATCH"

    breakdown_watch_no_breakdown = [
        _bar(0, 100, 105, 98, 102), _bar(5, 102, 107, 101, 106),
    ]
    assert diagnose_trap_funnel(breakdown_watch_no_breakdown) == "BREAKDOWN_WATCH"


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
