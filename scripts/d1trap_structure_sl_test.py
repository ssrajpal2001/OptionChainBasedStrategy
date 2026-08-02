"""
scripts/d1trap_structure_sl_test.py -- A/B backtest: old tick-level SL vs the
new structure-gated SL (bear_only_book.py, 2026-08-02) over the SAME
validated month-rolling NIFTY dataset used by
scripts/d1trap_month_rolling_backtest.py.

Old behaviour: any tick where ltp <= pos["sl"] closes the leg immediately.
New behaviour: pos["sl"] (the soft/zone SL, before TSL activates) only
closes the leg once the OWNING zone's `invalid` flag is True (a 15m candle
already closed below zone_lo -- the same structural-failure signal the zone
engine already computes for un-triggered zones). The hard Rs2000/lot risk
cap and the profit-locking TSL are unchanged in both variants.

Reuses scripts/d1trap_month_rolling_backtest.py's data loading, zone engine,
and day-loop wiring directly (imported as a module) -- only check_exit /
open_leg are swapped for the structure-gated variant.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta

import scripts.d1trap_month_rolling_backtest as mrb
import strategies.d1_trap_option.bear_only_book as bb

LOT_NIFTY, OFFSET_NIFTY, STEP_NIFTY = 65, 200, 100


def zone_for(state, zone_lock_ts):
    return next((z for z in state.zones if z["ref_ts"] == zone_lock_ts), None)


def open_leg_hardsl(state, lot_size, tranche, entry_price, sl, zone_lock_ts, entry_ts=None):
    """Same as mrb.open_leg but also stores hard_sl (the unconditional Rs2000/lot
    floor) separately from the soft/zone sl, mirroring the live 2026-08-02 fix."""
    sl_buffered = sl - mrb.SL_BUFFER_PTS
    hard_sl = entry_price - mrb.MAX_RISK_RS_PER_LOT / lot_size
    sl_final = max(sl_buffered, hard_sl)
    b, bl, s, sl_ = bb._TSL_TRANCHE_BASE_PCT, bb._TSL_TRANCHE_BASE_LOCK_PCT, \
        bb._TSL_TRANCHE_STEP_PCT, bb._TSL_TRANCHE_STEP_LOCK_PCT
    state.positions.append(dict(
        side=state.side, strike=state.strike, entry_price=entry_price,
        sl=sl_final, hard_sl=hard_sl, high_lock_pct=0.0, tranche=tranche,
        zone_lock_ts=zone_lock_ts, tsl_base_pct=b, tsl_base_lock_pct=bl,
        tsl_step_pct=s, tsl_step_lock_pct=sl_, entry_ts=entry_ts,
    ))


def check_exit_structure_gated(state, lot_size, ltp, ts, force=False, force_reason="day_switch"):
    now_t = ts.time()
    for pos in list(state.positions):
        entry = pos["entry_price"]
        if force:
            mrb._close(state, lot_size, pos, force_reason, ltp, ts)
            continue
        profit_pct = (ltp - entry) / entry
        if profit_pct >= pos["tsl_base_pct"]:
            steps = int((profit_pct - pos["tsl_base_pct"]) // pos["tsl_step_pct"])
            calc_lock = pos["tsl_base_lock_pct"] + steps * pos["tsl_step_lock_pct"]
            pos["high_lock_pct"] = max(pos["high_lock_pct"], calc_lock)

        if pos["high_lock_pct"] > 0:
            tsl_price = entry * (1 + pos["high_lock_pct"])
            if ltp <= tsl_price:
                mrb._close(state, lot_size, pos, "tsl_hit", tsl_price, ts)
                continue
        else:
            hard_sl = pos.get("hard_sl", pos["sl"])
            if ltp <= hard_sl:
                mrb._close(state, lot_size, pos, "sl_hit_hard_cap", hard_sl, ts)
                continue
            if ltp <= pos["sl"]:
                zone = zone_for(state, pos["zone_lock_ts"])
                if zone is None or zone.get("invalid"):
                    reason = "sl_hit_structure" if zone is not None else "sl_hit"
                    mrb._close(state, lot_size, pos, reason, pos["sl"], ts)
                    continue
                # zone still structurally intact -- let it breathe.

        if now_t >= mrb.EOD_TIME:
            mrb._close(state, lot_size, pos, "eod", ltp, ts)


def run_variant(check_exit_fn, open_leg_fn, label):
    """Copy of mrb.run_month's NIFTY-only loop, parameterized on which
    check_exit/open_leg implementation to use."""
    mrb.open_leg = open_leg_fn      # process_zones_tick/check_fast_t1/process_flip_entry_t2
    mrb.check_exit = check_exit_fn  # all call through the module-level names
    trades = mrb.run_month("NIFTY", "nifty", "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet",
                            OFFSET_NIFTY, STEP_NIFTY, LOT_NIFTY)
    return mrb.summarize(label, trades), trades


def reversal_check(trades):
    """Among trades that exited via a soft SL (sl_hit / sl_hit_structure), how
    many would a wider look have shown reversing back in the trade's favor
    shortly after -- approximated here as: reason startswith sl_hit AND pnl<0
    AND (for structure-gated) never happens on a still-intact zone by
    construction, so this just reports raw counts per reason for comparison."""
    from collections import Counter
    return Counter(t["reason"] for t in trades)


if __name__ == "__main__":
    print("=" * 100)
    print("VARIANT A: OLD tick-level SL (baseline)")
    print("=" * 100)
    stats_old, trades_old = run_variant(mrb.check_exit, mrb.open_leg, "OLD tick-level SL")
    print("  exit reasons:", dict(reversal_check(trades_old)))

    print("\n" + "=" * 100)
    print("VARIANT B: NEW structure-gated SL")
    print("=" * 100)
    stats_new, trades_new = run_variant(check_exit_structure_gated, open_leg_hardsl, "NEW structure-gated SL")
    print("  exit reasons:", dict(reversal_check(trades_new)))

    print("\n" + "=" * 100)
    print("COMPARISON")
    print("=" * 100)
    print(f"{'Variant':<28}{'Trades':>8}{'Win%':>8}{'PF':>8}{'NetPnL':>12}")
    print(f"{'OLD tick-level SL':<28}{stats_old['n']:>8}{stats_old['win_pct']:>7.1f}%{stats_old['pf']:>8.2f}{stats_old['total']:>+12,.0f}")
    print(f"{'NEW structure-gated SL':<28}{stats_new['n']:>8}{stats_new['win_pct']:>7.1f}%{stats_new['pf']:>8.2f}{stats_new['total']:>+12,.0f}")
