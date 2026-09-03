"""
scripts/d1trap_nifty_finestep_pctcap_sweep.py -- same two changes validated
for SENSEX (fine-step TSL beyond the 20% trigger, % of capital hard SL cap
instead of a flat Rs/lot number), tested on NIFTY -- HTF (60m) and strike
selection (150pt offset, 3-ITM) UNCHANGED, per direct instruction. No BOS.
"""
import sys
sys.path.insert(0, ".")
from datetime import timedelta, date
import pandas as pd
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"
OFFSET, ROUND_STEP, LOT, HTF = 150, 100, 65, 60
BASE_PCT, BASE_LOCK = 0.20, 0.125


def make_open_leg(step_pct, step_lock, pct_cap):
    def patched(state, lot_size, tranche, entry_price, sl, zone_lock_ts, audit=None, entry_ts=None):
        sl_buffered = sl - mrb.SL_BUFFER_PTS
        floor = entry_price * (1 - pct_cap) if pct_cap is not None else entry_price - mrb.MAX_RISK_RS_PER_LOT / lot_size
        sl_final = max(sl_buffered, floor)
        state.positions.append(dict(side=state.side, strike=state.strike, entry_price=entry_price, sl=sl_final,
                                     high_lock_pct=0.0, tranche=tranche, zone_lock_ts=zone_lock_ts,
                                     tsl_base_pct=BASE_PCT, tsl_base_lock_pct=BASE_LOCK,
                                     tsl_step_pct=step_pct, tsl_step_lock_pct=step_lock,
                                     audit=audit or {}, entry_ts=entry_ts))
    return patched


def run(step_pct, step_lock, pct_cap):
    orig_open_leg = mrb.open_leg
    mrb.open_leg = make_open_leg(step_pct, step_lock, pct_cap)
    try:
        trades = verify.run_month_live("NIFTY", "niftyladder", SPOT_PATH, OFFSET, ROUND_STEP, LOT, HTF)
    finally:
        mrb.open_leg = orig_open_leg
    return trades


if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    print("Baseline (current live: 20%/12.5% next-tier-at-40%, flat Rs2000 cap)")
    baseline = run(BASE_PCT, BASE_LOCK, None)  # step==base => no further ratchet beyond first tier (matches live tranche shape)
    mrb.summarize("  baseline", baseline)

    print("\nStep-pct sweep (flat Rs2000 cap held constant):")
    for step in (0.05, 0.075, 0.10):
        trades = run(step, step, None)
        mrb.summarize(f"  step={step*100:.1f}%/{step*100:.1f}%, flat cap", trades)

    print("\n% of capital cap sweep (step held at 7.5%/7.5%, the SENSEX-winning step):")
    for pct_cap in (0.06, 0.08, 0.10, 0.12):
        trades = run(0.075, 0.075, pct_cap)
        sl_hits = [t for t in trades if t["reason"] == "sl_hit"]
        worst = min((t["pnl"] for t in sl_hits), default=0)
        mrb.summarize(f"  step=7.5%/7.5%, cap={pct_cap*100:.0f}% of capital", trades)
        print(f"    worst SL-hit loss: Rs{worst:.0f}  ({len(sl_hits)} SL-hit trades)")
