"""
scripts/d1trap_tranche_tsl_sweep.py -- live question (2026-08-03): a NIFTY
flip-tranche position ran up in profit, never reached the 20% TSL trigger,
then reversed into a loss with zero profit locked in. Should the tranche
TSL trigger/lock be tightened (mirroring the SAME fix already applied to
regular entries on 2026-07-30: 20%/12.5% -> 10%/7%, "gave back ~9 points
of a real move purely from step-size coarseness")?

Sweeps the tranche TSL shape against the SAME real month-window NIFTY data
already used for today's optimization (3-ITM, 60m HTF, ref.close boundary
-- the current live defaults), varying ONLY the flip-tranche TSL trigger/
lock/step values via module-constant monkeypatch (bb._TSL_TRANCHE_*).
"""
import sys
sys.path.insert(0, ".")
import strategies.d1_trap_option.bear_only_book as bb
import scripts.d1trap_month_rolling_backtest as mrb
import scripts.d1trap_verify_live_defaults as verify

LADDER_DIR = "data/d1trap_fractal_cache/strike_ladder"
SPOT_PATH = "data/d1trap_fractal_cache/nifty_1m_month_backtest.parquet"

VARIANTS = [
    ("20% / 12.5% (current)", 0.20, 0.125, 0.20, 0.125),
    ("15% / 10%",             0.15, 0.10,  0.15, 0.10),
    ("10% / 7% (matches regular-entry shape)", 0.10, 0.07, 0.10, 0.07),
    ("10% / 5%",              0.10, 0.05,  0.10, 0.05),
]

if __name__ == "__main__":
    mrb.MONTH_DIR = LADDER_DIR
    results = []
    for label, base_pct, base_lock, step_pct, step_lock in VARIANTS:
        bb._TSL_TRANCHE_BASE_PCT = base_pct
        bb._TSL_TRANCHE_BASE_LOCK_PCT = base_lock
        bb._TSL_TRANCHE_STEP_PCT = step_pct
        bb._TSL_TRANCHE_STEP_LOCK_PCT = step_lock
        trades = verify.run_month_live("NIFTY", "niftyladder", SPOT_PATH, 150, 100, 65, 60)
        stats = mrb.summarize(f"NIFTY tranche TSL {label}", trades)
        tsl_hits = sum(1 for t in trades if t["reason"] == "tsl_hit")
        sl_hits = sum(1 for t in trades if t["reason"] in ("sl_hit", "sl_hit_hard_cap"))
        eod_hits = sum(1 for t in trades if t["reason"] == "eod")
        print(f"  -> TSL={tsl_hits} SL={sl_hits} EOD={eod_hits}")
        results.append((label, stats, tsl_hits, sl_hits, eod_hits))

    print(f"\n{'='*100}\nSUMMARY -- tranche TSL sweep, real NIFTY month, current live defaults\n{'='*100}")
    print(f"{'Variant':<42}{'Trades':>8}{'Win%':>8}{'PF':>8}{'NetPnL':>12}{'TSL':>5}{'SL':>5}{'EOD':>5}")
    for label, s, tsl_n, sl_n, eod_n in results:
        print(f"{label:<42}{s['n']:>8}{s['win_pct']:>7.1f}%{s['pf']:>8.2f}{s['total']:>+12,.0f}{tsl_n:>5}{sl_n:>5}{eod_n:>5}")
