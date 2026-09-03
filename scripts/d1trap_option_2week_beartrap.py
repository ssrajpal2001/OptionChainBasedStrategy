"""
Per correction: use 200-POINTS-OTM CE and PE (not ITM), a single fixed strike
each (today's ATM +/-200), 2 WEEKS of 60-min history built from that strike's
own 1-min series, bear-trap (LONG-direction) zones ONLY on each of CE and PE
independently (we only ever buy -- go long -- whichever fires), then the same
15m ref-candle -> 5m sub-zone -> arm -> swing_breach pipeline as before.
"""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

CACHE_DIR = bt.CACHE_DIR
OPT_DIR = os.path.join(CACHE_DIR, "aug4_options")
LOT_SIZE = 65

CE_STRIKE = 24050   # ATM 24250 - 200 (200-ITM call)
PE_STRIKE = 24450   # ATM 24250 + 200 (200-ITM put)
WINDOW_START = pd.Timestamp("2026-07-15", tz=bt.IST)
WINDOW_END = pd.Timestamp("2026-07-29 23:59:59", tz=bt.IST)


def load_option(strike: int, side: str) -> pd.DataFrame:
    path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
    df = pd.read_parquet(path)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def scan(strike: int, side: str):
    opt_1m = load_option(strike, side)
    opt_1m = opt_1m[(opt_1m["datetime"] >= WINDOW_START) & (opt_1m["datetime"] <= WINDOW_END)]
    print(f"\n{strike}{side}: {len(opt_1m)} 1-min bars in the 2-week window "
          f"({opt_1m['datetime'].min() if not opt_1m.empty else 'n/a'} -> "
          f"{opt_1m['datetime'].max() if not opt_1m.empty else 'n/a'})")
    if len(opt_1m) < 100:
        print("  Not enough data.")
        return []

    resamples = {}
    m_htf = bt.get_resample(60, opt_1m, resamples)
    m_ref = bt.get_resample(15, opt_1m, resamples)
    m_sub = bt.get_resample(5, opt_1m, resamples)
    print(f"  60m bars: {len(m_htf)}   15m bars: {len(m_ref)}   5m bars: {len(m_sub)}")

    htf_bars = bt.to_bars(m_htf)
    zones = bt.detect_d1_zones(htf_bars)
    bear_zones = [z for z in zones if z["direction"] == "LONG"]
    print(f"  Total zones (both directions): {len(zones)}   bear-trap (LONG) only: {len(bear_zones)}")
    for z in bear_zones:
        print(f"    zone: lo={z['zone_lo']:.2f} hi={z['zone_hi']:.2f} "
              f"entry_line={z['entry_line']:.2f} lock_ts={z['lock_ts']}")

    if not bear_zones:
        return []

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                     sub_minutes=5, entry_mode="swing_breach")
    trades = bt.run_backtest(bear_zones, opt_1m, m_ref, m_sub, m_sub, cfg)
    for t in trades:
        t["strike"] = strike
        t["side"] = side
    print(f"  Trades produced: {len(trades)}")
    return trades


def main():
    ce_trades = scan(CE_STRIKE, "CE")
    pe_trades = scan(PE_STRIKE, "PE")

    all_trades = sorted(ce_trades + pe_trades, key=lambda t: t["entry_ts"])
    final = []
    flat_until = None
    for t in all_trades:
        if flat_until is not None and t["entry_ts"] < flat_until:
            continue
        final.append(t)
        flat_until = t["exit_ts"]

    print(f"\n{'='*80}\nFinal trades (CE+PE combined, flat-gated): {len(final)}\n{'='*80}")
    if not final:
        return
    wins = [t for t in final if t["pnl_pts"] > 0]
    losses = [t for t in final if t["pnl_pts"] <= 0]
    gw = sum(t["pnl_pts"] for t in wins) * LOT_SIZE
    gl = abs(sum(t["pnl_pts"] for t in losses)) * LOT_SIZE
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    total = sum(t["pnl_pts"] for t in final) * LOT_SIZE
    print(f"n={len(final)}  win%={100*len(wins)/len(final):.1f}  Rs{total:+,.0f}  PF={pf:.2f}\n")
    for t in final:
        print(f"  {t['entry_ts']}  {t['strike']}{t['side']}  origin={t['origin']}  "
              f"entry={t['entry_price']:.2f} sl={t['sl']:.2f} exit={t['exit_price']:.2f}  "
              f"({t['reason']})  Rs{t['pnl_pts']*LOT_SIZE:+,.0f}")


if __name__ == "__main__":
    main()
