"""
Day-by-day walkthrough: for each trading day, compute that day's ATM (from
NIFTY's open), derive that day's 200-ITM CE (ATM-200) and PE (ATM+200)
strikes, pull EACH strike's own real (unstitched) 1-min history up through
that day, run the 60m zone -> 15m ref -> 5m sub-zone -> arm -> swing_breach
pipeline (bear-trap/LONG only -- buyers-only), and report whether an entry
completed on THAT specific day. No cross-strike stitching -- each day/strike
uses only its own genuine price series, so no artificial jumps.
"""
import sys, os
sys.path.insert(0, ".")
import scripts.d1trap_fractal_backtest as bt
import pandas as pd

CACHE_DIR = bt.CACHE_DIR
OPT_DIR = os.path.join(CACHE_DIR, "aug4_options")
LOT_SIZE = 65
STRIKE_STEP = 50
ITM_OFFSET_PTS = 200
WINDOW_START = pd.Timestamp("2026-06-29", tz=bt.IST)
WINDOW_END = pd.Timestamp("2026-07-29 23:59:59", tz=bt.IST)

_opt_1m_cache = {}
_pipeline_cache = {}   # (strike, side, as_of_day) -> trades list


def load_option_1m(strike: int, side: str) -> pd.DataFrame:
    key = (strike, side)
    if key not in _opt_1m_cache:
        path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        if not os.path.exists(path):
            _opt_1m_cache[key] = pd.DataFrame()
        else:
            df = pd.read_parquet(path)
            df["datetime"] = pd.to_datetime(df["datetime"])
            _opt_1m_cache[key] = df.sort_values("datetime").reset_index(drop=True)
    return _opt_1m_cache[key]


def trades_for_strike_up_to(strike: int, side: str, as_of_day) -> list:
    """All trades this strike's OWN chart produces, using only data through as_of_day."""
    key = (strike, side, as_of_day)
    if key in _pipeline_cache:
        return _pipeline_cache[key]

    df = load_option_1m(strike, side)
    df = df[df["datetime"].dt.date <= as_of_day]
    if len(df) < 100:
        _pipeline_cache[key] = []
        return []

    resamples = {}
    m_htf = bt.get_resample(60, df, resamples)
    m_ref = bt.get_resample(15, df, resamples)
    m_sub = bt.get_resample(5, df, resamples)
    zones = bt.detect_d1_zones(bt.to_bars(m_htf))
    bear_zones = [z for z in zones if z["direction"] == "LONG"]
    if not bear_zones:
        _pipeline_cache[key] = []
        return []

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                     sub_minutes=5, entry_mode="swing_breach")
    trades = bt.run_backtest(bear_zones, df, m_ref, m_sub, m_sub, cfg)
    _pipeline_cache[key] = trades
    return trades


def main():
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    window_spot = m1[(m1["datetime"] >= WINDOW_START) & (m1["datetime"] <= WINDOW_END)]

    rows = []
    for day, g in window_spot.groupby(window_spot["datetime"].dt.date):
        open_px = g.sort_values("datetime").iloc[0]["open"]
        atm = round(open_px / STRIKE_STEP) * STRIKE_STEP
        ce_strike = int(atm - ITM_OFFSET_PTS)
        pe_strike = int(atm + ITM_OFFSET_PTS)

        ce_trades = trades_for_strike_up_to(ce_strike, "CE", day)
        pe_trades = trades_for_strike_up_to(pe_strike, "PE", day)
        ce_today = [t for t in ce_trades if t["entry_ts"].date() == day]
        pe_today = [t for t in pe_trades if t["entry_ts"].date() == day]

        def _zone_count(strike, side):
            d = load_option_1m(strike, side)
            d = d[d["datetime"].dt.date <= day]
            if len(d) < 100:
                return 0
            m_htf = bt.get_resample(60, d, {})
            if m_htf.empty:
                return 0
            return len([z for z in bt.detect_d1_zones(bt.to_bars(m_htf)) if z["direction"] == "LONG"])

        ce_zone_count = _zone_count(ce_strike, "CE")
        pe_zone_count = _zone_count(pe_strike, "PE")

        todays = [("CE", t) for t in ce_today] + [("PE", t) for t in pe_today]
        if todays:
            for side_label, t in todays:
                rows.append(dict(date=day, atm=int(atm), ce_strike=ce_strike, pe_strike=pe_strike,
                                  ce_zones=ce_zone_count, pe_zones=pe_zone_count,
                                  side=side_label,
                                  entry=t["entry_price"], sl=t["sl"], exit=t["exit_price"],
                                  reason=t["reason"], pnl=t["pnl_pts"]*LOT_SIZE))
        else:
            rows.append(dict(date=day, atm=int(atm), ce_strike=ce_strike, pe_strike=pe_strike,
                              ce_zones=ce_zone_count, pe_zones=pe_zone_count,
                              side="-", entry=None, sl=None, exit=None, reason="no_trade", pnl=0))

    print(f"{'Date':<12} {'ATM':>6} {'CE_Strike':>10} {'PE_Strike':>10} {'CE_Zones':>9} {'PE_Zones':>9} "
          f"{'Side':>5} {'Entry':>8} {'SL':>8} {'Exit':>8} {'Reason':>10} {'PnL(Rs)':>10}")
    print("-" * 120)
    for r in rows:
        entry_s = f"{r['entry']:.2f}" if r['entry'] is not None else "-"
        sl_s = f"{r['sl']:.2f}" if r['sl'] is not None else "-"
        exit_s = f"{r['exit']:.2f}" if r['exit'] is not None else "-"
        print(f"{str(r['date']):<12} {r['atm']:>6} {r['ce_strike']:>10} {r['pe_strike']:>10} "
              f"{r['ce_zones']:>9} {r['pe_zones']:>9} {r['side']:>5} {entry_s:>8} {sl_s:>8} "
              f"{exit_s:>8} {r['reason']:>10} {r['pnl']:>+10,.0f}")

    traded_rows = [r for r in rows if r["side"] != "-"]
    days_with_trade = len(set(r["date"] for r in traded_rows))
    total_days = len(set(r["date"] for r in rows))
    total_pnl = sum(r["pnl"] for r in rows)
    print(f"\nDays with >=1 trade: {days_with_trade}/{total_days}")
    print(f"Total trades: {len(traded_rows)}   Total PnL: Rs{total_pnl:+,.0f}")


if __name__ == "__main__":
    main()
