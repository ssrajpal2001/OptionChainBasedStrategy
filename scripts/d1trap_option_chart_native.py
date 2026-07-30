"""
Entry signals generated DIRECTLY off the option's own OHLC chart, not spot.
For each trading day, compute the day's ATM (from spot) -> 200-ITM CE strike
(ATM-200) and 200-ITM PE strike (ATM+200). Run the fractal zone detector
(60m zone -> 15m ref-candle -> 5m sub-zone -> arm -> swing_breach) on EACH
strike's own 1-min series, restricted to LONG-direction zones only (bear-trap:
sellers/writers trapped, price reclaims higher) -- since we only ever BUY
(go long) whichever of CE/PE fires; a SHORT-direction signal on an option
chart would mean "sell this option," which this strategy never does, so
continuation/retest (which would flip to SHORT) are disabled entirely.
CE and PE are scanned in parallel; whichever fires first on any given day
is the trade. SL/TSL/P&L come out already denominated in option premium
points, since the detector is running on the option's own price series --
no spot->premium translation needed.
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

_opt_cache = {}


def load_option(strike: int, side: str) -> pd.DataFrame:
    key = (strike, side)
    if key not in _opt_cache:
        path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        df = pd.read_parquet(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        _opt_cache[key] = df.sort_values("datetime").reset_index(drop=True)
    return _opt_cache[key]


def daily_itm_strikes(m1_spot: pd.DataFrame) -> dict:
    out = {}
    for day, g in m1_spot.groupby(m1_spot["datetime"].dt.date):
        open_px = g.sort_values("datetime").iloc[0]["open"]
        atm = round(open_px / STRIKE_STEP) * STRIKE_STEP
        out[day] = dict(ce=int(atm - ITM_OFFSET_PTS), pe=int(atm + ITM_OFFSET_PTS))
    return out


def scan_option_chart(strike: int, side: str) -> list:
    """Run the LONG-only fractal detector on one option strike's own OHLC."""
    opt_1m = load_option(strike, side).rename(columns={"datetime": "dt"})
    opt_1m = opt_1m.rename(columns={"dt": "datetime"})
    if len(opt_1m) < 200:
        return []

    resamples = {}
    m_ref = bt.get_resample(15, opt_1m, resamples)
    m_sub = bt.get_resample(5, opt_1m, resamples)
    m_htf = bt.get_resample(60, opt_1m, resamples)
    htf_bars = bt.to_bars(m_htf)

    zones = bt.detect_d1_zones(htf_bars)
    zones = [z for z in zones if z["direction"] == "LONG"]   # bear-trap only -- buyers-only strategy
    if not zones:
        return []

    cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                     fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                     sub_minutes=5, entry_mode="swing_breach")
    trades = bt.run_backtest(zones, opt_1m, m_ref, m_sub, m_sub, cfg)
    for t in trades:
        t["strike"] = strike
        t["side"] = side
    return trades


def main():
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    month_spot = m1[(m1["datetime"] >= "2026-06-29") & (m1["datetime"] <= "2026-07-29")]
    day_strikes = daily_itm_strikes(month_spot)

    ce_strikes = sorted(set(v["ce"] for v in day_strikes.values()))
    pe_strikes = sorted(set(v["pe"] for v in day_strikes.values()))
    print(f"CE strikes needed: {ce_strikes}")
    print(f"PE strikes needed: {pe_strikes}\n")

    all_candidates = []
    for s in ce_strikes:
        trades = scan_option_chart(s, "CE")
        print(f"  CE {s}: {len(trades)} raw LONG signals on its own chart")
        all_candidates.extend(trades)
    for s in pe_strikes:
        trades = scan_option_chart(s, "PE")
        print(f"  PE {s}: {len(trades)} raw LONG signals on its own chart")
        all_candidates.extend(trades)

    # Keep only trades where the strike used WAS that day's actual 200-ITM strike
    # (a live system wouldn't be watching a strike that had drifted out of range).
    valid = []
    for t in all_candidates:
        day = t["entry_ts"].date()
        ds = day_strikes.get(day)
        if ds is None:
            continue
        if t["side"] == "CE" and t["strike"] == ds["ce"]:
            valid.append(t)
        elif t["side"] == "PE" and t["strike"] == ds["pe"]:
            valid.append(t)

    valid.sort(key=lambda t: t["entry_ts"])
    print(f"\nCandidates: {len(all_candidates)}   valid (day's actual 200-ITM strike): {len(valid)}")

    # Global one-position-at-a-time gate across CE+PE scanned simultaneously.
    final = []
    flat_until = None
    for t in valid:
        if flat_until is not None and t["entry_ts"] < flat_until:
            continue
        final.append(t)
        flat_until = t["exit_ts"]

    print(f"Final trades (after flat-gate): {len(final)}\n")
    if not final:
        return

    wins = [t for t in final if t["pnl_pts"] > 0]
    losses = [t for t in final if t["pnl_pts"] <= 0]
    gw = sum(t["pnl_pts"] for t in wins) * LOT_SIZE
    gl = abs(sum(t["pnl_pts"] for t in losses)) * LOT_SIZE
    pf = gw / gl if gl > 0 else (99.0 if gw > 0 else 0.0)
    total = sum(t["pnl_pts"] for t in final) * LOT_SIZE
    print(f"n={len(final)}  win%={100*len(wins)/len(final):.1f}  Rs{total:+,.0f}  PF={pf:.2f}  "
          f"avg_win=Rs{gw/len(wins) if wins else 0:,.0f}  avg_loss=Rs{-gl/len(losses) if losses else 0:,.0f}\n")
    for t in final:
        print(f"  {t['entry_ts']}  {t['strike']}{t['side']}  entry={t['entry_price']:.2f} "
              f"sl={t['sl']:.2f} exit={t['exit_price']:.2f}  ({t['reason']})  "
              f"Rs{t['pnl_pts']*LOT_SIZE:+,.0f}")


if __name__ == "__main__":
    main()
