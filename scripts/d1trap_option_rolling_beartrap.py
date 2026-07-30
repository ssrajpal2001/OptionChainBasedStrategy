"""
Fixes the strike-drift problem: builds a ROLLING synthetic "current 200-ITM CE"
and "current 200-ITM PE" series by recomputing the ITM strike fresh each day
(from that day's NIFTY open) and stitching each day's own correct-strike bars
together, before running zone detection -- instead of scanning one fixed
strike's chart across the whole window (which drifts out of relevance) or
scanning strikes independently and discarding almost everything post-hoc.

Caveat printed explicitly: because a strike roll swaps to a genuinely
different instrument, the stitched series can show an artificial price jump
at each day boundary (a 24050 CE and a 24000 CE are not the same value even
for the same spot) -- this is a real limitation of building a continuous
series this way, not smoothed over here.
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
WINDOW_START = pd.Timestamp("2026-07-15", tz=bt.IST)
WINDOW_END = pd.Timestamp("2026-07-29 23:59:59", tz=bt.IST)

_opt_cache = {}


def load_option(strike: int, side: str) -> pd.DataFrame:
    key = (strike, side)
    if key not in _opt_cache:
        path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        df = pd.read_parquet(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        _opt_cache[key] = df.sort_values("datetime").reset_index(drop=True)
    return _opt_cache[key]


def build_rolling_series(day_strikes: dict, side: str) -> pd.DataFrame:
    frames = []
    jumps = []
    prev_close = None
    for day in sorted(day_strikes):
        strike = day_strikes[day][side.lower()]
        path = os.path.join(OPT_DIR, f"{strike}_{side}.parquet")
        if not os.path.exists(path):
            print(f"  WARN {day} {side} strike {strike}: not cached, skipping day")
            continue
        df = load_option(strike, side)
        day_bars = df[df["datetime"].dt.date == day]
        if day_bars.empty:
            continue
        if prev_close is not None:
            jump = day_bars.iloc[0]["open"] - prev_close
            if abs(jump) > 1.0:
                jumps.append((day, strike, round(jump, 2)))
        prev_close = day_bars.iloc[-1]["close"]
        frames.append(day_bars.assign(roll_strike=strike))
    if not frames:
        return pd.DataFrame(), []
    out = pd.concat(frames, ignore_index=True).sort_values("datetime").reset_index(drop=True)
    return out, jumps


def main():
    d1, m1 = bt.load_data("2025-07-30_2026-07-29")
    window_spot = m1[(m1["datetime"] >= WINDOW_START) & (m1["datetime"] <= WINDOW_END)]

    day_strikes = {}
    for day, g in window_spot.groupby(window_spot["datetime"].dt.date):
        open_px = g.sort_values("datetime").iloc[0]["open"]
        atm = round(open_px / STRIKE_STEP) * STRIKE_STEP
        day_strikes[day] = dict(ce=int(atm - ITM_OFFSET_PTS), pe=int(atm + ITM_OFFSET_PTS))

    print("Daily 200-ITM strikes:")
    for day, s in sorted(day_strikes.items()):
        print(f"  {day}: CE={s['ce']}  PE={s['pe']}")

    all_trades = []
    for side in ("CE", "PE"):
        print(f"\n{'='*70}\n{side} rolling series\n{'='*70}")
        rolled, jumps = build_rolling_series(day_strikes, side)
        if rolled.empty:
            print("  no data")
            continue
        print(f"  {len(rolled)} 1-min bars, {rolled['roll_strike'].nunique()} distinct strikes used")
        if jumps:
            print(f"  ARTIFICIAL JUMPS at strike rolls (day, new_strike, jump_pts): {jumps}")
        else:
            print("  no jump >1pt detected at any roll boundary")

        resamples = {}
        m_htf = bt.get_resample(60, rolled, resamples)
        m_ref = bt.get_resample(15, rolled, resamples)
        m_sub = bt.get_resample(5, rolled, resamples)
        print(f"  60m bars: {len(m_htf)}  15m bars: {len(m_ref)}  5m bars: {len(m_sub)}")

        htf_bars = bt.to_bars(m_htf)
        zones = bt.detect_d1_zones(htf_bars)
        bear_zones = [z for z in zones if z["direction"] == "LONG"]
        print(f"  zones total={len(zones)}  bear-trap only={len(bear_zones)}")

        if not bear_zones:
            continue
        cfg = bt.Config(zone_size_threshold_pct=0.20, enable_continuation=False, enable_retest=False,
                         fallback_on_no_subzone="raw_breakout", htf_minutes=60, ref_minutes=15,
                         sub_minutes=5, entry_mode="swing_breach")
        trades = bt.run_backtest(bear_zones, rolled, m_ref, m_sub, m_sub, cfg)
        for t in trades:
            t["side"] = side
        print(f"  trades: {len(trades)}")
        all_trades.extend(trades)

    all_trades.sort(key=lambda t: t["entry_ts"])
    final, flat_until = [], None
    for t in all_trades:
        if flat_until is not None and t["entry_ts"] < flat_until:
            continue
        final.append(t)
        flat_until = t["exit_ts"]

    print(f"\n{'='*70}\nFinal (CE+PE combined, flat-gated): {len(final)}\n{'='*70}")
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
        print(f"  {t['entry_ts']}  {t['side']}  origin={t['origin']}  "
              f"entry={t['entry_price']:.2f} sl={t['sl']:.2f} exit={t['exit_price']:.2f}  "
              f"({t['reason']})  Rs{t['pnl_pts']*LOT_SIZE:+,.0f}")


if __name__ == "__main__":
    main()
