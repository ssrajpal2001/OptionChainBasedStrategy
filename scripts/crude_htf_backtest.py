"""
crude_htf_backtest.py — CrudeOil HTF Zone Comparison Backtest

Compares trap-scanner performance at three HTF resolutions (30min, 60min, 120min)
on CrudeOil MCX futures for a given date range.

Zone logic (mirrors live engine):
  - Prev-day bars resampled to HTF → scan_htf_spot → TRAPPED zones (SL hit on prev day)
  - Intraday fallback: if no prev-day TRAPPED zone, scan today's bars at HTF (progressively)
  - Entry: futures price retraces into zone (zone_low <= price <= zone_high)
           + 5-min futures all-sellers-cleared (BEAR) / all-buyers-cleared (BULL)
  - BEAR zone → LONG futures (CE direction)
  - BULL zone → SHORT futures (PE direction)
  - SL:  zone_low - SL_BUF (LONG) / zone_high + SL_BUF (SHORT)
  - T1:  zone's sl field (the trapped-party SL level that confirmed the trap)
  - EOD: square-off at 23:00

Usage:
  python scripts/crude_htf_backtest.py --token YOUR_TOKEN
  python scripts/crude_htf_backtest.py --token TOKEN --start 2026-06-01 --end 2026-06-24
  python scripts/crude_htf_backtest.py --token TOKEN --days 15 --lots 2 --csv
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

# ── Constants ─────────────────────────────────────────────────────────────────
FUT_KEY   = "MCX_FO|520702"
CRUDE_LOT = 100               # 1 lot = 100 barrels
SL_BUF    = 20.0
MKT_OPEN  = "09:00"
MKT_CLOSE = "23:30"
ENTRY_OPEN = "09:30"
SQ_OFF     = "23:00"
LTF_MIN    = 5
HTF_LIST   = [30, 60, 120]

HEADERS: dict = {}

# ── API helpers ────────────────────────────────────────────────────────────────
def _get(url: str) -> dict:
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 429:
                time.sleep(2)
                continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}


def fetch_1m(key: str, dt: str) -> pd.DataFrame:
    enc  = key.replace("|", "%7C")
    url  = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}"
    data = _get(url)
    cands = data.get("data", {}).get("candles", [])
    if not cands:
        return pd.DataFrame()
    df = pd.DataFrame(cands, columns=["datetime", "open", "high", "low", "close", "volume", "oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = (df.sort_values("datetime")
           .loc[lambda x: (x["datetime"].dt.strftime("%H:%M") >= MKT_OPEN) &
                          (x["datetime"].dt.strftime("%H:%M") <= MKT_CLOSE)]
           .reset_index(drop=True))
    return df


def resample_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    r = (df.set_index("datetime")
           .resample(f"{minutes}min", label="right", closed="right")
           .agg({"open": "first", "high": "max", "low": "min",
                 "close": "last", "volume": "sum"})
           .dropna(subset=["open"])
           .reset_index())
    return r


# ── Trading day helpers ────────────────────────────────────────────────────────
def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while p.weekday() >= 5:
        p -= timedelta(days=1)
    return p


def trading_days_range(start: date, end: date) -> list[str]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def trading_days_last_n(n: int) -> list[str]:
    days, d = [], date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.isoformat())
    return list(reversed(days))


# ── Prev-day bias filter ──────────────────────────────────────────────────────
def day_bias(prev_bars: pd.DataFrame, atr: float, bias_atr_mult: float = 0.5) -> str:
    """
    Returns 'LONG_OK', 'SHORT_OK', or 'BOTH' based on prev-day candle direction.

    If prev-day was a strong bearish candle (close < open - bias_atr_mult*ATR):
      → next day skip LONG entries (price likely to continue down).
    If prev-day was a strong bullish candle (close > open + bias_atr_mult*ATR):
      → next day skip SHORT entries.
    Otherwise: both directions allowed.
    """
    if prev_bars.empty or atr <= 0:
        return "BOTH"
    prev_open  = float(prev_bars.iloc[0]["open"])
    prev_close = float(prev_bars.iloc[-1]["close"])
    threshold  = bias_atr_mult * atr
    if prev_close < prev_open - threshold:
        return "SHORT_OK"   # bearish prev-day → avoid LONG next day
    if prev_close > prev_open + threshold:
        return "LONG_OK"    # bullish prev-day → avoid SHORT next day
    return "BOTH"


def calc_atr(bars: pd.DataFrame, period: int = 14) -> float:
    """Simple ATR from 1-min bars (true range avg)."""
    if len(bars) < 2:
        return 0.0
    df = bars.copy()
    df["tr"] = (df["high"] - df["low"]).abs()
    return float(df["tr"].tail(period * 60).mean())  # last ~14 sessions of 1m bars


# ── LTF confirmation ────────────────────────────────────────────────────────────
def _sellers_cleared(bars_5m: pd.DataFrame) -> bool:
    """BEAR sellers cleared: at least one CLOSED BEAR zone, zero TRAPPED."""
    if len(bars_5m) < 3:
        return False
    _, entries = scanner.scan_htf(bars_5m)
    trapped = [e for e in entries if e["status"] == "TRAPPED"]
    closed  = [e for e in entries if e["status"] == "CLOSED"]
    return len(closed) > 0 and len(trapped) == 0


def _buyers_cleared(bars_5m: pd.DataFrame) -> bool:
    """BULL buyers cleared: at least one CLOSED BULL zone, zero TRAPPED (using scan_htf_spot)."""
    if len(bars_5m) < 3:
        return False
    _, entries = scanner.scan_htf_spot(bars_5m)
    bull = [e for e in entries if e.get("kind") == "BULL"]
    trapped = [e for e in bull if e["status"] == "TRAPPED"]
    closed  = [e for e in bull if e["status"] == "CLOSED"]
    return len(closed) > 0 and len(trapped) == 0


# ── Zone detection ─────────────────────────────────────────────────────────────
def get_trapped_zones(bars: pd.DataFrame, htf_min: int) -> list[dict]:
    if bars.empty:
        return []
    htf = resample_htf(bars, htf_min)
    if len(htf) < 2:
        return []
    _, entries = scanner.scan_htf_spot(htf)
    return [e for e in entries if e.get("status") == "TRAPPED"]


# ── One-day simulation ─────────────────────────────────────────────────────────
def simulate_day(today_bars: pd.DataFrame, htf_zones: list[dict],
                 htf_min: int, sl_buf: float, lots: int, day_str: str,
                 bias: str = "BOTH") -> list[dict]:
    """
    Simulate trades for one day.
    - htf_zones: TRAPPED zones from prev-day (or intraday fallback)
    - bias: 'BOTH'|'LONG_OK'|'SHORT_OK' — filters entry direction (prev-day trend filter)
    - Entry bar-by-bar; one position at a time; intraday fallback added progressively
    - Each zone may only be entered ONCE per day (uid dedup — mirrors live _notified_uids).
    """
    trades: list[dict] = []
    position: dict | None = None
    entered_zones: set[str] = set()   # zone UIDs entered today — no re-entry per zone

    # Pre-build 1m index for slicing
    today_bars = today_bars.reset_index(drop=True)
    n = len(today_bars)

    for idx in range(n):
        row     = today_bars.iloc[idx]
        ts      = row["datetime"]
        ts_str  = ts.strftime("%H:%M")
        ltp     = float(row["close"])
        bar_low = float(row["low"])
        bar_high= float(row["high"])

        # ── EOD square-off (blocks both open position and new entries) ───────
        if ts_str >= SQ_OFF:
            if position:
                trades.append({**position, "exit_ts": ts, "exit_price": ltp, "reason": "EOD"})
                position = None
            continue          # no new entries at or after SQ_OFF

        if ts_str < ENTRY_OPEN:
            continue

        # ── Manage open position ────────────────────────────────────────────
        if position:
            if position["direction"] == "LONG":
                if bar_low <= position["sl"]:
                    trades.append({**position, "exit_ts": ts, "exit_price": position["sl"], "reason": "SL"})
                    position = None
                elif bar_high >= position["t1"]:
                    trades.append({**position, "exit_ts": ts, "exit_price": position["t1"], "reason": "T1"})
                    position = None
            else:  # SHORT
                if bar_high >= position["sl"]:
                    trades.append({**position, "exit_ts": ts, "exit_price": position["sl"], "reason": "SL"})
                    position = None
                elif bar_low <= position["t1"]:
                    trades.append({**position, "exit_ts": ts, "exit_price": position["t1"], "reason": "T1"})
                    position = None
            continue

        # ── Build active zone list: prev-day + intraday fallback ────────────
        active_zones = list(htf_zones)  # start with prev-day zones
        if not active_zones:
            # Intraday fallback: scan bars so far (no lookahead past current bar)
            intra_bars = today_bars.iloc[: idx + 1]
            active_zones = get_trapped_zones(intra_bars, htf_min)

        # ── Entry scan ───────────────────────────────────────────────────────
        for z in active_zones:
            zl   = float(z.get("zone_low",  0))
            zh   = float(z.get("zone_high", 0))
            zsl  = float(z.get("sl",        0))
            kind = z.get("kind", "BEAR")
            zuid = f"{kind}_{zl:.1f}_{zh:.1f}"

            if zuid in entered_zones:
                continue        # one entry per zone per day
            if not (zl <= ltp <= zh):
                continue
            if zsl <= 0:
                continue

            # LTF confirmation on futures bars up to and including current bar
            bars_so_far = today_bars.iloc[: idx + 1]
            ltf_bars    = resample_htf(bars_so_far, LTF_MIN)

            if kind == "BEAR":
                if bias == "SHORT_OK":
                    continue        # prev-day was bearish → skip LONG
                if not _sellers_cleared(ltf_bars):
                    continue
                direction = "LONG"
                sl_price  = zl - sl_buf
                t1_price  = zsl
                opt_type  = "CE"
            else:  # BULL
                if bias == "LONG_OK":
                    continue        # prev-day was bullish → skip SHORT
                if not _buyers_cleared(ltf_bars):
                    continue
                direction = "SHORT"
                sl_price  = zh + sl_buf
                t1_price  = zsl
                opt_type  = "PE"

            entered_zones.add(zuid)
            position = {
                "entry_ts":    ts,
                "entry_price": ltp,
                "direction":   direction,
                "opt_type":    opt_type,
                "sl":          sl_price,
                "t1":          t1_price,
                "zone":        f"{zl:.0f}-{zh:.0f}",
                "kind":        kind,
                "lots":        lots,
                "zone_source": "intraday" if not htf_zones else "prev-day",
            }
            break  # one entry per bar

    # EOD cleanup if still in trade at last bar
    if position:
        last = today_bars.iloc[-1]
        trades.append({**position,
                       "exit_ts":    last["datetime"],
                       "exit_price": float(last["close"]),
                       "reason":     "EOD"})
    return trades


# ── Per-trade P&L ──────────────────────────────────────────────────────────────
def trade_pnl(t: dict) -> float:
    pts = (t["exit_price"] - t["entry_price"]) if t["direction"] == "LONG" else \
          (t["entry_price"] - t["exit_price"])
    return round(pts * t["lots"] * CRUDE_LOT, 2)


def trade_pts(t: dict) -> float:
    return round((t["exit_price"] - t["entry_price"]) if t["direction"] == "LONG" else
                 (t["entry_price"] - t["exit_price"]), 2)


# ── One-HTF full backtest ──────────────────────────────────────────────────────
def run_htf(days: list[str], htf_min: int, sl_buf: float, lots: int,
            bias_mult: float = 0.5) -> dict:
    all_trades: list[dict] = []
    day_results: list[dict] = []

    print(f"\n{'='*60}")
    print(f"  HTF = {htf_min}-min")
    print(f"{'='*60}")

    for day_str in days:
        day_d  = date.fromisoformat(day_str)
        prev_d = prev_trading_day(day_d)

        print(f"  {day_str}  fetching...", end=" ", flush=True)
        prev_bars  = fetch_1m(FUT_KEY, prev_d.isoformat())
        today_bars = fetch_1m(FUT_KEY, day_str)

        if today_bars.empty:
            print("no bars (holiday?)")
            continue

        # Prev-day TRAPPED zones + prev-day bias
        htf_zones  = get_trapped_zones(prev_bars, htf_min) if not prev_bars.empty else []
        atr_val    = calc_atr(prev_bars) if not prev_bars.empty else 0.0
        bias       = day_bias(prev_bars, atr_val, bias_mult) if not prev_bars.empty and bias_mult > 0 else "BOTH"

        print(f"prev={len(prev_bars)} today={len(today_bars)} prev_zones={len(htf_zones)} "
              f"bias={bias}", end=" ", flush=True)

        day_trades = simulate_day(today_bars, htf_zones, htf_min, sl_buf, lots, day_str, bias)
        day_pnl    = sum(trade_pnl(t) for t in day_trades)

        print(f"trades={len(day_trades)} pnl=₹{day_pnl:,.0f}")

        for t in day_trades:
            pts = trade_pts(t)
            all_trades.append({
                "htf":     htf_min,
                "date":    day_str,
                "entry":   str(t["entry_ts"])[:16],
                "exit":    str(t["exit_ts"])[:16],
                "dir":     t["direction"],
                "type":    t["opt_type"],
                "zone":    t["zone"],
                "kind":    t["kind"],
                "src":     t.get("zone_source", ""),
                "ep":      round(t["entry_price"], 2),
                "xp":      round(t["exit_price"],  2),
                "sl":      round(t["sl"],           2),
                "t1":      round(t["t1"],           2),
                "reason":  t["reason"],
                "pnl_pts": pts,
                "pnl_rs":  trade_pnl(t),
            })
        day_results.append({"date": day_str, "trades": len(day_trades), "pnl_rs": day_pnl})
        time.sleep(0.4)   # Upstox rate-limit courtesy delay

    wins   = [t for t in all_trades if t["pnl_pts"] > 0]
    losses = [t for t in all_trades if t["pnl_pts"] <= 0]
    total  = sum(t["pnl_rs"] for t in all_trades)
    return {
        "htf":          htf_min,
        "days":         len(day_results),
        "trades":       len(all_trades),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_pct":      round(len(wins) / len(all_trades) * 100, 1) if all_trades else 0.0,
        "total_pnl":    round(total, 0),
        "avg_win_pts":  round(sum(t["pnl_pts"] for t in wins)   / len(wins),   2) if wins   else 0.0,
        "avg_loss_pts": round(sum(t["pnl_pts"] for t in losses) / len(losses), 2) if losses else 0.0,
        "all_trades":   all_trades,
        "day_results":  day_results,
    }


# ── Summary printer ────────────────────────────────────────────────────────────
def print_summary(results: list[dict], lots: int, sl_buf: float):
    print("\n" + "=" * 75)
    print("  CRUDEOIL HTF BACKTEST — SUMMARY")
    print(f"  Lots: {lots}  |  SL buffer: ₹{sl_buf}  |  Lot size: {CRUDE_LOT} barrels")
    print("=" * 75)
    hdr = f"{'HTF':>6}  {'Days':>5}  {'Trades':>7}  {'Wins':>5}  {'Win%':>6}  " \
          f"{'TotalPnL':>11}  {'AvgWin':>8}  {'AvgLoss':>9}"
    print(hdr)
    print("-" * 75)
    for r in results:
        print(f"{r['htf']:>4}min  {r['days']:>5}  {r['trades']:>7}  {r['wins']:>5}  "
              f"{r['win_pct']:>5.1f}%  ₹{r['total_pnl']:>10,.0f}  "
              f"{r['avg_win_pts']:>7.1f}pts  {r['avg_loss_pts']:>8.1f}pts")
    print("=" * 75)

    # Per-day matrix
    all_days = sorted({dr["date"] for r in results for dr in r["day_results"]})
    if not all_days:
        return
    print(f"\n  PER-DAY P&L (₹)")
    print(f"{'Date':>12}", end="")
    for r in results:
        print(f"  {r['htf']:>3}min", end="")
    print()
    print(f"  {'-'*40}")
    col_totals = {r["htf"]: 0 for r in results}
    for d in all_days:
        print(f"{d:>12}", end="")
        for r in results:
            dr = next((x for x in r["day_results"] if x["date"] == d), None)
            val = dr["pnl_rs"] if dr else 0
            col_totals[r["htf"]] += val
            tag = "  " + (f"₹{val:>7,.0f}" if dr and dr["trades"] > 0 else "        -")
            print(tag, end="")
        print()
    print(f"{'TOTAL':>12}", end="")
    for r in results:
        print(f"  ₹{col_totals[r['htf']]:>7,.0f}", end="")
    print()


def save_csv(results: list[dict]):
    out_dir = os.path.join(os.path.dirname(__file__))
    for r in results:
        df = pd.DataFrame(r["all_trades"])
        if df.empty:
            continue
        path = os.path.join(out_dir, f"crude_htf_{r['htf']}min.csv")
        df.to_csv(path, index=False)
        print(f"  Saved: {path}")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="CrudeOil HTF backtest (30/60/120 min)")
    ap.add_argument("--token",  required=True, help="Upstox access token")
    ap.add_argument("--start",  default="",    help="Start date YYYY-MM-DD")
    ap.add_argument("--end",    default="",    help="End date YYYY-MM-DD")
    ap.add_argument("--days",   type=int, default=0,    help="Last N trading days (overrides start/end)")
    ap.add_argument("--lots",   type=int, default=2,    help="Lots per trade (default 2)")
    ap.add_argument("--sl_buf", type=float, default=20.0, help="SL buffer Rs (default 20)")
    ap.add_argument("--htf",    default="30,60,120",    help="Comma-sep HTF list (default 30,60,120)")
    ap.add_argument("--csv",    action="store_true",    help="Save trade CSV per HTF")
    ap.add_argument("--bias_mult", type=float, default=0.5,
                    help="Prev-day bias ATR multiplier (default 0.5). "
                         "0=disable, 1.0=stricter filter (only block on big trend days)")
    args = ap.parse_args()

    global HEADERS
    HEADERS = {"Authorization": f"Bearer {args.token}", "Accept": "application/json"}

    htf_list = [int(x.strip()) for x in args.htf.split(",")]

    if args.days > 0:
        days = trading_days_last_n(args.days)
    elif args.start and args.end:
        days = trading_days_range(date.fromisoformat(args.start), date.fromisoformat(args.end))
    else:
        # Default: June 2026
        days = trading_days_range(date(2026, 6, 1), date(2026, 6, 24))

    print(f"Backtest: {days[0]} to {days[-1]}  ({len(days)} trading days)")
    print(f"HTF list: {htf_list}  |  Lots: {args.lots}  |  SL_buf: ₹{args.sl_buf}")

    results = [run_htf(days, h, args.sl_buf, args.lots, args.bias_mult) for h in htf_list]

    print_summary(results, args.lots, args.sl_buf)

    if args.csv:
        save_csv(results)

    # Trade details for the best HTF
    best = max(results, key=lambda r: r["total_pnl"])
    print(f"\n  Best HTF: {best['htf']}-min  (₹{best['total_pnl']:,.0f} over {best['days']} days)")
    if best["all_trades"]:
        df = pd.DataFrame(best["all_trades"])
        print(df[["date","entry","exit","dir","zone","reason","pnl_pts","pnl_rs"]].to_string(index=False))


if __name__ == "__main__":
    main()
