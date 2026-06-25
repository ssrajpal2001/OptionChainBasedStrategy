"""
scripts/crude_backtest.py — CrudeOil FUTURES backtest (pure intraday cascade 30m→5m→1m).

Phase 1 (current): Futures-only P&L — no option key lookup.
  Zone detection: MCX futures 1m bars resampled to HTF/sub timeframes.
  Scanner: scan_htf_spot (designed for spot/futures, not option premium).
  Entry: BEAR zone → LONG futures; BULL zone → SHORT futures.
  P&L: (exit − entry) × CRUDE_LOT × lots  (LONG)
       (entry − exit) × CRUDE_LOT × lots  (SHORT)
  SL: intrabar — exits at sl_trigger price when bar_low/high crosses it.
  T1: zone_high (LONG) / zone_low (SHORT).
  EOD: 23:00 MCX.

Phase 2 (after CSV data): Add option P&L (run --options flag).

Market hours: 09:00 – 23:30 IST (MCX)

Usage:
  python3 scripts/crude_backtest.py --token YOUR_TOKEN
  python3 scripts/crude_backtest.py --token TOKEN --start 2026-06-01 --end 2026-06-25
  python3 scripts/crude_backtest.py --token TOKEN --weeks 4 --sl-buf 20 --max-ltf 5
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests

sys.path.insert(0, ".")
from strategies.trap_scanner import scanner

# ── Constants ──────────────────────────────────────────────────────────────────
CRUDE_STEP = 100      # Rs per strike step
CRUDE_LOT  = 100      # 1 lot = 100 barrels (standard crude)
MKT_OPEN   = "09:00"
MKT_CLOSE  = "23:30"
EOD_TIME   = "23:00"
ENTRY_OPEN = "09:30"  # no entries before 09:30

HTF_MIN_DEFAULT = 30  # 30m parent zones (CrudeOil moves in bigger swings)
SUB_MIN_DEFAULT = 5   # 5m sub-zones

_HEADERS: dict = {}

# ── Data helpers ───────────────────────────────────────────────────────────────
def _get(url: str) -> dict:
    for _ in range(3):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=25)
            if r.status_code == 429:
                time.sleep(2); continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}


def _fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    """Fetch 1m bars in 28-day chunks (Upstox API limit)."""
    from urllib.parse import quote
    f, t = date.fromisoformat(from_dt), date.fromisoformat(to_dt)
    chunks = []
    cur = f
    while cur <= t:
        nxt = min(cur + timedelta(days=28), t)
        enc = quote(key, safe="")
        url = (f"https://api.upstox.com/v2/historical-candle/{enc}"
               f"/1minute/{nxt.isoformat()}/{cur.isoformat()}")
        data = _get(url)
        cands = data.get("data", {}).get("candles", [])
        if cands:
            rows = [{"datetime": pd.to_datetime(c[0]),
                     "open": float(c[1]), "high": float(c[2]),
                     "low": float(c[3]), "close": float(c[4]),
                     "volume": int(c[5] or 0)}
                    for c in reversed(cands)]
            df = pd.DataFrame(rows)
            df["datetime"] = df["datetime"].dt.tz_localize(None)
            chunks.append(df)
        time.sleep(0.3)
        cur = nxt + timedelta(days=1)
    if not chunks:
        return pd.DataFrame()
    out = (pd.concat(chunks, ignore_index=True)
           .sort_values("datetime")
           .drop_duplicates("datetime")
           .reset_index(drop=True))
    return out


def _mkt_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to MCX market hours 09:00–23:30."""
    t0 = pd.Timestamp(MKT_OPEN).time()
    t1 = pd.Timestamp(MKT_CLOSE).time()
    return df[(df["datetime"].dt.time >= t0) &
              (df["datetime"].dt.time <= t1)].copy()


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r


def get_trading_days(start: str, end: str) -> list[str]:
    f, t = date.fromisoformat(start), date.fromisoformat(end)
    return [str(f + timedelta(days=i))
            for i in range((t - f).days + 1)
            if (f + timedelta(days=i)).weekday() < 5]


# ── Simulate exit on 1m futures bars ──────────────────────────────────────────
def _simulate_exit(
    fut_1m: pd.DataFrame,
    entry_ts: pd.Timestamp,
    entry_price: float,
    direction: str,   # "LONG" or "SHORT"
    zone_low: float,
    zone_high: float,
    sl_buf: float,
    trade_date: str,
    total_qty: int,
) -> dict:
    """
    LONG : SL = zone_low − sl_buf  (intrabar: bar_low < sl → exit at sl)
           T1 = zone_high
    SHORT: SL = zone_high + sl_buf (intrabar: bar_high > sl → exit at sl)
           T1 = zone_low
    After T1, ratchet TSL via 5m zone floors (LONG) or ceilings (SHORT).
    """
    sq_ts = pd.Timestamp(f"{trade_date} {EOD_TIME}")

    if direction == "LONG":
        init_sl  = zone_low  - sl_buf
        t1_price = zone_high
    else:
        init_sl  = zone_high + sl_buf
        t1_price = zone_low

    future_bars = fut_1m[fut_1m["datetime"] > entry_ts].copy()
    if future_bars.empty:
        return {"exit": entry_price, "exit_ts": entry_ts,
                "reason": "EOD", "t1_hit": False, "pnl_rs": 0}

    # 5m ratchet levels for TSL after T1
    df_5m = _resample(fut_1m[fut_1m["datetime"] <= sq_ts], 5)
    _, ltf5_all = scanner.scan_htf_spot(df_5m) if len(df_5m) >= 2 else (None, [])
    if direction == "LONG":
        ratchet_levels = sorted(
            [float(e.get("zone_low", 0))
             for e in (ltf5_all or [])
             if e.get("status") in ("TRAPPED", "CLOSED")
             and float(e.get("zone_low", 0)) > 0]
        )
    else:
        ratchet_levels = sorted(
            [float(e.get("zone_high", 0))
             for e in (ltf5_all or [])
             if e.get("status") in ("TRAPPED", "CLOSED")
             and float(e.get("zone_high", 0)) > 0],
            reverse=True
        )

    trail_sl     = init_sl
    t1_hit       = False
    t1_qty       = total_qty // 2
    rem_qty      = total_qty - t1_qty
    t1_pnl       = 0.0
    exit_price   = None
    exit_reason  = "OPEN"
    exit_ts_out  = None

    for _, row in future_bars.iterrows():
        bar_ts    = row["datetime"]
        bar_high  = float(row["high"])
        bar_low   = float(row["low"])
        bar_close = float(row["close"])

        if bar_ts >= sq_ts:
            exit_price  = bar_close
            exit_reason = "EOD"
            exit_ts_out = bar_ts
            break

        # T1 check
        if not t1_hit:
            t1_triggered = (bar_high >= t1_price) if direction == "LONG" else (bar_low <= t1_price)
            if t1_triggered:
                t1_hit  = True
                t1_pnl  = ((t1_price - entry_price) * t1_qty if direction == "LONG"
                           else (entry_price - t1_price) * t1_qty)
                trail_sl = zone_low if direction == "LONG" else zone_high

        # Ratchet TSL
        if t1_hit:
            if direction == "LONG":
                new_floor = max(
                    (f for f in ratchet_levels if f > trail_sl and f < bar_close),
                    default=trail_sl
                )
                if new_floor > trail_sl:
                    trail_sl = new_floor
            else:
                new_ceil = min(
                    (c for c in ratchet_levels if c < trail_sl and c > bar_close),
                    default=trail_sl
                )
                if new_ceil < trail_sl:
                    trail_sl = new_ceil

        # SL check (intrabar)
        active_sl  = trail_sl
        sl_trigger = (active_sl - sl_buf) if direction == "LONG" else (active_sl + sl_buf)
        sl_hit = (bar_low < sl_trigger) if direction == "LONG" else (bar_high > sl_trigger)
        if sl_hit:
            exit_price  = sl_trigger
            exit_reason = "TRAIL_SL" if t1_hit else "SL"
            exit_ts_out = bar_ts
            break

    if exit_price is None:
        last        = future_bars.iloc[-1]
        exit_price  = float(last["close"])
        exit_reason = "EOD"
        exit_ts_out = last["datetime"]

    exit_qty  = rem_qty if t1_hit else total_qty
    if direction == "LONG":
        rem_pnl = (exit_price - entry_price) * exit_qty
    else:
        rem_pnl = (entry_price - exit_price) * exit_qty
    total_pnl = int(round(t1_pnl + rem_pnl, 0))

    return {
        "exit":    round(exit_price, 2),
        "exit_ts": exit_ts_out,
        "reason":  exit_reason,
        "t1_hit":  t1_hit,
        "pnl_rs":  total_pnl,
    }


# ── Per-day per-direction backtest ─────────────────────────────────────────────
def _run_direction(
    trade_date: str,
    direction: str,         # "LONG" (BEAR zone) or "SHORT" (BULL zone)
    fut_1m: pd.DataFrame,   # today's futures 1m bars
    sl_buf: float,
    lots: int,
    max_ltf: int,
    htf_min: int,
    sub_min: int,
    zcache: dict,
) -> list[dict]:
    """
    Pure intraday cascade:
      HTF (30m) zone → sub (5m) sub-zone → 1m breakout entry
    Zone kind filter: LONG uses BEAR zones, SHORT uses BULL zones.
    """
    td       = pd.to_datetime(trade_date).date()
    kind_want = "BEAR" if direction == "LONG" else "BULL"
    total_qty = lots * CRUDE_LOT

    # ── Cache: HTF parent zones ───────────────────────────────────────────────
    htf_ck = (td, direction, htf_min, "cas")
    if htf_ck in zcache:
        cas_zones = zcache[htf_ck]
    else:
        df_htf = _resample(fut_1m, htf_min)
        _, cas_raw = scanner.scan_htf_spot(df_htf) if len(df_htf) >= 2 else (None, [])
        cas_zones = sorted(
            [e for e in (cas_raw or [])
             if e.get("status") in ("TRAPPED", "CLOSED")
             and e.get("kind") == kind_want],
            key=lambda z: float(z.get("zone_low", 9999))
        )
        zcache[htf_ck] = cas_zones

    if not cas_zones:
        print(f"  {trade_date} {direction}: no {htf_min}m {kind_want} zones")
        return []

    # ── Cache: sub-zone scan ──────────────────────────────────────────────────
    sub_ck = (td, sub_min, "sub_spot")
    if sub_ck in zcache:
        sub_all = zcache[sub_ck]
    else:
        df_sub = _resample(fut_1m, sub_min)
        _, sub_all = scanner.scan_htf_spot(df_sub) if len(df_sub) >= 2 else (None, [])
        zcache[sub_ck] = sub_all or []

    trades     = []
    open_trade = None
    entry_open_ts = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}")

    for cz in cas_zones:
        zh = float(cz["zone_high"])
        zl = float(cz["zone_low"])

        # Sub-zones within this HTF zone, matching direction
        ltf_in = [
            e for e in (sub_all or [])
            if e.get("status") in ("TRAPPED", "CLOSED")
            and e.get("kind") == kind_want
            and float(e.get("zone_high", 0)) <= zh * 1.02
            and float(e.get("zone_low",  0)) >= zl * 0.98
        ]
        if not ltf_in:
            print(f"  {trade_date} {direction}: {htf_min}m {zl:.0f}-{zh:.0f} → "
                  f"no {sub_min}m sub-zone — SKIP")
            continue

        ltf_in.sort(key=lambda e: float(e.get("zone_low", 9999)))
        max_idx = max_ltf if max_ltf > 0 else len(ltf_in)
        added   = 0

        for idx, sz in enumerate(ltf_in[:max_idx]):
            sz_low  = float(sz["zone_low"])
            sz_high = float(sz["zone_high"])
            if (sz_high - sz_low) < sl_buf:   # zone too narrow
                continue

            # 1m breakout: find first 1m bar after zone close
            trap_ts = pd.to_datetime(
                sz.get("closed_on") or sz.get("trapped_on") or sz.get("ref_ts") or "NaT"
            )
            if trap_ts is pd.NaT or trap_ts is None:
                continue
            if hasattr(trap_ts, "tzinfo") and trap_ts.tzinfo:
                trap_ts = trap_ts.tz_localize(None)

            search = fut_1m[
                (fut_1m["datetime"] > trap_ts) &
                (fut_1m["datetime"] >= entry_open_ts)
            ]
            if direction == "LONG":
                # BEAR zone: entry when 1m HIGH breaks above zone_high
                breakout = search[search["high"] > sz_high]
            else:
                # BULL zone: entry when 1m LOW breaks below zone_low
                breakout = search[search["low"] < sz_low]

            if breakout.empty:
                continue

            entry_ts_bar  = breakout.iloc[0]["datetime"]
            entry_price   = float(breakout.iloc[0]["close"])
            if entry_price <= 0:
                continue

            # Skip if still in prior trade
            if open_trade is not None and entry_ts_bar <= open_trade["exit_ts"]:
                continue

            trap_pos = f"LTF-{idx+1}"
            ts_str   = entry_ts_bar.strftime("%H:%M")
            print(f"  {trade_date} {direction}: {htf_min}m→{sub_min}m "
                  f"{sz_low:.0f}-{sz_high:.0f} → 1m breakout @ {ts_str} "
                  f"entry={entry_price:.1f}  [{trap_pos}]")

            exit_info = _simulate_exit(
                fut_1m      = fut_1m,
                entry_ts    = entry_ts_bar,
                entry_price = entry_price,
                direction   = direction,
                zone_low    = sz_low,
                zone_high   = sz_high,
                sl_buf      = sl_buf,
                trade_date  = trade_date,
                total_qty   = total_qty,
            )

            trade = {
                "date":       trade_date,
                "direction":  direction,
                "trap_pos":   trap_pos,
                "entry_ts":   str(entry_ts_bar)[:16],
                "entry":      round(entry_price, 2),
                "zone_low":   round(sz_low, 2),
                "zone_high":  round(sz_high, 2),
                "sl":         round(sz_low - sl_buf if direction == "LONG"
                                    else sz_high + sl_buf, 2),
                "t1":         round(sz_high if direction == "LONG" else sz_low, 2),
                "exit":       exit_info["exit"],
                "exit_ts":    str(exit_info["exit_ts"])[:16],
                "reason":     exit_info["reason"],
                "t1_hit":     exit_info["t1_hit"],
                "pnl_rs":     exit_info["pnl_rs"],
            }
            trades.append(trade)
            open_trade = exit_info
            added += 1

        if added:
            print(f"  {trade_date} {direction}: {htf_min}m {zl:.0f}-{zh:.0f} → "
                  f"{sub_min}m ×{added}/{len(ltf_in[:max_idx])}")

    return trades


# ── Full backtest ──────────────────────────────────────────────────────────────
def run_crude_backtest(
    token:   str,
    fut_key: str,
    start:   str,
    end:     str,
    lots:    int   = 2,
    sl_buf:  float = 20.0,
    max_ltf: int   = 5,
    htf_min: int   = HTF_MIN_DEFAULT,
    sub_min: int   = SUB_MIN_DEFAULT,
) -> dict:
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    trading_days = get_trading_days(start, end)
    if not trading_days:
        return {"ok": False, "error": "no trading days in range"}

    print(f"\n{'='*65}")
    print(f"  CrudeOil Backtest — Futures Mode — {htf_min}m→{sub_min}m→1m")
    print(f"{'='*65}")
    print(f"  Date       : {trading_days[0]} → {trading_days[-1]}  ({len(trading_days)} days)")
    print(f"  Lots       : {lots}  (qty/trade = {lots * CRUDE_LOT} barrels)")
    print(f"  SL Buffer  : {sl_buf} Rs  (intrabar — exits AT sl price)")
    print(f"  Max LTF    : {max_ltf}  (sub-zones LTF-{max_ltf + 1}+ filtered out)")
    print(f"  HTF zones  : {htf_min}m parent  →  {sub_min}m sub-zones  →  1m breakout")
    print(f"  Futures key: {fut_key}")
    print(f"  P&L basis  : FUTURES points × {CRUDE_LOT} × lots")
    print(f"{'='*65}\n")

    print(f"Fetching MCX futures bars {start} to {end}...")
    fut_all = _fetch_1m(fut_key, start, end)
    if fut_all.empty:
        return {"ok": False, "error": "no futures bars — check fut_key and token"}
    fut_all = _mkt_hours(fut_all)
    print(f"  {len(fut_all)} futures bars loaded\n")

    zone_cache: dict = {}
    all_trades: list[dict] = []

    for trade_date in trading_days:
        td   = date.fromisoformat(trade_date)
        fut_today = fut_all[fut_all["datetime"].dt.date == td].copy()
        if len(fut_today) < 10:
            print(f"  {trade_date}: insufficient futures bars ({len(fut_today)}) — skip")
            continue

        atm = int(round(float(fut_today.iloc[0]["open"]) / CRUDE_STEP) * CRUDE_STEP)
        open_price = float(fut_today.iloc[0]["open"])
        print(f"  {trade_date}  open={open_price:.0f}  ATM≈{atm}")

        for direction in ("LONG", "SHORT"):
            day_trades = _run_direction(
                trade_date = trade_date,
                direction  = direction,
                fut_1m     = fut_today,
                sl_buf     = sl_buf,
                lots       = lots,
                max_ltf    = max_ltf,
                htf_min    = htf_min,
                sub_min    = sub_min,
                zcache     = zone_cache,
            )
            all_trades.extend(day_trades)

    return {"ok": True, "trades": all_trades}


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="CrudeOil futures backtest — pure intraday cascade HTF→sub→1m",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--token",   required=True, help="Upstox access token")
    ap.add_argument("--start",   default="",    help="Start date YYYY-MM-DD")
    ap.add_argument("--end",     default="",    help="End date YYYY-MM-DD")
    ap.add_argument("--weeks",   type=int, default=4,
                    help="Rolling weeks if --start/--end not set (default: 4)")
    ap.add_argument("--lots",    type=int, default=2,
                    help="Number of lots (default: 2, 1 lot = 100 barrels)")
    ap.add_argument("--sl-buf",  type=float, default=20.0,
                    help="SL buffer in Rs beyond zone boundary (default: 20)")
    ap.add_argument("--max-ltf", type=int, default=5,
                    help="Max sub-zone index, 0=no limit (default: 5)")
    ap.add_argument("--htf",     type=int, default=HTF_MIN_DEFAULT,
                    help=f"Parent zone timeframe minutes (default: {HTF_MIN_DEFAULT})")
    ap.add_argument("--sub",     type=int, default=SUB_MIN_DEFAULT,
                    help=f"Sub-zone timeframe minutes (default: {SUB_MIN_DEFAULT})")
    ap.add_argument("--fut-key", default="MCX_FO|520702",
                    help="MCX futures instrument key (default: MCX_FO|520702 = CrudeOil Jul-26)")
    args = ap.parse_args()

    if args.start and args.end:
        start_dt, end_dt = args.start, args.end
    else:
        end_d   = date.today() - timedelta(days=1)
        start_d = end_d - timedelta(weeks=args.weeks)
        start_dt, end_dt = start_d.isoformat(), end_d.isoformat()

    result = run_crude_backtest(
        token   = args.token,
        fut_key = args.fut_key,
        start   = start_dt,
        end     = end_dt,
        lots    = args.lots,
        sl_buf  = args.sl_buf,
        max_ltf = args.max_ltf,
        htf_min = args.htf,
        sub_min = args.sub,
    )

    if not result["ok"]:
        print(f"\nERROR: {result['error']}")
        sys.exit(1)

    trades = result.get("trades", [])
    if not trades:
        print("\nNo trades found in period.")
        sys.exit(0)

    wins   = [t for t in trades if t["pnl_rs"] > 0]
    losses = [t for t in trades if t["pnl_rs"] <= 0]
    gw     = sum(t["pnl_rs"] for t in wins)
    gl     = abs(sum(t["pnl_rs"] for t in losses))
    total  = gw - gl
    pf     = round(gw / gl, 2) if gl > 0 else (99.0 if gw > 0 else 0.0)
    wr     = round(100 * len(wins) / len(trades), 1) if trades else 0

    print(f"\n{'─'*90}")
    print(f"CrudeOil Futures  {start_dt} to {end_dt}  "
          f"Trades={len(trades)}  Win={wr}%  "
          f"Rs {total:+,}  PF={pf}")
    print(f"{'─'*90}")
    print(f"  {'Date':<12} {'Dir':<6} {'LTF':<7} {'Time':<6}  "
          f"{'Entry':>8} {'Zone':>12} {'T1':>8} {'Exit':>8} {'Reason':<12} {'P&L':>10}")
    print(f"  {'─'*87}")
    for t in trades:
        zone_str = f"{t['zone_low']:.0f}-{t['zone_high']:.0f}"
        ts = t["entry_ts"][11:16] if len(t["entry_ts"]) > 10 else t["entry_ts"]
        win_mark = "✓" if t["pnl_rs"] > 0 else "✗"
        print(f"  {t['date']:<12} {t['direction']:<6} {t['trap_pos']:<7} {ts:<6}  "
              f"{t['entry']:>8.1f} {zone_str:>12} {t['t1']:>8.1f} {t['exit']:>8.1f} "
              f"{t['reason']:<12} ₹{t['pnl_rs']:>+9,}  {win_mark}")

    print(f"\n{'='*65}")
    print(f"  RESULTS — {len(trades)} trades")
    print(f"{'='*65}")
    print(f"  Win Rate      : {len(wins)}/{len(trades)}  ({wr}%)")
    print(f"  Total P&L     : ₹{total:+,}")
    print(f"  Profit Factor : {pf}")
    if wins:   print(f"  Avg Win       : ₹{round(gw/len(wins)):+,}")
    if losses: print(f"  Avg Loss      : ₹{-round(gl/len(losses)):,}")
    print(f"  Gross Win     : ₹{gw:+,}")
    print(f"  Gross Loss    : ₹{-gl:,}")

    # Per-direction summary
    for d in ("LONG", "SHORT"):
        sub  = [t for t in trades if t["direction"] == d]
        sw   = [t for t in sub if t["pnl_rs"] > 0]
        if sub:
            sp = sum(t["pnl_rs"] for t in sub)
            print(f"\n  {d:5s}: {len(sub)} trades  {len(sw)}/{len(sub)} wins  "
                  f"P&L ₹{sp:+,}")
    print(f"{'='*65}\n")
