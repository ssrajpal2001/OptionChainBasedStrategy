"""
backtest_engine.py — CrudeOil futures backtest engine (Phase 1: futures only)

Four-combo analysis:
  GAP   + HTF_ZONE   — prev-day gap + 1-hour pre-day zone exists
  GAP   + NO_ZONE    — prev-day gap, no 1-hour zone → intraday 30-min cascade
  NO_GAP + HTF_ZONE  — no gap, 1-hour zone exists
  NO_GAP + NO_ZONE   — no gap, no 1-hour zone → intraday 30-min cascade

Entry:  futures price enters TRAPPED zone (zone_low ≤ fut ≤ zone_high)
SL:     zone_low − sl_buf (BEAR) / zone_high + sl_buf (BULL)
T1:     zone target (bears' SL level)
EOD:    square off at 23:00 MCX
"""
from __future__ import annotations
import sys, os, time
from datetime import date, timedelta, datetime
from typing import Optional
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

CRUDE_STEP   = 100
CRUDE_LOT    = 100
GAP_PCT      = 0.003   # 0.3%
SL_BUF       = 20.0
SQ_OFF       = "23:00"
MKT_OPEN     = "09:00"
MKT_CLOSE    = "23:30"
ENTRY_OPEN   = "09:30"

_HEADERS: dict = {}


# ── API helpers ───────────────────────────────────────────────────────────────
def _get(url: str, retries: int = 3) -> dict:
    for _ in range(retries):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
            if r.status_code == 429:
                time.sleep(2); continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}


def fetch_1m(key: str, dt: str) -> pd.DataFrame:
    enc  = key.replace("|", "%7C")
    data = _get(f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}")
    cands = data.get("data", {}).get("candles", [])
    if not cands:
        return pd.DataFrame()
    df = pd.DataFrame(cands, columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(None)
    df = df.sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.strftime("%H:%M") >= MKT_OPEN) &
            (df["datetime"].dt.strftime("%H:%M") <= MKT_CLOSE)]
    return df


def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    r = df.set_index("datetime").resample(
        f"{minutes}min", label="right", closed="right"
    ).agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
    return r.dropna().reset_index()


def detect_gap(spot_open: float, prev_close: float, threshold: float) -> dict:
    if prev_close <= 0 or spot_open <= 0:
        return {"gap": False, "direction": "NONE", "pct": 0.0}
    pct = (spot_open - prev_close) / prev_close
    if abs(pct) >= threshold:
        return {"gap": True, "direction": "UP" if pct > 0 else "DOWN",
                "pct": round(pct * 100, 2)}
    return {"gap": False, "direction": "NONE", "pct": round(pct * 100, 2)}


def get_trading_days(n: int) -> list[str]:
    days = []
    d = date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.isoformat())
    return list(reversed(days))


def _combo_label(has_gap: bool, has_zone: bool) -> str:
    g = "GAP" if has_gap else "NO_GAP"
    z = "HTF_ZONE" if has_zone else "NO_ZONE"
    return f"{g}+{z}"


# ── Zone helpers ──────────────────────────────────────────────────────────────
def _get_prev_day_htf_zones(prev_df: pd.DataFrame, htf_min: int) -> list[dict]:
    """Scan previous day's bars at htf_min resolution → return TRAPPED zones."""
    if prev_df.empty:
        return []
    htf = resample(prev_df, htf_min)
    if len(htf) < 2:
        return []
    _, zones = scanner.scan_htf_spot(htf)
    return [z for z in zones if z["status"] == "TRAPPED"]


def _get_combined_zones_at(prev_df: pd.DataFrame, today_df: pd.DataFrame,
                            bar_ts, htf_min: int) -> list[dict]:
    """
    Scan BOTH previous day bars + today's bars up to bar_ts combined.
    This ensures prev-day zones AND intraday zones forming during the day
    are both visible — giving a continuous picture of trapped zones.
    """
    today_date = bar_ts.date()
    today_hist = today_df[(today_df["datetime"].dt.date == today_date) &
                          (today_df["datetime"] <= bar_ts)]

    combined = pd.concat([prev_df, today_hist], ignore_index=True)
    combined = combined.sort_values("datetime").reset_index(drop=True)
    if combined.empty:
        return []
    htf = resample(combined, htf_min)
    if len(htf) < 2:
        return []
    _, zones = scanner.scan_htf_spot(htf)
    return [z for z in zones if z["status"] == "TRAPPED"]


def _zones_for_direction(zones: list[dict], direction: str) -> list[dict]:
    """Filter zones by kind: BEAR=CE(bullish), BULL=PE(bearish)."""
    return [z for z in zones if z.get("kind") == direction]


def _price_in_zone(price: float, zone: dict) -> bool:
    return zone.get("zone_low", 0) <= price <= zone.get("zone_high", 0)


def _ts_fmt(ts) -> str:
    return ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)[:16]


# ── Per-day backtest ──────────────────────────────────────────────────────────
def _run_day(
    trade_date: str,
    today_df: pd.DataFrame,
    lookback_df: pd.DataFrame,   # multi-day historical bars (prev week + current week)
    htf_min_zone: int,
    htf_min_cascade: int,
    sl_buf: float,
    gap_threshold: float,
    combo_filter: str,
    lot_size: int,
) -> list[dict]:
    """Run one day's backtest. Returns list of trade records."""
    trades: list[dict] = []
    if today_df.empty:
        return trades

    today_open = float(today_df.iloc[0]["open"])
    # prev_close = last bar of the most-recent previous day in lookback_df
    prev_date  = today_df.iloc[0]["datetime"].date()
    prev_bars  = lookback_df[lookback_df["datetime"].dt.date < prev_date]
    prev_close = float(prev_bars.iloc[-1]["close"]) if not prev_bars.empty else 0.0
    gap_info   = detect_gap(today_open, prev_close, gap_threshold)
    has_gap    = gap_info["gap"]

    # HTF zones from ALL historical bars (prev week + earlier this week)
    # This captures zones from multiple days back, not just yesterday
    htf_zones_pre  = _get_prev_day_htf_zones(lookback_df, htf_min_zone)
    has_htf_zone   = len(htf_zones_pre) > 0

    combo = _combo_label(has_gap, has_htf_zone)

    # Apply combo filter
    if combo_filter != "all":
        wanted = combo_filter.upper().replace("-", "_")
        if combo.upper() != wanted:
            return trades

    sq_time    = pd.Timestamp(f"{trade_date} {SQ_OFF}").tz_localize(None)
    entry_open = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}").tz_localize(None)

    def _run_direction(kind: str):
        """kind = BEAR (→ CE, bullish) or BULL (→ PE, bearish)."""
        nonlocal trades
        in_trade  = None
        notified  = set()

        scan_min = htf_min_cascade
        htf_bars = resample(today_df, scan_min)

        for _, row in htf_bars.iterrows():
            bar_ts  = row["datetime"]
            cur_fut = float(row["close"])

            if bar_ts < entry_open:
                continue
            if bar_ts >= sq_time:
                if in_trade:
                    ob = today_df[today_df["datetime"] <= bar_ts]
                    ep = float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry"]
                    _record_exit(in_trade, ep, bar_ts, "EOD", trades, lot_size, combo)
                    in_trade = None
                break

            # ── Exit check ─────────────────────────────────────────────────
            if in_trade:
                fwd = today_df[(today_df["datetime"] > in_trade["entry_ts"]) &
                               (today_df["datetime"] <= bar_ts)]
                result = None
                for _, fb in fwd.iterrows():
                    flo, fhi, fts = float(fb["low"]), float(fb["high"]), fb["datetime"]
                    if fts >= sq_time:
                        ob = today_df[today_df["datetime"] <= fts]
                        ep = float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry"]
                        result = ("EOD", ep, fts); break
                    if not in_trade["t1_hit"]:
                        t1h = (fhi >= in_trade["t1"]) if kind == "BEAR" \
                              else (flo <= in_trade["t1"])
                        if t1h:
                            in_trade["t1_hit"] = True
                            ob = today_df[today_df["datetime"] <= fts]
                            ep = float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry"]
                            result = ("T1", ep, fts); break
                    slh = (flo <= in_trade["sl"]) if kind == "BEAR" \
                          else (fhi >= in_trade["sl"])
                    if slh:
                        ob = today_df[today_df["datetime"] <= fts]
                        ep = float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry"]
                        result = ("SL", ep, fts); break
                if result:
                    _record_exit(in_trade, result[1], result[2], result[0], trades, lot_size, combo)
                    in_trade = None
                continue

            # ── Zone selection ─────────────────────────────────────────────
            # Always combine: all historical bars + today up to this bar
            # This gives zones from prev week, yesterday, AND intraday so far
            live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_zone)
            # Cascade fallback: also check shorter TF if no HTF zone
            if not live_zones:
                live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_cascade)
            kind_zones = _zones_for_direction(live_zones, kind)
            active = [z for z in kind_zones if _price_in_zone(cur_fut, z)]
            zone_src = "HTF" if has_htf_zone else "CASCADE"

            if not active:
                continue

            zone = min(active, key=lambda z: abs(cur_fut - z.get("zone_low", cur_fut)))
            uid  = f"{kind}_{zone.get('zone_low',0):.0f}_{zone.get('zone_high',0):.0f}"
            if uid in notified:
                continue
            notified.add(uid)

            ob = today_df[today_df["datetime"] <= bar_ts]
            if ob.empty:
                continue
            entry_p = float(ob.iloc[-1]["close"])
            if entry_p <= 0:
                continue

            sl_p  = round(zone["zone_low"] - sl_buf, 1) if kind == "BEAR" \
                    else round(zone["zone_high"] + sl_buf, 1)
            t1_p  = zone.get("sl", zone["zone_high"] + 50) if kind == "BEAR" \
                    else zone.get("sl", zone["zone_low"] - 50)

            in_trade = {
                "entry_ts":  bar_ts,
                "entry":     entry_p,
                "sl":        sl_p,
                "t1":        t1_p,
                "t1_hit":    False,
                "kind":      kind,
                "direction": "CE" if kind == "BEAR" else "PE",
                "zone":      f"{zone.get('zone_low',0):.0f}→{zone.get('zone_high',0):.0f}",
                "zone_src":  zone_src,
                "date":      trade_date,
                "gap":       f"{gap_info['direction']} {gap_info['pct']:+.2f}%" if has_gap else "none",
            }

        if in_trade:
            ob = today_df[today_df["datetime"] <= sq_time]
            ep = float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry"]
            _record_exit(in_trade, ep, sq_time, "EOD", trades, lot_size, combo)

    _run_direction("BEAR")
    _run_direction("BULL")
    return trades


def _record_exit(trade: dict, exit_p: float, exit_ts, reason: str,
                 out: list, lot_size: int, combo: str):
    ep   = trade["entry"]
    kind = trade["kind"]
    # BEAR = bought futures (bullish): profit when price rises
    # BULL = sold futures (bearish): profit when price falls
    pnl_pts = (exit_p - ep) if kind == "BEAR" else (ep - exit_p)
    out.append({
        "date":      trade["date"],
        "entry_ts":  _ts_fmt(trade["entry_ts"]),
        "exit_ts":   _ts_fmt(exit_ts),
        "direction": trade["direction"],
        "kind":      kind,
        "zone":      trade["zone"],
        "zone_src":  trade["zone_src"],
        "gap":       trade["gap"],
        "entry":     round(ep, 1),
        "exit":      round(exit_p, 1),
        "sl":        round(trade["sl"], 1),
        "t1":        round(trade["t1"], 1),
        "t1_hit":    trade["t1_hit"],
        "reason":    reason,
        "pnl_pts":   round(pnl_pts, 1),
        "pnl_rs":    round(pnl_pts * lot_size, 0),
        "combo":     combo,
    })


# ── Main entry point ──────────────────────────────────────────────────────────
def run_crude_backtest(params: dict, token: str) -> dict:
    """
    Run CrudeOil futures backtest.
    params keys: days, lots, gap_threshold, htf_min_zone, htf_min_cascade,
                 sl_buf, combo, fut_key
    Returns: {"trades": [...], "summary": {...}, "by_combo": {...}}
    """
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    days         = int(params.get("days", 7))
    lots         = int(params.get("lots", 2))
    gap_thr      = float(params.get("gap_threshold", GAP_PCT))
    htf_zone     = int(params.get("htf_min_zone", 60))
    htf_cascade  = int(params.get("htf_min_cascade", 30))
    sl_buf       = float(params.get("sl_buf", SL_BUF))
    combo_filter = str(params.get("combo", "all")).lower()
    fut_key      = str(params.get("fut_key", "MCX_FO|520702"))
    lot_size     = CRUDE_LOT * lots

    LOOKBACK_DAYS = 10   # how many calendar-trading-days back to scan for zones

    trading_days = get_trading_days(days)
    all_trades: list[dict] = []
    log: list[str] = []

    # Pre-build list of lookback dates for each trade date (10 trading days back)
    def _lookback_dates(trade_dt: str, n: int) -> list[str]:
        result = []
        d = date.fromisoformat(trade_dt) - timedelta(days=1)
        while len(result) < n:
            if d.weekday() < 5:
                result.append(d.isoformat())
            d -= timedelta(days=1)
        return list(reversed(result))

    for i, trade_date in enumerate(trading_days):
        log.append(f"Fetching {trade_date}...")
        today_df = fetch_1m(fut_key, trade_date)
        time.sleep(0.3)

        if today_df.empty:
            log.append(f"  {trade_date}: no data — skip")
            continue

        # Fetch 10 trading days of lookback bars and concatenate
        lb_dates = _lookback_dates(trade_date, LOOKBACK_DAYS)
        lb_frames = []
        for lb_dt in lb_dates:
            df = fetch_1m(fut_key, lb_dt)
            time.sleep(0.2)
            if not df.empty:
                lb_frames.append(df)

        lookback_df = pd.concat(lb_frames, ignore_index=True).sort_values("datetime").reset_index(drop=True) \
                      if lb_frames else pd.DataFrame()

        day_trades = _run_day(
            trade_date, today_df, lookback_df,
            htf_zone, htf_cascade, sl_buf, gap_thr, combo_filter, lot_size
        )
        all_trades.extend(day_trades)
        day_pnl = sum(t["pnl_rs"] for t in day_trades)
        log.append(f"  {trade_date}: {len(day_trades)} trades  Rs{day_pnl:+,.0f}")

    # ── Summary ───────────────────────────────────────────────────────────────
    def _stats(trades: list[dict]) -> dict:
        if not trades:
            return {"count": 0, "wins": 0, "losses": 0, "win_pct": 0,
                    "total_rs": 0, "avg_win": 0, "avg_loss": 0}
        wins   = [t for t in trades if t["pnl_rs"] > 0]
        losses = [t for t in trades if t["pnl_rs"] <= 0]
        return {
            "count":    len(trades),
            "wins":     len(wins),
            "losses":   len(losses),
            "win_pct":  round(len(wins) / len(trades) * 100, 1),
            "total_rs": int(sum(t["pnl_rs"] for t in trades)),
            "avg_win":  int(sum(t["pnl_rs"] for t in wins) / len(wins)) if wins else 0,
            "avg_loss": int(sum(t["pnl_rs"] for t in losses) / len(losses)) if losses else 0,
        }

    combos = ["GAP+HTF_ZONE", "GAP+NO_ZONE", "NO_GAP+HTF_ZONE", "NO_GAP+NO_ZONE"]
    by_combo = {
        c: _stats([t for t in all_trades if t["combo"] == c])
        for c in combos
    }

    # Equity curve: cumulative P&L per trade in time order
    equity = []
    cum = 0
    for t in all_trades:
        cum += t["pnl_rs"]
        equity.append({"ts": f"{t['date']} {t['exit_ts']}", "cum": cum})

    return {
        "ok":      True,
        "params":  params,
        "period":  f"{trading_days[0]} → {trading_days[-1]}",
        "trades":  all_trades,
        "summary": _stats(all_trades),
        "by_combo": by_combo,
        "equity":  equity,
        "log":     log,
    }


if __name__ == "__main__":
    import argparse, sqlite3, json
    parser = argparse.ArgumentParser()
    parser.add_argument("--token")
    parser.add_argument("--db", default="data/clients.db")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--lots", type=int, default=2)
    parser.add_argument("--combo", default="all")
    args = parser.parse_args()

    token = args.token
    if not token:
        conn = sqlite3.connect(args.db)
        row  = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox'"
        ).fetchone()
        conn.close()
        token = row[0] if row else ""

    result = run_crude_backtest({
        "days": args.days, "lots": args.lots, "combo": args.combo,
        "gap_threshold": 0.003, "htf_min_zone": 60, "htf_min_cascade": 30,
        "sl_buf": 20.0, "fut_key": "MCX_FO|520702",
    }, token)

    print(f"\nPeriod: {result['period']}")
    for line in result["log"]:
        print(line)
    print(f"\nSUMMARY: {result['summary']}")
    print("\nBY COMBO:")
    for c, s in result["by_combo"].items():
        if s["count"]:
            print(f"  {c}: {s['count']} trades  W={s['wins']} L={s['losses']}  Rs{s['total_rs']:+,}")
    print("\nTRADES:")
    for t in result["trades"]:
        print(f"  {t['date']} {t['entry_ts']}→{t['exit_ts']}  {t['direction']}  "
              f"entry={t['entry']}  exit={t['exit']}  {t['reason']}  "
              f"Rs{t['pnl_rs']:+.0f}  [{t['combo']}]")
