"""
BTC Futures Trap Scanner Backtest — Delta Exchange India
=========================================================
Fetches up to 1 year of 1m BTCUSD perpetual futures candles from the public
Delta Exchange API (no authentication required for market data), runs the
trap scanner zone detection on HTF bars, simulates LONG/SHORT futures entry
and exit on 1m bars, and sweeps parameters for optimisation.

Usage (standalone):
    python scripts/btc_backtest.py

API:
    run_btc_backtest(days_back, htf_min, sub_min, sl_buf, lots, ...)
    run_btc_optimize(days_back, lots, ...)
"""
from __future__ import annotations

import os
import sys
import time
import json
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

# ── ensure project root is on path when running standalone ──────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner  # noqa: E402

# ── Delta Exchange constants ─────────────────────────────────────────────────
DELTA_BASE    = "https://api.india.delta.exchange"
SYMBOL        = "BTCUSD"
CONTRACT_SIZE = 0.001          # BTC per lot
CACHE_FILE    = os.path.join(_ROOT, "data", "btc_1m_cache.parquet")
MAX_PER_REQ   = 4000           # Delta returns at most this many candles per call


# ─────────────────────────────────────────────────────────────────────────────
# Data layer
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_page(start_ts: int, end_ts: int) -> list:
    r = requests.get(
        DELTA_BASE + "/v2/history/candles",
        params={"symbol": SYMBOL, "resolution": "1m",
                "start": start_ts, "end": end_ts},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("result", [])


def _fetch_all_candles(start_date: date, end_date: date) -> pd.DataFrame:
    """Page backwards through Delta API to collect all 1m candles in range."""
    end_ts   = int(datetime(end_date.year,   end_date.month,   end_date.day,
                            23, 59, 59, tzinfo=timezone.utc).timestamp())
    start_ts = int(datetime(start_date.year, start_date.month, start_date.day,
                            0,  0,  0,  tzinfo=timezone.utc).timestamp())

    all_candles: list = []
    current_end = end_ts
    page = 0

    print(f"[BTC] Fetching 1m candles {start_date} → {end_date} ...", flush=True)
    while current_end > start_ts:
        candles = _fetch_page(start_ts, current_end)
        if not candles:
            break
        all_candles.extend(candles)
        oldest = min(c["time"] for c in candles)
        if oldest <= start_ts:
            break
        current_end = oldest - 60
        page += 1
        if page % 10 == 0:
            print(f"  ... {len(all_candles):,} bars fetched "
                  f"(oldest {datetime.utcfromtimestamp(oldest).date()})", flush=True)
        time.sleep(0.2)

    if not all_candles:
        return pd.DataFrame()

    df = pd.DataFrame(all_candles)
    df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = (df.drop_duplicates("time")
            .sort_values("time")
            .reset_index(drop=True))
    df = df[(df["time"] >= start_ts) & (df["time"] <= end_ts)].reset_index(drop=True)
    print(f"[BTC] {len(df):,} candles fetched", flush=True)
    return df


def _load_btc_1m(days_back: int = 365) -> pd.DataFrame:
    """Return 1m BTCUSD bars, using a local parquet cache when fresh enough."""
    extra   = 5  # extra warm-up days beyond the backtest window
    end_d   = date.today()
    start_d = end_d - timedelta(days=days_back + extra)

    if os.path.exists(CACHE_FILE):
        try:
            cached    = pd.read_parquet(CACHE_FILE)
            cache_min = pd.to_datetime(cached["time"].min(), unit="s").date()
            cache_max = pd.to_datetime(cached["time"].max(), unit="s").date()
            if cache_min <= start_d and cache_max >= end_d - timedelta(days=1):
                print(f"[BTC] Using cache: {cache_min} → {cache_max} ({len(cached):,} bars)",
                      flush=True)
                if cached["datetime"].dt.tz is None:
                    cached["datetime"] = cached["datetime"].dt.tz_localize("UTC")
                return cached
        except Exception as exc:
            print(f"[BTC] Cache read failed ({exc}), re-fetching …", flush=True)

    df = _fetch_all_candles(start_d, end_d)
    if not df.empty:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        df.to_parquet(CACHE_FILE, index=False)
        print(f"[BTC] Cache saved: {CACHE_FILE}", flush=True)
    return df


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample UTC-aware 1m OHLCV to any higher timeframe."""
    if df.empty or len(df) < 2:
        return pd.DataFrame()
    df2 = df.set_index("datetime")
    rule = f"{minutes}min"
    agg  = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    out  = df2.resample(rule, closed="left", label="left")[list(agg)].agg(agg)
    out  = out.dropna(subset=["open"]).copy()
    out["datetime"] = out.index
    return out.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# Trade simulation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _zone_trigger_price(e: dict) -> float:
    if "zone_trigger" in e:
        return float(e["zone_trigger"])
    zh, zl = float(e["zone_high"]), float(e["zone_low"])
    if e.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)   # upper third → SHORT entry
    return round(zl + (zh - zl) / 3, 2)        # lower third → LONG entry


def _init_sl(e: dict, sl_buf: float) -> float:
    if e.get("kind") == "BULL":
        return round(float(e["zone_high"]) + sl_buf, 2)   # SHORT: SL above zone
    return round(float(e["zone_low"]) - sl_buf, 2)         # LONG:  SL below zone


def _simulate_trade(
    entry_price: float,
    is_long: bool,
    init_sl: float,
    t1: float,
    df1m: pd.DataFrame,
    lots: int,
    sl_buf: float,
    profit_floor_pts: float,
    profit_cap_pts: float,
    force_close_ts: Optional[pd.Timestamp],
) -> Optional[dict]:
    """
    Simulate one BTC futures trade on 1m bars starting from df1m.

    All thresholds (sl_buf, profit_floor_pts, profit_cap_pts) are in BTC PRICE POINTS
    so they are lot-independent.  Actual USDT P&L = price_diff × CONTRACT_SIZE × lots.

    profit_floor_pts : BTC price movement in our favour before break-even SL kicks in.
                       e.g. 200 → if BTC moves $200 in our favour, lock SL at entry.
    profit_cap_pts   : BTC price movement at which we exit with profit.
                       e.g. 500 → exit when BTC is $500 above (LONG) / below (SHORT) entry.
    """
    if df1m.empty:
        return None

    size        = CONTRACT_SIZE * lots   # BTC — only used for final USDT P&L
    active_sl   = init_sl
    breakeven   = False
    entry_ts    = df1m["datetime"].iloc[0]

    for _, bar in df1m.iterrows():
        # Force-close at session boundary
        if force_close_ts is not None and bar["datetime"] >= force_close_ts:
            ep = bar["close"]
            pnl = (ep - entry_price if is_long else entry_price - ep) * size
            return {"exit_price": ep, "pnl_usdt": round(pnl, 4),
                    "exit_reason": "FORCE_CLOSE",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}

        cur = bar["close"]
        # Running P&L in PRICE POINTS (lot-independent)
        running_pts = (cur - entry_price) if is_long else (entry_price - cur)

        # Break-even gate: lock SL at entry once price moves profit_floor_pts in our favour
        if profit_floor_pts > 0 and not breakeven and running_pts >= profit_floor_pts:
            active_sl = entry_price
            breakeven = True

        # Profit-cap exit: price moved profit_cap_pts in our favour
        if profit_cap_pts > 0 and running_pts >= profit_cap_pts:
            pnl = running_pts * size
            return {"exit_price": cur, "pnl_usdt": round(pnl, 4),
                    "exit_reason": "TARGET",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}

        # Trail SL behind price
        if is_long:
            new_trail = bar["high"] - sl_buf
            if new_trail > active_sl:
                active_sl = new_trail
        else:
            new_trail = bar["low"] + sl_buf
            if new_trail < active_sl:
                active_sl = new_trail

        # SL hit
        if (is_long and bar["low"] <= active_sl) or (not is_long and bar["high"] >= active_sl):
            ep  = active_sl
            pnl = (ep - entry_price if is_long else entry_price - ep) * size
            return {"exit_price": ep, "pnl_usdt": round(pnl, 4),
                    "exit_reason": "SL",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}

        # T1 hit
        if (is_long and bar["high"] >= t1) or (not is_long and bar["low"] <= t1):
            ep  = t1
            pnl = (ep - entry_price if is_long else entry_price - ep) * size
            return {"exit_price": ep, "pnl_usdt": round(pnl, 4),
                    "exit_reason": "T1",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}

    # Ran out of bars without exit
    ep  = df1m["close"].iloc[-1]
    pnl = (ep - entry_price if is_long else entry_price - ep) * size
    return {"exit_price": ep, "pnl_usdt": round(pnl, 4),
            "exit_reason": "EOD",
            "bars_held": len(df1m)}


# ─────────────────────────────────────────────────────────────────────────────
# Core day runner
# ─────────────────────────────────────────────────────────────────────────────

def _to_naive(ts) -> Optional[pd.Timestamp]:
    """Convert any timestamp to timezone-naive pandas Timestamp."""
    if ts is None:
        return None
    t = pd.Timestamp(ts)
    return t.tz_localize(None) if t.tzinfo else t


def _run_btc_day(
    day_str: str,
    df_day: pd.DataFrame,
    df_lookback: pd.DataFrame,
    htf_min: int,
    sub_min: int,
    sl_buf: float,
    lots: int,
    profit_floor_pts: float,
    profit_cap_pts: float,
    verbose: bool = False,
) -> list:
    """
    Run trap scanner on one UTC calendar day.

    Strategy:
      1. Build HTF bars from lookback window → scan_htf_spot (BEAR+BULL zones).
         If no CLOSED zones in lookback, fall back to today's own HTF bars (cascade).
      2. For each CLOSED HTF zone find a confirming sub-zone (sub_min TF) inside it.
      3. On 1m bars after the sub-zone trigger: enter LONG (BEAR zone) or SHORT (BULL zone).
      4. Exit on TSL / profit target / force-close at UTC day end.
    """
    trades: list = []

    # ── Step 1: HTF zones from lookback ──────────────────────────────────────
    df_combined = pd.concat([df_lookback, df_day], ignore_index=True) if not df_lookback.empty else df_day.copy()
    htf_bars    = _resample(df_combined, htf_min)

    _, htf_entries = scanner.scan_htf_spot(htf_bars) if len(htf_bars) >= 3 else (None, [])
    htf_zones      = [e for e in (htf_entries or []) if e.get("status") == "CLOSED"]

    if not htf_zones:
        # Cascade: use today's own HTF bars
        htf_today = _resample(df_day, htf_min)
        _, today_ents = scanner.scan_htf_spot(htf_today) if len(htf_today) >= 3 else (None, [])
        htf_zones  = [e for e in (today_ents or []) if e.get("status") == "CLOSED"]

    if not htf_zones:
        return trades

    # ── Step 2: sub-zone scan on today's bars ────────────────────────────────
    sub_bars = _resample(df_day, sub_min)
    _, sub_ents = scanner.scan_htf_spot(sub_bars) if len(sub_bars) >= 3 else (None, [])
    sub_zones   = [e for e in (sub_ents or []) if e.get("status") == "CLOSED"]

    # Normalise datetime column to timezone-naive for comparison
    df_day_naive = df_day.copy()
    df_day_naive["dt_naive"] = df_day_naive["datetime"].dt.tz_localize(None) \
        if df_day_naive["datetime"].dt.tz is not None else df_day_naive["datetime"]

    force_ts_naive = df_day_naive["dt_naive"].iloc[-1] if not df_day_naive.empty else None

    open_pos = False   # one position per day

    for htf_z in htf_zones:
        if open_pos:
            break

        zh      = float(htf_z["zone_high"])
        zl      = float(htf_z["zone_low"])
        kind    = htf_z.get("kind", "BEAR")
        is_long = (kind == "BEAR")

        # Find confirming sub-zones inside the HTF zone (same direction)
        sub_in  = [s for s in sub_zones
                   if s.get("kind") == kind
                   and float(s.get("zone_high", 0)) <= zh * 1.02
                   and float(s.get("zone_low",  0)) >= zl * 0.98]
        if not sub_in:
            continue

        # Use most recently closed sub-zone
        def _ts_key(s):
            t = s.get("closed_on") or s.get("trapped_on")
            if t is None:
                return pd.Timestamp.min
            ts = pd.Timestamp(t)
            return ts.tz_localize(None) if ts.tzinfo else ts

        best_sub    = max(sub_in, key=_ts_key)
        trigger     = _zone_trigger_price(best_sub)
        sl          = _init_sl(best_sub, sl_buf)
        t1          = zh if is_long else zl   # zone far edge as T1

        # 1m bars after sub-zone closed_on
        close_ts_naive = _to_naive(best_sub.get("closed_on"))
        if close_ts_naive is None:
            df_after = df_day_naive.copy()
        else:
            df_after = df_day_naive[df_day_naive["dt_naive"] >= close_ts_naive].copy()

        if df_after.empty:
            continue

        # Find first 1m bar where trigger is hit
        if is_long:
            hit = df_after[df_after["low"] <= trigger]
        else:
            hit = df_after[df_after["high"] >= trigger]

        if hit.empty:
            continue

        entry_bar   = hit.index[0]
        entry_ts    = df_after.loc[entry_bar, "dt_naive"]
        entry_price = trigger
        df_for_exit = df_after.loc[entry_bar + 1:]   # bars after entry bar

        if df_for_exit.empty:
            continue

        # Rename dt_naive → datetime for _simulate_trade
        df_exit = df_for_exit.rename(columns={"dt_naive": "datetime"}).copy()

        result = _simulate_trade(
            entry_price       = entry_price,
            is_long           = is_long,
            init_sl           = sl,
            t1                = t1,
            df1m              = df_exit,
            lots              = lots,
            sl_buf            = sl_buf,
            profit_floor_pts  = profit_floor_pts,
            profit_cap_pts    = profit_cap_pts,
            force_close_ts    = force_ts_naive,
        )

        if result:
            result.update({
                "date"        : day_str,
                "kind"        : kind,
                "direction"   : "LONG" if is_long else "SHORT",
                "htf_zone"    : f"{zl:.0f}-{zh:.0f}",
                "entry_price" : entry_price,
                "entry_ts"    : str(entry_ts),
            })
            trades.append(result)
            open_pos = True
            if verbose:
                print(f"  {day_str} {result['direction']:5s} "
                      f"zone={result['htf_zone']} "
                      f"entry={entry_price:.0f} exit={result['exit_price']:.0f} "
                      f"pnl=${result['pnl_usdt']:.2f} [{result['exit_reason']}]",
                      flush=True)

    return trades


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

def _summarize(trades: list, params: dict) -> dict:
    wins   = [t for t in trades if t["pnl_usdt"] > 0]
    losses = [t for t in trades if t["pnl_usdt"] <= 0]
    gp     = sum(t["pnl_usdt"] for t in wins)
    gl     = abs(sum(t["pnl_usdt"] for t in losses))
    pf     = round(gp / gl, 3) if gl > 0 else (float("inf") if gp > 0 else 0)

    summary = {
        "total"          : len(trades),
        "wins"           : len(wins),
        "losses"         : len(losses),
        "win_rate_pct"   : round(len(wins) / len(trades) * 100, 1) if trades else 0,
        "gross_profit"   : round(gp, 4),
        "gross_loss"     : round(gl, 4),
        "profit_factor"  : pf,
        "net_pnl_usdt"   : round(gp - gl, 4),
        "avg_win_usdt"   : round(gp / len(wins),   4) if wins   else 0,
        "avg_loss_usdt"  : round(gl / len(losses), 4) if losses else 0,
        "exit_breakdown" : {},
    }
    for t in trades:
        reason = t.get("exit_reason", "?")
        summary["exit_breakdown"][reason] = summary["exit_breakdown"].get(reason, 0) + 1

    summary.update(params)
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def run_btc_backtest(
    days_back: int        = 365,
    htf_min: int          = 120,
    sub_min: int          = 15,
    sl_buf: float         = 100.0,
    lots: int             = 100,
    profit_floor_pts: float = 0.0,
    profit_cap_pts: float   = 0.0,
    lookback_days: int    = 3,
    verbose: bool         = True,
) -> dict:
    """
    Full BTC futures backtest.

    sl_buf, profit_floor_pts, profit_cap_pts are all in BTC PRICE POINTS.
    Actual USDT P&L = price_pts × CONTRACT_SIZE × lots.

    profit_floor_pts: BTC must move this many $ in our favour before break-even SL locks in.
    profit_cap_pts  : exit immediately when BTC moves this many $ in our favour.
    """
    df_all = _load_btc_1m(days_back + lookback_days + 2)
    if df_all.empty:
        return {"ok": False, "error": "No BTC data available"}
    if df_all["datetime"].dt.tz is None:
        df_all["datetime"] = df_all["datetime"].dt.tz_localize("UTC")

    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    all_dates  = [start_date + timedelta(days=i) for i in range(days_back)]

    all_trades: list = []
    for d in all_dates:
        d_str  = d.isoformat()
        d_s    = pd.Timestamp(f"{d_str}T00:00:00", tz="UTC")
        d_e    = pd.Timestamp(f"{d_str}T23:59:59", tz="UTC")
        lb_s   = d_s - pd.Timedelta(days=lookback_days)

        df_day = df_all[(df_all["datetime"] >= d_s) & (df_all["datetime"] <= d_e)].copy()
        df_lb  = df_all[(df_all["datetime"] >= lb_s) & (df_all["datetime"] < d_s)].copy()

        if len(df_day) < 60:
            continue

        day_trades = _run_btc_day(
            day_str          = d_str,
            df_day           = df_day,
            df_lookback      = df_lb,
            htf_min          = htf_min,
            sub_min          = sub_min,
            sl_buf           = sl_buf,
            lots             = lots,
            profit_floor_pts = profit_floor_pts,
            profit_cap_pts   = profit_cap_pts,
            verbose          = verbose,
        )
        all_trades.extend(day_trades)

    params = {"htf_min": htf_min, "sub_min": sub_min, "sl_buf": sl_buf, "lots": lots,
              "profit_floor_pts": profit_floor_pts, "profit_cap_pts": profit_cap_pts}
    return {"ok": True, "trades": all_trades,
            "summary": _summarize(all_trades, params)}


def run_btc_optimize(
    days_back: int     = 365,
    lots: int          = 100,
    lookback_days: int = 3,
) -> dict:
    """
    Sweep HTF / sub_min / SL / profit_floor / profit_cap parameters.
    Returns {"ok": True, "results": [...sorted by PF desc...], "top10": [...]}
    """
    df_all = _load_btc_1m(days_back + lookback_days + 2)
    if df_all.empty:
        return {"ok": False, "error": "No BTC data available"}
    if df_all["datetime"].dt.tz is None:
        df_all["datetime"] = df_all["datetime"].dt.tz_localize("UTC")

    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)
    all_dates  = [start_date + timedelta(days=i) for i in range(days_back)]

    # Pre-slice data by day (shared across all combos → read once, reuse)
    day_slices: dict = {}
    for d in all_dates:
        d_str = d.isoformat()
        d_s   = pd.Timestamp(f"{d_str}T00:00:00", tz="UTC")
        d_e   = pd.Timestamp(f"{d_str}T23:59:59", tz="UTC")
        lb_s  = d_s - pd.Timedelta(days=lookback_days)
        df_day = df_all[(df_all["datetime"] >= d_s) & (df_all["datetime"] <= d_e)].copy()
        df_lb  = df_all[(df_all["datetime"] >= lb_s) & (df_all["datetime"] < d_s)].copy()
        if len(df_day) >= 60:
            day_slices[d_str] = (df_day, df_lb)

    # ── Optimisation grid (all values in BTC PRICE POINTS) ───────────────────
    htf_grid   = [60, 120, 240]           # 1h / 2h / 4h
    sub_grid   = [5, 15, 30]              # LTF confirmation TF
    sl_grid    = [50, 100, 200, 500]      # SL buffer: price pts beyond zone
    # Break-even floor: BTC must move this many $ in our favour before BE kicks in.
    # e.g. 200 → lock SL at entry once BTC moves $200 in our direction
    floor_grid = [0, 100, 200, 500]
    # Profit cap: exit when BTC moves this many $ in our favour ($0 = ride TSL only)
    # e.g. 500 → exit when BTC is $500 above entry (LONG) / below entry (SHORT)
    cap_grid   = [0, 300, 500, 1000]

    valid_combos = [(h, s, sl, f, c)
                    for h  in htf_grid
                    for s  in sub_grid   if s < h
                    for sl in sl_grid
                    for f  in floor_grid
                    for c  in cap_grid]
    total = len(valid_combos)
    print(f"[BTC Optimize] {total} combos × {len(day_slices)} days", flush=True)

    results: list = []
    for idx, (htf_min, sub_min, sl_buf, floor_pts, cap_pts) in enumerate(valid_combos):
        all_trades: list = []
        for d_str, (df_day, df_lb) in day_slices.items():
            all_trades.extend(_run_btc_day(
                day_str          = d_str,
                df_day           = df_day,
                df_lookback      = df_lb,
                htf_min          = htf_min,
                sub_min          = sub_min,
                sl_buf           = sl_buf,
                lots             = lots,
                profit_floor_pts = floor_pts,
                profit_cap_pts   = cap_pts,
                verbose           = False,
            ))

        params = {"htf_min": htf_min, "sub_min": sub_min, "sl_buf": sl_buf,
                  "profit_floor_pts": floor_pts, "profit_cap_pts": cap_pts, "lots": lots}
        results.append(_summarize(all_trades, params))

        if (idx + 1) % 50 == 0:
            print(f"  {idx+1}/{total} combos done", flush=True)

    def _pf_sort(r):
        pf = r.get("profit_factor", 0)
        return (pf if pf != float("inf") else 9999) if r.get("total", 0) >= 5 else -1

    results.sort(key=_pf_sort, reverse=True)
    return {"ok": True, "results": results, "top10": results[:10]}


# ─────────────────────────────────────────────────────────────────────────────
# Standalone test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== BTC Backtest Quick-Test (30 days, HTF=120m, sub=15m, SL=$100) ===")
    result = run_btc_backtest(
        days_back=30, htf_min=120, sub_min=15,
        sl_buf=100, lots=100, verbose=True,
    )
    print("\n--- SUMMARY ---")
    print(json.dumps(result["summary"], indent=2))
