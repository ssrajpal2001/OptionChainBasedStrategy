"""
BTC 3-Month Optimization — Delta Exchange India (public API, no credentials needed)
===================================================================================
Precomputes HTF zones and LTF sub-zones for each unique timeframe ONCE, then sweeps
all SL/floor/cap combinations over the precomputed zones. ~10-20x faster than
resampling on every combo.

Usage:
    python3 scripts/btc_optimize_3m.py
"""
from __future__ import annotations

import os, sys, time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner  # noqa: E402

DELTA_BASE    = "https://api.india.delta.exchange"
SYMBOL        = "BTCUSD"
CONTRACT_SIZE = 0.001
CACHE_FILE    = os.path.join(_ROOT, "data", "btc_1m_cache.parquet")
OUT_CSV       = os.path.join(_ROOT, "data", "btc_optimize_3m_results.csv")
DAYS_BACK     = 90
LOOKBACK      = 3
LOTS          = 1

HTF_GRID   = [60, 120, 180, 240, 360]
SUB_GRID   = [5, 15, 30, 60]
SL_GRID    = [50, 100, 200, 300]
FLOOR_GRID = [0, 100, 200]
CAP_GRID   = [0, 300, 500, 1000]


# ── Data fetch ────────────────────────────────────────────────────────────────

def _fetch_all_candles(start_date: date, end_date: date) -> pd.DataFrame:
    end_ts   = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    start_ts = int(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    all_candles: list = []
    current_end = end_ts
    page = 0
    print(f"[BTC] Fetching 1m candles {start_date} -> {end_date} ...", flush=True)
    while current_end > start_ts:
        r = requests.get(DELTA_BASE + "/v2/history/candles",
                         params={"symbol": SYMBOL, "resolution": "1m",
                                 "start": start_ts, "end": current_end}, timeout=30)
        r.raise_for_status()
        candles = r.json().get("result", [])
        if not candles:
            break
        all_candles.extend(candles)
        oldest = min(c["time"] for c in candles)
        if oldest <= start_ts:
            break
        current_end = oldest - 60
        page += 1
        if page % 10 == 0:
            print(f"  ... {len(all_candles):,} bars (oldest {datetime.fromtimestamp(oldest, tz=timezone.utc).date()})", flush=True)
        time.sleep(0.2)
    if not all_candles:
        return pd.DataFrame()
    df = pd.DataFrame(all_candles)
    df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)
    df = df[(df["time"] >= start_ts) & (df["time"] <= end_ts)].reset_index(drop=True)
    print(f"[BTC] {len(df):,} 1m bars", flush=True)
    return df


def _load_btc_1m() -> pd.DataFrame:
    end_d   = date.today()
    start_d = end_d - timedelta(days=DAYS_BACK + LOOKBACK + 2)
    if os.path.exists(CACHE_FILE):
        try:
            cached    = pd.read_parquet(CACHE_FILE)
            cache_min = pd.to_datetime(cached["time"].min(), unit="s").date()
            cache_max = pd.to_datetime(cached["time"].max(), unit="s").date()
            if cache_min <= start_d and cache_max >= end_d - timedelta(days=1):
                print(f"[BTC] Cache: {cache_min} -> {cache_max} ({len(cached):,} bars)", flush=True)
                if cached["datetime"].dt.tz is None:
                    cached["datetime"] = cached["datetime"].dt.tz_localize("UTC")
                return cached
        except Exception as exc:
            print(f"[BTC] Cache miss ({exc}), re-fetching ...", flush=True)
    df = _fetch_all_candles(start_d, end_d)
    if not df.empty:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        df.to_parquet(CACHE_FILE, index=False)
    return df


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty or len(df) < 2:
        return pd.DataFrame()
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    out = df.set_index("datetime").resample(f"{minutes}min", closed="left", label="left")[list(agg)].agg(agg)
    out = out.dropna(subset=["open"]).copy()
    out["datetime"] = out.index
    return out.reset_index(drop=True)


def _get_zones(htf_bars: pd.DataFrame) -> list:
    if len(htf_bars) < 3:
        return []
    _, ents = scanner.scan_htf_spot(htf_bars)
    return [e for e in (ents or []) if e.get("status") == "CLOSED"]


# ── Trade simulation ──────────────────────────────────────────────────────────

def _zone_trigger(e: dict) -> float:
    if "zone_trigger" in e:
        return float(e["zone_trigger"])
    zh, zl = float(e["zone_high"]), float(e["zone_low"])
    if e.get("kind") == "BULL":
        return round(zh - (zh - zl) / 3, 2)
    return round(zl + (zh - zl) / 3, 2)


def _init_sl(e: dict, sl_buf: float) -> float:
    if e.get("kind") == "BULL":
        return round(float(e["zone_high"]) + sl_buf, 2)
    return round(float(e["zone_low"]) - sl_buf, 2)


def _simulate(entry_price, is_long, init_sl, t1, df1m, sl_buf, floor_pts, cap_pts) -> Optional[dict]:
    if df1m.empty:
        return None
    size      = CONTRACT_SIZE * LOTS
    active_sl = init_sl
    breakeven = False
    entry_ts  = df1m["datetime"].iloc[0]
    for _, bar in df1m.iterrows():
        cur = bar["close"]
        run = (cur - entry_price) if is_long else (entry_price - cur)
        if floor_pts > 0 and not breakeven and run >= floor_pts:
            active_sl = entry_price
            breakeven = True
        if cap_pts > 0 and run >= cap_pts:
            return {"exit_price": cur, "pnl_usdt": round(run * size, 4), "exit_reason": "TARGET",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}
        new_trail = (bar["high"] - sl_buf) if is_long else (bar["low"] + sl_buf)
        if is_long and new_trail > active_sl:
            active_sl = new_trail
        elif not is_long and new_trail < active_sl:
            active_sl = new_trail
        if (is_long and bar["low"] <= active_sl) or (not is_long and bar["high"] >= active_sl):
            pnl = (active_sl - entry_price if is_long else entry_price - active_sl) * size
            return {"exit_price": active_sl, "pnl_usdt": round(pnl, 4), "exit_reason": "SL",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}
        if (is_long and bar["high"] >= t1) or (not is_long and bar["low"] <= t1):
            pnl = (t1 - entry_price if is_long else entry_price - t1) * size
            return {"exit_price": t1, "pnl_usdt": round(pnl, 4), "exit_reason": "T1",
                    "bars_held": int((bar["datetime"] - entry_ts).total_seconds() / 60)}
    ep  = df1m["close"].iloc[-1]
    pnl = (ep - entry_price if is_long else entry_price - ep) * size
    return {"exit_price": ep, "pnl_usdt": round(pnl, 4), "exit_reason": "EOD", "bars_held": len(df1m)}


def _run_day_precomputed(day_str, df_day_naive, htf_zones, sub_zones,
                         sl_buf, floor_pts, cap_pts, sl_zone_history) -> list:
    trades = []
    open_pos = False
    for htf_z in htf_zones:
        if open_pos:
            break
        zh, zl   = float(htf_z["zone_high"]), float(htf_z["zone_low"])
        kind     = htf_z.get("kind", "BEAR")
        is_long  = (kind == "BEAR")
        zone_key = f"{zl:.0f}-{zh:.0f}"
        if zone_key in sl_zone_history:
            days_since = (date.fromisoformat(day_str) - date.fromisoformat(sl_zone_history[zone_key])).days
            if days_since <= 1:
                continue
        trigger = _zone_trigger(htf_z)
        sl      = _init_sl(htf_z, sl_buf)
        t1      = float(htf_z.get("sl", 0))
        if t1 <= 0:
            continue
        if is_long and t1 <= trigger:
            continue
        if not is_long and t1 >= trigger:
            continue
        if sub_zones:
            sub_in = [s for s in sub_zones
                      if s.get("kind") == kind
                      and float(s.get("zone_high", 0)) <= zh + (zh - zl) * 0.1
                      and float(s.get("zone_low",  0)) >= zl - (zh - zl) * 0.1]
            if not sub_in:
                continue
        hit = df_day_naive[df_day_naive["low"] <= trigger] if is_long else df_day_naive[df_day_naive["high"] >= trigger]
        if hit.empty:
            continue
        entry_bar   = hit.index[0]
        entry_price = trigger
        df_exit     = df_day_naive.loc[entry_bar + 1:].copy()
        if df_exit.empty:
            continue
        result = _simulate(entry_price, is_long, sl, t1, df_exit, sl_buf, floor_pts, cap_pts)
        if result:
            result.update({"date": day_str, "kind": kind,
                           "direction": "LONG" if is_long else "SHORT",
                           "entry_price": entry_price})
            if result["exit_reason"] == "SL":
                sl_zone_history[zone_key] = day_str
            trades.append(result)
            open_pos = True
    return trades


def _summarize(trades, params) -> dict:
    wins   = [t for t in trades if t["pnl_usdt"] > 0]
    losses = [t for t in trades if t["pnl_usdt"] <= 0]
    gp     = sum(t["pnl_usdt"] for t in wins)
    gl     = abs(sum(t["pnl_usdt"] for t in losses))
    pf     = round(gp / gl, 3) if gl > 0 else (9999.0 if gp > 0 else 0.0)
    long_t  = [t for t in trades if t["direction"] == "LONG"]
    short_t = [t for t in trades if t["direction"] == "SHORT"]
    s = {
        "total": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
        "profit_factor": pf,
        "net_pnl_usdt": round(gp - gl, 4),
        "gross_profit": round(gp, 4), "gross_loss": round(gl, 4),
        "avg_win": round(gp / len(wins), 4) if wins else 0.0,
        "avg_loss": round(gl / len(losses), 4) if losses else 0.0,
        "long_trades": len(long_t), "short_trades": len(short_t),
        "long_pnl": round(sum(t["pnl_usdt"] for t in long_t), 4),
        "short_pnl": round(sum(t["pnl_usdt"] for t in short_t), 4),
        "exits_target": sum(1 for t in trades if t.get("exit_reason") == "TARGET"),
        "exits_sl": sum(1 for t in trades if t.get("exit_reason") == "SL"),
        "exits_t1": sum(1 for t in trades if t.get("exit_reason") == "T1"),
        "exits_eod": sum(1 for t in trades if t.get("exit_reason") == "EOD"),
    }
    s.update(params)
    return s


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== BTC 3-Month Optimization ({DAYS_BACK} days) ===", flush=True)
    print("Delta Exchange public API -- no credentials required", flush=True)

    df_all = _load_btc_1m()
    if df_all.empty:
        print("ERROR: No BTC data", flush=True)
        sys.exit(1)
    if df_all["datetime"].dt.tz is None:
        df_all["datetime"] = df_all["datetime"].dt.tz_localize("UTC")

    end_date   = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)
    all_dates  = [start_date + timedelta(days=i) for i in range(DAYS_BACK)]

    # Build per-day 1m data slices (naive timestamps for fast comparison in simulation)
    day_slices_1m: dict = {}   # d_str -> df_day_naive
    day_raw: dict = {}         # d_str -> (df_day_utc, df_lb_utc) for resampling
    for d in all_dates:
        d_str = d.isoformat()
        d_s   = pd.Timestamp(f"{d_str}T00:00:00", tz="UTC")
        d_e   = pd.Timestamp(f"{d_str}T23:59:59", tz="UTC")
        lb_s  = d_s - pd.Timedelta(days=LOOKBACK)
        df_day = df_all[(df_all["datetime"] >= d_s) & (df_all["datetime"] <= d_e)].copy()
        df_lb  = df_all[(df_all["datetime"] >= lb_s) & (df_all["datetime"] < d_s)].copy()
        if len(df_day) < 60:
            continue
        df_naive = df_day.copy()
        df_naive["datetime"] = df_naive["datetime"].dt.tz_convert(None)
        day_slices_1m[d_str] = df_naive
        day_raw[d_str] = (df_day, df_lb)

    print(f"[BTC] {len(day_raw)} trading days with data", flush=True)

    # ── PRECOMPUTE HTF zones and sub-zones for every unique TF × day ──────────
    # There are only 5 HTF + 4 sub values = 9 unique resample jobs.
    # Each runs over 90 days. This replaces 912 × 90 = 82,080 resample calls
    # with 9 × 90 = 810 calls.
    all_tfs = sorted(set(HTF_GRID) | set(SUB_GRID))
    print(f"[BTC] Precomputing zones for {len(all_tfs)} unique TFs x {len(day_raw)} days ...", flush=True)
    t_pre = time.time()

    # zones_cache[(tf, d_str)] -> list of CLOSED zones
    zones_cache: dict = {}
    for tf in all_tfs:
        n_with_zones = 0
        for d_str, (df_day, df_lb) in day_raw.items():
            df_combined = pd.concat([df_lb, df_day], ignore_index=True)
            htf_bars    = _resample(df_combined, tf)
            zones       = _get_zones(htf_bars)
            if not zones:
                # Cascade: today-only
                htf_today = _resample(df_day, tf)
                zones     = _get_zones(htf_today)
            zones_cache[(tf, d_str)] = zones
            if zones:
                n_with_zones += 1
        print(f"  TF={tf:3d}m: {n_with_zones}/{len(day_raw)} days have zones", flush=True)

    print(f"[BTC] Precompute done in {time.time()-t_pre:.1f}s", flush=True)

    # ── Build combos ──────────────────────────────────────────────────────────
    combos = [(h, s, sl, fl, cap)
              for h   in HTF_GRID
              for s   in SUB_GRID  if s < h
              for sl  in SL_GRID
              for fl  in FLOOR_GRID
              for cap in CAP_GRID]
    total = len(combos)
    print(f"[BTC] Sweeping {total} combos (SL/floor/cap only, zones precomputed) ...", flush=True)

    results: list = []
    t0 = time.time()
    for idx, (htf_min, sub_min, sl_buf, floor_pts, cap_pts) in enumerate(combos):
        all_trades: list = []
        sl_hist: dict = {}
        for d_str in day_raw:
            htf_zones = zones_cache[(htf_min, d_str)]
            sub_zones = zones_cache[(sub_min, d_str)]
            df_naive  = day_slices_1m[d_str]
            all_trades.extend(_run_day_precomputed(
                d_str, df_naive, htf_zones, sub_zones,
                sl_buf, floor_pts, cap_pts, sl_hist,
            ))
        params = {"htf_min": htf_min, "sub_min": sub_min, "sl_buf": sl_buf,
                  "profit_floor_pts": floor_pts, "profit_cap_pts": cap_pts, "lots": LOTS}
        results.append(_summarize(all_trades, params))
        if (idx + 1) % 100 == 0:
            elapsed = time.time() - t0
            eta     = elapsed / (idx + 1) * (total - idx - 1)
            print(f"  {idx+1}/{total}  elapsed={elapsed:.0f}s  ETA={eta:.0f}s", flush=True)

    # Sort by PF (min 5 trades)
    def _pf_key(r):
        return (r["profit_factor"] if r["profit_factor"] != 9999.0 else 9998) if r["total"] >= 5 else -1

    results.sort(key=_pf_key, reverse=True)

    # Save CSV
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    pd.DataFrame(results).to_csv(OUT_CSV, index=False)
    print(f"\n[BTC] Results -> {OUT_CSV}", flush=True)

    # Print top 20
    print(f"\n{'='*100}")
    print(f"  BTC Trap Scanner -- Top 20 Configurations (90-day backtest, {DAYS_BACK} days)")
    print(f"  Contract size: {CONTRACT_SIZE} BTC/lot  |  Lots: {LOTS}")
    print(f"{'='*100}")
    print(f"{'Rank':>4}  {'HTF':>5}  {'LTF':>5}  {'SL$':>5}  {'Floor':>5}  {'Cap':>5}  "
          f"{'Trades':>6}  {'Win%':>5}  {'PF':>6}  {'Net$':>8}  {'AvgW':>7}  {'AvgL':>7}  "
          f"{'Long$':>7}  {'Short$':>7}  {'SLs':>4}  {'T1s':>4}")
    print(f"{'-'*100}")
    for rank, r in enumerate(results[:20], 1):
        print(f"{rank:>4}  {r['htf_min']:>4}m  {r['sub_min']:>4}m  "
              f"{r['sl_buf']:>5.0f}  {r['profit_floor_pts']:>5.0f}  {r['profit_cap_pts']:>5.0f}  "
              f"{r['total']:>6}  {r['win_rate_pct']:>4.0f}%  {r['profit_factor']:>6.3f}  "
              f"{r['net_pnl_usdt']:>8.4f}  {r['avg_win']:>7.4f}  {r['avg_loss']:>7.4f}  "
              f"{r['long_pnl']:>+7.4f}  {r['short_pnl']:>+7.4f}  "
              f"{r['exits_sl']:>4}  {r['exits_t1']:>4}")

    # Show current live config result
    print(f"\n{'='*100}")
    print("  Current live config: HTF=240m LTF=30m SL=$50 Floor=$0 Cap=$0")
    live = next((r for r in results if r["htf_min"]==240 and r["sub_min"]==30
                 and r["sl_buf"]==50 and r["profit_floor_pts"]==0 and r["profit_cap_pts"]==0), None)
    if live:
        print(f"  -> Trades={live['total']}  Win%={live['win_rate_pct']}  "
              f"PF={live['profit_factor']}  Net=${live['net_pnl_usdt']:.4f}")
    print(f"{'='*100}")
    print(f"\n[BTC] Total time: {time.time()-t0:.0f}s")
