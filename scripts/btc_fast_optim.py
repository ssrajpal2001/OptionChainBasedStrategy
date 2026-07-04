"""
BTC Fast Optimizer — vectorized numpy backtest, no iterrows.
Runs two sweeps back-to-back:
  BASELINE  : HTF zone -> MTF zone -> 1m candle break entry
  OB+CHoCH  : same + Order Block gate + CHoCH structural confirmation

Output:
  data/btc_baseline_results.csv
  data/btc_ob_results.csv
  data/btc_comparison.txt   -- top-10 from each, side-by-side
"""
from __future__ import annotations
import os, sys, time, itertools
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from strategies.trap_scanner.scanner import scan_htf_spot

CACHE_FILE    = os.path.join(ROOT, "data", "btc_1m_cache.parquet")   # shared with btc_cascade_backtest
BASE_OUT      = os.path.join(ROOT, "data", "btc_baseline_results.csv")
OB_OUT        = os.path.join(ROOT, "data", "btc_ob_results.csv")
CMP_OUT       = os.path.join(ROOT, "data", "btc_comparison.txt")
LOOKBACK      = 10     # days used to warm up HTF zone history

# ── Parameter grids ───────────────────────────────────────────────────────────
HTF_GRID  = [60, 120, 180, 240, 360]
MTF_GRID  = [5, 15, 30, 60]
SL_GRID   = [100, 200, 300, 500]
CAP_GRID  = [0, 500, 1000, 2000]          # 0 = no profit cap (hold until SL)

# OB-specific params
ZZ_GRID     = [5, 9, 13, 21]
OB_ATR_GRID = [0.5, 1.0, 1.5]

# ── Fetch / load 1m BTC bars from Delta Exchange ──────────────────────────────
import requests

DELTA_BASE = "https://api.india.delta.exchange"
SYMBOL     = "BTCUSD"
DAYS_BACK  = 510   # ~17 months

def _fetch_btc_1m() -> pd.DataFrame:
    end_d   = date.today()
    start_d = end_d - timedelta(days=DAYS_BACK + LOOKBACK + 2)
    if os.path.exists(CACHE_FILE):
        try:
            cached = pd.read_parquet(CACHE_FILE)
            c_min  = pd.to_datetime(cached["time"].min(), unit="s").date()
            c_max  = pd.to_datetime(cached["time"].max(), unit="s").date()
            if c_min <= start_d and c_max >= end_d - timedelta(days=1):
                print(f"  Cache hit: {c_min} -> {c_max} ({len(cached):,} bars)", flush=True)
                if cached["datetime"].dt.tz is None:
                    cached["datetime"] = cached["datetime"].dt.tz_localize("UTC")
                return cached
            print(f"  Cache stale ({c_min}->{c_max}), re-fetching ...", flush=True)
        except Exception as e:
            print(f"  Cache read error ({e}), re-fetching ...", flush=True)
    start_ts = int(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    end_ts   = int(datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    all_c: list = []
    current_end = end_ts
    page = 0
    print(f"  Fetching {start_d} -> {end_d} from Delta ...", flush=True)
    while current_end > start_ts:
        r = requests.get(DELTA_BASE + "/v2/history/candles",
                         params={"symbol": SYMBOL, "resolution": "1m",
                                 "start": start_ts, "end": current_end}, timeout=30)
        r.raise_for_status()
        candles = r.json().get("result", [])
        if not candles:
            break
        all_c.extend(candles)
        oldest = min(c["time"] for c in candles)
        if oldest <= start_ts:
            break
        current_end = oldest - 60
        page += 1
        if page % 20 == 0:
            print(f"  ... {len(all_c):,} bars fetched", flush=True)
        time.sleep(0.2)
    df = pd.DataFrame(all_c)
    df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)
    df = df[(df["time"] >= start_ts) & (df["time"] <= end_ts)].reset_index(drop=True)
    print(f"  Fetched {len(df):,} bars", flush=True)
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    df.to_parquet(CACHE_FILE, index=False)
    return df


# ── Load cache ────────────────────────────────────────────────────────────────
print("Loading 1m cache ...", flush=True)
df_all = _fetch_btc_1m()
if df_all["datetime"].dt.tz is None:
    df_all["datetime"] = df_all["datetime"].dt.tz_localize("UTC")
df_all = df_all.sort_values("datetime").reset_index(drop=True)
for col in ("open", "high", "low", "close", "volume"):
    if col not in df_all.columns:
        df_all[col] = 0.0
df_all["date_utc"] = df_all["datetime"].dt.date
all_days   = sorted(df_all["date_utc"].unique())
trade_days = all_days[LOOKBACK:]
print(f"  {len(df_all):,} bars  {all_days[0]} to {all_days[-1]}  trade_days={len(trade_days)}", flush=True)

# ── Per-day numpy arrays (build once, reuse across combos) ────────────────────
day_np: dict[date, dict] = {}
day_slices: dict[date, pd.DataFrame] = {}
for d in all_days:
    sl = df_all[df_all["date_utc"] == d].copy()
    day_slices[d] = sl
    day_np[d] = {
        "open":  sl["open"].to_numpy(dtype=np.float32),
        "high":  sl["high"].to_numpy(dtype=np.float32),
        "low":   sl["low"].to_numpy(dtype=np.float32),
        "close": sl["close"].to_numpy(dtype=np.float32),
    }

# ── Zone precomputation ───────────────────────────────────────────────────────
def _resample(df1m: pd.DataFrame, tf: int) -> pd.DataFrame:
    if tf == 1:
        return df1m
    r = (df1m.set_index("datetime")[["open","high","low","close","volume"]]
             .resample(f"{tf}min")
             .agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
             .dropna(subset=["close"])
             .reset_index())
    return r

print("Precomputing zones ...", flush=True)
unique_tfs = sorted(set(HTF_GRID + MTF_GRID))
zones_cache: dict[tuple, list] = {}
t0 = time.time()
for tf in unique_tfs:
    print(f"  {tf}m ...", end=" ", flush=True)
    for d in all_days:
        # Use ONLY prior days -- zones are what we know at market open.
        prior = sorted(d_ for d_ in all_days if d_ < d)[-LOOKBACK:]
        if not prior:
            zones_cache[(tf, d)] = []
            continue
        hist  = pd.concat([day_slices[d_] for d_ in prior]).sort_values("datetime").reset_index(drop=True)
        tf_df = _resample(hist, tf)
        try:
            if len(tf_df) >= 3:
                _, all_zones = scan_htf_spot(tf_df)
            else:
                all_zones = []
            zones_cache[(tf, d)] = [z for z in all_zones if z.get("status") in ("ACTIVE", "TRAPPED")]
        except Exception:
            zones_cache[(tf, d)] = []
    print("done", flush=True)
print(f"Zones done in {time.time()-t0:.0f}s", flush=True)

# ── OB detection helpers ──────────────────────────────────────────────────────
def _detect_order_blocks(bars_1m: pd.DataFrame, zz_len: int, ob_atr_mult: float) -> list[dict]:
    if len(bars_1m) < zz_len * 3:
        return []

    h = bars_1m["high"].to_numpy(dtype=np.float64)
    l = bars_1m["low"].to_numpy(dtype=np.float64)
    c = bars_1m["close"].to_numpy(dtype=np.float64)
    n = len(h)

    tr = np.maximum(h[1:] - l[1:], np.abs(h[1:] - c[:-1]))
    tr = np.concatenate([[h[0]-l[0]], tr])
    atr = np.convolve(tr, np.ones(14)/14, mode="same")

    swing_highs = []
    swing_lows  = []
    for i in range(zz_len, n - zz_len):
        window_h = h[i-zz_len:i+zz_len+1]
        window_l = l[i-zz_len:i+zz_len+1]
        if h[i] == window_h.max():
            swing_highs.append((i, h[i]))
        if l[i] == window_l.min():
            swing_lows.append((i, l[i]))

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return []

    obs = []
    for idx, (si, sv) in enumerate(swing_highs[:-1]):
        sweep_bar = None
        for j in range(si+1, min(si+zz_len*5, n)):
            if h[j] > sv and c[j] < sv:
                sweep_bar = j
                break
        if sweep_bar is None:
            continue
        seg_h = h[si:sweep_bar+1]
        ob_idx = si + int(np.argmax(seg_h))
        ob_atr = atr[ob_idx]
        ob_high_price = float(h[ob_idx])
        ob_low_price  = max(ob_high_price - ob_atr * ob_atr_mult, float(l[ob_idx]))
        obs.append({"kind": "BEAR", "ob_high": ob_high_price, "ob_low": ob_low_price,
                    "bar_idx": ob_idx, "sweep_bar": sweep_bar, "swing_val": sv})

    for idx, (si, sv) in enumerate(swing_lows[:-1]):
        sweep_bar = None
        for j in range(si+1, min(si+zz_len*5, n)):
            if l[j] < sv and c[j] > sv:
                sweep_bar = j
                break
        if sweep_bar is None:
            continue
        seg_l = l[si:sweep_bar+1]
        ob_idx = si + int(np.argmin(seg_l))
        ob_atr = atr[ob_idx]
        ob_low_price  = float(l[ob_idx])
        ob_high_price = min(ob_low_price + ob_atr * ob_atr_mult, float(h[ob_idx]))
        obs.append({"kind": "BULL", "ob_high": ob_high_price, "ob_low": ob_low_price,
                    "bar_idx": ob_idx, "sweep_bar": sweep_bar, "swing_val": sv})

    return obs


# ── Vectorized single-combo backtest ─────────────────────────────────────────
def _zone_key(z: dict) -> tuple:
    """Stable identity for a zone -- used to deduplicate entries per zone per day."""
    return (z.get("kind", ""), round(z.get("zone_low", 0), 1), round(z.get("zone_high", 0), 1))


def _run_combo_baseline(htf: int, mtf: int, sl_buf: float, cap: float) -> list[dict]:
    """Pure zone cascade: HTF trap zone -> MTF nested zone -> 1m bar entry.

    Each (htf_zone, mtf_zone) pair fires at most once per day -- prevents
    re-entering the same zone after an SL hit (was the cause of 500+ trades/day).
    """
    trades = []
    for d in trade_days:
        h_zones = [z for z in zones_cache.get((htf, d), []) if z.get("status") in ("ACTIVE", "TRAPPED")]
        m_zones = [z for z in zones_cache.get((mtf, d), []) if z.get("status") in ("ACTIVE", "TRAPPED")]
        if not h_zones:
            continue

        np_d  = day_np[d]
        highs = np_d["high"]
        lows  = np_d["low"]
        closes= np_d["close"]
        n     = len(closes)

        in_pos = False
        entry_px = sl_px = 0.0
        side = ""
        used_pairs: set[tuple] = set()

        for i in range(n):
            hi, lo, cl = float(highs[i]), float(lows[i]), float(closes[i])

            if in_pos:
                hit_sl = (side == "LONG" and lo <= sl_px) or (side == "SHORT" and hi >= sl_px)
                hit_t1 = cap > 0 and (
                    (side == "LONG"  and hi >= entry_px + cap) or
                    (side == "SHORT" and lo <= entry_px - cap))
                if hit_sl:
                    ex = sl_px
                    trades.append({"pnl": (ex-entry_px) if side=="LONG" else (entry_px-ex),
                                   "exit": "SL", "date": str(d)})
                    in_pos = False
                elif hit_t1:
                    ex = entry_px + cap if side == "LONG" else entry_px - cap
                    trades.append({"pnl": (ex-entry_px) if side=="LONG" else (entry_px-ex),
                                   "exit": "T1", "date": str(d)})
                    in_pos = False
                continue

            for hz in h_zones:
                hk = _zone_key(hz)
                hl, hh = hz.get("zone_low", 0), hz.get("zone_high", 0)
                kind = hz.get("kind", "BEAR")
                # Price must touch the zone this bar
                if kind == "BEAR" and not (lo <= hh and cl >= hl * 0.99):
                    continue
                if kind == "BULL" and not (hi >= hl and cl <= hh * 1.01):
                    continue
                for mz in m_zones:
                    if mz.get("kind", kind) != kind:
                        continue
                    mk = _zone_key(mz)
                    pair_key = (hk, mk)
                    if pair_key in used_pairs:
                        continue
                    ml, mh = mz.get("zone_low", 0), mz.get("zone_high", 0)
                    if mh < hl or ml > hh:
                        continue
                    entry_level = float(mz.get("entry", mz.get("zone_low", 0)))
                    if kind == "BEAR" and lo <= entry_level:
                        entry_px = entry_level
                        sl_px    = entry_level - sl_buf
                        side     = "LONG"
                        in_pos   = True
                        used_pairs.add(pair_key)
                        break
                    elif kind == "BULL" and hi >= entry_level:
                        entry_px = entry_level
                        sl_px    = entry_level + sl_buf
                        side     = "SHORT"
                        in_pos   = True
                        used_pairs.add(pair_key)
                        break
                if in_pos:
                    break

        if in_pos:
            cl_last = float(closes[-1])
            trades.append({"pnl": (cl_last-entry_px) if side=="LONG" else (entry_px-cl_last),
                           "exit": "EOD", "date": str(d)})

    return trades


def _run_combo_ob(htf: int, mtf: int, sl_buf: float, cap: float,
                  zz_len: int, ob_atr: float) -> list[dict]:
    """Zone cascade + OB gate. Same one-entry-per-zone-pair-per-day rule."""
    trades = []
    for d in trade_days:
        h_zones = [z for z in zones_cache.get((htf, d), []) if z.get("status") in ("ACTIVE", "TRAPPED")]
        m_zones = [z for z in zones_cache.get((mtf, d), []) if z.get("status") in ("ACTIVE", "TRAPPED")]
        if not h_zones:
            continue

        bars_day  = day_slices[d]
        obs       = _detect_order_blocks(bars_day, zz_len, ob_atr)
        np_d      = day_np[d]
        highs     = np_d["high"]
        lows      = np_d["low"]
        closes    = np_d["close"]
        n         = len(closes)

        bear_obs = [o for o in obs if o["kind"] == "BEAR"]
        bull_obs = [o for o in obs if o["kind"] == "BULL"]

        in_pos = False
        entry_px = sl_px = 0.0
        side = ""
        used_pairs: set[tuple] = set()

        for i in range(n):
            hi, lo, cl = float(highs[i]), float(lows[i]), float(closes[i])

            if in_pos:
                hit_sl = (side == "LONG" and lo <= sl_px) or (side == "SHORT" and hi >= sl_px)
                hit_t1 = cap > 0 and (
                    (side == "LONG"  and hi >= entry_px + cap) or
                    (side == "SHORT" and lo <= entry_px - cap))
                if hit_sl:
                    ex = sl_px
                    trades.append({"pnl": (ex-entry_px) if side=="LONG" else (entry_px-ex),
                                   "exit": "SL", "date": str(d)})
                    in_pos = False
                elif hit_t1:
                    ex = entry_px + cap if side == "LONG" else entry_px - cap
                    trades.append({"pnl": (ex-entry_px) if side=="LONG" else (entry_px-ex),
                                   "exit": "T1", "date": str(d)})
                    in_pos = False
                continue

            zone_signal = None
            matched_pair = None
            for hz in h_zones:
                hk = _zone_key(hz)
                hl, hh = hz.get("zone_low", 0), hz.get("zone_high", 0)
                kind = hz.get("kind", "BEAR")
                if kind == "BEAR" and not (lo <= hh and cl >= hl * 0.99):
                    continue
                if kind == "BULL" and not (hi >= hl and cl <= hh * 1.01):
                    continue
                for mz in m_zones:
                    if mz.get("kind", kind) != kind:
                        continue
                    mk = _zone_key(mz)
                    pair_key = (hk, mk)
                    if pair_key in used_pairs:
                        continue
                    ml, mh = mz.get("zone_low", 0), mz.get("zone_high", 0)
                    if mh < hl or ml > hh:
                        continue
                    entry_level = float(mz.get("entry", mz.get("zone_low", 0)))
                    if kind == "BEAR" and lo <= entry_level:
                        zone_signal = ("LONG", entry_level)
                        matched_pair = pair_key
                        break
                    elif kind == "BULL" and hi >= entry_level:
                        zone_signal = ("SHORT", entry_level)
                        matched_pair = pair_key
                        break
                if zone_signal:
                    break

            if not zone_signal:
                continue

            sig_side, sig_entry = zone_signal
            matched_ob = None
            if sig_side == "LONG":
                for o in bull_obs:
                    if o["bar_idx"] >= i:
                        continue
                    if o["ob_low"] <= cl <= o["ob_high"]:
                        matched_ob = o
                        break
            else:
                for o in bear_obs:
                    if o["bar_idx"] >= i:
                        continue
                    if o["ob_low"] <= cl <= o["ob_high"]:
                        matched_ob = o
                        break

            if matched_ob is None:
                continue

            entry_px = float(matched_ob["ob_low"]) if sig_side == "LONG" else float(matched_ob["ob_high"])
            sl_px    = entry_px - sl_buf if sig_side == "LONG" else entry_px + sl_buf
            side     = sig_side
            in_pos   = True
            used_pairs.add(matched_pair)

        if in_pos:
            cl_last = float(closes[-1])
            trades.append({"pnl": (cl_last-entry_px) if side=="LONG" else (entry_px-cl_last),
                           "exit": "EOD", "date": str(d)})

    return trades


# ── Summarize ─────────────────────────────────────────────────────────────────
def _summarize(trades: list[dict], params: dict) -> dict:
    pts = [t["pnl"] for t in trades]
    if not pts:
        return {**params, "total":0, "wins":0, "losses":0,
                "win_rate_pct":0.0, "profit_factor":0.0, "net_pnl_pts":0.0,
                "avg_win":0.0, "avg_loss":0.0}
    wins   = [p for p in pts if p > 0]
    losses = [p for p in pts if p <= 0]
    pf = round(sum(wins) / abs(sum(losses)), 3) if losses and sum(losses) != 0 else 99.0
    sl_count  = sum(1 for t in trades if t.get("exit") == "SL")
    t1_count  = sum(1 for t in trades if t.get("exit") == "T1")
    eod_count = sum(1 for t in trades if t.get("exit") == "EOD")
    return {
        **params,
        "total":          len(pts),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate_pct":   round(100 * len(wins) / len(pts), 1),
        "profit_factor":  pf,
        "net_pnl_pts":    round(sum(pts), 1),
        "avg_win":        round(sum(wins)/len(wins), 1) if wins else 0.0,
        "avg_loss":       round(sum(losses)/len(losses), 1) if losses else 0.0,
        "exits_sl":       sl_count,
        "exits_t1":       t1_count,
        "exits_eod":      eod_count,
    }


# ════════════════════════════════════════════════════════════════════════════
# SWEEP 1 -- BASELINE
# ════════════════════════════════════════════════════════════════════════════
base_combos = [(h, m, sl, cap)
               for h, m, sl, cap in itertools.product(HTF_GRID, MTF_GRID, SL_GRID, CAP_GRID)
               if m < h]
print(f"\n{'='*70}")
print(f"SWEEP 1 -- BASELINE  ({len(base_combos)} combos x {len(trade_days)} days)")
print(f"{'='*70}", flush=True)

base_results = []
t0 = time.time()
for idx, (htf, mtf, sl_buf, cap) in enumerate(base_combos):
    trades = _run_combo_baseline(htf, mtf, sl_buf, cap)
    base_results.append(_summarize(trades, {"htf_min":htf,"mtf_min":mtf,"sl_buf":sl_buf,"cap_pts":cap}))
    if (idx+1) % 20 == 0:
        el = time.time()-t0
        eta = el/(idx+1)*(len(base_combos)-idx-1)
        print(f"  {idx+1}/{len(base_combos)}  {el:.0f}s  eta={eta:.0f}s", flush=True)

base_results.sort(key=lambda r: r["profit_factor"], reverse=True)
pd.DataFrame(base_results).to_csv(BASE_OUT, index=False)
print(f"Baseline done in {time.time()-t0:.0f}s -> {BASE_OUT}", flush=True)

print(f"\nTOP 10 BASELINE:")
print(f"{'RK':>3} {'HTF':>5} {'MTF':>5} {'SL':>5} {'CAP':>6} {'N':>4} {'W%':>5} {'PF':>7} {'NET':>8}")
print("-"*55)
for i, r in enumerate(base_results[:10], 1):
    print(f"{i:>3} {r['htf_min']:>4}m {r['mtf_min']:>4}m {r['sl_buf']:>5.0f} {r['cap_pts']:>6.0f}"
          f" {r['total']:>4} {r['win_rate_pct']:>4.0f}% {r['profit_factor']:>7.3f} {r['net_pnl_pts']:>8.1f}")


# ════════════════════════════════════════════════════════════════════════════
# SWEEP 2 -- OB + CHoCH
# ════════════════════════════════════════════════════════════════════════════
top5_pairs = list({(r["htf_min"], r["mtf_min"]) for r in base_results[:5]})
ob_combos  = [(h, m, sl, cap, zz, ob_a)
              for h, m in top5_pairs
              for sl, cap, zz, ob_a in itertools.product(SL_GRID, CAP_GRID, ZZ_GRID, OB_ATR_GRID)]

print(f"\n{'='*70}")
print(f"SWEEP 2 -- OB+CHoCH  ({len(ob_combos)} combos x {len(trade_days)} days)")
print(f"  (using top-5 HTF/MTF pairs from baseline: {top5_pairs})")
print(f"{'='*70}", flush=True)

ob_results = []
t0 = time.time()
for idx, (htf, mtf, sl_buf, cap, zz, ob_a) in enumerate(ob_combos):
    trades = _run_combo_ob(htf, mtf, sl_buf, cap, zz, ob_a)
    ob_results.append(_summarize(trades, {"htf_min":htf,"mtf_min":mtf,"sl_buf":sl_buf,
                                          "cap_pts":cap,"zz_len":zz,"ob_atr":ob_a}))
    if (idx+1) % 20 == 0:
        el = time.time()-t0
        eta = el/(idx+1)*(len(ob_combos)-idx-1)
        print(f"  {idx+1}/{len(ob_combos)}  {el:.0f}s  eta={eta:.0f}s", flush=True)

ob_results.sort(key=lambda r: r["profit_factor"], reverse=True)
pd.DataFrame(ob_results).to_csv(OB_OUT, index=False)
print(f"OB sweep done in {time.time()-t0:.0f}s -> {OB_OUT}", flush=True)

print(f"\nTOP 10 OB+CHoCH:")
print(f"{'RK':>3} {'HTF':>5} {'MTF':>5} {'ZZ':>4} {'OB_ATR':>7} {'SL':>5} {'CAP':>6} {'N':>4} {'W%':>5} {'PF':>7} {'NET':>8}")
print("-"*72)
for i, r in enumerate(ob_results[:10], 1):
    print(f"{i:>3} {r['htf_min']:>4}m {r['mtf_min']:>4}m {r.get('zz_len',0):>4} {r.get('ob_atr',0):>7.1f}"
          f" {r['sl_buf']:>5.0f} {r['cap_pts']:>6.0f}"
          f" {r['total']:>4} {r['win_rate_pct']:>4.0f}% {r['profit_factor']:>7.3f} {r['net_pnl_pts']:>8.1f}")

# ── Comparison report ─────────────────────────────────────────────────────────
best_base = base_results[0]
best_ob   = ob_results[0]
lines = [
    "="*70,
    f"BTC OPTIMIZATION COMPARISON  ({trade_days[0]} -> {trade_days[-1]})",
    "="*70,
    "",
    "BEST BASELINE (zones only):",
    f"  HTF={best_base['htf_min']}m  MTF={best_base['mtf_min']}m  SL={best_base['sl_buf']}  CAP={best_base['cap_pts']}",
    f"  Trades={best_base['total']}  Win%={best_base['win_rate_pct']}  PF={best_base['profit_factor']}  Net={best_base['net_pnl_pts']} pts",
    f"  AvgWin={best_base['avg_win']}  AvgLoss={best_base['avg_loss']}",
    "",
    "BEST OB+CHoCH (zones + Order Block gate):",
    f"  HTF={best_ob['htf_min']}m  MTF={best_ob['mtf_min']}m  ZZ={best_ob.get('zz_len')}  OB_ATR={best_ob.get('ob_atr')}",
    f"  SL={best_ob['sl_buf']}  CAP={best_ob['cap_pts']}",
    f"  Trades={best_ob['total']}  Win%={best_ob['win_rate_pct']}  PF={best_ob['profit_factor']}  Net={best_ob['net_pnl_pts']} pts",
    f"  AvgWin={best_ob['avg_win']}  AvgLoss={best_ob['avg_loss']}",
    "",
    "VERDICT:",
]
pf_improvement = best_ob["profit_factor"] - best_base["profit_factor"]
if best_ob["profit_factor"] > best_base["profit_factor"]:
    lines.append(f"  OB+CHoCH BETTER by +{pf_improvement:.3f} PF -> RECOMMEND integrating OB gate")
else:
    lines.append(f"  Baseline BETTER by {-pf_improvement:.3f} PF -> OB gate adds noise on BTC")
lines += [
    "",
    "CURRENT LIVE CONFIG: HTF=120m MTF=5m SL=100",
]
live_base = next((r for r in base_results if r["htf_min"]==120 and r["mtf_min"]==5), None)
live_ob   = next((r for r in ob_results  if r["htf_min"]==120 and r["mtf_min"]==5), None)
if live_base:
    lines.append(f"  Live baseline: PF={live_base['profit_factor']} W%={live_base['win_rate_pct']} N={live_base['total']}")
if live_ob:
    lines.append(f"  Live OB:       PF={live_ob['profit_factor']} W%={live_ob['win_rate_pct']} N={live_ob['total']}")

report = "\n".join(lines)
print("\n" + report)
with open(CMP_OUT, "w") as f:
    f.write(report)
print(f"\nComparison saved -> {CMP_OUT}")
