"""
BTC Cascade Trap Scanner Backtest — 4-Tier Confirmation
========================================================
Zone logic (correct, from user example):
  BEAR trap example — Candle 1: H=100, L=50
    - Next bar breaches below 50 (to 25)
    - Future bar goes ABOVE 100  →  bears who shorted inside 100-50 are TRAPPED
    - Zone = [50, 100]  (candle 1's full range)
    - Entry: when price returns INTO 50-100 zone AND cascade confirms
    - Trade: LONG (riding the short-squeeze as bears cover)
    - T1  : 100 (bears' stop level, the squeeze target)
    - SL  : below zone low − buffer

  BULL trap (opposite) — Candle 1: H=100, L=50
    - Next bar breaches above 100 (to 120)
    - Future bar goes BELOW 50  →  bulls who bought inside 50-100 are TRAPPED
    - Zone = [50, 100]
    - Entry: when price returns INTO 50-100 zone AND cascade confirms
    - Trade: SHORT (riding the flush as bulls sell to exit)
    - T1  : 50 (bulls' stop level, the flush target)
    - SL  : above zone high + buffer

Cascade (4 tiers):
    HTF (e.g. 4h)  → zone detection
    MTF (e.g. 1h)  → same-kind trap INSIDE HTF zone
    LTF (e.g. 15m) → same-kind trap INSIDE MTF zone
    Exec (e.g. 5m) → entry on break of exec candle HIGH (LONG) or LOW (SHORT)
                     exec candle must be inside the LTF zone

Usage:
    python3 scripts/btc_cascade_backtest.py
"""
from __future__ import annotations

import os, sys, time
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple

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
OUT_CSV       = os.path.join(_ROOT, "data", "btc_cascade_results.csv")
DAYS_BACK     = 90
LOOKBACK      = 5
LOTS          = 1

HTF_GRID  = [120, 180, 240, 360]
MTF_GRID  = [30, 60]
LTF_GRID  = [5, 15]
EXEC_GRID = [1, 5]
SL_GRID   = [50, 100, 200, 300]
CAP_GRID  = [0, 500, 1000, 2000]


# ── Data ──────────────────────────────────────────────────────────────────────

def _fetch_all_candles(start_date: date, end_date: date) -> pd.DataFrame:
    end_ts   = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    start_ts = int(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    all_candles: list = []
    current_end = end_ts
    page = 0
    print(f"[BTC] Fetching {start_date} -> {end_date} ...", flush=True)
    while current_end > start_ts:
        r = requests.get(DELTA_BASE + "/v2/history/candles",
                         params={"symbol": SYMBOL, "resolution": "1m",
                                 "start": start_ts, "end": current_end}, timeout=30)
        r.raise_for_status()
        candles = r.json().get("result", [])
        if not candles: break
        all_candles.extend(candles)
        oldest = min(c["time"] for c in candles)
        if oldest <= start_ts: break
        current_end = oldest - 60
        page += 1
        if page % 10 == 0:
            print(f"  ... {len(all_candles):,} bars "
                  f"(oldest {datetime.fromtimestamp(oldest, tz=timezone.utc).date()})", flush=True)
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
    # Only TRAPPED or CLOSED zones are valid signals.
    # CLOSED = price already returned to zone (bearers/bulls started covering).
    # TRAPPED = bears/bulls just got stopped, zone is fresh and hot.
    return [e for e in (ents or []) if e.get("status") in ("CLOSED", "TRAPPED")]


def _eff_zone(z: dict) -> Tuple[float, float]:
    """
    Zone = [zone_low, zone_high] = the BREAKDOWN area where trapped traders entered.

    BEAR example: Candle1 H=100,L=50 → next candle L=25 → future bar goes above 100 (TRAP)
      zone_low=25, zone_high=50  (breakdown zone, where bears shorted)
      sl=100  (bears' stop = our T1 = squeeze target)
      Entry: price returns INTO [25, 50] after squeezing above 100 → we BUY

    BULL example (opposite):
      zone_low=100, zone_high=120  (breakout zone, where bulls bought)
      sl=50   (bulls' stop = our T1 = flush target)
      Entry: price returns INTO [100, 120] after flushing below 50 → we SELL
    """
    return float(z["zone_low"]), float(z["zone_high"])


def _zones_overlap(parent: dict, child: dict, tol: float = 0.10) -> bool:
    """Child zone's effective range overlaps parent's effective range (within tol% of parent width)."""
    pl, ph = _eff_zone(parent)
    cl, ch = _eff_zone(child)
    buf = (ph - pl) * tol
    return cl <= ph + buf and ch >= pl - buf


# ── Entry via exec-TF candle break ────────────────────────────────────────────

def _find_exec_entry(df_exec_naive: pd.DataFrame,
                     ltf_zone: dict,
                     htf_zone: dict,
                     kind: str,
                     sl_buf: float,
                     cap_pts: float) -> Optional[dict]:
    """
    Scan exec-TF bars for a candle INSIDE the LTF zone, then enter on
    the break of that candle's HIGH (LONG/BEAR trap) or LOW (SHORT/BULL trap).

    BEAR trap entry (LONG):
      - Exec candle must be inside LTF effective zone [ltf_low, ltf_high]
      - Entry: next exec candle breaks ABOVE this candle's HIGH
      - SL   : this candle's LOW - sl_buf
      - T1   : htf_zone["sl"] = bears' original stop (= reference candle HIGH)

    BULL trap entry (SHORT):
      - Exec candle inside LTF zone
      - Entry: next exec candle breaks BELOW this candle's LOW
      - SL   : this candle's HIGH + sl_buf
      - T1   : htf_zone["sl"] = bulls' original stop (= reference candle LOW)
    """
    is_long  = (kind == "BEAR")
    ltf_l, ltf_h = _eff_zone(ltf_zone)
    zone_range = max(ltf_h - ltf_l, 1.0)
    buf        = zone_range * 0.15   # 15% tolerance for "inside zone"

    t1   = float(htf_zone.get("sl", 0))
    size = CONTRACT_SIZE * LOTS
    if t1 <= 0:
        return None

    bars = df_exec_naive.reset_index(drop=True)
    n    = len(bars)

    for i in range(n - 1):
        bar = bars.iloc[i]
        c   = float(bar["close"])

        # Exec candle must be inside the LTF effective zone
        if not (ltf_l - buf <= c <= ltf_h + buf):
            continue

        # Entry trigger level = this candle's H (LONG) or L (SHORT)
        trig    = float(bar["high"]) if is_long else float(bar["low"])
        entry_sl = float(bar["low"]) - sl_buf if is_long else float(bar["high"]) + sl_buf

        # Sanity: T1 must be on the profitable side
        if is_long and (t1 <= trig or entry_sl >= trig):
            continue
        if not is_long and (t1 >= trig or entry_sl <= trig):
            continue

        entry_price = None
        active_sl   = entry_sl

        for j in range(i + 1, n):
            nb = bars.iloc[j]

            if entry_price is None:
                # Wait for trigger break
                if is_long  and float(nb["high"]) >= trig:
                    entry_price = trig
                elif not is_long and float(nb["low"]) <= trig:
                    entry_price = trig
                if entry_price is None:
                    continue

            cur = float(nb["close"])
            run = (cur - entry_price) if is_long else (entry_price - cur)

            # Trail SL
            new_trail = (float(nb["high"]) - sl_buf) if is_long else (float(nb["low"]) + sl_buf)
            if is_long and new_trail > active_sl:
                active_sl = new_trail
            elif not is_long and new_trail < active_sl:
                active_sl = new_trail

            # Profit cap
            if cap_pts > 0 and run >= cap_pts:
                return {"exit_price": cur, "pnl_usdt": round(run * size, 4),
                        "exit_reason": "CAP", "entry_price": entry_price,
                        "trig": round(trig, 2), "t1": round(t1, 2),
                        "exec_sl": round(entry_sl, 2)}

            # SL hit
            if (is_long  and float(nb["low"])  <= active_sl) or \
               (not is_long and float(nb["high"]) >= active_sl):
                pnl = (active_sl - entry_price if is_long else entry_price - active_sl) * size
                return {"exit_price": active_sl, "pnl_usdt": round(pnl, 4),
                        "exit_reason": "SL", "entry_price": entry_price,
                        "trig": round(trig, 2), "t1": round(t1, 2),
                        "exec_sl": round(entry_sl, 2)}

            # T1 hit
            if (is_long  and float(nb["high"]) >= t1) or \
               (not is_long and float(nb["low"])  <= t1):
                pnl = (t1 - entry_price if is_long else entry_price - t1) * size
                return {"exit_price": t1, "pnl_usdt": round(pnl, 4),
                        "exit_reason": "T1", "entry_price": entry_price,
                        "trig": round(trig, 2), "t1": round(t1, 2),
                        "exec_sl": round(entry_sl, 2)}

        if entry_price is not None:
            ep  = float(bars.iloc[-1]["close"])
            pnl = (ep - entry_price if is_long else entry_price - ep) * size
            return {"exit_price": ep, "pnl_usdt": round(pnl, 4),
                    "exit_reason": "EOD", "entry_price": entry_price,
                    "trig": round(trig, 2), "t1": round(t1, 2),
                    "exec_sl": round(entry_sl, 2)}

    return None


# ── Per-day cascade runner ─────────────────────────────────────────────────────

def _run_cascade_day(day_str, df_exec_naive,
                     htf_zones, mtf_zones, ltf_zones,
                     sl_buf, cap_pts, sl_zone_history) -> list:
    """
    For each HTF trap zone:
      1. Find matching MTF zone (same kind, overlaps HTF effective zone)
      2. Find matching LTF zone (same kind, overlaps MTF effective zone)
      3. Find exec candle inside LTF zone → enter on H/L break
    Max one trade per day.
    """
    trades   = []
    open_pos = False

    for htf_z in htf_zones:
        if open_pos:
            break
        kind     = htf_z.get("kind", "BEAR")
        hl, hh   = _eff_zone(htf_z)
        zone_key = f"{hl:.0f}-{hh:.0f}"

        # 1-day cooldown on zones that caused SL
        if zone_key in sl_zone_history:
            days_since = (date.fromisoformat(day_str) -
                          date.fromisoformat(sl_zone_history[zone_key])).days
            if days_since <= 1:
                continue

        # T1 sanity
        t1 = float(htf_z.get("sl", 0))
        if t1 <= 0:
            continue
        is_long = (kind == "BEAR")
        # BEAR: t1=sl=candle1.high must be ABOVE zone_high (= candle1.low)
        # BULL: t1=sl=candle1.low  must be BELOW zone_low  (= candle1.high)
        if is_long  and t1 <= hh: continue
        if not is_long and t1 >= hl: continue

        # MTF: same kind, overlaps HTF zone
        mtf_match = next((z for z in mtf_zones
                          if z.get("kind") == kind and _zones_overlap(htf_z, z)), None)
        if mtf_match is None:
            continue

        # LTF: same kind, overlaps MTF zone
        ltf_match = next((z for z in ltf_zones
                          if z.get("kind") == kind and _zones_overlap(mtf_match, z)), None)
        if ltf_match is None:
            continue

        # Exec: entry on candle H/L break inside LTF zone
        result = _find_exec_entry(df_exec_naive, ltf_match, htf_z, kind, sl_buf, cap_pts)
        if result:
            result.update({
                "date"     : day_str,
                "kind"     : kind,
                "direction": "LONG" if is_long else "SHORT",
                "htf_zone" : zone_key,
                "htf_eff_low": round(hl, 2), "htf_eff_high": round(hh, 2),
            })
            if result["exit_reason"] == "SL":
                sl_zone_history[zone_key] = day_str
            trades.append(result)
            open_pos = True

    return trades


# ── Summary ───────────────────────────────────────────────────────────────────

def _summarize(trades: list, params: dict) -> dict:
    wins   = [t for t in trades if t["pnl_usdt"] > 0]
    losses = [t for t in trades if t["pnl_usdt"] <= 0]
    gp     = sum(t["pnl_usdt"] for t in wins)
    gl     = abs(sum(t["pnl_usdt"] for t in losses))
    pf     = round(gp / gl, 3) if gl > 0 else (9999.0 if gp > 0 else 0.0)
    long_t  = [t for t in trades if t["direction"] == "LONG"]
    short_t = [t for t in trades if t["direction"] == "SHORT"]
    s = {
        "total"         : len(trades),
        "wins"          : len(wins),
        "losses"        : len(losses),
        "win_rate_pct"  : round(len(wins) / len(trades) * 100, 1) if trades else 0.0,
        "profit_factor" : pf,
        "net_pnl_usdt"  : round(gp - gl, 4),
        "gross_profit"  : round(gp, 4),
        "gross_loss"    : round(gl, 4),
        "avg_win"       : round(gp / len(wins),   4) if wins   else 0.0,
        "avg_loss"      : round(gl / len(losses), 4) if losses else 0.0,
        "long_trades"   : len(long_t),
        "short_trades"  : len(short_t),
        "long_pnl"      : round(sum(t["pnl_usdt"] for t in long_t),  4),
        "short_pnl"     : round(sum(t["pnl_usdt"] for t in short_t), 4),
        "exits_sl"      : sum(1 for t in trades if t.get("exit_reason") == "SL"),
        "exits_t1"      : sum(1 for t in trades if t.get("exit_reason") == "T1"),
        "exits_cap"     : sum(1 for t in trades if t.get("exit_reason") == "CAP"),
        "exits_eod"     : sum(1 for t in trades if t.get("exit_reason") == "EOD"),
    }
    s.update(params)
    return s


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"=== BTC 4-Tier Cascade Backtest ({DAYS_BACK} days) ===", flush=True)
    print("Zone = reference candle full range. Entry = exec candle H/L break inside cascade.", flush=True)

    df_all = _load_btc_1m()
    if df_all.empty:
        print("ERROR: No data", flush=True)
        sys.exit(1)
    if df_all["datetime"].dt.tz is None:
        df_all["datetime"] = df_all["datetime"].dt.tz_localize("UTC")

    end_date   = date.today()
    start_date = end_date - timedelta(days=DAYS_BACK)
    all_dates  = [start_date + timedelta(days=i) for i in range(DAYS_BACK)]

    day_raw: dict = {}
    for d in all_dates:
        d_str  = d.isoformat()
        d_s    = pd.Timestamp(f"{d_str}T00:00:00", tz="UTC")
        d_e    = pd.Timestamp(f"{d_str}T23:59:59", tz="UTC")
        lb_s   = d_s - pd.Timedelta(days=LOOKBACK)
        df_day = df_all[(df_all["datetime"] >= d_s) & (df_all["datetime"] <= d_e)].copy()
        df_lb  = df_all[(df_all["datetime"] >= lb_s) & (df_all["datetime"] < d_s)].copy()
        if len(df_day) >= 60:
            day_raw[d_str] = (df_day, df_lb)

    print(f"[BTC] {len(day_raw)} trading days", flush=True)

    all_zone_tfs = sorted(set(HTF_GRID) | set(MTF_GRID) | set(LTF_GRID))
    all_exec_tfs = sorted(set(EXEC_GRID))
    print(f"[BTC] Precomputing zones: {all_zone_tfs} ...", flush=True)
    t_pre = time.time()

    zones_cache: dict = {}
    for tf in all_zone_tfs:
        n = 0
        for d_str, (df_day, df_lb) in day_raw.items():
            combined = pd.concat([df_lb, df_day], ignore_index=True)
            bars     = _resample(combined, tf)
            zones    = _get_zones(bars)
            if not zones:
                zones = _get_zones(_resample(df_day, tf))
            zones_cache[(tf, d_str)] = zones
            if zones: n += 1
        print(f"  TF={tf:4d}m: {n}/{len(day_raw)} days have zones", flush=True)

    exec_cache: dict = {}
    for tf in all_exec_tfs:
        for d_str, (df_day, _) in day_raw.items():
            df_ex = _resample(df_day, tf).copy()
            if df_ex["datetime"].dt.tz is not None:
                df_ex["datetime"] = df_ex["datetime"].dt.tz_convert(None)
            exec_cache[(tf, d_str)] = df_ex

    print(f"[BTC] Precompute done in {time.time()-t_pre:.1f}s", flush=True)

    combos = [
        (htf, mtf, ltf, exc, sl, cap)
        for htf in HTF_GRID
        for mtf in MTF_GRID if mtf < htf
        for ltf in LTF_GRID if ltf < mtf
        for exc in EXEC_GRID if exc <= ltf
        for sl  in SL_GRID
        for cap in CAP_GRID
    ]
    total = len(combos)
    print(f"[BTC] {total} combos ...", flush=True)

    results: list = []
    t0 = time.time()
    for idx, (htf_min, mtf_min, ltf_min, exec_min, sl_buf, cap_pts) in enumerate(combos):
        all_trades: list = []
        sl_hist: dict    = {}
        for d_str in day_raw:
            htf_z = zones_cache[(htf_min, d_str)]
            mtf_z = zones_cache[(mtf_min, d_str)]
            ltf_z = zones_cache[(ltf_min, d_str)]
            df_ex = exec_cache[(exec_min, d_str)]
            if not htf_z or df_ex.empty:
                continue
            all_trades.extend(_run_cascade_day(
                d_str, df_ex, htf_z, mtf_z, ltf_z, sl_buf, cap_pts, sl_hist,
            ))
        params = {"htf_min": htf_min, "mtf_min": mtf_min, "ltf_min": ltf_min,
                  "exec_min": exec_min, "sl_buf": sl_buf, "cap_pts": cap_pts, "lots": LOTS}
        results.append(_summarize(all_trades, params))
        if (idx + 1) % 50 == 0:
            elapsed = time.time() - t0
            eta     = elapsed / (idx + 1) * (total - idx - 1)
            print(f"  {idx+1}/{total}  elapsed={elapsed:.0f}s  ETA={eta:.0f}s", flush=True)

    results.sort(key=lambda r: (r["profit_factor"] if r["profit_factor"] != 9999.0 else 9998)
                 if r["total"] >= 3 else -1, reverse=True)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    pd.DataFrame(results).to_csv(OUT_CSV, index=False)
    print(f"\n[BTC] Results -> {OUT_CSV}", flush=True)

    print(f"\n{'='*115}")
    print(f"  BTC 4-Tier Cascade -- Top 25  ({DAYS_BACK}-day backtest)")
    print(f"  Zone = ref-candle range. Entry = exec H/L break. LONG=BEAR trap. SHORT=BULL trap.")
    print(f"{'='*115}")
    print(f"{'Rank':>4}  {'HTF':>5}  {'MTF':>5}  {'LTF':>5}  {'Exc':>4}  {'SL$':>5}  {'Cap$':>5}  "
          f"{'#':>4}  {'Win%':>5}  {'PF':>6}  {'Net$':>8}  {'AvgW':>7}  {'AvgL':>7}  "
          f"{'L$':>7}  {'S$':>7}  {'SLs':>4}  {'T1s':>4}  {'Cap':>4}  {'EOD':>4}")
    print(f"{'-'*115}")
    for rank, r in enumerate(results[:25], 1):
        print(f"{rank:>4}  {r['htf_min']:>4}m  {r['mtf_min']:>4}m  {r['ltf_min']:>4}m  "
              f"{r['exec_min']:>3}m  {r['sl_buf']:>5.0f}  {r['cap_pts']:>5.0f}  "
              f"{r['total']:>4}  {r['win_rate_pct']:>4.0f}%  {r['profit_factor']:>6.3f}  "
              f"{r['net_pnl_usdt']:>8.4f}  {r['avg_win']:>7.4f}  {r['avg_loss']:>7.4f}  "
              f"{r['long_pnl']:>+7.4f}  {r['short_pnl']:>+7.4f}  "
              f"{r['exits_sl']:>4}  {r['exits_t1']:>4}  {r['exits_cap']:>4}  {r['exits_eod']:>4}")

    print(f"\n[BTC] Done in {time.time()-t0:.0f}s")
    print("Key: PF>1.5 = good | PF>2.0 = excellent | Net$ per 0.001 BTC contract x lots=1")
    print("LONG trades = bear sellers trapped, riding squeeze UP")
    print("SHORT trades = bull buyers trapped, riding flush DOWN")
