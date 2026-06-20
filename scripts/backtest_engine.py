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
GAP_PCT      = 0.005   # 0.5% — CrudeOil MCX: overnight window only 9.5hr, normal drift < 0.3%
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


# ── Zone detection (correct 2-candle algorithm) ───────────────────────────────
def scan_zones_consecutive(htf_df: pd.DataFrame) -> list[dict]:
    """
    Detect BEAR and BULL traps using consecutive candle lows/highs.

    BEAR zone (sellers trapped → we BUY):
      C1 = reference candle.  C2.low < C1.low → sellers entered short at C1.
      zone_high = C1.body_bottom = min(C1.open,C1.close)  (body-based, excludes wick)
                  Red C1: C1.close.  Green C1: C1.open.
      zone_low  = C2.low  (how far price fell   — zone bottom)
      t1        = C1.high  (sellers' SL = our profit target)
      TRAPPED   = any bar's high  > C1.high
      ENTRY_READY = after TRAPPED, price re-enters zone (low ≤ price ≤ zone_high)

    BULL zone (buyers trapped → we SELL / PE):
      C1 = reference candle.  C2.high > C1.high → buyers entered long at C1.
      zone_low  = C1.body_top = max(C1.open,C1.close)  (body-based, excludes wick)
                  Red C1: C1.open.  Green C1: C1.close.
      zone_high = C2.high  (how far price rose   — zone top)
      t1        = C1.low   (buyers' SL = our profit target)
      TRAPPED   = any bar's low   < C1.low
      ENTRY_READY = after TRAPPED, price re-enters zone (zone_low ≤ price ≤ zone_high)
    """
    if len(htf_df) < 3:
        return []

    bars   = htf_df.reset_index(drop=True)
    zones  = []

    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]
        p_open, p_close = float(prev["open"]), float(prev["close"])
        p_low,  p_high  = float(prev["low"]),  float(prev["high"])
        c_low,  c_high  = float(curr["low"]),  float(curr["high"])

        # Body boundaries (exclude wicks — use candle body for zone boundary)
        # Red C1:   body_top = open,  body_bottom = close
        # Green C1: body_top = close, body_bottom = open
        p_body_top    = max(p_open, p_close)   # where price was at end of C1's push
        p_body_bottom = min(p_open, p_close)

        # ── BEAR zone (sellers trapped → CE trade) ─────────────────────────
        # C2.low < C1.low → sellers entered short below C1.body_bottom
        if c_low < p_low:
            z: dict = {
                "kind":      "BEAR",
                "zone_high": p_body_bottom,  # C1 body bottom = where sellers entered
                "zone_low":  c_low,          # C2.low = how far price dropped
                "t1":        p_high,         # C1.high = sellers' SL = our T1
                "ref_dt":    prev["datetime"],
                "sellers_in_dt": curr["datetime"],
                "trapped_dt":    None,
                "entry_ready_dt": None,
                "status":    "SELLERS_IN",
            }
            trapped = False
            for j in range(i + 1, len(bars)):
                b = bars.iloc[j]
                if not trapped:
                    if float(b["high"]) > p_high:    # price breaks C1.high → sellers TRAPPED
                        trapped = True
                        z["trapped_dt"] = b["datetime"]
                        z["status"]     = "TRAPPED"
                else:
                    if float(b["low"]) <= p_body_bottom:  # price returns to zone_high
                        z["entry_ready_dt"] = b["datetime"]
                        z["status"]         = "ENTRY_READY"
                        break
            zones.append(z)

        # ── BULL zone (buyers trapped → PE trade) ──────────────────────────
        # C2.high > C1.high → buyers entered long above C1.body_top
        if c_high > p_high:
            z = {
                "kind":      "BULL",
                "zone_low":  p_body_top,   # C1 body top = where buyers entered
                "zone_high": c_high,       # C2.high = how far price rose
                "t1":        p_low,        # C1.low = buyers' SL = our T1
                "ref_dt":    prev["datetime"],
                "sellers_in_dt": curr["datetime"],   # buyers_in for BULL
                "trapped_dt":    None,
                "entry_ready_dt": None,
                "status":    "BUYERS_IN",
            }
            trapped = False
            for j in range(i + 1, len(bars)):
                b = bars.iloc[j]
                if not trapped:
                    if float(b["low"]) < p_low:       # price breaks C1.low → buyers TRAPPED
                        trapped = True
                        z["trapped_dt"] = b["datetime"]
                        z["status"]     = "TRAPPED"
                else:
                    if float(b["high"]) >= p_body_top:  # price returns to zone_low
                        z["entry_ready_dt"] = b["datetime"]
                        z["status"]         = "ENTRY_READY"
                        break
            zones.append(z)

    return zones


def _trapped_zones(htf_df: pd.DataFrame, min_width: float = 30.0,
                   max_age_days: int = 0, trade_date: str = "") -> list[dict]:
    """Return only zones that reached TRAPPED or ENTRY_READY, wide enough, and not too old."""
    zones = [z for z in scan_zones_consecutive(htf_df)
             if z["status"] in ("TRAPPED", "ENTRY_READY")
             and (z["zone_high"] - z["zone_low"]) >= min_width]
    if max_age_days > 0 and trade_date:
        cutoff = pd.Timestamp(trade_date) - pd.Timedelta(days=max_age_days)
        zones = [z for z in zones if pd.Timestamp(z["ref_dt"]) >= cutoff]
    return zones


# ── Zone helpers ──────────────────────────────────────────────────────────────
def _get_prev_day_htf_zones(prev_df: pd.DataFrame, htf_min: int,
                             min_width: float = 30.0, max_age_days: int = 0,
                             trade_date: str = "") -> list[dict]:
    """Scan previous day's bars at htf_min resolution → return TRAPPED zones."""
    if prev_df.empty:
        return []
    htf = resample(prev_df, htf_min)
    return _trapped_zones(htf, min_width, max_age_days, trade_date)


def _get_combined_zones_at(prev_df: pd.DataFrame, today_df: pd.DataFrame,
                            bar_ts, htf_min: int, min_width: float = 30.0,
                            max_age_days: int = 0, trade_date: str = "") -> list[dict]:
    """
    Scan BOTH previous day bars + today's bars up to bar_ts combined.
    """
    today_date = bar_ts.date()
    today_hist = today_df[(today_df["datetime"].dt.date == today_date) &
                          (today_df["datetime"] <= bar_ts)]

    combined = pd.concat([prev_df, today_hist], ignore_index=True)
    combined = combined.sort_values("datetime").reset_index(drop=True)
    if combined.empty:
        return []
    htf = resample(combined, htf_min)
    return _trapped_zones(htf, min_width, max_age_days, trade_date)


def _zones_for_direction(zones: list[dict], direction: str) -> list[dict]:
    """Filter zones by kind: BEAR=CE(bullish), BULL=PE(bearish)."""
    return [z for z in zones if z.get("kind") == direction]


def _price_in_zone(price: float, zone: dict) -> bool:
    return zone.get("zone_low", 0) <= price <= zone.get("zone_high", 0)


def _price_near_zone(price: float, zone: dict, buffer_pct: float = 0.005) -> bool:
    """True if price is within buffer% above zone_high (approaching from below for BEAR)
    or within buffer% below zone_low (approaching from above for BULL)."""
    zl, zh = zone.get("zone_low", 0), zone.get("zone_high", 0)
    buf = (zh - zl) * buffer_pct + buffer_pct * price
    return (zl - buf) <= price <= (zh + buf)


def _ts_fmt(ts) -> str:
    return ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)[:16]


def _find_ltf_trap_entries(
    today_df: pd.DataFrame,
    htf_zone: dict,
    touch_ts,
    direction: str,
    sq_time,
    ltf_min: int,
    wide_zone: bool = False,
) -> list[dict]:
    """
    Scan LTF bars from touch_ts for sub-traps in `direction` within the HTF zone area.
    wide_zone=True uses a 50% buffer (for opposite-direction scans above/below the zone).
    Returns list of {ts, price, zone, t1} sorted by time.

    direction: "BEAR" (sub-sellers trapped → CE) or "BULL" (sub-buyers trapped → PE)
    """
    zl = htf_zone.get("zone_low", 0)
    zh = htf_zone.get("zone_high", 0)
    buf_pct  = 0.50 if wide_zone else 0.15
    buf = (zh - zl) * buf_pct
    zone_min = zl - buf
    zone_max = zh + buf

    # Bars from touch_ts up to sq_time, within zone price range
    window = today_df[
        (today_df["datetime"] >= touch_ts) &
        (today_df["datetime"] < sq_time) &
        (today_df["close"] >= zone_min) &
        (today_df["close"] <= zone_max)
    ]
    if len(window) < ltf_min * 2:  # Need at least 2 LTF candles
        return []

    ltf = resample(window, ltf_min)
    if len(ltf) < 2:
        return []

    sub_zones = _trapped_zones(ltf)
    # Sub-traps of same direction within this zone — only ENTRY_READY
    # (price already returned to zone after sellers trapped)
    sub_traps = [z for z in sub_zones
                 if z.get("kind") == direction and z.get("status") == "ENTRY_READY"]

    entries = []
    for z in sub_traps:
        sub_zl   = z.get("zone_low", zl)
        sub_zh   = z.get("zone_high", zh)
        entry_dt = z.get("entry_ready_dt")
        if entry_dt is None:
            continue
        # Get price at ENTRY_READY moment
        match = ltf[ltf["datetime"] <= entry_dt]
        if match.empty:
            continue
        entry_bar = match.iloc[-1]
        entries.append({
            "ts":    entry_bar["datetime"],
            "price": float(entry_bar["close"]),
            "zone":  f"{sub_zl:.0f}→{sub_zh:.0f}",
            "t1":    z.get("t1", sub_zh if direction == "BEAR" else sub_zl),
        })

    # Sort by time, deduplicate
    seen = set()
    result = []
    for e in sorted(entries, key=lambda x: x["ts"]):
        key = f"{e['ts']}"
        if key not in seen:
            seen.add(key)
            result.append(e)
    return result


# ── Option bar fetching for Logic 2 ──────────────────────────────────────────
_MCX_MASTER_CACHE: list = []

def _load_mcx_master() -> list:
    global _MCX_MASTER_CACHE
    if _MCX_MASTER_CACHE:
        return _MCX_MASTER_CACHE
    import gzip, json
    try:
        r = requests.get("https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz", timeout=30)
        _MCX_MASTER_CACHE = json.loads(gzip.decompress(r.content))
    except Exception:
        _MCX_MASTER_CACHE = []
    return _MCX_MASTER_CACHE

def _find_crude_option_key(strike: int, otype: str, min_expiry) -> str:
    """Find Upstox instrument key for CrudeOil option at given strike/type/expiry."""
    master = _load_mcx_master()
    ot = otype.upper()
    candidates = []
    for row in master:
        itype     = str(row.get("instrument_type", "")).upper()
        row_otype = itype if itype in ("CE", "PE") else str(row.get("option_type", "")).upper()
        if row_otype != ot:
            continue
        if abs(float(row.get("strike", 0) or 0) - strike) > 0.5:
            continue
        sym = str(row.get("tradingsymbol", "") or row.get("name", "")).upper()
        und = str(row.get("underlying_symbol", "") or "").upper()
        if "CRUDE" not in sym and "CRUDE" not in und:
            continue
        exp_str = str(row.get("expiry", "") or "")
        try:
            exp_dt = date.fromisoformat(exp_str[:10])
        except Exception:
            continue
        if exp_dt < min_expiry:
            continue
        key = str(row.get("instrument_key", ""))
        if key:
            candidates.append((exp_dt, key))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# ── Per-day backtest ──────────────────────────────────────────────────────────
def _run_day(
    trade_date: str,
    today_df: pd.DataFrame,
    lookback_df: pd.DataFrame,
    htf_min_zone: int,
    htf_min_cascade: int,
    sl_buf: float,
    gap_threshold: float,
    combo_filter: str,
    lot_size: int,
    ltf_minutes: list[int],    # e.g. [5, 15]
    min_width: float = 30.0,
    max_age_days: int = 5,
    ltf_source: str = "futures",
    itm_offset: int = 300,
    gap_dir_filter: bool = True,
    require_gap:    bool = True,   # False = cascade on ALL days regardless of gap
) -> list[dict]:
    """Run one day's backtest with LTF sub-trap comparison. Returns list of trade records."""
    trades: list[dict] = []
    if today_df.empty:
        return trades

    today_open = float(today_df.iloc[0]["open"])
    prev_date  = today_df.iloc[0]["datetime"].date()
    prev_bars  = lookback_df[lookback_df["datetime"].dt.date < prev_date]
    prev_close = float(prev_bars.iloc[-1]["close"]) if not prev_bars.empty else 0.0
    gap_info   = detect_gap(today_open, prev_close, gap_threshold)
    has_gap    = gap_info["gap"]

    # Historical HTF zones from lookback window
    htf_zones_hist = _get_prev_day_htf_zones(lookback_df, htf_min_zone, min_width, max_age_days, trade_date)

    # Gap-through detection: if price gapped past ALL historical zones, skip them
    # and use intraday cascade only (fresh traps at current price level)
    def _zones_reachable(zones, price):
        """Return zones where price is within 3% of the zone (reachable today)."""
        result = []
        for z in zones:
            zh, zl = z.get("zone_high", 0), z.get("zone_low", 0)
            center = (zh + zl) / 2
            if abs(price - center) / max(center, 1) <= 0.03:
                result.append(z)
        return result

    reachable_hist = _zones_reachable(htf_zones_hist, today_open)

    if not require_gap:
        # Pure intraday mode: cascade on ALL days, no gap filter
        # Direction comes entirely from zone (who is trapped), not from gap
        use_cascade_only = True
        has_htf_zone     = False
        combo_base       = "CASCADE" + ("+GAP" if has_gap else "+NOGAP")
    else:
        # Gap-required mode (default): cascade only on gap days
        use_cascade_only = has_gap
        has_htf_zone     = (not has_gap) and len(reachable_hist) > 0
        combo_base       = _combo_label(has_gap, has_htf_zone)
        # NO_GAP+NO_ZONE = no directional bias → skip
        if combo_base == "NO_GAP+NO_ZONE":
            return trades

    if combo_filter != "all":
        wanted = combo_filter.upper().replace("-", "_")
        if combo_base.upper() != wanted:
            return trades

    sq_time    = pd.Timestamp(f"{trade_date} {SQ_OFF}").tz_localize(None)
    entry_open = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}").tz_localize(None)
    gap_label  = f"{gap_info['direction']} {gap_info['pct']:+.2f}%" if has_gap else "none"

    def _simulate_exit(entry_ts, entry_p, sl_p, t1_p, t2_p, kind, ltf_min):
        """
        Three-stage exit:
        Stage 1 (full qty): SL or T1.
          SL → exit 100%.
          T1 → exit 50% at T1, activate TSL on remaining 50%.
          EOD → exit 100%.

        Stage 2 (50% runner): Zone-based TSL.
          TSL = zone_low of the most recent LTF BEAR zone above entry that reached
                ENTRY_READY (lowest point where those bears exited).
          TSL only moves UP (BEAR) / DOWN (BULL), never back.
          T2 (if set): take another 50% of runner at T2, TSL on final 25%.
          Exit: price closes below TSL or EOD.
        """
        fwd = today_df[today_df["datetime"] > entry_ts]

        # ── Stage 1: SL or T1 ──────────────────────────────────────────────
        t1_price, t1_ts = None, None
        for _, fb in fwd.iterrows():
            flo, fhi, fts = float(fb["low"]), float(fb["high"]), fb["datetime"]
            if fts >= sq_time:
                ob = today_df[today_df["datetime"] <= fts]
                ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
                return _exit_result("EOD+EOD", entry_p, ep, fts, ep, fts, lot_size, kind, t2_p)
            if (flo <= sl_p) if kind == "BEAR" else (fhi >= sl_p):
                ob = today_df[today_df["datetime"] <= fts]
                ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
                return _exit_result("SL", entry_p, ep, fts, None, None, lot_size, kind, t2_p)
            if (fhi >= t1_p) if kind == "BEAR" else (flo <= t1_p):
                ob = today_df[today_df["datetime"] <= fts]
                t1_price = float(ob.iloc[-1]["close"]) if not ob.empty else t1_p
                t1_ts    = fts
                break

        if t1_price is None:
            ob = today_df[today_df["datetime"] <= sq_time]
            ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
            return _exit_result("EOD+EOD", entry_p, ep, sq_time, ep, sq_time, lot_size, kind, t2_p)

        # ── Stage 2: Zone-based TSL on runner (50%) ────────────────────────
        # TSL watches for fresh LTF BEAR zones that form ABOVE our entry.
        # When one reaches ENTRY_READY (those bears flushed out at zone_low),
        # TSL jumps to that zone_low — the lowest point where last bears exited.
        # TSL only moves in our favour (UP for BEAR, DOWN for BULL).

        after_t1 = today_df[today_df["datetime"] > t1_ts]
        tsl      = entry_p    # TSL starts at break-even
        tsl_p    = t1_price
        tsl_ts   = sq_time
        exit2_reason = "T1+EOD"

        if after_t1.empty:
            ob = today_df[today_df["datetime"] <= sq_time]
            tsl_p = float(ob.iloc[-1]["close"]) if not ob.empty else t1_price
            return _exit_result("T1+EOD", entry_p, t1_price, t1_ts, tsl_p, sq_time, lot_size, kind, t2_p)

        acc_rows: list = []
        _tsl_pending: dict = {}   # uid → zone at ENTRY_READY, waiting for price to re-hit t1

        for _, fb in after_t1.iterrows():
            fts    = fb["datetime"]
            flo    = float(fb["low"])
            fhi    = float(fb["high"])
            fclose = float(fb["close"])
            acc_rows.append(fb)

            if fts >= sq_time:
                tsl_p, tsl_ts, exit2_reason = fclose, fts, "T1+EOD"
                break

            # T2 hit → close all remaining, done
            if t2_p is not None:
                t2_hit = (fhi >= t2_p) if kind == "BEAR" else (flo <= t2_p)
                if t2_hit:
                    ob = today_df[today_df["datetime"] <= fts]
                    tsl_p = float(ob.iloc[-1]["close"]) if not ob.empty else t2_p
                    tsl_ts, exit2_reason = fts, "T1+T2"
                    break

            # TSL hit check (on 1m close)
            if (kind == "BEAR" and fclose < tsl) or (kind == "BULL" and fclose > tsl):
                tsl_p, tsl_ts, exit2_reason = fclose, fts, "T1+TSL"
                break

            # Zone-based TSL — full 5-step cycle:
            # ① SELLERS_IN  ② TRAPPED  ③ ENTRY_READY  ④ price re-hits C1.high
            # → ONLY at step ④ TSL jumps to zone_low (C2.low)
            # pending_tsl_zones: zones at ENTRY_READY, waiting for price to re-hit t1
            # Only rebuild LTF bars every ltf_min rows (not every 1m bar) — O(n) not O(n²)
            if len(acc_rows) >= ltf_min * 2 and len(acc_rows) % ltf_min == 0:
                try:
                    acc_df   = pd.DataFrame(acc_rows)
                    ltf_bars = resample(acc_df, ltf_min)
                    if len(ltf_bars) >= 3:
                        for z in scan_zones_consecutive(ltf_bars):
                            if z["kind"] != kind:
                                continue
                            if z["status"] != "ENTRY_READY":
                                continue
                            # Zone must be above our entry
                            if kind == "BEAR" and z["zone_low"] <= entry_p:
                                continue
                            if kind == "BULL" and z["zone_high"] >= entry_p:
                                continue
                            uid = f"{z['zone_low']:.1f}_{z['zone_high']:.1f}"
                            if uid not in _tsl_pending:
                                _tsl_pending[uid] = z  # register for step ④ check
                except Exception:
                    pass

            # Step ④: for each pending zone, check if price re-hit t1 (C1.high for BEAR)
            for uid, z in list(_tsl_pending.items()):
                z_t1 = z.get("t1", 0)
                re_hit = (fhi >= z_t1) if kind == "BEAR" else (flo <= z_t1)
                if re_hit:
                    new_tsl = z["zone_low"] if kind == "BEAR" else z["zone_high"]
                    if kind == "BEAR" and new_tsl > tsl:
                        tsl = new_tsl
                    elif kind == "BULL" and new_tsl < tsl:
                        tsl = new_tsl
                    del _tsl_pending[uid]
        else:
            ob = today_df[today_df["datetime"] <= sq_time]
            tsl_p     = float(ob.iloc[-1]["close"]) if not ob.empty else t1_price
            tsl_ts    = sq_time
            exit2_reason = "T1+EOD"

        return _exit_result(exit2_reason, entry_p, t1_price, t1_ts, tsl_p, tsl_ts, lot_size, kind, t2_p)

    def _exit_result(reason, entry_p, exit1_p, exit1_ts, exit2_p, exit2_ts, lot_size, kind, t2_p=None):
        """
        P&L calc for exits:
          SL           → 100% at exit1_p
          T1+EOD/TSL   → 50% at T1, 50% at TSL/EOD
          T1+T2+TSL    → 50% at T1, 25% at T2, 25% at TSL (exit2_p=TSL here)
        """
        half = lot_size // 2
        rest = lot_size - half

        def _pts(ep, xp):
            return (xp - ep) if kind == "BEAR" else (ep - xp)

        if reason == "SL":
            pts1 = _pts(entry_p, exit1_p)
            return {
                "reason":   "SL",
                "exit1_p":  round(exit1_p, 1),  "exit1_ts": _ts_fmt(exit1_ts),
                "exit2_p":  None,                "exit2_ts": None,
                "pnl_pts":  round(pts1, 1),
                "pnl_rs":   round(pts1 * lot_size, 0),
                "half1_rs": round(pts1 * lot_size, 0),
                "half2_rs": 0,
            }

        pts1 = _pts(entry_p, exit1_p)   # T1 exit (50%)
        pts2 = _pts(entry_p, exit2_p) if exit2_p else 0  # TSL/EOD exit

        total_rs = round(pts1 * half + pts2 * rest, 0)
        return {
            "reason":   reason,
            "exit1_p":  round(exit1_p, 1),  "exit1_ts": _ts_fmt(exit1_ts),
            "exit2_p":  round(exit2_p, 1) if exit2_p else None,
            "exit2_ts": _ts_fmt(exit2_ts) if exit2_ts else None,
            "pnl_pts":  round((pts1 * half + pts2 * rest) / lot_size, 1) if lot_size else 0,
            "pnl_rs":   int(total_rs),
            "half1_rs": round(pts1 * half, 0),
            "half2_rs": round(pts2 * rest, 0),
        }

    def _record(kind, zone, zone_src, touch_ts, entry_ts, entry_p,
                sl_p, t1_p, t2_p, ltf_label, trap_label, combo, ltf_min):
        ex = _simulate_exit(entry_ts, entry_p, sl_p, t1_p, t2_p, kind, ltf_min)
        trades.append({
            "date":        trade_date,
            "htf_touch":   _ts_fmt(touch_ts),
            "entry_ts":    _ts_fmt(entry_ts),
            "exit1_ts":    ex["exit1_ts"],
            "exit2_ts":    ex.get("exit2_ts") or "",
            "direction":   "CE" if kind == "BEAR" else "PE",
            "kind":        kind,
            "zone_low":    round(zone.get("zone_low", 0), 1),
            "zone_high":   round(zone.get("zone_high", 0), 1),
            "zone":        f"{zone.get('zone_low',0):.0f}→{zone.get('zone_high',0):.0f}",
            "zone_src":    zone_src,
            "gap":         gap_label,
            "entry":       round(entry_p, 1),
            "exit1_p":     ex["exit1_p"],
            "exit2_p":     ex.get("exit2_p") or "",
            "sl":          round(sl_p, 1),
            "t1":          round(t1_p, 1),
            "t2":          round(t2_p, 1) if t2_p else "",
            "reason":      ex["reason"],
            "half1_rs":    ex["half1_rs"],
            "half2_rs":    ex["half2_rs"],
            "ltf":         ltf_label,
            "trap_entry":  trap_label,
            "pnl_pts":     ex["pnl_pts"],
            "pnl_rs":      ex["pnl_rs"],
            "combo":       f"{combo}+{ltf_label}+{trap_label}",
            "ltf_mode":    ltf_source,
            # ── Trap timeline (for chart verification) ──────────────────
            "htf_ref_dt":         _ts_fmt(zone.get("ref_dt")),
            "htf_sellers_in_dt":  _ts_fmt(zone.get("sellers_in_dt")),
            "htf_trapped_dt":     _ts_fmt(zone.get("trapped_dt")),
            "htf_entry_ready_dt": _ts_fmt(zone.get("entry_ready_dt")),
        })

    # Track which (kind, zone_uid, ltf, trap) combos already recorded
    recorded: set = set()

    def _run_direction(kind: str):
        # Gap direction filter: on gap days only trade WITH the gap direction
        # GAP DOWN → only PE (bearish), GAP UP → only CE (bullish)
        if gap_dir_filter and has_gap:
            gap_dir = gap_info.get("direction", "NONE")
            if gap_dir == "DOWN" and kind == "BEAR":   # BEAR=CE=bullish → skip on down gap
                return
            if gap_dir == "UP" and kind == "BULL":     # BULL=PE=bearish → skip on up gap
                return

        htf_bars = resample(today_df, htf_min_cascade)

        for _, row in htf_bars.iterrows():
            bar_ts  = row["datetime"]
            cur_fut = float(row["close"])
            if bar_ts < entry_open or bar_ts >= sq_time:
                continue

            # ── Zone selection ─────────────────────────────────────────────
            if use_cascade_only:
                # Gap-through: only use intraday zones forming today
                live_zones = _get_combined_zones_at(pd.DataFrame(), today_df, bar_ts, htf_min_cascade, min_width, max_age_days, trade_date)
                zone_src   = "CASCADE"
            else:
                # Combined: 10-day lookback + today intraday
                live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_zone, min_width, max_age_days, trade_date)
                if not live_zones:
                    live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_cascade, min_width, max_age_days, trade_date)
                zone_src = "HTF" if has_htf_zone else "CASCADE"

            kind_zones = _zones_for_direction(live_zones, kind)
            active = [z for z in kind_zones if _price_near_zone(cur_fut, z)]
            if not active:
                continue

            zone = min(active, key=lambda z: abs(cur_fut - z.get("zone_low", cur_fut)))
            zone_uid = f"{kind}_{zone.get('zone_low',0):.0f}_{zone.get('zone_high',0):.0f}"
            sl_p = round(zone["zone_low"] - sl_buf, 1) if kind == "BEAR" \
                   else round(zone["zone_high"] + sl_buf, 1)
            # T1 = C1.high for BEAR (sellers' SL), C1.low for BULL (buyers' SL)
            t1_p = zone.get("t1", zone["zone_high"] if kind == "BEAR" else zone["zone_low"])

            # T2 = t1 of the next higher HTF zone above this zone's T1 (if any)
            # Gives the runner an extended target before TSL takes over
            t2_p = None
            for hz in kind_zones:
                if hz is zone:
                    continue
                hz_t1 = hz.get("t1", 0)
                if kind == "BEAR" and hz_t1 > t1_p:
                    if t2_p is None or hz_t1 < t2_p:
                        t2_p = hz_t1
                elif kind == "BULL" and hz_t1 < t1_p:
                    if t2_p is None or hz_t1 > t2_p:
                        t2_p = hz_t1

            # ── LTF sub-trap comparison ────────────────────────────────────
            # Logic 2: fetch deep ITM option bars for LTF detection
            # CE=BEAR direction: deep ITM CE = strike BELOW futures (put-call reversal: CE ITM when strike < spot)
            # PE=BULL direction: deep ITM PE = strike ABOVE futures
            if ltf_source == "option":
                trade_dt_obj = date.fromisoformat(trade_date)
                if kind == "BEAR":
                    det_strike = int(round((cur_fut - itm_offset) / CRUDE_STEP) * CRUDE_STEP)
                    opt_key    = _find_crude_option_key(det_strike, "CE", trade_dt_obj)
                else:
                    det_strike = int(round((cur_fut + itm_offset) / CRUDE_STEP) * CRUDE_STEP)
                    opt_key    = _find_crude_option_key(det_strike, "PE", trade_dt_obj)
                opt_df = fetch_1m(opt_key, trade_date) if opt_key else pd.DataFrame()
                ltf_src_df = opt_df if not opt_df.empty else today_df
            else:
                ltf_src_df = today_df

            for ltf_min in ltf_minutes:
                ltf_label = f"LTF{ltf_min}"

                # LTF scan starts from HTF TRAPPED time (catches sub-traps before ENTRY_READY)
                ltf_scan_start = zone.get("trapped_dt") or bar_ts
                opp_kind = "BULL" if kind == "BEAR" else "BEAR"

                # ── Same-direction sub-traps ───────────────────────────────
                same_traps = _find_ltf_trap_entries(
                    ltf_src_df, zone, ltf_scan_start, kind, sq_time, ltf_min
                )
                # ── Opposite-direction sub-traps (wide buffer above/below zone) ─
                # e.g. BULL sub-trap within BEAR zone = last buyers trapped → PE
                opp_traps = _find_ltf_trap_entries(
                    ltf_src_df, zone, ltf_scan_start, opp_kind, sq_time, ltf_min,
                    wide_zone=True
                )

                # Build IMMEDIATE + same-dir FIRST/LAST
                same_variants: dict[str, dict | None] = {"IMMEDIATE": None}
                if same_traps:
                    same_variants["FIRST"] = same_traps[0]
                    same_variants["LAST"]  = same_traps[-1]
                    if len(same_traps) >= 3:
                        same_variants["MIDDLE"] = same_traps[len(same_traps) // 2]

                # Opposite-dir FIRST/LAST only (no IMMEDIATE)
                opp_variants: dict[str, dict] = {}
                if opp_traps:
                    opp_variants["FIRST+OPP"] = opp_traps[0]
                    opp_variants["LAST+OPP"]  = opp_traps[-1]

                # ── Process same-direction entries ─────────────────────────
                for trap_label, ltf_entry in same_variants.items():
                    ts_key = _ts_fmt(ltf_entry["ts"]) if ltf_entry else _ts_fmt(bar_ts)
                    key = (zone_uid, kind, ltf_label, trap_label, ts_key)
                    if key in recorded:
                        continue
                    recorded.add(key)

                    if ltf_entry is None:
                        ob = today_df[today_df["datetime"] <= bar_ts]
                        ep = float(ob.iloc[-1]["close"]) if not ob.empty else 0.0
                        if ep <= 0:
                            continue
                        zl, zh = zone["zone_low"], zone["zone_high"]
                        if not (zl <= ep <= zh):
                            continue
                        if kind == "BEAR" and (sl_p >= ep or t1_p <= ep):
                            continue
                        if kind == "BULL" and (sl_p <= ep or t1_p >= ep):
                            continue
                        _record(kind, zone, zone_src, bar_ts, bar_ts, ep,
                                sl_p, t1_p, t2_p, ltf_label, trap_label, combo_base, ltf_min)
                    else:
                        entry_ts = ltf_entry["ts"]
                        ep       = ltf_entry["price"]
                        if ep <= 0 or entry_ts >= sq_time:
                            continue
                        zl, zh = zone["zone_low"], zone["zone_high"]
                        if not (zl <= ep <= zh):
                            continue
                        if kind == "BEAR" and (sl_p >= ep or t1_p <= ep):
                            continue
                        if kind == "BULL" and (sl_p <= ep or t1_p >= ep):
                            continue
                        _record(kind, zone, zone_src, bar_ts, entry_ts, ep,
                                sl_p, t1_p, t2_p, ltf_label, trap_label, combo_base, ltf_min)

                # ── Process opposite-direction entries ─────────────────────
                for trap_label, ltf_entry in opp_variants.items():
                    entry_ts = ltf_entry["ts"]
                    ts_key   = _ts_fmt(entry_ts)
                    key = (zone_uid, opp_kind, ltf_label, trap_label, ts_key)
                    if key in recorded:
                        continue
                    recorded.add(key)

                    ep = ltf_entry["price"]
                    if ep <= 0 or entry_ts >= sq_time:
                        continue

                    # SL/T1 come from the sub-trap's own zone boundaries
                    sub_t1 = ltf_entry.get("t1", 0)
                    if sub_t1 <= 0:
                        continue
                    sub_zone_str = ltf_entry.get("zone", "0→0")
                    try:
                        sub_zl = float(sub_zone_str.split("→")[0])
                        sub_zh = float(sub_zone_str.split("→")[1])
                    except Exception:
                        continue

                    if opp_kind == "BEAR":
                        # 5min sellers trapped → CE (buy)
                        sub_sl = round(sub_zl - sl_buf, 1)
                        if sub_sl >= ep or sub_t1 <= ep:
                            continue
                    else:
                        # 5min buyers trapped → PE (sell)
                        sub_sl = round(sub_zh + sl_buf, 1)
                        if sub_sl <= ep or sub_t1 >= ep:
                            continue

                    _record(opp_kind, zone, zone_src, bar_ts, entry_ts, ep,
                            sub_sl, sub_t1, t2_p, ltf_label, trap_label, combo_base, ltf_min)

    _run_direction("BEAR")
    _run_direction("BULL")
    return trades


def _record_exit(trade: dict, exit_p: float, exit_ts, reason: str,
                 out: list, lot_size: int, combo: str):
    # Legacy helper kept for compatibility — not used by new _run_day
    ep   = trade["entry"]
    kind = trade["kind"]
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
        "ltf":       "",
        "trap_entry": "",
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
    min_width    = float(params.get("min_zone_width", 30.0))
    max_age_days = int(params.get("max_zone_age_days", 5))
    ltf_source      = str(params.get("ltf_source", "futures"))
    itm_offset      = int(params.get("itm_offset", 300))
    gap_dir_filter  = bool(params.get("gap_dir_filter", True))
    require_gap     = bool(params.get("require_gap", True))
    ltf_minutes     = [5, 30]
    lot_size        = CRUDE_LOT * lots

    LOOKBACK_DAYS = 10

    # Support explicit date range (start_date / end_date) for single-day drill-down
    start_date = params.get("start_date", "")
    end_date   = params.get("end_date", "")
    if start_date and end_date:
        sd, ed = date.fromisoformat(start_date), date.fromisoformat(end_date)
        trading_days = []
        d = sd
        while d <= ed:
            if d.weekday() < 5:
                trading_days.append(d.isoformat())
            d += timedelta(days=1)
    else:
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
            htf_zone, htf_cascade, sl_buf, gap_thr, combo_filter, lot_size,
            ltf_minutes, min_width, max_age_days, ltf_source, itm_offset, gap_dir_filter, require_gap
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

    # Dynamic combo keys — includes LTF+trap variants
    # Ordered: base combos first, then LTF breakdown
    base_combos = ["GAP+HTF_ZONE", "GAP+NO_ZONE", "NO_GAP+HTF_ZONE", "NO_GAP+NO_ZONE"]
    seen_combos = []
    for t in all_trades:
        if t["combo"] not in seen_combos:
            seen_combos.append(t["combo"])
    # Sort: base combos first (no LTF suffix), then LTF variants alphabetically
    all_combos = sorted(seen_combos, key=lambda c: (
        0 if not any(x in c for x in ["LTF", "FIRST", "MIDDLE", "LAST", "IMMEDIATE"]) else 1,
        c
    ))
    by_combo = {c: _stats([t for t in all_trades if t["combo"] == c]) for c in all_combos}

    # Equity curve: cumulative P&L per trade in time order
    equity = []
    cum = 0
    for t in all_trades:
        cum += t["pnl_rs"]
        equity.append({"ts": f"{t['date']} {t.get('exit2_ts') or t.get('exit1_ts','')}", "cum": cum})

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


def run_batch_backtest(params: dict, token: str, widths: list[float] = None) -> dict:
    """
    Run backtest once with shared data fetching, then apply each zone width filter.
    Returns all width results from a single set of Upstox API calls.
    """
    if widths is None:
        widths = [10.0, 20.0, 30.0, 40.0, 50.0]

    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    days         = int(params.get("days", 7))
    lots         = int(params.get("lots", 2))
    gap_thr      = float(params.get("gap_threshold", GAP_PCT))
    htf_zone     = int(params.get("htf_min_zone", 60))
    htf_cascade  = int(params.get("htf_min_cascade", 30))
    sl_buf       = float(params.get("sl_buf", SL_BUF))
    combo_filter = "all"
    fut_key      = str(params.get("fut_key", "MCX_FO|520702"))
    max_age_days = int(params.get("max_zone_age_days", 5))
    ltf_source      = str(params.get("ltf_source", "futures"))
    itm_offset      = int(params.get("itm_offset", 300))
    gap_dir_filter  = bool(params.get("gap_dir_filter", True))
    require_gap     = bool(params.get("require_gap", True))
    ltf_minutes     = [5, 30]
    lot_size        = CRUDE_LOT * lots
    LOOKBACK_DAYS   = 10

    start_date = params.get("start_date", "")
    end_date   = params.get("end_date", "")
    if start_date and end_date:
        sd, ed = date.fromisoformat(start_date), date.fromisoformat(end_date)
        trading_days = []
        d = sd
        while d <= ed:
            if d.weekday() < 5:
                trading_days.append(d.isoformat())
            d += timedelta(days=1)
    else:
        trading_days = get_trading_days(days)

    def _lookback_dates(trade_dt: str, n: int) -> list[str]:
        result = []
        d = date.fromisoformat(trade_dt) - timedelta(days=1)
        while len(result) < n:
            if d.weekday() < 5:
                result.append(d.isoformat())
            d -= timedelta(days=1)
        return list(reversed(result))

    # ── Fetch all data ONCE ──────────────────────────────────────────────────
    day_data: list[tuple] = []   # (trade_date, today_df, lookback_df)
    for trade_date in trading_days:
        today_df = fetch_1m(fut_key, trade_date)
        time.sleep(0.3)
        if today_df.empty:
            continue
        lb_dates = _lookback_dates(trade_date, LOOKBACK_DAYS)
        lb_frames = []
        for lb_dt in lb_dates:
            df = fetch_1m(fut_key, lb_dt)
            time.sleep(0.2)
            if not df.empty:
                lb_frames.append(df)
        lookback_df = pd.concat(lb_frames, ignore_index=True).sort_values("datetime").reset_index(drop=True) \
                      if lb_frames else pd.DataFrame()
        day_data.append((trade_date, today_df, lookback_df))

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

    # ── Apply each width on the cached data ─────────────────────────────────
    period = f"{trading_days[0]} → {trading_days[-1]}" if trading_days else ""
    results: list[dict] = []
    for w in widths:
        all_trades: list[dict] = []
        for (trade_date, today_df, lookback_df) in day_data:
            day_trades = _run_day(
                trade_date, today_df, lookback_df,
                htf_zone, htf_cascade, sl_buf, gap_thr, combo_filter, lot_size,
                ltf_minutes, min_width=w, max_age_days=max_age_days,
                ltf_source=ltf_source, itm_offset=itm_offset, gap_dir_filter=gap_dir_filter
            )
            all_trades.extend(day_trades)

        seen: list[str] = []
        for t in all_trades:
            if t["combo"] not in seen:
                seen.append(t["combo"])
        all_combos = sorted(seen, key=lambda c: (
            0 if not any(x in c for x in ["LTF","FIRST","MIDDLE","LAST","IMMEDIATE"]) else 1, c
        ))
        by_combo = {c: _stats([t for t in all_trades if t["combo"] == c]) for c in all_combos}

        equity = []
        cum = 0
        for t in all_trades:
            cum += t["pnl_rs"]
            equity.append({"ts": f"{t['date']} {t.get('exit2_ts') or t.get('exit1_ts','')}", "cum": cum})

        results.append({
            "width":    w,
            "summary":  _stats(all_trades),
            "by_combo": by_combo,
            "trades":   all_trades,
            "equity":   equity,
            "period":   period,
        })

    return {"ok": True, "period": period, "results": results}


def run_zone_debug(params: dict, token: str) -> dict:
    """
    Show all BEAR and BULL trap zones from the 10-day lookback using the
    correct 2-candle consecutive algorithm. Shows each lifecycle event with
    exact timestamp.
    """
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    trade_date   = params.get("trade_date", "")
    htf_min      = int(params.get("htf_min_zone", 60))
    fut_key      = str(params.get("fut_key", "MCX_FO|520702"))
    min_width    = float(params.get("min_zone_width", 30.0))
    max_age_days = int(params.get("max_zone_age_days", 0))   # 0 = show all in debug
    LOOKBACK_DAYS = 10

    if not trade_date:
        return {"ok": False, "error": "trade_date required"}

    def _lookback_dates(td: str, n: int) -> list[str]:
        result = []
        d = date.fromisoformat(td) - timedelta(days=1)
        while len(result) < n:
            if d.weekday() < 5:
                result.append(d.isoformat())
            d -= timedelta(days=1)
        return list(reversed(result))

    lb_dates = _lookback_dates(trade_date, LOOKBACK_DAYS)
    lb_frames = []
    for lb_dt in lb_dates:
        df = fetch_1m(fut_key, lb_dt)
        time.sleep(0.2)
        if not df.empty:
            lb_frames.append(df)

    if not lb_frames:
        return {"ok": False, "error": "No lookback data"}

    lookback_df = pd.concat(lb_frames, ignore_index=True).sort_values("datetime").reset_index(drop=True)
    htf = resample(lookback_df, htf_min)

    def _fmt(ts) -> str:
        if ts is None:
            return "-"
        return ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]

    cutoff = pd.Timestamp(trade_date) - pd.Timedelta(days=max_age_days) if max_age_days > 0 else None
    all_zones = [z for z in scan_zones_consecutive(htf)
                 if (z["zone_high"] - z["zone_low"]) >= min_width
                 and (cutoff is None or pd.Timestamp(z["ref_dt"]) >= cutoff)]

    zone_rows = []
    for z in all_zones:
        zone_rows.append({
            "kind":          z["kind"],
            "zone_low":      round(z["zone_low"], 1),
            "zone_high":     round(z["zone_high"], 1),
            "t1":            round(z["t1"], 1),
            "ref_candle":    _fmt(z["ref_dt"]),          # ① C1 — reference candle
            "sellers_in":    _fmt(z["sellers_in_dt"]),   # ② C2 — next candle breaks C1 low
            "trapped":       _fmt(z.get("trapped_dt")),  # ③ price breaks C1 high
            "entry_ready":   _fmt(z.get("entry_ready_dt")), # ④ price returns to zone
            "status":        z["status"],
        })

    zone_rows.sort(key=lambda r: r["ref_candle"])

    return {
        "ok":         True,
        "trade_date": trade_date,
        "lookback":   lb_dates,
        "htf_min":    htf_min,
        "zones":      zone_rows,
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
        print(f"  {t['date']} {t['entry_ts']}→{t.get('exit1_ts','')}  {t['direction']}  "
              f"entry={t['entry']}  exit={t.get('exit1_p','')}  {t['reason']}  "
              f"Rs{t['pnl_rs']:+.0f}  [{t['combo']}]")
