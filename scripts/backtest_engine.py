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
) -> list[dict]:
    """
    After price enters an HTF zone at touch_ts, scan LTF bars within the zone
    for sub-traps. Returns list of {ts, price} for each sub-trap completion,
    sorted by time. Caller picks first/middle/last for entry comparison.

    direction: "BEAR" (expecting bounce up) or "BULL" (expecting drop down)
    """
    zl = htf_zone.get("zone_low", 0)
    zh = htf_zone.get("zone_high", 0)
    # Use a slightly wider window (zone ± 1%) to catch LTF traps near boundaries
    buf = (zh - zl) * 0.15
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

    try:
        _, zones = scanner.scan_htf_spot(ltf)
    except Exception:
        return []

    # Sub-traps of same direction within this zone
    sub_traps = [z for z in zones
                 if z.get("kind") == direction and z.get("status") == "TRAPPED"]

    entries = []
    for z in sub_traps:
        # Entry timestamp = sub-zone zone_high if BEAR (price broke above = sellers trapped)
        # Use the zone's sl field timestamp if available, else approximate from LTF bars
        # We use the bar closest to the sub-zone high/low as entry point
        sub_high = z.get("zone_high", zh)
        sub_low  = z.get("zone_low", zl)
        # Find first LTF bar where price re-enters the sub-zone after being above (BEAR) or below (BULL)
        if direction == "BEAR":
            # Sellers trapped above sub_high — entry when price returns to sub_high area
            match = ltf[ltf["close"] <= sub_high]
        else:
            match = ltf[ltf["close"] >= sub_low]
        if match.empty:
            continue
        entry_bar = match.iloc[-1]
        entries.append({
            "ts":    entry_bar["datetime"],
            "price": float(entry_bar["close"]),
            "zone":  f"{sub_low:.0f}→{sub_high:.0f}",
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
    htf_zones_hist = _get_prev_day_htf_zones(lookback_df, htf_min_zone)

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
    # If gap day and no reachable historical zones → intraday cascade only
    use_cascade_only = has_gap and len(reachable_hist) == 0
    has_htf_zone     = len(reachable_hist) > 0

    combo_base = _combo_label(has_gap, has_htf_zone)

    if combo_filter != "all":
        wanted = combo_filter.upper().replace("-", "_")
        if combo_base.upper() != wanted:
            return trades

    sq_time    = pd.Timestamp(f"{trade_date} {SQ_OFF}").tz_localize(None)
    entry_open = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}").tz_localize(None)
    gap_label  = f"{gap_info['direction']} {gap_info['pct']:+.2f}%" if has_gap else "none"

    def _simulate_exit(entry_ts, entry_p, sl_p, t1_p, kind, ltf_min):
        """
        Two-stage exit:
        Stage 1 (full qty): scan 1m bars for SL or T1.
          - SL hit → exit 100%, done.
          - T1 hit → exit 50% at T1 price, move to Stage 2.
          - EOD    → exit 100% at close.

        Stage 2 (remaining 50%): TSL on LTF candle lows (BEAR) / highs (BULL).
          - TSL = trailing LTF candle low/high, stepped in our direction.
          - Exit when price breaks TSL or EOD.

        Returns dict with full exit details for both halves.
        """
        fwd = today_df[today_df["datetime"] > entry_ts]

        # ── Stage 1: SL or T1 ──────────────────────────────────────────────
        t1_price, t1_ts = None, None
        for _, fb in fwd.iterrows():
            flo, fhi, fts = float(fb["low"]), float(fb["high"]), fb["datetime"]
            if fts >= sq_time:
                ob = today_df[today_df["datetime"] <= fts]
                ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
                return _exit_result("EOD+EOD", entry_p, ep, fts, ep, fts, lot_size, kind)
            # SL check first (priority)
            slh = (flo <= sl_p) if kind == "BEAR" else (fhi >= sl_p)
            if slh:
                ob = today_df[today_df["datetime"] <= fts]
                ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
                return _exit_result("SL", entry_p, ep, fts, None, None, lot_size, kind)
            # T1 check
            t1h = (fhi >= t1_p) if kind == "BEAR" else (flo <= t1_p)
            if t1h:
                ob = today_df[today_df["datetime"] <= fts]
                t1_price = float(ob.iloc[-1]["close"]) if not ob.empty else t1_p
                t1_ts    = fts
                break

        if t1_price is None:
            # Never hit T1 — EOD on full qty
            ob = today_df[today_df["datetime"] <= sq_time]
            ep = float(ob.iloc[-1]["close"]) if not ob.empty else entry_p
            return _exit_result("EOD+EOD", entry_p, ep, sq_time, ep, sq_time, lot_size, kind)

        # ── Stage 2: Trap-based TSL for remaining 50% ─────────────────────
        # TSL only moves when a NEW LTF trap of same kind completes:
        #   BEAR trade: LTF bears enter (zone_low) → get trapped → SL hit (zone_high)
        #               → our TSL jumps to zone_low of that trap
        #   BULL trade: LTF bulls enter (zone_high) → get trapped → SL hit (zone_low)
        #               → our TSL jumps to zone_high of that trap
        #
        # TSL starts at entry_p (break-even) and only moves in our favour.
        # Exit when 1m close breaks below TSL (BEAR) or above TSL (BULL).

        after_t1  = today_df[today_df["datetime"] > t1_ts]
        tsl       = entry_p          # current TSL level
        tsl_p     = t1_price
        tsl_ts    = sq_time
        pending_traps: list[dict] = []   # LTF traps detected, waiting for SL clear

        if after_t1.empty:
            ob = today_df[today_df["datetime"] <= sq_time]
            tsl_p = float(ob.iloc[-1]["close"]) if not ob.empty else t1_price
            return _exit_result("T1+EOD", entry_p, t1_price, t1_ts, tsl_p, sq_time, lot_size, kind)

        ltf_after = resample(after_t1, ltf_min)

        # Incrementally build LTF bars, scan for traps each step
        accumulated = []
        for _, lb in ltf_after.iterrows():
            lts    = lb["datetime"]
            lclose = float(lb["close"])
            llow   = float(lb["low"])
            lhigh  = float(lb["high"])
            accumulated.append(lb)

            if lts >= sq_time:
                ob = today_df[today_df["datetime"] <= lts]
                tsl_p  = float(ob.iloc[-1]["close"]) if not ob.empty else lclose
                tsl_ts = lts
                break

            # Check if current price breaks TSL
            if kind == "BEAR" and lclose < tsl:
                tsl_p, tsl_ts = lclose, lts
                break
            if kind == "BULL" and lclose > tsl:
                tsl_p, tsl_ts = lclose, lts
                break

            # Scan accumulated LTF bars for new same-kind traps
            # TSL moves only at ENTRY_READY — full cycle:
            #   BEAR long:  sellers in (< zone_low) → trapped (> zone_high)
            #               → price retests zone_low → bounces up → ENTRY_READY
            #               → TSL = zone_low (sellers' entry = now confirmed support)
            #   BULL short: buyers in (> zone_high) → trapped (< zone_low)
            #               → price retests zone_high → drops → ENTRY_READY
            #               → TSL = zone_high (buyers' entry = now confirmed resistance)
            if len(accumulated) >= 2:
                try:
                    ltf_df = pd.DataFrame(accumulated)
                    _, zones = scanner.scan_htf_spot(ltf_df)
                    for z in zones:
                        if z.get("kind") != kind:
                            continue
                        zl, zh = z.get("zone_low", 0), z.get("zone_high", 0)
                        uid    = f"{zl:.0f}_{zh:.0f}"
                        status = z.get("status", "")

                        # Register new zones
                        existing = next((p for p in pending_traps if p["uid"] == uid), None)
                        if not existing:
                            if status in ("ACTIVE", "TRAPPED", "ENTRY_READY"):
                                pending_traps.append({
                                    "uid": uid, "zone_low": zl, "zone_high": zh,
                                    "status": status
                                })
                        else:
                            existing["status"] = status   # update state

                    # Check for ENTRY_READY — retest confirmed, move TSL
                    for pt in list(pending_traps):
                        if pt["status"] == "ENTRY_READY":
                            # BEAR long: TSL = zone_low (sellers' entry = support after retest)
                            # BULL short: TSL = zone_high (buyers' entry = resistance after retest)
                            new_tsl = pt["zone_low"] if kind == "BEAR" else pt["zone_high"]
                            if kind == "BEAR" and new_tsl > tsl:
                                tsl = new_tsl
                            elif kind == "BULL" and new_tsl < tsl:
                                tsl = new_tsl
                            pending_traps.remove(pt)
                except Exception:
                    pass
        else:
            ob = today_df[today_df["datetime"] <= sq_time]
            tsl_p  = float(ob.iloc[-1]["close"]) if not ob.empty else t1_price
            tsl_ts = sq_time

        return _exit_result("T1+TSL", entry_p, t1_price, t1_ts, tsl_p, tsl_ts, lot_size, kind)

    def _exit_result(reason, entry_p, exit1_p, exit1_ts, exit2_p, exit2_ts, lot_size, kind):
        """Compute combined P&L for two-stage exit (50%+50% or 100%+0)."""
        half = lot_size // 2
        rest = lot_size - half

        if reason == "SL":
            pts1 = (exit1_p - entry_p) if kind == "BEAR" else (entry_p - exit1_p)
            return {
                "reason":     "SL",
                "exit1_p":    round(exit1_p, 1),
                "exit1_ts":   _ts_fmt(exit1_ts),
                "exit2_p":    None,
                "exit2_ts":   None,
                "pnl_pts":    round(pts1, 1),
                "pnl_rs":     round(pts1 * lot_size, 0),
                "half1_rs":   round(pts1 * lot_size, 0),
                "half2_rs":   0,
            }
        pts1 = (exit1_p - entry_p) if kind == "BEAR" else (entry_p - exit1_p)
        pts2 = (exit2_p - entry_p) if (exit2_p and kind == "BEAR") else \
               ((entry_p - exit2_p) if exit2_p else 0)
        total_rs = round(pts1 * half + pts2 * rest, 0)
        return {
            "reason":   reason,
            "exit1_p":  round(exit1_p, 1),
            "exit1_ts": _ts_fmt(exit1_ts),
            "exit2_p":  round(exit2_p, 1) if exit2_p else None,
            "exit2_ts": _ts_fmt(exit2_ts) if exit2_ts else None,
            "pnl_pts":  round((pts1 * half + pts2 * rest) / lot_size, 1) if lot_size else 0,
            "pnl_rs":   int(total_rs),
            "half1_rs": round(pts1 * half, 0),
            "half2_rs": round(pts2 * rest, 0),
        }

    def _record(kind, zone, zone_src, touch_ts, entry_ts, entry_p,
                sl_p, t1_p, ltf_label, trap_label, combo, ltf_min):
        ex = _simulate_exit(entry_ts, entry_p, sl_p, t1_p, kind, ltf_min)
        trades.append({
            "date":        trade_date,
            "htf_touch":   _ts_fmt(touch_ts),
            "entry_ts":    _ts_fmt(entry_ts),
            "exit1_ts":    ex["exit1_ts"],           # T1 / SL / EOD exit time
            "exit2_ts":    ex.get("exit2_ts") or "", # TSL exit time (if T1 hit)
            "direction":   "CE" if kind == "BEAR" else "PE",
            "kind":        kind,
            "zone_low":    round(zone.get("zone_low", 0), 1),
            "zone_high":   round(zone.get("zone_high", 0), 1),
            "zone":        f"{zone.get('zone_low',0):.0f}→{zone.get('zone_high',0):.0f}",
            "zone_src":    zone_src,
            "gap":         gap_label,
            "entry":       round(entry_p, 1),
            "exit1_p":     ex["exit1_p"],            # price at T1/SL/EOD
            "exit2_p":     ex.get("exit2_p") or "",  # price at TSL exit
            "sl":          round(sl_p, 1),
            "t1":          round(t1_p, 1),
            "reason":      ex["reason"],             # SL / T1+TSL / EOD+EOD
            "half1_rs":    ex["half1_rs"],           # 50% qty P&L at T1
            "half2_rs":    ex["half2_rs"],           # 50% qty P&L at TSL
            "ltf":         ltf_label,
            "trap_entry":  trap_label,
            "pnl_pts":     ex["pnl_pts"],
            "pnl_rs":      ex["pnl_rs"],
            "combo":       f"{combo}+{ltf_label}+{trap_label}",
        })

    # Track which (kind, zone_uid, ltf, trap) combos already recorded
    recorded: set = set()

    def _run_direction(kind: str):
        htf_bars = resample(today_df, htf_min_cascade)

        for _, row in htf_bars.iterrows():
            bar_ts  = row["datetime"]
            cur_fut = float(row["close"])
            if bar_ts < entry_open or bar_ts >= sq_time:
                continue

            # ── Zone selection ─────────────────────────────────────────────
            if use_cascade_only:
                # Gap-through: only use intraday zones forming today
                live_zones = _get_combined_zones_at(pd.DataFrame(), today_df, bar_ts, htf_min_cascade)
                zone_src   = "CASCADE"
            else:
                # Combined: 10-day lookback + today intraday
                live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_zone)
                if not live_zones:
                    live_zones = _get_combined_zones_at(lookback_df, today_df, bar_ts, htf_min_cascade)
                zone_src = "HTF" if has_htf_zone else "CASCADE"

            kind_zones = _zones_for_direction(live_zones, kind)
            active = [z for z in kind_zones if _price_near_zone(cur_fut, z)]
            if not active:
                continue

            zone = min(active, key=lambda z: abs(cur_fut - z.get("zone_low", cur_fut)))
            zone_uid = f"{kind}_{zone.get('zone_low',0):.0f}_{zone.get('zone_high',0):.0f}"
            sl_p = round(zone["zone_low"] - sl_buf, 1) if kind == "BEAR" \
                   else round(zone["zone_high"] + sl_buf, 1)
            # T1 = HTF zone high (BEAR: where bears' SL sat) / zone low (BULL)
            t1_p = zone["zone_high"] if kind == "BEAR" else zone["zone_low"]

            # ── LTF sub-trap comparison ────────────────────────────────────
            for ltf_min in ltf_minutes:
                ltf_label = f"LTF{ltf_min}"
                sub_traps = _find_ltf_trap_entries(
                    today_df, zone, bar_ts, kind, sq_time, ltf_min
                )

                # Variants: FIRST / MIDDLE / LAST sub-trap, and IMMEDIATE (no LTF wait)
                variants: dict[str, dict | None] = {"IMMEDIATE": None}
                if sub_traps:
                    variants["FIRST"]  = sub_traps[0]
                    variants["LAST"]   = sub_traps[-1]
                    if len(sub_traps) >= 3:
                        variants["MIDDLE"] = sub_traps[len(sub_traps) // 2]

                for trap_label, ltf_entry in variants.items():
                    key = (zone_uid, ltf_label, trap_label)
                    if key in recorded:
                        continue
                    recorded.add(key)

                    if ltf_entry is None:
                        # IMMEDIATE: enter at HTF zone touch
                        ob = today_df[today_df["datetime"] <= bar_ts]
                        ep = float(ob.iloc[-1]["close"]) if not ob.empty else 0.0
                        if ep <= 0:
                            continue
                        _record(kind, zone, zone_src, bar_ts, bar_ts, ep,
                                sl_p, t1_p, ltf_label, trap_label, combo_base, ltf_min)
                    else:
                        entry_ts = ltf_entry["ts"]
                        ep       = ltf_entry["price"]
                        if ep <= 0 or entry_ts >= sq_time:
                            continue
                        _record(kind, zone, zone_src, bar_ts, entry_ts, ep,
                                sl_p, t1_p, ltf_label, trap_label, combo_base, ltf_min)

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
    ltf_minutes  = [5, 15]   # always compare both LTF timeframes
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
            htf_zone, htf_cascade, sl_buf, gap_thr, combo_filter, lot_size,
            ltf_minutes
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
