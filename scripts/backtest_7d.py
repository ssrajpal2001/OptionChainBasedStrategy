"""
backtest_7d.py — New Logic Full Backtest
==========================================
Indices : NIFTY  (NSE, htf=75min, htf_source=option)
          SENSEX (BSE, htf=75min, htf_source=option)
          CRUDEOIL (MCX, htf=30min, htf_source=futures)

Logic (matches live engine after 2026-06-19 changes):
  1. HTF scan on OPTION bars (NIFTY/SENSEX) or FUTURES bars (CrudeOil)
  2. Full zone gate: [zone_low, zone_high] — NO 1/3 restriction
  3. GAP detection: if today open gaps >GAP_PCT from prev close → cascade mode forced
  4. CASCADE fallback: if no HTF zone within 1.5xATR → 15-min zone scan
  5. LTF 5-min inside zone → select_fresh_ltf_entry (existing + new traps)
  6. EXIT tier-1: T1 at 50% qty (HTF C1.HIGH = bears' SL)
  7. EXIT tier-2: trail remaining 50% using LTF zone-based SL (updates on each new TRAPPED zone)
  8. EXIT tier-3: EOD square-off

Usage:
  python scripts/backtest_7d.py
  python scripts/backtest_7d.py --indices SENSEX CRUDEOIL
  python scripts/backtest_7d.py --days 10 --lots 2

Update TOKEN below with your fresh Upstox access token before running.
"""

from __future__ import annotations
import argparse, gzip, json, sys, os, time
from datetime import date, timedelta
from typing import Optional
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

# ── TOKEN — update before running ────────────────────────────────────────────
TOKEN = "YOUR_UPSTOX_TOKEN_HERE"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# ── Per-index config ──────────────────────────────────────────────────────────
INDEX_CFG = {
    "NIFTY": {
        "step": 100, "lot": 75, "sl_buf": 2.0,
        "htf_min": 75, "ltf_min": 5, "cascade_min": 15,
        "spot_key": "NSE_INDEX|Nifty 50",
        "exchange": "NSE", "htf_source": "option",
        "sq_off": "15:20", "entry_open": "09:20",
        "atr_mult": 1.5, "gap_pct": 0.005,
    },
    "SENSEX": {
        "step": 100, "lot": 20, "sl_buf": 2.0,
        "htf_min": 75, "ltf_min": 5, "cascade_min": 15,
        "spot_key": "BSE_INDEX|SENSEX",
        "exchange": "BSE", "htf_source": "option",
        "sq_off": "15:25", "entry_open": "09:20",
        "atr_mult": 1.5, "gap_pct": 0.005,
    },
    "CRUDEOIL": {
        "step": 100, "lot": 100, "sl_buf": 20.0,
        "htf_min": 30, "ltf_min": 5, "cascade_min": 15,
        "spot_key": "",
        "exchange": "MCX", "htf_source": "futures",
        "sq_off": "23:00", "entry_open": "14:30",
        "atr_mult": 1.5, "gap_pct": 0.003,
        "fut_key": "MCX_FO|520702",
    },
}

# ── Instrument master cache ───────────────────────────────────────────────────
_MASTER_CACHE: dict[str, list] = {}

def _load_master(exchange: str) -> list:
    if exchange in _MASTER_CACHE:
        return _MASTER_CACHE[exchange]
    urls = {
        "NSE": "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
        "BSE": "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz",
        "MCX": "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz",
    }
    url = urls.get(exchange, "")
    if not url:
        return []
    try:
        r = requests.get(url, timeout=30)
        data = json.loads(gzip.decompress(r.content))
        _MASTER_CACHE[exchange] = data
        print(f"  [master] {exchange}: {len(data)} instruments loaded")
        return data
    except Exception as e:
        print(f"  [master] {exchange} load failed: {e}")
        return []

def find_option_key(exchange: str, underlying: str, strike: int,
                    otype: str, min_expiry: date) -> str:
    master = _load_master(exchange)
    ot = otype.upper()
    und_up = underlying.upper()
    candidates = []
    for row in master:
        itype = str(row.get("instrument_type", "")).upper()
        row_otype = itype if itype in ("CE", "PE") else str(row.get("option_type", "")).upper()
        if row_otype != ot:
            continue
        row_strike = float(row.get("strike", 0) or 0)
        if abs(row_strike - strike) > 0.5:
            continue
        exp_str = str(row.get("expiry", "") or "")
        try:
            exp_dt = date.fromisoformat(exp_str[:10])
        except Exception:
            continue
        if exp_dt < min_expiry:
            continue
        row_und = str(row.get("underlying_symbol", "") or "").upper()
        sym_str = str(row.get("tradingsymbol", "") or row.get("name", "")).upper()
        if und_up not in row_und and und_up not in sym_str:
            continue
        key = str(row.get("instrument_key", ""))
        if key:
            candidates.append((exp_dt, key))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

# ── API helpers ───────────────────────────────────────────────────────────────
def _get(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 429:
                time.sleep(2)
                continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}

def fetch_1m(key: str, dt: str, mkt_open: str = "09:00", mkt_close: str = "23:30") -> pd.DataFrame:
    enc = key.replace("|", "%7C")
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}"
    data = _get(url)
    candles = data.get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume", "oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.time >= pd.Timestamp(mkt_open).time()) &
            (df["datetime"].dt.time <= pd.Timestamp(mkt_close).time())]
    return df

def fetch_daily_ohlc(spot_key: str, trade_date: str) -> Optional[dict]:
    enc = spot_key.replace("|", "%7C")
    from_dt = (date.fromisoformat(trade_date) - timedelta(days=6)).isoformat()
    url = f"https://api.upstox.com/v2/historical-candle/{enc}/day/{trade_date}/{from_dt}"
    data = _get(url)
    candles = data.get("data", {}).get("candles", [])
    for c in candles:
        bar_dt = str(c[0])[:10]
        if bar_dt < trade_date:
            return {"date": bar_dt, "open": float(c[1]), "high": float(c[2]),
                    "low": float(c[3]), "close": float(c[4])}
    return None

def resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r

def compute_atr(htf: pd.DataFrame) -> float:
    if len(htf) < 2:
        return 0.0
    trs = []
    for i in range(1, len(htf)):
        h, l, pc = htf.iloc[i]["high"], htf.iloc[i]["low"], htf.iloc[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return float(pd.Series(trs).mean()) if trs else 0.0

# ── Strike helpers ────────────────────────────────────────────────────────────
def pivot_levels(H, L, C) -> dict:
    P = (H + L + C) / 3
    return {"r1": 2*P-L, "s1": 2*P-H}

def round_strike(price: float, step: int) -> int:
    return int(round(price / step) * step)

# ── Gap detection ─────────────────────────────────────────────────────────────
def detect_gap(spot_open: float, prev_close: float, gap_pct_threshold: float) -> dict:
    """Detect gap-up/down at market open. Returns info dict."""
    if prev_close <= 0 or spot_open <= 0:
        return {"gap": False, "direction": "NONE", "pct": 0.0}
    pct = (spot_open - prev_close) / prev_close
    if abs(pct) >= gap_pct_threshold:
        return {"gap": True, "direction": "UP" if pct > 0 else "DOWN",
                "pct": round(pct * 100, 2)}
    return {"gap": False, "direction": "NONE", "pct": round(pct * 100, 2)}

# ── HTF/cascade zone finder ───────────────────────────────────────────────────
def _find_zone(hist_1m: pd.DataFrame, htf_min: int, cascade_min: int,
               atr_mult: float, cur_ltp: float, scan_fn, force_cascade: bool
               ) -> tuple[Optional[dict], str]:
    """
    Returns (best_zone, mode_label).
    HTF first; if no zone within 1.5xATR or force_cascade → try cascade (15-min).
    """
    if not force_cascade and len(hist_1m) >= htf_min * 2:
        htf = resample(hist_1m, htf_min)
        if len(htf) >= 2:
            _, entries = scan_fn(htf)
            trapped = [e for e in entries if e["status"] == "TRAPPED"]
            if trapped:
                atr = compute_atr(htf)
                threshold = max(atr_mult * atr, 1.0)
                reachable = [e for e in trapped
                             if abs(cur_ltp - e.get("zone_trigger", e["zone_low"])) <= threshold]
                if reachable:
                    zone = min(reachable,
                               key=lambda e: abs(cur_ltp - e.get("zone_trigger", e["zone_low"])))
                    return zone, "HTF"

    # Cascade fallback
    if len(hist_1m) >= cascade_min * 2:
        casc = resample(hist_1m, cascade_min)
        if len(casc) >= 2:
            _, c_entries = scan_fn(casc)
            trapped_c = [e for e in c_entries if e["status"] == "TRAPPED"]
            if trapped_c:
                atr_c = compute_atr(casc)
                threshold_c = max(atr_mult * atr_c, 1.0)
                reachable_c = [e for e in trapped_c
                               if abs(cur_ltp - e.get("zone_trigger", e["zone_low"])) <= threshold_c]
                if reachable_c:
                    zone = min(reachable_c,
                               key=lambda e: abs(cur_ltp - e.get("zone_trigger", e["zone_low"])))
                    return zone, "CASCADE-15m"

    return None, "NONE"

# ── Trail SL updater (post-T1 LTF zone-based) ─────────────────────────────────
def _update_trail_sl(current_sl: float, ltf_entries: list, opt_type: str) -> float:
    """
    After T1 hit: trail SL using newly TRAPPED LTF zones.
    CE: move trail_sl UP to lowest trapped zone_low (protects profits as price rises)
    PE: move trail_sl DOWN to highest trapped zone_high
    """
    trapped = [e for e in ltf_entries if e["status"] == "TRAPPED"]
    if not trapped:
        return current_sl
    if opt_type == "CE":
        new_sl = max(current_sl, min(e["zone_low"] for e in trapped))
    else:
        new_sl = min(current_sl, max(e["zone_high"] for e in trapped))
    return new_sl

# ── Exit recorder ─────────────────────────────────────────────────────────────
def _record_exit(trade: dict, exit_price: float, exit_ts, reason: str, out: list):
    ep  = trade["entry_price"]
    t1  = trade["t1_price"]
    opt = trade["opt_type"]
    if trade["t1_hit"]:
        # 50% at T1, 50% at exit_price
        t1_pnl   = (t1 - ep) if opt == "CE" else (ep - t1)
        exit_pnl = (exit_price - ep) if opt == "CE" else (ep - exit_price)
        pnl_pts  = t1_pnl * 0.5 + exit_pnl * 0.5
    else:
        pnl_pts = (exit_price - ep) if opt == "CE" else (ep - exit_price)

    out.append({
        "leg":       trade["leg"],
        "opt_type":  opt,
        "mode":      trade.get("mode", "HTF"),
        "entry_ts":  trade["entry_ts"].strftime("%H:%M") if hasattr(trade["entry_ts"], "strftime") else str(trade["entry_ts"]),
        "exit_ts":   exit_ts.strftime("%H:%M")           if hasattr(exit_ts, "strftime")           else str(exit_ts),
        "entry":     round(ep, 2),
        "sl":        round(trade["sl_price"], 2),
        "t1":        round(t1, 2),
        "trail_sl":  round(trade["trail_sl"], 2),
        "exit":      round(exit_price, 2),
        "t1_hit":    trade["t1_hit"],
        "result":    reason,
        "pnl_pts":   round(pnl_pts, 2),
    })

# ── Option-mode backtest (NIFTY / SENSEX) ────────────────────────────────────
def run_day_option_mode(
    trade_date: str,
    ce1_df: pd.DataFrame,
    pe1_df: pd.DataFrame,
    ce1_label: str,
    pe1_label: str,
    cfg: dict,
    gap_info: dict,
) -> list:
    htf_min     = cfg["htf_min"]
    ltf_min     = cfg["ltf_min"]
    cascade_min = cfg["cascade_min"]
    sl_buf      = cfg["sl_buf"]
    atr_mult    = cfg["atr_mult"]
    sq_time     = pd.Timestamp(f"{trade_date} {cfg['sq_off']}")
    entry_open  = pd.Timestamp(f"{trade_date} {cfg['entry_open']}")
    force_cas   = gap_info["gap"]

    trades = []

    def _run_leg(df1m: pd.DataFrame, leg_label: str, opt_type: str) -> list:
        if df1m.empty or len(df1m) < ltf_min + 2:
            return []
        leg_trades = []
        in_trade   = None
        notified   = set()

        # Walk 1-min bars. Use 5-min gate normally, but bypass it when price
        # is inside a TRAPPED HTF zone (catches fast opening sweeps).
        for _, row1 in df1m.iterrows():
            bar_ts  = row1["datetime"]
            cur_ltp = float(row1["close"])

            # EOD
            if bar_ts >= sq_time:
                if in_trade:
                    eod_p = float(df1m.iloc[-1]["close"])
                    _record_exit(in_trade, eod_p, df1m.iloc[-1]["datetime"], "EOD", leg_trades)
                    in_trade = None
                break

            if bar_ts < entry_open:
                continue

            hist_1m = df1m[df1m["datetime"] <= bar_ts]

            # ── Gate: normally fire every 5-min, but also fire every 1-min
            # when price is inside a TRAPPED HTF zone (fast-open scenario where
            # HTF + LTF bears are swept simultaneously in the opening minutes).
            at_ltf_boundary = (bar_ts.minute % ltf_min == 0)
            if not in_trade and not at_ltf_boundary:
                # Quick zone check — only bypass gate if LTP is inside a zone
                htf_test = resample(hist_1m, htf_min) if len(hist_1m) >= htf_min * 2 else pd.DataFrame()
                in_zone_now = False
                if not htf_test.empty:
                    _, zt_entries = scanner.scan_htf(htf_test)
                    in_zone_now = any(
                        e["status"] == "TRAPPED" and e["zone_low"] <= cur_ltp <= e["zone_high"]
                        for e in zt_entries
                    )
                if not in_zone_now:
                    continue   # not at boundary and not in zone → skip this 1-min bar

            # ── Exit check if in trade ────────────────────────────────────
            if in_trade:
                fwd = df1m[(df1m["datetime"] > in_trade["entry_ts"]) &
                           (df1m["datetime"] <= bar_ts)]
                result = None
                for _, b in fwd.iterrows():
                    lo, hi, ts1 = float(b["low"]), float(b["high"]), b["datetime"]
                    if ts1 >= sq_time:
                        result = ("EOD", float(b["close"]), ts1)
                        break
                    # T1: option premium rises to T1 level
                    if not in_trade["t1_hit"] and hi >= in_trade["t1_price"]:
                        in_trade["t1_hit"]   = True
                        in_trade["rem_qty"]  = 0.5
                        in_trade["trail_sl"] = in_trade["entry_price"]  # cost-to-cost lock
                    # Update trail after T1
                    if in_trade["t1_hit"]:
                        h5 = resample(hist_1m, ltf_min)
                        if not h5.empty:
                            _, lents = scanner.scan_htf(h5)
                            in_trade["trail_sl"] = _update_trail_sl(
                                in_trade["trail_sl"], lents, opt_type)
                    # SL / trail check
                    act_sl = in_trade["trail_sl"] if in_trade["t1_hit"] else in_trade["sl_price"]
                    if lo <= act_sl:
                        result = ("TRAIL_SL" if in_trade["t1_hit"] else "SL", act_sl, ts1)
                        break
                if result:
                    _record_exit(in_trade, result[1], result[2], result[0], leg_trades)
                    in_trade = None
                continue   # only one position at a time

            # ── Look for entry ────────────────────────────────────────────
            zone, mode = _find_zone(hist_1m, htf_min, cascade_min, atr_mult, cur_ltp,
                                    scanner.scan_htf, force_cas)
            if zone is None:
                continue

            z_low  = zone["zone_low"]
            z_high = zone["zone_high"]
            t1_p   = zone["sl"]
            uid    = f"{zone.get('ref_ts','')}_{z_high:.2f}"

            if uid in notified or cur_ltp < z_low or cur_ltp > z_high:
                continue

            # LTF scan inside full zone — always 5-min bars.
            # (We check every 1-min close when in zone, but zones are 5-min structures.)
            h5 = resample(hist_1m, ltf_min)
            sd = h5[(h5["low"] <= z_high * 1.01) &
                    (h5["close"] >= z_low * 0.97)].copy().reset_index(drop=True)
            used_tf = f"{ltf_min}m"
            _, ltf_ents = scanner.scan_htf(sd)
            best = scanner.select_fresh_ltf_entry(ltf_ents, opt_type=opt_type) if len(sd) >= 2 else None
            if best is None:
                continue

            # Entry at zone_high = sellers' re-test level (C1.LOW).
            # If price is still above zone_high (post-trap bounce not yet retested),
            # skip this bar — will fire on the next bar when price comes back.
            entry_p = float(best["zone_high"])
            if cur_ltp > entry_p * 1.005:
                continue  # waiting for retest

            sl_p = round(best["zone_low"] - sl_buf, 2)
            if entry_p <= 0 or sl_p >= entry_p:
                continue

            notified.add(uid)
            in_trade = {
                "uid": uid, "mode": f"{mode}/{used_tf}",
                "opt_type": opt_type, "leg": leg_label,
                "entry_price": entry_p, "sl_price": sl_p,
                "t1_price": t1_p, "t1_hit": False,
                "trail_sl": sl_p, "entry_ts": bar_ts, "rem_qty": 1.0,
            }

        # EOD safety
        if in_trade and not df1m.empty:
            _record_exit(in_trade, float(df1m.iloc[-1]["close"]),
                         df1m.iloc[-1]["datetime"], "EOD", leg_trades)
        return leg_trades

    trades += _run_leg(ce1_df, ce1_label, "CE")
    trades += _run_leg(pe1_df, pe1_label, "PE")
    return trades


# ── Futures-mode backtest (CrudeOil) ──────────────────────────────────────────
def run_day_futures_mode(
    trade_date: str,
    fut_df: pd.DataFrame,
    cfg: dict,
    gap_info: dict,
) -> list:
    htf_min     = cfg["htf_min"]
    ltf_min     = cfg["ltf_min"]
    cascade_min = cfg["cascade_min"]
    sl_buf      = cfg["sl_buf"]
    atr_mult    = cfg["atr_mult"]
    sq_time     = pd.Timestamp(f"{trade_date} {cfg['sq_off']}")
    entry_open  = pd.Timestamp(f"{trade_date} {cfg['entry_open']}")
    force_cas   = gap_info["gap"]

    if fut_df.empty or len(fut_df) < ltf_min + 2:
        return []

    trades   = []
    in_trade = None
    notified = set()
    df5      = resample(fut_df, ltf_min)

    for idx5, row5 in df5.iterrows():
        bar_ts  = row5["datetime"]
        cur_fut = float(row5["close"])

        if bar_ts >= sq_time:
            if in_trade:
                eod_p = float(fut_df.iloc[-1]["close"])
                _record_exit(in_trade, eod_p, fut_df.iloc[-1]["datetime"], "EOD", trades)
                in_trade = None
            break

        if bar_ts < entry_open:
            continue

        hist_1m = fut_df[fut_df["datetime"] <= bar_ts]

        # ── Exit check ────────────────────────────────────────────────────
        if in_trade:
            fwd = fut_df[(fut_df["datetime"] > in_trade["entry_ts"]) &
                         (fut_df["datetime"] <= bar_ts)]
            opt    = in_trade["opt_type"]
            result = None
            for _, b in fwd.iterrows():
                lo, hi, ts1 = float(b["low"]), float(b["high"]), b["datetime"]
                if ts1 >= sq_time:
                    result = ("EOD", float(b["close"]), ts1)
                    break
                if not in_trade["t1_hit"]:
                    t1_hit = (hi >= in_trade["t1_price"]) if opt == "CE" \
                             else (lo <= in_trade["t1_price"])
                    if t1_hit:
                        in_trade["t1_hit"]   = True
                        in_trade["rem_qty"]  = 0.5
                        in_trade["trail_sl"] = in_trade["entry_price"]
                if in_trade["t1_hit"]:
                    h5 = resample(hist_1m, ltf_min)
                    if not h5.empty:
                        _, lents = scanner.scan_htf_spot(h5)
                        in_trade["trail_sl"] = _update_trail_sl(
                            in_trade["trail_sl"], lents, opt)
                act_sl = in_trade["trail_sl"] if in_trade["t1_hit"] else in_trade["sl_price"]
                sl_hit = (lo <= act_sl) if opt == "CE" else (hi >= act_sl)
                if sl_hit:
                    result = ("TRAIL_SL" if in_trade["t1_hit"] else "SL", act_sl, ts1)
                    break
            if result:
                _record_exit(in_trade, result[1], result[2], result[0], trades)
                in_trade = None
            continue

        # ── Find zone ─────────────────────────────────────────────────────
        zone, mode = _find_zone(hist_1m, htf_min, cascade_min, atr_mult, cur_fut,
                                scanner.scan_htf_spot, force_cas)
        if zone is None:
            continue

        z_low  = zone["zone_low"]
        z_high = zone["zone_high"]
        t1_p   = zone["sl"]
        kind   = zone.get("kind", "BEAR")
        opt    = "CE" if kind == "BEAR" else "PE"
        uid    = f"{zone.get('ref_ts','')}_{z_high:.1f}_{kind}"

        if uid in notified or cur_fut < z_low or cur_fut > z_high:
            continue

        # Direction-of-approach
        recent = df5.iloc[max(0, idx5-6):idx5]
        if opt == "CE" and not recent.empty and recent["high"].max() <= z_high:
            continue
        if opt == "PE" and not recent.empty and recent["low"].min() >= z_low:
            continue

        # Zone invalidation
        if opt == "CE" and not recent.empty and recent["low"].min() < z_low:
            continue
        if opt == "PE" and not recent.empty and recent["high"].max() > z_high:
            continue

        # LTF scan inside full zone
        h5 = resample(hist_1m, ltf_min)
        z5 = h5[(h5["low"]   <= z_high * 1.01) &
                (h5["close"] >= z_low  * 0.98)].copy().reset_index(drop=True)
        if len(z5) < 2:
            continue

        try:
            scan_fn_ltf = scanner.scan_ltf if opt == "CE" else scanner.scan_ltf_bull
            _, ltf_ents = scan_fn_ltf(z5, htf_zone_high=z_high,
                                      htf_zone_low=z_low, htf_target=t1_p)
        except TypeError:
            _, ltf_ents = scanner.scan_htf_spot(z5)

        best = scanner.select_fresh_ltf_entry(ltf_ents, opt_type=opt)
        if best is None:
            continue

        entry_p = float(best.get("zone_trigger", cur_fut))
        sl_p    = round(best["zone_low"] - sl_buf, 2) if opt == "CE" \
                  else round(best["zone_high"] + sl_buf, 2)

        notified.add(uid)
        in_trade = {
            "uid": uid, "mode": mode,
            "opt_type": opt, "leg": "FUT",
            "entry_price": entry_p, "sl_price": sl_p,
            "t1_price": t1_p, "t1_hit": False,
            "trail_sl": sl_p, "entry_ts": bar_ts, "rem_qty": 1.0,
        }

    if in_trade and not fut_df.empty:
        _record_exit(in_trade, float(fut_df.iloc[-1]["close"]),
                     fut_df.iloc[-1]["datetime"], "EOD", trades)
    return trades


# ── Trading days ──────────────────────────────────────────────────────────────
def get_trading_days(n: int) -> list[str]:
    days = []
    d = date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.isoformat())
    return list(reversed(days))


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Trap Scanner 7-day backtest (new logic)")
    parser.add_argument("--indices", nargs="+", default=["NIFTY", "SENSEX", "CRUDEOIL"])
    parser.add_argument("--days",  type=int, default=7)
    parser.add_argument("--lots",  type=int, default=2)
    args = parser.parse_args()

    if "YOUR_UPSTOX_TOKEN" in TOKEN:
        print("ERROR: Update TOKEN in this script with your Upstox access token.")
        sys.exit(1)

    trading_days = get_trading_days(args.days)
    print(f"\nBacktest period : {trading_days[0]} → {trading_days[-1]}  ({len(trading_days)} days)")
    print(f"Indices         : {args.indices}")
    print(f"Lots            : {args.lots}")
    print(f"Logic           : Full zone + Gap→Cascade + LTF trail SL + T1 50% partial\n")

    all_results = []

    for idx_name in args.indices:
        cfg = INDEX_CFG.get(idx_name)
        if not cfg:
            print(f"Unknown index: {idx_name}")
            continue

        htf_source = cfg["htf_source"]
        print(f"\n{'='*65}")
        print(f"  {idx_name}  (htf={cfg['htf_min']}m ltf={cfg['ltf_min']}m"
              f" cascade={cfg['cascade_min']}m source={htf_source})")
        print(f"{'='*65}")

        for trade_date in trading_days:
            print(f"\n  ── {trade_date} ──")

            if htf_source == "futures":
                fut_key = cfg["fut_key"] if trade_date > "2026-06-18" else "MCX_FO|499095"
                print(f"    Futures ({fut_key})...", end=" ", flush=True)
                fut_df = fetch_1m(fut_key, trade_date)
                print(f"{len(fut_df)} bars")
                time.sleep(0.4)
                if fut_df.empty:
                    print("    No data — skip"); continue

                # Gap: MCX evening session opens ~17:30, compare to prev close
                # Use day-before last bar vs today first bar
                gap_info = {"gap": False, "direction": "NONE", "pct": 0.0}
                prev_day = (date.fromisoformat(trade_date) - timedelta(days=1)).isoformat()
                prev_df  = fetch_1m(fut_key, prev_day)
                time.sleep(0.3)
                if not prev_df.empty and not fut_df.empty:
                    prev_close = float(prev_df.iloc[-1]["close"])
                    today_open = float(fut_df.iloc[0]["open"])
                    gap_info   = detect_gap(today_open, prev_close, cfg["gap_pct"])

                if gap_info["gap"]:
                    print(f"    GAP {gap_info['direction']} {gap_info['pct']:+.2f}% → cascade forced")

                trades = run_day_futures_mode(trade_date, fut_df, cfg, gap_info)

            else:
                # NIFTY / SENSEX option mode
                print(f"    Prev OHLC...", end=" ", flush=True)
                prev = fetch_daily_ohlc(cfg["spot_key"], trade_date)
                time.sleep(0.3)
                if not prev:
                    print("no data — skip"); continue
                H, L, C = prev["high"], prev["low"], prev["close"]
                piv = pivot_levels(H, L, C)
                ce1_s = round_strike(piv["s1"], cfg["step"])
                pe1_s = round_strike(piv["r1"], cfg["step"])
                print(f"H={H:.0f} L={L:.0f} C={C:.0f} → CE1={ce1_s} PE1={pe1_s}")

                dt_obj  = date.fromisoformat(trade_date)
                exch_fo = "NSE" if idx_name == "NIFTY" else "BSE"
                print(f"    Option keys...", end=" ", flush=True)
                ce1_key = find_option_key(exch_fo, idx_name, ce1_s, "CE", dt_obj)
                pe1_key = find_option_key(exch_fo, idx_name, pe1_s, "PE", dt_obj)
                time.sleep(0.3)

                if not (ce1_key or pe1_key):
                    atm     = round_strike(C, cfg["step"])
                    ce1_s   = atm - cfg["step"]
                    pe1_s   = atm + cfg["step"]
                    ce1_key = find_option_key(exch_fo, idx_name, ce1_s, "CE", dt_obj)
                    pe1_key = find_option_key(exch_fo, idx_name, pe1_s, "PE", dt_obj)
                    if not (ce1_key or pe1_key):
                        print("keys not found — skip"); continue
                    print(f"ATM±step: CE={ce1_s} PE={pe1_s}")
                else:
                    print("OK")

                ce1_df = pd.DataFrame()
                pe1_df = pd.DataFrame()
                if ce1_key:
                    print(f"    CE1 bars...", end=" ", flush=True)
                    ce1_df = fetch_1m(ce1_key, trade_date, mkt_open="09:00", mkt_close="15:30")
                    print(f"{len(ce1_df)} bars")
                    time.sleep(0.4)
                if pe1_key:
                    print(f"    PE1 bars...", end=" ", flush=True)
                    pe1_df = fetch_1m(pe1_key, trade_date, mkt_open="09:00", mkt_close="15:30")
                    print(f"{len(pe1_df)} bars")
                    time.sleep(0.4)

                # Gap: fetch spot open bar vs prev close
                gap_info = {"gap": False, "direction": "NONE", "pct": 0.0}
                spot_open_df = fetch_1m(cfg["spot_key"], trade_date,
                                        mkt_open="09:15", mkt_close="09:25")
                time.sleep(0.2)
                if not spot_open_df.empty and C > 0:
                    spot_open = float(spot_open_df.iloc[0]["open"])
                    gap_info  = detect_gap(spot_open, C, cfg["gap_pct"])

                if gap_info["gap"]:
                    print(f"    GAP {gap_info['direction']} {gap_info['pct']:+.2f}% "
                          f"(open={spot_open_df.iloc[0]['open']:.0f} vs prev_close={C:.0f}) "
                          f"→ cascade forced")

                trades = run_day_option_mode(
                    trade_date, ce1_df, pe1_df,
                    f"{idx_name}{ce1_s}CE", f"{idx_name}{pe1_s}PE",
                    cfg, gap_info
                )

            # Print trades
            lot_size = cfg["lot"] * args.lots
            if not trades:
                print("    No trades")
            for t in trades:
                pnl_rs = round(t["pnl_pts"] * lot_size, 2)
                tag    = "[T1+trail]" if t["t1_hit"] else "          "
                win    = "WIN " if t["pnl_pts"] > 0 else "LOSS"
                print(f"    {t['entry_ts']}→{t['exit_ts']}  {t['opt_type']}"
                      f"/{t['mode']:<12}  in={t['entry']:.1f}  sl={t['sl']:.1f}"
                      f"  t1={t['t1']:.1f}  trail={t['trail_sl']:.1f}"
                      f"  out={t['exit']:.1f}  {tag}  {t['result']:<10}"
                      f"  {t['pnl_pts']:+.1f}pts  Rs{pnl_rs:+.0f}  {win}")
                all_results.append({
                    "Date": trade_date, "Index": idx_name,
                    "OptType": t["opt_type"], "Mode": t["mode"],
                    "Entry Time": t["entry_ts"], "Exit Time": t["exit_ts"],
                    "Entry": t["entry"], "SL": t["sl"],
                    "T1": t["t1"], "Trail SL": t["trail_sl"],
                    "Exit": t["exit"], "T1 Hit": t["t1_hit"],
                    "Result": t["result"],
                    "P&L pts": t["pnl_pts"],
                    "Lot Size": lot_size,
                    "P&L Rs": round(t["pnl_pts"] * lot_size, 2),
                })

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n\n{'='*70}")
    print("  SUMMARY")
    print(f"{'='*70}")

    if not all_results:
        print("  No trades found.")
        return

    df_r = pd.DataFrame(all_results)

    for idx_name in args.indices:
        sub = df_r[df_r["Index"] == idx_name]
        if sub.empty:
            print(f"\n  {idx_name}: No trades")
            continue
        wins   = int((sub["P&L pts"] > 0).sum())
        total  = len(sub)
        wr     = round(wins / total * 100, 1)
        net_rs = sub["P&L Rs"].sum()
        print(f"\n  {idx_name}: Trades={total}  Wins={wins}  WR={wr}%  Net=Rs {net_rs:+,.0f}")
        print(f"    {'Date':<12}{'Type':<5}{'Mode':<14}{'In':<8}{'Out':<8}"
              f"{'P&L pts':>9}{'P&L Rs':>10}  Result")
        for _, r in sub.iterrows():
            print(f"    {r['Date']:<12}{r['OptType']:<5}{r['Mode']:<14}"
                  f"{r['Entry Time']:<8}{r['Exit Time']:<8}"
                  f"{r['P&L pts']:>+9.1f}{r['P&L Rs']:>+10.0f}  {r['Result']}")

    total_rs = df_r["P&L Rs"].sum()
    tw = len(df_r)
    ww = int((df_r["P&L pts"] > 0).sum())
    wr_all = round(ww/tw*100,1) if tw else 0
    print(f"\n  {'─'*55}")
    print(f"  COMBINED: Trades={tw}  Wins={ww}  WR={wr_all}%")
    print(f"  TOTAL NET P&L = Rs {total_rs:+,.0f}")
    print(f"{'='*70}\n")

    out_file = os.path.join(os.path.dirname(__file__), "backtest_7d_results.csv")
    df_r.to_csv(out_file, index=False)
    print(f"  Results saved → {out_file}\n")


if __name__ == "__main__":
    main()
