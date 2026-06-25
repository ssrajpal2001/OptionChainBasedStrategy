"""
scripts/crude_backtest.py — CrudeOil options backtest (pure intraday cascade 30m→5m→1m).

Strategy:
  - ATM from MCX futures first bar open
  - Strike: CE = ATM - depth × 100  (ITM call)
            PE = ATM + depth × 100  (ITM put)
  - Zone detection: on OPTION PREMIUM bars (same as NIFTY/SENSEX approach)
  - Pure intraday cascade: 30m parent zones → 5m sub-zones → 1m HIGH breakout entry
  - SL: zone_low − sl_buf (intrabar — exits at sl price, not candle close)
  - T1: zone_high (BEAR zone); trail SL on 5m ratchet after T1
  - EOD: 23:00 IST MCX square-off

Market hours: 09:00 – 23:30 IST (MCX)

Usage:
  python3 scripts/crude_backtest.py --token YOUR_UPSTOX_TOKEN
  python3 scripts/crude_backtest.py --token TOKEN --start 2026-06-01 --end 2026-06-25
  python3 scripts/crude_backtest.py --token TOKEN --weeks 4 --sl-buf 20 --max-ltf 5
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests

sys.path.insert(0, ".")
from strategies.trap_scanner import scanner

# ── Constants ──────────────────────────────────────────────────────────────────
CRUDE_STEP  = 100     # Rs per strike step
CRUDE_LOT   = 100     # 1 standard lot = 100 barrels
MKT_OPEN    = "09:00"
MKT_CLOSE   = "23:30"
EOD_TIME    = "23:00"
ENTRY_OPEN  = "09:30"  # no entries before 09:30

# Cascade timeframes — tuned for MCX CrudeOil (slower moves than NSE index)
HTF_MIN_DEFAULT = 30   # parent zone timeframe (30m)
SUB_MIN_DEFAULT = 5    # sub-zone timeframe (5m)

_HEADERS: dict = {}
_MCX_MASTER: list = []

# ── Data helpers ───────────────────────────────────────────────────────────────
def _get(url: str) -> dict:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=25)
            if r.status_code == 429:
                time.sleep(2); continue
            if r.status_code == 200:
                return r.json()
            return {}
        except Exception:
            time.sleep(1)
    return {}


def _fetch_1m(key: str, from_dt: str, to_dt: str) -> pd.DataFrame:
    """Fetch 1m bars, 28-day chunks (Upstox limit)."""
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
    out = pd.concat(chunks, ignore_index=True)
    out = out.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    return out


def _mkt_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to MCX market hours 09:00–23:30."""
    t0 = pd.Timestamp(MKT_OPEN).time()
    t1 = pd.Timestamp(MKT_CLOSE).time()
    return df[(df["datetime"].dt.time >= t0) & (df["datetime"].dt.time <= t1)].copy()


def _resample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r


def _price_at(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Closest 1m close at or before ts."""
    sub = df[df["datetime"] <= ts]
    if sub.empty:
        return 0.0
    return float(sub.iloc[-1]["close"])


def get_trading_days(start: str, end: str) -> list[str]:
    f, t = date.fromisoformat(start), date.fromisoformat(end)
    return [f"{f + timedelta(days=i)}" for i in range((t - f).days + 1)
            if (f + timedelta(days=i)).weekday() < 5]


# ── MCX instrument lookup ──────────────────────────────────────────────────────
def _load_mcx_master() -> list:
    global _MCX_MASTER
    if _MCX_MASTER:
        return _MCX_MASTER
    url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
    try:
        r = requests.get(url, timeout=30)
        _MCX_MASTER = json.loads(gzip.decompress(r.content))
        print(f"  [MCX master] {len(_MCX_MASTER)} instruments loaded")
    except Exception as e:
        print(f"  [MCX master] load failed: {e}")
        _MCX_MASTER = []
    return _MCX_MASTER


def _find_crude_option(strike: int, otype: str, min_expiry: date) -> str:
    """Return Upstox instrument key for the nearest CrudeOil option."""
    master = _load_mcx_master()
    ot = otype.upper()
    candidates = []
    for row in master:
        itype = str(row.get("instrument_type", "")).upper()
        row_ot = itype if itype in ("CE", "PE") else str(row.get("option_type", "")).upper()
        if row_ot != ot:
            continue
        if abs(float(row.get("strike", 0) or 0) - strike) > 0.5:
            continue
        sym = str(row.get("tradingsymbol", "") or row.get("name", "")).upper()
        und = str(row.get("underlying_symbol", "") or "").upper()
        if "CRUDE" not in sym and "CRUDE" not in und:
            continue
        exp_str = str(row.get("expiry", "") or "")[:10]
        try:
            exp_dt = date.fromisoformat(exp_str)
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


def _get_atm(fut_df: pd.DataFrame) -> int:
    """ATM = futures first bar open rounded to nearest 100."""
    if fut_df.empty:
        return 0
    return int(round(float(fut_df.iloc[0]["open"]) / CRUDE_STEP) * CRUDE_STEP)


# ── Simulate one entry/exit on 1m bars ────────────────────────────────────────
def _simulate_exit(
    opt_1m: pd.DataFrame,
    entry_ts: pd.Timestamp,
    entry_price: float,
    zone_low: float,
    zone_high: float,
    sl_buf: float,
    trade_date: str,
    total_qty: int,
) -> dict:
    """
    Simulate exit on 1m option bars after entry_ts.
    - init_sl  = zone_low − sl_buf   (intrabar: exits at sl price when low < sl)
    - T1       = zone_high            (50% off at T1)
    - Trail SL: after T1, ratchet up on 5m trap zone lows
    - EOD      = 23:00 MCX
    """
    sq_ts    = pd.Timestamp(f"{trade_date} {EOD_TIME}")
    init_sl  = zone_low - sl_buf
    t1_price = zone_high

    future_bars = opt_1m[opt_1m["datetime"] > entry_ts].copy()
    if future_bars.empty:
        return {"exit": entry_price, "exit_ts": entry_ts, "reason": "EOD",
                "t1_hit": False, "pnl_rs": 0}

    # Build 5m ratchet trap events (for TSL after T1)
    df_5m = _resample(opt_1m[opt_1m["datetime"] <= sq_ts], 5)
    _, ltf5_all = scanner.scan_htf(df_5m) if len(df_5m) >= 2 else (None, [])
    ratchet_floors = sorted(
        [float(e.get("zone_low", 0))
         for e in (ltf5_all or [])
         if e.get("status") in ("TRAPPED", "CLOSED") and float(e.get("zone_low", 0)) > 0],
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

        # EOD
        if bar_ts >= sq_ts:
            exit_price  = bar_close
            exit_reason = "EOD"
            exit_ts_out = bar_ts
            break

        # T1 hit: bar high crosses zone_high
        if not t1_hit and bar_high >= t1_price:
            t1_hit  = True
            t1_pnl  = (t1_price - entry_price) * t1_qty
            trail_sl = zone_low  # reset TSL to zone_low after T1

        # Ratchet trail SL up to highest 5m floor below current bar
        if t1_hit:
            new_floor = max(
                (f for f in ratchet_floors if f > trail_sl and f < bar_close),
                default=trail_sl
            )
            if new_floor > trail_sl:
                trail_sl = new_floor

        # SL check — intrabar: exit at sl_trigger price, not close
        active_sl  = trail_sl if t1_hit else init_sl
        sl_trigger = active_sl - sl_buf
        if bar_low < sl_trigger:
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
    rem_pnl   = (exit_price - entry_price) * exit_qty
    total_pnl = int(round(t1_pnl + rem_pnl, 0))

    return {
        "exit":    round(exit_price, 2),
        "exit_ts": exit_ts_out,
        "reason":  exit_reason,
        "t1_hit":  t1_hit,
        "pnl_rs":  total_pnl,
    }


# ── Per-day per-leg backtest ───────────────────────────────────────────────────
def _run_leg(
    trade_date: str,
    opt_type: str,
    strike: int,
    opt_1m: pd.DataFrame,
    sl_buf: float,
    lots: int,
    max_ltf: int,
    htf_min: int,
    sub_min: int,
    zcache: dict,
) -> list[dict]:
    """
    Pure intraday cascade:
      1. Scan today's option 1m bars → resample to htf_min → get zones
      2. For each zone, scan sub_min bars within that zone → sub-zones
      3. For each sub-zone, find 1m HIGH breakout → entry
    """
    td  = pd.to_datetime(trade_date).date()
    key = f"{trade_date}_{opt_type}_{strike}"

    today_1m = opt_1m[opt_1m["datetime"].dt.date == td].copy()
    if len(today_1m) < 5:
        print(f"  {trade_date} {opt_type} {strike}: no today option bars")
        return []

    total_qty = lots * CRUDE_LOT

    # ── Cache: HTF parent zones ────────────────────────────────────────────────
    htf_ck = (td, strike, opt_type, htf_min, "cas")
    if htf_ck in zcache:
        cas_zones = zcache[htf_ck]
    else:
        df_htf = _resample(today_1m, htf_min)
        _, cas_raw = scanner.scan_htf(df_htf) if len(df_htf) >= 2 else (None, [])
        cas_zones = sorted(
            [e for e in (cas_raw or []) if e.get("status") in ("TRAPPED", "CLOSED")],
            key=lambda z: float(z.get("zone_low", 9999))
        )
        zcache[htf_ck] = cas_zones

    if not cas_zones:
        print(f"  {trade_date} {opt_type} {strike}: no {htf_min}m zones")
        return []

    # ── Cache: sub-zone scan ───────────────────────────────────────────────────
    sub_ck = (td, strike, opt_type, sub_min, "sub")
    if sub_ck in zcache:
        sub_all = zcache[sub_ck]
    else:
        df_sub = _resample(today_1m, sub_min)
        _, sub_all = scanner.scan_htf(df_sub) if len(df_sub) >= 2 else (None, [])
        zcache[sub_ck] = sub_all or []

    trades     = []
    open_trade = None   # one position at a time per leg

    entry_open_ts = pd.Timestamp(f"{trade_date} {ENTRY_OPEN}")

    for cz in cas_zones:
        zh = float(cz["zone_high"])
        zl = float(cz["zone_low"])

        # Sub-zones within this HTF zone
        ltf_in = [
            e for e in (sub_all or [])
            if e.get("status") in ("TRAPPED", "CLOSED")
            and float(e.get("zone_high", 0)) <= zh * 1.02
            and float(e.get("zone_low",  0)) >= zl * 0.98
        ]
        if not ltf_in:
            print(f"  {trade_date} {opt_type} {strike}: {htf_min}m {zl:.0f}-{zh:.0f} → no {sub_min}m sub-trap — SKIP")
            continue

        ltf_in.sort(key=lambda e: float(e.get("zone_low", 9999)))
        max_idx = max_ltf if max_ltf > 0 else len(ltf_in)
        added   = 0

        for idx, sz in enumerate(ltf_in[:max_idx]):
            sz_low  = float(sz["zone_low"])
            sz_high = float(sz["zone_high"])
            zone_width = sz_high - sz_low
            if zone_width < sl_buf:   # zone too narrow relative to SL
                continue

            # 1m HIGH breakout: find first 1m bar where HIGH > sz_high after zone CLOSE time
            trap_closed_ts = pd.to_datetime(
                sz.get("closed_on") or sz.get("trapped_on") or sz.get("ref_ts") or "NaT"
            )
            if trap_closed_ts is pd.NaT or trap_closed_ts is None:
                continue
            if hasattr(trap_closed_ts, "tzinfo") and trap_closed_ts.tzinfo:
                trap_closed_ts = trap_closed_ts.tz_localize(None)

            search_bars = today_1m[
                (today_1m["datetime"] > trap_closed_ts) &
                (today_1m["datetime"] >= entry_open_ts)
            ]
            breakout_bar = search_bars[search_bars["high"] > sz_high]
            if breakout_bar.empty:
                continue

            entry_ts_bar  = breakout_bar.iloc[0]["datetime"]
            entry_price   = float(breakout_bar.iloc[0]["close"])  # buy at bar close after breakout
            if entry_price <= 0:
                continue

            # Skip if already in a trade on this leg
            if open_trade is not None and entry_ts_bar <= open_trade["exit_ts"]:
                continue

            trap_pos = f"LTF-{idx+1}"
            print(f"  {trade_date} {opt_type} {strike}: {htf_min}m→{sub_min}m "
                  f"{sz_low:.0f}-{sz_high:.0f} → 1m breakout @ {entry_ts_bar.strftime('%H:%M')} "
                  f"entry={entry_price:.1f}  [{trap_pos}]")

            exit_info = _simulate_exit(
                opt_1m=today_1m,
                entry_ts=entry_ts_bar,
                entry_price=entry_price,
                zone_low=sz_low,
                zone_high=sz_high,
                sl_buf=sl_buf,
                trade_date=trade_date,
                total_qty=total_qty,
            )

            trade = {
                "date":       trade_date,
                "opt_type":   opt_type,
                "strike":     strike,
                "trap_pos":   trap_pos,
                "entry_ts":   str(entry_ts_bar)[:16],
                "entry":      round(entry_price, 2),
                "zone_low":   round(sz_low, 2),
                "zone_high":  round(sz_high, 2),
                "sl":         round(sz_low - sl_buf, 2),
                "t1":         round(sz_high, 2),
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
            print(f"  {trade_date} {opt_type} {strike}: {htf_min}m {zl:.0f}-{zh:.0f} → "
                  f"{sub_min}m ×{added}/{len(ltf_in[:max_idx])}")

    return trades


# ── Full backtest ──────────────────────────────────────────────────────────────
def run_crude_backtest(
    token:        str,
    fut_key:      str,
    start:        str,
    end:          str,
    lots:         int   = 2,
    sl_buf:       float = 20.0,
    max_ltf:      int   = 5,
    strike_depth: int   = 1,    # ITM steps (1=near, 2=far)
    htf_min:      int   = HTF_MIN_DEFAULT,
    sub_min:      int   = SUB_MIN_DEFAULT,
) -> dict:
    global _HEADERS
    _HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    trading_days = get_trading_days(start, end)
    if not trading_days:
        return {"ok": False, "error": "no trading days in range"}

    print(f"\n{'='*65}")
    print(f"  CrudeOil Backtest — Pure Intraday Cascade {htf_min}m→{sub_min}m→1m")
    print(f"{'='*65}")
    print(f"  Date       : {trading_days[0]} → {trading_days[-1]}  ({len(trading_days)} days)")
    print(f"  Lots       : {lots}  (qty/trade={lots * CRUDE_LOT} barrels)")
    print(f"  SL Buffer  : {sl_buf} Rs")
    print(f"  Max LTF    : {max_ltf}  (sub-zones LTF-{max_ltf}+ filtered out)")
    print(f"  Strike     : ATM±{strike_depth}×100 ITM  (near={strike_depth == 1})")
    print(f"  HTF        : {htf_min}m parent zones")
    print(f"  Sub-zone   : {sub_min}m sub-zones")
    print(f"  Futures key: {fut_key}")
    print(f"{'='*65}\n")

    # Fetch futures bars for ATM calculation
    print(f"Fetching MCX futures bars {start} to {end}...")
    fut_all = _fetch_1m(fut_key, start, end)
    if fut_all.empty:
        return {"ok": False, "error": "no futures bars — check fut_key"}
    fut_all = _mkt_hours(fut_all)
    print(f"  {len(fut_all)} futures bars loaded\n")

    # Load MCX master for option key lookup
    _load_mcx_master()

    shared_opt_cache: dict = {}   # {key: df_1m} — fetch once per (strike, date range)
    shared_zone_cache: dict = {}  # zone scan results per (date, strike, tf)
    all_trades: list[dict] = []

    for trade_date in trading_days:
        td   = date.fromisoformat(trade_date)
        futs = fut_all[fut_all["datetime"].dt.date == td]
        if futs.empty:
            print(f"  {trade_date}: no futures data — skip")
            continue

        atm = _get_atm(futs)
        if atm <= 0:
            print(f"  {trade_date}: ATM=0 — skip")
            continue

        ce_strike = atm - strike_depth * CRUDE_STEP
        pe_strike = atm + strike_depth * CRUDE_STEP
        print(f"  {trade_date}  ATM={atm}  CE={ce_strike}  PE={pe_strike}")

        for opt_type, strike in [("CE", ce_strike), ("PE", pe_strike)]:
            # Look up option key (cached per strike)
            cache_key = (strike, opt_type)
            if cache_key not in shared_opt_cache:
                key = _find_crude_option(strike, opt_type, td)
                if not key:
                    print(f"  {trade_date} {opt_type} {strike}: key not found — skip")
                    shared_opt_cache[cache_key] = pd.DataFrame()
                    continue
                print(f"  Fetching {opt_type} {strike} option bars... ", end="", flush=True)
                opt_df = _fetch_1m(key, start, end)
                opt_df = _mkt_hours(opt_df)
                shared_opt_cache[cache_key] = opt_df
                print(f"{len(opt_df)} bars")
            else:
                opt_df = shared_opt_cache[cache_key]

            if opt_df.empty:
                continue

            day_trades = _run_leg(
                trade_date   = trade_date,
                opt_type     = opt_type,
                strike       = strike,
                opt_1m       = opt_df,
                sl_buf       = sl_buf,
                lots         = lots,
                max_ltf      = max_ltf,
                htf_min      = htf_min,
                sub_min      = sub_min,
                zcache       = shared_zone_cache,
            )
            all_trades.extend(day_trades)

    return {"ok": True, "trades": all_trades}


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="CrudeOil options backtest — pure intraday 30m→5m→1m cascade",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument("--token",    required=True, help="Upstox access token")
    ap.add_argument("--start",    default="", help="Start date YYYY-MM-DD")
    ap.add_argument("--end",      default="", help="End date YYYY-MM-DD")
    ap.add_argument("--weeks",    type=int, default=4, help="Rolling weeks if --start/--end not set")
    ap.add_argument("--lots",     type=int, default=2, help="Number of lots (default: 2)")
    ap.add_argument("--sl-buf",   type=float, default=20.0, help="SL buffer in Rs (default: 20)")
    ap.add_argument("--max-ltf",  type=int, default=5, help="Max sub-zone index (default: 5)")
    ap.add_argument("--depth",    type=int, default=1, choices=[1, 2, 3],
                    help="ITM strike depth: 1=near(default), 2=mid, 3=far")
    ap.add_argument("--htf",      type=int, default=HTF_MIN_DEFAULT,
                    help=f"Parent zone timeframe in minutes (default: {HTF_MIN_DEFAULT})")
    ap.add_argument("--sub",      type=int, default=SUB_MIN_DEFAULT,
                    help=f"Sub-zone timeframe in minutes (default: {SUB_MIN_DEFAULT})")
    ap.add_argument("--fut-key",  default="MCX_FO|520702",
                    help="MCX futures instrument key (default: MCX_FO|520702)")
    args = ap.parse_args()

    # Date range
    if args.start and args.end:
        start_dt = args.start
        end_dt   = args.end
    else:
        end_d   = date.today()
        start_d = end_d - timedelta(weeks=args.weeks)
        start_dt = start_d.isoformat()
        end_dt   = end_d.isoformat()

    result = run_crude_backtest(
        token        = args.token,
        fut_key      = args.fut_key,
        start        = start_dt,
        end          = end_dt,
        lots         = args.lots,
        sl_buf       = args.sl_buf,
        max_ltf      = args.max_ltf,
        strike_depth = args.depth,
        htf_min      = args.htf,
        sub_min      = args.sub,
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

    print(f"\n{'─'*90}")
    print(f"CrudeOil  {start_dt} to {end_dt}  "
          f"Trades={len(trades)}  Win={round(100*len(wins)/len(trades),1) if trades else 0}%  "
          f"Rs {total:+,}  PF={pf}")
    print(f"{'─'*90}")
    print(f"  {'Date':<12} {'Opt':<4} {'Strike':<7} {'LTF':<7} {'Time':<6}"
          f"  {'Entry':>7} {'Zone':>11} {'Exit':>7} {'Reason':<12} {'P&L':>9}")
    print(f"  {'─'*85}")
    for t in trades:
        zone_str = f"{t['zone_low']:.0f}-{t['zone_high']:.0f}"
        ts = t['entry_ts'][11:16] if len(t['entry_ts']) > 10 else t['entry_ts']
        print(f"  {t['date']:<12} {t['opt_type']:<4} {t['strike']:<7} "
              f"{t['trap_pos']:<7} {ts:<6}  "
              f"{t['entry']:>7.1f} {zone_str:>11} {t['exit']:>7.1f} "
              f"{t['reason']:<12} ₹{t['pnl_rs']:>+8,}")
    print(f"{'─'*90}")
    print(f"\n  RESULTS — {len(trades)} trades")
    print(f"  {'─'*40}")
    print(f"  Win Rate      : {len(wins)}/{len(trades)} ({round(100*len(wins)/len(trades),1)}%)")
    print(f"  Total P&L     : ₹{total:+,}")
    print(f"  Profit Factor : {pf}")
    print(f"  Avg Win       : ₹{round(gw/len(wins),0):,.0f}" if wins else "  Avg Win  : —")
    print(f"  Avg Loss      : ₹{-round(gl/len(losses),0):,.0f}" if losses else "  Avg Loss : —")
    print(f"  Gross Win     : ₹{gw:+,}")
    print(f"  Gross Loss    : ₹{-gl:,}")
    print()
