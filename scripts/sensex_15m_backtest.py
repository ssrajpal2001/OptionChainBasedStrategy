"""
SENSEX 15-min Intraday Trap Backtest
=====================================
Concept (simplified / backtest-only):
  1. Fetch SENSEX option 1-min bars for each day.
  2. Build 15-min bars progressively (bar-by-bar).
  3. After each 15-min close → run seller-trap scan on all completed 15-min bars.
  4. If a NEW trapped zone is found, watch 1-min bars in real time.
  5. Entry: FIRST 1-min bar whose close is inside the zone AND crosses zone_trigger.
             No LTF (5-min) confirmation needed — "immediately take trade".
  6. Exit:
       T1  = zone.sl field (bears' stop = scanner's target)
       SL  = zone_low - SL_BUF
       EOD = 15:25 IST
  7. Only one open position per leg (CE/PE) at a time.

Usage:
  python scripts/sensex_15m_backtest.py --token YOUR_UPSTOX_TOKEN
  python scripts/sensex_15m_backtest.py --token TOKEN --days 7
  python scripts/sensex_15m_backtest.py --token TOKEN --days 7 --ce-only
"""
from __future__ import annotations
import argparse, gzip, json, sys, os, time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

# ── Config ────────────────────────────────────────────────────────────────────
STEP     = 100      # SENSEX strike step
LOT      = 20       # SENSEX lot size (BSE weekly)
SL_BUF   = 2.0      # pts below zone_low for SL
HTF_MIN  = 15       # build 15-min bars, detect trap
MKT_OPEN = "09:15"
SQ_OFF   = "15:25"

SPOT_KEY = "BSE_INDEX|SENSEX"

_MASTER: list = []
_HEADERS: dict = {}

# ── Master / key helpers ──────────────────────────────────────────────────────
def _load_bse_master() -> list:
    global _MASTER
    if _MASTER:
        return _MASTER
    url = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
    print("  Loading BSE master...", end=" ", flush=True)
    r = requests.get(url, timeout=60)
    _MASTER = json.loads(gzip.decompress(r.content))
    print(f"{len(_MASTER)} instruments")
    return _MASTER


def find_option_key(strike: int, otype: str, min_expiry: date) -> str:
    """Find BSE_FO instrument key for SENSEX option."""
    master = _load_bse_master()
    ot = otype.upper()
    candidates = []
    for row in master:
        itype = str(row.get("instrument_type", "")).upper()
        row_ot = itype if itype in ("CE", "PE") else str(row.get("option_type", "")).upper()
        if row_ot != ot:
            continue
        row_strike = float(row.get("strike", 0) or 0)
        if abs(row_strike - strike) > 0.5:
            continue
        # BSE master stores expiry as epoch-ms integer (e.g. 1751049600000),
        # NOT an ISO string — date.fromisoformat fails silently and skips all rows.
        exp_raw = row.get("expiry", "")
        try:
            if isinstance(exp_raw, (int, float)) or (
                isinstance(exp_raw, str) and str(exp_raw).strip().lstrip("-").isdigit()
            ):
                _epoch = int(exp_raw)
                if _epoch > 10_000_000_000:   # milliseconds → seconds
                    _epoch //= 1000
                exp_dt = datetime.fromtimestamp(_epoch, tz=timezone.utc).date()
            else:
                exp_dt = date.fromisoformat(str(exp_raw)[:10])
        except Exception:
            continue
        if exp_dt < min_expiry:
            continue
        row_und = str(row.get("underlying_symbol", "") or "").upper()
        sym_str = str(row.get("tradingsymbol", "") or row.get("name", "")).upper()
        if "SENSEX" not in row_und and "SENSEX" not in sym_str:
            continue
        key = str(row.get("instrument_key", ""))
        if key:
            candidates.append((exp_dt, key))
    if not candidates:
        # Debug: print first BSE_FO row so field names are visible if lookup keeps failing
        sample = next((r for r in master if str(r.get("instrument_key","")).startswith("BSE_FO|")), None)
        if sample:
            print(f"    [DEBUG] First BSE_FO row keys: {list(sample.keys())[:10]} "
                  f"expiry={sample.get('expiry')} itype={sample.get('instrument_type')} "
                  f"strike={sample.get('strike')} name={sample.get('name')}")
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


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
    df = df[df["datetime"].dt.strftime("%H:%M") >= MKT_OPEN]
    df = df[df["datetime"].dt.strftime("%H:%M") <= SQ_OFF]
    return df


def fetch_spot_ohlc(trade_date: str) -> Optional[dict]:
    """Get prev-day spot OHLC to compute ATM."""
    enc  = SPOT_KEY.replace("|", "%7C")
    from_dt = (date.fromisoformat(trade_date) - timedelta(days=6)).isoformat()
    data = _get(f"https://api.upstox.com/v2/historical-candle/{enc}/day/{trade_date}/{from_dt}")
    cands = data.get("data", {}).get("candles", [])
    for c in cands:
        bar_dt = str(c[0])[:10]
        if bar_dt < trade_date:
            return {"date": bar_dt, "open": c[1], "high": c[2], "low": c[3], "close": c[4]}
    return None


def resample_to_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r


def get_trading_days(n: int) -> list[str]:
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(days))


def round_strike(price: float) -> int:
    return int(round(price / STEP) * STEP)


# ── Core backtest per day per leg ─────────────────────────────────────────────
def run_day_leg(df_1m: pd.DataFrame, leg: str, trade_date: str) -> list[dict]:
    """
    Simulate intraday 15-min trap entries on 1-min bars.
    Returns list of trade dicts.
    """
    trades = []
    if df_1m.empty or len(df_1m) < HTF_MIN:
        return trades

    # Track all 15-min bar endpoints seen so far
    known_zones: list[dict] = []      # all TRAPPED zones discovered today
    notified_uids: set = set()        # zones already used for entry (one entry per zone)
    position: Optional[dict] = None   # current open trade

    # Iterate 1-min bars in time order
    # Every time we cross a 15-min boundary, rebuild HTF and scan for new zones
    last_htf_ts = None

    for idx, row in df_1m.iterrows():
        ts   = row["datetime"]
        ltp  = row["close"]
        ts_str = ts.strftime("%H:%M")

        # ── Check EOD / SQ_OFF ────────────────────────────────────────────────
        if ts_str >= SQ_OFF:
            if position:
                reason = "EOD"
                pnl_pts = ltp - position["entry"]
                trades.append(_close(position, ltp, ts, reason))
                position = None
            break

        # ── Build up-to-now slice and resample to 15-min ─────────────────────
        df_so_far = df_1m.iloc[: idx + 1]
        htf = resample_to_htf(df_so_far, HTF_MIN)
        if htf.empty or len(htf) < 2:
            continue

        cur_htf_ts = htf.iloc[-1]["datetime"]

        # Only re-scan when a NEW 15-min bar closes
        if cur_htf_ts != last_htf_ts:
            last_htf_ts = cur_htf_ts
            # scan_htf(option_bars) → (df_events, entries)
            # entries = seller-trap zones on the OPTION PREMIUM
            # Same function for CE and PE — each runs on its own leg's bars
            _df_events, all_zones = scanner.scan_htf(htf)
            zones_raw = [z for z in all_zones if z.get("status") == "TRAPPED"]
            for z in zones_raw:
                uid = f"{z.get('zone_low',0):.2f}_{z.get('zone_high',0):.2f}"
                if uid not in notified_uids:
                    known_zones.append({**z, "_uid": uid})
                    # Don't add to notified_uids yet — wait for price to re-enter

        # ── Check exit on open position ───────────────────────────────────────
        if position:
            if ltp <= position["sl"]:
                trades.append(_close(position, ltp, ts, "SL"))
                position = None
            elif ltp >= position["t1"]:
                trades.append(_close(position, ltp, ts, "T1"))
                position = None
            # continue monitoring even if we'll look for new zones (no re-entry while in trade)
            continue

        # ── Look for entry ────────────────────────────────────────────────────
        for z in known_zones:
            uid = z["_uid"]
            if uid in notified_uids:
                continue
            zl  = z.get("zone_low",  0)
            zh  = z.get("zone_high", 0)
            zt  = z.get("zone_trigger", zl + (zh - zl) * 0.33)
            t1  = z.get("sl", zh + (zh - zl) * 1.5)   # scanner's sl field = bears' SL = our T1

            # Entry: price is inside zone AND has crossed the trigger
            if zl <= ltp <= zh and ltp >= zt:
                sl  = zl - SL_BUF
                position = {
                    "leg": leg, "date": trade_date,
                    "entry_ts": ts_str, "entry": ltp,
                    "zone": f"{zl:.1f}→{zh:.1f}", "trigger": round(zt, 2),
                    "t1": round(t1, 2), "sl": round(sl, 2),
                }
                notified_uids.add(uid)
                break   # one trade at a time

    return trades


def _close(pos: dict, exit_price: float, ts, reason: str) -> dict:
    pnl_pts = round(exit_price - pos["entry"], 2)
    pnl_rs  = round(pnl_pts * LOT, 2)
    return {**pos, "exit_ts": ts.strftime("%H:%M"), "exit": round(exit_price, 2),
            "reason": reason, "pnl_pts": pnl_pts, "pnl_rs": pnl_rs}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True, help="Upstox access token")
    ap.add_argument("--days",  type=int, default=7)
    ap.add_argument("--ce-only",  action="store_true")
    ap.add_argument("--pe-only",  action="store_true")
    args = ap.parse_args()

    _HEADERS["Authorization"] = f"Bearer {args.token}"
    _HEADERS["Accept"] = "application/json"

    legs = []
    if not args.pe_only: legs.append("CE")
    if not args.ce_only: legs.append("PE")

    trade_days = get_trading_days(args.days)
    print(f"\nSENSEX 15-min Intraday Backtest — {args.days} days: {trade_days[0]} → {trade_days[-1]}")
    print(f"Legs: {legs}  Lot: {LOT}  Step: {STEP}  SL_BUF: {SL_BUF}")

    all_trades: list[dict] = []

    for td in trade_days:
        print(f"\n{'─'*60}")
        print(f"DATE: {td}")

        # Get prev-day OHLC to compute ATM
        prev = fetch_spot_ohlc(td)
        if not prev:
            print(f"  No spot OHLC for {td}, skip")
            continue
        spot_prev_close = prev["close"]
        atm = round_strike(spot_prev_close)
        print(f"  Prev close: {spot_prev_close:.1f}  ATM: {atm}")

        td_date = date.fromisoformat(td)

        for leg in legs:
            strike = atm   # ATM for both CE and PE (simple intraday concept)
            key = find_option_key(strike, leg, td_date)
            if not key:
                print(f"  [{leg}] No instrument key for {strike}{leg} expiry≥{td}, skip")
                continue

            print(f"  [{leg}] Strike={strike}  Key={key}")
            df = fetch_1m(key, td)
            if df.empty:
                print(f"  [{leg}] No bars returned")
                continue
            print(f"  [{leg}] {len(df)} 1-min bars  "
                  f"LTP range: {df['close'].min():.1f}→{df['close'].max():.1f}")

            day_trades = run_day_leg(df, leg, td)
            if not day_trades:
                print(f"  [{leg}] No trades today")
            else:
                for t in day_trades:
                    print(f"  [{leg}] {t['entry_ts']}→{t['exit_ts']}  "
                          f"Zone {t['zone']}  Entry={t['entry']:.1f}  "
                          f"Exit={t['exit']:.1f}  {t['reason']}  "
                          f"PnL={t['pnl_pts']:+.1f}pts / ₹{t['pnl_rs']:+.0f}")
                all_trades.extend(day_trades)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"SUMMARY — {len(all_trades)} trades over {args.days} days")
    if not all_trades:
        print("No trades fired.")
        return

    df_t = pd.DataFrame(all_trades)
    total_rs  = df_t["pnl_rs"].sum()
    total_pts = df_t["pnl_pts"].sum()
    wins      = (df_t["pnl_rs"] > 0).sum()
    losses    = (df_t["pnl_rs"] < 0).sum()
    win_pct   = 100 * wins / len(df_t) if len(df_t) else 0

    print(f"  Win/Loss: {wins}W / {losses}L  ({win_pct:.0f}%)")
    print(f"  Total P&L: {total_pts:+.1f} pts  ₹{total_rs:+.0f}")
    print(f"  Avg per trade: {total_pts/len(df_t):+.1f} pts  ₹{total_rs/len(df_t):+.0f}")

    by_reason = df_t.groupby("reason")["pnl_rs"].agg(["count","sum"]).rename(
        columns={"count": "trades", "sum": "total_pnl"})
    print(f"\n  By exit reason:\n{by_reason.to_string()}")

    by_date = df_t.groupby("date")["pnl_rs"].sum()
    print(f"\n  Daily P&L:\n{by_date.to_string()}")

    # Full trade log
    print(f"\n  Full trade log:")
    cols = ["date","leg","entry_ts","exit_ts","zone","entry","exit","reason","pnl_pts","pnl_rs"]
    print(df_t[cols].to_string(index=False))


if __name__ == "__main__":
    main()
