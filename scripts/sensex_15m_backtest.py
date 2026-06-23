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
    """Find BSE_FO instrument key for SENSEX option.

    BSE master quirks vs NSE:
      - strike field is 'strike_price' (not 'strike')
      - expiry is epoch-milliseconds integer (not ISO string)
      - underlying_symbol must match exactly 'SENSEX' (SENSEX50 is a different product)
    """
    master = _load_bse_master()
    ot = otype.upper()
    candidates = []
    for row in master:
        # Only BSE F&O instruments
        if not str(row.get("instrument_key", "")).startswith("BSE_FO|"):
            continue
        # Exact underlying match — SENSEX ≠ SENSEX50
        row_und = str(row.get("underlying_symbol", "") or row.get("asset_symbol", "") or "").upper()
        if row_und != "SENSEX":
            continue
        # Option type (instrument_type = "CE"/"PE" for BSE_FO options)
        itype = str(row.get("instrument_type", "")).upper()
        row_ot = itype if itype in ("CE", "PE") else str(row.get("option_type", "")).upper()
        if row_ot != ot:
            continue
        # Strike: BSE master uses 'strike_price', not 'strike'
        row_strike = float(row.get("strike_price", 0) or row.get("strike", 0) or 0)
        if row_strike <= 0 or abs(row_strike - strike) > 0.5:
            continue
        # Expiry: epoch-ms integer in BSE master
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
        key = str(row.get("instrument_key", ""))
        if key:
            candidates.append((exp_dt, key))
    if not candidates:
        sample = next((r for r in master if str(r.get("instrument_key","")).startswith("BSE_FO|")), None)
        if sample:
            print(f"    [DEBUG] ALL BSE_FO keys: {list(sample.keys())}")
            print(f"    [DEBUG] strike_price={sample.get('strike_price')} "
                  f"underlying_symbol={sample.get('underlying_symbol')} "
                  f"asset_symbol={sample.get('asset_symbol')} name={sample.get('name')}")
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
def run_day_leg(df_1m: pd.DataFrame, leg: str, trade_date: str,
                max_trades: int = 999, cutoff: str = "15:25",
                min_rr: float = 0.0, ltf_min: int = 1,
                max_consec_sl: int = 999) -> list[dict]:
    """
    Simulate intraday 15-min trap entries on 1-min bars.

    Filters:
      max_trades    – max entries per day per leg
      cutoff        – no new entries at or after HH:MM
      min_rr        – minimum reward:risk (e.g. 2.0 = must offer 1:2)
      ltf_min       – LTF timeframe for entry confirmation + SL fires
                      (1 = every 1-min close, 3 = only on 3-min bar close)
      max_consec_sl – stop new entries after N consecutive pre-T1 SLs

    TSL target ladder (post-T1):
      T1 = zone sl field (1R), T2 = T1+1R, T3 = T1+2R
      T1 hit → SL to entry(BE), T2 hit → SL to T1, T3 hit → SL to T2
      TSL: trail to (prev LTF bar low − SL_BUF), only moves up
    """
    trades = []
    if df_1m.empty or len(df_1m) < HTF_MIN:
        return trades

    known_zones: list[dict] = []
    notified_uids: set = set()
    position: Optional[dict] = None
    trades_taken = 0
    consec_sl    = 0
    last_htf_ts  = None
    last_ltf_ts  = None

    for idx, row in df_1m.iterrows():
        ts     = row["datetime"]
        ltp    = row["close"]
        ts_str = ts.strftime("%H:%M")

        # ── EOD (every 1-min tick) ─────────────────────────────────────────────
        if ts_str >= SQ_OFF:
            if position:
                trades.append(_close(position, ltp, ts, "EOD"))
                position = None
            break

        # ── 15-min HTF zone scan ───────────────────────────────────────────────
        df_so_far = df_1m.iloc[: idx + 1]
        htf = resample_to_htf(df_so_far, HTF_MIN)
        if htf.empty or len(htf) < 2:
            continue

        cur_htf_ts = htf.iloc[-1]["datetime"]
        if cur_htf_ts != last_htf_ts:
            last_htf_ts = cur_htf_ts
            _df_events, all_zones = scanner.scan_htf(htf)
            zones_raw = [z for z in all_zones if z.get("status") == "TRAPPED"]
            for z in zones_raw:
                uid = f"{z.get('zone_low',0):.2f}_{z.get('zone_high',0):.2f}"
                if uid not in notified_uids:
                    known_zones.append({**z, "_uid": uid})

        # ── Determine bar_close / bar_low for SL+entry checks ─────────────────
        # ltf_min=1: use every 1-min close; ltf_min>1: only on new LTF bar close
        if ltf_min > 1:
            htf_ltf = resample_to_htf(df_so_far, ltf_min)
            if len(htf_ltf) < 2:
                continue
            cur_ltf_ts = htf_ltf.iloc[-1]["datetime"]
            if cur_ltf_ts == last_ltf_ts:
                continue   # same LTF bar still forming — skip SL/entry this tick
            last_ltf_ts = cur_ltf_ts
            prev_bar  = htf_ltf.iloc[-2]
            bar_close = prev_bar["close"]
            bar_low   = prev_bar["low"]
        else:
            bar_close = ltp
            bar_low   = row["low"]

        # ── TSL update: trail to prev LTF bar low (post-T1 only) ──────────────
        if position and position.get("stage", 0) >= 1:
            new_trail = round(bar_low - SL_BUF, 2)
            if new_trail > position["sl"]:
                position["sl"] = new_trail

        # ── Exit + target-ladder on LTF bar close ─────────────────────────────
        if position:
            stage   = position.get("stage", 0)
            targets = position["targets"]

            if bar_close <= position["sl"]:
                reason = f"SL(stage{stage})" if stage > 0 else "SL"
                trades.append(_close(position, bar_close, ts, reason))
                consec_sl = consec_sl + 1 if stage == 0 else 0
                position  = None
            elif stage < len(targets) and bar_close >= targets[stage]:
                if stage == 0:
                    position["sl"] = round(position["entry"], 2)    # T1 → BE
                else:
                    position["sl"] = round(targets[stage - 1], 2)   # Tn → T(n-1)
                position["stage"] = stage + 1
                consec_sl = 0   # runner advanced = reset streak
            continue   # in trade or just closed, skip entry

        # ── Entry filters ──────────────────────────────────────────────────────
        if ts_str >= cutoff or trades_taken >= max_trades or consec_sl >= max_consec_sl:
            continue

        # ── Entry check on LTF bar close ───────────────────────────────────────
        for z in known_zones:
            uid = z["_uid"]
            if uid in notified_uids:
                continue
            zl = z.get("zone_low",  0)
            zh = z.get("zone_high", 0)
            zt = z.get("zone_trigger", zl + (zh - zl) * 0.33)
            t1 = z.get("sl", zh + (zh - zl) * 1.5)

            if not (zl <= bar_close <= zh and bar_close >= zt):
                continue

            sl     = zl - SL_BUF
            risk   = bar_close - sl
            reward = t1 - bar_close
            if risk <= 0:
                continue
            if min_rr > 0 and reward / risk < min_rr:
                notified_uids.add(uid)
                continue

            targets = [round(t1, 2), round(t1 + reward, 2), round(t1 + 2 * reward, 2)]
            position = {
                "leg": leg, "date": trade_date,
                "entry_ts": ts_str, "entry": bar_close,
                "zone": f"{zl:.1f}→{zh:.1f}", "trigger": round(zt, 2),
                "t1": targets[0], "sl": round(sl, 2),
                "targets": targets, "stage": 0,
                "rr": round(reward / risk, 2),
            }
            notified_uids.add(uid)
            trades_taken += 1
            consec_sl = 0
            break

    return trades


def _close(pos: dict, exit_price: float, ts, reason: str) -> dict:
    pnl_pts = round(exit_price - pos["entry"], 2)
    pnl_rs  = round(pnl_pts * LOT, 2)
    return {**pos, "exit_ts": ts.strftime("%H:%M"), "exit": round(exit_price, 2),
            "reason": reason, "pnl_pts": pnl_pts, "pnl_rs": pnl_rs}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token",      required=True, help="Upstox access token")
    ap.add_argument("--days",       type=int,   default=7)
    ap.add_argument("--max-trades", type=int,   default=999,
                    help="Max entries per day per leg (default: unlimited)")
    ap.add_argument("--cutoff",     default="15:25",
                    help="No new entries at or after HH:MM (default: 15:25)")
    ap.add_argument("--min-rr",          type=float, default=0.0,
                    help="Minimum reward:risk ratio (e.g. 2.0 = 1:2 R:R, default: off)")
    ap.add_argument("--ltf",             type=int,   default=1,
                    help="LTF minutes for entry confirmation + SL (1=every tick, 3=3-min close)")
    ap.add_argument("--max-consec-sl",   type=int,   default=999,
                    help="Stop new entries after N consecutive pre-T1 SLs same leg (default: off)")
    ap.add_argument("--ce-only",    action="store_true")
    ap.add_argument("--pe-only",    action="store_true")
    args = ap.parse_args()

    _HEADERS["Authorization"] = f"Bearer {args.token}"
    _HEADERS["Accept"] = "application/json"

    legs = []
    if not args.pe_only: legs.append("CE")
    if not args.ce_only: legs.append("PE")

    trade_days = get_trading_days(args.days)
    print(f"\nSENSEX 15-min Intraday Backtest — {args.days} days: {trade_days[0]} → {trade_days[-1]}")
    print(f"Legs: {legs}  Lot: {LOT}  Step: {STEP}  SL_BUF: {SL_BUF}")
    filters = []
    if args.max_trades < 999:    filters.append(f"max_trades={args.max_trades}/leg/day")
    if args.cutoff != "15:25":   filters.append(f"cutoff={args.cutoff}")
    if args.min_rr > 0:          filters.append(f"min_R:R=1:{args.min_rr:.0f}")
    if args.ltf > 1:             filters.append(f"ltf={args.ltf}m (entry+SL on bar close)")
    if args.max_consec_sl < 999: filters.append(f"max_consec_sl={args.max_consec_sl}")
    if filters: print(f"Filters: {', '.join(filters)}")

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

            day_trades = run_day_leg(df, leg, td,
                                     max_trades=args.max_trades,
                                     cutoff=args.cutoff,
                                     min_rr=args.min_rr,
                                     ltf_min=args.ltf,
                                     max_consec_sl=args.max_consec_sl)
            if not day_trades:
                print(f"  [{leg}] No trades today")
            else:
                for t in day_trades:
                    print(f"  [{leg}] {t['entry_ts']}→{t['exit_ts']}  "
                          f"Zone {t['zone']}  Entry={t['entry']:.1f}  "
                          f"Exit={t['exit']:.1f}  {t['reason']}  "
                          f"R:R={t.get('rr',0):.1f}  "
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
    cols = ["date","leg","entry_ts","exit_ts","zone","entry","exit","reason","rr","pnl_pts","pnl_rs"]
    available = [c for c in cols if c in df_t.columns]
    print(df_t[available].to_string(index=False))


if __name__ == "__main__":
    main()
