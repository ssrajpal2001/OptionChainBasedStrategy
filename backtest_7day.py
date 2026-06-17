"""
7-Day TrapScanner Backtest — NIFTY, SENSEX, CRUDEOIL
=====================================================
Runs on the last 7 trading days (skips weekends automatically).
Uses the same scan logic as the live engine.

Usage:
    python backtest_7day.py [NIFTY|SENSEX|CRUDEOIL|ALL]

Output: tabular results per instrument per day with:
  Date | Scan Strike | Signal Side | Entry Time | Entry Price |
  SL Level | T1 Level | T1 Time | Exit Time | Exit Reason | P&L (Rs)
"""
import sys, os, asyncio
sys.path.insert(0, os.getcwd())

import aiohttp
import pandas as pd
from datetime import date, timedelta, time as dtime
from typing import Optional

from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY
from strategies.trap_scanner import scanner

# ── Config per instrument ─────────────────────────────────────────────────────
INST_CFG = {
    "NIFTY": {
        "step": 100, "lot": 75, "lots": 2,
        "htf_min": 75, "ltf_min": 5,
        "sl_buf_pct": 2.0,
        "gap_near": 200, "gap_far": 400,
        "htf_source": "option",   # scan option bars
        "eod_time": dtime(15, 20),
        "entry_window": None,     # all day
    },
    "SENSEX": {
        "step": 100, "lot": 20, "lots": 2,
        "htf_min": 75, "ltf_min": 5,
        "sl_buf_pct": 2.0,
        "gap_near": 200, "gap_far": 500,
        "htf_source": "option",
        "eod_time": dtime(15, 20),
        "entry_window": None,
    },
    "CRUDEOIL": {
        "step": 100, "lot": 100, "lots": 2,
        "htf_min": 75, "ltf_min": 5,
        "sl_buf_pct": 2.0,
        "gap_near": 200, "gap_far": 500,
        "htf_source": "futures",  # HTF + LTF on futures bars
        "eod_time": dtime(23, 0),
        "entry_window": (dtime(18, 45), dtime(19, 15)),
    },
}

DAYS_BACK = 10   # fetch 10 days to get 7 trading days


def _round_strike(price, step):
    return int(round(price / step) * step)


def _working_days(n: int = 7) -> list[str]:
    """Last n weekdays (Mon–Fri), newest first."""
    result = []
    d = date.today()
    while len(result) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon–Fri
            result.append(d.isoformat())
    return result


async def fetch_bars(key: str, token: str, days: int = DAYS_BACK) -> list:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    today   = date.today()
    to_date = today + timedelta(days=1)
    fr_date = today - timedelta(days=days)

    hist_url  = (f"https://api.upstox.com/v2/historical-candle/"
                 f"{key}/1minute/{to_date}/{fr_date}")
    intra_url = f"https://api.upstox.com/v2/historical-candle/intraday/{key}/1minute"

    all_bars, seen = [], set()
    async with aiohttp.ClientSession() as s:
        for url, label in [(hist_url, "hist"), (intra_url, "intra")]:
            try:
                async with s.get(url, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            except Exception:
                continue
            for c in reversed(data.get("data", {}).get("candles", [])):
                dt = c[0]
                if dt not in seen:
                    seen.add(dt)
                    all_bars.append({"datetime": dt, "open": float(c[1]),
                                     "high": float(c[2]), "low": float(c[3]),
                                     "close": float(c[4]), "volume": int(c[5])})
    all_bars.sort(key=lambda b: b["datetime"])
    return all_bars


def to_df(bars: list) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def resample_df(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    r = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return r


def simulate_trade(ltf_entry: dict, htf_zone: dict, df5: pd.DataFrame,
                   side: str, scan_label: str, cfg: dict, day: str) -> Optional[dict]:
    """
    Simulate one trade from signal to exit.
    Returns dict with all columns for the results table.
    """
    entry_ts = pd.to_datetime(ltf_entry.get("trapped_on") or ltf_entry.get("ref_ts"))
    if entry_ts is None:
        return None

    entry_price = float(ltf_entry.get("zone_trigger", ltf_entry.get("entry", 0)))
    opt_zone_low = float(ltf_entry.get("zone_low", 0))
    sl_pct_floor = entry_price * (1 - cfg["sl_buf_pct"] / 100)
    sl_price = max(opt_zone_low, sl_pct_floor) if opt_zone_low > 0 else sl_pct_floor

    # T1 = HTF zone's SL field (next resistance/support beyond the zone)
    t1_price = float(htf_zone.get("sl", 0))
    if t1_price <= 0 or t1_price <= entry_price:
        t1_price = entry_price * 1.05   # 5% fallback

    qty        = cfg["lot"] * cfg["lots"]
    t1_qty     = qty // 2
    eod_time   = cfg["eod_time"]
    win        = cfg.get("entry_window")
    entry_naive = entry_ts.tz_localize(None) if entry_ts.tzinfo else entry_ts

    # Entry window filter
    if win:
        if not (win[0] <= entry_naive.time() <= win[1]):
            return None

    # Iterate 5-min bars after entry
    future_bars = df5[
        df5["datetime"].apply(lambda x: (x.tz_localize(None) if x.tzinfo else x)) > entry_naive
    ].to_dict("records")

    t1_hit   = False
    t1_time  = None
    trail_sl = sl_price
    pnl      = 0.0
    exit_price  = None
    exit_time   = None
    exit_reason = None
    trap_states = {}
    qty_remaining = qty
    scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf

    for bar in future_bars:
        bt     = pd.to_datetime(bar["datetime"])
        bt_n   = bt.tz_localize(None) if bt.tzinfo else bt
        hi, lo, cl = bar["high"], bar["low"], bar["close"]

        # EOD
        if bt_n.time() >= eod_time:
            pnl += (cl - entry_price) * qty_remaining
            exit_price, exit_time, exit_reason = cl, bt, "EOD"
            break

        # T1
        if not t1_hit and t1_price > 0 and hi >= t1_price:
            pnl += (t1_price - entry_price) * t1_qty
            qty_remaining -= t1_qty
            t1_hit = True
            t1_time = bt_n
            trail_sl = entry_price    # CTC after T1
            print(f"      T1 @ {bt_n.strftime('%H:%M')} px={t1_price:.1f}")

        # Post-T1 trap-based trail SL
        if t1_hit and t1_time is not None:
            bars_so_far = df5[
                df5["datetime"].apply(lambda x: (x.tz_localize(None) if x.tzinfo else x)) <= bt_n
            ]
            if len(bars_so_far) >= 3:
                _, post_traps = scan_fn(bars_so_far,
                                        bars_so_far["high"].max(),
                                        bars_so_far["low"].min())
                for trap in post_traps:
                    if trap.get("status") != "TRAPPED":
                        continue
                    tts = pd.to_datetime(trap.get("trapped_on") or trap.get("ref_ts"))
                    if tts is None:
                        continue
                    tts_n = tts.tz_localize(None) if tts.tzinfo else tts
                    if tts_n <= t1_time:
                        continue
                    zt = trap.get("zone_trigger", 0)
                    zh = trap.get("zone_high", 0)
                    if zt <= entry_price or zh <= zt:
                        continue
                    key = tts_n.strftime("%H%M")
                    if key not in trap_states:
                        trap_states[key] = {"state": "WATCHING", "zone_trigger": zt, "zone_high": zh}

            for tk, ts_st in list(trap_states.items()):
                zh, zt = ts_st["zone_high"], ts_st["zone_trigger"]
                if ts_st["state"] == "WATCHING" and hi >= zh:
                    ts_st["state"] = "SQUEEZED"
                elif ts_st["state"] == "SQUEEZED" and lo <= zt:
                    ts_st["state"] = "PULLED_BACK"
                elif ts_st["state"] == "PULLED_BACK" and hi >= zh:
                    ts_st["state"] = "CONFIRMED"
                    trail_sl = max(trail_sl, zt)

        # SL
        active_sl = sl_price if not t1_hit else trail_sl
        if lo <= active_sl:
            pnl += (active_sl - entry_price) * qty_remaining
            exit_price  = active_sl
            exit_time   = bt
            exit_reason = "TRAIL_SL" if t1_hit else "SL"
            break

    if exit_time is None:
        exit_reason = "OPEN"

    entry_n = entry_naive
    return {
        "Date":          day,
        "Scan Strike":   scan_label,
        "Side":          side,
        "Entry Time":    entry_n.strftime("%H:%M"),
        "Entry Price":   round(entry_price, 1),
        "SL Level":      round(sl_price, 1),
        "T1 Level":      round(t1_price, 1),
        "T1 Hit":        "YES" if t1_hit else "NO",
        "T1 Time":       t1_time.strftime("%H:%M") if t1_time else "—",
        "Exit Time":     exit_time.strftime("%H:%M") if exit_time and exit_reason != "OPEN" else "—",
        "Exit Reason":   exit_reason or "OPEN",
        "Exit Price":    round(exit_price, 1) if exit_price else "—",
        "P&L (Rs)":      round(pnl),
    }


async def run_instrument(und: str, token: str) -> list[dict]:
    cfg = INST_CFG[und]
    results = []
    days = _working_days(7)

    print(f"\n{'='*60}")
    print(f"  {und} — last 7 trading days")
    print(f"{'='*60}")

    REGISTRY.load_sync(und, token)
    if not REGISTRY.is_loaded(und):
        print(f"  REGISTRY not loaded for {und}")
        return []

    expiry = REGISTRY.get_active_expiry(und)
    print(f"  Expiry: {expiry}")

    if cfg["htf_source"] == "futures":
        # CrudeOil: fetch futures bars
        fut_key = REGISTRY.historical_instrument_key(und) or ""
        if not fut_key:
            print(f"  No futures key for {und}")
            return []
        print(f"  Futures key: {fut_key}")
        print(f"  Fetching futures bars...")
        fut_bars = await fetch_bars(fut_key, token, DAYS_BACK)
        fut_df   = to_df(fut_bars)

        # Get ATM-based option keys for CE1/PE1
        # Use today's open to determine strikes (as live engine does)
        today_str = date.today().isoformat()
        today_fut = [b for b in fut_bars if b["datetime"][:10] == today_str]
        if not today_fut:
            # Use last day's close
            last_close = fut_bars[-1]["close"] if fut_bars else 7000
        else:
            last_close = today_fut[0]["open"]
        atm = _round_strike(last_close, cfg["step"])
        ce1_strike = atm - cfg["gap_near"]
        pe1_strike = atm + cfg["gap_near"]
        ce1_key = REGISTRY.get_upstox_key(und, expiry, ce1_strike, "CE") or ""
        pe1_key = REGISTRY.get_upstox_key(und, expiry, pe1_strike, "PE") or ""
        print(f"  ATM={atm}  CE1={ce1_strike}({ce1_key})  PE1={pe1_strike}({pe1_key})")

        # For CrudeOil: both HTF and LTF on futures bars
        for day in reversed(days):
            day_fut = [b for b in fut_bars if b["datetime"][:10] == day]
            if not day_fut:
                print(f"  {day}: no futures bars — skip")
                continue
            day_df = to_df(day_fut)
            htf_df = resample_df(day_df, cfg["htf_min"])
            ltf_df = resample_df(day_df, cfg["ltf_min"])
            if len(htf_df) < 2 or len(ltf_df) < 3:
                print(f"  {day}: not enough bars (HTF={len(htf_df)} LTF={len(ltf_df)})")
                continue

            _, bear_zones = scanner.scan_htf(htf_df)
            trapped_bear = [z for z in bear_zones if z["status"] == "TRAPPED"]
            print(f"\n  {day}: HTF bear zones TRAPPED={len(trapped_bear)}")

            entries_today = []
            for zone in trapped_bear:
                zh, zl = zone.get("zone_high", 0), zone.get("zone_low", 0)
                trig    = zone.get("zone_trigger", 0)
                print(f"    Zone [{zl:.0f}–{zh:.0f}] trigger={trig:.0f}")
                _, ltf_entries = scanner.scan_ltf(
                    ltf_df,
                    htf_zone_high=zh, htf_zone_low=zl,
                    htf_ref_bar=str(zone.get("ref_ts", "")),
                    htf_trap_bar=str(zone.get("trapped_on", "")),
                    htf_target=zone.get("sl", 0.0),
                )
                best = scanner.select_best_ltf_entry(ltf_entries)
                if best:
                    entries_today.append((best, zone, "CE", f"{ce1_strike}CE"))

            for ltf_entry, htf_zone, side, slabel in entries_today:
                r = simulate_trade(ltf_entry, htf_zone, ltf_df, side, slabel, cfg, day)
                if r:
                    results.append(r)
                    print(f"    TRADE: {slabel} entry={r['Entry Time']} px={r['Entry Price']}"
                          f" sl={r['SL Level']} t1={r['T1 Level']} "
                          f"exit={r['Exit Time']} reason={r['Exit Reason']} P&L=Rs{r['P&L (Rs)']}")

    else:
        # NIFTY / SENSEX: HTF and LTF on option bars
        today_str = date.today().isoformat()

        # Use the existing option keys from the hardcoded SENSEX backtest for recent days
        # or dynamically resolve from REGISTRY
        # We'll resolve fresh from REGISTRY using today's expiry
        # Need to get ATM from index spot — fetch index bars
        spot_keys = {
            "NIFTY":  "NSE_INDEX|Nifty 50",
            "SENSEX": "BSE_INDEX|SENSEX",
        }
        spot_key = spot_keys.get(und, "")
        print(f"  Fetching spot bars for ATM determination...")
        spot_bars = await fetch_bars(spot_key, token, DAYS_BACK) if spot_key else []
        spot_df   = to_df(spot_bars)

        for day in reversed(days):
            day_spot = [b for b in spot_bars if b["datetime"][:10] == day] if spot_bars else []
            if not day_spot:
                print(f"  {day}: no spot bars — skip")
                continue
            # Gap direction: PREV DAY CLOSE vs TODAY OPENING (first bar open, NOT current price)
            # spot_bars sorted ascending → day_spot[0] = first bar of day = true market open
            # reversed(spot_bars) newest-first → first match date < day = prev day last close
            today_open = day_spot[0]["open"]
            prev_close = next((b["close"] for b in reversed(spot_bars)
                               if b["datetime"][:10] < day), today_open)
            direction = "UP" if today_open >= prev_close else "DOWN"
            atm = _round_strike(today_open, cfg["step"])
            if direction == "UP":
                ce1_strike = atm - cfg["gap_near"]
                pe1_strike = atm + cfg["gap_near"]
            else:
                ce1_strike = atm + cfg["gap_near"]
                pe1_strike = atm - cfg["gap_near"]

            ce1_key = REGISTRY.get_upstox_key(und, expiry, ce1_strike, "CE") or ""
            pe1_key = REGISTRY.get_upstox_key(und, expiry, pe1_strike, "PE") or ""
            if not ce1_key or not pe1_key:
                print(f"  {day}: {ce1_strike}CE or {pe1_strike}PE not in REGISTRY — skip")
                continue

            print(f"\n  {day}: open={today_open:.0f} dir={direction} ATM={atm}"
                  f" CE1={ce1_strike} PE1={pe1_strike}")

            ce1_bars = await fetch_bars(ce1_key, token, DAYS_BACK)
            pe1_bars = await fetch_bars(pe1_key, token, DAYS_BACK)
            ce1_day = [b for b in ce1_bars if b["datetime"][:10] == day]
            pe1_day = [b for b in pe1_bars if b["datetime"][:10] == day]

            for opt_day, opt_all, side, scan_strike, scan_key_str in [
                (ce1_day, ce1_bars, "CE", ce1_strike, ce1_key),
                (pe1_day, pe1_bars, "PE", pe1_strike, pe1_key),
            ]:
                if not opt_day:
                    print(f"    {side}: no bars for {day}")
                    continue
                day_df    = to_df(opt_day)
                htf_df    = resample_df(to_df(opt_all), cfg["htf_min"])
                ltf_df    = resample_df(day_df, cfg["ltf_min"])
                if len(htf_df) < 2 or len(ltf_df) < 3:
                    continue

                scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_htf
                _, zones = scanner.scan_htf(htf_df)
                trapped  = [z for z in zones if z["status"] == "TRAPPED"]
                print(f"    {side} ({scan_strike}): HTF TRAPPED={len(trapped)}")

                for zone in trapped:
                    zh, zl = zone.get("zone_high", 0), zone.get("zone_low", 0)
                    ltf_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf
                    _, ltf_entries = ltf_fn(
                        ltf_df,
                        htf_zone_high=zh, htf_zone_low=zl,
                        htf_ref_bar=str(zone.get("ref_ts", "")),
                        htf_trap_bar=str(zone.get("trapped_on", "")),
                        htf_target=zone.get("sl", 0.0),
                    )
                    best = scanner.select_best_ltf_entry(ltf_entries)
                    if best:
                        slabel = f"{scan_strike}{side}"
                        r = simulate_trade(best, zone, ltf_df, side, slabel, cfg, day)
                        if r:
                            results.append(r)
                            print(f"    TRADE: {slabel} entry={r['Entry Time']}"
                                  f" px={r['Entry Price']} sl={r['SL Level']}"
                                  f" t1={r['T1 Level']} exit={r['Exit Time']}"
                                  f" reason={r['Exit Reason']} P&L=Rs{r['P&L (Rs)']}")

    return results


def print_table(results: list[dict], und: str) -> None:
    if not results:
        print(f"\n  {und}: No trades found in last 7 days")
        return
    df = pd.DataFrame(results)
    total = df["P&L (Rs)"].sum() if "P&L (Rs)" in df.columns else 0
    wins  = (df["P&L (Rs)"] > 0).sum()
    total_t = len(df)

    print(f"\n{'='*100}")
    print(f"  {und} — {total_t} trades | {wins}W/{total_t-wins}L | Total P&L: Rs {total:,.0f}")
    print(f"{'='*100}")
    print(df.to_string(index=False))
    print(f"{'='*100}")


async def main():
    db    = ClientDB("data/clients.db")
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token") or ""
    if not token:
        print("No Upstox token in DB — start the app first to refresh it")
        return

    instruments = sys.argv[1].upper().split(",") if len(sys.argv) > 1 else ["ALL"]
    if "ALL" in instruments:
        instruments = ["NIFTY", "SENSEX", "CRUDEOIL"]

    all_results = {}
    for und in instruments:
        if und not in INST_CFG:
            print(f"Unknown instrument: {und}")
            continue
        results = await run_instrument(und, token)
        all_results[und] = results

    print("\n\n" + "="*100)
    print("  SUMMARY — ALL INSTRUMENTS")
    print("="*100)
    for und, results in all_results.items():
        print_table(results, und)


if __name__ == "__main__":
    asyncio.run(main())
