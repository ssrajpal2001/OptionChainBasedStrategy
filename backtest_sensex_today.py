"""
Backtest TrapScanner on SENSEX for today (Jun 17, 2026).

Keys confirmed from check_sensex_expiry.py:
  Expiry: 2026-06-18
  CE1=76800 to BSE_FO|1145316
  CE2=76500 to BSE_FO|1148517
  PE1=77400 to BSE_FO|1140561
  PE2=77700 to BSE_FO|1142448

Gap UP 1.1% to CE1=76800 (near ITM call), PE1=77400 (near ITM put)
HTF=75min on CE1 bars for BEAR zones, PE1 bars for BULL zones
LTF=5min entry inside TRAPPED HTF zone
2 lots × 20 = 40 qty
T1 = 50% qty at HTF zone target
Trail = 5-min ratchet on remaining qty
"""
import sys, os, asyncio, json
sys.path.insert(0, os.getcwd())

import aiohttp
import pandas as pd
from datetime import date, timedelta
from strategies.trap_scanner import scanner

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFFNDciLCJqdGkiOiI2YTMyMTk4ZTRmNWFmZDdkOTUyM2YxMzgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjY4MjM4LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE3MzM2MDB9.Ypfp_KNM6CZoZyOf3MRASdrGdOHEJJioAGYqunIAVYY"

KEYS = {
    "CE1": "BSE_FO|1145316",   # 76800 CE
    "CE2": "BSE_FO|1148517",   # 76500 CE
    "PE1": "BSE_FO|1140561",   # 77400 PE
    "PE2": "BSE_FO|1142448",   # 77700 PE
}

LOT_SIZE   = 20
LOTS       = 2
QTY        = LOT_SIZE * LOTS   # 40
HTF_MIN    = 75
LTF_MIN    = 5
SL_BUF_PCT = 2.0


async def fetch_bars(key: str, days: int = 8) -> list:
    """Fetch historical bars (completed sessions) + today's intraday bars merged."""
    headers = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
    today   = date.today()
    to_date = today + timedelta(days=1)
    fr_date = today - timedelta(days=days)

    # 1. Historical (completed sessions)
    hist_url = (f"https://api.upstox.com/v2/historical-candle/"
                f"{key}/1minute/{to_date}/{fr_date}")
    # 2. Today's intraday (live session — Upstox intraday endpoint)
    intra_url = f"https://api.upstox.com/v2/historical-candle/intraday/{key}/1minute"

    all_bars = []
    seen_dts = set()

    async with aiohttp.ClientSession() as s:
        for url, label in [(hist_url, "hist"), (intra_url, "intraday")]:
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != 200:
                    body = await r.text()
                    print(f"    {label} HTTP {r.status}: {body[:120]}")
                    continue
                data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            for c in reversed(candles):
                dt = c[0]
                if dt not in seen_dts:
                    seen_dts.add(dt)
                    all_bars.append({"datetime": dt, "open": float(c[1]), "high": float(c[2]),
                                     "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])})
            print(f"    {key} [{label}]: {len(candles)} candles")

    all_bars.sort(key=lambda b: b["datetime"])
    print(f"  {key}: {len(all_bars)} bars total  "
          f"({all_bars[0]['datetime'][:10] if all_bars else '-'} to "
          f"{all_bars[-1]['datetime'][:10] if all_bars else '-'})")
    return all_bars


def to_df(bars):
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def resample_htf(df, minutes):
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def simulate_trade(ltf_entry, htf_zone, df5, side, label):
    """Simulate entry + T1 + trap-based trail on 5-min bars.

    After T1 (50% exit at zone_high):
      - SL moves to CTC (entry price = breakeven)
      - Watch for new 5-min bear traps forming ABOVE entry
      - When a new bear trap's zone_high is taken out (bears squeezed),
        trail_sl jumps to that trap's zone_trigger (where bears entered)
      - Repeat each time a new bear trap is squeezed out
    """
    entry_time  = pd.to_datetime(ltf_entry.get("trapped_on") or ltf_entry.get("ref_ts"))
    if entry_time is pd.NaT or entry_time is None:
        return None
    entry_price = ltf_entry.get("zone_trigger", ltf_entry.get("entry", 0))
    sl_from_zone = ltf_entry.get("zone_low", 0)
    sl_buf       = entry_price * (1 - SL_BUF_PCT / 100)
    sl_price     = max(sl_from_zone, sl_buf) if sl_from_zone > 0 else sl_buf
    t1_target    = htf_zone.get("zone_high", htf_zone.get("sl", 0))
    zone_high    = htf_zone.get("zone_high", 0)
    zone_low     = htf_zone.get("zone_low", 0)

    qty_remaining = QTY
    qty_t1        = QTY // 2
    t1_done       = False
    t1_time       = None
    trail_sl      = sl_price
    pnl           = 0.0
    exit_price    = None
    exit_time     = None
    exit_reason   = None

    # Pre-scan all 5-min bars for LTF traps — used for post-T1 trail detection
    scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf
    bars_list = df5.to_dict("records")

    # entry_time may be tz-aware; normalize for comparisons
    entry_time_naive = entry_time.tz_localize(None) if entry_time.tzinfo else entry_time

    future_bars = [b for b in bars_list
                   if (pd.to_datetime(b["datetime"]).tz_localize(None)
                       if pd.to_datetime(b["datetime"]).tzinfo
                       else pd.to_datetime(b["datetime"])) > entry_time_naive]

    print(f"\n  [{label}] ENTRY @ {entry_time.strftime('%H:%M')} price={entry_price:.1f} "
          f"SL={sl_price:.1f} T1_target={t1_target:.1f} zone=[{zone_low:.1f}-{zone_high:.1f}]")

    # Per-trap state machine for post-T1 trail:
    #   WATCHING      → bears_sl (zone_high) hit         → SQUEEZED
    #   SQUEEZED      → price pulls back to zone_trigger  → PULLED_BACK
    #   PULLED_BACK   → price bounces (close > zone_trig) → CONFIRMED → update trail_sl
    trap_states = {}   # trap_key → {"state": ..., "zone_trigger": ..., "zone_high": ...}

    for bar in future_bars:
        bt       = pd.to_datetime(bar["datetime"])
        bt_naive = bt.tz_localize(None) if bt.tzinfo else bt
        hi       = bar["high"]
        lo       = bar["low"]
        cl       = bar["close"]

        # T1: 50% qty at zone_high target → SL moves to CTC (entry)
        if not t1_done and t1_target > 0 and hi >= t1_target:
            t1_pnl = (t1_target - entry_price) * qty_t1
            pnl += t1_pnl
            qty_remaining -= qty_t1
            t1_done = True
            t1_time = bt_naive
            trail_sl = entry_price   # CTC: breakeven stop on runner
            print(f"    T1 @ {bt.strftime('%H:%M')} price={t1_target:.1f}  qty={qty_t1}  "
                  f"pnl_so_far={pnl:.0f}  trail_sl=CTC({trail_sl:.1f})")

        # Post-T1: scan bars up to now, find new bear traps formed after T1
        if t1_done and t1_time is not None:
            bars_so_far = df5[
                df5["datetime"].apply(lambda x: x.tz_localize(None) if x.tzinfo else x) <= bt_naive
            ]
            if len(bars_so_far) >= 3:
                _, post_traps = scan_fn(bars_so_far,
                                        bars_so_far["high"].max(),
                                        bars_so_far["low"].min())
                for trap in post_traps:
                    if trap.get("status") != "TRAPPED":
                        continue
                    trap_ts = pd.to_datetime(trap.get("trapped_on") or trap.get("ref_ts"))
                    if trap_ts is None:
                        continue
                    trap_ts_naive = trap_ts.tz_localize(None) if trap_ts.tzinfo else trap_ts
                    if trap_ts_naive <= t1_time:
                        continue
                    trap_entry = trap.get("zone_trigger", 0)
                    if trap_entry <= entry_price:
                        continue
                    trap_key = trap_ts_naive
                    # Register new trap
                    if trap_key not in trap_states:
                        trap_states[trap_key] = {
                            "state": "WATCHING",
                            "zone_trigger": trap_entry,
                            "zone_high": trap.get("zone_high", 0),
                        }

            # Advance state machine for all known post-T1 traps
            for tk, ts in list(trap_states.items()):
                if ts["state"] == "CONFIRMED":
                    continue
                zh = ts["zone_high"]
                zt = ts["zone_trigger"]

                if ts["state"] == "WATCHING":
                    # Step 1: bears' SL (zone_high) must be hit — bears squeezed
                    if hi >= zh:
                        ts["state"] = "SQUEEZED"
                        print(f"    BEARS SQUEEZED @ {bt.strftime('%H:%M')}  "
                              f"bears_entry={zt:.1f}  bears_sl={zh:.1f} HIT")

                elif ts["state"] == "SQUEEZED":
                    # Step 2: price pulls back to bears' entry (zone_trigger)
                    if lo <= zt:
                        ts["state"] = "PULLED_BACK"
                        print(f"    PULLBACK to bears entry={zt:.1f} @ {bt.strftime('%H:%M')}")

                elif ts["state"] == "PULLED_BACK":
                    # Step 3: price goes back ABOVE zone_high again (full confirmation)
                    # Market came back to zone_trigger (step 2), held, now above zone_high
                    if hi >= zh:
                        ts["state"] = "CONFIRMED"
                        new_trail = zt
                        if new_trail > trail_sl:
                            print(f"    TRAIL SL STEP UP @ {bt.strftime('%H:%M')}  "
                                  f"zone_trigger={zt:.1f} held as support, market above {zh:.1f} again  "
                                  f"trail_sl: {trail_sl:.1f} -> {new_trail:.1f}")
                            trail_sl = new_trail

        # SL hit check
        active_sl = sl_price if not t1_done else trail_sl
        if lo <= active_sl:
            sl_pnl = (active_sl - entry_price) * qty_remaining
            pnl += sl_pnl
            exit_price  = active_sl
            exit_time   = bt
            exit_reason = "TRAIL_SL" if t1_done else "SL"
            break

        # EOD 15:20
        if bt.hour > 15 or (bt.hour == 15 and bt.minute >= 20):
            eod_pnl = (cl - entry_price) * qty_remaining
            pnl += eod_pnl
            exit_price  = cl
            exit_time   = bt
            exit_reason = "EOD"
            break

    if exit_time is None:
        exit_reason = "OPEN"

    ep_str = f"{exit_price:.1f}" if exit_price is not None else "-"
    et_str = exit_time.strftime('%H:%M') if exit_time is not None else "-"
    print(f"    EXIT @ {et_str}  reason={exit_reason}  exit_px={ep_str}  NET P&L = Rs{pnl:.0f}")
    return {"label": label, "entry": entry_price, "entry_time": str(entry_time),
            "exit": exit_price, "exit_time": str(exit_time), "reason": exit_reason,
            "pnl": pnl, "t1_done": t1_done}


async def main():
    print("=" * 60)
    print("SENSEX TrapScanner Backtest — 17 Jun 2026")
    print(f"Lots={LOTS}  Qty={QTY}  HTF={HTF_MIN}m  LTF={LTF_MIN}m")
    print("=" * 60)

    print("\nFetching bars...")
    ce1_bars = await fetch_bars(KEYS["CE1"])
    pe1_bars = await fetch_bars(KEYS["PE1"])

    if not ce1_bars or not pe1_bars:
        print("No bars — check token / keys")
        return

    today = date.today().isoformat()

    # Split into seed (HTF) and today (LTF)
    ce1_all = to_df(ce1_bars)
    pe1_all = to_df(pe1_bars)

    ce1_today_bars = [b for b in ce1_bars if b["datetime"][:10] == today]
    pe1_today_bars = [b for b in pe1_bars if b["datetime"][:10] == today]
    print(f"\nToday bars: CE1={len(ce1_today_bars)}  PE1={len(pe1_today_bars)}")

    # ---- HTF scan (75-min on ALL bars for historical zones) ------------------------------------
    print("\n---- HTF Scan (75-min) ----")
    htf_ce1 = resample_htf(ce1_all, HTF_MIN)
    htf_pe1 = resample_htf(pe1_all, HTF_MIN)
    print(f"HTF CE1 bars: {len(htf_ce1)}  HTF PE1 bars: {len(htf_pe1)}")

    _, bear_zones = scanner.scan_htf(htf_ce1) if len(htf_ce1) >= 2 else ([], [])
    _, bull_zones = scanner.scan_htf(htf_pe1) if len(htf_pe1) >= 2 else ([], [])

    trapped_bear = [z for z in bear_zones if z["status"] == "TRAPPED"]
    trapped_bull = [z for z in bull_zones if z["status"] == "TRAPPED"]
    print(f"Bear zones (TRAPPED): {len(trapped_bear)}")
    for z in trapped_bear:
        print(f"  zone [{z['zone_low']:.1f}-{z['zone_high']:.1f}]  trigger={z.get('zone_trigger',0):.1f}  t1={z.get('t1_target',0):.1f}")
    print(f"Bull zones (TRAPPED): {len(trapped_bull)}")
    for z in trapped_bull:
        print(f"  zone [{z['zone_low']:.1f}-{z['zone_high']:.1f}]  trigger={z.get('zone_trigger',0):.1f}  t1={z.get('t1_target',0):.1f}")

    # ---- Cascade: 15-min scan on today's bars ----------------------------------------------------------------
    print("\n---- Cascade Scan (15-min on today) ----")
    ce1_today_df = to_df(ce1_today_bars)
    pe1_today_df = to_df(pe1_today_bars)
    htf15_ce1 = resample_htf(ce1_today_df, 15) if not ce1_today_df.empty else pd.DataFrame()
    htf15_pe1 = resample_htf(pe1_today_df, 15) if not pe1_today_df.empty else pd.DataFrame()
    print(f"15-min CE1 bars today: {len(htf15_ce1)}  PE1: {len(htf15_pe1)}")

    _, casc_bear = scanner.scan_htf(htf15_ce1) if len(htf15_ce1) >= 2 else ([], [])
    _, casc_bull = scanner.scan_htf(htf15_pe1) if len(htf15_pe1) >= 2 else ([], [])
    casc_bear_t = [z for z in casc_bear if z["status"] == "TRAPPED"]
    casc_bull_t = [z for z in casc_bull if z["status"] == "TRAPPED"]
    print(f"15-min bear TRAPPED: {len(casc_bear_t)}  bull TRAPPED: {len(casc_bull_t)}")

    # ---- LTF scan (5-min on today's bars) ------------------------------------------------------------------------
    print("\n---- LTF Scan (5-min entries) ----")
    all_results = []

    # Use HTF zones + cascade zones (union) — HTF zones from old data may be
    # out of today's premium range; cascade fills the gap with intraday 15-min zones
    bear_zones_all = trapped_bear + [z for z in casc_bear_t
                                     if not any(abs(z["zone_high"] - h["zone_high"]) < 1
                                                for h in trapped_bear)]
    bull_zones_all = trapped_bull + [z for z in casc_bull_t
                                     if not any(abs(z["zone_high"] - h["zone_high"]) < 1
                                                for h in trapped_bull)]
    print(f"\nCombined zones: bear={len(bear_zones_all)}  bull={len(bull_zones_all)}")

    for label, opt_bars_today, zones, side in [
        ("CE1-BEAR", ce1_today_bars, bear_zones_all, "CE"),
        ("PE1-BULL", pe1_today_bars, bull_zones_all, "PE"),
    ]:
        if not zones:
            print(f"  {label}: no zones to skip")
            continue
        df5 = resample_htf(to_df(opt_bars_today), LTF_MIN)
        if df5.empty or len(df5) < 3:
            print(f"  {label}: not enough 5-min bars ({len(df5)})")
            continue
        print(f"  5-min bars today: {len(df5)}  "
              f"close range [{df5['close'].min():.1f}-{df5['close'].max():.1f}]  "
              f"low range [{df5['low'].min():.1f}-{df5['low'].max():.1f}]")
        # Collect ALL entries across zones, then sort by time to one position at a time
        all_entries = []   # list of (entry_time, ltf_entry_dict, htf_zone_dict)
        for z in zones:
            zh = z.get("zone_high", 0)
            zl = z.get("zone_low", 0)
            trig = z.get("zone_trigger", 0)
            scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf
            _, ltf_list = scan_fn(df5, zh, zl)
            in_zone = df5[(df5["low"] <= zh) & (df5["close"] >= zl * 0.95)]
            ltf_all    = len(ltf_list)
            ltf_trapped = [e for e in ltf_list if e.get("status") == "TRAPPED"]
            if in_zone.shape[0] > 0 or ltf_all > 0:
                print(f"    zone [{zl:.1f}-{zh:.1f}] trigger={trig:.1f}  "
                      f"bars_in_zone={in_zone.shape[0]}  ltf_all={ltf_all}  ltf_trapped={len(ltf_trapped)}")
            for e in ltf_trapped:
                et = pd.to_datetime(e.get("trapped_on") or e.get("ref_ts"))
                all_entries.append((et, e, z))

        # Sort chronologically so we can enforce "one at a time"
        all_entries.sort(key=lambda x: x[0])
        print(f"  {label}: LTF TRAPPED entries (raw) = {len(all_entries)}")

        bars_list = df5.to_dict("records")
        MAX_SL_BEFORE_T1 = 2   # stop trading this side after 2 failed zones
        open_until = pd.Timestamp("2000-01-01")
        blacklisted_zones = set()
        sl_before_t1_count = 0
        trades_taken = 0
        day_stopped = False
        for et, ltf_e, htf_z in all_entries:
            if day_stopped:
                print(f"    SKIP {et.strftime('%H:%M')} -- day stopped ({sl_before_t1_count} SLs before T1)")
                continue
            zh_key = round(htf_z.get("zone_high", 0), 1)
            if zh_key in blacklisted_zones:
                print(f"    SKIP {et.strftime('%H:%M')} -- zone {zh_key} BLACKLISTED")
                continue
            et_naive = et.tz_localize(None) if et.tzinfo else et
            if et_naive <= open_until:
                print(f"    SKIP {et.strftime('%H:%M')} -- position open until {open_until.strftime('%H:%M')}")
                continue
            result = simulate_trade(ltf_e, htf_z, df5, side, label)
            if result:
                all_results.append(result)
                trades_taken += 1
                failed = result.get("reason") == "SL" and not result.get("t1_done")
                if failed:
                    sl_before_t1_count += 1
                    blacklisted_zones.add(zh_key)
                    print(f"    BLACKLIST zone {zh_key}  (SL#{sl_before_t1_count} before T1)")
                    if sl_before_t1_count >= MAX_SL_BEFORE_T1:
                        day_stopped = True
                        print(f"    DAY STOPPED for {label} -- {sl_before_t1_count} SLs before T1 reached")
                if result["exit_time"] and result["exit_time"] != "None":
                    ou = pd.to_datetime(result["exit_time"])
                    open_until = ou.tz_localize(None) if ou.tzinfo else ou
        print(f"  {label}: trades={trades_taken}  blacklisted={len(blacklisted_zones)}  sl_before_t1={sl_before_t1_count}")

    # ---- Summary --------------------------------------------------------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if not all_results:
        print("No trades fired today.")
    else:
        total_pnl = sum(r["pnl"] for r in all_results)
        for r in all_results:
            print(f"  {r['label']:12s}  entry={r['entry']:.1f}  exit={r['exit'] or 0:.1f}  "
                  f"reason={r['reason']:4s}  P&L=Rs{r['pnl']:,.0f}")
        print(f"\n  TOTAL NET P&L: Rs{total_pnl:,.0f}")
        print(f"  (Qty={QTY}, {LOTS} lots × {LOT_SIZE} lot_size)")


asyncio.run(main())
