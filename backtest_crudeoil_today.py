"""
Backtest TrapScanner on CRUDEOIL for today.

CrudeOil specifics vs SENSEX:
  - HTF source = FUTURES (not option bars)
  - Strikes: ATM +/- 200 (near), +/- 500 (far)
  - Lot size = 100
  - MCX_FO prefix for all keys
  - Entry window: 18:45-19:15 IST
  - EOD: 23:25 IST
  - Keys fetched dynamically from REGISTRY + DB token

Usage:
  python backtest_crudeoil_today.py
"""
import sys, os, asyncio
sys.path.insert(0, os.getcwd())

import aiohttp
import pandas as pd
from datetime import date, timedelta
from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY
from strategies.trap_scanner import scanner

LOT_SIZE   = 100
LOTS       = 2
QTY        = LOT_SIZE * LOTS   # 200
HTF_MIN    = 75
LTF_MIN    = 5
SL_BUF_PCT = 2.0
STEP       = 100   # CrudeOil strike step
GAP_NEAR   = 200
GAP_FAR    = 500


def _round_strike(price, step):
    return int(round(price / step) * step)


async def get_token() -> str:
    db = ClientDB("data/clients.db")
    creds = db.get_feeder_creds_sync("upstox")
    return (creds or {}).get("access_token") or ""


async def fetch_bars(key: str, token: str, days: int = 8) -> list:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    today   = date.today()
    to_date = today + timedelta(days=1)
    fr_date = today - timedelta(days=days)

    hist_url  = (f"https://api.upstox.com/v2/historical-candle/"
                 f"{key}/1minute/{to_date}/{fr_date}")
    intra_url = f"https://api.upstox.com/v2/historical-candle/intraday/{key}/1minute"

    all_bars = []
    seen_dts = set()

    async with aiohttp.ClientSession() as s:
        for url, label in [(hist_url, "hist"), (intra_url, "intraday")]:
            try:
                async with s.get(url, headers=headers,
                                 timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        body = await r.text()
                        print(f"    {label} HTTP {r.status}: {body[:120]}")
                        continue
                    data = await r.json()
            except Exception as e:
                print(f"    {label} error: {e}")
                continue
            candles = data.get("data", {}).get("candles", [])
            for c in reversed(candles):
                dt = c[0]
                if dt not in seen_dts:
                    seen_dts.add(dt)
                    all_bars.append({
                        "datetime": dt, "open": float(c[1]), "high": float(c[2]),
                        "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])
                    })
            print(f"    {key} [{label}]: {len(candles)} candles")

    all_bars.sort(key=lambda b: b["datetime"])
    if all_bars:
        print(f"  {key}: {len(all_bars)} bars  "
              f"({all_bars[0]['datetime'][:10]} to {all_bars[-1]['datetime'][:10]})")
    else:
        print(f"  {key}: 0 bars")
    return all_bars


def to_df(bars):
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def resample(df, minutes):
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def simulate_trade(ltf_entry, htf_zone, df5, side, label):
    """
    Entry + T1 (50% at zone_high) + trap-based trail on 5-min bars.

    Trail logic:
      After T1: SL = CTC (entry price)
      WATCHING  -> (hi >= bears_zone_high) -> SQUEEZED
      SQUEEZED  -> (lo <= bears_zone_trigger) -> PULLED_BACK
      PULLED_BACK -> (hi >= bears_zone_high again) -> CONFIRMED -> trail_sl = bears_zone_trigger
    """
    entry_time  = pd.to_datetime(ltf_entry.get("trapped_on") or ltf_entry.get("ref_ts"))
    if entry_time is None:
        return None
    entry_price = ltf_entry.get("zone_trigger", ltf_entry.get("entry", 0))
    sl_from_zone = ltf_entry.get("zone_low", 0)
    sl_buf       = entry_price * (1 - SL_BUF_PCT / 100)
    sl_price     = max(sl_from_zone, sl_buf) if sl_from_zone > 0 else sl_buf
    t1_target    = htf_zone.get("zone_high", 0)
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

    scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf

    entry_time_naive = entry_time.tz_localize(None) if entry_time.tzinfo else entry_time
    bars_list = df5.to_dict("records")
    future_bars = [b for b in bars_list
                   if (pd.to_datetime(b["datetime"]).tz_localize(None)
                       if pd.to_datetime(b["datetime"]).tzinfo
                       else pd.to_datetime(b["datetime"])) > entry_time_naive]

    print(f"\n  [{label}] ENTRY @ {entry_time.strftime('%H:%M')} price={entry_price:.1f} "
          f"SL={sl_price:.1f} T1={t1_target:.1f} zone=[{zone_low:.1f}-{zone_high:.1f}]")

    trap_states = {}

    for bar in future_bars:
        bt       = pd.to_datetime(bar["datetime"])
        bt_naive = bt.tz_localize(None) if bt.tzinfo else bt
        hi, lo, cl = bar["high"], bar["low"], bar["close"]

        # T1: 50% at zone_high
        if not t1_done and t1_target > 0 and hi >= t1_target:
            t1_pnl = (t1_target - entry_price) * qty_t1
            pnl += t1_pnl
            qty_remaining -= qty_t1
            t1_done = True
            t1_time = bt_naive
            trail_sl = entry_price   # CTC
            print(f"    T1 @ {bt.strftime('%H:%M')} price={t1_target:.1f}  qty={qty_t1}  "
                  f"pnl_so_far={pnl:.0f}  trail_sl=CTC({trail_sl:.1f})")

        # Post-T1 trap-based trail
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
                    if trap_ts_naive not in trap_states:
                        trap_states[trap_ts_naive] = {
                            "state": "WATCHING",
                            "zone_trigger": trap_entry,
                            "zone_high": trap.get("zone_high", 0),
                        }

            for tk, ts in list(trap_states.items()):
                if ts["state"] == "CONFIRMED":
                    continue
                zh, zt = ts["zone_high"], ts["zone_trigger"]
                if ts["state"] == "WATCHING":
                    if hi >= zh:
                        ts["state"] = "SQUEEZED"
                        print(f"    BEARS SQUEEZED @ {bt.strftime('%H:%M')}  "
                              f"bears_entry={zt:.1f}  bears_sl={zh:.1f} HIT")
                elif ts["state"] == "SQUEEZED":
                    if lo <= zt:
                        ts["state"] = "PULLED_BACK"
                        print(f"    PULLBACK to bears_entry={zt:.1f} @ {bt.strftime('%H:%M')}")
                elif ts["state"] == "PULLED_BACK":
                    if hi >= zh:
                        ts["state"] = "CONFIRMED"
                        if zt > trail_sl:
                            print(f"    TRAIL STEP UP @ {bt.strftime('%H:%M')}  "
                                  f"support={zt:.1f} confirmed  "
                                  f"trail_sl: {trail_sl:.1f} -> {zt:.1f}")
                            trail_sl = zt

        # SL check
        active_sl = sl_price if not t1_done else trail_sl
        if lo <= active_sl:
            sl_pnl = (active_sl - entry_price) * qty_remaining
            pnl += sl_pnl
            exit_price  = active_sl
            exit_time   = bt
            exit_reason = "TRAIL_SL" if t1_done else "SL"
            break

        # EOD 23:25 IST
        if bt.hour > 23 or (bt.hour == 23 and bt.minute >= 25):
            eod_pnl = (cl - entry_price) * qty_remaining
            pnl += eod_pnl
            exit_price  = cl
            exit_time   = bt
            exit_reason = "EOD"
            break

    if exit_time is None:
        exit_reason = "OPEN"

    ep = f"{exit_price:.1f}" if exit_price is not None else "-"
    et = exit_time.strftime('%H:%M') if exit_time is not None else "-"
    print(f"    EXIT @ {et}  reason={exit_reason}  exit_px={ep}  NET P&L = Rs{pnl:.0f}")
    return {"label": label, "entry": entry_price, "entry_time": str(entry_time),
            "exit": exit_price, "exit_time": str(exit_time), "reason": exit_reason,
            "pnl": pnl, "t1_done": t1_done}


async def main():
    print("=" * 60)
    print("CRUDEOIL TrapScanner Backtest")
    print(f"Lots={LOTS}  Qty={QTY}  HTF={HTF_MIN}m (FUTURES)  LTF={LTF_MIN}m (OPTIONS)")
    print("=" * 60)

    token = await get_token()
    if not token:
        print("No Upstox token in DB — exiting")
        return

    # Load REGISTRY for CrudeOil
    print("\nLoading REGISTRY for CRUDEOIL...")
    REGISTRY.load_sync("CRUDEOIL", token)
    if not REGISTRY.is_loaded("CRUDEOIL"):
        print("REGISTRY not loaded — check token / master data")
        return

    expiry_date = REGISTRY.get_active_expiry("CRUDEOIL")
    if expiry_date is None:
        print("No active expiry found for CRUDEOIL")
        return
    print(f"Active expiry: {expiry_date}")

    # Futures key
    fut_key = REGISTRY.historical_instrument_key("CRUDEOIL") or ""
    if not fut_key:
        print("No futures key from REGISTRY — cannot proceed")
        return
    print(f"Futures key: {fut_key}")

    # Fetch futures bars to determine ATM
    print("\nFetching futures bars...")
    fut_bars = await fetch_bars(fut_key, token, days=10)
    if not fut_bars:
        print("No futures bars — cannot determine ATM")
        return

    today = date.today().isoformat()
    fut_today = [b for b in fut_bars if b["datetime"][:10] == today]
    if not fut_today:
        print(f"No futures bars for today ({today})")
        return

    today_open = fut_today[0]["open"]
    prev_close = next((b["close"] for b in reversed(fut_bars)
                       if b["datetime"][:10] < today), today_open)
    direction  = "UP" if today_open >= prev_close else "DOWN"
    atm        = _round_strike(today_open, STEP)

    # Strike selection: ATM +/- GAP_NEAR/FAR based on direction
    if direction == "UP":
        ce1_strike = atm - GAP_NEAR   # ITM call
        ce2_strike = atm - GAP_FAR
        pe1_strike = atm + GAP_NEAR   # ITM put
        pe2_strike = atm + GAP_FAR
    else:
        ce1_strike = atm + GAP_NEAR
        ce2_strike = atm + GAP_FAR
        pe1_strike = atm - GAP_NEAR
        pe2_strike = atm - GAP_FAR

    print(f"\nToday open={today_open:.0f}  prev_close={prev_close:.0f}  "
          f"direction={direction}  ATM={atm}")
    print(f"Strikes: CE1={ce1_strike} CE2={ce2_strike} PE1={pe1_strike} PE2={pe2_strike}")

    # Resolve MCX_FO keys from REGISTRY
    def get_key(strike, ot):
        k = REGISTRY.get_upstox_key("CRUDEOIL", expiry_date, strike, ot)
        if k:
            print(f"  {strike}{ot} -> {k}")
        else:
            print(f"  {strike}{ot} -> NOT FOUND in REGISTRY")
        return k or ""

    print("\nResolving option keys from REGISTRY:")
    ce1_key = get_key(ce1_strike, "CE")
    pe1_key = get_key(pe1_strike, "PE")

    if not ce1_key or not pe1_key:
        print("Missing option keys — cannot run backtest")
        return

    # Fetch option bars
    print("\nFetching option bars...")
    ce1_bars = await fetch_bars(ce1_key, token, days=5)
    pe1_bars = await fetch_bars(pe1_key, token, days=5)

    ce1_today = [b for b in ce1_bars if b["datetime"][:10] == today]
    pe1_today = [b for b in pe1_bars if b["datetime"][:10] == today]
    print(f"\nToday bars: FUT={len(fut_today)}  CE1={len(ce1_today)}  PE1={len(pe1_today)}")

    if not ce1_today and not pe1_today:
        print("No option bars for today yet — session may not have started")
        return

    all_results = []

    for label, opt_today, opt_all, side in [
        ("CE1-BEAR", ce1_today, ce1_bars, "CE"),
        ("PE1-BULL", pe1_today, pe1_bars, "PE"),
    ]:
        if not opt_today:
            print(f"  {label}: no bars today")
            continue

        opt_all_df = to_df(opt_all)
        opt_today_df = to_df(opt_today)

        # HTF zones from FUTURES (75-min) — in futures price units (7000-8000)
        # Reason: traders trap in futures, not spot; we need futures zones for context
        fut_all_df = to_df(fut_bars)
        htf_fut = resample(fut_all_df, HTF_MIN)
        print(f"\n---- HTF Scan (75-min FUTURES) for {label} ----")
        print(f"  HTF futures bars: {len(htf_fut)}")
        if len(htf_fut) < 2:
            print("  Not enough HTF bars")
            continue
        _, bear_zones = scanner.scan_htf(htf_fut)
        _, bull_zones = scanner.scan_htf(htf_fut)
        trapped_bear = [z for z in bear_zones if z["status"] == "TRAPPED"]
        trapped_bull = [z for z in bull_zones if z["status"] == "TRAPPED"]

        # Cascade: 15-min FUTURES bars today (same price units as HTF)
        fut_today_df = to_df(fut_today)
        htf15_fut = resample(fut_today_df, 15)
        _, casc_bear = scanner.scan_htf(htf15_fut) if len(htf15_fut) >= 2 else ([], [])
        _, casc_bull = scanner.scan_htf(htf15_fut) if len(htf15_fut) >= 2 else ([], [])
        casc_bear_t = [z for z in casc_bear if z["status"] == "TRAPPED"]
        casc_bull_t = [z for z in casc_bull if z["status"] == "TRAPPED"]

        zones_to_use = (trapped_bear if side == "CE" else trapped_bull)
        casc_zones   = (casc_bear_t  if side == "CE" else casc_bull_t)
        # Union of HTF + cascade (dedup by zone_high)
        all_zones = zones_to_use + [z for z in casc_zones
                                    if not any(abs(z["zone_high"] - h["zone_high"]) < 1
                                               for h in zones_to_use)]
        print(f"  HTF TRAPPED={len(zones_to_use)}  cascade TRAPPED={len(casc_zones)}  combined={len(all_zones)}")
        if not all_zones:
            print(f"  {label}: no zones -> skip")
            continue

        # LTF scan on option bars (5-min today)
        df5 = resample(opt_today_df, LTF_MIN)
        if df5.empty or len(df5) < 3:
            print(f"  {label}: not enough 5-min option bars ({len(df5)})")
            continue
        print(f"  5-min option bars today: {len(df5)}  "
              f"close=[{df5['close'].min():.1f}-{df5['close'].max():.1f}]")

        # Dual confirmation for CrudeOil:
        #   1. Futures must be at/near the zone at the time of the option trap
        #   2. Option (CE/PE) shows its own 5-min trap
        # Both must align → entry fires
        scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf

        # Scan option bars for ALL 5-min traps (no zone filter on option bars)
        _, all_opt_traps = scan_fn(df5, df5["high"].max(), df5["low"].min())
        opt_trapped = [e for e in all_opt_traps if e.get("status") == "TRAPPED"]
        print(f"  Option 5-min traps (all, no zone filter): {len(opt_trapped)}")

        fut_today_1m = to_df(fut_today)  # 1-min futures for spot-check at trap time

        all_entries = []
        for z in all_zones:
            zh = z.get("zone_high", 0)    # futures price
            zl = z.get("zone_low", 0)     # futures price
            trig = z.get("zone_trigger", 0)
            print(f"  Futures zone [{zl:.0f}-{zh:.0f}] trigger={trig:.0f}:")

            for e in opt_trapped:
                trap_time = pd.to_datetime(e.get("trapped_on") or e.get("ref_ts"))
                if trap_time is None:
                    continue
                trap_time_naive = trap_time.tz_localize(None) if trap_time.tzinfo else trap_time

                # Find futures price at/around option trap time
                fut_at_time = fut_today_1m[
                    fut_today_1m["datetime"].apply(
                        lambda x: x.tz_localize(None) if x.tzinfo else x
                    ) <= trap_time_naive
                ]
                if fut_at_time.empty:
                    continue
                fut_close = fut_at_time.iloc[-1]["close"]

                # Dual confirmation: futures price strictly inside the zone
                if zl <= fut_close <= zh:
                    print(f"    CONFIRMED @ {trap_time.strftime('%H:%M')}  "
                          f"opt_entry={e.get('zone_trigger',0):.1f}  "
                          f"fut={fut_close:.0f} in [{zl:.0f}-{zh:.0f}]")
                    # Build a synthetic zone in option units for T1 target:
                    # scan option HTF zones to find the nearest resistance above entry
                    opt_entry = e.get("zone_trigger", 0)
                    htf_opt = resample(opt_all_df, HTF_MIN)
                    _, opt_bear_z = scanner.scan_htf(htf_opt)
                    _, opt_bull_z = scanner.scan_htf(htf_opt)
                    opt_zones_all = opt_bear_z + opt_bull_z
                    # Nearest zone_high above current option entry = T1 target
                    resistances = sorted(
                        [z2.get("zone_high", 0) for z2 in opt_zones_all
                         if z2.get("zone_high", 0) > opt_entry],
                    )
                    t1_opt = resistances[0] if resistances else opt_entry * 1.05
                    # Build a fake zone dict in option units for simulate_trade
                    opt_zone = {
                        "zone_high": t1_opt,
                        "zone_low":  e.get("zone_low", opt_entry * 0.98),
                        "zone_trigger": opt_entry,
                    }
                    all_entries.append((trap_time, e, opt_zone))

        all_entries.sort(key=lambda x: x[0])
        print(f"  {label}: LTF TRAPPED (raw) = {len(all_entries)}")

        MAX_SL_BEFORE_T1 = 2
        open_until        = pd.Timestamp("2000-01-01")
        blacklisted_zones = set()
        sl_before_t1_count = 0
        day_stopped        = False
        trades_taken       = 0

        for et, ltf_e, htf_z in all_entries:
            if day_stopped:
                print(f"    SKIP {et.strftime('%H:%M')} -- day stopped")
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
                        print(f"    DAY STOPPED -- {sl_before_t1_count} SLs before T1")
                if result["exit_time"] and result["exit_time"] != "None":
                    ou = pd.to_datetime(result["exit_time"])
                    open_until = ou.tz_localize(None) if ou.tzinfo else ou
        print(f"  {label}: trades={trades_taken}  blacklisted={len(blacklisted_zones)}  sl_before_t1={sl_before_t1_count}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if not all_results:
        print("No trades fired.")
    else:
        total_pnl = sum(r["pnl"] for r in all_results)
        for r in all_results:
            print(f"  {r['label']:12s}  entry={r['entry']:.1f}  exit={r['exit'] or 0:.1f}  "
                  f"reason={r['reason']:10s}  P&L=Rs{r['pnl']:,.0f}")
        print(f"\n  TOTAL NET P&L: Rs{total_pnl:,.0f}")
        print(f"  (Qty={QTY}, {LOTS} lots x {LOT_SIZE} lot_size)")


asyncio.run(main())
