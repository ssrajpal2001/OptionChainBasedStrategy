"""
CrudeOil 7-Day TrapScanner Backtest
====================================
Mirrors backtest_crudeoil_today.py logic exactly — loops over last 7 trading days.
- Active July contract used for ALL days (no expired contract data).
- Entry window: 14:30–22:45 IST (after NIFTY/SENSEX session).
- HTF = 30-min FUTURES; LTF = 5-min OPTION; dual confirmation required.
- T1 = 50 % at HTF option resistance; trap-ratchet trail after T1.

Usage:
  python backtest_crudeoil_7day.py
"""
import sys, os, asyncio
sys.path.insert(0, os.getcwd())

import aiohttp
import pandas as pd
from datetime import date, timedelta, time as dtime
from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY
from strategies.trap_scanner import scanner

# ── Config ─────────────────────────────────────────────────────────────────
LOT_SIZE   = 100
LOTS       = 2
QTY        = LOT_SIZE * LOTS   # 200
HTF_MIN    = 30                 # CrudeOil uses 30-min HTF (not 75-min)
LTF_MIN    = 5
SL_BUF_PCT = 2.0
STEP       = 100
GAP_NEAR   = 200
GAP_FAR    = 500
DAYS_BACK  = 12                # days of history to fetch

ENTRY_WIN_START = dtime(14, 30)
ENTRY_WIN_END   = dtime(22, 45)
ENTRY_CUTOFF    = dtime(22, 45)
EOD_HOUR_MIN    = (23, 25)     # MCX SQ OFF 23:25

MAX_SL_BEFORE_T1 = 2


def _round_strike(price, step):
    return int(round(price / step) * step)


def get_trading_days(n=7):
    """Return last n weekdays (Mon-Fri), oldest first."""
    days, d = [], date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(days))


async def get_token() -> str:
    db = ClientDB("data/clients.db")
    creds = db.get_feeder_creds_sync("upstox")
    return (creds or {}).get("access_token") or ""


async def fetch_bars(key: str, token: str, days: int = DAYS_BACK) -> list:
    """Fetch 1-min bars: historical endpoint + intraday (for today's MCX session)."""
    headers   = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    today     = date.today()
    to_date   = today + timedelta(days=1)
    fr_date   = today - timedelta(days=days)
    hist_url  = (f"https://api.upstox.com/v2/historical-candle/"
                 f"{key}/1minute/{to_date}/{fr_date}")
    intra_url = f"https://api.upstox.com/v2/historical-candle/intraday/{key}/1minute"

    all_bars, seen_dts = [], set()
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
    Mirrors simulate_trade in backtest_crudeoil_today.py exactly.
    """
    entry_time  = pd.to_datetime(ltf_entry.get("trapped_on") or ltf_entry.get("ref_ts"))
    if entry_time is None:
        return None
    entry_price  = ltf_entry.get("zone_trigger", ltf_entry.get("entry", 0))
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
    bars_list    = df5.to_dict("records")
    future_bars  = [b for b in bars_list
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
            trail_sl = entry_price   # CTC after T1
            print(f"    T1 @ {bt.strftime('%H:%M')} price={t1_target:.1f}  qty={qty_t1}  "
                  f"pnl_so_far={pnl:.0f}  trail_sl=CTC({trail_sl:.1f})")

        # Post-T1 trap-based ratchet trail
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

        # EOD: MCX 23:25 IST
        if bt.hour > EOD_HOUR_MIN[0] or (bt.hour == EOD_HOUR_MIN[0] and
                                          bt.minute >= EOD_HOUR_MIN[1]):
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
    return {"label": label, "entry": entry_price,
            "entry_time": entry_time.strftime('%H:%M'),
            "exit": exit_price,
            "exit_time": exit_time.strftime('%H:%M') if exit_time else "-",
            "reason": exit_reason, "pnl": pnl, "t1_done": t1_done}


async def run_day(day: str, fut_bars: list, ce1_bars: list, pe1_bars: list,
                  ce1_strike: int, pe1_strike: int) -> list:
    """Run the full CE1-BEAR + PE1-BULL scan for a single trading day."""
    all_results = []

    day_fut  = [b for b in fut_bars  if b["datetime"][:10] == day]
    ce1_day  = [b for b in ce1_bars  if b["datetime"][:10] == day]
    pe1_day  = [b for b in pe1_bars  if b["datetime"][:10] == day]

    print(f"  Day bars: FUT={len(day_fut)}  CE1={len(ce1_day)}  PE1={len(pe1_day)}")
    if not ce1_day and not pe1_day:
        print("  No option bars — skip")
        return all_results

    for label, opt_day, opt_all, side, strike in [
        ("CE1-BEAR", ce1_day, ce1_bars, "CE", ce1_strike),
        ("PE1-BULL", pe1_day, pe1_bars, "PE", pe1_strike),
    ]:
        if not opt_day:
            print(f"  {label}: no bars for {day} — skip")
            continue

        opt_all_df   = to_df(opt_all)
        opt_today_df = to_df(opt_day)
        fut_all_df   = to_df(fut_bars)
        fut_today_df = to_df(day_fut)

        # HTF zones from FUTURES (30-min) — covers full history for zone context
        htf_fut = resample(fut_all_df, HTF_MIN)
        print(f"\n---- HTF Scan (30-min FUTURES) for {label} [{day}] ----")
        print(f"  HTF futures bars: {len(htf_fut)}")
        if len(htf_fut) < 2:
            print("  Not enough HTF bars — skip")
            continue

        _, bear_zones = scanner.scan_htf(htf_fut)
        _, bull_zones = scanner.scan_htf(htf_fut)
        trapped_bear = [z for z in bear_zones if z["status"] == "TRAPPED"]
        trapped_bull = [z for z in bull_zones if z["status"] == "TRAPPED"]

        # Cascade: 15-min FUTURES for this specific day
        htf15_fut = resample(fut_today_df, 15)
        if len(htf15_fut) >= 2:
            _, casc_bear = scanner.scan_htf(htf15_fut)
            _, casc_bull = scanner.scan_htf(htf15_fut)
        else:
            casc_bear, casc_bull = [], []
        casc_bear_t = [z for z in casc_bear if z["status"] == "TRAPPED"]
        casc_bull_t = [z for z in casc_bull if z["status"] == "TRAPPED"]

        zones_to_use = trapped_bear if side == "CE" else trapped_bull
        casc_zones   = casc_bear_t  if side == "CE" else casc_bull_t
        all_zones = zones_to_use + [z for z in casc_zones
                                    if not any(abs(z["zone_high"] - h["zone_high"]) < 1
                                               for h in zones_to_use)]
        print(f"  HTF TRAPPED={len(zones_to_use)}  cascade={len(casc_zones)}  combined={len(all_zones)}")
        if not all_zones:
            print(f"  {label}: no zones — skip")
            continue

        # LTF: 5-min option bars for this day only
        df5 = resample(opt_today_df, LTF_MIN)
        if df5.empty or len(df5) < 3:
            print(f"  {label}: not enough 5-min option bars ({len(df5)}) — skip")
            continue
        print(f"  5-min option bars {day}: {len(df5)}  "
              f"close=[{df5['close'].min():.1f}-{df5['close'].max():.1f}]")

        scan_fn = scanner.scan_ltf_bull if side == "PE" else scanner.scan_ltf

        # All option 5-min traps (no zone filter on option side)
        _, all_opt_traps = scan_fn(df5, df5["high"].max(), df5["low"].min())
        opt_trapped = [e for e in all_opt_traps if e.get("status") == "TRAPPED"]
        print(f"  Option 5-min traps (all): {len(opt_trapped)}")

        # Futures 1-min for price-at-time lookup
        fut_1m_df = to_df(day_fut)

        all_entries = []
        for z in all_zones:
            zh, zl = z.get("zone_high", 0), z.get("zone_low", 0)
            trig   = z.get("zone_trigger", 0)
            print(f"  Futures zone [{zl:.0f}-{zh:.0f}] trigger={trig:.0f}:")

            for e in opt_trapped:
                trap_time = pd.to_datetime(e.get("trapped_on") or e.get("ref_ts"))
                if trap_time is None:
                    continue
                tn = trap_time.tz_localize(None) if trap_time.tzinfo else trap_time

                fut_at_time = fut_1m_df[
                    fut_1m_df["datetime"].apply(
                        lambda x: x.tz_localize(None) if x.tzinfo else x
                    ) <= tn
                ]
                if fut_at_time.empty:
                    continue
                fut_close = fut_at_time.iloc[-1]["close"]

                if zl <= fut_close <= zh:
                    print(f"    CONFIRMED @ {trap_time.strftime('%H:%M')}  "
                          f"opt_entry={e.get('zone_trigger',0):.1f}  "
                          f"fut={fut_close:.0f} in [{zl:.0f}-{zh:.0f}]")
                    opt_entry = e.get("zone_trigger", 0)
                    htf_opt = resample(opt_all_df, HTF_MIN)
                    _, opt_bear_z = scanner.scan_htf(htf_opt)
                    _, opt_bull_z = scanner.scan_htf(htf_opt)
                    opt_zones_all = opt_bear_z + opt_bull_z
                    resistances = sorted(
                        [z2.get("zone_high", 0) for z2 in opt_zones_all
                         if z2.get("zone_high", 0) > opt_entry]
                    )
                    t1_opt = resistances[0] if resistances else opt_entry * 1.05
                    opt_zone = {
                        "zone_high": t1_opt,
                        "zone_low":  e.get("zone_low", opt_entry * 0.98),
                        "zone_trigger": opt_entry,
                    }
                    all_entries.append((trap_time, e, opt_zone))

        all_entries.sort(key=lambda x: x[0])
        print(f"  {label}: LTF confirmed = {len(all_entries)}")

        open_until         = pd.Timestamp("2000-01-01")
        blacklisted_zones  = set()
        sl_before_t1_count = 0
        day_stopped        = False
        trades_taken       = 0

        for et, ltf_e, htf_z in all_entries:
            if day_stopped:
                print(f"    SKIP {et.strftime('%H:%M')} -- day stopped")
                continue
            et_time = et.time() if hasattr(et, "time") else et
            if not (ENTRY_WIN_START <= et_time <= ENTRY_WIN_END):
                print(f"    SKIP {et.strftime('%H:%M')} -- outside window")
                continue
            if et_time >= ENTRY_CUTOFF:
                print(f"    SKIP {et.strftime('%H:%M')} -- past cutoff")
                continue
            zh_key = round(htf_z.get("zone_high", 0), 1)
            if zh_key in blacklisted_zones:
                print(f"    SKIP {et.strftime('%H:%M')} -- zone {zh_key} BLACKLISTED")
                continue
            et_naive = et.tz_localize(None) if et.tzinfo else et
            if et_naive <= open_until:
                print(f"    SKIP {et.strftime('%H:%M')} -- position open")
                continue

            result = simulate_trade(ltf_e, htf_z, df5, side, label)
            if result:
                all_results.append({**result, "date": day, "side": label, "strike": strike})
                trades_taken += 1
                failed = result.get("reason") == "SL" and not result.get("t1_done")
                if failed:
                    sl_before_t1_count += 1
                    blacklisted_zones.add(zh_key)
                    print(f"    BLACKLIST zone {zh_key}  (SL#{sl_before_t1_count})")
                    if sl_before_t1_count >= MAX_SL_BEFORE_T1:
                        day_stopped = True
                        print(f"    DAY STOPPED — {sl_before_t1_count} SLs before T1")
                if result["exit_time"] != "-":
                    open_until = pd.Timestamp(f"{day} {result['exit_time']}")

        print(f"  {label}: trades={trades_taken}  SL_before_T1={sl_before_t1_count}")

    return all_results


async def main():
    trading_days = get_trading_days(7)

    print("=" * 70)
    print("CRUDEOIL TrapScanner — 7-Day Backtest (Active Contract)")
    print(f"Lots={LOTS}  Qty={QTY}  HTF={HTF_MIN}m (FUT)  LTF={LTF_MIN}m (OPT)")
    print(f"Entry window: {ENTRY_WIN_START.strftime('%H:%M')}–{ENTRY_WIN_END.strftime('%H:%M')} IST")
    print(f"Trading days: {trading_days}")
    print("=" * 70)

    token = await get_token()
    if not token:
        print("No Upstox token — exiting"); return

    print("\nLoading REGISTRY for CRUDEOIL...")
    REGISTRY.load_sync("CRUDEOIL", token)
    if not REGISTRY.is_loaded("CRUDEOIL"):
        print("REGISTRY not loaded"); return

    expiry_date = REGISTRY.get_active_expiry("CRUDEOIL")
    if not expiry_date:
        print("No active expiry"); return
    print(f"Active expiry: {expiry_date}")

    fut_key = REGISTRY.historical_instrument_key("CRUDEOIL") or ""
    if not fut_key:
        print("No futures key"); return
    print(f"Futures key: {fut_key}")

    # Fetch ALL futures bars once (covers all 7 days)
    print("\nFetching futures bars...")
    fut_bars = await fetch_bars(fut_key, token, days=DAYS_BACK)
    if not fut_bars:
        print("No futures bars"); return

    all_summary = []

    for day in trading_days:
        print("\n" + "=" * 70)
        print(f"DATE: {day}")
        print("=" * 70)

        day_fut = [b for b in fut_bars if b["datetime"][:10] == day]
        if not day_fut:
            print(f"  No futures bars for {day} — skip")
            continue

        # Gap direction: PREV DAY CLOSE vs TODAY OPENING (first bar, NOT live price)
        prev_bars  = [b for b in fut_bars if b["datetime"][:10] < day]
        today_open = day_fut[0]["open"]       # first bar of session = true market open
        prev_close = prev_bars[-1]["close"] if prev_bars else today_open
        gap_pct    = abs(today_open - prev_close) / prev_close * 100 if prev_close else 0.0
        direction  = "DOWN" if today_open < prev_close else "UP"
        atm        = _round_strike(today_open, STEP)

        if direction == "DOWN":
            ce1_strike = atm + GAP_NEAR
            pe1_strike = atm - GAP_NEAR
        else:
            ce1_strike = atm - GAP_NEAR
            pe1_strike = atm + GAP_NEAR

        print(f"  FUT open={today_open:.0f}  prev_close={prev_close:.0f}  "
              f"gap={gap_pct:.1f}% {direction}  ATM={atm}")
        print(f"  Strikes: CE1={ce1_strike}  PE1={pe1_strike}")

        ce1_key = REGISTRY.get_upstox_key("CRUDEOIL", expiry_date, ce1_strike, "CE") or ""
        pe1_key = REGISTRY.get_upstox_key("CRUDEOIL", expiry_date, pe1_strike, "PE") or ""
        if not ce1_key or not pe1_key:
            print(f"  Keys not in REGISTRY (ce1={ce1_key!r} pe1={pe1_key!r}) — skip")
            continue
        print(f"  CE1 key: {ce1_key}")
        print(f"  PE1 key: {pe1_key}")

        print(f"\nFetching option bars for {day}...")
        ce1_bars = await fetch_bars(ce1_key, token, days=DAYS_BACK)
        pe1_bars = await fetch_bars(pe1_key, token, days=DAYS_BACK)

        day_results = await run_day(day, fut_bars, ce1_bars, pe1_bars,
                                    ce1_strike, pe1_strike)
        all_summary.extend(day_results)

        day_pnl = sum(r["pnl"] for r in day_results)
        print(f"\n  >>> {day} total: {len(day_results)} trades  P&L = Rs{day_pnl:,.0f}")

    # ── Final summary ────────────────────────────────────────────────────────
    print("\n\n" + "=" * 90)
    print("7-DAY SUMMARY TABLE")
    print("=" * 90)
    hdr = (f"{'Date':<12} {'Side':<12} {'Strike':<8} "
           f"{'Entry':>7} {'@':>5} {'Exit':>7} {'@':>5} "
           f"{'Reason':<12} {'T1':>4} {'P&L':>10}")
    print(hdr)
    print("-" * 90)

    wins = losses = 0
    day_pnls: dict = {}
    for r in all_summary:
        ep  = f"{r['exit']:.1f}" if r["exit"] is not None else "-"
        pnl = r["pnl"]
        print(f"{r['date']:<12} {r['side']:<12} {r['strike']:<8} "
              f"{r['entry']:>7.1f} {r['entry_time']:>5} "
              f"{ep:>7} {r['exit_time']:>5} "
              f"{r['reason']:<12} {'Y' if r['t1_done'] else 'N':>4} "
              f"Rs{pnl:>8,.0f}")
        day_pnls[r["date"]] = day_pnls.get(r["date"], 0) + pnl
        if pnl >= 0:
            wins += 1
        else:
            losses += 1

    print("-" * 90)
    print("\nPer-day P&L:")
    grand_total = 0.0
    for d in sorted(day_pnls):
        pnl = day_pnls[d]
        grand_total += pnl
        tag = "+" if pnl >= 0 else "-"
        print(f"  {d}  {tag}  Rs{pnl:,.0f}")

    n = len(all_summary)
    print(f"\nTotal trades : {n}")
    if n:
        print(f"Win rate     : {wins}/{n} = {wins/n*100:.0f}%")
    print(f"TOTAL NET P&L: Rs{grand_total:,.0f}  (Qty={QTY}, {LOTS}x{LOT_SIZE})")
    print("=" * 90)


asyncio.run(main())
