"""
CORRECT Backtest — 3-layer architecture:

NIFTY/SENSEX:
  1. SPOT 75-min → bear/bull trap (direction only)
  2. If spot zone far → cascade SPOT 15-min intraday
  3. OPTION HTF 75-min → zones in option premium space for that side
  4. If no option HTF → cascade OPTION 15-min
  5. OPTION 5-min → LTF entry (actual entry + SL + T1)

CrudeOil:
  1. FUTURES 75-min → trap detection
  2. If far → cascade FUTURES 15-min
  3. FUTURES 5-min → LTF entry (FUTURES is both detector & trading vehicle)
"""
import sys, os, requests, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from strategies.trap_scanner import scanner
from strategies.trap_scanner_engine import _bars_to_df, _resample_htf

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFFNDciLCJqdGkiOiI2YTMzNjU1N2EwZTg2ODU4Y2ZkZmU0N2MiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNzUzMTc1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE4MjAwMDB9.DL0Vhwm0P2yGxKAn5HLGWkqIJvgxwp857Q4S_RnXF2E"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}


def fetch(key, from_dt, to_dt):
    k = key.replace("|", "%7C")
    r = requests.get(
        f"https://api.upstox.com/v2/historical-candle/{k}/1minute/{to_dt}/{from_dt}",
        headers=H, timeout=20
    )
    d = r.json()
    if d.get("status") != "success":
        return pd.DataFrame()
    df = pd.DataFrame(d["data"]["candles"], columns=["ts","o","h","l","c","v","oi"])
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    return df.sort_values("ts").reset_index(drop=True)


def mkt(df, start="09:15", end="15:30"):
    return df[(df["ts"].dt.time >= pd.Timestamp(start).time()) &
              (df["ts"].dt.time <= pd.Timestamp(end).time())]


def to_bars(df):
    return [{"datetime": str(r["ts"]), "open": r["o"], "high": r["h"],
             "low": r["l"], "close": r["c"], "volume": r["v"]} for _, r in df.iterrows()]


def regime_ok(z, ltp):
    return ltp <= 0 or (z["zone_high"] > ltp * 0.3 and z["zone_low"] < ltp * 3.0)


def get_ts(e):
    ts = e.get("closed_on") or e.get("trapped_on")
    if ts is None:
        return None
    t = pd.Timestamp(str(ts))
    return t.tz_localize(None) if t.tzinfo else t


def _zone_ref(z):
    """Reference price for distance check: zone_high for BEAR (price to recover above),
    zone_low for BULL (price to fall below)."""
    if z.get("kind") == "BULL":
        return z.get("zone_low", 0)
    return z.get("zone_high", 0)


def find_direction(ctx_df, day_df, atr_mult=1.5):
    """
    Scan spot/futures for bear/bull trapped zones using scan_htf_spot (BEAR+BULL kinds).
    Returns (bear_zones, bull_zones, mode, atr)
    Falls back to cascade 15-min intraday if no near HTF 75-min zones.
    """
    if ctx_df.empty:
        return [], [], "NO-DATA", 0

    open_ltp = day_df["c"].iloc[0] if not day_df.empty else 0

    htf75 = _resample_htf(_bars_to_df(to_bars(ctx_df)), 75)
    _, ents = scanner.scan_htf_spot(htf75)
    trapped = [e for e in ents if e["status"] == "TRAPPED"]

    atr = round((htf75["high"] - htf75["low"]).abs().mean(), 2) if len(htf75) > 1 else 0
    thr = atr_mult * atr

    near_bear = [z for z in trapped if z.get("kind") == "BEAR"
                 and abs(open_ltp - _zone_ref(z)) <= thr]
    near_bull = [z for z in trapped if z.get("kind") == "BULL"
                 and abs(open_ltp - _zone_ref(z)) <= thr]

    if near_bear or near_bull:
        return near_bear, near_bull, "HTF-75m", atr

    # Cascade: today's 15-min intraday zones
    if day_df.empty:
        return [], [], "NO-ZONE", atr
    casc15 = _resample_htf(_bars_to_df(to_bars(day_df)), 15)
    _, c_ents = scanner.scan_htf_spot(casc15)
    c_bear = [e for e in c_ents if e["status"] == "TRAPPED" and e.get("kind") == "BEAR"]
    c_bull = [e for e in c_ents if e["status"] == "TRAPPED" and e.get("kind") == "BULL"]
    return c_bear, c_bull, "CASCADE-15m", atr


def find_opt_entry(opt_ctx, opt_day_df, sl_buf=2.0):
    """
    Given option bars, find best HTF zone + LTF entry in OPTION premium space.
    Returns (entry_dict, zone_dict, ets, mode) or (None, None, None, mode)
    """
    if opt_ctx.empty or opt_day_df.empty:
        return None, None, None, "NO-DATA"

    open_ltp = opt_day_df["c"].iloc[0]

    # Option HTF 75-min
    htf75 = _resample_htf(_bars_to_df(to_bars(opt_ctx)), 75)
    _, ents = scanner.scan_htf(htf75)
    trapped = [e for e in ents if e["status"] == "TRAPPED"]
    trapped = [z for z in trapped if regime_ok(z, open_ltp)]

    atr = round((htf75["high"] - htf75["low"]).abs().mean(), 2) if len(htf75) > 1 else 0
    near = [z for z in trapped if abs(open_ltp - z.get("zone_trigger", open_ltp)) <= 1.5 * atr]

    # Option cascade 15-min if no near HTF
    if not near:
        casc15 = _resample_htf(_bars_to_df(to_bars(opt_day_df)), 15)
        _, c_ents = scanner.scan_htf(casc15)
        zones = [e for e in c_ents if e["status"] == "TRAPPED"]
        mode = "OPT-CASCADE"
    else:
        zones = near
        mode = "OPT-HTF"

    df5 = _resample_htf(_bars_to_df(to_bars(opt_day_df)), 5)

    def _scan_zones(zone_list):
        for z in zone_list:
            _, ltf = scanner.scan_ltf(df5, z["zone_high"], z["zone_low"])
            closed = [x for x in ltf if x["status"] == "CLOSED"]
            b = scanner.select_best_ltf_entry(closed)
            if not b:
                trapped_ltf = [x for x in ltf if x["status"] == "TRAPPED"]
                b = min(trapped_ltf, key=lambda e: e["zone_low"]) if trapped_ltf else None
            if not b:
                continue
            ts = get_ts(b)
            if ts is None:
                continue
            return b, z, ts
        return None, None, None

    # Try HTF near zones first
    if near:
        b, z, ts = _scan_zones(near)
        if b:
            return b, z, ts, "OPT-HTF"
        mode = "OPT-HTF+CASC"  # HTF found zones but price never entered; try cascade

    # Cascade fallback (always tried when HTF gives no entry)
    casc15 = _resample_htf(_bars_to_df(to_bars(opt_day_df)), 15)
    _, c_ents = scanner.scan_htf(casc15)
    casc = [e for e in c_ents if e["status"] == "TRAPPED"]
    b, z, ts = _scan_zones(casc)
    if b:
        return b, z, ts, mode if near else "OPT-CASCADE"

    return None, None, None, mode if near else "OPT-CASCADE"


def simulate_2lots(df_1m, ep, sl, t1, ets, sl_buf=2.0, eod="15:25"):
    """
    2-lot simulation:
    Lot 1: exits at T1 (partial profit booking)
    Lot 2: trails from T1 until SL hit or EOD
    Returns list of (lot, exit_price, reason, ts, pnl)
    eod: time string for force-exit e.g. "15:25" or "23:25" (CrudeOil)
    """
    tsl = sl
    t1_hit = False
    lot1_done = False
    results = []
    eod_time = pd.Timestamp(eod).time()

    for _, bar in df_1m[df_1m["ts"] > ets].iterrows():
        # T1 hit — book Lot 1
        if not t1_hit and bar["h"] >= t1:
            t1_hit = True
            tsl = ep  # trail starts at entry (break-even for Lot 2)
            if not lot1_done:
                results.append((1, t1, "T1", bar["ts"], round(t1 - ep, 1)))
                lot1_done = True

        # Plain SL (before T1)
        if not t1_hit and bar["l"] <= sl:
            if not lot1_done:
                results.append((1, sl, "SL", bar["ts"], round(sl - ep, 1)))
                lot1_done = True
            results.append((2, sl, "SL", bar["ts"], round(sl - ep, 1)))
            return results

        # Trail Lot 2 after T1
        if t1_hit:
            new = bar["l"] - sl_buf
            if new > tsl:
                tsl = new
            if bar["l"] <= tsl:
                results.append((2, round(tsl, 2), "TSL", bar["ts"], round(tsl - ep, 1)))
                return results

        if bar["ts"].time() >= eod_time:
            cp = round(bar["c"], 2)
            if not lot1_done:
                results.append((1, cp, "EOD", bar["ts"], round(cp - ep, 1)))
            results.append((2, cp, "EOD", bar["ts"], round(cp - ep, 1)))
            return results

    last = df_1m.iloc[-1]
    cp = round(last["c"], 2)
    if not lot1_done:
        results.append((1, cp, "EOD", last["ts"], round(cp - ep, 1)))
    results.append((2, cp, "EOD", last["ts"], round(cp - ep, 1)))
    return results


def run_sensex_backtest(days):
    print("\n" + "="*80)
    print("  SENSEX BACKTEST — Spot direction + Option entry")
    print("  Logic: SPOT 75m → bear/bull → OPTION zones → OPTION LTF entry")
    print("="*80)

    from datetime import timedelta
    first_day = pd.Timestamp(days[0]).date()
    last_day  = pd.Timestamp(days[-1]).date()
    from_dt   = str(first_day - timedelta(days=10))
    to_dt     = str(last_day)

    print("Fetching SENSEX SPOT (BSE_INDEX|SENSEX)...")
    spot_all = mkt(fetch("BSE_INDEX|SENSEX", from_dt, to_dt))
    print(f"  Spot bars: {len(spot_all)}")

    print("Fetching CE1 (BSE_FO|1137766)...")
    ce_all = mkt(fetch("BSE_FO|1137766", from_dt, to_dt))
    print(f"  CE1 bars: {len(ce_all)}")

    print("Fetching PE1 (BSE_FO|1147016)...")
    pe_all = mkt(fetch("BSE_FO|1147016", from_dt, to_dt))
    print(f"  PE1 bars: {len(pe_all)}")

    HDR = f"{'Date':<12}{'Side':<5}{'SpotMode':<14}{'OptMode':<13}{'LTF':<9}{'OptZone':<14}{'In':<6}{'Entry':<8}{'SL':<7}{'T1':<7}{'Out':<6}{'Exit':<8}{'PnL':<7}Res"
    print("\n" + HDR)
    print("-" * len(HDR))

    total = 0
    all_trades = []

    for dt in days:
        bt = pd.Timestamp(dt).date()

        spot_ctx = spot_all[spot_all["ts"].dt.date <= bt]
        spot_day = spot_all[spot_all["ts"].dt.date == bt].copy()
        if spot_day.empty:
            print(f"{dt}  SENSEX spot data missing")
            continue

        spot_ltp = spot_day["c"].iloc[0]
        bear_z, bull_z, spot_mode, spot_atr = find_direction(spot_ctx, spot_day)

        ce_signal = bool(bear_z)
        pe_signal = bool(bull_z)

        print(f"\n{dt}  spot_open={spot_ltp:.0f}  spot_atr={spot_atr:.0f}  "
              f"spot_mode={spot_mode}  bear={len(bear_z)}zones  bull={len(bull_z)}zones  "
              f"-> CE={'YES' if ce_signal else 'NO'} PE={'YES' if pe_signal else 'NO'}")

        for side, signal, df_opt_all in [("CE", ce_signal, ce_all), ("PE", pe_signal, pe_all)]:
            if not signal:
                print(f"  {side:<5}No {side} signal from spot")
                continue

            opt_ctx = df_opt_all[df_opt_all["ts"].dt.date <= bt]
            opt_day = df_opt_all[df_opt_all["ts"].dt.date == bt].copy()

            if opt_day.empty:
                print(f"  {side:<5}No option data for {dt}")
                continue

            best, zone, ets, opt_mode = find_opt_entry(opt_ctx, opt_day)

            if not best:
                print(f"  {side:<5}{spot_mode:<14}{opt_mode:<13}No option LTF entry  opt_open={opt_day['c'].iloc[0]:.0f}")
                continue

            ep  = round(best["entry"], 2)
            sl  = round(best["zone_low"] - 2.0, 2)  # SL below LTF option zone_low
            t1  = round(zone["zone_high"], 2)        # T1 = option HTF/cascade zone_high

            if sl >= ep:
                print(f"  {side:<5}SKIP — inverted SL (ep={ep} sl={sl})")
                continue

            trades_2lot = simulate_2lots(opt_day, ep, sl, t1, ets)

            oz  = f"{zone['zone_low']:.0f}->{zone['zone_high']:.0f}"
            lot_pnl = 0
            for lot, xp, xr, xt, pnl in trades_2lot:
                total += pnl
                lot_pnl += pnl
                all_trades.append({"pnl": pnl, "xr": xr})
                res = ("WIN" if pnl > 0 else "LOSS") + "/" + xr
                print(f"  {side:<5}{spot_mode:<14}{opt_mode:<13}{best['status']:<9}{oz:<14}"
                      f"{ets.strftime('%H:%M'):<6}{ep:<8}{sl:<7}{t1:<7}"
                      f"{xt.strftime('%H:%M'):<6}{xp:<8}L{lot}:{pnl:<5}{res}")

    wins = sum(1 for t in all_trades if t["pnl"] > 0)
    print(f"\nSENSEX: Trades={len(all_trades)}  Wins={wins}  Losses={len(all_trades)-wins}  Net={total:.1f} pts")
    print(f"Rupees (SENSEX lot=10): Rs {total*10:.0f}")
    return total


def run_crudeoil_backtest(days, crude_key):
    print("\n" + "="*80)
    print("  CRUDEOIL BACKTEST — Futures chart (detect + trade)")
    print("  Logic: FUTURES 75m → trap → FUTURES 5m LTF entry (futures is vehicle)")
    print("="*80)

    print(f"Fetching CrudeOil Futures ({crude_key})...")
    # MCX trades 09:00-23:30 IST
    # Fetch 10 calendar days before first backtest day for HTF context
    from datetime import datetime, timedelta
    first_day = pd.Timestamp(days[0]).date()
    last_day  = pd.Timestamp(days[-1]).date()
    from_dt   = str(first_day - timedelta(days=10))
    to_dt     = str(last_day)
    crude_all_raw = fetch(crude_key, from_dt, to_dt)
    if crude_all_raw.empty:
        print(f"  FAILED — key {crude_key} returned no data")
        return 0

    crude_all = crude_all_raw[(crude_all_raw["ts"].dt.time >= pd.Timestamp("09:00").time()) &
                              (crude_all_raw["ts"].dt.time <= pd.Timestamp("23:30").time())]
    print(f"  CrudeOil bars: {len(crude_all)}  "
          f"Range: {crude_all['c'].min():.0f}-{crude_all['c'].max():.0f}")

    HDR = f"{'Date':<12}{'Mode':<14}{'LTF':<9}{'Zone':<14}{'In':<6}{'Entry':<8}{'SL':<8}{'T1':<8}{'Out':<6}{'Exit':<8}{'PnL':<7}Res"
    print("\n" + HDR)
    print("-" * len(HDR))

    total = 0
    all_trades = []

    # CrudeOil runs two sessions per day — day (09:00-16:59) and evening (17:00-23:30)
    # Zones from each session are used ONLY for LTF entries within that session
    SESSIONS = [
        ("DAY",     "09:00", "16:59", "16:55"),
        ("EVENING", "17:00", "23:30", "23:25"),
    ]

    for dt in days:
        bt = pd.Timestamp(dt).date()
        day_all = crude_all[crude_all["ts"].dt.date == bt].copy()
        if day_all.empty:
            print(f"{dt}  NO DATA")
            continue

        for sess_name, sess_start, sess_end, sess_eod in SESSIONS:
            sess_day = day_all[
                (day_all["ts"].dt.time >= pd.Timestamp(sess_start).time()) &
                (day_all["ts"].dt.time <= pd.Timestamp(sess_end).time())
            ].copy()
            if sess_day.empty:
                continue

            # Historical context = all bars before this date + today's session bars
            ctx_hist = crude_all[crude_all["ts"].dt.date < bt]
            sess_ctx = pd.concat([ctx_hist, sess_day]).reset_index(drop=True)

            open_ltp = sess_day["c"].iloc[0]
            bear_z, bull_z, mode, atr = find_direction(sess_ctx, sess_day)
            all_zones = bear_z + bull_z

            if not all_zones:
                continue  # no zones in this session, skip quietly

            df5 = _resample_htf(_bars_to_df(to_bars(sess_day)), 5)
            best = zone = ets = None
            for z in all_zones:
                _, ltf = scanner.scan_ltf(df5, z["zone_high"], z["zone_low"])
                closed = [x for x in ltf if x["status"] == "CLOSED"]
                b = scanner.select_best_ltf_entry(closed)
                if not b:
                    trapped_ltf = [x for x in ltf if x["status"] == "TRAPPED"]
                    b = min(trapped_ltf, key=lambda e: e["zone_low"]) if trapped_ltf else None
                if not b:
                    continue
                ts = get_ts(b)
                if ts is None:
                    continue
                best, zone, ets = b, z, ts
                break

            if not best:
                continue

            ep  = round(best["entry"], 2)
            zone_width = zone["zone_high"] - zone["zone_low"]
            sl  = round(ep - zone_width - 5.0, 2)   # SL = entry - zone_width - buffer (always below)
            t1  = round(ep + 2.0 * zone_width, 2)   # T1 = 2R above entry

            # Inverted SL guard
            if sl >= ep:
                print(f"{dt}/{sess_name}  SKIP — inverted SL (ep={ep} sl={sl})")
                continue

            trades_2lot = simulate_2lots(sess_day, ep, sl, t1, ets, sl_buf=5.0, eod=sess_eod)
            oz  = f"{zone['zone_low']:.0f}->{zone['zone_high']:.0f}"
            for lot_n, xp2, xr2, xt2, pnl2 in trades_2lot:
                total += pnl2
                all_trades.append({"pnl": pnl2, "xr": xr2})
                res = ("WIN" if pnl2 > 0 else "LOSS") + "/" + xr2
                print(f"{dt}/{sess_name:<8}{mode:<14}{best['status']:<9}{oz:<14}"
                      f"{ets.strftime('%H:%M'):<6}{ep:<8}{sl:<8}{t1:<8}"
                      f"{xt2.strftime('%H:%M'):<6}{xp2:<8}L{lot_n}:{pnl2:<5}{res}")

    wins = sum(1 for t in all_trades if t["pnl"] > 0)
    print(f"\nCRUDEOIL: Trades={len(all_trades)}  Wins={wins}  Losses={len(all_trades)-wins}  Net={total:.1f} pts")
    print(f"Rupees (lot=100): Rs {total*100:.0f}")
    return total


# ── MAIN ──────────────────────────────────────────────────────────────────────
DAYS = ["2026-06-15", "2026-06-16", "2026-06-17"]

s_total = run_sensex_backtest(DAYS)

# CrudeOil — try multiple possible keys for Jul 2026 futures
# NSE_COM|149475 = CRUDEOIL26JULFUT, 100 barrel lot, Jul 2026 expiry
CRUDE_KEY = "NSE_COM|149475"
c_total = run_crudeoil_backtest(DAYS, CRUDE_KEY)

print(f"\n{'='*60}")
print(f"SENSEX net: {s_total:.1f} pts  (lot=10)  Rs {s_total*10:.0f}")
print(f"CRUDEOIL net: {c_total:.1f} pts  (lot=100)  Rs {c_total*100:.0f}")
