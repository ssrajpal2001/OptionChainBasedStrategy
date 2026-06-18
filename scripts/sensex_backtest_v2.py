"""
SENSEX + CrudeOil Trap Scanner Backtest — with all 3 fixes:
1. Regime filter (zone_high > ltp*0.3)
2. TRAPPED accepted on LTF (not just CLOSED)
3. Cascade fallback even when HTF near zones exist
4. Liquidity sweep re-entry after plain SL hit
"""
import sys, os, requests, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from strategies.trap_scanner import scanner
from strategies.trap_scanner_engine import _bars_to_df, _resample_htf

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI3NkFFNDciLCJqdGkiOiI2YTMzNjU1N2EwZTg2ODU4Y2ZkZmU0N2MiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNzUzMTc1LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE4MjAwMDB9.DL0Vhwm0P2yGxKAn5HLGWkqIJvgxwp857Q4S_RnXF2E"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
SL_BUF = 2.0

def fetch(key, f, t):
    k = key.replace("|", "%7C")
    r = requests.get(f"https://api.upstox.com/v2/historical-candle/{k}/1minute/{t}/{f}", headers=H)
    d = r.json()
    if d.get("status") != "success":
        return pd.DataFrame()
    df = pd.DataFrame(d["data"]["candles"], columns=["ts","o","h","l","c","v","oi"])
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_localize(None)
    return df.sort_values("ts").reset_index(drop=True)

def mkt(df):
    return df[(df["ts"].dt.time >= pd.Timestamp("09:15").time()) &
              (df["ts"].dt.time <= pd.Timestamp("15:30").time())]

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

def simulate_with_sweep(df_1m, ep, sl, t1, ets):
    """Simulate trade with liquidity sweep re-entry. Returns list of trade dicts."""
    tsl = sl
    t1_hit = False
    sweep_watch = None
    results = []
    current = {"ep": ep, "sl": sl, "t1": t1, "ets": ets, "sweep": False}

    for _, bar in df_1m[df_1m["ts"] > ets].iterrows():
        # Sweep re-entry check (no open position)
        if current is None and sweep_watch:
            if bar["h"] > sweep_watch["sl"]:
                re_ep = bar["c"]
                re_sl = sweep_watch["sl"] - SL_BUF
                re_t1 = sweep_watch["t1"]
                sweep_watch = None
                current = {"ep": re_ep, "sl": re_sl, "t1": re_t1, "ets": bar["ts"], "sweep": True}
                tsl = re_sl; t1_hit = False
                continue
            else:
                sweep_watch["candles"] -= 1
                if sweep_watch["candles"] <= 0:
                    sweep_watch = None
            continue

        if current is None:
            break

        # T1
        if not t1_hit and bar["h"] >= t1:
            t1_hit = True
            tsl = current["ep"]

        # Trail
        if t1_hit:
            new_tsl = bar["l"] - SL_BUF
            if new_tsl > tsl:
                tsl = new_tsl

        # Exit
        if bar["l"] <= tsl:
            xp = round(tsl, 2)
            xr = "TSL" if t1_hit else "SL"
            current.update({"xp": xp, "xr": xr, "xt": bar["ts"], "pnl": round(xp - current["ep"], 1)})
            results.append(current)
            # Sweep watch only on plain SL (not trail), and only once (no chain of sweeps)
            if not t1_hit and not current["sweep"]:
                sweep_watch = {"sl": tsl, "t1": t1, "candles": 2}
            current = None
            tsl = sl; t1_hit = False
            continue

        if bar["ts"].time() >= pd.Timestamp("15:25").time():
            xp = round(bar["c"], 2)
            current.update({"xp": xp, "xr": "EOD", "xt": bar["ts"], "pnl": round(xp - current["ep"], 1)})
            results.append(current)
            current = None
            break

    if current and "xp" not in current:
        last = df_1m.iloc[-1]
        current.update({"xp": round(last["c"], 2), "xr": "EOD", "xt": last["ts"],
                        "pnl": round(last["c"] - current["ep"], 1)})
        results.append(current)

    return results

def best_entry(df5, zones):
    for z in zones:
        _, ltf = scanner.scan_ltf(df5, z["zone_high"], z["zone_low"])
        closed = [x for x in ltf if x["status"] == "CLOSED"]
        best = scanner.select_best_ltf_entry(closed)
        if not best:
            trapped = [x for x in ltf if x["status"] == "TRAPPED"]
            best = min(trapped, key=lambda e: e["zone_low"]) if trapped else None
        if not best:
            continue
        ets = get_ts(best)
        if ets is None:
            continue
        return best, z, ets
    return None, None, None


def run_backtest(name, legs, days, sl_buf=2.0):
    global SL_BUF
    SL_BUF = sl_buf
    total = 0
    all_trades = []

    print(f"\n{'='*70}")
    print(f"  {name} BACKTEST")
    print(f"{'='*70}")
    print(f"{'Date':<12}{'Leg':<6}{'Mode':<15}{'LTF':<8}{'Zone':<14}{'In':<6}{'Entry':<7}{'SL':<7}{'T1':<7}{'Out':<6}{'Exit':<7}{'PnL':<7}{'Sw':<4}Res")
    print("-" * 115)

    for dt in days:
        bt = pd.Timestamp(dt).date()
        for label, df_all, ctx_all in legs:
            df_ctx = ctx_all[ctx_all["ts"].dt.date <= bt] if ctx_all is not None else df_all[df_all["ts"].dt.date <= bt]
            df_day = df_all[df_all["ts"].dt.date == bt].copy()
            if df_day.empty:
                print(f"{dt}  {label:<6}NO DATA")
                continue

            open_ltp = df_day["c"].iloc[0]

            # 75-min HTF zones with regime filter
            htf75 = _resample_htf(_bars_to_df(to_bars(df_ctx)), 75)
            _, ents = scanner.scan_htf(htf75)
            raw75 = [e for e in ents if e["status"] == "TRAPPED"]
            fil75 = [z for z in raw75 if regime_ok(z, open_ltp)]
            htf_atr = round((htf75["high"] - htf75["low"]).abs().mean(), 2) if len(htf75) > 1 else 0
            near75 = [z for z in fil75 if abs(open_ltp - z["zone_trigger"]) <= 1.5 * htf_atr]

            # 15-min cascade zones (today only)
            df_today = _bars_to_df(to_bars(df_day))
            htf15 = _resample_htf(df_today, 15)
            _, c_ents = scanner.scan_htf(htf15)
            casc15 = [e for e in c_ents if e["status"] == "TRAPPED"]
            df5 = _resample_htf(df_today, 5)

            # Try HTF first, then cascade as fallback
            best, zone, ets = best_entry(df5, near75)
            mode = "HTF-75m" if best else ""
            if not best:
                best, zone, ets = best_entry(df5, casc15)
                mode = "CASCADE" if not near75 else "HTF+CASCADE"

            if not best:
                m = "CASCADE" if not near75 else "HTF+CASCADE"
                print(f"{dt}  {label:<6}{m:<15}{'':8}NO ENTRY  open={open_ltp:.0f}  75near={len(near75)}  casc={len(casc15)}")
                continue

            ep = round(best["entry"], 2)
            sl = round(best["zone_low"] - sl_buf, 2)
            t1 = round(zone["zone_high"], 2)
            trades = simulate_with_sweep(df_day, ep, sl, t1, ets)

            for tr in trades:
                total += tr["pnl"]
                all_trades.append(tr)
                sw_tag = "Y" if tr.get("sweep") else ""
                res = ("WIN" if tr["pnl"] > 0 else "LOSS") + "/" + tr["xr"]
                ltf_s = best["status"] if not tr.get("sweep") else "SWEEP"
                zstr = f"{zone['zone_low']:.0f}->{zone['zone_high']:.0f}"
                print(f"{dt}  {label:<6}{mode:<15}{ltf_s:<8}{zstr:<14}{tr['ets'].strftime('%H:%M'):<6}{tr['ep']:<7}{tr['sl']:<7}{tr['t1']:<7}{tr['xt'].strftime('%H:%M'):<6}{tr['xp']:<7}{tr['pnl']:<7}{sw_tag:<4}{res}")

    wins = sum(1 for t in all_trades if t["pnl"] > 0)
    print(f"\n{name}: Trades={len(all_trades)}  Wins={wins}  Losses={len(all_trades)-wins}  Net={total:.1f} pts")
    return total


# ── SENSEX ─────────────────────────────────────────────────────────────────────
print("Fetching SENSEX CE1 + PE1...")
ce_ctx = mkt(fetch("BSE_FO|1137766", "2026-06-10", "2026-06-17"))
pe_ctx = mkt(fetch("BSE_FO|1147016", "2026-06-15", "2026-06-17"))
ce_day = ce_ctx
pe_day = pe_ctx[pe_ctx["ts"].dt.date >= pd.Timestamp("2026-06-15").date()]
print(f"CE1: {len(ce_ctx)} bars  PE1: {len(pe_day)} bars")

sensex_legs = [
    ("CE1", ce_day, ce_ctx),
    ("PE1", pe_day, pe_ctx),
]
sensex_days = ["2026-06-15", "2026-06-16", "2026-06-17"]
s_total = run_backtest("SENSEX", sensex_legs, sensex_days, sl_buf=2.0)

# ── CrudeOil ────────────────────────────────────────────────────────────────────
# CrudeOil Jul 2026 futures on MCX
# htf_source=futures: scan futures bars for zone, execute via CE option
# We backtest futures bars directly (same logic as engine)
print("\n\nFetching CrudeOil Jul26 futures...")
# MCX CrudeOil Jul 2026 — need correct instrument key
# Try to find via Upstox search or use known key
crude_key = "MCX_FO|431862"  # CrudeOil Jul 2026 — verify via logs

crude_ctx = fetch(crude_key, "2026-06-10", "2026-06-17")

# MCX CrudeOil trades 09:00-23:30 IST
if not crude_ctx.empty:
    crude_ctx = crude_ctx[(crude_ctx["ts"].dt.time >= pd.Timestamp("09:00").time()) &
                          (crude_ctx["ts"].dt.time <= pd.Timestamp("23:30").time())]
    print(f"CrudeOil: {len(crude_ctx)} bars  Range: {crude_ctx['c'].min():.0f}-{crude_ctx['c'].max():.0f}")

    crude_legs = [("FUT", crude_ctx, crude_ctx)]
    crude_days = ["2026-06-15", "2026-06-16", "2026-06-17"]
    c_total = run_backtest("CRUDEOIL", crude_legs, crude_days, sl_buf=5.0)
else:
    print("CrudeOil fetch failed — check instrument key")
    c_total = 0

print(f"\n{'='*70}")
print(f"COMBINED NET: {s_total + c_total:.1f} pts")
