"""
Check RSI/ROC time-series for a NIFTY straddle pair from today's Upstox 1m bars.
Usage: python3 scripts/check_rsi_roc.py [ce_strike] [pe_strike] [tf_minutes]
Example: python3 scripts/check_rsi_roc.py 24100 24100 3
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CE_STRIKE = int(sys.argv[1]) if len(sys.argv) > 1 else 24100
PE_STRIKE = int(sys.argv[2]) if len(sys.argv) > 2 else CE_STRIKE
TF         = int(sys.argv[3]) if len(sys.argv) > 3 else 3

async def main():
    import numpy as np
    from datetime import datetime, timezone, timedelta
    from data_layer.client_db import ClientDB
    from data_layer.historical_candles import fetch_upstox_warm_1m
    from data_layer.instrument_registry import REGISTRY
    from matrix_engine.indicators import rsi as _rsi
    from datetime import date

    IST = timezone(timedelta(hours=5, minutes=30))

    creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("ERROR: no upstox token in DB"); return

    und = "NIFTY"
    await asyncio.to_thread(REGISTRY.load_sync, und, token)
    exp = REGISTRY.get_active_expiry(und, date.today())
    print(f"Expiry: {exp}  CE={CE_STRIKE}  PE={PE_STRIKE}  TF={TF}m\n")

    ce_ikey = REGISTRY.get_broker_symbol(und, exp, CE_STRIKE, "CE", "upstox")
    pe_ikey = REGISTRY.get_broker_symbol(und, exp, PE_STRIKE, "PE", "upstox")
    if not ce_ikey or not pe_ikey:
        print("ERROR: could not resolve instrument keys"); return

    ce_bars = await fetch_upstox_warm_1m(ce_ikey, token)
    pe_bars = await fetch_upstox_warm_1m(pe_ikey, token)
    if not ce_bars or not pe_bars:
        print("ERROR: no bar data"); return

    n = min(len(ce_bars), len(pe_bars))
    ce_bars = ce_bars[-n:]
    pe_bars = pe_bars[-n:]

    # Build 1m combined series with timestamps
    bars_1m = []
    for cb, pb in zip(ce_bars, pe_bars):
        bars_1m.append({"ts": cb["ts"], "close": cb["close"] + pb["close"]})

    print(f"1m bars: {len(bars_1m)}  combined last={bars_1m[-1]['close']:.2f}")

    # Resample to TF: keep LAST bar per group (close of tf candle); include in-progress group
    def resample(bars_1m, tf):
        # Upstox 1m bars use CLOSE-time labels: bar "9:15" = 9:14:00-9:15:00.
        # So first 3m candle = bars 9:16, 9:17, 9:18 → labeled 9:18 (close time).
        # Grouping: (abs_minute - 1) // tf puts 9:16,9:17,9:18 in the same bucket.
        groups = {}
        for b in bars_1m:
            try:
                dt = datetime.fromisoformat(str(b["ts"]).replace("Z", "+00:00")) if isinstance(b["ts"], str) else datetime.fromtimestamp(float(b["ts"]), tz=timezone.utc)
            except Exception:
                dt = datetime.fromtimestamp(0, tz=timezone.utc)
            dt_ist = dt.astimezone(IST)
            abs_min = dt_ist.hour * 60 + dt_ist.minute
            g = abs_min // tf  # clock-aligned: 9:15,9:16,9:17 → same group; 9:18 → next
            if g not in groups:
                groups[g] = {"ts": dt_ist, "close": b["close"]}
            else:
                groups[g]["close"] = b["close"]
                # label = close time = last bar open + 1 min
                groups[g]["ts"] = dt_ist + timedelta(minutes=1)
        return [groups[g] for g in sorted(groups.keys())]

    tf_bars_all = resample(bars_1m, TF)

    # Split into prev-day (seed) and today's bars
    today_date = date.today()
    today_bars  = [b for b in tf_bars_all if b["ts"].date() == today_date]
    seed_bars   = [b for b in tf_bars_all if b["ts"].date() < today_date]

    seed_closes = [b["close"] for b in seed_bars]
    today_closes = [b["close"] for b in today_bars]

    print(f"Prev-day seed bars (3m): {len(seed_closes)} | Today bars (3m): {len(today_closes)}")

    RSI_LEN = 14
    ROC_LEN = 10

    print(f"\n{'='*62}")
    print(f"  TF={TF}m — TODAY only (RSI/ROC seeded from prev-day)")
    print(f"{'='*62}")
    print(f"  {'Time':<8} {'Close':>8} {'RSI(14)':>9} {'ROC(10)':>9}")
    print(f"  {'-'*8} {'-'*8} {'-'*9} {'-'*9}")

    for i in range(len(today_bars)):
        # full series = seed + today up to bar i (so RSI is valid from bar 0 of today)
        full_closes = seed_closes + today_closes[:i+1]
        arr_i = np.array(full_closes, dtype=np.float64)
        rsi_val = f"{float(_rsi(arr_i)):>9.2f}" if len(arr_i) >= RSI_LEN + 1 else f"{'N/A':>9}"
        roc_val = (f"{float((arr_i[-1]-arr_i[-ROC_LEN-1])/arr_i[-ROC_LEN-1]*100):>9.2f}"
                   if len(arr_i) >= ROC_LEN + 1 and arr_i[-ROC_LEN-1] != 0 else f"{'N/A':>9}")
        ts = today_bars[i]["ts"]
        t_str = ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)
        print(f"  {t_str:<8} {today_closes[i]:>8.2f} {rsi_val} {roc_val}")

asyncio.run(main())
