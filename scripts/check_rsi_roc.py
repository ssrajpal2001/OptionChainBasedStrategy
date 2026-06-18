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
        groups = {}
        for b in bars_1m:
            try:
                dt = datetime.fromisoformat(str(b["ts"]).replace("Z", "+00:00")) if isinstance(b["ts"], str) else datetime.fromtimestamp(float(b["ts"]), tz=timezone.utc)
            except Exception:
                dt = datetime.fromtimestamp(0, tz=timezone.utc)
            dt_ist = dt.astimezone(IST)
            # group index = minutes since 09:15 // tf
            mins_since_open = (dt_ist.hour * 60 + dt_ist.minute) - (9 * 60 + 15)
            g = max(mins_since_open, 0) // tf
            groups[g] = {"ts": dt_ist, "close": b["close"]}
        return [groups[g] for g in sorted(groups.keys())]

    tf_bars = resample(bars_1m, TF)
    # drop in-progress (last) group if market is live
    # always show all completed for EOD analysis

    closes = [b["close"] for b in tf_bars]
    arr_all = np.array(closes, dtype=np.float64)

    RSI_LEN = 14
    ROC_LEN = 10

    print(f"\n{'='*62}")
    print(f"  TF={TF}m  ({len(tf_bars)} bars)")
    print(f"{'='*62}")
    print(f"  {'Time':<8} {'Close':>8} {'RSI(14)':>9} {'ROC(10)':>9}")
    print(f"  {'-'*8} {'-'*8} {'-'*9} {'-'*9}")

    for i in range(len(tf_bars)):
        arr_i = arr_all[:i+1]
        rsi_val = f"{float(_rsi(arr_i)):>9.2f}" if len(arr_i) >= RSI_LEN + 1 else f"{'N/A':>9}"
        roc_val = (f"{float((arr_i[-1]-arr_i[-ROC_LEN-1])/arr_i[-ROC_LEN-1]*100):>9.2f}"
                   if len(arr_i) >= ROC_LEN + 1 and arr_i[-ROC_LEN-1] != 0 else f"{'N/A':>9}")
        ts = tf_bars[i]["ts"]
        t_str = ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)
        print(f"  {t_str:<8} {closes[i]:>8.2f} {rsi_val} {roc_val}")

asyncio.run(main())
