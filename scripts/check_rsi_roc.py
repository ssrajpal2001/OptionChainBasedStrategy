"""
Check RSI/ROC for NIFTY straddle pair from today's Upstox 1m bars.
Usage: python scripts/check_rsi_roc.py [ce_strike] [pe_strike]
Example: python scripts/check_rsi_roc.py 24100 24100

Reads Upstox token from DB. Prints RSI(14) and ROC(10) at 1m, 2m, 3m.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CE_STRIKE = int(sys.argv[1]) if len(sys.argv) > 1 else 24100
PE_STRIKE = int(sys.argv[2]) if len(sys.argv) > 2 else CE_STRIKE

async def main():
    import numpy as np
    from data_layer.client_db import ClientDB
    from data_layer.historical_candles import fetch_upstox_warm_1m
    from data_layer.instrument_registry import REGISTRY
    from matrix_engine.indicators import rsi as _rsi
    from datetime import date

    creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("ERROR: no upstox token in DB"); return

    und = "NIFTY"
    # Registry must be loaded before use
    await asyncio.to_thread(REGISTRY.load_sync, und, token)
    exp = REGISTRY.get_active_expiry(und, date.today())
    print(f"Using expiry: {exp}")

    def get_bars(strike, side):
        import asyncio as _a
        ikey = REGISTRY.get_broker_symbol(und, exp, strike, side, "upstox")
        print(f"  {strike}{side} ikey={ikey}")
        return ikey

    ce_ikey = get_bars(CE_STRIKE, "CE")
    pe_ikey = get_bars(PE_STRIKE, "PE")
    if not ce_ikey or not pe_ikey:
        print("ERROR: could not resolve instrument keys"); return

    ce_bars = await fetch_upstox_warm_1m(ce_ikey, token)
    pe_bars = await fetch_upstox_warm_1m(pe_ikey, token)

    ce_closes = [b["close"] for b in ce_bars] if ce_bars else []
    pe_closes = [b["close"] for b in pe_bars] if pe_bars else []

    print(f"\nCE{CE_STRIKE}: {len(ce_closes)} bars  last={ce_closes[-1] if ce_closes else 'N/A'}")
    print(f"PE{PE_STRIKE}: {len(pe_closes)} bars  last={pe_closes[-1] if pe_closes else 'N/A'}")

    if not ce_closes or not pe_closes:
        print("ERROR: no bar data"); return

    n = min(len(ce_closes), len(pe_closes))
    combined = [ce_closes[-n+i] + pe_closes[-n+i] for i in range(n)]
    print(f"\nCombined premium: {combined[-1]:.2f} (CE{ce_closes[-1]:.2f} + PE{pe_closes[-1]:.2f})")

    def resample_closes(closes, tf):
        """Resample 1m closes to tf-minute closes (last bar per group). Drop in-progress group."""
        groups = {}
        for i, c in enumerate(closes):
            groups[i // tf] = c
        keys = sorted(groups.keys())
        result = [groups[k] for k in keys]
        # drop in-progress group if current minute isn't aligned
        if len(closes) % tf != 0:
            result = result[:-1]
        return result

    # Build time series table
    ce_times = [b["ts"] for b in ce_bars] if ce_bars else []  # timestamps from CE bars
    # align times to combined window
    ce_times_aligned = ce_times[-n:] if len(ce_times) >= n else ce_times

    for tf_name, tf in [("1m", 1), ("2m", 2), ("3m", 3)]:
        tf_closes = combined if tf == 1 else resample_closes(combined, tf)
        tf_times  = ce_times_aligned if tf == 1 else resample_closes(ce_times_aligned, tf)
        print(f"\n{'='*55}")
        print(f"  Timeframe: {tf_name}  ({len(tf_closes)} bars)")
        print(f"{'='*55}")
        print(f"  {'Time':<20} {'Close':>8} {'RSI(14)':>9} {'ROC(10)':>9}")
        print(f"  {'-'*20} {'-'*8} {'-'*9} {'-'*9}")
        arr_all = np.array(tf_closes, dtype=np.float64)
        for i in range(len(tf_closes)):
            if i < 14:  # not enough history for RSI
                continue
            arr_i = arr_all[:i+1]
            rsi_i = float(_rsi(arr_i))
            roc_i = float((arr_i[-1] - arr_i[-11]) / arr_i[-11] * 100) if len(arr_i) >= 11 and arr_i[-11] != 0 else None
            ts = tf_times[i] if i < len(tf_times) else ""
            try:
                from datetime import timezone, timedelta
                IST = timezone(timedelta(hours=5, minutes=30))
                dt = datetime.fromtimestamp(float(ts), tz=IST).strftime("%H:%M") if ts else f"bar{i}"
            except Exception:
                dt = f"bar{i}"
            roc_str = f"{roc_i:>9.2f}" if roc_i is not None else f"{'N/A':>9}"
            print(f"  {dt:<20} {tf_closes[i]:>8.2f} {rsi_i:>9.2f} {roc_str}")

asyncio.run(main())
