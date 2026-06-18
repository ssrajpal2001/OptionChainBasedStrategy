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

    for tf_name, tf_closes in [("1m", combined), ("2m", resample_closes(combined, 2)), ("3m", resample_closes(combined, 3))]:
        arr = np.array(tf_closes, dtype=np.float64)
        n_tf = len(arr)
        rsi_val = float(_rsi(arr)) if n_tf >= 15 else None
        roc_val = float((arr[-1] - arr[-11]) / arr[-11] * 100) if n_tf >= 11 and arr[-11] != 0 else None
        rsi_str = f"{rsi_val:.2f}" if rsi_val is not None else f"N/A ({n_tf} bars, need 15)"
        roc_str = f"{roc_val:.2f}%" if roc_val is not None else f"N/A ({n_tf} bars, need 11)"
        print(f"\n  [{tf_name}] bars={n_tf}  close={tf_closes[-1]:.2f}")
        print(f"    RSI(14) = {rsi_str}")
        print(f"    ROC(10) = {roc_str}")

asyncio.run(main())
