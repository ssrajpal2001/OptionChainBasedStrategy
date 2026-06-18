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

    from data_layer.historical_candles import fetch_upstox_1m, fetch_upstox_intraday_1m
    # Always fetch both prev-day and today separately — warm_1m skips prev-day when today>=15 bars
    ce_prev, ce_today = await asyncio.gather(fetch_upstox_1m(ce_ikey, token), fetch_upstox_intraday_1m(ce_ikey, token))
    pe_prev, pe_today = await asyncio.gather(fetch_upstox_1m(pe_ikey, token), fetch_upstox_intraday_1m(pe_ikey, token))
    ce_bars = ce_prev + ce_today
    pe_bars = pe_prev + pe_today
    print(f"CE{CE_STRIKE}: prev={len(ce_prev)} today={len(ce_today)}  PE{PE_STRIKE}: prev={len(pe_prev)} today={len(pe_today)}")
    if not ce_bars or not pe_bars:
        print("ERROR: no bar data"); return

    n = min(len(ce_bars), len(pe_bars))
    ce_bars = ce_bars[-n:]
    pe_bars = pe_bars[-n:]

    def combine_1m(ce_list, pe_list):
        n = min(len(ce_list), len(pe_list))
        return [{"ts": ce_list[-n+i]["ts"], "close": ce_list[-n+i]["close"] + pe_list[-n+i]["close"]}
                for i in range(n)]

    prev_1m  = combine_1m(ce_prev, pe_prev)
    today_1m = combine_1m(ce_today, pe_today)
    print(f"1m bars: prev={len(prev_1m)} today={len(today_1m)}")

    def resample(bars_1m, tf):
        """Resample 1m bars to tf-minute candles. Each group keyed by bar index, not abs_min,
        so prev-day and today don't collide when resampled separately."""
        groups = {}
        for b in bars_1m:
            try:
                dt = datetime.fromisoformat(str(b["ts"]).replace("Z", "+00:00")) if isinstance(b["ts"], str) else datetime.fromtimestamp(float(b["ts"]), tz=timezone.utc)
            except Exception:
                dt = datetime.fromtimestamp(0, tz=timezone.utc)
            dt_ist = dt.astimezone(IST)
            abs_min = dt_ist.hour * 60 + dt_ist.minute
            g = abs_min // tf
            if g not in groups:
                groups[g] = {"ts": dt_ist, "close": b["close"]}
            else:
                groups[g]["close"] = b["close"]
                groups[g]["ts"] = dt_ist + timedelta(minutes=1)
        return [groups[g] for g in sorted(groups.keys())]

    # Resample SEPARATELY so same-time groups don't overwrite each other
    seed_bars  = resample(prev_1m,  TF)
    today_bars = resample(today_1m, TF)
    seed_closes  = [b["close"] for b in seed_bars]
    today_closes = [b["close"] for b in today_bars]
    print(f"Seed bars ({TF}m): {len(seed_closes)} | Today bars ({TF}m): {len(today_closes)}")

    RSI_LEN = 14
    ROC_LEN = 9   # (close - close[9]) / close[9] * 100 → 9 bars back

    print(f"\n{'='*62}")
    print(f"  TF={TF}m — TODAY only (RSI/ROC seeded from prev-day)")
    print(f"{'='*62}")
    print(f"  {'Time':<8} {'Close':>8} {'RSI(14)':>9} {'ROC(10)':>9}")
    print(f"  {'-'*8} {'-'*8} {'-'*9} {'-'*9}")

    def wilder_rsi_series(closes, period=14):
        """Proper Wilder RSI over the full series. Returns list of (rsi or None) per bar."""
        out = [None] * len(closes)
        if len(closes) < period + 1:
            return out
        deltas = np.diff(np.array(closes, dtype=np.float64))
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        # seed with SMA of first `period` changes
        avg_g = float(gains[:period].mean())
        avg_l = float(losses[:period].mean())
        out[period] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
        for i in range(period, len(deltas)):
            avg_g = (avg_g * (period - 1) + float(gains[i])) / period
            avg_l = (avg_l * (period - 1) + float(losses[i])) / period
            out[i + 1] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1.0 + avg_g / avg_l)
        return out

    # Compute RSI over full series (seed + today), then show only today's slice
    full_closes = seed_closes + today_closes
    rsi_series  = wilder_rsi_series(full_closes, RSI_LEN)
    rsi_today   = rsi_series[len(seed_closes):]  # today's RSI values aligned to today_closes

    for i in range(len(today_bars)):
        full_idx = len(seed_closes) + i
        rsi_v = rsi_series[full_idx]
        rsi_val = f"{rsi_v:>9.2f}" if rsi_v is not None else f"{'N/A':>9}"
        # ROC uses full series too
        full_so_far = full_closes[:full_idx + 1]
        roc_v = ((full_so_far[-1] - full_so_far[-ROC_LEN-1]) / full_so_far[-ROC_LEN-1] * 100
                 if len(full_so_far) >= ROC_LEN + 1 and full_so_far[-ROC_LEN-1] != 0 else None)
        roc_val = f"{roc_v:>9.2f}" if roc_v is not None else f"{'N/A':>9}"
        ts = today_bars[i]["ts"]
        t_str = ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)
        print(f"  {t_str:<8} {today_closes[i]:>8.2f} {rsi_val} {roc_val}")

asyncio.run(main())
