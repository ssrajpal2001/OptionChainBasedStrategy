"""Validate the new futures-key resolution added to InstrumentRegistry, and
check how far back NIFTY/SENSEX futures 1-min history is actually available
(rolling monthly contracts -- unlike spot, a given contract's instrument_key
only existed during its own expiry month, so multi-month backtests may need
per-month key resolution against archived masters, not just today's)."""
import sys, asyncio
sys.path.insert(0, ".")
from datetime import date, timedelta
from data_layer.instrument_registry import InstrumentRegistry
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN_PATH = r"C:\Users\SERVER\AppData\Local\Temp\claude\e--AlgoSoft-OptionChainBasedStrategy\3f952902-2e64-455f-be1c-fac0a7378cbc\scratchpad\upstox_token.txt"


async def main():
    token = open(TOKEN_PATH).read().strip()
    reg = InstrumentRegistry()
    for u in ("NIFTY", "SENSEX"):
        reg.load_sync(u, token)
        fkey = reg.get_futures_upstox(u)
        print(f"{u}: futures_key={fkey!r} expiry={reg._futures_expiry.get(u)}")
        if not fkey:
            for line in reg.get_diagnostics(u):
                print("  diag:", line.encode("ascii", "replace").decode())
            continue
        rows = await fetch_upstox_range_1m(fkey, token, date.today() - timedelta(days=5), date.today())
        print(f"  fetched {len(rows)} 1m bars, last 3: {rows[-3:] if rows else []}")

asyncio.run(main())
