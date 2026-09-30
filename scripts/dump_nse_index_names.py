"""scripts/dump_nse_index_names.py

One-off diagnostic: downloads Upstox's real public NSE instrument master
(same source strategies/relative_strength/sectors.py uses) and prints every
NSE_INDEX name containing any of the given keywords -- lets us find the
EXACT real spelling Upstox uses instead of guessing again (2026-09-30: 7 of
8 newly-added sector/factor index names failed to resolve on the first real
run, same class of miss this file's own history already had for a couple of
its original entries).

Usage:
    python scripts/dump_nse_index_names.py EV QUALITY VOLATILITY MOMENTUM ALPHA
    python scripts/dump_nse_index_names.py          # dumps ALL NSE_INDEX names
"""
import gzip
import json
import sys

from curl_cffi import requests as cc

_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"


def main() -> None:
    keywords = [k.upper() for k in sys.argv[1:]]
    r = cc.get(_URL, impersonate="chrome131", timeout=30)
    instruments = json.loads(gzip.decompress(r.content))
    names = sorted({
        str(inst.get("name") or inst.get("trading_symbol") or "").strip()
        for inst in instruments
        if inst.get("segment") == "NSE_INDEX"
    })
    for name in names:
        if not keywords or any(k in name.upper() for k in keywords):
            print(name)
    print(f"\n({len(names)} total NSE_INDEX names)", file=sys.stderr)


if __name__ == "__main__":
    main()
