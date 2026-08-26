"""scripts/check_stock_expiries_raw.py -- diagnostic: dump EVERY raw NSE F&O
contract for a given underlying (any expiry, no filtering beyond
ikey-prefix + underlying match), straight from Upstox's real live master
JSON -- the exact same download+parse InstrumentRegistry._load_from_master_json
uses for individual F&O stocks. Bypasses the registry's own `expiry >= today`
filter entirely, so this shows whether a specific month's contract genuinely
exists in the raw feed or was silently dropped somewhere in parsing.

Usage:
    python scripts/check_stock_expiries_raw.py KOTAKBANK
    python scripts/check_stock_expiries_raw.py VOLTAS
"""
import gzip
import json
import ssl
import sys
from datetime import date, datetime
from urllib.request import Request, urlopen

from config.global_config import IST

_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"


def _parse_instrument(inst):
    if isinstance(inst, dict):
        return (
            inst.get("instrument_key", ""), inst.get("trading_symbol", ""),
            inst.get("strike_price", 0), inst.get("expiry", ""),
            inst.get("instrument_type", ""),
        )
    return (
        getattr(inst, "instrument_key", ""), getattr(inst, "trading_symbol", ""),
        getattr(inst, "strike_price", 0), getattr(inst, "expiry", ""),
        getattr(inst, "instrument_type", ""),
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/check_stock_expiries_raw.py <UNDERLYING>")
        sys.exit(1)
    underlying = sys.argv[1].upper()

    print(f"Downloading real live NSE master JSON from {_URL} ...")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = Request(_URL, headers={"Accept-Encoding": "gzip"})
    with urlopen(req, timeout=60, context=ctx) as r:
        raw = r.read()
    try:
        instruments = json.loads(gzip.decompress(raw))
    except Exception:
        instruments = json.loads(raw)
    print(f"Downloaded {len(instruments)} total NSE instruments.\n")

    today = date.today()
    print(f"Today (server wall-clock date) = {today.isoformat()}\n")

    matches = []
    skipped_unparseable = 0
    for inst in instruments:
        ikey, ts, strike_raw, exp_raw, itype = _parse_instrument(inst)
        if not ikey or not ikey.startswith("NSE_FO|"):
            continue
        uns = (inst.get("underlying_symbol") or inst.get("name") or "") if isinstance(inst, dict) else ""
        if uns:
            if str(uns).upper() != underlying:
                continue
        elif not ts.startswith(underlying):
            continue

        try:
            if isinstance(exp_raw, (int, float)) or (isinstance(exp_raw, str) and str(exp_raw).strip().isdigit()):
                epoch = int(exp_raw)
                if epoch > 10_000_000_000:
                    epoch //= 1000
                expiry_date = datetime.fromtimestamp(epoch, IST).date()
            else:
                expiry_date = date.fromisoformat(str(exp_raw)[:10])
        except Exception as exc:
            skipped_unparseable += 1
            print(f"  UNPARSEABLE expiry for {ts!r} (ikey={ikey!r}): raw expiry={exp_raw!r} error={exc!r}")
            continue

        matches.append((expiry_date, ikey, ts, strike_raw, itype))

    if not matches:
        print(f"No NSE_FO contracts found at all for underlying={underlying!r}.")
        return

    matches.sort(key=lambda m: m[0])
    all_expiries = sorted(set(m[0] for m in matches))
    print(f"ALL distinct expiries found for {underlying} in the raw feed (no date filtering): ")
    for e in all_expiries:
        tag = ""
        if e < today:
            tag = "  <-- BEFORE today (already expired)"
        elif e == today:
            tag = "  <-- IS today"
        print(f"  {e.isoformat()}{tag}")

    print(f"\nTotal raw contracts matched: {len(matches)}  (unparseable/skipped: {skipped_unparseable})")

    # Show a few sample contracts for the NEAREST expiry, so it's obvious what's
    # actually being traded on that date (strike/type), not just the date itself.
    nearest = all_expiries[0]
    print(f"\nSample contracts for nearest expiry {nearest.isoformat()}:")
    for e, ikey, ts, strike_raw, itype in matches:
        if e == nearest:
            print(f"  {ts!r:30s} ikey={ikey!r} strike={strike_raw!r} type={itype!r}")


if __name__ == "__main__":
    main()
