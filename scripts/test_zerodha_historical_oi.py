"""One-shot verification: does Zerodha's Kite Connect historical_data API
(oi=1) actually return real minute-level open interest for a real NIFTY
option/futures instrument, for a REAL connected binding in this app's own
clients.db? Run this ON THE SERVER (data/clients.db lives there), not
locally -- prints the raw response so it can be eyeballed directly.

Usage: python scripts/test_zerodha_historical_oi.py <client_id> <binding_id>
e.g.:  python scripts/test_zerodha_historical_oi.py ssrajpal2001 SA5770
"""
import sys
from datetime import datetime, timedelta

from data_layer.client_db import ClientDB


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    client_id, binding_id = sys.argv[1], sys.argv[2]

    db = ClientDB()
    bindings = db.get_bindings_sync(client_id)
    binding = next((b for b in bindings if b.get("binding_id") == binding_id), None)
    if not binding:
        print(f"No binding {binding_id!r} found for client {client_id!r}.")
        sys.exit(1)
    if (binding.get("broker") or "").lower() != "zerodha":
        print(f"Binding {binding_id!r} is {binding.get('broker')!r}, not zerodha.")
        sys.exit(1)

    api_key = binding.get("api_key") or ""
    token = binding.get("access_token") or ""
    if not api_key or not token:
        print("Missing api_key or access_token on this binding.")
        sys.exit(1)

    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(token)

    # Resolve the real NIFTY near-month futures instrument_token via Kite's
    # own instrument dump (no hardcoded token -- expiries roll monthly).
    print("Fetching Kite instrument dump (NFO)...")
    instruments = kite.instruments("NFO")
    today = datetime.now().date()
    nifty_futs = sorted(
        (i for i in instruments if i["name"] == "NIFTY" and i["instrument_type"] == "FUT"
         and i["expiry"] >= today),
        key=lambda i: i["expiry"],
    )
    if not nifty_futs:
        print("No NIFTY futures contract found in the instrument dump.")
        sys.exit(1)
    fut = nifty_futs[0]
    print(f"Using {fut['tradingsymbol']} (token={fut['instrument_token']}, expiry={fut['expiry']})")

    frm = datetime.now() - timedelta(days=1)
    to = datetime.now()
    print(f"Fetching 1-minute historical candles with oi=1, {frm} -> {to} ...")
    candles = kite.historical_data(
        fut["instrument_token"], frm, to, interval="minute", oi=True,
    )
    print(f"Got {len(candles)} candles.")
    if candles:
        print("First 3 candles:")
        for c in candles[:3]:
            print(" ", c)
        print("Last 3 candles:")
        for c in candles[-3:]:
            print(" ", c)
        real_oi = [c for c in candles if c.get("oi", 0) not in (0, None)]
        print(f"\n{len(real_oi)} / {len(candles)} candles have a non-zero 'oi' field.")
        if real_oi:
            print("CONFIRMED: Zerodha's historical API returns real intraday OI.")
        else:
            print("Every candle's 'oi' field is 0/missing -- NOT usable, same as Upstox.")


if __name__ == "__main__":
    main()
