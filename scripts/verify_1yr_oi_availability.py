"""One-shot feasibility check (direct user ask, 2026-10-12): does Upstox
and/or Zerodha actually return REAL open interest for ~1-year-old EXPIRED
NIFTY contracts, at both daily and intraday granularity? This codebase has
repeatedly confirmed (OI-Flow, OI-ORB, vp_oi_regime/recorder.py) that
Upstox's INTRADAY candle API hardcodes OI to 0 -- but never specifically
re-tested a genuinely old EXPIRED contract, or Zerodha's own historical API
at this exact question (year-old expired options). Must be run ON THE
SERVER (real broker tokens live in data/clients.db there, not locally).

Usage: python scripts/verify_1yr_oi_availability.py <client_id> <binding_id>
e.g.:  python scripts/verify_1yr_oi_availability.py ssrajpal2001 SA5770

Prints raw responses so results can be eyeballed directly -- this is a
verification script, not a backtest; it makes NO claim about results ahead
of actually running it.
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB


def _get_binding(client_id: str, binding_id: str) -> dict:
    db = ClientDB()
    bindings = db.get_bindings_sync(client_id)
    b = next((x for x in bindings if x.get("binding_id") == binding_id), None)
    if not b:
        print(f"No binding {binding_id!r} for client {client_id!r}.")
        sys.exit(1)
    return b


def check_upstox() -> None:
    print("\n" + "=" * 70)
    print("UPSTOX -- expired NIFTY option, ~1 year back, day + 1minute")
    print("=" * 70)
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    if not creds or not creds.get("access_token"):
        print("No Upstox feeder credentials found (system feeder) -- skipping Upstox check.")
        return
    token = creds["access_token"]

    from curl_cffi import requests as cc
    # A real NIFTY weekly option expiry from ~1 year back. Upstox instrument
    # keys for expired contracts follow NSE_FO|<token> -- resolve via the
    # public instrument master (same source InstrumentRegistry already uses)
    # rather than guessing a key.
    import gzip
    import json
    import urllib.request
    print("Downloading Upstox NSE.json.gz instrument master (same source InstrumentRegistry uses)...")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = gzip.decompress(resp.read())
    instruments = json.loads(raw)
    one_year_ago = datetime.now() - timedelta(days=365)
    candidates = [
        i for i in instruments
        if i.get("name") == "NIFTY" and i.get("instrument_type") in ("CE", "PE")
        and i.get("expiry")
    ]
    # expiry is epoch millis in this master; find one closest to 1 year ago.
    candidates.sort(key=lambda i: abs((i["expiry"] / 1000.0) - one_year_ago.timestamp()))
    if not candidates:
        print("No NIFTY option instruments found in the master at all -- aborting Upstox check.")
        return
    inst = candidates[0]
    key = inst["instrument_key"]
    expiry_dt = datetime.fromtimestamp(inst["expiry"] / 1000.0)
    print(f"Using {inst.get('trading_symbol', key)} (key={key}, expiry={expiry_dt.date()})")

    for interval, frm, to in (
        ("day", expiry_dt - timedelta(days=30), expiry_dt),
        ("1minute", expiry_dt - timedelta(days=1), expiry_dt),
    ):
        url = (f"https://api.upstox.com/v2/historical-candle/{key}/{interval}/"
               f"{to.date()}/{frm.date()}")
        headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        r = cc.get(url, headers=headers, impersonate="chrome131", timeout=10)
        print(f"\n--- interval={interval} url={url}")
        print(f"status={r.status_code}")
        data = r.json()
        candles = (data.get("data", {}) or {}).get("candles", [])
        print(f"{len(candles)} candles returned.")
        if candles:
            print("Sample (newest-first, first 3):", candles[:3])
            oi_vals = [c[6] for c in candles if len(c) > 6]
            nonzero = [v for v in oi_vals if v not in (0, None)]
            print(f"OI column present on {len(oi_vals)}/{len(candles)} rows; "
                  f"{len(nonzero)} are non-zero.")


def check_zerodha(client_id: str, binding_id: str) -> None:
    print("\n" + "=" * 70)
    print("ZERODHA -- expired NIFTY option/future, ~1 year back, day + minute, oi=True")
    print("=" * 70)
    b = _get_binding(client_id, binding_id)
    if (b.get("broker") or "").lower() != "zerodha":
        print(f"Binding {binding_id!r} is {b.get('broker')!r}, not zerodha -- skipping.")
        return
    api_key, token = b.get("api_key") or "", b.get("access_token") or ""
    if not api_key or not token:
        print("Missing Zerodha api_key/access_token on this binding.")
        return

    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(token)

    print("Fetching Kite NFO instrument dump (only lists CURRENTLY active instruments)...")
    instruments = kite.instruments("NFO")
    print(f"{len(instruments)} active NFO instruments currently listed.")
    nifty_opts = [i for i in instruments if i["name"] == "NIFTY" and i["instrument_type"] in ("CE", "PE")]
    print(f"{len(nifty_opts)} of those are current NIFTY options "
          "(expired ones from a year ago will NOT be in this dump at all -- "
          "Kite only lists currently-tradeable instruments).")

    # Try a currently-near-expired or already-just-expired option's token
    # anyway, at the furthest-back interval Kite's API will allow, to at
    # least establish the real lookback ceiling (informational).
    if nifty_opts:
        test = sorted(nifty_opts, key=lambda i: i["expiry"])[0]
        print(f"\nTrying furthest lookback allowed, using {test['tradingsymbol']} "
              f"(token={test['instrument_token']}) as a stand-in since no "
              "1-year-old token is resolvable via this API at all:")
        for interval in ("day", "minute"):
            frm = datetime.now() - timedelta(days=365)
            to = datetime.now()
            try:
                candles = kite.historical_data(
                    test["instrument_token"], frm, to, interval=interval, oi=True,
                )
                print(f"  interval={interval}: {len(candles)} candles returned for a 365-day request.")
                if candles:
                    real_oi = [c for c in candles if c.get("oi", 0) not in (0, None)]
                    print(f"    {len(real_oi)}/{len(candles)} have non-zero oi.")
            except Exception as exc:
                print(f"  interval={interval}: REQUEST FAILED -- {exc}")
                print("    (this is the real answer to 'can Kite even serve a 365-day request "
                      "at this granularity' -- Kite enforces hard lookback limits per interval, "
                      "commonly ~60 days for minute data, longer for day data.)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    client_id, binding_id = sys.argv[1], sys.argv[2]
    check_upstox()
    check_zerodha(client_id, binding_id)
