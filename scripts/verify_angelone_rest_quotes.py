"""
scripts/verify_angelone_rest_quotes.py -- 2026-09-09, direct user request:
"we can check if angel is provide correct data of index and future and
some option ltp we can check now, websocket can be checked tomorrow in
live market."

Standalone REST-only sanity check (no WebSocket) -- authenticates using the
SAME headless MPIN+TOTP flow already proven in AngelOneFeeder.connect() and
execution_bridge/broker_angel.py, then fetches real quotes via SmartAPI's
ltpData() for:
  1. NIFTY spot index (well-known token, same as AngelOneFeeder's own
     _ANGELONE_INDEX_TOKENS).
  2. NIFTY near-month futures (resolved via the SAME
     SymbolTranslator.to_angelone_futures() + scrip search this session
     just added to AngelOneFeeder._resolve_futures_token()).
  3. An ATM-ish NIFTY option (CE), resolved via searchScrip the same way
     AngelOneFeeder._resolve_option_token() does.

Run this ON THE EC2 BOX (real feeder credentials live in data/clients.db
there, not in this repo).

Usage: python scripts/verify_angelone_rest_quotes.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

sys.path.insert(0, ".")


async def main():
    from data_layer.client_db import ClientDB
    from data_layer.instrument_registry import REGISTRY
    from data_layer.symbol_translator import SymbolTranslator, InternalSymbol

    db = ClientDB()
    creds = db.get_feeder_creds_sync("angelone")
    if not creds or not creds.get("client_id") or not creds.get("totp_secret"):
        print("No usable AngelOne feeder credentials found in data/clients.db "
              "(need client_id + api_key + password + totp_secret).")
        return

    try:
        from SmartApi import SmartConnect
        import pyotp
    except ImportError:
        print("smartapi-python / pyotp not installed -- pip install smartapi-python pyotp")
        return

    smartapi = SmartConnect(api_key=creds["api_key"])
    totp = pyotp.TOTP(creds["totp_secret"]).now()
    session = await asyncio.to_thread(
        smartapi.generateSession, creds["client_id"], creds["password"], totp)
    if not (session and session.get("status")):
        print(f"Headless auth FAILED: {session}")
        return
    print("Headless auth OK.")

    def quote(exchange, tradingsymbol, symboltoken, label):
        try:
            res = smartapi.ltpData(exchange, tradingsymbol, symboltoken)
            print(f"  {label:<24} exchange={exchange:<4} symbol={tradingsymbol:<20} "
                  f"token={symboltoken:<8} -> {res}")
        except Exception as exc:
            print(f"  {label:<24} FAILED: {exc}")

    print("\n--- 1) NIFTY spot index ---")
    quote("NSE", "Nifty 50", "99926000", "NIFTY SPOT")

    print("\n--- 2) NIFTY near-month futures ---")
    today = date.today()
    REGISTRY.load_futures_only_sync("NIFTY", today)
    fut_expiry = REGISTRY.get_futures_expiry("NIFTY")
    if fut_expiry is None:
        print("  Could not resolve NIFTY futures expiry from master JSON.")
    else:
        fut_symbol = SymbolTranslator.to_angelone_futures("NIFTY", fut_expiry)
        try:
            res = smartapi.searchScrip("NFO", fut_symbol)
            token = ""
            if res and res.get("status") and res.get("data"):
                for it in res["data"]:
                    if it.get("tradingsymbol") == fut_symbol:
                        token = str(it.get("symboltoken", ""))
                        break
            if token:
                quote("NFO", fut_symbol, token, "NIFTY FUTURES")
            else:
                print(f"  searchScrip found no match for {fut_symbol}: {res}")
        except Exception as exc:
            print(f"  searchScrip FAILED for {fut_symbol}: {exc}")

    # Same real rate limit AngelOneFeeder._resolve_option_token was fixed for
    # this session -- this standalone script calls searchScrip() directly
    # (no throttle infra to reuse for a one-shot check), so space the two
    # real searchScrip calls out manually instead.
    await asyncio.sleep(1.5)

    print("\n--- 3) An ATM-ish NIFTY option (nearest weekly expiry) ---")
    if not REGISTRY.is_loaded("NIFTY"):
        REGISTRY.load_sync("NIFTY")
    opt_expiry = REGISTRY.get_active_expiry("NIFTY", from_date=today)
    if opt_expiry is None:
        print("  Could not resolve NIFTY option expiry.")
    else:
        # Rough ATM guess (real spot from the query above) -- close enough,
        # this is purely a sanity check on quote correctness, not live trading.
        guess_strike = 23500
        internal = InternalSymbol(underlying="NIFTY", strike=float(guess_strike),
                                   option_type="CE", expiry=opt_expiry)
        opt_symbol = SymbolTranslator.to_angelone(internal)
        try:
            res = smartapi.searchScrip("NFO", opt_symbol)
            token = ""
            if res and res.get("status") and res.get("data"):
                for it in res["data"]:
                    if it.get("tradingsymbol") == opt_symbol:
                        token = str(it.get("symboltoken", ""))
                        break
            if token:
                quote("NFO", opt_symbol, token, "NIFTY OPTION CE")
            else:
                print(f"  searchScrip found no match for {opt_symbol}: {res}")
        except Exception as exc:
            print(f"  searchScrip FAILED for {opt_symbol}: {exc}")

    print("\nCompare the 'ltp'/'close' values above against a real chart "
          "(TradingView, broker terminal, etc.) for today's last traded levels.")


if __name__ == "__main__":
    asyncio.run(main())
