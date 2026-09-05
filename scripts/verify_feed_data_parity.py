"""
scripts/verify_feed_data_parity.py -- real REST-API comparison of Upstox vs
AngelOne data for the SAME live NIFTY option contract, to answer: "is
AngelOne providing everything our strategies actually need, the same way
Upstox does?"

Read-only. Places no orders. Uses real credentials already saved in
data/clients.db (system_feeder_creds for upstox; system_feeder_creds for
angelone -- see the admin Data Feeder panel's AngelOne TEST card).

What this checks, field by field, against what strategies actually consume
(see CLAUDE.md's "Indicator Usage Per Strategy" -- LTP, broker ATP/VWAP, OI
are load-bearing; IV/delta are NOT relied on anywhere live today):
    ltp        -- last traded price
    atp        -- average traded price (the live "broker VWAP" strategies
                  read directly, e.g. vwap_source=broker_atp)
    oi         -- open interest
    volume     -- session volume
    bid/ask    -- best bid/ask

Usage:
    python scripts/verify_feed_data_parity.py [--strike 24000] [--side CE]

If --strike is omitted, ATM is computed from Upstox's own live NIFTY LTP.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from data_layer.client_db import ClientDB
from data_layer.instrument_registry import REGISTRY


def _fetch_upstox_quote(access_token: str, instrument_key: str) -> dict:
    url = "https://api.upstox.com/v2/market-quote/quotes"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, params={"instrument_key": instrument_key}, timeout=10)
    body = r.json()
    if not body.get("data"):
        return {"error": f"HTTP {r.status_code}: {body}"}
    row = next(iter(body["data"].values()))
    return {
        "ltp": row.get("last_price"),
        "atp": row.get("average_price"),
        "oi": row.get("oi"),
        "volume": row.get("volume"),
        "bid": (row.get("depth", {}).get("buy", [{}])[0] or {}).get("price"),
        "ask": (row.get("depth", {}).get("sell", [{}])[0] or {}).get("price"),
        "raw_keys": sorted(row.keys()),
    }


def _fetch_upstox_spot_ltp(access_token: str) -> float:
    q = _fetch_upstox_quote(access_token, "NSE_INDEX|Nifty 50")
    return float(q.get("ltp") or 0.0)


def _angelone_login(client_code: str, api_key: str, password: str, totp_secret: str) -> dict:
    from SmartApi import SmartConnect
    import pyotp

    sm = SmartConnect(api_key=api_key)
    totp_code = pyotp.TOTP(totp_secret).now()
    session = sm.generateSession(client_code, password, totp_code)
    if not (session and session.get("status")):
        return {"error": f"AngelOne login failed: {session}"}
    data = session.get("data") or {}
    return {
        "smartapi": sm,
        "jwt_token": data.get("jwtToken", "").replace("Bearer ", ""),
        "feed_token": data.get("feedToken") or sm.getfeedToken(),
        "client_code": client_code,
        "api_key": api_key,
    }


def _resolve_angelone_token(sm, underlying: str, strike: float, opt_type: str, expiry: date) -> tuple:
    from data_layer.symbol_translator import InternalSymbol, SymbolTranslator

    internal = InternalSymbol(underlying=underlying, strike=strike, option_type=opt_type, expiry=expiry)
    tradingsymbol = SymbolTranslator.to_angelone(internal)
    exchange = "BFO" if underlying.upper() == "SENSEX" else "NFO"
    res = sm.searchScrip(exchange, tradingsymbol)
    if not (res and res.get("status") and res.get("data")):
        return None, None, f"searchScrip found nothing for {tradingsymbol}"
    for it in res["data"]:
        if it.get("tradingsymbol") == tradingsymbol:
            return exchange, str(it.get("symboltoken", "")), tradingsymbol
    return None, None, f"tradingsymbol {tradingsymbol} not found in searchScrip results"


def _fetch_angelone_quote(login: dict, exchange: str, symboltoken: str) -> dict:
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/"
    headers = {
        "Authorization": f"Bearer {login['jwt_token']}",
        "X-PrivateKey": login["api_key"],
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
    }
    body = {"mode": "FULL", "exchangeTokens": {exchange: [symboltoken]}}
    r = requests.post(url, headers=headers, json=body, timeout=10)
    resp = r.json()
    fetched = ((resp.get("data") or {}).get("fetched") or [])
    if not fetched:
        return {"error": f"HTTP {r.status_code}: {resp}"}
    row = fetched[0]
    depth = row.get("depth") or {}
    return {
        "ltp": row.get("ltp"),
        "atp": row.get("avgPrice"),
        "oi": row.get("opnInterest"),
        "volume": row.get("tradeVolume"),
        "bid": (depth.get("buy", [{}])[0] or {}).get("price"),
        "ask": (depth.get("sell", [{}])[0] or {}).get("price"),
        "raw_keys": sorted(row.keys()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strike", type=float, default=None)
    ap.add_argument("--side", default="CE", choices=["CE", "PE"])
    ap.add_argument("--underlying", default="NIFTY")
    args = ap.parse_args()

    db = ClientDB()
    up_creds = db.get_feeder_creds_sync("upstox") or {}
    up_token = up_creds.get("access_token", "")
    if not up_token:
        print("FAIL: no Upstox access_token saved -- connect Upstox at least once first.")
        return 1

    REGISTRY.load_sync(args.underlying, up_token)
    expiry = REGISTRY.get_active_expiry(args.underlying, date.today())
    if not expiry:
        print(f"FAIL: could not resolve an active expiry for {args.underlying}.")
        return 1

    strike = args.strike
    if strike is None:
        spot = _fetch_upstox_spot_ltp(up_token)
        if not spot:
            print("FAIL: could not fetch live NIFTY spot from Upstox to compute ATM.")
            return 1
        step = 50.0 if args.underlying == "NIFTY" else 100.0
        strike = round(spot / step) * step
        print(f"Live {args.underlying} spot (Upstox): {spot} -> ATM strike {strike}")

    print(f"\nComparing {args.underlying} {int(strike)}{args.side} (expiry {expiry}) — Upstox vs AngelOne\n")

    # -- Upstox --
    up_key = REGISTRY.get_upstox_key(args.underlying, expiry, strike, args.side)
    if not up_key:
        print(f"FAIL: could not resolve an Upstox instrument_key for {args.underlying} {strike}{args.side}.")
        return 1
    up_quote = _fetch_upstox_quote(up_token, up_key)

    # -- AngelOne --
    ao_creds = db.get_feeder_creds_sync("angelone") or {}
    if not (ao_creds.get("client_id") and ao_creds.get("api_key")
            and ao_creds.get("password") and ao_creds.get("totp_secret")):
        print("FAIL: AngelOne credentials incomplete -- save them via the admin Data Feeder panel first.")
        return 1
    login = _angelone_login(ao_creds["client_id"], ao_creds["api_key"],
                             ao_creds["password"], ao_creds["totp_secret"])
    if "error" in login:
        print(f"FAIL: {login['error']}")
        return 1
    exchange, symboltoken, sym_or_err = _resolve_angelone_token(
        login["smartapi"], args.underlying, strike, args.side, expiry)
    if not symboltoken:
        print(f"FAIL: AngelOne token resolution failed: {sym_or_err}")
        return 1
    ao_quote = _fetch_angelone_quote(login, exchange, symboltoken)

    print(f"{'FIELD':<10} {'UPSTOX':<20} {'ANGELONE':<20}")
    for field in ("ltp", "atp", "oi", "volume", "bid", "ask"):
        print(f"{field:<10} {str(up_quote.get(field, '<missing>')):<20} {str(ao_quote.get(field, '<missing>')):<20}")

    print(f"\nUpstox raw fields available: {up_quote.get('raw_keys')}")
    print(f"AngelOne raw fields available: {ao_quote.get('raw_keys')}")

    if "error" in up_quote:
        print(f"\nUpstox error: {up_quote['error']}")
    if "error" in ao_quote:
        print(f"\nAngelOne error: {ao_quote['error']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
