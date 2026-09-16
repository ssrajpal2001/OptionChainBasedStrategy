"""
scripts/oi_orb_feeder_credential_check.py

Direct user spec, 2026-09-16: "i want u to provide me code that will
check all 4 data feeders where is the issue." Rewritten after the first
version's Fyers check used the wrong field (creds["client_id"] instead
of creds["api_key"] as FyersModel's client_id param -- confirmed wrong
against the REAL working pattern in execution_bridge/broker_fyers.py)
and AngelOne's check assumed a static access_token the same way Upstox/
Fyers have -- AngelOne genuinely doesn't persist one; it re-
authenticates fresh via TOTP on every real connection
(data_layer/global_feeder.py's AngelOneFeeder.connect(), SmartConnect +
generateSession). Both fixed here to match those REAL, already-proven
patterns exactly, not reimplemented from a guess.

Checks, for each of the 4 real feeders:
  - upstox / upstox2: (a) /v2/user/profile (confirms the token itself
    authenticates), (b) a real 1-day historical-candle fetch for a
    known-liquid instrument (RELIANCE) -- confirmed real 2026-09-16
    incident: profile can be 200 while historical-candle is separately
    429'd, since brokers commonly rate-limit each endpoint CATEGORY
    independently, not the token as a whole. Distinguishes the two
    explicitly so "token valid" is never mistaken for "this endpoint
    has budget".
  - fyers: FyersModel(client_id=api_key, token=access_token).get_profile()
    -- the exact real construction broker_fyers.py already uses live.
  - angelone: full real headless re-auth (SmartConnect + generateSession
    with client_id/api_key/password/totp_secret via pyotp) -- the exact
    real flow AngelOneFeeder.connect() already uses live, since a
    stored access_token check alone can't tell you anything for this
    provider.

MUST run on EC2 (reads real data/clients.db feeder_creds, makes real
API calls).

Usage: python scripts/oi_orb_feeder_credential_check.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, timedelta

sys.path.insert(0, ".")

from data_layer.client_db import ClientDB

RELIANCE_EQ_KEY = "NSE_EQ|INE002A01018"   # known-liquid, always has real recent history


def _expiry_note(creds: dict) -> str:
    exp = creds.get("token_expiry_at") or ""
    gen = creds.get("token_generated_at") or ""
    if exp:
        return f"stored expiry={exp}"
    if gen:
        return f"no stored expiry, generated={gen}"
    return "no stored expiry/generated-at metadata"


async def _check_upstox(account: str):
    creds = ClientDB().get_feeder_creds_sync(account)
    if not creds or not creds.get("access_token"):
        return [f"{account}: NO credentials/access_token configured at all"]
    token = creds["access_token"]
    expiry_note = _expiry_note(creds)
    out = []
    from curl_cffi import requests as _cc

    # (a) profile -- confirms the token itself authenticates
    try:
        resp = await asyncio.to_thread(
            _cc.get, "https://api.upstox.com/v2/user/profile",
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            impersonate="chrome131", timeout=8,
        )
        if resp.status_code == 200:
            user = (resp.json().get("data") or {}).get("user_name", "?")
            out.append(f"{account} PROFILE: OK (200) -- token valid, real user={user} ({expiry_note})")
        elif resp.status_code == 429:
            out.append(f"{account} PROFILE: RATE LIMITED (429)")
        elif resp.status_code in (401, 403):
            out.append(f"{account} PROFILE: EXPIRED/INVALID ({resp.status_code}) -- real re-auth needed")
        else:
            out.append(f"{account} PROFILE: unexpected status={resp.status_code}")
    except Exception as exc:
        out.append(f"{account} PROFILE: real request failed -- {exc}")

    # (b) the ACTUAL endpoint category this session's backtests depend on --
    # a real 1-day historical-candle fetch, checked SEPARATELY from profile
    # since brokers commonly rate-limit each endpoint category on its own.
    try:
        d = date.today() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        from urllib.parse import quote as _q
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(RELIANCE_EQ_KEY, safe='')}/1minute/"
               f"{d.isoformat()}/{d.isoformat()}")
        resp = await asyncio.to_thread(
            _cc.get, url, headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            impersonate="chrome131", timeout=8,
        )
        if resp.status_code == 200:
            n = len((resp.json().get("data") or {}).get("candles", []) or [])
            out.append(f"{account} HISTORICAL-CANDLE: OK (200) -- {n} real candles for RELIANCE {d.isoformat()}")
        elif resp.status_code == 429:
            out.append(f"{account} HISTORICAL-CANDLE: RATE LIMITED (429) -- THIS is the endpoint every "
                       f"backtest script needs; separate budget from PROFILE above")
        elif resp.status_code in (401, 403):
            out.append(f"{account} HISTORICAL-CANDLE: EXPIRED/INVALID ({resp.status_code})")
        else:
            out.append(f"{account} HISTORICAL-CANDLE: unexpected status={resp.status_code}, "
                       f"body={resp.text[:150]}")
    except Exception as exc:
        out.append(f"{account} HISTORICAL-CANDLE: real request failed -- {exc}")
    return out


async def _check_fyers():
    creds = ClientDB().get_feeder_creds_sync("fyers")
    if not creds or not creds.get("access_token") or not creds.get("api_key"):
        return [f"fyers: NO complete credentials (access_token/api_key) configured"]
    expiry_note = _expiry_note(creds)
    try:
        from fyers_apiv3 import fyersModel
        # Matches execution_bridge/broker_fyers.py's own REAL, live-proven
        # construction exactly: client_id IS the stored api_key field, not
        # a separate "client_id" field.
        fy = fyersModel.FyersModel(client_id=creds["api_key"], token=creds["access_token"], log_path="logs/")
        resp = await asyncio.to_thread(fy.get_profile)
        if resp and resp.get("s") == "ok":
            name = (resp.get("data") or {}).get("name", "?")
            return [f"fyers: OK -- token valid, real user={name} ({expiry_note})"]
        return [f"fyers: token rejected -- {resp} ({expiry_note})"]
    except ImportError:
        return ["fyers: fyers-apiv3 not installed"]
    except Exception as exc:
        return [f"fyers: real request failed -- {exc} ({expiry_note})"]


async def _check_angelone():
    creds = ClientDB().get_feeder_creds_sync("angelone")
    if not creds:
        return ["angelone: NO credential row configured at all"]
    client_code = creds.get("client_id", "")
    api_key = creds.get("api_key", "")
    password = creds.get("password", "")
    totp_secret = creds.get("totp_secret", "")
    missing = [n for n, v in [("client_id", client_code), ("api_key", api_key),
                               ("password", password), ("totp_secret", totp_secret)] if not v]
    if missing:
        return [f"angelone: credential row exists but missing: {', '.join(missing)}"]
    try:
        from SmartApi import SmartConnect
        import pyotp
        # Matches data_layer/global_feeder.py's AngelOneFeeder.connect()
        # exactly -- AngelOne has no long-lived stored access_token to
        # check; it re-authenticates fresh via TOTP every real connection.
        smartapi = SmartConnect(api_key=api_key)
        totp_code = pyotp.TOTP(totp_secret).now()
        session = await asyncio.to_thread(smartapi.generateSession, client_code, password, totp_code)
        if session and session.get("status"):
            return ["angelone: OK -- real fresh headless auth succeeded (TOTP session generated)"]
        return [f"angelone: real headless auth FAILED -- {session}"]
    except ImportError:
        return ["angelone: SmartApi/pyotp not installed"]
    except Exception as exc:
        return [f"angelone: real request failed -- {exc}"]


async def main():
    print("=" * 120)
    print("Real feeder diagnostic -- upstox / upstox2 / fyers / angelone")
    print("PROFILE and HISTORICAL-CANDLE are checked SEPARATELY for Upstox -- a broker can rate-limit "
          "one endpoint category while the other is fine. 429=rate limited (token fine, wait). "
          "401/403=genuinely expired (re-auth needed).")
    print("=" * 120)
    results = await asyncio.gather(
        _check_upstox("upstox"), _check_upstox("upstox2"), _check_fyers(), _check_angelone(),
        return_exceptions=True,
    )
    for group in results:
        if isinstance(group, Exception):
            print(f"  EXCEPTION: {group}")
            continue
        for line in group:
            print(f"  {line}")
    print("=" * 120)


if __name__ == "__main__":
    asyncio.run(main())
