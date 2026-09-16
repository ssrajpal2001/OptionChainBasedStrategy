"""
scripts/oi_orb_feeder_credential_check.py

Direct user follow-up, 2026-09-16, after hitting a sustained Upstox 429:
"check if fyers and angel r working, if token is expired will get teh
token again and start teh backtest."

IMPORTANT distinction checked here: a 429 (Too Many Requests) is NOT the
same problem as an expired/invalid token (401/403) -- a fresh token from
the same account/IP would still be rate-limited, re-authenticating would
NOT fix a genuine rate limit. This script makes ONE lightweight real API
call per provider (not a historical-candle range fetch, to avoid eating
further into that specific rate-limited budget) to directly distinguish:
  - token missing entirely (never configured)
  - token present but rejected (401/403 -- genuinely expired/invalid,
    re-auth via broker_auth/headless_auth.py IS the real fix)
  - token present and rate-limited RIGHT NOW (429 -- re-auth would NOT
    help, only waiting/pacing does)
  - token present and genuinely working (200 -- real data returned)

Checks all 4: upstox, upstox2, fyers, angelone. Read-only.

MUST run on EC2 (reads real data/clients.db feeder_creds).

Usage: python scripts/oi_orb_feeder_credential_check.py
"""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, ".")

from data_layer.client_db import ClientDB


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
        return f"{account}: NO credentials/access_token configured at all"
    token = creds["access_token"]
    expiry_note = _expiry_note(creds)
    from curl_cffi import requests as _cc
    try:
        resp = _cc.get(
            "https://api.upstox.com/v2/user/profile",
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            impersonate="chrome131", timeout=8,
        )
        if resp.status_code == 200:
            body = resp.json()
            user = (body.get("data") or {}).get("user_name", "?")
            return f"{account}: OK (200) -- token valid, real user={user} ({expiry_note})"
        if resp.status_code == 429:
            return f"{account}: RATE LIMITED (429) -- token itself is fine, this is NOT a token problem, re-auth won't help ({expiry_note})"
        if resp.status_code in (401, 403):
            return f"{account}: EXPIRED/INVALID ({resp.status_code}) -- real re-auth needed ({expiry_note})"
        return f"{account}: unexpected status={resp.status_code}, body={resp.text[:200]} ({expiry_note})"
    except Exception as exc:
        return f"{account}: real request failed -- {exc} ({expiry_note})"


async def _check_fyers():
    creds = ClientDB().get_feeder_creds_sync("fyers")
    if not creds or not creds.get("access_token"):
        return "fyers: NO credentials/access_token configured at all"
    expiry_note = _expiry_note(creds)
    try:
        from fyers_apiv3 import fyersModel
        client_id = creds.get("client_id") or ""
        fy = fyersModel.FyersModel(client_id=client_id, token=creds["access_token"], is_async=False, log_path="")
        resp = await asyncio.to_thread(fy.get_profile)
        if resp.get("s") == "ok":
            name = (resp.get("data") or {}).get("name", "?")
            return f"fyers: OK -- token valid, real user={name} ({expiry_note})"
        return f"fyers: token rejected -- {resp} ({expiry_note})"
    except Exception as exc:
        return f"fyers: real request failed -- {exc} ({expiry_note})"


async def _check_angelone():
    creds = ClientDB().get_feeder_creds_sync("angelone")
    if not creds or not creds.get("access_token"):
        return "angelone: NO credentials/access_token configured at all"
    expiry_note = _expiry_note(creds)
    try:
        from curl_cffi import requests as _cc
        resp = await asyncio.to_thread(
            _cc.get,
            "https://apiconnect.angelbroking.com/rest/secure/angelbroking/user/v1/getProfile",
            headers={
                "Accept": "application/json", "Authorization": f"Bearer {creds['access_token']}",
                "X-PrivateKey": creds.get("api_key", ""), "X-SourceID": "WEB",
                "X-ClientLocalIP": "127.0.0.1", "X-ClientPublicIP": "127.0.0.1", "X-MACAddress": "00:00:00:00:00:00",
            }, timeout=8,
        )
        if resp.status_code == 200:
            body = resp.json()
            if body.get("status"):
                return f"angelone: OK -- token valid, real user={body.get('data', {}).get('name', '?')} ({expiry_note})"
            return f"angelone: token rejected -- {body.get('message')} ({expiry_note})"
        return f"angelone: unexpected status={resp.status_code} ({expiry_note})"
    except Exception as exc:
        return f"angelone: real request failed -- {exc} ({expiry_note})"


async def main():
    print("=" * 110)
    print("Real feeder credential check -- upstox / upstox2 / fyers / angelone")
    print("429 = rate limited (token is fine, re-auth won't help). 401/403 = genuinely expired (re-auth needed).")
    print("=" * 110)
    results = await asyncio.gather(
        _check_upstox("upstox"), _check_upstox("upstox2"), _check_fyers(), _check_angelone(),
        return_exceptions=True,
    )
    for r in results:
        print(f"  {r if isinstance(r, str) else f'EXCEPTION: {r}'}")
    print("=" * 110)


if __name__ == "__main__":
    asyncio.run(main())
