"""One-shot check: are the stored Upstox/Fyers SYSTEM FEEDER tokens actually
valid right now? Hits each broker's own lightweight profile/REST endpoint
directly -- independent of the main app's WebSocket connection state, so
this gives a clean yes/no without needing to restart or touch terminus.

Must be run ON THE SERVER (data/clients.db lives there).

Usage: python scripts/check_feeder_tokens.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB


def check_upstox() -> None:
    print("=" * 60)
    print("UPSTOX")
    print("=" * 60)
    creds = ClientDB().get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("No stored access_token for upstox feeder creds -- cannot check.")
        return
    from curl_cffi import requests as cc
    r = cc.get(
        "https://api.upstox.com/v2/user/profile",
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
        impersonate="chrome131", timeout=10,
    )
    print(f"HTTP {r.status_code}")
    print(r.text[:500])
    if r.status_code == 200:
        print("\n>>> UPSTOX TOKEN IS VALID")
    else:
        print("\n>>> UPSTOX TOKEN IS INVALID / EXPIRED")


def check_fyers() -> None:
    print("\n" + "=" * 60)
    print("FYERS")
    print("=" * 60)
    creds = ClientDB().get_feeder_creds_sync("fyers")
    token = (creds or {}).get("access_token", "")
    api_key = (creds or {}).get("api_key", "")
    if not token:
        print("No stored access_token for fyers feeder creds -- cannot check.")
        return
    # Fyers v3 REST wants "appId:jwt" in the Authorization header. Strip any
    # existing "appId:" prefix first (same normalization the live feeder
    # itself does, see FyersFeeder._normalize_access_token), then re-attach
    # the real api_key so this works regardless of how the token was stored.
    jwt = token
    if ":" in token:
        parts = token.split(":", 1)
        if "." in parts[1]:
            jwt = parts[1]
    auth_header = f"{api_key}:{jwt}" if api_key else token

    # Fyers v3 trading/profile APIs live on api-t1.fyers.in, NOT api.fyers.in
    # (that's the data-only host) -- the first attempt's 500 "please provide
    # valid method" was a malformed-request error from hitting the wrong
    # host/path, not a real auth failure (Fyers' genuine invalid-token
    # response looks completely different -- s":"error" with a clear
    # "Invalid authorization token" message and a negative error code).
    from curl_cffi import requests as cc
    r = cc.get(
        "https://api-t1.fyers.in/api/v3/profile",
        headers={"Accept": "application/json", "Authorization": auth_header},
        impersonate="chrome131", timeout=10,
    )
    print(f"HTTP {r.status_code}")
    print(r.text[:500])
    ok = r.status_code == 200
    try:
        ok = ok and r.json().get("s") == "ok"
    except Exception:
        pass
    if ok:
        print("\n>>> FYERS TOKEN IS VALID")
    else:
        print("\n>>> FYERS TOKEN IS INVALID / EXPIRED (or still a request-format issue -- check the raw response above)")


if __name__ == "__main__":
    check_upstox()
    check_fyers()
