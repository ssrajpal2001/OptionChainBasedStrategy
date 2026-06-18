"""
Test AngelOne token validity — places NO order.
Just calls getProfile() to verify token is working.
Run: python3 scripts/test_angel_token.py
"""
import sys, os, asyncio
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_layer.client_db import ClientDB

async def main():
    db = ClientDB("data/clients.db")
    bindings = db.get_bindings_sync("ssrajpal2001")
    angel = next((b for b in bindings if "angel" in b.get("provider","").lower()), None)
    if not angel:
        print("Available bindings:", [b.get("binding_id") for b in bindings])
        print("ERROR: No AngelOne binding found. Check provider field.")
        return

    access_token = angel.get("access_token", "")
    api_key      = angel.get("api_key", "")
    client_code  = angel.get("client_code") or angel.get("user_id", "")
    password     = angel.get("password", "")
    totp_secret  = angel.get("totp_secret", "")

    print(f"API Key     : {api_key[:8]}...")
    print(f"Access Token: {'SET ('+str(len(access_token))+' chars)' if access_token else 'NOT SET'}")
    print(f"Client Code : {client_code}")
    print()

    try:
        from SmartApi import SmartConnect
        import pyotp
        smart = SmartConnect(api_key=api_key)

        if access_token:
            print("Testing stored access_token via getProfile()...")
            smart.access_token = access_token
        else:
            print("No stored token — doing fresh headless auth...")
            totp = pyotp.TOTP(totp_secret).now() if totp_secret else ""
            data = smart.generateSession(client_code, password, totp)
            if not (data and data.get("status")):
                print(f"FAILED headless auth: {data}")
                return
            print(f"Headless auth OK — token: {smart.access_token[:20]}...")

        profile = smart.getProfile(smart.refreshToken if hasattr(smart,'refreshToken') else "")
        if profile and profile.get("status"):
            p = profile.get("data", {})
            print(f"✓ TOKEN VALID")
            print(f"  Name    : {p.get('name','')}")
            print(f"  Email   : {p.get('email','')}")
            print(f"  Broker  : AngelOne")
            print(f"  Exchanges: {p.get('exchanges','')}")
        else:
            print(f"✗ TOKEN INVALID or EXPIRED: {profile}")

    except Exception as e:
        print(f"ERROR: {e}")

asyncio.run(main())
