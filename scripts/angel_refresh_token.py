"""
AngelOne fresh token generator + save to DB.
Run on EC2 every morning before starting the bot.

Usage: python scripts/angel_refresh_token.py
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB

async def main():
    db = ClientDB("data/clients.db")
    bindings = await asyncio.to_thread(db.get_bindings_sync, "ssrajpal2001")
    angel = next((b for b in bindings if "angel" in b.get("provider","").lower()), None)
    if not angel:
        print("ERROR: No AngelOne binding found")
        return

    user_id    = angel.get("user_id", "")
    api_key    = angel.get("api_key", "")
    password   = angel.get("api_secret", "")   # AngelOne: api_secret = login password
    totp_key   = angel.get("totp_secret", "")  # raw TOTP secret

    print(f"binding_id : {angel.get('binding_id')}")
    print(f"user_id    : {user_id}")
    print(f"api_key    : {api_key[:8]}...")

    try:
        import pyotp
        from SmartApi import SmartConnect
    except ImportError as e:
        print(f"ERROR: missing package — {e}")
        print("Run: pip install smartapi-python pyotp")
        return

    totp  = pyotp.TOTP(totp_key.strip().replace(" ","").replace("-","").upper()).now()
    smart = SmartConnect(api_key=api_key)

    print(f"Generating session (TOTP={totp})...")
    data = smart.generateSession(user_id, password, totp)
    if not (data and data.get("status")):
        print(f"FAILED: {data}")
        return

    token = smart.access_token
    print(f"Token obtained: {token[:30]}...")

    # Save to DB
    from datetime import datetime, timezone, timedelta
    now  = datetime.now(timezone.utc).isoformat()
    exp  = (datetime.now(timezone.utc) + timedelta(hours=20)).isoformat()
    await db.update_access_token("ssrajpal2001", angel["binding_id"], token, now, exp)
    print("Token saved to DB — AngelOne binding is ready for today.")

asyncio.run(main())
