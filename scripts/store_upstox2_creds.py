"""
One-shot script: store Upstox-2 (CrudeOil dedicated) API key + secret in DB.
Run once from EC2:
  python scripts/store_upstox2_creds.py

After running, go to the dashboard → Feeder Status → Upstox2 → Login to get a fresh token.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_layer.client_db import ClientDB

API_KEY    = "90452d02-93c8-496e-962c-711e1b7b10d4"
API_SECRET = "m2y1a937te"
PROVIDER   = "upstox2"


async def main():
    db = ClientDB("data/clients.db")
    await db.upsert_feeder_creds(
        provider  = PROVIDER,
        api_key   = API_KEY,
        secret    = API_SECRET,
        client_id = "",
    )
    creds = db.get_feeder_creds_sync(PROVIDER)
    print(f"Stored {PROVIDER} creds:")
    print(f"  api_key : {creds.get('api_key','')[:8]}...")
    print(f"  secret  : {creds.get('secret','')[:4]}...")
    print()
    print("Next step: open the dashboard → Data Feeder panel → Upstox2 → click 'Login'")
    print("That will generate an access_token valid for today's CrudeOil session.")


asyncio.run(main())
