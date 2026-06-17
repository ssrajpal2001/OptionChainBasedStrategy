"""Quick check: what expiry does REGISTRY have for SENSEX?"""
import sys, os
sys.path.insert(0, os.getcwd())

from data_layer.instrument_registry import REGISTRY
from datetime import date

try:
    from data_layer.client_db import ClientDB
    db = ClientDB("data/clients.db")
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token") or ""
except Exception as e:
    print(f"DB error: {e}")
    token = ""

if not token:
    print("No Upstox token found in DB")
    sys.exit(1)

print(f"Token: {token[:20]}...")
print("\nLoading SENSEX contracts from BSE master...")
REGISTRY.load_sync("SENSEX", token)
print(f"is_loaded: {REGISTRY.is_loaded('SENSEX')}")

expiries = REGISTRY.all_expiries("SENSEX")
print(f"All expiries for SENSEX: {expiries}")

nearest = REGISTRY.get_active_expiry("SENSEX")
print(f"Nearest active expiry: {nearest}")

if nearest:
    print("\nStrike lookup around 77000:")
    for strike in [76500, 76800, 77000, 77100, 77400, 77700]:
        for ot in ["CE", "PE"]:
            key = REGISTRY.get_upstox_key("SENSEX", nearest, strike, ot)
            status = key if key else "NOT FOUND"
            print(f"  SENSEX {strike}{ot} exp={nearest} → {status}")
