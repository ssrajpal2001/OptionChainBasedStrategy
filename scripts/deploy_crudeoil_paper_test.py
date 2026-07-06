"""
Deploy a CRUDEOIL trap-scanner paper-test with small timeframes.

Usage:
  python scripts/deploy_crudeoil_paper_test.py

This script:
  1. Updates trap_scanner admin config to give CRUDEOIL:
       HTF=5m, LTF/execution=1m, SL buffer=20 pts
  2. Creates a trap_scanner deployment for CRUDEOIL on the first
     active client + trade-enabled binding.
  3. Sets the deployment is_running=1.

Then run the system in paper mode:
  python run_system.py --mode paper --index CRUDEOIL --strategies trap_scanner
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from data_layer.client_db import ClientDB


CRUDEOIL_CONFIG = {
    "htf_minutes": 5,
    "ltf_minutes": 1,
    "sl_buffer": 20.0,
    "entry_cutoff": "23:15",
    "sq_off_time": "23:25",
    "lot_size": 100,
}


async def main():
    db = ClientDB()
    await db.initialise()

    clients = [c for c in db.get_all_clients_sync() if int(c.get("is_active", 0) or 0) == 1]
    if not clients:
        print("ERROR: no active clients found.")
        return 1

    # Use the first active client with at least one trade-enabled binding.
    target = None
    for c in clients:
        cid = c["client_id"]
        bindings = db.get_bindings_safe_sync(cid)
        for b in bindings:
            if b.get("is_trade_enabled"):
                target = (cid, b["binding_id"])
                break
        if target:
            break

    if not target:
        print("ERROR: no trade-enabled binding found.")
        return 1

    cid, bid = target
    print(f"Deploying CRUDEOIL test for client={cid} binding={bid}")

    # Merge CRUDEOIL overrides into existing trap_scanner admin config.
    raw = db.get_setting_sync("trap_scanner", "{}")
    try:
        cfg = json.loads(raw) if raw else {}
    except Exception:
        cfg = {}
    cfg.setdefault("per_index", {})
    cfg["per_index"]["CRUDEOIL"] = CRUDEOIL_CONFIG
    await db.set_setting("trap_scanner", json.dumps(cfg))
    print("Updated trap_scanner admin config for CRUDEOIL:")
    for k, v in CRUDEOIL_CONFIG.items():
        print(f"  {k}={v}")

    # Create/start deployment.
    deploy_id = f"{cid}_{bid}_trap_scanner_CRUDEOIL"
    await db.save_deployment(
        client_id=cid,
        binding_id=bid,
        strategy_name="trap_scanner",
        underlying="CRUDEOIL",
        lot_multiplier=1,
        max_profit_rs=0.0,
        max_sl_rs=0.0,
        squareoff_time="23:25",
    )
    await db.set_deployment_running(deploy_id, cid, True)
    print(f"Deployment started: {deploy_id}")
    print("\nRun paper test with:")
    print("  python run_system.py --mode paper --index CRUDEOIL --strategies trap_scanner")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
