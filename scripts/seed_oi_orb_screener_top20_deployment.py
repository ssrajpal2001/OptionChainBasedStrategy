"""
scripts/seed_oi_orb_screener_top20_deployment.py -- one-off deployment-row
seed for the new "oi_orb_screener_top20" sibling strategy
(strategies/oi_orb_screener/, strategy_name="oi_orb_screener_top20").

Mirrors scripts/seed_oi_orb_screener_deployment.py's own precedent exactly --
data/*.db is gitignored, so a row inserted locally never reaches production.
Run this ONCE on the EC2 server itself, after `git pull`, before restarting
pm2.

Usage (on EC2, from the repo root):
    python scripts/seed_oi_orb_screener_top20_deployment.py
    python scripts/seed_oi_orb_screener_top20_deployment.py --test-now

Seeds exactly one row: client_id=ssrajpal2001, binding_id=SA5770,
strategy_name=oi_orb_screener_top20, underlying=SCREENER (same sentinel the
standard oi_orb_screener variant uses -- these are two separate deployment
rows, keyed by strategy_name, so both can run side-by-side on the same
binding without colliding).

--test-now sets strategy_params.ignore_time_windows=true so the book runs
immediately instead of waiting for the real 09:26-15:00 entry window --
useful for a same-day connectivity check outside market hours. Re-run
WITHOUT --test-now once confirmed to flip it back off (save_deployment()
upserts on deploy_id, no manual SQL needed).
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data_layer.client_db import ClientDB

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "SA5770"
STRATEGY_NAME = "oi_orb_screener_top20"
UNDERLYING = "SCREENER"


async def main(test_now: bool) -> None:
    db = ClientDB()
    await db.initialise()

    strategy_params = json.dumps({"ignore_time_windows": True}) if test_now else "{}"

    deploy_id = await db.save_deployment(
        client_id=CLIENT_ID,
        binding_id=BINDING_ID,
        strategy_name=STRATEGY_NAME,
        underlying=UNDERLYING,
        lot_multiplier=1,
        max_profit_rs=0,
        max_sl_rs=0,
        squareoff_time="15:15",
        product_type="MIS",
        strategy_params=strategy_params,
        carry_forward=False,
    )
    await db.set_deployment_running(deploy_id, CLIENT_ID, True)
    print(f"Seeded and started deployment: {deploy_id}")
    print(f"strategy_params: {strategy_params}")
    if test_now:
        print("TEST MODE: ignore_time_windows=true -- book runs right now, no entry-window gate. "
              "Re-run WITHOUT --test-now once confirmed working to turn real window timing back on.")
    print("Restart pm2 (with oi_orb_screener_top20 added to --strategies) for it to pick this up.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-now", action="store_true",
                         help="Bypass entry-window timing so the book runs immediately, "
                              "regardless of current time. Temporary -- see module docstring.")
    args = parser.parse_args()
    asyncio.run(main(args.test_now))
