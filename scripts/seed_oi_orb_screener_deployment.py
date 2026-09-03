"""
scripts/seed_oi_orb_screener_deployment.py -- one-off deployment-row seed
for the new OI-Spurt + ORB screener strategy (strategies/oi_orb_screener/).

`data/*.db` is gitignored (see .gitignore) -- this repo's local dev copy of
data/clients.db is NOT the same file the EC2 deployment reads, so a
strategy_deployments row inserted locally never reaches production. Run
this ONCE on the EC2 server itself, after `git pull`, before restarting
pm2 -- same one-off-seed pattern OI-Flow's/Liquidity Sweep's own first
paper deployments used before either had a dashboard deploy form.

Usage (on EC2, from the repo root):
    python scripts/seed_oi_orb_screener_deployment.py
    python scripts/seed_oi_orb_screener_deployment.py --test-now

Seeds exactly one row: client_id=ssrajpal2001, binding_id=SA5770 (already
confirmed trading_mode='paper_route' on that binding -- see
strategies/oi_orb_screener/__init__.py's own docstring for the full
context), strategy_name=oi_orb_screener, underlying=SCREENER (sentinel --
see book_manager.py), is_running set True via set_deployment_running()
right after the upsert (save_deployment() itself does not set is_running).

--test-now sets strategy_params.ignore_time_windows=true (see engine.py's
own IGNORE_TIME_WINDOWS docstring) -- lets the book run RIGHT NOW instead of
only 09:30-10:30 IST, so you can verify order placement + LTP subscription
outside the real entry window. save_deployment() upserts on deploy_id, so
re-running this script WITHOUT --test-now later flips it back off (no
manual SQL needed) -- do that once connectivity is confirmed.
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
STRATEGY_NAME = "oi_orb_screener"
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
        max_profit_rs=0,      # no per-day guardrail wired for this strategy this pass
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
        print("TEST MODE: ignore_time_windows=true -- book runs right now, ORB freezes "
              "immediately, no 09:30-10:30 gate. Re-run WITHOUT --test-now once confirmed "
              "working to turn real window timing back on.")
    print("Restart pm2 (with oi_orb_screener added to --strategies) for it to pick this up.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-now", action="store_true",
                         help="Bypass ORB/entry-window timing so the book runs immediately, "
                              "regardless of current time. Temporary -- see module docstring.")
    args = parser.parse_args()
    asyncio.run(main(args.test_now))
