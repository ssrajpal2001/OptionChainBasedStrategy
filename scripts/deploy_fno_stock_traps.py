"""
Deploy live trap-scanner books for tomorrow's FNO stock watchlist.

Reads the latest nightly scan (data/fno_scan_YYYY-MM-DD.json) produced by
scripts/fno_stock_scanner.py and creates/starts one trap_scanner deployment
per selected stock for every active client × trade-enabled binding.

Usage:
  python scripts/deploy_fno_stock_traps.py
  python scripts/deploy_fno_stock_traps.py --max-per-side 3 --dry-run
  python scripts/deploy_fno_stock_traps.py --scan-file data/fno_scan_2026-07-03.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from data_layer.client_db import ClientDB


SCAN_DIR = os.path.join(_ROOT, "data")


def _latest_scan_file() -> Optional[str]:
    files = [
        os.path.join(SCAN_DIR, f)
        for f in os.listdir(SCAN_DIR)
        if f.startswith("fno_scan_") and f.endswith(".json")
    ]
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def _pick_stocks(scan: dict, max_per_side: int, min_rr: float) -> List[dict]:
    """Return merged list of top CE + top PE stocks sorted by R:R."""
    def _filter(items):
        return [
            s for s in items
            if float(s.get("rr_ratio", 0) or 0) >= min_rr
            and s.get("symbol")
        ]

    ce = sorted(_filter(scan.get("ce_stocks", [])), key=lambda s: s.get("rr_ratio", 0), reverse=True)
    pe = sorted(_filter(scan.get("pe_stocks", [])), key=lambda s: s.get("rr_ratio", 0), reverse=True)
    selected = ce[:max_per_side] + pe[:max_per_side]
    selected.sort(key=lambda s: s.get("rr_ratio", 0), reverse=True)
    return selected


async def main():
    parser = argparse.ArgumentParser(description="Deploy FNO stock trap-scanner books")
    parser.add_argument("--scan-file", help="Path to fno_scan_*.json (default: newest)")
    parser.add_argument("--max-per-side", type=int, default=5, help="Top N CE + Top N PE stocks")
    parser.add_argument("--min-rr", type=float, default=1.5, help="Minimum R:R to deploy")
    parser.add_argument("--lot-multiplier", type=int, default=2, help="Lot multiplier per deployment")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be deployed")
    args = parser.parse_args()

    scan_file = args.scan_file or _latest_scan_file()
    if not scan_file or not os.path.exists(scan_file):
        print("ERROR: no fno_scan_*.json found. Run scripts/fno_stock_scanner.py first.")
        return 1

    with open(scan_file, "r", encoding="utf-8") as f:
        scan = json.load(f)

    stocks = _pick_stocks(scan, args.max_per_side, args.min_rr)
    if not stocks:
        print(f"No qualifying stocks in {scan_file}")
        return 0

    print(f"Scan file: {scan_file}")
    print(f"Selected {len(stocks)} stock(s) for trap-scanner deployment:")
    for s in stocks:
        print(f"  {s['symbol']:15} {s['direction']:2}  R:R={s['rr_ratio']:<5}  "
              f"lot={s.get('lot_size','?')}  strike_step={s.get('strike_step','?')}  "
              f"touch={s.get('touch_status','?')}")

    db = ClientDB()
    await db.initialise()

    clients = [c for c in db.get_all_clients_sync() if int(c.get("is_active", 0) or 0) == 1]
    if not clients:
        print("ERROR: no active clients found.")
        return 1

    actions = []
    for client in clients:
        cid = client.get("client_id", "")
        bindings = db.get_bindings_safe_sync(cid)
        for b in bindings:
            bid = b.get("binding_id", "")
            if not b.get("is_trade_enabled"):
                continue
            for s in stocks:
                symbol = str(s["symbol"]).upper()
                deploy_id = f"{cid}_{bid}_trap_scanner_{symbol}"
                if args.dry_run:
                    actions.append(("WOULD_DEPLOY", deploy_id, cid, bid, symbol))
                    continue
                await db.save_deployment(
                    client_id=cid,
                    binding_id=bid,
                    strategy_name="trap_scanner",
                    underlying=symbol,
                    lot_multiplier=args.lot_multiplier,
                    max_profit_rs=0.0,
                    max_sl_rs=0.0,
                    squareoff_time="15:20",
                )
                await db.set_deployment_running(deploy_id, cid, True)
                actions.append(("DEPLOYED", deploy_id, cid, bid, symbol))

    if args.dry_run:
        print(f"\nDRY-RUN: would create {len(actions)} deployment(s)")
    else:
        print(f"\nCreated/updated {len(actions)} deployment(s)")
    for action, deploy_id, cid, bid, symbol in actions:
        print(f"  {action}: {deploy_id}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
