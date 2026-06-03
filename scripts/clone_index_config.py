#!/usr/bin/env python3
"""clone_index_config.py — copy one index's full strategy config onto another.

Use it to make CRUDEOIL inherit EVERYTHING from NIFTY (all entry/re-entry/exit rules,
ratio/decay/tsl/vwap-rise/per-day settings) and then apply only the MCX deltas
(session times + roundoff/strike-step/lot = 100).

Run on EC2 against your live file so it uses YOUR current NIFTY (no clobber):
    python3 scripts/clone_index_config.py                 # NIFTY -> CRUDEOIL (default)
    python3 scripts/clone_index_config.py --src NIFTY --dst CRUDEOIL
    python3 scripts/clone_index_config.py --dst NATURALGAS # NIFTY -> NATURALGAS

Then restart the app so the dashboard/strategies pick it up.
"""
from __future__ import annotations

import argparse
import copy
import json
import os

CFG = os.path.join("data", "strategy_config.json")

# MCX session + contract deltas applied to the cloned block.
MCX_SS = {"entry_start": "09:00", "entry_end": "23:15", "squareoff_time": "23:30"}
MCX_IC = {"start_time": "09:00", "squareoff_time": "23:30", "entry_day": "daily",
          "product_type": "MIS", "strike_step": 100, "lot_size": 100, "min_ltp": 50}
MCX_TT = {"roundoff_step": 100}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="NIFTY", help="source index to copy FROM")
    ap.add_argument("--dst", default="CRUDEOIL", help="target index to copy TO")
    ap.add_argument("--file", default=CFG, help="path to strategy_config.json")
    args = ap.parse_args()

    with open(args.file) as f:
        cfg = json.load(f)

    indices = cfg.setdefault("indices", {})
    if args.src not in indices:
        raise SystemExit(f"Source index '{args.src}' not found in {args.file}. "
                         f"Available: {list(indices)}")

    block = copy.deepcopy(indices[args.src])

    # Apply MCX deltas (only when the corresponding strategy block exists).
    if "sell_straddle" in block:
        block["sell_straddle"].update(MCX_SS)
    if "iron_condor" in block:
        block["iron_condor"].update(MCX_IC)
    block.setdefault("trap_trading", {}).update(MCX_TT)

    existed = args.dst in indices
    indices[args.dst] = block

    tmp = args.file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, args.file)

    print(f"{'Updated' if existed else 'Created'} indices.{args.dst} "
          f"as a clone of indices.{args.src} + MCX deltas.")
    ss = block.get("sell_straddle", {})
    print(f"  sell_straddle: start={ss.get('entry_start')} end={ss.get('entry_end')} "
          f"squareoff={ss.get('squareoff_time')} "
          f"beginning_rules={len(ss.get('entry_rules_beginning', []))} "
          f"reentry_rules={len(ss.get('entry_rules_reentry', []))} "
          f"exit_rules={len(ss.get('exit_rules', []))} ltp_target={ss.get('ltp_target')}")
    ic = block.get("iron_condor", {})
    print(f"  iron_condor:   step={ic.get('strike_step')} lot={ic.get('lot_size')} "
          f"product={ic.get('product_type')} short/long={ic.get('short_leg_otm_pts')}/"
          f"{ic.get('long_leg_otm_pts')}")
    print("Restart the app for changes to take effect.")


if __name__ == "__main__":
    main()
