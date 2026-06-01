"""
tools/setup_crudeoil_strategy.py — make CRUDEOIL use the SAME strategy rules as
NIFTY, with only the session TIMES changed for the MCX evening session.

Sell straddle: full NIFTY rule set (entry/re-entry/exit rules + all risk
management) but entry_start 09:00, entry_end 23:15 (no new trade after 23:15),
squareoff 23:30. Iron condor: crude strikes/lot, times 09:00 / 23:30.

Run on EC2:  python3 tools/setup_crudeoil_strategy.py
Then:        pm2 restart terminus
"""
from __future__ import annotations

import copy
import json
import os

PATH = os.path.join("data", "strategy_config.json")

# MCX session times — the ONLY thing that differs from NIFTY for sell straddle.
SS_TIMES = {"entry_start": "09:00", "entry_end": "23:15", "squareoff_time": "23:30"}


def main():
    if not os.path.exists(PATH):
        print(f"ERROR: {PATH} not found — run from the project root.")
        return
    with open(PATH) as f:
        cfg = json.load(f)

    indices = cfg.setdefault("indices", {})
    nifty = indices.get("NIFTY", {})
    if "sell_straddle" not in nifty:
        print("ERROR: NIFTY.sell_straddle not found — nothing to clone.")
        return

    # 1) Clone NIFTY sell_straddle wholesale, then override only the times.
    crude_ss = copy.deepcopy(nifty["sell_straddle"])
    crude_ss.update(SS_TIMES)

    crude = indices.setdefault("CRUDEOIL", {})
    crude["sell_straddle"] = crude_ss

    # 2) Iron condor — crude contract specifics + MCX times.
    crude["iron_condor"] = {
        "enabled": True,
        "start_time": "09:00",
        "squareoff_time": "23:30",
        "entry_day": "daily",
        "product_type": "MIS",
        "lot_size": 100,
        "strike_step": 100,
        "max_adjustments_per_side": 3,
        "roll_step_pts": 5,
        "profit_target_inr": 5000.0,
        "stoploss_inr": 2000.0,
        "ratio_exit_threshold": 3.0,
        "short_leg_otm_pts": 100.0,
        "long_leg_otm_pts": 200.0,
        "ratio_trigger": 2,
        "lot_multiplier": 1.0,
    }

    # 3) Trap — MCX squareoff.
    crude.setdefault("trap_trading", {})
    crude["trap_trading"].update({"squareoff_time": "23:30", "lot_multiplier": 1.0})

    with open(PATH, "w") as f:
        json.dump(cfg, f, indent=2)

    ss = crude["sell_straddle"]
    print("CRUDEOIL configured from NIFTY rules.")
    print(f"  sell_straddle: {ss['entry_start']} -> {ss['entry_end']} squareoff {ss['squareoff_time']}")
    print(f"  entry_rules_beginning: {len(ss.get('entry_rules_beginning', []))} rules")
    print(f"  entry_rules_reentry:   {len(ss.get('entry_rules_reentry', []))} rules")
    print(f"  exit_rules:            {len(ss.get('exit_rules', []))} rules")
    print(f"  iron_condor: step 100, lot 100, squareoff 23:30")


if __name__ == "__main__":
    main()
