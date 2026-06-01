"""
tools/add_crudeoil_config.py — inject a CRUDEOIL section into the runtime
data/strategy_config.json with all three strategies configured for the MCX
evening session.

Sell-straddle: start 09:00, NO new trade after 23:15, square off 23:30.
Run on EC2:  python3 tools/add_crudeoil_config.py
Then restart: pm2 restart terminus
"""
from __future__ import annotations

import json
import os

PATH = os.path.join("data", "strategy_config.json")

CRUDEOIL = {
    "sell_straddle": {
        "entry_start": "09:00",
        "entry_end": "23:15",          # no NEW trade after 23:15
        "squareoff_time": "23:30",     # force square-off at MCX close
        "entry_workflow_mode": "hybrid",
        "ltp_target": 0,
        "trail_lock_pct": 20.0,
        "trail_floor_pct": 10.0,
        "entry_rules_beginning": [],   # empty = enter on schedule (no indicator gate) for the live test
        "entry_rules_reentry": [],
        "exit_rules": [],
        "profit_target_enabled": True,
        "profit_pct": 30.0,
        "sl_enabled": True,
        "sl_pct": 200.0,
        "profit_target_pct": 0.0,
        "loss_sl_pct": 0.0,
        "tsl_enabled": False,
        "ratio_exit": {"enabled": False, "threshold": 3.0},
        "ltp_decay": {"enabled": False, "ltp_exit_min": 20.0},
        "smart_rolling_enabled": False,
        "vwap_rise_sl": {"enabled": False, "tf": 1, "threshold": 1.0},
        "sl_cooldown_tf_multiplier": 1.0,
        "capital_deployed_inr": 0,
        "max_trades": 5,
        "per_day": {d: {"enabled": False, "profit_target_pct": 0.0, "loss_sl_pct": 0.0}
                    for d in ("monday", "tuesday", "wednesday", "thursday", "friday")},
    },
    "iron_condor": {
        "enabled": True,
        "start_time": "09:00",
        "squareoff_time": "23:30",
        "entry_day": "daily",
        "product_type": "MIS",
        "lot_size": 100,               # MCX crude lot
        "strike_step": 50,
        "max_adjustments_per_side": 3,
        "roll_step_pts": 5,
        "profit_target_inr": 5000,
        "stoploss_inr": 2000,
        "ratio_exit_threshold": 3,
        "short_leg_otm_pts": 100,      # crude strikes are 50 apart
        "long_leg_otm_pts": 200,
        "ratio_trigger": 2,
    },
}


def main():
    if not os.path.exists(PATH):
        print(f"ERROR: {PATH} not found — run from the project root.")
        return
    with open(PATH) as f:
        cfg = json.load(f)
    cfg.setdefault("indices", {})
    cfg["indices"]["CRUDEOIL"] = CRUDEOIL
    with open(PATH, "w") as f:
        json.dump(cfg, f, indent=2)
    ss = cfg["indices"]["CRUDEOIL"]["sell_straddle"]
    print("CRUDEOIL added. sell_straddle:",
          ss["entry_start"], "->", ss["entry_end"], "squareoff", ss["squareoff_time"])


if __name__ == "__main__":
    main()
