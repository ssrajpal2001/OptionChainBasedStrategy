"""
tools/clone_nifty_straddle_to_crudeoil.py

Clone NIFTY's sell_straddle config (all enabled entry + exit conditions) into
CRUDEOIL, so CRUDEOIL trades with the SAME enabled exits as NIFTY. Only the MCX
session timing is overridden (CRUDEOIL trades the evening session).

Strike step and lot size are NOT in this config — they come from ExchangeConfig
(CRUDEOIL step=100, lot=100), so cloning is safe.

Run on EC2 from the project root:
    python tools/clone_nifty_straddle_to_crudeoil.py
then restart:  pm2 restart all
"""

import copy
import json
import os
import sys

PATH = os.path.join(os.path.dirname(__file__), "..", "data", "strategy_config.json")

# MCX session timing (kept; everything else mirrors NIFTY)
MCX_TIMING = {"entry_start": "09:00", "entry_end": "23:15", "squareoff_time": "23:30"}


def main() -> int:
    path = os.path.abspath(PATH)
    if not os.path.exists(path):
        print(f"ERROR: {path} not found.")
        return 1
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    indices = cfg.setdefault("indices", {})
    nifty_ss = (indices.get("NIFTY") or {}).get("sell_straddle")
    if not nifty_ss:
        print("ERROR: indices.NIFTY.sell_straddle not found — nothing to clone.")
        return 1

    new_ss = copy.deepcopy(nifty_ss)
    new_ss.update(MCX_TIMING)   # keep MCX session, mirror everything else

    crude = indices.setdefault("CRUDEOIL", {})
    crude["sell_straddle"] = new_ss

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

    print("Cloned NIFTY sell_straddle -> CRUDEOIL.")
    print(f"  timing (MCX kept): {new_ss['entry_start']} / {new_ss['entry_end']} / {new_ss['squareoff_time']}")
    print("  enabled conditions now on CRUDEOIL:")
    for k in ("profit_pct", "sl_pct", "ratio_exit", "ltp_decay", "tsl_scalable",
              "guardrail_roc", "guardrail_pnl", "vwap_rise_sl",
              "entry_rules_beginning", "entry_rules_reentry", "exit_rules"):
        v = new_ss.get(k)
        if isinstance(v, list):
            print(f"    {k}: {len(v)} rule(s)")
        else:
            print(f"    {k}: {v}")
    print("\nNow restart:  pm2 restart all")
    return 0


if __name__ == "__main__":
    sys.exit(main())
