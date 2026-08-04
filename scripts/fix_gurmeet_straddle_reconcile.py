"""
One-off reconciliation: gurmeet's live SellStraddle NIFTY position drifted
from reality on 2026-08-04. The strategy believed it had rolled CE24650 ->
CE24400 at 11:57:05 (a real fill event WAS received and processed), but
the underlying broker order never actually reached Zerodha (broker object
was transiently unavailable in the execution router at that exact moment,
silently falling back to a local paper-simulated fill instead of a real
one -- confirmed via logs: routing showed mode=live, but the fill was
tagged [PAPER]). Confirmed directly against Zerodha's own positions page:
the real position is still CE24650 + PE24600 (the original pair), not
CE24400 + PE24600.

This reverts the persisted position file back to the ORIGINAL pair/prices/
credit, and resets TSL/peak-tracking state (which was computed against the
wrong, fictional CE24400 leg and is meaningless for the real position) back
to a fresh baseline -- so the strategy resumes managing the REAL position
with a clean starting state instead of corrupted TSL history.

Run on EC2 from the repo root: python3 scripts/fix_gurmeet_straddle_reconcile.py
Prints the before/after diff and asks for confirmation before writing.
After it saves, `pm2 restart terminus` to reload the corrected position.
"""
import json
import sys

KEY = "gurmeet_zerodha_NIFTY_sell_straddle"
PATH = f"data/positions/{KEY}.json"

# Original entry, confirmed from the 09:22:05 real [LIVE] fill in gurmeet's trade log.
ORIGINAL_CE = {"option_type": "CE", "strike": 24650.0, "entry_price": 126.45}
ORIGINAL_PE_ENTRY_PRICE = 130.55  # unchanged by the failed roll -- for net_credit recompute only

def main():
    try:
        with open(PATH) as f:
            payload = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {PATH} not found -- has the file path/key changed? Aborting, no changes made.")
        sys.exit(1)

    pos = payload.get("position", {})
    print("=== CURRENT (before) ===")
    print(json.dumps(pos, indent=2))

    ce = pos.get("ce_leg", {})
    old_open_time = ce.get("open_time")  # preserve -- CE24650 was never actually re-opened

    ce["strike"] = ORIGINAL_CE["strike"]
    ce["entry_price"] = ORIGINAL_CE["entry_price"]
    ce["open_time"] = old_open_time
    ce["close_time"] = None
    ce["open_reason"] = "beginning"
    pos["ce_leg"] = ce

    pos["net_credit"] = ORIGINAL_CE["entry_price"] + ORIGINAL_PE_ENTRY_PRICE

    # Reset TSL/peak-tracking state -- all computed against the fictional CE24400 leg,
    # meaningless for the real (reverted) CE24650 position. Start clean from here.
    pos["tsl_high_lock_rs"] = 0.0
    pos["peak_profit"] = 0.0
    pos["trailing_active"] = False
    pos["trail_peak_pct"] = 0.0
    pos["session_min_vwap"] = float("inf")
    pos["vwap_last_good"] = 0.0
    pos["realized_pnl"] = 0.0  # undo the phantom +28.30pt booked from the fake roll-close

    print("\n=== PROPOSED (after) ===")
    print(json.dumps(pos, indent=2))

    ans = input("\nWrite this correction to disk? [y/N]: ").strip().lower()
    if ans != "y":
        print("Aborted -- no changes made.")
        return

    payload["position"] = pos
    with open(PATH, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"\nSaved. Now run: pm2 restart terminus")

if __name__ == "__main__":
    main()
