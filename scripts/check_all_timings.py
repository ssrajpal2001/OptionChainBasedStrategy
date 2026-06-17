"""
Show ALL strategy timing settings currently stored in DB.
Run on EC2:
  python3 scripts/check_all_timings.py
  python3 scripts/check_all_timings.py --apply   # write correct timings
"""
import sys, os, asyncio, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_layer.client_db import ClientDB

# ── DESIRED TIMINGS ───────────────────────────────────────────────────────────
# Trap Scanner
TRAP_TIMINGS = {
    "NIFTY":    {"entry_cutoff": "15:10", "sq_off_time": "15:20"},
    "SENSEX":   {"entry_cutoff": "15:20", "sq_off_time": "15:25"},
    "CRUDEOIL": {"entry_cutoff": "22:45", "sq_off_time": "23:00",
                 "entry_window": [[14, 30], [22, 45]]},
}

# Sell Straddle — per-day timing lives inside ss["per_day"] but
# the global start/cutoff fields are:
#   ss["start_time"]      → when engine begins looking for entry (HH:MM)
#   ss["squareoff_time"]  → EOD force-exit (HH:MM)
# These are per-INDEX keys inside ss["indices"][index] or top-level
# Check what structure your DB has by reading current first.
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    db = ClientDB("data/clients.db")
    apply = "--apply" in sys.argv

    for key in ["trap_scanner", "sell_straddle", "iron_condor"]:
        raw = db.get_setting_sync(key, "")
        print(f"\n{'='*60}")
        print(f"KEY: {key}")
        print(f"{'='*60}")
        if not raw:
            print("  (not set in DB — using code defaults)")
        else:
            try:
                print(json.dumps(json.loads(raw), indent=2))
            except Exception:
                print(raw)

    if not apply:
        print("\n\n--- Pass --apply to write correct timings for both strategies ---")
        return

    # Apply trap_scanner per-index timings
    raw = db.get_setting_sync("trap_scanner", "{}")
    current = json.loads(raw) if raw else {}
    per_index = current.get("per_index", {})
    for idx, cfg in TRAP_TIMINGS.items():
        per_index.setdefault(idx, {}).update(cfg)
    current["per_index"] = per_index
    await db.set_setting("trap_scanner", json.dumps(current))
    print("\n✓ trap_scanner timings saved.")
    print(json.dumps(current, indent=2))

    # Apply sell_straddle global timings
    ss_raw = db.get_setting_sync("sell_straddle", "{}")
    ss = json.loads(ss_raw) if ss_raw else {}
    ss["entry_start"]    = "09:15"   # market opens 09:15
    ss["entry_end"]      = ss.get("entry_end",      "12:00")
    ss["squareoff_time"] = ss.get("squareoff_time", "15:15")
    await db.set_setting("sell_straddle", json.dumps(ss))
    print("\n✓ sell_straddle timings saved.")
    print(json.dumps(ss, indent=2))

asyncio.run(main())
