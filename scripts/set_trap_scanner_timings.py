"""
Check and set trap_scanner per-index timings in DB.
Run on EC2:
  python3 scripts/set_trap_scanner_timings.py          # show current
  python3 scripts/set_trap_scanner_timings.py --apply  # apply the timings below
"""
import sys, os, asyncio, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_layer.client_db import ClientDB

# ── EDIT THESE to change timings ─────────────────────────────────────────────
TIMINGS = {
    "NIFTY": {
        "entry_cutoff": "15:10",   # no new entries after this
        "sq_off_time":  "15:20",   # force-exit all positions
        "entry_window": None,      # None = all day
    },
    "SENSEX": {
        "entry_cutoff": "15:20",
        "sq_off_time":  "15:25",
        "entry_window": None,
    },
    "CRUDEOIL": {
        "entry_cutoff": "22:45",
        "sq_off_time":  "23:00",
        "entry_window": [[14, 30], [22, 45]],  # entry allowed 14:30–22:45 only
    },
}
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    db = ClientDB("data/clients.db")
    raw = db.get_setting_sync("trap_scanner", "{}")
    current = json.loads(raw) if raw else {}

    print("=" * 60)
    print("CURRENT trap_scanner settings in DB:")
    print(json.dumps(current, indent=2))
    print("=" * 60)

    if "--apply" not in sys.argv:
        print("\nDry run — pass --apply to save these timings:")
        print(json.dumps({"per_index": TIMINGS}, indent=2))
        print("\nRun:  python3 scripts/set_trap_scanner_timings.py --apply")
        return

    # Merge per_index timings into existing settings
    per_index = current.get("per_index", {})
    for idx, cfg in TIMINGS.items():
        if idx not in per_index:
            per_index[idx] = {}
        per_index[idx].update({k: v for k, v in cfg.items() if v is not None})
        if cfg.get("entry_window") is not None:
            per_index[idx]["entry_window"] = cfg["entry_window"]
    current["per_index"] = per_index

    await db.set_setting("trap_scanner", json.dumps(current))

    print("\nSAVED. New trap_scanner settings:")
    saved = json.loads(db.get_setting_sync("trap_scanner", "{}"))
    print(json.dumps(saved, indent=2))
    print("\nRestart pm2 for changes to take effect.")

asyncio.run(main())
