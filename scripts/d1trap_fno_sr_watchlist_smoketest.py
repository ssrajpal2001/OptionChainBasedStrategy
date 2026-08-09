"""
scripts/d1trap_fno_sr_watchlist_smoketest.py — 2026-08-09.

Verifies the WATCHLIST sentinel wiring just added for "d1_trap_fno_sr" in
book_manager.py: _wanted() must read data/fno_watchlist.json and thread
upstox_key/lot/step overrides through to a real D1TrapFnOSRBook instance via
_spawn_book(), exactly as it already does for "d1_trap_fno". Writes a
throwaway watchlist file + a throwaway "SMOKETEST" deployment row (cleaned
up at the end either way), never touches real client data.

Usage:
    python3 scripts/d1trap_fno_sr_watchlist_smoketest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB  # noqa: E402
from strategies.d1_trap_option.book_manager import D1TrapOptionBookManager  # noqa: E402

CID, BID = "ssrajpal2001", "SMOKETEST_BIND"  # real client (get_running_deployments_by_strategy_sync
                                              # JOINs clients WHERE is_active=1 -- a fake client_id
                                              # would never match), throwaway binding_id, cleaned up
WL_PATH = Path(__file__).resolve().parents[1] / "data" / "fno_watchlist.json"


async def main() -> int:
    db = ClientDB()

    wl_backup = WL_PATH.read_text(encoding="utf-8") if WL_PATH.exists() else None
    wl_payload = {
        "generated": "2026-08-09 15:30",
        "stocks": [
            {"symbol": "RELIANCE", "upstox_key": "NSE_EQ|INE002A01018", "lot": 500, "step": 20,
             "direction": "CE", "status": "APPROACHING", "btst_rr": 2.1},
            {"symbol": "TCS", "upstox_key": "NSE_EQ|INE467B01029", "lot": 175, "step": 20,
             "direction": "PE", "status": "APPROACHING", "btst_rr": 1.8},
        ],
    }
    WL_PATH.write_text(json.dumps(wl_payload, indent=2), encoding="utf-8")

    ok = True
    try:
        deploy_id = await db.save_deployment(
            client_id=CID, binding_id=BID, strategy_name="d1_trap_fno_sr",
            underlying="WATCHLIST", lot_multiplier=1, max_profit_rs=0, max_sl_rs=0,
            squareoff_time="15:15", product_type="NRML",
            strategy_params=json.dumps({"itm": 1, "hard_risk_pct": 0.10, "top_n": 5}),
            carry_forward=False,
        )
        await db.set_deployment_running(deploy_id, CID, True)

        mgr = D1TrapOptionBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
        wanted = mgr._wanted()

        keys = [k for k in wanted if k[0] == CID and k[1] == BID]
        print(f"_wanted() produced {len(keys)} book(s) from the WATCHLIST sentinel:")
        for k in keys:
            cfg = wanted[k]
            print(f"  {k} -> strategy={cfg.get('strategy_name')} upstox_key={cfg.get('upstox_key')!r} "
                  f"lot_override={cfg.get('lot_override')} step_override={cfg.get('step_override')}")

        if len(keys) != 2:
            print(f"FAIL: expected 2 books (RELIANCE, TCS), got {len(keys)}")
            ok = False

        for k in keys:
            book = mgr._spawn_book(k, wanted[k])
            klass = type(book).__name__
            print(f"  spawned {klass} for {k[2]}: upstox_key_override={getattr(book, '_upstox_key_override', None)!r} "
                  f"lot_size={getattr(book, '_lot_size', None)} strike_step={getattr(book, '_strike_step', None)}")
            if klass != "D1TrapFnOSRBook":
                print(f"FAIL: expected D1TrapFnOSRBook, got {klass}")
                ok = False
            if not getattr(book, "_upstox_key_override", ""):
                print(f"FAIL: {k[2]}: upstox_key_override not threaded through")
                ok = False

        print("\nRESULT:", "PASS" if ok else "FAIL")
        return 0 if ok else 1
    finally:
        try:
            await db.delete_deployment(f"{CID}_{BID}_d1_trap_fno_sr_WATCHLIST", CID)
        except Exception:
            pass
        if wl_backup is not None:
            WL_PATH.write_text(wl_backup, encoding="utf-8")
        else:
            WL_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
