"""
scripts/wipe_sell_straddle_data.py -- full reset of SellStraddle (all variants,
all clients/bindings) state, per direct user request 2026-09-23 after the
R1-breach duplicate-close incident: wipe today's corrupted state AND the
entire historical trade_history ledger for sell_straddle, across every
client/binding, and start fresh once the fixes in this same session's
commits (394ccb2, 20f2f46, e392a89) are deployed.

Scope (confirmed with the user): ALL sell_straddle deployments for ALL
clients (not just the affected one), and the ENTIRE historical trade ledger
(not just today's rows) -- this is paper trading data, not live capital.

What this touches (matched by "sell_straddle" appearing in the filename /
strategy field -- covers both the "sell_straddle" and "sell_straddle_calc_vwap"
variants, and every persistence suffix built from persist_key: plain,
_session, _session_carry, _pool, _pool_carry):
  1. data/positions/*sell_straddle*.json      -- open-position + session state
  2. data/chart_history/*sell_straddle*.json  -- per-binding premium chart series
  3. logs/clients/ss_*.log*                   -- per-binding rotating log files
                                                  (current + rotated .1/.2/.3)
  4. data/history/<client>.json               -- trade_history ledger: only the
                                                  entries whose "strategy" field
                                                  starts with "sell_straddle" are
                                                  REMOVED; other strategies'
                                                  entries in the same file
                                                  (oi_orb_screener, cag_straddle,
                                                  iron_fly, ...) are left intact.

Does NOT touch: any other strategy's data, data/clients.db (broker
credentials/bindings), data/oi_orb_screener.db, or anything outside the four
patterns above.

Safety: everything matched is copied into a timestamped backup directory
BEFORE deletion (data/_wipe_backup_<UTC-timestamp>/), so this is recoverable
if run in error. Defaults to --dry-run (lists what would be touched, deletes
nothing). Pass --confirm to actually perform the backup + wipe.

Usage (run from the repo root, on the machine that actually holds the data
-- e.g. on EC2 after `git pull`):
    python scripts/wipe_sell_straddle_data.py                 # dry run
    python scripts/wipe_sell_straddle_data.py --confirm        # do it

After running with --confirm, restart the strategy process (e.g.
`pm2 restart terminus`) so every SellStraddle book starts genuinely fresh --
this script only clears on-disk state; a running process's in-memory
position/session state is untouched until it restarts.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
from datetime import datetime, timezone


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POSITIONS_DIR = os.path.join(ROOT, "data", "positions")
CHART_HISTORY_DIR = os.path.join(ROOT, "data", "chart_history")
LOGS_DIR = os.path.join(ROOT, "logs", "clients")
HISTORY_DIR = os.path.join(ROOT, "data", "history")


def _find_position_files():
    return sorted(glob.glob(os.path.join(POSITIONS_DIR, "*sell_straddle*.json")))


def _find_chart_history_files():
    return sorted(glob.glob(os.path.join(CHART_HISTORY_DIR, "*sell_straddle*.json")))


def _find_log_files():
    # current + rotated backups (ss_NIFTY_client_binding_20260923.log, .log.1, ...)
    return sorted(
        glob.glob(os.path.join(LOGS_DIR, "ss_*.log"))
        + glob.glob(os.path.join(LOGS_DIR, "ss_*.log.*"))
    )


def _find_history_entries():
    """Return {file_path: [matching trade dicts]} for every data/history/*.json
    file that has at least one entry whose strategy starts with sell_straddle."""
    out = {}
    for path in sorted(glob.glob(os.path.join(HISTORY_DIR, "*.json"))):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as exc:
            print(f"  [skip, unreadable] {path}: {exc}")
            continue
        trades = data.get("trades", [])
        matches = [t for t in trades if str(t.get("strategy", "")).startswith("sell_straddle")]
        if matches:
            out[path] = matches
    return out


def _backup(paths: list, backup_root: str, label: str) -> None:
    if not paths:
        return
    dest_dir = os.path.join(backup_root, label)
    os.makedirs(dest_dir, exist_ok=True)
    for p in paths:
        shutil.copy2(p, os.path.join(dest_dir, os.path.basename(p)))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--confirm", action="store_true",
                     help="Actually perform the backup + wipe. Without this, dry-run only.")
    args = ap.parse_args()

    pos_files = _find_position_files()
    chart_files = _find_chart_history_files()
    log_files = _find_log_files()
    history_matches = _find_history_entries()
    total_history_rows = sum(len(v) for v in history_matches.values())

    print("=== SellStraddle data wipe (all clients, all variants) ===")
    print(f"Position/session files : {len(pos_files)}")
    for p in pos_files:
        print(f"    {p}")
    print(f"Chart-history files    : {len(chart_files)}")
    for p in chart_files:
        print(f"    {p}")
    print(f"Log files (incl. rotated): {len(log_files)}")
    for p in log_files:
        print(f"    {p}")
    print(f"Trade-history files with sell_straddle rows: {len(history_matches)} "
          f"({total_history_rows} rows total)")
    for p, rows in history_matches.items():
        print(f"    {p}  ({len(rows)} rows)")

    if not args.confirm:
        print("\nDRY RUN -- nothing deleted. Re-run with --confirm to perform the wipe "
              "(a timestamped backup is made first).")
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_root = os.path.join(ROOT, "data", f"_wipe_backup_{ts}")
    os.makedirs(backup_root, exist_ok=True)
    print(f"\nBacking up to {backup_root} ...")
    _backup(pos_files, backup_root, "positions")
    _backup(chart_files, backup_root, "chart_history")
    _backup(log_files, backup_root, "logs")
    _backup(list(history_matches.keys()), backup_root, "history_before_filter")

    print("Deleting position/chart-history/log files ...")
    for p in pos_files + chart_files + log_files:
        os.remove(p)

    print("Filtering sell_straddle rows out of trade_history files ...")
    for path, matches in history_matches.items():
        with open(path) as f:
            data = json.load(f)
        kept = [t for t in data.get("trades", [])
                if not str(t.get("strategy", "")).startswith("sell_straddle")]
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"trades": kept}, f, indent=2)
        os.replace(tmp, path)
        print(f"    {path}: removed {len(matches)} rows, {len(kept)} remain")

    print(f"\nDone. Backup kept at {backup_root} -- delete it manually once you've "
          f"confirmed the wipe looks right.")
    print("Restart the strategy process now (e.g. `pm2 restart terminus`) so every "
          "SellStraddle book starts genuinely fresh -- this script only cleared "
          "on-disk state; a running process's in-memory state is untouched until "
          "it restarts.")


if __name__ == "__main__":
    main()
