#!/usr/bin/env python3
"""
scripts/clean_instrument_state.py — remove persisted state for ONE instrument.

Run from the repo root, e.g.:
    python scripts/clean_instrument_state.py CRUDEOIL

Deletes:
  • position/session JSON files in data/positions
  • live-record CSVs in data/live_records
  • recorded feed files in data/recorded
  • historical CSVs in data/history
  • rows for this instrument in data/state_snapshots.db

Use before restarting a strategy after a major config/refactor change so the
engine boots with a clean slate instead of restoring stale positions.
"""
from __future__ import annotations

import glob
import os
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
STATE_DB = DATA_DIR / "state_snapshots.db"


def _delete_globs(patterns: list[str], dry_run: bool) -> int:
    removed = 0
    for pat in patterns:
        for p in glob.glob(pat, recursive=False):
            path = Path(p)
            if dry_run:
                print(f"  [dry-run] would delete {path}")
            else:
                print(f"  deleting {path}")
                try:
                    path.unlink()
                    removed += 1
                except OSError as exc:
                    print(f"    WARNING: could not delete {path}: {exc}")
    return removed


def _clean_state_db(instrument: str, dry_run: bool) -> None:
    if not STATE_DB.exists():
        print(f"  state DB not found: {STATE_DB}")
        return

    instrument_upper = instrument.upper()
    if dry_run:
        print(f"  [dry-run] would clean rows for {instrument_upper} in {STATE_DB}")
        return

    conn = sqlite3.connect(str(STATE_DB))
    cur = conn.cursor()
    tables = [
        ("candle_snapshots", "symbol"),
        ("strategy_b_state", "underlying"),
        ("order_tickets", "broker_symbol"),
    ]
    total = 0
    for table, col in tables:
        try:
            cur.execute(f"DELETE FROM {table} WHERE {col} LIKE ?", (f"%{instrument_upper}%",))
            n = cur.rowcount
            total += n
            print(f"    {table}.{col}: removed {n} rows")
        except sqlite3.OperationalError as exc:
            print(f"    WARNING: could not clean {table}: {exc}")
    conn.commit()
    conn.close()
    print(f"  total DB rows removed: {total}")


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <INSTRUMENT> [--dry-run]")
        return 1

    instrument = sys.argv[1].upper()
    dry_run = "--dry-run" in sys.argv

    print(f"Cleaning state for instrument: {instrument}" + (" (DRY RUN)" if dry_run else ""))

    file_patterns = [
        str(DATA_DIR / "positions" / f"*{instrument}*"),
        str(DATA_DIR / "live_records" / f"*{instrument}*"),
        str(DATA_DIR / "recorded" / f"*{instrument}*"),
        str(DATA_DIR / "history" / f"*{instrument}*"),
        str(DATA_DIR / "nse_option_cache" / f"*{instrument}*"),
    ]

    removed = _delete_globs(file_patterns, dry_run)
    print(f"  files removed: {removed}")

    _clean_state_db(instrument, dry_run)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
