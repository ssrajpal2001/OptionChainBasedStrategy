"""
scripts/oi_orb_db_inventory_sep1_to_date.py

Direct user follow-up, 2026-09-16: "I HAVE DATA FROM 1ST OF SEP TILL DATE
IN DB." First pass against data/oi_orb_screener.db found only 2026-09-16
rows -- but the LIVE process's own cwd (confirmed via `pm2 describe`) is
the same directory we already checked, so that emptiness is real, not a
path mismatch. What WAS found: a real archived copy from BEFORE a DB
wipe, at archive/20260910_085314/oi_orb_screener.db (5.4MB vs. the
current 266KB), dated 2026-09-10 -- almost certainly holds real history
from whenever the screener started logging through 2026-09-10 08:53.
This script now accepts that (or any) db path as an optional CLI arg so
it can inspect the archive without overwriting the DB_PATH default used
for the live one. Read-only, no writes, either way.

MUST run on EC2.

Usage:
  python scripts/oi_orb_db_inventory_sep1_to_date.py                    # live db (data/oi_orb_screener.db)
  python scripts/oi_orb_db_inventory_sep1_to_date.py archive/20260910_085314/oi_orb_screener.db
"""
from __future__ import annotations

import sqlite3
import sys

sys.path.insert(0, ".")

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/oi_orb_screener.db"
START_DATE = "2026-09-01"


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=" * 110)
    print(f"OI-ORB Screener DB inventory -- {DB_PATH}, {START_DATE} to date")
    print("=" * 110)

    tables = ["scans", "shortlist", "oi_orb_top20_daily_scan", "signal_events",
              "positions", "oi_spurt_history", "rank_snapshots"]
    for t in tables:
        if not _table_exists(conn, t):
            print(f"\n{t}: TABLE DOES NOT EXIST")
            continue
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({t})")]
        date_col = "trade_date" if "trade_date" in cols else (
            "poll_ts" if "poll_ts" in cols else None)
        print(f"\n{'-' * 110}\n{t}  (columns: {cols})\n{'-' * 110}")
        if date_col is None:
            total = conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"]
            print(f"  no date/trade_date column found -- total rows: {total}")
            continue
        rows = conn.execute(
            f"SELECT substr({date_col},1,10) d, COUNT(*) c, COUNT(DISTINCT symbol) syms "
            f"FROM {t} WHERE substr({date_col},1,10) >= ? GROUP BY d ORDER BY d"
            if "symbol" in cols else
            f"SELECT substr({date_col},1,10) d, COUNT(*) c FROM {t} "
            f"WHERE substr({date_col},1,10) >= ? GROUP BY d ORDER BY d",
            (START_DATE,),
        ).fetchall()
        if not rows:
            print(f"  NO rows from {START_DATE} onward")
            continue
        for r in rows:
            if "symbol" in cols:
                print(f"  {r['d']}: {r['c']} rows, {r['syms']} distinct symbols")
            else:
                print(f"  {r['d']}: {r['c']} rows")

    # positions table detail: real trades with entry/exit/pnl per day
    if _table_exists(conn, "positions"):
        print(f"\n{'-' * 110}\npositions -- real trade detail per day (entry/exit/pnl)\n{'-' * 110}")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(positions)")]
        date_col = "trade_date" if "trade_date" in cols else None
        if date_col:
            rows = conn.execute(
                f"SELECT trade_date, symbol, option_type, status, entry_price, exit_price, pnl "
                f"FROM positions WHERE trade_date >= ? ORDER BY trade_date, symbol", (START_DATE,)
            ).fetchall()
            if not rows:
                print("  NO real position rows recorded since 2026-09-01")
            for r in rows:
                print(f"  {r['trade_date']} {r['symbol']:12s} {r['option_type']:5s} status={r['status']:8s} "
                      f"entry={r['entry_price']} exit={r['exit_price']} pnl={r['pnl']}")

    print("\n" + "=" * 110)
    conn.close()


if __name__ == "__main__":
    main()
