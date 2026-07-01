"""
scripts/ec2_db_export.py
========================
Run this on your EC2 instance to export the FULL live database including
all encrypted credentials and access tokens.

Usage (on EC2):
    cd /home/ubuntu/OptionChainBasedStrategy   # or wherever the bot lives
    python scripts/ec2_db_export.py

Output: db_export_<timestamp>.json in the current directory.
Send that file to the dev machine, then run scripts/ec2_db_import.py locally.
"""
import json
import sqlite3
import os
import sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clients.db")

def export():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all table names
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]

    export_data = {
        "exported_at": datetime.now().isoformat(),
        "db_path": DB_PATH,
        "tables": {}
    }

    for table in tables:
        rows = cur.execute(f"SELECT * FROM {table}").fetchall()
        export_data["tables"][table] = [dict(r) for r in rows]
        print(f"  [{table}] {len(rows)} rows")

    conn.close()

    out_file = f"db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w") as f:
        json.dump(export_data, f, indent=2, default=str)

    print(f"\n[done] Exported to: {out_file}")
    print(f"       File size: {os.path.getsize(out_file):,} bytes")
    print(f"\nNext step: copy this file to your dev machine and run:")
    print(f"  python scripts/ec2_db_import.py db_export_*.json")

if __name__ == "__main__":
    print(f"Exporting DB: {DB_PATH}")
    export()
