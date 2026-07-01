"""
scripts/ec2_db_import.py
========================
Run locally after receiving the JSON export from EC2.
Wipes and rebuilds data/clients.db with the full live state.

Usage:
    python scripts/ec2_db_import.py db_export_20260701_120000.json

After this, run the bot normally:
    python run_system.py --mode live --ui --port 5000 --index NIFTY --strategies sell_straddle,trap_scanner
"""
import json
import sqlite3
import os
import sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "clients.db")


def _get_columns(cur, table: str):
    return [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]


def _ensure_schema(cur):
    """Create tables if they don't exist yet (mirrors ClientDB.init_db)."""
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id TEXT PRIMARY KEY, name TEXT, email TEXT, password_hash TEXT,
            capital REAL DEFAULT 500000, max_risk_pct REAL DEFAULT 1.0,
            max_daily_loss_pct REAL DEFAULT 3.0, lot_multiplier REAL DEFAULT 1.0,
            enabled_strategies TEXT DEFAULT '', strategy_selections TEXT DEFAULT '[]',
            is_admin_approved INTEGER DEFAULT 0, is_client_bot_active INTEGER DEFAULT 0,
            target_index TEXT DEFAULT 'NIFTY', is_active INTEGER DEFAULT 1,
            created_at TEXT, updated_at TEXT, trap_instruments TEXT DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS broker_bindings (
            client_id TEXT, binding_id TEXT, provider TEXT DEFAULT '',
            label TEXT DEFAULT '', assigned_strategy TEXT DEFAULT '',
            assigned_instrument TEXT DEFAULT 'NIFTY', trading_mode TEXT DEFAULT 'paper',
            is_trade_enabled INTEGER DEFAULT 0, lot_multiplier REAL DEFAULT 1.0,
            enabled INTEGER DEFAULT 1, terminal_connected INTEGER DEFAULT 0,
            terminal_connected_at TEXT DEFAULT '', engine_active INTEGER DEFAULT 0,
            product_type TEXT DEFAULT 'MIS', show_granular_ticks INTEGER DEFAULT 0,
            source_ip TEXT DEFAULT '', whitelist_ip TEXT DEFAULT '',
            password_enc TEXT DEFAULT '', totp_secret_enc TEXT DEFAULT '',
            api_key_enc TEXT DEFAULT '', api_secret_enc TEXT DEFAULT '',
            access_token TEXT DEFAULT '', created_at TEXT, updated_at TEXT,
            PRIMARY KEY (client_id, binding_id)
        );
        CREATE TABLE IF NOT EXISTS strategy_deployments (
            deploy_id TEXT PRIMARY KEY, client_id TEXT, binding_id TEXT,
            strategy_name TEXT, underlying TEXT, lot_multiplier REAL DEFAULT 1.0,
            max_profit_rs REAL DEFAULT 0, max_sl_rs REAL DEFAULT 0,
            squareoff_time TEXT DEFAULT '15:15', is_active INTEGER DEFAULT 1,
            created_at TEXT, updated_at TEXT,
            is_running INTEGER DEFAULT 0, expiry_mode TEXT DEFAULT 'current'
        );
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS system_feeder_creds (
            provider TEXT PRIMARY KEY, creds_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS btc_live_config (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_id TEXT, binding_id TEXT,
            strategy_name TEXT, underlying TEXT, leg TEXT, side TEXT,
            qty INTEGER, price REAL, pnl REAL, reason TEXT, ts TEXT,
            open_time TEXT, close_time TEXT, open_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS position_store (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS password_resets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, token_hash TEXT NOT NULL,
            target_role TEXT NOT NULL, target_id TEXT NOT NULL,
            created_at TEXT NOT NULL, expires_at TEXT NOT NULL, used INTEGER DEFAULT 0
        );
    """)


def import_db(export_file: str):
    if not os.path.exists(export_file):
        print(f"[ERROR] File not found: {export_file}")
        sys.exit(1)

    with open(export_file) as f:
        data = json.load(f)

    exported_at = data.get("exported_at", "unknown")
    print(f"Importing export from: {exported_at}")
    print(f"Target DB: {DB_PATH}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    # Back up existing DB if it exists
    if os.path.exists(DB_PATH):
        bak = DB_PATH + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        import shutil
        shutil.copy2(DB_PATH, bak)
        print(f"[backup] Existing DB backed up to: {bak}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    _ensure_schema(cur)
    conn.commit()

    tables = data.get("tables", {})
    for table, rows in tables.items():
        if not rows:
            print(f"  [{table}] 0 rows — skipping")
            continue

        # Get actual columns in local table (may differ from export if schema evolved)
        local_cols = set(_get_columns(cur, table))
        if not local_cols:
            print(f"  [{table}] table doesn't exist locally — skipping")
            continue

        imported = 0
        skipped = 0
        for row in rows:
            # Only insert columns that exist in local schema
            filtered = {k: v for k, v in row.items() if k in local_cols}
            if not filtered:
                skipped += 1
                continue
            cols_sql = ", ".join(filtered.keys())
            placeholders = ", ".join(["?" for _ in filtered])
            try:
                cur.execute(
                    f"INSERT OR REPLACE INTO {table} ({cols_sql}) VALUES ({placeholders})",
                    list(filtered.values())
                )
                imported += 1
            except Exception as e:
                print(f"  [{table}] row insert error: {e} — row: {list(filtered.keys())}")
                skipped += 1

        if skipped:
            print(f"  [{table}] {imported} imported, {skipped} skipped")
        else:
            print(f"  [{table}] {imported} imported")

    conn.commit()
    conn.close()
    print(f"\n[done] DB imported to: {DB_PATH}")
    print("\nTo run the bot locally with live tokens:")
    print("  python run_system.py --mode live --ui --port 5000 --index NIFTY --strategies sell_straddle,trap_scanner")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/ec2_db_import.py <db_export_file.json>")
        sys.exit(1)
    import_db(sys.argv[1])
