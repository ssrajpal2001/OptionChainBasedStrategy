"""
scripts/setup_btc_live.py
Run ONCE to store BTC live trading config in data/clients.db.

Usage:
    python3 scripts/setup_btc_live.py
"""
import sqlite3, json, os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "clients.db")

CONFIG = {
    # ── Delta Exchange credentials (LIVE account) ──────────────────────────────
    "api_key"           : "PASTE_YOUR_LIVE_API_KEY_HERE",
    "api_secret"        : "PASTE_YOUR_LIVE_API_SECRET_HERE",

    # ── Strategy parameters (validated by 90-day backtest) ────────────────────
    "lots"              : 20,       # 20 lots = 0.02 BTC per trade
    "htf_min"           : 240,      # 4h HTF zone detection
    "sub_min"           : 30,       # 30m LTF confirmation
    "sl_buf"            : 500,      # SL buffer: $500 pts beyond zone
    "profit_floor_pts"  : 200,      # break-even after $200 move in favour
    "profit_cap_pts"    : 1000,     # profit target: $1000 pts move
    "lookback_days"     : 3,        # lookback for HTF zone building
    "cooldown_days"     : 1,        # skip zone for 1 day after SL

    # ── Live / paper toggle ────────────────────────────────────────────────────
    # Set paper_mode=True for dry-run (signals logged, NO real orders placed)
    "paper_mode"        : False,

    # ── Risk limits ───────────────────────────────────────────────────────────
    "max_daily_loss_usdt": 300,     # halt trading if day loss exceeds this
    "max_open_trades"   : 1,        # 1 trade at a time (positional)
}

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS btc_live_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    for k, v in CONFIG.items():
        conn.execute(
            "INSERT OR REPLACE INTO btc_live_config (key, value) VALUES (?, ?)",
            (k, json.dumps(v))
        )
    conn.commit()
    conn.close()
    print(f"[setup] BTC live config saved to {DB_PATH}")
    print(f"[setup] paper_mode = {CONFIG['paper_mode']}")
    print(f"[setup] lots={CONFIG['lots']}  sl=${CONFIG['sl_buf']}pts  "
          f"floor=${CONFIG['profit_floor_pts']}pts  cap=${CONFIG['profit_cap_pts']}pts")
    if CONFIG["api_key"].startswith("PASTE"):
        print("\n*** WARNING: Replace api_key / api_secret with your real credentials before going live! ***")

if __name__ == "__main__":
    main()
