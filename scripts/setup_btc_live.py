"""
scripts/setup_btc_live.py
Run ONCE to store BTC live trading config in data/clients.db.

Usage:
    python3 scripts/setup_btc_live.py
"""
import sqlite3, json, os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "clients.db")

CONFIG = {
    # ── Delta Exchange credentials ─────────────────────────────────────────────
    "api_key"           : "PIHwljiMMoQsk9maAwEO83KVPhOmmB",
    "api_secret"        : "SAYuGdY5ma7xhAPpQsl854sM03oPXQCHsrDWX8G7grM8Zp5lF1w8m80ZVqzJ",

    # ── Strategy parameters (validated by 90-day backtest, PF=1.508) ──────────
    "lots"              : 20,       # 20 lots = 0.02 BTC per trade
    "htf_min"           : 240,      # 4h HTF zone detection
    "sub_min"           : 30,       # 30m LTF confirmation
    "sl_buf"            : 500,      # SL buffer: $500 pts beyond zone
    "profit_floor_pts"  : 200,      # break-even after $200 pts move in favour
    "profit_cap_pts"    : 1000,     # profit target: $1000 pts move
    "lookback_days"     : 3,        # lookback days for HTF zone building
    "cooldown_days"     : 1,        # skip zone for 1 day after losing SL

    # ── Live mode ─────────────────────────────────────────────────────────────
    "paper_mode"        : False,    # LIVE — real orders on Delta Exchange

    # ── Risk limits ───────────────────────────────────────────────────────────
    "max_daily_loss_usdt": 300,     # halt if day loss exceeds $300 USDT
    "max_open_trades"   : 1,        # 1 positional trade at a time
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
    print(f"[setup] paper_mode  = {CONFIG['paper_mode']}")
    print(f"[setup] lots        = {CONFIG['lots']}  (0.02 BTC per trade)")
    print(f"[setup] HTF         = {CONFIG['htf_min']//60}h")
    print(f"[setup] Sub         = {CONFIG['sub_min']}m")
    print(f"[setup] SL buf      = ${CONFIG['sl_buf']} pts")
    print(f"[setup] Floor       = ${CONFIG['profit_floor_pts']} pts (break-even)")
    print(f"[setup] Cap         = ${CONFIG['profit_cap_pts']} pts (profit target)")
    print(f"[setup] Day loss cap= ${CONFIG['max_daily_loss_usdt']} USDT")
    print("\n[setup] Done. Run: python3 scripts/btc_live_trader.py")

if __name__ == "__main__":
    main()
