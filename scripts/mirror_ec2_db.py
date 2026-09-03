"""
scripts/mirror_ec2_db.py
========================
Mirrors EC2 production DB state to local data/clients.db.
Run once after pulling the modular-strategy-refactor branch.

Safe to re-run: uses INSERT OR REPLACE everywhere.
Does NOT copy encrypted credentials or access tokens (EC2-specific).
"""
import json, sqlite3, os, sys
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "clients.db")

def run():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # ── 1. Schema migrations (missing columns/tables) ─────────────────────────

    # clients: add trap_instruments column if missing
    cols = {r[1] for r in cur.execute("PRAGMA table_info(clients)")}
    if "trap_instruments" not in cols:
        cur.execute("ALTER TABLE clients ADD COLUMN trap_instruments TEXT DEFAULT '[]'")
        print("[migration] clients: added trap_instruments column")

    # strategy_deployments: add is_running + expiry_mode if missing
    cols = {r[1] for r in cur.execute("PRAGMA table_info(strategy_deployments)")}
    if "is_running" not in cols:
        cur.execute("ALTER TABLE strategy_deployments ADD COLUMN is_running INTEGER DEFAULT 0")
        print("[migration] strategy_deployments: added is_running column")
    if "expiry_mode" not in cols:
        cur.execute("ALTER TABLE strategy_deployments ADD COLUMN expiry_mode TEXT DEFAULT 'current'")
        print("[migration] strategy_deployments: added expiry_mode column")

    # broker_bindings: add missing columns
    cols = {r[1] for r in cur.execute("PRAGMA table_info(broker_bindings)")}
    bb_adds = [
        ("product_type",        "TEXT    DEFAULT 'MIS'"),
        ("show_granular_ticks", "INTEGER DEFAULT 0"),
        ("source_ip",           "TEXT    DEFAULT ''"),
        ("whitelist_ip",        "TEXT    DEFAULT ''"),
        ("password_enc",        "TEXT    DEFAULT ''"),
        ("totp_secret_enc",     "TEXT    DEFAULT ''"),
    ]
    for col, defn in bb_adds:
        if col not in cols:
            cur.execute(f"ALTER TABLE broker_bindings ADD COLUMN {col} {defn}")
            print(f"[migration] broker_bindings: added {col}")

    # btc_live_config: create if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS btc_live_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # password_resets: create if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS password_resets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            token_hash  TEXT    NOT NULL,
            target_role TEXT    NOT NULL,
            target_id   TEXT    NOT NULL,
            created_at  TEXT    NOT NULL,
            expires_at  TEXT    NOT NULL,
            used        INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.commit()
    print("[migration] Schema up to date.\n")

    # ── 2. Clients ─────────────────────────────────────────────────────────────
    clients = [
        {
            "client_id": "ssrajpal2001",
            "name": "SARABJEET SINGH",
            "email": "ssrajpal2001@gmail.com",
            "password_hash": "6ed806940ea36eea0c1b0b097ac238e8:7c60fed6f4b1dd256ad2a9b843476a194719329345f8eaf02fabb9d2d06dd249",
            "capital": 500000.0,
            "max_risk_pct": 1.0,
            "max_daily_loss_pct": 3.0,
            "lot_multiplier": 1.0,
            "enabled_strategies": "",
            "strategy_selections": json.dumps([
                {"strategy": "sell_straddle", "instrument": "NIFTY"},
                {"strategy": "iron_condor",   "instrument": "NIFTY"},
                {"strategy": "trap_trading",  "instrument": "NIFTY"},
            ]),
            "is_admin_approved": 1,
            "is_client_bot_active": 0,
            "target_index": "SENSEX",
            "is_active": 1,
            "created_at": "2026-05-28T19:56:25.667355+05:30",
            "updated_at": "2026-06-29T11:31:04.559441+05:30",
            "trap_instruments": "[]",
        },
        {
            "client_id": "gurmeet",
            "name": "GURMEET",
            "email": "yourfamilydoctor@gmail.com",
            "password_hash": "4b7c384fd9ca9404a6ecae01a9e3d736:490499e9080bd0e87a9118924c0951c1ff6c5b5292b85ea87d4dcefac8cadfdd",
            "capital": 500000.0,
            "max_risk_pct": 1.0,
            "max_daily_loss_pct": 3.0,
            "lot_multiplier": 1.0,
            "enabled_strategies": "",
            "strategy_selections": json.dumps([
                {"strategy": "sell_straddle", "instrument": "NIFTY"},
                {"strategy": "iron_condor",   "instrument": "NIFTY"},
            ]),
            "is_admin_approved": 1,
            "is_client_bot_active": 0,
            "target_index": "SENSEX",
            "is_active": 1,
            "created_at": "2026-05-30T14:06:11.084482+05:30",
            "updated_at": "2026-06-15T04:44:04.912140+05:30",
            "trap_instruments": "[]",
        },
    ]
    for c in clients:
        cur.execute("""
            INSERT OR REPLACE INTO clients
            (client_id,name,email,password_hash,capital,max_risk_pct,max_daily_loss_pct,
             lot_multiplier,enabled_strategies,strategy_selections,is_admin_approved,
             is_client_bot_active,target_index,is_active,created_at,updated_at,trap_instruments)
            VALUES (:client_id,:name,:email,:password_hash,:capital,:max_risk_pct,
                    :max_daily_loss_pct,:lot_multiplier,:enabled_strategies,
                    :strategy_selections,:is_admin_approved,:is_client_bot_active,
                    :target_index,:is_active,:created_at,:updated_at,:trap_instruments)
        """, c)
    print(f"[clients] Upserted {len(clients)} clients.")

    # ── 3. Broker bindings ────────────────────────────────────────────────────
    # Only the live binding — SA5770 (Zerodha, ssrajpal2001)
    # Credentials (enc fields) left blank — EC2 has them; local = dev only
    bindings = [
        {
            "client_id": "ssrajpal2001",
            "binding_id": "SA5770",
            "provider": "zerodha",
            "label": "",
            "assigned_strategy": "",
            "assigned_instrument": "NIFTY",
            "trading_mode": "live",
            "is_trade_enabled": 0,
            "lot_multiplier": 1.0,
            "enabled": 1,
            "terminal_connected": 0,
            "terminal_connected_at": "",
            "engine_active": 0,
            "product_type": "MIS",
            "show_granular_ticks": 0,
            "source_ip": "172.31.29.159",
            "whitelist_ip": "",
            "created_at": "2026-05-28T19:56:25+05:30",
            "updated_at": datetime.now().isoformat(),
        },
    ]
    for b in bindings:
        cur.execute("""
            INSERT OR REPLACE INTO broker_bindings
            (client_id, binding_id, provider, label, assigned_strategy,
             assigned_instrument, trading_mode, is_trade_enabled, lot_multiplier,
             enabled, terminal_connected, terminal_connected_at, engine_active,
             product_type, show_granular_ticks, source_ip, whitelist_ip, created_at)
            VALUES
            (:client_id,:binding_id,:provider,:label,:assigned_strategy,
             :assigned_instrument,:trading_mode,:is_trade_enabled,:lot_multiplier,
             :enabled,:terminal_connected,:terminal_connected_at,:engine_active,
             :product_type,:show_granular_ticks,:source_ip,:whitelist_ip,:created_at)
        """, b)
    print(f"[broker_bindings] Upserted {len(bindings)} bindings.")

    # ── 4. Strategy deployments ───────────────────────────────────────────────
    deployments = [
        # Active live deployments (is_active=1, is_running from EC2)
        ("ssrajpal2001_SA5770_sell_straddle_NIFTY",    "ssrajpal2001","SA5770","sell_straddle","NIFTY",    1.0,  0.0,0.0,"15:15",1,"2026-06-15T08:44:52.068812+05:30","2026-06-30T08:56:34.962905+05:30",0,"current"),
        ("ssrajpal2001_SA5770_trap_scanner_NIFTY",     "ssrajpal2001","SA5770","trap_scanner", "NIFTY",    2.0,  0.0,0.0,"15:15",1,"2026-06-22T10:32:17.683697+05:30","2026-06-30T08:56:46.529644+05:30",0,"current"),
        ("ssrajpal2001_SA5770_trap_scanner_SENSEX",    "ssrajpal2001","SA5770","trap_scanner", "SENSEX",   2.0,  0.0,0.0,"15:15",1,"2026-06-24T09:04:23.367941+05:30","2026-06-30T08:56:51.258267+05:30",0,"current"),
        ("ssrajpal2001_SA5770_trap_scanner_BANKNIFTY", "ssrajpal2001","SA5770","trap_scanner", "BANKNIFTY",2.0,  0.0,0.0,"15:15",1,"2026-06-30T09:13:23.677184+05:30","2026-06-30T09:13:23.677184+05:30",0,"current"),
        ("ssrajpal2001_SA5770_trap_scanner_GOLDM",     "ssrajpal2001","SA5770","trap_scanner", "GOLDM",    2.0,  0.0,0.0,"23:00",0,"2026-06-29T10:59:31+05:30",        "2026-06-29T10:59:31+05:30",        1,"current"),
        ("ssrajpal2001_SA5770_trap_scanner_CRUDEOIL",  "ssrajpal2001","SA5770","trap_scanner", "CRUDEOIL", 2.0,  0.0,0.0,"23:30",0,"2026-06-23T10:58:10.898708+05:30","2026-06-29T17:29:09.277048+05:30",0,"current"),
        # Inactive legacy deployments (is_active=0) — kept for history
        ("ssrajpal2001_Zerodha_01_sell_straddle_NIFTY","ssrajpal2001","Zerodha_01","sell_straddle","NIFTY",10.0,5000.0,5000.0,"15:15",1,"2026-05-30T13:30:15.764456+05:30","2026-06-29T10:59:31+05:30",0,"current"),
        ("gurmeet_TS2839_sell_straddle_NIFTY",         "gurmeet",     "TS2839",   "sell_straddle","NIFTY",30.0,  0.0,0.0,"15:15",1,"2026-05-30T14:12:04.912584+05:30","2026-06-12T12:21:08.597772+05:30",0,"current"),
        ("ssrajpal2001_agelone_trap_scanner_GOLDM",    "ssrajpal2001","agelone",  "trap_scanner", "GOLDM",  1.0,  0.0,0.0,"23:25",1,"2026-06-26T17:37:51.197676+05:30","2026-06-26T17:37:51.197676+05:30",0,"current"),
    ]
    for d in deployments:
        cur.execute("""
            INSERT OR REPLACE INTO strategy_deployments
            (deploy_id,client_id,binding_id,strategy_name,underlying,lot_multiplier,
             max_profit_rs,max_sl_rs,squareoff_time,is_active,created_at,updated_at,
             is_running,expiry_mode)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, d)
    print(f"[strategy_deployments] Upserted {len(deployments)} deployments.")

    # ── 5. BTC live config ────────────────────────────────────────────────────
    btc_config = {
        "api_key"           : "PIHwljiMMoQsk9maAwEO83KVPhOmmB",
        "api_secret"        : "SAYuGdY5ma7xhAPpQsl854sM03oPXQCHsrDWX8G7grM8Zp5lF1w8m80ZVqzJ",
        "lots"              : 20,
        "htf_min"           : 240,
        "sub_min"           : 30,
        "sl_buf"            : 500,
        "profit_floor_pts"  : 200,
        "profit_cap_pts"    : 1000,
        "lookback_days"     : 3,
        "cooldown_days"     : 1,
        "paper_mode"        : False,
        "max_daily_loss_usdt": 300,
        "max_open_trades"   : 1,
    }
    for k, v in btc_config.items():
        cur.execute("INSERT OR REPLACE INTO btc_live_config (key,value) VALUES (?,?)",
                    (k, json.dumps(v)))
    print(f"[btc_live_config] Upserted {len(btc_config)} keys.")

    # ── 6. System settings ────────────────────────────────────────────────────
    settings = {
        "GLOBAL_REDIRECT_BASE": "https://13-200-171-160.sslip.io",
        "sell_straddle": json.dumps({
            "entry_start": "09:15",
            "entry_end": "12:00",
            "squareoff_time": "15:15",
        }),
        "trap_scanner": json.dumps({
            "htf_minutes": 75,
            "ltf_minutes": 1,
            "gap_threshold_pct": 0.5,
            "per_index": {
                "NIFTY": {
                    "entry_cutoff": "15:10",
                    "sq_off_time": "15:20",
                    "sl_buffer": 10.0,
                    "profit_floor": 5000.0,
                    "lot_size": 2,
                    "tsl_minutes": 3,
                    "htf_minutes": 15,
                    "sub_minutes": 3,
                    "max_ltf_index": 20,
                    "scale_in_enabled": True,
                },
                "GOLDM": {
                    "entry_cutoff": "23:20",
                    "sq_off_time": "23:25",
                    "sl_buffer": 20.0,
                    "max_sl_pts": 0,
                    "gap_itm_near": 500,
                    "gap_itm_far": 1000,
                    "profit_floor": 2000,
                    "no_target_tsl": False,
                    "lot_size": 1,
                    "scale_in_enabled": True,
                },
                "CRUDEOIL": {
                    "scale_in_enabled": True,
                },
            },
        }),
    }
    for k, v in settings.items():
        cur.execute("INSERT OR REPLACE INTO system_settings (key,value) VALUES (?,?)", (k, v))
    print(f"[system_settings] Upserted {len(settings)} settings.")

    conn.commit()
    conn.close()
    print(f"\n[done] Local DB mirrored: {DB_PATH}")

if __name__ == "__main__":
    run()
