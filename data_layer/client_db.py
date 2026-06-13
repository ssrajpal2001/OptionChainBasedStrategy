"""
data_layer/client_db.py — Multi-tenant lifecycle persistence layer.

Stores client profiles, broker bindings, lifecycle state flags, and
per-binding credentials in a dedicated SQLite database (data/clients.db).
Separate from state_persistence.py which handles market-data snapshots.

Schema:
  clients         — identity, capital params, lifecycle flags
  broker_bindings — per-binding credentials (XOR-obfuscated), strategy
                    assignment, token timestamps, trade enable switch

Security model:
  API keys/secrets are XOR-obfuscated with a key derived from the
  TERMINUS_CIPHER_KEY env var before being written.  For on-premises
  deployments the primary security boundary is filesystem permissions on
  the data/ directory.  Set TERMINUS_CIPHER_KEY in the environment to a
  strong random value before production deployment.

All write operations route through asyncio.to_thread() — the event loop
is never stalled by a disk write.  Synchronous helpers (_exec, get_*_sync)
are boot-time-safe and called before the event loop is running.

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from config.global_config import IST

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = "data/clients.db"

# ── Credential obfuscation ────────────────────────────────────────────────────

_CIPHER_KEY = hashlib.sha256(
    os.environ.get("TERMINUS_CIPHER_KEY", "terminus-key-change-in-production").encode()
).digest()


def _encode_cred(plaintext: str) -> str:
    """XOR-obfuscate credential before DB storage."""
    if not plaintext:
        return ""
    b = plaintext.encode("utf-8")
    key = (_CIPHER_KEY * ((len(b) // len(_CIPHER_KEY)) + 1))[: len(b)]
    return base64.b64encode(bytes(x ^ y for x, y in zip(b, key))).decode()


def _decode_cred(encoded: str) -> str:
    """Reverse XOR-obfuscate credential from DB storage."""
    if not encoded:
        return ""
    try:
        b = base64.b64decode(encoded)
        key = (_CIPHER_KEY * ((len(b) // len(_CIPHER_KEY)) + 1))[: len(b)]
        return bytes(x ^ y for x, y in zip(b, key)).decode("utf-8")
    except Exception:
        return ""


# ── Password hashing ──────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """One-way password hash stored as 'hex_salt:hex_hash'."""
    salt = os.urandom(16).hex()
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}:{h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Verify password against stored 'salt:hash'."""
    try:
        salt, h = stored.split(":", 1)
        new_h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
        return new_h.hex() == h
    except Exception:
        return False


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS strategy_deployments (
    deploy_id          TEXT PRIMARY KEY,
    client_id          TEXT NOT NULL,
    binding_id         TEXT NOT NULL,
    strategy_name      TEXT NOT NULL,
    underlying         TEXT NOT NULL DEFAULT 'NIFTY',
    lot_multiplier     REAL NOT NULL DEFAULT 1.0,
    max_profit_rs      REAL NOT NULL DEFAULT 0.0,
    max_sl_rs          REAL NOT NULL DEFAULT 0.0,
    squareoff_time     TEXT NOT NULL DEFAULT '15:15',
    is_active          INTEGER DEFAULT 1,
    is_running         INTEGER DEFAULT 0,   -- per-strategy Start/Stop toggle (0 = deployed but stopped)
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    client_id               TEXT    PRIMARY KEY,
    name                    TEXT    DEFAULT '',
    email                   TEXT    DEFAULT '',
    password_hash           TEXT    DEFAULT '',
    capital                 REAL    DEFAULT 500000,
    max_risk_pct            REAL    DEFAULT 1.0,
    max_daily_loss_pct      REAL    DEFAULT 3.0,
    lot_multiplier          REAL    DEFAULT 1.0,
    enabled_strategies      TEXT    DEFAULT '',
    strategy_selections     TEXT    DEFAULT '[]',
    is_admin_approved       INTEGER DEFAULT 0,
    is_client_bot_active    INTEGER DEFAULT 0,
    target_index            TEXT    DEFAULT 'NIFTY',
    is_active               INTEGER DEFAULT 1,
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_bindings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id           TEXT    NOT NULL REFERENCES clients(client_id),
    binding_id          TEXT    NOT NULL,
    provider            TEXT    NOT NULL,
    label               TEXT    DEFAULT '',
    user_id_enc         TEXT    DEFAULT '',
    api_key_enc         TEXT    DEFAULT '',
    api_secret_enc      TEXT    DEFAULT '',
    access_token        TEXT    DEFAULT '',
    token_generated_at  TEXT    DEFAULT '',
    token_expiry_at     TEXT    DEFAULT '',
    assigned_strategy   TEXT    DEFAULT '',
    assigned_instrument TEXT    DEFAULT 'NIFTY',
    trading_mode        TEXT    DEFAULT 'paper',
    product_type        TEXT    DEFAULT 'MIS',
    is_trade_enabled    INTEGER DEFAULT 1,
    lot_multiplier      REAL    DEFAULT 1.0,
    enabled             INTEGER DEFAULT 1,
    show_granular_ticks INTEGER DEFAULT 0,
    source_ip           TEXT    DEFAULT '',
    whitelist_ip        TEXT    DEFAULT '',
    created_at          TEXT    NOT NULL,
    UNIQUE(client_id, binding_id)
);

CREATE INDEX IF NOT EXISTS idx_clients_approved ON clients(is_admin_approved);
CREATE INDEX IF NOT EXISTS idx_bb_client        ON broker_bindings(client_id);

CREATE TABLE IF NOT EXISTS system_feeder_creds (
    provider           TEXT PRIMARY KEY,
    client_id_enc      TEXT DEFAULT '',
    api_key_enc        TEXT DEFAULT '',
    secret_enc         TEXT DEFAULT '',
    access_token       TEXT DEFAULT '',
    token_generated_at TEXT DEFAULT '',
    token_expiry_at    TEXT DEFAULT '',
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS option_1m_bar_repository (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol    TEXT    NOT NULL,
    timestamp TEXT    NOT NULL,
    open      REAL    NOT NULL DEFAULT 0.0,
    high      REAL    NOT NULL DEFAULT 0.0,
    low       REAL    NOT NULL DEFAULT 0.0,
    close     REAL    NOT NULL DEFAULT 0.0,
    volume    REAL    NOT NULL DEFAULT 0.0,
    UNIQUE(symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS ix_option1m_symbol_timestamp
    ON option_1m_bar_repository(symbol, timestamp);

CREATE TABLE IF NOT EXISTS ic_trade_log (
    trade_id            TEXT    NOT NULL,
    underlying          TEXT    NOT NULL,
    event               TEXT    NOT NULL,    -- ENTRY | ADJUST | EXIT
    short_ce_strike     REAL    DEFAULT 0,
    short_pe_strike     REAL    DEFAULT 0,
    long_ce_strike      REAL    DEFAULT 0,
    long_pe_strike      REAL    DEFAULT 0,
    net_credit          REAL    DEFAULT 0,
    cumulative_adj_pnl  REAL    DEFAULT 0,   -- in points across all rolls
    total_pnl_rs        REAL    DEFAULT 0,   -- in rupees
    adj_count_ce        INTEGER DEFAULT 0,
    adj_count_pe        INTEGER DEFAULT 0,
    status              TEXT    DEFAULT 'open',
    timestamp           TEXT    NOT NULL,
    is_active_adjustment INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ic_trade_log_id ON ic_trade_log(trade_id);

CREATE TABLE IF NOT EXISTS password_resets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash  TEXT    NOT NULL,
    target_role TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    expires_at  TEXT    NOT NULL,
    used        INTEGER NOT NULL DEFAULT 0
);
"""


# ── Client DB ─────────────────────────────────────────────────────────────────

class ClientDB:
    """
    Async-safe SQLite persistence for tenant lifecycle state.

    Write operations use asyncio.to_thread() to avoid blocking the event loop.
    Read operations (get_*_sync, load_all_profiles) are synchronous and
    intended for boot-time use before async tasks start.
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        self._db_path = db_path
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)

    async def initialise(self) -> None:
        """Create tables and indexes. Safe to call on every boot."""
        await asyncio.to_thread(self._create_tables)
        logger.info("ClientDB: ready at %s", self._db_path)

    # ── Client CRUD ───────────────────────────────────────────────────────────

    async def register_client(
        self,
        client_id: str,
        name: str,
        password: str,
        email: str = "",
        capital: float = 500_000.0,
        max_risk_pct: float = 1.0,
        max_daily_loss_pct: float = 3.0,
    ) -> None:
        """Create a new pending client record (is_admin_approved=0)."""
        now = datetime.now(IST).isoformat()
        ph = hash_password(password)
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO clients
               (client_id, name, email, password_hash, capital,
                max_risk_pct, max_daily_loss_pct,
                is_admin_approved, is_client_bot_active,
                created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,0,0,?,?)""",
            (client_id, name, email, ph, capital,
             max_risk_pct, max_daily_loss_pct, now, now),
        )

    async def upsert_client(self, client_id: str, **kwargs) -> None:
        """Partial update of any client columns."""
        now = datetime.now(IST).isoformat()
        kwargs["updated_at"] = now
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [client_id]
        await asyncio.to_thread(
            self._exec,
            f"UPDATE clients SET {sets} WHERE client_id = ?",
            vals,
        )

    def get_client_sync(self, client_id: str) -> Optional[dict]:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM clients WHERE client_id = ?", (client_id,)
        ).fetchone()
        con.close()
        return dict(row) if row else None

    def get_trap_instruments_sync(self, client_id: str) -> list:
        """Return the list of instruments this client has enabled for TrapTrading."""
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT trap_instruments FROM clients WHERE client_id = ?",
                (client_id,)
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return []
        try:
            return json.loads(row[0] or "[]")
        except (json.JSONDecodeError, TypeError):
            return []

    def set_trap_instruments_sync(self, client_id: str, instruments: list) -> None:
        """Persist the TrapTrading instrument list for a client."""
        con = sqlite3.connect(self._db_path)
        try:
            cursor = con.execute(
                "UPDATE clients SET trap_instruments = ?, updated_at = ? WHERE client_id = ?",
                (json.dumps(instruments), datetime.now(IST).isoformat(), client_id),
            )
            if cursor.rowcount == 0:
                raise ValueError(f"Client '{client_id}' not found")
            con.commit()
        except Exception as exc:
            con.rollback()
            raise
        finally:
            con.close()

    async def get_trap_instruments(self, client_id: str) -> list:
        return await asyncio.to_thread(self.get_trap_instruments_sync, client_id)

    async def set_trap_instruments(self, client_id: str, instruments: list) -> None:
        await asyncio.to_thread(self.set_trap_instruments_sync, client_id, instruments)

    def get_all_clients_sync(self) -> List[dict]:
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in
                    con.execute("SELECT * FROM clients ORDER BY created_at").fetchall()]
            con.close()
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_all_clients_sync: %s", exc)
            return []

    def get_pending_clients_sync(self) -> List[dict]:
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in
                    con.execute(
                        "SELECT * FROM clients WHERE is_admin_approved=0 ORDER BY created_at"
                    ).fetchall()]
            con.close()
            # Attach bindings for each pending client
            for row in rows:
                row["bindings"] = self.get_bindings_safe_sync(row["client_id"])
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_pending_clients_sync: %s", exc)
            return []

    def verify_client_password(self, client_id: str, password: str) -> bool:
        row = self.get_client_sync(client_id)
        if row is None:
            return False
        return verify_password(password, row.get("password_hash", ""))

    async def set_client_password(self, client_id: str, hashed: str) -> None:
        """Store a hashed password for a client (updates password_hash column)."""
        await asyncio.to_thread(
            self._exec,
            "UPDATE clients SET password_hash=? WHERE client_id=?",
            (hashed, client_id),
        )

    # ── Broker bindings ───────────────────────────────────────────────────────

    async def upsert_binding(
        self,
        client_id: str,
        binding_id: str,
        provider: str,
        label: str = "",
        user_id: str = "",
        api_key: str = "",
        api_secret: str = "",
        access_token: str = "",
        lot_multiplier: float = 1.0,
        trading_mode: str = "paper",
        product_type: str = "MIS",
        assigned_strategy: str = "",
        assigned_instrument: str = "NIFTY",
        source_ip: str = "",
        # Deprecated — accepted for backward compat but NOT stored
        password: str = "",
        totp_secret: str = "",
    ) -> None:
        logger.info(
            "[DB] upsert_binding [%s/%s] provider=%s mode=%s strategy=%s instrument=%s",
            client_id, binding_id, provider, trading_mode, assigned_strategy or "(none)", assigned_instrument,
        )
        """
        Insert or update a broker binding.

        Only stored: client_id, app_key (api_key), app_secret (api_secret).
        Passwords, PINs, and TOTP secrets are NOT stored — authentication
        is handled entirely via the broker's Interactive OAuth portal.
        """
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO broker_bindings
               (client_id, binding_id, provider, label,
                user_id_enc, api_key_enc, api_secret_enc,
                access_token, lot_multiplier, trading_mode, product_type,
                assigned_strategy, assigned_instrument, source_ip, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(client_id, binding_id) DO UPDATE SET
                 provider            = excluded.provider,
                 label               = excluded.label,
                 user_id_enc         = CASE WHEN excluded.user_id_enc  != '' THEN excluded.user_id_enc  ELSE user_id_enc  END,
                 api_key_enc         = CASE WHEN excluded.api_key_enc  != '' THEN excluded.api_key_enc  ELSE api_key_enc  END,
                 api_secret_enc      = CASE WHEN excluded.api_secret_enc != '' THEN excluded.api_secret_enc ELSE api_secret_enc END,
                 access_token        = CASE WHEN excluded.access_token  != '' THEN excluded.access_token  ELSE access_token  END,
                 lot_multiplier      = excluded.lot_multiplier,
                 trading_mode        = excluded.trading_mode,
                 product_type        = excluded.product_type,
                 source_ip           = CASE WHEN excluded.source_ip != '' THEN excluded.source_ip ELSE source_ip END,
                 assigned_strategy   = CASE WHEN excluded.assigned_strategy != '' THEN excluded.assigned_strategy ELSE assigned_strategy END,
                 assigned_instrument = CASE WHEN excluded.assigned_instrument != '' THEN excluded.assigned_instrument ELSE assigned_instrument END""",
            (
                client_id, binding_id, provider, label,
                _encode_cred(user_id),
                _encode_cred(api_key),
                _encode_cred(api_secret),
                access_token,
                lot_multiplier,
                trading_mode,
                product_type,
                assigned_strategy,
                assigned_instrument,
                source_ip,
                now,
            ),
        )

    async def set_assigned_strategy(
        self, client_id: str, binding_id: str, strategy: str
    ) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET assigned_strategy=? "
            "WHERE client_id=? AND binding_id=?",
            (strategy, client_id, binding_id),
        )

    async def update_access_token(
        self,
        client_id: str,
        binding_id: str,
        token: str,
        generated_at: str = "",
        expiry_at: str = "",
    ) -> None:
        logger.info(
            "[DB] update_access_token [%s/%s] token_len=%d generated_at=%s expiry_at=%s",
            client_id, binding_id, len(token), generated_at[:19] if generated_at else "now", expiry_at[:19] if expiry_at else "(none)",
        )
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """UPDATE broker_bindings
               SET access_token=?, token_generated_at=?, token_expiry_at=?
               WHERE client_id=? AND binding_id=?""",
            (token, generated_at or now, expiry_at, client_id, binding_id),
        )

    async def set_trade_enabled(
        self, client_id: str, binding_id: str, enabled: bool
    ) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET is_trade_enabled=? "
            "WHERE client_id=? AND binding_id=?",
            (1 if enabled else 0, client_id, binding_id),
        )

    async def set_trading_mode(
        self, client_id: str, binding_id: str, mode: str
    ) -> None:
        """Set 'paper' or 'live' trading mode for a specific broker binding."""
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET trading_mode=? "
            "WHERE client_id=? AND binding_id=?",
            (mode, client_id, binding_id),
        )

    async def set_terminal_connected(
        self, client_id: str, binding_id: str, connected: bool
    ) -> None:
        """Mark terminal as connected/disconnected (token validated)."""
        logger.info("[DB] set_terminal_connected [%s/%s] → %s", client_id, binding_id, connected)
        now = datetime.now(IST).isoformat() if connected else ""
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET terminal_connected=?, terminal_connected_at=? "
            "WHERE client_id=? AND binding_id=?",
            (1 if connected else 0, now, client_id, binding_id),
        )

    async def set_engine_active(
        self, client_id: str, binding_id: str, active: bool
    ) -> None:
        """Mark trading engine as active/inactive. Does NOT touch is_trade_enabled."""
        logger.info("[DB] set_engine_active [%s/%s] → %s", client_id, binding_id, active)
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET engine_active=? "
            "WHERE client_id=? AND binding_id=?",
            (1 if active else 0, client_id, binding_id),
        )

    async def set_show_granular_ticks(
        self, client_id: str, binding_id: str, enabled: bool
    ) -> None:
        """Toggle per-client tick-by-tick exit-audit streaming for this binding."""
        logger.info("[DB] set_show_granular_ticks [%s/%s] → %s", client_id, binding_id, enabled)
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET show_granular_ticks=? "
            "WHERE client_id=? AND binding_id=?",
            (1 if enabled else 0, client_id, binding_id),
        )

    async def set_binding_ips(
        self, client_id: str, binding_id: str, source_ip: str, whitelist_ip: str
    ) -> None:
        """Admin assigns this binding's egress IPs: source_ip (LOCAL/private — the bot binds
        orders to it) and whitelist_ip (PUBLIC — what the client whitelists in their broker)."""
        logger.info("[DB] set_binding_ips [%s/%s] source=%s whitelist=%s",
                    client_id, binding_id, source_ip, whitelist_ip)
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET source_ip=?, whitelist_ip=? "
            "WHERE client_id=? AND binding_id=?",
            (source_ip.strip(), whitelist_ip.strip(), client_id, binding_id),
        )

    # ── Strategy Deployments ──────────────────────────────────────────────────

    async def save_deployment(
        self,
        client_id:      str,
        binding_id:     str,
        strategy_name:  str,
        underlying:     str,
        lot_multiplier: float,
        max_profit_rs:  float,
        max_sl_rs:      float,
        squareoff_time: str,
    ) -> str:
        """Upsert a strategy deployment config. Returns the deploy_id."""
        deploy_id = f"{client_id}_{binding_id}_{strategy_name}_{underlying}"
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO strategy_deployments
               (deploy_id, client_id, binding_id, strategy_name, underlying,
                lot_multiplier, max_profit_rs, max_sl_rs, squareoff_time,
                is_active, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,1,?,?)
               ON CONFLICT(deploy_id) DO UPDATE SET
                 underlying=excluded.underlying,
                 lot_multiplier=excluded.lot_multiplier,
                 max_profit_rs=excluded.max_profit_rs,
                 max_sl_rs=excluded.max_sl_rs,
                 squareoff_time=excluded.squareoff_time,
                 is_active=1,
                 updated_at=excluded.updated_at""",
            (deploy_id, client_id, binding_id, strategy_name, underlying,
             lot_multiplier, max_profit_rs, max_sl_rs, squareoff_time, now, now),
        )
        logger.info(
            "ClientDB: deployment saved — %s [%s/%s %s %s lots=%.1f]",
            deploy_id, client_id, binding_id, strategy_name, underlying, lot_multiplier,
        )
        return deploy_id

    def get_deployments_sync(self, client_id: str) -> List[dict]:
        """Return all active deployments for a client."""
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in con.execute(
                "SELECT * FROM strategy_deployments WHERE client_id=? AND is_active=1 ORDER BY created_at",
                (client_id,),
            ).fetchall()]
            con.close()
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_deployments_sync(%s): %s", client_id, exc)
            return []

    async def delete_deployment(self, deploy_id: str, client_id: str) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE strategy_deployments SET is_active=0 WHERE deploy_id=? AND client_id=?",
            (deploy_id, client_id),
        )

    async def set_deployment_running(self, deploy_id: str, client_id: str, running: bool) -> None:
        """Per-strategy Start/Stop toggle. is_running gates whether the deployment's book trades."""
        await asyncio.to_thread(
            self._exec,
            "UPDATE strategy_deployments SET is_running=? WHERE deploy_id=? AND client_id=?",
            (1 if running else 0, deploy_id, client_id),
        )

    async def stop_all_deployments(self, client_id: str) -> None:
        """Global STOP — turn every deployment's run toggle OFF for a client."""
        await asyncio.to_thread(
            self._exec,
            "UPDATE strategy_deployments SET is_running=0 WHERE client_id=?",
            (client_id,),
        )

    async def delete_binding(self, client_id: str, binding_id: str) -> None:
        """Permanently remove a broker binding (credentials + config) from the DB."""
        await asyncio.to_thread(
            self._exec,
            "DELETE FROM broker_bindings WHERE client_id=? AND binding_id=?",
            (client_id, binding_id),
        )

    def get_bindings_sync(self, client_id: str) -> List[dict]:
        """Return bindings with credentials decoded — for internal execution use."""
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in
                    con.execute(
                        "SELECT * FROM broker_bindings WHERE client_id=? ORDER BY created_at",
                        (client_id,),
                    ).fetchall()]
            con.close()
            for r in rows:
                r["user_id"]   = _decode_cred(r.pop("user_id_enc", ""))
                r["api_key"]   = _decode_cred(r.pop("api_key_enc", ""))
                r["api_secret"] = _decode_cred(r.pop("api_secret_enc", ""))
                # password_enc / totp_secret_enc may exist in older DBs — pop and discard
                r.pop("password_enc", None)
                r.pop("totp_secret_enc", None)
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_bindings_sync(%s): %s", client_id, exc)
            return []

    def get_bindings_safe_sync(self, client_id: str) -> List[dict]:
        """Return bindings WITHOUT credentials — safe for API responses.

        Retries on a transient "database is locked": a square-off / kill writes deployment + binding
        rows, and a read racing that write used to throw → return [] → the UI panel showed ZERO
        brokers (they reappeared on the next 8s poll). `timeout` makes SQLite WAIT for the writer
        instead of erroring, and we retry a couple of times before giving up."""
        import time as _t
        last_exc = None
        for _attempt in range(3):
            try:
                con = sqlite3.connect(self._db_path, timeout=5.0)
                con.row_factory = sqlite3.Row
                rows = [dict(r) for r in
                        con.execute(
                            """SELECT binding_id, provider, label, access_token,
                                      token_generated_at, token_expiry_at,
                                      assigned_strategy, assigned_instrument,
                                      trading_mode, product_type,
                                      is_trade_enabled, lot_multiplier, enabled,
                                      terminal_connected, terminal_connected_at, engine_active,
                                      show_granular_ticks, source_ip, whitelist_ip
                               FROM broker_bindings WHERE client_id=? ORDER BY created_at""",
                            (client_id,),
                        ).fetchall()]
                con.close()
                return rows
            except Exception as exc:
                last_exc = exc
                _t.sleep(0.15)
        logger.error("ClientDB.get_bindings_safe_sync(%s): %s", client_id, last_exc)
        return []

    # ── System feeder credentials ─────────────────────────────────────────────

    async def upsert_feeder_creds(
        self,
        provider:  str,
        client_id: str = "",
        api_key:   str = "",
        secret:    str = "",
    ) -> None:
        """
        Persist admin feeder credentials (XOR-obfuscated).
        Only client_id (broker user ID), api_key, and secret are stored.
        Passwords, PINs, and TOTP secrets are NOT accepted.
        """
        logger.info(
            "[DB] upsert_feeder_creds provider=%s client_id_present=%s api_key_present=%s secret_present=%s",
            provider, bool(client_id), bool(api_key), bool(secret),
        )
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO system_feeder_creds
               (provider, client_id_enc, api_key_enc, secret_enc, updated_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(provider) DO UPDATE SET
                 client_id_enc = CASE WHEN excluded.client_id_enc != '' THEN excluded.client_id_enc ELSE client_id_enc END,
                 api_key_enc   = CASE WHEN excluded.api_key_enc   != '' THEN excluded.api_key_enc   ELSE api_key_enc   END,
                 secret_enc    = CASE WHEN excluded.secret_enc    != '' THEN excluded.secret_enc    ELSE secret_enc    END,
                 updated_at    = excluded.updated_at""",
            (
                provider,
                _encode_cred(client_id),
                _encode_cred(api_key),
                _encode_cred(secret),
                now,
            ),
        )

    def get_feeder_creds_sync(self, provider: str) -> Optional[dict]:
        """
        Return system feeder credentials for a provider with fields decoded.
        Returns None if no record exists.
        Only client_id, api_key, secret, and token fields are returned.
        """
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT * FROM system_feeder_creds WHERE provider = ?", (provider,)
            ).fetchone()
            con.close()
            if row is None:
                return None
            r = dict(row)
            return {
                "provider":           r["provider"],
                "client_id":          _decode_cred(r.get("client_id_enc", "")),
                "api_key":            _decode_cred(r.get("api_key_enc", "")),
                "secret":             _decode_cred(r.get("secret_enc", "")),
                "access_token":       r.get("access_token", ""),
                "token_generated_at": r.get("token_generated_at", ""),
                "token_expiry_at":    r.get("token_expiry_at", ""),
                "updated_at":         r.get("updated_at", ""),
            }
        except Exception as exc:
            logger.error("ClientDB.get_feeder_creds_sync(%s): %s", provider, exc)
            return None

    def find_by_broker_user_id_sync(
        self, provider: str, broker_user_id: str
    ) -> Optional[dict]:
        """
        Find a broker binding or feeder creds row by the broker-assigned user ID.
        Used by Dhan and AliceBlue callbacks to route the incoming token
        to the correct client binding without needing a state param.

        Returns dict with:
          scope      — 'feeder' | 'binding'
          client_id  — our internal client_id (or 'feeder' for admin feeder)
          binding_id — binding_id (or provider name for feeder)
          api_key    — decoded app key
          api_secret — decoded app secret
        Returns None if not found.
        """
        p = provider.lower()
        # Check admin feeder first
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT * FROM system_feeder_creds WHERE provider = ?", (p,)
            ).fetchone()
            con.close()
            if row:
                r = dict(row)
                stored_cid = _decode_cred(r.get("client_id_enc", ""))
                if stored_cid and stored_cid == broker_user_id:
                    return {
                        "scope":      "feeder",
                        "client_id":  "feeder",
                        "binding_id": p,
                        "api_key":    _decode_cred(r.get("api_key_enc", "")),
                        "api_secret": _decode_cred(r.get("secret_enc", "")),
                    }
        except Exception as exc:
            logger.error("ClientDB.find_by_broker_user_id_sync feeder check: %s", exc)

        # Check client broker bindings
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = con.execute(
                "SELECT * FROM broker_bindings WHERE provider = ?", (p,)
            ).fetchall()
            con.close()
            for row in rows:
                r = dict(row)
                stored_uid = _decode_cred(r.get("user_id_enc", ""))
                if stored_uid and stored_uid == broker_user_id:
                    return {
                        "scope":      "binding",
                        "client_id":  r["client_id"],
                        "binding_id": r["binding_id"],
                        "api_key":    _decode_cred(r.get("api_key_enc", "")),
                        "api_secret": _decode_cred(r.get("api_secret_enc", "")),
                    }
        except Exception as exc:
            logger.error("ClientDB.find_by_broker_user_id_sync binding check: %s", exc)

        return None

    def get_platform_credentials_sync(self, provider: str) -> Optional[dict]:
        """
        Get platform-level app credentials for a provider (api_key + secret).
        For Dhan: needed to call consumeApp-consent.
        Checks feeder_creds first, then falls back to first matching binding.
        Returns {"api_key": ..., "api_secret": ...} or None.
        """
        p = provider.lower()
        # Try feeder creds first
        feeder = self.get_feeder_creds_sync(p)
        if feeder and feeder.get("api_key"):
            return {"api_key": feeder["api_key"], "api_secret": feeder.get("secret", "")}
        # Fallback: first binding with credentials for this provider
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT api_key_enc, api_secret_enc FROM broker_bindings "
                "WHERE provider = ? AND api_key_enc != '' LIMIT 1",
                (p,),
            ).fetchone()
            con.close()
            if row:
                return {
                    "api_key":    _decode_cred(row["api_key_enc"]),
                    "api_secret": _decode_cred(row["api_secret_enc"]),
                }
        except Exception as exc:
            logger.error("ClientDB.get_platform_credentials_sync(%s): %s", p, exc)
        return None

    async def update_feeder_token(
        self,
        provider:     str,
        token:        str,
        generated_at: str = "",
        expiry_at:    str = "",
    ) -> None:
        """Persist a freshly-generated feeder access token."""
        logger.info(
            "[DB] update_feeder_token provider=%s token_len=%d generated_at=%s",
            provider, len(token), generated_at[:19] if generated_at else "now",
        )
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """UPDATE system_feeder_creds
               SET access_token=?, token_generated_at=?, token_expiry_at=?, updated_at=?
               WHERE provider=?""",
            (token, generated_at or now, expiry_at, now, provider),
        )

    # ── System settings (key-value) ───────────────────────────────────────────

    def get_setting_sync(self, key: str, default: str = "") -> str:
        """Read a system setting synchronously. Returns default if not set."""
        try:
            con = sqlite3.connect(self._db_path)
            row = con.execute(
                "SELECT value FROM system_settings WHERE key=?", (key,)
            ).fetchone()
            con.close()
            return row[0] if row else default
        except Exception as exc:
            logger.error("ClientDB.get_setting_sync(%s): %s", key, exc)
            return default

    async def set_setting(self, key: str, value: str) -> None:
        """Persist a system setting (upsert)."""
        logger.info("[DB] set_setting %s=%r", key, value[:80] if value else "")
        await asyncio.to_thread(
            self._exec,
            "INSERT INTO system_settings (key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    # ── Admin password (DB-stored, avoids server restart on change) ──────────────

    def get_admin_password_hash_sync(self) -> str:
        """Return stored admin password hash, or '' if never set."""
        return self.get_setting_sync("admin_password_hash", "")

    async def set_admin_password_hash(self, hashed: str) -> None:
        """Persist a new admin password hash."""
        await self.set_setting("admin_password_hash", hashed)

    # ── Password reset tokens ─────────────────────────────────────────────────────

    async def create_reset_token(self, target_role: str, target_id: str) -> str:
        """Generate a one-time reset token, store its hash, return plaintext token."""
        import secrets, hashlib
        from datetime import datetime, timedelta, timezone
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=24)
        await asyncio.to_thread(
            self._exec,
            "INSERT INTO password_resets (token_hash, target_role, target_id, created_at, expires_at, used) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            (token_hash, target_role, target_id, now.isoformat(), expires.isoformat()),
        )
        return token

    def consume_reset_token_sync(self, token: str) -> tuple[str, str] | None:
        """Validate token, mark used, return (target_role, target_id) or None if invalid/expired."""
        import hashlib
        from datetime import datetime, timezone
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT id, target_role, target_id, expires_at, used FROM password_resets "
                "WHERE token_hash=?",
                (token_hash,),
            ).fetchone()
            if row is None or row["used"]:
                con.close()
                return None
            expires = datetime.fromisoformat(row["expires_at"])
            if datetime.now(expires.tzinfo) > expires:
                con.close()
                return None
            con.execute("UPDATE password_resets SET used=1 WHERE id=?", (row["id"],))
            con.commit()
            con.close()
            return (row["target_role"], row["target_id"])
        except Exception as exc:
            logger.error("consume_reset_token_sync: %s", exc)
            return None

    # ── Batch straddle deployment query ──────────────────────────────────────────

    def get_running_straddle_deployments_sync(self) -> list[dict]:
        """Single JOIN: all is_running=1 sell_straddle deployments across active clients."""
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = con.execute(
                """
                SELECT c.client_id, d.binding_id, d.underlying, d.lot_multiplier,
                       d.strategy_name, d.is_running, d.assigned_instrument
                FROM clients c
                JOIN strategy_deployments d ON c.client_id = d.client_id
                WHERE c.is_active = 1
                  AND d.strategy_name = 'sell_straddle'
                  AND d.is_running = 1
                """
            ).fetchall()
            con.close()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("get_running_straddle_deployments_sync: %s", exc)
            return []

    # ── Boot-time bulk load ───────────────────────────────────────────────────

    def load_all_profiles(self) -> list:
        """
        Synchronous boot-time bulk load.
        Returns a list of ClientProfile objects for all admin-approved clients.
        Call at startup before the event loop starts.
        """
        from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding

        profiles = []
        for c in self.get_all_clients_sync():
            if not c.get("is_admin_approved"):
                continue
            strategies = [
                s.strip()
                for s in (c.get("enabled_strategies") or "A,B,C").split(",")
                if s.strip()
            ]
            risk = RiskProfile(
                capital=float(c.get("capital", 500_000)),
                max_risk_per_trade_pct=float(c.get("max_risk_pct", 1.0)),
                max_daily_loss_pct=float(c.get("max_daily_loss_pct", 3.0)),
                size_multiplier=float(c.get("lot_multiplier", 1.0)),
            )
            profile = ClientProfile(
                client_id=c["client_id"],
                name=c.get("name", ""),
                email=c.get("email", ""),
                risk=risk,
                enabled_strategies=strategies,
                active=bool(c.get("is_active", 1)),
                is_admin_approved=True,
                is_client_bot_active=bool(c.get("is_client_bot_active", 0)),
                target_index=c.get("target_index", "NIFTY"),
            )
            for b in self.get_bindings_sync(c["client_id"]):
                profile.broker_bindings.append(BrokerBinding(
                    binding_id=b["binding_id"],
                    provider=b["provider"],          # type: ignore[arg-type]
                    label=b.get("label", ""),
                    user_id=b.get("user_id", ""),
                    api_key=b.get("api_key", ""),
                    api_secret=b.get("api_secret", ""),
                    totp_secret=b.get("totp_secret", ""),
                    access_token=b.get("access_token", ""),
                    lot_multiplier=float(b.get("lot_multiplier", 1.0)),
                    enabled=bool(b.get("enabled", 1)),
                    assigned_strategy=b.get("assigned_strategy", ""),
                    is_trade_enabled=bool(b.get("is_trade_enabled", 1)),
                    token_generated_at=b.get("token_generated_at", ""),
                    token_expiry_at=b.get("token_expiry_at", ""),
                    product_type=b.get("product_type", "MIS"),
                    trading_mode=b.get("trading_mode", "paper"),
                    source_ip=b.get("source_ip", ""),
                ))
            profiles.append(profile)
            logger.info(
                "ClientDB: loaded profile %s (%d bindings).",
                profile.client_id, len(profile.broker_bindings),
            )
        return profiles

    # ── Iron Condor trade log ─────────────────────────────────────────────────

    def upsert_ic_trade_log(
        self, trade_id: str, underlying: str, event: str,
        short_ce_strike: float, short_pe_strike: float,
        long_ce_strike: float, long_pe_strike: float,
        net_credit: float, cumulative_adj_pnl: float, total_pnl_rs: float,
        adj_count_ce: int, adj_count_pe: int, status: str, timestamp: str,
    ) -> None:
        """Insert a trade log row for this IC trade event. Synchronous — call via to_thread."""
        con = sqlite3.connect(self._db_path)
        try:
            con.execute(
                """INSERT INTO ic_trade_log
                   (trade_id, underlying, event, short_ce_strike, short_pe_strike,
                    long_ce_strike, long_pe_strike, net_credit, cumulative_adj_pnl,
                    total_pnl_rs, adj_count_ce, adj_count_pe, status, timestamp,
                    is_active_adjustment)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (trade_id, underlying, event, short_ce_strike, short_pe_strike,
                 long_ce_strike, long_pe_strike, net_credit, cumulative_adj_pnl,
                 total_pnl_rs, adj_count_ce, adj_count_pe, status, timestamp,
                 1 if event == "ADJUST" else 0),
            )
            con.commit()
        except Exception as exc:
            import logging as _l
            _l.getLogger(__name__).error("upsert_ic_trade_log: %s", exc)
        finally:
            con.close()

    # ── 1-minute option bar repository ───────────────────────────────────────

    def upsert_1m_bar_sync(
        self,
        symbol: str,
        timestamp,          # datetime object
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        """
        INSERT OR REPLACE a 1-minute option premium candle.
        Called only via asyncio.to_thread() — never directly from async code.
        """
        ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        con = sqlite3.connect(self._db_path)
        try:
            con.execute(
                """
                INSERT INTO option_1m_bar_repository
                    (symbol, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timestamp) DO UPDATE SET
                    open   = excluded.open,
                    high   = excluded.high,
                    low    = excluded.low,
                    close  = excluded.close,
                    volume = excluded.volume
                """,
                (symbol, ts_str, open_, high, low, close, volume),
            )
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    async def upsert_1m_bar(
        self,
        symbol: str,
        timestamp,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        await asyncio.to_thread(
            self.upsert_1m_bar_sync,
            symbol, timestamp, open_, high, low, close, volume,
        )

    def get_1m_bars_sync(
        self,
        symbol: str,
        since,                      # datetime
        until=None,                 # datetime or None → now
    ) -> list:
        """
        Return list of row dicts for `symbol` in [since, until], ordered by timestamp ASC.
        Called via asyncio.to_thread() from the strategy engine.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        until_str = (until or datetime.now(IST)).strftime("%Y-%m-%dT%H:%M:%S")
        con = sqlite3.connect(self._db_path)
        try:
            cur = con.execute(
                """
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM option_1m_bar_repository
                WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (symbol, since_str, until_str),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            con.close()

    async def get_1m_bars(
        self,
        symbol: str,
        since,
        until=None,
    ) -> list:
        """Async wrapper — delegates to get_1m_bars_sync via asyncio.to_thread()."""
        return await asyncio.to_thread(self.get_1m_bars_sync, symbol, since, until)

    # ── SQLite helpers — only called from asyncio.to_thread() ─────────────────

    def _create_tables(self) -> None:
        con = sqlite3.connect(self._db_path)
        con.executescript(_DDL)
        # Additive migrations: add columns that may not exist in older DBs
        for migration in (
            "ALTER TABLE clients ADD COLUMN strategy_selections TEXT DEFAULT '[]'",
            "ALTER TABLE broker_bindings ADD COLUMN trading_mode TEXT DEFAULT 'paper'",
            "ALTER TABLE broker_bindings ADD COLUMN assigned_instrument TEXT DEFAULT 'NIFTY'",
            "ALTER TABLE broker_bindings ADD COLUMN terminal_connected INTEGER DEFAULT 0",
            "ALTER TABLE broker_bindings ADD COLUMN terminal_connected_at TEXT DEFAULT ''",
            "ALTER TABLE broker_bindings ADD COLUMN engine_active INTEGER DEFAULT 0",
            "ALTER TABLE clients ADD COLUMN trap_instruments TEXT DEFAULT '[]'",
            "ALTER TABLE broker_bindings ADD COLUMN product_type TEXT DEFAULT 'MIS'",
            "ALTER TABLE broker_bindings ADD COLUMN show_granular_ticks INTEGER DEFAULT 0",
            "ALTER TABLE broker_bindings ADD COLUMN source_ip TEXT DEFAULT ''",
            "ALTER TABLE broker_bindings ADD COLUMN whitelist_ip TEXT DEFAULT ''",
            "ALTER TABLE strategy_deployments ADD COLUMN is_running INTEGER DEFAULT 0",
        ):
            try:
                con.execute(migration)
                con.commit()
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e).lower():
                    pass   # idempotent migration — column already exists
                else:
                    raise

        # Security migration: drop password_enc / totp_secret_enc from broker_bindings
        existing_bb_cols = {row[1] for row in con.execute("PRAGMA table_info(broker_bindings)").fetchall()}
        if "password_enc" in existing_bb_cols or "totp_secret_enc" in existing_bb_cols:
            logger.info("ClientDB: migrating broker_bindings — dropping password/totp columns")
            try:
                con.executescript("""
                    BEGIN;
                    ALTER TABLE broker_bindings RENAME TO broker_bindings_old;
                    CREATE TABLE broker_bindings (
                        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                        client_id           TEXT    NOT NULL,
                        binding_id          TEXT    NOT NULL,
                        provider            TEXT    NOT NULL,
                        label               TEXT    DEFAULT '',
                        user_id_enc         TEXT    DEFAULT '',
                        api_key_enc         TEXT    DEFAULT '',
                        api_secret_enc      TEXT    DEFAULT '',
                        access_token        TEXT    DEFAULT '',
                        token_generated_at  TEXT    DEFAULT '',
                        token_expiry_at     TEXT    DEFAULT '',
                        assigned_strategy   TEXT    DEFAULT '',
                        assigned_instrument TEXT    DEFAULT 'NIFTY',
                        trading_mode        TEXT    DEFAULT 'paper',
                        is_trade_enabled    INTEGER DEFAULT 1,
                        lot_multiplier      REAL    DEFAULT 1.0,
                        enabled             INTEGER DEFAULT 1,
                        terminal_connected  INTEGER DEFAULT 0,
                        terminal_connected_at TEXT  DEFAULT '',
                        engine_active       INTEGER DEFAULT 0,
                        created_at          TEXT    NOT NULL,
                        UNIQUE(client_id, binding_id)
                    );
                    INSERT INTO broker_bindings
                        (id, client_id, binding_id, provider, label,
                         user_id_enc, api_key_enc, api_secret_enc,
                         access_token, token_generated_at, token_expiry_at,
                         assigned_strategy, assigned_instrument, trading_mode,
                         is_trade_enabled, lot_multiplier, enabled, created_at)
                    SELECT
                        id, client_id, binding_id, provider, label,
                        user_id_enc, api_key_enc, api_secret_enc,
                        access_token, token_generated_at, token_expiry_at,
                        assigned_strategy,
                        COALESCE(assigned_instrument, 'NIFTY'),
                        COALESCE(trading_mode, 'paper'),
                        is_trade_enabled, lot_multiplier, enabled, created_at
                    FROM broker_bindings_old;
                    DROP TABLE broker_bindings_old;
                    CREATE INDEX IF NOT EXISTS idx_bb_client ON broker_bindings(client_id);
                    COMMIT;
                """)
                logger.info("ClientDB: broker_bindings migration complete.")
            except Exception as exc:
                logger.error("ClientDB: broker_bindings migration FAILED: %s", exc)

        # Security migration: drop password_enc / totp_secret_enc from system_feeder_creds
        existing_fc_cols = {row[1] for row in con.execute("PRAGMA table_info(system_feeder_creds)").fetchall()}
        if "password_enc" in existing_fc_cols or "totp_secret_enc" in existing_fc_cols:
            logger.info("ClientDB: migrating system_feeder_creds — dropping password/totp columns")
            try:
                con.executescript("""
                    BEGIN;
                    ALTER TABLE system_feeder_creds RENAME TO system_feeder_creds_old;
                    CREATE TABLE system_feeder_creds (
                        provider           TEXT PRIMARY KEY,
                        client_id_enc      TEXT DEFAULT '',
                        api_key_enc        TEXT DEFAULT '',
                        secret_enc         TEXT DEFAULT '',
                        access_token       TEXT DEFAULT '',
                        token_generated_at TEXT DEFAULT '',
                        token_expiry_at    TEXT DEFAULT '',
                        updated_at         TEXT NOT NULL
                    );
                    INSERT INTO system_feeder_creds
                        (provider, client_id_enc, api_key_enc, secret_enc,
                         access_token, token_generated_at, token_expiry_at, updated_at)
                    SELECT
                        provider, client_id_enc, api_key_enc, secret_enc,
                        access_token, token_generated_at, token_expiry_at, updated_at
                    FROM system_feeder_creds_old;
                    DROP TABLE system_feeder_creds_old;
                    COMMIT;
                """)
                logger.info("ClientDB: system_feeder_creds migration complete.")
            except Exception as exc:
                logger.error("ClientDB: system_feeder_creds migration FAILED: %s", exc)

        con.close()

    def _exec(self, sql: str, params=()) -> None:
        con = sqlite3.connect(self._db_path)
        try:
            con.execute(sql, params)
            con.commit()
        except Exception as exc:
            logger.error("ClientDB._exec error: %s | %s", sql[:80], exc)
            raise
        finally:
            con.close()
