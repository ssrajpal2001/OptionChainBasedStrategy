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
    is_trade_enabled    INTEGER DEFAULT 1,
    lot_multiplier      REAL    DEFAULT 1.0,
    enabled             INTEGER DEFAULT 1,
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

    async def initialise(self) -> None:
        """Create tables and indexes. Safe to call on every boot."""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
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
        assigned_strategy: str = "",
        assigned_instrument: str = "NIFTY",
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
                access_token, lot_multiplier, trading_mode,
                assigned_strategy, assigned_instrument, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(client_id, binding_id) DO UPDATE SET
                 provider            = excluded.provider,
                 label               = excluded.label,
                 user_id_enc         = CASE WHEN excluded.user_id_enc  != '' THEN excluded.user_id_enc  ELSE user_id_enc  END,
                 api_key_enc         = CASE WHEN excluded.api_key_enc  != '' THEN excluded.api_key_enc  ELSE api_key_enc  END,
                 api_secret_enc      = CASE WHEN excluded.api_secret_enc != '' THEN excluded.api_secret_enc ELSE api_secret_enc END,
                 access_token        = CASE WHEN excluded.access_token  != '' THEN excluded.access_token  ELSE access_token  END,
                 lot_multiplier      = excluded.lot_multiplier,
                 trading_mode        = excluded.trading_mode,
                 assigned_strategy   = CASE WHEN excluded.assigned_strategy != '' THEN excluded.assigned_strategy ELSE assigned_strategy END,
                 assigned_instrument = CASE WHEN excluded.assigned_strategy != '' THEN excluded.assigned_instrument ELSE assigned_instrument END""",
            (
                client_id, binding_id, provider, label,
                _encode_cred(user_id),
                _encode_cred(api_key),
                _encode_cred(api_secret),
                access_token,
                lot_multiplier,
                trading_mode,
                assigned_strategy,
                assigned_instrument,
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
        """Mark trading engine as active/inactive for this broker."""
        logger.info("[DB] set_engine_active [%s/%s] → %s", client_id, binding_id, active)
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET engine_active=?, is_trade_enabled=? "
            "WHERE client_id=? AND binding_id=?",
            (1 if active else 0, 1 if active else 0, client_id, binding_id),
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
        deploy_id = f"{client_id}_{binding_id}_{strategy_name}"
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
        """Return bindings WITHOUT credentials — safe for API responses."""
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            rows = [dict(r) for r in
                    con.execute(
                        """SELECT binding_id, provider, label, access_token,
                                  token_generated_at, token_expiry_at,
                                  assigned_strategy, assigned_instrument, trading_mode,
                                  is_trade_enabled, lot_multiplier, enabled,
                                  terminal_connected, terminal_connected_at, engine_active
                           FROM broker_bindings WHERE client_id=? ORDER BY created_at""",
                        (client_id,),
                    ).fetchall()]
            con.close()
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_bindings_safe_sync(%s): %s", client_id, exc)
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
                ))
            profiles.append(profile)
            logger.info(
                "ClientDB: loaded profile %s (%d bindings).",
                profile.client_id, len(profile.broker_bindings),
            )
        return profiles

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
        ):
            try:
                con.execute(migration)
                con.commit()
            except Exception:
                pass  # column already exists

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
