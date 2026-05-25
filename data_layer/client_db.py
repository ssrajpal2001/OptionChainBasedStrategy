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
CREATE TABLE IF NOT EXISTS clients (
    client_id               TEXT    PRIMARY KEY,
    name                    TEXT    DEFAULT '',
    email                   TEXT    DEFAULT '',
    password_hash           TEXT    DEFAULT '',
    capital                 REAL    DEFAULT 500000,
    max_risk_pct            REAL    DEFAULT 1.0,
    max_daily_loss_pct      REAL    DEFAULT 3.0,
    lot_multiplier          REAL    DEFAULT 1.0,
    enabled_strategies      TEXT    DEFAULT 'A,B,C',
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
    totp_secret_enc     TEXT    DEFAULT '',
    access_token        TEXT    DEFAULT '',
    token_generated_at  TEXT    DEFAULT '',
    token_expiry_at     TEXT    DEFAULT '',
    assigned_strategy   TEXT    DEFAULT '',
    is_trade_enabled    INTEGER DEFAULT 1,
    lot_multiplier      REAL    DEFAULT 1.0,
    enabled             INTEGER DEFAULT 1,
    created_at          TEXT    NOT NULL,
    UNIQUE(client_id, binding_id)
);

CREATE INDEX IF NOT EXISTS idx_clients_approved ON clients(is_admin_approved);
CREATE INDEX IF NOT EXISTS idx_bb_client        ON broker_bindings(client_id);
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
        totp_secret: str = "",
        access_token: str = "",
        lot_multiplier: float = 1.0,
    ) -> None:
        """Insert or update a broker binding, encrypting credentials."""
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO broker_bindings
               (client_id, binding_id, provider, label,
                user_id_enc, api_key_enc, api_secret_enc, totp_secret_enc,
                access_token, lot_multiplier, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(client_id, binding_id) DO UPDATE SET
                 provider        = excluded.provider,
                 label           = excluded.label,
                 user_id_enc     = excluded.user_id_enc,
                 api_key_enc     = excluded.api_key_enc,
                 api_secret_enc  = excluded.api_secret_enc,
                 totp_secret_enc = excluded.totp_secret_enc,
                 access_token    = excluded.access_token,
                 lot_multiplier  = excluded.lot_multiplier""",
            (
                client_id, binding_id, provider, label,
                _encode_cred(user_id),
                _encode_cred(api_key),
                _encode_cred(api_secret),
                _encode_cred(totp_secret),
                access_token,
                lot_multiplier,
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
                r["user_id"]     = _decode_cred(r.pop("user_id_enc", ""))
                r["api_key"]     = _decode_cred(r.pop("api_key_enc", ""))
                r["api_secret"]  = _decode_cred(r.pop("api_secret_enc", ""))
                r["totp_secret"] = _decode_cred(r.pop("totp_secret_enc", ""))
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
                                  assigned_strategy, is_trade_enabled,
                                  lot_multiplier, enabled
                           FROM broker_bindings WHERE client_id=? ORDER BY created_at""",
                        (client_id,),
                    ).fetchall()]
            con.close()
            return rows
        except Exception as exc:
            logger.error("ClientDB.get_bindings_safe_sync(%s): %s", client_id, exc)
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
        con.commit()
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
