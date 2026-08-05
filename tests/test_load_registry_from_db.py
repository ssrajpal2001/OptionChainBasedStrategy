"""
Regression test for the 2026-08-05 live incident: a stale config/client_profiles.json
snapshot (written by any dashboard endpoint that calls registry.save(), which never
persists broker_bindings) caused _setup_live_clients() to skip _load_registry_from_db()
entirely for every already-known client, leaving ExecutionRouter with zero brokers for
ALL clients/strategies on the next restart.

_load_registry_from_db() must always refresh broker_bindings from the DB for a client
that's already present in the registry (e.g. loaded from the JSON snapshot first),
not just for brand-new clients.
"""
import os
import sqlite3

from config.client_profiles import ClientProfile, ClientRegistry
from run_system import _load_registry_from_db


def _make_db(path: str) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE clients (
            client_id TEXT PRIMARY KEY, name TEXT, email TEXT, capital REAL,
            max_risk_pct REAL, max_daily_loss_pct REAL, is_admin_approved INTEGER,
            is_client_bot_active INTEGER, target_index TEXT, is_active INTEGER,
            created_at TEXT, updated_at TEXT
        );
        CREATE TABLE broker_bindings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_id TEXT, binding_id TEXT,
            provider TEXT, label TEXT, user_id_enc TEXT, api_key_enc TEXT,
            api_secret_enc TEXT, access_token TEXT, trading_mode TEXT,
            assigned_strategy TEXT, is_trade_enabled INTEGER, lot_multiplier REAL,
            product_type TEXT, password_enc TEXT, totp_secret_enc TEXT,
            source_ip TEXT, enabled INTEGER, created_at TEXT
        );
        """
    )
    con.execute(
        "INSERT INTO clients (client_id, name, email, capital, max_risk_pct, "
        "max_daily_loss_pct, is_admin_approved, is_client_bot_active, target_index, "
        "is_active, created_at, updated_at) VALUES "
        "('ssrajpal2001','Test','t@x.com',500000,1.0,3.0,1,1,'NIFTY',1,'now','now')"
    )
    con.execute(
        "INSERT INTO broker_bindings (client_id, binding_id, provider, label, "
        "trading_mode, assigned_strategy, is_trade_enabled, lot_multiplier, "
        "product_type, enabled, created_at) VALUES "
        "('ssrajpal2001','SA5770','zerodha','','live','',1,1.0,'NRML',1,'now')"
    )
    con.commit()
    con.close()


def test_refreshes_broker_bindings_for_client_already_in_registry(tmp_path, monkeypatch):
    db_path = tmp_path / "clients.db"
    _make_db(str(db_path))
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)
    os.replace(str(db_path), os.path.join("data", "clients.db"))

    registry = ClientRegistry()
    # Simulate the JSON-snapshot path: client already registered, zero broker_bindings
    # (exactly what config/client_profiles.json produces — it never carries bindings).
    registry.register(ClientProfile(client_id="ssrajpal2001", broker_bindings=[]))
    assert registry.get("ssrajpal2001").enabled_brokers() == []

    _load_registry_from_db(registry)

    profile = registry.get("ssrajpal2001")
    assert len(profile.enabled_brokers()) == 1
    assert profile.enabled_brokers()[0].binding_id == "SA5770"


def test_registers_new_client_not_already_present(tmp_path, monkeypatch):
    db_path = tmp_path / "clients.db"
    _make_db(str(db_path))
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)
    os.replace(str(db_path), os.path.join("data", "clients.db"))

    registry = ClientRegistry()
    assert registry.get("ssrajpal2001") is None

    _load_registry_from_db(registry)

    profile = registry.get("ssrajpal2001")
    assert profile is not None
    assert len(profile.enabled_brokers()) == 1
