import asyncio
import sqlite3
import pytest
from data_layer.client_db import ClientDB


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_clients.db")


def _new_db(db_path):
    db = ClientDB(db_path)
    asyncio.run(db.initialise())
    return db


def test_system_feeder_creds_has_password_and_totp_columns(db_path):
    _new_db(db_path)
    con = sqlite3.connect(db_path)
    cols = {row[1] for row in con.execute("PRAGMA table_info(system_feeder_creds)").fetchall()}
    con.close()
    assert "password_enc" in cols
    assert "totp_secret_enc" in cols


def test_old_drop_migration_no_longer_wipes_the_columns_on_reopen(db_path):
    # Simulates a second process boot (e.g. a restart) re-running the migration
    # logic against an already-migrated DB -- must NOT re-drop the columns.
    _new_db(db_path)
    _new_db(db_path)
    con = sqlite3.connect(db_path)
    cols = {row[1] for row in con.execute("PRAGMA table_info(system_feeder_creds)").fetchall()}
    con.close()
    assert "password_enc" in cols
    assert "totp_secret_enc" in cols


def test_upsert_and_get_feeder_creds_round_trips_password_and_totp(db_path):
    db = _new_db(db_path)
    asyncio.run(db.upsert_feeder_creds(
        provider="upstox", client_id="UP123", api_key="key1", secret="sec1",
        password="123456", totp_secret="JBSWY3DPEHPK3PXP",
    ))
    creds = db.get_feeder_creds_sync("upstox")
    assert creds["password"] == "123456"
    assert creds["totp_secret"] == "JBSWY3DPEHPK3PXP"
    assert creds["api_key"] == "key1"


def test_upsert_feeder_creds_preserves_existing_password_when_omitted(db_path):
    db = _new_db(db_path)
    asyncio.run(db.upsert_feeder_creds(
        provider="upstox", client_id="UP123", api_key="key1", secret="sec1",
        password="123456", totp_secret="JBSWY3DPEHPK3PXP",
    ))
    # Re-upsert without password/totp (e.g. admin UI updating just api_key)
    asyncio.run(db.upsert_feeder_creds(provider="upstox", api_key="key2"))
    creds = db.get_feeder_creds_sync("upstox")
    assert creds["api_key"] == "key2"
    assert creds["password"] == "123456"
    assert creds["totp_secret"] == "JBSWY3DPEHPK3PXP"
