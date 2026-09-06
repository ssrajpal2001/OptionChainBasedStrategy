# tests/data_layer/test_client_db_additions.py
import pytest, asyncio, tempfile, os
from data_layer.client_db import ClientDB

@pytest.fixture
def db(tmp_path):
    instance = ClientDB(str(tmp_path / "test.db"))
    # asyncio.get_event_loop() raises RuntimeError once any earlier test in the
    # same session has called asyncio.run() (which explicitly unsets the
    # thread's event loop on completion) -- asyncio.run() is the correct,
    # loop-lifecycle-safe replacement, matching the pattern already used
    # throughout the rest of this test suite.
    asyncio.run(instance.initialise())
    return instance

def test_get_running_straddle_deployments_empty(db):
    rows = db.get_running_straddle_deployments_sync()
    assert rows == []


def test_deleted_deployment_never_shows_as_running(db):
    """2026-09-07 real incident: get_running_straddle_deployments_sync() only
    checked c.is_active (the client's), never d.is_active (the deployment's own).
    A deployment deleted via the dashboard (is_active=0) whose is_running was
    still 1 kept spawning and trading a real book forever, invisible in the
    dashboard's own deployment list (which correctly filters is_active=1).
    Found live: ssrajpal2001_UPSTOX_sell_straddle_NIFTY."""
    asyncio.run(db.register_client("C1", "Test Client", "pw"))
    deploy_id = asyncio.run(db.save_deployment(
        "C1", "Z1", "sell_straddle", "NIFTY",
        lot_multiplier=1, max_profit_rs=0, max_sl_rs=0, squareoff_time="15:15",
    ))
    asyncio.run(db.set_deployment_running(deploy_id, "C1", True))
    rows = db.get_running_straddle_deployments_sync()
    assert len(rows) == 1 and rows[0]["binding_id"] == "Z1"

    # Simulate the real incident: delete via the dashboard's "✕ Delete" action.
    asyncio.run(db.delete_deployment(deploy_id, "C1"))
    rows = db.get_running_straddle_deployments_sync()
    assert rows == [], "a deleted deployment must never keep spawning a book"

    # delete_deployment() also force-clears is_running=0 (belt-and-suspenders,
    # so no other is_running=1 query anywhere can be fooled by this state either).
    con = db._db_path
    import sqlite3
    raw = sqlite3.connect(con).execute(
        "SELECT is_active, is_running FROM strategy_deployments WHERE deploy_id=?",
        (deploy_id,),
    ).fetchone()
    assert raw == (0, 0)

def test_admin_password_hash_roundtrip(db):
    assert db.get_admin_password_hash_sync() == ""
    asyncio.run(
        db.set_admin_password_hash("salt:hash_value")
    )
    assert db.get_admin_password_hash_sync() == "salt:hash_value"

def test_create_and_consume_reset_token(db):
    token = asyncio.run(
        db.create_reset_token("client", "alice")
    )
    assert len(token) > 20
    result = db.consume_reset_token_sync(token)
    assert result == ("client", "alice")

def test_consume_token_twice_fails(db):
    token = asyncio.run(
        db.create_reset_token("client", "alice")
    )
    db.consume_reset_token_sync(token)
    result = db.consume_reset_token_sync(token)
    assert result is None

def test_consume_bad_token_fails(db):
    result = db.consume_reset_token_sync("notavalidtoken")
    assert result is None
