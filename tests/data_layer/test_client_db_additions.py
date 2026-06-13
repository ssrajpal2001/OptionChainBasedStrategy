# tests/data_layer/test_client_db_additions.py
import pytest, asyncio, tempfile, os
from data_layer.client_db import ClientDB

@pytest.fixture
def db(tmp_path):
    instance = ClientDB(str(tmp_path / "test.db"))
    asyncio.get_event_loop().run_until_complete(instance.initialise())
    return instance

def test_get_running_straddle_deployments_empty(db):
    rows = db.get_running_straddle_deployments_sync()
    assert rows == []

def test_admin_password_hash_roundtrip(db):
    assert db.get_admin_password_hash_sync() == ""
    asyncio.get_event_loop().run_until_complete(
        db.set_admin_password_hash("salt:hash_value")
    )
    assert db.get_admin_password_hash_sync() == "salt:hash_value"

def test_create_and_consume_reset_token(db):
    token = asyncio.get_event_loop().run_until_complete(
        db.create_reset_token("client", "alice")
    )
    assert len(token) > 20
    result = db.consume_reset_token_sync(token)
    assert result == ("client", "alice")

def test_consume_token_twice_fails(db):
    token = asyncio.get_event_loop().run_until_complete(
        db.create_reset_token("client", "alice")
    )
    db.consume_reset_token_sync(token)
    result = db.consume_reset_token_sync(token)
    assert result is None

def test_consume_bad_token_fails(db):
    result = db.consume_reset_token_sync("notavalidtoken")
    assert result is None
