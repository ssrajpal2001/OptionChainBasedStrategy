# tests/test_password_reset.py
import pytest, asyncio, tempfile, sqlite3
from datetime import datetime, timedelta, timezone
from data_layer.client_db import ClientDB

def make_db(tmp_path):
    db = ClientDB(str(tmp_path / "t.db"))
    asyncio.run(db.initialise())
    return db

def test_create_and_consume(tmp_path):
    db = make_db(tmp_path)
    token = asyncio.run(db.create_reset_token("client", "bob"))
    result = db.consume_reset_token_sync(token)
    assert result == ("client", "bob")

def test_expired_token_rejected(tmp_path):
    db = make_db(tmp_path)
    token = asyncio.run(db.create_reset_token("client", "bob"))
    # Manually expire it
    con = sqlite3.connect(str(tmp_path / "t.db"))
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    con.execute("UPDATE password_resets SET expires_at=?", (past,))
    con.commit(); con.close()
    result = db.consume_reset_token_sync(token)
    assert result is None

def test_unknown_token_rejected(tmp_path):
    db = make_db(tmp_path)
    assert db.consume_reset_token_sync("garbage_token") is None
