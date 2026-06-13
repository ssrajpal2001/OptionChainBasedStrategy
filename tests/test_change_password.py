# tests/test_change_password.py
import pytest, asyncio, tempfile
from data_layer.client_db import ClientDB, hash_password, verify_password

def test_admin_password_hash_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        db = ClientDB(f"{d}/test.db")
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(db.initialise())
            assert db.get_admin_password_hash_sync() == ""
            h = hash_password("newStrongPwd!")
            loop.run_until_complete(db.set_admin_password_hash(h))
            stored = db.get_admin_password_hash_sync()
            assert verify_password("newStrongPwd!", stored)
            assert not verify_password("wrongpwd", stored)
        finally:
            loop.close()
