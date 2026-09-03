"""Test suite for scripts/seed_headless_creds.py"""
import asyncio
import sqlite3
from data_layer.client_db import ClientDB, _decode_cred
from scripts.seed_headless_creds import seed_from_answers


def test_seed_from_answers_writes_upstox_zerodha_fyers(tmp_path):
    """Verify seed_from_answers writes all three provider types correctly."""
    db = ClientDB(str(tmp_path / "clients.db"))

    async def setup():
        await db.initialise()
        # Create a client first
        await db.register_client(client_id="ssrajpal2001", name="Test User", password="x", capital=100000)
        # Create a binding
        await db.upsert_binding(client_id="ssrajpal2001", binding_id="SA5770", provider="zerodha")

    asyncio.run(setup())

    answers = {
        "upstox": {
            "client_id": "UP1",
            "api_key": "upk",
            "secret": "ups",
            "password": "111111",
            "totp_secret": "JBSWY3DPEHPK3PXP",
        },
        "fyers": {
            "client_id": "FY1",
            "api_key": "fyk",
            "secret": "fys",
            "password": "2222",
            "totp_secret": "JBSWY3DPEHPK3PXP",
        },
        "zerodha_binding": {
            "client_id": "ssrajpal2001",
            "binding_id": "SA5770",
            "password": "zpw",
            "totp_secret": "JBSWY3DPEHPK3PXP",
        },
    }

    seed_from_answers(db, answers)

    # Verify Upstox feeder creds
    up = db.get_feeder_creds_sync("upstox")
    assert up["password"] == "111111"
    assert up["totp_secret"] == "JBSWY3DPEHPK3PXP"

    # Verify Fyers feeder creds
    fy = db.get_feeder_creds_sync("fyers")
    assert fy["password"] == "2222"
    assert fy["totp_secret"] == "JBSWY3DPEHPK3PXP"

    # Verify Zerodha binding password/TOTP via direct DB read (since get_bindings_safe_sync strips secrets)
    con = sqlite3.connect(str(tmp_path / "clients.db"))
    row = con.execute(
        "SELECT password_enc, totp_secret_enc FROM broker_bindings WHERE binding_id='SA5770'"
    ).fetchone()
    con.close()

    assert _decode_cred(row[0]) == "zpw"
    assert _decode_cred(row[1]) == "JBSWY3DPEHPK3PXP"
