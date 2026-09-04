import asyncio
import sqlite3

from data_layer.client_db import ClientDB, _decode_cred
from scripts.seed_headless_creds import seed_from_answers


def _new_db(db_path):
    db = ClientDB(db_path)
    asyncio.run(db.initialise())
    return db


def test_seed_from_answers_writes_upstox_zerodha_fyers(tmp_path):
    db_path = str(tmp_path / "clients.db")
    db = _new_db(db_path)
    asyncio.run(db.register_client("ssrajpal2001", "SS", "pw"))
    asyncio.run(db.upsert_binding("ssrajpal2001", "SA5770", "zerodha"))

    answers = {
        "upstox": {"client_id": "UP1", "api_key": "upk", "secret": "ups",
                    "password": "111111", "totp_secret": "JBSWY3DPEHPK3PXP"},
        "fyers": {"client_id": "FY1", "api_key": "fyk", "secret": "fys",
                   "password": "2222", "totp_secret": "JBSWY3DPEHPK3PXP"},
        "zerodha_binding": {"client_id": "ssrajpal2001", "binding_id": "SA5770",
                              "password": "zpw", "totp_secret": "JBSWY3DPEHPK3PXP"},
    }
    seed_from_answers(db, answers)

    up = db.get_feeder_creds_sync("upstox")
    assert up["password"] == "111111" and up["totp_secret"] == "JBSWY3DPEHPK3PXP"

    fy = db.get_feeder_creds_sync("fyers")
    assert fy["password"] == "2222"

    con = sqlite3.connect(db_path)
    row = con.execute(
        "SELECT password_enc, totp_secret_enc, trading_mode FROM broker_bindings WHERE binding_id='SA5770'"
    ).fetchone()
    con.close()
    assert _decode_cred(row[0]) == "zpw"
    assert _decode_cred(row[1]) == "JBSWY3DPEHPK3PXP"
    # trading_mode must survive untouched -- proves set_binding_password_totp
    # is a narrow update, not a full upsert_binding() call that would reset it.
    assert row[2] == "paper"


def test_seed_from_answers_skips_omitted_providers(tmp_path):
    db_path = str(tmp_path / "clients.db")
    db = _new_db(db_path)
    seed_from_answers(db, {})
    assert db.get_feeder_creds_sync("upstox") is None
    assert db.get_feeder_creds_sync("fyers") is None


def test_seed_zerodha_binding_also_stores_user_id(tmp_path):
    """2026-09-06 real incident regression: Zerodha's headless login failed
    "user_id ... required" on first live use because the OAuth flow never
    needed this field and nothing ever collected it. set_binding_password_totp
    now optionally accepts user_id too -- must round-trip through seeding."""
    db_path = str(tmp_path / "clients.db")
    db = _new_db(db_path)
    asyncio.run(db.register_client("ssrajpal2001", "SS", "pw"))
    asyncio.run(db.upsert_binding("ssrajpal2001", "SA5770", "zerodha"))

    seed_from_answers(db, {
        "zerodha_binding": {"client_id": "ssrajpal2001", "binding_id": "SA5770",
                              "user_id": "AB1234", "password": "zpw",
                              "totp_secret": "JBSWY3DPEHPK3PXP"},
    })

    bindings = db.get_bindings_sync("ssrajpal2001")
    sa = next(b for b in bindings if b["binding_id"] == "SA5770")
    assert sa["user_id"] == "AB1234"
    assert sa["password"] == "zpw"
