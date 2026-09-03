"""
Tests for utils/email_alert.py — Gmail SMTP alerting.
"""
from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

from utils.email_alert import send_summary_email, format_summary_body
from data_layer.client_db import ClientDB, _encode_cred, _decode_cred


def test_format_summary_body_shows_all_steps_with_status():
    body = format_summary_body([
        ("pm2 start", True, ""),
        ("Upstox login", True, "token generated 09:14:52 IST"),
        ("Fyers login", False, "Cloudflare challenge page"),
    ])
    assert "pm2 start" in body and "OK" in body
    assert "Fyers login" in body and "FAILED" in body
    assert "Cloudflare challenge page" in body


def test_send_summary_email_returns_false_and_does_not_raise_on_smtp_error(monkeypatch):
    import utils.email_alert as mod

    class BoomSMTP:
        def __init__(self, *a, **k):
            raise ConnectionRefusedError("smtp down")

    monkeypatch.setattr(mod.smtplib, "SMTP_SSL", BoomSMTP)
    monkeypatch.setattr(mod, "_get_gmail_credentials", lambda: ("bot@gmail.com", "app-pw"))

    ok = send_summary_email("user@example.com", "subject", [("step", True, "")])
    assert ok is False


def test_send_summary_email_sends_via_smtp_ssl(monkeypatch):
    import utils.email_alert as mod
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port):
            sent["host"] = host
            sent["port"] = port

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def login(self, user, pw):
            sent["user"] = user
            sent["pw"] = pw

        def send_message(self, msg):
            sent["msg"] = msg

    monkeypatch.setattr(mod.smtplib, "SMTP_SSL", FakeSMTP)
    monkeypatch.setattr(mod, "_get_gmail_credentials", lambda: ("bot@gmail.com", "app-pw"))

    ok = send_summary_email("user@example.com", "subject", [("step", True, "detail")])
    assert ok is True
    assert sent["user"] == "bot@gmail.com"
    assert sent["pw"] == "app-pw"
    assert sent["msg"]["To"] == "user@example.com"
    assert sent["msg"]["Subject"] == "subject"


# ── Integration tests for credential obfuscation ──────────────────────────────

def test_set_setting_sync_obfuscates_value():
    """Verify set_setting_sync stores the value encoded in the DB."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = ClientDB(db_path=str(db_path))

        # Manually initialize the DB schema so system_settings table exists
        con = sqlite3.connect(str(db_path))
        con.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        con.close()

        # Store a plaintext app password
        plaintext_pw = "my-secret-app-password-123"
        asyncio.run(db.set_setting_sync("test_secret", plaintext_pw))

        # Read directly from DB (circumvent the getter to see the raw stored value)
        con = sqlite3.connect(str(db_path))
        row = con.execute(
            "SELECT value FROM system_settings WHERE key=?", ("test_secret",)
        ).fetchone()
        con.close()

        stored_value = row[0] if row else None
        assert stored_value is not None, "Value was not stored"

        # Verify the stored value is NOT the plaintext (obfuscated)
        assert stored_value != plaintext_pw, (
            f"Password stored in plaintext! {stored_value} == {plaintext_pw}"
        )

        # Verify the stored value CAN be decoded back to the original
        decoded = _decode_cred(stored_value)
        assert decoded == plaintext_pw, (
            f"Decoded value does not match original: {decoded} != {plaintext_pw}"
        )


def test_gmail_credentials_round_trip():
    """Verify set_setting_sync + _get_gmail_credentials round-trip correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = ClientDB(db_path=str(db_path))

        # Initialize DB
        con = sqlite3.connect(str(db_path))
        con.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        con.close()

        # Store Gmail credentials
        gmail_user = "bot@example.com"
        gmail_app_pw = "xyzabc-secret-app-pw"

        asyncio.run(db.set_setting_sync("auto_alert_gmail_user", gmail_user))
        asyncio.run(db.set_setting_sync("auto_alert_gmail_app_password", gmail_app_pw))

        # Verify retrieval via get_setting_sync returns obfuscated in raw form
        raw_pw = db.get_setting_sync("auto_alert_gmail_app_password", "")
        assert raw_pw != gmail_app_pw, (
            f"Raw stored value should be obfuscated, but got plaintext: {raw_pw}"
        )

        # Verify decoding the stored value recovers the original
        decoded_pw = _decode_cred(raw_pw)
        assert decoded_pw == gmail_app_pw, (
            f"Decoded password does not match original: {decoded_pw} != {gmail_app_pw}"
        )


def test_seed_headless_creds_then_get_gmail_credentials_real_integration(tmp_path, monkeypatch):
    """
    Exercises the REAL, un-monkeypatched seam between
    scripts.seed_headless_creds.seed_from_answers() (how Gmail creds actually
    get written in production) and utils.email_alert._get_gmail_credentials()
    (how they actually get read back) -- against a real ClientDB backed by a
    tmp_path SQLite file. This is the exact integration gap that let the
    original "reads user raw/undecoded" bug ship silently, since every other
    test in this file monkeypatches _get_gmail_credentials directly and so
    never exercises its own body.
    """
    import data_layer.client_db as client_db_mod
    import utils.email_alert as email_alert_mod
    from scripts.seed_headless_creds import seed_from_answers

    db_path = tmp_path / "clients.db"
    db = ClientDB(db_path=str(db_path))
    asyncio.run(db.initialise())

    gmail_user = "realbot@gmail.com"
    gmail_app_pw = "real-secret-app-password-456"

    seed_from_answers(db, {"gmail": {"user": gmail_user, "app_password": gmail_app_pw}})

    # _get_gmail_credentials() constructs its own ClientDB() (default db path)
    # internally -- redirect that construction at the module level so it
    # resolves to the SAME tmp_path-backed db seed_from_answers just wrote to,
    # without touching _get_gmail_credentials itself.
    monkeypatch.setattr(client_db_mod, "ClientDB", lambda *a, **k: db)

    user, app_pw = email_alert_mod._get_gmail_credentials()

    assert user == gmail_user, (
        f"_get_gmail_credentials() returned {user!r} -- expected the decoded "
        f"plaintext Gmail address {gmail_user!r}, not the raw/encoded DB value"
    )
    assert app_pw == gmail_app_pw
