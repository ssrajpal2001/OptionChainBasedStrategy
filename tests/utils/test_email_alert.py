from utils.email_alert import send_summary_email, format_summary_body


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


def test_seed_and_read_gmail_credentials_round_trip(tmp_path):
    import asyncio
    from data_layer.client_db import ClientDB
    from utils.email_alert import seed_gmail_credentials, _get_gmail_credentials

    db_path = str(tmp_path / "clients.db")
    db = ClientDB(db_path)
    asyncio.run(db.initialise())
    asyncio.run(seed_gmail_credentials(db, "bot@gmail.com", "app-pw-secret"))

    # _get_gmail_credentials constructs its own ClientDB() (default path) via
    # a local `from data_layer.client_db import ClientDB` at call time -- patch
    # the class in its home module so that pickup resolves to our tmp DB.
    # (Patching the _DEFAULT_DB_PATH module constant would NOT work here: it's
    # a function-default value bound once at def-time, not re-read per call.)
    import data_layer.client_db as cdb_mod
    RealClientDB = cdb_mod.ClientDB
    import unittest.mock as _mock
    with _mock.patch.object(cdb_mod, "ClientDB", lambda *a, **kw: RealClientDB(db_path)):
        user, app_pw = _get_gmail_credentials()

    assert user == "bot@gmail.com"
    assert app_pw == "app-pw-secret"
