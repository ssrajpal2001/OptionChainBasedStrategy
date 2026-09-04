import pytest
from broker_auth.headless_totp_auth import HeadlessTotpAuthError, zerodha_totp_login


def test_zerodha_missing_required_field_raises():
    with pytest.raises(HeadlessTotpAuthError, match="required"):
        zerodha_totp_login(api_key="", api_secret="s", user_id="u",
                            password="p", totp_secret="JBSWY3DPEHPK3PXP")


def test_zerodha_missing_totp_secret_raises():
    with pytest.raises(HeadlessTotpAuthError, match="totp_secret"):
        zerodha_totp_login(api_key="k", api_secret="s", user_id="u", password="p", totp_secret="")


def test_zerodha_wrong_password_raises(monkeypatch):
    class FakeResp:
        def __init__(self, body, status_code=200, url=""):
            self._body = body
            self.status_code = status_code
            self.url = url

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, **k):
            return FakeResp({}, url=url)

        def post(self, url, **k):
            if "api/login" in url:
                return FakeResp({"status": "error", "message": "Invalid password"})
            raise AssertionError(f"unexpected POST {url}")

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_zerodha_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(HeadlessTotpAuthError, match="Invalid password"):
        zerodha_totp_login(api_key="k", api_secret="s", user_id="u",
                            password="wrong", totp_secret="JBSWY3DPEHPK3PXP")


def test_zerodha_full_success_path(monkeypatch):
    class FakeResp:
        def __init__(self, body, status_code=200, url=""):
            self._body = body
            self.status_code = status_code
            self.url = url

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, **k):
            if "connect/login" in url:
                # second call (post-2FA) redirects with request_token
                if getattr(self, "_authed", False):
                    return FakeResp({}, url="https://cb/?request_token=REQTOK123&action=login&status=success")
                self._authed = False
                return FakeResp({}, url="https://kite.zerodha.com/connect/login")
            raise AssertionError(f"unexpected GET {url}")

        def post(self, url, **k):
            if "api/login" in url:
                return FakeResp({"status": "success", "data": {"request_id": "REQ123"}})
            if "api/twofa" in url:
                self._authed = True
                return FakeResp({"status": "success", "data": {}})
            if "session/token" in url:
                return FakeResp({"status": "success", "data": {"access_token": "ZTOKEN123"}})
            raise AssertionError(f"unexpected POST {url}")

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_zerodha_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)

    token = zerodha_totp_login(api_key="k", api_secret="s", user_id="AB1234",
                                password="pw", totp_secret="JBSWY3DPEHPK3PXP")
    assert token == "ZTOKEN123"
