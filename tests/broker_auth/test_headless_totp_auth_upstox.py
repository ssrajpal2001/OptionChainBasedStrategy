import pytest
from broker_auth.headless_totp_auth import _mask, HeadlessTotpAuthError, upstox_totp_login


def test_mask_short_string():
    assert _mask("ab") == "****"


def test_mask_long_string():
    assert _mask("ABCDEFGH") == "ABCD****"


def test_upstox_missing_api_key_raises():
    with pytest.raises(HeadlessTotpAuthError, match="api_key"):
        upstox_totp_login(api_key="", api_secret="s", user_id="u",
                           password="123456", totp_secret="JBSWY3DPEHPK3PXP")


def test_upstox_missing_totp_secret_raises():
    with pytest.raises(HeadlessTotpAuthError, match="totp_secret"):
        upstox_totp_login(api_key="k", api_secret="s", user_id="u",
                           password="123456", totp_secret="")


def test_upstox_missing_redirect_uri_raises():
    """2026-09-06 real incident: a hardcoded default redirect_uri
    ("https://www.google.com") produced a real Upstox rejection
    ("client_id and redirect_uri ... incorrect") once it stopped matching
    what's actually registered for the app -- no default is provided
    anymore, so a caller that forgets to resolve the real one (from
    system_settings.GLOBAL_REDIRECT_BASE) gets a clear error here instead."""
    with pytest.raises(HeadlessTotpAuthError, match="redirect_uri"):
        upstox_totp_login(api_key="k", api_secret="s", user_id="u",
                           password="123456", totp_secret="JBSWY3DPEHPK3PXP")


def test_upstox_step1_no_redirect_error_includes_status_and_body(monkeypatch):
    """2026-09-06 real incident: Step 1 came back with the request URL
    unchanged (no redirect followed, no user_id in query) -- the error must
    surface enough detail (status + body snippet) to debug from the log
    alone, and must not itself crash on a minimal fake response object."""
    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def get(self, url, **k):
            class R:
                # No redirect happened -- url identical to the request, no user_id/client_id.
                def __init__(self):
                    self.url = url
                    self.status_code = 200
                    self.text = "<html>some login page html</html>"
            return R()

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_upstox_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(HeadlessTotpAuthError, match="status=200") as excinfo:
        upstox_totp_login(api_key="k", api_secret="s", user_id="9999999999",
                           password="123456", totp_secret="JBSWY3DPEHPK3PXP",
                           redirect_uri="https://example.com/callback/upstox")
    assert "some login page html" in str(excinfo.value)


def test_upstox_invalid_totp_secret_raises(monkeypatch):
    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def get(self, *a, **k):
            class R:
                url = "https://x/?user_id=SESSUSER&client_id=SESSCLIENT"
            return R()

        def post(self, *a, **k):
            class R:
                status_code = 200

                def json(self_r):
                    return {"success": True, "data": {"validateOTPToken": "tok123"}}
            return R()

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_upstox_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(HeadlessTotpAuthError, match="invalid TOTP secret"):
        upstox_totp_login(api_key="k", api_secret="s", user_id="9999999999",
                           password="123456", totp_secret="NOT-VALID-BASE32!!",
                           redirect_uri="https://example.com/callback/upstox")


def test_upstox_full_success_path(monkeypatch):
    calls = {"n": 0}

    class R:
        def __init__(self, url="", status_code=200, body=None):
            self.url = url
            self.status_code = status_code
            self._body = body or {}
            self.text = str(self._body)

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def get(self, url, **k):
            # Step 1: dialog redirect
            return R(url="https://x/?user_id=SESSUSER&client_id=SESSCLIENT")

        def post(self, url, **k):
            calls["n"] += 1
            if "otp/generate" in url:
                return R(body={"success": True, "data": {"validateOTPToken": "tok123"}})
            if "otp-totp/verify" in url:
                return R(body={"success": True, "data": {}})
            if "2fa" in url:
                return R(body={"success": True, "data": {}})
            if "oauth/authorize" in url:
                return R(body={"success": True, "data": {
                    "redirectUri": "https://cb/?code=AUTHCODE123"}})
            raise AssertionError(f"unexpected POST {url}")

    class FakeTokenSession:
        def __init__(self, *a, **k):
            pass

        def post(self, url, **k):
            return R(body={"access_token": "FINAL_TOKEN_XYZ"})

    import broker_auth.headless_totp_auth as mod
    sessions = iter([FakeSession(), FakeTokenSession()])
    monkeypatch.setattr(mod, "_upstox_session", lambda: next(sessions))
    monkeypatch.setattr("time.sleep", lambda *_: None)

    token = upstox_totp_login(api_key="k", api_secret="s", user_id="9999999999",
                               password="123456", totp_secret="JBSWY3DPEHPK3PXP",
                               redirect_uri="https://example.com/callback/upstox")
    assert token == "FINAL_TOKEN_XYZ"
