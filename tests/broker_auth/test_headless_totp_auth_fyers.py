import pytest
from broker_auth.headless_totp_auth_fyers import FyersHeadlessLoginError, fyers_totp_login


def test_fyers_missing_client_id_raises():
    with pytest.raises(FyersHeadlessLoginError, match="client_id"):
        fyers_totp_login(client_id="", app_id="a", password="p", totp_secret="JBSWY3DPEHPK3PXP", pin="1234")


def test_fyers_missing_totp_secret_raises():
    with pytest.raises(FyersHeadlessLoginError, match="totp_secret"):
        fyers_totp_login(client_id="c", app_id="a", password="p", totp_secret="", pin="1234")


def test_fyers_playwright_timeout_raises_named_error(monkeypatch):
    import broker_auth.headless_totp_auth_fyers as mod

    class FakePage:
        def goto(self, *a, **k):
            pass

        def fill(self, *a, **k):
            raise TimeoutError("locator not found")

    class FakeBrowser:
        def new_page(self, **k):
            return FakePage()

        def close(self):
            pass

    class FakeChromium:
        def launch(self, **k):
            return FakeBrowser()

    class FakePlaywrightCtx:
        def __enter__(self):
            class PW:
                chromium = FakeChromium()
            return PW()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(mod, "sync_playwright", lambda: FakePlaywrightCtx())

    with pytest.raises(FyersHeadlessLoginError, match="locator not found"):
        fyers_totp_login(client_id="c", app_id="a", password="p", totp_secret="JBSWY3DPEHPK3PXP", pin="1234")
