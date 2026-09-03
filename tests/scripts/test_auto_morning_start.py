import asyncio
import pytest
from scripts.auto_morning_start import run_morning_sequence


def _ok_step(name):
    return (name, True, "")


@pytest.mark.asyncio
async def test_dry_run_skips_pm2_and_db_writes_but_runs_logins(monkeypatch):
    import scripts.auto_morning_start as mod

    calls = {"pm2": 0, "db_writes": 0}

    async def fake_start_pm2():
        calls["pm2"] += 1
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        return "UPTOKEN"

    def fake_zerodha(**kw):
        return "ZTOKEN"

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

        async def update_feeder_token(self, *a, **k):
            calls["db_writes"] += 1

        async def update_access_token(self, *a, **k):
            calls["db_writes"] += 1

        async def set_terminal_connected(self, *a, **k):
            calls["db_writes"] += 1

        async def set_trade_enabled(self, *a, **k):
            calls["db_writes"] += 1

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())

    steps = await run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")

    assert calls["pm2"] == 0
    assert calls["db_writes"] == 0
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Upstox login"] is True
    assert names_ok["Zerodha login (SA5770)"] is True


@pytest.mark.asyncio
async def test_upstox_failure_does_not_block_zerodha_step(monkeypatch):
    import scripts.auto_morning_start as mod
    from broker_auth.headless_totp_auth import HeadlessTotpAuthError

    async def fake_start_pm2():
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        raise HeadlessTotpAuthError("Upstox: Step 4 failed — PIN rejected.")

    def fake_zerodha(**kw):
        return "ZTOKEN"

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())

    steps = await run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Upstox login"] is False
    assert names_ok["Zerodha login (SA5770)"] is True


@pytest.mark.asyncio
async def test_pm2_failure_skips_all_downstream_steps(monkeypatch):
    import scripts.auto_morning_start as mod

    async def fake_start_pm2():
        return False, "pm2 binary not found"

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)

    steps = await run_morning_sequence(dry_run=False, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["pm2 start"] is False
    assert "Upstox login" not in names_ok


@pytest.mark.asyncio
async def test_fyers_failure_does_not_block_strategy_resume_step(monkeypatch):
    import scripts.auto_morning_start as mod
    from broker_auth.headless_totp_auth_fyers import FyersHeadlessLoginError

    async def fake_start_pm2():
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        return "UPTOKEN"

    def fake_zerodha(**kw):
        return "ZTOKEN"

    def fake_fyers(**kw):
        raise FyersHeadlessLoginError("Fyers: Cloudflare challenge page")

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())
    import broker_auth.headless_totp_auth_fyers as fyers_mod
    monkeypatch.setattr(fyers_mod, "fyers_totp_login", fake_fyers)

    steps = await mod.run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Fyers login (best-effort)"] is False
    assert names_ok["Strategies auto-resume"] is True
