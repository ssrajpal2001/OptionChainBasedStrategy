import pytest
from scripts.auto_evening_stop import run_evening_sequence


@pytest.mark.asyncio
async def test_evening_sequence_stops_pm2(monkeypatch):
    import scripts.auto_evening_stop as mod
    calls = {"n": 0}

    async def fake_stop_pm2():
        calls["n"] += 1
        return True, ""

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    steps = await run_evening_sequence()
    assert calls["n"] == 1
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["pm2 stop"] is True


@pytest.mark.asyncio
async def test_evening_sequence_dry_run_does_not_call_pm2(monkeypatch):
    import scripts.auto_evening_stop as mod
    calls = {"n": 0}

    async def fake_stop_pm2():
        calls["n"] += 1
        return True, ""

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    await run_evening_sequence(dry_run=True)
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_evening_sequence_reports_pm2_failure(monkeypatch):
    import scripts.auto_evening_stop as mod

    async def fake_stop_pm2():
        return False, "pm2 process not found"

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    steps = await run_evening_sequence()
    names_ok = {name: (ok, detail) for name, ok, detail in steps}
    assert names_ok["pm2 stop"] == (False, "pm2 process not found")
