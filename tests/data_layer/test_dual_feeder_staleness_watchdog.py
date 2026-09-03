"""
tests/data_layer/test_dual_feeder_staleness_watchdog.py -- regression for
the 2026-08-23 addition of DualFeeder._staleness_watchdog()/_check_staleness_once().

Real gap found by an overnight commercial-launch audit: the ONLY staleness
detection in the whole feeder stack (GlobalFeeder's own 30s-heartbeat/
_reconnect) is wired up exclusively for the single-provider code path
(_start_single_internal) -- the actual production dual-broker path
(DualFeeder.start_providers, used whenever both Upstox and Fyers have
tokens) never had ANY detection of a "connected but silent" feed. A
zombie WebSocket (broker-side throttling, a subscription silently
dropped, a stale session that doesn't error) was completely invisible.
"""
import time

import pytest

from config.global_config import GlobalConfig, IST, SysEvent
from data_layer.base_feeder import EventBus, SystemEvent
from data_layer.global_feeder import DualFeeder


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _make_feeder(monitored_indices=("NIFTY",)) -> DualFeeder:
    cfg = GlobalConfig()
    cfg.monitored_indices = list(monitored_indices)
    feeder = DualFeeder(_CapturingBus(), cfg)
    feeder._running = True
    # Past the boot grace window and inside market hours by default -- each
    # test overrides only what it needs to isolate.
    feeder._staleness_boot_ts = time.monotonic() - 100.0
    return feeder


def _during_market_hours(monkeypatch, feeder):
    import data_layer.global_feeder as gf_mod

    class _FakeDT(gf_mod.datetime):
        @classmethod
        def now(cls, tz=None):
            return gf_mod.datetime(2026, 8, 24, 11, 0, tzinfo=IST)   # well inside 09:15-15:30
    monkeypatch.setattr(gf_mod, "datetime", _FakeDT)


@pytest.mark.asyncio
async def test_alerts_when_a_monitored_symbol_has_never_ticked_past_boot_grace(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    await feeder._check_staleness_once()
    alerts = [e for t, e in feeder._bus.published if e.code == SysEvent.FEEDER_DOWN]
    assert len(alerts) == 1
    assert "NIFTY" in alerts[0].message


@pytest.mark.asyncio
async def test_no_alert_within_boot_grace_window(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    feeder._staleness_boot_ts = time.monotonic()   # just started
    await feeder._check_staleness_once()
    assert feeder._bus.published == []


@pytest.mark.asyncio
async def test_no_alert_outside_market_hours(monkeypatch):
    feeder = _make_feeder()
    import data_layer.global_feeder as gf_mod

    class _FakeDT(gf_mod.datetime):
        @classmethod
        def now(cls, tz=None):
            return gf_mod.datetime(2026, 8, 24, 20, 0, tzinfo=IST)   # well after close
    monkeypatch.setattr(gf_mod, "datetime", _FakeDT)
    await feeder._check_staleness_once()
    assert feeder._bus.published == []


@pytest.mark.asyncio
async def test_no_alert_when_symbol_is_actively_ticking(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    feeder._dedup.accept("NIFTY", 24500.0, provider="upstox")   # fresh tick just landed
    await feeder._check_staleness_once()
    assert feeder._bus.published == []


@pytest.mark.asyncio
async def test_alerts_once_symbol_goes_stale_after_previously_ticking(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    feeder._dedup.accept("NIFTY", 24500.0, provider="upstox")
    feeder._dedup._last["NIFTY"] = (time.monotonic() - 60.0, 24500.0)   # age it past the 30s threshold
    await feeder._check_staleness_once()
    alerts = [e for t, e in feeder._bus.published if e.code == SysEvent.FEEDER_DOWN]
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_does_not_re_alert_every_check_during_a_sustained_outage(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    await feeder._check_staleness_once()   # first check -- alerts
    await feeder._check_staleness_once()   # second check, still stale -- must NOT alert again
    alerts = [e for t, e in feeder._bus.published if e.code == SysEvent.FEEDER_DOWN]
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_publishes_restored_once_a_fresh_tick_arrives_after_being_flagged(monkeypatch):
    feeder = _make_feeder()
    _during_market_hours(monkeypatch, feeder)
    await feeder._check_staleness_once()   # never ticked -> alerts, marks NIFTY as alerted
    assert "NIFTY" in feeder._staleness_alerted
    feeder._dedup.accept("NIFTY", 24500.0, provider="upstox")   # fresh tick arrives
    await feeder._check_staleness_once()
    restored = [e for t, e in feeder._bus.published if e.code == SysEvent.FEEDER_RESTORED]
    assert len(restored) == 1
    assert "NIFTY" not in feeder._staleness_alerted


@pytest.mark.asyncio
async def test_multiple_monitored_symbols_tracked_independently(monkeypatch):
    feeder = _make_feeder(monitored_indices=("NIFTY", "SENSEX"))
    _during_market_hours(monkeypatch, feeder)
    feeder._dedup.accept("NIFTY", 24500.0, provider="upstox")   # NIFTY healthy
    # SENSEX never ticked -- stale
    await feeder._check_staleness_once()
    alerts = [e.message for t, e in feeder._bus.published if e.code == SysEvent.FEEDER_DOWN]
    assert len(alerts) == 1
    assert "SENSEX" in alerts[0]
    assert "NIFTY" not in feeder._staleness_alerted
    assert "SENSEX" in feeder._staleness_alerted
