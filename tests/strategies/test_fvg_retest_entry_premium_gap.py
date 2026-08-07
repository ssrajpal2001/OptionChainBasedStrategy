"""
tests/strategies/test_fvg_retest_entry_premium_gap.py — a high-liquidity
MITIGATED FVG must NOT be permanently discarded just because no live option
premium tick has arrived yet for the exact entry strike.

2026-08-07 real incident: NIFTY, 12:25 IST -- a genuine high-liquidity
MITIGATED retest fired (log: "FVGStrategy[NIFTY]: no live premium for PE24550
yet -- entry skipped."), but strategies/fvg/engine.py's _check_retest_entry
marked the FVG INVALIDATED unconditionally the instant it scheduled
_open_position, regardless of whether the entry would actually succeed. The
missing premium was a transient data-availability gap (the strike hadn't
ticked yet), not a genuine market invalidation -- but the real trading
opportunity was thrown away forever anyway. Confirmed live: the dashboard
showed the same 3 zones frozen UNMITIGATED for 2+ hours with "no open
position", consistent with this bug.

Fix: _check_retest_entry now runs a synchronous pre-check
(_resolve_strike_and_premium) BEFORE consuming the FVG. If data isn't ready,
the FVG stays MITIGATED and is retried on the next LTF bar close instead of
being discarded.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import pytest

from config.global_config import GlobalConfig, IST
from data_layer import position_store
from data_layer.base_feeder import EventBus
from strategies.d1_trap_option.book import _Bar
from strategies.fvg.engine import FVGStrategy


def _make_strategy(tmp_path, monkeypatch) -> FVGStrategy:
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    cfg = GlobalConfig()
    strat = FVGStrategy(
        EventBus(), cfg, underlying="NIFTY", client_id="C", binding_id="B",
        lot_multiplier=1, feeder_token="",
    )
    return strat


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _fixed_intraday_ts() -> datetime:
    """A fixed mid-morning timestamp, safely inside the entry window
    (< _ENTRY_CUTOFF = 14:30) -- using datetime.now(IST) here made this test
    wall-clock dependent and it started failing for real whenever the test
    happened to run after 14:30 IST (confirmed: this session, run at 14:44)."""
    return datetime.now(IST).replace(hour=10, minute=15, second=0, microsecond=0)


def _make_bearish_fvg(zone_lo=24550.0, zone_hi=24560.0) -> dict:
    """A high-liquidity MITIGATED bearish (PE) FVG, matching detect_fvg's
    real dict shape closely enough for _check_retest_entry's own field reads."""
    return {
        "direction": "BEARISH", "zone_lo": zone_lo, "zone_hi": zone_hi,
        "ce": (zone_lo + zone_hi) / 2,
        "candle1_low": zone_lo, "candle1_high": zone_hi + 10.0,
        "candle1_ts": datetime.now(IST), "candle3_ts": datetime.now(IST),
        "index": 0, "state": "MITIGATED", "high_liquidity": True,
        "mitigated_ts": datetime.now(IST), "invalidated_ts": None,
    }


def _make_bar(ts: datetime, close: float) -> _Bar:
    return _Bar(timestamp=ts, open=close, high=close, low=close, close=close)


@pytest.mark.asyncio
async def test_retest_entry_does_not_consume_fvg_when_no_live_premium_yet(
    tmp_path, monkeypatch,
):
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._bus = _RecordingBus()
    import strategies.fvg.engine as fvg_engine
    expiry = date.today() + timedelta(days=7)
    monkeypatch.setattr(fvg_engine, "_next_week_expiry", lambda *a, **k: expiry)
    # Deliberately NOT wiring strat._option_ltp -- no live premium tick exists yet.

    fvg = _make_bearish_fvg()
    strat._fvgs = [fvg]
    strat._last_spot = 24540.0  # below the zone -- realistic for a bearish setup

    bar = _make_bar(_fixed_intraday_ts(), 24540.0)
    strat._check_retest_entry(bar)
    await asyncio.sleep(0)

    assert fvg["state"] == "MITIGATED", (
        "FVG must stay MITIGATED (retryable) when premium data isn't ready yet -- "
        "must NOT be discarded as if the market invalidated it."
    )
    assert strat._bus.published == [], "no order should be dispatched without a live premium"
    assert strat._position is None


@pytest.mark.asyncio
async def test_retest_entry_consumes_fvg_once_premium_becomes_available(
    tmp_path, monkeypatch,
):
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._bus = _RecordingBus()
    import strategies.fvg.engine as fvg_engine
    expiry = date.today() + timedelta(days=7)
    monkeypatch.setattr(fvg_engine, "_next_week_expiry", lambda *a, **k: expiry)

    fvg = _make_bearish_fvg(zone_lo=24550.0, zone_hi=24560.0)
    strat._fvgs = [fvg]
    strat._last_spot = 24540.0

    # PE strike = ATM(round 100) + itm_offset(50). spot=24540 -> atm=24500 -> PE24550.
    strat._option_ltp[(24550, "PE", expiry)] = 150.0

    bar = _make_bar(_fixed_intraday_ts(), 24540.0)
    strat._check_retest_entry(bar)
    await asyncio.sleep(0)

    assert fvg["state"] == "INVALIDATED", "consumed once a real entry is actually attempted"
    assert len(strat._bus.published) == 1
    topic, ev = strat._bus.published[0]
    assert ev.option_type == "PE"
    assert ev.strike == 24550
    assert ev.entry_price == 150.0  # the real premium, not a fallback/estimate


@pytest.mark.asyncio
async def test_retest_entry_retried_next_bar_after_premium_arrives(tmp_path, monkeypatch):
    """The exact real-world sequence: bar 1 has no premium yet (skipped, FVG
    survives), bar 2 (a later LTF close) has the premium and the entry fires."""
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._bus = _RecordingBus()
    import strategies.fvg.engine as fvg_engine
    expiry = date.today() + timedelta(days=7)
    monkeypatch.setattr(fvg_engine, "_next_week_expiry", lambda *a, **k: expiry)

    fvg = _make_bearish_fvg(zone_lo=24550.0, zone_hi=24560.0)
    strat._fvgs = [fvg]
    strat._last_spot = 24540.0

    ts1 = _fixed_intraday_ts()
    strat._check_retest_entry(_make_bar(ts1, 24540.0))
    await asyncio.sleep(0)
    assert fvg["state"] == "MITIGATED"
    assert strat._bus.published == []

    # Premium tick arrives before the next LTF bar close.
    strat._option_ltp[(24550, "PE", expiry)] = 150.0
    ts2 = ts1 + timedelta(minutes=3)
    strat._check_retest_entry(_make_bar(ts2, 24540.0))
    await asyncio.sleep(0)

    assert fvg["state"] == "INVALIDATED"
    assert len(strat._bus.published) == 1
