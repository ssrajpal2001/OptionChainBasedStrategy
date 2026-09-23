"""SellStraddle vwap_source (2026-09-03, direct user spec): a per-binding
config toggle letting one deployment run its entire entry/exit/roll
decision chain off a self-computed "calculative" VWAP instead of the
broker's own ATP -- for a live paper-trading side-by-side comparison
against a sibling binding left on the default "broker_atp" behavior.
The swap happens at the single point every downstream indicator (VWAP,
SLOPE, RSI, ROC) is derived from: the atp value fed into
self._pool_engine.update_tick() on every option tick.
"""
import asyncio
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig, Topic
from data_layer.base_feeder import EventBus, OptionTick
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.config import load_sell_straddle_config

IST = ZoneInfo("Asia/Kolkata")


def _tick(strike, side, ltp, atp, volume, ts) -> OptionTick:
    return OptionTick(
        symbol=f"NSE_FO|NIFTY{strike}{side}", underlying="NIFTY", strike=float(strike),
        option_type=side, expiry=date(2026, 9, 25), ltp=ltp, bid=ltp, ask=ltp,
        oi=0, change_oi=0, volume=volume, iv=0.0, delta=0.0, timestamp=ts, atp=atp,
    )


async def _no_op_async(*a, **k) -> None:
    """Stand-in for _seed_shadow_vwap_from_rest -- avoids real DB/network I/O
    inside a unit test; never marks a key seeded on its own."""
    return None


def _strategy(vwap_source: str = "broker_atp") -> SellStraddleStrategy:
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._running = True
    s._entry_expiry_date = None   # bypass the entry-expiry tick filter
    s._vwap_source = vwap_source
    s._shadow_vwap_enabled = True
    return s


async def _drive_option_loop(s: SellStraddleStrategy, ticks: list) -> None:
    task = asyncio.create_task(s._option_loop())
    # _option_loop subscribes to the bus lazily on its own first scheduling
    # turn -- publish() never yields control on its own (no real suspend
    # point), so a publish issued before the task's first turn would be
    # silently dropped (no queue registered yet). Let it run once first.
    await asyncio.sleep(0.05)
    try:
        for t in ticks:
            await s._bus.publish(Topic.OPTION_TICK, t)
            await asyncio.sleep(0.05)   # let _option_loop's queue.get() actually consume it
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ── Config loading ──────────────────────────────────────────────────────────

def test_config_defaults_to_broker_atp(monkeypatch):
    from data_layer.runtime_config import RuntimeConfig
    monkeypatch.setattr(RuntimeConfig, "index_section", staticmethod(lambda und, sec: {}))
    cfg = load_sell_straddle_config("NIFTY", GlobalConfig())
    assert cfg.vwap_source == "broker_atp"


def test_config_reads_calculative_when_set(monkeypatch):
    from data_layer.runtime_config import RuntimeConfig
    monkeypatch.setattr(RuntimeConfig, "index_section",
                         staticmethod(lambda und, sec: {"vwap_source": "calculative"}))
    cfg = load_sell_straddle_config("NIFTY", GlobalConfig())
    assert cfg.vwap_source == "calculative"


def test_config_invalid_value_falls_back_to_broker_atp(monkeypatch):
    from data_layer.runtime_config import RuntimeConfig
    monkeypatch.setattr(RuntimeConfig, "index_section",
                         staticmethod(lambda und, sec: {"vwap_source": "nonsense"}))
    cfg = load_sell_straddle_config("NIFTY", GlobalConfig())
    assert cfg.vwap_source == "broker_atp"


# ── Engine behavior ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_broker_atp_mode_feeds_pool_engine_the_broker_atp():
    """Default/unchanged behavior: the pool engine must receive the
    broker's own atp field, byte-for-byte, regardless of what the
    calculative series would have computed."""
    s = _strategy(vwap_source="broker_atp")
    ts = datetime(2026, 9, 3, 10, 0, tzinfo=IST)
    await _drive_option_loop(s, [
        _tick(24000, "CE", ltp=100.0, atp=95.0, volume=1000, ts=ts),
    ])
    _, atp = s._pool_engine._latest[(24000, "CE")]
    assert atp == 95.0


@pytest.mark.asyncio
async def test_calculative_mode_feeds_pool_engine_the_cumulative_vwap_not_broker_atp():
    """calculative mode: the pool engine must receive cum(ltp*volume_delta)/
    cum(volume_delta) -- the same formula _update_shadow_vwap already uses
    for its log-only comparison -- NOT the broker's atp field, even though
    the broker atp is still present on every tick.

    2026-09-23: pre-seeds _shadow_vwap_rest_seeded directly (bypassing the
    real async REST-seed task, same as this file's other tests mock out
    background REST calls) so this test isolates the cum_pv/cum_v MATH from
    the separate warmup-gate behavior covered by the test below it."""
    s = _strategy(vwap_source="calculative")
    s._seed_shadow_vwap_from_rest = _no_op_async  # no real network/DB I/O in a unit test
    s._shadow_vwap_rest_seeded.add((24000, "CE"))
    ts = datetime(2026, 9, 3, 10, 0, tzinfo=IST)
    ts2 = datetime(2026, 9, 3, 10, 0, 30, tzinfo=IST)
    await _drive_option_loop(s, [
        # First tick seeds the volume baseline (delta=0 -- no vwap contribution yet).
        _tick(24000, "CE", ltp=100.0, atp=95.0, volume=1000, ts=ts),
        # Second tick: 500 units traded at ltp=110 -> cum_pv=55000, cum_v=500 -> vwap=110.0
        _tick(24000, "CE", ltp=110.0, atp=95.0, volume=1500, ts=ts2),
    ])
    _, atp = s._pool_engine._latest[(24000, "CE")]
    assert atp == pytest.approx(110.0)
    assert atp != 95.0, "must not have used the broker's own atp field"


@pytest.mark.asyncio
async def test_calculative_mode_falls_back_to_broker_atp_before_rest_seed_completes():
    """2026-09-23 CRITICAL FIX, real live incident: confirmed live on a
    restart -- the very first tick(s) after a restart have cum_v>0 (a
    couple of live ticks' own tiny volume delta) but the async REST seed
    that restores the real day's history hasn't landed yet. Trusting
    cum_pv/cum_v at that point produced a wildly wrong "VWAP" (essentially
    just the latest tick's own raw LTP), which corrupted session_min_vwap
    and fired a spurious vwap_rise roll one second after restart. Same tick
    sequence as the test above (which WOULD compute vwap=110.0 from live
    ticks alone) but WITHOUT the key in _shadow_vwap_rest_seeded -- must
    still fall back to the broker's own atp (95.0), exactly like the
    pre-existing cum_v==0 warmup case."""
    s = _strategy(vwap_source="calculative")
    s._seed_shadow_vwap_from_rest = _no_op_async  # real REST-seed never completes in this test
    ts = datetime(2026, 9, 3, 10, 0, tzinfo=IST)
    ts2 = datetime(2026, 9, 3, 10, 0, 30, tzinfo=IST)
    await _drive_option_loop(s, [
        _tick(24000, "CE", ltp=100.0, atp=95.0, volume=1000, ts=ts),
        _tick(24000, "CE", ltp=110.0, atp=95.0, volume=1500, ts=ts2),
    ])
    _, atp = s._pool_engine._latest[(24000, "CE")]
    assert atp == 95.0, (
        "REST seed has not completed yet -- must fall back to broker atp, "
        "not trust a cum_pv/cum_v built from only 1-2 live ticks"
    )


@pytest.mark.asyncio
async def test_calculative_mode_falls_back_to_broker_atp_during_warmup():
    """Before any volume delta has accumulated for a strike/side (the very
    first tick, or a tick with no volume advance), calculative mode must
    fall back to the broker atp rather than feed the pool engine a bogus 0
    -- a 0 would otherwise be silently treated by PoolIndicatorEngine's own
    keep-last-good logic as 'no update'."""
    s = _strategy(vwap_source="calculative")
    s._seed_shadow_vwap_from_rest = _no_op_async  # avoid real DB/network I/O in a unit test
    ts = datetime(2026, 9, 3, 10, 0, tzinfo=IST)
    await _drive_option_loop(s, [
        _tick(24000, "CE", ltp=100.0, atp=95.0, volume=1000, ts=ts),
    ])
    _, atp = s._pool_engine._latest[(24000, "CE")]
    assert atp == 95.0, "no volume delta yet -- must fall back to broker atp, not feed 0"
