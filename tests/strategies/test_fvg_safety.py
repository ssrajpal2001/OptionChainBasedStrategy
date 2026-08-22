"""
tests/strategies/test_fvg_safety.py — confirm-then-finalize safety tests for
FVGStrategy (strategies/fvg/engine.py).

2026-08-05: mirrors tests/strategies/test_d1trap_bear_only_safety.py's shape --
drives the REAL _open_position / _square_off / _on_fill round trip through a
fake bus (not a hand-built isolated harness), asserting a broker-unreachable
BUY/SELL leaves the book exactly as it was: no phantom position on an aborted
ENTRY, still-open + still-persisted position on an aborted/timed-out EXIT.

Unlike D1Trap-BearOnly's _enter_leg (called synchronously from sync tick-
processing, so it must mutate optimistically before confirmation), FVG's
_open_position/_square_off are ONLY ever invoked via asyncio.create_task(...)
in production (see _check_retest_entry/_check_exit_premium/
_check_stagnation_exit), so they can fully await the bridge's confirmation
before ever touching self._position -- there is no optimistic value to revert.
All tests are async (@pytest.mark.asyncio) for the same reason.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

import pytest

from config.global_config import GlobalConfig
from data_layer import position_store
from data_layer.base_feeder import EventBus
from strategies.fvg.engine import FVGStrategy


def _make_strategy(tmp_path, monkeypatch) -> FVGStrategy:
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    cfg = GlobalConfig()
    strat = FVGStrategy(
        EventBus(), cfg, underlying="NIFTY", client_id="C", binding_id="B",
        lot_multiplier=1, feeder_token="",
    )
    return strat


def _fake_fvg() -> dict:
    return {"zone_lo": 24500.0, "zone_hi": 24520.0}


class _RecordingBus:
    """Captures every published (topic, event) pair. Does NOT auto-confirm --
    tests that need a fill wire it up explicitly via a subclass."""

    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _wire_expiry_and_premium(strat: FVGStrategy, monkeypatch, expiry, strike=24450,
                              opt_type="CE", premium=120.0) -> None:
    import strategies.fvg.engine as fvg_engine
    monkeypatch.setattr(fvg_engine, "_next_week_expiry", lambda *a, **k: expiry)
    strat._option_ltp[(strike, opt_type, expiry)] = premium


# ── ENTRY: confirm-then-finalize ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_open_position_leaves_book_flat_when_bridge_reports_entry_aborted(
    tmp_path, monkeypatch,
):
    """A synthetic entry_aborted FVGOrderFillEvent (what the bridge publishes
    when the broker is unreachable / can_trade() gate is closed) must leave
    self._position None -- no phantom position, nothing persisted."""
    strat = _make_strategy(tmp_path, monkeypatch)
    expiry = date.today() + timedelta(days=10)
    _wire_expiry_and_premium(strat, monkeypatch, expiry)

    class _AbortingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "BUY":
                from execution_bridge.fvg_bridge import FVGOrderFillEvent
                fill = FVGOrderFillEvent(
                    action="BUY", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=0.0, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, entry_aborted=True, routing_failed=True,
                )
                strat._on_fill(fill)

    strat._bus = _AbortingBus()

    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())

    assert strat._position is None
    assert position_store.load(strat._persist_key) is None
    assert strat._entry_in_flight is False
    assert len(strat._bus.published) == 1
    assert strat._bus.published[0][0] == "fvg_order_request"


@pytest.mark.asyncio
async def test_open_position_leaves_book_flat_on_confirmation_timeout(tmp_path, monkeypatch):
    """If the fill event is simply lost (no abort, no confirm -- just silence),
    the wait must time out and leave the book flat rather than hang or assume
    success."""
    strat = _make_strategy(tmp_path, monkeypatch)
    expiry = date.today() + timedelta(days=10)
    _wire_expiry_and_premium(strat, monkeypatch, expiry)
    monkeypatch.setattr(type(strat), "_ENTRY_CONFIRM_TIMEOUT_SEC", 0.05)

    strat._bus = _RecordingBus()  # never delivers a fill

    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())

    assert strat._position is None
    assert position_store.load(strat._persist_key) is None
    assert strat._entry_in_flight is False


@pytest.mark.asyncio
async def test_open_position_confirmed_entry_finalizes(tmp_path, monkeypatch):
    """Sanity check on the happy path: a REAL confirmed BUY fill (not aborted)
    must set self._position and persist it."""
    strat = _make_strategy(tmp_path, monkeypatch)
    expiry = date.today() + timedelta(days=10)
    _wire_expiry_and_premium(strat, monkeypatch, expiry)

    class _ConfirmingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "BUY":
                from execution_bridge.fvg_bridge import FVGOrderFillEvent
                fill = FVGOrderFillEvent(
                    action="BUY", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=event.entry_price, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, entry_aborted=False,
                )
                strat._on_fill(fill)

    strat._bus = _ConfirmingBus()

    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())

    assert strat._position is not None
    assert strat._position["option_type"] == "CE"
    assert strat._position["strike"] == 24450
    stored = position_store.load(strat._persist_key)
    assert stored is not None
    assert stored["strike"] == 24450


@pytest.mark.asyncio
async def test_open_position_is_a_noop_if_position_already_open(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._position = {"strike": 1}  # already holding something
    bus = _RecordingBus()
    strat._bus = bus

    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())

    assert bus.published == []


@pytest.mark.asyncio
async def test_open_position_is_a_noop_if_entry_already_in_flight(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._entry_in_flight = True
    bus = _RecordingBus()
    strat._bus = bus

    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())

    assert bus.published == []


# ── EXIT: confirm-then-finalize ──────────────────────────────────────────────


async def _open_confirmed_position(strat: FVGStrategy, monkeypatch) -> dict:
    """Drive a real position into existence via the real _open_position (not a
    hand-built dict) so its shape is exactly what production code produces."""
    expiry = date.today() + timedelta(days=10)
    _wire_expiry_and_premium(strat, monkeypatch, expiry)

    class _ConfirmingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "BUY":
                from execution_bridge.fvg_bridge import FVGOrderFillEvent
                fill = FVGOrderFillEvent(
                    action="BUY", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=event.entry_price, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, entry_aborted=False,
                )
                strat._on_fill(fill)

    strat._bus = _ConfirmingBus()
    await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, datetime.now())
    assert strat._position is not None
    return strat._position


@pytest.mark.asyncio
async def test_square_off_leaves_position_open_when_bridge_reports_exit_failed(
    tmp_path, monkeypatch,
):
    strat = _make_strategy(tmp_path, monkeypatch)
    pos = await _open_confirmed_position(strat, monkeypatch)

    class _AbortingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "SELL":
                from execution_bridge.fvg_bridge import FVGOrderFillEvent
                fill = FVGOrderFillEvent(
                    action="SELL", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=0.0, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, exit_failed=True,
                )
                strat._on_fill(fill)

    strat._bus = _AbortingBus()

    await strat._square_off("sl_hit")

    assert strat._position is pos
    assert strat._position is not None
    assert strat._position.get("_closing") is False  # free to retry on the next tick
    assert position_store.load(strat._persist_key) is not None
    assert len(strat._bus.published) == 1
    assert strat._bus.published[0][0] == "fvg_order_request"


@pytest.mark.asyncio
async def test_square_off_leaves_position_open_on_confirmation_timeout(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    pos = await _open_confirmed_position(strat, monkeypatch)
    monkeypatch.setattr(type(strat), "_EXIT_CONFIRM_TIMEOUT_SEC", 0.05)

    strat._bus = _RecordingBus()  # never delivers a fill

    await strat._square_off("sl_hit")

    assert strat._position is pos
    assert pos.get("_closing") is False
    assert position_store.load(strat._persist_key) is not None


@pytest.mark.asyncio
async def test_square_off_confirmed_exit_finalizes(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    await _open_confirmed_position(strat, monkeypatch)

    class _ConfirmingBus(_RecordingBus):
        async def publish(self, topic, event):
            await super().publish(topic, event)
            if event.action == "SELL":
                from execution_bridge.fvg_bridge import FVGOrderFillEvent
                fill = FVGOrderFillEvent(
                    action="SELL", underlying=event.underlying,
                    option_type=event.option_type, strike=event.strike,
                    fill_price=130.0, qty=event.quantity,
                    client_id=event.client_id, binding_id=event.binding_id,
                    event_id=event.event_id, exit_failed=False,
                )
                strat._on_fill(fill)

    strat._bus = _ConfirmingBus()

    await strat._square_off("tsl_hit")

    assert strat._position is None
    assert position_store.load(strat._persist_key) is None


@pytest.mark.asyncio
async def test_square_off_is_a_noop_if_already_closing(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    pos = await _open_confirmed_position(strat, monkeypatch)
    pos["_closing"] = True
    bus = _RecordingBus()
    strat._bus = bus

    await strat._square_off("eod")

    assert bus.published == []
    assert strat._position is pos


@pytest.mark.asyncio
async def test_square_off_is_a_noop_if_no_open_position(tmp_path, monkeypatch):
    strat = _make_strategy(tmp_path, monkeypatch)
    bus = _RecordingBus()
    strat._bus = bus

    await strat._square_off("eod")

    assert bus.published == []


# ── day-rollover (2026-08-22 CRITICAL fix) ───────────────────────────────────
# reset_session() had zero live call sites anywhere in engine.py before this
# fix -- self._day_done, once set True by _eod_loop at 15:15, was NEVER reset
# back to False except in __init__ or reset_session() itself (unreachable
# live). Every candle for every trading day after the first was silently
# dropped by _on_candle's own `... or self._day_done: return` gate, forever,
# for the remaining lifetime of the process.

def test_on_candle_processes_a_new_day_after_prior_day_done(tmp_path, monkeypatch):
    from data_layer.base_feeder import CandleEvent
    strat = _make_strategy(tmp_path, monkeypatch)
    strat._htf_loaded = True   # skip the real REST warmup for this test

    day1 = datetime(2026, 8, 20, 9, 20, tzinfo=None)
    ev1 = CandleEvent(symbol="NSE_INDEX|Nifty 50", timeframe=1, open=100, high=101,
                       low=99, close=100.5, volume=0, timestamp=day1)
    strat._on_candle(ev1)
    assert strat._today == day1.date()
    assert strat._last_spot == 100.5

    # Simulate _eod_loop having fired for day 1.
    strat._day_done = True

    day2 = datetime(2026, 8, 21, 9, 20, tzinfo=None)
    ev2 = CandleEvent(symbol="NSE_INDEX|Nifty 50", timeframe=1, open=110, high=111,
                       low=109, close=110.5, volume=0, timestamp=day2)
    strat._on_candle(ev2)

    assert strat._today == day2.date(), "a genuinely new day's candle must update self._today"
    assert strat._day_done is False, "reset_session() must have fired and cleared day_done"
    assert strat._last_spot == 110.5, (
        "day 2's candle must actually be PROCESSED, not silently dropped by "
        "the stale self._day_done==True left over from day 1 -- this is the "
        "exact bug: without reset_session() ever firing, this candle (and "
        "every one after it) would be silently ignored forever"
    )
