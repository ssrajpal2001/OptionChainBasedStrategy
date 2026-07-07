"""Safety-critical behaviours for sell-straddle live trading.

These tests guard the gaps that can strand real broker positions or block
future entries: kill-switch / shutdown / deployment removal liquidation,
re-entrant close protection, routing-failure cleanup, and per-trade SL/target.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time as dtime
from typing import Any, Dict, Tuple

import pytest

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.core import StrategyBookManager
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _open_position(
    ss: SellStraddleStrategy,
    ce_strike: float = 24500.0,
    pe_strike: float = 24500.0,
    ce_ltp: float = 120.0,
    pe_ltp: float = 110.0,
) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=ss._underlying,
        atm_at_entry=24500.0,
        entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=datetime.now(IST)),
        net_credit=ce_ltp + pe_ltp,
        open_time=datetime.now(IST),
        status="open",
        lot_size=ss._lot_size * ss._lot_multiplier,
        expiry_date=date.today(),
    )
    ss._position = pos
    return pos


def _patch_emit(ss: SellStraddleStrategy):
    """Replace _emit_order with a recorder that captures EXIT/ENTRY events."""
    emitted: list = []

    async def _fake_emit(ev):
        emitted.append(ev)

    ss._emit_order = _fake_emit
    return emitted


def _disable_eod_squareoff(ss: SellStraddleStrategy) -> None:
    """Push force_exit to end-of-day so _check_exits does not trigger EOD first."""
    ss._force_exit = dtime(23, 59)


# ── SellStraddleStrategy liquidate / close safety ───────────────────────────


def test_liquidate_closes_open_position():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    emitted = _patch_emit(ss)

    asyncio.run(ss.liquidate("kill_switch"))

    assert ss._position is None
    assert len(emitted) == 1
    assert emitted[0].action == "EXIT"
    assert emitted[0].close_reason == "kill_switch"


def test_liquidate_is_idempotent_when_flat():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    emitted = _patch_emit(ss)

    asyncio.run(ss.liquidate("kill_switch"))
    asyncio.run(ss.liquidate("kill_switch"))

    assert len(emitted) == 0


def test_close_position_is_reentrant_safe():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    emitted = _patch_emit(ss)

    async def run():
        await asyncio.gather(
            ss._close_position("day_loss_sl"),
            ss._close_position("trailing_sl_ltp"),
        )

    asyncio.run(run())

    assert ss._position is None
    assert len(emitted) == 1  # only one EXIT order despite two concurrent triggers


def test_close_position_does_not_apply_cooldown_on_kill_switch():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    _patch_emit(ss)

    asyncio.run(ss._close_position("kill_switch"))

    assert ss._sl_cooldown_until is None


# ── Routing failure cleanup ────────────────────────────────────────────────


def test_entry_routing_failed_clears_optimistic_position():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._order_pending = True
    ss._trades_today = 1
    _open_position(ss)

    fill = StraddleFillEvent(
        action="ENTRY",
        underlying="NIFTY",
        atm=24500.0,
        ce_strike=24500.0,
        pe_strike=24500.0,
        ce_fill=0.0,
        pe_fill=0.0,
        client_id="",
        binding_id="",
        event_id="test_routing_failed",
        entry_aborted=True,
        routing_failed=True,
    )
    ss._on_fill(fill)

    assert ss._position is None
    assert ss._order_pending is False
    assert ss._trades_today == 0
    assert ss._sl_cooldown_until is None  # no cooldown for pure routing failures


def test_on_fill_exception_does_not_crash_loop_and_clears_pending_when_flat():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._order_pending = True
    ss._position = None

    bad_fill = object()  # has no action attribute → will raise AttributeError
    try:
        ss._on_fill(bad_fill)
    except Exception:
        pytest.fail("_on_fill must swallow malformed fill events")

    assert ss._order_pending is False


# ── StrategyBookManager lifecycle safety ───────────────────────────────────


class _FakeBook:
    def __init__(self):
        self._position: Any = None
        self.liquidated: Tuple[str, ...] = ()
        self.stopped: bool = False

    async def liquidate(self, reason: str = "kill_switch") -> None:
        self.liquidated = (reason,)

    async def stop_async(self) -> None:
        self.stopped = True

    def stop(self) -> None:
        self.stopped = True


class _TestBookManager(StrategyBookManager):
    def __init__(self):
        super().__init__(bus=None, cfg=None, client_db=None,
                         monitored_indices=["NIFTY"], reconcile_sec=5.0)

    def _wanted(self) -> Dict[Tuple[str, str, str], Any]:
        return {}

    def _spawn_book(self, key, value):
        return _FakeBook()


def test_liquidate_all_closes_open_positions():
    mgr = _TestBookManager()
    book = _FakeBook()
    book._position = object()
    mgr._books[("C1", "B1", "NIFTY")] = book

    asyncio.run(mgr.liquidate_all(scope="FIRM_WIDE"))

    assert book.liquidated == ("kill_switch",)
    assert book.stopped is True


def test_stop_async_liquidates_open_positions():
    mgr = _TestBookManager()
    book = _FakeBook()
    book._position = object()
    mgr._books[("C1", "B1", "NIFTY")] = book

    asyncio.run(mgr.stop_async())

    assert book.liquidated == ("system_shutdown",)
    assert book.stopped is True
    assert not mgr._books  # cleared after shutdown


def test_reconcile_liquidates_open_position_on_removal():
    mgr = _TestBookManager()
    book = _FakeBook()
    book._position = object()
    mgr._books[("C1", "B1", "NIFTY")] = book

    # The book is no longer wanted; reconcile should remove it.
    # _reconcile is sync and schedules liquidation on the running loop.
    async def run_reconcile():
        mgr._reconcile()
        # Yield so the scheduled liquidation task can run.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(run_reconcile())

    assert ("C1", "B1", "NIFTY") not in mgr._books
    assert book.liquidated == ("deployment_stop",)
    assert book.stopped is True
