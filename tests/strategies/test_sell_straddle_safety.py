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
    """Replace _emit_order with a recorder that captures EXIT/ENTRY events AND immediately
    confirms EXIT orders by feeding a real fill back through _on_fill -- simulating an
    always-available broker. _close_position/_close_leg now block on the bridge's confirmation
    (see 2026-08-04 fail-loud fix), so tests that expect a close to actually finalize need this;
    tests that want to exercise the broker-unavailable / timeout paths patch _emit_order
    themselves instead (see test_close_position_leaves_position_open_* below).
    """
    emitted: list = []

    async def _fake_emit(ev):
        emitted.append(ev)
        if ev.action == "EXIT":
            fill = StraddleFillEvent(
                action="EXIT", underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id="C", binding_id="B", event_id=ev.event_id,
                legs=ev.legs,
            )
            ss._on_fill(fill)

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


# ── Fail-loud EXIT: broker unavailable must NOT fake a close (2026-08-04 incident) ──────────


def test_close_position_leaves_position_open_when_bridge_reports_exit_aborted():
    """Real sequence: _close_position dispatches the EXIT via _emit_order (capturing the real
    ORDER_REQUEST event_id), then a synthetic exit_aborted StraddleFillEvent (what the bridge
    publishes when resolve_broker_or_alert can't find a broker) is fed back through the real
    _on_fill using that same event_id. The position must come out exactly as it went in --
    same object, same strikes, same entry prices, still 'open' -- so a later tick can retry."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    emitted: list = []

    async def _fake_emit(ev):
        emitted.append(ev)
        # Simulate the bridge: broker never resolved -> exit_aborted fill, no real fill prices.
        fill = StraddleFillEvent(
            action="EXIT", underlying=ev.underlying, atm=ev.atm,
            ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
            ce_fill=0.0, pe_fill=0.0,
            client_id="C", binding_id="B", event_id=ev.event_id,
            legs=ev.legs, exit_aborted=True, routing_failed=True,
        )
        ss._on_fill(fill)

    ss._emit_order = _fake_emit

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is pos
    assert ss._position.status == "open"
    assert ss._position.ce_leg.strike == 24500.0
    assert ss._position.ce_leg.entry_price == 120.0
    assert ss._position.pe_leg.entry_price == 110.0
    assert len(emitted) == 1
    assert emitted[0].action == "EXIT"
    # No P&L booked, no cooldown applied -- nothing about this was a real close.
    assert ss._session_realized_pnl_pts == 0.0
    assert ss._sl_cooldown_until is None
    assert ss._close_in_progress is False  # free to retry on the next tick


def test_close_position_leaves_position_open_on_confirmation_timeout(monkeypatch):
    """If the fill event is simply lost (no exit_aborted, no real fill -- just silence), the
    wait must time out and leave the position open rather than hang or assume success."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    monkeypatch.setattr(type(ss), "_CLOSE_CONFIRM_TIMEOUT_SEC", 0.05)

    async def _fake_emit(ev):
        pass  # never deliver a fill

    ss._emit_order = _fake_emit

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is pos
    assert ss._position.status == "open"
    assert ss._session_realized_pnl_pts == 0.0
    assert ss._sl_cooldown_until is None


def test_close_position_confirmed_exit_still_finalizes():
    """Sanity check on the happy path through the new confirm-then-finalize sequence: a REAL
    confirmed EXIT fill (not aborted) must still close the position, book P&L, and persist."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    emitted = _patch_emit(ss)  # auto-confirms EXIT fills

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is None
    assert len(emitted) == 1
    assert emitted[0].action == "EXIT"


# ── 2026-08-06 CRITICAL FIX: close-confirm timeout vs bridge worst-case latency ─────────────
#
# Real incident: straddle_bridge.py's EXIT path can legitimately take up to ~23s to publish a
# fill -- SmartOrderExecutor's exit market_fill_timeout_sec=8.0s, then (if still under-filled)
# the bridge's OWN under-fill retry loop polls get_order_status for up to 15 more seconds
# (range(15) x 1s sleep, straddle_bridge.py::_do_leg). _CLOSE_CONFIRM_TIMEOUT_SEC was 15.0s --
# shorter than the bridge's own worst case -- so _close_position gave up before the bridge could
# ever answer. This is exactly what paper_route's expected broker-rejection path hits on every
# single exit (confirmed live 2026-08-06: 20+ real duplicate BUY orders on ssrajpal2001 inside
# 5 minutes at EOD squareoff, one new real order roughly every 15-16s).


def test_close_confirm_timeout_has_margin_over_bridge_worst_case():
    """Documents and locks the invariant that caused the incident: the strategy's wait for a
    close confirmation must exceed the bridge's own worst-case time to determine one (8s
    executor timeout + 15s under-fill retry = 23s), with real margin -- not just barely above
    it. If either side's timing constant changes in the future, this test must be revisited
    together with the other, not independently."""
    _bridge_worst_case_sec = 8.0 + 15.0  # SmartOrderExecutor exit market_fill_timeout_sec + straddle_bridge.py's under-fill retry loop
    assert SellStraddleStrategy._CLOSE_CONFIRM_TIMEOUT_SEC >= _bridge_worst_case_sec + 10.0, (
        "close-confirm timeout no longer has safe margin over the bridge's worst-case fill "
        "latency -- a slow (but real) exit confirmation would be dropped, leaving the position "
        "marked open and causing EOD to redispatch a brand new real close order every cycle."
    )


def test_close_position_finalizes_when_bridge_confirmation_is_delayed_past_old_timeout(monkeypatch):
    """Reproduces the real incident at test scale: a delayed bridge confirmation that would have
    been dropped by the OLD 15s timeout (proportionally, arrives after the 'old' cutoff but
    before the 'new' one) must still let _close_position finalize -- proving the fixed timeout
    actually catches a same-cycle late confirmation instead of leaving the position open for
    EOD to redispatch a duplicate real order on the next tick."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    # Scale the real 15s(old)/35s(new)/~23s(bridge) numbers down by 100x for a fast test.
    old_timeout, new_timeout, bridge_delay = 0.15, 0.35, 0.23
    monkeypatch.setattr(type(ss), "_CLOSE_CONFIRM_TIMEOUT_SEC", new_timeout)

    async def _delayed_emit(ev):
        async def _deliver():
            await asyncio.sleep(bridge_delay)  # simulates the bridge's real ~23s worst case
            fill = StraddleFillEvent(
                action="EXIT", underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id="C", binding_id="B", event_id=ev.event_id,
                legs=ev.legs,
            )
            ss._on_fill(fill)
        asyncio.create_task(_deliver())

    ss._emit_order = _delayed_emit

    asyncio.run(ss._close_position("eod_squareoff"))

    # The bridge_delay (0.23) is well past what the OLD timeout (0.15) would have tolerated --
    # confirming this scenario really does reproduce the incident's dropped-confirmation shape --
    # but is comfortably under the NEW timeout (0.35), so the position must be fully closed.
    assert bridge_delay > old_timeout
    assert bridge_delay < new_timeout
    assert ss._position is None
    assert pos.status == "closed"


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
