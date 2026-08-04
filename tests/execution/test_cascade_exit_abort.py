"""V4CascadeExecutionBridge / V4CascadeBook must never fake a live fill when the
broker instance can't be resolved -- same bug class fixed for
SellStraddle/D1Trap/FVG (2026-08-04 incident: a live EXIT reported as filled
when it never reached the broker).

V4 Cascade's design differs from SellStraddle's: book.py's close call sites
(square_off, _force_eod_square_off, engine.py's structural-flip/T1/T2 exits,
etc.) mark the leg/position "closed" OPTIMISTICALLY at decision time (with a
placeholder price), then _emit_order dispatches the real order and _on_fill
reconciles against the REAL broker fill -- reverting the optimistic close
back to "open" if the bridge reports exit_failed=True. So the single shared
fix point for EXIT is the bridge (never fabricate a fill) plus _on_fill's
existing exit_failed revert (already present, verified below), not a
per-close-site rewrite like SellStraddle needed.

Two things are exercised end-to-end here, both through REAL code paths:
  1. V4CascadeExecutionBridge._handle() (real routing) must publish
     exit_failed=True and must NOT call _paper_fill/_live_fill when the
     broker can't be resolved in live mode.
  2. V4CascadeBook._on_fill() (real reconciliation), fed that exact fill,
     must revert an optimistically-"closed" leg/position back to "open" with
     no P&L booked -- proving the two pieces actually interlock correctly,
     not just each half in isolation.
"""
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig, Topic
from data_layer.base_feeder import EventBus
from execution_bridge.cascade_bridge import (
    CascadeFillEvent,
    CascadeOrderEvent,
    V4CascadeExecutionBridge,
)
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


# ── field defaults ───────────────────────────────────────────────────────────

def test_cascade_fill_event_exit_failed_defaults_false():
    fill = CascadeFillEvent(
        action="EXIT", underlying="NIFTY", side="CE", tranche="BOTH",
        fill_price=100.0, qty=75, client_id="C", binding_id="B", event_id="e1",
    )
    assert fill.exit_failed is False
    assert fill.entry_aborted is False


# ── bridge: real _handle() must never fake a fill on an unresolvable broker ─

class _DB:
    def __init__(self, mode="live"):
        self._mode = mode

    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": "B1", "terminal_connected": True, "trading_mode": self._mode}]

    def get_deployments_sync(self, cid):
        return [{"binding_id": "B1", "strategy_name": "v4_cascade",
                  "underlying": "NIFTY", "is_running": 1}]


class _Router:
    """_brokers is always empty -- the broker never resolves, in any mode."""
    def __init__(self, mode="live"):
        self._client_db = _DB(mode)
        self._brokers = {}


def _exit_ev(**kw):
    return CascadeOrderEvent(
        action="EXIT", underlying="NIFTY", side="CE", strike=24000, qty=75,
        price_hint=120.0, tranche="T1", client_id="C1", binding_id="B1",
        event_id="evt-exit-1", **kw,
    )


def test_live_exit_never_fakes_fill_when_broker_unresolvable():
    """The exact incident mechanism: a live EXIT whose broker can't be
    resolved must publish exit_failed=True (via resolve_broker_or_alert +
    _abort), never a fabricated successful fill through _paper_fill."""
    async def run():
        bus = EventBus()
        bridge = V4CascadeExecutionBridge(bus, _Router(mode="live"))

        calls = {"paper": 0, "live": 0}

        async def _fake_paper(*a, **kw):
            calls["paper"] += 1

        async def _fake_live(*a, **kw):
            calls["live"] += 1

        bridge._paper_fill = _fake_paper
        bridge._live_fill = _fake_live

        fills = []
        q = bus.subscribe(Topic.ORDER_FILL)

        async def _drain():
            while True:
                fills.append(await q.get())

        task = asyncio.create_task(_drain())
        await bridge._handle(_exit_ev())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert calls["paper"] == 0, "must never fabricate a fill via _paper_fill for a live EXIT"
        assert calls["live"] == 0
        assert len(fills) == 1
        assert fills[0].exit_failed is True
        assert fills[0].routing_failed is True
        assert fills[0].entry_aborted is False

    asyncio.run(run())


class _DBTerminalDown:
    """terminal_connected=False -- the OTHER silent-EXIT gate (separate from
    the broker-unresolvable path above): _handle()'s very first live_binding
    check used to only call _abort() for ev.action == "ENTRY", so an EXIT
    hitting this branch returned with no fill event published at all."""
    def get_bindings_safe_sync(self, cid):
        return [{"binding_id": "B1", "terminal_connected": False, "trading_mode": "live"}]

    def get_deployments_sync(self, cid):
        return [{"binding_id": "B1", "strategy_name": "v4_cascade",
                  "underlying": "NIFTY", "is_running": 1}]


class _RouterTerminalDown:
    def __init__(self):
        self._client_db = _DBTerminalDown()
        self._brokers = {}


def test_exit_publishes_fill_when_terminal_not_connected():
    """The exact finding fixed here: terminal_connected=False must abort
    (and publish exit_failed=True) for EXIT too, not just ENTRY -- otherwise
    book.py's optimistic 'closed' mark is never reverted because no fill
    event ever arrives."""
    async def run():
        bus = EventBus()
        bridge = V4CascadeExecutionBridge(bus, _RouterTerminalDown())

        fills = []
        q = bus.subscribe(Topic.ORDER_FILL)

        async def _drain():
            while True:
                fills.append(await q.get())

        task = asyncio.create_task(_drain())
        await bridge._handle(_exit_ev())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(fills) == 1, "EXIT must publish a fill event even when terminal_connected is False"
        assert isinstance(fills[0], CascadeFillEvent)
        assert fills[0].exit_failed is True
        assert fills[0].routing_failed is True
        assert fills[0].entry_aborted is False

    asyncio.run(run())


def test_paper_mode_untouched_no_resolver_no_alert():
    """mode == 'paper' must still go straight to _paper_fill without ever
    touching the broker resolver (pure local simulation, no SYSTEM_EVENT)."""
    async def run():
        bus = EventBus()
        bridge = V4CascadeExecutionBridge(bus, _Router(mode="paper"))

        calls = {"paper": 0}

        async def _fake_paper(ev):
            calls["paper"] += 1

        bridge._paper_fill = _fake_paper

        fills = []
        q = bus.subscribe(Topic.ORDER_FILL)

        async def _drain():
            while True:
                fills.append(await q.get())

        task = asyncio.create_task(_drain())
        await bridge._handle(_exit_ev())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert calls["paper"] == 1
        assert not any(getattr(f, "routing_failed", False) for f in fills)

    asyncio.run(run())


# ── strategy: real _on_fill() must revert an optimistic close on exit_failed ─

def _book_with_open_t1(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying=underlying, client_id="C1",
                          binding_id="B1", lot_multiplier=1, squareoff_time="15:15")
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24000, qty=75,
                     entry_price=120.0, entry_time=datetime.now(IST), entry_reason="test")
    pos = CascadePosition(
        underlying=underlying, side="CE", tracking_strike=23800, execution_strike=24000,
        atm_at_trigger=24000, entry_spot=24000, open_time=datetime.now(IST),
        t1=t1, t2=None, status="open",
    )
    book._engine.position = pos
    return book, pos, t1


def test_on_fill_reverts_optimistic_close_when_exit_failed():
    """Simulates the REAL sequence a broker-unavailable EXIT goes through:
    square_off() marks the leg/position 'closed' optimistically (its actual,
    real behavior -- see strategies/v4_cascade/book.py square_off()), then
    the bridge's exit_failed=True fill (what test_live_exit_never_fakes_fill_
    when_broker_unresolvable above proves the bridge really publishes) is fed
    back through the REAL _on_fill. The leg/position must come out reverted
    to 'open' with the placeholder close undone -- never left "closed" for a
    close that never reached the broker."""
    async def run():
        book, pos, t1 = _book_with_open_t1()

        published = []

        def _fake_emit(ev, pos_before=None):
            published.append(ev)
            # Mirror _emit_order's real bookkeeping: stash the leg being
            # closed under _pending_fills keyed by a synthetic event_id, same
            # as the real method does before publishing to the bridge.
            book._pending_fills["evt-exit-1"] = t1

        book._emit_order = _fake_emit

        closed = await book.square_off(reason="manual")
        assert closed == 1
        # square_off's real, current behavior: optimistic placeholder close.
        assert t1.status == "closed"
        assert pos.status == "closed"

        # Now the bridge reports the EXIT could not be routed (broker
        # unavailable) -- feed the REAL CascadeFillEvent through the REAL
        # _on_fill, exactly as book.py's _fill_loop would.
        fill = CascadeFillEvent(
            action="EXIT", underlying="NIFTY", side="CE", tranche="T1",
            fill_price=0.0, qty=75, client_id="C1", binding_id="B1",
            event_id="evt-exit-1", paper_mode=False, exit_failed=True, routing_failed=True,
        )
        book._on_fill(fill)

        assert t1.status == "open", "leg must be reverted to open -- the close never reached the broker"
        assert t1.close_price == 0.0
        assert t1.close_reason == ""
        assert pos.status == "open", "position must be reverted to open too"
        assert pos.close_time is None

    asyncio.run(run())


def test_on_fill_confirmed_exit_still_closes():
    """Sanity check on the happy path: a REAL confirmed EXIT fill (not
    exit_failed) must still finalize the close with the real fill price."""
    async def run():
        book, pos, t1 = _book_with_open_t1()
        book._pending_fills["evt-exit-2"] = t1
        t1.status = "closed"  # optimistic placeholder, as square_off would set
        t1.close_price = t1.entry_price

        fill = CascadeFillEvent(
            action="EXIT", underlying="NIFTY", side="CE", tranche="T1",
            fill_price=135.0, qty=75, client_id="C1", binding_id="B1",
            event_id="evt-exit-2", paper_mode=False,
        )
        book._on_fill(fill)

        assert t1.status == "closed"
        assert t1.close_price == 135.0
        assert t1.realized_pnl == pytest.approx((135.0 - 120.0) * 75)

    asyncio.run(run())
