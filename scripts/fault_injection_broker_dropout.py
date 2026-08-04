"""
scripts/fault_injection_broker_dropout.py — pre-commercial fault-injection harness.

Runs REAL strategy + REAL execution-bridge code (SellStraddleStrategy,
StraddleExecutionBridge) against a fake router whose `_brokers` dict can be told
to "go missing" for a window — reproducing the exact ExecutionRouter._brokers gap
seen in the 2026-08-04 gurmeet incident (a live SellStraddle EXIT was silently
"confirmed" by the bridge even though the order never reached Zerodha, because the
bridge fell back to a local simulated fill instead of refusing to fake success).

Tasks 2-6 (already merged) fixed this at the bridge layer (`resolve_broker_or_alert`,
`execution_bridge/broker_resolve.py`) and, for SellStraddle specifically, at the
strategy layer too (`strategies/sell_straddle/exits.py` confirm-then-finalize via
`_roll_close_waiters`, so an aborted EXIT leaves `self._position` genuinely
untouched instead of being nulled optimistically before confirmation).

This script is the operational regression guard for both fixes together: it drives
a REAL strategy instance with a REAL open position through a REAL bridge whose
router is forced into the "broker unavailable" state, and asserts:
  1. No fabricated/paper fill is logged during the dropout ("[PAPER]" tag never
     appears in execution_bridge.straddle_bridge's logger output).
  2. The bridge publishes an aborted fill (`exit_aborted`/`entry_aborted` +
     `routing_failed`), never a fake successful one.
  3. Exactly one BROKER_UNAVAILABLE SYSTEM_EVENT fires per dropout.
  4. The strategy's own state is left exactly as it was before the attempt — an
     open position stays open (EXIT scenario), an optimistic entry is discarded
     (ENTRY scenario) — never silently "confirmed" or left half-applied.

Usage: python3 scripts/fault_injection_broker_dropout.py
Exits non-zero if any implemented scenario fails.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import date, datetime

# Allow `python3 scripts/fault_injection_broker_dropout.py` from any cwd by putting
# the repo root (parent of this scripts/ dir) on sys.path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fault_injection")


# ── Fault-injection scaffolding ─────────────────────────────────────────────


class TogglableRouter:
    """A router whose `_brokers` dict can be forced empty on demand, to simulate the
    exact ExecutionRouter._brokers gap seen in the 2026-08-04 incidents. Also carries
    a `_client_db` stand-in, since every bridge's routing method reads
    `getattr(router, "_client_db", None)` to look up binding/deployment state."""

    def __init__(self, real_brokers: dict, client_db=None):
        self._real = real_brokers
        self._dropped = False
        self._client_db = client_db

    @property
    def _brokers(self):
        return {} if self._dropped else self._real

    def drop(self):
        self._dropped = True

    def restore(self):
        self._dropped = False


class AlertCollector:
    """Wraps a real EventBus, recording every SYSTEM_EVENT published so the script
    can assert BROKER_UNAVAILABLE fired for each induced dropout."""

    def __init__(self, real_bus):
        self._real = real_bus
        self.system_events = []

    def subscribe(self, topic):
        return self._real.subscribe(topic)

    def unsubscribe(self, topic, q):
        return self._real.unsubscribe(topic, q)

    async def publish(self, topic, event):
        from config.global_config import Topic
        if topic == Topic.SYSTEM_EVENT:
            self.system_events.append(event)
        await self._real.publish(topic, event)


class _LogCapture(logging.Handler):
    """Captures formatted messages from a given logger for the duration of a `with`
    block, so a scenario can assert no [PAPER]-tagged fabricated-fill line was ever
    emitted during a dropout."""

    def __init__(self, logger_name: str):
        super().__init__()
        self._target = logging.getLogger(logger_name)
        self.records: list = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record.getMessage())

    def __enter__(self):
        self._target.addHandler(self)
        return self

    def __exit__(self, *exc):
        self._target.removeHandler(self)


class _FakeClientDB:
    """Minimal stand-in for data_layer.client_db.ClientDB — only the two sync lookup
    methods every bridge's routing loop actually calls."""

    def __init__(self, mode: str = "live", is_running: int = 1, strategy_name: str = "sell_straddle"):
        self._mode = mode
        self._is_running = is_running
        self._strategy_name = strategy_name

    def get_bindings_safe_sync(self, cid):
        return [{
            "binding_id": f"{cid}_b1",
            "engine_active": True,
            "terminal_connected": True,
            "trading_mode": self._mode,
        }]

    def get_deployments_sync(self, cid):
        return [{
            "binding_id": f"{cid}_b1",
            "strategy_name": self._strategy_name,
            "underlying": "NIFTY",
            "is_running": self._is_running,
        }]


class _FakeClient:
    def __init__(self, cid: str):
        self.client_id = cid


class _FakeRegistry:
    def __init__(self, client_ids):
        self._clients = [_FakeClient(c) for c in client_ids]

    def all_active(self):
        return self._clients


async def run_scenario(name: str, coro_factory) -> bool:
    """Run one fault-injection scenario, return True if it passed."""
    logger.info("=== SCENARIO: %s ===", name)
    try:
        ok = await coro_factory()
        logger.info("=== %s: %s ===", name, "PASS" if ok else "FAIL")
        return ok
    except Exception:
        logger.exception("=== %s: ERROR ===", name)
        return False


# ── Shared harness for the SellStraddle scenarios ───────────────────────────


def _make_bridge_and_strategy(mode: str = "live", is_running: int = 1):
    """Build a REAL StraddleExecutionBridge wired to a REAL SellStraddleStrategy over a
    shared EventBus, with the router's broker map pre-dropped. Returns
    (bus, alert_bus, router, bridge, strategy)."""
    from config.global_config import GlobalConfig
    from data_layer.base_feeder import EventBus
    from execution_bridge.straddle_bridge import StraddleExecutionBridge
    from strategies.sell_straddle import SellStraddleStrategy

    bus = EventBus()
    alert_bus = AlertCollector(bus)

    real_brokers = {"A": {"A_b1": object()}}  # would resolve fine if not dropped
    client_db = _FakeClientDB(mode=mode, is_running=is_running)
    router = TogglableRouter(real_brokers, client_db=client_db)
    router.drop()  # simulate the exact broker-map gap from the incident

    registry = _FakeRegistry(["A"])
    bridge = StraddleExecutionBridge(alert_bus, registry, router)

    ss = SellStraddleStrategy(bus, GlobalConfig(), underlying="NIFTY",
                               client_id="A", binding_id="A_b1")
    return bus, alert_bus, router, bridge, ss


async def _run_with_bridge_and_fill_loop(bus, bridge, ss, body):
    """Run `body()` while the REAL bridge.run() and REAL ss._fill_loop() consume the
    shared EventBus in the background — the same end-to-end wiring production uses
    (strategy publishes ORDER_REQUEST -> bridge routes -> publishes ORDER_FILL ->
    strategy's own fill loop calls _on_fill), not a shortcut that calls _on_fill
    directly."""
    ss._running = True
    bridge_task = asyncio.create_task(bridge.run())
    fill_task = asyncio.create_task(ss._fill_loop())
    try:
        await body()
    finally:
        ss._running = False
        bridge.stop()
        for t in (bridge_task, fill_task):
            t.cancel()
        await asyncio.gather(bridge_task, fill_task, return_exceptions=True)


# ── Scenario 1: broker drops mid-EXIT (the 2026-08-04 gurmeet incident) ─────


async def scenario_broker_drop_mid_exit() -> bool:
    """
    Reproduces the 2026-08-04 gurmeet incident directly: a live SellStraddle position
    gets a Day-Loss-SL EXIT decision while the router's broker entry is (simulated)
    transiently missing. Assert: no [PAPER] fill is faked, a BROKER_UNAVAILABLE
    SYSTEM_EVENT fires, and the strategy's position is STILL OPEN afterward (not
    falsely closed) -- this is the actual regression the incident represents; the
    bridge-only fix (Task 2) alone would not catch a strategy that still finalized
    the close optimistically, which is exactly what the pre-fix _close_position did.
    """
    from config.global_config import IST, SysEvent
    from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition

    bus, alert_bus, router, bridge, ss = _make_bridge_and_strategy(mode="live")

    pos = StraddlePosition(
        underlying=ss._underlying,
        atm_at_entry=24500.0,
        entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", 24500.0, 120.0, 120.0, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24500.0, 110.0, 110.0, open_time=datetime.now(IST)),
        net_credit=230.0,
        open_time=datetime.now(IST),
        status="open",
        lot_size=ss._lot_size * ss._lot_multiplier,
        expiry_date=date.today(),
    )
    ss._position = pos

    with _LogCapture("execution_bridge.straddle_bridge") as cap:
        async def body():
            await ss._close_position("day_loss_sl")
        await _run_with_bridge_and_fill_loop(bus, bridge, ss, body)

    no_fake_paper_fill = not any("[PAPER]" in m for m in cap.records)
    alert_ok = (
        len(alert_bus.system_events) == 1
        and alert_bus.system_events[0].get("event") == SysEvent.BROKER_UNAVAILABLE
    )
    position_untouched = (
        ss._position is pos
        and ss._position.status == "open"
        and ss._position.ce_leg.entry_price == 120.0
        and ss._position.pe_leg.entry_price == 110.0
    )
    no_pnl_booked = ss._session_realized_pnl_pts == 0.0
    free_to_retry = ss._close_in_progress is False

    ok = no_fake_paper_fill and alert_ok and position_untouched and no_pnl_booked and free_to_retry
    if not ok:
        logger.error(
            "broker_drop_mid_exit detail: no_fake_paper_fill=%s alert_ok=%s(n=%d) "
            "position_untouched=%s no_pnl_booked=%s free_to_retry=%s",
            no_fake_paper_fill, alert_ok, len(alert_bus.system_events),
            position_untouched, no_pnl_booked, free_to_retry,
        )
    return ok


# ── Scenario 2: broker drops mid-ENTRY ──────────────────────────────────────


async def scenario_broker_drop_mid_entry() -> bool:
    """
    ENTRY-side equivalent of scenario 1. SellStraddle's entry path sets
    self._position OPTIMISTICALLY (and self._order_pending=True, self._trades_today
    += 1) BEFORE the order is confirmed, then corrects it in _on_fill once the real
    fill (or an aborted one) arrives -- see strategies/sell_straddle/entries.py. If
    the broker is unavailable during that window, the bridge must publish an
    entry_aborted+routing_failed fill (never a fabricated fill), and the strategy
    must discard the optimistic position/pending flag/trade-count bump, not leave a
    half-open "position" nothing was ever sent to the broker for.
    """
    from config.global_config import IST, SysEvent
    from execution_bridge.straddle_bridge import StraddleOrderEvent
    from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition

    bus, alert_bus, router, bridge, ss = _make_bridge_and_strategy(mode="live", is_running=1)

    # Mirror entries.py's optimistic-entry sequence up to the point it calls _emit_order.
    ce_strike, pe_strike, ce_ltp, pe_ltp = 24500.0, 24500.0, 120.0, 110.0
    now = datetime.now(IST)
    pos = StraddlePosition(
        underlying=ss._underlying,
        atm_at_entry=24500.0,
        entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=now),
        pe_leg=StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=now),
        net_credit=ce_ltp + pe_ltp,
        open_time=now,
        status="open",
        lot_size=ss._lot_size * ss._lot_multiplier,
        expiry_date=date.today(),
    )
    ss._position = pos
    ss._order_pending = True
    ss._trades_today = 1
    ss._initial_net_credit = ce_ltp + pe_ltp

    order_ev = StraddleOrderEvent(
        action="ENTRY", underlying=ss._underlying, atm=24500.0,
        ce_strike=ce_strike, pe_strike=pe_strike, ce_ltp=ce_ltp, pe_ltp=pe_ltp,
        lot_multiplier=ss._lot_multiplier, lot_size=ss._lot_size,
        event_id="fault_inject_entry_1",
    )

    with _LogCapture("execution_bridge.straddle_bridge") as cap:
        async def body():
            await ss._emit_order(order_ev)
            # resolve_broker_or_alert retries 3x with a 1s delay between attempts (~2s)
            # before it gives up and publishes the abort fill; poll rather than a flat
            # sleep so this is both robust and no slower than necessary.
            for _ in range(100):  # up to ~5s
                if ss._position is None:
                    break
                await asyncio.sleep(0.05)
        await _run_with_bridge_and_fill_loop(bus, bridge, ss, body)

    no_fake_paper_fill = not any("[PAPER]" in m for m in cap.records)
    alert_ok = (
        len(alert_bus.system_events) == 1
        and alert_bus.system_events[0].get("event") == SysEvent.BROKER_UNAVAILABLE
    )
    entry_discarded = (
        ss._position is None
        and ss._order_pending is False
        and ss._trades_today == 0
    )

    ok = no_fake_paper_fill and alert_ok and entry_discarded
    if not ok:
        logger.error(
            "broker_drop_mid_entry detail: no_fake_paper_fill=%s alert_ok=%s(n=%d) "
            "position=%r order_pending=%s trades_today=%s",
            no_fake_paper_fill, alert_ok, len(alert_bus.system_events),
            ss._position, ss._order_pending, ss._trades_today,
        )
    return ok


# ── TODO scenarios: deliberately NOT implemented here ───────────────────────
#
# These need infrastructure well beyond a broker-dropout harness (a real feed
# simulator, a real restart/process boundary, or a real multi-tick backtest replay
# of the exact BearTrap timing bug) -- implementing a shallow version would just be a
# tautology that always passes, not a real regression guard. Left as documented TODOs
# rather than fabricated:
#
#   - feed_drop_mid_position: requires a controllable fake feeder publishing (or
#     withholding) INDEX_TICK/OPTION_TICK so the strategy's own tick/candle loops can
#     be driven through a live data outage while a position is open. This script only
#     drives the ORDER_REQUEST/ORDER_FILL side of the bus; a real feed-drop harness is
#     a separate, larger piece of infrastructure (see data_layer/base_feeder.py,
#     GlobalFeeder heartbeat/provider-switch logic).
#
#   - restart_mid_position: requires exercising the actual position_store save/load
#     round-trip (data_layer/position_store.py) across a simulated process restart --
#     i.e. tear down the in-memory SellStraddleStrategy entirely and reconstruct a new
#     one from persisted state, then verify it reconciles correctly against a live
#     broker position. Out of scope for this broker-dropout-focused script.
#
#   - duplicate_entry_regression: the BearTrap 10:04:00 double-entry issue referenced
#     in project handoff notes is a *different* engine (D1TrapBearOnlyBook, tranche
#     T1/T2 timing) with its own entry state machine; reproducing it needs a scripted
#     multi-tick replay against that book's zone/tranche logic, not a broker-dropout
#     fault. Needs its own harness once that regression is triaged.


SCENARIOS = {
    "broker_drop_mid_exit": scenario_broker_drop_mid_exit,
    "broker_drop_mid_entry": scenario_broker_drop_mid_entry,
    # TODO: feed_drop_mid_position — needs a controllable fake feeder (see note above).
    # TODO: restart_mid_position — needs a real position_store save/load round-trip across
    #       a simulated process restart (see note above).
    # TODO: duplicate_entry_regression — BearTrap 10:04:00 double-entry; needs a scripted
    #       multi-tick replay against D1TrapBearOnlyBook's tranche state machine (see note above).
}


async def main():
    results = {}
    for name, factory in SCENARIOS.items():
        results[name] = await run_scenario(name, factory)
    failed = [n for n, ok in results.items() if not ok]
    if failed:
        logger.error("FAILED scenarios: %s", failed)
        sys.exit(1)
    logger.info("All fault-injection scenarios passed (%d implemented; see the TODO "
                "block in this file for scenarios intentionally left unimplemented).",
                len(results))


if __name__ == "__main__":
    asyncio.run(main())
