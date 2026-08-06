"""2026-08-06 CRITICAL FIX: StraddleExecutionBridge.run() used to `await self._handle(ev)`
directly inside its single consumer loop, so EVERY client's orders funneled through one
queue were processed strictly one-at-a-time, globally -- a slow order for one client
blocked every other client's order from even starting. Real incident: gurmeet and
ssrajpal2001 both hit their 15:20 EOD force-exit within 13ms of each other; gurmeet's
order sat queued behind ssrajpal2001's and didn't reach the broker until ~16s later,
blowing past gurmeet's own strategy-side confirm wait even though the order itself filled
in under a second once actually picked up.

Fixed: different (client_id, binding_id) keys now run on fully independent, concurrent
asyncio tasks. A single client's OWN events must still process strictly in the order
they were published (no reordering/racing within one client's own order stream)."""
import asyncio

from config.global_config import Topic
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleExecutionBridge, StraddleOrderEvent


def _ev(client_id, binding_id, tag) -> StraddleOrderEvent:
    return StraddleOrderEvent(action="EXIT", underlying="NIFTY", atm=24650,
                              ce_strike=24700, pe_strike=24600, ce_ltp=90.0, pe_ltp=70.0,
                              lot_size=65, lot_multiplier=1, client_id=client_id,
                              binding_id=binding_id, close_reason=tag)


def test_different_clients_process_concurrently_not_queued_behind_each_other(monkeypatch):
    """The core fix: client B's order must start (and can finish) WITHOUT waiting for
    client A's slower order to complete first -- reproduces the gurmeet/ssrajpal2001
    EOD collision at test scale."""
    async def run():
        bus = EventBus()
        br = StraddleExecutionBridge(bus, registry=None, router=None)

        events: list = []

        async def _fake_handle(ev):
            events.append(("start", ev.client_id))
            if ev.client_id == "slow_client":
                await asyncio.sleep(0.3)  # simulates a client whose order takes a while
            events.append(("end", ev.client_id))

        br._handle = _fake_handle

        run_task = asyncio.create_task(br.run())
        await bus.publish(Topic.ORDER_REQUEST, _ev("slow_client", "B1", "eod_squareoff"))
        await asyncio.sleep(0.02)  # let the bridge pick up and start the slow order first
        await bus.publish(Topic.ORDER_REQUEST, _ev("fast_client", "B2", "eod_squareoff"))

        # fast_client's order must both START and FINISH well before slow_client's 0.3s
        # completes -- proving it was never queued behind it.
        await asyncio.sleep(0.15)

        br._running = False
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

        fast_events = [e for e in events if e[1] == "fast_client"]
        assert fast_events == [("start", "fast_client"), ("end", "fast_client")], (
            f"fast_client's order did not complete promptly -- it was likely queued behind "
            f"slow_client instead of running concurrently. events={events}"
        )
        # slow_client should have started but not finished yet at the 0.15s checkpoint.
        assert ("start", "slow_client") in events
        assert ("end", "slow_client") not in events

    asyncio.run(run())


def test_same_client_events_still_process_strictly_in_order(monkeypatch):
    """Safety check: the fix must NOT let a single client's own events race each other --
    two orders for the SAME (client,binding) must still run strictly sequentially, in the
    order they were published."""
    async def run():
        bus = EventBus()
        br = StraddleExecutionBridge(bus, registry=None, router=None)

        order: list = []

        async def _fake_handle(ev):
            order.append(f"start-{ev.close_reason}")
            await asyncio.sleep(0.05)
            order.append(f"end-{ev.close_reason}")

        br._handle = _fake_handle

        run_task = asyncio.create_task(br.run())
        await bus.publish(Topic.ORDER_REQUEST, _ev("gurmeet", "zerodha", "first"))
        await asyncio.sleep(0.01)
        await bus.publish(Topic.ORDER_REQUEST, _ev("gurmeet", "zerodha", "second"))

        await asyncio.sleep(0.2)

        br._running = False
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

        # "second" must never start before "first" finishes -- same-client ordering intact.
        assert order == ["start-first", "end-first", "start-second", "end-second"], order

    asyncio.run(run())
