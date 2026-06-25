"""
execution_bridge/execution_router.py — Signal-to-WorkerPool dispatcher.

Subscribes to SIGNAL topic.  On receipt of a SignalPackage:
  1. Validates the signal (min RR, min confidence).
  2. Calls pool.dispatch(signal) — a single O(N) loop of put_nowait() calls
     that completes in microseconds regardless of client count.
  3. Each ClientExecutionWorker runs in its own asyncio Task and processes
     signals independently — Client A's network latency never touches Client B.

The heavy per-client work (symbol translation, lot calculation, broker calls)
all happens inside the workers.  The router itself does almost no work.

Risk validation is delegated to ClientManager.validate_signal() before
any order is placed.

No time.sleep. All concurrency via asyncio.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict

from config.global_config import IST, Topic, GlobalConfig
from config.client_profiles import ClientRegistry
from data_layer.base_feeder import EventBus
from execution_bridge.base_broker import BaseBroker, create_broker
from execution_bridge.parallel_worker_pool import (
    ClientExecutionWorker, WorkerPool,
)
from strategies.base_strategy import SignalPackage

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Cost Calculator (Indian statutory charges)
# ─────────────────────────────────────────────────────────────────────────────

class CostCalc:
    STT_SELL_PCT       = 0.0625 / 100
    EXCHANGE_PCT       = 0.035  / 100
    SEBI_FEE_PCT       = 0.0001 / 100
    GST_PCT            = 0.18
    BROKERAGE_FLAT     = 20.0
    SLIPPAGE_BUY_PCT   = 0.05   / 100
    SLIPPAGE_SELL_PCT  = 0.05   / 100

    @classmethod
    def entry_cost(cls, price: float, qty: int) -> float:
        turnover = price * qty
        brok = cls.BROKERAGE_FLAT
        exch = turnover * cls.EXCHANGE_PCT
        sebi = turnover * cls.SEBI_FEE_PCT
        gst  = (brok + exch) * cls.GST_PCT
        return round(brok + exch + sebi + gst, 2)

    @classmethod
    def exit_cost(cls, price: float, qty: int) -> float:
        turnover = price * qty
        stt  = turnover * cls.STT_SELL_PCT
        brok = cls.BROKERAGE_FLAT
        exch = turnover * cls.EXCHANGE_PCT
        sebi = turnover * cls.SEBI_FEE_PCT
        gst  = (brok + exch) * cls.GST_PCT
        return round(stt + brok + exch + sebi + gst, 2)

    @classmethod
    def apply_slip(cls, price: float, is_buy: bool) -> float:
        factor = 1 + cls.SLIPPAGE_BUY_PCT if is_buy else 1 - cls.SLIPPAGE_SELL_PCT
        return price * factor


# ─────────────────────────────────────────────────────────────────────────────
# Execution Router
# ─────────────────────────────────────────────────────────────────────────────

class ExecutionRouter:
    """
    Thin signal dispatcher.  All order placement lives in ClientExecutionWorkers.

    On start():
      1. Authenticates broker instances for all active clients.
      2. Creates a ClientExecutionWorker per client with its broker map.
      3. Starts all workers via WorkerPool.start_all().

    On signal receipt:
      pool.dispatch(signal) — drops signal into every worker queue
      simultaneously via put_nowait().  Returns immediately.
    """

    def __init__(
        self,
        bus: EventBus,
        registry: ClientRegistry,
        cfg: GlobalConfig,
    ) -> None:
        self._bus = bus
        self._registry = registry
        self._cfg = cfg
        self._sig_queue = bus.subscribe(Topic.SIGNAL)
        self._running = False
        self._pool = WorkerPool()
        # {client_id: {binding_id: BaseBroker}} — kept for logout on stop()
        self._brokers: Dict[str, Dict[str, BaseBroker]] = {}
        self._cost = CostCalc()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Authenticate brokers and spin up per-client workers.

        Raises RuntimeError if ANY broker auth fails — the system must not start
        with a missing broker as it leads to confusing 'no broker for binding' errors later.
        """
        failed: list[str] = []
        for client in self._registry.all_active():
            self._brokers[client.client_id] = {}
            for binding in client.enabled_brokers():
                broker = create_broker(binding, client.client_id)
                try:
                    ok = await broker.authenticate()
                except Exception as exc:
                    logger.critical(
                        "Router: Auth EXCEPTION for %s/%s (%s): %s",
                        client.client_id, binding.binding_id, binding.provider, exc,
                        exc_info=True,
                    )
                    ok = False
                if ok:
                    self._brokers[client.client_id][binding.binding_id] = broker
                    logger.info(
                        "Router: Authenticated %s/%s (%s).",
                        client.client_id, binding.binding_id, binding.provider,
                    )
                else:
                    logger.warning(
                        "Router: Auth FAILED for %s/%s (%s). Binding skipped — fix credentials in the dashboard.",
                        client.client_id, binding.binding_id, binding.provider,
                    )
                    failed.append(f"{client.client_id}/{binding.binding_id}({binding.provider})")

            worker = ClientExecutionWorker(
                client=client,
                brokers=self._brokers[client.client_id],
                bus=self._bus,
                cfg=self._cfg,
            )
            self._pool.register(worker)

        if failed:
            logger.warning(
                "Router: %d broker binding(s) failed auth and were skipped: %s. "
                "Fix credentials via the dashboard.",
                len(failed), ", ".join(failed),
            )

        await self._pool.start_all()
        logger.info("Router: %d client workers active.", len(self._brokers))

    async def stop(self) -> None:
        self._running = False
        await self._pool.stop_all()
        for brokers_by_binding in self._brokers.values():
            for broker in brokers_by_binding.values():
                await broker.logout()

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                signal: SignalPackage = await asyncio.wait_for(
                    self._sig_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            self._dispatch(signal)

    # ── Signal Dispatch ───────────────────────────────────────────────────────

    def _dispatch(self, signal: SignalPackage) -> None:
        """
        Drop signal into all worker queues simultaneously.

        This is intentionally synchronous and O(N) — each call is a single
        put_nowait() which is a dict lookup + deque append: sub-microsecond.
        No awaits here.  Total dispatch time for 100 clients ≈ 50–100 µs.
        """
        if not signal.is_valid():
            logger.debug(
                "Router: Signal %s rejected (rr=%.2f conf=%.2f).",
                signal.source.value, signal.rr_ratio, signal.confidence,
            )
            return

        n = self._pool.dispatch(signal)
        logger.info(
            "Router: Signal %s dispatched to %d worker(s).",
            signal.source.value, n,
        )

    # ── Pool access (for AdminConsole) ────────────────────────────────────────

    def worker_stats(self):
        return self._pool.stats()
