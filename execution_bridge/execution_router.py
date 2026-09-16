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

        2026-09-16, direct user spec (real incident, see the inline comment
        at this method's own auth-failure branch below): no longer raises on
        a failed broker auth, even for a Trade-enabled binding. The whole
        system always finishes starting; a binding with no working broker
        just has no broker entry for that binding_id, and any real order
        attempted on it is refused and alerted via
        execution_bridge/broker_resolve.py's resolve_broker_or_alert(),
        never silently faked."""
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
                    # 2026-09-04 CRITICAL FIX, real incident: run_system.py's own
                    # boot-time Upstox instrument-map build (_refresh_upstox_
                    # instrument_maps) runs BEFORE router.run() is even scheduled
                    # as a task, so it can only ever inject into brokers that
                    # already exist in self._brokers at that instant -- ZERO of
                    # them, on every single boot, since THIS loop is what
                    # populates self._brokers in the first place. Confirmed live
                    # twice the same morning: map built at 09:50:12 with the
                    # correct current-week expiry, this broker didn't exist
                    # until 09:50:13 (one second later), so the earlier
                    # injection loop had nothing to inject into -- the very
                    # next SellStraddle entry on this exact binding still
                    # failed with Upstox's "Invalid Instrument key". Fix: give
                    # every broker whatever map is CURRENTLY cached in the
                    # registry the moment it becomes available, instead of
                    # relying on a fixed point in the boot sequence lining up
                    # with when authentication happens to finish. Cheap/local
                    # -- build_instrument_map() only reads already-fetched
                    # in-process data (REGISTRY.load_sync already ran
                    # synchronously earlier in run_system.py's own startup),
                    # never a network call, safe to run for every successful
                    # auth. Does NOT cover a brand-new broker connected via the
                    # dashboard's own OAuth reconnect flow mid-day (a separate
                    # code path that never calls start() again) -- still a
                    # known, smaller, deliberately deferred gap.
                    if hasattr(broker, "inject_instrument_map"):
                        from data_layer.instrument_registry import REGISTRY as _registry
                        for _idx in getattr(self._cfg, "monitored_indices", []) or []:
                            try:
                                _map = _registry.build_instrument_map(_idx)
                                if _map:
                                    broker.inject_instrument_map(_map)
                            except Exception:
                                logger.warning(
                                    "Router: could not inject instrument map for %s into %s/%s.",
                                    _idx, client.client_id, binding.binding_id,
                                )
                elif binding.is_trade_enabled:
                    # 2026-09-16 CRITICAL FIX, real incident: this used to hard-abort
                    # the ENTIRE process (raising below, which pm2 then crash-loops
                    # forever) the instant ANY Trade-enabled binding's daily broker
                    # session token had expired and not yet been re-authenticated --
                    # a NORMAL, expected daily occurrence (Upstox/Fyers/etc tokens
                    # are day-scoped), not a genuine misconfiguration. Confirmed as
                    # a real deadlock: since a SellStraddle overnight-carry position
                    # now requires Trade staying ON through a shutdown (see
                    # strategies/core/book_manager.py's 2026-09-16 skip_close
                    # change + this file's own set_trade endpoint fix), a stale
                    # token on that SAME binding the next morning would previously
                    # crash-loop the whole app before it ever finished booting --
                    # taking the dashboard down with it, so there was no way to
                    # even reach the OAuth reconnect flow to fix it. Now: log just
                    # as loudly (CRITICAL, unchanged) but DO NOT add this binding
                    # to `failed` -- the rest of the system (every other binding,
                    # every strategy) boots normally; a real order attempt on this
                    # one broken binding hits execution_bridge/broker_resolve.py's
                    # resolve_broker_or_alert(), which already has a complete
                    # graceful "broker unavailable" fallback (retries, logs
                    # CRITICAL, publishes Topic.SYSTEM_EVENT/BROKER_UNAVAILABLE,
                    # refuses to fake a fill) built for exactly this case.
                    logger.critical(
                        "Router: Auth FAILED for %s/%s (%s) -- Trade is enabled but this "
                        "binding has NO working broker connection. System is starting "
                        "anyway; re-authenticate this binding via the dashboard's OAuth "
                        "reconnect flow. Any real order attempted on it will be refused "
                        "and alerted, never silently faked.",
                        client.client_id, binding.binding_id, binding.provider,
                    )
                else:
                    logger.warning(
                        "Router: Auth FAILED for %s/%s (%s) but trading is disabled; skipping.",
                        client.client_id, binding.binding_id, binding.provider,
                    )

            worker = ClientExecutionWorker(
                client=client,
                brokers=self._brokers[client.client_id],
                bus=self._bus,
                cfg=self._cfg,
            )
            self._pool.register(worker)

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
