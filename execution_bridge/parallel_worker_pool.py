"""
execution_bridge/parallel_worker_pool.py — Per-client isolated execution workers.

Architecture:
  ExecutionRouter drops a SignalPackage into every ClientExecutionWorker queue
  simultaneously — one put_nowait() per client.  Each worker runs its own
  independent asyncio task that:
    1. Pops signals from its isolated Queue(maxsize=100).
    2. Translates symbol, computes lots.
    3. Places orders via its dedicated broker instances.
    4. Publishes OrderFill events.

Fault isolation guarantees:
  • Client A's network latency NEVER touches Client B's execution path.
  • Router returns immediately after N put_nowait() calls — zero blocking.
  • If a worker's queue fills, the signal is dropped for THAT client only.
  • If a broker call raises an exception, it is caught inside _place() and
    inside _process() — the worker loop NEVER exits due to a broker error.
  • Each worker has an independent circuit breaker: after
    _CIRCUIT_OPEN_AFTER_FAILURES consecutive failures, the worker stops
    attempting orders and drops incoming signals until the cooldown expires.
    All other workers are completely unaffected.
  • WorkerPool._watcher_task monitors for dead tasks and restarts them
    automatically, so a catastrophic runtime error (e.g., memory error in
    the asyncio runtime itself) doesn't leave a client permanently unserved.

No time.sleep. All concurrency via asyncio.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from config.global_config import IST, Topic, GlobalConfig
from config.client_profiles import ClientProfile
from data_layer.base_feeder import EventBus
from data_layer.symbol_translator import SymbolTranslator, InternalSymbol
from execution_bridge.base_broker import BaseBroker, OrderRequest, OrderSide, OrderType
from execution_bridge.rate_limiter import ClientRateLimiterRegistry
from strategies.base_strategy import SignalPackage

logger = logging.getLogger(__name__)

_WORKER_QUEUE_SIZE = 100           # Per spec: asyncio.Queue(maxsize=100)
_CIRCUIT_OPEN_AFTER_FAILURES = 5   # Consecutive failures before circuit opens
_CIRCUIT_COOLDOWN_SECONDS = 60.0   # Seconds before automatic circuit reset attempt
_WATCHER_INTERVAL_SECONDS = 10.0   # Task health-check cadence


# ─────────────────────────────────────────────────────────────────────────────
# Strategy letter mapping (keeps router and worker consistent)
# ─────────────────────────────────────────────────────────────────────────────

_SOURCE_TO_LETTER: Dict[str, str] = {
    "OI_Zone_Breakout": "A",
    "Liquidity_Trap":   "B",
    "Panic_Scanner":    "C",
}


def strategy_letter(source_value: str) -> str:
    return _SOURCE_TO_LETTER.get(source_value, source_value[0].upper() if source_value else "?")


# ─────────────────────────────────────────────────────────────────────────────
# Per-Client Execution Worker
# ─────────────────────────────────────────────────────────────────────────────

class ClientExecutionWorker:
    """
    Isolated execution unit for a single client.

    Fault isolation: every exception from broker calls or signal processing is
    caught inside this worker.  The task loop NEVER exits due to a broker error.

    Circuit breaker: after _CIRCUIT_OPEN_AFTER_FAILURES consecutive failures
    the worker enters OPEN state and drops new signals for _CIRCUIT_COOLDOWN_SECONDS.
    It then attempts a HALF-OPEN probe; if that succeeds the circuit closes.
    """

    def __init__(
        self,
        client: ClientProfile,
        brokers: Dict[str, BaseBroker],   # {binding_id: broker}
        bus: EventBus,
        cfg: GlobalConfig,
    ) -> None:
        self._client = client
        self._brokers = brokers
        self._bus = bus
        self._cfg = cfg
        self._queue: asyncio.Queue[SignalPackage] = asyncio.Queue(maxsize=_WORKER_QUEUE_SIZE)
        self._running = False

        # Rate limiter — one TokenBucket per broker binding, keyed by binding_id
        self._rate_limiter = ClientRateLimiterRegistry(client.client_id)
        # Pre-build binding_id → provider map so _place() doesn't search the list each time
        self._binding_provider: Dict[str, str] = {
            b.binding_id: b.provider
            for b in client.broker_bindings
        }

        # Metrics
        self._processed = 0
        self._dropped = 0
        self._total_failures = 0

        # Circuit breaker state
        self._consecutive_failures: int = 0
        self._circuit_open: bool = False
        self._circuit_opened_at: float = 0.0   # asyncio monotonic time

    @property
    def client_id(self) -> str:
        return self._client.client_id

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def drop_count(self) -> int:
        return self._dropped

    @property
    def circuit_is_open(self) -> bool:
        return self._circuit_open

    # ── Called by router ──────────────────────────────────────────────────────

    def enqueue(self, signal: SignalPackage) -> None:
        """
        Non-blocking drop into this client's queue.
        Called by the router simultaneously for all clients.
        Never blocks — drops with warning if queue is full.
        """
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 10 == 1:
                logger.warning(
                    "Worker[%s]: signal queue full — dropped %d signals.",
                    self._client.client_id, self._dropped,
                )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("Worker[%s]: started.", self._client.client_id)
        while self._running:
            try:
                signal: SignalPackage = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                self._maybe_reset_circuit()
                continue

            # ── Circuit breaker guard ──────────────────────────────────────
            if self._circuit_open:
                if not self._try_half_open():
                    self._dropped += 1
                    if self._dropped % 5 == 1:
                        logger.warning(
                            "Worker[%s]: circuit OPEN — dropping signal (failures=%d, "
                            "cooldown=%.0fs remaining).",
                            self._client.client_id, self._consecutive_failures,
                            max(0.0, _CIRCUIT_COOLDOWN_SECONDS - (
                                asyncio.get_event_loop().time() - self._circuit_opened_at
                            )),
                        )
                    continue
            # ── End circuit breaker guard ──────────────────────────────────

            # Process signal — ALL exceptions caught here so the task loop survives
            try:
                await self._process(signal)
                # Successful execution resets failure streak
                if self._consecutive_failures > 0:
                    logger.info(
                        "Worker[%s]: execution succeeded — resetting failure counter "
                        "(was %d).", self._client.client_id, self._consecutive_failures,
                    )
                self._consecutive_failures = 0
                self._circuit_open = False

            except Exception as exc:
                self._consecutive_failures += 1
                self._total_failures += 1
                logger.error(
                    "Worker[%s]: _process() raised (failure %d/%d): %s",
                    self._client.client_id,
                    self._consecutive_failures, _CIRCUIT_OPEN_AFTER_FAILURES,
                    exc, exc_info=True,
                )
                if self._consecutive_failures >= _CIRCUIT_OPEN_AFTER_FAILURES:
                    self._circuit_open = True
                    self._circuit_opened_at = asyncio.get_event_loop().time()
                    logger.critical(
                        "Worker[%s]: CIRCUIT BREAKER OPENED after %d consecutive "
                        "failures. Will retry in %.0fs. Other clients unaffected.",
                        self._client.client_id,
                        self._consecutive_failures, _CIRCUIT_COOLDOWN_SECONDS,
                    )

        logger.info("Worker[%s]: stopped.", self._client.client_id)

    def stop(self) -> None:
        self._running = False

    # ── Circuit breaker helpers ───────────────────────────────────────────────

    def _maybe_reset_circuit(self) -> None:
        """Called on idle timeout — automatically close circuit after cooldown."""
        if self._circuit_open:
            elapsed = asyncio.get_event_loop().time() - self._circuit_opened_at
            if elapsed >= _CIRCUIT_COOLDOWN_SECONDS:
                self._circuit_open = False
                self._consecutive_failures = 0
                logger.info(
                    "Worker[%s]: circuit auto-reset after %.0fs cooldown.",
                    self._client.client_id, elapsed,
                )

    def _try_half_open(self) -> bool:
        """Returns True if the cooldown has elapsed and we should attempt a probe."""
        elapsed = asyncio.get_event_loop().time() - self._circuit_opened_at
        if elapsed >= _CIRCUIT_COOLDOWN_SECONDS:
            # Half-open: allow ONE signal through as a probe
            self._circuit_open = False
            self._consecutive_failures = 0
            logger.info(
                "Worker[%s]: circuit half-open probe after %.0fs.",
                self._client.client_id, elapsed,
            )
            return True
        return False

    # ── Signal processing ─────────────────────────────────────────────────────

    async def _process(self, signal: SignalPackage) -> None:
        """
        Translate and place orders for all enabled bindings.

        Raises on truly unexpected errors (caught by run() for circuit tracking).
        Individual broker call failures are caught inside _place() and do NOT
        raise — they are logged and counted at the binding level.
        """
        client = self._client
        letter = strategy_letter(signal.source.value)

        if letter not in [s.upper() for s in client.enabled_strategies]:
            return

        for binding_id, broker in self._brokers.items():
            sym_str = self._translate(signal, binding_id)
            if sym_str is None:
                continue
            lots = self._compute_lots(signal, broker)
            if lots <= 0:
                continue
            qty = lots * self._cfg.exchange.lot_sizes.get(signal.underlying, 25)
            await self._place(broker, sym_str, qty, signal)

        self._processed += 1

    async def _place(
        self,
        broker: BaseBroker,
        broker_symbol: str,
        qty: int,
        signal: SignalPackage,
    ) -> None:
        """
        Place one order and publish the fill.

        All broker exceptions are caught HERE so a single failed binding
        never propagates to _process() and never opens the circuit breaker.
        The circuit breaker only opens when _process() itself raises (i.e.
        a logic error, not a network error from a single broker call).
        """
        from execution_bridge.base_broker import OrderFill
        req = OrderRequest(
            broker_symbol=broker_symbol,
            exchange="NFO" if signal.underlying != "SENSEX" else "BFO",
            side=OrderSide.BUY,
            qty=qty,
            order_type=OrderType.MARKET,
            tag=(
                f"{self._client.client_id}_{signal.source.value}"
                f"_{signal.timestamp.strftime('%H%M%S')}"
            ),
            client_id=self._client.client_id,
        )
        try:
            provider = self._binding_provider.get(broker.binding_id, "mock")
            bucket = self._rate_limiter.get_limiter(broker.binding_id, provider)
            await bucket.acquire()
            order_id = await broker.place_order(req)
            await bucket.acquire()
            fill: OrderFill = await broker.get_order_status(order_id)
            await self._bus.publish(Topic.ORDER_FILL, fill)
            self._client.record_trade(0.0)
            logger.info(
                "Worker[%s/%s]: FILLED %s qty=%d avg=%.2f",
                self._client.client_id, broker.binding_id,
                broker_symbol, qty, fill.avg_price,
            )
        except Exception as exc:
            # Broker-level failure: log, do NOT re-raise.
            # This binding failed; other bindings in the same loop iteration continue.
            logger.error(
                "Worker[%s/%s]: place_order failed for %s: %s",
                self._client.client_id, broker.binding_id, broker_symbol, exc,
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _translate(self, signal: SignalPackage, binding_id: str) -> Optional[str]:
        expiry = self._derive_expiry()
        internal = InternalSymbol(
            underlying=signal.underlying,
            strike=signal.target_strike,
            option_type=signal.option_type,
            expiry=expiry,
        )
        provider = next(
            (b.provider for b in self._client.broker_bindings if b.binding_id == binding_id),
            "mock",
        )
        try:
            if provider == "shoonya":
                return SymbolTranslator.to_shoonya(internal)
            elif provider == "fyers":
                return SymbolTranslator.to_fyers(internal)
            elif provider == "angelone":
                return SymbolTranslator.to_angelone(internal)
            elif provider == "dhan":
                return SymbolTranslator.to_dhan_lookup_key(internal)
            elif provider == "upstox":
                return SymbolTranslator.to_upstox(internal)
            else:
                return str(internal)
        except Exception as exc:
            logger.warning("Worker[%s]: symbol translation failed: %s", self._client.client_id, exc)
            return None

    def _derive_expiry(self) -> date:
        today = datetime.now(IST).date()
        pref = self._client.expiry_preference
        days_thu = (3 - today.weekday()) % 7 or 7
        expiry = today + timedelta(days=days_thu)
        if pref == "NEXT_WEEK":
            expiry += timedelta(weeks=1)
        elif pref == "MONTHLY":
            from calendar import monthrange
            m = today.month % 12 + 1
            y = today.year + (1 if today.month == 12 else 0)
            last = date(y, m, 1) - timedelta(days=1)
            back = (last.weekday() - 3) % 7
            expiry = last - timedelta(days=back)
        return expiry

    def _compute_lots(self, signal: SignalPackage, broker: BaseBroker) -> int:
        risk = self._client.risk
        max_risk_inr = risk.capital * risk.max_risk_per_trade_pct / 100
        lot_size = self._cfg.exchange.lot_sizes.get(signal.underlying, 25)
        option_sl_dist = 50.0
        risk_per_lot = option_sl_dist * lot_size
        if risk_per_lot <= 0:
            return 0
        lots = int(max_risk_inr / risk_per_lot * risk.size_multiplier)
        return max(1, min(lots, 10))


# ─────────────────────────────────────────────────────────────────────────────
# Worker Pool — manages all per-client workers
# ─────────────────────────────────────────────────────────────────────────────

class WorkerPool:
    """
    Owns the collection of ClientExecutionWorkers.

    ExecutionRouter calls pool.dispatch(signal) — a single O(N) loop of
    put_nowait() calls that returns in microseconds regardless of N.
    Each worker runs in its own asyncio Task.

    _watcher_task() runs every _WATCHER_INTERVAL_SECONDS and restarts any
    worker task that has died unexpectedly (task.done() == True).
    """

    def __init__(self) -> None:
        self._workers: Dict[str, ClientExecutionWorker] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._watcher: Optional[asyncio.Task] = None
        self._running = False

    def register(self, worker: ClientExecutionWorker) -> None:
        self._workers[worker.client_id] = worker

    async def start_all(self) -> None:
        self._running = True
        for cid, worker in self._workers.items():
            self._tasks[cid] = asyncio.create_task(
                worker.run(), name=f"exec_worker_{cid}"
            )
            logger.info("WorkerPool: started task for client %s.", cid)
        # Start the watcher task
        self._watcher = asyncio.create_task(
            self._watcher_loop(), name="exec_worker_pool_watcher"
        )

    async def stop_all(self) -> None:
        self._running = False
        for worker in self._workers.values():
            worker.stop()
        if self._watcher and not self._watcher.done():
            self._watcher.cancel()
            try:
                await self._watcher
            except asyncio.CancelledError:
                pass
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    def dispatch(self, signal: SignalPackage) -> int:
        """
        Drop signal into every worker queue simultaneously.
        Returns the number of workers that accepted the signal.
        """
        accepted = 0
        for worker in self._workers.values():
            worker.enqueue(signal)
            accepted += 1
        return accepted

    def stats(self) -> List[Dict]:
        return [
            {
                "client_id": w.client_id,
                "queue_depth": w.queue_size,
                "processed": w._processed,
                "dropped": w.drop_count,
                "total_failures": w._total_failures,
                "circuit_open": w.circuit_is_open,
                "task_alive": not self._tasks.get(w.client_id, _DEAD_SENTINEL).done(),
                "rate_limiter": w._rate_limiter.all_stats(),
            }
            for w in self._workers.values()
        ]

    def worker(self, client_id: str) -> Optional[ClientExecutionWorker]:
        return self._workers.get(client_id)

    async def add_worker(self, worker: ClientExecutionWorker) -> None:
        """Register and immediately start a worker at runtime (live broker provisioning)."""
        self._workers[worker.client_id] = worker
        if self._running:
            self._tasks[worker.client_id] = asyncio.create_task(
                worker.run(), name=f"exec_worker_{worker.client_id}_live"
            )
            logger.info("WorkerPool: dynamically started worker for client %s.", worker.client_id)

    def add_broker_to_worker(
        self, client_id: str, binding_id: str, broker: "BaseBroker", provider: str
    ) -> bool:
        """
        Inject a new broker into an already-running worker's broker map.

        Returns True if the worker existed and was updated, False if the worker
        does not exist yet (caller should use add_worker() instead).
        """
        w = self._workers.get(client_id)
        if w is None:
            return False
        w._brokers[binding_id] = broker
        w._binding_provider[binding_id] = provider
        return True

    # ── Task watcher ──────────────────────────────────────────────────────────

    async def _watcher_loop(self) -> None:
        """
        Periodically checks every worker Task for unexpected termination.
        If a task is done (exited or raised), it is restarted immediately.

        A Task only exits run() normally if worker.stop() was called or if
        a truly catastrophic exception escaped the run() loop (should be
        impossible given the try/except in run(), but hardware failures and
        asyncio internal errors can still kill tasks).
        """
        while self._running:
            try:
                await asyncio.sleep(_WATCHER_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                return

            for cid, task in list(self._tasks.items()):
                if not task.done():
                    continue
                worker = self._workers.get(cid)
                if worker is None or not worker._running:
                    continue   # Worker was intentionally stopped

                exc = None
                try:
                    exc = task.exception()
                except (asyncio.CancelledError, asyncio.InvalidStateError):
                    pass

                logger.error(
                    "WorkerPool: task for client %s died unexpectedly (exc=%s) — restarting.",
                    cid, exc,
                )
                # Reset the worker's running flag so run() starts cleanly
                worker._running = False
                new_task = asyncio.create_task(
                    worker.run(), name=f"exec_worker_{cid}_restart"
                )
                self._tasks[cid] = new_task
                logger.info("WorkerPool: restarted worker task for client %s.", cid)


# Sentinel used in stats() to avoid KeyError on missing task
class _DeadSentinel:
    def done(self) -> bool:
        return True

_DEAD_SENTINEL = _DeadSentinel()
