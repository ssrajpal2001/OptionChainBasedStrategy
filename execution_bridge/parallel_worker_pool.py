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

This means:
  • Client A's network latency NEVER touches Client B's execution path.
  • Router returns immediately after N put_nowait() calls — zero blocking.
  • If a worker's queue fills (backpressure), the signal is dropped for that
    client only with a warning — other clients are unaffected.

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
from strategies.base_strategy import SignalPackage

logger = logging.getLogger(__name__)

_WORKER_QUEUE_SIZE = 100   # Per spec: asyncio.Queue(maxsize=100)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy letter mapping (keeps router and worker consistent)
# ─────────────────────────────────────────────────────────────────────────────

_SOURCE_TO_LETTER: Dict[str, str] = {
    "OI_Zone_Breakout": "A",
    "Liquidity_Trap":   "B",
    "Panic_Scanner":    "C",
}


def strategy_letter(source_value: str) -> str:
    return _SOURCE_TO_LETTER.get(source_value, source_value[0].upper())


# ─────────────────────────────────────────────────────────────────────────────
# Per-Client Execution Worker
# ─────────────────────────────────────────────────────────────────────────────

class ClientExecutionWorker:
    """
    Isolated execution unit for a single client.

    Holds all broker instances for this client.
    Processes signals from its own queue independently of all other clients.
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
        self._processed = 0
        self._dropped = 0

    @property
    def client_id(self) -> str:
        return self._client.client_id

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def drop_count(self) -> int:
        return self._dropped

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
                continue
            await self._process(signal)

    def stop(self) -> None:
        self._running = False

    # ── Signal processing ─────────────────────────────────────────────────────

    async def _process(self, signal: SignalPackage) -> None:
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
            order_id = await broker.place_order(req)
            fill: OrderFill = await broker.get_order_status(order_id)
            await self._bus.publish(Topic.ORDER_FILL, fill)
            self._client.record_trade(0.0)
            logger.info(
                "Worker[%s/%s]: FILLED %s qty=%d avg=%.2f",
                self._client.client_id, broker.binding_id,
                broker_symbol, qty, fill.avg_price,
            )
        except Exception as exc:
            logger.error(
                "Worker[%s]: place failed for %s: %s",
                self._client.client_id, broker_symbol, exc,
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
    """

    def __init__(self) -> None:
        self._workers: Dict[str, ClientExecutionWorker] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    def register(self, worker: ClientExecutionWorker) -> None:
        self._workers[worker.client_id] = worker

    async def start_all(self) -> None:
        for cid, worker in self._workers.items():
            task = asyncio.create_task(
                worker.run(), name=f"exec_worker_{cid}"
            )
            self._tasks[cid] = task
            logger.info("WorkerPool: started task for client %s.", cid)

    async def stop_all(self) -> None:
        for worker in self._workers.values():
            worker.stop()
        # Let tasks drain gracefully
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
            }
            for w in self._workers.values()
        ]

    def worker(self, client_id: str) -> Optional[ClientExecutionWorker]:
        return self._workers.get(client_id)
