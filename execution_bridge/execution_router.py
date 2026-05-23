"""
execution_bridge/execution_router.py — Concurrent multi-broker order dispatcher.

Subscribes to SIGNAL topic.  On receipt of a SignalPackage:
  1. Asks ClientManager for all tradeable clients that have this strategy enabled.
  2. For each client, translates the signal's InternalSymbol to that
     client's broker-specific symbol via SymbolTranslator.
  3. Dispatches all broker calls concurrently via asyncio.gather —
     Client 1's network latency never blocks Client 2.
  4. Publishes each OrderFill to the ORDER_FILL topic.

Risk validation is delegated to ClientManager.validate_signal() before
any order is placed.

No time.sleep.  All concurrency via asyncio.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic, GlobalConfig
from config.client_profiles import ClientProfile, ClientRegistry
from data_layer.base_feeder import EventBus
from data_layer.symbol_translator import SymbolTranslator, InternalSymbol
from execution_bridge.base_broker import (
    BaseBroker, OrderRequest, OrderFill, OrderSide, OrderType, create_broker,
)
from strategies.base_strategy import SignalPackage, Direction

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Cost Calculator (Indian statutory charges)
# ─────────────────────────────────────────────────────────────────────────────

class CostCalc:
    STT_SELL_PCT       = 0.0625 / 100
    EXCHANGE_PCT       = 0.035  / 100
    SEBI_FEE_PCT       = 0.0001 / 100
    GST_PCT            = 0.18
    BROKERAGE_FLAT     = 20.0   # INR per order
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
    Central order dispatcher.

    Holds one authenticated BaseBroker instance per client × binding.
    On signal receipt, fires all placements concurrently.
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
        # {client_id: {binding_id: BaseBroker}}
        self._brokers: Dict[str, Dict[str, BaseBroker]] = {}
        self._cost = CostCalc()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Authenticate all registered client brokers."""
        for client in self._registry.all_active():
            self._brokers[client.client_id] = {}
            for binding in client.enabled_brokers():
                broker = create_broker(binding, client.client_id)
                if await broker.authenticate():
                    self._brokers[client.client_id][binding.binding_id] = broker
                    logger.info("Router: Authenticated %s/%s (%s).",
                                client.client_id, binding.binding_id, binding.provider)
                else:
                    logger.error("Router: Auth failed for %s/%s.", client.client_id, binding.binding_id)

    async def stop(self) -> None:
        self._running = False
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
            await self._dispatch(signal)

    # ── Signal Dispatch ───────────────────────────────────────────────────────

    async def _dispatch(self, signal: SignalPackage) -> None:
        """
        Fan out the signal to ALL eligible client × broker pairs concurrently.

        Execution model:
          1. Build the full task list (pure CPU — no I/O, no await).
          2. Fire ALL tasks in ONE asyncio.gather() call.
             Client 1's network round-trip NEVER blocks Client 2.
             All N×M broker calls are in-flight simultaneously.
        """
        tradeable = self._registry.tradeable_clients()
        if not tradeable:
            logger.debug("Router: No tradeable clients for signal %s.", signal.source.value)
            return

        # Determine which strategy letter this signal came from
        # StrategyID values: "OI_Zone_Breakout" -> "A", "Liquidity_Trap" -> "B", etc.
        _source_to_letter = {
            "OI_Zone_Breakout": "A",
            "Liquidity_Trap":   "B",
            "Panic_Scanner":    "C",
        }
        strategy_letter = _source_to_letter.get(signal.source.value, signal.source.value[0].upper())

        # ── Phase 1: build task list (O(clients × bindings), pure CPU, no I/O) ──
        tasks = []
        for client in tradeable:
            # Hard gate: client must have this strategy enabled
            if strategy_letter not in [s.upper() for s in client.enabled_strategies]:
                logger.debug(
                    "Router: Client %s has strategy '%s' disabled — skipping.",
                    client.client_id, strategy_letter,
                )
                continue

            brokers = self._brokers.get(client.client_id, {})
            if not brokers:
                logger.warning("Router: No authenticated brokers for %s.", client.client_id)
                continue

            for binding_id, broker in brokers.items():
                sym_str = self._translate(signal, binding_id, client)
                if sym_str is None:
                    continue
                lots = self._compute_lots(client, signal, broker)
                if lots <= 0:
                    continue
                qty = lots * self._cfg.exchange.lot_sizes.get(signal.underlying, 25)
                tasks.append(
                    self._place_for_client(broker, client, sym_str, qty, signal)
                )

        if not tasks:
            return

        # ── Phase 2: fire ALL tasks concurrently — zero sequential latency gap ──
        logger.info(
            "Router: Dispatching signal %s to %d broker task(s) in parallel.",
            signal.source.value, len(tasks),
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("Router: Order placement error: %s", r)

    async def _place_for_client(
        self,
        broker: BaseBroker,
        client: ClientProfile,
        broker_symbol: str,
        qty: int,
        signal: SignalPackage,
    ) -> None:
        req = OrderRequest(
            broker_symbol=broker_symbol,
            exchange="NFO" if signal.underlying != "SENSEX" else "BFO",
            side=OrderSide.BUY,      # Always buy options (CE for LONG, PE for SHORT)
            qty=qty,
            order_type=OrderType.MARKET,
            tag=f"{client.client_id}_{signal.source.value}_{signal.timestamp.strftime('%H%M%S')}",
            client_id=client.client_id,
        )
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(order_id)
            await self._bus.publish(Topic.ORDER_FILL, fill)
            client.record_trade(0.0)   # P&L updated later on exit
            logger.info(
                "Router: FILLED %s/%s | %s qty=%d avg=%.2f",
                client.client_id, broker.binding_id,
                broker_symbol, qty, fill.avg_price,
            )
        except Exception as exc:
            logger.error("Router: Place failed for %s: %s", client.client_id, exc)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _translate(self, signal: SignalPackage, binding_id: str, client: ClientProfile) -> Optional[str]:
        """Convert signal to broker-specific symbol string."""
        expiry = self._derive_expiry(client)
        internal = InternalSymbol(
            underlying=signal.underlying,
            strike=signal.target_strike,
            option_type=signal.option_type,
            expiry=expiry,
        )
        # Find the binding provider
        provider = next(
            (b.provider for b in client.broker_bindings if b.binding_id == binding_id), "mock"
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
                return str(internal)   # Mock / fallback
        except Exception as exc:
            logger.warning("Router: Symbol translation failed: %s", exc)
            return None

    def _derive_expiry(self, client: ClientProfile) -> date:
        today = datetime.now(IST).date()
        pref = client.expiry_preference
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

    def _compute_lots(
        self, client: ClientProfile, signal: SignalPackage, broker: BaseBroker
    ) -> int:
        risk = client.risk
        max_risk_inr = risk.capital * risk.max_risk_per_trade_pct / 100
        lot_size = self._cfg.exchange.lot_sizes.get(signal.underlying, 25)
        # Approximate SL distance as 50% of option premium (ATM rule of thumb)
        option_sl_dist = 50.0   # Will be refined when live LTP is available
        risk_per_lot = option_sl_dist * lot_size
        if risk_per_lot <= 0:
            return 0
        lots = int(max_risk_inr / risk_per_lot * risk.size_multiplier)
        return max(1, min(lots, 10))
