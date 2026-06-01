"""
execution_bridge/ic_bridge.py — Iron Condor 4-leg order router.

Subscribes to Topic.IC_ORDER_REQUEST for ICOrderEvent objects.
Places all 4 legs: SELL short_ce, SELL short_pe, BUY long_ce, BUY long_pe on ENTRY.
On EXIT reverses all 4 legs.

Paper mode  — fills immediately at the LTP sent in the event.
Live mode   — calls broker.place_order() for each leg sequentially.

Publishes ICFillEvent to Topic.ORDER_FILL after each 4-leg execution.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import ICOrderEvent, ICFillEvent

logger = logging.getLogger(__name__)


class ICExecutionBridge:
    """
    Routes Iron Condor 4-leg orders to all engine-active client brokers.

    Wire-up (run_system.py):
        ic_bridge = ICExecutionBridge(bus, registry, router)
        asyncio.create_task(ic_bridge.run(), name="ic_bridge")
    """

    def __init__(self, bus: EventBus, registry, router) -> None:
        self._bus      = bus
        self._registry = registry
        self._router   = router
        self._running  = False
        self._q: Optional[asyncio.Queue] = None

    async def run(self) -> None:
        self._q       = self._bus.subscribe(Topic.IC_ORDER_REQUEST)
        self._running = True
        logger.info("ICExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(ev, ICOrderEvent):
                continue
            try:
                await self._handle(ev)
            except Exception as exc:
                logger.exception("ICExecutionBridge: handle error: %s", exc)

    def stop(self) -> None:
        self._running = False
        logger.info("ICExecutionBridge: stopped.")

    # ── Routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev: ICOrderEvent) -> None:
        clients = self._registry.all_active()
        routed  = 0

        for client in clients:
            db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
            live_bindings = []
            if db and hasattr(db, "get_bindings_safe_sync"):
                try:
                    live_bindings = db.get_bindings_safe_sync(client.client_id)
                except Exception:
                    live_bindings = []

            for live_b in live_bindings:
                binding_id = live_b.get("binding_id", "")

                if not live_b.get("engine_active"):
                    continue
                if not live_b.get("terminal_connected"):
                    continue

                assigned = live_b.get("assigned_strategy", "") or ""
                if assigned and assigned != "iron_condor":
                    continue

                assigned_idx = live_b.get("assigned_instrument", "") or ""
                if assigned_idx and assigned_idx != ev.underlying:
                    continue

                broker = (self._router._brokers or {}).get(client.client_id, {}).get(binding_id)
                mode   = live_b.get("trading_mode", "paper") or "paper"

                logger.info(
                    "ICExecutionBridge: routing %s %s → [%s/%s] mode=%s",
                    ev.action, ev.underlying, client.client_id, binding_id, mode,
                )

                if mode == "paper" or broker is None:
                    await self._paper_fill(ev, client.client_id, binding_id)
                else:
                    await self._live_fill(ev, client.client_id, binding_id, broker)
                routed += 1

        if routed == 0:
            logger.warning(
                "ICExecutionBridge: %s %s — no engine-active iron_condor brokers. "
                "Ensure Terminal ON + Engine ON + assigned_strategy=iron_condor.",
                ev.action, ev.underlying,
            )

    # ── Paper fill ────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev: ICOrderEvent, client_id: str, binding_id: str) -> None:
        fill = ICFillEvent(
            action         = ev.action,
            underlying     = ev.underlying,
            short_ce_fill  = ev.short_ce_ltp,
            short_pe_fill  = ev.short_pe_ltp,
            long_ce_fill   = ev.long_ce_ltp,
            long_pe_fill   = ev.long_pe_ltp,
            client_id      = client_id,
            binding_id     = binding_id,
            event_id       = ev.event_id,
            paper_mode     = True,
        )
        net_credit = (ev.short_ce_ltp + ev.short_pe_ltp) - (ev.long_ce_ltp + ev.long_pe_ltp)
        logger.info(
            "[PAPER IC] %s %s %s | SELL %sCE@%.2f + %sPE@%.2f | BUY %sCE@%.2f + %sPE@%.2f | net=%.2f | PnL=₹%.0f | client=%s",
            ev.action, ev.underlying, ev.atm,
            ev.short_ce_strike, ev.short_ce_ltp,
            ev.short_pe_strike, ev.short_pe_ltp,
            ev.long_ce_strike,  ev.long_ce_ltp,
            ev.long_pe_strike,  ev.long_pe_ltp,
            net_credit,
            ev.cumulative_pnl * ev.lot_size * ev.lot_multiplier,
            client_id,
        )
        await self._bus.publish(Topic.ORDER_FILL, fill)

    # ── Live fill ─────────────────────────────────────────────────────────────

    async def _live_fill(
        self, ev: ICOrderEvent, client_id: str, binding_id: str, broker
    ) -> None:
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        from data_layer.instrument_registry import REGISTRY as _REG
        from datetime import date as _date

        qty      = ev.lot_size * ev.lot_multiplier
        provider = getattr(broker, "_binding", None)
        provider = provider.provider if provider else getattr(broker, "provider", "mock")

        # On ENTRY: SELL short legs, BUY long legs
        # On EXIT:  BUY short legs (close), SELL long legs (close)
        if ev.action == "ENTRY":
            legs = [
                (ev.short_ce_strike, "CE", OrderSide.SELL, ev.short_ce_ltp),
                (ev.short_pe_strike, "PE", OrderSide.SELL, ev.short_pe_ltp),
                (ev.long_ce_strike,  "CE", OrderSide.BUY,  ev.long_ce_ltp),
                (ev.long_pe_strike,  "PE", OrderSide.BUY,  ev.long_pe_ltp),
            ]
        else:
            legs = [
                (ev.short_ce_strike, "CE", OrderSide.BUY,  ev.short_ce_ltp),
                (ev.short_pe_strike, "PE", OrderSide.BUY,  ev.short_pe_ltp),
                (ev.long_ce_strike,  "CE", OrderSide.SELL, ev.long_ce_ltp),
                (ev.long_pe_strike,  "PE", OrderSide.SELL, ev.long_pe_ltp),
            ]

        fills: Dict[str, float] = {}
        today  = datetime.now(IST).date()
        expiry = _REG.all_expiries(ev.underlying)
        expiry = next((e for e in expiry if e >= today), today)

        for strike, opt_type, side, fallback_ltp in legs:
            broker_sym = _REG.get_broker_symbol(ev.underlying, expiry, int(strike), opt_type, provider)
            if not broker_sym:
                logger.warning("ICBridge [%s]: no broker symbol for %s %s %d%s — using fallback ltp",
                               client_id, ev.underlying, expiry, int(strike), opt_type)
                fills[f"{opt_type}{int(strike)}"] = fallback_ltp
                continue

            req = OrderRequest(
                broker_symbol=broker_sym,
                exchange=_order_exchange(ev.underlying),
                side=side,
                qty=qty,
                order_type=OrderType.MARKET,
                tag=f"IC_{ev.underlying}_{ev.action[:3]}",
                client_id=client_id,
            )
            try:
                order_id = await broker.place_order(req)
                order_fill = await broker.get_order_status(order_id)
                fill_price = order_fill.avg_price if order_fill else fallback_ltp
                logger.info("[LIVE IC] %s %s %s%s@%.2f | client=%s",
                            ev.action, ev.underlying, side.value, broker_sym, fill_price, client_id)
            except Exception as exc:
                logger.error("[LIVE IC] %s leg FAILED: %s — using ltp fallback", broker_sym, exc)
                fill_price = fallback_ltp

            fills[f"{opt_type}{int(strike)}"] = fill_price

        fill_ev = ICFillEvent(
            action        = ev.action,
            underlying    = ev.underlying,
            short_ce_fill = fills.get(f"CE{int(ev.short_ce_strike)}", ev.short_ce_ltp),
            short_pe_fill = fills.get(f"PE{int(ev.short_pe_strike)}", ev.short_pe_ltp),
            long_ce_fill  = fills.get(f"CE{int(ev.long_ce_strike)}",  ev.long_ce_ltp),
            long_pe_fill  = fills.get(f"PE{int(ev.long_pe_strike)}",  ev.long_pe_ltp),
            client_id     = client_id,
            binding_id    = binding_id,
            event_id      = ev.event_id,
            paper_mode    = False,
        )
        await self._bus.publish(Topic.ORDER_FILL, fill_ev)
