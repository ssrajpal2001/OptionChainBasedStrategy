"""
execution_bridge/smart_executor.py — slippage-aware order execution (all scripts).

Goal: stop giving away the bid/ask spread on illiquid options (BTC/ETH, CRUDEOIL, far-OTM), and
ONLY book a position from a CONFIRMED real fill — never an optimistic feed LTP.

Per leg, when use_limit=True:
    1. LIMIT @ mid = round((best_bid + best_ask) / 2, tick)
    2. poll the order up to fill_timeout_sec
    3. not filled → CANCEL, then RE-CHECK status (cancel-race: it may have just filled — book that,
       only chase the truly-unfilled remainder), re-price to the NEW mid, repeat up to chase_attempts
    4. chase exhausted → MARKET the remainder (guaranteed completion — never hold a naked straddle leg)
use_limit=False → straight MARKET (NSE/MCX default — liquid, fills near LTP).

Returns a LegFill with the REAL volume-weighted average fill price and filled qty. The bridge books
the position ONLY from this (both legs reconciled first). Broker-agnostic: works against any object
implementing place_order/get_order_status/cancel_order, and (for limit) get_quote(symbol)->(bid,ask).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

from execution_bridge.base_broker import OrderRequest, OrderSide, OrderStatus, OrderType

logger = logging.getLogger(__name__)


@dataclass
class LegFill:
    filled_qty: int
    avg_price: float
    order_ids: list           # every broker order_id used for this leg (limit + chases + market)
    completed: bool           # True if the full qty filled


def _round_tick(price: float, tick: float) -> float:
    if tick and tick > 0:
        return round(round(price / tick) * tick, 8)
    return round(price, 2)


class SmartOrderExecutor:
    def __init__(self, fill_timeout_sec: float = 4.0, chase_attempts: int = 2,
                 poll_interval: float = 0.5, settle_timeout_sec: float = 6.0) -> None:
        self._timeout = fill_timeout_sec
        self._chases = chase_attempts
        self._poll = poll_interval
        self._settle = settle_timeout_sec   # max wait for a cancelled order to reach a TERMINAL state

    async def _await_fill(self, broker, order_id: str) -> Tuple[int, float, str]:
        """Poll until filled / timeout. Returns (filled_qty, avg_price, state)."""
        deadline = asyncio.get_event_loop().time() + self._timeout
        last = (0, 0.0, "unknown")
        while asyncio.get_event_loop().time() < deadline:
            try:
                f = await broker.get_order_status(order_id)
            except Exception as exc:
                logger.debug("SmartExec: status poll failed for %s: %s", order_id, exc)
                await asyncio.sleep(self._poll)
                continue
            qty = int(getattr(f, "qty", 0) or 0)
            avg = float(getattr(f, "avg_price", 0.0) or 0.0)
            st = getattr(f, "status", OrderStatus.UNKNOWN)
            last = (qty, avg, st)
            if st == OrderStatus.COMPLETE and avg > 0:
                return qty, avg, st
            if st in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                return 0, 0.0, st
            await asyncio.sleep(self._poll)
        return last[0] if last[2] == OrderStatus.COMPLETE else 0, last[1], last[2]

    async def _cancel_and_settle(self, broker, order_id: str) -> Tuple[int, float, bool]:
        """Cancel, then POLL until the order is TERMINAL (cancelled/complete/rejected) before we are
        allowed to place anything else. Returns (filled_qty, avg, settled). This is the fix for the
        cancel-race: a resting LIMIT can fill (as a maker) in the instant we cancel it; checking its
        status ONCE missed that and we double-sold. We now wait for the broker's final state and book
        the ACTUAL filled qty — and if it never settles, we report settled=False so the caller STOPS
        (an under-fill is recoverable; a double-fill is not)."""
        try:
            await broker.cancel_order(order_id)
        except Exception:
            pass
        deadline = asyncio.get_event_loop().time() + self._settle
        last_q, last_avg = 0, 0.0
        while asyncio.get_event_loop().time() < deadline:
            try:
                f = await broker.get_order_status(order_id)
            except Exception:
                await asyncio.sleep(self._poll)
                continue
            q = int(getattr(f, "qty", 0) or 0)
            avg = float(getattr(f, "avg_price", 0.0) or 0.0)
            st = getattr(f, "status", OrderStatus.UNKNOWN)
            last_q, last_avg = q, avg
            if st in (OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.COMPLETE):
                return q, avg, True
            await asyncio.sleep(self._poll)
        return last_q, last_avg, False

    async def execute_leg(self, broker, *, broker_symbol: str, exchange: str, side: OrderSide,
                          qty: int, product: str, tag: str, client_id: str,
                          use_limit: bool, tick: float = 0.0) -> LegFill:
        order_ids: list = []
        filled_qty = 0
        notional = 0.0   # Σ(price × qty) for VWAP of the real fills

        async def _market(remaining: int) -> None:
            nonlocal filled_qty, notional
            req = OrderRequest(broker_symbol=broker_symbol, exchange=exchange, side=side, qty=remaining,
                               order_type=OrderType.MARKET, product=product, tag=tag, client_id=client_id)
            oid = await broker.place_order(req)
            order_ids.append(str(oid))
            q, avg, _ = await self._await_fill(broker, oid)
            if q <= 0 or avg <= 0:                       # market should fill; last-resort status fetch
                f = await broker.get_order_status(oid)
                q = int(getattr(f, "qty", 0) or 0); avg = float(getattr(f, "avg_price", 0.0) or 0.0)
            if q > 0 and avg > 0:
                filled_qty += q; notional += avg * q

        if not use_limit:
            await _market(qty)
            avg = notional / filled_qty if filled_qty else 0.0
            return LegFill(filled_qty, round(avg, 8), order_ids, filled_qty >= qty)

        # LIMIT path with chase, then market for the remainder.
        for attempt in range(self._chases + 1):
            remaining = qty - filled_qty
            if remaining <= 0:
                break
            try:
                bid, ask = await broker.get_quote(broker_symbol)
            except Exception:
                bid, ask = 0.0, 0.0
            if bid > 0 and ask > 0:
                price = _round_tick((bid + ask) / 2.0, tick)
            else:
                price = 0.0
            if price <= 0:                                # no quote → cannot price a limit, go market
                break
            req = OrderRequest(broker_symbol=broker_symbol, exchange=exchange, side=side, qty=remaining,
                               order_type=OrderType.LIMIT, price=price, product=product,
                               tag=tag, client_id=client_id)
            oid = await broker.place_order(req)
            order_ids.append(str(oid))
            q, avg, st = await self._await_fill(broker, oid)
            if q > 0 and avg > 0 and st == OrderStatus.COMPLETE:
                filled_qty += q; notional += avg * q
                continue
            # Not (fully) filled → cancel and SETTLE (wait for terminal) before placing anything else,
            # booking the real filled qty. If it never settles we MUST stop (avoid a double-fill).
            cq, cavg, settled = await self._cancel_and_settle(broker, oid)
            if cq > 0 and cavg > 0:
                filled_qty += cq; notional += cavg * cq
            if not settled:
                logger.error("SmartExec[%s]: order %s did not reach a terminal state after cancel — "
                             "STOPPING (filled %d/%d) to avoid a double-fill. Reconcile manually.",
                             broker_symbol, oid, filled_qty, qty)
                avg = notional / filled_qty if filled_qty else 0.0
                return LegFill(filled_qty, round(avg, 8), order_ids, filled_qty >= qty)
            logger.info("SmartExec[%s]: limit @ %.4f unfilled (attempt %d, settled +%d) — chasing new mid.",
                        broker_symbol, price, attempt + 1, cq)

        if filled_qty < qty:                              # chase exhausted → guarantee completion
            await _market(qty - filled_qty)

        avg = notional / filled_qty if filled_qty else 0.0
        return LegFill(filled_qty, round(avg, 8), order_ids, filled_qty >= qty)
