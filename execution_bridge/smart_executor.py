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

        # LIMIT path — IOC MARKETABLE chase so an order NEVER rests on the book. A resting limit was
        # the root of the naked-leg risk: on a thin Delta book a mid-priced sell sits as a maker, our
        # cancel can fail, and it fills LATE (asymmetric). IOC = fill what's available NOW, kill the
        # rest instantly → nothing rests, the cancel-race disappears. Try the MID once (free fill only
        # if the book already crosses us), then become MARKETABLE (sell@bid / buy@ask) so the IOC truly
        # fills, then a plain MARKET mops up any remainder. We book the broker's AUTHORITATIVE filled
        # qty after each attempt (IOC orders are terminal immediately — no cancel/settle needed).
        for attempt in range(self._chases + 1):
            remaining = qty - filled_qty
            if remaining <= 0:
                break
            try:
                bid, ask = await broker.get_quote(broker_symbol)
            except Exception:
                bid, ask = 0.0, 0.0
            if not (bid > 0 and ask > 0):
                break                                    # no quote → cannot price a limit, go market
            # SLIPPAGE LADDER: walk the price from the MID toward the marketable touch in equal steps
            # across the attempts. We pay only as much of the half-spread as is needed to fill at the
            # first level that has resting liquidity — attempt 0 is the mid (free if the book already
            # crosses us), the final attempt is the touch (bid for a sell / ask for a buy = the MOST
            # we ever give up = half the spread). IOC means nothing rests in between.
            mid = (bid + ask) / 2.0
            touch = bid if side == OrderSide.SELL else ask
            frac = attempt / max(self._chases, 1)        # 0 → mid, 1 → touch
            price = _round_tick(mid + (touch - mid) * frac, tick)
            if price <= 0:
                break
            req = OrderRequest(broker_symbol=broker_symbol, exchange=exchange, side=side, qty=remaining,
                               order_type=OrderType.LIMIT, price=price, product=product,
                               tag=tag, client_id=client_id, time_in_force="ioc")
            oid = await broker.place_order(req)
            order_ids.append(str(oid))
            q, avg, _st = await self._await_fill(broker, oid)
            # IOC is terminal immediately — take the broker's AUTHORITATIVE final fill (handles a
            # partial fill where the await loop saw it mid-flight). Book once; never double-count.
            try:
                f = await broker.get_order_status(oid)
                fq = int(getattr(f, "qty", 0) or 0); favg = float(getattr(f, "avg_price", 0.0) or 0.0)
            except Exception:
                fq, favg = 0, 0.0
            if fq <= 0 and q > 0:                         # fall back to the await result
                fq, favg = q, avg
            if fq > 0 and favg > 0:
                filled_qty += fq; notional += favg * fq
            if filled_qty >= qty:
                break
            logger.info("SmartExec[%s]: IOC @ %.4f (%.0f%% mid→touch) — filled %d/%d (attempt %d) — escalating.",
                        broker_symbol, price, frac * 100.0, filled_qty, qty, attempt + 1)

        if filled_qty < qty:                              # chase exhausted → guarantee completion
            await _market(qty - filled_qty)

        avg = notional / filled_qty if filled_qty else 0.0
        return LegFill(filled_qty, round(avg, 8), order_ids, filled_qty >= qty)
