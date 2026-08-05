"""
execution_bridge/d1_trap_bridge.py — D1 Trap + Option order router.

Subscribes to Topic.D1_TRAP_ORDER_REQUEST for D1TrapOrderEvent objects
published by strategies/d1_trap_option/book.py.

Routes STRICTLY to the event's own (client_id, binding_id) broker — never
broadcasts. Symbol resolution is per-broker at order time (Zerodha weekly
format: NIFTY2672924300CE; Upstox: numeric instrument key, etc.).

Paper mode  — local simulated fill at strategy spot price; NO real order.
Live mode   — MARKET order via broker.place_order() + get_order_status();
              position booked from the real fill price.

Product: MIS (intraday — all positions are squared off by EOD in book.py).
Exchange: NFO (NIFTY), BFO (SENSEX), etc. resolved via order_exchange().
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import _resolve_option_symbol

logger = logging.getLogger(__name__)


@dataclass
class D1TrapFillEvent:
    """Published by D1TrapExecutionBridge to Topic.D1_TRAP_ORDER_FILL after every
    order attempt (paper and live, success and failure). Field shape mirrors
    CascadeFillEvent (execution_bridge/cascade_bridge.py) exactly -- same proven
    confirm-then-finalize contract, separate class per this codebase's
    per-strategy-event convention.

    2026-08-05: previously this bridge never published to D1_TRAP_ORDER_FILL at
    all -- bear_only_book.py's _enter_leg/_square_off_leg mutated (and persisted)
    self._positions before the order was even dispatched, so nothing ever told
    the book whether a BUY/SELL actually reached the broker. A broker-unreachable
    EXIT silently looked like a successful close while the leg was still open at
    the broker."""
    action:      str    # "BUY" | "SELL"
    underlying:  str
    option_type: str    # "CE" | "PE"
    strike:      int
    fill_price:  float
    qty:         int
    client_id:   str
    binding_id:  str
    event_id:    str
    paper_mode:  bool = True
    symbol:      str = ""
    timestamp:   datetime = field(default_factory=lambda: datetime.now(IST))
    # True when a LIVE BUY failed to route (no route/no broker) -- the book must
    # discard its optimistic leg rather than manage a phantom position.
    entry_aborted:  bool = False
    routing_failed: bool = False
    # True when a LIVE SELL (exit) could not be routed -- the book must leave the
    # leg exactly as it was (still open, still persisted) rather than believe an
    # unrouted close actually happened. This is the field cascade_bridge.py's
    # _abort()/exit_failed and sell_straddle's exit_aborted solve the exact same
    # problem for.
    exit_failed: bool = False


class _D1TrapTradeLogger:
    def __init__(self, log_dir: str = "logs/trades") -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-d1_trap-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  D1TRAP  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class D1TrapExecutionBridge:
    """
    Listens for D1TrapOrderEvent on Topic.D1_TRAP_ORDER_REQUEST.

    Routes BUY (ENTRY) and SELL (EXIT) to the owning (client_id, binding_id)
    broker.  Resolves the correct broker option symbol (Zerodha NIFTY2672924300CE,
    Upstox numeric key, etc.) at order time using the broker's provider field.
    """

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _D1TrapTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.D1_TRAP_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        logger.info("D1TrapExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                from strategies.d1_trap_option.book import D1TrapOrderEvent
                if not isinstance(ev, D1TrapOrderEvent):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("D1TrapExecutionBridge: _handle error.")

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("D1TrapExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("D1TrapExecutionBridge: event missing client_id/binding_id — dropped.")
            return

        db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
        live_binding = None
        if db is not None and hasattr(db, "get_bindings_safe_sync"):
            try:
                for b in db.get_bindings_safe_sync(ev.client_id):
                    if b.get("binding_id") == ev.binding_id:
                        live_binding = b
                        break
            except Exception:
                live_binding = None

        if live_binding is None or not live_binding.get("terminal_connected"):
            logger.warning(
                "D1TrapExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            # SELL (EXIT) must abort too, not just silently drop -- bear_only_book.py
            # optimistically appends a leg / awaits a fill for a leg it already
            # decided to close, and relies on a D1_TRAP_ORDER_FILL abort to revert/
            # not-finalize that if the order never reached the broker. Silently
            # returning here (no fill event at all) means the book hangs waiting
            # for a confirmation that will never come (BUY) or -- pre-confirm-then-
            # finalize -- believed a leg closed that never left the exchange (SELL).
            await self._abort(ev, routing_failed=True)
            return

        # EXIT must always route — gate only ENTRY on the shared can_trade() gate
        # (terminal_connected AND is_trade_enabled AND a running deployment of
        # THIS exact strategy for THIS underlying on THIS binding).
        # Uses ev.strategy (the strategy that actually placed the order — e.g.
        # "d1_trap_bear_only" for the live-traded engine) rather than a hardcoded
        # name allowlist, which previously omitted "d1_trap_bear_only" entirely and
        # would have silently blocked every live BUY for that strategy.
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            if not can_trade(ev.client_id, ev.binding_id, db, ev.strategy, ev.underlying):
                logger.warning(
                    "D1TrapExecutionBridge: BUY %s — [%s/%s] can_trade() gate closed "
                    "(strategy=%s).", ev.underlying, ev.client_id, ev.binding_id, ev.strategy,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            logger.info(
                "D1TrapExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.broker_resolve import resolve_broker_or_alert
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, "D1Trap",
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "D1TrapExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            # Do NOT call _paper_fill here -- that would fabricate a fill the book
            # would treat as real. resolve_broker_or_alert already logged CRITICAL
            # and published SYSTEM_EVENT; abort loudly instead of just dropping the
            # order (the book is waiting/relying on this to revert or not-finalize).
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker)

    async def _abort(self, ev, routing_failed: bool = False) -> None:
        """Convert a routing failure into a fill-shaped event instead of silence.
        Mirrors execution_bridge/cascade_bridge.py's _abort() exactly, adapted to
        D1TrapOrderEvent's field names."""
        await self._bus.publish(Topic.D1_TRAP_ORDER_FILL, D1TrapFillEvent(
            action=ev.action, underlying=ev.underlying,
            option_type=getattr(ev, "option_type", "") or "",
            strike=int(getattr(ev, "strike", 0) or 0),
            fill_price=0.0, qty=int(getattr(ev, "quantity", 0) or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "",
            entry_aborted=(ev.action == "BUY"),
            # SELL never gets a fabricated fill either -- bear_only_book.py's
            # _on_fill leaves a leg awaiting confirmation exactly as it was (still
            # open, still persisted) on exit_failed=True, same as SellStraddle's
            # exit_aborted and V4Cascade's exit_failed.
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev) -> None:
        # 2026-08-03 fix: entry_price on a SELL/exit event is the ORIGINAL entry, not the
        # fill -- use the event's real exit_price for a SELL, entry_price for a BUY.
        # (exit_price defaults to 0.0 on older/legacy events that never set it, e.g.
        # book.py's D1TrapOptionBook -- fall back to entry_price rather than logging 0.)
        fill_price = ev.entry_price
        if ev.action == "SELL" and getattr(ev, "exit_price", 0.0) > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] D1Trap %s %s %s%d exp=%s qty=%d spot=%.2f | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} spot={fill_price:.2f} reason={ev.reason}",
        )
        self._record_history(ev, fill_price, paper=True)
        await self._bus.publish(Topic.D1_TRAP_ORDER_FILL, D1TrapFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=fill_price, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "", paper_mode=True,
        ))

    # ── live ──────────────────────────────────────────────────────────────────

    async def _live_fill(self, ev, broker) -> None:
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "D1TrapExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
                ev.underlying, ev.option_type, ev.strike,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        side = OrderSide.BUY if ev.action == "BUY" else OrderSide.SELL
        exchange = order_exchange(ev.underlying)

        product = getattr(ev, "product_type", None) or "MIS"
        req = OrderRequest(
            symbol=symbol,
            exchange=exchange,
            side=side,
            qty=ev.quantity,
            order_type=OrderType.MARKET,
            product=product,
            price=ev.entry_price,  # ignored for MARKET; MockBroker uses as fill
            tag=f"D1T_{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        order_id = ""
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            if avg > 0:
                logger.info(
                    "[LIVE] D1Trap %s %s %s%d exp=%s qty=%d @ %.2f order_id=%s | client=%s/%s",
                    ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                    ev.quantity, avg, order_id, ev.client_id, ev.binding_id,
                )
                self._trade_log.log(
                    ev.client_id, ev.binding_id,
                    f"[LIVE] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                    f"exp={ev.expiry} qty={ev.quantity} @ {avg:.2f} symbol={symbol} "
                    f"order_id={order_id} reason={ev.reason}",
                )
        except Exception as exc:
            logger.error(
                "[LIVE] D1Trap %s %s %s%d order FAILED: %s.",
                ev.action, ev.underlying, ev.option_type, ev.strike, exc,
            )
            self._trade_log.log(
                ev.client_id, ev.binding_id,
                f"LIVE {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
                f"FAILED: {exc}",
            )
            avg = 0.0

        # No confirmed fill price -- the order did not actually execute (rejected,
        # zero-fill, or the broker call raised an exception). Previously this fell
        # back to ev.entry_price and reported success regardless -- a rejected/
        # failed EXIT would silently look like a real close (2026-08-05 fix, the
        # exact class of bug already fixed for SellStraddle/V4Cascade: fabricating
        # a fill the book would treat as a real exchange confirmation). Abort
        # instead of faking it; bear_only_book.py's _on_fill leaves the leg
        # exactly as it was awaiting confirmation.
        if avg <= 0:
            logger.error(
                "[LIVE] D1Trap %s %s %s%d — NO confirmed fill, %s NOT reported as a "
                "fill (no phantom position/close). client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        self._record_history(ev, avg, paper=False)
        await self._bus.publish(Topic.D1_TRAP_ORDER_FILL, D1TrapFillEvent(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=avg, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "", paper_mode=False, symbol=symbol,
        ))

    def _resolve_symbol(self, ev, broker) -> str:
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        _b = getattr(broker, "_binding", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return _resolve_option_symbol(
            ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider
        )

    def _record_history(self, ev, fill_price: float, paper: bool) -> None:
        if ev.action != "SELL":
            return
        try:
            from data_layer import trade_history as _th
            # 2026-08-03 fix: was hardcoded 0.0 -- both book.py and bear_only_book.py are
            # buyer-only (BUY to open/pay premium, SELL to close/receive premium)
            # regardless of the LONG/SHORT signal direction, so P&L is always
            # (exit - entry) * qty for the option premium itself.
            pnl = round((fill_price - ev.entry_price) * ev.quantity, 2)
            strategy_name = getattr(ev, "strategy", "d1_trap_option") or "d1_trap_option"
            _entry_ts = getattr(ev, "entry_ts", None)
            _th.record(
                ev.client_id, strategy_name, ev.underlying,
                ev.entry_price, fill_price, ev.reason, pnl,
                binding_id=ev.binding_id,
                legs=[{
                    "side": ev.option_type,
                    "strike": ev.strike,
                    "entry": ev.entry_price,
                    "exit": fill_price,
                    "pnl": pnl,
                    # 2026-08-03 fix: entry_reason used to reuse ev.reason (the CLOSE
                    # reason, e.g. eod/sl_hit) since that was the only reason string
                    # available -- now uses the real order_reason the leg was opened
                    # with (e.g. bear_trap_flip_t1), falling back to ev.reason only if
                    # an older event never set it.
                    "entry_reason": getattr(ev, "entry_reason", "") or ev.reason,
                    "entry_ts": _entry_ts.isoformat() if hasattr(_entry_ts, "isoformat") else _entry_ts,
                    "exit_ts": ev.trigger_ts.isoformat() if hasattr(ev.trigger_ts, "isoformat") else ev.trigger_ts,
                }],
            )
        except Exception:
            logger.exception(
                "D1TrapExecutionBridge: trade_history record failed for %s/%s",
                ev.client_id, ev.binding_id,
            )
