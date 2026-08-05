"""
execution_bridge/fvg_bridge.py — FVG (Fair Value Gap) strategy order router.

Subscribes to Topic.FVG_ORDER_REQUEST for FVGOrderEvent objects published by
strategies/fvg/engine.py. Modeled directly on execution_bridge/d1_trap_bridge.py
(same routing/paper/live/trade-log shape) — see that file for the fuller
design rationale; this is a thin swap of topic/event type/log tag.

Routes STRICTLY to the event's own (client_id, binding_id) broker — never
broadcasts. Paper mode — local simulated fill at strategy spot price; NO
real order. Live mode — MARKET order via broker.place_order() +
get_order_status(); position booked from the real fill price.

Product: MIS (intraday — all positions are squared off by EOD in engine.py).

2026-08-05: confirm-then-finalize fill-confirmation feedback loop, mirroring
execution_bridge/d1_trap_bridge.py's D1TrapFillEvent/_abort() exactly (same
proven pattern, same problem: strategies/fvg/engine.py's _open_position/
_square_off now dispatch an order and WAIT for this bridge's FVGOrderFillEvent
to confirm a real fill -- or an entry_aborted/exit_failed abort -- before
mutating/persisting self._position. Previously this bridge never published to
Topic.FVG_ORDER_FILL at all, and a broker-unreachable EXIT silently looked
like a successful close while the leg was still open at the broker (same root
class of bug already fixed for SellStraddle/V4Cascade/D1Trap-BearOnly)."""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import _resolve_option_symbol

logger = logging.getLogger(__name__)


@dataclass
class FVGOrderFillEvent:
    """Published by FVGExecutionBridge to Topic.FVG_ORDER_FILL after every
    order attempt (paper and live, success and failure). Field shape mirrors
    D1TrapFillEvent (execution_bridge/d1_trap_bridge.py) exactly -- same
    proven confirm-then-finalize contract, separate class per this
    codebase's per-strategy-event convention (CascadeFillEvent/
    StraddleFillEvent/D1TrapFillEvent are likewise separate classes despite
    near-identical shape)."""
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
    # True when a LIVE BUY failed to route (no route/no broker) -- the engine
    # must discard its optimistic position rather than manage a phantom one.
    entry_aborted:  bool = False
    routing_failed: bool = False
    # True when a LIVE SELL (exit) could not be routed -- the engine must
    # leave the position exactly as it was (still open, still persisted)
    # rather than believe an unrouted close actually happened.
    exit_failed: bool = False


class _FVGTradeLogger:
    def __init__(self, log_dir: str = "logs/trades") -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-fvg-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  FVG  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class FVGExecutionBridge:
    """
    Listens for FVGOrderEvent on Topic.FVG_ORDER_REQUEST.

    Routes BUY (ENTRY) and SELL (EXIT) to the owning (client_id, binding_id)
    broker. Resolves the correct broker option symbol at order time using
    the broker's provider field (same _resolve_option_symbol helper the
    D1Trap/straddle bridges already use).
    """

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _FVGTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.FVG_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        logger.info("FVGExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                from strategies.fvg.engine import FVGOrderEvent
                if not isinstance(ev, FVGOrderEvent):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("FVGExecutionBridge: _handle error.")

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("FVGExecutionBridge: stopped.")

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("FVGExecutionBridge: event missing client_id/binding_id — dropped.")
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
                "FVGExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            # SELL (EXIT) must abort too, not just silently drop -- engine.py's
            # _open_position/_square_off optimistically dispatch and AWAIT a
            # FVGOrderFillEvent before mutating/persisting self._position; a
            # silent return here (no fill event at all) leaves that wait
            # hanging until its own timeout instead of failing loud immediately.
            await self._abort(ev, routing_failed=True)
            return

        # EXIT must always route — gate only ENTRY on the shared can_trade() gate
        # (terminal_connected AND is_trade_enabled AND a running fvg deployment
        # for this underlying on this binding).
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            if not can_trade(ev.client_id, ev.binding_id, db, "fvg", ev.underlying):
                logger.warning(
                    "FVGExecutionBridge: BUY %s — [%s/%s] can_trade() gate closed.",
                    ev.underlying, ev.client_id, ev.binding_id,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            logger.info(
                "FVGExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.broker_resolve import resolve_broker_or_alert
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, "FVG",
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "FVGExecutionBridge: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            # Do NOT call _paper_fill here -- that would fabricate a fill the
            # engine would treat as real. resolve_broker_or_alert already logged
            # CRITICAL and published SYSTEM_EVENT; abort loudly instead of just
            # dropping the order (the engine is waiting/relying on this to
            # revert or not-finalize).
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker)

    async def _abort(self, ev, routing_failed: bool = False) -> None:
        """Convert a routing failure into a fill-shaped event instead of silence.
        Mirrors execution_bridge/cascade_bridge.py's _abort() / d1_trap_bridge.py's
        _abort() exactly, adapted to FVGOrderEvent's field names."""
        await self._bus.publish(Topic.FVG_ORDER_FILL, FVGOrderFillEvent(
            action=ev.action, underlying=ev.underlying,
            option_type=getattr(ev, "option_type", "") or "",
            strike=int(getattr(ev, "strike", 0) or 0),
            fill_price=0.0, qty=int(getattr(ev, "quantity", 0) or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "",
            entry_aborted=(ev.action == "BUY"),
            # SELL never gets a fabricated fill either -- engine.py's _on_fill
            # leaves the position awaiting confirmation exactly as it was
            # (still open, still persisted) on exit_failed=True, same as
            # D1Trap-BearOnly's exit_failed and SellStraddle's exit_aborted.
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev) -> None:
        # 2026-08-03 fix: same as D1TrapExecutionBridge -- entry_price on a SELL/exit
        # event is the ORIGINAL entry, not the fill; use the real exit_price for SELL.
        fill_price = ev.entry_price
        if ev.action == "SELL" and getattr(ev, "exit_price", 0.0) > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] FVG %s %s %s%d exp=%s qty=%d spot=%.2f | client=%s/%s",
            ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} spot={fill_price:.2f} reason={ev.reason}",
        )
        self._record_history(ev, fill_price, paper=True)
        await self._bus.publish(Topic.FVG_ORDER_FILL, FVGOrderFillEvent(
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
                "FVGExecutionBridge: no tradable symbol for %s %s%d — paper fallback.",
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
            tag=f"FVG_{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        order_id = ""
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            if avg > 0:
                logger.info(
                    "[LIVE] FVG %s %s %s%d exp=%s qty=%d @ %.2f order_id=%s | client=%s/%s",
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
                "[LIVE] FVG %s %s %s%d order FAILED: %s.",
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
        # same class of bug already fixed for SellStraddle/V4Cascade/D1Trap-
        # BearOnly: fabricating a fill the engine would treat as a real exchange
        # confirmation). Abort instead of faking it; engine.py's _square_off/
        # _open_position leave the position exactly as it was awaiting
        # confirmation.
        if avg <= 0:
            logger.error(
                "[LIVE] FVG %s %s %s%d — NO confirmed fill, %s NOT reported as a "
                "fill (no phantom position/close). client=%s/%s",
                ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        self._record_history(ev, avg, paper=False)
        await self._bus.publish(Topic.FVG_ORDER_FILL, FVGOrderFillEvent(
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
            # 2026-08-03 fix: was hardcoded 0.0 -- FVG is buyer-only (BUY to open/pay
            # premium, SELL to close/receive premium), so P&L is (exit - entry) * qty.
            pnl = round((fill_price - ev.entry_price) * ev.quantity, 2)
            _entry_ts = getattr(ev, "entry_ts", None)
            _th.record(
                ev.client_id, "fvg", ev.underlying,
                ev.entry_price, fill_price, ev.reason, pnl,
                binding_id=ev.binding_id,
                legs=[{
                    "side": ev.option_type,
                    "strike": ev.strike,
                    "entry": ev.entry_price,
                    "exit": fill_price,
                    "pnl": pnl,
                    "entry_reason": getattr(ev, "entry_reason", "") or ev.reason,
                    "entry_ts": _entry_ts.isoformat() if hasattr(_entry_ts, "isoformat") else _entry_ts,
                    "exit_ts": ev.trigger_ts.isoformat() if hasattr(ev.trigger_ts, "isoformat") else ev.trigger_ts,
                }],
            )
        except Exception:
            logger.exception(
                "FVGExecutionBridge: trade_history record failed for %s/%s",
                ev.client_id, ev.binding_id,
            )
