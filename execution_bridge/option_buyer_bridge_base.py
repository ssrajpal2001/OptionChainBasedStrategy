"""
execution_bridge/option_buyer_bridge_base.py — shared base for the D1Trap and
FVG execution bridges.

Both `execution_bridge/d1_trap_bridge.py` and `execution_bridge/fvg_bridge.py`
route a per-(client_id, binding_id) option-BUYER order event (BUY to open,
SELL to close) to the owning broker, with the exact same paper/live/abort
mechanics: gate ENTRY on `strategies/core/gate.py::can_trade()`, EXIT always
routes, paper mode books a local sim fill, live mode places a MARKET order and
only reports a fill on a confirmed avg_price > 0 (2026-08-05 confirm-then-
finalize fix — see D1TrapFillEvent's docstring for the fuller rationale: a
broker-unreachable EXIT must never silently look like a successful close).

Task 7 mirrored Task 6 exactly (by design — same proven shape), which made
`d1_trap_bridge.py` and `fvg_bridge.py` near-identical files apart from five
things: the request/fill Topics, the fill-event dataclass, the request-event
class used for the `run()` isinstance check, the strategy-name string used for
the ENTRY `can_trade()` gate, and cosmetic labels (log tag, order tag prefix).
This module extracts everything else — trade logger, `_handle`'s routing
skeleton, `_abort`, `_paper_fill`, `_live_fill`'s broker-order-placement
mechanics, `_resolve_symbol` (confirmed identical in both files — both just
delegate to `execution_bridge.straddle_bridge._resolve_option_symbol` with no
strategy-specific strike/expiry logic in the bridge itself; any OI-wall-aware
vs fixed-ITM-offset selection lives upstream in the book/engine that builds
the order event, not in either bridge), and `_record_history` — into one base
class, parameterized by the five real differences via class attributes and two
small overridable hooks.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Type

from config.global_config import IST, order_exchange
from data_layer.base_feeder import EventBus

logger = logging.getLogger(__name__)


@dataclass
class OptionBuyerFillEvent:
    """Common field shape for the per-strategy fill-event dataclasses
    (D1TrapFillEvent, FVGOrderFillEvent). Published after every order attempt
    (paper and live, success and failure) — mirrors CascadeFillEvent
    (execution_bridge/cascade_bridge.py) exactly, same proven confirm-then-
    finalize contract. Subclassed (not used directly) so each bridge keeps
    its own concrete type name per this codebase's per-strategy-event
    convention (CascadeFillEvent/StraddleFillEvent/D1TrapFillEvent/
    FVGOrderFillEvent are likewise separate classes despite identical shape)."""
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
    # True when a LIVE BUY failed to route (no route/no broker) -- the
    # book/engine must discard its optimistic position rather than manage a
    # phantom one.
    entry_aborted:  bool = False
    routing_failed: bool = False
    # True when a LIVE SELL (exit) could not be routed -- the book/engine must
    # leave the position exactly as it was (still open, still persisted)
    # rather than believe an unrouted close actually happened.
    exit_failed: bool = False


class _OptionBuyerTradeLogger:
    """Per-(client,binding,day) append-only trade log, one line per order
    attempt. `key_tag` scopes the log filename per strategy (e.g. "d1_trap",
    "fvg"); `line_tag` is the human-readable prefix written into each line
    (e.g. "D1TRAP", "FVG")."""

    def __init__(self, key_tag: str, line_tag: str, log_dir: str = "logs/trades") -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._key_tag = key_tag
        self._line_tag = line_tag
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-{self._key_tag}-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  {self._line_tag}  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


class OptionBuyerExecutionBridge:
    """
    Shared routing/paper/live/abort/history mechanics for a per-binding
    option-buyer order bridge. Subclasses supply:

    Class attributes (required):
    - `REQUEST_TOPIC` / `FILL_TOPIC` — `Topic.*` the bridge subscribes to /
      publishes fills on.
    - `FILL_EVENT_CLS` — concrete `OptionBuyerFillEvent` subclass to
      instantiate for every fill/abort.
    - `KEY_TAG` — short strategy tag used in the trade-log filename
      (e.g. "d1_trap", "fvg").
    - `LOG_TAG` — human-readable trade-log line prefix (e.g. "D1TRAP", "FVG").
    - `BROKER_RESOLVE_LABEL` — label passed to `resolve_broker_or_alert()`'s
      `strategy` arg (e.g. "D1Trap", "FVG") — purely cosmetic (alert text).
    - `ORDER_TAG_PREFIX` — prefix for the broker `OrderRequest.tag`
      (e.g. "D1T_", "FVG_").
    - `DEFAULT_GATE_STRATEGY` — fallback strategy name for the ENTRY
      `can_trade()` gate when the order event has no usable `.strategy`
      attribute of its own.
    - `DEFAULT_HISTORY_STRATEGY` — fallback strategy name passed to
      `trade_history.record()`.

    Hooks (overridable, sane defaults provided):
    - `_order_event_cls()` — returns the request-event class used to filter
      `run()`'s queue (`D1TrapOrderEvent` / `FVGOrderEvent`). Must be
      implemented by each subclass (import is strategy-module-specific and
      would otherwise create a circular import at bridge-module load time).
    """

    # -- subclass-supplied identity (see class docstring) --------------------
    REQUEST_TOPIC = None
    FILL_TOPIC = None
    FILL_EVENT_CLS: Optional[Type[OptionBuyerFillEvent]] = None
    KEY_TAG = ""
    LOG_TAG = ""
    BROKER_RESOLVE_LABEL = ""
    ORDER_TAG_PREFIX = ""
    DEFAULT_GATE_STRATEGY = ""
    DEFAULT_HISTORY_STRATEGY = ""

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _OptionBuyerTradeLogger(self.KEY_TAG, self.LOG_TAG, log_dir)
        self._running = False
        self._q = bus.subscribe(self.REQUEST_TOPIC)

    def _order_event_cls(self):
        """Returns the request-event class used to filter run()'s queue.
        Subclasses override with their own (lazily-imported, to avoid a
        circular import at module load time) event class."""
        raise NotImplementedError

    async def run(self) -> None:
        self._running = True
        logger.info("%s: started.", type(self).__name__)
        event_cls = self._order_event_cls()
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                if not isinstance(ev, event_cls):
                    continue
                await self._handle(ev)
            except Exception:
                logger.exception("%s: _handle error.", type(self).__name__)

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("%s: stopped.", type(self).__name__)

    # ── routing ───────────────────────────────────────────────────────────────

    async def _handle(self, ev) -> None:
        cls_name = type(self).__name__
        if not ev.client_id or not ev.binding_id:
            logger.error("%s: event missing client_id/binding_id — dropped.", cls_name)
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
                "%s: %s %s — [%s/%s] terminal not connected, no route.",
                cls_name, ev.action, ev.underlying, ev.client_id, ev.binding_id,
            )
            # SELL (EXIT) must abort too, not just silently drop -- the owning
            # book/engine optimistically appends/mutates a position and awaits
            # a fill event, relying on an abort to revert/not-finalize it if
            # the order never reached the broker. Silently returning here (no
            # fill event at all) means the caller hangs waiting for a
            # confirmation that will never come (BUY) or -- pre-confirm-then-
            # finalize -- believed a leg closed that never left the exchange
            # (SELL).
            await self._abort(ev, routing_failed=True)
            return

        # EXIT must always route — gate only ENTRY on the shared can_trade()
        # gate (terminal_connected AND is_trade_enabled AND a running
        # deployment of THIS exact strategy for THIS underlying on THIS
        # binding).
        if ev.action == "BUY" and db is not None:
            from strategies.core.gate import can_trade
            gate_strategy = self._gate_strategy_name(ev)
            if not can_trade(ev.client_id, ev.binding_id, db, gate_strategy, ev.underlying):
                logger.warning(
                    "%s: BUY %s — [%s/%s] can_trade() gate closed (strategy=%s).",
                    cls_name, ev.underlying, ev.client_id, ev.binding_id, gate_strategy,
                )
                await self._abort(ev, routing_failed=True)
                return

        mode = live_binding.get("trading_mode", "paper") or "paper"

        if mode == "paper":
            logger.info(
                "%s: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=paper",
                cls_name, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
                ev.quantity, ev.client_id, ev.binding_id,
            )
            await self._paper_fill(ev)
            return

        from execution_bridge.broker_resolve import resolve_broker_or_alert
        broker = await resolve_broker_or_alert(
            self._bus, self._router, ev.client_id, ev.binding_id, self.BROKER_RESOLVE_LABEL,
            context=f"{ev.action} {ev.underlying} {ev.option_type}{ev.strike}",
        )

        logger.info(
            "%s: %s %s %s%d exp=%s qty=%d → [%s/%s] mode=%s broker=%s",
            cls_name, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, ev.client_id, ev.binding_id, mode,
            "resolved" if broker is not None else "UNAVAILABLE",
        )

        if broker is None:
            # Do NOT call _paper_fill here -- that would fabricate a fill the
            # caller would treat as real. resolve_broker_or_alert already
            # logged CRITICAL and published SYSTEM_EVENT; abort loudly
            # instead of just dropping the order (the caller is waiting/
            # relying on this to revert or not-finalize).
            await self._abort(ev, routing_failed=True)
            return

        await self._live_fill(ev, broker)

    def _gate_strategy_name(self, ev) -> str:
        """Strategy name passed to can_trade() for the ENTRY gate. Defaults
        to the order event's own `.strategy` attribute (D1TrapOrderEvent
        always sets one; FVGOrderEvent also carries one, defaulting to
        "fvg") falling back to DEFAULT_GATE_STRATEGY if the event has none."""
        return getattr(ev, "strategy", None) or self.DEFAULT_GATE_STRATEGY

    async def _abort(self, ev, routing_failed: bool = False) -> None:
        """Convert a routing failure into a fill-shaped event instead of
        silence. Mirrors execution_bridge/cascade_bridge.py's _abort()
        exactly, adapted to the subclass's own fill-event class."""
        await self._bus.publish(self.FILL_TOPIC, self.FILL_EVENT_CLS(
            action=ev.action, underlying=ev.underlying,
            option_type=getattr(ev, "option_type", "") or "",
            strike=int(getattr(ev, "strike", 0) or 0),
            fill_price=0.0, qty=int(getattr(ev, "quantity", 0) or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "",
            entry_aborted=(ev.action == "BUY"),
            # SELL never gets a fabricated fill either -- the owning
            # book/engine leaves a position awaiting confirmation exactly as
            # it was (still open, still persisted) on exit_failed=True, same
            # as SellStraddle's exit_aborted and V4Cascade's exit_failed.
            exit_failed=(ev.action == "SELL"),
            routing_failed=routing_failed,
        ))

    # ── paper ─────────────────────────────────────────────────────────────────

    async def _paper_fill(self, ev) -> None:
        # 2026-08-03 fix: entry_price on a SELL/exit event is the ORIGINAL entry, not the
        # fill -- use the event's real exit_price for a SELL, entry_price for a BUY.
        # (exit_price defaults to 0.0 on older/legacy events that never set it -- fall
        # back to entry_price rather than logging 0.)
        cls_name = type(self).__name__
        fill_price = ev.entry_price
        if ev.action == "SELL" and getattr(ev, "exit_price", 0.0) > 0:
            fill_price = ev.exit_price
        logger.info(
            "[PAPER] %s %s %s %s%d exp=%s qty=%d spot=%.2f | client=%s/%s",
            self.LOG_TAG, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
            ev.quantity, fill_price, ev.client_id, ev.binding_id,
        )
        self._trade_log.log(
            ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.option_type} strike={ev.strike} "
            f"exp={ev.expiry} qty={ev.quantity} spot={fill_price:.2f} reason={ev.reason}",
        )
        self._record_history(ev, fill_price, paper=True)
        await self._bus.publish(self.FILL_TOPIC, self.FILL_EVENT_CLS(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=fill_price, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "", paper_mode=True,
        ))

    # ── live ──────────────────────────────────────────────────────────────────

    async def _live_fill(self, ev, broker) -> None:
        cls_name = type(self).__name__
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error(
                "%s: no tradable symbol for %s %s%d — paper fallback.",
                cls_name, ev.underlying, ev.option_type, ev.strike,
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
            tag=f"{self.ORDER_TAG_PREFIX}{ev.underlying}_{ev.action}"[:20],
        )

        avg = 0.0
        order_id = ""
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(str(order_id))
            avg = float(getattr(fill, "avg_price", 0.0) or 0.0)
            if avg > 0:
                logger.info(
                    "[LIVE] %s %s %s %s%d exp=%s qty=%d @ %.2f order_id=%s | client=%s/%s",
                    self.LOG_TAG, ev.action, ev.underlying, ev.option_type, ev.strike, ev.expiry,
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
                "[LIVE] %s %s %s %s%d order FAILED: %s.",
                self.LOG_TAG, ev.action, ev.underlying, ev.option_type, ev.strike, exc,
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
        # a fill the caller would treat as a real exchange confirmation). Abort
        # instead of faking it; the owning book/engine leaves the leg/position
        # exactly as it was awaiting confirmation.
        if avg <= 0:
            logger.error(
                "[LIVE] %s %s %s %s%d — NO confirmed fill, %s NOT reported as a "
                "fill (no phantom position/close). client=%s/%s",
                self.LOG_TAG, ev.action, ev.underlying, ev.option_type, ev.strike,
                "entry" if ev.action == "BUY" else "exit", ev.client_id, ev.binding_id,
            )
            await self._abort(ev, routing_failed=False)
            return

        self._record_history(ev, avg, paper=False)
        await self._bus.publish(self.FILL_TOPIC, self.FILL_EVENT_CLS(
            action=ev.action, underlying=ev.underlying, option_type=ev.option_type or "",
            strike=int(ev.strike or 0), fill_price=avg, qty=int(ev.quantity or 0),
            client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=getattr(ev, "event_id", "") or "", paper_mode=False, symbol=symbol,
        ))

    def _resolve_symbol(self, ev, broker) -> str:
        # Confirmed identical between D1Trap and FVG (Task 8 Step 1 diff): both
        # just delegate to _resolve_option_symbol with no strategy-specific
        # strike/expiry logic in the bridge itself -- any OI-wall-aware vs
        # fixed-ITM-offset selection happens upstream, in the book/engine that
        # builds the order event, not here.
        if not ev.expiry or not ev.strike or not ev.option_type:
            return ""
        from execution_bridge.straddle_bridge import _resolve_option_symbol
        _b = getattr(broker, "_binding", None)
        provider = _b.provider if _b else getattr(broker, "provider", "mock")
        return _resolve_option_symbol(
            ev.underlying, ev.expiry, int(ev.strike), ev.option_type, provider
        )

    def _history_strategy_name(self, ev) -> str:
        """Strategy name passed to trade_history.record(). Defaults to the
        order event's own `.strategy` attribute, falling back to
        DEFAULT_HISTORY_STRATEGY."""
        return getattr(ev, "strategy", None) or self.DEFAULT_HISTORY_STRATEGY

    def _record_history(self, ev, fill_price: float, paper: bool) -> None:
        if ev.action != "SELL":
            return
        try:
            from data_layer import trade_history as _th
            # 2026-08-03 fix: was hardcoded 0.0 -- both bridges' owning books/engines are
            # buyer-only (BUY to open/pay premium, SELL to close/receive premium)
            # regardless of the LONG/SHORT signal direction, so P&L is always
            # (exit - entry) * qty for the option premium itself.
            pnl = round((fill_price - ev.entry_price) * ev.quantity, 2)
            strategy_name = self._history_strategy_name(ev)
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
                    # available -- now uses the real order_reason the leg/position was
                    # opened with (e.g. bear_trap_flip_t1, fvg_retest), falling back to
                    # ev.reason only if an older event never set it.
                    "entry_reason": getattr(ev, "entry_reason", "") or ev.reason,
                    "entry_ts": _entry_ts.isoformat() if hasattr(_entry_ts, "isoformat") else _entry_ts,
                    "exit_ts": ev.trigger_ts.isoformat() if hasattr(ev.trigger_ts, "isoformat") else ev.trigger_ts,
                }],
            )
        except Exception:
            logger.exception(
                "%s: trade_history record failed for %s/%s",
                type(self).__name__, ev.client_id, ev.binding_id,
            )
