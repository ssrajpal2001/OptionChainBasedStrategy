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

2026-08-05 (Task 8): the routing/paper/live/abort/history mechanics (which
were byte-for-byte identical to execution_bridge/fvg_bridge.py after Tasks
6-7 mirrored this file exactly) now live in
execution_bridge/option_buyer_bridge_base.py::OptionBuyerExecutionBridge.
This file is a thin subclass supplying only what's genuinely D1Trap-specific:
the Topics, the fill-event class, the request-event class, and the trade-log/
label strings.
"""
from __future__ import annotations

from dataclasses import dataclass

from config.global_config import Topic
from execution_bridge.option_buyer_bridge_base import (
    OptionBuyerExecutionBridge,
    OptionBuyerFillEvent,
)


@dataclass
class D1TrapFillEvent(OptionBuyerFillEvent):
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


class D1TrapExecutionBridge(OptionBuyerExecutionBridge):
    """
    Listens for D1TrapOrderEvent on Topic.D1_TRAP_ORDER_REQUEST.

    Routes BUY (ENTRY) and SELL (EXIT) to the owning (client_id, binding_id)
    broker.  Resolves the correct broker option symbol (Zerodha NIFTY2672924300CE,
    Upstox numeric key, etc.) at order time using the broker's provider field.
    """

    REQUEST_TOPIC = Topic.D1_TRAP_ORDER_REQUEST
    FILL_TOPIC = Topic.D1_TRAP_ORDER_FILL
    FILL_EVENT_CLS = D1TrapFillEvent
    KEY_TAG = "d1_trap"
    LOG_TAG = "D1TRAP"
    BROKER_RESOLVE_LABEL = "D1Trap"
    ORDER_TAG_PREFIX = "D1T_"
    # ev.strategy is a required (non-default) field on D1TrapOrderEvent, so this
    # fallback is never actually hit in practice -- kept for parity with
    # _record_history's original `getattr(ev, "strategy", "d1_trap_option")`.
    DEFAULT_GATE_STRATEGY = "d1_trap_option"
    DEFAULT_HISTORY_STRATEGY = "d1_trap_option"

    def _order_event_cls(self):
        from strategies.d1_trap_option.book import D1TrapOrderEvent
        return D1TrapOrderEvent
