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
class of bug already fixed for SellStraddle/V4Cascade/D1Trap-BearOnly).

2026-08-05 (Task 8): the routing/paper/live/abort/history mechanics (which
were byte-for-byte identical to execution_bridge/d1_trap_bridge.py after
Tasks 6-7 mirrored that file exactly) now live in
execution_bridge/option_buyer_bridge_base.py::OptionBuyerExecutionBridge.
This file is a thin subclass supplying only what's genuinely FVG-specific:
the Topics, the fill-event class, the request-event class, and the trade-log/
label strings."""
from __future__ import annotations

from dataclasses import dataclass

from config.global_config import Topic
from execution_bridge.option_buyer_bridge_base import (
    OptionBuyerExecutionBridge,
    OptionBuyerFillEvent,
)


@dataclass
class FVGOrderFillEvent(OptionBuyerFillEvent):
    """Published by FVGExecutionBridge to Topic.FVG_ORDER_FILL after every
    order attempt (paper and live, success and failure). Field shape mirrors
    D1TrapFillEvent (execution_bridge/d1_trap_bridge.py) exactly -- same
    proven confirm-then-finalize contract, separate class per this
    codebase's per-strategy-event convention (CascadeFillEvent/
    StraddleFillEvent/D1TrapFillEvent are likewise separate classes despite
    near-identical shape)."""


class FVGExecutionBridge(OptionBuyerExecutionBridge):
    """
    Listens for FVGOrderEvent on Topic.FVG_ORDER_REQUEST.

    Routes BUY (ENTRY) and SELL (EXIT) to the owning (client_id, binding_id)
    broker. Resolves the correct broker option symbol at order time using
    the broker's provider field (same _resolve_option_symbol helper the
    D1Trap/straddle bridges already use).
    """

    REQUEST_TOPIC = Topic.FVG_ORDER_REQUEST
    FILL_TOPIC = Topic.FVG_ORDER_FILL
    FILL_EVENT_CLS = FVGOrderFillEvent
    KEY_TAG = "fvg"
    LOG_TAG = "FVG"
    BROKER_RESOLVE_LABEL = "FVG"
    ORDER_TAG_PREFIX = "FVG_"
    # FVGOrderEvent.strategy defaults to "fvg" (never overridden at any
    # construction site in strategies/fvg/engine.py), so this fallback
    # matches the original hardcoded "fvg" byte-for-byte.
    DEFAULT_GATE_STRATEGY = "fvg"
    DEFAULT_HISTORY_STRATEGY = "fvg"

    def _order_event_cls(self):
        from strategies.fvg.engine import FVGOrderEvent
        return FVGOrderEvent
