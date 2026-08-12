"""
strategies/oi_flow/events.py — order/fill event dataclasses for the OI-Flow
Pre-Breakout strategy.

Written fresh, not subclassing execution_bridge.option_buyer_bridge_base's
OptionBuyerFillEvent -- that base class is specifically the shared
foundation the D1Trap/FVG bridges already build on, and this strategy
shares zero runtime code with any other strategy by explicit user
direction (see strategies/oi_flow/__init__.py). The field shape below is
independently arrived at, even though it looks similar to the proven
confirm-then-finalize contract used elsewhere in this codebase -- that
similarity is deliberate (it's a genuinely good, battle-tested shape), the
class itself is not shared.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class OIFlowOrderEvent:
    """Published to Topic.OI_FLOW_ORDER_REQUEST. action="BUY" opens a
    position (pre-breakout entry); action="SELL" closes it (SL/EOD/risk-cap
    exit -- whatever the engine decided)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: the position's original entry (for P&L).
    sl_price:     float          # option-chart-anchored stop (see detector.confirm_option_price_action)
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    entry_ts:     Optional[datetime] = None
    product_type: str = "MIS"
    strategy:     str = "oi_flow"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class OIFlowFillEvent:
    """Published to Topic.OI_FLOW_ORDER_FILL after every order attempt
    (paper and live, success and failure) -- same confirm-then-finalize
    contract this codebase always uses: a routing failure or an
    unconfirmed live fill must never be silently treated as a real fill by
    the owning engine."""
    action:        str
    underlying:    str
    option_type:   str
    strike:        int
    fill_price:    float
    qty:           int
    client_id:     str
    binding_id:    str
    event_id:      str
    paper_mode:    bool = True
    symbol:        str = ""
    timestamp:     datetime = field(default_factory=lambda: datetime.now(IST))
    # True when a live BUY failed to route (no route/no broker/gate closed) --
    # the engine must discard its optimistic position, never manage a phantom one.
    entry_aborted:  bool = False
    routing_failed: bool = False
    # True when a live SELL (exit) could not be routed -- the engine must
    # leave the position exactly as it was (still open, still persisted).
    exit_failed: bool = False
