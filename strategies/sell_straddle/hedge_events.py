"""
strategies/sell_straddle/hedge_events.py — order/fill events for the EOD
hedge-and-carry feature (2026-08-20, user spec).

Deliberately separate from execution_bridge/straddle_bridge.py's own
StraddleOrderEvent/StraddleFillEvent: that bridge is hardcoded SELL-to-open
(entry) / BUY-to-close (exit) everywhere -- the entire shape of a sold
straddle leg. A hedge leg is the opposite direction (BUY-to-open,
SELL-to-close), and it needed its own bridge + own Topic
(Topic.STRADDLE_HEDGE_ORDER_REQUEST/FILL) rather than touching the shared
bridge that's routing real live sold-leg orders for existing clients right
now. One event per LEG (not a combined CE+PE pair like StraddleOrderEvent) --
the hedge builds/closes its two legs (CE hedge, PE hedge) as two independent
single-leg BUY/SELL orders, mirroring the same shape already proven by
LiquiditySweepOrderEvent/OI-Flow's own single-leg option-buyer events.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class StraddleHedgeOrderEvent:
    """Published to Topic.STRADDLE_HEDGE_ORDER_REQUEST. action="BUY" opens a
    protective hedge leg; action="SELL" closes it (T-1-from-expiry forced
    closure, or the same-strike-collision guard)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: original entry (for P&L).
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    entry_ts:     Optional[datetime] = None
    product_type: str = "NRML"   # hedge-and-carry always runs NRML -- see engine.py's module docstring
    strategy:     str = "sell_straddle"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class StraddleHedgeFillEvent:
    """Published to Topic.STRADDLE_HEDGE_ORDER_FILL after every order attempt
    (paper and live, success and failure) -- same confirm-then-finalize
    contract every bridge in this codebase uses: a routing failure or an
    unconfirmed live fill must never be silently treated as a real fill by
    the owning book."""
    action:        str
    underlying:    str
    option_type:   str
    strike:        int
    fill_price:    float
    qty:           int          # the REQUESTED quantity -- see filled_qty for what actually filled
    client_id:     str
    binding_id:    str
    event_id:      str
    paper_mode:    bool = True
    symbol:        str = ""
    timestamp:     datetime = field(default_factory=lambda: datetime.now(IST))
    entry_aborted:  bool = False   # BUY failed to route -- book must discard the optimistic hedge leg
    routing_failed: bool = False
    exit_failed:    bool = False   # SELL (close) failed to route -- book must leave the hedge leg as-is
    filled_qty:     int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
