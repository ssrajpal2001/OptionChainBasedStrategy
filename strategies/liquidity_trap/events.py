"""
strategies/liquidity_trap/events.py — order/fill event dataclasses for the
Liquidity Trap strategy.

Written fresh, not subclassing execution_bridge.option_buyer_bridge_base's
OptionBuyerFillEvent -- zero shared runtime code with any other strategy,
same standalone mandate as strategies/oi_flow/ and strategies/liquidity_
sweep/. The field shape mirrors those two strategies' own proven confirm-
then-finalize contract (deliberate, battle-tested shape) -- independently
defined here, not imported.

A scale-in add-on is just a second BUY of the SAME strike/expiry (more
quantity, same instrument) -- no new action type needed beyond "BUY"/"SELL".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class LiquidityTrapOrderEvent:
    """Published to Topic.LIQUIDITY_TRAP_ORDER_REQUEST. action="BUY" opens or
    adds to a position (CHoCH entry, half size; or the bear/bull-trap scale-in
    add-on, same strike/expiry, other half); action="SELL" closes it
    (SL/Target/EOD)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: original entry (for P&L).
    # sl_price/target_price are SPOT-INDEX levels, not option premium --
    # deliberate design choice mirroring strategies/liquidity_sweep/'s own
    # (see that package's engine.py docstring for the full rationale): no
    # validated delta/greeks model exists in this codebase to translate a
    # spot SL into a premium SL.
    sl_price:     float
    target_price: float
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    is_add_on:    bool = False   # True for the scale-in BUY (vs the initial CHoCH entry)
    entry_ts:     Optional[datetime] = None
    product_type: str = "MIS"
    strategy:     str = "liquidity_trap"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class LiquidityTrapFillEvent:
    """Published to Topic.LIQUIDITY_TRAP_ORDER_FILL after every order attempt
    (paper and live, success and failure) -- same confirm-then-finalize
    contract this codebase always uses: a routing failure or an unconfirmed
    live fill must never be silently treated as a real fill by the owning
    engine."""
    action:        str
    underlying:    str
    option_type:   str
    strike:        int
    fill_price:    float
    qty:           int          # the REQUESTED quantity -- see filled_qty for what actually filled
    client_id:     str
    binding_id:    str
    event_id:      str
    is_add_on:     bool = False
    paper_mode:    bool = True
    symbol:        str = ""
    timestamp:     datetime = field(default_factory=lambda: datetime.now(IST))
    entry_aborted:  bool = False   # BUY failed to route -- engine must discard the optimistic leg
    routing_failed: bool = False
    exit_failed:    bool = False   # SELL failed to route -- engine must leave the position as-is
    filled_qty:     int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
