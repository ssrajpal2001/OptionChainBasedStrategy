"""
strategies/cag_straddle/events.py — order/fill event dataclasses for the
CAG Long Straddle strategy.

Written fresh -- zero shared runtime code with any other strategy, same
standalone mandate as strategies/oi_flow/, strategies/liquidity_sweep/,
strategies/liquidity_trap/. SL here is an OPTION-PREMIUM level (NOT a
spot-index level, unlike liquidity_sweep/liquidity_trap's own deliberate
spot-based design) -- this strategy's whole S&R read runs directly on each
side's own option premium chart (see engine.py's own module docstring), so
no spot-to-premium translation is needed or wanted. No fixed target_price
field -- the mechanic has no fixed take-profit, only the trailing S1 SL and
the 15:35 EOD close.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class CagStraddleOrderEvent:
    """Published to Topic.CAG_STRADDLE_ORDER_REQUEST. action="BUY" opens
    the (single) position; action="SELL" closes it (SL/EOD/hard-risk-cap)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: original entry (for P&L).
    sl_price:     float          # OPTION-PREMIUM level (see module docstring)
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    entry_ts:     Optional[datetime] = None
    product_type: str = "MIS"
    strategy:     str = "cag_straddle"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class CagStraddleFillEvent:
    """Published to Topic.CAG_STRADDLE_ORDER_FILL after every order attempt
    (paper and live, success and failure) -- same confirm-then-finalize
    contract this codebase always uses."""
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
    entry_aborted:  bool = False   # BUY failed to route -- engine must discard the optimistic position
    routing_failed: bool = False
    exit_failed:    bool = False   # SELL failed to route -- engine must leave the position as-is
    filled_qty:     int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
