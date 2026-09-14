"""
strategies/iron_fly/events.py — order/fill event dataclasses for the NIFTY
Weekly Iron Condor -> Iron Fly strategy.

Fully standalone -- zero shared runtime code with any other strategy, same
mandate as strategies/cag_straddle/, strategies/oi_orb_screener/. Unlike
every single-leg strategy's own order event, this one is LEG-CENTRIC: an
Iron Condor/Fly always has 4 independent legs and any single trigger (an
entry, a roll, a fly conversion, a profit-target exit+reinit) can open
and/or close several of them at once, at different premium levels -- so one
IronFlyOrderEvent always represents exactly ONE leg's ONE order (an open OR
a close), never a bundled multi-leg action.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class IronFlyOrderEvent:
    """Published to Topic.IRON_FLY_ORDER_REQUEST.

    `order_side` is the actual broker action ("BUY"/"SELL") and is fully
    determined by (is_open, is_short): opening a short leg = SELL, opening
    a long leg = BUY, closing a short leg = BUY (buy back), closing a long
    leg = SELL. `is_open`/`is_short` are carried separately (not re-derived
    from order_side) so the bridge and dashboard never have to reverse that
    logic themselves.
    """
    client_id:    str
    binding_id:   str
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    order_side:   str            # "BUY" | "SELL" -- the real broker action
    is_open:      bool           # True = opening this leg, False = closing it
    is_short:     bool           # True if this leg is conceptually SHORT (once open)
    price:        float          # intended fill price (the engine's own live-LTP decision)
    reason:       str            # "entry" | "roll_call" | "roll_put" | "fly_conversion" | "profit_target_exit"
    event_id:     str
    entry_price:  float = 0.0    # CLOSE only -- this leg's original entry premium, for P&L/history
    entry_ts:     Optional[datetime] = None
    product_type: str = "NRML"   # carries overnight -- NRML, not MIS, per the approved plan
    strategy:     str = "iron_fly"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class IronFlyFillEvent:
    """Published to Topic.IRON_FLY_ORDER_FILL after every order attempt
    (paper and live, success and failure) -- same confirm-then-finalize
    contract this codebase always uses."""
    is_open:       bool
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
    aborted:        bool = False   # order failed to route at all -- engine must discard the optimistic leg
    routing_failed: bool = False
    filled_qty:     int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.aborted:
            self.filled_qty = self.qty
