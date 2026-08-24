"""
strategies/oi_orb_screener/events.py -- order/fill event dataclasses for the
OI-Spurt + ORB screener strategy.

Written fresh, not subclassing any other strategy's event base -- this
strategy shares zero runtime code with any other strategy in this codebase
(see strategies/oi_orb_screener/__init__.py). Field shape deliberately
mirrors the proven confirm-then-finalize contract already used elsewhere
(strategies/oi_flow/events.py, strategies/liquidity_sweep/events.py,
strategies/liquidity_trap/events.py) -- that similarity is a good, battle-
tested shape worth repeating, not a sign of a shared class.

`underlying` here is the STOCK symbol (e.g. "MANAPPURAM"), not an index --
this is the first strategy in this codebase's live pipeline to trade
individual F&O stocks chosen dynamically each day.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class OiOrbOrderEvent:
    """Published to Topic.OI_ORB_ORDER_REQUEST. action="BUY" opens a
    position (ORB breakout entry); action="SELL" closes it (EOD square-off
    this pass -- no SL/target logic yet, see engine.py's own docstring)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str            # stock symbol, e.g. "MANAPPURAM"
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: the position's original entry (for P&L).
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    entry_ts:     Optional[datetime] = None
    product_type: str = "MIS"
    strategy:     str = "oi_orb_screener"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class OiOrbFillEvent:
    """Published to Topic.OI_ORB_ORDER_FILL after every order attempt
    (paper_route and live, success and failure) -- same confirm-then-
    finalize contract this codebase always uses: a routing failure or an
    unconfirmed live fill must never be silently treated as a real fill by
    the owning engine."""
    action:        str
    underlying:    str
    option_type:   str
    strike:        int
    fill_price:    float
    qty:           int          # the REQUESTED quantity (ev.quantity) -- see filled_qty for what actually filled
    client_id:     str
    binding_id:    str
    event_id:      str
    paper_mode:    bool = True
    symbol:        str = ""
    timestamp:     datetime = field(default_factory=lambda: datetime.now(IST))
    entry_aborted:  bool = False
    routing_failed: bool = False
    exit_failed: bool = False
    filled_qty: int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
