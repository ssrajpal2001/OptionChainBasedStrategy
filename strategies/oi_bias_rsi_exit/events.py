"""
strategies/oi_bias_rsi_exit/events.py — order/fill event dataclasses for the
OI-spurt selection + combined-OI bias + StochRSI entry/exit strategy.

Written fresh -- zero shared runtime code with any other strategy, same
standalone mandate as every other strategy package in this codebase.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class OiBiasRsiExitOrderEvent:
    """Published to Topic.OI_BIAS_RSI_EXIT_ORDER_REQUEST. action="BUY" opens
    a position on one shortlisted stock; action="SELL" closes it."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str            # the real stock symbol (e.g. "TCS")
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float
    reason:       str
    event_id:     str
    exit_price:   float = 0.0
    entry_ts:     Optional[datetime] = None
    product_type: str = "NRML"
    strategy:     str = "oi_bias_rsi_exit"


@dataclass
class OiBiasRsiExitFillEvent:
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
    entry_aborted:  bool = False
    routing_failed: bool = False
    exit_failed:    bool = False
    filled_qty:     int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
