"""
strategies/liquidity_sweep/events.py — order/fill event dataclasses for the
Liquidity Sweep strategy.

Written fresh, not subclassing execution_bridge.option_buyer_bridge_base's
OptionBuyerFillEvent -- that base class is the shared foundation D1Trap/FVG
already build on, and this strategy shares zero runtime code with any other
strategy, same standalone mandate as strategies/oi_flow/ (see
strategies/liquidity_sweep/__init__.py). The field shape below mirrors
strategies/oi_flow/events.py's own proven confirm-then-finalize contract --
deliberate, since it's a genuinely good, battle-tested shape -- the class
itself is independently defined, not imported.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from config.global_config import IST


@dataclass
class LiquiditySweepOrderEvent:
    """Published to Topic.LIQUIDITY_SWEEP_ORDER_REQUEST. action="BUY" opens
    a position (sweep+displacement+FVG+retest entry); action="SELL" closes
    it (SL/Target1/Target2/EOD -- whatever the engine decided)."""
    client_id:    str
    binding_id:   str
    action:       str            # "BUY" | "SELL"
    underlying:   str
    option_type:  str            # "CE" | "PE"
    strike:       int
    expiry:       date
    quantity:     int
    entry_price:  float          # BUY: intended entry premium. SELL: the position's original entry (for P&L).
    # sl_price/target1_price/target2_price are SPOT-INDEX levels, not option
    # premium -- deliberate design choice (see engine.py's module docstring):
    # the validated Pine script's whole SL/T1/T2 pipeline is spot-based
    # (sweep-candle extreme, R-multiples off spot risk, opposing spot
    # liquidity), and translating that into a premium-terms SL would need a
    # live delta/greeks model this codebase doesn't have -- inventing one
    # under time pressure would be new, unvalidated logic layered on top of
    # an already-unvalidated (no backtest possible for real option premium)
    # strategy. Kept honestly as spot levels; engine.py's own exit checks
    # compare live SPOT ticks against these fields directly, and the
    # option's own live LTP is simply the fill price whenever a spot-level
    # exit condition fires -- not itself a premium-based SL/target.
    sl_price:     float
    target1_price: float
    target2_price: float
    reason:       str
    event_id:     str
    exit_price:   float = 0.0    # SELL only -- intended exit premium
    entry_ts:     Optional[datetime] = None
    product_type: str = "MIS"
    strategy:     str = "liquidity_sweep"   # used by strategies.core.gate.can_trade()'s ENTRY gate


@dataclass
class LiquiditySweepFillEvent:
    """Published to Topic.LIQUIDITY_SWEEP_ORDER_FILL after every order
    attempt (paper and live, success and failure) -- same confirm-then-
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
    # True when a live BUY failed to route (no route/no broker/gate closed) --
    # the engine must discard its optimistic position, never manage a phantom one.
    entry_aborted:  bool = False
    routing_failed: bool = False
    # True when a live SELL (exit) could not be routed -- the engine must
    # leave the position exactly as it was (still open, still persisted).
    exit_failed: bool = False
    # The ACTUAL filled quantity, as reported by the broker (OrderFill.qty)
    # -- may be LESS than `qty` on a partial fill. Defaults to `qty` for
    # paper/paper_route's simulated fills (always "full" by construction).
    filled_qty: int = 0

    def __post_init__(self) -> None:
        if self.filled_qty <= 0 and not self.entry_aborted and not self.exit_failed:
            self.filled_qty = self.qty
