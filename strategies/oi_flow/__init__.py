"""strategies/oi_flow — OI-Flow Pre-Breakout strategy (option buyer).

Fully standalone: its own events, its own execution bridge, its own book
manager, its own Topics -- deliberately shares NO runtime infrastructure
with any other strategy (SellStraddle / D1 Trap / FVG). See
C:\\Users\\SERVER\\.claude\\plans\\curried-snuggling-sunrise.md for the full
design plan and rationale.

Cannot be backtested against history (Upstox's intraday historical-candle
API has no OI field) -- validated forward, in paper mode, via structured
telemetry instead. Status: Phases 1-4 built (tracker, detector, bridge,
engine/book manager); Phase 5 (telemetry + paper deployment) pending.
"""
from strategies.oi_flow.book_manager import OIFlowBookManager
from strategies.oi_flow.engine import OIFlowStrategy

__all__ = ["OIFlowStrategy", "OIFlowBookManager"]
