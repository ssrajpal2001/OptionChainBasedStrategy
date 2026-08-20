"""strategies/liquidity_trap — Liquidity Trap strategy (option buyer).

Fully standalone: its own events, its own execution bridge, its own book
manager, its own Topics -- deliberately shares NO runtime infrastructure
with any other strategy in this codebase, same mandate as strategies/
oi_flow/ and strategies/liquidity_sweep/.

15m ref-candle-rolling bias lock -> 15m SL-watch (ref candle's own opposite
level swept) -> 5m single-fixed-reference confirmation -> 1m CHoCH entry
(half size, 2 lots) -> 1:2 risk-reward SL/target off the 5m sweep extreme ->
bear-trap/bull-trap 3-candle (ref/sweep/reclaim) scale-in zone on 1m, adding
the other half (2 more lots, 4 total) on a retrace into the lowest third
(long) / highest third (short) of that zone -- SL/target unchanged by the
add-on.

Built and validated as a real-data backtest against SENSEX spot (1 year,
Upstox) BEFORE being wired up as a live strategy, per this codebase's
established discipline: scripts/liquidity_trap_backtest.py is the source of
truth this engine is a direct, faithful live port of -- not a
reimplementation that can drift from what was actually validated. See
CLAUDE.md's "Liquidity Trap" section for the full validated mechanic,
parameter provenance, and real backtest numbers (68 trades over 1yr,
win 52.9%, PF 1.95, lot-weighted).

SL/Target1/Target2 are SPOT-INDEX levels, not option premium -- same
deliberate, honestly-flagged design choice as strategies/liquidity_sweep/
(see that package's own engine.py docstring for the full rationale): no
validated delta/greeks model exists in this codebase to translate a spot
SL into a premium SL. The option's own live LTP is simply the fill price
whenever a spot-level entry/exit condition fires.

Intraday only, same-day EOD squareoff -- no overnight carry.
"""
from strategies.liquidity_trap.book_manager import LiquidityTrapBookManager
from strategies.liquidity_trap.engine import LiquidityTrapStrategy

__all__ = ["LiquidityTrapStrategy", "LiquidityTrapBookManager"]
