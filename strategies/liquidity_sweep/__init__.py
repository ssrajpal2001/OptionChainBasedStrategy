"""strategies/liquidity_sweep — Liquidity Sweep strategy (option buyer).

Fully standalone: its own events, its own execution bridge, its own book
manager, its own Topics -- deliberately shares NO runtime infrastructure
with any other strategy (SellStraddle / D1 Trap / FVG / OI-Flow), same
mandate as strategies/oi_flow/.

Sweep -> HTF/structure bias -> displacement -> FVG -> retest entry, a
direct Python port of pinescript/liquidity_sweep_indicator_with_risk.pine
after it was iteratively built and validated against real NIFTY chart data
in TradingView (not backtested in Python first, per direct user
instruction). See CLAUDE.md's "Liquidity Sweep Strategy" section for the
full tuning history and every parameter default's provenance.

Intraday only, same-day EOD squareoff -- no overnight carry.
"""
from strategies.liquidity_sweep.book_manager import LiquiditySweepBookManager
from strategies.liquidity_sweep.engine import LiquiditySweepStrategy

__all__ = ["LiquiditySweepStrategy", "LiquiditySweepBookManager"]
