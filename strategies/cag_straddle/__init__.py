"""
strategies/cag_straddle/ — CAG Long Straddle strategy (2026-08-27).

8th standalone strategy in this codebase, an explicit exception to
CLAUDE.md's prior 7-strategy cap (same precedent as OI-ORB Screener's own
addition). Fully standalone -- own Topics (CAG_STRADDLE_ORDER_REQUEST/
FILL), own events, own execution bridge, own book manager -- shares zero
runtime infrastructure with any other strategy in this codebase, same
mandate as OI-Flow/Liquidity Sweep/Liquidity Trap/OI-ORB Screener.

Built from a real-data-validated backtest --
scripts/nifty_1500_sr_breakout_backtest.py -- refined through several
rounds of direct user review against real minute-by-minute NIFTY option
premium charts before being ported into this live engine. See
strategies/cag_straddle/detector.py and engine.py's own module docstrings
for the full mechanic.
"""
from strategies.cag_straddle.book_manager import CagStraddleBookManager
from strategies.cag_straddle.engine import CagStraddleStrategy

__all__ = ["CagStraddleStrategy", "CagStraddleBookManager"]
