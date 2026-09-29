"""
strategies/oi_bias_rsi_exit/ — OI-spurt selection + combined-OI bias +
StochRSI entry/exit strategy (built 2026-09-26/29).

Fully standalone -- own Topics (OI_BIAS_RSI_EXIT_ORDER_REQUEST/FILL), own
events, own execution bridge, own book manager -- shares zero runtime
infrastructure with any other strategy in this codebase, same mandate as
OI-ORB Screener/CAG Straddle/Iron Fly. Built from a real-data backtest +
parameter sweep across 20 real trading days (scripts/oi_bias_rsi_exit_
backtest.py / _optimize.py) before this live engine. See engine.py's own
module docstring for the full mechanic and the deliberate REST-poll (not
WS-tick) design choice for this first live cut.
"""
from strategies.oi_bias_rsi_exit.book_manager import OiBiasRsiExitBookManager
from strategies.oi_bias_rsi_exit.engine import OiBiasRsiExitStrategy

__all__ = ["OiBiasRsiExitStrategy", "OiBiasRsiExitBookManager"]
