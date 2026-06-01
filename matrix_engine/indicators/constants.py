"""
matrix_engine/indicators/constants.py — hard-pinned indicator periods.

RSI_PERIOD  = 14   (Wilder's 14-period RSI)
VWAP_WINDOW = 500  (rolling VWAP window; for an intraday 1-minute series this
                    exceeds a full session so it behaves as cumulative-intraday
                    VWAP, matching the Option_Selling_May_2026 reference)
ADX_PERIOD  = 20   (20-period ADX + DI)
"""

from __future__ import annotations

RSI_PERIOD:  int = 14
VWAP_WINDOW: int = 500
ADX_PERIOD:  int = 20
