"""
matrix_engine.indicators — modular indicator package.

One file per indicator, each independently verifiable against the
Option_Selling_May_2026 reference. VWAP is sourced from broker ATP
(see vwap.py), not computed.
"""
from matrix_engine.indicators.constants import RSI_PERIOD, VWAP_WINDOW, ADX_PERIOD
from matrix_engine.indicators.rsi import rsi
from matrix_engine.indicators.roc import roc
from matrix_engine.indicators.vwap import vwap, leg_atp, combined_vwap
from matrix_engine.indicators.slope import vwap_slope
from matrix_engine.indicators.adx import adx
from matrix_engine.indicators.ema import ema
from matrix_engine.indicators.atr import atr
from matrix_engine.indicators.volume import volume_spike
from matrix_engine.indicators.snapshot import TechSnapshot

__all__ = [
    "RSI_PERIOD", "VWAP_WINDOW", "ADX_PERIOD",
    "rsi", "roc", "vwap", "leg_atp", "combined_vwap", "vwap_slope",
    "adx", "ema", "atr", "volume_spike", "TechSnapshot",
]
