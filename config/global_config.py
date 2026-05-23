"""
config/global_config.py — System-wide constants and non-tenant configuration.

All times are pinned to Asia/Kolkata (IST, UTC+5:30).
No magic numbers anywhere else in the codebase — every tunable
parameter is expressed as a typed field here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import time
from typing import Dict, List, Literal
from zoneinfo import ZoneInfo

# ─────────────────────────────────────────────────────────────────────────────
# Timezone — single source of truth for the entire system
# ─────────────────────────────────────────────────────────────────────────────

IST = ZoneInfo("Asia/Kolkata")


# ─────────────────────────────────────────────────────────────────────────────
# Exchange / Session Config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ExchangeConfig:
    """NSE/BSE session boundaries, all times in IST."""
    pre_open_connect: time = time(9, 0, 0)       # Connect WS, download masters
    market_open: time = time(9, 15, 0)           # Begin processing + recording
    market_close: time = time(15, 30, 0)         # Halt processing, flush all buffers
    eod_cleanup: time = time(15, 45, 0)          # Rotate log/parquet files

    # Strike granularity per index (points)
    strike_steps: Dict[str, float] = field(default_factory=lambda: {
        "NIFTY": 50.0,
        "BANKNIFTY": 100.0,
        "FINNIFTY": 50.0,
        "SENSEX": 100.0,
        "MIDCPNIFTY": 50.0,
    })

    # Standard lot sizes (NSE/BSE current values)
    lot_sizes: Dict[str, int] = field(default_factory=lambda: {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
        "SENSEX": 20,
        "MIDCPNIFTY": 120,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Indicator Params — hard-pinned per specification
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class IndicatorParams:
    rsi_period: int = 14        # Strictly 14 candles
    vwap_window: int = 500      # Strictly 500 candles
    adx_period: int = 20        # Strictly 20 candles
    ema_fast: int = 9
    ema_slow: int = 21
    atr_period: int = 14
    volume_ma_period: int = 20


# ─────────────────────────────────────────────────────────────────────────────
# Storage Config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StorageConfig:
    root_dir: str = "data"
    recorded_dir: str = "data/recorded"      # Live tick recordings
    backtest_dir: str = "data/backtest"      # Processed backtest datasets
    log_dir: str = "logs"
    log_level: str = "INFO"
    log_rotation_mb: int = 50

    # Tick recorder settings
    recorder_flush_interval_seconds: int = 5     # Flush parquet every N seconds
    recorder_compression: str = "zstd"           # zstd or snappy or none
    recorder_row_group_size: int = 50_000        # Parquet row group size

    def ensure_dirs(self) -> None:
        for d in (self.recorded_dir, self.backtest_dir, self.log_dir):
            os.makedirs(d, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Event Bus Topic Registry
# ─────────────────────────────────────────────────────────────────────────────

class Topic:
    """String constants for all pub-sub topics. Never use bare strings."""
    INDEX_TICK       = "index_tick"
    OPTION_TICK      = "option_tick"
    CANDLE_CLOSE     = "candle_close"
    MATRIX_SNAPSHOT  = "matrix_snapshot"
    SIGNAL           = "signal"
    ORDER_REQUEST    = "order_request"
    ORDER_FILL       = "order_fill"
    SYSTEM_EVENT     = "system_event"


# ─────────────────────────────────────────────────────────────────────────────
# System Event Codes
# ─────────────────────────────────────────────────────────────────────────────

class SysEvent:
    PRE_OPEN        = "PRE_OPEN"
    MARKET_OPEN     = "MARKET_OPEN"
    MARKET_CLOSE    = "MARKET_CLOSE"
    KILL_SWITCH     = "KILL_SWITCH"
    FEEDER_DOWN     = "FEEDER_DOWN"
    FEEDER_RESTORED = "FEEDER_RESTORED"
    DAILY_RESET     = "DAILY_RESET"


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Tuning (global defaults — overridable per client)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StrategyParams:
    # Strategy A — OI Zone Breakout/Rejection
    oi_zone_min_delta_oi: float = 50_000
    breakout_body_ratio: float = 0.60
    rsi_overbought: float = 65.0
    rsi_oversold: float = 35.0
    zone_proximity_strikes: float = 1.5    # ATM ± N × step = zone radius

    # Strategy B — Liquidity Trap / Rolling Base
    trap_oi_spike_mult: float = 2.0
    trap_vol_spike_mult: float = 2.5
    trap_stall_candles: int = 2
    trap_reversal_pct: float = 0.30
    void_atr_mult: float = 2.0             # Void state if price runs 2× ATR past level
    void_lift_retest_tolerance: float = 0.10  # % tolerance for void-lift retest

    # Strategy C — Panic Selling
    panic_red_candles: int = 3
    panic_put_oi_mult: float = 3.0
    panic_pcr_drop: float = 0.30
    panic_unwind_delta_threshold: float = -30_000

    # Global confluence gate
    min_risk_reward: float = 2.0
    min_confidence: float = 0.50


# ─────────────────────────────────────────────────────────────────────────────
# Global System Config (single root object)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class GlobalConfig:
    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    indicators: IndicatorParams = field(default_factory=IndicatorParams)
    storage: StorageConfig = field(default_factory=StorageConfig)
    strategy: StrategyParams = field(default_factory=StrategyParams)

    # Active indices to monitor (feeder subscribes to all)
    monitored_indices: List[str] = field(
        default_factory=lambda: ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"]
    )
    active_index: str = "NIFTY"

    # OTM/ITM depth for chain subscription
    chain_depth: int = 10          # ATM ± 10 strikes

    # Candle timeframes (minutes)
    candle_timeframes: List[int] = field(default_factory=lambda: [5, 15, 75])

    # Primary feeder broker (admin-selected)
    primary_feeder_provider: Literal["shoonya", "dhan", "fyers", "angelone", "mock"] = "mock"

    def validate(self) -> None:
        if self.active_index not in self.monitored_indices:
            raise ValueError(f"active_index '{self.active_index}' not in monitored_indices.")
        if self.chain_depth < 1:
            raise ValueError("chain_depth must be ≥ 1.")
        self.storage.ensure_dirs()

    @classmethod
    def default(cls) -> "GlobalConfig":
        cfg = cls()
        cfg.validate()
        return cfg


# Module-level singleton — import `GLOBAL_CFG` directly where needed
GLOBAL_CFG: GlobalConfig = GlobalConfig.default()
