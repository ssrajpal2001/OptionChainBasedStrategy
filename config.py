"""
config.py — Central configuration registry for the Multi-Index Options Confluence Bot.

All tunable parameters live here. Strategy modules, data providers, and the execution
engine import from this module. No magic numbers outside this file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional


# ---------------------------------------------------------------------------
# Broker / API Credentials
# ---------------------------------------------------------------------------

@dataclass
class BrokerCredentials:
    provider: Literal["shoonya", "angelone", "dhan", "fyers", "mock"]
    user_id: str = ""
    password: str = ""
    api_key: str = ""
    api_secret: str = ""
    totp_secret: str = ""          # For TOTP-based 2FA (Angel One, Fyers)
    vendor_code: str = ""          # Shoonya-specific
    imei: str = ""                 # Shoonya-specific
    access_token: str = ""         # Pre-fetched token (optional override)

    @classmethod
    def from_env(cls, provider: str = "mock") -> "BrokerCredentials":
        """Load credentials from environment variables."""
        return cls(
            provider=provider,  # type: ignore[arg-type]
            user_id=os.getenv("BROKER_USER_ID", ""),
            password=os.getenv("BROKER_PASSWORD", ""),
            api_key=os.getenv("BROKER_API_KEY", ""),
            api_secret=os.getenv("BROKER_API_SECRET", ""),
            totp_secret=os.getenv("BROKER_TOTP_SECRET", ""),
            vendor_code=os.getenv("BROKER_VENDOR_CODE", ""),
            imei=os.getenv("BROKER_IMEI", ""),
            access_token=os.getenv("BROKER_ACCESS_TOKEN", ""),
        )


# ---------------------------------------------------------------------------
# Asset & Contract Configuration
# ---------------------------------------------------------------------------

@dataclass
class AssetConfig:
    indices: List[str] = field(
        default_factory=lambda: ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"]
    )
    active_index: str = "NIFTY"

    # Expiry selection: CURRENT_WEEK | NEXT_WEEK | MONTHLY
    expiry_preference: Literal["CURRENT_WEEK", "NEXT_WEEK", "MONTHLY"] = "CURRENT_WEEK"

    # Which moneyness to execute on: ATM | ITM_1 | OTM_1
    moneyness_execution: Literal["ATM", "ITM_1", "OTM_1"] = "ATM"

    # Number of strikes to stream on each side of ATM
    otm_depth: int = 10

    # Candle timeframes to maintain (minutes)
    candle_timeframes: List[int] = field(default_factory=lambda: [5, 15, 75])

    # Index lot sizes (standard NSE values)
    lot_sizes: Dict[str, int] = field(default_factory=lambda: {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
        "SENSEX": 20,
        "MIDCPNIFTY": 120,
    })

    # Underlying futures token map (populated at runtime from instrument master)
    futures_tokens: Dict[str, str] = field(default_factory=dict)

    # Index spot token map (populated at runtime)
    spot_tokens: Dict[str, str] = field(default_factory=dict)

    @property
    def lot_size(self) -> int:
        return self.lot_sizes.get(self.active_index, 25)


# ---------------------------------------------------------------------------
# Technical Indicator Parameters
# ---------------------------------------------------------------------------

@dataclass
class IndicatorConfig:
    rsi_period: int = 14
    vwap_period: int = 500         # Rolling bars for VWAP calculation
    adx_period: int = 20
    ema_fast: int = 9
    ema_slow: int = 21
    atr_period: int = 14
    volume_ma_period: int = 20     # For volume spike detection
    pcr_smoothing: int = 5         # Rolling average period for PCR


# ---------------------------------------------------------------------------
# Risk Management Parameters
# ---------------------------------------------------------------------------

@dataclass
class RiskConfig:
    min_risk_reward_ratio: float = 2.0    # Hard floor: never take < 1:2 RR
    max_risk_per_trade_percent: float = 1.0
    max_open_positions: int = 1
    max_daily_loss_percent: float = 3.0   # Halt trading if daily loss > 3%
    max_daily_trades: int = 5
    trailing_sl_activation_rr: float = 1.0  # Activate trailing SL after 1R profit
    trailing_sl_distance_atr: float = 1.5   # Trail by 1.5× ATR
    capital: float = 500_000.0              # Starting capital in INR
    margin_utilization_limit: float = 0.80  # Use max 80% available margin


# ---------------------------------------------------------------------------
# Strategy Toggle & Tuning
# ---------------------------------------------------------------------------

@dataclass
class StrategyConfig:
    # Enable/disable individual strategies
    strategy_a_enabled: bool = True    # OI Zone Breakout/Rejection
    strategy_b_enabled: bool = True    # Liquidity Trap Engine
    strategy_c_enabled: bool = True    # Panic Selling Scanner

    # Strategy A parameters
    oi_zone_min_delta_oi: float = 50_000    # Min ΔOI for a zone to qualify
    breakout_candle_body_ratio: float = 0.6  # Candle body / total range ≥ 60%
    rsi_overbought: float = 65.0
    rsi_oversold: float = 35.0

    # Strategy B parameters
    trap_oi_spike_multiplier: float = 2.0    # ΔOI must be 2× rolling average
    trap_volume_spike_multiplier: float = 2.5
    trap_stall_candles: int = 2              # Bars price must stall after breakout
    reversal_confirmation_pct: float = 0.3   # Price must reclaim level by 0.3%

    # Strategy C parameters
    panic_consecutive_red_candles: int = 3
    panic_put_oi_surge_multiplier: float = 3.0
    panic_pcr_drop_threshold: float = 0.3    # PCR must drop by 0.3+ to signal panic
    put_unwinding_delta_threshold: float = -30_000  # Negative ΔPE confirms unwind


# ---------------------------------------------------------------------------
# Backtesting Parameters
# ---------------------------------------------------------------------------

@dataclass
class BacktestConfig:
    data_dir: str = "data/historical"
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    candle_interval: int = 5          # Minutes
    slippage_pct: float = 0.05        # 0.05% per side

    # Indian statutory transaction costs (as % of turnover)
    stt_pct: float = 0.0625           # Securities Transaction Tax on sell side (options)
    exchange_charge_pct: float = 0.035
    sebi_fee_pct: float = 0.0001
    gst_pct: float = 18.0             # On (brokerage + exchange charges)
    brokerage_per_order: float = 20.0  # Flat ₹20 per order (Zerodha-style)


# ---------------------------------------------------------------------------
# Logging & Alerting
# ---------------------------------------------------------------------------

@dataclass
class LogConfig:
    log_level: str = "INFO"
    log_to_file: bool = True
    log_file_path: str = "logs/bot.log"
    log_rotation_mb: int = 50
    telegram_alerts: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    discord_webhook_url: str = ""


# ---------------------------------------------------------------------------
# Trading Mode Flags
# ---------------------------------------------------------------------------

@dataclass
class TradingModeConfig:
    live_trading: bool = False
    paper_trading: bool = True
    backtest: bool = False


# ---------------------------------------------------------------------------
# Master Configuration — Single Source of Truth
# ---------------------------------------------------------------------------

@dataclass
class SystemConfig:
    trading_modes: TradingModeConfig = field(default_factory=TradingModeConfig)
    broker: BrokerCredentials = field(default_factory=lambda: BrokerCredentials.from_env("mock"))
    assets: AssetConfig = field(default_factory=AssetConfig)
    indicators: IndicatorConfig = field(default_factory=IndicatorConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    logging: LogConfig = field(default_factory=LogConfig)

    def validate(self) -> None:
        """Sanity-check the configuration before engine startup."""
        active_modes = sum([
            self.trading_modes.live_trading,
            self.trading_modes.paper_trading,
            self.trading_modes.backtest,
        ])
        if active_modes == 0:
            raise ValueError("At least one trading mode must be enabled.")
        if self.trading_modes.live_trading and self.trading_modes.paper_trading:
            raise ValueError("Cannot run live_trading and paper_trading simultaneously.")
        if self.assets.active_index not in self.assets.indices:
            raise ValueError(f"active_index '{self.assets.active_index}' not in indices list.")
        if self.risk.min_risk_reward_ratio < 1.0:
            raise ValueError("min_risk_reward_ratio must be ≥ 1.0.")
        if not (0 < self.risk.max_risk_per_trade_percent <= 5):
            raise ValueError("max_risk_per_trade_percent should be between 0 and 5.")

    @classmethod
    def default(cls) -> "SystemConfig":
        """Factory method returning a ready-to-use paper-trading config."""
        cfg = cls()
        cfg.validate()
        return cfg

    @classmethod
    def for_backtest(
        cls,
        start_date: str = "2024-01-01",
        end_date: str = "2024-12-31",
        active_index: str = "NIFTY",
    ) -> "SystemConfig":
        cfg = cls()
        cfg.trading_modes.live_trading = False
        cfg.trading_modes.paper_trading = False
        cfg.trading_modes.backtest = True
        cfg.assets.active_index = active_index
        cfg.backtest.start_date = start_date
        cfg.backtest.end_date = end_date
        cfg.validate()
        return cfg


# Module-level default — importable directly by other modules.
CONFIG: SystemConfig = SystemConfig.default()
