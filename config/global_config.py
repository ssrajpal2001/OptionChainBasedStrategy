"""
config/global_config.py — System-wide constants and non-tenant configuration.

All times are pinned to Asia/Kolkata (IST, UTC+5:30).
No magic numbers anywhere else in the codebase — every tunable
parameter is expressed as a typed field here.
"""

from __future__ import annotations

import os
import threading as _threading
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

    # MCX (commodity) session — open well past the NSE/BSE close.
    # Used for CRUDEOIL etc. so the after-hours test (and live commodity trading)
    # is not force-squared-off at 15:30.
    mcx_market_open: time = time(9, 0, 0)
    mcx_market_close: time = time(23, 30, 0)     # MCX evening session close (~23:30 IST)

    # Underlyings that trade on MCX (commodity segment, futures-driven ATM)
    mcx_underlyings: tuple = ("CRUDEOIL", "CRUDEOILM", "NATURALGAS", "GOLD", "SILVER")

    # Strike granularity per index (points)
    strike_steps: Dict[str, float] = field(default_factory=lambda: {
        "NIFTY": 50.0,
        "BANKNIFTY": 100.0,
        "FINNIFTY": 50.0,
        "SENSEX": 100.0,
        "MIDCPNIFTY": 50.0,
        # MCX commodities
        "CRUDEOIL": 50.0,        # crude option strikes are 50 apart
        "CRUDEOILM": 50.0,
        "NATURALGAS": 5.0,
    })

    # Standard lot sizes (NSE/BSE current values + MCX commodity lots)
    lot_sizes: Dict[str, int] = field(default_factory=lambda: {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 60,
        "SENSEX": 20,
        "MIDCPNIFTY": 120,
        # MCX commodities (verify against current contract spec before live)
        "CRUDEOIL": 100,        # 100 barrels
        "CRUDEOILM": 10,        # mini = 10 barrels
        "NATURALGAS": 1250,
    })

    def is_mcx(self, underlying: str) -> bool:
        """True if this underlying trades on MCX (commodity session + segment)."""
        return underlying.upper() in self.mcx_underlyings

    def session_close(self, underlying: str) -> time:
        """Return the force-exit/close time appropriate for this underlying's exchange."""
        return self.mcx_market_close if self.is_mcx(underlying) else self.market_close


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
    IC_ORDER_REQUEST = "ic_order_request"
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
    GAP_OPEN        = "GAP_OPEN"        # >1% drift detected at market open
    POSITION_CLOSED = "POSITION_CLOSED" # Explicit close notification from position manager


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
# TrapEngine Config — runtime-tunable params for the MTF TrapTradingEngine
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TrapEngineConfig:
    """
    Runtime-tunable parameters for the MTF TrapTradingEngine.

    All reads and writes are serialised through _lock (threading.RLock) so that
    async engine workers and the Admin UI/REST thread can operate concurrently
    without torn reads or partial updates.

    LOT_SIZE is immutable — set by NSE exchange contract specification.
    bars_lookback_days is used by the warm-start loader.
    """

    # --- mutable fields (protected by _lock) ---
    _HTF_MINUTES:         int   = field(default=75,          init=False, repr=False)
    _MTF_MINUTES:         int   = field(default=5,           init=False, repr=False)
    _LTF_MINUTES:         int   = field(default=1,           init=False, repr=False)
    _RETEST_ZONE_PERCENT: float = field(default=0.5,         init=False, repr=False)
    _SLIPPAGE_BUFFER:     float = field(default=0.5,         init=False, repr=False)
    _bars_lookback_days:  int   = field(default=5,           init=False, repr=False)
    _SL_MODE:             str   = field(default="dynamic",   init=False, repr=False)  # "dynamic" | "structural"
    _SL_PCT:              float = field(default=2.0,         init=False, repr=False)  # % below entry (dynamic mode)
    _SL_BUFFER_PCT:       float = field(default=0.3,         init=False, repr=False)  # % below structural level (buffer so 1m wick doesn't exit)
    _ENTRY_CUTOFF_TIME:   str   = field(default="14:45",     init=False, repr=False)  # no new entries after this IST time (HH:MM)

    # --- internal lock ---
    _lock: object = field(default_factory=_threading.RLock, init=False, repr=False, compare=False)

    # ── Thread-safe getters ───────────────────────────────────────────────────

    @property
    def HTF_MINUTES(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return self._HTF_MINUTES

    @property
    def MTF_MINUTES(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return self._MTF_MINUTES

    @property
    def LTF_MINUTES(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return self._LTF_MINUTES

    @property
    def RETEST_ZONE_PERCENT(self) -> float:
        with self._lock:  # type: ignore[attr-defined]
            return self._RETEST_ZONE_PERCENT

    @property
    def SLIPPAGE_BUFFER(self) -> float:
        with self._lock:  # type: ignore[attr-defined]
            return self._SLIPPAGE_BUFFER

    @property
    def bars_lookback_days(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return self._bars_lookback_days

    @property
    def SL_MODE(self) -> str:
        with self._lock:  # type: ignore[attr-defined]
            return self._SL_MODE

    @property
    def SL_PCT(self) -> float:
        with self._lock:  # type: ignore[attr-defined]
            return self._SL_PCT

    @property
    def SL_BUFFER_PCT(self) -> float:
        with self._lock:  # type: ignore[attr-defined]
            return self._SL_BUFFER_PCT

    @property
    def ENTRY_CUTOFF_TIME(self) -> str:
        with self._lock:  # type: ignore[attr-defined]
            return self._ENTRY_CUTOFF_TIME

    # ── Thread-safe setters ───────────────────────────────────────────────────

    @HTF_MINUTES.setter
    def HTF_MINUTES(self, value: int) -> None:
        if value < 1:
            raise ValueError(f"HTF_MINUTES must be >= 1, got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_HTF_MINUTES", int(value))

    @MTF_MINUTES.setter
    def MTF_MINUTES(self, value: int) -> None:
        if value < 1:
            raise ValueError(f"MTF_MINUTES must be >= 1, got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_MTF_MINUTES", int(value))

    @LTF_MINUTES.setter
    def LTF_MINUTES(self, value: int) -> None:
        if value < 1:
            raise ValueError(f"LTF_MINUTES must be >= 1, got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_LTF_MINUTES", int(value))

    @RETEST_ZONE_PERCENT.setter
    def RETEST_ZONE_PERCENT(self, value: float) -> None:
        if not (0.0 < value <= 10.0):
            raise ValueError(f"RETEST_ZONE_PERCENT must be in (0, 10], got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_RETEST_ZONE_PERCENT", float(value))

    @SLIPPAGE_BUFFER.setter
    def SLIPPAGE_BUFFER(self, value: float) -> None:
        if value < 0.0:
            raise ValueError(f"SLIPPAGE_BUFFER must be >= 0, got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_SLIPPAGE_BUFFER", float(value))

    @SL_MODE.setter
    def SL_MODE(self, value: str) -> None:
        if value not in ("dynamic", "structural"):
            raise ValueError(f"SL_MODE must be 'dynamic' or 'structural', got {value!r}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_SL_MODE", value)

    @SL_PCT.setter
    def SL_PCT(self, value: float) -> None:
        if not (0.1 <= value <= 20.0):
            raise ValueError(f"SL_PCT must be in [0.1, 20.0], got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_SL_PCT", float(value))

    @SL_BUFFER_PCT.setter
    def SL_BUFFER_PCT(self, value: float) -> None:
        if not (0.0 <= value <= 5.0):
            raise ValueError(f"SL_BUFFER_PCT must be in [0.0, 5.0], got {value}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_SL_BUFFER_PCT", float(value))

    @ENTRY_CUTOFF_TIME.setter
    def ENTRY_CUTOFF_TIME(self, value: str) -> None:
        try:
            time.fromisoformat(value if len(value) == 8 else value + ":00")
        except ValueError:
            raise ValueError(f"ENTRY_CUTOFF_TIME must be HH:MM or HH:MM:SS, got {value!r}")
        with self._lock:  # type: ignore[attr-defined]
            object.__setattr__(self, "_ENTRY_CUTOFF_TIME", value)

    # ── Atomic bulk update (used by Admin REST endpoint) ─────────────────────

    def reconfigure(self, **kwargs) -> dict:
        """
        Atomically update one or more mutable fields in a single lock acquisition.
        Validates all values before applying any — either all succeed or none apply.
        Returns a dict of the updated field names and their new values.

        Raises ValueError on invalid input.
        Raises AttributeError if an unknown field name is passed.
        LOT_SIZE is immutable and will raise ValueError if passed.
        """
        _MUTABLE = {
            "HTF_MINUTES":         (int,   lambda v: v >= 1,                      "must be >= 1"),
            "MTF_MINUTES":         (int,   lambda v: v >= 1,                      "must be >= 1"),
            "LTF_MINUTES":         (int,   lambda v: v >= 1,                      "must be >= 1"),
            "RETEST_ZONE_PERCENT": (float, lambda v: 0.0 < v <= 10.0,             "must be in (0, 10]"),
            "SLIPPAGE_BUFFER":     (float, lambda v: v >= 0.0,                    "must be >= 0"),
            "bars_lookback_days":  (int,   lambda v: v >= 1,                      "must be >= 1"),
            "SL_MODE":             (str,   lambda v: v in ("dynamic","structural"),"must be 'dynamic' or 'structural'"),
            "SL_PCT":              (float, lambda v: 0.1 <= v <= 20.0,            "must be in [0.1, 20.0]"),
            "SL_BUFFER_PCT":       (float, lambda v: 0.0 <= v <= 5.0,             "must be in [0.0, 5.0]"),
            "ENTRY_CUTOFF_TIME":   (str,   lambda v: len(v) in (5, 8) and v[2] == ":", "must be HH:MM or HH:MM:SS"),
        }
        unknown = set(kwargs) - set(_MUTABLE)
        if unknown:
            raise AttributeError(f"Unknown TrapEngineConfig fields: {unknown}")

        # Validate all values before acquiring the lock
        validated: dict = {}
        for key, raw in kwargs.items():
            cast_type, validator, msg = _MUTABLE[key]
            casted = cast_type(raw)
            if not validator(casted):
                raise ValueError(f"{key}={casted} invalid: {msg}")
            validated[key] = casted

        with self._lock:  # type: ignore[attr-defined]
            for key, val in validated.items():
                object.__setattr__(self, f"_{key}", val)

        return validated

    def snapshot(self) -> dict:
        """Return a thread-safe copy of all current values."""
        with self._lock:  # type: ignore[attr-defined]
            return {
                "HTF_MINUTES":         self._HTF_MINUTES,
                "MTF_MINUTES":         self._MTF_MINUTES,
                "LTF_MINUTES":         self._LTF_MINUTES,
                "RETEST_ZONE_PERCENT": self._RETEST_ZONE_PERCENT,
                "SLIPPAGE_BUFFER":     self._SLIPPAGE_BUFFER,
                "bars_lookback_days":  self._bars_lookback_days,
                "SL_MODE":             self._SL_MODE,
                "SL_PCT":              self._SL_PCT,
                "SL_BUFFER_PCT":       self._SL_BUFFER_PCT,
                "ENTRY_CUTOFF_TIME":   self._ENTRY_CUTOFF_TIME,
            }


# ─────────────────────────────────────────────────────────────────────────────
# Auth Config — dashboard login credentials (must precede GlobalConfig)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AuthConfig:
    """
    Dashboard login credentials.

    Admin password and client PINs are read from environment variables so
    they are never embedded in source code.

    Environment variables:
      TERMINUS_ADMIN_USER      — admin username  (default: "admin")
      TERMINUS_ADMIN_PASSWORD  — admin password  (default: "admin123"  ← CHANGE IN PROD)
      TERMINUS_CLIENT_PIN_<ID> — per-client PIN  (default: the client_id itself)
    """
    admin_username: str = field(
        default_factory=lambda: os.getenv("TERMINUS_ADMIN_USER", "admin")
    )
    admin_password: str = field(
        default_factory=lambda: os.getenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    )

    @staticmethod
    def client_pin(client_id: str) -> str:
        """Return the dashboard PIN for a client (default = the client_id itself)."""
        return os.getenv(f"TERMINUS_CLIENT_PIN_{client_id.upper()}", client_id)


# ─────────────────────────────────────────────────────────────────────────────
# Global System Config (single root object)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class GlobalConfig:
    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    indicators: IndicatorParams = field(default_factory=IndicatorParams)
    storage: StorageConfig = field(default_factory=StorageConfig)
    strategy: StrategyParams = field(default_factory=StrategyParams)
    auth: AuthConfig = field(default_factory=AuthConfig)
    trap_engine: TrapEngineConfig = field(default_factory=TrapEngineConfig)

    # Active indices to monitor (feeder subscribes to all)
    monitored_indices: List[str] = field(
        default_factory=lambda: ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"]
    )
    active_index: str = "NIFTY"

    # OTM/ITM depth for chain subscription
    chain_depth: int = 10          # ATM ± 10 strikes

    # Candle timeframes (minutes)
    candle_timeframes: List[int] = field(default_factory=lambda: [1, 2, 5, 15, 75])

    # Primary feeder broker (admin-selected)
    primary_feeder_provider: Literal["upstox", "fyers", "dhan", "angelone", "mock"] = "mock"

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
