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
    mcx_underlyings: tuple = ("CRUDEOIL", "CRUDEOILM", "NATURALGAS", "GOLD", "GOLDM", "SILVER")

    # Crypto (Delta Exchange) underlyings — DAILY options, 24/7, expire 17:30 IST.
    crypto_underlyings: tuple = ("BTC", "ETH")

    # Strike granularity per index (points). CRYPTO defaults are a FALLBACK ONLY — Delta strike
    # steps are non-uniform (BTC ~200 near ATM, 400/600 wings) so the live step is discovered from
    # DeltaBroker.discover_chain() per expiry and overrides these.
    strike_steps: Dict[str, float] = field(default_factory=lambda: {
        "NIFTY": 50.0,
        "BANKNIFTY": 100.0,
        "FINNIFTY": 50.0,
        "SENSEX": 100.0,
        "MIDCPNIFTY": 50.0,
        # MCX commodities — round ATM to 100 (50-step crude strikes are illiquid)
        "CRUDEOIL": 100.0,
        "CRUDEOILM": 100.0,
        "NATURALGAS": 5.0,
        "GOLD": 100.0,
        "GOLDM": 100.0,
        "SILVER": 100.0,
        # Crypto (Delta) — fallback near-ATM step; discover_chain overrides live
        "BTC": 200.0,
        "ETH": 20.0,
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
        "GOLD": 100,
        "GOLDM": 100,
        "SILVER": 30,
        # Crypto (Delta) — order size is in CONTRACTS (1 = min). lot_multiplier scales it.
        "BTC": 1,
        "ETH": 1,
    })

    def is_mcx(self, underlying: str) -> bool:
        """True if this underlying trades on MCX (commodity session + segment)."""
        return underlying.upper() in self.mcx_underlyings

    def is_crypto(self, underlying: str) -> bool:
        """True if this underlying trades on Delta Exchange (crypto daily options, 24/7)."""
        return underlying.upper() in self.crypto_underlyings

    def session_close(self, underlying: str) -> time:
        """Force-exit/close time per exchange. Crypto daily options expire 17:30 IST (the rollover
        boundary) — that's the natural square-off; MCX ~23:30; NSE 15:30."""
        if self.is_crypto(underlying):
            return time(17, 30, 0)
        return self.mcx_market_close if self.is_mcx(underlying) else self.market_close

    def load_from_db(self, db) -> None:
        """Override strike_steps and lot_sizes from system_settings table if DB values exist.

        Keys: strike_step_<UNDERLYING> (float), lot_size_<UNDERLYING> (int).
        Runs synchronously at startup before any async tasks are started.
        ExchangeConfig is frozen so we mutate the inner dicts directly.
        """
        import logging as _logging
        _log = _logging.getLogger(__name__)
        for prefix, target, cast in (
            ("strike_step_", self.strike_steps, float),
            ("lot_size_",    self.lot_sizes,    int),
        ):
            for key, value in db.get_all_settings_sync().items():
                if key.startswith(prefix):
                    underlying = key[len(prefix):].upper()
                    try:
                        target[underlying] = cast(value)
                        _log.info("ExchangeConfig: %s=%s (from DB)", key, value)
                    except (ValueError, TypeError):
                        _log.warning("ExchangeConfig: invalid DB value for %s=%r, skipping.", key, value)


# Module-level set + helper so execution bridges can pick the order exchange
# without threading a config object through (kept in sync with ExchangeConfig).
_MCX_UNDERLYINGS = {"CRUDEOIL", "CRUDEOILM", "NATURALGAS", "GOLD", "GOLDM", "SILVER"}
_CRYPTO_UNDERLYINGS = {"BTC", "ETH"}


def order_exchange(underlying: str) -> str:
    """Broker order exchange for an underlying: DELTA (crypto), MCX (commodity), BFO (SENSEX), else NFO."""
    u = (underlying or "").upper()
    if u in _CRYPTO_UNDERLYINGS:
        return "DELTA"
    if u in _MCX_UNDERLYINGS:
        return "MCX"
    if u == "SENSEX":
        return "BFO"
    return "NFO"


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
    EXIT_AUDIT       = "exit_audit"    # per-tick exit-criteria validation stream (granular UI)
    POSITION_UPDATE  = "position_update"  # strategy book position snapshot (entry/exit/roll)
    TRAP_TICK        = "trap_tick"     # trap scanner per-tick telemetry (real-time LTP + zone status)
    FNO_STOCK_ALERT  = "fno_stock_alert"   # Stage-2 intraday stock monitor alert
    FNO_STOCK_STATUS = "fno_stock_status"  # live LTP + MTF/LTF state per stock (3s broadcast)


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
    trap_scanner_enabled: bool = False   # set True in run_system when --strategies trap_scanner

    # Active indices to monitor (feeder subscribes to all). Trimmed to keep the total
    # WS subscription under the broker's ~50/connection cap (each index ≈ (2*chain_depth+1)*2
    # symbols). Add back others only if the count stays under the cap (watch the
    # "EXCEEDS the ~50/connection WS limit" warning).
    monitored_indices: List[str] = field(
        default_factory=lambda: ["CRUDEOIL"]
    )
    active_index: str = "CRUDEOIL"

    # OTM/ITM depth for chain subscription. ATM ± chain_depth strikes. Keep small so the
    # WS subscription stays under the ~50/connection cap (SS pool only needs ≈ ±4).
    chain_depth: int = 4           # ATM ± 4 strikes (9 strikes × 2 = 18 per index)

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
