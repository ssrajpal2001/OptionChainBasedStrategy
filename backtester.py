"""
backtester.py — Event-Driven Historical Backtesting Engine.

Architecture:
  • Reads synchronized historical candle + option chain CSV/Parquet files
  • Replays them tick-by-tick through the exact same MatrixEngine and
    StrategyEngine used in live trading — zero code duplication
  • Applies Indian market transaction costs (STT, exchange charges, GST)
  • Outputs a full performance report with all standard quant metrics

Expected data directory layout (configurable via BacktestConfig.data_dir):
  data/historical/
    NIFTY_5m_2024.csv          → spot/futures OHLCV
    NIFTY_chain_2024.csv       → option chain snapshots
    BANKNIFTY_5m_2024.csv
    BANKNIFTY_chain_2024.csv

Spot/futures CSV columns:
  datetime, open, high, low, close, volume

Option chain CSV columns:
  datetime, strike, call_oi, call_change_oi, call_volume, call_ltp,
  put_ltp, put_volume, put_change_oi, put_oi
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

from config import SystemConfig
from data_provider import IndexTick, OptionTick, InstrumentInfo, MockBroker
from matrix_engine import MarketMatrixEngine, TechnicalSnapshot, OptionChainSnapshot
from strategies import ConfluenceEngine, SignalEvent, SignalDirection
from execution import (
    CostCalculator, ExecutionCoordinator, OptionPosition, PositionStatus,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Synthetic Instrument Builder for Backtesting
# ---------------------------------------------------------------------------

def build_bt_instrument(
    underlying: str, strike: float, option_type: str,
    expiry: date, lot_size: int,
) -> InstrumentInfo:
    sym = f"{underlying}{expiry.strftime('%d%b%y').upper()}{int(strike)}{option_type}"
    return InstrumentInfo(
        token=f"BT-{sym}",
        symbol=sym,
        exchange="NFO",
        instrument_type="OPTIDX",
        strike=strike,
        option_type=option_type,
        expiry=expiry,
        lot_size=lot_size,
    )


# ---------------------------------------------------------------------------
# Data Loader
# ---------------------------------------------------------------------------

class HistoricalDataLoader:
    """Loads and validates historical data files for backtesting."""

    def __init__(self, config: SystemConfig) -> None:
        self._config = config
        self._data_dir = config.backtest.data_dir

    def load_spot(self, underlying: str) -> pd.DataFrame:
        path = self._find_file(underlying, "spot")
        df = self._read_df(path)
        if df.empty:
            return df
        df = self._filter_dates(df)
        if "datetime" not in df.columns:
            raise ValueError(f"Spot data file missing 'datetime' column: {path}")
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime").sort_index()
        required = {"open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Spot data missing columns: {missing}")
        logger.info("Loaded %d spot candles for %s from %s.", len(df), underlying, path)
        return df

    def load_chain(self, underlying: str) -> pd.DataFrame:
        path = self._find_file(underlying, "chain")
        df = self._read_df(path)
        if df.empty:
            return df
        df = self._filter_dates(df)
        if "datetime" not in df.columns:
            return pd.DataFrame()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime")
        for col in ["strike", "call_oi", "call_change_oi", "call_volume",
                    "call_ltp", "put_ltp", "put_volume", "put_change_oi", "put_oi"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        logger.info("Loaded %d chain snapshots for %s from %s.", len(df), underlying, path)
        return df

    def _find_file(self, underlying: str, data_type: str) -> str:
        year = self._config.backtest.start_date[:4]
        tf = self._config.backtest.candle_interval
        candidates = [
            f"{underlying}_{tf}m_{year}.csv",
            f"{underlying}_{tf}m_{year}.parquet",
            f"{underlying}_{data_type}_{year}.csv",
            f"{underlying}_{data_type}_{year}.parquet",
        ]
        if data_type == "chain":
            candidates = [f"{underlying}_chain_{year}.csv", f"{underlying}_chain_{year}.parquet"]

        for name in candidates:
            full = os.path.join(self._data_dir, name)
            if os.path.exists(full):
                return full

        # Fallback: synthesize data for testing when no file exists
        logger.warning("No %s data file found for %s — generating synthetic data.", data_type, underlying)
        return ""

    def _read_df(self, path: str) -> pd.DataFrame:
        if not path:
            return pd.DataFrame()
        if path.endswith(".parquet"):
            return pd.read_parquet(path)
        return pd.read_csv(path)

    def _filter_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        if "datetime" not in df.columns:
            return df
        df["datetime"] = pd.to_datetime(df["datetime"])
        start = pd.Timestamp(self._config.backtest.start_date)
        end = pd.Timestamp(self._config.backtest.end_date) + pd.Timedelta(days=1)
        return df[(df["datetime"] >= start) & (df["datetime"] < end)]

    def generate_synthetic_data(
        self, underlying: str, start: str, end: str, interval: int = 5
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate synthetic OHLCV + chain data for testing the backtester
        without real data files.
        """
        import random
        rng = random.Random(42)
        timestamps = pd.date_range(start, end, freq=f"{interval}min")
        # Filter to market hours (9:15 – 15:30)
        timestamps = timestamps[(timestamps.hour * 60 + timestamps.minute >= 555) &
                                 (timestamps.hour * 60 + timestamps.minute <= 930)]
        n = len(timestamps)

        base_prices = {"NIFTY": 22_000.0, "BANKNIFTY": 48_000.0, "FINNIFTY": 21_000.0}
        base = base_prices.get(underlying, 22_000.0)
        step = 50.0 if underlying in ("NIFTY", "FINNIFTY") else 100.0

        # Spot data
        closes = [base]
        for _ in range(n - 1):
            closes.append(max(closes[-1] * (1 + rng.gauss(0, 0.0008)), 1))
        opens = [closes[max(0, i - 1)] for i in range(n)]
        highs = [max(o, c) * (1 + abs(rng.gauss(0, 0.0003))) for o, c in zip(opens, closes)]
        lows = [min(o, c) * (1 - abs(rng.gauss(0, 0.0003))) for o, c in zip(opens, closes)]
        volumes = [rng.randint(50_000, 2_000_000) for _ in range(n)]

        spot_df = pd.DataFrame({
            "datetime": timestamps,
            "open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes,
        })

        # Chain data
        chain_records = []
        for i, ts in enumerate(timestamps):
            spot = closes[i]
            atm = round(spot / step) * step
            for offset in range(-5, 6):
                strike = atm + offset * step
                ce_oi = rng.randint(100_000, 10_000_000)
                pe_oi = rng.randint(100_000, 10_000_000)
                intrinsic_ce = max(spot - strike, 0)
                intrinsic_pe = max(strike - spot, 0)
                chain_records.append({
                    "datetime": ts,
                    "strike": strike,
                    "call_oi": ce_oi,
                    "call_change_oi": rng.randint(-200_000, 300_000),
                    "call_volume": rng.randint(1_000, 200_000),
                    "call_ltp": max(intrinsic_ce + abs(rng.gauss(50, 20)), 0.5),
                    "put_ltp": max(intrinsic_pe + abs(rng.gauss(50, 20)), 0.5),
                    "put_volume": rng.randint(1_000, 200_000),
                    "put_change_oi": rng.randint(-200_000, 300_000),
                    "put_oi": pe_oi,
                })

        chain_df = pd.DataFrame(chain_records)
        logger.info("Synthetic data generated: %d candles, %d chain rows.", n, len(chain_df))
        return spot_df, chain_df


# ---------------------------------------------------------------------------
# Performance Report
# ---------------------------------------------------------------------------

@dataclass
class BacktestReport:
    underlying: str
    start_date: str
    end_date: str
    initial_capital: float
    final_capital: float
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    total_costs: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    positions: List[OptionPosition] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)

    @property
    def net_pnl(self) -> float:
        return self.final_capital - self.initial_capital

    @property
    def net_return_pct(self) -> float:
        return (self.net_pnl / self.initial_capital) * 100

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    @property
    def profit_factor(self) -> float:
        if self.gross_loss == 0:
            return float("inf")
        return abs(self.gross_profit / self.gross_loss)

    @property
    def avg_win(self) -> float:
        if self.winning_trades == 0:
            return 0.0
        return self.gross_profit / self.winning_trades

    @property
    def avg_loss(self) -> float:
        if self.losing_trades == 0:
            return 0.0
        return self.gross_loss / self.losing_trades

    def print(self) -> None:
        INR = "INR"
        sep = "=" * 60
        lines = [
            f"\n{sep}",
            f"  BACKTEST REPORT - {self.underlying}",
            f"  Period: {self.start_date} to {self.end_date}",
            sep,
            f"  Initial Capital    : {INR} {self.initial_capital:>14,.2f}",
            f"  Final Capital      : {INR} {self.final_capital:>14,.2f}",
            f"  Net P&L            : {INR} {self.net_pnl:>14,.2f}  ({self.net_return_pct:+.2f}%)",
            f"  Total Costs        : {INR} {self.total_costs:>14,.2f}",
            sep,
            f"  Total Trades       : {self.total_trades:>15}",
            f"  Winning Trades     : {self.winning_trades:>15}  ({self.win_rate:.1%})",
            f"  Losing Trades      : {self.losing_trades:>15}",
            f"  Avg Win            : {INR} {self.avg_win:>14,.2f}",
            f"  Avg Loss           : {INR} {self.avg_loss:>14,.2f}",
            f"  Profit Factor      : {self.profit_factor:>15.2f}",
            sep,
            f"  Max Drawdown       : {self.max_drawdown_pct:>14.2f}%",
            f"  Sharpe Ratio       : {self.sharpe_ratio:>15.3f}",
            f"  Sortino Ratio      : {self.sortino_ratio:>15.3f}",
            f"  Calmar Ratio       : {self.calmar_ratio:>15.3f}",
            sep,
        ]
        output = "\n".join(lines)
        # Write to stdout with UTF-8 to avoid Windows cp1252 errors
        sys.stdout.buffer.write((output + "\n").encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()


def _compute_sharpe(equity_curve: List[float], risk_free_annual: float = 0.07) -> float:
    if len(equity_curve) < 2:
        return 0.0
    returns = np.diff(equity_curve) / np.array(equity_curve[:-1])
    if returns.std() == 0:
        return 0.0
    daily_rf = risk_free_annual / 252
    excess = returns - daily_rf
    return float(np.sqrt(252) * excess.mean() / returns.std())


def _compute_sortino(equity_curve: List[float], risk_free_annual: float = 0.07) -> float:
    if len(equity_curve) < 2:
        return 0.0
    returns = np.diff(equity_curve) / np.array(equity_curve[:-1])
    daily_rf = risk_free_annual / 252
    excess = returns - daily_rf
    downside = returns[returns < 0]
    if len(downside) == 0 or downside.std() == 0:
        return float("inf")
    return float(np.sqrt(252) * excess.mean() / downside.std())


def _compute_max_drawdown(equity_curve: List[float]) -> float:
    if len(equity_curve) < 2:
        return 0.0
    arr = np.array(equity_curve)
    peak = np.maximum.accumulate(arr)
    with np.errstate(divide="ignore", invalid="ignore"):
        dd = np.where(peak > 0, (peak - arr) / peak * 100, 0.0)
    return float(dd.max())


# ---------------------------------------------------------------------------
# Backtest Engine
# ---------------------------------------------------------------------------

class BacktestEngine:
    """
    Event-driven backtesting engine that replays historical data through
    the live trading pipeline with full cost modeling.

    Usage:
        engine = BacktestEngine(config)
        report = await engine.run()
        report.print()
    """

    def __init__(self, config: SystemConfig) -> None:
        self._config = config
        self._loader = HistoricalDataLoader(config)
        self._underlying = config.assets.active_index
        self._lot_size = config.assets.lot_size
        self._cost_calc = CostCalculator(config)

        # Wire up the same engines used in live trading
        self._matrix = MarketMatrixEngine(config)
        self._confluence = ConfluenceEngine(config)

        # Paper broker for order simulation
        self._broker = MockBroker(config.broker, config)

        self._equity = config.risk.capital
        self._equity_curve: List[float] = []
        self._open_positions: Dict[str, "_BTPosition"] = {}
        self._closed_positions: List["_BTPosition"] = []
        self._total_costs = 0.0
        self._trade_counter = 0
        self._active_expiry: Optional[date] = None
        self._instruments: Dict[Tuple[float, str], InstrumentInfo] = {}

    async def run(self) -> BacktestReport:
        """Main entry point. Returns the completed performance report."""
        logger.info(
            "BacktestEngine: Starting %s | %s → %s",
            self._underlying,
            self._config.backtest.start_date,
            self._config.backtest.end_date,
        )

        # Load or synthesize data
        try:
            spot_df = self._loader.load_spot(self._underlying)
            chain_df = self._loader.load_chain(self._underlying)
            if spot_df.empty:
                raise FileNotFoundError("No spot data")
        except (FileNotFoundError, ValueError):
            logger.warning("Real data not found — using synthetic data for demonstration.")
            spot_df, chain_df = self._loader.generate_synthetic_data(
                self._underlying,
                self._config.backtest.start_date,
                self._config.backtest.end_date,
                self._config.backtest.candle_interval,
            )
            spot_df = spot_df.set_index("datetime")

        # Seed the candle cache with early history for indicator warm-up
        warmup = spot_df.head(100)
        for tf in self._config.assets.candle_timeframes:
            self._matrix.load_historical_candles(self._underlying, tf, warmup)

        # Initialize the option chain matrix with the first candle's price
        first_close = float(spot_df.iloc[0]["close"])
        step = 50.0 if self._underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100.0
        first_atm = round(first_close / step) * step
        first_ts = spot_df.index[0]
        first_expiry_date = first_ts.date() if isinstance(first_ts, datetime) else first_ts
        from data_layer.instrument_registry import next_expiry as _nexp
        first_expiry = _nexp(self._underlying, first_expiry_date)
        self._matrix.initialize_chain(self._underlying, first_close, first_atm, first_expiry)
        self._active_expiry = first_expiry
        self._build_instruments(first_expiry)

        # Main replay loop
        for timestamp, row in spot_df.iterrows():
            await self._process_candle(timestamp, row, chain_df)
            self._equity_curve.append(self._equity)

        # Force-close any open positions at last price
        last_row = spot_df.iloc[-1]
        for pos_id, pos in list(self._open_positions.items()):
            self._close_bt_position(pos, exit_price=pos.last_option_ltp, reason="EOD_EXPIRY")

        report = self._build_report(spot_df.index[0], spot_df.index[-1])
        return report

    # ------------------------------------------------------------------
    # Per-candle processing
    # ------------------------------------------------------------------

    async def _process_candle(
        self,
        timestamp: datetime,
        row: pd.Series,
        chain_df: pd.DataFrame,
    ) -> None:
        spot = float(row["close"])

        # Refresh the rolling expiry weekly
        self._refresh_expiry(timestamp)

        # --- Build IndexTick from the candle --------------------------------
        tick = IndexTick(
            symbol=self._underlying,
            ltp=spot,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row.get("volume", 0)),
            timestamp=timestamp,
        )
        await self._matrix.process_tick(tick)

        # --- Inject option chain data for this candle -----------------------
        # If we have a real chain_df, use it; otherwise synthesize from spot.
        chain_rows_for_ts = (
            chain_df[chain_df["datetime"] == timestamp]
            if not chain_df.empty and "datetime" in chain_df.columns
            else pd.DataFrame()
        )
        if not chain_rows_for_ts.empty:
            await self._inject_chain_from_df(chain_rows_for_ts, timestamp)
        else:
            await self._inject_synthetic_chain(spot, timestamp)

        # --- Grab snapshots -------------------------------------------------
        snap_queue = self._matrix.get_snapshot_queue()
        tech: Optional[TechnicalSnapshot] = None
        chain_snap: Optional[OptionChainSnapshot] = None
        while not snap_queue.empty():
            tech, chain_snap = await snap_queue.get()

        if tech is None or chain_snap is None:
            self._update_open_positions(spot, timestamp)
            return

        chain_snap.spot_price = spot
        step = 50.0 if self._underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100.0
        chain_snap.atm_strike = round(spot / step) * step

        # --- Run confluence strategy engine ---------------------------------
        multi_tf = self._matrix.get_multi_timeframe_snapshots(self._underlying)

        # Update open positions first
        self._update_open_positions(spot, timestamp, chain_snap)

        # Only evaluate new signals if no positions open
        if not self._open_positions and self._risk_ok():
            signal = self._confluence.evaluate(tech, chain_snap, multi_tf)
            if signal:
                await self._enter_bt_position(signal, chain_snap, timestamp)

    async def _inject_chain_from_df(self, rows: pd.DataFrame, ts: datetime) -> None:
        expiry = self._active_expiry or date.today()
        for _, c_row in rows.iterrows():
            for opt_type in ("CE", "PE"):
                prefix = "call" if opt_type == "CE" else "put"
                ltp = float(c_row.get(f"{prefix}_ltp", 50))
                opt_tick = OptionTick(
                    symbol=f"{self._underlying}OPT{c_row['strike']}{opt_type}",
                    underlying=self._underlying, strike=float(c_row["strike"]),
                    option_type=opt_type, expiry=expiry,
                    ltp=ltp, bid=ltp - 0.5, ask=ltp + 0.5,
                    oi=int(c_row.get(f"{prefix}_oi", 0)),
                    change_oi=int(c_row.get(f"{prefix}_change_oi", 0)),
                    volume=int(c_row.get(f"{prefix}_volume", 0)),
                    iv=0.0, delta=0.5, timestamp=ts,
                )
                await self._matrix.process_tick(opt_tick)

    async def _inject_synthetic_chain(self, spot: float, ts: datetime) -> None:
        """
        Synthesize realistic option chain data from spot price.
        This ensures the backtester works without historical chain files.
        """
        import random
        rng = random.Random(int(ts.timestamp()) % (2**31))
        step = 50.0 if self._underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100.0
        atm = round(spot / step) * step
        expiry = self._active_expiry or date.today()
        depth = min(self._config.assets.otm_depth, 5)   # Limit synthetic depth

        for i in range(-depth, depth + 1):
            strike = atm + i * step
            # Update instrument registry with this strike if missing
            for opt_type in ("CE", "PE"):
                if (strike, opt_type) not in self._instruments:
                    self._instruments[(strike, opt_type)] = build_bt_instrument(
                        self._underlying, strike, opt_type, expiry, self._lot_size,
                    )

            # Compute synthetic OI with heavier concentration at ATM
            distance = abs(i)
            oi_multiplier = max(1.0 - distance * 0.08, 0.1)
            call_oi = int(rng.randint(500_000, 8_000_000) * oi_multiplier)
            put_oi = int(rng.randint(500_000, 8_000_000) * oi_multiplier)

            # Occasionally inject a large OI spike to trigger strategy signals
            if distance <= 1:
                if rng.random() < 0.08:   # 8% chance of spike
                    call_oi += rng.randint(2_000_000, 5_000_000)
                if rng.random() < 0.08:
                    put_oi += rng.randint(2_000_000, 5_000_000)

            call_change_oi = rng.randint(-200_000, 400_000)
            put_change_oi = rng.randint(-200_000, 400_000)
            call_vol = rng.randint(10_000, 500_000)
            put_vol = rng.randint(10_000, 500_000)

            # Option LTPs: intrinsic + time value
            tv = max(30.0 - distance * 8.0, 2.0)
            call_ltp = max(spot - strike, 0) + tv + abs(rng.gauss(0, 5))
            put_ltp = max(strike - spot, 0) + tv + abs(rng.gauss(0, 5))

            for opt_type, oi, d_oi, vol, ltp in [
                ("CE", call_oi, call_change_oi, call_vol, call_ltp),
                ("PE", put_oi, put_change_oi, put_vol, put_ltp),
            ]:
                opt_tick = OptionTick(
                    symbol=f"{self._underlying}OPT{strike}{opt_type}",
                    underlying=self._underlying, strike=strike,
                    option_type=opt_type, expiry=expiry,
                    ltp=round(ltp, 2), bid=round(ltp - 0.5, 2), ask=round(ltp + 0.5, 2),
                    oi=oi, change_oi=d_oi, volume=vol,
                    iv=float(abs(rng.gauss(15, 3))), delta=0.5 - i * 0.1,
                    timestamp=ts,
                )
                await self._matrix.process_tick(opt_tick)

    def _refresh_expiry(self, timestamp: datetime) -> None:
        """Set active expiry from registry (real contract dates, not day-of-week math)."""
        from data_layer.instrument_registry import next_expiry as _nexp
        ts_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp
        candidate = _nexp(self._underlying, ts_date)
        if self._active_expiry != candidate:
            self._active_expiry = candidate
            self._build_instruments(candidate)

    def _build_instruments(self, expiry: date) -> None:
        """Pre-build InstrumentInfo objects for all relevant strikes."""
        chain = self._matrix.option_chains.get(self._underlying)
        if chain:
            snap = chain.get_snapshot()
            if snap and snap.rows:
                for strike in snap.strikes_sorted():
                    for opt_type in ("CE", "PE"):
                        self._instruments[(strike, opt_type)] = build_bt_instrument(
                            self._underlying, strike, opt_type,
                            expiry, self._lot_size,
                        )
                logger.debug("Built %d instrument entries for expiry %s.", len(self._instruments), expiry)
                return

        # Chain not yet initialized — build a placeholder set around ATM
        step = 50.0 if self._underlying in ("NIFTY", "FINNIFTY", "MIDCPNIFTY") else 100.0
        base_prices = {"NIFTY": 22_000.0, "BANKNIFTY": 48_000.0, "FINNIFTY": 21_000.0}
        base = base_prices.get(self._underlying, 22_000.0)
        atm = round(base / step) * step
        depth = self._config.assets.otm_depth
        for i in range(-depth, depth + 1):
            strike = atm + i * step
            for opt_type in ("CE", "PE"):
                self._instruments[(strike, opt_type)] = build_bt_instrument(
                    self._underlying, strike, opt_type, expiry, self._lot_size,
                )

    async def _enter_bt_position(
        self,
        signal: SignalEvent,
        chain: OptionChainSnapshot,
        timestamp: datetime,
    ) -> None:
        """Simulate entry into an option position."""
        key = (signal.target_strike, signal.option_type)
        instrument = self._instruments.get(key)
        if instrument is None:
            return

        # Fetch option LTP from chain
        row = chain.get_row(signal.target_strike)
        if row is None:
            return
        option_ltp = row.call_ltp if signal.option_type == "CE" else row.put_ltp
        if option_ltp <= 0:
            option_ltp = 50.0   # Fallback

        entry_price = self._cost_calc.slippage(option_ltp, is_buy=True)
        lots = max(1, int(
            self._equity * self._config.risk.max_risk_per_trade_percent / 100
            / (entry_price * 0.5 * self._lot_size)
        ))
        lots = min(lots, 10)
        quantity = lots * self._lot_size

        entry_cost = self._cost_calc.compute(entry_price * quantity, is_sell=False)
        self._equity -= (entry_price * quantity + entry_cost)
        self._total_costs += entry_cost

        self._trade_counter += 1
        pos_id = f"BT-{self._trade_counter:04d}"

        sl_price = entry_price * 0.50
        target_price = entry_price * (1 + self._config.risk.min_risk_reward_ratio)

        pos = _BTPosition(
            pos_id=pos_id,
            signal=signal,
            instrument=instrument,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            quantity=quantity,
            entry_time=timestamp,
            last_option_ltp=entry_price,
        )
        self._open_positions[pos_id] = pos
        logger.debug(
            "BT ENTER %s | %s | entry=%.2f SL=%.2f TGT=%.2f qty=%d",
            pos_id, instrument.symbol, entry_price, sl_price, target_price, quantity,
        )

    def _update_open_positions(
        self,
        underlying_ltp: float,
        timestamp: datetime,
        chain: Optional[OptionChainSnapshot] = None,
    ) -> None:
        for pos_id, pos in list(self._open_positions.items()):
            # Estimate option LTP from chain if available
            if chain:
                row = chain.get_row(pos.signal.target_strike)
                if row:
                    pos.last_option_ltp = (
                        row.call_ltp if pos.signal.option_type == "CE" else row.put_ltp
                    ) or pos.last_option_ltp

            ltp = pos.last_option_ltp
            if ltp >= pos.target_price:
                self._close_bt_position(pos, exit_price=ltp, reason="TARGET_HIT")
            elif ltp <= pos.sl_price:
                self._close_bt_position(pos, exit_price=ltp, reason="SL_TRIGGERED")

    def _close_bt_position(
        self, pos: "_BTPosition", exit_price: float, reason: str
    ) -> None:
        exit_price = self._cost_calc.slippage(exit_price, is_buy=False)
        exit_cost = self._cost_calc.compute(exit_price * pos.quantity, is_sell=True)
        gross_pnl = (exit_price - pos.entry_price) * pos.quantity
        net_pnl = gross_pnl - exit_cost

        self._equity += exit_price * pos.quantity - exit_cost
        self._total_costs += exit_cost
        pos.realized_pnl = net_pnl
        pos.exit_price = exit_price
        pos.exit_time = datetime.now()
        pos.exit_reason = reason

        self._open_positions.pop(pos.pos_id, None)
        self._closed_positions.append(pos)
        logger.debug(
            "BT CLOSE %s | exit=%.2f | net_pnl=₹%.2f | reason=%s",
            pos.pos_id, exit_price, net_pnl, reason,
        )

    def _risk_ok(self) -> bool:
        total_loss_pct = (self._config.risk.capital - self._equity) / self._config.risk.capital * 100
        return total_loss_pct < self._config.risk.max_daily_loss_percent * 5

    def _build_report(self, start_ts: datetime, end_ts: datetime) -> BacktestReport:
        closed = self._closed_positions
        winners = [p for p in closed if p.realized_pnl > 0]
        losers = [p for p in closed if p.realized_pnl <= 0]

        gross_profit = sum(p.realized_pnl for p in winners)
        gross_loss = sum(p.realized_pnl for p in losers)

        report = BacktestReport(
            underlying=self._underlying,
            start_date=str(start_ts.date()),
            end_date=str(end_ts.date()),
            initial_capital=self._config.risk.capital,
            final_capital=self._equity,
            total_trades=len(closed),
            winning_trades=len(winners),
            losing_trades=len(losers),
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            total_costs=self._total_costs,
            equity_curve=self._equity_curve,
        )

        report.max_drawdown_pct = _compute_max_drawdown(self._equity_curve)
        report.sharpe_ratio = _compute_sharpe(self._equity_curve)
        report.sortino_ratio = _compute_sortino(self._equity_curve)
        if report.max_drawdown_pct > 0:
            annual_return = report.net_return_pct / max(
                ((end_ts - start_ts).days / 365), 0.01
            )
            report.calmar_ratio = annual_return / report.max_drawdown_pct
        return report


# ---------------------------------------------------------------------------
# Internal BT position dataclass (lighter than OptionPosition)
# ---------------------------------------------------------------------------

@dataclass
class _BTPosition:
    pos_id: str
    signal: SignalEvent
    instrument: InstrumentInfo
    entry_price: float
    sl_price: float
    target_price: float
    quantity: int
    entry_time: datetime
    last_option_ltp: float = 0.0
    realized_pnl: float = 0.0
    exit_price: float = 0.0
    exit_time: Optional[datetime] = None
    exit_reason: str = ""
