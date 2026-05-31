"""
backtester/historical_core.py — Event-driven backtester on recorded tick data.

Data source: Parquet files written by data_layer/tick_recorder.py.

Architecture:
  1. Load spot (IndexTick) data from Parquet → replay as synthetic IndexTick events
  2. Inject synthetic OptionTick data from a simplified Black-Scholes approximation
     (or from recorded option Parquet files when available)
  3. Feed ticks through CandleCache → indicators → ChainSnapshot → ConfluenceEngine
  4. Each generated SignalPackage is tracked as a virtual trade
  5. Exit trades at EOD (15:30) or on stop-loss / target hit
  6. Produce BacktestReport with per-trade details and aggregate stats

No time.sleep. Replay uses direct method calls (synchronous path).
All asyncio-dependent paths are bypassed via force_evaluate().
"""

from __future__ import annotations

import logging
import math
import os
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

from config.global_config import IST, GlobalConfig, StrategyParams
from data_layer.base_feeder import IndexTick, OptionTick, CandleEvent
from matrix_engine.candle_cache import _CandleSeries, _Bucket
from matrix_engine.indicators import rsi, vwap, atr, adx, ema, volume_spike, TechSnapshot
from matrix_engine.option_matrix import ChainRow, ChainSnapshot
from strategies.base_strategy import ConfluenceEngine, SignalPackage, Direction

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Trade Record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    signal: SignalPackage
    entry_time: datetime
    entry_option_price: float
    exit_time: Optional[datetime] = None
    exit_option_price: float = 0.0
    qty: int = 1
    exit_reason: str = ""      # "TARGET", "SL", "EOD", "SIGNAL_EXIT"
    gross_pnl: float = 0.0
    entry_cost: float = 0.0
    exit_cost: float = 0.0

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.entry_cost - self.exit_cost

    @property
    def is_winner(self) -> bool:
        return self.net_pnl > 0


# ─────────────────────────────────────────────────────────────────────────────
# Cost Calculator (mirrors execution_bridge/execution_router.py)
# ─────────────────────────────────────────────────────────────────────────────

class _CostCalc:
    STT_SELL_PCT   = 0.0625 / 100
    EXCHANGE_PCT   = 0.035  / 100
    SEBI_FEE_PCT   = 0.0001 / 100
    GST_PCT        = 0.18
    BROKERAGE_FLAT = 20.0

    @classmethod
    def entry(cls, price: float, qty: int) -> float:
        t = price * qty
        b = cls.BROKERAGE_FLAT
        e = t * cls.EXCHANGE_PCT
        s = t * cls.SEBI_FEE_PCT
        return round(b + e + s + (b + e) * cls.GST_PCT, 2)

    @classmethod
    def exit_(cls, price: float, qty: int) -> float:
        t = price * qty
        stt = t * cls.STT_SELL_PCT
        b = cls.BROKERAGE_FLAT
        e = t * cls.EXCHANGE_PCT
        s = t * cls.SEBI_FEE_PCT
        return round(stt + b + e + s + (b + e) * cls.GST_PCT, 2)


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic Option Pricer (simplified ATM approx for backtesting)
# ─────────────────────────────────────────────────────────────────────────────

def _approx_option_price(
    spot: float,
    strike: float,
    tte_days: float,      # Days to expiry
    iv: float = 0.18,     # Annualized vol (default 18% for ATM options)
    is_call: bool = True,
) -> float:
    """
    Black-Scholes approximation for ATM/near-ATM options.
    Returns a minimum of 0.05 (tick size).
    """
    if tte_days <= 0:
        intrinsic = max(0.0, spot - strike) if is_call else max(0.0, strike - spot)
        return max(0.05, intrinsic)

    t = tte_days / 365.0
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / (iv * math.sqrt(t))
    d2 = d1 - iv * math.sqrt(t)
    nd1 = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    nd2 = 0.5 * (1 + math.erf(d2 / math.sqrt(2)))

    if is_call:
        price = spot * nd1 - strike * nd2
    else:
        price = strike * (1 - nd2) - spot * (1 - nd1)
    return max(0.05, round(price, 2))


# ─────────────────────────────────────────────────────────────────────────────
# Lightweight Candle Builder (no asyncio dependency)
# ─────────────────────────────────────────────────────────────────────────────

class _LightCandleBuilder:
    """Builds OHLCV candles from a stream of (timestamp, ltp, volume) tuples."""

    def __init__(self, timeframe_minutes: int) -> None:
        self._tf = timeframe_minutes
        self._bucket_ts: Optional[datetime] = None
        self._open = self._high = self._low = self._close = 0.0
        self._volume = 0
        self._candles: List[Tuple[datetime, float, float, float, float, int]] = []

    def _bucket_start(self, ts: datetime) -> datetime:
        t = ts
        minutes = (t.hour * 60 + t.minute) // self._tf * self._tf
        return t.replace(hour=minutes // 60, minute=minutes % 60, second=0, microsecond=0)

    def update(self, ts: datetime, ltp: float, volume: int) -> Optional[Tuple]:
        bs = self._bucket_start(ts)
        if self._bucket_ts is None:
            self._bucket_ts = bs
            self._open = self._high = self._low = self._close = ltp
            self._volume = volume
            return None

        if bs == self._bucket_ts:
            self._high = max(self._high, ltp)
            self._low  = min(self._low,  ltp)
            self._close = ltp
            self._volume += volume
            return None

        # Candle closed
        closed = (self._bucket_ts, self._open, self._high, self._low, self._close, self._volume)
        self._candles.append(closed)
        # Start new bucket
        self._bucket_ts = bs
        self._open = self._high = self._low = self._close = ltp
        self._volume = volume
        return closed

    def flush(self) -> Optional[Tuple]:
        if self._bucket_ts and self._close > 0:
            closed = (self._bucket_ts, self._open, self._high, self._low, self._close, self._volume)
            self._candles.append(closed)
            self._bucket_ts = None
            return closed
        return None

    @property
    def candles(self) -> List[Tuple]:
        return self._candles


# ─────────────────────────────────────────────────────────────────────────────
# Indicator State Manager
# ─────────────────────────────────────────────────────────────────────────────

class _IndicatorState:
    """Rolling numpy arrays for indicator computation, bounded by max window."""

    MAX_LEN = 600   # More than 500-candle VWAP window

    def __init__(self) -> None:
        self.opens   = deque(maxlen=self.MAX_LEN)
        self.highs   = deque(maxlen=self.MAX_LEN)
        self.lows    = deque(maxlen=self.MAX_LEN)
        self.closes  = deque(maxlen=self.MAX_LEN)
        self.volumes = deque(maxlen=self.MAX_LEN)

    def push(self, o: float, h: float, l: float, c: float, v: int) -> None:
        self.opens.append(o)
        self.highs.append(h)
        self.lows.append(l)
        self.closes.append(c)
        self.volumes.append(float(v))

    def compute(self, cfg: GlobalConfig, ts: datetime, symbol: str) -> Optional[TechSnapshot]:
        if len(self.closes) < 22:
            return None

        cl = np.array(self.closes, dtype=np.float64)
        hi = np.array(self.highs,  dtype=np.float64)
        lo = np.array(self.lows,   dtype=np.float64)
        vo = np.array(self.volumes, dtype=np.float64)
        op = np.array(self.opens,  dtype=np.float64)

        adx_v, pdi, mdi = adx(hi, lo, cl)

        return TechSnapshot(
            symbol=symbol,
            timeframe=cfg.candle_timeframes[0],
            timestamp=ts,
            ltp=float(cl[-1]),
            rsi=rsi(cl),
            vwap_val=vwap(hi, lo, cl, vo),
            adx_val=adx_v,
            plus_di=pdi,
            minus_di=mdi,
            ema_fast=ema(cl, cfg.indicators.ema_fast),
            ema_slow=ema(cl, cfg.indicators.ema_slow),
            atr_val=atr(hi, lo, cl, cfg.indicators.atr_period),
            vol_ma=float(vo[-20:].mean()) if len(vo) >= 20 else float(vo.mean()),
            c_open=float(op[-1]),
            c_high=float(hi[-1]),
            c_low=float(lo[-1]),
            c_close=float(cl[-1]),
            c_volume=int(vo[-1]),
            p_open=float(op[-2]) if len(op) >= 2 else 0.0,
            p_high=float(hi[-2]) if len(hi) >= 2 else 0.0,
            p_low=float(lo[-2]) if len(lo) >= 2 else 0.0,
            p_close=float(cl[-2]) if len(cl) >= 2 else 0.0,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Backtest Report
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BacktestReport:
    underlying: str
    start_date: date
    end_date: date
    trades: List[TradeRecord] = field(default_factory=list)
    initial_capital: float = 500_000.0

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def winners(self) -> int:
        return sum(1 for t in self.trades if t.is_winner)

    @property
    def losers(self) -> int:
        return self.total_trades - self.winners

    @property
    def win_rate(self) -> float:
        return self.winners / self.total_trades if self.total_trades else 0.0

    @property
    def net_pnl(self) -> float:
        return sum(t.net_pnl for t in self.trades)

    @property
    def gross_pnl(self) -> float:
        return sum(t.gross_pnl for t in self.trades)

    @property
    def total_costs(self) -> float:
        return sum(t.entry_cost + t.exit_cost for t in self.trades)

    @property
    def profit_factor(self) -> float:
        gross_win = sum(t.net_pnl for t in self.trades if t.is_winner)
        gross_loss = abs(sum(t.net_pnl for t in self.trades if not t.is_winner))
        return gross_win / gross_loss if gross_loss > 0 else float("inf")

    @property
    def avg_winner(self) -> float:
        w = [t.net_pnl for t in self.trades if t.is_winner]
        return sum(w) / len(w) if w else 0.0

    @property
    def avg_loser(self) -> float:
        l = [t.net_pnl for t in self.trades if not t.is_winner]
        return sum(l) / len(l) if l else 0.0

    @property
    def max_drawdown(self) -> float:
        equity = self.initial_capital
        peak = equity
        max_dd = 0.0
        for t in self.trades:
            equity += t.net_pnl
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
        return max_dd

    def print(self) -> None:
        import sys
        lines = [
            "",
            "=" * 60,
            f"BACKTEST REPORT — {self.underlying}",
            f"Period : {self.start_date} to {self.end_date}",
            f"Capital: INR {self.initial_capital:,.0f}",
            "-" * 60,
            f"Total Trades  : {self.total_trades}",
            f"Winners       : {self.winners}  ({self.win_rate:.1%})",
            f"Losers        : {self.losers}",
            f"Gross P&L     : INR {self.gross_pnl:,.2f}",
            f"Total Costs   : INR {self.total_costs:,.2f}",
            f"Net P&L       : INR {self.net_pnl:,.2f}",
            f"Profit Factor : {self.profit_factor:.2f}",
            f"Avg Winner    : INR {self.avg_winner:,.2f}",
            f"Avg Loser     : INR {self.avg_loser:,.2f}",
            f"Max Drawdown  : INR {self.max_drawdown:,.2f}",
            "-" * 60,
        ]

        exit_counts: Dict[str, int] = defaultdict(int)
        for t in self.trades:
            exit_counts[t.exit_reason] += 1
        for reason, cnt in sorted(exit_counts.items()):
            lines.append(f"  {reason:<12}: {cnt}")

        lines += ["=" * 60, ""]

        output = "\n".join(lines)
        sys.stdout.buffer.write((output + "\n").encode("utf-8", errors="replace"))
        sys.stdout.buffer.flush()

    def to_dict(self) -> Dict:
        return {
            "underlying": self.underlying,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "total_trades": self.total_trades,
            "win_rate": round(self.win_rate, 4),
            "net_pnl": round(self.net_pnl, 2),
            "profit_factor": round(self.profit_factor, 4),
            "max_drawdown": round(self.max_drawdown, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Backtester Engine
# ─────────────────────────────────────────────────────────────────────────────

class HistoricalBacktester:
    """
    Replays recorded Parquet tick data through the full strategy pipeline.

    Usage:
        bt = HistoricalBacktester(cfg, confluence_engine)
        report = bt.run(
            underlying="NIFTY",
            start=date(2024, 1, 15),
            end=date(2024, 2, 14),
            capital=500_000,
        )
        report.print()
    """

    def __init__(self, cfg: GlobalConfig, confluence: ConfluenceEngine) -> None:
        self._cfg = cfg
        self._confluence = confluence
        self._cost = _CostCalc()

    def run(
        self,
        underlying: str,
        start: date,
        end: date,
        capital: float = 500_000.0,
        lot_size: Optional[int] = None,
    ) -> BacktestReport:
        report = BacktestReport(
            underlying=underlying,
            start_date=start,
            end_date=end,
            initial_capital=capital,
        )

        ls = lot_size or self._cfg.exchange.lot_sizes.get(underlying, 25)
        spot_data = self._load_spot_data(underlying, start, end)

        if not spot_data:
            logger.warning("Backtester: No spot data found for %s %s-%s.", underlying, start, end)
            return report

        logger.info(
            "Backtester: Replaying %d ticks for %s from %s to %s.",
            len(spot_data), underlying, start, end,
        )

        ind_state = _IndicatorState()
        candle_builder = _LightCandleBuilder(self._cfg.candle_timeframes[0])
        open_trade: Optional[TradeRecord] = None
        from data_layer.instrument_registry import REGISTRY as _REG
        expiry = _REG.get_active_expiry(underlying, start)

        for tick_ts, ltp, volume in spot_data:
            # Build candle
            closed = candle_builder.update(tick_ts, ltp, volume)
            if closed is None:
                # Still inside the current candle — update open trade SL/target
                if open_trade:
                    open_trade = self._check_intracandle(open_trade, ltp, tick_ts)
                continue

            _, o, h, l, c, v = closed
            ind_state.push(o, h, l, c, v)

            tech = ind_state.compute(self._cfg, tick_ts, underlying)
            if tech is None:
                continue

            # Rebuild expiry if past current
            if expiry is None or tick_ts.date() > expiry:
                expiry = _REG.get_active_expiry(underlying, tick_ts.date())

            chain = self._build_chain(underlying, c, tick_ts, expiry)

            # Check exit conditions on open trade
            if open_trade:
                open_trade = self._check_exit(open_trade, tech, ltp, tick_ts, report)

            # Only consider new signals if flat
            if open_trade is None:
                signal = self._confluence.force_evaluate(tech, chain)
                if signal:
                    opt_price = self._option_price(signal, ltp, tick_ts, expiry)
                    qty = self._compute_qty(capital, opt_price, ls)
                    entry_cost = self._cost.entry(opt_price, qty)
                    open_trade = TradeRecord(
                        signal=signal,
                        entry_time=tick_ts,
                        entry_option_price=opt_price,
                        qty=qty,
                        entry_cost=entry_cost,
                    )
                    logger.info(
                        "BT ENTRY: %s %s @ opt=%.2f (spot=%.2f) qty=%d",
                        signal.direction.name, underlying, opt_price, ltp, qty,
                    )

            # EOD exit at 15:25
            if tick_ts.hour == 15 and tick_ts.minute >= 25 and open_trade:
                self._close_trade(open_trade, ltp, tick_ts, "EOD", report)
                open_trade = None

        # Flush any open trade at end of replay
        if open_trade and spot_data:
            last_ts, last_ltp, _ = spot_data[-1]
            self._close_trade(open_trade, last_ltp, last_ts, "EOD", report)

        return report

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _load_spot_data(
        self, underlying: str, start: date, end: date
    ) -> List[Tuple[datetime, float, int]]:
        """
        Load spot tick data from recorded Parquet files or generate synthetic data.

        Looks in: {storage.recorded_dir}/{underlying}/spot/YYYY-MM-DD.parquet
        Falls back to synthetic intraday data if no files found.
        """
        storage = self._cfg.storage
        recorded_dir = os.path.join(storage.recorded_dir, underlying, "spot")
        rows: List[Tuple[datetime, float, int]] = []

        current = start
        while current <= end:
            parquet_path = os.path.join(recorded_dir, f"{current}.parquet")
            if os.path.exists(parquet_path):
                try:
                    import pyarrow.parquet as pq  # type: ignore[import]
                    table = pq.read_table(parquet_path, columns=["timestamp", "ltp", "volume"])
                    for ts_val, ltp_val, vol_val in zip(
                        table["timestamp"].to_pylist(),
                        table["ltp"].to_pylist(),
                        table["volume"].to_pylist(),
                    ):
                        if isinstance(ts_val, datetime):
                            ts = ts_val if ts_val.tzinfo else ts_val.replace(tzinfo=IST)
                        else:
                            ts = datetime.fromisoformat(str(ts_val)).replace(tzinfo=IST)
                        rows.append((ts, float(ltp_val), int(vol_val or 0)))
                except Exception as exc:
                    logger.warning("Backtester: Could not read %s: %s", parquet_path, exc)
            else:
                # Synthetic fallback: generate 1-minute candles for the day
                rows.extend(self._synthetic_day(underlying, current))

            current += timedelta(days=1)
            # Skip weekends
            while current.weekday() >= 5:
                current += timedelta(days=1)

        rows.sort(key=lambda x: x[0])
        return rows

    def _synthetic_day(
        self, underlying: str, day: date
    ) -> List[Tuple[datetime, float, int]]:
        """Generate realistic synthetic NIFTY-like 1-minute data for a trading day."""
        base_prices = {
            "NIFTY": 22000.0, "BANKNIFTY": 48000.0,
            "FINNIFTY": 19000.0, "SENSEX": 73000.0, "MIDCPNIFTY": 10500.0,
        }
        base = base_prices.get(underlying, 20000.0)
        np.random.seed(int(day.strftime("%j")) + hash(underlying) % 1000)

        market_open = datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST)
        rows = []
        price = base * (1 + np.random.uniform(-0.02, 0.02))
        drift = np.random.uniform(-0.0001, 0.0002)   # Slight upward/downward drift

        for minute in range(375):   # 09:15 to 15:30 = 375 minutes
            ts = market_open + timedelta(minutes=minute)
            noise = np.random.normal(0, base * 0.0008)
            price = max(price * (1 + drift) + noise, base * 0.85)
            volume = int(np.random.randint(50_000, 500_000))
            rows.append((ts, round(price, 2), volume))

        return rows

    def _build_chain(
        self, underlying: str, spot: float, ts: datetime, expiry: date
    ) -> ChainSnapshot:
        """Construct a synthetic ChainSnapshot for the given spot price."""
        step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        atm = round(spot / step) * step
        tte = (expiry - ts.date()).days + 1

        rows: Dict[float, ChainRow] = {}
        depth = self._cfg.chain_depth
        min_delta_oi = self._cfg.strategy.oi_zone_min_delta_oi  # 50_000

        for i in range(-depth, depth + 1):
            strike = atm + i * step
            call_p = _approx_option_price(spot, strike, tte, is_call=True)
            put_p  = _approx_option_price(spot, strike, tte, is_call=False)
            # OI decays away from ATM; peaks at ATM+2 (call) and ATM-2 (put)
            decay = max(0.2, 1.0 - abs(i) * 0.08)
            c_oi = int(np.random.randint(500_000, 2_000_000) * decay)
            p_oi = int(np.random.randint(500_000, 2_000_000) * decay)

            # Synthetic DOI: active writing/unwinding near ATM boundaries
            # Positive DOI = fresh writing; negative = unwinding
            c_doi = int(np.random.choice([-1, 0, 0, 1, 2]) * np.random.randint(
                int(min_delta_oi * 0.5), int(min_delta_oi * 3)
            ))
            p_doi = int(np.random.choice([-1, 0, 0, 1, 2]) * np.random.randint(
                int(min_delta_oi * 0.5), int(min_delta_oi * 3)
            ))

            row = ChainRow(strike=strike)
            row.call_ltp  = call_p
            row.put_ltp   = put_p
            row.call_oi   = c_oi
            row.put_oi    = p_oi
            row.call_doi  = c_doi
            row.put_doi   = p_doi
            row.call_iv   = 0.18 + abs(i) * 0.005
            row.put_iv    = 0.18 + abs(i) * 0.005
            rows[strike]  = row

        total_call_oi = sum(r.call_oi for r in rows.values())
        total_put_oi  = sum(r.put_oi  for r in rows.values())
        pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 1.0

        max_c = max(rows.values(), key=lambda r: r.call_oi).strike
        max_p = max(rows.values(), key=lambda r: r.put_oi).strike

        snap = ChainSnapshot(
            underlying=underlying,
            spot=spot,
            atm_strike=atm,
            expiry=expiry,
            timestamp=ts,
            rows=rows,
            max_call_oi_strike=max_c,
            max_put_oi_strike=max_p,
            pcr=pcr,
        )
        return snap

    def _option_price(
        self, signal: SignalPackage, spot: float, ts: datetime, expiry: date
    ) -> float:
        tte = (expiry - ts.date()).days + 1
        is_call = signal.option_type == "CE"
        return _approx_option_price(spot, signal.target_strike, tte, is_call=is_call)

    def _compute_qty(self, capital: float, opt_price: float, lot_size: int) -> int:
        risk_capital = capital * 0.01   # 1% of capital per trade
        max_lots = int(risk_capital / (opt_price * lot_size))
        return max(1, min(max_lots, 5)) * lot_size

    def _check_intracandle(
        self, trade: TradeRecord, ltp: float, ts: datetime
    ) -> Optional[TradeRecord]:
        """Check spot-level SL/target during an open candle."""
        sig = trade.signal
        if sig.direction == Direction.LONG:
            if ltp <= sig.stop_spot:
                # Approximate option price at SL level
                opt_price = trade.entry_option_price * 0.5
                trade.exit_option_price = opt_price
                trade.exit_time = ts
                trade.exit_reason = "SL"
                trade.gross_pnl = (opt_price - trade.entry_option_price) * trade.qty
                trade.exit_cost = self._cost.exit_(opt_price, trade.qty)
                return None
        else:
            if ltp >= sig.stop_spot:
                opt_price = trade.entry_option_price * 0.5
                trade.exit_option_price = opt_price
                trade.exit_time = ts
                trade.exit_reason = "SL"
                trade.gross_pnl = (opt_price - trade.entry_option_price) * trade.qty
                trade.exit_cost = self._cost.exit_(opt_price, trade.qty)
                return None
        return trade

    def _check_exit(
        self,
        trade: TradeRecord,
        tech: TechSnapshot,
        ltp: float,
        ts: datetime,
        report: BacktestReport,
    ) -> Optional[TradeRecord]:
        sig = trade.signal
        hit_target = hit_sl = False

        if sig.direction == Direction.LONG:
            hit_target = ltp >= sig.target_spot
            hit_sl     = ltp <= sig.stop_spot
        else:
            hit_target = ltp <= sig.target_spot
            hit_sl     = ltp >= sig.stop_spot

        if hit_target or hit_sl:
            reason = "TARGET" if hit_target else "SL"
            self._close_trade(trade, ltp, ts, reason, report)
            return None
        return trade

    def _close_trade(
        self,
        trade: TradeRecord,
        spot: float,
        ts: datetime,
        reason: str,
        report: BacktestReport,
    ) -> None:
        from data_layer.instrument_registry import REGISTRY as _REG
        expiry = _REG.get_active_expiry(trade.signal.underlying if hasattr(trade.signal, "underlying") else "NIFTY", ts.date())
        tte = (expiry - ts.date()).days + 1 if expiry else 1
        is_call = trade.signal.option_type == "CE"
        opt_price = _approx_option_price(
            spot, trade.signal.target_strike, tte, is_call=is_call
        )
        trade.exit_option_price = opt_price
        trade.exit_time = ts
        trade.exit_reason = reason
        trade.gross_pnl = (opt_price - trade.entry_option_price) * trade.qty
        trade.exit_cost = self._cost.exit_(opt_price, trade.qty)
        report.trades.append(trade)
        logger.info(
            "BT EXIT [%s]: opt_exit=%.2f  gross=%.2f  net=%.2f",
            reason, opt_price, trade.gross_pnl, trade.net_pnl,
        )

