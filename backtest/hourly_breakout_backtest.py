"""
backtest/hourly_breakout_backtest.py — walk-forward backtest for HourlyBreakoutStrategy.

Reads saved CSVs from data/hourly_breakout/ (spot + per-strike option files) and
runs the strategy bar-by-bar, selecting the daily ATM strike from the manifest.
Prints a P&L summary and saves a trade log CSV.

Usage:
    python backtest/hourly_breakout_backtest.py [--data-dir data/hourly_breakout]
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from strategies.hourly_breakout.strategy import HourlyBreakoutStrategy, Side

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)
EOD_SQUAREOFF = time(15, 25)
STRIKE_STEP = 50.0
LOT_SIZE = 65


@dataclass
class Trade:
    day: date
    side: str
    entry_time: datetime
    entry_price: float
    sl_price: float
    target_price: float
    exit_time: Optional[datetime]
    exit_price: Optional[float]
    reason: str
    pnl_pts: float = 0.0
    pnl_rs: float = 0.0


def _load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df.get("ts", df.get("datetime")))
    df = df.sort_values("ts").drop_duplicates("ts").reset_index(drop=True)
    df = df.rename(columns={"ts": "datetime"})
    return df[["datetime", "open", "high", "low", "close", "volume"]]


def _resample(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    df = df.set_index("datetime")
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    rs = df.resample(freq).agg(agg).dropna()
    return rs


def _market_days(spot: pd.DataFrame) -> List[date]:
    spot = spot.copy()
    spot["date"] = spot["datetime"].dt.date
    days = sorted(spot["date"].unique())
    return [d for d in days if isinstance(d, date)]


def _day_data(df: pd.DataFrame, d: date) -> pd.DataFrame:
    mask = df["datetime"].dt.date == d
    data = df.loc[mask].copy()
    data = data[(data["datetime"].dt.time >= MARKET_OPEN) & (data["datetime"].dt.time <= MARKET_CLOSE)]
    return data


def _read_option(data_dir: Path, side: str, strike: int) -> Optional[pd.DataFrame]:
    path = data_dir / f"opt_NIFTY{side}{strike}_1m.csv"
    if not path.exists():
        return None
    return _load_csv(path)


class BacktestEngine:
    def __init__(self, data_dir: Path, sl_buffer: float = 2.0, min_rr: float = 1.5):
        self.data_dir = data_dir
        self.spot = _load_csv(data_dir / "spot_NIFTY_1m.csv")
        try:
            self.manifest = pd.read_csv(data_dir / "strike_manifest.csv")
            self.manifest["date"] = pd.to_datetime(self.manifest["date"]).dt.date
        except Exception:
            self.manifest = None
        self.sl_buffer = sl_buffer
        self.min_rr = min_rr
        self.trades: List[Trade] = []

    def _atm_for_day(self, d: date) -> int:
        if self.manifest is not None:
            row = self.manifest[self.manifest["date"] == d]
            if not row.empty:
                return int(row.iloc[0]["atm_strike"])
        day_spot = _day_data(self.spot, d)
        if day_spot.empty:
            return 0
        o = float(day_spot.iloc[0]["open"])
        return int(round(o / STRIKE_STEP) * STRIKE_STEP)

    def _select_options(self, d: date) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        strike = self._atm_for_day(d)
        if strike <= 0:
            return None, None
        # If exact ATM not saved, try neighbours.
        for delta in (0, -1, 1, -2, 2):
            s = strike + delta * int(STRIKE_STEP)
            ce = _read_option(self.data_dir, "CE", s)
            pe = _read_option(self.data_dir, "PE", s)
            if ce is not None and pe is not None:
                return _day_data(ce, d), _day_data(pe, d)
        return None, None

    def run_day(self, d: date) -> None:
        day_spot = _day_data(self.spot, d)
        ce, pe = self._select_options(d)
        if day_spot.empty or ce is None or pe is None:
            return

        spot5 = _resample(day_spot, "5min")
        ce5 = _resample(ce, "5min")
        pe5 = _resample(pe, "5min")
        spot1h = _resample(day_spot, "1h")
        ce1h = _resample(ce, "1h")
        pe1h = _resample(pe, "1h")

        if len(spot5) < 3 or len(ce5) < 3 or len(pe5) < 3:
            return

        strategy = HourlyBreakoutStrategy(
            underlying="NIFTY",
            lot_size=LOT_SIZE,
            lot_multiplier=1,
            sl_buffer_pts=self.sl_buffer,
            min_rr=self.min_rr,
            max_spread_pct=1.5,
        )

        active_trade: Optional[Trade] = None
        pending_entry: Optional[Tuple[datetime, str, float, float, float, str]] = None

        # Walk 5m bars in chronological order.
        for i in range(1, len(spot5) + 1):
            ts = spot5.index[i - 1]
            if ts.time() > EOD_SQUAREOFF:
                break

            spot5_slice = spot5.iloc[:i]
            ce5_slice = ce5.iloc[:i]
            pe5_slice = pe5.iloc[:i]
            spot1h_slice = spot1h.loc[:ts]
            ce1h_slice = ce1h.loc[:ts]
            pe1h_slice = pe1h.loc[:ts]

            strategy.on_1h_candle_close(spot1h_slice, ce1h_slice, pe1h_slice)
            strategy.on_5m_candle_close(spot5_slice, ce5_slice, pe5_slice)

            for sig in strategy.get_active_orders_and_signals():
                if sig.action == "ENTRY" and active_trade is None:
                    active_trade = Trade(
                        day=d,
                        side=sig.side.value,
                        entry_time=sig.trigger_timestamp,
                        entry_price=sig.entry_price,
                        sl_price=sig.sl_price,
                        target_price=sig.target_price,
                        exit_time=None,
                        exit_price=None,
                        reason="",
                    )
                elif sig.action == "EXIT" and active_trade is not None:
                    active_trade.exit_time = sig.trigger_timestamp
                    active_trade.exit_price = sig.exit_price or sig.entry_price
                    active_trade.reason = sig.reason
                    self._close_trade(active_trade)
                    active_trade = None

            # Manual EOD square-off.
            if active_trade and ts.time() >= EOD_SQUAREOFF:
                active_trade.exit_time = ts
                active_trade.exit_price = float(ce5_slice.iloc[-1]["close"]) if active_trade.side == "CE" else float(pe5_slice.iloc[-1]["close"])
                active_trade.reason = "eod_squareoff"
                self._close_trade(active_trade)
                active_trade = None
                break

    def _close_trade(self, t: Trade) -> None:
        if t.exit_price is None:
            return
        mult = 1 if t.side == "CE" else -1
        t.pnl_pts = mult * (t.exit_price - t.entry_price)
        t.pnl_rs = t.pnl_pts * LOT_SIZE
        self.trades.append(t)

    def run(self) -> None:
        days = _market_days(self.spot)
        logger.info("Backtesting %d trading days", len(days))
        for d in days:
            self.run_day(d)
        self._report()

    def _report(self) -> None:
        if not self.trades:
            print("No trades generated.")
            return
        wins = sum(1 for t in self.trades if t.pnl_rs > 0)
        losses = len(self.trades) - wins
        gross_profit = sum(t.pnl_rs for t in self.trades if t.pnl_rs > 0)
        gross_loss = sum(t.pnl_rs for t in self.trades if t.pnl_rs < 0)
        net = sum(t.pnl_rs for t in self.trades)
        pf = gross_profit / abs(gross_loss) if gross_loss != 0 else float("inf")
        avg_win = gross_profit / wins if wins else 0
        avg_loss = gross_loss / losses if losses else 0
        max_dd = self._max_drawdown()
        print("\n========== HourlyBreakout Backtest ==========")
        print(f"Total trades   : {len(self.trades)}")
        print(f"Wins / Losses  : {wins} / {losses}")
        print(f"Win rate       : {100.0*wins/len(self.trades):.1f}%")
        print(f"Gross profit   : +Rs {gross_profit:,.2f}")
        print(f"Gross loss     : Rs {gross_loss:,.2f}")
        print(f"Net P&L        : Rs {net:,.2f}")
        print(f"Profit factor  : {pf:.2f}")
        print(f"Avg win / loss : {avg_win:,.2f} / {avg_loss:,.2f}")
        print(f"Max drawdown   : Rs {max_dd:,.2f}")
        # Reason breakdown
        reasons: Dict[str, List[float]] = {}
        for t in self.trades:
            reasons.setdefault(t.reason, []).append(t.pnl_rs)
        print("\nExit reason breakdown:")
        for r, pnl in reasons.items():
            print(f"  {r:20s}: {len(pnl):3d} trades, sum Rs {sum(pnl):,.2f}")
        # Save trade log
        out = self.data_dir / "hourly_breakout_trades.csv"
        pd.DataFrame([
            {"day": t.day, "side": t.side, "entry_time": t.entry_time, "entry_price": t.entry_price,
             "sl": t.sl_price, "target": t.target_price, "exit_time": t.exit_time,
             "exit_price": t.exit_price, "reason": t.reason, "pnl_pts": t.pnl_pts, "pnl_rs": t.pnl_rs}
            for t in self.trades
        ]).to_csv(out, index=False)
        print(f"\nTrade log saved: {out}")

    def _max_drawdown(self) -> float:
        peak, dd = 0.0, 0.0
        cum = 0.0
        for t in self.trades:
            cum += t.pnl_rs
            peak = max(peak, cum)
            dd = min(dd, cum - peak)
        return abs(dd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(ROOT / "data" / "hourly_breakout"))
    parser.add_argument("--sl-buffer", type=float, default=2.0)
    parser.add_argument("--min-rr", type=float, default=1.5)
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    if not (data_dir / "spot_NIFTY_1m.csv").exists():
        logger.error("Spot CSV not found at %s", data_dir / "spot_NIFTY_1m.csv")
        return 1
    engine = BacktestEngine(data_dir, sl_buffer=args.sl_buffer, min_rr=args.min_rr)
    engine.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
