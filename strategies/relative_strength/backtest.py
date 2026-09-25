"""
strategies/relative_strength/backtest.py -- RS sign-crossover trading rule,
direct user spec (2026-09-24):

  ENTRY: previous bar's RS (stock vs its own sector index) was negative,
         current bar's RS is positive -> BUY the stock at the current bar's
         close.
  EXIT:  RS goes negative again (any later bar) -> SELL at that bar's close.

Pure backtest logic only -- no network/fetch code here (that lives in
scripts/relative_strength_rs_crossover_backtest.py). Operates on real
aligned (timestamp, stock_close, sector_close) series, same alignment
convention as scan.py's own align_closes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from strategies.relative_strength.detector import compute_rs_series


@dataclass
class Trade:
    symbol: str
    entry_ts: object
    entry_price: float
    exit_ts: object
    exit_price: float
    return_pct: float
    still_open: bool  # True: position never got an RS<0 exit before data ran out


def backtest_rs_crossover(
    symbol: str, timestamps: Sequence, stock_closes: Sequence[float],
    sector_closes: Sequence[float], length: int = 123,
) -> List[Trade]:
    """One symbol's full trade history under the RS sign-crossover rule.
    `timestamps`/`stock_closes`/`sector_closes` must be the SAME length and
    already aligned bar-for-bar (see detector.align_closes_with_ts).

    Only ONE position at a time (a fresh entry signal while already holding
    is ignored -- matches the user's own framing, "buy those stocks and
    exit stocks when RS is -ve", a simple flat/long state machine, not a
    pyramiding one). A position still open when the data ends is included
    as a trade with still_open=True, marked-to-market at the last available
    close -- excluded from win/loss counts by the caller (summarize_trades)
    since it hasn't actually closed, but still shown for visibility."""
    n = len(timestamps)
    if len(stock_closes) != n or len(sector_closes) != n:
        raise ValueError("timestamps/stock_closes/sector_closes must be the same length")

    rs_series = compute_rs_series(stock_closes, sector_closes, length)
    trades: List[Trade] = []
    entry: Optional[dict] = None
    prev_rs: Optional[float] = None

    for i, rs in enumerate(rs_series):
        if rs is None:
            prev_rs = None
            continue
        if entry is None:
            if prev_rs is not None and prev_rs < 0 and rs > 0:
                entry = {"ts": timestamps[i], "price": stock_closes[i]}
        else:
            if rs < 0:
                exit_price = stock_closes[i]
                ret = (exit_price - entry["price"]) / entry["price"]
                trades.append(Trade(
                    symbol=symbol, entry_ts=entry["ts"], entry_price=entry["price"],
                    exit_ts=timestamps[i], exit_price=exit_price, return_pct=ret,
                    still_open=False,
                ))
                entry = None
        prev_rs = rs

    if entry is not None:
        last_price = stock_closes[-1]
        ret = (last_price - entry["price"]) / entry["price"]
        trades.append(Trade(
            symbol=symbol, entry_ts=entry["ts"], entry_price=entry["price"],
            exit_ts=timestamps[-1], exit_price=last_price, return_pct=ret,
            still_open=True,
        ))

    return trades


@dataclass
class BacktestSummary:
    symbol: str
    total_trades: int
    closed_trades: int
    open_trades: int
    wins: int
    losses: int
    win_rate: Optional[float]
    total_return_pct: float
    avg_return_pct: Optional[float]
    best_trade_pct: Optional[float]
    worst_trade_pct: Optional[float]


def summarize_trades(symbol: str, trades: Sequence[Trade]) -> BacktestSummary:
    """Win rate / avg return are computed over CLOSED trades only (an
    open trade hasn't actually realized a result yet); total_return_pct
    sums every trade including the still-open one, since a currently-held
    position's mark-to-market gain/loss is still real for a "how did this
    do overall" reading."""
    closed = [t for t in trades if not t.still_open]
    open_ = [t for t in trades if t.still_open]
    wins = [t for t in closed if t.return_pct > 0]
    losses = [t for t in closed if t.return_pct <= 0]
    returns = [t.return_pct for t in closed]
    return BacktestSummary(
        symbol=symbol,
        total_trades=len(trades),
        closed_trades=len(closed),
        open_trades=len(open_),
        wins=len(wins),
        losses=len(losses),
        win_rate=(len(wins) / len(closed)) if closed else None,
        total_return_pct=sum(t.return_pct for t in trades),
        avg_return_pct=(sum(returns) / len(returns)) if returns else None,
        best_trade_pct=max(returns) if returns else None,
        worst_trade_pct=min(returns) if returns else None,
    )


def aggregate_summaries(summaries: Sequence[BacktestSummary]) -> dict:
    """Portfolio-level rollup across many symbols' individual backtests --
    equal-weighted (every trade counts once, not weighted by symbol) since
    no position-sizing/capital-allocation scheme was specified."""
    total_trades = sum(s.total_trades for s in summaries)
    closed_trades = sum(s.closed_trades for s in summaries)
    open_trades = sum(s.open_trades for s in summaries)
    wins = sum(s.wins for s in summaries)
    losses = sum(s.losses for s in summaries)
    all_returns_sum = sum(s.total_return_pct for s in summaries)
    return {
        "symbols_scanned": len(summaries),
        "symbols_with_trades": len([s for s in summaries if s.total_trades > 0]),
        "total_trades": total_trades,
        "closed_trades": closed_trades,
        "open_trades": open_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / closed_trades) if closed_trades else None,
        "sum_of_all_trade_returns_pct": all_returns_sum,
        "avg_return_per_trade_pct": (all_returns_sum / total_trades) if total_trades else None,
    }
