"""
Unit tests for strategies/relative_strength/backtest.py -- the RS
sign-crossover entry/exit rule (direct user spec, 2026-09-24: buy when RS
flips negative->positive, exit when RS goes negative again).
"""
from strategies.relative_strength.backtest import (
    backtest_rs_crossover, summarize_trades, aggregate_summaries, Trade,
)


def _series(stock_vals, sector_vals):
    """length=1 for compact, hand-computed test series -- compute_rs_series
    just needs `length` bars of prior history, and the SIGN of RS is what
    this rule cares about, not any specific length value."""
    ts = list(range(len(stock_vals)))
    return ts, stock_vals, sector_vals


def test_no_trade_when_rs_never_crosses():
    # stock flat, sector flat -> RS stays ~0/None the whole way, no crossover.
    ts, stock, sector = _series([100.0] * 10, [100.0] * 10)
    trades = backtest_rs_crossover("X", ts, stock, sector, length=1)
    assert trades == []


def test_entry_fires_on_negative_to_positive_flip_and_exit_on_negative_again():
    # Construct stock/sector so RS (length=1: stock[i]/stock[i-1] / (sector[i]/sector[i-1]) - 1)
    # goes: bar1 neg, bar2 pos (ENTRY here), bar3 pos (hold), bar4 neg (EXIT here).
    stock =  [100.0, 95.0, 110.0, 115.0, 100.0]
    sector = [100.0, 100.0, 100.0, 100.0, 100.0]
    # bar1: (95/100)/(100/100)-1 = -0.05 (neg)
    # bar2: (110/95)/(100/100)-1 = +0.158 (pos) -> ENTRY at stock[2]=110.0
    # bar3: (115/110)/(100/100)-1 = +0.045 (pos) -> still holding
    # bar4: (100/115)/(100/100)-1 = -0.130 (neg) -> EXIT at stock[4]=100.0
    ts, stock_c, sector_c = _series(stock, sector)
    trades = backtest_rs_crossover("X", ts, stock_c, sector_c, length=1)
    assert len(trades) == 1
    t = trades[0]
    assert t.entry_price == 110.0
    assert t.exit_price == 100.0
    assert t.still_open is False
    assert abs(t.return_pct - ((100.0 - 110.0) / 110.0)) < 1e-9


def test_position_stays_open_at_end_of_data_marked_still_open():
    stock =  [100.0, 95.0, 110.0, 120.0]
    sector = [100.0, 100.0, 100.0, 100.0]
    # bar1 neg, bar2 pos -> entry @110; bar3 stays positive -> never exits.
    ts, stock_c, sector_c = _series(stock, sector)
    trades = backtest_rs_crossover("X", ts, stock_c, sector_c, length=1)
    assert len(trades) == 1
    assert trades[0].still_open is True
    assert trades[0].entry_price == 110.0
    assert trades[0].exit_price == 120.0  # marked at last available close


def test_no_reentry_while_already_holding_a_position():
    # RS goes neg -> pos (entry) -> pos -> pos (another "still positive" bar,
    # must NOT re-enter) -> neg (exit). Only one trade should result.
    stock =  [100.0, 95.0, 110.0, 130.0, 150.0, 100.0]
    sector = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    ts, stock_c, sector_c = _series(stock, sector)
    trades = backtest_rs_crossover("X", ts, stock_c, sector_c, length=1)
    assert len(trades) == 1


def test_multiple_round_trips_produce_multiple_trades():
    # neg,pos(entry@B),neg(exit@C),neg,pos(entry@E),neg(exit@F)
    stock =  [100.0, 95.0, 110.0, 90.0, 85.0, 100.0, 80.0]
    sector = [100.0] * 7
    ts, stock_c, sector_c = _series(stock, sector)
    trades = backtest_rs_crossover("X", ts, stock_c, sector_c, length=1)
    assert len(trades) == 2
    assert all(not t.still_open for t in trades)


def test_length_mismatch_raises():
    import pytest
    with pytest.raises(ValueError):
        backtest_rs_crossover("X", [0, 1, 2], [1.0, 2.0], [1.0, 2.0, 3.0], length=1)


def test_summarize_trades_win_rate_and_return_excludes_open_from_win_loss():
    trades = [
        Trade("X", 0, 100.0, 1, 110.0, 0.10, still_open=False),   # win
        Trade("X", 2, 100.0, 3, 90.0, -0.10, still_open=False),   # loss
        Trade("X", 4, 100.0, 5, 105.0, 0.05, still_open=True),    # open, excluded from win/loss
    ]
    s = summarize_trades("X", trades)
    assert s.total_trades == 3
    assert s.closed_trades == 2
    assert s.open_trades == 1
    assert s.wins == 1
    assert s.losses == 1
    assert abs(s.win_rate - 0.5) < 1e-9
    # total_return_pct includes the open trade's mark-to-market too.
    assert abs(s.total_return_pct - (0.10 - 0.10 + 0.05)) < 1e-9
    assert abs(s.avg_return_pct - 0.0) < 1e-9  # avg over CLOSED trades only: (0.10-0.10)/2


def test_summarize_trades_no_trades_gives_none_win_rate():
    s = summarize_trades("X", [])
    assert s.total_trades == 0
    assert s.win_rate is None
    assert s.avg_return_pct is None
    assert s.total_return_pct == 0.0


def test_aggregate_summaries_rolls_up_across_symbols():
    a = summarize_trades("A", [
        Trade("A", 0, 100.0, 1, 110.0, 0.10, still_open=False),
        Trade("A", 2, 100.0, 3, 95.0, -0.05, still_open=False),
    ])
    b = summarize_trades("B", [
        Trade("B", 0, 50.0, 1, 55.0, 0.10, still_open=False),
    ])
    c = summarize_trades("C", [])  # no trades at all for this symbol
    agg = aggregate_summaries([a, b, c])
    assert agg["symbols_scanned"] == 3
    assert agg["symbols_with_trades"] == 2
    assert agg["total_trades"] == 3
    assert agg["closed_trades"] == 3
    assert agg["wins"] == 2
    assert agg["losses"] == 1
    assert abs(agg["win_rate"] - (2 / 3)) < 1e-9
    assert abs(agg["sum_of_all_trade_returns_pct"] - (0.10 - 0.05 + 0.10)) < 1e-9
