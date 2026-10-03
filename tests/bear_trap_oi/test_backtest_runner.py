from datetime import datetime, timedelta, timezone
from strategies.bear_trap_oi.models import Bar
from scripts.bear_trap_oi_backtest import run_side_backtest


def _bar(minute, o, h, l, c):
    base = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    return Bar(ts=base + timedelta(minutes=minute),
                open=o, high=h, low=l, close=c)


def test_full_sequence_produces_one_winning_trade_closed_at_eod():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 (breaks c1.low=98) -> zone [90,102]
        _bar(25, 95, 110, 95, 108),    # closes above c1.high=105 -> trap confirmed, armed
        _bar(30, 108, 112, 96, 97),    # dips back inside [90,102] -> entry @ 97 (bar close)
        _bar(315, 97, 150, 97, 145),   # last bar of day -> forced EOD close @ 145
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert len(trades) == 1
    trade = trades[0]
    assert trade.entry_price == 97.0
    assert trade.exit_price == 145.0
    assert trade.pnl == (145.0 - 97.0) * 75


def test_no_trap_ever_confirmed_produces_zero_trades():
    bars = [
        _bar(15, 100, 105, 98, 102),
        _bar(20, 102, 107, 101, 106),  # never breaks c1.low -> c1 rolls forward
        _bar(25, 106, 109, 104, 108),
    ]
    trades = run_side_backtest(bars, side="PE", strike=24700, lot_qty=75)
    assert trades == []


def test_armed_but_never_reentered_produces_zero_trades():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 -> zone [90,102]
        _bar(25, 95, 110, 95, 108),    # trap confirmed, armed, zone [90,102]
        _bar(30, 108, 160, 108, 155),  # never comes back down into [90,102]
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert trades == []
