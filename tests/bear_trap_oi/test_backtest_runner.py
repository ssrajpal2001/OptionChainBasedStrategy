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


def test_entry_requires_bar_close_inside_zone_not_just_range_overlap():
    # A bar whose RANGE touches the zone but whose CLOSE is outside it must
    # NOT fire an entry -- the live engine's check_zone_reentry() tests the
    # live price (here, the bar's close), not whether the bar merely swept
    # through the zone. A later bar that actually CLOSES inside the zone
    # must fire at that close.
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 -> zone [90, 102]
        _bar(25, 95, 110, 95, 108),    # trap confirmed, armed, zone [90,102]
        _bar(30, 108, 112, 96, 109),   # range touches zone (low=96<=102) but CLOSES at 109, outside -> no entry
        _bar(35, 109, 109, 97, 100),   # closes at 100, inside [90,102] -> entry @ 100
        _bar(315, 100, 150, 100, 145),  # EOD force close
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert len(trades) == 1
    assert trades[0].entry_price == 100.0


def test_run_side_backtest_emits_verbose_audit_log_when_logger_given():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 breakdown
        _bar(25, 95, 110, 95, 108),    # trap confirmed
        _bar(30, 108, 112, 96, 97),    # re-entry @ 97
        _bar(315, 97, 150, 97, 145),   # EOD close @ 145
    ]
    log: list[str] = []
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75,
                                logger=log.append)
    assert len(trades) == 1
    trade = trades[0]
    assert trade.pnl_pct == ((145.0 - 97.0) / 97.0) * 100

    joined = "\n".join(log)
    assert "C1" in joined and "102" in joined  # c1 close logged
    assert "C2" in joined or "BREAKDOWN" in joined
    assert "90" in joined  # zone_lo logged
    assert "TRAP" in joined or "CONFIRM" in joined
    assert "ZONE" in joined and "[90" in joined  # zone range printed
    assert "ENTRY" in joined and "97" in joined
    assert "EXIT" in joined and "145" in joined
    # state transitions must be in chronological order in the log
    assert log.index([l for l in log if "C1" in l][0]) < \
           log.index([l for l in log if "ENTRY" in l][0])


def test_armed_but_never_reentered_produces_zero_trades():
    bars = [
        _bar(15, 100, 105, 98, 102),   # c1
        _bar(20, 97, 99, 90, 94),      # c2 -> zone [90,102]
        _bar(25, 95, 110, 95, 108),    # trap confirmed, armed, zone [90,102]
        _bar(30, 108, 160, 108, 155),  # never comes back down into [90,102]
    ]
    trades = run_side_backtest(bars, side="CE", strike=24500, lot_qty=75)
    assert trades == []
