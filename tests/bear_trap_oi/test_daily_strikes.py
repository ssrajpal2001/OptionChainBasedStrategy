from datetime import date
from scripts.bear_trap_oi_backtest import compute_daily_strikes


def _daily(d, high, low):
    return {"ts": f"{d.isoformat()}T00:00:00+05:30", "high": high, "low": low,
            "open": low, "close": high, "volume": 0, "oi": 0}


def test_each_trading_day_gets_strikes_from_its_own_preceding_day():
    daily = [
        _daily(date(2026, 9, 28), high=24680.0, low=24510.0),  # Mon (PDH/PDL source for Tue)
        _daily(date(2026, 9, 29), high=24720.0, low=24550.0),  # Tue (source for Wed)
        _daily(date(2026, 9, 30), high=24400.0, low=24150.0),  # Wed (source for Thu)
        _daily(date(2026, 10, 1), high=24990.0, low=24800.0),  # Thu (trades on this day, not a source)
    ]
    result = compute_daily_strikes(daily, step=50)
    # 3 tradeable days: Tue, Wed, Thu -- each keyed by its OWN preceding day's PDH/PDL
    assert result == [
        (date(2026, 9, 29), 24500, 24700),   # from Mon's 24510/24680
        (date(2026, 9, 30), 24550, 24700),   # from Tue's 24550/24720
        (date(2026, 10, 1), 24150, 24400),   # from Wed's 24150/24400
    ]


def test_fewer_than_two_daily_candles_yields_no_tradeable_days():
    daily = [_daily(date(2026, 9, 28), high=24680.0, low=24510.0)]
    assert compute_daily_strikes(daily, step=50) == []
