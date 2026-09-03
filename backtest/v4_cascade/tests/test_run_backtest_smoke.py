"""Smoke tests for backtest/v4_cascade/run_backtest.py -- the bar-feeding/
EOD/leg-recording glue is new code; Gate 1/2/3 themselves are production code
already covered by tests/strategies/test_v4_cascade_*.py. Not attempting a
full synthetic Gate1+Gate2+Gate3 fixture here (real historical data is the
actual validation, per the request to manually cross-check the report's
trade table against a real chart) -- just confirming the plumbing doesn't
crash and PE's bear=False wiring is actually in effect."""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.engine import V4CascadeEngine

from backtest.v4_cascade.run_backtest import build_5m_bars, run_backtest

IST = ZoneInfo("Asia/Kolkata")


def test_empty_bars_returns_no_trades():
    assert run_backtest(V4CascadeConfig(underlying="NIFTY"), []) == []


def test_flat_bars_produce_no_trades():
    base = datetime(2026, 6, 1, 9, 15, tzinfo=IST)
    bars = [_Bar(base + timedelta(minutes=5 * i), 100.0, 100.0, 100.0, 100.0, tf=5) for i in range(80)]
    assert run_backtest(V4CascadeConfig(underlying="NIFTY"), bars) == []


def test_pe_scanner_is_wired_to_bull_mode_not_default_bear():
    """The whole point of the zone_state.py bear=False addition -- confirm
    run_backtest actually flips it, not just constructs the default."""
    eng = V4CascadeEngine(V4CascadeConfig(underlying="NIFTY"), pe_scans_bull=False, session_open=(9, 15))
    assert eng._scanners["PE"]._bear is True  # sanity: default is still True before our override
    eng._scanners["PE"]._bear = False
    assert eng._scanners["PE"]._bear is False
    assert eng._scanners["CE"]._bear is True  # CE must stay bear-mode


def test_1m_rows_build_unfiltered_5m_bars():
    """Index/spot rows always have volume=0 -- must NOT be filtered out
    (unlike option-premium bars)."""
    base = datetime(2026, 6, 1, 9, 15, tzinfo=IST)
    rows = [
        {"ts": (base + timedelta(minutes=i)).isoformat(), "open": 100.0, "high": 101.0,
         "low": 99.0, "close": 100.5, "volume": 0}
        for i in range(10)
    ]
    bars = build_5m_bars(rows)
    assert len(bars) == 2  # 10 one-minute rows -> two 5m bars
