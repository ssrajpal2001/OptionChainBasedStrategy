"""Regression test for the 2026-09-29 CRITICAL FIX, real incident: a thin OTM
leg can genuinely have no reported OI in Upstox's own intraday candle
response for its first several minutes. The old _oi_at_snapshots only had a
single hardcoded 09:15->09:16 fallback, with none at all for 09:20/09:25, so
a leg missing OI at those points permanently classified the stock's bias as
"none" even though a real, carried-forward OI value existed moments earlier
(confirmed live via a real backtest of JUBLFOOD's 2026-09-29 data: its OTM
CE485 leg had zero reported OI at both 09:15 and 09:20).

Fixed by treating OI as a snapshot LEVEL: the read for "OI at 09:20" is now
the most recent REAL (>0) reading at-or-before 09:20, not an exact-minute
match, applied uniformly to all three snapshot times."""
import asyncio
from datetime import datetime, time as dtime, timedelta
from unittest.mock import AsyncMock, patch

from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.oi_bias_breakout.detector import SignalStrikes
from strategies.oi_bias_rsi_exit.engine import OiBiasRsiExitStrategy, OI_RECHECK_MINUTES


def _strategy():
    return OiBiasRsiExitStrategy(EventBus(), GlobalConfig(), client_id="C", binding_id="B")


def test_entry_exit_and_oi_recheck_params_are_genuinely_overridable_per_deployment():
    """2026-09-30 CRITICAL FIX, direct user audit request: these used to be
    module-level constants in engine.py with ZERO per-deployment override --
    confirms the constructor now genuinely accepts and stores different
    values than the module defaults, not just re-reading the same constant
    under a new name."""
    s = OiBiasRsiExitStrategy(
        EventBus(), GlobalConfig(), client_id="C", binding_id="B",
        entry_timeframe_min=5, entry_stoch_rsi_lengths=(14, 14, 3, 3),
        exit_timeframe_min=30, exit_stoch_rsi_lengths=(9, 9, 3, 3),
        oi_recheck_minutes=10, oi_bias_flip_count=3,
    )
    assert s._entry_timeframe_min == 5
    assert s._entry_stoch_rsi_lengths == (14, 14, 3, 3)
    assert s._exit_timeframe_min == 30
    assert s._exit_stoch_rsi_lengths == (9, 9, 3, 3)
    assert s._oi_recheck_minutes == 10
    assert s._oi_bias_flip_count == 3


class _FakeContract:
    upstox_key = "NSE_FO|999999"


def _bars(rows):
    """rows: list of (HH, MM, oi) -> Upstox-shaped candle dicts."""
    out = []
    for hh, mm, oi in rows:
        out.append({
            "ts": f"2026-09-29T{hh:02d}:{mm:02d}:00+05:30",
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "volume": 100, "oi": oi,
        })
    return out


def test_missing_oi_at_920_and_925_forward_fills_from_last_real_reading():
    """The exact JUBLFOOD incident shape: real OI at 09:15, then NOTHING
    (zero/no data) reported again until 09:26 -- 09:20 and 09:25 must both
    resolve to the 09:15 value via forward-fill, not None."""
    s = _strategy()
    rows = _bars([
        (9, 15, 200.0),
        (9, 16, 0), (9, 17, 0), (9, 18, 0), (9, 19, 0),
        (9, 20, 0), (9, 21, 0), (9, 22, 0), (9, 23, 0), (9, 24, 0), (9, 25, 0),
        (9, 26, 250.0),
    ])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("JUBLFOOD", 485, "CE", "tok"))

    assert out[dtime(9, 15)] == 200.0
    assert out[dtime(9, 20)] == 200.0, "must forward-fill from the last real reading, not return None"
    assert out[dtime(9, 25)] == 200.0


def test_no_real_oi_before_915_returns_none():
    """A key with genuinely zero OI anywhere at or before 09:15 (its very
    first trade hasn't happened yet) must stay None -- never fabricate a
    value from a later reading."""
    s = _strategy()
    rows = _bars([(9, 15, 0), (9, 16, 0), (9, 20, 150.0), (9, 25, 180.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("XYZ", 100, "PE", "tok"))

    assert out[dtime(9, 15)] is None
    assert out[dtime(9, 20)] == 150.0
    assert out[dtime(9, 25)] == 180.0


def test_exact_minute_readings_used_when_present():
    """Baseline: when every snapshot minute has a genuine real reading,
    each resolves to its own exact value (unchanged behavior)."""
    s = _strategy()
    rows = _bars([(9, 15, 100.0), (9, 20, 120.0), (9, 25, 140.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("ABC", 100, "CE", "tok"))

    assert out[dtime(9, 15)] == 100.0
    assert out[dtime(9, 20)] == 120.0
    assert out[dtime(9, 25)] == 140.0


# ── Third exit condition: OI bias flips to the opposite direction, twice ───

def _pos(entry_bias_strikes=None, next_oi_check=None, prev_oi=None, history=None):
    return {
        "strikes": entry_bias_strikes or SignalStrikes(atm=100, otm_call=110, otm_put=90),
        "prev_oi": prev_oi or {"atm_call": 1000.0, "otm_call": 1000.0, "atm_put": 1000.0, "otm_put": 1000.0},
        "oi_bias_history": history if history is not None else [],
        "next_oi_check": next_oi_check or datetime.now(IST) - timedelta(seconds=1),
        "db_row_id": 1, "upstox_key": "NSE_FO|1", "entry_price": 10.0,
        "option_type": "CE", "strike": 100, "expiry": "2026-10-06", "qty": 75,
        "entry_ts": datetime.now(IST) - timedelta(minutes=30),
    }


def test_oi_flip_does_not_recheck_before_the_interval_elapses():
    """Re-check must not fire before next_oi_check -- avoids hammering REST
    every 60s poll cycle when the real cadence is every 5 minutes."""
    s = _strategy()
    pos = _pos(next_oi_check=datetime.now(IST) + timedelta(minutes=4))
    s._current_oi = AsyncMock(return_value=1000.0)
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bullish", "tok"))
    assert fired is False
    s._current_oi.assert_not_awaited()
    assert pos["oi_bias_history"] == []


def test_oi_flip_records_history_but_does_not_exit_on_a_single_opposite_reading():
    s = _strategy()
    pos = _pos()
    # OTM Call falls, ATM Put rises -- bullish per classify_oi_bias -- the
    # OPPOSITE of an entered "bearish" position -- but only ONE reading so far.
    s._current_oi = AsyncMock(side_effect=[900.0, 800.0, 1200.0, 1000.0])  # atm_call, otm_call, atm_put, otm_put
    s._close_position = AsyncMock()
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bearish", "tok"))
    assert fired is False
    assert pos["oi_bias_history"] == ["bullish"]
    s._close_position.assert_not_awaited()
    # next_oi_check advanced and prev_oi updated for the next re-check.
    assert pos["next_oi_check"] > datetime.now(IST)
    assert pos["prev_oi"] == {"atm_call": 900.0, "otm_call": 800.0, "atm_put": 1200.0, "otm_put": 1000.0}


def test_oi_flip_exits_after_two_opposite_readings_not_necessarily_consecutive():
    s = _strategy()
    pos = _pos(history=["none", "bullish"])  # already one opposite reading for a "bearish" entry
    s._current_oi = AsyncMock(side_effect=[900.0, 800.0, 1200.0, 1000.0])  # -> "bullish" again
    s._close_position = AsyncMock()
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bearish", "tok"))
    assert fired is True
    assert pos["oi_bias_history"] == ["none", "bullish", "bullish"]
    s._close_position.assert_awaited_once_with("SYM", "oi_bias_flip_twice", "tok")


def test_check_exit_skips_stoch_rsi_check_when_oi_flip_already_closed_position():
    """Once the OI-flip exit has fired and closed the position, _check_exit
    must not also run the StochRSI crossover check against a now-closed
    position."""
    s = _strategy()
    pos = _pos(history=["none", "bearish"])
    s._positions = {"SYM": pos}
    s._bias = {"SYM": "bullish"}
    s._current_oi = AsyncMock(side_effect=[1200.0, 1000.0, 1000.0, 800.0])  # atm_call,otm_call,atm_put,otm_put -> "bearish"

    async def _fake_close(symbol, reason, token):
        del s._positions[symbol]
    s._close_position = AsyncMock(side_effect=_fake_close)

    from unittest.mock import Mock
    fetch_mock = AsyncMock()
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m", fetch_mock):
        asyncio.run(s._check_exit("SYM", "tok"))

    s._close_position.assert_awaited_once_with("SYM", "oi_bias_flip_twice", "tok")
    fetch_mock.assert_not_awaited()  # never reached the StochRSI bar-fetch path
