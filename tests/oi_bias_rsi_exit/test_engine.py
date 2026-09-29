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
from datetime import time as dtime
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.oi_bias_rsi_exit.engine import OiBiasRsiExitStrategy


def _strategy():
    return OiBiasRsiExitStrategy(EventBus(), GlobalConfig(), client_id="C", binding_id="B")


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
