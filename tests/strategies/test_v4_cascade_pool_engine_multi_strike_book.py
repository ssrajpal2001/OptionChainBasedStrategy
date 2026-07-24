"""strategies/v4_cascade/book.py -- multi-strike candidate resolution for
the pool engine (Task 3 of the multi-strike-scan plan). Exercises
_resolve_symbols'/_maybe_recenter_tracking_strikes'-adjacent strike-list-
building logic directly via a constructed V4CascadeBook, without touching
the network (REGISTRY.get_upstox_key is monkeypatched)."""
from datetime import date

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
import strategies.v4_cascade.book as book_mod


def _book(tracking_offsets_pts=None, locked_ce=None, locked_pe=None):
    b = V4CascadeBook(
        EventBus(), GlobalConfig(), underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=2, use_pool_engine=True, tracking_offsets_pts=tracking_offsets_pts,
    )
    if locked_ce is not None or locked_pe is not None:
        b.set_locked_strikes(locked_ce, locked_pe)
    return b


def _fake_get_upstox_key(underlying, expiry, strike, opt_type):
    return f"NSE_FO|{underlying}{expiry}{int(strike)}{opt_type}"


@pytest.mark.asyncio
async def test_default_offsets_produce_single_candidate_each_side(monkeypatch):
    """tracking_offsets_pts=None (env var unset) must produce EXACTLY today's
    single-strike behavior: one CE strike, one PE strike, both equal to the
    existing scalar self._ce_strike/self._pe_strike."""
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=None)
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=23700.0)
    assert b._ce_strikes == [23500]
    assert b._pe_strikes == [23900]
    assert b._ce_strike == 23500
    assert b._pe_strike == 23900


@pytest.mark.asyncio
async def test_multi_offsets_produce_five_candidates_each_side(monkeypatch):
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0])
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=24100.0)
    assert b._ce_strikes == [24000, 23900, 23800, 23700, 23600]
    assert b._pe_strikes == [24200, 24300, 24400, 24500, 24600]
    assert len(b._ce_symbols) == 5 and len(b._pe_symbols) == 5
    assert b._ce_strike == 24000  # first candidate, for dashboard backward-compat
    assert b._pe_strike == 24200


@pytest.mark.asyncio
async def test_locked_strike_collapses_to_single_candidate(monkeypatch):
    """Admin manual override (set_locked_strikes) must still work under
    multi-strike -- locking a side collapses its candidate list to exactly
    that one strike, ignoring tracking_offsets_pts entirely for that side."""
    monkeypatch.setattr(book_mod.REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    b = _book(tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0],
              locked_ce=23850, locked_pe=None)
    b._expiry = date(2026, 7, 28)
    b._build_candidate_strikes(atm_open=24100.0)
    assert b._ce_strikes == [23850]
    assert b._pe_strikes == [24200, 24300, 24400, 24500, 24600]
