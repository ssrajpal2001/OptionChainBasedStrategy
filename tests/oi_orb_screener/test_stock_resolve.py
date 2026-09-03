"""
2026-08-24: unit tests for strategies/oi_orb_screener/stock_resolve.py --
resolving broker symbol/lot/strike-step/expiry for an arbitrary F&O stock
the screener shortlists on a given day (the fast FNO_STOCK_CONFIG path, the
Upstox-master lot fallback, and resolve_contract's load-only-if-not-loaded
behavior against the real InstrumentRegistry singleton).
"""
import threading
import time
from datetime import date

import pytest

from data_layer.instrument_registry import REGISTRY
from strategies.oi_orb_screener import stock_resolve


# ── resolve_lot / resolve_strike_step_for_price -- FNO_STOCK_CONFIG fast path ──

def test_resolve_lot_fast_path_curated_stock():
    # RELIANCE is a real curated entry in FNO_STOCK_CONFIG (lot=500).
    assert stock_resolve.resolve_lot("RELIANCE") == 500
    assert stock_resolve.resolve_lot("reliance") == 500   # case-insensitive


def test_resolve_strike_step_fast_path_curated_stock():
    assert stock_resolve.resolve_strike_step_for_price("RELIANCE", 1234.0) == 20.0


# ── fallback path -- stock NOT in FNO_STOCK_CONFIG ───────────────────────────

def test_resolve_lot_fallback_uses_upstox_master_cache(monkeypatch):
    monkeypatch.setattr(stock_resolve, "_lot_cache_loaded", True)
    monkeypatch.setattr(stock_resolve, "_lot_cache", {"MANAPPURAM": 6900})
    assert stock_resolve.resolve_lot("MANAPPURAM") == 6900


def test_resolve_lot_fallback_unresolvable_returns_zero(monkeypatch):
    monkeypatch.setattr(stock_resolve, "_lot_cache_loaded", True)
    monkeypatch.setattr(stock_resolve, "_lot_cache", {})
    assert stock_resolve.resolve_lot("SOMEUNKNOWNSTOCK") == 0


def test_resolve_lot_fallback_triggers_one_shot_load(monkeypatch):
    calls = {"n": 0}
    def _fake_load():
        calls["n"] += 1
        stock_resolve._lot_cache["MANAPPURAM"] = 6900
        stock_resolve._lot_cache_loaded = True
    monkeypatch.setattr(stock_resolve, "_lot_cache_loaded", False)
    monkeypatch.setattr(stock_resolve, "_lot_cache", {})
    monkeypatch.setattr(stock_resolve, "_load_upstox_lot_cache_locked", _fake_load)

    lot = stock_resolve.resolve_lot("MANAPPURAM")
    assert lot == 6900
    assert calls["n"] == 1


def test_resolve_lot_concurrent_calls_do_not_race(monkeypatch):
    """2026-08-24 CRITICAL fix, confirmed live: 5 signals firing in the same
    batch (real OS threads via asyncio.to_thread) used to race on
    _lot_cache_loaded -- the first thread claimed "loaded" before its own
    fetch even started, so the other 4 threads read the still-empty cache
    and got 0 ("could not resolve lot size") for stocks that were never
    actually unresolvable. Drives real threading.Thread objects (not
    asyncio -- the real bug is a thread race, not an event-loop one) at a
    mocked SLOW fetch to force the overlap, and asserts every concurrent
    caller gets the real resolved value, none of them 0."""
    calls = {"n": 0}

    def _slow_fake_load():
        calls["n"] += 1
        time.sleep(0.2)   # force genuine overlap with the other waiting threads
        stock_resolve._lot_cache.update({
            "BANKBARODA": 5307, "CROMPTON": 1000, "CANBK": 4875, "HAL": 150, "DIXON": 350,
        })
        stock_resolve._lot_cache_loaded = True

    monkeypatch.setattr(stock_resolve, "_lot_cache_loaded", False)
    monkeypatch.setattr(stock_resolve, "_lot_cache", {})
    monkeypatch.setattr(stock_resolve, "_load_upstox_lot_cache_locked", _slow_fake_load)

    symbols = ["BANKBARODA", "CROMPTON", "CANBK", "HAL", "DIXON"]
    results: dict = {}

    def _worker(sym):
        results[sym] = stock_resolve.resolve_lot(sym)

    threads = [threading.Thread(target=_worker, args=(s,)) for s in symbols]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert calls["n"] == 1, "the slow fetch must only actually run ONCE, not once per racing thread"
    assert results == {"BANKBARODA": 5307, "CROMPTON": 1000, "CANBK": 4875, "HAL": 150, "DIXON": 350}, (
        "every concurrent caller must get the real resolved lot, none of them the raced 0"
    )


def test_resolve_strike_step_fallback_uses_price_band_heuristic():
    # SIEMENS (not in FNO_STOCK_CONFIG) at ~4041 -> price-band heuristic step=50.
    assert stock_resolve.resolve_strike_step_for_price("SIEMENS", 4041.20) == 50.0
    # MANAPPURAM (not in FNO_STOCK_CONFIG) at ~365 -> 250<=price<500 band -> step=5.
    assert stock_resolve.resolve_strike_step_for_price("MANAPPURAM", 365.0) == 5.0


# ── resolve_contract -- load_sync only when not already loaded ──────────────

def test_resolve_contract_calls_load_sync_only_if_not_loaded(monkeypatch):
    calls = {"load_sync": 0}
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: False)
    monkeypatch.setattr(REGISTRY, "load_sync", lambda sym, **kw: calls.__setitem__("load_sync", calls["load_sync"] + 1))
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 8, 27))
    monkeypatch.setattr(REGISTRY, "get_upstox_key", lambda sym, exp, strike, opt: "NSE_FO|12345")
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: f"{sym}CE")

    contract = stock_resolve.resolve_contract("MANAPPURAM", 365.25, "CE")

    assert calls["load_sync"] == 1
    assert contract is not None
    assert contract.underlying == "MANAPPURAM"
    assert contract.upstox_key == "NSE_FO|12345"
    assert contract.expiry == date(2026, 8, 27)


def test_resolve_contract_skips_load_sync_when_already_loaded(monkeypatch):
    calls = {"load_sync": 0}
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "load_sync", lambda sym, **kw: calls.__setitem__("load_sync", calls["load_sync"] + 1))
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 8, 27))
    monkeypatch.setattr(REGISTRY, "get_upstox_key", lambda sym, exp, strike, opt: "NSE_FO|12345")
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: f"{sym}CE")

    stock_resolve.resolve_contract("MANAPPURAM", 365.25, "CE")

    assert calls["load_sync"] == 0


def test_resolve_contract_returns_none_when_no_active_expiry(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: None)

    contract = stock_resolve.resolve_contract("DELISTEDSTOCK", 100.0, "CE")
    assert contract is None


def test_resolve_contract_returns_none_when_no_upstox_key(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 8, 27))
    monkeypatch.setattr(REGISTRY, "get_upstox_key", lambda sym, exp, strike, opt: "")

    contract = stock_resolve.resolve_contract("MANAPPURAM", 365.25, "CE")
    assert contract is None


def test_resolve_contract_rounds_strike_to_step(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 8, 27))
    # No real strikes loaded for this underlying/expiry -- falls back to the
    # price-band/FNO_STOCK_CONFIG heuristic, same as before this was added.
    # Explicit (not relying on REGISTRY's real, incidental empty state) so
    # this test can't silently start disagreeing with itself if some OTHER
    # test in the same session happens to load real RELIANCE strikes first.
    monkeypatch.setattr(REGISTRY, "get_available_strikes", lambda sym, exp, opt: [])
    captured = {}
    def _fake_get_upstox_key(sym, exp, strike, opt):
        captured["strike"] = strike
        return "NSE_FO|1"
    monkeypatch.setattr(REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: "SYM")

    # RELIANCE (FNO_STOCK_CONFIG step=20) at 1234.0 -> rounds to 1240.
    stock_resolve.resolve_contract("RELIANCE", 1234.0, "CE")
    assert captured["strike"] == 1240


# ── 2026-08-27 CRITICAL fix: snap to the nearest REAL listed strike ─────────
# Real incident: GVT&D PE entry failed with "no upstox_key resolved for
# GVT&D PE4350" -- the price-band heuristic assumed a flat 50pt grid for
# anything under Rs5000, but GVT&D's real listed grid switches to 100pt
# around that price level (4300/4400 real, 4350 never listed at all).
# Verified generic (not a GVT&D-only patch) -- any stock whose real grid
# doesn't match the heuristic's price-band assumption is fixed the same way.

def test_resolve_contract_snaps_to_nearest_real_strike_when_available(monkeypatch):
    """The exact real incident: raw_strike=4342.77 rounds to 4350 under the
    old heuristic (never listed); with real strikes loaded, it must snap to
    the nearest one that's ACTUALLY listed (4300 or 4400, not 4350)."""
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 9, 29))
    monkeypatch.setattr(
        REGISTRY, "get_available_strikes",
        lambda sym, exp, opt: [3900, 4000, 4100, 4200, 4300, 4400, 4500] if sym == "GVT&D" else [],
    )
    captured = {}
    def _fake_get_upstox_key(sym, exp, strike, opt):
        captured["strike"] = strike
        return "NSE_FO|107736" if strike == 4400 else "NSE_FO|999"
    monkeypatch.setattr(REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: "SYM")

    contract = stock_resolve.resolve_contract("GVT&D", 4342.77, "PE")
    assert captured["strike"] == 4300   # nearest real strike to 4342.77 -- NOT 4350
    assert contract is not None
    assert contract.strike == 4300


def test_resolve_contract_falls_back_to_heuristic_when_no_strikes_loaded(monkeypatch):
    """Defensive fallback: an underlying/expiry the registry has no strikes
    loaded for yet must still fall back to the old heuristic, never crash
    or silently return no contract when the heuristic would have worked."""
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 8, 27))
    monkeypatch.setattr(REGISTRY, "get_available_strikes", lambda sym, exp, opt: [])
    captured = {}
    def _fake_get_upstox_key(sym, exp, strike, opt):
        captured["strike"] = strike
        return "NSE_FO|1"
    monkeypatch.setattr(REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: "SYM")

    stock_resolve.resolve_contract("GVT&D", 4342.77, "PE")
    # No real strikes loaded -> old price-band heuristic (step=50 under Rs5000).
    assert captured["strike"] == 4350


def test_resolve_contract_strike_snapping_is_generic_not_gvtd_specific(monkeypatch):
    """Same mechanic, a completely different (non-curated, non-GVT&D) stock --
    proves this isn't a special case hardcoded for one symbol."""
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_active_expiry", lambda sym, **kw: date(2026, 9, 29))
    monkeypatch.setattr(
        REGISTRY, "get_available_strikes",
        lambda sym, exp, opt: [1180, 1200, 1220, 1260, 1300] if sym == "SOMESTOCK" else [],
    )
    captured = {}
    def _fake_get_upstox_key(sym, exp, strike, opt):
        captured["strike"] = strike
        return "NSE_FO|555"
    monkeypatch.setattr(REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: "SYM")

    # raw_strike=1234 -> old heuristic (step=20 under Rs2500) would round to
    # 1240, which ISN'T in this stock's real (irregular, partly 20pt/40pt)
    # grid -- must snap to the nearest REAL one (1220) instead.
    stock_resolve.resolve_contract("SOMESTOCK", 1234.0, "CE")
    assert captured["strike"] == 1220
