"""
2026-08-24: unit tests for strategies/oi_orb_screener/stock_resolve.py --
resolving broker symbol/lot/strike-step/expiry for an arbitrary F&O stock
the screener shortlists on a given day (the fast FNO_STOCK_CONFIG path, the
Upstox-master lot fallback, and resolve_contract's load-only-if-not-loaded
behavior against the real InstrumentRegistry singleton).
"""
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
    monkeypatch.setattr(stock_resolve, "_load_upstox_lot_cache", _fake_load)

    lot = stock_resolve.resolve_lot("MANAPPURAM")
    assert lot == 6900
    assert calls["n"] == 1


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
    captured = {}
    def _fake_get_upstox_key(sym, exp, strike, opt):
        captured["strike"] = strike
        return "NSE_FO|1"
    monkeypatch.setattr(REGISTRY, "get_upstox_key", _fake_get_upstox_key)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: "SYM")

    # RELIANCE (FNO_STOCK_CONFIG step=20) at 1234.0 -> rounds to 1240.
    stock_resolve.resolve_contract("RELIANCE", 1234.0, "CE")
    assert captured["strike"] == 1240
