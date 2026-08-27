"""
tests/oi_orb_screener/test_build_stock_chain.py -- regression for
strategies/oi_orb_screener/engine.py's _build_stock_chain() (2026-08-27 fix):
same root cause as the GVT&D PE4350 stock_resolve.py fix -- a synthetic
ATM+/-depth*step window built from the price-band heuristic can include
strikes that were never actually listed on an irregular real grid. Now
prefers REGISTRY.get_available_strikes() when the registry already has real
strikes loaded for this underlying/expiry, falling back to the old
heuristic only when it doesn't.
"""
from datetime import date

from data_layer.instrument_registry import REGISTRY
from strategies.oi_orb_screener.engine import _build_stock_chain


def test_build_stock_chain_uses_real_listed_strikes_when_available(monkeypatch):
    """The exact real incident's shape: spot=4342.77 near a stock whose real
    grid has 4300/4400 (not 4350) -- the chain window must be built from the
    REAL strikes, centered on the nearest real one to spot."""
    monkeypatch.setattr(
        REGISTRY, "get_available_strikes",
        lambda sym, exp: [3900, 4000, 4100, 4200, 4300, 4400, 4500, 4600, 4700],
    )
    mat = _build_stock_chain("GVT&D", spot=4342.77, expiry=date(2026, 9, 29), depth=2)
    assert mat is not None
    snap = mat.snapshot()
    # ATM = nearest real strike to 4342.77 -> 4300 (not a synthetic 4350).
    assert snap.atm_strike == 4300
    # depth=2 real strikes either side of 4300 in the actual grid.
    assert snap.strikes() == [4100, 4200, 4300, 4400, 4500]
    assert 4350 not in snap.strikes()


def test_build_stock_chain_falls_back_to_heuristic_when_no_strikes_loaded(monkeypatch):
    """Defensive fallback -- registry has nothing loaded yet for this
    underlying/expiry, must still build SOME window, not return None."""
    monkeypatch.setattr(REGISTRY, "get_available_strikes", lambda sym, exp: [])
    mat = _build_stock_chain("SOMESTOCK", spot=1234.0, expiry=date(2026, 9, 29), depth=1)
    assert mat is not None
    snap = mat.snapshot()
    # SOMESTOCK isn't in FNO_STOCK_CONFIG -> price-band heuristic, step=20 under Rs2500.
    assert snap.atm_strike == 1240
    assert snap.strikes() == [1220, 1240, 1260]


def test_build_stock_chain_clamps_window_at_the_edge_of_the_real_grid(monkeypatch):
    """If the nearest real strike is near the top/bottom of the loaded
    grid, depth must clamp instead of indexing out of range."""
    monkeypatch.setattr(REGISTRY, "get_available_strikes", lambda sym, exp: [4300, 4400, 4500])
    mat = _build_stock_chain("EDGESTOCK", spot=4500.0, expiry=date(2026, 9, 29), depth=5)
    assert mat is not None
    snap = mat.snapshot()
    assert snap.atm_strike == 4500
    assert snap.strikes() == [4300, 4400, 4500]   # clamped, no IndexError
