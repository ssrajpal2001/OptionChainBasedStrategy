"""
tests/data_layer/test_instrument_registry_available_strikes.py -- regression
for InstrumentRegistry.get_available_strikes() (2026-08-27), added after a
real incident: GVT&D's PE entry failed with "no upstox_key resolved for
GVT&D PE4350" because stock_resolve.py's price-band strike-step heuristic
assumed a flat 50pt grid, but GVT&D's real listed grid switches to 100pt
around that price level. This exposes the ACTUAL listed strikes already
loaded in self._upstox_keys so a caller can snap to a real strike instead
of guessing a step.
"""
from datetime import date

import pytest

from data_layer.instrument_registry import REGISTRY


@pytest.fixture(autouse=True)
def _isolate_upstox_keys():
    """REGISTRY is a process-wide singleton -- never leak a test fixture's
    fake strikes into another test file that reads real registry state."""
    original = REGISTRY._upstox_keys.get("TESTSTOCK")
    yield
    if original is None:
        REGISTRY._upstox_keys.pop("TESTSTOCK", None)
    else:
        REGISTRY._upstox_keys["TESTSTOCK"] = original


def _seed(expiry_iso: str, strike: int, opt_type: str, key: str) -> None:
    REGISTRY._upstox_keys.setdefault("TESTSTOCK", {})[(expiry_iso, strike, opt_type)] = key


def test_get_available_strikes_returns_sorted_distinct_strikes_for_the_expiry():
    exp = date(2026, 9, 29)
    _seed("2026-09-29", 4400, "CE", "NSE_FO|1")
    _seed("2026-09-29", 4300, "CE", "NSE_FO|2")
    _seed("2026-09-29", 4300, "PE", "NSE_FO|3")
    _seed("2026-10-27", 4350, "CE", "NSE_FO|4")   # different expiry -- must NOT appear

    strikes = REGISTRY.get_available_strikes("TESTSTOCK", exp)
    assert strikes == [4300, 4400]


def test_get_available_strikes_filters_by_option_type_when_given():
    exp = date(2026, 9, 29)
    _seed("2026-09-29", 4300, "CE", "NSE_FO|1")
    _seed("2026-09-29", 4400, "PE", "NSE_FO|2")

    assert REGISTRY.get_available_strikes("TESTSTOCK", exp, "CE") == [4300]
    assert REGISTRY.get_available_strikes("TESTSTOCK", exp, "PE") == [4400]
    assert REGISTRY.get_available_strikes("TESTSTOCK", exp) == [4300, 4400]


def test_get_available_strikes_empty_for_unloaded_underlying():
    assert REGISTRY.get_available_strikes("NEVERLOADEDSTOCK", date(2026, 9, 29)) == []


def test_get_available_strikes_case_insensitive_underlying():
    exp = date(2026, 9, 29)
    _seed("2026-09-29", 4300, "CE", "NSE_FO|1")
    assert REGISTRY.get_available_strikes("teststock", exp) == [4300]
