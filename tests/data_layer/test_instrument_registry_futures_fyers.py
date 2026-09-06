"""tests/data_layer/test_instrument_registry_futures_fyers.py -- regression for the
2026-08-26 real incident: InstrumentRegistry._resolve_futures_key (the INDEX
underlying path used by NIFTY/SENSEX/etc) only ever populated
self._futures_upstox, never self._futures_fyers.
get_futures_fyers("NIFTY") therefore always returned "" -- not a timing race
that would self-correct, a permanent gap. Confirmed live: UpstoxFeeder
correctly subscribed to the futures key (a real NSE_FO|... instrument, tick
diverging ~177pts from spot as expected for cost-of-carry) while FyersFeeder
fell back to real spot on every single connect, forever -- a standing
mismatch between the primary and standby feed that would have made
self._spot jump instantly on any Fyers failover.

Mocks the master-JSON download by pre-seeding the module-level _MASTER_CACHE
dict directly (same cache _resolve_futures_key itself reads/writes), so no
real network call happens.
"""
from datetime import date

from data_layer.instrument_registry import InstrumentRegistry, _MASTER_CACHE


def _seed_master_cache(exch: str, today: date, instruments: list) -> None:
    _MASTER_CACHE[f"{exch}:{today.isoformat()}"] = instruments


def test_resolve_futures_key_populates_both_upstox_and_fyers_for_nifty():
    reg = InstrumentRegistry()
    today = date(2026, 8, 26)
    _seed_master_cache("NSE", today, [
        {
            "instrument_key": "NSE_FO|99999",
            "trading_symbol": "NIFTY26SEPFUT",
            "strike_price": 0,
            "expiry": "2026-09-24",
            "underlying_symbol": "NIFTY",
            "instrument_type": "FUT",
        },
    ])
    diag = []
    try:
        reg._resolve_futures_key("NIFTY", today, diag)
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|99999"
        assert reg.get_futures_fyers("NIFTY") == "NSE:NIFTY26SEPFUT"
    finally:
        _MASTER_CACHE.pop(f"NSE:{today.isoformat()}", None)


def test_resolve_futures_key_uses_bse_prefix_for_sensex():
    reg = InstrumentRegistry()
    today = date(2026, 8, 26)
    _seed_master_cache("BSE", today, [
        {
            "instrument_key": "BSE_FO|88888",
            "trading_symbol": "SENSEX26SEPFUT",
            "strike_price": 0,
            "expiry": "2026-09-24",
            "underlying_symbol": "SENSEX",
            "instrument_type": "FUT",
        },
    ])
    diag = []
    try:
        reg._resolve_futures_key("SENSEX", today, diag)
        assert reg.get_futures_upstox("SENSEX") == "BSE_FO|88888"
        assert reg.get_futures_fyers("SENSEX") == "BSE:SENSEX26SEPFUT"
    finally:
        _MASTER_CACHE.pop(f"BSE:{today.isoformat()}", None)


def test_resolve_futures_key_picks_near_month_when_multiple_expiries_present():
    reg = InstrumentRegistry()
    today = date(2026, 8, 26)
    _seed_master_cache("NSE", today, [
        {
            "instrument_key": "NSE_FO|22222",
            "trading_symbol": "NIFTY26OCTFUT",
            "strike_price": 0,
            "expiry": "2026-10-29",
            "underlying_symbol": "NIFTY",
            "instrument_type": "FUT",
        },
        {
            "instrument_key": "NSE_FO|99999",
            "trading_symbol": "NIFTY26SEPFUT",
            "strike_price": 0,
            "expiry": "2026-09-24",
            "underlying_symbol": "NIFTY",
            "instrument_type": "FUT",
        },
    ])
    diag = []
    try:
        reg._resolve_futures_key("NIFTY", today, diag)
        # Near-month (Sep) must win over the farther (Oct) contract, for BOTH providers.
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|99999"
        assert reg.get_futures_fyers("NIFTY") == "NSE:NIFTY26SEPFUT"
    finally:
        _MASTER_CACHE.pop(f"NSE:{today.isoformat()}", None)


def test_resolve_futures_key_never_re_resolves_while_cached_contract_still_valid():
    """A second call on a LATER day, while the cached contract hasn't expired
    yet, must be a cheap no-op (no re-scan of the master JSON needed)."""
    reg = InstrumentRegistry()
    day1 = date(2026, 8, 26)
    _seed_master_cache("NSE", day1, [
        {"instrument_key": "NSE_FO|99999", "trading_symbol": "NIFTY26SEPFUT",
         "strike_price": 0, "expiry": "2026-09-24", "underlying_symbol": "NIFTY",
         "instrument_type": "FUT"},
    ])
    try:
        reg._resolve_futures_key("NIFTY", day1, [])
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|99999"

        # A later day, well before the cached contract's own 2026-09-24 expiry --
        # no cache seeded for THIS day, so if it tried to re-resolve it would
        # crash on a missing master-JSON download. Must stay cached instead.
        day2 = date(2026, 9, 1)
        reg._resolve_futures_key("NIFTY", day2, [])
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|99999"
    finally:
        _MASTER_CACHE.pop(f"NSE:{day1.isoformat()}", None)


def test_resolve_futures_key_re_resolves_once_cached_contract_has_expired():
    """2026-08-31 fix -- the real gap: load_sync() is only ever called ONCE
    at process startup (run_system.py), so a long-running process crossing a
    monthly futures rollover with the old `resolve once, cache forever` guard
    would silently keep computing off a dead, expired contract forever. Once
    the cached contract's own expiry has passed, the NEXT call must re-scan
    and pick up the new near-month contract."""
    reg = InstrumentRegistry()
    day1 = date(2026, 8, 26)
    _seed_master_cache("NSE", day1, [
        {"instrument_key": "NSE_FO|99999", "trading_symbol": "NIFTY26SEPFUT",
         "strike_price": 0, "expiry": "2026-09-24", "underlying_symbol": "NIFTY",
         "instrument_type": "FUT"},
    ])
    try:
        reg._resolve_futures_key("NIFTY", day1, [])
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|99999"

        # A day AFTER the Sep contract's own expiry -- must re-resolve to the
        # new near-month (Oct) contract, not keep serving the dead Sep one.
        day2 = date(2026, 9, 25)
        _seed_master_cache("NSE", day2, [
            {"instrument_key": "NSE_FO|22222", "trading_symbol": "NIFTY26OCTFUT",
             "strike_price": 0, "expiry": "2026-10-29", "underlying_symbol": "NIFTY",
             "instrument_type": "FUT"},
        ])
        reg._resolve_futures_key("NIFTY", day2, [])
        assert reg.get_futures_upstox("NIFTY") == "NSE_FO|22222"
        assert reg._futures_expiry["NIFTY"] == date(2026, 10, 29)
    finally:
        _MASTER_CACHE.pop(f"NSE:{day1.isoformat()}", None)
        _MASTER_CACHE.pop(f"NSE:{date(2026, 9, 25).isoformat()}", None)
