"""Regression test for the 2026-09-30 direct user request: fetch_fno_price_
universe/fetch_oi_spurts_nse/fetch_nifty_pchange are real NSE HTTP calls for
market-WIDE data (not per-client, not per-binding), but every book
(oi_orb_screener, oi_orb_screener_top20, oi_bias_rsi_exit -- one instance
per client/binding) used to poll them independently on its own cadence.
Running 2+ bindings of these strategies simultaneously meant each fired its
own separate real NSE request for identical data at the same moment.

Fixed with a shared, short-TTL, thread-safe cache -- the first caller after
the TTL expires triggers one real fetch; every other caller within the
window gets that same result without a second real request."""
import time
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from strategies.oi_orb_screener import screener


@pytest.fixture(autouse=True)
def _clear_cache():
    with screener._nse_cache_lock:
        screener._nse_cache.clear()
    yield
    with screener._nse_cache_lock:
        screener._nse_cache.clear()


def _fake_nse():
    return Mock()


def test_second_call_within_ttl_does_not_hit_nse_again():
    calls = []

    def _fake_uncached(nse):
        calls.append(1)
        return pd.DataFrame({"symbol": ["RELIANCE"], "pChange": [1.0]})

    with patch.object(screener, "_fetch_fno_price_universe_uncached", side_effect=_fake_uncached):
        df1 = screener.fetch_fno_price_universe(_fake_nse())
        df2 = screener.fetch_fno_price_universe(_fake_nse())  # a DIFFERENT book's own session object

    assert len(calls) == 1, "second call within the TTL window must reuse the cached result, not refetch"
    assert df1.equals(df2)


def test_call_after_ttl_expires_refetches():
    calls = []

    def _fake_uncached(nse):
        calls.append(1)
        return pd.DataFrame({"symbol": ["RELIANCE"]})

    with patch.object(screener, "_fetch_fno_price_universe_uncached", side_effect=_fake_uncached), \
         patch.object(screener, "_NSE_CACHE_TTL_SEC", 0.05):
        screener.fetch_fno_price_universe(_fake_nse())
        time.sleep(0.1)
        screener.fetch_fno_price_universe(_fake_nse())

    assert len(calls) == 2, "a call after the TTL has expired must trigger a genuine refetch"


def test_returned_dataframe_is_a_copy_not_a_shared_mutable_reference():
    """Two callers sharing the same cached result must never be able to
    mutate each other's view of it -- a real risk with a naive shared cache
    returning the same DataFrame object to every caller."""
    def _fake_uncached(nse):
        return pd.DataFrame({"symbol": ["RELIANCE"], "pChange": [1.0]})

    with patch.object(screener, "_fetch_fno_price_universe_uncached", side_effect=_fake_uncached):
        df1 = screener.fetch_fno_price_universe(_fake_nse())
        df2 = screener.fetch_fno_price_universe(_fake_nse())

    df1.loc[0, "pChange"] = 999.0
    assert df2.loc[0, "pChange"] == 1.0, "mutating one caller's copy must never affect another caller's"


def test_different_functions_cache_independently():
    calls_universe, calls_spurts = [], []

    with patch.object(screener, "_fetch_fno_price_universe_uncached",
                       side_effect=lambda nse: calls_universe.append(1) or pd.DataFrame({"symbol": ["A"]})), \
         patch.object(screener, "_fetch_oi_spurts_nse_uncached",
                       side_effect=lambda nse: calls_spurts.append(1) or pd.DataFrame({"symbol": ["A"], "oi_spurt_pct": [8.0]})):
        screener.fetch_fno_price_universe(_fake_nse())
        screener.fetch_oi_spurts_nse(_fake_nse())
        screener.fetch_fno_price_universe(_fake_nse())
        screener.fetch_oi_spurts_nse(_fake_nse())

    assert len(calls_universe) == 1
    assert len(calls_spurts) == 1


def test_a_failed_fetch_is_cached_briefly_so_concurrent_callers_dont_all_hammer_nse():
    calls = []

    def _fake_uncached(nse):
        calls.append(1)
        raise RuntimeError("NSE rate-limited")

    with patch.object(screener, "_fetch_fno_price_universe_uncached", side_effect=_fake_uncached):
        with pytest.raises(RuntimeError):
            screener.fetch_fno_price_universe(_fake_nse())
        with pytest.raises(RuntimeError):
            screener.fetch_fno_price_universe(_fake_nse())

    assert len(calls) == 1, "a second caller within the TTL window must re-raise the cached failure, not retry NSE itself"
