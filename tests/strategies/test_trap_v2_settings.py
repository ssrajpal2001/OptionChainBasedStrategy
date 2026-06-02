"""Task 2 — per-index trap_trading defaults in runtime_config."""

import pytest

from data_layer.runtime_config import RuntimeConfig

EXPECTED_LADDER = {"5": 5, "4": 4, "3": 3, "2": 2, "1": 1}
ROUNDOFF = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100, "CRUDEOIL": 100}


@pytest.mark.parametrize("idx", ["CRUDEOIL", "NIFTY"])
def test_common_keys(idx):
    tt = RuntimeConfig.index_section(idx, "trap_trading")
    assert isinstance(tt, dict)
    assert tt["dte_offset_ladder"] == EXPECTED_LADDER
    assert tt["lookback_days"] == 2
    assert tt["buy_depth"] == 0
    assert tt["htf_minutes"] == 75
    assert tt["mtf_minutes"] == 5
    assert tt["sl_min_minutes"] == 1


@pytest.mark.parametrize("idx,step", list(ROUNDOFF.items()))
def test_roundoff_step(idx, step):
    tt = RuntimeConfig.index_section(idx, "trap_trading")
    assert tt["roundoff_step"] == step


@pytest.mark.parametrize("idx", ["NIFTY", "BANKNIFTY", "SENSEX", "CRUDEOIL"])
def test_lookback_min(idx):
    tt = RuntimeConfig.index_section(idx, "trap_trading")
    assert tt["lookback_days"] >= 2
