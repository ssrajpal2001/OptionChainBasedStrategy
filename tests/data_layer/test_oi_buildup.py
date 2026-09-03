"""Pure classification tests for data_layer/oi_buildup.py -- the four standard
futures OI-buildup buckets and their agreement/contradiction with a CE
(bullish) or PE (bearish) trade thesis. See FnOPositionalBook._check_oi_buildup
for where this feeds into the (non-blocking) entry-decision log."""
from data_layer.oi_buildup import (
    classify_oi_buildup, oi_agreement,
    LONG_BUILDUP, SHORT_BUILDUP, SHORT_COVERING, LONG_UNWINDING, FLAT,
    CONFIRMS, CONTRADICTS, NEUTRAL,
)


def test_price_up_oi_up_is_long_buildup():
    assert classify_oi_buildup(100, 105, 1000, 1200) == LONG_BUILDUP


def test_price_down_oi_up_is_short_buildup():
    assert classify_oi_buildup(100, 95, 1000, 1200) == SHORT_BUILDUP


def test_price_up_oi_down_is_short_covering():
    assert classify_oi_buildup(100, 105, 1200, 1000) == SHORT_COVERING


def test_price_down_oi_down_is_long_unwinding():
    assert classify_oi_buildup(100, 95, 1200, 1000) == LONG_UNWINDING


def test_unusable_prev_close_is_flat():
    assert classify_oi_buildup(0, 105, 1000, 1200) == FLAT


def test_unusable_prev_oi_is_flat():
    assert classify_oi_buildup(100, 105, 0, 1200) == FLAT


def test_unchanged_price_is_flat():
    assert classify_oi_buildup(100, 100, 1000, 1200) == FLAT


def test_ce_confirmed_by_long_buildup():
    assert oi_agreement("CE", LONG_BUILDUP) == CONFIRMS


def test_ce_confirmed_by_short_covering():
    assert oi_agreement("CE", SHORT_COVERING) == CONFIRMS


def test_ce_contradicted_by_short_buildup():
    assert oi_agreement("CE", SHORT_BUILDUP) == CONTRADICTS


def test_pe_confirmed_by_short_buildup():
    assert oi_agreement("PE", SHORT_BUILDUP) == CONFIRMS


def test_pe_confirmed_by_long_unwinding():
    assert oi_agreement("PE", LONG_UNWINDING) == CONFIRMS


def test_pe_contradicted_by_long_buildup():
    assert oi_agreement("PE", LONG_BUILDUP) == CONTRADICTS


def test_flat_is_always_neutral_regardless_of_direction():
    assert oi_agreement("CE", FLAT) == NEUTRAL
    assert oi_agreement("PE", FLAT) == NEUTRAL
