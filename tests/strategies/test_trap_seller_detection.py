from strategies.trap_seller_detection import SellerTrapDetector, State


def _c(o, h, l, c):
    return {"open": o, "high": h, "low": l, "close": c}


def test_below_above_return_fires_entry():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 960))

    d.on_tick(880)
    assert d.state == State.SELLERS_IN
    assert d.active_level.entry_l == 900
    assert d.active_level.sl_h == 1000
    assert d.entry_ready is False

    d.on_tick(1010)
    assert d.state == State.TRAPPED
    assert d.entry_ready is False

    d.on_tick(900)
    assert d.state == State.ENTRY_READY
    assert d.entry_ready is True


def test_below_without_above_no_entry():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 960))
    d.on_tick(880)
    assert d.state == State.SELLERS_IN
    assert d.entry_ready is False


def test_above_without_return_trapped_no_entry():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 960))
    d.on_tick(880)
    d.on_tick(1010)
    assert d.state == State.TRAPPED
    assert d.entry_ready is False


def test_return_before_above_stays_sellers_in():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 960))
    d.on_tick(880)
    d.on_tick(900)
    assert d.state == State.SELLERS_IN
    assert d.entry_ready is False


def test_invalidate_pops_to_prior_level():
    d = SellerTrapDetector()
    d.on_candle(_c(950, 1000, 900, 960))
    d.on_candle(_c(860, 920, 800, 880))
    assert d.active_level.entry_l == 800
    d.invalidate_active()
    assert d.active_level.entry_l == 900
