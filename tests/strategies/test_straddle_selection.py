from strategies.straddle_selection import strip_intrinsic, pair_indicators


def test_strip_intrinsic_ce_itm():
    # spot 100, strike 90 → CE intrinsic = 10, time value = ltp - 10
    assert strip_intrinsic(ltp=25.0, side="CE", strike=90, spot=100) == 15.0


def test_strip_intrinsic_pe_itm():
    # spot 100, strike 110 → PE intrinsic = 10, time value = ltp - 10
    assert strip_intrinsic(ltp=25.0, side="PE", strike=110, spot=100) == 15.0


def test_strip_intrinsic_otm_unchanged():
    # OTM → intrinsic 0 → time value = ltp
    assert strip_intrinsic(ltp=20.0, side="CE", strike=110, spot=100) == 20.0


def test_pair_indicators_full():
    cache = {
        (100, "CE"): {"ltp": 30.0, "atp": 28.0},
        (100, "PE"): {"ltp": 26.0, "atp": 25.0},
    }
    prev = {(100, "CE"): 29.0, (100, "PE"): 27.0}  # prev closed atp
    ind = pair_indicators(cache, prev, 100, 100)
    assert ind == {"close": 56.0, "vwap": 53.0, "slope": 53.0 - 56.0}


def test_pair_indicators_missing_leg_returns_none():
    cache = {(100, "CE"): {"ltp": 30.0, "atp": 28.0}}
    assert pair_indicators(cache, {}, 100, 100) is None


def test_pair_indicators_no_prev_omits_slope():
    cache = {
        (100, "CE"): {"ltp": 30.0, "atp": 28.0},
        (100, "PE"): {"ltp": 26.0, "atp": 25.0},
    }
    ind = pair_indicators(cache, {}, 100, 100)
    assert ind["close"] == 56.0 and ind["vwap"] == 53.0
    assert "slope" not in ind
