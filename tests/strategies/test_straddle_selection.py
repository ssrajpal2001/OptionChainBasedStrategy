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


from strategies.straddle_selection import select_balanced_pair


def _cache(d):
    # d: {(strike, side): ltp} -> full cache with atp == ltp
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_select_balanced_anchor_is_lower_time_value_side():
    # spot=atm=100, CE=60, PE=80 → CE is anchor (lower LTP, both OTM-ish).
    # Partner = PE side strike with ltp_target<=ltp<60, highest such.
    cache = _cache({
        (100, "CE"): 60.0, (100, "PE"): 80.0,
        (105, "PE"): 55.0, (110, "PE"): 40.0,
    })
    res = select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0)
    assert res is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = res
    # Anchor CE@100=60; partner PE = highest strictly below 60 and >=30 → 105@55
    assert (ce_strike, ce_ltp) == (100, 60.0)
    assert (pe_strike, pe_ltp) == (105, 55.0)


def test_select_balanced_anchor_below_target_returns_none():
    cache = _cache({(100, "CE"): 20.0, (100, "PE"): 80.0, (105, "PE"): 15.0})
    # CE anchor LTP 20 < target 30 → None
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None


def test_select_balanced_no_partner_below_anchor_returns_none():
    # All partner candidates >= anchor LTP → no strictly-lower partner.
    cache = _cache({(100, "CE"): 50.0, (100, "PE"): 80.0, (105, "PE"): 90.0})
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None


def test_select_balanced_missing_atm_returns_none():
    cache = _cache({(100, "CE"): 60.0})  # no PE ATM
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None


from strategies.straddle_selection import scan_pool


def test_scan_pool_picks_min_balanced_score():
    # ATM (100): CE corr 55 > PE corr 50 → CE bias stronger → require ce_ltp < pe_ltp.
    # ATM is INCLUDED as a candidate (matches reference). Valid pairs (ce<pe, both>=30):
    #   100CE55 / 110PE70 → score 0.12
    #   95CE40  / 105PE42 → score 0.024   ← min
    #   95CE40  / 100PE50 → score 0.111
    #   90CE30  / ...     → larger
    # Global min balanced score → 95CE / 105PE.
    cache = _cache({
        (100, "CE"): 55.0, (100, "PE"): 50.0,
        (95, "CE"): 40.0, (105, "PE"): 42.0,
        (90, "CE"): 30.0, (110, "PE"): 70.0,
    })

    def always_ok(ce_s, pe_s):
        return True

    res = scan_pool(
        cache, spot=100, step=5, offset=4, ltp_target=30.0,
        rule_pass=always_ok, metric="balanced_premium",
    )
    assert res is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = res
    assert (ce_strike, pe_strike) == (95, 105)
    assert (ce_ltp, pe_ltp) == (40.0, 42.0)


def test_scan_pool_respects_ltp_target_floor():
    # ATM (100): PE corr 55 > CE corr 50 → PE bias stronger → require pe_ltp < ce_ltp.
    # ATM included. Candidate CE strikes: 100=50, 105=40. PE strikes: 100=55, 95=25.
    # Valid pairs need pe_ltp < ce_ltp and BOTH >= 30:
    #   pe=100(55): needs ce>55 → none (50,40) → no pair
    #   pe=95(25): below floor 30 → excluded
    # → no valid pair → None (proves the below-floor 95PE is excluded).
    cache = _cache({
        (100, "CE"): 50.0, (100, "PE"): 55.0,
        (105, "CE"): 40.0, (95, "PE"): 25.0,
    })

    def always_ok(ce_s, pe_s):
        return True

    res = scan_pool(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                    rule_pass=always_ok, metric="balanced_premium")
    assert res is None


def test_scan_pool_rule_rejection_excludes_pair():
    cache = _cache({
        (100, "CE"): 55.0, (100, "PE"): 50.0,
        (95, "CE"): 40.0, (105, "PE"): 60.0,
    })

    def reject_all(ce_s, pe_s):
        return False

    res = scan_pool(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                    rule_pass=reject_all, metric="balanced_premium")
    assert res is None
