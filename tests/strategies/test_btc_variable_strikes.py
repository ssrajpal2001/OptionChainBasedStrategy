"""
Tests for variable-strike (crypto/BTC) selection in sell-straddle.

Delta BTC options have non-uniform strike gaps (e.g. 100 near ATM, 200/500 wings).
These tests verify that selection discovers ATM from actual quotes and scans the
nearest strikes rather than assuming a fixed step.
"""
import pytest

from strategies.sell_straddle.selection import (
    _common_atm,
    _strikes_near_spot,
    scan_pool,
    select_balanced_pair,
    select_partner_for,
)


def _sp(d):
    """Build strike_prem dict from {(strike, side): ltp} or {(strike, side): (ltp, atp)}."""
    out = {}
    for (strike, side), v in d.items():
        if isinstance(v, tuple):
            out[(strike, side)] = {"ltp": float(v[0]), "atp": float(v[1])}
        else:
            out[(strike, side)] = {"ltp": float(v), "atp": float(v)}
    return out


class TestHelpers:
    def test_common_atm_picks_closest_common_strike(self):
        sp = _sp({
            (100000, "CE"): 500,
            (100200, "CE"): 400,
            (100500, "CE"): 300,
            (100000, "PE"): 450,
            (100200, "PE"): 350,
            (100500, "PE"): 250,
        })
        assert _common_atm(sp, 100150) == 100200
        assert _common_atm(sp, 100050) == 100000

    def test_common_atm_zero_spot_returns_first_common(self):
        sp = _sp({(100000, "CE"): 1, (100000, "PE"): 1})
        assert _common_atm(sp, 0) == 100000

    def test_strikes_near_spot_sorted_by_proximity(self):
        sp = _sp({
            (100000, "CE"): 1,
            (100200, "CE"): 1,
            (100500, "CE"): 1,
            (101000, "CE"): 1,
        })
        assert _strikes_near_spot(sp, "CE", 100150, n=2) == [100000, 100200]
        assert _strikes_near_spot(sp, "CE", 100400, n=2) == [100200, 100500]


class TestSelectBalancedPairVariable:
    def test_selects_near_atm_with_variable_gaps(self):
        # Spot ~100150. Nearest strikes 100000 and 100200 (100 apart).
        # Far wing 100500 is 500 away.
        sp = _sp({
            (100000, "CE"): 500,  # TV ~650 (ITM by 150)
            (100200, "CE"): 400,  # TV ~400
            (100500, "CE"): 250,  # TV ~0 (deep OTM)
            (100000, "PE"): 300,  # TV ~300
            (100200, "PE"): 350,  # TV ~350
            (100500, "PE"): 600,  # TV ~450 (ITM by 350)
        })
        # ATM = 100200. CE TV=400, PE TV=350. PE lower TV -> anchor PE100200, anchor_ltp=350.
        # 2026-09-25, direct user spec: lowest |anchor-candidate|/(anchor+candidate)
        # score wins, not "candidate <= anchor". Scores vs anchor_ltp=350:
        # CE100000=500 -> 0.1765, CE100200=400 -> 0.0667 (lowest), CE100500=250 -> 0.1667.
        # CE100200 wins -- the same strike as the anchor, a plain ATM-both-legs pair.
        res = select_balanced_pair(
            sp, spot=100150, step=200, offset=6, ltp_target=20,
            theta_target=10, variable_strikes=True,
        )
        assert res == (100200, 100200, 400.0, 350.0)

    def test_no_partner_without_variable_flag_uses_fixed_step(self):
        # Same data, but fixed-step logic rounds spot to nearest 200 -> atm=100200
        # (round(100150/200)*200), and with offset=1 scans only {100000,100200,100400}
        # for the partner -- 100400 has no quote, so only 100000/100200 are candidates.
        sp = _sp({
            (100000, "CE"): 500,
            (100200, "CE"): 400,
            (100500, "CE"): 250,
            (100000, "PE"): 300,
            (100200, "PE"): 350,
            (100500, "PE"): 600,
        })
        res = select_balanced_pair(
            sp, spot=100150, step=200, offset=1, ltp_target=20,
            theta_target=10, variable_strikes=False,
        )
        # anchor = PE100200@350 (lower TV). 2026-09-25, direct user spec: lowest
        # score wins. CE100000=500 -> score 0.1765; CE100200=400 -> score 0.0667
        # (lowest) -> CE100200/PE100200 selected (same strike, plain ATM pair).
        assert res == (100200, 100200, 400.0, 350.0)


class TestSelectPartnerForVariable:
    def test_rolls_to_nearest_strike(self):
        sp = _sp({
            (100000, "CE"): 500,
            (100200, "CE"): 400,
            (100500, "CE"): 250,
            (100000, "PE"): 300,
            (100200, "PE"): 350,
            (100500, "PE"): 600,
        })
        # Keep CE100000 @500, roll PE side. Nearest PE to spot=100150 are 100000,100200,100500.
        # ltp_le_kept is DISABLED by default, so partner is simply closest premium to 500.
        # PE100500 @600 diff=100 is closer than PE100200 @350 diff=150 -> selected.
        res = select_partner_for(
            sp, roll_side="PE", kept_strike=100000, kept_ltp=500,
            spot=100150, step=200, offset=6, ltp_target=20,
            rule_pass=lambda c, p: True,
            theta_target=10, variable_strikes=True,
        )
        assert res == (100500, 600.0)

    def test_rolls_respects_ltp_le_kept_when_enabled(self):
        sp = _sp({
            (100000, "CE"): 500,
            (100200, "CE"): 400,
            (100500, "CE"): 250,
            (100000, "PE"): 300,
            (100200, "PE"): 350,
            (100500, "PE"): 600,
        })
        # With ltp_le_kept=True, partner must be <= kept_ltp=500.
        # PE100500 @600 is too rich -> PE100200 @350 selected.
        res = select_partner_for(
            sp, roll_side="PE", kept_strike=100000, kept_ltp=500,
            spot=100150, step=200, offset=6, ltp_target=20,
            rule_pass=lambda c, p: True,
            theta_target=10, variable_strikes=True, ltp_le_kept=True,
        )
        assert res == (100200, 350.0)


class TestScanPoolVariable:
    def test_scan_pool_variable_strikes_finds_balanced_pair(self):
        sp = _sp({
            (100000, "CE"): 500,
            (100200, "CE"): 400,
            (100500, "CE"): 250,
            (100000, "PE"): 300,
            (100200, "PE"): 350,
            (100500, "PE"): 600,
        })
        # ATM = 100200. CE TV=400, PE TV=300 -> CE bias stronger -> require CE ltp < PE ltp.
        # Best balanced pair with CE ltp < PE ltp is (CE100000=500, PE100500=600):
        # score = 100 / 1100 = 0.0909.
        res = scan_pool(
            sp, spot=100150, step=200, offset=6, ltp_target=20,
            rule_pass=lambda c, p: True, metric="balanced_premium",
            theta_target=10, variable_strikes=True,
        )
        assert res is not None
        ce, pe, ce_ltp, pe_ltp = res
        assert (ce, pe) == (100000, 100500)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
