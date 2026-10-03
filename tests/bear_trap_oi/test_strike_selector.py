from strategies.bear_trap_oi.strike_selector import (
    round_to_strike_step, map_strikes,
)


def test_round_to_strike_step_rounds_to_nearest():
    assert round_to_strike_step(24513.0, 50) == 24500
    assert round_to_strike_step(24538.0, 50) == 24550
    assert round_to_strike_step(24525.0, 50) == 24550  # round-half-up


def test_map_strikes_ce_near_pdl_pe_near_pdh():
    ce_strike, pe_strike = map_strikes(pdh=24680.0, pdl=24510.0, step=50)
    assert ce_strike == 24500  # closest to PDL
    assert pe_strike == 24700  # closest to PDH
