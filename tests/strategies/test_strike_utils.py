# tests/strategies/test_strike_utils.py
from strategies.strike_utils import compute_atm


def test_compute_atm_exact():
    assert compute_atm(24500.0, 50.0) == 24500.0


def test_compute_atm_rounds_up():
    assert compute_atm(24526.0, 50.0) == 24550.0


def test_compute_atm_rounds_down():
    assert compute_atm(24524.0, 50.0) == 24500.0


def test_compute_atm_crypto():
    assert compute_atm(63787.30, 1000.0) == 64000.0


def test_compute_atm_small_step():
    assert compute_atm(200.75, 0.5) == 201.0
