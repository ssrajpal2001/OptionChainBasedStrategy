"""Tests for pure TT strike-selection logic."""

import pytest

from strategies.trap_strike_selection import (
    dte_offset_steps, prev_day_atm, select_trap_strikes, TrapStrikes,
)


# ── DTE offset ladder ────────────────────────────────────────────────────────
@pytest.mark.parametrize("dte,expected", [
    (10, 5), (7, 5), (6, 5),   # > 5 -> 5
    (5, 4),                    # > 4 -> 4
    (4, 3),                    # > 3 -> 3
    (3, 2),                    # > 2 -> 2
    (2, 1),                    # > 1 -> 1
    (1, 0), (0, 0), (-3, 0),   # <= 1 -> 0
])
def test_dte_offset_steps(dte, expected):
    assert dte_offset_steps(dte) == expected


# ── Prev-day ATM rounding ────────────────────────────────────────────────────
def test_prev_day_atm_crudeoil_step100():
    # (8650 + 8750)/2 = 8700 -> already on grid
    assert prev_day_atm(8750, 8650, 100) == 8700


def test_prev_day_atm_rounds_to_nearest_step():
    # (8680 + 8740)/2 = 8710 -> nearest 100 = 8700
    assert prev_day_atm(8740, 8680, 100) == 8700
    # (8690 + 8772)/2 = 8731 -> nearest 100 = 8700
    assert prev_day_atm(8772, 8690, 100) == 8700


def test_prev_day_atm_nifty_step50():
    # (24470 + 24550)/2 = 24510 -> nearest 50 = 24500
    assert prev_day_atm(24550, 24470, 50) == 24500


def test_prev_day_atm_bad_step():
    with pytest.raises(ValueError):
        prev_day_atm(100, 90, 0)


# ── Full selection: CRUDEOIL (step 100) ──────────────────────────────────────
def test_select_crudeoil_dte6_500itm():
    s = select_trap_strikes(prev_high=8750, prev_low=8650, dte=6, step=100)
    assert isinstance(s, TrapStrikes)
    assert s.atm == 8700
    assert s.offset_steps == 5
    assert s.offset_pts == 500
    assert s.ce_strike == 8200      # ATM - 500 (ITM call)
    assert s.pe_strike == 9200      # ATM + 500 (ITM put)


def test_select_crudeoil_dte3_200itm():
    # dte=3 -> "> 2" branch -> offset_steps = min(max(3-1,0),5) = 2 -> 200 pts
    s = select_trap_strikes(8750, 8650, dte=3, step=100)
    assert s.offset_steps == 2
    assert s.offset_pts == 200
    assert s.ce_strike == 8500
    assert s.pe_strike == 8900


def test_select_crudeoil_expiry_day_atm():
    s = select_trap_strikes(8750, 8650, dte=1, step=100)
    assert s.offset_steps == 0
    assert s.ce_strike == s.pe_strike == s.atm == 8700


# ── Step-multiple semantics on NIFTY (step 50) ───────────────────────────────
def test_select_nifty_dte6_uses_step_multiples():
    # 5 steps * 50 = 250 ITM (NOT 500) — offset is in strike-step multiples
    s = select_trap_strikes(prev_high=24550, prev_low=24470, dte=6, step=50)
    assert s.atm == 24500
    assert s.offset_steps == 5
    assert s.offset_pts == 250
    assert s.ce_strike == 24250
    assert s.pe_strike == 24750
