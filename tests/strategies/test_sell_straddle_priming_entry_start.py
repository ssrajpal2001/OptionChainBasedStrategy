"""Regression tests for the 2026-08-20 priming fixes (real incident: NIFTY entry_start=09:16,
first BEGINNING trade fired at 09:16:05 off a single candle + 5s of live ticks instead of two
genuinely closed candles).

Two separate bugs, both covered here:
  A) `_priming_wait_minutes` skipped the ×2 SLOPE wait for any rule stored in "advanced"
     (operand1/operand2) form, regardless of what it actually compared.
  B) `_is_primed` anchored its wait off the fixed 09:15 market-open constant even when the
     deployment's own configured `entry_start` is later — letting a candle that closed BEFORE
     the entry window opened count toward the required warm-up.
"""
from datetime import datetime, time as dtime, timedelta

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _make(entry_start: dtime = dtime(9, 16)) -> SellStraddleStrategy:
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._entry_start = entry_start
    return ss


def test_priming_wait_doubles_for_plain_slope_rule():
    ss = _make()
    rules = [{"indicator": "slope", "tf": 1, "operator_sym": "<", "threshold": 0}]
    assert ss._priming_wait_minutes(rules) == 2


def test_priming_wait_doubles_for_advanced_mode_slope_rule():
    """The real deployment's rule: BEGINNING ENTRY: SLOPE<VALUE(1m), stored as
    indicator="advanced", operand1="SLOPE", operand2="VALUE" — must get the same ×2
    treatment as a plain SLOPE rule, not silently skip it."""
    ss = _make()
    rules = [{"indicator": "advanced", "operand1": "SLOPE", "operand2": "VALUE",
              "operator_sym": "<", "tf": 1}]
    assert ss._priming_wait_minutes(rules) == 2


def test_priming_wait_advanced_mode_slope_in_operand2():
    ss = _make()
    rules = [{"indicator": "advanced", "operand1": "VALUE", "operand2": "VWAP_SLOPE",
              "operator_sym": ">", "tf": 1}]
    assert ss._priming_wait_minutes(rules) == 2


def test_priming_wait_advanced_mode_no_slope_stays_single():
    ss = _make()
    rules = [{"indicator": "advanced", "operand1": "CLOSE", "operand2": "VWAP",
              "operator_sym": "<", "tf": 2}]
    assert ss._priming_wait_minutes(rules) == 2  # max_tf(2) * 1


def test_is_primed_anchors_on_entry_start_when_later_than_market_open():
    """entry_start=09:16 (later than the fixed 09:15 market-open constant) with an
    advanced-mode SLOPE(1m) rule must not be primed until 09:16 + 2min = 09:18, not
    09:15 + 2min = 09:17 (the real incident's actual bad timing)."""
    ss = _make(entry_start=dtime(9, 16))
    today = datetime.now(IST).date()
    ss._market_open_dt = datetime.combine(today, dtime(9, 15), tzinfo=IST)
    rules = [{"indicator": "advanced", "operand1": "SLOPE", "operand2": "VALUE", "tf": 1}]

    not_yet = datetime.combine(today, dtime(9, 17, 30), tzinfo=IST)
    assert ss._is_primed(not_yet, rules) is False
    assert ss._primed is False

    ready = datetime.combine(today, dtime(9, 18, 5), tzinfo=IST)
    assert ss._is_primed(ready, rules) is True
    assert ss._primed is True


def test_is_primed_still_uses_market_open_when_entry_start_is_earlier():
    """If entry_start is earlier than (or equal to) real market open, market open
    still wins as the anchor — entry_start can't pull the wait window backward."""
    ss = _make(entry_start=dtime(9, 0))
    today = datetime.now(IST).date()
    ss._market_open_dt = datetime.combine(today, dtime(9, 15), tzinfo=IST)
    rules = [{"indicator": "slope", "tf": 1}]

    not_yet = datetime.combine(today, dtime(9, 16, 30), tzinfo=IST)
    assert ss._is_primed(not_yet, rules) is False

    ready = datetime.combine(today, dtime(9, 17, 5), tzinfo=IST)
    assert ss._is_primed(ready, rules) is True
