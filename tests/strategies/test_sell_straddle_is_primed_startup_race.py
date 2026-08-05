"""
Regression test for the 2026-08-05 live incident: on a fresh restart,
SellStraddleStrategy's tick loop can call _maybe_try_entry() -> _try_entry()
-> _eval_ruleset() -> _is_primed() before the first 1m CANDLE_CLOSE has been
handled by _on_candle() -- the only place that sets self._market_open_dt.
_is_primed() unconditionally did `self._market_open_dt + timedelta(...)`,
raising `TypeError: unsupported operand type(s) for +: 'NoneType' and
'datetime.timedelta'` every time this race was hit, logged as a "recovered"
tick-handler error but a real bug -- entry evaluation for that tick was lost.
"""
from datetime import datetime

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def test_is_primed_false_not_crash_when_market_open_dt_unset():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    assert ss._market_open_dt is None

    rules = [{"indicator": "SLOPE", "tf": 1}]
    result = ss._is_primed(datetime.now(IST), rules)

    assert result is False
    assert ss._primed is False


def test_is_primed_still_works_normally_once_market_open_dt_set():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    now = datetime.now(IST)
    ss._market_open_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)

    rules = [{"indicator": "SLOPE", "tf": 1}]
    # wait_min > 0 (has slope) and `now` is likely well past market_open + wait
    # for a same-day test run -- either branch (primed or still-waiting) must
    # not raise, and must not silently report primed with no market_open_dt.
    result = ss._is_primed(now, rules)
    assert isinstance(result, bool)
