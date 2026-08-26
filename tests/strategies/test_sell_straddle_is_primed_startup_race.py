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
from datetime import datetime, timedelta

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


# ── 2026-08-26 real incident: mid-day restart primed INSTANTLY ─────────────
# _market_open_dt/entry_start alone is only a correct priming anchor for a
# genuine morning start. On a restart at, say, 13:29 (pm2 restart, common
# during real deploys), the OLD anchor's own ready_at (entry_start + wait,
# e.g. 09:18) had already passed hours earlier -- so priming completed on the
# very FIRST post-restart evaluation, against a completely fresh, empty pool/
# strike_prem cache with zero real ticks accumulated yet. Confirmed live: an
# ATM anchor read ltp=0.00 at restart+1s, and by restart+19s priming had
# "completed" and the low-anchor-LTP expiry-shift fired off that stale/absent
# data. self._process_start_dt (set unconditionally in start()) is now ALSO a
# priming anchor candidate so every restart gets its own genuine fresh wait.

def test_is_primed_false_immediately_after_a_mid_day_restart():
    """The exact real incident: market_open/entry_start's own ready_at has
    long passed (afternoon), but the PROCESS itself only just started --
    priming must NOT report complete on the very first post-restart tick."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    now = datetime.now(IST).replace(hour=13, minute=29, second=46, microsecond=0)
    ss._market_open_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)
    ss._process_start_dt = now   # start() just ran, this same instant

    rules = [{"indicator": "SLOPE", "tf": 1}]   # wait_min = 1*2 = 2 minutes
    assert ss._is_primed(now, rules) is False
    assert ss._primed is False


def test_is_primed_true_two_minutes_after_a_mid_day_restart():
    """The flip side: once the process's OWN 2-minute wait has genuinely
    elapsed since restart, priming must complete normally -- this isn't a
    permanent block, just a restart-anchored one."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    restart_at = datetime.now(IST).replace(hour=13, minute=29, second=46, microsecond=0)
    ss._market_open_dt = restart_at.replace(hour=9, minute=15, second=0, microsecond=0)
    ss._process_start_dt = restart_at

    rules = [{"indicator": "SLOPE", "tf": 1}]
    now = restart_at + timedelta(minutes=2, seconds=1)
    assert ss._is_primed(now, rules) is True
    assert ss._primed is True


def test_is_primed_unaffected_by_process_start_dt_on_a_genuine_morning_start():
    """A real morning start: process_start_dt is at/before market open, so it
    must never PUSH the anchor later than the pre-fix market-open/entry_start
    logic already correctly computed -- only a restart happening AFTER that
    anchor should ever move it."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    market_open = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0)
    ss._market_open_dt = market_open
    ss._process_start_dt = market_open   # process started at/right after market open

    rules = [{"indicator": "SLOPE", "tf": 1}]   # wait_min=2, ready_at = entry_start+2min
    entry_start_dt = market_open.replace(hour=ss._entry_start.hour, minute=ss._entry_start.minute)
    ready_at = entry_start_dt + timedelta(minutes=2)
    assert ss._is_primed(ready_at - timedelta(seconds=1), rules) is False
    assert ss._is_primed(ready_at + timedelta(seconds=1), rules) is True


def test_is_primed_backward_compatible_when_process_start_dt_never_set():
    """A harness/test that constructs the book without calling start() (so
    _process_start_dt stays None) must behave exactly as before this fix --
    confirms the getattr(..., None) guard degrades safely."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    assert ss._process_start_dt is None
    now = datetime.now(IST)
    ss._market_open_dt = now.replace(hour=9, minute=15, second=0, microsecond=0)
    rules = [{"indicator": "SLOPE", "tf": 1}]
    result = ss._is_primed(now, rules)
    assert isinstance(result, bool)
