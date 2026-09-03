"""
tests/strategies/test_sell_straddle_priming_fastpath.py -- regression for a
real 2026-08-26 live-observed inefficiency: _maybe_try_entry's once-per-
minute tf-boundary dedup bucket gets consumed by the FIRST tick of a
minute regardless of whether priming was actually ready yet -- if priming
completes a few seconds LATER in that SAME minute (confirmed live: ready
at 14:57:07, but the 14:57:05 tick had already logged "PRIMING -- waiting"
and consumed minute 57's bucket), the real evaluation didn't run until the
NEXT minute's boundary, a full extra ~1 minute of dead time on top of the
already-necessary 2-live-bar SLOPE wait. Fixed: _maybe_try_entry now
detects the False->True priming transition and fires immediately even if
that minute's bucket was already consumed by an earlier not-yet-primed
check.
"""
import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from data_layer.runtime_config import RuntimeConfig
from strategies.sell_straddle import SellStraddleStrategy

_RULES = [{
    "indicator": "advanced", "operand1": "slope", "operand2": "value",
    "operand2_val": 0.0, "operator_sym": "<", "tf": 1,
}]  # wait_min = 1*2 = 2 (has_slope)


def _strategy(process_start: datetime):
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = 24500.0
    s._ce_ltp = 150.0
    s._pe_ltp = 140.0
    s._market_open_dt = process_start.replace(hour=9, minute=15, second=0, microsecond=0)
    s._process_start_dt = process_start
    calls = []

    async def _fake_try_entry(now, due_beginning=True, due_reentry=True):
        calls.append((now, due_beginning, due_reentry))
    s._try_entry = _fake_try_entry
    return s, calls


def _no_rules_runtime_config():
    return patch.object(
        RuntimeConfig, "index_section",
        return_value={"entry_rules_beginning": _RULES, "entry_rules_reentry": []},
    )


def test_bucket_consumed_by_early_not_yet_primed_check_does_not_block_the_fastpath():
    """Reproduces the exact real incident: tick #1 in a minute (not yet primed)
    fires via the normal boundary path (and consumes that minute's bucket,
    same as always -- _eval_ruleset itself, mocked away here via _try_entry,
    is what actually logs "PRIMING -- waiting" and blocks deeper). Tick #2 a
    couple seconds later, SAME minute, priming now genuinely ready: WITHOUT
    the fix this would NOT fire again (bucket already consumed) and would
    have to wait for the 14:58 boundary; WITH the fix it fires immediately."""
    process_start = datetime.now(IST).replace(hour=14, minute=55, second=6, microsecond=0)
    ready_at = process_start + timedelta(minutes=2)   # 14:57:06
    s, calls = _strategy(process_start)

    with _no_rules_runtime_config():
        # Tick #1: 14:57:05 -- boundary window open (second>=5), not yet primed
        # (ready_at is 14:57:06). Fires via the normal once-per-minute path
        # (unchanged behavior -- this is what lets "PRIMING -- waiting" log
        # once per minute in the real _eval_ruleset, not spammed every tick).
        tick1 = ready_at - timedelta(seconds=1)
        asyncio.run(s._maybe_try_entry(tick1))
        assert calls == [(tick1, True, False)]
        assert s._primed is False

        # Tick #2: 14:57:07 -- priming now genuinely ready, SAME minute as
        # tick #1 whose bucket is already consumed. Must fire AGAIN NOW (the
        # fast-path), not wait for the 14:58 boundary.
        tick2 = ready_at + timedelta(seconds=1)
        asyncio.run(s._maybe_try_entry(tick2))
        assert calls == [(tick1, True, False), (tick2, True, False)]
        assert s._primed is True


def test_normal_once_per_minute_dedup_still_holds_once_primed():
    """Once primed, a second tick in the SAME minute must NOT re-fire --
    the fast-path must not turn into a flood."""
    process_start = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0)
    s, calls = _strategy(process_start)
    s._primed = True   # already primed (e.g. well into the day)

    with _no_rules_runtime_config():
        now = process_start.replace(hour=11, minute=30, second=10)
        asyncio.run(s._maybe_try_entry(now))
        assert len(calls) == 1
        # A second tick in the same minute must not fire again.
        now2 = now.replace(second=45)
        asyncio.run(s._maybe_try_entry(now2))
        assert len(calls) == 1


def test_no_fastpath_double_fire_while_still_genuinely_not_primed():
    """Two ticks in the same minute, priming genuinely not ready for either --
    the SECOND tick must NOT fire a second time (no flooding while waiting);
    only the first boundary tick of the minute fires, same as always."""
    process_start = datetime.now(IST).replace(hour=14, minute=55, second=6, microsecond=0)
    s, calls = _strategy(process_start)

    with _no_rules_runtime_config():
        t1 = process_start.replace(second=10)
        asyncio.run(s._maybe_try_entry(t1))
        t2 = process_start.replace(second=40)
        asyncio.run(s._maybe_try_entry(t2))
        assert calls == [(t1, True, False)]
        assert s._primed is False
