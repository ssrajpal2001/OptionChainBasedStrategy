"""
Regression test for the 2026-08-05 SellStraddle entry-flow fix (user-specified
hybrid contract):

  - BEGINNING is retried on every eligible cycle for as long as
    trades_today == 0 -- a single blocked BEGINNING check must NOT
    permanently lock it out for the rest of the day.
  - RE-ENTRY must never be evaluated at all until the first trade has
    actually happened (trades_today > 0).

Previously: `want_re` was unconditionally True in hybrid mode, so RE-ENTRY
ran in parallel with BEGINNING from the very first tick, even before any
trade existed; and a single failed BEGINNING check set `_beginning_failed`
permanently True, silently switching to RE-ENTRY-only for the rest of the
day even with trades_today still 0.
"""
import asyncio
from datetime import datetime

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = 24500.0
    s._ce_ltp = 150.0
    s._pe_ltp = 140.0
    calls = []

    async def _fake_eval_ruleset(now, rule_key, use_beginning_sel):
        calls.append(rule_key)
    s._eval_ruleset = _fake_eval_ruleset
    return s, calls


def _now():
    # Any daytime instant with second>=5 satisfies the default 1-min TF
    # boundary check; SellStraddleStrategy's default entry window covers this.
    return datetime.now(IST).replace(hour=11, minute=30, second=10, microsecond=0)


def test_beginning_only_evaluated_pre_first_trade():
    s, calls = _strategy()
    s._trades_today = 0
    asyncio.run(s._maybe_try_entry(_now()))
    assert calls == ["entry_rules_beginning"]


def test_beginning_retried_every_cycle_not_locked_out_after_a_block():
    """A BEGINNING check failing (the real _eval_ruleset would just return
    without passing) must not prevent it from being tried again on the next
    eligible cycle -- simulate by calling _maybe_try_entry twice with
    trades_today still 0 both times."""
    s, calls = _strategy()
    s._trades_today = 0
    now1 = _now()
    asyncio.run(s._maybe_try_entry(now1))
    # Full hour later so the TF-boundary dedup bucket (which includes hour)
    # differs regardless of whatever tf the real/configured beginning rules use.
    now2 = now1.replace(hour=now1.hour + 1)
    asyncio.run(s._maybe_try_entry(now2))
    assert calls == ["entry_rules_beginning", "entry_rules_beginning"]
    assert "entry_rules_reentry" not in calls


def test_reentry_only_evaluated_after_first_trade():
    s, calls = _strategy()
    s._trades_today = 1
    asyncio.run(s._maybe_try_entry(_now()))
    assert calls == ["entry_rules_reentry"]


def test_reentry_never_evaluated_while_trades_today_is_zero():
    s, calls = _strategy()
    s._trades_today = 0
    asyncio.run(s._maybe_try_entry(_now()))
    assert "entry_rules_reentry" not in calls
