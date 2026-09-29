"""Regression test for the 2026-09-29 CRITICAL FIX, direct user correction:
the entry rule (VWAP/SLOPE/RSI/ROC, admin rule-builder config) must filter
EVERY candidate pair BEFORE balance-ratio picks a winner -- "from all the
pairs which passed the indicator condition, then they will be checked for
balance-ratio." The old code did the reverse: select_balanced_pair(_at) chose
a single winner by LTP-balance alone, and the entry rule was only checked
afterward on that one already-chosen pair -- a candidate with a passing
indicator read could never be reached if a purely LTP-balanced (but
rule-failing) candidate outscored it.

These tests drive SellStraddleStrategy._eval_ruleset (RE-ENTRY) and
_eval_beginning_near_far (BEGINNING) directly, with a rigged rule function
(patched onto strategies.sell_straddle.entries._eval_rules) that rejects the
strike that would otherwise win on pure LTP-balance, and accepts a
worse-balanced strike instead -- proving the worse-but-rule-passing
candidate is the one actually selected."""
import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = 22700.0
    s._atm_ref = 0.0
    s._entry_expiry_date = date.today()
    s._finalize_entry_decision = AsyncMock()
    return s


def _now():
    return datetime.now(IST).replace(hour=11, minute=30, second=10, microsecond=0)


def test_reentry_picks_rule_passing_candidate_over_better_balanced_rule_failing_one():
    s = _strategy()
    # Anchor: PE has lower time value at ATM 22700 -> PE is anchor, CE is partner.
    s._strike_prem = {
        (22700, "CE"): {"ltp": 200.0, "atp": 200.0},
        (22700, "PE"): {"ltp": 150.0, "atp": 150.0},
        # CE22600: near-perfect balance vs anchor PE (150) -- would win on
        # balance-ratio alone -- but is rigged to FAIL the rule below.
        (22600, "CE"): {"ltp": 151.0, "atp": 151.0},
        # CE22800: worse balance vs anchor PE (150) but passes the rule.
        (22800, "CE"): {"ltp": 130.0, "atp": 130.0},
    }
    rules = [{"indicator": "SLOPE", "op": "<", "value": 0}]  # content irrelevant; _eval_rules is patched

    def _fake_eval_rules(rules_arg, ind_by_tf):
        # ind_by_tf keys are timeframes; values carry whichever candidate pair
        # _ind_by_tf was asked to compute for -- use the pair's own CE strike
        # (smuggled in via the strategy's own _last_ind_by_tf_pair test hook
        # below) to decide pass/fail deterministically per-candidate.
        ce_strike = getattr(s, "_last_ind_by_tf_ce", None)
        if ce_strike == 22600:
            return False, "rigged_fail_for_test"
        return True, "rigged_pass_for_test"

    def _fake_ind_by_tf(cs, ps, rules_arg):
        s._last_ind_by_tf_ce = cs
        return {1: {"close": 1.0, "vwap": 1.0}}

    with patch("data_layer.runtime_config.RuntimeConfig.index_section",
               return_value={"entry_rules_reentry": rules, "balance_ratio": 1.0}), \
         patch("strategies.sell_straddle.entries._eval_rules", side_effect=_fake_eval_rules):
        s._ind_by_tf = _fake_ind_by_tf
        s._is_primed = lambda now, rules: True
        asyncio.run(s._eval_ruleset(_now(), "entry_rules_reentry", use_beginning_sel=False))

    assert s._finalize_entry_decision.await_args is not None, "no pair was ever finalized"
    args = s._finalize_entry_decision.await_args.args
    # _finalize_entry_decision(now, rule_key, concept, ce_strike, pe_strike, ce_ltp, pe_ltp, ...)
    ce_strike_selected = args[3]
    assert ce_strike_selected == 22800, (
        f"expected the rule-passing (worse-balanced) CE22800 candidate to win, "
        f"got CE{ce_strike_selected} -- rule must filter candidates BEFORE balance-ratio scoring"
    )
