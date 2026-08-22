"""
tests/strategies/test_sell_straddle_expiry_shift_low_anchor_ltp.py --
regression for the 2026-08-23 direct user spec: "if ltp is less then
threshold then jump to next week expriy -- applicable for anchor
selection part... and if we have entered next expiry, that expiry will
be used for the complete trading day til EOD."

Covers SellStraddleStrategy._maybe_shift_expiry_for_low_anchor_ltp
(entries.py) and the sticky guard added to _effective_entry_expiry()
(engine.py).
"""
import asyncio
from datetime import date, datetime

import pytest

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY
from strategies.sell_straddle import SellStraddleStrategy


CURRENT_EXPIRY = date(2026, 8, 27)
NEXT_EXPIRY = date(2026, 9, 3)


@pytest.fixture(autouse=True)
def _restore_registry_expiries():
    """REGISTRY is a process-wide singleton -- tests in this file overwrite
    REGISTRY._expiries["NIFTY"] wholesale (including one test that reduces
    it to a single entry to simulate "no next expiry loaded"). Without
    restoring the real value afterward, any OTHER test file that runs later
    in the same pytest session and relies on REGISTRY._expiries["NIFTY"]
    holding realistic data would silently see this file's leftover test
    fixture instead -- confirmed as a real, live bug: test_slope_ordering.py
    failed when run as part of the full suite but passed in isolation,
    until this fixture was added."""
    original = REGISTRY._expiries.get("NIFTY")
    yield
    if original is None:
        REGISTRY._expiries.pop("NIFTY", None)
    else:
        REGISTRY._expiries["NIFTY"] = original


def _seed_registry_expiries(underlying="NIFTY"):
    REGISTRY._expiries[underlying] = [CURRENT_EXPIRY, NEXT_EXPIRY]


def _strategy(spot=24512.0, entry_expiry=CURRENT_EXPIRY):
    _seed_registry_expiries()
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = spot
    s._entry_expiry_date = entry_expiry
    s._expiry_shifted_low_anchor_ltp = False
    s._is_crypto = False
    s._rebalancer = None   # _subscribe_expiry_window no-ops safely without one
    return s


def _low_anchor_strike_prem():
    # spot=24512 -> atm=24500 (step 50). PE has lower time value near ATM
    # (no intrinsic either side at spot~=strike) -> PE is the anchor.
    # ltp=20 will fail a ltp_target=50 floor.
    return {
        (24500, "CE"): {"ltp": 60.0, "atp": 60.0},
        (24500, "PE"): {"ltp": 20.0, "atp": 20.0},
    }


def _healthy_strike_prem():
    return {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
    }


# ── _maybe_shift_expiry_for_low_anchor_ltp ───────────────────────────────────

def test_no_shift_when_anchor_ltp_is_healthy():
    s = _strategy()
    s._strike_prem = _healthy_strike_prem()
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False
    assert s._entry_expiry_date == CURRENT_EXPIRY
    assert s._expiry_shifted_low_anchor_ltp is False


def test_shifts_to_next_week_when_anchor_ltp_below_floor():
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is True
    assert s._entry_expiry_date == NEXT_EXPIRY
    assert s._expiry_shifted_low_anchor_ltp is True


def test_shift_clears_strike_prem_cache():
    """Stale current-week LTPs cached under the same (strike, side) keys
    must not linger and get misread as next week's real prices once the
    option loop starts accepting next-week ticks."""
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert s._strike_prem == {}


def test_no_shift_once_already_shifted_today():
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    s._expiry_shifted_low_anchor_ltp = True   # already shifted earlier today
    s._entry_expiry_date = NEXT_EXPIRY
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False
    assert s._entry_expiry_date == NEXT_EXPIRY   # unchanged


def test_no_shift_for_crypto():
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    s._is_crypto = True
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False
    assert s._expiry_shifted_low_anchor_ltp is False


def test_no_shift_when_no_further_expiry_exists():
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    REGISTRY._expiries["NIFTY"] = [CURRENT_EXPIRY]   # no next expiry loaded
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False
    assert s._entry_expiry_date == CURRENT_EXPIRY


def test_no_shift_when_not_on_current_week_expiry():
    """If some other mechanism (e.g. the existing expiry-day shift) already
    moved entry_expiry_date off the current week, this trigger has nothing
    further to do."""
    s = _strategy(entry_expiry=NEXT_EXPIRY)
    s._strike_prem = _low_anchor_strike_prem()
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False
    assert s._expiry_shifted_low_anchor_ltp is False


def test_no_shift_when_atm_not_resolvable():
    s = _strategy(spot=0.0)
    s._strike_prem = {}
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))
    assert shifted is False


# ── _effective_entry_expiry() stickiness ─────────────────────────────────────

def test_effective_entry_expiry_sticky_once_shifted():
    s = _strategy()
    s._entry_expiry_date = NEXT_EXPIRY
    s._expiry_shifted_low_anchor_ltp = True
    # Even though today isn't the expiry day (no date-based reason to be on
    # next week), the sticky flag must win over the normal recompute.
    assert s._effective_entry_expiry() == NEXT_EXPIRY


def test_effective_entry_expiry_not_sticky_before_shift():
    """Before the sticky flag is set, the method must actually recompute
    from the registry -- not just blindly echo back whatever
    self._entry_expiry_date currently happens to hold."""
    s = _strategy()
    # An arbitrary value that could never come out of a real REGISTRY-based
    # computation (REGISTRY only knows CURRENT_EXPIRY/NEXT_EXPIRY here) --
    # if the sticky (early-return) branch fired despite the flag being
    # False, this exact bogus date would come straight back out.
    s._entry_expiry_date = date(2099, 1, 1)
    s._expiry_shifted_low_anchor_ltp = False
    result = s._effective_entry_expiry()
    assert result != date(2099, 1, 1)
    assert result in (CURRENT_EXPIRY, NEXT_EXPIRY, None)


def test_reset_session_clears_the_sticky_flag():
    s = _strategy()
    s._entry_expiry_date = NEXT_EXPIRY
    s._expiry_shifted_low_anchor_ltp = True
    s.reset_session()
    assert s._expiry_shifted_low_anchor_ltp is False


# ── _eval_ruleset integration: skip selection the cycle a shift happens ─────

def test_eval_ruleset_skips_selection_on_the_shift_cycle():
    s = _strategy()
    s._strike_prem = _low_anchor_strike_prem()
    s._ltp_target = 50.0
    s._theta_target = 0.0
    s._is_primed = lambda now, rules: True
    called = {"selection": False}

    async def _boom_if_called(*a, **k):
        called["selection"] = True
    s._eval_beginning_near_far = _boom_if_called

    from data_layer.runtime_config import RuntimeConfig
    import unittest.mock as _mock
    with _mock.patch.object(RuntimeConfig, "index_section", return_value={"entry_rules_beginning": []}):
        asyncio.run(s._eval_ruleset(datetime.now(IST), "entry_rules_beginning", use_beginning_sel=True))

    assert called["selection"] is False, "selection must be skipped the same cycle a shift just happened"
    assert s._entry_expiry_date == NEXT_EXPIRY
