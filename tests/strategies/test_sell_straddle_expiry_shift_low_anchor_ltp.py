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


def _raw_atm_passes_but_1otm_anchor_fails_strike_prem():
    """2026-08-24 -> 2026-08-26 history: an earlier version of this fix made
    this call check the SHIFTED 1-OTM anchor's own floor instead of raw
    ATM's, reasoning that the real traded leg (1-OTM for BEGINNING) could be
    below floor even while raw ATM passed. Direct user correction on
    2026-08-26: that was wrong -- "otm ltp and theta will not be checked...
    it will be selected directly." The floor gate is ATM-only, always,
    regardless of use_beginning_sel; the OTM leg is never re-checked against
    it. This fixture (raw ATM PE=55 passes a 50 floor, 1-OTM PE@24450=15
    would fail it) now exists specifically to prove NO shift happens in
    either case -- the OTM value is irrelevant to this decision."""
    return {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 55.0, "atp": 55.0},     # raw ATM -- passes a 50 floor
        (24450, "PE"): {"ltp": 15.0, "atp": 15.0},     # 1-OTM value -- irrelevant now
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


def test_shift_resets_pool_engine():
    """2026-08-31 CRITICAL FIX, real incident: self._pool_engine (the REAL
    VWAP/SLOPE/RSI/ROC source every entry rule reads) was never reset on a
    shift -- its per-(strike,side) series is keyed by strike NUMBER alone,
    which repeats across different weekly contracts, so it kept blending
    the OLD contract's price history into the NEW contract's incoming ticks
    under the same key. Confirmed live via the shadow-VWAP diagnostic
    (~77pt gap between the real broker ATP, already on the new contract,
    and the pool-engine-derived value, still anchored to the old one)."""
    from datetime import timedelta
    today = datetime.now(IST).date()
    _today_expiry = today + timedelta(days=1)
    _today_next_expiry = today + timedelta(days=8)
    s = _strategy(entry_expiry=_today_expiry)
    REGISTRY._expiries["NIFTY"] = [_today_expiry, _today_next_expiry]
    s._strike_prem = _low_anchor_strike_prem()

    # Seed the pool engine with data as if strike 24500 CE had been ticking
    # all day on the OLD (current-week) contract.
    s._pool_engine.update_tick(24500, "CE", ltp=184.25, atp=180.0)
    old_engine_id = id(s._pool_engine)
    assert s._pool_engine._latest.get((24500, "CE")) is not None

    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(ltp_target=50.0, theta_target=0.0))

    assert shifted is True
    assert id(s._pool_engine) != old_engine_id, "must be a genuinely fresh instance, not the same one cleared in place"
    assert s._pool_engine._latest == {}, "old contract's price history must not survive the shift"
    # Same rsi_len/roc_len/maxlen the original instance was built with.
    assert s._pool_engine._rsi_len == 14
    assert s._pool_engine._roc_len == 10


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


# ── 2026-08-26 direct user correction: floor gate is ATM-only, always ───────
# An earlier (2026-08-24) version of this fix checked the shifted 1-OTM
# anchor's own floor for BEGINNING. Direct user correction: "otm ltp and
# theta will not be checked ... it will be selected directly" -- the floor
# decision is made at ATM only, identically for BEGINNING and RE-ENTRY.

def test_beginning_does_not_shift_when_raw_atm_passes_even_if_1otm_would_fail_floor():
    """Raw ATM anchor LTP (55) clears the floor (50) -- the fact that the
    1-OTM strike BEGINNING actually trades (PE@24450=15) would itself fail
    that same floor is irrelevant: the OTM leg is never checked, only ATM
    decides whether to shift. use_beginning_sel=True must NOT shift here."""
    s = _strategy()
    s._strike_prem = _raw_atm_passes_but_1otm_anchor_fails_strike_prem()
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(
        ltp_target=50.0, theta_target=0.0, use_beginning_sel=True))
    assert shifted is False
    assert s._entry_expiry_date == CURRENT_EXPIRY


def test_reentry_does_not_shift_on_the_same_data():
    """RE-ENTRY's own real selection (select_balanced_pair, anchor_otm_
    steps=0 always) genuinely trades the raw ATM anchor -- raw ATM passing
    the floor must NOT shift, same as BEGINNING now (both are ATM-only)."""
    s = _strategy()
    s._strike_prem = _raw_atm_passes_but_1otm_anchor_fails_strike_prem()
    shifted = asyncio.run(s._maybe_shift_expiry_for_low_anchor_ltp(
        ltp_target=50.0, theta_target=0.0, use_beginning_sel=False))
    assert shifted is False
    assert s._entry_expiry_date == CURRENT_EXPIRY


def test_default_use_beginning_sel_is_false_backward_compatible():
    """Every pre-existing call site/test in this file omits use_beginning_
    sel -- must default to False and still not shift on this healthy-ATM
    fixture, same as every other case above."""
    s = _strategy()
    s._strike_prem = _raw_atm_passes_but_1otm_anchor_fails_strike_prem()
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

def test_beginning_shifts_to_next_week_when_near_and_far_both_exhausted(monkeypatch):
    """2026-08-31, direct user spec: 'when we jump to the OTM and the pair we
    are looking for is not available due to threshold, we will jump to next
    week' -- same next-week safety net the raw-anchor-fails-floor case
    already uses, now ALSO firing when BEGINNING's near/far selection tries
    both candidates (each with its own 1-OTM shift + partner search) and
    BOTH come up with no viable pair.

    Seeds REGISTRY expiries relative to REAL today (not the file's other
    hardcoded CURRENT_EXPIRY/NEXT_EXPIRY, which are fixed past dates and
    already the cause of this file's 3 other, pre-existing/unrelated
    failures -- REGISTRY.get_active_expiry resolves against the real wall-
    clock date, so a fixed past date no longer round-trips.)"""
    from datetime import timedelta
    today = datetime.now(IST).date()
    _today_expiry = today + timedelta(days=1)
    _today_next_expiry = today + timedelta(days=8)
    s = _strategy(spot=24512.0, entry_expiry=_today_expiry)
    # _strategy() itself seeds the file's own stale hardcoded dates -- override
    # AFTER construction so this test's own dynamic dates actually stick.
    REGISTRY._expiries["NIFTY"] = [_today_expiry, _today_next_expiry]
    # Sparse strike_prem: only the two ATM anchor strikes have any data at
    # all -- no partner candidates exist anywhere, so select_balanced_pair_at
    # must return None for BOTH near(24500) and far(24550).
    s._strike_prem = {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
    }
    s._entry_basis = "ltp"
    s._balance_ratio = 1.0

    asyncio.run(s._eval_beginning_near_far(
        datetime.now(IST), "entry_rules_beginning", [], step=50.0, offset=7,
        ltp_target=50.0, theta_target=0.0, variable_strikes=False, balance_ratio=1.0,
    ))

    assert s._expiry_shifted_low_anchor_ltp is True
    assert s._entry_expiry_date == _today_next_expiry
    assert s._strike_prem == {}   # cleared by the shift, same as the original trigger


def test_beginning_does_not_shift_when_at_least_one_candidate_finds_a_pair():
    """Only ONE of near/far needs a viable pair (even if it fails entry
    rules, e.g. SLOPE) for the shift to NOT fire -- exhaustion means neither
    candidate found ANY pair at all, not that neither one traded."""
    s = _strategy(spot=24512.0)
    s._strike_prem = {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
        (24450, "CE"): {"ltp": 210.0, "atp": 205.0},   # near's 1-OTM anchor shift target
        (24550, "PE"): {"ltp": 90.0, "atp": 88.0},     # a real partner candidate for near
    }
    s._entry_basis = "ltp"
    s._balance_ratio = 1.0

    asyncio.run(s._eval_beginning_near_far(
        datetime.now(IST), "entry_rules_beginning", [], step=50.0, offset=7,
        ltp_target=50.0, theta_target=0.0, variable_strikes=False, balance_ratio=1.0,
    ))

    assert s._expiry_shifted_low_anchor_ltp is False
    assert s._entry_expiry_date == CURRENT_EXPIRY


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
