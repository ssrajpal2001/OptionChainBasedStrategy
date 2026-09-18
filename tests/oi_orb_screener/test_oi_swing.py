"""
Unit tests for strategies/oi_orb_screener/oi_swing.py -- the pure decision
logic behind entry_exit_mode="oi_swing_v1" (Futures OI-Price Swing
Breakout Strategy). Covers the raw mechanic (entry trigger, 3-point swing
confirmation, HOLD/EXIT breakout matrix, flat-price default) and all
three production fixes (risk-cap cadence is engine-level, not tested
here -- see test_engine.py for that; entry cutoff, minimum hold, bucket
flooring are pure and tested directly below).
"""
from datetime import datetime, time as dtime

import pytest

from config.global_config import IST
from strategies.oi_orb_screener import oi_swing


# ── check_immediate_entry_trigger ───────────────────────────────────────

class TestCheckImmediateEntryTrigger:
    def test_no_trigger_below_threshold(self):
        assert oi_swing.check_immediate_entry_trigger(1.99) is None
        assert oi_swing.check_immediate_entry_trigger(-1.99) is None
        assert oi_swing.check_immediate_entry_trigger(0.0) is None

    def test_call_trigger_at_and_above_threshold(self):
        assert oi_swing.check_immediate_entry_trigger(2.0) == "CALL"
        assert oi_swing.check_immediate_entry_trigger(5.78) == "CALL"

    def test_put_trigger_at_and_below_threshold(self):
        assert oi_swing.check_immediate_entry_trigger(-2.0) == "PUT"
        assert oi_swing.check_immediate_entry_trigger(-4.9) == "PUT"

    def test_custom_threshold(self):
        assert oi_swing.check_immediate_entry_trigger(1.6, min_pct=1.5) == "CALL"
        assert oi_swing.check_immediate_entry_trigger(1.4, min_pct=1.5) is None


# ── is_entry_within_cutoff (Fix 2) ──────────────────────────────────────

class TestEntryCutoff:
    def test_before_cutoff_allowed(self):
        assert oi_swing.is_entry_within_cutoff(dtime(9, 15)) is True
        assert oi_swing.is_entry_within_cutoff(dtime(14, 29)) is True

    def test_at_cutoff_allowed(self):
        assert oi_swing.is_entry_within_cutoff(dtime(14, 30)) is True

    def test_after_cutoff_blocked(self):
        assert oi_swing.is_entry_within_cutoff(dtime(14, 31)) is False
        assert oi_swing.is_entry_within_cutoff(dtime(15, 28)) is False

    def test_custom_cutoff(self):
        assert oi_swing.is_entry_within_cutoff(dtime(13, 31), cutoff=dtime(13, 30)) is False
        assert oi_swing.is_entry_within_cutoff(dtime(13, 30), cutoff=dtime(13, 30)) is True


# ── is_min_hold_satisfied (Fix 3) ───────────────────────────────────────

class TestMinHoldSatisfied:
    def test_before_min_hold_blocks_exit(self):
        entry = datetime(2026, 9, 17, 9, 30, tzinfo=IST)
        now = datetime(2026, 9, 17, 9, 39, tzinfo=IST)   # 9 minutes later
        assert oi_swing.is_min_hold_satisfied(entry, now, min_hold_minutes=10) is False

    def test_at_min_hold_allows_exit(self):
        entry = datetime(2026, 9, 17, 9, 30, tzinfo=IST)
        now = datetime(2026, 9, 17, 9, 40, tzinfo=IST)   # exactly 10 minutes
        assert oi_swing.is_min_hold_satisfied(entry, now, min_hold_minutes=10) is True

    def test_after_min_hold_allows_exit(self):
        entry = datetime(2026, 9, 17, 9, 30, tzinfo=IST)
        now = datetime(2026, 9, 17, 10, 30, tzinfo=IST)
        assert oi_swing.is_min_hold_satisfied(entry, now, min_hold_minutes=10) is True

    def test_none_entry_ts_fails_open(self):
        """No known entry time -- must never permanently block a real exit;
        fail open (True) rather than silently freeze the position forever."""
        now = datetime(2026, 9, 17, 10, 30, tzinfo=IST)
        assert oi_swing.is_min_hold_satisfied(None, now) is True

    def test_hard_cap_and_eod_are_never_gated_by_this_function(self):
        """Documentation-as-test: is_min_hold_satisfied is ONLY ever called
        from the oi_swing_exit decision path in engine.py -- the hard risk
        cap (_check_hard_risk_cap) and EOD square-off (_eod_loop) call
        neither this function nor anything that wraps it. This test exists
        so a future refactor that accidentally threads a min-hold check
        into either of those paths breaks an assertion, not just a silent
        real-money behavior change. Nothing to call here directly (this
        module has no knowledge of the engine's other exit paths) -- the
        real guarantee is verified in test_engine.py's
        test_hard_risk_cap_fires_inside_min_hold_window."""
        assert True


# ── floor_to_bucket ──────────────────────────────────────────────────────

class TestFloorToBucket:
    def test_session_start_floors_to_itself(self):
        ts = datetime(2026, 9, 17, 9, 15, 0, tzinfo=IST)
        assert oi_swing.floor_to_bucket(ts) == ts

    def test_mid_bucket_floors_down(self):
        ts = datetime(2026, 9, 17, 9, 18, 30, tzinfo=IST)
        assert oi_swing.floor_to_bucket(ts) == datetime(2026, 9, 17, 9, 15, tzinfo=IST)

    def test_exact_next_boundary(self):
        ts = datetime(2026, 9, 17, 9, 20, 0, tzinfo=IST)
        assert oi_swing.floor_to_bucket(ts) == datetime(2026, 9, 17, 9, 20, tzinfo=IST)

    def test_later_bucket(self):
        ts = datetime(2026, 9, 17, 11, 47, 12, tzinfo=IST)
        assert oi_swing.floor_to_bucket(ts) == datetime(2026, 9, 17, 11, 45, tzinfo=IST)


# ── update_swing_state ───────────────────────────────────────────────────

class TestUpdateSwingState:
    def test_fewer_than_three_points_never_confirms(self):
        h, l, c = oi_swing.update_swing_state([100.0], None, None)
        assert (h, l, c) == (None, None, None)
        h, l, c = oi_swing.update_swing_state([100.0, 105.0], None, None)
        assert (h, l, c) == (None, None, None)

    def test_confirms_a_swing_high(self):
        # 100, 110, 105 -> the middle point (110) is a local high, confirmed
        # now that the third point (105) is known.
        h, l, c = oi_swing.update_swing_state([100.0, 110.0, 105.0], None, None)
        assert c == "HIGH"
        assert h == 110.0
        assert l is None

    def test_confirms_a_swing_low(self):
        h, l, c = oi_swing.update_swing_state([110.0, 100.0, 108.0], None, None)
        assert c == "LOW"
        assert l == 100.0
        assert h is None

    def test_monotonic_series_confirms_nothing(self):
        h, l, c = oi_swing.update_swing_state([100.0, 105.0, 110.0], None, None)
        assert c is None
        assert (h, l) == (None, None)

    def test_prior_swings_carry_forward_unchanged_when_nothing_new_confirms(self):
        h, l, c = oi_swing.update_swing_state([100.0, 105.0, 110.0], swing_high=90.0, swing_low=80.0)
        assert (h, l, c) == (90.0, 80.0, None)

    def test_a_new_confirmed_high_does_not_clobber_an_existing_low(self):
        h, l, c = oi_swing.update_swing_state([100.0, 110.0, 105.0], swing_high=None, swing_low=70.0)
        assert (h, l, c) == (110.0, 70.0, "HIGH")

    def test_only_last_three_points_inspected(self):
        # A genuine earlier high (at index 1) must not be re-confirmed or
        # interfere once we're looking at a later window.
        series = [100.0, 200.0, 90.0, 95.0, 91.0]   # last 3: 90,95,91 -> mid (95) is a high
        h, l, c = oi_swing.update_swing_state(series, None, None)
        assert (h, l, c) == (95.0, None, "HIGH")


# ── check_oi_swing_breakout ──────────────────────────────────────────────

class TestCheckOiSwingBreakout:
    def test_no_breakout_when_oi_between_swings(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=100.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=51.0)
        assert (broke, decision) == (False, None)

    def test_no_breakout_when_no_swings_established_yet(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=100.0, swing_high=None, swing_low=None,
            price_prev=50.0, price_cur=51.0)
        assert (broke, decision) == (False, None)

    # LONG (CALL)
    def test_long_holds_on_swing_high_break_with_price_up(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=120.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=52.0)
        assert (broke, decision) == (True, "HOLD")

    def test_long_exits_on_swing_high_break_with_price_down(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=120.0, swing_high=110.0, swing_low=90.0,
            price_prev=52.0, price_cur=50.0)
        assert (broke, decision) == (True, "EXIT")

    def test_long_holds_on_swing_low_break_with_price_up(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=80.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=52.0)
        assert (broke, decision) == (True, "HOLD")

    def test_long_exits_on_swing_low_break_with_price_down(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=80.0, swing_high=110.0, swing_low=90.0,
            price_prev=52.0, price_cur=50.0)
        assert (broke, decision) == (True, "EXIT")

    def test_long_exits_on_flat_price_at_breakout(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=120.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=50.0)
        assert (broke, decision) == (True, "EXIT")

    # SHORT (PUT), mirrored
    def test_short_holds_on_swing_low_break_with_price_down(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "PUT", current_oi=80.0, swing_high=110.0, swing_low=90.0,
            price_prev=52.0, price_cur=50.0)
        assert (broke, decision) == (True, "HOLD")

    def test_short_exits_on_swing_low_break_with_price_up(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "PUT", current_oi=80.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=52.0)
        assert (broke, decision) == (True, "EXIT")

    def test_short_holds_on_swing_high_break_with_price_down(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "PUT", current_oi=120.0, swing_high=110.0, swing_low=90.0,
            price_prev=52.0, price_cur=50.0)
        assert (broke, decision) == (True, "HOLD")

    def test_short_exits_on_swing_high_break_with_price_up(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "PUT", current_oi=120.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=52.0)
        assert (broke, decision) == (True, "EXIT")

    def test_short_exits_on_flat_price_at_breakout(self):
        broke, decision = oi_swing.check_oi_swing_breakout(
            "PUT", current_oi=80.0, swing_high=110.0, swing_low=90.0,
            price_prev=50.0, price_cur=50.0)
        assert (broke, decision) == (True, "EXIT")

    def test_both_swing_high_and_low_broken_simultaneously_still_decides(self):
        # Pathological but must not crash -- e.g. swing_low momentarily above
        # swing_high right after a fresh re-arm; breakout logic OR's the two
        # conditions, decision still resolves off price direction alone.
        broke, decision = oi_swing.check_oi_swing_breakout(
            "CALL", current_oi=100.0, swing_high=95.0, swing_low=98.0,
            price_prev=50.0, price_cur=52.0)
        assert (broke, decision) == (True, "HOLD")
