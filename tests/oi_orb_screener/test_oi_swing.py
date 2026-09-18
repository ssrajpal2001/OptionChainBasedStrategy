"""
Unit tests for strategies/oi_orb_screener/oi_swing.py -- the pure OI-swing
structure/breakout functions (update_swing_state, check_oi_swing_breakout,
floor_to_bucket) kept for reuse by the new entry/exit design's own Section
16. 2026-09-18: entry_exit_mode="oi_swing_v1" itself (the entry trigger and
its two production-fix gates -- entry cutoff, minimum hold) was removed
entirely along with its own tests here.
"""
from datetime import datetime

from config.global_config import IST
from strategies.oi_orb_screener import oi_swing


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
