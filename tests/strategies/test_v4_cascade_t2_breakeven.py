"""strategies/v4_cascade/exits.py's TrailingBaseTracker -- 2026-07-21, two
fixes per explicit user direction:

1. T2 previously had ZERO stop-loss protection from entry until the
   tracker's OWN scanner happened to lock its first new base (current_stop
   started at None, and check_hit() returns hit=False unconditionally while
   it's None) -- a fast adverse move right after entry had nothing to check
   against. Fixed by seeding current_stop with T2's own entry-time
   structural SL (same zone_low-buffer level T1 gets), via a new
   `initial_stop` constructor param.

2. Once T1's target (not its SL) hits, T2's stop now ratchets up (long) /
   down (short) to breakeven immediately -- guarantees the combined position
   can no longer net a loss after T1 has already booked profit. Never
   regresses an already more-favorable trail.
"""
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.exits import TrailingBaseTracker

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 21, 12, 0, tzinfo=IST)


def _bar(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


# ── TrailingBaseTracker unit tests ──────────────────────────────────────────

def test_seeded_initial_stop_protects_before_any_base_locks():
    """The exact live gap: no scanner activity has happened yet (fresh
    entry), but the seeded floor must still catch a fast adverse move."""
    trail = TrailingBaseTracker(bear=True, initial_stop=100.0)
    hit = trail.check_hit(_bar(0, 105, 106, 90, 95))  # low=90 breaches 100
    assert hit.hit is True
    assert hit.price == 100.0
    assert hit.reason == "t2_trailing_base_stop"


def test_no_seed_means_no_protection_until_a_base_locks_unchanged():
    """Regression guard: omitting initial_stop preserves the OLD (still
    legitimate) behavior of no protection until the scanner locks a base --
    this constructor default must not itself change."""
    trail = TrailingBaseTracker(bear=True)
    hit = trail.check_hit(_bar(0, 105, 106, 1, 95))  # even a huge drop
    assert hit.hit is False


def test_move_to_breakeven_raises_stop_when_below_entry_long():
    trail = TrailingBaseTracker(bear=True, initial_stop=100.0)
    trail.move_to_breakeven(150.0)
    assert trail.current_stop == 150.0


def test_move_to_breakeven_never_regresses_already_favorable_trail_long():
    trail = TrailingBaseTracker(bear=True, initial_stop=100.0)
    trail.current_stop = 200.0  # a base already locked above entry
    trail.move_to_breakeven(150.0)
    assert trail.current_stop == 200.0  # unchanged -- no regression


def test_move_to_breakeven_lowers_stop_when_above_entry_short():
    trail = TrailingBaseTracker(bear=False, initial_stop=250.0)
    trail.move_to_breakeven(200.0)
    assert trail.current_stop == 200.0


def test_move_to_breakeven_never_regresses_already_favorable_trail_short():
    trail = TrailingBaseTracker(bear=False, initial_stop=250.0)
    trail.current_stop = 150.0  # already better (lower) than entry
    trail.move_to_breakeven(200.0)
    assert trail.current_stop == 150.0  # unchanged -- no regression


# ── Engine-level integration: T1 target hit moves T2 to breakeven ──────────

def test_t1_target_hit_moves_t2_trail_to_breakeven():
    eng = V4CascadeEngine()
    entry_price = 100.0
    sl_price = 80.0       # T2's seeded floor, well below entry
    target_price = 120.0  # T1's target
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=0.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, target_price=target_price, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=0.0, qty=65,
                     entry_price=entry_price, sl_price=sl_price, status="open")
    eng.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=0.0, execution_strike=0.0,
        atm_at_trigger=0.0, entry_spot=0.0, t1=t1, t2=t2, open_time=_BASE,
    )
    eng._tracking_entry_price["CE"] = entry_price
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=sl_price)
    breakeven_buffer = eng._cfg.sl_buffer   # default V4CascadeConfig -- 10.0 (NIFTY)

    # Bar's high clears T1's target (120); low stays above T2's new
    # breakeven+buffer floor (entry_price + breakeven_buffer = 110) so T2
    # itself doesn't also close this same bar.
    events = eng._check_exits("CE", _bar(0, 115, 125, 112, 118))

    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_target_2r"
    assert t1.status == "closed"
    assert t2.status == "open"  # T2 itself did not close
    expected_breakeven = entry_price + breakeven_buffer
    assert eng._trackers["CE"].current_stop == expected_breakeven
    assert t2.trail_stop_price == expected_breakeven


def test_breakeven_uses_tracking_scale_not_post_fill_execution_price():
    """The exact live bug: book.py's _on_fill overwrites t1/t2.entry_price
    with the REAL EXECUTION fill almost immediately after entry -- well
    before T1 could ever hit target 5+ minutes later. So by the time the
    breakeven ratchet fires, t2.entry_price is already execution-scale
    (545.0 in the real trade), NOT the tracking-scale price (719.87) that
    current_stop must be expressed in (it's compared against TRACKING bars
    everywhere else). Using t2.entry_price there silently corrupted
    current_stop onto the wrong scale."""
    eng = V4CascadeEngine()
    # Same numbers as the real live CRUDEOIL trade this bug was found in.
    tracking_entry = 719.8666666666667
    exec_entry = 545.0   # what t2.entry_price becomes after _on_fill runs
    t1 = TrancheLeg(tranche="T1", option_type="PE", strike=0.0, qty=100,
                     entry_price=exec_entry, sl_price=421.0, target_price=722.3, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="PE", strike=0.0, qty=100,
                     entry_price=exec_entry, sl_price=421.0, status="open")
    eng.position = CascadePosition(
        underlying="CRUDEOIL", side="PE", tracking_strike=0.0, execution_strike=0.0,
        atm_at_trigger=0.0, entry_spot=0.0, t1=t1, t2=t2, open_time=_BASE,
    )
    eng._tracking_entry_price["PE"] = tracking_entry
    eng._trackers["PE"] = TrailingBaseTracker(bear=True, initial_stop=421.0)
    breakeven_buffer = eng._cfg.sl_buffer   # default V4CascadeConfig -- 10.0

    # Bar's high clears T1's target (722.3, tracking scale); low (730.0)
    # stays above the NEW breakeven+buffer level (tracking_entry + 10 =
    # 729.87) so T2 doesn't also close in this same bar -- all values on the
    # TRACKING scale, same as every real bar _check_exits ever receives.
    events = eng._check_exits("PE", _bar(0, 731, 735, 730.0, 733))

    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1 and fired[0].reason == "t1_target_2r"
    # current_stop must land on the TRACKING scale (719.87 + buffer), not
    # the execution-scale entry price (545.0) -- the bug's exact symptom.
    expected_tracking_breakeven = tracking_entry + breakeven_buffer
    assert eng._trackers["PE"].current_stop == expected_tracking_breakeven
    # Mapped to execution scale using T2's real entry (545.0) -- not some
    # other, nonsensical value.
    scale = exec_entry / tracking_entry
    expected_exec_breakeven = expected_tracking_breakeven * scale
    assert abs(t2.trail_stop_price - expected_exec_breakeven) < 1e-6


def test_t1_sl_hit_does_not_move_t2_to_breakeven():
    """Only a TARGET hit (profit) should trigger the breakeven ratchet --
    T1 stopping out at a loss must not also drag T2's floor up. T2's seeded
    floor is set well below the bar's low here (unlike production, where T1
    and T2 share the identical SL) purely to isolate this from T2 ALSO
    closing in the same bar, which would otherwise pop its tracker entirely
    and make the assertion moot."""
    eng = V4CascadeEngine()
    entry_price = 100.0
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=0.0, qty=65,
                     entry_price=entry_price, sl_price=80.0, target_price=120.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=0.0, qty=65,
                     entry_price=entry_price, sl_price=80.0, status="open")
    eng.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=0.0, execution_strike=0.0,
        atm_at_trigger=0.0, entry_spot=0.0, t1=t1, t2=t2, open_time=_BASE,
    )
    eng._tracking_entry_price["CE"] = entry_price
    eng._trackers["CE"] = TrailingBaseTracker(bear=True, initial_stop=50.0)

    # Bar's low (75) clears T1's SL (80) but stays above T2's floor (50).
    events = eng._check_exits("CE", _bar(0, 85, 90, 75, 82))

    fired = [e for e in events if e.tranche == "T1"]
    assert len(fired) == 1
    assert fired[0].reason == "t1_sl_structural_floor"
    assert t2.status == "open"
    assert eng._trackers["CE"].current_stop == 50.0  # unchanged, still the seeded floor
