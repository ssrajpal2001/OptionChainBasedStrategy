"""Regression tests for strategies/cag_straddle/detector.py -- the pure S&R
breach/standing-order mechanic (2026-08-27), reusing the REAL
SupportResistanceCalculator (strategies/d1_trap_option/support_resistance.py).

Bar sequences below were verified interactively against the real calculator
before being encoded here (not hand-guessed) -- see detector.py's own
module docstring for the validated mechanic these tests lock in.
"""
from datetime import datetime

from strategies.cag_straddle.detector import Bar, BarAccumulator, SideTracker, pick_strike


def _mk(m, o, h, l, c):
    return Bar(ts=datetime(2026, 1, 1, 15, m), open=o, high=h, low=l, close=c)


# ── BarAccumulator ───────────────────────────────────────────────────────────

def test_bar_accumulator_closes_on_new_minute():
    acc = BarAccumulator()
    assert acc.on_tick(datetime(2026, 1, 1, 15, 0, 10), 100.0) is None
    assert acc.on_tick(datetime(2026, 1, 1, 15, 0, 40), 102.0) is None
    closed = acc.on_tick(datetime(2026, 1, 1, 15, 1, 5), 101.0)
    assert closed is not None
    assert closed.open == 100.0 and closed.high == 102.0 and closed.low == 100.0
    assert len(acc.bars) == 1


# ── pick_strike ──────────────────────────────────────────────────────────────

def test_pick_strike_picks_closest_to_target():
    candidates = {24000: 180.0, 24050: 150.0, 24100: 95.0, 24150: 70.0}
    assert pick_strike(candidates, 100.0) == 24100


def test_pick_strike_empty_returns_none():
    assert pick_strike({}, 100.0) is None


# ── SideTracker: entry standing order ───────────────────────────────────────

def test_r1_breach_excludes_initial_trend_establishment():
    """The very first INITIAL_TREND_ESTABLISHMENT -> R1_TRACKING transition
    (a fresh base-candle breakout) must NEVER count as an r1_breach_event --
    only a later S2/R2_TRACKING -> R1_TRACKING re-breach does."""
    bars = [
        _mk(0, 95, 100, 90, 97),
        _mk(1, 97, 105, 92, 103),   # INITIAL_TREND_ESTABLISHMENT -> R1_TRACKING
    ]
    t = SideTracker()
    infos = [t.on_bar(b) for b in bars]
    assert infos[0]["phase_after"] == "INITIAL_TREND_ESTABLISHMENT"
    assert infos[1]["phase_before"] == "INITIAL_TREND_ESTABLISHMENT"
    assert infos[1]["phase_after"] == "R1_TRACKING"
    assert infos[1]["r1_breach_event"] is False


def test_entry_standing_order_persists_across_an_unconfirmed_bar():
    """A signal armed on one bar must stay live -- the very next bar not
    breaching it must NOT expire it; a LATER bar that does breach it fills
    at the originally-armed level."""
    bars = [
        _mk(0, 95, 100, 90, 97),
        _mk(1, 97, 105, 92, 103),   # -> R1_TRACKING
        _mk(2, 103, 104, 91, 102),  # -> S2_TRACKING (R1 established)
        _mk(3, 102, 106, 100, 105), # S2_TRACKING -> R1_TRACKING: R1 BREACH, arm @106
        _mk(4, 105, 103, 101, 102), # does NOT breach 106 -- order must stay live
        _mk(5, 102, 107, 101, 106), # breaches 106 -- FILLS here, not bar 4
    ]
    t = SideTracker()
    fills = []
    breaches = []
    for b in bars:
        info = t.on_bar(b)
        breaches.append(info["r1_breach_event"])
        fills.append(t.check_entry_fill(b, info["r1_breach_event"]))
    assert breaches[3] is True
    assert fills[3] is None    # armed, not filled on the same bar
    assert fills[4] is None    # bar 4 does not breach -- must NOT expire
    assert fills[5] == 106.0   # bar 5 (a later bar) fills at the armed level


# ── SideTracker: SL standing order ──────────────────────────────────────────

def test_sl_standing_order_persists_across_an_unconfirmed_bar():
    bars = [
        _mk(0, 95, 100, 90, 95),
        _mk(1, 95, 99, 88, 88),   # close=88 < s1_before(90) -> arm SL @ this bar's own low=88
        _mk(2, 88, 92, 89, 90),   # low=89, does NOT breach 88 -- must NOT expire
        _mk(3, 90, 91, 86, 87),   # low=86 breaches 88 -- FILLS here
    ]
    t = SideTracker()
    fills = []
    for b in bars:
        info = t.on_bar(b)
        fills.append(t.check_sl_fill(b, info["s1_before"]))
    assert fills[1] is None
    assert fills[2] is None    # unconfirmed bar must not expire the order
    assert fills[3] == 88.0    # a later bar fills at the armed level
