"""strategies/v4_cascade/engine.py -- end-to-end coverage of the 2026-07-20
Index/Premium decoupling via V4CascadeEngine.update(): the Index gate (75m
spot) hard-gates premium Gate-2 discovery, the full funnel fires an
OPEN_LONG event, and the "never abort in-flight" decision (a fired setup
still opens even if the Index later flips away) -- plus a guardrail that
crypto's construction path is completely unaffected."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.zone_state import PremiumGateScanner, IndexGatedPremiumScanner

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 20, 9, 15, tzinfo=IST)


def _bar5(offset_5m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=5 * offset_5m), o, h, l, c, tf=5)


def _bar75(offset_75m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset_75m), o, h, l, c, tf=75)


def _index_bear_confirm_bars():
    """3 bars of 75m Index/spot data that confirm a bear trap (arms CE) --
    same construction as test_v4_cascade_index_trap_kind.py's equivalent."""
    return [
        _bar75(0, 200, 210, 200, 205),
        _bar75(1, 105, 110, 100, 105),
        _bar75(2, 95, 115, 90, 112),
    ]


def _premium_trap_pattern(offset0):
    """A 3-bar 2-candle bear-trap on the premium (CE/PE) chart, same shape as
    test_v4_cascade_index_gated_scanner.py's _trap_pattern: entry_line=100,
    sweep_low=95, limit_entry_price=98.33."""
    return [
        _bar5(offset0, 105, 110, 100, 105),
        _bar5(offset0 + 1, 98, 105, 95, 100),
        _bar5(offset0 + 2, 110, 115, 105, 112),
    ]


def test_index_confirmation_arms_only_the_matching_side():
    eng = V4CascadeEngine()
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    assert eng._scanners["CE"].armed is True
    assert eng._scanners["PE"].armed is False


def test_premium_pattern_before_arming_is_not_discovered_after_arming_it_is():
    eng = V4CascadeEngine()
    # Feed a premium trap pattern BEFORE the Index ever confirms -- since
    # discovery is hard-gated, nothing should be found.
    for b in _premium_trap_pattern(0):
        eng.update(ce_bar=b)
    assert eng._scanners["CE"].setups == []

    # Now arm CE via the Index chart (confirms at 75m-offset 2 = 150min),
    # then feed a SECOND, distinct premium pattern (offset 60 = 300min,
    # well after the confirmation, and price-shifted so it's unambiguously
    # a different structure) -- this one must be found.
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    second_pattern = [
        _Bar(b.timestamp, b.open + 50, b.high + 50, b.low + 50, b.close + 50, tf=5)
        for b in _premium_trap_pattern(60)
    ]
    for b in second_pattern:
        eng.update(ce_bar=b)
    assert len(eng._scanners["CE"].setups) == 1
    assert eng._scanners["CE"].setups[0].zone.entry_line == 150


def test_full_funnel_fires_open_long_event():
    eng = V4CascadeEngine()
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    events = []
    for b in _premium_trap_pattern(60):
        events += eng.update(ce_bar=b)
    assert eng._scanners["CE"].setups[0].state.value == "premium_locked"

    # Re-entry bar -> LIMIT_ARMED. limit_entry_price = 100-(100-95)/3 = 98.33 --
    # this bar's low (99) re-enters [95,100] WITHOUT itself piercing 98.33,
    # so it advances the setup without also firing in the same step.
    events += eng.update(ce_bar=_bar5(63, 99, 100, 99, 99.5))
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"

    # Pierce bar (low=97 <= 98.33) -> fires.
    events += eng.update(ce_bar=_bar5(64, 98, 99, 97, 97.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 1
    assert eng.position is not None
    assert eng.position.side == "CE"
    assert eng._scanners["CE"].setups == []  # popped on fire


def test_in_flight_setup_still_fires_after_index_flips_away():
    """The decision-2 regression: once a premium setup is discovered while
    armed, it must fire even if the Index classification later flips to the
    OTHER side (or NONE) before the pierce happens -- discovery is
    hard-gated, but an already-discovered setup's progress to trigger is
    never re-gated."""
    eng = V4CascadeEngine()
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    eng._scanners["CE"]._scan_window_start_ts = _BASE
    for b in _premium_trap_pattern(0):
        eng.update(ce_bar=b)
    # Re-entry bar (low=99, above the 98.33 limit price) -> LIMIT_ARMED
    # without also piercing in the same step.
    eng.update(ce_bar=_bar5(10, 99, 100, 99, 99.5))
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"

    # Flip the Index gate away from CE -- CE is now explicitly unarmed.
    eng._scanners["CE"].set_armed(False)
    assert eng._scanners["CE"].armed is False

    # The pierce must still fire and open a position, despite CE being unarmed.
    events = eng.update(ce_bar=_bar5(11, 98, 99, 97, 97.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 1
    assert eng.position is not None
    assert eng.position.side == "CE"


def test_crypto_construction_still_uses_legacy_scanner():
    eng = V4CascadeEngine(pe_scans_bull=True)
    assert isinstance(eng._scanners["CE"], PremiumGateScanner)
    assert isinstance(eng._scanners["PE"], PremiumGateScanner)
    assert eng._legacy_scanners is True


def test_nifty_construction_uses_index_gated_scanner():
    eng = V4CascadeEngine(pe_scans_bull=False)
    assert isinstance(eng._scanners["CE"], IndexGatedPremiumScanner)
    assert isinstance(eng._scanners["PE"], IndexGatedPremiumScanner)
    assert eng._legacy_scanners is False
