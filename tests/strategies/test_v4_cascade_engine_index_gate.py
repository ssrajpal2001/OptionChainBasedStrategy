"""strategies/v4_cascade/engine.py -- end-to-end coverage of the Index/Premium
decoupling via V4CascadeEngine.update(). 2026-07-21: discovery (Gate 2) runs
unconditionally on both sides regardless of Index arming -- required for
structural flip, since PE must be able to independently discover and
progress its own setup while CE currently holds Index bias. `armed` is
checked ONLY at trigger time (Gate 3): a pierce on a currently-unarmed side
is left pending, not fired, until that side becomes armed. Plus a guardrail
that crypto's construction path is completely unaffected (it already worked
this exact way)."""
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
    """4 bars of 75m Index/spot data that confirm a bear trap (arms CE) --
    ref/sweep-only/reclaim as 3 DISTINCT candles (find_bear_zone requires
    the reclaim strictly after the sweep candle) -- same construction as
    test_v4_cascade_index_trap_kind.py's equivalent."""
    return [
        _bar75(0, 200, 210, 200, 205),
        _bar75(1, 105, 110, 100, 105),
        _bar75(2, 100, 105, 90, 95),
        _bar75(3, 96, 115, 95, 112),
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


def test_premium_pattern_discovered_regardless_of_arming():
    """2026-07-21: discovery must find a Demand Block on PE even though only
    CE is armed by the Index -- PE has to be able to build up its own
    in-flight setup for structural flip to ever be possible."""
    eng = V4CascadeEngine()
    # PE is never armed at all in this test.
    for b in _premium_trap_pattern(0):
        eng.update(pe_bar=b)
    assert len(eng._scanners["PE"].setups) == 1
    assert eng._scanners["PE"].armed is False
    assert eng._scanners["PE"].setups[0].zone.entry_line == 100

    # CE, meanwhile, gets armed via the Index chart and independently
    # discovers its own, distinct pattern too.
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    assert eng._scanners["CE"].armed is True
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


def test_pierce_on_unarmed_side_does_not_fire_but_fires_once_rearmed():
    """2026-07-21 (inverted from the earlier same-day 'never abort in-flight
    even if unarmed at fire time' decision, per explicit later user
    direction): a fully-formed LIMIT_ARMED setup on a side the Index does
    NOT currently favor must NOT fire on pierce -- it stays pending
    (un-popped) and fires the next time that side is armed AND pierced
    again. This is what makes the trigger-time armed check meaningful now
    that discovery itself is unconditional."""
    eng = V4CascadeEngine()
    for b in _premium_trap_pattern(0):
        eng.update(ce_bar=b)  # CE never armed in this test
    eng.update(ce_bar=_bar5(10, 99, 100, 99, 99.5))  # -> LIMIT_ARMED
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"
    assert eng._scanners["CE"].armed is False

    # Pierce while unarmed -- must NOT fire, setup stays in place (un-popped).
    events = eng.update(ce_bar=_bar5(11, 98, 99, 97, 97.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 0
    assert eng.position is None
    assert len(eng._scanners["CE"].setups) == 1
    assert eng._scanners["CE"].setups[0].state.value == "limit_armed"

    # Now arm CE via the Index chart and pierce again -- fires this time.
    for b in _index_bear_confirm_bars():
        eng.update(spot_bar=b)
    assert eng._scanners["CE"].armed is True
    events = eng.update(ce_bar=_bar5(12, 97, 98, 96, 96.5))
    fired = [e for e in events if e.event_type.value == "open_long_ce"]
    assert len(fired) == 1
    assert eng.position is not None
    assert eng.position.side == "CE"
    assert eng._scanners["CE"].setups == []  # popped on fire


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
