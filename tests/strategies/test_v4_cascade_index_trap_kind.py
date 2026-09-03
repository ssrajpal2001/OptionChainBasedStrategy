"""spot_confirm.py's SpotConfirmTracker — 2026-07-20 Index/Premium decoupling:
IndexTrapKind (renamed from SpotTrapKind), current_zone tracking, and
confirmation_ts() -- used by IndexGatedPremiumScanner to anchor its Gate-2
scan window."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.book import _Bar
from strategies.v4_cascade.dataclasses import IndexTrapKind
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker

IST = ZoneInfo("Asia/Kolkata")
_BASE = datetime(2026, 7, 20, 9, 15, tzinfo=IST)


def _bar75(offset_75m, o, h, l, c):
    return _Bar(_BASE + timedelta(minutes=75 * offset_75m), o, h, l, c, tf=75)


def test_default_kind_is_none():
    t = SpotConfirmTracker()
    assert t.current_kind == IndexTrapKind.NONE
    assert t.current_zone is None
    assert t.confirms("CE") is False
    assert t.confirms("PE") is False
    assert t.confirmation_ts("CE") is None


def test_bear_trap_confirmed_arms_ce():
    t = SpotConfirmTracker()
    # bar0: filler. bar1 (ref): low=100 high=110. bar2 sweeps below the low
    # (90) WITHOUT reclaiming yet (high=105 <= 110) -- a genuine, separate
    # sweep candle. bar3 then reclaims above the ref's high (115),
    # confirming a bear trap (find_bear_zone, unbounded span, 3 distinct
    # candles: ref/sweep/reclaim).
    t.on_75m_bar(_bar75(0, 200, 210, 200, 205))
    t.on_75m_bar(_bar75(1, 105, 110, 100, 105))
    t.on_75m_bar(_bar75(2, 100, 105, 90, 95))
    kind = t.on_75m_bar(_bar75(3, 96, 115, 95, 112))
    assert kind == IndexTrapKind.BEAR_TRAP_CONFIRMED
    assert t.current_kind == IndexTrapKind.BEAR_TRAP_CONFIRMED
    assert t.confirms("CE") is True
    assert t.confirms("PE") is False
    assert t.current_zone is not None
    assert t.current_zone.entry_line == 100
    assert t.confirmation_ts("CE") == t.current_zone.lock_ts
    assert t.confirmation_ts("PE") is None


def test_bull_trap_confirmed_arms_pe():
    t = SpotConfirmTracker()
    # ref=bar0 (low=100, high=110). bar1 clears the high (120, "buyers in")
    # WITHOUT going below bar0's low (its own low=105) -- so this sequence
    # can never also satisfy the bear pattern (which needs a single candle
    # with BOTH a lower low and a higher high than ref). bar2 then clears
    # bar0's low (90 < 100), confirming the bull trap (buyers trapped).
    t.on_75m_bar(_bar75(0, 100, 110, 100, 105))
    t.on_75m_bar(_bar75(1, 110, 120, 105, 115))
    kind = t.on_75m_bar(_bar75(2, 95, 95, 90, 92))
    assert kind == IndexTrapKind.BULL_TRAP_CONFIRMED
    assert t.confirms("PE") is True
    assert t.confirms("CE") is False
    assert t.confirmation_ts("PE") == t.current_zone.lock_ts


def test_reset_clears_current_zone():
    t = SpotConfirmTracker()
    t.on_75m_bar(_bar75(0, 200, 210, 200, 205))
    t.on_75m_bar(_bar75(1, 105, 110, 100, 105))
    t.on_75m_bar(_bar75(2, 100, 105, 90, 95))
    t.on_75m_bar(_bar75(3, 96, 115, 95, 112))
    assert t.current_zone is not None
    t.reset()
    assert t.current_kind == IndexTrapKind.NONE
    assert t.current_zone is None
    assert t.current_bucket_ts is None
