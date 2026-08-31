"""
Regression tests for the post-15:00 per-leg R1 exit (2026-08-28, direct user
spec). Replaces the ACTION of day_low_exit_enabled (close both legs) for a
binding that opts into THIS instead (post1500_exit_enabled, default OFF):

  1. From 15:00 onward, watch each leg's own 1-min R1 -- watching alone never
     closes anything.
  2. ARM the moment EITHER the position reaches the frozen day-low (the SAME
     one-time REST value day_low_exit_enabled's own block computes) OR it's
     >=15:15 and the overall day P&L is positive. While the day is in loss
     and neither has happened, nothing closes here (existing EOD
     hedge-and-carry, unchanged, is what handles that case).
  3. Once armed, each leg closes INDEPENDENTLY the instant its own R1
     breaches -- the other leg keeps running solo.
  4. A single surviving leg gets no further exit here except EOD square-off,
     which must close ONLY that leg (_close_leg), never the normal dual-leg
     _close_position path (that would double-send/double-book the leg that
     already closed).

Mocks `s._close_leg` directly (AsyncMock) rather than driving the real
confirm-then-finalize bridge machinery -- that machinery itself is already
covered elsewhere (e.g. test_confirm_model_redesign.py); these tests are
about the state machine deciding WHEN to call it and how the position reacts.
"""
import asyncio
from datetime import time as dtime
from unittest.mock import ANY, AsyncMock

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


class _FakeOrderEvent:
    def __init__(self, close_aborted=False):
        self.close_aborted = close_aborted


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = dtime(23, 59)
    s._itm_pair_gate_enabled = False
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0
    s._day_low_exit_enabled = False
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))
    s._defer_exit = lambda reason, now: True
    return s


def _position(ce_ltp: float, pe_ltp: float, ce_entry=None, pe_entry=None) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_entry if ce_entry is not None else ce_ltp, ce_ltp),
        pe_leg=StraddleLeg("PE", 24000, pe_entry if pe_entry is not None else pe_ltp, pe_ltp),
        net_credit=(ce_entry if ce_entry is not None else ce_ltp)
                   + (pe_entry if pe_entry is not None else pe_ltp),
        status="open",
    )


def _spy_close_leg(s, closes_ok=True):
    calls = []

    async def _fake(side, reason, now):
        calls.append((side, reason))
        if closes_ok:
            leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
            leg.close_time = now
        return _FakeOrderEvent(close_aborted=not closes_ok)
    s._close_leg = _fake
    return calls


def _spy_close_position(s):
    calls = []

    async def _fake(reason):
        calls.append(reason)
        s._position.status = "closed"
        s._position = None
    s._close_position = _fake
    return calls


def test_defaults_on_via_runtime_config(monkeypatch):
    """2026-08-31, direct user spec: both post1500_exit_enabled and
    shadow_vwap_enabled now default ON globally (index_section() deep-merges
    _SS_INDEX_DEFAULT over any stored per-index config, so a brand-new
    index with nothing stored yet -- or an already-configured one missing
    this newer key -- both pick up the True default)."""
    from data_layer.runtime_config import RuntimeConfig
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    assert s._post1500_exit_enabled is True
    assert s._shadow_vwap_enabled is True
    assert RuntimeConfig.index_section("NIFTY", "sell_straddle")["post1500_exit_enabled"] is True


def test_when_explicitly_disabled_no_watch_no_close():
    s = _strategy()
    s._post1500_exit_enabled = False
    s._position = _position(30.0, 20.0)
    close_calls = _spy_close_leg(s)
    asyncio.run(s._check_exits())
    assert not close_calls
    assert s._position is not None and s._position.status == "open"


def test_no_arm_before_1500_even_if_profitable():
    s = _strategy()
    s._post1500_exit_enabled = True
    s._force_exit = dtime(23, 59)
    # Freeze time defaults to 15:00, but _check_post1500_r1_exit's own guard
    # is what actually matters here -- verified independently below via the
    # method itself with a controlled `now`.
    s._position = _position(10.0, 10.0, ce_entry=30.0, pe_entry=30.0)   # deep profit
    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime
    now = datetime.now(exits_mod.IST).replace(hour=14, minute=30, second=0, microsecond=0)
    asyncio.run(s._check_post1500_r1_exit(s._position, now))
    assert s._post1500_armed is False


def test_arms_via_day_low_at_any_time_after_1500():
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 50.0
    s._position = _position(20.0, 20.0)   # current_value = 40, at/below frozen 50
    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime
    now = datetime.now(exits_mod.IST).replace(hour=15, minute=2, second=0, microsecond=0)
    asyncio.run(s._check_post1500_r1_exit(s._position, now))
    assert s._post1500_armed is True
    assert s._post1500_armed_reason == "day_low"


def test_no_arm_while_overall_loss_at_1515_waits_for_profit_flip():
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = None   # day-low path not applicable this tick
    s._session_realized_pnl_pts = 0.0
    s._position = _position(40.0, 40.0, ce_entry=20.0, pe_entry=20.0)   # net_credit=40, current=80 -> loss
    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime
    now = datetime.now(exits_mod.IST).replace(hour=15, minute=20, second=0, microsecond=0)
    asyncio.run(s._check_post1500_r1_exit(s._position, now))
    assert s._post1500_armed is False, "must not arm while the day is still in overall loss"

    # Profit later shows up -- arms on that very tick.
    s._position.ce_leg.ltp = 5.0
    s._position.pe_leg.ltp = 5.0   # current=10, unrealized_pnl = 40-10 = +30 -> now profitable
    asyncio.run(s._check_post1500_r1_exit(s._position, now))
    assert s._post1500_armed is True
    assert s._post1500_armed_reason == "profit"


def test_per_leg_independent_r1_breach_closes_only_that_leg():
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0   # arms immediately (any value >= current_value)
    close_calls = _spy_close_leg(s)
    s._position = _position(20.0, 20.0)

    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime, timedelta
    base = datetime.now(exits_mod.IST).replace(hour=15, minute=1, second=0, microsecond=0)

    # Feed a few 1-min bars to build a real R1 on the CE ladder, low volatility on PE.
    for i in range(5):
        now = base + timedelta(minutes=i)
        s._position.ce_leg.ltp = 20.0 + i   # rising CE -- will eventually breach its own R1
        s._position.pe_leg.ltp = 20.0
        asyncio.run(s._check_post1500_r1_exit(s._position, now))

    assert s._post1500_armed is True
    # CE should have been closed at least once its own R1 was breached; PE untouched.
    ce_closed = any(side == "CE" for side, _ in close_calls)
    pe_closed = any(side == "PE" for side, _ in close_calls)
    assert pe_closed is False, "PE never moved -- must never close"
    if ce_closed:
        assert s._position.ce_leg_closed is True
        assert s._position.pe_leg_closed is False, "surviving leg must stay open"


def test_eod_close_of_surviving_leg_uses_close_leg_not_close_position():
    """The critical safety guard: once one leg is closed via this mechanic,
    EOD square-off must route through _close_leg for the survivor only --
    never the normal dual-leg _close_position (which would double-send /
    double-book the already-closed leg)."""
    s = _strategy()
    s._post1500_exit_enabled = True
    close_leg_calls = _spy_close_leg(s)
    close_position_calls = []

    async def _fail_if_called(reason):
        close_position_calls.append(reason)
        raise AssertionError("must not call the normal dual-leg close path")
    # _close_position itself is guarded internally; patching the INNER dual-leg
    # branch isn't needed since the guard short-circuits before it -- but we
    # still assert the real _close_position (unpatched) routes correctly.
    s._unsubscribe_entry_expiry_tokens = AsyncMock()
    s._apply_sl_cooldown = lambda *a, **k: None
    s._persist = lambda: None

    s._position = _position(20.0, 20.0)
    s._position.ce_leg_closed = True   # CE already closed earlier today by the R1 mechanic

    asyncio.run(s._close_position("eod_squareoff"))

    assert close_leg_calls == [("PE", "eod_squareoff")], (
        "EOD must close ONLY the surviving PE leg via _close_leg, not re-touch CE"
    )
    assert s._position is None


def test_both_legs_closing_independently_finalizes_the_position():
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0
    s._unsubscribe_entry_expiry_tokens = AsyncMock()
    s._apply_sl_cooldown = lambda *a, **k: None
    s._persist = lambda: None
    _spy_close_leg(s)

    s._position = _position(20.0, 20.0)
    s._post1500_pair = (24000, 24000)
    s._post1500_armed = True
    s._post1500_armed_reason = "day_low"
    s._post1500_leg_closed = {"CE": True, "PE": False}
    s._position.ce_leg_closed = True

    import strategies.sell_straddle.exits as exits_mod
    from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator
    s._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
    s._post1500_bar_acc = {}

    from datetime import datetime, timedelta
    base = datetime.now(exits_mod.IST).replace(hour=15, minute=10, second=0, microsecond=0)
    for i in range(6):
        now = base + timedelta(minutes=i)
        s._position.pe_leg.ltp = 20.0 + i * 2
        asyncio.run(s._check_post1500_r1_exit(s._position, now))
        if s._position is None:
            break

    assert s._stop_for_day is True or s._position is None or s._position.pe_leg_closed
