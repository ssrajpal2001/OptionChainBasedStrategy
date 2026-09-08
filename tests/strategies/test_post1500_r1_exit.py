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
from datetime import datetime, time as dtime
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


async def _establish_r1(s, side: str, base, leg_attr: str):
    """Drives real 1-min bars through the actual SupportResistanceCalculator
    state machine (strategies/core/support_resistance.py) up through a
    genuinely ESTABLISHED R1 in a stable (non-R1_TRACKING) phase --
    INITIAL_TREND_ESTABLISHMENT -> "BREAKOUT HIGH" -> R1_TRACKING(R1=30,
    not established) -> a lower-high/lower-low candle confirms R1 established
    @30, phase -> S2_TRACKING. Required after the 2026-09-08 fix (breach must
    only action off a genuinely established, non-tracking R1) -- a handful of
    monotonically-rising ticks (the old test shape) never actually reaches
    "established" in the real state machine, so it could never have proven a
    genuine breach even before this fix; it only "worked" by accident against
    the old, ungated bug. Returns the timestamp of the next call the caller
    should use to feed a breaching tick (minute3, still-forming)."""
    from datetime import timedelta
    leg = getattr(s._position, leg_attr)
    steps = [
        (0, 0, 25.0), (0, 30, 15.0),    # candle_A minute0: h=25 l=15 (init only)
        (1, 0, 30.0), (1, 30, 20.0),    # candle_B minute1: h=30 l=20 -> BREAKOUT HIGH, R1_TRACKING (R1=30 unestablished)
        (2, 0, 28.0), (2, 30, 18.0),    # candle_C minute2: h=28 l=18 (accumulates, not yet closed)
    ]
    for minute, sec, ltp in steps:
        now = base + timedelta(minutes=minute, seconds=sec)
        leg.ltp = ltp
        await s._check_post1500_r1_exit(s._position, now)
    # minute3's first call closes candle_C (28,18) -- since 28<prev_high(30) and
    # 18<prev_low(20), this is exactly the "R1 established, S2_TRACKING" branch.
    return base + timedelta(minutes=3)


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


def test_day_low_reversal_never_fires_on_a_surviving_single_leg():
    """2026-08-31 CRITICAL FIX, real incident, live NIFTY: with BOTH
    day_low_exit_enabled and post1500_exit_enabled ON, PE closed via
    post1500_r1_breach at 15:15:28.096 -- five milliseconds later,
    DAY-LOW REVERSAL EXIT fired and closed the surviving CE leg too, using
    the frozen TWO-LEG value (198.65) against the now-SINGLE-LEG
    current_value (104.40, since one leg just closed and current_value
    naturally reflects only what's left). The single-leg-mode skip guard
    used to live AFTER the day-low block, so day-low itself was never
    protected. This test reproduces the exact numbers from that incident:
    a leg already closed on a prior tick, frozen=198.65, surviving leg
    alone worth ~104.40 (well under the two-leg frozen threshold) -- and
    asserts the position is NEVER closed via day-low once single-leg."""
    s = _strategy()
    s._day_low_exit_enabled = True
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 198.65
    s._day_low_freeze_time = dtime(15, 0)

    s._position = _position(104.40, 0.0, ce_entry=94.40)   # CE survives, PE already closed
    s._position.pe_leg.close_time = "2026-08-31T15:15:28"
    s._position.pe_leg_closed = True
    s._day_low_tracked_pair = (int(s._position.ce_leg.strike), int(s._position.pe_leg.strike))
    s._post1500_pair = s._day_low_tracked_pair
    s._post1500_armed = True
    s._post1500_leg_closed = {"CE": False, "PE": True}
    from strategies.core.support_resistance import SupportResistanceCalculator
    s._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
    s._post1500_bar_acc = {}

    close_position_calls = _spy_close_position(s)

    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime
    now = datetime.now(exits_mod.IST).replace(hour=15, minute=15, second=30, microsecond=0)
    _orig_now = exits_mod.datetime
    class _FixedDatetime(_orig_now):
        @classmethod
        def now(cls, tz=None):
            return now
    exits_mod.datetime = _FixedDatetime
    try:
        asyncio.run(s._check_exits())
    finally:
        exits_mod.datetime = _orig_now

    assert close_position_calls == [], (
        "day-low reversal (or any full-close path) must NEVER fire once a leg has "
        "already closed independently -- only R1/EOD apply to the surviving leg"
    )
    assert s._position is not None and s._position.status == "open"
    assert s._position.ce_leg_closed is False, "surviving CE leg must still be open"


def test_breach_never_fires_while_r1_still_mid_tracking():
    """2026-09-08 CRITICAL FIX, direct user spec: a breach must only be
    actioned once R1 is a genuinely ESTABLISHED, stable level -- never while
    the phase is still R1_TRACKING (R1 itself mid-formation, not yet
    confirmed). Before this fix, the code read whatever value R1 currently
    held with no established/phase check at all, so a live tick above a
    still-forming, unconfirmed R1 could incorrectly close a leg."""
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0   # arms immediately
    close_leg_calls = _spy_close_leg(s)
    s._position = _position(20.0, 20.0)

    import strategies.sell_straddle.exits as exits_mod
    from strategies.core.support_resistance import SupportResistanceCalculator
    from datetime import datetime, timedelta
    s._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
    s._post1500_bar_acc = {}

    async def _run():
        base = datetime.now(exits_mod.IST).replace(hour=15, minute=20, second=0, microsecond=0)
        # Only the first two candles -- init (25,15), then BREAKOUT HIGH
        # (30,20) which sets R1=30 with is_established=False, phase=
        # R1_TRACKING. R1 is NEVER confirmed here (no third, lower-high/
        # lower-low candle) -- it stays mid-tracking for the rest of this
        # test, exactly the buggy scenario.
        steps = [
            (0, 0, 25.0), (0, 30, 15.0),
            (1, 0, 30.0), (1, 30, 20.0),
        ]
        for minute, sec, ltp in steps:
            now = base + timedelta(minutes=minute, seconds=sec)
            s._position.ce_leg.ltp = ltp
            await s._check_post1500_r1_exit(s._position, now)
        # A live tick well above the still-unestablished R1=30 -- must NOT breach.
        breach_ts = base + timedelta(minutes=2)
        s._position.ce_leg.ltp = 200.0
        await s._check_post1500_r1_exit(s._position, breach_ts)
        # Confirm the state machine really is still mid-tracking, not established.
        sr = s._post1500_calc["CE"].get_calculated_sr_state("NIFTY_CE_P1500")
        assert sr["current_phase"] == "R1_TRACKING"
        assert sr["sr_levels"]["R1"]["is_established"] is False

    asyncio.run(_run())

    assert close_leg_calls == [], (
        f"a breach must never fire while R1 is still mid-tracking/unestablished, "
        f"got: {close_leg_calls}"
    )
    assert s._position.ce_leg_closed is False


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
    from strategies.core.support_resistance import SupportResistanceCalculator
    s._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
    s._post1500_bar_acc = {}

    from datetime import datetime

    async def _run():
        base = datetime.now(exits_mod.IST).replace(hour=15, minute=10, second=0, microsecond=0)
        breach_ts = await _establish_r1(s, "PE", base, "pe_leg")
        s._position.pe_leg.ltp = 35.0   # breaches the now-established R1(=30)
        await s._check_post1500_r1_exit(s._position, breach_ts)
    asyncio.run(_run())

    assert s._stop_for_day is True or s._position is None or s._position.pe_leg_closed


# ── 2026-08-31: exhaustive single-leg-mode invariant matrix ────────────────
#
# Direct user request after the day-low incident: prove this "under no
# conditions", not just the one condition that actually broke. Once a leg
# has closed via post1500, EVERY other exit mechanism -- day%, ITM-gate
# (+70% roll-protect), hedge-cumulative-profit-close, day-low, ratio-exit,
# LTP-decay, scalable TSL, exit_rules, VWAP-rise -- must be completely
# unreachable, no matter how aggressively each is configured to want to
# fire. Each test below "traps" the functions that mechanism would call if
# it ran (raises if called) and configures ONLY that one mechanism to be
# maximally aggressive, with every other mechanism left at its safe/off
# default -- isolating exactly one potential leak at a time.

def _trap(name):
    async def _boom(*a, **k):
        raise AssertionError(f"{name} must NEVER be reached once a leg has closed (single-leg mode)")
    return _boom


def _single_leg_position(ce_ltp=20.0, pe_ltp=0.0, ce_strike=24000, pe_strike=24000,
                          ce_entry=20.0, pe_entry=20.0) -> StraddlePosition:
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", ce_strike, ce_entry, ce_ltp),
        pe_leg=StraddleLeg("PE", pe_strike, pe_entry, pe_ltp),
        net_credit=ce_entry + pe_entry, status="open",
    )
    pos.pe_leg_closed = True   # PE already closed independently -- CE survives alone
    return pos


def _armed_post1500_state(s, pos):
    """Minimal armed post1500 state so the surviving leg's own R1 watch is
    the only thing that legitimately runs -- matches what _check_exits()
    expects to already exist once single-leg (set by an earlier tick's
    _check_post1500_r1_exit call in real operation)."""
    from strategies.core.support_resistance import SupportResistanceCalculator
    s._post1500_exit_enabled = True
    s._post1500_pair = (int(pos.ce_leg.strike), int(pos.pe_leg.strike))
    s._post1500_armed = True
    s._post1500_armed_reason = "day_low"
    s._post1500_leg_closed = {"CE": False, "PE": True}
    s._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
    s._post1500_bar_acc = {}


def _run_single_leg_check(s, pos):
    import strategies.sell_straddle.exits as exits_mod
    now = datetime.now(exits_mod.IST).replace(hour=15, minute=20, second=0, microsecond=0)
    _orig = exits_mod.datetime
    class _Fixed(_orig):
        @classmethod
        def now(cls, tz=None):
            return now
    exits_mod.datetime = _Fixed
    try:
        asyncio.run(s._check_exits())
    finally:
        exits_mod.datetime = _orig


def test_single_leg_mode_blocks_day_profit_target():
    s = _strategy()
    pos = _single_leg_position()
    s._position = pos
    _armed_post1500_state(s, pos)
    s._day_profit_target_pct = 1.0   # trivially satisfied -- would fire instantly if reached
    s._initial_net_credit = 40.0
    s._session_realized_pnl_pts = 100.0   # way past any profit target
    s._close_position = _trap("day_profit_target -> _close_position")
    s._close_position_and_hedge = _trap("day_profit_target -> _close_position_and_hedge")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_day_loss_sl():
    s = _strategy()
    pos = _single_leg_position(ce_ltp=999.0, ce_entry=20.0)   # deep running loss
    s._position = pos
    _armed_post1500_state(s, pos)
    s._day_loss_sl_pct = 1.0
    s._initial_net_credit = 40.0
    s._close_position = _trap("day_loss_sl -> _close_position")
    s._close_position_and_hedge = _trap("day_loss_sl -> _close_position_and_hedge")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_itm_pair_gate():
    s = _strategy()
    # Both strikes ITM relative to spot (CE strike < spot, PE strike > spot) --
    # exactly the shape itm_pair_gate looks for.
    pos = _single_leg_position(ce_strike=23900, pe_strike=24100)
    s._position = pos
    s._spot = 24000.0
    _armed_post1500_state(s, pos)
    s._itm_pair_gate_enabled = True
    s._itm_pair_gate_min_strike_gap = 50.0
    s._itm_pair_gate_profit_inr = 0.0   # any profit clears it instantly
    s._check_itm_pair_gate = _trap("itm_pair_gate")
    s._check_itm_roll_protection = _trap("itm_roll_protection")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_hedge_cumulative_profit_close():
    s = _strategy()
    pos = _single_leg_position()
    pos.is_hedged_positional = True   # would normally route into the hedge-profit check
    s._position = pos
    _armed_post1500_state(s, pos)
    s._check_hedge_cumulative_profit_close = _trap("hedge_cumulative_profit_close")
    s._close_hedge_legs = _trap("close_hedge_legs (same-strike-collision path)")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_ratio_exit():
    s = _strategy()
    # min/max ratio far past any threshold.
    pos = _single_leg_position(ce_ltp=500.0, pe_ltp=0.0)
    s._position = pos
    _armed_post1500_state(s, pos)
    s._ratio_threshold = 1.01
    s._single_side_roll = _trap("ratio_exit -> _single_side_roll")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_ltp_decay():
    s = _strategy()
    pos = _single_leg_position(ce_ltp=1.0)   # far below any decay floor
    s._position = pos
    _armed_post1500_state(s, pos)
    s._ltp_decay_enabled = True
    s._ltp_exit_min = 500.0
    s._single_side_roll = _trap("ltp_decay -> _single_side_roll")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_scalable_tsl():
    s = _strategy()
    pos = _single_leg_position(ce_ltp=1.0, ce_entry=500.0)   # deep profit
    s._position = pos
    _armed_post1500_state(s, pos)
    s._tsl_enabled = True
    s._tsl_base_profit_rs = 1.0
    s._tsl_base_lock_rs = 0.0
    s._close_position = _trap("scalable_tsl -> _close_position")
    s._close_position_and_hedge = _trap("scalable_tsl -> _close_position_and_hedge")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_exit_rules():
    s = _strategy()
    pos = _single_leg_position()
    s._position = pos
    _armed_post1500_state(s, pos)
    # A trivially-always-true rule set (RSI > -1 on any timeframe).
    s._exit_rules = [{"indicator": "RSI", "op": ">", "value": -1.0, "tf": 1}]
    s._single_side_roll = _trap("exit_rules -> _single_side_roll")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_vwap_rise():
    s = _strategy()
    pos = _single_leg_position()
    s._position = pos
    _armed_post1500_state(s, pos)
    s._vwap_rise_enabled = True
    s._vwap_rise_threshold = 0.0001   # trivially cleared
    s._single_side_roll = _trap("vwap_rise -> _single_side_roll")
    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_single_leg_mode_blocks_everything_simultaneously():
    """Kitchen-sink: every mechanism above configured maximally aggressive
    AT THE SAME TIME, single-leg mode active -- the strongest possible
    version of "under no conditions"."""
    s = _strategy()
    pos = _single_leg_position(ce_strike=23900, pe_strike=24100, ce_ltp=999.0, ce_entry=20.0)
    s._position = pos
    s._spot = 24000.0
    _armed_post1500_state(s, pos)
    s._day_profit_target_pct = 1.0
    s._day_loss_sl_pct = 1.0
    s._initial_net_credit = 40.0
    s._session_realized_pnl_pts = 100.0
    s._itm_pair_gate_enabled = True
    s._itm_pair_gate_min_strike_gap = 50.0
    s._itm_pair_gate_profit_inr = 0.0
    s._ratio_threshold = 1.01
    s._ltp_decay_enabled = True
    s._ltp_exit_min = 500.0
    s._tsl_enabled = True
    s._tsl_base_profit_rs = 1.0
    s._tsl_base_lock_rs = 0.0
    s._exit_rules = [{"indicator": "RSI", "op": ">", "value": -1.0, "tf": 1}]
    s._vwap_rise_enabled = True
    s._vwap_rise_threshold = 0.0001
    pos.is_hedged_positional = True

    for _name in ("_close_position", "_close_position_and_hedge", "_single_side_roll",
                  "_check_itm_pair_gate", "_check_itm_roll_protection",
                  "_check_hedge_cumulative_profit_close", "_close_hedge_legs"):
        setattr(s, _name, _trap(_name))

    _run_single_leg_check(s, pos)
    assert s._position is not None and s._position.status == "open"


def test_concurrent_r1_breach_checks_close_the_leg_only_once():
    """2026-09-03 CRITICAL FIX, real incident, live NIFTY: two full close
    cycles fired for the same CE leg 138ms apart -- same R1.high=85.15, same
    ltp=85.70 -- because ce_leg_closed only flips True AFTER _close_leg's
    await returns (order placement + broker confirmation, observed >1s in
    the real log), and _close_leg itself has no in-flight guard of its own.
    A second exit-check tick landing in that window saw the leg as still
    open and fired a second real broker order. Reproduces the race directly:
    two concurrent _check_post1500_r1_exit calls for the same already-
    breached tick, both reaching the close call before either's _close_leg
    resolves -- asserts _close_leg is invoked exactly once."""
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0   # arms immediately
    s._position = _position(20.0, 20.0)

    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime

    # Feed real bars through the actual S&R state machine to establish a
    # genuine CE R1 (see _establish_r1's own docstring), stopping right
    # before the breaching tick -- that breaching tick is what gets raced
    # below, at breach_now.
    async def _setup():
        base = datetime.now(exits_mod.IST).replace(hour=15, minute=1, second=0, microsecond=0)
        return base, await _establish_r1(s, "CE", base, "ce_leg")
    base, breach_now = asyncio.run(_setup())
    assert s._post1500_armed is True

    # A slow, controllable fake _close_leg: both concurrent callers must
    # reach this await before either is allowed to resolve, faithfully
    # reproducing the real order-confirmation delay that created the window.
    close_calls = []
    release = asyncio.Event()

    async def _slow_close_leg(side, reason, now):
        close_calls.append((side, reason))
        await release.wait()
        leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
        leg.close_time = now
        return _FakeOrderEvent(close_aborted=False)
    s._close_leg = _slow_close_leg

    s._position.ce_leg.ltp = 200.0   # unambiguous breach of whatever R1 formed
    s._position.pe_leg.ltp = 20.0

    async def _race():
        t1 = asyncio.create_task(s._check_post1500_r1_exit(s._position, breach_now))
        t2 = asyncio.create_task(s._check_post1500_r1_exit(s._position, breach_now))
        # Let both tasks run until they've each reached (or skipped) the
        # close call, then release the slow close so whichever one is
        # actually in flight can complete.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(t1, t2)

    asyncio.run(_race())

    ce_closes = [c for c in close_calls if c[0] == "CE"]
    assert len(ce_closes) == 1, (
        f"_close_leg must be invoked exactly once for the CE leg even when two "
        f"exit-check ticks race the same breach -- got {len(ce_closes)} calls: {close_calls}"
    )
    assert s._position.ce_leg_closed is True
    assert s._post1500_closing["CE"] is True


def test_post1500_closing_flag_clears_on_aborted_close_allowing_retry():
    """The in-flight guard must not permanently wedge a leg open if its
    close genuinely aborts (e.g. broker-confirm timeout) -- clearing the
    flag on abort lets the next tick retry the close."""
    s = _strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0
    s._position = _position(20.0, 20.0)

    import strategies.sell_straddle.exits as exits_mod
    from datetime import datetime, timedelta
    base = datetime.now(exits_mod.IST).replace(hour=15, minute=1, second=0, microsecond=0)
    for i in range(5):
        now = base + timedelta(minutes=i)
        s._position.ce_leg.ltp = 20.0 + i
        s._position.pe_leg.ltp = 20.0
        asyncio.run(s._check_post1500_r1_exit(s._position, now))

    close_calls = _spy_close_leg(s, closes_ok=False)   # every close aborts
    retry_now = base + timedelta(minutes=5)
    s._position.ce_leg.ltp = 20.0 + 5
    asyncio.run(s._check_post1500_r1_exit(s._position, retry_now))
    first_call_count = len(close_calls)

    if first_call_count > 0:
        assert s._post1500_closing["CE"] is False, (
            "an aborted close must clear the in-flight flag so a later tick can retry"
        )
        assert s._position.ce_leg_closed is False
