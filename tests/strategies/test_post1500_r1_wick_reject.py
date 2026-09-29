"""Regression test for the 2026-09-29 CRITICAL FIX, direct user correction,
real incident (2026-09-29 15:04:47): a POST-15:00 R1 breach fired off a mere
INTRABAR WICK (ltp=136.30 momentarily exceeding established R1=136.20) even
though that same 1-min bar went on to CLOSE at 135.30 -- BELOW R1 -- a
wick-and-reject, not a real breakout. Independently confirmed against the
real TradingView chart and a REST replay of real Upstox 1-min bars for that
exact incident: 136.65 (the true peak, one bar later) was never breached
again afterward.

Fixed by requiring the most recently CLOSED bar to have genuinely closed
above the R1 value established BEFORE that bar closed -- both captured
atomically in the bar-accumulation loop, so a same-bar mutation (a breach
candle can itself flip R1's established/phase state) can never corrupt the
comparison. Near EOD (15:00-15:35), the ~1 extra minute of latency this adds
(waiting for bar close instead of reacting to a live tick) is an acceptable
cost for filtering out a false intrabar wick."""
import asyncio
from datetime import datetime, timedelta, time as dtime
from unittest.mock import AsyncMock

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
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0   # arms immediately
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))
    s._defer_exit = lambda reason, now: True
    return s


def _position(ce_ltp: float, pe_ltp: float) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_ltp, ce_ltp),
        pe_leg=StraddleLeg("PE", 24000, pe_ltp, pe_ltp),
        net_credit=ce_ltp + pe_ltp, status="open",
    )


def _spy_close_leg(s):
    calls = []

    async def _fake(side, reason, now):
        calls.append((side, reason))
        return _FakeOrderEvent(close_aborted=False)
    s._close_leg = _fake
    return calls


async def _establish_r1(s, side: str, base, leg_attr: str):
    """Same shape as test_post1500_r1_exit.py's own helper: drives real bars
    to a genuinely ESTABLISHED, non-tracking R1=30. Returns the timestamp of
    the call that closes candle_C and establishes R1 -- the caller then
    controls the NEXT bar (minute3) directly to test the wick-vs-close
    distinction."""
    leg = getattr(s._position, leg_attr)
    steps = [
        (0, 0, 25.0), (0, 30, 15.0),
        (1, 0, 30.0), (1, 30, 20.0),
        (2, 0, 28.0), (2, 30, 18.0),
    ]
    for minute, sec, ltp in steps:
        now = base + timedelta(minutes=minute, seconds=sec)
        leg.ltp = ltp
        await s._check_post1500_r1_exit(s._position, now)
    return base + timedelta(minutes=3)


def test_intrabar_wick_above_r1_that_closes_back_below_never_breaches():
    s = _strategy()
    s._position = _position(20.0, 20.0)
    close_calls = _spy_close_leg(s)

    async def _run():
        from config.global_config import IST
        base = datetime.now(IST).replace(hour=15, minute=1, second=0, microsecond=0)
        commit_ts = await _establish_r1(s, "PE", base, "pe_leg")
        # minute3: wicks well above R1(=30) mid-bar...
        s._position.pe_leg.ltp = 35.0
        await s._check_post1500_r1_exit(s._position, commit_ts)
        s._position.pe_leg.ltp = 40.0
        await s._check_post1500_r1_exit(s._position, commit_ts)
        # ...but closes the bar BELOW R1 (a wick-and-reject).
        s._position.pe_leg.ltp = 27.0
        await s._check_post1500_r1_exit(s._position, commit_ts)
        # minute4: this call closes minute3's bar (h=40 l=27... but CLOSE=27).
        await s._check_post1500_r1_exit(s._position, commit_ts + timedelta(minutes=1))

    asyncio.run(_run())
    assert close_calls == [], (
        "a mere intrabar wick above R1 that closes back below it must NOT "
        f"trigger a breach -- got close calls: {close_calls}"
    )
    assert s._position is not None and s._position.pe_leg_closed is False


def test_bar_that_genuinely_closes_above_r1_does_breach():
    """Same setup, but the bar genuinely CLOSES above R1 -- must still fire,
    confirming the fix only filters wick-and-reject, not real breakouts."""
    s = _strategy()
    s._position = _position(20.0, 20.0)
    close_calls = _spy_close_leg(s)

    async def _run():
        from config.global_config import IST
        base = datetime.now(IST).replace(hour=15, minute=1, second=0, microsecond=0)
        commit_ts = await _establish_r1(s, "PE", base, "pe_leg")
        s._position.pe_leg.ltp = 35.0   # rises and stays there through bar close
        await s._check_post1500_r1_exit(s._position, commit_ts)
        # minute4: closes minute3's bar at close=35, genuinely above R1=30.
        await s._check_post1500_r1_exit(s._position, commit_ts + timedelta(minutes=1))

    asyncio.run(_run())
    assert close_calls == [("PE", "post1500_r1_breach")], (
        f"a bar that genuinely closes above R1 must still breach -- got {close_calls}"
    )
    assert s._position.pe_leg_closed is True
