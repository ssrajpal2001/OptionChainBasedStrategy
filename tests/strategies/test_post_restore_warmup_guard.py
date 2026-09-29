"""
Regression test for the 2026-08-05 live incident: after a mid-day restart,
SellStraddleStrategy holds ALL exit checks until both legs get a fresh live
tick (self._ce_ltp_fresh/_pe_ltp_fresh), or a fallback timeout releases the
hold. The original fallback was a flat 20s -- in a real restart, CE took 82s
to get its first tick, so exits (including the ITM pair gate) ran for ~60s
using CE's stale, frozen restored/entry price before a real tick corrected
it.

Fixed (final, user-specified): ceiling raised to 5 minutes (300s), and past
that ceiling a still-not-fresh leg is no longer armed for trading on a
possibly-stale price at all -- it's treated as a stuck data feed and the
position is CLOSED instead.
"""
import asyncio
import time as _time
from datetime import date, datetime
from unittest.mock import AsyncMock

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _restored_strategy(bus, elapsed_sec: float, ce_fresh: bool, pe_fresh: bool, market_open: bool = True):
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = datetime.strptime("23:59", "%H:%M").time()
    s._itm_pair_gate_enabled = False
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 23900, 60.0, 60.0),
        pe_leg=StraddleLeg("PE", 24100, 40.0, 40.0),
        net_credit=100.0, status="open", expiry_date=date.today(),
    )
    s._post_restore_warmup = True
    s._post_restore_at = _time.monotonic() - elapsed_sec
    # 2026-09-29: the real timeout clock is _post_restore_warmup_clock_start,
    # which only starts once the market is genuinely open (see engine.py's
    # own fix comment) -- a pre-market restart no longer burns its 5-minute
    # window before the exchange even opens. Simulate "the clock has already
    # been running for elapsed_sec" directly when market_open=True; leave it
    # unset (None) when market_open=False, matching how it genuinely never
    # starts pre-market.
    s._post_restore_warmup_clock_start = (_time.monotonic() - elapsed_sec) if market_open else None
    if not market_open:
        s._market_genuinely_open = lambda: False
    s._ce_ltp_fresh = ce_fresh
    s._pe_ltp_fresh = pe_fresh
    # Spy on the ITM-pair-gate check (called near the end of _check_exits,
    # after every earlier step) to detect whether the guard let evaluation
    # proceed past it, without needing to drive a full real exit.
    s._check_itm_pair_gate = AsyncMock()
    s._check_itm_roll_protection = AsyncMock()
    return s


def test_exits_held_when_not_fresh_and_under_ceiling():
    s = _restored_strategy(EventBus(), elapsed_sec=10.0, ce_fresh=False, pe_fresh=True)
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is True
    s._check_itm_pair_gate.assert_not_awaited()
    assert s._position is not None and s._position.status == "open"


def test_exits_armed_immediately_when_both_fresh():
    s = _restored_strategy(EventBus(), elapsed_sec=2.0, ce_fresh=True, pe_fresh=True)
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is False
    s._check_itm_pair_gate.assert_awaited()
    assert s._position is not None and s._position.status == "open"


def test_calculative_binding_stays_held_until_shadow_vwap_seeded_even_with_fresh_ltp():
    """2026-09-23, direct user spec: "fresh LTP has arrived" alone isn't
    enough for a calculative vwap_source binding -- its own VWAP source
    (self._shadow_vwap) has no persistence and needs its async REST seed to
    land first (see the same-day _eng_atp fix). Both legs' LTP ticking must
    NOT be enough to arm exits on its own here."""
    s = _restored_strategy(EventBus(), elapsed_sec=2.0, ce_fresh=True, pe_fresh=True)
    s._vwap_source = "calculative"
    # Neither leg's key is in _shadow_vwap_rest_seeded yet.
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is True, "must stay held -- shadow VWAP not seeded yet"
    s._check_itm_pair_gate.assert_not_awaited()


def test_calculative_binding_arms_once_both_legs_shadow_vwap_seeded():
    s = _restored_strategy(EventBus(), elapsed_sec=2.0, ce_fresh=True, pe_fresh=True)
    s._vwap_source = "calculative"
    s._shadow_vwap_rest_seeded.add((23900, "CE"))
    s._shadow_vwap_rest_seeded.add((24100, "PE"))
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is False
    s._check_itm_pair_gate.assert_awaited()


def test_old_20s_ceiling_no_longer_releases_a_still_stale_leg():
    """The exact 2026-08-05 shape: CE still not fresh at 20s (would have
    fired under the original 20s ceiling) -- must now stay held, well under
    the new 5-minute ceiling."""
    s = _restored_strategy(EventBus(), elapsed_sec=25.0, ce_fresh=False, pe_fresh=True)
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is True
    s._check_itm_pair_gate.assert_not_awaited()
    assert s._position is not None and s._position.status == "open"


def test_90s_no_longer_closes_or_arms_either_still_under_5min_ceiling():
    """90s (the previous ceiling) must no longer trigger anything -- neither
    the old "arm with stale data" behavior nor the new "close" behavior --
    since the ceiling is now 300s."""
    s = _restored_strategy(EventBus(), elapsed_sec=95.0, ce_fresh=False, pe_fresh=True)
    asyncio.run(s._check_exits())
    assert s._post_restore_warmup is True
    s._check_itm_pair_gate.assert_not_awaited()
    assert s._position is not None and s._position.status == "open"


def test_5min_ceiling_closes_position_instead_of_arming_stale_data(caplog):
    import logging
    s = _restored_strategy(EventBus(), elapsed_sec=301.0, ce_fresh=False, pe_fresh=True)
    # 2026-09-17: the safety close now additionally requires the exchange to
    # be genuinely open (real incident -- see _market_genuinely_open's own
    # docstring) -- force that True here so this test stays deterministic
    # regardless of the real wall-clock time it happens to run at.
    s._market_genuinely_open = lambda: True
    close_calls = []

    async def _fake_close_position(reason):
        close_calls.append(reason)
        s._position.status = "closed"
    s._close_position = _fake_close_position

    with caplog.at_level(logging.CRITICAL, logger="strategies.sell_straddle.exits"):
        asyncio.run(s._check_exits())

    assert s._post_restore_warmup is False
    assert close_calls == ["post_restore_data_stale"]
    # Must NOT proceed to arm/evaluate further exit checks on stale data --
    # the position is being closed, not traded.
    s._check_itm_pair_gate.assert_not_awaited()
    assert any("TIMED OUT" in r.message for r in caplog.records)
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


def test_5min_ceiling_never_touches_hedge_legs(caplog):
    """2026-09-17, direct user spec: "when we bring back the positions dont
    close hedge leg -- hedge leg will get closed in only 1 case when there
    is cumulative profit of already booked plus running 4 leg >500 then
    only hedge leg will get closed." The post-restore stale-feed safety
    close must call the plain _close_position (sold legs only, which
    stashes any standing hedge into _pending_hedge_ce_leg/_pe_leg to carry
    forward into the next entry), never _close_position_and_hedge (which
    would close the hedge legs too)."""
    import logging
    s = _restored_strategy(EventBus(), elapsed_sec=301.0, ce_fresh=False, pe_fresh=True)
    s._market_genuinely_open = lambda: True
    s._position.hedge_ce_leg = StraddleLeg("CE", 23600, 39.05, 39.05)
    s._position.hedge_pe_leg = StraddleLeg("PE", 22950, 65.85, 65.85)
    s._position.is_hedged_positional = True

    hedge_close_calls = []
    async def _fake_close_position_and_hedge(reason):
        hedge_close_calls.append(reason)
    s._close_position_and_hedge = _fake_close_position_and_hedge

    sold_close_calls = []
    async def _fake_close_position(reason):
        sold_close_calls.append(reason)
        s._position.status = "closed"
    s._close_position = _fake_close_position

    with caplog.at_level(logging.CRITICAL, logger="strategies.sell_straddle.exits"):
        asyncio.run(s._check_exits())

    assert hedge_close_calls == [], "hedge legs must never be closed by the stale-feed guard"
    assert sold_close_calls == ["post_restore_data_stale"]


def test_5min_ceiling_defers_close_when_market_not_genuinely_open(caplog):
    """2026-09-17 real live incident: a restart shortly before market open
    (e.g. 08:56:50) hit the 5-min ceiling at ~09:01:50 -- still inside NSE's
    pre-open/call-auction window. The safety close must NOT attempt a real
    order in that window at all (Zerodha rejects it with "could not be
    converted to AMO", and the retry-until-confirmed design then hammers the
    broker every tick) -- it must defer instead, staying ARMED, and try
    again on a later tick once the exchange is genuinely open.

    2026-09-29 update: the warmup clock itself now only starts once the
    market is genuinely open (see engine.py/exits.py's own fix comments for
    the real 09:15:00.108 incident this superseded) -- so while pre-market,
    _post_restore_warmup_clock_start simply never gets set and _elapsed stays
    0.0, which already can't exceed the 300s ceiling. The old dedicated
    "defer" branch + its own warning log are gone (dead code under the new
    design, since elapsed can never be >300 while market isn't open) -- the
    deferral is now implicit, not a separately logged event."""
    s = _restored_strategy(EventBus(), elapsed_sec=301.0, ce_fresh=False, pe_fresh=True, market_open=False)
    close_calls = []

    async def _fake_close_position(reason):
        close_calls.append(reason)
        s._position.status = "closed"
    s._close_position = _fake_close_position

    asyncio.run(s._check_exits())

    assert close_calls == [], "must not attempt a real close before market is genuinely open"
    assert s._post_restore_warmup is True
    assert s._position is not None and s._position.status == "open"
    s._check_itm_pair_gate.assert_not_awaited()
    assert s._post_restore_warmup_clock_start is None, (
        "clock must not start at all while market is not genuinely open"
    )


def test_5min_ceiling_keeps_guard_armed_when_close_is_not_confirmed(caplog):
    """2026-08-06 CRITICAL FIX regression test. If the safety close itself
    fails to confirm (broker unavailable/timeout -- plausible under the same
    conditions causing a stuck feed), the OLD code cleared
    _post_restore_warmup BEFORE attempting the close, so the very next tick
    would fall straight through to normal exit checks on the still-stale
    leg price -- exactly what this guard exists to prevent. The guard must
    now stay ARMED so the safety close is retried next cycle instead."""
    import logging
    s = _restored_strategy(EventBus(), elapsed_sec=301.0, ce_fresh=False, pe_fresh=True)
    s._market_genuinely_open = lambda: True

    async def _fake_close_position_that_fails(reason):
        # Simulates _close_position's own fail-safe behavior: broker
        # unavailable / confirmation timeout -> position left exactly as it
        # was, still open, nothing finalized.
        pass
    s._close_position = _fake_close_position_that_fails

    with caplog.at_level(logging.CRITICAL, logger="strategies.sell_straddle.exits"):
        asyncio.run(s._check_exits())

    assert s._post_restore_warmup is True, (
        "guard was disarmed even though the safety close was never confirmed -- "
        "the next tick would trade blind on the still-stale leg price."
    )
    assert s._position is not None and s._position.status == "open"
    s._check_itm_pair_gate.assert_not_awaited()
    assert any("NOT confirmed" in r.message for r in caplog.records)
