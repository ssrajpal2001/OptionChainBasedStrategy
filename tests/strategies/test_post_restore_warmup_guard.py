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


def _restored_strategy(bus, elapsed_sec: float, ce_fresh: bool, pe_fresh: bool):
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
