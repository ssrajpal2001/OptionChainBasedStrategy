"""tests/strategies/test_roll_exit_ladder_never_freezes.py -- simulator for the
2026-08-26 real incident flagged "MAJOR MAJOR MAJOR" by direct user request, ahead
of going live with real capital: a completed rollover left self._roll_in_progress
stuck True, and _check_exits' own guard on that flag sits ahead of almost the whole
exit ladder (Day%/ITMgate/DayLow/LTPdecay/Ratio/ScalableTSL/exit_rules/VWAPrise) --
so one successful roll silently disabled nearly every protective exit for the rest
of the session, with only the EOD force-squareoff path still able to fire.

Fixed in strategies/sell_straddle/rolling.py's _single_side_roll via a try/finally
that unconditionally resets the flag on every exit from the roll body -- success,
an aborted close-leg confirmation, OR a genuine unhandled exception. This file
drives the REAL _single_side_roll + _check_exits + _on_fill flow (no mocking of
the roll mechanics themselves) through several consecutive rolls, and a forced
mid-roll crash, proving _check_exits keeps evaluating normally afterward every time.
"""
import asyncio
import datetime
from unittest.mock import patch

import pytest

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _no_rules_runtime_config():
    """Bypasses the real re-entry rules (default config needs warm pool-engine
    indicators this synthetic simulation has none of) -- isolates the roll/
    exit-ladder mechanics themselves, which is what this file targets."""
    patcher = patch("strategies.sell_straddle.rolling.RuntimeConfig")
    rc = patcher.start()
    rc.index_section.return_value = {"entry_rules_reentry": []}
    return patcher


def _make_book(ce_strike, ce_ltp, pe_strike, pe_ltp, spot):
    bus = EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._itm_pair_gate_enabled = False
    s._ltp_target = 0.0
    s._theta_target = 0.0
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ltp_decay_enabled = False
    s._ratio_threshold = 0.0
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_low_exit_enabled = False
    s._force_exit = datetime.time(23, 59)   # never past real EOD in this test
    s._spot = spot
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=datetime.datetime.now(IST)),
        pe_leg=StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=datetime.datetime.now(IST)),
        net_credit=ce_ltp + pe_ltp, status="open",
    )
    return s


async def _drive_roll(s, reason="ltp_decay"):
    """Run one real single-side roll through the actual fill-confirmation
    round-trip (no mocking of _single_side_roll/select_partner_for/_close_leg/
    _open_leg themselves), same pattern as test_single_side_roll.py."""
    emitted = []
    orig_emit = s._emit_order

    async def capture_emit(ev):
        emitted.append(ev)
        await orig_emit(ev)

    s._emit_order = capture_emit

    async def deliver_fills():
        await asyncio.sleep(0.02)
        close_ev = [o for o in emitted if o.action == "EXIT"][0]
        rolled_side = close_ev.legs[0]
        s._on_fill(StraddleFillEvent(
            action="EXIT", underlying="NIFTY", atm=24500.0,
            ce_strike=close_ev.ce_strike, pe_strike=close_ev.pe_strike,
            ce_fill=close_ev.ce_ltp if rolled_side == "CE" else 0.0,
            pe_fill=close_ev.pe_ltp if rolled_side == "PE" else 0.0,
            client_id="C", binding_id="B", event_id=close_ev.event_id, legs=[rolled_side],
        ))
        await asyncio.sleep(0.02)
        open_ev = [o for o in emitted if o.action == "ENTRY"][0]
        s._on_fill(StraddleFillEvent(
            action="ENTRY", underlying="NIFTY", atm=24500.0,
            ce_strike=open_ev.ce_strike, pe_strike=open_ev.pe_strike,
            ce_fill=open_ev.ce_ltp if rolled_side == "CE" else 0.0,
            pe_fill=open_ev.pe_ltp if rolled_side == "PE" else 0.0,
            client_id="C", binding_id="B", event_id=open_ev.event_id, legs=[rolled_side],
        ))

    task = asyncio.create_task(deliver_fills())
    rolled = await s._single_side_roll(datetime.datetime.now(IST), reason)
    await task
    return rolled


def test_exit_ladder_keeps_running_after_a_real_successful_roll():
    """The core regression: after ONE completed roll, _check_exits must reach
    all the way down to the EXIT-EVAL block, proving _roll_in_progress reset."""
    async def run():
        patcher = _no_rules_runtime_config()
        try:
            s = _make_book(24550, 60.0, 24450, 90.0, spot=24460.0)
            s._strike_prem = {(24450, "CE"): {"ltp": 65.0, "atp": 65.0}}
            rolled = await _drive_roll(s)
            assert rolled is True
            assert s._roll_in_progress is False

            await s._check_exits()
            assert s._roll_in_progress is False   # _check_exits' own guard never re-armed it
            assert s._last_exit_eval is not None   # proves the ladder reached the EXIT-EVAL block
        finally:
            patcher.stop()
    asyncio.run(run())


def test_exit_ladder_survives_five_consecutive_rolls():
    """Simulates a realistic live session with several rollovers back to back --
    the flag must reset EVERY time, never accumulate into a stuck state."""
    async def run():
        patcher = _no_rules_runtime_config()
        try:
            s = _make_book(24550, 60.0, 24450, 90.0, spot=24460.0)
            strikes = [24450, 24350, 24250, 24150, 24050]
            for i, new_strike in enumerate(strikes):
                closing_strike = int(s._position.ce_leg.strike)
                s._strike_prem = {(new_strike, "CE"): {"ltp": 60.0 + i, "atp": 60.0 + i}}
                rolled = await _drive_roll(s, reason=f"ltp_decay_{i}")
                assert rolled is True, f"roll #{i} (closing {closing_strike}) did not execute"
                assert s._roll_in_progress is False, f"stuck True after roll #{i}"
                await s._check_exits()
                assert s._last_exit_eval is not None, f"exit ladder frozen after roll #{i}"
        finally:
            patcher.stop()
    asyncio.run(run())


def test_exit_ladder_resets_flag_even_when_the_roll_itself_crashes():
    """Direct proof of the try/finally fix: force an unhandled exception INSIDE
    the roll body (after the close-leg confirms, during the open-leg dispatch)
    and confirm _roll_in_progress still comes back to False -- the scenario the
    old code (manual resets on specific return statements only) could not
    handle at all, since an exception skips every one of those return lines."""
    async def run():
        patcher = _no_rules_runtime_config()
        try:
            s = _make_book(24550, 60.0, 24450, 90.0, spot=24460.0)
            s._strike_prem = {(24450, "CE"): {"ltp": 65.0, "atp": 65.0}}

            async def _boom(*a, **kw):
                raise RuntimeError("simulated bridge/network failure mid-roll")
            s._open_leg = _boom

            emitted = []
            orig_emit = s._emit_order

            async def capture_emit(ev):
                emitted.append(ev)
                await orig_emit(ev)
            s._emit_order = capture_emit

            async def deliver_close_fill():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=close_ev.ce_strike, pe_strike=close_ev.pe_strike,
                    ce_fill=close_ev.ce_ltp, pe_fill=0.0,
                    client_id="C", binding_id="B", event_id=close_ev.event_id, legs=["CE"],
                ))

            task = asyncio.create_task(deliver_close_fill())
            with pytest.raises(RuntimeError, match="simulated bridge/network failure"):
                await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
            await task

            assert s._roll_in_progress is False   # reset by `finally` despite the crash

            await s._check_exits()
            assert s._last_exit_eval is not None   # ladder still reaches EXIT-EVAL afterward
        finally:
            patcher.stop()
    asyncio.run(run())
