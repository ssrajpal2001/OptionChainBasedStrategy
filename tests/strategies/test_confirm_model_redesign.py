"""2026-08-06 CONFIRM-MODEL REDESIGN: pos.status ("open"/"closing"/"closed") replaces the
ephemeral _close_in_progress flag as the reentrancy guard for _close_position, and becomes
the single persistent source of truth _check_exits uses to refuse re-dispatching a close
while one is genuinely in flight -- regardless of how long the real fill takes to confirm.
Complements tests/strategies/test_sell_straddle_safety.py (which already covers the
abort/timeout/confirm paths structurally) with the NEW status-transition invariants."""
import asyncio
from datetime import date, datetime, time as dtime

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _open_position(ss: SellStraddleStrategy) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=ss._underlying, atm_at_entry=24500.0, entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", 24500.0, 120.0, 120.0, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24500.0, 110.0, 110.0, open_time=datetime.now(IST)),
        net_credit=230.0, open_time=datetime.now(IST), status="open",
        lot_size=ss._lot_size * ss._lot_multiplier, expiry_date=date.today(),
    )
    ss._position = pos
    return pos


def test_status_flips_to_closing_synchronously_before_any_await(monkeypatch):
    """The core reentrancy fix: pos.status must already be 'closing' the instant
    _close_position starts -- before it ever awaits anything -- so a second trigger on the
    very next line of code (not just a later tick) sees it and backs off immediately."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)

    async def _never_returns_emit(ev):
        await asyncio.sleep(999)  # simulate a close that's still genuinely in flight

    ss._emit_order = _never_returns_emit

    async def run():
        task = asyncio.create_task(ss._close_position("day_loss_sl"))
        await asyncio.sleep(0)  # yield once -- just enough for the sync prelude to run
        assert pos.status == "closing"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())


class _AsyncMockThatWouldFailIfCalled:
    async def __call__(self, *a, **kw):
        raise AssertionError("_close_position must not be called while pos.status == 'closing'")


def test_check_exits_is_a_noop_while_closing():
    """_check_exits must not evaluate ANY exit criteria (day%/TSL/ratio/EOD/etc.) while a
    close is already in flight -- this IS the duplicate-dispatch guard now, replacing the
    old timeout-based one."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    pos.status = "closing"
    ss._close_position = _AsyncMockThatWouldFailIfCalled()

    asyncio.run(ss._check_exits())
    # No assertion needed beyond "didn't raise" -- _close_position replaced with a stub that
    # raises if called proves _check_exits returned before reaching any exit-check logic that
    # could call it.
    assert pos.status == "closing"


def test_timeout_reverts_status_to_open_for_retry(monkeypatch):
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    monkeypatch.setattr(type(ss), "_CLOSE_CONFIRM_TIMEOUT_SEC", 0.05)

    async def _fake_emit(ev):
        pass  # never deliver a fill

    ss._emit_order = _fake_emit

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is pos
    assert pos.status == "open"  # reverted -- next tick's _check_exits will retry


def test_placement_failed_reverts_status_to_open_for_retry():
    """A brand-new signal (2026-08-06): the order never even reached the broker after 3
    retries. Must behave exactly like exit_aborted -- revert to open, never abandon a real
    open position."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)

    async def _fake_emit(ev):
        fill = StraddleFillEvent(
            action="EXIT", underlying=ev.underlying, atm=ev.atm,
            ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
            client_id="C", binding_id="B", event_id=ev.event_id,
            legs=ev.legs, placement_failed=True,
        )
        ss._on_fill(fill)

    ss._emit_order = _fake_emit

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is pos
    assert pos.status == "open"
    assert ss._session_realized_pnl_pts == 0.0


def test_accepted_signal_does_not_finalize_or_touch_waiters():
    """The 'accepted' fill (order reached the broker, no price yet) must be purely
    informational -- it must NOT wake _close_position's waiter or touch position status by
    itself (status was already flipped to 'closing' synchronously at dispatch time)."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _open_position(ss)
    pos.status = "closing"

    accepted_fill = StraddleFillEvent(
        action="EXIT", underlying="NIFTY", atm=24500, ce_strike=24500, pe_strike=24500,
        ce_fill=0.0, pe_fill=0.0, client_id="C", binding_id="B", event_id="EV1",
        legs=["CE", "PE"], accepted=True,
    )
    waiter = asyncio.Event()
    ss._roll_close_waiters["EV1"] = waiter

    ss._on_fill(accepted_fill)

    assert pos.status == "closing"          # unchanged by the accepted signal itself
    assert not waiter.is_set()              # _close_position's real wait is untouched
    assert "EV1" not in ss._roll_close_results


def test_confirmed_exit_after_accepted_still_finalizes_normally():
    """Sanity check on the full happy path: accepted (informational) followed by the real
    confirmed fill must still finalize exactly as before."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    emitted: list = []

    async def _fake_emit(ev):
        emitted.append(ev)
        accepted = StraddleFillEvent(
            action="EXIT", underlying=ev.underlying, atm=ev.atm,
            ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
            client_id="C", binding_id="B", event_id=ev.event_id, legs=ev.legs, accepted=True,
        )
        ss._on_fill(accepted)
        fill = StraddleFillEvent(
            action="EXIT", underlying=ev.underlying, atm=ev.atm,
            ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
            ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
            client_id="C", binding_id="B", event_id=ev.event_id, legs=ev.legs,
        )
        ss._on_fill(fill)

    ss._emit_order = _fake_emit

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is None
    assert len(emitted) == 1


def test_entry_placement_failed_sets_stop_for_day():
    """2026-08-06: ENTRY placement failing after 3 retries means something is genuinely
    wrong (broker unreachable) -- stop opening NEW positions for the rest of the day. Must
    NEVER apply to exits (covered separately above)."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._order_pending = True
    ss._trades_today = 1

    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="", binding_id="",
        event_id="ev_placement_fail", entry_aborted=True, placement_failed=True,
    )
    ss._on_fill(fill)

    assert ss._stop_for_day is True
    assert ss._position is None


def test_entry_asymmetric_abort_does_not_set_stop_for_day():
    """Contrast case: a recoverable asymmetric-fill abort (not a placement failure) must
    NOT stop future entries -- only a genuine placement_failed does."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._order_pending = True
    ss._trades_today = 1

    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="", binding_id="",
        event_id="ev_asym", entry_aborted=True,
    )
    ss._on_fill(fill)

    assert ss._stop_for_day is False


def test_entry_aborted_pushes_cleared_position_to_ui():
    """2026-08-07 (ssrajpal2001 incident investigation): a UI-push gap was the first
    hypothesis for why the dashboard stayed on a stale position after a rejected live
    entry. Disproven -- this test PASSES even on the pre-fix engine.py, because
    _persist() already pushes notify_position_update(None, force=True) via its own
    'clearing position store' branch whenever self._position is cleared. Kept as a
    regression lock on that existing (correct) behavior, not as evidence of a fix --
    the real cause of the incident was the unconditional cooldown+retry loop, covered
    by the tests below."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    _open_position(ss)
    pushed: list = []
    ss.notify_position_update = lambda data, **kw: pushed.append((data, kw.get("force")))

    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="C", binding_id="B",
        event_id="ev_reject", entry_aborted=True,
    )
    ss._on_fill(fill)

    assert ss._position is None
    assert pushed == [(None, True)]


def test_three_consecutive_entry_rejections_stops_entries_for_day():
    """2026-08-07 real incident: a live entry rejected by the broker (insufficient funds,
    not a placement_failed -- the order DID reach the broker) used to cooldown-and-retry
    forever, hammering the broker with the same doomed order all session. Must stop entries
    for the day after 3 CONSECUTIVE such rejections -- same '3 tries then stop' principle
    already applied to placement_failed, just for broker-side rejections instead of
    transport failures. The first two rejections must NOT stop for the day (could be
    transient) -- only the third."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")

    def _reject(n):
        ss._order_pending = True
        ss._trades_today = 1
        fill = StraddleFillEvent(
            action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
            ce_fill=0.0, pe_fill=0.0, client_id="", binding_id="",
            event_id=f"ev_reject_{n}", entry_aborted=True,
        )
        ss._on_fill(fill)

    _reject(1)
    assert ss._stop_for_day is False
    assert ss._consecutive_entry_rejections == 1
    _reject(2)
    assert ss._stop_for_day is False
    assert ss._consecutive_entry_rejections == 2
    _reject(3)
    assert ss._stop_for_day is True
    assert ss._consecutive_entry_rejections == 3


def test_confirmed_entry_resets_consecutive_rejection_counter():
    """A real confirmed entry proves the broker connection works right now -- it must reset
    the rejection streak so 2 old rejections don't combine with 1 new one to falsely stop
    entries for the day."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")

    for n in (1, 2):
        ss._order_pending = True
        ss._trades_today = 1
        fill = StraddleFillEvent(
            action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
            ce_fill=0.0, pe_fill=0.0, client_id="", binding_id="",
            event_id=f"ev_reject_{n}", entry_aborted=True,
        )
        ss._on_fill(fill)
    assert ss._consecutive_entry_rejections == 2

    _open_position(ss)
    confirmed_fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=120.0, pe_fill=110.0, client_id="C", binding_id="B", event_id="ev_confirmed",
    )
    ss._on_fill(confirmed_fill)
    assert ss._consecutive_entry_rejections == 0

    ss._order_pending = True
    ss._trades_today = 1
    ss._position = None
    third_fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="", binding_id="",
        event_id="ev_reject_3", entry_aborted=True,
    )
    ss._on_fill(third_fill)
    assert ss._stop_for_day is False
    assert ss._consecutive_entry_rejections == 1
