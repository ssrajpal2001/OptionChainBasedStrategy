"""2026-09-07 CRITICAL FIX, real live incident: a restart mid-day restores an
open position and re-arms its own expiry as a sticky pin
(_reapply_expiry_stickiness_from_restored_position), so subscriptions don't
get orphaned across the restart. But once THAT position closes, the pin used
to stay locked on the closed position's (possibly stale/illiquid) expiry for
the rest of the day -- confirmed live: pool warm-seeding kept failing against
it ("no token/spot"), and since the main strategy loop is tick-driven, a
subscription that never resolves left the book permanently silent (no
exception, no heartbeat) until the next restart.

These tests lock in the fix: _close_position()/_close_surviving_leg_and_
finalize() must release the pin (and recompute the genuinely current expiry)
ONLY when it was armed for the restore reason -- never touching the
legitimate same-day "expiry-day shift" use of the same underlying flag.
"""
import asyncio
from datetime import date, datetime, timedelta

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _open_position(ss: SellStraddleStrategy, expiry: date) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=ss._underlying, atm_at_entry=24500.0, entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", 24500.0, 120.0, 120.0, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24500.0, 110.0, 110.0, open_time=datetime.now(IST)),
        net_credit=230.0, open_time=datetime.now(IST), status="open",
        lot_size=ss._lot_size * ss._lot_multiplier, expiry_date=expiry,
    )
    ss._position = pos
    return pos


def _patch_confirming_emit(ss: SellStraddleStrategy):
    async def _fake_emit(ev):
        if ev.action == "EXIT":
            fill = StraddleFillEvent(
                action="EXIT", underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id="C", binding_id="B", event_id=ev.event_id,
                legs=ev.legs,
            )
            ss._on_fill(fill)
    ss._emit_order = _fake_emit


def test_close_releases_pin_armed_by_restore(monkeypatch):
    """The exact real incident: a restored position's stale expiry (far week,
    e.g. next-next Monday's contract) must NOT stay pinned once that position
    closes -- the next entry scan needs the genuinely current expiry."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    stale_expiry = date.today() + timedelta(days=8)
    current_expiry = date.today() + timedelta(days=1)
    _open_position(ss, stale_expiry)

    # Simulate _reapply_expiry_stickiness_from_restored_position()'s own effect.
    ss._entry_expiry_date = stale_expiry
    ss._expiry_shifted_low_anchor_ltp = True
    ss._entry_expiry_pinned_from_restore = True

    monkeypatch.setattr(ss, "_effective_entry_expiry", lambda: current_expiry)
    _patch_confirming_emit(ss)

    async def _noop_unsub():
        pass
    ss._unsubscribe_entry_expiry_tokens = _noop_unsub

    asyncio.run(ss._close_position("post_restore_data_stale"))

    assert ss._position is None
    assert ss._entry_expiry_pinned_from_restore is False, \
        "restore-only pin must release once the position it protected closes"
    assert ss._expiry_shifted_low_anchor_ltp is False
    assert ss._entry_expiry_date == current_expiry, \
        "next entry scan must use the genuinely current expiry, not the stale restored one"


def test_close_does_not_touch_a_genuine_expiry_day_shift_pin(monkeypatch):
    """Contrast case: the SAME flag is also set for the legitimate "expiry-day
    shift, sticky for the rest of today" feature -- a plain close (not a
    restore-originated one) must never release that pin, since real re-entries
    later the same day are supposed to keep using the shifted expiry."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    shifted_expiry = date.today() + timedelta(days=8)
    _open_position(ss, shifted_expiry)

    ss._entry_expiry_date = shifted_expiry
    ss._expiry_shifted_low_anchor_ltp = True
    ss._entry_expiry_pinned_from_restore = False  # NOT restore-originated

    monkeypatch.setattr(ss, "_effective_entry_expiry",
                         lambda: (_ for _ in ()).throw(
                             AssertionError("must not recompute for a genuine expiry-day shift")))
    _patch_confirming_emit(ss)

    async def _noop_unsub():
        pass
    ss._unsubscribe_entry_expiry_tokens = _noop_unsub

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._position is None
    assert ss._expiry_shifted_low_anchor_ltp is True, \
        "a genuine same-day expiry-shift pin must survive a close"
    assert ss._entry_expiry_date == shifted_expiry


def test_reset_session_clears_the_restore_pin_flag():
    """A fresh trading day must never inherit yesterday's restore-pin state,
    same discipline as the existing _expiry_shifted_low_anchor_ltp reset."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._entry_expiry_pinned_from_restore = True
    ss._expiry_shifted_low_anchor_ltp = True

    ss.reset_session()

    assert ss._entry_expiry_pinned_from_restore is False


# ── trades_today revert on post_restore_data_stale (2026-09-07, direct user
# spec): "the close which happened today was not due to the condition met it
# was due to the ltp was not coming so it will be considered that we will
# start from beginning." A post_restore_data_stale close is a defensive
# safety-close caused by a data/feed problem, never a genuine trading
# decision -- unlike day_loss_sl/day_profit_target/day_low_reversal_exit/EOD/
# ITM-pair-gate (real exits driven by the strategy's own rules) or the
# single-side rolls (decay/ratio_exit/exit_rules/vwap_rise, which never touch
# trades_today at all since they don't go through entry-selection). ─────────

def test_post_restore_data_stale_close_reverts_trades_today_to_beginning():
    """The exact real incident: a restored position force-closed by the
    post-restore stale-feed guard must NOT consume the day's 'first trade'
    slot -- the next entry evaluation must see trades_today==0 again."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._trades_today = 1  # the restored position's own original entry incremented this
    _open_position(ss, date.today())
    _patch_confirming_emit(ss)

    async def _noop_unsub():
        pass
    ss._unsubscribe_entry_expiry_tokens = _noop_unsub

    asyncio.run(ss._close_position("post_restore_data_stale"))

    assert ss._trades_today == 0
    is_beginning = (ss._trades_today == 0)
    assert is_beginning is True


def test_post_restore_data_stale_close_never_goes_negative():
    """Defensive floor -- trades_today must never go below zero even if this
    close somehow fires when trades_today is already 0 (e.g. a future caller
    or an edge-case double-fire)."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._trades_today = 0
    _open_position(ss, date.today())
    _patch_confirming_emit(ss)

    async def _noop_unsub():
        pass
    ss._unsubscribe_entry_expiry_tokens = _noop_unsub

    asyncio.run(ss._close_position("post_restore_data_stale"))

    assert ss._trades_today == 0


def test_genuine_exit_reasons_do_not_revert_trades_today():
    """Contrast case: a REAL trading-decision exit (day_loss_sl here, same
    category as day_profit_target/day_low_reversal_exit/EOD/itm_pair_gate_
    profit) must never revert trades_today -- that trade genuinely happened
    and genuinely completed on its own merits, so RE-ENTRY logic is correct
    for whatever comes next today."""
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    ss._trades_today = 1
    _open_position(ss, date.today())
    _patch_confirming_emit(ss)

    async def _noop_unsub():
        pass
    ss._unsubscribe_entry_expiry_tokens = _noop_unsub

    asyncio.run(ss._close_position("day_loss_sl"))

    assert ss._trades_today == 1
    is_beginning = (ss._trades_today == 0)
    assert is_beginning is False
