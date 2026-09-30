"""tests/data_layer/test_strike_rebalancer_late_pin_catchup.py -- regression for the
2026-09-30 real live incident: an Iron Fly mid-day restart showed every leg's LTP
stuck at "--" indefinitely on the dashboard.

Root cause: StrikeRebalancer.pin_strike() was pure bookkeeping (added the strike to
pinned_strikes/active_strikes) but never actually told the feeder to subscribe it.
The real WS subscribe only ever happened in _initial_subscribe() (once per day, on
the first live index tick) or a later ATM-drift _rebalance(). On a mid-day process
restart, _UnderlyingState is fresh again, so _initial_subscribe() does fire on the
next tick -- but it's a pure task-scheduling race against a book's own
_restore_position()/pin_strike() calls. If pin_strike() lands AFTER
_initial_subscribe() has already run for the session, the strike was pinned but
never subscribed, and (being a well-OTM Iron Fly wing leg) could go the whole
session with zero live ticks.

Fix: pin_strike() now fires an immediate one-strike catch-up subscribe
(_catchup_pin_subscribe) whenever the strike is pinned AFTER this session's
open_atm has already been recorded and the strike isn't already active -- mirrors
enable_chain()'s existing _catchup_chain_subscribe pattern for the same class of
"registered after the fact" gap.
"""
import asyncio

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from data_layer.strike_rebalancer import StrikeRebalancer


class _FakeFeeder:
    def __init__(self):
        self.subscribed: list = []

    async def subscribe_tokens(self, tokens):
        self.subscribed.append(list(tokens))


def _make_rebalancer(feeder):
    cfg = GlobalConfig()
    return StrikeRebalancer(EventBus(), cfg, feeder=feeder)


def _run(coro):
    # pin_strike() calls asyncio.ensure_future() synchronously (outside any
    # running loop), which schedules onto the thread's default event loop --
    # so this helper must run on that SAME loop (get_event_loop, not a fresh
    # new_event_loop()) for the scheduled catch-up task to actually execute.
    asyncio.get_event_loop().run_until_complete(coro)


def test_pin_before_open_atm_does_not_trigger_catchup(monkeypatch):
    """Before this session's _initial_subscribe() has ever run (open_atm is
    still None), a pin is pure bookkeeping -- the strike will naturally be
    unioned into the window when _initial_subscribe() eventually fires, so no
    separate catch-up subscribe should happen."""
    feeder = _FakeFeeder()
    rb = _make_rebalancer(feeder)
    monkeypatch.setattr(rb, "_strikes_to_tokens", lambda u, s: [f"{u}:{k}" for k in s])

    rb.pin_strike("NIFTY", 23100.0)
    _run(asyncio.sleep(0))  # let any scheduled tasks run

    assert feeder.subscribed == []
    assert 23100.0 in rb.pinned_strikes("NIFTY")


def test_pin_after_open_atm_and_not_active_triggers_immediate_catchup(monkeypatch):
    """The real incident: a strike pinned (e.g. IronFlyStrategy._sync_strike_pins()
    on restore) after _initial_subscribe() already ran this session, and not
    already inside the subscribed window -- must be caught up immediately, not
    left to wait on a lucky ATM-drift rebalance."""
    feeder = _FakeFeeder()
    rb = _make_rebalancer(feeder)
    monkeypatch.setattr(rb, "_strikes_to_tokens", lambda u, s: [f"{u}:{k}" for k in s])

    st = rb._state["NIFTY"]
    st.open_atm = 23500.0
    st.current_atm = 23500.0
    st.active_strikes = {23500.0, 23550.0, 23450.0}  # simulates a narrow already-subscribed window

    rb.pin_strike("NIFTY", 23100.0)  # a far-OTM Iron Fly wing leg, outside that window
    _run(asyncio.sleep(0))

    assert feeder.subscribed == [["NIFTY:23100.0"]]
    assert 23100.0 in rb.active_strikes("NIFTY")


def test_pin_of_an_already_active_strike_does_not_resubscribe(monkeypatch):
    """A strike already inside the live subscription window (e.g. an ATM leg
    that happens to already be ticking) must not trigger a redundant
    subscribe call just because pin_strike() is called on it too."""
    feeder = _FakeFeeder()
    rb = _make_rebalancer(feeder)
    monkeypatch.setattr(rb, "_strikes_to_tokens", lambda u, s: [f"{u}:{k}" for k in s])

    st = rb._state["NIFTY"]
    st.open_atm = 23500.0
    st.current_atm = 23500.0
    st.active_strikes = {23500.0}

    rb.pin_strike("NIFTY", 23500.0)
    _run(asyncio.sleep(0))

    assert feeder.subscribed == []
