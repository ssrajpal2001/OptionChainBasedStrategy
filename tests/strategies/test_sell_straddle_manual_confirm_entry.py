"""
tests/strategies/test_sell_straddle_manual_confirm_entry.py -- regression
for the 2026-08-23 direct user spec: "entry price should come from the
broker which is connected to the client. If broker doesn't send the data
we can manually enter the price in UI and click save and then application
will move depending on the price which is entered... client can update
entry rate of both legs and save and that value changes inside the app
for that client and exit condition will be checked accordingly."

Covers:
  1. _on_fill's existing ENTRY-abort branch now ALSO retains the decided
     strikes/expiry (self._last_aborted_entry) instead of just discarding
     everything -- the automatic discard behavior itself is UNCHANGED.
  2. manual_confirm_entry() / discard_aborted_entry() (entries.py).
"""
import asyncio
from datetime import date, datetime, timedelta

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _strategy() -> SellStraddleStrategy:
    s = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    return s


def _open_optimistic_position(s: SellStraddleStrategy, ce_strike=24500.0, pe_strike=24500.0) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=s._underlying, atm_at_entry=24500.0, entry_spot=24500.0,
        ce_leg=StraddleLeg("CE", ce_strike, 120.0, 120.0, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", pe_strike, 110.0, 110.0, open_time=datetime.now(IST)),
        net_credit=230.0, open_time=datetime.now(IST), status="open",
        lot_size=s._lot_size * s._lot_multiplier, expiry_date=date.today(),
    )
    s._position = pos
    s._trades_today = 1
    s._initial_net_credit = 230.0
    return pos


# ── _on_fill: retention on abort ─────────────────────────────────────────────

def test_full_entry_abort_retains_strikes_for_manual_confirm():
    s = _strategy()
    _open_optimistic_position(s)

    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="C", binding_id="B",
        event_id="ev_abort", entry_aborted=True,
    )
    s._on_fill(fill)

    # Automatic discard behavior is UNCHANGED.
    assert s._position is None
    # NEW: the attempt's strikes/expiry are retained for manual confirmation.
    assert s._last_aborted_entry is not None
    assert s._last_aborted_entry["ce_strike"] == 24500.0
    assert s._last_aborted_entry["pe_strike"] == 24500.0
    assert s._last_aborted_entry["expiry_date"] == date.today()


def test_no_retention_when_there_was_no_position_to_begin_with():
    s = _strategy()
    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="C", binding_id="B",
        event_id="ev_abort2", entry_aborted=True,
    )
    s._on_fill(fill)
    assert s._last_aborted_entry is None


def test_no_retention_for_single_leg_roll_reopen_abort():
    """The single-leg roll-reopen abort path returns early via
    _abort_roll_reopen and never reaches the full-discard branch this
    feature hooks into -- must not be affected."""
    s = _strategy()
    pos = _open_optimistic_position(s)
    called = {"abort_roll_reopen": False}

    async def _fake_abort_roll_reopen(fill):
        called["abort_roll_reopen"] = True
    s._abort_roll_reopen = _fake_abort_roll_reopen

    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=0.0, pe_fill=0.0, client_id="C", binding_id="B",
        event_id="ev_roll_abort", entry_aborted=True, legs=["CE"],
    )

    async def run():
        s._on_fill(fill)   # schedules asyncio.create_task(self._abort_roll_reopen(fill))
        await asyncio.sleep(0)   # let that task actually run
    asyncio.run(run())

    assert called["abort_roll_reopen"] is True
    assert s._last_aborted_entry is None
    assert s._position is pos   # untouched by this dispatch -- _abort_roll_reopen owns it


def test_confirmed_entry_does_not_touch_last_aborted_entry():
    s = _strategy()
    pos = _open_optimistic_position(s)
    s._last_aborted_entry = {"ce_strike": 24400.0, "pe_strike": 24400.0, "atm_at_entry": 24400.0,
                              "entry_spot": 24400.0, "expiry_date": date.today(),
                              "aborted_at": datetime.now(IST), "reason": "x",
                              "client_id": "C", "binding_id": "B"}
    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24500.0, ce_strike=24500.0, pe_strike=24500.0,
        ce_fill=121.0, pe_fill=111.0, client_id="C", binding_id="B", event_id="ev_ok",
    )
    s._on_fill(fill)
    assert s._position is not None and s._position.status == "open"
    # A confirmed fill is unrelated to any earlier pending manual-confirm record.
    assert s._last_aborted_entry is not None


# ── manual_confirm_entry ──────────────────────────────────────────────────────

def _pending(ce_strike=24500.0, pe_strike=24500.0, aborted_at=None):
    return {
        "ce_strike": ce_strike, "pe_strike": pe_strike, "atm_at_entry": 24500.0,
        "entry_spot": 24500.0, "expiry_date": date.today(),
        "aborted_at": aborted_at or datetime.now(IST), "reason": "asymmetric fill",
        "client_id": "C", "binding_id": "B",
    }


def test_manual_confirm_creates_open_position_from_retained_strikes_and_supplied_prices():
    s = _strategy()
    s._last_aborted_entry = _pending()

    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, 108.25))

    assert ok is True
    assert s._position is not None
    assert s._position.status == "open"
    assert s._position.ce_leg.strike == 24500.0
    assert s._position.ce_leg.entry_price == 125.5
    assert s._position.pe_leg.entry_price == 108.25
    assert s._position.ce_leg.open_reason == "manual_confirm"
    assert s._last_aborted_entry is None


def test_manual_confirm_increments_trades_today_and_credit():
    s = _strategy()
    s._trades_today = 0
    s._initial_net_credit = 0.0
    s._last_aborted_entry = _pending()

    asyncio.run(s.manual_confirm_entry(125.5, 108.25))

    assert s._trades_today == 1
    assert s._initial_net_credit == 125.5 + 108.25


def test_manual_confirm_never_dispatches_a_new_broker_order():
    """The real trade already happened at the broker -- this must only
    inform the app of the outcome, never place a fresh order."""
    from config.global_config import Topic
    s = _strategy()
    s._last_aborted_entry = _pending()
    published = []

    async def _capture_publish(topic, event):
        published.append((topic, event))
    s._bus.publish = _capture_publish

    asyncio.run(s.manual_confirm_entry(125.5, 108.25))

    order_events = [e for t, e in published if t == Topic.ORDER_REQUEST]
    assert order_events == []


def test_manual_confirm_rejects_when_nothing_pending():
    s = _strategy()
    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, 108.25))
    assert ok is False
    assert "no aborted entry" in msg.lower()


def test_manual_confirm_rejects_when_a_position_is_already_open():
    s = _strategy()
    _open_optimistic_position(s)
    s._last_aborted_entry = _pending()
    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, 108.25))
    assert ok is False
    assert "already has" in msg.lower()
    assert s._last_aborted_entry is not None   # left untouched, not silently discarded


def test_manual_confirm_rejects_non_positive_prices():
    s = _strategy()
    s._last_aborted_entry = _pending()
    ok, msg = asyncio.run(s.manual_confirm_entry(0.0, 108.25))
    assert ok is False
    assert s._position is None

    s._last_aborted_entry = _pending()
    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, -5.0))
    assert ok is False
    assert s._position is None


def test_manual_confirm_rejects_and_discards_a_stale_pending_record():
    s = _strategy()
    old = datetime.now(IST) - timedelta(hours=3)
    s._last_aborted_entry = _pending(aborted_at=old)
    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, 108.25))
    assert ok is False
    assert "stale" in msg.lower()
    assert s._position is None
    assert s._last_aborted_entry is None   # discarded, not left dangling


def test_manual_confirm_bypasses_stop_for_day():
    """A manual confirm isn't a NEW entry attempt -- it must work even if
    self._stop_for_day was set by repeated automatic-attempt failures."""
    s = _strategy()
    s._stop_for_day = True
    s._last_aborted_entry = _pending()
    ok, msg = asyncio.run(s.manual_confirm_entry(125.5, 108.25))
    assert ok is True
    assert s._position is not None


# ── discard_aborted_entry ─────────────────────────────────────────────────────

def test_discard_clears_pending_record():
    s = _strategy()
    s._last_aborted_entry = _pending()
    ok, msg = asyncio.run(s.discard_aborted_entry())
    assert ok is True
    assert s._last_aborted_entry is None
    assert s._position is None


def test_discard_rejects_when_nothing_pending():
    s = _strategy()
    ok, msg = asyncio.run(s.discard_aborted_entry())
    assert ok is False


# ── reset_session ──────────────────────────────────────────────────────────

def test_reset_session_clears_pending_record():
    s = _strategy()
    s._last_aborted_entry = _pending()
    s.reset_session()
    assert s._last_aborted_entry is None
