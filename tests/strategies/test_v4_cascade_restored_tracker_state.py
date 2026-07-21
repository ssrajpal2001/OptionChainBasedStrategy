"""strategies/v4_cascade/book.py's _restore_tracker_state_for_open_position
-- 2026-07-21 critical bugfix: self._engine._trackers/_tracking_entry_price
are pure in-memory engine state, never touched by _restore_position (which
only ever restores self._engine.position itself). That meant ANY restart
while a position was open left self._engine._trackers completely EMPTY, so
T2's entire exit-check block in engine.py._check_exits
(`if t2 is not None and t2.status == "open" and trail is not None:`) was
silently skipped forever after that restart -- T2 ran with ZERO stop-loss
enforcement for the rest of the trade's life, no matter what its persisted
sl_price/trail_stop_price displayed in the UI. This must rebuild both dicts
at boot from the position's persisted tracking_entry_price/
tracking_current_stop fields."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def _open_position(side="CE", strike=24200.0, tracking_entry_price=719.87,
                    t2_status="open", t2_tracking_current_stop=695.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=545.0, sl_price=695.0, target_price=722.3, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=545.0, sl_price=695.0, status=t2_status,
                     tracking_current_stop=t2_tracking_current_stop)
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, status="open",
        open_time=datetime(2026, 7, 21, 12, 15, tzinfo=IST),
        tracking_entry_price=tracking_entry_price,
    )


def test_restores_tracker_with_persisted_current_stop():
    book = _book()
    book._engine.position = _open_position()

    book._restore_tracker_state_for_open_position()

    assert book._engine._tracking_entry_price["CE"] == 719.87
    tracker = book._engine._trackers["CE"]
    assert tracker is not None
    assert tracker.current_stop == 695.0
    assert tracker._bear is True   # NIFTY always bear-geometry


def test_falls_back_to_sl_price_when_tracking_current_stop_missing():
    """Positions persisted before this fix have no tracking_current_stop at
    all -- must still get SOME protection (T2's own structural SL) rather
    than being left with current_stop=None (unprotected)."""
    book = _book()
    book._engine.position = _open_position(t2_tracking_current_stop=None)

    book._restore_tracker_state_for_open_position()

    tracker = book._engine._trackers["CE"]
    assert tracker.current_stop == 695.0   # t2.sl_price fallback


def test_noop_when_no_position_open():
    book = _book()
    book._engine.position = None

    book._restore_tracker_state_for_open_position()

    assert book._engine._trackers == {}
    assert book._engine._tracking_entry_price == {}


def test_noop_tracker_when_t2_already_closed():
    """T2 already closed (only T1 was ever open, or T2 closed before a
    restart) -- no tracker needed, but tracking_entry_price should still be
    restored (harmless, and needed if T1 itself somehow still referenced it)."""
    book = _book()
    book._engine.position = _open_position(t2_status="closed")

    book._restore_tracker_state_for_open_position()

    assert "CE" not in book._engine._trackers
    assert book._engine._tracking_entry_price["CE"] == 719.87
