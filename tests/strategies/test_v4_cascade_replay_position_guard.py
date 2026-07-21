"""strategies/v4_cascade/book.py's _guard_replay_position -- 2026-07-21
critical bugfix: a real, restored-from-disk open position silently
disappeared from the UI after a routine restart, with zero log trace,
because the OLD guard compared OBJECT IDENTITY (`is not
pos_before_replay`), which only catches replay OPENING a brand-new
position -- it completely misses replay CLOSING an already-restored
position, since the exit logic mutates pos.status/leg.status IN PLACE on
the SAME object rather than replacing it. The fix compares the full
serialized VALUE (to_dict()) instead of identity."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

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


def _open_position(side="CE", entry_price=27.25, strike=24250.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=entry_price, sl_price=20.0, target_price=40.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=entry_price, sl_price=20.0, status="open")
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, status="open",
        open_time=datetime(2026, 7, 21, 11, 45, tzinfo=IST),
    )


def test_in_place_close_mutation_during_replay_is_detected_and_reverted():
    """The exact live bug: replay mutates pos.status/leg.status to 'closed'
    IN PLACE on the SAME object (no new object assigned) -- must still be
    caught and reverted."""
    book = _book()
    pos = _open_position()
    book._engine.position = pos
    snapshot = book._position_snapshot(pos)

    # Simulate exactly what _check_exits does during replay: mutate the leg
    # statuses and the position status IN PLACE, same object identity.
    pos.t1.status = "closed"
    pos.t1.close_price = 208.10
    pos.t2.status = "closed"
    pos.t2.close_price = 173.20
    pos.status = "closed"
    assert book._engine.position is pos  # still the same object (the bug's blind spot)

    book._guard_replay_position(snapshot)

    assert book._engine.position is not None
    assert book._engine.position.status == "open"
    assert book._engine.position.t1.status == "open"
    assert book._engine.position.t2.status == "open"
    assert book._engine.position.side == "CE"
    assert book._engine.position.t1.entry_price == 27.25


def test_object_replacement_open_during_replay_is_still_detected():
    """The case the OLD identity-only guard DID already catch -- confirm the
    new value-based guard still catches it too (no regression)."""
    book = _book()
    book._engine.position = None
    snapshot = book._position_snapshot(None)

    # Simulate replay fabricating a brand-new position out of thin air.
    book._engine.position = _open_position(side="PE", entry_price=50.0)

    book._guard_replay_position(snapshot)

    assert book._engine.position is None


def test_untouched_position_is_left_alone_same_object():
    book = _book()
    pos = _open_position()
    book._engine.position = pos
    snapshot = book._position_snapshot(pos)

    # Replay ran but genuinely didn't touch the position at all.
    book._guard_replay_position(snapshot)

    assert book._engine.position is pos  # untouched -- no unnecessary reconstruction


def test_no_position_before_and_after_is_a_noop():
    book = _book()
    book._engine.position = None
    snapshot = book._position_snapshot(None)

    book._guard_replay_position(snapshot)

    assert book._engine.position is None
