"""
2026-08-31: real incident, live NIFTY paper session -- a position entered
under a same-day low-anchor-LTP expiry shift (2026-09-08, not the natural
near-week expiry) got its ticks silently orphaned after a routine restart.
Root cause: self._expiry_shifted_low_anchor_ltp is plain in-memory state,
never persisted -- always False on a fresh process, so
_effective_entry_expiry() recomputed the ORIGINAL (unshifted) expiry while
the restored position's own .expiry_date stayed on the real, shifted
contract. _option_loop only updates a leg's ltp when
tick.expiry == pos.expiry_date, so once self._entry_expiry_date (which
subscriptions are built around) diverged from that, the position's own legs
stopped receiving ticks entirely -- eventually caught only by the
post-restore-stale-data safety close 5 minutes later (a real loss-of-
tracking event the guard happened to catch, not a fix).

Fix: _reapply_expiry_stickiness_from_restored_position() re-arms the sticky
flag to the restored position's own real expiry immediately after restore,
before any subscription/_effective_entry_expiry() call can run.
"""
from datetime import date

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _strategy():
    return SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")


def _position(expiry: date) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24150, entry_spot=24150,
        ce_leg=StraddleLeg("CE", 24150, 122.50, 122.50),
        pe_leg=StraddleLeg("PE", 24050, 130.40, 130.40),
        net_credit=252.90, status="open", expiry_date=expiry,
    )


def test_noop_when_no_position_restored():
    s = _strategy()
    s._position = None
    s._entry_expiry_date = None
    s._expiry_shifted_low_anchor_ltp = False
    s._reapply_expiry_stickiness_from_restored_position()
    assert s._entry_expiry_date is None
    assert s._expiry_shifted_low_anchor_ltp is False


def test_reapplies_sticky_flag_to_restored_positions_own_expiry():
    """The exact scenario from the real incident: a position entered under
    a shifted expiry (2026-09-08, not the natural near-week 2026-09-01)
    must re-arm the sticky flag to ITS OWN expiry on restore, not whatever
    _effective_entry_expiry() would otherwise recompute fresh."""
    s = _strategy()
    s._position = _position(date(2026, 9, 8))
    s._entry_expiry_date = date(2026, 9, 1)   # what a fresh process would default to
    s._expiry_shifted_low_anchor_ltp = False  # always starts False on a fresh process

    s._reapply_expiry_stickiness_from_restored_position()

    assert s._entry_expiry_date == date(2026, 9, 8)
    assert s._expiry_shifted_low_anchor_ltp is True


def test_effective_entry_expiry_honors_the_reapplied_sticky_flag():
    """End-to-end: after re-arming, _effective_entry_expiry() (the function
    subscriptions/new entries actually read) must return the restored
    position's own expiry, not recompute from scratch."""
    s = _strategy()
    s._position = _position(date(2026, 9, 8))
    s._entry_expiry_date = date(2026, 9, 1)
    s._expiry_shifted_low_anchor_ltp = False

    s._reapply_expiry_stickiness_from_restored_position()

    assert s._effective_entry_expiry() == date(2026, 9, 8)


def test_noop_when_restored_position_has_no_expiry_date():
    s = _strategy()
    s._position = _position(None)  # type: ignore[arg-type]
    s._entry_expiry_date = date(2026, 9, 1)
    s._expiry_shifted_low_anchor_ltp = False
    s._reapply_expiry_stickiness_from_restored_position()
    assert s._entry_expiry_date == date(2026, 9, 1)   # untouched
    assert s._expiry_shifted_low_anchor_ltp is False
