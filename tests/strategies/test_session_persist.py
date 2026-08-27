"""Day booked-P&L (session_realized_pnl_pts) must survive a same-day restart so the
Booked P&L / dashboard / header P&L don't reset to 0 (user bug 2026-06-11: physical roll
booked +234 in history but Booked P&L showed 0 after a restart)."""
import data_layer.position_store as ps
from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus


def _ss():
    return SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")


def test_session_pnl_survives_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._session_realized_pnl_pts = 3.6      # ~+234 / 65
    a._trades_today = 2
    a._persist_session()
    # New instance (simulates restart) restores the booked P&L.
    b = _ss()
    b._restore_session()
    assert round(b._session_realized_pnl_pts, 2) == 3.6
    assert b._trades_today >= 2


def test_itm_roll_protection_survives_restart(tmp_path, monkeypatch):
    """2026-08-27, real user-found incident: an armed 70%-roll-protection
    budget was NEVER persisted -- a restart silently wiped it while the
    rolled leg kept running with zero protective stop (ran past 100% of
    the profit that armed it, unprotected, across a restart)."""
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._itm_roll_protection = {
        "CE": {"protect_rs": 111.0, "new_side": "CE", "new_strike": 24350,
               "orig_strike": 24450, "kept_side": "PE", "kept_strike": 24450},
    }
    a._persist_session()
    # New instance (simulates restart) restores the armed budget exactly.
    b = _ss()
    b._restore_session()
    assert "CE" in b._itm_roll_protection
    assert b._itm_roll_protection["CE"]["protect_rs"] == 111.0
    assert b._itm_roll_protection["CE"]["new_strike"] == 24350
    assert b._itm_roll_protection["CE"]["orig_strike"] == 24450


def test_sl_cooldown_survives_restart(tmp_path, monkeypatch):
    """2026-08-27, same audit: a stop-out's re-entry cooldown was in-memory
    only -- a restart right after a stop-out silently forgot it and let the
    book re-enter immediately, defeating the point of resting after a loss."""
    from config.global_config import IST
    from datetime import datetime, timedelta
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    future = datetime.now(IST) + timedelta(minutes=30)
    a._sl_cooldown_until = future
    a._persist_session()
    b = _ss()
    b._restore_session()
    assert b._sl_cooldown_until is not None
    assert abs((b._sl_cooldown_until - future).total_seconds()) < 1


def test_sl_cooldown_not_restored_once_already_expired(tmp_path, monkeypatch):
    """A cooldown boundary that's already in the past by the time of restart
    must not be restored -- would needlessly block re-entry forever."""
    from config.global_config import IST
    from datetime import datetime, timedelta
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._sl_cooldown_until = datetime.now(IST) - timedelta(minutes=5)
    a._persist_session()
    b = _ss()
    b._sl_cooldown_until = None
    b._restore_session()
    assert b._sl_cooldown_until is None


def test_session_resets_next_day(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._session_realized_pnl_pts = 5.0
    a._persist_session()
    # Force the stored file to a prior date → MIS store discards it on load.
    import json, os
    p = ps._path("NIFTY_sell_straddle_session")
    d = json.load(open(p)); d["date"] = "2020-01-01"; json.dump(d, open(p, "w"))
    b = _ss()
    b._restore_session()
    assert b._session_realized_pnl_pts == 0.0
