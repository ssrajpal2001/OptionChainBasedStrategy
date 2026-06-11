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
