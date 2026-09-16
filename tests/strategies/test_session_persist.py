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


# ── 2026-09-16, direct user spec: a position genuinely carrying overnight
# (is_hedged_positional=True) needs ALL of today's session bookkeeping to
# survive the day boundary intact -- "we require all data for current day so
# that immediate decision can be taken when trade starts next day at opening
# bell." Booked P&L feeds _check_hedge_cumulative_profit_close's own
# cumulative-profit math directly; itm_roll_protection/sl_cooldown_until
# matter for a still-open carried position's ongoing risk management. A
# normal (non-hedged) position keeps today's existing every-day-resets-fresh
# behavior completely unchanged (test_session_resets_next_day above). ───────

class _FakeHedgedPosition:
    """Minimal stand-in with a real .to_dict() so position_store.save()
    (called via self.persist(), the same mixin _persist() itself uses) can
    serialize it -- mirrors StraddlePosition's own shape just enough for
    _restore_session()'s peek (which only reads position['is_hedged_positional'])."""
    def __init__(self, is_hedged_positional=True):
        self.is_hedged_positional = is_hedged_positional
        self.status = "open"

    def to_dict(self):
        return {"is_hedged_positional": self.is_hedged_positional, "status": self.status}


def test_session_carries_forward_intact_across_day_boundary_when_hedged(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._position = _FakeHedgedPosition(is_hedged_positional=True)
    a.persist(a._persist_key, a._position.to_dict(), product_type="NRML")   # the real position file
    a._session_realized_pnl_pts = 7.25
    a._itm_roll_protection = {"CE": {"protect_rs": 90.0}}
    a._persist_session()

    # Force the CARRY file's own date old too, same as test_session_resets_next_day --
    # unlike that test, this must NOT be discarded, since it's hedged.
    import json
    p = ps._path("NIFTY_sell_straddle_session_carry")
    d = json.load(open(p)); d["date"] = "2020-01-01"; json.dump(d, open(p, "w"))

    b = _ss()
    b._restore_session()
    assert b._session_realized_pnl_pts == 7.25
    assert b._itm_roll_protection.get("CE", {}).get("protect_rs") == 90.0


def test_session_still_resets_next_day_when_position_open_but_not_hedged(tmp_path, monkeypatch):
    """A same-day-only position (is_hedged_positional=False, the common
    case) must keep today's existing reset-every-day behavior -- carry-
    forward is exclusively for a genuine overnight hedge-and-carry."""
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._position = _FakeHedgedPosition(is_hedged_positional=False)
    a.persist(a._persist_key, a._position.to_dict(), product_type="MIS")
    a._session_realized_pnl_pts = 5.0
    a._persist_session()
    import json
    p = ps._path("NIFTY_sell_straddle_session")
    d = json.load(open(p)); d["date"] = "2020-01-01"; json.dump(d, open(p, "w"))

    b = _ss()
    b._restore_session()
    assert b._session_realized_pnl_pts == 0.0


def test_persist_session_writes_carry_key_when_hedged(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._position = _FakeHedgedPosition(is_hedged_positional=True)
    a._session_realized_pnl_pts = 2.0
    a._persist_session()
    assert ps.load("NIFTY_sell_straddle_session_carry") is not None


def test_persist_session_never_writes_carry_key_when_not_hedged(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    c = _ss()
    c._position = _FakeHedgedPosition(is_hedged_positional=False)
    c._session_realized_pnl_pts = 2.0
    c._persist_session()
    assert ps.load("NIFTY_sell_straddle_session_carry") is None


def test_persist_clears_session_carry_key_when_position_genuinely_closes(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    a = _ss()
    a._position = _FakeHedgedPosition(is_hedged_positional=True)
    a._session_realized_pnl_pts = 2.0
    a._persist_session()
    assert ps.load("NIFTY_sell_straddle_session_carry") is not None

    a._position = None   # position genuinely closed
    a._persist()
    assert ps.load("NIFTY_sell_straddle_session_carry") is None
