"""Regression test for the 2026-08-06 CRITICAL fix to _build_exit_criteria
(strategies/sell_straddle/exits.py). Before the fix, every criterion was
built inside ONE giant try/except -- an exception in an EARLIER section
(e.g. Day%) silently discarded every criterion after it, including the
Dynamic (exit_rules) stop-loss, with zero log trace. This is the most
dangerous class of bug possible for a stop-loss: it silently stops firing,
indistinguishable in the logs from "rules genuinely didn't pass."

Each criterion must now be independently guarded so a failure in one
section can never suppress evaluation of the others.
"""
import datetime

from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _position():
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, 90.0, open_time=datetime.datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24500, 100.0, 95.0, open_time=datetime.datetime.now(IST)),
        net_credit=200.0, status="open",
    )


def test_day_pct_exception_does_not_suppress_dynamic_exit_rules(monkeypatch):
    """The exact scenario the audit flagged: Day% throws, but the Dynamic
    (exit_rules) stop-loss must still be evaluated and appear in _crit."""
    bus = EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._day_profit_target_pct = 30.0
    s._day_loss_sl_pct = 30.0
    s._exit_rules = [{"tf": "1", "indicator": "rsi", "op": ">", "value": 200}]

    def _boom(pos):
        raise RuntimeError("simulated Day% failure")
    monkeypatch.setattr(s, "_day_pct", _boom)

    crit, _dump = s._build_exit_criteria(_position(), pnl=0.0, credit=200.0)

    names = [c[0] for c in crit]
    assert "Dynamic" in names, f"Dynamic criterion was silently suppressed by the Day% exception: {crit}"


def test_dynamic_exit_rules_exception_does_not_suppress_itm_gate(monkeypatch):
    """The reverse direction: exit_rules itself throws -- ITMgate (evaluated
    after it) must still run."""
    bus = EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._exit_rules = [{"tf": "1", "indicator": "rsi", "op": ">", "value": 200}]
    s._itm_pair_gate_enabled = True

    def _boom(*a, **k):
        raise RuntimeError("simulated exit_rules failure")
    monkeypatch.setattr(s, "_ind_by_tf", _boom)
    monkeypatch.setattr(s, "_both_itm", lambda: False)

    crit, _dump = s._build_exit_criteria(_position(), pnl=0.0, credit=200.0)

    names = [c[0] for c in crit]
    assert "ITMgate" in names, f"ITMgate was silently suppressed by the exit_rules exception: {crit}"
    assert "Dynamic" not in names  # it genuinely failed and should not silently appear as passing


def test_all_criteria_independently_survive_each_others_failures(monkeypatch):
    """Sanity check: even with Day% AND exit_rules both throwing, the
    remaining independent criteria (LTPdecay/Ratio/VWAPrise) still build."""
    bus = EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._day_profit_target_pct = 30.0
    s._day_loss_sl_pct = 30.0
    s._ltp_decay_enabled = True
    s._ltp_exit_min = 20.0
    s._exit_rules = [{"tf": "1", "indicator": "rsi", "op": ">", "value": 200}]

    monkeypatch.setattr(s, "_day_pct", lambda pos: (_ for _ in ()).throw(RuntimeError("boom1")))
    monkeypatch.setattr(s, "_ind_by_tf", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom2")))

    crit, _dump = s._build_exit_criteria(_position(), pnl=0.0, credit=200.0)

    names = [c[0] for c in crit]
    assert "LTPdecay" in names
