import datetime
from config.global_config import IST
from strategies.sell_straddle import StraddleLeg


def test_leg_has_open_close_time_fields():
    lg = StraddleLeg("CE", 23500, 100.0, 100.0)
    assert hasattr(lg, "open_time") and hasattr(lg, "close_time")
    lg.open_time = datetime.datetime.now(IST)
    assert lg.open_time is not None


def test_trade_history_leg_timestamps_roundtrip(tmp_path, monkeypatch):
    from data_layer import trade_history as th

    cid = "test_leg_ts_cid"
    monkeypatch.setattr(th, "_path", lambda c: str(tmp_path / f"{c}.json"))

    th.record(
        cid, "sell_straddle", "NIFTY", 100.0, 80.0, "profit_target", 1000.0,
        legs=[{"side": "CE", "strike": 23500, "entry": 100.0, "exit": 80.0,
               "pnl": 1000.0, "entry_ts": "2026-06-06T09:20:00",
               "exit_ts": "2026-06-06T11:30:00"}],
    )
    recs = th.load(cid)
    assert len(recs) == 1
    leg = recs[0]["legs"][0]
    assert leg["entry_ts"] == "2026-06-06T09:20:00"
    assert leg["exit_ts"] == "2026-06-06T11:30:00"


def test_trade_history_legs_without_timestamps_still_load(tmp_path, monkeypatch):
    from data_layer import trade_history as th

    cid = "test_leg_nots_cid"
    monkeypatch.setattr(th, "_path", lambda c: str(tmp_path / f"{c}.json"))
    th.record(
        cid, "sell_straddle", "NIFTY", 100.0, 80.0, "x", 1000.0,
        legs=[{"side": "CE", "strike": 23500, "entry": 100.0, "exit": 80.0, "pnl": 1000.0}],
    )
    recs = th.load(cid)
    leg = recs[0]["legs"][0]
    assert leg["entry_ts"] is None
    assert leg["exit_ts"] is None
