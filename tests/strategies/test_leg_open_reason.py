from strategies.sell_straddle import StraddleLeg


def test_leg_has_open_reason():
    lg = StraddleLeg("CE", 8800, 380.0, 380.0)
    assert hasattr(lg, "open_reason") and lg.open_reason == ""
    lg.open_reason = "single_side_roll_vwap_rise_roll"
    assert lg.open_reason


def test_trade_history_leg_entry_reason_roundtrip(tmp_path, monkeypatch):
    from data_layer import trade_history as th

    cid = "test_leg_reason_cid"
    monkeypatch.setattr(th, "_path", lambda c: str(tmp_path / f"{c}.json"))

    th.record(
        cid, "sell_straddle", "NIFTY", 100.0, 80.0, "profit_target", 1000.0,
        legs=[{"side": "CE", "strike": 23500, "entry": 100.0, "exit": 80.0,
               "pnl": 1000.0, "entry_ts": "2026-06-06T09:20:00",
               "exit_ts": "2026-06-06T11:30:00",
               "entry_reason": "beginning"}],
    )
    recs = th.load(cid)
    assert len(recs) == 1
    leg = recs[0]["legs"][0]
    assert leg["entry_reason"] == "beginning"


def test_trade_history_legs_without_entry_reason_default(tmp_path, monkeypatch):
    from data_layer import trade_history as th

    cid = "test_leg_noreason_cid"
    monkeypatch.setattr(th, "_path", lambda c: str(tmp_path / f"{c}.json"))
    th.record(
        cid, "sell_straddle", "NIFTY", 100.0, 80.0, "x", 1000.0,
        legs=[{"side": "CE", "strike": 23500, "entry": 100.0, "exit": 80.0, "pnl": 1000.0}],
    )
    recs = th.load(cid)
    leg = recs[0]["legs"][0]
    assert leg["entry_reason"] == ""
