"""Persistent client trade history: record on close, read newest-first."""
from data_layer import trade_history as th

CID = "TESTHISTCLIENT"


def _cleanup():
    import os
    p = th._path(CID)
    if os.path.exists(p):
        os.remove(p)


def test_record_and_load_newest_first():
    _cleanup()
    try:
        th.record(CID, "sell_straddle", "NIFTY", 100.0, 80.0, "profit_target", 20.0, ts="2026-06-02T10:00:00")
        th.record(CID, "iron_condor", "FINNIFTY", 50.0, 55.0, "stop_loss", -5.0, ts="2026-06-02T11:00:00")
        rows = th.load(CID)
        assert len(rows) == 2
        assert rows[0]["instrument"] == "FINNIFTY"   # newest first
        assert rows[0]["pnl"] == -5.0
        assert rows[1]["strategy"] == "sell_straddle"
        assert rows[1]["exit_reason"] == "profit_target"
    finally:
        _cleanup()


def test_load_empty_for_unknown_client():
    assert th.load("NO_SUCH_CLIENT_XYZ") == []
