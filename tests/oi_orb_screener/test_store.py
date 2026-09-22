"""
2026-08-24: tests for strategies/oi_orb_screener/store.py -- the SQLite
persistence + audit-trail layer built after a real incident (DIXON PE14500,
entered 13:51, a pm2 restart at ~14:40 silently lost all memory the position
existed). Each test points store._DB_PATH at an isolated tmp_path file and
resets store._initialized so tests never touch the real
data/oi_orb_screener.db or leak WAL state across tests in the same process.
"""
import pytest

from strategies.oi_orb_screener import store


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "_DB_PATH", str(tmp_path / "oi_orb_test.db"))
    monkeypatch.setattr(store, "_initialized", False)
    yield


def test_record_scan_then_update_regime_roundtrip():
    store.record_scan("C1", "B1", 1.23, "ok", trade_date="2026-08-24")
    store.update_scan_regime("C1", "B1", "bullish", trade_date="2026-08-24")

    con = __import__("sqlite3").connect(store._DB_PATH)
    row = con.execute(
        "SELECT nifty_pchange, status, regime FROM scans WHERE client_id=? AND binding_id=?",
        ("C1", "B1"),
    ).fetchone()
    con.close()
    assert row == (1.23, "ok", "bullish")


def test_record_scan_upsert_overwrites_same_day_row():
    store.record_scan("C1", "B1", 0.0, "fetch_failed", "boom", trade_date="2026-08-24")
    store.record_scan("C1", "B1", 2.5, "ok", trade_date="2026-08-24")

    con = __import__("sqlite3").connect(store._DB_PATH)
    rows = con.execute("SELECT status, nifty_pchange FROM scans WHERE client_id=? AND binding_id=?",
                        ("C1", "B1")).fetchall()
    con.close()
    assert rows == [("ok", 2.5)]  # one row, overwritten -- not two


def test_record_shortlist_and_update_orb_levels():
    store.record_shortlist("C1", "B1", [
        {"symbol": "DIXON", "price_change_pct": -2.03, "oi_spurt_pct": 8.1, "score": 0.6, "side_bias": "bearish"},
        {"symbol": "SIEMENS", "price_change_pct": 3.93, "oi_spurt_pct": 9.0, "score": 0.8, "side_bias": "bullish"},
    ], trade_date="2026-08-24")
    store.update_orb_levels("C1", "B1", "DIXON", 14976.0, 14542.0, trade_date="2026-08-24")

    con = __import__("sqlite3").connect(store._DB_PATH)
    con.row_factory = __import__("sqlite3").Row
    rows = {r["symbol"]: dict(r) for r in con.execute(
        "SELECT * FROM shortlist WHERE client_id=? AND binding_id=?", ("C1", "B1")).fetchall()}
    con.close()
    assert rows["DIXON"]["orb_high"] == 14976.0
    assert rows["DIXON"]["orb_low"] == 14542.0
    assert rows["SIEMENS"]["orb_high"] is None  # never frozen for this symbol


def test_record_rank_snapshot_multiple_polls_each_kept_separately():
    """2026-08-30, direct user spec: every poll is its own permanent
    snapshot (not upserted) -- the whole point is the time-series across
    the 09:16-09:30 window for after-market time/threshold optimization."""
    store.record_rank_snapshot("C1", "B1", "2026-08-30T09:16:00+05:30", [
        {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "price_change_pct": 3.0},
        {"symbol": "BBB", "rank": 2, "oi_spurt_pct": 15.0, "price_change_pct": -2.5},
    ], trade_date="2026-08-30")
    store.record_rank_snapshot("C1", "B1", "2026-08-30T09:18:00+05:30", [
        {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 22.0, "price_change_pct": 3.2},
    ], trade_date="2026-08-30")

    con = __import__("sqlite3").connect(store._DB_PATH)
    rows = con.execute(
        "SELECT poll_ts, symbol, rank, oi_spurt_pct FROM rank_snapshots "
        "WHERE client_id=? AND binding_id=? ORDER BY poll_ts, rank", ("C1", "B1")).fetchall()
    con.close()
    assert rows == [
        ("2026-08-30T09:16:00+05:30", "AAA", 1, 20.0),
        ("2026-08-30T09:16:00+05:30", "BBB", 2, 15.0),
        ("2026-08-30T09:18:00+05:30", "AAA", 1, 22.0),
    ]


def test_log_signal_event_and_load_already_fired():
    store.log_signal_event("C1", "B1", "DIXON", "signal_fired", side="PUT",
                            detail="orb_low_breakdown", trigger_price=14540.0,
                            orb_high=14976.0, orb_low=14542.0, trade_date="2026-08-24")
    store.log_signal_event("C1", "B1", "SIEMENS", "signal_skipped_rejected", side="CALL",
                            trade_date="2026-08-24")

    fired = store.load_already_fired("C1", "B1", trade_date="2026-08-24")
    assert fired == {("DIXON", "PUT")}
    # signal_skipped_rejected must NOT count as already-fired
    assert ("SIEMENS", "CALL") not in fired


def test_log_rejection_rule_triggered_and_load_rejected():
    store.log_signal_event("C1", "B1", "VMM", "rejection_rule_triggered", side="CALL",
                            trade_date="2026-08-24")
    rejected = store.load_rejected("C1", "B1", trade_date="2026-08-24")
    assert rejected == {("VMM", "CALL")}


def test_open_then_close_position_roundtrip_with_pnl():
    store.open_position("C1", "B1", "DIXON", "PE", 14500, "2026-08-25", 50, 118.80,
                         "orb_low_breakdown", True, "EVT1", trade_date="2026-08-24")

    open_rows = store.load_open_positions("C1", "B1", trade_date="2026-08-24")
    assert len(open_rows) == 1
    assert open_rows[0]["symbol"] == "DIXON"
    assert open_rows[0]["strike"] == 14500
    assert open_rows[0]["status"] == "open"

    store.close_position("C1", "B1", "DIXON", 95.30, "sma_exit", -1176.0, trade_date="2026-08-24")

    still_open = store.load_open_positions("C1", "B1", trade_date="2026-08-24")
    assert still_open == []

    con = __import__("sqlite3").connect(store._DB_PATH)
    con.row_factory = __import__("sqlite3").Row
    row = dict(con.execute("SELECT * FROM positions WHERE client_id=? AND binding_id=?",
                            ("C1", "B1")).fetchone())
    con.close()
    assert row["status"] == "closed"
    assert row["exit_price"] == 95.30
    assert row["exit_reason"] == "sma_exit"
    assert row["pnl"] == -1176.0


def test_load_open_positions_is_scoped_to_the_given_trade_date():
    """A position genuinely opened YESTERDAY and never explicitly closed
    (e.g. broker EOD auto-squareoff happened but this book never confirmed
    it) must NOT resurrect as still-open when restoring TODAY -- these are
    always intraday positions, same MIS same-day-only discipline as
    data_layer/position_store.py."""
    store.open_position("C1", "B1", "DIXON", "PE", 14500, "2026-08-23", 50, 118.80,
                         "orb_low_breakdown", True, "EVT_YESTERDAY", trade_date="2026-08-23")

    today_rows = store.load_open_positions("C1", "B1", trade_date="2026-08-24")
    assert today_rows == []

    yesterday_rows = store.load_open_positions("C1", "B1", trade_date="2026-08-23")
    assert len(yesterday_rows) == 1


def test_close_position_with_no_open_row_is_a_safe_noop():
    # Must not raise even though nothing was ever opened for this symbol.
    store.close_position("C1", "B1", "GHOST", 100.0, "eod_squareoff", 0.0, trade_date="2026-08-24")
    assert store.load_open_positions("C1", "B1", trade_date="2026-08-24") == []


def test_close_position_records_exit_detail(monkeypatch):
    """2026-09-16, direct user spec: the history should show WHY an exit
    happened with real values and the exact candle time, not just the bare
    exit_reason code."""
    store.open_position("C1", "B1", "DIXON", "PE", 14500, "2026-08-25", 50, 118.80,
                         "orb_low_breakdown", True, "EVT1", trade_date="2026-08-24")
    detail = "candle=[10:15-10:35) close=553.00 vwap=551.65 live_ltp=553.00"
    store.close_position("C1", "B1", "DIXON", 108.0, "vwap_close_sl", -540.0,
                          trade_date="2026-08-24", exit_detail=detail)

    con = __import__("sqlite3").connect(store._DB_PATH)
    row = con.execute(
        "SELECT exit_reason, exit_detail, pnl FROM positions WHERE client_id=? AND binding_id=? AND symbol=?",
        ("C1", "B1", "DIXON"),
    ).fetchone()
    con.close()
    assert row == ("vwap_close_sl", detail, -540.0)


def test_close_position_exit_detail_defaults_to_empty_string():
    store.open_position("C1", "B1", "DIXON", "PE", 14500, "2026-08-25", 50, 118.80,
                         "orb_low_breakdown", True, "EVT1", trade_date="2026-08-24")
    store.close_position("C1", "B1", "DIXON", 108.0, "eod_squareoff", -540.0, trade_date="2026-08-24")

    con = __import__("sqlite3").connect(store._DB_PATH)
    row = con.execute(
        "SELECT exit_detail FROM positions WHERE client_id=? AND binding_id=? AND symbol=?",
        ("C1", "B1", "DIXON"),
    ).fetchone()
    con.close()
    assert row == ("",)


def test_positions_are_scoped_per_client_binding():
    store.open_position("C1", "B1", "DIXON", "PE", 14500, "2026-08-25", 50, 118.80,
                         "orb_low_breakdown", True, "EVT1", trade_date="2026-08-24")
    store.open_position("C2", "B2", "DIXON", "PE", 14500, "2026-08-25", 50, 120.0,
                         "orb_low_breakdown", True, "EVT2", trade_date="2026-08-24")

    c1_rows = store.load_open_positions("C1", "B1", trade_date="2026-08-24")
    c2_rows = store.load_open_positions("C2", "B2", trade_date="2026-08-24")
    assert len(c1_rows) == 1 and len(c2_rows) == 1
    assert c1_rows[0]["entry_price"] == 118.80
    assert c2_rows[0]["entry_price"] == 120.0


# ── option_native_feature_history / option_native_selection (2026-09-22) ──
# See strategies/oi_orb_screener/option_native.py's own module docstring
# for what each table captures -- these tests just confirm the DB
# round-trip, same isolated-tmp-db pattern as every other table above.

import json as _json
import sqlite3 as _sqlite3


def test_record_option_native_feature_bar_roundtrip():
    store.record_option_native_feature_bar("C1", "B1", "2026-09-22T09:25:00", [
        {"symbol": "TCS", "option_type": "CE", "upstox_key": "NSE_FO|CE2100",
         "ltp_close": 36.5, "volume_5min": 500.0, "change_oi": 12.0, "bid": 36.2, "ask": 36.8,
         "iv": 25.1, "delta": 0.57, "vwap": 35.9, "score": 7,
         "score_breakdown": _json.dumps({"ltp_above_vwap": True}),
         "oi_price_reversal_state": "long_buildup"},
    ], trade_date="2026-09-22")

    con = _sqlite3.connect(store._DB_PATH)
    con.row_factory = _sqlite3.Row
    row = dict(con.execute(
        "SELECT * FROM option_native_feature_history WHERE client_id=? AND binding_id=?",
        ("C1", "B1")).fetchone())
    con.close()
    assert row["symbol"] == "TCS"
    assert row["option_type"] == "CE"
    assert row["score"] == 7
    assert row["oi_price_reversal_state"] == "long_buildup"
    assert _json.loads(row["score_breakdown"]) == {"ltp_above_vwap": True}
    assert row["delta"] == 0.57


def test_record_option_native_feature_bar_not_upserted_each_bucket_is_its_own_row():
    for i in range(3):
        store.record_option_native_feature_bar("C1", "B1", f"2026-09-22T09:{25+i*5}:00", [
            {"symbol": "TCS", "option_type": "CE", "upstox_key": "NSE_FO|CE2100",
             "ltp_close": 36.0 + i, "volume_5min": 500.0, "change_oi": 1.0},
        ], trade_date="2026-09-22")
    con = _sqlite3.connect(store._DB_PATH)
    n = con.execute(
        "SELECT COUNT(*) FROM option_native_feature_history WHERE client_id=? AND binding_id=?",
        ("C1", "B1")).fetchone()[0]
    con.close()
    assert n == 3


def test_record_option_native_selection_picked_side():
    store.record_option_native_selection("C1", "B1", "2026-09-22", [
        {"symbol": "TCS", "option_type": "CE", "expiry": "2026-09-25", "strike": 2100,
         "upstox_key": "NSE_FO|CE2100", "delta": 0.57, "iv": 25.1, "theta": -1.2, "gamma": 0.002,
         "vega": 3.1, "ltp": 36.5, "bid": 36.2, "ask": 36.8, "oi": 4500, "prev_oi": 4000,
         "volume": 1200, "target_delta": 0.55, "delta_distance": 0.02, "tie_break_used": False,
         "candidates_in_band": _json.dumps([{"strike": 2100, "delta": 0.57}]),
         "price_change_pct": 2.3, "oi_spurt_pct": 8.5, "skipped": False},
    ])
    con = _sqlite3.connect(store._DB_PATH)
    con.row_factory = _sqlite3.Row
    row = dict(con.execute(
        "SELECT * FROM option_native_selection WHERE client_id=? AND binding_id=?",
        ("C1", "B1")).fetchone())
    con.close()
    assert row["strike"] == 2100
    assert row["skipped"] == 0
    assert row["tie_break_used"] == 0
    assert row["delta_distance"] == pytest.approx(0.02)
    assert _json.loads(row["candidates_in_band"]) == [{"strike": 2100, "delta": 0.57}]


def test_record_option_native_selection_skipped_side():
    store.record_option_native_selection("C1", "B1", "2026-09-22", [
        {"symbol": "TCS", "option_type": "PE", "expiry": "2026-09-25",
         "target_delta": -0.55, "candidates_in_band": "[]", "price_change_pct": 2.3,
         "oi_spurt_pct": 8.5, "skipped": True,
         "skip_reason": "no PE candidate with delta in [-0.65, -0.45]"},
    ])
    con = _sqlite3.connect(store._DB_PATH)
    con.row_factory = _sqlite3.Row
    row = dict(con.execute(
        "SELECT * FROM option_native_selection WHERE client_id=? AND binding_id=? AND option_type='PE'",
        ("C1", "B1")).fetchone())
    con.close()
    assert row["skipped"] == 1
    assert row["strike"] is None
    assert "no PE candidate" in row["skip_reason"]


def test_record_option_native_selection_tie_break_used_flag():
    store.record_option_native_selection("C1", "B1", "2026-09-22", [
        {"symbol": "TCS", "option_type": "CE", "expiry": "2026-09-25", "strike": 2110,
         "target_delta": 0.55, "tie_break_used": True, "candidates_in_band": "[]", "skipped": False},
    ])
    con = _sqlite3.connect(store._DB_PATH)
    row = con.execute(
        "SELECT tie_break_used FROM option_native_selection WHERE client_id=? AND binding_id=?",
        ("C1", "B1")).fetchone()
    con.close()
    assert row == (1,)


def test_record_option_native_selection_multiple_rows_per_call():
    store.record_option_native_selection("C1", "B1", "2026-09-22", [
        {"symbol": "TCS", "option_type": "CE", "expiry": "2026-09-25", "strike": 2100,
         "target_delta": 0.55, "candidates_in_band": "[]", "skipped": False},
        {"symbol": "TCS", "option_type": "PE", "expiry": "2026-09-25",
         "target_delta": -0.55, "candidates_in_band": "[]", "skipped": True,
         "skip_reason": "none in band"},
    ])
    con = _sqlite3.connect(store._DB_PATH)
    n = con.execute(
        "SELECT COUNT(*) FROM option_native_selection WHERE client_id=? AND binding_id=?",
        ("C1", "B1")).fetchone()[0]
    con.close()
    assert n == 2


