"""
2026-08-12: unit tests for strategies/oi_flow/telemetry.py -- the
structured JSONL signal-evaluation log that substitutes for backtest
evidence (this strategy cannot be backtested against history, see
strategies/oi_flow/__init__.py).
"""
import json
import os
import tempfile
from datetime import datetime

from config.global_config import IST
from strategies.oi_flow.telemetry import SignalTelemetryRow, log_signal_evaluation, new_row


def test_new_row_has_required_fields_and_sane_defaults():
    row = new_row("BANKNIFTY", "CE", now=datetime(2026, 8, 12, 9, 20, tzinfo=IST))
    assert row.underlying == "BANKNIFTY"
    assert row.side == "CE"
    assert row.entered is False
    assert row.skip_reason == ""
    assert row.spot_gate_fired is False
    assert row.ts.startswith("2026-08-12T09:20:00")


def test_log_signal_evaluation_writes_one_json_line_per_call():
    with tempfile.TemporaryDirectory() as tmp:
        row1 = new_row("BANKNIFTY", "CE", now=datetime(2026, 8, 12, 9, 20, tzinfo=IST))
        row1.spot = 57690.0
        row1.wall_strike = 57700.0
        row1.skip_reason = "spot_gate_no_signal"
        row2 = new_row("BANKNIFTY", "PE", now=datetime(2026, 8, 12, 9, 21, tzinfo=IST))
        row2.entered = True

        log_signal_evaluation(row1, log_dir=tmp)
        log_signal_evaluation(row2, log_dir=tmp)

        files = os.listdir(tmp)
        assert len(files) == 1   # both rows are the same underlying+day -> same file
        path = os.path.join(tmp, files[0])
        with open(path) as f:
            lines = [json.loads(line) for line in f if line.strip()]
        assert len(lines) == 2
        assert lines[0]["side"] == "CE"
        assert lines[0]["skip_reason"] == "spot_gate_no_signal"
        assert lines[1]["side"] == "PE"
        assert lines[1]["entered"] is True


def test_log_signal_evaluation_never_raises_on_write_failure():
    # An unwritable/invalid directory (a file path used as a dir) must be
    # swallowed, not propagated -- telemetry failures must never interrupt
    # live trading logic.
    with tempfile.NamedTemporaryFile() as tmp_file:
        row = new_row("BANKNIFTY", "CE")
        log_signal_evaluation(row, log_dir=tmp_file.name)   # tmp_file.name is a FILE, not a dir
        # No exception raised -- that's the whole assertion.
