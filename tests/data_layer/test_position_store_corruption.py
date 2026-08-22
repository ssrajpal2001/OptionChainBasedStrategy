"""
tests/data_layer/test_position_store_corruption.py -- regression for the
2026-08-23 fix escalating position_store.load()'s corrupt-file log to
CRITICAL.

save() already writes atomically (tmp file + os.replace), which protects
against the most common corruption path (a crash mid-write). But if an
existing persistence file DOES fail to parse for any other reason (disk
corruption, manual tampering, a bug elsewhere overwriting it), the caller
just sees None and proceeds as if flat -- if a real broker position is
still open, nothing else in this codebase cross-checks that belief
against the broker's own position book. A WARNING-level log for exactly
this failure mode was easy to miss in a busy log stream.
"""
import logging

from data_layer import position_store


def test_load_missing_file_is_quiet(tmp_path, monkeypatch, caplog):
    """A file that genuinely never existed (normal "no position" case)
    must NOT be treated as corruption -- no CRITICAL log."""
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    with caplog.at_level(logging.CRITICAL):
        result = position_store.load("never_existed_key")
    assert result is None
    assert not any(r.levelno >= logging.CRITICAL for r in caplog.records)


def test_load_corrupt_file_logs_critical(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    import os
    os.makedirs(str(tmp_path), exist_ok=True)
    corrupt_path = os.path.join(str(tmp_path), "NIFTY_sell_straddle.json")
    with open(corrupt_path, "w") as f:
        f.write("{not valid json at all")

    with caplog.at_level(logging.CRITICAL):
        result = position_store.load("NIFTY_sell_straddle")

    assert result is None
    critical_records = [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert len(critical_records) == 1
    assert "FAILED TO PARSE" in critical_records[0].message


def test_load_valid_file_still_works_normally(tmp_path, monkeypatch):
    """The fix must not change behavior for the normal, successful path."""
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    position_store.save("NIFTY_oi_flow", {"side": "CE", "strike": 24500}, product_type="MIS")
    result = position_store.load("NIFTY_oi_flow")
    assert result == {"side": "CE", "strike": 24500}
