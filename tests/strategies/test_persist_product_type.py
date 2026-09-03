"""2026-08-06 CRITICAL FIX regression test. _persist() used to always tag the
on-disk position "MIS" (self._product_type never existed on this class, so
getattr(..., "MIS") silently always fell back to the default) regardless of
the underlying's REAL configured product type. Any NRML (carry-forward)
deployment would have its genuinely-still-open overnight position wrongly
discarded on the next restart -- position_store.py's MIS new-day-discard
rule fires on the STORED tag, not the real product type.
"""
import datetime

import data_layer.position_store as ps
from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus
from data_layer.runtime_config import RuntimeConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _position():
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, 90.0, open_time=datetime.datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24500, 100.0, 95.0, open_time=datetime.datetime.now(IST)),
        net_credit=200.0, status="open",
    )


def test_persist_tags_the_real_configured_product_type_nrml(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    monkeypatch.setattr(RuntimeConfig, "index_section",
                        staticmethod(lambda underlying, strategy: {"product_type": "NRML"}))

    s = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    s._position = _position()
    s._persist()

    stored = ps.load(s._persist_key)
    assert stored is not None
    # position_store.load() strips product_type off the returned dict (it's a
    # store-level field, not part of the position payload) -- read the raw
    # file to confirm what was actually written.
    import json
    raw = json.load(open(ps._path(s._persist_key)))
    assert raw["product_type"] == "NRML", (
        f"persisted product_type={raw['product_type']!r}, expected NRML to match "
        f"the real configured deployment -- an overnight NRML position would be "
        f"wrongly discarded as 'yesterday's already-squared-off MIS position'."
    )


def test_persist_defaults_to_mis_when_unconfigured(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))
    monkeypatch.setattr(RuntimeConfig, "index_section",
                        staticmethod(lambda underlying, strategy: {}))

    s = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    s._position = _position()
    s._persist()

    import json
    raw = json.load(open(ps._path(s._persist_key)))
    assert raw["product_type"] == "MIS"


def test_persist_retries_once_then_logs_critical_on_repeated_failure(tmp_path, monkeypatch, caplog):
    """2026-08-06 CRITICAL FIX: save()/clear() used to always silently swallow
    a write failure with no signal the caller could act on. _persist() must
    now retry once, and if that also fails, log a loud CRITICAL alert rather
    than continuing as if nothing happened."""
    import logging
    monkeypatch.setattr(ps, "_DIR", str(tmp_path))

    s = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    s._position = _position()

    call_count = {"n": 0}

    def _always_fail(*a, **k):
        call_count["n"] += 1
        return False

    monkeypatch.setattr(s, "persist", _always_fail)

    with caplog.at_level(logging.CRITICAL):
        s._persist()

    assert call_count["n"] == 2, "expected exactly one retry after the first failure"
    assert any("PERSIST FAILED TWICE" in r.message for r in caplog.records), (
        "no CRITICAL alert was logged after two consecutive persist failures"
    )
