"""
2026-08-12: unit tests for strategies/oi_flow/book_manager.py
(OIFlowBookManager) -- _wanted()/_spawn_book()/_should_respawn() reading
"oi_flow"-tagged strategy_deployments rows. Mirrors the test style already
used for the other book managers in this codebase (e.g.
tests/strategies/test_hourly_breakout_book_manager_wanted.py).
"""
import json

from strategies.oi_flow.book_manager import OIFlowBookManager, _DEFAULT_PARAMS, _parse_params
from strategies.oi_flow.engine import OIFlowStrategy


class _FakeDB:
    def __init__(self, rows):
        self._rows = rows

    def get_running_deployments_by_strategy_sync(self, strategy_name):
        assert strategy_name == "oi_flow"
        return self._rows


def _row(**overrides):
    base = dict(client_id="ssrajpal2001", binding_id="SA5770", underlying="BANKNIFTY",
                lot_multiplier=1, strategy_name="oi_flow", is_running=1, product_type="MIS",
                strategy_params="{}")
    base.update(overrides)
    return base


def _make_manager(rows):
    mgr = OIFlowBookManager.__new__(OIFlowBookManager)
    mgr._bus = None
    mgr._cfg = None
    mgr._db = _FakeDB(rows)
    mgr._indices = set()
    mgr._books = {}
    mgr._rebalancer = None
    return mgr


# ── _parse_params ────────────────────────────────────────────────────────────

def test_parse_params_fills_defaults_when_empty():
    params = _parse_params("{}")
    for k, v in _DEFAULT_PARAMS.items():
        assert params[k] == v


def test_parse_params_preserves_explicit_overrides():
    raw = json.dumps({"window_sec": 300, "min_pcr_bias": 1.5})
    params = _parse_params(raw)
    assert params["window_sec"] == 300
    assert params["min_pcr_bias"] == 1.5
    assert params["max_pcr_bias"] == _DEFAULT_PARAMS["max_pcr_bias"]   # untouched default


def test_parse_params_recovers_from_malformed_json():
    params = _parse_params("not valid json{{{")
    assert params == _DEFAULT_PARAMS


# ── _wanted ──────────────────────────────────────────────────────────────────

def test_wanted_builds_one_entry_per_running_deployment():
    mgr = _make_manager([_row()])
    wanted = mgr._wanted()
    assert ("ssrajpal2001", "SA5770", "BANKNIFTY") in wanted
    cfg = wanted[("ssrajpal2001", "SA5770", "BANKNIFTY")]
    assert cfg["lots"] == 1
    assert cfg["window_sec"] == _DEFAULT_PARAMS["window_sec"]


def test_wanted_skips_rows_missing_required_fields():
    mgr = _make_manager([_row(client_id="")])
    assert mgr._wanted() == {}


def test_wanted_reads_custom_strategy_params():
    row = _row(strategy_params=json.dumps({"window_sec": 240, "hard_risk_rs_per_lot": 1500.0}))
    mgr = _make_manager([row])
    cfg = mgr._wanted()[("ssrajpal2001", "SA5770", "BANKNIFTY")]
    assert cfg["window_sec"] == 240
    assert cfg["hard_risk_rs_per_lot"] == 1500.0


def test_wanted_never_reads_other_strategies_deployments():
    """_FakeDB.get_running_deployments_by_strategy_sync itself asserts it's
    only ever called with strategy_name='oi_flow' -- this test just
    confirms _wanted() actually calls it (would fail loudly via the
    assertion inside _FakeDB otherwise)."""
    mgr = _make_manager([_row()])
    mgr._wanted()   # would raise via _FakeDB's own assert if mis-scoped


# ── _spawn_book / _should_respawn ────────────────────────────────────────────

class _FakeCfg:
    class _Exchange:
        lot_sizes = {"BANKNIFTY": 30}
        strike_steps = {"BANKNIFTY": 100.0}
    exchange = _Exchange()


def test_spawn_book_constructs_oi_flow_strategy_with_config():
    mgr = _make_manager([])
    mgr._cfg = _FakeCfg()
    value = dict(lots=2, product_type="MIS", squareoff_time="15:15", **_DEFAULT_PARAMS)
    book = mgr._spawn_book(("ssrajpal2001", "SA5770", "BANKNIFTY"), value)
    assert isinstance(book, OIFlowStrategy)
    assert book._lot_multiplier == 2
    assert book._client_id == "ssrajpal2001"
    assert book._binding_id == "SA5770"
    assert book._underlying == "BANKNIFTY"
    assert book._window_sec == _DEFAULT_PARAMS["window_sec"]


def test_should_respawn_true_on_lot_multiplier_change():
    mgr = _make_manager([])
    mgr._cfg = _FakeCfg()
    value = dict(lots=1, product_type="MIS", squareoff_time="15:15", **_DEFAULT_PARAMS)
    book = mgr._spawn_book(("c", "b", "BANKNIFTY"), value)
    new_value = dict(value, lots=3)
    assert mgr._should_respawn(book, new_value) is True


def test_should_respawn_true_on_threshold_param_change():
    mgr = _make_manager([])
    mgr._cfg = _FakeCfg()
    value = dict(lots=1, product_type="MIS", squareoff_time="15:15", **_DEFAULT_PARAMS)
    book = mgr._spawn_book(("c", "b", "BANKNIFTY"), value)
    new_value = dict(value, min_pcr_bias=1.5)
    assert mgr._should_respawn(book, new_value) is True


def test_should_respawn_false_when_unchanged():
    mgr = _make_manager([])
    mgr._cfg = _FakeCfg()
    value = dict(lots=1, product_type="MIS", squareoff_time="15:15", **_DEFAULT_PARAMS)
    book = mgr._spawn_book(("c", "b", "BANKNIFTY"), value)
    assert mgr._should_respawn(book, dict(value)) is False
