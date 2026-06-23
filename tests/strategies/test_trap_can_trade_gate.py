"""TrapScannerEngine._can_trade() — terminal + trade gating per binding.

Reproduces the live bug: a trade fired while the broker terminal AND the Trade
toggle were OFF. The gate must block firing unless BOTH terminal_connected=1
AND is_trade_enabled=1 for THIS book's (client, binding).
"""
import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.trap_scanner_engine import TrapScannerEngine


class _FakeDB:
    """Minimal stand-in exposing get_bindings_safe_sync for one binding."""
    def __init__(self, terminal, trade):
        self._terminal = terminal
        self._trade = trade

    def get_bindings_safe_sync(self, client_id):
        return [{
            "binding_id": "b1",
            "terminal_connected": 1 if self._terminal else 0,
            "is_trade_enabled": 1 if self._trade else 0,
        }]


def _engine(db):
    eng = TrapScannerEngine(
        bus=EventBus(), cfg=GlobalConfig(), underlying="NIFTY",
        lot_multiplier=2, client_id="c1", binding_id="b1",
        ts_admin_cfg={}, client_db=db, expiry_mode="current",
    )
    return eng


def test_blocks_when_terminal_and_trade_off():
    eng = _engine(_FakeDB(terminal=False, trade=False))
    assert eng._can_trade() is False


def test_blocks_when_terminal_off_trade_on():
    eng = _engine(_FakeDB(terminal=False, trade=True))
    assert eng._can_trade() is False


def test_blocks_when_terminal_on_trade_off():
    eng = _engine(_FakeDB(terminal=True, trade=False))
    assert eng._can_trade() is False


def test_allows_when_terminal_and_trade_on():
    eng = _engine(_FakeDB(terminal=True, trade=True))
    assert eng._can_trade() is True


def test_fail_open_without_db():
    eng = _engine(None)
    assert eng._can_trade() is True


def test_caches_within_5s():
    db = _FakeDB(terminal=True, trade=True)
    eng = _engine(db)
    assert eng._can_trade() is True
    # Flip both off; cached result (<5s) should still report True until TTL passes.
    db._terminal = False
    db._trade = False
    assert eng._can_trade() is True
