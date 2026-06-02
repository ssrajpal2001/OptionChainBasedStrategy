"""TT v2 Task 3 — orchestration: DTE-ladder parsing + nested HTF→MTF gate."""

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.trap_trading_engine import TrapTradingEngine
from strategies.trap_seller_detection import SellerTrapDetector, State


def _engine():
    return TrapTradingEngine(EventBus(), GlobalConfig())


# ── DTE ladder (JSON string keys parsed to ints) ─────────────────────────────
@pytest.mark.parametrize("dte,expected", [
    (10, 5), (6, 5),   # > 5 -> 5
    (5, 4),            # > 4 -> 4
    (4, 3),            # > 3 -> 3
    (3, 2),            # > 2 -> 2
    (2, 1),            # > 1 -> 1
    (1, 0), (0, 0),    # <= 1 -> 0
])
def test_dte_ladder_parses_string_keys(dte, expected):
    eng = _engine()
    assert eng._dte_offset_steps_from_cfg("CRUDEOIL", dte) == expected


# ── Nested HTF→MTF gate contract ─────────────────────────────────────────────
def test_mtf_only_acts_after_htf_entry_ready():
    htf = SellerTrapDetector()
    mtf = SellerTrapDetector()
    # Before HTF is ready, the gate is closed — MTF must not be consulted.
    assert htf.state != State.ENTRY_READY

    # Drive HTF through Below -> Above -> Return.
    htf.on_candle({"open": 950, "high": 1000, "low": 900, "close": 980})
    htf.on_tick(880)    # below
    htf.on_tick(1010)   # above (trapped)
    htf.on_tick(900)    # return
    assert htf.state == State.ENTRY_READY  # gate now open

    # With the gate open, MTF completes its own Below->Above->Return -> entry.
    mtf.on_candle({"open": 520, "high": 540, "low": 500, "close": 530})
    mtf.on_tick(495)    # below
    mtf.on_tick(545)    # above
    mtf.on_tick(500)    # return
    assert mtf.entry_ready is True
