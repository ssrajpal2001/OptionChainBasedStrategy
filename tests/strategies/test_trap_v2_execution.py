"""TT v2 Task 4 — execution: fresh ATM±buy_depth strike from live spot."""

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.trap_trading_engine import exec_strike, TrapTradingEngine


# ── Pure strike resolution ───────────────────────────────────────────────────
def test_exec_strike_atm_when_depth_zero():
    # round(8765/100)*100 = 8800 (ATM)
    assert exec_strike(8765, "CE", buy_depth=0, step=100) == 8800
    assert exec_strike(8765, "PE", buy_depth=0, step=100) == 8800


def test_exec_strike_ce_itm_below_spot():
    # CE ITM is below the ATM
    assert exec_strike(8765, "CE", buy_depth=1, step=100) == 8700
    assert exec_strike(8765, "CE", buy_depth=2, step=100) == 8600


def test_exec_strike_pe_itm_above_spot():
    # PE ITM is above the ATM
    assert exec_strike(8765, "PE", buy_depth=1, step=100) == 8900
    assert exec_strike(8765, "PE", buy_depth=2, step=100) == 9000


def test_exec_strike_nifty_step50():
    # round(24512/50)*50 = 24500 ATM; CE 1 step ITM = 24450
    assert exec_strike(24512, "CE", buy_depth=1, step=50) == 24450


# ── Engine entry payload (uses live spot + per-instrument settings) ───────────
def _engine():
    return TrapTradingEngine(EventBus(), GlobalConfig())


def test_build_entry_payload_atm_buy_crudeoil():
    eng = _engine()
    eng._spot_cache["CRUDEOIL"] = 8765.0   # live future
    p = eng._build_entry_payload("CRUDEOIL", "CE")
    assert p["side"] == "BUY"
    assert p["opt_type"] == "CE"
    assert p["strike"] == 8800             # ATM (buy_depth default 0)
    assert p["qty"] == 100                 # CRUDEOIL lot size
    assert p["spot"] == 8765.0


def test_build_entry_payload_pe_side():
    eng = _engine()
    eng._spot_cache["CRUDEOIL"] = 8765.0
    p = eng._build_entry_payload("CRUDEOIL", "PE")
    assert p["opt_type"] == "PE" and p["strike"] == 8800 and p["side"] == "BUY"


def test_build_entry_payload_no_spot_returns_none():
    eng = _engine()
    assert eng._build_entry_payload("CRUDEOIL", "CE") is None
