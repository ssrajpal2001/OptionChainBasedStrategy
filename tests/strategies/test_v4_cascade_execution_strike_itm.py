"""V4CascadeBook._resolve_execution_strike -- 2026-07-21: flips from 1-OTM
to 1-ITM per user direction (an OTM strike's premium is theta/low-delta
dominated, contaminating any SL/target signal derived from it; an ITM
strike's premium is delta-dominated, a cleaner reflection of real price
action). CE: was ATM+step (OTM), now ATM-step (ITM). PE: was ATM-step
(OTM), now ATM+step (ITM)."""
from datetime import date

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    return book


def test_ce_execution_strike_is_itm_below_atm_nifty():
    book = _book("NIFTY")
    book._atm_open = 24216.05
    book._live_spot = 24216.05
    strike = book._resolve_execution_strike("CE")
    # ATM strike (rounded to step 50) is 24200; ITM for CE means BELOW atm.
    assert strike < 24200.0


def test_pe_execution_strike_is_itm_above_atm_nifty():
    book = _book("NIFTY")
    book._atm_open = 24216.05
    book._live_spot = 24216.05
    strike = book._resolve_execution_strike("PE")
    assert strike > 24200.0


def test_ce_execution_strike_is_itm_below_atm_crudeoil():
    book = _book("CRUDEOIL")
    book._atm_open = 7961.0
    book._live_spot = 7961.0
    strike = book._resolve_execution_strike("CE")
    assert strike < 8000.0


def test_pe_execution_strike_is_itm_above_atm_crudeoil():
    book = _book("CRUDEOIL")
    book._atm_open = 7961.0
    book._live_spot = 7961.0
    strike = book._resolve_execution_strike("PE")
    assert strike > 8000.0
