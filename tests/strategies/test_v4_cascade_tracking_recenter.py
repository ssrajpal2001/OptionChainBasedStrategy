"""V4CascadeBook._maybe_recenter_tracking_strikes -- 2026-07-21: re-centers
the tracking/scanner strikes when the underlying has drifted far enough from
the ATM the CURRENT tracking strikes were derived from, but ONLY while flat
(no open position) -- carrying an open position's zone/SL/target state
across a strike change has no valid conversion between two different
instruments' unrelated price scales, so re-centering never happens mid-trade."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24216.05
    book._ce_strike = 24000
    book._pe_strike = 24400
    return book


def test_recenters_when_flat_and_drift_exceeds_threshold_nifty():
    book = _book("NIFTY")
    book._engine.position = None
    assert book._v4cfg.tracking_recenter_pts == 100.0

    book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    assert book._tracking_reference_atm == 24320.0
    assert book._ce_strike != 24000 or book._pe_strike != 24400


def test_does_not_recenter_when_drift_under_threshold():
    book = _book("NIFTY")
    book._engine.position = None

    book._maybe_recenter_tracking_strikes(current_atm=24250.0)   # drift = 33.95 < 100

    assert book._tracking_reference_atm == 24216.05
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


def test_does_not_recenter_while_position_open_regardless_of_drift():
    book = _book("NIFTY")
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST),
    )

    book._maybe_recenter_tracking_strikes(current_atm=25000.0)   # huge drift, but position open

    assert book._tracking_reference_atm == 24216.05   # unchanged
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


def test_crudeoil_uses_200_point_threshold():
    book = _book("CRUDEOIL")
    assert book._v4cfg.tracking_recenter_pts == 200.0
