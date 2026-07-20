"""strategies/v4_cascade/book.py's _apply_eod_gate23_rules -- 2026-07-20
Index/Premium decoupling: non-crypto (NIFTY/CRUDEOIL, IndexGatedPremiumScanner)
now discards EVERY in-flight premium setup at the daily EOD/gate23 boundary
(there's no outer "HTF" zone on the Index chart to roll back to -- Gate 1
lives on spot_confirm.py, which is never day-scoped). Crypto (legacy
PremiumGateScanner) keeps its exact prior behavior: roll back to HTF_LOCKED,
discarding only the finer MTF/limit progress, never the setup itself."""
from datetime import datetime
from zoneinfo import ZoneInfo

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import GateState, PremiumZoneState, RollingBaseZone
from strategies.v4_cascade.zone_state import _HTFSetup, _PremiumSetup

IST = ZoneInfo("Asia/Kolkata")


def _book(underlying, squareoff_time="15:15"):
    cfg = GlobalConfig()
    return V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=2, squareoff_time=squareoff_time,
    )


def _zone(entry_line=100.0, sweep_low=95.0, sl_level=110.0):
    return RollingBaseZone(
        reference_low=entry_line, reference_low_ts=datetime(2026, 7, 20, 10, 0, tzinfo=IST),
        entry_line=entry_line, sweep_low=sweep_low, sl_level=sl_level,
        locked=True, lock_ts=datetime(2026, 7, 20, 10, 30, tzinfo=IST),
    )


def test_non_crypto_clears_all_in_flight_setups():
    book = _book("NIFTY")
    scanner = book._engine._scanners["CE"]
    setup = _PremiumSetup(_zone(), timeframe=5)
    setup.state = PremiumZoneState.LIMIT_ARMED
    setup.limit_entry_price = 98.33
    scanner.setups.append(setup)
    assert len(scanner.setups) == 1

    book._apply_eod_gate23_rules(datetime(2026, 7, 20, 15, 30, tzinfo=IST))

    assert scanner.setups == []


def test_crypto_rolls_back_to_htf_locked_setup_preserved():
    book = _book("BTC", squareoff_time="16:30")
    scanner = book._engine._scanners["CE"]
    htf_zone = _zone(entry_line=100.0, sweep_low=95.0, sl_level=110.0)
    setup = _HTFSetup(htf_zone)
    setup.mtf_zone = _zone(entry_line=99.0, sweep_low=96.0, sl_level=101.0)
    setup.mtf_timeframe = 5
    setup.state = GateState.LIMIT_ARMED
    setup.limit_entry_price = 97.5
    scanner.setups.append(setup)
    assert len(scanner.setups) == 1

    book._apply_eod_gate23_rules(datetime(2026, 7, 20, 16, 45, tzinfo=IST))

    # Setup survives (never removed for crypto) but rolled back.
    assert len(scanner.setups) == 1
    rolled = scanner.setups[0]
    assert rolled is setup
    assert rolled.state == GateState.HTF_LOCKED
    assert rolled.mtf_zone is None
    assert rolled.mtf_timeframe is None
    assert rolled.limit_entry_price is None
    # The HTF zone itself (Gate 1) is left completely untouched.
    assert rolled.htf_zone is htf_zone
    assert rolled.htf_zone.entry_line == 100.0


def test_crypto_setup_already_htf_locked_is_left_untouched():
    book = _book("ETH", squareoff_time="16:30")
    scanner = book._engine._scanners["CE"]
    htf_zone = _zone()
    setup = _HTFSetup(htf_zone)
    assert setup.state == GateState.HTF_LOCKED
    scanner.setups.append(setup)

    book._apply_eod_gate23_rules(datetime(2026, 7, 20, 16, 45, tzinfo=IST))

    assert len(scanner.setups) == 1
    assert scanner.setups[0] is setup
    assert scanner.setups[0].state == GateState.HTF_LOCKED
