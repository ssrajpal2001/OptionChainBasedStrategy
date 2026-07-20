"""PremiumGateScanner's Gate-2 15m fallback resample must respect a
configurable session-open time, same as Gate-1's 75m scan."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from strategies.v4_cascade.zone_state import PremiumGateScanner
from strategies.v4_cascade.engine import V4CascadeEngine

IST = ZoneInfo("Asia/Kolkata")


def test_scanner_default_session_open():
    s = PremiumGateScanner(bear=True)
    assert s._session_open == (9, 15)


def test_scanner_custom_session_open():
    s = PremiumGateScanner(bear=True, session_open=(9, 0))
    assert s._session_open == (9, 0)


def test_engine_forwards_session_open_to_both_scanners():
    eng = V4CascadeEngine(session_open=(9, 0))
    assert eng._scanners["CE"]._session_open == (9, 0)
    assert eng._scanners["PE"]._session_open == (9, 0)


def test_engine_default_session_open_matches_nifty():
    eng = V4CascadeEngine()
    assert eng._scanners["CE"]._session_open == (9, 15)
