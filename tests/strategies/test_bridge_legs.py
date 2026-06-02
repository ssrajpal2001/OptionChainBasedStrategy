from execution_bridge.straddle_bridge import StraddleOrderEvent


def test_order_event_defaults_both_legs():
    ev = StraddleOrderEvent(
        action="ENTRY", underlying="NIFTY", atm=100, ce_strike=100, pe_strike=100,
        ce_ltp=10.0, pe_ltp=10.0,
    )
    assert ev.legs == ["CE", "PE"]


def test_order_event_single_leg():
    ev = StraddleOrderEvent(
        action="EXIT", underlying="NIFTY", atm=100, ce_strike=100, pe_strike=100,
        ce_ltp=10.0, pe_ltp=10.0, legs=["CE"],
    )
    assert ev.legs == ["CE"]
