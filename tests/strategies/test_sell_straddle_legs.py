from strategies.sell_straddle import StraddlePosition, StraddleLeg


def _route_tick(pos, strike, side, ltp):
    """Mirror the strategy's open-position LTP update rule (per-leg strike)."""
    if side == "CE" and int(strike) == int(pos.ce_leg.strike):
        pos.ce_leg.ltp = ltp
    elif side == "PE" and int(strike) == int(pos.pe_leg.strike):
        pos.pe_leg.ltp = ltp


def test_asymmetric_legs_route_to_correct_leg():
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=100, entry_spot=100,
        ce_leg=StraddleLeg("CE", 100, 60.0, 60.0),
        pe_leg=StraddleLeg("PE", 105, 55.0, 55.0),
        net_credit=115.0,
    )
    _route_tick(pos, 100, "CE", 58.0)   # CE leg strike
    _route_tick(pos, 105, "PE", 50.0)   # PE leg strike
    _route_tick(pos, 105, "CE", 999.0)  # wrong strike for CE → ignored
    assert pos.ce_leg.ltp == 58.0
    assert pos.pe_leg.ltp == 50.0
    assert pos.current_value == 108.0
    assert pos.unrealized_pnl == 115.0 - 108.0
