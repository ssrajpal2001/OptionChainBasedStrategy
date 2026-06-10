"""Theta% = decay tracked against TOTAL THETA RECEIVED at entry (user spec 2026-06-10).
= (entry premium − current premium) / entry_time_value × 100 ; thresholds = entry_theta × day%."""
from strategies.sell_straddle import StraddlePosition, StraddleLeg


def _pos(ce_entry, pe_entry, ce_ltp, pe_ltp, entry_theta=None):
    p = StraddlePosition(
        underlying="NIFTY", atm_at_entry=23400, entry_spot=23400,
        ce_leg=StraddleLeg("CE", 23450, ce_entry, ce_entry),
        pe_leg=StraddleLeg("PE", 23400, pe_entry, pe_entry),
        net_credit=ce_entry + pe_entry,
    )
    # ATM straddle: total theta received == entry premium
    p.entry_time_value = entry_theta if entry_theta is not None else (ce_entry + pe_entry)
    p.ce_leg.ltp = ce_ltp
    p.pe_leg.ltp = pe_ltp
    return p


def test_decay_profit_side():
    # entry 167.40+172.40 = 339.80; current 150+140 = 290 → decayed 49.8 → 14.66%
    p = _pos(167.40, 172.40, 150.0, 140.0)
    assert round(p.premium_decay_pct(), 2) == round((339.80 - 290.0) / 339.80 * 100, 2)
    assert p.premium_decay_pct() > 0


def test_decay_loss_side():
    p = _pos(160.0, 160.0, 200.0, 200.0)   # 320 → 400, rose 80, theta=320 → -25%
    assert round(p.premium_decay_pct(), 2) == -25.0


def test_target_is_pct_of_total_theta_received():
    # 12% target on 339.80 theta → fires when decay >= 40.776 pts
    p = _pos(167.40, 172.40, 150.0, 339.80 - 40.78 - 150.0)
    assert round(p.premium_decay_pct(), 1) == 12.0


def test_denominator_is_entry_theta_not_premium():
    # entry_theta differs from premium (e.g. some intrinsic at entry) → uses theta as base
    p = _pos(160.0, 160.0, 150.0, 150.0, entry_theta=200.0)   # decay 320-300=20 over theta 200 = 10%
    assert round(p.premium_decay_pct(), 2) == 10.0


def test_zero_theta_safe():
    p = _pos(0.0, 0.0, 0.0, 0.0, entry_theta=0.0)
    assert p.premium_decay_pct() == 0.0
