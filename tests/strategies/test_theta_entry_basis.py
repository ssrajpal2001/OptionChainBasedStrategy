"""ENTRY threshold basis = ltp | theta. basis='ltp' must be byte-identical to the legacy path;
basis='theta' floors each leg on TIME VALUE (intrinsic-stripped) instead of raw LTP."""
from strategies.straddle_selection import (
    select_balanced_pair, scan_pool, leg_entry_value,
)


def test_leg_entry_value_ltp_vs_theta():
    # ITM CE: strike 100 below spot 120 → intrinsic 20. LTP 35 → time value 15.
    assert leg_entry_value("CE", 100, 35.0, 120.0, "ltp") == 35.0
    assert leg_entry_value("CE", 100, 35.0, 120.0, "theta") == 15.0
    # OTM PE: strike 100 below spot 120 → intrinsic 0 → time value == ltp.
    assert leg_entry_value("PE", 100, 12.0, 120.0, "theta") == 12.0


def _pool(spot, step=50):
    # ATM at spot; both ATM legs present, plus a partner band.
    atm = int(round(spot / step) * step)
    return {
        (atm, "CE"): {"ltp": 100.0, "atp": 100.0},
        (atm, "PE"): {"ltp": 90.0,  "atp": 90.0},
        (atm - step, "PE"): {"ltp": 95.0, "atp": 95.0},
        (atm + step, "CE"): {"ltp": 85.0, "atp": 85.0},
    }


def test_ltp_basis_matches_legacy_default():
    sp = _pool(20000)
    a = select_balanced_pair(sp, 20000, 50, 4, 50.0)
    b = select_balanced_pair(sp, 20000, 50, 4, 50.0, entry_basis="ltp", theta_target=0.0)
    assert a == b


def test_theta_basis_rejects_when_time_value_below_target():
    # At ATM the legs are pure time value (intrinsic 0), so a high theta_target floors them out.
    sp = _pool(20000)
    assert select_balanced_pair(sp, 20000, 50, 4, 0.0,
                                entry_basis="theta", theta_target=200.0) is None
    # A reachable theta target still selects.
    assert select_balanced_pair(sp, 20000, 50, 4, 0.0,
                                entry_basis="theta", theta_target=50.0) is not None


def test_scan_pool_theta_floor():
    sp = _pool(20000)
    # ltp_target ignored under theta basis; theta_target too high → no pair.
    assert scan_pool(sp, 20000, 50, 4, 0.0, rule_pass=lambda c, p: True,
                     entry_basis="theta", theta_target=500.0) is None
