from strategies.theta_calc import (
    intrinsic_value, time_value, combined_time_value, theta_decay_pct,
)


def test_intrinsic_value_ce_and_pe():
    # CE ITM by 50, PE OTM
    assert intrinsic_value("CE", 23500, 23550) == 50
    assert intrinsic_value("PE", 23500, 23550) == 0
    # PE ITM by 50, CE OTM
    assert intrinsic_value("PE", 23500, 23450) == 50
    assert intrinsic_value("CE", 23500, 23450) == 0


def test_time_value_is_premium_minus_intrinsic_abs():
    # CE ATM: intrinsic 0 → time value = full premium
    assert time_value("CE", 23500, 23500, 120.0) == 120.0
    # CE ITM by 50, premium 180 → time value 130
    assert time_value("CE", 23500, 23550, 180.0) == 130.0
    # sub-intrinsic quote → abs keeps it non-negative
    assert time_value("CE", 23500, 23600, 80.0) == 20.0


def test_combined_time_value_sums_both_legs():
    # ATM straddle, both intrinsic 0 → combined = ce_prem + pe_prem
    assert combined_time_value(23500, 23500, 23500, 120.0, 110.0) == 230.0


def test_theta_decay_pct_positive_when_time_value_shrinks():
    # entry tv 200, current 150 → 25% decayed (profit for a short straddle)
    assert theta_decay_pct(200.0, 150.0) == 25.0
    # expanded against us → negative
    assert theta_decay_pct(200.0, 240.0) == -20.0
    # guard: zero entry tv → 0
    assert theta_decay_pct(0.0, 100.0) == 0.0
