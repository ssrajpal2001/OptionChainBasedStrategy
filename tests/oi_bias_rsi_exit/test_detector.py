"""
tests/oi_bias_rsi_exit/test_detector.py -- locks in the entry/exit mechanic
for the new "OI-spurt selection + StochRSI(14,14,3,3) K/D" strategy against
the real pure functions in strategies/oi_bias_rsi_exit/detector.py.
"""
from strategies.oi_bias_rsi_exit import detector as d


# ── StochRSI(14,14,3,3), double-smoothed ────────────────────────────────────

def test_compute_stoch_rsi_double_smoothed_is_none_during_warmup():
    closes = [100.0 + i * 0.1 for i in range(10)]  # far short of warm-up
    k, dline = d.compute_stoch_rsi_double_smoothed(closes)
    assert all(v is None for v in k)
    assert all(v is None for v in dline)


def test_compute_stoch_rsi_double_smoothed_produces_values_once_warm():
    # Monotonically rising closes -> RSI pins at 100 once warm, so raw stoch
    # is also pinned at 100 (hi==lo branch) -- both k and d converge to 100.0.
    closes = [100.0 + i for i in range(40)]
    k, dline = d.compute_stoch_rsi_double_smoothed(closes)
    assert k[-1] == 100.0
    assert dline[-1] == 100.0
    # d needs strictly more warm-up bars than k (an extra smoothing pass).
    first_k_idx = next(i for i, v in enumerate(k) if v is not None)
    first_d_idx = next(i for i, v in enumerate(dline) if v is not None)
    assert first_d_idx > first_k_idx


# ── Entry: state check, MIRRORED by bias (2026-09-26 direct user correction:
# the indicator now runs on the STOCK's own price, not the always-long
# option premium, so a bearish trade -- profiting when the stock FALLS --
# needs the stock's own bearish-momentum reading, D>K, not K>D) ────────────

def test_check_entry_state_bullish_true_when_k_above_d():
    assert d.check_entry_state(k=60.0, d=40.0, bias="bullish") is True


def test_check_entry_state_bullish_false_when_k_below_or_equal_d():
    assert d.check_entry_state(k=40.0, d=60.0, bias="bullish") is False
    assert d.check_entry_state(k=50.0, d=50.0, bias="bullish") is False


def test_check_entry_state_bearish_true_when_d_above_k():
    assert d.check_entry_state(k=40.0, d=60.0, bias="bearish") is True


def test_check_entry_state_bearish_false_when_d_below_or_equal_k():
    assert d.check_entry_state(k=60.0, d=40.0, bias="bearish") is False
    assert d.check_entry_state(k=50.0, d=50.0, bias="bearish") is False


def test_check_entry_state_false_when_either_side_not_warm_yet():
    assert d.check_entry_state(k=None, d=40.0, bias="bullish") is False
    assert d.check_entry_state(k=60.0, d=None, bias="bullish") is False
    assert d.check_entry_state(k=None, d=40.0, bias="bearish") is False
    assert d.check_entry_state(k=60.0, d=None, bias="bearish") is False


# ── Exit: a genuine crossover EVENT, MIRRORED by bias -- bullish exits on
# D-crosses-above-K (stock momentum turning bearish); bearish exits on the
# opposite, K-crosses-above-D (stock momentum turning bullish) ─────────────

def test_check_exit_cross_bullish_fires_on_the_bar_d_crosses_above_k():
    # prev bar: K still >= D (no cross yet). current bar: D > K -- crossed.
    assert d.check_exit_cross(prev_k=55.0, prev_d=50.0, k=48.0, d=52.0, bias="bullish") is True


def test_check_exit_cross_bullish_does_not_fire_when_d_was_already_above_k():
    # D>K on BOTH bars -- this is a persisting state, not a fresh cross.
    assert d.check_exit_cross(prev_k=45.0, prev_d=50.0, k=40.0, d=55.0, bias="bullish") is False


def test_check_exit_cross_bullish_does_not_fire_when_k_stays_above_d():
    assert d.check_exit_cross(prev_k=60.0, prev_d=40.0, k=58.0, d=42.0, bias="bullish") is False


def test_check_exit_cross_bearish_fires_on_the_bar_k_crosses_above_d():
    # Mirror of the bullish case: prev bar D>=K (no cross yet), current bar
    # K>D -- crossed. This is the SAME raw numbers as the bullish "does not
    # fire" test above, proving bias genuinely flips which event counts.
    assert d.check_exit_cross(prev_k=45.0, prev_d=50.0, k=56.0, d=50.0, bias="bearish") is True


def test_check_exit_cross_bearish_does_not_fire_when_k_was_already_above_d():
    assert d.check_exit_cross(prev_k=55.0, prev_d=50.0, k=58.0, d=45.0, bias="bearish") is False


def test_check_exit_cross_bearish_does_not_fire_when_d_stays_above_k():
    assert d.check_exit_cross(prev_k=40.0, prev_d=60.0, k=42.0, d=58.0, bias="bearish") is False


def test_check_exit_cross_false_when_any_value_not_warm_yet():
    assert d.check_exit_cross(prev_k=None, prev_d=50.0, k=48.0, d=52.0, bias="bullish") is False
    assert d.check_exit_cross(prev_k=55.0, prev_d=None, k=48.0, d=52.0, bias="bullish") is False
    assert d.check_exit_cross(prev_k=55.0, prev_d=50.0, k=None, d=52.0, bias="bullish") is False
    assert d.check_exit_cross(prev_k=55.0, prev_d=50.0, k=48.0, d=None, bias="bullish") is False


# ── Exit: bias flips to the opposite of the entry direction, twice ─────────

def test_count_opposite_bias_readings_counts_only_the_opposite_of_entry_bias():
    history = ["bullish", "bearish", "bullish", "bearish", "bearish"]
    assert d.count_opposite_bias_readings(history, entry_bias="bullish") == 3
    assert d.count_opposite_bias_readings(history, entry_bias="bearish") == 2


def test_count_opposite_bias_readings_ignores_conflict_and_none_readings():
    history = ["bullish", "conflict", "none", "bearish"]
    assert d.count_opposite_bias_readings(history, entry_bias="bullish") == 1


# ── classify_combined_oi_bias: the user's ORIGINAL spec (2026-09-26 real
# correction) -- combined (ATM+OTM summed) Call/Put OI, BOTH transitions
# (9:15->9:20 AND 9:20->9:25) must agree, not just the last one. This is
# deliberately a DIFFERENT rule from strategies.oi_bias_breakout.detector.
# classify_oi_bias (which cross-references OTM Call vs ATM Put separately
# and only checks 9:20->9:25) -- that function was mistakenly used as a
# stand-in for this one earlier in the same session; this is the real rule.

def test_classify_combined_oi_bias_bullish_needs_call_falling_both_legs():
    # Combined Call OI falls 915->920 AND 920->925; combined Put OI rises
    # both transitions too -> bullish.
    bias = d.classify_combined_oi_bias(
        call_oi_915=1000, call_oi_920=900, call_oi_925=800,
        put_oi_915=500, put_oi_920=600, put_oi_925=700,
    )
    assert bias == "bullish"


def test_classify_combined_oi_bias_bearish_needs_put_falling_both_legs():
    bias = d.classify_combined_oi_bias(
        call_oi_915=500, call_oi_920=600, call_oi_925=700,
        put_oi_915=1000, put_oi_920=900, put_oi_925=800,
    )
    assert bias == "bearish"


def test_classify_combined_oi_bias_none_when_only_one_transition_agrees():
    # Call falls 915->920 but RISES 920->925 -- not a sustained fall across
    # both transitions, so bullish never confirms (matches the real
    # CGPOWER/TCS/PAYTM 2026-09-17/18 cases found in this session).
    bias = d.classify_combined_oi_bias(
        call_oi_915=1000, call_oi_920=900, call_oi_925=950,
        put_oi_915=500, put_oi_920=600, put_oi_925=700,
    )
    assert bias == "none"


def test_classify_combined_oi_bias_none_when_both_rise():
    bias = d.classify_combined_oi_bias(
        call_oi_915=500, call_oi_920=600, call_oi_925=700,
        put_oi_915=500, put_oi_920=600, put_oi_925=700,
    )
    assert bias == "none"


def test_classify_combined_oi_bias_none_when_any_reading_missing():
    bias = d.classify_combined_oi_bias(
        call_oi_915=None, call_oi_920=900, call_oi_925=800,
        put_oi_915=500, put_oi_920=600, put_oi_925=700,
    )
    assert bias == "none"
