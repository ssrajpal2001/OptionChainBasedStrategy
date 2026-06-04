import numpy as np
from strategies.pool_indicator_engine import PoolIndicatorEngine

def test_pair_indicators_combined_close_and_vwap():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # (ce_ltp, ce_atp, pe_ltp, pe_atp) per 1-min bar
    bars = [(50, 49, 40, 39), (51, 50, 41, 40), (52, 51, 42, 41)]
    for cl, ca, pl, pa in bars:
        eng.update_tick(100, "CE", cl, ca)
        eng.update_tick(100, "PE", pl, pa)
        eng.commit_bar()
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 52 + 42
    assert ind["vwap"] == 51 + 41
    assert round(ind["slope"], 6) == round((51 + 41) - (50 + 40), 6)

def test_pair_rsi_roc_present_when_enough_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    ce_closes = list(range(50, 70))   # 20 ascending
    for c in ce_closes:
        eng.update_tick(100, "CE", c, c)
        eng.update_tick(100, "PE", 10, 10)   # flat PE every bar
        eng.commit_bar()
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind
    assert ind["rsi"] > 50

def test_seed_prefills_series_for_rsi():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.seed_strike(100, "CE", closes=list(range(50, 70)), atps=list(range(49, 69)))
    eng.seed_strike(100, "PE", closes=[10] * 20, atps=[10] * 20)
    eng.update_tick(100, "CE", 70, 69); eng.update_tick(100, "PE", 10, 10)
    assert eng.is_warm(100, "CE")
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind

def test_commit_bar_forward_fills_all_strikes():
    # a strike that ticked once keeps advancing on later commits (minute-aligned)
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.update_tick(100, "CE", 50, 50)
    eng.update_tick(100, "PE", 10, 10)
    eng.commit_bar()
    eng.update_tick(100, "CE", 55, 55)   # only CE ticks this minute
    eng.commit_bar()                      # PE forward-fills 10
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 55 + 10        # PE held at 10

def test_tf_resample_close_and_vwap_5min():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # 12 one-min bars (minutes 0..11). CE close = 100+min, PE close = 50 (flat). atp = close-1.
    for m in range(12):
        eng.update_tick(100, "CE", 100 + m, 100 + m - 1)
        eng.update_tick(100, "PE", 50, 49)
        eng.commit_bar(minute=m)
    ind = eng.pair_indicators_tf(100, 100, tf=5)
    # groups: 0->mins0-4 (last min4: CE104), 1->mins5-9 (last min9: CE109), 2->mins10-11 INCOMPLETE(dropped)
    # last complete group = 1 -> CE109 + PE50 = 159 close ; vwap = 108 + 49 = 157
    assert ind["close"] == 159
    assert ind["vwap"] == 157
    # slope = group1 vwap (108+49) - group0 vwap (103+49) = 157 - 152 = 5
    assert round(ind["slope"], 6) == 5.0

def test_tf_le_1_delegates_to_1min():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine()
    for m in range(3):
        eng.update_tick(100, "CE", 60 + m, 60 + m); eng.update_tick(100, "PE", 40, 40); eng.commit_bar(minute=m)
    assert eng.pair_indicators_tf(100, 100, tf=1) == eng.pair_indicators(100, 100)

def test_tf_none_when_no_complete_group():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine()
    for m in range(3):  # only 3 bars, tf=5 -> group 0 is incomplete -> dropped -> None
        eng.update_tick(100, "CE", 60, 60); eng.update_tick(100, "PE", 40, 40); eng.commit_bar(minute=m)
    assert eng.pair_indicators_tf(100, 100, tf=5) is None
