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
