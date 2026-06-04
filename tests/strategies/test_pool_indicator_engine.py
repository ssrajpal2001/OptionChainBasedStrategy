import numpy as np
from strategies.pool_indicator_engine import PoolIndicatorEngine

def _feed_bars(eng, strike, side, closes, atp=None):
    atp = atp if atp is not None else closes
    for c, a in zip(closes, atp):
        eng.update_tick(strike, side, ltp=c, atp=a)
        eng.commit_bar()

def test_pair_indicators_combined_close_and_vwap():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    _feed_bars(eng, 100, "CE", [50, 51, 52], atp=[49, 50, 51])
    _feed_bars(eng, 100, "PE", [40, 41, 42], atp=[39, 40, 41])
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 52 + 42
    assert ind["vwap"]  == 51 + 41
    assert round(ind["slope"], 6) == round((51 + 41) - (50 + 40), 6)

def test_pair_rsi_roc_present_when_enough_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    closes = list(range(50, 70))
    _feed_bars(eng, 100, "CE", closes)
    _feed_bars(eng, 100, "PE", [10] * len(closes))
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind
    assert ind["rsi"] > 50
