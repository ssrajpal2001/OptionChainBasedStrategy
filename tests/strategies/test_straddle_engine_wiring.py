from strategies.sell_straddle import SellStraddleStrategy
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus

def test_pair_indicators_use_pool_engine():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    eng = ss._pool_engine
    for c in range(50, 70):
        eng.update_tick(23450, "CE", c, c)
        eng.update_tick(23400, "PE", 10, 10)
        eng.commit_bar()
    eng.update_tick(23450, "CE", 70, 70)
    eng.update_tick(23400, "PE", 10, 10)
    ind = ss._pair_indicators(23450, 23400)
    assert ind is not None and "rsi" in ind and "roc" in ind
