from strategies.sell_straddle import pool_strike_set

def test_pool_set_covers_itm_atm_otm():
    s = pool_strike_set(atm=100, step=5, itm_depth=2, otm_depth=3)
    assert min(s) == 90 and max(s) == 115
    assert 100 in s and len(s) == (2 + 3 + 1)

def test_pool_set_keeps_running_legs():
    s = pool_strike_set(atm=100, step=5, itm_depth=1, otm_depth=1, pinned={80, 130})
    assert 80 in s and 130 in s   # running legs pinned even if outside range

def test_pool_set_rounds_atm_to_step():
    s = pool_strike_set(atm=102, step=5, itm_depth=0, otm_depth=0)
    assert s == {100}             # 102 rounds to 100
