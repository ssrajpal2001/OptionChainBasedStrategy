from strategies.sell_straddle import SellStraddleStrategy as S

def test_boundary_predicate():
    assert S._at_tf_boundary(5, 5, 5) is True
    assert S._at_tf_boundary(5, 4, 5) is False   # before +5s
    assert S._at_tf_boundary(4, 30, 5) is False  # not a 5-min boundary
    assert S._at_tf_boundary(3, 10, 1) is True    # 1-min: every minute, after 5s
    assert S._at_tf_boundary(4, 10, 2) is True    # 2-min boundary
    assert S._at_tf_boundary(3, 10, 2) is False
