from strategies.sell_straddle import _eval_rules

def test_eval_rules_uses_per_rule_tf():
    rules = [
        {"indicator":"advanced","operator":"AND","tf":"1","operator_sym":"<","operand1":"CLOSE","operand2":"VWAP","operand2_val":0},
        {"indicator":"advanced","operator":"AND","tf":"5","operator_sym":"<","operand1":"SLOPE","operand2":"VALUE","operand2_val":0},
    ]
    ind_by_tf = {
        1: {"close": 100.0, "vwap": 110.0},          # CLOSE<VWAP true at 1m
        5: {"close": 100.0, "vwap": 110.0, "slope": -2.0},  # SLOPE<0 true at 5m
    }
    passed, reason = _eval_rules(rules, ind_by_tf)
    assert passed is True

def test_eval_rules_blocks_when_tf_operand_missing():
    rules = [{"indicator":"advanced","operator":"AND","tf":"5","operator_sym":">","operand1":"RSI","operand2":"VALUE","operand2_val":55}]
    ind_by_tf = {1: {"close":1,"vwap":1}, 5: {"close":1,"vwap":1}}  # no rsi at 5m
    passed, _ = _eval_rules(rules, ind_by_tf)
    assert passed is False

def test_eval_rules_backward_compat_flat_dict():
    rules = [{"indicator":"advanced","operator":"AND","tf":"1","operator_sym":"<","operand1":"CLOSE","operand2":"VWAP","operand2_val":0}]
    passed, _ = _eval_rules(rules, {"close": 100.0, "vwap": 110.0})  # flat dict -> treated as tf=1
    assert passed is True
