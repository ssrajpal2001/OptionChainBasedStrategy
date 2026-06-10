"""Entry-time prepend seed warms RSI at the rule timeframe immediately (user spec 2026-06-10)."""
from strategies.pool_indicator_engine import PoolIndicatorEngine


def _live_bars(e, n_from=700, n=5):
    for m in range(n_from, n_from + n):
        e.update_tick(23450, "CE", ltp=150 + m % 3, atp=150)
        e.update_tick(23400, "PE", ltp=140 + m % 2, atp=140)
        e.commit_bar(minute=m)


def test_seed_prepend_warms_tf2_rsi():
    e = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    _live_bars(e)                                   # a few live bars first (positive minutes)
    assert e.warm_tf(23450, 23400, 2) is False      # not enough tf=2 groups yet
    seed = [150.0 + (i % 5) for i in range(40)]      # 40 1m bars → 20 tf=2 groups
    e.seed_strike(23450, "CE", seed, seed)
    e.seed_strike(23400, "PE", seed, seed)
    assert e.warm_tf(23450, 23400, 2) is True
    pi = e.pair_indicators_tf(23450, 23400, 2)
    assert pi is not None and "rsi" in pi


def test_seed_prepend_keeps_minutes_ascending():
    e = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    _live_bars(e, n_from=700, n=5)
    e.seed_strike(23450, "CE", [1.0] * 10, [1.0] * 10)
    mins = list(e._mins[e._key(23450, "CE")])
    assert mins == sorted(mins)        # seeds prepended below live, order preserved
    assert mins[0] < 0 <= mins[-1]     # seeds negative, live non-negative


def test_seed_into_empty_strike():
    e = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    e.seed_strike(23450, "CE", [1.0, 2.0, 3.0], [1.0, 2.0, 3.0])
    mins = list(e._mins[e._key(23450, "CE")])
    assert mins == [-3, -2, -1]
