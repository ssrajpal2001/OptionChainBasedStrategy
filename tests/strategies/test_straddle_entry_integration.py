from strategies.straddle_selection import select_balanced_pair


def test_select_uses_live_cache_shape():
    # Build a cache exactly as _option_loop would, then select.
    cache = {}
    spot = 100.0
    samples = [
        (100, "CE", 60.0, 59.0), (100, "PE", 80.0, 79.0),
        (105, "PE", 55.0, 54.0), (110, "PE", 40.0, 39.0),
    ]
    for strike, side, ltp, atp in samples:
        cache[(strike, side)] = {"ltp": ltp, "atp": atp}
    res = select_balanced_pair(cache, spot=spot, step=5, offset=4, ltp_target=30.0)
    assert res == (100, 105, 60.0, 55.0)
