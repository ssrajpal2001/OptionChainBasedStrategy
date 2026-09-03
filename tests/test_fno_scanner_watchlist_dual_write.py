"""
Regression test: save_watchlist() must produce a file load_watchlist() can read
back correctly at either of the two paths the live system uses --
data/fno_watchlist.json (D1TrapOptionBookManager's WATCHLIST sentinel) and
data/fno_positional_watchlist.json (FnOPositionalBook.load_watchlist()).

2026-08-05 incident: the documented nightly command
(`scan_live.py --save --top-n N`, no --out) only ever wrote the first of
these by default -- FnOPositionalBook kept trading a stale
fno_positional_watchlist.json until someone happened to pass --out
explicitly. The CLI now writes both default paths whenever --out isn't
given (see the `if args.save:` block) -- this test locks in that
save_watchlist()/load_watchlist() round-trip works identically regardless
of which of the two default paths is used, since the CLI change itself is
just wiring that call twice.
"""
import json

from backtest.fno_scanner.scan_live import Signal, save_watchlist, load_watchlist


def _signal() -> Signal:
    return Signal(
        symbol="POLYCAB", direction="PE", status="APPROACHING",
        entry_line=9162.0, current=9034.0, dist_pct=1.40,
        hard_sl=9242.86, day_t1=9001.0, zone_age=3, lock_date="30 Jul",
        rr=2.94, btst_rr=1.34, suggested_strike=9300, expiry="25 AUG 26",
        upstox_key="NSE_EQ|INE455K01017",
    )


def test_save_and_load_roundtrip_at_either_default_path(tmp_path):
    sig = _signal()

    path_a = str(tmp_path / "fno_watchlist.json")
    path_b = str(tmp_path / "fno_positional_watchlist.json")

    written_a = save_watchlist([sig], universe=None, top_n=5, out_path=path_a)
    written_b = save_watchlist([sig], universe=None, top_n=5, out_path=path_b)

    assert written_a == path_a
    assert written_b == path_b

    for p in (path_a, path_b):
        loaded = load_watchlist(path=p)
        assert len(loaded) == 1
        assert loaded[0].symbol == "POLYCAB"
        assert loaded[0].direction == "PE"
        assert loaded[0].hard_sl == 9242.86

    # Independent writes -- not accidentally the same file/content object.
    with open(path_a) as f:
        data_a = json.load(f)
    with open(path_b) as f:
        data_b = json.load(f)
    assert data_a["stocks"][0]["symbol"] == data_b["stocks"][0]["symbol"] == "POLYCAB"
