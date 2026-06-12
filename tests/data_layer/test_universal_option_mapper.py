"""Stage 1 — UniversalOptionMapper: CE/PE↔C/P, Delta symbol round-trip, and the 17:30 IST
daily-rollover engine. Pure + timezone-aware."""
from datetime import date, datetime

from data_layer.universal_option_mapper import UniversalOptionMapper as M, IST
from data_layer.symbol_translator import InternalSymbol


def test_option_type_normalization():
    assert M.to_short_type("CE") == "C" and M.to_short_type("CALL") == "C" and M.to_short_type("c") == "C"
    assert M.to_short_type("PE") == "P" and M.to_short_type("PUT") == "P"
    assert M.to_internal_type("C") == "CE" and M.to_internal_type("p") == "PE"
    try:
        M.to_short_type("XX"); assert False
    except ValueError:
        pass


def test_to_delta_symbol():
    s = InternalSymbol(underlying="BTC", strike=70000, option_type="CE", expiry=date(2026, 6, 12))
    assert M.to_delta_symbol(s) == "BTC-12JUN26-70000-C"
    p = InternalSymbol(underlying="ETH", strike=3500.0, option_type="PE", expiry=date(2026, 6, 12))
    assert M.to_delta_symbol(p) == "ETH-12JUN26-3500-P"


def test_parse_delta_symbol_roundtrip():
    sym = "BTC-12JUN26-70000-C"
    internal = M.parse_delta_symbol(sym)
    assert internal.underlying == "BTC" and internal.strike == 70000 and internal.option_type == "CE"
    assert internal.expiry == date(2026, 6, 12)
    assert M.to_delta_symbol(internal) == sym


def test_active_daily_expiry_before_cutoff():
    # 17:29 IST → today's contract still active.
    now = datetime(2026, 6, 12, 17, 29, tzinfo=IST)
    assert M.active_daily_expiry(now) == date(2026, 6, 12)


def test_active_daily_expiry_at_and_after_cutoff():
    # 17:30 IST exactly → rolled to next day; 23:00 IST → next day.
    assert M.active_daily_expiry(datetime(2026, 6, 12, 17, 30, tzinfo=IST)) == date(2026, 6, 13)
    assert M.active_daily_expiry(datetime(2026, 6, 12, 23, 0, tzinfo=IST)) == date(2026, 6, 13)


def test_next_rollover_and_seconds():
    now = datetime(2026, 6, 12, 17, 0, tzinfo=IST)          # 30 min before cutoff
    assert M.next_rollover_at(now) == datetime(2026, 6, 12, 17, 30, tzinfo=IST)
    assert M.seconds_to_next_rollover(now) == 30 * 60
    after = datetime(2026, 6, 12, 18, 0, tzinfo=IST)        # past cutoff → tomorrow's boundary
    assert M.next_rollover_at(after) == datetime(2026, 6, 13, 17, 30, tzinfo=IST)


def test_build_internal_delta_resolves_active_expiry():
    now = datetime(2026, 6, 12, 9, 0, tzinfo=IST)
    s = M.build_internal("btc", "CALL", 70000, exchange="DELTA", now=now)
    assert s.underlying == "BTC" and s.option_type == "CE" and s.expiry == date(2026, 6, 12)
    assert M.to_delta_symbol(s) == "BTC-12JUN26-70000-C"


def test_build_internal_nse_requires_expiry():
    try:
        M.build_internal("NIFTY", "CALL", 22000, exchange="NSE"); assert False
    except ValueError:
        pass
    s = M.build_internal("NIFTY", "CE", 22000, exchange="NSE", expiry=date(2026, 6, 18))
    assert s.option_type == "CE" and s.expiry == date(2026, 6, 18)
