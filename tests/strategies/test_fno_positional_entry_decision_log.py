"""
Regression tests for the 2026-08-06 FnO Positional log-clarity fix: entering a
real position previously only logged the final order confirmation (symbol/
strike/ltp/sl/t1), with no trace of WHY the trade was taken -- the zone
boundaries, when the zone was locked, or what triggered it now. Also, Signal
never carried zone_lo/zone_hi through the save->load round-trip at all (only
computed transiently inside save_watchlist() for the JSON record), so this
data was unavailable to the live book even if it wanted to log it.

1. Signal/load_watchlist now carry zone_lo/zone_hi through.
2. _log_entry_decision() produces a log line with the zone bounds, lock date,
   age, entry_line/hard_sl/day_t1, R:R figures, and a WHY clause distinguishing
   TRIGGERED (already retested) from APPROACHING (just touched entry_line).

Also covers the 2026-08-06 OI-buildup confirmation note (_check_oi_buildup):
a best-effort, NEVER-blocking futures-OI classification appended to the same
entry-decision log line. Buildup can lag a zone touch by hours/days, so this
is diagnostic-only -- it must never raise or delay/skip an entry, only degrade
to an "unavailable" note on any failure (see data_layer/oi_buildup.py for the
pure classification logic, tested separately in tests/data_layer/).
"""
import asyncio

from backtest.fno_scanner.scan_live import Signal, save_watchlist, load_watchlist
from strategies.fno_positional.book import FnOPositionalBook


class _LogSpy:
    """Stand-in for FnOPositionalBook._log (a real make_strategy_logger file
    logger) that just records the formatted message instead of touching any
    real handler -- avoids fighting caplog/console-encoding specifics of
    whatever machine runs the test; production already proves the real
    logger handles these messages fine (em-dashes throughout this codebase's
    log lines run on the real Linux server all day)."""
    def __init__(self):
        self.messages = []

    def info(self, fmt, *args):
        self.messages.append(fmt % args)

    def warning(self, fmt, *args):
        self.messages.append(fmt % args)


def _signal(**overrides):
    base = dict(
        symbol="GLENMARK", direction="PE", status="APPROACHING",
        entry_line=1500.0, current=1495.0, dist_pct=0.33, hard_sl=1515.0,
        day_t1=1460.0, zone_age=5, lock_date="1 Aug", rr=2.5, btst_rr=2.1,
        suggested_strike=1500, expiry="25 AUG 26", upstox_key="NSE_EQ|X",
        zone_lo=1495.0, zone_hi=1515.0,
    )
    base.update(overrides)
    return Signal(**base)


def _book() -> FnOPositionalBook:
    b = FnOPositionalBook(bus=None, upstox_token="", client_id="c1", binding_id="b1", mode="paper")
    b._log = _LogSpy()
    return b


def test_zone_bounds_round_trip_through_save_and_load(tmp_path):
    sig = _signal()
    path = str(tmp_path / "watchlist.json")
    save_watchlist([sig], universe=None, top_n=5, out_path=path)
    loaded = load_watchlist(path=path)
    assert len(loaded) == 1
    # save_watchlist recomputes zone_lo/zone_hi from entry_line/hard_sl for the
    # JSON record (not a straight passthrough of the input Signal's own
    # zone_lo/zone_hi) -- just confirm the round-tripped Signal actually has
    # non-zero zone bounds now, where before this fix it would always be 0.0.
    assert loaded[0].zone_lo > 0
    assert loaded[0].zone_hi > 0


def test_log_entry_decision_approaching_includes_full_context():
    book = _book()
    sig = _signal(status="APPROACHING")
    book._log_entry_decision(sig, spot=1497.0, concept="APPROACHING")
    msg = book._log.messages[-1]
    assert "GLENMARK" in msg
    assert "1495.00" in msg and "1515.00" in msg  # zone bounds
    assert "1 Aug" in msg  # lock date
    assert "age=5d" in msg
    assert "touched entry_line" in msg


def test_log_entry_decision_triggered_says_already_retested():
    book = _book()
    sig = _signal(status="TRIGGERED")
    book._log_entry_decision(sig, spot=0.0, concept="TRIGGERED")
    msg = book._log.messages[-1]
    assert "already retested" in msg


def test_log_entry_decision_handles_missing_zone_bounds_gracefully():
    """A Signal loaded from a pre-2026-08-06 watchlist file (before this fix)
    would have zone_lo=zone_hi=0.0 -- must not crash, must say so plainly."""
    book = _book()
    sig = _signal(zone_lo=0.0, zone_hi=0.0)
    book._log_entry_decision(sig, spot=1497.0, concept="APPROACHING")
    msg = book._log.messages[-1]
    assert "unavailable" in msg


def test_log_entry_decision_includes_oi_note_when_passed():
    book = _book()
    sig = _signal(status="APPROACHING")
    book._log_entry_decision(sig, spot=1497.0, concept="APPROACHING",
                              oi_note="OI: LONG_BUILDUP (CONFIRMS CE thesis)")
    msg = book._log.messages[-1]
    assert "OI: LONG_BUILDUP (CONFIRMS CE thesis)" in msg


def test_check_oi_buildup_never_raises_when_no_futures_key_resolved(monkeypatch):
    """No futures key resolvable (e.g. symbol not found in the master JSON) must
    degrade to a plain 'unavailable' note -- never raise, never block entry."""
    import strategies.fno_positional.book as book_mod

    monkeypatch.setattr(book_mod.REGISTRY, "load_futures_only_sync", lambda *a, **k: None)
    monkeypatch.setattr(book_mod.REGISTRY, "get_futures_upstox", lambda *a, **k: "")

    book = _book()
    sig = _signal()
    note = asyncio.run(book._check_oi_buildup(sig))
    assert "unavailable" in note


def test_check_oi_buildup_never_raises_on_fetch_exception(monkeypatch):
    """Any unexpected failure in the futures-key resolve or Upstox fetch (network
    error, bad token, etc) must degrade to an 'unavailable' note, never raise --
    this check must never be able to block or delay a real entry."""
    import strategies.fno_positional.book as book_mod

    def _boom(*a, **k):
        raise RuntimeError("network blew up")

    monkeypatch.setattr(book_mod.REGISTRY, "load_futures_only_sync", _boom)

    book = _book()
    sig = _signal()
    note = asyncio.run(book._check_oi_buildup(sig))
    assert "unavailable" in note


def test_check_oi_buildup_classifies_and_reports_agreement(monkeypatch):
    import strategies.fno_positional.book as book_mod

    monkeypatch.setattr(book_mod.REGISTRY, "load_futures_only_sync", lambda *a, **k: None)
    monkeypatch.setattr(book_mod.REGISTRY, "get_futures_upstox", lambda *a, **k: "NSE_FO|999")

    async def _fake_daily(*a, **k):
        return [
            {"ts": "d1", "open": 0, "high": 0, "low": 0, "close": 100.0, "volume": 0, "oi": 1000},
            {"ts": "d2", "open": 0, "high": 0, "low": 0, "close": 105.0, "volume": 0, "oi": 1200},
        ]
    monkeypatch.setattr(book_mod, "fetch_upstox_daily", _fake_daily)

    book = _book()
    sig = _signal(direction="CE")
    note = asyncio.run(book._check_oi_buildup(sig))
    assert "LONG_BUILDUP" in note
    assert "CONFIRMS CE thesis" in note
