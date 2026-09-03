"""
Regression test for the 2026-08-05 live incident: FnOPositionalBook entered a
2-day-stale APPROACHING signal (POLYCAB PE) whose spot had already moved past
its own hard_sl before entry -- the position was SL'd on the very next poll,
then _rescan_and_refill() reloaded the same static watchlist file and
re-entered the identical already-invalid signal, looping forever (6 real
BUY/SELL orders on one script within ~2 minutes).

Fix: _already_sl_side() rejects entry (both TRIGGERED and APPROACHING paths)
when spot has already crossed hard_sl, and _blocked_today excludes a
(symbol, direction) pair from re-entry for the rest of the session once it's
been skipped for this reason or actually stopped out live.
"""
import asyncio

from backtest.fno_scanner.scan_live import Signal
from strategies.fno_positional.book import FnOPositionalBook


def _book() -> FnOPositionalBook:
    # bus/upstox_token are never touched by the entry-guard code paths under
    # test -- _open_position() is monkeypatched out below.
    return FnOPositionalBook(bus=None, upstox_token="", client_id="c1", binding_id="b1", mode="paper")


def _polycab_signal(status: str) -> Signal:
    # Exact real-incident shape: entry_line=9162, hard_sl=9242.86 (PE), scanned
    # 2 days stale. Spot below models the live spot (~9266) that had already
    # blown past hard_sl by the time the book tried to act on this signal.
    return Signal(
        symbol="POLYCAB", direction="PE", status=status,
        entry_line=9162.0, current=9266.0, dist_pct=0.61,
        hard_sl=9242.86, day_t1=8924.0, zone_age=3, lock_date="30 Jul",
        rr=2.94, btst_rr=1.34, suggested_strike=9300, expiry="25 AUG 26",
        upstox_key="NSE_EQ|INE455K01017",
    )


def test_already_sl_side_true_when_spot_past_hard_sl_pe():
    book = _book()
    sig = _polycab_signal("APPROACHING")
    assert book._already_sl_side(sig, spot=9266.0) is True


def test_already_sl_side_false_when_spot_within_zone_pe():
    book = _book()
    sig = _polycab_signal("APPROACHING")
    assert book._already_sl_side(sig, spot=9165.0) is False


def test_try_enter_approaching_skips_and_blocks_stale_signal():
    book = _book()
    sig = _polycab_signal("APPROACHING")
    book._pending = [sig]
    book._equity_ltp[sig.symbol] = 9266.0  # already past hard_sl=9242.86

    entered = []

    async def _fake_open(s):
        entered.append(s)

    book._open_position = _fake_open  # type: ignore[assignment]

    asyncio.run(book._try_enter_approaching())

    assert entered == [], "must not enter a position already past its own hard_sl"
    assert sig not in book._pending
    assert ("POLYCAB", "PE") in book._blocked_today


def test_try_enter_approaching_re_run_after_rescan_stays_blocked():
    """The exact loop from the incident: _rescan_and_refill() reloads the same
    stale signal into _pending a second time -- it must stay excluded."""
    book = _book()
    sig = _polycab_signal("APPROACHING")
    book._equity_ltp[sig.symbol] = 9266.0

    entered = []

    async def _fake_open(s):
        entered.append(s)

    book._open_position = _fake_open  # type: ignore[assignment]

    # First pass: skip + block.
    book._pending = [sig]
    asyncio.run(book._try_enter_approaching())
    assert entered == []

    # Simulate a rescan reloading the identical stale signal again.
    book._pending = [_polycab_signal("APPROACHING")]
    asyncio.run(book._try_enter_approaching())
    assert entered == [], "blocked (symbol, direction) must not re-enter after a rescan"


def test_try_enter_triggered_skips_and_blocks_stale_signal():
    book = _book()
    sig = _polycab_signal("TRIGGERED")
    book._pending = [sig]
    book._equity_ltp[sig.symbol] = 9266.0

    entered = []

    async def _fake_open(s):
        entered.append(s)

    book._open_position = _fake_open  # type: ignore[assignment]

    asyncio.run(book._try_enter_triggered())

    assert entered == []
    assert ("POLYCAB", "PE") in book._blocked_today


def test_healthy_approaching_signal_still_enters():
    """Sanity check the fix doesn't block legitimate entries: spot at the zone,
    nowhere near hard_sl."""
    book = _book()
    sig = _polycab_signal("APPROACHING")
    book._pending = [sig]
    book._equity_ltp[sig.symbol] = 9165.0  # right at entry_line, well inside SL

    entered = []

    async def _fake_open(s):
        entered.append(s)

    book._open_position = _fake_open  # type: ignore[assignment]

    asyncio.run(book._try_enter_approaching())

    assert entered == [sig]
    assert ("POLYCAB", "PE") not in book._blocked_today
