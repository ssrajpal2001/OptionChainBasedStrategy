"""
tests/strategies/test_d1trap_option_book_day_rollover.py -- regression for
the 2026-08-22 fix to strategies/d1_trap_option/book.py (D1TrapOptionBook,
"d1_trap_index"/"d1_trap_fno" -- frozen/legacy, superseded by
D1TrapBearOnlyBook for active development, but still real if ever deployed).

reset_session() had zero live call sites anywhere in this file -- the exact
same class of bug independently found and fixed the same day in
strategies/fvg/engine.py. self._day_done, once set True by _eod_loop at
15:15 (non-positional/d1_trap_index only), was never reset back to False,
so every candle for every day after the first would be silently dropped.
"""
from __future__ import annotations

from datetime import datetime

from config.global_config import GlobalConfig
from data_layer.base_feeder import CandleEvent, EventBus
from strategies.d1_trap_option.book import D1TrapOptionBook


def _make_book() -> D1TrapOptionBook:
    return D1TrapOptionBook(
        EventBus(), GlobalConfig(), underlying="NIFTY",
        client_id="C", binding_id="B", strategy_name="d1_trap_index",
        feeder_token="",
    )


def test_on_candle_processes_a_new_day_after_prior_day_done():
    book = _make_book()
    book._htf_loaded = True   # skip the real REST warmup for this test

    day1 = datetime(2026, 8, 20, 9, 20)
    ev1 = CandleEvent(symbol="NSE_INDEX|Nifty 50", timeframe=5, open=100, high=101,
                       low=99, close=100.5, volume=0, timestamp=day1)
    book._on_candle(ev1)
    assert book._today == day1.date()
    assert book._last_spot == 100.5

    # Simulate _eod_loop having fired for day 1 (non-positional only).
    book._day_done = True

    day2 = datetime(2026, 8, 21, 9, 20)
    ev2 = CandleEvent(symbol="NSE_INDEX|Nifty 50", timeframe=5, open=110, high=111,
                       low=109, close=110.5, volume=0, timestamp=day2)
    book._on_candle(ev2)

    assert book._today == day2.date(), "a genuinely new day's candle must update self._today"
    assert book._day_done is False, "reset_session() must have fired and cleared day_done"
    assert book._last_spot == 110.5, (
        "day 2's candle must actually be PROCESSED, not silently dropped by "
        "the stale self._day_done==True left over from day 1"
    )


def test_on_candle_calls_reset_session_which_preserves_position_for_positional():
    """d1_trap_fno (positional/NRML carry-forward) deliberately keeps an open
    position across a day rollover -- reset_session()'s own documented
    behavior. Confirms _on_candle's new day-check actually invokes
    reset_session() (not a hand-rolled reimplementation) for a positional
    book too, without needing a fully-realistic position dict to exercise
    the unrelated _check_exit path this legacy file's _on_candle also runs."""
    book = D1TrapOptionBook(
        EventBus(), GlobalConfig(), underlying="RELIANCE",
        client_id="C", binding_id="B", strategy_name="d1_trap_fno",
        feeder_token="", lot_override=1, step_override=10,
    )
    book._htf_loaded = True
    book._today = datetime(2026, 8, 20).date()
    # direction/sl/tsl_level kept well clear of the candle's close (2510) so
    # _check_exit (unconditionally called by _on_candle, unrelated to this
    # test's own concern) doesn't fire and square off the position.
    position = {"side": "CE", "strike": 2500, "direction": "LONG", "sl": 2000.0, "tsl_level": 2000.0}
    book._position = position

    day2 = datetime(2026, 8, 21, 9, 20)
    ev2 = CandleEvent(symbol="RELIANCE", timeframe=5, open=2505, high=2515,
                       low=2500, close=2510, volume=0, timestamp=day2)
    book._on_candle(ev2)

    assert book._today == day2.date()
    assert book._position == position
