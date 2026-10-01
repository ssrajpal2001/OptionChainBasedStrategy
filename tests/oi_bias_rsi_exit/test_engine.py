"""Regression test for the 2026-09-29 CRITICAL FIX, real incident: a thin OTM
leg can genuinely have no reported OI in Upstox's own intraday candle
response for its first several minutes. The old _oi_at_snapshots only had a
single hardcoded 09:15->09:16 fallback, with none at all for 09:20/09:25, so
a leg missing OI at those points permanently classified the stock's bias as
"none" even though a real, carried-forward OI value existed moments earlier
(confirmed live via a real backtest of JUBLFOOD's 2026-09-29 data: its OTM
CE485 leg had zero reported OI at both 09:15 and 09:20).

Fixed by treating OI as a snapshot LEVEL: the read for "OI at 09:20" is now
the most recent REAL (>0) reading at-or-before 09:20, not an exact-minute
match, applied uniformly to all three snapshot times."""
import asyncio
from datetime import date, datetime, time as dtime, timedelta
from unittest.mock import AsyncMock, patch

from config.global_config import IST, GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.oi_bias_breakout.detector import SignalStrikes
from strategies.oi_bias_rsi_exit.engine import OiBiasRsiExitStrategy, OI_RECHECK_MINUTES


def _strategy():
    return OiBiasRsiExitStrategy(EventBus(), GlobalConfig(), client_id="C", binding_id="B")


def test_entry_exit_and_oi_recheck_params_are_genuinely_overridable_per_deployment():
    """2026-09-30 CRITICAL FIX, direct user audit request: these used to be
    module-level constants in engine.py with ZERO per-deployment override --
    confirms the constructor now genuinely accepts and stores different
    values than the module defaults, not just re-reading the same constant
    under a new name."""
    s = OiBiasRsiExitStrategy(
        EventBus(), GlobalConfig(), client_id="C", binding_id="B",
        entry_timeframe_min=5, entry_stoch_rsi_lengths=(14, 14, 3, 3),
        exit_timeframe_min=30, exit_stoch_rsi_lengths=(9, 9, 3, 3),
        oi_recheck_minutes=10, oi_bias_flip_count=3,
    )
    assert s._entry_timeframe_min == 5
    assert s._entry_stoch_rsi_lengths == (14, 14, 3, 3)
    assert s._exit_timeframe_min == 30
    assert s._exit_stoch_rsi_lengths == (9, 9, 3, 3)
    assert s._oi_recheck_minutes == 10
    assert s._oi_bias_flip_count == 3


class _FakeContract:
    upstox_key = "NSE_FO|999999"


def _bars(rows):
    """rows: list of (HH, MM, oi) -> Upstox-shaped candle dicts."""
    out = []
    for hh, mm, oi in rows:
        out.append({
            "ts": f"2026-09-29T{hh:02d}:{mm:02d}:00+05:30",
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "volume": 100, "oi": oi,
        })
    return out


def test_missing_oi_at_920_and_925_forward_fills_from_last_real_reading():
    """The exact JUBLFOOD incident shape: real OI at 09:15, then NOTHING
    (zero/no data) reported again until 09:26 -- 09:20 and 09:25 must both
    resolve to the 09:15 value via forward-fill, not None."""
    s = _strategy()
    rows = _bars([
        (9, 15, 200.0),
        (9, 16, 0), (9, 17, 0), (9, 18, 0), (9, 19, 0),
        (9, 20, 0), (9, 21, 0), (9, 22, 0), (9, 23, 0), (9, 24, 0), (9, 25, 0),
        (9, 26, 250.0),
    ])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("JUBLFOOD", 485, "CE", "tok"))

    assert out[dtime(9, 15)] == 200.0
    assert out[dtime(9, 20)] == 200.0, "must forward-fill from the last real reading, not return None"
    assert out[dtime(9, 25)] == 200.0


def test_no_real_oi_before_915_returns_none():
    """A key with genuinely zero OI anywhere at or before 09:15 (its very
    first trade hasn't happened yet) must stay None -- never fabricate a
    value from a later reading."""
    s = _strategy()
    rows = _bars([(9, 15, 0), (9, 16, 0), (9, 20, 150.0), (9, 25, 180.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("XYZ", 100, "PE", "tok"))

    assert out[dtime(9, 15)] is None
    assert out[dtime(9, 20)] == 150.0
    assert out[dtime(9, 25)] == 180.0


def test_exact_minute_readings_used_when_present():
    """Baseline: when every snapshot minute has a genuine real reading,
    each resolves to its own exact value (unchanged behavior)."""
    s = _strategy()
    rows = _bars([(9, 15, 100.0), (9, 20, 120.0), (9, 25, 140.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_at_snapshots("ABC", 100, "CE", "tok"))

    assert out[dtime(9, 15)] == 100.0
    assert out[dtime(9, 20)] == 120.0
    assert out[dtime(9, 25)] == 140.0


# ── Third exit condition: OI bias flips to the opposite direction, twice ───

def _pos(entry_bias_strikes=None, next_oi_check=None, prev_oi=None, history=None):
    return {
        "strikes": entry_bias_strikes or SignalStrikes(atm=100, otm_call=110, otm_put=90),
        "prev_oi": prev_oi or {"atm_call": 1000.0, "otm_call": 1000.0, "atm_put": 1000.0, "otm_put": 1000.0},
        "oi_bias_history": history if history is not None else [],
        "next_oi_check": next_oi_check or datetime.now(IST) - timedelta(seconds=1),
        "db_row_id": 1, "upstox_key": "NSE_FO|1", "entry_price": 10.0,
        "option_type": "CE", "strike": 100, "expiry": "2026-10-06", "qty": 75,
        "entry_ts": datetime.now(IST) - timedelta(minutes=30),
    }


def test_oi_flip_does_not_recheck_before_the_interval_elapses():
    """Re-check must not fire before next_oi_check -- avoids hammering REST
    every 60s poll cycle when the real cadence is every 5 minutes."""
    s = _strategy()
    pos = _pos(next_oi_check=datetime.now(IST) + timedelta(minutes=4))
    s._current_oi = AsyncMock(return_value=1000.0)
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bullish", "tok"))
    assert fired is False
    s._current_oi.assert_not_awaited()
    assert pos["oi_bias_history"] == []


def test_oi_flip_records_history_but_does_not_exit_on_a_single_opposite_reading():
    s = _strategy()
    pos = _pos()
    # OTM Call falls, ATM Put rises -- bullish per classify_oi_bias -- the
    # OPPOSITE of an entered "bearish" position -- but only ONE reading so far.
    s._current_oi = AsyncMock(side_effect=[900.0, 800.0, 1200.0, 1000.0])  # atm_call, otm_call, atm_put, otm_put
    s._close_position = AsyncMock()
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bearish", "tok"))
    assert fired is False
    assert pos["oi_bias_history"] == ["bullish"]
    s._close_position.assert_not_awaited()
    # next_oi_check advanced and prev_oi updated for the next re-check.
    assert pos["next_oi_check"] > datetime.now(IST)
    assert pos["prev_oi"] == {"atm_call": 900.0, "otm_call": 800.0, "atm_put": 1200.0, "otm_put": 1000.0}


def test_oi_flip_exits_after_two_opposite_readings_not_necessarily_consecutive():
    s = _strategy()
    pos = _pos(history=["none", "bullish"])  # already one opposite reading for a "bearish" entry
    s._current_oi = AsyncMock(side_effect=[900.0, 800.0, 1200.0, 1000.0])  # -> "bullish" again
    s._close_position = AsyncMock()
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bearish", "tok"))
    assert fired is True
    assert pos["oi_bias_history"] == ["none", "bullish", "bullish"]
    s._close_position.assert_awaited_once_with("SYM", "oi_bias_flip_twice", "tok")


def test_check_exit_skips_stoch_rsi_check_when_oi_flip_already_closed_position():
    """Once the OI-flip exit has fired and closed the position, _check_exit
    must not also run the StochRSI crossover check against a now-closed
    position."""
    s = _strategy()
    pos = _pos(history=["none", "bearish"])
    s._positions = {"SYM": pos}
    s._bias = {"SYM": "bullish"}
    s._current_oi = AsyncMock(side_effect=[1200.0, 1000.0, 1000.0, 800.0])  # atm_call,otm_call,atm_put,otm_put -> "bearish"

    async def _fake_close(symbol, reason, token):
        del s._positions[symbol]
    s._close_position = AsyncMock(side_effect=_fake_close)

    from unittest.mock import Mock
    fetch_mock = AsyncMock()
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m", fetch_mock):
        asyncio.run(s._check_exit("SYM", "tok"))

    s._close_position.assert_awaited_once_with("SYM", "oi_bias_flip_twice", "tok")
    fetch_mock.assert_not_awaited()  # never reached the StochRSI bar-fetch path


# ── Continuous rescan after 09:25, 2026-09-30 direct user spec ─────────────

import pandas as pd


def _df(symbols):
    return pd.DataFrame({"symbol": symbols})


def _oi_df(rows):
    """rows: dict symbol -> oi_spurt_pct."""
    return pd.DataFrame({"symbol": list(rows.keys()), "oi_spurt_pct": list(rows.values())})


def test_rescan_adds_only_genuinely_new_symbols():
    """First call (is_rescan=False) qualifies SYM_A. A later rescan's fetch
    returns SYM_A again (still qualifying) plus a genuinely new SYM_B --
    only SYM_B should trigger a fresh bias computation; SYM_A must not be
    reprocessed (would wipe/duplicate its already-tracked state)."""
    s = _strategy()
    s._compute_bias_for = AsyncMock()

    with patch("strategies.oi_bias_rsi_exit.engine._screener.NSESession", return_value=object()), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_fno_price_universe", return_value=_df(["SYM_A"])), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_oi_spurts_nse", return_value=_oi_df({"SYM_A": 9.0})), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_top_gainers_losers", return_value=_df(["SYM_A"])), \
         patch("strategies.oi_bias_rsi_exit.engine.REGISTRY.load_sync"):
        asyncio.run(s._run_selection_and_bias("tok", is_rescan=False))

    assert s._candidates == ["SYM_A"]
    s._compute_bias_for.assert_awaited_once_with("SYM_A", "tok")
    s._compute_bias_for.reset_mock()

    with patch("strategies.oi_bias_rsi_exit.engine._screener.NSESession", return_value=object()), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_fno_price_universe", return_value=_df(["SYM_A", "SYM_B"])), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_oi_spurts_nse", return_value=_oi_df({"SYM_A": 9.0, "SYM_B": 8.0})), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_top_gainers_losers", return_value=_df(["SYM_A", "SYM_B"])), \
         patch("strategies.oi_bias_rsi_exit.engine.REGISTRY.load_sync"):
        asyncio.run(s._run_selection_and_bias("tok", is_rescan=True))

    assert s._candidates == ["SYM_A", "SYM_B"]
    s._compute_bias_for.assert_awaited_once_with("SYM_B", "tok")


def test_rescan_with_no_new_symbols_does_not_reprocess_bias():
    s = _strategy()
    s._candidates = ["SYM_A"]
    s._compute_bias_for = AsyncMock()

    with patch("strategies.oi_bias_rsi_exit.engine._screener.NSESession", return_value=object()), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_fno_price_universe", return_value=_df(["SYM_A"])), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_oi_spurts_nse", return_value=_oi_df({"SYM_A": 9.0})), \
         patch("strategies.oi_bias_rsi_exit.engine._screener.fetch_top_gainers_losers", return_value=_df(["SYM_A"])), \
         patch("strategies.oi_bias_rsi_exit.engine.REGISTRY.load_sync"):
        asyncio.run(s._run_selection_and_bias("tok", is_rescan=True))

    assert s._candidates == ["SYM_A"]
    s._compute_bias_for.assert_not_awaited()


def test_rescan_interval_configurable_and_zero_disables():
    s = OiBiasRsiExitStrategy(EventBus(), GlobalConfig(), client_id="C", binding_id="B",
                               rescan_interval_sec=90)
    assert s._rescan_interval_sec == 90
    s2 = OiBiasRsiExitStrategy(EventBus(), GlobalConfig(), client_id="C", binding_id="B",
                                rescan_interval_sec=0)
    assert s2._rescan_interval_sec == 0


# ── _oi_max_in_windows, 2026-09-30 direct user correction ──────────────────

def _win_rows(rows):
    """rows: list of (HH, MM, oi) -> Upstox-shaped candle dicts (no row at
    all for minutes where the contract genuinely didn't trade -- matches
    the real raw Upstox response confirmed live for ABB PE6700)."""
    out = []
    for hh, mm, oi in rows:
        out.append({
            "ts": f"2026-09-30T{hh:02d}:{mm:02d}:00+05:30",
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "volume": 100, "oi": oi,
        })
    return out


def test_oi_max_in_windows_real_abb_incident_shape():
    """Real incident: ABB PE6700 had ZERO rows at all from market open
    through 09:28 (confirmed via a direct raw Upstox dict dump) -- both
    windows must resolve to None, not a fabricated 0 or a stale carry-in."""
    s = _strategy()
    rows = _win_rows([(9, 29, 27750.0), (9, 32, 27875.0)])  # first real print after both windows
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_max_in_windows("ABB", 6700, "PE", "tok"))
    assert out[0] is None  # W1 [09:15,09:20)
    assert out[1] is None  # W2 [09:20,09:25)


def test_oi_max_in_windows_takes_max_within_each_window():
    s = _strategy()
    rows = _win_rows([
        (9, 15, 100.0), (9, 17, 150.0), (9, 19, 120.0),   # W1: max=150
        (9, 21, 200.0), (9, 23, 250.0), (9, 24, 180.0),   # W2: max=250
    ])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_max_in_windows("SYM", 100, "CE", "tok"))
    assert out[0] == 150.0
    assert out[1] == 250.0


def test_oi_max_in_windows_window_boundaries_are_half_open():
    """09:20:00 itself belongs to W2 [09:20,09:25), never W1 -- confirms no
    off-by-one double counting at the boundary."""
    s = _strategy()
    rows = _win_rows([(9, 19, 50.0), (9, 20, 999.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=rows)):
        out = asyncio.run(s._oi_max_in_windows("SYM", 100, "CE", "tok"))
    assert out[0] == 50.0
    assert out[1] == 999.0


# ── Prior-day StochRSI warmup, 2026-09-30 direct user correction ───────────
# "for warmup u need to fetch data from prev day as well" -- StochRSI
# (21,21,3,3) needs ~46 bars minimum before any real K/D exists; at 3-min
# entry bars that's ~138 real minutes (not reachable until ~11:33 IST from a
# 09:15 open); at 75-min exit bars it's ~9.2 TRADING DAYS -- unreachable in
# a single day at all without prior-day seeding.

def _hist_rows(day_str, entries):
    """entries: list of (HH, MM, close) -> Upstox-shaped candle dicts for a
    given calendar day (YYYY-MM-DD)."""
    out = []
    for hh, mm, close in entries:
        out.append({
            "ts": f"{day_str}T{hh:02d}:{mm:02d}:00+05:30",
            "open": close, "high": close, "low": close, "close": close, "volume": 100,
        })
    return out


def test_bars_with_history_prepends_prior_day_and_caches_it():
    s = _strategy()
    s._today = date(2026, 9, 30)
    hist_rows = _hist_rows("2026-09-29", [(9, 15, 100.0), (9, 18, 101.0)])
    today_rows = _hist_rows("2026-09-30", [(9, 15, 110.0), (9, 18, 111.0)])
    range_mock = AsyncMock(return_value=hist_rows)
    intraday_mock = AsyncMock(return_value=today_rows)
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m", range_mock), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m", intraday_mock), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"):
        bars1 = asyncio.run(s._bars_with_history("SYM", "tok"))
        bars2 = asyncio.run(s._bars_with_history("SYM", "tok"))

    assert [b.ts.date() for b in bars1] == [date(2026, 9, 29), date(2026, 9, 29),
                                             date(2026, 9, 30), date(2026, 9, 30)]
    assert len(bars2) == 4
    range_mock.assert_awaited_once()  # prior-day history fetched ONCE, cached
    assert intraday_mock.await_count == 2  # today's bars refetched every call (real-time)


def test_bars_with_history_cache_invalidated_on_new_day():
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._history_bars_cache = {"SYM": [object()]}
    s._history_cache_date = date(2026, 9, 29)  # stale, from a prior day
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=[])), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=[])), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"):
        asyncio.run(s._bars_with_history("SYM", "tok"))
    assert s._history_cache_date == date(2026, 9, 30)


def test_check_entry_ignores_prior_day_bar_even_if_time_of_day_matches():
    """CRITICAL: a prior-day bar at e.g. 10:00 must NOT satisfy the entry
    time gate just because its clock time is >= start_time -- only a bar
    genuinely dated TODAY may trigger an entry."""
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._bias["SYM"] = "bullish"
    s._start_time = dtime(9, 25)
    # Prior day has PLENTY of bars with a K/D state that would satisfy
    # check_entry_state if the date guard were missing -- today has only 2
    # bars (nowhere near enough to independently produce a real K/D itself).
    hist_entries = [((9 * 60 + 15 + i) // 60, (9 * 60 + 15 + i) % 60, 100.0 + i)
                    for i in range(200)]  # trending up -> K>D eventually, spans past 10:00
    hist_rows = _hist_rows("2026-09-29", hist_entries)
    today_rows = _hist_rows("2026-09-30", [(9, 15, 200.0), (9, 18, 199.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=today_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract") as _rc:
        asyncio.run(s._check_entry("SYM", "tok"))
        # If the date guard were missing, a prior-day bar could satisfy the
        # entry condition and this would have been called to resolve a
        # contract for a real entry -- assert it never got that far.
        _rc.assert_not_called()


def test_check_entry_tracks_kd_for_none_bias_without_firing():
    """2026-10-01 CRITICAL FIX, direct user report ("ENRIN stocks is being
    scanned but its d and k r n/a"): a candidate whose OI bias hasn't
    resolved to a direction yet (bias="none") used to never have
    _check_entry called for it at all, so its scan-panel K/D stayed
    permanently N/A even though it's genuinely being scanned. _check_entry
    must now track K/D for a "none"-bias symbol too, while never resolving
    a contract or firing an entry for it (check_entry_state has no sane
    answer for "none")."""
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._bias["SYM"] = "none"
    hist_entries = [((9 * 60 + 15 + i) // 60, (9 * 60 + 15 + i) % 60, 100.0 + i)
                    for i in range(200)]
    hist_rows = _hist_rows("2026-09-29", hist_entries)
    today_rows = _hist_rows("2026-09-30", [(9, 15, 200.0), (9, 18, 199.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=today_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract") as _rc:
        asyncio.run(s._check_entry("SYM", "tok"))
        _rc.assert_not_called()  # never fires an entry for an unresolved bias

    assert "SYM" in s._last_entry_kd
    assert s._last_entry_kd["SYM"]["k"] is not None
    assert s._last_entry_kd["SYM"]["d"] is not None
    assert s._last_entry_kd["SYM"]["bias"] == "none"


# ── monitoring_state, 2026-09-30 direct user request ────────────────────────
# "position not showing anything that which all stocks were scanned and
# what r they doing now" -- every candidate's live bias/K/D, plus a fully
# JSON-serializable positions dict (SignalStrikes/datetime/date all flattened).

def test_monitoring_state_shows_every_scanned_candidate_not_just_positions():
    s = _strategy()
    s._candidates = ["COFORGE", "SOLARINDS", "ABB"]
    s._bias = {"COFORGE": "bearish", "SOLARINDS": "bullish", "ABB": "none"}
    s._last_entry_kd = {
        "COFORGE": {"k": 1.02, "d": 2.55, "bias": "bearish", "bars": 1787},
        "SOLARINDS": {"k": None, "d": None, "bias": "bullish", "bars": 5},
    }
    state = s.monitoring_state()
    by_sym = {row["symbol"]: row for row in state["scanned"]}
    assert by_sym["COFORGE"]["bias"] == "bearish"
    assert by_sym["COFORGE"]["k"] == 1.02
    assert by_sym["COFORGE"]["d"] == 2.55
    assert by_sym["COFORGE"]["in_position"] is False
    assert by_sym["ABB"]["bias"] == "none"
    assert by_sym["ABB"]["k"] is None  # never had an entry check yet


def test_monitoring_state_positions_are_json_serializable():
    import json
    s = _strategy()
    s._candidates = ["SYM"]
    s._bias = {"SYM": "bullish"}
    s._positions["SYM"] = {
        "option_type": "CE", "strike": 100.0, "qty": 75, "entry_price": 50.0,
        "entry_ts": datetime.now(IST), "expiry": date(2026, 10, 27),
        "upstox_key": "NSE_FO|1",
        "strikes": SignalStrikes(atm=100, otm_call=110, otm_put=90),
        "prev_oi": {}, "oi_bias_history": [],
        "next_oi_check": datetime.now(IST),
    }
    state = s.monitoring_state()
    json.dumps(state)  # must not raise
    assert state["positions"]["SYM"]["strikes"] == {"atm": 100, "otm_call": 110, "otm_put": 90}
    assert isinstance(state["positions"]["SYM"]["expiry"], str)
    assert isinstance(state["positions"]["SYM"]["entry_ts"], str)
    by_sym = {row["symbol"]: row for row in state["scanned"]}
    assert by_sym["SYM"]["in_position"] is True
    assert "CE100" in by_sym["SYM"]["position_summary"]


# ── Exit-side tracking visibility, 2026-09-30 direct user request ──────────
# "for exit what r v tracking that shoudl eb shows in log as well" -- same
# WAIT-ENTRY-style diagnostic, now for the StochRSI exit-cross check.

def _exit_bars(day_str, entries):
    return _hist_rows(day_str, entries)


def test_check_exit_logs_wait_exit_when_no_cross_yet(caplog):
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._bias["SYM"] = "bearish"
    entry_ts = datetime(2026, 9, 30, 9, 30, tzinfo=IST)
    s._positions["SYM"] = {
        "option_type": "PE", "strike": 100, "qty": 75, "entry_price": 50.0,
        "entry_ts": entry_ts, "upstox_key": "NSE_FO|1",
        "prev_oi": {"atm_call": 1.0, "otm_call": 1.0, "atm_put": 1.0, "otm_put": 1.0},
        "oi_bias_history": ["none"],
        "next_oi_check": datetime.now(IST) + timedelta(minutes=10),  # not due yet
    }
    # Enough real post-entry 75-min bars to produce a real K/D (needs ~46
    # bars minimum = ~10 trading days at 5 usable 75-min buckets/day) --
    # 1-min bars across the real trading session on each of several days.
    days = ["2026-09-18", "2026-09-19", "2026-09-22", "2026-09-23", "2026-09-24",
            "2026-09-25", "2026-09-26", "2026-09-28", "2026-09-29"]
    hist_rows = []
    for di, day_str in enumerate(days):
        day_entries = [((9 * 60 + 15 + m) // 60, (9 * 60 + 15 + m) % 60, 100.0 + di + m * 0.01)
                       for m in range(0, 375, 3)]  # every 3 min, 09:15-15:30
        hist_rows.extend(_exit_bars(day_str, day_entries))
    today_rows = _exit_bars("2026-09-30", [(9, 15, 200.0), (11, 0, 199.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=today_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"):
        asyncio.run(s._check_exit("SYM", "tok"))

    assert "SYM" in s._last_exit_kd
    assert s._last_exit_kd["SYM"]["bias"] == "bearish"
    assert s._last_exit_kd["SYM"]["oi_flip_threshold"] == s._oi_bias_flip_count
    assert "SYM" in s._positions  # not closed


def test_check_exit_display_kd_uses_warmed_history_even_with_no_post_entry_bar_yet():
    """2026-10-01 CRITICAL FIX, direct user correction ("i said to get the
    values from pev day data to warmup teh same"): a position entered only
    moments ago (before even one post-entry exit_timeframe_min bar has
    closed) used to show K/D=None/N/A on the dashboard for the entire first
    75-min window after every entry -- even though _bars_with_history()
    already prefixes real prior-trading-day bars specifically so this
    indicator is continuously warm. The crossover-detection scan correctly
    stays gated to post-entry bars only (an exit must never fire off a
    pre-entry historical crossover) -- but the DISPLAY value must show
    whatever the indicator's current, warmed-up value genuinely is, not
    N/A."""
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._bias["SYM"] = "bearish"
    # Entry happens "right now" -- deliberately AFTER every bar this test
    # supplies, so zero post-entry 75-min buckets exist yet (the exact
    # scenario this fix addresses).
    entry_ts = datetime.now(IST)
    s._positions["SYM"] = {
        "option_type": "PE", "strike": 100, "qty": 75, "entry_price": 50.0,
        "entry_ts": entry_ts, "upstox_key": "NSE_FO|1",
        "prev_oi": {"atm_call": 1.0, "otm_call": 1.0, "atm_put": 1.0, "otm_put": 1.0},
        "oi_bias_history": ["none"],
        "next_oi_check": datetime.now(IST) + timedelta(minutes=10),
    }
    days = ["2026-09-18", "2026-09-19", "2026-09-22", "2026-09-23", "2026-09-24",
            "2026-09-25", "2026-09-26", "2026-09-28", "2026-09-29"]
    hist_rows = []
    for di, day_str in enumerate(days):
        day_entries = [((9 * 60 + 15 + m) // 60, (9 * 60 + 15 + m) % 60, 100.0 + di + m * 0.01)
                       for m in range(0, 375, 3)]
        hist_rows.extend(_exit_bars(day_str, day_entries))
    # "Today" supplies only pre-entry bars (2026-09-30 historically, but in
    # terms of entry_ts = now, these are all comfortably in the past too).
    today_rows = _exit_bars("2026-09-30", [(9, 15, 200.0), (11, 0, 199.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=today_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"):
        asyncio.run(s._check_exit("SYM", "tok"))

    assert "SYM" in s._last_exit_kd
    k, d = s._last_exit_kd["SYM"]["k"], s._last_exit_kd["SYM"]["d"]
    assert k is not None and d is not None, (
        "display K/D must come from the warmed-up historical series, not be "
        "None just because no post-entry bar has closed yet"
    )
    assert isinstance(k, float) and isinstance(d, float)


def test_monitoring_state_exposes_exit_tracking_for_open_positions():
    """2026-09-30 CRITICAL FIX, direct user correction ("u need to check
    for d and k every 5 min ... and keep changign that in posiiton
    section"): once a symbol is in a position, _check_entry never runs
    again (see _tick()'s own gate) so entry-side k/d is frozen forever --
    the PRIMARY k/d shown must be the still-live exit-side reading, not
    the stale entry one. The frozen entry snapshot is kept separately as
    entry_k/entry_d for reference."""
    s = _strategy()
    s._candidates = ["SYM"]
    s._bias = {"SYM": "bearish"}
    s._positions["SYM"] = _pos()
    s._last_entry_kd["SYM"] = {"k": 1.0, "d": 2.0, "bias": "bearish", "bars": 100}
    s._last_exit_kd["SYM"] = {"k": 10.0, "d": 20.0, "bias": "bearish",
                               "oi_flip_count": 1, "oi_flip_threshold": 2}
    state = s.monitoring_state()
    row = next(r for r in state["scanned"] if r["symbol"] == "SYM")
    assert row["k"] == 10.0  # primary k/d is the live exit-side reading
    assert row["d"] == 20.0
    assert row["entry_k"] == 1.0  # frozen entry snapshot kept for reference
    assert row["entry_d"] == 2.0
    assert row["oi_flip_count"] == 1
    assert row["oi_flip_threshold"] == 2


# ── Live option-tick LTP/PnL, 2026-09-30 direct user request ───────────────
# "trade is taken that shoudl eb subscribed to websocket also ltp of option
# shoudl eb shown in position also it is nto showign proper pnl"

class _FakeGlobalFeeder:
    def __init__(self):
        self.subscribed = []

    async def subscribe_tokens(self, keys):
        self.subscribed.extend(keys)


class _FakeTick:
    def __init__(self, underlying, strike, option_type, expiry, ltp):
        self.underlying = underlying
        self.strike = strike
        self.option_type = option_type
        self.expiry = expiry
        self.ltp = ltp


def test_ensure_option_feed_subscribes_once_per_symbol():
    s = _strategy()
    gf = _FakeGlobalFeeder()
    s._bus._global_feeder = gf

    async def _drive():
        s._ensure_option_feed("SYM", "NSE_FO|1")
        s._ensure_option_feed("SYM", "NSE_FO|1")  # idempotent, no duplicate subscribe
        await asyncio.sleep(0)  # let the fire-and-forget subscribe task run

    asyncio.run(_drive())
    assert s._option_key_subscribed["SYM"] == "NSE_FO|1"


def test_option_tick_loop_updates_live_ltp_only_for_matching_position():
    s = _strategy()
    s._positions["SYM"] = _pos()  # strike=100, option_type="CE" per _pos() default fixture
    s._positions["SYM"]["strike"] = 100
    s._positions["SYM"]["option_type"] = "CE"
    s._positions["SYM"]["expiry"] = date(2026, 10, 6)

    from config.global_config import Topic as Topic_for_test

    async def _drive():
        s._running = True
        task = asyncio.create_task(s._option_tick_loop())
        await asyncio.sleep(0)  # let the loop subscribe before we publish
        # matching tick
        await s._bus.publish(Topic_for_test.OPTION_TICK, _FakeTick("SYM", 100, "CE", date(2026, 10, 6), 55.5))
        # non-matching tick (different strike) -- must not overwrite
        await s._bus.publish(Topic_for_test.OPTION_TICK, _FakeTick("SYM", 200, "CE", date(2026, 10, 6), 999.0))
        # tick for a symbol with no open position -- ignored
        await s._bus.publish(Topic_for_test.OPTION_TICK, _FakeTick("OTHER", 100, "CE", date(2026, 10, 6), 1.0))
        await asyncio.sleep(0.05)
        s._running = False
        task.cancel()

    asyncio.run(_drive())
    assert s._live_ltp["SYM"] == 55.5
    assert "OTHER" not in s._live_ltp


def test_monitoring_state_shows_live_ltp_and_unrealized_pnl():
    s = _strategy()
    s._candidates = ["SYM"]
    s._bias = {"SYM": "bullish"}
    pos = _pos()
    pos["entry_price"] = 50.0
    pos["qty"] = 75
    pos["option_type"] = "CE"
    pos["strike"] = 100
    s._positions["SYM"] = pos
    s._live_ltp["SYM"] = 60.0

    state = s.monitoring_state()
    row = next(r for r in state["scanned"] if r["symbol"] == "SYM")
    assert row["live_ltp"] == 60.0
    assert row["unrealized_pnl"] == (60.0 - 50.0) * 75
    assert state["positions"]["SYM"]["live_ltp"] == 60.0
    assert state["positions"]["SYM"]["unrealized_pnl"] == (60.0 - 50.0) * 75


def test_monitoring_state_scanned_row_exposes_entry_ts_for_open_position():
    """2026-10-01 direct user request ("UI NOT SHOWIGN ENTRY TIM") -- the
    scanned row for an open position must carry its own real entry
    timestamp (ISO string, JSON-safe) so the dashboard can show exactly
    when the trade fired."""
    s = _strategy()
    s._candidates = ["SYM"]
    s._bias = {"SYM": "bullish"}
    pos = _pos()
    s._positions["SYM"] = pos
    state = s.monitoring_state()
    row = next(r for r in state["scanned"] if r["symbol"] == "SYM")
    assert row["entry_ts"] == pos["entry_ts"].isoformat()

    other = s.monitoring_state()
    other_row = next(r for r in other["scanned"] if r["symbol"] == "SYM")
    assert isinstance(other_row["entry_ts"], str)  # JSON-serializable, not a raw datetime

    s2 = _strategy()
    s2._candidates = ["FLAT"]
    s2._bias = {"FLAT": "bullish"}
    flat_row = next(r for r in s2.monitoring_state()["scanned"] if r["symbol"] == "FLAT")
    assert flat_row["entry_ts"] is None


def test_monitoring_state_ltp_none_when_no_live_tick_yet():
    s = _strategy()
    s._candidates = ["SYM"]
    s._bias = {"SYM": "bullish"}
    s._positions["SYM"] = _pos()
    state = s.monitoring_state()
    row = next(r for r in state["scanned"] if r["symbol"] == "SYM")
    assert row["live_ltp"] is None
    assert row["unrealized_pnl"] is None


def test_close_position_cleans_up_live_ltp_tracking():
    s = _strategy()
    s._positions["SYM"] = _pos()
    s._live_ltp["SYM"] = 42.0
    s._option_key_subscribed["SYM"] = "NSE_FO|1"
    s._last_exit_kd["SYM"] = {"k": 1.0}
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=[])), \
         patch("strategies.oi_bias_rsi_exit.engine.store.record_exit"):
        asyncio.run(s._close_position("SYM", "test_reason", "tok"))
    assert "SYM" not in s._live_ltp
    assert "SYM" not in s._option_key_subscribed
    assert "SYM" not in s._last_exit_kd


# ── Position restore on restart, 2026-09-30 CRITICAL real live incident ────
# "oi scanner again send tard eto broke rwhere as tarde was rnnign it shoudl
# have checked whcih tard eis runngin adn shoudl not have send teh dat a"

def test_restore_positions_repopulates_from_db_without_reentering():
    s = _strategy()
    s._today = date(2026, 9, 30)
    db_row = {
        "id": 42, "symbol": "COFORGE", "option_type": "PE", "strike": 1760.0,
        "expiry": "2026-10-27", "qty": 475, "entry_price": 54.90,
        "entry_ts": "2026-09-30T11:19:13+05:30",
    }
    with patch("strategies.oi_bias_rsi_exit.engine.store.get_open_positions",
               return_value=[db_row]), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|COFORGE"), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=[])), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=[])):
        asyncio.run(s._restore_positions("tok"))

    assert "COFORGE" in s._positions
    pos = s._positions["COFORGE"]
    assert pos["option_type"] == "PE"
    assert pos["strike"] == 1760.0
    assert pos["qty"] == 475
    assert pos["entry_price"] == 54.90
    assert pos["db_row_id"] == 42
    assert s._bias["COFORGE"] == "bearish"  # PE -> bearish, deterministic from option_type
    assert "COFORGE" in s._candidates


def test_restore_positions_backfills_entry_kd_from_current_history():
    """2026-10-01 CRITICAL FIX, direct user report: the dashboard's "Entry
    K/D" permanently showed N/A for an already-open position after ANY
    restart, since _check_entry (the only thing that normally keeps
    _last_entry_kd fresh) is permanently skipped once a symbol is in
    self._positions. _restore_positions must now backfill a real value
    using current history, not leave it blank for the rest of the day."""
    s = _strategy()
    s._today = date(2026, 9, 30)
    db_row = {
        "id": 42, "symbol": "COFORGE", "option_type": "PE", "strike": 1760.0,
        "expiry": "2026-10-27", "qty": 475, "entry_price": 54.90,
        "entry_ts": "2026-09-30T11:19:13+05:30",
    }
    hist_entries = [((9 * 60 + 15 + i) // 60, (9 * 60 + 15 + i) % 60, 100.0 + i)
                    for i in range(200)]
    hist_rows = _hist_rows("2026-09-29", hist_entries)
    today_rows = _hist_rows("2026-09-30", [(9, 15, 200.0), (9, 18, 199.0)])
    with patch("strategies.oi_bias_rsi_exit.engine.store.get_open_positions",
               return_value=[db_row]), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract",
               return_value=_FakeContract()), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|COFORGE"), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=today_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)):
        asyncio.run(s._restore_positions("tok"))

    assert "COFORGE" in s._last_entry_kd
    assert s._last_entry_kd["COFORGE"]["k"] is not None
    assert s._last_entry_kd["COFORGE"]["d"] is not None
    assert s._last_entry_kd["COFORGE"]["bias"] == "bearish"


def test_restore_positions_is_idempotent_and_does_not_duplicate():
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._positions["COFORGE"] = _pos()  # already restored/tracked
    db_row = {
        "id": 42, "symbol": "COFORGE", "option_type": "PE", "strike": 1760.0,
        "expiry": "2026-10-27", "qty": 475, "entry_price": 54.90,
        "entry_ts": "2026-09-30T11:19:13+05:30",
    }
    with patch("strategies.oi_bias_rsi_exit.engine.store.get_open_positions",
               return_value=[db_row]), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_contract") as _rc:
        asyncio.run(s._restore_positions("tok"))
        _rc.assert_not_called()  # already tracked -- must not re-resolve/re-touch


def test_check_oi_bias_flip_defers_gracefully_when_strikes_missing():
    """A restored position whose 09:15 bar wasn't available has
    strikes=None -- must defer (not crash) and push next_oi_check out."""
    s = _strategy()
    pos = _pos()
    pos["strikes"] = None
    pos["next_oi_check"] = datetime.now(IST) - timedelta(seconds=1)
    fired = asyncio.run(s._check_oi_bias_flip("SYM", pos, "bearish", "tok"))
    assert fired is False
    assert pos["next_oi_check"] > datetime.now(IST)


def test_tick_restores_positions_exactly_once():
    s = _strategy()
    s._start_time = dtime(9, 25)
    s._get_token = AsyncMock(return_value="tok")
    s._restore_positions = AsyncMock()
    s._run_selection_and_bias = AsyncMock()
    now = datetime.now(IST).replace(hour=10, minute=0)
    with patch("strategies.oi_bias_rsi_exit.engine.datetime") as dt_mock:
        dt_mock.now.return_value = now
        asyncio.run(s._tick())
        asyncio.run(s._tick())
    assert s._restore_positions.await_count == 1
    assert s._positions_restored is True


def test_check_exit_oi_flip_count_is_real_opposite_count_not_raw_length():
    """CRITICAL FIX, real live incident: confirmed live that SOLARINDS's
    oi_bias_history grew to 8 entries, ALL "none" (never opposite of its
    bullish entry), yet the WAIT-EXIT log showed "OI-flip 8/2" -- falsely
    implying the flip-twice exit should have fired. The real count (what
    _check_oi_bias_flip itself compares against the threshold) must stay 0
    when every reading is "none"."""
    s = _strategy()
    s._today = date(2026, 9, 30)
    s._bias["SOLARINDS"] = "bullish"
    pos = _pos()
    pos["option_type"] = "CE"
    pos["oi_bias_history"] = ["none"] * 8  # matches the real live incident shape
    s._positions["SOLARINDS"] = pos

    hist_entries = [((9 * 60 + 15 + i) // 60, (9 * 60 + 15 + i) % 60, 100.0 + i * 0.1)
                    for i in range(400)]
    hist_rows = _exit_bars("2026-09-25", hist_entries)
    with patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_range_1m",
               new=AsyncMock(return_value=hist_rows)), \
         patch("strategies.oi_bias_rsi_exit.engine.fetch_upstox_intraday_1m",
               new=AsyncMock(return_value=[])), \
         patch("strategies.oi_bias_rsi_exit.engine.stock_resolve.resolve_eq_instrument_key",
               return_value="NSE_EQ|TEST"), \
         patch.object(s, "_check_oi_bias_flip", new=AsyncMock(return_value=False)):
        asyncio.run(s._check_exit("SOLARINDS", "tok"))

    assert s._last_exit_kd["SOLARINDS"]["oi_flip_count"] == 0, (
        "8 'none' readings must count as 0 opposite readings, not 8"
    )
    assert s._last_exit_kd["SOLARINDS"]["oi_flip_threshold"] == s._oi_bias_flip_count
