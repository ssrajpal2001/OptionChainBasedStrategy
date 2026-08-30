"""
2026-08-24: unit tests for strategies/oi_orb_screener/screener.py -- the
ported Colab screener's pure logic (classify_nifty_regime, evaluate_
breakout, MinuteBars.orb(), build_shortlist's filter/score logic).

Hand-built inputs only, no live NSE calls -- mirrors this session's own
confirmed-live behavior (regime table, abort-pct guard, already-fired
dedup) so a future refactor can't silently drift from what was validated
against real NSE data on 2026-08-24.
"""
from datetime import datetime

import pandas as pd
import pytest

from strategies.oi_orb_screener import screener


def _cfg(**overrides):
    cfg = dict(screener.CONFIG)
    cfg.update(overrides)
    return cfg


# ── classify_nifty_regime ────────────────────────────────────────────────

def test_regime_bullish_at_or_above_threshold():
    cfg = _cfg(NIFTY_BULLISH_PCT=0.3, NIFTY_BEARISH_PCT=-0.3)
    assert screener.classify_nifty_regime(0.30, cfg) == "bullish"
    assert screener.classify_nifty_regime(0.50, cfg) == "bullish"


def test_regime_bearish_at_or_below_threshold():
    cfg = _cfg(NIFTY_BULLISH_PCT=0.3, NIFTY_BEARISH_PCT=-0.3)
    assert screener.classify_nifty_regime(-0.30, cfg) == "bearish"
    assert screener.classify_nifty_regime(-0.50, cfg) == "bearish"


def test_regime_neutral_inside_band():
    cfg = _cfg(NIFTY_BULLISH_PCT=0.3, NIFTY_BEARISH_PCT=-0.3)
    assert screener.classify_nifty_regime(0.09, cfg) == "neutral"
    assert screener.classify_nifty_regime(0.0, cfg) == "neutral"


# ── evaluate_breakout -- regime table ────────────────────────────────────

def test_neutral_day_no_trade_when_filter_enabled():
    cfg = _cfg(REGIME_FILTER_ENABLED=True)
    fired = set()
    sig = screener.evaluate_breakout("MANAPPURAM", 365.0, 357.5, 362.30, 358.95,
                                      "neutral", fired, cfg)
    assert sig is None


def test_neutral_day_still_fires_when_filter_disabled():
    """2026-08-24 calibration toggle -- confirmed live against real
    MANAPPURAM/SIEMENS data on a neutral NIFTY day."""
    cfg = _cfg(REGIME_FILTER_ENABLED=False)
    fired = set()
    sig = screener.evaluate_breakout("MANAPPURAM", 365.25, 357.5, 362.30, 358.95,
                                      "neutral", fired, cfg)
    assert sig is not None
    assert sig.side == "CALL"
    assert sig.reason == "orb_high_breakout"


def test_bullish_day_orb_high_breakout_fires_call():
    # prev_close=110, ltp=113.5 -> +3.2%, safely under the 4% abort guard.
    cfg = _cfg()
    fired = set()
    sig = screener.evaluate_breakout("VMM", 113.5, 110.0, 113.31, 109.52,
                                      "bullish", fired, cfg)
    assert sig is not None
    assert sig.side == "CALL"
    assert sig.symbol == "VMM"


def test_bearish_day_orb_high_breakout_is_ignored():
    """Explicit regime table: bearish day + ORB-high breakout -> IGNORE."""
    cfg = _cfg()
    fired = set()
    sig = screener.evaluate_breakout("VMM", 113.5, 110.0, 113.31, 109.52,
                                      "bearish", fired, cfg)
    assert sig is None
    assert ("VMM", "CALL") in fired   # still marked fired so it's not re-evaluated


def test_bearish_day_orb_low_breakdown_fires_put():
    # prev_close=14700, ltp=14400 -> -2.0%, safely under the 4% abort guard.
    cfg = _cfg()
    fired = set()
    sig = screener.evaluate_breakout("DIXON", 14400.0, 14700.0, 14976.0, 14542.0,
                                      "bearish", fired, cfg)
    assert sig is not None
    assert sig.side == "PUT"


def test_bullish_day_orb_low_breakdown_still_fires_put():
    """Bullish day: ORB Low breakdown -> BUY PUT too (per the regime table,
    not just bearish days)."""
    cfg = _cfg()
    fired = set()
    sig = screener.evaluate_breakout("DIXON", 14400.0, 14700.0, 14976.0, 14542.0,
                                      "bullish", fired, cfg)
    assert sig is not None
    assert sig.side == "PUT"


def test_already_fired_signal_not_repeated():
    cfg = _cfg()
    fired = {("VMM", "CALL")}
    sig = screener.evaluate_breakout("VMM", 114.0, 103.43, 113.31, 109.52,
                                      "bullish", fired, cfg)
    assert sig is None


def test_stock_move_abort_pct_skips_already_extended_stock():
    """VMM at +9.28% (real 2026-08-24 value) is past the 4% abort guard --
    must not fire even on an otherwise-valid breakout."""
    cfg = _cfg(STOCK_MOVE_ABORT_PCT=4.0)
    fired = set()
    sig = screener.evaluate_breakout("VMM", 112.77, 103.43, 108.0, 105.0,
                                      "bullish", fired, cfg)
    assert sig is None


def test_no_breakout_when_price_inside_orb_range():
    cfg = _cfg()
    fired = set()
    sig = screener.evaluate_breakout("MANAPPURAM", 360.0, 357.5, 362.30, 358.95,
                                      "bullish", fired, cfg)
    assert sig is None


# ── MinuteBars.orb() ──────────────────────────────────────────────────────

def test_minute_bars_orb_range():
    bars = screener.MinuteBars()
    ts = lambda hm: datetime.strptime(f"2026-08-24 {hm}:00", "%Y-%m-%d %H:%M:%S")
    bars.on_quote("VMM", 110.0, ts("09:15"))
    bars.on_quote("VMM", 113.7, ts("09:20"))
    bars.on_quote("VMM", 109.0, ts("09:25"))
    bars.on_quote("VMM", 111.0, ts("09:29"))
    hi, lo = bars.orb("VMM", "09:15", "09:30")
    assert hi == 113.7
    assert lo == 109.0


def test_minute_bars_orb_none_when_no_bars():
    bars = screener.MinuteBars()
    hi, lo = bars.orb("UNKNOWN", "09:15", "09:30")
    assert hi is None and lo is None


def test_minute_bars_ignores_nonpositive_price():
    bars = screener.MinuteBars()
    ts = datetime.strptime("2026-08-24 09:16:00", "%Y-%m-%d %H:%M:%S")
    bars.on_quote("VMM", 0.0, ts)
    bars.on_quote("VMM", -5.0, ts)
    hi, lo = bars.orb("VMM", "09:15", "09:30")
    assert hi is None and lo is None


# ── build_shortlist -- filter/scoring logic (no live NSE calls) ─────────

class _FakeNSE:
    def __init__(self, universe_df, nifty_pchange, oi_spurt_df):
        self._universe_df = universe_df
        self._nifty_pchange = nifty_pchange
        self._oi_spurt_df = oi_spurt_df


def test_build_shortlist_filters_and_ranks(monkeypatch):
    universe = pd.DataFrame([
        {"symbol": "VMM", "lastPrice": 112.46, "pChange": 9.03, "open": 109.2,
         "dayHigh": 113.7, "dayLow": 109.0, "previousClose": 103.43, "totalTradedVolume": 37083092},
        {"symbol": "MUTHOOTFIN", "lastPrice": 3154.0, "pChange": 4.34, "open": 3056.0,
         "dayHigh": 3156.0, "dayLow": 3049.8, "previousClose": 3022.0, "totalTradedVolume": 779998},
        {"symbol": "TOOSMALL", "lastPrice": 100.0, "pChange": 1.0, "open": 99.0,
         "dayHigh": 101.0, "dayLow": 98.0, "previousClose": 99.0, "totalTradedVolume": 1000},
        {"symbol": "NOSPURT", "lastPrice": 500.0, "pChange": 3.0, "open": 480.0,
         "dayHigh": 505.0, "dayLow": 478.0, "previousClose": 485.0, "totalTradedVolume": 2000},
    ])
    oi_spurts = pd.DataFrame({
        "symbol": ["VMM", "MUTHOOTFIN", "TOOSMALL"],
        "oi_spurt_pct": [32.84, 21.95, 50.0],   # NOSPURT deliberately absent -- inner join drops it
    })

    monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
    monkeypatch.setattr(screener, "fetch_nifty_pchange", lambda nse: 0.11)
    monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

    cfg = _cfg(OI_SPURT_MIN_PCT=7.0, PRICE_MOVE_MIN_PCT=2.0, TOP_N_PER_SIDE=5)
    shortlist, nifty_pchange = screener.build_shortlist(nse=None, cfg=cfg)

    assert nifty_pchange == 0.11
    symbols = set(shortlist["symbol"].tolist())
    assert symbols == {"VMM", "MUTHOOTFIN"}   # TOOSMALL fails price-move filter, NOSPURT fails OI join
    # VMM has both the bigger price move and bigger OI spurt -- must rank first.
    assert shortlist.iloc[0]["symbol"] == "VMM"


def test_poll_oi_rank_ranks_by_oi_spurt_no_threshold_filter(monkeypatch):
    """2026-08-30, direct user spec: rank-based, not threshold-based -- a
    stock below OI_SPURT_MIN_PCT/PRICE_MOVE_MIN_PCT must still appear if it
    ranks within RANK_TOP_N, unlike build_shortlist."""
    universe = pd.DataFrame([
        {"symbol": "VMM", "lastPrice": 112.46, "pChange": 9.03, "open": 109.2,
         "dayHigh": 113.7, "dayLow": 109.0, "previousClose": 103.43, "totalTradedVolume": 37083092},
        {"symbol": "MUTHOOTFIN", "lastPrice": 3154.0, "pChange": 4.34, "open": 3056.0,
         "dayHigh": 3156.0, "dayLow": 3049.8, "previousClose": 3022.0, "totalTradedVolume": 779998},
        # Deliberately BELOW both build_shortlist's thresholds (oi<7%, |pChange|<2%) --
        # must still rank (and appear) here since poll_oi_rank applies no threshold.
        {"symbol": "BELOWTHRESH", "lastPrice": 500.0, "pChange": 0.5, "open": 498.0,
         "dayHigh": 502.0, "dayLow": 497.0, "previousClose": 497.5, "totalTradedVolume": 5000},
    ])
    oi_spurts = pd.DataFrame({
        "symbol": ["VMM", "MUTHOOTFIN", "BELOWTHRESH"],
        "oi_spurt_pct": [32.84, 21.95, 3.0],
    })
    monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
    monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

    cfg = _cfg(RANK_TOP_N=10)
    ranked = screener.poll_oi_rank(nse=None, cfg=cfg)

    assert list(ranked["symbol"]) == ["VMM", "MUTHOOTFIN", "BELOWTHRESH"]
    assert list(ranked["rank"]) == [1, 2, 3]


def test_poll_oi_rank_truncates_to_top_n(monkeypatch):
    universe = pd.DataFrame([
        {"symbol": f"S{i}", "lastPrice": 100.0, "pChange": 1.0, "open": 99.0,
         "dayHigh": 101.0, "dayLow": 98.0, "previousClose": 99.0, "totalTradedVolume": 1000}
        for i in range(15)
    ])
    oi_spurts = pd.DataFrame({
        "symbol": [f"S{i}" for i in range(15)],
        "oi_spurt_pct": [float(15 - i) for i in range(15)],   # S0 highest
    })
    monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
    monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

    cfg = _cfg(RANK_TOP_N=10)
    ranked = screener.poll_oi_rank(nse=None, cfg=cfg)
    assert len(ranked) == 10
    assert ranked.iloc[0]["symbol"] == "S0"
    assert list(ranked["rank"]) == list(range(1, 11))


def test_poll_oi_rank_empty_on_no_overlap(monkeypatch):
    universe = pd.DataFrame([
        {"symbol": "NOMATCH", "lastPrice": 100.0, "pChange": 1.0, "open": 99.0,
         "dayHigh": 101.0, "dayLow": 98.0, "previousClose": 99.0, "totalTradedVolume": 1000},
    ])
    oi_spurts = pd.DataFrame({"symbol": ["OTHER"], "oi_spurt_pct": [10.0]})
    monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
    monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

    ranked = screener.poll_oi_rank(nse=None, cfg=_cfg())
    assert ranked.empty


def test_build_shortlist_empty_when_nothing_passes(monkeypatch):
    universe = pd.DataFrame([
        {"symbol": "FLAT", "lastPrice": 100.0, "pChange": 0.1, "open": 99.9,
         "dayHigh": 100.5, "dayLow": 99.5, "previousClose": 99.9, "totalTradedVolume": 1000},
    ])
    oi_spurts = pd.DataFrame({"symbol": ["FLAT"], "oi_spurt_pct": [1.0]})

    monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
    monkeypatch.setattr(screener, "fetch_nifty_pchange", lambda nse: 0.0)
    monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

    cfg = _cfg()
    shortlist, nifty_pchange = screener.build_shortlist(nse=None, cfg=cfg)
    assert shortlist.empty


# ── MinuteBars.closes() ────────────────────────────────────────────────────

def test_minute_bars_closes_ordered_and_filtered():
    bars = screener.MinuteBars()
    ts = lambda hm: datetime.strptime(f"2026-08-24 {hm}:00", "%Y-%m-%d %H:%M:%S")
    for hm, price in [("09:25", 100.0), ("09:26", 101.0), ("09:27", 99.0), ("09:28", 102.0)]:
        bars.on_quote("VMM", price, ts(hm))
    assert bars.closes("VMM") == [100.0, 101.0, 99.0, 102.0]
    assert bars.closes("VMM", after="09:26") == [99.0, 102.0]
    assert bars.closes("VMM", before="09:27") == [100.0, 101.0]
    assert bars.closes("VMM", after="09:25", before="09:28") == [101.0, 99.0]


def test_minute_bars_closes_empty_for_unknown_symbol():
    bars = screener.MinuteBars()
    assert bars.closes("UNKNOWN") == []


# ── compute_sma ─────────────────────────────────────────────────────────────

def test_compute_sma_basic():
    assert screener.compute_sma([1.0, 2.0, 3.0, 4.0], 4) == pytest.approx(2.5)


def test_compute_sma_uses_only_last_period_closes():
    # 8-period SMA over 10 closes -- must use only the LAST 8, not all 10.
    closes = [100.0] * 2 + [10.0] * 8   # first two would badly skew the average if included
    assert screener.compute_sma(closes, 8) == pytest.approx(10.0)


def test_compute_sma_none_when_insufficient_data():
    assert screener.compute_sma([1.0, 2.0], 8) is None


# ── check_rejection_pattern ("50% rejection rule") ──────────────────────────

def test_rejection_call_side_fires_on_deep_retrace():
    # orb_high=100. Peak pushed to 103 (+3%, past the 2% min-rise). Current
    # price has given back 60% of that 3-point move (>= the 50% fraction).
    fired = screener.check_rejection_pattern(
        extreme_since_orb=103.0, orb_level=100.0, current_ltp=101.2,
        side="CALL", min_rise_pct=2.0, retrace_fraction=0.5)
    assert fired is True


def test_rejection_call_side_does_not_fire_below_min_rise():
    # Peak only reached 100.5 above orb_high=100 -- a 0.5% push, below the
    # 2% minimum -- must not fire regardless of any later retrace.
    fired = screener.check_rejection_pattern(
        extreme_since_orb=100.5, orb_level=100.0, current_ltp=100.0,
        side="CALL", min_rise_pct=2.0, retrace_fraction=0.5)
    assert fired is False


def test_rejection_call_side_does_not_fire_on_shallow_retrace():
    # Peak at 103 (+3%, clears the min-rise), but current price has only
    # given back 20% of the move -- below the 50% retrace_fraction.
    fired = screener.check_rejection_pattern(
        extreme_since_orb=103.0, orb_level=100.0, current_ltp=102.4,
        side="CALL", min_rise_pct=2.0, retrace_fraction=0.5)
    assert fired is False


def test_rejection_put_side_fires_symmetrically():
    # orb_low=100. Trough pushed to 97 (-3%). Price has since recovered 60%
    # of that 3-point drop.
    fired = screener.check_rejection_pattern(
        extreme_since_orb=97.0, orb_level=100.0, current_ltp=98.8,
        side="PUT", min_rise_pct=2.0, retrace_fraction=0.5)
    assert fired is True


def test_rejection_call_side_no_move_beyond_orb_level():
    # extreme_since_orb never actually exceeded orb_level -- nothing to
    # reject against.
    fired = screener.check_rejection_pattern(
        extreme_since_orb=99.0, orb_level=100.0, current_ltp=99.0,
        side="CALL", min_rise_pct=2.0, retrace_fraction=0.5)
    assert fired is False


# ── check_sma_exit ──────────────────────────────────────────────────────────

def test_sma_exit_call_side_fires_on_two_consecutive_closes_below():
    # 8-SMA of the first 8 closes (all 100) = 100. Last 2 closes (95, 94)
    # are both below it.
    closes = [100.0] * 8 + [95.0, 94.0]
    assert screener.check_sma_exit(closes, sma_period=8, consec_closes=2, side="CALL") is True


def test_sma_exit_call_side_does_not_fire_on_only_one_close_below():
    closes = [100.0] * 8 + [105.0, 94.0]   # only the LAST close is below the SMA
    assert screener.check_sma_exit(closes, sma_period=8, consec_closes=2, side="CALL") is False


def test_sma_exit_put_side_fires_on_two_consecutive_closes_above():
    closes = [100.0] * 8 + [105.0, 106.0]
    assert screener.check_sma_exit(closes, sma_period=8, consec_closes=2, side="PUT") is True


def test_sma_exit_none_when_insufficient_closes():
    closes = [100.0] * 5   # fewer than sma_period + consec_closes - 1
    assert screener.check_sma_exit(closes, sma_period=8, consec_closes=2, side="CALL") is False


def test_sma_exit_uses_each_bars_own_rolling_sma_not_a_single_snapshot():
    """2026-08-26 real incident regression: a live VBL position exited on
    sma_exit, but the user's own chart showed the SMA still on the correct
    side of price at that moment. Root cause: the old implementation checked
    the last N closes against a single "current" SMA snapshot (computed from
    the most recent sma_period closes) instead of each close's own rolling
    SMA as of that bar -- since the snapshot window includes the very closes
    being tested, a later sharp move can retroactively change the verdict for
    an earlier close, unlike a real chart where the SMA line's value at a
    given bar never changes as later bars print.

    closes[-2]=104.0, closes[-1]=140.0, sma_period=8:
      - closes[-2]'s OWN rolling SMA (its trailing 8 closes, ending at
        itself) = 100.5 -> 104.0 > 100.5 (above).
      - closes[-1]'s OWN rolling SMA = 105.5 -> 140.0 > 105.5 (above).
      Both genuinely above their own bar's SMA -> a PUT exit should fire.

      The OLD single-snapshot method instead computed ONE SMA from the most
      recent 8 closes (105.5, coincidentally same as closes[-1]'s own) and
      applied it to BOTH: 104.0 > 105.5 is FALSE, so the old code would have
      missed this genuine two-bar exit entirely."""
    closes = [100.0] * 8 + [104.0, 140.0]
    assert screener.check_sma_exit(closes, sma_period=8, consec_closes=2, side="PUT") is True


# ── backfill_orb_from_yahoo ──────────────────────────────────────────────
# 2026-08-25 CRITICAL FIX regression: real incident, a single-stock shortlist
# (SAIL alone) produced an empty ORB for the day even though Yahoo genuinely
# returned real 09:15-09:25 bars -- yf.download(..., group_by="ticker")
# ALWAYS returns MultiIndex columns like ('SAIL.NS', 'High'), even for one
# ticker, but the old code assumed single-ticker downloads came back flat
# and skipped the df[ticker] indexing step, so every row's High/Low read as
# None and got silently discarded. Shape below is copied verbatim from a
# real yf.download(['SAIL.NS'], ..., group_by='ticker') call.

def test_backfill_orb_from_yahoo_single_ticker_multiindex_columns(monkeypatch):
    import sys
    import types
    from datetime import datetime as _dt

    import pandas as pd

    ist = screener.IST
    idx = pd.DatetimeIndex(
        [_dt(2026, 8, 25, 9, 15), _dt(2026, 8, 25, 9, 16), _dt(2026, 8, 25, 9, 26)],
        tz=ist,
    )
    cols = pd.MultiIndex.from_tuples(
        [("SAIL.NS", "Open"), ("SAIL.NS", "High"), ("SAIL.NS", "Low"),
         ("SAIL.NS", "Close"), ("SAIL.NS", "Volume")],
        names=["Ticker", "Price"],
    )
    data = [
        [180.75, 182.98, 180.50, 182.84, 0],
        [182.86, 183.10, 182.50, 182.67, 500251],
        [183.00, 183.50, 182.90, 183.20, 100000],   # 09:26 -- outside ORB_END, must be excluded
    ]
    fake_df = pd.DataFrame(data, index=idx, columns=cols)

    fake_yf = types.SimpleNamespace(download=lambda *a, **kw: fake_df)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    bars = screener.MinuteBars()
    screener.backfill_orb_from_yahoo(bars, ["SAIL"], screener.CONFIG)

    h, l = bars.orb("SAIL", "09:15", "09:25")
    assert h is not None and l is not None, "single-ticker MultiIndex bars must not be silently dropped"
    assert h == pytest.approx(183.10)
    assert l == pytest.approx(180.50)
    assert "09:26" not in bars.bars["SAIL"]   # outside the ORB window, correctly excluded


# ── SmaBars (2026-08-26: moved SMA exit from 1-min to a configurable
# tf_min, backed by real historical seeding across the day boundary) ───────

def test_smabars_bucket_key_floors_to_tf_min():
    from datetime import datetime as _dt
    ts = _dt(2026, 8, 26, 9, 27)
    assert screener.SmaBars._bucket_key(ts, 5) == "2026-08-26 09:25"
    assert screener.SmaBars._bucket_key(ts, 1) == "2026-08-26 09:27"


def test_smabars_on_quote_bucketed_and_last_quote_wins():
    from datetime import datetime as _dt
    bars = screener.SmaBars()
    bars.on_quote("VBL", 100.0, _dt(2026, 8, 26, 9, 30), tf_min=5)
    bars.on_quote("VBL", 101.0, _dt(2026, 8, 26, 9, 32), tf_min=5)   # same 5-min bucket
    bars.on_quote("VBL", 105.0, _dt(2026, 8, 26, 9, 36), tf_min=5)   # next bucket
    closes = bars.closes("VBL", before=_dt(2026, 8, 26, 9, 40), tf_min=5)
    assert closes == [101.0, 105.0]   # last quote in the 09:30 bucket wins


def test_smabars_closes_excludes_the_still_forming_bucket():
    from datetime import datetime as _dt
    bars = screener.SmaBars()
    bars.on_quote("VBL", 100.0, _dt(2026, 8, 26, 9, 30), tf_min=5)
    bars.on_quote("VBL", 102.0, _dt(2026, 8, 26, 9, 33), tf_min=5)   # still inside the 09:30-09:35 bucket
    closes = bars.closes("VBL", before=_dt(2026, 8, 26, 9, 33), tf_min=5)
    assert closes == []   # the 09:30 bucket hasn't closed yet as of 09:33 itself


# ── VWAP retest entry + VWAP-relative SL (2026-08-27, direct user spec:
# replaces the ORB-breach entry trigger and the S&R R1/S1/R2/S2 SL) ────────

def test_vwapstate_current_none_until_any_volume_recorded():
    vwap = screener.VwapState()
    assert vwap.current("VBL") is None


def test_vwapstate_update_computes_running_weighted_average():
    vwap = screener.VwapState()
    vwap.update("VBL", 100.0, 10.0)   # 100*10 / 10 = 100
    assert vwap.current("VBL") == 100.0
    vwap.update("VBL", 200.0, 10.0)   # (1000+2000)/20 = 150
    assert vwap.current("VBL") == 150.0


def test_vwapstate_update_ignores_non_positive_price_or_volume():
    vwap = screener.VwapState()
    vwap.update("VBL", 0.0, 10.0)
    vwap.update("VBL", 100.0, 0.0)
    assert vwap.current("VBL") is None


def test_vwapstate_seed_adds_to_not_replaces_existing_accumulation():
    vwap = screener.VwapState()
    vwap.seed("VBL", 500.0, 5.0)   # 500/5 = 100
    assert vwap.current("VBL") == 100.0
    vwap.seed("VBL", 1000.0, 5.0)   # (500+1000)/(5+5) = 150
    assert vwap.current("VBL") == 150.0


def test_side_from_pchange_gainer_is_call_loser_is_put():
    assert screener.side_from_pchange(2.5) == "CALL"
    assert screener.side_from_pchange(-2.5) == "PUT"


def test_side_allowed_by_regime_matches_evaluate_breakouts_own_table():
    # Bullish day: both sides tradeable.
    assert screener.side_allowed_by_regime("CALL", "bullish", True) is True
    assert screener.side_allowed_by_regime("PUT", "bullish", True) is True
    # Bearish day: CALL blocked, PUT still tradeable.
    assert screener.side_allowed_by_regime("CALL", "bearish", True) is False
    assert screener.side_allowed_by_regime("PUT", "bearish", True) is True
    # Neutral day: nothing tradeable.
    assert screener.side_allowed_by_regime("CALL", "neutral", True) is False
    assert screener.side_allowed_by_regime("PUT", "neutral", True) is False
    # Filter off: everything tradeable regardless of regime.
    assert screener.side_allowed_by_regime("CALL", "neutral", False) is True


def test_check_vwap_retest_entry_call_arms_above_then_fires_on_touch_back_down():
    # 2026-08-28 direct user correction: arming is pure directional positioning
    # relative to vwap, no minimum-gap threshold -- any amount above arms CALL.
    armed, fire = screener.check_vwap_retest_entry("CALL", 100.05, 100.0, False, 0.15)
    assert armed is True and fire is False
    # Now armed, price pulls back down to touch vwap -- fires.
    armed, fire = screener.check_vwap_retest_entry("CALL", 100.0, 100.0, True, 0.15)
    assert armed is True and fire is True
    # Armed, still above vwap -- no fire yet.
    armed, fire = screener.check_vwap_retest_entry("CALL", 100.10, 100.0, True, 0.15)
    assert armed is True and fire is False


def test_check_vwap_retest_entry_put_arms_below_then_fires_on_bounce_back_up():
    armed, fire = screener.check_vwap_retest_entry("PUT", 99.95, 100.0, False, 0.15)
    assert armed is True and fire is False
    armed, fire = screener.check_vwap_retest_entry("PUT", 100.0, 100.0, True, 0.15)
    assert armed is True and fire is True


def test_check_vwap_retest_entry_none_vwap_never_arms_or_fires():
    armed, fire = screener.check_vwap_retest_entry("CALL", 100.0, 0.0, False, 0.15)
    assert armed is False and fire is False


# ── option-premium SL/target (2026-08-27, direct user spec: "checking for
# target and SL in stock, change it to the option which we are taking") ────

def test_compute_option_premium_sl_arm_arms_on_close_below_vwap():
    # Side-independent -- a bought CE and a bought PE both want their OWN
    # premium to rise, so adverse is always "closed below its own vwap".
    assert screener.compute_option_premium_sl_arm(95.0, 100.0, 90.0) == 90.0
    # Favorable close (above vwap) -> no arm.
    assert screener.compute_option_premium_sl_arm(105.0, 100.0, 90.0) is None


def test_compute_option_premium_sl_arm_no_vwap_never_arms():
    assert screener.compute_option_premium_sl_arm(95.0, 0.0, 90.0) is None


# ── pooled multi-touch SL anchor (2026-08-28, real incident fix: COFORGE
# CE2000 and KPITTECH CE620 both got stopped by the single-bar-low anchor
# right before a genuine reversal, confirmed on real TradingView charts) ──

def test_is_adverse_bar_close():
    assert screener.is_adverse_bar_close(95.0, 100.0) is True
    assert screener.is_adverse_bar_close(105.0, 100.0) is False
    assert screener.is_adverse_bar_close(95.0, 0.0) is False


def test_pool_sl_from_adverse_lows_lone_touch_never_arms():
    assert screener.pool_sl_from_adverse_lows([90.0]) is None


def test_pool_sl_from_adverse_lows_arms_on_a_clustering_second_touch():
    # 90.0 then 90.5 -- within 1% of each other -- clusters, anchor = most
    # recent (90.5).
    assert screener.pool_sl_from_adverse_lows([90.0, 90.5]) == 90.5


def test_pool_sl_from_adverse_lows_far_apart_lows_do_not_cluster():
    # 90.0 then 80.0 -- outside 1% tolerance of each other -- no anchor yet.
    assert screener.pool_sl_from_adverse_lows([90.0, 80.0]) is None


def test_pool_sl_from_adverse_lows_moves_to_a_newer_cluster():
    # 90.0/90.3 cluster first (anchor=90.3); a later 80.0/80.2 cluster
    # supersedes it once ITS OWN second touch lands.
    assert screener.pool_sl_from_adverse_lows([90.0, 90.3, 80.0]) == 90.3
    assert screener.pool_sl_from_adverse_lows([90.0, 90.3, 80.0, 80.2]) == 80.2


def test_pool_sl_from_adverse_lows_respects_custom_tol_and_min_touches():
    assert screener.pool_sl_from_adverse_lows([90.0, 95.0], tol_pct=10.0) == 95.0
    assert screener.pool_sl_from_adverse_lows([90.0, 90.1, 90.2], min_touches=3) == 90.2
    assert screener.pool_sl_from_adverse_lows([90.0, 90.1], min_touches=3) is None


def test_compute_option_premium_target_uses_rr_multiple_off_sl_distance():
    # entry=100, sl=90 -> risk=10, rr=2.0 -> target=100+20=120
    assert screener.compute_option_premium_target(100.0, 90.0, 2.0) == 120.0


def test_compute_option_premium_target_none_without_a_valid_sl():
    assert screener.compute_option_premium_target(100.0, None, 2.0) is None
    # An sl at or above entry can't define a sane risk distance.
    assert screener.compute_option_premium_target(100.0, 100.0, 2.0) is None
    assert screener.compute_option_premium_target(100.0, 105.0, 2.0) is None


def test_check_option_premium_exit_sl_hit():
    assert screener.check_option_premium_exit(90.0, 120.0, 90.0) == "sl"
    assert screener.check_option_premium_exit(90.0, 120.0, 89.99) == "sl"


def test_check_option_premium_exit_target_hit():
    assert screener.check_option_premium_exit(90.0, 120.0, 120.0) == "target"
    assert screener.check_option_premium_exit(90.0, 120.0, 120.01) == "target"


def test_check_option_premium_exit_none_when_neither_hit():
    assert screener.check_option_premium_exit(90.0, 120.0, 105.0) is None


def test_check_option_premium_exit_sl_takes_priority_if_both_somehow_hit():
    # A single tick straddling both (a large gap move) -- the loss-cap wins.
    assert screener.check_option_premium_exit(90.0, 80.0, 85.0) == "sl"


def test_check_option_premium_exit_handles_missing_levels():
    assert screener.check_option_premium_exit(None, None, 100.0) is None
    assert screener.check_option_premium_exit(None, 120.0, 130.0) == "target"
    assert screener.check_option_premium_exit(90.0, None, 80.0) == "sl"


def test_backfill_vwap_from_yahoo_seeds_typical_price_times_volume(monkeypatch):
    import pandas as _pd

    class _FakeYF:
        @staticmethod
        def download(tickers, period, interval, progress, group_by):
            idx = _pd.date_range("2026-08-27 09:15", periods=2, freq="1min", tz="Asia/Kolkata")
            df = _pd.DataFrame({
                "Open": [100.0, 101.0], "High": [102.0, 103.0],
                "Low": [99.0, 100.0], "Close": [101.0, 102.0],
                "Volume": [1000.0, 2000.0],
            }, index=idx)
            df.columns = _pd.MultiIndex.from_product([["VBL.NS"], df.columns])
            return df

    import sys
    monkeypatch.setitem(sys.modules, "yfinance", _FakeYF)

    vwap = screener.VwapState()
    screener.backfill_vwap_from_yahoo(vwap, ["VBL"], screener.CONFIG)

    # bar1: typical=(102+99+101)/3=100.667, vol=1000 -> 100666.67
    # bar2: typical=(103+100+102)/3=101.667, vol=2000 -> 203333.33
    # vwap = (100666.67+203333.33)/(1000+2000) = 101.333...
    assert vwap.current("VBL") == pytest.approx(101.333, abs=0.01)


def test_smabars_seed_close_never_overwrites_live_quote():
    from datetime import datetime as _dt
    bars = screener.SmaBars()
    bars.on_quote("VBL", 100.0, _dt(2026, 8, 26, 9, 30), tf_min=5)   # real live quote first
    bars.seed_close("VBL", _dt(2026, 8, 26, 9, 30), tf_min=5, close=999.0)   # backfill arrives later
    closes = bars.closes("VBL", before=_dt(2026, 8, 26, 9, 40), tf_min=5)
    assert closes == [100.0]   # live data wins, backfill never overwrites it


def test_smabars_seeds_across_the_day_boundary():
    """The whole point of this class vs MinuteBars: previous day's tail and
    today's bars coexist without colliding, ordered correctly by real
    timestamp, not just an "HH:MM" key."""
    from datetime import datetime as _dt
    bars = screener.SmaBars()
    bars.seed_close("VBL", _dt(2026, 8, 25, 15, 25), tf_min=5, close=440.0)   # yesterday's last bar
    bars.seed_close("VBL", _dt(2026, 8, 26, 9, 15), tf_min=5, close=445.0)    # today's first bar
    bars.on_quote("VBL", 446.0, _dt(2026, 8, 26, 9, 20), tf_min=5)
    closes = bars.closes("VBL", before=_dt(2026, 8, 26, 9, 25), tf_min=5)
    assert closes == [440.0, 445.0, 446.0]


def test_smabars_prune_drops_only_old_calendar_days():
    from datetime import datetime as _dt
    bars = screener.SmaBars()
    bars.seed_close("VBL", _dt(2026, 8, 10, 9, 15), tf_min=5, close=400.0)   # old
    bars.seed_close("VBL", _dt(2026, 8, 26, 9, 15), tf_min=5, close=445.0)   # recent
    import unittest.mock as _mock
    with _mock.patch("strategies.oi_orb_screener.screener.datetime") as _dt_mod:
        _dt_mod.now.return_value = _dt(2026, 8, 26, 10, 0, tzinfo=screener.IST)
        bars.prune(keep_days=5)
    remaining = bars.closes("VBL", before=_dt(2026, 8, 27, 0, 0), tf_min=5)
    assert remaining == [445.0]


def test_backfill_sma_bars_from_yahoo_single_ticker_multiindex_columns(monkeypatch):
    import sys
    import types
    from datetime import datetime as _dt

    import pandas as pd

    ist = screener.IST
    idx = pd.DatetimeIndex(
        [_dt(2026, 8, 25, 15, 20), _dt(2026, 8, 26, 9, 15), _dt(2026, 8, 26, 9, 20)],
        tz=ist,
    )
    cols = pd.MultiIndex.from_tuples(
        [("VBL.NS", "Open"), ("VBL.NS", "High"), ("VBL.NS", "Low"),
         ("VBL.NS", "Close"), ("VBL.NS", "Volume")],
        names=["Ticker", "Price"],
    )
    data = [
        [438.0, 441.0, 437.5, 440.0, 0],
        [444.0, 446.0, 443.0, 445.0, 500251],
        [445.5, 447.0, 445.0, 446.5, 100000],
    ]
    fake_df = pd.DataFrame(data, index=idx, columns=cols)

    fake_yf = types.SimpleNamespace(download=lambda *a, **kw: fake_df)
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    sma_bars = screener.SmaBars()
    screener.backfill_sma_bars_from_yahoo(sma_bars, ["VBL"], tf_min=5, lookback_days=5)

    closes = sma_bars.closes("VBL", before=_dt(2026, 8, 26, 9, 25), tf_min=5)
    assert closes == [440.0, 445.0, 446.5]   # spans the day boundary, single-ticker MultiIndex handled
