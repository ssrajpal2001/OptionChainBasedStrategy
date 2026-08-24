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
