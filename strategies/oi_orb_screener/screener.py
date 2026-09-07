"""
strategies/oi_orb_screener/screener.py -- pure/synchronous screener logic.

Faithful port of colab/oi_orb_screener/screener_nse_direct.py's pipeline
(NSE session, F&O universe fetch, OI-spurt fetch, NIFTY regime, shortlist
build, ORB bar accumulation, breakout signal evaluation) -- confirmed
working against real NSE endpoints on 2026-08-24 (NextApi/apiClient/
marketWatchApi for the F&O universe, live-analysis-oi-spurts-underlyings
for OI spurts, allIndices for NIFTY regime).

This module is deliberately kept SYNCHRONOUS (same `requests`-based shape
as the Colab original) so it stays a simple, directly-testable port with
zero asyncio coupling. strategies/oi_orb_screener/engine.py wraps every
call into this module with asyncio.to_thread() at the call site, per this
codebase's own blocking-I/O rule (CLAUDE.md "Development Notes").

CONFIG default REGIME_FILTER_ENABLED=True here (the real spec, unlike the
one-off comparison run done directly in Colab on 2026-08-24) -- the live
book always runs with the regime table enforced unless a deployment's own
strategy_params explicitly override it.

Deliberately excluded from this port (out of scope for the "does live
order placement + LTP tracking work" pass, see engine.py's own docstring):
_notify_browser (Colab/IPython-only), wait_until_actionable's blocking
sleep-loop (engine.py's own async loop handles daily gating instead),
run_screener_and_monitor's full orchestration loop (engine.py owns that,
adapted to asyncio).
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import requests

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

logger = logging.getLogger(__name__)


CONFIG = {
    "OI_SPURT_MIN_PCT": 7.0,
    "PRICE_MOVE_MIN_PCT": 2.0,
    "STOCK_MOVE_ABORT_PCT": 4.0,
    "NIFTY_BULLISH_PCT": 0.3,
    "NIFTY_BEARISH_PCT": -0.3,
    "TOP_N_PER_SIDE": 5,
    "ORB_START": "09:15",
    # 2026-08-24, direct user spec: opening range is 09:15-09:25 (10 min),
    # not the earlier 09:15-09:30 guess -- and the whole scan/shortlist/ORB
    # pipeline now deliberately WAITS until this time to start at all (see
    # SCAN_START below), rather than starting any time after 09:10 and
    # possibly catching a partial live-polled range. This also means the
    # Yahoo backfill (screener.backfill_orb_from_yahoo) is no longer just a
    # "late start" fallback -- it is now the ONLY source of real 09:15-09:25
    # bars, every single day, since live polling never starts before the
    # range has already closed.
    "ORB_END": "09:25",
    # 2026-08-27, direct user spec: TWO scan sessions, not one continuous
    # scan. Session 1 ("morning") is a single point-in-time shortlist build
    # at SCAN_START (09:26) -- whichever stocks qualify AT THAT MOMENT get
    # added; no further morning scanning. Session 2 ("afternoon") re-runs
    # the scan periodically between AFTERNOON_SCAN_START (12:00) and
    # AFTERNOON_SCAN_END -- 2026-09-01, direct user spec: raised from 13:00 to
    # 15:00 to match ENTRY_WINDOW_END (no reason to stop rescanning while
    # entries can still fire) -- ADDING any newly-qualifying stock to the
    # existing shortlist (never dropping one already being watched). NO
    # scanning happens outside these two windows -- entry evaluation (VWAP
    # retest) for whatever's already shortlisted keeps running continuously
    # all day regardless of the scan gaps. See engine.py's
    # _wait_until_actionable / _maybe_run_afternoon_scan.
    "SCAN_START": "09:26",
    # 2026-08-30, direct user spec: instead of the pipeline only ever looking
    # at the OI-spurt list ONCE at SCAN_START, poll the SAME underlying
    # data (universe + OI-spurts) repeatedly through this earlier window and
    # track each stock's RANK by oi_spurt_pct (not the raw 7% threshold) --
    # "rank goes down" is the new alarm signal, not just crossing a fixed %.
    # SCAN_START itself is unchanged (09:26 still locks the real shortlist,
    # "minimal change" per direct user choice) -- this window runs ALONGSIDE
    # it, purely to track momentum and to log every poll's ranked snapshot
    # (see store.record_rank_snapshot) for after-market time optimization.
    "RANK_WINDOW_START": "09:16",
    "RANK_WINDOW_END": "09:30",
    "RANK_POLL_INTERVAL_SEC": 90.0,
    "RANK_TOP_N": 10,
    # 2026-09-07, direct user spec: "instead of checking for only stocks
    # whose spurt is above 7% we will get the top 20 stocks data and save
    # in db ... so that after 1 to 2 week we have all the stocks with
    # their oi spurt to analyse what is best threshold". Deliberately a
    # SEPARATE, purely-observational poll loop from _rank_tracking_loop
    # above (which also DROPS a pre-entry candidate from the shortlist if
    # its rank falls -- a real trading-behavior effect) -- this one only
    # ever logs, all day, never touches self._shortlist_symbols/_rejected.
    # See engine.py's _oi_spurt_history_loop + store.record_oi_spurt_history.
    "OI_SPURT_HISTORY_ENABLED": True,
    "OI_SPURT_HISTORY_START": "09:15",
    "OI_SPURT_HISTORY_END": "15:30",
    "OI_SPURT_HISTORY_POLL_SEC": 60.0,
    "OI_SPURT_HISTORY_TOP_N": 20,
    # 2026-09-07, direct user spec, new strategy "oi_orb_screener_top20":
    # "start scanning for 1st 20 stocks where oi spurt is more in
    # descending order from 9.15 onwards... select those stocks which are
    # 2% high or low from prev day, no oi spurt threshold for condition
    # matching... check for vwap touch within last 15 min." Rank-based
    # top-N (no OI_SPURT_MIN_PCT gate at all -- see build_top20_shortlist,
    # deliberately separate from build_shortlist's threshold-based
    # selection), then the SAME PRICE_MOVE_MIN_PCT (2%) filter.
    "TOP20_RANK_N": 20,
    # Rolling window size for VwapTouchTracker's directional touch check
    # (trailing N CLOSED 1-min candles, plus the live still-forming one).
    "VWAP_TOUCH_WINDOW_MIN": 15,
    # 2026-09-07, direct user spec, REVERSES the 2026-08-27 spec below:
    # "understand stocks which got scanned at 9.25 will be considered for
    # complete day, no need to scan fresh stocks after 9.25am." Default
    # flipped to False -- the shortlist built once at/after ORB_END (09:25)
    # is now used for the whole trading day; _maybe_run_afternoon_scan()
    # itself is left in place (still opt-in via strategy_params) rather than
    # deleted, in case this is revisited again.
    "TWO_SESSION_SCAN_ENABLED": False,
    "AFTERNOON_SCAN_START": "12:00",
    "AFTERNOON_SCAN_END": "15:00",
    "AFTERNOON_SCAN_INTERVAL_SEC": 300.0,
    "ENTRY_WINDOW_START": "09:26",
    # 2026-08-27, direct user spec: "if that stock does not hit vwap till
    # 15.00 it will get cancelled" -- both sessions' candidates share this
    # SAME cutoff (was 10:30). No new entries fire and no further scanning
    # happens after this time; a position already running is unaffected --
    # it still only closes at EOD square-off, target, or SL.
    "ENTRY_WINDOW_END": "15:00",
    "SCORE_WEIGHTS": {"price": 0.25, "oi_spurt": 0.25, "rel_strength": 0.25, "volume": 0.25},
    "POLL_SECONDS": 20,
    "MAX_MONITOR_MINUTES": 90,
    # 2026-08-24, direct user spec, "50% rejection rule": if price rises at
    # least this % above the ORB high (for a CALL-side candidate; symmetric
    # below the ORB low for PUT-side) and then retraces REJECTION_RETRACE_
    # FRACTION of that specific up/down-move before ever cleanly breaking
    # the range, the setup is marked rejected for the rest of the day --
    # even a later real breakout of the range is skipped. Interpretation
    # choice, not fully specified in the source spec (see
    # screener.check_rejection_pattern's own docstring for the exact
    # definition chosen) -- confirm this matches intent before trusting it.
    "REJECTION_MIN_RISE_PCT": 2.0,
    "REJECTION_RETRACE_FRACTION": 0.5,
    # 2026-08-24, direct user spec, exit rule: 8-period SMA on the
    # UNDERLYING STOCK's own closes (not the option premium -- same
    # deliberate spot-based-exit precedent as Liquidity Sweep/Liquidity
    # Trap in this codebase), exit on SMA_EXIT_CONSEC_CLOSES consecutive
    # candle closes on the wrong side of it.
    #
    # 2026-08-26, direct user spec: moved from 1-min to SMA_TF_MIN-min bars
    # (default 5) -- checked against a real live discrepancy where a 1-min
    # SMA exit fired while the user's own chart showed the SMA still on the
    # correct side. SmaBars (below) is seeded via backfill_sma_bars_from_
    # yahoo with real historical closes spanning the previous trading day's
    # tail AND today's own elapsed intraday bars, so an sma_period-bar
    # window is available from the START of the entry window, not ~40min
    # into the day (an 8-period 5-min SMA needs 40min of same-day bars
    # alone). Seeding across the day boundary is deliberate and standard --
    # unlike FVG's own multi-day gap-pool bug (a stale SETUP PATTERN
    # persisting across days), a plain SMA legitimately blends the prior
    # session's tail into an early reading on any real charting platform.
    "SMA_PERIOD": 8,
    "SMA_EXIT_CONSEC_CLOSES": 2,
    "SMA_TF_MIN": 5,
    "SMA_SEED_LOOKBACK_DAYS": 5,
    # 2026-09-06, direct user correction (supersedes the 2026-08-24 2% OTM
    # spec): strike is ATM -- 0% offset from the spot trigger price, which
    # resolve_contract then rounds to the nearest real listed strike step.
    # See the caller in engine.py for where this is actually applied.
    "STRIKE_OTM_PCT": 0.0,
    # Live default (unlike the 2026-08-24 one-off Colab comparison run, which
    # temporarily set this False for calibration only) -- the real regime
    # table stays enforced. strategy_params can still override per-deployment.
    "REGIME_FILTER_ENABLED": True,
    # 2026-08-24, direct user request -- TEMPORARY connectivity-test toggle,
    # NOT a spec change: default False (real ORB_START/ORB_END/ENTRY_WINDOW_
    # START/END timing always applies). Set True only to verify end-to-end
    # order placement + LTP subscription outside the real 09:30-10:30 IST
    # entry window (e.g. testing mid-afternoon right after a deploy) -- ORB
    # freezes immediately off whatever bars exist (real Yahoo backfill still
    # covers the real elapsed 09:15-09:30 session regardless of what time the
    # book actually starts), and the entry-window check is skipped entirely.
    # Turn this back OFF once connectivity is confirmed -- see
    # strategies/oi_orb_screener/engine.py's own docstring.
    "IGNORE_TIME_WINDOWS": False,
    # 2026-08-27, direct user spec: SL/target now track the OPTION's own
    # premium (see compute_option_premium_sl_arm/_target). RR_MULTIPLE is a
    # fixed risk-reward target off the currently-armed SL's own points
    # distance from entry -- fresh, unvalidated default (this strategy still
    # can't be backtested).
    "RR_MULTIPLE": 2.0,
}

NSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    # 2026-08-24: the REAL bug behind every "blocked/rate-limited" failure on
    # EC2 today, confirmed by comparison with a one-off diagnostic that never
    # set this header. Advertising "br" (Brotli) here means NSE may respond
    # with Brotli-compressed data -- decoding it needs the optional `brotli`/
    # `brotlicffi` package, not bundled with `requests` by default and
    # apparently not installed in this environment. The failed decode raised
    # an exception that get_json()'s broad `except Exception: pass` silently
    # swallowed, so every attempt looked identical to a real block/throttle
    # with zero diagnostic signal. data_layer/instrument_registry.py (already
    # proven, live-production code) already deliberately avoids "br" for
    # exactly this reason ("Accept-Encoding": "gzip") -- matching that here.
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
}

FNO_UNIVERSE_URL = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi"
FNO_UNIVERSE_PARAMS = {"functionName": "getIndicesData", "symbol": "SECURITIES IN F&O"}
ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
OI_SPURT_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"


class NSESession:
    """Cookie warm-up + retry -- same pattern as the Colab script's own
    NSESession, confirmed working live 2026-08-24.

    2026-08-24 (later same day) -- ROOT CAUSE FOUND, supersedes an earlier
    wrong diagnosis: every "blocked/rate-limited IP" failure on EC2 today
    was actually NSE_HEADERS advertising "br" (Brotli) in Accept-Encoding.
    Decoding a Brotli response needs the optional `brotli`/`brotlicffi`
    package, not installed in this environment -- the failed decode raised
    an exception that get_json()'s broad `except Exception: pass` silently
    swallowed, so every attempt looked identical to a real IP block with
    zero diagnostic signal (a live side-by-side comparison against a
    one-off diagnostic script that never set this header, and consistently
    succeeded, is what exposed it). Fixed by dropping "br" from
    Accept-Encoding, matching data_layer/instrument_registry.py's own
    already-proven, live-production pattern ("Accept-Encoding": "gzip").
    The warm-up trim (single homepage hit instead of three pages) and the
    lighter outer retry loop (engine.py's _BUILD_SHORTLIST_* constants),
    added earlier the same day chasing what looked like an escalating
    Akamai throttle, turned out not to be the real fix -- left in place
    anyway since sending less automated-looking traffic per attempt is
    still reasonable, but the header fix above is what actually mattered."""

    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(NSE_HEADERS)
        self._warm()

    def _warm(self) -> None:
        try:
            self.s.get("https://www.nseindia.com", timeout=10)
        except Exception as exc:
            # best-effort -- retried on first real 401/403 in get_json() --
            # but 2026-08-24 confirmed a silently-swallowed exception here is
            # exactly what hid the real Brotli-decode bug for hours, looking
            # identical to a genuine IP block. Never let that happen again.
            logger.warning("NSESession._warm() failed (non-fatal, retried later): %r", exc)

    def get_json(self, url, params=None, retries: int = 2):
        for attempt in range(retries):
            try:
                r = self.s.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    return r.json()
                logger.warning("NSESession.get_json(%s) attempt %d/%d: HTTP %d",
                                url, attempt + 1, retries, r.status_code)
                if r.status_code in (401, 403):
                    self._warm()
            except Exception as exc:
                # 2026-08-24: this used to be `except Exception: pass` -- a
                # real Brotli-decode exception fired on EVERY attempt for
                # hours, invisible, making it look exactly like NSE blocking
                # the IP. Always log the real exception now.
                logger.warning("NSESession.get_json(%s) attempt %d/%d raised: %r",
                                url, attempt + 1, retries, exc)
            time.sleep(1.5 * (attempt + 1))
        return None


def fetch_fno_price_universe(nse: "NSESession") -> pd.DataFrame:
    """Response shape: {"data": {"aduCount": {...}, "data": [...rows...]}} --
    confirmed against a real captured request/response pair, 2026-08-24."""
    payload = nse.get_json(FNO_UNIVERSE_URL, params=FNO_UNIVERSE_PARAMS)
    outer = (payload or {}).get("data")
    rows = (outer or {}).get("data") if isinstance(outer, dict) else None
    if payload is None or outer is None or rows is None:
        raise RuntimeError(
            "Could not fetch the F&O price universe from NSE (NextApi/marketWatchApi/"
            "getIndicesData) -- unexpected response shape or blocked/rate-limited IP. "
            f"Raw payload top-level keys: "
            f"{list(payload.keys()) if isinstance(payload, dict) else type(payload)}."
        )
    if not rows:
        raise RuntimeError(
            "NSE returned an empty F&O universe list (0 rows) even though the request "
            f"itself succeeded. symbol value '{FNO_UNIVERSE_PARAMS['symbol']}' may have "
            "changed again on NSE's side."
        )
    df = pd.DataFrame(rows)
    wanted = ["symbol", "lastPrice", "pChange", "open", "dayHigh", "dayLow",
              "previousClose", "totalTradedVolume"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        wanted = [c for c in wanted if c in df.columns]
    return df[wanted].copy()


def fetch_nifty_pchange(nse: "NSESession") -> float:
    payload = nse.get_json(ALL_INDICES_URL)
    if not payload or "data" not in payload:
        raise RuntimeError("Could not fetch NIFTY 50 data from NSE (allIndices).")
    for row in payload["data"]:
        name = str(row.get("index", row.get("indexName", ""))).strip().upper()
        if name == "NIFTY 50":
            for key in ("percentChange", "pChange", "perChange"):
                if key in row:
                    return float(row[key])
    raise RuntimeError("NIFTY 50 not found in allIndices response.")


def _find_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "records", "OISpurts", "oi_spurts", "resultData"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return None


def _find_column(columns, candidates):
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def fetch_oi_spurts_nse(nse: "NSESession") -> pd.DataFrame:
    """symbol/avgInOI field names confirmed against a real live response,
    2026-08-23 (avgInOI verified arithmetically against a real row)."""
    payload = nse.get_json(OI_SPURT_URL)
    if payload is None:
        raise RuntimeError("NSE OI-Spurts endpoint returned nothing after retries.")
    records = _find_records(payload)
    if not records:
        raise RuntimeError("Could not locate the OI-Spurts record list in the NSE response.")
    df = pd.DataFrame(records)
    sym_col = _find_column(df.columns, ["symbol", "underlying", "underlyingValue"])
    pct_col = _find_column(df.columns, [
        "avgInOI", "percentageChangeInOI", "percentChangeOI", "pctChangeInOI",
        "changeInOIPercentage", "perChangeOI", "pChangeOI", "% chng in OI",
    ])
    if sym_col is None or pct_col is None:
        raise RuntimeError(f"Could not auto-detect symbol/%-change-in-OI columns. Columns: {list(df.columns)}.")
    out = pd.DataFrame({
        "symbol": df[sym_col].astype(str).str.upper().str.strip(),
        "oi_spurt_pct": pd.to_numeric(df[pct_col], errors="coerce"),
    }).dropna()
    return out


def build_shortlist(nse: "NSESession", cfg=CONFIG):
    universe = fetch_fno_price_universe(nse)
    nifty_pchange = fetch_nifty_pchange(nse)
    oi_spurts = fetch_oi_spurts_nse(nse)

    merged = universe.merge(oi_spurts, on="symbol", how="inner")
    merged = merged[merged["oi_spurt_pct"] >= cfg["OI_SPURT_MIN_PCT"]]
    merged = merged[merged["pChange"].abs() >= cfg["PRICE_MOVE_MIN_PCT"]]

    if merged.empty:
        return pd.DataFrame(), nifty_pchange

    merged["rel_strength"] = merged["pChange"] - nifty_pchange

    def _minmax(s: pd.Series) -> pd.Series:
        lo, hi = s.min(), s.max()
        if hi - lo < 1e-9:
            return pd.Series(0.5, index=s.index)
        return (s - lo) / (hi - lo)

    w = cfg["SCORE_WEIGHTS"]
    vol_col = "totalTradedVolume" if "totalTradedVolume" in merged.columns else None
    merged["score"] = (
        w["price"] * _minmax(merged["pChange"].abs())
        + w["oi_spurt"] * _minmax(merged["oi_spurt_pct"])
        + w["rel_strength"] * _minmax(merged["rel_strength"].abs())
        + (w["volume"] * _minmax(merged[vol_col]) if vol_col else 0.0)
    )

    bullish = merged[merged["pChange"] >= cfg["PRICE_MOVE_MIN_PCT"]].sort_values(
        "score", ascending=False).head(cfg["TOP_N_PER_SIDE"])
    bearish = merged[merged["pChange"] <= -cfg["PRICE_MOVE_MIN_PCT"]].sort_values(
        "score", ascending=False).head(cfg["TOP_N_PER_SIDE"])

    shortlist = pd.concat([bullish, bearish], ignore_index=True)
    return shortlist, nifty_pchange


def build_top20_shortlist(nse: "NSESession", cfg=CONFIG):
    """2026-09-07, direct user spec, new strategy "oi_orb_screener_top20":
    "start scanning for 1st 20 stocks where oi spurt is more in descending
    order from 9.15 onwards... select those stocks which are 2% high or
    low from prev day, no oi spurt threshold for condition matching."

    Unlike build_shortlist() (which hard-gates on OI_SPURT_MIN_PCT before
    ranking anything), this ranks the WHOLE F&O universe by oi_spurt_pct
    descending and takes exactly the top TOP20_RANK_N (default 20)
    regardless of their actual % -- a pure rank cutoff, same underlying
    merge/sort poll_oi_rank() already does, but exposed here as its own
    named entry point since this strategy's semantics ("top 20, no
    threshold") are conceptually distinct from that function's own
    "momentum tracking, not a real shortlist" purpose.

    Returns (top20, tradeable, nifty_pchange):
      top20     -- ALL 20 ranked stocks, unfiltered (for daily DB
                   registration/backtesting -- store.record_top20_daily_scan
                   logs every one of these, traded or not).
      tradeable -- the subset of top20 that ALSO passes the price-move
                   filter (|pChange| >= PRICE_MOVE_MIN_PCT) -- only these
                   go on to the VWAP-touch check."""
    universe = fetch_fno_price_universe(nse)
    nifty_pchange = fetch_nifty_pchange(nse)
    oi_spurts = fetch_oi_spurts_nse(nse)
    merged = universe.merge(oi_spurts, on="symbol", how="inner")
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame(), nifty_pchange
    merged = merged.sort_values("oi_spurt_pct", ascending=False).reset_index(drop=True)
    merged["rank"] = merged.index + 1
    top_n = int(cfg.get("TOP20_RANK_N", 20) or 20)
    top20 = merged.head(top_n).copy()
    tradeable = top20[top20["pChange"].abs() >= cfg.get("PRICE_MOVE_MIN_PCT", 2.0)].copy()
    return top20, tradeable, nifty_pchange


class VwapTouchTracker:
    """2026-09-07, direct user spec, new strategy "oi_orb_screener_top20":
    "now if any stocks which passes this criteria is checked for vwap
    touch within last 15 min." Followed by the directional clarification:
    "for long ltp should come from above vwap and low should touch vwap,
    for short ltp should come from below vwap and high should touch
    vwap" -- the same CALL-arms-above/PUT-arms-below directionality
    check_vwap_retest_entry() already uses, but expressed as a rolling
    time-window touch check instead of an arm-then-retest state machine.

    Maintains a FIFO rolling window of the trailing N (default 15) CLOSED
    1-min candles -- each new candle both appends and flushes the oldest,
    exactly matching the direct user spec ("every min we have new 16th
    candle which is the 15th candle and 1st candle is flushed"). The
    still-forming ("current") candle's own running high/low is tracked
    separately and checked on every live tick, per the same spec ("also
    tick by tick").

    A "touch" is a genuine price-level cross into VWAP -- CALL: some
    candle's LOW <= vwap (a dip down to it from above); PUT: some
    candle's HIGH >= vwap (a rise up to it from below) -- checked across
    both the closed-candle window AND the live current candle."""

    def __init__(self, window_min: int = 15) -> None:
        self._window_min = window_min
        self._closed: deque = deque(maxlen=window_min)
        self._cur_key: Optional[tuple] = None   # (hour, minute) of the still-forming candle
        self._cur_high: float = float("-inf")
        self._cur_low: float = float("inf")

    def on_tick(self, ts: datetime, ltp: float) -> None:
        key = (ts.hour, ts.minute)
        if self._cur_key is None:
            self._cur_key = key
            self._cur_high = self._cur_low = ltp
            return
        if key != self._cur_key:
            # The still-forming candle just closed -- push it into the
            # rolling window (deque's own maxlen does the "flush oldest"
            # part automatically) and start a fresh one.
            self._closed.append((self._cur_high, self._cur_low))
            self._cur_key = key
            self._cur_high = self._cur_low = ltp
        else:
            self._cur_high = max(self._cur_high, ltp)
            self._cur_low = min(self._cur_low, ltp)

    def check_touch(self, ltp: float, vwap: float) -> Optional[str]:
        """Returns "CALL" or "PUT" the instant this symbol's current side +
        rolling-window touch condition is satisfied, else None. Directional
        side is derived from ltp vs vwap right now (same convention
        side_from_pchange/check_vwap_retest_entry already use elsewhere)."""
        if vwap <= 0:
            return None
        if ltp > vwap:
            lows = [lo for _, lo in self._closed]
            if self._cur_key is not None:
                lows.append(self._cur_low)
            if any(lo <= vwap for lo in lows):
                return "CALL"
        elif ltp < vwap:
            highs = [hi for hi, _ in self._closed]
            if self._cur_key is not None:
                highs.append(self._cur_high)
            if any(hi >= vwap for hi in highs):
                return "PUT"
        return None


def sharp_bear_zones(bars_3m: list) -> List[dict]:
    """2026-08-31, direct user spec: replaces the VWAP-retest entry mechanic
    for OI-ORB's CALL side (bullish underlying) with the same bear-trap
    zone detection validated in this session's own backtests (scripts/
    oi_orb_bear_trap_coforge_backtest.py / _fullday.py) -- ported here
    VERBATIM (same logic, same real functions reused: find_all_setups,
    strict-adjacency sweep, unbounded SL-hit scan) so the live engine can't
    behaviorally drift from what was actually validated.

    `bars_3m` is a live, growing list of strategies.liquidity_trap.detector.
    Bar objects (3-min OHLC, built from the underlying's own polled price) --
    engine.py re-scans the whole list on every new 3-min bar close, same
    "re-scan growing bar list" pattern strategies/liquidity_trap/engine.py
    itself already uses to guarantee zero drift from a validated design."""
    from strategies.core.trap_zone_utils import find_all_setups
    setups = [s for s in find_all_setups(bars_3m) if s.direction == "BEAR"]
    out = []
    for s in setups:
        ref = bars_3m[s.ref_idx]
        nxt = bars_3m[s.locked_idx]
        lock_ts = None
        for k in range(s.locked_idx + 1, len(bars_3m)):
            if bars_3m[k].high > ref.high:
                lock_ts = bars_3m[k].ts
                break
        if lock_ts is None:
            continue   # SL hasn't broken yet -- not a confirmed trap
        out.append(dict(
            zone_lo=nxt.low, zone_hi=ref.close, entry_line=ref.low,
            lock_ts=lock_ts, ref_ts=ref.ts, ref_idx=s.ref_idx,
        ))
    return out


def bull_trap_zones(bars_3m: list) -> List[dict]:
    """Mirror of sharp_bear_zones for OI-ORB's PUT side (bearish underlying),
    ported verbatim from scripts/oi_orb_bull_trap_tatapower_backtest.py."""
    from strategies.core.trap_zone_utils import find_all_setups
    setups = [s for s in find_all_setups(bars_3m) if s.direction == "BULL"]
    out = []
    for s in setups:
        ref = bars_3m[s.ref_idx]
        nxt = bars_3m[s.locked_idx]
        lock_ts = None
        for k in range(s.locked_idx + 1, len(bars_3m)):
            if bars_3m[k].low < ref.low:
                lock_ts = bars_3m[k].ts
                break
        if lock_ts is None:
            continue
        out.append(dict(
            zone_lo=ref.close, zone_hi=nxt.high, entry_line=ref.high,
            lock_ts=lock_ts, ref_ts=ref.ts, ref_idx=s.ref_idx,
        ))
    return out


def poll_oi_rank(nse: "NSESession", cfg=CONFIG, top_n: Optional[int] = None) -> pd.DataFrame:
    """2026-08-30, direct user spec: a single poll of the OI-Spurt + price
    universe, RANKED by oi_spurt_pct descending -- deliberately does NOT
    apply build_shortlist's OI_SPURT_MIN_PCT/PRICE_MOVE_MIN_PCT threshold
    filters ("instead of OI percent we can use OI change rank"). Returns the
    top-N rows with an explicit `rank` column (1 = highest OI-spurt %).

    top_n: explicit override (2026-09-07, added for the full-day OI-spurt
    history collector, which wants top 20 -- distinct from RANK_TOP_N's own
    10, used by the narrow 09:16-09:30 rank-tracking window). Falls back to
    cfg["RANK_TOP_N"] when not passed, unchanged behavior for that caller.

    Pure/synchronous, same shape as build_shortlist -- callers wrap with
    asyncio.to_thread() per this codebase's blocking-I/O rule."""
    universe = fetch_fno_price_universe(nse)
    oi_spurts = fetch_oi_spurts_nse(nse)
    merged = universe.merge(oi_spurts, on="symbol", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=["symbol", "rank", "oi_spurt_pct", "pChange"])
    merged = merged.sort_values("oi_spurt_pct", ascending=False).reset_index(drop=True)
    merged["rank"] = merged.index + 1
    if top_n is None:
        top_n = int(cfg.get("RANK_TOP_N", 10) or 10)
    return merged.head(int(top_n))


class MinuteBars:
    """Buckets polled (symbol, price, timestamp) into 1-min OHLC bars in-process."""

    def __init__(self) -> None:
        self.bars: dict = defaultdict(dict)

    def on_quote(self, symbol: str, price: float, ts: datetime) -> None:
        if price <= 0:
            return
        key = ts.strftime("%H:%M")
        sym_bars = self.bars[symbol]
        b = sym_bars.get(key)
        if b is None:
            sym_bars[key] = {"o": price, "h": price, "l": price, "c": price}
        else:
            b["h"] = max(b["h"], price)
            b["l"] = min(b["l"], price)
            b["c"] = price

    def orb(self, symbol: str, start: str, end: str):
        keys = sorted(k for k in self.bars.get(symbol, {}) if start <= k < end)
        if not keys:
            return None, None
        highs = [self.bars[symbol][k]["h"] for k in keys]
        lows = [self.bars[symbol][k]["l"] for k in keys]
        return max(highs), min(lows)

    def closes(self, symbol: str, after: str = "", before: str = "") -> list:
        """Ordered list of CLOSE prices for symbol, oldest first, optionally
        restricted to bars with a time-key strictly after `after` (e.g. the
        ORB end, so pre-range bars never pollute an SMA meant to describe
        only the post-range trend) and/or strictly before `before` (e.g.
        the CURRENT, still-forming minute, so an in-progress bar's partial
        close is never misread as a completed candle's real close)."""
        keys = sorted(k for k in self.bars.get(symbol, {})
                       if (not after or k > after) and (not before or k < before))
        return [self.bars[symbol][k]["c"] for k in keys]


class SmaBars:
    """Rolling per-symbol CLOSE history used ONLY for the SMA exit
    (2026-08-26), bucketed to `tf_min`-minute bars and keyed by a
    (date, bucket-start) string so multi-day data can never collide the way
    a bare "HH:MM" key (MinuteBars' own scheme, correct for its own
    deliberately intraday-only ORB use) would. Deliberately NOT reset daily
    -- a plain SMA is legitimately continuous across a session boundary on
    any real chart; see this module's own SMA_PERIOD config comment for why
    that's a different case from FVG's multi-day gap-pool bug."""

    def __init__(self) -> None:
        self.bars: dict = defaultdict(dict)   # symbol -> {bucket_key: close}

    @staticmethod
    def _bucket_key(ts: datetime, tf_min: int) -> str:
        floored_minute = (ts.minute // tf_min) * tf_min
        return f"{ts.strftime('%Y-%m-%d')} {ts.hour:02d}:{floored_minute:02d}"

    def on_quote(self, symbol: str, price: float, ts: datetime, tf_min: int) -> None:
        """Live poll update -- last quote in a bucket wins as its close,
        same convention as MinuteBars.on_quote."""
        if price <= 0:
            return
        self.bars[symbol][self._bucket_key(ts, tf_min)] = price

    def seed_close(self, symbol: str, ts: datetime, tf_min: int, close: float) -> None:
        """Historical backfill seed. Never overwrites a bucket the live poll
        loop has already updated this session -- live data always wins over
        a backfilled seed for the same bucket."""
        key = self._bucket_key(ts, tf_min)
        self.bars[symbol].setdefault(key, close)

    def closes(self, symbol: str, before: datetime, tf_min: int) -> list:
        """Ordered list of CLOSE prices for symbol, oldest first, strictly
        before the bucket containing `before` -- an in-progress bucket's
        partial close is never misread as a completed candle's real close."""
        cutoff = self._bucket_key(before, tf_min)
        keys = sorted(k for k in self.bars.get(symbol, {}) if k < cutoff)
        return [self.bars[symbol][k] for k in keys]

    def prune(self, keep_days: int) -> None:
        """Bound memory -- drop any bucket older than `keep_days` calendar
        days. Safe to call once per new trading day; never mid-session."""
        cutoff_date = (datetime.now(IST) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        for symbol in list(self.bars.keys()):
            self.bars[symbol] = {k: v for k, v in self.bars[symbol].items() if k[:10] >= cutoff_date}


def backfill_sma_bars_from_yahoo(sma_bars: "SmaBars", symbols, tf_min: int = 5,
                                  lookback_days: int = 5) -> None:
    """Best-effort seed of SmaBars with real historical closes -- both the
    previous trading day(s)' tail AND today's own elapsed intraday bars --
    so the SMA exit has a full sma_period-bar window from the START of the
    entry window, not ~40min into the day. Mirrors backfill_orb_from_
    yahoo's own yfinance pattern (same library, same df[ticker] MultiIndex-
    column handling already confirmed live for this file). Deliberately
    degrades safely on any failure -- check_sma_exit's own "insufficient
    data -> no exit yet" guard already handles a partial/empty seed
    correctly, so a failed backfill just means the SMA exit activates later
    in the day instead of firing on bad/guessed data."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("backfill_sma_bars_from_yahoo: yfinance not installed -- "
                        "SMA exit will only warm up from live intraday polling.")
        return
    try:
        tickers = [s + ".NS" for s in symbols]
        df = yf.download(tickers, period=f"{lookback_days}d", interval=f"{tf_min}m",
                          progress=False, group_by="ticker")
        filled = 0
        for sym, ticker in zip(symbols, tickers):
            try:
                sub = df[ticker]
            except Exception as exc:
                logger.warning("backfill_sma_bars_from_yahoo: no data for %s (%s): %r", sym, ticker, exc)
                continue
            sym_filled = 0
            for ts, row in sub.iterrows():
                if pd.isna(row.get("Close")):
                    continue
                ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
                sma_bars.seed_close(sym, ts_ist, tf_min, float(row["Close"]))
                sym_filled += 1
            filled += sym_filled
            if sym_filled == 0:
                logger.warning("backfill_sma_bars_from_yahoo: %s (%s) returned data but 0 bars landed.",
                                sym, ticker)
        logger.info("backfill_sma_bars_from_yahoo: seeded %d total %d-min bars across %d symbols.",
                     filled, tf_min, len(symbols))
    except Exception as exc:
        logger.warning("backfill_sma_bars_from_yahoo: failed entirely: %r", exc)


def compute_sma(closes: list, period: int) -> Optional[float]:
    """Simple moving average of the LAST `period` closes. None if fewer
    than `period` closes exist yet -- never guess a partial-window SMA."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    return sum(window) / period


def check_rejection_pattern(extreme_since_orb: float, orb_level: float, current_ltp: float,
                             side: str, min_rise_pct: float, retrace_fraction: float) -> bool:
    """2026-08-24, direct user spec ("50% rejection rule"): "If the stock
    rises (e.g., by 2%), then retraces 50% of that move before breaking the
    9:25 AM high, do not enter the trade." This is an interpretation choice
    -- the source description does not pin down the exact reference point
    for the initial "rise" -- chosen here as: how far price has pushed
    beyond the ORB level itself (orb_high for CALL, orb_low for PUT), as a
    %% of that orb_level. If that push reaches min_rise_pct and price has
    since given back retrace_fraction (default 50%%) of the move from
    orb_level to its post-ORB extreme, the setup counts as rejected --
    caller is responsible for remembering this (a stock, once rejected,
    should not be re-evaluated later even if it goes on to legitimately
    break the range -- see engine.py's own _rejected set).

    side: "CALL" (watching orb_level as a HIGH, extreme_since_orb is the
    running post-ORB MAX) or "PUT" (orb_level is a LOW, extreme_since_orb
    is the running post-ORB MIN)."""
    if side == "CALL":
        move = extreme_since_orb - orb_level
        if move <= 0 or orb_level <= 0:
            return False
        rise_pct = move / orb_level * 100.0
        if rise_pct < min_rise_pct:
            return False
        retrace_level = extreme_since_orb - retrace_fraction * move
        return current_ltp <= retrace_level
    else:  # PUT
        move = orb_level - extreme_since_orb
        if move <= 0 or orb_level <= 0:
            return False
        fall_pct = move / orb_level * 100.0
        if fall_pct < min_rise_pct:
            return False
        retrace_level = extreme_since_orb + retrace_fraction * move
        return current_ltp >= retrace_level


class VwapState:
    """2026-08-27, direct user spec: replaces the ORB-breach entry trigger
    with a session-anchored VWAP retest, and replaces the S&R (R1/S1/R2/S2)
    SL with a VWAP-relative structural stop. Session VWAP = cumulative(price
    x volume) / cumulative(volume), anchored to market open (ORB_START).

    Built incrementally from POLL-TO-POLL (price, cumulative-volume)
    samples -- the live poll (fetch_fno_price_universe) already carries a
    real cumulative `totalTradedVolume` field per symbol (same one the
    existing volume-confirmation filter already tracks), so the caller
    feeds this the SAME poll-to-poll volume DELTA it already computes for
    that filter, never the raw cumulative number itself."""

    def __init__(self) -> None:
        self._num: dict = defaultdict(float)   # symbol -> cumulative price*volume
        self._den: dict = defaultdict(float)   # symbol -> cumulative volume

    def seed(self, symbol: str, price_vol_sum: float, vol_sum: float) -> None:
        """Adds to (never replaces) whatever's already accumulated -- used
        once by backfill_vwap_from_yahoo before live polling begins for the
        day, so the running VWAP has a real 09:15-start basis instead of
        only starting from whatever time live polling first began."""
        if vol_sum <= 0:
            return
        self._num[symbol] += price_vol_sum
        self._den[symbol] += vol_sum

    def update(self, symbol: str, price: float, volume_delta: float) -> None:
        if price <= 0 or volume_delta <= 0:
            return
        self._num[symbol] += price * volume_delta
        self._den[symbol] += volume_delta

    def current(self, symbol: str) -> Optional[float]:
        den = self._den.get(symbol, 0.0)
        if den <= 0:
            return None
        return self._num[symbol] / den


def backfill_vwap_from_yahoo(vwap: "VwapState", symbols, cfg=CONFIG) -> None:
    """Best-effort seed of VwapState with real historical (typical price x
    volume) from ORB_START (09:15) up to now -- mirrors backfill_orb_from_
    yahoo/backfill_sma_bars_from_yahoo's own yfinance pattern exactly (same
    library, same df[ticker] MultiIndex-column handling already confirmed
    live for this file). Without this, the very first live poll's cumulative
    totalTradedVolume already bundles up everything traded since 09:15 in
    one lump with no weighted-price breakdown, so VWAP would start life
    wrong for the first ~10-15 minutes. Degrades safely on any failure --
    VwapState.current() returning None (insufficient data) already means
    "not ready yet" everywhere it's checked."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("backfill_vwap_from_yahoo: yfinance not installed -- "
                        "VWAP will only warm up from live intraday polling.")
        return
    try:
        tickers = [s + ".NS" for s in symbols]
        df = yf.download(tickers, period="1d", interval="1m", progress=False, group_by="ticker")
        filled = 0
        for sym, ticker in zip(symbols, tickers):
            try:
                sub = df[ticker]
            except Exception as exc:
                logger.warning("backfill_vwap_from_yahoo: no data for %s (%s): %r", sym, ticker, exc)
                continue
            price_vol_sum = 0.0
            vol_sum = 0.0
            sym_filled = 0
            for ts, row in sub.iterrows():
                if any(pd.isna(row.get(c)) for c in ("High", "Low", "Close", "Volume")):
                    continue
                ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
                key = ts_ist.strftime("%H:%M")
                if key >= cfg.get("ORB_START", "09:15") and float(row["Volume"]) > 0:
                    typical = (float(row["High"]) + float(row["Low"]) + float(row["Close"])) / 3.0
                    price_vol_sum += typical * float(row["Volume"])
                    vol_sum += float(row["Volume"])
                    sym_filled += 1
            if vol_sum > 0:
                vwap.seed(sym, price_vol_sum, vol_sum)
            filled += sym_filled
            if sym_filled == 0:
                logger.warning("backfill_vwap_from_yahoo: %s (%s) returned data but 0 bars landed.",
                                sym, ticker)
        logger.info("backfill_vwap_from_yahoo: seeded %d total 1-min bars across %d symbols.",
                     filled, len(symbols))
    except Exception as exc:
        logger.warning("backfill_vwap_from_yahoo: failed entirely: %r", exc)


def replay_vwap_retest_from_bars(bars: list, side: str, orb_start: str,
                                  min_gap_pct: float = 0.15) -> dict:
    """2026-09-07, direct user spec: "when we started the application and
    stocks were already there in the scan list it should have called
    intraday historical data and found if it satisfied the vwap touch
    concept or not -- if yes, immediately trade should have started."

    Pure, unit-testable replay of the SAME check_vwap_retest_entry() state
    machine the live engine uses, bar-by-bar, over already-fetched intraday
    1-min bars -- so a stock that only enters the shortlist well after
    market open (a restart, or the 12:00-15:00 afternoon rescan) doesn't
    start its arm/retest state cold at that moment, silently discarding
    whatever genuine cross-and-retest already happened in the real market
    before this book ever started watching it (confirmed live: MANAPPURAM
    sat within a few paise of its own VWAP the entire time since being
    shortlisted at 09:51, having been shortlisted 26 minutes after the
    entry window opened, with no way to know if today's real retest had
    already come and gone in that gap).

    bars: chronologically-ordered dicts with high/low/close/volume (same
    shape screener already gets from yfinance rows). Maintains its own
    running typical-price VWAP exactly like VwapState.update() does, so the
    replayed vwap values match what backfill_vwap_from_yahoo would have
    seeded, then feeds each bar's CLOSE through check_vwap_retest_entry --
    the exact same function and threshold the live tick loop uses, just
    fed historical closes instead of live ticks.

    Returns {"armed": bool, "fired": bool, "fire_ts": str|None,
    "fire_price": float|None, "final_vwap": float|None, "bars_replayed": int}.
    "fired" means a genuine retest already completed somewhere in this
    history -- the caller should treat this exactly like a live fire
    (immediate entry), not just seed the arm state."""
    cum_pv = 0.0
    cum_v = 0.0
    armed = False
    fired = False
    fire_ts: Optional[str] = None
    fire_price: Optional[float] = None
    final_vwap: Optional[float] = None
    bars_replayed = 0
    for b in bars:
        ts = b.get("ts")
        key = ts if isinstance(ts, str) else (ts.strftime("%H:%M") if ts is not None else None)
        if key is not None and key < orb_start:
            continue
        vol = float(b.get("volume", 0) or 0)
        if vol <= 0:
            continue
        high, low, close = float(b["high"]), float(b["low"]), float(b["close"])
        typical = (high + low + close) / 3.0
        cum_pv += typical * vol
        cum_v += vol
        if cum_v <= 0:
            continue
        vwap = cum_pv / cum_v
        final_vwap = vwap
        bars_replayed += 1
        if fired:
            continue  # keep accumulating vwap for final_vwap, but the fire itself is one-shot
        armed, fire = check_vwap_retest_entry(side, close, vwap, armed, min_gap_pct)
        if fire:
            fired = True
            fire_ts = key
            fire_price = close
    return {
        "armed": armed, "fired": fired, "fire_ts": fire_ts, "fire_price": fire_price,
        "final_vwap": final_vwap, "bars_replayed": bars_replayed,
    }


def historical_vwap_retest_check(symbols_sides: dict, cfg=CONFIG) -> dict:
    """Bulk Yahoo-backed wrapper around replay_vwap_retest_from_bars, one
    yfinance.download() call for every symbol (mirrors backfill_vwap_from_
    yahoo/backfill_orb_from_yahoo's own bulk-fetch pattern exactly, so this
    doesn't add a second per-symbol network round trip on top of those).

    symbols_sides: {symbol: "CALL"|"PUT"} for every symbol to check.
    Returns {symbol: replay_vwap_retest_from_bars(...) result} -- a symbol
    missing from the result (or whose value is None) means Yahoo had no
    usable data for it; caller should treat that exactly like "not fired,
    not armed" (safe default, same as a cold start) rather than raising."""
    out: dict = {}
    if not symbols_sides:
        return out
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("historical_vwap_retest_check: yfinance not installed -- "
                        "no historical retest check possible, arm state starts cold.")
        return out
    symbols = list(symbols_sides.keys())
    try:
        tickers = [s + ".NS" for s in symbols]
        df = yf.download(tickers, period="1d", interval="1m", progress=False, group_by="ticker")
    except Exception as exc:
        logger.warning("historical_vwap_retest_check: yfinance download failed entirely: %r", exc)
        return out
    orb_start = cfg.get("ORB_START", "09:15")
    for sym, ticker in zip(symbols, tickers):
        try:
            sub = df[ticker]
        except Exception as exc:
            logger.warning("historical_vwap_retest_check: no data for %s (%s): %r", sym, ticker, exc)
            continue
        bars = []
        for ts, row in sub.iterrows():
            if any(pd.isna(row.get(c)) for c in ("High", "Low", "Close", "Volume")):
                continue
            ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
            bars.append({
                "ts": ts_ist.strftime("%H:%M"),
                "high": float(row["High"]), "low": float(row["Low"]),
                "close": float(row["Close"]), "volume": float(row["Volume"]),
            })
        bars.sort(key=lambda b: b["ts"])
        result = replay_vwap_retest_from_bars(bars, symbols_sides[sym], orb_start)
        out[sym] = result
        if result["fired"]:
            logger.info(
                "historical_vwap_retest_check: %s %s ALREADY RETESTED at %s (price=%.2f) in "
                "today's real history -- treat as immediate entry.",
                sym, symbols_sides[sym], result["fire_ts"], result["fire_price"],
            )
    return out


def side_from_pchange(pchange: float) -> str:
    """Gainer (pChange > 0) -> CALL candidate; loser -> PUT candidate. Same
    bullish/bearish split build_shortlist() already used to bucket the
    shortlist itself."""
    return "CALL" if pchange > 0 else "PUT"


def side_allowed_by_regime(side: str, regime: Optional[str], regime_filter_on: bool = True) -> bool:
    """Same regime table evaluate_breakout() already enforces: Bullish day
    -> CALL and PUT both tradeable. Bearish day -> CALL ignored, PUT
    tradeable. Neutral day -> nothing tradeable (unless the filter is
    explicitly off)."""
    if not regime_filter_on:
        return True
    if regime is None or regime == "neutral":
        return False
    if side == "CALL":
        return regime == "bullish"
    return True   # PUT tradeable on both bullish and bearish days


def check_vwap_retest_entry(side: str, ltp: float, vwap: float, armed: bool,
                             min_gap_pct: float) -> tuple:
    """2026-08-27, direct user spec: "wait for the stock to come back to
    vwap then we enter" -- not an ORB breach.

    2026-08-28 correction, direct user spec: arming is now PURE DIRECTIONAL
    positioning relative to VWAP -- no minimum-gap threshold. "If we are
    going long it should [be] above vwap, and vice versa[;] that is [the]
    criteria for vwap, no threshold required." CALL arms the instant
    ltp > vwap (any amount); PUT arms the instant ltp < vwap (any amount).
    `min_gap_pct` is kept as a parameter (still threaded through
    book_manager.py/engine.py/the dashboard) purely so this isn't an
    invasive plumbing change during live market hours, but it is no longer
    read anywhere in this function -- a future cleanup pass can remove the
    parameter/config knob entirely once there's a safe window to also touch
    the UI/persistence layer.

    Once armed, entry fires the instant price touches back to VWAP FROM
    THE ARMED DIRECTION -- CALL: ltp <= vwap (price was above, comes down
    onto it); PUT: ltp >= vwap (price was below, comes up onto it) -- a
    simple tick-based touch, no candle-close confirmation.

    Returns (new_armed, fire_entry). Idempotent: once fired, the caller is
    responsible for marking the (symbol, side) as already-fired so this
    isn't called again for it same day."""
    if vwap <= 0:
        return armed, False
    if side == "CALL":
        if not armed:
            return (ltp > vwap), False
        return armed, (ltp <= vwap)
    else:  # PUT
        if not armed:
            return (ltp < vwap), False
        return armed, (ltp >= vwap)


def compute_option_premium_sl_arm(bar_close: float, vwap_at_close: float,
                                   bar_low: float) -> Optional[float]:
    """2026-08-27, direct user spec: "checking for target and SL in stock,
    change it to the option which we are taking" -- SL/target now track the
    OPTION'S OWN premium (its own VWAP, its own bars), not the underlying
    stock's spot price. OI-ORB only ever BUYS options (side="CALL" -> bought
    CE, side="PUT" -> bought PE) -- a bought option's owner ALWAYS wants its
    OWN premium to rise, regardless of CE/PE, so this is side-INDEPENDENT
    (unlike the old stock-spot SL, where CALL/PUT genuinely pointed opposite
    directions on the underlying). Adverse = a vwap_sl_tf_minutes bar closes
    BELOW the option's own vwap -- re-arms the SL to THAT bar's own LOW,
    replacing whatever was armed before (re-arms on every adverse bar, not
    just the first). Returns None (no re-arm) on a bar that closed on the
    favorable side.

    2026-08-28 note: this single-bar-low anchor is kept as a small,
    independently tested primitive (mirrors strategies/oi_flow/detector.py's
    own swing_low() vs pool_swing_low() split) -- the SL actually armed live
    now goes through pool_sl_from_adverse_lows() below, not this function
    directly. See that function's docstring for the real incident that
    prompted the change."""
    if vwap_at_close <= 0:
        return None
    return bar_low if bar_close < vwap_at_close else None


def is_adverse_bar_close(bar_close: float, vwap_at_close: float) -> bool:
    """True if a vwap_sl_tf_minutes bar closed BELOW the option's own broker-
    ATP VWAP -- the trigger to consider that bar's own LOW as a fresh
    SL-anchor candidate. Split out of compute_option_premium_sl_arm so the
    caller can grow a running history of candidate lows for
    pool_sl_from_adverse_lows() below."""
    return vwap_at_close > 0 and bar_close < vwap_at_close


_SL_POOL_TOL_PCT_DEFAULT = 1.0
_SL_POOL_MIN_TOUCHES_DEFAULT = 2


def pool_sl_from_adverse_lows(adverse_lows: List[float], tol_pct: float = _SL_POOL_TOL_PCT_DEFAULT,
                               min_touches: int = _SL_POOL_MIN_TOUCHES_DEFAULT) -> Optional[float]:
    """2026-08-28, direct user chart review of a real incident: two real
    trades the same session (COFORGE CE2000, KPITTECH CE620) both got
    stopped out by compute_option_premium_sl_arm's single-bar-low anchor
    right before a genuine reversal -- confirmed on real TradingView charts
    (COFORGE rallied ~13pts, KPITTECH reclaimed its own VWAP, both shortly
    after the SL hit). A lone adverse bar's own low is ordinary intraday
    noise, not a real defended level -- same root cause and same fix shape
    as OI-Flow's own real 2026-08-19 incident
    (strategies/oi_flow/detector.py's pool_swing_low), reimplemented fresh
    here per this strategy's standalone mandate. Adapted from OI-Flow's flat
    ₹ tolerance to a PERCENTAGE tolerance because OI-ORB trades many
    different F&O stocks at wildly different premium scales in the same
    session (₹15 KPITTECH vs ₹400+ OFSS on this very day) -- one flat ₹
    tolerance can't fit both.

    `adverse_lows` is the full chronological history of every adverse bar's
    own low for THIS position since entry (caller-maintained, oldest
    first -- reset on every fresh entry, never carried across positions).
    Walks them in order; each low's cluster size = itself + every EARLIER
    low within tol_pct% of it. The most recent low whose own cluster
    reaches min_touches becomes the active anchor -- i.e. the SL only arms
    once at least two separate adverse bars have found roughly the same
    floor, not on the very first dip. Returns None if no low has ever
    reached that threshold yet -- callers must treat this exactly like "not
    armed yet" (no fallback to the single most recent low)."""
    active: Optional[float] = None
    for i, low in enumerate(adverse_lows):
        tol = abs(low) * (tol_pct / 100.0)
        cluster = 1 + sum(1 for prior in adverse_lows[:i] if abs(prior - low) <= tol)
        if cluster >= min_touches:
            active = low
    return active


def compute_option_premium_target(entry_price: float, sl_level: Optional[float],
                                   rr_multiple: float) -> Optional[float]:
    """2026-08-27, direct user spec: a fixed risk-reward target off the
    CURRENTLY ARMED SL's own points-distance from entry -- re-computed
    every time the SL re-arms, so it shifts alongside it. None until a real
    (below-entry) SL has armed at least once -- a bar_low sitting AT or
    ABOVE entry can't define a sane risk distance."""
    if sl_level is None or entry_price <= 0 or sl_level >= entry_price:
        return None
    risk = entry_price - sl_level
    return entry_price + rr_multiple * risk


def check_option_premium_exit(sl_level: Optional[float], target_level: Optional[float],
                               ltp: float) -> Optional[str]:
    """Tick-basis check (not candle-close). Returns "sl", "target", or None.
    SL checked first -- if a single tick somehow straddles both (a large gap
    move), the loss-cap takes priority over locking a gain."""
    if sl_level is not None and ltp <= sl_level:
        return "sl"
    if target_level is not None and ltp >= target_level:
        return "target"
    return None


def check_sma_exit(closes: list, sma_period: int, consec_closes: int, side: str) -> bool:
    """2026-08-24, direct user spec: exit when `consec_closes` (default 2)
    consecutive candle CLOSES land on the wrong side of an `sma_period`
    (default 8) SMA of the underlying STOCK's own closes (not the option
    premium). side="CALL" (bought on a gainer) exits when closes are BELOW
    the SMA; side="PUT" (bought on a loser) exits when closes are ABOVE it.

    2026-08-26 fix (real incident): this originally checked the last
    `consec_closes` closes against ONE current SMA value (computed from the
    most recent `sma_period` closes) -- documented at the time as a
    simplification "to revisit if real forward data shows it matters". It
    did: a real VBL PE position exited on "sma_exit", but the user's own
    chart showed the SMA still on the correct side of price at that moment.
    Root cause -- the single "current" SMA window INCLUDES the very closes
    being tested against it, so as price moved, the SMA was still dragging
    toward those same recent closes rather than reflecting where the SMA
    line actually sat at each of those historical bars on a real chart. A
    real chart's SMA is a genuinely ROLLING value, recomputed fresh at every
    bar from THAT bar's own trailing window -- this now matches that: each
    of the last `consec_closes` closes is compared against the SMA as it
    stood AT THAT BAR (its own trailing `sma_period`-close window), not a
    single snapshot borrowed from the most recent bar."""
    if len(closes) < sma_period + consec_closes - 1:
        return False
    for i in range(len(closes) - consec_closes, len(closes)):
        window = closes[i - sma_period + 1: i + 1]
        sma_i = sum(window) / sma_period
        c = closes[i]
        if side == "CALL":
            if not (c < sma_i):
                return False
        else:
            if not (c > sma_i):
                return False
    return True


@dataclass
class Signal:
    symbol: str
    side: str            # "CALL" | "PUT"
    reason: str           # "orb_high_breakout" | "orb_low_breakdown"
    trigger_price: float
    orb_high: float
    orb_low: float
    ts: str


def classify_nifty_regime(nifty_orb_pchange: float, cfg=CONFIG) -> str:
    if nifty_orb_pchange >= cfg["NIFTY_BULLISH_PCT"]:
        return "bullish"
    if nifty_orb_pchange <= cfg["NIFTY_BEARISH_PCT"]:
        return "bearish"
    return "neutral"


def evaluate_breakout(symbol, ltp, prev_close, orb_high, orb_low, regime,
                       already_fired: set, cfg=CONFIG, now: datetime = None) -> "Signal | None":
    """Same regime table as the Colab original: Bullish day -> CALL on ORB-high,
    PUT on ORB-low. Bearish day -> ORB-high breakout ignored, PUT on ORB-low.
    Neutral day -> no trade (unless REGIME_FILTER_ENABLED is explicitly off).

    Note: unlike the Colab original, this does NOT compute a strike/expiry --
    that belongs to stock_resolve.py (real Upstox-registry-resolved values),
    not a price-band/calendar heuristic embedded in the pure signal function."""
    regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
    if regime_filter_on and regime == "neutral":
        return None
    stock_move_pct = (ltp - prev_close) / prev_close * 100.0 if prev_close else 0.0
    if abs(stock_move_pct) >= cfg["STOCK_MOVE_ABORT_PCT"]:
        # 2026-08-27 fix, confirmed live: TATAPOWER's own ORB-low was genuinely
        # breached but NO signal ever fired and nothing logged anywhere -- this
        # abort path used to be completely silent, indistinguishable from "no
        # breach happened at all." Logs to screener's own module logger (same
        # general-log visibility stock_resolve.py's warnings already use, NOT
        # engine.py's per-binding self._clog -- this is a pure function with no
        # access to that instance logger).
        logger.warning(
            "OiOrb screener: %s breakout ABORTED -- stock moved %.2f%% since prev close "
            "(abs >= STOCK_MOVE_ABORT_PCT=%.1f%%), ltp=%.2f prev_close=%.2f.",
            symbol, stock_move_pct, cfg["STOCK_MOVE_ABORT_PCT"], ltp, prev_close,
        )
        return None

    now = now or datetime.now(IST)

    if ltp > orb_high and (symbol, "CALL") not in already_fired:
        if not regime_filter_on or regime == "bullish":
            already_fired.add((symbol, "CALL"))
            return Signal(symbol, "CALL", "orb_high_breakout", ltp, orb_high, orb_low,
                          now.strftime("%H:%M:%S"))
        already_fired.add((symbol, "CALL"))
        return None

    if ltp < orb_low and (symbol, "PUT") not in already_fired:
        already_fired.add((symbol, "PUT"))
        return Signal(symbol, "PUT", "orb_low_breakdown", ltp, orb_high, orb_low,
                      now.strftime("%H:%M:%S"))

    return None


def backfill_orb_from_yahoo(bars: MinuteBars, symbols, cfg=CONFIG) -> None:
    """Best-effort only, mirrors the Colab original -- never required for
    the live 09:15-09:30 window itself (real polling covers that), but it
    IS the only source of real 09:15-09:30 bars when the book starts later
    than that (including ignore_time_windows test runs). 2026-08-24: this
    used to fail (or simply find nothing) completely silently -- exactly
    the same silent-failure pattern that hid the NSESession Brotli bug for
    hours. Always log the outcome now, success or failure, so an empty
    ORB never again looks identical to "nothing went wrong"."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("backfill_orb_from_yahoo: yfinance not installed -- "
                        "no ORB backfill possible, ORB will only have live-polled bars.")
        return
    try:
        tickers = [s + ".NS" for s in symbols]
        df = yf.download(tickers, period="1d", interval="1m", progress=False, group_by="ticker")
        filled = 0
        for sym, ticker in zip(symbols, tickers):
            try:
                # 2026-08-25 CRITICAL FIX, confirmed live: yf.download(..., group_by="ticker")
                # ALWAYS returns MultiIndex columns like ('SAIL.NS', 'High'), even for a SINGLE
                # ticker -- the old `if len(tickers) > 1 else df` assumed single-ticker downloads
                # came back with flat columns, which is false. On a single-stock shortlist (common
                # on quiet days), that took the `else df` branch straight into the unflattened
                # MultiIndex frame, so row.get("High")/row.get("Low") always returned None (real
                # column was the tuple ('SAIL.NS','High'), not 'High') -- every row then failed the
                # pd.isna() check and got silently skipped, producing "returned data but 0 bars
                # landed" and an empty ORB for the day's only shortlisted stock. Reproduced directly
                # against real yfinance/NSE data before fixing: df[ticker] recovers all 10 real
                # 09:15-09:25 bars regardless of how many tickers were requested.
                sub = df[ticker]
            except Exception as exc:
                logger.warning("backfill_orb_from_yahoo: no data for %s (%s): %r", sym, ticker, exc)
                continue
            sym_filled = 0
            for ts, row in sub.iterrows():
                ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
                key = ts_ist.strftime("%H:%M")
                if key >= cfg["ORB_END"]:
                    continue
                if key in bars.bars[sym]:
                    continue
                if pd.isna(row.get("High")) or pd.isna(row.get("Low")):
                    continue
                bars.bars[sym][key] = {
                    "o": float(row["Open"]), "h": float(row["High"]),
                    "l": float(row["Low"]), "c": float(row["Close"]),
                }
                sym_filled += 1
            filled += sym_filled
            if sym_filled == 0:
                logger.warning("backfill_orb_from_yahoo: %s (%s) returned data but 0 bars "
                                "landed in the %s-%s window.", sym, ticker,
                                cfg["ORB_START"], cfg["ORB_END"])
        logger.info("backfill_orb_from_yahoo: filled %d total ORB bars across %d symbols.",
                     filled, len(symbols))
    except Exception as exc:
        logger.warning("backfill_orb_from_yahoo: failed entirely: %r", exc)
