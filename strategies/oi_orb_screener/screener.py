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
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

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
    # Scanning/shortlist-build/ORB-freeze all wait until this exact time,
    # per direct user spec ("scan for stocks after 9:25am only... will
    # start at 9.25.05am") -- NOT the same as ENTRY_WINDOW_END's old
    # "already too late, do nothing" cutoff. See engine.py's
    # _wait_until_actionable.
    "SCAN_START": "09:25",
    "ENTRY_WINDOW_START": "09:25",
    "ENTRY_WINDOW_END": "10:30",
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
    # UNDERLYING STOCK's own 1-min closes (not the option premium -- same
    # deliberate spot-based-exit precedent as Liquidity Sweep/Liquidity
    # Trap in this codebase), exit on SMA_EXIT_CONSEC_CLOSES consecutive
    # candle closes on the wrong side of it.
    "SMA_PERIOD": 8,
    "SMA_EXIT_CONSEC_CLOSES": 2,
    # 2026-08-24, direct user spec: strike is 2% OTM (above spot for a
    # CALL, below spot for a PUT), not ATM -- see resolve_strike_step_for_
    # price's caller in engine.py for where this is actually applied.
    "STRIKE_OTM_PCT": 2.0,
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


def check_sma_exit(closes: list, sma_period: int, consec_closes: int, side: str) -> bool:
    """2026-08-24, direct user spec: exit when `consec_closes` (default 2)
    consecutive candle CLOSES land on the wrong side of an `sma_period`
    (default 8) SMA of the underlying STOCK's own closes (not the option
    premium). side="CALL" (bought on a gainer) exits when closes are BELOW
    the SMA; side="PUT" (bought on a loser) exits when closes are ABOVE it.

    Simplification, documented per this codebase's own "explain the
    non-obvious" convention: checks the last `consec_closes` closes against
    ONE current SMA value (computed from the most recent `sma_period`
    closes), not a separately-recomputed rolling SMA per historical bar.
    An 8-period SMA moves slowly enough that this is a very close
    approximation of the fully-rolling version in practice, at a fraction
    of the complexity -- revisit if real forward data shows it matters."""
    if len(closes) < sma_period + consec_closes - 1:
        return False
    sma = compute_sma(closes, sma_period)
    if sma is None:
        return False
    last_n = closes[-consec_closes:]
    if side == "CALL":
        return all(c < sma for c in last_n)
    return all(c > sma for c in last_n)


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
