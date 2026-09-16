"""Shared 1-min historical candle fetch for RSI/ROC warm-up (sell-straddle pool engine) and
(later) the trap engine. Uses curl_cffi Chrome impersonation (Upstox edge 403s plain urllib).

IMPORTANT — intraday vs historical are DIFFERENT endpoints:
  - Prev-day / dated bars: /v2/historical-candle/{key}/1minute/{from}/{to}  (fetch_upstox_1m)
  - TODAY's open→now bars: /v2/historical-candle/intraday/{key}/1minute    (fetch_upstox_intraday_1m)
A strike subscribed mid-day must be warmed with TODAY's bars, not yesterday's.
`fetch_upstox_warm_1m` combines them (today + prev-day backfill when the session is young).

FYERS has the same distinction: its `data/history` endpoint serves intraday when called with
resolution=1 and range_from/range_to set to today (vs a past dated range for historical). A Fyers
warm-fetch is a documented follow-up — Upstox is the primary seed source for now.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from config.global_config import IST

logger = logging.getLogger(__name__)

# TTL cache for warm-up candles so multiple strategy books starting on the same
# underlying do not hammer Upstox with identical REST calls.
_WARM_CACHE: dict[Tuple[str, date], tuple[List[dict], float]] = {}
_WARM_CACHE_TTL_SECONDS = 300.0


def _parse_candles(r: dict) -> List[dict]:
    """Upstox candle response (newest-first) -> oldest-first list of candle dicts.
    'oi' is Upstox's optional 7th column (open interest) -- 0 for instruments/
    intervals that don't carry it (e.g. equity spot, 1-minute)."""
    rows = (r.get("data", {}) or {}).get("candles", []) or []
    return [{"ts": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4],
             "volume": c[5], "oi": (c[6] if len(c) > 6 else 0)} for c in reversed(rows)]


def _fyers_ts_to_iso(ts: int) -> str:
    """Fyers history returns epoch seconds; convert to ISO with IST offset."""
    return datetime.fromtimestamp(ts, tz=IST).isoformat()


def _http_get_json(url: str, access_token: str) -> dict:
    """Blocking curl_cffi GET (Chrome131 TLS) returning parsed JSON. {} on error."""
    from curl_cffi import requests as _cc
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    try:
        return _cc.get(url, headers=headers, impersonate="chrome131", timeout=8).json()
    except Exception as exc:
        logger.debug("http_get_json %s: %s", url, exc)
        return {}


_ORIGINAL_HTTP_GET_JSON = _http_get_json  # used to skip cache when tests monkeypatch


async def fetch_upstox_1m(instrument_key: str, access_token: str, max_step_back: int = 7) -> List[dict]:
    """Most recent available day's 1-min candles (oldest-first) for an Upstox instrument_key,
    stepping back day-by-day over holidays/empties up to max_step_back days. Each candle:
    {'ts','open','high','low','close','volume'}. [] if none found."""
    def _get(d: date):
        from urllib.parse import quote as _q
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/1minute/"
               f"{d.isoformat()}/{d.isoformat()}")
        return _parse_candles(_http_get_json(url, access_token))

    d = date.today() - timedelta(days=1)
    for _ in range(max_step_back):
        rows = await asyncio.to_thread(_get, d)
        if rows:
            return rows
        d -= timedelta(days=1)
    return []


async def fetch_upstox_range_1m(
    instrument_key: str, access_token: str, start: date, end: date,
) -> List[dict]:
    """2026-07-19 — full date-range 1-min history (oldest-first), one Upstox
    call per weekday in [start, end] inclusive, merged and sorted. Used for
    deep multi-week re-ingestion on strategy boot (v4_cascade's HTF/MTF zone
    rebuild) — a production version of the ad-hoc per-day fetch loop used in
    this session's validation scripts. Each candle:
    {'ts','open','high','low','close','volume'}. [] if the range yields
    nothing (holiday-only range, bad instrument_key, etc)."""
    def _get_day(d: date) -> List[dict]:
        from urllib.parse import quote as _q
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/1minute/"
               f"{d.isoformat()}/{d.isoformat()}")
        try:
            return _parse_candles(_http_get_json(url, access_token))
        except Exception as exc:
            logger.debug("fetch_upstox_range_1m day=%s: %s", d, exc)
            return []

    def _get_all() -> List[dict]:
        rows: List[dict] = []
        d = start
        while d <= end:
            if d.weekday() < 5:  # Mon-Fri only
                rows.extend(_get_day(d))
            d += timedelta(days=1)
        return rows

    rows = await asyncio.to_thread(_get_all)
    rows.sort(key=lambda r: r["ts"])
    return rows


async def fetch_upstox_prev_day_last_tick_oi(
    instrument_key: str, access_token: str, max_step_back: int = 5,
) -> Optional[float]:
    """2026-09-16, direct user spec (OI-ORB futures-OI-regime gate): the
    previous trading day's RAW open interest as of its own last real 1-min
    tick (e.g. 15:39 IST) -- deliberately distinct from Upstox's own
    `previous_oi` field (fetch_upstox_v3_quote), which tracks NSE's
    OFFICIALLY SETTLED end-of-day OI instead. Confirmed live via NSE's own
    Bhavcopy for a real contract (SOLARINDS futures, 2026-09-15): the
    official settled OpnIntrst (1,037,850) matched `previous_oi` exactly,
    while the raw last continuous-trading tick that same day (1,137,750)
    did not -- NSE's post-close settlement reconciliation genuinely revises
    the figure. Both numbers are real and meaningful; this function exists
    to surface the raw pre-settlement one alongside the official one, not
    to replace it.

    Steps back day-by-day (skipping weekends, matching fetch_upstox_range_1m's
    own Mon-Fri filter) up to max_step_back times to survive a holiday.
    Returns None on any failure (no data for the whole step-back window,
    network error) -- caller degrades safely, same convention as every
    other real-data fetch in this module."""
    d = date.today() - timedelta(days=1)
    for _ in range(max_step_back):
        if d.weekday() < 5:
            rows = await fetch_upstox_range_1m(instrument_key, access_token, d, d)
            if rows:
                oi = rows[-1].get("oi")
                return float(oi) if oi else None
        d -= timedelta(days=1)
    return None


async def fetch_upstox_daily(instrument_key: str, access_token: str, lookback_days: int = 5) -> List[dict]:
    """Daily candles (oldest-first) for instrument_key over the trailing
    lookback_days calendar days, via the 'day' interval endpoint. Each candle
    includes 'oi' (open interest) -- populated by Upstox for F&O instruments
    (e.g. a stock's near-month futures key), 0 for spot/equity keys. Used for
    day-over-day OI-buildup classification, not candle price analysis. []
    on error/empty."""
    def _get():
        from urllib.parse import quote as _q
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=lookback_days)
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/day/"
               f"{end.isoformat()}/{start.isoformat()}")
        return _parse_candles(_http_get_json(url, access_token))

    return await asyncio.to_thread(_get)


async def fetch_upstox_v3_quote(instrument_key: str, access_token: str) -> Optional[dict]:
    """2026-09-16, direct user spec (OI-ORB Screener futures-OI regime gate):
    live snapshot via Upstox's V3 Full Market Quotes endpoint
    (https://api.upstox.com/v3/market-quote/quotes) -- the ONLY endpoint in
    this codebase that returns a live current 'oi' alongside 'previous_oi'
    (the previous trading session's closing OI for the SAME contract) in one
    call. Confirmed live, 2026-09-15/16, against 4 real F&O futures
    contracts (MPHASIS/RELIANCE/TCS/ABB): previous_oi matched
    fetch_upstox_daily's own 'oi' field on the last completed daily candle
    exactly in all 4 cases -- verified empirically, not just trusted from
    the field name/docs (Upstox's own docs describe previous_oi as "The
    open interest of the symbol from the previous session (only F&O)").

    Returns the single instrument's quote dict (whatever keys Upstox
    returns, e.g. 'oi', 'previous_oi', 'last_price', 'ohlc', ...), or None
    on any failure/empty response -- caller degrades safely, same
    convention as every other real-data fetch in this module."""
    def _get():
        from urllib.parse import quote as _q
        url = f"https://api.upstox.com/v3/market-quote/quotes?instrument_key={_q(instrument_key, safe='')}"
        resp = _http_get_json(url, access_token)
        if not resp or resp.get("status") != "success":
            return None
        data = resp.get("data") or {}
        return next(iter(data.values()), None) if data else None

    return await asyncio.to_thread(_get)


async def fetch_upstox_intraday_1m(instrument_key: str, access_token: str) -> List[dict]:
    """TODAY's 1-min candles (oldest-first, open→now) for an Upstox instrument_key via the
    intraday endpoint (no date range). [] on error/empty."""
    def _get():
        from urllib.parse import quote as _q
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{_q(instrument_key, safe='')}/1minute"
        return _parse_candles(_http_get_json(url, access_token))

    return await asyncio.to_thread(_get)


async def fetch_fyers_intraday_1m(symbol: str, client_id: str, access_token: str) -> List[dict]:
    """TODAY's 1-min candles (oldest-first) for a Fyers symbol.

    Symbol examples:
      NSE:NIFTY50-INDEX
      NSE:NIFTY26JUN24000CE
      BSE:SENSEX-INDEX
      BSE:SENSEX26JUN77100PE
    """
    if not symbol or not client_id or not access_token:
        return []
    try:
        from fyers_apiv3 import fyersModel  # type: ignore[import]
    except ImportError:
        logger.warning("fetch_fyers_intraday_1m: fyers-apiv3 not installed")
        return []

    try:
        today = date.today().isoformat()
        fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=access_token,
            log_path="logs/",
        )
        data = {
            "symbol": symbol,
            "resolution": "1",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1",
        }
        resp = await asyncio.to_thread(fyers.get_history, data=data)
        if not resp or resp.get("s") != "ok":
            logger.warning("fetch_fyers_intraday_1m %s: %s", symbol, resp)
            return []
        candles = resp.get("candles", [])
        return [
            {"ts": _fyers_ts_to_iso(c[0]), "open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5] or 0)}
            for c in candles
        ]
    except Exception as exc:
        logger.warning("fetch_fyers_intraday_1m %s: %s", symbol, exc)
        return []


async def fetch_upstox_warm_1m(instrument_key: str, access_token: str, min_bars: int = 15) -> List[dict]:
    """Warm-up series (oldest-first) for RSI/ROC: today's intraday bars, backfilled with the
    previous trading day's bars (prepended, older-first) when the session is too young to have
    >= min_bars. Returns [] if both sources are empty.

    Results are cached per (instrument_key, today) for 5 minutes so N clients trading
    the same underlying share the same warm-up data without duplicate Upstox calls."""
    # Skip cache when tests monkeypatch _http_get_json; otherwise share results
    # across strategy books for the same instrument on the same day.
    use_cache = _http_get_json is _ORIGINAL_HTTP_GET_JSON
    cache_key = (instrument_key, date.today())
    if use_cache:
        cached, cached_at = _WARM_CACHE.get(cache_key, (None, 0.0))
        if cached is not None and (time.monotonic() - cached_at) < _WARM_CACHE_TTL_SECONDS:
            logger.debug("fetch_upstox_warm_1m cache hit: %s", instrument_key)
            return cached

    today = await fetch_upstox_intraday_1m(instrument_key, access_token)
    if len(today) >= min_bars:
        if use_cache:
            _WARM_CACHE[cache_key] = (today, time.monotonic())
        return today
    prev = await fetch_upstox_1m(instrument_key, access_token)
    result = prev + today
    if use_cache:
        _WARM_CACHE[cache_key] = (result, time.monotonic())
    return result
