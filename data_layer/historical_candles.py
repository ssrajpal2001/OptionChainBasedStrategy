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
from datetime import date, timedelta
from typing import List, Tuple

logger = logging.getLogger(__name__)

# TTL cache for warm-up candles so multiple strategy books starting on the same
# underlying do not hammer Upstox with identical REST calls.
_WARM_CACHE: dict[Tuple[str, date], tuple[List[dict], float]] = {}
_WARM_CACHE_TTL_SECONDS = 300.0


def _parse_candles(r: dict) -> List[dict]:
    """Upstox candle response (newest-first) -> oldest-first list of candle dicts."""
    rows = (r.get("data", {}) or {}).get("candles", []) or []
    return [{"ts": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4],
             "volume": c[5]} for c in reversed(rows)]


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


async def fetch_upstox_intraday_1m(instrument_key: str, access_token: str) -> List[dict]:
    """TODAY's 1-min candles (oldest-first, open→now) for an Upstox instrument_key via the
    intraday endpoint (no date range). [] on error/empty."""
    def _get():
        from urllib.parse import quote as _q
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{_q(instrument_key, safe='')}/1minute"
        return _parse_candles(_http_get_json(url, access_token))

    return await asyncio.to_thread(_get)


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
