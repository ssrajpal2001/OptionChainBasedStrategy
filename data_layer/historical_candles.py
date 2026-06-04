"""Shared prev-day 1-min historical candle fetch with holiday step-back. Used to seed RSI/ROC
warm-up (sell-straddle pool engine) and (later) the trap engine. Uses curl_cffi Chrome
impersonation (Upstox edge 403s plain urllib)."""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import List

logger = logging.getLogger(__name__)


async def fetch_upstox_1m(instrument_key: str, access_token: str, max_step_back: int = 7) -> List[dict]:
    """Most recent available day's 1-min candles (oldest-first) for an Upstox instrument_key,
    stepping back day-by-day over holidays/empties up to max_step_back days. Each candle:
    {'ts','open','high','low','close','volume'}. [] if none found."""
    def _get(d: date):
        from curl_cffi import requests as _cc
        url = (f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/"
               f"{d.isoformat()}/{d.isoformat()}")
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        try:
            r = _cc.get(url, headers=headers, impersonate="chrome131", timeout=8).json()
            rows = (r.get("data", {}) or {}).get("candles", []) or []
            return [{"ts": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                     "volume": c[5]} for c in reversed(rows)]
        except Exception as exc:
            logger.debug("fetch_upstox_1m %s %s: %s", instrument_key, d, exc)
            return []

    d = date.today() - timedelta(days=1)
    for _ in range(max_step_back):
        rows = await asyncio.to_thread(_get, d)
        if rows:
            return rows
        d -= timedelta(days=1)
    return []
