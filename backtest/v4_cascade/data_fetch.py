"""backtest/v4_cascade/data_fetch.py -- fetch + cache NIFTY spot 1-minute
history via the same Upstox REST function production book.py already uses
(data_layer.historical_candles.fetch_upstox_range_1m). One instrument (the
index itself never expires), so there is no per-strike contract-resolution
problem the way there would be for option premium history.

Token is read from the UPSTOX_TOKEN env var by the caller (main.py) and
passed in here -- never written to any cached file, never logged."""
from __future__ import annotations

import json
import os
from datetime import date

from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY

_CACHE_DIR = os.path.join(os.path.dirname(__file__), "data_cache")


def _cache_path(instrument_key: str, start: date, end: date) -> str:
    safe = instrument_key.replace("|", "_").replace(" ", "_").replace("/", "_")
    return os.path.join(_CACHE_DIR, f"{safe}_{start.isoformat()}_{end.isoformat()}.json")


async def fetch_nifty_spot_1m(token: str, start: date, end: date) -> list:
    """Oldest-first list of {'ts','open','high','low','close','volume'} dicts
    for NIFTY spot, [start, end] inclusive. Cached to disk keyed by the exact
    date range requested -- a re-run with the SAME range is instant; a wider
    range re-fetches (no partial-range stitching, kept simple since a single
    index fetch is already cheap)."""
    instrument_key = REGISTRY.historical_instrument_key("NIFTY")
    os.makedirs(_CACHE_DIR, exist_ok=True)
    path = _cache_path(instrument_key, start, end)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    rows = await fetch_upstox_range_1m(instrument_key, token, start, end)
    if rows:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(rows, f)
        os.replace(tmp, path)
    return rows
