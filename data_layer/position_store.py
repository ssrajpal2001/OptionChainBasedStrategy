"""
data_layer/position_store.py — JSON persistence for open strategy positions.

Why: strategy positions live in memory, so a restart/refresh loses them and the
bot stops managing a live broker position. This stores each strategy's open
position to data/positions/<key>.json on open/change, restores it on startup,
and enforces the MIS new-day rule:

  • NRML positions carry forward across days → restored as-is.
  • MIS positions are intraday and auto-squared by the broker at EOD, so on a
    NEW trading day a stored MIS position is treated as already closed → the
    file is discarded and NOT restored.

Pure JSON, synchronous (call via asyncio.to_thread from async code if needed).
No external deps. Key format: "<UNDERLYING>_<strategy>" e.g. "NIFTY_iron_condor".
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

_DIR = os.path.join("data", "positions")


def _path(key: str) -> str:
    return os.path.join(_DIR, f"{key}.json")


def save(key: str, position: dict, product_type: str = "MIS") -> bool:
    """Persist an open position. `position` must be JSON-serialisable.

    2026-08-06: now returns True/False instead of always None. Previously a
    write failure was only a WARNING log the caller had no way to detect --
    _persist()'s own try/except was structurally incapable of ever noticing
    a failed write to retry it, since the failure was already swallowed one
    layer down here. A single transient I/O failure (disk full, permission,
    AV lock) could silently desync the on-disk file from the true in-memory
    state until the next state-transition event, or forever if the process
    crashed/restarted in between."""
    try:
        os.makedirs(_DIR, exist_ok=True)
        payload = {
            "date": date.today().isoformat(),
            "product_type": (product_type or "MIS").upper(),
            "position": position,
        }
        tmp = _path(key) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        os.replace(tmp, _path(key))   # atomic
        return True
    except Exception as exc:
        logger.warning("PositionStore.save[%s] failed: %s", key, exc)
        return False


def load(key: str) -> Optional[dict]:
    """
    Return the stored position dict, or None.
    MIS positions stored on a PREVIOUS day are discarded (broker auto-squared)
    and the file is removed — they are never restored on a fresh day.
    """
    p = _path(key)
    if not os.path.exists(p):
        return None
    try:
        with open(p) as f:
            payload = json.load(f)
    except Exception as exc:
        logger.warning("PositionStore.load[%s] failed: %s", key, exc)
        return None

    stored_date = payload.get("date", "")
    product = (payload.get("product_type") or "MIS").upper()
    today = date.today().isoformat()

    if product == "MIS" and stored_date != today:
        logger.info(
            "PositionStore[%s]: stored MIS position from %s is a new-day carryover "
            "— treating as squared-off and discarding.", key, stored_date,
        )
        clear(key)
        return None

    return payload.get("position")


def clear(key: str) -> bool:
    """Remove a stored position (call on exit/close). Returns True on success
    or if there was nothing to remove; False if removal was attempted and
    failed (2026-08-06, same rationale as save()'s return value: a failed
    clear() left a stale "still open" file on disk that a later restart would
    wrongly resurrect as a real position the broker no longer holds)."""
    try:
        if os.path.exists(_path(key)):
            os.remove(_path(key))
        return True
    except Exception as exc:
        logger.warning("PositionStore.clear[%s] failed: %s", key, exc)
        return False


def list_keys() -> list:
    """All currently stored position keys (for diagnostics/UI)."""
    if not os.path.isdir(_DIR):
        return []
    return [f[:-5] for f in os.listdir(_DIR) if f.endswith(".json")]
