"""
data_layer/trade_history.py — persistent per-client closed-trade history.

The dashboard's client History view was reading a non-existent in-memory
`_event_log`, so it was always empty. This module records every closed trade
(straddle / iron condor / trap exit) to an append-only JSON file per client and
serves it back to the History endpoint — surviving restarts.

File: data/history/<client_id>.json  → {"trades": [ {record}, ... ]}  (capped).
Pure JSON, synchronous (call via asyncio.to_thread from async code if needed).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

_DIR = os.path.join("data", "history")
_CAP = 500  # keep the most recent N trades per client


def _path(client_id: str) -> str:
    safe = "".join(c for c in str(client_id) if c.isalnum() or c in ("_", "-")) or "unknown"
    return os.path.join(_DIR, f"{safe}.json")


def record(
    client_id: str,
    strategy: str,
    instrument: str,
    entry_price: float,
    exit_price: float,
    exit_reason: str,
    pnl: float,
    binding_id: str = "",
    ts: Optional[str] = None,
) -> None:
    """Append one closed-trade record for a client (append-only, capped)."""
    try:
        os.makedirs(_DIR, exist_ok=True)
        rec = {
            "ts": ts or datetime.now().isoformat(timespec="seconds"),
            "strategy": strategy,
            "instrument": instrument,
            "binding_id": binding_id,
            "entry_price": round(float(entry_price), 2),
            "exit_price": round(float(exit_price), 2),
            "exit_reason": exit_reason,
            "pnl": round(float(pnl), 2),
        }
        trades = _load_raw(client_id)
        trades.append(rec)
        if len(trades) > _CAP:
            trades = trades[-_CAP:]
        tmp = _path(client_id) + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"trades": trades}, f, indent=2)
        os.replace(tmp, _path(client_id))
    except Exception as exc:
        logger.warning("trade_history.record[%s] failed: %s", client_id, exc)


def _load_raw(client_id: str) -> List[dict]:
    p = _path(client_id)
    if not os.path.exists(p):
        return []
    try:
        with open(p) as f:
            return json.load(f).get("trades", [])
    except Exception:
        return []


def load(client_id: str, limit: int = 200) -> List[dict]:
    """Most-recent-first closed trades for a client (up to `limit`)."""
    trades = _load_raw(client_id)
    return list(reversed(trades))[:limit]
