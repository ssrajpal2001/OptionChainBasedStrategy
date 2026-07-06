"""
strategies/sell_straddle/audit.py — Structured decision audit for sell-straddle.

Appends one JSONL line per decision/event so post-market replay and
optimisation can reconstruct exactly what the engine saw and why it acted.

Output:
    data/recorded/YYYYMMDD/sell_straddle_decisions.jsonl
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from config.global_config import IST

logger = logging.getLogger(__name__)


_AUDIT_FILENAME = "sell_straddle_decisions.jsonl"


def _audit_path() -> str:
    day_dir = f"data/recorded/{datetime.now(IST).strftime('%Y%m%d')}"
    os.makedirs(day_dir, exist_ok=True)
    return os.path.join(day_dir, _AUDIT_FILENAME)


def _write(record: Dict[str, Any]) -> None:
    try:
        with open(_audit_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("SellStraddle audit write failed: %s", exc)


def audit_entry_eval(
    *,
    client_id: str,
    binding_id: str,
    underlying: str,
    ts: datetime,
    rule_key: str,
    concept: str,
    spot: float,
    ltp_target: float,
    theta_target: float,
    offset: int,
    selected_pair: Optional[tuple],
    ind_by_tf: Dict[str, Any],
    passed: bool,
    reason: str,
    blocked_by: str = "",
) -> None:
    _write({
        "event": "entry_eval",
        "client_id": client_id,
        "binding_id": binding_id,
        "underlying": underlying,
        "ts": ts.isoformat(),
        "rule_key": rule_key,
        "concept": concept,
        "spot": spot,
        "ltp_target": ltp_target,
        "theta_target": theta_target,
        "offset": offset,
        "selected_pair": selected_pair,
        "ind_by_tf": ind_by_tf,
        "passed": passed,
        "reason": reason,
        "blocked_by": blocked_by,
    })


def audit_entry_exec(
    *,
    client_id: str,
    binding_id: str,
    underlying: str,
    ts: datetime,
    ce_strike: float,
    pe_strike: float,
    ce_ltp: float,
    pe_ltp: float,
    credit: float,
    expiry_date: Optional[str],
    rule_key: str,
    reason: str,
) -> None:
    _write({
        "event": "entry_exec",
        "client_id": client_id,
        "binding_id": binding_id,
        "underlying": underlying,
        "ts": ts.isoformat(),
        "ce_strike": ce_strike,
        "pe_strike": pe_strike,
        "ce_ltp": ce_ltp,
        "pe_ltp": pe_ltp,
        "credit": credit,
        "expiry_date": expiry_date,
        "rule_key": rule_key,
        "reason": reason,
    })


def audit_exit_eval(
    *,
    client_id: str,
    binding_id: str,
    underlying: str,
    ts: datetime,
    pnl: float,
    credit: float,
    criteria: Any,
    ind_by_tf: Dict[str, Any],
    fired: bool,
    fired_reason: str = "",
) -> None:
    _write({
        "event": "exit_eval",
        "client_id": client_id,
        "binding_id": binding_id,
        "underlying": underlying,
        "ts": ts.isoformat(),
        "pnl": pnl,
        "credit": credit,
        "criteria": criteria,
        "ind_by_tf": ind_by_tf,
        "fired": fired,
        "fired_reason": fired_reason,
    })


def audit_exit_exec(
    *,
    client_id: str,
    binding_id: str,
    underlying: str,
    ts: datetime,
    reason: str,
    realized_pnl: float,
    ce_entry: float,
    pe_entry: float,
    ce_exit: float,
    pe_exit: float,
) -> None:
    _write({
        "event": "exit_exec",
        "client_id": client_id,
        "binding_id": binding_id,
        "underlying": underlying,
        "ts": ts.isoformat(),
        "reason": reason,
        "realized_pnl": realized_pnl,
        "ce_entry": ce_entry,
        "pe_entry": pe_entry,
        "ce_exit": ce_exit,
        "pe_exit": pe_exit,
    })
