"""
strategies/oi_flow/telemetry.py — structured signal-evaluation logging.

This strategy cannot be backtested (Upstox's intraday historical-candle
API has no OI field -- confirmed, see the plan doc and CLAUDE.md). Every
signal evaluation -- fired or not -- gets logged to
logs/oi_flow/{underlying}_{date}.jsonl so real forward performance can be
reviewed after the fact, the way a backtest report would normally be
reviewed for every other strategy in this codebase. The point is not just
logging trades: logging the evaluations that DIDN'T fire, and why, is what
lets a false-negative review happen later ("was the spot gate right to
reject this, in hindsight?").

Does NOT reimplement detect_pre_breakout_signal()'s / confirm_option_
price_action()'s pass/fail DECISION logic -- those stay the single source
of truth, called directly by strategies/oi_flow/engine.py. This module
only defines the row shape and the (trivial, side-effect-free) append-to-
file writer; the caller populates the row from values it already computed
via direct, read-only queries against the same tracker/snap/bars it used
for the real decision.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

from config.global_config import IST

logger = logging.getLogger(__name__)

_LOG_DIR = "logs/oi_flow"


@dataclass
class SignalTelemetryRow:
    ts: str
    underlying: str
    side: str

    # Spot gate (detect_pre_breakout_signal) diagnostics -- populated
    # regardless of whether the gate passed, so a rejection is reviewable.
    spot: Optional[float] = None
    wall_strike: Optional[float] = None
    opposing_roc: Optional[int] = None
    supporting_roc: Optional[int] = None
    pcr: Optional[float] = None
    spot_gate_fired: bool = False

    # Option gate (confirm_option_price_action) diagnostics -- only
    # populated when the spot gate fired (that's the only time it runs).
    option_premium: Optional[float] = None
    option_vwap: Optional[float] = None
    option_sl_level: Optional[float] = None
    option_gate_ok: Optional[bool] = None
    option_gate_reason: Optional[str] = None
    # Absorption/catalyst read (2026-08-13) -- option's own 1-min bar
    # volume vs. its trailing average. Soft/logged only, never gates an
    # entry (see detector.py's OptionConfirmation docstring) -- this is
    # the forward-review evidence that decides whether it should.
    volume_spike: Optional[bool] = None
    volume_ratio: Optional[float] = None

    # Final outcome.
    entered: bool = False
    skip_reason: str = ""   # "spot_gate_no_signal" | "option_gate_blocked" | "no_live_ltp" | ""


def log_signal_evaluation(row: SignalTelemetryRow, log_dir: str = _LOG_DIR) -> None:
    """Append-only, best-effort -- a telemetry write failure must never
    interrupt live trading logic (matches this codebase's own convention
    for trade-log writers: log and swallow, never raise)."""
    try:
        os.makedirs(log_dir, exist_ok=True)
        today = datetime.now(IST).strftime("%Y%m%d")
        path = os.path.join(log_dir, f"{row.underlying}_{today}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(row), default=str) + "\n")
    except Exception:
        logger.exception("oi_flow telemetry: failed to write signal evaluation row (non-fatal).")


def new_row(underlying: str, side: str, now: Optional[datetime] = None) -> SignalTelemetryRow:
    ts = (now or datetime.now(IST)).isoformat()
    return SignalTelemetryRow(ts=ts, underlying=underlying, side=side)
