"""
strategies/vp_oi_regime/recorder.py -- SQLite history of every real VP/OI
regime evaluation, so a future day can be backtested/reviewed offline.

2026-10-09, direct user ask after today's first live day: nothing was being
persisted beyond a 60s-throttled text-log heartbeat (regime label + the
raw futures OI number only) -- not enough to ever reconstruct or backtest
what actually happened. Real structural reason this can't be backfilled
retroactively for days before this was added: Upstox's historical candle
REST API hardcodes OI to 0 at any intraday granularity (see
data_layer/historical_candles.py's own _parse_candles docstring) -- OI only
ever exists in the live tick stream, so if it isn't recorded as it happens,
it's gone forever. Same "log everything now, backtest later" pattern as
strategies/oi_orb_screener/store.py and strategies/oi_flow/telemetry.py.

Own dedicated DB file, plain sqlite3, per-call connect/close -- same
convention as oi_orb_screener/store.py. Callers wrap every call with
asyncio.to_thread() per this codebase's blocking-I/O rule.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Optional

from config.global_config import IST

_DB_PATH = "data/vp_oi_regime.db"

_DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id       TEXT NOT NULL,
    binding_id      TEXT NOT NULL,
    underlying      TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    ts              TEXT NOT NULL,
    spot            REAL,
    poc             REAL,
    vah             REAL,
    val             REAL,
    future_oi_now     REAL,
    future_oi_anchor  REAL,
    future_oi_pct     REAL,
    future_oi_trend   TEXT,
    call_oi_now       REAL,
    call_oi_anchor    REAL,
    call_oi_pct       REAL,
    call_oi_trend     TEXT,
    put_oi_now        REAL,
    put_oi_anchor     REAL,
    put_oi_pct        REAL,
    put_oi_trend      TEXT,
    regime          TEXT,
    naked_state     TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
    ON snapshots (underlying, trade_date, client_id, binding_id);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.executescript(_DDL)
    return conn


def load_today_peaks(
    *, client_id: str, binding_id: str, underlying: str,
) -> dict:
    """2026-10-13, direct user spec: on a mid-day restart, the live
    StickyTrendLatch anchors used to reset to whatever OI happened to be
    live at that exact moment -- wiping the real session peak tracked since
    market open. Returns today's real MAX(call_oi_now)/MAX(put_oi_now)/
    MAX(future_oi_now) from this book's own already-recorded snapshots
    (full resolution, written every real evaluate() cycle since whenever
    this was first deployed) -- None for any value with no rows yet today.
    Synchronous (sqlite3) -- caller must wrap with asyncio.to_thread()."""
    from datetime import datetime
    trade_date = datetime.now(IST).strftime("%Y-%m-%d")
    conn = _connect()
    try:
        row = conn.execute(
            """SELECT MAX(call_oi_now), MAX(put_oi_now), MAX(future_oi_now)
               FROM snapshots WHERE client_id=? AND binding_id=? AND underlying=? AND trade_date=?""",
            (client_id, binding_id, underlying, trade_date),
        ).fetchone()
    finally:
        conn.close()
    call_max, put_max, future_max = row if row else (None, None, None)
    return {"call_oi_peak": call_max, "put_oi_peak": put_max, "future_oi_peak": future_max}


def record_snapshot(
    *, client_id: str, binding_id: str, underlying: str, state: dict,
) -> None:
    """Write one row from a VpOiRegimeAdapter.monitoring_state() dict.
    Synchronous (sqlite3) -- caller must wrap with asyncio.to_thread()."""
    now = datetime.now(IST)
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO snapshots (
                client_id, binding_id, underlying, trade_date, ts, spot,
                poc, vah, val,
                future_oi_now, future_oi_anchor, future_oi_pct, future_oi_trend,
                call_oi_now, call_oi_anchor, call_oi_pct, call_oi_trend,
                put_oi_now, put_oi_anchor, put_oi_pct, put_oi_trend,
                regime, naked_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                client_id, binding_id, underlying, now.strftime("%Y-%m-%d"), now.isoformat(),
                state.get("spot"), state.get("poc"), state.get("vah"), state.get("val"),
                state.get("future_oi_now"), state.get("future_oi_anchor"),
                state.get("future_oi_pct"), state.get("future_oi_trend"),
                state.get("call_oi_now"), state.get("call_oi_anchor"),
                state.get("call_oi_pct"), state.get("call_oi_trend"),
                state.get("put_oi_now"), state.get("put_oi_anchor"),
                state.get("put_oi_pct"), state.get("put_oi_trend"),
                state.get("regime"), state.get("naked_state"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_day(underlying: str, trade_date: str, client_id: Optional[str] = None,
              binding_id: Optional[str] = None) -> list:
    """Read back one day's snapshots, oldest-first -- for an offline backtest/
    review script. Synchronous; fine to call directly from a standalone script
    (not the live app)."""
    conn = _connect()
    try:
        q = "SELECT * FROM snapshots WHERE underlying=? AND trade_date=?"
        params: list = [underlying, trade_date]
        if client_id:
            q += " AND client_id=?"
            params.append(client_id)
        if binding_id:
            q += " AND binding_id=?"
            params.append(binding_id)
        q += " ORDER BY ts ASC"
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()
