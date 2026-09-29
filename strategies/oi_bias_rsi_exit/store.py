"""
strategies/oi_bias_rsi_exit/store.py -- SQLite persistence for the OI-spurt
selection + combined-OI bias + StochRSI entry/exit strategy.

Direct user instruction (2026-09-29): save every shortlisted stock and its
real OI-bias inputs (not just the ones that got a real signal or a real
trade) so a future backtest/analysis can be re-run purely from the DB --
same "log everything now, optimize later" precedent as strategies/
oi_orb_screener/store.py's own oi_spurt_history/option_native_feature_
history tables.

Own dedicated DB file (data/oi_bias_rsi_exit.db), not data/clients.db or
data/oi_orb_screener.db -- keeps this strategy's own zero-shared-runtime
mandate. Pure synchronous sqlite3 (same per-call connect/close pattern as
data_layer/client_db.py) -- callers in engine.py wrap every call with
asyncio.to_thread().
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Optional

from config.global_config import IST

logger = logging.getLogger(__name__)

_DB_PATH = "data/oi_bias_rsi_exit.db"

_DDL = """
-- One row per (client, binding, trade_date, symbol) -- every shortlisted
-- stock's full combined-OI reading and the resulting bias, whether or not
-- it ever got a real entry. This is the table a future backtest re-runs
-- classify_combined_oi_bias against, independent of any trade outcome.
CREATE TABLE IF NOT EXISTS shortlist_oi (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id           TEXT NOT NULL,
    binding_id          TEXT NOT NULL,
    trade_date          TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    open_915            REAL,
    strike_step         REAL,
    atm_strike          REAL,
    otm_call_strike     REAL,
    otm_put_strike      REAL,
    call_oi_915         REAL,
    call_oi_920         REAL,
    call_oi_925         REAL,
    put_oi_915          REAL,
    put_oi_920          REAL,
    put_oi_925          REAL,
    bias                TEXT NOT NULL,
    recorded_ts         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shortlist_oi_day
    ON shortlist_oi(client_id, binding_id, trade_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_shortlist_oi_unique
    ON shortlist_oi(client_id, binding_id, trade_date, symbol);

-- Full trade lifecycle -- entry/exit price, reason, P&L, and the exact
-- StochRSI timeframe/lengths in force at the time (so a later parameter
-- change never makes an old row ambiguous about what produced it).
CREATE TABLE IF NOT EXISTS positions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id      TEXT NOT NULL,
    binding_id     TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    option_type    TEXT NOT NULL,
    strike         REAL NOT NULL,
    expiry         TEXT NOT NULL,
    qty            INTEGER NOT NULL,
    entry_price    REAL NOT NULL,
    entry_ts       TEXT NOT NULL,
    exit_price     REAL,
    exit_ts        TEXT,
    exit_reason    TEXT,
    pnl            REAL,
    entry_tf_min   INTEGER,
    entry_lengths  TEXT,
    exit_tf_min    INTEGER,
    exit_lengths   TEXT,
    status         TEXT NOT NULL DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS idx_oi_bias_rsi_exit_positions_day
    ON positions(client_id, binding_id, trade_date, status);
"""

_initialized = False


def init_db() -> None:
    global _initialized
    if _initialized:
        return
    con = sqlite3.connect(_DB_PATH)
    try:
        con.executescript(_DDL)
        con.execute("PRAGMA journal_mode=WAL;")
        con.commit()
    finally:
        con.close()
    _initialized = True


def _today() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def record_shortlist_oi(client_id: str, binding_id: str, symbol: str, row: dict,
                         trade_date: Optional[str] = None) -> None:
    """row: {"open_915","strike_step","atm_strike","otm_call_strike",
    "otm_put_strike","call_oi_915","call_oi_920","call_oi_925","put_oi_915",
    "put_oi_920","put_oi_925","bias"}. Upserts -- a later poll in the same
    day for the same symbol replaces the earlier (fresher OI-925 reading is
    always the one worth keeping)."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """INSERT INTO shortlist_oi
                   (client_id, binding_id, trade_date, symbol, open_915, strike_step,
                    atm_strike, otm_call_strike, otm_put_strike,
                    call_oi_915, call_oi_920, call_oi_925,
                    put_oi_915, put_oi_920, put_oi_925, bias, recorded_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(client_id, binding_id, trade_date, symbol) DO UPDATE SET
                   call_oi_915=excluded.call_oi_915, call_oi_920=excluded.call_oi_920,
                   call_oi_925=excluded.call_oi_925, put_oi_915=excluded.put_oi_915,
                   put_oi_920=excluded.put_oi_920, put_oi_925=excluded.put_oi_925,
                   bias=excluded.bias, recorded_ts=excluded.recorded_ts""",
            (client_id, binding_id, td, symbol,
             row.get("open_915"), row.get("strike_step"), row.get("atm_strike"),
             row.get("otm_call_strike"), row.get("otm_put_strike"),
             row.get("call_oi_915"), row.get("call_oi_920"), row.get("call_oi_925"),
             row.get("put_oi_915"), row.get("put_oi_920"), row.get("put_oi_925"),
             row.get("bias", "none"), datetime.now(IST).isoformat()),
        )
        con.commit()
    except Exception:
        logger.exception("oi_bias_rsi_exit store.record_shortlist_oi failed (non-fatal).")
    finally:
        con.close()


def record_entry(client_id: str, binding_id: str, symbol: str, pos: dict,
                  entry_tf_min: int, entry_lengths: tuple, trade_date: Optional[str] = None) -> int:
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        cur = con.execute(
            """INSERT INTO positions
                   (client_id, binding_id, trade_date, symbol, option_type, strike, expiry,
                    qty, entry_price, entry_ts, entry_tf_min, entry_lengths, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'open')""",
            (client_id, binding_id, td, symbol, pos["option_type"], pos["strike"], str(pos["expiry"]),
             pos["qty"], pos["entry_price"], pos["entry_ts"].isoformat(),
             entry_tf_min, str(entry_lengths)),
        )
        con.commit()
        return cur.lastrowid
    except Exception:
        logger.exception("oi_bias_rsi_exit store.record_entry failed (non-fatal).")
        return -1
    finally:
        con.close()


def record_exit(row_id: int, exit_price: float, exit_reason: str, pnl: float,
                 exit_tf_min: int, exit_lengths: tuple) -> None:
    if row_id < 0:
        return
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """UPDATE positions SET exit_price=?, exit_ts=?, exit_reason=?, pnl=?,
                   exit_tf_min=?, exit_lengths=?, status='closed' WHERE id=?""",
            (exit_price, datetime.now(IST).isoformat(), exit_reason, pnl,
             exit_tf_min, str(exit_lengths), row_id),
        )
        con.commit()
    except Exception:
        logger.exception("oi_bias_rsi_exit store.record_exit failed (non-fatal).")
    finally:
        con.close()
