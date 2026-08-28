"""
strategies/oi_orb_screener/store.py -- SQLite persistence + full decision
audit trail for the OI-Spurt + ORB screener.

Why a dedicated DB (not the existing JSON position_store.py / trade_history.py
used elsewhere in this codebase): this strategy is running a full month in
pure paper mode before any live-capital decision, per direct user instruction
2026-08-24 -- the point of that month isn't just P&L, it's being able to ask
"why did (or didn't) we trade stock X on day Y" after the fact, for every
stock the screener ever looked at, not just the ones that became positions.
That needs a queryable table of every scan/shortlist/signal/rejection
decision, not just a JSON blob of closed trades -- SQL is the right tool for
"show me every PUT signal rejected by the 50% rule in the last 30 days," a
JSON file or a log grep is not.

Also fixes a real, confirmed bug found the same day: OiOrbScreenerStrategy's
open positions (self._positions) were pure in-memory with zero persistence --
a mid-day pm2 restart (2026-08-24, DIXON PE14500, entered 13:51, restart at
~14:40) silently erased all memory that a position existed: no close, no
warning, no way to reconcile against the broker afterward. Every other
strategy in this codebase persists its open position(s)
(data_layer/position_store.py); this module's `positions` table is this
strategy's equivalent, restored on every book startup for the CURRENT
trading date only (same MIS same-day-only discipline position_store.py
already enforces -- these are always intraday EOD-squareoff positions,
never carried forward).

Own dedicated DB file (data/oi_orb_screener.db), not data/clients.db --
keeps this strategy's own zero-shared-runtime-infrastructure mandate (see
strategies/oi_orb_screener/__init__.py) and makes the month-long paper
evaluation a single self-contained file to open with any SQLite browser.

Pure synchronous sqlite3 (same per-call connect/close pattern as
data_layer/client_db.py) -- callers in engine.py wrap every call with
asyncio.to_thread(), per this codebase's blocking-I/O rule.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import List, Optional

from config.global_config import IST

logger = logging.getLogger(__name__)

_DB_PATH = "data/oi_orb_screener.db"

_DDL = """
CREATE TABLE IF NOT EXISTS scans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id      TEXT NOT NULL,
    binding_id     TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    ts             TEXT NOT NULL,
    nifty_pchange  REAL,
    regime         TEXT,
    regime_ts      TEXT,
    status         TEXT NOT NULL DEFAULT 'ok',
    detail         TEXT NOT NULL DEFAULT '',
    UNIQUE(client_id, binding_id, trade_date)
);

CREATE TABLE IF NOT EXISTS shortlist (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id         TEXT NOT NULL,
    binding_id        TEXT NOT NULL,
    trade_date        TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    price_change_pct  REAL,
    oi_spurt_pct      REAL,
    score             REAL,
    side_bias         TEXT,
    orb_high          REAL,
    orb_low           REAL,
    UNIQUE(client_id, binding_id, trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS signal_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id      TEXT NOT NULL,
    binding_id     TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    ts             TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    side           TEXT NOT NULL DEFAULT '',
    event_type     TEXT NOT NULL,
    detail         TEXT NOT NULL DEFAULT '',
    trigger_price  REAL,
    orb_high       REAL,
    orb_low        REAL
);

CREATE TABLE IF NOT EXISTS positions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id      TEXT NOT NULL,
    binding_id     TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    option_type    TEXT NOT NULL,
    strike         INTEGER NOT NULL,
    expiry         TEXT NOT NULL,
    qty            INTEGER NOT NULL,
    entry_price    REAL NOT NULL,
    entry_ts       TEXT NOT NULL,
    entry_reason   TEXT NOT NULL DEFAULT '',
    exit_price     REAL,
    exit_ts        TEXT,
    exit_reason    TEXT,
    pnl            REAL,
    paper_mode     INTEGER NOT NULL DEFAULT 1,
    status         TEXT NOT NULL DEFAULT 'open',
    event_id       TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_positions_open
    ON positions(client_id, binding_id, status, trade_date);
CREATE INDEX IF NOT EXISTS idx_signal_events_day
    ON signal_events(client_id, binding_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_shortlist_day
    ON shortlist(client_id, binding_id, trade_date);
"""

_initialized = False


def init_db() -> None:
    """Idempotent -- safe to call from every writer function below. Creates
    the DB file/tables/indices and enables WAL (same rationale as
    data_layer/client_db.py: lets a concurrent reader -- someone running SQL
    queries mid-session to review the paper month -- proceed without
    blocking the live book's own writes)."""
    global _initialized
    if _initialized:
        return
    con = sqlite3.connect(_DB_PATH)
    try:
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as exc:
            logger.warning("oi_orb store: could not enable WAL mode (%s) -- falling back to "
                            "default journal mode.", exc)
        con.executescript(_DDL)
        con.commit()
    finally:
        con.close()
    _initialized = True


def _now_ts() -> str:
    return datetime.now(IST).isoformat(timespec="seconds")


def _today() -> str:
    return datetime.now(IST).date().isoformat()


# ── scans (one row per client/binding/day) ──────────────────────────────

def record_scan(client_id: str, binding_id: str, nifty_pchange: Optional[float],
                 status: str = "ok", detail: str = "", trade_date: Optional[str] = None) -> None:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """INSERT INTO scans (client_id, binding_id, trade_date, ts, nifty_pchange, status, detail)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(client_id, binding_id, trade_date) DO UPDATE SET
                   ts=excluded.ts, nifty_pchange=excluded.nifty_pchange,
                   status=excluded.status, detail=excluded.detail""",
            (client_id, binding_id, trade_date or _today(), _now_ts(), nifty_pchange, status, detail),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.record_scan failed: %s", exc)
    finally:
        con.close()


def update_scan_regime(client_id: str, binding_id: str, regime: str,
                        trade_date: Optional[str] = None) -> None:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            "UPDATE scans SET regime=?, regime_ts=? WHERE client_id=? AND binding_id=? AND trade_date=?",
            (regime, _now_ts(), client_id, binding_id, trade_date or _today()),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.update_scan_regime failed: %s", exc)
    finally:
        con.close()


# ── shortlist (per-symbol candidate, one row per client/binding/day/symbol) ─

def record_shortlist(client_id: str, binding_id: str, rows: List[dict],
                      trade_date: Optional[str] = None) -> None:
    """rows: [{"symbol", "price_change_pct", "oi_spurt_pct", "score", "side_bias"}, ...]"""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        for r in rows:
            con.execute(
                """INSERT INTO shortlist
                       (client_id, binding_id, trade_date, symbol, price_change_pct,
                        oi_spurt_pct, score, side_bias)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(client_id, binding_id, trade_date, symbol) DO UPDATE SET
                       price_change_pct=excluded.price_change_pct,
                       oi_spurt_pct=excluded.oi_spurt_pct,
                       score=excluded.score, side_bias=excluded.side_bias""",
                (client_id, binding_id, td, r["symbol"], r.get("price_change_pct"),
                 r.get("oi_spurt_pct"), r.get("score"), r.get("side_bias")),
            )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.record_shortlist failed: %s", exc)
    finally:
        con.close()


def update_orb_levels(client_id: str, binding_id: str, symbol: str,
                       orb_high: float, orb_low: float, trade_date: Optional[str] = None) -> None:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """UPDATE shortlist SET orb_high=?, orb_low=?
               WHERE client_id=? AND binding_id=? AND trade_date=? AND symbol=?""",
            (orb_high, orb_low, client_id, binding_id, trade_date or _today(), symbol),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.update_orb_levels failed: %s", exc)
    finally:
        con.close()


# ── signal_events -- the "why" audit trail ──────────────────────────────

def log_signal_event(client_id: str, binding_id: str, symbol: str, event_type: str,
                      side: str = "", detail: str = "", trigger_price: Optional[float] = None,
                      orb_high: Optional[float] = None, orb_low: Optional[float] = None,
                      trade_date: Optional[str] = None) -> None:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """INSERT INTO signal_events
                   (client_id, binding_id, trade_date, ts, symbol, side, event_type,
                    detail, trigger_price, orb_high, orb_low)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (client_id, binding_id, trade_date or _today(), _now_ts(), symbol, side,
             event_type, detail, trigger_price, orb_high, orb_low),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.log_signal_event failed: %s", exc)
    finally:
        con.close()


def load_already_fired(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> set:
    """Reconstructs the (symbol, side) pairs that already fired a signal
    today, from signal_events -- so a restart can't re-fire (and potentially
    duplicate-enter) a signal that already fired before the restart wiped
    the in-memory _already_fired set. Real, confirmed live consequence of
    NOT doing this: 2026-08-24, a restart at ~14:40 let DIXON PUT re-signal
    a second time the same day (only harmless because contract resolution
    happened to fail on the retry).

    2026-08-28 fix, real incident: a `signal_fired` event alone used to
    permanently consume that (symbol, side) for the rest of the day, even
    when the ENTRY itself never actually happened (e.g. `entry_ltp_timeout`
    because the option feed had no live tick yet) -- a stock could get one
    real shot ruined by a transient feed problem and then never get another
    chance, even after the feed came back. `already_fired` now EXCLUDES any
    (symbol, side) that also has one of the "entry never completed" abort
    event types -- it's safe to let these retry: an entry that DID actually
    succeed is independently protected by the `_positions`/
    `_pending_contracts` checks in `_handle_signal`/`start()`'s own restore,
    so under-restoring `_already_fired` here can never cause a real
    duplicate entry, only a legitimate extra chance for one that never
    happened."""
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        td = trade_date or _today()
        fired_rows = con.execute(
            """SELECT DISTINCT symbol, side FROM signal_events
               WHERE client_id=? AND binding_id=? AND trade_date=? AND event_type='signal_fired'""",
            (client_id, binding_id, td),
        ).fetchall()
        aborted_rows = con.execute(
            """SELECT DISTINCT symbol, side FROM signal_events
               WHERE client_id=? AND binding_id=? AND trade_date=?
                 AND event_type IN ('entry_ltp_timeout', 'lot_resolve_failed', 'contract_resolve_failed')""",
            (client_id, binding_id, td),
        ).fetchall()
        aborted = {(r[0], r[1]) for r in aborted_rows}
        return {(r[0], r[1]) for r in fired_rows} - aborted
    except Exception as exc:
        logger.error("oi_orb store.load_already_fired failed: %s", exc)
        return set()
    finally:
        con.close()


def load_rejected(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> set:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        rows = con.execute(
            """SELECT DISTINCT symbol, side FROM signal_events
               WHERE client_id=? AND binding_id=? AND trade_date=? AND event_type='rejection_rule_triggered'""",
            (client_id, binding_id, trade_date or _today()),
        ).fetchall()
        return {(r[0], r[1]) for r in rows}
    except Exception as exc:
        logger.error("oi_orb store.load_rejected failed: %s", exc)
        return set()
    finally:
        con.close()


# ── positions -- persistence (restore-on-restart) + P&L review ─────────

def open_position(client_id: str, binding_id: str, symbol: str, option_type: str,
                   strike: int, expiry: str, qty: int, entry_price: float,
                   entry_reason: str, paper_mode: bool, event_id: str,
                   trade_date: Optional[str] = None) -> None:
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """INSERT INTO positions
                   (client_id, binding_id, trade_date, symbol, option_type, strike, expiry,
                    qty, entry_price, entry_ts, entry_reason, paper_mode, status, event_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'open', ?)""",
            (client_id, binding_id, trade_date or _today(), symbol, option_type, strike,
             str(expiry), qty, entry_price, _now_ts(), entry_reason, int(paper_mode), event_id),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.open_position failed: %s", exc)
    finally:
        con.close()


def close_position(client_id: str, binding_id: str, symbol: str, exit_price: float,
                    exit_reason: str, pnl: float, trade_date: Optional[str] = None) -> None:
    """Closes the oldest still-open row for this symbol today -- there
    should only ever be one (_handle_signal already blocks a duplicate
    concurrent position in the same symbol)."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        row = con.execute(
            """SELECT id FROM positions
               WHERE client_id=? AND binding_id=? AND trade_date=? AND symbol=? AND status='open'
               ORDER BY id ASC LIMIT 1""",
            (client_id, binding_id, td, symbol),
        ).fetchone()
        if row is None:
            logger.warning("oi_orb store.close_position: no open row found for %s/%s/%s -- "
                            "nothing to close in the DB.", client_id, binding_id, symbol)
            return
        con.execute(
            """UPDATE positions SET exit_price=?, exit_ts=?, exit_reason=?, pnl=?, status='closed'
               WHERE id=?""",
            (exit_price, _now_ts(), exit_reason, pnl, row[0]),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.close_position failed: %s", exc)
    finally:
        con.close()


def load_open_positions(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> List[dict]:
    """Restore-on-startup: only ever returns rows for TODAY's trade_date --
    these are always intraday (EOD squareoff) positions, same MIS same-day-
    only discipline as data_layer/position_store.py. A row still 'open' from
    a PREVIOUS date means the broker's own EOD squareoff already flattened
    it in reality; resurrecting it here would track a ghost position the
    broker no longer holds. If reconciling yesterday's dangling opens is
    ever needed, that's a deliberate separate report, not silent auto-
    restore."""
    init_db()
    con = sqlite3.connect(_DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """SELECT * FROM positions
               WHERE client_id=? AND binding_id=? AND trade_date=? AND status='open'""",
            (client_id, binding_id, trade_date or _today()),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("oi_orb store.load_open_positions failed: %s", exc)
        return []
    finally:
        con.close()
