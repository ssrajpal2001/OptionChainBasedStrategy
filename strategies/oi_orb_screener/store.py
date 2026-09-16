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
    exit_detail    TEXT NOT NULL DEFAULT '',
    pnl            REAL,
    paper_mode     INTEGER NOT NULL DEFAULT 1,
    status         TEXT NOT NULL DEFAULT 'open',
    event_id       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS rank_snapshots (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id         TEXT NOT NULL,
    binding_id        TEXT NOT NULL,
    trade_date        TEXT NOT NULL,
    poll_ts           TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    rank              INTEGER NOT NULL,
    oi_spurt_pct      REAL,
    price_change_pct  REAL
);

-- 2026-09-07, direct user spec: full-trading-day, threshold-agnostic
-- top-20 OI-spurt capture -- deliberately a SEPARATE table from
-- rank_snapshots (which only covers the narrow 09:16-09:30 pre-market
-- window and is coupled to a real trading-behavior side effect, dropping
-- a pre-entry candidate whose rank falls). This table is purely
-- observational, written by an independent poll loop, so 1-2 weeks of
-- data can be queried cleanly to work out the best OI_SPURT_MIN_PCT
-- threshold without needing to filter out the other window's rows.
CREATE TABLE IF NOT EXISTS oi_spurt_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id         TEXT NOT NULL,
    binding_id        TEXT NOT NULL,
    trade_date        TEXT NOT NULL,
    poll_ts           TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    rank              INTEGER NOT NULL,
    oi_spurt_pct      REAL,
    price_change_pct  REAL
);

-- 2026-09-07, direct user spec, new strategy "oi_orb_screener_top20":
-- "register all stocks in database for future backtest and optimisation"
-- -- ALL 20 rank-scanned stocks logged once per day (not a repeated poll
-- like oi_spurt_history above), whether or not each one went on to pass
-- the price-move filter or fire a real VWAP-touch entry. One row per
-- (client, binding, day, symbol) -- UNIQUE + ON CONFLICT UPDATE so the
-- vwap_touch_pass/touch_ts/traded columns can be updated later the same
-- day as those events actually happen, without duplicating the row.
CREATE TABLE IF NOT EXISTS oi_orb_top20_daily_scan (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id          TEXT NOT NULL,
    binding_id         TEXT NOT NULL,
    trade_date         TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    rank               INTEGER NOT NULL,
    oi_spurt_pct       REAL,
    price_change_pct   REAL,
    price_move_pass    INTEGER NOT NULL DEFAULT 0,
    vwap_touch_pass    INTEGER NOT NULL DEFAULT 0,
    touch_side         TEXT,
    touch_ts           TEXT,
    traded             INTEGER NOT NULL DEFAULT 0,
    scanned_ts         TEXT NOT NULL,
    UNIQUE(client_id, binding_id, trade_date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_positions_open
    ON positions(client_id, binding_id, status, trade_date);
CREATE INDEX IF NOT EXISTS idx_signal_events_day
    ON signal_events(client_id, binding_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_shortlist_day
    ON shortlist(client_id, binding_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_rank_snapshots_day
    ON rank_snapshots(client_id, binding_id, trade_date, poll_ts);
CREATE INDEX IF NOT EXISTS idx_oi_spurt_history_day
    ON oi_spurt_history(client_id, binding_id, trade_date, poll_ts);
CREATE INDEX IF NOT EXISTS idx_oi_spurt_history_symbol
    ON oi_spurt_history(symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_top20_daily_scan_day
    ON oi_orb_top20_daily_scan(client_id, binding_id, trade_date);

-- 2026-09-16, direct user spec: real-time futures-OI histogram per shortlisted
-- stock in the dashboard -- needs an actual stored intraday time series, since
-- the OI-regime gate (_compute_oi_regime_side) only ever takes ONE point-in-time
-- snapshot per symbol per day and never kept a running series anywhere. This
-- table is that series: one row per (symbol, poll) every OI_REGIME_CHECK_TIME
-- cycle for ALL shortlisted stocks (not just when the gate is enabled), same
-- "log everything now" precedent as oi_spurt_history above.
CREATE TABLE IF NOT EXISTS futures_oi_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id         TEXT NOT NULL,
    binding_id        TEXT NOT NULL,
    trade_date        TEXT NOT NULL,
    poll_ts           TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    current_oi        REAL,
    previous_oi       REAL,
    oi_change_pct     REAL
);
CREATE INDEX IF NOT EXISTS idx_futures_oi_history_day
    ON futures_oi_history(client_id, binding_id, trade_date, symbol, poll_ts);
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
        # 2026-09-16, direct user spec: existing deployed DBs predate the
        # exit_detail column -- CREATE TABLE IF NOT EXISTS above is a no-op
        # against an already-existing positions table, so this ALTER is the
        # real migration path. SQLite has no "ADD COLUMN IF NOT EXISTS";
        # guard with try/except instead (raises "duplicate column name" once
        # already applied -- safe to ignore every run after the first).
        try:
            con.execute("ALTER TABLE positions ADD COLUMN exit_detail TEXT NOT NULL DEFAULT ''")
            con.commit()
        except sqlite3.OperationalError:
            pass
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


def load_scan_regime(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> Optional[str]:
    """2026-09-10, real incident fix: a restart happening AFTER
    ENTRY_WINDOW_END used to leave the whole OI-ORB dashboard panel blank
    for the rest of the day -- _run_today_pipeline's own actionable-window
    check (_wait_until_actionable) returns early before the main polling
    loop (the ONLY place that restores regime/shortlist/ORB from the DB)
    is ever reached. This lets _restore_from_db pull back the regime
    that was already frozen earlier today by a prior process instance,
    unconditionally, regardless of what time it is now."""
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        row = con.execute(
            "SELECT regime FROM scans WHERE client_id=? AND binding_id=? AND trade_date=?",
            (client_id, binding_id, trade_date or _today()),
        ).fetchone()
        return row[0] if row and row[0] else None
    except Exception as exc:
        logger.error("oi_orb store.load_scan_regime failed: %s", exc)
        return None
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


# ── rank_snapshots (2026-08-30, direct user spec): "save which stocks came
# at 9:16 till 9:20 so we can optimise the time" -- since this screener
# cannot be backtested (no historical OI), the raw ranked poll output at
# EVERY poll during the 09:16-09:30 ranking window is logged here so the
# best action time (9:16? 9:20? 9:26?) and the best OI-spurt/price-move
# thresholds can be worked out after the fact by comparing these snapshots
# against what the shortlisted stocks actually did afterward -- the same
# "log everything, optimize live since backtesting is impossible" pattern
# already used for OI-Flow's telemetry.py and SellStraddle's shadow VWAP. ──

def record_rank_snapshot(client_id: str, binding_id: str, poll_ts: str, rows: List[dict],
                          trade_date: Optional[str] = None) -> None:
    """rows: [{"symbol", "rank", "oi_spurt_pct", "price_change_pct"}, ...] -- the
    full ranked poll output, one row per symbol per poll (not upserted --
    every poll is its own permanent snapshot, so the time-series itself is
    the point)."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        for r in rows:
            con.execute(
                """INSERT INTO rank_snapshots
                       (client_id, binding_id, trade_date, poll_ts, symbol, rank,
                        oi_spurt_pct, price_change_pct)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (client_id, binding_id, td, poll_ts, r["symbol"], int(r["rank"]),
                 r.get("oi_spurt_pct"), r.get("price_change_pct")),
            )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.record_rank_snapshot failed: %s", exc)
    finally:
        con.close()


# ── oi_spurt_history (2026-09-07, direct user spec): "get the top 20 stocks
# data and save in db with its oi spurt so that after 1 to 2 week we have
# all the stocks with their oi spurt to analyse what is best threshold" --
# same non-upserted, one-row-per-symbol-per-poll shape as rank_snapshots,
# but a dedicated table since this poll runs on its own full-day cadence,
# independent of and never coupled to the 09:16-09:30 rank-tracking window
# above (that one can drop a pre-entry candidate from the shortlist; this
# one only ever logs). ──────────────────────────────────────────────────

def record_oi_spurt_history(client_id: str, binding_id: str, poll_ts: str, rows: List[dict],
                             trade_date: Optional[str] = None) -> None:
    """rows: [{"symbol", "rank", "oi_spurt_pct", "price_change_pct"}, ...] --
    the full top-N poll output, one row per symbol per poll."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        for r in rows:
            con.execute(
                """INSERT INTO oi_spurt_history
                       (client_id, binding_id, trade_date, poll_ts, symbol, rank,
                        oi_spurt_pct, price_change_pct)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (client_id, binding_id, td, poll_ts, r["symbol"], int(r["rank"]),
                 r.get("oi_spurt_pct"), r.get("price_change_pct")),
            )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.record_oi_spurt_history failed: %s", exc)
    finally:
        con.close()


def load_latest_scan_symbols(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> List[dict]:
    """2026-09-08, direct user spec: on a mid-day restart, reconstruct
    'what should currently be tracked' from the continuous per-minute
    oi_spurt_history log rather than trusting a fresh live re-scan to
    reproduce the same top-N -- OI-spurt/price-move values drift minute to
    minute, so a restart's own scan can genuinely differ from what was
    already being watched before the restart (the real 2026-09-08 incident
    that dropped GVT&D/HAL/NATIONALUM/HINDZINC). Returns the rows from the
    MOST RECENT poll_ts recorded today -- [] if oi_spurt_history has no
    rows yet today (first-ever start of the day, nothing to reconstruct)."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        latest_ts = con.execute(
            "SELECT MAX(poll_ts) FROM oi_spurt_history WHERE client_id=? AND binding_id=? AND trade_date=?",
            (client_id, binding_id, td),
        ).fetchone()[0]
        if not latest_ts:
            return []
        rows = con.execute(
            """SELECT symbol, rank, oi_spurt_pct, price_change_pct FROM oi_spurt_history
               WHERE client_id=? AND binding_id=? AND trade_date=? AND poll_ts=?
               ORDER BY rank""",
            (client_id, binding_id, td, latest_ts),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("oi_orb store.load_latest_scan_symbols failed: %s", exc)
        return []
    finally:
        con.close()


# ── oi_orb_top20_daily_scan (2026-09-07, direct user spec, new strategy
# "oi_orb_screener_top20"): "register all stocks in database for future
# backtest and optimisation" -- ALL 20 rank-scanned stocks once per day,
# whether or not they went on to pass the price-move filter or fire a
# real VWAP-touch entry. ──────────────────────────────────────────────

def record_top20_daily_scan(client_id: str, binding_id: str, rows: List[dict],
                             trade_date: Optional[str] = None) -> None:
    """rows: [{"symbol", "rank", "oi_spurt_pct", "price_change_pct",
    "price_move_pass"}, ...] -- the full top-20 scan, ALL stocks, called
    once at scan time. ON CONFLICT UPDATE so a re-scan after a restart
    corrects rather than duplicates each symbol's row for the day."""
    init_db()
    td = trade_date or _today()
    now_ts = datetime.now(IST).isoformat(timespec="seconds")
    con = sqlite3.connect(_DB_PATH)
    try:
        for r in rows:
            con.execute(
                """INSERT INTO oi_orb_top20_daily_scan
                       (client_id, binding_id, trade_date, symbol, rank,
                        oi_spurt_pct, price_change_pct, price_move_pass, scanned_ts)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(client_id, binding_id, trade_date, symbol) DO UPDATE SET
                       rank=excluded.rank, oi_spurt_pct=excluded.oi_spurt_pct,
                       price_change_pct=excluded.price_change_pct,
                       price_move_pass=excluded.price_move_pass""",
                (client_id, binding_id, td, r["symbol"], int(r["rank"]),
                 r.get("oi_spurt_pct"), r.get("price_change_pct"),
                 int(bool(r.get("price_move_pass"))), now_ts),
            )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.record_top20_daily_scan failed: %s", exc)
    finally:
        con.close()


def update_top20_vwap_touch(client_id: str, binding_id: str, symbol: str, touch_side: str,
                             trade_date: Optional[str] = None) -> None:
    """Marks a symbol's VWAP-touch condition as satisfied -- called the
    moment VwapTouchTracker.check_touch() first fires for it. No-op
    (safe) if the symbol was never scanned into today's row at all."""
    init_db()
    td = trade_date or _today()
    now_ts = datetime.now(IST).isoformat(timespec="seconds")
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """UPDATE oi_orb_top20_daily_scan
               SET vwap_touch_pass=1, touch_side=?, touch_ts=?
               WHERE client_id=? AND binding_id=? AND trade_date=? AND symbol=?""",
            (touch_side, now_ts, client_id, binding_id, td, symbol),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.update_top20_vwap_touch failed: %s", exc)
    finally:
        con.close()


def update_top20_traded(client_id: str, binding_id: str, symbol: str,
                         trade_date: Optional[str] = None) -> None:
    """Marks a symbol as having actually fired a real entry today."""
    init_db()
    td = trade_date or _today()
    con = sqlite3.connect(_DB_PATH)
    try:
        con.execute(
            """UPDATE oi_orb_top20_daily_scan SET traded=1
               WHERE client_id=? AND binding_id=? AND trade_date=? AND symbol=?""",
            (client_id, binding_id, td, symbol),
        )
        con.commit()
    except Exception as exc:
        logger.error("oi_orb store.update_top20_traded failed: %s", exc)
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


def load_historical_check_done(client_id: str, binding_id: str, trade_date: Optional[str] = None) -> set:
    """2026-09-10, real incident fix: the "replay ALL of today's real 1-min
    history and fire immediately if a retest already completed" check
    (screener.historical_rolling_retest_check/historical_vwap_retest_check)
    is DETERMINISTIC given the same day's history -- it always finds and
    reports the SAME first-ever retest moment, no matter how many times or
    how much later it's called. self._morning_historical_retest_applied/
    _restart_db_reconcile_applied only guard this ONCE PER PROCESS
    LIFETIME, not once per DAY -- every restart resets them to False, so
    the SAME historical replay re-runs and re-logs a "signal_fired" using
    an increasingly stale reference price from hours earlier, even for a
    symbol whose real live market conditions have moved on completely.

    Real incident: GVT&D's historical check kept re-reporting "retested at
    09:18, price=4638.90" across 10+ separate log lines through the whole
    session (09:18 through 12:21), even though real spot by 11:11 (when a
    genuine entry actually fired off this stale reference) was trading
    around 4540-4547 -- a ~2% discrepancy that corrupted the trade's own
    recorded entry rationale and made post-hoc analysis actively
    misleading, not just noisy.

    This is the restart-safe fix: reconstructs which (symbol, side) pairs
    have ALREADY had this historical replay performed at all today
    (regardless of outcome -- fired, aborted, or rejected; ANY row at all
    in signal_events counts), so a restart never re-runs it a second time.
    Any GENUINELY new opportunity for a symbol from that point forward
    comes exclusively from the live tick loop's own continuously-running
    real-time arm/retest tracker (self._rolling_retest_trackers /
    self._vwap_armed), which is seeded by the one real historical replay
    and has no staleness risk of its own -- it only ever reacts to
    real-time ticks."""
    init_db()
    con = sqlite3.connect(_DB_PATH)
    try:
        td = trade_date or _today()
        rows = con.execute(
            """SELECT DISTINCT symbol, side FROM signal_events
               WHERE client_id=? AND binding_id=? AND trade_date=?""",
            (client_id, binding_id, td),
        ).fetchall()
        return {(r[0], r[1]) for r in rows}
    except Exception as exc:
        logger.error("oi_orb store.load_historical_check_done failed: %s", exc)
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
                    exit_reason: str, pnl: float, trade_date: Optional[str] = None,
                    exit_detail: str = "") -> None:
    """Closes the oldest still-open row for this symbol today -- there
    should only ever be one (_handle_signal already blocks a duplicate
    concurrent position in the same symbol).

    2026-09-16, direct user spec: `exit_detail` carries the WHY behind the
    exit in human-readable form -- the real candle/bucket time and the
    values (e.g. bucket close vs VWAP) that satisfied the condition, not
    just the bare exit_reason code. Optional/best-effort: an empty string
    is fine for exit paths that don't have this detail available."""
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
            """UPDATE positions SET exit_price=?, exit_ts=?, exit_reason=?, exit_detail=?, pnl=?, status='closed'
               WHERE id=?""",
            (exit_price, _now_ts(), exit_reason, exit_detail, pnl, row[0]),
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
