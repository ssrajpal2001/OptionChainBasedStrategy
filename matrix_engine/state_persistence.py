"""
matrix_engine/state_persistence.py — SQLite candle-state snapshot engine.

Persists on every CANDLE_CLOSE event so mid-day reboots can recover the
indicator state, Strategy B rolling_base, void phase, and order fills.

Schema (4 tables):
  candle_snapshots   — per-candle OHLCV + RSI/VWAP/ADX/EMA/ATR per symbol×timeframe
  strategy_b_state   — rolling_base, htf_entry_level, void_phase, void_since
  order_tickets      — client_id, broker_symbol, side, qty, order_id, avg_price
  risk_params        — capital, max_risk_pct, daily_pnl snapshot

Boot recovery:
  restore_candle_history() returns the last N OHLCV rows per symbol×timeframe
  as pandas DataFrames — feed directly to CandleCache.load_history() so that
  RSI(14), VWAP(500), and ADX(20) have full analytical parity on restart.

  restore_state() returns the latest scalar snapshot values and Strategy B
  state dicts for injection into strategy state machines.

Blocking I/O policy:
  ALL SQLite writes go through asyncio.to_thread() — the event loop is never
  stalled by a disk write.  Synchronous helpers (_write_rows, _write_ticket,
  _close_ticket_sync) are ONLY called from asyncio.to_thread() threads.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, CandleEvent

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = "data/state_snapshots.db"

# Minimum historical candles needed for each indicator to reach full parity:
#   RSI(14)  — needs 15 closes
#   ADX(20)  — needs 42 candles (2×period + 2)
#   VWAP(500)— needs up to 500 candles
# We request 500 on restore to cover the worst case (VWAP).
_RESTORE_CANDLE_DEPTH = 500


# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS candle_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT    NOT NULL,
    timeframe       INTEGER NOT NULL,
    timestamp       TEXT    NOT NULL,
    ltp             REAL,
    rsi             REAL,
    vwap_val        REAL,
    adx_val         REAL,
    plus_di         REAL,
    minus_di        REAL,
    ema_fast        REAL,
    ema_slow        REAL,
    atr_val         REAL,
    c_open          REAL,
    c_high          REAL,
    c_low           REAL,
    c_close         REAL,
    c_volume        INTEGER,
    saved_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_b_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying      TEXT    NOT NULL,
    phase           TEXT    NOT NULL,
    rolling_base    REAL    DEFAULT 0.0,
    htf_entry_level REAL    DEFAULT 0.0,
    trap_level      REAL    DEFAULT 0.0,
    trap_type       TEXT    DEFAULT '',
    void_since      TEXT,
    saved_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS order_tickets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id       TEXT    NOT NULL,
    broker_symbol   TEXT    NOT NULL,
    side            TEXT    NOT NULL,
    qty             INTEGER NOT NULL,
    order_id        TEXT,
    avg_price       REAL    DEFAULT 0.0,
    strategy_tag    TEXT    DEFAULT '',
    status          TEXT    DEFAULT 'open',
    saved_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_params (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id       TEXT    NOT NULL,
    capital         REAL,
    max_risk_pct    REAL,
    daily_pnl       REAL    DEFAULT 0.0,
    trade_count     INTEGER DEFAULT 0,
    is_halted       INTEGER DEFAULT 0,
    saved_at        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cs_sym_tf  ON candle_snapshots(symbol, timeframe, saved_at DESC);
CREATE INDEX IF NOT EXISTS idx_sb_und     ON strategy_b_state(underlying, saved_at DESC);
CREATE INDEX IF NOT EXISTS idx_ot_client  ON order_tickets(client_id, saved_at DESC);
CREATE INDEX IF NOT EXISTS idx_rp_client  ON risk_params(client_id, saved_at DESC);
"""


# ─────────────────────────────────────────────────────────────────────────────
# State Persistence Engine
# ─────────────────────────────────────────────────────────────────────────────

class StatePersistence:
    """
    Subscribes to CANDLE_CLOSE and flushes tech snapshot + strategy state
    to a local SQLite database via non-blocking asyncio.to_thread writes.

    Usage:
        persist = StatePersistence(bus, cfg)
        await persist.initialise()              # Create tables
        asyncio.create_task(persist.run())      # Start snapshot loop
        ...
        # On boot (after initialise, before run):
        history = persist.restore_candle_history(symbols, timeframes)
        for (sym, tf), df in history.items():
            candle_cache.load_history(sym, tf, df)
        state = persist.restore_state()         # Strategy B + risk params
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        db_path: str = _DEFAULT_DB_PATH,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._db_path = db_path
        self._candle_queue = bus.subscribe(Topic.CANDLE_CLOSE)
        self._running = False
        # Callbacks registered externally to avoid circular imports
        self._snapshot_provider: Optional[Callable] = None
        self._strategy_b_provider: Optional[Callable] = None
        self._risk_provider: Optional[Callable] = None
        self._flush_count = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def initialise(self) -> None:
        """Create tables and indexes. Safe to call on every boot."""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._create_tables)
        logger.info("StatePersistence: DB ready at %s", self._db_path)

    async def run(self) -> None:
        self._running = True
        logger.info("StatePersistence: snapshot loop started.")
        while self._running:
            try:
                candle: CandleEvent = await asyncio.wait_for(
                    self._candle_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            await self._on_candle_close(candle)

    def stop(self) -> None:
        self._running = False

    # ── Provider registration ─────────────────────────────────────────────────

    def register_snapshot_provider(self, fn: Callable) -> None:
        """fn(symbol, timeframe) -> Optional[TechSnapshot]"""
        self._snapshot_provider = fn

    def register_strategy_b_provider(self, fn: Callable) -> None:
        """fn() -> Dict[str, dict]  (keys: phase, rolling_base, htf_entry_level, ...)"""
        self._strategy_b_provider = fn

    def register_risk_provider(self, fn: Callable) -> None:
        """fn() -> List[dict]  (keys: client_id, capital, max_risk_pct, daily_pnl, ...)"""
        self._risk_provider = fn

    # ── Candle close handler ──────────────────────────────────────────────────

    async def _on_candle_close(self, candle: CandleEvent) -> None:
        now_str = datetime.now(IST).isoformat()
        rows_snap: list = []
        rows_strat: list = []
        rows_risk: list = []

        # Tech snapshot — persisted on every candle close (all timeframes)
        if self._snapshot_provider:
            snap = self._snapshot_provider(candle.symbol, candle.timeframe)
            if snap is not None:
                rows_snap.append({
                    "symbol":    snap.symbol,
                    "timeframe": snap.timeframe,
                    "timestamp": snap.timestamp.isoformat(),
                    "ltp":       snap.ltp,
                    "rsi":       snap.rsi,
                    "vwap_val":  snap.vwap_val,
                    "adx_val":   snap.adx_val,
                    "plus_di":   snap.plus_di,
                    "minus_di":  snap.minus_di,
                    "ema_fast":  snap.ema_fast,
                    "ema_slow":  snap.ema_slow,
                    "atr_val":   snap.atr_val,
                    "c_open":    snap.c_open,
                    "c_high":    snap.c_high,
                    "c_low":     snap.c_low,
                    "c_close":   snap.c_close,
                    "c_volume":  snap.c_volume,
                    "saved_at":  now_str,
                })

        # Strategy B state — only on the highest timeframe (avoid excessive rows)
        if self._strategy_b_provider and candle.timeframe == self._cfg.candle_timeframes[-1]:
            states = self._strategy_b_provider()
            for underlying, st in states.items():
                rows_strat.append({
                    "underlying":      underlying,
                    "phase":           st.get("phase", "IDLE"),
                    "rolling_base":    st.get("rolling_base", 0.0),
                    "htf_entry_level": st.get("htf_entry_level", 0.0),
                    "trap_level":      st.get("trap_level", 0.0),
                    "trap_type":       st.get("trap_type", ""),
                    "void_since":      st.get("void_since"),
                    "saved_at":        now_str,
                })

        # Risk params — on the fastest timeframe only (cheap dict, no need for every tf)
        if self._risk_provider and candle.timeframe == self._cfg.candle_timeframes[0]:
            for row in self._risk_provider():
                row["saved_at"] = now_str
                rows_risk.append(row)

        # All writes go through asyncio.to_thread — event loop never stalled
        if rows_snap or rows_strat or rows_risk:
            await asyncio.to_thread(
                self._write_rows, rows_snap, rows_strat, rows_risk
            )
            self._flush_count += 1

    # ── Boot recovery: candle history ─────────────────────────────────────────

    def restore_candle_history(
        self,
        symbols: List[str],
        timeframes: List[int],
        n_candles: int = _RESTORE_CANDLE_DEPTH,
    ) -> Dict[Tuple[str, int], Any]:
        """
        Returns the last n_candles OHLCV rows per symbol×timeframe as a
        pandas DataFrame with columns [open, high, low, close, volume] and a
        DatetimeIndex.

        Feed directly to CandleCache.load_history(symbol, tf, df) to restore
        the indicator ring-buffer state so that:
          • RSI(14)  is immediately accurate (needs 15 closes; you get 500)
          • ADX(20)  is immediately accurate (needs 42 candles; you get 500)
          • VWAP(500) is fully accurate (needs exactly 500 candles; you get 500)

        Caller:
            history = persist.restore_candle_history(cfg.monitored_indices,
                                                     cfg.candle_timeframes)
            for (sym, tf), df in history.items():
                cache.load_history(sym, tf, df)
        """
        try:
            import pandas as pd
        except ImportError:
            logger.error("StatePersistence: pandas not available — cannot restore candle history.")
            return {}

        result: Dict[Tuple[str, int], Any] = {}
        try:
            con = sqlite3.connect(self._db_path)
            for sym in symbols:
                for tf in timeframes:
                    cur = con.cursor()
                    # Fetch the last n_candles rows ordered by saved_at DESC,
                    # then reverse so the DataFrame is oldest-first (required by load_history)
                    cur.execute(
                        """
                        SELECT timestamp, c_open, c_high, c_low, c_close, c_volume
                        FROM candle_snapshots
                        WHERE symbol = ? AND timeframe = ?
                        ORDER BY saved_at DESC
                        LIMIT ?
                        """,
                        (sym, tf, n_candles),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        continue
                    rows.reverse()   # oldest-first
                    df = pd.DataFrame(
                        rows,
                        columns=["timestamp", "open", "high", "low", "close", "volume"],
                    )
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                    df = df.set_index("timestamp")
                    result[(sym, tf)] = df
                    logger.info(
                        "StatePersistence: restored %d candles for %s/%dm.",
                        len(df), sym, tf,
                    )
            con.close()
        except Exception as exc:
            logger.error("StatePersistence: restore_candle_history() error: %s", exc)
        return result

    # ── Boot recovery: scalar state ───────────────────────────────────────────

    def restore_state(self) -> Dict[str, Any]:
        """
        Synchronous boot-time call.  Returns latest scalar snapshots and
        Strategy B state dicts.  Call restore_candle_history() separately
        to warm up indicator ring buffers.

        Returns:
          {
            "snapshots":     {(symbol, timeframe): row_dict, ...},
            "strategy_b":    {underlying: row_dict, ...},
            "order_tickets": [row_dict, ...],   # status='open' only
            "risk_params":   {client_id: row_dict, ...},
          }
        """
        result: Dict[str, Any] = {
            "snapshots": {},
            "strategy_b": {},
            "order_tickets": [],
            "risk_params": {},
        }
        try:
            con = sqlite3.connect(self._db_path)
            con.row_factory = sqlite3.Row
            cur = con.cursor()

            # Latest indicator snapshot per symbol × timeframe
            cur.execute("""
                SELECT cs.*
                FROM candle_snapshots cs
                INNER JOIN (
                    SELECT symbol, timeframe, MAX(saved_at) AS max_ts
                    FROM candle_snapshots
                    GROUP BY symbol, timeframe
                ) latest ON cs.symbol = latest.symbol
                         AND cs.timeframe = latest.timeframe
                         AND cs.saved_at = latest.max_ts
            """)
            for row in cur.fetchall():
                result["snapshots"][(row["symbol"], row["timeframe"])] = dict(row)

            # Latest Strategy B state per underlying
            cur.execute("""
                SELECT sb.*
                FROM strategy_b_state sb
                INNER JOIN (
                    SELECT underlying, MAX(saved_at) AS max_ts
                    FROM strategy_b_state
                    GROUP BY underlying
                ) latest ON sb.underlying = latest.underlying
                         AND sb.saved_at = latest.max_ts
            """)
            for row in cur.fetchall():
                result["strategy_b"][row["underlying"]] = dict(row)

            # All open order tickets (active positions)
            cur.execute(
                "SELECT * FROM order_tickets WHERE status = 'open' ORDER BY saved_at DESC"
            )
            result["order_tickets"] = [dict(r) for r in cur.fetchall()]

            # Latest risk snapshot per client
            cur.execute("""
                SELECT rp.*
                FROM risk_params rp
                INNER JOIN (
                    SELECT client_id, MAX(saved_at) AS max_ts
                    FROM risk_params
                    GROUP BY client_id
                ) latest ON rp.client_id = latest.client_id
                         AND rp.saved_at = latest.max_ts
            """)
            for row in cur.fetchall():
                result["risk_params"][row["client_id"]] = dict(row)

            con.close()
        except Exception as exc:
            logger.warning("StatePersistence: restore_state() error: %s", exc)
        return result

    # ── Order ticket helpers (async — never block the event loop) ─────────────

    async def persist_order_ticket(
        self,
        client_id: str,
        broker_symbol: str,
        side: str,
        qty: int,
        order_id: str,
        avg_price: float,
        strategy_tag: str = "",
    ) -> None:
        """
        Async — safe to call from any async context.
        Writes via asyncio.to_thread so the event loop is not stalled.
        """
        row = {
            "client_id":    client_id,
            "broker_symbol": broker_symbol,
            "side":          side,
            "qty":           qty,
            "order_id":      order_id,
            "avg_price":     avg_price,
            "strategy_tag":  strategy_tag,
            "status":        "open",
            "saved_at":      datetime.now(IST).isoformat(),
        }
        await asyncio.to_thread(self._write_ticket, row)

    async def close_order_ticket(self, order_id: str) -> None:
        """
        Async — safe to call from any async context.
        Marks the order ticket closed (position exited) without blocking.
        """
        await asyncio.to_thread(self._close_ticket_sync, order_id)

    @property
    def flush_count(self) -> int:
        return self._flush_count

    # ── SQLite helpers — ONLY called from asyncio.to_thread() ─────────────────

    def _create_tables(self) -> None:
        con = sqlite3.connect(self._db_path)
        con.executescript(_DDL)
        con.commit()
        con.close()

    def _write_rows(
        self,
        rows_snap: list,
        rows_strat: list,
        rows_risk: list,
    ) -> None:
        con = sqlite3.connect(self._db_path)
        try:
            if rows_snap:
                con.executemany(
                    """
                    INSERT INTO candle_snapshots
                        (symbol, timeframe, timestamp, ltp, rsi, vwap_val, adx_val,
                         plus_di, minus_di, ema_fast, ema_slow, atr_val,
                         c_open, c_high, c_low, c_close, c_volume, saved_at)
                    VALUES
                        (:symbol, :timeframe, :timestamp, :ltp, :rsi, :vwap_val, :adx_val,
                         :plus_di, :minus_di, :ema_fast, :ema_slow, :atr_val,
                         :c_open, :c_high, :c_low, :c_close, :c_volume, :saved_at)
                    """,
                    rows_snap,
                )
            if rows_strat:
                con.executemany(
                    """
                    INSERT INTO strategy_b_state
                        (underlying, phase, rolling_base, htf_entry_level,
                         trap_level, trap_type, void_since, saved_at)
                    VALUES
                        (:underlying, :phase, :rolling_base, :htf_entry_level,
                         :trap_level, :trap_type, :void_since, :saved_at)
                    """,
                    rows_strat,
                )
            if rows_risk:
                con.executemany(
                    """
                    INSERT OR REPLACE INTO risk_params
                        (client_id, capital, max_risk_pct, daily_pnl,
                         trade_count, is_halted, saved_at)
                    VALUES
                        (:client_id, :capital, :max_risk_pct, :daily_pnl,
                         :trade_count, :is_halted, :saved_at)
                    """,
                    rows_risk,
                )
            con.commit()
        except Exception as exc:
            logger.error("StatePersistence: _write_rows() error: %s", exc)
        finally:
            con.close()

    def _write_ticket(self, row: dict) -> None:
        try:
            con = sqlite3.connect(self._db_path)
            con.execute(
                """
                INSERT INTO order_tickets
                    (client_id, broker_symbol, side, qty, order_id,
                     avg_price, strategy_tag, status, saved_at)
                VALUES
                    (:client_id, :broker_symbol, :side, :qty, :order_id,
                     :avg_price, :strategy_tag, :status, :saved_at)
                """,
                row,
            )
            con.commit()
            con.close()
        except Exception as exc:
            logger.error("StatePersistence: _write_ticket() error: %s", exc)

    def _close_ticket_sync(self, order_id: str) -> None:
        try:
            con = sqlite3.connect(self._db_path)
            con.execute(
                "UPDATE order_tickets SET status = 'closed' WHERE order_id = ?",
                (order_id,),
            )
            con.commit()
            con.close()
        except Exception as exc:
            logger.error("StatePersistence: _close_ticket_sync() error: %s", exc)
