"""
strategies/fno_stock_monitor.py — FnO Stock Stage-2 Intraday Monitor.

Watches nightly-scan shortlisted stocks during market hours. Builds 15m (MTF)
and 5m (LTF) spot candle bars from live INDEX_TICK events. When MTF shows a
new TRAPPED zone matching the D1 direction, arms LTF. When LTF also shows a
TRAPPED zone, fires Topic.FNO_STOCK_ALERT with full trade details.

No auto-trade. Alert-only.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import math
import os
from datetime import date, datetime, time as dtime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, IndexTick

logger = logging.getLogger(__name__)

MARKET_OPEN  = dtime(9, 15, 0)
MARKET_CLOSE = dtime(15, 15, 0)
EOD_CLEAR    = dtime(15, 30, 0)


@dataclasses.dataclass
class FnoStockAlert:
    uid: str              # dedup key: f"{symbol}_{direction}_{zone_high:.0f}"
    symbol: str
    direction: str        # "CE" | "PE"
    spot_price: float
    d1_zone_low: float
    d1_zone_high: float
    d1_zone_date: str     # "Jun 30"
    strike: int
    lot_size: int
    sl: float
    t1: float
    risk_pts: float
    reward_pts: float
    rr_ratio: float
    mtf_trap_price: float
    ltf_trap_price: float
    fired_at: datetime


_SCAN_DIR = "data"


class FnoStockMonitor:
    """
    Intraday MTF+LTF cascade alert monitor for FnO stocks.

    Lifecycle (called by run_system.py):
      1. warm_start()       — load today's scan file
      2. set_feeder(f)      — subscribe + register stock spot instrument keys
      3. await start()      — subscribe INDEX_TICK queue, run async loop
    """

    def __init__(
        self,
        bus: EventBus,
        cfg,
        client_db=None,
        scan_dir: str = _SCAN_DIR,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._db  = client_db
        self._scan_dir = scan_dir
        self._feeder = None

        # State — populated by warm_start()
        self._watched: Dict[str, dict] = {}          # symbol → scan entry
        self._buckets: Dict[str, dict] = {}          # bkey (e.g. "5m_EICHERMOT") → open bucket
        self._bars_5m:  Dict[str, List[dict]] = {}   # symbol → closed 5m bars
        self._bars_15m: Dict[str, List[dict]] = {}   # symbol → closed 15m bars
        self._ltf_armed: Dict[str, float] = {}       # symbol → mtf_trap_price
        self._active_alerts: List[FnoStockAlert] = []
        self._notified_uids: Set[str] = set()
        self._running = False
        self._tasks: List[asyncio.Task] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def warm_start(self) -> None:
        """Load today's scan file. No-op if file missing (no scan ran today)."""
        today = date.today().isoformat()
        path  = os.path.join(self._scan_dir, f"fno_scan_{today}.json")
        if not os.path.exists(path):
            logger.info("FnoStockMonitor: no scan file for %s — idle", today)
            return
        # Build instrument_key lookup from fno_stocks.csv as fallback.
        csv_keys: dict = {}
        csv_path = os.path.join(self._scan_dir, "fno_stocks.csv")
        if os.path.exists(csv_path):
            try:
                import csv as _csv
                with open(csv_path) as cf:
                    for row in _csv.reader(cf):
                        if len(row) >= 2:
                            csv_keys[row[0].strip()] = row[1].strip()
            except Exception:
                pass
        with open(path) as f:
            data = json.load(f)
        for entry in (data.get("ce_stocks") or []) + (data.get("pe_stocks") or []):
            sym = entry.get("symbol")
            if not sym:
                continue
            if not entry.get("instrument_key") and sym in csv_keys:
                entry["instrument_key"] = csv_keys[sym]
            self._watched[sym] = entry
            self._bars_5m[sym]  = []
            self._bars_15m[sym] = []
        logger.info("FnoStockMonitor: watching %d stocks: %s",
                    len(self._watched), list(self._watched))

    def set_feeder(self, feeder) -> None:
        self._feeder = feeder
        if not self._watched:
            return
        mapping = {
            e["instrument_key"]: sym
            for sym, e in self._watched.items()
            if e.get("instrument_key")
        }
        keys = list(mapping.keys())
        if hasattr(feeder, "register_extra_spot_keys"):
            feeder.register_extra_spot_keys(mapping)
        asyncio.ensure_future(feeder.subscribe_tokens(keys))
        logger.info("FnoStockMonitor: subscribed %d spot keys", len(keys))

    async def start(self) -> None:
        if not self._watched:
            logger.info("FnoStockMonitor: no stocks to watch — idle")
            # Stay alive so run_system task-monitor doesn't trigger shutdown.
            while True:
                await asyncio.sleep(3600)
        self._running = True
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._tasks.append(asyncio.create_task(self._tick_loop(q)))
        self._tasks.append(asyncio.create_task(self._eod_clear_loop()))
        self._tasks.append(asyncio.create_task(self._status_broadcast_loop()))
        logger.info("FnoStockMonitor: started — watching %d stocks", len(self._watched))
        # Keep coroutine alive; real work happens in sub-tasks.
        while self._running:
            await asyncio.sleep(60)

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    def get_active_alerts(self) -> List[dict]:
        return [dataclasses.asdict(a) for a in self._active_alerts]

    def get_status(self) -> List[dict]:
        """Return live status for each watched stock — for dashboard display."""
        out = []
        for sym, entry in self._watched.items():
            bkey5  = f"5m_{sym}"
            bkey15 = f"15m_{sym}"
            cur5   = self._buckets.get(bkey5, {})
            cur15  = self._buckets.get(bkey15, {})
            ltp = cur5.get("close") or cur15.get("close") or 0.0
            mtf_state = "TRAPPED ✓" if sym in self._ltf_armed else "WATCH"
            ltf_state = "ARMED" if sym in self._ltf_armed else "—"
            out.append({
                "symbol":    sym,
                "direction": entry["direction"],
                "ltp":       round(ltp, 2),
                "sl":        entry["sl"],
                "t1":        entry["t1"],
                "zone_high": entry["zone_high"],
                "zone_low":  entry["zone_low"],
                "rr_ratio":  entry["rr_ratio"],
                "mtf_state": mtf_state,
                "ltf_state": ltf_state,
                "bars_5m":   len(self._bars_5m.get(sym, [])),
                "bars_15m":  len(self._bars_15m.get(sym, [])),
            })
        return out

    def mark_notified(self, uid: str) -> None:
        self._notified_uids.add(uid)
        self._active_alerts = [a for a in self._active_alerts if a.uid != uid]
        logger.info("FnoStockMonitor: alert %s marked notified", uid)

    # ── Internal loops ────────────────────────────────────────────────────────

    async def _tick_loop(self, q: asyncio.Queue) -> None:
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            if tick.symbol not in self._watched:
                continue
            now_t = tick.timestamp.time()
            if not (MARKET_OPEN <= now_t <= MARKET_CLOSE):
                continue
            self._check_sl_breach(tick)
            if tick.symbol not in self._watched:
                continue
            self._process_tick(tick)

    async def _eod_clear_loop(self) -> None:
        """Clear active alerts at 15:30 IST."""
        while self._running:
            await asyncio.sleep(60)
            now_t = datetime.now(IST).time()
            if now_t >= EOD_CLEAR:
                self._active_alerts.clear()
                logger.info("FnoStockMonitor: EOD cleared alerts")
                break

    async def _status_broadcast_loop(self) -> None:
        """Publish fno_stock_status to EventBus every 3s for dashboard live update."""
        while self._running:
            await asyncio.sleep(3)
            try:
                status = self.get_status()
                if status:
                    await self._bus.publish(Topic.FNO_STOCK_STATUS, status)
            except Exception:
                pass

    # ── Tick processing ───────────────────────────────────────────────────────

    def _check_sl_breach(self, tick: IndexTick) -> None:
        entry = self._watched.get(tick.symbol)
        if not entry:
            return
        sl = entry["sl"]
        direction = entry["direction"]
        breached = (direction == "PE" and tick.ltp > sl) or \
                   (direction == "CE" and tick.ltp < sl)
        if breached:
            logger.info("FnoStockMonitor: %s D1 SL breached (ltp=%.2f sl=%.2f) — removed",
                        tick.symbol, tick.ltp, sl)
            self._watched.pop(tick.symbol, None)

    def _process_tick(self, tick: IndexTick) -> None:
        sym = tick.symbol
        ts  = tick.timestamp

        bar5 = self._update_bucket("5m", sym, tick.ltp, ts)
        if bar5:
            self._bars_5m[sym].append(bar5)
            self._on_5m_close(sym, tick.ltp)

        bar15 = self._update_bucket("15m", sym, tick.ltp, ts)
        if bar15:
            self._bars_15m[sym].append(bar15)
            self._on_15m_close(sym, tick.ltp)

    def _update_bucket(self, tf: str, symbol: str, ltp: float, ts: datetime) -> Optional[dict]:
        """
        Returns completed bar dict when a new bucket boundary is crossed, else None.
        tf must be e.g. "5m" or "15m"; minutes is parsed from the prefix.
        bkey = f"{tf}_{symbol}"
        """
        minutes = int(tf.rstrip("m"))
        bkey = f"{tf}_{symbol}"
        # Align to minute boundary (floor to `minutes` interval)
        m = (ts.minute // minutes) * minutes
        bucket_ts = ts.replace(minute=m, second=0, microsecond=0)

        if bkey not in self._buckets:
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp}
            return None
        b = self._buckets[bkey]
        if b["ts"] != bucket_ts:
            completed = {"datetime": b["ts"].isoformat(),
                         "open": b["open"], "high": b["high"],
                         "low": b["low"], "close": b["close"], "volume": 0}
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp}
            return completed
        b["high"]  = max(b["high"],  ltp)
        b["low"]   = min(b["low"],   ltp)
        b["close"] = ltp
        return None

    def _on_15m_close(self, sym: str, current_ltp: float) -> None:
        if sym in self._ltf_armed:
            return  # already armed — don't re-check MTF
        bars = self._bars_15m[sym]
        if len(bars) < 2:
            return
        entry = self._watched[sym]
        direction = entry["direction"]
        kind = "BEAR" if direction == "CE" else "BULL"
        trap = self._find_new_trap(bars, kind)
        if trap:
            self._ltf_armed[sym] = current_ltp
            logger.info("FnoStockMonitor: %s MTF ARMED — 15m %s trap at %.2f",
                        sym, kind, current_ltp)

    def _on_5m_close(self, sym: str, current_ltp: float) -> None:
        if sym not in self._ltf_armed:
            return
        uid = self._make_uid(sym)
        if uid in self._notified_uids:
            return
        bars = self._bars_5m[sym]
        if len(bars) < 2:
            return
        entry = self._watched[sym]
        direction = entry["direction"]
        kind = "BEAR" if direction == "CE" else "BULL"
        trap = self._find_new_trap(bars, kind)
        if trap:
            self._fire_alert(sym, entry, current_ltp, uid)

    def _find_new_trap(self, bars: List[dict], kind: str) -> Optional[dict]:
        """Run scan_htf_spot on bars; return the most recent TRAPPED zone matching kind, or None."""
        from strategies.trap_scanner import scanner
        df = pd.DataFrame(bars)
        df["datetime"] = pd.to_datetime(df["datetime"])
        try:
            _, zones = scanner.scan_htf_spot(df)
        except Exception as exc:
            logger.debug("FnoStockMonitor: scan_htf_spot error: %s", exc)
            return None
        today_str = date.today().isoformat()
        candidates = [
            z for z in zones
            if z.get("kind") == kind
            and z.get("status") == "TRAPPED"
            and str(z.get("trapped_on", ""))[:10] == today_str
        ]
        return candidates[-1] if candidates else None

    def _make_uid(self, sym: str) -> str:
        entry = self._watched[sym]
        return f"{sym}_{entry['direction']}_{entry['zone_high']:.0f}"

    def _fire_alert(self, sym: str, entry: dict, spot: float, uid: str) -> None:
        mtf_price = self._ltf_armed.get(sym, spot)
        alert = FnoStockAlert(
            uid=uid,
            symbol=sym,
            direction=entry["direction"],
            spot_price=spot,
            d1_zone_low=entry["zone_low"],
            d1_zone_high=entry["zone_high"],
            d1_zone_date=entry.get("zone_date", ""),
            strike=entry["strike"],
            lot_size=entry["lot_size"],
            sl=entry["sl"],
            t1=entry["t1"],
            risk_pts=entry["risk_pts"],
            reward_pts=entry["reward_pts"],
            rr_ratio=entry["rr_ratio"],
            mtf_trap_price=mtf_price,
            ltf_trap_price=spot,
            fired_at=datetime.now(IST),
        )
        self._active_alerts.append(alert)
        self._notified_uids.add(uid)
        self._bus.publish(Topic.FNO_STOCK_ALERT, dataclasses.asdict(alert))
        logger.info("FnoStockMonitor: ALERT %s %s R:R %.1f×",
                    sym, entry["direction"], entry["rr_ratio"])
