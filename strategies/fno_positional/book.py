"""
strategies/fno_positional/book.py — FnO Positional Option Buyer

Manages up to MAX_SLOTS concurrent long-option positions on NSE FnO stocks.
Signals come from backtest/fno_scanner/scan_live.scan() — bear-trap zones
generate CE buys, bull-trap zones generate PE buys.

Entry mechanics
---------------
- Run scan at startup (once, before market open).  TRIGGERED signals enter at
  market open (9:16–9:30 IST); APPROACHING signals enter when spot touches the
  entry_line intraday.
- Orders published to Topic.FNO_ORDER_REQUEST → picked up by FnOExecutionBridge.

Position monitoring
-------------------
- Polls spot + option LTP via Upstox REST every POLL_INTERVAL seconds.
- SL condition is spot-based (not option premium): spot ≤ spot_sl (CE) or
  spot ≥ spot_sl (PE).
- On SL hit: close position, trigger rescan to fill the vacant slot.
- On T1 hit: alert published to dashboard; manual hedge decision v1.
- EOD 15:20 IST: force-close all remaining open positions.

Persistence: state is saved to data/fno_positions.json after every change so
a quick pm2-restart can recover in-flight positions without re-entering.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.global_config import IST, Topic
from utils.logging_utils import make_strategy_logger

logger = logging.getLogger(__name__)

ROOT           = Path(__file__).resolve().parents[2]
POSITIONS_PATH = ROOT / "data" / "fno_positions.json"

MAX_SLOTS        = 2
POLL_INTERVAL    = 30          # seconds between REST LTP polls
ENTRY_TIME_START   = time(9, 15)
ENTRY_TIME_END     = time(14, 30)   # positional — enter any time a zone fires during the day
MARKET_OPEN        = time(9, 15)
MARKET_CLOSE       = time(15, 30)
EXPIRY_WEEK_DAYS   = 7   # close position when ≤7 days left on expiry
GAP_SKIP_PCT       = 2.5 # skip entry if spot gapped >2.5% from entry_line


# ─────────────────────────────────────────────────────────────────────────────
# Instrument-master cache (shared across books; refreshed once per day)
# ─────────────────────────────────────────────────────────────────────────────

_MASTER_CACHE: Dict[str, Any] = {}   # {"instruments": [...], "date": date}


def _get_master(token: str) -> list:
    """Return cached NSE instrument master, refreshing if it's from a previous day."""
    today = date.today()
    if _MASTER_CACHE.get("date") == today and _MASTER_CACHE.get("instruments"):
        return _MASTER_CACHE["instruments"]
    try:
        from curl_cffi import requests as cc
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
        r = cc.get(url, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
        _MASTER_CACHE["instruments"] = instruments
        _MASTER_CACHE["date"] = today
        logger.info("FnO: instrument master refreshed — %d entries", len(instruments))
        return instruments
    except Exception as exc:
        logger.warning("FnO: could not fetch instrument master: %s", exc)
        return _MASTER_CACHE.get("instruments", [])


# ─────────────────────────────────────────────────────────────────────────────
# Symbol helpers
# ─────────────────────────────────────────────────────────────────────────────

def zerodha_monthly_symbol(symbol: str, strike: int, direction: str, expiry_str: str) -> str:
    """Zerodha NFO monthly option symbol.

    Zerodha format: {SYMBOL}{YY}{MON}{STRIKE}{CE|PE}
    expiry_str format: "28 AUG 26"  → year_code="26", month="AUG"
    e.g. RELIANCE26AUG1280CE
    """
    parts = expiry_str.upper().split()   # ["28", "AUG", "26"]
    yy  = parts[2] if len(parts) >= 3 else "26"
    mon = parts[1] if len(parts) >= 2 else "AUG"
    return f"{symbol.upper()}{yy}{mon}{int(strike)}{direction.upper()}"


def resolve_option_key(instruments: list, symbol: str, strike: int,
                       direction: str, expiry_str: str) -> str:
    """Find Upstox NSE_FO instrument_key for a given option contract.

    First tries an exact match on trading_symbol; falls back to nearest available
    strike for the same symbol/direction/expiry.
    """
    target = f"{symbol.upper()} {int(strike)} {direction.upper()} {expiry_str.upper()}"
    for inst in instruments:
        if inst.get("segment") != "NSE_FO":
            continue
        if inst.get("trading_symbol", "").upper() == target:
            return inst.get("instrument_key", "")

    # Nearest-strike fallback
    try:
        parts  = expiry_str.upper().split()
        exp_dt = datetime.strptime(f"{parts[0]} {parts[1]} {parts[2]}", "%d %b %y").date()
    except Exception:
        return ""

    candidates = []
    for inst in instruments:
        if inst.get("segment") != "NSE_FO":
            continue
        if inst.get("instrument_type", "").upper() != direction.upper():
            continue
        ts = inst.get("trading_symbol", "")
        p  = ts.split()
        if len(p) < 6 or p[0].upper() != symbol.upper():
            continue
        try:
            ed = datetime.strptime(f"{p[3]} {p[4]} {p[5]}", "%d %b %y").date()
            if ed == exp_dt:
                candidates.append((abs(int(float(p[1])) - strike),
                                   int(float(p[1])),
                                   inst.get("instrument_key", "")))
        except Exception:
            continue
    if candidates:
        candidates.sort()
        logger.info("FnO: nearest strike %d (target=%d) key=%s",
                    candidates[0][1], strike, candidates[0][2])
        return candidates[0][2]
    return ""


def resolve_lot_size(instruments: list, option_key: str) -> int:
    for inst in instruments:
        if inst.get("instrument_key") == option_key:
            return int(inst.get("lot_size") or 1)
    return 1


# ─────────────────────────────────────────────────────────────────────────────
# Position record
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FnOPosition:
    slot_id:              str
    symbol:               str
    direction:            str            # "CE" | "PE"
    spot_instrument_key:  str
    option_instrument_key: str
    broker_symbol:        str            # Zerodha NFO symbol e.g. "RELIANCE26AUG1280CE"
    strike:               int
    expiry_str:           str
    lot_size:             int
    qty:                  int
    spot_entry:           float
    spot_sl:              float
    day_t1:               float
    entry_ltp:            float  = 0.0
    current_spot:         float  = 0.0
    current_ltp:          float  = 0.0
    entry_order_id:       str    = ""
    exit_order_id:        str    = ""
    status:               str    = "PENDING"
    open_time:            str    = ""
    close_time:           str    = ""
    close_reason:         str    = ""
    pnl:                  float  = 0.0
    client_id:            str    = ""
    binding_id:           str    = ""


# ─────────────────────────────────────────────────────────────────────────────
# Book
# ─────────────────────────────────────────────────────────────────────────────

class FnOPositionalBook:
    """One independent FnO positional scanner per (client, binding) deployment."""

    def __init__(
        self,
        bus,
        upstox_token: str,
        client_id: str,
        binding_id: str,
        mode: str = "paper",
        max_slots: int = MAX_SLOTS,
    ):
        self._bus        = bus
        self._token      = upstox_token
        self._client_id  = client_id
        self._binding_id = binding_id
        self._mode       = mode
        self._max_slots  = max_slots
        self._positions: List[FnOPosition] = []
        self._pending:   list              = []    # Signal objects waiting for entry
        self._scan_done  = False
        self._running    = False
        self._task: Optional[asyncio.Task] = None
        self._instruments: list            = []    # cached NSE master

        date_str = datetime.now(IST).strftime("%Y%m%d")
        self._log = make_strategy_logger(
            f"fno_{client_id}_{binding_id}_{date_str}",
            log_dir="logs/clients",
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._load_positions()
        # Kick off the main loop as an asyncio task; instrument master fetch happens
        # inside _main_loop before the first scan so start() stays synchronous (the
        # base class _reconcile() calls book.start() without await).
        self._task = asyncio.create_task(self._startup_and_loop(), name=f"fno_{self._client_id}_{self._binding_id}")
        self._log.info("FnOBook[%s/%s]: started (mode=%s, max_slots=%d)",
                       self._client_id, self._binding_id, self._mode, self._max_slots)

    async def _startup_and_loop(self) -> None:
        """Fetch instrument master then run the main loop. Called as a task by start()."""
        try:
            self._instruments = await asyncio.to_thread(_get_master, self._token)
        except Exception as exc:
            self._log.warning("FnOBook[%s/%s]: instrument master fetch failed: %s — proceeding without pre-warm",
                              self._client_id, self._binding_id, exc)
        await self._main_loop()

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load_positions(self) -> None:
        if not POSITIONS_PATH.exists():
            return
        try:
            raw = json.loads(POSITIONS_PATH.read_text(encoding="utf-8"))
            all_pos = [FnOPosition(**p) for p in raw.get("positions", [])]
            # Restore only positions owned by this book that are not closed
            self._positions = [
                p for p in all_pos
                if p.client_id == self._client_id
                and p.binding_id == self._binding_id
                and p.status not in ("CLOSED",)
            ]
            self._log.info("FnOBook[%s/%s]: restored %d positions",
                           self._client_id, self._binding_id, len(self._positions))
        except Exception as exc:
            self._log.warning("FnOBook: restore failed: %s", exc)

    def _save_positions(self) -> None:
        try:
            # Merge with positions from other books (different client/binding)
            existing: list = []
            if POSITIONS_PATH.exists():
                raw = json.loads(POSITIONS_PATH.read_text(encoding="utf-8"))
                existing = [
                    p for p in raw.get("positions", [])
                    if not (p.get("client_id") == self._client_id
                            and p.get("binding_id") == self._binding_id)
                ]
            merged = existing + [asdict(p) for p in self._positions]
            POSITIONS_PATH.write_text(json.dumps({
                "positions":  merged,
                "updated_at": datetime.now(IST).isoformat(),
            }, indent=2), encoding="utf-8")
        except Exception as exc:
            self._log.warning("FnOBook: save failed: %s", exc)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _main_loop(self) -> None:
        _last_reset_date = None
        while self._running:
            now        = datetime.now(IST)
            t          = now.time()
            today_date = now.date()

            # Daily reset — once per day after market close
            if t >= MARKET_CLOSE and _last_reset_date != today_date:
                self._scan_done     = False
                self._pending       = []
                _last_reset_date    = today_date

            # Run scan once per day before entry window
            if not self._scan_done and t >= time(9, 0):
                await self._run_scan()
                self._scan_done = True

            # Entry window: check both TRIGGERED and APPROACHING signals all day.
            # TRIGGERED = zone broken on D1 (enter with gap filter).
            # APPROACHING = zone not yet hit, enter when intraday price reaches entry_line.
            if ENTRY_TIME_START <= t <= ENTRY_TIME_END:
                await self._try_enter_triggered()
                await self._try_enter_approaching()

            # Monitor open positions during market hours
            if self._open_positions and MARKET_OPEN <= t <= MARKET_CLOSE:
                await self._poll_and_monitor()

            # Positional exit: close positions in last week of expiry
            if self._open_positions and MARKET_OPEN <= t <= time(15, 15):
                await self._check_expiry_exit()

            await asyncio.sleep(POLL_INTERVAL)

    # ── Scan ─────────────────────────────────────────────────────────────────

    async def _run_scan(self) -> None:
        self._log.info("FnOBook[%s/%s]: running zone scan...", self._client_id, self._binding_id)
        try:
            from backtest.fno_scanner.scan_live import scan as _scan
            signals = await asyncio.to_thread(_scan, self._token)
        except Exception as exc:
            self._log.error("FnOBook: scan failed: %s", exc)
            return
        triggered   = sorted([s for s in signals if s.status == "TRIGGERED"],
                              key=lambda s: s.rr, reverse=True)
        approaching = sorted([s for s in signals if s.status == "APPROACHING"],
                              key=lambda s: abs(s.dist_pct))
        self._pending = triggered + approaching
        self._log.info("FnOBook[%s/%s]: %d triggered, %d approaching",
                       self._client_id, self._binding_id, len(triggered), len(approaching))

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _try_enter_triggered(self) -> None:
        free = self._max_slots - len(self._open_positions)
        if free <= 0:
            return
        for sig in list(self._pending):
            if free <= 0:
                break
            if sig.status != "TRIGGERED":
                continue
            # Gap filter: if spot has moved >GAP_SKIP_PCT% from entry_line since
            # yesterday's close, the zone is blown — skip this signal entirely.
            spot_key = self._spot_key(sig.symbol)
            if spot_key:
                spot = await self._fetch_ltp(spot_key)
                if spot > 0:
                    gap_pct = abs(spot - sig.entry_line) / sig.entry_line * 100
                    if gap_pct > GAP_SKIP_PCT:
                        self._log.warning(
                            "FnOBook: SKIP %s %s — gap %.1f%% from zone %.1f (spot=%.1f) > %.1f%% threshold",
                            sig.symbol, sig.direction, gap_pct, sig.entry_line, spot, GAP_SKIP_PCT,
                        )
                        self._pending.remove(sig)
                        continue
            self._pending.remove(sig)
            await self._open_position(sig)
            free -= 1

    async def _try_enter_approaching(self) -> None:
        free = self._max_slots - len(self._open_positions)
        if free <= 0 or not self._pending:
            return
        for sig in list(self._pending):
            if free <= 0:
                break
            if sig.status != "APPROACHING":
                continue
            spot = await self._fetch_ltp(sig.spot_instrument_key if hasattr(sig, "spot_instrument_key")
                                         else self._spot_key(sig.symbol))
            if spot <= 0:
                continue
            touched = (
                (sig.direction == "CE" and spot <= sig.entry_line * 1.002) or
                (sig.direction == "PE" and spot >= sig.entry_line * 0.998)
            )
            if touched:
                self._pending.remove(sig)
                await self._open_position(sig)
                free -= 1

    async def _open_position(self, sig) -> None:
        from backtest.fno_scanner.backtest import TOP_30_STOCKS

        spot_key = TOP_30_STOCKS.get(sig.symbol, "")
        if not spot_key:
            self._log.warning("FnOBook: unknown symbol %s — skip", sig.symbol)
            return

        # Resolve option instrument key and broker symbol
        opt_key = await asyncio.to_thread(
            resolve_option_key,
            self._instruments, sig.symbol, sig.suggested_strike, sig.direction, sig.expiry,
        )
        if not opt_key:
            self._log.warning("FnOBook: could not resolve option key for %s %d %s %s",
                              sig.symbol, sig.suggested_strike, sig.direction, sig.expiry)
            return

        lot_size  = await asyncio.to_thread(resolve_lot_size, self._instruments, opt_key)
        zerodha_s = zerodha_monthly_symbol(sig.symbol, sig.suggested_strike, sig.direction, sig.expiry)
        qty       = max(1, 1) * lot_size   # 1 lot

        # Fetch current LTPs for fill reference
        spot_ltp = await self._fetch_ltp(spot_key) or sig.entry_line
        opt_ltp  = await self._fetch_ltp(opt_key)  or 0.0

        pos = FnOPosition(
            slot_id=f"fno_{sig.symbol}_{sig.direction}_{uuid.uuid4().hex[:6]}",
            symbol=sig.symbol,
            direction=sig.direction,
            spot_instrument_key=spot_key,
            option_instrument_key=opt_key,
            broker_symbol=zerodha_s,
            strike=sig.suggested_strike,
            expiry_str=sig.expiry,
            lot_size=lot_size,
            qty=qty,
            spot_entry=spot_ltp,
            spot_sl=sig.hard_sl,
            day_t1=sig.day_t1,
            current_spot=spot_ltp,
            current_ltp=opt_ltp,
            status="ENTRY_PLACED",
            open_time=datetime.now(IST).isoformat(),
            client_id=self._client_id,
            binding_id=self._binding_id,
        )

        order_id = await self._place_order("BUY", pos, opt_ltp)
        if order_id:
            pos.entry_order_id = order_id
            pos.entry_ltp      = opt_ltp
            pos.status         = "OPEN"
            self._log.info("FnOBook[%s/%s]: ENTRY %s %s %d %s  ltp=%.2f  sl=%.1f  t1=%.1f",
                           self._client_id, self._binding_id,
                           sig.symbol, sig.direction, sig.suggested_strike, sig.expiry,
                           opt_ltp, sig.hard_sl, sig.day_t1)
        else:
            pos.status       = "CLOSED"
            pos.close_reason = "entry_failed"

        self._positions.append(pos)
        self._save_positions()
        await self._bus.publish(Topic.SYSTEM_EVENT, {"type": "fno_entry", "pos": asdict(pos)})

    # ── Monitor ───────────────────────────────────────────────────────────────

    async def _poll_and_monitor(self) -> None:
        for pos in list(self._open_positions):
            spot = await self._fetch_ltp(pos.spot_instrument_key)
            opt  = await self._fetch_ltp(pos.option_instrument_key)

            if spot > 0:
                pos.current_spot = spot
            if opt  > 0:
                pos.current_ltp  = opt
            if pos.entry_ltp > 0 and opt > 0:
                pos.pnl = (opt - pos.entry_ltp) * pos.qty

            sl_hit = (
                (pos.direction == "CE" and spot > 0 and spot <= pos.spot_sl) or
                (pos.direction == "PE" and spot > 0 and spot >= pos.spot_sl)
            )
            t1_hit = (
                (pos.direction == "CE" and spot > 0 and spot >= pos.day_t1) or
                (pos.direction == "PE" and spot > 0 and spot <= pos.day_t1)
            )

            if sl_hit:
                self._log.warning("FnOBook[%s/%s]: SL HIT %s  spot=%.1f <= sl=%.1f",
                                  self._client_id, self._binding_id, pos.symbol, spot, pos.spot_sl)
                await self._close_position(pos, "sl_hit")
                await self._rescan_and_refill()

            elif t1_hit and pos.close_reason != "t1_hit":
                self._log.info("FnOBook[%s/%s]: T1 HIT %s  spot=%.1f >= t1=%.1f",
                               self._client_id, self._binding_id, pos.symbol, spot, pos.day_t1)
                pos.close_reason = "t1_hit"
                await self._bus.publish(Topic.SYSTEM_EVENT, {
                    "type":      "fno_t1_alert",
                    "client_id": self._client_id,
                    "binding_id":self._binding_id,
                    "symbol":    pos.symbol,
                    "direction": pos.direction,
                    "strike":    pos.strike,
                    "expiry":    pos.expiry_str,
                    "spot":      spot,
                    "t1":        pos.day_t1,
                    "message":   (f"{pos.symbol} {pos.direction}: Day T1 {pos.day_t1:.1f} hit "
                                  f"at {spot:.1f} — add hedge "
                                  f"{pos.strike} {'PE' if pos.direction == 'CE' else 'CE'} {pos.expiry_str}"),
                })

        self._save_positions()

    async def _close_position(self, pos: FnOPosition, reason: str) -> None:
        order_id = await self._place_order("SELL", pos, pos.current_ltp)
        pos.exit_order_id = order_id or ""
        pos.status        = "CLOSED"
        pos.close_reason  = reason
        pos.close_time    = datetime.now(IST).isoformat()
        if pos.entry_ltp > 0 and pos.current_ltp > 0:
            pos.pnl = (pos.current_ltp - pos.entry_ltp) * pos.qty
        self._log.info("FnOBook[%s/%s]: EXIT %s reason=%s pnl=%.2f",
                       self._client_id, self._binding_id, pos.symbol, reason, pos.pnl)
        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "type":       "fno_exit",
            "client_id":  self._client_id,
            "binding_id": self._binding_id,
            "symbol":     pos.symbol,
            "direction":  pos.direction,
            "reason":     reason,
            "pnl":        pos.pnl,
        })

    async def _force_close_all(self) -> None:
        self._log.info("FnOBook[%s/%s]: EOD force-close", self._client_id, self._binding_id)
        for pos in list(self._open_positions):
            await self._close_position(pos, "eod")
        self._save_positions()

    async def _rescan_and_refill(self) -> None:
        await self._run_scan()
        await self._try_enter_triggered()

    # ── Order placement ───────────────────────────────────────────────────────

    async def _place_order(self, side: str, pos: FnOPosition, price_hint: float) -> Optional[str]:
        """Publish FnOOrderEvent; bridge handles broker call and replies via fill queue."""
        from execution_bridge.fno_bridge import FnOOrderEvent
        event_id = uuid.uuid4().hex
        ev = FnOOrderEvent(
            action=        "ENTRY" if side == "BUY" else "EXIT",
            symbol=        pos.symbol,
            direction=     pos.direction,
            strike=        pos.strike,
            expiry_str=    pos.expiry_str,
            broker_symbol= pos.broker_symbol,
            qty=           pos.qty,
            price_hint=    price_hint,
            client_id=     self._client_id,
            binding_id=    self._binding_id,
            event_id=      event_id,
            mode=          self._mode,
        )
        # Subscribe a per-event reply queue BEFORE publishing to avoid race
        fill_q = self._bus.subscribe(Topic.FNO_ORDER_FILL)
        await self._bus.publish(Topic.FNO_ORDER_REQUEST, ev)
        try:
            deadline = 15.0
            while deadline > 0:
                try:
                    fill = await asyncio.wait_for(fill_q.get(), timeout=2.0)
                    if getattr(fill, "event_id", None) == event_id:
                        if getattr(fill, "order_failed", False):
                            return None
                        return fill.order_id
                    # Not our fill — put it back (EventBus doesn't support unget; accept the drop)
                    deadline -= 2.0
                except asyncio.TimeoutError:
                    deadline -= 2.0
        except Exception as exc:
            self._log.error("FnOBook: order wait error: %s", exc)
        return None

    # ── Upstox REST LTP poll ──────────────────────────────────────────────────

    async def _fetch_ltp(self, instrument_key: str) -> float:
        if not instrument_key:
            return 0.0
        return await asyncio.to_thread(self._fetch_ltp_sync, instrument_key)

    def _fetch_ltp_sync(self, instrument_key: str) -> float:
        try:
            import requests
            r = requests.get(
                f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={instrument_key}",
                headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
                timeout=5,
            )
            if r.status_code == 200:
                quotes = r.json().get("data", {})
                for v in quotes.values():
                    return float(v.get("last_price", 0) or 0)
        except Exception as exc:
            self._log.debug("FnOBook: LTP poll error %s: %s", instrument_key, exc)
        return 0.0

    def _spot_key(self, symbol: str) -> str:
        from backtest.fno_scanner.backtest import TOP_30_STOCKS
        return TOP_30_STOCKS.get(symbol, "")

    # ── Expiry-week exit ──────────────────────────────────────────────────────

    async def _check_expiry_exit(self) -> None:
        """Close positions that are within EXPIRY_WEEK_DAYS of their expiry."""
        today = datetime.now(IST).date()
        closed_any = False
        for pos in list(self._open_positions):
            try:
                exp_date = datetime.strptime(pos.expiry_str, "%d %b %y").date()
                days_left = (exp_date - today).days
                if days_left <= EXPIRY_WEEK_DAYS:
                    self._log.info(
                        "FnOBook[%s/%s]: EXPIRY WEEK exit %s  days_left=%d  expiry=%s",
                        self._client_id, self._binding_id, pos.symbol, days_left, pos.expiry_str,
                    )
                    await self._close_position(pos, "expiry_week")
                    closed_any = True
            except Exception as exc:
                self._log.warning("FnOBook: expiry check error for %s: %s", pos.symbol, exc)
        if closed_any:
            self._save_positions()

    # ── Accessors ─────────────────────────────────────────────────────────────

    @property
    def _open_positions(self) -> List[FnOPosition]:
        return [p for p in self._positions if p.status in ("ENTRY_PLACED", "OPEN")]

    def get_state(self) -> dict:
        return {
            "client_id":    self._client_id,
            "binding_id":   self._binding_id,
            "mode":         self._mode,
            "max_slots":    self._max_slots,
            "open_count":   len(self._open_positions),
            "pending_count":len(self._pending),
            "positions":    [asdict(p) for p in self._positions],
        }
