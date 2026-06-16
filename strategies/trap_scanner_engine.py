"""
strategies/trap_scanner_engine.py — Trap Scanner Engine.

Plug-and-play adapter between our EventBus data feed and NiftyTrapScanner's
core detection logic (strategies/trap_scanner/scanner.py — unchanged).

Per-instance: one (client_id, binding_id, underlying).

Strike selection (auto, NOT admin-configurable):
  • No gap  → Pivot-based: CE at R1, PE at S1 (from prev-day spot OHLC)
  • Gap ≥ threshold → Fixed ITM offsets per index:
      Nifty/BankNifty:  200 pts near / 400 pts far
      Sensex/Finnifty:  300 pts near / 600 pts far
      CrudeOil:         200 pts near / 400 pts far
  • No HTF zone found → intraday fallback (today's bars only)

HTF scan source:
  • Nifty / Sensex / BankNifty / Finnifty / Midcpnifty → OPTION bars (CE+PE)
  • CrudeOil → FUTURES bars  (institutions trade via futures, not options)

LTF scan (5m) runs INSIDE open HTF zones only.

Position sizing:
  • lot_multiplier MUST be a multiple of 2 (50% exits at T1, 50% trails)
  • T1 = HTF Ref Bar HIGH (bears' SL = your profit target)
  • Trailing SL = after T1 hit, SL moves to entry (breakeven)

Broker: direct call to client's Angel One (or any configured) broker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, OptionTick, IndexTick
from strategies.trap_scanner import scanner

logger = logging.getLogger(__name__)

# ── Per-index defaults ────────────────────────────────────────────────────────
# gap_near / gap_far : ITM offset in points when gap fires
# htf_source: "option" → scan CE+PE option bars; "futures" → scan futures bars
_INDEX_CFG: Dict[str, dict] = {
    "NIFTY":      {"step": 50,  "lot": 75,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0,  "cutoff": "13:45", "sq_off": "14:00",
                   "window": None,  "exchange": "NFO",  "htf_source": "option"},
    "BANKNIFTY":  {"step": 100, "lot": 30,  "gap_near": 400, "gap_far": 800,
                   "sl_buf": 4.0,  "cutoff": "13:45", "sq_off": "14:00",
                   "window": None,  "exchange": "NFO",  "htf_source": "option"},
    "FINNIFTY":   {"step": 50,  "lot": 40,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0,  "cutoff": "13:45", "sq_off": "14:00",
                   "window": None,  "exchange": "NFO",  "htf_source": "option"},
    "SENSEX":     {"step": 100, "lot": 20,  "gap_near": 300, "gap_far": 600,
                   "sl_buf": 2.0,  "cutoff": "13:45", "sq_off": "14:00",
                   "window": None,  "exchange": "BFO",  "htf_source": "option"},
    "MIDCPNIFTY": {"step": 25,  "lot": 75,  "gap_near": 100, "gap_far": 200,
                   "sl_buf": 1.0,  "cutoff": "13:45", "sq_off": "14:00",
                   "window": None,  "exchange": "NFO",  "htf_source": "option"},
    "CRUDEOIL":   {"step": 100, "lot": 100, "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0,  "cutoff": "22:45", "sq_off": "23:00",
                   "window": [[18, 45], [19, 15]],  "exchange": "MCX",
                   "htf_source": "futures"},
}

# Upstox REST instrument keys for spot index data
_SPOT_KEYS: Dict[str, str] = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "CRUDEOIL":   "MCX_FO|170100100",   # CrudeOil near-month futures key (Upstox)
}


def _pivot_levels(H: float, L: float, C: float) -> Dict[str, float]:
    P = (H + L + C) / 3
    return {
        "pivot": P,
        "r1": 2 * P - L, "r2": P + (H - L),
        "s1": 2 * P - H, "s2": P - (H - L),
    }


def _round_strike(price: float, step: int) -> int:
    return int(round(price / step) * step)


def _bars_to_df(bars: List[dict]) -> pd.DataFrame:
    """Convert list of {datetime,open,high,low,close,volume} dicts to scanner-ready DataFrame."""
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _resample_to_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample 1m bars to HTF. Returns DataFrame with 'datetime' column."""
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf  # 'datetime' column preserved from reset_index


class TrapScannerEngine:
    """
    One independent trading book per (client_id, binding_id, underlying).
    Data source: our EventBus OPTION_TICK / INDEX_TICK (same feeder, zero extra cost).
    Core logic: strategies/trap_scanner/scanner.py (unchanged from NiftyTrapScanner).
    Broker: client's Angel One (or any configured broker) via direct call.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        underlying: str,
        lot_multiplier: int,        # MUST be multiple of 2
        client_id: str,
        binding_id: str,
        ts_admin_cfg: dict,         # from system_settings["trap_scanner"] JSON
        client_db,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._und = underlying.upper()
        self._lot_mul = lot_multiplier
        self._cid = client_id
        self._bid = binding_id
        self._ts_cfg = ts_admin_cfg
        self._db = client_db

        # Per-index config: admin overrides merged on top of _INDEX_CFG defaults
        _def = _INDEX_CFG.get(self._und, _INDEX_CFG["NIFTY"])
        _adm = ts_admin_cfg.get("per_index", {}).get(self._und, {})
        self._step        = int(_def["step"])
        self._lot_size    = int(_adm.get("lot_size",    _def["lot"]))
        self._sl_buf      = float(_adm.get("sl_buffer", _def["sl_buf"]))
        self._gap_near    = int(_adm.get("gap_itm_near", _def["gap_near"]))
        self._gap_far     = int(_adm.get("gap_itm_far",  _def["gap_far"]))
        self._cutoff_str  = _adm.get("entry_cutoff",  _def["cutoff"])
        self._sq_off_str  = _adm.get("sq_off_time",   _def["sq_off"])
        self._entry_win   = _adm.get("entry_window",  _def["window"])   # [[h,m],[h,m]] or None
        self._exchange    = _def["exchange"]
        self._htf_source  = _def["htf_source"]   # "option" or "futures"
        self._gap_thresh  = float(ts_admin_cfg.get("gap_threshold_pct", 1.0))
        self._htf_min     = int(ts_admin_cfg.get("htf_minutes", 75))
        self._ltf_min     = int(ts_admin_cfg.get("ltf_minutes", 5))

        # Runtime state
        self._running    = False
        self._tasks: List[asyncio.Task] = []
        self._loop_queues: Dict[str, asyncio.Queue] = {}

        # Strike / instrument state (set at morning init)
        self._ce_strike:   Optional[int]  = None
        self._pe_strike:   Optional[int]  = None
        self._ce_key:      Optional[str]  = None   # Upstox instrument key for CE option
        self._pe_key:      Optional[str]  = None
        self._fut_key:     Optional[str]  = None   # CrudeOil futures key
        self._expiry_str:  Optional[str]  = None
        self._gap_fired:   bool = False
        self._spot_open:   float = 0.0
        self._spot_cache:  float = 0.0

        # 1m candle bars (dicts with 'datetime','open','high','low','close','volume')
        self._bars_ce:  List[dict] = []   # CE option 1m bars
        self._bars_pe:  List[dict] = []   # PE option 1m bars
        self._bars_fut: List[dict] = []   # CrudeOil futures 1m bars
        self._bars_spot: List[dict] = []  # Spot 1m bars (Nifty/Sensex spot scan)
        self._buckets:  Dict[str, dict] = {}   # symbol → open bucket

        # HTF scan results (list of entry dicts from scanner.scan_htf)
        self._htf_ce:  List[dict] = []
        self._htf_pe:  List[dict] = []
        self._htf_fut: List[dict] = []   # CrudeOil only

        # Position
        self._position: Optional[Dict] = None
        # {side, strike, opt_type, entry_price, sl_price, t1_price,
        #  total_qty, t1_qty, remaining_qty, t1_hit, entry_ts,
        #  order_id_entry, order_id_t1}

        # Broker (lazy-authenticated on first entry)
        self._broker: Optional[Any] = None

        # Flags
        self._initialized   = False
        self._intraday_mode = False     # fallback: no HTF zone → use intraday bars
        self._day_init_done = False     # morning init ran for today

        # Logger
        self._log = self._make_logger()

    def _make_logger(self) -> logging.Logger:
        name = f"client.ts.{self._und}.{self._cid}.{self._bid}"
        lg = logging.getLogger(name)
        if lg.handlers:
            return lg
        lg.setLevel(logging.INFO)
        log_dir = os.path.join("logs", "clients")
        os.makedirs(log_dir, exist_ok=True)
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(
            os.path.join(log_dir,
                f"ts_{self._und}_{self._cid}_{self._bid}_{datetime.now(IST).strftime('%Y%m%d')}.log"),
            encoding="utf-8", maxBytes=10 * 1024 * 1024, backupCount=3,
        )
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
        lg.addHandler(fh)
        lg.propagate = False
        return lg

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        self._running = True
        t1 = loop.create_task(self._lifecycle_loop(),  name=f"ts_life_{self._und}_{self._cid}")
        t2 = loop.create_task(self._opt_tick_loop(),   name=f"ts_opt_{self._und}_{self._cid}")
        t3 = loop.create_task(self._idx_tick_loop(),   name=f"ts_idx_{self._und}_{self._cid}")
        self._tasks = [t1, t2, t3]
        logger.info("TrapScannerEngine[%s/%s/%s]: started.", self._cid, self._bid, self._und)

    async def stop_async(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for topic, q in self._loop_queues.items():
            try:
                self._bus.unsubscribe(topic, q)
            except Exception:
                pass
        if self._broker:
            try:
                await self._broker.logout()
            except Exception:
                pass

    # ── Lifecycle loop — morning init + EOD reset ─────────────────────────────

    async def _lifecycle_loop(self) -> None:
        while self._running:
            try:
                now = datetime.now(IST)
                sq_h, sq_m = map(int, self._sq_off_str.split(":"))
                sq_time = time(sq_h, sq_m)
                is_mcx = self._und in ("CRUDEOIL", "CRUDEOILM")
                market_open = time(9, 0) if is_mcx else time(9, 15)

                if now.time() >= sq_time and self._initialized:
                    await self._eod_square_off()
                    self._reset_day_state()

                elif not self._initialized and now.time() >= market_open:
                    if not self._day_init_done:
                        ok = await self._morning_init()
                        self._day_init_done = True
                        if ok:
                            self._initialized = True
                            self._log.info("Morning init OK — CE=%s PE=%s gap=%s",
                                           self._ce_strike, self._pe_strike, self._gap_fired)
                        else:
                            self._log.warning("Morning init failed; retrying in 120s")
                            await asyncio.sleep(120)
                            self._day_init_done = False  # allow retry
                            continue

                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("TrapScannerEngine[%s/%s] lifecycle error: %s",
                                 self._cid, self._und, exc)
                await asyncio.sleep(60)

    # ── Morning init ──────────────────────────────────────────────────────────

    async def _morning_init(self) -> bool:
        """
        1. Fetch prev-day spot OHLC from Upstox REST
        2. Compute pivot levels
        3. Detect gap vs threshold
        4. Select CE/PE strikes (pivot or fixed ITM offsets)
        5. Resolve instrument keys + fetch 1m historical bars
        6. Run initial HTF scan (seed zones)
        7. If no zones found → switch to intraday mode
        8. Authenticate broker
        """
        try:
            # 1. Prev-day OHLC
            prev = await self._fetch_prev_day_ohlc()
            if not prev:
                self._log.warning("No prev-day OHLC — skipping init")
                return False
            H, L, C = prev["high"], prev["low"], prev["close"]
            pivots = _pivot_levels(H, L, C)
            self._log.info("Prev-day H=%.0f L=%.0f C=%.0f | Pivot=%.0f R1=%.0f S1=%.0f",
                           H, L, C, pivots["pivot"], pivots["r1"], pivots["s1"])

            # 2. Gap detection (use live spot if available, else today's open from REST)
            today_open = self._spot_cache if self._spot_cache > 0 else await self._fetch_today_open()
            if today_open <= 0:
                today_open = C
            self._spot_open = today_open
            gap_pct = abs(today_open - C) / C * 100 if C > 0 else 0.0
            self._gap_fired = gap_pct >= self._gap_thresh

            # 3. Strike selection
            atm = _round_strike(today_open, self._step)
            if self._gap_fired:
                direction = "UP" if today_open > C else "DOWN"
                if direction == "UP":
                    self._ce_strike = atm - self._gap_near
                    self._pe_strike = atm + self._gap_near
                else:
                    self._ce_strike = atm + self._gap_near
                    self._pe_strike = atm - self._gap_near
                self._log.info("GAP %.1f%% (%s) → CE=%d PE=%d (ITM±%d)",
                               gap_pct, direction, self._ce_strike, self._pe_strike, self._gap_near)
            else:
                # Normal day: CE at R1, PE at S1 (pivot)
                self._ce_strike = _round_strike(pivots["r1"], self._step)
                self._pe_strike = _round_strike(pivots["s1"], self._step)
                self._log.info("No gap (%.1f%%) → Pivot CE=%d (R1=%.0f) PE=%d (S1=%.0f)",
                               gap_pct, self._ce_strike, pivots["r1"],
                               self._pe_strike, pivots["s1"])

            # 4. Get expiry + instrument keys
            self._expiry_str = await self._get_expiry()
            if not self._expiry_str:
                self._log.warning("No expiry found for %s", self._und)
                return False

            if self._htf_source == "futures":
                self._fut_key = _SPOT_KEYS.get(self._und, "")
            else:
                self._ce_key = self._build_upstox_key(self._ce_strike, "CE")
                self._pe_key = self._build_upstox_key(self._pe_strike, "PE")

            # 5. Fetch historical 1m bars
            if self._htf_source == "futures":
                self._bars_fut = await self._fetch_1m_history(self._fut_key or "")
            else:
                self._bars_ce = await self._fetch_1m_history(self._ce_key or "")
                self._bars_pe = await self._fetch_1m_history(self._pe_key or "")
                # Spot bars for Nifty/Sensex (secondary HTF scan per NiftyTrapScanner)
                spot_key = _SPOT_KEYS.get(self._und, "")
                if spot_key:
                    self._bars_spot = await self._fetch_1m_history(spot_key)

            # 6. HTF scan
            self._run_htf_scan()

            open_zones = self._open_zone_count()
            if open_zones == 0:
                self._log.info("No HTF zones found — switching to intraday fallback")
                self._intraday_mode = True
                await self._init_intraday_fallback()
            else:
                self._intraday_mode = False
                self._log.info("HTF scan complete: %d open zones", open_zones)

            # 7. Subscribe to live option/futures ticks (pin so rebalancer doesn't drop)
            await self._subscribe_instruments()

            # 8. Authenticate broker
            await self._ensure_broker()

            return True
        except Exception as exc:
            self._log.exception("morning_init error: %s", exc)
            return False

    async def _init_intraday_fallback(self) -> None:
        """No HTF zones from historical → use today's 1m intraday bars for HTF scan."""
        spot_key = _SPOT_KEYS.get(self._und, "")
        if not spot_key:
            return
        intraday_bars = await self._fetch_intraday_1m(spot_key)
        if not intraday_bars:
            return
        if self._htf_source == "futures":
            self._bars_fut = intraday_bars
        else:
            self._bars_ce  = intraday_bars
            self._bars_pe  = intraday_bars
            self._bars_spot = intraday_bars
        self._run_htf_scan()
        self._log.info("Intraday fallback HTF scan: %d open zones", self._open_zone_count())

    # ── HTF scan ──────────────────────────────────────────────────────────────

    def _run_htf_scan(self) -> None:
        """Resample 1m bars to HTF_MINUTES and run scan_htf on each leg."""
        if self._htf_source == "futures":
            df = _bars_to_df(self._bars_fut)
            if not df.empty:
                htf = _resample_to_htf(df, self._htf_min)
                if len(htf) >= 2:
                    _, self._htf_fut = scanner.scan_htf(htf)
                    self._log.info("HTF futures scan: %d entries (%d open)",
                                   len(self._htf_fut),
                                   sum(1 for e in self._htf_fut if e["status"] in ("ACTIVE","TRAPPED")))
        else:
            for attr, bars, label in [
                ("_htf_ce", self._bars_ce, "CE"),
                ("_htf_pe", self._bars_pe, "PE"),
            ]:
                df = _bars_to_df(bars)
                if df.empty:
                    continue
                htf = _resample_to_htf(df, self._htf_min)
                if len(htf) < 2:
                    continue
                _, entries = scanner.scan_htf(htf)
                setattr(self, attr, entries)
                self._log.info("HTF %s scan: %d entries (%d open)", label, len(entries),
                               sum(1 for e in entries if e["status"] in ("ACTIVE","TRAPPED")))

    def _open_zone_count(self) -> int:
        if self._htf_source == "futures":
            return sum(1 for e in self._htf_fut if e["status"] == "TRAPPED")
        return (sum(1 for e in self._htf_ce if e["status"] == "TRAPPED") +
                sum(1 for e in self._htf_pe if e["status"] == "TRAPPED"))

    # ── Option / futures tick processing ─────────────────────────────────────

    async def _opt_tick_loop(self) -> None:
        """Subscribe to OPTION_TICK and aggregate into 1m OHLCV."""
        q = self._bus.subscribe(Topic.OPTION_TICK)
        self._loop_queues[Topic.OPTION_TICK] = q
        try:
            while self._running:
                try:
                    tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if not self._initialized:
                    continue
                sym = tick.symbol
                # Determine which bar list this tick belongs to
                is_ce  = (sym == self._ce_key or (tick.option_type == "CE"
                          and str(int(tick.strike)) == str(self._ce_strike)
                          and str(tick.underlying).upper() == self._und))
                is_pe  = (sym == self._pe_key or (tick.option_type == "PE"
                          and str(int(tick.strike)) == str(self._pe_strike)
                          and str(tick.underlying).upper() == self._und))
                is_fut = (self._htf_source == "futures"
                          and str(tick.underlying).upper() == self._und)

                if is_ce:
                    closed = self._update_bucket("CE", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_ce.append(closed)
                        if len(self._bars_ce) > 2000:
                            del self._bars_ce[:-2000]
                        self._on_candle_close("CE", tick.timestamp)
                elif is_pe:
                    closed = self._update_bucket("PE", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_pe.append(closed)
                        if len(self._bars_pe) > 2000:
                            del self._bars_pe[:-2000]
                        self._on_candle_close("PE", tick.timestamp)
                elif is_fut:
                    closed = self._update_bucket("FUT", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_fut.append(closed)
                        if len(self._bars_fut) > 2000:
                            del self._bars_fut[:-2000]
                        self._on_candle_close("FUT", tick.timestamp)

                # Tick-level exit check for open position
                if self._position:
                    pos_side = self._position.get("side", "")
                    if (is_ce and pos_side == "CE") or (is_pe and pos_side == "PE") or \
                       (is_fut and pos_side == "FUT"):
                        await self._check_tick_exit(float(tick.ltp), tick.timestamp)
        except asyncio.CancelledError:
            pass
        finally:
            self._bus.unsubscribe(Topic.OPTION_TICK, q)

    async def _idx_tick_loop(self) -> None:
        """Subscribe to INDEX_TICK to keep spot price for gap detection."""
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._loop_queues[Topic.INDEX_TICK] = q
        try:
            while self._running:
                try:
                    tick: IndexTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if str(tick.symbol).upper() == self._und:
                    self._spot_cache = float(tick.ltp)
                    # Build spot 1m candles for Nifty/Sensex secondary HTF scan
                    if self._initialized and self._htf_source == "option":
                        closed = self._update_bucket("SPOT", tick.ltp, tick.timestamp)
                        if closed:
                            self._bars_spot.append(closed)
                            if len(self._bars_spot) > 2000:
                                del self._bars_spot[:-2000]
        except asyncio.CancelledError:
            pass
        finally:
            self._bus.unsubscribe(Topic.INDEX_TICK, q)

    def _update_bucket(self, bkey: str, ltp: float, ts: datetime) -> Optional[dict]:
        """Update open 1m candle bucket. Returns completed candle dict on close, else None."""
        bucket_ts = ts.replace(second=0, microsecond=0)
        ltp = float(ltp)
        if bkey not in self._buckets:
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp, "volume": 0}
            return None
        b = self._buckets[bkey]
        if b["ts"] != bucket_ts:
            completed = {"datetime": b["ts"].isoformat(),
                         "open": b["open"], "high": b["high"],
                         "low": b["low"],   "close": b["close"], "volume": b["volume"]}
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp, "volume": 0}
            return completed
        b["high"]   = max(b["high"], ltp)
        b["low"]    = min(b["low"],  ltp)
        b["close"]  = ltp
        b["volume"] += 1
        return None

    # ── Candle close → run LTF / refresh HTF ─────────────────────────────────

    def _on_candle_close(self, leg: str, ts: datetime) -> None:
        """Called each time a 1m candle closes. Drives LTF and HTF scans."""
        # Refresh HTF zones on every HTF boundary
        if ts.minute % self._htf_min == 0:
            self._run_htf_scan()

        # Run LTF scan on every LTF boundary
        if ts.minute % self._ltf_min != 0:
            return

        if self._position:
            return  # already in a trade — no new entries

        if self._htf_source == "futures":
            zones = [e for e in self._htf_fut if e["status"] == "TRAPPED"]
            self._run_ltf(leg="FUT", bars=self._bars_fut, zones=zones)
        else:
            if leg == "CE":
                zones = [e for e in self._htf_ce if e["status"] == "TRAPPED"]
                self._run_ltf(leg="CE", bars=self._bars_ce, zones=zones)
            elif leg == "PE":
                zones = [e for e in self._htf_pe if e["status"] == "TRAPPED"]
                self._run_ltf(leg="PE", bars=self._bars_pe, zones=zones)

    def _run_ltf(self, leg: str, bars: List[dict], zones: List[dict]) -> None:
        if not zones or len(bars) < 3:
            return
        df = _bars_to_df(bars[-200:])   # last 200 1m bars
        for zone in zones:
            _, ltf_entries = scanner.scan_ltf(
                df,
                htf_zone_high=zone["zone_high"],
                htf_zone_low=zone["zone_low"],
                htf_ref_bar=str(zone.get("ref_ts", "")),
                htf_trap_bar=str(zone.get("trapped_on", "")),
                htf_target=zone.get("sl", 0.0),
            )
            best = scanner.select_best_ltf_entry(ltf_entries)
            if best:
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg, best, zone)
                )
                return  # one entry per LTF scan

    # ── Entry signal ──────────────────────────────────────────────────────────

    async def _on_entry_signal(self, leg: str, entry: dict, htf_zone: dict) -> None:
        """Gate-check then place order."""
        if self._position:
            return
        now = datetime.now(IST)

        # Entry cutoff gate
        ch, cm = map(int, self._cutoff_str.split(":"))
        if now.time() >= time(ch, cm):
            self._log.info("SKIP entry — past cutoff %s", self._cutoff_str)
            return

        # Entry window gate (CrudeOil W2: 18:45–19:15)
        if self._entry_win:
            wh, wm = self._entry_win[0]; eh, em = self._entry_win[1]
            if not (time(wh, wm) <= now.time() <= time(eh, em)):
                self._log.info("SKIP entry — outside entry window %02d:%02d–%02d:%02d",
                               wh, wm, eh, em)
                return

        # Determine option type from leg
        if leg == "FUT":
            opt_type = "CE"    # futures trap → buy CE (bearish trap = bullish move)
        else:
            opt_type = leg     # "CE" or "PE"

        strike   = (self._ce_strike if opt_type == "CE" else self._pe_strike) or 0
        ep       = round(entry["zone_trigger"], 2)
        t1_price = round(htf_zone["sl"], 2)         # bears' SL = your profit target
        sl_price = round(entry["zone_low"] - self._sl_buf, 2)
        total_qty = self._lot_size * self._lot_mul
        t1_qty    = total_qty // 2                  # 50% at T1

        self._log.info(
            "ENTRY SIGNAL %s %d%s entry=%.2f sl=%.2f t1=%.2f qty=%d lots",
            self._und, strike, opt_type, ep, sl_price, t1_price, self._lot_mul
        )

        broker = await self._ensure_broker()
        if not broker:
            self._log.error("No broker available — entry aborted")
            return

        broker_sym = self._build_broker_symbol(strike, opt_type)
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=OrderSide.BUY,
            qty=total_qty,
            order_type=OrderType.MARKET,
            price=ep,
            tag=f"TRAP_{self._und}_{opt_type}",
            client_id=self._cid,
        )
        try:
            order_id = await broker.place_order(req)
            fill = await broker.get_order_status(order_id)
            avg  = fill.avg_price if fill.avg_price > 0 else ep
        except Exception as exc:
            self._log.error("Entry order failed: %s", exc)
            return

        self._position = {
            "leg": leg, "side": opt_type, "strike": strike,
            "entry_price": round(avg, 2),
            "sl_price":    sl_price,        # initial SL (zone_low - buffer)
            "trail_sl":    sl_price,        # trailing SL (starts same, trails 5m lows upward after T1)
            "last_5m_ts":  now.replace(second=0, microsecond=0),  # last 5m bucket processed for trail
            "t1_price":    t1_price, "total_qty": total_qty,
            "t1_qty":      t1_qty, "remaining_qty": total_qty,
            "t1_hit":      False, "entry_ts": now.isoformat(),
            "order_id_entry": order_id, "order_id_t1": None,
        }
        self._log.info("ENTRY PLACED fill=%.2f order_id=%s", avg, order_id)

    # ── Tick-level exit checks ─────────────────────────────────────────────────

    async def _check_tick_exit(self, ltp: float, ts: Optional[datetime] = None) -> None:
        pos = self._position
        if not pos:
            return

        # T1: 50% exit at HTF target (bears' SL)
        if not pos["t1_hit"] and ltp >= pos["t1_price"]:
            pos["t1_hit"] = True
            pos["remaining_qty"] -= pos["t1_qty"]
            self._log.info("T1 HIT ltp=%.2f t1=%.2f selling %d units",
                           ltp, pos["t1_price"], pos["t1_qty"])
            oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
            pos["order_id_t1"] = oid
            # After T1: trailing SL kicks in; trail_sl remains at zone_low-buffer
            # (it will trail UP on each 5m close in _update_trail_sl)

        # After T1: advance 5m trailing SL (on every new 5m bucket)
        if pos["t1_hit"] and ts is not None:
            self._update_trail_sl(pos, ts, ltp)

        # Exit: SL check uses trail_sl when T1 hit, else original sl_price
        active_sl = pos["trail_sl"] if pos["t1_hit"] else pos["sl_price"]
        if ltp <= active_sl:
            remaining = pos["remaining_qty"]
            reason = "TRAIL_SL" if pos["t1_hit"] else "SL"
            self._log.info("%s HIT ltp=%.2f sl=%.2f selling %d units",
                           reason, ltp, active_sl, remaining)
            await self._place_exit(remaining, active_sl, reason)
            self._position = None

    def _update_trail_sl(self, pos: dict, ts: datetime, ltp: float) -> None:
        """
        After T1: trail the remaining 50% SL using 5m bar lows.
        On every new 5m bucket boundary: candidate_sl = prev_5m_bar_low - sl_buffer.
        trail_sl only moves UP (ratchet). This mirrors live_tracker._run_cascade_simulation.
        """
        if not pos["t1_hit"]:
            return
        bar_5m_ts = ts.replace(second=0, microsecond=0)
        bar_5m_ts = bar_5m_ts.replace(minute=(bar_5m_ts.minute // 5) * 5)
        last = pos.get("last_5m_ts")
        if last is not None and bar_5m_ts <= last:
            return   # same 5m bucket — nothing to do
        pos["last_5m_ts"] = bar_5m_ts
        # Get the 5m low from the leg's recent 1m bars
        bars = self._bars_ce if pos["side"] == "CE" else (
               self._bars_pe if pos["side"] == "PE" else self._bars_fut)
        if not bars:
            return
        # Find all 1m bars belonging to the PREVIOUS 5m bucket
        prev_5m_start = bar_5m_ts - timedelta(minutes=5)
        bucket_bars = [
            b for b in bars[-10:]
            if prev_5m_start <= datetime.fromisoformat(b["datetime"]) < bar_5m_ts
        ]
        if not bucket_bars:
            return
        prev_5m_low = min(b["low"] for b in bucket_bars)
        candidate_sl = round(prev_5m_low - self._sl_buf, 2)
        if candidate_sl > pos["trail_sl"]:
            old = pos["trail_sl"]
            pos["trail_sl"] = candidate_sl
            self._log.info("TRAIL_SL raised %.2f → %.2f (5m_low=%.2f buf=%.2f)",
                           old, candidate_sl, prev_5m_low, self._sl_buf)

    async def _place_exit(self, qty: int, price: float, reason: str) -> Optional[str]:
        if qty <= 0 or not self._position:
            return None
        broker = await self._ensure_broker()
        if not broker:
            return None
        pos = self._position
        broker_sym = self._build_broker_symbol(pos["strike"], pos["side"])
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=OrderSide.SELL,
            qty=qty,
            order_type=OrderType.MARKET,
            price=price,
            tag=f"TRAP_EXIT_{reason}",
            client_id=self._cid,
        )
        try:
            order_id = await broker.place_order(req)
            self._log.info("EXIT %s placed qty=%d order_id=%s", reason, qty, order_id)
            return order_id
        except Exception as exc:
            self._log.error("Exit order failed (%s): %s", reason, exc)
            return None

    # ── EOD ──────────────────────────────────────────────────────────────────

    async def _eod_square_off(self) -> None:
        pos = self._position
        if pos and pos["remaining_qty"] > 0:
            self._log.info("EOD square-off: %d units", pos["remaining_qty"])
            await self._place_exit(pos["remaining_qty"], 0.0, "EOD")
        self._position = None

    def _reset_day_state(self) -> None:
        self._initialized   = False
        self._intraday_mode = False
        self._day_init_done = False
        self._bars_ce  = []; self._bars_pe  = []
        self._bars_fut = []; self._bars_spot = []
        self._htf_ce   = []; self._htf_pe   = []; self._htf_fut = []
        self._buckets  = {}
        self._ce_strike = None; self._pe_strike = None
        self._ce_key = None; self._pe_key = None
        self._expiry_str = None

    # ── Broker ────────────────────────────────────────────────────────────────

    async def _ensure_broker(self):
        if self._broker and self._broker.is_authenticated:
            return self._broker
        try:
            bindings = self._db.get_bindings_sync(self._cid)
            binding_row = next((b for b in bindings if b.get("binding_id") == self._bid), None)
            if not binding_row:
                self._log.error("Binding %s not found for client %s", self._bid, self._cid)
                return None
            from config.client_profiles import BrokerBinding
            b = BrokerBinding(**{k: v for k, v in binding_row.items()
                                 if k in BrokerBinding.__dataclass_fields__})
            from execution_bridge.base_broker import create_broker
            broker = create_broker(b, self._cid)
            ok = await broker.authenticate()
            if not ok:
                self._log.error("Broker auth failed for %s/%s", self._cid, self._bid)
                return None
            self._broker = broker
            return broker
        except Exception as exc:
            self._log.error("_ensure_broker error: %s", exc)
            return None

    # ── Data fetching ─────────────────────────────────────────────────────────

    async def _fetch_prev_day_ohlc(self) -> Optional[Dict]:
        """Fetch previous trading day OHLC from Upstox daily candles REST API."""
        try:
            token = self._get_upstox_token()
            if not token:
                return None
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key:
                return None
            import aiohttp
            today  = date.today()
            fr_date = today - timedelta(days=10)
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{spot_key}/1day/{today}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        self._log.warning("Upstox daily OHLC status=%d", r.status)
                        return None
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            # candles[0] = today (may be partial), candles[1] = yesterday (complete)
            if len(candles) < 2:
                return None
            prev = candles[1]  # [ts, open, high, low, close, volume, oi]
            return {"open": float(prev[1]), "high": float(prev[2]),
                    "low":  float(prev[3]), "close": float(prev[4])}
        except Exception as exc:
            self._log.warning("_fetch_prev_day_ohlc: %s", exc)
            return None

    async def _fetch_today_open(self) -> float:
        """Fetch today's opening price from Upstox intraday candles."""
        try:
            token = self._get_upstox_token()
            if not token:
                return 0.0
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key:
                return 0.0
            import aiohttp
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{spot_key}/1minute"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return 0.0
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            if not candles:
                return 0.0
            # Candles newest-first; last entry = oldest = today's open
            first_bar = candles[-1]
            return float(first_bar[1])   # open of first 1m bar
        except Exception as exc:
            self._log.warning("_fetch_today_open: %s", exc)
            return 0.0

    async def _fetch_1m_history(self, instrument_key: str) -> List[dict]:
        """Fetch 1m historical bars (up to 7 days) for an instrument from Upstox REST."""
        if not instrument_key:
            return []
        try:
            token = self._get_upstox_token()
            if not token:
                return []
            import aiohttp
            today  = date.today()
            fr_date = today - timedelta(days=7)
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{instrument_key}/1minute/{today}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        self._log.warning("_fetch_1m_history(%s) status=%d", instrument_key, r.status)
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            # Upstox returns newest-first — reverse to oldest-first for scanner
            bars = []
            for c in reversed(candles):
                bars.append({"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])})
            return bars
        except Exception as exc:
            self._log.warning("_fetch_1m_history(%s): %s", instrument_key, exc)
            return []

    async def _fetch_intraday_1m(self, instrument_key: str) -> List[dict]:
        """Fetch today's intraday 1m bars from Upstox (for fallback mode)."""
        if not instrument_key:
            return []
        try:
            token = self._get_upstox_token()
            if not token:
                return []
            import aiohttp
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            bars = []
            for c in reversed(candles):  # oldest first
                bars.append({"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                             "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])})
            return bars
        except Exception as exc:
            self._log.warning("_fetch_intraday_1m(%s): %s", instrument_key, exc)
            return []

    def _get_upstox_token(self) -> Optional[str]:
        creds = self._db.get_feeder_creds_sync("upstox")
        return (creds or {}).get("access_token") or ""

    async def _get_expiry(self) -> Optional[str]:
        """Return nearest weekly/monthly expiry for the underlying (DDMONYY format)."""
        from datetime import date, timedelta
        _EXPIRY_DOW = {
            "NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
            "SENSEX": 4, "MIDCPNIFTY": 1,
        }
        if self._und == "CRUDEOIL":
            # CrudeOil uses monthly futures; Upstox key is already set
            return date.today().strftime("%b%y").upper()
        weekday = _EXPIRY_DOW.get(self._und, 3)
        d = date.today()
        for _ in range(7):
            if d.weekday() == weekday:
                return d.strftime("%d%b%y").upper()
            d += timedelta(days=1)
        return None

    async def _subscribe_instruments(self) -> None:
        """Ask the feeder to subscribe + pin the trap scanner's instruments."""
        keys_to_pin = [k for k in [self._ce_key, self._pe_key, self._fut_key] if k]
        if not keys_to_pin:
            return
        try:
            from data_layer.strike_rebalancer import PinRequest
            for key in keys_to_pin:
                pr = PinRequest(instrument_key=key, owner=f"trap_{self._cid}_{self._bid}")
                await self._bus.publish(Topic.PIN_REQUEST, pr)
        except Exception as exc:
            self._log.warning("_subscribe_instruments: %s", exc)

    def _build_upstox_key(self, strike: int, opt_type: str) -> str:
        """Build Upstox instrument key for an NSE/BSE option."""
        _PFX = {
            "NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "FINNIFTY": "NSE_FO|", "SENSEX": "BSE_FO|", "MIDCPNIFTY": "NSE_FO|",
        }
        pfx = _PFX.get(self._und, "NSE_FO|")
        exp = self._expiry_str or ""
        return f"{pfx}{self._und}{exp}{strike}{opt_type}"

    def _build_broker_symbol(self, strike: int, opt_type: str) -> str:
        """Build Angel One broker symbol for the option contract."""
        exp = self._expiry_str or ""
        # Angel One format: SENSEX2661876800CE (SENSEX + YY + M_no_zero + DD + strike + CE/PE)
        # Simple fallback: UNDERLYING + EXPIRY + STRIKE + OPTTYPE
        return f"{self._und}{exp}{strike}{opt_type}"

    # ── Dashboard telemetry ───────────────────────────────────────────────────

    def telemetry_snapshot(self) -> dict:
        pos = self._position
        return {
            "underlying":    self._und,
            "client_id":     self._cid,
            "binding_id":    self._bid,
            "initialized":   self._initialized,
            "intraday_mode": self._intraday_mode,
            "gap_fired":     self._gap_fired,
            "ce_strike":     self._ce_strike,
            "pe_strike":     self._pe_strike,
            "expiry":        self._expiry_str,
            "open_zones_ce": sum(1 for e in self._htf_ce if e["status"] == "TRAPPED"),
            "open_zones_pe": sum(1 for e in self._htf_pe if e["status"] == "TRAPPED"),
            "open_zones_fut":sum(1 for e in self._htf_fut if e["status"] == "TRAPPED"),
            "bars_1m_ce":    len(self._bars_ce),
            "bars_1m_pe":    len(self._bars_pe),
            "position":      {
                "side":          pos["side"],
                "strike":        pos["strike"],
                "entry_price":   pos["entry_price"],
                "sl_price":      pos["sl_price"],
                "trail_sl":      pos["trail_sl"],
                "t1_price":      pos["t1_price"],
                "total_qty":     pos["total_qty"],
                "remaining_qty": pos["remaining_qty"],
                "t1_hit":        pos["t1_hit"],
                "entry_ts":      pos["entry_ts"],
            } if pos else None,
        }
