"""
strategies/trap_scanner_engine.py — Trap Scanner Engine (v2).

Plug-and-play adapter between our EventBus data feed and NiftyTrapScanner's
core detection logic (strategies/trap_scanner/scanner.py — unchanged).

Per-instance: one (client_id, binding_id, underlying).

Strike selection (auto, NOT admin-configurable):
  No gap  → Pivot-based:
      CE1 = S1,  CE2 = S2   (support levels, where bears short → CE trapped)
      PE1 = R1,  PE2 = R2   (resistance levels, where bulls buy → PE trapped)
  Gap >= threshold → Fixed ITM offsets per index:
      UP  gap: CE = ATM − offset, PE = ATM + offset
      DOWN gap: CE = ATM + offset, PE = ATM − offset
  No HTF zone found → intraday cascade (15-min → 5-min)

HTF scan source (SPOT index bars):
  NSE / BSE indices → scan_htf_spot() on 1m SPOT bars (catches BEAR + BULL traps)
  CrudeOil         → scan_htf()      on 1m FUTURES bars (bearish traps only)

Trade direction:
  BEAR trap on spot → buy CE (spot going UP → CE gains)
  BULL trap on spot → buy PE (spot going DOWN → PE gains)

LTF scan (5-min) runs on OPTION PREMIUM bars inside open HTF spot zones.

Two-tier exit:
  T1  = 50% at HTF zone target (bears'/bulls' SL = your profit)
  Rest = 5-min ratchet trail on OPTION bars until exit or EOD

Intraday cascade (no HTF zone TRAPPED):
  1. Resample today's bars to 15-min → scan_htf_spot / scan_htf
  2. If 15-min zone TRAPPED → scan_ltf on 5-min option bars inside it
  3. Entry fires on 5-min TRAPPED (cascade: no CLOSED step required)

Dedup: notified_uids set — same zone uid never fires twice per day.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, date, time, timedelta
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import EventBus, OptionTick, IndexTick
from strategies.trap_scanner import scanner

logger = logging.getLogger(__name__)

# ── Per-index config ──────────────────────────────────────────────────────────
_INDEX_CFG: Dict[str, dict] = {
    # htf_source="option": HTF and LTF both scan OPTION premium bars (same units → scan_ltf works)
    # Reference: NiftyTrapScanner phase2/ltf-entry-engine CLAUDE.md Section 2
    "NIFTY":      {"step": 100, "lot": 75,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "BANKNIFTY":  {"step": 100, "lot": 30,  "gap_near": 400, "gap_far": 800,
                   "sl_buf": 4.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "FINNIFTY":   {"step": 50,  "lot": 40,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "SENSEX":     {"step": 100, "lot": 20,  "gap_near": 300, "gap_far": 600,
                   "sl_buf": 2.0, "cutoff": "15:20", "sq_off": "15:25",
                   "window": None, "exchange": "BFO", "htf_source": "option"},
    "MIDCPNIFTY": {"step": 25,  "lot": 75,  "gap_near": 100, "gap_far": 200,
                   "sl_buf": 1.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "CRUDEOIL":   {"step": 100, "lot": 100, "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0, "cutoff": "22:45", "sq_off": "23:00",
                   "window": [[18, 45], [19, 15]], "exchange": "MCX",
                   "htf_source": "futures", "htf_min_override": 30},
    "BTC":        {"step": 1000, "lot": 1,  "gap_near": 2000, "gap_far": 4000,
                   "sl_buf": 50.0, "cutoff": "23:00", "sq_off": "23:15",
                   "window": None, "exchange": "DELTA", "htf_source": "futures"},
    "ETH":        {"step": 100, "lot": 1,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 5.0, "cutoff": "23:00", "sq_off": "23:15",
                   "window": None, "exchange": "DELTA", "htf_source": "futures"},
}

# Upstox REST instrument keys for spot / futures data
_SPOT_KEYS: Dict[str, str] = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "CRUDEOIL":   "MCX_FO|499095",   # CRUDEOIL near-month futures (dynamic in production)
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
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _resample_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def _zone_uid(e: dict) -> str:
    """Stable unique ID for a zone so notified_uids deduplicate across candle refreshes."""
    return f"{e.get('ref_ts','')}_{e.get('zone_high',0):.2f}_{e.get('kind','BEAR')}"


class TrapScannerEngine:
    """
    One independent trading book per (client_id, binding_id, underlying).

    Architecture:
      HTF scan → SPOT bars (scan_htf_spot gives BEAR + BULL)
      LTF scan → OPTION premium bars (5-min option chart)
      4 contracts in parallel: CE1(S1), CE2(S2), PE1(R1), PE2(R2)
      Bear HTF trap → CE entry;  Bull HTF trap → PE entry
      Cascade: 15-min spot → 5-min option
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        underlying: str,
        lot_multiplier: int,
        client_id: str,
        binding_id: str,
        ts_admin_cfg: dict,
        client_db,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._und = underlying.upper()
        self._lot_mul = lot_multiplier
        self._cid = client_id
        self._bid = binding_id
        self._db = client_db

        _def = _INDEX_CFG.get(self._und, _INDEX_CFG["NIFTY"])
        _adm = ts_admin_cfg.get("per_index", {}).get(self._und, {})
        self._step       = int(_def["step"])
        self._lot_size   = int(_adm.get("lot_size",     _def["lot"]))
        self._sl_buf     = float(_adm.get("sl_buffer",  _def["sl_buf"]))
        self._gap_near   = int(_adm.get("gap_itm_near", _def["gap_near"]))
        self._gap_far    = int(_adm.get("gap_itm_far",  _def["gap_far"]))
        self._cutoff_str = _adm.get("entry_cutoff",     _def["cutoff"])
        self._sq_off_str = _adm.get("sq_off_time",      _def["sq_off"])
        self._entry_win  = _adm.get("entry_window",     _def["window"])
        self._exchange   = _def["exchange"]
        self._htf_source = _def["htf_source"]   # "spot" or "futures"
        self._gap_thresh  = float(ts_admin_cfg.get("gap_threshold_pct", 1.0))
        # CrudeOil HTF = 30-min (frozen per spec); all others = admin-configurable (default 75)
        _htf_override     = _def.get("htf_min_override")
        self._htf_min     = _htf_override if _htf_override else int(ts_admin_cfg.get("htf_minutes", 75))
        self._ltf_min     = int(ts_admin_cfg.get("ltf_minutes", 5))
        self._cascade_min = 15   # intermediate TF for intraday cascade

        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._loop_queues: Dict[str, asyncio.Queue] = {}

        # Strikes: 4 contracts
        # CE side: S1 (near), S2 (far)  — support levels, bears short here
        # PE side: R1 (near), R2 (far)  — resistance levels, bulls buy here
        self._ce1_strike: Optional[int] = None   # S1
        self._ce2_strike: Optional[int] = None   # S2
        self._pe1_strike: Optional[int] = None   # R1
        self._pe2_strike: Optional[int] = None   # R2

        # Upstox instrument keys for fetching option premium bars
        self._ce1_key: Optional[str] = None
        self._ce2_key: Optional[str] = None
        self._pe1_key: Optional[str] = None
        self._pe2_key: Optional[str] = None
        self._fut_key: Optional[str] = None
        self._expiry_str: Optional[str] = None

        self._gap_fired  = False
        self._spot_open  = 0.0
        self._spot_cache = 0.0
        self._expiry_date: Optional[date] = None   # date object, set alongside _expiry_str

        # Live option LTP cache: bkey → last seen LTP
        # Used by zone-reachability check when htf_source="option" (option units vs spot units)
        self._ltp_cache: Dict[str, float] = {}

        # 1m bars — SPOT for HTF scan (htf_source="spot"); per-option for LTF/HTF
        self._bars_spot: List[dict] = []
        self._bars_ce1: List[dict] = []
        self._bars_ce2: List[dict] = []
        self._bars_pe1: List[dict] = []
        self._bars_pe2: List[dict] = []
        self._bars_fut: List[dict] = []
        self._buckets: Dict[str, dict] = {}

        # HTF zones from last spot scan (tuples: (zone_dict, "CE"|"PE"))
        # BEAR zone → CE signal;  BULL zone → PE signal
        self._htf_bear_zones: List[dict] = []   # bear traps → CE entry
        self._htf_bull_zones: List[dict] = []   # bull traps → PE entry
        self._htf_fut_zones: List[dict] = []    # futures only

        # Dedup: zones that already fired an entry today
        self._notified_uids: Set[str] = set()

        # Per-zone LTF status for telemetry: uid → "watching"|"ltf_signal"|"entered"
        self._zone_ltf_status: Dict[str, str] = {}
        # HTF ATR for zone-reachability check (Point 1)
        self._htf_atr_val: float = 0.0

        # Cascade mode: no 75-min zone TRAPPED → use 15-min → 5-min
        self._intraday_mode = False

        # Position
        self._position: Optional[Dict] = None

        self._broker: Optional[Any] = None
        self._rebalancer: Optional[Any] = None   # set via set_rebalancer()
        self._initialized   = False
        self._day_init_done = False

        self._log = self._make_logger()

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer

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
        self._tasks = [
            loop.create_task(self._lifecycle_loop(), name=f"ts_life_{self._und}_{self._cid}"),
            loop.create_task(self._opt_tick_loop(),  name=f"ts_opt_{self._und}_{self._cid}"),
            loop.create_task(self._idx_tick_loop(),  name=f"ts_idx_{self._und}_{self._cid}"),
        ]
        logger.info("TrapScannerEngine[%s/%s/%s]: started.", self._cid, self._bid, self._und)

    async def stop_async(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for q in self._loop_queues.values():
            try:
                self._bus.unsubscribe(Topic.OPTION_TICK, q)
            except Exception:
                pass
        if self._broker:
            try:
                await self._broker.logout()
            except Exception:
                pass

    # ── Lifecycle loop ────────────────────────────────────────────────────────

    async def _lifecycle_loop(self) -> None:
        while self._running:
            try:
                now = datetime.now(IST)
                sq_h, sq_m = map(int, self._sq_off_str.split(":"))
                is_mcx = self._und in ("CRUDEOIL",)
                market_open = time(9, 0) if is_mcx else time(9, 15)

                if now.time() >= time(sq_h, sq_m) and self._initialized:
                    await self._eod_square_off()
                    self._reset_day_state()
                elif not self._initialized and now.time() >= market_open:
                    # Never re-init after sq_off time — prevents infinite EOD→init→EOD loop
                    if now.time() >= time(sq_h, sq_m):
                        await asyncio.sleep(60)
                        continue
                    if not self._day_init_done:
                        ok = await self._morning_init()
                        self._day_init_done = True
                        if ok:
                            self._initialized = True
                        else:
                            self._log.warning("Morning init failed; retrying in 120s")
                            await asyncio.sleep(120)
                            self._day_init_done = False
                            continue
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("TrapScannerEngine lifecycle error: %s", exc)
                await asyncio.sleep(60)

    # ── Morning init ──────────────────────────────────────────────────────────

    async def _morning_init(self) -> bool:
        try:
            prev = await self._fetch_prev_day_ohlc()
            if not prev:
                self._log.warning(
                    "No prev-day OHLC for %s — cannot compute pivots/strikes; will retry in 120s",
                    self._und,
                )
                return False
            H, L, C = prev["high"], prev["low"], prev["close"]
            pivots = _pivot_levels(H, L, C)
            self._log.info(
                "Prev-day H=%.0f L=%.0f C=%.0f | P=%.0f R1=%.0f R2=%.0f S1=%.0f S2=%.0f",
                H, L, C, pivots["pivot"], pivots["r1"], pivots["r2"],
                pivots["s1"], pivots["s2"],
            )

            # Always fetch today_open via REST (not _spot_cache) — live tick can give stale/wrong
            # values for BSE SENSEX; REST is more reliable for gap calculation at morning init
            today_open = await self._fetch_today_open()
            if today_open <= 0:
                today_open = self._spot_cache if self._spot_cache > 0 else C
            if today_open <= 0:
                today_open = C
            self._spot_open = today_open
            gap_pct = abs(today_open - C) / C * 100 if C > 0 else 0.0
            # Sanity: index gaps > 4% are almost impossible for NSE/BSE — bad feed value
            if gap_pct > 4.0 and self._htf_source == "option":
                self._log.warning(
                    "Gap %.1f%% > 4%% looks like bad spot_cache; re-fetching today_open via REST",
                    gap_pct,
                )
                fallback = await self._fetch_today_open()
                if fallback > 0:
                    today_open = fallback
                    self._spot_open = today_open
                    gap_pct = abs(today_open - C) / C * 100
            self._gap_fired = gap_pct >= self._gap_thresh

            if self._gap_fired:
                direction = "UP" if today_open > C else "DOWN"
                atm = _round_strike(today_open, self._step)
                if direction == "UP":
                    self._ce1_strike = atm - self._gap_near
                    self._ce2_strike = atm - self._gap_far
                    self._pe1_strike = atm + self._gap_near
                    self._pe2_strike = atm + self._gap_far
                else:
                    self._ce1_strike = atm + self._gap_near
                    self._ce2_strike = atm + self._gap_far
                    self._pe1_strike = atm - self._gap_near
                    self._pe2_strike = atm - self._gap_far
                self._log.info(
                    "GAP %s %.1f%% → CE1=%d CE2=%d PE1=%d PE2=%d",
                    direction, gap_pct,
                    self._ce1_strike, self._ce2_strike,
                    self._pe1_strike, self._pe2_strike,
                )
            else:
                # CE at support (S1/S2): bears short at support → CE trapped when price bounces
                # PE at resistance (R1/R2): bulls buy at resistance → PE trapped when price drops
                self._ce1_strike = _round_strike(pivots["s1"], self._step)
                self._ce2_strike = _round_strike(pivots["s2"], self._step)
                self._pe1_strike = _round_strike(pivots["r1"], self._step)
                self._pe2_strike = _round_strike(pivots["r2"], self._step)
                self._log.info(
                    "No gap (%.1f%%) → CE1=%d(S1=%.0f) CE2=%d(S2=%.0f) "
                    "PE1=%d(R1=%.0f) PE2=%d(R2=%.0f)",
                    gap_pct,
                    self._ce1_strike, pivots["s1"],
                    self._ce2_strike, pivots["s2"],
                    self._pe1_strike, pivots["r1"],
                    self._pe2_strike, pivots["r2"],
                )

            self._expiry_str, self._expiry_date = await self._get_expiry()
            if not self._expiry_str:
                self._log.warning("No expiry found")
                return False

            if self._htf_source == "futures":
                self._fut_key = _SPOT_KEYS.get(self._und, "")
                self._bars_fut = await self._fetch_1m_history(self._fut_key)
            elif self._htf_source == "spot":
                # Legacy: SPOT bars for HTF, option bars for LTF
                spot_key = _SPOT_KEYS.get(self._und, "")
                self._bars_spot = await self._fetch_1m_history(spot_key)
                self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)
            else:
                # htf_source="option" (NSE/BSE): option bars for BOTH HTF and LTF
                # CE1=S1 bars detect bear seller traps; PE1=R1 bars detect bull seller traps
                # All zone H/L values in option premium units → scan_ltf is consistent
                self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)

            self._run_htf_scan()
            self._htf_atr_val = self._compute_htf_atr()
            self._check_zone_reachability()
            # Point 11: restore today's position if restarted mid-day; discard yesterday's.
            self._load_persisted_position()
            self._log.info(
                "HTF scan: bear=%d bull=%d fut=%d ATR=%.2f intraday_mode=%s position=%s",
                sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED"),
                sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED"),
                sum(1 for e in self._htf_fut_zones  if e["status"] == "TRAPPED"),
                self._htf_atr_val, self._intraday_mode,
                self._position["side"] if self._position else "none",
            )

            await self._subscribe_instruments()
            await self._ensure_broker()
            return True
        except Exception as exc:
            self._log.exception("morning_init error: %s", exc)
            return False

    # ── HTF scan ──────────────────────────────────────────────────────────────

    def _compute_htf_atr(self) -> float:
        """14-bar ATR on HTF bars. Used for zone-reachability distance check."""
        if self._htf_source == "futures":
            bars = self._bars_fut
        elif self._htf_source == "spot":
            bars = self._bars_spot
        else:  # "option": use CE1 bars (representative; same scale as zones)
            bars = self._bars_ce1
        df = _bars_to_df(bars)
        if df.empty:
            return 0.0
        htf = _resample_htf(df, self._htf_min)
        if len(htf) < 2:
            return 0.0
        trs = []
        for i in range(1, len(htf)):
            h, l, pc = htf.iloc[i]["high"], htf.iloc[i]["low"], htf.iloc[i - 1]["close"]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return round(sum(trs[-14:]) / min(len(trs), 14), 2) if trs else 0.0

    def _check_zone_reachability(self) -> None:
        """
        Point 1: if nearest TRAPPED zone trigger is further than 1.5×ATR from current
        LTP, switch to intraday cascade (15-min) immediately.  Uses live LTP — not
        prev_close — so the check stays relevant through the session.
        If a zone later comes into range (HTF boundary rescan), flip back to normal.
        """
        # For option-bar HTF, zone levels are in option premium units — compare option LTP
        if self._htf_source == "option":
            ltp = (self._ltp_cache.get("CE1") or self._ltp_cache.get("PE1") or
                   self._ltp_cache.get("CE2") or self._ltp_cache.get("PE2") or 0.0)
        else:
            ltp = self._spot_cache or self._spot_open
        if not ltp or self._htf_atr_val <= 0:
            # No ATR yet — fall back to simple trapped-count logic
            if self._trapped_zone_count() == 0:
                if not self._intraday_mode:
                    self._intraday_mode = True
                    self._log.info("No TRAPPED zones → cascade")
            else:
                if self._intraday_mode:
                    self._intraday_mode = False
            return

        threshold = 1.5 * self._htf_atr_val
        trapped = (
            [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"] +
            [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"] +
            [e for e in self._htf_fut_zones  if e["status"] == "TRAPPED"]
        )
        if not trapped:
            if not self._intraday_mode:
                self._intraday_mode = True
                self._log.info("No TRAPPED zones → cascade")
            return

        nearest_dist = min(
            abs(ltp - z.get("zone_trigger", z.get("entry", ltp))) for z in trapped
        )
        if nearest_dist > threshold:
            if not self._intraday_mode:
                self._intraday_mode = True
                self._log.info(
                    "Nearest zone too far: dist=%.2f > 1.5×ATR=%.2f ltp=%.2f → cascade",
                    nearest_dist, threshold, ltp,
                )
        else:
            if self._intraday_mode:
                self._intraday_mode = False
                self._log.info(
                    "Zone reachable: dist=%.2f ≤ 1.5×ATR=%.2f → normal mode",
                    nearest_dist, threshold,
                )

    def _run_htf_scan(self, bars_override: Optional[List[dict]] = None,
                      minutes_override: Optional[int] = None) -> None:
        """
        Run HTF scan on option premium bars (NSE/BSE) or futures bars (MCX).
        htf_source="option": scan_htf on CE1 bars for BEAR zones; scan_htf on PE1 for BULL zones
          → zone H/L in option premium units; scan_ltf with same bars is consistent (no mismatch)
        htf_source="spot":   scan_htf_spot on SPOT bars (legacy path)
        htf_source="futures": scan_htf on futures bars
        """
        minutes = minutes_override or self._htf_min
        if self._htf_source == "futures":
            bars = bars_override or self._bars_fut
            df = _bars_to_df(bars)
            if df.empty or len(df) < 2:
                return
            htf = _resample_htf(df, minutes)
            if len(htf) < 2:
                return
            _, entries = scanner.scan_htf(htf)
            self._htf_fut_zones = entries
        elif self._htf_source == "option":
            # Bear zones from CE1 bars: seller traps on CE premium → buy CE
            df_ce = _bars_to_df(bars_override or self._bars_ce1)
            if not df_ce.empty and len(df_ce) >= 2:
                htf_ce = _resample_htf(df_ce, minutes)
                if len(htf_ce) >= 2:
                    _, bear_entries = scanner.scan_htf(htf_ce)
                    self._htf_bear_zones = bear_entries
            # Bull zones from PE1 bars: seller traps on PE premium → buy PE
            df_pe = _bars_to_df(self._bars_pe1)
            if not df_pe.empty and len(df_pe) >= 2:
                htf_pe = _resample_htf(df_pe, minutes)
                if len(htf_pe) >= 2:
                    _, bull_entries = scanner.scan_htf(htf_pe)
                    self._htf_bull_zones = bull_entries
        else:  # "spot" legacy
            bars = bars_override or self._bars_spot
            df = _bars_to_df(bars)
            if df.empty or len(df) < 2:
                return
            htf = _resample_htf(df, minutes)
            if len(htf) < 2:
                return
            _, all_entries = scanner.scan_htf_spot(htf)
            self._htf_bear_zones = [e for e in all_entries if e.get("kind") == "BEAR"]
            self._htf_bull_zones = [e for e in all_entries if e.get("kind") == "BULL"]

    def _trapped_zone_count(self) -> int:
        if self._htf_source == "futures":
            return sum(1 for e in self._htf_fut_zones if e["status"] == "TRAPPED")
        return (sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED") +
                sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED"))

    # ── Tick loops ────────────────────────────────────────────────────────────

    async def _opt_tick_loop(self) -> None:
        q = self._bus.subscribe(Topic.OPTION_TICK)
        self._loop_queues["opt"] = q
        try:
            while self._running:
                try:
                    tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if not self._initialized:
                    continue

                sym = tick.symbol
                ltp = float(tick.ltp)
                ts  = tick.timestamp

                def _match(key: Optional[str], strike: Optional[int], otype: str) -> bool:
                    if key and sym == key:
                        return True
                    return (tick.option_type == otype
                            and abs(float(tick.strike or 0) - (strike or 0)) < 0.1
                            and str(tick.underlying or "").upper() == self._und)

                is_ce1 = _match(self._ce1_key, self._ce1_strike, "CE")
                is_ce2 = _match(self._ce2_key, self._ce2_strike, "CE") and not is_ce1
                is_pe1 = _match(self._pe1_key, self._pe1_strike, "PE")
                is_pe2 = _match(self._pe2_key, self._pe2_strike, "PE") and not is_pe1
                is_fut = (self._htf_source == "futures"
                          and str(tick.underlying or "").upper() == self._und)

                for bkey, bars_list, label in [
                    ("CE1", self._bars_ce1, "CE1"),
                    ("CE2", self._bars_ce2, "CE2"),
                    ("PE1", self._bars_pe1, "PE1"),
                    ("PE2", self._bars_pe2, "PE2"),
                    ("FUT", self._bars_fut, "FUT"),
                ]:
                    active = {
                        "CE1": is_ce1, "CE2": is_ce2,
                        "PE1": is_pe1, "PE2": is_pe2,
                        "FUT": is_fut,
                    }[bkey]
                    if not active:
                        continue
                    self._ltp_cache[bkey] = ltp   # track live option LTP per leg
                    closed = self._update_bucket(bkey, ltp, ts)
                    if closed:
                        bars_list.append(closed)
                        if len(bars_list) > 2000:
                            del bars_list[:-2000]
                        self._on_candle_close(label, ts)

                # SL/T1/trail monitoring uses SCAN-STRIKE option LTP (not 1-ITM exec key).
                # Zone SL levels (zone_high/low) are derived from scan-strike price action,
                # so the scan-strike feed is the correct reference for all exit checks.
                # exec_key is used ONLY for order placement — never for price monitoring.
                if self._position:
                    ps = self._position.get("leg", "")
                    if ((is_ce1 and ps == "CE1") or (is_ce2 and ps == "CE2") or
                            (is_pe1 and ps == "PE1") or (is_pe2 and ps == "PE2") or
                            (is_fut and ps == "FUT")):
                        await self._check_tick_exit(ltp, ts)
        except asyncio.CancelledError:
            pass

    async def _idx_tick_loop(self) -> None:
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._loop_queues["idx"] = q
        try:
            while self._running:
                try:
                    tick: IndexTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if str(tick.symbol).upper() != self._und:
                    continue
                self._spot_cache = float(tick.ltp)
                # SPOT bars only needed when htf_source="spot" (legacy path)
                if self._initialized and self._htf_source == "spot":
                    closed = self._update_bucket("SPOT", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_spot.append(closed)
                        if len(self._bars_spot) > 2000:
                            del self._bars_spot[:-2000]
                        self._on_candle_close("SPOT", tick.timestamp)
        except asyncio.CancelledError:
            pass

    def _update_bucket(self, bkey: str, ltp: float, ts: datetime) -> Optional[dict]:
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
        b["high"]  = max(b["high"], ltp)
        b["low"]   = min(b["low"],  ltp)
        b["close"] = ltp
        b["volume"] += 1
        return None

    # ── Candle close → scan logic ─────────────────────────────────────────────

    def _on_candle_close(self, leg: str, ts: datetime) -> None:
        if self._position:
            return

        # Refresh HTF scan on HTF boundary
        # "option": trigger on CE1 bar close (scans both CE1 and PE1)
        # "spot":   trigger on SPOT bar close
        # "futures": trigger on FUT bar close
        is_htf_boundary = ts.minute % self._htf_min == 0
        if is_htf_boundary:
            htf_trigger = (
                (self._htf_source == "option"  and leg == "CE1") or
                (self._htf_source == "spot"    and leg == "SPOT") or
                (self._htf_source == "futures" and leg == "FUT")
            )
            if htf_trigger:
                self._run_htf_scan()
                self._htf_atr_val = self._compute_htf_atr()
                self._check_zone_reachability()

        # On every LTF boundary — scan option premium bars inside HTF zones
        if ts.minute % self._ltf_min != 0:
            return

        if self._intraday_mode:
            asyncio.get_event_loop().create_task(self._cascade_scan(ts))
        else:
            self._ltf_scan_normal(leg, ts)

    def _ltf_scan_normal(self, leg: str, ts: datetime) -> None:
        """
        Normal mode: 5-min LTF scan on OPTION premium bars inside HTF spot zones.
        BEAR spot zones → scan CE1/CE2 option bars
        BULL spot zones → scan PE1/PE2 option bars
        FUTURES zones   → scan FUT bars
        """
        if self._htf_source == "futures":
            zones = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"]
            self._run_ltf_on("FUT", self._bars_fut, zones, "CE")
            return

        # BEAR zones → buy CE
        bear_zones = [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"]
        if bear_zones:
            # Try CE1 (S1) first, then CE2 (S2)
            if leg in ("CE1",):
                self._run_ltf_on("CE1", self._bars_ce1, bear_zones, "CE")
            elif leg in ("CE2",):
                self._run_ltf_on("CE2", self._bars_ce2, bear_zones, "CE")

        # BULL zones → buy PE
        bull_zones = [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"]
        if bull_zones:
            if leg in ("PE1",):
                self._run_ltf_on("PE1", self._bars_pe1, bull_zones, "PE")
            elif leg in ("PE2",):
                self._run_ltf_on("PE2", self._bars_pe2, bull_zones, "PE")

    def _run_ltf_on(self, leg_key: str, bars: List[dict],
                    htf_zones: List[dict], opt_type: str,
                    require_closed: bool = True) -> None:
        """
        require_closed=True  (normal mode): entry only when 5-min zone is CLOSED
        require_closed=False (cascade mode): entry on TRAPPED (price hit bears' SL is enough)
        """
        if not htf_zones or len(bars) < 3:
            return
        # Bug C fix: LTF scan today-only — historical seeded bars must not produce stale zones
        today = datetime.now(IST).date()
        today_bars = [b for b in bars
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return
        df = _bars_to_df(today_bars[-200:])
        for zone in htf_zones:
            uid = _zone_uid(zone)
            if uid in self._notified_uids:
                continue
            if uid not in self._zone_ltf_status:
                self._zone_ltf_status[uid] = "watching"
            _, ltf_entries = scanner.scan_ltf(
                df,
                htf_zone_high=zone["zone_high"],
                htf_zone_low=zone["zone_low"],
                htf_ref_bar=str(zone.get("ref_ts", "")),
                htf_trap_bar=str(zone.get("trapped_on", zone.get("closed_on", ""))),
                htf_target=zone.get("sl", 0.0),
            )
            if require_closed:
                best = scanner.select_best_ltf_entry(ltf_entries)  # CLOSED only
            else:
                # Cascade: accept TRAPPED (price crossed bears' SL — that IS the signal)
                trapped_ltf = [e for e in ltf_entries if e["status"] in ("TRAPPED", "CLOSED")]
                best = min(trapped_ltf, key=lambda e: e["zone_low"]) if trapped_ltf else None
            if best:
                self._zone_ltf_status[uid] = "ltf_signal"
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg_key, opt_type, best, zone)
                )
                return

    async def _cascade_scan(self, ts: datetime) -> None:
        """
        Intraday cascade: no 75-min zone TRAPPED.
        1. Resample today's completed 1m bars to 15-min (drop current incomplete bar)
        2. scan_htf_spot / scan_htf on those completed 15-min bars
        3. If a 15-min zone TRAPPED → scan_ltf on completed 5-min option bars
        4. Entry fires on TRAPPED status (not CLOSED — cascade rule)
        """
        today = datetime.now(IST).date()
        # Current 15-min bucket start — bars in this bucket are still forming
        cur_15m_start = ts.replace(second=0, microsecond=0)
        cur_15m_start = cur_15m_start.replace(minute=(cur_15m_start.minute // self._cascade_min) * self._cascade_min)

        def _complete_today(src: List[dict]) -> List[dict]:
            """Return today's bars that belong to a COMPLETED 15-min bucket."""
            return [
                b for b in src
                if (pd.to_datetime(b["datetime"]).date() == today and
                    pd.to_datetime(b["datetime"]) < cur_15m_start)
            ]

        if self._htf_source == "futures":
            today_bars = _complete_today(self._bars_fut)
            if len(today_bars) < 4:
                return
            self._run_htf_scan(bars_override=today_bars, minutes_override=self._cascade_min)
            zones_15m = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"]
            self._run_ltf_on("FUT", self._bars_fut, zones_15m, "CE", require_closed=False)
        elif self._htf_source == "option":
            # 15-min CE1 bars for bear cascade; 15-min PE1 bars for bull cascade
            today_ce = _complete_today(self._bars_ce1)
            today_pe = _complete_today(self._bars_pe1)
            bear_15: list = []
            bull_15: list = []
            if len(today_ce) >= 4:
                df_ce = _bars_to_df(today_ce)
                htf_ce = _resample_htf(df_ce, self._cascade_min)
                if len(htf_ce) >= 2:
                    _, be = scanner.scan_htf(htf_ce)
                    bear_15 = [e for e in be if e["status"] == "TRAPPED"]
            if len(today_pe) >= 4:
                df_pe = _bars_to_df(today_pe)
                htf_pe = _resample_htf(df_pe, self._cascade_min)
                if len(htf_pe) >= 2:
                    _, bu = scanner.scan_htf(htf_pe)
                    bull_15 = [e for e in bu if e["status"] == "TRAPPED"]
            if bear_15:
                self._run_ltf_on("CE1", self._bars_ce1, bear_15, "CE", require_closed=False)
                self._run_ltf_on("CE2", self._bars_ce2, bear_15, "CE", require_closed=False)
            if bull_15:
                self._run_ltf_on("PE1", self._bars_pe1, bull_15, "PE", require_closed=False)
                self._run_ltf_on("PE2", self._bars_pe2, bull_15, "PE", require_closed=False)
        else:  # "spot" legacy cascade
            today_bars = _complete_today(self._bars_spot)
            if len(today_bars) < 4:
                return
            df_today = _bars_to_df(today_bars)
            htf_15 = _resample_htf(df_today, self._cascade_min)
            if len(htf_15) < 2:
                return
            _, all_15 = scanner.scan_htf_spot(htf_15)
            bear_15 = [e for e in all_15 if e.get("kind") == "BEAR" and e["status"] == "TRAPPED"]
            bull_15 = [e for e in all_15 if e.get("kind") == "BULL" and e["status"] == "TRAPPED"]
            if bear_15:
                self._run_ltf_on("CE1", self._bars_ce1, bear_15, "CE", require_closed=False)
                self._run_ltf_on("CE2", self._bars_ce2, bear_15, "CE", require_closed=False)
            if bull_15:
                self._run_ltf_on("PE1", self._bars_pe1, bull_15, "PE", require_closed=False)
                self._run_ltf_on("PE2", self._bars_pe2, bull_15, "PE", require_closed=False)

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _on_entry_signal(self, leg: str, opt_type: str,
                                entry: dict, htf_zone: dict) -> None:
        if self._position:
            return
        now = datetime.now(IST)

        # Cutoff gate
        ch, cm = map(int, self._cutoff_str.split(":"))
        if now.time() >= time(ch, cm):
            return

        # Entry window gate (e.g. CrudeOil W2: 18:45–19:15)
        if self._entry_win:
            wh, wm = self._entry_win[0]; eh, em = self._entry_win[1]
            if not (time(wh, wm) <= now.time() <= time(eh, em)):
                return

        uid = _zone_uid(htf_zone)
        if uid in self._notified_uids:
            return
        self._notified_uids.add(uid)
        self._zone_ltf_status[uid] = "entered"

        # Strike: CE1/CE2 or PE1/PE2 based on leg
        scan_strike_map = {
            "CE1": self._ce1_strike, "CE2": self._ce2_strike,
            "PE1": self._pe1_strike, "PE2": self._pe2_strike,
            "FUT": self._ce1_strike,
        }
        scan_strike = scan_strike_map.get(leg) or 0

        # 1-ITM from SPOT ATM (not from scan_strike).
        # Scan/HTF/LTF zone detection runs on the pivot strike (S1/S2/R1/R2).
        # Only the ORDER and exec tracking use spot ATM − 1 step (CE) / + 1 step (PE)
        # so the entry is always ~1 step ITM regardless of how far S1 is from current price.
        spot = self._spot_cache or self._spot_open
        atm  = _round_strike(spot, self._step)
        if opt_type == "CE":
            strike = atm - self._step       # 1 step ITM for CE
        elif opt_type == "PE":
            strike = atm + self._step       # 1 step ITM for PE
        else:
            strike = scan_strike

        ep       = round(entry.get("zone_trigger", entry.get("zone_high", 0)), 2)
        t1_price = round(htf_zone.get("sl", 0), 2)
        sl_price = round(entry["zone_low"] - self._sl_buf, 2)
        total_qty = self._lot_size * self._lot_mul
        t1_qty    = total_qty // 2

        self._log.info(
            "ENTRY %s scan_strike=%d order_strike=%d%s spot=%.2f atm=%d "
            "ep=%.2f sl=%.2f t1=%.2f qty=%d",
            self._und, scan_strike, strike, opt_type, spot, atm,
            ep, sl_price, t1_price, total_qty,
        )

        # Subscribe the 1-ITM option key for live P&L and SL tracking
        exec_key = self._build_upstox_key(strike, opt_type)
        if self._rebalancer is not None:
            try:
                self._rebalancer.pin_strike(self._und, float(strike))
            except Exception:
                pass

        broker = await self._ensure_broker()
        if not broker:
            self._log.error("No broker — entry aborted")
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

        scan_key = {
            "CE1": self._ce1_key, "CE2": self._ce2_key,
            "PE1": self._pe1_key, "PE2": self._pe2_key,
            "FUT": self._fut_key,
        }.get(leg, "")
        self._position = {
            "leg":           leg,
            "side":          opt_type,
            "strike":        strike,        # 1-ITM exec strike (order + P&L)
            "scan_strike":   scan_strike,   # pivot strike used for zone detection
            "spot_at_entry": round(spot, 2),
            "exec_key":      exec_key,      # Upstox key for 1-ITM contract
            "scan_key":      scan_key,      # Upstox key for scan strike (SL tracking)
            "entry_price":   round(avg, 2),
            "sl_price":      sl_price,
            "trail_sl":      sl_price,    # trails 5m option-bar lows after T1
            "last_5m_ts":    None,
            "t1_price":      t1_price,
            "total_qty":     total_qty,
            "t1_qty":        t1_qty,
            "remaining_qty": total_qty,
            "t1_hit":        False,
            "entry_ts":      now.isoformat(),
            "signal_source": f"HTF zone {_zone_uid(htf_zone)} → LTF {leg}",
            "order_id_entry": order_id,
            "order_id_t1":   None,
        }
        self._persist_position()
        self._log.info(
            "ENTRY PLACED scan=%d exec=%d%s spot=%.2f fill=%.2f sl=%.2f t1=%.2f order=%s",
            scan_strike, strike, opt_type, spot, avg, sl_price, t1_price, order_id,
        )

    # ── Tick exit ─────────────────────────────────────────────────────────────

    async def _check_tick_exit(self, ltp: float, ts: Optional[datetime] = None) -> None:
        pos = self._position
        if not pos:
            return

        # T1: 50% at HTF target
        if not pos["t1_hit"] and ltp >= pos["t1_price"]:
            pos["t1_hit"] = True
            pos["remaining_qty"] -= pos["t1_qty"]
            self._log.info("T1 HIT ltp=%.2f t1=%.2f qty=%d", ltp, pos["t1_price"], pos["t1_qty"])
            oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
            pos["order_id_t1"] = oid
            self._persist_position()

        # Advance 5m trail SL using OPTION bar lows (only after T1)
        if pos["t1_hit"] and ts is not None:
            self._update_trail_sl(pos, ts)

        # Exit check
        active_sl = pos["trail_sl"] if pos["t1_hit"] else pos["sl_price"]
        if ltp <= active_sl:
            remaining = pos["remaining_qty"]
            reason = "TRAIL_SL" if pos["t1_hit"] else "SL"
            self._log.info("%s ltp=%.2f sl=%.2f qty=%d", reason, ltp, active_sl, remaining)
            await self._place_exit(remaining, active_sl, reason)
            self._position = None
            self._clear_persisted_position()

    def _update_trail_sl(self, pos: dict, ts: datetime) -> None:
        """
        Trail SL on SCAN-STRIKE option bars. We are always a buyer — option UP = profit.
        On each new 5-min candle close: candidate = prev_5m_LOW - sl_buf.
        trail_sl only moves UP (ratchet). Never down. Same rule for CE and PE.

        Why bar LOW (not HIGH): new bears enter below the 5m bar low (shorting there).
        Their SL is above the bar high. Our SL = their entry minus buffer.
        We exit when those bears are in profit = real reversal, not a normal retracement.
        As each successive 5m bar low is higher, our SL ratchets up with it.
        """
        bar_5m = ts.replace(second=0, microsecond=0)
        bar_5m = bar_5m.replace(minute=(bar_5m.minute // 5) * 5)
        last = pos.get("last_5m_ts")
        if last is not None and bar_5m <= last:
            return
        pos["last_5m_ts"] = bar_5m

        leg_bars_map = {
            "CE1": self._bars_ce1, "CE2": self._bars_ce2,
            "PE1": self._bars_pe1, "PE2": self._bars_pe2,
            "FUT": self._bars_fut,
        }
        bars = leg_bars_map.get(pos["leg"], [])
        if not bars:
            return

        prev_start = bar_5m - timedelta(minutes=5)
        bucket = [
            b for b in bars[-15:]
            if prev_start <= datetime.fromisoformat(b["datetime"]) < bar_5m
        ]
        if not bucket:
            return

        prev_low  = min(b["low"] for b in bucket)
        candidate = round(prev_low - self._sl_buf, 2)
        if candidate > pos["trail_sl"]:
            old = pos["trail_sl"]
            pos["trail_sl"] = candidate
            self._log.info("TRAIL_SL %.2f → %.2f (5m_low=%.2f buf=%.2f)",
                           old, candidate, prev_low, self._sl_buf)
            self._persist_position()

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
            oid = await broker.place_order(req)
            self._log.info("EXIT %s qty=%d order=%s", reason, qty, oid)
            return oid
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
        self._clear_persisted_position()

    def _reset_day_state(self) -> None:
        self._initialized   = False
        self._intraday_mode = False
        self._day_init_done = False
        self._bars_spot = []; self._bars_fut = []
        self._bars_ce1  = []; self._bars_ce2 = []
        self._bars_pe1  = []; self._bars_pe2 = []
        self._htf_bear_zones = []; self._htf_bull_zones = []
        self._htf_fut_zones  = []
        self._buckets        = {}
        self._notified_uids  = set()
        self._zone_ltf_status = {}
        self._htf_atr_val = 0.0
        self._ltp_cache   = {}
        self._ce1_strike = None; self._ce2_strike = None
        self._pe1_strike = None; self._pe2_strike = None
        self._ce1_key = None; self._ce2_key = None
        self._pe1_key = None; self._pe2_key = None
        self._expiry_str = None; self._expiry_date = None
        self._clear_persisted_position()

    # ── Position persistence (Point 11: no carryforward across days) ──────────

    def _position_file(self) -> str:
        os.makedirs("data", exist_ok=True)
        return os.path.join("data", f"trap_scanner_{self._cid}_{self._bid}_{self._und}.json")

    def _persist_position(self) -> None:
        """Write current position to disk so a restart can recover today's trade."""
        try:
            with open(self._position_file(), "w") as f:
                json.dump(self._position, f)
        except Exception as exc:
            self._log.warning("_persist_position failed: %s", exc)

    def _clear_persisted_position(self) -> None:
        try:
            p = self._position_file()
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    def _load_persisted_position(self) -> None:
        """
        On morning_init: restore today's trade if the process restarted mid-day.
        Any position whose entry_ts is NOT today is discarded (Point 11).
        """
        try:
            p = self._position_file()
            if not os.path.exists(p):
                return
            with open(p) as f:
                saved = json.load(f)
            if not saved:
                return
            entry_ts = saved.get("entry_ts", "")
            entry_date = datetime.fromisoformat(entry_ts).date() if entry_ts else None
            if entry_date != date.today():
                self._log.info("Discarding persisted position from %s (not today)", entry_date)
                self._clear_persisted_position()
                return
            self._position = saved
            self._log.info(
                "Restored persisted position: %s %s strike=%s entry=%.2f sl=%.2f qty=%d",
                saved.get("side"), saved.get("leg"), saved.get("strike"),
                saved.get("entry_price", 0), saved.get("sl_price", 0),
                saved.get("remaining_qty", 0),
            )
        except Exception as exc:
            self._log.warning("_load_persisted_position failed: %s", exc)

    # ── Broker ────────────────────────────────────────────────────────────────

    async def _ensure_broker(self):
        if self._broker and self._broker.is_authenticated:
            return self._broker
        try:
            bindings = self._db.get_bindings_sync(self._cid)
            row = next((b for b in bindings if b.get("binding_id") == self._bid), None)
            if not row:
                return None
            from config.client_profiles import BrokerBinding
            b = BrokerBinding(**{k: v for k, v in row.items()
                                 if k in BrokerBinding.__dataclass_fields__})
            from execution_bridge.base_broker import create_broker
            broker = create_broker(b, self._cid)
            if not await broker.authenticate():
                return None
            self._broker = broker
            return broker
        except Exception as exc:
            self._log.error("_ensure_broker: %s", exc)
            return None

    # ── Data fetching ─────────────────────────────────────────────────────────

    async def _fetch_prev_day_ohlc(self) -> Optional[Dict]:
        try:
            token = self._get_upstox_token()
            if not token:
                self._log.warning("_fetch_prev_day_ohlc: no Upstox token")
                return None
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key:
                self._log.warning("_fetch_prev_day_ohlc: no spot key for %s", self._und)
                return None
            import aiohttp
            today   = date.today()
            fr_date = today - timedelta(days=10)
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{spot_key}/day/{today}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        body = await r.text()
                        self._log.warning(
                            "_fetch_prev_day_ohlc: HTTP %d for %s — %s",
                            r.status, spot_key, body[:200],
                        )
                        return None
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            if len(candles) < 2:
                self._log.warning(
                    "_fetch_prev_day_ohlc: only %d candle(s) returned for %s",
                    len(candles), spot_key,
                )
                return None
            prev = candles[1]
            return {"open": float(prev[1]), "high": float(prev[2]),
                    "low":  float(prev[3]), "close": float(prev[4])}
        except Exception as exc:
            self._log.warning("_fetch_prev_day_ohlc: %s", exc)
            return None

    async def _fetch_today_open(self) -> float:
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
            return float(candles[-1][1]) if candles else 0.0
        except Exception as exc:
            self._log.warning("_fetch_today_open: %s", exc)
            return 0.0

    async def _fetch_1m_history(self, instrument_key: str) -> List[dict]:
        if not instrument_key:
            return []
        try:
            token = self._get_upstox_token()
            if not token:
                return []
            import aiohttp
            today   = date.today()
            to_date = today + timedelta(days=1)   # include today (Upstox excludes to_date)
            fr_date = today - timedelta(days=8)   # prev week + current week for HTF pattern seed
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{instrument_key}/1minute/{to_date}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            return [
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
                for c in reversed(candles)  # oldest first
            ]
        except Exception as exc:
            self._log.warning("_fetch_1m_history(%s): %s", instrument_key, exc)
            return []

    def _get_upstox_token(self) -> Optional[str]:
        creds = self._db.get_feeder_creds_sync("upstox")
        return (creds or {}).get("access_token") or ""

    async def _get_expiry(self) -> tuple:
        """Returns (expiry_str, expiry_date). expiry_str = e.g. '18JUN26', expiry_date = date obj."""
        _EXPIRY_DOW = {
            "NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
            "SENSEX": 4, "MIDCPNIFTY": 1,
        }
        if self._und == "CRUDEOIL":
            return date.today().strftime("%b%y").upper(), None
        weekday = _EXPIRY_DOW.get(self._und, 3)
        d = date.today()
        for _ in range(7):
            if d.weekday() == weekday:
                return d.strftime("%d%b%y").upper(), d
            d += timedelta(days=1)
        return None, None

    async def _subscribe_instruments(self) -> None:
        if self._rebalancer is None:
            self._log.warning("No rebalancer set — tracked strikes will rely on ATM window only")
            return
        for strike in [self._ce1_strike, self._ce2_strike, self._pe1_strike, self._pe2_strike]:
            if strike:
                try:
                    self._rebalancer.pin_strike(self._und, float(strike))
                except Exception as exc:
                    self._log.warning("pin_strike %s %s: %s", self._und, strike, exc)
        self._log.info(
            "pinned CE1=%s CE2=%s PE1=%s PE2=%s for %s",
            self._ce1_strike, self._ce2_strike,
            self._pe1_strike, self._pe2_strike, self._und,
        )

    def _build_upstox_key(self, strike: Optional[int], opt_type: str) -> str:
        if not strike:
            return ""
        exp = self._expiry_str or ""
        # Try global REGISTRY first — BSE_FO requires a numeric token (not symbol format).
        # REGISTRY is pre-loaded by the rebalancer at startup; if loaded it has correct keys.
        try:
            from data_layer.instrument_registry import REGISTRY
            if self._expiry_date is not None and REGISTRY.is_loaded(self._und):
                key = REGISTRY.get_upstox_key(self._und, self._expiry_date, int(strike), opt_type)
                if key:
                    return key
        except Exception:
            pass
        # Fallback: constructed symbol (works for NSE_FO; BSE_FO may return empty from REST)
        _PFX = {
            "NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "FINNIFTY": "NSE_FO|", "SENSEX": "BSE_FO|", "MIDCPNIFTY": "NSE_FO|",
        }
        pfx = _PFX.get(self._und, "NSE_FO|")
        return f"{pfx}{self._und}{exp}{strike}{opt_type}"

    def _build_broker_symbol(self, strike: Optional[int], opt_type: str) -> str:
        exp = self._expiry_str or ""
        return f"{self._und}{exp}{strike}{opt_type}"

    # ── Telemetry ─────────────────────────────────────────────────────────────

    def _zone_info_list(self, zones: List[dict], opt_type: str) -> list:
        ltp = self._spot_cache or 0.0
        atr = self._htf_atr_val
        threshold = 1.5 * atr if atr > 0 else None
        result = []
        for z in zones:
            uid = _zone_uid(z)
            trigger = round(z.get("zone_trigger", z.get("entry", 0)), 2)
            dist = round(abs(ltp - trigger), 2) if ltp else None
            reachable = (dist is not None and threshold is not None and dist <= threshold)
            result.append({
                "uid":          uid,
                "opt_type":     opt_type,
                "zone_low":     round(z.get("zone_low",  0), 2),
                "zone_high":    round(z.get("zone_high", 0), 2),
                "zone_trigger": trigger,
                "htf_target":   round(z.get("sl", 0), 2),
                "trapped_on":   str(z.get("trapped_on", "") or ""),
                "status":       z.get("status", ""),
                "reachable":    reachable,
                "dist_from_ltp": dist,
                "ltf_status":   self._zone_ltf_status.get(uid, "watching"),
            })
        return result

    def telemetry_snapshot(self) -> dict:
        pos = self._position
        ltp = self._spot_cache or 0.0
        atr = self._htf_atr_val

        zones = (
            self._zone_info_list(self._htf_bear_zones, "CE") +
            self._zone_info_list(self._htf_bull_zones, "PE") +
            self._zone_info_list(self._htf_fut_zones,  "FUT")
        )
        bear_trapped = sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED")
        bull_trapped = sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED")
        fut_trapped  = sum(1 for e in self._htf_fut_zones  if e["status"] == "TRAPPED")

        def _best_zone_summary(zone_list: list) -> Optional[dict]:
            """Most recent TRAPPED zone for UI display."""
            trapped = [z for z in zone_list if z["status"] == "TRAPPED"]
            if not trapped:
                return None
            z = trapped[-1]
            uid = _zone_uid(z)
            return {
                "zone_high":    round(z.get("zone_high", 0), 2),
                "zone_low":     round(z.get("zone_low", 0), 2),
                "zone_trigger": round(z.get("zone_trigger", z.get("entry", 0)), 2),
                "t1_target":    round(z.get("sl", 0), 2),
                "trapped_on":   str(z.get("trapped_on", "")),
                "ltf_status":   self._zone_ltf_status.get(uid, "watching"),
            }

        # Per-contract LTP and status for UI (mirrors NiftyTrapScanner dashboard table)
        contracts = {
            "CE1": {"strike": self._ce1_strike, "ltp": self._ltp_cache.get("CE1"),
                    "bars": len(self._bars_ce1),
                    "zone": _best_zone_summary(self._htf_bear_zones)},
            "CE2": {"strike": self._ce2_strike, "ltp": self._ltp_cache.get("CE2"),
                    "bars": len(self._bars_ce2), "zone": None},
            "PE1": {"strike": self._pe1_strike, "ltp": self._ltp_cache.get("PE1"),
                    "bars": len(self._bars_pe1),
                    "zone": _best_zone_summary(self._htf_bull_zones)},
            "PE2": {"strike": self._pe2_strike, "ltp": self._ltp_cache.get("PE2"),
                    "bars": len(self._bars_pe2), "zone": None},
        }

        return {
            "underlying":     self._und,
            "client_id":      self._cid,
            "binding_id":     self._bid,
            "initialized":    self._initialized,
            "intraday_mode":  self._intraday_mode,
            "gap_fired":      self._gap_fired,
            "spot_ltp":       ltp,
            "htf_source":     self._htf_source,
            "htf_atr":        atr,
            "ce1_strike":     self._ce1_strike,
            "ce2_strike":     self._ce2_strike,
            "pe1_strike":     self._pe1_strike,
            "pe2_strike":     self._pe2_strike,
            "expiry":         self._expiry_str,
            "bear_zones":     bear_trapped,
            "bull_zones":     bull_trapped,
            "fut_zones":      fut_trapped,
            "zones":          zones,
            "contracts":      contracts,
            "bars_spot":      len(self._bars_spot),
            "bars_ce1":       len(self._bars_ce1),
            "bars_pe1":       len(self._bars_pe1),
            "notified_uids":  len(self._notified_uids),
            "position": {
                "leg":           pos["leg"],
                "side":          pos["side"],
                "strike":        pos["strike"],        # 1-ITM exec strike
                "scan_strike":   pos.get("scan_strike"),
                "spot_at_entry": pos.get("spot_at_entry"),
                "scan_key":      pos.get("scan_key", ""),
                "exec_key":      pos.get("exec_key", ""),
                "signal_source": pos.get("signal_source", ""),
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
