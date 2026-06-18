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
                   "window": None, "exchange": "NFO", "htf_source": "spot"},
    "BANKNIFTY":  {"step": 100, "lot": 30,  "gap_near": 400, "gap_far": 800,
                   "sl_buf": 4.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "FINNIFTY":   {"step": 50,  "lot": 40,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "SENSEX":     {"step": 100, "lot": 20,  "gap_near": 300, "gap_far": 600,
                   "sl_buf": 2.0, "cutoff": "15:20", "sq_off": "15:25",
                   "window": None, "exchange": "BFO", "htf_source": "spot"},
    "MIDCPNIFTY": {"step": 25,  "lot": 75,  "gap_near": 100, "gap_far": 200,
                   "sl_buf": 1.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "CRUDEOIL":   {"step": 100, "lot": 100, "gap_near": 200, "gap_far": 500,
                   "sl_buf": 20.0, "cutoff": "22:45", "sq_off": "23:00",
                   "window": [[14, 30], [22, 45]], "exchange": "MCX",
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
        self._admin_cfg   = ts_admin_cfg
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

        # htf_source="spot": separate option-level zones for LTF entry
        # Spot zones (above) decide direction; these decide the actual option entry level
        self._opt_bear_zones: List[dict] = []   # CE option HTF zones (option premium units)
        self._opt_bull_zones: List[dict] = []   # PE option HTF zones (option premium units)

        # Dedup: zones that already fired an entry today
        self._notified_uids: Set[str] = set()

        # Per-zone LTF status for telemetry: uid → "watching"|"ltf_signal"|"entered"
        self._zone_ltf_status: Dict[str, str] = {}
        # HTF ATR for zone-reachability check (Point 1)
        self._htf_atr_val: float = 0.0

        # Cascade mode: no 75-min zone TRAPPED → use 15-min → 5-min
        # Per-leg for htf_source="option" (CE and PE evaluated independently)
        self._intraday_mode = False      # legacy / futures / spot
        self._intraday_mode_ce = False   # option-source CE leg
        self._intraday_mode_pe = False   # option-source PE leg

        # Position
        self._position: Optional[Dict] = None
        self._sweep_watch: Optional[Dict] = None   # liquidity sweep re-entry after SL

        self._broker: Optional[Any] = None
        self._rebalancer: Optional[Any] = None   # set via set_rebalancer()
        self._mcx_feeder: Optional[Any] = None   # dedicated Upstox2 feeder for MCX
        self._initialized   = False
        self._day_init_done = False

        self._log = self._make_logger()

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer

    def set_mcx_feeder(self, feeder) -> None:
        """Dedicated Upstox2 feeder for MCX option subscriptions (CrudeOil/Gold)."""
        self._mcx_feeder = feeder

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
                market_open = time(9, 5) if is_mcx else time(9, 16)

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
            if self._htf_source == "futures":
                # Futures-mode (CrudeOil/BTC/ETH): get prev_close + today_open from the SAME
                # active contract's 1m bars. Daily-candle API can return an expired contract's
                # close (e.g. June close when July contract is active) → false 5.9% gap.
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    fk_early = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    fk_early = ""
                if not fk_early:
                    self._log.warning(
                        "Futures-mode %s: no futures key in REGISTRY; will retry in 120s", self._und
                    )
                    return False
                C_fut, today_open_fut = await self._fetch_prev_close_and_today_open_from_1m(fk_early)
                if C_fut <= 0:
                    self._log.warning(
                        "Futures-mode %s: no prev-day close from 1m bars; will retry in 120s", self._und
                    )
                    return False
                C = C_fut
                H, L = C, C   # H/L unused for futures-mode (no pivots needed)
                pivots = _pivot_levels(C, C, C)  # placeholder — pivots not used in futures-mode
                today_open = today_open_fut if today_open_fut > 0 else C
                self._log.info("Futures prev_close=%.0f today_open=%.0f (from 1m bars, same contract)",
                               C, today_open)
            else:
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
                # Always fetch today_open via REST — must be the FIRST bar's OPEN (9:15 AM),
                # NOT the current live price. Gap direction = prev-day close vs today's open.
                today_open = await self._fetch_today_open()
                if today_open <= 0:
                    today_open = self._spot_cache if self._spot_cache > 0 else C
                if today_open <= 0:
                    today_open = C
                # Sanity: index gaps > 4% are almost impossible for NSE/BSE — bad feed value
                gap_check = abs(today_open - C) / C * 100 if C > 0 else 0.0
                if gap_check > 4.0:
                    self._log.warning(
                        "Gap %.1f%% > 4%% looks like bad spot_cache; re-fetching today_open via REST",
                        gap_check,
                    )
                    fallback = await self._fetch_today_open()
                    if fallback > 0:
                        today_open = fallback

            self._spot_open = today_open
            gap_pct = abs(today_open - C) / C * 100 if C > 0 else 0.0
            self._gap_fired = gap_pct >= self._gap_thresh

            # Futures-mode underlyings (CrudeOil etc.) always use ATM ± fixed ITM offsets —
            # pivot-based S1/S2/R1/R2 is meaningless for commodity options.
            use_atm_offsets = self._htf_source == "futures" or self._gap_fired

            if use_atm_offsets:
                direction = "UP" if today_open >= C else "DOWN"
                atm = _round_strike(today_open, self._step)
                # Futures mode: always buy ITM options → CE = ATM-offset (ITM call),
                # PE = ATM+offset (ITM put). Direction never flips strike assignment.
                self._ce1_strike = atm - self._gap_near
                self._ce2_strike = atm - self._gap_far
                self._pe1_strike = atm + self._gap_near
                self._pe2_strike = atm + self._gap_far
                label = f"GAP {direction} {gap_pct:.1f}%" if self._gap_fired else f"ATM±offsets (no gap {gap_pct:.1f}%)"
                self._log.info(
                    "%s → CE1=%d CE2=%d PE1=%d PE2=%d",
                    label,
                    self._ce1_strike, self._ce2_strike,
                    self._pe1_strike, self._pe2_strike,
                )
            else:
                # CE at support (S1/S2): CE option tracks support zone traps
                # PE at resistance (R1/R2): PE option tracks resistance zone traps
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
                # Use REGISTRY for the near-month futures key (correct for MCX CrudeOil)
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    fk = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    fk = ""
                self._fut_key = fk or _SPOT_KEYS.get(self._und, "")
                self._bars_fut = await self._fetch_1m_history(self._fut_key)
                # Also seed option bars (needed for LTF scan)
                self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)
                self._log.info(
                    "Bars seeded — FUT(%s)=%d CE1(%s)=%d CE2(%s)=%d PE1(%s)=%d PE2(%s)=%d",
                    self._fut_key, len(self._bars_fut),
                    self._ce1_key, len(self._bars_ce1),
                    self._ce2_key, len(self._bars_ce2),
                    self._pe1_key, len(self._bars_pe1),
                    self._pe2_key, len(self._bars_pe2),
                )
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
                self._log.info(
                    "Bars seeded — CE1(%s)=%d CE2(%s)=%d PE1(%s)=%d PE2(%s)=%d",
                    self._ce1_key, len(self._bars_ce1),
                    self._ce2_key, len(self._bars_ce2),
                    self._pe1_key, len(self._bars_pe1),
                    self._pe2_key, len(self._bars_pe2),
                )

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
            bars = self._bars_ce1  # option bars — ATR must match option zone scale
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
        Per-leg cascade for htf_source=option: CE and PE evaluated independently.
        CE cascades if no bear zones OR nearest bear zone > 1.5×ATR from CE LTP.
        PE cascades if no bull zones OR nearest bull zone > 1.5×ATR from PE LTP.
        For futures/spot: single shared _intraday_mode (unchanged).
        """
        threshold = 1.5 * self._htf_atr_val if self._htf_atr_val > 0 else None

        if self._htf_source == "option":
            ltp_ce = self._ltp_cache.get("CE1") or self._ltp_cache.get("CE2") or 0.0
            ltp_pe = self._ltp_cache.get("PE1") or self._ltp_cache.get("PE2") or 0.0

            def _regime_ok(zone: dict, ltp: float) -> bool:
                """Drop zones from a different premium regime (theta-decayed or expired)."""
                if ltp <= 0:
                    return True
                zh = zone.get("zone_high", 0.0)
                zl = zone.get("zone_low", 0.0)
                return zh > ltp * 0.3 and zl < ltp * 3.0

            # --- CE leg ---
            bear_trapped = [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"]
            bear_trapped = [z for z in bear_trapped if _regime_ok(z, ltp_ce)]
            if not bear_trapped or not ltp_ce or threshold is None:
                new_ce = True
                reason_ce = "No bear zones" if not bear_trapped else "No CE LTP/ATR"
            else:
                dist_ce = min(abs(ltp_ce - z.get("zone_trigger", ltp_ce)) for z in bear_trapped)
                new_ce = dist_ce > threshold
                reason_ce = f"dist={dist_ce:.1f} {'>' if new_ce else '<='} {threshold:.1f}"
            if new_ce != self._intraday_mode_ce:
                self._intraday_mode_ce = new_ce
                self._log.info("CE cascade=%s (%s) ltp_ce=%.1f", new_ce, reason_ce, ltp_ce)

            # --- PE leg ---
            bull_trapped = [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"]
            bull_trapped = [z for z in bull_trapped if _regime_ok(z, ltp_pe)]
            if not bull_trapped or not ltp_pe or threshold is None:
                new_pe = True
                reason_pe = "No bull zones" if not bull_trapped else "No PE LTP/ATR"
            else:
                dist_pe = min(abs(ltp_pe - z.get("zone_trigger", ltp_pe)) for z in bull_trapped)
                new_pe = dist_pe > threshold
                reason_pe = f"dist={dist_pe:.1f} {'>' if new_pe else '<='} {threshold:.1f}"
            if new_pe != self._intraday_mode_pe:
                self._intraday_mode_pe = new_pe
                self._log.info("PE cascade=%s (%s) ltp_pe=%.1f", new_pe, reason_pe, ltp_pe)

            # Legacy single flag = True if either leg is cascading
            self._intraday_mode = self._intraday_mode_ce or self._intraday_mode_pe
            return

        # --- futures / spot: single shared flag ---
        ltp = self._spot_cache or self._spot_open
        if not ltp or threshold is None:
            if self._trapped_zone_count() == 0:
                if not self._intraday_mode:
                    self._intraday_mode = True
                    self._log.info("No TRAPPED zones → cascade")
            else:
                if self._intraday_mode:
                    self._intraday_mode = False
            return

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
                    "Nearest zone too far: dist=%.2f > 1.5*ATR=%.2f ltp=%.2f → cascade",
                    nearest_dist, threshold, ltp,
                )
        else:
            if self._intraday_mode:
                self._intraday_mode = False
                self._log.info(
                    "Zone reachable: dist=%.2f <= 1.5*ATR=%.2f → normal mode",
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
        else:  # "spot": scan spot for direction, option bars for entry zones
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

            # Now scan option bars for option-premium-level zones (used for LTF entry)
            # Direction (CE vs PE) is gated by spot bear/bull zones above
            df_ce = _bars_to_df(self._bars_ce1)
            if not df_ce.empty and len(df_ce) >= 2:
                htf_ce = _resample_htf(df_ce, minutes)
                if len(htf_ce) >= 2:
                    _, bear_opt = scanner.scan_htf(htf_ce)
                    self._opt_bear_zones = bear_opt
            df_pe = _bars_to_df(self._bars_pe1)
            if not df_pe.empty and len(df_pe) >= 2:
                htf_pe = _resample_htf(df_pe, minutes)
                if len(htf_pe) >= 2:
                    _, bull_opt = scanner.scan_htf(htf_pe)
                    self._opt_bull_zones = bull_opt

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
                # FUT bars built from INDEX_TICK in _idx_tick_loop — never route option ticks to FUT
                is_fut = False

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

                    # Futures-mode T1 is in option domain — check option ltp here.
                    # SL stays in futures domain (_idx_tick_loop). Only T1 uses option ltp.
                    if (self._htf_source == "futures" and ps == "FUT"
                            and not self._position.get("t1_hit")
                            and self._position.get("t1_price", 0) > 0):
                        opt_side = self._position.get("opt_type", "CE")
                        if (opt_side == "CE" and is_ce1) or (opt_side == "PE" and is_pe1):
                            await self._check_option_t1(ltp, ts)
        except asyncio.CancelledError:
            pass

    async def _idx_tick_loop(self) -> None:
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._loop_queues["idx"] = q
        _last_resub = datetime.now(IST)
        try:
            while self._running:
                try:
                    tick: IndexTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if str(tick.symbol).upper() != self._und:
                    continue
                raw_ltp = float(tick.ltp)
                # Sanity guard: BSE SENSEX WebSocket intermittently mis-decodes to ~80000
                # (3-4% above real value). _spot_open is set during warm_start from a
                # URL-encoded REST call and is a reliable anchor. Before warm_start
                # completes (_initialized=False, _spot_open=0) we skip _spot_cache
                # entirely so pre-init bad ticks can't contaminate the distance check.
                if not self._initialized:
                    continue
                # Guard against broker mis-decode spikes (e.g. BSE 80025 vs real 73840).
                # Use LAST GOOD TICK as rolling reference (not day-open) so legitimate
                # intraday moves of any size are accepted; only sudden per-tick jumps
                # are rejected. MCX crude/gold can gap 5%+ at session open so 8% cap.
                _ref = self._spot_cache if self._spot_cache > 0 else self._spot_open
                _guard = 0.08 if self._cfg.exchange.is_mcx(self._und) else 0.04
                if _ref > 0 and abs(raw_ltp - _ref) / _ref > _guard:
                    self._log.warning(
                        "SPOT tick rejected: ltp=%.2f deviates >%.1f%% from last=%.2f (mis-decode?)",
                        raw_ltp, _guard * 100, _ref,
                    )
                    continue
                self._spot_cache = raw_ltp
                # Re-subscribe tracked option keys every 60s — survives feeder reconnect
                now = datetime.now(IST)
                if (now - _last_resub).total_seconds() >= 60:
                    ce1_ltp = self._ltp_cache.get("CE1", 0)
                    pe1_ltp = self._ltp_cache.get("PE1", 0)
                    self._log.info(
                        "heartbeat: spot=%.2f CE1=%.1f PE1=%.1f pos=%s [keys: ce1=%s pe1=%s]",
                        raw_ltp, ce1_ltp, pe1_ltp,
                        self._position["side"] if self._position else "none",
                        self._ce1_key, self._pe1_key,
                    )
                    _last_resub = now
                    keys = [k for k in [self._ce1_key, self._ce2_key,
                                        self._pe1_key, self._pe2_key] if k]
                    if keys:
                        feeder = (self._mcx_feeder if self._mcx_feeder is not None
                                  else getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None)
                        if feeder and hasattr(feeder, "subscribe_tokens"):
                            try:
                                await feeder.subscribe_tokens(keys)
                            except Exception:
                                pass
                # SPOT bars: legacy htf_source="spot" path
                if self._htf_source == "spot":
                    closed = self._update_bucket("SPOT", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_spot.append(closed)
                        if len(self._bars_spot) > 2000:
                            del self._bars_spot[:-2000]
                        self._on_candle_close("SPOT", tick.timestamp)
                # FUT bars: futures-mode (CrudeOil/BTC/ETH) — underlying LTP arrives as INDEX_TICK
                elif self._htf_source == "futures":
                    fut_ltp = float(tick.ltp)
                    self._ltp_cache["FUT"] = fut_ltp
                    closed = self._update_bucket("FUT", tick.ltp, tick.timestamp)
                    if closed:
                        self._bars_fut.append(closed)
                        if len(self._bars_fut) > 2000:
                            del self._bars_fut[:-2000]
                        self._on_candle_close("FUT", tick.timestamp)
                    # Futures-mode: SL/T1/trail all checked against futures LTP.
                    # Signal, SL level, and T1 level all in futures ₹ → consistent.
                    # When triggered, _place_exit closes the 1-ITM option via exec_key.
                    if self._position and self._position.get("leg") == "FUT":
                        await self._check_tick_exit(fut_ltp, tick.timestamp)
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
        # Futures-mode TSL: on every FUT candle close while in position,
        # check for new bear traps ABOVE entry → advance trail_sl
        if self._position and self._htf_source == "futures" and leg == "FUT":
            self._update_futures_tsl(ts)
            return
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
                prev_mode = self._intraday_mode
                self._check_zone_reachability()
                self._log.info(
                    "HTF scan: bear=%d bull=%d fut=%d ATR=%.2f intraday_mode=%s position=%s",
                    sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED"),
                    sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED"),
                    sum(1 for e in self._htf_fut_zones  if e["status"] == "TRAPPED"),
                    self._htf_atr_val, self._intraday_mode,
                    self._position["side"] if self._position else "none",
                )

        # On every LTF boundary — scan option premium bars inside HTF zones
        if ts.minute % self._ltf_min != 0:
            return

        if self._htf_source == "option":
            ce_leg = leg in ("CE1", "CE2")
            pe_leg = leg in ("PE1", "PE2")
            do_cascade_ce = ce_leg and self._intraday_mode_ce
            do_cascade_pe = pe_leg and self._intraday_mode_pe
            # Always run HTF normal scan first (uses 75-min near zones)
            self._ltf_scan_normal(leg, ts)
            # Also always dispatch cascade as fallback: if HTF zones exist but price never
            # enters them (different premium regime intraday), cascade finds today's zones.
            # _on_entry_signal's _notified_uids + self._position guard prevent double entry.
            asyncio.get_event_loop().create_task(
                self._cascade_scan(ts, cascade_ce=ce_leg, cascade_pe=pe_leg)
            )
        else:
            if self._intraday_mode:
                asyncio.get_event_loop().create_task(self._cascade_scan(ts))
            else:
                self._ltf_scan_normal(leg, ts)

    def _ltf_scan_normal(self, leg: str, ts: datetime) -> None:
        """
        Normal mode: 5-min LTF scan on OPTION premium bars inside HTF spot zones.
        BEAR spot zones → scan CE1/CE2 option bars
        BULL spot zones → scan PE1/PE2 option bars
        FUTURES mode    → LTF also on FUTURES bars (institutions move via futures, not options).
                          Option bars are illiquid/gappy — unsuitable for trap detection.
                          Only ORDER and post-entry exits use options.
        """
        if self._htf_source == "futures":
            if leg != "FUT":
                return  # LTF scan only on FUT candle close
            zones = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"]
            self._run_ltf_futures_mode("FUT", self._bars_fut, zones, "CE")
            return

        # BEAR zones → buy CE
        # htf_source="spot": direction gate = spot bear zones; entry zones = option premium zones
        spot_has_bear = bool([e for e in self._htf_bear_zones if e["status"] == "TRAPPED"])
        spot_has_bull = bool([e for e in self._htf_bull_zones if e["status"] == "TRAPPED"])

        if self._htf_source == "spot":
            bear_zones = [e for e in self._opt_bear_zones if e["status"] == "TRAPPED"] if spot_has_bear else []
            bull_zones = [e for e in self._opt_bull_zones if e["status"] == "TRAPPED"] if spot_has_bull else []
        else:
            bear_zones = [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"]
            bull_zones = [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"]

        if bear_zones:
            # option-source: accept TRAPPED on LTF (same rule as cascade — premium traps
            # rarely complete a full CLOSED 5-min bar; TRAPPED is sufficient signal)
            _rc = self._htf_source not in ("option", "spot")
            if leg in ("CE1",):
                self._run_ltf_on("CE1", self._bars_ce1, bear_zones, "CE", require_closed=_rc)
            elif leg in ("CE2",):
                self._run_ltf_on("CE2", self._bars_ce2, bear_zones, "CE", require_closed=_rc)

        if bull_zones:
            _rc = self._htf_source not in ("option", "spot")
            if leg in ("PE1",):
                self._run_ltf_on("PE1", self._bars_pe1, bull_zones, "PE", require_closed=_rc)
            elif leg in ("PE2",):
                self._run_ltf_on("PE2", self._bars_pe2, bull_zones, "PE", require_closed=_rc)

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
        current_price = self._ltp_cache.get(leg_key, 0) or self._ltp_cache.get("SPOT", 0)

        for zone in htf_zones:
            uid = _zone_uid(zone)
            if uid in self._notified_uids:
                continue
            if uid not in self._zone_ltf_status:
                self._zone_ltf_status[uid] = "watching"

            # Gate: price must be inside HTF zone [zone_trigger, zone_high].
            z_low  = zone["zone_low"]
            z_high = zone["zone_high"]
            zone_trigger = zone.get("zone_trigger", z_low + (z_high - z_low) / 3)
            if current_price > 0 and (current_price < zone_trigger or current_price > z_high):
                continue

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

    def _run_ltf_futures_mode(self, leg_key: str, bars: List[dict],
                               htf_zones: List[dict], opt_type: str) -> None:
        """
        Futures-mode LTF: scan FUTURES bars for 5-min traps (same price domain as HTF).
        CrudeOil institutions move via futures; option bars are illiquid/gappy.

        Both HTF and LTF use futures bars — zone prices are consistent.
        Normal scan_ltf zone-filter works because bars and zones share futures price units.

        Compared to option-mode _run_ltf_on:
          - No dual-confirm needed (LTF bars ARE the futures signal)
          - opt_type is always "CE" (bear trap → buy CE); PE logic TBD
          - tracking_leg in position is CE1/PE1 (option ticks for SL/T1/trail)
        """
        if not htf_zones or len(bars) < 3:
            return
        today = datetime.now(IST).date()
        today_bars = [b for b in bars
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return
        df = _bars_to_df(today_bars[-200:])
        current_spot = self._ltp_cache.get("FUT", 0) or self._ltp_cache.get("SPOT", 0)

        for zone in htf_zones:
            uid = _zone_uid(zone)
            if uid in self._notified_uids:
                continue
            if uid not in self._zone_ltf_status:
                self._zone_ltf_status[uid] = "watching"

            # Gate: price must be INSIDE the HTF zone [zone_low, zone_high].
            # Bulls entered at zone_high; their SL = zone_low.
            # 5-min scan starts at zone_trigger (1/3 from zone_low) and is valid up to zone_high.
            # If spot < zone_low → zone already broken, skip.
            # If spot > zone_high → price above the zone, no entry context, skip.
            z_low  = zone["zone_low"]
            z_high = zone["zone_high"]
            zone_trigger = zone.get("zone_trigger", z_low + (z_high - z_low) / 3)
            if current_spot > 0 and (current_spot < zone_trigger or current_spot > z_high):
                self._log.debug("zone %s skipped: spot=%.1f not in trigger zone [%.1f, %.1f]",
                                uid, current_spot, zone_trigger, z_high)
                continue

            _, ltf_entries = scanner.scan_ltf(
                df,
                htf_zone_high=zone["zone_high"],
                htf_zone_low=zone["zone_low"],
                htf_ref_bar=str(zone.get("ref_ts", "")),
                htf_trap_bar=str(zone.get("trapped_on", zone.get("closed_on", ""))),
                htf_target=zone.get("sl", 0.0),
            )
            best = scanner.select_best_ltf_entry(ltf_entries)
            if best:
                self._zone_ltf_status[uid] = "ltf_signal"
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg_key, opt_type, best, zone)
                )
                return

    async def _cascade_scan(self, ts: datetime,
                             cascade_ce: bool = True, cascade_pe: bool = True) -> None:
        """
        Intraday cascade: no 75-min zone TRAPPED (or zone too far).
        Per-leg for htf_source=option: cascade_ce/cascade_pe control which leg scans.
        1. Resample today's completed 1m bars to 15-min (drop current incomplete bar)
        2. scan_htf on those completed 15-min bars
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
            # Per-leg: only scan the leg(s) that are in cascade mode
            bear_15: list = []
            bull_15: list = []
            if cascade_ce:
                today_ce = _complete_today(self._bars_ce1)
                self._log.info("CE cascade scan: today_bars=%d", len(today_ce))
                if len(today_ce) >= 4:
                    df_ce = _bars_to_df(today_ce)
                    htf_ce = _resample_htf(df_ce, self._cascade_min)
                    if len(htf_ce) >= 2:
                        _, be = scanner.scan_htf(htf_ce)
                        bear_15 = [e for e in be if e["status"] == "TRAPPED"]
                        self._log.info("CE cascade: %d/%d zones TRAPPED from %d 15m candles",
                                       len(bear_15), len(be), len(htf_ce))
            if cascade_pe:
                today_pe = _complete_today(self._bars_pe1)
                self._log.info("PE cascade scan: today_bars=%d", len(today_pe))
                if len(today_pe) >= 4:
                    df_pe = _bars_to_df(today_pe)
                    htf_pe = _resample_htf(df_pe, self._cascade_min)
                    if len(htf_pe) >= 2:
                        _, bu = scanner.scan_htf(htf_pe)
                        bull_15 = [e for e in bu if e["status"] == "TRAPPED"]
                        self._log.info("PE cascade: %d/%d zones TRAPPED from %d 15m candles",
                                       len(bull_15), len(bu), len(htf_pe))
            if bear_15:
                self._run_ltf_on("CE1", self._bars_ce1, bear_15, "CE", require_closed=False)
                self._run_ltf_on("CE2", self._bars_ce2, bear_15, "CE", require_closed=False)
            if bull_15:
                self._run_ltf_on("PE1", self._bars_pe1, bull_15, "PE", require_closed=False)
                self._run_ltf_on("PE2", self._bars_pe2, bull_15, "PE", require_closed=False)
        else:  # "spot" cascade: spot 15-min for direction, option 15-min for entry zones
            today_spot = _complete_today(self._bars_spot)
            if len(today_spot) < 4:
                return
            df_spot = _bars_to_df(today_spot)
            htf_spot_15 = _resample_htf(df_spot, self._cascade_min)
            if len(htf_spot_15) < 2:
                return
            _, all_15 = scanner.scan_htf_spot(htf_spot_15)
            spot_bear = [e for e in all_15 if e.get("kind") == "BEAR" and e["status"] == "TRAPPED"]
            spot_bull = [e for e in all_15 if e.get("kind") == "BULL" and e["status"] == "TRAPPED"]
            self._log.info("spot cascade: bear=%d bull=%d from %d 15m candles",
                           len(spot_bear), len(spot_bull), len(htf_spot_15))

            # CE side: spot bear trap (bears squeezed → market up → CE rises → BUY CE)
            if spot_bear:
                today_ce = _complete_today(self._bars_ce1)
                if len(today_ce) >= 4:
                    df_ce = _bars_to_df(today_ce)
                    htf_ce_15 = _resample_htf(df_ce, self._cascade_min)
                    if len(htf_ce_15) >= 2:
                        _, ce_ents = scanner.scan_htf(htf_ce_15)
                        bear_opt_15 = [e for e in ce_ents if e["status"] == "TRAPPED"]
                        self._log.info("CE opt cascade: %d/%d zones TRAPPED from %d 15m candles ltp=%.1f",
                                       len(bear_opt_15), len(ce_ents), len(htf_ce_15),
                                       self._ltp_cache.get("CE1") or 0)
                        # Update live display so LEG shows today's zones not stale historical
                        if bear_opt_15:
                            self._opt_bear_zones = ce_ents  # all (incl non-trapped) for zone table
                            self._run_ltf_on("CE1", self._bars_ce1, bear_opt_15, "CE", require_closed=False)
                            self._run_ltf_on("CE2", self._bars_ce2, bear_opt_15, "CE", require_closed=False)
                        else:
                            self._log.info("CE opt cascade: no trapped zones — ltp=%.1f zones=%s",
                                           self._ltp_cache.get("CE1") or 0,
                                           [(round(e.get("zone_low",0),1), round(e.get("zone_high",0),1),
                                             e["status"]) for e in ce_ents[-5:]])

            # PE side: spot bull trap (bulls squeezed → market down → PE rises → BUY PE)
            if spot_bull:
                today_pe = _complete_today(self._bars_pe1)
                if len(today_pe) >= 4:
                    df_pe = _bars_to_df(today_pe)
                    htf_pe_15 = _resample_htf(df_pe, self._cascade_min)
                    if len(htf_pe_15) >= 2:
                        _, pe_ents = scanner.scan_htf(htf_pe_15)
                        bull_opt_15 = [e for e in pe_ents if e["status"] == "TRAPPED"]
                        self._log.info("PE opt cascade: %d/%d zones TRAPPED from %d 15m candles ltp=%.1f",
                                       len(bull_opt_15), len(pe_ents), len(htf_pe_15),
                                       self._ltp_cache.get("PE1") or 0)
                        if bull_opt_15:
                            self._opt_bull_zones = pe_ents  # update display
                            self._run_ltf_on("PE1", self._bars_pe1, bull_opt_15, "PE", require_closed=False)
                            self._run_ltf_on("PE2", self._bars_pe2, bull_opt_15, "PE", require_closed=False)
                        else:
                            self._log.info("PE opt cascade: no trapped zones — ltp=%.1f zones=%s",
                                           self._ltp_cache.get("PE1") or 0,
                                           [(round(e.get("zone_low",0),1), round(e.get("zone_high",0),1),
                                             e["status"]) for e in pe_ents[-5:]])

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

        # Scan strike (S1 CE / R1 PE) is naturally ITM relative to futures LTP
        scan_strike_map = {
            "CE1": self._ce1_strike, "CE2": self._ce2_strike,
            "PE1": self._pe1_strike, "PE2": self._pe2_strike,
            "FUT": self._ce1_strike if opt_type == "CE" else self._pe1_strike,
        }
        scan_strike = scan_strike_map.get(leg) or 0

        spot = self._spot_cache or self._spot_open
        atm  = _round_strike(spot, self._step)

        if self._htf_source == "futures":
            # CrudeOil/BTC/ETH: order goes to scan strike (S1 CE / R1 PE).
            # S1/R1 pivot strikes are naturally ITM — no separate 1-ITM computation needed.
            # Spread check: if scan strike too wide, fall back to ATM.
            primary_strike = scan_strike
            primary_key    = self._build_upstox_key(primary_strike, opt_type)
            atm_key        = self._build_upstox_key(atm, opt_type)
            max_spread_pct = float(self._admin_cfg.get("max_spread_pct", 3.0))
            strike, exec_key = await self._pick_liquid_strike(
                primary_strike, primary_key, atm, atm_key, opt_type, max_spread_pct
            )
        else:
            # Sensex/Nifty: 1-ITM option is primary; ATM as fallback if spread too wide.
            # tracked_sym (scan_key) = SCAN STRIKE option key — never changes, even if
            # exec_key falls back to ATM. SL/T1 are always on the scan strike option LTP.
            if opt_type == "CE":
                primary_1itm = atm - self._step
            elif opt_type == "PE":
                primary_1itm = atm + self._step
            else:
                primary_1itm = scan_strike
            primary_key    = self._build_upstox_key(primary_1itm, opt_type)
            atm_key        = self._build_upstox_key(atm, opt_type)
            max_spread_pct = float(self._admin_cfg.get("max_spread_pct", 3.0))
            strike, exec_key = await self._pick_liquid_strike(
                primary_1itm, primary_key, atm, atm_key, opt_type, max_spread_pct
            )

        ep       = round(entry.get("zone_trigger", entry.get("zone_high", 0)), 2)
        total_qty = self._lot_size * self._lot_mul
        t1_qty    = total_qty // 2

        # Price domain for SL/T1/monitoring depends on htf_source:
        #
        # futures-mode (CrudeOil/BTC/ETH):
        #   Signal from FUTURES bars → SL/T1 also in FUTURES ₹ (same chart).
        #   pos["leg"]="FUT" → _idx_tick_loop drives _check_tick_exit with futures LTP.
        #   Order close goes to exec_key (scan strike option or ATM fallback) via _place_exit.
        #   scan_key = futures key (tracked_sym fixed; never the option key).
        #
        # option-mode (Sensex/Nifty):
        #   Signal from SCAN STRIKE option bars → SL/T1 in OPTION ₹.
        #   pos["leg"]=CE1/PE1 → _opt_tick_loop drives _check_tick_exit with option LTP.
        #   scan_key = scan strike option key (tracked_sym fixed; NOT the exec/1-ITM key).
        if self._htf_source == "futures":
            tracking_leg = "FUT"
            # CE (bear trap): SL = floor below zone → exit if FUT drops to sl (ltp <= sl)
            # PE (bull trap): SL = ceiling above zone → exit if FUT rises to sl (ltp >= sl)
            if opt_type == "CE":
                sl_price = round(entry["zone_low"]  - self._sl_buf, 2)
            else:
                sl_price = round(entry["zone_high"] + self._sl_buf, 2)
            # T1 = option chart HTF: latest TRAPPED bear zone sl on CE1/PE1 bars.
            # Bears shorted the option → their SL (ref bar HIGH on option chart) = our T1.
            # Checked against option ltp (not futures) in _opt_tick_loop.
            opt_bars = self._bars_ce1 if opt_type == "CE" else self._bars_pe1
            t1_price     = self._compute_option_t1(opt_bars)
            t1_price_fut = round(htf_zone.get("sl", 0), 2)  # kept for logging/UI reference
        else:
            tracking_leg = leg   # CE1 or PE1 — scan strike option bars
            sl_price     = round(entry["zone_low"] - self._sl_buf, 2)  # option zone_low (option ₹)
            t1_price     = round(htf_zone.get("sl", 0), 2)             # HTF ref bar HIGH (bears' SL)
            t1_price_fut = None

        self._log.info(
            "ENTRY %s scan_strike=%d order_strike=%d%s spot=%.2f atm=%d "
            "ep=%.2f sl=%.2f t1=%.2f qty=%d tracking=%s exec_key=%s",
            self._und, scan_strike, strike, opt_type, spot, atm,
            ep, sl_price, t1_price, total_qty, tracking_leg, exec_key,
        )

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

        # scan_key = the key used for SL/T1 monitoring ticks.
        # futures-mode: tracking_leg="FUT" → futures key (SL/T1 in futures ₹).
        # option-mode:  tracking_leg=CE1/PE1 → that option's Upstox key.
        scan_key = {
            "CE1": self._ce1_key, "CE2": self._ce2_key,
            "PE1": self._pe1_key, "PE2": self._pe2_key,
            "FUT": self._fut_key,
        }.get(tracking_leg, self._fut_key if self._htf_source == "futures" else "")
        self._position = {
            "leg":            tracking_leg,  # FUT for futures-mode (futures ticks drive SL/T1)
            "signal_leg":     leg,           # original detection leg (FUT for CrudeOil)
            "side":           opt_type,
            "strike":         strike,        # exec strike (scan strike or ATM fallback)
            "scan_strike":    scan_strike,   # pivot strike (S1/R1) used for zone detection
            "spot_at_entry":  round(spot, 2),
            "exec_key":       exec_key,      # Upstox key for 1-ITM contract
            "scan_key":       scan_key,      # Upstox key for tracking leg (SL monitoring)
            "entry_price":    round(avg, 2),
            "sl_price":       sl_price,
            "trail_sl":       sl_price,      # steps up via trap-based trail after T1
            "last_5m_ts":     None,
            # Trap-based trail state: bears trapped above entry → squeezed → pullback → confirmed
            # Each entry: {zone_trigger, zone_high, state: WATCHING|SQUEEZED|PULLED_BACK|CONFIRMED}
            "trail_traps":    [],
            "t1_price":       t1_price,      # futures ₹ for futures-mode; option ₹ for option-mode
            "t1_price_fut":   t1_price_fut,  # unused (kept for backward compat with persisted state)
            "total_qty":      total_qty,
            "t1_qty":         t1_qty,
            "remaining_qty":  total_qty,
            "t1_hit":         False,
            "entry_ts":       now.isoformat(),
            "signal_source":  f"HTF zone {_zone_uid(htf_zone)} → LTF {leg}",
            "order_id_entry": order_id,
            "order_id_t1":    None,
            "htf_zone":       htf_zone,
            "opt_type":       opt_type,
        }
        self._persist_position()
        self._log.info(
            "ENTRY PLACED scan=%d exec=%d%s spot=%.2f fill=%.2f sl=%.2f t1=%.2f order=%s",
            scan_strike, strike, opt_type, spot, avg, sl_price, t1_price, order_id,
        )

    # ── Tick exit ─────────────────────────────────────────────────────────────

    async def _check_sweep_reentry(self, ltp: float) -> None:
        """
        After a plain SL hit, watch for liquidity sweep: price recovers above sl_level
        within 2 ticks/candles → re-enter same zone (bears swept longs, now reversing).
        """
        sw = self._sweep_watch
        if not sw or self._position:
            return
        if ltp > sw["sl_level"]:
            self._log.info(
                "SWEEP REENTRY: ltp=%.2f recovered above sl=%.2f → re-entering %s",
                ltp, sw["sl_level"], sw["leg"],
            )
            self._sweep_watch = None
            # Re-fire entry signal using stored zone — notified_uid already consumed,
            # so create a synthetic entry dict at current ltp.
            zone = sw.get("entry_zone", {})
            synth_entry = {
                "entry": ltp, "zone_low": zone.get("zone_low", ltp - 5),
                "zone_high": zone.get("zone_high", ltp + 5),
                "status": "SWEEP", "closed_on": None, "trapped_on": None,
            }
            await self._on_entry_signal(sw["leg"], sw["opt_type"], synth_entry, zone)
        else:
            sw["candles_left"] -= 1
            if sw["candles_left"] <= 0:
                self._log.info("Sweep watch expired — no recovery above %.2f", sw["sl_level"])
                self._sweep_watch = None

    async def _check_option_t1(self, opt_ltp: float, ts: Optional[datetime] = None) -> None:
        """Futures-mode T1 check against option ltp (CE1/PE1). SL stays in futures domain."""
        pos = self._position
        if not pos or pos.get("t1_hit") or not pos.get("t1_price", 0):
            return
        if opt_ltp >= pos["t1_price"]:
            pos["t1_hit"] = True
            pos["remaining_qty"] -= pos["t1_qty"]
            # CTC trail_sl = spot_at_entry (futures domain, not option fill price)
            pos["trail_sl"] = pos.get("spot_at_entry", pos["sl_price"])
            self._log.info("T1 HIT (option) opt_ltp=%.2f t1=%.2f qty=%d → trail_sl=%.2f (CTC futures)",
                           opt_ltp, pos["t1_price"], pos["t1_qty"], pos["trail_sl"])
            oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
            pos["order_id_t1"] = oid
            self._record_closed_trade(pos, exit_price=opt_ltp, exit_reason="T1", qty_override=pos["t1_qty"])
            self._persist_position()

    async def _check_tick_exit(self, ltp: float, ts: Optional[datetime] = None) -> None:
        pos = self._position
        if not pos:
            await self._check_sweep_reentry(ltp)
            return

        # T1: 50% at HTF target (option-mode only; futures-mode T1 handled in _check_option_t1)
        if not pos["t1_hit"] and ltp >= pos["t1_price"] and self._htf_source != "futures":
            pos["t1_hit"] = True
            pos["remaining_qty"] -= pos["t1_qty"]
            # CTC: futures-mode uses spot_at_entry; option-mode uses entry_price
            pos["trail_sl"] = pos.get("spot_at_entry", pos["entry_price"])
            self._log.info("T1 HIT ltp=%.2f t1=%.2f qty=%d → trail_sl reset to CTC %.2f",
                           ltp, pos["t1_price"], pos["t1_qty"], pos["entry_price"])
            oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
            pos["order_id_t1"] = oid
            self._record_closed_trade(pos, exit_price=ltp, exit_reason="T1", qty_override=pos["t1_qty"])
            self._persist_position()

        # Advance 5m trail SL using OPTION bar lows (only after T1)
        if pos["t1_hit"] and ts is not None:
            self._update_trail_sl(pos, ts)

        # Exit check — direction-aware:
        # CE (futures): sl is floor → exit when FUT drops to/below sl
        # PE (futures): sl is ceiling → exit when FUT rises to/above sl
        # Option-mode (NIFTY/SENSEX): always buyers → option LTP drops → ltp <= sl
        active_sl = pos["trail_sl"] if pos["t1_hit"] else pos["sl_price"]
        is_pe_fut = (self._htf_source == "futures" and pos.get("opt_type") == "PE")
        sl_hit = (ltp >= active_sl) if is_pe_fut else (ltp <= active_sl)
        if sl_hit:
            remaining = pos["remaining_qty"]
            reason = "TRAIL_SL" if pos["t1_hit"] else "SL"
            self._log.info("%s ltp=%.2f sl=%.2f qty=%d", reason, ltp, active_sl, remaining)
            await self._place_exit(remaining, active_sl, reason)
            self._record_closed_trade(pos, exit_price=ltp, exit_reason=reason)
            # Liquidity sweep watch: only on plain SL (not trail), not after T1.
            # If price recovers above SL within 2 candles → sweep → re-enter same zone.
            if not pos["t1_hit"]:
                self._sweep_watch = {
                    "opt_type":   pos.get("opt_type", "CE"),
                    "leg":        pos["leg"],
                    "sl_level":   active_sl,
                    "entry_zone": pos.get("htf_zone", {}),
                    "qty":        remaining,
                    "candles_left": 2,
                    "orig_entry": pos["entry_price"],
                }
                self._log.info("SL hit — watching for liquidity sweep re-entry above %.2f", active_sl)
            self._position = None
            self._clear_persisted_position()

    def _update_trail_sl(self, pos: dict, ts: datetime) -> None:
        """
        Trap-based trail SL on SCAN-STRIKE option bars. Only active after T1.

        Logic (per new 5-min bar close):
          1. Scan latest option bars for bear traps that formed ABOVE our entry
          2. Register new traps as WATCHING
          3. State machine per trap:
             WATCHING    → hi >= zone_high (bears' SL hit = bears squeezed)     → SQUEEZED
             SQUEEZED    → lo <= zone_trigger (price pulls back to bears' entry) → PULLED_BACK
             PULLED_BACK → hi >= zone_high again (confirmed support held)        → CONFIRMED
             CONFIRMED   → trail_sl steps up to zone_trigger − sl_buf (bears' entry − buffer)
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

        # Build 5-min bars from raw 1-min bars for the scan
        import pandas as pd
        df1 = pd.DataFrame(bars)
        if df1.empty or "datetime" not in df1.columns:
            return
        df1["datetime"] = pd.to_datetime(df1["datetime"])
        df1 = df1.set_index("datetime")
        df5 = df1.resample("5min").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna().reset_index()

        if len(df5) < 3:
            return

        entry_price = pos["entry_price"]
        entry_ts    = datetime.fromisoformat(pos["entry_ts"])
        trail_traps = pos.setdefault("trail_traps", [])

        # Scan 5-min option bars for new bear traps above entry
        from strategies.trap_scanner import scanner as _sc
        try:
            _, all_traps = _sc.scan_ltf(df5, df5["high"].max(), df5["low"].min())
        except Exception:
            return

        # Register new traps formed after entry and above entry price
        known_keys = {t["key"] for t in trail_traps}
        for trap in all_traps:
            if trap.get("status") != "TRAPPED":
                continue
            trap_ts = pd.to_datetime(trap.get("trapped_on") or trap.get("ref_ts"))
            if trap_ts is None:
                continue
            trap_ts_dt = trap_ts.to_pydatetime().replace(tzinfo=None)
            if trap_ts_dt <= entry_ts:
                continue
            zt = trap.get("zone_trigger", 0)
            zh = trap.get("zone_high", 0)
            if zt <= entry_price or zh <= zt:
                continue
            key = trap_ts_dt.strftime("%H%M")
            if key not in known_keys:
                trail_traps.append({"key": key, "zone_trigger": zt,
                                    "zone_high": zh, "state": "WATCHING"})
                self._log.debug("TRAIL trap registered key=%s zt=%.2f zh=%.2f", key, zt, zh)

        # Get current bar hi/lo for state advances
        prev_start = bar_5m - timedelta(minutes=5)
        bucket = [b for b in bars[-15:]
                  if prev_start <= datetime.fromisoformat(b["datetime"]) < bar_5m]
        if not bucket:
            return
        bar_hi = max(b["high"] for b in bucket)
        bar_lo = min(b["low"]  for b in bucket)

        changed = False
        for trap in trail_traps:
            if trap["state"] == "CONFIRMED":
                continue
            zh, zt = trap["zone_high"], trap["zone_trigger"]
            if trap["state"] == "WATCHING":
                if bar_hi >= zh:
                    trap["state"] = "SQUEEZED"
                    self._log.info("TRAIL SQUEEZED zt=%.2f zh=%.2f bar_hi=%.2f",
                                   zt, zh, bar_hi)
            elif trap["state"] == "SQUEEZED":
                if bar_lo <= zt:
                    trap["state"] = "PULLED_BACK"
                    self._log.info("TRAIL PULLED_BACK zt=%.2f bar_lo=%.2f", zt, bar_lo)
            elif trap["state"] == "PULLED_BACK":
                if bar_hi >= zh:
                    trap["state"] = "CONFIRMED"
                    # CE (futures / option-mode): sl is floor → step UP
                    # PE (futures): sl is ceiling → step DOWN (zone_trigger + buf moves ceiling down)
                    is_pe_fut = (self._htf_source == "futures"
                                 and pos.get("opt_type") == "PE")
                    if is_pe_fut:
                        new_sl = round(zt + self._sl_buf, 2)
                        better = new_sl < pos["trail_sl"]
                        direction = "DOWN"
                    else:
                        new_sl = round(zt - self._sl_buf, 2)
                        better = new_sl > pos["trail_sl"]
                        direction = "UP"
                    if better:
                        old = pos["trail_sl"]
                        pos["trail_sl"] = new_sl
                        changed = True
                        self._log.info("TRAIL_SL STEP %s %.2f -> %.2f "
                                       "(zt=%.2f confirmed, buf=%.2f, zh=%.2f)",
                                       direction, old, new_sl, zt, self._sl_buf, zh)

        if changed:
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
            eod_ltp = self._ltp_cache.get(pos["leg"], 0.0)
            await self._place_exit(pos["remaining_qty"], 0.0, "EOD")
            self._record_closed_trade(pos, exit_price=eod_ltp, exit_reason="EOD")
        self._position = None
        self._clear_persisted_position()
        # Unsubscribe all option keys from the live feeder after market close so
        # those WS slots are freed up (important when running NIFTY + SENSEX + CrudeOil
        # on a single account — NSE/BSE slots released before CrudeOil session starts).
        await self._unsubscribe_all_legs()

    async def _unsubscribe_all_legs(self) -> None:
        """Release all pinned/subscribed option keys from the feeder after EOD."""
        keys = [k for k in [self._ce1_key, self._ce2_key,
                             self._pe1_key, self._pe2_key] if k]
        if not keys:
            return
        feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
        if feeder and hasattr(feeder, "unsubscribe_tokens"):
            try:
                await feeder.unsubscribe_tokens(keys)
                self._log.info("EOD: unsubscribed %d option keys for %s: %s",
                               len(keys), self._und, keys)
            except Exception as exc:
                self._log.warning("EOD unsubscribe failed: %s", exc)
        # Also unpin strikes so rebalancer doesn't re-subscribe them tomorrow at wrong prices
        if self._rebalancer is not None:
            for strike in [self._ce1_strike, self._ce2_strike,
                           self._pe1_strike, self._pe2_strike]:
                if strike:
                    try:
                        self._rebalancer.unpin_strike(self._und, float(strike))
                    except Exception:
                        pass

    def _reset_day_state(self) -> None:
        self._initialized      = False
        self._intraday_mode    = False
        self._intraday_mode_ce = False
        self._intraday_mode_pe = False
        self._sweep_watch      = None
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
            import pandas as pd

            def _default(obj):
                if isinstance(obj, (pd.Timestamp,)):
                    return str(obj)
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                raise TypeError(f"Not serializable: {type(obj)}")

            with open(self._position_file(), "w") as f:
                json.dump(self._position, f, default=_default)
        except Exception as exc:
            self._log.warning("_persist_position failed: %s", exc)

    def _update_futures_tsl(self, ts: datetime) -> None:
        """
        Futures-mode TSL: on each 5-min FUT candle close while in a CE/PE position,
        scan for new TRAPPED bear zones ABOVE (CE) or below (PE) the entry spot.

        TSL step 1: first new zone above entry → trail_sl = spot_at_entry (CTC)
        TSL step 2: each subsequent zone → trail_sl = new zone_low - sl_buf (trails up)

        Only advances trail_sl — never moves it back down.
        Only runs before T1 is hit (after T1, trail is handled by _update_trail_sl).
        """
        pos = self._position
        if not pos or pos.get("t1_hit"):
            return

        today = datetime.now(IST).date()
        today_bars = [b for b in self._bars_fut
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return

        df  = _bars_to_df(today_bars[-100:])
        opt_type      = pos.get("opt_type", "CE")
        spot_at_entry = pos.get("spot_at_entry", pos.get("ep", 0))
        current_sl    = pos.get("trail_sl", pos.get("sl_price", 0))
        orig_sl       = pos.get("sl_price", 0)

        _, entries = scanner.scan_htf(df)
        trapped = [e for e in entries if e["status"] == "TRAPPED"]

        if opt_type == "CE":
            # New bear traps ABOVE entry spot confirm upward momentum
            above = [e for e in trapped if e.get("zone_high", 0) > spot_at_entry]
            if not above:
                return
            # Best zone = highest zone_low above entry (most conservative trail)
            best = max(above, key=lambda e: e.get("zone_low", 0))
            new_sl = round(best["zone_low"] - self._sl_buf, 2)

            if current_sl < spot_at_entry:
                # Step 1: advance to CTC (break-even) first
                new_sl = max(new_sl, spot_at_entry)
            # Only advance, never retreat
            if new_sl > current_sl:
                self._log.info(
                    "TSL advance (CE): trail_sl %.2f → %.2f (zone_low=%.2f above entry=%.2f)",
                    current_sl, new_sl, best["zone_low"], spot_at_entry,
                )
                pos["trail_sl"] = new_sl
                self._persist_position()
        else:
            # PE: new bull traps BELOW entry spot
            below = [e for e in trapped if e.get("zone_low", 0) < spot_at_entry]
            if not below:
                return
            best  = min(below, key=lambda e: e.get("zone_high", float("inf")))
            new_sl = round(best["zone_high"] + self._sl_buf, 2)
            if current_sl > spot_at_entry:
                new_sl = min(new_sl, spot_at_entry)
            if new_sl < current_sl:
                self._log.info(
                    "TSL advance (PE): trail_sl %.2f → %.2f (zone_high=%.2f below entry=%.2f)",
                    current_sl, new_sl, best["zone_high"], spot_at_entry,
                )
                pos["trail_sl"] = new_sl
                self._persist_position()

    def _compute_option_t1(self, opt_bars: list) -> float:
        """T1 = latest TRAPPED bear zone sl on the option chart (ref bar HIGH = bears' SL)."""
        try:
            if not opt_bars or len(opt_bars) < 3:
                return 0.0
            df  = _bars_to_df(opt_bars[-200:])
            htf = _resample_htf(df, self._htf_min)
            if len(htf) < 2:
                return 0.0
            _, entries = scanner.scan_htf(htf)
            trapped = [e for e in entries if e["status"] == "TRAPPED"]
            if not trapped:
                return 0.0
            return round(trapped[-1]["sl"], 2)   # most recent trapped zone's ref bar HIGH
        except Exception as exc:
            self._log.warning("_compute_option_t1 failed: %s", exc)
            return 0.0

    def _record_closed_trade(self, pos: dict, exit_price: float,
                             exit_reason: str, qty_override: int = 0) -> None:
        """Write one closed-trade record to data/history and logs/trades."""
        try:
            from data_layer.trade_history import record as _hist_record
            qty   = qty_override or pos.get("remaining_qty", 0)
            ep    = float(pos.get("entry_price", 0))
            xp    = float(exit_price)
            pnl   = round((xp - ep) * qty, 2)
            _hist_record(
                client_id   = self._cid,
                strategy    = "trap_scanner",
                instrument  = f"{self._und} {pos.get('side','')} {pos.get('strike','')}",
                entry_price = ep,
                exit_price  = xp,
                exit_reason = exit_reason,
                pnl         = pnl,
                binding_id  = self._bid,
                ts          = datetime.now(IST).isoformat(timespec="seconds"),
                legs        = [{
                    "side":       pos.get("side", ""),
                    "strike":     pos.get("strike", 0),
                    "entry":      ep,
                    "exit":       xp,
                    "pnl":        pnl,
                    "entry_ts":   pos.get("entry_ts", ""),
                    "exit_ts":    datetime.now(IST).isoformat(timespec="seconds"),
                    "entry_reason": pos.get("signal_source", "HTF"),
                }],
            )
            # Also append to logs/trades/ (mirrors straddle format)
            os.makedirs(os.path.join("logs", "trades"), exist_ok=True)
            trade_file = os.path.join("logs", "trades",
                f"ts_{self._und}_{self._cid}_{datetime.now(IST).strftime('%Y%m%d')}.log")
            with open(trade_file, "a") as f:
                f.write(
                    f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"{pos.get('side')} {pos.get('strike')} | "
                    f"entry={ep} exit={xp:.2f} qty={qty} pnl={pnl:+.2f} reason={exit_reason} "
                    f"spot@entry={pos.get('spot_at_entry','?')}\n"
                )
            self._log.info("trade recorded: %s %s entry=%.2f exit=%.2f pnl=%+.2f reason=%s",
                           pos.get('side'), pos.get('strike'), ep, xp, pnl, exit_reason)
        except Exception as exc:
            self._log.warning("_record_closed_trade failed: %s", exc)

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
            if not spot_key and self._htf_source == "futures":
                # Futures-mode (CrudeOil, BTC, ETH): use REGISTRY futures key for daily OHLC
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    spot_key = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    pass
            if not spot_key:
                self._log.warning("_fetch_prev_day_ohlc: no spot key for %s", self._und)
                return None
            import aiohttp
            from urllib.parse import quote as _quote
            today   = date.today()
            fr_date = today - timedelta(days=10)
            encoded_key = _quote(spot_key, safe="")
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{encoded_key}/day/{today}/{fr_date}")
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
            if not candles:
                self._log.warning(
                    "_fetch_prev_day_ohlc: no candles returned for %s", spot_key,
                )
                return None
            # Upstox returns newest-first. During market hours today's daily candle
            # is NOT included → candles[0] = yesterday. After market close candles[0]
            # = today (partial/closed) → candles[1] = yesterday.
            # Detect by comparing candle date to today.
            first_date = str(candles[0][0])[:10]
            if first_date == str(today):
                if len(candles) < 2:
                    self._log.warning("_fetch_prev_day_ohlc: only today's candle for %s", spot_key)
                    return None
                prev = candles[1]
            else:
                prev = candles[0]
            self._log.info("_fetch_prev_day_ohlc(%s): prev=%s H=%.2f L=%.2f C=%.2f",
                           spot_key, str(prev[0])[:10], float(prev[2]), float(prev[3]), float(prev[4]))
            return {"open": float(prev[1]), "high": float(prev[2]),
                    "low":  float(prev[3]), "close": float(prev[4])}
        except Exception as exc:
            self._log.warning("_fetch_prev_day_ohlc: %s", exc)
            return None

    async def _fetch_today_open(self) -> float:
        """Return today's OPENING price (first bar of the session), NOT the current live price."""
        try:
            token = self._get_upstox_token()
            if not token:
                return 0.0
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key and self._htf_source == "futures":
                # Futures-mode (CrudeOil, BTC, ETH): use REGISTRY futures key
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    spot_key = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    pass
            if not spot_key:
                return 0.0
            import aiohttp
            from urllib.parse import quote
            encoded_key = quote(spot_key, safe="")
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{encoded_key}/1minute"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return 0.0
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            if not candles:
                return 0.0
            # Upstox returns candles newest-first; candles[-1] = oldest = first bar of the day
            today_open = float(candles[-1][1])
            self._log.info("_fetch_today_open(%s): first_bar_open=%.2f (from %d candles)",
                           spot_key, today_open, len(candles))
            return today_open
        except Exception as exc:
            self._log.warning("_fetch_today_open: %s", exc)
            return 0.0

    async def _fetch_prev_close_and_today_open_from_1m(self, fut_key: str) -> tuple:
        """
        For futures-mode (CrudeOil/BTC/ETH): return (prev_close, today_open).

        prev_close: historical 1m endpoint (ends yesterday) — same active contract,
          avoids daily-candle API returning an EXPIRED contract's close (e.g. June
          close when July contract is active → false 5.9% gap).
        today_open: intraday endpoint via _fetch_today_open() — MCX historical
          endpoint excludes today's session, intraday endpoint has today's bars.

        Returns (prev_close, today_open) — (0.0, 0.0) on failure.
        """
        try:
            # Historical bars → last bar of yesterday = prev_close (same active contract)
            # MCX historical endpoint excludes today's session — that's expected and correct
            bars = await self._fetch_1m_history(fut_key)
            if not bars:
                return 0.0, 0.0
            today_str  = date.today().isoformat()
            prev_bars  = [b for b in bars if b["datetime"][:10] < today_str]
            prev_close = float(prev_bars[-1]["close"]) if prev_bars else 0.0

            # Intraday endpoint → first bar of today's session = today_open
            # _fetch_today_open() uses REGISTRY for CrudeOil/BTC/ETH (no _SPOT_KEYS entry)
            # and returns candles[-1][1] = oldest (9:00 AM) bar's open = true market open
            today_open = await self._fetch_today_open()

            return prev_close, today_open
        except Exception as exc:
            self._log.warning("_fetch_prev_close_and_today_open_from_1m: %s", exc)
            return 0.0, 0.0

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
                        body = await r.text()
                        self._log.warning(
                            "_fetch_1m_history(%s): HTTP %d — %s", instrument_key, r.status, body[:200]
                        )
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            bars = [
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
                for c in reversed(candles)  # oldest first
            ]
            self._log.info("_fetch_1m_history(%s): %d bars (%s → %s)",
                           instrument_key, len(bars),
                           bars[0]["datetime"][:10] if bars else "—",
                           bars[-1]["datetime"][:10] if bars else "—")
            return bars
        except Exception as exc:
            self._log.warning("_fetch_1m_history(%s): %s", instrument_key, exc)
            return []

    async def _pick_liquid_strike(
        self,
        primary_strike: int, primary_key: str,
        atm_strike: int,    atm_key: str,
        opt_type: str, max_spread_pct: float
    ) -> tuple:
        """
        Check bid-ask spread on primary (scan) strike; fall back to ATM if too wide.
        Returns (strike, upstox_key) for the chosen exec strike.

        For futures-mode (CrudeOil): primary = scan strike (S1 CE / R1 PE).
        For option-mode (Sensex/Nifty): primary = 1-ITM option.
        """
        async def _spread_pct(key: str) -> float:
            try:
                import aiohttp
                token = self._get_upstox_token()
                if not token:
                    return 0.0
                url = (f"https://api.upstox.com/v2/market-quote/quotes"
                       f"?instrument_key={key.replace('|', '%7C')}")
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url, headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=3)) as r:
                        if r.status != 200:
                            return 0.0
                        data = await r.json()
                quotes = data.get("data", {})
                quote = list(quotes.values())[0] if quotes else {}
                depth = quote.get("depth", {})
                bid = (depth.get("buy") or [{}])[0].get("price", 0)
                ask = (depth.get("sell") or [{}])[0].get("price", 0)
                if not bid or not ask:
                    return 0.0
                mid = (bid + ask) / 2
                return round((ask - bid) / mid * 100, 2) if mid > 0 else 0.0
            except Exception as exc:
                self._log.warning("spread check %s: %s", key, exc)
                return 0.0

        sp = await _spread_pct(primary_key)
        if sp == 0.0 or sp <= max_spread_pct:
            if sp > 0:
                self._log.info("spread OK: %s%s spread=%.1f%%", primary_strike, opt_type, sp)
            return primary_strike, primary_key

        self._log.warning(
            "spread too wide: %s%s spread=%.1f%% > %.1f%% — trying ATM %s",
            primary_strike, opt_type, sp, max_spread_pct, atm_strike,
        )
        sp_atm = await _spread_pct(atm_key)
        if sp_atm == 0.0 or sp_atm <= max_spread_pct:
            self._log.info("ATM fallback: %s%s spread=%.1f%%", atm_strike, opt_type, sp_atm)
            return atm_strike, atm_key

        self._log.warning(
            "ATM also too wide: %s%s spread=%.1f%% — using scan strike anyway",
            atm_strike, opt_type, sp_atm,
        )
        return primary_strike, primary_key  # last resort: place anyway

    def _get_upstox_token(self) -> Optional[str]:
        # CrudeOil uses a dedicated second Upstox account (upstox2) for MCX data.
        # Falls back to the primary "upstox" account if upstox2 has no token yet.
        if self._und == "CRUDEOIL":
            creds2 = self._db.get_feeder_creds_sync("upstox2")
            token2 = (creds2 or {}).get("access_token") or ""
            if token2:
                return token2
        creds = self._db.get_feeder_creds_sync("upstox")
        return (creds or {}).get("access_token") or ""

    async def _get_expiry(self) -> tuple:
        """Returns (expiry_str, expiry_date). expiry_str = e.g. '18JUN26', expiry_date = date obj.

        Primary: asks REGISTRY for the nearest loaded expiry — this is always correct because
        it comes from the actual Upstox master JSON (BSE_FO stores epochs, not symbol strings,
        so hardcoded weekday math was getting the right calendar date but the REGISTRY had a
        different date key due to timezone offset in the BSE epoch).

        Fallback: weekday math, used only if REGISTRY is not yet loaded.
        """
        try:
            from data_layer.instrument_registry import REGISTRY
            if REGISTRY.is_loaded(self._und):
                exp_date = REGISTRY.get_active_expiry(self._und)
                if exp_date is not None:
                    exp_str = exp_date.strftime("%d%b%y").upper()
                    self._log.info("_get_expiry %s → REGISTRY: %s (%s)", self._und, exp_str, exp_date)
                    return exp_str, exp_date
                self._log.warning("_get_expiry %s → REGISTRY loaded but no active expiry found", self._und)
        except Exception as exc:
            self._log.warning("_get_expiry REGISTRY lookup failed: %s", exc)
        # Fallback: weekday math (works for NSE; BSE/MCX may differ — prefer REGISTRY)
        _EXPIRY_DOW = {
            "NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
            "SENSEX": 3, "MIDCPNIFTY": 1,  # SENSEX = Thursday (verified from BSE master 2026-06-17)
        }
        if self._und == "CRUDEOIL":
            # MCX CrudeOil options expire on 20th (or nearest preceding Monday if 20th is weekend)
            today = date.today()
            d = date(today.year, today.month, 20)
            while d.weekday() > 4:   # weekend → back to Friday (but MCX uses Monday; keep simple)
                d -= timedelta(days=1)
            if d < today:   # 20th already passed this month → next month
                import calendar as _cal
                nm = today.month + 1 if today.month < 12 else 1
                ny = today.year if today.month < 12 else today.year + 1
                d = date(ny, nm, 20)
                while d.weekday() > 4:
                    d -= timedelta(days=1)
            self._log.warning("_get_expiry CRUDEOIL → REGISTRY unavailable; date-20 fallback: %s", d)
            return d.strftime("%d%b%y").upper(), d
        weekday = _EXPIRY_DOW.get(self._und, 3)
        d = date.today()
        for _ in range(7):
            if d.weekday() == weekday:
                self._log.warning("_get_expiry %s → REGISTRY unavailable; weekday fallback: %s", self._und, d)
                return d.strftime("%d%b%y").upper(), d
            d += timedelta(days=1)
        return None, None

    async def _subscribe_instruments(self) -> None:
        """Pin + force-subscribe all tracked option keys so ticks arrive regardless of ATM window."""
        # Step 1: pin strikes so rebalancer never unsubscribes them on ATM drift
        if self._rebalancer is not None:
            for strike in [self._ce1_strike, self._ce2_strike, self._pe1_strike, self._pe2_strike]:
                if strike:
                    try:
                        self._rebalancer.pin_strike(self._und, float(strike))
                    except Exception as exc:
                        self._log.warning("pin_strike %s %s: %s", self._und, strike, exc)
        else:
            self._log.warning("No rebalancer set — falling back to direct feeder subscription only")

        # Step 2: force-subscribe the specific instrument keys directly via feeder.
        # pin_strike alone only prevents UNsubscription; it does NOT subscribe a key that
        # was never in the ATM window.  Deep-ITM / OTM legs used by TrapScanner are often
        # outside the ±N-strike window, so they never receive ticks without this call.
        keys = [k for k in [self._ce1_key, self._ce2_key,
                             self._pe1_key, self._pe2_key] if k]
        if keys:
            # MCX indices (CrudeOil) use dedicated Upstox2 feeder; others use main feeder
            if self._mcx_feeder is not None:
                feeder = self._mcx_feeder
            else:
                feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
            if feeder and hasattr(feeder, "subscribe_tokens"):
                try:
                    await feeder.subscribe_tokens(keys)
                    self._log.info("force-subscribed %d option keys via %s: %s",
                                   len(keys), type(feeder).__name__, keys)
                except Exception as exc:
                    self._log.warning("feeder.subscribe_tokens failed: %s", exc)
            else:
                self._log.warning("No feeder accessible — option ticks depend on ATM window")

        self._log.info(
            "pinned CE1=%s(%s) CE2=%s(%s) PE1=%s(%s) PE2=%s(%s) for %s",
            self._ce1_strike, self._ce1_key,
            self._ce2_strike, self._ce2_key,
            self._pe1_strike, self._pe1_key,
            self._pe2_strike, self._pe2_key,
            self._und,
        )

    def _build_upstox_key(self, strike: Optional[int], opt_type: str) -> str:
        if not strike:
            return ""
        exp = self._expiry_str or ""
        # Try global REGISTRY first — BSE_FO requires a numeric token (not symbol format).
        # REGISTRY is pre-loaded by the rebalancer at startup; if loaded it has correct keys.
        try:
            from data_layer.instrument_registry import REGISTRY
            reg_loaded = REGISTRY.is_loaded(self._und)
            if self._expiry_date is not None and reg_loaded:
                key = REGISTRY.get_upstox_key(self._und, self._expiry_date, int(strike), opt_type)
                if key:
                    self._log.debug(
                        "_build_upstox_key %s %s%s → REGISTRY: %s", self._und, strike, opt_type, key
                    )
                    return key
                self._log.warning(
                    "_build_upstox_key %s %s%s → REGISTRY loaded but strike NOT found (expiry=%s)",
                    self._und, strike, opt_type, self._expiry_date,
                )
            elif not reg_loaded:
                self._log.warning(
                    "_build_upstox_key %s %s%s → REGISTRY NOT loaded for %s; using fallback key",
                    self._und, strike, opt_type, self._und,
                )
        except Exception as exc:
            self._log.warning("_build_upstox_key REGISTRY lookup failed: %s", exc)
        # Fallback: constructed symbol (works for NSE_FO; BSE_FO may return empty from REST)
        _PFX = {
            "NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "FINNIFTY": "NSE_FO|", "SENSEX": "BSE_FO|", "MIDCPNIFTY": "NSE_FO|",
            "CRUDEOIL": "MCX_FO|",
        }
        pfx = _PFX.get(self._und, "NSE_FO|")
        fallback = f"{pfx}{self._und}{exp}{strike}{opt_type}"
        self._log.warning("_build_upstox_key %s %s%s → FALLBACK key: %s", self._und, strike, opt_type, fallback)
        return fallback

    def _build_broker_symbol(self, strike: Optional[int], opt_type: str) -> str:
        exp = self._expiry_str or ""
        return f"{self._und}{exp}{strike}{opt_type}"

    # ── Telemetry ─────────────────────────────────────────────────────────────

    def _zone_info_list(self, zones: List[dict], opt_type: str) -> list:
        if self._htf_source in ("option", "spot"):
            # Zones are in option premium units — compare against option LTP
            ltp = (self._ltp_cache.get(opt_type + "1") or self._ltp_cache.get(opt_type + "2") or
                   self._ltp_cache.get("CE1") or self._ltp_cache.get("PE1") or 0.0)
        else:
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

    @staticmethod
    def _to_py(obj):
        """Recursively convert numpy scalars to native Python types for JSON serialization."""
        import numpy as np
        if isinstance(obj, dict):
            return {k: TrapScannerEngine._to_py(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [TrapScannerEngine._to_py(v) for v in obj]
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    def telemetry_snapshot(self) -> dict:
        pos = self._position
        ltp = self._spot_cache or 0.0
        # For option-bar HTF, zone distance should be in option premium units
        opt_ltp = (self._ltp_cache.get("CE1") or self._ltp_cache.get("PE1") or
                   self._ltp_cache.get("CE2") or self._ltp_cache.get("PE2") or 0.0)
        zone_ltp = opt_ltp if self._htf_source in ("option", "spot") else ltp
        atr = self._htf_atr_val

        # zones built later: opt_zones + fut_zones_ui (sorted, capped at 10)
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
            trig = round(z.get("zone_trigger", z.get("entry", 0)), 2)
            dist = round(abs(zone_ltp - trig), 1) if zone_ltp else None
            return {
                "zone_high":    round(z.get("zone_high", 0), 2),
                "zone_low":     round(z.get("zone_low", 0), 2),
                "zone_trigger": trig,
                "t1_target":    round(z.get("sl", 0), 2),
                "dist_pts":     dist,
                "trapped_on":   str(z.get("trapped_on", "")),
                "htf_label":    f"{self._htf_min}-min",
                "ltf_status":   self._zone_ltf_status.get(uid, "watching"),
            }

        # Entry window status
        now_ist  = datetime.now(IST)
        win      = self._entry_win   # [[h,m],[h,m]] or None
        if win:
            ws = time(win[0][0], win[0][1])
            we = time(win[1][0], win[1][1])
            in_win = ws <= now_ist.time() <= we
            if now_ist.time() < ws:
                now_naive  = now_ist.replace(tzinfo=None)
                mins_to    = int((datetime.combine(now_ist.date(), ws) - now_naive).total_seconds() // 60)
                win_status = f"Opens in {mins_to}m ({ws.strftime('%H:%M')})"
            elif in_win:
                win_status = f"OPEN until {we.strftime('%H:%M')}"
            else:
                win_status = f"Closed ({we.strftime('%H:%M')}) — next session"
        else:
            in_win     = True
            win_status = "All-day"

        # Nearest FUT zone for futures-mode instruments (closest TRAPPED zone by distance from spot)
        def _nearest_fut_zone(side: str) -> Optional[dict]:
            # CE → BEAR zones (sellers trapped, entry on price falling through zone)
            # PE → BULL zones (buyers trapped, entry on price rising through zone)
            kind_filter = "BEAR" if side == "CE" else "BULL"
            typed = [z for z in self._htf_fut_zones if z.get("kind") == kind_filter]
            if not typed:
                typed = list(self._htf_fut_zones)  # fallback if kind not set
            trapped = [z for z in typed if z["status"] == "TRAPPED"]
            pool = trapped or [z for z in typed if z.get("status")]
            if not pool or not ltp:
                return None
            best = min(pool, key=lambda z: abs(z.get("zone_trigger", 0) - ltp))
            uid  = _zone_uid(best)
            trig = round(best.get("zone_trigger", 0), 2)
            dist = round(abs(ltp - trig), 1) if ltp else None
            return {
                "zone_high":    round(best.get("zone_high",    0), 2),
                "zone_low":     round(best.get("zone_low",     0), 2),
                "zone_trigger": trig,
                "t1_target":    round(best.get("sl", 0), 2),
                "dist_pts":     dist,
                "trapped_on":   str(best.get("trapped_on", "") or ""),
                "htf_label":    f"{self._htf_min}-min",
                "ltf_status":   self._zone_ltf_status.get(uid, "watching"),
                "kind":         best.get("kind", kind_filter),
            }

        # Per-contract LTP and status for UI (mirrors NiftyTrapScanner dashboard table)
        # futures-mode: zone column shows nearest FUT zone (not empty option-bar zone)
        if self._htf_source == "futures":
            ce_zone = _nearest_fut_zone("CE")
            pe_zone = _nearest_fut_zone("PE")
        elif self._htf_source == "spot":
            # Show option-level zones; show if either HTF 75-min OR cascade found direction
            ce_zone = _best_zone_summary(self._opt_bear_zones) if self._opt_bear_zones else None
            pe_zone = _best_zone_summary(self._opt_bull_zones) if self._opt_bull_zones else None
        else:
            ce_zone = _best_zone_summary(self._htf_bear_zones)
            pe_zone = _best_zone_summary(self._htf_bull_zones)

        contracts = {
            "CE1": {"strike": self._ce1_strike, "ltp": self._ltp_cache.get("CE1"),
                    "bars": len(self._bars_ce1), "zone": ce_zone},
            "CE2": {"strike": self._ce2_strike, "ltp": self._ltp_cache.get("CE2"),
                    "bars": len(self._bars_ce2), "zone": None},
            "PE1": {"strike": self._pe1_strike, "ltp": self._ltp_cache.get("PE1"),
                    "bars": len(self._bars_pe1), "zone": pe_zone},
            "PE2": {"strike": self._pe2_strike, "ltp": self._ltp_cache.get("PE2"),
                    "bars": len(self._bars_pe2), "zone": None},
        }

        # FUT zones: sort by distance from spot and return nearest 10 (not all 90)
        def _sorted_fut_zones(max_n: int = 10) -> list:
            raw = self._zone_info_list(self._htf_fut_zones, "FUT")
            if ltp:
                raw.sort(key=lambda z: abs((z.get("zone_trigger") or 0) - ltp))
            return raw[:max_n]

        if self._htf_source == "spot":
            # Show option zones (updated every cascade cycle); spot zones are direction-only
            opt_zones = (self._zone_info_list(self._opt_bear_zones, "CE") +
                         self._zone_info_list(self._opt_bull_zones, "PE"))
            # Add spot direction info to header via bear/bull counts (already in bear_trapped/bull_trapped)
        else:
            opt_zones = (self._zone_info_list(self._htf_bear_zones, "CE") +
                         self._zone_info_list(self._htf_bull_zones, "PE"))
        fut_zones_ui = _sorted_fut_zones(10)

        snap = {
            "underlying":       self._und,
            "client_id":        self._cid,
            "binding_id":       self._bid,
            "initialized":      self._initialized,
            "intraday_mode":    self._intraday_mode,
            "cascade_ce":       self._intraday_mode_ce,
            "cascade_pe":       self._intraday_mode_pe,
            "gap_fired":        self._gap_fired,
            "spot_ltp":         ltp,
            "htf_source":       self._htf_source,
            "htf_atr":          atr,
            "ce1_strike":       self._ce1_strike,
            "ce2_strike":       self._ce2_strike,
            "pe1_strike":       self._pe1_strike,
            "pe2_strike":       self._pe2_strike,
            "expiry":           self._expiry_str,
            "bear_zones":       bear_trapped,
            "bull_zones":       bull_trapped,
            "fut_zones":        fut_trapped,
            "zones":            opt_zones + fut_zones_ui,
            "entry_window_open":   in_win,
            "entry_window_status": win_status,
            "nearest_ce_zone":     _nearest_fut_zone("CE") if self._htf_source == "futures" else None,
            "nearest_pe_zone":     _nearest_fut_zone("PE") if self._htf_source == "futures" else None,
            "contracts":        contracts,
            "bars_spot":      len(self._bars_spot),
            "bars_ce1":       len(self._bars_ce1),
            "bars_pe1":       len(self._bars_pe1),
            "notified_uids":  len(self._notified_uids),
            "position": {
                "leg":           pos["leg"],
                "signal_leg":    pos.get("signal_leg", pos["leg"]),
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
                "t1_price":      pos.get("t1_price", 0),
                "t1_price_fut":  pos.get("t1_price_fut"),
                "total_qty":     pos["total_qty"],
                "remaining_qty": pos["remaining_qty"],
                "t1_hit":        pos["t1_hit"],
                "entry_ts":      pos["entry_ts"],
                "trail_traps":   [
                    {"zt": float(t["zone_trigger"]), "zh": float(t["zone_high"]), "state": str(t["state"])}
                    for t in pos.get("trail_traps", [])
                ],
            } if pos else None,
        }
        return self._to_py(snap)
