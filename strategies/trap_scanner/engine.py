"""
strategies/trap_scanner/engine.py — Trap Scanner Engine orchestrator (v2).

Plug-and-play adapter between our EventBus data feed and NiftyTrapScanner's
core detection logic (strategies/trap_scanner/scanner.py — unchanged).

Per-instance: one (client_id, binding_id, underlying).
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
from strategies.core.base_book import AbstractStrategyBook
from strategies.core.gate import can_trade
from strategies.core.position_update import PositionUpdateMixin
from strategies.trap_scanner import scanner
from strategies.trap_scanner.config import ConfigMixin, _pivot_levels, _round_strike, _SPOT_KEYS
from strategies.trap_scanner.data import DataMixin
from strategies.trap_scanner.zones import ZonesMixin, _bars_to_df, _resample_htf, _zone_uid
from strategies.trap_scanner.entries import EntryMixin
from strategies.trap_scanner.exits import ExitMixin

logger = logging.getLogger(__name__)


class TrapScannerEngine(AbstractStrategyBook, PositionUpdateMixin, ConfigMixin, DataMixin,
                        ZonesMixin, EntryMixin, ExitMixin):
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
        expiry_mode: str = "current",  # current|next_week|monthly|YYYY-MM-DD (per-client DB value)
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        PositionUpdateMixin.__init__(self, bus, client_id, binding_id, "trap_scanner", underlying)
        self._und = underlying.upper()
        self._lot_mul = lot_multiplier
        self._cid = client_id
        self._bid = binding_id
        self._db = client_db
        # Per-client expiry preference: overrides admin next_week/monthly toggles
        self._expiry_mode = str(expiry_mode or "current").strip()

        self._load_index_config(self._und, ts_admin_cfg)
        # _cascade_min is set by _load_index_config from mtf_min_override / admin / default 15m

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

        self._gap_fired     = False
        self._gap_direction = "FLAT"   # "UP" | "DOWN" | "FLAT" (no gap)
        self._spot_open  = 0.0
        self._spot_cache = 0.0
        self._spot_bad_cnt: int = 0  # consecutive SPOT filter rejections
        self._spot_bad_t0: float = 0.0  # monotonic time when bad-tick streak started
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
        self._htf_bear_zones: List[dict] = []    # seller traps in CE1 premium → buy CE1
        self._htf_bear_zones_2: List[dict] = [] # seller traps in CE2 premium → buy CE2
        self._htf_bull_zones: List[dict] = []   # seller traps in PE1 premium → buy PE1
        self._htf_bull_zones_2: List[dict] = [] # seller traps in PE2 premium → buy PE2
        self._htf_fut_zones: List[dict] = []    # futures only

        # htf_source="spot": separate option-level zones for LTF entry
        # Spot zones (above) decide direction; these decide the actual option entry level
        self._opt_bear_zones: List[dict] = []   # CE option HTF zones (option premium units)
        self._opt_bull_zones: List[dict] = []   # PE option HTF zones (option premium units)

        # Dedup: zones that already fired an entry today
        self._notified_uids: Set[str] = set()
        self._no_margin_today: bool     = False  # set True after all 3 strikes rejected

        # Per-zone LTF status for telemetry: uid → "watching"|"ltf_signal"|"entered"
        self._zone_ltf_status: Dict[str, str] = {}
        # Scale-in state per HTF zone uid: "probe" | "added_5m" | "full"
        self._zone_scale_state: Dict[str, str] = {}
        # HTF ATR for zone-reachability check (Point 1)
        self._htf_atr_val: float = 0.0

        # Cascade mode: no 75-min zone TRAPPED → use 15-min → 5-min
        # Per-leg for htf_source="option" (each of 4 legs evaluated independently)
        self._intraday_mode = False       # legacy / futures / spot
        self._intraday_mode_ce = False    # option-source CE side (CE1 OR CE2)
        self._intraday_mode_pe = False    # option-source PE side (PE1 OR PE2)
        self._intraday_mode_ce1 = False   # CE1 independent flag
        self._intraday_mode_ce2 = False   # CE2 independent flag
        self._intraday_mode_pe1 = False   # PE1 independent flag
        self._intraday_mode_pe2 = False   # PE2 independent flag

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

    def set_expiry_mode(self, mode: str) -> None:
        """Change expiry mode at runtime and force re-initialization on next loop tick."""
        old = self._expiry_mode
        self._expiry_mode = mode.strip()
        if old != self._expiry_mode:
            # Reset expiry so morning_init re-runs _get_expiry with the new mode
            self._expiry_str  = None
            self._expiry_date = None
            self._initialized = False
            self._log.info("Expiry mode changed %s → %s; forcing re-init", old, self._expiry_mode)

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

    def _log_settings_banner(self) -> None:
        W = 72
        border = "-" * W
        adm = self._admin_cfg.get("per_index", {}).get(self._und, {})
        source_label = {"option": "OPTION-premium", "futures": "FUTURES", "spot": "SPOT"}.get(self._htf_source, self._htf_source)
        sq = self._sq_off_str or "none (24/7)"
        cut = self._cutoff_str or "none (24/7)"
        lines = [
            f"+{border}",
            f"| ACTIVE TRAP-SCANNER SETTINGS -- {self._und} ({self._cid}/{self._bid})",
            f"|{border}",
            f"|  HTF: {self._htf_min}m  |  LTF: {self._ltf_min}m  |  source: {source_label}",
            f"|  SL buf: {self._sl_buf} pts below zone_low  |  Gap thresh: {self._gap_thresh}%",
            f"|  Entry cutoff: {cut}  |  Square-off: {sq}",
            f"|  Lots: {self._lot_mul} x {self._lot_size} = {self._lot_mul * self._lot_size} qty",
            f"|  Profit floor: {self._profit_floor:,.0f}  |  No-target-TSL: {self._no_target_tsl}  |  Scale-in: {self._scale_in_enabled}",
            f"|  Gap-skip DTE <= {self._gap_skip_dte} (0=off)  |  DTE min filter <= {self._dte_min} (0=off)  |  Expiry mode: {self._expiry_mode}",
            f"|  Per-index admin keys present: {sorted(adm.keys()) or 'none (all defaults from code)'}",
            f"+{border}",
        ]
        for line in lines:
            self._log.info(line)
            logger.info("TS[%s/%s/%s] %s", self._cid, self._bid, self._und, line)

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        self._running = True
        self._tasks = [
            loop.create_task(self._lifecycle_loop(), name=f"ts_life_{self._und}_{self._cid}"),
            loop.create_task(self._opt_tick_loop(),  name=f"ts_opt_{self._und}_{self._cid}"),
            loop.create_task(self._idx_tick_loop(),  name=f"ts_idx_{self._und}_{self._cid}"),
        ]
        logger.info("TrapScannerEngine[%s/%s/%s]: started.", self._cid, self._bid, self._und)
        self._log_settings_banner()

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
                is_mcx = self._und in ("CRUDEOIL",)
                is_crypto = self._und in ("BTC", "ETH")
                # crypto = 24/7; no market_open gate; init can run at any hour
                market_open = time(0, 0) if is_crypto else (time(9, 5) if is_mcx else time(9, 16))
                # sq_off_str=None means 24/7 (crypto) — never force-exit on EOD
                _eod_due = False
                if self._sq_off_str:
                    sq_h, sq_m = map(int, self._sq_off_str.split(":"))
                    _eod_due = now.time() >= time(sq_h, sq_m)

                if _eod_due and self._initialized:
                    await self._eod_square_off()
                    self._reset_day_state()
                elif not self._initialized and now.time() >= market_open:
                    # Never re-init after sq_off time — prevents infinite EOD→init→EOD loop
                    # (24/7 crypto: _eod_due is always False so this guard is never hit)
                    if _eod_due:
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
                if self._exchange == "DELTA":
                    # BTC/ETH: use Delta REST (Upstox has no crypto) — public endpoint, no auth
                    C_fut, today_open_fut = await self._fetch_delta_prev_close_and_today_open(self._und)
                    if C_fut <= 0:
                        self._log.warning(
                            "Delta %s: no prev-day close from REST; will retry in 120s", self._und
                        )
                        return False
                else:
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
            _raw_gap = gap_pct >= self._gap_thresh
            # Near-expiry gap skip: backtest shows GapWR=0% when DTE<=gap_skip_dte.
            # MTF intraday cascade unreliable near monthly expiry (IV spike distorts zones).
            _dte = (self._expiry_date - date.today()).days if self._expiry_date else 999
            _near_expiry_skip = _raw_gap and self._gap_skip_dte > 0 and _dte <= self._gap_skip_dte
            if _near_expiry_skip:
                self._log.info(
                    "Gap %.1f%% >= %.1f%% BUT DTE=%d <= %d → suppressing gap-fired (near-expiry skip)",
                    gap_pct, self._gap_thresh, _dte, self._gap_skip_dte,
                )
            self._gap_fired = _raw_gap and not _near_expiry_skip
            self._gap_direction = ("UP" if today_open >= C else "DOWN") if self._gap_fired else "FLAT"

            if self._exchange == "DELTA":
                # BTC/ETH perpetual: no CE/PE options, no strike selection, no expiry.
                # All zone detection + SL/T1 use _bars_fut (BTCUSD/ETHUSD perpetual) only.
                self._ce1_strike = 0; self._ce2_strike = 0
                self._pe1_strike = 0; self._pe2_strike = 0
                self._ce1_key = "";   self._ce2_key = ""
                self._pe1_key = "";   self._pe2_key = ""
                self._expiry_str = "PERP"; self._expiry_date = None
                self._log.info("DELTA perpetual mode: no CE/PE strikes; trading BTCUSD/ETHUSD directly")
            else:
                # Futures-mode underlyings (CrudeOil): ATM ± fixed ITM offsets for option strikes.
                # Option-mode (NSE/BSE): pivot-based S1/S2/R1/R2 strike selection.
                use_atm_offsets = self._htf_source == "futures" or self._gap_fired

                if use_atm_offsets:
                    direction = self._gap_direction if self._gap_fired else ("UP" if today_open >= C else "DOWN")
                    atm = _round_strike(today_open, self._step)
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
                    self._ce1_strike = _round_strike(pivots["s1"], self._step)
                    self._ce2_strike = _round_strike(pivots["s2"], self._step)
                    self._pe1_strike = _round_strike(pivots["r1"], self._step)
                    self._pe2_strike = _round_strike(pivots["r2"], self._step)
                    if self._ce2_strike == self._ce1_strike:
                        self._ce2_strike -= self._step
                    if self._pe2_strike == self._pe1_strike:
                        self._pe2_strike += self._step
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
                if self._exchange == "DELTA":
                    # BTC/ETH: historical bars from Delta REST — no Upstox instrument key exists
                    self._fut_key = "BTCUSD" if self._und == "BTC" else "ETHUSD"
                    if not self._bars_fut:
                        self._bars_fut = await self._fetch_delta_1m_bars(self._und, lookback_days=3)
                else:
                    # Use REGISTRY for the near-month futures key (correct for MCX CrudeOil)
                    try:
                        from data_layer.instrument_registry import REGISTRY as _REG
                        fk = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                    except Exception:
                        fk = ""
                    self._fut_key = fk or _SPOT_KEYS.get(self._und, "")
                    # Guard: skip re-fetch if bars already seeded (retry path or live ticks already added)
                    if not self._bars_fut:
                        self._bars_fut = await self._fetch_1m_history(self._fut_key)
                # Also seed option bars (needed for LTF scan on entry)
                if self._exchange == "DELTA":
                    # BTC/ETH: option keys use Delta symbol format; historical bars from Delta
                    from data_layer.universal_option_mapper import UniversalOptionMapper as _M
                    _exp = self._expiry_date
                    self._ce1_key = _M.to_delta_symbol(__import__("data_layer.symbol_translator", fromlist=["InternalSymbol"]).InternalSymbol(self._und, float(self._ce1_strike), "CE", _exp)) if _exp else ""
                    self._ce2_key = _M.to_delta_symbol(__import__("data_layer.symbol_translator", fromlist=["InternalSymbol"]).InternalSymbol(self._und, float(self._ce2_strike), "CE", _exp)) if _exp else ""
                    self._pe1_key = _M.to_delta_symbol(__import__("data_layer.symbol_translator", fromlist=["InternalSymbol"]).InternalSymbol(self._und, float(self._pe1_strike), "PE", _exp)) if _exp else ""
                    self._pe2_key = _M.to_delta_symbol(__import__("data_layer.symbol_translator", fromlist=["InternalSymbol"]).InternalSymbol(self._und, float(self._pe2_strike), "PE", _exp)) if _exp else ""
                    # Delta option historical bars not yet implemented → seed empty; live ticks fill them
                    self._log.info(
                        "Delta option keys — CE1=%s CE2=%s PE1=%s PE2=%s",
                        self._ce1_key, self._ce2_key, self._pe1_key, self._pe2_key,
                    )
                else:
                    self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                    self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                    self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                    self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                    if not self._bars_ce1:
                        self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                    if not self._bars_ce2:
                        self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                    if not self._bars_pe1:
                        self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                    if not self._bars_pe2:
                        self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)
                # Merge today's intraday bars (historical API ends at prev day).
                # Upstox primary, Fyers fallback; cached per key for 5 minutes so a
                # reconnect-storm re-init does not hammer the REST API.
                # DELTA exchange: intraday already included in _fetch_delta_1m_bars above — skip.
                merge_map = [
                    ("_bars_fut", self._fut_key, None, None),
                    ("_bars_ce1", self._ce1_key, self._ce1_strike, "CE"),
                    ("_bars_ce2", self._ce2_key, self._ce2_strike, "CE"),
                    ("_bars_pe1", self._pe1_key, self._pe1_strike, "PE"),
                    ("_bars_pe2", self._pe2_key, self._pe2_strike, "PE"),
                ]
                for attr, key, strike, otype in merge_map:
                    if not key or self._exchange == "DELTA":
                        continue
                    intra = await self._fetch_intraday_bars_with_fallback(key, strike, otype)
                    if intra:
                        setattr(self, attr, self._merge_bars(getattr(self, attr), intra))
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
                self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                if not self._bars_spot:
                    self._bars_spot = await self._fetch_1m_history(spot_key)
                if not self._bars_ce1:
                    self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                if not self._bars_ce2:
                    self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                if not self._bars_pe1:
                    self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                if not self._bars_pe2:
                    self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)
                # Merge today's intraday bars for LTF cascade; Upstox primary + Fyers fallback.
                for attr, key, strike, otype in [
                    ("_bars_ce1", self._ce1_key, self._ce1_strike, "CE"),
                    ("_bars_ce2", self._ce2_key, self._ce2_strike, "CE"),
                    ("_bars_pe1", self._pe1_key, self._pe1_strike, "PE"),
                    ("_bars_pe2", self._pe2_key, self._pe2_strike, "PE"),
                ]:
                    if not key:
                        continue
                    intra = await self._fetch_intraday_bars_with_fallback(key, strike, otype)
                    if intra:
                        setattr(self, attr, self._merge_bars(getattr(self, attr), intra))
                self._log.info(
                    "Bars seeded — SPOT(%s)=%d CE1(%s)=%d CE2(%s)=%d PE1(%s)=%d PE2(%s)=%d",
                    spot_key, len(self._bars_spot),
                    self._ce1_key, len(self._bars_ce1),
                    self._ce2_key, len(self._bars_ce2),
                    self._pe1_key, len(self._bars_pe1),
                    self._pe2_key, len(self._bars_pe2),
                )
            else:
                # htf_source="option" (NSE/BSE): option bars for BOTH HTF and LTF
                # CE1=S1 bars detect bear seller traps; PE1=R1 bars detect bull seller traps
                # All zone H/L values in option premium units → scan_ltf is consistent
                self._ce1_key = self._build_upstox_key(self._ce1_strike, "CE")
                self._ce2_key = self._build_upstox_key(self._ce2_strike, "CE")
                self._pe1_key = self._build_upstox_key(self._pe1_strike, "PE")
                self._pe2_key = self._build_upstox_key(self._pe2_strike, "PE")
                if not self._bars_ce1:
                    self._bars_ce1 = await self._fetch_1m_history(self._ce1_key)
                if not self._bars_ce2:
                    self._bars_ce2 = await self._fetch_1m_history(self._ce2_key)
                if not self._bars_pe1:
                    self._bars_pe1 = await self._fetch_1m_history(self._pe1_key)
                if not self._bars_pe2:
                    self._bars_pe2 = await self._fetch_1m_history(self._pe2_key)
                # Merge today's intraday bars for LTF cascade; Upstox primary + Fyers fallback.
                for attr, key, strike, otype in [
                    ("_bars_ce1", self._ce1_key, self._ce1_strike, "CE"),
                    ("_bars_ce2", self._ce2_key, self._ce2_strike, "CE"),
                    ("_bars_pe1", self._pe1_key, self._pe1_strike, "PE"),
                    ("_bars_pe2", self._pe2_key, self._pe2_strike, "PE"),
                ]:
                    if not key:
                        continue
                    intra = await self._fetch_intraday_bars_with_fallback(key, strike, otype)
                    if intra:
                        setattr(self, attr, self._merge_bars(getattr(self, attr), intra))
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
                    prev_ltp = self._ltp_cache.get(bkey, 0)
                    self._ltp_cache[bkey] = ltp   # track live option LTP per leg
                    # Re-evaluate cascade once the first real LTP arrives for a leg that was
                    # 0 at day-init (option ticks arrive after startup HTF scan runs).
                    if prev_ltp == 0 and ltp > 0 and self._intraday_mode and self._htf_atr_val > 0:
                        self._check_zone_reachability()
                    # TICK-LEVEL zone scan: run immediately on every tick when LTP is inside
                    # an HTF zone (do not wait for 1-min candle close). This catches trigger
                    # crossings that fall between two 1-min boundaries (e.g. 0.9 pts away miss).
                    if not self._position and self._ltp_in_any_htf_zone(bkey):
                        self._on_candle_close(label, ts)
                    # Push real-time LTP to UI via TRAP_TICK (bypasses 2s heartbeat poll).
                    self._bus.publish(Topic.TRAP_TICK, {
                        "cid": self._cid, "bid": self._bid, "und": self._und,
                        "leg": bkey, "ltp": ltp,
                    })
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
                    # If ALSO far from day-open (REST, reliable) -> hard-reject, no unstick.
                    # Catches Upstox BSE SENSEX ~80000 garbage while real market is ~76700.
                    if self._spot_open > 0 and abs(raw_ltp - self._spot_open) / self._spot_open > _guard:
                        self._spot_bad_cnt += 1
                        if self._spot_bad_cnt % 60 == 1:
                            self._log.warning(
                                "SPOT mis-decode rejected: ltp=%.2f is %.1f%% from "
                                "day_open=%.2f (cnt=%d)",
                                raw_ltp,
                                abs(raw_ltp - self._spot_open) / self._spot_open * 100,
                                self._spot_open, self._spot_bad_cnt,
                            )
                        continue
                    # Far from rolling ref but plausible vs day-open: genuine move.
                    # Accept after 5 consecutive (not a one-off spike).
                    self._spot_bad_cnt += 1
                    if self._spot_bad_cnt < 5:
                        self._log.warning(
                            "SPOT tick rejected: ltp=%.2f deviates >%.1f%% from last=%.2f "
                            "(consecutive=%d)",
                            raw_ltp, _guard * 100, _ref, self._spot_bad_cnt,
                        )
                        continue
                    self._log.info(
                        "SPOT filter unstick: accepting ltp=%.2f after %d consecutive rejects "
                        "(old_ref=%.2f day_open=%.2f)",
                        raw_ltp, self._spot_bad_cnt, _ref, self._spot_open,
                    )
                self._spot_bad_cnt = 0
                self._spot_cache = raw_ltp
                # Re-subscribe tracked option keys every 60s — survives feeder reconnect
                now = datetime.now(IST)
                if (now - _last_resub).total_seconds() >= 60:
                    if self._exchange == "DELTA":
                        self._log.info(
                            "heartbeat: %s=%.2f pos=%s [perp_side=%s]",
                            self._fut_key, raw_ltp,
                            self._position["side"] if self._position else "none",
                            (self._position or {}).get("perp_side", "—"),
                        )
                    else:
                        ce1_ltp = self._ltp_cache.get("CE1", 0)
                        pe1_ltp = self._ltp_cache.get("PE1", 0)
                        self._log.info(
                            "heartbeat: spot=%.2f CE1=%.1f PE1=%.1f pos=%s [keys: ce1=%s pe1=%s]",
                            raw_ltp, ce1_ltp, pe1_ltp,
                            self._position["side"] if self._position else "none",
                            self._ce1_key, self._pe1_key,
                        )
                    _last_resub = now
                    # DELTA perpetuals: only re-subscribe the perpetual key — no option keys
                    if self._exchange == "DELTA":
                        keys = [k for k in [self._fut_key] if k]
                    else:
                        keys = [k for k in [self._fut_key,
                                            self._ce1_key, self._ce2_key,
                                            self._pe1_key, self._pe2_key] if k]
                    if keys:
                        feeder = (self._mcx_feeder if self._mcx_feeder is not None
                                  else getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None)
                        if feeder:
                            try:
                                # resubscribe_tokens always sends the WS subscribe command
                                # even for already-subscribed keys — recovers from silent drops
                                if hasattr(feeder, "resubscribe_tokens"):
                                    await feeder.resubscribe_tokens(keys)
                                elif hasattr(feeder, "subscribe_tokens"):
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

    # ── Trade gating ──────────────────────────────────────────────────────────

    def _can_trade(self) -> bool:
        """Gate new entries on THIS binding's Terminal (broker connected) AND Trade
        toggle (is_trade_enabled). Mirrors SellStraddle._any_active_terminal but for
        the single (client, binding) this book belongs to. Throttled to one DB read
        per 5s. Fail-OPEN when no DB is wired (unit tests / headless) so tests are
        unaffected; production always injects the DB via TrapBookManager.

        NOTE: this gates FIRING only — the book stays alive so an already-open
        position keeps its SL/T1 management even if Terminal/Trade is toggled off."""
        return can_trade(
            self._cid, self._bid, self._db,
            strategy_name="trap_scanner", underlying=self._und,
        )

    # ── Day state reset ───────────────────────────────────────────────────────

    def _reset_day_state(self) -> None:
        self._initialized      = False
        self._intraday_mode    = False
        self._intraday_mode_ce = False;  self._intraday_mode_pe = False
        self._intraday_mode_ce1 = False; self._intraday_mode_ce2 = False
        self._intraday_mode_pe1 = False; self._intraday_mode_pe2 = False
        self._sweep_watch      = None
        self._day_init_done = False
        self._bars_spot = []; self._bars_fut = []
        self._bars_ce1  = []; self._bars_ce2 = []
        self._bars_pe1  = []; self._bars_pe2 = []
        self._htf_bear_zones = []; self._htf_bear_zones_2 = []
        self._htf_bull_zones = []; self._htf_bull_zones_2 = []
        self._htf_fut_zones  = []
        self._buckets        = {}
        self._notified_uids  = set()
        self._zone_ltf_status = {}
        self._zone_scale_state = {}
        self._htf_atr_val = 0.0
        self._ltp_cache   = {}
        self._ce1_strike = None; self._ce2_strike = None
        self._pe1_strike = None; self._pe2_strike = None
        self._ce1_key = None; self._ce2_key = None
        self._pe1_key = None; self._pe2_key = None
        self._expiry_str = None; self._expiry_date = None
        self._clear_persisted_position()

    def reset_session(self) -> None:
        """AbstractStrategyBook hook — reset intraday/session state."""
        self._reset_day_state()

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
            self.notify_position_update(self._position, force=True)
        except Exception as exc:
            self._log.warning("_persist_position failed: %s", exc)

    def _clear_persisted_position(self) -> None:
        try:
            p = self._position_file()
            if os.path.exists(p):
                os.remove(p)
            self.notify_position_update(None, force=True)
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

    # ── Closed-trade recording ────────────────────────────────────────────────

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

        def _best_zone_summary(zone_list: list, ltp_override: float = 0.0) -> Optional[dict]:
            """
            Pick zone closest to LTP:
              1. LTP inside zone (zone_low <= LTP <= zone_high) -> best match
              2. Nearest zone_high above LTP
              3. Nearest zone_high below LTP (fallback)
            Skips dead zones where LTP < zone_low.
            ltp_override: pass per-leg LTP so CE zones use CE LTP and PE zones use PE LTP.
            """
            zltp = ltp_override if ltp_override > 0 else zone_ltp
            trapped = [z for z in zone_list if z["status"] == "TRAPPED"]
            if zltp > 0:
                trapped = [z for z in trapped if zltp >= z.get("zone_low", 0)]
            if not trapped:
                return None

            if zltp > 0:
                inside = [z for z in trapped
                          if z.get("zone_low", 0) <= zltp <= z.get("zone_high", 0)]
                if inside:
                    z = max(inside, key=lambda zz: zz.get("zone_high", 0))
                else:
                    above = [z for z in trapped if z.get("zone_high", 0) > zltp]
                    if above:
                        z = min(above, key=lambda zz: zz.get("zone_high", 0) - zltp)
                    else:
                        z = max(trapped, key=lambda zz: zz.get("zone_high", 0))
            else:
                z = trapped[-1]

            uid = _zone_uid(z)
            trig = round(z.get("zone_trigger", z.get("entry", 0)), 2)
            dist = round(abs(zltp - trig), 1) if zltp else None
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
            def _zone_trig(z: dict) -> float:
                # zone_trigger not stored in scan_htf_spot entries; compute from 2/3 rule
                zl, zh = z.get("zone_low", 0), z.get("zone_high", 0)
                w = zh - zl
                if kind_filter == "BEAR":
                    return zl + 2 * w / 3   # lower 2/3 = bear entry region
                else:
                    return zh - 2 * w / 3   # upper 2/3 = bull entry region

            best = min(pool, key=lambda z: abs(_zone_trig(z) - ltp))
            uid  = _zone_uid(best)
            trig = round(_zone_trig(best), 2)
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
        ltp_ce1 = self._ltp_cache.get("CE1") or 0.0
        ltp_ce2 = self._ltp_cache.get("CE2") or 0.0
        ltp_pe1 = self._ltp_cache.get("PE1") or 0.0
        ltp_pe2 = self._ltp_cache.get("PE2") or 0.0

        if self._htf_source == "futures":
            ce_zone = _nearest_fut_zone("CE")
            pe_zone = _nearest_fut_zone("PE")
        elif self._htf_source == "spot":
            # Show option-level zones; show if either HTF 75-min OR cascade found direction
            ce_zone = _best_zone_summary(self._opt_bear_zones, ltp_ce1) if self._opt_bear_zones else None
            pe_zone = _best_zone_summary(self._opt_bull_zones, ltp_pe1) if self._opt_bull_zones else None
        else:
            ce_zone = _best_zone_summary(self._htf_bear_zones, ltp_ce1)
            pe_zone = _best_zone_summary(self._htf_bull_zones, ltp_pe1)

        contracts = {
            "CE1": {"strike": self._ce1_strike, "ltp": self._ltp_cache.get("CE1"),
                    "bars": len(self._bars_ce1), "zone": ce_zone},
            "CE2": {"strike": self._ce2_strike, "ltp": self._ltp_cache.get("CE2"),
                    "bars": len(self._bars_ce2), "zone": _best_zone_summary(self._htf_bear_zones_2, ltp_ce2)},
            "PE1": {"strike": self._pe1_strike, "ltp": self._ltp_cache.get("PE1"),
                    "bars": len(self._bars_pe1), "zone": pe_zone},
            "PE2": {"strike": self._pe2_strike, "ltp": self._ltp_cache.get("PE2"),
                    "bars": len(self._bars_pe2), "zone": _best_zone_summary(self._htf_bull_zones_2, ltp_pe2)},
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
            "cascade_ce1":      self._intraday_mode_ce1,
            "cascade_ce2":      self._intraday_mode_ce2,
            "cascade_pe1":      self._intraday_mode_pe1,
            "cascade_pe2":      self._intraday_mode_pe2,
            "gap_fired":        self._gap_fired,
            "gap_direction":    self._gap_direction,
            "spot_ltp":         ltp,
            "htf_source":       self._htf_source,
            "exchange":         self._exchange,
            "htf_atr":          atr,
            "ce1_strike":       self._ce1_strike,
            "ce2_strike":       self._ce2_strike,
            "pe1_strike":       self._pe1_strike,
            "pe2_strike":       self._pe2_strike,
            "expiry":           self._expiry_str,
            "expiry_date":      str(self._expiry_date) if self._expiry_date else None,
            "expiry_mode":      self._expiry_mode,
            "bear_zones":       bear_trapped,
            "bull_zones":       bull_trapped,
            "fut_zones":        fut_trapped,
            "zones":            opt_zones + fut_zones_ui,
            "entry_window_open":   in_win,
            "entry_window_status": win_status,
            "nearest_ce_zone":     _nearest_fut_zone("CE") if self._htf_source == "futures" else None,
            "nearest_pe_zone":     _nearest_fut_zone("PE") if self._htf_source == "futures" else None,
            "contracts":        contracts,
            "htf_min":        self._htf_min,
            "cascade_min":    self._cascade_min,
            "ltf_min":        self._ltf_min,
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
                "fut_entry_ref": pos.get("fut_entry_ref"),
                "opt_ltp":       next(
                    (self._ltp_cache.get(lb, 0) or 0
                     for lb in ["CE1","CE2","PE1","PE2"]
                     if getattr(self, f"_{lb.lower()}_key", "") == pos.get("exec_key","")),
                    0.0
                ),
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
