"""
strategies/trap_trading_engine.py — NewTrap MTF Institutional Liquidity Sweep Engine.

5-stage sequential protocol:
  Stage 1 (HTF 75-min): Bearish candle recorded → HTF_BEARISH
  Stage 2 (HTF 75-min): Next bar sweeps Stage 1 high → TRAP_LOCKED
  Stage 3 (live tick):  Premium retraces to entry_origin ±RETEST_ZONE_PERCENT → RETEST_ALERT
  Stage 4 (MTF 5-min):  4a: bearish 5-min candle → MTF_BEARISH
                        4b: next 5-min bar sweeps mtf_bearish_high → ARMED
  Stage 5 (live tick):  premium <= ltf_entry_line + SLIPPAGE_BUFFER → fire BUY orders

Exit guard (1-min candle close):
  - 1m_close < ltf_sl_line → VOID (SL)
  - current_premium >= target_high → MITIGATE (profit)
  - time >= 15:30 IST → EOD force-exit

Rolling Base: any HTF or MTF candle closing below previous candle's close → update
rolling_base = candle.low.

All timestamps in Asia/Kolkata (IST).
"""

from __future__ import annotations

import asyncio
import logging
import math
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import numpy as np

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import CandleEvent, IndexTick, OptionTick, EventBus
from strategies.trap_seller_detection import SellerTrapDetector, State as _DetState

import os

logger = logging.getLogger(__name__)


def _make_trap_logger(underlying: str) -> logging.Logger:
    """Per-symbol Trap log → logs/clients/tt_{underlying}_YYYYMMDD.log so the 5-stage
    HTF→MTF→LTF progression is visible (the engine had no per-symbol log file)."""
    name = f"client.tt.{underlying}"
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(logging.INFO)
    log_dir = os.path.join("logs", "clients")
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    fh = logging.FileHandler(os.path.join(log_dir, f"tt_{underlying}_{date_str}.log"), encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = False
    return lg


# ─────────────────────────────────────────────────────────────────────────────
# Module-level constants
# ─────────────────────────────────────────────────────────────────────────────

_MARKET_CLOSE = time(15, 30, 0)      # IST — NSE/BSE force-exit time
_MCX_MARKET_CLOSE = time(23, 30, 0)  # IST — MCX commodities trade the evening session
_MCX_SET = {"CRUDEOIL", "CRUDEOILM", "NATURALGAS", "GOLD", "SILVER"}


def exec_strike(spot: float, opt_type: str, buy_depth: int, step: float) -> int:
    """Resolve the execution BUY strike from the live spot/future. ATM = round(spot/step)*step;
    CE goes `buy_depth` steps ITM (below spot), PE goes `buy_depth` steps ITM (above spot)."""
    atm = round(float(spot) / step) * step
    off = int(buy_depth) * int(step)
    return int(atm - off) if opt_type == "CE" else int(atm + off)


def trap_transition_msg(opt: str, strike, tf: str, new_state: str,
                        level_l: float, level_h: float, price: float) -> str:
    """Human-readable line for a per-leg detector state change (the Below→Above→Return
    story), so the dry-run log shows exactly when sellers enter/get trapped/we arm."""
    head = f"{opt} {strike} {tf} {new_state}:"
    if new_state == "SELLERS_IN":
        return f"{head} price {price:.2f} broke below L={level_l:.2f} (sl={level_h:.2f}) — sellers entered"
    if new_state == "TRAPPED":
        return f"{head} price {price:.2f} broke above H={level_h:.2f} — sellers trapped"
    if new_state == "ENTRY_READY":
        return f"{head} price {price:.2f} returned to L={level_l:.2f}"
    return f"{head} price {price:.2f}"


def should_rotate(running_side, signal_side, has_position: bool) -> bool:
    """Rotate (close the runner, open the new leg) only when a position is open and the
    new entry signal is for the OPPOSITE leg. Same-side signals are ignored."""
    return bool(has_position and running_side is not None and signal_side != running_side)


def sl_triggered(ltp: float, sl_5m: float, sl_active) -> bool:
    """Two-tier stop: before a 1-min close breaks the 5m low, the stop is the 5m low;
    after, it is the breaching 1-min candle's low (`sl_active`). Exit when ltp < ref."""
    ref = sl_active if sl_active is not None else sl_5m
    return float(ltp) < float(ref)


def _row_date(row: dict):
    """Extract a date from a 1m-bar row's timestamp (datetime or ISO string)."""
    ts = row.get("timestamp")
    if ts is None:
        return None
    if hasattr(ts, "date"):
        return ts.date()
    try:
        from datetime import datetime as _dt
        return _dt.fromisoformat(str(ts).replace("Z", "")).date()
    except Exception:
        return None

_DOW_OFFSET: Dict[int, int] = {
    0: 200,  # Monday
    1: 100,  # Tuesday
    2: 500,  # Wednesday
    3: 400,  # Thursday
    4: 300,  # Friday
}


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV Ring Buffer  (kept from existing — O(1) push, O(n) array conversion)
# ─────────────────────────────────────────────────────────────────────────────

class OHLCVBuffer:
    """
    Fixed-capacity OHLCV ring buffer backed by deques (O(1) push, O(n) array
    conversion).  Array conversion is called only on candle close, not on ticks.
    """

    __slots__ = ("_cap", "_o", "_h", "_l", "_c", "_v", "_t")

    def __init__(self, capacity: int) -> None:
        self._cap = capacity
        self._o: deque[float]    = deque(maxlen=capacity)
        self._h: deque[float]    = deque(maxlen=capacity)
        self._l: deque[float]    = deque(maxlen=capacity)
        self._c: deque[float]    = deque(maxlen=capacity)
        self._v: deque[float]    = deque(maxlen=capacity)
        self._t: deque[datetime] = deque(maxlen=capacity)

    def push(self, c: CandleEvent) -> None:
        self._o.append(c.open)
        self._h.append(c.high)
        self._l.append(c.low)
        self._c.append(c.close)
        self._v.append(float(c.volume))
        self._t.append(c.timestamp)

    def arrays(self) -> Tuple[
        np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray
    ]:
        """Return (opens, highs, lows, closes, volumes) as float64 arrays."""
        n = len(self._c)
        return (
            np.fromiter(self._o, dtype=np.float64, count=n),
            np.fromiter(self._h, dtype=np.float64, count=n),
            np.fromiter(self._l, dtype=np.float64, count=n),
            np.fromiter(self._c, dtype=np.float64, count=n),
            np.fromiter(self._v, dtype=np.float64, count=n),
        )

    def __len__(self) -> int:
        return len(self._c)

    # ── Scalar accessors ──────────────────────────────────────────────────────

    def last_close(self) -> float:
        return self._c[-1] if self._c else 0.0

    def prev_close(self) -> float:
        return self._c[-2] if len(self._c) >= 2 else 0.0

    def last_high(self) -> float:
        return self._h[-1] if self._h else 0.0

    def last_low(self) -> float:
        return self._l[-1] if self._l else 0.0

    def last_ts(self) -> Optional[datetime]:
        return self._t[-1] if self._t else None

    def swing_high(self, lookback: int) -> float:
        """Maximum high over the last `lookback` bars — the liquidity pool level."""
        if not self._h:
            return 0.0
        n = min(lookback, len(self._h))
        return max(list(self._h)[-n:])

    def recent_high_pct(self, window: int = 20) -> float:
        if not self._c:
            return 0.0
        n = min(window, len(self._c))
        return max(list(self._c)[-n:])

    def recent_low_pct(self, window: int = 20) -> float:
        if not self._c:
            return 0.0
        n = min(window, len(self._c))
        return min(list(self._c)[-n:])


# ─────────────────────────────────────────────────────────────────────────────
# State machine phases
# ─────────────────────────────────────────────────────────────────────────────

class _Phase(Enum):
    IDLE         = auto()  # No active setup
    HTF_BEARISH  = auto()  # Stage 1 complete — bearish HTF candle recorded
    TRAP_LOCKED  = auto()  # Stage 2 complete — HTF sweep confirmed
    RETEST_ALERT = auto()  # Stage 3 complete — premium retested entry_origin
    MTF_BEARISH  = auto()  # Stage 4a complete — 5-min bearish candle found
    MTF_LOCKED   = auto()  # Stage 4b complete — nested 5-min sweep (→ ARMED immediately)
    ARMED        = auto()  # Stage 5 — waiting for premium touch trigger
    LIVE         = auto()  # Position open


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol trap state
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _TrapLevel:
    """One HTF bearish candle's trap zone. Multiple can be active simultaneously."""
    entry_origin:    float          # low of the bearish candle (bears' entry reference)
    bears_sl:        float          # high of the bearish candle (bears' stop loss)
    target_high:     float = 0.0    # filled when sweep fires (sweep candle high)
    disabled:        bool  = False  # True after this level's SL is hit in a live trade
    ts:              Optional[datetime] = None

    @property
    def active(self) -> bool:
        return not self.disabled and self.target_high > 0.0


@dataclass
class _TrapState:
    phase: _Phase = _Phase.IDLE

    # Stage 1 accumulator — bearish candles pending a sweep
    # Each entry: (entry_origin=candle_low, bears_sl=candle_high)
    pending_levels:    list = field(default_factory=list)  # List[_TrapLevel], no sweep yet

    # Stage 2 — all confirmed trap levels (sweep fired, sorted highest→lowest)
    trap_levels:       list = field(default_factory=list)  # List[_TrapLevel]

    # Active level currently being retested (Stage 3 onward)
    active_level:      Optional[_TrapLevel] = None

    # Legacy single-level fields kept for backward compat with MTF/exit logic
    htf_bearish_open:  float = 0.0
    htf_bearish_high:  float = 0.0
    htf_bearish_ts:    Optional[datetime] = None
    entry_origin:      float = 0.0   # mirrors active_level.entry_origin
    target_high:       float = 0.0   # mirrors active_level.target_high

    # Stage 4 — MTF nested trap
    mtf_bearish_open:  float = 0.0
    mtf_bearish_high:  float = 0.0
    mtf_bearish_low:   float = 0.0
    mtf_bearish_ts:    Optional[datetime] = None
    htf_trap_low:      float = 0.0   # HTF single-candle trap candle low (widest structural SL)
    mtf_sweep_low:     float = 0.0   # Stage 4b sweep candle low (structural SL reference)
    ltf_entry_line:    float = 0.0   # 5-min bearish.low → touch trigger (retest entry)
    ltf_sl_line:       float = 0.0   # set at entry time (dynamic: % below fill; structural: sweep low)

    # Rolling base (survives across trades)
    rolling_base:      float = 0.0

    # Live position
    trade_id:          Optional[str] = None
    entry_price:       float = 0.0
    quantity:          int = 0

    # Backtest mode flag
    is_backtest:       bool = False


# ─────────────────────────────────────────────────────────────────────────────
# TrapTradingEngine
# ─────────────────────────────────────────────────────────────────────────────

class TrapTradingEngine:
    """
    Standalone async multi-timeframe engine implementing the NewTrap 5-stage
    institutional liquidity sweep detection protocol.

    Subscribes to:
      • Topic.CANDLE_CLOSE  — routed by timeframe (HTF / MTF / LTF)
      • Topic.OPTION_TICK   — premium cache updates, stage 3 retest, stage 5 touch

    Publishes to:
      • Topic.SIGNAL  — BUY / exit signals (per-client capital allocation)

    Wire-up (run_system.py):
        trap_engine = TrapTradingEngine(bus, cfg, client_db)
        asyncio.create_task(trap_engine.run(), name="trap_engine")
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        client_db=None,
    ) -> None:
        self._bus        = bus
        self._cfg        = cfg
        self._client_db  = client_db
        self._running    = False

        # Queues — subscribed in run() so the event loop is already running
        self._candle_q: Optional[asyncio.Queue] = None
        self._opt_q:    Optional[asyncio.Queue] = None
        self._index_q:  Optional[asyncio.Queue] = None

        # Per-symbol state (legacy — retained only for graceful telemetry fallback)
        self._states: Dict[str, _TrapState] = {}

        # v2 per-leg seller-trap detectors, keyed by leg_key. Two timeframes:
        # HTF arms the setup; MTF (gated by HTF ENTRY_READY) fires the entry signal.
        self._htf_det: Dict[str, SellerTrapDetector] = {}
        self._mtf_det: Dict[str, SellerTrapDetector] = {}
        # v2 single live position (one leg at a time; rotation in Task 6).
        self._v2_position: Optional[dict] = None

        # Day-fixed tracked strikes per underlying (prev-day ATM + DTE offset).
        # Computed once at warm-start; drives which CE/PE the trap follows.
        from strategies.trap_strike_selection import TrapStrikes as _TS
        self._day_strikes: Dict[str, _TS] = {}

        # Per-symbol MTF (5-min) OHLCV buffers — instance variable, NOT class variable
        self._mtf_bufs: Dict[str, OHLCVBuffer] = {}

        # Premium cache: option_symbol → last ltp
        self._prem_cache:  Dict[str, float] = {}
        # Per-leg premium cache: (underlying, strike, opt_type) → last ltp.
        # Lets the engine track the SPECIFIC tracked CE/PE without the future spot.
        self._leg_prem: Dict[Tuple[str, int, str], float] = {}
        # Per-leg live candle builders: (leg_key, timeframe) → building OHLC bucket.
        self._leg_bars: Dict[Tuple[str, int], dict] = {}
        # Spot cache: underlying → last spot (used ONLY to pick the day's centre once)
        self._spot_cache:  Dict[str, float] = {}

        # Feeder for subscribing the tracked CE/PE strikes (set via set_feeder()).
        self._feeder = None
        self._rebalancer = None   # set via set_rebalancer() — pins tracked strikes
        self._subscribed_keys: set = set()

        # Open positions: trade_id → (trade_id, option_symbol, entry_price, quantity)
        self._open_positions: Dict[str, Tuple[str, str, float, int]] = {}
        # Per-symbol log files (HTF/MTF/LTF stage visibility)
        self._clogs: Dict[str, logging.Logger] = {}

        # Telemetry / stats
        self._signals:      int = 0
        self._backtest_log: List[dict] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main async entry — starts candle, index tick, and option tick loops."""
        self._candle_q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        self._opt_q    = self._bus.subscribe(Topic.OPTION_TICK)
        self._index_q  = self._bus.subscribe(Topic.INDEX_TICK)
        self._running  = True
        logger.info("TrapTradingEngine: starting NewTrap MTF engine.")
        await asyncio.gather(
            self._candle_loop(),
            self._index_tick_loop(),
            self._option_tick_loop(),
        )

    def stop(self) -> None:
        self._running = False
        logger.info("TrapTradingEngine: stop requested.")

    # ── Position persistence (survive restarts) ────────────────────────────────
    # Trap is multi-symbol: one JSON per symbol that holds a LIVE trade, keyed
    # "<SYMBOL>_trap". Only the live-trade fields needed to manage the exit are
    # persisted; detection state (rolling_base/trap_levels) is rebuilt by
    # warm_start() from client_db bars. MIS prior-day trades are discarded by the
    # store (broker auto-squared at EOD) — Trap is intraday.

    @staticmethod
    def _persist_key(symbol: str) -> str:
        return f"{symbol}_trap"

    def _persist_trade(self, symbol: str) -> None:
        try:
            from data_layer import position_store as _ps
            st = self._states.get(symbol)
            if (st and st.phase == _Phase.LIVE and st.trade_id
                    and st.trade_id in self._open_positions):
                _, opt_sym, entry_price, qty = self._open_positions[st.trade_id]
                _ps.save(self._persist_key(symbol), {
                    "trade_id": st.trade_id, "option_symbol": opt_sym,
                    "entry_price": float(entry_price), "quantity": int(qty),
                    "ltf_sl_line": float(st.ltf_sl_line), "target_high": float(st.target_high),
                    "entry_origin": float(st.entry_origin),
                }, product_type="MIS")
            else:
                _ps.clear(self._persist_key(symbol))
        except Exception as exc:
            logger.warning("TrapEngine: persist failed for %s: %s", symbol, exc)

    def _clear_trade(self, symbol: str) -> None:
        try:
            from data_layer import position_store as _ps
            _ps.clear(self._persist_key(symbol))
        except Exception:
            pass

    def _restore_trade(self, symbol: str) -> None:
        """Restore a LIVE trade for one symbol from the store (called at startup)."""
        try:
            from data_layer import position_store as _ps
            saved = _ps.load(self._persist_key(symbol))
            if not saved:
                return
            st = self._get_state(symbol)
            st.trade_id     = saved.get("trade_id")
            st.entry_price  = float(saved.get("entry_price", 0.0))
            st.quantity     = int(saved.get("quantity", 0))
            st.ltf_sl_line  = float(saved.get("ltf_sl_line", 0.0))
            st.target_high  = float(saved.get("target_high", 0.0))
            st.entry_origin = float(saved.get("entry_origin", 0.0))
            st.phase        = _Phase.LIVE
            if st.trade_id:
                self._open_positions[st.trade_id] = (
                    st.trade_id, saved.get("option_symbol", ""),
                    st.entry_price, st.quantity,
                )
                logger.info(
                    "TrapEngine[%s]: restored LIVE trade %s entry=%.2f qty=%d sl=%.2f target=%.2f",
                    symbol, st.trade_id, st.entry_price, st.quantity,
                    st.ltf_sl_line, st.target_high,
                )
        except Exception as exc:
            logger.warning("TrapEngine: restore failed for %s: %s", symbol, exc)

    # ── Warm start ────────────────────────────────────────────────────────────

    async def warm_start(self, symbols: List[str]) -> None:
        """
        Replay historical 1-min bars from DB to restore HTF/MTF state
        without waiting for live bars to build up.
        """
        # Restore any LIVE trade per symbol FIRST — independent of client_db, so a
        # running Trap position survives a restart even if bar replay is skipped.
        for sym in symbols:
            self._restore_trade(sym)

        # v2: lock each symbol's day strikes (prev open-day high/low → ATM+DTE) and seed
        # the per-leg HTF detectors from each option's own history — all inside
        # _lock_day_strikes → _seed_leg_detection. No underlying-bar replay needed.
        # Scheduled in the BACKGROUND so startup never blocks on the historical API.
        for sym in symbols:
            asyncio.create_task(self._lock_day_strikes(sym))

    def _market_close_for(self, symbol: str):
        """EOD force-exit time for the symbol's exchange (MCX trades the evening
        session, so its close is ~23:30, not the NSE 15:30). `symbol` may be an
        underlying or a leg key (UNDERLYING:STRIKE:OPT)."""
        u = (str(symbol).split(":")[0] if symbol else "").upper()
        try:
            mcx = set(self._cfg.exchange.mcx_underlyings)
        except Exception:
            mcx = _MCX_SET
        return _MCX_MARKET_CLOSE if u in mcx else _MARKET_CLOSE

    def set_feeder(self, feeder) -> None:
        """Inject the GlobalFeeder so the trap can subscribe its tracked CE/PE strikes."""
        self._feeder = feeder

    def set_rebalancer(self, rebalancer) -> None:
        """Inject the StrikeRebalancer so the trap can PIN its deep-ITM tracked strikes
        (otherwise the ATM-window cleanup unsubscribes them and their LTP freezes)."""
        self._rebalancer = rebalancer

    async def _ensure_subscribed_legs(self, underlying: str) -> None:
        """Subscribe the day's tracked CE + PE (deep-ITM by DTE) so their premiums
        stream — these strikes are far from ATM and aren't covered by the SS/IC
        rebalancer subscriptions, so the trap must subscribe them itself."""
        if self._feeder is None:
            return
        sel = self._day_strikes.get(underlying)
        if sel is None:
            return
        try:
            from data_layer.instrument_registry import REGISTRY
            today = datetime.now(IST).date()
            expiry = REGISTRY.get_active_expiry(underlying, today)
            if expiry is None:
                exps = REGISTRY.all_expiries(underlying)
                expiry = next((e for e in exps if e >= today), None)
            if expiry is None:
                logger.warning("TrapEngine[%s]: no expiry to subscribe tracked legs.", underlying)
                return
            providers = ["upstox", "fyers"]
            if hasattr(self._feeder, "active_provider"):
                ap = self._feeder.active_provider
                if ap in ("fyers", "upstox"):
                    providers = [ap]
            tokens = []
            for strike, opt_type in ((sel.ce_strike, "CE"), (sel.pe_strike, "PE")):
                for provider in providers:
                    key = REGISTRY.get_broker_symbol(underlying, expiry, int(strike), opt_type, provider)
                    if key and key not in self._subscribed_keys:
                        tokens.append(key)
                        self._subscribed_keys.add(key)
            if tokens:
                await self._feeder.subscribe_tokens(tokens)
                self._tlog(underlying).info(
                    "subscribed tracked legs CE=%d PE=%d exp=%s (%d tokens, providers=%s)",
                    sel.ce_strike, sel.pe_strike, expiry, len(tokens), providers,
                )
            # Pin both tracked strikes so the ATM-window cleanup never unsubscribes
            # them (deep-ITM, far from ATM) — keeps their LTP live.
            if self._rebalancer is not None:
                try:
                    self._rebalancer.pin_strike(underlying, float(sel.ce_strike))
                    self._rebalancer.pin_strike(underlying, float(sel.pe_strike))
                    self._tlog(underlying).info("pinned tracked strikes %d, %d (cleanup-protected)",
                                                sel.ce_strike, sel.pe_strike)
                except Exception as exc:
                    logger.warning("TrapEngine[%s]: pin failed: %s", underlying, exc)
        except Exception as exc:
            logger.warning("TrapEngine[%s]: _ensure_subscribed_legs failed: %s", underlying, exc)

    def _dte(self, underlying: str) -> int:
        """Days-to-expiry from today to the active contract expiry (calendar days)."""
        try:
            from data_layer.instrument_registry import REGISTRY as _REG
            today = datetime.now(IST).date()
            exp = _REG.get_active_expiry(underlying, today)
            if exp is None:
                exps = _REG.all_expiries(underlying)
                exp = next((e for e in exps if e >= today), None)
            if exp is None:
                return 0
            return max((exp - today).days, 0)
        except Exception:
            return 0

    async def _lock_day_strikes(self, underlying: str) -> None:
        """Lock the day's tracked CE/PE strikes from the PREVIOUS open trading day's
        high/low. Source priority: (1) recorded 1m bars in the DB, else (2) the broker's
        HISTORICAL daily candle for the last open day (Upstox v2 historical-candle).
        The centre = (prev_high + prev_low)/2 drives the ITM CE/PE selection."""
        try:
            from datetime import timedelta
            now   = datetime.now(IST)
            today = now.date()
            # (1) Recorded 1m bars
            prior = []
            if self._client_db is not None:
                start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=10)
                rows = await asyncio.to_thread(
                    self._client_db.get_1m_bars_sync, underlying, start, now
                )
                prior = [r for r in (rows or []) if _row_date(r) is not None and _row_date(r) < today]
            if prior:
                target_day = max(_row_date(r) for r in prior)
                day_rows = [r for r in prior if _row_date(r) == target_day]
                prev_high = max(float(r["high"]) for r in day_rows)
                prev_low  = min(float(r["low"])  for r in day_rows)
                self._compute_day_strikes(underlying, prev_high, prev_low, source="db-prev-day")
                await self._ensure_subscribed_legs(underlying)
                await self._seed_legs_from_history(underlying)
                await self._seed_leg_detection(underlying)
                return
            # (2) Broker historical daily candle (last open day)
            hl = await self._fetch_prev_day_hl_upstox(underlying)
            if hl is not None:
                prev_high, prev_low = hl
                self._compute_day_strikes(underlying, prev_high, prev_low, source="upstox-historical")
                await self._ensure_subscribed_legs(underlying)
                await self._seed_legs_from_history(underlying)
                await self._seed_leg_detection(underlying)
                return
            logger.warning("TrapEngine[%s]: could not obtain prev open-day high/low "
                           "(no DB bars, no historical candle).", underlying)
        except Exception as exc:
            logger.warning("TrapEngine[%s]: _lock_day_strikes failed: %s", underlying, exc)

    async def _upstox_candles(self, ikey: str, interval: str, days_back: int):
        """Fetch Upstox v2 historical candles for any instrument_key. Returns the candle
        list (newest-first) or []. Uses curl_cffi Chrome impersonation (Upstox edge 403s
        plain urllib). Each candle: [ts, open, high, low, close, volume, oi]."""
        if not ikey:
            return []
        try:
            import json as _json
            import urllib.parse, urllib.request
            from datetime import timedelta
            token = ""
            if self._client_db is not None:
                creds = await asyncio.to_thread(self._client_db.get_feeder_creds_sync, "upstox")
                token = (creds or {}).get("access_token", "")
            today = datetime.now(IST).date()
            to_d  = today.isoformat()
            from_d = (today - timedelta(days=days_back)).isoformat()
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{urllib.parse.quote(ikey, safe='')}/{interval}/{to_d}/{from_d}")
            headers = {
                "Accept": "application/json",
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/140.0.0.0 Safari/537.36"),
            }
            if token:
                headers["Authorization"] = f"Bearer {token}"

            def _get():
                try:
                    from curl_cffi import requests as _cc
                    return _cc.get(url, headers=headers, impersonate="chrome131", timeout=8).json()
                except ImportError:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=8) as r:
                        return _json.loads(r.read().decode("utf-8"))

            data = await asyncio.to_thread(_get)
            return ((data or {}).get("data") or {}).get("candles") or []
        except Exception as exc:
            logger.warning("TrapEngine: upstox candles failed (key=%s): %s", ikey, exc)
            return []

    async def _fetch_prev_day_hl_upstox(self, underlying: str):
        """LAST open trading day's high/low from the underlying's daily candles."""
        from data_layer.instrument_registry import REGISTRY as _REG
        ikey = _REG.historical_instrument_key(underlying)
        if not ikey:
            logger.warning("TrapEngine[%s]: no historical instrument_key.", underlying)
            return None
        logger.info("TrapEngine[%s]: historical fetch using key=%s", underlying, ikey)
        candles = await self._upstox_candles(ikey, "day", 10)
        today = datetime.now(IST).date().isoformat()
        for c in candles:                       # newest-first
            if str(c[0])[:10] < today:          # strictly before today = last open day
                return float(c[2]), float(c[3])
        if candles:
            return float(candles[0][2]), float(candles[0][3])
        logger.warning("TrapEngine[%s]: historical returned no candles (key=%s).", underlying, ikey)
        return None

    @staticmethod
    def _leg_key(underlying: str, strike: int, opt: str) -> str:
        """Canonical per-leg detection key (state is keyed by this, not the underlying)."""
        return f"{underlying}:{int(strike)}:{opt}"

    async def _seed_leg_detection(self, underlying: str) -> None:
        """Seed per-leg HTF/MTF trap state from each tracked option's INTRADAY 1-min
        history (Upstox), so rolling_base / trap_levels populate immediately and the
        HTF/MTF trap appears without waiting hours for live candles to build."""
        from data_layer.instrument_registry import REGISTRY as _REG
        sel = self._day_strikes.get(underlying)
        if sel is None:
            return
        today = datetime.now(IST).date()
        expiry = _REG.get_active_expiry(underlying, today)
        if expiry is None:
            expiry = next((e for e in _REG.all_expiries(underlying) if e >= today), None)
        if expiry is None:
            return
        tc = self._cfg.trap_engine
        lookback = self._lookback_days(underlying)
        import pandas as pd
        for strike, opt in ((sel.ce_strike, "CE"), (sel.pe_strike, "PE")):
            okey = _REG.get_upstox_key(underlying, expiry, int(strike), opt)
            if not okey:
                continue
            candles = await self._upstox_candles(okey, "1minute", lookback)
            if not candles:
                self._tlog(underlying).info("no intraday history to seed %d%s detection", strike, opt)
                continue
            leg_key = self._leg_key(underlying, strike, opt)
            rows = [{"timestamp": c[0], "open": float(c[1]), "high": float(c[2]),
                     "low": float(c[3]), "close": float(c[4]),
                     "volume": float(c[5] or 0)} for c in reversed(candles)]  # oldest-first
            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp").sort_index()
            agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
            _origin = df.index[0].normalize().replace(hour=9, minute=15, second=0)
            # Seed the HTF detector by replaying each HTF candle: feed its intrabar price
            # path as ticks (testing the PRIOR reference), then register it as a new level.
            htf = self._det(leg_key, "htf")
            res = df.resample(f"{tc.HTF_MINUTES}min", closed="left", label="right",
                              origin=_origin).agg(agg).dropna()
            for _ts, r in res.iterrows():
                o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
                htf.on_tick(o)
                if c < o:                # bearish: high then low
                    htf.on_tick(h); htf.on_tick(l)
                else:                    # bullish: low then high
                    htf.on_tick(l); htf.on_tick(h)
                htf.on_tick(c)
                htf.on_candle({"open": o, "high": h, "low": l, "close": c})
            lv = htf.active_level
            self._tlog(underlying).info(
                "seeded %d%s HTF detection: state=%s%s",
                strike, opt, htf.state.name,
                (f" level L={lv.entry_l:.2f}/H={lv.sl_h:.2f}" if lv else ""))

    async def _seed_legs_from_history(self, underlying: str) -> None:
        """Seed the tracked CE/PE LTP from each option's latest historical close so the
        panel shows a value immediately (live ticks update it once they arrive). This is
        also where per-leg HTF/MTF seeding will hook in."""
        from data_layer.instrument_registry import REGISTRY as _REG
        sel = self._day_strikes.get(underlying)
        if sel is None:
            return
        today = datetime.now(IST).date()
        expiry = _REG.get_active_expiry(underlying, today)
        if expiry is None:
            expiry = next((e for e in _REG.all_expiries(underlying) if e >= today), None)
        if expiry is None:
            return
        for strike, opt in ((sel.ce_strike, "CE"), (sel.pe_strike, "PE")):
            okey = _REG.get_upstox_key(underlying, expiry, int(strike), opt)
            if not okey:
                logger.warning("TrapEngine[%s]: no upstox option key for %d%s exp=%s",
                               underlying, strike, opt, expiry)
                continue
            candles = await self._upstox_candles(okey, "day", 5)
            if candles:
                close = float(candles[0][4])    # newest close
                self._leg_prem[(underlying, int(strike), opt)] = close
                self._tlog(underlying).info(
                    "seeded %d%s LTP=%.2f from historical (live ticks will update)",
                    strike, opt, close)

    # ── v2 settings helpers ───────────────────────────────────────────────────
    def _tt_cfg(self, underlying: str) -> dict:
        try:
            from data_layer.runtime_config import RuntimeConfig
            return RuntimeConfig.index_section(underlying, "trap_trading") or {}
        except Exception:
            return {}

    def _dte_offset_steps_from_cfg(self, underlying: str, dte: int) -> int:
        """Resolve the DTE→ITM step offset from the configured `dte_offset_ladder`
        (JSON string keys, e.g. {"5":5,...}). Returns the value for the HIGHEST
        threshold k where dte > k; else 0. Falls back to min(max(dte-1,0),5)."""
        ladder = self._tt_cfg(underlying).get("dte_offset_ladder") or {}
        if not ladder:
            return min(max(int(dte) - 1, 0), 5)
        best_k, best_v = -1, 0
        for k, v in ladder.items():
            try:
                thr = int(k)
            except (TypeError, ValueError):
                continue
            if int(dte) > thr and thr > best_k:
                best_k, best_v = thr, int(v)
        return best_v

    def _lookback_days(self, underlying: str) -> int:
        try:
            n = int(self._tt_cfg(underlying).get("lookback_days", 2))
        except (TypeError, ValueError):
            n = 2
        return max(n, 2)

    def _det(self, leg_key: str, kind: str) -> SellerTrapDetector:
        """Lazily create/return the per-leg detector ('htf' or 'mtf')."""
        store = self._htf_det if kind == "htf" else self._mtf_det
        if leg_key not in store:
            store[leg_key] = SellerTrapDetector()
        return store[leg_key]

    def _compute_day_strikes(self, underlying: str, prev_high: float, prev_low: float,
                             source: str = "prev-day") -> None:
        """Lock the day's tracked CE/PE strikes from the (prev-day or seeded) high/low + DTE,
        using the configured DTE offset ladder + round-off step."""
        try:
            from strategies.trap_strike_selection import TrapStrikes
            step = float(self._cfg.exchange.strike_steps.get(underlying, 50.0))
            dte  = self._dte(underlying)
            atm  = int(round(((float(prev_high) + float(prev_low)) / 2.0) / step) * step)
            steps = self._dte_offset_steps_from_cfg(underlying, dte)
            offset_pts = int(steps * step)
            sel = TrapStrikes(atm=atm, ce_strike=atm - offset_pts, pe_strike=atm + offset_pts,
                              offset_steps=steps, offset_pts=offset_pts, dte=int(dte))
            self._day_strikes[underlying] = sel
            self._tlog(underlying).info(
                "DAY STRIKES locked [%s]: high=%.2f low=%.2f ATM=%d DTE=%d "
                "offset=%d (%d steps) | track CE=%d PE=%d",
                source, prev_high, prev_low, sel.atm, sel.dte, sel.offset_pts,
                sel.offset_steps, sel.ce_strike, sel.pe_strike,
            )
        except Exception as exc:
            logger.warning("TrapEngine[%s]: _compute_day_strikes failed: %s", underlying, exc)

    # ── Event loops ───────────────────────────────────────────────────────────

    async def _candle_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._candle_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(event, CandleEvent):
                continue
            try:
                await self._on_candle(event)
            except Exception as exc:
                logger.exception(
                    "TrapTradingEngine: candle error [%s] TF%d: %s",
                    event.symbol, event.timeframe, exc,
                )

    async def _index_tick_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._index_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(event, IndexTick):
                continue
            # Store real underlying spot price (index LTP, e.g. 24500 for NIFTY)
            self._spot_cache[event.symbol] = event.ltp
            # Lazily lock day strikes if not set yet (DB prev-day, else broker
            # historical). Throttled so we don't hammer the API/DB every tick.
            if event.symbol not in self._day_strikes:
                import time as _t
                if not hasattr(self, "_lock_try"):
                    self._lock_try = {}
                if _t.monotonic() - self._lock_try.get(event.symbol, 0.0) > 60.0:
                    self._lock_try[event.symbol] = _t.monotonic()
                    # Background: never block tick draining on the HTTP/DB lookup.
                    asyncio.create_task(self._lock_day_strikes(event.symbol))
            # Live heartbeat driven by ticks (not candles), so the per-symbol log
            # is created immediately and shows what the engine sees every minute —
            # even between 5m/75m candle closes.
            self._heartbeat(event.symbol, event.ltp)

    async def _option_tick_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._opt_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(event, OptionTick):
                continue
            try:
                # Update option premium cache only — spot comes from INDEX_TICK
                self._prem_cache[event.symbol] = event.ltp
                try:
                    self._leg_prem[(event.underlying, int(event.strike), event.option_type)] = event.ltp
                    # If this tick is one of the day's tracked legs, build its live
                    # HTF/MTF candles so detection progresses intraday on the PREMIUM.
                    sel = self._day_strikes.get(event.underlying)
                    strike = int(event.strike)
                    if sel is not None and (
                        (strike == sel.ce_strike and event.option_type == "CE") or
                        (strike == sel.pe_strike and event.option_type == "PE")):
                        leg_key = self._leg_key(event.underlying, strike, event.option_type)
                        self._feed_leg_tick(leg_key, event.timestamp, event.ltp)
                    # If this tick is the executed position's contract, run the two-tier SL.
                    pos = self._v2_position
                    if (pos is not None and event.underlying == pos["underlying"]
                            and int(event.strike) == pos["strike"]
                            and event.option_type == pos["opt_type"]):
                        self._v2_track_exec_tick(event.timestamp, event.ltp)
                except Exception:
                    pass

                await self._check_touch_trigger(event)
            except Exception as exc:
                logger.exception(
                    "TrapTradingEngine: option tick error [%s]: %s",
                    event.symbol, exc,
                )

    def _feed_leg_tick(self, leg_key: str, ts, ltp: float) -> None:
        """Drive the per-leg seller-trap detectors from a premium tick. The HTF detector
        is always advanced; the MTF detector is advanced ONLY while HTF is ENTRY_READY
        (the nested gate). On MTF entry-ready → fire the (stubbed) entry signal."""
        tc = self._cfg.trap_engine
        try:
            base = ts.replace(second=0, microsecond=0)
        except Exception:
            base = datetime.now(IST).replace(second=0, microsecond=0)

        htf = self._det(leg_key, "htf")
        mtf = self._det(leg_key, "mtf")
        htf_armed = (htf.state == _DetState.ENTRY_READY)

        # Price-path detection (Below→Above→Return) on each tick, logging transitions.
        _p = htf.state
        htf.on_tick(ltp)
        self._log_leg_transition(leg_key, "HTF", htf, _p, ltp)
        if htf_armed:
            _p = mtf.state
            mtf.on_tick(ltp)
            self._log_leg_transition(leg_key, "MTF", mtf, _p, ltp)
            if mtf.entry_ready:
                self._on_mtf_entry_signal(leg_key, ltp)
                mtf.consume_entry()

        # Build the leg's HTF/MTF candles; on bucket close feed det.on_candle().
        # MTF candles are only fed while the HTF gate is open.
        for tf, det, gated in ((tc.HTF_MINUTES, htf, False), (tc.MTF_MINUTES, mtf, True)):
            minute = base.hour * 60 + base.minute
            floored = minute - (minute % tf)
            bstart = base.replace(hour=floored // 60, minute=floored % 60)
            key = (leg_key, tf)
            cur = self._leg_bars.get(key)
            if cur is None or cur["start"] != bstart:
                if cur is not None and ((not gated) or htf_armed):
                    _p = det.state
                    det.on_candle({"open": cur["o"], "high": cur["h"],
                                   "low": cur["l"], "close": cur["c"]})
                    self._log_leg_transition(leg_key, "HTF" if det is htf else "MTF",
                                             det, _p, cur["c"])
                self._leg_bars[key] = {"start": bstart, "o": ltp, "h": ltp, "l": ltp, "c": ltp}
            else:
                cur["h"] = max(cur["h"], ltp)
                cur["l"] = min(cur["l"], ltp)
                cur["c"] = ltp

    def _log_leg_transition(self, leg_key: str, tf_label: str,
                            det: SellerTrapDetector, prev_state, price: float) -> None:
        """Log a per-leg detector state change with the price + active level, so the
        dry-run shows the exact Below→Above→Return moments."""
        if det.state == prev_state:
            return
        parts = leg_key.split(":")
        underlying = parts[0] if parts else leg_key
        strike = parts[1] if len(parts) > 1 else "?"
        opt = parts[2] if len(parts) > 2 else "?"
        lv = det.active_level
        l = float(lv.entry_l) if lv is not None else 0.0
        h = float(lv.sl_h) if lv is not None else 0.0
        self._tlog(underlying).info(
            trap_transition_msg(opt, strike, tf_label, det.state.name, l, h, float(price)))

    def _on_mtf_entry_signal(self, leg_key: str, ltp: float) -> None:
        """Nested HTF→MTF seller trap completed for this leg → fire the BUY entry.
        Scheduled as a task because order placement is async and this runs from the
        sync tick path."""
        parts = leg_key.split(":")
        underlying = parts[0] if parts else leg_key
        opt = parts[2] if len(parts) > 2 else "?"
        self._tlog(underlying).info("MTF ENTRY SIGNAL leg=%s type=%s ltp=%.2f", leg_key, opt, ltp)
        try:
            asyncio.create_task(self._fire_entry_v2(underlying, opt, float(ltp)))
        except RuntimeError:
            # No running loop (e.g. unit context) — caller may invoke _fire_entry_v2 directly.
            pass

    def _build_entry_payload(self, underlying: str, opt_type: str) -> Optional[dict]:
        """Resolve the fresh ATM±buy_depth execution strike from the live spot/future.
        Returns the order payload, or None if spot is unknown."""
        spot = float(self._spot_cache.get(underlying, 0.0) or 0.0)
        if spot <= 0.0:
            return None
        step = float(self._cfg.exchange.strike_steps.get(underlying, 50.0))
        buy_depth = int(self._tt_cfg(underlying).get("buy_depth", 0) or 0)
        strike = exec_strike(spot, opt_type, buy_depth, step)
        lot = int(self._cfg.exchange.lot_sizes.get(underlying, 1) or 1)
        return {"underlying": underlying, "side": "BUY", "opt_type": opt_type,
                "strike": int(strike), "qty": int(lot), "spot": spot}

    async def _fire_entry_v2(self, underlying: str, opt_type: str, entry_premium: float) -> None:
        """Execute a BUY of the fresh ATM±buy_depth strike and record the position.
        One position at a time; an OPPOSITE-leg signal rotates (close runner → open new),
        a SAME-leg signal is ignored."""
        if self._v2_position is not None:
            running = self._v2_position.get("opt_type")
            if not should_rotate(running, opt_type, True):
                return  # same side → ignore (no over-leveraging)
            # opposite side → close the runner immediately, then fall through to enter.
            pos = self._v2_position
            exit_prem = float(self._leg_prem.get(
                (pos["underlying"], pos["strike"], pos["opt_type"]), pos.get("entry_premium", 0.0)))
            self._tlog(underlying).info(
                "V2 ROTATION: opposite-leg signal — close %s %d (prem=%.2f), open %s",
                pos["opt_type"], pos["strike"], exit_prem, opt_type)
            self._v2_position = None   # clear runner + its SL state
            await self._v2_publish_exit(pos, exit_prem, "rotation")
        payload = self._build_entry_payload(underlying, opt_type)
        if payload is None:
            self._tlog(underlying).info("V2 ENTRY aborted: no live spot for %s", underlying)
            return
        from data_layer.instrument_registry import REGISTRY as _REG
        today = datetime.now(IST).date()
        expiry = _REG.get_active_expiry(underlying, today)
        if expiry is None:
            expiry = next((e for e in _REG.all_expiries(underlying) if e >= today), None)
        # Subscribe + pin the execution strike so its premium streams for management.
        try:
            if self._feeder is not None and expiry is not None:
                providers = ["upstox", "fyers"]
                if hasattr(self._feeder, "active_provider") and self._feeder.active_provider in ("upstox", "fyers"):
                    providers = [self._feeder.active_provider]
                toks = []
                for prov in providers:
                    k = _REG.get_broker_symbol(underlying, expiry, payload["strike"], opt_type, prov)
                    if k and k not in self._subscribed_keys:
                        toks.append(k); self._subscribed_keys.add(k)
                if toks:
                    await self._feeder.subscribe_tokens(toks)
                if self._rebalancer is not None:
                    self._rebalancer.pin_strike(underlying, float(payload["strike"]))
        except Exception as exc:
            logger.warning("TrapEngine[%s]: v2 subscribe/pin failed: %s", underlying, exc)

        # Publish the BUY signal (same SIGNAL path the engine already uses).
        try:
            from strategies.base_strategy import Direction, SignalPackage, StrategyID
            sig = SignalPackage(
                source=StrategyID.TRAP_ENGINE, direction=Direction.LONG,
                underlying=underlying, option_type=opt_type,
                target_strike=payload["strike"], entry_spot=payload["spot"],
                stop_spot=0.0, target_spot=0.0, confidence=1.0,
                timestamp=datetime.now(IST),
                notes=f"TTv2 BUY {opt_type} {payload['strike']} qty={payload['qty']} prem={entry_premium:.2f}",
            )
            await self._bus.publish(Topic.SIGNAL, sig)
        except Exception as exc:
            logger.warning("TrapEngine[%s]: v2 signal publish failed: %s", underlying, exc)

        self._v2_position = {
            **payload, "entry_premium": float(entry_premium), "expiry": expiry,
            "ts": datetime.now(IST),
            # SL fields: sl_5m starts at the entry premium and trails down to the entry
            # 5m candle low while inside that bucket (see _v2_track_exec_tick); sl_active
            # is the tier-2 1-min low once a 1m closes below sl_5m.
            "sl_5m": float(entry_premium), "sl_active": None,
            "entry_bucket": None, "_m1": None,
        }
        self._tlog(underlying).info(
            "V2 ENTRY BUY %s %d qty=%d @ prem=%.2f (spot=%.2f exp=%s)",
            opt_type, payload["strike"], payload["qty"], entry_premium, payload["spot"], expiry)

    # ── v2 two-tier stop loss ─────────────────────────────────────────────────
    def _v2_check_sl(self, ltp: float) -> bool:
        pos = self._v2_position
        if pos is None:
            return False
        return sl_triggered(ltp, pos.get("sl_5m", 0.0), pos.get("sl_active"))

    def _v2_update_sl_on_1m_close(self, low: float, close: float) -> None:
        """Tier-2 activation: if a 1-min candle CLOSES below the (fixed) 5m entry-candle
        low, that 1-min candle's low becomes the active stop."""
        pos = self._v2_position
        if pos is None:
            return
        if float(close) < float(pos.get("sl_5m", 0.0)):
            pos["sl_active"] = float(low)

    def _v2_maybe_stop(self, ltp: float) -> bool:
        """If the stop is hit, clear the position cleanly and (best-effort) publish the
        exit. Returns True when an exit fired."""
        pos = self._v2_position
        if pos is None or not self._v2_check_sl(ltp):
            return False
        self._tlog(pos["underlying"]).info(
            "V2 SL HIT %s %d ltp=%.2f sl_5m=%.2f sl_active=%s — EXIT",
            pos["opt_type"], pos["strike"], ltp, pos.get("sl_5m", 0.0), pos.get("sl_active"))
        self._v2_position = None   # clear stored position cleanly
        try:
            asyncio.create_task(self._v2_publish_exit(pos, ltp, "sl"))
        except RuntimeError:
            pass
        return True

    async def _v2_publish_exit(self, pos: dict, exit_premium: float, reason: str) -> None:
        try:
            from strategies.base_strategy import Direction, SignalPackage, StrategyID
            sig = SignalPackage(
                source=StrategyID.TRAP_ENGINE, direction=Direction.SHORT,  # SELL to close the long
                underlying=pos["underlying"], option_type=pos["opt_type"],
                target_strike=pos["strike"], entry_spot=pos.get("spot", 0.0),
                stop_spot=0.0, target_spot=0.0, confidence=1.0,
                timestamp=datetime.now(IST),
                notes=f"TTv2 EXIT {reason} {pos['opt_type']} {pos['strike']} prem={exit_premium:.2f}",
            )
            await self._bus.publish(Topic.SIGNAL, sig)
        except Exception as exc:
            logger.warning("TrapEngine: v2 exit publish failed: %s", exc)

    def _v2_track_exec_tick(self, ts, ltp: float) -> None:
        """Maintain the executed contract's entry-5m-candle low (sl_5m, frozen after the
        entry bucket) and its 1-min candles (tier-2 activation), then evaluate the stop."""
        pos = self._v2_position
        if pos is None:
            return
        tc = self._cfg.trap_engine
        try:
            base = ts.replace(second=0, microsecond=0)
        except Exception:
            base = datetime.now(IST).replace(second=0, microsecond=0)
        minute = base.hour * 60 + base.minute
        # 5m entry-candle low: while still inside the entry 5m bucket, trail sl_5m down to
        # the running low; once the bucket closes it stays frozen (the entry candle low).
        b5 = minute - (minute % tc.MTF_MINUTES)
        b5start = base.replace(hour=b5 // 60, minute=b5 % 60)
        if pos.get("entry_bucket") is None:
            pos["entry_bucket"] = b5start
        if b5start == pos["entry_bucket"]:
            pos["sl_5m"] = min(float(pos.get("sl_5m", ltp)), float(ltp))
        # 1-min candle of the executed contract.
        m1 = minute - (minute % tc.SL_MIN_MINUTES if hasattr(tc, "SL_MIN_MINUTES") else 1)
        m1start = base.replace(hour=m1 // 60, minute=m1 % 60)
        cur = pos.get("_m1")
        if cur is None or cur["start"] != m1start:
            if cur is not None:
                self._v2_update_sl_on_1m_close(cur["l"], cur["c"])
            pos["_m1"] = {"start": m1start, "o": ltp, "h": ltp, "l": ltp, "c": ltp}
        else:
            cur["h"] = max(cur["h"], ltp); cur["l"] = min(cur["l"], ltp); cur["c"] = ltp
        self._v2_maybe_stop(ltp)

    # ── Candle router ─────────────────────────────────────────────────────────

    async def _on_candle(self, c: CandleEvent) -> None:
        # v2: per-leg detection runs in _feed_leg_tick (on the option premium), not on
        # the underlying CANDLE_CLOSE. This handler only enforces the MCX-aware EOD guard.
        if datetime.now(IST).time() >= self._market_close_for(c.symbol):
            await self._force_exit_all("EOD")
            return

    def _tlog(self, symbol: str) -> logging.Logger:
        lg = self._clogs.get(symbol)
        if lg is None:
            lg = _make_trap_logger(symbol)
            self._clogs[symbol] = lg
        return lg

    def _heartbeat(self, symbol: str, spot: float) -> None:
        """Tick-driven heartbeat. Creates the per-symbol log on first tick and
        emits one line/minute showing exactly what the engine sees — so the log
        is never blank, even before any 5m/75m candle has closed."""
        import time as _t
        if not hasattr(self, "_last_hb"):
            self._last_hb = {}
        if _t.monotonic() - self._last_hb.get(symbol, 0.0) < 60.0:
            return
        self._last_hb[symbol] = _t.monotonic()
        try:
            sel = self._day_strikes.get(symbol)
            if sel is None:
                self._tlog(symbol).info("heartbeat (awaiting day-strikes) spot=%.2f", spot)
                return
            # Per-leg view: each CE/PE leg has its own HTF + MTF seller-trap detector.
            def _leg(strike, opt):
                ltp = self._leg_prem.get((symbol, strike, opt), 0.0)
                lk = self._leg_key(symbol, strike, opt)
                h = self._htf_det.get(lk)
                m = self._mtf_det.get(lk)
                hs = h.state.name if h else "—"
                ms = m.state.name if m else "—"
                lvl = ""
                if h is not None and h.active_level is not None:
                    lvl = f" [L={h.active_level.entry_l:.2f} H={h.active_level.sl_h:.2f}]"
                return f"{opt} {strike}={ltp:.2f} HTF={hs} MTF={ms}{lvl}"
            self._tlog(symbol).info(
                "heartbeat DTE=%d | %s | %s",
                sel.dte, _leg(sel.ce_strike, "CE"), _leg(sel.pe_strike, "PE"),
            )
        except Exception as exc:
            self._tlog(symbol).info("heartbeat spot=%.2f (state warming up: %s)", spot, exc)


    # ── Stage 3 + Stage 5: option tick handler ────────────────────────────────

    async def _check_touch_trigger(self, tick: OptionTick) -> None:
        """v2: entry detection moved to the per-leg seller-trap detectors driven from
        `_feed_leg_tick`. This handler now only enforces the MCX-aware EOD guard.
        (Stage-3/5 legacy entry logic removed.)"""
        if datetime.now(IST).time() >= self._market_close_for(tick.underlying):
            await self._force_exit_all("EOD")
            return

    # ── LTF (1-min) exit guard ────────────────────────────────────────────────

    async def _fire_entry(
        self,
        underlying: str,
        option_symbol: str,
        entry_price: float,
        st: _TrapState,
    ) -> None:
        """Stage 5 execution — allocate quantity per client and fire orders."""
        tc = self._cfg.trap_engine
        cutoff_str = tc.ENTRY_CUTOFF_TIME
        try:
            h, m = int(cutoff_str[:2]), int(cutoff_str[3:5])
            cutoff = time(h, m, 0)
        except Exception:
            cutoff = time(14, 45, 0)
        if datetime.now(IST).time() >= cutoff:
            logger.info(
                "TrapEngine [%s] entry blocked — past ENTRY_CUTOFF_TIME %s",
                underlying, cutoff_str,
            )
            return

        if st.is_backtest:
            self._record_backtest_entry(underlying, option_symbol, entry_price, st)
            return

        tc  = self._cfg.trap_engine
        lot = self._cfg.exchange.lot_sizes.get(underlying, 75)

        # Get active clients
        active_clients: List[dict] = []
        if self._client_db is not None:
            try:
                active_clients = await asyncio.to_thread(
                    self._client_db.get_all_clients_sync
                )
                active_clients = [c for c in active_clients
                                  if c.get("is_admin_approved") and c.get("is_active")]
            except Exception as exc:
                logger.error("TrapTradingEngine _fire_entry: client fetch error: %s", exc)

        trade_id = str(uuid.uuid4())[:8]
        total_qty = 0

        for client in active_clients:
            capital = float(client.get("capital", 0.0))
            if capital <= 0 or entry_price <= 0:
                continue
            raw_qty = math.floor(capital / (entry_price * lot)) * lot
            qty = max(raw_qty, lot)  # minimum 1 lot
            total_qty += qty

        # Even if no clients, record the position internally (paper/demo)
        if total_qty == 0:
            total_qty = lot  # default 1 lot for demo

        # Set SL based on configured mode — always below entry price.
        # For single-candle traps htf_trap_low (HTF candle low) is preferred over
        # mtf_sweep_low because the MTF candle is the same bar and its low equals
        # ltf_entry_line — giving zero room. The HTF low is the natural invalidation.
        sl_mode = tc.SL_MODE
        structural_ref = (
            st.htf_trap_low  if st.htf_trap_low > 0.0 else st.mtf_sweep_low
        )
        if sl_mode == "structural" and structural_ref > 0.0:
            computed_sl = structural_ref * (1.0 - tc.SL_BUFFER_PCT / 100.0)
            if computed_sl >= entry_price:
                computed_sl = entry_price * (1.0 - tc.SL_PCT / 100.0)
                logger.warning(
                    "TrapEngine [%s] structural SL %.2f >= entry %.2f — "
                    "falling back to dynamic SL %.2f",
                    underlying, structural_ref, entry_price, computed_sl,
                )
        else:
            computed_sl = entry_price * (1.0 - tc.SL_PCT / 100.0)
        st.ltf_sl_line = computed_sl

        st.trade_id    = trade_id
        st.entry_price = entry_price
        st.quantity    = total_qty
        st.phase       = _Phase.LIVE

        self._open_positions[trade_id] = (
            trade_id, option_symbol, entry_price, total_qty
        )
        self._persist_trade(underlying)   # survive restarts

        self._signals += 1
        logger.info(
            "TrapTradingEngine ENTRY #%d | trade_id=%s | %s | %s "
            "entry=%.2f qty=%d",
            self._signals, trade_id, underlying, option_symbol,
            entry_price, total_qty,
        )
        # Per-client log file
        try:
            from run_system import get_client_logger
            for client in active_clients:
                cid = client.get("client_id", "unknown")
                cl = get_client_logger(cid, f"trap_{underlying}")
                cl.info(
                    "ENTRY trade_id=%s symbol=%s entry=%.2f qty=%d "
                    "sl=%.2f target=%.2f entry_origin=%.2f",
                    trade_id, option_symbol, entry_price, total_qty,
                    computed_sl, st.target_high, st.entry_origin,
                )
        except Exception:
            pass

        # Publish to SIGNAL topic
        from strategies.base_strategy import Direction, SignalPackage, StrategyID
        signal = SignalPackage(
            source        = StrategyID.TRAP_ENGINE,
            direction     = Direction.LONG,
            underlying    = underlying,
            option_type   = "CE",
            target_strike = self._select_itm_strike(underlying),
            entry_spot    = self._spot_cache.get(underlying, entry_price),
            stop_spot     = st.ltf_sl_line,
            target_spot   = st.target_high,
            confidence    = self._confidence(st),
            timestamp     = datetime.now(IST),
            notes         = (
                f"NewTrap ENTRY | trade_id={trade_id} "
                f"entry_origin={st.entry_origin:.2f} "
                f"ltf_entry={st.ltf_entry_line:.2f} "
                f"ltf_sl={st.ltf_sl_line:.2f} "
                f"target={st.target_high:.2f} "
                f"rolling_base={st.rolling_base:.2f}"
            ),
        )
        await self._bus.publish(Topic.SIGNAL, signal)

    async def _fire_exit(
        self,
        trade_id: Optional[str],
        exit_price: float,
        reason: str,
    ) -> None:
        """Fire exit for a specific trade_id."""
        if trade_id is None or trade_id not in self._open_positions:
            logger.warning("TrapEngine _fire_exit: unknown trade_id=%s", trade_id)
            return

        pos = self._open_positions.pop(trade_id)
        _, opt_sym, entry_price, qty = pos

        pnl = (exit_price - entry_price) * qty

        # Find symbol for state reset
        underlying = None
        for sym, st in self._states.items():
            if st.trade_id == trade_id:
                underlying = sym
                break

        if underlying:
            st = self._states[underlying]
            rb          = st.rolling_base
            trap_levels = st.trap_levels      # preserve for level-aware SL handling
            act_level   = st.active_level
            self._reset_state(underlying)
            self._states[underlying].rolling_base = rb
            # Restore trap levels so _reset_to_next_level can use them
            self._states[underlying].trap_levels  = trap_levels
            self._states[underlying].active_level = act_level
            self._clear_trade(underlying)   # position closed → drop persisted trade

        logger.info(
            "TrapTradingEngine EXIT | trade_id=%s | %s | reason=%s "
            "entry=%.2f exit=%.2f qty=%d pnl=%.2f",
            trade_id, opt_sym, reason,
            entry_price, exit_price, qty, pnl,
        )
        # Per-client log file
        try:
            from run_system import get_client_logger
            if self._client_db is not None:
                clients = await asyncio.to_thread(self._client_db.get_all_clients_sync)
                for client in clients:
                    if client.get("is_admin_approved") and client.get("is_active"):
                        cid = client.get("client_id", "unknown")
                        sym_part = opt_sym.split("|")[-1] if "|" in opt_sym else opt_sym
                        cl = get_client_logger(cid, f"trap_{underlying or sym_part}")
                        cl.info(
                            "EXIT trade_id=%s reason=%s entry=%.2f exit=%.2f qty=%d pnl=%.2f",
                            trade_id, reason, entry_price, exit_price, qty, pnl,
                        )
        except Exception:
            pass

    async def _force_exit_all(self, reason: str) -> None:
        """Force-exit all LIVE positions (EOD or kill switch)."""
        live_ids = [
            st.trade_id
            for st in self._states.values()
            if st.phase == _Phase.LIVE and st.trade_id is not None
        ]
        for trade_id in live_ids:
            if trade_id in self._open_positions:
                _, opt_sym, _, _ = self._open_positions[trade_id]
                exit_price = self._prem_cache.get(opt_sym, 0.0)
                await self._fire_exit(trade_id, exit_price, reason)

    # ── Backtest recording ────────────────────────────────────────────────────

    def _record_backtest_entry(
        self,
        underlying: str,
        option_symbol: str,
        entry_price: float,
        st: _TrapState,
    ) -> None:
        tc = self._cfg.trap_engine
        cutoff_str = tc.ENTRY_CUTOFF_TIME
        try:
            h, m = int(cutoff_str[:2]), int(cutoff_str[3:5])
            cutoff = time(h, m, 0)
        except Exception:
            cutoff = time(14, 45, 0)
        if datetime.now(IST).time() >= cutoff:
            logger.debug(
                "TrapEngine [%s][backtest] entry blocked — past ENTRY_CUTOFF_TIME %s",
                underlying, cutoff_str,
            )
            return
        qty = self._cfg.exchange.lot_sizes.get(underlying, 75)

        tc = self._cfg.trap_engine
        structural_ref = (
            st.htf_trap_low if st.htf_trap_low > 0.0 else st.mtf_sweep_low
        )
        if tc.SL_MODE == "structural" and structural_ref > 0.0:
            computed_sl = structural_ref * (1.0 - tc.SL_BUFFER_PCT / 100.0)
            if computed_sl >= entry_price:
                computed_sl = entry_price * (1.0 - tc.SL_PCT / 100.0)
        else:
            computed_sl = entry_price * (1.0 - tc.SL_PCT / 100.0)
        st.ltf_sl_line = computed_sl

        trade_id = str(uuid.uuid4())[:8]
        st.trade_id    = trade_id
        st.entry_price = entry_price
        st.quantity    = qty
        st.phase       = _Phase.LIVE

        self._open_positions[trade_id] = (
            trade_id, option_symbol, entry_price, qty
        )
        self._backtest_log.append({
            "trade_id":     trade_id,
            "underlying":   underlying,
            "option_symbol": option_symbol,
            "entry_price":  entry_price,
            "quantity":     qty,
            "entry_origin": st.entry_origin,
            "target_high":  st.target_high,
            "ltf_entry":    st.ltf_entry_line,
            "ltf_sl":       st.ltf_sl_line,
            "rolling_base": st.rolling_base,
            "timestamp":    datetime.now(IST).isoformat(),
        })
        self._signals += 1
        logger.debug(
            "TrapEngine [backtest] ENTRY trade_id=%s underlying=%s prem=%.2f",
            trade_id, underlying, entry_price,
        )

    # ── State helpers ─────────────────────────────────────────────────────────

    def _get_state(self, symbol: str) -> _TrapState:
        if symbol not in self._states:
            self._states[symbol] = _TrapState()
        return self._states[symbol]

    def _reset_state(self, symbol: str) -> None:
        """Reset state to IDLE, preserving rolling_base."""
        rb = self._states.get(symbol, _TrapState()).rolling_base
        self._states[symbol] = _TrapState()
        self._states[symbol].rolling_base = rb

    def _reset_to_next_level(self, symbol: str) -> None:
        """
        After a SL exit: disable the active level and check if any remaining
        trap levels can still be retested. If yes, stay TRAP_LOCKED watching
        the next-highest active level. If none remain, go IDLE.
        """
        st = self._states[symbol]
        if st.active_level:
            st.active_level.disabled = True
            st.active_level = None

        # Find next highest active level
        remaining = [lv for lv in st.trap_levels if lv.active]
        if remaining:
            # Already sorted highest→lowest, pick first
            next_lv = remaining[0]
            st.active_level  = None   # will be picked up on next retest tick
            st.entry_origin  = 0.0    # clear so retest check rescans
            st.target_high   = next_lv.target_high
            st.phase = _Phase.TRAP_LOCKED
            # Reset MTF state so Stage 4 starts fresh at the new level
            st.htf_trap_low     = 0.0
            st.mtf_bearish_open = 0.0
            st.mtf_bearish_high = 0.0
            st.mtf_bearish_low  = 0.0
            st.ltf_entry_line   = 0.0
            st.ltf_sl_line      = 0.0
            logger.info(
                "TrapEngine [%s] level disabled after SL — %d level(s) remaining, "
                "back to TRAP_LOCKED watching highest=%.2f",
                symbol, len(remaining), remaining[0].entry_origin,
            )
        else:
            self._reset_state(symbol)
            logger.info("TrapEngine [%s] all trap levels exhausted → IDLE", symbol)

    def _get_mtf_buf(self, key: str, capacity: int = 200) -> OHLCVBuffer:
        """Get (or create) the OHLCVBuffer for a given key (symbol or symbol+suffix)."""
        if key not in self._mtf_bufs:
            self._mtf_bufs[key] = OHLCVBuffer(capacity)
        return self._mtf_bufs[key]

    def _activate_all_pending_levels(
        self, symbol: str, sweep_high: float, ts
    ) -> None:
        """
        Stage 2 sweep fired. Stamp target_high on all pending levels,
        sort highest entry_origin first, and transition to TRAP_LOCKED.
        """
        st = self._states[symbol]
        for lv in st.pending_levels:
            lv.target_high = sweep_high
        # Sort highest entry_origin first (check closest to sweep first)
        st.trap_levels = sorted(
            st.pending_levels, key=lambda lv: lv.entry_origin, reverse=True
        )
        st.pending_levels = []
        st.active_level   = None
        st.entry_origin   = 0.0      # retest scan will populate from trap_levels
        st.target_high    = sweep_high
        st.htf_bearish_high = sweep_high
        st.phase = _Phase.TRAP_LOCKED
        logger.info(
            "TrapEngine [%s] Stage 2 TRAP_LOCKED — sweep_high=%.2f "
            "activated %d level(s): %s @ %s",
            symbol, sweep_high, len(st.trap_levels),
            [f"{lv.entry_origin:.0f}" for lv in st.trap_levels],
            ts.strftime("%H:%M") if hasattr(ts, "strftime") else str(ts)[:5],
        )

    # ── Strike helpers ────────────────────────────────────────────────────────

    def _atm_strike(self, underlying: str) -> float:
        spot = self._spot_cache.get(underlying, 0.0)
        step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        if spot <= 0.0:
            return 0.0
        return round(spot / step) * step

    def _select_itm_strike(
        self, underlying: str, direction: str = "bearish"
    ) -> float:
        """ITM strike to track. Prefer the day-locked strikes from prev-day ATM +
        DTE (CE = ITM call below ATM, PE = ITM put above ATM). Falls back to the
        legacy live-spot weekday offset only if day strikes were not computed."""
        sel = self._day_strikes.get(underlying)
        if sel is not None:
            return float(sel.pe_strike if direction == "bullish" else sel.ce_strike)
        # Fallback (no warm-start data): legacy live-spot weekday offset
        spot = self._spot_cache.get(underlying, 0.0)
        if spot <= 0.0:
            return self._atm_strike(underlying)
        step   = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        offset = _DOW_OFFSET.get(datetime.now(IST).weekday(), 200)
        raw    = spot - offset if direction == "bearish" else spot + offset
        return round(raw / step) * step

    # ── Confidence score ──────────────────────────────────────────────────────

    def _confidence(self, st: _TrapState) -> float:
        """
        Simple confidence heuristic based on how many stages completed cleanly.
        Returns a score in [0.5, 1.0].
        """
        score = 0.50
        if st.entry_origin > 0.0:
            score += 0.10  # stage 1+2 confirmed
        if st.ltf_entry_line > 0.0:
            score += 0.20  # stage 4 nested trap confirmed
        if st.rolling_base > 0.0:
            score += 0.10  # rolling base active
        if st.target_high > 0.0 and st.ltf_sl_line > 0.0:
            # Simple RR estimate
            risk   = abs(st.ltf_entry_line - st.ltf_sl_line)
            reward = abs(st.target_high - st.ltf_entry_line)
            if risk > 0 and reward / risk >= 2.0:
                score += 0.10
        return min(score, 1.0)

    # ── Public telemetry / stats ──────────────────────────────────────────────

    def signal_count(self) -> int:
        return self._signals

    def state_snapshot(self) -> Dict[str, str]:
        return {sym: st.phase.name for sym, st in self._states.items()}

    def backtest_log(self) -> List[dict]:
        return list(self._backtest_log)

    def telemetry_snapshot(self) -> dict:
        """Return per-symbol telemetry dict for the admin dashboard endpoint."""
        out: dict = {}
        for sym, st in self._states.items():
            # Get current premium from cache if LIVE
            current_prem = 0.0
            if st.phase == _Phase.LIVE and st.trade_id in self._open_positions:
                _, opt_sym, _, _ = self._open_positions[st.trade_id]
                current_prem = self._prem_cache.get(opt_sym, 0.0)

            unrealized = (
                (current_prem - st.entry_price) * st.quantity
                if st.phase == _Phase.LIVE
                else 0.0
            )
            out[sym] = {
                "phase":          st.phase.name,
                "entry_origin":   round(st.entry_origin,    2),
                "target_high":    round(st.target_high,     2),
                "ltf_entry_line": round(st.ltf_entry_line,  2),
                "ltf_sl_line":    round(st.ltf_sl_line,     2),
                "rolling_base":   round(st.rolling_base,    2),
                "trade_id":       st.trade_id,
                "entry_price":    round(st.entry_price,     2),
                "quantity":       st.quantity,
                "current_prem":   round(current_prem,       2),
                "unrealized_pnl": round(unrealized,         2),
                "signal_count":   self._signals,
            }
        return out
