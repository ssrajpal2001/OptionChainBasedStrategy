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
from data_layer.base_feeder import CandleEvent, OptionTick, EventBus

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Module-level constants
# ─────────────────────────────────────────────────────────────────────────────

_MARKET_CLOSE = time(15, 30, 0)  # IST — force-exit all positions at this time

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
class _TrapState:
    phase: _Phase = _Phase.IDLE

    # Stage 1+2 — HTF structural levels
    htf_bearish_open:  float = 0.0
    htf_bearish_high:  float = 0.0
    htf_bearish_ts:    Optional[datetime] = None
    entry_origin:      float = 0.0   # htf_bearish.open → premium sell level
    target_high:       float = 0.0   # sweep bar high → profit target

    # Stage 4 — MTF nested trap
    mtf_bearish_open:  float = 0.0
    mtf_bearish_high:  float = 0.0
    mtf_bearish_low:   float = 0.0
    mtf_bearish_ts:    Optional[datetime] = None
    ltf_entry_line:    float = 0.0   # 5-min bearish.open → touch trigger
    ltf_sl_line:       float = 0.0   # 5-min bearish.low  → stop-loss

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

        # Per-symbol state
        self._states: Dict[str, _TrapState] = {}

        # Per-symbol MTF (5-min) OHLCV buffers — instance variable, NOT class variable
        self._mtf_bufs: Dict[str, OHLCVBuffer] = {}

        # Premium cache: option_symbol → last ltp
        self._prem_cache:  Dict[str, float] = {}
        # Spot cache: underlying → last spot
        self._spot_cache:  Dict[str, float] = {}

        # Open positions: trade_id → (trade_id, option_symbol, entry_price, quantity)
        self._open_positions: Dict[str, Tuple[str, str, float, int]] = {}

        # Telemetry / stats
        self._signals:      int = 0
        self._backtest_log: List[dict] = []

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main async entry — starts candle and option tick loops concurrently."""
        self._candle_q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        self._opt_q    = self._bus.subscribe(Topic.OPTION_TICK)
        self._running  = True
        logger.info("TrapTradingEngine: starting NewTrap MTF engine.")
        await asyncio.gather(
            self._candle_loop(),
            self._option_tick_loop(),
        )

    def stop(self) -> None:
        self._running = False
        logger.info("TrapTradingEngine: stop requested.")

    # ── Warm start ────────────────────────────────────────────────────────────

    async def warm_start(self, symbols: List[str]) -> None:
        """
        Replay historical 1-min bars from DB to restore HTF/MTF state
        without waiting for live bars to build up.
        """
        if self._client_db is None:
            logger.warning("TrapTradingEngine.warm_start: no client_db — skipping.")
            return

        tc   = self._cfg.trap_engine
        now  = datetime.now(IST)
        from datetime import timedelta
        since = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=tc.bars_lookback_days
        )

        import pandas as pd

        for sym in symbols:
            try:
                rows = await asyncio.to_thread(
                    self._client_db.get_1m_bars_sync, sym, since, now
                )
                if not rows:
                    logger.debug("TrapTradingEngine warm_start [%s]: no bars.", sym)
                    continue

                df = pd.DataFrame(rows)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.set_index("timestamp").sort_index()

                agg = {"open": "first", "high": "max", "low": "min",
                       "close": "last", "volume": "sum"}

                # Replay HTF bars
                htf_df = df.resample(
                    f"{tc.HTF_MINUTES}min", closed="left", label="left"
                ).agg(agg).dropna()
                for ts, row in htf_df.iterrows():
                    fake = CandleEvent(
                        symbol=sym, timeframe=tc.HTF_MINUTES,
                        timestamp=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        open=float(row["open"]), high=float(row["high"]),
                        low=float(row["low"]), close=float(row["close"]),
                        volume=int(row["volume"]),
                    )
                    self._process_htf(fake)

                # Replay MTF bars
                mtf_df = df.resample(
                    f"{tc.MTF_MINUTES}min", closed="left", label="left"
                ).agg(agg).dropna()
                for ts, row in mtf_df.iterrows():
                    fake = CandleEvent(
                        symbol=sym, timeframe=tc.MTF_MINUTES,
                        timestamp=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        open=float(row["open"]), high=float(row["high"]),
                        low=float(row["low"]), close=float(row["close"]),
                        volume=int(row["volume"]),
                    )
                    self._process_mtf(fake)

                st = self._get_state(sym)
                logger.info(
                    "TrapTradingEngine warm_start [%s]: restored phase=%s "
                    "entry_origin=%.2f rolling_base=%.2f",
                    sym, st.phase.name, st.entry_origin, st.rolling_base,
                )
            except Exception as exc:
                logger.exception("TrapTradingEngine warm_start [%s]: %s", sym, exc)

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

    async def _option_tick_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._opt_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(event, OptionTick):
                continue
            try:
                # Update premium and spot caches
                self._prem_cache[event.symbol] = event.ltp
                self._spot_cache[event.underlying] = event.ltp  # underlying proxy via option

                await self._check_touch_trigger(event)
            except Exception as exc:
                logger.exception(
                    "TrapTradingEngine: option tick error [%s]: %s",
                    event.symbol, exc,
                )

    # ── Candle router ─────────────────────────────────────────────────────────

    async def _on_candle(self, c: CandleEvent) -> None:
        tc = self._cfg.trap_engine

        # EOD guard — force-exit all if market has closed
        if datetime.now(IST).time() >= _MARKET_CLOSE:
            await self._force_exit_all("EOD")
            return

        if c.timeframe == tc.HTF_MINUTES:
            self._process_htf(c)
        elif c.timeframe == tc.MTF_MINUTES:
            self._process_mtf(c)
        elif c.timeframe == tc.LTF_MINUTES:
            await self._process_ltf_exit_guard(c)

    # ── Stage 1+2: HTF processing ─────────────────────────────────────────────

    def _process_htf(self, c: CandleEvent) -> None:
        """
        Process a 75-min candle through stages 1 and 2.

        Stage 1 requires TWO consecutive bearish candles for confirmation:
          Candle 1 (prev): bearish (close < open)
          Candle 2 (curr): close < low of Candle 1
          → Bears entered at LOW of Candle 1 (breakdown level)
          → Bears' stop loss = HIGH of Candle 2 (entry candle's high)

        Stage 2: Any subsequent candle HIGH > htf_bearish_high (bears' SL)
          → Bears stopped out → TRAP_LOCKED
          → entry_origin = Candle 1's low (where bears entered)
          → target_high  = Stage 2 candle's high (where the sweep went)

        Synchronous — no await.
        """
        st = self._get_state(c.symbol)

        # Rolling base: any candle closing below previous bar's close
        htf_buf = self._get_mtf_buf(c.symbol + "__htf__", 100)
        if len(htf_buf) >= 1 and c.close < htf_buf.last_close():
            st.rolling_base = c.low
            logger.debug(
                "TrapEngine [%s] HTF rolling_base -> %.2f @ %s",
                c.symbol, st.rolling_base, c.timestamp.strftime("%H:%M"),
            )

        is_bearish      = c.close < c.open
        prev_is_bearish = (len(htf_buf) >= 1
                           and htf_buf.last_close() < htf_buf._o[-1])
        prev_low        = htf_buf.last_low()   # Candle 1's low (before push)
        prev_high       = htf_buf.last_high()  # Candle 1's high (before push)

        # Push current candle AFTER reading previous values
        htf_buf.push(c)

        if st.phase == _Phase.IDLE:
            # Stage 1: need two bearish candles where candle 2 closes < candle 1 low
            if (is_bearish
                    and prev_is_bearish
                    and len(htf_buf) >= 2
                    and c.close < prev_low):
                # Two-candle confirmation: bears entered at Candle 1's low
                st.htf_bearish_open = prev_low    # bears' entry reference (Candle 1 low)
                st.htf_bearish_high = prev_high   # bears' stop loss = HIGH of Candle 1
                st.htf_bearish_ts   = c.timestamp
                st.phase            = _Phase.HTF_BEARISH
                logger.debug(
                    "TrapEngine [%s] Stage 1 HTF_BEARISH confirmed: "
                    "entry_ref=%.2f (C1 low) bears_sl=%.2f (C1 high) @ %s",
                    c.symbol, st.htf_bearish_open, st.htf_bearish_high,
                    c.timestamp.strftime("%H:%M"),
                )

        elif st.phase == _Phase.HTF_BEARISH:
            # Stage 2: any bar sweeping above bears' stop loss = trap confirmed
            if c.high > st.htf_bearish_high:
                st.entry_origin = st.htf_bearish_open   # C1 low = where bears entered
                st.target_high  = c.high                 # sweep high = our profit target
                st.phase        = _Phase.TRAP_LOCKED
                logger.info(
                    "TrapEngine [%s] Stage 2 TRAP_LOCKED — bears SL hit: "
                    "entry_origin=%.2f (C1 low) target=%.2f (sweep high) @ %s",
                    c.symbol, st.entry_origin, st.target_high,
                    c.timestamp.strftime("%H:%M"),
                )
            elif is_bearish and c.close < prev_low and prev_is_bearish:
                # New two-candle bearish confirmation, update candidate
                st.htf_bearish_open = prev_low    # new C1 low
                st.htf_bearish_high = prev_high   # new C1 high = bears' SL
                st.htf_bearish_ts   = c.timestamp
                logger.debug(
                    "TrapEngine [%s] HTF_BEARISH candidate refreshed: "
                    "entry_ref=%.2f sl=%.2f", c.symbol, prev_low, c.high,
                )
            elif not is_bearish and c.high <= st.htf_bearish_high:
                # Bullish bar that doesn't sweep — reset
                logger.debug(
                    "TrapEngine [%s] HTF bullish without sweep — reset IDLE", c.symbol,
                )
                self._reset_state(c.symbol)

        # Beyond TRAP_LOCKED: HTF bars update rolling_base only (done above)

    # ── Stage 4: MTF processing ───────────────────────────────────────────────

    def _process_mtf(self, c: CandleEvent) -> None:
        """
        Process a 5-min candle through stage 4a and 4b.
        Synchronous — no await.
        """
        st = self._get_state(c.symbol)

        # Rolling base: any MTF candle closing below previous
        buf = self._get_mtf_buf(c.symbol, 200)
        if len(buf) >= 1 and c.close < buf.last_close():
            st.rolling_base = c.low
            logger.debug(
                "TrapEngine [%s] MTF rolling_base -> %.2f @ %s",
                c.symbol, st.rolling_base, c.timestamp.strftime("%H:%M"),
            )
        buf.push(c)

        is_bearish = c.close < c.open

        if st.phase == _Phase.RETEST_ALERT:
            # Stage 4a: find a 5-min bearish candle
            if is_bearish:
                st.mtf_bearish_open = c.open
                st.mtf_bearish_high = c.high
                st.mtf_bearish_low  = c.low
                st.mtf_bearish_ts   = c.timestamp
                st.phase            = _Phase.MTF_BEARISH
                logger.debug(
                    "TrapEngine [%s] Stage 4a MTF_BEARISH open=%.2f high=%.2f low=%.2f @ %s",
                    c.symbol, c.open, c.high, c.low, c.timestamp.strftime("%H:%M"),
                )

        elif st.phase == _Phase.MTF_BEARISH:
            # Stage 4b: next 5-min bar's high sweeps mtf_bearish_high?
            if c.high > st.mtf_bearish_high:
                st.ltf_entry_line = st.mtf_bearish_open
                st.ltf_sl_line    = st.mtf_bearish_low
                # MTF_LOCKED → ARMED immediately
                st.phase = _Phase.ARMED
                logger.info(
                    "TrapEngine [%s] Stage 4b MTF sweep confirmed → ARMED "
                    "ltf_entry=%.2f ltf_sl=%.2f @ %s",
                    c.symbol, st.ltf_entry_line, st.ltf_sl_line,
                    c.timestamp.strftime("%H:%M"),
                )
            elif is_bearish:
                # Update MTF candidate
                st.mtf_bearish_open = c.open
                st.mtf_bearish_high = c.high
                st.mtf_bearish_low  = c.low
                st.mtf_bearish_ts   = c.timestamp
                logger.debug(
                    "TrapEngine [%s] MTF_BEARISH candidate updated open=%.2f",
                    c.symbol, c.open,
                )
            else:
                # Bullish bar without sweep — back to RETEST_ALERT
                st.phase = _Phase.RETEST_ALERT
                logger.debug(
                    "TrapEngine [%s] MTF bullish without sweep — back to RETEST_ALERT",
                    c.symbol,
                )

    # ── Stage 3 + Stage 5: option tick handler ────────────────────────────────

    async def _check_touch_trigger(self, tick: OptionTick) -> None:
        """
        Stage 3: Check if premium has retested entry_origin (TRAP_LOCKED → RETEST_ALERT).
        Stage 5: Check if premium has touched ltf_entry_line (ARMED → fire entry).
        Also updates the spot cache from index tick.
        Async — fires orders.
        """
        # EOD guard
        if datetime.now(IST).time() >= _MARKET_CLOSE:
            await self._force_exit_all("EOD")
            return

        underlying = tick.underlying
        st = self._get_state(underlying)
        prem = tick.ltp

        # Stage 3 — TRAP_LOCKED → RETEST_ALERT
        if st.phase == _Phase.TRAP_LOCKED and st.entry_origin > 0.0:
            tc = self._cfg.trap_engine
            retest_pct = tc.RETEST_ZONE_PERCENT / 100.0
            low_band   = st.entry_origin * (1.0 - retest_pct)
            high_band  = st.entry_origin * (1.0 + retest_pct)
            if low_band <= prem <= high_band:
                st.phase = _Phase.RETEST_ALERT
                logger.info(
                    "TrapEngine [%s] Stage 3 RETEST_ALERT prem=%.2f "
                    "entry_origin=%.2f ±%.1f%%",
                    underlying, prem, st.entry_origin,
                    tc.RETEST_ZONE_PERCENT,
                )

        # Stage 5 — ARMED → fire entry
        elif st.phase == _Phase.ARMED and st.ltf_entry_line > 0.0:
            tc = self._cfg.trap_engine
            if prem <= st.ltf_entry_line + tc.SLIPPAGE_BUFFER:
                logger.info(
                    "TrapEngine [%s] Stage 5 TOUCH TRIGGER prem=%.2f "
                    "ltf_entry=%.2f slippage=%.2f",
                    underlying, prem, st.ltf_entry_line, tc.SLIPPAGE_BUFFER,
                )
                await self._fire_entry(underlying, tick.symbol, prem, st)

    # ── LTF (1-min) exit guard ────────────────────────────────────────────────

    async def _process_ltf_exit_guard(self, c: CandleEvent) -> None:
        """
        On every 1-min candle close for LIVE positions:
          - close < ltf_sl_line → VOID (SL)
          - prem >= target_high  → MITIGATE (profit)
          - time >= 15:30        → EOD force-exit
        Async — fires exit orders.
        """
        st = self._get_state(c.symbol)
        if st.phase != _Phase.LIVE:
            return

        # EOD
        if datetime.now(IST).time() >= _MARKET_CLOSE:
            await self._force_exit_all("EOD")
            return

        # Retrieve current premium from cache using trade_id → option_symbol
        if st.trade_id and st.trade_id in self._open_positions:
            _, opt_sym, _, _ = self._open_positions[st.trade_id]
            current_prem = self._prem_cache.get(opt_sym, 0.0)
        else:
            current_prem = 0.0

        # SL check
        if st.ltf_sl_line > 0.0 and c.close < st.ltf_sl_line:
            logger.info(
                "TrapEngine [%s] EXIT SL — 1m_close=%.2f < ltf_sl=%.2f",
                c.symbol, c.close, st.ltf_sl_line,
            )
            await self._fire_exit(st.trade_id, current_prem, "SL")
            return

        # Profit target
        if st.target_high > 0.0 and current_prem >= st.target_high:
            logger.info(
                "TrapEngine [%s] EXIT PROFIT — prem=%.2f >= target_high=%.2f",
                c.symbol, current_prem, st.target_high,
            )
            await self._fire_exit(st.trade_id, current_prem, "MITIGATE")

    # ── Entry / Exit ──────────────────────────────────────────────────────────

    async def _fire_entry(
        self,
        underlying: str,
        option_symbol: str,
        entry_price: float,
        st: _TrapState,
    ) -> None:
        """Stage 5 execution — allocate quantity per client and fire orders."""
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

        st.trade_id    = trade_id
        st.entry_price = entry_price
        st.quantity    = total_qty
        st.phase       = _Phase.LIVE

        self._open_positions[trade_id] = (
            trade_id, option_symbol, entry_price, total_qty
        )

        self._signals += 1
        logger.info(
            "TrapTradingEngine ENTRY #%d | trade_id=%s | %s | %s "
            "entry=%.2f qty=%d",
            self._signals, trade_id, underlying, option_symbol,
            entry_price, total_qty,
        )

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
            rb = st.rolling_base  # preserve rolling base
            self._reset_state(underlying)
            self._states[underlying].rolling_base = rb

        logger.info(
            "TrapTradingEngine EXIT | trade_id=%s | %s | reason=%s "
            "entry=%.2f exit=%.2f qty=%d pnl=%.2f",
            trade_id, opt_sym, reason,
            entry_price, exit_price, qty, pnl,
        )

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
        qty = self._cfg.exchange.lot_sizes.get(underlying, 75)

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

    def _get_mtf_buf(self, key: str, capacity: int = 200) -> OHLCVBuffer:
        """Get (or create) the OHLCVBuffer for a given key (symbol or symbol+suffix)."""
        if key not in self._mtf_bufs:
            self._mtf_bufs[key] = OHLCVBuffer(capacity)
        return self._mtf_bufs[key]

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
        """Day-of-week ITM strike selection."""
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
