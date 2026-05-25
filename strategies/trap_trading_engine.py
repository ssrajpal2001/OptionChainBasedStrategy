"""
strategies/trap_trading_engine.py — TrapTrading Strategy Engine.

Dual-timeframe institutional footprint tracker:

  HTF (75-min): Continuously aggregates OHLC bars to map structural
                Supply Zones (institutional selling / resistance blocks) and
                Demand Zones (institutional buying / support blocks).
                GUARD: LTF signals are gated — they only fire when the 5-min
                price action is inside or immediately adjacent to these macro
                boundaries.

  LTF (5-min):  Dynamic Rolling Base tracking + bearish liquidity-sweep
                detection.  Every 5-min bar is processed for:
                  • Rolling Base update (spec-exact rule)
                  • Bearish trap trigger (liquidity pool sweep)
                  • Void state management and lift

Indicator invariants — HARD-PINNED, no parameterisation at call sites:
  RSI  = 14 candles (Wilder's smoothing)
  VWAP = 500 candles rolling
  ADX  = 20 candles (+DI / -DI included)

State machine per underlying symbol:
  IDLE
    → (price enters/near HTF supply zone) → ZONE_WATCH
  ZONE_WATCH
    → (5-min candle sweeps liquidity pool: high > trap_high AND close < trap_high)
      → ARMED
    → (price leaves zone without sweep) → IDLE
  ARMED
    → (reversal confirmed: next 5-min candle bearish + -DI > +DI) → CONFIRMED
    → (price runs away: close > trap_high + VOID_ATR_MULT * ATR) → VOID
  VOID
    → (spec-exact lift: candle.low <= htf_entry_level) → CONFIRMED
  CONFIRMED → emit SignalPackage(SHORT, PE) → IDLE

All timestamps use Asia/Kolkata (IST).
All state is in-memory — no I/O on hot path.
Subscribes to Topic.CANDLE_CLOSE; publishes to Topic.SIGNAL.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import numpy as np

from config.global_config import IST, Topic, GlobalConfig
from data_layer.base_feeder import CandleEvent, EventBus
from matrix_engine.indicators import rsi, vwap, adx, atr, ema
from strategies.base_strategy import Direction, SignalPackage, StrategyID

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Module-level constants
# ─────────────────────────────────────────────────────────────────────────────

_LTF_TF = 5            # 5-minute candle timeframe
_HTF_TF = 75           # 75-minute candle timeframe

_LTF_CAPACITY = 600    # Covers VWAP-500 + 100 bars headroom (~7 trading days)
_HTF_CAPACITY = 50     # ~8 trading days of 75-min structure

_MIN_LTF_BARS = 45     # Minimum LTF bars before enabling signal detection
                       # (ensures ADX-20 has 2×20+2=42 bars of input)

_SWING_LOOKBACK = 5    # LTF bars to survey for the swing-high (liquidity pool)
_MAX_ZONES = 3         # Keep the N most recent supply / demand zones per symbol

_ZONE_TOL_PCT = 0.0050          # 0.50% proximity tolerance around zone boundaries
_VOID_ATR_MULT = 2.0            # Enter VOID if price closes > N×ATR past trap level
_ZONE_INVALIDATE_BUFFER = 0.002 # 0.2% buffer — zone invalid only after clean close-through


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV Ring Buffer
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

    # ── Scalar accessors (avoid full array conversion for single values) ───────

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
        """Highest close in last `window` bars — for zone placement context."""
        if not self._c:
            return 0.0
        n = min(window, len(self._c))
        return max(list(self._c)[-n:])

    def recent_low_pct(self, window: int = 20) -> float:
        """Lowest close in last `window` bars."""
        if not self._c:
            return 0.0
        n = min(window, len(self._c))
        return min(list(self._c)[-n:])


# ─────────────────────────────────────────────────────────────────────────────
# HTF Zone
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class HTFZone:
    """
    An institutional Supply or Demand zone identified from a single 75-min bar.

    Supply zone: bearish rejection bar — upper boundary = bar.high,
                 lower boundary = min(bar.open, bar.close).
    Demand zone: bullish support bar — upper boundary = max(bar.open, bar.close),
                 lower boundary = bar.low.

    The zone's lower boundary is the `htf_entry_level` used for void-lift checks.
    """
    zone_type:  str       # "supply" | "demand"
    upper:      float     # Zone ceiling
    lower:      float     # Zone floor  ← htf_entry_level reference
    origin_ts:  datetime
    origin_bar_high: float = 0.0
    origin_bar_low:  float = 0.0

    def contains(self, price: float) -> bool:
        return self.lower <= price <= self.upper

    def is_near(self, price: float, tol_pct: float = _ZONE_TOL_PCT) -> bool:
        """True if price is inside the zone OR within tol_pct of its boundaries."""
        span = (self.upper - self.lower)
        tol  = max(span * tol_pct, self.upper * tol_pct)
        return (self.lower - tol) <= price <= (self.upper + tol)

    def invalidated_by(self, close: float) -> bool:
        """
        Zone is consumed / invalidated once price closes cleanly through it.
        Supply zone: invalidated when price closes above the zone ceiling.
        Demand zone: invalidated when price closes below the zone floor.
        """
        buf = _ZONE_INVALIDATE_BUFFER
        if self.zone_type == "supply":
            return close > self.upper * (1.0 + buf)
        return close < self.lower * (1.0 - buf)


# ─────────────────────────────────────────────────────────────────────────────
# Per-Symbol State Machine
# ─────────────────────────────────────────────────────────────────────────────

class _Phase(Enum):
    IDLE       = auto()   # No active setup
    ZONE_WATCH = auto()   # Price near/inside HTF supply zone, watching for sweep
    ARMED      = auto()   # Liquidity sweep detected — awaiting reversal confirmation
    VOID       = auto()   # Setup invalidated — waiting for HTF level retest
    CONFIRMED  = auto()   # Signal ready to publish


@dataclass
class _SymbolState:
    phase:             _Phase   = _Phase.IDLE
    # Rolling Base (LTF dynamic support reference)
    rolling_base:      float    = 0.0
    # Active trap geometry
    trap_high:         float    = 0.0   # Liquidity pool / stop-loss cluster level
    sweep_candle_high: float    = 0.0   # High of the sweep candle (SL calculation ref)
    trap_ts:           Optional[datetime] = None
    # HTF structural reference — used for void-lift condition check
    htf_entry_level:   float    = 0.0   # Lower boundary of triggering supply zone
    # Void tracking
    void_since:        Optional[datetime] = None
    # OHLCV buffers — initialised lazily inside _get_state()
    ltf_buf:           OHLCVBuffer = field(default_factory=lambda: OHLCVBuffer(_LTF_CAPACITY))
    htf_buf:           OHLCVBuffer = field(default_factory=lambda: OHLCVBuffer(_HTF_CAPACITY))
    # Zone registries
    supply_zones:      List[HTFZone] = field(default_factory=list)
    demand_zones:      List[HTFZone] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# TrapTrading Engine
# ─────────────────────────────────────────────────────────────────────────────

class TrapTradingEngine:
    """
    Standalone async dual-timeframe engine.

    Subscribes to Topic.CANDLE_CLOSE on the EventBus for both 5-min and
    75-min CandleEvent objects.  Maintains per-symbol OHLCV buffers,
    computes RSI-14 / VWAP-500 / ADX-20 from the LTF buffer, and publishes
    a SignalPackage to Topic.SIGNAL when a bearish institutional trap is
    confirmed.

    Wire-up (run_system.py):
        trap_engine = TrapTradingEngine(bus, cfg)
        asyncio.create_task(trap_engine.run(), name="trap_engine")
    """

    def __init__(self, bus: EventBus, cfg: GlobalConfig) -> None:
        self._bus     = bus
        self._cfg     = cfg
        self._queue   = bus.subscribe(Topic.CANDLE_CLOSE)
        self._running = False
        self._states:         Dict[str, _SymbolState] = {}
        self._signals:        int = 0
        self._last_indicators: Dict[str, dict] = {}  # cached per-symbol after each LTF bar

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("TrapTradingEngine: running — listening on CANDLE_CLOSE.")
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not isinstance(event, CandleEvent):
                continue
            try:
                await self._on_candle(event)
            except Exception as exc:
                logger.exception(
                    "TrapTradingEngine: unhandled error on %s TF%dmin: %s",
                    event.symbol, event.timeframe, exc,
                )

    def stop(self) -> None:
        self._running = False

    def signal_count(self) -> int:
        return self._signals

    def state_snapshot(self) -> Dict[str, str]:
        return {sym: st.phase.name for sym, st in self._states.items()}

    def telemetry_snapshot(self) -> dict:
        """Return a per-symbol telemetry dict for the admin dashboard endpoint."""
        out: dict = {}
        for sym, st in self._states.items():
            ind  = self._last_indicators.get(sym, {})
            sup  = st.supply_zones[-1] if st.supply_zones else None
            dem  = st.demand_zones[-1] if st.demand_zones else None
            out[sym] = {
                "phase":                 st.phase.name,
                "is_void_state":         st.phase == _Phase.VOID,
                "rolling_base":          round(st.rolling_base,      2),
                "trap_high":             round(st.trap_high,         2),
                "htf_entry_level":       round(st.htf_entry_level,   2),
                "htf_supply_zone_high":  round(sup.upper, 2) if sup else None,
                "htf_supply_zone_low":   round(sup.lower, 2) if sup else None,
                "htf_demand_zone_high":  round(dem.upper, 2) if dem else None,
                "htf_demand_zone_low":   round(dem.lower, 2) if dem else None,
                "supply_zones": [
                    {"upper": round(z.upper, 2), "lower": round(z.lower, 2),
                     "ts": z.origin_ts.isoformat()}
                    for z in st.supply_zones
                ],
                "demand_zones": [
                    {"upper": round(z.upper, 2), "lower": round(z.lower, 2),
                     "ts": z.origin_ts.isoformat()}
                    for z in st.demand_zones
                ],
                "current_rsi_14":  ind.get("rsi"),
                "current_vwap_500": ind.get("vwap"),
                "current_adx_20":  ind.get("adx"),
                "current_pdi":     ind.get("pdi"),
                "current_mdi":     ind.get("mdi"),
                "void_since":      st.void_since.isoformat() if st.void_since else None,
                "ltf_bar_count":   len(st.ltf_buf),
                "htf_bar_count":   len(st.htf_buf),
                "signal_count":    self._signals,
                "ind_ts":          ind.get("ts"),
            }
        return out

    # ── Candle router ─────────────────────────────────────────────────────────

    async def _on_candle(self, c: CandleEvent) -> None:
        if c.timeframe == _HTF_TF:
            self._process_htf(c)
        elif c.timeframe == _LTF_TF:
            await self._process_ltf(c)

    # ─────────────────────────────────────────────────────────────────────────
    # HTF (75-min) — institutional zone mapping
    # ─────────────────────────────────────────────────────────────────────────

    def _process_htf(self, c: CandleEvent) -> None:
        """
        Ingest a 75-min bar, classify it as supply/demand zone origin, and
        prune zones that price has closed through.
        """
        st = self._get_state(c.symbol)
        st.htf_buf.push(c)

        o, h, l, cl = c.open, c.high, c.low, c.close
        bar_range = h - l
        if bar_range == 0.0:
            return

        upper_wick_ratio = (h - max(o, cl)) / bar_range
        lower_wick_ratio = (min(o, cl) - l)  / bar_range
        is_bearish = cl < o
        is_bullish = cl > o

        # ── Supply zone: bearish rejection bar at relative highs ──────────────
        # Criteria: bearish body AND meaningful upper wick (rejection at resistance)
        # Zone boundaries: ceiling = bar.high, floor = min(open, close)
        if is_bearish and upper_wick_ratio >= 0.15:
            zone = HTFZone(
                zone_type       = "supply",
                upper           = h,
                lower           = min(o, cl),
                origin_ts       = c.timestamp,
                origin_bar_high = h,
                origin_bar_low  = l,
            )
            st.supply_zones.append(zone)
            if len(st.supply_zones) > _MAX_ZONES:
                st.supply_zones.pop(0)
            logger.debug(
                "TrapEngine [%s] HTF supply zone: %.0f-%.0f @ %s",
                c.symbol, zone.lower, zone.upper,
                c.timestamp.strftime("%H:%M"),
            )

        # ── Demand zone: bullish support bar at relative lows ─────────────────
        elif is_bullish and lower_wick_ratio >= 0.15:
            zone = HTFZone(
                zone_type       = "demand",
                upper           = max(o, cl),
                lower           = l,
                origin_ts       = c.timestamp,
                origin_bar_high = h,
                origin_bar_low  = l,
            )
            st.demand_zones.append(zone)
            if len(st.demand_zones) > _MAX_ZONES:
                st.demand_zones.pop(0)

        # Invalidate zones consumed by price
        st.supply_zones = [z for z in st.supply_zones if not z.invalidated_by(cl)]
        st.demand_zones = [z for z in st.demand_zones if not z.invalidated_by(cl)]

    # ─────────────────────────────────────────────────────────────────────────
    # LTF (5-min) — indicators + state machine
    # ─────────────────────────────────────────────────────────────────────────

    async def _process_ltf(self, c: CandleEvent) -> None:
        st = self._get_state(c.symbol)
        buf = st.ltf_buf
        buf.push(c)

        # ── Rolling Base update (spec-exact) ──────────────────────────────────
        # "Any 5-minute candle that closes below its immediately preceding
        # candle's close dynamically becomes the new, active Rolling Base."
        if len(buf) >= 2 and c.close < buf.prev_close():
            st.rolling_base = c.low
            logger.debug(
                "TrapEngine [%s] Rolling Base -> %.2f @ %s",
                c.symbol, st.rolling_base,
                c.timestamp.strftime("%H:%M"),
            )

        # Guard: require minimum bars for stable ADX computation
        if len(buf) < _MIN_LTF_BARS:
            return

        # ── Compute hard-pinned indicators ────────────────────────────────────
        o_arr, h_arr, l_arr, cl_arr, v_arr = buf.arrays()
        rsi_val           = rsi(cl_arr)
        vwap_val          = vwap(h_arr, l_arr, cl_arr, v_arr)
        adx_val, pdi, mdi = adx(h_arr, l_arr, cl_arr)
        atr_val           = atr(h_arr, l_arr, cl_arr)
        ema_fast_val      = ema(cl_arr, 9)
        ema_slow_val      = ema(cl_arr, 21)

        # Cache latest computed indicators so the telemetry endpoint can read them
        self._last_indicators[c.symbol] = {
            "rsi":  round(float(rsi_val),  2),
            "vwap": round(float(vwap_val), 2),
            "adx":  round(float(adx_val),  2),
            "pdi":  round(float(pdi),      2),
            "mdi":  round(float(mdi),      2),
            "atr":  round(float(atr_val),  4),
            "ts":   datetime.now(IST).isoformat(),
        }

        spot = c.close

        # ── VOID state — check spec-exact lift condition (highest priority) ───
        if st.phase == _Phase.VOID:
            # "The void state restriction must be lifted cleanly the moment a
            # 5-minute candle retests back down to the exact entry level,
            # evaluated precisely as: candle.low <= htf_entry_level"
            if st.htf_entry_level > 0.0 and c.low <= st.htf_entry_level:
                logger.info(
                    "TrapEngine [%s] VOID LIFTED — "
                    "candle.low=%.2f <= htf_entry_level=%.2f @ %s",
                    c.symbol, c.low, st.htf_entry_level,
                    datetime.now(IST).strftime("%H:%M:%S"),
                )
                st.phase = _Phase.CONFIRMED
                await self._emit_signal(c, st, atr_val, rsi_val, adx_val,
                                        pdi, mdi, void_lift=True)
                st.phase = _Phase.IDLE
            return

        # ── Identify nearest active supply zone for HTF guard ─────────────────
        active_zone = self._nearest_supply_zone(st, spot)

        # ── IDLE — scan for zone entry ────────────────────────────────────────
        if st.phase == _Phase.IDLE:
            if active_zone and active_zone.is_near(spot):
                # Price is interacting with a supply zone — engage watch mode
                st.htf_entry_level = active_zone.lower
                st.trap_high       = buf.swing_high(_SWING_LOOKBACK)
                st.phase           = _Phase.ZONE_WATCH
                logger.debug(
                    "TrapEngine [%s] ZONE_WATCH entered | "
                    "supply=%.0f-%.0f trap_high=%.2f RSI=%.1f ADX=%.1f",
                    c.symbol, active_zone.lower, active_zone.upper,
                    st.trap_high, rsi_val, adx_val,
                )

        # ── ZONE_WATCH — update swing high; detect liquidity sweep ────────────
        elif st.phase == _Phase.ZONE_WATCH:
            # Keep the liquidity pool level current
            swing = buf.swing_high(_SWING_LOOKBACK)
            if swing > st.trap_high:
                st.trap_high = swing

            # Guard: if price has drifted below the zone without a sweep, reset
            if active_zone is None and spot < st.htf_entry_level * 0.995:
                st.phase = _Phase.IDLE
                logger.debug(
                    "TrapEngine [%s] zone watch cancelled — price %.2f left zone.",
                    c.symbol, spot,
                )
                return

            # ── Bearish Trap Trigger ──────────────────────────────────────────
            # "A bearish trade signal must confirm the exact millisecond a 5-minute
            # candle spikes upward to breach the psychological stop-loss level
            # (liquidity pool) of a previous bearish execution, trapping early retail
            # breakout traders right before a sharp institutional reversal."
            #
            # Trigger conditions:
            #   1. candle.high sweeps the stop-loss pool (high > trap_high)
            #   2. candle.close fails to hold above the pool (close < trap_high)
            #      → wick-up / failed breakout pattern
            #   3. Candle body is bearish (close < open)
            #   4. RSI not yet in oversold territory (setup still has downside momentum)
            if (
                st.trap_high > 0.0
                and st.rolling_base > 0.0
                and c.high  > st.trap_high   # sweep
                and c.close < st.trap_high   # rejection / close below pool
                and c.close < c.open         # bearish candle body
                and rsi_val > 38.0           # not oversold
            ):
                st.sweep_candle_high = c.high
                st.trap_ts           = datetime.now(IST)
                st.phase             = _Phase.ARMED
                logger.info(
                    "TrapEngine [%s] ARMED — liquidity sweep %.2f → close %.2f "
                    "(RSI=%.1f ADX=%.1f +DI=%.1f -DI=%.1f) @ %s",
                    c.symbol, st.trap_high, c.close,
                    rsi_val, adx_val, pdi, mdi,
                    c.timestamp.strftime("%H:%M"),
                )

        # ── ARMED — confirm reversal OR declare void ───────────────────────────
        elif st.phase == _Phase.ARMED:
            void_ceiling = st.trap_high + _VOID_ATR_MULT * atr_val

            # Void: buyers were stronger — price ran past the trap level
            if c.close > void_ceiling:
                st.phase      = _Phase.VOID
                st.void_since = datetime.now(IST)
                logger.info(
                    "TrapEngine [%s] VOID — close=%.2f exceeded threshold=%.2f",
                    c.symbol, c.close, void_ceiling,
                )
                return

            # Reversal confirmation: next candle is bearish AND -DI dominates
            # (directional pressure flipped to sellers)
            reversal_confirmed = (
                c.close < c.open          # Bearish candle body
                and c.close < buf.prev_close()   # Making a lower close
                and mdi > pdi                    # Bearish directional dominance
            )
            if reversal_confirmed:
                st.phase = _Phase.CONFIRMED
                await self._emit_signal(c, st, atr_val, rsi_val, adx_val,
                                        pdi, mdi, void_lift=False)
                st.phase = _Phase.IDLE

    # ─────────────────────────────────────────────────────────────────────────
    # Signal emission
    # ─────────────────────────────────────────────────────────────────────────

    async def _emit_signal(
        self,
        c:         CandleEvent,
        st:        _SymbolState,
        atr_val:   float,
        rsi_val:   float,
        adx_val:   float,
        pdi:       float,
        mdi:       float,
        void_lift: bool,
    ) -> None:
        """
        Construct and publish a SignalPackage for a confirmed bearish institutional
        trap.  Stop-loss is placed above the sweep candle high.  Target is derived
        from the minimum risk-reward ratio in GlobalConfig.strategy.
        """
        spot = c.close
        sl   = st.sweep_candle_high + atr_val * 0.30   # SL above wick high

        risk   = sl - spot
        if risk <= 0.0:
            logger.warning(
                "TrapEngine [%s] signal aborted — invalid risk geometry "
                "(spot=%.2f sl=%.2f). Resetting to IDLE.",
                c.symbol, spot, sl,
            )
            st.phase = _Phase.IDLE
            return

        target = spot - risk * self._cfg.strategy.min_risk_reward

        # Approximate ATM strike from spot + index strike step
        step   = self._cfg.exchange.strike_steps.get(c.symbol, 50.0)
        strike = round(spot / step) * step   # ATM PE strike

        conf = self._confidence(rsi_val, adx_val, mdi, pdi, st, void_lift)

        signal = SignalPackage(
            source        = StrategyID.TRAP_ENGINE,
            direction     = Direction.SHORT,
            underlying    = c.symbol,
            option_type   = "PE",
            target_strike = strike,
            entry_spot    = spot,
            stop_spot     = sl,
            target_spot   = target,
            confidence    = conf,
            timestamp     = datetime.now(IST),
            notes         = (
                f"TrapEngine {'VoidLift' if void_lift else 'Sweep'} | "
                f"trap_high={st.trap_high:.0f} "
                f"htf_entry={st.htf_entry_level:.0f} "
                f"roll_base={st.rolling_base:.0f} "
                f"RSI={rsi_val:.1f} ADX={adx_val:.1f} "
                f"+DI={pdi:.1f} -DI={mdi:.1f}"
            ),
        )

        if not signal.is_valid(
            self._cfg.strategy.min_risk_reward,
            self._cfg.strategy.min_confidence,
        ):
            logger.debug(
                "TrapEngine [%s] signal filtered (conf=%.2f RR=%.1f min_rr=%.1f).",
                c.symbol, conf, signal.rr_ratio,
                self._cfg.strategy.min_risk_reward,
            )
            return

        await self._bus.publish(Topic.SIGNAL, signal)
        self._signals += 1
        logger.info(
            "TrapEngine SIGNAL #%d | %s SHORT PE@%.0f | "
            "entry=%.2f sl=%.2f tgt=%.2f conf=%.2f RR=%.1f | %s",
            self._signals, c.symbol, strike,
            spot, sl, target, conf, signal.rr_ratio,
            datetime.now(IST).strftime("%H:%M:%S IST"),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _get_state(self, symbol: str) -> _SymbolState:
        if symbol not in self._states:
            self._states[symbol] = _SymbolState()
        return self._states[symbol]

    def _nearest_supply_zone(
        self, st: _SymbolState, spot: float,
    ) -> Optional[HTFZone]:
        """
        Return the most proximal active supply zone around the current spot,
        or None if price is not near any supply zone.
        """
        candidates = [z for z in st.supply_zones if z.is_near(spot)]
        if not candidates:
            return None
        # Prefer zone whose lower boundary is closest to current spot
        return min(candidates, key=lambda z: abs(z.lower - spot))

    def _confidence(
        self,
        rsi_val:   float,
        adx_val:   float,
        mdi:       float,
        pdi:       float,
        st:        _SymbolState,
        void_lift: bool,
    ) -> float:
        """
        Build a confidence score [0, 1] for the signal.

        Base score starts at 0.45 and is boosted by corroborating factors:
          RSI overbought range  → stronger resistance rejection
          ADX trending          → institutional momentum present
          -DI > +DI             → directional bearish dominance confirmed
          Rolling Base active   → prior downtrend structure intact
          HTF supply zone hit   → macro-level confluence
          Void-lift scenario    → double-confirmed structural retest
        """
        score = 0.45

        # RSI momentum context
        if rsi_val >= 65.0:
            score += 0.15    # Overbought — strong rejection probability
        elif rsi_val >= 55.0:
            score += 0.08    # Elevated — moderate resistance

        # ADX trend strength
        if adx_val >= 25.0:
            score += 0.12
        elif adx_val >= 20.0:
            score += 0.07

        # Directional dominance (-DI > +DI = bearish)
        if mdi > pdi and (mdi - pdi) >= 5.0:
            score += 0.08

        # Rolling Base confirms ongoing downtrend structure
        if st.rolling_base > 0.0:
            score += 0.08

        # HTF supply zone — primary macro confluence
        if st.htf_entry_level > 0.0:
            score += 0.10

        # Void-lift: setup re-confirmed after structural retest
        if void_lift:
            score += 0.07

        return min(score, 1.0)
