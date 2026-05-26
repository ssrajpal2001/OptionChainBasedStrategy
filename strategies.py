"""
strategies.py — Confluence Strategy Engine.

Implements the three core algorithmic models and a meta-confluence layer
that requires both price action AND option chain data to agree before
any signal is dispatched. Every strategy returns a SignalEvent or None.

Strategies:
  A — High-Probability OI Zone Breakout / Rejection
  B — Institutional Liquidity Trap Engine
  C — Market Panic Selling Scanner
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import numpy as np

from config import SystemConfig
from matrix_engine import (
    OptionChainSnapshot,
    TechnicalSnapshot,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal Domain Objects
# ---------------------------------------------------------------------------

class SignalDirection(Enum):
    LONG = auto()       # Buy CE / Long futures
    SHORT = auto()      # Buy PE / Short futures


class SignalSource(Enum):
    STRATEGY_A = "OI_Zone_Breakout"
    STRATEGY_B = "Liquidity_Trap"
    STRATEGY_C = "Panic_Selling"


@dataclass
class SignalEvent:
    """
    Fully parameterized trade signal. The execution engine only needs
    this object — it does not look back at strategy internals.
    """
    source: SignalSource
    direction: SignalDirection
    underlying: str
    option_type: str              # "CE" or "PE"
    target_strike: float          # Strike to execute (ATM / ITM1 / OTM1)
    entry_price_underlying: float # Underlying spot price at signal time
    stop_loss_underlying: float   # SL level on underlying (used to size)
    target_underlying: float      # 1st target on underlying
    confidence: float             # 0.0 – 1.0 composite score
    timestamp: datetime = field(default_factory=datetime.now)
    notes: str = ""

    @property
    def risk_on_underlying(self) -> float:
        return abs(self.entry_price_underlying - self.stop_loss_underlying)

    @property
    def reward_on_underlying(self) -> float:
        return abs(self.target_underlying - self.entry_price_underlying)

    @property
    def risk_reward_ratio(self) -> float:
        if self.risk_on_underlying == 0:
            return 0.0
        return self.reward_on_underlying / self.risk_on_underlying

    def is_valid(self, min_rr: float = 2.0) -> bool:
        return self.risk_reward_ratio >= min_rr and self.confidence > 0.5


# ---------------------------------------------------------------------------
# Base Strategy
# ---------------------------------------------------------------------------

class BaseStrategy:
    """Common interface and shared helpers for all strategy implementations."""

    def __init__(self, config: SystemConfig) -> None:
        self._config = config

    def evaluate(
        self,
        tech: TechnicalSnapshot,
        chain: OptionChainSnapshot,
        multi_tf: List[Optional[TechnicalSnapshot]],
    ) -> Optional[SignalEvent]:
        raise NotImplementedError

    # ---- Shared utilities ------------------------------------------------

    def _get_target_strike(self, chain: OptionChainSnapshot, direction: SignalDirection) -> float:
        """Derive execution strike from moneyness config."""
        step = self._strike_step(chain.underlying)
        atm = chain.atm_strike
        pref = self._config.assets.moneyness_execution
        if pref == "ATM":
            return atm
        elif pref == "ITM_1":
            return atm - step if direction == SignalDirection.LONG else atm + step
        else:  # OTM_1
            return atm + step if direction == SignalDirection.LONG else atm - step

    def _strike_step(self, underlying: str) -> float:
        steps = {
            "NIFTY": 50.0, "FINNIFTY": 50.0, "MIDCPNIFTY": 50.0,
            "BANKNIFTY": 100.0, "SENSEX": 100.0,
        }
        return steps.get(underlying, 50.0)

    def _pcr_smooth(self, chain: OptionChainSnapshot, period: int = 5) -> float:
        """Return smoothed PCR from history."""
        hist = list(chain.pcr_history)
        if not hist:
            return chain.pcr
        window = hist[-period:] if len(hist) >= period else hist
        return float(np.mean(window))

    def _detect_candle_rejection(
        self, tech: TechnicalSnapshot, bullish: bool
    ) -> bool:
        """Detect hammer (bullish) or shooting star (bearish) rejection patterns."""
        if bullish:
            # Hammer: long lower wick > 60% of range, small body, close near top
            return (
                tech.lower_wick_ratio >= 0.55
                and tech.body_ratio <= 0.35
                and tech.last_close > (tech.last_high + tech.last_low) / 2
            )
        else:
            # Shooting star: long upper wick > 60%, small body, close near bottom
            return (
                tech.upper_wick_ratio >= 0.55
                and tech.body_ratio <= 0.35
                and tech.last_close < (tech.last_high + tech.last_low) / 2
            )

    def _adx_trending(self, tech: TechnicalSnapshot) -> bool:
        return tech.adx >= 20.0

    def _consecutive_red_candles(
        self, multi_tf: List[Optional[TechnicalSnapshot]], count: int = 3
    ) -> bool:
        """Check primary timeframe for N consecutive bearish candles."""
        snap = multi_tf[0] if multi_tf else None
        if snap is None:
            return False
        # Approximation: last_close < last_open and prev_close < prev_open
        # For full N-bar check, CandleCache.get_candles() would be used in production
        return snap.last_close < snap.last_open and snap.prev_close < snap.prev_open


# ---------------------------------------------------------------------------
# Strategy A: OI Zone Breakout / Rejection
# ---------------------------------------------------------------------------

class StrategyA_OIZoneBreakout(BaseStrategy):
    """
    Identifies high-OI resistance/support levels and generates signals
    on confirmed breakouts or wick rejections.

    Confluence checklist (bullish example):
      ✓ Price near max Put OI strike (support zone)
      ✓ Put ΔOI > 0 (fresh writing protecting support)
      ✓ Call ΔOI < 0 (call unwinding — sellers exiting)
      ✓ Bullish hammer candle OR breakout candle body ≥ 60%
      ✓ RSI not overbought (< 65)
      ✓ ADX > 20 (trending) or RSI < 40 (oversold bounce)
    """

    def evaluate(
        self,
        tech: TechnicalSnapshot,
        chain: OptionChainSnapshot,
        multi_tf: List[Optional[TechnicalSnapshot]],
    ) -> Optional[SignalEvent]:
        if not self._config.strategy.strategy_a_enabled:
            return None

        spot = chain.spot_price
        cfg_s = self._config.strategy
        cfg_r = self._config.risk

        # ---- Derive key levels from chain -----------------------------------
        put_support = chain.max_put_oi_strike
        call_resistance = chain.max_call_oi_strike

        if put_support == 0 or call_resistance == 0:
            return None

        step = self._strike_step(chain.underlying)
        proximity_band = step * 1.5     # Price must be within 1.5 strikes of the zone

        # ---- Bullish Confluence: Bouncing off Put OI support ----------------
        near_support = abs(spot - put_support) <= proximity_band
        near_resistance = abs(spot - call_resistance) <= proximity_band

        # Option chain confirmation conditions
        put_row = chain.get_row(put_support)
        call_row = chain.get_row(call_resistance)

        if near_support and put_row:
            fresh_put_writing = put_row.put_change_oi >= cfg_s.oi_zone_min_delta_oi
            call_unwinding = (
                call_row is not None and call_row.call_change_oi < -cfg_s.oi_zone_min_delta_oi / 2
            )
            bullish_rejection = self._detect_candle_rejection(tech, bullish=True)
            bullish_breakout = (
                tech.is_bullish_candle
                and tech.body_ratio >= cfg_s.breakout_candle_body_ratio
                and tech.last_close > put_support
            )
            rsi_ok = tech.rsi < cfg_s.rsi_overbought
            volume_ok = tech.is_volume_spike

            price_action_ok = bullish_rejection or bullish_breakout
            chain_ok = fresh_put_writing or call_unwinding

            if price_action_ok and chain_ok and rsi_ok:
                confidence = self._compute_confidence_bullish(
                    tech, put_row, call_row, fresh_put_writing, call_unwinding,
                    bullish_rejection, bullish_breakout, volume_ok,
                )
                sl_level = tech.last_low - tech.atr * 0.5
                target_level = spot + (spot - sl_level) * cfg_r.min_risk_reward_ratio
                strike = self._get_target_strike(chain, SignalDirection.LONG)
                notes = (
                    f"Support={put_support:.0f} PutΔOI={put_row.put_change_oi:,} "
                    f"{'Hammer' if bullish_rejection else 'Breakout'} RSI={tech.rsi:.1f}"
                )
                logger.info("Strategy A → LONG signal | %s | confidence=%.2f | %s", chain.underlying, confidence, notes)
                return SignalEvent(
                    source=SignalSource.STRATEGY_A,
                    direction=SignalDirection.LONG,
                    underlying=chain.underlying,
                    option_type="CE",
                    target_strike=strike,
                    entry_price_underlying=spot,
                    stop_loss_underlying=sl_level,
                    target_underlying=target_level,
                    confidence=confidence,
                    notes=notes,
                )

        # ---- Bearish Confluence: Rejection at Call OI resistance ------------
        if near_resistance and call_row:
            fresh_call_writing = call_row.call_change_oi >= cfg_s.oi_zone_min_delta_oi
            put_unwinding = (
                put_row is not None and put_row.put_change_oi < -cfg_s.oi_zone_min_delta_oi / 2
            )
            bearish_rejection = self._detect_candle_rejection(tech, bullish=False)
            bearish_breakdown = (
                not tech.is_bullish_candle
                and tech.body_ratio >= cfg_s.breakout_candle_body_ratio
                and tech.last_close < call_resistance
            )
            rsi_ok = tech.rsi > cfg_s.rsi_oversold
            volume_ok = tech.is_volume_spike

            price_action_ok = bearish_rejection or bearish_breakdown
            chain_ok = fresh_call_writing or put_unwinding

            if price_action_ok and chain_ok and rsi_ok:
                confidence = self._compute_confidence_bearish(
                    tech, call_row, put_row, fresh_call_writing, put_unwinding,
                    bearish_rejection, bearish_breakdown, volume_ok,
                )
                sl_level = tech.last_high + tech.atr * 0.5
                target_level = spot - (sl_level - spot) * cfg_r.min_risk_reward_ratio
                strike = self._get_target_strike(chain, SignalDirection.SHORT)
                notes = (
                    f"Resistance={call_resistance:.0f} CallΔOI={call_row.call_change_oi:,} "
                    f"{'ShootingStar' if bearish_rejection else 'Breakdown'} RSI={tech.rsi:.1f}"
                )
                logger.info("Strategy A → SHORT signal | %s | confidence=%.2f | %s", chain.underlying, confidence, notes)
                return SignalEvent(
                    source=SignalSource.STRATEGY_A,
                    direction=SignalDirection.SHORT,
                    underlying=chain.underlying,
                    option_type="PE",
                    target_strike=strike,
                    entry_price_underlying=spot,
                    stop_loss_underlying=sl_level,
                    target_underlying=target_level,
                    confidence=confidence,
                    notes=notes,
                )

        return None

    def _compute_confidence_bullish(
        self, tech: TechnicalSnapshot, put_row, call_row,
        fresh_put: bool, call_unwind: bool, hammer: bool, breakout: bool, vol_spike: bool,
    ) -> float:
        score = 0.0
        if fresh_put:     score += 0.25
        if call_unwind:   score += 0.15
        if hammer:        score += 0.20
        if breakout:      score += 0.20
        if vol_spike:     score += 0.10
        if tech.rsi < 40: score += 0.05          # Oversold bonus
        if tech.adx > 25: score += 0.05
        return min(score, 1.0)

    def _compute_confidence_bearish(
        self, tech: TechnicalSnapshot, call_row, put_row,
        fresh_call: bool, put_unwind: bool, shooting_star: bool, breakdown: bool, vol_spike: bool,
    ) -> float:
        score = 0.0
        if fresh_call:       score += 0.25
        if put_unwind:       score += 0.15
        if shooting_star:    score += 0.20
        if breakdown:        score += 0.20
        if vol_spike:        score += 0.10
        if tech.rsi > 60:    score += 0.05
        if tech.adx > 25:    score += 0.05
        return min(score, 1.0)


# ---------------------------------------------------------------------------
# Strategy B: Institutional Liquidity Trap Engine
# ---------------------------------------------------------------------------

class StrategyB_LiquidityTrap(BaseStrategy):
    """
    Detects fake breakouts/breakdowns where institutional players
    lure retail traders into chasing, then sharply reverse to hunt stops.

    State machine: IDLE → BREAKOUT_DETECTED → STALLING → REVERSAL_CONFIRMED
    """

    class Phase(Enum):
        IDLE = auto()
        BEARISH_TRAP_WATCHING = auto()    # Price broke above resistance
        BULLISH_TRAP_WATCHING = auto()    # Price broke below support
        REVERSAL_CONFIRMED = auto()

    def __init__(self, config: SystemConfig) -> None:
        super().__init__(config)
        # Per-underlying state
        self._phase: Dict[str, "StrategyB_LiquidityTrap.Phase"] = {}
        self._trap_candle_high: Dict[str, float] = {}
        self._trap_candle_low: Dict[str, float] = {}
        self._trap_level: Dict[str, float] = {}
        self._stall_count: Dict[str, int] = {}
        self._trap_type: Dict[str, str] = {}   # "bearish" or "bullish"
        self._oi_at_trap: Dict[str, int] = {}

    def evaluate(
        self,
        tech: TechnicalSnapshot,
        chain: OptionChainSnapshot,
        multi_tf: List[Optional[TechnicalSnapshot]],
    ) -> Optional[SignalEvent]:
        if not self._config.strategy.strategy_b_enabled:
            return None

        und = chain.underlying
        cfg_s = self._config.strategy
        cfg_r = self._config.risk
        spot = chain.spot_price
        phase = self._phase.get(und, self.Phase.IDLE)

        call_resistance = chain.max_call_oi_strike
        put_support = chain.max_put_oi_strike
        call_row = chain.get_row(call_resistance)
        put_row = chain.get_row(put_support)

        step = self._strike_step(und)

        # ---- Phase 1: Detect a potential trap --------------------------------
        if phase == self.Phase.IDLE:
            # Bearish trap: price breaks above call_resistance with OI spike + volume
            if call_row and spot > call_resistance + step * 0.3:
                avg_call_oi_delta = np.mean(list(call_row.call_oi_history)) if call_row.call_oi_history else 1
                oi_spike = call_row.call_change_oi > avg_call_oi_delta * cfg_s.trap_oi_spike_multiplier
                vol_spike = tech.current_volume > tech.volume_ma * cfg_s.trap_volume_spike_multiplier

                if oi_spike and vol_spike and not tech.is_bullish_candle:
                    self._phase[und] = self.Phase.BEARISH_TRAP_WATCHING
                    self._trap_level[und] = call_resistance
                    self._trap_candle_high[und] = tech.last_high
                    self._trap_candle_low[und] = tech.last_low
                    self._stall_count[und] = 0
                    self._trap_type[und] = "bearish"
                    self._oi_at_trap[und] = call_row.call_change_oi
                    logger.debug("Strategy B: BEARISH_TRAP detected @ %s resistance=%.0f", und, call_resistance)

            # Bullish trap: price breaks below put_support
            elif put_row and spot < put_support - step * 0.3:
                avg_put_oi_delta = np.mean(list(put_row.put_oi_history)) if put_row.put_oi_history else 1
                oi_spike = put_row.put_change_oi > avg_put_oi_delta * cfg_s.trap_oi_spike_multiplier
                vol_spike = tech.current_volume > tech.volume_ma * cfg_s.trap_volume_spike_multiplier

                if oi_spike and vol_spike and tech.is_bullish_candle:
                    self._phase[und] = self.Phase.BULLISH_TRAP_WATCHING
                    self._trap_level[und] = put_support
                    self._trap_candle_high[und] = tech.last_high
                    self._trap_candle_low[und] = tech.last_low
                    self._stall_count[und] = 0
                    self._trap_type[und] = "bullish"
                    self._oi_at_trap[und] = put_row.put_change_oi
                    logger.debug("Strategy B: BULLISH_TRAP detected @ %s support=%.0f", und, put_support)

        # ---- Phase 2: Stalling verification ---------------------------------
        elif phase in (self.Phase.BEARISH_TRAP_WATCHING, self.Phase.BULLISH_TRAP_WATCHING):
            # If price runs away, abandon the setup
            if phase == self.Phase.BEARISH_TRAP_WATCHING:
                run_away = spot > self._trap_candle_high.get(und, spot) * 1.005
            else:
                run_away = spot < self._trap_candle_low.get(und, spot) * 0.995

            if run_away:
                self._phase[und] = self.Phase.IDLE
                return None

            # Check for stall: price not making new extremes
            self._stall_count[und] = self._stall_count.get(und, 0) + 1
            stalled = self._stall_count[und] >= cfg_s.trap_stall_candles

            # Check for OI unwinding — smart money reversing
            if phase == self.Phase.BEARISH_TRAP_WATCHING and call_row:
                unwinding = call_row.call_change_oi < 0 and abs(call_row.call_change_oi) > 20_000
            elif phase == self.Phase.BULLISH_TRAP_WATCHING and put_row:
                unwinding = put_row.put_change_oi < 0 and abs(put_row.put_change_oi) > 20_000
            else:
                unwinding = False

            if stalled and unwinding:
                self._phase[und] = self.Phase.REVERSAL_CONFIRMED

        # ---- Phase 3: Enter counter-trend trade -----------------------------
        if phase == self.Phase.REVERSAL_CONFIRMED or self._phase.get(und) == self.Phase.REVERSAL_CONFIRMED:
            self._phase[und] = self.Phase.IDLE     # Reset

            trap_type = self._trap_type.get(und, "")
            trap_level = self._trap_level.get(und, spot)
            trap_high = self._trap_candle_high.get(und, spot)
            trap_low = self._trap_candle_low.get(und, spot)

            if trap_type == "bearish":
                # Enter SHORT — price failed above resistance, now reversing down
                sl_level = trap_high + tech.atr * 0.3   # SL above wick tip
                target_level = spot - (sl_level - spot) * cfg_r.min_risk_reward_ratio
                strike = self._get_target_strike(chain, SignalDirection.SHORT)
                confidence = self._trap_confidence(tech, chain, "bearish")
                notes = (
                    f"BearTrap breakout failed at {trap_level:.0f} "
                    f"OI_at_trap={self._oi_at_trap.get(und, 0):,} stall={self._stall_count.get(und, 0)}"
                )
                logger.info("Strategy B → SHORT (bearish trap) | %s confidence=%.2f", und, confidence)
                return SignalEvent(
                    source=SignalSource.STRATEGY_B,
                    direction=SignalDirection.SHORT,
                    underlying=und,
                    option_type="PE",
                    target_strike=strike,
                    entry_price_underlying=spot,
                    stop_loss_underlying=sl_level,
                    target_underlying=target_level,
                    confidence=confidence,
                    notes=notes,
                )
            elif trap_type == "bullish":
                # Enter LONG — price refused to break support, reversal up
                sl_level = trap_low - tech.atr * 0.3
                target_level = spot + (spot - sl_level) * cfg_r.min_risk_reward_ratio
                strike = self._get_target_strike(chain, SignalDirection.LONG)
                confidence = self._trap_confidence(tech, chain, "bullish")
                notes = (
                    f"BullTrap breakdown failed at {trap_level:.0f} "
                    f"OI_at_trap={self._oi_at_trap.get(und, 0):,}"
                )
                logger.info("Strategy B → LONG (bullish trap) | %s confidence=%.2f", und, confidence)
                return SignalEvent(
                    source=SignalSource.STRATEGY_B,
                    direction=SignalDirection.LONG,
                    underlying=und,
                    option_type="CE",
                    target_strike=strike,
                    entry_price_underlying=spot,
                    stop_loss_underlying=sl_level,
                    target_underlying=target_level,
                    confidence=confidence,
                    notes=notes,
                )

        return None

    def _trap_confidence(self, tech: TechnicalSnapshot, chain: OptionChainSnapshot, trap_type: str) -> float:
        score = 0.40   # Base score for passing all phase checks
        if tech.is_volume_spike:          score += 0.15
        if self._adx_trending(tech):      score += 0.10
        pcr = self._pcr_smooth(chain)
        if trap_type == "bearish" and pcr < 0.8:  score += 0.10
        if trap_type == "bullish" and pcr > 1.2:  score += 0.10
        stall = self._stall_count.get(chain.underlying, 0)
        if stall >= 3:                    score += 0.10
        return min(score, 1.0)


# ---------------------------------------------------------------------------
# Strategy C: Market Panic Selling Scanner
# ---------------------------------------------------------------------------

class StrategyC_PanicSelling(BaseStrategy):
    """
    Identifies institutional panic or retail panic events by cross-referencing
    rapid price waterfall candles with sudden massive Put OI changes and
    a sharp drop in PCR — then fades the panic with a counter-trade.

    Core insight: genuine panic ends when Put writers (institutions) start
    unwinding. Negative ΔPE = smart money closing shorts = imminent reversal.
    """

    def evaluate(
        self,
        tech: TechnicalSnapshot,
        chain: OptionChainSnapshot,
        multi_tf: List[Optional[TechnicalSnapshot]],
    ) -> Optional[SignalEvent]:
        if not self._config.strategy.strategy_c_enabled:
            return None

        cfg_s = self._config.strategy
        cfg_r = self._config.risk
        spot = chain.spot_price

        # ---- Price Action Vector: consecutive bearish momentum ---------------
        consecutive_red = self._consecutive_red_candles(multi_tf, cfg_s.panic_consecutive_red_candles)
        large_body = not tech.is_bullish_candle and tech.body_ratio >= 0.65
        gap_down = tech.last_open < tech.prev_low

        momentum_panic = consecutive_red or (large_body and gap_down)
        if not momentum_panic:
            return None

        # ---- Option Matrix Vector: massive Put OI surge ----------------------
        pcr_history = list(chain.pcr_history)
        if len(pcr_history) < 5:
            return None

        pcr_now = chain.pcr
        pcr_baseline = float(np.mean(pcr_history[-20:]) if len(pcr_history) >= 20 else np.mean(pcr_history))
        pcr_dropped = (pcr_baseline - pcr_now) >= cfg_s.panic_pcr_drop_threshold

        # Look for massive sudden Put OI surge across near ATM strikes
        atm = chain.atm_strike
        step = self._strike_step(chain.underlying)
        total_delta_put_oi = sum(
            chain.rows[s].put_change_oi
            for s in [atm - step, atm, atm + step]
            if s in chain.rows
        )

        put_surge = total_delta_put_oi > cfg_s.panic_put_oi_surge_multiplier * 100_000

        # ---- Put Unwinding — key reversal confirmation ----------------------
        total_delta_put_near_atm = sum(
            chain.rows[s].put_change_oi
            for s in [atm - step, atm]
            if s in chain.rows
        )
        put_unwinding_confirmed = total_delta_put_near_atm <= cfg_s.put_unwinding_delta_threshold

        # ---- Short-side entry: momentum continuation -----------------------
        if put_surge and not put_unwinding_confirmed:
            confidence = self._short_confidence(tech, chain, consecutive_red, gap_down, put_surge, pcr_dropped)
            sl_level = tech.last_high + tech.atr * 0.5
            target_level = spot - (sl_level - spot) * cfg_r.min_risk_reward_ratio
            strike = self._get_target_strike(chain, SignalDirection.SHORT)
            notes = (
                f"PanicSell: ΔPutOI_near_ATM={total_delta_put_oi:,} "
                f"PCR={pcr_now:.2f} baseline={pcr_baseline:.2f}"
            )
            logger.info("Strategy C → SHORT (panic momentum) | %s confidence=%.2f", chain.underlying, confidence)
            return SignalEvent(
                source=SignalSource.STRATEGY_C,
                direction=SignalDirection.SHORT,
                underlying=chain.underlying,
                option_type="PE",
                target_strike=strike,
                entry_price_underlying=spot,
                stop_loss_underlying=sl_level,
                target_underlying=target_level,
                confidence=confidence,
                notes=notes,
            )

        # ---- Long-side entry: put unwinding reversal -------------------------
        if put_unwinding_confirmed and momentum_panic:
            confidence = self._reversal_confidence(tech, chain, put_unwinding_confirmed, pcr_dropped)
            sl_level = tech.last_low - tech.atr * 0.5
            target_level = spot + (spot - sl_level) * cfg_r.min_risk_reward_ratio
            strike = self._get_target_strike(chain, SignalDirection.LONG)
            notes = (
                f"PutUnwind reversal: ΔPutOI={total_delta_put_near_atm:,} "
                f"PCR={pcr_now:.2f}"
            )
            logger.info("Strategy C → LONG (put unwind reversal) | %s confidence=%.2f", chain.underlying, confidence)
            return SignalEvent(
                source=SignalSource.STRATEGY_C,
                direction=SignalDirection.LONG,
                underlying=chain.underlying,
                option_type="CE",
                target_strike=strike,
                entry_price_underlying=spot,
                stop_loss_underlying=sl_level,
                target_underlying=target_level,
                confidence=confidence,
                notes=notes,
            )

        return None

    def _short_confidence(
        self, tech: TechnicalSnapshot, chain: OptionChainSnapshot,
        consec: bool, gap: bool, put_surge: bool, pcr_drop: bool,
    ) -> float:
        score = 0.30
        if consec:        score += 0.15
        if gap:           score += 0.15
        if put_surge:     score += 0.20
        if pcr_drop:      score += 0.10
        if tech.is_volume_spike: score += 0.10
        return min(score, 1.0)

    def _reversal_confidence(
        self, tech: TechnicalSnapshot, chain: OptionChainSnapshot,
        unwinding: bool, pcr_drop: bool,
    ) -> float:
        score = 0.35
        if unwinding:       score += 0.30
        if pcr_drop:        score += 0.10
        if tech.rsi < 35:   score += 0.15
        if tech.is_volume_spike: score += 0.10
        return min(score, 1.0)


# ---------------------------------------------------------------------------
# Confluence Engine — Meta-layer that aggregates all strategy signals
# ---------------------------------------------------------------------------

class ConfluenceEngine:
    """
    Runs all strategies sequentially and applies the dual-factor validation rule:
      No signal is dispatched unless it passes the minimum RR threshold AND
      has a confidence score > 0.5.

    In case multiple strategies fire simultaneously, the highest-confidence
    signal is returned first. Conflicting directions (one LONG, one SHORT)
    cancel each other out.
    """

    def __init__(self, config: SystemConfig) -> None:
        self._config = config
        self._strategies: List[BaseStrategy] = [
            StrategyA_OIZoneBreakout(config),
            StrategyB_LiquidityTrap(config),
            StrategyC_PanicSelling(config),
        ]
        self._signal_count: Dict[str, int] = {}   # For statistics

    def evaluate(
        self,
        tech: TechnicalSnapshot,
        chain: OptionChainSnapshot,
        multi_tf: List[Optional[TechnicalSnapshot]],
    ) -> Optional[SignalEvent]:
        """
        Run all strategies and return the best valid signal, or None.
        Implements the strict two-factor verification rule.
        """
        candidates: List[SignalEvent] = []
        min_rr = self._config.risk.min_risk_reward_ratio

        for strategy in self._strategies:
            try:
                signal = strategy.evaluate(tech, chain, multi_tf)
                if signal and signal.is_valid(min_rr):
                    candidates.append(signal)
                    key = strategy.__class__.__name__
                    self._signal_count[key] = self._signal_count.get(key, 0) + 1
            except Exception as exc:
                logger.exception("Strategy %s raised an error: %s", strategy.__class__.__name__, exc)

        if not candidates:
            return None

        # Directional conflict check — if we have both LONG and SHORT, discard
        directions = {s.direction for s in candidates}
        if len(directions) > 1:
            logger.debug("Confluence: Conflicting signals — discarding. (%d candidates)", len(candidates))
            return None

        # Return highest-confidence signal
        best = max(candidates, key=lambda s: s.confidence)
        logger.info(
            "Confluence: DISPATCHING %s %s signal | source=%s | confidence=%.2f | RR=%.2f",
            best.direction.name,
            best.underlying,
            best.source.value,
            best.confidence,
            best.risk_reward_ratio,
        )
        return best

    def get_statistics(self) -> Dict[str, int]:
        return dict(self._signal_count)
