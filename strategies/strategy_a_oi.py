"""
strategies/strategy_a_oi.py — High-Probability OI Zone Breakout / Rejection.

Confluence checklist (bullish example):
  ✓ Spot near max Put OI strike (support zone within 1.5 × strike_step)
  ✓ Put ΔOI > oi_zone_min_delta_oi (fresh writing protecting support)
  ✓ Call ΔOI < 0 (call unwinding — resistance sellers exiting)
  ✓ Hammer candle OR strong bullish breakout (body ≥ 60%)
  ✓ RSI not overbought (< 65)
  ✓ Volume spike confirmation

Bearish mirror image at Call OI resistance.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from config.global_config import GlobalConfig
from matrix_engine.indicators import TechSnapshot
from matrix_engine.option_matrix import ChainSnapshot
from strategies.base_strategy import BaseStrategy, Direction, SignalPackage, StrategyID

logger = logging.getLogger(__name__)


class StrategyA_OIZone(BaseStrategy):

    def evaluate(
        self,
        tech: TechSnapshot,
        chain: ChainSnapshot,
        all_tf: List[Optional[TechSnapshot]],
    ) -> Optional[SignalPackage]:
        sp = self._sp
        spot = chain.spot
        step = chain._step()
        prox = step * sp.zone_proximity_strikes

        put_s = chain.max_put_oi_strike
        call_s = chain.max_call_oi_strike
        if put_s == 0 or call_s == 0:
            return None

        put_row  = chain.row(put_s)
        call_row = chain.row(call_s)

        # ── BULLISH: bounce off Put OI support ────────────────────────────────
        if abs(spot - put_s) <= prox and put_row:
            fresh_put  = put_row.put_doi >= sp.oi_zone_min_delta_oi
            call_unw   = call_row is not None and call_row.call_doi < -sp.oi_zone_min_delta_oi / 2
            hammer     = self._hammer(tech)
            breakout   = self._breakout_bull(tech) and tech.c_close > put_s
            rsi_ok     = tech.rsi < sp.rsi_overbought
            vol_ok     = tech.is_vol_spike

            if (hammer or breakout) and (fresh_put or call_unw) and rsi_ok:
                conf = self._conf_bull(fresh_put, call_unw, hammer, breakout, vol_ok, tech)
                sl   = tech.c_low - tech.atr_val * 0.5
                tgt  = spot + (spot - sl) * self._cfg.strategy.min_risk_reward
                strike = self._execution_strike(chain, Direction.LONG)
                notes = (f"PutSupport={put_s:.0f} PutDOI={put_row.put_doi:,} "
                         f"{'Hammer' if hammer else 'Breakout'} RSI={tech.rsi:.1f}")
                logger.info("StratA LONG | %s | conf=%.2f | %s", chain.underlying, conf, notes)
                return SignalPackage(
                    source=StrategyID.A_OI_ZONE, direction=Direction.LONG,
                    underlying=chain.underlying, option_type="CE",
                    target_strike=strike, entry_spot=spot,
                    stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
                )

        # ── BEARISH: rejection at Call OI resistance ──────────────────────────
        if abs(spot - call_s) <= prox and call_row:
            fresh_call = call_row.call_doi >= sp.oi_zone_min_delta_oi
            put_unw    = put_row is not None and put_row.put_doi < -sp.oi_zone_min_delta_oi / 2
            star       = self._shooting_star(tech)
            breakdown  = self._breakout_bear(tech) and tech.c_close < call_s
            rsi_ok     = tech.rsi > sp.rsi_oversold
            vol_ok     = tech.is_vol_spike

            if (star or breakdown) and (fresh_call or put_unw) and rsi_ok:
                conf = self._conf_bear(fresh_call, put_unw, star, breakdown, vol_ok, tech)
                sl   = tech.c_high + tech.atr_val * 0.5
                tgt  = spot - (sl - spot) * self._cfg.strategy.min_risk_reward
                strike = self._execution_strike(chain, Direction.SHORT)
                notes = (f"CallRes={call_s:.0f} CallDOI={call_row.call_doi:,} "
                         f"{'ShootingStar' if star else 'Breakdown'} RSI={tech.rsi:.1f}")
                logger.info("StratA SHORT | %s | conf=%.2f | %s", chain.underlying, conf, notes)
                return SignalPackage(
                    source=StrategyID.A_OI_ZONE, direction=Direction.SHORT,
                    underlying=chain.underlying, option_type="PE",
                    target_strike=strike, entry_spot=spot,
                    stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
                )
        return None

    # ── Confidence scorers ────────────────────────────────────────────────────

    def _conf_bull(
        self, fresh_put: bool, call_unw: bool, hammer: bool,
        breakout: bool, vol: bool, tech: TechSnapshot,
    ) -> float:
        s = 0.0
        if fresh_put:  s += 0.25
        if call_unw:   s += 0.15
        if hammer:     s += 0.20
        if breakout:   s += 0.20
        if vol:        s += 0.10
        if tech.rsi < 40: s += 0.05
        if tech.adx_val > 25: s += 0.05
        return min(s, 1.0)

    def _conf_bear(
        self, fresh_call: bool, put_unw: bool, star: bool,
        breakdown: bool, vol: bool, tech: TechSnapshot,
    ) -> float:
        s = 0.0
        if fresh_call: s += 0.25
        if put_unw:    s += 0.15
        if star:       s += 0.20
        if breakdown:  s += 0.20
        if vol:        s += 0.10
        if tech.rsi > 60: s += 0.05
        if tech.adx_val > 25: s += 0.05
        return min(s, 1.0)
