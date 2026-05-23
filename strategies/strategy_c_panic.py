"""
strategies/strategy_c_panic.py — Market Panic Selling Scanner.

Two sub-modes:
  SHORT (momentum): consecutive red candles + PCR collapse + massive
        fresh Put writing → ride the panic waterfall.
  LONG  (reversal): Put Unwinding (negative ΔPE near ATM) signals smart
        money closing short puts → imminent V-shape bounce.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np

from config.global_config import GlobalConfig
from matrix_engine.indicators import TechSnapshot
from matrix_engine.option_matrix import ChainSnapshot
from strategies.base_strategy import BaseStrategy, Direction, SignalPackage, StrategyID

logger = logging.getLogger(__name__)


class StrategyC_Panic(BaseStrategy):

    def evaluate(
        self,
        tech: TechSnapshot,
        chain: ChainSnapshot,
        all_tf: List[Optional[TechSnapshot]],
    ) -> Optional[SignalPackage]:
        sp   = self._sp
        spot = chain.spot

        # ── Price-action vector ──────────────────────────────────────────────
        consec_red  = self._consec_red(all_tf)
        large_body  = not tech.is_bullish and tech.body_ratio >= 0.65
        gap_down    = tech.c_open < tech.p_low
        momentum    = consec_red or (large_body and gap_down)
        if not momentum:
            return None

        # ── PCR vector ───────────────────────────────────────────────────────
        pcr_hist = list(chain.pcr_history)
        if len(pcr_hist) < 5:
            return None
        pcr_now      = chain.pcr
        pcr_baseline = float(np.mean(pcr_hist[-20:]) if len(pcr_hist) >= 20 else np.mean(pcr_hist))
        pcr_dropped  = (pcr_baseline - pcr_now) >= sp.panic_pcr_drop

        # ── OI surge / unwinding ─────────────────────────────────────────────
        total_put_doi  = chain.total_doi_near_atm("PE", half_width=1)
        put_surge      = total_put_doi > sp.panic_put_oi_mult * 100_000
        put_unwinding  = total_put_doi <= sp.panic_unwind_delta_threshold

        # ── SHORT: momentum continuation ─────────────────────────────────────
        if put_surge and not put_unwinding:
            conf = self._short_conf(consec_red, gap_down, put_surge, pcr_dropped, tech)
            sl   = tech.c_high + tech.atr_val * 0.5
            tgt  = spot - (sl - spot) * sp.min_risk_reward
            strike = self._execution_strike(chain, Direction.SHORT)
            notes = (f"PanicShort: PCR={pcr_now:.2f} baseline={pcr_baseline:.2f} "
                     f"PutDOI={total_put_doi:,}")
            logger.info("StratC SHORT (panic) | %s | conf=%.2f | %s", chain.underlying, conf, notes)
            return SignalPackage(
                source=StrategyID.C_PANIC, direction=Direction.SHORT,
                underlying=chain.underlying, option_type="PE",
                target_strike=strike, entry_spot=spot,
                stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
            )

        # ── LONG: put unwinding reversal ──────────────────────────────────────
        if put_unwinding:
            conf = self._reversal_conf(put_unwinding, pcr_dropped, tech)
            sl   = tech.c_low - tech.atr_val * 0.5
            tgt  = spot + (spot - sl) * sp.min_risk_reward
            strike = self._execution_strike(chain, Direction.LONG)
            notes = (f"PanicReversal: PutDOI={total_put_doi:,} "
                     f"PCR={pcr_now:.2f} RSI={tech.rsi:.1f}")
            logger.info("StratC LONG (put-unwind) | %s | conf=%.2f | %s", chain.underlying, conf, notes)
            return SignalPackage(
                source=StrategyID.C_PANIC, direction=Direction.LONG,
                underlying=chain.underlying, option_type="CE",
                target_strike=strike, entry_spot=spot,
                stop_spot=sl, target_spot=tgt, confidence=conf, notes=notes,
            )
        return None

    def _short_conf(
        self, consec: bool, gap: bool, surge: bool, pcr_drop: bool, tech: TechSnapshot
    ) -> float:
        s = 0.30
        if consec:    s += 0.15
        if gap:       s += 0.15
        if surge:     s += 0.20
        if pcr_drop:  s += 0.10
        if tech.is_vol_spike: s += 0.10
        return min(s, 1.0)

    def _reversal_conf(self, unwinding: bool, pcr_drop: bool, tech: TechSnapshot) -> float:
        s = 0.35
        if unwinding:       s += 0.30
        if pcr_drop:        s += 0.10
        if tech.rsi < 35:   s += 0.15
        if tech.is_vol_spike: s += 0.10
        return min(s, 1.0)
