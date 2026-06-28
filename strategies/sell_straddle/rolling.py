"""
strategies/sell_straddle/rolling.py — single-side roll + smart/scalable TSL helpers.

Rollover logic shared by ratio exit, LTP decay, VWAP rise, and scalable TSL.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from config.global_config import IST
from data_layer.runtime_config import RuntimeConfig
from strategies.core.rule_evaluator import eval_rules as _eval_rules

logger = logging.getLogger(__name__)


class RollingMixin:
    """Rolling / single-side-roll logic for the sell-straddle book."""

    async def _single_side_roll(self, now: datetime, reason: str) -> None:
        """Check-first rollover: close the GOOD leg (less loss / more profit) and re-sell a new
        partner for the RUNNING / bleeding leg ONLY if a candidate passes LTP threshold +
        re-entry rules + ratio. If no candidate passes, the existing trade continues unchanged."""
        from strategies.sell_straddle.selection import find_rollover_partner
        pos = self._position
        if not pos or pos.status != "open":
            return

        # 1. Identify the "good" leg to roll: higher short P&L = more profit / less loss.
        ce_pnl = float(getattr(pos.ce_leg, "entry_price", 0.0) or 0.0) - float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0)
        pe_pnl = float(getattr(pos.pe_leg, "entry_price", 0.0) or 0.0) - float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0)
        roll_side = "CE" if ce_pnl >= pe_pnl else "PE"
        keep_side = "PE" if roll_side == "CE" else "CE"
        keep_leg = pos.pe_leg if keep_side == "PE" else pos.ce_leg
        keep_strike = int(keep_leg.strike)
        keep_ltp = float(getattr(keep_leg, "ltp", 0.0) or getattr(keep_leg, "entry_price", 0.0) or 0.0)
        orig_strike = int((pos.ce_leg if roll_side == "CE" else pos.pe_leg).strike)

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        max_itm = int(ss.get("roll_max_itm_steps", 2))

        # 2. CHECK-FIRST: find a valid partner before closing anything.
        partner = find_rollover_partner(
            self._strike_prem,
            roll_side=roll_side,
            kept_strike=keep_strike,
            kept_ltp=keep_ltp,
            spot=self._spot,
            step=step,
            offset=offset,
            ltp_target=ltp_target,
            max_entry_ratio=self._max_entry_ratio,
            rule_eval=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules)),
            max_itm_steps=max_itm,
        )

        if not partner:
            logger.info(
                "SellStraddle[%s]: ROLLOVER CHECK %s — no valid partner for running %s%d @%.2f "
                "(CE pnl=%.2f PE pnl=%.2f); keeping trade unchanged.",
                self._underlying, reason, keep_side, keep_strike, keep_ltp, ce_pnl, pe_pnl,
            )
            return

        new_strike, new_ltp = partner
        if int(new_strike) == orig_strike:
            logger.info("SellStraddle[%s]: ROLLOVER %s SKIPPED — best partner is the SAME strike %d "
                        "(no-op, no orders sent).", self._underlying, reason, orig_strike)
            return

        # 3. Execute the roll: close the good leg, open the new partner.
        logger.info("SellStraddle[%s]: ROLL %s → %s%d @%.2f (good leg vs running %s%d @%.2f) [%s]",
                    self._underlying, roll_side, roll_side, new_strike, new_ltp,
                    keep_side, keep_strike, keep_ltp, reason)
        await self._close_leg(roll_side, reason, now)
        await self._open_leg(roll_side, int(new_strike), float(new_ltp), now, f"single_side_roll_{reason}")
        if self._position:
            self._position.session_min_vwap = float("inf")
            self._position.peak_profit = 0.0
            self._position.tsl_high_lock_rs = 0.0
            self._position.trailing_active = False
            self._position.trail_peak_pct = 0.0
            self._position.session_min_vwap = float("inf")
            self._position.vwap_last_good = 0.0
            try:
                self._position.entry_time_value = self._position.current_time_value(self._spot)
                if self._position.entry_time_value > self._initial_entry_time_value:
                    self._initial_entry_time_value = self._position.entry_time_value
            except Exception:
                pass
        self._persist()

    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side."""
        other = "PE" if side == "CE" else "CE"
        await self._close_leg(side, f"partial_roll_{reason}", now)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        if strike and ltp and ltp >= ltp_target:
            await self._open_leg(side, strike, ltp, now, f"partial_roll_{reason}")
            if self._position:
                self._position.session_min_vwap = float("inf")
                self._position.peak_profit = 0.0
                self._position.tsl_high_lock_rs = 0.0
                self._position.trailing_active = False
                self._position.trail_peak_pct = 0.0
                try:
                    self._position.entry_time_value = self._position.current_time_value(self._spot)
                    if self._position.entry_time_value > self._initial_entry_time_value:
                        self._initial_entry_time_value = self._position.entry_time_value
                except Exception:
                    pass
            self._persist()
            return
        logger.warning("SellStraddle[%s]: partial roll %s invalid candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"partial_cleanup_{reason}", now)
        self._position = None
        self._persist()

    @property
    def _contract_cv(self) -> float:
        """Contract value multiplier: BTC=0.001, ETH=0.01, NSE/MCX=1.0."""
        u = str(self._underlying).upper()
        if u == "BTC":
            return 0.001
        if u == "ETH":
            return 0.01
        return 1.0

    @property
    def _ccy_symbol(self) -> str:
        return "$" if self._contract_cv < 1.0 else "₹"

    def _pnl_rs(self, pnl_pts: float) -> float:
        """Convert P&L in premium points to currency units."""
        qty = self._lot_size * self._lot_multiplier
        return pnl_pts * qty * self._contract_cv

    def _check_scalable_tsl(self, pos, pnl_pts: float) -> bool:
        """Rupee-based per-lot scalable TSL."""
        _cv = self._contract_cv
        qty_mult = self._lot_multiplier
        base_profit = self._tsl_base_profit_rs * qty_mult * _cv
        base_lock = self._tsl_base_lock_rs * qty_mult * _cv
        step_profit = self._tsl_step_profit_rs * qty_mult * _cv
        step_lock = self._tsl_step_lock_rs * qty_mult * _cv

        profit_rs = self._pnl_rs(pnl_pts)

        if profit_rs >= base_profit and step_profit > 0:
            num_steps = int((profit_rs - base_profit) // step_profit)
            calc_lock = base_lock + num_steps * step_lock
            if calc_lock > pos.tsl_high_lock_rs:
                pos.tsl_high_lock_rs = calc_lock
                logger.debug(
                    "SellStraddle[%s]: TSL lock updated — %s%.4f (profit=%s%.4f step=%d)",
                    self._underlying, self._ccy_symbol, calc_lock,
                    self._ccy_symbol, profit_rs, num_steps,
                )

        if pos.tsl_high_lock_rs > 0 and profit_rs < pos.tsl_high_lock_rs:
            return True
        return False

    def _apply_sl_cooldown(self) -> None:
        """Block re-entry for the configured number of MINUTES after a full exit."""
        cooldown_min = int(self._sl_cooldown_minutes)
        if cooldown_min > 0:
            self._sl_cooldown_until = datetime.now(IST) + timedelta(minutes=cooldown_min)
            logger.info("SellStraddle[%s]: re-entry cooldown %d min (no re-entry until %s).",
                        self._underlying, cooldown_min, self._sl_cooldown_until.strftime("%H:%M"))
