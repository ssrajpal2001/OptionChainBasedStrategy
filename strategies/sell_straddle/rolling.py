"""
strategies/sell_straddle/rolling.py — single-side roll + smart/scalable TSL helpers.

Rollover logic shared by ratio exit, LTP decay, VWAP rise, and scalable TSL.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from config.global_config import IST
from data_layer.runtime_config import RuntimeConfig
from strategies.core.rule_evaluator import eval_rules as _eval_rules

logger = logging.getLogger(__name__)

# Minimum seconds between rollover retries for the same reason, to avoid
# spamming partner searches every tick when a market condition persists.
_ROLL_RETRY_SECONDS = 60


def _summarize_partner_trace(trace: list) -> dict:
    """Return a human-readable summary of why select_partner_for rejected every candidate."""
    if not trace:
        return {"reason": "no_trace"}
    start = next((t for t in trace if t.get("event") == "select_partner_for_start"), {})
    end = next((t for t in trace if t.get("event") == "select_partner_for_end"), {})
    candidates_total = end.get("candidates_total", start.get("candidate_strikes", []).__len__())
    counts = end.get("reject_counts", {})
    if candidates_total == 0:
        return {"reason": "no_candidates", "candidates_total": 0}
    # Determine the dominant blocker.
    dominant = max(counts.items(), key=lambda kv: kv[1]) if counts else ("unknown", 0)
    return {
        "candidates_total": candidates_total,
        "reject_counts": counts,
        "dominant_reason": dominant[0],
        "dominant_count": dominant[1],
        "message": (
            f"checked {candidates_total} candidates; "
            f"all blocked ({dominant[0]}={dominant[1]})"
        ),
    }


def _format_partner_trace(trace: list) -> str:
    """Return a detailed, line-by-line dump of the partner search for logs."""
    if not trace:
        return "no candidate trace"
    start = next((t for t in trace if t.get("event") == "select_partner_for_start"), {})
    end = next((t for t in trace if t.get("event") == "select_partner_for_end"), {})
    roll_side = start.get("roll_side", "?")
    kept_strike = start.get("kept_strike", "?")
    kept_ltp = float(start.get("kept_ltp", 0.0) or 0.0)
    spot = float(start.get("spot", 0.0) or 0.0)
    ltp_target = float(start.get("ltp_target", 0.0) or 0.0)
    theta_target = float(start.get("theta_target", 0.0) or 0.0)
    max_itm_steps = start.get("max_itm_steps")
    lines = [
        f"Partner search: keep {kept_strike} @ {kept_ltp:.2f} | "
        f"roll_side={roll_side} | spot={spot:.2f}",
        f"Filters: ltp_target={ltp_target:.2f} theta_target={theta_target:.2f} "
        f"max_itm_steps={max_itm_steps}",
    ]
    candidates = [t for t in trace if t.get("event") == "candidate"]
    if not candidates:
        lines.append("No candidate trace records.")
    else:
        lines.append("Candidates:")
        for c in candidates:
            strike = c.get("strike", "?")
            ltp = float(c.get("ltp", 0.0) or 0.0)
            selected = bool(c.get("selected"))
            reject = c.get("reject_reason") or ""
            if selected:
                status = "✓ PASSED (selected)"
            elif reject:
                status = f"✗ BLOCKED: {reject}"
            else:
                status = "✓ PASSED (not closest)"
            lines.append(f"  {roll_side}{strike:>6} ltp={ltp:>8.2f}  {status}")
            # For rule-fail candidates, also dump the exact indicators that were evaluated.
            _rr = str(c.get("reject_reason") or "")
            if _rr.startswith("rule_fail") and c.get("rule_ind_by_tf"):
                for tf, inds in c["rule_ind_by_tf"].items():
                    ind_summary = " ".join(f"{k}={v:.2f}" for k, v in inds.items() if isinstance(v, float))
                    lines.append(f"          ind_by_tf[{tf}]: {ind_summary}")
    if end:
        best_strike = end.get("best_strike")
        best_ltp = end.get("best_ltp")
        if best_strike is not None and best_ltp is not None:
            lines.append(f"Selected: {roll_side}{best_strike} @ {float(best_ltp):.2f}")
        else:
            lines.append("Selected: NONE")
        counts = end.get("reject_counts")
        if counts:
            lines.append(f"Reject counts: {counts}")
    return "\n".join(lines)


class RollingMixin:
    """Rolling / single-side-roll logic for the sell-straddle book."""

    async def _single_side_roll(self, now: datetime, reason: str) -> None:
        """Check-first rollover: close the GOOD leg (less loss / more profit) and re-sell a new
        partner for the RUNNING / bleeding leg ONLY if a candidate passes LTP threshold +
        re-entry rules + ratio. If no candidate passes, the existing trade continues unchanged."""
        from strategies.sell_straddle.selection import select_partner_for
        pos = self._position
        if not pos or pos.status != "open":
            return

        # Throttle: do not re-attempt the same rollover reason more often than
        # _ROLL_RETRY_SECONDS, otherwise every tick would re-run partner search.
        _attempts = getattr(self, "_last_roll_attempt", None) or {}
        _last = _attempts.get(reason)
        if _last and (now - _last).total_seconds() < _ROLL_RETRY_SECONDS:
            return
        _attempts[reason] = now
        self._last_roll_attempt = _attempts

        # 1. Identify the "good" leg to roll: higher short P&L = more profit / less loss.
        ce_pnl = float(getattr(pos.ce_leg, "entry_price", 0.0) or 0.0) - float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0)
        pe_pnl = float(getattr(pos.pe_leg, "entry_price", 0.0) or 0.0) - float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0)
        roll_side = "CE" if ce_pnl >= pe_pnl else "PE"
        keep_side = "PE" if roll_side == "CE" else "CE"
        keep_leg = pos.pe_leg if keep_side == "PE" else pos.ce_leg
        keep_strike = int(keep_leg.strike)
        keep_ltp = float(getattr(keep_leg, "ltp", 0.0) or getattr(keep_leg, "entry_price", 0.0) or 0.0)
        orig_strike = int((pos.ce_leg if roll_side == "CE" else pos.pe_leg).strike)

        self._clog.info(
            "SellStraddle[%s]: ROLLOVER STARTED — reason=%s | position CE%d@%.2f PE%d@%.2f | "
            "rolling the %s leg (CE pnl=%.2f PE pnl=%.2f), keeping %s%d@%.2f",
            self._underlying, reason,
            int(pos.ce_leg.strike), float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0),
            int(pos.pe_leg.strike), float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0),
            roll_side, ce_pnl, pe_pnl, keep_side, keep_strike, keep_ltp,
        )

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        max_itm = int(ss.get("roll_max_itm_steps", 5))
        variable_strikes = bool(ss.get("variable_strikes", False))

        # 2. FIND A VALID PARTNER for the running/bleeding leg.
        #    - premium must be <= kept leg (select_partner_for)
        #    - must pass LTP/theta floor and re-entry rule
        #    - must be within roll_max_itm_steps
        def _rule_pass_with_detail(ce_s: int, pe_s: int):
            """Return (passed, reason, ind_by_tf) so the trace can show exact values."""
            ind = self._ind_by_tf(ce_s, pe_s, rules)
            passed, reason = _eval_rules(rules, ind)
            return passed, reason, ind

        _partner_trace: list = []
        partner = select_partner_for(
            self._strike_prem,
            roll_side=roll_side,
            kept_strike=keep_strike,
            kept_ltp=keep_ltp,
            spot=self._spot,
            step=step,
            offset=offset,
            ltp_target=ltp_target,
            rule_pass=_rule_pass_with_detail,
            max_itm_steps=max_itm,
            theta_target=self._theta_target,
            variable_strikes=variable_strikes,
            trace=_partner_trace,
            ltp_le_kept=False,
            metric="balanced_ratio",
        )

        # Always dump the full partner-search trace so it is obvious which
        # candidates were checked, which filters blocked them, and which passed.
        _trace_dump = _format_partner_trace(_partner_trace)
        _pool_diag = self._pool_warmth_diag(roll_side, candidate_count=offset * 2 + 1)
        self._clog.info(
            "SellStraddle[%s]: ROLLOVER %s partner-search trace for running %s%d @%.2f "
            "(CE pnl=%.2f PE pnl=%.2f):\n%s\npool_warmth=%s",
            self._underlying, reason, keep_side, keep_strike, keep_ltp, ce_pnl, pe_pnl,
            _trace_dump, _pool_diag,
        )

        if not partner:
            _summary = _summarize_partner_trace(_partner_trace)
            _why_plain = _summary.get("message", "no candidates")
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s — no valid partner; keeping original pair. reason: %s",
                self._underlying, reason, _why_plain,
            )
            return

        new_strike, new_ltp = partner
        if int(new_strike) == orig_strike:
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s — best partner is the SAME strike %d; "
                "no new pair, keeping original pair.", self._underlying, reason, orig_strike
            )
            return

        # 3. MAX SKEW CHECK (max_entry_ratio).
        #    Because select_partner_for guarantees new_ltp <= keep_ltp,
        #    ratio = keep_ltp / new_ltp. Set ratio_exit.max_entry_ratio > 0 to enable.
        if self._max_entry_ratio > 0 and keep_ltp > 0 and new_ltp > 0:
            _skew = float(keep_ltp) / float(new_ltp)
            if _skew > self._max_entry_ratio:
                self._clog.info(
                    "SellStraddle[%s]: ROLLOVER %s — partner %s%d @%.2f is too skewed "
                    "vs running %s%d @%.2f (ratio=%.2f > max=%.2f); keeping original pair.",
                    self._underlying, reason, roll_side, new_strike, new_ltp,
                    keep_side, keep_strike, keep_ltp, _skew, self._max_entry_ratio,
                )
                return

        # 4. CURRENT-TICK SANITY CHECK: the configured re-entry rule may use a higher
        # timeframe (e.g. tf=2), so a partner can pass on the last closed candle while
        # the current 1-min tick already shows close >= vwap. Reject the roll in that
        # case — rolling into a pair that is already above its combined VWAP is a bad
        # re-entry, exactly what the chart at 13:46 showed.
        cand_ce = int(new_strike if roll_side == "CE" else keep_strike)
        cand_pe = int(keep_strike if roll_side == "CE" else new_strike)
        cur_ind = self._pair_indicators(cand_ce, cand_pe) or {}
        cur_close = float(cur_ind.get("close", 0.0) or 0.0)
        cur_vwap = float(cur_ind.get("vwap", 0.0) or 0.0)
        if cur_close > 0 and cur_vwap > 0 and cur_close >= cur_vwap:
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s — partner CE%d/PE%d current close=%.2f "
                ">= vwap=%.2f; keeping original pair.",
                self._underlying, reason, cand_ce, cand_pe, cur_close, cur_vwap,
            )
            return

        # 5. Execute the roll: close the good leg FIRST, wait for the close fill,
        #    then open the new partner. This guarantees the buy-to-close is confirmed
        #    before the sell-to-open, avoiding a transient double-short / margin spike.
        self._clog.info("SellStraddle[%s]: ROLL %s → %s%d @%.2f (good leg vs running %s%d @%.2f) [%s]",
                    self._underlying, roll_side, roll_side, new_strike, new_ltp,
                    keep_side, keep_strike, keep_ltp, reason)
        self._roll_in_progress = True
        close_ev = await self._close_leg(roll_side, reason, now)
        close_eid = getattr(close_ev, "event_id", "") or ""
        waiter: asyncio.Event | None = None
        if close_eid:
            waiter = asyncio.Event()
            self._roll_close_waiters[close_eid] = waiter
        try:
            if waiter is not None:
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    self._clog.warning(
                        "SellStraddle[%s]: roll close fill not confirmed within 10s (close_eid=%s) — "
                        "aborting the open side to avoid a naked/duplicate position.",
                        self._underlying, close_eid,
                    )
                    self._roll_in_progress = False
                    return
        finally:
            self._roll_close_waiters.pop(close_eid, None)

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
            self._clog.info(
                "SellStraddle[%s]: ROLL complete — fresh pair CE%d/PE%d. "
                "Exit conditions reset: min_vwap=inf, peak_profit=0, tsl_lock=0, entry_tv=%.2f. "
                "Day%% guardrail continues on cumulative realized=%.2f.",
                self._underlying,
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike),
                float(getattr(self._position, "entry_time_value", 0.0) or 0.0),
                float(getattr(self, "_session_realized_pnl_pts", 0.0) or 0.0),
            )
        self._persist()
        await self._check_itm_pair_gate(now)

    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side."""
        other = "PE" if side == "CE" else "CE"
        await self._close_leg(side, f"partial_roll_{reason}", now)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        theta_target = getattr(self, "_theta_target", 0.0)
        _tv_ok = True
        if theta_target > 0:
            from strategies.sell_straddle.selection import strip_intrinsic
            _tv_ok = strip_intrinsic(ltp, side, strike, self._spot) >= theta_target
        if strike and ltp and ltp >= ltp_target and _tv_ok:
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
        """Contract value multiplier: BTC=0.001, ETH=0.01, NSE/MCX=1.0.
        Reverted 2026-07-19 (same day, later) — user confirmed 1 lot = 0.001
        BTC is correct after all (matches Delta's real product API
        contract_value field, see execution_bridge/broker_delta.py
        discover_chain()); the earlier same-day cv=1.0 "no scaling" change
        is superseded."""
        u = str(self._underlying).upper()
        if u == "BTC":
            return 0.001
        if u == "ETH":
            return 0.01
        return 1.0

    @property
    def _ccy_symbol(self) -> str:
        return "$" if str(self._underlying).upper() in ("BTC", "ETH") else "₹"

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

    def _both_itm(self) -> bool:
        """Return True if BOTH open legs are ITM relative to current spot."""
        pos = self._position
        if not pos or pos.status != "open":
            return False
        spot = self._spot
        if not spot:
            return False
        ce_itm = float(pos.ce_leg.strike) < spot   # CE ITM = strike below spot
        pe_itm = float(pos.pe_leg.strike) > spot   # PE ITM = strike above spot
        return ce_itm and pe_itm

    def _cumulative_pnl_pts(self) -> float:
        """Today's booked P&L (all closed legs) + current running P&L in option pts."""
        running = self._position.unrealized_pnl if self._position else 0.0
        return self._session_realized_pnl_pts + running

    async def _check_itm_pair_gate(self, now: datetime) -> None:
        """
        Called after every rollover completes and on every exit-check cycle while armed.
        If both legs are ITM AND cumulative P&L in INR >= itm_pair_gate_profit_inr → close
        both legs fully and restart via re-entry.
        If both legs are ITM AND cumulative P&L < threshold → just hold and keep watching;
        we no longer attempt single-side rollover escapes because they churn the position
        and deepen losses when the gate fires below the profit threshold.
        Does nothing if the toggle is OFF or the pair is not both ITM.
        """
        if not getattr(self, "_itm_pair_gate_enabled", False):
            return
        if not self._both_itm():
            self._itm_gate_armed = False
            return

        pos = self._position
        if not pos or pos.status != "open":
            self._itm_gate_armed = False
            return

        cumulative_pts = self._cumulative_pnl_pts()
        cumulative_inr = self._pnl_rs(cumulative_pts)
        threshold_inr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))
        ce_s = int(pos.ce_leg.strike)
        pe_s = int(pos.pe_leg.strike)

        if cumulative_inr >= threshold_inr:
            logger.info(
                "SellStraddle[%s]: ITM-PAIR GATE — both legs ITM (CE%d/PE%d) spot=%.0f "
                "cumulative=%.2f pts (₹%.2f) >= threshold ₹%.2f → closing both and restarting.",
                self._underlying, ce_s, pe_s, self._spot, cumulative_pts, cumulative_inr, threshold_inr,
            )
            self._itm_gate_armed = False
            await self._close_position("itm_pair_gate_profit")
            # No cooldown — restart immediately via re-entry (not beginning).
            self._beginning_failed = True   # force re-entry path on next entry attempt
            return

        # Below threshold: arm the gate and hold.  No rollover escape — we wait for either
        # the cumulative P&L to cross the profit threshold (full exit above) or the pair
        # to stop being both ITM.
        if not getattr(self, "_itm_gate_armed", False):
            self._itm_gate_armed = True
            logger.info(
                "SellStraddle[%s]: ITM-PAIR GATE ARMED — both legs ITM (CE%d/PE%d) spot=%.0f "
                "cumulative=%.2f pts (₹%.2f) < threshold ₹%.2f. Holding; no rollover until profit threshold is met.",
                self._underlying, ce_s, pe_s, self._spot, cumulative_pts, cumulative_inr, threshold_inr,
            )

    def _apply_sl_cooldown(self) -> None:
        """Block re-entry until the next boundary of the max re-entry timeframe.
        This makes the cooldown dynamic: if an exit happens mid-candle, re-entry is
        allowed only after that candle/tf closes.
        Additionally honours `sl_cooldown_minutes` from config so crude/MCX can rest
        longer between failed rolls without changing the indicator timeframe."""
        from data_layer.runtime_config import RuntimeConfig
        now = datetime.now(IST)
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        max_tf = max((int(r.get("tf", 1)) for r in rules), default=1)
        boundary = self._next_boundary(now, max_tf)
        fixed_minutes = float(getattr(self, "_sl_cooldown_minutes", 0.0) or 0.0)
        if fixed_minutes > 0:
            fixed_boundary = now + timedelta(minutes=fixed_minutes)
            if fixed_boundary > boundary:
                boundary = fixed_boundary
        self._sl_cooldown_until = boundary
        logger.info(
            "SellStraddle[%s]: re-entry cooldown dynamic — max_tf=%d min, fixed=%.0f min, no re-entry until %s.",
            self._underlying, max_tf, fixed_minutes, boundary.strftime("%H:%M:%S"),
        )
