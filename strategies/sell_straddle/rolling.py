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

# Minimum seconds between rollover retries for the same reason, to avoid
# spamming partner searches every tick when a market condition persists.
_ROLL_RETRY_SECONDS = 60

# 2026-08-20 user spec: still used by the ITM-roll-PROTECTION pool search
# (_check_itm_roll_protection_side, a DIFFERENT mechanic from the main rollover
# path below -- searching for a fresh pool partner after a protection-budget
# stop-out, not "hit a loss, enter rollover mode"). BEGINNING keeps the real
# listed strike grid (50pt for NIFTY); this one deliberately still uses an
# artificial 100pt grid, NOT derived from ExchangeConfig.strike_steps.
_ROLLOVER_STRIKE_STEP = 100.0

# 2026-08-27, direct user spec ("one major change in rollover"): the MAIN
# rollover path (_single_side_roll) now searches the REAL strike grid in one
# direction only (toward spot), but still enforces a hard minimum gap from the
# strike being closed -- a candidate closer than this is ignored outright, no
# matter how good its premium match, and the search keeps walking outward
# until it finds one that clears this gap.
_ROLLOVER_MIN_GAP_PTS = 100.0


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
    max_itm_steps = start.get("max_itm_steps")
    lines = [
        f"Partner search: keep {kept_strike} @ {kept_ltp:.2f} | "
        f"roll_side={roll_side} | spot={spot:.2f}",
    ]
    if "min_gap_pts" in start:
        # 2026-08-27 directional min-gap search (select_rollover_partner_directional).
        lines.append(
            f"Filters: closing_strike={start.get('closing_strike')} direction={start.get('direction')} "
            f"real_step={float(start.get('real_step', 0.0) or 0.0):.2f} "
            f"min_gap_pts={float(start.get('min_gap_pts', 0.0) or 0.0):.2f} "
            f"max_itm_steps={max_itm_steps}"
        )
    else:
        # Legacy select_partner_for trace shape (still used by ITM-roll-protection).
        ltp_target = float(start.get("ltp_target", 0.0) or 0.0)
        theta_target = float(start.get("theta_target", 0.0) or 0.0)
        lines.append(
            f"Filters: ltp_target={ltp_target:.2f} theta_target={theta_target:.2f} "
            f"max_itm_steps={max_itm_steps}"
        )
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

    async def _single_side_roll(self, now: datetime, reason: str) -> bool:
        """Check-first rollover: close the GOOD leg (less loss / more profit) and re-sell a new
        partner for the RUNNING / bleeding leg ONLY if a candidate passes LTP threshold +
        re-entry rules + ratio. If no candidate passes, the existing trade continues unchanged.

        Returns True if the roll actually executed (new leg opened), False otherwise --
        callers (e.g. the ITM-pair-gate rollover path) use this to decide whether to fall
        back to a full close."""
        from strategies.sell_straddle.selection import select_rollover_partner_directional
        pos = self._position
        if not pos or pos.status != "open":
            return False

        # Throttle: do not re-attempt the same rollover reason more often than
        # _ROLL_RETRY_SECONDS, otherwise every tick would re-run partner search.
        _attempts = getattr(self, "_last_roll_attempt", None) or {}
        _last = _attempts.get(reason)
        if _last and (now - _last).total_seconds() < _ROLL_RETRY_SECONDS:
            return False
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
        # 2026-08-27, direct user spec ("one major change in rollover"): the real
        # LISTED strike grid (50pt for NIFTY), not the old artificial flat 100pt
        # ring step -- so a real, live, already-warm strike (e.g. NIFTY 24200)
        # can actually be considered instead of being structurally skipped.
        real_step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        max_itm = int(ss.get("roll_max_itm_steps", 5))

        # 2. FIND A VALID PARTNER for the running/bleeding leg.
        #    - premium must be <= kept leg (LOSING/kept leg's own LTP)
        #    - must pass the re-entry rule
        #    - must be within roll_max_itm_steps
        #    - must be at least _ROLLOVER_MIN_GAP_PTS away from the strike being closed
        def _rule_pass_with_detail(ce_s: int, pe_s: int):
            """Return (passed, reason, ind_by_tf) so the trace can show exact values."""
            ind = self._ind_by_tf(ce_s, pe_s, rules)
            passed, reason = _eval_rules(rules, ind)
            return passed, reason, ind

        _partner_trace: list = []
        # 2026-08-27, direct user spec ("one major change in rollover"): replaces the
        # old bidirectional anchor-ring search (select_partner_for) with a directional,
        # minimum-100pt-gap search on the REAL strike grid -- search only strikes LESS
        # OTM than orig_strike (moving toward the market price, never away from it),
        # skip anything closer than _ROLLOVER_MIN_GAP_PTS to orig_strike outright (even
        # if its premium is a perfect match), and take the FIRST strike (walking
        # outward) that also clears the premium condition (<= kept leg's LTP) and the
        # re-entry rule. See select_rollover_partner_directional's own docstring for
        # the full worked example this was built from.
        partner = select_rollover_partner_directional(
            self._strike_prem,
            roll_side=roll_side,
            kept_strike=keep_strike,
            kept_ltp=keep_ltp,
            closing_strike=orig_strike,
            # 2026-08-26, direct user confirmation: intrinsic/time-value stripping
            # is ALSO computed off the mean-of-spot-and-futures reference, not real
            # spot, for consistency with entry/expiry-shift selection (self._atm_ref
            # falls back to plain self._spot for a non-futures_atm underlying).
            spot=(self._atm_ref if self._atm_ref > 0 else self._spot),
            real_step=real_step,
            min_gap_pts=_ROLLOVER_MIN_GAP_PTS,
            rule_pass=_rule_pass_with_detail,
            max_itm_steps=max_itm,
            max_search_steps=max(1, offset * 2),
            trace=_partner_trace,
            itm_cap_step_pts=_ROLLOVER_STRIKE_STEP,
        )

        # 2026-09-23 fix (log-noise pass): this used to dump the full ~20-30
        # line candidate-by-candidate trace on EVERY 60s retry attempt (already
        # throttled to 60s, but a stuck position -- same blocker every cycle --
        # could keep dumping the identical trace for hours, per direct user
        # report of a live log repeating the exact same reject_counts every
        # minute). The one-line "no valid partner" summary below still fires on
        # EVERY attempt (that's the genuine "rollover checking should be
        # logged" signal) -- only the expensive full dump is now deduped: it
        # fires when the candidate actually passed, when the reject-count
        # signature genuinely changed since the last dump (a real change in
        # market conditions), or at least once every 5 minutes as a heartbeat
        # so a human watching the log never loses the detailed picture for long.
        _end_evt = next((t for t in _partner_trace if t.get("event") == "select_partner_for_end"), {})
        _sig = (roll_side, keep_strike, tuple(sorted((_end_evt.get("reject_counts") or {}).items())))
        _sig_map = getattr(self, "_last_roll_trace_sig", None) or {}
        _ts_map = getattr(self, "_last_roll_trace_dump_ts", None) or {}
        _prev_ts = _ts_map.get(reason)
        _heartbeat_due = (_prev_ts is None) or ((now - _prev_ts).total_seconds() >= 300)
        if partner is not None or _sig != _sig_map.get(reason) or _heartbeat_due:
            _trace_dump = _format_partner_trace(_partner_trace)
            _pool_diag = self._pool_warmth_diag(roll_side, candidate_count=max(1, offset * 2))
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s partner-search trace for running %s%d @%.2f "
                "(CE pnl=%.2f PE pnl=%.2f):\n%s\npool_warmth=%s",
                self._underlying, reason, keep_side, keep_strike, keep_ltp, ce_pnl, pe_pnl,
                _trace_dump, _pool_diag,
            )
            _sig_map[reason] = _sig
            _ts_map[reason] = now
            self._last_roll_trace_sig = _sig_map
            self._last_roll_trace_dump_ts = _ts_map

        if not partner:
            _summary = _summarize_partner_trace(_partner_trace)
            _why_plain = _summary.get("message", "no candidates")
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s — no valid partner; keeping original pair. reason: %s",
                self._underlying, reason, _why_plain,
            )
            return False

        new_strike, new_ltp = partner
        if int(new_strike) == orig_strike:
            self._clog.info(
                "SellStraddle[%s]: ROLLOVER %s — best partner is the SAME strike %d; "
                "no new pair, keeping original pair.", self._underlying, reason, orig_strike
            )
            return False

        # 3. MAX SKEW CHECK (max_entry_ratio).
        #    Because select_rollover_partner_directional guarantees new_ltp <= keep_ltp,
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
                return False

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
            return False

        # 5. Execute the roll: close the good leg FIRST, wait for the close fill,
        #    then open the new partner. This guarantees the buy-to-close is confirmed
        #    before the sell-to-open, avoiding a transient double-short / margin spike.
        # _close_leg itself now waits for the bridge's confirmation (or exit_aborted /
        # timeout) before returning -- see strategies/sell_straddle/exits.py.
        self._clog.info("SellStraddle[%s]: ROLL %s → %s%d @%.2f (good leg vs running %s%d @%.2f) [%s]",
                    self._underlying, roll_side, roll_side, new_strike, new_ltp,
                    keep_side, keep_strike, keep_ltp, reason)
        # 2026-08-26 fix (real incident, "MAJOR" per direct user flag): the whole roll
        # body below used to reset _roll_in_progress back to False only on specific
        # manual return paths (the close-aborted branch, and previously not at all on
        # a clean success). _check_exits' own _roll_in_progress guard sits ahead of
        # almost everything else in the exit ladder (Day%/ITMgate/DayLow/LTPdecay/
        # Ratio/ScalableTSL/exit_rules/VWAPrise -- see exits.py's own priority-order
        # comment), so ANY unhandled exception raised anywhere in this block (a
        # bridge/network error, a bug in the position-reset bookkeeping below, etc.)
        # would skip every manual reset and leave the flag stuck True FOREVER --
        # silently disabling nearly all protective exits for the rest of the
        # session with zero visible error (the caller, _check_exits via _tick_loop,
        # already catches and logs exceptions, so nothing would even crash loudly).
        # Confirmed live: a completed roll left zero EXIT-EVAL/SELECT/EVAL log lines
        # for 90+ minutes, with only the EOD force-squareoff path (ahead of this
        # guard) still able to fire. A try/finally guarantees the reset on every
        # possible exit from this block -- success, an aborted close, or a genuine
        # exception -- not just the return statements we happened to write.
        self._roll_in_progress = True
        try:
            close_ev = await self._close_leg(roll_side, reason, now)
            if getattr(close_ev, "close_aborted", False):
                self._clog.warning(
                    "SellStraddle[%s]: roll close leg not confirmed — aborting the open side to "
                    "avoid a naked/duplicate position. Original pair kept.",
                    self._underlying,
                )
                return False

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

            # 2026-08-27, direct user spec (broadens this from the original ITM-pair-gate-
            # only scoping): fund a protective stop on the freshly-rolled leg worth 70% of
            # the ₹ profit just booked by closing the good leg -- for EVERY single-side
            # roll, irrespective of whether it was ITM/OTM or which reason triggered it
            # (ltp_decay, ratio_exit, vwap_rise, exit_rules, scalable_tsl, or the ITM pair
            # gate itself all arm this the same way now). Once the new leg's own running
            # loss reaches this budget, _check_itm_roll_protection_side closes it and
            # shifts back to the SAME STRIKE that was closed during this rollover (if it
            # still passes re-entry), a pool-searched balanced replacement, or closes the
            # whole position if neither is available -- see that method's own docstring.
            # Keyed by side (roll_side) -- arming/clearing this side's budget must never
            # touch the other side's still-active budget (e.g. CE rolls again while PE's
            # protection from an earlier rollover is still armed and running).
            if not isinstance(getattr(self, "_itm_roll_protection", None), dict):
                self._itm_roll_protection = {}
            booked_pnl_rs = self._pnl_rs(float(getattr(close_ev, "realized_pnl", 0.0) or 0.0))
            # 2026-09-22, direct user instruction: disabled by default
            # (itm_roll_protection_enabled=False) -- a real incident on Gurmeet's
            # live book showed a rolled-in leg getting a 70% protection budget
            # while the ORIGINAL never-rolled leg on the same position had none,
            # an inconsistency the user wants off entirely until revisited.
            if not getattr(self, "_itm_roll_protection_enabled", False):
                self._itm_roll_protection.pop(roll_side, None)
            elif booked_pnl_rs > 0:
                protect_rs = 0.70 * booked_pnl_rs
                self._itm_roll_protection[roll_side] = {
                    "protect_rs": protect_rs,
                    "new_side": roll_side,
                    "new_strike": int(new_strike),
                    "orig_strike": orig_strike,
                    "kept_side": keep_side,
                    "kept_strike": keep_strike,
                }
                self._clog.info(
                    "SellStraddle[%s]: ITM-ROLL PROTECTION ARMED — %s%d budget=₹%.0f "
                    "(70%% of ₹%.0f booked on closed %s%d, reason=%s). Other side's budget "
                    "(if any) unaffected.",
                    self._underlying, roll_side, int(new_strike), protect_rs,
                    booked_pnl_rs, roll_side, orig_strike, reason,
                )
            else:
                self._itm_roll_protection.pop(roll_side, None)

            self._persist()
            await self._check_itm_pair_gate(now)
            return True
        finally:
            self._roll_in_progress = False

    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side."""
        other = "PE" if side == "CE" else "CE"
        close_ev = await self._close_leg(side, f"partial_roll_{reason}", now)
        if getattr(close_ev, "close_aborted", False):
            self._clog.warning(
                "SellStraddle[%s]: partial roll %s close not confirmed — leaving position "
                "as-is (no open side, no cleanup).", self._underlying, side,
            )
            return
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
        cleanup_ev = await self._close_leg(other, f"partial_cleanup_{reason}", now)
        if getattr(cleanup_ev, "close_aborted", False):
            self._clog.critical(
                "SellStraddle[%s]: partial-roll cleanup close of %s not confirmed — position "
                "left as-is (one leg already closed, one leg unconfirmed). RECONCILE MANUALLY.",
                self._underlying, other,
            )
            return
        self._position = None
        self._persist()

    @property
    def _contract_cv(self) -> float:
        """Contract value multiplier: BTC=0.001, ETH=0.01, NSE/BSE=1.0.
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
        """Today's booked P&L (all closed legs) + current running P&L in option pts.

        2026-08-28 fix: when the position is hedged (is_hedged_positional),
        the running P&L now also folds in the hedge legs' own running P&L
        (via _combined_pnl_pts, exits.py) -- this feeds the ITM-pair-gate's
        own profit threshold, which must treat all 4 legs as one position
        once hedged, same as every other full-close exit check (see
        _close_position_and_hedge's own docstring for the real incident)."""
        pos = self._position
        running = pos.unrealized_pnl if pos else 0.0
        running = self._combined_pnl_pts(pos, running) if pos else running
        return self._session_realized_pnl_pts + running

    async def _check_itm_pair_gate(self, now: datetime) -> None:
        """
        Called after every rollover completes and on every exit-check cycle while armed.
        Only activates (arms/watches) when both legs are ITM AND the point-distance between
        the two ITM strikes is > itm_pair_gate_min_strike_gap -- a narrow both-ITM pair is
        left alone entirely (no arm, no watch, no action).
        Once armed, when cumulative P&L in INR >= itm_pair_gate_profit_inr, attempt a
        rollover via _single_side_roll first; only if no valid partner is found does this
        fall back to closing both legs fully and restarting via re-entry.
        Does nothing if the toggle is OFF, the pair is not both ITM, or the gap is too small.
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

        ce_s = int(pos.ce_leg.strike)
        pe_s = int(pos.pe_leg.strike)
        min_gap = float(getattr(self, "_itm_pair_gate_min_strike_gap", 100.0))
        strike_gap = abs(ce_s - pe_s)
        if strike_gap <= min_gap:
            self._itm_gate_armed = False
            return

        cumulative_pts = self._cumulative_pnl_pts()
        cumulative_inr = self._pnl_rs(cumulative_pts)
        threshold_inr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))

        if cumulative_inr >= threshold_inr:
            # 2026-08-06 CRITICAL FIX: _single_side_roll's own tail unconditionally
            # re-calls _check_itm_pair_gate after every successful roll. When the
            # reason IS itm_pair_gate_profit_rollover, that reentrant call can see
            # the SAME still-both-ITM, still-over-threshold pair and attempt to
            # roll AGAIN with the identical reason -- but _last_roll_attempt's 60s
            # throttle (just set by the roll still unwinding) blocks it, and
            # _single_side_roll's "no partner found" return is indistinguishable
            # from that throttle block. Net effect: close 1 leg -> open 1 leg ->
            # (misread as "no partner") -> close both -> reopen fresh -- 4 real
            # orders instead of 2, immediately after the roll that just succeeded.
            # Guarded so the reentrant call this exact path triggers is a clean
            # no-op instead of a second attempt.
            if getattr(self, "_itm_gate_rolling", False):
                return
            self._itm_gate_armed = False
            logger.info(
                "SellStraddle[%s]: ITM-PAIR GATE — both legs ITM & gap=%d>%.0f (CE%d/PE%d) spot=%.0f "
                "cumulative=%.2f pts (₹%.2f) >= threshold ₹%.2f → attempting rollover.",
                self._underlying, strike_gap, min_gap, ce_s, pe_s, self._spot,
                cumulative_pts, cumulative_inr, threshold_inr,
            )
            self._itm_gate_rolling = True
            try:
                rolled = await self._single_side_roll(now, "itm_pair_gate_profit_rollover")
            finally:
                self._itm_gate_rolling = False
            if rolled:
                return
            logger.info(
                "SellStraddle[%s]: ITM-PAIR GATE — no rollover partner found; closing both and restarting.",
                self._underlying,
            )
            # 2026-08-28 fix: close-fallback (no roll partner found) is a FULL
            # exit, not a rollover -- must also close any standing hedge legs
            # (same "treat all 4 legs as one" fix as the Day%/TSL guardrails).
            await self._close_position_and_hedge("itm_pair_gate_profit")
            # No cooldown — restart immediately. Already goes via re-entry (not
            # beginning) since trades_today >= 1 at this point (a trade already
            # happened earlier today to reach this position) -- is_beginning is
            # trades_today==0, already False here regardless of _beginning_failed
            # (removed 2026-08-05: was a redundant belt-and-suspenders write).
            return

        # Below threshold: arm the gate and hold.  We wait for either the cumulative P&L
        # to cross the profit threshold (rollover attempt above) or the pair to stop being
        # both ITM / the gap to close back up.
        if not getattr(self, "_itm_gate_armed", False):
            self._itm_gate_armed = True
            logger.info(
                "SellStraddle[%s]: ITM-PAIR GATE ARMED — both legs ITM & gap=%d>%.0f (CE%d/PE%d) spot=%.0f "
                "cumulative=%.2f pts (₹%.2f) < threshold ₹%.2f. Holding; no rollover until profit threshold is met.",
                self._underlying, strike_gap, min_gap, ce_s, pe_s, self._spot,
                cumulative_pts, cumulative_inr, threshold_inr,
            )

    async def _check_itm_roll_protection(self, now: datetime) -> None:
        """Tick-level protective stop for the leg(s) just opened by an ITM-pair-gate
        rollover (part 2 of the 70% rule). Must run unconditionally every cycle, same
        cadence as _check_itm_pair_gate -- it cheaply no-ops when _itm_roll_protection
        is empty.

        Scoped strictly to the itm_pair_gate_profit_rollover path: standard ltp_decay /
        ratio / vwap_rise / scalable-TSL rolls never set _itm_roll_protection, so this
        check never fires for them.

        Tracked independently per side (CE/PE) -- roll_side is decided dynamically each
        time a rollover fires (whichever leg currently has the better P&L), so CE and PE
        can each roll via this gate at different times while the OTHER side's budget is
        still armed and running. Checking/clearing one side's entry must never touch the
        other side's.
        """
        prot_map = getattr(self, "_itm_roll_protection", None)
        if not isinstance(prot_map, dict):
            self._itm_roll_protection = {}
            return
        if not prot_map:
            return
        for side in ("CE", "PE"):
            prot = prot_map.get(side)
            if prot:
                await self._check_itm_roll_protection_side(side, prot, now)

    async def _check_itm_roll_protection_side(self, new_side: str, prot: dict, now: datetime) -> None:
        pos = self._position
        if not pos or pos.status != "open":
            self._itm_roll_protection.pop(new_side, None)
            return

        leg = pos.ce_leg if new_side == "CE" else pos.pe_leg
        if int(leg.strike) != int(prot["new_strike"]):
            # Position moved on since the roll (another roll / manual change) —
            # this protection budget no longer applies to whatever leg is open now.
            self._itm_roll_protection.pop(new_side, None)
            return

        pnl_pts = float(leg.entry_price or 0.0) - float(getattr(leg, "ltp", 0.0) or 0.0)
        running_loss_rs = -self._pnl_rs(pnl_pts) if pnl_pts < 0 else 0.0
        protect_rs = float(prot.get("protect_rs", 0.0) or 0.0)
        if protect_rs <= 0 or running_loss_rs < protect_rs:
            return

        self._clog.info(
            "SellStraddle[%s]: ITM-ROLL PROTECTION STOP — %s%d running loss ₹%.0f >= budget ₹%.0f "
            "(70%% of profit booked on the prior roll) — closing.",
            self._underlying, new_side, int(leg.strike), running_loss_rs, protect_rs,
        )
        self._itm_roll_protection.pop(new_side, None)
        stopped_strike = int(leg.strike)
        kept_side = prot["kept_side"]
        kept_strike = int(prot["kept_strike"])
        orig_strike = int(prot["orig_strike"])

        close_ev = await self._close_leg(new_side, "itm_roll_protection_stop", now)
        if getattr(close_ev, "close_aborted", False):
            self._clog.warning(
                "SellStraddle[%s]: ITM-roll protection stop close of %s not confirmed — "
                "leaving position as-is; protection budget stays armed for next tick.",
                self._underlying, new_side,
            )
            self._itm_roll_protection[new_side] = prot
            return
        pos = self._position
        if not pos or pos.status != "open":
            return

        kept_leg = pos.pe_leg if kept_side == "PE" else pos.ce_leg
        kept_ltp = float(getattr(kept_leg, "ltp", 0.0) or getattr(kept_leg, "entry_price", 0.0) or 0.0)

        from strategies.sell_straddle.selection import select_partner_for
        from strategies.core.rule_evaluator import eval_rules as _eval_rules_prot

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = _ROLLOVER_STRIKE_STEP
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        max_itm = int(ss.get("roll_max_itm_steps", 5))
        variable_strikes = bool(ss.get("variable_strikes", False))

        def _rule_pass(ce_s: int, pe_s: int) -> bool:
            ind = self._ind_by_tf(ce_s, pe_s, rules)
            passed, _reason = _eval_rules_prot(rules, ind)
            return bool(passed)

        # Step 1: does the strike we ORIGINALLY rolled out of still pass re-entry now?
        orig_v = self._strike_prem.get((orig_strike, new_side))
        orig_ltp = float(orig_v.get("ltp", 0.0) or 0.0) if orig_v else 0.0
        ce_s = orig_strike if new_side == "CE" else kept_strike
        pe_s = kept_strike if new_side == "CE" else orig_strike
        if orig_ltp > 0 and _rule_pass(ce_s, pe_s):
            self._clog.info(
                "SellStraddle[%s]: ITM-ROLL PROTECTION — restoring prior strike %s%d @%.2f (passes re-entry).",
                self._underlying, new_side, orig_strike, orig_ltp,
            )
            await self._open_leg(new_side, orig_strike, orig_ltp, now, "itm_roll_protection_restore")
            self._persist()
            return

        # Step 2: broader pool search for the still-running kept leg, excluding ONLY the
        # strike we just stopped out of (not orig_strike, which was already checked above).
        pool = {k: v for k, v in self._strike_prem.items()
                if not (k[0] == stopped_strike and k[1] == new_side)}
        partner = select_partner_for(
            pool, roll_side=new_side, kept_strike=kept_strike, kept_ltp=kept_ltp,
            spot=(self._atm_ref if self._atm_ref > 0 else self._spot),
            step=step, offset=offset, ltp_target=ltp_target,
            rule_pass=_rule_pass, max_itm_steps=max_itm, theta_target=self._theta_target,
            variable_strikes=variable_strikes, ltp_le_kept=True, metric="balanced_ratio",
        )
        if partner:
            pool_strike, pool_ltp = partner
            self._clog.info(
                "SellStraddle[%s]: ITM-ROLL PROTECTION — pool search found %s%d @%.2f (excl %d).",
                self._underlying, new_side, pool_strike, pool_ltp, stopped_strike,
            )
            await self._open_leg(new_side, int(pool_strike), float(pool_ltp), now, "itm_roll_protection_pool")
            self._persist()
            return

        self._clog.info(
            "SellStraddle[%s]: ITM-ROLL PROTECTION — no valid strike (old strike or pool); "
            "closing entire position.", self._underlying,
        )
        # 2026-08-28 fix: full exit, not a rollover -- must also close any
        # standing hedge legs (same "treat all 4 legs as one" fix as the
        # Day%/TSL/ITM-pair-gate guardrails).
        await self._close_position_and_hedge("itm_roll_protection_exit_all")

    def _apply_sl_cooldown(self, rule_key: str = "entry_rules_reentry") -> None:
        """Block re-entry until the next boundary of the max timeframe among
        `rule_key`'s rules. This makes the cooldown dynamic: if an exit happens
        mid-candle, re-entry is allowed only after that candle/tf closes.
        Additionally honours `sl_cooldown_minutes` from config so an underlying
        can rest longer between failed rolls without changing the indicator
        timeframe.

        `rule_key` defaults to entry_rules_reentry (every existing caller — a
        failed roll/SL on an already-running day). The hedge-cumulative-profit
        close (2026-08-24, user spec) passes entry_rules_beginning instead,
        since that close is meant to start the next attempt completely fresh,
        not as a same-day re-entry."""
        from data_layer.runtime_config import RuntimeConfig
        now = datetime.now(IST)
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get(rule_key, [])
        max_tf = max((int(r.get("tf", 1)) for r in rules), default=1)
        boundary = self._next_boundary(now, max_tf)
        fixed_minutes = float(getattr(self, "_sl_cooldown_minutes", 0.0) or 0.0)
        if fixed_minutes > 0:
            fixed_boundary = now + timedelta(minutes=fixed_minutes)
            if fixed_boundary > boundary:
                boundary = fixed_boundary
        self._sl_cooldown_until = boundary
        logger.info(
            "SellStraddle[%s]: re-entry cooldown dynamic (%s) — max_tf=%d min, fixed=%.0f min, no re-entry until %s.",
            self._underlying, rule_key, max_tf, fixed_minutes, boundary.strftime("%H:%M:%S"),
        )
