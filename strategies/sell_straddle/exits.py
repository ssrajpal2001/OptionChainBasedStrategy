"""
strategies/sell_straddle/exits.py — exit checks + close_position + close_leg/open_leg.

Implements the exact exit priority order and log format strings used by the engine.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time as dtime, timedelta
from typing import TYPE_CHECKING

from config.global_config import IST, Topic
from strategies.core.rule_evaluator import eval_rules as _eval_rules
from strategies.sell_straddle.audit import (
    audit_exit_eval,
    audit_exit_exec,
)
from strategies.sell_straddle.dataclasses import format_exit_eval
from strategies.core.support_resistance import SupportResistanceCalculator

if TYPE_CHECKING:
    from strategies.sell_straddle.dataclasses import StraddlePosition

logger = logging.getLogger(__name__)

# 2026-08-27, direct user correction: the EOD hedge-and-carry's cumulative-profit
# close fires once real profit (today's booked P&L + running P&L across all
# sold+hedge legs, converted to rupees) reaches this much -- NOT merely >=0/breakeven.
_HEDGE_CLOSE_PROFIT_RS = 500.0


class ExitMixin:
    """Exit-side logic for the sell-straddle book."""

    async def _seed_exec_legs(self, ce_strike: int, pe_strike: int) -> None:
        """At strike selection (entry), warm the EXACT exec strikes' RSI/ROC from REST 1m history."""
        try:
            if getattr(self, "_is_crypto", False):
                # Crypto indicators warm from live Delta ticks; no Upstox history available.
                return
            from data_layer.historical_candles import fetch_upstox_warm_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            import asyncio as _aio
            _max_tf = max((int(r.get("tf", 1)) for r in (self._exit_rules or [])), default=2)
            if self._pool_engine.warm_tf(ce_strike, pe_strike, _max_tf):
                return
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return
            pos = self._position
            if pos and pos.expiry_date:
                exp = pos.expiry_date
            else:
                exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            _need = self._pool_engine._rsi_len * max(_max_tf, 1) + 5
            for stk, side in ((ce_strike, "CE"), (pe_strike, "PE")):
                ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(stk), side, "upstox")
                if not ikey:
                    continue
                bars = await fetch_upstox_warm_1m(ikey, token, min_bars=_need)
                if bars:
                    closes = [b["close"] for b in bars]
                    self._pool_engine.seed_strike(int(stk), side, closes, closes)
            logger.info("SellStraddle[%s]: entry-seeded exec legs CE%d/PE%d from REST "
                        "(warm RSI/ROC up to tf=%d).", self._underlying, int(ce_strike),
                        int(pe_strike), _max_tf)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: entry-seed exec legs failed: %s", self._underlying, exc)

    async def _compute_day_low_for_pair(self, ce_strike: int, pe_strike: int,
                                        cutoff: dtime) -> float:
        """REST-fetch today's 1-min intraday history for a pair and compute
        the lowest combined (CE+PE) premium up to `cutoff` (a datetime.time),
        in ONE SHOT -- this is the whole point (2026-08-21 redesign, direct
        user spec): the day-low is no longer tracked tick-by-tick from
        whenever a pair starts running (that design depended on continuous
        in-memory tracking surviving every restart until the freeze moment,
        which it didn't -- a real 95.05 low at 14:20 was correctly tracked
        live, then silently lost to a routine restart at 14:49, well before
        the 15:00 freeze). Instead: don't track anything at all before the
        freeze time. The MOMENT the freeze time is reached (by however many
        restarts it took to get there), do exactly ONE REST fetch covering
        the pair's ENTIRE real trading history up to `cutoff` and compute the
        true low directly from that -- correct regardless of restart count,
        since it's derived from real historical data, not accumulated live
        state.

        Combines CE.close + PE.close minute-by-minute (aligned by timestamp)
        -- CLOSE (= LTP, "last traded"), matching how the live tracker itself
        already reads a position's value everywhere else (ce_ltp+pe_ltp).

        2026-08-31 CRITICAL FIX, direct user spec + real-data verification:
        this briefly used CE.low+PE.low instead (2026-08-21 direct user
        instruction: "we want the low value not the close value"), on the
        reasoning that the low-of-candle was a stricter/safer floor. Verified
        live against real data that this was wrong in practice: CE24250's own
        candle-low (80.15) and PE24100's own candle-low (92.0) occurred
        nearly 4.5 hours apart (10:12 vs 14:46) -- the low-based per-minute
        sum found its minimum (198.65) at 09:15, a value the real combined
        premium never actually traded at. Direct comparison against a live
        LTP-based straddle chart (Sensibull) showed the REAL observed low was
        ~225.60 around 11:50-11:53 -- an independent REST re-fetch of the
        same two legs' CLOSE prices reproduced that almost exactly (225.60 @
        11:53). Reverted to CLOSE, matching the ORIGINAL 2026-08-19 design.

        Returns float('inf') on ANY failure (crypto, no token, no data, no
        overlapping minutes, network error) -- caller falls back to the
        current tick's own value, never blocks or silently sets a wrong
        floor."""
        try:
            if getattr(self, "_is_crypto", False):
                return float("inf")
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return float("inf")
            pos = self._position
            if pos and pos.expiry_date:
                exp = pos.expiry_date
            else:
                exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            ce_key = REGISTRY.get_broker_symbol(self._underlying, exp, int(ce_strike), "CE", "upstox")
            pe_key = REGISTRY.get_broker_symbol(self._underlying, exp, int(pe_strike), "PE", "upstox")
            if not ce_key or not pe_key:
                return float("inf")
            ce_bars, pe_bars = await asyncio.gather(
                fetch_upstox_intraday_1m(ce_key, token),
                fetch_upstox_intraday_1m(pe_key, token),
            )
            if not ce_bars or not pe_bars:
                return float("inf")
            # Key by (hour, minute) only, not the raw ISO string -- Upstox 1-min
            # candles should always land on :00 seconds, but matching on
            # hour:minute directly removes any dependence on that holding exactly,
            # at zero cost (user's explicit instruction, 2026-08-21).
            def _hm(ts_str: str):
                t = datetime.fromisoformat(ts_str)
                return (t.hour, t.minute), t.time()
            # 2026-08-31 CRITICAL FIX, direct user spec + real-data verification:
            # reverted from CE.low+PE.low back to CE.close+PE.close. The
            # 2026-08-21 low-based instruction ("we want the low value not the
            # close value") produced a THEORETICAL floor that can be lower than
            # any value the combined premium ever actually traded at -- verified
            # live today: CE24250's own low (80.15) hit at 10:12, PE24100's own
            # low (92.0) hit at 14:46, nearly 4.5 hours apart. The low-based
            # per-minute-sum method found its minimum (198.65) at 09:15 -- a
            # value that direct comparison against a real LTP-based straddle
            # chart (Sensibull) showed was never the actual observed low; the
            # REAL low the user saw on their live chart was ~225.60 around
            # 11:50-11:53, which an independent REST re-fetch of the same two
            # legs' CLOSE prices reproduced almost exactly (225.60 @ 11:53).
            # CLOSE (= LTP, "last traded") is what a real trader's chart plots
            # and what the live tracker itself already uses elsewhere
            # (ce_ltp+pe_ltp) -- matches the ORIGINAL 2026-08-19 design this
            # function had before the since-reverted 2026-08-21 change.
            pe_close_by_hm = {}
            for b in pe_bars:
                (hm, t) = _hm(b["ts"])
                if t <= cutoff:
                    pe_close_by_hm[hm] = float(b["close"])
            combined = []
            for b in ce_bars:
                (hm, t) = _hm(b["ts"])
                if t <= cutoff and hm in pe_close_by_hm:
                    combined.append(float(b["close"]) + pe_close_by_hm[hm])
            if not combined:
                return float("inf")
            _low = min(combined)
            self._clog.info(
                "SellStraddle[%s]: DAY-LOW ONE-TIME CALC — fetched %d aligned 1m bars "
                "(CE.close+PE.close, up to %s) for CE%d/PE%d, low=%.2f.",
                self._underlying, len(combined), cutoff.strftime("%H:%M"),
                int(ce_strike), int(pe_strike), _low,
            )
            return _low
        except Exception as exc:
            self._clog.warning("SellStraddle[%s]: day-low one-time calc failed for CE%d/PE%d: %s",
                                self._underlying, int(ce_strike), int(pe_strike), exc)
            return float("inf")

    def _exit_max_tf(self, reason: str) -> int:
        """Maximum timeframe (minutes) for an exit reason.
        Rule-based reasons use the max tf of their rule list; tick-based reasons default to 1."""
        if reason == "exit_rules":
            return max((int(r.get("tf", 1)) for r in (self._exit_rules or [])), default=1)
        if reason == "vwap_rise":
            return int(getattr(self, "_vwap_rise_tf", 1) or 1)
        return 1

    @staticmethod
    def _at_exit_boundary(now: datetime, tf: int) -> bool:
        """True when `now` is at the execution boundary for a tf-minute decision."""
        return now.minute % tf == 0 and now.second >= 5

    @staticmethod
    def _next_boundary(now: datetime, tf: int) -> datetime:
        """Next execution boundary at or after `now`."""
        rem = now.minute % tf
        if rem == 0 and now.second < 5:
            return now.replace(second=5, microsecond=0)
        add = tf - rem
        nxt = now + timedelta(minutes=add)
        return nxt.replace(second=5, microsecond=0)

    def _defer_exit(self, reason: str, now: datetime) -> bool:
        """Return True if we are at the boundary for this reason's max tf and should execute now.
        Otherwise log once and defer to the next boundary."""
        tf = self._exit_max_tf(reason)
        if self._at_exit_boundary(now, tf):
            self._exit_pending_reason = None
            return True
        if getattr(self, "_exit_pending_reason", None) != reason:
            self._exit_pending_reason = reason
            self._exit_pending_tf = tf
            boundary = self._next_boundary(now, tf)
            logger.info(
                "SellStraddle[%s]: EXIT %s triggered mid-candle — deferring to next %d-min boundary at %s",
                self._underlying, reason, tf, boundary.strftime("%H:%M:%S"),
            )
        return False

    def _build_exit_criteria(self, pos, pnl: float, credit: float):
        """Build the live exit-criteria list (and per-tf indicator dump).

        2026-08-06 CRITICAL FIX: this used to be one giant try/except around
        every criterion -- an exception in an EARLIER section (e.g. Day%)
        silently discarded every criterion after it, including the Dynamic
        (exit_rules) stop-loss, with zero log trace. Indistinguishable from
        "rules genuinely didn't pass" -- the single most dangerous failure
        mode for a stop-loss: it silently stops firing. Each criterion is now
        independently guarded so one failure can't suppress the others, and
        every failure is logged instead of swallowed."""
        _crit = []
        _exit_dump = None
        try:
            _dpt = float(getattr(self, "_day_profit_target_pct", 0.0) or 0.0)
            _dsl = float(getattr(self, "_day_loss_sl_pct", 0.0) or 0.0)
            if credit and (_dpt or _dsl):
                _dpct = self._day_pct(pos)
                _lbl = "Day%(θ)" if self._day_exit_basis == "theta" else "Day%"
                _crit.append((_lbl, f"{_dpct:.1f}% vs T{_dpt:.0f}/SL{_dsl:.0f}",
                              (_dpt > 0 and _dpct >= _dpt) or (_dsl > 0 and _dpct <= -_dsl)))
            elif not credit:
                _crit.append(("Day%", "SKIPPED (initial_credit=0!)", False))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria Day%% failed: %s", self._underlying, exc)

        _ce_ltp = float(getattr(getattr(pos, "ce_leg", None), "ltp", 0) or 0)
        _pe_ltp = float(getattr(getattr(pos, "pe_leg", None), "ltp", 0) or 0)

        try:
            if self._ltp_decay_enabled:
                _lo = min(_ce_ltp, _pe_ltp) if (_ce_ltp > 0 and _pe_ltp > 0) else 0.0
                _crit.append(("LTPdecay", f"min({_lo:.1f}) < {self._ltp_exit_min:.0f}",
                              _lo > 0 and _lo < self._ltp_exit_min))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria LTPdecay failed: %s", self._underlying, exc)

        try:
            if _ce_ltp > 0 and _pe_ltp > 0 and getattr(self, "_ratio_threshold", 0.0):
                _r = max(_ce_ltp, _pe_ltp) / min(_ce_ltp, _pe_ltp)
                _crit.append(("Ratio", f"{_r:.2f} vs {self._ratio_threshold:.1f}x", _r >= self._ratio_threshold))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria Ratio failed: %s", self._underlying, exc)

        try:
            if self._tsl_enabled:
                _crit.append(("TSL", "ON (scalable)", False))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria TSL failed: %s", self._underlying, exc)

        try:
            if self._vwap_rise_enabled:
                _stale = not self._pool_engine.pair_atp_fresh(
                    pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec)
                _crit.append(("VWAPrise",
                              f"ON {self._vwap_rise_threshold:.1f}%{' STALE-skip' if _stale else ''}", False))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria VWAPrise failed: %s", self._underlying, exc)

        try:
            if self._exit_rules:
                _exit_ind_by_tf = self._ind_by_tf(pos.ce_leg.strike, pos.pe_leg.strike, self._exit_rules)
                _passed, _reason = _eval_rules(self._exit_rules, _exit_ind_by_tf)
                _crit.append(("Dynamic", _reason, _passed))
                _exit_dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()}
                              for tf, d in _exit_ind_by_tf.items()}
                if 1 in _exit_dump:
                    _exit_dump[1]["stale"] = (0.0 if self._pool_engine.pair_atp_fresh(
                        pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec) else 1.0)
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria Dynamic(exit_rules) failed "
                         "-- this SL is NOT being evaluated this cycle: %s", self._underlying, exc)

        try:
            if getattr(self, "_itm_pair_gate_enabled", False):
                _both = self._both_itm()
                _cum_inr = self._pnl_rs(self._cumulative_pnl_pts()) if _both else 0.0
                _thr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))
                _crit.append(("ITMgate", f"bothITM={_both} cum₹{_cum_inr:.0f} vs threshold₹{_thr:.0f}",
                              _both and _cum_inr >= _thr))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria ITMgate failed: %s", self._underlying, exc)

        # 2026-08-27, direct user request: the 70%-of-booked-profit roll-protection
        # stop (rolling.py's _check_itm_roll_protection_side) only ever logged when
        # it actually FIRED -- there was no visibility that this check was even
        # running while armed but not yet triggered. Surfaced here so it appears in
        # the same periodic EXIT-CHECK log line + UI panel as every other exit rule,
        # every cycle, whether or not it's currently armed.
        try:
            _prot_map = getattr(self, "_itm_roll_protection", None) or {}
            for _side, _prot in _prot_map.items():
                _leg = pos.ce_leg if _side == "CE" else pos.pe_leg
                _pnl_pts = float(_leg.entry_price or 0.0) - float(getattr(_leg, "ltp", 0.0) or 0.0)
                _running_loss_rs = -self._pnl_rs(_pnl_pts) if _pnl_pts < 0 else 0.0
                _budget_rs = float(_prot.get("protect_rs", 0.0) or 0.0)
                _crit.append((
                    f"ITMrollProt({_side})",
                    f"strike={int(_leg.strike)} loss=₹{_running_loss_rs:.0f} vs budget=₹{_budget_rs:.0f} "
                    f"(70% of profit booked on the prior roll)",
                    _budget_rs > 0 and _running_loss_rs >= _budget_rs,
                ))
        except Exception as exc:
            logger.error("SellStraddle[%s]: _build_exit_criteria ITMrollProt failed: %s", self._underlying, exc)

        return _crit, _exit_dump

    def _day_pct(self, pos) -> float:
        """Session day-% using the same basis as the guardrail trigger in _check_exits."""
        if self._day_exit_basis == "theta" and getattr(self, "_initial_entry_time_value", 0.0) > 0:
            _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
            _running_theta = (_etv - pos.current_time_value(self._spot)) if _etv > 0 else pos.unrealized_pnl
            _day_pts = self._session_realized_pnl_pts + _running_theta
            _day_denom = self._initial_entry_time_value
            return (_day_pts / _day_denom * 100.0) if _day_denom > 0 else 0.0
        _credit = self._initial_net_credit or pos.net_credit or 0.0
        return ((self._session_realized_pnl_pts + pos.unrealized_pnl) / _credit * 100.0) if _credit else 0.0

    def _close_remark(self, pos, reason: str, now: datetime, side: str = "") -> str:
        """Build a human-readable remark for a full or single-leg exit.

        `reason` is the internal close code (kept for tests / throttling).  This method
        returns display text with live numbers (peak%, thresholds, ratios, etc.) so the
        dashboard History column is immediately understandable.
        """
        try:
            _side = side or (
                "CE"
                if (pos.ce_leg.entry_price - pos.ce_leg.ltp) >= (pos.pe_leg.entry_price - pos.pe_leg.ltp)
                else "PE"
            )
            _full_close = not side

            if reason in ("eod_squareoff", "time_exit_eod"):
                _pnl = self._pnl_rs(pos.realized_pnl) if pos else 0.0
                return f"EOD square-off at {now.strftime('%H:%M')} | P&L {self._ccy_symbol}{_pnl:+.0f}"

            if reason == "day_profit_target":
                _pct = self._day_pct(pos)
                return (f"Day profit target | day={_pct:.1f}% (basis {self._day_exit_basis}) "
                        f"vs T={self._day_profit_target_pct:.1f}%")

            if reason == "day_loss_sl":
                _pct = self._day_pct(pos)
                return (f"Day loss stoploss | day={_pct:.1f}% (basis {self._day_exit_basis}) "
                        f"vs SL={self._day_loss_sl_pct:.1f}%")

            if reason == "itm_pair_gate":
                _cum = self._session_realized_pnl_pts + pos.unrealized_pnl
                _inr = self._pnl_rs(_cum)
                _thr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))
                return (f"ITM pair gate escape | both legs ITM (CE{int(pos.ce_leg.strike)}/PE{int(pos.pe_leg.strike)}) "
                        f"spot={self._spot:.0f} cumulative {self._ccy_symbol}{_inr:+.0f} < threshold {self._ccy_symbol}{_thr:.0f} "
                        f"→ rolled {_side}")

            if reason == "itm_pair_gate_profit":
                _cum = self._session_realized_pnl_pts + pos.unrealized_pnl
                _inr = self._pnl_rs(_cum)
                _thr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))
                return (f"Both legs ITM | cumulative P&L {self._ccy_symbol}{_inr:+.0f} ≥ threshold {self._ccy_symbol}{_thr:.0f} "
                        f"| CE{int(pos.ce_leg.strike)} PE{int(pos.pe_leg.strike)} spot={self._spot:.0f}")

            if reason == "day_low_reversal_exit":
                _cv = pos.current_value
                _frozen = getattr(self, "_session_min_straddle_frozen", 0.0) or 0.0
                return (f"Day-low reversal exit | CE{int(pos.ce_leg.strike)} PE{int(pos.pe_leg.strike)} "
                        f"rate={_cv:.2f} reached its own frozen low={_frozen:.2f} "
                        f"(frozen @{self._day_low_freeze_time.strftime('%H:%M')}) "
                        f"→ closed, stopped for day")

            if reason == "post1500_r1_breach":
                _armed = getattr(self, "_post1500_armed_reason", None) or "?"
                _leg = pos.ce_leg if _side == "CE" else pos.pe_leg
                if _full_close:
                    return (f"Post-15:00 R1 exit | both legs closed independently on their own "
                            f"R1 breach (armed via {_armed}) → position closed, stopped for day")
                return (f"Post-15:00 R1 exit | {_side} leg (strike={int(_leg.strike)}) breached its own "
                        f"R1, closed independently (armed via {_armed}) → other leg still running solo")

            if reason == "r1_breach_post_roll":
                _info = getattr(self, "_r1_last_breach_info", None) or {}
                return (
                    f"R1 breach (post-roll watch) | {_side} leg (strike={int(_info.get('strike', 0))}) "
                    f"phase={_info.get('phase', '?')} R1_established={_info.get('r1_established', '?')} "
                    f"R1={float(_info.get('r1', 0.0) or 0.0):.2f} ltp={float(_info.get('ltp', 0.0) or 0.0):.2f} "
                    f"→ closed this rolled-in leg early, now searching for an immediate replacement pair"
                )

            if reason == "r1_reentry_giveup_no_pair":
                _pending = getattr(self, "_r1_pending", None) or {}
                _summary = _pending.get("last_search_summary") or {}
                return (
                    f"R1-reentry give-up | no partner passed within "
                    f"{float(getattr(self, '_R1_GIVEUP_SECONDS', 60.0)):.0f}s of retrying "
                    f"(checked={_summary.get('checked')} reject_counts={_summary.get('reject_counts')}) "
                    f"→ closed remaining leg, resuming fresh BEGINNING entry"
                )

            if reason == "r1_reentry_ltp_below_threshold":
                return (
                    f"R1-reentry candidate below ltp_target floor "
                    f"(target={self._ltp_target if self._ltp_target > 0 else 50.0:.2f}) "
                    f"→ closed remaining leg, shifted to next week's expiry"
                )

            if reason.startswith("manual_squareoff_"):
                return f"Manual square-off ({reason})"
            if reason == "kill_switch":
                return "Kill switch / emergency liquidation"
            if reason == "deployment_stop":
                return "Deployment stopped"
            if reason == "system_shutdown":
                return "System shutdown"

            if reason.startswith("trailing_sl_"):
                _basis = reason.split("_")[-1] if "_" in reason else self._trail_basis
                if _basis == "theta":
                    _profit_pct = pos.premium_decay_pct()
                elif pos.net_credit > 0:
                    _profit_pct = pos.unrealized_pnl / pos.net_credit * 100.0
                else:
                    _profit_pct = 0.0
                _lock_pct = self._trail_lock_pct * 100.0
                _floor_pct = self._trail_floor_pct * 100.0
                _roll = "closed" if _full_close else f"rolled {_side}"
                return (f"Trailing SL ({_basis}) | profit={_profit_pct:.1f}% peak={pos.trail_peak_pct:.1f}% "
                        f"lock≥{_lock_pct:.1f}% floor={_floor_pct:.1f}% → {_roll}")

            if reason == "ltp_decay":
                _min = min(pos.ce_leg.ltp, pos.pe_leg.ltp) if pos.ce_leg and pos.pe_leg else 0.0
                _roll = "closed" if _full_close else f"rolled {_side}"
                return f"LTP decay | min LTP={_min:.2f} < threshold={self._ltp_exit_min:.2f} → {_roll}"

            if reason == "ratio_exit":
                _ce = pos.ce_leg.ltp if pos.ce_leg else 0.0
                _pe = pos.pe_leg.ltp if pos.pe_leg else 0.0
                _r = max(_ce, _pe) / min(_ce, _pe) if _ce > 0 and _pe > 0 else 0.0
                _roll = "closed" if _full_close else f"rolled {_side}"
                return f"Ratio exit | leg ratio={_r:.2f}x ≥ threshold={self._ratio_threshold:.1f}x → {_roll}"

            if reason == "scalable_tsl":
                _pnl = pos.unrealized_pnl
                if self._tsl_basis == "theta":
                    _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                    if _etv > 0:
                        _pnl = _etv - pos.current_time_value(self._spot)
                _profit_rs = self._pnl_rs(_pnl)
                return (f"Scalable TSL ({self._tsl_basis}) hit | locked={self._ccy_symbol}{pos.tsl_high_lock_rs:.0f} "
                        f"current={self._ccy_symbol}{_profit_rs:.0f} → FULL EXIT (reset)")

            if reason == "exit_rules":
                _credit = self._initial_net_credit or pos.net_credit or 0.0
                _crit, _ = self._build_exit_criteria(pos, pos.unrealized_pnl, _credit)
                _dyn = next((d for d in _crit if d[0] == "Dynamic"), None)
                _detail = _dyn[1] if _dyn else "rules fired"
                _roll = "closed" if _full_close else f"rolled {_side}"
                return f"Dynamic exit rules | {_detail} → {_roll}"

            if reason == "vwap_rise_roll":
                _vp = self._pool_engine.pair_indicators(int(pos.ce_leg.strike), int(pos.pe_leg.strike)) or {}
                _curr = float(_vp.get("vwap", 0.0))
                _min = getattr(pos, "session_min_vwap", 0.0) or 0.0
                _rise = ((_curr - _min) / _min * 100.0) if _min > 0 else 0.0
                _roll = "closed" if _full_close else f"rolled {_side}"
                return (f"VWAP rise | VWAP {_curr:.2f} rose {_rise:.2f}% from session low {_min:.2f} "
                        f"(threshold {self._vwap_rise_threshold:.2f}%) → {_roll}")

            if reason == "itm_pair_gate_profit_rollover":
                _cum_inr = self._pnl_rs(self._cumulative_pnl_pts())
                _thr = float(getattr(self, "_itm_pair_gate_profit_inr", 500.0))
                return (f"ITM pair gate rollover | both legs ITM, gap>{self._itm_pair_gate_min_strike_gap:.0f}pts, "
                        f"cumulative {self._ccy_symbol}{_cum_inr:+.0f} ≥ threshold {self._ccy_symbol}{_thr:.0f} "
                        f"→ rolled {_side}")

            if reason == "itm_roll_protection_stop":
                return f"ITM-roll protection stop | {_side} leg hit 70%-of-booked-profit loss budget → closed"
            if reason == "itm_roll_protection_restore":
                return f"ITM-roll protection | restored prior strike {_side} (passed re-entry)"
            if reason == "itm_roll_protection_pool":
                return f"ITM-roll protection | pool-selected new {_side} partner (excl. stopped-out strike)"
            if reason == "itm_roll_protection_exit_all":
                return "ITM-roll protection | no valid strike found — closed entire position"

            if reason.startswith("partial_roll_"):
                return f"Partial roll | closed old {_side} leg"
            if reason.startswith("partial_cleanup_"):
                return f"Partial roll failed | closed {_side} leg to flatten"
            if reason.startswith("single_side_cleanup_"):
                return f"No rollover partner | closed {_side} leg"
            if reason.startswith("single_side_roll_"):
                return f"Single-side roll | closed old {_side} leg"

            return reason
        except Exception as _exc:
            return reason

    async def _publish_exit_audit(self, pos, pnl: float, now: datetime) -> None:
        """Publish the live exit-criteria to enabled client UIs, throttled to ~3s."""
        _audit_clients = self._granular_audit_clients()
        if not _audit_clients:
            return
        import time as _t
        if _t.monotonic() - getattr(self, "_last_audit_pub", 0.0) < 3.0:
            return
        self._last_audit_pub = _t.monotonic()
        _credit = self._initial_net_credit or pos.net_credit or 0.0
        _crit, _exit_dump = self._build_exit_criteria(pos, pnl, _credit)
        _criteria = [{"name": _n, "detail": _d, "hit": bool(_h)} for (_n, _d, _h) in _crit]
        for _cid, _bid in _audit_clients:
            await self._bus.publish(Topic.EXIT_AUDIT, {
                "type": "exit_audit", "client_id": _cid, "binding_id": _bid,
                "underlying": self._underlying, "pnl": round(pnl, 2),
                "credit": round(_credit, 2), "criteria": _criteria,
                "ind_by_tf": _exit_dump or {}, "ts": now.timestamp(),
            })

    # ── EOD hedge-and-carry (2026-08-20, user spec) ─────────────────────────────

    def _is_t1_from_expiry(self, pos: "StraddlePosition", now: datetime) -> bool:
        """True on the REAL trading day immediately before expiry (or on/after
        expiry itself). 2026-10-06 CRITICAL FIX, direct user correction: this used
        to be a plain calendar-date subtraction (`(expiry_date - now.date()).days
        <= 1`), which breaks whenever a real NSE holiday falls between now and
        expiry -- e.g. a Tuesday-expiry week with a Monday holiday: the TRUE T-1
        trading day is Friday, but the old check never read True on Friday (4
        calendar days out) and the holiday Monday itself has no trading at all, so
        this would have only ever fired on expiry day itself -- too late, since
        the whole point of T-1 is to act the day BEFORE expiry. Now computed via
        data_layer.trading_calendar.previous_trading_day (weekends + real NSE
        holidays, see that module's own docstring for why its holiday list starts
        empty rather than guessed).

        2026-09-10, direct user spec: when same_day_expiry_enabled is on (testing
        running the entry side through expiry day rather than shifting to next week),
        the hedge-and-carry side's own pre-emptive T-1/T-0 roll-to-next-week no
        longer makes sense either -- both flags now move together so a carried
        position isn't rolled away from the same-week contract the entry side was
        deliberately told to keep using."""
        if getattr(self, "_same_day_expiry_enabled", False):
            return False
        if not pos.expiry_date:
            return False
        from data_layer.trading_calendar import previous_trading_day
        t1_trading_day = previous_trading_day(pos.expiry_date)
        return now.date() >= t1_trading_day

    def _cumulative_hedge_pnl(self, pos: "StraddlePosition", include_hedge: bool = False) -> float:
        """2026-08-24, user spec correction: the hedge decision is driven by
        the OVERALL cumulative P&L -- today's already-booked P&L plus the
        running P&L on the sold legs -- NOT by requiring each leg to
        individually be in loss. CE +50 / PE -80 (net -30) now qualifies,
        where the old per-leg check would have skipped it since CE alone was
        "in profit". `include_hedge=True` additionally nets in the hedge
        legs' own running P&L (0.0 for any not yet built) -- used for the
        tick-by-tick close-the-hedge check, not the build trigger (no hedge
        legs exist yet at that point, so it would be a no-op either way)."""
        total = self._session_realized_pnl_pts + pos.unrealized_pnl
        if include_hedge:
            total += pos.hedge_unrealized_pnl
        return total

    async def _dispatch_hedge_order(
        self, action: str, side: str, strike: int, price: float, entry_price: float,
        expiry, reason: str, entry_ts=None,
    ):
        """BUY (open) or SELL (close) one hedge leg via the standalone
        StraddleHedgeExecutionBridge (Topic.STRADDLE_HEDGE_ORDER_REQUEST/FILL) --
        never touches the sold-leg StraddleOrderEvent/StraddleFillEvent flow.
        Waits (confirm-then-finalize, same contract every bridge in this codebase
        uses) for the matching StraddleHedgeFillEvent. Returns that event, or None
        on a 15s confirm timeout."""
        from strategies.sell_straddle.hedge_events import StraddleHedgeOrderEvent
        self._event_counter += 1
        eid = f"{self._underlying}_HEDGE_{action}_{side}{int(strike)}_{self._event_counter}"
        qty = self._lot_size * self._lot_multiplier
        order_ev = StraddleHedgeOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action=action,
            underlying=self._underlying, option_type=side, strike=int(strike),
            expiry=expiry, quantity=qty, entry_price=entry_price,
            exit_price=price if action == "SELL" else 0.0,
            reason=reason, event_id=eid, entry_ts=entry_ts,
            product_type=self._current_product_type(),
            # 2026-09-07 real incident fix: left at the dataclass default
            # ("sell_straddle") this would fail straddle_hedge_bridge.py's own
            # can_trade(ev.strategy, ...) ENTRY gate for a sell_straddle_calc_vwap
            # book near EOD hedge-and-carry, the exact same class of bug found and
            # fixed the same day in entries.py/straddle_bridge.py.
            strategy=self._strategy_name,
        )
        waiter = asyncio.Event()
        self._hedge_fill_waiters[eid] = waiter
        try:
            await self._bus.publish(Topic.STRADDLE_HEDGE_ORDER_REQUEST, order_ev)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                logger.critical(
                    "SellStraddle[%s]: HEDGE %s %s%d fill NOT CONFIRMED within 15s "
                    "(event_id=%s reason=%s).",
                    self._underlying, action, side, strike, eid, reason,
                )
                return None
        finally:
            self._hedge_fill_waiters.pop(eid, None)
        return self._hedge_fill_results.pop(eid, None)

    async def _try_build_hedge(self, pos: "StraddlePosition", now: datetime) -> bool:
        """Both sold legs in loss at EOD, not T-1 -- find + buy a protective leg for
        each side (further OTM, LTP <=50% of the running sold leg's own LTP, same
        qty). Returns True once the position is (at least partially) hedged and
        should NOT also receive a normal EOD close; False if no valid hedge could be
        built at all, so the caller should fall back to a normal close."""
        from strategies.sell_straddle.selection import find_hedge_strike
        from strategies.sell_straddle.dataclasses import StraddleLeg

        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        ce_ltp = float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0)
        pe_ltp = float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0)
        ce_hedge = find_hedge_strike(self._strike_prem, "CE", pos.ce_leg.strike, ce_ltp, step)
        pe_hedge = find_hedge_strike(self._strike_prem, "PE", pos.pe_leg.strike, pe_ltp, step)

        if ce_hedge is None or pe_hedge is None:
            self._clog.info(
                "HEDGE — no valid 50%%-or-below strike found for %s -- falling back to normal EOD close",
                "CE" if ce_hedge is None else "PE",
            )
            return False
        ce_strike, ce_hedge_ltp = ce_hedge
        pe_strike, pe_hedge_ltp = pe_hedge

        # Degenerate case (2026-08-20 user spec): the computed hedge strike lands on
        # the exact same strike as the sold leg it's meant to protect -- buying that
        # back is not a real hedge, it's just closing the leg. Don't hedge; let the
        # caller do a normal full close instead.
        if int(ce_strike) == int(pos.ce_leg.strike) or int(pe_strike) == int(pos.pe_leg.strike):
            self._clog.info(
                "HEDGE — computed hedge strike collided with its own sold leg's strike "
                "(CE %d vs %d, PE %d vs %d) -- skipping hedge, normal close instead",
                ce_strike, int(pos.ce_leg.strike), pe_strike, int(pos.pe_leg.strike),
            )
            return False

        ce_fill = await self._dispatch_hedge_order(
            "BUY", "CE", ce_strike, ce_hedge_ltp, ce_hedge_ltp, pos.expiry_date, "eod_hedge",
        )
        if ce_fill is None or ce_fill.entry_aborted or ce_fill.routing_failed or ce_fill.fill_price <= 0:
            self._clog.critical("HEDGE — CE %d BUY failed/unconfirmed -- aborting hedge, normal EOD close instead", ce_strike)
            return False

        # 2026-09-15, real incident fix: hedge strikes are deliberately far OTM (<=50%
        # of the sold leg's LTP) and are NOT the position's own pinned CE/PE strikes --
        # the StrikeRebalancer's normal ATM-window rebalance can drop them the moment
        # spot drifts, silently killing live ticks for a leg that's still genuinely
        # open. Pin both hedge strikes the same way the sold legs already are (see
        # _restore_pool_seed's re-pin comment above) so they can never be unsubscribed
        # while the hedge is standing.
        if not self._is_crypto and self._rebalancer:
            try:
                self._rebalancer.pin_strike(self._underlying, float(ce_strike))
                self._rebalancer.pin_strike(self._underlying, float(pe_strike))
            except Exception as exc:
                logger.warning("SellStraddle[%s]: pin_strike for hedge legs failed: %s",
                               self._underlying, exc)

        pe_fill = await self._dispatch_hedge_order(
            "BUY", "PE", pe_strike, pe_hedge_ltp, pe_hedge_ltp, pos.expiry_date, "eod_hedge",
        )
        pos.hedge_ce_leg = StraddleLeg("CE", ce_strike, ce_fill.fill_price, ce_fill.fill_price,
                                       open_time=now, open_reason="eod_hedge")
        if pe_fill is None or pe_fill.entry_aborted or pe_fill.routing_failed or pe_fill.fill_price <= 0:
            # CE already genuinely bought -- do NOT discard it, it's a real position.
            # Mark hedged (partial) and flag loudly for manual review rather than
            # silently leaving a real bought leg untracked.
            logger.critical(
                "SellStraddle[%s]: HEDGE PE %d BUY failed/unconfirmed AFTER CE already filled -- "
                "position is PARTIALLY hedged (CE only). Needs manual review.",
                self._underlying, pe_strike,
            )
            self._clog.critical(
                "HEDGE PE %d BUY failed AFTER CE%d already filled @ %.2f -- PARTIALLY hedged, "
                "needs manual review", pe_strike, ce_strike, ce_fill.fill_price,
            )
            pos.is_hedged_positional = True
            self._persist()
            return True

        pos.hedge_pe_leg = StraddleLeg("PE", pe_strike, pe_fill.fill_price, pe_fill.fill_price,
                                       open_time=now, open_reason="eod_hedge")
        pos.is_hedged_positional = True
        logger.info(
            "SellStraddle[%s]: HEDGE BUILT — CE%d@%.2f PE%d@%.2f -- position converted to "
            "positional carry (NRML), will only close on T-1-from-expiry or a same-strike "
            "collision after a future rollover.",
            self._underlying, ce_strike, ce_fill.fill_price, pe_strike, pe_fill.fill_price,
        )
        self._clog.info(
            "HEDGE BUILT — CE%d@%.2f PE%d@%.2f -- position converted to positional carry (NRML)",
            ce_strike, ce_fill.fill_price, pe_strike, pe_fill.fill_price,
        )
        self._persist()
        return True

    async def _close_hedge_legs(self, pos: "StraddlePosition", reason: str) -> None:
        """Close (SELL) both hedge legs, if any. Never touches the sold legs --
        caller is responsible for closing those separately (_close_position)."""
        for attr in ("hedge_ce_leg", "hedge_pe_leg"):
            leg = getattr(pos, attr, None)
            if leg is None:
                continue
            current = self._strike_prem.get((int(leg.strike), leg.option_type), {})
            current_ltp = float(current.get("ltp", 0.0) or 0.0) or float(leg.ltp or 0.0)
            fill = await self._dispatch_hedge_order(
                "SELL", leg.option_type, int(leg.strike), current_ltp,
                leg.entry_price, pos.expiry_date, reason, entry_ts=leg.open_time,
            )
            if fill is None or fill.exit_failed or fill.routing_failed:
                logger.critical(
                    "SellStraddle[%s]: HEDGE CLOSE %s%d NOT confirmed (reason=%s) -- hedge leg "
                    "stays OPEN, will retry next cycle.",
                    self._underlying, leg.option_type, int(leg.strike), reason,
                )
                self._clog.critical(
                    "HEDGE CLOSE %s%d NOT confirmed (reason=%s) -- stays open, retrying",
                    leg.option_type, int(leg.strike), reason,
                )
                continue
            logger.info(
                "SellStraddle[%s]: HEDGE CLOSED — %s%d @ %.2f (reason=%s).",
                self._underlying, leg.option_type, int(leg.strike), fill.fill_price, reason,
            )
            self._clog.info("HEDGE CLOSED — %s%d @ %.2f (reason=%s)",
                            leg.option_type, int(leg.strike), fill.fill_price, reason)
            if not self._is_crypto and self._rebalancer:
                try:
                    self._rebalancer.unpin_strike(self._underlying, float(leg.strike))
                except Exception as exc:
                    logger.warning("SellStraddle[%s]: unpin_strike for hedge leg failed: %s",
                                   self._underlying, exc)
            setattr(pos, attr, None)
        if pos.hedge_ce_leg is None and pos.hedge_pe_leg is None:
            pos.is_hedged_positional = False
        self._persist()

    async def _close_position_and_hedge(self, reason: str) -> None:
        """2026-08-28, real user-caught incident: while a position is
        is_hedged_positional, EVERY full-close exit (Day% guardrail, day-low
        reversal, scalable TSL, ITM-pair-gate's close-fallback) must treat
        all 4 legs (2 sold + 2 hedge) as ONE position -- close them together,
        not just the 2 sold legs. Before this fix, only the same-strike-
        collision guard and hedge-cumulative-profit-close routed through
        _close_hedge_legs first; every other full-close check called
        _close_position() alone, silently orphaning the hedge legs (a real
        incident: day_loss_sl fired on the sold legs' own running loss --
        which the hedge was already offsetting -- closed just the 2 sold
        legs and left the 2 hedge legs sitting exposed with nothing hedging
        them anymore).

        Rollover (single-side roll: ltp_decay, ratio_exit, exit_rules,
        vwap_rise, and ITM-pair-gate's own roll-ATTEMPT before this
        fallback) is deliberately UNCHANGED -- it continues to operate on
        the individual sold leg only, per direct user instruction ("only
        rollover happens on individual sell leg") and the pre-existing
        same-strike-collision guard's own precedent (rollover while hedged
        is expected and already handled)."""
        pos = self._position
        if pos is not None and pos.is_hedged_positional:
            await self._close_hedge_legs(pos, reason)
        await self._close_position(reason)

    def _combined_pnl_pts(self, pos: "StraddlePosition", sold_pnl_pts: float) -> float:
        """Sold-legs P&L, PLUS the hedge legs' own running P&L when this
        position is hedged -- the "treat all 4 legs as one" basis every
        full-close exit threshold (Day%, ScalableTSL, ITM-pair-gate) must
        use once hedged, per the same 2026-08-28 incident described in
        _close_position_and_hedge's own docstring. 0.0 contribution when
        not hedged (byte-identical to the old sold-legs-only behavior)."""
        if pos is not None and pos.is_hedged_positional:
            return sold_pnl_pts + pos.hedge_unrealized_pnl
        return sold_pnl_pts

    async def _hedge_or_roll_if_eligible(self, pos: "StraddlePosition", now: datetime,
                                          stop_for_day_on_hedge: bool = True) -> bool:
        """Shared hedge/roll eligibility + dispatch, factored out of
        _eod_close_or_hedge (2026-08-25) so the SAME decision can also run
        earlier via _maybe_prehedge (the pre-squareoff precheck) without
        duplicating the T-1/roll/hedge branching. Only ever called on a
        NOT-YET-hedged position (pos.is_hedged_positional is False) -- the
        already-hedged/T-1-roll branch stays in _eod_close_or_hedge, since
        that one is specific to a position that's already carrying a hedge
        from a prior day. Returns True if a hedge or roll was started
        (caller must not also close).

        2026-09-11, direct user spec: day_loss_sl now also reaches this same
        method (see the day-level guardrail below) to activate a hedge
        instead of closing on a mid-day loss, rather than only at EOD.
        stop_for_day_on_hedge=False for that call site specifically --
        direct user instruction: "stop for the day is false, it is only
        true when day profit happened; in day loss we jump to add hedge so
        day stop is false for that." No explicit block is needed anyway
        since a beginning-entry can't fire while this position (now 4 legs)
        is still open. The EOD-time callers keep the default True, unchanged."""
        if not (getattr(self, "_hedge_carry_enabled", False) and self._cumulative_hedge_pnl(pos) < 0):
            return False
        if self._is_t1_from_expiry(pos, now):
            # 2026-10-01 CRITICAL FIX, direct user correction: this used to
            # roll straight onto next week's expiry SAME DAY
            # (_start_hedge_roll) instead of hedging a contract that expires
            # tomorrow -- user clarified the real intended behavior: on T-1
            # EOD, a still-unhedged position in loss should NOT hedge (same
            # reasoning as before -- a hedge built against tomorrow's expiry
            # is pointless) AND should NOT roll/reopen same-day either. It
            # simply closes both legs, books the loss, and starts genuinely
            # fresh tomorrow via the normal entry_rules_beginning path (which
            # already has its own low-anchor-LTP-shift safety net to move to
            # next week's expiry on its own if the now-expiring-today
            # contract's premiums are too decayed to clear the entry floor --
            # no special-casing needed here for that).
            logger.info(
                "SellStraddle[%s]: T-1 EOD CLOSE (no hedge) — cumulative loss on the "
                "expiring week; not hedging/rolling a contract that expires tomorrow -- "
                "closing and starting fresh tomorrow.", self._underlying,
            )
            self._clog.info(
                "T-1 EOD CLOSE (no hedge) — cumulative loss on the expiring week; "
                "closing both legs, starting fresh tomorrow (no same-day reopen)."
            )
            await self._close_position_and_hedge("t1_no_hedge_eod_close")
            self._stop_for_day = True
            return True
        hedged = await self._try_build_hedge(pos, now)
        if hedged:
            if stop_for_day_on_hedge:
                self._stop_for_day = True
            return True
        # Hedge couldn't be built -- caller falls through to a normal close.
        return False

    _HEDGE_PRECHECK_LEAD_MIN = 1

    def _hedge_precheck_time(self, now: datetime) -> bool:
        """2026-08-25, direct user suggestion: evaluate the hedge-and-carry
        decision _HEDGE_PRECHECK_LEAD_MIN minutes BEFORE the hard EOD
        deadline (self._force_exit), not exactly at it. Building a hedge
        takes up to ~30s (two sequential order-confirm waits); running that
        decision only at the exact squareoff instant left a window where a
        separate EOD trigger could independently close the sold legs while
        the hedge was still mid-flight (real 2026-08-25 incident -- see
        _check_exits' own comment). True only in the window
        [force_exit - lead, force_exit) -- once _past_squareoff(now) is
        itself true this returns False, so it never re-fires after the
        real deadline has already passed."""
        if self._is_crypto:
            return False  # crypto force-exit/hedge semantics not in scope here
        fe_total = self._force_exit.hour * 60 + self._force_exit.minute
        now_total = now.hour * 60 + now.minute
        return (fe_total - self._HEDGE_PRECHECK_LEAD_MIN) <= now_total < fe_total

    async def _maybe_prehedge(self, pos: "StraddlePosition", now: datetime) -> None:
        """Fire the hedge decision early (see _hedge_precheck_time). If a
        hedge or roll is genuinely built here, _eod_close_or_hedge's own
        is_hedged_positional branch at the real squareoff time will see it
        already standing and simply leave it running -- the sold legs are
        never touched by the real EOD close path at all. If hedging isn't
        eligible/needed (feature off, cumulative P&L not negative, or the
        hedge build itself fails), this is a no-op and the real squareoff
        check later runs exactly as before.

        2026-09-02 CRITICAL FIX, real incident: this only checked
        is_hedged_positional -- not single-leg mode (pos.ce_leg_closed /
        pe_leg_closed). _eod_close_or_hedge (the REAL EOD decision, one
        minute later) already treats single-leg mode as always going
        straight to a surviving-leg-only close, never hedge-eligible -- but
        this earlier precheck didn't know that, so on 2026-09-01 it built a
        genuine 2-leg hedge (CE24250@43.70 + PE23750@42.45) for a position
        whose PE side had already closed via the post-15:00 R1 mechanic one
        minute earlier, leaving only CE24100 as the real sold leg. One
        minute later _eod_close_or_hedge correctly closed just the surviving
        CE leg and finalized the position -- but _close_surviving_leg_and_
        finalize has no concept of hedge legs, so the freshly-bought hedge
        pair was left completely orphaned (no P&L tracking, no exit plan).
        Manually closed by the user; see also the defensive backstop added
        to _close_surviving_leg_and_finalize itself, in case a hedge is ever
        already standing when single-leg mode kicks in (e.g. carried in from
        a prior day) rather than freshly built here."""
        if pos.is_hedged_positional:
            return
        if pos.ce_leg_closed or pos.pe_leg_closed:
            return   # single-leg mode is always a surviving-leg-only close -- never hedge-eligible
        started = await self._hedge_or_roll_if_eligible(pos, now)
        if started:
            logger.info(
                "SellStraddle[%s]: PRE-SQUAREOFF HEDGE — hedge/roll started %dmin ahead of "
                "the %s deadline; real EOD squareoff will leave it carried.",
                self._underlying, self._HEDGE_PRECHECK_LEAD_MIN, self._force_exit.strftime("%H:%M"),
            )
            self._clog.info(
                "PRE-SQUAREOFF HEDGE — started %dmin ahead of %s squareoff",
                self._HEDGE_PRECHECK_LEAD_MIN, self._force_exit.strftime("%H:%M"),
            )

    async def _eod_close_or_hedge(self, pos: "StraddlePosition", now: datetime) -> None:
        """The EOD decision, in priority order (2026-08-20, user spec; T-1
        handling corrected 2026-08-24 per direct user spec):
          1. Already hedged (carried from a prior day, or from this same
             session's own _maybe_prehedge precheck) -- if T-1 on ITS OWN
             sold legs' expiry, ROLL to next week's expiry instead of
             stopping the carry (NSE cash-settles the current week's
             contracts at expiry regardless of what this code does, so
             "carry through expiry" has to mean rolling onto fresh
             contracts, not literally holding the same ones past
             settlement). Otherwise leave it running -- the tick-by-tick
             cumulative-profit check (_check_hedge_cumulative_profit_close)
             is what closes it early.
          2. Not yet hedged, cumulative P&L (booked + running sold legs) is
             negative, feature enabled -- hedge (see _hedge_or_roll_if_eligible),
             regardless of how close to expiry (T-1, T-2, or any other day --
             user spec: this decision no longer special-cases proximity to
             expiry). If it's T-1, don't build a hedge against a contract
             expiring tomorrow -- roll straight onto next week's expiry instead.
          3. Otherwise -- normal EOD close, exactly as before this feature existed.
        """
        # 2026-08-28: a position that already closed one leg via the post-15:00
        # R1 mechanic is never a hedge-and-carry candidate -- that mechanic
        # exists specifically to flatten the day's straddle, not carry a lone
        # remaining leg forward. Straight to closing the survivor.
        if pos.ce_leg_closed or pos.pe_leg_closed:
            logger.info("SellStraddle[%s]: EOD SQUAREOFF (surviving leg only) — time=%s",
                        self._underlying, now.strftime("%H:%M"))
            await self._close_position("eod_squareoff")
            self._stop_for_day = True
            return

        if pos.is_hedged_positional:
            if self._is_t1_from_expiry(pos, now):
                # 2026-10-01 CRITICAL FIX, direct user correction: this used
                # to roll the whole 4-leg position onto next week's expiry
                # SAME DAY (_start_hedge_roll) instead of stopping the carry.
                # Real intended behavior: a hedge that hasn't yet reached the
                # ₹500/lot cumulative profit target (that case is handled
                # separately, every tick, by _check_hedge_cumulative_profit_
                # close -- including ON T-1 -- and already closes + starts
                # fresh THE SAME DAY, unaffected by this change) should, once
                # T-1 EOD genuinely arrives without having hit that target,
                # simply close out for a loss and start completely fresh
                # tomorrow -- no same-day roll/reopen. Tomorrow's normal
                # entry_rules_beginning already has its own low-anchor-LTP-
                # shift safety net to move to next week's expiry on its own
                # if today's (now-expiring) contract's premiums are too
                # decayed to clear the entry floor.
                logger.info(
                    "SellStraddle[%s]: T-1 EOD CLOSE (hedge not yet profitable) — carried "
                    "hedge's own sold legs expire tomorrow; closing out and starting fresh "
                    "tomorrow (no same-day reopen).", self._underlying,
                )
                self._clog.info(
                    "T-1 EOD CLOSE (hedge not yet profitable) — carried hedge's own sold "
                    "legs expire tomorrow; closing all 4 legs, starting fresh tomorrow."
                )
                await self._close_position_and_hedge("t1_hedge_eod_close")
                self._stop_for_day = True
            # else: leave running -- tick-by-tick profit-close handles it.
            return

        if await self._hedge_or_roll_if_eligible(pos, now):
            return

        logger.info("SellStraddle[%s]: EOD SQUAREOFF — time=%s", self._underlying, now.strftime("%H:%M"))
        await self._close_position("eod_squareoff")
        self._stop_for_day = True

    async def _start_hedge_roll(self, pos: "StraddlePosition", now: datetime, reason: str) -> None:
        """2026-08-24, user spec: roll a hedge (or hedge-candidate) position onto
        next week's expiry instead of stopping the carry at T-1. Closes whatever
        legs are currently open -- hedge legs for real (never stashed: they're on
        the SAME expiring contract as the sold legs, so carrying them onto a
        next-week sold pair would mismatch expiries between the hedge and what
        it's meant to protect), then the sold legs -- subscribes to next week's
        expiry, and marks a pending roll. _try_complete_hedge_roll (checked every
        tick from the entry loop) opens the fresh sold pair + fresh hedge once
        next week's ATM strikes have live data."""
        if pos.hedge_ce_leg is not None or pos.hedge_pe_leg is not None:
            await self._close_hedge_legs(pos, reason)
        await self._close_position(reason)

        from data_layer.instrument_registry import REGISTRY
        later_expiries = sorted(e for e in REGISTRY.all_expiries(self._underlying) if e > pos.expiry_date)
        if not later_expiries:
            logger.critical(
                "SellStraddle[%s]: HEDGE ROLL — no next expiry found in registry past %s, "
                "cannot roll. Position closed, NOT re-hedged -- needs manual review.",
                self._underlying, pos.expiry_date.isoformat() if pos.expiry_date else "?",
            )
            self._clog.critical("HEDGE ROLL — no next expiry available, closed with no roll")
            return
        next_expiry = later_expiries[0]

        self._hedge_roll_pending = True
        self._hedge_roll_reason = reason
        self._entry_expiry_date = next_expiry
        # Reuse the low-anchor-LTP feature's sticky guard so no periodic
        # _effective_entry_expiry() recompute elsewhere can overwrite this
        # back to the current (expiring) week before the roll completes.
        self._expiry_shifted_low_anchor_ltp = True
        self._strike_prem.clear()
        # 2026-09-06, direct user follow-up (stale-value audit) -- this hedge
        # roll moves to next week's expiry the exact same way
        # _shift_to_next_week_expiry (entries.py) does, but never carried
        # over that path's own 2026-08-31 CRITICAL FIX: self._pool_engine is
        # keyed by strike NUMBER alone, which repeats across weekly
        # contracts, so without a rebuild it kept blending the OLD
        # (expiring) contract's VWAP/SLOPE/RSI/ROC history into the NEW
        # contract's incoming ticks under the same key -- corrupting the
        # indicators that drive the fresh sold pair's entry AND the carried
        # position's own exit checks. Same fix, same reasoning, mirrored
        # here. _prev_atp_closed / _shadow_vwap are the other two per-
        # (strike,side) caches that feed the SAME decision chain (the
        # fallback SLOPE source and the calculative-VWAP-source pool feed,
        # respectively) and were found to have the identical gap -- cleared
        # alongside the pool engine so no cache in this decision chain
        # survives the contract change.
        from strategies.pool_indicator_engine import PoolIndicatorEngine
        _old = self._pool_engine
        self._pool_engine = PoolIndicatorEngine(
            rsi_len=_old._rsi_len, roc_len=_old._roc_len, maxlen=_old._maxlen)
        self._prev_atp_closed.clear()
        self._shadow_vwap.clear()
        self._clog.info(
            "HEDGE ROLL: pool indicator engine (VWAP/SLOPE/RSI/ROC) + _prev_atp_closed + "
            "_shadow_vwap reset fresh for the new expiry -- prevents old-contract price "
            "history from blending into new-contract ticks under the same strike numbers."
        )
        await self._subscribe_expiry_window(next_expiry)
        logger.info(
            "SellStraddle[%s]: HEDGE ROLL (%s) — rolling to next expiry %s, waiting for "
            "live ATM data before opening the fresh sold pair + hedge.",
            self._underlying, reason, next_expiry.isoformat(),
        )
        self._clog.info("HEDGE ROLL (%s) — rolling to %s, waiting for live ATM data",
                        reason, next_expiry.isoformat())

    async def _try_complete_hedge_roll(self, now: datetime) -> None:
        """Checked every tick from the entry loop while a roll is pending
        (see _start_hedge_roll). Opens the fresh sold pair the instant next
        week's ATM strikes have live LTPs, then immediately builds a fresh
        hedge against it -- both legitimately new, on the new expiry, not
        carried from the old (already-closed-for-real) ones."""
        if not self._hedge_roll_pending:
            return
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        if self._spot <= 0 or step <= 0:
            return
        # 2026-08-26: same mean-of-spot-and-futures ATM reference every other
        # entry/selection path now uses (falls back to plain self._spot otherwise).
        _atm_src = self._atm_ref if self._atm_ref > 0 else self._spot
        atm = int(round(_atm_src / step) * step)
        ce_ltp = float(self._strike_prem.get((atm, "CE"), {}).get("ltp", 0.0) or 0.0)
        pe_ltp = float(self._strike_prem.get((atm, "PE"), {}).get("ltp", 0.0) or 0.0)
        if ce_ltp <= 0 or pe_ltp <= 0:
            return   # still waiting for live ticks on next week's ATM strikes

        reason = self._hedge_roll_reason
        self._hedge_roll_pending = False
        self._hedge_roll_reason = ""
        await self._open_position(now, atm, atm, ce_ltp, pe_ltp, "entry_rules_beginning",
                                  reason, expiry_date=self._entry_expiry_date)
        if self._position is None or self._position.status != "open":
            logger.critical(
                "SellStraddle[%s]: HEDGE ROLL — fresh sold pair failed to open on %s -- "
                "roll incomplete, no position, needs manual review.",
                self._underlying, self._entry_expiry_date.isoformat() if self._entry_expiry_date else "?",
            )
            self._clog.critical("HEDGE ROLL — fresh sold pair failed to open, roll incomplete")
            return

        hedged = await self._try_build_hedge(self._position, now)
        if not hedged:
            logger.critical(
                "SellStraddle[%s]: HEDGE ROLL — fresh sold pair CE%d/PE%d opened on %s but "
                "no valid hedge strike found -- position is UNHEDGED, needs manual review.",
                self._underlying, atm, atm, self._entry_expiry_date.isoformat(),
            )
            self._clog.critical(
                "HEDGE ROLL — fresh pair CE%d/PE%d opened but hedge build failed -- UNHEDGED",
                atm, atm,
            )

    async def _check_hedge_cumulative_profit_close(self, pos: "StraddlePosition", now: datetime) -> bool:
        """2026-08-24, user spec: while a hedge is standing, check EVERY TICK
        (not just at EOD) whether the combined economics -- both sold legs'
        running P&L, both hedge legs' running P&L, plus whatever's already
        booked today -- have turned net positive. The instant they do, close
        all four legs (hedge first, then sold -- same order as the existing
        T-1-from-expiry path) and start a cooldown before the next entry
        attempt, rather than waiting for T-1-from-expiry. The next entry uses
        entry_rules_beginning (a genuine fresh start, not a same-day
        re-entry) -- already true by construction once flat again
        (_trades_today resets on the calendar-day boundary independently of
        this feature), the cooldown just delays that next attempt until
        entry_rules_beginning's own max timeframe has elapsed.

        Returns True if this fired (caller should stop evaluating any other
        exit for this position this tick -- it's closing).

        2026-08-27, direct user correction: the close trigger is NOT
        breakeven (total >= 0) -- it's a genuine ₹{_HEDGE_CLOSE_PROFIT_RS:.0f}
        of real cumulative profit (today's already-booked P&L + running P&L
        across all sold+hedge legs), same shape/spirit as the ITM-pair-gate's
        own ₹500 profit threshold elsewhere in this file. A day where the
        booked profit and the running loss happen to net to exactly ₹0 must
        NOT close the trade early -- only stop once ₹500 of real profit is
        actually sitting there.

        2026-09-11, direct user spec: this ₹500 is PER LOT, not a flat total
        -- threshold now scales by self._lot_multiplier (same pattern
        _check_scalable_tsl's own base_profit/base_lock already use for a
        per-lot rupee constant). _pnl_rs() already folds lot_size*
        lot_multiplier into total_pnl_rs itself, so a multi-lot book's real
        P&L was already being compared against a threshold that never grew
        with lot count -- a 3-lot book used to trigger at the same flat ₹500
        a 1-lot book would, undershooting the intended "₹500 of real profit
        per lot traded."
        """
        total_pnl_pts = self._cumulative_hedge_pnl(pos, include_hedge=True)
        total_pnl_rs = self._pnl_rs(total_pnl_pts)
        _threshold_rs = _HEDGE_CLOSE_PROFIT_RS * self._lot_multiplier
        if total_pnl_rs < _threshold_rs:
            return False
        if pos.status != "open":
            # 2026-09-17 CRITICAL FIX, real live incident: _tick_loop and
            # _eod_backstop_loop are two INDEPENDENT asyncio tasks that can
            # both reach _check_exits() for the same position around the
            # same moment (see _eod_backstop_loop's own docstring -- this is
            # already a documented, accepted risk for _close_position, which
            # guards itself by setting pos.status="closing" SYNCHRONOUSLY
            # before its first await). This method had NO equivalent guard:
            # both tasks could independently pass the threshold check above
            # and both call _close_hedge_legs before either one set any
            # status flag. Confirmed live: PE22950's real SELL order was
            # dispatched TWICE (two identical rows in the broker's own order
            # book, both REJECTED only because this account has zero margin
            # -- had margin existed, this would have doubled the real
            # hedge-close size). Re-checking pos.status here (now genuinely
            # racy-safe since the caller always re-reads self._position
            # fresh) catches the second task before it repeats the dispatch.
            return False
        pos.status = "closing"
        logger.info(
            "SellStraddle[%s]: HEDGE CUMULATIVE PROFIT — total=₹%.2f (%.2f pts, threshold=₹%.2f) "
            "(booked=%.2f sold=%.2f hedge=%.2f pts) — closing all 4 legs, starting fresh.",
            self._underlying, total_pnl_rs, total_pnl_pts, _threshold_rs, self._session_realized_pnl_pts,
            pos.unrealized_pnl, pos.hedge_unrealized_pnl,
        )
        self._clog.info(
            "HEDGE CUMULATIVE PROFIT total=₹%.2f (%.2f pts, booked=%.2f sold=%.2f hedge=%.2f pts) — "
            "closing all 4 legs", total_pnl_rs, total_pnl_pts, self._session_realized_pnl_pts,
            pos.unrealized_pnl, pos.hedge_unrealized_pnl,
        )
        await self._close_hedge_legs(pos, "hedge_cumulative_profit")
        await self._close_position("hedge_cumulative_profit")
        # 2026-09-17, direct user spec, OVERRIDES the 2026-08-24 original
        # design (which deliberately cooled down before the next entry --
        # see this method's own opening docstring): "when we close complete
        # 4 leg no cooldown needed immediate start to check for beginning
        # entry logic." A hedge-cumulative-profit close is a genuine, clean,
        # profit-driven exit (not an SL/loss event) -- the next entry should
        # be evaluated as BEGINNING starting the very next tick, no delay.
        # _close_position's own generic cooldown call is also skipped for
        # this reason (see its exclusion-list comment).
        return True

    _POST1500_START = dtime(15, 0)
    # 2026-09-26, direct user spec: was 15:15 -- moved to 15:00 (same as
    # _POST1500_START) so the profit-only arm path is live from the very
    # start of this mechanic's window, not 15 minutes into it. Combined with
    # the day-low branch below also now requiring profit, both arm paths are
    # active across the same [15:00, force_exit) window and differ only in
    # WHICH condition (day-low retest vs. plain profit) actually triggers.
    _POST1500_PROFIT_CHECK = dtime(15, 0)

    async def _check_post1500_r1_exit(self, pos: "StraddlePosition", now: datetime) -> None:
        """Post-15:00 per-leg R1 exit (2026-08-28, direct user spec).

        Replaces the ACTION of day_low_exit_enabled (close both legs) for any
        binding that opts into THIS instead -- day_low_exit_enabled's own
        close action is untouched for anyone not opting in (see the guard
        split in the day-low block right above this call site).

        Confirmed state machine, direct user sign-off across several rounds
        of clarification:
          1. From 15:00 onward, each leg's own 1-min R1 is tracked via a
             FRESH SupportResistanceCalculator per leg (mirrors the OI-ORB
             3-min-TSL bar-accumulator pattern) -- watching alone never
             closes anything.
          2. ARM (start actually acting on an R1 breach) the moment EITHER:
             (a) the position's combined value reaches the frozen day-low
                 (self._session_min_straddle_frozen -- the SAME one-time
                 REST value day_low_exit_enabled's own block computes; read
                 here, never written by this feature), from 15:00 onward,
                 AND the overall day P&L (booked + running, hedge-inclusive
                 via _combined_pnl_pts) is positive, OR
             (b) from 15:00 onward (2026-09-26: was 15:15, moved to align
                 with (a)'s own start) the overall day P&L alone is positive.
             2026-09-26 CRITICAL FIX, direct user spec: BOTH paths now
             require day P&L > 0 -- the day-low branch used to arm on the
             price retest alone, with no profit check, meaning a day still
             in overall LOSS could arm this mechanic and go on to fire a
             real one-side R1-breach close. That put the position into
             single-leg mode, and single-leg mode is never hedge-eligible at
             EOD (_maybe_prehedge/_eod_close_or_hedge both refuse to hedge a
             position with either leg already closed) -- so a losing day
             that happened to retest its own day-low could end up closing
             one side early for no protective reason and riding the other
             side to EOD with no hedge, exactly the outcome this fix
             prevents. If the day is still in overall loss, NOTHING arms
             here regardless of day-low -- the existing EOD hedge-and-carry
             mechanic (unchanged, not touched by this feature) is what takes
             over as force_exit approaches. The arm check keeps re-running
             every tick, so a loss that later flips to profit arms
             immediately at that point.
          3. Once armed, each leg is watched INDEPENDENTLY: the instant a
             leg's own live LTP closes above its own R1.high, that ONE leg
             closes via _close_leg -- the other, not-yet-breached leg keeps
             running solo, tracked by dataclasses.StraddlePosition's
             ce_leg_closed/pe_leg_closed flags (current_value/unrealized_pnl
             already exclude a closed leg so downstream sums can't double
             its already-booked P&L).
          4. A surviving single leg has no further R1-independent exit here
             -- EOD square-off (already checked unconditionally earlier in
             _check_exits, before this function is ever reached) is its only
             remaining backstop, exactly as the user confirmed ("R1 logic
             will survive and EOD").
        """
        # 2026-09-06, direct user follow-up (stale-value audit F6): keyed by
        # strike NUMBER alone, this "has the pair changed" check couldn't
        # tell an expiry roll apart from "still the same pair" whenever the
        # roll happened to land on identical strike numbers (e.g. a hedge
        # roll to next week keeping the same ATM strikes) -- reusing the OLD
        # contract's R1 calculators/armed-state for the NEW contract's ticks
        # after 15:00. expiry_date makes a same-strike-different-contract
        # roll register as a genuine pair change, same fix shape as F1/F2/F3.
        _pair_id = (int(pos.ce_leg.strike), int(pos.pe_leg.strike), pos.expiry_date)
        _pair_changed = (self._post1500_pair != _pair_id)
        # 2026-09-06, direct user follow-up (stale-value audit F11): a restart
        # after 15:00 now restores _post1500_pair/_post1500_armed from disk
        # (see _restore_session) so the arm decision survives -- but the
        # SupportResistanceCalculator instances themselves are deliberately
        # NOT persisted (they safely re-warm from live bars within a few
        # minutes). If the restored pair matches the live position exactly,
        # the OLD version of this check would skip creating _post1500_calc
        # entirely (pair "unchanged") and the very next bar-close would
        # KeyError on an empty dict. _calc_missing catches that restart case
        # specifically -- calculators get created fresh either way, but the
        # restored armed/armed_reason/leg_closed/closing flags are only
        # wiped on a GENUINE pair change, never on this restart re-init.
        _calc_missing = not self._post1500_calc
        if _pair_changed or _calc_missing:
            self._post1500_pair = _pair_id
            self._post1500_calc = {"CE": SupportResistanceCalculator(), "PE": SupportResistanceCalculator()}
            self._post1500_bar_acc = {}
            self._post1500_bar_closed_at = {}
            if _pair_changed:
                self._post1500_armed = False
                self._post1500_armed_reason = None
                self._post1500_leg_closed = {"CE": False, "PE": False}
                self._post1500_closing = {"CE": False, "PE": False}
            self._persist_session()

        if now.time() < self._POST1500_START:
            return

        # Feed 1-min bars per still-open leg into that leg's own ladder.
        for side in ("CE", "PE"):
            leg_closed_attr = f"{side.lower()}_leg_closed"
            if self._post1500_leg_closed.get(side) or getattr(pos, leg_closed_attr):
                continue
            leg = pos.ce_leg if side == "CE" else pos.pe_leg
            ltp = float(leg.ltp or 0.0)
            if ltp <= 0:
                continue
            minute = now.replace(second=0, microsecond=0)
            acc = self._post1500_bar_acc.get(side)
            if acc is None:
                self._post1500_bar_acc[side] = {"minute": minute, "h": ltp, "l": ltp, "c": ltp}
            elif minute != acc["minute"]:
                # 2026-09-29 CRITICAL FIX, direct user correction, real incident:
                # a genuine intrabar breach at 15:04:47 (ltp=136.30 vs established
                # R1=136.20) fired even though that same 1-min bar went on to
                # CLOSE at 135.30 -- BELOW the established R1 -- a wick-and-reject,
                # not a real breakout. Save this just-closed bar's own close price
                # (the last live tick seen before the minute rolled over) BEFORE
                # updating the calculator with its own high/low, so the breach
                # check below can require the bar to have genuinely CLOSED above
                # R1, not merely wicked above it intrabar. Near EOD (15:00-15:35),
                # the ~1 extra minute of latency this adds is an acceptable cost
                # for filtering out a false wick-based signal.
                # Capture R1's state as it stood BEFORE this same bar updates it --
                # process_straddle_candle below can itself flip is_established/phase
                # using this very bar's own high/low, so reading R1 AFTER that call
                # would compare the bar's close against a value the bar itself just
                # changed (e.g. a breach candle can flip phase back to R1_TRACKING
                # and clear is_established in the same call, silently masking a
                # real breach instead of confirming it).
                _prior_state = self._post1500_calc[side].get_calculated_sr_state(
                    f"{self._underlying}_{side}_P1500")
                _prior_r1 = _prior_state.get("sr_levels", {}).get("R1")
                if (_prior_r1 and _prior_r1.get("is_established")
                        and _prior_state.get("current_phase") != "R1_TRACKING"):
                    self._post1500_bar_closed_at[side] = (float(acc["c"]), float(_prior_r1["high"]))
                self._post1500_calc[side].process_straddle_candle(
                    f"{self._underlying}_{side}_P1500",
                    {"timestamp": acc["minute"], "high": acc["h"], "low": acc["l"], "duration": 1},
                )
                self._post1500_bar_acc[side] = {"minute": minute, "h": ltp, "l": ltp, "c": ltp}
                # 2026-09-03 diagnostic (direct user request): prove/disprove
                # whether R1 genuinely recomputes every closed 1-min bar, since
                # this is only observable per-bar here, never dumped elsewhere.
                _r1_now = self._post1500_calc[side].get_calculated_sr_state(
                    f"{self._underlying}_{side}_P1500").get("sr_levels", {}).get("R1")
                self._clog.info(
                    "POST-15:00 R1 BAR CLOSE %s — bar %s h=%.2f l=%.2f c=%.2f -> R1.high=%s established=%s",
                    side, acc["minute"].strftime("%H:%M"), acc["h"], acc["l"], acc["c"],
                    f"{_r1_now['high']:.2f}" if _r1_now else "None",
                    _r1_now.get("is_established") if _r1_now else "-",
                )
            else:
                acc["h"] = max(acc["h"], ltp)
                acc["l"] = min(acc["l"], ltp)
                acc["c"] = ltp

        if not self._post1500_armed:
            # 2026-09-26 CRITICAL FIX, direct user spec: the day-low branch
            # used to arm on the price retest ALONE, with no P&L check --
            # meaning a day still in overall loss could arm this mechanic
            # (and go on to fire a real one-side R1-breach close, which then
            # makes the position single-leg -- and single-leg mode is never
            # hedge-eligible at EOD, see _maybe_prehedge/_eod_close_or_hedge).
            # Direct user correction: "day low will be checked after 15:00
            # and if overall pnl is in profit then only R1 for single side
            # will be active for exit ... if day low is there but overall
            # p&l is in loss it will not fire r1 breach." Both arm paths now
            # require the SAME day P&L > 0 condition, computed once.
            _day_pnl = self._session_realized_pnl_pts + self._combined_pnl_pts(pos, pos.unrealized_pnl)
            _frozen = self._session_min_straddle_frozen
            if _frozen is not None and pos.current_value <= _frozen and _day_pnl > 0:
                self._post1500_armed = True
                self._post1500_armed_reason = "day_low"
            elif now.time() >= self._POST1500_PROFIT_CHECK and _day_pnl > 0:
                self._post1500_armed = True
                self._post1500_armed_reason = "profit"
            if self._post1500_armed:
                self._clog.info(
                    "POST-15:00 R1 EXIT ARMED (%s) — now watching each open leg's own R1 "
                    "independently; a breach on either side closes that leg alone.",
                    self._post1500_armed_reason,
                )
                self._persist_session()

        if not self._post1500_armed:
            return

        for side in ("CE", "PE"):
            leg_closed_attr = f"{side.lower()}_leg_closed"
            if getattr(pos, leg_closed_attr):
                continue
            # 2026-09-03 CRITICAL FIX: a real duplicate-close incident (two
            # broker orders for the same CE leg, 138ms apart, same R1.high,
            # same ltp) traced to this exact gap -- ce_leg_closed/pe_leg_closed
            # only flip True AFTER _close_leg's await returns (order placement
            # + broker confirmation, observed >1s), so a second exit-check tick
            # landing during that window saw the leg as still open and fired a
            # second real close. This flag is set True BEFORE the await, so a
            # concurrent re-entry sees the leg is already being closed and
            # skips; cleared only on an aborted close, so a genuine retry after
            # a broker-confirm timeout is still possible.
            if self._post1500_closing.get(side):
                continue
            leg = pos.ce_leg if side == "CE" else pos.pe_leg
            # 2026-09-29 CRITICAL FIX, direct user correction, real incident
            # (2026-09-29 15:04:47): the old check compared a live tick against
            # WHATEVER R1 currently holds -- but a genuine breach candle can
            # itself flip R1's phase/is_established in the SAME bar-close call
            # that produced it (see the bar-accumulation loop above), so
            # re-reading R1 here, AFTER that update, could either mask a real
            # breach (phase already flipped back to R1_TRACKING) or -- the
            # actual live incident -- fire on a mere intrabar WICK that the
            # same bar went on to close back BELOW R1 (a wick-and-reject, not a
            # real breakout). Fixed by requiring the most recently CLOSED bar
            # to have genuinely closed above the R1 value that was established
            # BEFORE that bar closed -- both captured together, atomically, in
            # the bar-accumulation loop's own _post1500_bar_closed_at entry, so
            # there's no window for either value to have since been mutated by
            # the same or a later bar. Cleared via pop() so a stale close can't
            # re-trigger a breach on a later tick with no new bar having closed
            # since. Near EOD, the ~1 extra minute this adds (waiting for the
            # bar to close, rather than reacting to a live tick) is an
            # acceptable cost for filtering out a false intrabar wick.
            _closed = self._post1500_bar_closed_at.pop(side, None)
            if _closed is None:
                continue
            _bar_close, _r1_high_at_close = _closed
            if _bar_close <= _r1_high_at_close:
                continue
            ltp = float(leg.ltp or 0.0)
            if not self._defer_exit(f"post1500_r1_breach_{side}", now):
                continue
            self._post1500_closing[side] = True
            order_ev = await self._close_leg(side, "post1500_r1_breach", now)
            if getattr(order_ev, "close_aborted", False):
                self._post1500_closing[side] = False
                continue
            setattr(pos, leg_closed_attr, True)
            self._post1500_leg_closed[side] = True
            self._clog.info(
                "POST-15:00 R1 BREACH — %s leg (strike=%.0f) closed independently "
                "(R1.high=%.2f, bar_close=%.2f, ltp=%.2f); the other leg keeps running solo.",
                side, leg.strike, _r1_high_at_close, _bar_close, ltp,
            )
            self._persist()
            if pos.ce_leg_closed and pos.pe_leg_closed:
                # Both sides have now closed independently -- finalize the whole
                # position exactly like a normal full close. Each leg's own P&L was
                # already booked into self._session_realized_pnl_pts by _close_leg
                # above (twice, once per side) -- do NOT add pos.realized_pnl again.
                pos.status = "closed"
                pos.close_reason = "post1500_r1_breach"
                pos.close_time = now
                self._unpin_position_legs(pos)
                self._position = None
                await self._unsubscribe_entry_expiry_tokens()
                self._apply_sl_cooldown()
                self._persist()
                self._clog.info(
                    "POST-15:00 R1 EXIT — both legs now closed independently; "
                    "position finalized, stopping for the day."
                )
                self._stop_for_day = True

    async def _check_exits(self) -> None:
        # 2026-10-05, direct user spec ("every part to run"): VP/OI naked-leg
        # management (9-EMA stop + Re-entry Rule) must keep running even
        # with NO open StraddlePosition -- the Volatile regime's own exit
        # closes the whole position (self._position -> None) while leaving
        # naked longs bought against it. This call is BEFORE the `if not
        # pos: return` guard below specifically so it isn't skipped in that
        # state; it's a cheap no-op whenever vp_oi isn't enabled or nothing
        # is naked (see its own early-return).
        await self._check_vp_oi_naked_legs_standalone(datetime.now(IST))
        pos = self._position
        if not pos:
            return
        if pos.status != "open":
            # "closing" -- a close is already dispatched and genuinely in flight (real
            # order_id at the broker, per the 2026-08-06 confirm-model redesign). This is
            # the single source of truth that stops a duplicate close from ever being
            # dispatched; no other exit check may run until it resolves back to "open"
            # (retry) or "closed" (done).
            return
        now = datetime.now(IST)
        pnl = pos.unrealized_pnl

        import time as _t_ev
        if _t_ev.monotonic() - getattr(self, "_last_eval_cache_t", 0.0) >= 3.0:
            self._last_eval_cache_t = _t_ev.monotonic()
            _credit_ev = self._initial_net_credit or pos.net_credit or 0.0
            _crit_ev, _dump_ev = self._build_exit_criteria(pos, pnl, _credit_ev)
            _max_tf_ev = max((int(r.get("tf", 1)) for r in (self._exit_rules or [])), default=1)
            self._last_exit_eval = {
                "criteria": [{"name": n, "detail": d, "hit": bool(h)} for n, d, h in _crit_ev],
                "ind_by_tf": {str(tf): {k: round(v, 2) for k, v in (idict or {}).items()}
                              for tf, idict in (_dump_ev or {}).items()},
                "max_tf": _max_tf_ev,
                "ts": now.timestamp(),
            }

        await self._publish_exit_audit(pos, pnl, now)

        # ── Per-tick DEBUG trace (only emitted when log level is DEBUG) ──────
        import time as _t
        if logger.isEnabledFor(logging.DEBUG):
            _dbg_credit = self._initial_net_credit or pos.net_credit or 0.0
            _dbg_day_pct = (
                (self._session_realized_pnl_pts + pnl) / _dbg_credit * 100.0
                if _dbg_credit > 0 else 0.0
            )
            _dbg_ce = float(getattr(pos.ce_leg, "ltp", 0) or 0)
            _dbg_pe = float(getattr(pos.pe_leg, "ltp", 0) or 0)
            _dbg_ratio = (
                max(_dbg_ce, _dbg_pe) / min(_dbg_ce, _dbg_pe)
                if _dbg_ce > 0 and _dbg_pe > 0 else 0.0
            )
            _dbg_tsl_pnl = pnl
            if self._tsl_basis == "theta":
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                if _etv > 0:
                    _dbg_tsl_pnl = _etv - pos.current_time_value(self._spot)
            _dbg_tsl_rs = self._pnl_rs(_dbg_tsl_pnl)
            _dbg_tsl_lock = getattr(pos, "tsl_high_lock_rs", 0.0)
            logger.debug(
                "TICK-EXIT[%s] pnl=%.2f day%%=%.1f(T%.0f/SL%.0f) "
                "CE=%.2f PE=%.2f ratio=%.2fx "
                "tsl_rs=%.2f lock=%.2f spot=%.2f",
                self._underlying, pnl, _dbg_day_pct,
                self._day_profit_target_pct, self._day_loss_sl_pct,
                _dbg_ce, _dbg_pe, _dbg_ratio,
                _dbg_tsl_rs, _dbg_tsl_lock,
                getattr(self, "_spot", 0.0),
            )

        if _t.monotonic() - getattr(self, "_last_exit_log", 0.0) > 60.0:
            self._last_exit_log = _t.monotonic()
            _prot_map = getattr(self, "_itm_roll_protection", None) or {}
            _active = "".join([
                " Decay" if self._ltp_decay_enabled else "",
                " Ratio" if getattr(self, "_ratio_threshold", 0.0) > 0 else "",
                " TSL" if self._tsl_enabled else "",
                " VWAPrise" if self._vwap_rise_enabled else "",
                " exit_rules" if getattr(self, "_exit_rules", None) else "",
                " ITMgate" if getattr(self, "_itm_pair_gate_enabled", False) else "",
                # 2026-08-27, direct user request: visible every cycle while armed,
                # not just when the stop actually fires -- shows WHICH side(s).
                f" ITMrollProt({'/'.join(sorted(_prot_map.keys()))})" if _prot_map else "",
            ]) or " (none)"
            logger.info(
                "SellStraddle[%s]: EXIT-CHECK pnl=%.2f pts | Day%% T:%.0f%%/SL:%.0f%% (credit=%.2f) | "
                "EOD@%s | active exits:%s",
                self._underlying, pnl, self._day_profit_target_pct, self._day_loss_sl_pct,
                self._initial_net_credit, self._force_exit.strftime("%H:%M"), _active,
            )

        # 1. EOD FORCE SQUARE-OFF (2026-08-20: hedge-and-carry + T-1-from-expiry, user spec)
        #
        # Reentrancy guard (2026-08-25, real incident -- both sold legs closed via
        # a SEPARATE candle-close EOD path while _try_build_hedge was still mid-flight
        # from THIS path, orphaning the hedge leg with nothing left to protect).
        # _tick_loop and _eod_backstop_loop are independent asyncio tasks that can
        # both reach this point for the same still-"open" position -- pos.status
        # only flips to "closing" once an actual close is dispatched, never while a
        # hedge is merely being built (_try_build_hedge/_start_hedge_roll never touch
        # pos.status), so without this flag a second task could start a duplicate
        # hedge/close attempt on the same position while the first is still awaiting
        # order confirmations (up to ~30s). Set synchronously, no await before it, so
        # there's no window between the check and the set. The candle-close loop's
        # own former direct-close copy of this check has been removed entirely --
        # this is now the ONLY place that decides EOD hedge-or-close.
        if getattr(self, "_eod_decision_in_progress", False):
            return

        if (not self._prehedge_attempted_today and not self._past_squareoff(now)
                and self._hedge_precheck_time(now)):
            self._prehedge_attempted_today = True
            self._eod_decision_in_progress = True
            try:
                if self._position and self._position.status == "open":
                    await self._maybe_prehedge(self._position, now)
            finally:
                self._eod_decision_in_progress = False
            return

        if self._past_squareoff(now):
            self._eod_decision_in_progress = True
            try:
                if self._position and self._position.status == "open":
                    await self._eod_close_or_hedge(self._position, now)
            finally:
                self._eod_decision_in_progress = False
            return

        # A single-side roll is in flight (close fill awaited / open fill pending).
        # Skip further exit checks so we don't close the new or remaining leg twice.
        if getattr(self, "_roll_in_progress", False):
            return

        # POST-RESTORE WARM-UP GUARD
        # 2026-08-05 incident: the old 20s fallback fired while CE's leg still hadn't
        # received a single live tick since restart (took 82s in the observed case) --
        # every exit check (Day%/ITMgate/etc.) then ran against CE's stale, frozen
        # restored/entry price for ~60s before a real tick corrected it. The eventual
        # ITM-pair-gate close that day used numbers that turned out correct once the
        # real tick landed, but that was luck, not a guarantee -- a threshold could just
        # as easily have been crossed USING the stale price.
        # Fixed: ceiling raised to 5 minutes (user-specified — long enough that a genuine
        # feed problem, not just slow warm-up, is the real explanation by then) AND the
        # old "arm exits anyway using stale data" fallback is gone entirely — if a fresh
        # tick for both legs still hasn't arrived after 5 minutes, that's treated as the
        # feed being stuck, not just slow, so the position is closed instead of being
        # traded blind on a frozen/unknown leg price.
        if self._post_restore_warmup:
            _both_fresh = self._ce_ltp_fresh and self._pe_ltp_fresh
            # 2026-09-23, direct user spec ("when we start in middle of the
            # day... all indicators should be warmed up"): "fresh LTP has
            # arrived" alone doesn't mean every indicator is actually ready --
            # confirmed live, a calculative vwap_source binding's own VWAP
            # accumulator (self._shadow_vwap) has NO persistence at all and
            # is rebuilt by a separate async REST fetch (see engine.py's
            # _eng_atp computation, fixed 2026-09-23 to fall back safely to
            # broker ATP mid-seed). That per-tick fallback already prevents a
            # bad VALUE from being trusted; this is the belt-and-suspenders
            # layer on the SEQUENCING side the user asked for -- exits stay
            # held a little longer after a restart specifically for a
            # calculative binding, until its own VWAP source has genuinely
            # finished warming up, not just until a price has ticked.
            if _both_fresh and self._vwap_source == "calculative":
                _ce_key = (int(pos.ce_leg.strike), "CE")
                _pe_key = (int(pos.pe_leg.strike), "PE")
                _both_fresh = (_ce_key in self._shadow_vwap_rest_seeded
                                and _pe_key in self._shadow_vwap_rest_seeded)
            # 2026-09-29 CRITICAL FIX, real live incident (2026-09-29 open):
            # the clock used to start at _post_restore_at, set the instant the
            # position was restored from disk -- i.e. at PROCESS RESTART, which
            # the daily deploy script runs well before market open (observed:
            # 08:56:22). The 2026-09-17 fix below correctly stopped the guard
            # from ATTEMPTING a real close while genuinely pre-market (avoiding
            # an AMO-rejection storm), but never paused the underlying clock --
            # so by the time the market actually opened at 09:15 and this
            # branch could finally act, _elapsed was already ~19 minutes, 4x
            # past the 5-min timeout, and the safety-close fired on the very
            # FIRST post-open tick (09:15:00.108, confirmed live) regardless of
            # whether a fresh tick might have arrived moments later. This
            # closed a genuinely-intended-to-carry EOD hedge position every
            # single morning, defeating the whole point of carrying it.
            # Fixed: the 5-minute clock now only starts once the market is
            # genuinely open -- a pre-market restart gets a full, real 5
            # minutes after 09:15 to see a fresh tick, same as an intraday
            # restart already got (market already open -> clock starts
            # immediately, unchanged behavior for that case).
            if self._post_restore_warmup_clock_start is None and self._market_genuinely_open():
                self._post_restore_warmup_clock_start = _t.monotonic()
            _elapsed = (
                _t.monotonic() - self._post_restore_warmup_clock_start
                if self._post_restore_warmup_clock_start is not None else 0.0
            )
            if _both_fresh:
                self._post_restore_warmup = False
                logger.info("SellStraddle[%s]: post-restore warm-up complete — exits armed "
                            "(CE_ltp=%.2f PE_ltp=%.2f pnl=%.2f pts).",
                            self._underlying, pos.ce_leg.ltp, pos.pe_leg.ltp, pos.unrealized_pnl)
            elif _elapsed > self._POST_RESTORE_WARMUP_MAX_SEC:
                # 2026-08-06 fix: do NOT clear _post_restore_warmup until the safety
                # close actually confirms. The old order (clear the flag, then
                # attempt the close) meant that if _close_position itself timed out
                # or got exit_aborted (broker unavailable -- plausible under the
                # same conditions causing a stuck feed), the position stayed open
                # but the guard was already disarmed -- the very next tick would
                # fall straight through to normal Day%/ITMgate/etc. checks using
                # the still-stale/frozen leg price, exactly what this guard exists
                # to prevent. Now the flag only clears on a CONFIRMED close, so a
                # failed attempt correctly retries the safety close next cycle
                # instead of silently trading blind.
                logger.critical(
                    "SellStraddle[%s]: post-restore warm-up TIMED OUT after %.0fs with "
                    "CE_fresh=%s PE_fresh=%s — no fresh tick for %s%s within %.0fs of "
                    "restart, treating this as a stuck data feed. Closing the restored "
                    "position rather than trading on an unknown/frozen leg price "
                    "(CE_ltp=%.2f PE_ltp=%.2f pnl=%.2f pts).",
                    self._underlying, _elapsed, self._ce_ltp_fresh, self._pe_ltp_fresh,
                    "CE" if not self._ce_ltp_fresh else "",
                    "+PE" if not self._pe_ltp_fresh else "",
                    self._POST_RESTORE_WARMUP_MAX_SEC,
                    pos.ce_leg.ltp, pos.pe_leg.ltp, pos.unrealized_pnl,
                )
                # 2026-09-17, direct user spec, OVERRIDES the 2026-08-28
                # "treat all 4 legs as one" fix for THIS specific reason only:
                # "when we bring back the positions dont close hedge leg --
                # hedge leg will get closed in only 1 case when there is
                # cumulative profit of already booked plus running 4 leg
                # >500 then only hedge leg will get closed." A stuck/stale
                # feed on restore is a data-quality problem, not a trading
                # decision -- it must never be allowed to close a standing
                # hedge; only _check_hedge_cumulative_profit_close (the real
                # >=Rs500/lot combined-P&L rule) may do that. Close ONLY the
                # sold legs here -- any standing hedge keeps running
                # untouched, protected regardless of this guard's outcome.
                await self._close_position("post_restore_data_stale")
                if not (self._position and self._position.status == "open"):
                    self._post_restore_warmup = False
                else:
                    logger.critical(
                        "SellStraddle[%s]: post-restore safety close NOT confirmed -- "
                        "guard stays ARMED, will retry closing next cycle rather than "
                        "falling through to normal exit checks on stale data.",
                        self._underlying,
                    )
                return
            else:
                return

        # 2026-08-31 CRITICAL FIX (real incident, live NIFTY): once EITHER leg
        # has closed independently via the post-15:00 R1 mechanic, EVERY check
        # below this point (hedge/day%/ITM-gate/day-low/ratio/ltp-decay/TSL/
        # exit_rules/vwap_rise) must be skipped -- direct user confirmation:
        # "will not consider these exits when 1 leg is open, only R1 logic
        # will survive and EOD." This guard used to live further down, AFTER
        # the day-low block -- which meant day-low itself was NOT protected:
        # the instant a leg closed, pos.current_value correctly dropped to
        # just the surviving leg's own (much smaller) value, which is almost
        # always below a frozen threshold that was computed for BOTH legs
        # combined -- so day_low_exit_enabled's own "close both" action fired
        # on the very next tick and closed the surviving leg too, using a
        # threshold that was never meant to apply to a single leg. Confirmed
        # live: PE closed via post1500_r1_breach at 15:15:28.096, and
        # DAY-LOW REVERSAL EXIT closed the surviving CE leg five milliseconds
        # later using the two-leg frozen value (198.65) against the now-
        # single-leg current_value (104.40). Moved to the TOP of the ladder
        # (right after EOD + the roll-in-progress/post-restore guards, which
        # are data-validity/concurrency guards that must still apply
        # regardless of leg state) so it protects EVERY check, not just the
        # ones that happened to be coded after the old location. Still calls
        # _check_post1500_r1_exit for the surviving leg's own R1 watch --
        # EOD square-off (already checked above, unconditionally, before this
        # point) remains its only other backstop.
        #
        # 2026-09-24 CRITICAL FIX, real live incident (Gurmeet's NIFTY book):
        # this guard was missing its own time check. _check_post1500_r1_exit
        # already correctly no-ops before self._POST1500_START (15:00) --
        # `if now.time() < self._POST1500_START: return` is its very first
        # real line -- but this CALLER's `return` right after the await fires
        # regardless of what that function actually did. The 2026-08-31 fix
        # above implicitly assumed a leg could only ever become
        # ce_leg_closed/pe_leg_closed independently (position still "open",
        # one leg gone) via the post-15:00 R1 mechanic itself -- true at the
        # time it was written. That assumption broke the moment
        # r1_breach_reentry.py (2026-09-22/23) added a SECOND, EARLIER way to
        # reach that exact state: a rolled-in leg's R1-breach close, which can
        # fire at any time of day. Confirmed live: CE23150 closed via
        # r1_breach_post_roll at 10:55am, and every check below this point --
        # including _check_r1_breach_and_reentry() itself, section 2c further
        # down, which is what's supposed to scan for and track a new S1-breach
        # re-entry candidate on the now-empty CE side -- never ran again for
        # the rest of the session, because this guard's own `return` fired on
        # every single tick from 10:55am onward, ~4 hours before 15:00. The
        # dashboard's S1-candidate panel going silent (frozen exactly at its
        # last update, 10:55:00) was the visible symptom; the real bug was
        # this mechanic being completely starved, not a display issue. Fix:
        # only take this early-return branch once genuinely at/after
        # self._POST1500_START, matching the function's own internal gate
        # exactly -- before that, a closed leg correctly falls through to the
        # rest of the ladder below (ITM-gate/day%/R1-breach-reentry), same as
        # it would if this guard didn't exist at all. The day-low protection
        # this guard also provides remains intact for its real post-15:00
        # window, since day_low_exit_enabled's own freeze (self.
        # _session_min_straddle_frozen) can never have happened yet this
        # early anyway -- freeze times are always configured at/after 15:00
        # in every real deployment (Gurmeet's own is 15:25).
        if (self._post1500_exit_enabled and (pos.ce_leg_closed or pos.pe_leg_closed)
                and now.time() >= self._POST1500_START):
            await self._check_post1500_r1_exit(pos, now)
            return

        # 1b. HEDGE CUMULATIVE PROFIT CLOSE (2026-08-24, user spec): while a hedge
        # is standing, checked every tick (not just EOD) -- see
        # _check_hedge_cumulative_profit_close's own docstring for the full
        # mechanic. Placed ahead of every other exit type since a hedged
        # position is in an entirely different risk regime (carry, not
        # intraday roll/TSL/ratio management) -- if this doesn't fire, the
        # existing stash-and-carry behavior for the sold legs' own normal
        # exits (ratio_exit, ltp_decay, TSL, etc.) is unchanged below.
        if pos.is_hedged_positional:
            if await self._check_hedge_cumulative_profit_close(pos, now):
                return

        # 1c. HEDGE SAME-STRIKE COLLISION GUARD (2026-08-20, user spec): a standing
        # hedge leg from a prior EOD hedge-and-carry can end up sharing the exact
        # same strike as a FRESH sold leg taken after a later rollover/re-entry --
        # the market moved enough that the normal pair-selection logic, run
        # independently of the hedge, happened to land back on it. A sold leg and
        # its own hedge leg at the identical strike+expiry net to ~zero real
        # exposure on that side -- not worth continuing to carry. Close everything
        # (sold + hedge) and let the next eligible entry cycle start genuinely
        # fresh, no hedge attached.
        if pos.is_hedged_positional and (
            (pos.hedge_ce_leg is not None and int(pos.hedge_ce_leg.strike) == int(pos.ce_leg.strike))
            or (pos.hedge_pe_leg is not None and int(pos.hedge_pe_leg.strike) == int(pos.pe_leg.strike))
        ):
            logger.info(
                "SellStraddle[%s]: HEDGE SAME-STRIKE COLLISION — sold CE%d/PE%d now matches a "
                "standing hedge leg -- closing everything and starting fresh.",
                self._underlying, int(pos.ce_leg.strike), int(pos.pe_leg.strike),
            )
            self._clog.info(
                "HEDGE SAME-STRIKE COLLISION — sold CE%d/PE%d matches standing hedge -- "
                "closing everything, starting fresh",
                int(pos.ce_leg.strike), int(pos.pe_leg.strike),
            )
            await self._close_hedge_legs(pos, "hedge_strike_collision")
            await self._close_position("hedge_strike_collision")
            return

        # 2. DAY-LEVEL % GUARDRAILS
        # 2026-08-28 fix: total_day_pts now folds in the hedge legs' own
        # running P&L when hedged (_combined_pnl_pts) -- see
        # _close_position_and_hedge's own docstring for the real incident
        # this fixes (day_loss_sl fired on the sold legs' own loss, which
        # the hedge was already offsetting, and orphaned the hedge legs).
        #
        # 2026-09-13, direct user spec: while a hedge is standing, this whole
        # block (BOTH day_profit_target and day_loss_sl) is skipped entirely
        # -- a hedged position is a positional carry with its OWN dedicated
        # full-close trigger (_check_hedge_cumulative_profit_close, section
        # 1b above: booked+sold+hedge combined >= Rs500/lot); day_profit_target
        # doesn't apply (there's no "day" profit concept once carrying past a
        # loss event) and day_loss_sl already did its job triggering the
        # hedge in the first place -- re-checking it here would just find the
        # hedge already active and be a no-op at best, or (before this fix)
        # risk re-attempting _hedge_or_roll_if_eligible on an already-hedged
        # position, a case _try_build_hedge was never designed for. Every
        # OTHER exit below (ITM-gate, ltp_decay, ratio_exit, exit_rules,
        # vwap_rise, TSL) is deliberately UNCHANGED and still runs while
        # hedged -- direct user spec: "hedge and sell are 2 diff entity, all
        # exit which we were checking before hedge will be there except
        # day_profit_target and day_loss_sl [and day-low reversal / post1500,
        # gated separately below]."
        if self._initial_net_credit > 0 and not pos.is_hedged_positional:
            if self._day_exit_basis == "theta" and self._initial_entry_time_value > 0:
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                _running_theta = (_etv - pos.current_time_value(self._spot)) if _etv > 0 else pnl
                total_day_pts = self._session_realized_pnl_pts + self._combined_pnl_pts(pos, _running_theta)
                _day_denom = self._initial_entry_time_value
                _basis_lbl = "theta(cumulative)"
            else:
                total_day_pts = self._session_realized_pnl_pts + self._combined_pnl_pts(pos, pnl)
                _day_denom = self._initial_net_credit
                _basis_lbl = "ltp"
            total_day_pct = total_day_pts / _day_denom * 100

            if self._day_profit_target_pct > 0 and total_day_pct >= self._day_profit_target_pct:
                # 2026-09-23 fix (log-noise pass, same class as VWAP RISE): this
                # used to log unconditionally on EVERY tick from first-true until
                # the defer boundary -- gate it the same way, once on first
                # detection, once on execution.
                _was_pending = getattr(self, "_exit_pending_reason", None) == "day_profit_target"
                _execute_now = self._defer_exit("day_profit_target", now)
                if _execute_now or not _was_pending:
                    logger.info(
                        "SellStraddle[%s]: DAY PROFIT TARGET [%s] — day=%.1f%% (≥%.1f%%) | "
                        "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                        self._underlying, _basis_lbl, total_day_pct, self._day_profit_target_pct,
                        self._session_realized_pnl_pts, pnl, _day_denom,
                        pos.net_credit, pos.current_value,
                    )
                if not _execute_now:
                    return
                self._stop_for_day = True
                await self._close_position_and_hedge("day_profit_target")
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (profit target reached).", self._underlying)
                return

            if self._day_loss_sl_pct > 0 and total_day_pct <= -self._day_loss_sl_pct:
                _was_pending = getattr(self, "_exit_pending_reason", None) == "day_loss_sl"
                _execute_now = self._defer_exit("day_loss_sl", now)
                if _execute_now or not _was_pending:
                    logger.info(
                        "SellStraddle[%s]: DAY LOSS SL [%s] — day=%.1f%% (≤-%.1f%%) | "
                        "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                        self._underlying, _basis_lbl, total_day_pct, self._day_loss_sl_pct,
                        self._session_realized_pnl_pts, pnl, _day_denom,
                        pos.net_credit, pos.current_value,
                    )
                if not _execute_now:
                    return
                # 2026-10-06 CRITICAL FIX, direct user correction: REVERTS the
                # 2026-09-29 change (commit 801929b) back to the 2026-09-11
                # spec -- real 2026-10-05 incident: day_loss_sl_roll fired 3x
                # in under 25 minutes (13:45/13:46/14:06), each roll landing
                # worse than the last (+2.90 -> -17.10 -> -51.15pts), because
                # a single-side roll has no built-in loss cap -- if spot keeps
                # trending the same direction it just keeps re-entering fresh
                # losing strikes. A day-loss-SL breach now ALWAYS builds the
                # 4-leg hedge (reusing the same _try_build_hedge mechanism EOD
                # hedge-and-carry uses, via _hedge_or_roll_if_eligible) instead
                # of rolling -- direct user spec, no roll fallback even when
                # self._hedge_carry_enabled is off (option A: "always hedges").
                # stop_for_day_on_hedge=False -- this fires mid-day, not at
                # EOD, so a successful hedge must NOT stop the book for the
                # day; it converts to a positional carry and every OTHER exit
                # check keeps running on it (day_profit_target/day_loss_sl
                # themselves are what then skip, via pos.is_hedged_positional,
                # same as the existing EOD hedge-and-carry gate above).
                # 2026-10-06 CRITICAL SAFETY FIX, found before this ever went
                # live: _defer_exit's "_execute_now" fires on nearly EVERY
                # TICK once inside its boundary window (proven by yesterday's
                # real log -- "no roll partner found" spammed hundreds of
                # times per minute for over a minute straight). The old
                # roll version was safe to spam because _single_side_roll has
                # its OWN internal 60s throttle (_ROLL_RETRY_SECONDS) and only
                # ever does an in-memory search. _try_build_hedge has NO such
                # throttle and dispatches REAL broker BUY orders -- without a
                # guard here, a tick where a valid hedge strike briefly
                # qualifies would re-attempt (and potentially re-dispatch)
                # every single tick. Two protections, mirroring this
                # codebase's own existing patterns exactly:
                # 1. _eod_decision_in_progress (the SAME reentrancy guard the
                #    EOD hedge path already uses) -- prevents two overlapping
                #    hedge-build attempts (this codebase already had a real
                #    incident from exactly this race on the EOD path).
                # 2. An explicit 60s minimum retry gap (matching rolling.py's
                #    own _ROLL_RETRY_SECONDS), so a repeatedly-failing hedge
                #    attempt doesn't re-run find_hedge_strike/dispatch every
                #    tick even once the reentrancy guard above has cleared.
                if getattr(self, "_eod_decision_in_progress", False):
                    return
                _last_attempt = getattr(self, "_last_day_loss_hedge_attempt", None)
                if _last_attempt and (now - _last_attempt).total_seconds() < 60:
                    return
                self._last_day_loss_hedge_attempt = now
                self._eod_decision_in_progress = True
                try:
                    hedged = await self._hedge_or_roll_if_eligible(pos, now, stop_for_day_on_hedge=False)
                finally:
                    self._eod_decision_in_progress = False
                if hedged:
                    logger.info(
                        "SellStraddle[%s]: DAY LOSS SL — hedge activated (position now carried "
                        "positionally; day_loss_sl/day_profit_target now skipped while hedged).",
                        self._underlying,
                    )
                    self._clog.info("DAY LOSS SL — hedge activated, position now carried positionally")
                    return
                _why = ("hedge_carry_enabled is OFF for this binding"
                        if not getattr(self, "_hedge_carry_enabled", False)
                        else "no valid hedge strike found (or already T-1, closed instead -- see log above)")
                logger.info(
                    "SellStraddle[%s]: DAY LOSS SL — no hedge activated (%s); retrying in 60s; holding "
                    "position unchanged (no roll, no close).",
                    self._underlying, _why,
                )
                self._clog.info("DAY LOSS SL — no hedge activated (%s); holding position unchanged", _why)
                return

        # 2b. ITM PAIR GATE + 70% ROLL PROTECTION -- must run BEFORE the generic
        # ratio/ltp_decay/TSL/exit_rules/vwap_rise checks below, each of which
        # `return`s immediately the moment its own condition fires. An itm-pair-gate
        # pair is, by definition, two ITM legs with a wide strike gap -- exactly the
        # shape most likely to keep the CE/PE premium ratio persistently elevated.
        # 2026-08-17 real incident: ratio_exit kept re-triggering (and re-rolling)
        # every cycle on such a pair, so _check_itm_roll_protection -- previously
        # last in this list -- was starved and never got to run even though its own
        # 70%-budget running loss had genuinely crossed the line. Running these two
        # first (still cheap no-ops when the gate is off / nothing is armed) means a
        # hard ₹-loss cap on an already-profit-funded leg always gets first look.
        await self._check_itm_pair_gate(now)
        await self._check_itm_roll_protection(now)
        if not (self._position and self._position.status == "open"):
            # Either check may have closed the position outright (no roll partner
            # found / no valid recovery strike) -- don't fall through to the
            return

        # 2b-ii. VP/OI REGIME (2026-10-04, opt-in via strategy_params.vp_oi_enabled,
        # default False -- byte-for-byte no-op for every binding not explicitly
        # opted in, including every existing live deployment). Runs right after
        # the ITM-gate/roll-protection hard risk checks, before the generic
        # roll ladder below, per the flowchart's own placement: a structural
        # regime signal outranks routine roll triggers but not a hard loss cap.
        await self._check_vp_oi_regime(now)
        if not (self._position and self._position.status == "open"):
            # Either check may have closed the position outright (no roll partner
            # found / no valid recovery strike) -- don't fall through to the
            # remaining checks below using the now-stale `pos` reference.
            return

        # 2c. R1-BREACH EXIT / S1-BREACH RE-ENTRY on rolled-in legs (2026-09-22,
        # direct user spec) -- runs on EVERY roll reason (any leg tagged
        # single_side_roll_*), immediately after the ITM-gate/roll-protection
        # checks above and before the generic ratio/ltp_decay/TSL/exit_rules/
        # vwap_rise ladder below. See r1_breach_reentry.py's own module
        # docstring for the full mechanic.
        # DISABLED 2026-09-25, direct user instruction: the main rollover
        # partner search alone should govern a rolled-in leg -- no separate
        # early R1-breach exit/replacement watch on top of it. Gated behind
        # r1_breach_reentry_enabled (config.py, default False) rather than
        # deleted outright, so it can be re-enabled per-deployment without a
        # further code change if ever revisited.
        if getattr(self, "_r1_breach_reentry_enabled", False):
            await self._check_r1_breach_and_reentry(now)
            if not (self._position and self._position.status == "open"):
                return
        pos = self._position  # refresh -- a roll above may have swapped legs/strikes

        # 2c. DAY-LOW REVERSAL EXIT (2026-08-18/19, user spec; ONE-TIME REST
        # CALC redesign 2026-08-21). Original design tick-tracked a running
        # minimum in memory from whenever the pair started running, seeded
        # via REST on pair-change, frozen at self._day_low_freeze_time
        # (default 15:00 IST). Real incident: that running-min lived ONLY in
        # memory between position-lifecycle persists -- a genuine 95.05 low
        # at 14:20 was correctly tracked live, then silently erased by a
        # routine restart at 14:49 (well before the 15:00 freeze), leaving
        # the frozen value at 97.15 instead of the true 95.05.
        #
        # Direct user redesign: don't track anything continuously AT ALL.
        # Do NOTHING until self._day_low_freeze_time is reached. The moment
        # it is (by however many restarts it took to get there), do exactly
        # ONE REST fetch covering the pair's entire real trading history up
        # to that moment and compute the true low directly from it --
        # correct regardless of restart count, since it's derived from real
        # historical data, not accumulated live state. self._day_low_computing
        # guards against firing the same REST call twice concurrently (this
        # block runs on every tick; the fetch can take a moment).
        #
        # A rollover/re-entry mid-day still resets tracking to the NEW pair's
        # own history (prior pair's frozen low discarded, not inherited) --
        # same "focus only on the running pair" principle as before. A pair
        # that starts running AFTER freeze time has already elapsed still
        # gets a correct one-time calc, using "now" (not the fixed freeze
        # time) as the cutoff, so it captures that pair's real history up to
        # the moment it's actually checked.
        # 2026-08-28: post1500_exit_enabled reuses this SAME one-time frozen-low
        # value as its own arm condition (see _check_post1500_r1_exit below) --
        # the tracking/freeze computation runs for either flag; only the actual
        # "close both legs" ACTION a few lines down stays gated to
        # day_low_exit_enabled specifically, so a post1500-only binding never
        # gets the old both-legs-close behavior this feature replaces.
        #
        # 2026-09-13, direct user spec: this ENTIRE block (tracking + both
        # consumers -- day-low's own close action below, and post1500's R1
        # exit further down which reads this same frozen value) is skipped
        # while a hedge is standing. Neither mechanic is meaningful once
        # hedged: day-low's close and post1500's per-leg close are both
        # full-close/leg-close actions from the pre-hedge risk regime, and
        # the ONLY full-close trigger while hedged is the cumulative-profit
        # check (section 1b) plus the same-strike-collision guard (1c).
        # Skipping tracking too (not just the close actions) avoids a wasted
        # REST fetch for the freeze calc that no consumer will ever act on
        # while hedged.
        if (self._day_low_exit_enabled or self._post1500_exit_enabled) and not pos.is_hedged_positional:
            _cv = pos.current_value
            # 2026-09-06, direct user follow-up (stale-value audit F6): add
            # expiry_date so an expiry roll landing on identical strike
            # numbers registers as a genuine pair change, not "still the
            # same pair" -- same fix shape as _post1500_pair above.
            _pair_id = (int(pos.ce_leg.strike), int(pos.pe_leg.strike), pos.expiry_date)
            if tuple(getattr(self, "_day_low_tracked_pair", None) or ()) != _pair_id:
                self._day_low_tracked_pair = _pair_id
                self._session_min_straddle_frozen = None
                self._persist_session()
                self._clog.info(
                    "SellStraddle[%s]: DAY-LOW TRACKING RESET — now watching CE%d/PE%d "
                    "(prior pair's frozen low discarded on roll/re-entry); one-time calc "
                    "deferred until %s.",
                    self._underlying, _pair_id[0], _pair_id[1],
                    self._day_low_freeze_time.strftime("%H:%M"),
                )
            if (self._session_min_straddle_frozen is None
                    and not getattr(self, "_day_low_computing", False)
                    and now.time() >= self._day_low_freeze_time):
                self._day_low_computing = True
                try:
                    _low = await self._compute_day_low_for_pair(_pair_id[0], _pair_id[1], now.time())
                except Exception:
                    _low = float("inf")
                finally:
                    self._day_low_computing = False
                # Re-check nothing changed (a roll/close) while the REST call was in flight.
                if not (self._position and self._position.status == "open"):
                    return
                if tuple(getattr(self, "_day_low_tracked_pair", None) or ()) != _pair_id:
                    return
                self._session_min_straddle_frozen = _low if _low != float("inf") else _cv
                self._persist_session()
                self._clog.info(
                    "SellStraddle[%s]: DAY-LOW FROZEN (one-time REST calc @ %s) — CE%d/PE%d "
                    "low=%.2f. From now until squareoff: exit in full the moment the rate "
                    "reaches this value again.",
                    self._underlying, now.strftime("%H:%M"),
                    _pair_id[0], _pair_id[1], self._session_min_straddle_frozen,
                )
            # 2026-09-02 CRITICAL FIX, real incident: this block's own comment
            # above (2026-08-28) already documents the intent -- "post1500_exit_
            # enabled REPLACES day_low_exit's own 'close both legs' action" --
            # but the gate below never actually enforced that when a binding had
            # BOTH flags on simultaneously (this NIFTY binding did). day_low_exit
            # ran first in the ladder and unconditionally closed both legs the
            # instant the frozen low was touched, so post1500's per-leg R1 watch
            # (section 2c-2, right below) never got a chance to arm or run at
            # all. Real incident, 2026-09-02: day-low fired at 15:00:00 and
            # closed the whole position (net +11.00pts, itself a clean exit) --
            # but per direct user spec, that should have been post1500's per-leg
            # R1 watch arming instead, not an immediate both-legs close. Fixed:
            # day_low's own close action now explicitly stands down whenever
            # post1500_exit_enabled is also on -- post1500 is authoritative for
            # any binding that has both configured, matching the comment's own
            # stated intent. Tracking/freeze computation above is UNCHANGED --
            # post1500 still needs that same frozen value for its own arm check.
            if (self._day_low_exit_enabled and not self._post1500_exit_enabled
                    and self._session_min_straddle_frozen is not None
                    and _cv <= self._session_min_straddle_frozen):
                _was_pending = getattr(self, "_exit_pending_reason", None) == "day_low_reversal"
                _execute_now = self._defer_exit("day_low_reversal", now)
                if _execute_now or not _was_pending:
                    self._clog.info(
                        "SellStraddle[%s]: DAY-LOW REVERSAL EXIT — CE%d/PE%d rate=%.2f "
                        "reached its frozen low=%.2f (frozen @ %s) — closing full "
                        "position, stopping for the day.",
                        self._underlying, _pair_id[0], _pair_id[1], _cv,
                        self._session_min_straddle_frozen, self._day_low_freeze_time.strftime("%H:%M"),
                    )
                if not _execute_now:
                    return
                self._stop_for_day = True
                await self._close_position_and_hedge("day_low_reversal_exit")
                return

        # 2c-2. POST-15:00 PER-LEG R1 EXIT (2026-08-28, direct user spec): see
        # _check_post1500_r1_exit's own docstring for the full state machine.
        # Runs AFTER the day-low block above so it can read the SAME frozen-low
        # value that block just computed/updated this tick. Mutually exclusive
        # in practice with day_low_exit_enabled's own close action (a binding
        # opts into one or the other), but both can safely read the shared
        # frozen-low state either way.
        #
        # 2026-09-13, direct user spec: skipped entirely while hedged, same
        # reasoning as the day-low block above -- see that block's own
        # 2026-09-13 comment.
        if self._post1500_exit_enabled and not pos.is_hedged_positional:
            await self._check_post1500_r1_exit(pos, now)
            if not (self._position and self._position.status == "open"):
                return
            # 2026-08-28: once EITHER leg has closed independently under this
            # mechanic, the surviving leg runs the rest of the day on its OWN
            # R1 watch alone -- day%/ITM-gate/ratio/ltp-decay/TSL/exit_rules/
            # vwap_rise below all assume two live legs and must NOT run
            # against a single-leg position (direct user confirmation:
            # "will not consider these exit when 1 leg is open, only R1 logic
            # will survive and EOD"). EOD square-off itself is unaffected --
            # it already ran, unconditionally, earlier in this function.
            if pos.ce_leg_closed or pos.pe_leg_closed:
                return

        # 3. LTP Decay → single-side roll
        if self._ltp_decay_enabled:
            _min_ltp = min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if 0 < _min_ltp < self._ltp_exit_min and self._position and self._position.status == "open":
                # 2026-09-23 fix (log-noise pass): dedupe per running pair, same
                # pattern as vwap_rise -- a failed roll (no partner) would
                # otherwise re-log this line every defer-tf boundary for as long
                # as the pair stays under the decay threshold. The roll attempt
                # itself (_single_side_roll, its own throttled ROLLOVER STARTED
                # log) still runs every cycle unchanged.
                _pair_id = (int(pos.ce_leg.strike), int(pos.pe_leg.strike))
                _was_pending = getattr(self, "_exit_pending_reason", None) == "ltp_decay"
                _execute_now = self._defer_exit("ltp_decay", now)
                _already_alerted = getattr(self, "_ltp_decay_alert_pair", None) == _pair_id
                if (_execute_now or not _was_pending) and not _already_alerted:
                    self._clog.info("SellStraddle[%s]: LTP DECAY min_ltp=%.2f < %.2f — single-side roll",
                                self._underlying, _min_ltp, self._ltp_exit_min)
                    self._ltp_decay_alert_pair = _pair_id
                if not _execute_now:
                    return
                await self._single_side_roll(now, "ltp_decay")
                return

        # 4. Ratio exit → rollover
        if pos.ce_leg.ltp > 0 and pos.pe_leg.ltp > 0:
            ratio = max(pos.ce_leg.ltp, pos.pe_leg.ltp) / min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if ratio >= self._ratio_threshold:
                _pair_id = (int(pos.ce_leg.strike), int(pos.pe_leg.strike))
                _was_pending = getattr(self, "_exit_pending_reason", None) == "ratio_exit"
                _execute_now = self._defer_exit("ratio_exit", now)
                _already_alerted = getattr(self, "_ratio_exit_alert_pair", None) == _pair_id
                if (_execute_now or not _was_pending) and not _already_alerted:
                    self._clog.info("SellStraddle[%s]: RATIO EXIT ratio=%.2fx — single-side roll",
                                self._underlying, ratio)
                    self._ratio_exit_alert_pair = _pair_id
                if not _execute_now:
                    return
                await self._single_side_roll(now, "ratio_exit")
                return

        # 5. Scalable TSL → FULL EXIT (no rollover; reset on next entry)
        # 2026-08-28 fix: _tsl_pnl now folds in the hedge legs' own running
        # P&L when hedged (_combined_pnl_pts) -- same "treat all 4 legs as
        # one" fix as the Day% guardrail above.
        if self._tsl_enabled:
            _tsl_pnl = pnl
            if self._tsl_basis == "theta":
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                if _etv > 0:
                    _tsl_pnl = _etv - pos.current_time_value(self._spot)
            _tsl_pnl = self._combined_pnl_pts(pos, _tsl_pnl)
            if self._check_scalable_tsl(pos, _tsl_pnl):
                # 2026-09-23 fix (log-noise pass): _check_scalable_tsl is
                # level-triggered (True on every tick the running profit sits
                # below the locked level, not just on the crossing tick), so
                # this used to log on every tick until the defer boundary.
                _was_pending = getattr(self, "_exit_pending_reason", None) == "scalable_tsl"
                _execute_now = self._defer_exit("scalable_tsl", now)
                if _execute_now or not _was_pending:
                    logger.info("SellStraddle[%s]: SCALABLE TSL (%s) — locked=%s%.4f pnl=%s%.4f → FULL EXIT",
                                self._underlying, self._tsl_basis,
                                self._ccy_symbol, pos.tsl_high_lock_rs,
                                self._ccy_symbol, self._pnl_rs(_tsl_pnl))
                if not _execute_now:
                    return
                await self._close_position_and_hedge("scalable_tsl")
                return

        # 6. EXIT-EVAL — dynamic exit_rules → single-side roll
        _max_tf = (max((int(r.get("tf", 1)) for r in self._exit_rules), default=1)
                   if self._exit_rules else 5)
        _er_bucket = f"{now.strftime('%Y%m%d_%H')}{(now.minute // _max_tf) * _max_tf:02d}"
        if (now.minute % _max_tf == 0 and now.second >= 5
                and _er_bucket != self._last_exit_rules_bucket):
            self._last_exit_rules_bucket = _er_bucket
            _credit = self._initial_net_credit or pos.net_credit or 0.0
            _passed, _reason = (False, "—")
            try:
                _crit, _exit_dump = self._build_exit_criteria(pos, pnl, _credit)
                self._clog.info(format_exit_eval(self._underlying, pnl, _credit, _crit))
                if _exit_dump is not None:
                    self._clog.info("EXIT-EVAL %s exit_ind_by_tf=%s", self._underlying, _exit_dump)
                # 2026-09-30, direct user request: a real live/REST-replay RSI
                # discrepancy (52.56 live vs ~77 independently recomputed on
                # identical Upstox price data) couldn't be explained from
                # outside the process -- one-time dump of the pool engine's
                # OWN raw 1-min (_mins, _closes) arrays for this exact pair,
                # so they can be compared bar-by-bar against a REST replay.
                # Fires exactly once per process run (self._rsi_diag_dumped
                # guard) to avoid permanent log noise -- remove once the gap
                # is understood.
                if not getattr(self, "_rsi_diag_dumped", False):
                    try:
                        _ce_k = self._pool_engine._key(int(pos.ce_leg.strike), "CE")
                        _pe_k = self._pool_engine._key(int(pos.pe_leg.strike), "PE")
                        self._clog.info(
                            "RSI-DIAG %s CE%d mins=%s closes=%s",
                            self._underlying, int(pos.ce_leg.strike),
                            list(self._pool_engine._mins.get(_ce_k, [])),
                            list(self._pool_engine._closes.get(_ce_k, [])),
                        )
                        self._clog.info(
                            "RSI-DIAG %s PE%d mins=%s closes=%s",
                            self._underlying, int(pos.pe_leg.strike),
                            list(self._pool_engine._mins.get(_pe_k, [])),
                            list(self._pool_engine._closes.get(_pe_k, [])),
                        )
                        self._rsi_diag_dumped = True
                    except Exception:
                        self._clog.exception("RSI-DIAG dump failed (non-fatal).")
                for _n, _d, _h in _crit:
                    if _n == "Dynamic":
                        _passed, _reason = bool(_h), _d
                        break
            except Exception as _exc:
                self._clog.info("EXIT-EVAL %s (formatting error: %s)", self._underlying, _exc)
                if self._exit_rules:
                    _passed, _reason = _eval_rules(
                        self._exit_rules,
                        self._ind_by_tf(pos.ce_leg.strike, pos.pe_leg.strike, self._exit_rules),
                    )

            if self._exit_rules and _passed:
                self._clog.info("SellStraddle[%s]: EXIT_RULES triggered — %s", self._underlying, _reason)
                await self._single_side_roll(now, "exit_rules")
                return
            # Persist the exit-evaluation audit once per max-TF bucket (throttled by bucket).
            audit_exit_eval(
                client_id=getattr(self, "_client_id", "") or "",
                binding_id=getattr(self, "_binding_id", "") or "",
                underlying=self._underlying,
                ts=now,
                pnl=pnl,
                credit=self._initial_net_credit or pos.net_credit or 0.0,
                criteria=[{"name": n, "detail": d, "hit": bool(h)} for n, d, h in _crit],
                ind_by_tf=_exit_dump or {},
                fired=_passed,
                fired_reason=_reason if _passed else "",
            )

        # 7. VWAP Rise SL → smart roll
        if self._vwap_rise_enabled and self._pool_engine.pair_atp_fresh(
                int(pos.ce_leg.strike), int(pos.pe_leg.strike), self._vwap_stale_sec):
            _vp = self._pool_engine.pair_indicators(int(pos.ce_leg.strike), int(pos.pe_leg.strike))
            curr_vwap = float(_vp.get("vwap", 0.0)) if _vp else 0.0
            _vp_close = float(_vp.get("close", 0.0)) if _vp else 0.0
            # 2026-09-29, direct user request: curr_vwap is ALWAYS the COMBINED
            # ce_atp+pe_atp sum (see pool_indicator_engine.pair_indicators) -- a
            # single leg spiking can drive the whole "rise" even if the other leg
            # barely moved, and this binding's vwap_source="calculative" already
            # skips the one diagnostic (SHADOW_VWAP) that would otherwise show each
            # leg's own VWAP. Read each leg's own atp directly off the pool engine's
            # latest tick so the VWAP RISE line below is per-leg auditable, not just
            # the combined number.
            _ce_atp_now = self._pool_engine._latest.get((int(pos.ce_leg.strike), "CE"), (0.0, 0.0))[1]
            _pe_atp_now = self._pool_engine._latest.get((int(pos.pe_leg.strike), "PE"), (0.0, 0.0))[1]
            _glitch = (pos.vwap_last_good > 0 and curr_vwap > 0
                       and curr_vwap < 0.80 * pos.vwap_last_good)
            if curr_vwap > 0 and not _glitch and (_vp_close <= 0 or curr_vwap >= 0.60 * _vp_close):
                pos.vwap_last_good = curr_vwap
                if curr_vwap < pos.session_min_vwap:
                    pos.session_min_vwap = curr_vwap
                if pos.session_min_vwap < float("inf"):
                    rise_pct = (curr_vwap - pos.session_min_vwap) / pos.session_min_vwap * 100
                    if rise_pct >= self._vwap_rise_threshold:
                        _ce_pnl = float(pos.ce_leg.entry_price) - float(getattr(pos.ce_leg, "ltp", 0.0) or 0.0)
                        _pe_pnl = float(pos.pe_leg.entry_price) - float(getattr(pos.pe_leg, "ltp", 0.0) or 0.0)
                        _less_burning = "CE" if _ce_pnl >= _pe_pnl else "PE"
                        # 2026-08-25 fix: throttled to at most twice per defer cycle (once on
                        # first detection, once on execution) instead of once per tick.
                        # 2026-09-23 fix (user request -- real log showed this still repeating
                        # every defer-tf boundary, e.g. every 1-2 min, for as long as the rise
                        # condition stayed true -- sometimes the whole session -- bloating the
                        # log file with duplicate info that rolling.py's own "ROLLOVER STARTED"
                        # line (which DOES need to log every retry attempt, and does so on its
                        # own throttle) already covers. This line now logs at most ONCE per
                        # rise "episode" -- suppressed on every later boundary for the same
                        # session_min_vwap low, and only re-armed once a roll actually succeeds
                        # (rolling.py resets session_min_vwap=inf, which yields a new low value
                        # and a new episode) or the rise condition drops back below threshold.
                        # The actual roll attempt (_single_side_roll, and its own per-retry
                        # ROLLOVER STARTED / partner-search-trace logging) still runs every
                        # cycle unchanged -- only this outer diagnostic line is de-duplicated.
                        _was_pending = getattr(self, "_exit_pending_reason", None) == "vwap_rise"
                        _execute_now = self._defer_exit("vwap_rise", now)
                        _already_alerted = getattr(self, "_vwap_rise_alert_low", None) == pos.session_min_vwap
                        if (_execute_now or not _was_pending) and not _already_alerted:
                            self._clog.info(
                                "SellStraddle[%s]: VWAP RISE — rise=%.2f%% curr=%.2f low=%.2f "
                                "[CE%d_atp=%.2f PE%d_atp=%.2f] → single-side roll (CE pnl=%.2f PE pnl=%.2f)",
                                self._underlying, rise_pct, curr_vwap, pos.session_min_vwap,
                                int(pos.ce_leg.strike), _ce_atp_now, int(pos.pe_leg.strike), _pe_atp_now,
                                _ce_pnl, _pe_pnl,
                            )
                            self._vwap_rise_alert_low = pos.session_min_vwap
                        if not _execute_now:
                            return
                        await self._single_side_roll(now, "vwap_rise_roll")
                        return

    # ── Close / leg helpers ───────────────────────────────────────────────────

    def discard_position_after_squareoff(self, reason: str) -> None:
        """Clear the in-memory + persisted position WITHOUT sending any exit orders."""
        if not self._position:
            return
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.status = "closed"
        self._session_realized_pnl_pts += pos.realized_pnl
        self._itm_roll_protection = {}
        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        logger.info(
            "SellStraddle[%s|%s|%s]: position DISCARDED after external square-off (%s) — pnl=%.2f pts; "
            "cleared persisted store so it will NOT restore on restart.",
            self._underlying, _cid, _bid, reason, pos.realized_pnl,
        )
        self._position = None
        self._persist()

    # Max time to wait for the bridge to confirm (or abort) a full-position EXIT before giving
    # up and leaving the position open for a later retry.
    # 2026-08-06 CRITICAL FIX: must exceed the bridge's OWN worst-case time to determine a
    # fill, or this timer always loses the race. Confirmed live incident: straddle_bridge.py's
    # exit executor waits up to market_fill_timeout_sec=8.0s (smart_executor.py), and if still
    # under-filled, the bridge's own under-fill retry loop then polls get_order_status for up to
    # 15 MORE seconds (range(15) x 1s, straddle_bridge.py::_do_leg -- extended from 5s->15s
    # earlier the same day to give Zerodha time to settle a genuinely-filling order). That's a
    # ~23s worst case for the bridge to publish ANY fill (real or paper_route's simulated one) --
    # but this constant was still 15s, so _close_position gave up ~8s before the bridge could
    # ever answer. The position was left "open" every single time a close didn't fill instantly
    # (always true for paper_route's expected broker-rejection path, and for any live exit that
    # takes more than an instant to confirm). The very next tick then saw the position still
    # open, past force-exit, and fired a BRAND NEW real EOD close order -- repeating every
    # ~15-16s indefinitely, each cycle placing a genuinely new order on the real broker (2026-08-06
    # ssrajpal2001 paper_route: 20+ real BUY orders in 5 minutes at EOD squareoff). Set with real
    # margin over the ~23s bridge worst case, not just barely above it.
    _CLOSE_CONFIRM_TIMEOUT_SEC = 35.0

    # Max time to hold ALL exit checks after a restart-restore before treating a still-
    # not-fresh leg as a stuck data feed rather than just slow warm-up (see POST-RESTORE
    # WARM-UP GUARD above). 2026-08-05: a real restart took CE 82s to get its first tick
    # -- the original 20s ceiling let exits run on a stale leg price for ~60s of that.
    # User-specified: 5 minutes -- long enough that a genuine feed problem, not just
    # slow warm-up, is the real explanation; past this the position is CLOSED rather
    # than armed for trading on an unknown/frozen leg price.
    _POST_RESTORE_WARMUP_MAX_SEC = 300.0

    @staticmethod
    def _market_genuinely_open() -> bool:
        """2026-09-17: real wall-clock check, independent of self._market_open_dt
        (which is only set by the first 1-min CANDLE_CLOSE and may still be None
        this early) -- used to stop the post-restore warm-up safety close from
        attempting a real order during NSE's pre-open/call-auction session. See
        the POST-RESTORE WARM-UP GUARD's own comment for the real incident."""
        from config.global_config import ExchangeConfig, IST
        now_t = datetime.now(IST).time()
        cfg = ExchangeConfig()
        return cfg.market_open <= now_t < cfg.market_close

    async def _close_surviving_leg_and_finalize(self, reason: str) -> None:
        """2026-08-28: EOD (or any other future caller) closing a position that
        already has ONE leg closed independently via the post-15:00 R1 mechanic
        -- close ONLY the still-open side via _close_leg (never the normal
        dual-leg _close_position order), then finalize the position exactly
        like a normal full close. The already-closed leg's P&L was booked into
        self._session_realized_pnl_pts once already, at the time IT closed --
        do not add pos.realized_pnl again here.

        2026-09-02 CRITICAL FIX, real incident: this had no concept of hedge
        legs, unlike every other full-close path (_close_position_and_hedge).
        _maybe_prehedge itself is now gated against ever building a hedge in
        single-leg mode (see its own docstring), but this is the defensive
        backstop for the case a hedge is ALREADY standing when single-leg
        mode kicks in regardless (e.g. carried in from a prior day's EOD
        hedge-and-carry, then post1500 R1 closes one leg the next session)
        -- without this, the hedge legs would be silently orphaned the same
        way the 2026-09-01 incident orphaned a freshly-built one."""
        pos = self._position
        if not pos:
            return
        if pos.is_hedged_positional:
            await self._close_hedge_legs(pos, reason)
        surviving = "PE" if pos.ce_leg_closed else "CE"
        if getattr(pos, f"{surviving.lower()}_leg_closed"):
            # Both sides already closed (shouldn't reach here -- _check_post1500_r1_exit
            # already finalizes and nulls self._position the moment both close -- but
            # guard anyway rather than double-finalizing).
            return
        now = datetime.now(IST)
        order_ev = await self._close_leg(surviving, reason, now)
        if getattr(order_ev, "close_aborted", False):
            logger.critical(
                "SellStraddle[%s]: surviving leg %s close NOT confirmed (reason=%s) — "
                "left open, will retry on a later tick.",
                self._underlying, surviving, reason,
            )
            return
        setattr(pos, f"{surviving.lower()}_leg_closed", True)
        pos.status = "closed"
        pos.close_reason = reason
        pos.close_time = now
        self._unpin_position_legs(pos)
        self._position = None
        await self._unsubscribe_entry_expiry_tokens()
        # 2026-09-07 fix: same restore-only sticky-expiry-pin release as
        # _close_position() -- see that method's own writeup for the real
        # incident this fixes.
        if self._entry_expiry_pinned_from_restore:
            self._entry_expiry_pinned_from_restore = False
            self._expiry_shifted_low_anchor_ltp = False
            self._entry_expiry_date = self._effective_entry_expiry()
        self._apply_sl_cooldown()
        self._persist()
        logger.info(
            "SellStraddle[%s]: surviving %s leg closed [%s] — position finalized.",
            self._underlying, surviving, reason,
        )
        self._clog.info("surviving %s leg closed [%s] — position finalized.", surviving, reason)

    async def _close_position(self, reason: str) -> None:
        # 2026-08-06 CONFIRM-MODEL REDESIGN: pos.status is now the reentrancy guard, not the
        # ephemeral _close_in_progress flag. Set to "closing" SYNCHRONOUSLY here, before the
        # first await -- this is what stops a duplicate close from ever being dispatched,
        # regardless of how long the real fill takes to confirm (the confirm wait below no
        # longer has to be short to prevent duplicates; it only decides how long we wait
        # before giving up and reverting to "open" for a retry).
        if not self._position or self._position.status != "open":
            return
        # 2026-08-28: a post1500_exit_enabled position that already closed ONE
        # leg independently (pos.ce_leg_closed/pe_leg_closed) must never be
        # closed via the normal dual-leg path below -- that always dispatches
        # a combined EXIT for BOTH ce_strike/pe_strike, which would send a
        # real duplicate order for the leg that's already flat AND double-book
        # its P&L (already booked once by _close_leg at the time it closed).
        # The only path that can still reach _close_position in this state is
        # EOD square-off (_check_exits' single-leg-mode gate skips every other
        # caller of _close_position/_close_position_and_hedge once a leg is
        # closed) -- so this is effectively EOD-only, but the guard is placed
        # here (not just at the EOD call site) so ANY future caller is safe.
        if self._position.ce_leg_closed or self._position.pe_leg_closed:
            await self._close_surviving_leg_and_finalize(reason)
            return
        self._position.status = "closing"
        self._roll_in_progress = False
        self._itm_roll_protection = {}
        try:
            from execution_bridge.straddle_bridge import StraddleOrderEvent
            pos = self._position
            # Do NOT null self._position, mark it closed, or persist/cooldown/audit yet -- the
            # exit order has not been confirmed by the broker. Compute what finalization will
            # need, dispatch the order, then WAIT for _on_fill to confirm a real fill (or an
            # exit_aborted signal from the bridge) before touching any of that. This mirrors the
            # _roll_close_waiters pattern _single_side_roll already uses for single-leg closes.
            # (2026-08-04 incident: the old code finalized -- nulled self._position, persisted,
            # applied cooldown -- BEFORE the order was even dispatched, so a broker outage during
            # a live EXIT silently discarded a still-open real position.)
            realized_pnl = pos.unrealized_pnl
            close_time = datetime.now(IST)
            _close_remark = self._close_remark(pos, reason, close_time)

            _cid = getattr(self, "_client_id", "") or "-"
            _bid = getattr(self, "_binding_id", "") or "-"
            logger.info(
                "SellStraddle[%s|%s|%s]: CLOSING — reason=%s pnl=%s%.4f (%.2f pts) "
                "CE %.2f→%.2f PE %.2f→%.2f (awaiting broker confirmation)",
                self._underlying, _cid, _bid, reason,
                self._ccy_symbol, self._pnl_rs(realized_pnl), realized_pnl,
                pos.ce_leg.entry_price, pos.ce_leg.ltp,
                pos.pe_leg.entry_price, pos.pe_leg.ltp,
            )
            self._clog.info(
                "CLOSING — reason=%s pnl=%.2fpts CE %.2f→%.2f PE %.2f→%.2f (awaiting confirmation)",
                reason, realized_pnl,
                pos.ce_leg.entry_price, pos.ce_leg.ltp,
                pos.pe_leg.entry_price, pos.pe_leg.ltp,
            )

            self._event_counter += 1
            order_ev = StraddleOrderEvent(
                action="EXIT",
                underlying=self._underlying,
                atm=pos.atm_at_entry,
                ce_strike=pos.ce_leg.strike,
                pe_strike=pos.pe_leg.strike,
                ce_ltp=pos.ce_leg.ltp,
                pe_ltp=pos.pe_leg.ltp,
                lot_multiplier=self._lot_multiplier,
                lot_size=self._lot_size,
                spot=self._spot,
                close_reason=reason,
                close_remark=_close_remark,
                realized_pnl=realized_pnl,
                ce_entry=pos.ce_leg.entry_price,
                pe_entry=pos.pe_leg.entry_price,
                event_id=f"{self._underlying}_EXIT_{self._event_counter}",
                leg_open_times={
                    "CE": pos.ce_leg.open_time.isoformat() if pos.ce_leg.open_time else None,
                    "PE": pos.pe_leg.open_time.isoformat() if pos.pe_leg.open_time else None,
                },
                leg_open_reasons={
                    "CE": pos.ce_leg.open_reason,
                    "PE": pos.pe_leg.open_reason,
                },
                expiry=pos.expiry_date,
                close_time=close_time,
            )

            eid = order_ev.event_id
            waiter = asyncio.Event()
            self._roll_close_waiters[eid] = waiter
            try:
                await self._emit_order(order_ev)
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=self._CLOSE_CONFIRM_TIMEOUT_SEC)
                except asyncio.TimeoutError:
                    logger.critical(
                        "SellStraddle[%s|%s|%s]: EXIT fill NOT CONFIRMED within %.0fs "
                        "(event_id=%s reason=%s) — reverting to OPEN; will retry on a "
                        "later tick. NOT booking P&L, NOT applying cooldown.",
                        self._underlying, _cid, _bid, self._CLOSE_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    self._clog.critical(
                        "EXIT fill NOT CONFIRMED within %.0fs (event_id=%s reason=%s) — "
                        "reverting to OPEN.", self._CLOSE_CONFIRM_TIMEOUT_SEC, eid, reason,
                    )
                    pos.status = "open"
                    return
            finally:
                self._roll_close_waiters.pop(eid, None)

            fill = self._roll_close_results.pop(eid, None)
            if fill is not None and (getattr(fill, "exit_aborted", False)
                                      or getattr(fill, "placement_failed", False)):
                _why = "placement failed" if getattr(fill, "placement_failed", False) else "broker unavailable"
                logger.critical(
                    "SellStraddle[%s|%s|%s]: EXIT ABORTED by bridge (%s, "
                    "event_id=%s reason=%s) — reverting to OPEN; will retry on a later "
                    "tick. NOT booking P&L, NOT applying cooldown.",
                    self._underlying, _cid, _bid, _why, eid, reason,
                )
                self._clog.critical(
                    "EXIT ABORTED by bridge (%s, event_id=%s reason=%s) — reverting to OPEN.",
                    _why, eid, reason,
                )
                pos.status = "open"
                return

            # ── Confirmed by the broker (or a paper/paper_route sim fill) — finalize ──────
            # 2026-08-20: a standing hedge OUTLIVES the sold pair it was built against --
            # stash it before this position object is discarded so the NEXT fresh entry
            # (_open_position) can carry it forward and re-run the same-strike-collision
            # guard. Only stash on a genuine full close of a hedged position (reason
            # already handles t1_expiry_close's own hedge teardown separately via
            # _close_hedge_legs -- by the time we get here for that reason the hedge legs
            # are already None, so this is a no-op for that path).
            if pos.hedge_ce_leg is not None or pos.hedge_pe_leg is not None:
                self._pending_hedge_ce_leg = pos.hedge_ce_leg
                self._pending_hedge_pe_leg = pos.hedge_pe_leg
                self._clog.info(
                    "HEDGE — standing hedge legs carried past this close (reason=%s), "
                    "will attach to the next fresh entry.", reason,
                )
            self._position = None
            pos.realized_pnl = realized_pnl
            pos.close_reason = reason
            pos.close_time = close_time
            pos.ce_leg.close_time = close_time
            pos.pe_leg.close_time = close_time
            pos.status = "closed"
            self._unpin_position_legs(pos)

            logger.info(
                "SellStraddle[%s|%s|%s]: CLOSED — reason=%s pnl=%s%.4f (%.2f pts) confirmed "
                "(event_id=%s).",
                self._underlying, _cid, _bid, reason,
                self._ccy_symbol, self._pnl_rs(pos.realized_pnl), pos.realized_pnl, eid,
            )
            self._clog.info("CLOSED — reason=%s pnl=%.2fpts confirmed (event_id=%s).",
                             reason, pos.realized_pnl, eid)

            self._session_realized_pnl_pts += pos.realized_pnl
            logger.info(
                "SellStraddle[%s]: Session P&L — trade=%.2fpts cumulative=%.2fpts "
                "(day=%.1f%% of initial credit=%.2f)",
                self._underlying, pos.realized_pnl, self._session_realized_pnl_pts,
                (self._session_realized_pnl_pts / self._initial_net_credit * 100)
                if self._initial_net_credit > 0 else 0.0,
                self._initial_net_credit,
            )

            self._persist()
            await self._unsubscribe_entry_expiry_tokens()
            # 2026-09-07 CRITICAL FIX, real incident: release the sticky expiry
            # pin if it was only ever armed to protect a RESTORED position's own
            # expiry across a restart (see engine.py's _entry_expiry_pinned_from_
            # restore for the full writeup) -- that reason ends the moment this
            # very position closes. Without this, the pin stayed on the closed
            # position's (possibly stale/illiquid) expiry for the rest of the
            # day, pool warm-seeding kept failing against it, and since this
            # book's main loop is tick-driven, a subscription that never
            # resolves left it permanently silent -- no exception, just dead
            # air until the next restart. Recompute fresh immediately so the
            # NEXT entry scan uses the genuinely current expiry, not the stale
            # one. Deliberately does NOT touch the flag for the legitimate
            # same-day "expiry-day shift" reason (entries.py/exits.py's own
            # sets of this flag never touch _entry_expiry_pinned_from_restore).
            if self._entry_expiry_pinned_from_restore:
                self._entry_expiry_pinned_from_restore = False
                self._expiry_shifted_low_anchor_ltp = False
                self._entry_expiry_date = self._effective_entry_expiry()
                logger.info(
                    "SellStraddle[%s]: released restore-only sticky expiry pin on close -- "
                    "next entry scan will use the genuinely current expiry (%s).",
                    self._underlying,
                    self._entry_expiry_date.isoformat() if self._entry_expiry_date else None,
                )
            # 2026-09-07, direct user spec: "the close which happened today was not
            # due to the condition met it was due to the ltp was not coming so it
            # will be considered that we will start from beginning." A
            # post_restore_data_stale close is a defensive safety-close caused by
            # a data/feed problem, never a genuine trading decision (unlike
            # day_loss_sl/day_profit_target/day_low_reversal_exit/EOD/ITM-pair-gate
            # -- real exits driven by the strategy's own rules; single-side rolls
            # like decay/ratio_exit/exit_rules/vwap_rise never touch trades_today
            # at all, since they don't go through entry-selection). Undo the
            # trades_today increment this position's own original entry made, so
            # is_beginning (entries.py: self._trades_today == 0) is true again on
            # the next cycle -- the strategy has not genuinely completed its first
            # real trade of the day, so it should still be treated as one.
            if reason == "post_restore_data_stale" and self._trades_today > 0:
                self._trades_today -= 1
                logger.info(
                    "SellStraddle[%s]: post_restore_data_stale close was not a genuine "
                    "trading decision -- trades_today reverted to %d so the next entry "
                    "is evaluated as BEGINNING, not RE-ENTRY.",
                    self._underlying, self._trades_today,
                )
            # Cooldown is for organic SL events; forced liquidation / deployment removal
            # should not penalise future entries (and kill-switch means no future entries).
            # 2026-09-17, direct user spec: hedge_cumulative_profit added alongside
            # itm_pair_gate_profit -- both are genuine profit-driven full closes, not
            # SL/loss events, and the next entry should be evaluated as BEGINNING
            # immediately, no cooldown delay.
            if reason not in ("itm_pair_gate_profit", "hedge_cumulative_profit", "kill_switch",
                              "deployment_stop", "system_shutdown"):
                self._apply_sl_cooldown()
            audit_exit_exec(
                client_id=_cid,
                binding_id=_bid,
                underlying=self._underlying,
                ts=pos.close_time or datetime.now(IST),
                reason=reason,
                realized_pnl=float(pos.realized_pnl or 0.0),
                ce_entry=float(pos.ce_leg.entry_price or 0.0),
                pe_entry=float(pos.pe_leg.entry_price or 0.0),
                ce_exit=float(pos.ce_leg.ltp or 0.0),
                pe_exit=float(pos.pe_leg.ltp or 0.0),
            )
        finally:
            # Safety net for a truly unexpected exception escaping the try block above (a bug
            # in _persist()/_close_remark()/etc, not a normal abort path -- those already reset
            # status to "open" explicitly before returning). Without this, such an exception
            # would leave pos.status stuck at "closing" forever, permanently blocking every
            # future exit check for this position -- worse than the bug it would be escaping.
            if self._position is pos and pos.status == "closing":
                logger.critical(
                    "SellStraddle[%s]: _close_position exited unexpectedly while still "
                    "'closing' (reason=%s) — reverting to OPEN so exit checks can resume.",
                    self._underlying, reason,
                )
                pos.status = "open"

    async def _close_leg(self, side: str, reason: str, now: datetime) -> StraddleOrderEvent:
        """Close ONE leg (publish EXIT legs=[side]) and WAIT for the bridge to confirm the fill
        before booking that leg's P&L into the session total.

        Returns the emitted order event; callers MUST check `order_ev.close_aborted` before
        proceeding to open a new partner or otherwise treating the leg as actually closed --
        on a broker-unavailable / unconfirmed close, the leg is left exactly as it was (still
        open, no P&L booked) and `close_aborted=True` is set on the return value.
        """
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            # Return a sentinel with the expected attributes so callers don't crash.
            return StraddleOrderEvent(
                action="EXIT", underlying=self._underlying, atm=0.0,
                ce_strike=0.0, pe_strike=0.0, ce_ltp=0.0, pe_ltp=0.0,
                event_id="", legs=[side], close_aborted=True,
            )
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        if leg.entry_price and leg.entry_price > 0:
            leg_pnl = leg.entry_price - leg.ltp
        else:
            leg_pnl = 0.0
            logger.error("SellStraddle[%s]: %s%d entry_price=%.2f invalid at close — booking pnl=0 "
                         "(NOT a real loss; entry was lost). reason=%s",
                         self._underlying, side, int(leg.strike), float(leg.entry_price or 0.0), reason)
        _close_remark = self._close_remark(pos, reason, now, side=side)
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="EXIT", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, close_reason=reason, close_remark=_close_remark, realized_pnl=leg_pnl,
            ce_entry=pos.ce_leg.entry_price, pe_entry=pos.pe_leg.entry_price,
            event_id=f"{self._underlying}_EXITLEG_{side}_{self._event_counter}",
            legs=[side],
            leg_open_times={side: leg.open_time.isoformat() if leg.open_time else None},
            leg_open_reasons={side: leg.open_reason},
            expiry=pos.expiry_date,
        )

        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        eid = order_ev.event_id
        waiter = asyncio.Event()
        self._roll_close_waiters[eid] = waiter
        try:
            await self._emit_order(order_ev)
            try:
                # 2026-08-06: was hardcoded 10.0, shorter than the bridge's own
                # documented worst-case confirmation latency (confirmed live: Kite
                # needed longer than 5s to settle a genuinely-filling order, which is
                # why the bridge's own retry loop was extended to 15s on top of the
                # executor's own wait). A too-short local wait here risks the SAME
                # leg being closed twice -- once by a late-arriving real fill, once
                # by a second close order sent after this wait gave up too early.
                # Aligned to the same constant _close_position already uses.
                await asyncio.wait_for(waiter.wait(), timeout=self._CLOSE_CONFIRM_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.critical(
                    "SellStraddle[%s|%s|%s]: LEG CLOSE %s NOT CONFIRMED within %.0fs (event_id=%s "
                    "reason=%s) — leg left OPEN, no P&L booked; caller must abort the roll.",
                    self._underlying, _cid, _bid, side, self._CLOSE_CONFIRM_TIMEOUT_SEC, eid, reason,
                )
                order_ev.close_aborted = True
                return order_ev
        finally:
            self._roll_close_waiters.pop(eid, None)

        fill = self._roll_close_results.pop(eid, None)
        if fill is not None and getattr(fill, "exit_aborted", False):
            logger.critical(
                "SellStraddle[%s|%s|%s]: LEG CLOSE %s ABORTED by bridge (broker unavailable, "
                "event_id=%s reason=%s) — leg left OPEN, no P&L booked; caller must abort the roll.",
                self._underlying, _cid, _bid, side, eid, reason,
            )
            order_ev.close_aborted = True
            return order_ev

        # ── Confirmed — finalize this leg's close ─────────────────────────────
        leg.close_time = now
        self._session_realized_pnl_pts += leg_pnl
        logger.info("SellStraddle[%s|%s|%s]: CLOSE LEG %s strike=%.0f pnl=%.2fpts [%s] confirmed",
                    self._underlying, _cid, _bid, side, leg.strike, leg_pnl, reason)
        self._clog.info("CLOSE LEG %s strike=%.0f pnl=%.2fpts [%s] confirmed",
                        side, leg.strike, leg_pnl, reason)
        return order_ev

    # ── VP/OI REGIME (2026-10-04, opt-in, see config.py's vp_oi_enabled) ──────
    #
    # The "naked long" this adds (one side's SOLD leg exited + a hedge bought
    # in its place, per the 27-row matrix) is deliberately tracked in its OWN
    # bookkeeping (self._vp_oi_naked_leg), NOT via StraddlePosition's existing
    # hedge_ce_leg/hedge_pe_leg/is_hedged_positional fields -- those belong to
    # the EOD hedge-and-carry feature and specifically mean "BOTH sold legs
    # are closed, running a 4-leg T-1 carry." A VP/OI single-leg exit only
    # ever removes ONE sold leg while the other keeps running/rolling
    # normally -- setting is_hedged_positional here would wrongly pull this
    # position into the EOD feature's T-1/4-leg-close logic. Reusing
    # pos.ce_leg_closed/pe_leg_closed IS correct and intentional, though --
    # that flag's "one leg closed, the other runs solo" meaning already
    # exists in this codebase (see _check_post1500_r1_exit above) and every
    # other ladder step already respects it.
    #
    # KNOWN LIMITATION: self._vp_oi_naked_leg is in-memory only, not
    # persisted/restored across a restart (same accepted gap as CAG
    # Straddle's own standing-order state -- see that strategy's own
    # module docstring for the precedent). A restart while a naked long is
    # open loses the 9-EMA/re-entry watch for that leg; the leg itself is
    # NOT force-closed by this gap, just no longer actively managed by this
    # mechanic until the adapter/9-EMA re-warms (it won't, having no
    # persistence) -- flagged here, not silently papered over.
    async def _check_vp_oi_regime(self, now: datetime) -> None:
        adapter = getattr(self, "_vp_oi_adapter", None)
        if not getattr(self, "_vp_oi_enabled", False) or adapter is None:
            return
        pos = self._position
        if not pos or pos.status != "open":
            return
        if not hasattr(self, "_vp_oi_naked_leg"):
            self._vp_oi_naked_leg: dict = {}
        if not hasattr(self, "_vp_oi_last_exited_strike"):
            self._vp_oi_last_exited_strike: dict = {}

        # Feed the Volume Profile off the futures/spot price every tick, bucketed
        # into 1-min bars. Real traded volume isn't plumbed to this engine today
        # (see live_adapter.py's own docstring) -- every tick within a bar is
        # weighted equally (tick count as a volume proxy), a documented, honest
        # simplification, not fabricated volume data.
        fut_px = float(getattr(self, "_futures_spot", 0.0) or getattr(self, "_spot", 0.0) or 0.0)
        if fut_px > 0:
            minute = now.replace(second=0, microsecond=0)
            acc = getattr(self, "_vp_oi_fut_bar", None)
            if acc is None:
                self._vp_oi_fut_bar = {"minute": minute, "h": fut_px, "l": fut_px, "n": 1}
            elif acc["minute"] != minute:
                # 2026-10-04: real futures OI, when the primary feeder
                # populates it (see IndexTick.oi's own docstring) -- 0.0
                # otherwise (non-futures_atm underlying, or a provider that
                # doesn't send it), in which case FuturesRolloverTracker
                # degrades to "No Change" forever, same honest fallback this
                # module already had before OI capture existed.
                adapter.on_futures_bar(acc["h"], acc["l"], volume=float(acc["n"]),
                                        oi=float(getattr(self, "_futures_oi", 0.0) or 0.0),
                                        ts=acc["minute"].timestamp())
                self._vp_oi_fut_bar = {"minute": minute, "h": fut_px, "l": fut_px, "n": 1}
            else:
                acc["h"] = max(acc["h"], fut_px)
                acc["l"] = min(acc["l"], fut_px)
                acc["n"] += 1

        # Any side currently naked or awaiting re-entry owns this tick --
        # don't also re-evaluate a fresh regime decision the same cycle.
        # (Naked-leg management itself already ran this tick, unconditionally,
        # via _check_vp_oi_naked_legs_standalone at the very top of
        # _check_exits -- NOT repeated here, to avoid double-feeding the
        # same tick's price into the 9-EMA / double-firing a stop-exit.)
        if self._vp_oi_naked_leg or adapter.awaiting_reentry:
            return

        dr = adapter.evaluate(spot=float(getattr(self, "_spot", 0.0) or 0.0), now_ts=now.timestamp())

        # 2026-10-05, real live-day gap found while debugging the first paper
        # session: this whole module only ever logged when it actually ACTED
        # (closed a leg, bought a hedge) -- total silence was indistinguishable
        # from "working fine, nothing actionable yet" vs. "quietly stuck".
        # Throttled heartbeat (same ~60s cadence as EXIT-CHECK's own periodic
        # log above) makes the live state actually observable.
        import time as _t_vpoi
        if _t_vpoi.monotonic() - getattr(self, "_vp_oi_last_heartbeat", 0.0) > 60.0:
            self._vp_oi_last_heartbeat = _t_vpoi.monotonic()
            if dr is None:
                self._clog.info(
                    "VP/OI REGIME — not warm yet (OiRegimeTracker needs full "
                    "ATM±5 CE+PE OI coverage before it returns a reading)."
                )
            else:
                self._clog.info(
                    "VP/OI REGIME — regime=%s call_action=%s put_action=%s "
                    "call_hedge=%s put_hedge=%s futures_oi=%.0f",
                    dr.regime, dr.call_action, dr.put_action,
                    dr.call_hedge, dr.put_hedge, float(getattr(self, "_futures_oi", 0.0) or 0.0),
                )

        if dr is None:
            return

        if dr.regime.startswith("Highly Bearish") and not pos.pe_leg_closed:
            await self._vp_oi_handle_directional(pos, "PE", dr.put_hedge, now)
        elif dr.regime.startswith("Highly Bullish") and not pos.ce_leg_closed:
            await self._vp_oi_handle_directional(pos, "CE", dr.call_hedge, now)
        else:
            # 2026-10-09: regime left Highly Bearish/Bullish (or the other
            # leg already closed) -- clear any OTM-shift state so a FUTURE
            # Highly Bearish/Bullish episode on this side starts fresh
            # instead of thinking it already shifted.
            self._vp_oi_adapter.shifted_strike.pop("PE", None)
            self._vp_oi_adapter.shifted_strike.pop("CE", None)
        if dr.regime == "Volatile" and not (pos.ce_leg_closed or pos.pe_leg_closed):
            # 2026-10-05: re-enabled now that _check_vp_oi_naked_legs_standalone
            # runs unconditionally from the top of _check_exits (before the
            # "no open position" early-return) -- a full close here sets
            # self._position to None, but naked-leg management (9-EMA stop +
            # Hedge Exit Rule) keeps running regardless. The Re-entry Rule's
            # own re-SELL side still won't fire in this specific state (needs
            # a fresh BEGINNING-style entry with no reference leg to balance
            # against -- see _vp_oi_resell_leg's own pos-is-None branch,
            # logged clearly rather than silently dropped), but the stop/
            # hedge-exit side is fully managed.
            await self._vp_oi_exit_both(dr, now)

    async def _vp_oi_handle_directional(self, pos, side: str, hedge, now: datetime) -> None:
        """2026-10-09, direct user spec (closing the Volume-Profile
        confirmation gap), per the real source table (observations 5/6/23/
        24's own "Action Near Support"/"Action Near Resistance" columns):
        a Highly Bearish/Bullish OI regime no longer exits+hedges the
        losing leg immediately off the regime alone. Three price-location
        outcomes, checked in this order:

        1. LVN volume acceptance beyond the near zone (below VAL for a
           losing PUT, above VAH for a losing CALL) -- "the downside/upside
           trend is confirmed." Real breakout -- exit the leg and buy the
           70%-of-entry-premium replacement long (_vp_oi_exit_leg,
           unchanged), to ride it with the 9-EMA trailing stop.
        2. Price reverses back through POC into the OPPOSITE zone without
           ever reaching that LVN confirmation -- the directional thesis
           failed. Exit the leg FLAT, no replacement hedge (judgment call,
           not explicit in the source doc: the hedge exists to ride a
           CONFIRMED breakout: "Strategy is holding a naked Long Put: Ride
           the downside breakout..." -- buying a fresh directional long
           against a move that just reversed would contradict the read
           that triggered the exit in the first place).
        3. Neither yet -- still testing the near (dangerous) zone, breakout
           not yet confirmed either way. Shift the leg 100pts further OTM
           (the doc's own concrete number; "between POC and VAL/VAH" is a
           qualitative placement refinement with no formula given, not
           implemented) instead of doing nothing. Shifts once per regime
           episode (adapter.shifted_strike), not every cycle."""
        adapter = self._vp_oi_adapter
        snap = adapter.last_snapshot
        spot = float(getattr(self, "_spot", 0.0) or 0.0)
        if snap is None or spot <= 0:
            return  # Volume Profile not warm yet -- safe no-op, same as before this feature existed
        lvn_side = "below" if side == "PE" else "above"
        if snap.in_lvn(spot, lvn_side):
            adapter.add_remark(
                f"{side}: LVN acceptance confirmed {lvn_side} "
                f"{'VAL' if side=='PE' else 'VAH'} (spot={spot:.2f}) -> exiting leg + hedge")
            adapter.shifted_strike.pop(side, None)
            await self._vp_oi_exit_leg(side, hedge, now)
            return

        reversed_through_poc = (spot >= snap.poc) if side == "PE" else (spot <= snap.poc)
        if reversed_through_poc:
            adapter.add_remark(
                f"{side}: price reversed back through POC ({snap.poc:.2f}, spot={spot:.2f}) "
                f"without LVN confirmation -> thesis failed, exiting leg flat (no hedge)")
            adapter.shifted_strike.pop(side, None)
            await self._vp_oi_exit_leg_flat(side, now)
            return

        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        shift_pts = max(100.0, step)  # never shift by less than one real strike step
        already_shifted_from = adapter.shifted_strike.get(side)
        if already_shifted_from == int(leg.strike):
            return  # already shifted this episode, waiting on LVN confirmation
        new_strike = int(leg.strike + shift_pts) if side == "CE" else int(leg.strike - shift_pts)
        new_leg = self._strike_prem.get((new_strike, side))
        new_ltp = float(new_leg.get("ltp", 0.0) or 0.0) if new_leg else 0.0
        if new_ltp <= 0:
            self._clog.info(
                "VP/OI REGIME — %s leg not yet LVN-confirmed, wants OTM shift to %d but no "
                "live quote there yet; retrying next cycle", side, new_strike,
            )
            return
        old_strike = int(leg.strike)
        close_ev = await self._close_leg(side, "vp_oi_regime_otm_shift", now)
        if getattr(close_ev, "close_aborted", False):
            return
        await self._open_leg(side, new_strike, new_ltp, now, "vp_oi_regime_otm_shift")
        adapter.shifted_strike[side] = new_strike
        adapter.add_remark(
            f"{side}: still in near zone (spot={spot:.2f}, POC={snap.poc:.2f}), no LVN "
            f"confirmation yet -> shifted OTM {old_strike}->{new_strike}")
        self._persist()
        self._clog.info(
            "VP/OI REGIME — %s leg shifted OTM %d -> %d (no LVN confirmation yet; "
            "watching for breakout)", side, old_strike, new_strike,
        )

    async def _vp_oi_exit_leg_flat(self, side: str, now: datetime) -> None:
        """2026-10-09: price reversed back through POC without ever
        LVN-confirming the breakout -- the directional thesis failed.
        Close the leg plainly, no replacement hedge, no naked-leg tracking
        (there is no naked long to ride a 9-EMA stop on). The OTHER leg
        keeps running untouched, same as _vp_oi_exit_leg."""
        pos = self._position
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        order_ev = await self._close_leg(side, "vp_oi_regime_exit_reversal", now)
        if getattr(order_ev, "close_aborted", False):
            return
        setattr(pos, f"{side.lower()}_leg_closed", True)
        self._vp_oi_last_exited_strike[side] = int(leg.strike)
        self._persist()
        self._clog.info(
            "VP/OI REGIME — exited %s leg flat (strike=%.0f, price reversed through POC "
            "without LVN confirmation -- no replacement hedge)", side, leg.strike,
        )

    async def _vp_oi_exit_leg(self, side: str, hedge, now: datetime) -> None:
        """Close one SOLD leg (Highly Bearish/Bullish row) and, if the
        matrix row's hedge is enabled, buy a premium-matched replacement
        long -- tracked as a naked position via self._vp_oi_naked_leg."""
        pos = self._position
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        entry_premium = float(leg.entry_price or 0.0)
        exited_strike = int(leg.strike)
        order_ev = await self._close_leg(side, "vp_oi_regime_exit", now)
        if getattr(order_ev, "close_aborted", False):
            return
        setattr(pos, f"{side.lower()}_leg_closed", True)
        self._vp_oi_last_exited_strike[side] = exited_strike
        self._persist()
        self._clog.info("VP/OI REGIME — exited %s leg (strike=%.0f, regime-driven)",
                         side, leg.strike)

        if not (hedge.enabled and entry_premium > 0):
            return
        from strategies.vp_oi_regime.decision_matrix import hedge_target_premium, pick_strike_by_premium_match
        target = hedge_target_premium(hedge, entry_premium)
        if not target:
            return
        chain = {s: v.get("ltp", 0.0) for (s, sd), v in self._strike_prem.items() if sd == side}
        picked = pick_strike_by_premium_match(chain, target[0], target[1])
        if not picked:
            self._clog.info("VP/OI REGIME — no strike in [%.2f,%.2f] premium band for %s hedge; "
                             "leg stays fully closed, no naked long opened", target[0], target[1], side)
            return
        h_strike, h_ltp = picked
        h_fill = await self._dispatch_hedge_order(
            "BUY", side, int(h_strike), h_ltp, h_ltp, pos.expiry_date, "vp_oi_hedge")
        if h_fill is None or getattr(h_fill, "entry_aborted", False) or h_fill.fill_price <= 0:
            self._clog.critical("VP/OI REGIME — hedge %s BUY failed/unconfirmed -- leg left "
                                 "closed with NO naked long opened (needs review)", side)
            return
        self._vp_oi_naked_leg[side] = {"strike": int(h_strike), "entry": float(h_fill.fill_price)}
        self._vp_oi_adapter.mark_naked(
            "NAKED_BOTH" if len(self._vp_oi_naked_leg) == 2 else f"NAKED_{side}",
            {s: d["entry"] for s, d in self._vp_oi_naked_leg.items()},
        )
        self._vp_oi_adapter.add_remark(
            f"{side}: hedge bought {h_strike}@{h_fill.fill_price:.2f} -- naked long armed, "
            f"riding with 9-EMA trailing stop")
        self._clog.info("VP/OI REGIME — hedge %s%d BUY@%.2f confirmed (naked long armed, 9-EMA trailing)",
                         side, h_strike, h_fill.fill_price)

    async def _vp_oi_exit_both(self, dr, now: datetime) -> None:
        """Volatile regime: close the whole straddle, then buy both hedges.

        Called from _check_vp_oi_regime; naked-leg management (9-EMA stop +
        Hedge Exit Rule) for the resulting naked longs runs via
        _check_vp_oi_naked_legs_standalone, unconditionally, even with
        self._position == None after this closes the whole position."""
        pos = self._position
        ce_entry = float(pos.ce_leg.entry_price or 0.0)
        pe_entry = float(pos.pe_leg.entry_price or 0.0)
        self._vp_oi_last_exited_strike["CE"] = int(pos.ce_leg.strike)
        self._vp_oi_last_exited_strike["PE"] = int(pos.pe_leg.strike)
        await self._close_position("vp_oi_regime_exit_volatile")
        if self._position is not None:
            return  # close_position aborted (still "closing"/"open") -- don't hedge a live position
        for side, hedge, entry_premium in (("CE", dr.call_hedge, ce_entry), ("PE", dr.put_hedge, pe_entry)):
            if not (hedge.enabled and entry_premium > 0):
                continue
            from strategies.vp_oi_regime.decision_matrix import hedge_target_premium, pick_strike_by_premium_match
            target = hedge_target_premium(hedge, entry_premium)
            if not target:
                continue
            chain = {s: v.get("ltp", 0.0) for (s, sd), v in self._strike_prem.items() if sd == side}
            picked = pick_strike_by_premium_match(chain, target[0], target[1])
            if not picked:
                continue
            h_strike, h_ltp = picked
            expiry = getattr(self, "_entry_expiry_date", None)
            h_fill = await self._dispatch_hedge_order("BUY", side, int(h_strike), h_ltp, h_ltp, expiry, "vp_oi_hedge")
            if h_fill is None or getattr(h_fill, "entry_aborted", False) or h_fill.fill_price <= 0:
                self._clog.critical("VP/OI REGIME (Volatile) — hedge %s BUY failed/unconfirmed", side)
                continue
            self._vp_oi_naked_leg[side] = {"strike": int(h_strike), "entry": float(h_fill.fill_price)}
        if self._vp_oi_naked_leg:
            self._vp_oi_adapter.mark_naked(
                "NAKED_BOTH" if len(self._vp_oi_naked_leg) == 2 else
                f"NAKED_{next(iter(self._vp_oi_naked_leg))}",
                {s: d["entry"] for s, d in self._vp_oi_naked_leg.items()},
            )

    async def _vp_oi_manage_naked_legs(self, pos: "StraddlePosition", now: datetime) -> None:
        """Every tick while any side is naked: feed its live LTP into the
        9-EMA trailing stop; on a stop-hit, sell the naked long and arm the
        Re-entry Rule. Every tick while a side is merely awaiting re-entry
        (no naked long currently open on it), watch for the POC-crossing
        fakeout confirmation and re-sell a fresh short leg if it fires."""
        adapter = self._vp_oi_adapter
        spot = float(getattr(self, "_spot", 0.0) or 0.0)
        for side in ("CE", "PE"):
            leg_info = self._vp_oi_naked_leg.get(side)
            if leg_info is not None:
                ltp = float(self._strike_prem.get((leg_info["strike"], side), {}).get("ltp", 0.0) or 0.0)
                if ltp <= 0:
                    continue
                adapter.on_naked_leg_price(side, ltp)
                if adapter.stop_hit(side, ltp):
                    await self._vp_oi_close_naked(side, leg_info, now)
                    adapter.on_naked_stopped(side)
            elif adapter.awaiting_reentry.get(side):
                if spot > 0 and adapter.check_reentry(side, spot):
                    await self._vp_oi_resell_leg(side, now)

    async def _vp_oi_close_naked(self, side: str, leg_info: dict, now: datetime) -> None:
        """9-EMA stop fired on a naked long -- sell it (Hedge Exit Rule)."""
        ltp = float(self._strike_prem.get((leg_info["strike"], side), {}).get("ltp", 0.0) or 0.0)
        pos = self._position
        expiry = pos.expiry_date if pos else getattr(self, "_entry_expiry_date", None)
        fill = await self._dispatch_hedge_order(
            "SELL", side, leg_info["strike"], ltp, leg_info["entry"], expiry, "vp_oi_hedge_exit")
        self._vp_oi_naked_leg.pop(side, None)
        if fill is None or getattr(fill, "exit_aborted", False):
            self._clog.critical("VP/OI REGIME — naked long %s SELL (stop-exit) failed/unconfirmed",
                                 side)
            return
        pnl = float(fill.fill_price or 0.0) - leg_info["entry"]
        self._vp_oi_adapter.add_remark(
            f"{side}: 9-EMA stop hit on naked long {leg_info['strike']} @ {ltp:.2f} "
            f"(pnl={pnl:+.2f}pts) -- Re-entry Rule now watching for POC reclaim")
        self._clog.info("VP/OI REGIME — naked long %s%d stopped out @ %.2f pnl=%.2fpts; "
                         "Re-entry Rule now watching", side, leg_info["strike"], ltp, pnl)

    async def _vp_oi_resell_leg(self, side: str, now: datetime) -> None:
        """Re-entry Rule fired (price closed back on the fakeout side of
        POC) -- re-sell a fresh short leg on this side.

        NOTE: _single_side_roll is NOT reusable here -- it picks WHICH side
        to roll by comparing both legs' own P&L and assumes both are
        currently open, neither of which holds for a side that's already
        fully closed. Instead this reuses select_rollover_partner_directional
        the same way a roll does, balancing the new strike against the
        OTHER leg (if it's still open) as the reference "kept" leg, then
        places it via _open_leg (the same primitive every roll path uses)."""
        pos = self._position
        if pos is None:
            # Volatile-regime double exit: the whole StraddlePosition is gone,
            # so there's no "other leg" to balance a fresh short against --
            # re-opening would mean a full fresh BEGINNING-style entry
            # (strike selection, subscriptions, position creation), a
            # separate, larger change not built this pass. Logged clearly
            # rather than silently dropped.
            self._clog.info("VP/OI REGIME — Re-entry Rule fired for %s but no position exists "
                             "to resume (Volatile double-exit case); needs a fresh BEGINNING-"
                             "style entry path, not yet implemented -- staying flat", side)
            self._vp_oi_adapter.awaiting_reentry[side] = False
            return
        if getattr(pos, f"{side.lower()}_leg_closed", False) is False:
            return  # leg already re-sold by something else
        other_side = "PE" if side == "CE" else "CE"
        other_leg = pos.pe_leg if side == "CE" else pos.ce_leg
        if getattr(pos, f"{other_side.lower()}_leg_closed", False):
            # Both sides flat (Volatile-regime double re-entry) -- balancing
            # against a closed other leg has no meaning. Known gap: needs a
            # fresh BEGINNING-style pair selection, not built this pass.
            self._clog.info("VP/OI REGIME — Re-entry Rule fired for %s but the other side is "
                             "also flat (no reference leg); not re-selling -- needs a fresh "
                             "pair-selection path, not yet implemented", side)
            return
        from strategies.sell_straddle.selection import select_rollover_partner_directional
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        old_strike = self._vp_oi_last_exited_strike.get(side, other_leg.strike)
        picked = select_rollover_partner_directional(
            self._strike_prem, side, int(other_leg.strike), float(other_leg.ltp or 0.0),
            int(old_strike), float(getattr(self, "_spot", 0.0) or 0.0), step,
            min_gap_pts=100.0, rule_pass=True,
        )
        if not picked:
            self._clog.info("VP/OI REGIME — Re-entry Rule fired for %s but no valid re-sell "
                             "strike found; staying flat on this side", side)
            return
        strike, ltp = picked
        await self._open_leg(side, int(strike), float(ltp), now, "vp_oi_reentry_resell")
        self._vp_oi_adapter.add_remark(
            f"{side}: Re-entry Rule fired (fakeout confirmed) -- fresh short re-sold @ {int(strike)}")
        self._clog.info("VP/OI REGIME — Re-entry Rule fired, %s leg re-sold @ strike=%d", side, int(strike))

    async def _check_vp_oi_naked_legs_standalone(self, now: datetime) -> None:
        """Entry point called from _check_exits() UNCONDITIONALLY, before the
        'no open position' early-return -- see that call site's own comment.
        _vp_oi_manage_naked_legs itself never actually reads its `pos` arg
        (every lookup goes through self._vp_oi_naked_leg/self._strike_prem),
        so passing self._position (possibly None, e.g. right after a
        Volatile-regime full close) through unchanged is safe."""
        adapter = getattr(self, "_vp_oi_adapter", None)
        if not getattr(self, "_vp_oi_enabled", False) or adapter is None:
            return
        if not getattr(self, "_vp_oi_naked_leg", None) and not adapter.awaiting_reentry:
            return
        await self._vp_oi_manage_naked_legs(self._position, now)

    async def _abort_roll_reopen(self, fill) -> None:
        """A single-leg roll-reopen (_open_leg, called mid-roll after the old leg's
        close already confirmed) was rejected by the broker. The OTHER leg (the one
        being kept) is still genuinely open — close it for real too, matching this
        codebase's "0 or 2" rollover doctrine, rather than leaving it orphaned or
        (the 2026-08-06 bug this replaces) nulling self._position outright while a
        real leg is still live at the broker."""
        _legs = list(getattr(fill, "legs", []) or [])
        if not _legs:
            return
        roll_side = _legs[0]
        keep_side = "PE" if roll_side == "CE" else "CE"
        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        now = datetime.now(IST)
        logger.error(
            "SellStraddle[%s|%s|%s]: ROLL-REOPEN ABORTED on %s leg — the %s leg is still "
            "genuinely open at the broker (the roll's close of the old %s leg already "
            "confirmed). Closing %s for real to return to a clean flat state.",
            self._underlying, _cid, _bid, roll_side, keep_side, roll_side, keep_side,
        )
        close_ev = await self._close_leg(keep_side, "roll_reopen_aborted", now)
        if getattr(close_ev, "close_aborted", False):
            self._clog.critical(
                "ROLL-REOPEN ABORT CLEANUP: closing kept leg %s ALSO not confirmed -- "
                "position state is AMBIGUOUS (rolled leg rejected, kept leg's close "
                "unconfirmed). NOT clearing tracking blindly. RECONCILE MANUALLY against "
                "the real broker positions before trusting this book's state.",
                keep_side,
            )
            logger.critical(
                "SellStraddle[%s|%s|%s]: ROLL-REOPEN ABORT CLEANUP FAILED — kept leg %s "
                "close also unconfirmed. RECONCILE MANUALLY.",
                self._underlying, _cid, _bid, keep_side,
            )
            self._roll_in_progress = False
            self._order_pending = False
            return
        if self._position:
            self._position.status = "closed"
            self._position.close_reason = "roll_reopen_aborted"
            self._position.close_time = now
        self._position = None
        self._roll_in_progress = False
        self._order_pending = False
        self._persist()
        self._apply_sl_cooldown()

    async def _open_leg(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Open ONE leg at a new strike (publish ENTRY legs=[side]); update the leg."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        # A leg being (re-)opened is by definition no longer "closed" -- clears
        # ce_leg_closed/pe_leg_closed (set by e.g. the R1-breach-post-roll
        # single-leg-close path) so current_value/unrealized_pnl resume
        # counting this leg, and clears the matching R1-watch "closing in
        # progress" guard so a future re-arm on this same side isn't
        # permanently blocked by a stale flag from the leg this one replaced.
        setattr(pos, f"{side.lower()}_leg_closed", False)
        if hasattr(self, "_r1_closing") and isinstance(getattr(self, "_r1_closing", None), dict):
            self._r1_closing[side] = False
        leg.strike = strike
        leg.entry_price = ltp
        leg.ltp = ltp
        leg.open_time = now
        leg.open_reason = reason
        leg.close_time = None
        pos.net_credit = pos.ce_leg.entry_price + pos.pe_leg.entry_price
        pos.tsl_high_lock_rs = 0.0
        # 2026-09-23 CRITICAL FIX, real live incident: this used to also do
        # `pos.open_time = now` on every single-leg roll -- but pos.open_time
        # means "when this POSITION/cycle began" (set once, at genuine entry,
        # in entries.py), and dashboard_server.py's Booked-P&L filter uses it
        # as the cycle-start boundary, only counting trade_history rows with
        # ts >= pos.open_time (its own comment: "a same-cycle roll/re-entry's
        # own closed leg still has a close ts strictly after this position's
        # own open_time, so it correctly stays included" -- an invariant THIS
        # line broke). Resetting it here moved the cycle-start goalpost to the
        # roll's own timestamp, which is AFTER the close record this same roll
        # just wrote a moment earlier -- silently excluding the leg's own
        # just-booked P&L from Booked P&L display every single roll. Confirmed
        # live: a real rollover completed (PE leg closed+reopened, R1-watch
        # correctly armed on the new leg) yet Booked P&L showed +Rs0. Only
        # leg.open_time (used for per-leg display/tracking) should update on
        # a roll -- the position's own open_time is untouched here now.
        # 2026-09-06, direct user follow-up (indicator-freshness audit): a roll
        # used to leave two stale gaps the ENTRY path already avoided --
        # (1) self._ind's RSI/ROC/SLOPE/VWAP keys only get conditionally
        # overwritten by _recompute_indicators (indicators.py:58-60), so the
        # PREVIOUS strike's numbers lingered here and leaked into this exact
        # order event's `indicators=dict(self._ind)` snapshot below until the
        # new pair warmed up on its own; (2) RSI/ROC were never REST-warmed
        # for a rolled-into strike the way _seed_exec_legs already does for a
        # fresh ENTRY, so a roll target outside the startup pool ring could
        # sit RSI/ROC-blind for ~15 minutes. Clearing the stale keys makes a
        # not-yet-warm indicator read as genuinely missing (None, fail-closed
        # -- matches how pair_indicators() already behaves for a cold pair)
        # instead of quietly showing the strike we just rolled OUT of; the
        # REST warm-seed (idempotent -- warm_tf() no-ops if already warm)
        # closes that window as fast as entry does.
        for _k in ("rsi", "roc", "slope", "slope_prev", "vwap", "vwap_prev"):
            self._ind.pop(_k, None)
        try:
            await self._seed_exec_legs(int(pos.ce_leg.strike), int(pos.pe_leg.strike))
        except Exception as exc:
            logger.warning("SellStraddle[%s]: roll-seed exec legs failed (non-fatal): %s",
                            self._underlying, exc)
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, indicators=dict(self._ind),
            event_id=f"{self._underlying}_OPENLEG_{side}_{self._event_counter}",
            legs=[side],
            expiry=pos.expiry_date,
        )
        await self._emit_order(order_ev)
        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        logger.info("SellStraddle[%s|%s|%s]: OPEN LEG %s strike=%.0f @%.2f [%s]",
                    self._underlying, _cid, _bid, side, strike, ltp, reason)
        self._clog.info("OPEN LEG %s strike=%.0f @%.2f [%s]",
                        side, strike, ltp, reason)
