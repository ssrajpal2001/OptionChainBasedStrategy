"""
strategies/sell_straddle/r1_breach_reentry.py -- R1-breach exit / S1-breach
re-entry mechanic for rolled-in legs (2026-09-22, direct user spec).

Scope: ONLY legs opened via a single-side roll (leg.open_reason starts with
"single_side_roll") are ever watched by this mechanic -- never the original
"beginning"/"re_entry" legs. Real incident that prompted this: a rolled-in
leg with no protection kept bleeding alongside a never-rolled leg that also
had no protection; this gives the ROLLED-IN leg specifically an early,
structure-based exit instead of waiting on the generic exit ladder.

Mechanic (direct user spec, confirmed across several rounds of clarification):
1. TICK-BY-TICK: once a rolled-in leg's running P&L goes negative, arm a
   fresh SupportResistanceCalculator for that leg's own strike -- seeded
   (not blank) from REAL intraday 1-min history (09:15-to-now, REST-fetched)
   so R1/S1 reflect the strike's genuine structure since market open.
2. R1 BREACH (checked every tick against the seeded/updated state) = TRUE
   when EITHER: R1 is not yet established, OR the current phase is literally
   R1_TRACKING (a fresh "R2 breaches R1" transition always lands the phase
   there, so that case is covered by this same check). The instant true,
   close that leg alone (exit_reason="r1_breach_post_roll") -- the kept leg
   keeps running untouched/naked.
3. NO immediate replacement leg. Each cycle, re-derive the single best
   candidate strike via the SAME existing partner-selection rule
   (select_rollover_partner_directional) already used by every other roll,
   seed/track THAT ONE candidate's own SR the same way, and only enter it
   once its own S1 BREACH (mirror of #2, on S1/S1_TRACKING) fires AND it
   still independently passes the partner rules on that exact cycle
   (entry_reason="s1_breach_reentry_post_roll"). If the best candidate
   changes strike between cycles, drop the old tracker and seed fresh for
   the new one. Keeps re-checking every cycle until a pair is found -- no
   leg sits open on that side meanwhile.

SR calculator instances are intentionally NOT persisted across a restart
(same choice already made for _post1500_calc) -- they safely re-seed from
real REST history the next time this mechanic arms/tracks a candidate.
"""
from __future__ import annotations

import logging
from datetime import datetime

from config.global_config import IST

logger = logging.getLogger(__name__)

# Throttle for re-deriving/re-checking a waiting-for-S1-breach candidate --
# avoids hammering the partner-selection search every single tick.
_R1_CANDIDATE_RETRY_SECONDS = 15


def _level_breached(sr_state: dict, level_key: str, tracking_phase: str) -> bool:
    """Direct user spec: <level> breach is true when it is not established,
    OR the calculator is currently in that level's own TRACKING phase (a
    genuine re-breach transition -- e.g. "R2 breaches R1" -- always lands
    the phase there, so that case is already covered by this check)."""
    sr = sr_state.get("sr_levels", {}) or {}
    lvl = sr.get(level_key)
    established = bool(lvl.get("is_established")) if lvl else False
    return (not established) or (sr_state.get("current_phase") == tracking_phase)


class R1BreachReentryMixin:
    """Per-leg R1-breach exit + S1-breach re-entry, scoped to rolled-in legs."""

    def _r1_init_state(self) -> None:
        if not hasattr(self, "_r1_watch") or not isinstance(getattr(self, "_r1_watch", None), dict):
            # side -> {"calc": SupportResistanceCalculator, "inst_key": str,
            #          "bar_acc": {"minute":, "h":, "l":} | None, "strike": int}
            self._r1_watch = {}
        if not hasattr(self, "_r1_pending") or self._r1_pending is None:
            # None, or {"side":, "candidate_strike":, "calc":, "inst_key":,
            #           "bar_acc":, "last_check": datetime}
            self._r1_pending = None

    async def _seed_r1s1_calc(self, strike: int, side: str, inst_key: str):
        """REST-fetch today's 1-min history for (strike, side) up to now and
        replay it into a fresh SupportResistanceCalculator -- see this
        module's own docstring for why (real structure since 09:15, not a
        blank start)."""
        from strategies.core.support_resistance import SupportResistanceCalculator
        calc = SupportResistanceCalculator()
        try:
            if getattr(self, "_is_crypto", False):
                return calc
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            import asyncio as _aio
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return calc
            pos = self._position
            exp = pos.expiry_date if (pos and pos.expiry_date) else \
                REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(strike), side, "upstox")
            if not ikey:
                return calc
            bars = await fetch_upstox_intraday_1m(ikey, token)
            if not bars:
                return calc
            candles = [
                {"timestamp": datetime.fromisoformat(b["ts"]), "high": float(b["high"]),
                 "low": float(b["low"]), "duration": 1}
                for b in bars
            ]
            calc.reset_and_process_sequence(inst_key, candles)
            _st = calc.get_calculated_sr_state(inst_key)
            self._clog.info(
                "SellStraddle[%s]: R1/S1 SEEDED %s%d from %d real 1-min bars -- "
                "phase=%s r1_established=%s s1_established=%s",
                self._underlying, side, int(strike), len(candles),
                _st.get("current_phase"), _st.get("r1_established"), _st.get("s1_established"),
            )
        except Exception as exc:
            self._clog.warning("SellStraddle[%s]: R1/S1 seed failed for %s%d: %s",
                                self._underlying, side, int(strike), exc)
        return calc

    def _r1_feed_bar(self, entry: dict, ltp: float, now: datetime) -> None:
        """Feed a live tick into a 1-min bar accumulator; process a completed
        candle into the tracked SupportResistanceCalculator on bar close."""
        if ltp <= 0:
            return
        minute = now.replace(second=0, microsecond=0)
        acc = entry.get("bar_acc")
        if acc is None:
            entry["bar_acc"] = {"minute": minute, "h": ltp, "l": ltp}
        elif minute != acc["minute"]:
            entry["calc"].process_straddle_candle(
                entry["inst_key"],
                {"timestamp": acc["minute"], "high": acc["h"], "low": acc["l"], "duration": 1},
            )
            entry["bar_acc"] = {"minute": minute, "h": ltp, "l": ltp}
        else:
            acc["h"] = max(acc["h"], ltp)
            acc["l"] = min(acc["l"], ltp)

    async def _check_r1_breach_and_reentry(self, now: datetime) -> None:
        self._r1_init_state()
        pos = self._position
        if not pos or pos.status != "open":
            return

        # ── Part 1: watch rolled-in legs for an R1 breach ──────────────
        for side in ("CE", "PE"):
            leg_closed_attr = f"{side.lower()}_leg_closed"
            if getattr(pos, leg_closed_attr):
                self._r1_watch.pop(side, None)
                continue
            leg = pos.ce_leg if side == "CE" else pos.pe_leg
            open_reason = str(getattr(leg, "open_reason", "") or "")
            if not open_reason.startswith("single_side_roll"):
                # Not a rolled-in leg (beginning/re_entry/etc.) -- never watched.
                self._r1_watch.pop(side, None)
                continue
            strike = int(leg.strike)
            watch = self._r1_watch.get(side)
            if watch is not None and watch.get("strike") != strike:
                # This side rolled again onto a different strike since we last
                # armed -- drop stale tracker, a fresh arm will pick it up if
                # the new leg also goes into loss.
                self._r1_watch.pop(side, None)
                watch = None

            ltp = float(getattr(leg, "ltp", 0.0) or 0.0)
            running_pnl = float(getattr(leg, "entry_price", 0.0) or 0.0) - ltp  # short leg

            if watch is None:
                if running_pnl >= 0:
                    continue  # not yet in loss -- do not arm
                inst_key = f"{self._underlying}_{side}_{strike}_R1S1_ROLL"
                calc = await self._seed_r1s1_calc(strike, side, inst_key)
                self._r1_watch[side] = watch = {
                    "calc": calc, "inst_key": inst_key, "bar_acc": None, "strike": strike,
                }
                self._clog.info(
                    "SellStraddle[%s]: R1-WATCH ARMED on rolled-in %s%d (running loss=%.2f pts) "
                    "-- watching for R1 breach.",
                    self._underlying, side, strike, running_pnl,
                )

            self._r1_feed_bar(watch, ltp, now)
            sr_state = watch["calc"].get_calculated_sr_state(watch["inst_key"])
            if not _level_breached(sr_state, "R1", "R1_TRACKING"):
                continue

            r1 = (sr_state.get("sr_levels") or {}).get("R1") or {}
            # Stashed for _close_remark (exits.py) to build the real History
            # remark string from -- see the "r1_breach_post_roll" case there.
            self._r1_last_breach_info = {
                "strike": strike, "phase": sr_state.get("current_phase"),
                "r1_established": sr_state.get("r1_established"),
                "r1": float(r1.get("high", 0.0) or 0.0), "ltp": ltp,
            }
            self._clog.info(
                "SellStraddle[%s]: R1 BREACH on rolled-in %s%d -- phase=%s r1_established=%s "
                "R1=%.2f ltp=%.2f -> closing this leg, will scan for an S1-breach replacement.",
                self._underlying, side, strike, sr_state.get("current_phase"),
                sr_state.get("r1_established"), float(r1.get("high", 0.0) or 0.0), ltp,
            )
            close_ev = await self._close_leg(side, "r1_breach_post_roll", now)
            self._r1_watch.pop(side, None)
            if getattr(close_ev, "close_aborted", False):
                # Close not confirmed -- leave the leg as-is, try again next tick
                # (same retry discipline as every other exit path in this book).
                continue
            self._r1_pending = {
                "side": side, "candidate_strike": None, "calc": None,
                "inst_key": None, "bar_acc": None, "last_check": None,
                "_last_closed_strike": strike,
            }
            self._persist()
            pos = self._position
            if not pos or pos.status != "open":
                return

        # ── Part 2: watch for a candidate S1 breach to re-enter the empty side ──
        pending = self._r1_pending
        if not pending:
            return
        pos = self._position
        if not pos or pos.status != "open":
            return
        side = pending["side"]

        _last = pending.get("last_check")
        if _last and (now - _last).total_seconds() < _R1_CANDIDATE_RETRY_SECONDS:
            return
        pending["last_check"] = now

        from strategies.sell_straddle.selection import select_rollover_partner_directional
        from strategies.sell_straddle.rolling import _ROLLOVER_MIN_GAP_PTS, _ROLLOVER_STRIKE_STEP
        from data_layer.runtime_config import RuntimeConfig
        from strategies.core.rule_evaluator import eval_rules as _eval_rules

        keep_side = "PE" if side == "CE" else "CE"
        keep_leg = pos.pe_leg if keep_side == "PE" else pos.ce_leg
        keep_strike = int(keep_leg.strike)
        keep_ltp = float(getattr(keep_leg, "ltp", 0.0) or getattr(keep_leg, "entry_price", 0.0) or 0.0)

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        real_step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or 4)
        max_itm = int(ss.get("roll_max_itm_steps", 5))

        def _rule_pass(ce_s: int, pe_s: int):
            ind = self._ind_by_tf(ce_s, pe_s, rules)
            passed, reason = _eval_rules(rules, ind)
            return passed, reason, ind

        partner = select_rollover_partner_directional(
            self._strike_prem, roll_side=side, kept_strike=keep_strike, kept_ltp=keep_ltp,
            closing_strike=pending.get("_last_closed_strike") or keep_strike,
            spot=(self._atm_ref if self._atm_ref > 0 else self._spot),
            real_step=real_step, min_gap_pts=_ROLLOVER_MIN_GAP_PTS, rule_pass=_rule_pass,
            max_itm_steps=max_itm, max_search_steps=max(1, offset * 2),
            trace=[], itm_cap_step_pts=_ROLLOVER_STRIKE_STEP,
        )
        if not partner:
            self._clog.info(
                "SellStraddle[%s]: S1-BREACH RE-ENTRY WAIT (%s side empty) -- no partner "
                "currently passes re-entry rules; still watching.", self._underlying, side,
            )
            return

        new_strike, new_ltp = partner
        if pending.get("candidate_strike") != int(new_strike):
            inst_key = f"{self._underlying}_{side}_{int(new_strike)}_S1S1_CANDIDATE"
            calc = await self._seed_r1s1_calc(int(new_strike), side, inst_key)
            pending.update({
                "candidate_strike": int(new_strike), "calc": calc,
                "inst_key": inst_key, "bar_acc": None,
            })
            self._clog.info(
                "SellStraddle[%s]: S1-BREACH RE-ENTRY WAIT — tracking new candidate %s%d @%.2f "
                "for its own S1 breach.", self._underlying, side, int(new_strike), new_ltp,
            )

        self._r1_feed_bar(pending, new_ltp, now)
        sr_state = pending["calc"].get_calculated_sr_state(pending["inst_key"])
        if not _level_breached(sr_state, "S1", "S1_TRACKING"):
            return

        # Re-verify the candidate still passes the existing partner rules on
        # this exact cycle (price may have moved since the search above).
        passed, _reason, _ind = _rule_pass(
            int(new_strike) if side == "CE" else keep_strike,
            keep_strike if side == "CE" else int(new_strike),
        )
        if not passed:
            self._clog.info(
                "SellStraddle[%s]: S1-BREACH RE-ENTRY — candidate %s%d showed S1 breach but "
                "no longer passes partner rules (%s); keep watching.",
                self._underlying, side, int(new_strike), _reason,
            )
            return

        s1 = (sr_state.get("sr_levels") or {}).get("S1") or {}
        _remark = (
            f"S1 breach (post-roll re-entry) | phase={sr_state.get('current_phase')} "
            f"S1_established={sr_state.get('s1_established')} "
            f"S1={float(s1.get('low', 0.0) or 0.0):.2f} ltp={new_ltp:.2f} "
            f"-> re-entered {side}{int(new_strike)} against kept {keep_side}{keep_strike}"
        )
        self._clog.info("SellStraddle[%s]: %s", self._underlying, _remark)
        await self._open_leg(side, int(new_strike), float(new_ltp), now, "s1_breach_reentry_post_roll")
        self._r1_pending = None
        self._persist()
