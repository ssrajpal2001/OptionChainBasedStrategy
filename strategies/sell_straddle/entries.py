"""
strategies/sell_straddle/entries.py — entry evaluation + priming + open_position.

Contains the beginning/re-entry rule evaluation, balanced-pair selection, and the
optimistic position open that publishes the ENTRY StraddleOrderEvent.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Tuple

from config.global_config import IST, Topic
from data_layer.runtime_config import RuntimeConfig
from strategies.core.rule_evaluator import eval_rules as _eval_rules
from strategies.sell_straddle.audit import (
    audit_entry_eval,
    audit_entry_exec,
)
from strategies.sell_straddle.rolling import _ROLLOVER_STRIKE_STEP

logger = logging.getLogger(__name__)


class EntryMixin:
    """Entry-side logic for the sell-straddle book."""

    # ── Priming wait ──────────────────────────────────────────────────────────

    def _priming_wait_minutes(self, rules: List[dict]) -> int:
        """
        Mirrors old base.py _is_in_priming_wait():
          wait = max_rule_tf × 2   if any rule uses SLOPE / VWAP_SLOPE
               = max_rule_tf × 1   otherwise

        2026-08-20 fix: a rule stored in "advanced" (operand1/operand2) form used to be
        skipped by the has_slope check entirely, regardless of what it actually compares --
        so a SLOPE rule built via the advanced editor never got the extra ×2 wait a plain
        SLOPE rule gets, letting entries prime a full candle earlier than intended. Now an
        "advanced" rule counts as slope-based if either operand names a slope indicator.
        """
        if not rules:
            return 0
        tfs = [int(r.get("tf", 1)) for r in rules if r.get("tf")]
        max_tf = max(tfs) if tfs else 1
        slope_names = {"slope", "vwap_slope", "slope_curr", "slope_prev"}

        def _uses_slope(r: dict) -> bool:
            ind = (r.get("indicator") or "").lower()
            if ind == "advanced":
                o1 = (r.get("operand1") or "").lower()
                o2 = (r.get("operand2") or "").lower()
                return o1 in slope_names or o2 in slope_names
            return ind in slope_names

        has_slope = any(_uses_slope(r) for r in rules)
        return max_tf * (2 if has_slope else 1)

    def _is_primed(self, now: datetime, rules: List[dict]) -> bool:
        """True once priming_anchor + wait_minutes has passed.

        2026-08-20 fix: the anchor used to always be `_market_open_dt` (the fixed 09:15
        market-open constant), even when the strategy's own configured `entry_start` is
        later than that. That let the priming window count a candle that closed BEFORE
        the entry window even opened as part of the required warm-up -- e.g. entry_start
        09:16 still primed off 09:15+wait, one candle too early. The anchor is now
        whichever is later: real market open or this deployment's own entry_start.
        """
        if self._primed:
            return True
        if self._market_open_dt is None:
            # _market_open_dt is set by the first 1m CANDLE_CLOSE handled in
            # _on_candle(). On a fresh restart, the tick loop (which calls this via
            # _maybe_try_entry) can run before that first candle closes -- not
            # primed yet is the correct answer, not a crash (2026-08-05: this used
            # to raise TypeError: None + timedelta on every such restart).
            return False
        wait_min = self._priming_wait_minutes(rules)
        if wait_min == 0:
            self._primed = True
            return True
        entry_start_dt = self._market_open_dt.replace(
            hour=self._entry_start.hour, minute=self._entry_start.minute,
            second=0, microsecond=0,
        )
        anchor = max(self._market_open_dt, entry_start_dt)
        ready_at = anchor + timedelta(minutes=wait_min)
        if now >= ready_at:
            self._primed = True
            logger.info(
                "SellStraddle[%s]: priming complete — waited %d min (ready at %s)",
                self._underlying, wait_min, ready_at.strftime("%H:%M"),
            )
            return True
        remaining = max(0, int((ready_at - now).total_seconds() / 60.0 + 0.999))
        logger.info(
            "SellStraddle[%s]: priming — ~%d min remaining (ready %s; wait=%d min from your rules: "
            "max_tf=%d ×%d slope)",
            self._underlying, remaining, ready_at.strftime("%H:%M"), wait_min,
            max((int(r.get("tf", 1)) for r in rules if r.get("tf")), default=1),
            2 if wait_min > max((int(r.get("tf", 1)) for r in rules if r.get("tf")), default=1) else 1,
        )
        return False

    # ── Public helpers ────────────────────────────────────────────────────────

    def set_client_db(self, db) -> None:
        """Inject the shared ClientDB so entry can be gated on terminal+trade activation."""
        self._client_db = db

    def get_premium_series(self) -> list:
        """Timestamped 1-min combined-premium chart series."""
        return list(self._chart_series)

    def _any_active_terminal(self) -> bool:
        """True if at least one client has a binding with terminal_connected AND engine_active,
        deployed to sell_straddle for this underlying."""
        from strategies.core import can_trade
        db = self._client_db
        if db is None:
            return True
        try:
            if self._client_id and self._binding_id:
                return can_trade(
                    self._client_id, self._binding_id, db,
                    strategy_name="sell_straddle", underlying=self._underlying,
                )
            active = False
            for _client in db.get_all_clients_sync():
                _cid = _client.get("client_id", "")
                if not _cid:
                    continue
                _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(_cid)}
                for _dep in db.get_deployments_sync(_cid):
                    _sn = str(_dep.get("strategy_name", "")).lower()
                    _ul = str(_dep.get("underlying", "") or _dep.get("assigned_instrument", "")).upper()
                    if _sn == "sell_straddle" and _ul == self._underlying.upper():
                        _b = _binds.get(_dep.get("binding_id"))
                        if _b and _b.get("engine_active") and _b.get("terminal_connected"):
                            active = True
                            break
                if active:
                    break
            return active
        except Exception as _exc:
            logger.debug("SellStraddle[%s]: terminal-active check error: %s", self._underlying, _exc)
            return False

    def _granular_audit_clients(self) -> list:
        """Return [(client_id, binding_id), …] for granular audit."""
        db = self._client_db
        if db is None:
            return []
        import time as _t
        _now = _t.monotonic()
        if _now - getattr(self, "_gran_check_t", 0.0) < 5.0:
            return getattr(self, "_gran_cached", [])
        self._gran_check_t = _now
        out: list = []
        try:
            for _client in db.get_all_clients_sync():
                _cid = _client.get("client_id", "")
                if not _cid:
                    continue
                _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(_cid)}
                for _dep in db.get_deployments_sync(_cid):
                    _sn = str(_dep.get("strategy_name", "")).lower()
                    _ul = str(_dep.get("underlying", "") or _dep.get("assigned_instrument", "")).upper()
                    if _sn == "sell_straddle" and _ul == self._underlying.upper():
                        _b = _binds.get(_dep.get("binding_id"))
                        if _b and _b.get("show_granular_ticks"):
                            out.append((_cid, _b.get("binding_id")))
        except Exception as _exc:
            logger.debug("SellStraddle[%s]: granular-audit check error: %s", self._underlying, _exc)
            out = []
        self._gran_cached = out
        return out

    @staticmethod
    def _at_tf_boundary(minute: int, second: int, max_tf: int) -> bool:
        return minute % max_tf == 0 and second >= 5

    # ── Entry dispatch ────────────────────────────────────────────────────────

    async def _maybe_try_entry(self, now: datetime) -> None:
        # Defense-in-depth: engine.py's tick-loop dispatch already routes any existing
        # self._position (open OR closing) to _check_exits, never here -- but guard
        # defensively against ANY non-empty position, not just "open", so a future/other
        # call site can never dispatch a fresh entry while a close is still in flight.
        if self._position and self._position.status != "closed":
            return
        # 2026-08-24 user spec: a pending T-1 hedge roll (old legs already
        # closed for real, waiting on next week's ATM strikes to have live
        # data) takes priority over normal entry-rule evaluation -- it's not
        # a rule-conditioned entry, it opens as soon as data is available.
        if getattr(self, "_hedge_roll_pending", False):
            await self._try_complete_hedge_roll(now)
            return
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        workflow = ss.get("entry_workflow_mode", "hybrid")
        is_beginning = (self._trades_today == 0)

        # User-specified hybrid contract (2026-08-05): BEGINNING is retried on every
        # eligible cycle for as long as trades_today == 0 -- a single blocked check
        # must NOT permanently lock it out for the rest of the day (the old
        # _beginning_failed flip did exactly that after just one failure). RE-ENTRY
        # must never be evaluated at all until the first trade has actually happened
        # (trades_today > 0) -- previously want_re was unconditionally True in hybrid
        # mode, so re-entry ran in parallel with beginning from tick one, even before
        # any trade existed. A mid-day restart with trades_today==0 still correctly
        # goes through is_beginning (state-based, not time-of-day-based) -- no change
        # needed there.
        want_beg = (workflow == "beginning_only") or (workflow == "hybrid" and is_beginning)
        want_re = (workflow == "reentry_only") or (workflow == "hybrid" and not is_beginning)

        due_beg = False
        if want_beg:
            rb = ss.get("entry_rules_beginning", [])
            mtf = max((int(r.get("tf", 1)) for r in rb), default=1)
            if self._at_tf_boundary(now.minute, now.second, mtf):
                bkt = f"{now:%Y%m%d_%H}{(now.minute // mtf) * mtf:02d}"
                if bkt != self._last_entry_bucket_b:
                    self._last_entry_bucket_b = bkt
                    due_beg = True
        due_re = False
        if want_re:
            rr = ss.get("entry_rules_reentry", [])
            mtf = max((int(r.get("tf", 1)) for r in rr), default=1)
            if self._at_tf_boundary(now.minute, now.second, mtf):
                bkt = f"{now:%Y%m%d_%H}{(now.minute // mtf) * mtf:02d}"
                if bkt != self._last_entry_bucket_r:
                    self._last_entry_bucket_r = bkt
                    due_re = True

        if due_beg or due_re:
            await self._try_entry(now, due_beg, due_re)

    async def _try_entry(self, now: datetime, due_beginning: bool = True,
                         due_reentry: bool = True) -> None:
        if self._stop_for_day:
            return
        # 2026-08-06 HIGH-priority fix: _try_entry (driven by INDEX_TICK, its own
        # async loop) and reset_session() (driven by CANDLE_CLOSE, a SEPARATE
        # async loop) are unsynchronized. Index ticks can start flowing and this
        # function can run before the first candle of a new day has closed and
        # triggered reset_session() -- in that window, self._primed/_trades_today/
        # _entry_expiry_date/_strike_prem are all still YESTERDAY's values. For
        # NIFTY that mainly risks the wrong entry-rule-set being used for the
        # day's real first trade (is_beginning miscomputed); for any deployment
        # using a 1-minute rule the window is a solid ~55s, not a rare fluke.
        # Defer entirely until reset_session() has actually run for today's
        # session -- correctness over a few seconds of extra latency at the
        # literal start of the trading day.
        if (self._market_open_dt is not None
                and self._session_day(self._market_open_dt) != self._session_day(now)):
            return
        if not self._any_active_terminal():
            import time as _t
            if _t.monotonic() - getattr(self, "_no_term_log", 0.0) > 60.0:
                self._no_term_log = _t.monotonic()
                logger.info("SellStraddle[%s]: WAITING — no terminal+trade active "
                            "(feeder running; entry starts when a client turns Terminal ON + Trade ON).",
                            self._underlying)
            return
        if not self._is_in_entry_window(now):
            import time as _t
            if _t.monotonic() - getattr(self, "_sleep_log", 0.0) > 60.0:
                self._sleep_log = _t.monotonic()
                window = (f"{self._entry_start.strftime('%H:%M')}"
                          f"-{self._entry_cutoff.strftime('%H:%M')}")
                logger.info("SellStraddle[%s]: SLEEP — outside entry window %s "
                            "(no new entries until window reopens).",
                            self._underlying, window)
            return
        if self._trades_today >= self._max_trades:
            return
        if self._sl_cooldown_until and now < self._sl_cooldown_until:
            return
        if self._order_pending:
            return
        if self._spot <= 0 or self._ce_ltp <= 0 or self._pe_ltp <= 0:
            _step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
            _atm = int(round(self._spot / _step) * _step) if self._spot > 0 else 0
            self._clog.info(
                "WAIT  spot=%.2f ATM=%d CE%d_ltp=%.2f PE%d_ltp=%.2f — waiting for option ticks",
                self._spot, _atm, _atm, self._ce_ltp, _atm, self._pe_ltp,
            )
            return

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        workflow_mode = ss.get("entry_workflow_mode", "hybrid")
        is_beginning = (self._trades_today == 0)
        if workflow_mode == "beginning_only":
            if due_beginning:
                await self._eval_ruleset(now, "entry_rules_beginning", use_beginning_sel=True)
            return
        if workflow_mode == "reentry_only":
            if due_reentry:
                await self._eval_ruleset(now, "entry_rules_reentry", use_beginning_sel=False)
            return
        if is_beginning and due_beginning:
            await self._eval_ruleset(now, "entry_rules_beginning", use_beginning_sel=True)
            if self._position and self._position.status == "open":
                return
        if due_reentry:
            await self._eval_ruleset(now, "entry_rules_reentry", use_beginning_sel=False)

    async def _maybe_shift_expiry_for_low_anchor_ltp(self, ltp_target: float, theta_target: float,
                                                       use_beginning_sel: bool = False) -> bool:
        """2026-08-23, direct user spec: "if ltp is less than threshold then
        jump to next week expiry -- applicable for anchor selection part...
        if we have entered next expiry, that expiry will be used for the
        complete trading day till EOD." The threshold is the SAME
        ltp_target/theta_target the anchor floor check already rejects a
        pair on -- not a new one.

        2026-08-24 CRITICAL fix, confirmed live: this call to anchor_fails_
        floor() never passed anchor_otm_steps/step, so it silently defaulted
        to 0/0.0 -- meaning it always re-checked the RAW ATM strike's LTP,
        never the actual 1-OTM-shifted strike select_balanced_pair_at (the
        REAL entry-selection function, called with anchor_otm_steps=1 for
        BEGINNING -- see _select_beginning_pair below) would genuinely trade
        as the anchor. A pair could pass this gate at raw ATM's LTP while
        the real 1-OTM anchor leg that actually gets sold was already below
        the floor, with the next-week-expiry-shift safety net never firing
        for exactly the case it exists to catch. RE-ENTRY still correctly
        uses anchor_otm_steps=0 (it never shifts the anchor at all -- see
        the anchor_otm_steps=1 comment in _select_beginning_pair), so
        use_beginning_sel selects the right value here, matching whichever
        selection path the caller is actually about to run.

        Returns True the one cycle a shift actually happens -- the caller
        should skip its own selection attempt that cycle, since the new
        expiry's strikes have no live premium data yet (self._strike_prem
        is cleared below; _option_loop's own _entry_exp_ok filter already
        refuses to accept ticks for any expiry other than
        self._entry_expiry_date, so stale current-week prices could
        otherwise linger under the same (strike, side) keys and be
        misread as next-week's real prices -- same strike NUMBERS exist
        on both weekly contracts, just at different real premiums)."""
        if self._is_crypto or self._expiry_shifted_low_anchor_ltp:
            return False
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm = int(round(self._spot / step) * step) if self._spot > 0 and step > 0 else 0
        if atm <= 0:
            return False
        from strategies.sell_straddle.selection import anchor_fails_floor
        anchor_otm_steps = 1 if use_beginning_sel else 0
        if not anchor_fails_floor(self._strike_prem, atm, self._spot, ltp_target, theta_target,
                                   anchor_otm_steps=anchor_otm_steps, step=step):
            return False

        from data_layer.instrument_registry import REGISTRY
        today = datetime.now(IST).date()
        current = REGISTRY.get_active_expiry(self._underlying, today)
        # Only meaningful while still genuinely on the current week's expiry --
        # if some other reason (e.g. the existing expiry-day shift) already
        # moved us off it, there's nothing further for this trigger to do.
        if not current or self._entry_expiry_date != current:
            return False
        next_exps = [e for e in REGISTRY.all_expiries(self._underlying) if e > current]
        if not next_exps:
            return False
        next_expiry = next_exps[0]

        logger.info(
            "SellStraddle[%s]: anchor LTP below floor (ltp≥%.0f theta≥%.0f) at ATM=%d on current "
            "expiry %s -- shifting to next expiry %s for the REST OF TODAY (user spec: sticky "
            "once shifted).",
            self._underlying, ltp_target, theta_target, atm, current.isoformat(), next_expiry.isoformat(),
        )
        self._clog.info(
            "EXPIRY-SHIFT low anchor LTP @ATM=%d on %s -> %s (sticky for today)",
            atm, current.isoformat(), next_expiry.isoformat(),
        )
        self._entry_expiry_date = next_expiry
        self._expiry_shifted_low_anchor_ltp = True
        self._strike_prem.clear()
        await self._subscribe_expiry_window(next_expiry)
        return True

    async def _eval_ruleset(self, now: datetime, rule_key: str, use_beginning_sel: bool) -> None:
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get(rule_key, [])
        concept = "beginning" if use_beginning_sel else "reentry"

        if not self._is_primed(now, rules):
            self._clog.info(
                "EVAL %s [%s] PRIMING — waiting for indicator priming", self._underlying, rule_key,
            )
            return

        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        theta_target = self._theta_target
        if await self._maybe_shift_expiry_for_low_anchor_ltp(ltp_target, theta_target, use_beginning_sel):
            return
        variable_strikes = bool(ss.get("variable_strikes", False))
        balance_ratio = float(ss.get("balance_ratio", getattr(self, "_balance_ratio", 1.0)))

        if (self._position is None or self._position.status != "open"):
            _audit_clients = self._granular_audit_clients()
            if _audit_clients:
                try:
                    _atm = round(self._spot / step) * step if self._spot else 0
                    _crit_h = [
                        {"name": "Status", "detail": f"no open position — {concept} scan", "hit": False},
                        {"name": "Spot/ATM", "detail": f"{self._spot:.2f} / {int(_atm)}", "hit": False},
                        {"name": "Target/Offset", "detail": f"ltp≥{ltp_target:.0f} theta≥{theta_target:.0f}, ±{offset}", "hit": False},
                    ]
                    for _cid, _bid in _audit_clients:
                        await self._bus.publish(Topic.EXIT_AUDIT, {
                            "type": "exit_audit", "client_id": _cid, "binding_id": _bid,
                            "underlying": self._underlying, "pnl": 0.0, "credit": 0.0,
                            "criteria": _crit_h, "ind_by_tf": {}, "ts": now.timestamp(),
                        })
                except Exception:
                    pass

        # BEGINNING entry (2026-08-05): near/far dual-anchor selection, not single-ATM.
        # Instead of rounding spot to one nearest strike, evaluate the two strikes that
        # actually bracket spot as two independent candidates -- see
        # _eval_beginning_near_far for the full mechanic. RE-ENTRY is unchanged below.
        if use_beginning_sel:
            await self._eval_beginning_near_far(
                now, rule_key, rules, step, offset, ltp_target, theta_target,
                variable_strikes, balance_ratio,
            )
            return

        from strategies.sell_straddle.selection import select_balanced_pair, reentry_block_reason

        # 2026-08-20 user spec: RE-ENTRY (like rollover) rounds ATM and enumerates
        # candidate strikes on a 100pt grid, NOT the real 50pt NIFTY grid `step` above
        # (which stays 50 and is only used for BEGINNING).
        reentry_step = _ROLLOVER_STRIKE_STEP

        _trace: list = []
        # Re-entry now uses the same balanced-pair logic as beginning: anchor at the
        # ATM side with lower TIME VALUE, partner raw LTP must be <= anchor time value
        # and pass the dual floor.  This prevents the old scan_pool behaviour that picked
        # the globally most-balanced LTP pair, often deep ITM on both sides (e.g.
        # CE6500/PE7300 when ATM was 6900).  The re-entry rules are evaluated AFTER the
        # pair is selected, not during selection (same as beginning).
        sel = select_balanced_pair(
            self._strike_prem, self._spot, reentry_step, offset, ltp_target, trace=_trace,
            entry_basis=self._entry_basis, theta_target=self._theta_target,
            variable_strikes=variable_strikes, balance_ratio=balance_ratio,
        )

        for _ln in _trace:
            self._clog.info("SELECT %s | %s", self._underlying, _ln)

        if not sel:
            if use_beginning_sel:
                self._clog.info(
                    "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair (ltp≥%.0f theta≥%.0f offset=%d)",
                    self._underlying, rule_key, self._spot, ltp_target, theta_target, offset,
                )
            else:
                diag = reentry_block_reason(
                    self._strike_prem, self._spot, reentry_step, offset, ltp_target,
                    rule_eval=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules)),
                    theta_target=self._theta_target,
                    variable_strikes=variable_strikes,
                    balance_ratio=balance_ratio,
                )
                if diag["kind"] == "no_pair":
                    self._clog.info(
                        "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair exists (ltp≥%.0f theta≥%.0f offset=%d)",
                        self._underlying, rule_key, self._spot, ltp_target, theta_target, offset,
                        )
                else:
                    self._clog.info(
                        "EVAL %s [%s] BLOCK — best pair CE%d=%.2f PE%d=%.2f credit=%.2f | %s "
                        "(pairs exist but none passed the re-entry gate)",
                        self._underlying, rule_key, diag["ce"], diag["ce_ltp"],
                        diag["pe"], diag["pe_ltp"], diag["ce_ltp"] + diag["pe_ltp"], diag["reason"],
                    )
            return
        ce_strike, pe_strike, ce_ltp, pe_ltp = sel
        ind_by_tf = self._ind_by_tf(ce_strike, pe_strike, rules)
        passed, reason = _eval_rules(rules, ind_by_tf)
        await self._finalize_entry_decision(
            now, rule_key, concept, ce_strike, pe_strike, ce_ltp, pe_ltp,
            ind_by_tf, passed, reason, ltp_target, theta_target, offset,
        )

    async def _eval_beginning_near_far(
        self, now: datetime, rule_key: str, rules: list, step: float, offset: int,
        ltp_target: float, theta_target: float, variable_strikes: bool, balance_ratio: float,
    ) -> None:
        """BEGINNING entry (2026-08-05, user-specified): instead of rounding spot to one
        nearest strike, evaluate the two strikes that actually bracket spot --
        near = floor(spot/step)*step, far = near+step -- as two independent anchor
        candidates, each via the same anchor+partner balanced-pair search RE-ENTRY
        uses (select_balanced_pair_at). Entry criteria (SLOPE etc.) is checked on
        BOTH resulting pairs. If both pass, the max/min-premium ratio (the same
        ratio concept used as the _max_entry_ratio safety gate right before entry)
        decides between them -- lower ratio (more balanced) wins. If only one
        passes, take it directly. If neither passes, no trade this cycle -- exactly
        like today, BEGINNING keeps retrying every eligible cycle regardless."""
        from strategies.sell_straddle.selection import select_balanced_pair_at

        near = int(self._spot // step) * int(step) if self._spot > 0 and step > 0 else 0
        far = near + int(step)

        candidates: list = []
        for label, atm in (("near", near), ("far", far)):
            _trace: list = []
            # 2026-08-20 user spec: anchor SIDE decision stays at raw ATM, but the
            # anchor's own strike used for pairing shifts 1 step further OTM (BEGINNING
            # only -- RE-ENTRY keeps anchor_otm_steps=0/unshifted).
            sel = select_balanced_pair_at(
                self._strike_prem, atm, self._spot, step, offset, ltp_target, trace=_trace,
                entry_basis=self._entry_basis, theta_target=theta_target,
                variable_strikes=variable_strikes, balance_ratio=balance_ratio,
                anchor_otm_steps=1,
            )
            for _ln in _trace:
                self._clog.info("SELECT %s | [%s@%d] %s", self._underlying, label, atm, _ln)
            if not sel:
                self._clog.info(
                    "EVAL %s [%s] NO-PAIR @ %s(%d) — spot=%.2f (ltp≥%.0f theta≥%.0f offset=%d)",
                    self._underlying, rule_key, label, atm, self._spot, ltp_target, theta_target, offset,
                )
                continue
            ce_strike, pe_strike, ce_ltp, pe_ltp = sel
            ind_by_tf = self._ind_by_tf(ce_strike, pe_strike, rules)
            passed, reason = _eval_rules(rules, ind_by_tf)
            self._clog.info(
                "EVAL %s [%s/beginning] %s(%d) sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s",
                self._underlying, rule_key, label, atm, ce_strike, ce_ltp, pe_strike, pe_ltp,
                ce_ltp + pe_ltp, reason, "PASS" if passed else "BLOCK",
            )
            candidates.append({
                "label": label, "ce_strike": ce_strike, "pe_strike": pe_strike,
                "ce_ltp": ce_ltp, "pe_ltp": pe_ltp, "ind_by_tf": ind_by_tf,
                "passed": passed, "reason": reason,
            })

        if not candidates:
            return  # both NO-PAIR, already logged above

        passing = [c for c in candidates if c["passed"]]
        if not passing:
            return  # both evaluated, neither passed entry criteria -- already logged above

        def _ratio(c: dict) -> float:
            hi, lo = max(c["ce_ltp"], c["pe_ltp"]), min(c["ce_ltp"], c["pe_ltp"])
            return (hi / lo) if lo > 0 else float("inf")

        if len(passing) == 1:
            chosen = passing[0]
        else:
            chosen = min(passing, key=_ratio)
            self._clog.info(
                "EVAL %s [%s] BOTH near/far pairs passed entry criteria — "
                "%s=CE%d/PE%d(ratio=%.3fx) vs %s=CE%d/PE%d(ratio=%.3fx) -> choosing %s (lower ratio)",
                self._underlying, rule_key,
                passing[0]["label"], passing[0]["ce_strike"], passing[0]["pe_strike"], _ratio(passing[0]),
                passing[1]["label"], passing[1]["ce_strike"], passing[1]["pe_strike"], _ratio(passing[1]),
                chosen["label"],
            )

        await self._finalize_entry_decision(
            now, rule_key, "beginning", chosen["ce_strike"], chosen["pe_strike"],
            chosen["ce_ltp"], chosen["pe_ltp"], chosen["ind_by_tf"], chosen["passed"], chosen["reason"],
            ltp_target, theta_target, offset,
        )

    async def _finalize_entry_decision(
        self, now: datetime, rule_key: str, concept: str,
        ce_strike: int, pe_strike: int, ce_ltp: float, pe_ltp: float,
        ind_by_tf: dict, passed: bool, reason: str,
        ltp_target: float, theta_target: float, offset: int,
    ) -> None:
        """Shared tail for both entry paths once a single candidate pair has been
        selected and rule-evaluated: log, audit, the _max_entry_ratio safety gate,
        and (if everything passes) dispatch _open_position. Extracted 2026-08-05 so
        BEGINNING's near/far dual-anchor path and RE-ENTRY's single-ATM path share
        the exact same finalize logic rather than risking behavior drift between
        two copies."""
        _dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()} for tf, d in ind_by_tf.items()}
        self._clog.info(
            "EVAL %s [%s/%s] sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s | ind_by_tf=%s",
            self._underlying, rule_key, concept, ce_strike, ce_ltp, pe_strike, pe_ltp,
            ce_ltp + pe_ltp, reason, "PASS" if passed else "BLOCK", _dump,
        )
        audit_entry_eval(
            client_id=getattr(self, "_client_id", "") or "",
            binding_id=getattr(self, "_binding_id", "") or "",
            underlying=self._underlying,
            ts=now,
            rule_key=rule_key,
            concept=concept,
            spot=self._spot,
            ltp_target=ltp_target,
            theta_target=theta_target,
            offset=offset,
            selected_pair=(int(ce_strike), int(pe_strike), float(ce_ltp), float(pe_ltp)),
            ind_by_tf=_dump,
            passed=passed,
            reason=reason,
        )
        if not passed:
            return

        if self._max_entry_ratio > 0 and ce_ltp > 0 and pe_ltp > 0:
            _entry_ratio = max(ce_ltp, pe_ltp) / min(ce_ltp, pe_ltp)
            if _entry_ratio > self._max_entry_ratio:
                self._clog.info(
                    "EVAL %s [%s] ENTRY-BLOCKED ratio=%.2fx > max_entry_ratio=%.2fx — pair CE%d/PE%d skewed, skipping",
                    self._underlying, rule_key, _entry_ratio, self._max_entry_ratio, ce_strike, pe_strike,
                )
                return

        self._clog.info(
            "ENTRY attempting — CE%d=%.2f PE%d=%.2f credit=%.2f rules_passed",
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
        )
        if not self._entry_expiry_date:
            # Lazy fallback: registry may have been loaded after _seed_pool ran (e.g. new client
            # deployed on a system using shared feed that skipped the warm seed early).
            self._entry_expiry_date = self._effective_entry_expiry()
        if not self._entry_expiry_date:
            self._clog.warning("ENTRY abort — no effective entry expiry resolved yet")
            return
        await self._open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason,
                                  expiry_date=self._entry_expiry_date)

    async def _open_position(
        self, now: datetime, ce_strike: int, pe_strike: int,
        ce_ltp: float, pe_ltp: float, rule_key: str, reason: str,
        expiry_date=None,
    ) -> None:
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition
        from strategies.theta_calc import combined_time_value as _ctv

        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm = round(self._spot / step) * step

        self._event_counter += 1
        event_id = f"{self._underlying}_ENTRY_{self._event_counter}"

        _open_reason = "beginning" if rule_key == "entry_rules_beginning" else "reentry"

        self._position = StraddlePosition(
            underlying=self._underlying,
            atm_at_entry=atm,
            entry_spot=self._spot,
            ce_leg=StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=now, open_reason=_open_reason),
            pe_leg=StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=now, open_reason=_open_reason),
            net_credit=ce_ltp + pe_ltp,
            open_time=now,
            status="open",
            session_min_vwap=float("inf"),
            entry_indicators=self._pair_indicators(ce_strike, pe_strike) or dict(self._ind),
            lot_size=self._lot_size * self._lot_multiplier,
            expiry_date=expiry_date,
        )
        self._position.entry_time_value = _ctv(ce_strike, pe_strike, self._spot, ce_ltp, pe_ltp)

        # 2026-08-20 user spec: a standing hedge from a prior EOD hedge-and-carry
        # outlives the sold pair it was built against -- carry it onto this fresh
        # position. The same-strike-collision guard (in _check_exits) catches the
        # case where this fresh pair happens to land on the hedge's own strike on
        # the very next tick.
        if self._pending_hedge_ce_leg is not None or self._pending_hedge_pe_leg is not None:
            self._position.hedge_ce_leg = self._pending_hedge_ce_leg
            self._position.hedge_pe_leg = self._pending_hedge_pe_leg
            self._position.is_hedged_positional = True
            self._clog.info(
                "HEDGE — carried standing hedge (CE%s / PE%s) onto fresh pair CE%d/PE%d",
                int(self._pending_hedge_ce_leg.strike) if self._pending_hedge_ce_leg else "-",
                int(self._pending_hedge_pe_leg.strike) if self._pending_hedge_pe_leg else "-",
                ce_strike, pe_strike,
            )
            self._pending_hedge_ce_leg = None
            self._pending_hedge_pe_leg = None

        self._pin_position_legs(self._position)
        self._persist()
        asyncio.create_task(self._seed_exec_legs(int(ce_strike), int(pe_strike)))
        self._trades_today += 1
        self._order_pending = True
        # Accumulate total premium/credit deployed today so day-level % guardrails
        # use the correct denominator across multiple trades/re-entries.
        _trade_credit = ce_ltp + pe_ltp
        self._initial_net_credit += _trade_credit
        if self._position:
            _new_etv = float(getattr(self._position, "entry_time_value", 0.0) or 0.0) or (ce_ltp + pe_ltp)
            if _new_etv > self._initial_entry_time_value:
                self._initial_entry_time_value = _new_etv

        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        logger.info(
            "SellStraddle[%s|%s|%s]: ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f | %s=PASS [%s]",
            self._underlying, _cid, _bid, ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            rule_key, reason,
        )
        self._clog.info(
            "ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f | %s=PASS [%s]",
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            rule_key, reason,
        )
        audit_entry_exec(
            client_id=_cid,
            binding_id=_bid,
            underlying=self._underlying,
            ts=now,
            ce_strike=float(ce_strike),
            pe_strike=float(pe_strike),
            ce_ltp=float(ce_ltp),
            pe_ltp=float(pe_ltp),
            credit=float(ce_ltp + pe_ltp),
            expiry_date=expiry_date.isoformat() if expiry_date else None,
            rule_key=rule_key,
            reason=reason,
        )

        order_ev = StraddleOrderEvent(
            action="ENTRY",
            underlying=self._underlying,
            atm=atm,
            ce_strike=ce_strike,
            pe_strike=pe_strike,
            ce_ltp=ce_ltp,
            pe_ltp=pe_ltp,
            lot_multiplier=self._lot_multiplier,
            lot_size=self._lot_size,
            spot=self._spot,
            indicators=dict(self._ind),
            event_id=event_id,
            expiry=expiry_date,
        )
        await self._emit_order(order_ev)

    _MANUAL_CONFIRM_MAX_STALENESS = timedelta(hours=2)

    async def manual_confirm_entry(self, ce_ltp: float, pe_ltp: float) -> Tuple[bool, str]:
        """2026-08-23, direct user spec: "entry price should come from the
        broker which is connected to the client. If broker doesn't send the
        data we can manually enter the price in UI and click save and then
        application will move depending on the price which is entered...
        client can update entry rate of both legs and save and that value
        changes inside the app for that client and exit condition will be
        checked accordingly."

        Fires when a full 2-leg ENTRY's broker fill confirmation was
        aborted/timed out -- _on_fill's existing ENTRY-abort branch already
        discards the optimistic position (unchanged, still the automatic
        default), but now ALSO retains the strikes/expiry it had already
        decided on in self._last_aborted_entry. The client can check their
        OWN broker terminal, see the trade genuinely went through, and
        supply the real fill price(s) here -- this recreates the position
        from the RETAINED strikes/expiry + the manually-supplied prices and
        resumes normal exit monitoring on it. Deliberately does NOT dispatch
        a new broker order (StraddleOrderEvent/_emit_order) -- the real
        trade already happened at the broker; this only informs the app of
        its outcome, mirroring the "detection + human confirms, never
        auto-remediate" philosophy already established for broker-position
        reconciliation. Bypasses self._stop_for_day on purpose: a manual
        confirm isn't a NEW entry attempt, it's confirming one that already
        happened, even if repeated automatic attempts around it failed and
        tripped that guard."""
        if self._position is not None and self._position.status != "closed":
            return False, "book already has an open/closing position -- cannot manually confirm on top of it"
        pending = self._last_aborted_entry
        if pending is None:
            return False, "no aborted entry is currently pending manual confirmation"
        if ce_ltp <= 0 or pe_ltp <= 0:
            return False, "both CE and PE entry prices must be greater than zero"
        age = datetime.now(IST) - pending["aborted_at"]
        if age > self._MANUAL_CONFIRM_MAX_STALENESS:
            self._last_aborted_entry = None
            return False, (
                f"the pending aborted entry is {age} old (more than "
                f"{self._MANUAL_CONFIRM_MAX_STALENESS} old) -- too stale to confirm safely, discarded"
            )

        from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition
        from strategies.theta_calc import combined_time_value as _ctv

        now = datetime.now(IST)
        ce_strike, pe_strike = pending["ce_strike"], pending["pe_strike"]
        expiry_date = pending["expiry_date"]

        self._event_counter += 1
        event_id = f"{self._underlying}_MANUALENTRY_{self._event_counter}"

        self._position = StraddlePosition(
            underlying=self._underlying,
            atm_at_entry=pending["atm_at_entry"],
            entry_spot=pending["entry_spot"],
            ce_leg=StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp, open_time=now, open_reason="manual_confirm"),
            pe_leg=StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp, open_time=now, open_reason="manual_confirm"),
            net_credit=ce_ltp + pe_ltp,
            open_time=now,
            status="open",
            session_min_vwap=float("inf"),
            entry_indicators=self._pair_indicators(ce_strike, pe_strike) or dict(self._ind),
            lot_size=self._lot_size * self._lot_multiplier,
            expiry_date=expiry_date,
        )
        self._position.entry_time_value = _ctv(ce_strike, pe_strike, self._spot, ce_ltp, pe_ltp)

        self._pin_position_legs(self._position)
        self._persist()
        asyncio.create_task(self._seed_exec_legs(int(ce_strike), int(pe_strike)))
        self._trades_today += 1
        _trade_credit = ce_ltp + pe_ltp
        self._initial_net_credit += _trade_credit
        _new_etv = float(getattr(self._position, "entry_time_value", 0.0) or 0.0) or _trade_credit
        if _new_etv > self._initial_entry_time_value:
            self._initial_entry_time_value = _new_etv

        _cid = getattr(self, "_client_id", "") or "-"
        _bid = getattr(self, "_binding_id", "") or "-"
        logger.critical(
            "SellStraddle[%s|%s|%s]: MANUALLY CONFIRMED entry — CE%d=%.2f PE%d=%.2f credit=%.2f "
            "(client-supplied prices; original broker confirmation was aborted: %s) event_id=%s",
            self._underlying, _cid, _bid, ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            pending.get("reason", "?"), event_id,
        )
        self._clog.info(
            "MANUAL-CONFIRM ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f (client-supplied, "
            "original abort reason: %s) event_id=%s",
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp, pending.get("reason", "?"), event_id,
        )
        audit_entry_exec(
            client_id=_cid, binding_id=_bid, underlying=self._underlying, ts=now,
            ce_strike=float(ce_strike), pe_strike=float(pe_strike),
            ce_ltp=float(ce_ltp), pe_ltp=float(pe_ltp), credit=float(ce_ltp + pe_ltp),
            expiry_date=expiry_date.isoformat() if expiry_date else None,
            rule_key="manual_confirm", reason="client-supplied fill price after broker confirmation aborted",
        )

        self._last_aborted_entry = None
        return True, f"confirmed CE{ce_strike}={ce_ltp:.2f} PE{pe_strike}={pe_ltp:.2f}"

    async def discard_aborted_entry(self) -> Tuple[bool, str]:
        """The client-facing counterpart to manual_confirm_entry(): "no, that
        attempt genuinely did not go through" -- clears the retained pending
        record without creating any position. The automatic abort path
        already leaves the book flat regardless, so this is purely
        dismissing the prompt/record, not an additional safety action."""
        if self._last_aborted_entry is None:
            return False, "nothing is currently pending manual confirmation"
        pending = self._last_aborted_entry
        self._last_aborted_entry = None
        self._clog.info(
            "MANUAL-CONFIRM DISCARDED — client confirmed the CE%d/PE%d attempt (aborted: %s) "
            "did not actually happen at the broker.",
            pending["ce_strike"], pending["pe_strike"], pending.get("reason", "?"),
        )
        return True, "discarded"
