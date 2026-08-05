"""
strategies/sell_straddle/entries.py — entry evaluation + priming + open_position.

Contains the beginning/re-entry rule evaluation, balanced-pair selection, and the
optimistic position open that publishes the ENTRY StraddleOrderEvent.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

from config.global_config import IST, Topic
from data_layer.runtime_config import RuntimeConfig
from strategies.core.rule_evaluator import eval_rules as _eval_rules
from strategies.sell_straddle.audit import (
    audit_entry_eval,
    audit_entry_exec,
)

logger = logging.getLogger(__name__)


class EntryMixin:
    """Entry-side logic for the sell-straddle book."""

    # ── Priming wait ──────────────────────────────────────────────────────────

    def _priming_wait_minutes(self, rules: List[dict]) -> int:
        """
        Mirrors old base.py _is_in_priming_wait():
          wait = max_rule_tf × 2   if any rule uses SLOPE / VWAP_SLOPE
               = max_rule_tf × 1   otherwise
        """
        if not rules:
            return 0
        tfs = [int(r.get("tf", 1)) for r in rules if r.get("tf")]
        max_tf = max(tfs) if tfs else 1
        slope_names = {"slope", "vwap_slope", "slope_curr", "slope_prev"}
        has_slope = any(
            r.get("indicator", "").lower() in slope_names
            for r in rules
            if r.get("indicator", "").lower() != "advanced"
        )
        return max_tf * (2 if has_slope else 1)

    def _is_primed(self, now: datetime, rules: List[dict]) -> bool:
        """True once market_open + wait_minutes has passed."""
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
        ready_at = self._market_open_dt + timedelta(minutes=wait_min)
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
        if self._position and self._position.status == "open":
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

        _trace: list = []
        # Re-entry now uses the same balanced-pair logic as beginning: anchor at the
        # ATM side with lower TIME VALUE, partner raw LTP must be <= anchor time value
        # and pass the dual floor.  This prevents the old scan_pool behaviour that picked
        # the globally most-balanced LTP pair, often deep ITM on both sides (e.g.
        # CE6500/PE7300 when ATM was 6900).  The re-entry rules are evaluated AFTER the
        # pair is selected, not during selection (same as beginning).
        sel = select_balanced_pair(
            self._strike_prem, self._spot, step, offset, ltp_target, trace=_trace,
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
                    self._strike_prem, self._spot, step, offset, ltp_target,
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
            sel = select_balanced_pair_at(
                self._strike_prem, atm, self._spot, step, offset, ltp_target, trace=_trace,
                entry_basis=self._entry_basis, theta_target=theta_target,
                variable_strikes=variable_strikes, balance_ratio=balance_ratio,
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
