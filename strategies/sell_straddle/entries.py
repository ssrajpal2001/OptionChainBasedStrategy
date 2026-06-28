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

        want_beg = (workflow == "beginning_only") or (
            workflow == "hybrid" and is_beginning and not self._beginning_failed)
        want_re = (workflow == "reentry_only") or (workflow == "hybrid")

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
        _eff_target = self._theta_target if self._entry_basis == "theta" else ltp_target

        if (self._position is None or self._position.status != "open"):
            _audit_clients = self._granular_audit_clients()
            if _audit_clients:
                try:
                    _atm = round(self._spot / step) * step if self._spot else 0
                    _crit_h = [
                        {"name": "Status", "detail": f"no open position — {concept} scan", "hit": False},
                        {"name": "Spot/ATM", "detail": f"{self._spot:.2f} / {int(_atm)}", "hit": False},
                        {"name": "Target/Offset", "detail": f"{self._entry_basis}≥{_eff_target:.0f}, ±{offset}", "hit": False},
                    ]
                    for _cid, _bid in _audit_clients:
                        await self._bus.publish(Topic.EXIT_AUDIT, {
                            "type": "exit_audit", "client_id": _cid, "binding_id": _bid,
                            "underlying": self._underlying, "pnl": 0.0, "credit": 0.0,
                            "criteria": _crit_h, "ind_by_tf": {}, "ts": now.timestamp(),
                        })
                except Exception:
                    pass

        from strategies.sell_straddle.selection import select_balanced_pair, scan_pool

        _trace: list = []
        if use_beginning_sel:
            sel = select_balanced_pair(
                self._strike_prem, self._spot, step, offset, ltp_target, trace=_trace,
                entry_basis=self._entry_basis, theta_target=self._theta_target,
            )
        else:
            sel = scan_pool(
                self._strike_prem, self._spot, step, offset, ltp_target,
                rule_pass=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules))[0],
                metric=ss.get("reentry_best_metric", "balanced_premium"),
                trace=_trace,
                entry_basis=self._entry_basis, theta_target=self._theta_target,
            )

        for _ln in _trace:
            self._clog.info("SELECT %s | %s", self._underlying, _ln)

        if not sel:
            if use_beginning_sel:
                self._clog.info(
                    "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair (target=%.2f[%s] offset=%d)",
                    self._underlying, rule_key, self._spot, _eff_target, self._entry_basis, offset,
                )
            else:
                from strategies.sell_straddle.selection import reentry_block_reason
                diag = reentry_block_reason(
                    self._strike_prem, self._spot, step, offset, ltp_target,
                    rule_eval=lambda cs, ps: _eval_rules(rules, self._ind_by_tf(cs, ps, rules)),
                )
                if diag["kind"] == "no_pair":
                    self._clog.info(
                        "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair exists (target=%.2f[%s] offset=%d)",
                        self._underlying, rule_key, self._spot, _eff_target, self._entry_basis, offset,
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
        _dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()} for tf, d in ind_by_tf.items()}
        self._clog.info(
            "EVAL %s [%s/%s] sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s | ind_by_tf=%s",
            self._underlying, rule_key, concept, ce_strike, ce_ltp, pe_strike, pe_ltp,
            ce_ltp + pe_ltp, reason, "PASS" if passed else "BLOCK", _dump,
        )
        if not passed:
            if use_beginning_sel and "N/A" not in reason:
                self._beginning_failed = True
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
        await self._open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason)

    async def _open_position(
        self, now: datetime, ce_strike: int, pe_strike: int,
        ce_ltp: float, pe_ltp: float, rule_key: str, reason: str,
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
        )
        self._position.entry_time_value = _ctv(ce_strike, pe_strike, self._spot, ce_ltp, pe_ltp)
        self._persist()
        asyncio.create_task(self._seed_exec_legs(int(ce_strike), int(pe_strike)))
        self._trades_today += 1
        self._beginning_failed = False
        self._order_pending = True
        if self._initial_net_credit <= 0:
            self._initial_net_credit = ce_ltp + pe_ltp
        if self._position:
            _new_etv = float(getattr(self._position, "entry_time_value", 0.0) or 0.0) or (ce_ltp + pe_ltp)
            if _new_etv > self._initial_entry_time_value:
                self._initial_entry_time_value = _new_etv

        logger.info(
            "SellStraddle[%s]: ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f | %s=PASS [%s]",
            self._underlying, ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            rule_key, reason,
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
        )
        await self._emit_order(order_ev)
