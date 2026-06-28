"""
strategies/sell_straddle/exits.py — exit checks + close_position + close_leg/open_leg.

Implements the exact exit priority order and log format strings used by the engine.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from config.global_config import IST, Topic
from strategies.core.rule_evaluator import eval_rules as _eval_rules
from strategies.sell_straddle.dataclasses import format_exit_eval

if TYPE_CHECKING:
    from strategies.sell_straddle.dataclasses import StraddlePosition

logger = logging.getLogger(__name__)


class ExitMixin:
    """Exit-side logic for the sell-straddle book."""

    async def _seed_exec_legs(self, ce_strike: int, pe_strike: int) -> None:
        """At strike selection (entry), warm the EXACT exec strikes' RSI/ROC from REST 1m history."""
        try:
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

    def _build_exit_criteria(self, pos, pnl: float, credit: float):
        """Build the live exit-criteria list (and per-tf indicator dump)."""
        _crit = []
        try:
            _dpt = float(getattr(self, "_day_profit_target_pct", 0.0) or 0.0)
            _dsl = float(getattr(self, "_day_loss_sl_pct", 0.0) or 0.0)
            if credit and (_dpt or _dsl):
                _dpct = (self._session_realized_pnl_pts + pnl) / credit * 100.0
                _lbl = "Day%(θ)" if self._day_exit_basis == "theta" else "Day%"
                _crit.append((_lbl, f"{_dpct:.1f}% vs T{_dpt:.0f}/SL{_dsl:.0f}",
                              (_dpt > 0 and _dpct >= _dpt) or (_dsl > 0 and _dpct <= -_dsl)))
            elif not credit:
                _crit.append(("Day%", "SKIPPED (initial_credit=0!)", False))
            _ce_ltp = float(getattr(getattr(pos, "ce_leg", None), "ltp", 0) or 0)
            _pe_ltp = float(getattr(getattr(pos, "pe_leg", None), "ltp", 0) or 0)
            if self._ltp_decay_enabled:
                _lo = min(_ce_ltp, _pe_ltp) if (_ce_ltp > 0 and _pe_ltp > 0) else 0.0
                _crit.append(("LTPdecay", f"min({_lo:.1f}) < {self._ltp_exit_min:.0f}",
                              _lo > 0 and _lo < self._ltp_exit_min))
            if _ce_ltp > 0 and _pe_ltp > 0 and getattr(self, "_ratio_threshold", 0.0):
                _r = max(_ce_ltp, _pe_ltp) / min(_ce_ltp, _pe_ltp)
                _crit.append(("Ratio", f"{_r:.2f} vs {self._ratio_threshold:.1f}x", _r >= self._ratio_threshold))
            if self._tsl_enabled:
                _crit.append(("TSL", "ON (scalable)", False))
            if self._vwap_rise_enabled:
                _stale = not self._pool_engine.pair_atp_fresh(
                    pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec)
                _crit.append(("VWAPrise",
                              f"ON {self._vwap_rise_threshold:.1f}%{' STALE-skip' if _stale else ''}", False))
            if self._guardrail_pnl_enabled:
                _crit.append(("PnLguard", f"T{self._guardrail_pnl_target_pts:.0f}/SL{self._guardrail_pnl_sl_pts:.0f}pts", False))
            if self._guardrail_roc_enabled:
                _crit.append(("ROCguard", "ON", False))
            _exit_dump = None
            if self._exit_rules:
                _exit_ind_by_tf = self._ind_by_tf(pos.ce_leg.strike, pos.pe_leg.strike, self._exit_rules)
                _passed, _reason = _eval_rules(self._exit_rules, _exit_ind_by_tf)
                _crit.append(("Dynamic", _reason, _passed))
                _exit_dump = {tf: {k: round(v, 2) for k, v in (d or {}).items()}
                              for tf, d in _exit_ind_by_tf.items()}
                if 1 in _exit_dump:
                    _exit_dump[1]["stale"] = (0.0 if self._pool_engine.pair_atp_fresh(
                        pos.ce_leg.strike, pos.pe_leg.strike, self._vwap_stale_sec) else 1.0)
            return _crit, _exit_dump
        except Exception:
            return _crit, None

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

    async def _check_exits(self) -> None:
        pos = self._position
        if not pos:
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

        import time as _t
        if _t.monotonic() - getattr(self, "_last_exit_log", 0.0) > 60.0:
            self._last_exit_log = _t.monotonic()
            _active = "".join([
                " PnLguard" if self._guardrail_pnl_enabled else "",
                " Decay" if self._ltp_decay_enabled else "",
                " Ratio" if getattr(self, "_ratio_exit_enabled", False) else "",
                " TSL" if self._tsl_enabled else "",
                " ROC" if getattr(self, "_guardrail_roc_enabled", getattr(self, "_roc_guardrail_enabled", False)) else "",
                " VWAPrise" if self._vwap_rise_enabled else "",
                " exit_rules" if getattr(self, "_exit_rules", None) else "",
            ]) or " (none)"
            logger.info(
                "SellStraddle[%s]: EXIT-CHECK pnl=%.2f pts | Day%% T:%.0f%%/SL:%.0f%% (credit=%.2f) | "
                "EOD@%s | active exits:%s",
                self._underlying, pnl, self._day_profit_target_pct, self._day_loss_sl_pct,
                self._initial_net_credit, self._force_exit.strftime("%H:%M"), _active,
            )

        # 1. EOD FORCE SQUARE-OFF
        if self._past_squareoff(now):
            if self._position and self._position.status == "open":
                logger.info("SellStraddle[%s]: EOD SQUAREOFF — time=%s", self._underlying, now.strftime("%H:%M"))
                await self._close_position("eod_squareoff")
                self._stop_for_day = True
            return

        # POST-RESTORE WARM-UP GUARD
        if self._post_restore_warmup:
            if (self._ce_ltp_fresh and self._pe_ltp_fresh) or (_t.monotonic() - self._post_restore_at > 20.0):
                self._post_restore_warmup = False
                logger.info("SellStraddle[%s]: post-restore warm-up complete — exits armed "
                            "(CE_ltp=%.2f PE_ltp=%.2f pnl=%.2f pts).",
                            self._underlying, pos.ce_leg.ltp, pos.pe_leg.ltp, pos.unrealized_pnl)
            else:
                return

        # 2. MANDATORY GLOBAL GUARDRAILS
        if self._guardrail_pnl_enabled:
            _session_pts = self._session_realized_pnl_pts + pnl
            if self._guardrail_pnl_target_pts > 0 and _session_pts >= self._guardrail_pnl_target_pts:
                logger.info(
                    "SellStraddle[%s]: GUARDRAIL_PNL TARGET — session=%.2f pts >= %.2f",
                    self._underlying, _session_pts, self._guardrail_pnl_target_pts,
                )
                await self._close_position("guardrail_pnl_target")
                self._stop_for_day = True
                return
            if self._guardrail_pnl_sl_pts != 0 and _session_pts <= self._guardrail_pnl_sl_pts:
                logger.info(
                    "SellStraddle[%s]: GUARDRAIL_PNL SL — session=%.2f pts <= %.2f",
                    self._underlying, _session_pts, self._guardrail_pnl_sl_pts,
                )
                await self._close_position("guardrail_pnl_sl")
                return

        # 3. DAY-LEVEL % GUARDRAILS
        if self._initial_net_credit > 0:
            if self._day_exit_basis == "theta" and self._initial_entry_time_value > 0:
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                _running_theta = (_etv - pos.current_time_value(self._spot)) if _etv > 0 else pnl
                total_day_pts = self._session_realized_pnl_pts + _running_theta
                _day_denom = self._initial_entry_time_value
                _basis_lbl = "theta(cumulative)"
            else:
                total_day_pts = self._session_realized_pnl_pts + pnl
                _day_denom = self._initial_net_credit
                _basis_lbl = "ltp"
            total_day_pct = total_day_pts / _day_denom * 100

            if self._day_profit_target_pct > 0 and total_day_pct >= self._day_profit_target_pct:
                logger.info(
                    "SellStraddle[%s]: DAY PROFIT TARGET [%s] — day=%.1f%% (≥%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                    self._underlying, _basis_lbl, total_day_pct, self._day_profit_target_pct,
                    self._session_realized_pnl_pts, pnl, _day_denom,
                    pos.net_credit, pos.current_value,
                )
                await self._close_position("day_profit_target")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (profit target reached).", self._underlying)
                return

            if self._day_loss_sl_pct > 0 and total_day_pct <= -self._day_loss_sl_pct:
                logger.info(
                    "SellStraddle[%s]: DAY LOSS SL [%s] — day=%.1f%% (≤-%.1f%%) | "
                    "closed=%.2f running=%.2f credit=%.2f prem(sold=%.2f cur=%.2f)",
                    self._underlying, _basis_lbl, total_day_pct, self._day_loss_sl_pct,
                    self._session_realized_pnl_pts, pnl, _day_denom,
                    pos.net_credit, pos.current_value,
                )
                await self._close_position("day_loss_sl")
                self._stop_for_day = True
                logger.info("SellStraddle[%s]: STOPPED FOR DAY (loss SL hit).", self._underlying)
                return

        # 4. TRAILING SL (lock-%/floor-%-below-peak)
        if self._trail_sl_enabled:
            if self._trail_basis == "theta":
                _profit_pct = pos.premium_decay_pct()
            elif pos.net_credit > 0:
                _profit_pct = pnl / pos.net_credit * 100.0
            else:
                _profit_pct = None
            if _profit_pct is not None:
                if _profit_pct > pos.trail_peak_pct:
                    pos.trail_peak_pct = _profit_pct
                _lock_pts = self._trail_lock_pct * 100.0
                _floor_pts = self._trail_floor_pct * 100.0
                if pos.trail_peak_pct >= _lock_pts and _profit_pct <= (pos.trail_peak_pct - _floor_pts):
                    logger.info(
                        "SellStraddle[%s]: TRAILING SL [%s] — profit=%.1f%% dropped to peak(%.1f%%)−floor(%.1f%%) → full exit",
                        self._underlying, self._trail_basis, _profit_pct, pos.trail_peak_pct, _floor_pts,
                    )
                    await self._close_position(f"trailing_sl_{self._trail_basis}")
                    return

        # 5. LTP Decay → single-side roll
        if self._ltp_decay_enabled:
            _min_ltp = min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if 0 < _min_ltp < self._ltp_exit_min and self._position and self._position.status == "open":
                logger.info("SellStraddle[%s]: LTP DECAY min_ltp=%.2f < %.2f — single-side roll",
                            self._underlying, _min_ltp, self._ltp_exit_min)
                await self._single_side_roll(now, "ltp_decay")
                return

        # 6. Ratio exit → rollover
        if pos.ce_leg.ltp > 0 and pos.pe_leg.ltp > 0:
            ratio = max(pos.ce_leg.ltp, pos.pe_leg.ltp) / min(pos.ce_leg.ltp, pos.pe_leg.ltp)
            if ratio >= self._ratio_threshold:
                logger.info("SellStraddle[%s]: RATIO EXIT ratio=%.2fx — single-side roll",
                            self._underlying, ratio)
                await self._single_side_roll(now, "ratio_exit")
                return

        # 7. Scalable TSL → single-side roll
        if self._tsl_enabled:
            _tsl_pnl = pnl
            if self._tsl_basis == "theta":
                _etv = float(getattr(pos, "entry_time_value", 0.0) or 0.0)
                if _etv > 0:
                    _tsl_pnl = _etv - pos.current_time_value(self._spot)
            if self._check_scalable_tsl(pos, _tsl_pnl):
                logger.info("SellStraddle[%s]: SCALABLE TSL (%s) — locked=%s%.4f pnl=%s%.4f",
                            self._underlying, self._tsl_basis,
                            self._ccy_symbol, pos.tsl_high_lock_rs,
                            self._ccy_symbol, self._pnl_rs(_tsl_pnl))
                await self._single_side_roll(now, "scalable_tsl")
                return

        # 8. ROC guardrail
        if self._guardrail_roc_enabled and len(self._prem_closes) >= self._guardrail_roc_length + 1:
            _rg_bucket = f"{now.strftime('%Y%m%d_%H')}{(now.minute // self._guardrail_roc_tf) * self._guardrail_roc_tf:02d}"
            if _rg_bucket != self._last_roc_guard_bucket:
                self._last_roc_guard_bucket = _rg_bucket
                _closes = list(self._prem_closes)
                _denom = _closes[-(self._guardrail_roc_length + 1)]
                if _denom == 0:
                    _roc_val = None
                else:
                    _roc_val = (_closes[-1] - _denom) / _denom * 100
                if _roc_val is not None and self._guardrail_roc_target < 0 and _roc_val <= self._guardrail_roc_target:
                    logger.info(
                        "SellStraddle[%s]: ROC GUARDRAIL TARGET — roc=%.2f <= target=%.2f",
                        self._underlying, _roc_val, self._guardrail_roc_target,
                    )
                    await self._close_position("guardrail_roc_target")
                    return
                if _roc_val is not None and self._guardrail_roc_stoploss >= 0 and _roc_val >= self._guardrail_roc_stoploss:
                    logger.info(
                        "SellStraddle[%s]: ROC GUARDRAIL SL — roc=%.2f >= sl=%.2f",
                        self._underlying, _roc_val, self._guardrail_roc_stoploss,
                    )
                    await self._close_position("guardrail_roc_sl")
                    return

        # 9. VWAP Rise SL → smart roll
        if self._vwap_rise_enabled and self._pool_engine.pair_atp_fresh(
                int(pos.ce_leg.strike), int(pos.pe_leg.strike), self._vwap_stale_sec):
            _vp = self._pool_engine.pair_indicators(int(pos.ce_leg.strike), int(pos.pe_leg.strike))
            curr_vwap = float(_vp.get("vwap", 0.0)) if _vp else 0.0
            _vp_close = float(_vp.get("close", 0.0)) if _vp else 0.0
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
                        logger.info(
                            "SellStraddle[%s]: VWAP RISE — rise=%.2f%% curr=%.2f low=%.2f → "
                            "single-side roll (CE pnl=%.2f PE pnl=%.2f)",
                            self._underlying, rise_pct, curr_vwap, pos.session_min_vwap,
                            _ce_pnl, _pe_pnl,
                        )
                        await self._single_side_roll(now, "vwap_rise_roll")
                        return

        # 10. EXIT-EVAL — dynamic exit_rules
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
                logger.info("SellStraddle[%s]: EXIT_RULES triggered — %s", self._underlying, _reason)
                await self._close_position("exit_rules")
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
        logger.info(
            "SellStraddle[%s]: position DISCARDED after external square-off (%s) — pnl=%.2f pts; "
            "cleared persisted store so it will NOT restore on restart.",
            self._underlying, reason, pos.realized_pnl,
        )
        self._position = None
        self._persist()

    async def _close_position(self, reason: str) -> None:
        if not self._position:
            return
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        pos.realized_pnl = pos.unrealized_pnl
        pos.close_reason = reason
        pos.close_time = datetime.now(IST)
        pos.ce_leg.close_time = pos.close_time
        pos.pe_leg.close_time = pos.close_time
        pos.status = "closed"

        logger.info(
            "SellStraddle[%s]: CLOSED — reason=%s pnl=%s%.4f (%.2f pts) "
            "CE %.2f→%.2f PE %.2f→%.2f",
            self._underlying, reason,
            self._ccy_symbol, self._pnl_rs(pos.realized_pnl), pos.realized_pnl,
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
            realized_pnl=pos.realized_pnl,
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
        )
        await self._emit_order(order_ev)

        self._session_realized_pnl_pts += pos.realized_pnl
        logger.info(
            "SellStraddle[%s]: Session P&L — trade=%.2fpts cumulative=%.2fpts "
            "(day=%.1f%% of initial credit=%.2f)",
            self._underlying, pos.realized_pnl, self._session_realized_pnl_pts,
            (self._session_realized_pnl_pts / self._initial_net_credit * 100)
            if self._initial_net_credit > 0 else 0.0,
            self._initial_net_credit,
        )

        self._position = None
        self._persist()
        self._apply_sl_cooldown()

    async def _close_leg(self, side: str, reason: str, now: datetime) -> float:
        """Close ONE leg (publish EXIT legs=[side]); book that leg's P&L into the session total."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return 0.0
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        if leg.entry_price and leg.entry_price > 0:
            leg_pnl = leg.entry_price - leg.ltp
        else:
            leg_pnl = 0.0
            logger.error("SellStraddle[%s]: %s%d entry_price=%.2f invalid at close — booking pnl=0 "
                         "(NOT a real loss; entry was lost). reason=%s",
                         self._underlying, side, int(leg.strike), float(leg.entry_price or 0.0), reason)
        leg.close_time = now
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="EXIT", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, close_reason=reason, realized_pnl=leg_pnl,
            ce_entry=pos.ce_leg.entry_price, pe_entry=pos.pe_leg.entry_price,
            event_id=f"{self._underlying}_EXITLEG_{side}_{self._event_counter}",
            legs=[side],
            leg_open_times={side: leg.open_time.isoformat() if leg.open_time else None},
            leg_open_reasons={side: leg.open_reason},
        )
        await self._emit_order(order_ev)
        self._session_realized_pnl_pts += leg_pnl
        logger.info("SellStraddle[%s]: CLOSE LEG %s strike=%.0f pnl=%.2fpts [%s]",
                    self._underlying, side, leg.strike, leg_pnl, reason)
        return leg_pnl

    async def _open_leg(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Open ONE leg at a new strike (publish ENTRY legs=[side]); update the leg."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        leg.strike = strike
        leg.entry_price = ltp
        leg.ltp = ltp
        leg.open_time = now
        leg.open_reason = reason
        leg.close_time = None
        pos.net_credit = pos.ce_leg.entry_price + pos.pe_leg.entry_price
        pos.tsl_high_lock_rs = 0.0
        pos.open_time = now
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, indicators=dict(self._ind),
            event_id=f"{self._underlying}_OPENLEG_{side}_{self._event_counter}",
            legs=[side],
        )
        await self._emit_order(order_ev)
        logger.info("SellStraddle[%s]: OPEN LEG %s strike=%.0f @%.2f [%s]",
                    self._underlying, side, strike, ltp, reason)
