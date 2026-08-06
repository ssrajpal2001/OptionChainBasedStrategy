"""
strategies/sell_straddle/engine.py — SellStraddleStrategy orchestrator.

Inherits from ``AbstractStrategyBook`` and composes the indicator / entry / exit /
rolling mixins.  Owns the async feed loops, persistence, session lifecycle, and
public accessors.  Strategy-specific logic lives in the sibling modules.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, date, time as dtime
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic
from data_layer.base_feeder import CandleEvent, EventBus
from data_layer.runtime_config import RuntimeConfig
# Indicator computations live in strategies.sell_straddle.indicators

from strategies.core import OrderEmitter, PositionStoreMixin, PositionUpdateMixin
from strategies.core.base_book import AbstractStrategyBook
from strategies.pool_indicator_engine import PoolIndicatorEngine
from strategies.sell_straddle.config import ConfigMixin
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition
from strategies.sell_straddle.entries import EntryMixin
from strategies.sell_straddle.exits import ExitMixin
from strategies.sell_straddle.indicators import IndicatorMixin
from strategies.sell_straddle.rolling import RollingMixin

logger = logging.getLogger(__name__)

_BUF = 600
_MARKET_OPEN = dtime(9, 15)


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(f"ss_{tag}_{date_str}", propagate=False)


def pool_strike_set(atm: float, step: float, itm_depth: int, otm_depth: int,
                    pinned: Optional[set] = None) -> set:
    """Strikes to keep subscribed: ATM-itm_depth*step .. ATM+otm_depth*step (inclusive),
    PLUS any pinned strikes (the running position's legs — never dropped even if out of range)."""
    atm_r = round(atm / step) * step
    out = {int(atm_r + i * step) for i in range(-itm_depth, otm_depth + 1)}
    if pinned:
        out |= {int(p) for p in pinned}
    return out


class SellStraddleStrategy(AbstractStrategyBook, PositionStoreMixin, PositionUpdateMixin,
                           ConfigMixin, IndicatorMixin, EntryMixin, ExitMixin, RollingMixin):

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        underlying: str = "NIFTY",
        lot_multiplier: int = 1,
        client_id: str = "",
        binding_id: str = "",
    ) -> None:
        if cfg is None:
            from config.global_config import GlobalConfig
            cfg = GlobalConfig()
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        PositionUpdateMixin.__init__(self, bus, client_id, binding_id, "sell_straddle", underlying)
        self._lot_multiplier = lot_multiplier
        self._client_db = None

        self._position: Optional[StraddlePosition] = None
        self._trades_today: int = 0

        self._spot: float = 0.0
        self._spot_reject_streak: int = 0  # consecutive suspect-jump ticks ignored
        self._ce_ltp: float = 0.0
        self._pe_ltp: float = 0.0
        self._ce_atp: float = 0.0
        self._pe_atp: float = 0.0
        self._prev_vwap_atp: Optional[float] = None
        self._strike_prem: Dict[Tuple[int, str], dict] = {}
        self._prev_atp_closed: Dict[Tuple[int, str], float] = {}
        self._itm_gate_armed: bool = False
        self._ltp_target: float = 0.0

        self._market_open_dt: Optional[datetime] = None
        self._primed: bool = False
        self._order_pending: bool = False
        self._roll_close_waiters: Dict[str, asyncio.Event] = {}
        # event_id -> the StraddleFillEvent that woke the matching waiter above (both leg-closes
        # and full-position closes). Consumed by _close_leg / _close_position to see whether the
        # wake-up was a REAL confirmed exit or an exit_aborted (broker unavailable) fill.
        self._roll_close_results: Dict[str, object] = {}
        self._roll_in_progress: bool = False
        self._last_roll_attempt: Dict[str, datetime] = {}
        self._last_exit_rules_bucket: str = ""
        self._last_entry_bucket_b: str = ""
        self._last_entry_bucket_r: str = ""
        self._chart_last_min = None

        self._session_realized_pnl_pts: float = 0.0
        self._initial_net_credit: float = 0.0
        self._initial_entry_time_value: float = 0.0
        self._stop_for_day: bool = False

        self._post_restore_warmup: bool = False
        self._post_restore_at: float = 0.0
        self._ce_ltp_fresh: bool = True
        self._pe_ltp_fresh: bool = True

        self._tasks: list = []
        self._close_in_progress: bool = False
        self._sl_cooldown_until: Optional[datetime] = None
        self._event_counter: int = 0
        self._order_emitter = OrderEmitter(self._bus, self._client_id, self._binding_id)
        self._rebalancer = None  # set via set_rebalancer()
        self._delta_chain = None  # set via set_delta_chain_manager() for crypto
        self._entry_expiry_date: Optional[date] = None  # effective expiry for new entries
        self._entry_expiry_tokens: list = []  # window tokens subscribed for _entry_expiry_date

        self._prem_closes: deque = deque(maxlen=_BUF)
        self._prem_volumes: deque = deque(maxlen=_BUF)
        self._chart_series: deque = deque(maxlen=375)
        self._load_chart_history()

        self._pool_engine = PoolIndicatorEngine(rsi_len=14, roc_len=10)

        self._idx_highs: deque = deque(maxlen=_BUF)
        self._idx_lows: deque = deque(maxlen=_BUF)
        self._idx_closes: deque = deque(maxlen=_BUF)

        self._ind: Dict[str, float] = {
            "rsi": 50.0, "vwap": 0.0,
            "adx": 0.0, "pdi": 0.0, "mdi": 0.0,
            "ema_fast": 0.0, "ema_slow": 0.0,
            "ltp": 0.0, "close": 0.0,
        }

        self._clog: logging.Logger = _make_strategy_logger(underlying, client_id, binding_id)
        self._load_thresholds()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @property
    def _persist_key(self) -> str:
        if self._client_id and self._binding_id:
            return f"{self._client_id}_{self._binding_id}_{self._underlying}_sell_straddle"
        return f"{self._underlying}_sell_straddle"

    def set_rebalancer(self, rebalancer) -> None:
        """Inject StrikeRebalancer so the engine can fetch option-chain snapshots and subscribe
        next-week expiry strikes when the current weekly expiry fails the dual floor."""
        self._rebalancer = rebalancer

    def set_delta_chain_manager(self, delta_chain) -> None:
        """Inject DeltaChainManager so crypto books can pin open-position legs and keep them
        subscribed through sharp moves / re-subscription. No-op for non-crypto underlyings."""
        self._delta_chain = delta_chain

    async def _emit_order(self, ev) -> None:
        """Stamp this book's identity on every order so the bridge routes to ONLY this binding."""
        await self._order_emitter.emit(Topic.ORDER_REQUEST, ev)

    def _current_product_type(self) -> str:
        """The REAL configured product type for this underlying's sell_straddle
        deployment (MIS/NRML) -- same source straddle_bridge.py reads when it
        actually places the order. 2026-08-06 CRITICAL FIX: self._product_type
        was never set anywhere on this class, so _persist() always silently
        tagged every stored position "MIS" regardless of real config. Any
        NRML (carry-forward) deployment would have a legitimately-still-open
        overnight position wrongly discarded on the next restart as
        "yesterday's already-squared-off intraday position" (position_store.py's
        MIS new-day-discard rule)."""
        try:
            _pt = str(RuntimeConfig.index_section(self._underlying, "sell_straddle")
                      .get("product_type", "MIS")).upper()
        except Exception:
            _pt = "MIS"
        return _pt if _pt in ("MIS", "NRML") else "MIS"

    def _persist(self) -> None:
        try:
            if self._position and self._position.status == "open":
                # 2026-08-06 CRITICAL FIX: save()/clear() now report success/failure
                # instead of always silently swallowing an I/O error two layers
                # down (position_store.py). One retry, then a LOUD, impossible-to-
                # miss alert on repeated failure -- previously a single transient
                # disk/permission hiccup could desync the on-disk file from the
                # real in-memory position with zero signal anywhere, until the
                # next state-transition event or forever if the process crashed
                # in between.
                _ok = self.persist(self._persist_key, self._position.to_dict(),
                                   product_type=self._current_product_type())
                if not _ok:
                    _ok = self.persist(self._persist_key, self._position.to_dict(),
                                       product_type=self._current_product_type())
                if not _ok:
                    _cid = getattr(self, "_client_id", "") or "-"
                    _bid = getattr(self, "_binding_id", "") or "-"
                    logger.critical(
                        "SellStraddle[%s|%s|%s]: POSITION PERSIST FAILED TWICE -- "
                        "on-disk state may be STALE/DESYNCED from the real in-memory "
                        "position. A restart before the next successful persist would "
                        "orphan this real broker position. Check disk space/permissions "
                        "on the positions data directory NOW.",
                        self._underlying, _cid, _bid,
                    )
                self.notify_position_update(self._position.to_dict(), force=True)
            else:
                # 2026-08-06 DIAGNOSTIC (temporary): a real, freshly-filled position has
                # been observed going missing (self._position -> None/closed) within
                # seconds-to-minutes of entry, on multiple independent client books,
                # with no single _persist() caller consistently reproducing it despite
                # extensive log-based investigation. Log the full call stack every time
                # this branch clears the store so the NEXT occurrence identifies the
                # exact caller directly instead of another round of guessing. Remove
                # once root-caused.
                import traceback
                logger.warning(
                    "SellStraddle[%s|%s|%s]: _persist() CLEARING position store "
                    "(self._position=%r) -- call stack:\n%s",
                    self._underlying, getattr(self, "_client_id", "") or "-",
                    getattr(self, "_binding_id", "") or "-", self._position,
                    "".join(traceback.format_stack(limit=10)),
                )
                _ok = self.clear(self._persist_key)
                if not _ok:
                    _ok = self.clear(self._persist_key)
                if not _ok:
                    logger.critical(
                        "SellStraddle[%s|%s|%s]: POSITION CLEAR FAILED TWICE -- a stale "
                        "'still open' file may be left on disk. A restart before the "
                        "next successful clear could RESURRECT an already-closed "
                        "position the broker no longer holds. Check disk space/"
                        "permissions on the positions data directory NOW.",
                        self._underlying, getattr(self, "_client_id", "") or "-",
                        getattr(self, "_binding_id", "") or "-",
                    )
                self.notify_position_update(None, force=True)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: persist failed: %s", self._underlying, exc)
        self._persist_session()

    def _persist_session(self) -> None:
        try:
            from data_layer import position_store as _ps
            _ps.save(self._persist_key + "_session", {
                "session_realized_pnl_pts": self._session_realized_pnl_pts,
                "trades_today": self._trades_today,
                "stop_for_day": self._stop_for_day,
                "session_day": str(self._session_day(datetime.now(IST))),
                "initial_net_credit": self._initial_net_credit,
            }, product_type="MIS")
        except Exception as exc:
            logger.debug("SellStraddle[%s]: session persist failed: %s", self._underlying, exc)

    def _restore_session(self) -> None:
        try:
            from data_layer import position_store as _ps
            _sess = _ps.load(self._persist_key + "_session")
            if _sess and str(_sess.get("session_day", str(self._session_day(datetime.now(IST))))) \
                    != str(self._session_day(datetime.now(IST))):
                logger.info("SellStraddle[%s]: persisted session is from a prior trading day "
                            "(%s) — starting fresh.", self._underlying, _sess.get("session_day"))
                _sess = None
            if _sess:
                self._session_realized_pnl_pts = float(_sess.get("session_realized_pnl_pts", 0.0) or 0.0)
                self._trades_today = max(self._trades_today, int(_sess.get("trades_today", 0) or 0))
                self._stop_for_day = bool(_sess.get("stop_for_day", False))
                _saved_credit = float(_sess.get("initial_net_credit", 0.0) or 0.0)
                if _saved_credit > 0 and self._initial_net_credit <= 0:
                    self._initial_net_credit = _saved_credit
                # If session losses already breach day_loss_sl, lock immediately so a
                # fresh book can't re-enter and trigger an immediate day_loss_sl exit.
                if (not self._stop_for_day and self._day_loss_sl_pct > 0
                        and self._initial_net_credit > 0):
                    _restored_pct = self._session_realized_pnl_pts / self._initial_net_credit * 100
                    if _restored_pct <= -self._day_loss_sl_pct:
                        self._stop_for_day = True
                        logger.info(
                            "SellStraddle[%s]: STOP FOR DAY set on restore — "
                            "session P&L=%.1f%% already ≤ -%.1f%% (booked=%.2f credit=%.2f)",
                            self._underlying, _restored_pct, self._day_loss_sl_pct,
                            self._session_realized_pnl_pts, self._initial_net_credit,
                        )
                logger.info("SellStraddle[%s]: restored session — booked=%.2f pts trades=%d "
                            "credit=%.2f stop_for_day=%s",
                            self._underlying, self._session_realized_pnl_pts, self._trades_today,
                            self._initial_net_credit, self._stop_for_day)
        except Exception as exc:
            logger.debug("SellStraddle[%s]: session restore failed: %s", self._underlying, exc)

    def start(self) -> None:
        self._running = True
        self._restore_session()
        try:
            from data_layer import position_store as _ps
            _saved = _ps.load(self._persist_key)
            if _saved:
                self._position = StraddlePosition.from_dict(_saved)
                if not self._position.lot_size:
                    self._position.lot_size = self._lot_size * self._lot_multiplier
                self._trades_today = max(self._trades_today, 1)
                if self._initial_net_credit <= 0 and self._position.net_credit > 0:
                    self._initial_net_credit = self._position.net_credit
                if self._initial_entry_time_value <= 0:
                    self._initial_entry_time_value = float(
                        getattr(self._position, "entry_time_value", 0.0) or 0.0
                    ) or self._initial_net_credit
                import time as _t
                self._post_restore_warmup = True
                self._post_restore_at = _t.monotonic()
                self._ce_ltp_fresh = False
                self._pe_ltp_fresh = False
                logger.info("SellStraddle[%s]: restored open position from store (credit=%.2f, qty=%d) "
                            "— exits HELD until fresh LTPs arrive.",
                            self._underlying, self._position.net_credit, self._position.lot_size)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: restore failed: %s", self._underlying, exc)
        _tag = f"{self._underlying}" + (f"_{self._client_id}_{self._binding_id}"
                                        if self._client_id and self._binding_id else "")
        self._loop_queues: Dict[str, asyncio.Queue] = {}
        self._tasks = [
            asyncio.create_task(self._candle_loop(), name=f"ss_{_tag}_candle"),
            asyncio.create_task(self._tick_loop(), name=f"ss_{_tag}_tick"),
            asyncio.create_task(self._option_loop(), name=f"ss_{_tag}_opt"),
            asyncio.create_task(self._fill_loop(), name=f"ss_{_tag}_fill"),
        ]
        asyncio.create_task(self._seed_pool())
        logger.info("SellStraddleStrategy[%s]: started.", self._underlying)
        try:
            self._log_settings_banner()
        except Exception as exc:
            logger.warning("SellStraddle[%s]: settings banner failed: %s", self._underlying, exc)

    async def _seed_pool(self):
        try:
            from data_layer.historical_candles import fetch_upstox_warm_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            import asyncio as _aio
            if self._is_crypto:
                # Crypto (Delta) uses live ticks from DeltaChainManager; no Upstox warm seed available.
                self._entry_expiry_date = self._effective_entry_expiry()
                logger.info("SellStraddle[%s]: pool seed skipped for crypto (relying on live Delta ticks).",
                            self._underlying)
                return
            # Resolve entry expiry NOW — before token check — because this only needs
            # the REGISTRY (loaded at run_system.py startup), not the Upstox feeder token.
            # Without this, a book started after startup (new client deploy) that uses a
            # shared feed (no local Upstox creds) would keep _entry_expiry_date = None
            # and abort every entry attempt.
            self._entry_expiry_date = self._effective_entry_expiry()
            if self._entry_expiry_date:
                logger.info("SellStraddle[%s]: entry_expiry resolved = %s",
                            self._underlying, self._entry_expiry_date.isoformat())
            else:
                logger.warning("SellStraddle[%s]: entry_expiry not yet resolvable — registry may not be loaded.",
                               self._underlying)

            for _ in range(30):
                if self._spot > 0:
                    break
                await _aio.sleep(2)
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token or self._spot <= 0:
                logger.info("SellStraddle[%s]: pool warm seed skipped (no token/spot). entry_expiry=%s",
                            self._underlying,
                            self._entry_expiry_date.isoformat() if self._entry_expiry_date else None)
                return

            step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
            ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
            itm = int(ss.get("pool_itm_depth", 4))
            otm = int(ss.get("pool_otm_depth", 4))

            # Re-confirm expiry (may have changed if token arrived after registry was re-loaded)
            self._entry_expiry_date = self._effective_entry_expiry()
            exp = self._entry_expiry_date
            if not exp:
                logger.warning("SellStraddle[%s]: pool seed skipped — no expiry resolved.", self._underlying)
                return

            today = datetime.now(IST).date()
            _shift_msg = " (expiry-day shift)" if exp != REGISTRY.get_active_expiry(self._underlying, today) else ""
            logger.info("SellStraddle[%s]: entry expiry = %s%s.", self._underlying, exp.isoformat(), _shift_msg)

            # On expiry day, subscribe the next-week window immediately and seed strike_prem from snapshot
            # so the first 09:20 entry attempt has data even before live ticks arrive.
            if _shift_msg:
                await self._subscribe_expiry_window(exp)
                try:
                    underlying_key = REGISTRY.get_upstox_index_key(self._underlying)
                    chain = await self._rebalancer.fetch_option_chain(underlying_key, exp)
                    if chain:
                        sp = self._build_strike_prem_from_chain(chain)
                        self._strike_prem.update(sp)
                        logger.info("SellStraddle[%s]: seeded strike_prem from %s chain rows.",
                                    self._underlying, len(sp))
                except Exception as exc:
                    logger.warning("SellStraddle[%s]: expiry-day chain seed failed: %s", self._underlying, exc)

            strikes = pool_strike_set(self._spot, step, itm, otm)
            seeded = 0
            seed_pairs: list = [(int(stk), side) for stk in strikes for side in ("CE", "PE")]
            pos = self._position
            if pos and pos.status == "open" and pos.ce_leg and pos.pe_leg:
                for _stk, _side in [(int(pos.ce_leg.strike), "CE"), (int(pos.pe_leg.strike), "PE")]:
                    if (_stk, _side) not in seed_pairs:
                        seed_pairs.append((_stk, _side))
                # 2026-07-24 fix: StrikeRebalancer only pins a position's
                # strikes reactively, via the ORDER_FILL event on a fresh
                # entry -- a position RESTORED from disk after a restart
                # never re-fires that event, so on a fresh process the
                # rebalancer's pinned_strikes starts empty and has no idea
                # this position exists. If ATM has drifted since entry (the
                # normal case after any real time has passed), the next
                # ATM-window rebalance silently unsubscribes the position's
                # own legs -- confirmed live: a real SENSEX position lost
                # its CE/PE ticks after a restart, indicators went stale,
                # and P&L was computed from garbage. Re-pin explicitly here,
                # not just for entries -- restore counts too.
                if not self._is_crypto and self._rebalancer:
                    try:
                        self._rebalancer.pin_strike(self._underlying, float(pos.ce_leg.strike))
                        self._rebalancer.pin_strike(self._underlying, float(pos.pe_leg.strike))
                        logger.info(
                            "SellStraddle[%s]: re-pinned restored position legs CE%d/PE%d "
                            "in StrikeRebalancer (pinned_strikes now %s).",
                            self._underlying, int(pos.ce_leg.strike), int(pos.pe_leg.strike),
                            sorted(self._rebalancer.pinned_strikes(self._underlying)),
                        )
                    except Exception as exc:
                        logger.warning("SellStraddle[%s]: re-pin of restored position legs "
                                       "failed: %s", self._underlying, exc)
            for stk, side in seed_pairs:
                ikey = REGISTRY.get_broker_symbol(self._underlying, exp, stk, side, "upstox")
                if not ikey:
                    continue
                bars = await fetch_upstox_warm_1m(ikey, token)
                if bars:
                    closes = [b["close"] for b in bars]
                    self._pool_engine.seed_strike(stk, side, closes, closes)
                    seeded += 1
            logger.info("SellStraddle[%s]: pool engine seeded %d legs (warm RSI/ROC) expiry=%s.",
                        self._underlying, seeded, exp.isoformat())
        except Exception as exc:
            logger.warning("SellStraddle[%s]: pool seed failed: %s", self._underlying, exc)

    def _log_settings_banner(self) -> None:
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")

        def _render(rules: list) -> str:
            if not rules:
                return "(none — immediate when LTP target met)"
            parts: list = []
            for i, r in enumerate(rules):
                if (r.get("indicator") or "").lower() == "advanced":
                    o1, o2 = (r.get("operand1") or "").upper(), (r.get("operand2") or "").upper()
                    seg = f"{o1}{r.get('operator_sym','')}{o2}({r.get('tf','')}m)"
                else:
                    seg = f"{(r.get('indicator') or '').upper()}{r.get('operator_sym','')}{r.get('threshold','')}({r.get('tf','')}m)"
                if i > 0:
                    parts.append((r.get("operator") or "AND").upper())
                parts.append(seg)
            return " ".join(parts)

        workflow = ss.get("entry_workflow_mode", "hybrid")
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        beg = _render(ss.get("entry_rules_beginning", []))
        ren = _render(ss.get("entry_rules_reentry", []))
        exit_rules = _render(ss.get("exit_rules", []))
        ratio_on = ss.get("ratio_exit", {}).get("enabled", True)
        decay_on = self._ltp_decay_enabled

        L = [
            "╔══════════════════════════════════════════════════════════════════════",
            f"║ ACTIVE SELL-STRADDLE SETTINGS — {self._underlying}",
            "╠══════════════════════════════════════════════════════════════════════",
            f"║ TIMING: Start:{self._entry_start.strftime('%H:%M')} | EntryEnd:{self._entry_cutoff.strftime('%H:%M')} | "
            f"SquareOff:{self._force_exit.strftime('%H:%M')} | Lot:{self._lot_size} x{self._lot_multiplier}",
            f"║ SELECTION: workflow={workflow} | pool_offset=±{offset} | "
            f"variable_strikes={'ON' if ss.get('variable_strikes') else 'OFF'} | "
            f"DUAL FLOOR: ltp≥{self._ltp_target:.0f} theta≥{self._theta_target:.0f} | "
            f"BALANCE:{self._balance_ratio:.2f}",


            f"║ BEGINNING ENTRY: {beg}",
            f"║ RE-ENTRY GATES:  {ren}",
            f"║ ROLLOVERS: Decay:{'ON' if decay_on else 'OFF'}({self._ltp_exit_min:.0f}) | "
            f"Ratio:{'ON' if ratio_on else 'OFF'}({self._ratio_threshold:.1f}x"
            + (f" MaxEntry:{self._max_entry_ratio:.1f}x" if self._max_entry_ratio > 0 else "")
            + ") | SmartRoll:ON",
            f"║ SCALABLE TSL: {'ON' if self._tsl_enabled else 'OFF'} "
            f"Base:{self._tsl_base_profit_rs:.0f}/{self._tsl_base_lock_rs:.0f} "
            f"Step:{self._tsl_step_profit_rs:.0f}/{self._tsl_step_lock_rs:.0f} ({self._ccy_symbol}/BTC if crypto) "
            f"BASIS:{self._tsl_basis.upper()}",
            f"║ VWAP RISE SL: {'ON' if self._vwap_rise_enabled else 'OFF'}({self._vwap_rise_threshold:.2f}%)",
            f"║ ITM PAIR GATE: {'ON' if self._itm_pair_gate_enabled else 'OFF'} "
            f"(profit≥₹{self._itm_pair_gate_profit_inr:.0f}, gap>{self._itm_pair_gate_min_strike_gap:.0f}pts "
            f"→ rollover, 70% roll-protect)",
            f"║ SAME-DAY EXPIRY: {'ALLOWED' if getattr(self, '_same_day_expiry_enabled', False) else 'SHIFT TO NEXT'}",
            f"║ DAY: T:{self._day_profit_target_pct:.0f}% SL:{self._day_loss_sl_pct:.0f}% "
            f"BASIS:{self._day_exit_basis.upper()}",
            f"║ DYNAMIC EXITS: {exit_rules}",
            f"║ EXIT PRIORITY: EOD→Day%→LTPdecay→Ratio→ScalableTSL→exit_rules→VWAPrise→ITMgate",
            f"║ LIMITS: Max Daily Trades:{self._max_trades}",
            "╚══════════════════════════════════════════════════════════════════════",
        ]
        for line in L:
            logger.info(line)
            self._clog.info(line)

    # ── Expiry-day shift helpers ──────────────────────────────────────────────

    def _effective_entry_expiry(self) -> Optional[date]:
        """Return the expiry to use for NEW entries.
        On the current active expiry's date we shift to the next weekly expiry.
        On all other days we stay on the current active expiry.
        For Delta crypto (BTC/ETH) the active daily expiry is computed directly."""
        if self._is_crypto:
            from data_layer.universal_option_mapper import UniversalOptionMapper
            return UniversalOptionMapper.active_daily_expiry()
        from data_layer.instrument_registry import REGISTRY
        today = datetime.now(IST).date()
        current = REGISTRY.get_active_expiry(self._underlying, today)
        if not current:
            return None
        if today == current:
            if getattr(self, "_same_day_expiry_enabled", False):
                return current
            exps = [e for e in REGISTRY.all_expiries(self._underlying) if e > current]
            if exps:
                return exps[0]
        return current

    def _build_strike_prem_from_chain(self, chain: dict) -> Dict[Tuple[int, str], dict]:
        """Build a { (strike, side): {'ltp': ..., 'atp': ...} } map from an Upstox option-chain snapshot.
        ATP is not available in the chain snapshot, so it is seeded with LTP as a placeholder."""
        out: Dict[Tuple[int, str], dict] = {}
        if not chain or not isinstance(chain, dict):
            return out
        for row in chain.get("data") or []:
            strike = int(float(row.get("strike_price") or 0))
            if strike <= 0:
                continue
            for side, side_key in (("CE", "call_options"), ("PE", "put_options")):
                side_data = row.get(side_key) or {}
                md = side_data.get("market_data") or {}
                ltp = float(md.get("ltp") or 0)
                if ltp > 0:
                    out[(strike, side)] = {"ltp": ltp, "atp": ltp}
        return out

    async def _subscribe_expiry_window(self, expiry: date) -> None:
        """Subscribe the ATM ± pool-depth window for the chosen expiry so live ticks arrive."""
        if self._is_crypto:
            # DeltaChainManager already maintains the active expiry window.
            return
        if not self._rebalancer:
            return
        feeder = getattr(self._rebalancer, "_feeder", None)
        if not feeder:
            return
        from data_layer.instrument_registry import REGISTRY
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        itm = int(ss.get("pool_itm_depth", 4))
        otm = int(ss.get("pool_otm_depth", 4))
        strikes = pool_strike_set(self._spot, step, itm, otm)
        tokens = []
        for stk in strikes:
            for side in ("CE", "PE"):
                key = REGISTRY.get_broker_symbol(self._underlying, expiry, int(stk), side, "upstox")
                if key:
                    tokens.append(key)
        if tokens:
            await feeder.subscribe_tokens(tokens)
            self._entry_expiry_tokens = tokens
            logger.info(
                "SellStraddle[%s]: subscribed %s window (%d tokens) for expiry %s.",
                self._underlying, self._underlying, len(tokens), expiry.isoformat(),
            )

    def _pin_position_legs(self, pos) -> None:
        """Pin the open position legs in DeltaChainManager so a sharp move or window
        re-subscription never unsubscribes them. No-op for non-crypto or missing manager."""
        if not getattr(self, "_is_crypto", False) or not self._delta_chain or not pos:
            return
        from data_layer.universal_option_mapper import UniversalOptionMapper as _M
        from data_layer.symbol_translator import InternalSymbol
        exp = getattr(pos, "expiry_date", None) or self._entry_expiry_date
        if not exp:
            return
        syms = []
        for side, leg in (("CE", pos.ce_leg), ("PE", pos.pe_leg)):
            if leg and getattr(leg, "strike", 0) > 0:
                syms.append(_M.to_delta_symbol(InternalSymbol(self._underlying, float(leg.strike), side, exp)))
        if syms:
            self._delta_chain.pin_symbols(self._underlying, syms)

    def _unpin_position_legs(self, pos) -> None:
        """Remove the DeltaChainManager pin for the given position legs."""
        if not getattr(self, "_is_crypto", False) or not self._delta_chain or not pos:
            return
        from data_layer.universal_option_mapper import UniversalOptionMapper as _M
        from data_layer.symbol_translator import InternalSymbol
        exp = getattr(pos, "expiry_date", None) or self._entry_expiry_date
        if not exp:
            return
        syms = []
        for side, leg in (("CE", pos.ce_leg), ("PE", pos.pe_leg)):
            if leg and getattr(leg, "strike", 0) > 0:
                syms.append(_M.to_delta_symbol(InternalSymbol(self._underlying, float(leg.strike), side, exp)))
        if syms:
            self._delta_chain.unpin_symbols(self._underlying, syms)

    async def _unsubscribe_entry_expiry_tokens(self) -> None:
        """Clean up tokens that were subscribed solely for the entry-expiry window."""
        if not getattr(self, "_entry_expiry_tokens", None):
            return
        if self._rebalancer:
            feeder = getattr(self._rebalancer, "_feeder", None)
            if feeder:
                try:
                    await feeder.unsubscribe_tokens(self._entry_expiry_tokens)
                except Exception:
                    pass
        self._entry_expiry_tokens = []

    async def liquidate(self, reason: str = "kill_switch") -> None:
        """Emergency close of any open position. Used by kill-switch, deployment removal
        and graceful shutdown so broker positions are not stranded."""
        if not (self._position and self._position.status == "open"):
            return
        _cid = self._client_id or "-"
        _bid = self._binding_id or "-"
        logger.warning(
            "SellStraddle[%s|%s|%s]: LIQUIDATE received (%s) — closing open position.",
            self._underlying, _cid, _bid, reason,
        )
        await self._close_position(reason)

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()

    async def stop_async(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._unsubscribe_all()

    def reset_session(self) -> None:
        self._trades_today = 0
        self._position = None
        self._sl_cooldown_until = None
        self._market_open_dt = None
        self._primed = False
        self._session_realized_pnl_pts = 0.0
        self._initial_net_credit = 0.0
        self._initial_entry_time_value = 0.0
        self._stop_for_day = False
        self._prem_closes.clear()
        self._prem_volumes.clear()
        self._chart_series.clear()
        self._chart_last_min = None
        self._idx_highs.clear()
        self._idx_lows.clear()
        self._idx_closes.clear()
        self._last_exit_rules_bucket = ""
        self._last_entry_bucket_b = ""
        self._last_entry_bucket_r = ""
        self._strike_prem.clear()
        self._prev_atp_closed.clear()
        # Recompute effective entry expiry for the new session/day.
        self._entry_expiry_date = self._effective_entry_expiry()
        try:
            import asyncio as _aio
            loop = _aio.get_running_loop()
            loop.create_task(self._unsubscribe_entry_expiry_tokens())
        except RuntimeError:
            pass
        logger.info("SellStraddleStrategy[%s]: session reset. entry_expiry=%s",
                    self._underlying,
                    self._entry_expiry_date.isoformat() if self._entry_expiry_date else None)

    # ── EventBus loops ────────────────────────────────────────────────────────

    async def _candle_loop(self) -> None:
        q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        self._loop_queues["candle"] = q
        try:
            while self._running:
                try:
                    ev: CandleEvent = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if ev.symbol != self._underlying:
                    continue
                try:
                    await self._on_candle(ev)
                except Exception as exc:
                    logger.exception("SellStraddle[%s]: _on_candle error: %s", self._underlying, exc)
        finally:
            self._bus.unsubscribe(Topic.CANDLE_CLOSE, q)
            self._loop_queues.pop("candle", None)

    async def _tick_loop(self) -> None:
        from data_layer.base_feeder import IndexTick
        import time as _t
        q = self._bus.subscribe(Topic.INDEX_TICK)
        self._loop_queues["tick"] = q
        _idx_count = 0
        _last_hb = 0.0
        try:
            while self._running:
                try:
                    tick: IndexTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if tick.symbol != self._underlying:
                    continue
                # 2026-08-06 HIGH-priority fix: Day%/theta and the ITM-pair-gate's
                # both-ITM check all trust self._spot with zero validation -- a
                # single garbage/glitched index tick (decimal misprint, wrong
                # instrument leaking in) could misclassify both legs as ITM or
                # distort the theta split enough to fire a real close/roll off
                # one bad tick, before the next real tick corrects it. Reject a
                # single-tick jump > 20% vs the last accepted spot (genuine NSE/
                # crypto index moves essentially never do this in one tick) --
                # but with a safety valve: after 5 consecutive rejections, accept
                # anyway rather than risk getting permanently stuck on a stale
                # value if the market genuinely gapped that far.
                _new_spot = float(tick.ltp or 0.0)
                if _new_spot > 0:
                    if (self._spot > 0 and self._spot_reject_streak < 5
                            and abs(_new_spot - self._spot) / self._spot > 0.20):
                        self._spot_reject_streak += 1
                        logger.warning(
                            "SellStraddle[%s]: SUSPECT index tick spot=%.2f vs last=%.2f "
                            "(%.1f%% jump) -- ignoring for this tick (streak=%d/5).",
                            self._underlying, _new_spot, self._spot,
                            abs(_new_spot - self._spot) / self._spot * 100,
                            self._spot_reject_streak,
                        )
                    else:
                        self._spot_reject_streak = 0
                        self._spot = _new_spot
                _idx_count += 1
                try:
                    self._append_chart_point(datetime.now(IST))
                except Exception:
                    pass
                _now_m = _t.monotonic()
                if _now_m - _last_hb >= 60.0:
                    _last_hb = _now_m
                    # Crypto daily expiry rolls at 17:30 IST. Refresh the effective entry
                    # expiry periodically so we don't ignore new-expiry ticks after rollover.
                    if self._is_crypto:
                        _new_exp = self._effective_entry_expiry()
                        if _new_exp and _new_exp != self._entry_expiry_date:
                            logger.info(
                                "SellStraddle[%s]: crypto expiry rollover detected "
                                "entry_expiry %s -> %s",
                                self._underlying,
                                self._entry_expiry_date.isoformat() if self._entry_expiry_date else None,
                                _new_exp.isoformat(),
                            )
                            self._entry_expiry_date = _new_exp
                    if self._position and self._position.status == "open":
                        _state = "position OPEN — exit-checking"
                    elif self._sl_cooldown_until and datetime.now(IST) < self._sl_cooldown_until:
                        _left = int((self._sl_cooldown_until - datetime.now(IST)).total_seconds())
                        _strikes = len(getattr(self._pool_engine, "_closes", {}) or {})
                        _state = (f"COOLDOWN active — re-entry at "
                                  f"{self._sl_cooldown_until.strftime('%H:%M:%S')} ({_left}s left) | "
                                  f"data flowing: {_strikes} pool strikes tracked")
                    elif not self._is_in_entry_window(datetime.now(IST)):
                        _state = (f"SLEEP until {self._entry_start.strftime('%H:%M')} "
                                  f"(trades_today={self._trades_today} "
                                  f"stop_for_day={self._stop_for_day})")
                    else:
                        _state = (f"no position — entry path (trades_today={self._trades_today} "
                                  f"stop_for_day={self._stop_for_day} term={self._any_active_terminal()})")
                    self._clog.info("IDX_TICKS: %d index ticks/60s spot=%.2f | %s",
                                    _idx_count, self._spot, _state)
                    _idx_count = 0
                    # Republish position state every heartbeat so late-connecting browsers
                    # see the open position without waiting for the next entry/exit event.
                    if self._position and self._position.status == "open":
                        self.notify_position_update(self._position.to_dict(), force=True)
                try:
                    if self._position and self._position.status == "open":
                        await self._check_exits()
                    else:
                        await self._maybe_try_entry(datetime.now(IST))
                except Exception as _exc:
                    logger.exception("SellStraddle[%s]: tick-handler error (recovered, engine alive): %s",
                                     self._underlying, _exc)
        finally:
            self._bus.unsubscribe(Topic.INDEX_TICK, q)
            self._loop_queues.pop("tick", None)

    async def _fill_loop(self) -> None:
        from execution_bridge.straddle_bridge import StraddleFillEvent
        q = self._bus.subscribe(Topic.ORDER_FILL)
        self._loop_queues["fill"] = q
        try:
            while self._running:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if not isinstance(ev, StraddleFillEvent):
                    continue
                if ev.underlying != self._underlying:
                    continue
                # 2026-08-06 CRITICAL FIX: Topic.ORDER_FILL is broadcast to every
                # subscriber -- EventBus.publish() has no per-book routing. When two
                # different (client, binding) books trade the SAME underlying
                # concurrently (confirmed real today: ssrajpal2001/SA5770 and
                # gurmeet/zerodha both running sell_straddle on NIFTY), a fill event
                # for ONE book's order was being delivered to and processed by BOTH
                # books' _on_fill, because only `underlying` was ever checked. The
                # entry_aborted branch unconditionally nulls self._position with no
                # further guard -- one client's broker timeout/asymmetric-fill abort
                # was silently wiping a completely unrelated client's real, already-
                # confirmed position. Root cause of the "confirmed real position
                # vanishes within seconds, no matching log for THIS book" incidents
                # investigated throughout 2026-08-06.
                if ev.client_id != self._client_id or ev.binding_id != self._binding_id:
                    continue
                try:
                    self._on_fill(ev)
                except Exception as _exc:
                    logger.exception(
                        "SellStraddle[%s]: _on_fill error (recovered, fill loop alive): %s",
                        self._underlying, _exc,
                    )
                    # If the abort path left us without a position but the pending
                    # flag is still set, clear it so future entries are not blocked.
                    if self._position is None:
                        self._order_pending = False
        finally:
            self._bus.unsubscribe(Topic.ORDER_FILL, q)
            self._loop_queues.pop("fill", None)

    def _on_fill(self, fill) -> None:
        try:
            if fill.action == "ENTRY":
                _routing_failed = getattr(fill, "routing_failed", False)
                if getattr(fill, "entry_aborted", False) or _routing_failed:
                    _reason = "routing failed" if _routing_failed else "asymmetric fill"
                    # 2026-08-06 CRITICAL FIX: this branch was written for a full 2-leg
                    # ENTRY (BEGINNING/RE-ENTRY) and reused verbatim for a single-leg
                    # roll-reopen (_open_leg, called mid-roll after the old leg already
                    # closed for real). Unconditionally nulling self._position here for
                    # a single-leg abort would discard tracking of the OTHER leg, which
                    # is still genuinely open at the broker -- an orphaned real position
                    # the engine then believes is flat and could double up on. A fill
                    # carries legs=[side] (length 1) ONLY from _open_leg's roll-reopen
                    # call; every full-entry fill carries the 2-element default.
                    _legs = list(getattr(fill, "legs", ["CE", "PE"]) or [])
                    if len(_legs) == 1 and self._position is not None and self._position.status == "open":
                        asyncio.create_task(self._abort_roll_reopen(fill))
                        return
                    logger.error(
                        "SellStraddle[%s]: ENTRY ABORTED (%s) — discarding optimistic position. [%s/%s]",
                        self._underlying, _reason, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                    )
                    self._position = None
                    self._trades_today = max(0, self._trades_today - 1)
                    self._order_pending = False
                    self._roll_in_progress = False
                    self._persist()
                    # Routing failures carry no broker risk; cooldown only for real asymmetric fills.
                    if not _routing_failed:
                        self._apply_sl_cooldown()
                    return
                if self._position and self._position.status == "open":
                    _legs = getattr(fill, "legs", ["CE", "PE"])
                    if "CE" in _legs and fill.ce_fill and fill.ce_fill > 0:
                        self._position.ce_leg.ltp = fill.ce_fill
                        self._position.ce_leg.entry_price = fill.ce_fill
                        if getattr(fill, "ce_symbol", ""):
                            self._position.ce_leg.symbol = fill.ce_symbol
                    if "PE" in _legs and fill.pe_fill and fill.pe_fill > 0:
                        self._position.pe_leg.ltp = fill.pe_fill
                        self._position.pe_leg.entry_price = fill.pe_fill
                        if getattr(fill, "pe_symbol", ""):
                            self._position.pe_leg.symbol = fill.pe_symbol
                    self._position.net_credit = self._position.ce_leg.entry_price + self._position.pe_leg.entry_price
                    self._persist()
                    self.notify_position_update(self._position.to_dict(), force=True)
                    _ce_disp = self._position.ce_leg.symbol or f"CE{int(self._position.ce_leg.strike)}"
                    _pe_disp = self._position.pe_leg.symbol or f"PE{int(self._position.pe_leg.strike)}"
                    logger.info(
                        "SellStraddle[%s|%s|%s]: ENTRY confirmed — %s=%.2f %s=%.2f credit=%.2f legs=%s",
                        self._underlying, fill.client_id, fill.binding_id,
                        _ce_disp, self._position.ce_leg.entry_price,
                        _pe_disp, self._position.pe_leg.entry_price,
                        self._position.net_credit, _legs,
                    )
                    self._clog.info(
                        "ENTRY confirmed — %s=%.2f %s=%.2f credit=%.2f legs=%s",
                        _ce_disp, self._position.ce_leg.entry_price,
                        _pe_disp, self._position.pe_leg.entry_price,
                        self._position.net_credit, _legs,
                    )
                self._roll_in_progress = False
                self._order_pending = False
            elif fill.action == "EXIT":
                _exit_legs = getattr(fill, "legs", ["CE", "PE"])
                _legtag = "+".join(sorted(_exit_legs)) if set(_exit_legs) != {"CE", "PE"} else "CE+PE"
                _eid = getattr(fill, "event_id", "")
                if getattr(fill, "exit_aborted", False):
                    # Broker unavailable / order never confirmed. Do NOT finalize anything here --
                    # _close_position / _close_leg (the waiter below wakes them) are responsible
                    # for leaving the position exactly as it was and retrying later. Never treat
                    # this as a real close (2026-08-04 incident: bridge faked a successful EXIT).
                    logger.error(
                        "SellStraddle[%s|%s|%s]: EXIT ABORTED (broker unavailable) — legs=%s "
                        "event_id=%s. Position stays OPEN; will be retried.",
                        self._underlying, fill.client_id, fill.binding_id, _legtag, _eid,
                    )
                    self._clog.error("EXIT ABORTED (broker unavailable) — legs=%s event_id=%s",
                                      _legtag, _eid)
                else:
                    logger.info(
                        "SellStraddle[%s|%s|%s]: EXIT confirmed — legs=%s CE=%.2f PE=%.2f",
                        self._underlying, fill.client_id, fill.binding_id,
                        _legtag, fill.ce_fill, fill.pe_fill,
                    )
                    self._clog.info(
                        "EXIT confirmed — legs=%s CE=%.2f PE=%.2f",
                        _legtag, fill.ce_fill, fill.pe_fill,
                    )
                self._order_pending = False
                # Record the fill so the waiting _close_position/_close_leg can tell a real
                # confirmed exit apart from an exit_aborted one, then wake it. Use .get() (not
                # .pop()) on the waiter so a fill that arrives before the waiter is registered
                # still leaves the event set when the closing routine checks it.
                if _eid:
                    self._roll_close_results[_eid] = fill
                waiter = self._roll_close_waiters.get(_eid)
                if waiter is not None:
                    try:
                        waiter.set()
                    except RuntimeError:
                        pass
        except Exception as _exc:
            logger.exception(
                "SellStraddle[%s]: _on_fill error (recovered): %s",
                self._underlying, _exc,
            )
            # If we no longer have a position but the pending flag is still set,
            # unblock future entries so a malformed fill cannot deadlock the book.
            if self._position is None:
                self._order_pending = False

    async def _option_loop(self) -> None:
        from data_layer.base_feeder import OptionTick
        q = self._bus.subscribe(Topic.OPTION_TICK)
        self._loop_queues["option"] = q
        _tick_count = 0
        _last_log_ts = 0.0
        import time as _time
        try:
            while self._running:
                try:
                    tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if tick.underlying != self._underlying:
                    continue
                _tick_count += 1
                now_ts = _time.monotonic()
                if now_ts - _last_log_ts >= 60.0:
                    _step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
                    _atm = int(round(self._spot / _step) * _step) if self._spot > 0 else 0
                    self._clog.info("OPT_TICKS: %d option ticks/60s  ATM=%d  CE%d=%.2f PE%d=%.2f",
                                    _tick_count, _atm, _atm, self._ce_ltp, _atm, self._pe_ltp)
                    _tick_count = 0
                    _last_log_ts = now_ts
                step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
                atm = round(self._spot / step) * step if self._spot > 0 else 0

                # Only populate the internal strike_prem from the effective entry expiry.
                # This prevents current-expiry ticks from polluting data on expiry day.
                _entry_exp_ok = (self._entry_expiry_date is None or
                                 tick.expiry == self._entry_expiry_date)

                if tick.ltp > 0 and _entry_exp_ok:
                    _k = (int(tick.strike), tick.option_type)
                    _a = float(getattr(tick, "atp", 0.0) or 0.0)
                    entry = self._strike_prem.get(_k)
                    if entry is None:
                        self._strike_prem[_k] = {"ltp": float(tick.ltp), "atp": _a}
                    else:
                        entry["ltp"] = float(tick.ltp)
                        if _a > 0:
                            entry["atp"] = _a
                    _eng_atp = float(self._strike_prem[_k].get("atp", 0.0) or 0.0)
                    self._pool_engine.update_tick(
                        int(tick.strike), tick.option_type,
                        ltp=float(tick.ltp), atp=_eng_atp)
                if atm > 0 and tick.ltp > 0 and _entry_exp_ok and abs(tick.strike - atm) < step / 2:
                    _atp = float(getattr(tick, "atp", 0.0) or 0.0)
                    if tick.option_type == "CE":
                        self._ce_ltp = tick.ltp
                        if _atp > 0:
                            self._ce_atp = _atp
                    elif tick.option_type == "PE":
                        self._pe_ltp = tick.ltp
                        if _atp > 0:
                            self._pe_atp = _atp
                if self._position and self._position.status == "open":
                    pos = self._position
                    # Ignore ticks from a different expiry than the position's.
                    if tick.expiry != pos.expiry_date:
                        continue
                    _mk = float(getattr(tick, "atp", 0.0) or 0.0)
                    if tick.option_type == "CE" and abs(tick.strike - pos.ce_leg.strike) < 0.01:
                        pos.ce_leg.ltp = tick.ltp
                        if _mk > 0:
                            pos.ce_leg.mark = _mk
                        self._ce_ltp_fresh = True
                    elif tick.option_type == "PE" and abs(tick.strike - pos.pe_leg.strike) < 0.01:
                        pos.pe_leg.ltp = tick.ltp
                        if _mk > 0:
                            pos.pe_leg.mark = _mk
                        self._pe_ltp_fresh = True
        finally:
            self._bus.unsubscribe(Topic.OPTION_TICK, q)
            self._loop_queues.pop("option", None)

    # ── Candle processing ─────────────────────────────────────────────────────

    async def _on_candle(self, ev: CandleEvent) -> None:
        if getattr(ev, "timeframe", 1) != 1:
            return

        now = datetime.now(IST)
        self._load_thresholds()

        if self._market_open_dt is not None and self._session_day(self._market_open_dt) != self._session_day(now):
            logger.info(
                "SellStraddle[%s]: new %s detected (%s→%s) — resetting session state.",
                self._underlying, "expiry-day (17:30 IST)" if self._is_crypto else "day",
                self._session_day(self._market_open_dt), self._session_day(now),
            )
            self.reset_session()

        if self._market_open_dt is None or self._session_day(self._market_open_dt) != self._session_day(now):
            _mcx = set(getattr(self._cfg, "mcx_underlyings", ())) if self._cfg else set()
            if self._is_crypto:
                self._market_open_dt = now.replace(second=0, microsecond=0)
            else:
                _open = dtime(9, 0) if self._underlying in _mcx else _MARKET_OPEN
                self._market_open_dt = now.replace(
                    hour=_open.hour, minute=_open.minute, second=0, microsecond=0,
                )
            self._primed = False

        self._idx_highs.append(float(ev.high))
        self._idx_lows.append(float(ev.low))
        self._idx_closes.append(float(ev.close))
        _c, _p, _, _ = self._active_premium()
        combined = _c + _p
        if combined > 0:
            self._prem_closes.append(combined)
            self._prem_volumes.append(float(ev.volume) if ev.volume else 1.0)

        self._pool_engine.commit_bar(minute=ev.timestamp.hour * 60 + ev.timestamp.minute)

        if logger.isEnabledFor(logging.DEBUG) and self._position:
            _pos = self._position
            _pi = self._pool_engine.pair_indicators(
                int(_pos.ce_leg.strike), int(_pos.pe_leg.strike))
            if _pi:
                logger.debug(
                    "CANDLE[%s] t=%s | VWAP=%.2f SLOPE=%.4f RSI=%.1f ROC=%.2f "
                    "close=%.2f | CE=%.2f PE=%.2f pnl=%.2f",
                    self._underlying, ev.timestamp.strftime("%H:%M"),
                    _pi.get("vwap", 0), _pi.get("slope", 0), _pi.get("rsi", 0),
                    _pi.get("roc", 0), _pi.get("close", 0),
                    _pos.ce_leg.ltp, _pos.pe_leg.ltp, _pos.unrealized_pnl,
                )

        self._recompute_indicators()

        self._append_chart_point(ev.timestamp)

        if self._past_squareoff(now):
            if self._position and self._position.status == "open":
                await self._close_position("time_exit_eod")
            return

        for _k, _v in self._strike_prem.items():
            _a = _v.get("atp", 0.0)
            if _a and _a > 0:
                self._prev_atp_closed[_k] = _a

    # ── Session / timing helpers ──────────────────────────────────────────────

    def _session_day(self, when: datetime):
        if self._is_crypto:
            from datetime import timedelta as _td
            return when.date() if when.time() >= self._entry_start else (when.date() - _td(days=1))
        return when.date()

    def _is_in_entry_window(self, now: datetime) -> bool:
        t = now.time()
        if self._is_crypto:
            return not (self._entry_cutoff <= t < self._entry_start)
        return self._entry_start <= t < self._entry_cutoff

    def _past_squareoff(self, now: datetime) -> bool:
        t = now.time()
        if self._is_crypto:
            return self._force_exit <= t < self._entry_start
        return t >= self._force_exit

    # ── Public accessors ─────────────────────────────────────────────────────

    @property
    def has_open_position(self) -> bool:
        return self._position is not None and self._position.status == "open"

    @property
    def position(self) -> Optional[StraddlePosition]:
        return self._position

    @property
    def trades_today(self) -> int:
        return self._trades_today

    @property
    def indicators(self) -> Dict[str, float]:
        return dict(self._ind)
