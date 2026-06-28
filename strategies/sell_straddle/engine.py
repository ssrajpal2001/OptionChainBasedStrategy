"""
strategies/sell_straddle/engine.py — SellStraddleStrategy orchestrator.

Inherits from ``AbstractStrategyBook`` and composes the indicator / entry / exit /
rolling mixins.  Owns the async feed loops, persistence, session lifecycle, and
public accessors.  Strategy-specific logic lives in the sibling modules.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic
from data_layer.base_feeder import CandleEvent, EventBus
# Indicator computations live in strategies.sell_straddle.indicators

from strategies.core import OrderEmitter, PositionStoreMixin
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


class SellStraddleStrategy(AbstractStrategyBook, PositionStoreMixin, ConfigMixin,
                           IndicatorMixin, EntryMixin, ExitMixin, RollingMixin):

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
        self._lot_multiplier = lot_multiplier
        self._client_db = None

        self._position: Optional[StraddlePosition] = None
        self._trades_today: int = 0

        self._spot: float = 0.0
        self._ce_ltp: float = 0.0
        self._pe_ltp: float = 0.0
        self._ce_atp: float = 0.0
        self._pe_atp: float = 0.0
        self._prev_vwap_atp: Optional[float] = None
        self._strike_prem: Dict[Tuple[int, str], dict] = {}
        self._prev_atp_closed: Dict[Tuple[int, str], float] = {}
        self._beginning_failed: bool = False
        self._ltp_target: float = 0.0

        self._guardrail_pnl_enabled: bool = False
        self._guardrail_pnl_target_pts: float = 0.0
        self._guardrail_pnl_sl_pts: float = 0.0

        self._guardrail_roc_enabled: bool = False
        self._guardrail_roc_tf: int = 15
        self._guardrail_roc_length: int = 9
        self._guardrail_roc_target: float = -20.0
        self._guardrail_roc_stoploss: float = 10.0
        self._last_roc_guard_bucket: str = ""

        self._market_open_dt: Optional[datetime] = None
        self._primed: bool = False
        self._order_pending: bool = False
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
        self._sl_cooldown_until: Optional[datetime] = None
        self._event_counter: int = 0
        self._order_emitter = OrderEmitter(self._bus, self._client_id, self._binding_id)

        self._prem_closes: deque = deque(maxlen=_BUF)
        self._prem_volumes: deque = deque(maxlen=_BUF)
        self._chart_series: deque = deque(maxlen=375)

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

    async def _emit_order(self, ev) -> None:
        """Stamp this book's identity on every order so the bridge routes to ONLY this binding."""
        await self._order_emitter.emit(Topic.ORDER_REQUEST, ev)

    def _persist(self) -> None:
        try:
            if self._position and self._position.status == "open":
                self.persist(self._persist_key, self._position.to_dict(),
                             product_type=getattr(self, "_product_type", "MIS"))
            else:
                self.clear(self._persist_key)
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
                logger.info("SellStraddle[%s]: restored session — booked=%.2f pts trades=%d stop_for_day=%s",
                            self._underlying, self._session_realized_pnl_pts, self._trades_today, self._stop_for_day)
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
            for _ in range(30):
                if self._spot > 0:
                    break
                await _aio.sleep(2)
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token or self._spot <= 0:
                logger.info("SellStraddle[%s]: pool seed skipped (no token/spot).", self._underlying)
                return
            step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
            ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
            itm = int(ss.get("pool_itm_depth", 4))
            otm = int(ss.get("pool_otm_depth", 4))
            strikes = pool_strike_set(self._spot, step, itm, otm)
            exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            seeded = 0
            seed_pairs: list = [(int(stk), side) for stk in strikes for side in ("CE", "PE")]
            pos = self._position
            if pos and pos.status == "open" and pos.ce_leg and pos.pe_leg:
                for _stk, _side in [(int(pos.ce_leg.strike), "CE"), (int(pos.pe_leg.strike), "PE")]:
                    if (_stk, _side) not in seed_pairs:
                        seed_pairs.append((_stk, _side))
            for stk, side in seed_pairs:
                ikey = REGISTRY.get_broker_symbol(self._underlying, exp, stk, side, "upstox")
                if not ikey:
                    continue
                bars = await fetch_upstox_warm_1m(ikey, token)
                if bars:
                    closes = [b["close"] for b in bars]
                    self._pool_engine.seed_strike(stk, side, closes, closes)
                    seeded += 1
            logger.info("SellStraddle[%s]: pool engine seeded %d legs (warm RSI/ROC).",
                        self._underlying, seeded)
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
            f"ENTRY BASIS:{self._entry_basis.upper()} | "
            f"floor:{(self._theta_target if self._entry_basis=='theta' else self._ltp_target):.0f}"
            f"({'theta' if self._entry_basis=='theta' else 'ltp'})",
            f"║ BEGINNING ENTRY: {beg}",
            f"║ RE-ENTRY GATES:  {ren}",
            f"║ ROLLOVERS: Decay:{'ON' if decay_on else 'OFF'}({self._ltp_exit_min:.0f}) | "
            f"Ratio:{'ON' if ratio_on else 'OFF'}({self._ratio_threshold:.1f}x"
            + (f" MaxEntry:{self._max_entry_ratio:.1f}x" if self._max_entry_ratio > 0 else "")
            + ") | SmartRoll:ON",
            f"║ TRAILING SL: {'ON' if self._trail_sl_enabled else 'OFF'} "
            f"Lock:{self._trail_lock_pct*100:.1f}% Floor:{self._trail_floor_pct*100:.1f}% "
            f"BASIS:{self._trail_basis.upper()}",
            f"║ SCALABLE TSL: {'ON' if self._tsl_enabled else 'OFF'} "
            f"Base:{self._tsl_base_profit_rs:.0f}/{self._tsl_base_lock_rs:.0f} "
            f"Step:{self._tsl_step_profit_rs:.0f}/{self._tsl_step_lock_rs:.0f} ({self._ccy_symbol}/BTC if crypto) "
            f"BASIS:{self._tsl_basis.upper()}",
            f"║ VWAP RISE SL: {'ON' if self._vwap_rise_enabled else 'OFF'}({self._vwap_rise_threshold:.2f}%) | "
            f"ROC GUARDRAIL: {'ON' if self._guardrail_roc_enabled else 'OFF'}"
            f"({self._guardrail_roc_tf}m T:{self._guardrail_roc_target}/SL:{self._guardrail_roc_stoploss})",
            f"║ PNL GUARDRAIL: {'ON' if self._guardrail_pnl_enabled else 'OFF'} "
            f"T:{self._guardrail_pnl_target_pts:.0f}pts SL:{self._guardrail_pnl_sl_pts:.0f}pts | "
            f"DAY: T:{self._day_profit_target_pct:.0f}% SL:{self._day_loss_sl_pct:.0f}% "
            f"BASIS:{self._day_exit_basis.upper()}",
            f"║ DYNAMIC EXITS: {exit_rules}",
            f"║ EXIT PRIORITY: EOD→PnLguard→Day%→LTPdecay→Ratio→ScalableTSL→ROC→VWAPrise→exit_rules",
            f"║ LIMITS: Max Daily Trades:{self._max_trades}",
            "╚══════════════════════════════════════════════════════════════════════",
        ]
        for line in L:
            logger.info(line)
            self._clog.info(line)

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
        self._last_roc_guard_bucket = ""
        self._strike_prem.clear()
        self._prev_atp_closed.clear()
        self._beginning_failed = False
        logger.info("SellStraddleStrategy[%s]: session reset.", self._underlying)

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
                self._spot = tick.ltp
                _idx_count += 1
                try:
                    self._append_chart_point(datetime.now(IST))
                except Exception:
                    pass
                _now_m = _t.monotonic()
                if _now_m - _last_hb >= 60.0:
                    _last_hb = _now_m
                    if self._position and self._position.status == "open":
                        _state = "position OPEN — exit-checking"
                    elif self._sl_cooldown_until and datetime.now(IST) < self._sl_cooldown_until:
                        _left = int((self._sl_cooldown_until - datetime.now(IST)).total_seconds())
                        _strikes = len(getattr(self._pool_engine, "_closes", {}) or {})
                        _state = (f"COOLDOWN active — re-entry at "
                                  f"{self._sl_cooldown_until.strftime('%H:%M:%S')} ({_left}s left) | "
                                  f"data flowing: {_strikes} pool strikes tracked")
                    else:
                        _state = (f"no position — entry path (trades_today={self._trades_today} "
                                  f"stop_for_day={self._stop_for_day} term={self._any_active_terminal()})")
                    self._clog.info("IDX_TICKS: %d index ticks/60s spot=%.2f | %s",
                                    _idx_count, self._spot, _state)
                    _idx_count = 0
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
                self._on_fill(ev)
        finally:
            self._bus.unsubscribe(Topic.ORDER_FILL, q)
            self._loop_queues.pop("fill", None)

    def _on_fill(self, fill) -> None:
        if fill.action == "ENTRY":
            if getattr(fill, "entry_aborted", False):
                logger.error(
                    "SellStraddle[%s]: ENTRY ABORTED by bridge (asymmetric fill) — discarding "
                    "optimistic position; broker leg(s) were flattened. [%s/%s]",
                    self._underlying, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                )
                self._position = None
                self._trades_today = max(0, self._trades_today - 1)
                self._order_pending = False
                self._persist()
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
                _ce_disp = self._position.ce_leg.symbol or f"CE{int(self._position.ce_leg.strike)}"
                _pe_disp = self._position.pe_leg.symbol or f"PE{int(self._position.pe_leg.strike)}"
                logger.info(
                    "SellStraddle[%s]: ENTRY confirmed — %s=%.2f %s=%.2f credit=%.2f [%s/%s] legs=%s",
                    self._underlying, _ce_disp, self._position.ce_leg.entry_price,
                    _pe_disp, self._position.pe_leg.entry_price,
                    self._position.net_credit, fill.client_id, fill.binding_id, _legs,
                )
            self._order_pending = False
        elif fill.action == "EXIT":
            logger.info(
                "SellStraddle[%s]: EXIT confirmed — CE=%.2f PE=%.2f [%s/%s]",
                self._underlying, fill.ce_fill, fill.pe_fill,
                fill.client_id, fill.binding_id,
            )
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
                if tick.ltp > 0:
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
                if atm > 0 and tick.ltp > 0 and abs(tick.strike - atm) < step / 2:
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
