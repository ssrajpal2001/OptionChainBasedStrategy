"""
strategies/hourly_breakout/book.py — live wrapper around HourlyBreakoutStrategy.

One book per (client, binding, underlying).  Subscribes to spot and ATM CE/PE
5-minute candle closes, builds 1-hour resampled frames, and drives the pure
strategy class.  Entry signals are published to ``Topic.ORDER_REQUEST``.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Deque, Dict, List, Optional

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import CandleEvent
from data_layer.instrument_registry import REGISTRY
from strategies.core import OrderEmitter
from strategies.core.base_book import AbstractStrategyBook
from strategies.hourly_breakout.strategy import HourlyBreakoutSignal, HourlyBreakoutStrategy, Side

logger = logging.getLogger(__name__)

_EOD_SQUAREOFF_TIME = time(15, 25)


@dataclass
class HourlyBreakoutOrderEvent:
    """Normalised order event published on ``Topic.ORDER_REQUEST``."""
    client_id: str
    binding_id: str
    strategy: str
    side: str                       # CE | PE
    action: str                     # BUY | SELL
    quantity: int
    entry_price: float
    sl_price: float
    target_price: float
    trigger_timestamp: datetime
    reason: str
    underlying: str = ""
    option_symbol: str = ""
    order_type: str = "LIMIT"


class HourlyBreakoutBook(AbstractStrategyBook):
    """
    Live book for the hourly-breakout strategy.

    Keeps rolling 5-minute and 1-hour OHLCV frames for spot + one ATM CE + one
    ATM PE, and feeds them to ``HourlyBreakoutStrategy``.
    """

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        sl_buffer_pts: float = 2.0,
        min_rr: float = 1.5,
        max_spread_pct: float = 1.5,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)

        self._lot_multiplier = max(1, lot_multiplier)
        self._strike_step = cfg.exchange_config.strike_steps.get(underlying, 50.0)
        self._lot_size = cfg.exchange_config.lot_sizes.get(underlying, 1)

        self._strategy = HourlyBreakoutStrategy(
            underlying=underlying,
            lot_size=self._lot_size,
            lot_multiplier=self._lot_multiplier,
            sl_buffer_pts=sl_buffer_pts,
            min_rr=min_rr,
            max_spread_pct=max_spread_pct,
        )
        self._emitter = OrderEmitter(bus, client_id, binding_id) if bus is not None else None

        self._5m: Dict[str, Deque[dict]] = {
            "spot": deque(maxlen=300), "CE": deque(maxlen=300), "PE": deque(maxlen=300)
        }
        self._1h: Dict[str, Deque[dict]] = {
            "spot": deque(maxlen=120), "CE": deque(maxlen=120), "PE": deque(maxlen=120)
        }

        self._spot_symbol = f"NSE_INDEX|{underlying}"
        self._option_symbols: Dict[str, str] = {}
        self._last_spot_ltp: Optional[float] = None
        self._day_done = False

    # ── public lifecycle ─────────────────────────────────────────────────────
    def start(self) -> None:
        super().start()
        self._subscribe(Topic.CANDLE_CLOSE)
        self._tasks.append(asyncio.create_task(self._candle_loop(), name=f"hb_candle_{self._underlying}"))
        self._tasks.append(asyncio.create_task(self._eod_loop(), name=f"hb_eod_{self._underlying}"))
        logger.info("HourlyBreakoutBook[%s/%s/%s] started.", self._client_id, self._binding_id, self._underlying)

    def reset_session(self) -> None:
        from strategies.hourly_breakout.strategy import HourlyBreakoutStrategy as _HB
        self._strategy = _HB(
            underlying=self._underlying,
            lot_size=self._lot_size,
            lot_multiplier=self._lot_multiplier,
        )
        for q in self._5m.values():
            q.clear()
        for q in self._1h.values():
            q.clear()
        self._option_symbols.clear()
        self._last_spot_ltp = None
        self._day_done = False

    # ── feed loops ───────────────────────────────────────────────────────────
    async def _candle_loop(self) -> None:
        q = self._loop_queues.get(Topic.CANDLE_CLOSE)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                self._on_candle(ev)
            except Exception as exc:
                logger.exception("HourlyBreakoutBook[%s]: candle handling error: %s", self._underlying, exc)

    async def _eod_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            now = datetime.now(IST)
            if now.time() >= _EOD_SQUAREOFF_TIME and not self._day_done:
                if self._strategy.is_in_position:
                    await self._squareoff("eod_squareoff")
                self._day_done = True

    # ── candle handling ──────────────────────────────────────────────────────
    def _on_candle(self, ev: CandleEvent) -> None:
        if not isinstance(ev, CandleEvent):
            return

        if ev.symbol == self._spot_symbol and ev.timeframe == 5:
            self._last_spot_ltp = ev.close
            self._ingest_5m("spot", ev)
            self._maybe_roll_1h("spot")
            self._update_option_subscriptions()

        for side in ("CE", "PE"):
            if ev.symbol == self._option_symbols.get(side) and ev.timeframe == 5:
                self._ingest_5m(side, ev)
                self._maybe_roll_1h(side)

        self._drive_strategy()

    def _ingest_5m(self, key: str, ev: CandleEvent) -> None:
        self._5m[key].append({
            "open": ev.open,
            "high": ev.high,
            "low": ev.low,
            "close": ev.close,
            "volume": ev.volume,
        })

    def _maybe_roll_1h(self, key: str) -> None:
        if len(self._5m[key]) < 12:
            return
        df5 = pd.DataFrame(self._5m[key])
        df5.index = pd.date_range(end=datetime.now(IST), periods=len(df5), freq="5min")
        df1 = df5.resample("1h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
        }).dropna()
        self._1h[key].clear()
        for ts, row in df1.iterrows():
            self._1h[key].append({
                "open": row["open"], "high": row["high"], "low": row["low"],
                "close": row["close"], "volume": row["volume"],
            })

    def _df_from_deque(self, q: Deque[dict]) -> pd.DataFrame:
        df = pd.DataFrame(q)
        if df.empty:
            return df
        df.index = pd.date_range(end=datetime.now(IST), periods=len(df), freq="5min")
        return df

    def _drive_strategy(self) -> None:
        spot5 = self._df_from_deque(self._5m["spot"])
        ce5 = self._df_from_deque(self._5m["CE"])
        pe5 = self._df_from_deque(self._5m["PE"])
        if spot5.empty or ce5.empty or pe5.empty:
            return

        spot1h = self._df_from_deque(self._1h["spot"])
        ce1h = self._df_from_deque(self._1h["CE"])
        pe1h = self._df_from_deque(self._1h["PE"])

        self._strategy.on_1h_candle_close(spot1h, ce1h, pe1h)
        self._strategy.on_5m_candle_close(spot5, ce5, pe5)

        for sig in self._strategy.get_active_orders_and_signals():
            self._handle_signal(sig)

    # ── option subscription / symbol resolution ────────────────────────────────
    def _update_option_subscriptions(self) -> None:
        if self._last_spot_ltp is None or self._option_symbols:
            return
        today = datetime.now(IST).date()
        expiry = REGISTRY.get_active_expiry(self._underlying, today)
        if not expiry:
            return
        atm = round(self._last_spot_ltp / self._strike_step) * self._strike_step
        for side in ("CE", "PE"):
            key = REGISTRY.get_upstox_key(self._underlying, expiry, int(atm), side)
            if key:
                self._option_symbols[side] = key
                logger.info("HourlyBreakoutBook[%s]: tracking %s %s strike=%.0f key=%s",
                            self._underlying, side, expiry, atm, key)

    # ── signal → order translation ───────────────────────────────────────────
    def _handle_signal(self, sig: HourlyBreakoutSignal) -> None:
        if sig.action != "ENTRY":
            return
        qty = self._lot_size * self._lot_multiplier
        ev = HourlyBreakoutOrderEvent(
            client_id=self._client_id,
            binding_id=self._binding_id,
            strategy="hourly_breakout",
            side=sig.side.value,
            action="BUY",
            quantity=qty,
            entry_price=sig.entry_price,
            sl_price=sig.sl_price,
            target_price=sig.target_price,
            trigger_timestamp=sig.trigger_timestamp,
            reason=sig.reason,
            underlying=self._underlying,
            option_symbol=sig.option_symbol or self._option_symbols.get(sig.side.value, ""),
            order_type="SL_LIMIT",
        )
        if self._emitter is not None:
            asyncio.create_task(self._emitter.emit(Topic.ORDER_REQUEST, ev))
            logger.info("HourlyBreakoutBook[%s]: emitted %s %s @ %.2f SL %.2f T %.2f reason=%s",
                        self._underlying, ev.action, ev.side, ev.entry_price, ev.sl_price, ev.target_price, ev.reason)

    async def _squareoff(self, reason: str) -> None:
        pos = self._strategy.position_summary
        if pos is None:
            return
        qty = self._lot_size * self._lot_multiplier
        ev = HourlyBreakoutOrderEvent(
            client_id=self._client_id,
            binding_id=self._binding_id,
            strategy="hourly_breakout",
            side=pos["side"],
            action="SELL",
            quantity=qty,
            entry_price=pos["entry_price"],
            sl_price=pos["sl_price"],
            target_price=pos["target_price"],
            trigger_timestamp=datetime.now(IST),
            reason=reason,
            underlying=self._underlying,
            option_symbol=self._option_symbols.get(pos["side"], ""),
            order_type="MARKET",
        )
        if self._emitter is not None:
            await self._emitter.emit(Topic.ORDER_REQUEST, ev)
        self._strategy._reset_scan()
        logger.info("HourlyBreakoutBook[%s]: EOD square-off %s reason=%s", self._underlying, pos["side"], reason)

    # Unused abstract placeholders
    async def _tick_loop(self) -> None:
        pass

    async def _option_loop(self) -> None:
        pass
