"""
strategies/v4_cascade/book.py — V4CascadeBook: live bus/broker adapter
wrapping the pure V4CascadeEngine. One instance per (client, binding,
NIFTY) deployment, mirrors strategies/sell_straddle/engine.py's
SellStraddleStrategy structural pattern (AbstractStrategyBook subclass,
_subscribe()-tracked EventBus loops, set_client_db/set_rebalancer,
position_store persistence).

2026-07-19 go-live requirements implemented here:
  - Deep historical REST re-ingestion on boot (_ingest_history): resolves
    the monthly expiry, the CURRENT session's 09:15 spot open (via history,
    never waiting for a live tick — needed for mid-day boots), derives
    CE/PE tracking strikes (ATM-200/ATM+200, 100-pt rounding), then fetches
    ~3 weeks of 1-minute spot+CE+PE history and replays it through the same
    V4CascadeEngine.update() used for live ticks, rebuilding all unmitigated
    multi-day HTF/MTF zone state before going live. Also exposed as a
    public coroutine for the admin "Force Ingest Zones" button.
  - EOD square-off at 15:15 (market-order close of any open position) and a
    15:30 Gate 2/3 rollback (in-flight setups reset to HTF_LOCKED, HTF zones
    themselves left untouched) on every trading day, live or replayed.
  - Position persistence via data_layer.position_store — HTF/MTF zone state
    is NOT separately persisted; it's always rebuilt by the (idempotent,
    _known_ref_ts-deduped) historical replay on every boot instead.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import quote as _q

from config.global_config import IST, Topic
from data_layer import position_store
from data_layer.base_feeder import CandleEvent
from data_layer.historical_candles import _http_get_json, _parse_candles, fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY, is_monthly_expiry
from strategies.base_strategy import Direction, SignalPackage, StrategyID
from strategies.core.base_book import AbstractStrategyBook
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import CascadeEventType, CascadePosition
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS = 21          # 2 full weeks + current week-to-date headroom
_EOD_SQUARE_OFF = (15, 15)
_GATE23_RESET = (15, 30)
_STRIKE_STEP = 100.0
_TRACKING_OFFSET = 200.0

# ── Crypto (BTC/ETH via Delta) branch — 2026-07-19 weekend-validated ────────
# No option chain: CE and PE both track the underlying's OWN spot/perpetual
# price directly (a directional long-bias read, not a real option premium).
# Reuses the SAME EventBus CANDLE_CLOSE stream every other consumer already
# gets from matrix_engine/candle_cache.py (which builds 1/2/5/15/75m candles
# for ANY symbol receiving INDEX_TICK, including BTC via DeltaChainManager's
# perpetual feed) -- no separate tick/bucket-accumulation path needed.
# HTF/MTF are faster (15m/1m vs NIFTY's 75m/5m) to match BTC's pace, exactly
# as validated in scripts/v4_btc_weekend_test.py. No EOD/Gate2-3 daily reset
# for crypto -- BTC has no session boundary, multi-day zones persist
# continuously by design (matches the "long-term institutional footprint"
# philosophy the strategy is built around).
_CRYPTO_UNDERLYINGS = {"BTC", "ETH"}
_CRYPTO_HTF_MINUTES = 60   # 2026-07-19 — multiples of 60m for crypto (not 75m NIFTY, not 15m)
_CRYPTO_MTF_MINUTES = 1
_CRYPTO_LOOKBACK_DAYS = 10   # crypto is 24/7 -- 10 calendar days of history is enough to warm HTF zones
_CRYPTO_CONTRACT_VALUE = {"BTC": 0.001, "ETH": 0.01}   # 1 lot = this fraction of a coin (Delta's real contract_value)
_DELTA_BASE = "https://api.india.delta.exchange"
_DELTA_SYMBOL = {"BTC": "BTCUSD", "ETH": "ETHUSD"}


class _Bar:
    """Minimal CandleEvent-shaped object for internal bucket accumulation."""
    __slots__ = ("timestamp", "open", "high", "low", "close", "volume", "timeframe")

    def __init__(self, ts, o, h, l, c, v=0, tf=5):
        self.timestamp, self.open, self.high, self.low, self.close = ts, o, h, l, c
        self.volume, self.timeframe = v, tf


class V4CascadeBook(AbstractStrategyBook):
    def __init__(
        self, bus, cfg, underlying: str, client_id: str, binding_id: str,
        lot_multiplier: int = 1,
    ) -> None:
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        self._lot_multiplier = lot_multiplier
        self._db = None
        self._rebalancer = None
        self._is_crypto = underlying.upper() in _CRYPTO_UNDERLYINGS
        self._v4cfg = V4CascadeConfig(underlying=underlying, lot_multiplier=lot_multiplier)
        self._engine = V4CascadeEngine(self._v4cfg, pe_scans_bull=self._is_crypto)

        self._persist_key = f"{client_id}_{binding_id}_{underlying}_v4_cascade"
        self._expiry: Optional[date] = None
        self._atm_open: Optional[float] = None
        self._ce_strike: Optional[int] = None
        self._pe_strike: Optional[int] = None
        self._ce_symbol: str = ""
        self._pe_symbol: str = ""
        self._locked_ce_strike: Optional[int] = None   # admin manual override
        self._locked_pe_strike: Optional[int] = None

        self._session_day: Optional[date] = None
        self._history_ingested = False
        self._live_price: Dict[str, float] = {"CE": 0.0, "PE": 0.0}   # per-side last price — "distance to trap" in the UI

        # per-side 5m bucket accumulators, fed by live OPTION_TICK
        self._buckets: Dict[str, Optional[_Bar]] = {"CE": None, "PE": None}
        self._bars_5m: Dict[str, List] = {"CE": [], "PE": []}

    # ── injected deps (mirrors sell_straddle's set_client_db/set_rebalancer) ──
    def set_client_db(self, db) -> None:
        self._db = db

    def set_rebalancer(self, rebalancer) -> None:
        self._rebalancer = rebalancer

    def set_locked_strikes(self, ce_strike: Optional[int], pe_strike: Optional[int]) -> None:
        """Admin manual CE/PE override — bypasses auto ATM-200/+200 derivation."""
        self._locked_ce_strike = ce_strike
        self._locked_pe_strike = pe_strike

    # ── lifecycle ────────────────────────────────────────────────────────────
    def start(self) -> None:
        super().start()
        self._restore_position()
        self._tasks.append(asyncio.create_task(
            self._boot(), name=f"v4cascade_boot_{self._client_id}_{self._binding_id}"))

    def reset_session(self) -> None:
        self._engine.reset_session()
        self._session_day = None
        self._history_ingested = False
        self._buckets = {"CE": None, "PE": None}
        self._bars_5m = {"CE": [], "PE": []}

    async def _boot(self) -> None:
        try:
            await self._ingest_history()
        except Exception:
            logger.exception("V4CascadeBook[%s/%s/%s]: boot ingestion failed.",
                              self._client_id, self._binding_id, self._underlying)
        if self._is_crypto:
            self._tasks.append(asyncio.create_task(self._crypto_candle_loop(), name="v4cascade_crypto_candle"))
            self._tasks.append(asyncio.create_task(self._crypto_tick_loop(), name="v4cascade_crypto_tick"))
        else:
            self._tasks.append(asyncio.create_task(self._option_loop(), name="v4cascade_option"))
            self._tasks.append(asyncio.create_task(self._candle_loop(), name="v4cascade_candle"))

    # ── strike resolution ────────────────────────────────────────────────────
    def _access_token(self) -> str:
        if self._db is None:
            return ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox")
            return (creds or {}).get("access_token", "")
        except Exception:
            return ""

    def _fetch_session_open(self, token: str, day: date) -> Optional[float]:
        """The CURRENT session's 09:15 spot open, fetched via history (never
        waits for a live tick — required for mid-day boots)."""
        spot_key = REGISTRY.get_upstox_index_key(self._underlying)
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(spot_key, safe='')}/1minute/"
               f"{day.isoformat()}/{day.isoformat()}")
        try:
            rows = _parse_candles(_http_get_json(url, token))
        except Exception:
            rows = []
        if not rows:
            return None
        rows.sort(key=lambda r: r["ts"])
        return float(rows[0]["open"])

    def _resolve_expiry(self) -> Optional[date]:
        for exp in REGISTRY.all_expiries(self._underlying):
            if is_monthly_expiry(exp, self._underlying):
                return exp
        return REGISTRY.get_active_expiry(self._underlying)

    async def _resolve_symbols(self) -> bool:
        token = await asyncio.to_thread(self._access_token)
        if not token:
            logger.warning("V4CascadeBook[%s]: no Upstox access_token — cannot resolve strikes.",
                            self._underlying)
            return False
        today = datetime.now(IST).date()
        atm_open = await asyncio.to_thread(self._fetch_session_open, token, today)
        if atm_open is None:
            logger.warning("V4CascadeBook[%s]: could not fetch 09:15 session open for %s.",
                            self._underlying, today)
            return False
        self._atm_open = atm_open
        atm = round(atm_open / _STRIKE_STEP) * _STRIKE_STEP
        self._ce_strike = self._locked_ce_strike or int(atm - _TRACKING_OFFSET)
        self._pe_strike = self._locked_pe_strike or int(atm + _TRACKING_OFFSET)

        await asyncio.to_thread(REGISTRY.load_sync, self._underlying, token)
        self._expiry = self._resolve_expiry()
        if self._expiry is None:
            logger.warning("V4CascadeBook[%s]: no monthly expiry resolvable.", self._underlying)
            return False
        self._ce_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._ce_strike, "CE")
        self._pe_symbol = REGISTRY.get_upstox_key(self._underlying, self._expiry, self._pe_strike, "PE")
        logger.info("V4CascadeBook[%s/%s/%s]: ATM_open=%.2f CE=%d(%s) PE=%d(%s) expiry=%s",
                    self._underlying, self._client_id, self._binding_id, atm_open,
                    self._ce_strike, self._ce_symbol, self._pe_strike, self._pe_symbol, self._expiry)
        return bool(self._ce_symbol and self._pe_symbol)

    async def _subscribe_tracking_contracts(self) -> None:
        if not self._rebalancer:
            return
        feeder = getattr(self._rebalancer, "_feeder", None)
        if not feeder:
            return
        tokens = [t for t in (self._ce_symbol, self._pe_symbol) if t]
        if tokens:
            await feeder.subscribe_tokens(tokens)

    # ── deep historical re-ingestion (boot + admin "Force Ingest Zones") ────
    async def _ingest_history(self) -> bool:
        """Public — also called directly by the admin force-ingest endpoint
        on an already-running book. Idempotent: PremiumGateScanner de-dupes
        already-known HTF refs via _known_ref_ts, so re-running never
        duplicates zone state."""
        if self._is_crypto:
            return await self._ingest_history_crypto()
        ok = await self._resolve_symbols()
        if not ok:
            return False
        await self._subscribe_tracking_contracts()

        token = await asyncio.to_thread(self._access_token)
        if not token:
            return False
        today = datetime.now(IST).date()
        start = today - timedelta(days=_LOOKBACK_DAYS)

        spot_key = REGISTRY.get_upstox_index_key(self._underlying)
        spot_rows, ce_rows, pe_rows = await asyncio.gather(
            fetch_upstox_range_1m(spot_key, token, start, today),
            fetch_upstox_range_1m(self._ce_symbol, token, start, today),
            fetch_upstox_range_1m(self._pe_symbol, token, start, today),
        )
        spot_5m = _to_5m_bars(spot_rows, filter_zero_volume=False)
        ce_5m = _to_5m_bars(ce_rows, filter_zero_volume=True)
        pe_5m = _to_5m_bars(pe_rows, filter_zero_volume=True)
        self._bars_5m["CE"] = ce_5m
        self._bars_5m["PE"] = pe_5m

        _replay_through_engine(self._engine, spot_5m, ce_5m, pe_5m,
                                on_daily_boundary=self._apply_eod_gate23_rules)
        self._history_ingested = True
        self._persist_position()
        logger.info("V4CascadeBook[%s/%s/%s]: history ingested — spot=%d CE=%d PE=%d 5m bars.",
                    self._underlying, self._client_id, self._binding_id,
                    len(spot_5m), len(ce_5m), len(pe_5m))
        return True

    # ── crypto (BTC/ETH) spot-only path — 2026-07-19, see module header ─────
    def _delta_symbol(self) -> str:
        return _DELTA_SYMBOL.get(self._underlying.upper(), self._underlying.upper() + "USD")

    @staticmethod
    def _delta_get(url: str, params: dict) -> dict:
        from curl_cffi import requests as _cc
        try:
            return _cc.get(url, params=params, impersonate="chrome131", timeout=15).json()
        except Exception as exc:
            logger.debug("V4CascadeBook crypto http error: %s: %s", url, exc)
            return {}

    async def _ingest_history_crypto(self) -> bool:
        self._ce_strike = self._pe_strike = 0
        self._ce_symbol = self._pe_symbol = self._delta_symbol()
        end = int(datetime.now(IST).timestamp())
        start = end - _CRYPTO_LOOKBACK_DAYS * 86400
        data = await asyncio.to_thread(
            self._delta_get, f"{_DELTA_BASE}/v2/history/candles",
            {"resolution": "1m", "symbol": self._ce_symbol, "start": start, "end": end},
        )
        rows = sorted(data.get("result", []) or [], key=lambda c: c["time"])
        if not rows:
            logger.warning("V4CascadeBook[%s]: no Delta history returned — ingestion skipped.", self._underlying)
            return False
        bars_1m = [
            _Bar(datetime.fromtimestamp(int(c["time"]), tz=IST), float(c["open"]), float(c["high"]),
                 float(c["low"]), float(c["close"]), int(c.get("volume", 0) or 0), tf=5)
            for c in rows
        ]
        self._bars_5m["CE"] = list(bars_1m)
        self._bars_5m["PE"] = list(bars_1m)
        bars_htf = [_Bar(b.timestamp, b.close, b.high, b.low, b.close, tf=75)
                    for b in resample_bars(bars_1m, _CRYPTO_HTF_MINUTES)]
        htf_by_ts = {b.timestamp: b for b in bars_htf}

        for i, bar in enumerate(bars_1m):
            self._engine.update(ce_bar=bar, pe_bar=bar)
            if _bucket_end_1m(bar.timestamp, _CRYPTO_HTF_MINUTES):
                bstart = _bucket_start(bar.timestamp, _CRYPTO_HTF_MINUTES)
                htf_bar = htf_by_ts.get(bstart)
                if htf_bar is not None:
                    self._engine.update(spot_bar=htf_bar, ce_bar=htf_bar, pe_bar=htf_bar)
        if bars_1m:
            self._live_price["CE"] = self._live_price["PE"] = bars_1m[-1].close
        self._history_ingested = True
        self._persist_position()
        logger.info("V4CascadeBook[%s/%s/%s]: crypto history ingested — %d x 1m bars, "
                    "CE setups=%d PE setups=%d.",
                    self._underlying, self._client_id, self._binding_id, len(bars_1m),
                    len(self._engine._scanners["CE"].setups), len(self._engine._scanners["PE"].setups))
        return True

    async def _crypto_candle_loop(self) -> None:
        """No option chain for crypto — CE/PE both track the underlying's OWN
        live price directly (Topic.CANDLE_CLOSE, already published for BTC by
        matrix_engine/candle_cache.py off DeltaChainManager's perpetual
        IndexTick — no separate subscription/bucket-building needed)."""
        q = self._subscribe(Topic.CANDLE_CLOSE)
        symbol = self._underlying.upper()
        buf_1m: List = list(self._bars_5m.get("CE") or [])
        while self._running:
            try:
                ev: CandleEvent = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if getattr(ev, "symbol", "") != symbol or getattr(ev, "timeframe", 0) != 1:
                continue
            bar = _Bar(ev.timestamp, ev.open, ev.high, ev.low, ev.close, ev.volume, tf=5)
            self._live_price["CE"] = self._live_price["PE"] = bar.close
            buf_1m.append(bar)
            self._bars_5m["CE"].append(bar)
            self._bars_5m["PE"].append(bar)
            events = self._engine.update(ce_bar=bar, pe_bar=bar)
            for order_ev in events:
                self._emit_order(order_ev)

            if _bucket_end_1m(bar.timestamp, _CRYPTO_HTF_MINUTES):
                bstart = _bucket_start(bar.timestamp, _CRYPTO_HTF_MINUTES)
                recent = [b for b in buf_1m if b.timestamp >= bstart]
                htf_bars = resample_bars(recent, _CRYPTO_HTF_MINUTES)
                if htf_bars:
                    last = htf_bars[-1]
                    htf_bar = _Bar(last.timestamp, last.close, last.high, last.low, last.close, tf=75)
                    self._engine.update(spot_bar=htf_bar, ce_bar=htf_bar, pe_bar=htf_bar)
            self._persist_position()
            # trim the rolling 1m buffer so it doesn't grow unbounded across days
            if len(buf_1m) > 60 * 24 * (_CRYPTO_LOOKBACK_DAYS + 1):
                buf_1m = buf_1m[-60 * 24 * (_CRYPTO_LOOKBACK_DAYS + 1):]

    async def _crypto_tick_loop(self) -> None:
        """Pure display-layer live-price updater. The funnel engine only reacts
        to closed 1m bars (via _crypto_candle_loop) — that's correct and
        unchanged. But the UI's SPOT/LTP + 'distance to trap' figures looked
        frozen for up to a minute at a time since _live_price only moved on
        bar close. Subscribing raw INDEX_TICK here updates _live_price on
        every tick instead, same as the NIFTY path already does via
        _on_option_tick — this never touches engine.update() or any gate
        state, so it cannot affect trade logic, only what the client card
        displays between bar closes."""
        q = self._subscribe(Topic.INDEX_TICK)
        symbol = self._underlying.upper()
        while self._running:
            try:
                tick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if getattr(tick, "symbol", "") != symbol:
                continue
            ltp = float(getattr(tick, "ltp", 0.0) or 0.0)
            if ltp > 0:
                self._live_price["CE"] = self._live_price["PE"] = ltp

    async def force_ingest(self) -> bool:
        """Admin-triggered re-ingestion on a live book (POST .../force_ingest)."""
        return await self._ingest_history()

    async def square_off(self, reason: str = "manual") -> int:
        """Client Run-toggle-OFF square-off — closes any open position at its
        own entry price (paper-mode fill assumption; a real execution bridge
        would fetch the live LTP). Returns 1 if a leg was closed, else 0."""
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return 0
        for leg in (pos.t1, pos.t2):
            if leg is not None and leg.status == "open":
                leg.status = "closed"
                leg.close_price = leg.entry_price
                leg.close_reason = reason
                leg.close_time = datetime.now(IST)
        pos.status = "closed"
        pos.close_time = datetime.now(IST)
        self._persist_position()
        return 1

    # ── live loops ───────────────────────────────────────────────────────────
    async def _candle_loop(self) -> None:
        q = self._subscribe(Topic.CANDLE_CLOSE)
        while self._running:
            try:
                ev: CandleEvent = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if getattr(ev, "symbol", "") != self._underlying or getattr(ev, "timeframe", 0) != 75:
                continue
            self._check_daily_boundary(ev.timestamp)
            self._engine.update(spot_bar=ev)
            self._persist_position()

    async def _option_loop(self) -> None:
        q = self._subscribe(Topic.OPTION_TICK)
        while self._running:
            try:
                tick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            side = None
            if getattr(tick, "symbol", "") == self._ce_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._ce_strike and tick.option_type == "CE"):
                side = "CE"
            elif getattr(tick, "symbol", "") == self._pe_symbol or (
                    tick.underlying == self._underlying and int(tick.strike) == self._pe_strike and tick.option_type == "PE"):
                side = "PE"
            if side is None or tick.ltp <= 0:
                continue
            self._on_option_tick(side, float(tick.ltp), tick.timestamp)

    def _on_option_tick(self, side: str, ltp: float, ts: datetime) -> None:
        self._live_price[side] = ltp
        bucket = _bucket_start(ts, 5)
        cur = self._buckets[side]
        if cur is None or cur.timestamp != bucket:
            if cur is not None:
                self._close_5m_bucket(side, cur)
            self._buckets[side] = _Bar(bucket, ltp, ltp, ltp, ltp, tf=5)
        else:
            cur.high = max(cur.high, ltp)
            cur.low = min(cur.low, ltp)
            cur.close = ltp

    def _close_5m_bucket(self, side: str, bar) -> None:
        self._check_daily_boundary(bar.timestamp)
        self._bars_5m[side].append(bar)
        if side == "CE":
            events = self._engine.update(ce_bar=bar)
        else:
            events = self._engine.update(pe_bar=bar)
        for ev in events:
            self._emit_order(ev)
        self._persist_position()
        if _bucket_end(bar.timestamp, 75):
            window = [b for b in self._bars_5m[side] if b.timestamp.date() == bar.timestamp.date()]
            r75 = resample_bars(window, 75)
            if r75:
                last = r75[-1]
                b75 = _Bar(last.timestamp, last.close, last.high, last.low, last.close, tf=75)
                if side == "CE":
                    self._engine.update(ce_bar=b75)
                else:
                    self._engine.update(pe_bar=b75)

    # ── daily EOD/Gate2-3 rules (mirrors scripts/v4_backtest_july2026.py) ────
    def _check_daily_boundary(self, ts: datetime) -> None:
        day = ts.date()
        if self._session_day is None:
            self._session_day = day
            return
        if day != self._session_day:
            self._session_day = day
            self._history_ingested = False  # next boot-equivalent scan will re-check strikes at new 09:15
        if (ts.hour, ts.minute) == _EOD_SQUARE_OFF:
            self._force_eod_square_off(ts)
        if (ts.hour, ts.minute) == _GATE23_RESET:
            self._apply_eod_gate23_rules(ts)

    def _force_eod_square_off(self, ts: datetime) -> None:
        pos = self._engine.position
        if pos is None or not pos.is_open:
            return
        logger.info("V4CascadeBook[%s/%s/%s]: EOD 15:15 force square-off.",
                    self._underlying, self._client_id, self._binding_id)
        for leg in (pos.t1, pos.t2):
            if leg is not None and leg.status == "open":
                leg.status = "closed"
                leg.close_price = leg.entry_price
                leg.close_reason = "eod_force_close"
                leg.close_time = ts
        pos.status = "closed"
        pos.close_time = ts
        self._persist_position()

    def _apply_eod_gate23_rules(self, ts: datetime) -> None:
        from strategies.v4_cascade.dataclasses import GateState
        for side in ("CE", "PE"):
            scanner = self._engine._scanners[side]
            for setup in scanner.setups:
                if setup.state != GateState.HTF_LOCKED:
                    setup.state = GateState.HTF_LOCKED
                    setup.mtf_zone = None
                    setup.mtf_timeframe = None
                    setup.limit_entry_price = None
                    setup.mtf_consumed_before_ts = None

    # ── order emission ───────────────────────────────────────────────────────
    def _emit_order(self, ev) -> None:
        if ev.event_type in (CascadeEventType.OPEN_LONG_CE, CascadeEventType.OPEN_LONG_PE):
            sig = SignalPackage(
                source=StrategyID.V4_CASCADE, direction=Direction.LONG,
                underlying=self._underlying, option_type=ev.side,
                target_strike=float(self._ce_strike if ev.side == "CE" else self._pe_strike),
                entry_spot=0.0, stop_spot=0.0, target_spot=0.0, confidence=1.0,
                timestamp=ev.timestamp or datetime.now(IST),
                notes=f"v4_cascade {ev.side} gate3 fire",
                premium_entry=ev.price_hint, premium_sl=ev.sl_price, premium_target=ev.target_price,
            )
            self._bus.publish(Topic.SIGNAL, sig)
        logger.info("V4CascadeBook[%s/%s/%s]: %s side=%s tranche=%s price=%s reason=%s",
                    self._underlying, self._client_id, self._binding_id,
                    ev.event_type.value, ev.side, ev.tranche, ev.price_hint, ev.reason)

    # ── persistence ──────────────────────────────────────────────────────────
    def _persist_position(self) -> None:
        pos = self._engine.position
        if pos is not None:
            position_store.save(self._persist_key, pos.to_dict())
        else:
            position_store.clear(self._persist_key)

    def _restore_position(self) -> None:
        try:
            data = position_store.load(self._persist_key)
        except Exception:
            data = None
        if data:
            try:
                self._engine.position = CascadePosition.from_dict(data)
                logger.info("V4CascadeBook[%s/%s/%s]: restored open position from disk (side=%s).",
                            self._underlying, self._client_id, self._binding_id,
                            self._engine.position.side)
            except Exception:
                logger.exception("V4CascadeBook[%s]: position restore failed.", self._underlying)


# ── module-level bar helpers (shared by ingestion + live bucket close) ──────

def _to_5m_bars(rows: List[dict], filter_zero_volume: bool) -> List:
    import pandas as pd
    if not rows:
        return []
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    if filter_zero_volume:
        df = df[df["volume"] > 0]
        if df.empty:
            return []
    df = df.set_index("ts")
    ohlc = df.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    bars = []
    for ts, row in ohlc.iterrows():
        py = ts.to_pydatetime()
        if py.tzinfo is None:
            py = py.replace(tzinfo=IST)
        bars.append(_Bar(py, float(row["open"]), float(row["high"]), float(row["low"]),
                          float(row["close"]), int(row["volume"]), tf=5))
    return bars


def _bucket_start(ts: datetime, multiplier: int) -> datetime:
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return open_dt + timedelta(minutes=(minutes_since_open // multiplier) * multiplier)


def _bucket_end(ts: datetime, multiplier: int) -> bool:
    """True if the 5-MINUTE bar at ``ts`` is the last one in its
    ``multiplier``-minute bucket (NIFTY option-premium path, 5m granularity)."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (minutes_since_open + 5) % multiplier == 0


def _bucket_end_1m(ts: datetime, multiplier: int) -> bool:
    """Same as ``_bucket_end`` but for 1-MINUTE bar granularity (crypto spot-
    only path) — a real, not just cosmetic, difference: using the 5m-granularity
    check here would silently never fire for most multiples of 60."""
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (minutes_since_open + 1) % multiplier == 0


def _replay_through_engine(engine: V4CascadeEngine, spot_5m, ce_5m, pe_5m, on_daily_boundary=None) -> None:
    """Chronological replay identical in shape to
    scripts/test_real_premium_replay.py — feeds 5m bars continuously and 75m
    closes (spot bias + CE/PE Gate 1) at each 75m bucket boundary, applying
    the 15:15/15:30 daily rules along the way so the rebuilt state exactly
    matches what live ticks would have produced."""
    spot_75m_by_ts = {b.timestamp: b for b in resample_bars(spot_5m, 75)} if spot_5m else {}
    ce_75m_by_ts = {b.timestamp: b for b in resample_bars(ce_5m, 75)} if ce_5m else {}
    pe_75m_by_ts = {b.timestamp: b for b in resample_bars(pe_5m, 75)} if pe_5m else {}
    ce_by_ts = {b.timestamp: b for b in ce_5m}
    pe_by_ts = {b.timestamp: b for b in pe_5m}
    all_ts = sorted(set(ce_by_ts) | set(pe_by_ts))

    last_day = None
    for ts in all_ts:
        day = ts.date()
        if last_day is not None and day != last_day and on_daily_boundary is not None:
            pass  # boundary-time rules (EOD/gate reset) applied via exact-time checks below
        last_day = day

        engine.update(ce_bar=ce_by_ts.get(ts), pe_bar=pe_by_ts.get(ts))

        if _bucket_end(ts, 75):
            bstart = _bucket_start(ts, 75)
            sbar = spot_75m_by_ts.get(bstart)
            ce75 = ce_75m_by_ts.get(bstart)
            pe75 = pe_75m_by_ts.get(bstart)
            if sbar is not None:
                sbar75 = _Bar(sbar.timestamp, sbar.close, sbar.high, sbar.low, sbar.close, tf=75)
            else:
                sbar75 = None
            engine.update(
                spot_bar=sbar75,
                ce_bar=_Bar(ce75.timestamp, ce75.close, ce75.high, ce75.low, ce75.close, tf=75) if ce75 else None,
                pe_bar=_Bar(pe75.timestamp, pe75.close, pe75.high, pe75.low, pe75.close, tf=75) if pe75 else None,
            )

        if (ts.hour, ts.minute) == _EOD_SQUARE_OFF:
            pos = engine.position
            if pos is not None and pos.is_open:
                for leg in (pos.t1, pos.t2):
                    if leg is not None and leg.status == "open":
                        leg.status = "closed"
                        leg.close_price = leg.entry_price
                        leg.close_reason = "eod_force_close_replay"
                        leg.close_time = ts
                pos.status = "closed"
                pos.close_time = ts
        if (ts.hour, ts.minute) == _GATE23_RESET and on_daily_boundary is not None:
            on_daily_boundary(ts)
