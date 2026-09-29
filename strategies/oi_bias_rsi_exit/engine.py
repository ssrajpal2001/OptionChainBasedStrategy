"""
strategies/oi_bias_rsi_exit/engine.py — OiBiasRsiExitStrategy, the live
per-(client,binding) book for the OI-spurt selection + combined-OI bias +
StochRSI entry/exit strategy.

First live cut (2026-09-29), built directly from a real-data backtest +
parameter sweep (scripts/oi_bias_rsi_exit_backtest.py / _optimize.py) run
against 20 real trading days. Deliberately POLLS REST (stock spot + option
premium via data_layer.historical_candles.fetch_upstox_intraday_1m) on a
fixed cadence rather than subscribing to live WS ticks -- this is a genuine
design tradeoff (≈1-poll-cycle latency, not tick-level) made so the live
engine computes off the EXACT SAME bar-based functions the validated
backtest used (strategies.core.candle_indicators.to_n_min_bars_market_
anchored, strategies.oi_bias_rsi_exit.detector.compute_stoch_rsi_double_
smoothed/check_entry_state/check_exit_cross) -- it can never behaviorally
drift from what was actually backtested. Revisit for a tick-driven version
once this first cut has run cleanly in paper mode.

Mechanic per trading day, one book per (client, binding):
  1. Selection: reuses strategies.oi_orb_screener.screener's own real NSE
     fetch functions (fetch_fno_price_universe, fetch_oi_spurts_nse,
     fetch_top_gainers_losers) UNCHANGED -- top gainers/losers cross-matched
     against the real OI-spurt list, >=7% qualifies. Runs once at
     start_time (default 09:25, direct user spec).
  2. Per qualifying stock: freeze ATM/OTM strikes from the stock's real
     09:15 1-min open (strategies.oi_bias_breakout.detector.
     freeze_signal_strikes, reused unchanged), then read each of the 4
     legs' (ATM Call, OTM Call, ATM Put, OTM Put) real OI at 09:15/09:20/
     09:25 via fetch_upstox_intraday_1m on each contract, combining ATM+OTM
     per side. classify_combined_oi_bias (this module's own function, the
     user's ORIGINAL combined-both-transitions rule) decides bullish/
     bearish/none.
  3. Entry: for a bullish/bearish stock with no open position, scans the
     stock's own intraday 1-min bars resampled to ENTRY_TIMEFRAME_MIN (3,
     optimized) with StochRSI(21,21,3,3) (optimized), mirrored by bias
     (bullish=K>D, bearish=D>K), starting only at/after start_time. First
     true bar fires a BUY at the ATM CE/PE's own live premium (last real
     intraday bar close).
  4. Exit, first of two to fire: a genuine crossover event (mirrored by
     bias) on the stock's 75-min (optimized) StochRSI, or EOD (default
     15:25, direct user spec) force-close.

Deliberately NOT implemented yet (flagged, not hidden): the frozen spec's
third exit ("OI bias flips to the opposite direction, twice") -- re-reading
combined OI every 5 minutes intraday for every open position adds real
polling load and hasn't been validated even in backtest (the backtest
couldn't simulate it either, see oi_bias_rsi_exit_backtest.py's own
docstring). Watch real paper-mode logs before adding it.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus
from data_layer.client_db import ClientDB
from data_layer.historical_candles import fetch_upstox_intraday_1m
from data_layer.instrument_registry import REGISTRY
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.oi_bias_breakout.detector import freeze_signal_strikes
from strategies.oi_bias_rsi_exit.detector import (
    classify_combined_oi_bias, compute_stoch_rsi_double_smoothed,
    check_entry_state, check_exit_cross,
)
from strategies.oi_bias_rsi_exit.events import OiBiasRsiExitOrderEvent
from strategies.oi_bias_rsi_exit import store
from strategies.oi_orb_screener import screener as _screener
from strategies.oi_orb_screener import stock_resolve

logger = logging.getLogger(__name__)

UNDERLYING_SENTINEL = "SCREENER"

# Optimized (2026-09-27, scripts/oi_bias_rsi_exit_optimize.py, 20 real
# trades): entry 3-min StochRSI(21,21,3,3), exit 75-min StochRSI(21,21,3,3).
ENTRY_TIMEFRAME_MIN = 3
ENTRY_STOCH_RSI_LENGTHS = (21, 21, 3, 3)
EXIT_TIMEFRAME_MIN = 75
EXIT_STOCH_RSI_LENGTHS = (21, 21, 3, 3)

_OI_SNAPSHOT_TIMES = (dtime(9, 15), dtime(9, 20), dtime(9, 25))


def _to_bars(rows: List[dict]) -> List[Bar]:
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts, open=r["open"], high=r["high"], low=r["low"], close=r["close"]))
    return out


class OiBiasRsiExitStrategy:
    def __init__(
        self, bus: EventBus, cfg, client_id: str, binding_id: str,
        lot_multiplier: int = 1, product_type: str = "NRML",
        start_time: str = "09:25", force_exit_time: str = "15:25",
        oi_spurt_min_pct: float = 7.0, top_n_per_side: int = 10,
        poll_seconds: float = 60.0,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._client_id = client_id
        self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self._product_type = product_type
        self._start_time = dtime.fromisoformat(start_time)
        self._force_exit_time = dtime.fromisoformat(force_exit_time)
        self._oi_spurt_min_pct = oi_spurt_min_pct
        self._top_n_per_side = top_n_per_side
        self._poll_seconds = poll_seconds

        self._clog = logging.getLogger(f"OiBiasRsiExit[{client_id}/{binding_id}]")
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._today: Optional[date] = None
        self._selection_done = False
        self._candidates: List[str] = []
        self._bias: Dict[str, str] = {}
        self._entry_idx_start: Dict[str, int] = {}
        self._positions: Dict[str, dict] = {}
        self._day_done = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"OiBiasRsiExit_{self._client_id}_{self._binding_id}")
        self._clog.info("started.")

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        self._clog.info("stopped.")

    async def _get_token(self) -> str:
        creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
        return (creds or {}).get("access_token", "")

    def _reset_session(self, today: date) -> None:
        self._today = today
        self._selection_done = False
        self._candidates = []
        self._bias = {}
        self._entry_idx_start = {}
        self._positions = {}
        self._day_done = False
        self._clog.info("session reset for new trading day %s.", today)

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._tick()
            except Exception:
                self._clog.exception("tick error (non-fatal, continuing).")
            await asyncio.sleep(self._poll_seconds)

    async def _tick(self) -> None:
        now = datetime.now(IST)
        today = now.date()
        if self._today != today:
            self._reset_session(today)
        if self._day_done:
            return
        if now.time() >= self._force_exit_time:
            await self._square_off_all("eod")
            self._day_done = True
            return
        if now.time() < self._start_time:
            return

        token = await self._get_token()
        if not token:
            self._clog.warning("no upstox access_token in ClientDB feeder_creds -- skipping this tick.")
            return

        if not self._selection_done:
            await self._run_selection_and_bias(token)
            self._selection_done = True

        for sym in list(self._bias):
            if self._bias[sym] in ("bullish", "bearish") and sym not in self._positions:
                await self._check_entry(sym, token)

        for sym in list(self._positions):
            await self._check_exit(sym, token)

    # ── Step 1: selection (reuses oi_orb_screener.screener unchanged) ──────

    async def _run_selection_and_bias(self, token: str) -> None:
        try:
            nse = await asyncio.to_thread(_screener.NSESession)
            universe = await asyncio.to_thread(_screener.fetch_fno_price_universe, nse)
            oi_spurts = await asyncio.to_thread(_screener.fetch_oi_spurts_nse, nse)
            candidates = await asyncio.to_thread(_screener.fetch_top_gainers_losers, universe, self._top_n_per_side)
        except Exception:
            self._clog.exception("Step 1 selection fetch failed.")
            return
        merged = candidates.merge(oi_spurts, on="symbol", how="left")
        qualifying = merged[merged["oi_spurt_pct"].fillna(0) >= self._oi_spurt_min_pct]
        self._candidates = sorted(set(qualifying["symbol"].tolist()))
        self._clog.info("selection: %d candidates qualified (>=%.1f%% OI-spurt): %s",
                         len(self._candidates), self._oi_spurt_min_pct, self._candidates)

        REGISTRY.load_sync("NIFTY", token)
        for sym in self._candidates:
            try:
                await self._compute_bias_for(sym, token)
            except Exception:
                self._clog.exception("bias computation failed for %s.", sym)

    async def _oi_at_snapshots(self, symbol: str, strike: float, option_type: str, token: str) -> Dict[dtime, Optional[float]]:
        out: Dict[dtime, Optional[float]] = {t: None for t in _OI_SNAPSHOT_TIMES}
        contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strike, option_type, ("upstox",))
        if contract is None:
            return out
        rows = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        by_time = {}
        for r in rows:
            ts = r["ts"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            by_time[ts.time()] = r.get("oi")
        for t in _OI_SNAPSHOT_TIMES:
            out[t] = by_time.get(t)
        if out[dtime(9, 15)] is None:
            out[dtime(9, 15)] = by_time.get(dtime(9, 16))  # direct user fallback
        return out

    async def _compute_bias_for(self, symbol: str, token: str) -> None:
        REGISTRY.load_sync(symbol, token)
        stock_key = stock_resolve.resolve_eq_instrument_key(symbol)
        rows = await fetch_upstox_intraday_1m(stock_key, token)
        bars = sorted(_to_bars(rows), key=lambda b: b.ts)
        bar_915 = next((b for b in bars if b.ts.time() == dtime(9, 15)), None)
        if bar_915 is None:
            self._clog.warning("%s: no real 09:15 stock bar yet -- skipping bias for now.", symbol)
            return
        step = stock_resolve.resolve_strike_step_for_price(symbol, bar_915.open)
        strikes = freeze_signal_strikes(open_915_price=bar_915.open, strike_step=step)

        atm_call = await self._oi_at_snapshots(symbol, strikes.atm, "CE", token)
        otm_call = await self._oi_at_snapshots(symbol, strikes.otm_call, "CE", token)
        atm_put = await self._oi_at_snapshots(symbol, strikes.atm, "PE", token)
        otm_put = await self._oi_at_snapshots(symbol, strikes.otm_put, "PE", token)

        def combine(a, b, t):
            av, bv = a[t], b[t]
            return None if av is None or bv is None else av + bv

        call = {t: combine(atm_call, otm_call, t) for t in _OI_SNAPSHOT_TIMES}
        put = {t: combine(atm_put, otm_put, t) for t in _OI_SNAPSHOT_TIMES}

        bias = classify_combined_oi_bias(
            call_oi_915=call[dtime(9, 15)], call_oi_920=call[dtime(9, 20)], call_oi_925=call[dtime(9, 25)],
            put_oi_915=put[dtime(9, 15)], put_oi_920=put[dtime(9, 20)], put_oi_925=put[dtime(9, 25)],
        )
        self._bias[symbol] = bias
        self._clog.info("%s: combined OI bias=%s (call=%s put=%s)", symbol, bias, call, put)
        await asyncio.to_thread(
            store.record_shortlist_oi, self._client_id, self._binding_id, symbol,
            {
                "open_915": bar_915.open, "strike_step": step, "atm_strike": strikes.atm,
                "otm_call_strike": strikes.otm_call, "otm_put_strike": strikes.otm_put,
                "call_oi_915": call[dtime(9, 15)], "call_oi_920": call[dtime(9, 20)], "call_oi_925": call[dtime(9, 25)],
                "put_oi_915": put[dtime(9, 15)], "put_oi_920": put[dtime(9, 20)], "put_oi_925": put[dtime(9, 25)],
                "bias": bias,
            },
        )

    # ── Entry ────────────────────────────────────────────────────────────

    async def _check_entry(self, symbol: str, token: str) -> None:
        bias = self._bias[symbol]
        rows = await fetch_upstox_intraday_1m(stock_resolve.resolve_eq_instrument_key(symbol), token)
        bars = sorted(_to_bars(rows), key=lambda b: b.ts)
        if not bars:
            return
        bars_e = to_n_min_bars_market_anchored(bars, ENTRY_TIMEFRAME_MIN)
        closes_e = [b.close for b in bars_e]
        k, d = compute_stoch_rsi_double_smoothed(closes_e, *ENTRY_STOCH_RSI_LENGTHS)
        idx = next(
            (i for i, b in enumerate(bars_e) if b.ts.time() >= self._start_time and check_entry_state(k[i], d[i], bias)),
            None)
        if idx is None:
            return

        option_type = "CE" if bias == "bullish" else "PE"
        bar_915 = next((b for b in bars if b.ts.time() == dtime(9, 15)), None)
        if bar_915 is None:
            return
        step = stock_resolve.resolve_strike_step_for_price(symbol, bar_915.open)
        strikes = freeze_signal_strikes(open_915_price=bar_915.open, strike_step=step)
        expiry = REGISTRY.get_active_expiry(symbol)
        if expiry is None:
            self._clog.warning("%s: no active expiry resolved -- skipping entry.", symbol)
            return
        contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strikes.atm, option_type, ("upstox",))
        if contract is None:
            self._clog.warning("%s: could not resolve a real %s contract -- skipping entry.", symbol, option_type)
            return
        prem_rows = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        if not prem_rows:
            self._clog.warning("%s: no real premium print yet for %s%s -- skipping entry this tick.",
                                symbol, contract.strike, option_type)
            return
        entry_price = prem_rows[-1]["close"]
        lot = await stock_resolve.resolve_lot_async(symbol)
        qty = lot * self._lot_multiplier

        pos = {
            "option_type": option_type, "strike": contract.strike, "expiry": expiry,
            "qty": qty, "entry_price": entry_price, "entry_ts": datetime.now(IST),
            "upstox_key": contract.upstox_key,
        }
        row_id = await asyncio.to_thread(
            store.record_entry, self._client_id, self._binding_id, symbol, pos,
            ENTRY_TIMEFRAME_MIN, ENTRY_STOCH_RSI_LENGTHS,
        )
        pos["db_row_id"] = row_id
        self._positions[symbol] = pos
        self._clog.info("ENTRY %s %s%s qty=%d @ %.2f (bias=%s)",
                         symbol, contract.strike, option_type, qty, entry_price, bias)
        await self._bus.publish(Topic.OI_BIAS_RSI_EXIT_ORDER_REQUEST, OiBiasRsiExitOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=symbol, option_type=option_type, strike=contract.strike, expiry=expiry,
            quantity=qty, entry_price=entry_price, reason="stoch_entry",
            event_id=str(uuid.uuid4()), entry_ts=datetime.now(IST), product_type=self._product_type,
        ))

    # ── Exit ─────────────────────────────────────────────────────────────

    async def _check_exit(self, symbol: str, token: str) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        bias = self._bias[symbol]
        rows = await fetch_upstox_intraday_1m(stock_resolve.resolve_eq_instrument_key(symbol), token)
        bars = sorted(_to_bars(rows), key=lambda b: b.ts)
        if not bars:
            return
        bars_x = to_n_min_bars_market_anchored(bars, EXIT_TIMEFRAME_MIN)
        closes_x = [b.close for b in bars_x]
        k, d = compute_stoch_rsi_double_smoothed(closes_x, *EXIT_STOCH_RSI_LENGTHS)
        entry_ts = pos["entry_ts"]
        crossed = False
        for i in range(1, len(bars_x)):
            bucket_close = bars_x[i].ts + timedelta(minutes=EXIT_TIMEFRAME_MIN)
            if bucket_close <= entry_ts:
                continue
            if bucket_close > datetime.now(IST):
                break
            if check_exit_cross(k[i - 1], d[i - 1], k[i], d[i], bias):
                crossed = True
                break
        if not crossed:
            return
        await self._close_position(symbol, "stoch_exit_cross", token)

    async def _close_position(self, symbol: str, reason: str, token: str) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        prem_rows = await fetch_upstox_intraday_1m(pos["upstox_key"], token)
        exit_price = prem_rows[-1]["close"] if prem_rows else pos["entry_price"]
        self._clog.info("EXIT %s %s%s qty=%d @ %.2f (reason=%s)",
                         symbol, pos["strike"], pos["option_type"], pos["qty"], exit_price, reason)
        pnl = (exit_price - pos["entry_price"]) * pos["qty"]
        await asyncio.to_thread(
            store.record_exit, pos.get("db_row_id", -1), exit_price, reason, pnl,
            EXIT_TIMEFRAME_MIN, EXIT_STOCH_RSI_LENGTHS,
        )
        await self._bus.publish(Topic.OI_BIAS_RSI_EXIT_ORDER_REQUEST, OiBiasRsiExitOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="SELL",
            underlying=symbol, option_type=pos["option_type"], strike=pos["strike"], expiry=pos["expiry"],
            quantity=pos["qty"], entry_price=pos["entry_price"], exit_price=exit_price, reason=reason,
            event_id=str(uuid.uuid4()), entry_ts=pos["entry_ts"], product_type=self._product_type,
        ))
        del self._positions[symbol]

    async def _square_off_all(self, reason: str) -> None:
        token = await self._get_token()
        for symbol in list(self._positions):
            try:
                await self._close_position(symbol, reason, token)
            except Exception:
                self._clog.exception("EOD square-off failed for %s.", symbol)

    def monitoring_state(self) -> dict:
        return {
            "client_id": self._client_id, "binding_id": self._binding_id,
            "candidates": self._candidates, "bias": dict(self._bias),
            "positions": {s: dict(p) for s, p in self._positions.items()},
        }
