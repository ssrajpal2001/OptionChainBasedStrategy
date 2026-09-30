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
  4. Exit, first of three to fire:
       (a) a genuine crossover event (mirrored by bias) on the stock's
           75-min (optimized) StochRSI;
       (b) the OI bias (re-run every OI_RECHECK_MINUTES on the SAME frozen
           ATM/OTM strikes, via strategies.oi_bias_breakout.detector.
           classify_oi_bias -- a single-transition, cross-referenced rule,
           distinct from classify_combined_oi_bias's stricter both-
           transition rule used once at entry) reading as the exact
           opposite of the entry direction, twice (not necessarily
           consecutively);
       (c) EOD (default 15:25, direct user spec) force-close.

2026-09-29, direct user request: (b) implemented -- previously deferred at
first ship since it adds real polling load and hadn't been validated even
in backtest (the backtest couldn't simulate it either, see
oi_bias_rsi_exit_backtest.py's own docstring). Unvalidated against real
forward data; watch real paper-mode logs before trusting it broadly, same
graduation discipline as every other new mechanic in this codebase.
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
from data_layer.historical_candles import fetch_upstox_intraday_1m, fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.oi_bias_breakout.detector import freeze_signal_strikes, classify_oi_bias
from strategies.oi_bias_rsi_exit.detector import (
    classify_combined_oi_bias, classify_windowed_oi_bias, compute_stoch_rsi_double_smoothed,
    check_entry_state, check_exit_cross, count_opposite_bias_readings,
)
from strategies.oi_bias_rsi_exit.events import OiBiasRsiExitOrderEvent
from strategies.oi_bias_rsi_exit import store
from strategies.oi_orb_screener import screener as _screener
from strategies.oi_orb_screener import stock_resolve


def _make_strategy_logger(client_id: str, binding_id: str) -> logging.Logger:
    """Dedicated, rotating, per-(client,binding,day) log file -- same
    utils.logging_utils.make_strategy_logger platform utility every other
    strategy here uses (oi_orb_screener/iron_fly/cag_straddle/sell_straddle).
    2026-09-30: this book previously only had a bare named logger with no
    file handler of its own, so its lines only ever reached the shared
    process log -- fixed per direct user spec ("RSI exit will have
    individual log to register all the details")."""
    from utils.logging_utils import make_strategy_logger
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"oibiasrsi_{client_id}_{binding_id}_{date_str}", propagate=False)

logger = logging.getLogger(__name__)

UNDERLYING_SENTINEL = "SCREENER"

# Optimized (2026-09-27, scripts/oi_bias_rsi_exit_optimize.py, 20 real
# trades): entry 3-min StochRSI(21,21,3,3), exit 75-min StochRSI(21,21,3,3).
ENTRY_TIMEFRAME_MIN = 3
ENTRY_STOCH_RSI_LENGTHS = (21, 21, 3, 3)
EXIT_TIMEFRAME_MIN = 75
EXIT_STOCH_RSI_LENGTHS = (21, 21, 3, 3)

_OI_SNAPSHOT_TIMES = (dtime(9, 15), dtime(9, 20), dtime(9, 25))

# 2026-09-30, direct user correction ("we want total oi from 915-920 and
# from 920-925 and then do the needful"): replaces the fixed-instant
# snapshot design above (kept only for the still-referencing regression
# tests' historical record -- _oi_at_snapshots itself is no longer called
# by _compute_bias_for). See classify_windowed_oi_bias's own docstring for
# the full incident/rationale.
_OI_WINDOWS = ((dtime(9, 15), dtime(9, 20)), (dtime(9, 20), dtime(9, 25)))

# 2026-09-29, direct user request: the third exit condition from the
# original frozen spec, "OI bias flips to the opposite direction, twice" --
# deliberately deferred at first ship since it adds real polling load and
# couldn't be validated in backtest either. Re-checks the SAME frozen
# ATM/OTM strikes' OI every OI_RECHECK_MINUTES using
# oi_bias_breakout.detector.classify_oi_bias (a single-transition,
# cross-referenced rule -- OTM Call vs ATM Put for bullish, OTM Put vs ATM
# Call for bearish -- distinct from classify_combined_oi_bias's stricter
# both-transition ATM+OTM-summed rule used once at entry).
OI_RECHECK_MINUTES = 5


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
        # 2026-09-30 CRITICAL FIX, direct user audit request: these were
        # module-level constants used directly throughout this file with
        # ZERO per-deployment override -- the exact "nothing hardcoded"
        # violation flagged this session, now fixed to match the
        # constructor-param + strategy_params pattern every other current
        # strategy (CAG Straddle, Iron Fly) already uses correctly. Defaults
        # unchanged (the same 2026-09-27 optimize.py-validated values).
        entry_timeframe_min: int = ENTRY_TIMEFRAME_MIN,
        entry_stoch_rsi_lengths: tuple = ENTRY_STOCH_RSI_LENGTHS,
        exit_timeframe_min: int = EXIT_TIMEFRAME_MIN,
        exit_stoch_rsi_lengths: tuple = EXIT_STOCH_RSI_LENGTHS,
        oi_recheck_minutes: int = OI_RECHECK_MINUTES,
        oi_bias_flip_count: int = 2,
        # 2026-09-30, direct user spec ("scan for fresh stocks after 9:25 as
        # well ... check OI as same concept and entry and exit will be
        # same"; cadence corrected same day to "every 60 sec" per direct
        # follow-up instruction): the original build only ever scanned ONCE
        # at start_time, per OI-ORB Screener's original single-scan
        # precedent. This strategy now keeps re-running the exact same
        # selection/OI-bias pipeline every `rescan_interval_sec` seconds for
        # the rest of the day, ADDING any newly-qualifying stock to the same
        # candidate pool -- entry/exit logic is untouched, a rescan-added
        # symbol goes through _check_entry/_check_exit exactly like a 09:25
        # one. Set to 0 to disable (reverts to the original single-scan
        # behavior). The underlying NSE fetches (fetch_fno_price_universe/
        # fetch_oi_spurts_nse/fetch_top_gainers_losers) already share a 30s-
        # TTL cache (screener.py's _cached_nse_fetch) across every caller, so
        # a 60s rescan cadence does not mean a fresh real NSE hit every 60s.
        rescan_interval_sec: int = 60,
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
        self._entry_timeframe_min = entry_timeframe_min
        self._entry_stoch_rsi_lengths = tuple(entry_stoch_rsi_lengths)
        self._exit_timeframe_min = exit_timeframe_min
        self._exit_stoch_rsi_lengths = tuple(exit_stoch_rsi_lengths)
        self._oi_recheck_minutes = oi_recheck_minutes
        self._oi_bias_flip_count = oi_bias_flip_count
        self._rescan_interval_sec = rescan_interval_sec

        self._clog = _make_strategy_logger(client_id, binding_id)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._today: Optional[date] = None
        self._selection_done = False
        self._next_rescan_at: Optional[datetime] = None
        self._candidates: List[str] = []
        self._bias: Dict[str, str] = {}
        self._entry_idx_start: Dict[str, int] = {}
        self._positions: Dict[str, dict] = {}
        self._day_done = False
        # 2026-09-30, direct user correction: StochRSI(21,21,3,3) needs ~46
        # bars minimum before ANY real K/D value can exist -- at 3-min entry
        # bars that's 138 real minutes (~11:33 IST from a 09:15 open) before
        # entry can EVER fire; at 75-min exit bars it's 3,450 minutes (~9.2
        # TRADING DAYS) before the exit cross could ever fire at all if
        # seeded from scratch every day. Prior-day history is fetched once
        # per symbol (via fetch_upstox_range_1m, real 1-min bars, real
        # trading days only) and cached here so the indicator is already
        # warm at market open -- see _bars_with_history().
        self._history_bars_cache: Dict[str, List[Bar]] = {}
        self._history_cache_date: Optional[date] = None

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
        self._next_rescan_at = None
        self._candidates = []
        self._bias = {}
        self._entry_idx_start = {}
        self._positions = {}
        self._day_done = False
        self._history_bars_cache = {}
        self._history_cache_date = None
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
            await self._run_selection_and_bias(token, is_rescan=False)
            self._selection_done = True
            if self._rescan_interval_sec > 0:
                self._next_rescan_at = now + timedelta(seconds=self._rescan_interval_sec)
        elif (
            self._rescan_interval_sec > 0
            and self._next_rescan_at is not None
            and now >= self._next_rescan_at
        ):
            await self._run_selection_and_bias(token, is_rescan=True)
            self._next_rescan_at = now + timedelta(seconds=self._rescan_interval_sec)

        for sym in list(self._bias):
            if self._bias[sym] in ("bullish", "bearish") and sym not in self._positions:
                await self._check_entry(sym, token)

        for sym in list(self._positions):
            await self._check_exit(sym, token)

    # ── Step 1: selection (reuses oi_orb_screener.screener unchanged) ──────

    async def _run_selection_and_bias(self, token: str, is_rescan: bool = False) -> None:
        """Runs the exact same OI-spurt selection pipeline whether this is the
        initial 09:25 scan or a later periodic rescan (`is_rescan=True`,
        direct 2026-09-30 user spec: "scan for fresh stocks after 9:25 as
        well... check OI as same concept"). On a rescan, only symbols that
        were NOT already candidates get added and get bias/entry/exit
        processing -- an already-seen symbol (traded or not) is left alone,
        so a rescan can never re-wipe or re-run an in-progress/closed
        position's own state."""
        _tag = "rescan" if is_rescan else "selection"
        try:
            nse = await asyncio.to_thread(_screener.NSESession)
            universe = await asyncio.to_thread(_screener.fetch_fno_price_universe, nse)
            oi_spurts = await asyncio.to_thread(_screener.fetch_oi_spurts_nse, nse)
            candidates = await asyncio.to_thread(_screener.fetch_top_gainers_losers, universe, self._top_n_per_side)
        except Exception:
            self._clog.exception("%s: fetch failed.", _tag)
            return
        merged = candidates.merge(oi_spurts, on="symbol", how="left")
        qualifying = merged[merged["oi_spurt_pct"].fillna(0) >= self._oi_spurt_min_pct]
        qualifying_syms = sorted(set(qualifying["symbol"].tolist()))
        # 2026-09-30, direct user request: the old single summary line only
        # ever showed the FINAL post-filter result -- when 0 qualified there
        # was zero visibility into why (empty top-gainers/losers fetch? a
        # real OI-spurt list that just missed the threshold? symbols that
        # never merged at all?). Log every intermediate stage so a 0-result
        # day is diagnosable from this log alone, same transparency standard
        # as oi_orb_screener's own scan/shortlist logging.
        self._clog.info("%s: universe=%d rows, oi_spurts=%d rows, top-gainers/losers candidates=%d: %s",
                         _tag, len(universe), len(oi_spurts), len(candidates),
                         sorted(candidates["symbol"].tolist()) if "symbol" in candidates else [])
        _ranked = merged[["symbol", "oi_spurt_pct"]].copy()
        _ranked["oi_spurt_pct"] = _ranked["oi_spurt_pct"].fillna(0)
        _ranked = _ranked.sort_values("oi_spurt_pct", ascending=False)
        self._clog.info("%s: candidate OI-spurt%% (threshold=%.1f%%): %s",
                         _tag, self._oi_spurt_min_pct,
                         [f"{r.symbol}={r.oi_spurt_pct:.2f}%" for r in _ranked.itertuples()])

        new_syms = [s for s in qualifying_syms if s not in self._candidates]
        self._candidates = sorted(set(self._candidates) | set(qualifying_syms))
        if is_rescan:
            self._clog.info("rescan: %d qualified this pass (>=%.1f%% OI-spurt): %s -- %d genuinely NEW: %s",
                             len(qualifying_syms), self._oi_spurt_min_pct, qualifying_syms,
                             len(new_syms), new_syms)
        else:
            self._clog.info("selection: %d candidates qualified (>=%.1f%% OI-spurt): %s",
                             len(qualifying_syms), self._oi_spurt_min_pct, qualifying_syms)

        if not new_syms:
            return
        REGISTRY.load_sync("NIFTY", token)
        for sym in new_syms:
            try:
                await self._compute_bias_for(sym, token)
            except Exception:
                self._clog.exception("bias computation failed for %s.", sym)

    async def _oi_at_snapshots(self, symbol: str, strike: float, option_type: str, token: str) -> Dict[dtime, Optional[float]]:
        """2026-09-29 CRITICAL FIX, real incident (JUBLFOOD 2026-09-29
        backtest): a thin OTM leg can genuinely have no reported OI in
        Upstox's own intraday candle response for its first several minutes
        -- the old code only had a single hardcoded 9:15->9:16 fallback,
        with none at all for 9:20/9:25, so a leg missing OI at those points
        permanently classified the stock's bias as "none" even though a
        real, carried-forward OI value existed moments earlier. OI is a
        snapshot LEVEL (open interest outstanding), not a per-trade field --
        a contract not trading in one exact minute doesn't mean its OI
        vanished, so the correct read for "OI at 09:20" is the most recent
        REAL (>0) reading at-or-before 09:20, not an exact-minute match.
        Applied uniformly to all three snapshot times now, not just the
        first."""
        out: Dict[dtime, Optional[float]] = {t: None for t in _OI_SNAPSHOT_TIMES}
        contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strike, option_type, ("upstox",))
        if contract is None:
            return out
        rows = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        parsed = []
        for r in rows:
            ts = r["ts"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            oi = r.get("oi")
            if oi:
                parsed.append((ts.time(), float(oi)))
        parsed.sort(key=lambda x: x[0])
        for t in _OI_SNAPSHOT_TIMES:
            best = None
            for bar_t, oi in parsed:
                if bar_t > t:
                    break
                best = oi
            out[t] = best
        return out

    async def _oi_max_in_windows(self, symbol: str, strike: float, option_type: str, token: str) -> Dict[int, Optional[float]]:
        """2026-09-30 CRITICAL FIX, direct user correction after a real live
        incident: the point-in-time design above (_oi_at_snapshots) was
        found to permanently misclassify most real candidates as "none" --
        confirmed via direct REST verification (raw Upstox dict dump) that
        several genuinely tradeable stocks simply had NO candle at all for
        one or more of the exact 09:15/09:20/09:25 minutes (thin early-
        session trading is normal, not a data/feed bug). Direct user spec:
        "we want total oi from 915-920 and from 920-925 and then do the
        needful" -- replaces the fixed instants with two 5-minute windows,
        returning the MAX real (>0) OI reading found anywhere within each
        window (direct user choice among last/max/net-change alternatives).
        Returns {0: W1[09:15,09:20) max, 1: W2[09:20,09:25) max}, None for
        a window with no real reading at all (never fabricated)."""
        out: Dict[int, Optional[float]] = {0: None, 1: None}
        contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strike, option_type, ("upstox",))
        if contract is None:
            return out
        rows = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        parsed = []
        for r in rows:
            ts = r["ts"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            oi = r.get("oi")
            if oi:
                parsed.append((ts.time(), float(oi)))
        for i, (start, end) in enumerate(_OI_WINDOWS):
            in_window = [oi for bar_t, oi in parsed if start <= bar_t < end]
            out[i] = max(in_window) if in_window else None
        return out

    async def _current_oi(self, symbol: str, strike: float, option_type: str, token: str) -> Optional[float]:
        """Latest real (>0) OI reading for a contract, for the rolling
        OI-bias-flip re-check -- same forward-fill-from-last-real-reading
        principle as _oi_at_snapshots, but "as of now" instead of a fixed
        historical time."""
        contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strike, option_type, ("upstox",))
        if contract is None:
            return None
        rows = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        best = None
        for r in rows:
            oi = r.get("oi")
            if oi:
                best = float(oi)
        return best

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

        atm_call = await self._oi_max_in_windows(symbol, strikes.atm, "CE", token)
        otm_call = await self._oi_max_in_windows(symbol, strikes.otm_call, "CE", token)
        atm_put = await self._oi_max_in_windows(symbol, strikes.atm, "PE", token)
        otm_put = await self._oi_max_in_windows(symbol, strikes.otm_put, "PE", token)

        def combine(a, b, w):
            av, bv = a[w], b[w]
            return None if av is None or bv is None else av + bv

        call = {w: combine(atm_call, otm_call, w) for w in (0, 1)}
        put = {w: combine(atm_put, otm_put, w) for w in (0, 1)}

        bias = classify_windowed_oi_bias(
            call_w1=call[0], call_w2=call[1], put_w1=put[0], put_w2=put[1],
        )
        self._bias[symbol] = bias
        self._clog.info("%s: windowed OI bias=%s (call=%s put=%s)", symbol, bias, call, put)
        await asyncio.to_thread(
            store.record_shortlist_oi, self._client_id, self._binding_id, symbol,
            {
                "open_915": bar_915.open, "strike_step": step, "atm_strike": strikes.atm,
                "otm_call_strike": strikes.otm_call, "otm_put_strike": strikes.otm_put,
                "call_oi_w1": call[0], "call_oi_w2": call[1],
                "put_oi_w1": put[0], "put_oi_w2": put[1],
                "bias": bias,
            },
        )

    # ── Prior-day warmup, 2026-09-30 direct user correction ────────────────
    # "for warmup u need to fetch data from prev day as well" -- StochRSI
    # (21,21,3,3) needs ~46 bars minimum before ANY real K/D value exists.
    # At 3-min entry bars that's 138 real minutes -- not reachable until
    # ~11:33 IST from a 09:15 open if seeded from scratch every day. At
    # 75-min exit bars it's 3,450 minutes (~9.2 TRADING DAYS) -- the exit
    # cross could never fire at all within one day without this.

    _HISTORY_LOOKBACK_CALENDAR_DAYS = 20  # comfortably covers ~14 trading
    # days after weekends -- the 75-min exit side needs ~10 trading days'
    # worth of real bars (5 usable 75-min bars/trading day) to warm up.

    async def _bars_with_history(self, symbol: str, token: str) -> List[Bar]:
        """Today's real bars, prefixed with real prior-trading-day bars
        (fetched once per symbol per day, cached -- prior days never change
        intraday so there's no reason to refetch them every poll cycle).
        Only TODAY's bars ever get treated as a fresh entry/exit signal by
        the callers below; the historical prefix exists purely so the
        indicator's internal rolling state is already warm by the time
        today's own bars start evaluating."""
        if self._history_cache_date != self._today:
            self._history_bars_cache = {}
            self._history_cache_date = self._today
        if symbol not in self._history_bars_cache:
            stock_key = stock_resolve.resolve_eq_instrument_key(symbol)
            end = (self._today or date.today()) - timedelta(days=1)
            start = end - timedelta(days=self._HISTORY_LOOKBACK_CALENDAR_DAYS)
            try:
                hist_rows = await fetch_upstox_range_1m(stock_key, token, start, end)
            except Exception:
                self._clog.exception("%s: prior-day history fetch failed -- warming up from scratch today.", symbol)
                hist_rows = []
            self._history_bars_cache[symbol] = sorted(_to_bars(hist_rows), key=lambda b: b.ts)
        today_rows = await fetch_upstox_intraday_1m(stock_resolve.resolve_eq_instrument_key(symbol), token)
        today_bars = sorted(_to_bars(today_rows), key=lambda b: b.ts)
        return self._history_bars_cache[symbol] + today_bars

    # ── Entry ────────────────────────────────────────────────────────────

    async def _check_entry(self, symbol: str, token: str) -> None:
        bias = self._bias[symbol]
        bars = await self._bars_with_history(symbol, token)
        if not bars:
            return
        bars_e = to_n_min_bars_market_anchored(bars, self._entry_timeframe_min)
        closes_e = [b.close for b in bars_e]
        k, d = compute_stoch_rsi_double_smoothed(closes_e, *self._entry_stoch_rsi_lengths)
        # 2026-09-30 CRITICAL FIX: now that bars_e can include PRIOR DAYS'
        # bars (needed to warm up k/d above), the entry-time gate must also
        # require the bar's DATE is genuinely today -- b.ts.time() alone
        # would let a prior day's bar at e.g. 10:00 wrongly satisfy
        # "time >= start_time" and fire an entry off yesterday's data.
        idx = next(
            (i for i, b in enumerate(bars_e)
             if b.ts.date() == self._today and b.ts.time() >= self._start_time
             and check_entry_state(k[i], d[i], bias)),
            None)
        if idx is None:
            # 2026-09-30, direct user request (same visibility gap already
            # fixed for sell_straddle's SELECT trace and OI-ORB's selection
            # funnel this same session): this branch used to return in
            # total silence -- indistinguishable from "broken" vs "still
            # waiting on its own momentum state." Log the latest K/D and
            # the exact condition still required, mirrored by bias
            # (bearish needs D>K, bullish needs K>D -- check_entry_state's
            # own rule), so a quiet stock is diagnosable from the log alone.
            _last_k = k[-1] if k else None
            _last_d = d[-1] if d else None
            _need = "D>K" if bias == "bearish" else "K>D"
            _k_s = f"{_last_k:.2f}" if isinstance(_last_k, float) else "N/A"
            _d_s = f"{_last_d:.2f}" if isinstance(_last_d, float) else "N/A"
            self._clog.info(
                "%s: WAIT-ENTRY bias=%s need %s -- K=%s D=%s (not yet, %d %dmin bars so far)",
                symbol, bias, _need, _k_s, _d_s, len(bars_e), self._entry_timeframe_min,
            )
            return

        option_type = "CE" if bias == "bullish" else "PE"
        # 2026-09-30 CRITICAL FIX: bars now includes prior-day history for
        # StochRSI warmup -- must match TODAY's 09:15 bar specifically, not
        # any historical day's.
        bar_915 = next((b for b in bars if b.ts.date() == self._today and b.ts.time() == dtime(9, 15)), None)
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
        entry_ts = datetime.now(IST)

        # Baseline OI reading for the rolling "bias flips twice" exit check
        # below (strategies.oi_bias_breakout.detector.classify_oi_bias) --
        # the SAME frozen ATM/OTM strikes used for the entry bias, read
        # fresh at entry time so the first re-check has a real prior
        # reading to compare against.
        atm_call_oi = await self._current_oi(symbol, strikes.atm, "CE", token)
        otm_call_oi = await self._current_oi(symbol, strikes.otm_call, "CE", token)
        atm_put_oi = await self._current_oi(symbol, strikes.atm, "PE", token)
        otm_put_oi = await self._current_oi(symbol, strikes.otm_put, "PE", token)

        pos = {
            "option_type": option_type, "strike": contract.strike, "expiry": expiry,
            "qty": qty, "entry_price": entry_price, "entry_ts": entry_ts,
            "upstox_key": contract.upstox_key,
            "strikes": strikes,
            "prev_oi": {"atm_call": atm_call_oi, "otm_call": otm_call_oi,
                        "atm_put": atm_put_oi, "otm_put": otm_put_oi},
            "oi_bias_history": [],
            "next_oi_check": entry_ts + timedelta(minutes=self._oi_recheck_minutes),
        }
        row_id = await asyncio.to_thread(
            store.record_entry, self._client_id, self._binding_id, symbol, pos,
            self._entry_timeframe_min, self._entry_stoch_rsi_lengths,
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

    async def _check_oi_bias_flip(self, symbol: str, pos: dict, bias: str, token: str) -> bool:
        """2026-09-29, direct user request: the third exit condition from
        the original frozen spec, deferred at first ship -- re-runs the OI
        bias on the SAME frozen ATM/OTM strikes every OI_RECHECK_MINUTES,
        using classify_oi_bias (single-transition, cross-referenced:
        OTM Call vs ATM Put for bullish, OTM Put vs ATM Call for bearish --
        distinct from the stricter both-transition, ATM+OTM-summed rule
        classify_combined_oi_bias uses once at entry). Exits once the
        opposite-of-entry bias has read twice (not necessarily
        consecutively) via count_opposite_bias_readings. Returns True if
        this fired an exit (caller must not also check the StochRSI
        crossover once this has already closed the position)."""
        now = datetime.now(IST)
        if now < pos["next_oi_check"]:
            return False
        strikes = pos["strikes"]
        prev = pos["prev_oi"]
        cur = {
            "atm_call": await self._current_oi(symbol, strikes.atm, "CE", token),
            "otm_call": await self._current_oi(symbol, strikes.otm_call, "CE", token),
            "atm_put": await self._current_oi(symbol, strikes.atm, "PE", token),
            "otm_put": await self._current_oi(symbol, strikes.otm_put, "PE", token),
        }
        reading = classify_oi_bias(
            otm_call_oi_920=prev["otm_call"], otm_call_oi_925=cur["otm_call"],
            atm_put_oi_920=prev["atm_put"], atm_put_oi_925=cur["atm_put"],
            otm_put_oi_920=prev["otm_put"], otm_put_oi_925=cur["otm_put"],
            atm_call_oi_920=prev["atm_call"], atm_call_oi_925=cur["atm_call"],
        )
        pos["oi_bias_history"].append(reading)
        pos["prev_oi"] = cur
        pos["next_oi_check"] = now + timedelta(minutes=self._oi_recheck_minutes)
        self._clog.info("%s: OI re-check reading=%s (entry_bias=%s) history=%s",
                         symbol, reading, bias, pos["oi_bias_history"])
        if count_opposite_bias_readings(pos["oi_bias_history"], bias) >= self._oi_bias_flip_count:
            await self._close_position(symbol, "oi_bias_flip_twice", token)
            return True
        return False

    async def _check_exit(self, symbol: str, token: str) -> None:
        pos = self._positions.get(symbol)
        if pos is None:
            return
        bias = self._bias[symbol]

        if await self._check_oi_bias_flip(symbol, pos, bias, token):
            return

        # 2026-09-30 CRITICAL FIX, direct user correction: prior-day history
        # is needed to warm up StochRSI here too -- at 75-min exit bars,
        # (21,21,3,3) needs ~46 bars = ~9.2 TRADING DAYS to ever produce a
        # real K/D from scratch. The existing bucket_close<=entry_ts guard
        # below already safely excludes any historical (pre-entry_ts) bar
        # from ever being treated as a real post-entry exit signal -- the
        # historical prefix here exists purely to warm up k/d.
        bars = await self._bars_with_history(symbol, token)
        if not bars:
            return
        bars_x = to_n_min_bars_market_anchored(bars, self._exit_timeframe_min)
        closes_x = [b.close for b in bars_x]
        k, d = compute_stoch_rsi_double_smoothed(closes_x, *self._exit_stoch_rsi_lengths)
        entry_ts = pos["entry_ts"]
        crossed = False
        for i in range(1, len(bars_x)):
            bucket_close = bars_x[i].ts + timedelta(minutes=self._exit_timeframe_min)
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
            self._exit_timeframe_min, self._exit_stoch_rsi_lengths,
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
