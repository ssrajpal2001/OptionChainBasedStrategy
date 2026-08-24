"""
strategies/oi_orb_screener/engine.py -- OiOrbScreenerStrategy, the live/
paper_route book for the OI-Spurt + ORB screener.

Fully standalone (see strategies/oi_orb_screener/__init__.py). One book per
(client, binding) -- unlike every other strategy here, its underlying is
NOT fixed at deployment time: screener.build_shortlist() picks a fresh set
of F&O STOCKS every trading day, so this book can hold several concurrent
positions (one per shortlisted stock), keyed by stock symbol.

Scope for this pass, per direct user instruction 2026-08-24 (see the plan
at C:\\Users\\SERVER\\.claude\\plans\\curried-snuggling-sunrise.md): prove
that a fired signal (1) places a real paper_route order through the broker
and (2) subscribes to the resulting option's live LTP. Deliberately NO
SL/target/trailing/hard-risk-cap logic -- EOD square-off is the ONLY exit
this pass. That comes in a follow-up before any real live capital sits
behind this.

Pipeline per trading day (mirrors colab/oi_orb_screener/screener_nse_direct.
py's run_screener_and_monitor(), adapted from a blocking script loop to a
non-blocking asyncio book):
  1. Wait until actionable (>=09:10 IST, <ENTRY_WINDOW_END).
  2. One build_shortlist() call (wrapped asyncio.to_thread -- blocking NSE
     I/O must never run on the event loop, CLAUDE.md "Development Notes").
  3. Poll fetch_fno_price_universe every POLL_SECONDS, feed MinuteBars.
  4. Freeze ORB + NIFTY regime at ORB_END.
  5. Evaluate breakouts during the entry window; on a fired Signal, resolve
     the real contract (stock_resolve.py), subscribe its live option feed
     BEFORE placing the order (so entry_price is a real live LTP, not a
     guess), then emit a BUY OiOrbOrderEvent.
  6. EOD square-off loop closes every open position at squareoff_time.

Persistence + audit trail (added 2026-08-24, same day as the pass above,
after a real incident): strategies/oi_orb_screener/store.py is a dedicated
SQLite DB (data/oi_orb_screener.db) that (a) persists every open position so
a restart no longer silently loses it -- restored on this book's very first
_daily_loop iteration, before _run_today_pipeline() re-evaluates today's
signals (see _restore_from_db()) -- and (b) logs the full "why" trail every
day: every scan outcome, every shortlisted stock, every ORB level, every
fired/rejected/skipped signal and every entry-path failure reason, plus the
closed-trade P&L. Built because this strategy is running a full month in
paper mode before any live-capital decision -- the evaluation needs SQL-
queryable history, not just a JSONL/log grep. See store.py's own module
docstring for the real incident (DIXON PE14500, 2026-08-24) that motivated
the position-persistence half of this.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Optional, Set

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick
from strategies.core.base_book import AbstractStrategyBook
from strategies.oi_orb_screener import screener
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener import store
from strategies.oi_orb_screener.events import OiOrbOrderEvent, OiOrbFillEvent

logger = logging.getLogger(__name__)

_EOD_TIME_DEFAULT = dtime(15, 15)
_EOD_POLL_SEC = 10.0
_ENTRY_LTP_WAIT_TIMEOUT_SEC = 5.0
_UNDERLYING_SENTINEL = "SCREENER"
# 2026-08-24, confirmed live: an aggressive retry pattern here (many
# attempts, short spacing, each doing its own internal re-warm) can make
# an Akamai throttle WORSE rather than let it clear -- confirmed on EC2,
# see screener.py's NSESession docstring for the full incident. Kept
# deliberately light: few attempts, spaced minutes apart, not seconds.
_BUILD_SHORTLIST_MAX_ATTEMPTS = 3
_BUILD_SHORTLIST_RETRY_SEC = 180.0


def _make_strategy_logger(client_id: str, binding_id: str) -> logging.Logger:
    """Dedicated, rotating, per-(client,binding,day) log file -- same
    utils.logging_utils.make_strategy_logger platform utility every other
    strategy here uses."""
    from utils.logging_utils import make_strategy_logger
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"oiorb_{client_id}_{binding_id}_{date_str}", propagate=False)


class OiOrbScreenerStrategy(AbstractStrategyBook):
    """One instance per (client, binding). underlying is always the
    sentinel "SCREENER" -- the real stocks traded are chosen dynamically
    each day (mirrors D1 Trap FnO's own WATCHLIST sentinel precedent)."""

    def __init__(
        self,
        bus,
        cfg,
        client_id: str,
        binding_id: str,
        lot_multiplier: int = 1,
        product_type: str = "MIS",
        squareoff_time: str = "15:15",
        oi_spurt_min_pct: float = 7.0,
        price_move_min_pct: float = 2.0,
        stock_move_abort_pct: float = 4.0,
        top_n_per_side: int = 5,
        poll_seconds: int = 20,
        regime_filter_enabled: bool = True,
        ignore_time_windows: bool = False,
    ) -> None:
        super().__init__(bus, cfg, _UNDERLYING_SENTINEL, client_id, binding_id)
        self._strategy_name = "oi_orb_screener"
        self._lot_multiplier = max(1, lot_multiplier)
        self._product_type = product_type
        try:
            _h, _m = str(squareoff_time or "15:15").split(":")
            self._squareoff_time = dtime(int(_h), int(_m))
        except Exception:
            self._squareoff_time = _EOD_TIME_DEFAULT

        self._screener_cfg = dict(screener.CONFIG)
        self._screener_cfg["OI_SPURT_MIN_PCT"] = oi_spurt_min_pct
        self._screener_cfg["PRICE_MOVE_MIN_PCT"] = price_move_min_pct
        self._screener_cfg["STOCK_MOVE_ABORT_PCT"] = stock_move_abort_pct
        self._screener_cfg["TOP_N_PER_SIDE"] = top_n_per_side
        self._screener_cfg["POLL_SECONDS"] = poll_seconds
        self._screener_cfg["REGIME_FILTER_ENABLED"] = regime_filter_enabled
        self._screener_cfg["IGNORE_TIME_WINDOWS"] = ignore_time_windows

        self._clog = _make_strategy_logger(client_id, binding_id)

        # ── daily scan/ORB state ────────────────────────────────────────
        self._today: Optional[date] = None
        self._nse: Optional["screener.NSESession"] = None
        self._shortlist_symbols: list = []
        self._shortlist_pchange: dict = {}
        self._prev_close_map: dict = {}
        self._bars = screener.MinuteBars()
        self._orb_frozen: dict = {}
        self._regime: Optional[str] = None
        self._already_fired: set = set()
        self._entry_window_done_logged = False
        # "50% rejection rule" state (screener.check_rejection_pattern) --
        # running post-ORB extreme per symbol, and the set of (symbol, side)
        # pairs already marked rejected for today (never re-evaluated even
        # on a later legitimate breakout).
        self._peak_since_orb: Dict[str, float] = {}
        self._trough_since_orb: Dict[str, float] = {}
        self._rejected: set = set()
        # 8-SMA exit state: last minute-bar key seen per symbol with an open
        # position, so the exit check runs once per COMPLETED candle, not
        # once per poll (which could still be mid-candle).
        self._last_sma_check_key: Dict[str, str] = {}

        # ── contract/feed/position state, keyed by stock symbol ────────
        self._pending_contracts: Dict[str, "stock_resolve.ResolvedContract"] = {}
        self._pending_fills: Dict[str, dict] = {}   # event_id -> context
        self._pending_closes: Dict[str, str] = {}   # event_id -> exit reason (OiOrbFillEvent carries no reason field)
        self._positions: Dict[str, dict] = {}        # stock symbol -> position dict
        self._live_option_ltp: Dict[str, float] = {}
        self._option_key_subscribed: Dict[str, str] = {}
        self._eod_closing: Set[str] = set()

    # ── lifecycle ────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        """New-day reset -- clears scanning/ORB/signal state only. Open
        positions are deliberately NOT touched here: the EOD loop is what's
        responsible for closing them, and this book assumes (same as every
        other strategy here) that a position never genuinely survives past
        its own day's square-off while the process keeps running."""
        self._nse = None
        self._shortlist_symbols = []
        self._shortlist_pchange = {}
        self._prev_close_map = {}
        self._bars = screener.MinuteBars()
        self._orb_frozen = {}
        self._regime = None
        self._already_fired = set()
        self._entry_window_done_logged = False
        self._peak_since_orb = {}
        self._trough_since_orb = {}
        self._rejected = set()
        self._last_sma_check_key = {}
        self._clog.info("OiOrb[%s/%s]: session reset for new trading day.",
                         self._client_id, self._binding_id)

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.OI_ORB_ORDER_FILL)
        self._subscribe(Topic.OPTION_TICK)
        self._tasks.append(asyncio.create_task(
            self._daily_loop(), name=f"oiorb_daily_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._fill_loop(), name=f"oiorb_fill_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"oiorb_opttick_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"oiorb_eod_{self._client_id}_{self._binding_id}"))
        self._clog.info("OiOrb[%s/%s]: started.", self._client_id, self._binding_id)

    # ── daily pipeline ───────────────────────────────────────────────────

    async def _daily_loop(self) -> None:
        while self._running:
            now = datetime.now(IST)
            if self._today != now.date():
                self.reset_session()
                self._today = now.date()
                try:
                    await self._restore_from_db()
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: restore-from-DB failed (recovered, "
                                          "starting flat with no already-fired/rejected memory).",
                                          self._client_id, self._binding_id)
                try:
                    await self._run_today_pipeline()
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: today's pipeline crashed (recovered, "
                                          "will retry next day-rollover check).",
                                          self._client_id, self._binding_id)
            await asyncio.sleep(30)

    async def _restore_from_db(self) -> None:
        """Restore open positions + today's already-fired/rejected signal
        state from the DB -- called once, right after reset_session() on
        this book's first _daily_loop iteration (i.e. every process
        start/restart), BEFORE _run_today_pipeline() re-evaluates today's
        signals. Without this, a mid-day restart both silently lost every
        open position (real 2026-08-24 incident: DIXON PE14500, entered
        13:51, restart ~14:40, position never closed, just forgotten) AND
        could re-fire (and potentially duplicate-enter) a signal that had
        already fired before the restart, since reset_session() always
        starts _already_fired/_rejected empty (same incident: DIXON PUT
        re-signaled at 14:40:32, only harmless because contract resolution
        happened to fail on the retry)."""
        td = self._today.isoformat() if self._today else None
        rows = await asyncio.to_thread(store.load_open_positions, self._client_id, self._binding_id, td)
        for r in rows:
            contract = await stock_resolve.resolve_contract_exact_async(
                r["symbol"], r["expiry"], r["strike"], r["option_type"])
            if contract is None:
                self._clog.critical(
                    "OiOrb[%s/%s]: RESTORE FAILED for %s %s%d -- could not re-resolve the "
                    "contract. This position is still marked open in the DB and may still be "
                    "open at the broker, but this process cannot track it (no SMA-exit/EOD-close "
                    "will fire for it) until manually reconciled.",
                    self._client_id, self._binding_id, r["symbol"], r["option_type"], r["strike"])
                continue
            self._positions[r["symbol"]] = {
                "contract": contract, "qty": r["qty"], "entry_price": r["entry_price"],
                "paper_mode": bool(r["paper_mode"]), "opened_at": datetime.fromisoformat(r["entry_ts"]),
            }
            self._ensure_option_feed(r["symbol"], contract)
            self._clog.info("OiOrb[%s/%s]: RESTORED open position %s %s%d qty=%d @ %.2f from DB.",
                             self._client_id, self._binding_id, r["symbol"],
                             contract.option_type, contract.strike, r["qty"], r["entry_price"])

        already_fired = await asyncio.to_thread(store.load_already_fired, self._client_id, self._binding_id, td)
        rejected = await asyncio.to_thread(store.load_rejected, self._client_id, self._binding_id, td)
        self._already_fired |= already_fired
        self._rejected |= rejected
        if already_fired or rejected:
            self._clog.info("OiOrb[%s/%s]: restored %d already-fired + %d rejected signal(s) from DB.",
                             self._client_id, self._binding_id, len(already_fired), len(rejected))

    async def _run_today_pipeline(self) -> None:
        cfg = self._screener_cfg
        if not await self._wait_until_actionable(cfg):
            self._clog.info("OiOrb[%s/%s]: started too late for today (past %s) -- no scan run.",
                             self._client_id, self._binding_id, cfg["ENTRY_WINDOW_END"])
            return

        self._nse = await asyncio.to_thread(screener.NSESession)
        # 2026-08-24, confirmed live: without ANY retry, a single transient
        # NSE/Akamai hiccup silently kills the WHOLE trading day, since
        # _daily_loop only calls this once per calendar day -- worth
        # retrying a FEW times. But also confirmed live the same day: a
        # heavier retry pattern (many attempts, short spacing) can make an
        # Akamai throttle WORSE, not better -- kept deliberately light
        # (few attempts, minutes apart, see _BUILD_SHORTLIST_* constants
        # and screener.py's NSESession docstring for the full incident).
        shortlist = None
        nifty_pchange = 0.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, _BUILD_SHORTLIST_MAX_ATTEMPTS + 1):
            try:
                shortlist, nifty_pchange = await asyncio.to_thread(
                    screener.build_shortlist, self._nse, cfg)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                self._clog.warning(
                    "OiOrb[%s/%s]: build_shortlist attempt %d/%d failed: %s",
                    self._client_id, self._binding_id, attempt, _BUILD_SHORTLIST_MAX_ATTEMPTS, exc,
                )
                if attempt < _BUILD_SHORTLIST_MAX_ATTEMPTS:
                    await asyncio.sleep(_BUILD_SHORTLIST_RETRY_SEC)
                    self._nse = await asyncio.to_thread(screener.NSESession)  # fresh session/cookies
        if last_exc is not None:
            self._clog.error(
                "OiOrb[%s/%s]: build_shortlist failed after %d attempts, giving up for today: %s",
                self._client_id, self._binding_id, _BUILD_SHORTLIST_MAX_ATTEMPTS, last_exc,
            )
            await asyncio.to_thread(store.record_scan, self._client_id, self._binding_id,
                                     nifty_pchange, "fetch_failed", str(last_exc))
            return

        if shortlist is None or shortlist.empty:
            self._clog.info("OiOrb[%s/%s]: no candidates passed the filters today (NIFTY pChange %+.2f%%).",
                             self._client_id, self._binding_id, nifty_pchange)
            await asyncio.to_thread(store.record_scan, self._client_id, self._binding_id,
                                     nifty_pchange, "no_candidates")
            return

        self._shortlist_symbols = shortlist["symbol"].tolist()
        self._prev_close_map = (shortlist.set_index("symbol")["previousClose"].to_dict()
                                 if "previousClose" in shortlist.columns else {})
        # pChange sign tells you which side of the regime table each stock is
        # even before any ORB level exists -- bullish (pChange>0) watches for
        # a CALL on ORB-high breakout, bearish (pChange<0) watches for a PUT
        # on ORB-low breakdown. Surfaced in monitoring_state() so the
        # dashboard panel isn't just "ORB pending" with zero directional
        # signal while ORB levels are still empty/pending.
        self._shortlist_pchange = (shortlist.set_index("symbol")["pChange"].to_dict()
                                    if "pChange" in shortlist.columns else {})
        self._clog.info("OiOrb[%s/%s]: shortlist ready (%d): %s",
                         self._client_id, self._binding_id, len(self._shortlist_symbols),
                         ", ".join(f"{s}({self._shortlist_pchange.get(s, 0):+.2f}%)"
                                   for s in self._shortlist_symbols))

        await asyncio.to_thread(store.record_scan, self._client_id, self._binding_id, nifty_pchange, "ok")
        sl_indexed = shortlist.set_index("symbol")
        shortlist_rows = []
        for sym in self._shortlist_symbols:
            row = sl_indexed.loc[sym] if sym in sl_indexed.index else None
            shortlist_rows.append({
                "symbol": sym,
                "price_change_pct": self._shortlist_pchange.get(sym),
                "oi_spurt_pct": (float(row["oi_spurt_pct"])
                                 if row is not None and "oi_spurt_pct" in shortlist.columns else None),
                "score": float(row["score"]) if row is not None and "score" in shortlist.columns else None,
                "side_bias": "bullish" if self._shortlist_pchange.get(sym, 0) > 0 else "bearish",
            })
        await asyncio.to_thread(store.record_shortlist, self._client_id, self._binding_id, shortlist_rows)

        await asyncio.to_thread(screener.backfill_orb_from_yahoo, self._bars, self._shortlist_symbols, cfg)

        start_time = datetime.now(IST)
        deadline = start_time + timedelta(minutes=cfg["MAX_MONITOR_MINUTES"])

        while self._running and datetime.now(IST) < deadline:
            now = datetime.now(IST)
            now_key = now.strftime("%H:%M")

            try:
                live = await asyncio.to_thread(screener.fetch_fno_price_universe, self._nse)
                live = live.set_index("symbol")
            except Exception as exc:
                self._clog.warning("OiOrb[%s/%s]: live quote fetch failed, retrying next cycle: %s",
                                    self._client_id, self._binding_id, exc)
                await asyncio.sleep(cfg["POLL_SECONDS"])
                continue

            for sym in self._shortlist_symbols:
                if sym not in live.index:
                    continue
                ltp = float(live.loc[sym, "lastPrice"])
                self._bars.on_quote(sym, ltp, now)
                # "50% rejection rule" tracking -- only meaningful once the
                # ORB level actually exists to measure a push beyond.
                orb_lvl = self._orb_frozen.get(sym)
                if orb_lvl is not None:
                    orb_high, orb_low = orb_lvl
                    self._peak_since_orb[sym] = max(self._peak_since_orb.get(sym, ltp), ltp)
                    self._trough_since_orb[sym] = min(self._trough_since_orb.get(sym, ltp), ltp)
                    if (sym, "CALL") not in self._rejected and screener.check_rejection_pattern(
                            self._peak_since_orb[sym], orb_high, ltp, "CALL",
                            cfg["REJECTION_MIN_RISE_PCT"], cfg["REJECTION_RETRACE_FRACTION"]):
                        self._rejected.add((sym, "CALL"))
                        self._clog.info("OiOrb[%s/%s]: %s CALL marked REJECTED (50%% rejection rule) -- "
                                         "peak=%.2f orb_high=%.2f current=%.2f",
                                         self._client_id, self._binding_id, sym,
                                         self._peak_since_orb[sym], orb_high, ltp)
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sym,
                            "rejection_rule_triggered", side="CALL",
                            detail=f"peak={self._peak_since_orb[sym]:.2f} orb_high={orb_high:.2f} current={ltp:.2f}",
                            trigger_price=ltp, orb_high=orb_high, orb_low=orb_low)
                    if (sym, "PUT") not in self._rejected and screener.check_rejection_pattern(
                            self._trough_since_orb[sym], orb_low, ltp, "PUT",
                            cfg["REJECTION_MIN_RISE_PCT"], cfg["REJECTION_RETRACE_FRACTION"]):
                        self._rejected.add((sym, "PUT"))
                        self._clog.info("OiOrb[%s/%s]: %s PUT marked REJECTED (50%% rejection rule) -- "
                                         "trough=%.2f orb_low=%.2f current=%.2f",
                                         self._client_id, self._binding_id, sym,
                                         self._trough_since_orb[sym], orb_low, ltp)
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sym,
                            "rejection_rule_triggered", side="PUT",
                            detail=f"trough={self._trough_since_orb[sym]:.2f} orb_low={orb_low:.2f} current={ltp:.2f}",
                            trigger_price=ltp, orb_high=orb_high, orb_low=orb_low)

            await self._check_sma_exits(now, live)

            if self._regime is None and (now_key >= cfg["ORB_END"] or cfg.get("IGNORE_TIME_WINDOWS")):
                try:
                    nifty_pchange_now = await asyncio.to_thread(screener.fetch_nifty_pchange, self._nse)
                except Exception as exc:
                    self._clog.warning("OiOrb[%s/%s]: NIFTY regime fetch failed at freeze time: %s",
                                        self._client_id, self._binding_id, exc)
                    nifty_pchange_now = 0.0
                self._regime = screener.classify_nifty_regime(nifty_pchange_now, cfg)
                await asyncio.to_thread(store.update_scan_regime, self._client_id, self._binding_id, self._regime)
                for sym in self._shortlist_symbols:
                    h, l = self._bars.orb(sym, cfg["ORB_START"], cfg["ORB_END"])
                    if h is not None:
                        self._orb_frozen[sym] = (h, l)
                        await asyncio.to_thread(store.update_orb_levels, self._client_id, self._binding_id,
                                                 sym, h, l)
                self._clog.info("OiOrb[%s/%s]: ORB frozen. NIFTY regime=%s (pChange %+.2f%%). Levels: %s",
                                 self._client_id, self._binding_id, self._regime.upper(), nifty_pchange_now,
                                 {s: v for s, v in self._orb_frozen.items()})

            entry_window_open = cfg.get("IGNORE_TIME_WINDOWS") or (
                cfg["ENTRY_WINDOW_START"] <= now_key < cfg["ENTRY_WINDOW_END"])
            if self._regime is not None and entry_window_open:
                for sym in self._shortlist_symbols:
                    if sym not in self._orb_frozen or sym not in live.index:
                        continue
                    orb_high, orb_low = self._orb_frozen[sym]
                    ltp = float(live.loc[sym, "lastPrice"])
                    prev_close = self._prev_close_map.get(sym, 0.0)
                    sig = screener.evaluate_breakout(sym, ltp, prev_close, orb_high, orb_low,
                                                      self._regime, self._already_fired, cfg, now=now)
                    if sig is not None and (sig.symbol, sig.side) in self._rejected:
                        self._clog.info("OiOrb[%s/%s]: %s %s breakout fired but skipped -- "
                                         "already REJECTED (50%% rejection rule) earlier today.",
                                         self._client_id, self._binding_id, sig.symbol, sig.side)
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                            "signal_skipped_rejected", side=sig.side, detail=sig.reason,
                            trigger_price=sig.trigger_price, orb_high=sig.orb_high, orb_low=sig.orb_low)
                        sig = None
                    if sig is not None:
                        self._clog.info("OiOrb[%s/%s]: SIGNAL %s BUY %s trigger=%.2f ORB=%.2f-%.2f reason=%s",
                                         self._client_id, self._binding_id, sig.symbol, sig.side,
                                         sig.trigger_price, sig.orb_low, sig.orb_high, sig.reason)
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                            "signal_fired", side=sig.side, detail=sig.reason,
                            trigger_price=sig.trigger_price, orb_high=sig.orb_high, orb_low=sig.orb_low)
                        asyncio.create_task(self._handle_signal(sig))
            elif (not cfg.get("IGNORE_TIME_WINDOWS") and now_key >= cfg["ENTRY_WINDOW_END"]
                  and not self._entry_window_done_logged):
                self._entry_window_done_logged = True
                self._clog.info("OiOrb[%s/%s]: entry window closed for today (%s). Monitoring stops; "
                                 "EOD loop will still square off any open positions.",
                                 self._client_id, self._binding_id, cfg["ENTRY_WINDOW_END"])
                break

            await asyncio.sleep(cfg["POLL_SECONDS"])

    async def _wait_until_actionable(self, cfg) -> bool:
        """2026-08-24, direct user spec: "it should scan for stocks after
        9.25am only... application will start at 9.25.05am" -- waits for
        SCAN_START (default 09:25, matching the new 09:15-09:25 ORB window),
        not the old "any time after 09:10" gate. Since the whole pipeline
        now never starts before the ORB window has already closed, live
        polling can never build real 09:15-09:25 bars itself -- the Yahoo
        backfill is the ONLY source of them, every day (see screener.py's
        ORB_END docstring)."""
        if cfg.get("IGNORE_TIME_WINDOWS"):
            return True
        scan_start = cfg.get("SCAN_START", "09:25")
        while self._running:
            now = datetime.now(IST)
            now_key = now.strftime("%H:%M")
            if now_key >= cfg["ENTRY_WINDOW_END"]:
                return False
            if now_key >= scan_start:
                return True
            await asyncio.sleep(15)
        return False

    # ── signal → contract resolution → order ────────────────────────────

    async def _handle_signal(self, sig: "screener.Signal") -> None:
        if sig.symbol in self._positions or sig.symbol in self._pending_contracts:
            self._clog.info("OiOrb[%s/%s]: %s already has an open/pending position -- skipping duplicate signal.",
                             self._client_id, self._binding_id, sig.symbol)
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                "signal_skipped_duplicate", side=sig.side, trigger_price=sig.trigger_price,
                orb_high=sig.orb_high, orb_low=sig.orb_low)
            return

        opt_type = "CE" if sig.side == "CALL" else "PE"

        lot = await stock_resolve.resolve_lot_async(sig.symbol)
        if lot <= 0:
            self._clog.warning("OiOrb[%s/%s]: could not resolve lot size for %s -- skipping entry.",
                                self._client_id, self._binding_id, sig.symbol)
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                "lot_resolve_failed", side=sig.side, trigger_price=sig.trigger_price,
                orb_high=sig.orb_high, orb_low=sig.orb_low)
            return

        # 2026-08-24, direct user spec: strike is 2% OTM (above spot for a
        # CALL, below spot for a PUT), not ATM -- resolve_contract rounds
        # whatever raw price it's given to the nearest valid strike step.
        otm_frac = self._screener_cfg.get("STRIKE_OTM_PCT", 2.0) / 100.0
        raw_strike = sig.trigger_price * (1 + otm_frac if opt_type == "CE" else 1 - otm_frac)

        contract = await stock_resolve.resolve_contract_async(sig.symbol, raw_strike, opt_type)
        if contract is None:
            self._clog.warning("OiOrb[%s/%s]: could not resolve option contract for %s %s -- skipping entry.",
                                self._client_id, self._binding_id, sig.symbol, opt_type)
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                "contract_resolve_failed", side=sig.side, trigger_price=sig.trigger_price,
                orb_high=sig.orb_high, orb_low=sig.orb_low)
            return

        self._pending_contracts[sig.symbol] = contract
        self._ensure_option_feed(sig.symbol, contract)

        entry_price = await self._await_first_ltp(sig.symbol, timeout=_ENTRY_LTP_WAIT_TIMEOUT_SEC)
        if entry_price <= 0:
            self._clog.warning(
                "OiOrb[%s/%s]: no live option LTP for %s %s%d within %.0fs -- skipping entry "
                "(feed may still warm up; will retry on the next fired signal, if any).",
                self._client_id, self._binding_id, sig.symbol, opt_type, contract.strike,
                _ENTRY_LTP_WAIT_TIMEOUT_SEC,
            )
            self._pending_contracts.pop(sig.symbol, None)
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                "entry_ltp_timeout", side=sig.side,
                detail=f"{opt_type}{contract.strike}", trigger_price=sig.trigger_price,
                orb_high=sig.orb_high, orb_low=sig.orb_low)
            return

        qty = lot * self._lot_multiplier
        event_id = f"{self._client_id}_{self._binding_id}_{sig.symbol}_{contract.strike}{opt_type}_{int(_time.time())}"
        self._pending_fills[event_id] = {
            "symbol": sig.symbol, "contract": contract, "qty": qty,
            "entry_price": entry_price, "reason": sig.reason,
        }

        order_ev = OiOrbOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="BUY",
            underlying=sig.symbol, option_type=opt_type, strike=contract.strike,
            expiry=contract.expiry, quantity=qty, entry_price=entry_price,
            reason=sig.reason, event_id=event_id, entry_ts=datetime.now(IST),
            product_type=self._product_type, strategy=self._strategy_name,
        )
        self._clog.info("OiOrb[%s/%s]: emitting BUY %s %s%d exp=%s qty=%d @ %.2f event_id=%s",
                         self._client_id, self._binding_id, sig.symbol, opt_type, contract.strike,
                         contract.expiry, qty, entry_price, event_id)
        await self._bus.publish(Topic.OI_ORB_ORDER_REQUEST, order_ev)

    async def _await_first_ltp(self, stock_symbol: str, timeout: float) -> float:
        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            ltp = self._live_option_ltp.get(stock_symbol, 0.0)
            if ltp > 0:
                return ltp
            await asyncio.sleep(0.2)
        return 0.0

    def _ensure_option_feed(self, stock_symbol: str, contract: "stock_resolve.ResolvedContract") -> None:
        """Subscribe the live feed to this contract BEFORE the order is
        placed (unlike every other strategy here, which subscribes only
        AFTER a fill) -- deliberate, since the whole point of this pass is
        proving the feed subscription works, and a real entry_price needs a
        live tick to exist first anyway. Idempotent per stock symbol."""
        key = contract.upstox_key
        if not key or self._option_key_subscribed.get(stock_symbol) == key:
            return
        gf = getattr(self._bus, "_global_feeder", None)
        if gf is None or not hasattr(gf, "subscribe_tokens"):
            self._clog.warning("OiOrb[%s/%s]: no live GlobalFeeder available -- cannot subscribe %s.",
                                self._client_id, self._binding_id, stock_symbol)
            return
        asyncio.create_task(gf.subscribe_tokens([key]))
        self._option_key_subscribed[stock_symbol] = key
        self._clog.info("OiOrb[%s/%s]: subscribed live option feed for %s %s%d (%s).",
                         self._client_id, self._binding_id, stock_symbol,
                         contract.option_type, contract.strike, key)

    async def _option_tick_loop(self) -> None:
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            return
        while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            contract = self._pending_contracts.get(tick.underlying) or \
                (self._positions.get(tick.underlying) or {}).get("contract")
            if contract is None:
                continue
            if (tick.strike == contract.strike and tick.option_type == contract.option_type
                    and tick.expiry == contract.expiry):
                self._live_option_ltp[tick.underlying] = tick.ltp
                self._clog.debug("OiOrb[%s/%s]: LTP %s %s%d = %.2f",
                                  self._client_id, self._binding_id, tick.underlying,
                                  contract.option_type, contract.strike, tick.ltp)

    # ── fills ────────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        q = self._loop_queues.get(Topic.OI_ORB_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, OiOrbFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id:
                continue
            try:
                await self._on_fill(ev)
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: _on_fill error (recovered).",
                                      self._client_id, self._binding_id)

    async def _on_fill(self, fill: OiOrbFillEvent) -> None:
        eid = getattr(fill, "event_id", "")
        if fill.action == "BUY":
            pending = self._pending_fills.pop(eid, None)
            if pending is None:
                return
            symbol = pending["symbol"]
            if getattr(fill, "entry_aborted", False):
                self._pending_contracts.pop(symbol, None)
                self._clog.critical("OiOrb[%s/%s]: ENTRY ABORTED for %s (event_id=%s) -- discarding.",
                                     self._client_id, self._binding_id, symbol, eid)
                await asyncio.to_thread(
                    store.log_signal_event, self._client_id, self._binding_id, symbol,
                    "entry_aborted", detail=eid)
                return
            contract = self._pending_contracts.pop(symbol, pending["contract"])
            entry_price = float(fill.fill_price or pending["entry_price"])
            paper_mode = bool(getattr(fill, "paper_mode", True))
            self._positions[symbol] = {
                "contract": contract, "qty": pending["qty"],
                "entry_price": entry_price,
                "paper_mode": paper_mode,
                "opened_at": datetime.now(IST),
            }
            self._clog.info("OiOrb[%s/%s]: ENTRY CONFIRMED %s %s%d qty=%d @ %.2f (paper_mode=%s)",
                             self._client_id, self._binding_id, symbol, contract.option_type,
                             contract.strike, pending["qty"], fill.fill_price, fill.paper_mode)
            await asyncio.to_thread(
                store.open_position, self._client_id, self._binding_id, symbol,
                contract.option_type, contract.strike, contract.expiry.isoformat(),
                pending["qty"], entry_price, pending.get("reason", ""), paper_mode, eid)
            return

        if fill.action == "SELL":
            symbol = fill.underlying
            self._eod_closing.discard(symbol)
            pos = self._positions.pop(symbol, None)
            if getattr(fill, "exit_failed", False):
                # Leave the position untouched so the next EOD cycle retries the close --
                # same confirm-then-finalize discipline every strategy here follows. Keep
                # _pending_closes[eid] too -- the retry re-emits with a NEW event_id via
                # _emit_close, so this stale one is simply abandoned, not consumed.
                if pos is not None:
                    self._positions[symbol] = pos
                self._clog.critical("OiOrb[%s/%s]: EXIT FAILED for %s (event_id=%s) -- will retry.",
                                     self._client_id, self._binding_id, symbol, eid)
                await asyncio.to_thread(
                    store.log_signal_event, self._client_id, self._binding_id, symbol,
                    "exit_failed", detail=eid)
                return
            exit_reason = self._pending_closes.pop(eid, "")
            if pos is not None:
                pnl = round((fill.fill_price - pos["entry_price"]) * pos["qty"], 2)
                self._clog.info("OiOrb[%s/%s]: EXIT CONFIRMED %s qty=%d @ %.2f (entry %.2f) P&L=%.2f",
                                 self._client_id, self._binding_id, symbol, pos["qty"],
                                 fill.fill_price, pos["entry_price"], pnl)
                await asyncio.to_thread(
                    store.close_position, self._client_id, self._binding_id, symbol,
                    fill.fill_price, exit_reason, pnl)

    # ── EOD square-off (the ONLY exit logic this pass) ──────────────────

    async def _emit_close(self, symbol: str, pos: dict, reason: str) -> None:
        contract = pos["contract"]
        exit_price = self._live_option_ltp.get(symbol, pos["entry_price"])
        event_id = f"{self._client_id}_{self._binding_id}_{symbol}_{reason}_{int(_time.time())}"
        order_ev = OiOrbOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="SELL",
            underlying=symbol, option_type=contract.option_type, strike=contract.strike,
            expiry=contract.expiry, quantity=pos["qty"], entry_price=pos["entry_price"],
            exit_price=exit_price, reason=reason, event_id=event_id,
            product_type=self._product_type, strategy=self._strategy_name,
        )
        self._pending_closes[event_id] = reason
        self._clog.info("OiOrb[%s/%s]: closing %s qty=%d @ %.2f reason=%s",
                         self._client_id, self._binding_id, symbol, pos["qty"], exit_price, reason)
        await self._bus.publish(Topic.OI_ORB_ORDER_REQUEST, order_ev)

    async def _check_sma_exits(self, now: datetime, live) -> None:
        """2026-08-24, direct user spec: exit an open position on
        SMA_EXIT_CONSEC_CLOSES consecutive candle closes on the wrong side
        of an SMA_PERIOD SMA of the UNDERLYING STOCK's own closes (not the
        option premium). Runs once per COMPLETED 1-min candle per symbol
        (guarded by _last_sma_check_key), not once per poll -- checking a
        still-forming bar's partial close would be checking against a
        number that hasn't actually closed yet."""
        if not self._positions:
            return
        cfg = self._screener_cfg
        current_key = now.strftime("%H:%M")
        for symbol, pos in list(self._positions.items()):
            if symbol in self._eod_closing:
                continue
            if self._last_sma_check_key.get(symbol) == current_key:
                continue  # already checked this minute
            self._last_sma_check_key[symbol] = current_key
            closes = self._bars.closes(symbol, before=current_key)
            side = "CALL" if pos["contract"].option_type == "CE" else "PUT"
            if screener.check_sma_exit(closes, cfg["SMA_PERIOD"], cfg["SMA_EXIT_CONSEC_CLOSES"], side):
                self._eod_closing.add(symbol)
                sma = screener.compute_sma(closes, cfg["SMA_PERIOD"])
                self._clog.info(
                    "OiOrb[%s/%s]: %s SMA EXIT -- last %d closes %s %d-SMA=%.2f (closes=%s)",
                    self._client_id, self._binding_id, symbol, cfg["SMA_EXIT_CONSEC_CLOSES"],
                    "below" if side == "CALL" else "above", cfg["SMA_PERIOD"], sma,
                    closes[-cfg["SMA_EXIT_CONSEC_CLOSES"]:],
                )
                await asyncio.to_thread(
                    store.log_signal_event, self._client_id, self._binding_id, symbol,
                    "sma_exit_triggered", side=side,
                    detail=f"{cfg['SMA_PERIOD']}-SMA={sma:.2f} closes={closes[-cfg['SMA_EXIT_CONSEC_CLOSES']:]}")
                await self._emit_close(symbol, pos, "sma_exit")

    async def _eod_loop(self) -> None:
        while self._running:
            now = datetime.now(IST)
            if now.time() >= self._squareoff_time and self._positions:
                for symbol, pos in list(self._positions.items()):
                    if symbol in self._eod_closing:
                        continue
                    self._eod_closing.add(symbol)
                    await self._emit_close(symbol, pos, "eod_squareoff")
            await asyncio.sleep(_EOD_POLL_SEC)

    async def liquidate(self, reason: str = "kill_switch") -> None:
        """Immediate close of every open position, for the firm-wide kill
        switch / graceful shutdown path (StrategyBookManager.liquidate_all
        calls book.liquidate(reason) if present, before stopping tasks)."""
        for symbol, pos in list(self._positions.items()):
            if symbol in self._eod_closing:
                continue
            self._eod_closing.add(symbol)
            await self._emit_close(symbol, pos, reason)

    # ── monitoring (no dashboard UI wiring this pass) ───────────────────

    def monitoring_state(self) -> dict:
        positions = {}
        for sym, p in self._positions.items():
            ltp = self._live_option_ltp.get(sym)
            entry = p["entry_price"]
            pnl = round((ltp - entry) * p["qty"], 2) if ltp is not None and entry else None
            pnl_pct = round((ltp - entry) / entry * 100.0, 2) if ltp is not None and entry else None
            opened_at = p.get("opened_at")
            positions[sym] = {
                "option_type": p["contract"].option_type,
                "strike": p["contract"].strike,
                "qty": p["qty"],
                "entry_price": entry,
                "live_ltp": ltp,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "opened_at": opened_at.isoformat() if hasattr(opened_at, "isoformat") else opened_at,
            }
        return {
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "today": self._today.isoformat() if self._today else None,
            "shortlist": self._shortlist_symbols,
            "shortlist_pchange": self._shortlist_pchange,
            "regime": self._regime,
            "orb_frozen": self._orb_frozen,
            "positions": positions,
        }
