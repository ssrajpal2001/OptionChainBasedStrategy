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

        # ── contract/feed/position state, keyed by stock symbol ────────
        self._pending_contracts: Dict[str, "stock_resolve.ResolvedContract"] = {}
        self._pending_fills: Dict[str, dict] = {}   # event_id -> context
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
                    await self._run_today_pipeline()
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: today's pipeline crashed (recovered, "
                                          "will retry next day-rollover check).",
                                          self._client_id, self._binding_id)
            await asyncio.sleep(30)

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
            return

        if shortlist is None or shortlist.empty:
            self._clog.info("OiOrb[%s/%s]: no candidates passed the filters today (NIFTY pChange %+.2f%%).",
                             self._client_id, self._binding_id, nifty_pchange)
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

            if self._regime is None and (now_key >= cfg["ORB_END"] or cfg.get("IGNORE_TIME_WINDOWS")):
                try:
                    nifty_pchange_now = await asyncio.to_thread(screener.fetch_nifty_pchange, self._nse)
                except Exception as exc:
                    self._clog.warning("OiOrb[%s/%s]: NIFTY regime fetch failed at freeze time: %s",
                                        self._client_id, self._binding_id, exc)
                    nifty_pchange_now = 0.0
                self._regime = screener.classify_nifty_regime(nifty_pchange_now, cfg)
                for sym in self._shortlist_symbols:
                    h, l = self._bars.orb(sym, cfg["ORB_START"], cfg["ORB_END"])
                    if h is not None:
                        self._orb_frozen[sym] = (h, l)
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
                    if sig is not None:
                        self._clog.info("OiOrb[%s/%s]: SIGNAL %s BUY %s trigger=%.2f ORB=%.2f-%.2f reason=%s",
                                         self._client_id, self._binding_id, sig.symbol, sig.side,
                                         sig.trigger_price, sig.orb_low, sig.orb_high, sig.reason)
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
        if cfg.get("IGNORE_TIME_WINDOWS"):
            return True
        while self._running:
            now = datetime.now(IST)
            now_key = now.strftime("%H:%M")
            if now_key >= cfg["ENTRY_WINDOW_END"]:
                return False
            if now_key >= "09:10":
                return True
            await asyncio.sleep(60)
        return False

    # ── signal → contract resolution → order ────────────────────────────

    async def _handle_signal(self, sig: "screener.Signal") -> None:
        if sig.symbol in self._positions or sig.symbol in self._pending_contracts:
            self._clog.info("OiOrb[%s/%s]: %s already has an open/pending position -- skipping duplicate signal.",
                             self._client_id, self._binding_id, sig.symbol)
            return

        opt_type = "CE" if sig.side == "CALL" else "PE"

        lot = await stock_resolve.resolve_lot_async(sig.symbol)
        if lot <= 0:
            self._clog.warning("OiOrb[%s/%s]: could not resolve lot size for %s -- skipping entry.",
                                self._client_id, self._binding_id, sig.symbol)
            return

        contract = await stock_resolve.resolve_contract_async(sig.symbol, sig.trigger_price, opt_type)
        if contract is None:
            self._clog.warning("OiOrb[%s/%s]: could not resolve option contract for %s %s -- skipping entry.",
                                self._client_id, self._binding_id, sig.symbol, opt_type)
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
                self._on_fill(ev)
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: _on_fill error (recovered).",
                                      self._client_id, self._binding_id)

    def _on_fill(self, fill: OiOrbFillEvent) -> None:
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
                return
            contract = self._pending_contracts.pop(symbol, pending["contract"])
            self._positions[symbol] = {
                "contract": contract, "qty": pending["qty"],
                "entry_price": float(fill.fill_price or pending["entry_price"]),
                "paper_mode": bool(getattr(fill, "paper_mode", True)),
                "opened_at": datetime.now(IST),
            }
            self._clog.info("OiOrb[%s/%s]: ENTRY CONFIRMED %s %s%d qty=%d @ %.2f (paper_mode=%s)",
                             self._client_id, self._binding_id, symbol, contract.option_type,
                             contract.strike, pending["qty"], fill.fill_price, fill.paper_mode)
            return

        if fill.action == "SELL":
            symbol = fill.underlying
            self._eod_closing.discard(symbol)
            pos = self._positions.pop(symbol, None)
            if getattr(fill, "exit_failed", False):
                # Leave the position untouched so the next EOD cycle retries the close --
                # same confirm-then-finalize discipline every strategy here follows.
                if pos is not None:
                    self._positions[symbol] = pos
                self._clog.critical("OiOrb[%s/%s]: EXIT FAILED for %s (event_id=%s) -- will retry.",
                                     self._client_id, self._binding_id, symbol, eid)
                return
            if pos is not None:
                pnl = round((fill.fill_price - pos["entry_price"]) * pos["qty"], 2)
                self._clog.info("OiOrb[%s/%s]: EXIT CONFIRMED %s qty=%d @ %.2f (entry %.2f) P&L=%.2f",
                                 self._client_id, self._binding_id, symbol, pos["qty"],
                                 fill.fill_price, pos["entry_price"], pnl)

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
        self._clog.info("OiOrb[%s/%s]: closing %s qty=%d @ %.2f reason=%s",
                         self._client_id, self._binding_id, symbol, pos["qty"], exit_price, reason)
        await self._bus.publish(Topic.OI_ORB_ORDER_REQUEST, order_ev)

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
        return {
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "today": self._today.isoformat() if self._today else None,
            "shortlist": self._shortlist_symbols,
            "shortlist_pchange": self._shortlist_pchange,
            "regime": self._regime,
            "orb_frozen": self._orb_frozen,
            "positions": {
                sym: {
                    "option_type": p["contract"].option_type,
                    "strike": p["contract"].strike,
                    "qty": p["qty"],
                    "entry_price": p["entry_price"],
                    "live_ltp": self._live_option_ltp.get(sym),
                } for sym, p in self._positions.items()
            },
        }
