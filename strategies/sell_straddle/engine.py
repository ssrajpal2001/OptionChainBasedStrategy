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
import time as _time
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
from strategies.sell_straddle.r1_breach_reentry import R1BreachReentryMixin
from strategies.sell_straddle.indicators import IndicatorMixin
from strategies.sell_straddle.rolling import RollingMixin

logger = logging.getLogger(__name__)

_BUF = 600
_MARKET_OPEN = dtime(9, 15)


def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "",
                           strategy_name: str = "sell_straddle") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    # 2026-09-07: byte-identical filename for the default "sell_straddle" so no
    # existing log file's naming changes; a non-default strategy_name (e.g.
    # sell_straddle_calc_vwap, run side-by-side on the SAME client/binding/
    # underlying for a VWAP-source A/B comparison) gets its own suffixed file
    # instead of interleaving into the plain sell_straddle book's log.
    if strategy_name and strategy_name != "sell_straddle":
        tag = f"{tag}_{strategy_name}"
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
                           ConfigMixin, IndicatorMixin, EntryMixin, ExitMixin, RollingMixin,
                           R1BreachReentryMixin):

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        underlying: str = "NIFTY",
        lot_multiplier: int = 1,
        client_id: str = "",
        binding_id: str = "",
        shadow_on_reject: bool = False,
        vwap_source_override: Optional[str] = None,
        strategy_name: str = "sell_straddle",
    ) -> None:
        if cfg is None:
            from config.global_config import GlobalConfig
            cfg = GlobalConfig()
        super().__init__(bus, cfg, underlying, client_id, binding_id)
        # 2026-09-07: distinguishes this book from a plain sell_straddle book running
        # on the SAME (client,binding,underlying) -- e.g. "sell_straddle_calc_vwap",
        # a deliberate side-by-side VWAP-source A/B comparison on one broker account
        # (see StraddleBookManager._wanted()'s own docstring for the collision this
        # fixes). Threaded into _persist_key/log tag/PositionUpdateMixin's broadcast
        # label so the two books never share a session file, log file, or mislabel
        # their live position_update events.
        self._strategy_name = strategy_name or "sell_straddle"
        PositionUpdateMixin.__init__(self, bus, client_id, binding_id, self._strategy_name, underlying)
        self._lot_multiplier = lot_multiplier
        self._client_db = None
        # 2026-08-12, direct request, opt-in per deployment (strategy_params
        # {"shadow_on_reject": true}): when the broker rejects an order this
        # strategy would otherwise abort the position entirely (see
        # OrderPlacementFailed handling in straddle_bridge.py). With this flag
        # set, the bridge instead falls back to a local paper-style fill --
        # SAME code path a genuinely paper-mode deployment already uses, just
        # triggered by a rejection instead of the binding's own trading_mode --
        # so the position runs full real exit logic (SL/TSL/Day%/etc.)
        # against a fill that never actually reached the broker. Never
        # confused with a real position: paper_mode=True on every fill this
        # produces, same flag the UI already uses to badge paper trades.
        self._shadow_on_reject = shadow_on_reject

        # 2026-09-03, direct user spec: per-deployment vwap_source override
        # (strategy_params {"vwap_source": "calculative"|"broker_atp"}), for
        # running two paper bindings side-by-side on the SAME client and
        # underlying with genuinely independent VWAP behavior -- the admin/
        # client-level RuntimeConfig resolution (SellStraddleConfig.
        # vwap_source, see config.py) is scoped by (underlying, client_id),
        # NOT binding_id, so it alone cannot differentiate two bindings under
        # the same client trading the same index. Set once here and never
        # touched again by the periodic config-apply reload (see ConfigMixin
        # in config.py, which only assigns self._vwap_source from cfg when
        # this override is None) -- same pattern shadow_on_reject already
        # uses to survive config reloads untouched.
        self._vwap_source_override = (
            vwap_source_override if vwap_source_override in ("broker_atp", "calculative") else None
        )

        self._position: Optional[StraddlePosition] = None
        self._trades_today: int = 0

        self._spot: float = 0.0
        self._spot_reject_streak: int = 0  # consecutive suspect-jump ticks ignored
        # 2026-08-26, direct user spec revision: for a futures_atm underlying, track
        # the real spot AND the futures price SEPARATELY (self._spot stays the real
        # spot -- unchanged meaning), and derive self._atm_ref = round(mean(spot,
        # futures)/step)*step as the ONLY value used to compute the `atm` strike for
        # entry/expiry-shift selection. Everywhere else that already reads self._spot
        # (intrinsic/time-value stripping, P&L, ITM checks) is deliberately untouched --
        # only ATM/strike selection uses the mean now. self._futures_spot stays 0.0
        # (and _atm_ref falls back to self._spot) for any underlying not listed in
        # cfg.futures_atm_underlyings, so this is a complete no-op elsewhere.
        self._futures_spot: float = 0.0
        self._atm_ref: float = 0.0
        _fa = {u.upper() for u in (getattr(self._cfg, "futures_atm_underlyings", None) or [])}
        self._uses_mean_atm: bool = self._underlying.upper() in _fa
        self._ce_ltp: float = 0.0
        self._pe_ltp: float = 0.0
        self._ce_atp: float = 0.0
        self._pe_atp: float = 0.0
        self._prev_vwap_atp: Optional[float] = None
        self._prev_slope: Optional[float] = None
        self._strike_prem: Dict[Tuple[int, str], dict] = {}
        self._prev_atp_closed: Dict[Tuple[int, str], float] = {}
        self._itm_gate_armed: bool = False
        self._ltp_target: float = 0.0

        self._market_open_dt: Optional[datetime] = None
        # 2026-08-26 fix: this process/book's own start time -- see start()'s own
        # comment for the real incident this fixes. None until start() actually
        # runs; _is_primed treats None as "no extra restart-anchor" (falls back
        # to the pre-fix, market-open-only anchor) rather than crashing, so a
        # test/harness that constructs a book without calling start() is unaffected.
        self._process_start_dt: Optional[datetime] = None
        self._primed: bool = False
        self._order_pending: bool = False
        self._roll_close_waiters: Dict[str, asyncio.Event] = {}
        # event_id -> the StraddleFillEvent that woke the matching waiter above (both leg-closes
        # and full-position closes). Consumed by _close_leg / _close_position to see whether the
        # wake-up was a REAL confirmed exit or an exit_aborted (broker unavailable) fill.
        self._roll_close_results: Dict[str, object] = {}
        # EOD hedge-and-carry (2026-08-20): own waiter/result dicts, own fill loop,
        # own Topic (STRADDLE_HEDGE_ORDER_FILL) -- deliberately parallel to, never
        # sharing state with, the sold-leg _roll_close_waiters/_roll_close_results
        # above, since a hedge leg is the opposite order direction (buy-to-open).
        self._hedge_fill_waiters: Dict[str, asyncio.Event] = {}
        self._hedge_fill_results: Dict[str, object] = {}
        # A hedge's protective legs outlive the SOLD pair they were built against --
        # a rollover/re-entry fully closes the old StraddlePosition and constructs a
        # brand-new one. Stashed here (by _close_position, right before the old
        # position object is discarded) so _open_position can carry them onto the
        # fresh position and re-check the same-strike-collision guard.
        self._pending_hedge_ce_leg = None
        self._pending_hedge_pe_leg = None
        # 2026-08-24 user spec: T-1-from-expiry no longer force-closes a hedge
        # candidate/standing hedge -- it ROLLS to next week's expiry instead
        # (the exchange settles the current week's contracts at expiry
        # regardless of what this code does, so "carry through expiry" has
        # to mean rolling onto fresh contracts, not literally holding the
        # same ones past their own settlement). _start_hedge_roll closes
        # whatever's currently open and sets these; _try_complete_hedge_roll
        # (checked every tick from the entry loop) opens the fresh sold pair
        # + fresh hedge once next week's ATM strikes have live data. NOTE: if
        # the process restarts in the narrow window while this is pending
        # (old legs already closed for real, new ones not opened yet), the
        # roll is simply abandoned on restart rather than persisted/resumed
        # -- accepted as a low-probability, low-severity gap (nothing is
        # left unprotected, since the old position was already genuinely
        # closed; worst case is just a missed roll that quarter's normal EOD
        # logic would reconsider the next day).
        self._hedge_roll_pending: bool = False
        self._hedge_roll_reason: str = ""
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
        # 2026-08-25 fix (real incident -- see _eod_close_or_hedge / _check_exits):
        # _eod_decision_in_progress is a synchronous reentrancy guard around the
        # whole EOD hedge/close decision. _tick_loop and _eod_backstop_loop are
        # two INDEPENDENT asyncio tasks that can both reach _check_exits() for the
        # same still-"open" position while a hedge build (up to ~30s, two
        # sequential order-confirm waits) from the OTHER task is still in flight --
        # pos.status only flips to "closing" once an actual close is dispatched,
        # never while a hedge is merely being built, so without this flag a second
        # task could start a duplicate hedge attempt (or close) on the same
        # position mid-build. _prehedge_attempted_today gates the new
        # pre-squareoff hedge precheck (see _hedge_precheck_time) to once per day.
        self._eod_decision_in_progress: bool = False
        self._prehedge_attempted_today: bool = False
        # 2026-08-07: a live entry that reached the broker and was rejected/aborted
        # (insufficient funds, asymmetric fill, etc — entry_aborted, NOT
        # placement_failed which already stops-for-day on its own first
        # occurrence) used to just cooldown-and-retry forever, hammering the
        # broker with the same doomed order all session. Now stops entries for
        # the day after 3 CONSECUTIVE such rejections (resets to 0 on any real
        # confirmed entry) — same "3 tries then stop" principle already applied
        # to placement failures. See _on_fill's entry_aborted branch.
        self._consecutive_entry_rejections: int = 0

        self._post_restore_warmup: bool = False
        self._post_restore_at: float = 0.0
        self._post_restore_warmup_clock_start: Optional[float] = None
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
        # 2026-08-23, direct user spec: "if ltp is less than threshold then jump to
        # next week expiry -- applicable for anchor selection part... if we have
        # entered next expiry, that expiry will be used for the complete trading
        # day till EOD." Once True, _effective_entry_expiry() stops recomputing
        # from scratch and just holds self._entry_expiry_date fixed for the rest
        # of the day -- see that method's own updated docstring.
        self._expiry_shifted_low_anchor_ltp: bool = False
        # 2026-09-07 CRITICAL FIX, real incident: distinguishes WHY the sticky
        # pin above was armed. The genuine "expiry-day shift" reason (comment
        # above) is meant to persist for the whole day, across multiple
        # entries/exits, by design. But _restore_position() (below) also sets
        # the SAME flag purely so a restart doesn't forget an EXISTING open
        # position's own expiry -- that reason should NOT outlive the
        # position it was protecting. Real incident: after that restored
        # position closed, the pin stayed stuck on its (stale, no-longer-
        # relevant) expiry for the rest of the day; pool warm-seeding kept
        # failing against that stale expiry ("no token/spot"), and since this
        # book's main loop is tick-driven, a subscription that never
        # resolves means the book silently never wakes up again -- no
        # exception, just permanent silence until the next restart. See
        # _close_position()'s own use of this flag for the actual fix.
        self._entry_expiry_pinned_from_restore: bool = False

        # 2026-08-23, direct user spec: "entry price should come from the broker
        # which is connected to the client. If broker doesn't send the data we
        # can manually enter the price in UI and click save and then application
        # will move depending on the price which is entered." When a full 2-leg
        # ENTRY's broker fill confirmation is aborted/times out (the existing
        # _on_fill ENTRY-abort branch, which discards the optimistic position by
        # design), the strikes/expiry/qty that were ALREADY decided are retained
        # here separately from self._position so the client can later supply the
        # real fill price(s) they see on their OWN broker terminal and have the
        # app adopt the trade as genuinely open, instead of it staying silently
        # discarded while a real position may be sitting open and unmonitored at
        # the broker. See entries.py's manual_confirm_entry()/
        # discard_aborted_entry(). None whenever there is nothing pending.
        self._last_aborted_entry: Optional[dict] = None

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

        self._clog: logging.Logger = _make_strategy_logger(
            underlying, client_id, binding_id, self._strategy_name)
        self._load_thresholds()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    @property
    def _persist_key(self) -> str:
        # 2026-09-07: suffix is the real strategy_name, not a hardcoded literal --
        # byte-identical to before for the default "sell_straddle" (preserves every
        # existing book's session-restore key untouched), but a book running under
        # "sell_straddle_calc_vwap" on the SAME (client,binding,underlying) now gets
        # its own distinct persistence key instead of silently sharing/clobbering
        # the plain sell_straddle book's session file.
        suffix = self._strategy_name
        if self._client_id and self._binding_id:
            return f"{self._client_id}_{self._binding_id}_{self._underlying}_{suffix}"
        return f"{self._underlying}_{suffix}"

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
        # 2026-09-07: also stamp strategy_name so the bridge's trade_history.record()
        # call can attribute the row to the real book (sell_straddle vs
        # sell_straddle_calc_vwap) instead of a hardcoded "sell_straddle" literal --
        # otherwise two books trading the SAME (client,binding,underlying) for an
        # A/B VWAP-source comparison would produce indistinguishable history rows.
        ev.strategy_name = self._strategy_name
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
                    # 2026-08-31, direct user spec: this critical alert used to go ONLY
                    # to the module-level logger (pm2's stdout capture, erasable by
                    # `pm2 flush`) -- never to self._clog, which is a dedicated rotating
                    # file per binding that pm2 flush cannot touch. Mirror it so this
                    # exact evidence survives independently of pm2's log lifecycle (real
                    # incident: investigating a missing-after-restart position turned
                    # into a dead end because the only copy of this diagnostic had
                    # already been flushed away).
                    self._clog.critical(
                        "POSITION PERSIST FAILED TWICE -- on-disk state may be STALE/"
                        "DESYNCED from the real in-memory position. Check disk space/"
                        "permissions NOW."
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
                _stack = "".join(traceback.format_stack(limit=10))
                logger.warning(
                    "SellStraddle[%s|%s|%s]: _persist() CLEARING position store "
                    "(self._position=%r) -- call stack:\n%s",
                    self._underlying, getattr(self, "_client_id", "") or "-",
                    getattr(self, "_binding_id", "") or "-", self._position,
                    _stack,
                )
                # 2026-08-31: mirrored to self._clog (see the PERSIST FAILED TWICE
                # comment above for why -- this exact diagnostic branch is THE one a
                # 2026-08-31 investigation needed and couldn't find, because the only
                # copy lived in the pm2-managed log a `pm2 flush` had already erased.
                self._clog.warning(
                    "_persist() CLEARING position store (self._position=%r) -- "
                    "call stack:\n%s", self._position, _stack,
                )
                _ok = self.clear(self._persist_key)
                if not _ok:
                    _ok = self.clear(self._persist_key)
                # 2026-09-16, direct user spec: the position is genuinely
                # closed now (this branch only runs when self._position is
                # falsy/not-open) -- any cross-day carry-forward pool state
                # from a prior hedge-and-carry episode on this same
                # (client,binding,underlying,strategy) key is stale and must
                # not be picked up by a future, unrelated fresh position.
                # Best-effort: a failure here only leaves harmless stale data
                # behind (the NEXT _persist_pool_engine() call for the new
                # position, if any, overwrites it before it could ever be
                # read -- _restore_pool_engine() only ever reads this key
                # while THIS process is starting, never mid-session), never
                # blocks the real position clear above.
                try:
                    self.clear(self._persist_key + "_pool_carry")
                except Exception:
                    pass
                try:
                    self.clear(self._persist_key + "_session_carry")
                except Exception:
                    pass
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
                    self._clog.critical(
                        "POSITION CLEAR FAILED TWICE -- a stale 'still open' file may "
                        "be left on disk. Check disk space/permissions NOW."
                    )
                self.notify_position_update(None, force=True)
        except Exception as exc:
            logger.warning("SellStraddle[%s]: persist failed: %s", self._underlying, exc)
            try:
                self._clog.warning("persist failed: %s", exc)
            except Exception:
                pass
        self._persist_session()

    def _persist_session(self) -> None:
        try:
            from data_layer import position_store as _ps
            _payload = {
                "session_realized_pnl_pts": self._session_realized_pnl_pts,
                "trades_today": self._trades_today,
                "stop_for_day": self._stop_for_day,
                "session_day": str(self._session_day(datetime.now(IST))),
                "initial_net_credit": self._initial_net_credit,
                "session_min_straddle_frozen": self._session_min_straddle_frozen,
                # 2026-09-06 (stale-value audit F6): the tracked-pair key now
                # includes expiry_date (a date object) as its 3rd element --
                # serialize it as an isoformat string so it round-trips through
                # JSON; the restore side below parses it back with
                # date.fromisoformat.
                "day_low_tracked_pair": ([self._day_low_tracked_pair[0], self._day_low_tracked_pair[1]] +
                                          ([self._day_low_tracked_pair[2].isoformat()]
                                           if len(self._day_low_tracked_pair) > 2 and self._day_low_tracked_pair[2]
                                           else []))
                                         if self._day_low_tracked_pair else None,
                # 2026-08-27, direct user-found gap: the 70%-of-booked-profit roll-
                # protection budget (rolling.py's _itm_roll_protection) was armed in
                # memory only -- a restart silently wiped it while the rolled leg kept
                # running with NO protective stop at all (a real incident: a rolled CE
                # leg ran past 100% of the profit that armed it, unprotected, across a
                # restart). Persisted here so a restart restores exactly what was armed.
                "itm_roll_protection": getattr(self, "_itm_roll_protection", None) or {},
                # 2026-08-27, same audit: a stop-out's re-entry cooldown (rolling.py's
                # _apply_sl_cooldown) was in-memory only -- a restart right after a
                # stop-out silently forgot the cooldown and let the book re-enter
                # immediately, defeating the whole point of resting after a loss.
                "sl_cooldown_until": (self._sl_cooldown_until.isoformat()
                                       if getattr(self, "_sl_cooldown_until", None) else None),
                # 2026-09-06, direct user follow-up (stale-value audit F11): only
                # the ARMED decision (not the per-leg SupportResistanceCalculator
                # bar history, which safely re-warms from live 1-min bars within
                # a few minutes of restart -- same graceful degradation this
                # codebase already accepts for RSI/ROC warm-up elsewhere) is
                # persisted here. Without this, a restart after 15:00 lost the
                # arm decision outright and the R1 exit could not re-arm before
                # force_exit without a fresh day-low retest or a fresh flip to
                # profit at/after 15:15 -- the EOD backstop still covers the gap
                # either way, but re-arming immediately (rather than possibly
                # never, for the rest of a short post-15:00 window) is strictly
                # better and costs nothing to persist.
                "post1500_armed": getattr(self, "_post1500_armed", False),
                "post1500_armed_reason": getattr(self, "_post1500_armed_reason", None),
                "post1500_pair": ([self._post1500_pair[0], self._post1500_pair[1]] +
                                   ([self._post1500_pair[2].isoformat()]
                                    if len(self._post1500_pair) > 2 and self._post1500_pair[2] else []))
                                  if getattr(self, "_post1500_pair", None) else None,
            }
            _ps.save(self._persist_key + "_session", _payload, product_type="MIS")
            # 2026-09-16, direct user spec: while a position is genuinely
            # carrying overnight (is_hedged_positional=True), ALL of today's
            # session bookkeeping above -- not just session_realized_pnl_pts
            # -- needs to survive the day boundary intact, so tomorrow's
            # first tick can make an immediate, fully-informed decision
            # ("we require all data for current day so that immediate
            # decision can be taken when trade starts next day at opening
            # bell"). Concretely: _check_hedge_cumulative_profit_close's own
            # "booked" component (session_realized_pnl_pts) would otherwise
            # silently undercount real cumulative profit the day after a
            # same-day roll; itm_roll_protection (an armed 70%-of-booked-
            # profit stop on an already-rolled leg) would otherwise silently
            # disarm; sl_cooldown_until would otherwise silently lift early.
            # Same mechanism as _persist_pool_engine()'s own carry-forward
            # key: saved with product_type="NRML" so position_store's own
            # generic MIS-new-day-discard rule never wipes it, and it is
            # NEVER written at all for a normal (non-hedged) position --
            # that keeps today's existing every-day-resets-fresh behavior
            # completely unchanged for the common case.
            if self._position is not None and getattr(self._position, "is_hedged_positional", False):
                _ps.save(self._persist_key + "_session_carry", _payload, product_type="NRML")
        except Exception as exc:
            logger.debug("SellStraddle[%s]: session persist failed: %s", self._underlying, exc)

    def _restore_session(self) -> None:
        try:
            from data_layer import position_store as _ps
            # 2026-09-16, direct user spec: a position that's genuinely
            # carrying overnight gets ALL of today's session bookkeeping from
            # the SEPARATE cross-day carry key, day-boundary check skipped
            # entirely -- see _persist_session()'s own docstring for the full
            # mechanic. Peeked from the RAW saved position file directly (not
            # self._position) because this runs BEFORE start() restores
            # self._position -- see start()'s own call order (same pattern
            # _restore_pool_engine() already uses).
            _raw_pos = _ps.load(self._persist_key)
            _hedged = bool(_raw_pos and (_raw_pos.get("position") or {}).get("is_hedged_positional"))
            if _hedged:
                _sess = _ps.load(self._persist_key + "_session_carry")
                if _sess:
                    logger.info(
                        "SellStraddle[%s]: restoring CARRY-FORWARD session state (overnight "
                        "hedge-and-carry position) -- booked P&L/roll-protection/cooldowns "
                        "intact from yesterday, not reset for the new day.", self._underlying)
                else:
                    logger.info(
                        "SellStraddle[%s]: position is hedge-and-carry but no carry-forward "
                        "session state found -- starting fresh this once.", self._underlying)
            else:
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
                _saved_frozen = _sess.get("session_min_straddle_frozen", None)
                if _saved_frozen is not None:
                    self._session_min_straddle_frozen = float(_saved_frozen)
                _saved_pair = _sess.get("day_low_tracked_pair", None)
                if _saved_pair is not None:
                    # 2026-09-06 (stale-value audit F6): tolerate BOTH the old
                    # 2-element (strike, strike) shape persisted before this
                    # fix and the new 3-element (strike, strike, expiry_iso)
                    # shape -- an in-flight restart right after this deploy
                    # must not crash on an old-format record still on disk.
                    if len(_saved_pair) >= 3 and _saved_pair[2]:
                        self._day_low_tracked_pair = (
                            int(_saved_pair[0]), int(_saved_pair[1]), date.fromisoformat(str(_saved_pair[2])))
                    else:
                        self._day_low_tracked_pair = (int(_saved_pair[0]), int(_saved_pair[1]))
                # 2026-09-06 (stale-value audit F11): restore the post-1500 R1
                # ARM decision (not the calculators -- see this key's own
                # comment in _persist_session for why that's fine to lose).
                # Deliberately NOT re-validated against the current position
                # here -- _check_post1500_r1_exit's own existing pair-mismatch
                # check (self._post1500_pair != the LIVE position's current
                # strikes/expiry) already runs on every real tick and discards
                # a stale/non-matching restore automatically, same
                # restore-then-self-correct pattern already relied on for
                # _day_low_tracked_pair above.
                self._post1500_armed = bool(_sess.get("post1500_armed", False))
                self._post1500_armed_reason = _sess.get("post1500_armed_reason", None)
                _saved_p1500_pair = _sess.get("post1500_pair", None)
                if _saved_p1500_pair is not None:
                    if len(_saved_p1500_pair) >= 3 and _saved_p1500_pair[2]:
                        self._post1500_pair = (
                            int(_saved_p1500_pair[0]), int(_saved_p1500_pair[1]),
                            date.fromisoformat(str(_saved_p1500_pair[2])))
                    else:
                        self._post1500_pair = (int(_saved_p1500_pair[0]), int(_saved_p1500_pair[1]))
                # 2026-08-27: restore any armed 70%-roll-protection budget exactly as it
                # was -- without this, a restart silently wiped it and the rolled leg
                # kept running with zero protective stop (real incident).
                _saved_prot = _sess.get("itm_roll_protection", None)
                if _saved_prot:
                    if not isinstance(getattr(self, "_itm_roll_protection", None), dict):
                        self._itm_roll_protection = {}
                    self._itm_roll_protection.update(_saved_prot)
                    logger.info("SellStraddle[%s]: restored %d armed roll-protection budget(s): %s",
                                self._underlying, len(_saved_prot), list(_saved_prot.keys()))
                _saved_cooldown = _sess.get("sl_cooldown_until", None)
                if _saved_cooldown:
                    _cd = datetime.fromisoformat(_saved_cooldown)
                    if _cd > datetime.now(IST):
                        self._sl_cooldown_until = _cd
                        logger.info("SellStraddle[%s]: restored re-entry cooldown -- no re-entry "
                                    "until %s.", self._underlying, _cd.strftime("%H:%M:%S"))
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

    def _persist_pool_engine(self) -> None:
        """Persists self._pool_engine's rolling VWAP/SLOPE/RSI/ROC series
        (2026-08-21) -- restart-proofing, NOT REST-seeding. VWAP/SLOPE are
        deliberately never REST-seeded (2026-08-19 'Seed VWAP Contamination'
        fix -- REST-derived bars poisoned the intraday baseline), which left
        them exposed to a different gap: every one of today's routine mid-day
        restarts cold-starts VWAP/SLOPE from scratch, running degraded on
        whatever pair is currently open until enough fresh live ticks
        re-accumulate. Persisting the engine's OWN already-correctly-computed
        live bars (verbatim, including their original minute indices) is not
        REST-seeding -- it's the same live data surviving a restart, so the
        already-correct seed-vs-live boundary pair_indicators() relies on
        stays exactly as correct after a restore as before it."""
        try:
            from data_layer import position_store as _ps
            _ps.save(self._persist_key + "_pool", {
                "session_day": str(self._session_day(datetime.now(IST))),
                "pool_state": self._pool_engine.to_dict(),
            }, product_type="MIS")
            # 2026-09-16, direct user spec: a position that has genuinely gone
            # into EOD hedge-and-carry (is_hedged_positional=True) needs its
            # own SEPARATE, cross-day-surviving copy of the pool engine's real
            # live-computed state -- so a rollover/re-entry decision the very
            # next trading morning has a warm baseline instead of waiting
            # several minutes for VWAP/SLOPE to rebuild from scratch. This is
            # NOT a reversal of the 'Seed VWAP Contamination' fix above (which
            # banned REST-*derived* seeding specifically because REST bars
            # were subtly different from what live ticks actually produced) --
            # this carries forward the engine's OWN genuine prior-day live
            # values, the same "not REST-seeding, the same live data
            # surviving" principle the intraday _pool key above already
            # relies on, just deliberately NOT wiped at the day boundary.
            # Saved with product_type="NRML" so position_store's own generic
            # MIS-new-day-discard rule (see its own module docstring: "NRML
            # positions carry forward across days -> restored as-is") does the
            # cross-day survival for free -- no separate day-check needed here.
            # A same-day close (never actually carried) never writes this key
            # at all; once written, it's explicitly cleared the moment the
            # position is genuinely closed (see _persist()'s own clear branch).
            if self._position is not None and getattr(self._position, "is_hedged_positional", False):
                _ps.save(self._persist_key + "_pool_carry", {
                    "pool_state": self._pool_engine.to_dict(),
                }, product_type="NRML")
        except Exception as exc:
            logger.debug("SellStraddle[%s]: pool engine persist failed: %s", self._underlying, exc)

    def _restore_pool_engine(self) -> None:
        try:
            from data_layer import position_store as _ps
            # 2026-09-16, direct user spec: a position that was genuinely
            # carrying overnight (is_hedged_positional=True) gets its warm
            # pool-engine baseline from the SEPARATE cross-day carry key
            # instead of the intraday-only one below -- see
            # _persist_pool_engine()'s own docstring for the full mechanic.
            # Peeked from the RAW saved position file directly (not
            # self._position) because this runs BEFORE start() restores
            # self._position -- see start()'s own call order.
            _raw_pos = _ps.load(self._persist_key)
            if _raw_pos and (_raw_pos.get("position") or {}).get("is_hedged_positional"):
                _carry = _ps.load(self._persist_key + "_pool_carry")
                if _carry:
                    self._pool_engine.load_dict(_carry.get("pool_state") or {})
                    logger.info(
                        "SellStraddle[%s]: restored CARRY-FORWARD pool-engine state "
                        "(overnight hedge-and-carry position) -- warm from yesterday's "
                        "own real close, not cold-started.", self._underlying)
                    return
                logger.info(
                    "SellStraddle[%s]: position is hedge-and-carry but no carry-forward "
                    "pool state found -- starting VWAP/SLOPE fresh this once.",
                    self._underlying)
                return
            _saved = _ps.load(self._persist_key + "_pool")
            if not _saved:
                return
            if str(_saved.get("session_day", "")) != str(self._session_day(datetime.now(IST))):
                logger.info("SellStraddle[%s]: persisted pool-engine state is from a prior "
                            "trading day — starting VWAP/SLOPE fresh (intraday-only, by design).",
                            self._underlying)
                return
            self._pool_engine.load_dict(_saved.get("pool_state") or {})
            logger.info("SellStraddle[%s]: restored pool-engine VWAP/SLOPE/RSI/ROC state.", self._underlying)
        except Exception as exc:
            logger.debug("SellStraddle[%s]: pool engine restore failed: %s", self._underlying, exc)

    async def _pool_engine_persist_loop(self) -> None:
        """Periodic save (independent of position state -- VWAP/SLOPE matter
        just as much while scanning/flat as while holding a position)."""
        while self._running:
            try:
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                break
            self._persist_pool_engine()

    def _reapply_expiry_stickiness_from_restored_position(self) -> None:
        """2026-08-31 CRITICAL FIX (real incident: a restart during a
        same-day low-anchor-LTP expiry-shifted session orphaned an already-
        open position's live ticks entirely). The sticky-shift flag
        (self._expiry_shifted_low_anchor_ltp) is plain in-memory state,
        never persisted -- it always starts False on a fresh process, so
        _effective_entry_expiry() silently recomputed the ORIGINAL
        (unshifted) expiry after restart while a restored position's own
        .expiry_date stayed on the real, shifted contract it was actually
        entered under. _option_loop only updates a leg's ltp when
        tick.expiry == pos.expiry_date -- once self._entry_expiry_date
        (which subscriptions/strike_prem are built around) diverged from
        that, the position's own legs stopped receiving ticks entirely
        (confirmed live: CE leg frozen at its entry price for 5 straight
        minutes while OTHER strikes at the reverted expiry kept ticking
        normally), eventually caught only by the post-restore-stale-data
        safety close -- a real loss-of-tracking event the guard happened
        to catch in time, not a fix.

        Called right after restoring self._position in start() -- re-arms
        the sticky flag to the RESTORED position's own real expiry
        immediately, before any subscription/_effective_entry_expiry()
        call can run. No-op if there's no position (nothing to re-arm) or
        it has no expiry_date recorded."""
        if self._position is None or self._position.expiry_date is None:
            return
        self._entry_expiry_date = self._position.expiry_date
        self._expiry_shifted_low_anchor_ltp = True
        self._entry_expiry_pinned_from_restore = True
        logger.info(
            "SellStraddle[%s]: restored position's own expiry (%s) re-armed as "
            "the sticky entry expiry -- subscriptions/new entries stay pinned to "
            "it for the rest of today, same as before the restart.",
            self._underlying, self._position.expiry_date.isoformat(),
        )

    def start(self) -> None:
        self._running = True
        # 2026-08-26 fix (real incident, direct user report): _is_primed's own
        # anchor used to be ONLY real market-open/entry_start -- correct for a
        # genuine morning start, but on a MID-DAY RESTART (pm2 restart, common
        # during active development/testing and any real deploy) that anchor's
        # own ready_at (entry_start + priming wait) had already long passed, so
        # priming completed on the very FIRST post-restart evaluation -- with a
        # completely FRESH, EMPTY pool/strike_prem cache that had zero actual
        # time to accumulate real ticks. Confirmed live: at 13:29:46 the ATM
        # anchor showed ltp=0.00 (genuinely no tick yet), and by 13:30:05 (just
        # ~19s after restart) priming had ALREADY "completed" and the
        # low-anchor-LTP expiry-shift fired off that same stale/absent data --
        # a real trading-day contract shift the strategy will now stay stuck
        # on for the rest of the day, based on nothing but restart timing.
        # self._process_start_dt gives _is_primed a SECOND anchor -- this
        # process's own start time -- so every restart gets its own genuine
        # fresh wait, regardless of how far into the day it happens.
        self._process_start_dt = datetime.now(IST)
        self._restore_session()
        self._restore_pool_engine()
        try:
            from data_layer import position_store as _ps
            _saved = _ps.load(self._persist_key)
            if _saved:
                self._position = StraddlePosition.from_dict(_saved)
                if not self._position.lot_size:
                    self._position.lot_size = self._lot_size * self._lot_multiplier
                self._reapply_expiry_stickiness_from_restored_position()
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
                # 2026-09-29 fix: the actual 5-min timeout clock (see exits.py's
                # POST-RESTORE WARM-UP GUARD) starts from this, set lazily once
                # the market is genuinely open -- NOT from _post_restore_at
                # above, which is set at raw process-restart time and may be
                # well before market open (a pre-market daily restart used to
                # make the timeout expire before the exchange even opened).
                self._post_restore_warmup_clock_start = None
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
            asyncio.create_task(self._hedge_fill_loop(), name=f"ss_{_tag}_hedge_fill"),
            asyncio.create_task(self._eod_backstop_loop(), name=f"ss_{_tag}_eod_backstop"),
        ]
        asyncio.create_task(self._seed_pool())
        self._tasks.append(asyncio.create_task(self._pool_engine_persist_loop(), name=f"ss_{_tag}_pool_persist"))
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
                        # 2026-09-15: standing hedge legs need the same restart re-pin --
                        # otherwise a restart with an active hedge silently loses their
                        # subscription exactly like the sold legs used to before this fix.
                        _hedge_pinned = []
                        if pos.hedge_ce_leg is not None:
                            self._rebalancer.pin_strike(self._underlying, float(pos.hedge_ce_leg.strike))
                            _hedge_pinned.append(f"CE{int(pos.hedge_ce_leg.strike)}")
                        if pos.hedge_pe_leg is not None:
                            self._rebalancer.pin_strike(self._underlying, float(pos.hedge_pe_leg.strike))
                            _hedge_pinned.append(f"PE{int(pos.hedge_pe_leg.strike)}")
                        # 2026-09-17, direct user ask ("still spectical if sell straddle
                        # and iron fly strikes are subscribed to websocket immediately...
                        # i cant see that in log"): the pin calls above always ran, but
                        # this log line never explicitly said whether a standing hedge
                        # got re-subscribed too -- made it unverifiable from the log alone
                        # whether a restart's hedge legs actually resumed receiving live
                        # ticks. Now states hedge status explicitly every restart.
                        logger.info(
                            "SellStraddle[%s]: re-pinned restored position legs CE%d/PE%d "
                            "in StrikeRebalancer (pinned_strikes now %s). Hedge legs: %s.",
                            self._underlying, int(pos.ce_leg.strike), int(pos.pe_leg.strike),
                            sorted(self._rebalancer.pinned_strikes(self._underlying)),
                            (f"re-pinned {', '.join(_hedge_pinned)}" if _hedge_pinned else "none standing"),
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
            f"║ DAY-LOW REVERSAL EXIT: {'ON' if self._day_low_exit_enabled else 'OFF'} "
            f"(freeze@{self._day_low_freeze_time.strftime('%H:%M')}, exit on retest of frozen day-low)",
            f"║ POST-15:00 R1 EXIT: {'ON' if self._post1500_exit_enabled else 'OFF'} "
            f"(arms on day-low retest or profit@15:15+, then per-leg R1 close, other leg runs solo)",
            f"║ SHADOW VWAP (log-only): {'ON' if self._shadow_vwap_enabled else 'OFF'}",
            f"║ VWAP SOURCE (drives every decision): {self._vwap_source}",
            # 2026-08-26 fix (user request): hedge_carry_enabled was invisible in this banner --
            # no way to tell from the log alone whether a book's EOD hedge-and-carry behavior is
            # armed for the day without grepping config directly. precheck lead is the new
            # 2026-08-25 pre-squareoff hedge timing (see exits.py _hedge_precheck_time).
            f"║ HEDGE-AND-CARRY (EOD): {'ON' if getattr(self, '_hedge_carry_enabled', False) else 'OFF'} "
            f"(precheck {getattr(self, '_HEDGE_PRECHECK_LEAD_MIN', 1)}min before squareoff)",
            f"║ SAME-DAY EXPIRY: {'ALLOWED' if getattr(self, '_same_day_expiry_enabled', False) else 'SHIFT TO NEXT'}",
            f"║ DAY: T:{self._day_profit_target_pct:.0f}% SL:{self._day_loss_sl_pct:.0f}% "
            f"BASIS:{self._day_exit_basis.upper()}",
            f"║ DYNAMIC EXITS: {exit_rules}",
            f"║ EXIT PRIORITY: EOD→Day%→ITMgate→DayLow→LTPdecay→Ratio→ScalableTSL→exit_rules→VWAPrise",
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
        For Delta crypto (BTC/ETH) the active daily expiry is computed directly.

        2026-08-23, direct user spec: once a low-anchor-LTP shift to next
        week has fired today (self._expiry_shifted_low_anchor_ltp,
        set by _maybe_shift_expiry_for_low_anchor_ltp in entries.py), that
        choice is STICKY for the rest of the trading day -- this method is
        called from several places (start(), the periodic crypto-rollover
        check inside _tick_loop, reset_session()) that would otherwise
        recompute from scratch and silently flip back to current-week the
        next time any of them runs. Checked first, before even the crypto
        branch, so it's a single, unconditional guard every caller benefits
        from without needing its own awareness of the sticky state."""
        if self._expiry_shifted_low_anchor_ltp and self._entry_expiry_date is not None:
            return self._entry_expiry_date
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
        # 2026-08-24 fix: a standing EOD hedge-and-carry position must survive
        # the day-boundary transition even when the process keeps running
        # continuously (no restart) -- previously this unconditionally nulled
        # self._position, silently losing track of a real carried position
        # (both sold and hedge legs still genuinely open at the broker) the
        # moment the first candle of the next trading day arrived. The
        # restore-from-persistence path in start() already handled this
        # correctly, but only runs on an actual process restart, not on a
        # live day-boundary tick while already running. Every OTHER per-day
        # counter below still resets normally (today's own fresh bookkeeping)
        # -- entries.py's _maybe_try_entry already refuses any new entry
        # while self._position is non-None/not-closed, so preserving it here
        # can never race with a fresh beginning/re-entry attempt.
        _carried_hedge = (self._position
                           if self._position is not None and self._position.is_hedged_positional
                           else None)
        self._trades_today = 0
        self._position = _carried_hedge
        # A pending expiry roll (old legs already closed, new ones not opened
        # yet) does not carry across a day boundary -- abandon it rather than
        # risk opening a stale-expiry pair after a genuinely new day starts.
        self._hedge_roll_pending = False
        self._hedge_roll_reason = ""
        self._last_aborted_entry = None
        self._sl_cooldown_until = None
        self._market_open_dt = None
        self._primed = False
        self._session_realized_pnl_pts = 0.0
        self._initial_net_credit = 0.0
        self._initial_entry_time_value = 0.0
        self._stop_for_day = False
        self._eod_decision_in_progress = False
        self._prehedge_attempted_today = False
        self._consecutive_entry_rejections = 0
        self._session_min_straddle_frozen = None
        self._day_low_tracked_pair = None
        self._day_low_computing = False
        self._post1500_pair = None
        self._post1500_armed = False
        self._post1500_armed_reason = None
        self._post1500_leg_closed = {"CE": False, "PE": False}
        self._post1500_closing = {"CE": False, "PE": False}
        self._post1500_calc = {}
        self._post1500_bar_acc = {}
        self._post1500_bar_closed_at = {}
        self._shadow_vwap = {}
        self._shadow_vwap_seeding = set()
        # 2026-09-23 CRITICAL FIX, real live incident (calculative-vwap_source
        # binding, restart at 13:42:15): side -> whether the ONE-SHOT REST
        # seed for this key has finished at least once THIS process (success
        # or failure) -- see _eng_atp's own computation below for why.
        self._shadow_vwap_rest_seeded = set()
        # 2026-09-29 CRITICAL FIX, real live incident (09-28 09:20:03 false
        # vwap_rise_roll): minimum real seconds since a key's own first tick
        # before its cum_pv/cum_v is trusted -- see _eng_atp's own computation
        # for the full incident this closes.
        self._SHADOW_VWAP_MIN_AGE_SEC = 75.0
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
        # 2026-09-06, direct user follow-up (stale-value audit F4): a LIVE
        # (no-restart) day-boundary tick never reset the pool engine or the
        # legacy _prev_vwap_atp/_prev_slope -- only the restart/restore path
        # (_restore_pool_engine) had a same-day check. Left alone, the pool
        # engine's maxlen=240 per-strike deques would carry yesterday's tail
        # bars into the new session (indistinguishable from today's own bars
        # by minute-index alone) until enough new ticks aged them out, and
        # the legacy indicator fallback's first SLOPE of the new day would be
        # computed against yesterday's last combined ATP. Same rebuild used
        # by both expiry-shift paths -- a day boundary is exactly as much a
        # "genuine instrument-history change" as an expiry shift.
        from strategies.pool_indicator_engine import PoolIndicatorEngine
        _old_pool = self._pool_engine
        self._pool_engine = PoolIndicatorEngine(
            rsi_len=_old_pool._rsi_len, roc_len=_old_pool._roc_len, maxlen=_old_pool._maxlen)
        self._prev_vwap_atp = None
        self._prev_slope = None
        # Reset the low-anchor-LTP expiry-shift sticky flag BEFORE recomputing --
        # a fresh trading day starts back on the normal current-week expiry,
        # never inheriting yesterday's shift.
        self._expiry_shifted_low_anchor_ltp = False
        self._entry_expiry_pinned_from_restore = False
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
                # 2026-08-26: for a mean-ATM underlying, a futures-sourced tick updates
                # self._futures_spot, never self._spot itself -- self._spot always keeps
                # its true meaning (real index). Every other underlying only ever gets
                # source="spot" ticks, so this is a no-op there (unchanged behavior).
                _is_fut_tick = self._uses_mean_atm and getattr(tick, "source", "spot") == "futures"
                _target = self._futures_spot if _is_fut_tick else self._spot
                if _new_spot > 0:
                    if (_target > 0 and self._spot_reject_streak < 5
                            and abs(_new_spot - _target) / _target > 0.20):
                        self._spot_reject_streak += 1
                        logger.warning(
                            "SellStraddle[%s]: SUSPECT %s tick=%.2f vs last=%.2f "
                            "(%.1f%% jump) -- ignoring for this tick (streak=%d/5).",
                            self._underlying, "futures" if _is_fut_tick else "spot",
                            _new_spot, _target,
                            abs(_new_spot - _target) / _target * 100,
                            self._spot_reject_streak,
                        )
                    else:
                        self._spot_reject_streak = 0
                        if _is_fut_tick:
                            self._futures_spot = _new_spot
                        else:
                            self._spot = _new_spot
                        # mean(spot, futures), rounded per-caller -- falls back to plain
                        # spot until both streams have ticked at least once, and is a
                        # complete no-op (== self._spot) for non-mean-ATM underlyings.
                        if self._uses_mean_atm and self._spot > 0 and self._futures_spot > 0:
                            self._atm_ref = (self._spot + self._futures_spot) / 2.0
                        else:
                            self._atm_ref = self._spot
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
                    elif self._position and self._position.status == "closing":
                        _state = "position CLOSING — order in flight, awaiting broker confirmation"
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
                    # see the open/closing position without waiting for the next entry/exit event.
                    if self._position and self._position.status in ("open", "closing"):
                        self.notify_position_update(self._position.to_dict(), force=True)
                try:
                    if self._position:
                        # Route to _check_exits whenever a position object exists at all --
                        # including status=="closing" (a close is genuinely in flight; see
                        # 2026-08-06 confirm-model redesign). _check_exits itself is a no-op
                        # while "closing". The entry path must NEVER run here: _maybe_try_entry's
                        # own guard only checks == "open", so a "closing" position would
                        # otherwise fall through this dispatch into evaluating a fresh entry
                        # while the old position hasn't finished closing yet.
                        await self._check_exits()
                    else:
                        await self._maybe_try_entry(datetime.now(IST))
                except Exception as _exc:
                    logger.exception("SellStraddle[%s]: tick-handler error (recovered, engine alive): %s",
                                     self._underlying, _exc)
        finally:
            self._bus.unsubscribe(Topic.INDEX_TICK, q)
            self._loop_queues.pop("tick", None)

    _EOD_BACKSTOP_INTERVAL_SEC = 5.0

    async def _eod_backstop_loop(self) -> None:
        """2026-08-23 fix: SellStraddle had NO standalone EOD task -- unlike
        OI-Flow/Liquidity Trap, which both run _eod_loop as its own
        independent task specifically so EOD squareoff survives even if the
        main tick loop stops producing new work. self._check_exits() (the
        FULL exit ladder: EOD -> Day% -> ITMgate -> DayLow -> LTPdecay ->
        Ratio -> ScalableTSL -> exit_rules -> VWAPrise) was ONLY ever invoked
        from _tick_loop's own "a genuine IndexTick arrived" branch -- on a
        1s queue-get timeout it just `continue`s, calling nothing. This isn't
        only a "the task crashed" risk (that loop is already exception-
        guarded per-iteration): if Topic.INDEX_TICK simply stops being
        published for this underlying -- a stale/zombie feed, exactly the
        class of gap this session's own feeder-resilience work
        (DualFeeder._staleness_watchdog) was built to catch, but from a
        DIFFERENT angle: detecting the FEED is dead vs. protecting THIS
        POSITION regardless of why nothing is arriving -- _tick_loop stays
        alive and healthy with simply nothing to process, and EOD/Day%/TSL
        all silently stop being evaluated for as long as the drought lasts.
        A position could ride straight through 15:15 with zero force-exit.

        This loop is a pure backstop, not a replacement: it calls the SAME
        self._check_exits() the tick path already calls (no reimplementation
        of the exit ladder), using whatever self._spot / leg LTPs are
        currently known -- fresh if ticks are flowing normally, last-known
        if they've stopped, exactly the same "last known price" fallback
        philosophy OI-Flow/Liquidity Trap's own _eod_loop already uses.
        Concurrent calls to _check_exits() (this loop AND _tick_loop firing
        around the same moment) are already safe without any new guard here:
        _close_position() sets pos.status="closing" SYNCHRONOUSLY before its
        first await (the 2026-08-06 confirm-model redesign's own reentrancy
        guard, see exits.py), so whichever caller reaches that check first
        wins and every other concurrent caller sees status != "open" and
        returns -- this loop relies on that existing guarantee rather than
        adding a second one."""
        while self._running:
            try:
                await asyncio.sleep(self._EOD_BACKSTOP_INTERVAL_SEC)
            except asyncio.CancelledError:
                break
            await self._eod_backstop_check_once()

    async def _eod_backstop_check_once(self) -> None:
        """One backstop pass, split out of _eod_backstop_loop's own sleep
        loop so it's directly unit-testable without needing to wait through
        real 5s intervals."""
        try:
            if self._position and self._position.status == "open":
                await self._check_exits()
        except Exception:
            logger.exception("SellStraddle[%s]: _eod_backstop_loop iteration error (recovered).",
                              self._underlying)

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

    async def _hedge_fill_loop(self) -> None:
        """EOD hedge-and-carry (2026-08-20): own fill loop for
        Topic.STRADDLE_HEDGE_ORDER_FILL, completely separate from _fill_loop above
        (which only ever handles the sold-leg StraddleFillEvent flow). Same
        client_id/binding_id identity filter as _fill_loop -- this Topic is also a
        true broadcast (EventBus.publish() has no per-book routing), and two
        different books trading the same underlying concurrently is a real,
        confirmed-live scenario (see _fill_loop's own 2026-08-06 comment)."""
        from strategies.sell_straddle.hedge_events import StraddleHedgeFillEvent
        q = self._bus.subscribe(Topic.STRADDLE_HEDGE_ORDER_FILL)
        self._loop_queues["hedge_fill"] = q
        try:
            while self._running:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                if not isinstance(ev, StraddleHedgeFillEvent):
                    continue
                if ev.underlying != self._underlying:
                    continue
                if ev.client_id != self._client_id or ev.binding_id != self._binding_id:
                    continue
                self._hedge_fill_results[ev.event_id] = ev
                waiter = self._hedge_fill_waiters.get(ev.event_id)
                if waiter is not None:
                    waiter.set()
        finally:
            self._bus.unsubscribe(Topic.STRADDLE_HEDGE_ORDER_FILL, q)
            self._loop_queues.pop("hedge_fill", None)

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
                    _placement_failed = getattr(fill, "placement_failed", False)
                    if _placement_failed:
                        _reason = "placement failed after 3 retries"
                    logger.error(
                        "SellStraddle[%s]: ENTRY ABORTED (%s) — discarding optimistic position. [%s/%s]",
                        self._underlying, _reason, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                    )
                    # 2026-08-22 fix: entries.py adds this position's own credit
                    # (ce_ltp+pe_ltp, == position.net_credit at construction) to
                    # _initial_net_credit OPTIMISTICALLY, before the order's real
                    # outcome is known -- this abort branch nulled self._position
                    # on a rejection/failure but never reversed that add. Left
                    # unrolled-back, _initial_net_credit (the day%-guardrail's
                    # denominator, _day_pct() in exits.py) stays permanently
                    # inflated by a phantom credit that was never actually
                    # collected, silently weakening day_loss_sl_pct/
                    # day_profit_target_pct for the rest of the session (and
                    # across restarts, since it's persisted). Reverse the exact
                    # amount this specific optimistic position added -- sum
                    # arithmetic is order-independent, so this is safe even if
                    # other real entries added to the same running total before
                    # or after this one aborted.
                    if self._position is not None:
                        self._initial_net_credit = max(
                            0.0, self._initial_net_credit - float(self._position.net_credit or 0.0))
                        # 2026-09-06, direct user follow-up (stale-value audit): the
                        # net_credit rollback above only ever fixed half of the
                        # theta day-stop denominator problem -- _initial_entry_time_value
                        # (entries.py's optimistic MAX-ratchet update) was left
                        # permanently inflated by a phantom aborted trade's time
                        # value, silently weakening day_loss_sl_pct/
                        # day_profit_target_pct for theta-basis bindings for the
                        # rest of the session. Restore the pre-ratchet snapshot
                        # entries.py stashed on this position IF this specific
                        # optimistic entry was the one that raised it (the stash
                        # only exists on that condition) -- a genuinely safe
                        # restore even if another real entry raised the ratchet
                        # again in between, since reverting to a lower prior
                        # value only ever makes the day-stop denominator SMALLER
                        # (i.e. the guardrail fires MORE conservatively, never
                        # less), the safe direction for a live risk cap.
                        _pre_ivt = getattr(self._position, "_pre_optimistic_ivt", None)
                        if _pre_ivt is not None:
                            self._initial_entry_time_value = _pre_ivt
                        # 2026-08-23, direct user spec: retain the strikes/expiry this
                        # attempt had already decided on (before discarding the position
                        # below) so the client can later manually confirm the trade with
                        # the real fill price(s) they see on their own broker terminal --
                        # see entries.py's manual_confirm_entry(). Only ever set here, for
                        # a full 2-leg abort (the single-leg roll-reopen case above already
                        # returned via _abort_roll_reopen and never reaches this branch).
                        self._last_aborted_entry = {
                            "ce_strike": self._position.ce_leg.strike,
                            "pe_strike": self._position.pe_leg.strike,
                            "atm_at_entry": self._position.atm_at_entry,
                            "entry_spot": self._position.entry_spot,
                            "expiry_date": self._position.expiry_date,
                            "aborted_at": datetime.now(IST),
                            "reason": _reason,
                            "client_id": getattr(fill, "client_id", "") or self._client_id,
                            "binding_id": getattr(fill, "binding_id", "") or self._binding_id,
                        }
                    self._position = None
                    self._trades_today = max(0, self._trades_today - 1)
                    self._order_pending = False
                    self._roll_in_progress = False
                    self._persist()
                    # (_persist() above already pushes notify_position_update(None, force=True)
                    # via its own "clearing position store" branch -- confirmed by reading it,
                    # not assumed; a UI-push gap was the first hypothesis for the 2026-08-07
                    # ssrajpal2001 incident and disproven by a regression test that passed on
                    # pre-fix code. Real cause of the "again and again" symptom was the
                    # unconditional cooldown+retry below, fixed by the rejection counter.)
                    # Routing failures carry no broker risk; cooldown only for real asymmetric fills.
                    if not _routing_failed:
                        self._apply_sl_cooldown()
                    if _placement_failed:
                        # 2026-08-06 CONFIRM-MODEL REDESIGN: the order never even reached the
                        # broker after 3 retries -- something is genuinely wrong (connectivity,
                        # broker outage). Stop opening NEW positions for the rest of the day.
                        # This must NEVER apply to exits -- an open real position is always
                        # retried, never abandoned; see _close_position/_close_leg.
                        self._stop_for_day = True
                        logger.critical(
                            "SellStraddle[%s|%s|%s]: STOPPING ENTRIES FOR TODAY — order "
                            "placement failed after 3 retries (broker unreachable?).",
                            self._underlying, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                        )
                    elif not _routing_failed:
                        # 2026-08-07 fix: the order DID reach the broker and was rejected/
                        # aborted (e.g. insufficient funds, asymmetric fill) -- not a transport
                        # failure, so this alone isn't grounds to stop immediately (a single
                        # rejection could be transient). But left unchecked this cooldown-and-
                        # retry loops forever on a durable rejection reason (no funds doesn't
                        # fix itself) -- real incident 2026-08-07: repeated live orders sent to
                        # Zerodha and rejected every cycle, cooldown re-arming each time. Same
                        # "3 tries then stop" principle as placement_failed above, just scoped
                        # to broker-side rejections instead of transport failures. Resets to 0
                        # on any real confirmed entry (see the ENTRY-confirmed branch below).
                        self._consecutive_entry_rejections += 1
                        if self._consecutive_entry_rejections >= 3:
                            self._stop_for_day = True
                            logger.critical(
                                "SellStraddle[%s|%s|%s]: STOPPING ENTRIES FOR TODAY — %d "
                                "consecutive live entry rejections by the broker (insufficient "
                                "funds or a persistent reject reason?).",
                                self._underlying, getattr(fill, "client_id", ""), getattr(fill, "binding_id", ""),
                                self._consecutive_entry_rejections,
                            )
                    return
                if self._position and self._position.status == "open":
                    # A real confirmed entry (full 2-leg or a roll-reopen single leg) proves
                    # the broker connection works right now -- clear the rejection streak so
                    # a later, unrelated rejection doesn't inherit count from an old one.
                    self._consecutive_entry_rejections = 0
                    # 2026-08-20 fix: do NOT set `.ltp` here to the broker's real fill
                    # price. `.ltp` is a purely LIVE, continuously-updated field (already
                    # initialized from the live feed at optimistic-open in entries.py, and
                    # kept fresh every tick by the OPTION_TICK handler above) -- only
                    # `entry_price` should ever reflect the real fill. Real incident: a
                    # live Zerodha fill (CE=45.90) landed noticeably below the strategy's
                    # own live LTP estimate (CE=55.40) at entry; setting `.ltp` to the fill
                    # briefly made `pos.current_value` read ~114 instead of the genuinely
                    # traded ~122 -- and the Day-Low Reversal Exit tracker (which reads
                    # `pos.current_value` on every tick to find the pair's own running
                    # minimum) latched onto that one-tick artifact as the day's low, since
                    # it ran before the very next live tick corrected `.ltp` back. The
                    # frozen 15:00 low ended up ~8pts below anything the live market ever
                    # actually traded at, for that client only (paper/simulated fills
                    # always match the strategy's own LTP exactly, so this never surfaced
                    # there). `.ltp` is left untouched here; only `entry_price` (used for
                    # real P&L) is set from the fill.
                    _legs = getattr(fill, "legs", ["CE", "PE"])
                    if "CE" in _legs and fill.ce_fill and fill.ce_fill > 0:
                        self._position.ce_leg.entry_price = fill.ce_fill
                        if getattr(fill, "ce_symbol", ""):
                            self._position.ce_leg.symbol = fill.ce_symbol
                    if "PE" in _legs and fill.pe_fill and fill.pe_fill > 0:
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
                if getattr(fill, "accepted", False):
                    # 2026-08-06 CONFIRM-MODEL REDESIGN: the order reached the broker (a real
                    # order_id exists) -- no price yet, not a fill. _close_position/_close_leg
                    # already set pos.status="closing" synchronously the instant they started
                    # (that's what actually stops a duplicate dispatch), so this signal's only
                    # job is fast UI feedback -- do NOT touch waiters/results, that's reserved
                    # for the real terminal outcome (fill or abort) below.
                    logger.info(
                        "SellStraddle[%s|%s|%s]: EXIT accepted (order in flight) — legs=%s event_id=%s",
                        self._underlying, fill.client_id, fill.binding_id, _legtag, _eid,
                    )
                    self._clog.info("EXIT accepted (order in flight) — legs=%s event_id=%s", _legtag, _eid)
                    if self._position is not None:
                        self.notify_position_update(self._position.to_dict(), force=True)
                    return
                if getattr(fill, "exit_aborted", False) or getattr(fill, "placement_failed", False):
                    # Broker unavailable / order never confirmed / never reached the broker at
                    # all. Do NOT finalize anything here -- _close_position / _close_leg (the
                    # waiter below wakes them) are responsible for reverting to "open" and
                    # retrying later. Never treat this as a real close (2026-08-04 incident:
                    # bridge faked a successful EXIT).
                    _why = "placement failed" if getattr(fill, "placement_failed", False) else "broker unavailable"
                    logger.error(
                        "SellStraddle[%s|%s|%s]: EXIT ABORTED (%s) — legs=%s "
                        "event_id=%s. Position will revert to OPEN; will be retried.",
                        self._underlying, fill.client_id, fill.binding_id, _why, _legtag, _eid,
                    )
                    self._clog.error("EXIT ABORTED (%s) — legs=%s event_id=%s", _why, _legtag, _eid)
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

    def _update_shadow_vwap(self, key: tuple, ltp: float, tick) -> None:
        """Shadow VWAP (2026-08-28, direct user spec): a second, self-computed
        VWAP per (strike, option_type), running purely in parallel for
        after-market comparison against the real broker-ATP VWAP that
        actually drives every live decision -- this value is NEVER read by
        any entry/exit/roll check anywhere in this codebase, only logged
        (see the periodic SHADOW_VWAP log line in _option_loop above).

        cum(ltp * volume_delta) / cum(volume_delta), where volume_delta is
        the difference between this tick's own CUMULATIVE session volume
        (OptionTick.volume -- Upstox vtt / Fyers vol_traded_today, both
        confirmed live cumulative-session fields, same pattern already
        validated for OI-Flow's own volume-spike detector) and the last
        cumulative volume seen for this same (strike, side). A tick with no
        volume field, or a volume that hasn't advanced (duplicate/backwards
        tick), contributes nothing rather than corrupting the running sum.

        2026-09-06, direct user follow-up: on a strike ROLL, this used to
        start a brand-new key at cum_pv=cum_v=0 -- silently discarding
        whatever real volume/price history that strike already had since
        market open (the broker's own vwap_source="broker_atp" reflects the
        WHOLE session; a "self" series that only starts counting from the
        moment it happens to get rolled into is not a fair like-for-like
        comparison against it). Fixes this the same way
        _compute_day_low_for_pair already does for a different tracker: a
        ONE-SHOT REST fetch of today's own 1-min bars for this exact
        strike/side, fired in the background (never awaited inline here --
        this loop also carries every OPTION_TICK for the live position, and
        blocking it on a REST call for a log-only shadow value would be a
        real regression for a strategy trading real capital elsewhere).
        Live ticks keep accumulating into cum_pv/cum_v from zero exactly as
        before while the fetch is in flight; _seed_shadow_vwap_from_rest
        ADDS the REST-derived sums on top once it lands, rather than
        overwriting, so no live-tick volume seen during the fetch window is
        lost."""
        st = self._shadow_vwap.get(key)
        vol = int(getattr(tick, "volume", 0) or 0)
        if st is None:
            # 2026-09-29 fix, real live incident: stamp this key's own first-tick
            # time so _eng_atp (below) can tell a genuinely-matured accumulator
            # from a paper-thin one a few seconds old -- see that computation's
            # own comment for the full incident (a fresh key's cum_pv/cum_v was
            # trusted the instant its one-shot REST seed call returned, even when
            # that call found zero bars because the strike hadn't traded long
            # enough yet for Upstox's own intraday endpoint to have indexed a
            # candle -- a noisy, unconverged average got locked in as
            # session_min_vwap's permanent floor).
            self._shadow_vwap[key] = {
                "cum_pv": 0.0, "cum_v": 0.0, "last_vol": vol,
                "first_tick_ts": _time.monotonic(),
            }
            if key not in self._shadow_vwap_seeding:
                self._shadow_vwap_seeding.add(key)
                asyncio.create_task(self._seed_shadow_vwap_from_rest(key))
            return
        delta = vol - st.get("last_vol", 0)
        st["last_vol"] = vol
        if delta > 0:
            st["cum_pv"] += ltp * delta
            st["cum_v"] += delta

    async def _seed_shadow_vwap_from_rest(self, key: tuple) -> None:
        """One-shot background REST seed for a freshly-rolled (strike, side)
        shadow-VWAP key -- see _update_shadow_vwap's own docstring for why.
        Mirrors exits.py's _compute_day_low_for_pair (same credential/
        symbol-resolution pattern, same fetch_upstox_intraday_1m source,
        same fail-safe-to-no-op-on-any-error contract): never raises, never
        blocks a decision, degrades to "just start from zero" (the
        pre-existing behavior) on any failure -- crypto, no token, no
        broker symbol, no data, network error."""
        strike, side = key
        try:
            if getattr(self, "_is_crypto", False):
                return
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return
            pos = self._position
            if pos and pos.expiry_date:
                exp = pos.expiry_date
            elif self._entry_expiry_date:
                exp = self._entry_expiry_date
            else:
                exp = REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(strike), side, "upstox")
            if not ikey:
                return
            bars = await fetch_upstox_intraday_1m(ikey, token)
            if not bars:
                return
            cum_pv = 0.0
            cum_v = 0
            for b in bars:
                bar_vol = int(b.get("volume", 0) or 0)
                if bar_vol <= 0:
                    continue
                typical = (float(b["high"]) + float(b["low"]) + float(b["close"])) / 3.0
                cum_pv += typical * bar_vol
                cum_v += bar_vol
            if cum_v <= 0:
                return
            st = self._shadow_vwap.get(key)
            if st is None:
                return
            st["cum_pv"] += cum_pv
            st["cum_v"] += cum_v
            # 2026-09-23: originally kept at INFO for a "calculative" vwap_source
            # binding (e.g. sell_straddle_calc_vwap) on the reasoning that this
            # accumulator IS the real, decision-driving VWAP there, not just a
            # broker-ATP diagnostic -- direct user follow-up overrode that: the
            # ~85-line per-strike burst every restart/pool-resubscribe is noise
            # regardless of WHY the accumulator exists, same as any other
            # binding. Always DEBUG now; still recoverable by enabling DEBUG
            # logging if the real seed values ever need auditing again.
            self._clog.debug(
                "SHADOW_VWAP REST-SEED %s%d — %d bars, seed_vwap=%.2f (cum_v=%d) merged in "
                "(now cum_pv=%.2f cum_v=%.2f).",
                side, int(strike), len(bars), cum_pv / cum_v, cum_v, st["cum_pv"], st["cum_v"],
            )
        except Exception as exc:
            self._clog.warning("SHADOW_VWAP REST-SEED %s%d failed (non-fatal, stays zero-started): %s",
                                side, int(strike), exc)
        finally:
            self._shadow_vwap_seeding.discard(key)
            # 2026-09-23 CRITICAL FIX, real live incident: marks this key
            # "ready to trust" regardless of outcome (success, crypto skip,
            # no token, no data, exception) -- see _eng_atp's own computation
            # for the real bug this closes.
            self._shadow_vwap_rest_seeded.add(key)

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
                    _atm_src = self._atm_ref if self._atm_ref > 0 else self._spot
                    _atm = int(round(_atm_src / _step) * _step) if _atm_src > 0 else 0
                    self._clog.info("OPT_TICKS: %d option ticks/60s  ATM=%d  CE%d=%.2f PE%d=%.2f",
                                    _tick_count, _atm, _atm, self._ce_ltp, _atm, self._pe_ltp)
                    if self._uses_mean_atm:
                        # 2026-08-31, direct user request: spot/futures/mean were never
                        # actually printed anywhere -- only the resulting ATM (OPT_TICKS
                        # above), which meant verifying "did the futures mean genuinely
                        # apply" required back-solving futures price from spot+ATM by
                        # hand. Log all three inputs directly, same 60s cadence as
                        # OPT_TICKS/SHADOW_VWAP.
                        _fut_note = (" (no futures tick yet -- ATM is plain spot until one arrives)"
                                     if self._futures_spot <= 0 else "")
                        self._clog.info(
                            "FUTURES_ATM spot=%.2f futures=%.2f mean=%.2f -> ATM=%d%s",
                            self._spot, self._futures_spot, _atm_src, _atm, _fut_note,
                        )
                    # 2026-09-23, direct user spec: this line exists purely to
                    # compare self-computed VWAP against the BROKER's own ATP --
                    # meaningless (and per direct feedback, unwanted) for a
                    # "calculative" vwap_source binding, which by design doesn't
                    # use broker ATP for anything. Skipped entirely in that case;
                    # demoted to DEBUG for a plain broker-VWAP binding that only
                    # has shadow_vwap_enabled on for occasional audit purposes,
                    # so it no longer clutters the per-binding INFO log every 60s.
                    if (self._shadow_vwap_enabled and _atm > 0
                            and self._vwap_source != "calculative"):
                        _ce_shadow = self._shadow_vwap.get((_atm, "CE"), {})
                        _pe_shadow = self._shadow_vwap.get((_atm, "PE"), {})
                        _ce_sv = (_ce_shadow.get("cum_pv", 0.0) / _ce_shadow.get("cum_v", 0.0)
                                  if _ce_shadow.get("cum_v", 0.0) > 0 else 0.0)
                        _pe_sv = (_pe_shadow.get("cum_pv", 0.0) / _pe_shadow.get("cum_v", 0.0)
                                  if _pe_shadow.get("cum_v", 0.0) > 0 else 0.0)
                        self._clog.debug(
                            "SHADOW_VWAP (log-only, never used for decisions) ATM=%d | "
                            "CE atp=%.2f self=%.2f (Δ%.2f) | PE atp=%.2f self=%.2f (Δ%.2f)",
                            _atm, self._ce_atp, _ce_sv, (self._ce_atp - _ce_sv) if _ce_sv else 0.0,
                            self._pe_atp, _pe_sv, (self._pe_atp - _pe_sv) if _pe_sv else 0.0,
                        )
                    _tick_count = 0
                    _last_log_ts = now_ts
                step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
                # 2026-08-26: mean-of-spot-and-futures ATM reference for a futures_atm
                # underlying (falls back to plain self._spot otherwise) -- this is the
                # SAME atm used everywhere entry/expiry-shift selection reads it, so the
                # raw-ATM CE/PE LTP tracked below (self._ce_ltp/_pe_ltp) matches it too.
                _atm_src = self._atm_ref if self._atm_ref > 0 else self._spot
                atm = round(_atm_src / step) * step if _atm_src > 0 else 0

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
                    # 2026-09-03, direct user spec: run a paper-mode A/B comparison
                    # between the broker-ATP VWAP (default, every other deployment)
                    # and the calculative (self-computed) VWAP on two separate paper
                    # bindings. The calculative series itself is the SAME cumulative
                    # VWAP the shadow-VWAP feature already computes -- must run here
                    # regardless of shadow_vwap_enabled once this binding's decisions
                    # actually depend on it, not just when it's opted into for logging.
                    if self._shadow_vwap_enabled or self._vwap_source == "calculative":
                        self._update_shadow_vwap(_k, float(tick.ltp), tick)
                    if self._vwap_source == "calculative":
                        _sv = self._shadow_vwap.get(_k, {})
                        _cum_v = _sv.get("cum_v", 0.0)
                        # 2026-09-23 CRITICAL FIX, real live incident: the old
                        # guard only protected cum_v==0 (the literal first
                        # tick ever) -- but self._shadow_vwap is reset EMPTY
                        # on every process restart, and the REST seed that
                        # restores the real day's history is an async
                        # background task (_seed_shadow_vwap_from_rest),
                        # not synchronous. In the window between "first live
                        # tick after restart" and "REST seed lands"
                        # (confirmed live: ~1-2 real seconds), cum_v was
                        # already >0 (from just 1-2 live ticks' own volume
                        # delta) so the OLD guard trusted it -- but cum_pv/
                        # cum_v at that point is essentially just the most
                        # recent tick's own raw LTP, not a real intraday
                        # VWAP. Confirmed live: a restart at 13:42:15 produced
                        # curr_vwap=177.67 on the very first post-restart
                        # read (vs. ~191 the whole session before AND right
                        # after, once the seed landed) -- that single bad
                        # reading became session_min_vwap, and the NEXT
                        # (correct, ~191.58) reading looked like a spurious
                        # 7.83% "rise", firing an unwanted vwap_rise roll one
                        # second after restart. Now also requires the
                        # one-shot REST seed to have genuinely finished at
                        # least once THIS process (_shadow_vwap_rest_seeded,
                        # set in _seed_shadow_vwap_from_rest's own finally
                        # block, success or failure) before trusting cum_pv/
                        # cum_v -- falls back to broker ATP (same as a
                        # non-calculative binding) until then, exactly the
                        # same safe fallback the old guard already used for
                        # the cum_v==0 case.
                        _rest_ready = _k in self._shadow_vwap_rest_seeded
                        # 2026-09-29 CRITICAL FIX, real live incident (09-28
                        # 09:20:03, vwap_rise_roll fired on a false rise=5.27%):
                        # _rest_ready alone isn't enough -- the one-shot REST
                        # seed (_seed_shadow_vwap_from_rest) marks a key "ready"
                        # even when it found ZERO historical bars (by design,
                        # so crypto/no-token/no-symbol keys that can NEVER get a
                        # real seed aren't stuck on broker-ATP forever). A
                        # strike selected within the first ~60-90s of its own
                        # first trade hits exactly this: Upstox's intraday
                        # endpoint hasn't indexed a candle for it yet, the seed
                        # genuinely finds nothing, gets marked ready anyway, and
                        # cum_pv/cum_v -- still just a handful of live ticks --
                        # gets trusted as if it were a converged session VWAP.
                        # Confirmed via real REST reconstruction of the incident
                        # day: the true combined VWAP for that pair was stable
                        # at ~137-139 the whole time; the live tracker recorded
                        # a false low of 130.27 moments after entry, then read
                        # the normal settling-back-up as a 5%+ "rise".
                        # Fixed with a time-since-first-tick floor: cum_pv/cum_v
                        # is only trusted once BOTH the REST seed has had its
                        # one shot AND at least _SHADOW_VWAP_MIN_AGE_SEC real
                        # seconds have passed since this key's own first tick
                        # -- by then a real 1-min candle for this strike is
                        # guaranteed to exist (and be re-fetchable) if the
                        # strike has traded at all, so the accumulator has had
                        # a genuine chance to mature either way. Falls back to
                        # broker ATP until then, same safe fallback as before.
                        _age_ok = (_time.monotonic() - _sv.get("first_tick_ts", 0.0)
                                   ) >= self._SHADOW_VWAP_MIN_AGE_SEC
                        _eng_atp = (_sv["cum_pv"] / _cum_v) if (_cum_v > 0 and _rest_ready and _age_ok) else \
                            float(self._strike_prem[_k].get("atp", 0.0) or 0.0)
                    else:
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
                    # 2026-09-15, real incident fix: hedge legs (eod_hedge, is_hedged_positional)
                    # were bought but never had their own .ltp updated by any live tick --
                    # hedge_unrealized_pnl silently stayed frozen at 0 forever, so the combined
                    # 4-leg P&L used by _check_hedge_cumulative_profit_close was blind to the
                    # hedge's real contribution. Mirror the sold-leg matching above for whichever
                    # hedge legs are standing.
                    if pos.hedge_ce_leg is not None and tick.option_type == "CE" and \
                            abs(tick.strike - pos.hedge_ce_leg.strike) < 0.01:
                        pos.hedge_ce_leg.ltp = tick.ltp
                        if _mk > 0:
                            pos.hedge_ce_leg.mark = _mk
                    elif pos.hedge_pe_leg is not None and tick.option_type == "PE" and \
                            abs(tick.strike - pos.hedge_pe_leg.strike) < 0.01:
                        pos.hedge_pe_leg.ltp = tick.ltp
                        if _mk > 0:
                            pos.hedge_pe_leg.mark = _mk
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
            if self._is_crypto:
                self._market_open_dt = now.replace(second=0, microsecond=0)
            else:
                self._market_open_dt = now.replace(
                    hour=_MARKET_OPEN.hour, minute=_MARKET_OPEN.minute, second=0, microsecond=0,
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

        # 2026-08-25 fix (real incident): this used to independently call
        # await self._close_position("time_exit_eod") directly, right here, with
        # zero awareness of the hedge-and-carry feature. _tick_loop (via
        # _check_exits -> _eod_close_or_hedge) and _eod_backstop_loop already cover
        # EOD squareoff -- ticks arrive continuously through market hours and the
        # backstop loop re-checks every 5s independent of ticks, so this candle-close
        # copy was pure redundant duplication, not a needed safety net. Because it
        # called _close_position directly instead of going through
        # _eod_close_or_hedge, it could (and on 2026-08-25 did) close the sold legs
        # while _check_exits' own call to _try_build_hedge was still mid-flight on a
        # DIFFERENT task, orphaning the hedge leg with no sold legs left to protect.
        # Removed entirely rather than made hedge-aware here too -- EOD squareoff now
        # has exactly one decision path (_check_exits), not two kept in sync by hand.

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
