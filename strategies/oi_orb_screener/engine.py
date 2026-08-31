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
from typing import Dict, List, Optional, Set

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer.instrument_registry import REGISTRY
from matrix_engine.option_matrix import ChainRow, ChainSnapshot, OptionMatrix
from strategies.core.base_book import AbstractStrategyBook
# Shared hard ₹/lot risk-cap constant (not the S&R tracker itself, which OI-ORB
# no longer uses as of 2026-08-27 -- see _update_vwap_sl_and_check's own docstring).
from strategies.d1_trap_option.support_resistance import (
    _MAX_RISK_RS_PER_LOT as _SR_MAX_RISK_RS_PER_LOT,
)
from strategies.oi_orb_screener import filters as oi_filters
from strategies.oi_orb_screener import screener
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener import store
from strategies.oi_orb_screener.events import OiOrbOrderEvent, OiOrbFillEvent

logger = logging.getLogger(__name__)

_EOD_TIME_DEFAULT = dtime(15, 15)
_EOD_POLL_SEC = 10.0
_ENTRY_LTP_WAIT_TIMEOUT_SEC = 5.0
# 2026-08-27, direct user spec (real observation: "log is not showing which
# stock is for which side, rest of the log is blank"): before this, nothing
# logged between "ORB frozen" and an actual fired/rejected signal -- with a
# small shortlist and neither stock breaching yet, that could be the whole
# rest of the session with zero visibility into whether the book was even
# still alive/polling. A periodic status line (see _maybe_log_heartbeat)
# shows every shortlisted stock's live price against its own ORB levels.
_HEARTBEAT_INTERVAL_SEC = 60.0
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


# 2026-08-25, direct user spec: each of the 5 additive filters gets its OWN
# dedicated log file, so a scenario can be reviewed filter-by-filter
# independently -- not mixed into the main oiorb_* log.
_FILTER_NAMES = ("oi_wall", "distance_to_wall", "pcr", "volume_confirmation", "oi_roc")


def _make_filter_logger(filter_name: str, client_id: str, binding_id: str) -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"oiorb_filter_{filter_name}_{client_id}_{binding_id}_{date_str}",
                                 propagate=False)


def _build_stock_chain(stock_symbol: str, spot: float, expiry: date, depth: int) -> Optional[OptionMatrix]:
    """Construct an OptionMatrix chain tracker for a dynamically-chosen
    STOCK (not an index) -- deliberately does NOT call OptionMatrix.
    initialize(), which hardcodes cfg.exchange.strike_steps (index-only,
    defaults to a flat 50pt step) and would generate garbage, non-existent
    strikes for a stock trading well outside that grid (e.g. a ~Rs180
    stock needs a ~2.5-5pt step, not 50).

    2026-08-27 CRITICAL fix, confirmed live (same root cause as the GVT&D
    PE4350 entry-resolution bug -- see stock_resolve.resolve_contract's own
    fix comment): the price-band heuristic step assumes a UNIFORM grid, but
    a real stock's grid can switch step size across price bands (GVT&D:
    100pt around Rs4300-5000, not the assumed flat 50pt). A synthetic
    ATM+/-depth*step window built from that heuristic could include several
    strikes that were NEVER actually listed -- silently wasting subscription
    slots on rows that can never receive a real tick. Now prefers the
    REGISTRY's own already-loaded real listed strikes (REGISTRY.
    get_available_strikes) when available: picks the real strike nearest
    spot as ATM, then takes `depth` real strikes on either side of it from
    the actual grid -- correct regardless of how irregular that grid is.
    Falls back to the old heuristic-based synthetic window only when the
    registry has no strikes loaded yet for this underlying/expiry (never
    blocks chain-tracking outright -- same best-effort discipline as
    everywhere else this feature already documents)."""
    if spot <= 0 or expiry is None:
        return None
    from data_layer.instrument_registry import REGISTRY as _REGISTRY
    real_strikes = _REGISTRY.get_available_strikes(stock_symbol, expiry)
    if real_strikes:
        atm = min(real_strikes, key=lambda s: abs(s - spot))
        atm_idx = real_strikes.index(atm)
        lo = max(0, atm_idx - depth)
        hi = min(len(real_strikes), atm_idx + depth + 1)
        rows = {s: ChainRow(strike=s) for s in real_strikes[lo:hi]}
    else:
        step = stock_resolve.resolve_strike_step_for_price(stock_symbol, spot)
        if step <= 0:
            return None
        # Match stock_resolve.resolve_contract()'s own int-cast convention exactly
        # (strategies/oi_orb_screener/stock_resolve.py) -- real traded contracts, and
        # therefore real incoming OptionTick.strike values, are always int-cast even
        # for a non-integer step like 2.5. A float-keyed chain here would silently
        # never match a single real tick.
        atm_raw = round(spot / step) * step
        rows = {}
        for i in range(-depth, depth + 1):
            strike = int(round(atm_raw + i * step))
            rows[strike] = ChainRow(strike=strike)
        atm = int(round(atm_raw))
    mat = OptionMatrix(stock_symbol, None)   # cfg unused once _snap is set directly (see above)
    mat._snap = ChainSnapshot(underlying=stock_symbol, spot=spot, atm_strike=atm,
                               expiry=expiry, timestamp=datetime.now(IST), rows=rows)
    return mat


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
        nifty_bullish_pct: float = 0.3,
        nifty_bearish_pct: float = -0.3,
        max_monitor_minutes: int = 90,
        rejection_min_rise_pct: float = 2.0,
        rejection_retrace_fraction: float = 0.5,
        strike_otm_pct: float = 2.0,
        orb_start: str = "09:15",
        orb_end: str = "09:25",
        scan_start: str = "09:26",
        entry_window_start: str = "09:26",
        entry_window_end: str = "15:00",
        # 2026-08-27, direct user spec: two scan sessions -- session 1 is the
        # single point-in-time scan_start above; session 2 re-scans between
        # afternoon_scan_start and afternoon_scan_end, adding any newly-
        # qualifying stock. No scanning happens outside these two windows.
        two_session_scan_enabled: bool = True,
        afternoon_scan_start: str = "12:00",
        afternoon_scan_end: str = "13:00",
        afternoon_scan_interval_sec: float = 300.0,
        # 2026-08-25, direct user spec: five additive, independently-
        # toggleable filters (see filters.py's own module docstring for the
        # real incident this addresses -- SAIL fired a CALL breakout right
        # under a large Call-OI wall). Each *_enabled flag controls ONLY
        # whether that filter's verdict can actually BLOCK a trade -- every
        # filter always evaluates and logs to its own dedicated file
        # regardless of enabled state, so all five can be compared against
        # real outcomes before deciding which (if any) to promote to a real
        # gate. Default OFF (log-only) until proven.
        oi_wall_check_enabled: bool = False,
        oi_wall_dominance_ratio: float = 1.5,
        distance_to_wall_enabled: bool = False,
        distance_to_wall_min_pct: float = 1.5,
        pcr_gate_enabled: bool = False,
        pcr_max_for_call: float = 1.2,
        pcr_min_for_put: float = 0.8,
        volume_confirmation_enabled: bool = False,
        volume_confirmation_min_ratio: float = 1.5,
        oi_roc_enabled: bool = False,
        oi_roc_min_pct: float = 3.0,
        oi_roc_lookback_sec: float = 300.0,
        chain_watch_max_stocks: int = 2,
        # 2026-08-27, direct user spec: replaces the ORB-breach entry trigger with
        # a stock-VWAP retest. See screener.py's VwapState/check_vwap_retest_entry
        # docstrings for the full mechanic. Fresh, unvalidated defaults (this
        # strategy still can't be backtested -- same honest limitation as every
        # OI-ORB mechanic so far) -- watch real forward telemetry before trusting
        # them.
        vwap_entry_min_gap_pct: float = 0.15,
        vwap_cancel_if_unreached: bool = True,
        vwap_sl_tf_minutes: int = 5,
        # 2026-08-27, direct user spec: SL/target now track the OPTION's own
        # premium (its own VWAP, its own bars), not the underlying stock's
        # spot price -- see screener.compute_option_premium_sl_arm/_target.
        rr_multiple: float = 2.0,
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
        self._screener_cfg["NIFTY_BULLISH_PCT"] = nifty_bullish_pct
        self._screener_cfg["NIFTY_BEARISH_PCT"] = nifty_bearish_pct
        self._screener_cfg["MAX_MONITOR_MINUTES"] = max_monitor_minutes
        self._screener_cfg["REJECTION_MIN_RISE_PCT"] = rejection_min_rise_pct
        self._screener_cfg["REJECTION_RETRACE_FRACTION"] = rejection_retrace_fraction
        self._screener_cfg["STRIKE_OTM_PCT"] = strike_otm_pct
        self._screener_cfg["ORB_START"] = orb_start
        self._screener_cfg["ORB_END"] = orb_end
        self._screener_cfg["SCAN_START"] = scan_start
        self._screener_cfg["ENTRY_WINDOW_START"] = entry_window_start
        self._screener_cfg["ENTRY_WINDOW_END"] = entry_window_end
        self._screener_cfg["TWO_SESSION_SCAN_ENABLED"] = two_session_scan_enabled
        self._screener_cfg["AFTERNOON_SCAN_START"] = afternoon_scan_start
        self._screener_cfg["AFTERNOON_SCAN_END"] = afternoon_scan_end
        self._screener_cfg["AFTERNOON_SCAN_INTERVAL_SEC"] = afternoon_scan_interval_sec
        self._screener_cfg["RR_MULTIPLE"] = rr_multiple

        # ── 5 additive filters: config + one dedicated log file each ────
        self._filters_cfg = {
            "oi_wall_check_enabled": oi_wall_check_enabled,
            "oi_wall_dominance_ratio": oi_wall_dominance_ratio,
            "distance_to_wall_enabled": distance_to_wall_enabled,
            "distance_to_wall_min_pct": distance_to_wall_min_pct,
            "pcr_gate_enabled": pcr_gate_enabled,
            "pcr_max_for_call": pcr_max_for_call,
            "pcr_min_for_put": pcr_min_for_put,
            "volume_confirmation_enabled": volume_confirmation_enabled,
            "volume_confirmation_min_ratio": volume_confirmation_min_ratio,
            "oi_roc_enabled": oi_roc_enabled,
            "oi_roc_min_pct": oi_roc_min_pct,
            "oi_roc_lookback_sec": oi_roc_lookback_sec,
        }
        self._chain_watch_max_stocks = max(0, int(chain_watch_max_stocks))
        self._vwap_entry_min_gap_pct = max(0.0, float(vwap_entry_min_gap_pct))
        self._vwap_cancel_if_unreached = bool(vwap_cancel_if_unreached)
        self._vwap_sl_tf_minutes = max(1, int(vwap_sl_tf_minutes))
        self._rr_multiple = max(0.1, float(rr_multiple))
        self._flog = {
            name: _make_filter_logger(name, client_id, binding_id) for name in _FILTER_NAMES
        }

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
        # (symbol, side) pairs no longer eligible to enter today -- as of
        # 2026-08-27 populated solely by the VWAP cancel-if-unreached rule
        # (see _run_today_pipeline's entry-window-close handling). The old
        # ORB-push-then-retrace "50% rejection rule" was removed along with
        # the ORB-breach entry trigger it was designed to gate -- it doesn't
        # map onto a VWAP-retest entry (screener.check_rejection_pattern is
        # left in place, unused, in case a future ORB-based mechanic wants it).
        self._rejected: set = set()

        # ── 5-filter tracking state, keyed by stock symbol ──────────────
        self._stock_chains: Dict[str, OptionMatrix] = {}          # chain tracker (OI-wall/distance/PCR)
        self._chain_subscribed: Dict[str, list] = {}               # upstox_keys already subscribed for this stock's chain
        self._volume_cum_last: Dict[str, float] = {}                # last CUMULATIVE totalTradedVolume reading
        self._volume_recent_delta: Dict[str, float] = {}            # most recent poll-to-poll delta
        self._volume_history: Dict[str, list] = {}                  # rolling deltas, for a trailing average
        self._oi_history: Dict[str, list] = {}                      # [(unix_ts, oi_spurt_pct), ...] for OI ROC
        self._oi_history_last_poll_ts: float = 0.0                  # throttle: don't re-hit the OI-Spurt endpoint every cycle
        self._last_heartbeat_log_ts: float = 0.0                    # throttle: periodic per-stock status line
        # ── VWAP retest entry state (2026-08-27, replaces the ORB-breach
        # entry trigger) -- see screener.VwapState/check_vwap_retest_entry.
        self._vwap = screener.VwapState()
        self._vwap_armed: Dict[str, bool] = {}   # symbol -> has it moved far enough from vwap to arm a retest yet

        # ── Bear/bull-trap entry + parallel 3-min TSL (2026-08-31, direct
        # user spec, replaces the VWAP-retest entry + option-premium-VWAP SL
        # for all NEW entries -- see _trap_check_entry/_trap_update_tsl_and_
        # check_exit's own docstrings for the full mechanic, ported directly
        # from this session's own validated backtests). Positions already
        # open under the old mechanic (restored from DB) are tagged
        # sl_mechanic="vwap" and stay on the old exit path untouched --
        # see _restore_from_db.
        self._trap_1m_acc: Dict[str, "object"] = {}
        self._trap_3m_acc: Dict[str, "object"] = {}
        self._trap_zones: Dict[str, list] = {}
        self._trap_entry_calc: Dict[str, "object"] = {}
        self._trap_tsl_calc: Dict[str, "object"] = {}
        self._trap_tsl_acc: Dict[str, "object"] = {}
        self._trap_tsl_fed_bars: Dict[str, int] = {}

        # ── contract/feed/position state, keyed by stock symbol ────────
        self._pending_contracts: Dict[str, "stock_resolve.ResolvedContract"] = {}
        self._pending_fills: Dict[str, dict] = {}   # event_id -> context
        self._pending_closes: Dict[str, str] = {}   # event_id -> exit reason (OiOrbFillEvent carries no reason field)
        self._positions: Dict[str, dict] = {}        # stock symbol -> position dict
        self._live_option_ltp: Dict[str, float] = {}
        self._ltp_log_last: Dict[str, float] = {}
        self._option_key_subscribed: Dict[str, str] = {}
        self._eod_closing: Set[str] = set()

        # ── VWAP-relative structural stop-loss + fixed-RR target (2026-08-27,
        # replaces the removed S&R R1/S1/R2/S2 tracker) -- tracked on the
        # OPTION's OWN live premium (its own VWAP, its own bars), NOT the
        # underlying stock's spot price ("checking for target and SL in
        # stock, change it to the option which we are taking", direct user
        # spec). See _update_option_sl_target_and_check's own docstring for
        # the full mechanic. VWAP reference = the broker's own ATP field on
        # each OptionTick (same "VWAP = broker ATP, never computed
        # internally" convention SellStraddle's PoolIndicatorEngine already
        # uses for this exact reason -- the feed already supplies it, no
        # need to reconstruct it from a volume-delta accumulation). Bar
        # state is seeded FRESH the moment a position opens on that symbol
        # (not before) -- a same-day re-entry, or simply a different
        # strike, is a different instrument entirely.
        self._live_option_atp: Dict[str, float] = {}    # symbol -> latest broker ATP (= VWAP) for the held option
        self._option_sl_bar_key: Dict[str, str] = {}    # symbol -> current forming vwap_sl_tf_minutes bar's "HH:MM" key
        self._option_sl_bar_cur: Dict[str, dict] = {}   # symbol -> {"h","l","c","ts"} for the forming OPTION-premium bar
        self._live_sl: Dict[str, float] = {}         # symbol -> current live SL level (armed bar's low, option-premium terms)
        self._live_target: Dict[str, float] = {}     # symbol -> current live target level (option-premium terms)
        # 2026-08-28 real incident fix: chronological history of every ADVERSE
        # bar's own low since entry, per symbol -- feeds
        # screener.pool_sl_from_adverse_lows() so the SL only arms once two
        # separate bars cluster near the same floor, not on the first dip.
        self._option_adverse_lows: Dict[str, List[float]] = {}
        # Stock spot tick feed -- kept for LIVE UI VISIBILITY ONLY now (the
        # SL/target check itself no longer uses it); see _ensure_spot_feed's
        # own docstring.
        self._live_spot_ltp: Dict[str, float] = {}   # symbol -> most recent live spot tick (UI only)
        self._spot_tick_subscribed: Dict[str, bool] = {}
        # Two-session scan state (2026-08-27, direct user spec).
        self._afternoon_scan_last_ts: float = 0.0    # throttle: don't re-scan every poll cycle

        # ── OI-change rank tracking (2026-08-30, direct user spec) ──────
        self._rank_last_poll_ts: float = 0.0
        self._rank_prev_top: set = set()
        self._rank_dropped: set = set()

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
        self._rejected = set()
        self._stock_chains = {}
        self._chain_subscribed = {}
        self._volume_cum_last = {}
        self._volume_recent_delta = {}
        self._volume_history = {}
        self._oi_history = {}
        self._oi_history_last_poll_ts = 0.0
        self._vwap = screener.VwapState()
        self._vwap_armed = {}
        self._trap_1m_acc = {}
        self._trap_3m_acc = {}
        self._trap_zones = {}
        self._trap_entry_calc = {}
        self._trap_tsl_calc = {}
        self._trap_tsl_acc = {}
        self._trap_tsl_fed_bars = {}
        self._afternoon_scan_last_ts = 0.0
        # 2026-08-30, direct user spec: OI-change rank tracking (09:16-09:30
        # poll window) -- see _rank_tracking_loop's own docstring.
        self._rank_last_poll_ts = 0.0
        self._rank_prev_top: set = set()
        self._rank_dropped: set = set()
        self._clog.info("OiOrb[%s/%s]: session reset for new trading day.",
                         self._client_id, self._binding_id)

    def start(self) -> None:
        super().start()
        self._subscribe(Topic.OI_ORB_ORDER_FILL)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.EQUITY_TICK)
        # 2026-08-27: the dedicated upstox2 feeder's register_extra_spot_keys()
        # publishes stock spot ticks as INDEX_TICK (Upstox-native mechanic),
        # not EQUITY_TICK (that's the Fyers-only subscribe_fno_equity route,
        # still used as a fallback when no dedicated feeder is configured --
        # see _ensure_spot_feed). Subscribe both so _spot_tick_loop catches
        # whichever one actually fires.
        self._subscribe(Topic.INDEX_TICK)
        self._tasks.append(asyncio.create_task(
            self._daily_loop(), name=f"oiorb_daily_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._fill_loop(), name=f"oiorb_fill_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"oiorb_opttick_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._spot_tick_loop(), name=f"oiorb_spottick_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._eod_loop(), name=f"oiorb_eod_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._rank_tracking_loop(), name=f"oiorb_rank_{self._client_id}_{self._binding_id}"))
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
                # 2026-08-31: a restored position predates the trap+TSL mechanic --
                # keep it on the old option-premium-VWAP SL it was actually entered
                # under, never retroactively switch an already-open position's risk
                # model. New entries (see _on_fill's BUY branch) get "trap" instead.
                "sl_mechanic": "vwap",
            }
            self._ensure_option_feed(r["symbol"], contract)
            self._ensure_spot_feed(r["symbol"])
            self._clog.info("OiOrb[%s/%s]: RESTORED open position %s %s%d qty=%d @ %.2f from DB.",
                             self._client_id, self._binding_id, r["symbol"],
                             contract.option_type, contract.strike, r["qty"], r["entry_price"])
            # 2026-08-27, direct user spec: "when we start from middle of the day and
            # any trade is running it should get historical intraday data for that
            # option chart from time entry happened and then evaluate the SL and
            # target in tf which we have applied" -- reconstructs the REAL SL/target
            # from actual history since entry, instead of starting cold at whatever
            # moment the process happened to restart (see the method's own docstring
            # for the mechanic; best-effort, never blocks the restore on failure).
            await self._seed_option_bars_from_history(r["symbol"], contract, self._positions[r["symbol"]]["opened_at"])

        already_fired = await asyncio.to_thread(store.load_already_fired, self._client_id, self._binding_id, td)
        rejected = await asyncio.to_thread(store.load_rejected, self._client_id, self._binding_id, td)
        self._already_fired |= already_fired
        self._rejected |= rejected
        if already_fired or rejected:
            self._clog.info("OiOrb[%s/%s]: restored %d already-fired + %d rejected signal(s) from DB.",
                             self._client_id, self._binding_id, len(already_fired), len(rejected))

    async def _seed_option_bars_from_history(self, symbol: str, contract: "stock_resolve.ResolvedContract",
                                              entry_ts: datetime) -> None:
        """2026-08-27, direct user spec: on a mid-day restart with a position
        already running, fetch TODAY's real intraday 1-min candles for the
        OPTION CONTRACT ITSELF (not the underlying stock) from Upstox, replay
        them into vwap_sl_tf_minutes buckets, and run the exact same arm logic
        _update_option_sl_target_and_check uses live -- so the SL/target
        reflect the position's REAL history since entry, not a blank slate
        starting at whatever moment the process happened to restart.

        VWAP reference: real historical broker ATP isn't available (Upstox's
        historical-candle response is plain OHLCV) -- self-computed instead as
        a running cumulative(typical price x volume), the SAME "self-computed
        VWAP as a backfill proxy" pattern already used for the underlying
        stock's own VWAP (screener.backfill_vwap_from_yahoo) and every other
        ORB/SMA backfill in this file. This is deliberately a same-day-only
        fetch (fetch_upstox_intraday_1m, no date range) -- OI-ORB positions
        are MIS/EOD-only by design, so there is never a prior day's bar to
        seed from for this instrument anyway.

        Best-effort throughout: no Upstox token, no data, a network error, or
        the contract's own upstox_key being empty all just mean the position
        starts cold from this moment instead -- exactly the pre-existing
        behavior -- never blocks the restore."""
        try:
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            from data_layer.client_db import ClientDB
            if not contract.upstox_key:
                return
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                self._clog.warning(
                    "OiOrb[%s/%s]: %s option history backfill skipped -- no Upstox access "
                    "token available; SL/target will start cold from this moment.",
                    self._client_id, self._binding_id, symbol)
                return
            bars = await fetch_upstox_intraday_1m(contract.upstox_key, token)
        except Exception:
            self._clog.exception(
                "OiOrb[%s/%s]: %s option history backfill failed -- SL/target will start "
                "cold from this moment.", self._client_id, self._binding_id, symbol)
            return
        if not bars:
            self._clog.info(
                "OiOrb[%s/%s]: %s option history backfill returned no bars -- SL/target "
                "will start cold from this moment.", self._client_id, self._binding_id, symbol)
            return

        entry_time = entry_ts.time()
        buckets: Dict[str, dict] = {}
        cum_num = 0.0
        cum_den = 0.0
        for b in bars:
            ts = datetime.fromisoformat(b["ts"])
            vol = float(b.get("volume", 0) or 0)
            typical = (float(b["high"]) + float(b["low"]) + float(b["close"])) / 3.0
            if vol > 0:
                cum_num += typical * vol
                cum_den += vol
            floored = (ts.minute // self._vwap_sl_tf_minutes) * self._vwap_sl_tf_minutes
            key = f"{ts.hour:02d}:{floored:02d}"
            bkt = buckets.get(key)
            vwap_now = (cum_num / cum_den) if cum_den > 0 else None
            if bkt is None:
                buckets[key] = {"h": float(b["high"]), "l": float(b["low"]), "c": float(b["close"]),
                                 "ts": ts, "vwap_at_close": vwap_now}
            else:
                bkt["h"] = max(bkt["h"], float(b["high"]))
                bkt["l"] = min(bkt["l"], float(b["low"]))
                bkt["c"] = float(b["close"])
                bkt["vwap_at_close"] = vwap_now
        if not buckets:
            return
        if cum_den > 0:
            # Seed the live ATP reference so the FIRST live tick after restart
            # already has a real vwap to compare against, not None.
            self._live_option_atp[symbol] = cum_num / cum_den

        ordered_keys = sorted(buckets.keys())
        pos = self._positions.get(symbol)
        # 2026-08-28 fix: replay through the SAME pooled multi-touch anchor the
        # live loop uses (screener.pool_sl_from_adverse_lows), not the old
        # single-bar-low re-arm, so a restored position's SL matches exactly
        # what it would have been had the process never restarted.
        lows = self._option_adverse_lows.setdefault(symbol, [])
        for key in ordered_keys[:-1]:   # last bucket is still-forming -- live loop continues it
            bkt = buckets[key]
            if bkt["ts"].time() < entry_time or bkt["vwap_at_close"] is None or pos is None:
                continue
            if not screener.is_adverse_bar_close(bkt["c"], bkt["vwap_at_close"]):
                continue
            lows.append(bkt["l"])
            new_sl = screener.pool_sl_from_adverse_lows(lows)
            if new_sl is not None:
                self._live_sl[symbol] = new_sl
                new_target = screener.compute_option_premium_target(
                    pos["entry_price"], new_sl, self._rr_multiple)
                if new_target is not None:
                    self._live_target[symbol] = new_target

        # Seed the bar accumulator's "current" bucket to the LAST (still-forming)
        # one so the live tick loop continues it seamlessly instead of starting a
        # brand new bucket mid-way through.
        last_key = ordered_keys[-1]
        self._option_sl_bar_key[symbol] = last_key
        self._option_sl_bar_cur[symbol] = {
            "h": buckets[last_key]["h"], "l": buckets[last_key]["l"],
            "c": buckets[last_key]["c"], "ts": buckets[last_key]["ts"],
        }
        if symbol in self._live_sl:
            self._clog.info(
                "OiOrb[%s/%s]: %s option history backfill complete -- SL=%.2f target=%s "
                "(reconstructed from %d real intraday bars since entry).",
                self._client_id, self._binding_id, symbol, self._live_sl[symbol],
                f"{self._live_target[symbol]:.2f}" if symbol in self._live_target else "n/a",
                len(bars))
        else:
            self._clog.info(
                "OiOrb[%s/%s]: %s option history backfill complete -- no adverse bar found "
                "since entry yet (%d real intraday bars replayed).",
                self._client_id, self._binding_id, symbol, len(bars))

    async def _maybe_run_afternoon_scan(self, now: datetime, cfg: dict) -> None:
        """2026-08-27, direct user spec: "next session is after noon session
        which will start scan for stocks form 12.00 onwards till 1 pm and any
        stosk which comes will be added." Session 2 -- unlike session 1's
        single point-in-time scan at SCAN_START, this window re-runs
        build_shortlist() periodically (throttled to
        AFTERNOON_SCAN_INTERVAL_SEC, default 300s, so it doesn't hammer NSE
        every POLL_SECONDS cycle) and ADDS any newly-qualifying symbol to the
        existing shortlist -- never drops or replaces one already being
        watched. Regime stays whatever was frozen at ORB_END (09:25) all day
        (direct user choice) -- no fresh NIFTY regime fetch here. No-ops
        entirely outside the AFTERNOON_SCAN_START-AFTERNOON_SCAN_END window,
        or if TWO_SESSION_SCAN_ENABLED is off, or if IGNORE_TIME_WINDOWS is on
        (connectivity-test mode already bypasses all real timing)."""
        if not cfg.get("TWO_SESSION_SCAN_ENABLED", True) or cfg.get("IGNORE_TIME_WINDOWS"):
            return
        now_key = now.strftime("%H:%M")
        if not (cfg["AFTERNOON_SCAN_START"] <= now_key < cfg["AFTERNOON_SCAN_END"]):
            return
        now_ts = now.timestamp()
        interval = float(cfg.get("AFTERNOON_SCAN_INTERVAL_SEC", 300.0) or 300.0)
        if now_ts - self._afternoon_scan_last_ts < interval:
            return
        self._afternoon_scan_last_ts = now_ts

        try:
            shortlist, _nifty_pchange = await asyncio.to_thread(screener.build_shortlist, self._nse, cfg)
        except Exception as exc:
            self._clog.warning("OiOrb[%s/%s]: afternoon scan failed (non-fatal, will retry next "
                                "interval): %s", self._client_id, self._binding_id, exc)
            return
        if shortlist is None or shortlist.empty:
            return

        sl_indexed = shortlist.set_index("symbol")
        new_symbols = [s for s in shortlist["symbol"].tolist() if s not in self._shortlist_symbols]
        if not new_symbols:
            return

        new_rows = []
        for sym in new_symbols:
            row = sl_indexed.loc[sym]
            pchange = float(row["pChange"]) if "pChange" in shortlist.columns else 0.0
            self._shortlist_symbols.append(sym)
            self._shortlist_pchange[sym] = pchange
            if "previousClose" in shortlist.columns:
                self._prev_close_map[sym] = float(row["previousClose"])
            new_rows.append({
                "symbol": sym, "price_change_pct": pchange,
                "oi_spurt_pct": float(row["oi_spurt_pct"]) if "oi_spurt_pct" in shortlist.columns else None,
                "score": float(row["score"]) if "score" in shortlist.columns else None,
                "side_bias": "bullish" if pchange > 0 else "bearish",
            })
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sym,
                "afternoon_scan_added", side=screener.side_from_pchange(pchange),
                detail=f"pChange={pchange:+.2f}%")
            # Backfill this NEW symbol's own ORB/VWAP from Yahoo (real 09:15-start
            # basis) -- it was never polled during the morning session, so it has
            # no bar history of its own yet, unlike session-1 stocks.
            try:
                await asyncio.to_thread(screener.backfill_orb_from_yahoo, self._bars, [sym], cfg)
                await asyncio.to_thread(screener.backfill_vwap_from_yahoo, self._vwap, [sym], cfg)
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: afternoon-scan backfill failed for %s "
                                      "(non-fatal, VWAP will start cold from now).",
                                      self._client_id, self._binding_id, sym)
            # Chain subscription for the 5 additive filters, same shared-WS-budget
            # cap as the morning batch -- only if there's still room.
            if len(self._shortlist_symbols) <= self._chain_watch_max_stocks:
                try:
                    spot = float(row["lastPrice"]) if "lastPrice" in shortlist.columns else 0.0
                    await self._ensure_chain_subscription(sym, spot)
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: afternoon-scan chain subscription failed "
                                          "for %s.", self._client_id, self._binding_id, sym)

        await asyncio.to_thread(store.record_shortlist, self._client_id, self._binding_id, new_rows)
        self._clog.info(
            "OiOrb[%s/%s]: afternoon scan added %d new stock(s): %s",
            self._client_id, self._binding_id, len(new_symbols),
            ", ".join(f"{s}({self._shortlist_pchange[s]:+.2f}%)" for s in new_symbols),
        )

    async def _rank_tracking_loop(self) -> None:
        """2026-08-30, direct user spec: "instead of checking for stocks at
        9:26, rank 10 stocks starting 9:16 till 9:30 and also alarm if rank
        goes down -- instead of OI percent use OI change rank."

        Runs independently of _run_today_pipeline's own SCAN_START wait --
        polls screener.poll_oi_rank every RANK_POLL_INTERVAL_SEC through the
        RANK_WINDOW_START-RANK_WINDOW_END window (default 09:16-09:30,
        default poll cadence 90s), logging every poll's full ranked
        snapshot (store.record_rank_snapshot) purely so the best action time
        and threshold can be worked out after the fact -- this screener
        can't be backtested (no historical OI), so that comparison can only
        ever be done against real logged snapshots, same reasoning as
        SellStraddle's shadow-VWAP log.

        SCAN_START (default 09:26) is UNCHANGED -- it still locks the real
        shortlist exactly as before ("minimal change", direct user choice).
        This loop's only behavioral effect on trading: once a symbol is
        shortlisted (after the 09:26 lock) but has NOT yet fired an entry,
        if that symbol falls OUT of the current top-N rank on a later poll
        within this same window, it is dropped from the active shortlist
        (added to self._rejected, same set _run_today_pipeline's entry loop
        already skips) and an alert is logged -- an ALREADY-OPEN position is
        never touched by this (only pre-entry candidates can be dropped).
        """
        while self._running:
            now = datetime.now(IST)
            cfg = self._screener_cfg
            win_start = cfg.get("RANK_WINDOW_START", "09:16")
            win_end = cfg.get("RANK_WINDOW_END", "09:30")
            now_key = now.strftime("%H:%M")
            if not (win_start <= now_key < win_end) or cfg.get("IGNORE_TIME_WINDOWS"):
                await asyncio.sleep(30)
                continue
            now_ts = now.timestamp()
            interval = float(cfg.get("RANK_POLL_INTERVAL_SEC", 90.0) or 90.0)
            if now_ts - self._rank_last_poll_ts < interval:
                await asyncio.sleep(5)
                continue
            self._rank_last_poll_ts = now_ts
            try:
                if self._nse is None:
                    self._nse = await asyncio.to_thread(screener.NSESession)
                await self._do_rank_poll(now, cfg)
            except Exception:
                self._clog.warning("OiOrb[%s/%s]: rank-tracking poll failed (non-fatal, will "
                                    "retry next interval).", self._client_id, self._binding_id,
                                    exc_info=True)
            await asyncio.sleep(5)

    async def _do_rank_poll(self, now: datetime, cfg: dict) -> None:
        """One poll + drop-detection cycle, split out from _rank_tracking_loop's
        own timing/sleep wrapper so it's directly unit-testable (mirrors
        _maybe_run_afternoon_scan's own (now, cfg) shape)."""
        ranked = await asyncio.to_thread(screener.poll_oi_rank, self._nse, cfg)
        if ranked is None or ranked.empty:
            return

        poll_ts = now.isoformat(timespec="seconds")
        rows = [
            {"symbol": r["symbol"], "rank": int(r["rank"]),
             "oi_spurt_pct": float(r["oi_spurt_pct"]), "price_change_pct": float(r["pChange"])}
            for _, r in ranked.iterrows()
        ]
        await asyncio.to_thread(store.record_rank_snapshot, self._client_id, self._binding_id,
                                 poll_ts, rows)
        new_top = set(ranked["symbol"].tolist())
        self._clog.info(
            "OiOrb[%s/%s]: RANK POLL @%s top-%d by OI-spurt: %s",
            self._client_id, self._binding_id, now.strftime("%H:%M:%S"), len(new_top),
            ", ".join(f"{r['symbol']}(#{int(r['rank'])},{r['oi_spurt_pct']:.1f}%)"
                      for _, r in ranked.iterrows()),
        )

        if self._rank_prev_top:
            fell_out = self._rank_prev_top - new_top
            for sym in fell_out:
                if sym not in self._shortlist_symbols:
                    continue   # never was a real candidate -- nothing to drop
                side = screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))
                if (sym, side) in self._already_fired or sym in self._positions:
                    continue   # already entered -- never touched by rank tracking
                if (sym, side) in self._rejected:
                    continue   # already dropped on an earlier poll
                self._rejected.add((sym, side))
                self._rank_dropped.add(sym)
                if sym in self._shortlist_symbols:
                    self._shortlist_symbols.remove(sym)
                self._clog.warning(
                    "OiOrb[%s/%s]: RANK ALARM -- %s fell out of top-%d OI-spurt rank "
                    "(last seen in top-%d) -- dropped from shortlist (not yet entered).",
                    self._client_id, self._binding_id, sym, len(new_top), len(self._rank_prev_top),
                )
                await asyncio.to_thread(
                    store.log_signal_event, self._client_id, self._binding_id, sym,
                    "rank_dropped_out_of_top_n", side=side,
                    detail=f"fell out of top-{len(new_top)} OI-spurt rank")
        self._rank_prev_top = new_top

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

        # 2026-08-25: build a live option-chain tracker for each shortlisted
        # stock, regardless of whether any of the 3 chain-dependent filters
        # (oi_wall/distance_to_wall/pcr) are currently enabled as a real
        # gate -- all 5 filters must have real data to compare, per direct
        # user spec ("run parallel... own log file... compare tomorrow").
        # Best-effort per stock: a failure here must never abort the whole
        # day's pipeline, it just leaves that stock's chain-dependent
        # filters reporting "unavailable" (which never blocks on its own).
        #
        # SAFETY CAP, direct user spec 2026-08-25: this app's WS feed
        # subscription is a SINGLE SHARED budget across every strategy
        # (~50 symbols/connection, see data_layer/global_feeder.py's
        # _WS_SYMBOL_LIMIT -- exceeding it doesn't error, the broker
        # SILENTLY DROPS the excess, which could starve a completely
        # different strategy's ticks, not just this one's). Each chain is
        # ~(2*chain_depth+1)*2 symbols -- watching every shortlisted stock
        # unbounded could add 50-100+ new subscriptions on a busy day. Cap
        # to the first chain_watch_max_stocks (by shortlist rank, i.e. the
        # highest-scored candidates) until this strategy gets its own
        # dedicated feeder connection (a separate broker account/token,
        # mirroring the existing upstox2-for-CrudeOil precedent) -- not
        # built yet, needs a real credential provisioned first.
        watch_list = self._shortlist_symbols[: self._chain_watch_max_stocks]
        if len(self._shortlist_symbols) > len(watch_list):
            self._clog.warning(
                "OiOrb[%s/%s]: chain_watch_max_stocks=%d -- only watching %s for the OI-wall/"
                "distance/PCR filters, skipping %s (shared WS subscription budget, ~50/connection "
                "cap). OI-Spurt/price-move/volume/OI-ROC filters are unaffected for the skipped ones.",
                self._client_id, self._binding_id, self._chain_watch_max_stocks, watch_list,
                [s for s in self._shortlist_symbols if s not in watch_list],
            )
        for sym in watch_list:
            try:
                row = sl_indexed.loc[sym] if sym in sl_indexed.index else None
                spot = float(row["lastPrice"]) if row is not None and "lastPrice" in shortlist.columns else 0.0
                await self._ensure_chain_subscription(sym, spot)
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: chain subscription setup failed for %s "
                                      "(chain-dependent filters will report unavailable for it).",
                                      self._client_id, self._binding_id, sym)

        await asyncio.to_thread(screener.backfill_orb_from_yahoo, self._bars, self._shortlist_symbols, cfg)
        # 2026-08-27: VWAP is now the entry trigger (see below) AND the SL reference --
        # backfilled the same way ORB/SMA already are, real 09:15-start basis instead of
        # starting cold from whatever time live polling first begins.
        await asyncio.to_thread(screener.backfill_vwap_from_yahoo, self._vwap, self._shortlist_symbols, cfg)

        # 2026-08-27: MAX_MONITOR_MINUTES used to be this loop's own outer deadline, but a
        # position can now stay open (and needs live VWAP updates for its own SL) all the
        # way to EOD square-off -- far longer than a 90-min scan window. The loop now keeps
        # running (poll + VWAP maintenance) past ENTRY_WINDOW_END for as long as ANY
        # position is still open, bounded only by a hard end-of-day safety cutoff; new
        # entries still stop being evaluated the instant the entry window itself closes.
        _hard_stop_time = dtime(15, 20)

        while self._running and datetime.now(IST).time() < _hard_stop_time:
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

            await self._maybe_run_afternoon_scan(now, cfg)

            _heartbeat_parts: list = []
            for sym in self._shortlist_symbols:
                if sym not in live.index:
                    continue
                ltp = float(live.loc[sym, "lastPrice"])
                self._bars.on_quote(sym, ltp, now)

                # 2026-08-27, direct user spec: per-stock live price vs its own ORB
                # levels, logged periodically (see _maybe_log_heartbeat below) --
                # neither side is a "restriction", a stock can fire either CALL or
                # PUT depending on which level actually gets breached; this is just
                # visibility into how close each one currently is to either.
                # 2026-08-27, direct user spec ("open interest value or anything
                # which I wanted to see"): the latest polled OI-Spurt% for this
                # symbol (self._oi_history, fed by _maybe_poll_oi_history -- the
                # SAME data the OI-ROC filter uses, just surfaced here too since
                # that filter only ever logs at signal time, which may never
                # happen for a stock that never breaches).
                oi_hist = self._oi_history.get(sym)
                oi_str = f"OI={oi_hist[-1][1]:+.1f}%" if oi_hist else "OI=—"

                vwap_now = self._vwap.current(sym)
                orb_lvl = self._orb_frozen.get(sym)
                _vwap_str = f"VWAP={vwap_now:.2f} armed={self._vwap_armed.get(sym, False)}" if vwap_now else "VWAP=—"
                if orb_lvl is not None:
                    orb_high, orb_low = orb_lvl
                    _heartbeat_parts.append(
                        f"{sym}={ltp:.2f} [ORB {orb_low:.2f}-{orb_high:.2f}] {_vwap_str} {oi_str}"
                    )
                else:
                    _heartbeat_parts.append(f"{sym}={ltp:.2f} [ORB pending] {_vwap_str} {oi_str}")

                # Volume-confirmation filter: totalTradedVolume is a CUMULATIVE session
                # total (same gotcha OI-Flow's own BarAccumulator already handles for
                # option volume) -- track the poll-to-poll DELTA, not the raw number,
                # and keep a short rolling history for a trailing average. The SAME
                # delta also feeds the running session VWAP (screener.VwapState) --
                # both need exactly the same poll-to-poll volume delta.
                if "totalTradedVolume" in live.columns:
                    try:
                        cum_vol = float(live.loc[sym, "totalTradedVolume"])
                        last_cum = self._volume_cum_last.get(sym)
                        if last_cum is not None and cum_vol >= last_cum:
                            delta = cum_vol - last_cum
                            self._volume_recent_delta[sym] = delta
                            hist = self._volume_history.setdefault(sym, [])
                            hist.append(delta)
                            if len(hist) > 30:
                                del hist[:-30]
                            self._vwap.update(sym, ltp, delta)
                        self._volume_cum_last[sym] = cum_vol
                    except Exception:
                        pass

            self._maybe_log_heartbeat(_heartbeat_parts)
            await self._maybe_poll_oi_history(now)

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

            # Trap-mechanic TSL: runs for every currently-open "trap"-tagged
            # position regardless of the entry window/regime state above --
            # an open position's own exit tracking must never pause just
            # because new entries aren't being evaluated right now.
            for sym, pos in list(self._positions.items()):
                if pos.get("sl_mechanic") != "trap" or sym not in live.index:
                    continue
                side = screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))
                ltp = float(live.loc[sym, "lastPrice"])
                await self._trap_update_tsl_and_check_exit(sym, side, ltp, now)

            entry_window_open = cfg.get("IGNORE_TIME_WINDOWS") or (
                cfg["ENTRY_WINDOW_START"] <= now_key < cfg["ENTRY_WINDOW_END"])
            if self._regime is not None and entry_window_open:
                regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
                for sym in self._shortlist_symbols:
                    if sym not in live.index or sym in self._positions or sym in self._pending_contracts:
                        continue
                    side = screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))
                    if (sym, side) in self._already_fired or (sym, side) in self._rejected:
                        continue
                    if not screener.side_allowed_by_regime(side, self._regime, regime_filter_on):
                        continue
                    ltp = float(live.loc[sym, "lastPrice"])
                    fire = self._trap_check_entry(sym, side, ltp, now)
                    if not fire:
                        continue
                    self._already_fired.add((sym, side))
                    orb_lvl = self._orb_frozen.get(sym, (0.0, 0.0))
                    sig = screener.Signal(symbol=sym, side=side, reason="trap_retest",
                                          trigger_price=ltp, orb_high=orb_lvl[0], orb_low=orb_lvl[1],
                                          ts=now.strftime("%H:%M:%S"))
                    self._clog.info(
                        "OiOrb[%s/%s]: SIGNAL %s BUY %s TRAP-RETEST ltp=%.2f reason=%s",
                        self._client_id, self._binding_id, sig.symbol, sig.side,
                        sig.trigger_price, sig.reason)
                    await asyncio.to_thread(
                        store.log_signal_event, self._client_id, self._binding_id, sig.symbol,
                        "signal_fired", side=sig.side, detail=sig.reason,
                        trigger_price=sig.trigger_price, orb_high=sig.orb_high, orb_low=sig.orb_low)
                    if self._evaluate_additive_filters(sig):
                        asyncio.create_task(self._handle_signal(sig))
                    else:
                        self._clog.info(
                            "OiOrb[%s/%s]: %s %s signal BLOCKED by an enabled additive filter -- "
                            "see the individual oiorb_filter_* logs for which one and why.",
                            self._client_id, self._binding_id, sig.symbol, sig.side)
            elif (not cfg.get("IGNORE_TIME_WINDOWS") and now_key >= cfg["ENTRY_WINDOW_END"]
                  and not self._entry_window_done_logged):
                self._entry_window_done_logged = True
                # 2026-08-27, direct user spec, default ON: any shortlisted candidate that
                # never armed+retested VWAP by entry-window-end is cancelled for the day --
                # logged to the audit trail; the loop itself already naturally stops
                # evaluating new entries here regardless (entry_window_open goes False), this
                # just makes the "why nothing happened for this stock" reason explicit.
                if self._vwap_cancel_if_unreached:
                    regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
                    for sym in self._shortlist_symbols:
                        side = screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))
                        if ((sym, side) in self._already_fired or (sym, side) in self._rejected
                                or not screener.side_allowed_by_regime(side, self._regime, regime_filter_on)):
                            continue
                        self._rejected.add((sym, side))
                        self._clog.info(
                            "OiOrb[%s/%s]: %s %s CANCELLED -- no trap zone retested by entry-window-end (%s).",
                            self._client_id, self._binding_id, sym, side, cfg["ENTRY_WINDOW_END"])
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sym,
                            "vwap_entry_window_expired", side=side)
                self._clog.info("OiOrb[%s/%s]: entry window closed for today (%s). New entries stop; "
                                 "still polling for VWAP/SL maintenance on any open position until EOD.",
                                 self._client_id, self._binding_id, cfg["ENTRY_WINDOW_END"])

            if (self._entry_window_done_logged and not self._positions
                    and not cfg.get("IGNORE_TIME_WINDOWS")):
                # Entry window closed AND every position is already flat -- nothing left
                # for this loop to usefully do today.
                break

            await asyncio.sleep(cfg["POLL_SECONDS"])

    async def _wait_until_actionable(self, cfg) -> bool:
        """Waits for SCAN_START (session 1's single point-in-time scan,
        default 09:26 -- direct user spec) before running today's pipeline at
        all. Since the whole pipeline never starts before the 09:15-09:25 ORB
        window has already closed, live polling can never build real
        09:15-09:25 bars itself -- the Yahoo backfill is the ONLY source of
        them, every day (see screener.py's ORB_END docstring). Session 2 (the
        12:00-13:00 afternoon re-scan) is handled separately, inside the main
        loop -- see _maybe_run_afternoon_scan."""
        if cfg.get("IGNORE_TIME_WINDOWS"):
            return True
        scan_start = cfg.get("SCAN_START", "09:26")
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

    def _evaluate_additive_filters(self, sig: "screener.Signal") -> bool:
        """Evaluates all 5 additive filters (filters.py) for this fired
        signal and logs each verdict to its OWN dedicated log file,
        regardless of that filter's own enabled state, per direct user
        spec ("run parallel... own log file... compare tomorrow"). Only an
        ENABLED filter's block verdict actually stops the trade -- returns
        True iff no enabled filter objects. entry_strike is an early
        approximation (the raw OTM-offset target, matching _handle_signal's
        own formula) since the real rounded contract strike isn't resolved
        until after this point."""
        fcfg = self._filters_cfg
        opt_type = "CE" if sig.side == "CALL" else "PE"
        otm_frac = self._screener_cfg.get("STRIKE_OTM_PCT", 2.0) / 100.0
        entry_strike = sig.trigger_price * (1 + otm_frac if opt_type == "CE" else 1 - otm_frac)

        snap = None
        mat = self._stock_chains.get(sig.symbol)
        if mat is not None:
            try:
                snap = mat.snapshot()
            except Exception:
                snap = None

        verdicts = [
            oi_filters.evaluate_oi_wall(snap, sig.side, entry_strike,
                                         dominance_ratio=fcfg["oi_wall_dominance_ratio"]),
            oi_filters.evaluate_distance_to_wall(snap, sig.side, entry_strike,
                                                  min_distance_pct=fcfg["distance_to_wall_min_pct"]),
            oi_filters.evaluate_pcr(snap.pcr if snap is not None else None, sig.side,
                                     max_pcr_for_call=fcfg["pcr_max_for_call"],
                                     min_pcr_for_put=fcfg["pcr_min_for_put"]),
            oi_filters.evaluate_volume_confirmation(
                self._volume_recent_delta.get(sig.symbol),
                (sum(self._volume_history[sig.symbol][:-1]) / len(self._volume_history[sig.symbol][:-1])
                 if len(self._volume_history.get(sig.symbol, [])) > 1 else None),
                min_ratio=fcfg["volume_confirmation_min_ratio"]),
            oi_filters.evaluate_oi_roc(self._oi_history.get(sig.symbol, []),
                                        min_roc_pct=fcfg["oi_roc_min_pct"],
                                        lookback_sec=fcfg["oi_roc_lookback_sec"],
                                        now_ts=datetime.now(IST).timestamp()),
        ]

        # FilterVerdict.name ("oi_wall", "pcr", ...) does not always match its own
        # enable-flag key verbatim (oi_wall_check_enabled, pcr_gate_enabled) -- an
        # explicit map is safer than string concatenation, which silently produced
        # a nonexistent config key and let a genuinely-blocking verdict never block.
        _enable_key = {
            "oi_wall": "oi_wall_check_enabled",
            "distance_to_wall": "distance_to_wall_enabled",
            "pcr": "pcr_gate_enabled",
            "volume_confirmation": "volume_confirmation_enabled",
            "oi_roc": "oi_roc_enabled",
        }
        ok = True
        for v in verdicts:
            enabled = bool(fcfg.get(_enable_key[v.name], False))
            blocked = v.blocks(enabled)
            ok = ok and not blocked
            self._flog[v.name].info(
                "%s %s %s trigger=%.2f entry_strike~%.2f | available=%s passed=%s enabled=%s "
                "-> %s | %s | %s",
                sig.symbol, sig.side, "BLOCK" if blocked else ("PASS" if v.passed else "logged-only"),
                sig.trigger_price, entry_strike, v.available, v.passed, enabled,
                "WOULD BLOCK" if blocked else "no block", v.reason, v.numbers,
            )
        return ok

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
            self._already_fired.discard((sig.symbol, sig.side))
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
            self._already_fired.discard((sig.symbol, sig.side))
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
            self._already_fired.discard((sig.symbol, sig.side))
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
        gf = getattr(self._bus, "_oiorb_feeder", None) or getattr(self._bus, "_global_feeder", None)
        if gf is None or not hasattr(gf, "subscribe_tokens"):
            self._clog.warning("OiOrb[%s/%s]: no live GlobalFeeder available -- cannot subscribe %s.",
                                self._client_id, self._binding_id, stock_symbol)
            return
        asyncio.create_task(gf.subscribe_tokens([key]))
        self._option_key_subscribed[stock_symbol] = key
        self._clog.info("OiOrb[%s/%s]: subscribed live option feed for %s %s%d (%s).",
                         self._client_id, self._binding_id, stock_symbol,
                         contract.option_type, contract.strike, key)

    def _ensure_spot_feed(self, stock_symbol: str) -> None:
        """2026-08-26, direct user spec: subscribe a genuine live tick feed for
        this stock's OWN spot price, but ONLY once a position actually opens on
        it (never for the whole shortlist all day) -- keeps the shared WS feed
        budget impact minimal (same budget-conscious philosophy already used for
        chain_watch_max_stocks).

        2026-08-27: SL/target now track the OPTION's own premium, not the
        stock's spot price (see _update_option_sl_target_and_check) -- this
        feed is kept purely so monitoring_state()'s `spot_ltp` field still
        shows a live reference price in the UI, not because anything trading-
        relevant depends on it anymore. Idempotent per stock symbol.

        2026-08-27: prefers the dedicated upstox2 feeder (`bus._oiorb_feeder`,
        see run_system.py) via Upstox-native `register_extra_spot_keys` --
        needs this stock's own NSE_EQ instrument key, resolved via
        stock_resolve.resolve_eq_instrument_key() (same master-JSON cache
        used for lot-size lookups). Falls back to the shared feeder's
        Fyers-only `subscribe_fno_equity` route (same one
        strategies/fno_positional/book.py uses) only when no dedicated
        upstox2 feeder is configured."""
        if self._spot_tick_subscribed.get(stock_symbol):
            return
        oiorb_gf = getattr(self._bus, "_oiorb_feeder", None)
        if oiorb_gf is not None and hasattr(oiorb_gf, "register_extra_spot_keys"):
            eq_key = stock_resolve.resolve_eq_instrument_key(stock_symbol)
            if eq_key:
                try:
                    oiorb_gf.register_extra_spot_keys({eq_key: stock_symbol})
                    self._spot_tick_subscribed[stock_symbol] = True
                    self._clog.info("OiOrb[%s/%s]: subscribed live spot feed for %s via dedicated "
                                     "upstox2 feeder (%s) (live UI reference price only).",
                                     self._client_id, self._binding_id, stock_symbol, eq_key)
                    return
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: dedicated upstox2 spot feed subscribe "
                                          "failed for %s -- falling back to shared feeder.",
                                          self._client_id, self._binding_id, stock_symbol)
            else:
                self._clog.warning("OiOrb[%s/%s]: no NSE_EQ instrument key resolved for %s -- "
                                    "cannot use dedicated upstox2 spot feed, falling back.",
                                    self._client_id, self._binding_id, stock_symbol)

        gf = getattr(self._bus, "_global_feeder", None)
        if gf is None or not hasattr(gf, "subscribe_fno_equity"):
            self._clog.warning("OiOrb[%s/%s]: no live GlobalFeeder available -- cannot subscribe "
                                "live spot feed for %s (UI reference price only).",
                                self._client_id, self._binding_id, stock_symbol)
            return
        try:
            gf.subscribe_fno_equity(f"NSE:{stock_symbol}-EQ", stock_symbol)
            self._spot_tick_subscribed[stock_symbol] = True
            self._clog.info("OiOrb[%s/%s]: subscribed live spot feed for %s (live UI reference price only).",
                             self._client_id, self._binding_id, stock_symbol)
        except Exception:
            self._clog.exception("OiOrb[%s/%s]: live spot feed subscribe failed for %s.",
                                  self._client_id, self._binding_id, stock_symbol)

    async def _ensure_chain_subscription(self, stock_symbol: str, spot: float) -> None:
        """Widen the live feed subscription from 'just the one traded
        contract' (the pre-existing behaviour) to a small ATM ± depth range
        of BOTH CE and PE for this shortlisted stock, and build a chain
        tracker for it -- feeds the 3 chain-dependent filters (oi_wall/
        distance_to_wall/pcr). Idempotent per stock symbol. Best-effort:
        any failure leaves that stock's chain-dependent filters simply
        reporting "unavailable" (never a block on its own -- see
        filters.py's FilterVerdict.blocks())."""
        if spot <= 0 or stock_symbol in self._stock_chains:
            return
        if not REGISTRY.is_loaded(stock_symbol):
            REGISTRY.load_sync(stock_symbol)
        expiry = REGISTRY.get_active_expiry(stock_symbol)
        if expiry is None:
            self._clog.warning("OiOrb[%s/%s]: no active expiry for %s -- chain tracking unavailable.",
                                self._client_id, self._binding_id, stock_symbol)
            return
        depth = int(getattr(self._cfg, "chain_depth", 4) or 4)
        mat = _build_stock_chain(stock_symbol, spot, expiry, depth)
        if mat is None:
            return
        self._stock_chains[stock_symbol] = mat

        gf = getattr(self._bus, "_oiorb_feeder", None) or getattr(self._bus, "_global_feeder", None)
        if gf is None or not hasattr(gf, "subscribe_tokens"):
            self._clog.warning("OiOrb[%s/%s]: no live GlobalFeeder available -- chain for %s built "
                                "but cannot subscribe strikes.", self._client_id, self._binding_id, stock_symbol)
            return
        keys = []
        for strike in mat.snapshot().strikes():
            for opt_type in ("CE", "PE"):
                try:
                    k = REGISTRY.get_upstox_key(stock_symbol, expiry, strike, opt_type)
                except Exception:
                    k = None
                if k:
                    keys.append(k)
        if keys:
            asyncio.create_task(gf.subscribe_tokens(keys))
        self._chain_subscribed[stock_symbol] = keys
        self._clog.info("OiOrb[%s/%s]: chain tracking started for %s (ATM=%.2f depth=%d, %d contracts).",
                         self._client_id, self._binding_id, stock_symbol,
                         mat.snapshot().atm_strike, depth, len(keys))

    def _maybe_log_heartbeat(self, heartbeat_parts: list) -> None:
        """2026-08-27, direct user spec: real observation was "only one log is
        showing data, rest are blank" -- before this, nothing logged between
        "ORB frozen" and an actual fired/rejected signal, so a small shortlist
        with neither stock breaching yet looked identical to "book stalled" as
        "book alive but nothing's happened." Logs each shortlisted stock's live
        price against its own ORB-high/ORB-low once per _HEARTBEAT_INTERVAL_SEC,
        purely informational -- neither side is a restriction (a stock can fire
        either CALL or PUT depending on which level actually breaches)."""
        if not heartbeat_parts:
            return
        _now_mono = _time.monotonic()
        if _now_mono - self._last_heartbeat_log_ts < _HEARTBEAT_INTERVAL_SEC:
            return
        self._last_heartbeat_log_ts = _now_mono
        self._clog.info("OiOrb[%s/%s]: WATCH  %s",
                         self._client_id, self._binding_id, " | ".join(heartbeat_parts))

    async def _maybe_poll_oi_history(self, now: datetime) -> None:
        """Feeds the OI rate-of-change filter -- ADDITIVE to the existing
        static daily OI-Spurt% shortlist filter (screener.CONFIG's
        OI_SPURT_MIN_PCT, completely unchanged), never a replacement.
        Throttled deliberately: fetch_oi_spurts_nse() re-fetches ALL ~214
        F&O symbols from NSE every call, so this must NOT run every
        POLL_SECONDS cycle (confirmed live 2026-08-24: an aggressive NSE
        polling pattern makes an Akamai throttle worse, see screener.py's
        NSESession docstring) -- polls at most once every max(60s,
        lookback/3) so a lookback window gets a few real samples without
        hammering the endpoint."""
        lookback = float(self._filters_cfg.get("oi_roc_lookback_sec", 300.0) or 300.0)
        interval = max(60.0, lookback / 3.0)
        now_ts = now.timestamp()
        if now_ts - self._oi_history_last_poll_ts < interval:
            return
        self._oi_history_last_poll_ts = now_ts
        try:
            oi_spurts = await asyncio.to_thread(screener.fetch_oi_spurts_nse, self._nse)
        except Exception as exc:
            self._clog.debug("OiOrb[%s/%s]: OI-ROC history poll failed (non-fatal): %s",
                              self._client_id, self._binding_id, exc)
            return
        by_symbol = oi_spurts.set_index("symbol")["oi_spurt_pct"].to_dict() if "symbol" in oi_spurts.columns else {}
        for sym in self._shortlist_symbols:
            if sym not in by_symbol:
                continue
            hist = self._oi_history.setdefault(sym, [])
            hist.append((now_ts, float(by_symbol[sym])))
            cutoff = now_ts - lookback * 3   # keep a bit more than one lookback window
            self._oi_history[sym] = [(t, v) for t, v in hist if t >= cutoff]

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

            # Feed the chain tracker (OI-wall/distance/PCR filters) whenever this tick
            # matches a strike we widened the subscription for -- independent of whether
            # there's a pending/open TRADED contract for this stock at all.
            mat = self._stock_chains.get(tick.underlying)
            if mat is not None:
                try:
                    if mat.on_option_tick(tick):
                        mat.recompute()
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: chain tick update failed for %s",
                                          self._client_id, self._binding_id, tick.underlying)

            contract = self._pending_contracts.get(tick.underlying) or \
                (self._positions.get(tick.underlying) or {}).get("contract")
            if contract is None:
                continue
            if (tick.strike == contract.strike and tick.option_type == contract.option_type
                    and tick.expiry == contract.expiry):
                self._live_option_ltp[tick.underlying] = tick.ltp
                # VWAP = broker ATP, never computed internally -- same convention
                # SellStraddle's PoolIndicatorEngine already uses. Guard against a
                # stale/zero ATP overwriting a genuinely good prior reading.
                if tick.atp and tick.atp > 0:
                    self._live_option_atp[tick.underlying] = float(tick.atp)
                # 2026-08-26 fix (user request): this used to log every single tick --
                # dozens per minute per open position, flooding the per-underlying log
                # with near-duplicate lines. Throttled to once/60s per symbol, matching
                # the OPT_TICKS/IDX_TICKS "N ticks/60s" summary style already used
                # elsewhere in this codebase (e.g. sell_straddle's _option_loop).
                _now_mono = _time.monotonic()
                _last = self._ltp_log_last.get(tick.underlying, 0.0)
                if _now_mono - _last >= 60.0:
                    self._ltp_log_last[tick.underlying] = _now_mono
                    self._clog.debug("OiOrb[%s/%s]: LTP %s %s%d = %.2f",
                                      self._client_id, self._binding_id, tick.underlying,
                                      contract.option_type, contract.strike, tick.ltp)
                await self._check_hard_risk_cap(tick.underlying, tick.ltp)
                await self._update_option_sl_target_and_check(tick.underlying, tick.ltp, tick.timestamp)

    async def _check_hard_risk_cap(self, symbol: str, option_ltp: float) -> None:
        """2026-08-26, added alongside the structural SL -- a fresh entry (or a
        restored one after a restart) needs a few live option bars before its
        first real SL/target confirms, running with NO structural protection
        at all until then. This is the SAME universal hard ₹/lot risk cap
        every other option-buyer strategy in this codebase already carries as
        a backstop (_MAX_RISK_RS_PER_LOT=2000, reused directly from
        support_resistance.py, not reimplemented) -- an independent safety
        net that fires regardless of whether the structural SL has armed yet."""
        pos = self._positions.get(symbol)
        if pos is None or symbol in self._eod_closing or option_ltp <= 0:
            return
        loss_rs = (pos["entry_price"] - option_ltp) * pos["qty"]
        cap_rs = _SR_MAX_RISK_RS_PER_LOT * self._lot_multiplier
        if loss_rs >= cap_rs:
            self._eod_closing.add(symbol)
            self._clog.info(
                "OiOrb[%s/%s]: %s HARD RISK CAP HIT -- entry=%.2f current=%.2f qty=%d "
                "loss=Rs%.2f >= cap Rs%.2f -- closing.",
                self._client_id, self._binding_id, symbol, pos["entry_price"], option_ltp,
                pos["qty"], loss_rs, cap_rs,
            )
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, symbol,
                "hard_risk_cap_triggered",
                detail=f"entry={pos['entry_price']:.2f} current={option_ltp:.2f} "
                       f"loss=Rs{loss_rs:.2f} cap=Rs{cap_rs:.2f}")
            await self._emit_close(symbol, pos, "hard_risk_cap")

    async def _spot_tick_loop(self) -> None:
        """2026-08-26, direct user spec: live spot ticks for a stock, ONLY once
        a position is open on it (see _ensure_spot_feed).

        2026-08-27: SL/target moved to the OPTION's own premium (see
        _update_option_sl_target_and_check, driven by _option_tick_loop
        instead) -- this loop now exists purely to keep monitoring_state()'s
        `spot_ltp` field showing a live reference price in the UI."""
        eq_q = self._loop_queues.get(Topic.EQUITY_TICK)
        idx_q = self._loop_queues.get(Topic.INDEX_TICK)
        queues = [q for q in (eq_q, idx_q) if q is not None]
        if not queues:
            return
        pending_gets = {asyncio.create_task(q.get()): q for q in queues}
        while self._running:
            try:
                done, _ = await asyncio.wait(pending_gets.keys(), timeout=1.0,
                                              return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                for t in pending_gets:
                    t.cancel()
                break
            if not done:
                continue
            task = next(iter(done))
            q = pending_gets.pop(task)
            pending_gets[asyncio.create_task(q.get())] = q
            try:
                ev = task.result()
            except asyncio.CancelledError:
                continue
            try:
                if not isinstance(ev, IndexTick):
                    continue
                symbol = ev.symbol
                if symbol not in self._positions:
                    continue
                ltp = float(ev.ltp or 0.0)
                if ltp <= 0:
                    continue
                self._live_spot_ltp[symbol] = ltp
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: spot tick processing error (recovered).",
                                      self._client_id, self._binding_id)

    def _trap_check_entry(self, sym: str, side: str, ltp: float, ts: datetime) -> bool:
        """Bear-trap (side="CALL")/bull-trap (side="PUT") zone detection ->
        retest -> 1-min R2->R1/S2->S1 ladder entry (2026-08-31, direct user
        spec, replaces the VWAP-retest entry mechanic for all NEW entries).
        Ported directly from this session's own validated backtests
        (scripts/oi_orb_bear_trap_coforge_*.py / oi_orb_bull_trap_
        tatapower_backtest.py) -- reuses the SAME real, already-validated
        zone functions (screener.sharp_bear_zones/bull_trap_zones, which
        themselves reuse strategies.liquidity_trap.detector.find_all_setups)
        rather than reimplementing detection, and the same
        strategies.d1_trap_option.bear_only_book._collapse_nearby_zones
        merge every other trap mechanic in this codebase already uses.

        Bars are built from the underlying's OWN polled price (the same
        `ltp` the screener's poll loop already reads every POLL_SECONDS),
        via strategies.liquidity_trap.detector.BarAccumulator -- a genuine
        NSE poll cadence (~20s), not tick-level, same data source the
        VWAP-retest mechanic it replaces already used.

        Returns True the instant a genuine entry fires (caller emits the
        Signal + tags the fresh position "sl_mechanic": "trap")."""
        from strategies.liquidity_trap.detector import BarAccumulator as _TrapAcc
        from strategies.d1_trap_option.bear_only_book import _collapse_nearby_zones
        from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator

        acc1 = self._trap_1m_acc.setdefault(sym, _TrapAcc(timeframe_min=1))
        acc3 = self._trap_3m_acc.setdefault(sym, _TrapAcc(timeframe_min=3))
        closed1 = acc1.on_tick(ts, ltp)
        closed3 = acc3.on_tick(ts, ltp)

        if sym not in self._trap_entry_calc:
            if closed3 and len(acc3.bars) >= 3:
                zones_fn = screener.sharp_bear_zones if side == "CALL" else screener.bull_trap_zones
                try:
                    zones = zones_fn(acc3.bars)
                    zones = _collapse_nearby_zones(zones)
                except Exception:
                    self._clog.exception("OiOrb[%s/%s]: %s trap zone detection error (recovered).",
                                          self._client_id, self._binding_id, sym)
                    zones = self._trap_zones.get(sym, [])
                self._trap_zones[sym] = zones
            zones = self._trap_zones.get(sym, [])
            retested = None
            for z in zones:
                if side == "CALL" and ltp >= z["zone_lo"]:
                    retested = z
                    break
                if side == "PUT" and ltp <= z["zone_hi"]:
                    retested = z
                    break
            if retested is None:
                return False
            self._trap_entry_calc[sym] = SupportResistanceCalculator()
            self._trap_tsl_calc[sym] = SupportResistanceCalculator()
            self._trap_tsl_acc[sym] = _TrapAcc(timeframe_min=3)
            self._trap_tsl_fed_bars[sym] = 0
            self._clog.info(
                "OiOrb[%s/%s]: %s TRAP RETEST (%s) zone=[%.2f,%.2f] @ ltp=%.2f -- 1-min entry "
                "ladder starting fresh from here.",
                self._client_id, self._binding_id, sym, side, retested["zone_lo"], retested["zone_hi"], ltp,
            )

        calc = self._trap_entry_calc[sym]
        if not closed1 or not acc1.bars:
            return False
        b = acc1.bars[-1]
        state_before = calc.get_calculated_sr_state(sym)
        phase_before = state_before.get("current_phase")
        calc.process_straddle_candle(sym, {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": 1})
        phase_after = calc.get_calculated_sr_state(sym).get("current_phase")
        target_phase = "R1_TRACKING" if side == "CALL" else "S1_TRACKING"
        if phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == target_phase:
            self._clog.info(
                "OiOrb[%s/%s]: %s TRAP ENTRY CONFIRMED (%s) 1m bar %s H=%.2f L=%.2f -- %s->%s",
                self._client_id, self._binding_id, sym, side, b.ts.strftime("%H:%M"),
                b.high, b.low, phase_before, phase_after,
            )
            return True
        return False

    async def _trap_update_tsl_and_check_exit(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """Parallel 3-min S1(long)/R1(short) trailing stop for a trap-
        mechanic position (2026-08-31, direct user spec) -- tracks the
        underlying STOCK's own price structure (same source the entry zones
        were built from), matching the validated backtest's own risk model
        exactly (SL/TSL on the stock chart; the option is simply what's
        bought). Ratchets one direction only, never loosens.

        Cold-start safety (real bug found and fixed during this session's
        own backtesting -- scripts/oi_orb_bull_trap_tatapower_backtest.py's
        own history): NO exit check at all until the parallel 3-min ladder
        has produced a genuine R1/S1 value -- a naive fallback to
        entry_price would create a zero-risk stop that fires on the very
        next tick."""
        from strategies.liquidity_trap.detector import BarAccumulator as _TrapAcc

        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        acc = self._trap_tsl_acc.setdefault(sym, _TrapAcc(timeframe_min=3))
        acc.on_tick(ts, ltp)
        calc = self._trap_tsl_calc.get(sym)
        if calc is None:
            return   # no ladder ever started for this position (shouldn't happen -- defensive)

        fed = self._trap_tsl_fed_bars.get(sym, 0)
        for b in acc.bars[fed:]:
            calc.process_straddle_candle(sym, {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": 3})
        self._trap_tsl_fed_bars[sym] = len(acc.bars)

        sr = calc.get_calculated_sr_state(sym).get("sr_levels", {})
        level = sr.get("S1") if side == "CALL" else sr.get("R1")
        if level is None:
            return   # cold-start -- no exit check until a real ladder value exists
        breach = (ltp <= level["low"]) if side == "CALL" else (ltp >= level["high"])
        if not breach:
            return
        self._eod_closing.add(sym)
        self._clog.info(
            "OiOrb[%s/%s]: %s TRAP TSL HIT -- underlying_ltp=%.2f level=%.2f side=%s -- closing.",
            self._client_id, self._binding_id, sym, ltp,
            level["low"] if side == "CALL" else level["high"], side,
        )
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            "trap_tsl_triggered",
            detail=f"underlying_ltp={ltp:.2f} level={level}")
        await self._emit_close(sym, pos, "trap_tsl")

    async def _update_option_sl_target_and_check(self, symbol: str, ltp: float, ts: datetime) -> None:
        """2026-08-27, direct user spec, replaces the S&R (R1/S1/R2/S2) SL and
        adds a fixed-RR target -- BOTH now track the OPTION's OWN premium
        ("checking for target and SL in stock, change it to the option which
        we are taking"), not the underlying stock's spot price.

        OI-ORB only ever BUYS options -- a bought CE and a bought PE both
        want their OWN premium to rise, so this mechanic is side-INDEPENDENT
        (unlike the old stock-spot version, where CALL/PUT genuinely pointed
        opposite directions on the underlying spot).

        Builds self._vwap_sl_tf_minutes-min OHLC bars from live OPTION ticks
        (own accumulator, driven by _option_tick_loop). On each bar CLOSE,
        screener.compute_option_premium_sl_arm() checks whether that bar
        closed BELOW the option's own live VWAP (broker ATP,
        self._live_option_atp) -- adverse -- and if so RE-ARMS self._live_sl
        to THAT bar's own LOW, replacing whatever was armed before (re-arms
        on every adverse bar, not just the first). The target
        (self._live_target) is recomputed alongside every re-arm: entry +
        rr_multiple * (entry - armed_sl). A bar closing on the favorable
        side leaves both levels untouched.

        The CURRENT live tick (not just the last closed bar) is checked
        against BOTH levels on EVERY tick via screener.
        check_option_premium_exit() -- a real-time breach doesn't wait for
        the next candle to close.

        2026-08-31: skipped entirely for a "trap"-tagged position -- that
        mechanic's own exit (_trap_update_tsl_and_check_exit) runs off the
        underlying's own poll price, not option ticks, and is called from
        the poll loop instead. Only a restored ("vwap"-tagged) position from
        before this change still reaches this method."""
        pos = self._positions.get(symbol)
        if pos is not None and pos.get("sl_mechanic") == "trap":
            return
        floored_minute = (ts.minute // self._vwap_sl_tf_minutes) * self._vwap_sl_tf_minutes
        key = f"{ts.hour:02d}:{floored_minute:02d}"
        cur_key = self._option_sl_bar_key.get(symbol)
        if cur_key is None:
            self._option_sl_bar_key[symbol] = key
            self._option_sl_bar_cur[symbol] = {"h": ltp, "l": ltp, "c": ltp, "ts": ts}
        elif key != cur_key:
            closed = self._option_sl_bar_cur[symbol]
            vwap_at_close = self._live_option_atp.get(symbol)
            if pos is not None:
                # 2026-08-27, direct user request (same visibility gap flagged for
                # SellStraddle's ITM-roll-protection): log EVERY bar close, not just
                # an actual re-arm -- otherwise "no RE-ARMED line yet" is impossible
                # to tell apart from "genuinely favorable/flat so far" vs "ATP never
                # arriving for this contract at all, silently stuck forever".
                if vwap_at_close is None:
                    self._clog.info(
                        "OiOrb[%s/%s]: %s OPTION BAR CLOSED -- close=%.2f (bar %s) -- "
                        "no broker ATP received yet for this contract, SL/target cannot "
                        "arm until one arrives.",
                        self._client_id, self._binding_id, symbol, closed["c"], cur_key)
                elif screener.is_adverse_bar_close(closed["c"], vwap_at_close):
                    lows = self._option_adverse_lows.setdefault(symbol, [])
                    lows.append(closed["l"])
                    new_sl = screener.pool_sl_from_adverse_lows(lows)
                    if new_sl is not None and new_sl != self._live_sl.get(symbol):
                        self._live_sl[symbol] = new_sl
                        new_target = screener.compute_option_premium_target(
                            pos["entry_price"], new_sl, self._rr_multiple)
                        if new_target is not None:
                            self._live_target[symbol] = new_target
                        self._clog.info(
                            "OiOrb[%s/%s]: %s OPTION-SL RE-ARMED -- bar_close=%.2f vwap(atp)=%.2f "
                            "new_sl=%.2f target=%s (bar %s)",
                            self._client_id, self._binding_id, symbol, closed["c"],
                            vwap_at_close, new_sl,
                            f"{new_target:.2f}" if new_target is not None else "n/a", cur_key)
                    else:
                        self._clog.info(
                            "OiOrb[%s/%s]: %s OPTION BAR CLOSED ADVERSE -- close=%.2f < vwap(atp)=%.2f, "
                            "low=%.2f logged as candidate -- SL %s (bar %s)",
                            self._client_id, self._binding_id, symbol, closed["c"],
                            vwap_at_close, closed["l"],
                            "unchanged (still awaiting a 2nd nearby low)" if new_sl is None
                            else "unchanged (no closer cluster found)", cur_key)
                else:
                    self._clog.info(
                        "OiOrb[%s/%s]: %s OPTION BAR CLOSED -- close=%.2f >= vwap(atp)=%.2f "
                        "(favorable) -- no re-arm (bar %s)",
                        self._client_id, self._binding_id, symbol, closed["c"],
                        vwap_at_close, cur_key)
            self._option_sl_bar_key[symbol] = key
            self._option_sl_bar_cur[symbol] = {"h": ltp, "l": ltp, "c": ltp, "ts": ts}
        else:
            b = self._option_sl_bar_cur[symbol]
            b["h"] = max(b["h"], ltp)
            b["l"] = min(b["l"], ltp)
            b["c"] = ltp

        if pos is None or symbol in self._eod_closing:
            return
        sl = self._live_sl.get(symbol)
        target = self._live_target.get(symbol)
        hit = screener.check_option_premium_exit(sl, target, ltp)
        if hit is not None:
            self._eod_closing.add(symbol)
            self._clog.info(
                "OiOrb[%s/%s]: %s OPTION %s HIT -- option_ltp=%.2f sl=%s target=%s -- closing.",
                self._client_id, self._binding_id, symbol, hit.upper(), ltp,
                f"{sl:.2f}" if sl is not None else "n/a",
                f"{target:.2f}" if target is not None else "n/a",
            )
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, symbol,
                f"option_{hit}_triggered",
                detail=f"option_ltp={ltp:.2f} sl={sl} target={target}")
            await self._emit_close(symbol, pos, f"option_{hit}")

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
                # 2026-08-31, direct user spec: every NEW entry uses the trap+TSL
                # mechanic -- _update_option_sl_target_and_check branches on this
                # tag to route to _trap_update_tsl_and_check_exit instead of the
                # old option-premium-VWAP SL.
                "sl_mechanic": "trap",
            }
            # 2026-08-27, direct user spec: option-premium SL/target tracking starts
            # FRESH the moment the trade starts, not before -- pop any stale state
            # (shouldn't exist for a fresh symbol, but a same-day re-entry on a
            # symbol that already ran once -- possibly a different strike/contract
            # entirely -- must not inherit its prior armed level/ATP/target).
            self._option_sl_bar_key.pop(symbol, None)
            self._option_sl_bar_cur.pop(symbol, None)
            self._live_sl.pop(symbol, None)
            self._live_target.pop(symbol, None)
            self._live_option_atp.pop(symbol, None)
            self._option_adverse_lows[symbol] = []
            self._ensure_spot_feed(symbol)
            self._clog.info("OiOrb[%s/%s]: ENTRY CONFIRMED %s %s%d qty=%d @ %.2f (paper_mode=%s) "
                             "-- option-premium SL/target tracking starts now.",
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
                # spot_ltp: live UI reference price only (2026-08-27 -- SL/target no
                # longer track the stock, see _update_option_sl_target_and_check).
                # sl/target: the OPTION's own live levels, both None until the first
                # adverse vwap_sl_tf_minutes bar re-arms them -- the position runs on
                # the hard risk cap alone until then.
                "spot_ltp": self._live_spot_ltp.get(sym),
                "sl": self._live_sl.get(sym),
                "target": self._live_target.get(sym),
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
