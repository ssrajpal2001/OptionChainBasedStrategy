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

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from data_layer.instrument_registry import REGISTRY
from matrix_engine.option_matrix import ChainRow, ChainSnapshot, OptionMatrix
from strategies.core.base_book import AbstractStrategyBook
# Shared hard ₹/lot risk-cap constant (not the S&R tracker itself, which OI-ORB
# no longer uses as of 2026-08-27 -- see _update_vwap_sl_and_check's own docstring).
from strategies.core.support_resistance import (
    _MAX_RISK_RS_PER_LOT as _SR_MAX_RISK_RS_PER_LOT,
)
from strategies.oi_orb_screener import filters as oi_filters
from strategies.oi_orb_screener import oi_swing
from strategies.oi_orb_screener import screener
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener import store
from strategies.oi_orb_screener.events import OiOrbOrderEvent, OiOrbFillEvent

logger = logging.getLogger(__name__)

_EOD_TIME_DEFAULT = dtime(15, 15)
_EOD_POLL_SEC = 10.0
# 2026-09-07, real incident: was 5.0s. Real live data the same day showed
# upstox2/Fyers reconnecting roughly every ~10 minutes (each reconnect cycle
# itself taking ~10-20s to fully re-establish) -- two genuinely-fired signals
# (SOLARINDS, MANAPPURAM, both correctly detected via the new historical
# VWAP-retest check) were lost to entry_ltp_timeout purely because their
# option-tick subscription landed within a few hundred ms of one of these
# reconnects. 5s gives a feed reconnect essentially no room to recover
# before the entry is abandoned. Raised to 20s -- comfortably covers a normal
# reconnect cycle without meaningfully changing the entry price's freshness
# (a real signal's price doesn't stale out in 20s the way it would in
# minutes), and the strategy already treats a timeout as a hard skip (no
# partial/guessed price), so a longer wait only ever helps a signal that was
# genuinely about to get a real tick, never delays one that wasn't.
_ENTRY_LTP_WAIT_TIMEOUT_SEC = 20.0
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

# 2026-09-08, direct user spec: live exit mechanic, replaces HA+StochRSI --
# see the module docstring note at self._trap_exit_* in __init__ for the
# full three-tier design (multi-day HTF trap -> intraday HTF trap -> EOD).
# 2026-09-09: HTF re-optimized 75 -> 180min (scripts/oi_orb_candle_and_
# target_tf_backtest.py, real option-premium-priced backtest across the
# 46-row historical + real 2026-09-09 streamed dataset) -- a small but
# consistent improvement (PF 13.47->13.65, total +Rs192,286->+Rs195,069,
# return +84.37%->+85.59%) over every other HTF tested (60/90/120/240min).
# Widening this ONLY changes how far back the zone-DETECTION step looks to
# find genuine multi-day structure -- every trade is still fully intraday,
# entry and exit both same-day, EOD square-off unchanged.
_TRAP_EXIT_HTF_MULTIDAY_MIN = 180
_TRAP_EXIT_HTF_INTRADAY_MIN = 15
_TRAP_EXIT_LTF_MIN = 3
_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS = 15   # ~10 real trading days, same validated window
# (PF 13.47->13.65 via scripts/oi_orb_trap_target_tf_backtest.py). A 2026-09-10 attempt
# to narrow this to 7 days (reasoning: only the single most-recently-locked zone is ever
# checked, so less history "shouldn't" matter) was tested against today's real data and
# REVERTED same-day -- it changed which multiday zone set gets detected (fewer bars can
# surface a different, closer zone), and materially changed a real trade's outcome
# (FORCEMOT: 15-day version rode to +Rs6,660 via the intraday-tier fallback; 7-day
# version's multiday tier touched a different zone and exited far earlier for only
# +Rs1,150). Don't re-narrow this without a real backtest validating the new value
# specifically, not just the "recency-only usage" reasoning alone.

# 2026-09-08, direct user spec: hard SL, validated via scripts/oi_orb_trap_
# target_sl_backtest.py -- HA-candle close on the WRONG side of the running
# session VWAP (CALL: close < vwap; PUT: close > vwap).
# 2026-09-09 re-optimization (scripts/oi_orb_shaped_sl_streaming_backtest.py
# + scripts/oi_orb_sl_concept_comparison.py, real option-premium-priced,
# same real dataset): TF re-tuned 30->20min (best PF/win% of every TF
# tested, 15/20/30/45/60), and a minimum-distance buffer added so a merely
# marginal/noise-level VWAP cross no longer counts as adverse -- the
# adverse close must clear VWAP by at least this fraction of price. Chosen
# over 0.3%/0.5% (best PF at 0.2%, same max single-trade loss as the
# unbuffered baseline -- pure risk reduction, not a tradeoff). A shape gate
# (candle wick constraint) and several other SL concepts (ATR, flat %,
# LTF trap/S&R, option-native %, two-phase trailing, liquidity-sweep
# anchor) were also real-data-tested and did NOT beat this combination --
# see CLAUDE.md's OI-ORB Screener section for the full comparison.
_VWAP_SL_TF_MIN = 20
_VWAP_SL_MIN_GAP_PCT = 0.002

# 2026-09-10, real incident fix: see _spot_feed_retry_loop's own docstring.
_SPOT_FEED_RETRY_GRACE_SEC = 45.0
_SPOT_FEED_RETRY_POLL_SEC = 20.0

# 2026-09-15, real incident fix: see _live_price's own docstring -- 7 real
# shortlisted stocks (INFY/TCS/LTM/PERSISTENT/WIPRO/HDFCBANK/TATAELXSI,
# 2026-09-15) went completely dark for the entire day (zero signal_events,
# zero heartbeat lines) because a single incomplete/throttled NSE poll
# response dropped them from live_df with no fallback at all. POLL_SECONDS
# default is 20s; this allows several consecutive missed polls (a real
# Akamai throttle can span minutes, not just one cycle) before a symbol is
# treated as genuinely priceless.
_POLL_PRICE_STALE_SEC = 90.0

# 2026-09-16, direct user spec: futures-OI-regime directional gate (opt-in,
# default OFF until validated -- same graduation discipline as every other
# feature addition in this codebase). Two mutually-exclusive regimes off the
# underlying's OWN futures OI (never options OI, never today's opening OI):
#   OI_Change% > +1%  -> INCREASING -- direction comes ONLY from yesterday's
#                        candle (green->CALL-only, red->PUT-only), even if
#                        today's price action initially disagrees.
#   OI_Change% < -5%  -> DECREASING -- yesterday's candle is discarded;
#                        direction comes ONLY from today's trend (mapped to
#                        the stock's own live pChange sign -- the same
#                        convention screener.side_from_pchange already uses
#                        everywhere else in this codebase for "today's
#                        direction", not a separate new definition).
#   otherwise         -> NEUTRAL -- no trade that stock today.
# Evaluated ONCE per (symbol, day) at/after OI_REGIME_CHECK_TIME, using
# Upstox's V3 Full Market Quotes endpoint (oi + previous_oi in one call --
# verified live 2026-09-15/16 against 4 real contracts, previous_oi matched
# fetch_upstox_daily's own 'oi' on the last completed session exactly every
# time) for the OI side, and fetch_upstox_daily's own open/close for
# yesterday's candle direction.
_OI_REGIME_CHECK_TIME_DEFAULT = "09:16"
_OI_REGIME_INCREASE_MIN_PCT_DEFAULT = 1.0
_OI_REGIME_DECREASE_MAX_PCT_DEFAULT = -5.0

# 2026-09-16, direct user spec: pre-entry trap-target-already-touched gate
# (see _check_trap_target_touched_today's own docstring). Once a signal is
# skipped this way, re-checking the (real, network-fetching) touched-today
# condition on EVERY tick that makes a fresh new-high/new-low would hammer
# the same NSE/Upstox endpoints this codebase has repeatedly documented as
# throttle-sensitive -- rate-limit the re-check, same philosophy as every
# other throttled poll in this file.
_TRAP_GATE_RECHECK_MIN_SEC = 30.0

# 2026-09-18, direct user spec: "Future OI-Price Swing Breakout Strategy",
# an opt-in additive entry_exit_mode ("oi_swing_v1") -- see
# strategies/oi_orb_screener/oi_swing.py's own module docstring for the
# full mechanic + the three production fixes. Default remains the existing
# VWAP-retest/regime-gate mechanic for every currently-deployed binding;
# nothing here changes unless a deployment's own strategy_params sets
# entry_exit_mode="oi_swing_v1" explicitly.
_ENTRY_EXIT_MODE_DEFAULT = "vwap_retest"
_ENTRY_EXIT_MODE_OI_SWING = "oi_swing_v1"


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
        # 2026-09-01, direct user spec: raised from 13:00 to 15:00 to match
        # entry_window_end -- no reason to stop rescanning while entries can
        # still fire.
        afternoon_scan_end: str = "15:00",
        afternoon_scan_interval_sec: float = 300.0,
        # 2026-09-02, opt-in alternate entry mode -- see _immediate_check_
        # entry's own docstring. Default OFF; the existing zone/retest trap
        # mechanic remains the default for every deployment unless this is
        # explicitly turned on.
        immediate_entry_enabled: bool = False,
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
        # 2026-09-18, direct user spec: opt-in additive entry/exit decision
        # engine -- see oi_swing.py's own module docstring + the three
        # constants below. Any value other than _ENTRY_EXIT_MODE_OI_SWING
        # keeps the existing default mechanic completely unchanged.
        entry_exit_mode: str = _ENTRY_EXIT_MODE_DEFAULT,
        oi_swing_entry_cutoff: str = "14:30",
        oi_swing_min_hold_min: int = 10,
        # 2026-09-18, direct user spec: standalone "top gainer/loser" data
        # pipeline -- see screener.poll_top_gainers_losers's own module
        # docstring for the full 4-step spec. Verify-only this pass: never
        # touches self._shortlist_symbols/_positions/any entry decision.
        top_gainer_loser_oi_spurt_min_pct: float = 7.0,
        top_gainer_loser_pchange_max_pct: float = 4.0,
        top_gainer_loser_pchange_filter_enabled: bool = False,
        strategy_name: str = "oi_orb_screener",
    ) -> None:
        super().__init__(bus, cfg, _UNDERLYING_SENTINEL, client_id, binding_id)
        self._strategy_name = strategy_name
        self._vwap_touch_trackers: dict = {}   # legacy, left unused not deleted
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
        self._screener_cfg["IMMEDIATE_ENTRY_ENABLED"] = immediate_entry_enabled
        self._screener_cfg["RR_MULTIPLE"] = rr_multiple
        self._screener_cfg["TOP_GAINER_LOSER_OI_SPURT_MIN_PCT"] = top_gainer_loser_oi_spurt_min_pct
        self._screener_cfg["TOP_GAINER_LOSER_PCHANGE_MAX_PCT"] = top_gainer_loser_pchange_max_pct
        self._screener_cfg["TOP_GAINER_LOSER_PCHANGE_FILTER_ENABLED"] = top_gainer_loser_pchange_filter_enabled

        # ── standalone top gainer/loser pipeline (2026-09-18, direct user
        # spec) -- verify-only, never read by any entry/exit/trading
        # decision. See _top_gainer_loser_loop/_do_top_gainer_loser_poll. ──
        self._top_gainer_loser_last_poll_ts: float = 0.0
        self._top_gainer_loser_all: list = []          # last poll's full 2*N candidates
        self._top_gainer_loser_qualifying: list = []   # last poll's qualifying subset

        # ── entry_exit_mode="oi_swing_v1" (2026-09-18, direct user spec) ──
        self._entry_exit_mode = (
            entry_exit_mode if entry_exit_mode == _ENTRY_EXIT_MODE_OI_SWING
            else _ENTRY_EXIT_MODE_DEFAULT
        )
        try:
            _h, _m = str(oi_swing_entry_cutoff or "14:30").split(":")
            self._oi_swing_entry_cutoff = dtime(int(_h), int(_m))
        except Exception:
            self._oi_swing_entry_cutoff = oi_swing.ENTRY_CUTOFF_DEFAULT
        self._oi_swing_min_hold_min = max(0, int(oi_swing_min_hold_min))
        # Per-symbol state: real-time 5-min (bucket_ts, price, oi) series
        # built from live REST polls (see _oi_swing_exit_check), the
        # latest CONFIRMED swing high/low ratchet, and the last bucket a
        # point was already recorded for (so a poll cycle landing inside
        # an already-recorded bucket is a cheap no-op, not a duplicate
        # point / duplicate REST call).
        self._oi_swing_series: Dict[str, list] = {}
        self._oi_swing_high: Dict[str, Optional[float]] = {}
        self._oi_swing_low: Dict[str, Optional[float]] = {}
        self._oi_swing_last_bucket: Dict[str, datetime] = {}

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
        # 2026-09-10, real incident fix: see store.load_historical_check_done's
        # own docstring / _restore_from_db's restore call for the full incident.
        self._historical_check_done: set = set()
        # 2026-09-10, real incident fix (LTM double-evaluation): _daily_loop's
        # restore sequence (reset_session -> _restore_from_db -> _run_today_
        # pipeline) and _oi_spurt_history_loop are launched as separate,
        # concurrent asyncio.create_task() calls at start() -- nothing
        # synchronizes them, so _oi_spurt_history_loop's own poll cycle (which
        # streams newly-qualifying symbols and calls _apply_historical_
        # rolling_retest on them) can run its FIRST cycle before _restore_
        # from_db has finished populating self._historical_check_done from
        # the DB. Real incident: LTM's historical retest check ran twice
        # (13:20:37 and again 13:28:57, both correctly "not fired") because
        # the 13:28:49 restart's OI-spurt poll fired at 13:28:51 -- BEFORE
        # "restored 8 already-fired..." logged at 13:28:54. Both evaluations
        # happened to return the same correct answer here, but the race
        # itself is real and could re-fire a stale reference on a future
        # restart, same class of incident as the GVT&D/FORCEMOT bugs this
        # session already fixed. See _restore_from_db's own end-of-function
        # comment for where this gets set True.
        self._restore_from_db_ready = False
        self._entry_window_done_logged = False
        self._morning_historical_retest_applied = False
        self._restart_db_reconcile_applied = False
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
        # 2026-09-02, opt-in immediate-entry alternate mode -- see
        # _immediate_check_entry's own docstring. Hybrid stop: fixed ORB
        # floor (self._orb_frozen) from the instant entry fires, tightened
        # to a 15-min S1/R1 ladder once one establishes -- the ladder state
        # below is keyed the same way the 3-min trap TSL's own ladder is.
        self._immediate_tsl_calc: Dict[str, "object"] = {}
        self._immediate_tsl_acc: Dict[str, "object"] = {}
        self._immediate_tsl_fed_bars: Dict[str, int] = {}
        # 2026-09-06, direct user spec: the confirmed HA-shape + StochRSI(9,9)
        # exit backtested this week (scripts/oi_orb_ha_stochrsi_exit_backtest.py)
        # ported into live -- see _ha_stoch_check_exit's own docstring. Applies
        # to EVERY open position regardless of sl_mechanic (unlike the trap/
        # immediate_15m TSLs above, which only apply to their own entry style) --
        # this is meant as the universal, always-on exit, same as the backtest.
        self._ha_stoch_1m_acc: Dict[str, "object"] = {}
        self._ha_stoch_last_checked_bar_ts: Dict[str, datetime] = {}

        # 2026-09-08, direct user spec: replaces the HA+StochRSI exit above as
        # the LIVE exit mechanic (kept defined, not deleted, per this codebase's
        # own convention of leaving superseded mechanics in place unused) --
        # multi-day 75min HTF same-side trap zone + 3min S&R ladder, validated
        # this session (scripts/oi_orb_same_side_trap_multiday_htf_sweep.py) as
        # the config the user chose to run live, falling back to the
        # already-validated intraday 15min/3min version of the same mechanic
        # (scripts/oi_orb_trap_target_full_htf_ltf_sweep.py) when no multi-day
        # zone ever locks+touches, and EOD square-off as the final fallback --
        # same three-tier fallback shape the backtest itself used. See
        # _seed_trap_exit_state / _trap_multiday_exit_check /
        # _trap_intraday_exit_check for the full mechanic.
        self._trap_exit_multiday_zones: Dict[str, list] = {}
        self._trap_exit_multiday_fetch_done: Dict[str, bool] = {}
        self._trap_exit_touched: Dict[str, bool] = {}
        self._trap_exit_source: Dict[str, str] = {}
        self._trap_exit_calc: Dict[str, "object"] = {}
        self._trap_exit_ltf_acc: Dict[str, "object"] = {}
        self._trap_exit_ltf_fed: Dict[str, int] = {}
        self._trap_exit_intraday_1m_acc: Dict[str, "object"] = {}
        self._trap_exit_intraday_zones: Dict[str, list] = {}
        self._trap_exit_intraday_htf_fed: Dict[str, int] = {}

        # 2026-09-08, direct user spec: hard STOP-LOSS layer (this strategy had
        # target-style exits only -- trap zones, HA+StochRSI -- but never an
        # actual downside stop; the disabled hard-risk-cap was the only thing
        # that ever played this role, and it was turned off 2026-09-07). Wired
        # in with the SAME restart-replay discipline as the trap-exit target
        # mechanism above -- see _vwap_close_sl_check / _replay_vwap_close_sl.
        # Validated this session (scripts/oi_orb_trap_target_sl_backtest.py,
        # 4 SL families compared): 30-min HA-candle close vs session VWAP was
        # the only candidate that improved BOTH win% and PF over the no-SL
        # baseline while cutting the worst single-trade loss by ~75% -- every
        # other candidate (structural swing, ATR, tight fixed-%, faster VWAP
        # timeframes) either fired too often on noise (dragging win%/PF down
        # WITH total points) or barely fired at all (no real risk cap).
        self._sl_vwap_1m_acc: Dict[str, "object"] = {}
        self._sl_vwap_last_checked_bar_ts: Dict[str, datetime] = {}
        # 2026-09-08, direct user spec: "I want re-entry allowed after a SL
        # stopped out, but just once in that specific script for that day" --
        # (symbol, side) pairs that have already consumed their one-time
        # SL-stop-out re-entry allowance today. Day-scoped (cleared in
        # reset_session, never per-entry in _on_fill -- it must survive the
        # re-entry itself to correctly block a SECOND allowance).
        self._sl_reentry_used: Set[tuple] = set()

        # ── contract/feed/position state, keyed by stock symbol ────────
        self._pending_contracts: Dict[str, "stock_resolve.ResolvedContract"] = {}
        self._pending_fills: Dict[str, dict] = {}   # event_id -> context
        self._pending_closes: Dict[str, str] = {}   # event_id -> exit reason (OiOrbFillEvent carries no reason field)
        self._pending_close_details: Dict[str, str] = {}   # event_id -> human-readable exit detail (candle time/values)
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
        # 2026-09-16, direct user spec, REVISED: the futures-OI-regime gate
        # compares two FIXED historical points (today's own 09:15 OI vs
        # yesterday's own 15:39 OI), not a repeatedly-polled "live now"
        # value -- see _compute_oi_regime_side's own docstring for the full
        # mechanic and rationale (independently verified against NSE's own
        # real Bhavcopy). Both fetched lazily, once per (symbol, day), and
        # cached forever after that -- they never change once fetched, so
        # there is deliberately no refresh/poll loop for either.
        self._today_0915_oi: Dict[str, float] = {}
        self._prev_day_last_tick_oi: Dict[str, float] = {}
        # 2026-08-28 real incident fix: chronological history of every ADVERSE
        # bar's own low since entry, per symbol -- feeds
        # screener.pool_sl_from_adverse_lows() so the SL only arms once two
        # separate bars cluster near the same floor, not on the first dip.
        self._option_adverse_lows: Dict[str, List[float]] = {}
        # Stock spot tick feed -- kept for LIVE UI VISIBILITY ONLY now (the
        # SL/target check itself no longer uses it); see _ensure_spot_feed's
        # own docstring.
        self._live_spot_ltp: Dict[str, float] = {}   # symbol -> most recent live spot tick
        # 2026-09-06, direct user spec: real upstox2 ticks are now the PRIMARY
        # price source for entry/exit decisions (not just the dashboard's
        # spot_ltp display field, as this dict originally was) -- the 20s
        # NSE-poll dataframe (`live`) is kept as an automatic fallback for any
        # symbol whose tick feed has gone stale/quiet, same tick-primary/
        # poll-fallback philosophy this codebase's own dual-feeder active-
        # passive design already uses. See _live_price().
        self._live_spot_ltp_ts: Dict[str, datetime] = {}   # symbol -> monotonic-safe wall-clock of last tick
        self._TICK_STALE_SEC = 10.0
        # 2026-09-07: last _live_price() result per symbol (tick-primary,
        # NSE-poll-fallback already resolved) -- monitoring_state()'s
        # shortlist_vwap needs a real LTP to show even when the upstox2 tick
        # has gone stale/quiet, same as the WATCH heartbeat log already does
        # via _live_price()'s own fallback; reading self._live_spot_ltp
        # directly there would show "VWAP --" whenever ONLY the NSE-poll
        # fallback has a price, which is misleading since the heartbeat log
        # right above it clearly has one.
        self._last_known_price: Dict[str, float] = {}
        # 2026-09-15, real incident fix: carried-forward last-poll price, for
        # when a shortlisted symbol drops out of a given poll's live_df
        # entirely (not just a stale tick) -- see _live_price's own docstring.
        self._last_poll_price: Dict[str, float] = {}
        self._last_poll_price_ts: Dict[str, datetime] = {}
        # 2026-09-16, direct user spec: pre-entry trap-target-already-touched
        # gate -- (symbol, side) -> {"extreme": float, "last_check_ts": float
        # (time.monotonic)} once a VWAP-retest signal is skipped because
        # today's 180-min trap target was already touched. "extreme" is the
        # day's-high-so-far (CALL) / day's-low-so-far (PUT) tracked from the
        # skip point onward; a genuine NEW extreme re-triggers a fresh
        # touched-today check (rate-limited) and fires immediately if it
        # passes. See _check_trap_target_touched_today's own docstring.
        self._trap_gate_skipped: Dict[tuple, dict] = {}
        # 2026-09-16, direct user spec: futures-OI-regime directional gate --
        # computed at most ONCE per (symbol, day), cached here. None means
        # either "not computed yet" (not in _oi_regime_computed) or "computed
        # and blocked" (NEUTRAL regime / data failure / flat candle) -- check
        # membership in _oi_regime_computed to tell those two apart.
        self._oi_regime_side: Dict[str, Optional[str]] = {}
        self._oi_regime_computed: Set[str] = set()
        # 2026-09-16, direct user spec: a symbol that passed the 2%
        # price-move filter into the shortlist but then got blocked/removed
        # by the OI-regime gate used to just vanish from the UI entirely --
        # nothing persisted why. This keeps a per-symbol record (pChange,
        # both raw OI points, computed change%, and a human reason string)
        # so the dashboard can show "this stock passed step 1 (2% move) but
        # failed step 2 (futures OI regime), here's why" instead of the
        # symbol silently disappearing. Populated at the two spots a symbol
        # gets removed for a blocked OI-regime verdict (the live entry loop
        # and _gate_symbols_by_oi_regime); cleared only on reset_session().
        self._oi_regime_blocked: Dict[str, dict] = {}
        self._spot_tick_subscribed: Dict[str, bool] = {}
        # 2026-09-10, real incident fix: when _ensure_spot_feed's own subscribe
        # attempt was seen, so _spot_feed_retry_loop can detect "subscribed a
        # while ago, still zero live ticks" and force a genuine retry -- see
        # that loop's own docstring for the full incident.
        self._spot_feed_subscribed_at: Dict[str, datetime] = {}
        # Two-session scan state (2026-08-27, direct user spec).
        self._afternoon_scan_last_ts: float = 0.0    # throttle: don't re-scan every poll cycle

        # ── OI-change rank tracking (2026-08-30, direct user spec) ──────
        self._rank_last_poll_ts: float = 0.0
        self._rank_prev_top: set = set()
        self._rank_dropped: set = set()
        # ── Full-day OI-spurt history capture (2026-09-07, direct user spec) ──
        self._oi_spurt_hist_last_poll_ts: float = 0.0
        # ── Tick-by-tick VWAP accumulation (2026-09-10, direct user spec) ──
        self._vwap_tick_volume_cum_last: dict = {}

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
        self._historical_check_done = set()
        self._restore_from_db_ready = False
        self._entry_window_done_logged = False
        self._rejected = set()
        self._sl_reentry_used = set()
        self._trap_gate_skipped = {}
        self._oi_regime_side = {}
        self._oi_regime_computed = set()
        self._oi_regime_blocked = {}
        # 2026-09-07: guards the ONE morning call to _apply_historical_vwap_retest
        # (right after the regime freeze block below) so it doesn't re-run on every
        # poll cycle for the life of the day -- the afternoon rescan calls it again
        # itself, per newly-added symbol, so this flag is morning-path-only.
        self._morning_historical_retest_applied = False
        # 2026-09-08, direct user spec: guards the ONE restart-recovery
        # reconciliation against the DB's continuous per-minute scan log --
        # see _reconcile_shortlist_from_db's own docstring.
        self._restart_db_reconcile_applied = False
        self._stock_chains = {}
        self._chain_subscribed = {}
        self._volume_cum_last = {}
        self._volume_recent_delta = {}
        self._volume_history = {}
        self._oi_history = {}
        self._oi_history_last_poll_ts = 0.0
        self._vwap = screener.VwapState()
        self._vwap_armed = {}
        self._oi_swing_series = {}
        self._oi_swing_high = {}
        self._oi_swing_low = {}
        self._oi_swing_last_bucket = {}
        self._top_gainer_loser_last_poll_ts = 0.0
        self._top_gainer_loser_all = []
        self._top_gainer_loser_qualifying = []
        self._trap_1m_acc = {}
        self._trap_3m_acc = {}
        self._trap_zones = {}
        self._trap_entry_calc = {}
        self._trap_tsl_calc = {}
        self._trap_tsl_acc = {}
        self._trap_tsl_fed_bars = {}
        # 2026-09-02, opt-in immediate-entry alternate mode -- see
        # _immediate_check_entry's own docstring.
        self._immediate_tsl_calc = {}
        self._immediate_tsl_acc = {}
        self._immediate_tsl_fed_bars = {}
        self._afternoon_scan_last_ts = 0.0
        # 2026-08-30, direct user spec: OI-change rank tracking (09:16-09:30
        # poll window) -- see _rank_tracking_loop's own docstring.
        self._rank_last_poll_ts = 0.0
        self._rank_prev_top: set = set()
        self._rank_dropped: set = set()
        # 2026-09-07, direct user spec: independent full-day, threshold-agnostic
        # OI-spurt history capture -- see _oi_spurt_history_loop's own docstring.
        self._oi_spurt_hist_last_poll_ts = 0.0
        self._today_0915_oi = {}
        self._prev_day_last_tick_oi = {}
        # 2026-09-10, direct user spec: tick-by-tick VWAP accumulation --
        # see _spot_tick_loop's own VWAP-update block.
        self._vwap_tick_volume_cum_last = {}
        self._clog.info("OiOrb[%s/%s]: session reset for new trading day.",
                         self._client_id, self._binding_id)

    def start(self) -> None:
        super().start()
        # 2026-09-16, real live incident: 6+ minutes of active entry-loop
        # cycling produced zero OI-REGIME log lines (success or failure) even
        # though screener.CONFIG["OI_REGIME_GATE_ENABLED"] verified True via a
        # direct fresh import on the same server -- needed a permanent,
        # unambiguous boot-time record of what THIS book's own resolved
        # _screener_cfg actually contains, since none existed before.
        self._clog.info(
            "OiOrb[%s/%s]: OI_REGIME_GATE_ENABLED=%s OI_REGIME_CHECK_TIME=%s "
            "OI_REGIME_INCREASE_MIN_PCT=%s OI_REGIME_DECREASE_MAX_PCT=%s",
            self._client_id, self._binding_id,
            self._screener_cfg.get("OI_REGIME_GATE_ENABLED"),
            self._screener_cfg.get("OI_REGIME_CHECK_TIME", _OI_REGIME_CHECK_TIME_DEFAULT),
            self._screener_cfg.get("OI_REGIME_INCREASE_MIN_PCT", _OI_REGIME_INCREASE_MIN_PCT_DEFAULT),
            self._screener_cfg.get("OI_REGIME_DECREASE_MAX_PCT", _OI_REGIME_DECREASE_MAX_PCT_DEFAULT),
        )
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
        self._tasks.append(asyncio.create_task(
            self._oi_spurt_history_loop(), name=f"oiorb_spurthist_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._top_gainer_loser_loop(), name=f"oiorb_gainerloser_{self._client_id}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._spot_feed_retry_loop(), name=f"oiorb_spotretry_{self._client_id}_{self._binding_id}"))
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
                finally:
                    # 2026-09-10 real incident fix (see self._restore_from_db_ready's
                    # own comment in __init__): unblocks _oi_spurt_history_loop's
                    # own poll cycle, which was racing ahead of this restore on a
                    # fresh restart. Set unconditionally (success OR failure) so a
                    # restore exception can never block the OI-spurt loop forever --
                    # same best-effort-never-block discipline as every other restore
                    # step in this file.
                    self._restore_from_db_ready = True
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
            # 2026-09-18 CRITICAL FIX, found while adding entry_exit_mode=
            # "oi_swing_v1": this used to hardcode "sl_mechanic": "vwap" for
            # EVERY restored position regardless of what it was actually
            # entered under -- the comment above it claimed "keep it on the
            # mechanic it was actually entered under," but the code never
            # did that; a restored "trap"/"immediate_15m" position was
            # silently switched onto the old VWAP-close SL after ANY
            # restart. r["entry_reason"] (persisted by store.open_position,
            # set from pending["reason"] in _on_fill) already carries
            # exactly what's needed to reconstruct the real mechanic --
            # same mapping _on_fill's own BUY branch uses, just read back
            # from the DB instead of the live pending-fill context. This
            # also fixes restore for a genuine oi_swing_v1 position (which
            # otherwise would have been silently downgraded onto the wrong
            # exit mechanic after any mid-session restart -- exactly the
            # kind of real-money-relevant restart bug this new mode's own
            # review explicitly asked to check for).
            entry_reason = r.get("entry_reason") or ""
            sl_mechanic = (
                _ENTRY_EXIT_MODE_OI_SWING if entry_reason == "oi_swing_v1_entry"
                else "immediate_15m" if entry_reason == "immediate_orb_entry"
                else "vwap" if entry_reason == "vwap_retest"
                else "trap" if entry_reason
                else "vwap"   # unknown/blank reason (e.g. a pre-existing row from
                               # before entry_reason was ever recorded) -- degrade
                               # to the same safe default this code always used.
            )
            self._positions[r["symbol"]] = {
                "contract": contract, "qty": r["qty"], "entry_price": r["entry_price"],
                "paper_mode": bool(r["paper_mode"]), "opened_at": datetime.fromisoformat(r["entry_ts"]),
                "sl_mechanic": sl_mechanic,
            }
            self._ensure_option_feed(r["symbol"], contract)
            self._ensure_spot_feed(r["symbol"])
            # 2026-09-10, real incident fix: a restored OPEN position's VWAP
            # was never seeded at all -- _reconcile_shortlist_from_db (the
            # shortlist-side restart recovery) only re-seeds symbols it
            # re-adds to self._shortlist_symbols, and this restore-positions
            # path runs independently, possibly before that. Without this,
            # the position's VWAP-close SL silently has no protection at all
            # (the check bails out on vwap is None) until enough live ticks
            # happen to accumulate one from zero. Same real-Upstox-intraday-
            # bar seed used everywhere else.
            await self._seed_vwap_from_upstox_intraday(r["symbol"])
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
            # 2026-09-08: the trap-exit tiers apply to EVERY open position
            # regardless of sl_mechanic (same universal-exit shape as the
            # HA+StochRSI check they replaced) -- a restored position needs
            # its own state seed too, not just a fresh _on_fill entry, AND
            # (direct user catch) that seed must REPLAY real history since
            # this position's actual entry_ts, not just re-detect zones --
            # otherwise a zone-touch/ladder progress (or even an already-
            # earned breach) that happened before this restart would be
            # silently lost. Background task, best-effort.
            # 2026-09-10 real incident fix: this position's own actual side
            # (from the contract just resolved above), never re-derived from
            # the stock's current/possibly-flipped pChange sign -- see
            # _side_from_option_type's own docstring for the full incident.
            # 2026-09-17: trap-zone exit tiers disabled (see the main exit
            # loop's own block comment) -- seeding them on restart would
            # just be wasted NSE/Upstox calls for state nothing consumes.
            # side_for_zones = self._side_from_option_type(r["option_type"])
            # asyncio.create_task(self._seed_trap_exit_state(
            #     r["symbol"], side_for_zones, self._positions[r["symbol"]]["opened_at"]))

        already_fired = await asyncio.to_thread(store.load_already_fired, self._client_id, self._binding_id, td)
        rejected = await asyncio.to_thread(store.load_rejected, self._client_id, self._binding_id, td)
        self._already_fired |= already_fired
        self._rejected |= rejected
        if already_fired or rejected:
            self._clog.info("OiOrb[%s/%s]: restored %d already-fired + %d rejected signal(s) from DB.",
                             self._client_id, self._binding_id, len(already_fired), len(rejected))
        # 2026-09-10, real incident fix: the historical-replay-and-fire-
        # immediately check is deterministic given the same day's history --
        # it always finds and reports the SAME first-ever retest moment, no
        # matter how many times it's called. Without this restore, every
        # restart re-ran it and re-logged a "signal_fired" using an
        # increasingly stale reference price (real incident: GVT&D re-logged
        # "retested at 09:18, price=4638.90" across 10+ restarts through
        # 12:21, even though real spot by then was trading ~4540-4547 --
        # corrupting the recorded entry rationale for the real trade this
        # eventually produced). See store.load_historical_check_done's own
        # docstring for the full incident.
        historical_check_done = await asyncio.to_thread(
            store.load_historical_check_done, self._client_id, self._binding_id, td)
        self._historical_check_done |= historical_check_done

        # 2026-09-10 CRITICAL FIX, real incident: a restart happening AFTER
        # cfg["ENTRY_WINDOW_END"] used to leave the WHOLE dashboard panel
        # blank for the rest of the day (real report: "AFTER RESTART
        # EVERYTHING IS GONE FROM OI SCANNER, NOTHING SHOWING IN UI").
        # _run_today_pipeline's own _wait_until_actionable() bails out
        # before its main polling loop even starts once the entry window
        # has closed -- and that loop was the ONLY place regime/shortlist/
        # ORB ever got reconstructed from the DB on a restart
        # (_reconcile_shortlist_from_db, gated on reaching cfg["ORB_END"]
        # inside that loop). A late restart therefore never ran it at all,
        # regardless of how much real scan/shortlist/ORB data a PRIOR
        # process instance had already written to the DB earlier that same
        # day. Restoring regime + shortlist here instead -- unconditionally,
        # every restart, regardless of time of day -- means the dashboard
        # panel is never blank just because the process happened to
        # restart late; only genuinely NEW entries still correctly stop
        # once the entry window has closed. Guarded on
        # _restart_db_reconcile_applied (same flag _run_today_pipeline's
        # own loop already used) so a restart that's still WITHIN the
        # actionable window doesn't redundantly reconcile twice.
        cfg = self._screener_cfg
        regime = await asyncio.to_thread(store.load_scan_regime, self._client_id, self._binding_id, td)
        if regime and self._regime is None:
            self._regime = regime
            self._clog.info("OiOrb[%s/%s]: restored regime=%s from DB (restart recovery).",
                             self._client_id, self._binding_id, regime)
        if not self._restart_db_reconcile_applied:
            self._restart_db_reconcile_applied = True
            try:
                await self._reconcile_shortlist_from_db(cfg)
            except Exception:
                self._clog.exception(
                    "OiOrb[%s/%s]: restart-recovery shortlist reconciliation failed "
                    "(non-fatal, dashboard panel may stay empty until the next scan).",
                    self._client_id, self._binding_id)

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
        # 2026-09-16: never re-add a symbol the OI-regime gate already
        # permanently blocked for today.
        oi_gate_on = cfg.get("OI_REGIME_GATE_ENABLED", False)
        new_symbols = [
            s for s in shortlist["symbol"].tolist()
            if s not in self._shortlist_symbols
            and not (oi_gate_on and s in self._oi_regime_computed and self._oi_regime_side.get(s) is None)
        ]
        if not new_symbols:
            return

        new_rows = []
        new_orb_levels = []
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
                # 2026-09-07, direct user spec: same historical-retest check as the
                # morning path -- an afternoon-added symbol has been trading since
                # 09:15 too, so check whether its VWAP-retest already completed
                # before the afternoon scan even noticed it.
                await self._apply_historical_vwap_retest([sym], cfg)
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: afternoon-scan backfill failed for %s "
                                      "(non-fatal, VWAP will start cold from now).",
                                      self._client_id, self._binding_id, sym)
            # 2026-09-04 CRITICAL FIX, found while preparing a backtest: the morning
            # path's ORB freeze block (self._regime is None -> ...) runs EXACTLY ONCE
            # per day, only over self._shortlist_symbols as they exist at that moment
            # -- any symbol added later, here, was backfilled with real 09:15-09:25
            # bars but self._orb_frozen[sym] was never actually set from them, and
            # orb_high/orb_low were never persisted (record_shortlist's own INSERT
            # doesn't carry those columns; update_orb_levels() is a separate call this
            # function never made). _immediate_check_entry() hard-requires
            # self._orb_frozen.get(sym) to be non-None before it will EVER fire, so
            # immediate-entry mode was silently, permanently blocked for every
            # afternoon-added stock -- no error, it just sat there unable to enter.
            # Trap-retest mode was unaffected (it only reads live-polled bars, never
            # self._orb_frozen). Mirrors the morning freeze block's own two lines.
            # The DB write itself is deferred to AFTER record_shortlist below (see
            # new_orb_levels) -- update_orb_levels() is an UPDATE, not an upsert, so
            # calling it here (before record_shortlist's own INSERT has even run for
            # this brand-new row) would silently match zero rows and do nothing.
            h, l = self._bars.orb(sym, cfg["ORB_START"], cfg["ORB_END"])
            if h is not None:
                self._orb_frozen[sym] = (h, l)
                new_orb_levels.append((sym, h, l))
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
        for sym, h, l in new_orb_levels:
            await asyncio.to_thread(store.update_orb_levels, self._client_id, self._binding_id, sym, h, l)
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

    # ── full-day OI-spurt history capture (2026-09-07, direct user spec) ──

    async def _oi_spurt_history_loop(self) -> None:
        """"instead of checking for only stocks whose spurt is above 7% we
        will get the top 20 stocks data and save in db with its oi spurt so
        that after 1 to 2 week we have all the stocks with their oi spurt to
        analyse what is best threshold... save complete oi spurt from start
        of day till end of day."

        Deliberately INDEPENDENT of _rank_tracking_loop above: that loop
        only covers 09:16-09:30 and, as a side effect, DROPS a not-yet-
        entered candidate from the live shortlist if its rank falls -- a
        real trading-behavior effect this data-collection pass must never
        touch. This loop runs OI_SPURT_HISTORY_START-END (default full
        session, 09:15-15:30), never reads or writes
        self._shortlist_symbols/_rejected/_positions, and only ever calls
        store.record_oi_spurt_history -- pure logging, zero effect on any
        trading decision.
        """
        while self._running:
            # 2026-09-10 real incident fix: this loop is started as a
            # separate, concurrent task from _daily_loop's own restore
            # sequence -- without this wait, this loop's poll cycle can run
            # (and stream a symbol into _apply_historical_rolling_retest)
            # BEFORE self._historical_check_done has been restored from the
            # DB on a fresh restart, causing a duplicate historical-retest
            # evaluation. See self._restore_from_db_ready's own comment in
            # __init__ for the full incident (LTM double-evaluation).
            if not self._restore_from_db_ready:
                await asyncio.sleep(1)
                continue
            now = datetime.now(IST)
            cfg = self._screener_cfg
            if not cfg.get("OI_SPURT_HISTORY_ENABLED", True):
                await asyncio.sleep(60)
                continue
            win_start = cfg.get("OI_SPURT_HISTORY_START", "09:15")
            win_end = cfg.get("OI_SPURT_HISTORY_END", "15:30")
            now_key = now.strftime("%H:%M")
            if not (win_start <= now_key < win_end) or cfg.get("IGNORE_TIME_WINDOWS"):
                await asyncio.sleep(30)
                continue
            now_ts = now.timestamp()
            interval = float(cfg.get("OI_SPURT_HISTORY_POLL_SEC", 60.0) or 60.0)
            if now_ts - self._oi_spurt_hist_last_poll_ts < interval:
                await asyncio.sleep(5)
                continue
            self._oi_spurt_hist_last_poll_ts = now_ts
            try:
                if self._nse is None:
                    self._nse = await asyncio.to_thread(screener.NSESession)
                await self._do_oi_spurt_history_poll(now, cfg)
            except Exception:
                self._clog.warning("OiOrb[%s/%s]: OI-spurt history poll failed (non-fatal, will "
                                    "retry next interval).", self._client_id, self._binding_id,
                                    exc_info=True)
            await asyncio.sleep(5)

    async def _do_oi_spurt_history_poll(self, now: datetime, cfg: dict) -> None:
        """One purely-observational poll cycle -- split out for direct unit
        testing, same shape as _do_rank_poll."""
        top_n = int(cfg.get("OI_SPURT_HISTORY_TOP_N", 20) or 20)
        ranked = await asyncio.to_thread(screener.poll_oi_rank, self._nse, cfg, top_n)
        if ranked is None or ranked.empty:
            return

        poll_ts = now.isoformat(timespec="seconds")
        rows = [
            {"symbol": r["symbol"], "rank": int(r["rank"]),
             "oi_spurt_pct": float(r["oi_spurt_pct"]), "price_change_pct": float(r["pChange"])}
            for _, r in ranked.iterrows()
        ]
        await asyncio.to_thread(store.record_oi_spurt_history, self._client_id, self._binding_id,
                                 poll_ts, rows)
        self._clog.info(
            "OiOrb[%s/%s]: OI-SPURT HISTORY POLL @%s top-%d: %s",
            self._client_id, self._binding_id, now.strftime("%H:%M:%S"), len(rows),
            ", ".join(f"{r['symbol']}(#{int(r['rank'])},{r['oi_spurt_pct']:.1f}%)"
                      for _, r in ranked.iterrows()),
        )

    async def _top_gainer_loser_loop(self) -> None:
        """2026-09-18, direct user spec: standalone "top gainer/loser" data
        pipeline. Deliberately SEPARATE from _oi_spurt_history_loop/
        _rank_tracking_loop/build_shortlist above -- never reads or writes
        self._shortlist_symbols/_rejected/_positions, only ever calls
        screener.poll_top_gainers_losers (pure) + store.record_top_gainer_
        loser_poll (logging). This pass is verify-only: its own qualifying
        list (self._top_gainer_loser_qualifying) is not consumed by any
        entry/exit/trading decision anywhere in this file.

        Same polling-loop shape as _oi_spurt_history_loop (restore-ready
        gate, enabled flag, session window, fixed poll interval) -- see
        that method's own docstring for why each piece is there; ported
        here unchanged rather than reinvented."""
        while self._running:
            if not self._restore_from_db_ready:
                await asyncio.sleep(1)
                continue
            now = datetime.now(IST)
            cfg = self._screener_cfg
            if not cfg.get("TOP_GAINER_LOSER_ENABLED", True):
                await asyncio.sleep(60)
                continue
            win_start = cfg.get("TOP_GAINER_LOSER_START", "09:15")
            win_end = cfg.get("TOP_GAINER_LOSER_END", "15:30")
            now_key = now.strftime("%H:%M")
            if not (win_start <= now_key < win_end) or cfg.get("IGNORE_TIME_WINDOWS"):
                await asyncio.sleep(30)
                continue
            now_ts = now.timestamp()
            interval = float(cfg.get("TOP_GAINER_LOSER_POLL_SEC", 60.0) or 60.0)
            if now_ts - self._top_gainer_loser_last_poll_ts < interval:
                await asyncio.sleep(5)
                continue
            self._top_gainer_loser_last_poll_ts = now_ts
            try:
                if self._nse is None:
                    self._nse = await asyncio.to_thread(screener.NSESession)
                await self._do_top_gainer_loser_poll(now, cfg)
            except Exception:
                self._clog.warning(
                    "OiOrb[%s/%s]: top gainer/loser poll failed (non-fatal, will retry next "
                    "interval).", self._client_id, self._binding_id, exc_info=True)
            await asyncio.sleep(5)

    async def _do_top_gainer_loser_poll(self, now: datetime, cfg: dict) -> None:
        """One purely-observational poll cycle -- split out for direct unit
        testing, same shape as _do_oi_spurt_history_poll/_do_rank_poll."""
        candidates, qualifying = await asyncio.to_thread(screener.poll_top_gainers_losers, self._nse, cfg)
        if candidates is None or candidates.empty:
            return

        qualifying_symbols = set(qualifying["symbol"]) if not qualifying.empty else set()
        poll_ts = now.isoformat(timespec="seconds")
        rows = []
        for _, r in candidates.iterrows():
            oi_spurt = r.get("oi_spurt_pct")
            rows.append({
                "symbol": r["symbol"], "rank_type": r["rank_type"], "rank": int(r["rank"]),
                "price_change_pct": float(r["pChange"]) if pd.notna(r.get("pChange")) else None,
                "oi_spurt_pct": float(oi_spurt) if pd.notna(oi_spurt) else None,
                "qualified": r["symbol"] in qualifying_symbols,
            })
        await asyncio.to_thread(store.record_top_gainer_loser_poll, self._client_id, self._binding_id,
                                 poll_ts, rows)

        self._top_gainer_loser_all = rows
        self._top_gainer_loser_qualifying = [r for r in rows if r["qualified"]]
        self._clog.info(
            "OiOrb[%s/%s]: TOP GAINER/LOSER POLL @%s: %d candidates, %d qualifying: %s",
            self._client_id, self._binding_id, now.strftime("%H:%M:%S"), len(rows),
            len(self._top_gainer_loser_qualifying),
            ", ".join(f"{r['symbol']}({r['rank_type']}#{r['rank']},px={r['price_change_pct']:+.2f}%,"
                      f"oi={r['oi_spurt_pct']:.1f}%)" if r["oi_spurt_pct"] is not None
                      else f"{r['symbol']}({r['rank_type']}#{r['rank']},px={r['price_change_pct']:+.2f}%,oi=n/a)"
                      for r in self._top_gainer_loser_qualifying) or "(none)",
        )

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
            # 2026-09-01 CRITICAL FIX, real incident: this used to `return` here,
            # which meant the polling `while` loop below -- the ONLY place
            # _maybe_run_afternoon_scan() ever gets called -- was never reached.
            # _daily_loop() only calls _run_today_pipeline() once per calendar
            # day, so a morning that started empty had ZERO path to ever find
            # afternoon candidates, no matter what two_session_scan_enabled said.
            # Real incident, 2026-09-01: NIFTY flat (-0.18%) at 09:25 -> morning
            # shortlist empty -> book went silent for the rest of the day, EVEN
            # THOUGH 7 real candidates (HEROMOTOCO/BAJAJ-AUTO/ADANIENSOL/MARUTI/
            # KALYANKJIL/POLYCAB/KEI) existed by 12:51 and two_session_scan_enabled
            # was explicitly turned on for exactly this scenario. Fixed: fall
            # through into the polling loop with an empty shortlist instead of
            # returning -- the loop's own shortlist-dependent work (heartbeat,
            # chain subscriptions, ORB tracking) is naturally a no-op on an empty
            # list, but _maybe_run_afternoon_scan() now gets its real chance to
            # run every cycle and populate self._shortlist_symbols later in the day.
            self._clog.info("OiOrb[%s/%s]: no candidates passed the filters today (NIFTY pChange %+.2f%%) "
                             "-- still entering the monitor loop so an afternoon rescan (if enabled) can "
                             "find candidates later.",
                             self._client_id, self._binding_id, nifty_pchange)
            await asyncio.to_thread(store.record_scan, self._client_id, self._binding_id,
                                     nifty_pchange, "no_candidates")
            self._shortlist_symbols = []
            self._prev_close_map = {}
            self._shortlist_pchange = {}
        else:
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
            _oi_spurt_map = (shortlist.set_index("symbol")["oi_spurt_pct"].to_dict()
                             if "oi_spurt_pct" in shortlist.columns else {})
            self._clog.info("OiOrb[%s/%s]: shortlist ready (%d): %s",
                             self._client_id, self._binding_id, len(self._shortlist_symbols),
                             ", ".join(f"{s}(px={self._shortlist_pchange.get(s, 0):+.2f}%,"
                                       f"oi_spurt={_oi_spurt_map.get(s, 0):.2f}%)"
                                       for s in self._shortlist_symbols))
            # 2026-09-06, direct user spec: subscribe every shortlisted stock to
            # the dedicated upstox2 WebSocket the MOMENT it's shortlisted, not
            # only once a position opens on it -- real ticks are now consumed
            # for entry/exit DECISIONS too (see _live_price(), _spot_tick_loop's
            # new self._live_tick_ltp/_live_tick_ts recording), not just the
            # dashboard's spot_ltp display field as before. Tick-primary,
            # NSE-poll-fallback: this subscribe call was already idempotent
            # and safe to call early (_ensure_spot_feed no-ops if already
            # subscribed), so widening WHEN it's called is the only change.
            for sym in self._shortlist_symbols:
                self._ensure_spot_feed(sym)

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
            # 2026-09-07: the historical-retest check itself is deferred until AFTER
            # the regime freeze block below (self._regime is still None here) -- see
            # that block's own call to _apply_historical_vwap_retest. Firing before
            # regime is known would either wrongly block on regime=None (side_allowed_
            # by_regime treats None like neutral -> nothing tradeable) or, worse, skip
            # the regime check entirely (the actual 2026-09-07 incident: SOLARINDS
            # CALL fired historically while today's frozen regime was BEARISH, which
            # should have blocked it -- see _apply_historical_vwap_retest's own gate).

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
                ltp = self._live_price(sym, live)
                if ltp is None:
                    continue
                self._last_known_price[sym] = ltp
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
                # 2026-08-31: "armed=" used to reflect the VWAP-retest mechanic's own
                # self._vwap_armed dict -- that entry path was replaced by the trap+S1
                # mechanic (_trap_check_entry), which never touches _vwap_armed, so this
                # would otherwise permanently freeze at "armed=False" forever, misleading
                # rather than merely uninformative. Shows the trap zone count + whether a
                # retest ladder is currently running instead.
                _n_zones = len(self._trap_zones.get(sym, []))
                _trap_str = (f"trap=running(ladder)" if sym in self._trap_entry_calc
                             else f"trap={_n_zones}zone{'s' if _n_zones != 1 else ''}")
                _vwap_str = f"VWAP={vwap_now:.2f} {_trap_str}" if vwap_now else f"VWAP=— {_trap_str}"
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
                # and keep a short rolling history for a trailing average.
                # 2026-09-10, direct user spec: this NSE-poll delta used to also feed
                # self._vwap.update() (a coarse ~20s-snapshot approximation) -- removed.
                # VWAP is now driven by genuine tick-by-tick accumulation in
                # _spot_tick_loop (real IndexTick.volume deltas, continuous, not a
                # 20s snapshot) plus the one-time real-history seed on shortlist-entry/
                # restart (_seed_vwap_from_upstox_intraday). This filter's own delta
                # tracking is unrelated to VWAP and is untouched.
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
                # 2026-09-07: deferred from right after the morning backfill (before
                # this block) specifically so self._regime is real by the time this
                # runs -- _apply_historical_vwap_retest's own regime gate needs it,
                # and firing with self._regime still None would either wrongly block
                # everything (None reads like neutral) or, the real incident this
                # ordering fix closes, skip the regime check while it was still
                # unset. Guarded to run once -- this freeze block itself only ever
                # runs once too (the `if self._regime is None` guard above).
                if not self._restart_db_reconcile_applied:
                    self._restart_db_reconcile_applied = True
                    await self._reconcile_shortlist_from_db(cfg)
                if not self._morning_historical_retest_applied:
                    self._morning_historical_retest_applied = True
                    await self._apply_historical_vwap_retest(self._shortlist_symbols, cfg)

            # Trap-mechanic TSL: runs for every currently-open "trap"-tagged
            # position regardless of the entry window/regime state above --
            # an open position's own exit tracking must never pause just
            # because new entries aren't being evaluated right now.
            # 2026-09-02: "immediate_15m"-tagged positions get their own
            # parallel 15-min S1/R1 TSL instead.
            # 2026-09-08, direct user spec (SUPERSEDED 2026-09-17, see below):
            # the universal exit for EVERY open position used to be (1) a
            # hard SL -- 20-min HA/plain candle close on the wrong side of
            # session VWAP, checked FIRST every cycle -- then (2) the
            # multi-day 180min HTF same-side trap + 3min S&R ladder, falling
            # back to (3) the intraday 15min/3min version of the same
            # mechanic when no multi-day zone had locked+touched yet.
            #
            # 2026-09-17, direct user decision, backed by a full real-data
            # backtest series against the same 19 real traded stocks
            # (scripts/oi_orb_trailing_exit_backtest.py): give-back TSL,
            # real S&R S1/R1 structural TSL (swept across 15 intraday
            # timeframes from 1 to 90min, in BOTH a pure-pin and a
            # trails-to-current-active-level variant -- 0/19 fires on
            # EVERY combination, a complete and decisive null), and a
            # from-scratch intraday trap-zone exit concept were all
            # checked against the plain "20-min VWAP-close SL + EOD,
            # no target" baseline -- none of them beat it with anything
            # more than thin, single-sample evidence. Direct user
            # instruction: "as u have run the backtest and u confirm that
            # no target is needed only 20 min sl concept stick with that
            # and move on with live implementation." The
            # _trap_multiday_exit_check/_trap_intraday_exit_check calls
            # below are therefore DISABLED (commented out, not deleted --
            # same convention already used for the superseded
            # _ha_stoch_check_exit) -- the live exit for every open
            # position is now just _vwap_close_sl_check (20-min VWAP-close
            # hard SL) + EOD square-off, matching the validated baseline
            # exactly. The functions/state they use (self._trap_exit_*,
            # self._trap_ladder_check) are kept defined, not removed, in
            # case future real-data evidence ever reopens this.
            for sym, pos in list(self._positions.items()):
                await self._backfill_futures_oi_display(sym)

                ltp = self._live_price(sym, live)
                if ltp is None:
                    continue
                # 2026-09-10 CRITICAL FIX, real incident (TECHM): this open
                # position's own actual side, never re-derived from the
                # stock's current/possibly-flipped pChange sign -- see
                # _side_from_option_type's own docstring for the full
                # incident (a CALL position got mislabeled "PUT" once real
                # price decline flipped pChange negative after entry,
                # silently running the wrong exit mechanic for the rest of
                # the day).
                side = self._side_from_option_type(pos["contract"].option_type)
                # 2026-09-18, direct user spec: a position opened under
                # entry_exit_mode="oi_swing_v1" uses its OWN exit decision
                # engine entirely -- never the VWAP-close SL / trap-TSL
                # mechanics below (those stay exactly as-is for every other
                # position). Tagged on the position itself (sl_mechanic),
                # not the book's current self._entry_exit_mode, so a
                # restored position keeps using the mechanic it was
                # actually opened under.
                if pos.get("sl_mechanic") == _ENTRY_EXIT_MODE_OI_SWING:
                    await self._oi_swing_exit_check(sym, side, ltp, now)
                    continue
                await self._vwap_close_sl_check(sym, side, ltp, now)
                if sym in self._eod_closing:
                    continue
                # 2026-09-17: disabled, see the block comment above.
                # await self._trap_multiday_exit_check(sym, side, ltp, now)
                # if sym in self._eod_closing:
                #     continue
                # await self._trap_intraday_exit_check(sym, side, ltp, now)
                # if sym in self._eod_closing:
                #     continue   # already claimed by the SL/trap-exit checks above this cycle
                mech = pos.get("sl_mechanic")
                if mech not in ("trap", "immediate_15m"):
                    continue
                if mech == "trap":
                    await self._trap_update_tsl_and_check_exit(sym, side, ltp, now)
                else:
                    await self._immediate_update_tsl_and_check_exit(sym, side, ltp, now)

            # 2026-09-18, direct user spec: entry_exit_mode="oi_swing_v1"
            # replaces the ENTIRE entry-evaluation block below (VWAP-retest/
            # regime-gate/trap-target-touched-gate/immediate-ORB) with the
            # plain immediate 2% price trigger -- no NIFTY regime dependency,
            # no additive filters, no trap-target gate. See
            # _oi_swing_entry_scan's own docstring.
            if self._entry_exit_mode == _ENTRY_EXIT_MODE_OI_SWING:
                await self._oi_swing_entry_scan(live, now, now_key)
                if (not cfg.get("IGNORE_TIME_WINDOWS") and not self._positions
                        and now.time() > self._squareoff_time):
                    # Past EOD, flat -- nothing left for this loop to do today
                    # (mirrors the existing mode's own end-of-day break below).
                    break
                await asyncio.sleep(cfg["POLL_SECONDS"])
                continue

            entry_window_open = cfg.get("IGNORE_TIME_WINDOWS") or (
                cfg["ENTRY_WINDOW_START"] <= now_key < cfg["ENTRY_WINDOW_END"])
            immediate_entry_on = cfg.get("IMMEDIATE_ENTRY_ENABLED", False)
            if self._regime is not None and entry_window_open:
                regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
                for sym in list(self._shortlist_symbols):
                    if sym in self._positions or sym in self._pending_contracts:
                        continue

                    # 2026-09-16, direct user spec: futures-OI-regime
                    # directional gate (opt-in, default OFF -- see
                    # _compute_oi_regime_side's own docstring for the full
                    # mechanic). When enabled, REPLACES the plain pChange-
                    # based side with the OI-regime's own verdict -- computed
                    # once per (symbol, day) at/after OI_REGIME_CHECK_TIME.
                    if cfg.get("OI_REGIME_GATE_ENABLED", False):
                        if sym not in self._oi_regime_computed:
                            check_time = cfg.get("OI_REGIME_CHECK_TIME", _OI_REGIME_CHECK_TIME_DEFAULT)
                            if now_key < check_time and not cfg.get("IGNORE_TIME_WINDOWS"):
                                continue
                            self._oi_regime_computed.add(sym)
                            self._oi_regime_side[sym] = await self._compute_oi_regime_side(sym)
                        side = self._oi_regime_side.get(sym)
                        if side is None:
                            # 2026-09-16, direct user spec: a symbol whose OI-
                            # regime comes back NEUTRAL (or fails to compute
                            # at all -- conservative, same as the gate's own
                            # docstring) does not just get skipped this cycle,
                            # it comes OUT of the pool entirely for the rest
                            # of today ("that stock will come out of pool for
                            # whose day"). New stocks keep entering the pool
                            # separately via the existing OI-spurt streaming
                            # mechanism, unaffected.
                            self._record_oi_regime_blocked(sym, cfg)
                            if sym in self._shortlist_symbols:
                                self._shortlist_symbols.remove(sym)
                            self._shortlist_pchange.pop(sym, None)
                            self._clog.info(
                                "OiOrb[%s/%s]: %s removed from pool -- OI-regime NEUTRAL/blocked "
                                "for today.", self._client_id, self._binding_id, sym)
                            continue
                    else:
                        side = screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))

                    if (sym, side) in self._already_fired or (sym, side) in self._rejected:
                        continue
                    if not screener.side_allowed_by_regime(
                            side, self._regime, regime_filter_on):
                        continue
                    ltp = self._live_price(sym, live)
                    if ltp is None:
                        continue

                    # 2026-09-16, direct user spec: a symbol currently gated
                    # (skipped earlier because its 180-min trap target was
                    # already touched today) skips the normal entry mechanic
                    # entirely and instead just watches for a genuine new
                    # day-high (CALL) / day-low (PUT) -- the re-trigger.
                    gate_key = (sym, side)
                    gate_state = self._trap_gate_skipped.get(gate_key)
                    if gate_state is not None:
                        extreme = gate_state["extreme"]
                        breached = (ltp > extreme) if side == "CALL" else (ltp < extreme)
                        if not breached:
                            continue
                        gate_state["extreme"] = ltp
                        now_mono = _time.monotonic()
                        if now_mono - gate_state.get("last_check_ts", 0.0) < _TRAP_GATE_RECHECK_MIN_SEC:
                            continue
                        gate_state["last_check_ts"] = now_mono
                        touched, _zone = await self._check_trap_target_touched_today(sym, side)
                        if touched:
                            continue   # still spent even with the fresh zone -- keep waiting
                        del self._trap_gate_skipped[gate_key]
                        self._already_fired.add(gate_key)
                        orb_lvl = self._orb_frozen.get(sym, (0.0, 0.0))
                        await self._emit_vwap_signal(
                            sym, side, ltp, "trap_gate_new_extreme_retrigger", orb_lvl,
                            now.strftime("%H:%M:%S"), label="TRAP-GATE-RETRIGGER")
                        continue

                    if immediate_entry_on:
                        fire = self._immediate_check_entry(sym, side, now)
                        reason = "immediate_orb_entry"
                    else:
                        # 2026-09-06, direct user spec: VWAP-retest is the
                        # active entry mechanic again, matching this week's
                        # entire validated backtest series exactly (see
                        # _vwap_check_entry's own docstring for why).
                        # _trap_check_entry (bear/bull-trap zone entry) is
                        # left in place, unused, not deleted.
                        fire = self._vwap_check_entry(sym, side, ltp)
                        reason = "vwap_retest"
                    if not fire:
                        continue

                    # 2026-09-16, direct user spec: a genuine VWAP-retest just
                    # fired -- before actually taking the trade, check whether
                    # today's own 180-min trap target has already been
                    # touched. If so, there's no real edge left for the day;
                    # skip this trade and start watching for a fresh day-high/
                    # day-low breach instead (see the gate_state block above).
                    touched, zone = await self._check_trap_target_touched_today(sym, side)
                    if touched:
                        self._trap_gate_skipped[gate_key] = {
                            "extreme": ltp, "last_check_ts": _time.monotonic()}
                        zone_str = (f"zone=[{zone['zone_lo']:.2f},{zone['zone_hi']:.2f}]"
                                    if zone else "zone=n/a")
                        self._clog.info(
                            "OiOrb[%s/%s]: %s TRAP-TARGET already touched today -- skipping entry "
                            "(%s ltp=%.2f), watching for a new day-%s to re-trigger.",
                            self._client_id, self._binding_id, sym, zone_str, ltp,
                            "high" if side == "CALL" else "low")
                        await asyncio.to_thread(
                            store.log_signal_event, self._client_id, self._binding_id, sym,
                            "trap_target_already_touched_today_skip", side=side,
                            detail=f"{zone_str} ltp={ltp:.2f}")
                        continue

                    self._already_fired.add((sym, side))
                    orb_lvl = self._orb_frozen.get(sym, (0.0, 0.0))
                    await self._emit_vwap_signal(
                        sym, side, ltp, reason, orb_lvl, now.strftime("%H:%M:%S"),
                        label=("IMMEDIATE-ORB" if immediate_entry_on else "TRAP-RETEST"),
                    )
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

    async def _emit_vwap_signal(self, sym: str, side: str, ltp: float, reason: str,
                                 orb_lvl: tuple, ts_str: str, label: str) -> None:
        """Factored out of the live tick loop (2026-09-07) so the historical-
        replay path (_apply_historical_vwap_retest) can fire an identical
        signal for a retest that already completed in real intraday history
        before this book started watching the stock, not just a live tick."""
        sig = screener.Signal(symbol=sym, side=side, reason=reason,
                              trigger_price=ltp, orb_high=orb_lvl[0], orb_low=orb_lvl[1],
                              ts=ts_str)
        self._clog.info(
            "OiOrb[%s/%s]: SIGNAL %s BUY %s %s ltp=%.2f reason=%s",
            self._client_id, self._binding_id, sig.symbol, sig.side,
            label, sig.trigger_price, sig.reason)
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

    async def _reconcile_shortlist_from_db(self, cfg: dict) -> None:
        """2026-09-08, direct user spec: on a mid-day restart, don't trust
        this restarted process's own single fresh scan to reproduce the same
        top-N a prior process instance already found and was tracking --
        OI-spurt/price-move values drift minute to minute, so a restart's own
        scan can genuinely differ (the real 2026-09-08 incident that dropped
        GVT&D/HAL/NATIONALUM/HINDZINC from the shortlist entirely). The DB's
        own continuous per-minute scan log (oi_spurt_history, written by
        _oi_spurt_history_loop, on by default all session) is the
        authoritative record of what should currently be tracked -- reads
        its MOST RECENT poll and onboards any symbol that passes this book's
        own tradeable filter and isn't already on this process's own
        shortlist, using the SAME onboarding steps _maybe_run_afternoon_scan
        already uses for a newly-discovered symbol (ORB/VWAP backfill from
        Yahoo, orb_frozen, and -- critically -- the historical VWAP-retest
        replay, so a retest that already genuinely happened before this
        process restarted still fires immediately). Best-effort, runs once
        per process life (same guard shape as _morning_historical_retest_
        applied): no rows in the DB yet (first-ever start of the day) is a
        normal no-op, not a failure."""
        rows = await asyncio.to_thread(
            store.load_latest_scan_symbols, self._client_id, self._binding_id,
            datetime.now(IST).date().isoformat())
        if not rows:
            return
        min_pct = cfg.get("OI_SPURT_MIN_PCT", 7.0)
        new_syms = []
        for r in rows:
            sym = r.get("symbol")
            if not sym or sym in self._shortlist_symbols:
                continue
            metric = r.get("oi_spurt_pct")
            if metric is None or abs(metric) < min_pct:
                continue
            pchange = r.get("price_change_pct") or 0.0
            self._shortlist_symbols.append(sym)
            self._shortlist_pchange[sym] = pchange
            new_syms.append(sym)
            self._clog.info(
                "OiOrb[%s/%s]: %s reconstructed from DB scan history (restart recovery) -- "
                "pChange=%+.2f%% oi_spurt=%s.",
                self._client_id, self._binding_id, sym, pchange, r.get("oi_spurt_pct"))
            try:
                await asyncio.to_thread(screener.backfill_orb_from_yahoo, self._bars, [sym], cfg)
                # 2026-09-10, direct user spec: restart recovery re-seeds VWAP from
                # real Upstox intraday history (not Yahoo) -- same real-bar,
                # HLC3-weighted, REPLACE-semantics seed used when a symbol first
                # streams into the shortlist. This is the exact restart-safety
                # mechanism the user asked for: "if any time app restart we again
                # get historic data and from there start computing tick by tick" --
                # tick-by-tick accumulation resumes naturally in _spot_tick_loop the
                # moment ticks start arriving again after this seed completes.
                await self._seed_vwap_from_upstox_intraday(sym)
            except Exception:
                self._clog.exception(
                    "OiOrb[%s/%s]: %s restart-recovery ORB/VWAP backfill failed (non-fatal, "
                    "VWAP starts cold from now).", self._client_id, self._binding_id, sym)
            h, l = self._bars.orb(sym, cfg["ORB_START"], cfg["ORB_END"])
            if h is not None:
                self._orb_frozen[sym] = (h, l)
                await asyncio.to_thread(store.update_orb_levels, self._client_id, self._binding_id, sym, h, l)
            self._ensure_spot_feed(sym)
        if not new_syms:
            return
        await asyncio.to_thread(
            store.record_shortlist, self._client_id, self._binding_id,
            [{"symbol": s, "price_change_pct": self._shortlist_pchange.get(s), "oi_spurt_pct": None,
              "score": None, "side_bias": "bullish" if self._shortlist_pchange.get(s, 0) > 0 else "bearish"}
             for s in new_syms])
        self._clog.info(
            "OiOrb[%s/%s]: restart recovery reconstructed %d stock(s) from DB scan history: %s",
            self._client_id, self._binding_id, len(new_syms), ", ".join(new_syms))
        # Historical VWAP-retest replay for the reconstructed symbols only --
        # session-1 symbols already got this from the morning call right
        # after this method returns; re-running it for symbols already
        # checked would be harmless (idempotent, gated on _already_fired/
        # _rejected) but wasteful.
        await self._apply_historical_vwap_retest(new_syms, cfg)

    def _record_oi_regime_blocked(self, sym: str, cfg: dict) -> None:
        """2026-09-16, direct user spec: "show which stocks were scanned in
        2% logic and will show that future OI is the issue due to which it
        did not pass step 2." Called right before a symbol is removed from
        the pool for a blocked OI-regime verdict -- snapshots the pChange
        that got it into the shortlist in the first place (step 1) plus the
        futures-OI numbers that blocked it (step 2), with a plain-English
        reason, into self._oi_regime_blocked so monitoring_state() can keep
        showing it after it's gone from self._shortlist_symbols. The reason
        bucket (INCREASING-no-direction / DECREASING-continuation-or-no-
        direction / no-data -- NEUTRAL removed 2026-09-16, second same-day
        revision, see _compute_oi_regime_side's own docstring) is re-derived
        here from the same cached today_0915_oi/prev_day_last_tick_oi +
        threshold _compute_oi_regime_side already uses, rather than widening
        that function's own return type (which every caller/test currently
        treats as a plain Optional[str] side) -- good enough for display
        purposes without touching a shared, already-tested decision
        function."""
        today_oi = self._today_0915_oi.get(sym)
        yday_oi = self._prev_day_last_tick_oi.get(sym)
        pchange = self._shortlist_pchange.get(sym)
        dec_max = float(cfg.get("OI_REGIME_DECREASE_MAX_PCT", _OI_REGIME_DECREASE_MAX_PCT_DEFAULT))
        if today_oi is None or not yday_oi:
            oi_change_pct = None
            reason = "futures-OI data unavailable (no futures key/token/09:15 bar/prior-day bar)"
        else:
            oi_change_pct = round((today_oi - yday_oi) / yday_oi * 100.0, 2)
            if oi_change_pct <= dec_max:
                reason = (f"futures OI {oi_change_pct:.2f}% (DECREASING) but today's move only "
                          f"continues yesterday's own direction, not a reversal (or flat/no data)")
            else:
                reason = (f"futures OI {oi_change_pct:+.2f}% (INCREASING) but yesterday's candle "
                          f"gave no clear direction (doji/no data)")
        self._oi_regime_blocked[sym] = {
            "pchange": pchange,
            "today_0915_oi": today_oi,
            "yday_1539_oi": yday_oi,
            "oi_change_pct": oi_change_pct,
            "reason": reason,
        }

    async def _gate_symbols_by_oi_regime(self, symbols: list, cfg: dict) -> list:
        """2026-09-16 CRITICAL FIX, real incident (COFORGE, see the caller's
        own comment): the futures-OI-regime gate (OI_REGIME_GATE_ENABLED,
        added 2026-09-16) only ever ran inside the live per-cycle entry
        loop -- the historical catch-up replay function
        (_apply_historical_vwap_retest) could fire an entry for a symbol
        whose retest had already completed earlier today WITHOUT ever
        checking whether that symbol's OI-regime would have blocked it.
        This shared helper applies the IDENTICAL gate check + pool-removal
        behavior the live loop already uses (same self._oi_regime_computed/
        self._oi_regime_side cache -- computing it here means the live loop
        below simply reuses the cached result, never recomputes) so a
        symbol can never trade via either replay path without passing the
        same gate every other entry has to pass. Returns the subset of
        `symbols` that are still eligible (gate disabled -> everything
        passes through unchanged; gate enabled but not yet at its check
        time -> left in, deferred to the live loop; gate enabled and
        genuinely NEUTRAL/blocked -> removed from the pool here, same
        "comes out of pool for the rest of today" behavior as the live
        loop's own block)."""
        if not cfg.get("OI_REGIME_GATE_ENABLED", False):
            return symbols
        now_key = datetime.now(IST).strftime("%H:%M")
        check_time = cfg.get("OI_REGIME_CHECK_TIME", _OI_REGIME_CHECK_TIME_DEFAULT)
        ignore_windows = cfg.get("IGNORE_TIME_WINDOWS")
        eligible = []
        for sym in symbols:
            if sym not in self._oi_regime_computed:
                if now_key < check_time and not ignore_windows:
                    # Not yet time to compute -- leave it in the shortlist,
                    # the live loop will gate it once OI_REGIME_CHECK_TIME
                    # passes. Do NOT let it fire via the replay path in the
                    # meantime.
                    continue
                self._oi_regime_computed.add(sym)
                self._oi_regime_side[sym] = await self._compute_oi_regime_side(sym)
            side = self._oi_regime_side.get(sym)
            if side is None:
                self._record_oi_regime_blocked(sym, cfg)
                if sym in self._shortlist_symbols:
                    self._shortlist_symbols.remove(sym)
                self._shortlist_pchange.pop(sym, None)
                self._clog.info(
                    "OiOrb[%s/%s]: %s removed from pool -- OI-regime NEUTRAL/blocked "
                    "for today (checked before historical replay could fire it).",
                    self._client_id, self._binding_id, sym)
                continue
            eligible.append(sym)
        return eligible

    async def _apply_historical_vwap_retest(self, symbols: list, cfg: dict) -> None:
        """2026-09-07, direct user spec: "when we started the application and
        stocks were already there in the scan list it should have called
        intraday historical data and found if it satisfied the vwap touch
        concept or not -- if yes, immediately trade should have started."

        Called once for every symbol newly added to the shortlist (morning
        scan AND the 12:00-15:00 afternoon rescan) -- replays real intraday
        1-min history through the exact same check_vwap_retest_entry() state
        machine the live tick loop uses (screener.historical_vwap_retest_check/
        replay_vwap_retest_from_bars). A symbol whose retest already
        genuinely completed earlier today fires immediately, right here,
        instead of silently starting its arm/retest state cold and waiting
        for a brand new cross-and-retest cycle that may not come again for
        the rest of the day. A symbol that only got as far as arming (crossed
        to the correct side but never retested) has that arm state carried
        forward into self._vwap_armed, so the very next live tick continues
        from where the real market already was, instead of restarting from
        scratch. Best-effort: any missing/failed Yahoo data for a symbol
        leaves it at the safe cold-start default (armed=False), identical to
        today's pre-existing behavior for that symbol.

        2026-09-16 CRITICAL FIX, real incident: this replay path never
        applied the futures-OI-regime gate at all -- a symbol whose plain
        VWAP-retest already completed earlier today (real history, replayed
        once right after shortlist-add) could fire and trade here BEFORE
        the live per-cycle entry loop ever got a chance to evaluate/remove
        it via _compute_oi_regime_side. Applying the identical gate check
        here, before this function is allowed to fire anything, so the
        historical catch-up path can never trade a symbol the live loop
        would have blocked."""
        if not symbols:
            return
        symbols = await self._gate_symbols_by_oi_regime(symbols, cfg)
        if not symbols:
            return
        # 2026-09-10, same restart-safety fix as _apply_historical_rolling_retest
        # (see that function's own comment + store.load_historical_check_done's
        # docstring for the GVT&D incident this closes) -- never re-run this
        # deterministic replay for a symbol already evaluated today.
        symbols_sides = {
            sym: screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0))
            for sym in symbols
            if (sym, screener.side_from_pchange(self._shortlist_pchange.get(sym, 0.0)))
               not in self._historical_check_done
        }
        if not symbols_sides:
            return
        try:
            results = await asyncio.to_thread(screener.historical_vwap_retest_check, symbols_sides, cfg)
        except Exception:
            self._clog.exception("OiOrb[%s/%s]: historical VWAP-retest check failed "
                                  "(non-fatal, arm state stays cold for %s).",
                                  self._client_id, self._binding_id, symbols)
            return
        now = datetime.now(IST)
        for sym, side in symbols_sides.items():
            result = results.get(sym)
            if not result:
                continue
            self._historical_check_done.add((sym, side))
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sym,
                "historical_check_evaluated", side=side,
                detail=f"fired={result['fired']}")
            if result["fired"]:
                if (sym, side) in self._already_fired or (sym, side) in self._rejected:
                    continue
                # 2026-09-07 real incident fix: this path was firing unconditionally,
                # unlike the live tick loop (which always gates on side_allowed_by_
                # regime before entering) -- confirmed live, a SOLARINDS CALL fired
                # here on a day whose frozen regime was BEARISH, which should have
                # blocked it (bearish day: CALL ignored, PUT tradeable). A retest that
                # genuinely completed in history is still a real fact worth knowing,
                # so it's logged either way -- only the actual entry is gated.
                regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
                if not screener.side_allowed_by_regime(side, self._regime, regime_filter_on):
                    self._clog.info(
                        "OiOrb[%s/%s]: %s %s HISTORICAL VWAP-RETEST completed at %s (price=%.2f) "
                        "but BLOCKED by regime=%s -- not entering.",
                        self._client_id, self._binding_id, sym, side,
                        result["fire_ts"], result["fire_price"], self._regime)
                    continue
                self._clog.info(
                    "OiOrb[%s/%s]: %s %s HISTORICAL VWAP-RETEST already completed at %s "
                    "(price=%.2f) before this book started watching it -- firing immediately.",
                    self._client_id, self._binding_id, sym, side,
                    result["fire_ts"], result["fire_price"])
                self._already_fired.add((sym, side))
                orb_lvl = self._orb_frozen.get(sym, (0.0, 0.0))
                await self._emit_vwap_signal(
                    sym, side, result["fire_price"], "vwap_retest_historical", orb_lvl,
                    now.strftime("%H:%M:%S"), label="HISTORICAL-RETEST",
                )
            else:
                self._vwap_armed[sym] = result["armed"]
                if result["armed"]:
                    self._clog.info(
                        "OiOrb[%s/%s]: %s %s armed from real intraday history (bars_replayed=%d) "
                        "-- waiting for the retest touch on the next live tick.",
                        self._client_id, self._binding_id, sym, side, result["bars_replayed"])

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
        otm_frac = self._screener_cfg.get("STRIKE_OTM_PCT", 0.0) / 100.0
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

        # 2026-09-06, direct user correction (supersedes the 2026-08-24 2% OTM
        # spec): strike is ATM -- otm_frac defaults to 0.0, so raw_strike is
        # just the spot trigger price; resolve_contract rounds it to the
        # nearest valid listed strike step.
        otm_frac = self._screener_cfg.get("STRIKE_OTM_PCT", 0.0) / 100.0
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

        # 2026-09-10, real incident fix: a re-entry (or any second position on
        # the same symbol today) onto a DIFFERENT strike than the just-closed
        # one used to read a STALE leftover LTP here -- _live_option_ltp is
        # keyed only by stock symbol, and _option_tick_loop's own strike-match
        # guard correctly stops the OLD contract's ticks from overwriting it
        # going forward, but nothing ever cleared the value the old contract's
        # LAST tick had already written. _await_first_ltp's poll loop just
        # checks "> 0" -- it can't distinguish a genuinely fresh tick from
        # that stale leftover, so it returned the old contract's last price
        # immediately, before the new contract had ticked even once. Real
        # trade: ATHERENERG re-entered CE1620, entry recorded at 54.50 --
        # actually CE1640's (the closed leg's) last price; real CE1620 LTP
        # was 64.95 per the broker terminal at that moment. Clearing here
        # forces _await_first_ltp to genuinely wait for the new contract's
        # own first real tick, same as a brand-new entry already does.
        self._live_option_ltp.pop(sig.symbol, None)
        self._live_option_atp.pop(sig.symbol, None)

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

    def _live_price(self, symbol: str, live_df, log_source: bool = False) -> Optional[float]:
        """2026-09-06, direct user spec: tick-primary, NSE-poll-fallback price
        lookup. Prefers a real upstox2 tick (self._live_spot_ltp) if one has
        arrived within self._TICK_STALE_SEC seconds; otherwise falls back to
        the 20s-polled NSE dataframe (`live_df.loc[symbol, "lastPrice"]`,
        exactly what every call site used unconditionally before this pass)
        so a symbol whose tick feed hasn't started yet (or has gone quiet)
        never goes blind.

        2026-09-15, real incident fix: a THIRD fallback -- the last known
        poll price, carried forward for up to _POLL_PRICE_STALE_SEC seconds
        -- for when `symbol` is missing from `live_df` itself (not just a
        stale/absent tick). Real incident: INFY/TCS/LTM/PERSISTENT/WIPRO/
        HDFCBANK/TATAELXSI (all correctly shortlisted, all comfortably past
        the 2% filter) dropped out of live_df on every poll cycle after the
        initial shortlist build for the entire 2026-09-15 session -- likely
        a throttled/incomplete NSE response (this codebase already has
        multiple confirmed Akamai-throttle incidents, see NSESession's own
        docstring) -- and with no fallback at all, this function returned
        None every single cycle, all day, for all 7: zero signal_events,
        zero heartbeat lines, completely dark despite being live, valid,
        tradeable candidates the whole time. Returns None only when NONE of
        tick / current poll / recent-carried-forward poll has a price."""
        ts = self._live_spot_ltp_ts.get(symbol)
        if ts is not None and (datetime.now(IST) - ts).total_seconds() <= self._TICK_STALE_SEC:
            ltp = self._live_spot_ltp.get(symbol)
            if ltp is not None and ltp > 0:
                if log_source:
                    self._clog.debug("OiOrb[%s/%s]: %s price from live tick (upstox2) ltp=%.2f",
                                      self._client_id, self._binding_id, symbol, ltp)
                return ltp
        if live_df is not None and symbol in live_df.index:
            ltp = float(live_df.loc[symbol, "lastPrice"])
            if ltp > 0:
                self._last_poll_price[symbol] = ltp
                self._last_poll_price_ts[symbol] = datetime.now(IST)
            if log_source:
                self._clog.debug("OiOrb[%s/%s]: %s price from NSE-poll fallback ltp=%.2f "
                                  "(tick stale or not yet arrived)",
                                  self._client_id, self._binding_id, symbol, ltp)
            return ltp
        last_ts = self._last_poll_price_ts.get(symbol)
        if last_ts is not None and (datetime.now(IST) - last_ts).total_seconds() <= _POLL_PRICE_STALE_SEC:
            ltp = self._last_poll_price.get(symbol)
            if ltp is not None and ltp > 0:
                if log_source:
                    self._clog.debug(
                        "OiOrb[%s/%s]: %s price from carried-forward last poll ltp=%.2f "
                        "(missing from this cycle's live_df -- age=%.0fs)",
                        self._client_id, self._binding_id, symbol, ltp,
                        (datetime.now(IST) - last_ts).total_seconds())
                return ltp
        return None

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
                    self._spot_feed_subscribed_at[stock_symbol] = datetime.now(IST)
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
            self._spot_feed_subscribed_at[stock_symbol] = datetime.now(IST)
            self._clog.info("OiOrb[%s/%s]: subscribed live spot feed for %s (live UI reference price only).",
                             self._client_id, self._binding_id, stock_symbol)
        except Exception:
            self._clog.exception("OiOrb[%s/%s]: live spot feed subscribe failed for %s.",
                                  self._client_id, self._binding_id, stock_symbol)

    async def _seed_vwap_from_upstox_intraday(self, sym: str) -> None:
        """2026-09-10, real incident fix + direct user spec: when a stock
        (re-)enters the shortlist, compute its running session VWAP from
        REAL Upstox intraday 1-min bars (09:15->now), never starting cold
        from whatever moment it happened to get noticed.

        Real incident: TECHM entered the shortlist via the STREAMING path
        (_stream_new_top20_symbols) at 09:39:28, mid-way through a real
        ~24-minute rally (1498->1538+) that had already been running since
        the open -- but that path never seeded VWAP from any historical
        source at all (unlike the other two shortlist-add paths, which both
        call backfill_vwap_from_yahoo), so TECHM's VWAP started completely
        empty and only began accumulating from that moment's live polls
        onward. Result: computed VWAP (1530.05) diverged sharply from the
        real broker/TradingView session VWAP, because it never saw the
        first 24 minutes of real trading at all.

        Uses REAL Upstox intraday bars (not Yahoo, not broker ATP -- ATP
        resets on every restart per direct user spec) with a genuine HLC3
        (typical-price) weighting, matching the standard VWAP formula every
        broker terminal / TradingView uses -- NOT the coarse poll-snapshot
        approximation this file's live self._vwap.update() otherwise uses
        (that one still runs afterward for the rest of the day; this seed
        just gives it a real starting basis instead of zero).

        REPLACE semantics (VwapState.replace, not .seed): a stock
        re-entering the shortlist later in the day gets a fresh,
        authoritative recompute from the full real history up to that
        moment, rather than merging with whatever fragile poll-based state
        it may have accumulated in between. Best-effort throughout -- any
        failure just leaves VWAP on the live poll-based accumulation alone,
        same behavior as before this fix existed."""
        try:
            eq_key = stock_resolve.resolve_eq_instrument_key(sym)
            if not eq_key:
                return
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            rows = await fetch_upstox_intraday_1m(eq_key, token)
            if not rows:
                return
            num = 0.0
            den = 0.0
            for r in rows:
                vol = float(r.get("volume", 0) or 0)
                if vol <= 0:
                    continue
                hlc3 = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3.0
                num += hlc3 * vol
                den += vol
            if den > 0:
                self._vwap.replace(sym, num, den)
                self._clog.info(
                    "OiOrb[%s/%s]: %s VWAP seeded from %d real Upstox intraday bars "
                    "(HLC3-weighted, 09:15->now) -- vwap=%.2f.",
                    self._client_id, self._binding_id, sym, len(rows), num / den)
        except Exception:
            self._clog.exception(
                "OiOrb[%s/%s]: %s VWAP intraday seed failed -- falls back to live "
                "poll-based accumulation only.", self._client_id, self._binding_id, sym)

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
                # 2026-09-07, direct user spec: "only exit is HA+StockRSI as we
                # have done backtest with that only -- remove other exit
                # condition from oi scanner." Confirmed explicitly to also
                # include the hard_risk_cap backstop, not just the SL/target
                # ratchet -- HA+StochRSI + EOD square-off are now the ONLY
                # things that can close a position. Both calls disabled here
                # (not deleted -- their state/dashboard fields are still read
                # elsewhere; a full cleanup of the now-dead
                # _check_hard_risk_cap/_update_option_sl_target_and_check
                # methods and their self._live_sl/_live_target/etc. state is
                # a separate, lower-risk-window follow-up, not done live
                # mid-trading-day).
                # await self._check_hard_risk_cap(tick.underlying, tick.ltp)
                # await self._update_option_sl_target_and_check(tick.underlying, tick.ltp, tick.timestamp)
                # 2026-09-18, direct user follow-up (same day as shipping the
                # entry_exit_mode="oi_swing_v1" mode above): the hard risk cap
                # was re-enabled here as this mode's own Fix 1, then explicitly
                # turned back off the same trading day after it fired live in
                # paper_route (ZYDUSLIFE, 09:59:37, -Rs2070 -- confirmed working
                # exactly as coded, not a bug) -- direct instruction: "dont use
                # hard stoploss". oi_swing_v1 positions now rely purely on the
                # OI-swing exit + EOD square-off, no independent risk-cap
                # backstop. This is the real 13-day-backtest "no cap" variant
                # (best win rate on that sample, 73.7%, but also that sweep's
                # single worst loss, -Rs25,048.75) -- a real, known tradeoff,
                # not an oversight. Every other position's exit mechanics are
                # completely unaffected (the two disabled lines above are
                # unrelated to this mode entirely).
                # pos = self._positions.get(tick.underlying)
                # if pos is not None and pos.get("sl_mechanic") == _ENTRY_EXIT_MODE_OI_SWING:
                #     await self._check_hard_risk_cap(tick.underlying, tick.ltp)

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
                # 2026-09-06: record for ANY subscribed symbol (shortlisted,
                # not just already-open positions) -- this is now the
                # tick-primary price source for entry evaluation too, not
                # just the UI's spot_ltp field for already-open positions.
                if symbol not in self._shortlist_symbols and symbol not in self._positions:
                    continue
                ltp = float(ev.ltp or 0.0)
                if ltp <= 0:
                    continue
                self._live_spot_ltp[symbol] = ltp
                self._live_spot_ltp_ts[symbol] = datetime.now(IST)
                # 2026-09-10, direct user spec: TRUE tick-by-tick VWAP
                # accumulation off the already-subscribed live feed, replacing
                # the old 20-second poll-snapshot self._vwap.update() call in
                # the scan loop (removed) -- direct user correction to an
                # earlier draft of this fix that instead re-fetched Upstox's
                # full intraday REST history once a minute per shortlisted
                # symbol: "will this not create rate limit... instead of get
                # the historical data for computing vwap and when it is added
                # to websocket we start tick by tick calculation and if any
                # time app restart we again get historic data and from there
                # start computing tick by tick." That's exactly the design
                # here: ONE real historical REST fetch when a symbol first
                # streams into the shortlist (_seed_vwap_from_upstox_intraday,
                # also re-run once per symbol on restart-recovery -- see
                # _reconcile_shortlist_from_db), then continuous real-tick
                # accumulation from here on, zero further REST calls. Uses
                # ev.volume -- the SAME real cumulative-session-volume field
                # every IndexTick already carries (confirmed present on the
                # dataclass) -- diffed against the last tick's cumulative
                # value, mirroring the exact price*volume-delta formula the
                # old poll-based method used, just driven by every real tick
                # instead of a 20s snapshot.
                #
                # 2026-09-17 CRITICAL FIX, real gap found while removing the
                # oi_orb_screener_top20 variant: this real tick-by-tick VWAP
                # accumulation call was wrongly gated behind
                # "if self._top20_mode:" -- meaning the STANDARD variant's
                # self._vwap has likely been frozen at its one-time seed
                # value (_seed_vwap_from_upstox_intraday) all day, every day,
                # since this design was introduced (see this block's own
                # 2026-09-10 comment above, which describes this as THE real
                # tick-by-tick source for BOTH variants, replacing the old
                # poll-based update -- it was never actually top20-exclusive).
                # Ungated now so the standard variant's VWAP genuinely
                # updates continuously, matching what was always documented.
                if symbol in self._shortlist_symbols or symbol in self._positions:
                    cum_vol = float(ev.volume or 0)
                    last_cum = self._vwap_tick_volume_cum_last.get(symbol)
                    if last_cum is not None and cum_vol >= last_cum:
                        vol_delta = cum_vol - last_cum
                        if vol_delta > 0:
                            self._vwap.update(symbol, ltp, vol_delta)
                    self._vwap_tick_volume_cum_last[symbol] = cum_vol
            except Exception:
                self._clog.exception("OiOrb[%s/%s]: spot tick processing error (recovered).",
                                      self._client_id, self._binding_id)

    def _vwap_check_entry(self, sym: str, side: str, ltp: float) -> bool:
        """2026-09-06, direct user spec: switch the live entry mechanic BACK
        to VWAP-retest (screener.check_vwap_retest_entry) -- the exact
        mechanic this whole week's real-data backtest series (VWAP-retest
        entry + 15-min HA-shape/StochRSI(9,9) exit, the Fib-extension
        comparison, the option-vs-stock-MIS comparison) actually used.
        _trap_check_entry (bear/bull-trap zone entry, live since 2026-08-31)
        was found to be the engine's REAL active entry mechanic while
        porting this week's exit work -- a mismatch nothing this week's
        backtests ever covered, since they all assumed VWAP-retest timing.
        Direct user decision: make live match validated backtest exactly,
        rather than trust the exit mechanic works equally well paired with
        a different, never-backtested-together entry. _trap_check_entry/
        _immediate_check_entry are left in place, unused, not deleted --
        available again if a future decision reopens that door.

        self._vwap (screener.VwapState) is already fed every poll cycle
        (see the volume-confirmation block above, which feeds the SAME
        poll-to-poll volume delta into both the volume filter and VWAP) --
        this method only adds the actual entry DECISION, which nothing
        called before this pass despite the state already existing.

        Returns True the instant a genuine retest-entry fires (caller emits
        the Signal exactly as it already does for _trap_check_entry)."""
        vwap = self._vwap.current(sym)
        if vwap is None:
            return False
        armed = self._vwap_armed.get(sym, False)
        new_armed, fire = screener.check_vwap_retest_entry(side, ltp, vwap, armed, 0.15)
        self._vwap_armed[sym] = new_armed
        return fire

    async def _oi_swing_entry_scan(self, live, now: datetime, now_key: str) -> None:
        """entry_exit_mode="oi_swing_v1" ENTRY path (2026-09-18, direct user
        spec) -- immediate 2.0% price trigger vs real yesterday close, no
        VWAP-retest wait, no OI-regime gate, no trap-target-touched gate,
        no NIFTY regime dependency at all. Side locked from the trigger's
        own sign the instant it fires (oi_swing.check_immediate_entry_
        trigger, mirrors screener.side_from_pchange's CALL/PUT convention).

        Fix 2 (production, validated via the 13-day backtest sweep): no NEW
        entry may open after self._oi_swing_entry_cutoff (default 14:30) --
        the raw backtest had no cutoff at all, confirmed a real bug
        (PREMIERENE entered 15:28 on 2026-09-17, 13 minutes before its own
        15:15 EOD square-off)."""
        if not oi_swing.is_entry_within_cutoff(now.time(), self._oi_swing_entry_cutoff):
            return
        for sym in list(self._shortlist_symbols):
            if sym in self._positions or sym in self._pending_contracts:
                continue
            prev_close = self._prev_close_map.get(sym)
            if not prev_close:
                continue
            ltp = self._live_price(sym, live)
            if ltp is None:
                continue
            pchange = (ltp - prev_close) / prev_close * 100.0
            side = oi_swing.check_immediate_entry_trigger(pchange)
            if side is None:
                continue
            if (sym, side) in self._already_fired or (sym, side) in self._rejected:
                continue
            self._already_fired.add((sym, side))
            self._clog.info(
                "OiOrb[%s/%s]: %s OI-SWING-V1 2%% TRIGGER: pchange=%+.2f%% ltp=%.2f prev_close=%.2f "
                "-> side=%s -- entering immediately (no VWAP/OI-regime gate).",
                self._client_id, self._binding_id, sym, pchange, ltp, prev_close, side,
            )
            await self._emit_vwap_signal(
                sym, side, ltp, "oi_swing_v1_entry", (0.0, 0.0),
                now.strftime("%H:%M:%S"), label="OI-SWING-ENTRY")

    async def _seed_oi_swing_history(self, sym: str) -> None:
        """2026-09-18, direct user follow-up (real trade: ZYDUSLIFE) -- backfill
        the OI-swing series from REAL Upstox 1-min history (09:15->now) the
        instant a position is opened, instead of letting the tracker start
        cold from the entry tick. Mirrors _seed_vwap_from_upstox_intraday's
        own real-history-seed pattern in this same file (same eq-key/token
        resolution for the spot side); the futures-OI side reuses
        _resolve_futures_key_and_token, same as every other OI fetch in this
        class. Best-effort throughout: any failure just leaves the tracker
        to build live from zero (5-min bucket at a time), same behavior as
        before this fix existed -- never blocks the entry or the exit loop."""
        try:
            eq_key = stock_resolve.resolve_eq_instrument_key(sym)
            resolved = await self._resolve_futures_key_and_token(sym)
            if not eq_key or resolved is None:
                return
            fut_key, fut_token = resolved
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            eq_token = (creds or {}).get("access_token", "") or fut_token
            if not eq_token:
                return
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            price_rows = await fetch_upstox_intraday_1m(eq_key, eq_token)
            oi_rows = await fetch_upstox_intraday_1m(fut_key, fut_token)
            if not price_rows or not oi_rows:
                return

            def _minute(r: dict) -> datetime:
                ts = r["ts"]
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                return ts.astimezone(IST)

            oi_by_min: Dict[datetime, float] = {}
            for r in oi_rows:
                oi_val = r.get("oi")
                if oi_val:
                    oi_by_min[_minute(r).replace(second=0, microsecond=0)] = float(oi_val)
            if not oi_by_min:
                return

            # One 5-min bucket per completed window, taking the LAST price/OI
            # reading within it (rows are oldest-first -- later assignment to
            # the same bucket key naturally wins) -- exactly what the live
            # loop's own per-bucket recording would have produced.
            buckets: Dict[datetime, tuple] = {}
            for r in price_rows:
                minute = _minute(r).replace(second=0, microsecond=0)
                oi_val = oi_by_min.get(minute)
                if oi_val is None:
                    continue
                bkt = oi_swing.floor_to_bucket(_minute(r))
                buckets[bkt] = (float(r["close"]), oi_val)
            if not buckets:
                return

            series = [(bkt, px, oi) for bkt, (px, oi) in sorted(buckets.items())]
            swing_high: Optional[float] = None
            swing_low: Optional[float] = None
            for i in range(3, len(series) + 1):
                swing_high, swing_low, _confirmed = oi_swing.update_swing_state(
                    [p[2] for p in series[:i]], swing_high, swing_low)
            self._oi_swing_series[sym] = series
            self._oi_swing_high[sym] = swing_high
            self._oi_swing_low[sym] = swing_low
            self._oi_swing_last_bucket[sym] = series[-1][0]
            self._clog.info(
                "OiOrb[%s/%s]: %s OI-SWING seeded from %d real 5-min bars (09:15->now) -- "
                "swingH=%s swingL=%s.",
                self._client_id, self._binding_id, sym, len(series), swing_high, swing_low,
            )
        except Exception:
            self._clog.exception(
                "OiOrb[%s/%s]: %s OI-swing history seed failed (non-fatal -- tracker "
                "starts cold, same as before this fix existed).",
                self._client_id, self._binding_id, sym)

    async def _oi_swing_exit_check(self, sym: str, side: str, ltp: float, now: datetime) -> None:
        """entry_exit_mode="oi_swing_v1" EXIT path (2026-09-18, direct user
        spec) -- real 5-min Futures-OI + price series (REST-polled, session-
        anchored to 09:15), 3-point-immediate-neighbor swing confirmation,
        breakout/breakdown-vs-price-direction HOLD/EXIT matrix. See
        strategies/oi_orb_screener/oi_swing.py's own module docstring for
        the full mechanic and the two fixes this method applies (the third,
        the hard risk-cap cadence fix, lives in _option_tick_loop instead --
        this method only ever produces the "oi_swing_exit" reason, never
        the risk-cap one).

        RESTART/DEGRADE-SAFELY NOTE (explicit judgment call, flagged for
        review): the real-time 5-min OI/price series + swing ratchet are
        NOT persisted to store.py. A restart mid-session resumes this
        symbol's tracker from a genuinely empty series -- the position
        itself is unaffected (restored normally via _restore_from_db,
        including its real opened_at for the min-hold check below), it
        simply needs ~3 fresh live 5-min bars (about 10-15 minutes) after
        the restart before the OI-swing exit can arm again. During that
        window the position is NOT unprotected: the hard risk-cap
        (independent, tick-driven, re-armed automatically from pos[
        "entry_price"]/pos["qty"] with no separate state to lose) and EOD
        square-off both continue to apply exactly as before the restart.
        This mirrors the same "best-effort, never block, degrade safely"
        discipline every other REST-polled seed in this file already uses
        (e.g. _backfill_futures_oi_display) -- a full REST-replay-based
        restore (matching _seed_trap_exit_state's own pattern) was
        deliberately deferred, not attempted, for this first production
        pass.

        2026-09-18 real-trade fix (direct user follow-up on the very first
        live day): ZYDUSLIFE entered at 09:51 and closed at 09:59 (hard
        risk cap, since removed) without the OI-swing tracker ever
        confirming a single swing -- it needs 3 bars, but only had time
        for 2 (09:50, 09:55) before the position closed. The real 09:15-
        onward history for BOTH the price and the futures OI already
        existed the whole time (same real Upstox intraday data the VWAP
        seed / OI-regime 09:15 read already fetch elsewhere in this file)
        -- there is no reason to make a freshly-entered position start its
        swing tracker cold. See _seed_oi_swing_history below, now called
        once per symbol on this method's first invocation."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        if sym not in self._oi_swing_series:
            await self._seed_oi_swing_history(sym)
        bucket = oi_swing.floor_to_bucket(now)
        last_bucket = self._oi_swing_last_bucket.get(sym)
        if last_bucket is not None and bucket <= last_bucket:
            return   # still inside the same 5-min bucket -- nothing new to record yet
        resolved = await self._resolve_futures_key_and_token(sym)
        if resolved is None:
            return   # best-effort (no futures key/token this cycle) -- try again next cycle
        fut_key, token = resolved
        from data_layer.historical_candles import fetch_upstox_v3_quote
        try:
            quote = await fetch_upstox_v3_quote(fut_key, token)
        except Exception:
            self._clog.exception(
                "OiOrb[%s/%s]: %s OI-swing v3-quote fetch failed (recovered, try again next cycle).",
                self._client_id, self._binding_id, sym)
            return
        oi_now = (quote or {}).get("oi")
        if not oi_now or float(oi_now) <= 0:
            return
        self._oi_swing_last_bucket[sym] = bucket
        series = self._oi_swing_series.setdefault(sym, [])
        series.append((bucket, ltp, float(oi_now)))
        swing_high = self._oi_swing_high.get(sym)
        swing_low = self._oi_swing_low.get(sym)
        new_high, new_low, confirmed = oi_swing.update_swing_state(
            [p[2] for p in series], swing_high, swing_low)
        self._oi_swing_high[sym] = new_high
        self._oi_swing_low[sym] = new_low
        if confirmed:
            self._clog.info(
                "OiOrb[%s/%s]: %s OI-SWING %s CONFIRMED @ %s: %.0f",
                self._client_id, self._binding_id, sym, confirmed, bucket.strftime("%H:%M"),
                new_high if confirmed == "HIGH" else new_low,
            )
        if len(series) < 2:
            return
        price_prev, price_cur = series[-2][1], series[-1][1]
        broke, decision = oi_swing.check_oi_swing_breakout(
            side, series[-1][2], new_high, new_low, price_prev, price_cur)
        if not broke:
            return
        detail = (f"oi={series[-1][2]:.0f} swingH={new_high} swingL={new_low} "
                  f"price {price_prev:.2f}->{price_cur:.2f} decision={decision}")
        if decision != "EXIT":
            self._clog.info("OiOrb[%s/%s]: %s OI-SWING BREAKOUT -- HOLD (%s)",
                             self._client_id, self._binding_id, sym, detail)
            return
        # Fix 3 (production, validated via the 13-day backtest sweep): a
        # genuine "oi_swing_exit" decision is still suppressed inside the
        # minimum hold window since entry -- the hard risk cap and EOD
        # square-off are explicitly NOT subject to this and remain fully
        # active throughout.
        entry_ts = pos.get("opened_at")
        if not oi_swing.is_min_hold_satisfied(entry_ts, now, self._oi_swing_min_hold_min):
            self._clog.info(
                "OiOrb[%s/%s]: %s OI-SWING EXIT suppressed -- inside the %d-min minimum hold "
                "window since entry (%s). %s",
                self._client_id, self._binding_id, sym, self._oi_swing_min_hold_min,
                entry_ts.strftime("%H:%M:%S") if entry_ts else "unknown", detail,
            )
            return
        self._eod_closing.add(sym)
        self._clog.info("OiOrb[%s/%s]: %s OI-SWING EXIT -- %s",
                         self._client_id, self._binding_id, sym, detail)
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            "oi_swing_exit_triggered", side=side, detail=detail)
        await self._emit_close(sym, pos, "oi_swing_exit", detail=detail)

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
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        from strategies.core.trap_zone_utils import _collapse_nearby_zones
        from strategies.core.support_resistance import SupportResistanceCalculator

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

    def _immediate_check_entry(self, sym: str, side: str, ts: datetime) -> bool:
        """2026-09-02, opt-in alternate entry mode (immediate_entry_enabled,
        default False): skips the zone-detection + retest wait _trap_check_
        entry uses entirely -- fires the instant ORB has frozen for this
        symbol (self._orb_frozen already set by the poll loop above), no
        further confirmation. Real-data comparison (2026-09-02, both a
        simple ORB-extreme-as-SL proxy and this exact 15-min S1/R1 TSL
        simulated against real intraday bars for that day's actual
        shortlist) showed the zone/retest wait was costing genuine moves on
        fast movers -- by the time the ladder confirmed, the move was often
        already largely spent (see e.g. VOLTAS/KEI that day: entered late
        via trap_retest and lost, vs. an immediate-at-ORB-freeze entry that
        would have caught the same move early and profited). Single-day
        evidence only -- true historical backtesting isn't possible for
        this OI-based signal (Upstox's historical-candle API has no intraday
        OI field, same structural limitation OI-Flow already has), so this
        stays opt-in and should be watched over real forward days before
        trusting it broadly, same graduation discipline as every other
        strategy addition in this codebase.

        Returns True exactly once per (sym, side) -- caller's own
        self._already_fired set (same one _trap_check_entry's caller uses)
        prevents a re-fire on a later tick.

        2026-09-02, direct user spec: risk is a HYBRID, not either concept
        alone -- the INITIAL SL is the fixed ORB(09:15-09:25) extreme
        (self._orb_frozen[sym]), protecting the position immediately from
        the moment it enters (no warm-up gap at all, unlike the 3-min trap
        TSL which has none until its own ladder produces a first value).
        Once the parallel 15-min S1(long)/R1(short) ladder produces a
        genuine ESTABLISHED level, the stop RATCHETS to it if -- and only
        if -- that level is tighter (closer to price) than the ORB floor;
        it never loosens back past the ORB extreme. See
        _immediate_update_tsl_and_check_exit for the actual ratchet logic."""
        if sym in self._immediate_tsl_calc:
            return False   # already fired for this symbol this session
        if self._orb_frozen.get(sym) is None:
            return False   # ORB hasn't frozen for this symbol yet
        from strategies.core.support_resistance import SupportResistanceCalculator
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        self._immediate_tsl_calc[sym] = SupportResistanceCalculator()
        self._immediate_tsl_acc[sym] = _TrapAcc(timeframe_min=15)
        self._immediate_tsl_fed_bars[sym] = 0
        orb_h, orb_l = self._orb_frozen[sym]
        self._clog.info(
            "OiOrb[%s/%s]: %s IMMEDIATE ORB ENTRY (%s) -- skipping zone/retest wait, entering "
            "now that ORB has frozen; initial SL = ORB %s (09:15-09:25) = %.2f, will tighten to "
            "the 15-min S1/R1 ladder once it establishes a level closer than that.",
            self._client_id, self._binding_id, sym, side,
            "low" if side == "CALL" else "high", orb_l if side == "CALL" else orb_h,
        )
        return True

    async def _immediate_update_tsl_and_check_exit(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """Hybrid stop for an immediate_15m-tagged position (2026-09-02,
        direct user spec): starts at the fixed ORB(09:15-09:25) extreme
        (self._orb_frozen[sym] -- day-low for a CALL, day-high for a PUT),
        protecting the position from the instant it enters. In parallel, a
        15-min S1(CALL)/R1(PUT) ladder (same SupportResistanceCalculator
        mechanic and is_established gate as the 3-min trap TSL) builds up;
        the moment it produces a genuinely ESTABLISHED level, the effective
        stop RATCHETS to it if that level is tighter (closer to current
        price) than the ORB floor -- ratchet only, the ORB floor is never
        given back even if the 15-min level is somehow looser."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        orb_lvl = self._orb_frozen.get(sym)
        if orb_lvl is None:
            return   # defensive -- shouldn't happen, ORB must already be frozen to have entered
        orb_h, orb_l = orb_lvl
        sl_level = orb_l if side == "CALL" else orb_h

        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        acc = self._immediate_tsl_acc.setdefault(sym, _TrapAcc(timeframe_min=15))
        acc.on_tick(ts, ltp)
        calc = self._immediate_tsl_calc.get(sym)
        if calc is not None:
            fed = self._immediate_tsl_fed_bars.get(sym, 0)
            for b in acc.bars[fed:]:
                calc.process_straddle_candle(sym, {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": 15})
            self._immediate_tsl_fed_bars[sym] = len(acc.bars)

            sr = calc.get_calculated_sr_state(sym).get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                ladder_level = level["low"] if side == "CALL" else level["high"]
                # Ratchet only -- a CALL's stop only ever moves UP (tighter),
                # a PUT's stop only ever moves DOWN (tighter). Never looser
                # than the ORB floor either direction.
                if side == "CALL" and ladder_level > sl_level:
                    sl_level = ladder_level
                elif side == "PUT" and ladder_level < sl_level:
                    sl_level = ladder_level

        # 2026-09-03: surface the live effective stop to monitoring_state()'s
        # "sl" field -- was never written for trap/immediate_15m positions
        # (only the legacy vwap mechanic populated it), so the dashboard
        # showed "establishing..." even though real protection was active.
        self._live_sl[sym] = sl_level

        breach = (ltp <= sl_level) if side == "CALL" else (ltp >= sl_level)
        if not breach:
            return
        self._eod_closing.add(sym)
        self._clog.info(
            "OiOrb[%s/%s]: %s IMMEDIATE-MODE HYBRID SL HIT -- underlying_ltp=%.2f level=%.2f "
            "(orb_floor=%.2f) side=%s -- closing.",
            self._client_id, self._binding_id, sym, ltp, sl_level,
            orb_l if side == "CALL" else orb_h, side,
        )
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            "immediate_hybrid_sl_triggered",
            detail=f"underlying_ltp={ltp:.2f} sl_level={sl_level:.2f}")
        await self._emit_close(sym, pos, "immediate_hybrid_sl")

    async def _replay_trap_state(self, sym: str, side: str, tier: str, zones: list,
                                  bars_1m: list, entry_ts: datetime) -> bool:
        """2026-09-08, direct user spec: on a mid-day restart with a position
        already running, a zone may have ALREADY locked and been touched, and
        the 3-min ladder may already be well underway (or even already
        breached) -- this replays that real history bar-by-bar (using each
        1-min bar's own CLOSE as a synthetic tick, same tolerance every other
        replay/seed in this codebase already accepts) through the EXACT SAME
        `_trap_ladder_check` the live tick loop uses, so there is zero drift
        between "replayed" and "live" state -- not a parallel reimplementation.
        If the replay finds the level was already breached before this
        process ever came back up, closes the position for real, immediately
        (matching the DIXON-incident lesson: a restart must never silently
        leave a position that should already be closed sitting open).
        Returns True iff this call closed the position."""
        for b in [x for x in bars_1m if x.ts >= entry_ts]:
            zone = self._latest_locked_zone(zones, b.ts)
            if zone is None:
                continue
            if await self._trap_ladder_check(sym, side, b.close, b.ts, zone, tier, f"trap_{tier}_exit"):
                return True
        return False

    async def _fire_vwap_close_sl(self, sym: str, side: str, ltp: float, vwap: float,
                                   candle_bar=None) -> None:
        """Closes the position on a confirmed VWAP-close SL breach, and
        grants a ONE-TIME re-entry allowance for this (symbol, side) today
        (direct user spec, 2026-09-08: "I want re-entry allowed after a SL
        stopped out, but just once in that specific script for that day") --
        clears self._already_fired for this (symbol, side) exactly once per
        day via self._sl_reentry_used; a SECOND SL stop-out on the same
        (symbol, side) the same day does NOT grant another re-entry (already
        consumed). Target-hit and EOD exits are completely unaffected --
        this allowance is specific to the SL close path only.

        2026-09-16, direct user spec: `candle_bar` (the actual 20-min Bar
        whose close satisfied the adverse condition, when the caller has
        one) is used to build a human-readable detail string with the REAL
        candle/bucket time window + the exact close/VWAP values that fired
        the SL -- not just the live tick at the moment of closing, which
        made today's own debugging session (2026-09-15) need several rounds
        of manual log cross-referencing to reconstruct. Threaded into both
        signal_events and the positions table (via _emit_close's own
        detail param) so it's visible without a log dive next time."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        self._eod_closing.add(sym)
        if candle_bar is not None:
            bucket_end = candle_bar.ts + timedelta(minutes=_VWAP_SL_TF_MIN)
            detail = (f"candle=[{candle_bar.ts.strftime('%H:%M')}-{bucket_end.strftime('%H:%M')}) "
                      f"close={candle_bar.close:.2f} vwap={vwap:.2f} live_ltp={ltp:.2f}")
        else:
            detail = f"underlying_ltp={ltp:.2f} vwap={vwap:.2f}"
        self._clog.info(
            "OiOrb[%s/%s]: %s VWAP-CLOSE SL HIT -- %s side=%s -- closing.",
            self._client_id, self._binding_id, sym, detail, side,
        )
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            "vwap_close_sl_triggered", side=side, detail=detail)
        key = (sym, side)
        if key not in self._sl_reentry_used:
            self._sl_reentry_used.add(key)
            self._already_fired.discard(key)
            self._clog.info(
                "OiOrb[%s/%s]: %s %s one-time re-entry allowance granted for today (SL stop-out).",
                self._client_id, self._binding_id, sym, side)
        await self._emit_close(sym, pos, "vwap_close_sl", detail=detail)

    @staticmethod
    def _ha_vwap_close_sl_adverse(ha_bar, vwap: float, side: str) -> bool:
        """2026-09-16, direct user spec (reverses the 2026-09-10 plain-
        candle decision -- see _vwap_close_sl_check's own docstring):
        combined HA-shape + VWAP-gap adverse condition, BOTH must hold.

        Shape half reuses the SAME already-established/validated
        definition as ha_stoch_shape_exit_signal's own bearish-type/
        bullish-type check -- CALL: HA_high==HA_open (no upper wick at
        all); PUT: HA_low==HA_open (no lower wick). EXACT float equality
        is safe here, not a fragile rounding coincidence -- by
        construction, to_heikin_ashi's `ha_high = max(b.high, ha_open,
        ha_close)` returns ha_open completely unchanged (never computed
        via subtraction) whenever ha_open is the winning branch, same
        reasoning ha_stoch_shape_exit_signal's own docstring already
        states.

        Gap half is the original VWAP-relative test, unchanged in shape,
        just now evaluated against the HA candle's own close instead of a
        plain candle's close: CALL adverse if close sits >=_VWAP_SL_
        MIN_GAP_PCT below vwap; PUT mirrored above."""
        if vwap <= 0:
            return False
        if side == "CALL":
            return (ha_bar.high == ha_bar.open) and \
                   ((vwap - ha_bar.close) / vwap >= _VWAP_SL_MIN_GAP_PCT)
        return (ha_bar.low == ha_bar.open) and \
               ((ha_bar.close - vwap) / vwap >= _VWAP_SL_MIN_GAP_PCT)

    async def _vwap_close_sl_check(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """Live per-tick SL check (2026-09-08, direct user spec): a 20-min
        Heikin-Ashi candle both (a) shows the genuine no-wick adverse shape
        AND (b) closes on the wrong side of the running session VWAP by the
        gap threshold -- see _ha_vwap_close_sl_adverse's own docstring for
        the exact combined condition. Checked ahead of both trap-exit
        target tiers in the main exit loop (a real stop takes priority over
        a target/reversal read).

        History of the candle-type decision, both directions direct user
        spec: originally Heikin-Ashi. 2026-09-10: switched to PLAIN candles
        -- an earlier backtest that session found HA-vs-normal made no
        meaningful difference for the gap-only version of this check, kept
        plain for simplicity (explicitly NOT because HA itself was ever the
        bug -- the real 2026-09-10 bug was midnight-aligned vs market-open-
        anchored bucketing, see below, unrelated to candle type). 2026-09-16:
        switched BACK to Heikin-Ashi, this time combined with the shape
        condition above (a materially different test than the earlier
        gap-only HA version that was found equivalent to plain) -- direct
        user spec, using the SAME HA-shape definition already validated for
        ha_stoch_shape_exit_signal.

        2026-09-10, direct user spec/real incident fix (kept unchanged by
        the 2026-09-16 HA reversal -- this bug was about bucket alignment,
        not candle type): switched from midnight-aligned to
        market-open-anchored buckets (to_n_min_bars_market_anchored) --
        real trade ATHERENERG entered 09:25:11, SL-stopped 09:25:31 with
        ZERO real price movement in between, because the old midnight-
        aligned [09:00-09:20) bucket only ever held ~5 real minutes
        (09:15-09:20) but was already "confirmed closed" the instant
        wall-clock passed 09:20, comparing a stale 5-minute snapshot
        against a materially newer VWAP. Market-anchored buckets
        ([09:15-09:35), [09:35-09:55), ...) give the first bucket a genuine
        full 20 minutes before it's ever evaluated.
        Uses self._vwap.current(sym) -- the SAME
        running session VWAP the entry mechanic itself reads, already
        backfilled from real history and refreshed on every restart via
        _run_today_pipeline's own morning flow, so no separate VWAP seeding
        is needed here beyond what already exists.

        2026-09-11, direct user spec/real incident fix: added a guard against
        evaluating a bucket that closed BEFORE this position's own entry_ts
        (pos["opened_at"]) -- same species of bug as the ATHERENERG/FORCEMOT
        incidents above, just via a third path neither of those fixes
        covered. _seed_trap_exit_state resets the SL accumulator on every
        fresh entry (including a same-day re-entry) then seeds it in the
        background via _replay_vwap_close_sl with TODAY'S FULL-DAY bars, not
        just post-entry ones -- that replay itself correctly only checks
        buckets starting at/after entry_ts for an already-earned breach, but
        the accumulator it hands back to this live check still carries the
        whole day. A re-entry (or an entry landing mid-bucket, incl. the
        historical/immediate-fire path) would then have its FIRST live tick
        see "latest" as whatever bucket most recently closed -- which, for a
        late-in-bucket entry, is one that closed BEFORE this position even
        existed -- and fire an SL off it using TODAY'S CURRENT vwap, seconds
        after entry, with zero real post-entry price action involved. Real
        incidents this exact session: DIXON/ABCAPITAL/HDFCAMC/PRESTIGE/LODHA
        re-entries and a PIIND historical-fire entry all closed 3-20 seconds
        after opening. Skipping any bucket whose own close time is at/before
        entry_ts closes this off entirely -- mirrors the guard
        _replay_vwap_close_sl already has for the same reason."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        from strategies.core.candle_indicators import to_n_min_bars_market_anchored, to_heikin_ashi
        acc = self._sl_vwap_1m_acc.setdefault(sym, _TrapAcc(timeframe_min=1))
        acc.on_tick(ts, ltp)
        if not acc.bars:
            return
        # HA MUST be computed on the 1-min series first, then resampled up --
        # never on an already-aggregated bar (to_heikin_ashi's own docstring;
        # same rule ha_stoch_check_exit already follows).
        ha_1m = to_heikin_ashi(acc.bars)
        ha_tf = to_n_min_bars_market_anchored(ha_1m, _VWAP_SL_TF_MIN)
        if not ha_tf:
            return
        last_bar = ha_tf[-1]
        if ts < last_bar.ts + timedelta(minutes=_VWAP_SL_TF_MIN):
            ha_tf = ha_tf[:-1]   # still-forming bucket -- never evaluate early
        if not ha_tf:
            return
        latest = ha_tf[-1]
        if self._sl_vwap_last_checked_bar_ts.get(sym) == latest.ts:
            return
        self._sl_vwap_last_checked_bar_ts[sym] = latest.ts
        entry_ts = pos.get("opened_at")
        if entry_ts is not None and latest.ts + timedelta(minutes=_VWAP_SL_TF_MIN) <= entry_ts:
            return   # this bucket closed before this position existed -- stale, not a real SL
        vwap = self._vwap.current(sym)
        if vwap is None or vwap <= 0:
            return
        if not self._ha_vwap_close_sl_adverse(latest, vwap, side):
            return
        await self._fire_vwap_close_sl(sym, side, ltp, vwap, candle_bar=latest)

    async def _replay_vwap_close_sl(self, sym: str, side: str, today_bars: list, entry_ts: datetime,
                                     today_rows: Optional[list] = None) -> bool:
        """Restart-safety replay for the SL, same discipline as the trap-exit
        target mechanism's own _replay_trap_state: walks TODAY's real 1-min
        history (bucketed on the FULL day's bars first, never a sliced
        post-entry-only series -- that misaligns the first bucket) and fires
        a real close immediately if the SL condition was ALREADY genuinely
        satisfied before this process started/restarted. Seeds
        self._sl_vwap_1m_acc with today's real bars either way, so live
        ticks continue seamlessly regardless of whether a breach was found.
        Returns True iff this call closed the position.

        2026-09-10, real incident fix: switched to plain candles + market-
        anchored buckets, same reasoning as _vwap_close_sl_check. Also
        ADDED the still-forming-bucket guard this replay was previously
        missing entirely -- unlike the live check, this loop had no
        boundary check at all, so it could evaluate the CURRENTLY-forming
        last bucket (whatever ticks had arrived so far) as if it were a
        genuine closed candle. Combined with the old midnight-aligned
        bucketing this is exactly what fired the false ATHERENERG SL 19
        seconds after entry -- this replay runs immediately after every
        entry (via _seed_trap_exit_state), not only on a real restart.

        2026-09-10, SECOND real incident fix, same day: this used to compare
        EVERY historical bucket's close against self._vwap.current(sym) --
        today's single CURRENT/latest VWAP snapshot -- instead of the VWAP
        as it genuinely stood at that bucket's own close time. VWAP is
        cumulative and only grows more data as the day progresses, so
        "VWAP right now" is a materially different (and always more-final)
        number than "VWAP two hours ago." Real incident: FORCEMOT, entered
        10:54:41 with VWAP~=17796.52 at the time, was closed at 13:10:04 by
        this replay citing "close=17826.00 vs vwap=17862.01" -- but the live
        spot price the ENTIRE time around that restart was 18077-18078,
        comfortably ~1.3% ABOVE the real contemporaneous VWAP (17842-17866)
        -- nowhere near a genuine breach. The replay had found some earlier
        bucket that legitimately closed below the FINAL 17862.01 snapshot,
        even though VWAP was materially lower (closer to its 10:54 entry-
        time value) when that bucket itself actually closed, and used the
        wrong (future, too-high) VWAP to judge it -- a false SL that closed
        a real, healthy, and ultimately +Rs5,373.75 position for the wrong
        reason. Fixed by reconstructing a genuine minute-by-minute running
        VWAP from today_rows' real (HLC3, volume) pairs (same real-Upstox-
        intraday data the caller already fetched for this seed pass, just
        previously discarding the volume field when building the volume-
        less Bar objects) -- each bucket is now judged against the VWAP AS
        IT ACTUALLY STOOD at that bucket's own close, exactly matching what
        the live tick-driven check would have computed at that moment.
        Degrades safely to self._vwap.current(sym) (the old behavior) only
        if today_rows has no usable volume data for a given bucket."""
        if not today_bars:
            return False
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        from strategies.core.candle_indicators import to_n_min_bars_market_anchored, to_heikin_ashi
        # 2026-09-16: HA MUST mirror the live check exactly -- computed on
        # the 1-min series first, then resampled -- so this replay can never
        # drift from what the live tick-driven check would have found.
        ha_1m_full = to_heikin_ashi(today_bars)
        ha_tf = to_n_min_bars_market_anchored(ha_1m_full, _VWAP_SL_TF_MIN)
        if ha_tf:
            now = datetime.now(IST)
            last_bar = ha_tf[-1]
            if now < last_bar.ts + timedelta(minutes=_VWAP_SL_TF_MIN):
                ha_tf = ha_tf[:-1]   # still-forming bucket -- never evaluate early

        # Real minute-by-minute running VWAP, so each bucket is judged
        # against the VWAP as it genuinely stood at that bucket's own close.
        vwap_at_minute: Dict[datetime, float] = {}
        if today_rows:
            cum_pv = cum_v = 0.0
            for r in sorted(today_rows, key=lambda r: r["ts"]):
                vol = float(r.get("volume", 0) or 0)
                if vol <= 0:
                    continue
                typical = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3.0
                cum_pv += typical * vol
                cum_v += vol
                if cum_v > 0:
                    r_ts = datetime.fromisoformat(r["ts"]) if isinstance(r["ts"], str) else r["ts"]
                    vwap_at_minute[r_ts.replace(second=0, microsecond=0)] = cum_pv / cum_v
        sorted_minutes = sorted(vwap_at_minute.keys())

        def _vwap_as_of(bucket_end: datetime) -> Optional[float]:
            # bucket_end is the bucket's own EXCLUSIVE upper edge (e.g. bucket
            # [09:35,09:55) -> bucket_end=09:55) -- strict '<' so the very
            # first minute of the NEXT bucket never leaks into "vwap as of
            # THIS bucket's own close."
            eligible = [ts for ts in sorted_minutes if ts < bucket_end]
            if eligible:
                return vwap_at_minute[eligible[-1]]
            return self._vwap.current(sym)   # safe degrade -- no historical series available

        for hb in ha_tf:
            if hb.ts < entry_ts:
                continue
            vwap = _vwap_as_of(hb.ts + timedelta(minutes=_VWAP_SL_TF_MIN))
            if vwap is None or vwap <= 0:
                continue
            if self._ha_vwap_close_sl_adverse(hb, vwap, side):
                if sym not in self._positions or sym in self._eod_closing:
                    return False
                self._clog.critical(
                    "OiOrb[%s/%s]: %s restart-recovery replay found an ALREADY-EARNED VWAP-close "
                    "SL breach (HA %d-min close=%.2f vs vwap-as-of-then=%.2f, real history since "
                    "entry=%s) -- closing now.", self._client_id, self._binding_id, sym,
                    _VWAP_SL_TF_MIN, hb.close, vwap, entry_ts.isoformat())
                await self._fire_vwap_close_sl(sym, side, hb.close, vwap, candle_bar=hb)
                return True
        acc = _TrapAcc(timeframe_min=1)
        acc.bars = list(today_bars)
        self._sl_vwap_1m_acc[sym] = acc
        return False

    async def _seed_trap_exit_state(self, sym: str, side: str, entry_ts: datetime) -> None:
        """2026-09-08, direct user spec: seeds BOTH exit tiers from real
        history and replays any zone-touch/ladder progress that already
        happened -- fired on every fresh entry AND on every restart-restored
        open position (never just once cold).

        Two real gaps fixed here vs the first version of this method (direct
        user catch): (1) fetch_upstox_range_1m hits Upstox's HISTORICAL/
        completed-candle endpoint, which never includes today's own
        still-forming session -- so a mid-day restart's multi-day zone
        detection was silently missing today's own real 75-min bar(s)
        entirely. Fixed by ALSO fetching today via fetch_upstox_intraday_1m
        (the separate live-session endpoint every other same-day seed in this
        file already uses) and merging both into one real, gap-free series.
        (2) Neither tier ever replayed pre-restart history -- a restart with
        a position already running started both tiers stone cold at the
        restart tick, discarding any zone-touch/ladder progress (or even an
        already-earned breach) that happened before the process came back up.
        Fixed via _replay_trap_state, using the SAME live check function
        (_trap_ladder_check) real ticks use, so replay can never drift from
        live behavior.

        Best-effort throughout: no token/no data/network error/thin history
        just means that tier stays a no-op and the position relies on
        whichever tier (or EOD) still works -- never blocks entry, never
        leaves a position with zero protection (the intraday tier's own
        live-tick accumulation, wired in _trap_intraday_exit_check, keeps
        running from real ticks regardless of whether this seed ever
        completes)."""
        self._trap_exit_multiday_fetch_done[sym] = True
        try:
            eq_key = stock_resolve.resolve_eq_instrument_key(sym)
            if not eq_key:
                self._clog.warning(
                    "OiOrb[%s/%s]: %s trap-exit zone seed skipped -- no NSE_EQ instrument "
                    "key resolved; both tiers rely on live ticks only from here.",
                    self._client_id, self._binding_id, sym)
                return
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                self._clog.warning(
                    "OiOrb[%s/%s]: %s trap-exit zone seed skipped -- no Upstox access token; "
                    "both tiers rely on live ticks only from here.",
                    self._client_id, self._binding_id, sym)
                return
            from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m
            from strategies.core.trap_zone_utils import Bar as _Bar, BarAccumulator as _TrapAcc
            today = datetime.now(IST).date()
            start = today - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
            prior_rows = await fetch_upstox_range_1m(eq_key, token, start, today)
            today_rows = await fetch_upstox_intraday_1m(eq_key, token)
            all_rows = list(prior_rows or []) + list(today_rows or [])
            if not all_rows:
                self._clog.info(
                    "OiOrb[%s/%s]: %s trap-exit zone seed returned no bars -- both tiers "
                    "rely on live ticks only from here.", self._client_id, self._binding_id, sym)
                return
            seen_ts = set()
            bars = []
            for r in sorted(all_rows, key=lambda r: r["ts"]):
                if r["ts"] in seen_ts:
                    continue
                seen_ts.add(r["ts"])
                bars.append(_Bar(ts=datetime.fromisoformat(r["ts"]), open=float(r["open"]),
                                  high=float(r["high"]), low=float(r["low"]), close=float(r["close"])))
            today_bars = [b for b in bars if b.ts.date() == today]
            zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
            from strategies.core.candle_indicators import to_n_min_bars_dateaware

            # ---- Hard SL first (checked ahead of both target tiers -- a real
            # stop takes priority over a target/reversal read) ----
            if await self._replay_vwap_close_sl(sym, side, today_bars, entry_ts, today_rows):
                return   # position already closed via an already-earned SL breach

            # ---- Multi-day tier ----
            htf_multiday = to_n_min_bars_dateaware(bars, _TRAP_EXIT_HTF_MULTIDAY_MIN)
            if len(htf_multiday) >= 3:
                zones_all = zones_fn(htf_multiday)
                zones = self._drop_already_touched_zones(zones_all, bars, today)
                self._trap_exit_multiday_zones[sym] = zones
                if len(zones) != len(zones_all):
                    self._clog.info(
                        "OiOrb[%s/%s]: %s %d of %d multi-day zone(s) already touched by real price "
                        "on a PRIOR day -- treated as used up, dropped; only untouched zones remain "
                        "candidate.", self._client_id, self._binding_id, sym,
                        len(zones_all) - len(zones), len(zones_all))
                self._clog.info(
                    "OiOrb[%s/%s]: %s multi-day trap-exit zones seeded: %d zone(s) from %d real "
                    "75-min bars across %d calendar days (incl. %d real bars from today).",
                    self._client_id, self._binding_id, sym, len(zones), len(htf_multiday),
                    _TRAP_EXIT_LOOKBACK_CALENDAR_DAYS, len(today_bars))
                if zones and today_bars:
                    if await self._replay_trap_state(sym, side, "multiday", zones, today_bars, entry_ts):
                        return   # position already closed via replay -- nothing left to seed
            else:
                self._clog.info(
                    "OiOrb[%s/%s]: %s multi-day trap-exit HTF bars too thin (%d) -- "
                    "relies on the intraday tier.", self._client_id, self._binding_id, sym, len(htf_multiday))

            if sym in self._eod_closing or self._trap_exit_source.get(sym) == "multiday":
                return   # multiday tier already claimed this position (touched or closed)

            # ---- Intraday tier: pre-seed from real today's history instead of
            # starting the live accumulator genuinely blank at the seed/restart
            # instant -- direct user catch: "if application starts mid-day
            # again you need to get the intraday data as well". ----
            if today_bars:
                htf_intraday = to_n_min_bars_dateaware(today_bars, _TRAP_EXIT_HTF_INTRADAY_MIN)
                if len(htf_intraday) >= 3:
                    zones = zones_fn(htf_intraday)
                    self._trap_exit_intraday_zones[sym] = zones
                    acc = _TrapAcc(timeframe_min=1)
                    acc.bars = list(today_bars)   # continues seamlessly from here on real live ticks
                    self._trap_exit_intraday_1m_acc[sym] = acc
                    self._trap_exit_intraday_htf_fed[sym] = len(today_bars)
                    self._clog.info(
                        "OiOrb[%s/%s]: %s intraday trap-exit zones seeded from %d real bars today: "
                        "%d zone(s).", self._client_id, self._binding_id, sym, len(today_bars), len(zones))
                    if zones:
                        await self._replay_trap_state(sym, side, "intraday", zones, today_bars, entry_ts)
        except Exception:
            self._clog.exception(
                "OiOrb[%s/%s]: %s trap-exit zone seed failed -- both tiers rely on live "
                "ticks only from here.", self._client_id, self._binding_id, sym)

    @staticmethod
    def _side_from_option_type(option_type: str) -> str:
        """2026-09-10 CRITICAL FIX, real incident: an OPEN position's side
        (CALL/PUT) must come from the option it actually holds
        (contract.option_type -- CE/PE, fixed at entry, never changes), NOT
        be re-derived from screener.side_from_pchange(self._shortlist_
        pchange[sym]) -- the stock's CURRENT price-change-vs-previous-close
        sign, which can and does flip during the day, completely
        independent of which option was actually bought.

        Real incident: TECHM was entered as a genuine CE1540 (CALL) at
        09:39:59 while pChange was still positive. Real price then declined
        steadily all session (confirmed via real Upstox intraday data,
        1544.70 peak at 09:30 down to ~1515-1517 by early afternoon) until
        pChange vs previous close flipped negative sometime after entry.
        From that point on, every exit-check cycle re-derived side="PUT"
        for this CALL position -- log evidence: "TECHM trap_intraday_exit
        HIT -- underlying_ltp=1517.50 level=1517.50 side=PUT -- closing."
        That single mislabeling cascaded through everything: the wrong
        trap-zone type (sharp_bear_zones instead of bull_trap_zones), the
        wrong S&R level (R1 instead of S1), and the wrong breach direction
        (ltp>=level instead of ltp<=level) -- an entirely different
        (PUT-side) exit mechanic ran against this CALL position for the
        rest of the day. Independently confirmed: replaying the CORRECT
        (CALL-side) zone logic against real market data for the same
        session finds the target zone was NEVER genuinely touched at all."""
        return "CALL" if option_type == "CE" else "PUT"

    @staticmethod
    def _latest_locked_zone(zones: list, now: datetime) -> Optional[dict]:
        """Direct user correction (2026-09-08): only ever check the MOST
        RECENTLY locked zone as of `now`, not every zone confirmed anywhere
        across the whole lookback -- a stale zone from days ago shouldn't win
        a touch race against a fresher, more relevant one."""
        locked = [z for z in zones if z.get("lock_ts") is not None and z["lock_ts"] <= now]
        if not locked:
            return None
        return max(locked, key=lambda z: z["lock_ts"])

    @staticmethod
    def _drop_already_touched_zones(zones: list, bars: list, today: "date") -> list:
        """2026-09-10, direct user spec: a multi-day zone that real price
        already entered on any PRIOR calendar day (before today) is treated
        as "used up" -- no longer a valid candidate, even though it's still
        the most-recently-locked zone by timestamp. Direct user example:
        "zone on 8th... already touched on 9th... this zone will not be
        valid, will check previous zone before 8th." Filters the whole zone
        list down to only zones NEVER touched by a real 1-min close between
        their own lock_ts and the start of today -- _latest_locked_zone then
        naturally falls back to the next-older survivor, since it always
        just picks the most recent zone from whatever list it's given."""
        out = []
        for z in zones:
            lock_ts = z.get("lock_ts")
            if lock_ts is None:
                continue
            touched_before_today = any(
                lock_ts <= b.ts < datetime.combine(today, dtime.min, tzinfo=IST)
                and z["zone_lo"] <= b.close <= z["zone_hi"]
                for b in bars
            )
            if not touched_before_today:
                out.append(z)
        return out

    async def _check_trap_target_touched_today(self, sym: str, side: str) -> tuple:
        """2026-09-16, direct user spec: pre-ENTRY gate -- when a genuine
        VWAP-retest signal fires, check whether the 180-min multi-day trap
        target zone (the SAME zones_fn already used for the exit side) has
        ALREADY been touched by real price TODAY (market open -> now),
        BEFORE actually taking the trade. If it has, there's no real target
        left for the day -- skip this entry rather than take a trade whose
        own exit target is already spent.

        Distinct from _drop_already_touched_zones above, which only ever
        excludes zones touched on a PRIOR calendar day (used to pick a
        target AFTER a position is already open) -- this checks TODAY's own
        candles specifically, and runs BEFORE entry, not after.

        Returns (touched: bool, zone: Optional[dict]). Best-effort, same
        discipline as every other real-data seed in this file: any failure
        (no instrument key, no token, no data, too little multi-day history)
        returns (False, None) -- an auxiliary data hiccup must never block a
        real trade, same reasoning _seed_trap_exit_state's own docstring
        already states for the exit-side seed."""
        try:
            eq_key = stock_resolve.resolve_eq_instrument_key(sym)
            if not eq_key:
                return False, None
            from data_layer.client_db import ClientDB
            creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return False, None
            from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m
            from strategies.core.trap_zone_utils import Bar as _Bar
            from strategies.core.candle_indicators import to_n_min_bars_dateaware
            today = datetime.now(IST).date()
            start = today - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
            prior_rows = await fetch_upstox_range_1m(eq_key, token, start, today)
            today_rows = await fetch_upstox_intraday_1m(eq_key, token)
            all_rows = list(prior_rows or []) + list(today_rows or [])
            if not all_rows:
                return False, None
            seen_ts = set()
            bars = []
            for r in sorted(all_rows, key=lambda r: r["ts"]):
                if r["ts"] in seen_ts:
                    continue
                seen_ts.add(r["ts"])
                bars.append(_Bar(ts=datetime.fromisoformat(r["ts"]), open=float(r["open"]),
                                  high=float(r["high"]), low=float(r["low"]), close=float(r["close"])))
            htf = to_n_min_bars_dateaware(bars, _TRAP_EXIT_HTF_MULTIDAY_MIN)
            if len(htf) < 3:
                return False, None
            zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
            zones_all = zones_fn(htf)
            zone = self._latest_locked_zone(zones_all, datetime.now(IST))
            if zone is None:
                return False, None
            today_start = datetime.combine(today, dtime.min, tzinfo=IST)
            touched_today = any(
                zone["lock_ts"] <= b.ts and b.ts >= today_start
                and zone["zone_lo"] <= b.close <= zone["zone_hi"]
                for b in bars
            )
            return touched_today, zone
        except Exception:
            self._clog.warning(
                "OiOrb[%s/%s]: %s pre-entry trap-target touched-today check failed (non-fatal, "
                "treated as not-touched -- entry proceeds normally).",
                self._client_id, self._binding_id, sym, exc_info=True)
            return False, None

    async def _resolve_futures_key_and_token(self, sym: str) -> Optional[tuple]:
        """Resolves sym's own futures contract key + the dedicated "upstox2"
        feeder credential -- shared plumbing for both fixed-point OI fetches
        below. Returns None (with a WARNING log identifying exactly which
        precondition failed) on any failure, never silently.

        2026-09-16, direct user correction: the plain "upstox" provider row
        is the NIFTY/SENSEX account, shared with sell_straddle/iron_fly/
        cag_straddle -- and, confirmed live the same day, effectively also
        with this client's OWN execution-broker Upstox session (same real
        account, single-session-per-account enforced server-side by
        Upstox), so refreshing either one kept silently invalidating the
        other. "upstox2" (labeled CRUDEOIL in the admin feeder panel) is
        the user's own separate, dedicated account already set up
        specifically for sell_straddle + this OI-ORB screener."""
        from data_layer.instrument_registry import REGISTRY
        await asyncio.to_thread(REGISTRY.load_futures_only_sync, sym, datetime.now(IST).date())
        fut_key = REGISTRY.get_futures_upstox(sym)
        if not fut_key:
            self._clog.warning(
                "OiOrb[%s/%s]: %s futures-OI fetch -- no futures key resolved.",
                self._client_id, self._binding_id, sym)
            return None
        from data_layer.client_db import ClientDB
        creds = await asyncio.to_thread(ClientDB().get_feeder_creds_sync, "upstox2")
        token = (creds or {}).get("access_token", "")
        if not token:
            self._clog.warning(
                "OiOrb[%s/%s]: %s futures-OI fetch -- no upstox2 access_token stored "
                "in ClientDB (feeder_creds) for this client.",
                self._client_id, self._binding_id, sym)
            return None
        return (fut_key, token)

    async def _backfill_futures_oi_display(self, sym: str) -> None:
        """2026-09-16, real live incident: a process restart (or a position
        simply outliving the process that opened it) left an open
        position's futures_oi display data blank forever -- the OI-regime
        gate only ever populates self._today_0915_oi/_prev_day_last_tick_oi
        from the entry-loop's own fresh-evaluation path, which a symbol
        already sitting in self._positions never reaches again (it's
        skipped there by design). Called once per (open-position) symbol
        per position-management cycle; a no-op the moment both caches are
        already populated. Backfill is PURELY for display -- it never
        re-decides the side or affects the already-open position in any
        way, and any failure here is non-fatal (position/exits unaffected)."""
        if sym in self._today_0915_oi and sym in self._prev_day_last_tick_oi:
            return
        try:
            resolved = await self._resolve_futures_key_and_token(sym)
            if resolved is None:
                return
            fut_key, token = resolved
            from data_layer.historical_candles import (
                fetch_upstox_today_0915_oi, fetch_upstox_prev_day_last_tick_oi,
            )
            if sym not in self._today_0915_oi:
                oi = await fetch_upstox_today_0915_oi(fut_key, token)
                if oi is not None:
                    self._today_0915_oi[sym] = oi
            if sym not in self._prev_day_last_tick_oi:
                oi = await fetch_upstox_prev_day_last_tick_oi(fut_key, token)
                if oi is not None:
                    self._prev_day_last_tick_oi[sym] = oi
        except Exception:
            self._clog.warning(
                "OiOrb[%s/%s]: %s futures_oi display backfill failed (non-fatal, "
                "position/exits unaffected).",
                self._client_id, self._binding_id, sym, exc_info=True)

    async def _yesterday_candle_direction(self, fut_key: str, token: str) -> Optional[str]:
        """Returns "bullish" (close>open), "bearish" (close<open), or None
        (a doji -- open==close, no directional read possible -- or no
        daily-candle data at all). Shared by both the INCREASING and
        DECREASING branches of _compute_oi_regime_side below."""
        from data_layer.historical_candles import fetch_upstox_daily
        daily = await fetch_upstox_daily(fut_key, token, lookback_days=7)
        if not daily:
            return None
        last = daily[-1]
        prev_open, prev_close = float(last["open"]), float(last["close"])
        if prev_close > prev_open:
            return "bullish"
        if prev_close < prev_open:
            return "bearish"
        return None

    async def _compute_oi_regime_side(self, sym: str) -> Optional[str]:
        """2026-09-16, direct user spec, REVISED same day after independently
        verifying (against NSE's own real Bhavcopy) exactly what Upstox's
        various OI fields represent: the gate now compares two FIXED
        historical points, not a "live now" reading --

        OI_Change% = (today's own 09:15 futures OI - yesterday's own 15:39
        futures OI) / yesterday's own 15:39 futures OI * 100

        -- computed identically regardless of what time of day this symbol
        actually enters the shortlist (a stock added at 14:00 still gets
        its own real 09:15 reading, fetched by walking back into today's
        already-elapsed intraday history, never a "value right now").
        Evaluated once per (symbol, day) at/after OI_REGIME_CHECK_TIME, then
        cached forever for that symbol -- these two points never change
        once the day's 09:15 bar has printed, so there is deliberately no
        repeated re-fetch/refresh loop for this (unlike the superseded
        current-OI-vs-settled-close design this replaced).

        2026-09-16, second same-day REVISION, direct user spec: the NEUTRAL
        band is REMOVED entirely -- every stock is now either DECREASING or
        INCREASING, nothing is blocked purely for sitting "in the middle."
        ("CONFIRM PLEASE IMPLEMENT AND MAKE IT LIVE" -- explicit confirmation
        that this eliminates the previous -5%..+1% neutral band, verified
        against a worked example before implementing: every stock in that
        day's blocked panel, e.g. PAYTM -1.94%/PREMIERENE -1.09%/OFSS -0.63%,
        would flip from NEUTRAL/blocked to INCREASING/tradeable under this
        rule.)

        <= -5%  (DECREASING): direction starts from "today's trend", mapped
               to the stock's own live pChange sign (self._shortlist_pchange,
               the SAME convention screener.side_from_pchange already uses
               everywhere else in this codebase) -- a trade only fires if
               that direction is the OPPOSITE of yesterday's own candle (a
               genuine reversal confirmation), never a continuation of
               yesterday's own move (unchanged from the first 2026-09-16
               revision, only the boundary itself moved from "< -5%" to
               "<= -5%", i.e. exactly -5.00% now counts as DECREASING
               instead of falling into the now-removed NEUTRAL band).
               Worked example: today +2% pChange (would-be CALL) but
               yesterday was ALSO bullish -> BLOCKED (same direction as
               yesterday, not a reversal); today +2% pChange with yesterday
               BEARISH -> CALL fires (today reverses yesterday's move).
               Mirrored for a negative today: blocked against a bearish
               yesterday (continuation), fires PUT against a bullish
               yesterday (reversal). A doji yesterday (no directional read)
               blocks.
        > -5%  (INCREASING, everything else): direction comes ONLY from
               yesterday's candle (close>open -> CALL-only, close<open ->
               PUT-only) -- today's price action can time entry but never
               overrides this. A doji yesterday (no directional read) blocks.
               OI_REGIME_INCREASE_MIN_PCT (+1% default) is no longer
               consulted for this decision -- left in place as a config key
               only for backward-compat / any future re-introduction of a
               narrower band, not read by this method any more.

        Any None result here (a same-direction-as-yesterday block in the
        DECREASING branch, a doji in either branch, or a data failure) is
        treated identically by the caller (the entry loop): the symbol is
        removed from the pool entirely for the rest of the day, per direct
        spec ("that stock will come out of pool for whose day") -- one
        consistent rule for every "blocked today" reason, not a special
        case per cause.

        Best-effort but CONSERVATIVE, unlike most other real-data seeds in
        this file: this gate is a hard prerequisite for entry per direct
        spec, so any failure (no futures key, no token, no 09:15 bar yet,
        no prior-day bar, no daily candle) also returns None -- blocking
        the trade (and removing the symbol from the pool, same as a
        genuine NEUTRAL) -- rather than degrading to "let it proceed" the
        way purely auxiliary seeds (VWAP backfill, trap-exit zones) do."""
        try:
            resolved = await self._resolve_futures_key_and_token(sym)
            if resolved is None:
                return None
            fut_key, token = resolved

            from data_layer.historical_candles import (
                fetch_upstox_today_0915_oi, fetch_upstox_prev_day_last_tick_oi,
            )

            if sym not in self._today_0915_oi:
                oi = await fetch_upstox_today_0915_oi(fut_key, token)
                if oi is not None:
                    self._today_0915_oi[sym] = oi
            today_oi = self._today_0915_oi.get(sym)
            if today_oi is None:
                self._clog.warning(
                    "OiOrb[%s/%s]: %s OI-regime -- no 09:15 bar for today yet (key=%s).",
                    self._client_id, self._binding_id, sym, fut_key)
                return None

            if sym not in self._prev_day_last_tick_oi:
                oi = await fetch_upstox_prev_day_last_tick_oi(fut_key, token)
                if oi is not None:
                    self._prev_day_last_tick_oi[sym] = oi
            yday_oi = self._prev_day_last_tick_oi.get(sym)
            if not yday_oi:
                self._clog.warning(
                    "OiOrb[%s/%s]: %s OI-regime -- no prior trading day's 1-min data (key=%s).",
                    self._client_id, self._binding_id, sym, fut_key)
                return None

            oi_change_pct = (today_oi - yday_oi) / yday_oi * 100.0

            cfg = self._screener_cfg
            dec_max = float(cfg.get("OI_REGIME_DECREASE_MAX_PCT", _OI_REGIME_DECREASE_MAX_PCT_DEFAULT))

            # 2026-09-16, second same-day revision, direct user spec: NEUTRAL
            # band removed -- <=-5% is DECREASING (reversal-gated, unchanged
            # logic), everything else (>-5%) is INCREASING (yesterday's
            # candle direction only). See this method's own docstring.
            if oi_change_pct <= dec_max:
                regime = "DECREASING"
                pchange = self._shortlist_pchange.get(sym, 0.0)
                today_side = "CALL" if pchange > 0 else ("PUT" if pchange < 0 else None)
                if today_side is None:
                    side = None
                else:
                    yday_dir = await self._yesterday_candle_direction(fut_key, token)
                    # 2026-09-16, direct user follow-up: only a REVERSAL of
                    # yesterday's own candle qualifies -- today's direction
                    # continuing yesterday's own move is blocked outright,
                    # not just "the other side" -- doji (yday_dir is None)
                    # also blocks, same as INCREASING's own doji handling.
                    if yday_dir is None:
                        side = None
                    elif (yday_dir == "bullish" and today_side == "CALL") or \
                         (yday_dir == "bearish" and today_side == "PUT"):
                        side = None
                    else:
                        side = today_side
            else:
                regime = "INCREASING"
                yday_dir = await self._yesterday_candle_direction(fut_key, token)
                side = "CALL" if yday_dir == "bullish" else ("PUT" if yday_dir == "bearish" else None)

            self._clog.info(
                "OiOrb[%s/%s]: %s OI-REGIME -- today_0915_oi=%.0f yday_1539_oi=%.0f change=%+.2f%% "
                "regime=%s -> side=%s",
                self._client_id, self._binding_id, sym, today_oi, yday_oi, oi_change_pct,
                regime, side or "NONE (blocked)",
            )
            await asyncio.to_thread(
                store.log_signal_event, self._client_id, self._binding_id, sym,
                "oi_regime_computed", side=side or "",
                detail=f"oi_change_pct={oi_change_pct:+.2f}% regime={regime} "
                       f"today_0915_oi={today_oi:.0f} yday_1539_oi={yday_oi:.0f}")
            return side
        except Exception:
            self._clog.warning(
                "OiOrb[%s/%s]: %s OI-regime computation failed -- treated as BLOCKED (no trade "
                "today for this symbol, per direct spec -- conservative, unlike most other "
                "best-effort seeds in this file).",
                self._client_id, self._binding_id, sym, exc_info=True)
            return None

    async def _trap_ladder_check(self, sym: str, side: str, ltp: float, ts: datetime, zone: dict,
                                  tier: str, exit_reason: str) -> bool:
        """Shared 3-min S1(CALL)/R1(PUT) ladder, started fresh from the
        instant `zone` is first touched -- same SupportResistanceCalculator
        mechanic already validated live for the (currently unused)
        _trap_check_entry/_trap_update_tsl_and_check_exit pair, reused here
        for the exit side. Returns True if this call closed the position.

        CRITICAL FIX (2026-09-08, caught before this shipped, not after):
        self._trap_exit_source[sym] is claimed for `tier` ONLY the instant a
        genuine touch is confirmed here, never merely because a caller found
        a locked zone to check. An earlier version had callers claim source
        BEFORE calling this method, which meant the moment the multi-day tier
        found ANY locked zone -- even one price might never actually reach --
        it permanently blocked the intraday tier from ever running for that
        position, even though the multi-day zone might never get touched all
        day. That would have left a live position with real capital riding on
        it with effectively zero protection beyond the EOD fallback. Claiming
        source only at confirmed-touch time means an untouched, far-away
        multi-day zone can coexist with the intraday tier actively
        protecting the position in the meantime."""
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        from strategies.core.support_resistance import SupportResistanceCalculator

        if not self._trap_exit_touched.get(sym):
            touched = zone["zone_lo"] <= ltp <= zone["zone_hi"]
            if not touched:
                return False
            self._trap_exit_touched[sym] = True
            self._trap_exit_source[sym] = tier
            self._trap_exit_calc[sym] = SupportResistanceCalculator()
            self._trap_exit_ltf_acc[sym] = _TrapAcc(timeframe_min=_TRAP_EXIT_LTF_MIN)
            self._trap_exit_ltf_fed[sym] = 0
            self._clog.info(
                "OiOrb[%s/%s]: %s TRAP-EXIT zone touched [%.2f,%.2f] @ ltp=%.2f (source=%s) -- "
                "%d-min S&R ladder starting fresh.",
                self._client_id, self._binding_id, sym, zone["zone_lo"], zone["zone_hi"], ltp,
                tier, _TRAP_EXIT_LTF_MIN,
            )

        acc = self._trap_exit_ltf_acc[sym]
        acc.on_tick(ts, ltp)
        calc = self._trap_exit_calc[sym]
        fed = self._trap_exit_ltf_fed.get(sym, 0)
        for b in acc.bars[fed:]:
            calc.process_straddle_candle(sym, {"timestamp": b.ts, "high": b.high, "low": b.low,
                                                 "duration": _TRAP_EXIT_LTF_MIN})
        self._trap_exit_ltf_fed[sym] = len(acc.bars)

        sr = calc.get_calculated_sr_state(sym).get("sr_levels", {})
        level = sr.get("S1") if side == "CALL" else sr.get("R1")
        if level is None or not level.get("is_established"):
            return False   # cold/unconfirmed ladder -- hard risk cap still protects independently

        lvl = level["low"] if side == "CALL" else level["high"]
        self._live_sl[sym] = lvl
        breach = (ltp <= lvl) if side == "CALL" else (ltp >= lvl)
        if not breach:
            return False

        pos = self._positions.get(sym)
        if pos is None:
            return False
        self._eod_closing.add(sym)
        # 2026-09-16, direct user spec: candle/bucket time + values in the
        # exit history, not just the bare reason code -- this ladder is
        # tick-driven (not candle-close based like vwap_close_sl), so the
        # closest real "candle time" is the last fed LTF bar's own close
        # time; include it alongside the breach tick's own timestamp.
        last_bar_ts = acc.bars[-1].ts.strftime('%H:%M') if acc.bars else "n/a"
        detail = (f"breach_ts={ts.strftime('%H:%M:%S')} last_{_TRAP_EXIT_LTF_MIN}m_bar={last_bar_ts} "
                  f"underlying_ltp={ltp:.2f} level={lvl:.2f} zone=[{zone['zone_lo']:.2f},{zone['zone_hi']:.2f}]")
        self._clog.info(
            "OiOrb[%s/%s]: %s %s HIT -- %s side=%s -- closing.",
            self._client_id, self._binding_id, sym, exit_reason, detail, side,
        )
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            f"{exit_reason}_triggered", detail=detail)
        await self._emit_close(sym, pos, exit_reason, detail=detail)
        return True

    async def _trap_multiday_exit_check(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """Primary exit tier (2026-09-08, direct user spec, replaces
        HA+StochRSI as the live exit): multi-day 75min HTF same-side trap
        zone + 3min S&R ladder. Only ever acts once self._trap_exit_multiday_
        zones[sym] has real zones (seeded once by _seed_multiday_trap_exit_
        zones right after entry) -- if that seed never produced zones (no
        token, no data, thin history), this tier is permanently a no-op for
        this position and _trap_intraday_exit_check (called right after,
        every cycle) provides the real protection instead.

        2026-09-10 update: _trap_intraday_exit_check now gates on zones
        merely EXISTING, not on self._trap_exit_source -- see its own
        docstring for the reverted priority rule. self._trap_exit_source ==
        'intraday' can therefore no longer actually happen while multiday
        zones exist, but the guard below is left in place as a harmless
        defensive no-op rather than removed."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        if self._trap_exit_source.get(sym) == "intraday":
            return   # already escalated to the intraday tier -- never mix ladders mid-flight
        zones = self._trap_exit_multiday_zones.get(sym)
        if not zones:
            return
        zone = self._latest_locked_zone(zones, ts)
        if zone is None:
            return
        await self._trap_ladder_check(sym, side, ltp, ts, zone, "multiday", "trap_multiday_exit")

    async def _trap_intraday_exit_check(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """Secondary/fallback exit tier -- the already-validated intraday
        15min HTF same-side trap + 3min S&R ladder (scripts/oi_orb_trap_
        target_full_htf_ltf_sweep.py), built live from THIS SESSION's own
        ticks (single-day only, matching the backtest exactly).

        2026-09-10, direct user spec, REVERTS the 2026-09-08 "claim on touch,
        not on find" fix (see _trap_ladder_check's own docstring for that
        fix's original reasoning): intraday now only ever runs when the
        multiday tier has ZERO usable zones at all for this position --
        merely finding a real, genuinely-locked multiday zone blocks
        intraday for the rest of the day, even if that zone is never
        actually touched by price. Direct user framing: "if higher
        timeframe zone is found... it will not jump to the intraday trap
        concept only and only if the higher timeframe zone is not found."
        Traced against the real LODHA 2026-09-10 trade: 5 real multiday
        zones existed but only one was ever touched (11:44, 40min after
        LODHA had already exited via intraday at 11:04) -- under this
        reverted rule, LODHA would instead have sat with NO trap-ladder
        protection at all from entry (10:39) until 11:44 (or never, if that
        zone was never touched), relying solely on the VWAP-close SL and
        EOD square-off in the meantime. This tradeoff (favor honoring the
        higher-timeframe zone's structural priority vs. the 2026-09-08 fix's
        original "never leave a position unprotected" concern) was
        explicitly surfaced and accepted before this change, not an
        oversight."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return
        if self._trap_exit_multiday_zones.get(sym):
            return   # a real multiday zone exists -- intraday never runs, touched or not
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        acc = self._trap_exit_intraday_1m_acc.setdefault(sym, _TrapAcc(timeframe_min=1))
        acc.on_tick(ts, ltp)
        fed = self._trap_exit_intraday_htf_fed.get(sym, 0)
        if len(acc.bars) > fed:
            from strategies.core.candle_indicators import to_n_min_bars_dateaware
            htf_bars = to_n_min_bars_dateaware(acc.bars, _TRAP_EXIT_HTF_INTRADAY_MIN)
            if len(htf_bars) >= 3:
                zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
                try:
                    self._trap_exit_intraday_zones[sym] = zones_fn(htf_bars)
                except Exception:
                    self._clog.exception(
                        "OiOrb[%s/%s]: %s intraday trap-exit zone detection error (recovered).",
                        self._client_id, self._binding_id, sym)
            self._trap_exit_intraday_htf_fed[sym] = len(acc.bars)

        zones = self._trap_exit_intraday_zones.get(sym)
        if not zones:
            return
        zone = self._latest_locked_zone(zones, ts)
        if zone is None:
            return
        await self._trap_ladder_check(sym, side, ltp, ts, zone, "intraday", "trap_intraday_exit")

    async def _ha_stoch_check_exit(self, sym: str, side: str, ltp: float, ts: datetime) -> None:
        """2026-09-06, direct user spec: the confirmed exit mechanic from
        this week's real-data backtest series (scripts/oi_orb_ha_stochrsi_
        exit_backtest.py) -- entry and this exit are BOTH evaluated on the
        underlying's own SPOT price only, never on the option's own premium
        chart (direct user correction: "all entry and exit condition are on
        spot not on options. option is only activated when entry is
        triggered and it exit when exit is triggered in spot chart not in
        option chart"). This applies to EVERY open position regardless of
        which entry mechanic opened it (unlike the trap/immediate_15m TSLs,
        which only run for their own tagged positions) -- matches the
        backtest's own universal design.

        Mechanic: a fresh 1-min Heikin-Ashi series (Bar dataclass from
        strategies.core.trap_zone_utils, via strategies.core.candle_
        indicators.to_heikin_ashi -- HA MUST be computed on the 1-min series
        first, never on an already-aggregated 15-min bar, a different and
        wrong result) is resampled to 15-min bars. On the most recently
        FULLY CLOSED 15-min bar (never the still-forming one -- checked via
        its own end boundary against wall-clock `ts`, since to_n_min_bars'
        bucketing has no concept of "is this bucket done yet" on its own):
        CALL exits the instant that bar is "bearish-type" (HA_high==HA_open,
        no upper wick at all) AND 15-min StochRSI(9,9,3) has %D>=%K
        (inclusive cross); PUT mirrored (HA_low==HA_open AND %K>=%D). No SL
        beyond this -- EOD square-off remains the fallback if it never
        fires, exactly as backtested.

        `self._ha_stoch_last_checked_bar_ts[sym]` skips a bar this position
        has already been evaluated against once, so a losing condition
        isn't repeatedly logged/re-evaluated every poll cycle for the same
        already-passed bar (harmless either way since self._eod_closing
        would no-op a duplicate close, but avoids redundant recompute/log
        spam)."""
        pos = self._positions.get(sym)
        if pos is None or sym in self._eod_closing:
            return

        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
        from strategies.core.candle_indicators import (
            to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
        )

        acc = self._ha_stoch_1m_acc.setdefault(sym, _TrapAcc(timeframe_min=1))
        acc.on_tick(ts, ltp)
        closed_1m = acc.bars   # CLOSED 1-min bars only -- excludes the still-forming bucket
        if len(closed_1m) < 15:
            return   # not even one real 15-min bar's worth of closed 1-min data yet

        ha_1m = to_heikin_ashi(closed_1m)
        ha_15m = to_n_min_bars(ha_1m, 15)
        if not ha_15m:
            return

        # Only ever evaluate a 15-min bucket once its own 15-minute window has
        # genuinely elapsed in wall-clock time -- to_n_min_bars has no notion
        # of "complete", so the LAST bucket in its output can still be a
        # partially-formed bar (e.g. only 2 of 15 minutes closed so far).
        last_bar = ha_15m[-1]
        if ts < last_bar.ts + timedelta(minutes=15):
            ha_15m = ha_15m[:-1]
        if not ha_15m:
            return
        latest = ha_15m[-1]
        if self._ha_stoch_last_checked_bar_ts.get(sym) == latest.ts:
            return   # already evaluated this exact closed bar for this position
        self._ha_stoch_last_checked_bar_ts[sym] = latest.ts

        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, 9, 9, 3)
        if not ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            return

        self._eod_closing.add(sym)
        self._clog.info(
            "OiOrb[%s/%s]: %s HA+STOCHRSI EXIT (spot chart) -- underlying_ltp=%.2f "
            "ha15m_close=%.2f k=%.2f d=%.2f side=%s -- closing.",
            self._client_id, self._binding_id, sym, ltp, latest.close,
            k[-1] if k[-1] is not None else -1.0, d[-1] if d[-1] is not None else -1.0, side,
        )
        await asyncio.to_thread(
            store.log_signal_event, self._client_id, self._binding_id, sym,
            "ha_stochrsi_exit_triggered",
            detail=f"underlying_ltp={ltp:.2f} ha15m_close={latest.close:.2f} k={k[-1]} d={d[-1]}")
        await self._emit_close(sym, pos, "ha_stoch_exit")

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
        next tick.

        2026-09-01 CRITICAL FIX, real incident: a genuine R1/S1 value can
        exist within 1-2 three-minute bars of entry, but SupportResistance
        Calculator itself flags a freshly-seeded level `is_established:
        False` until a LATER bar actually confirms it as real structure --
        an unconfirmed level is essentially the extreme of the first couple
        bars since entry, not a real swing point, and stopped out two REAL
        live trades within 2-4 minutes of entry on ordinary noise (ITC:
        entered 13:33:11, TSL level {low:267.6,high:267.95} (0.35pts wide,
        is_established=False) hit at 13:37:30 on a 0.15% underlying
        pullback; ASHOKLEY: same shape, stopped in under 2 minutes). Same
        root-cause CLASS as the earlier OI-Flow fix (a bare single-touch
        swing pivot with no confirmation, fixed there via pool_swing_low's
        2+-touch requirement) -- here, simply requiring is_established
        before a level is eligible to trigger an exit is enough, since
        _check_hard_risk_cap's own independent Rs2000/lot backstop already
        protects every position on every option tick regardless of whether
        this structural TSL has armed yet -- a position is never genuinely
        naked while waiting for a level to establish."""
        from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc

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
        if not level.get("is_established"):
            return   # level exists but isn't confirmed yet -- hard risk cap still protects

        # 2026-09-03: surface the live effective stop to monitoring_state()'s
        # "sl" field -- was never written for trap/immediate_15m positions
        # (only the legacy vwap mechanic populated it), so the dashboard
        # showed "establishing..." even though real protection was active.
        self._live_sl[sym] = level["low"] if side == "CALL" else level["high"]

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
        before this change still reaches this method.

        2026-09-02: same bypass for "immediate_15m"-tagged positions -- that
        mechanic's own exit (_immediate_update_tsl_and_check_exit) is also
        driven from the poll loop, off the underlying's own price."""
        pos = self._positions.get(symbol)
        if pos is not None and pos.get("sl_mechanic") in ("trap", "immediate_15m"):
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

        # 2026-09-10, direct user spec: "backtest also doesn't use the option
        # sl and target, it used only spot sl and target, then why are we
        # checking for option sl and target" -- the universal spot-based exit
        # (_vwap_close_sl_check/_trap_multiday_exit_check/_trap_intraday_
        # exit_check, called unconditionally for every open position from the
        # main poll loop regardless of sl_mechanic) is the ONLY thing that was
        # ever actually validated against real backtests. This option-premium
        # check used to ALSO be able to independently close a "vwap"-tagged
        # (legacy/restored) position off its own option premium -- a second,
        # never-backtested exit path racing the correct spot-based one.
        # Removed the trigger; self._live_sl/self._live_target above are still
        # computed and shown in the UI's "Option SL/Target" field for
        # reference, but no longer close anything on their own.

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
                # 2026-09-02, direct user spec: an opt-in alternate entry mode
                # (immediate_entry_enabled) skips the zone/retest wait entirely
                # and enters the moment ORB freezes -- tagged "immediate_15m" so
                # its own 15-min S1/R1 TSL (_immediate_update_tsl_and_check_exit)
                # is used instead of the 3-min trap ladder. pending["reason"]
                # carries which entry path actually fired (set by whichever
                # signal-fire site created this fill).
                "sl_mechanic": (_ENTRY_EXIT_MODE_OI_SWING if pending.get("reason") == "oi_swing_v1_entry"
                                else "immediate_15m" if pending.get("reason") == "immediate_orb_entry"
                                else "vwap" if pending.get("reason") == "vwap_retest"
                                else "trap"),
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
            # 2026-09-18, direct user spec: a same-day re-entry on this
            # symbol under entry_exit_mode="oi_swing_v1" must not inherit a
            # stale OI-swing series/ratchet from an earlier trade today --
            # fresh position, fresh swing tracking, same discipline as
            # every other per-entry state reset in this block.
            self._oi_swing_series.pop(symbol, None)
            self._oi_swing_high.pop(symbol, None)
            self._oi_swing_low.pop(symbol, None)
            self._oi_swing_last_bucket.pop(symbol, None)
            self._ensure_spot_feed(symbol)
            # 2026-09-08, direct user spec: trap-exit state (multiday zones,
            # touch/ladder progress, intraday zone accumulator) must not
            # survive into a fresh entry on the same symbol -- a same-day
            # re-entry after an earlier exit on this symbol would otherwise
            # inherit a stale touched/escalated ladder from the previous
            # trade. Fresh multi-day zone fetch kicked off in the background
            # (asyncio.create_task, same fire-and-forget pattern already used
            # elsewhere in this engine) so a slow/failed fetch never delays
            # fill processing -- the intraday tier protects the position from
            # tick one regardless of when/whether the multiday seed lands.
            # 2026-09-10 real incident fix: use this fresh entry's own actual
            # contract side, not the stock's current pChange sign -- see
            # _side_from_option_type's own docstring for the full incident.
            side_for_zones = self._side_from_option_type(contract.option_type)
            self._trap_exit_multiday_zones.pop(symbol, None)
            self._trap_exit_multiday_fetch_done.pop(symbol, None)
            self._trap_exit_touched.pop(symbol, None)
            self._trap_exit_source.pop(symbol, None)
            self._trap_exit_calc.pop(symbol, None)
            self._trap_exit_ltf_acc.pop(symbol, None)
            self._trap_exit_ltf_fed.pop(symbol, None)
            self._trap_exit_intraday_1m_acc.pop(symbol, None)
            self._trap_exit_intraday_zones.pop(symbol, None)
            self._trap_exit_intraday_htf_fed.pop(symbol, None)
            # SL accumulator state resets per-entry too (NOT self._sl_reentry_used --
            # that must survive a re-entry to correctly block a second allowance).
            self._sl_vwap_1m_acc.pop(symbol, None)
            self._sl_vwap_last_checked_bar_ts.pop(symbol, None)
            # 2026-09-17: trap-zone exit tiers disabled (see the main exit
            # loop's own block comment) -- seeding them on every fresh
            # entry would just be wasted NSE/Upstox calls for state
            # nothing consumes.
            # asyncio.create_task(self._seed_trap_exit_state(symbol, side_for_zones, datetime.now(IST)))
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
            exit_detail = self._pending_close_details.pop(eid, "")
            if pos is not None:
                pnl = round((fill.fill_price - pos["entry_price"]) * pos["qty"], 2)
                self._clog.info("OiOrb[%s/%s]: EXIT CONFIRMED %s qty=%d @ %.2f (entry %.2f) P&L=%.2f",
                                 self._client_id, self._binding_id, symbol, pos["qty"],
                                 fill.fill_price, pos["entry_price"], pnl)
                await asyncio.to_thread(
                    store.close_position, self._client_id, self._binding_id, symbol,
                    fill.fill_price, exit_reason, pnl, exit_detail=exit_detail)

    # ── EOD square-off (the ONLY exit logic this pass) ──────────────────

    async def _emit_close(self, symbol: str, pos: dict, reason: str, detail: str = "") -> None:
        """2026-09-16, direct user spec: `detail` carries the WHY behind the
        exit in human-readable form (the real candle/bucket time + values
        that satisfied the condition, not just the bare reason code) --
        threaded through to store.close_position's exit_detail column so
        it's visible in the trade history, not just buried in the log."""
        contract = pos["contract"]
        exit_price = self._live_option_ltp.get(symbol, pos["entry_price"])
        event_id = f"{self._client_id}_{self._binding_id}_{symbol}_{reason}_{int(_time.time())}"
        order_ev = OiOrbOrderEvent(
            client_id=self._client_id, binding_id=self._binding_id, action="SELL",
            underlying=symbol, option_type=contract.option_type, strike=contract.strike,
            expiry=contract.expiry, quantity=pos["qty"], entry_price=pos["entry_price"],
            exit_price=exit_price, reason=reason, event_id=event_id,
            product_type=self._product_type, strategy=self._strategy_name,
            # 2026-09-10, real finding: entry_ts was only ever set on the BUY
            # event's own construction -- the SELL/close event never threaded
            # the original entry time forward, so every closed trade's
            # dashboard History row showed a blank entry TIME (oi_orb_bridge.py's
            # _record_history reads ev.entry_ts, which defaulted to None here).
            entry_ts=pos.get("opened_at"),
        )
        self._pending_closes[event_id] = reason
        self._pending_close_details[event_id] = detail
        self._clog.info("OiOrb[%s/%s]: closing %s qty=%d @ %.2f reason=%s%s",
                         self._client_id, self._binding_id, symbol, pos["qty"], exit_price, reason,
                         f" ({detail})" if detail else "")
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

    async def _spot_feed_retry_loop(self) -> None:
        """2026-09-10, real incident fix: _ensure_spot_feed's own
        self._spot_tick_subscribed[symbol] guard is set to True the instant
        it CALLS the feeder's register_extra_spot_keys(), not once a real
        tick has actually arrived -- so if that call happens to race ahead
        of the dedicated upstox2 feeder's WebSocket being fully connected
        yet (a genuine timing race at boot: this book's daily_loop and the
        feeder's own connection sequence start concurrently, with no
        ordering guarantee between them), the underlying subscribe can
        silently defer (see UpstoxFeeder.register_extra_spot_keys' own
        2026-09-10 fix) while this engine-level flag permanently believes
        it already succeeded -- meaning _ensure_spot_feed would NEVER be
        asked to retry for that symbol again, no matter how many feeder
        reconnects happen afterward.

        Real incident: TECHM/LODHA/OIL/ATHERENERG/FORCEMOT/DIXON/GVT&D all
        showed spot_ltp=null in the live API for the entire session after a
        restart, confirmed via direct /api/oiorb/status inspection -- even
        for positions entered fresh AFTER the restart (DIXON, GVT&D), which
        rules out this being restart-recovery-specific; it's a genuine race
        that can happen on ANY _ensure_spot_feed call.

        This loop periodically checks every symbol with a live open
        position or shortlist membership: if it was marked subscribed more
        than _SPOT_FEED_RETRY_GRACE_SEC ago but self._live_spot_ltp still
        has nothing for it, clear the subscribed flag and call
        _ensure_spot_feed again -- a genuine, safe retry (idempotent,
        subscribing an already-subscribed key is a no-op on the feeder
        side)."""
        while self._running:
            now = datetime.now(IST)
            candidates = set(self._shortlist_symbols) | set(self._positions.keys())
            for sym in candidates:
                subscribed_at = self._spot_feed_subscribed_at.get(sym)
                if subscribed_at is None:
                    continue
                if self._live_spot_ltp.get(sym) is not None:
                    continue
                if (now - subscribed_at).total_seconds() < _SPOT_FEED_RETRY_GRACE_SEC:
                    continue
                self._clog.warning(
                    "OiOrb[%s/%s]: %s subscribed %.0fs ago but still zero live spot ticks -- "
                    "retrying subscribe.", self._client_id, self._binding_id, sym,
                    (now - subscribed_at).total_seconds())
                self._spot_tick_subscribed.pop(sym, None)
                self._ensure_spot_feed(sym)
            await asyncio.sleep(_SPOT_FEED_RETRY_POLL_SEC)

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
            # 2026-09-10 real incident fix: this open position's own actual
            # side for display, not the stock's current/possibly-flipped
            # pChange sign -- see _side_from_option_type's own docstring.
            # This exact field is what showed "side":"PUT" for TECHM (a real
            # CE position) in the live dashboard API response.
            side = self._side_from_option_type(p["contract"].option_type)
            vwap = self._vwap.current(sym)
            spot_ltp = self._live_spot_ltp.get(sym)
            vwap_gap_pct = (round((spot_ltp - vwap) / vwap * 100.0, 3)
                             if (vwap and spot_ltp is not None) else None)
            # 2026-09-16, direct user spec, REVISED: the two FIXED points the
            # regime gate actually compares (today's own 09:15 OI, yesterday's
            # own 15:39 OI) -- plain, directly visible on the position card.
            _t0915 = self._today_0915_oi.get(sym)
            _y1539 = self._prev_day_last_tick_oi.get(sym)
            futures_oi = ({"today_0915_oi": _t0915, "yday_1539_oi": _y1539,
                           "oi_change_pct": round((_t0915 - _y1539) / _y1539 * 100.0, 2)}
                          if (_t0915 is not None and _y1539) else None)
            positions[sym] = {
                "option_type": p["contract"].option_type,
                "strike": p["contract"].strike,
                "expiry": p["contract"].expiry.isoformat() if hasattr(p["contract"].expiry, "isoformat")
                          else p["contract"].expiry,
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
                "spot_ltp": spot_ltp,
                "sl": self._live_sl.get(sym),
                "target": self._live_target.get(sym),
                # 2026-09-09, direct user spec: "have all values necessary in ui
                # position section to understand what is happening" -- the SL/
                # target mechanic is now spot-VWAP/trap-zone based (not the
                # option-premium levels above), so surface those directly too:
                # is the spot currently on the adverse or favorable side of
                # VWAP (the live SL's own reference), which trap tier (multiday
                # 180min / intraday 15min) claimed this position's target if
                # any, that zone's own [lo, hi], and whether the one-time
                # re-entry-after-SL has already been used for this symbol.
                "side": side,
                "vwap": round(vwap, 2) if vwap is not None else None,
                "vwap_gap_pct": vwap_gap_pct,
                "trap_source": self._trap_exit_source.get(sym),
                "trap_zone_touched": self._trap_exit_touched.get(sym, False),
                "sl_reentry_used": (sym, side) in self._sl_reentry_used,
                "sl_mechanic": p.get("sl_mechanic"),
                "futures_oi": futures_oi,
                # 2026-09-18, direct user spec: entry_exit_mode="oi_swing_v1"
                # own live state -- the latest CONFIRMED swing high/low, how
                # many real 5-min bars have been recorded so far today, and
                # whether the position is still inside its own minimum-hold
                # window (see oi_swing.py's own module docstring).
                "oi_swing": ({
                    "swing_high": self._oi_swing_high.get(sym),
                    "swing_low": self._oi_swing_low.get(sym),
                    "bars_recorded": len(self._oi_swing_series.get(sym, [])),
                    "min_hold_satisfied": oi_swing.is_min_hold_satisfied(
                        opened_at, datetime.now(IST), self._oi_swing_min_hold_min),
                } if p.get("sl_mechanic") == _ENTRY_EXIT_MODE_OI_SWING else None),
            }
        # 2026-09-07, direct user spec: "when stocks are scanned the ui should
        # show how far is ltp from vwap as we have already subscribed to all
        # the stocks after 9.25 when they got scanned" -- self._vwap tracks a
        # running VWAP per symbol from the same subscribed feed, no new
        # subscription needed.
        #
        # 2026-09-07 real incident fix: originally read self._live_spot_ltp
        # directly, which is ONLY the raw upstox2 tick -- whenever that tick
        # had gone stale/quiet (confirmed live: real WATCH heartbeat log
        # lines showed a valid LTP the whole time, but this panel showed
        # "VWAP --" for the same symbols at the same moment), this dict alone
        # was empty/stale even though a real, currently-displayed price
        # existed via _live_price()'s own NSE-poll fallback. self._last_known_price
        # is the exact value the WATCH heartbeat itself already computed and
        # trusts (tick-primary, poll-fallback already resolved), so reading
        # it here instead keeps this panel consistent with what the log
        # already shows, rather than re-deriving a stricter, tick-only value.
        shortlist_vwap = {}
        for sym in self._shortlist_symbols:
            ltp = self._last_known_price.get(sym)
            vwap = self._vwap.current(sym)
            dist = round(ltp - vwap, 2) if (ltp is not None and vwap) else None
            dist_pct = round((ltp - vwap) / vwap * 100.0, 2) if (ltp is not None and vwap) else None
            _t0915 = self._today_0915_oi.get(sym)
            _y1539 = self._prev_day_last_tick_oi.get(sym)
            shortlist_vwap[sym] = {
                "ltp": ltp, "vwap": round(vwap, 2) if vwap is not None else None,
                "vwap_dist": dist, "vwap_dist_pct": dist_pct,
                "futures_oi": ({"today_0915_oi": _t0915, "yday_1539_oi": _y1539,
                                "oi_change_pct": round((_t0915 - _y1539) / _y1539 * 100.0, 2)}
                               if (_t0915 is not None and _y1539) else None),
            }
        # Standard variant's own arm/retest state shows via self._vwap_armed
        # elsewhere -- retest_trackers stays present (empty) for frontend
        # backwards-compatibility.
        retest_trackers: dict = {}
        return {
            "client_id": self._client_id,
            "binding_id": self._binding_id,
            "strategy_name": self._strategy_name,
            "today": self._today.isoformat() if self._today else None,
            "shortlist": self._shortlist_symbols,
            "shortlist_pchange": self._shortlist_pchange,
            "shortlist_vwap": shortlist_vwap,
            "retest_trackers": retest_trackers,
            "regime": self._regime,
            "orb_frozen": self._orb_frozen,
            "positions": positions,
            # 2026-09-16, direct user spec: stocks that passed the 2%
            # price-move filter (step 1) but got blocked/removed by the
            # futures-OI-regime gate (step 2) -- see _record_oi_regime_
            # blocked's own docstring. Kept even after the symbol leaves
            # self._shortlist_symbols, so the UI can show WHY it didn't
            # proceed instead of it just vanishing.
            "oi_regime_blocked": self._oi_regime_blocked,
            # 2026-09-18, direct user spec: standalone top gainer/loser data
            # pipeline's own last-poll snapshot -- verify-only, purely for
            # visibility/manual cross-check against real NSE numbers; never
            # consumed by any entry/exit decision anywhere in this file.
            "top_gainer_loser": {
                "all": self._top_gainer_loser_all,
                "qualifying": self._top_gainer_loser_qualifying,
            },
        }
