"""
strategies/oi_orb_screener/book_manager.py — lifecycle manager for
OiOrbScreenerStrategy books.

One OiOrbScreenerStrategy per (client, binding) with a running
"oi_orb_screener" deployment -- underlying is always the sentinel
"SCREENER" (mirrors D1 Trap FnO's own WATCHLIST sentinel: the real stocks
traded are chosen dynamically each day by the screener itself, not fixed
at deployment time). strategy_params JSON configures the screener
thresholds per book (see _DEFAULT_PARAMS).

Fresh, independent implementation -- reads its own "oi_orb_screener"-tagged
deployment rows only, never touches or is touched by any other strategy's
manager, per strategies/oi_orb_screener/__init__.py's standalone design.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core.book_manager import StrategyBookManager
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy, _UNDERLYING_SENTINEL

logger = logging.getLogger(__name__)

_STRATEGY_NAME = "oi_orb_screener"
_DEFAULT_PARAMS = {
    "oi_spurt_min_pct": 7.0,
    "price_move_min_pct": 2.0,
    "stock_move_abort_pct": 4.0,
    "top_n_per_side": 5,
    "poll_seconds": 20,
    # 2026-08-24, direct user request -- TEMPORARY connectivity-test toggle.
    # Default False. See screener.py's own CONFIG["IGNORE_TIME_WINDOWS"]
    # docstring for what this actually does and when to turn it back off.
    "ignore_time_windows": False,
    # 2026-08-25, direct user request: expose every remaining screener.CONFIG
    # tunable per-deployment too (admin panel, ui_layer/dashboard_server.py's
    # /api/admin/oiorb/config/{deploy_id}), not just the original 7. SCORE_WEIGHTS
    # deliberately excluded -- a 4-way dict, not a scalar knob, lower priority.
    "nifty_bullish_pct": 0.3,
    "nifty_bearish_pct": -0.3,
    "max_monitor_minutes": 90,
    "rejection_min_rise_pct": 2.0,
    "rejection_retrace_fraction": 0.5,
    "strike_otm_pct": 0.0,  # 2026-09-06: ATM (was 2% OTM) -- direct user correction
    "orb_start": "09:15",
    "orb_end": "09:25",
    # 2026-08-27, direct user spec: TWO scan sessions. Session 1 is a single
    # point-in-time scan at scan_start (09:26) -- whichever stocks qualify AT
    # THAT MOMENT get added, no further morning scanning. Session 2 re-runs
    # the scan periodically between afternoon_scan_start (12:00) and
    # afternoon_scan_end -- 2026-09-01, direct user spec: raised from 13:00 to
    # 15:00 to match entry_window_end below (no reason to stop rescanning
    # while entries can still fire), ADDING any newly-qualifying stock. No
    # scanning happens outside these two windows. Both default ON.
    "scan_start": "09:26",
    # 2026-09-07, direct user spec, REVERSES the 2026-08-27 spec above: "stocks
    # which got scanned at 9.25 will be considered for complete day, no need
    # to scan fresh stocks after 9.25am." Default flipped to False; still
    # opt-in per deployment via strategy_params if ever revisited.
    "two_session_scan_enabled": False,
    "afternoon_scan_start": "12:00",
    "afternoon_scan_end": "15:00",
    "afternoon_scan_interval_sec": 300.0,
    "entry_window_start": "09:26",
    # direct user spec: "if that stock does not hit vwap till 15.00 it will
    # get cancelled" -- both sessions' candidates share this cutoff (was
    # 10:30). No new entries fire and no further scanning after this time;
    # a position already running is unaffected -- it only closes at EOD
    # square-off, target, or SL.
    #
    # 2026-09-17 CRITICAL FIX, real gap found via a direct log check: this
    # dict is COMPLETELY SEPARATE from screener.py's own CONFIG default --
    # screener.CONFIG["ENTRY_WINDOW_END"] was already tightened to "13:30"
    # earlier today (real 7-day backtest evidence: entries firing after
    # 13:30 netted only +3.99 pts across 7 real trades vs +204.20 pts across
    # the 12 real trades at/before 13:30; separately, 32 of 35 real stocks
    # checked had their single biggest intraday range in the 09:15-11:30
    # morning session, ZERO in the afternoon), but book_manager.py's own
    # _parse_params() always passes value["entry_window_end"] from THIS
    # dict, never reading screener.CONFIG at all -- confirmed live: the
    # actual running deployment's reconcile log still showed
    # 'entry_window_end': '15:00' hours after the screener.py fix was
    # deployed and restarted. Updated here to match -- this is the value
    # that actually governs the live engine.
    "entry_window_end": "13:30",
    # 2026-08-25, direct user spec: five additive, independently-toggleable
    # filters (see strategies/oi_orb_screener/filters.py's module docstring
    # for the real incident -- a SAIL CALL breakout fired right under a
    # large Call-OI wall). Each *_enabled flag ONLY controls whether that
    # filter can actually block a trade -- every filter always evaluates
    # and logs to its own dedicated file regardless. Default OFF (log-only)
    # until real forward telemetry earns a promotion to a real gate.
    "oi_wall_check_enabled": False,
    "oi_wall_dominance_ratio": 1.5,
    "distance_to_wall_enabled": False,
    "distance_to_wall_min_pct": 1.5,
    "pcr_gate_enabled": False,
    "pcr_max_for_call": 1.2,
    "pcr_min_for_put": 0.8,
    "volume_confirmation_enabled": False,
    "volume_confirmation_min_ratio": 1.5,
    "oi_roc_enabled": False,
    "oi_roc_min_pct": 3.0,
    "oi_roc_lookback_sec": 300.0,
    # 2026-08-25, direct user spec: the WS feed subscription is a single
    # SHARED budget across every strategy in this app (~50 symbols/broker
    # connection) -- watching every shortlisted stock's full option chain
    # unbounded could silently starve ticks for a completely different
    # strategy. Cap chain-watching (the 3 chain-dependent filters: oi_wall/
    # distance_to_wall/pcr) to the top N shortlisted stocks by rank.
    # 2026-08-27: OI-ORB now gets its own DEDICATED upstox2 feeder
    # (run_system.py's `bus._oiorb_feeder`, separate WS connection from every
    # other strategy) -- a real live trade (KOTAKBANK) proved the old cap=2
    # directly starved the OI-wall/distance-to-wall/PCR filters of real data
    # for any stock outside the top-2 rank. 2026-09-23 CORRECTION, real
    # production incident: the prior "10" default's own reasoning ("watching
    # more stocks no longer risks starving a different strategy's shared
    # feed, now that OI-ORB has its own dedicated connection") was wrong in
    # practice -- the DEDICATED connection itself overflowed (confirmed live,
    # "101 symbols subscribed -- EXCEEDS the ~50/connection WS limit") at 5
    # shortlisted stocks x depth=2's 10 contracts/stock = 50, right at the
    # limit with zero margin. Lowered to 4 (40 symbols, real headroom),
    # direct user instruction -- every shortlisted stock still gets full
    # option_native scoring/selection persisted regardless of this cap; it
    # only limits the separate (currently dead-filter-only) wide chain watch.
    "chain_watch_max_stocks": 4,
    "vwap_sl_tf_minutes": 5,
    # 2026-08-27, direct user spec: SL/target now track the OPTION's own
    # premium ("checking for target and SL in stock, change it to the
    # option which we are taking"), not the underlying stock's spot price.
    # rr_multiple is a fixed risk-reward target off the currently-armed
    # SL's own points distance from entry -- fresh, unvalidated default.
    "rr_multiple": 2.0,
    # 2026-09-18, direct user spec: standalone "top gainer/loser" data
    # pipeline (verify-only this pass, see screener.poll_top_gainers_
    # losers's own docstring) -- steps 3-4's two dynamic thresholds.
    "top_gainer_loser_oi_spurt_min_pct": 7.0,
    "top_gainer_loser_pchange_max_pct": 4.0,
    # 2026-09-18, direct user follow-up: "DISABLE THIS AS OFF NOW" -- Step 4
    # starts OFF; Step 3 (OI-spurt floor) alone decides qualification.
    "top_gainer_loser_pchange_filter_enabled": False,
    # 2026-09-22, direct user spec: option-contract-native entry/exit
    # mechanic (see strategies/oi_orb_screener/option_native.py's own
    # module docstring for the full 4-layer design, and the approved plan
    # at C:\Users\SERVER\.claude\plans\immutable-popping-sphinx.md for the
    # 14 frozen decisions these defaults encode). Additive/opt-in --
    # default OFF, same discipline as every other tunable/filter in this
    # package. When enabled, this mechanic's own Layer 4 exit fully
    # REPLACES the universal 20-min VWAP-close SL for any position it
    # opens; the existing shortlist pipeline (top-gainer/loser, OI-Spurt)
    # is completely unchanged and still decides WHICH stocks this mechanic
    # ever gets to look at.
    # 2026-09-23, direct user instruction: default flipped to True -- was
    # additive opt-in (default False) per the original rollout plan; this
    # now activates for EVERY oi_orb_screener deployment whose own
    # strategy_params doesn't explicitly override it, not just SA5770.
    "option_native_enabled": True,
    "min_score": 6,                 # decision 2 -- minimum 8-point score to trade
    "min_score_gap": 2,             # decision 3 -- CE-vs-PE winning margin, both required together
    "max_spread_pct": 1.0,          # decision 5 -- (ask-bid)/mid*100 "tight spread" score threshold
    "delta_ce_min": 0.45,
    "delta_ce_max": 0.65,
    "delta_pe_min": -0.65,
    "delta_pe_max": -0.45,
    "option_native_poll_seconds": 300,   # REST option-chain re-poll cadence for bid/ask/iv/delta
}
_FLOAT_KEYS = ("oi_spurt_min_pct", "price_move_min_pct", "stock_move_abort_pct",
               "nifty_bullish_pct", "nifty_bearish_pct", "rejection_min_rise_pct",
               "rejection_retrace_fraction", "strike_otm_pct",
               "oi_wall_dominance_ratio", "distance_to_wall_min_pct",
               "pcr_max_for_call", "pcr_min_for_put", "volume_confirmation_min_ratio",
               "oi_roc_min_pct", "oi_roc_lookback_sec",
               "afternoon_scan_interval_sec", "rr_multiple",
               "top_gainer_loser_oi_spurt_min_pct", "top_gainer_loser_pchange_max_pct",
               "max_spread_pct", "delta_ce_min", "delta_ce_max", "delta_pe_min", "delta_pe_max")
_INT_KEYS = ("top_n_per_side", "poll_seconds", "max_monitor_minutes",
             "chain_watch_max_stocks", "vwap_sl_tf_minutes",
             "min_score", "min_score_gap", "option_native_poll_seconds")
_STR_KEYS = ("orb_start", "orb_end", "scan_start", "entry_window_start", "entry_window_end",
             "afternoon_scan_start", "afternoon_scan_end")
_FILTER_BOOL_KEYS = ("oi_wall_check_enabled", "distance_to_wall_enabled", "pcr_gate_enabled",
                      "volume_confirmation_enabled", "oi_roc_enabled")


def _parse_params(raw: str, defaults: dict = _DEFAULT_PARAMS) -> dict:
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}
    for k, v in defaults.items():
        params.setdefault(k, v)
    return params


class OiOrbScreenerBookManager(StrategyBookManager):
    STRATEGY_NAME = _STRATEGY_NAME
    DEFAULT_PARAMS = _DEFAULT_PARAMS

    def _wanted(self) -> Dict[tuple, dict]:
        wanted: Dict[tuple, dict] = {}
        rows = self._db.get_running_deployments_by_strategy_sync(self.STRATEGY_NAME)
        for d in rows or []:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            if not cid or not bid:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            _defaults = self.DEFAULT_PARAMS
            params = _parse_params(d.get("strategy_params", "{}"), _defaults)
            cfg = {
                "lots": lots,
                "product_type": d.get("product_type") or "MIS",
                "squareoff_time": d.get("squareoff_time") or "15:15",
            }
            for k in _FLOAT_KEYS:
                cfg[k] = float(params.get(k, _defaults[k]))
            for k in _INT_KEYS:
                cfg[k] = int(params.get(k, _defaults[k]))
            for k in _STR_KEYS:
                cfg[k] = str(params.get(k, _defaults[k]))
            cfg["ignore_time_windows"] = bool(params.get("ignore_time_windows",
                                                           _defaults["ignore_time_windows"]))
            cfg["two_session_scan_enabled"] = bool(params.get("two_session_scan_enabled",
                                                                _defaults["two_session_scan_enabled"]))
            for k in _FILTER_BOOL_KEYS:
                cfg[k] = bool(params.get(k, _defaults[k]))
            cfg["top_gainer_loser_pchange_filter_enabled"] = bool(params.get(
                "top_gainer_loser_pchange_filter_enabled",
                _defaults["top_gainer_loser_pchange_filter_enabled"]))
            cfg["option_native_enabled"] = bool(params.get(
                "option_native_enabled", _defaults["option_native_enabled"]))
            # Key on the sentinel underlying so this fits the base class's
            # generic (client_id, binding_id, underlying) Key shape without
            # a real per-stock underlying -- the screener itself decides
            # which real stocks to trade each day, inside the one book.
            wanted[(cid, bid, _UNDERLYING_SENTINEL)] = cfg
        return wanted

    def _spawn_book(self, key: tuple, value: dict) -> OiOrbScreenerStrategy:
        client_id, binding_id, _underlying = key
        book = OiOrbScreenerStrategy(
            self._bus, self._cfg, client_id, binding_id,
            lot_multiplier=value["lots"],
            product_type=value["product_type"],
            squareoff_time=value["squareoff_time"],
            oi_spurt_min_pct=value["oi_spurt_min_pct"],
            price_move_min_pct=value["price_move_min_pct"],
            stock_move_abort_pct=value["stock_move_abort_pct"],
            top_n_per_side=value["top_n_per_side"],
            poll_seconds=value["poll_seconds"],
            ignore_time_windows=value["ignore_time_windows"],
            nifty_bullish_pct=value["nifty_bullish_pct"],
            nifty_bearish_pct=value["nifty_bearish_pct"],
            max_monitor_minutes=value["max_monitor_minutes"],
            rejection_min_rise_pct=value["rejection_min_rise_pct"],
            rejection_retrace_fraction=value["rejection_retrace_fraction"],
            strike_otm_pct=value["strike_otm_pct"],
            orb_start=value["orb_start"],
            orb_end=value["orb_end"],
            scan_start=value["scan_start"],
            entry_window_start=value["entry_window_start"],
            entry_window_end=value["entry_window_end"],
            two_session_scan_enabled=value["two_session_scan_enabled"],
            afternoon_scan_start=value["afternoon_scan_start"],
            afternoon_scan_end=value["afternoon_scan_end"],
            afternoon_scan_interval_sec=value["afternoon_scan_interval_sec"],
            oi_wall_check_enabled=value["oi_wall_check_enabled"],
            oi_wall_dominance_ratio=value["oi_wall_dominance_ratio"],
            distance_to_wall_enabled=value["distance_to_wall_enabled"],
            distance_to_wall_min_pct=value["distance_to_wall_min_pct"],
            pcr_gate_enabled=value["pcr_gate_enabled"],
            pcr_max_for_call=value["pcr_max_for_call"],
            pcr_min_for_put=value["pcr_min_for_put"],
            volume_confirmation_enabled=value["volume_confirmation_enabled"],
            volume_confirmation_min_ratio=value["volume_confirmation_min_ratio"],
            oi_roc_enabled=value["oi_roc_enabled"],
            oi_roc_min_pct=value["oi_roc_min_pct"],
            oi_roc_lookback_sec=value["oi_roc_lookback_sec"],
            chain_watch_max_stocks=value["chain_watch_max_stocks"],
            vwap_sl_tf_minutes=value["vwap_sl_tf_minutes"],
            rr_multiple=value["rr_multiple"],
            top_gainer_loser_oi_spurt_min_pct=value["top_gainer_loser_oi_spurt_min_pct"],
            top_gainer_loser_pchange_max_pct=value["top_gainer_loser_pchange_max_pct"],
            top_gainer_loser_pchange_filter_enabled=value["top_gainer_loser_pchange_filter_enabled"],
            option_native_enabled=value["option_native_enabled"],
            min_score=value["min_score"],
            min_score_gap=value["min_score_gap"],
            max_spread_pct=value["max_spread_pct"],
            delta_ce_min=value["delta_ce_min"],
            delta_ce_max=value["delta_ce_max"],
            delta_pe_min=value["delta_pe_min"],
            delta_pe_max=value["delta_pe_max"],
            option_native_poll_seconds=value["option_native_poll_seconds"],
            strategy_name=self.STRATEGY_NAME,
        )
        logger.info(
            "OiOrbScreenerBookManager[%s]: spawned %s/%s (lots=%d oi_spurt>=%.1f%% price_move>=%.1f%% "
            "top_n=%d ignore_time_windows=%s).",
            self.STRATEGY_NAME, client_id, binding_id, value["lots"], value["oi_spurt_min_pct"],
            value["price_move_min_pct"], value["top_n_per_side"],
            value["ignore_time_windows"],
        )
        if value["ignore_time_windows"]:
            logger.warning(
                "OiOrbScreenerBookManager: %s/%s spawned with ignore_time_windows=True -- "
                "TEMPORARY connectivity-test mode, real ORB/entry-window timing is bypassed. "
                "Turn this back off (strategy_params) once connectivity is confirmed.",
                client_id, binding_id,
            )
        return book

    def _is_flat(self, book: OiOrbScreenerStrategy) -> bool:
        """Overrides the base class's single-`_position` check -- this book
        can hold several concurrent stock positions in self._positions."""
        return not book._positions

    def _should_respawn(self, book: OiOrbScreenerStrategy, value: dict) -> bool:
        if book._lot_multiplier != value["lots"]:
            return True
        return (
            book._product_type != value["product_type"]
            or book._screener_cfg["OI_SPURT_MIN_PCT"] != value["oi_spurt_min_pct"]
            or book._screener_cfg["PRICE_MOVE_MIN_PCT"] != value["price_move_min_pct"]
            or book._screener_cfg["STOCK_MOVE_ABORT_PCT"] != value["stock_move_abort_pct"]
            or book._screener_cfg["TOP_N_PER_SIDE"] != value["top_n_per_side"]
            or book._screener_cfg["POLL_SECONDS"] != value["poll_seconds"]
            or book._screener_cfg["IGNORE_TIME_WINDOWS"] != value["ignore_time_windows"]
            or book._screener_cfg["NIFTY_BULLISH_PCT"] != value["nifty_bullish_pct"]
            or book._screener_cfg["NIFTY_BEARISH_PCT"] != value["nifty_bearish_pct"]
            or book._screener_cfg["MAX_MONITOR_MINUTES"] != value["max_monitor_minutes"]
            or book._screener_cfg["REJECTION_MIN_RISE_PCT"] != value["rejection_min_rise_pct"]
            or book._screener_cfg["REJECTION_RETRACE_FRACTION"] != value["rejection_retrace_fraction"]
            or book._screener_cfg["STRIKE_OTM_PCT"] != value["strike_otm_pct"]
            or book._screener_cfg["ORB_START"] != value["orb_start"]
            or book._screener_cfg["ORB_END"] != value["orb_end"]
            or book._screener_cfg["SCAN_START"] != value["scan_start"]
            or book._screener_cfg["ENTRY_WINDOW_START"] != value["entry_window_start"]
            or book._screener_cfg["ENTRY_WINDOW_END"] != value["entry_window_end"]
            or any(book._filters_cfg[k] != value[k] for k in (
                "oi_wall_check_enabled", "oi_wall_dominance_ratio",
                "distance_to_wall_enabled", "distance_to_wall_min_pct",
                "pcr_gate_enabled", "pcr_max_for_call", "pcr_min_for_put",
                "volume_confirmation_enabled", "volume_confirmation_min_ratio",
                "oi_roc_enabled", "oi_roc_min_pct", "oi_roc_lookback_sec",
            ))
            or book._chain_watch_max_stocks != value["chain_watch_max_stocks"]
            or book._vwap_sl_tf_minutes != value["vwap_sl_tf_minutes"]
            or book._rr_multiple != value["rr_multiple"]
            or book._screener_cfg["TWO_SESSION_SCAN_ENABLED"] != value["two_session_scan_enabled"]
            or book._screener_cfg["AFTERNOON_SCAN_START"] != value["afternoon_scan_start"]
            or book._screener_cfg["AFTERNOON_SCAN_END"] != value["afternoon_scan_end"]
            or book._screener_cfg["AFTERNOON_SCAN_INTERVAL_SEC"] != value["afternoon_scan_interval_sec"]
            or book._screener_cfg["TOP_GAINER_LOSER_OI_SPURT_MIN_PCT"] != value["top_gainer_loser_oi_spurt_min_pct"]
            or book._screener_cfg["TOP_GAINER_LOSER_PCHANGE_MAX_PCT"] != value["top_gainer_loser_pchange_max_pct"]
            or book._screener_cfg["TOP_GAINER_LOSER_PCHANGE_FILTER_ENABLED"] != value["top_gainer_loser_pchange_filter_enabled"]
            or book._option_native_enabled != value["option_native_enabled"]
            or book._native_min_score != value["min_score"]
            or book._native_min_score_gap != value["min_score_gap"]
            or book._native_max_spread_pct != value["max_spread_pct"]
            or book._native_delta_band["CE"][0] != value["delta_ce_min"]
            or book._native_delta_band["CE"][1] != value["delta_ce_max"]
            or book._native_delta_band["PE"][0] != value["delta_pe_min"]
            or book._native_delta_band["PE"][1] != value["delta_pe_max"]
            or book._native_poll_seconds != value["option_native_poll_seconds"]
        )

    def _log_spawned(self, key: tuple, value: dict) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile spawned %s -> %s", key, value)

    def _log_stopped(self, key: tuple) -> None:
        logger.info("OiOrbScreenerBookManager: reconcile stopped %s", key)
