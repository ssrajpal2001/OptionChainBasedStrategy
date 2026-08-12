"""
strategies/d1_trap_option/book_manager.py — lifecycle manager for Trap Scanner books.

Handles two strategy names:
  "d1_trap_index"  — Indices (NIFTY/SENSEX/BANKNIFTY), intraday MIS, configurable HTF/MTF
  "d1_trap_fno"    — FnO stocks (RELIANCE etc.), positional NRML, D1 HTF / 1H MTF

One D1TrapOptionBook per (client, binding, underlying) with is_running=1.
strategy_params JSON in the deployment row configures htf/mtf/ltf/itm per book.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

_STRATEGY_NAMES = {"d1_trap_index", "d1_trap_fno", "d1_trap_option", "d1_trap_bear_only", "d1_trap_sr",
                    "d1_trap_fno_sr"}

_D1TrapOptionBook = None  # lazy-imported
_D1TrapBearOnlyBook = None  # lazy-imported
_D1TrapSRBook = None  # lazy-imported
_D1TrapFnOSRBook = None  # lazy-imported


def _parse_params(raw: str, strategy_name: str) -> dict:
    """Parse strategy_params JSON with per-strategy defaults."""
    try:
        params = json.loads(raw or "{}")
    except Exception:
        params = {}

    if strategy_name == "d1_trap_fno":
        defaults = {"htf": "D1", "mtf": "75min", "ltf": "5min", "itm": 1, "top_n": 5}
    elif strategy_name == "d1_trap_sr":
        # 2026-08-08: S&R ping-pong mechanic (strategies/d1_trap_option/sr_book.py),
        # validated for BANKNIFTY (see CLAUDE.md's D1 Trap BearTrap section). "htf"/
        # "itm" here reuse d1_trap_bear_only's own key names/values so a deployment
        # row can carry both without ambiguity; sr_tf/exit_mode are S&R-specific.
        # 2026-08-12: strike_mode/execute_strike_mode -- opt-in fixed-period strike
        # (prev month/week high-low anchor) + daily-1-ITM real execution, both
        # default OFF (byte-identical to prior behavior unless explicitly set).
        defaults = {"sr_tf": 3, "exit_mode": "raw", "strike_mode": "daily_atm",
                    "execute_strike_mode": "same"}
    elif strategy_name == "d1_trap_fno_sr":
        # 2026-08-09: positional S&R ping-pong for FnO stocks
        # (strategies/d1_trap_option/fno_sr_book.py) -- daily bars only (validated
        # best swing tf via scripts/fno_positional_sr_backtest.py), 1-ITM option
        # selection, day-low/day-high TSL. "itm" here means ITM STEPS (matches
        # d1_trap_fno's own convention), not points.
        defaults = {"itm": 1, "hard_risk_pct": 0.10}
    else:
        defaults = {"htf": "75min", "mtf": "15min", "ltf": "5min", "itm": 1}

    for k, v in defaults.items():
        params.setdefault(k, v)
    return params


class D1TrapOptionBookManager(StrategyBookManager):

    def _wanted(self) -> Dict[tuple, dict]:
        """Return {(cid, bid, underlying): book_config_dict} for all running trap deployments."""
        wanted: Dict[tuple, dict] = {}
        for strategy_name in _STRATEGY_NAMES:
            rows = self._db.get_running_deployments_by_strategy_sync(strategy_name)
            for d in rows or []:
                cid = d.get("client_id", "")
                bid = d.get("binding_id", "")
                underlying = str(d.get("underlying") or "").upper()
                if not cid or not bid or not underlying:
                    continue
                try:
                    lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
                except Exception:
                    lots = 1
                params = _parse_params(d.get("strategy_params", "{}"), strategy_name)
                product = d.get("product_type") or (
                    "NRML" if strategy_name in ("d1_trap_fno", "d1_trap_fno_sr") else "MIS"
                )
                from strategies.d1_trap_option.bear_only_book import (
                    _ITM_OFFSET_DEFAULT_BY_UNDERLYING, _HTF_MINUTES_DEFAULT_BY_UNDERLYING,
                )
                cfg = {
                    "lots": lots,
                    "strategy_name": strategy_name,
                    "htf_tf": params.get("htf", "75min"),
                    "mtf_tf": params.get("mtf", "15min"),
                    "itm_offset": int(params.get("itm", 1)),
                    "itm_offset_pts": int(params.get(
                        "itm_offset_pts", _ITM_OFFSET_DEFAULT_BY_UNDERLYING.get(underlying, 200))),
                    "htf_minutes": int(params.get(
                        "htf_minutes", _HTF_MINUTES_DEFAULT_BY_UNDERLYING.get(underlying, 60))),
                    "sr_tf_minutes": int(params.get("sr_tf", 3)),
                    "exit_mode": params.get("exit_mode", "raw"),
                    "strike_mode": params.get("strike_mode", "daily_atm"),
                    "execute_strike_mode": params.get("execute_strike_mode", "same"),
                    "hard_risk_pct": float(params.get("hard_risk_pct", 0.10)),
                    "product_type": product,
                    # 2026-08-04: carry_forward is independent of product_type -- see
                    # client_db.py's DDL comment. Defaults to same-day close (0).
                    "carry_forward": bool(int(d.get("carry_forward", 0) or 0)),
                    "squareoff_time": d.get("squareoff_time") or "15:15",
                }

                # ALL_FNO sentinel: one deployment record → all stocks in FNO_STOCK_CONFIG.
                if underlying == "ALL_FNO" and strategy_name == "d1_trap_fno":
                    try:
                        from config.global_config import FNO_STOCK_CONFIG
                        for stock_sym in FNO_STOCK_CONFIG:
                            wanted[(cid, bid, stock_sym.upper())] = cfg.copy()
                    except Exception:
                        logger.warning("TrapBookManager: FNO_STOCK_CONFIG not available")
                    continue

                # WATCHLIST sentinel: reads data/fno_watchlist.json (written by nightly scan).
                # top_n stocks (default 5, override via strategy_params {"top_n": N}) are
                # subscribed — only APPROACHING status entries, sorted by btst_rr descending.
                if underlying == "WATCHLIST" and strategy_name in ("d1_trap_fno", "d1_trap_fno_sr"):
                    try:
                        import json as _json
                        from pathlib import Path as _Path
                        wl_path = _Path(__file__).resolve().parents[2] / "data" / "fno_watchlist.json"
                        with open(wl_path) as _f:
                            wl = _json.load(_f)
                        # Hard cap: WATCHLIST always spawns at most top_n books.
                        # The file is pre-sorted by the scan (APPROACHING first, by btst_rr),
                        # so taking stocks[:top_n] gives the best picks regardless of status.
                        top_n = min(int(params.get("top_n", 5)), 5)  # never exceed 5
                        stocks = wl.get("stocks", [])
                        for entry in stocks[:top_n]:
                            sym = entry.get("symbol", "").upper()
                            if sym:
                                book_cfg = cfg.copy()
                                # Thread upstox_key / lot / step from watchlist JSON
                                # so WATCHLIST stocks not in FNO_STOCK_CONFIG get correct
                                # instrument key, lot size, and strike step.
                                if entry.get("upstox_key"):
                                    book_cfg["upstox_key"] = entry["upstox_key"]
                                if entry.get("lot", 0) > 0:
                                    book_cfg["lot_override"] = int(entry["lot"])
                                if entry.get("step", 0) > 0:
                                    book_cfg["step_override"] = int(entry["step"])
                                wanted[(cid, bid, sym)] = book_cfg
                        logger.info(
                            "TrapBookManager: WATCHLIST loaded %d stocks (top_n=%d) from %s",
                            min(top_n, len(stocks)), top_n, wl_path,
                        )
                    except FileNotFoundError:
                        logger.warning(
                            "TrapBookManager: data/fno_watchlist.json not found — "
                            "run backtest/fno_scanner/scan_live.py --save first"
                        )
                    except Exception as exc:
                        logger.warning("TrapBookManager: WATCHLIST load failed: %s", exc)
                    continue

                wanted[(cid, bid, underlying)] = cfg
        return wanted

    def _should_respawn(self, book, value) -> bool:
        """Re-spawn when lot_multiplier changes while the book is flat."""
        return getattr(book, "_lot_multiplier", None) != value.get("lots", 1)

    def _reconcile(self) -> None:
        """Run base reconcile then retry equity feed registration for any FnO books
        whose GlobalFeeder was not yet ready at spawn time (gf was None)."""
        super()._reconcile()
        self._retry_fno_equity_registration()

    def _retry_fno_equity_registration(self) -> None:
        """After each reconcile, ensure all live d1_trap_fno / d1_trap_fno_sr books
        have their equity symbol subscribed on the GlobalFeeder. Safe to call
        repeatedly — idempotent."""
        gf = getattr(self._bus, "_global_feeder", None)
        if gf is None:
            return
        if not hasattr(gf, "register_extra_spot_keys"):
            return
        for key, book in list(self._books.items()):
            if getattr(book, "_strategy_name", "") not in ("d1_trap_fno", "d1_trap_fno_sr"):
                continue
            upstox_key = getattr(book, "_upstox_key_override", "")
            underlying  = book._underlying
            if not upstox_key:
                from config.global_config import FNO_STOCK_CONFIG
                stock = FNO_STOCK_CONFIG.get(underlying.upper()) or {}
                upstox_key = stock.get("upstox_key", "")
            if upstox_key and not getattr(book, "_equity_feed_registered", False):
                try:
                    gf.register_extra_spot_keys({upstox_key: underlying})
                    book._equity_feed_registered = True
                    logger.info(
                        "TrapBookManager: equity feed registered for %s (%s)",
                        underlying, upstox_key,
                    )
                except Exception as exc:
                    logger.debug("TrapBookManager: equity feed retry failed for %s: %s", underlying, exc)

    def _spawn_book(self, key, cfg):
        global _D1TrapOptionBook, _D1TrapBearOnlyBook, _D1TrapSRBook, _D1TrapFnOSRBook

        cid, bid, underlying = key

        feeder_token = ""
        try:
            creds = self._db.get_feeder_creds_sync("upstox") or {}
            feeder_token = creds.get("access_token", "")
        except Exception:
            pass

        strategy_name = cfg.get("strategy_name", "d1_trap_index")

        # 2026-08-03 fix: D1Trap never called enable_chain() for its own underlying --
        # StrikeRebalancer's ATM+/-chain_depth subscription only activates once SOME
        # strategy calls this (see StraddleBookManager/V4CascadeBookManager, which both
        # already do). A Bear Trap Only deployment on an underlying with no OTHER
        # strategy also running there (e.g. SENSEX with no sell_straddle deployment)
        # got chain_enabled=False forever -- book spawned, warmup/REST worked fine,
        # but live OPTION_TICK never flowed for its strikes, so it could never progress
        # past whatever zone state the historical replay produced. NIFTY only "worked"
        # here because a sell_straddle NIFTY deployment happened to already enable it.
        self._enable_chain(underlying)

        if strategy_name == "d1_trap_bear_only":
            if _D1TrapBearOnlyBook is None:
                from strategies.d1_trap_option.bear_only_book import D1TrapBearOnlyBook as _cls
                _D1TrapBearOnlyBook = _cls
            return _D1TrapBearOnlyBook(
                bus=self._bus,
                cfg=self._cfg,
                underlying=underlying,
                client_id=cid,
                binding_id=bid,
                lot_multiplier=cfg.get("lots", 1),
                feeder_token=feeder_token,
                itm_offset_pts=int(cfg.get("itm_offset_pts", 200)),
                htf_minutes=int(cfg["htf_minutes"]) if cfg.get("htf_minutes") else None,
                product_type=cfg.get("product_type", "MIS"),
                carry_forward=cfg.get("carry_forward", False),
                squareoff_time=cfg.get("squareoff_time", "15:15"),
            )

        if strategy_name == "d1_trap_sr":
            if _D1TrapSRBook is None:
                from strategies.d1_trap_option.sr_book import D1TrapSRBook as _cls
                _D1TrapSRBook = _cls
            return _D1TrapSRBook(
                bus=self._bus,
                cfg=self._cfg,
                underlying=underlying,
                client_id=cid,
                binding_id=bid,
                lot_multiplier=cfg.get("lots", 1),
                feeder_token=feeder_token,
                itm_offset_pts=int(cfg.get("itm_offset_pts", 200)),
                htf_minutes=int(cfg["htf_minutes"]) if cfg.get("htf_minutes") else None,
                sr_tf_minutes=int(cfg.get("sr_tf_minutes", 3)),
                exit_mode=cfg.get("exit_mode", "raw"),
                product_type=cfg.get("product_type", "MIS"),
                carry_forward=cfg.get("carry_forward", False),
                squareoff_time=cfg.get("squareoff_time", "15:15"),
                strike_mode=cfg.get("strike_mode", "daily_atm"),
                execute_strike_mode=cfg.get("execute_strike_mode", "same"),
            )

        if strategy_name == "d1_trap_fno_sr":
            if _D1TrapFnOSRBook is None:
                from strategies.d1_trap_option.fno_sr_book import D1TrapFnOSRBook as _cls
                _D1TrapFnOSRBook = _cls
            return _D1TrapFnOSRBook(
                bus=self._bus,
                cfg=self._cfg,
                underlying=underlying,
                client_id=cid,
                binding_id=bid,
                lot_multiplier=cfg.get("lots", 1),
                feeder_token=feeder_token,
                itm_offset=int(cfg.get("itm_offset", 1)),
                hard_risk_pct=float(cfg.get("hard_risk_pct", 0.10)),
                upstox_key=cfg.get("upstox_key", ""),
                lot_override=cfg.get("lot_override", 0),
                step_override=cfg.get("step_override", 0),
            )

        if _D1TrapOptionBook is None:
            from strategies.d1_trap_option.book import D1TrapOptionBook as _cls
            _D1TrapOptionBook = _cls

        book = _D1TrapOptionBook(
            bus=self._bus,
            cfg=self._cfg,
            underlying=underlying,
            client_id=cid,
            binding_id=bid,
            strategy_name=strategy_name,
            lot_multiplier=cfg.get("lots", 1),
            feeder_token=feeder_token,
            htf_tf=cfg.get("htf_tf", "D1"),
            mtf_tf=cfg.get("mtf_tf", "1H"),
            itm_offset=cfg.get("itm_offset", 1),
            product_type=cfg.get("product_type", "MIS"),
            upstox_key=cfg.get("upstox_key", ""),
            lot_override=cfg.get("lot_override", 0),
            step_override=cfg.get("step_override", 0),
        )

        # Register FnO equity subscriptions on Fyers feeder
        if strategy_name == "d1_trap_fno":
            self._register_fno_equity(underlying, cfg.get("upstox_key", ""))

        return book

    def _register_fno_equity(self, underlying: str, upstox_key_override: str = "") -> None:
        """Register equity spot ticks for FnO D1Trap books via both feeders.

        Fyers path  → subscribe_fno_equity → EQUITY_TICK + INDEX_TICK
        Upstox path → register_extra_spot_keys → INDEX_TICK natively
        Both paths feed CandleCache → CANDLE_CLOSE events → d1_trap_fno C2 logic.
        upstox_key_override: used for WATCHLIST stocks not in FNO_STOCK_CONFIG.
        """
        try:
            from config.global_config import FNO_STOCK_CONFIG
            gf = getattr(self._bus, "_global_feeder", None)
            if gf is None:
                return
            stock = FNO_STOCK_CONFIG.get(underlying.upper()) or {}
            fyers_sym  = stock.get("fyers", "")
            upstox_key = stock.get("upstox_key", "") or upstox_key_override
            # Fyers: subscribe equity symbol → EQUITY_TICK (+INDEX_TICK from feeder fix)
            if fyers_sym and hasattr(gf, "subscribe_fno_equity"):
                gf.subscribe_fno_equity(fyers_sym, underlying)
            # Upstox (primary): map NSE_EQ key → symbol name so ticks flow as INDEX_TICK
            if upstox_key and hasattr(gf, "register_extra_spot_keys"):
                gf.register_extra_spot_keys({upstox_key: underlying})
                logger.info("TrapBookManager: registered equity feed for %s (%s)", underlying, upstox_key)
            elif not upstox_key:
                logger.warning("TrapBookManager: no upstox_key for %s — spot ticks won't arrive", underlying)
        except Exception:
            logger.debug("TrapBookManager: FnO equity registration skipped for %s", underlying)
