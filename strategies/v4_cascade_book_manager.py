"""
strategies/v4_cascade_book_manager.py — per-binding V4Cascade lifecycle.

Mirrors strategies/straddle_book_manager.py exactly. Maintains ONE
independent V4CascadeBook per (client, binding, underlying) deployment.
Supported underlyings: NIFTY (real option-premium 3-gate funnel, ATM-200/
ATM+200 tracking strikes, monthly expiry) and BTC/ETH (2026-07-19, spot-only
weekend-validated path via Delta Exchange — no option chain, CE scans spot
for bear traps, PE scans spot for genuine bull traps; see book.py's
_is_crypto branch). Any OTHER underlying is defensively skipped with a
warning log — the strategy's structural assumptions don't generalize past
these two data sources yet.

On/off control reuses the SAME mechanism sell_straddle already has — the
generic per-deployment Run/Stop toggle (is_running, via the strategy-agnostic
/api/client/deployment/{deploy_id}/run endpoint) already filters
get_running_deployments_by_strategy_sync() to is_running=1 rows. No separate
global flag — that would just duplicate this existing control.
"""
from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

# Local import inside _spawn_book() to avoid a circular import with
# strategies.v4_cascade.__init__. Unit tests monkeypatch this attribute.
V4CascadeBook = None

_SUPPORTED_UNDERLYINGS = {"NIFTY", "BTC", "ETH", "CRUDEOIL"}

# 2026-07-24 dev-phase toggle: no dashboard/DB field exists yet for
# V4CascadeConfig.use_pool_engine (the new HTF/LTF engine, validated via
# backtest -- see docs/superpowers/specs/2026-07-23-v4-cascade-htf-ltf-live-design.md),
# so this is a quick env-var switch rather than a proper per-deployment
# config field. Scoped to NIFTY only -- the pool engine has only ever been
# validated against NIFTY data; book.py's own self._is_mcx/_is_crypto guard
# would block it for CRUDEOIL/BTC/ETH anyway, but being explicit here too
# avoids ever depending on that as the only safeguard. Set
# V4CASCADE_USE_POOL_ENGINE=1 in the process environment (e.g. pm2 env
# config) to activate it for every NIFTY v4_cascade deployment.
_POOL_ENGINE_UNDERLYINGS = {"NIFTY"}


def _single_tranche_enabled() -> bool:
    """2026-07-27: dev-phase toggle for single-tranche mode. Set
    V4CASCADE_SINGLE_TRANCHE=1 to put the full position into one leg and
    exit entirely at the T1 target/SL/structural flip (no T2 trail)."""
    return os.environ.get("V4CASCADE_SINGLE_TRANCHE", "").strip().lower() in ("1", "true", "yes", "on")


def _pool_engine_enabled(underlying: str) -> bool:
    if underlying not in _POOL_ENGINE_UNDERLYINGS:
        return False
    return os.environ.get("V4CASCADE_USE_POOL_ENGINE", "").strip().lower() in ("1", "true", "yes", "on")


def _tracking_offsets_from_env() -> Optional[List[float]]:
    """2026-07-24: comma-separated CE/PE candidate strike offsets for the
    pool engine's multi-strike scanning (e.g. "100,200,300,400,500"),
    parsed once at spawn time. Returns None when unset -- V4CascadeBook
    resolves None -> [self._tracking_offset] itself (today's exact single-
    offset behavior), keeping "what a missing env var means" defined in
    exactly one place."""
    raw = os.environ.get("V4CASCADE_TRACKING_OFFSETS", "").strip()
    if not raw:
        return None
    try:
        return [float(x.strip()) for x in raw.split(",") if x.strip()]
    except ValueError:
        logger.warning("V4CascadeBookManager: could not parse V4CASCADE_TRACKING_OFFSETS=%r "
                       "-- falling back to single-offset default.", raw)
        return None

# Default squareoff_time fallback, per underlying -- a deployment row with no
# configured squareoff_time must not silently fall back to NIFTY's 15:15 for
# an MCX underlying (would force-close it minutes after the 09:00 open,
# hours before MCX's real ~23:15-23:30 close -- the same class of bug
# project memory already documents for sell_straddle).
#
# NOTE: this dict must gain an entry for any new MCX underlying added to
# _SUPPORTED_UNDERLYINGS above, or it will silently fall back to NIFTY's
# 15:15 default and force-close it mid-session. book.py's self._is_mcx
# branch (__init__) also needs its own tracking/execution-offset and SL
# buffer numbers for that underlying -- it does not silently inherit
# CRUDEOIL's, but is currently written as if only CRUDEOIL exists.
_DEFAULT_SQUAREOFF_TIME = {"CRUDEOIL": "23:15"}
_FALLBACK_SQUAREOFF_TIME = "15:15"


class V4CascadeBookManager(StrategyBookManager):
    def _wanted(self) -> Dict[tuple, int]:
        wanted: Dict[tuple, int] = {}
        rows = self._db.get_running_deployments_by_strategy_sync("v4_cascade")
        for d in rows:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
            if not cid or not bid:
                continue
            if und not in _SUPPORTED_UNDERLYINGS:
                logger.warning(
                    "V4CascadeBookManager: skipping %s/%s/%s — unsupported underlying "
                    "(only NIFTY/BTC/ETH have a working data path).", cid, bid, und,
                )
                continue
            if self._indices and und not in self._indices:
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            wanted[(cid, bid, und)] = lots
        return wanted

    def _spawn_book(self, key, lots):
        cls = V4CascadeBook
        if cls is None:
            from strategies.v4_cascade.book import V4CascadeBook as cls
        cid, bid, und = key
        # squareoff_time was previously hardcoded (15:15) regardless of what
        # the client configured on the deploy form -- a real bug: the UI
        # showed the configured time as if it were live, but a fresh entry
        # firing after 15:15 would never get force-closed today. Re-fetch
        # the deployment row here (not carried through _wanted()'s lots-only
        # value, to avoid changing the base class's Dict[tuple,int] contract)
        # and pass the real configured time through.
        squareoff_time = _DEFAULT_SQUAREOFF_TIME.get(und, _FALLBACK_SQUAREOFF_TIME)
        try:
            for d in (self._db.get_deployments_sync(cid) or []):
                if (d.get("binding_id") == bid and d.get("strategy_name") == "v4_cascade"
                        and str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper() == und):
                    squareoff_time = str(d.get("squareoff_time") or squareoff_time)
                    break
        except Exception:
            pass
        use_pool_engine = _pool_engine_enabled(und)
        tracking_offsets_pts = _tracking_offsets_from_env() if use_pool_engine else None
        single_tranche = _single_tranche_enabled()
        book = cls(self._bus, self._cfg, underlying=und, client_id=cid, binding_id=bid,
                   lot_multiplier=lots, squareoff_time=squareoff_time,
                   use_pool_engine=use_pool_engine, tracking_offsets_pts=tracking_offsets_pts,
                   single_tranche=single_tranche)
        if use_pool_engine:
            logger.info("V4CascadeBookManager: %s/%s/%s starting with use_pool_engine=True "
                       "(HTF/LTF pool engine, dev-phase env toggle).", cid, bid, und)
        if single_tranche:
            logger.info("V4CascadeBookManager: %s/%s/%s starting with single_tranche=True "
                       "(one leg, exit at T1).", cid, bid, und)
        book.set_client_db(self._db)
        if self._rebalancer is not None and hasattr(book, "set_rebalancer"):
            book.set_rebalancer(self._rebalancer)
        if self._rebalancer is not None and hasattr(self._rebalancer, "enable_chain"):
            self._rebalancer.enable_chain(und)
        return book

    def _should_respawn(self, book, lots):
        return getattr(book, "_lot_multiplier", 1) != lots

    def _log_spawned(self, key, lots):
        logger.info("V4CascadeBookManager: spawned book %s/%s/%s (lots=%d)", *key, lots)

    def _log_stopped(self, key):
        logger.info("V4CascadeBookManager: stopped book %s/%s/%s", *key)

    def _log_respawned(self, key, lots):
        logger.info("V4CascadeBookManager: re-spawned %s/%s/%s lots->%d", *key, lots)

    def force_ingest(self, client_id: str, binding_id: str, underlying: str) -> bool:
        """Admin 'Force Ingest Zones' action — re-runs deep history ingestion
        on an already-running book. Returns False if no matching book exists
        (endpoint should report 404)."""
        import asyncio
        book = self.find(client_id, binding_id, underlying)
        if book is None or not hasattr(book, "force_ingest"):
            return False
        asyncio.create_task(book.force_ingest())
        return True
