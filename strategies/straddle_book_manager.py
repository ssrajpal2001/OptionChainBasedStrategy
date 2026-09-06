"""
strategies/straddle_book_manager.py — per-binding SellStraddle lifecycle.

Maintains ONE independent SellStraddleStrategy "book" per (client, binding, underlying) that has a
sell_straddle deployment. Each book trades fully independently — its own beginning entry (anchored
to when THAT terminal turns ON), own strikes, rolls, exits, position and P&L — sharing only the
admin-configured generic rules and the per-index market feed (the books read the same EventBus
ticks; each keeps its own pool engine).

Reconciles against the DB on an interval so a deployment added in the UI auto-spawns a book
(auto-start-on-deploy) and a removed deployment stops its book. No order mirroring: each book stamps
its own client/binding so the bridge routes only to that broker.
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from strategies.core import StrategyBookManager

logger = logging.getLogger(__name__)

# Placeholder for SellStraddleStrategy.  Production code leaves this as None and
# performs a local import inside _spawn_book() to avoid a circular import with
# strategies.sell_straddle.__init__.  Unit tests monkeypatch this attribute to
# inject a fake book class.
SellStraddleStrategy = None


class StraddleBookManager(StrategyBookManager):
    def __init__(self, bus, cfg, client_db, monitored_indices, reconcile_sec: float = 5.0) -> None:
        super().__init__(bus, cfg, client_db, monitored_indices, reconcile_sec)
        self._delta_chain = None

    def set_delta_chain_manager(self, delta_chain) -> None:
        self._delta_chain = delta_chain
        for book in self._books.values():
            if hasattr(book, "set_delta_chain_manager"):
                book.set_delta_chain_manager(delta_chain)

    def _wanted(self) -> Dict[tuple, dict]:
        """Map of (client,binding,underlying,strategy_name) → {"lots", "shadow_on_reject"}
        for every sell_straddle/sell_straddle_calc_vwap deployment that is RUNNING
        (is_running=1). Single JOIN query — O(1) regardless of client count (replaces
        N+1 per-client loop).

        2026-09-07, real incident fix: the key used to be (client,binding,underlying)
        only, with strategy_name dropped entirely. Two deployments sharing the exact
        same (client,binding,underlying) -- sell_straddle AND sell_straddle_calc_vwap
        both on (ssrajpal2001, UPSTOX, NIFTY), the user's own intended side-by-side
        A/B comparison of VWAP source on the SAME broker account -- collided on one
        dict key, so only whichever row SQLite happened to return last actually
        spawned a book; the other was silently dropped with no trace in any log.
        strategy_name is now part of the key so both run as fully independent books.
        """
        wanted: Dict[tuple, dict] = {}
        rows = self._db.get_running_straddle_deployments_sync()
        for d in rows:
            cid = d.get("client_id", "")
            bid = d.get("binding_id", "")
            und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
            sname = d.get("strategy_name") or "sell_straddle"
            if not cid or not bid:
                continue
            if self._indices and und not in self._indices:
                logger.info(
                    "StraddleBookManager: skipping %s/%s/%s — not in monitored_indices %s",
                    cid, bid, und, sorted(self._indices),
                )
                continue
            try:
                lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
            except Exception:
                lots = 1
            # 2026-08-12, direct request, opt-in per deployment: when set, a broker
            # rejection falls back to a local paper-style fill instead of aborting
            # the position — see SellStraddleStrategy.__init__'s shadow_on_reject
            # docstring and OrderPlacementFailed handling in straddle_bridge.py.
            shadow = False
            vwap_source_override = None
            try:
                params = json.loads(d.get("strategy_params") or "{}")
                shadow = bool(params.get("shadow_on_reject", False))
                _vs = params.get("vwap_source")
                if _vs in ("broker_atp", "calculative"):
                    vwap_source_override = _vs
            except Exception:
                pass
            # 2026-09-06, direct user spec: 'sell_straddle_calc_vwap' is a distinct
            # strategy_name (selectable in the deploy dropdown, unlike the
            # strategy_params.vwap_source override above, which had no UI control
            # anywhere -- a client had no actual way to pick it when deploying a
            # second binding for side-by-side comparison). This strategy_name
            # HARD-forces calculative regardless of strategy_params, since picking
            # this name from the dropdown IS the selection -- no ambiguity to leave
            # room for.
            if sname == "sell_straddle_calc_vwap":
                vwap_source_override = "calculative"
            wanted[(cid, bid, und, sname)] = {
                "lots": lots, "shadow_on_reject": shadow,
                "vwap_source_override": vwap_source_override,
                "strategy_name": sname,
            }
        return wanted

    def _spawn_book(self, key, value):
        # Avoid circular import with strategies.sell_straddle.__init__.py at module
        # load time.  Tests can monkeypatch SellStraddleStrategy directly.
        cls = SellStraddleStrategy
        if cls is None:
            from strategies.sell_straddle import SellStraddleStrategy as cls
        cid, bid, und, sname = key
        book = cls(
            self._bus, self._cfg, underlying=und,
            lot_multiplier=value["lots"], client_id=cid, binding_id=bid,
            shadow_on_reject=value.get("shadow_on_reject", False),
            vwap_source_override=value.get("vwap_source_override"),
            strategy_name=sname,
        )
        book.set_client_db(self._db)
        if self._rebalancer is not None and hasattr(book, "set_rebalancer"):
            book.set_rebalancer(self._rebalancer)
        if self._delta_chain is not None and hasattr(book, "set_delta_chain_manager"):
            book.set_delta_chain_manager(self._delta_chain)
        self._enable_chain(und)
        return book

    def _should_respawn(self, book, value):
        return (getattr(book, "_lot_multiplier", 1) != value["lots"]
                or getattr(book, "_shadow_on_reject", False) != value.get("shadow_on_reject", False)
                or getattr(book, "_vwap_source_override", None) != value.get("vwap_source_override"))

    def _log_spawned(self, key, value):
        logger.info("StraddleBookManager: spawned book %s/%s/%s/%s (lots=%d shadow_on_reject=%s)",
                     *key, value["lots"], value.get("shadow_on_reject", False))

    def _log_stopped(self, key):
        logger.info("StraddleBookManager: stopped book %s/%s/%s/%s", *key)

    def _log_respawned(self, key, value):
        logger.info("StraddleBookManager: re-spawned %s/%s/%s/%s lots→%d shadow_on_reject=%s",
                     *key, value["lots"], value.get("shadow_on_reject", False))

    def _log_reconcile(self, wanted, current):
        # Log the reconcile snapshot at INFO only when the wanted set changes so
        # operators can verify which (client,binding,underlying) books are active
        # without being flooded every 5s.
        _wanted_keys = list(wanted.keys())
        _current_keys = list(current)
        if getattr(self, "_last_logged_wanted", None) != _wanted_keys:
            self._last_logged_wanted = _wanted_keys
            logger.info(
                "StraddleBookManager reconcile: wanted=%s current=%s",
                _wanted_keys, _current_keys,
            )
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "StraddleBookManager reconcile: wanted=%s current=%s",
                _wanted_keys, _current_keys,
            )
