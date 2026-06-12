"""
data_layer/delta_chain_manager.py — drives the Delta (crypto) market feed for the sell-straddle.

The NSE flow uses StrikeRebalancer + InstrumentRegistry to subscribe ATM±N strikes. Crypto needs a
parallel: this manager runs ONE DeltaFeeder, discovers the live option chain from /v2/products
(strikes/steps are NON-uniform → never hardcoded), and keeps the feeder subscribed to ATM±window
strikes (both C and P) for the ACTIVE daily expiry — re-subscribing on ATM drift and at the 17:30 IST
rollover. Ticks flow as neutral OptionTick/IndexTick on the same EventBus the sell-straddle book
already consumes, so the strategy is unchanged.

Runs ALONGSIDE the NSE GlobalFeeder — only for crypto underlyings in monitored_indices.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List

import requests

from config.global_config import Topic
from data_layer.delta_feeder import DeltaFeeder, WS_URL  # noqa: F401
from data_layer.delta_rollover import DeltaRolloverWorker
from data_layer.universal_option_mapper import UniversalOptionMapper as _M
from data_layer.symbol_translator import InternalSymbol

logger = logging.getLogger(__name__)

PROD_BASE = "https://api.india.delta.exchange"


class DeltaChainManager:
    def __init__(self, bus, cfg, underlyings: List[str], window: int = 6,
                 reconcile_sec: float = 20.0) -> None:
        self._bus = bus
        self._cfg = cfg
        self._unds = [u.upper() for u in underlyings if str(u).upper() in ("BTC", "ETH")]
        self._window = window
        self._reconcile_sec = reconcile_sec
        self._feeder = DeltaFeeder(bus, cfg)
        self._spot: Dict[str, float] = {}
        self._subbed: Dict[str, set] = {u: set() for u in self._unds}
        self._chain: Dict[str, list] = {}            # underlying -> sorted strikes (active expiry)
        self._running = False
        self._idx_q = bus.subscribe(Topic.INDEX_TICK)

    # ── chain discovery (public REST) ─────────────────────────────────────────
    def _fetch_chain_sync(self, und: str):
        try:
            rows = requests.get(PROD_BASE + "/v2/products", timeout=12).json().get("result", [])
        except Exception as exc:
            logger.warning("DeltaChain: products fetch failed: %s", exc); return [], 0.0
        active = _M.active_daily_expiry()
        ddmmyy = active.strftime("%d%m%y")
        strikes, sym0 = [], None
        for p in rows:
            if p.get("contract_type") not in ("call_options", "put_options"):
                continue
            if (p.get("underlying_asset") or {}).get("symbol") != und:
                continue
            s = str(p.get("symbol", ""))
            if s.endswith(ddmmyy):                    # active daily expiry only
                try:
                    strikes.append(int(float(p.get("strike_price") or 0)))
                except Exception:
                    pass
                sym0 = sym0 or s
        strikes = sorted(set(strikes))
        spot = 0.0
        if sym0:
            try:
                spot = float(requests.get(PROD_BASE + f"/v2/tickers/{sym0}", timeout=10)
                             .json().get("result", {}).get("spot_price") or 0)
            except Exception:
                pass
        return strikes, spot

    def _window_symbols(self, und: str) -> set:
        strikes = self._chain.get(und, [])
        spot = self._spot.get(und, 0.0)
        if not strikes or spot <= 0:
            return set()
        atm = min(strikes, key=lambda k: abs(k - spot))
        i = strikes.index(atm)
        sel = strikes[max(0, i - self._window): i + self._window + 1]
        exp = _M.active_daily_expiry()
        out = set()
        for k in sel:
            for ot in ("CE", "PE"):
                out.add(_M.to_delta_symbol(InternalSymbol(und, float(k), ot, exp)))
        return out

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def run(self) -> None:
        if not self._unds:
            return
        self._running = True
        ok = await self._feeder.connect()
        if not ok:
            logger.error("DeltaChain: feeder connect failed — crypto feed unavailable."); return
        asyncio.create_task(self._feeder.run(), name="delta_feeder_run")
        asyncio.create_task(self._track_spot(), name="delta_spot_track")
        # rollover → re-discover + re-subscribe
        rollover = DeltaRolloverWorker(self._on_rollover)
        asyncio.create_task(rollover.run(), name="delta_rollover")
        logger.info("DeltaChainManager: started for %s.", self._unds)
        # initial discovery + subscribe
        for und in self._unds:
            await self._reconcile(und, force=True)
        while self._running:
            try:
                await asyncio.sleep(self._reconcile_sec)
            except asyncio.CancelledError:
                break
            for und in self._unds:
                await self._reconcile(und)

    async def _track_spot(self) -> None:
        """Update spot from the feeder's IndexTick stream (DeltaFeeder publishes spot_price)."""
        while self._running:
            try:
                tick = await asyncio.wait_for(self._idx_q.get(), timeout=5.0)
                u = str(getattr(tick, "symbol", "")).upper()
                if u in self._spot or u in self._unds:
                    self._spot[u] = float(getattr(tick, "ltp", 0.0) or 0.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

    async def _reconcile(self, und: str, force: bool = False) -> None:
        if force or und not in self._chain:
            strikes, spot = await asyncio.to_thread(self._fetch_chain_sync, und)
            if strikes:
                self._chain[und] = strikes
            if spot > 0 and self._spot.get(und, 0.0) <= 0:
                self._spot[und] = spot
        want = self._window_symbols(und)
        if not want:
            return
        cur = self._subbed[und]
        add = want - cur
        rem = cur - want
        if add:
            await self._feeder.subscribe_tokens(list(add))
        if rem:
            await self._feeder.unsubscribe_tokens(list(rem))
        if add or rem:
            self._subbed[und] = want
            logger.info("DeltaChain[%s]: subscribed %d strikes (ATM~%.0f, +%d/-%d).",
                        und, len(want) // 2, self._spot.get(und, 0.0), len(add), len(rem))

    async def _on_rollover(self, old, new) -> None:
        logger.info("DeltaChain: rollover %s→%s — re-discovering chains.", old, new)
        for und in self._unds:
            self._subbed[und] = set()        # force full re-subscribe to the new daily expiry
            await self._reconcile(und, force=True)

    def stop(self) -> None:
        self._running = False
        try:
            asyncio.create_task(self._feeder.disconnect())
        except Exception:
            pass
