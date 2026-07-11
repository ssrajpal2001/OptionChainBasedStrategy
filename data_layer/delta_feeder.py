"""
data_layer/delta_feeder.py — Delta Exchange (India) market-data feeder.

STAGE 2b of the Delta integration. Subclasses the EXISTING BaseFeeder and publishes the SAME
neutral IndexTick / OptionTick events the strategies already consume — so the strategy layer never
knows it's crypto.

VERIFIED field map (live /v2/tickers + v2_ticker WS):
    OptionTick.ltp   ← ticker "close"        (last traded premium)
    OptionTick.atp   ← ticker "mark_price"   (Delta's smooth fair value = our VWAP source;
                                              no broker "ATP" exists on crypto, mark_price is the
                                              clean equivalent and avoids LTP dropout noise)
    OptionTick.bid/ask ← quotes.best_bid / best_ask
    OptionTick.oi    ← "oi"      OptionTick.iv ← quotes.mark_iv     OptionTick.delta ← greeks.delta
    IndexTick (spot) ← ticker "spot_price"   (underlying BTC/ETH spot)

Symbology: C-BTC-60000-310726 (UniversalOptionMapper). WS:
    {"cmd":"subscribe","channels":[{"name":"v2_ticker","symbols":["C-BTC-65000-150626", ...]}]}

NOTE: Delta API keys are IP-whitelisted (whitelist the EC2 IP on the Delta key) — same pattern as
the SEBI source-IP work for Indian brokers. Market data (public channels) needs no key.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, List

from config.global_config import IST
from data_layer.base_feeder import BaseFeeder, IndexTick, OptionTick
from data_layer.universal_option_mapper import UniversalOptionMapper

logger = logging.getLogger(__name__)

WS_URL = "wss://socket.india.delta.exchange"   # Delta India market-data socket (v2/ticker channel)

_STALE_TICK_SEC = 300   # 5 min no ticks → assume WS silently dead → force close


class DeltaFeeder(BaseFeeder):
    def __init__(self, bus, cfg=None) -> None:
        super().__init__(bus)
        self._cfg = cfg
        self._session = None
        self._ws = None
        self._subs: set = set()
        self._heartbeat_task = None
        self._last_tick_ts: float = 0.0

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def connect(self) -> bool:
        try:
            import aiohttp
        except ImportError:
            logger.error("DeltaFeeder: aiohttp not installed. pip install aiohttp")
            return False
        try:
            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(WS_URL, heartbeat=30)
            self._connected = True
            self._last_tick_ts = time.monotonic()
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            if self._subs:
                await self.subscribe_tokens(list(self._subs))
            logger.info("DeltaFeeder: connected to %s.", WS_URL)
            return True
        except Exception as exc:
            logger.error("DeltaFeeder: connect failed: %s", exc)
            return False

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        for t in (self._heartbeat_task,):
            if t:
                t.cancel()
        try:
            if self._ws:
                await self._ws.close()
            if self._session:
                await self._session.close()
        except Exception:
            pass

    async def _heartbeat_loop(self) -> None:
        # Enable server-side heartbeats once, then watch for stale ticks.
        try:
            await self._ws.send_json({"type": "enable_heartbeat"})
        except Exception:
            pass
        while self._running and self._ws is not None:
            await asyncio.sleep(60)
            if not self._running:
                break
            elapsed = time.monotonic() - self._last_tick_ts
            if elapsed > _STALE_TICK_SEC:
                logger.warning(
                    "DeltaFeeder: no ticks for %.0fs — closing WS to force reconnect.", elapsed
                )
                try:
                    await self._ws.close()
                except Exception:
                    pass
                break

    # ── subscription ──────────────────────────────────────────────────────────
    async def subscribe_tokens(self, tokens: List[str]) -> None:
        self._subs.update(tokens)
        if not self._ws:
            return
        await self._ws.send_json({
            "type": "subscribe",
            "payload": {"channels": [{"name": "v2/ticker", "symbols": list(tokens)}]},
        })
        logger.info("DeltaFeeder: subscribed %d symbols.", len(tokens))

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        self._subs.difference_update(tokens)
        if not self._ws:
            return
        await self._ws.send_json({
            "type": "unsubscribe",
            "payload": {"channels": [{"name": "v2/ticker", "symbols": list(tokens)}]},
        })

    # ── ws loop + parse (BaseFeeder two-stage pipeline) ───────────────────────
    async def _ws_loop(self) -> None:
        import aiohttp
        while self._running and self._ws is not None:
            try:
                msg = await self._ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._enqueue_raw(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
            except Exception as exc:
                logger.warning("DeltaFeeder: ws recv error: %s", exc)
                break

    async def _parse_frame(self, raw: Any) -> None:
        try:
            d = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        except Exception:
            return
        if d.get("type") != "v2/ticker":
            return
        self._last_tick_ts = time.monotonic()
        sym = str(d.get("symbol", "")).upper()
        if not sym:
            return
        # Perpetual futures (e.g. BTCUSD) — emit IndexTick directly; no option parse needed.
        if sym[0] not in ("C", "P"):
            spot = float(d.get("spot_price") or d.get("close") or d.get("mark_price") or 0.0)
            if spot > 0 and sym.endswith("USD"):
                und = sym.replace("USD", "")  # BTCUSD → BTC, ETHUSD → ETH
                await self._publish_index(IndexTick(
                    symbol=und, ltp=spot, open=spot, high=spot, low=spot,
                    close=spot, volume=0, timestamp=datetime.now(IST),
                ))
            return
        try:
            internal = UniversalOptionMapper.parse_delta_symbol(sym)
        except Exception:
            return
        q = d.get("quotes") or {}
        g = d.get("greeks") or {}
        now = datetime.now(IST)
        await self._publish_option(OptionTick(
            symbol=sym, underlying=internal.underlying, strike=internal.strike,
            option_type=internal.option_type, expiry=internal.expiry,
            ltp=float(d.get("close") or 0.0),
            bid=float(q.get("best_bid") or 0.0), ask=float(q.get("best_ask") or 0.0),
            oi=int(float(d.get("oi") or 0.0)), change_oi=0,
            volume=int(float(d.get("volume") or 0.0)),
            iv=float(q.get("mark_iv") or 0.0), delta=float(g.get("delta") or 0.0),
            timestamp=now,
            atp=float(d.get("mark_price") or 0.0),     # VWAP source on Delta
        ))
        # Underlying spot → IndexTick (so the strategy's ATM/spot logic works unchanged).
        spot = float(d.get("spot_price") or 0.0)
        if spot > 0:
            await self._publish_index(IndexTick(
                symbol=internal.underlying, ltp=spot, open=spot, high=spot, low=spot,
                close=spot, volume=0, timestamp=now,
            ))
