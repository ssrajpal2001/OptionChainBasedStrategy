"""
data_layer/shared_feed_client.py — TCP client for the shared FeedServer.

Drop-in replacement for any BaseFeeder.  Instead of opening its own broker
WebSocket the SharedFeedClient connects to a running FeedServer (default
127.0.0.1:15765) and receives normalized JSON ticks from it.

Usage (via GlobalFeeder provider name "shared"):
    cfg.primary_feeder_provider = "shared"   # or set FEEDER_PROVIDER=shared
    # GlobalFeeder will instantiate SharedFeedClient automatically.

Or direct:
    client = SharedFeedClient(bus, cfg, host="127.0.0.1", port=15765)
    await client.connect()
    await client.run()

Fallback:  if the FeedServer is unreachable after _FALLBACK_ROUNDS consecutive
reconnect attempts, the client logs a FEEDER_DOWN system event.  The
GlobalFeeder heartbeat will then trigger a switch to the mock feeder so the
rest of the system keeps running.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set

from config.global_config import IST, Topic
from data_layer.base_feeder import (
    BaseFeeder, EventBus, IndexTick, OptionTick, SystemEvent,
)

logger = logging.getLogger(__name__)

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 15765
_CONNECT_TIMEOUT = 3.0
_CONNECT_RETRIES = 3
_RECONNECT_DELAY = 5.0
_IDLE_TIMEOUT    = 90.0
_FALLBACK_ROUNDS = 3   # disconnect rounds before giving up and emitting FEEDER_DOWN


class SharedFeedClient(BaseFeeder):
    """
    TCP client that subscribes to a shared FeedServer and converts its
    normalized JSON tick stream into this project's IndexTick / OptionTick
    events on the local EventBus.

    Registered as provider "shared" in the feeder registry.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg=None,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
    ) -> None:
        super().__init__(bus)
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._fail_rounds = 0
        self._subscribed: Set[str] = set()   # instrument symbols requested so far

    # ── BaseFeeder contract ───────────────────────────────────────────────────

    async def connect(self) -> bool:
        for attempt in range(1, _CONNECT_RETRIES + 1):
            try:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port),
                    timeout=_CONNECT_TIMEOUT,
                )
                self._reader = r
                self._writer = w
                self._connected = True
                self._fail_rounds = 0
                logger.info(
                    "SharedFeedClient: connected to FeedServer %s:%d (attempt %d).",
                    self._host, self._port, attempt,
                )
                # Re-send any pending subscriptions
                if self._subscribed:
                    self._send_cmd({
                        "cmd": "subscribe",
                        "instruments": list(self._subscribed),
                    })
                return True
            except (ConnectionRefusedError, asyncio.TimeoutError, OSError) as exc:
                logger.warning(
                    "SharedFeedClient: connect attempt %d/%d failed: %s",
                    attempt, _CONNECT_RETRIES, exc,
                )
                if attempt < _CONNECT_RETRIES:
                    await asyncio.sleep(1)
        return False

    async def disconnect(self) -> None:
        self._running = False
        self._connected = False
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def subscribe_tokens(self, tokens: List[str]) -> None:
        self._subscribed.update(tokens)
        if self._connected and self._writer:
            self._send_cmd({"cmd": "subscribe", "instruments": tokens})

    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        self._subscribed.difference_update(tokens)
        if self._connected and self._writer:
            self._send_cmd({"cmd": "unsubscribe", "instruments": tokens})

    async def _ws_loop(self) -> None:
        """
        Read newline-delimited JSON from the FeedServer and enqueue raw frames.
        Reconnects automatically on connection loss.
        """
        while self._running:
            if not self._connected:
                ok = await self.connect()
                if not ok:
                    self._fail_rounds += 1
                    logger.warning(
                        "SharedFeedClient: FeedServer unreachable "
                        "(round %d/%d).", self._fail_rounds, _FALLBACK_ROUNDS,
                    )
                    if self._fail_rounds >= _FALLBACK_ROUNDS:
                        logger.error(
                            "SharedFeedClient: giving up after %d failed reconnect rounds. "
                            "Switch to another feeder provider.", _FALLBACK_ROUNDS,
                        )
                        from config.global_config import SysEvent
                        await self._bus.publish(
                            Topic.SYSTEM_EVENT,
                            SystemEvent(SysEvent.FEEDER_DOWN, "shared_feed_unreachable"),
                        )
                        return
                    await asyncio.sleep(_RECONNECT_DELAY)
                    continue

            # Connected — read until the server drops us
            await self._read_loop()

            if self._running:
                logger.warning(
                    "SharedFeedClient: disconnected from FeedServer — reconnecting in %.0fs.",
                    _RECONNECT_DELAY,
                )
                self._connected = False
                self._reader = None
                self._writer = None
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _read_loop(self) -> None:
        while self._running and self._connected and self._reader:
            try:
                line = await asyncio.wait_for(self._reader.readline(), timeout=_IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                # Ping the server to check liveness
                if self._writer:
                    try:
                        self._send_cmd({"cmd": "ping"})
                    except Exception:
                        break
                continue
            except Exception:
                break
            if not line:
                logger.warning("SharedFeedClient: server closed connection.")
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            msg_type = msg.get("type")
            if msg_type in ("tick", "opt_tick"):
                self._enqueue_raw(msg)
            # pong / keepalive / status — silently acknowledged

        self._connected = False

    async def _parse_frame(self, raw: Any) -> None:
        """
        Convert a FeedServer JSON message into IndexTick or OptionTick
        and publish to the local EventBus.
        """
        msg_type = raw.get("type")
        now = datetime.now(IST)

        if msg_type == "tick":
            tick = IndexTick(
                symbol    = raw["symbol"],
                ltp       = float(raw.get("ltp", 0)),
                open      = float(raw.get("open", 0)),
                high      = float(raw.get("high", 0)),
                low       = float(raw.get("low",  0)),
                close     = float(raw.get("close", 0)),
                volume    = int(raw.get("volume", 0)),
                timestamp = now,
            )
            await self._publish_index(tick)

        elif msg_type == "opt_tick":
            try:
                expiry_str = raw.get("expiry", "")
                expiry = date.fromisoformat(expiry_str) if expiry_str else now.date()
            except ValueError:
                expiry = now.date()
            tick = OptionTick(
                symbol      = raw["symbol"],
                underlying  = raw.get("underlying", ""),
                strike      = float(raw.get("strike", 0)),
                option_type = raw.get("option_type", "CE"),
                expiry      = expiry,
                ltp         = float(raw.get("ltp", 0)),
                bid         = float(raw.get("bid", 0)),
                ask         = float(raw.get("ask", 0)),
                oi          = int(raw.get("oi", 0)),
                change_oi   = int(raw.get("change_oi", 0)),
                volume      = int(raw.get("volume", 0)),
                iv          = float(raw.get("iv", 0)),
                delta       = float(raw.get("delta", 0)),
                timestamp   = now,
            )
            await self._publish_option(tick)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _send_cmd(self, payload: dict) -> None:
        if not self._writer:
            return
        try:
            self._writer.write((json.dumps(payload) + "\n").encode())
            asyncio.create_task(self._writer.drain())
        except Exception as exc:
            logger.warning("SharedFeedClient: send_cmd failed: %s", exc)

    async def get_status(self) -> Optional[dict]:
        """Ask the FeedServer for its current status. Returns dict or None."""
        if not self._connected or not self._writer:
            return None
        self._send_cmd({"cmd": "status"})
        # Status response will arrive in the read loop — we can't await it here
        # without a dedicated response channel. Callers should use FeedServer.set_provider_status().
        return None
