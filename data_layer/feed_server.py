"""
data_layer/feed_server.py — Shared market data TCP broadcast hub.

Runs as a standalone process (python run_feed_server.py).
Maintains ONE Upstox + ONE Fyers WebSocket connection for the entire host,
then fans out normalized JSON ticks to every connected subscriber process
via a lightweight TCP newline-delimited JSON protocol.

Any number of processes on the same machine (or same LAN with port forwarding)
can connect on port 15765 and receive a live stream of ticks without opening
their own broker WebSocket sessions.

Protocol (newline-delimited JSON):
  Client → Server:
    {"cmd": "subscribe",    "instruments": ["NIFTY", "BANKNIFTY", ...]}
    {"cmd": "unsubscribe",  "instruments": ["NIFTY", ...]}
    {"cmd": "ping"}
    {"cmd": "status"}

  Server → Client:
    {"type": "tick",     "symbol": "NIFTY", "ltp": 24500.0,
     "open": 24480.0, "high": 24520.0, "low": 24460.0, "close": 24500.0,
     "volume": 12345, "ts": 1714486539.0, "source": "upstox"}
    {"type": "opt_tick", "symbol": "NIFTY24500CE", "underlying": "NIFTY",
     "strike": 24500.0, "option_type": "CE", "ltp": 150.0,
     "bid": 149.5, "ask": 150.5, "oi": 1234567, "iv": 12.5, "ts": ...}
    {"type": "pong"}
    {"type": "status", "upstox": true, "fyers": true, "clients": 3}
    {"type": "keepalive"}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

from config.global_config import Topic, IST
from data_layer.base_feeder import EventBus, IndexTick, OptionTick

logger = logging.getLogger(__name__)

_HOST = "0.0.0.0"        # listen on all interfaces so LAN peers can connect
_PORT = 15765
_KEEPALIVE_INTERVAL = 30  # seconds between keepalive heartbeats to idle clients


class FeedServer:
    """
    Singleton TCP broadcast hub.

    Start via run_feed_server.py or embedded in any asyncio app:
        server = FeedServer(event_bus)
        await server.start()   # blocks; runs the TCP server forever

    The caller is responsible for populating the EventBus with real ticks
    (usually by running a GlobalFeeder / DualFeeder alongside).
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._writers: List[asyncio.StreamWriter] = []
        self._subscriptions: Dict[asyncio.StreamWriter, Set[str]] = {}
        self._server: Optional[asyncio.Server] = None
        self._index_sub_task: Optional[asyncio.Task] = None
        self._option_sub_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._upstox_connected = False
        self._fyers_connected = False
        self._last_tick_ts: float = 0.0

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Bind TCP socket and start consuming EventBus ticks. Blocks forever."""
        self._index_q = self._bus.subscribe(Topic.INDEX_TICK)
        self._option_q = self._bus.subscribe(Topic.OPTION_TICK)

        self._index_sub_task = asyncio.create_task(
            self._drain_index_ticks(), name="feed_server_index"
        )
        self._option_sub_task = asyncio.create_task(
            self._drain_option_ticks(), name="feed_server_option"
        )
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(), name="feed_server_keepalive"
        )

        self._server = await asyncio.start_server(
            self._handle_client, _HOST, _PORT, reuse_address=True,
        )
        logger.info("FeedServer: listening on %s:%d", _HOST, _PORT)
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        for task in (self._index_sub_task, self._option_sub_task, self._keepalive_task):
            if task and not task.done():
                task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("FeedServer: stopped.")

    def set_provider_status(self, upstox: bool, fyers: bool) -> None:
        self._upstox_connected = upstox
        self._fyers_connected = fyers

    # ── EventBus consumers ───────────────────────────────────────────────────

    async def _drain_index_ticks(self) -> None:
        while True:
            try:
                tick: IndexTick = await asyncio.wait_for(self._index_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            self._last_tick_ts = time.time()
            msg = {
                "type":   "tick",
                "symbol": tick.symbol,
                "ltp":    tick.ltp,
                "open":   tick.open,
                "high":   tick.high,
                "low":    tick.low,
                "close":  tick.close,
                "volume": tick.volume,
                "ts":     self._last_tick_ts,
                "source": "feeder",
            }
            await self._broadcast(msg, tick.symbol)

    async def _drain_option_ticks(self) -> None:
        while True:
            try:
                tick: OptionTick = await asyncio.wait_for(self._option_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            msg = {
                "type":        "opt_tick",
                "symbol":      tick.symbol,
                "underlying":  tick.underlying,
                "strike":      tick.strike,
                "option_type": tick.option_type,
                "expiry":      tick.expiry.isoformat(),
                "ltp":         tick.ltp,
                "bid":         tick.bid,
                "ask":         tick.ask,
                "oi":          tick.oi,
                "change_oi":   tick.change_oi,
                "volume":      tick.volume,
                "iv":          tick.iv,
                "delta":       tick.delta,
                "ts":          time.time(),
            }
            await self._broadcast(msg, tick.underlying)

    # ── TCP broadcast ─────────────────────────────────────────────────────────

    async def _broadcast(self, msg: dict, instrument: str) -> None:
        if not self._writers:
            return
        try:
            line = (json.dumps(msg) + "\n").encode()
        except Exception:
            return
        dead: List[asyncio.StreamWriter] = []
        for w in list(self._writers):
            # Only send to clients that subscribed to this symbol OR subscribed to everything
            subs = self._subscriptions.get(w)
            if subs is not None and len(subs) > 0 and instrument not in subs:
                continue
            try:
                w.write(line)
                await w.drain()
            except Exception:
                dead.append(w)
        for w in dead:
            self._remove_writer(w)

    async def _keepalive_loop(self) -> None:
        ka = (json.dumps({"type": "keepalive"}) + "\n").encode()
        while True:
            await asyncio.sleep(_KEEPALIVE_INTERVAL)
            dead: List[asyncio.StreamWriter] = []
            for w in list(self._writers):
                try:
                    w.write(ka)
                    await w.drain()
                except Exception:
                    dead.append(w)
            for w in dead:
                self._remove_writer(w)

    # ── Client handler ────────────────────────────────────────────────────────

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername", "unknown")
        logger.info("FeedServer: client connected — %s  (total: %d)", peer, len(self._writers) + 1)
        self._writers.append(writer)
        self._subscriptions[writer] = set()   # empty = receives all symbols
        try:
            while True:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=90)
                except asyncio.TimeoutError:
                    # Send keepalive instead of disconnecting
                    writer.write(b'{"type":"keepalive"}\n')
                    await writer.drain()
                    continue
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                await self._handle_command(msg, writer)
        except Exception:
            pass
        finally:
            logger.info("FeedServer: client disconnected — %s", peer)
            self._remove_writer(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_command(self, msg: dict, writer: asyncio.StreamWriter) -> None:
        cmd = msg.get("cmd")
        if cmd == "subscribe":
            instruments = msg.get("instruments") or []
            self._subscriptions.setdefault(writer, set()).update(instruments)
            logger.debug(
                "FeedServer: subscribe +%d instruments from %s",
                len(instruments), writer.get_extra_info("peername"),
            )
        elif cmd == "unsubscribe":
            instruments = msg.get("instruments") or []
            subs = self._subscriptions.get(writer)
            if subs:
                subs.difference_update(instruments)
        elif cmd == "ping":
            writer.write(b'{"type":"pong"}\n')
            await writer.drain()
        elif cmd == "status":
            resp = {
                "type":    "status",
                "upstox":  self._upstox_connected,
                "fyers":   self._fyers_connected,
                "clients": len(self._writers),
                "last_tick_ago": round(time.time() - self._last_tick_ts, 1) if self._last_tick_ts else None,
            }
            writer.write((json.dumps(resp) + "\n").encode())
            await writer.drain()

    def _remove_writer(self, writer: asyncio.StreamWriter) -> None:
        if writer in self._writers:
            self._writers.remove(writer)
        self._subscriptions.pop(writer, None)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def client_count(self) -> int:
        return len(self._writers)

    @property
    def last_tick_ago(self) -> float:
        """Seconds since last tick was broadcast."""
        return time.time() - self._last_tick_ts if self._last_tick_ts else float("inf")
