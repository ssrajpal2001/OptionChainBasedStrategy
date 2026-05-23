"""
data_layer/base_feeder.py — Core pub-sub event bus + normalized tick types.

The EventBus is the zero-copy message spine of the system.
Every module communicates exclusively through it — no direct method
calls across module boundaries in the hot path.

Design:
  Publisher  →  EventBus.publish(topic, event)
                    │
               per-topic  asyncio.Queue list
                    │
  Subscriber  ←  EventBus.subscribe(topic)  →  returns Queue
               Consumer awaits queue.get() in its own async loop.

No time.sleep anywhere. All yielding done via asyncio primitives.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from config.global_config import IST

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Normalized Tick Structs  (immutable, slot-optimized)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True, frozen=True)
class IndexTick:
    symbol: str
    ltp: float
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime          # Always IST-aware


@dataclass(slots=True, frozen=True)
class OptionTick:
    symbol: str                  # Broker-specific; use InternalSymbol for logic
    underlying: str
    strike: float
    option_type: str             # "CE" | "PE"
    expiry: date
    ltp: float
    bid: float
    ask: float
    oi: int
    change_oi: int
    volume: int
    iv: float
    delta: float
    timestamp: datetime          # Always IST-aware


@dataclass(slots=True, frozen=True)
class CandleEvent:
    """Emitted when a candle closes (bucket timestamp changes)."""
    symbol: str
    timeframe: int               # Minutes
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime          # Candle open time, IST-aware
    is_bullish: bool = False

    @property
    def body_ratio(self) -> float:
        rng = self.high - self.low
        return abs(self.close - self.open) / rng if rng > 0 else 0.0

    @property
    def lower_wick_ratio(self) -> float:
        rng = self.high - self.low
        return (min(self.open, self.close) - self.low) / rng if rng > 0 else 0.0

    @property
    def upper_wick_ratio(self) -> float:
        rng = self.high - self.low
        return (self.high - max(self.open, self.close)) / rng if rng > 0 else 0.0


@dataclass(slots=True)
class SystemEvent:
    code: str                    # SysEvent constant
    message: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))


# ─────────────────────────────────────────────────────────────────────────────
# Event Bus
# ─────────────────────────────────────────────────────────────────────────────

class EventBus:
    """
    Zero-latency pub-sub backbone.

    • publish() puts events onto every subscriber queue for that topic.
      If a subscriber queue is full, the event is DROPPED (not blocked)
      and a warning is logged — backpressure must be handled by the
      consumer keeping up with the feed.

    • subscribe() returns a new asyncio.Queue bound to the topic.
      The caller is responsible for draining it in its own task.
    """

    DEFAULT_QUEUE_SIZE = 20_000

    def __init__(self, queue_size: int = DEFAULT_QUEUE_SIZE) -> None:
        self._queue_size = queue_size
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._drop_counts: Dict[str, int] = defaultdict(int)

    def subscribe(self, topic: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_size)
        self._subs[topic].append(q)
        return q

    async def publish(self, topic: str, event: Any) -> None:
        queues = self._subs.get(topic, [])
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self._drop_counts[topic] += 1
                if self._drop_counts[topic] % 1000 == 1:
                    logger.warning(
                        "EventBus: topic '%s' dropped %d events (consumer too slow).",
                        topic, self._drop_counts[topic],
                    )

    def subscriber_count(self, topic: str) -> int:
        return len(self._subs.get(topic, []))

    def drop_stats(self) -> Dict[str, int]:
        return dict(self._drop_counts)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract Base Feeder
# ─────────────────────────────────────────────────────────────────────────────

class BaseFeeder(ABC):
    """
    Contract for all data source adapters.

    Architecture — two-stage pipeline to keep the WebSocket thread unblocked:

        WebSocket callback                   Parse worker task
        ──────────────────                   ─────────────────────────────
        on_message(raw_bytes)  ──put_nowait──> _raw_queue
                                               │  drain (no network I/O here)
                                               ▼
                                          _parse_frame(raw)
                                               │
                                               ▼
                                          _publish_index / _publish_option
                                               │
                                               ▼
                                           EventBus subscribers

    Rule for all concrete implementations:
      • on_message / on_data callbacks MUST call _enqueue_raw() only —
        never parse synchronously inside the callback.
      • All parsing (JSON decode, protobuf decode, etc.) happens in
        _parse_frame(), which runs in the independent _parse_worker task.
      • _parse_worker is started by run() and cancelled by stop().

    This ensures:
      1. The WebSocket recv loop is never stalled by parsing latency.
      2. The EventBus publish() is always called from the event loop thread
         (not from a callback thread), so put_nowait() is safe.
    """

    # Enough capacity for a full second of high-frequency option ticks
    RAW_QUEUE_SIZE = 50_000

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._running = False
        self._connected = False
        # Raw frame queue: WebSocket callback → parse worker
        self._raw_queue: asyncio.Queue = asyncio.Queue(maxsize=self.RAW_QUEUE_SIZE)
        self._parse_task: Optional[asyncio.Task] = None
        self._raw_drop_count: int = 0

    # ── Abstract interface ─────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> bool:
        """Authenticate and open the websocket. Return True on success."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections."""

    @abstractmethod
    async def subscribe_tokens(self, tokens: List[str]) -> None:
        """Subscribe to live feed for the given broker token list."""

    @abstractmethod
    async def unsubscribe_tokens(self, tokens: List[str]) -> None:
        """Unsubscribe from the given broker token list."""

    @abstractmethod
    async def _ws_loop(self) -> None:
        """
        Pure websocket receive loop.  Must call _enqueue_raw() for every
        incoming frame.  No JSON / protobuf decoding here.  Runs until
        stop() is called or connection drops.
        """

    @abstractmethod
    async def _parse_frame(self, raw: Any) -> None:
        """
        Parse one raw frame and publish IndexTick / OptionTick to the bus.
        Called from the _parse_worker task — never from a WS callback.
        May be CPU-intensive; the event loop is not blocked because this
        is the only task that calls it.
        """

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Start parse worker then run the WS loop. When _ws_loop() returns
        (clean disconnect or error), drain the remaining raw queue before
        exiting so no frames are lost.
        """
        self._running = True
        self._parse_task = asyncio.create_task(
            self._parse_worker(), name=f"{self.__class__.__name__}_parse_worker"
        )
        try:
            await self._ws_loop()
        finally:
            # Flush remaining raw frames before teardown
            await self._drain_raw_queue()
            if self._parse_task and not self._parse_task.done():
                self._parse_task.cancel()
                try:
                    await self._parse_task
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._running = False

    # ── Raw queue helpers ─────────────────────────────────────────────────────

    def _enqueue_raw(self, raw: Any) -> None:
        """
        Called from the WebSocket on_message callback (may be non-async).
        Uses put_nowait so the callback returns immediately; drops if full.
        """
        try:
            self._raw_queue.put_nowait(raw)
        except asyncio.QueueFull:
            self._raw_drop_count += 1
            if self._raw_drop_count % 5_000 == 1:
                logger.warning(
                    "%s: raw_queue full — dropped %d frames. "
                    "Increase RAW_QUEUE_SIZE or speed up _parse_frame().",
                    self.__class__.__name__, self._raw_drop_count,
                )

    async def _parse_worker(self) -> None:
        """
        Drains _raw_queue and calls _parse_frame() for each frame.
        Runs as an independent asyncio task — completely decoupled from
        the WebSocket receive loop.
        """
        while self._running:
            try:
                raw = await asyncio.wait_for(self._raw_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._parse_frame(raw)
            except Exception as exc:
                logger.error("%s: _parse_frame() error: %s", self.__class__.__name__, exc)

    async def _drain_raw_queue(self) -> None:
        """Process any frames still sitting in the raw queue after WS closes."""
        while not self._raw_queue.empty():
            try:
                raw = self._raw_queue.get_nowait()
                await self._parse_frame(raw)
            except Exception:
                break

    # ── Publish helpers ──────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def raw_drop_count(self) -> int:
        return self._raw_drop_count

    async def _publish_index(self, tick: IndexTick) -> None:
        from config.global_config import Topic
        await self._bus.publish(Topic.INDEX_TICK, tick)

    async def _publish_option(self, tick: OptionTick) -> None:
        from config.global_config import Topic
        await self._bus.publish(Topic.OPTION_TICK, tick)
