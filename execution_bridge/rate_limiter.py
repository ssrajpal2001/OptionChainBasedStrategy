"""
execution_bridge/rate_limiter.py — Async token-bucket rate limiter.

Each broker binding gets its own independent TokenBucket instance held inside
ClientExecutionWorker.  Every outgoing API call (place_order, get_order_status,
cancel_order) must call `await bucket.acquire()` before proceeding.

Token-bucket algorithm:
  • Bucket holds up to `burst` tokens.
  • Tokens refill continuously at `rate` tokens per second.
  • Each API call consumes exactly 1 token.
  • When the bucket is empty, `acquire()` calculates the exact wait time and
    yields control via `asyncio.sleep()` — no busy-wait, no time.sleep.

At 10 req/s (default):
  • 1 token = 100 ms minimum spacing between calls.
  • Burst of 10 means up to 10 simultaneous calls are absorbed immediately,
    then rate-limited to ≤ 10/s thereafter.
  • If a signal fires while 3 trailing order adjustments are in flight, all
    four requests queue locally inside the bucket rather than hitting the broker
    API together, preventing 429 / rate-limit rejections.

Broker-specific profiles:
  BrokerRateProfile defines (rate, burst) per provider so different brokers
  can enforce different limits without any code change in the worker.

No time.sleep. All yielding via asyncio.sleep with computed wait duration.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Broker Rate Profiles
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BrokerRateProfile:
    """Maximum outgoing API request rate for a given broker provider."""
    rate: float    # Tokens refilled per second (= max sustained req/s)
    burst: int     # Bucket capacity (= max instantaneous burst)


# Hard limits per broker (conservative; raise if broker confirms higher limits)
BROKER_RATE_PROFILES: Dict[str, BrokerRateProfile] = {
    "shoonya":   BrokerRateProfile(rate=10.0, burst=10),
    "fyers":     BrokerRateProfile(rate=10.0, burst=10),
    "angelone":  BrokerRateProfile(rate=10.0, burst=10),
    "dhan":      BrokerRateProfile(rate=10.0, burst=10),
    "upstox":    BrokerRateProfile(rate=10.0, burst=10),
    "mock":      BrokerRateProfile(rate=1000.0, burst=1000),   # Unlimited for mock
}

_DEFAULT_PROFILE = BrokerRateProfile(rate=10.0, burst=10)


def profile_for(provider: str) -> BrokerRateProfile:
    return BROKER_RATE_PROFILES.get(provider.lower(), _DEFAULT_PROFILE)


# ─────────────────────────────────────────────────────────────────────────────
# Token Bucket
# ─────────────────────────────────────────────────────────────────────────────

class TokenBucket:
    """
    Async token-bucket rate limiter.

    Thread-safe within a single asyncio event loop (no threading involved).
    One instance per broker binding per client.

    Usage:
        bucket = TokenBucket(rate=10.0, burst=10)
        await bucket.acquire()        # blocks if empty; yields for exact wait
        await broker.place_order(req) # guaranteed ≤ rate req/s
    """

    def __init__(self, rate: float = 10.0, burst: int = 10) -> None:
        if rate <= 0:
            raise ValueError(f"TokenBucket: rate must be > 0, got {rate}")
        self._rate = rate
        self._burst = burst
        self._tokens: float = float(burst)   # Start full so first burst is free
        self._last_refill: float = 0.0       # 0 = not yet initialised
        self._total_waits: int = 0
        self._total_acquired: int = 0

    @property
    def available_tokens(self) -> float:
        return self._tokens

    @property
    def total_waits(self) -> int:
        return self._total_waits

    async def acquire(self, tokens: int = 1) -> None:
        """
        Wait until `tokens` tokens are available, then consume them.

        This is the hot path — called before every broker API call.
        If tokens are available immediately, returns in O(1) with zero yield.
        If not, yields for exactly (deficit / rate) seconds, then returns.
        """
        if tokens <= 0:
            return

        loop = asyncio.get_event_loop()
        now = loop.time()

        # Lazy initialise _last_refill to avoid anomalous first-call behaviour
        if self._last_refill == 0.0:
            self._last_refill = now

        while True:
            now = loop.time()
            elapsed = now - self._last_refill
            # Refill tokens proportional to elapsed time
            self._tokens = min(float(self._burst), self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                self._total_acquired += 1
                return

            # Calculate exact time to wait for `tokens` tokens
            deficit = tokens - self._tokens
            wait_secs = deficit / self._rate
            self._total_waits += 1
            logger.debug(
                "TokenBucket(rate=%.0f): waiting %.1fms for %d token(s) "
                "(available=%.2f, total_waits=%d).",
                self._rate, wait_secs * 1000, tokens,
                self._tokens, self._total_waits,
            )
            await asyncio.sleep(wait_secs)

    def stats(self) -> dict:
        return {
            "rate": self._rate,
            "burst": self._burst,
            "available_tokens": round(self._tokens, 2),
            "total_acquired": self._total_acquired,
            "total_waits": self._total_waits,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Per-Client Rate Limiter Registry
# ─────────────────────────────────────────────────────────────────────────────

class ClientRateLimiterRegistry:
    """
    Creates and stores one TokenBucket per (client_id, binding_id, provider).

    ClientExecutionWorker calls get_limiter(binding_id, provider) to retrieve
    the bucket for a specific broker — no knowledge of profiles needed at the
    call site.
    """

    def __init__(self, client_id: str) -> None:
        self._client_id = client_id
        self._limiters: Dict[str, TokenBucket] = {}

    def get_limiter(self, binding_id: str, provider: str) -> TokenBucket:
        if binding_id not in self._limiters:
            p = profile_for(provider)
            self._limiters[binding_id] = TokenBucket(rate=p.rate, burst=p.burst)
            logger.info(
                "RateLimiter[%s/%s]: created bucket rate=%.0f/s burst=%d.",
                self._client_id, binding_id, p.rate, p.burst,
            )
        return self._limiters[binding_id]

    def all_stats(self) -> Dict[str, dict]:
        return {bid: b.stats() for bid, b in self._limiters.items()}

    def override_rate(self, binding_id: str, rate: float, burst: int) -> None:
        """Admin override — replace the bucket for a specific binding at runtime."""
        self._limiters[binding_id] = TokenBucket(rate=rate, burst=burst)
        logger.info(
            "RateLimiter[%s/%s]: rate overridden to %.0f/s burst=%d.",
            self._client_id, binding_id, rate, burst,
        )
