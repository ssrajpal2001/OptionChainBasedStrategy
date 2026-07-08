"""
run_feed_server.py — Standalone shared market data feed server.

Starts a GlobalFeeder (Upstox + Fyers dual-feed) and a TCP broadcast hub
on port 15765.  Any number of strategy processes on the same machine (or LAN)
can connect as SharedFeedClient subscribers and receive a unified tick stream.

Usage:
    python run_feed_server.py [options]

Options:
    --provider  upstox|fyers|mock   Primary feeder (default: mock)
    --dual                          Start Upstox + Fyers dual-feed (reads creds from DB)
    --port      PORT                TCP port (default: 15765)
    --loglevel  DEBUG|INFO|WARNING  Logging level (default: INFO)

Credentials:
    Upstox and Fyers credentials are loaded from the client database
    (data/clients.db) using the same feeder credential table that the
    dashboard configures.  Set them via the dashboard before starting
    the feed server in --dual mode.

Examples:
    # Paper/test mode (synthetic ticks, no real broker):
    python run_feed_server.py

    # Live dual-feed mode (reads creds from DB):
    python run_feed_server.py --dual

    # Single-provider mode:
    python run_feed_server.py --provider upstox
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure the project root is on sys.path when run directly
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Shared FeedServer — Upstox + Fyers TCP hub")
    p.add_argument("--provider", default="mock",
                   choices=["mock", "upstox", "fyers"],
                   help="Primary feeder when not using --dual (default: mock)")
    p.add_argument("--dual", action="store_true",
                   help="Start Upstox + Fyers dual-feed using DB credentials")
    p.add_argument("--port", type=int, default=15765,
                   help="TCP port (default: 15765)")
    p.add_argument("--loglevel", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Logging level (default: INFO)")
    return p.parse_args()


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def _load_feeder_creds(db_path: str = "data/clients.db") -> dict[str, dict]:
    """Load all known feeder credentials from the feeder creds table in ClientDB."""
    creds_map: dict[str, dict] = {}
    try:
        from data_layer.client_db import ClientDB
        db = ClientDB(db_path)
        await db.initialise()
        for provider in ("upstox", "upstox2", "fyers"):
            row = db.get_feeder_creds_sync(provider)
            if row and row.get("access_token"):
                creds_map[provider] = {
                    "api_key":      row.get("api_key", ""),
                    "api_secret":   row.get("secret", ""),
                    "user_id":      row.get("client_id", ""),
                    "access_token": row.get("access_token", ""),
                }
        return creds_map
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "FeedServer: could not load credentials from DB: %s — using empty creds.", exc
        )
        return {}


async def _main(args: argparse.Namespace) -> None:
    logger = logging.getLogger("feed_server_main")

    from config.global_config import GlobalConfig, ExchangeConfig, IndicatorParams, StorageConfig
    from data_layer.base_feeder import EventBus
    from data_layer.feed_server import FeedServer
    from data_layer.global_feeder import GlobalFeeder, register_feeder, MockFeeder

    # Register SharedFeedClient so "shared" can be used as a provider
    from data_layer.shared_feed_client import SharedFeedClient
    register_feeder("shared", SharedFeedClient)

    cfg = GlobalConfig()
    cfg.primary_feeder_provider = args.provider
    bus = EventBus()

    # ── Start GlobalFeeder ────────────────────────────────────────────────────
    feeder = GlobalFeeder(bus, cfg)

    if args.dual:
        logger.info("FeedServer: loading feeder credentials from DB...")
        creds_map = await _load_feeder_creds()
        logger.info(
            "FeedServer: credentials loaded — %s",
            ", ".join(f"{p}={bool(c.get('access_token'))}" for p, c in creds_map.items()),
        )
        # Start mock feeder first so the system is live immediately
        await feeder.start()
        # Then bring up the real dual-feed
        if creds_map:
            try:
                await feeder.start_providers(creds_map)
                logger.info("FeedServer: dual-feed active (%s).", ", ".join(creds_map))
            except Exception as exc:
                logger.error("FeedServer: dual-feed start failed: %s — running on mock.", exc)
    else:
        await feeder.start()
        logger.info("FeedServer: single-provider '%s' feeder started.", args.provider)

    # ── Start TCP broadcast server ────────────────────────────────────────────
    tcp_server = FeedServer(bus)
    # Override port if specified
    import data_layer.feed_server as _fs_module
    _fs_module._PORT = args.port

    logger.info(
        "FeedServer: TCP hub starting on 0.0.0.0:%d — waiting for subscribers...", args.port,
    )

    # Run both concurrently; serve_forever blocks until cancelled
    try:
        await asyncio.gather(
            tcp_server.start(),
            _heartbeat_reporter(feeder, tcp_server),
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("FeedServer: shutting down...")
    finally:
        await feeder.stop()
        await tcp_server.stop()
        logger.info("FeedServer: stopped cleanly.")


async def _heartbeat_reporter(feeder, tcp_server) -> None:
    """Log a status line every 60 seconds."""
    logger = logging.getLogger("feed_server_hb")
    while True:
        await asyncio.sleep(60)
        logger.info(
            "FeedServer: provider=%s  connected=%s  clients=%d  last_tick=%.1fs ago",
            feeder.active_provider,
            feeder.is_running,
            tcp_server.client_count,
            tcp_server.last_tick_ago,
        )


if __name__ == "__main__":
    args = _parse_args()
    _setup_logging(args.loglevel)
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        pass
