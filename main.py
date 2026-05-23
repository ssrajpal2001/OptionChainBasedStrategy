"""
main.py — Enterprise algo-trading system bootstrap.

Modes:
  paper     -- Live data, mock broker (no real orders)
  live      -- Live data, real broker execution
  backtest  -- Replay recorded ticks through strategy pipeline
  demo      -- Synthetic data, mock broker (no market connection required)

Usage:
  python main.py --mode paper   --index NIFTY
  python main.py --mode backtest --index NIFTY --start 2024-01-15 --end 2024-02-14
  python main.py --mode demo    --index BANKNIFTY --capital 300000
  python main.py --mode live    --index NIFTY  (requires credentials in environment)

Environment variables for live/paper (example: client C001, Shoonya binding):
  C001_SHOONYA_USER_ID=...
  C001_SHOONYA_PASSWORD=...
  C001_SHOONYA_API_SECRET=...
  C001_SHOONYA_TOTP_SECRET=...
  C001_SHOONYA_VENDOR_CODE=...
  C001_SHOONYA_IMEI=...
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime

# Bootstrap import order matters -- broker modules self-register on import
from config.global_config import IST, GlobalConfig, Topic, SysEvent, GLOBAL_CFG
from config.client_profiles import (
    BrokerBinding, ClientProfile, ClientRegistry, RiskProfile, REGISTRY,
)
from data_layer.base_feeder import EventBus
from data_layer.global_feeder import GlobalFeeder
from matrix_engine.candle_cache import CandleCache
from matrix_engine.option_matrix import OptionMatrixEngine
from strategies.base_strategy import ConfluenceEngine
from strategies.strategy_a_oi import StrategyA_OIZone
from strategies.strategy_b_trap import StrategyB_Trap
from strategies.strategy_c_panic import StrategyC_Panic
from execution_bridge import ExecutionRouter       # triggers broker self-registration
from management.client_manager import ClientManager
from management.admin_console import AdminConsole
from backtester.historical_core import HistoricalBacktester


# ─────────────────────────────────────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(cfg: GlobalConfig) -> None:
    os.makedirs(cfg.storage.log_dir, exist_ok=True)
    log_file = os.path.join(
        cfg.storage.log_dir,
        f"algo_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.log",
    )
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, cfg.storage.log_level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=handlers,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Default Client Setup (paper / demo modes)
# ─────────────────────────────────────────────────────────────────────────────

def _setup_default_client(registry: ClientRegistry, capital: float) -> None:
    binding = BrokerBinding(
        binding_id="mock_default",
        provider="mock",
        label="Paper Trading",
    )
    profile = ClientProfile(
        client_id="C001",
        name="Demo Client",
        risk=RiskProfile(
            capital=capital,
            max_risk_per_trade_pct=1.0,
            max_daily_loss_pct=3.0,
            max_daily_trades=10,
        ),
        broker_bindings=[binding],
        enabled_strategies=["A", "B", "C"],
        expiry_preference="CURRENT_WEEK",
    )
    registry.register(profile)
    logging.getLogger(__name__).info(
        "Registered paper client C001 (capital=%.0f).", capital
    )


def _setup_live_clients(registry: ClientRegistry) -> None:
    """
    Load profiles from disk and inject credentials from environment variables.

    Add your client-loading logic here.  Example for C001 with Shoonya:
        registry.load_non_sensitive()
        registry.inject_credentials("C001", "C001_shoonya",
            user_id=os.getenv("C001_SHOONYA_USER_ID", ""), ...)
    """
    registry.load_non_sensitive()
    if registry.count() == 0:
        logging.getLogger(__name__).warning(
            "No client profiles found. Register clients via AdminConsole "
            "or add them to config/client_profiles.json."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Backtest Mode
# ─────────────────────────────────────────────────────────────────────────────

def _run_backtest(
    cfg: GlobalConfig,
    underlying: str,
    start: date,
    end: date,
    capital: float,
) -> None:
    logger = logging.getLogger(__name__)
    logger.info(
        "BACKTEST: %s from %s to %s (capital=%.0f)",
        underlying, start, end, capital,
    )
    cfg.active_index = underlying

    strategies = [StrategyA_OIZone(cfg), StrategyB_Trap(cfg), StrategyC_Panic(cfg)]
    bus = EventBus()
    confluence = ConfluenceEngine(bus, cfg, strategies)

    bt = HistoricalBacktester(cfg, confluence)
    report = bt.run(underlying=underlying, start=start, end=end, capital=capital)
    report.print()

    import json
    out_path = os.path.join(
        cfg.storage.backtest_dir,
        f"backtest_{underlying}_{start}_{end}.json",
    )
    os.makedirs(cfg.storage.backtest_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2)
    logger.info("Backtest result saved: %s", out_path)


# ─────────────────────────────────────────────────────────────────────────────
# Live / Paper / Demo Async Main
# ─────────────────────────────────────────────────────────────────────────────

async def _run_live(
    cfg: GlobalConfig,
    registry: ClientRegistry,
    mode: str,
    underlying: str,
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Starting %s mode for %s", mode.upper(), underlying)
    cfg.active_index = underlying

    bus = EventBus()

    strategies = [StrategyA_OIZone(cfg), StrategyB_Trap(cfg), StrategyC_Panic(cfg)]
    confluence = ConfluenceEngine(bus, cfg, strategies)
    candle_cache = CandleCache(bus, cfg)
    option_matrix = OptionMatrixEngine(bus, cfg)
    feeder = GlobalFeeder(bus, cfg)
    router = ExecutionRouter(bus, registry, cfg)
    client_mgr = ClientManager(bus, registry)

    shutdown_event = asyncio.Event()

    async def _shutdown() -> None:
        logger.info("Admin shutdown requested.")
        shutdown_event.set()

    admin = AdminConsole(bus, registry, router=router, shutdown_callback=_shutdown)

    await router.start()

    tasks = [
        asyncio.create_task(feeder.start(),         name="feeder"),
        asyncio.create_task(candle_cache.run(),      name="candle_cache"),
        asyncio.create_task(option_matrix.run(),     name="option_matrix"),
        asyncio.create_task(confluence.run(),        name="confluence"),
        asyncio.create_task(router.run(),            name="router"),
        asyncio.create_task(client_mgr.run(),        name="client_mgr"),
        asyncio.create_task(admin.run(),             name="admin_console"),
        asyncio.create_task(shutdown_event.wait(),   name="shutdown_sentinel"),
    ]

    logger.info("System live. Type 'help' in admin console for commands.")
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for t in done:
        exc = t.exception() if not t.cancelled() else None
        if exc and t.get_name() != "shutdown_sentinel":
            logger.error("Task '%s' crashed: %s", t.get_name(), exc)

    logger.info("Initiating shutdown...")
    confluence.stop()
    await router.stop()
    await client_mgr.stop()
    await admin.stop()
    feeder.stop()

    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("System stopped cleanly.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multi-Index Option Chain Algo Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["paper", "live", "backtest", "demo"],
        default="demo",
        help="Execution mode (default: demo)",
    )
    p.add_argument(
        "--index",
        default="NIFTY",
        choices=["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"],
    )
    p.add_argument("--start", default=None, help="Backtest start YYYY-MM-DD")
    p.add_argument("--end",   default=None, help="Backtest end YYYY-MM-DD")
    p.add_argument("--capital", type=float, default=500_000.0)
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    cfg = GLOBAL_CFG
    cfg.storage.log_level = args.log_level

    _setup_logging(cfg)
    logger = logging.getLogger(__name__)
    logger.info(
        "OptionChain AlgoTrading | mode=%s | index=%s | capital=%.0f",
        args.mode, args.index, args.capital,
    )

    if args.mode == "backtest":
        if not args.start or not args.end:
            logger.error("--start and --end are required for backtest mode.")
            sys.exit(1)
        _run_backtest(
            cfg,
            underlying=args.index,
            start=date.fromisoformat(args.start),
            end=date.fromisoformat(args.end),
            capital=args.capital,
        )

    elif args.mode in ("paper", "demo", "live"):
        registry = REGISTRY
        if args.mode in ("paper", "demo"):
            _setup_default_client(registry, args.capital)
        else:
            _setup_live_clients(registry)
        asyncio.run(_run_live(cfg, registry, args.mode, args.index))

    else:
        logger.error("Unknown mode: %s", args.mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
