"""
run_system.py — Unified single-command launcher for the OptionChain AlgoTrader.

Replaces direct invocations of main.py by adding:
  • Automatic dependency verification on startup
  • Directory and database bootstrap
  • Optional web dashboard (--ui flag)

Usage:
  # Live trading with web dashboard
  python run_system.py --mode live  --ui --port 8080 --index NIFTY

  # Paper trading with local web UI
  python run_system.py --mode paper --ui --port 8080

  # Demo mode (synthetic data, no broker needed)
  python run_system.py --mode demo

  # Historical backtest
  python run_system.py --mode backtest --start 2024-01-15 --end 2024-02-14

  # Full flag list
  python run_system.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import sys
from datetime import date, datetime

# ─────────────────────────────────────────────────────────────────────────────
# Dependency manifest
# ─────────────────────────────────────────────────────────────────────────────

_CORE_PACKAGES = [
    ("numpy",    "numpy"),
    ("pyarrow",  "pyarrow"),
    ("zstandard","zstandard"),
]

_UI_PACKAGES = [
    ("fastapi",  "fastapi"),
    ("uvicorn",  "uvicorn"),
]

_OPTIONAL_BROKER_PACKAGES = [
    ("NorenRestApiPy", "NorenRestApiPy"),
    ("fyers-apiv3",    "fyers_apiv3"),
    ("smartapi-python","SmartApi"),
    ("dhanhq",         "dhanhq"),
    ("upstox-python-sdk", "upstox_client"),
]


def _check_packages(packages: list) -> list[str]:
    """Return display names of packages that cannot be imported."""
    missing = []
    for display, import_name in packages:
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append(display)
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_system.py",
        description="OptionChain AlgoTrader — unified launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
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
        help="Primary index to monitor (default: NIFTY)",
    )
    p.add_argument("--capital",   type=float, default=500_000.0, help="Client capital in INR")
    p.add_argument("--start",     default=None, help="Backtest start date YYYY-MM-DD")
    p.add_argument("--end",       default=None, help="Backtest end date YYYY-MM-DD")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument("--ui",        action="store_true", help="Start web dashboard alongside system")
    p.add_argument("--port",      type=int, default=8080, help="Web dashboard port (default: 8080)")
    p.add_argument("--host",      default="0.0.0.0",    help="Web dashboard bind host")
    p.add_argument(
        "--no-preflight",
        action="store_true",
        help="Skip dependency checks (faster startup if you know packages are present)",
    )
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(log_dir: str, level: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"algo_{ts}.log")
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Directory bootstrap
# ─────────────────────────────────────────────────────────────────────────────

def _bootstrap_dirs(cfg) -> None:
    """Create all storage directories the system writes to."""
    dirs = [
        cfg.storage.root_dir,
        cfg.storage.recorded_dir,
        cfg.storage.backtest_dir,
        cfg.storage.log_dir,
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Client setup
# ─────────────────────────────────────────────────────────────────────────────

def _setup_default_client(registry, capital: float) -> None:
    from config.client_profiles import BrokerBinding, ClientProfile, RiskProfile
    binding = BrokerBinding(binding_id="mock_default", provider="mock", label="Paper Trading")
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
        "Registered default paper client C001 (capital=%.0f).", capital
    )


def _setup_live_clients(registry) -> None:
    registry.load_non_sensitive()
    if registry.count() == 0:
        logging.getLogger(__name__).warning(
            "No client profiles found. Add profiles to config/client_profiles.json "
            "or register via AdminConsole add_client command."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Backtest runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_backtest(cfg, underlying: str, start: date, end: date, capital: float) -> None:
    logger = logging.getLogger(__name__)
    logger.info("BACKTEST: %s  %s → %s  capital=%.0f", underlying, start, end, capital)

    from data_layer.base_feeder import EventBus
    from strategies.base_strategy import ConfluenceEngine
    from strategies.strategy_a_oi import StrategyA_OIZone
    from strategies.strategy_b_trap import StrategyB_Trap
    from strategies.strategy_c_panic import StrategyC_Panic
    from backtester.historical_core import HistoricalBacktester

    cfg.active_index = underlying
    bus = EventBus()
    strategies = [StrategyA_OIZone(cfg), StrategyB_Trap(cfg), StrategyC_Panic(cfg)]
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
    logger.info("Backtest results saved: %s", out_path)


# ─────────────────────────────────────────────────────────────────────────────
# Live / Paper / Demo async runner
# ─────────────────────────────────────────────────────────────────────────────

async def _run_live(
    cfg,
    registry,
    mode: str,
    underlying: str,
    ui: bool = False,
    ui_host: str = "0.0.0.0",
    ui_port: int = 8080,
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Starting %s mode for %s%s", mode.upper(), underlying,
                f" | Dashboard → http://localhost:{ui_port}" if ui else "")

    cfg.active_index = underlying

    from data_layer.base_feeder import EventBus
    from data_layer.global_feeder import GlobalFeeder
    from data_layer.strike_rebalancer import StrikeRebalancer
    from data_layer.strike_cleanup import StrikeCleanup
    from matrix_engine.candle_cache import CandleCache
    from matrix_engine.gap_handler import GapHandler
    from matrix_engine.option_matrix import OptionMatrixEngine
    from strategies.base_strategy import ConfluenceEngine
    from strategies.strategy_a_oi import StrategyA_OIZone
    from strategies.strategy_b_trap import StrategyB_Trap
    from strategies.strategy_c_panic import StrategyC_Panic
    from execution_bridge import ExecutionRouter
    from management.client_manager import ClientManager
    from management.admin_console import AdminConsole

    bus = EventBus()
    strategies = [StrategyA_OIZone(cfg), StrategyB_Trap(cfg), StrategyC_Panic(cfg)]
    confluence    = ConfluenceEngine(bus, cfg, strategies)
    candle_cache  = CandleCache(bus, cfg)
    option_matrix = OptionMatrixEngine(bus, cfg)
    feeder        = GlobalFeeder(bus, cfg)
    router        = ExecutionRouter(bus, registry, cfg)
    client_mgr    = ClientManager(bus, registry)

    # ── Data-layer operational modules ────────────────────────────────────────
    rebalancer     = StrikeRebalancer(bus, cfg, feeder)
    strike_cleanup = StrikeCleanup(bus, cfg, feeder, rebalancer)
    gap_handler    = GapHandler(bus, cfg, candle_cache=candle_cache)

    # Reset ATM baseline on gap-open so the rebalancer re-anchors to the new spot
    async def _atm_reset_on_gap(underlying_: str, _opening_spot: float) -> None:
        st = rebalancer._state.get(underlying_)
        if st:
            st.current_atm = None
            st.open_atm    = None
    gap_handler.register_reset_callback(_atm_reset_on_gap)

    # Optional web dashboard
    dashboard = None
    if ui:
        try:
            from ui_layer.dashboard_server import DashboardServer
            dashboard = DashboardServer(
                bus, cfg, registry,
                router=router,
                rebalancer=rebalancer,
            )
        except ImportError as exc:
            logger.warning("Could not start dashboard (missing deps): %s", exc)

    shutdown_event = asyncio.Event()

    async def _shutdown() -> None:
        logger.info("Shutdown requested.")
        shutdown_event.set()

    admin = AdminConsole(
        bus, registry,
        router=router,
        shutdown_callback=_shutdown,
        dashboard_server=dashboard,
        dashboard_port=ui_port,
        dashboard_host=ui_host,
    )

    await router.start()

    tasks = [
        asyncio.create_task(feeder.start(),            name="feeder"),
        asyncio.create_task(candle_cache.run(),         name="candle_cache"),
        asyncio.create_task(option_matrix.run(),        name="option_matrix"),
        asyncio.create_task(confluence.run(),           name="confluence"),
        asyncio.create_task(router.run(),               name="router"),
        asyncio.create_task(client_mgr.run(),           name="client_mgr"),
        asyncio.create_task(rebalancer.run(),           name="rebalancer"),
        asyncio.create_task(strike_cleanup.run(),       name="strike_cleanup"),
        asyncio.create_task(gap_handler.run(),          name="gap_handler"),
        asyncio.create_task(admin.run(),                name="admin_console"),
        asyncio.create_task(shutdown_event.wait(),      name="shutdown_sentinel"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for t in done:
        exc = t.exception() if not t.cancelled() else None
        if exc and t.get_name() != "shutdown_sentinel":
            logger.error("Task '%s' crashed: %s", t.get_name(), exc, exc_info=exc)

    logger.info("Shutting down…")
    confluence.stop()
    rebalancer.stop()
    strike_cleanup.stop()
    gap_handler.stop()
    await router.stop()
    await client_mgr.stop()
    await admin.stop()   # also stops the dashboard server
    feeder.stop()

    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("System stopped cleanly.")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    # ── Dependency check ──────────────────────────────────────────────────────
    if not args.no_preflight:
        missing_core = _check_packages(_CORE_PACKAGES)
        if missing_core:
            print(
                "\n[run_system] FATAL — missing core dependencies:\n"
                f"  pip install {' '.join(missing_core)}\n",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.ui:
            missing_ui = _check_packages(_UI_PACKAGES)
            if missing_ui:
                print(
                    "\n[run_system] FATAL — --ui requires:\n"
                    f"  pip install {' '.join(missing_ui)}\n"
                    "  (e.g.  pip install fastapi 'uvicorn[standard]')\n",
                    file=sys.stderr,
                )
                sys.exit(1)

        missing_opt = _check_packages(_OPTIONAL_BROKER_PACKAGES)
        if missing_opt:
            print(
                f"[run_system] Optional broker packages not installed: {', '.join(missing_opt)}\n"
                "  Install the ones for your broker if using live mode.",
            )

    # ── Config + logging ──────────────────────────────────────────────────────
    from config.global_config import GLOBAL_CFG
    cfg = GLOBAL_CFG
    cfg.storage.log_level = args.log_level
    _setup_logging(cfg.storage.log_dir, args.log_level)
    _bootstrap_dirs(cfg)

    logger = logging.getLogger(__name__)
    logger.info(
        "OptionChain AlgoTrader  mode=%-8s  index=%-12s  capital=%.0f%s",
        args.mode, args.index, args.capital,
        f"  dashboard=http://localhost:{args.port}" if args.ui else "",
    )

    # ── Mode dispatch ─────────────────────────────────────────────────────────
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
        from config.client_profiles import REGISTRY
        registry = REGISTRY

        if args.mode in ("paper", "demo"):
            _setup_default_client(registry, args.capital)
        else:
            _setup_live_clients(registry)

        asyncio.run(
            _run_live(
                cfg, registry, args.mode, args.index,
                ui=args.ui,
                ui_host=args.host,
                ui_port=args.port,
            )
        )

    else:
        logger.error("Unknown mode: %s", args.mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
