"""
run_system.py — Unified single-command launcher for the OptionChain AlgoTrader.

  • Automatic dependency verification on startup
  • Directory and database bootstrap
  • Optional web dashboard (--ui flag)

Usage:
  # Live trading with web dashboard
  python run_system.py --mode live  --ui --port 5000 --index NIFTY

  # Paper trading (real order routed + local sim fill) with local web UI
  python run_system.py --mode paper --ui --port 5000

  # Full flag list
  python run_system.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import gc
gc.set_threshold(700, 10, 10)   # gen0 less frequent → fewer GC pauses during tick storms
import importlib
import logging
import os
import sys
from datetime import date, datetime
from typing import List

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

_WEAK_PASSWORDS = {"admin123", "password", "changeme", "secret", ""}


def _enforce_secrets(mode: str) -> None:
    """Refuse to start in live mode with default/weak credentials."""
    if mode not in ("live",):
        return
    pwd = os.getenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    if pwd in _WEAK_PASSWORDS:
        sys.exit(
            "\nFATAL: TERMINUS_ADMIN_PASSWORD is a default/weak value.\n"
            "  Set a strong password: export TERMINUS_ADMIN_PASSWORD=<your-password>\n"
            "  Then restart.\n"
        )
    jwt = os.getenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    if "CHANGE-IN-PRODUCTION" in jwt or len(jwt) < 32:
        sys.exit(
            "\nFATAL: TERMINUS_JWT_SECRET is the dev default or too short (< 32 chars).\n"
            "  Generate one: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
            "  Then: export TERMINUS_JWT_SECRET=<generated-value>\n"
        )


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
        choices=["paper", "live"],
        default="live",
        help="Execution mode (default: live). 'paper' = real order routed + local sim fill.",
    )
    p.add_argument(
        "--index",
        default="NIFTY",
        help="Index/commodity to run, e.g. NIFTY or CRUDEOIL. Comma-separate to run several "
             "at once (e.g. NIFTY,SENSEX). This drives which strategies SPAWN (monitored_indices).",
    )
    p.add_argument("--capital",   type=float, default=500_000.0, help="Client capital in INR")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument("--ui",        action="store_true", help="Start web dashboard alongside system")
    p.add_argument("--port",      type=int, default=5000, help="Web dashboard port (default: 5000)")
    p.add_argument("--host",      default="0.0.0.0",    help="Web dashboard bind host")
    p.add_argument(
        "--no-preflight",
        action="store_true",
        help="Skip dependency checks (faster startup if you know packages are present)",
    )
    p.add_argument(
        "--strategies",
        default="sell_straddle,iron_condor,trap_scanner",
        help="Comma-list of strategies to RUN. Others are constructed but never started. "
             "e.g. --strategies sell_straddle (run only the sell-straddle).",
    )
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(log_dir: str, level: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(os.path.join(log_dir, "trades"), exist_ok=True)
    os.makedirs(os.path.join(log_dir, "clients"), exist_ok=True)
    # System log: logs/system-YYYYMMDD.log — rotating 50 MB × 5 files = 250 MB max
    import logging.handlers as _lh
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(log_dir, f"system-{date_str}.log")
    _fh = _lh.RotatingFileHandler(
        log_file, encoding="utf-8", maxBytes=50 * 1024 * 1024, backupCount=5,
    )
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            _fh,
        ],
    )


def get_client_logger(client_id: str, strategy: str, log_dir: str = "logs") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(
        f"{client_id}_{strategy}_{date_str}",
        log_dir=os.path.join(log_dir, "clients"),
        propagate=True,
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
        # JSON file empty — load from DB instead
        _load_registry_from_db(registry)
    if registry.count() == 0:
        logging.getLogger(__name__).warning(
            "No client profiles found. Add profiles to config/client_profiles.json "
            "or register via AdminConsole add_client command."
        )


def _load_registry_from_db(registry) -> None:
    """Populate in-memory ClientRegistry from clients.db at startup."""
    import sqlite3
    from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding
    log = logging.getLogger(__name__)
    db_path = os.path.join("data", "clients.db")
    if not os.path.exists(db_path):
        return
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        clients = con.execute(
            "SELECT * FROM clients WHERE is_active=1"
        ).fetchall()
        for row in clients:
            cid = row["client_id"]
            # capital lives on RiskProfile; lot_multiplier is per BrokerBinding —
            # neither is a ClientProfile field, so do NOT pass them here.
            profile = ClientProfile(
                client_id=cid,
                name=row["name"] or "",
                email=row["email"] or "",
                risk=RiskProfile(
                    capital=float(row["capital"] or 500000),
                    max_risk_per_trade_pct=float(row["max_risk_pct"] or 1.0),
                    max_daily_loss_pct=float(row["max_daily_loss_pct"] or 3.0),
                ),
                is_admin_approved=bool(row["is_admin_approved"]),
                is_client_bot_active=bool(row["is_client_bot_active"]),
                target_index=row["target_index"] or "NIFTY",
            )
            # Load broker bindings. Use a defensive getter so a missing DB column
            # can never crash the whole registry load (which leaves Router with 0
            # clients and blocks all order routing). Pass auth creds so the broker
            # can authenticate for LIVE orders (otherwise broker=None → paper fill).
            from data_layer.client_db import _decode_cred

            def _bget(row, key, default=""):
                try:
                    val = row[key]
                except (IndexError, KeyError):
                    return default
                return val if val is not None else default

            def _bdec(row, key):
                return _decode_cred(_bget(row, key, ""))

            bindings = con.execute(
                "SELECT * FROM broker_bindings WHERE client_id=? AND enabled=1", (cid,)
            ).fetchall()
            for b in bindings:
                # Credentials are stored XOR-encoded in *_enc columns; access_token
                # is plaintext. Decode so the broker can authenticate for LIVE orders.
                profile.broker_bindings.append(BrokerBinding(
                    binding_id=b["binding_id"],
                    provider=b["provider"],
                    label=_bget(b, "label", "") or "",
                    user_id=_bdec(b, "user_id_enc"),
                    api_key=_bdec(b, "api_key_enc"),
                    api_secret=_bdec(b, "api_secret_enc"),
                    access_token=_bget(b, "access_token", "") or "",
                    trading_mode=_bget(b, "trading_mode", "paper") or "paper",
                    assigned_strategy=_bget(b, "assigned_strategy", "") or "",
                    is_trade_enabled=bool(_bget(b, "is_trade_enabled", 1)),
                    lot_multiplier=float(_bget(b, "lot_multiplier", 1.0) or 1.0),
                    product_type=_bget(b, "product_type", "MIS") or "MIS",
                    password=_bdec(b, "password_enc"),
                    totp_secret=_bdec(b, "totp_secret_enc"),
                    source_ip=_bget(b, "source_ip", "") or "",
                ))
            registry.register(profile)
            log.info("Loaded client from DB: %s (approved=%s)", cid, profile.is_admin_approved)
        con.close()
    except Exception as exc:
        logging.getLogger(__name__).error("_load_registry_from_db failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Live / Paper async runner
# ─────────────────────────────────────────────────────────────────────────────

async def _run_live(
    cfg,
    registry,
    mode: str,
    underlying: str,
    ui: bool = False,
    ui_host: str = "0.0.0.0",
    ui_port: int = 5000,
    strategies: str = "sell_straddle,iron_condor,trap",
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Starting %s mode for %s%s", mode.upper(), underlying,
                f" | Dashboard http://localhost:{ui_port}" if ui else "")

    # --index drives which strategies actually SPAWN. Previously only the feeder primary
    # followed --index while strategies spawned from the hardcoded cfg.monitored_indices, so
    # launching --index NIFTY while monitored_indices=[CRUDEOIL] ran CRUDEOIL and NIFTY
    # deployments found no running strategy. Comma-separate to run several (NIFTY,SENSEX).
    _idxs = [s.strip().upper() for s in str(underlying).split(",") if s.strip()]
    if _idxs:
        cfg.monitored_indices = _idxs
        cfg.active_index = _idxs[0]
        if len(_idxs) > 1:
            logger.info("Running %d indices: %s (active=%s)", len(_idxs), _idxs, _idxs[0])
    else:
        cfg.active_index = underlying

    from data_layer.base_feeder import EventBus
    from data_layer.global_feeder import GlobalFeeder
    from data_layer.strike_rebalancer import StrikeRebalancer
    from data_layer.strike_cleanup import StrikeCleanup
    from matrix_engine.candle_cache import CandleCache
    from matrix_engine.gap_handler import GapHandler
    from matrix_engine.option_matrix import OptionMatrixEngine
    from execution_bridge import ExecutionRouter
    from strategies.registry import STRATEGY_REGISTRY, create_strategy_manager
    from execution_bridge.straddle_bridge import StraddleExecutionBridge
    from execution_bridge.ic_bridge import ICExecutionBridge
    from management.client_manager import ClientManager
    from management.admin_console import AdminConsole
    from management.risk_manager import RiskManager

    _enabled_strats = {s.strip().lower() for s in (strategies or "").split(",") if s.strip()}
    logger.info("run_system: enabled strategies = %s", sorted(_enabled_strats) or "ALL")

    bus = EventBus()

    candle_cache  = CandleCache(bus, cfg)
    option_matrix = OptionMatrixEngine(bus, cfg)
    feeder        = GlobalFeeder(bus, cfg)
    router        = ExecutionRouter(bus, registry, cfg)
    from data_layer.client_db import ClientDB as _ClientDB
    _shared_client_db = _ClientDB()
    await _shared_client_db.initialise()
    cfg.exchange.load_from_db(_shared_client_db)
    # Share the same DB instance across bridge + dashboard so engine_active state is consistent
    router._client_db = _shared_client_db
    # Build all enabled strategy managers from the registry.
    managers: dict = {}
    for name in _enabled_strats:
        if name in STRATEGY_REGISTRY:
            managers[name] = create_strategy_manager(name, bus, cfg, _shared_client_db, cfg.monitored_indices)

    # Backward-compat variables consumed by the dashboard and bridges.
    straddle_manager = managers.get("sell_straddle")
    trap_scanner_manager = managers.get("trap_scanner")
    iron_condor_manager = managers.get("iron_condor")
    _iron_condors = iron_condor_manager.books if iron_condor_manager else []

    # Give each Iron Condor the feeder so it can subscribe next-expiry strikes
    # for the min-LTP expiry shift (no-op if the feature is unused).
    if iron_condor_manager is not None and hasattr(iron_condor_manager, "set_feeder"):
        iron_condor_manager.set_feeder(feeder)
    # Crypto (Delta) feed: for any BTC/ETH in monitored_indices, run a DeltaChainManager that
    # drives a DeltaFeeder (spot + ATM±N strikes for the active daily expiry + 17:30 rollover) onto
    # the SAME EventBus the sell-straddle books consume. Runs alongside the NSE GlobalFeeder.
    # DeltaChainManager always starts for BTC/ETH — crypto has its own Delta feed independent
    # of monitored_indices (Upstox/Fyers). A client can deploy BTC even if --index BTC is absent.
    from data_layer.delta_chain_manager import DeltaChainManager
    _crypto_idx_base = list({u for u in cfg.monitored_indices if cfg.exchange.is_crypto(u)})
    _crypto_all = list({u for u in (cfg.exchange.crypto_underlyings or ("BTC", "ETH"))})
    # Always run for BTC + ETH so a deploy works even without --index BTC in pm2 args
    delta_chain = DeltaChainManager(bus, cfg, _crypto_all)
    logger.info("Delta crypto feed always-on for %s (monitored: %s).", _crypto_all, _crypto_idx_base)
    straddle_bridge = StraddleExecutionBridge(
        bus, registry, router,
        log_dir=os.path.join(cfg.storage.log_dir, "trades"),
    )
    ic_bridge     = ICExecutionBridge(bus, registry, router)
    client_mgr    = ClientManager(bus, registry)
    risk_mgr      = RiskManager(bus, registry, router=router)

    # ── Instrument registry — load active contracts from Upstox API ───────────
    from data_layer.instrument_registry import REGISTRY as _instrument_registry
    _upstox_creds = await asyncio.to_thread(
        _shared_client_db.get_feeder_creds_sync, "upstox"
    )
    _upstox_token = (_upstox_creds or {}).get("access_token", "")
    from data_layer.instrument_registry import _MCX_UNDERLYINGS as _MCX_SET
    # MCX commodities (CRUDEOIL) load from the public MCX master — no token needed.
    # NSE/BSE indices need the Upstox token for get_option_contracts.
    for _idx in cfg.monitored_indices:
        _is_mcx = _idx.upper() in _MCX_SET
        if not _is_mcx and not _upstox_token:
            continue
        try:
            await asyncio.wait_for(
                asyncio.to_thread(_instrument_registry.load_sync, _idx, _upstox_token),
                timeout=30.0,
            )
            _upstox_map = _instrument_registry.build_instrument_map(_idx)
            for _brokers_by_binding in router._brokers.values():
                for _broker in _brokers_by_binding.values():
                    if hasattr(_broker, "inject_instrument_map"):
                        _broker.inject_instrument_map(_upstox_map)
        except asyncio.TimeoutError:
            logging.getLogger(__name__).warning(
                "InstrumentRegistry: load [%s] timed out (30s) — skipping, will use constructed symbols", _idx
            )
        except Exception as _exc:
            logging.getLogger(__name__).warning(
                "InstrumentRegistry: failed to load [%s]: %s", _idx, _exc
            )
    if not _upstox_token:
        logging.getLogger(__name__).warning(
            "InstrumentRegistry: no Upstox token — using constructed symbols. "
            "Authenticate Upstox feeder via Admin > Feeder for exact instrument keys."
        )

    # ── Data-layer operational modules ────────────────────────────────────────
    rebalancer     = StrikeRebalancer(bus, cfg, feeder)
    # Give each manager the rebalancer so new books can pin their strikes / subscribe chains.
    for manager in managers.values():
        if hasattr(manager, "set_rebalancer"):
            manager.set_rebalancer(rebalancer)
    # Iron condor needs the full ATM chain — enable for all IC indices
    for _ic in _iron_condors:
        if hasattr(rebalancer, "enable_chain"):
            rebalancer.enable_chain(_ic._underlying)
    # Dedicated Upstox2 feeder for MCX (CrudeOil/Gold) option subscriptions + tick delivery.
    # Upstox1+Fyers handle NSE/BSE; Upstox2 handles MCX. Both publish to the same EventBus.
    _mcx_feeder = None  # Upstox1 handles MCX options (Upstox2 lacks MCX options data plan)
    # Wire DeltaFeeder to TrapBookManager — it propagates to all current + future BTC/ETH books.
    if trap_scanner_manager and hasattr(trap_scanner_manager, "set_delta_feeder"):
        trap_scanner_manager.set_delta_feeder(delta_chain._feeder)
        logger.info("DeltaFeeder wired to TrapBookManager for all crypto books.")
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
                feeder=feeder,
                risk_manager=risk_mgr,
                iron_condors=_iron_condors,
                straddle_manager=straddle_manager,
                straddle_bridge=straddle_bridge,
                trap_scanner_manager=trap_scanner_manager,
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

    # feeder.start() connects and spawns its own internal asyncio tasks, then
    # returns immediately — it is a setup coroutine, not a run loop.  Calling
    # it inside create_task() puts a already-completing task in the barrier,
    # which fires FIRST_COMPLETED ~50 ms after boot.  Await it here alongside
    # router.start() so the feeder is live before the barrier is entered.
    try:
        await router.start()
    except RuntimeError as exc:
        logger.critical("Startup aborted: %s", exc)
        print(f"\n\nFATAL: {exc}\n\nCheck broker credentials in the dashboard and retry.\n")
        raise SystemExit(1)
    await feeder.start()


    if "iron_condor" in _enabled_strats:
        for _ic in _iron_condors:
            _ic.start()
    # SellStraddle: the book manager spawns/starts one independent book per (client,binding,index)
    # deployment and keeps reconciling (auto-start on deploy). Started as a task below.
    # TrapScanner: same per-binding pattern via trap_scanner_manager (task below).

    # FnO Stock Monitor — intraday alert engine driven off the nightly scan file
    try:
        from strategies.fno_stock_monitor import FnoStockMonitor
        fno_monitor = FnoStockMonitor(bus, cfg, _shared_client_db)
        fno_monitor.warm_start()
        if feeder:
            fno_monitor.set_feeder(feeder)
        if dashboard is not None:
            dashboard.set_fno_monitor(fno_monitor)
        tasks_pre = [asyncio.create_task(fno_monitor.start(), name="fno_stock_monitor")]
    except ImportError as _exc:
        logger.warning("FnoStockMonitor not available: %s", _exc)
        tasks_pre = []

    # Admin console runs as a detached background task — its completion or any
    # internal stream error must NOT trigger the engine shutdown.  Only the
    # engine primitives below participate in the FIRST_COMPLETED barrier.
    admin_task = asyncio.create_task(admin.run(), name="admin_console")

    tasks = tasks_pre + [
        asyncio.create_task(candle_cache.run(),         name="candle_cache"),
        asyncio.create_task(option_matrix.run(),        name="option_matrix"),
    ]
    for name, manager in managers.items():
        if STRATEGY_REGISTRY[name].get("per_binding"):
            tasks.append(asyncio.create_task(manager.run(), name=f"{name}_books"))
    if delta_chain is not None:
        tasks.append(asyncio.create_task(delta_chain.run(), name="delta_chain"))
    async def _memory_watchdog() -> None:
        """Log RSS every 30 min and force a GC cycle. Logs a WARNING if RSS > 2.5 GB
        so we know well before the 4 GB t3.medium limit is approached."""
        try:
            import resource
            _have_resource = True
        except ImportError:
            _have_resource = False   # Windows — resource module not available
        while True:
            await asyncio.sleep(1800)   # 30 minutes
            gc.collect()
            bus_stats = {t: len(qs) for t, qs in bus._subs.items() if qs}
            if _have_resource:
                rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
                logger.info("MEMORY rss=%.0f MB | eventbus_queues=%s", rss_mb, bus_stats)
                if rss_mb > 2500:
                    logger.warning("MEMORY HIGH: %.0f MB RSS — approaching 4 GB limit. "
                                   "Consider restarting after market hours.", rss_mb)
            else:
                logger.info("MEMORY gc done | eventbus_queues=%s", bus_stats)

    tasks += [
        asyncio.create_task(router.run(),               name="router"),
        asyncio.create_task(straddle_bridge.run(),      name="straddle_bridge"),
        asyncio.create_task(ic_bridge.run(),            name="ic_bridge"),
        asyncio.create_task(client_mgr.run(),           name="client_mgr"),
        asyncio.create_task(risk_mgr.run(),             name="risk_mgr"),
        asyncio.create_task(rebalancer.run(),           name="rebalancer"),
        asyncio.create_task(strike_cleanup.run(),       name="strike_cleanup"),
        asyncio.create_task(gap_handler.run(),          name="gap_handler"),
        asyncio.create_task(shutdown_event.wait(),      name="shutdown_sentinel"),
        asyncio.create_task(_memory_watchdog(),         name="memory_watchdog"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for t in done:
        name = t.get_name()
        if t.cancelled():
            if name != "shutdown_sentinel":
                logger.warning("Task '%s' was cancelled unexpectedly.", name)
        else:
            exc = t.exception()
            if exc:
                logger.error("Task '%s' crashed: %s", name, exc, exc_info=exc)
            elif name != "shutdown_sentinel":
                # A task returning normally (no exception) also fires FIRST_COMPLETED.
                # Log it so the root cause is always visible in the shutdown trace.
                logger.warning("Task '%s' completed normally — triggered shutdown.", name)

    logger.info("Shutting down…")
    for manager in managers.values():
        if hasattr(manager, "stop_async"):
            await manager.stop_async()
    risk_mgr.stop()
    rebalancer.stop()
    strike_cleanup.stop()
    gap_handler.stop()
    straddle_bridge.stop()
    ic_bridge.stop()
    await router.stop()
    await client_mgr.stop()
    await admin.stop()   # stops console + dashboard server + cancels dashboard task
    await feeder.stop()

    # Cancel both the engine tasks and the detached admin task
    for t in list(pending) + [admin_task]:
        if not t.done():
            t.cancel()
    await asyncio.gather(*pending, admin_task, return_exceptions=True)
    logger.info("System stopped cleanly.")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    # ── Enforce secrets for live mode ─────────────────────────────────────────
    _enforce_secrets(args.mode)

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
    if args.mode in ("paper", "live"):
        from config.client_profiles import REGISTRY
        registry = REGISTRY

        if args.mode == "paper":
            _setup_default_client(registry, args.capital)
        else:
            _setup_live_clients(registry)

        asyncio.run(
            _run_live(
                cfg, registry, args.mode, args.index,
                ui=args.ui,
                ui_host=args.host,
                ui_port=args.port,
                strategies=args.strategies,
            )
        )

    else:
        logger.error("Unknown mode: %s (valid: paper, live)", args.mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
