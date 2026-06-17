"""
ui_layer/dashboard_server.py — FastAPI async web dashboard server with RBAC.

Endpoints:
  PUBLIC
    POST /api/auth/login              — issue JWT (admin or client role)

  ADMIN-ONLY  (Bearer admin JWT required)
    GET  /api/status                  — all client status summary
    GET  /api/workers                 — execution worker stats
    GET  /api/broker_status           — auth status for all broker bindings
    POST /api/halt/{client_id}        — halt a single client
    POST /api/resume/{client_id}      — resume a halted client
    POST /api/halt_all                — halt every client
    POST /api/kill_switch             — halt all + KILL_SWITCH event
    POST /api/rebalance/{index}       — force ATM rebalance for an underlying
    POST /api/set_lots/{cid}/{bid}/{m}— override lot multiplier
    POST /api/client/update_token     — inject new broker credentials + re-auth
    GET  /api/admin/feeder/status     — GlobalFeeder connection info
    POST /api/admin/feeder/connect    — switch / reconnect data feeder live

  CLIENT-ONLY  (Bearer client JWT required)
    GET  /api/client/me               — own portfolio snapshot
    GET  /api/client/brokers          — own broker binding status
    POST /api/client/register_broker  — provision + authenticate a new broker on-the-fly
    GET  /api/admin/feeder/auth-url?provider={p} — generate feeder OAuth login URL
    POST /api/admin/feeder/save-creds           — save feeder api_key + secret (no password/totp)

  COMMON (any valid JWT)
    WS   /ws?token=<jwt>              — live event stream

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from config.global_config import IST, Topic, SysEvent
from data_layer.base_feeder import EventBus
from ui_layer.auth import create_token, verify_token
from ui_layer.ws_bridge import WsBridge

logger = logging.getLogger(__name__)

from broker_auth.headless_auth import _ist_eod


def _base_url(request) -> str:
    """Return the public base URL, respecting X-Forwarded-Proto from nginx."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host   = request.headers.get("x-forwarded-host") or request.url.netloc
    host   = host.split(":")[0] if request.headers.get("x-forwarded-proto") else host
    return f"{scheme}://{host}"


async def _start_feeder_stream(feeder, provider: str, api_key: str, token: str, client_db=None) -> None:
    """
    Switch GlobalFeeder to the given live provider.

    If the OTHER provider also has a fresh token in DB, starts DualFeeder (active-active).
    Otherwise starts single-provider stream.
    Upstox is primary; Fyers is backup. Both run in parallel when available.
    """
    if feeder is None:
        logger.warning("[Feeder/Toggle] GlobalFeeder not wired — cannot start stream.")
        return

    from broker_auth.headless_auth import _token_is_fresh

    current_creds = {"api_key": api_key, "access_token": token}
    other = "fyers" if provider == "upstox" else "upstox"

    # Check if the other provider also has a valid token
    other_creds: dict = {}
    if client_db is not None:
        other_row = client_db.get_feeder_creds_sync(other) or {}
        other_token = other_row.get("access_token", "")
        other_api_key = other_row.get("api_key", "")
        other_gen_at = other_row.get("token_generated_at", "")
        other_exp_at = other_row.get("token_expiry_at", "")
        if other_token and _token_is_fresh(other_gen_at, other_exp_at):
            other_creds = {"api_key": other_api_key, "access_token": other_token}

    try:
        if other_creds:
            # Both providers have valid tokens → active-active DualFeeder
            upstox_creds = current_creds if provider == "upstox" else other_creds
            fyers_creds  = current_creds if provider == "fyers"  else other_creds
            await feeder.start_dual(upstox_creds, fyers_creds)
            logger.info("[Feeder/Toggle] DUAL stream started (upstox primary + fyers backup).")
        else:
            # Only this provider available → single stream
            await feeder.start_single(provider, current_creds)
            logger.info("[Feeder/Toggle] [%s] single stream started.", provider)
    except Exception as exc:
        logger.error("[Feeder/Toggle] [%s] stream start failed: %s", provider, exc)


def _redirect_base(request, client_db) -> str:
    """
    Return the base URL used as redirect_uri root for all broker OAuth flows.

    Priority:
      1. DB: system_settings.GLOBAL_REDIRECT_BASE  (admin-configured, persists)
      2. Fallback: derive from incoming request (handles nginx X-Forwarded-Proto)

    Set GLOBAL_REDIRECT_BASE in Admin Workspace → Data Feeder → Global Redirect Base.
    Must begin with https:// for brokers that enforce HTTPS redirect URIs (Upstox, Zerodha).
    """
    stored = client_db.get_setting_sync("GLOBAL_REDIRECT_BASE", "").strip().rstrip("/")
    if stored:
        return stored
    logger.warning(
        "[OAuth] GLOBAL_REDIRECT_BASE not set — falling back to request-derived URL. "
        "Configure it in Admin Workspace to ensure the correct redirect URI."
    )
    return _base_url(request)


def _callback_page(status: str, provider: str, message: str) -> str:
    """Return a minimal HTML page shown after broker OAuth redirect."""
    color   = "#22c55e" if status == "success" else "#ef4444"
    icon    = "✓" if status == "success" else "✗"
    title   = "Connected!" if status == "success" else "Authentication Failed"
    script  = (
        "if(window.opener){window.opener.postMessage({type:'broker_connected',provider:'" + provider + "'},\"*\");}"
        "setTimeout(()=>window.close(),3000);"
    ) if status == "success" else ""
    return f"""<!DOCTYPE html><html><head><title>TERMINUS — {provider.upper()}</title>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:monospace;background:#0a0a0f;color:#e2e8f0;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;padding:40px;border:1px solid {color}33;border-radius:12px;
background:{color}0a;max-width:400px}}
.icon{{font-size:48px;color:{color};margin-bottom:16px}}
h2{{color:{color};margin:0 0 12px}}
p{{color:#94a3b8;margin:8px 0}}
small{{color:#475569}}
</style></head><body>
<div class="box">
<div class="icon">{icon}</div>
<h2>{title}</h2>
<p>{provider.upper()} broker</p>
<p style="color:{color}">{message}</p>
<small>{"This tab will close automatically in 3 seconds." if status == "success" else "Please close this tab and try again."}</small>
</div>
<script>{script}</script>
</body></html>"""


def _upstox_translate_error(raw: str) -> str:
    """Map known Upstox error codes to actionable UI messages."""
    if "UDAPI100060" in raw:
        return (
            "Upstox Resource Not Found (UDAPI100060). Please check your settings: "
            "(1) Confirm your API Key is correct and has no hidden spaces or dashes mixed up, "
            "(2) Ensure the Redirect URI in your Upstox Developer Console is set EXACTLY to https://www.google.com, "
            "(3) Confirm the Mobile number field contains your 10-digit registered number (NOT your Client ID)."
        )
    return raw

# FastAPI imports at module level so 'from __future__ import annotations' doesn't
# prevent FastAPI from resolving type hints on route functions (lazy-string annotations
# are resolved against module globals, so locally-imported types are invisible).
try:
    from fastapi import (
        FastAPI, WebSocket, WebSocketDisconnect,
        HTTPException, Depends, Query, Body, Request,
    )
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from pydantic import BaseModel as _PydanticBase
    _HAS_FASTAPI = True

    # Pydantic schemas at module level so FastAPI can resolve type hints even
    # when 'from __future__ import annotations' makes all annotations lazy strings.
    class _LoginSchema(_PydanticBase):
        role:     str = ""
        username: str = ""
        password: str = ""

    class _TokenUpdateSchema(_PydanticBase):
        client_id:    str
        binding_id:   str
        access_token: Optional[str] = None
        api_key:      Optional[str] = None
        api_secret:   Optional[str] = None
        user_id:      Optional[str] = None

    class _FeederConnectSchema(_PydanticBase):
        provider: str

    class _SaveFeederCredsSchema(_PydanticBase):
        provider:  str
        client_id: str = ""
        api_key:   str = ""
        secret:    str = ""

    class _BrokerProvisionSchema(_PydanticBase):
        binding_id:     str
        provider:       str
        label:          str = ""
        user_id:        str = ""
        api_key:        str = ""
        api_secret:     str = ""
        access_token:   str = ""
        lot_multiplier: float = 1.0

    # _DualFeederSchema removed — replaced by _SaveFeederCredsSchema per-provider

    class _RmsConfigSchema(_PydanticBase):
        max_drawdown_pct:       float = 5.0
        order_throttle_per_sec: int   = 5
        squareoff_time:         str   = "15:15"
        distance_filter_pct:    float = 5.0

    class _StrategyConfigSchema(_PydanticBase):
        # Iron Condor (global fallback — per-index config preferred)
        ic_squareoff_time: str   = "15:15"
        ic_rsi_min:        float = 40.0
        ic_rsi_max:        float = 60.0
        ic_adx_max:        float = 25.0
        ic_profit_pct:     float = 50.0
        ic_sl_pct:         float = 200.0
        ic_nifty_otm:      float = 200.0
        ic_nifty_wing:     float = 200.0
        ic_banknifty_otm:  float = 400.0
        ic_banknifty_wing: float = 500.0
        ic_finnifty_otm:   float = 200.0
        ic_finnifty_wing:  float = 200.0
        ic_sensex_otm:     float = 500.0
        ic_sensex_wing:    float = 500.0
        ic_midcp_otm:      float = 150.0
        ic_midcp_wing:     float = 200.0
        # Sell Straddle (global fallback — per-index config preferred)
        ss_entry_start:    str   = "09:20"
        ss_entry_end:      str   = "12:00"
        ss_squareoff_time: str   = "15:15"
        ss_max_trades:     int   = 1
        # Liquidity Trap Trading (new 5-stage MTF engine)
        tt_htf_minutes:   int   = 75
        tt_ltf_minutes:   int   = 5
        tt_sl_mode:       str   = "dynamic"
        tt_sl_pct:        float = 2.0

    class _ChangeAdminPasswordSchema(_PydanticBase):
        current_password: str
        new_password:     str

    class _ResetPasswordSchema(_PydanticBase):
        token:        str
        new_password: str

    class _SystemSettingSchema(_PydanticBase):
        key:   str
        value: str

    class _KillAllConfirmSchema(_PydanticBase):
        confirm: bool = False   # must be True to proceed

    class _SaveUpstoxCredsSchema(_PydanticBase):
        client_id: str = ""
        api_key:   str = ""
        secret:    str = ""

    class _SaveFyersCredsSchema(_PydanticBase):
        client_id: str = ""
        app_key:   str = ""
        secret:    str = ""

    class _ClientRegisterSchema(_PydanticBase):
        client_id:          str
        name:               str   = ""
        capital:            float = 500_000.0
        provider:           str   = "mock"
        binding_id:         str   = ""
        lot_multiplier:     float = 1.0
        max_risk_pct:       float = 1.0
        max_daily_loss_pct: float = 3.0
        strategies:         List[str] = ["A", "B", "C"]

    # Phase 7 — multi-tenant lifecycle schemas
    class _ClientSelfRegisterSchema(_PydanticBase):
        client_id:          str
        name:               str   = ""
        email:              str   = ""
        password:           str
        capital:            float = 500_000.0
        max_risk_pct:       float = 1.0
        max_daily_loss_pct: float = 3.0

    class _ApproveClientSchema(_PydanticBase):
        strategy_assignments: Dict[str, str] = {}  # binding_id -> "A"/"B"/"C"

    class _AddPortalBrokerSchema(_PydanticBase):
        binding_id:          str
        provider:            str
        label:               str   = ""
        user_id:             str   = ""
        api_key:             str   = ""
        api_secret:          str   = ""
        password:            str   = ""
        totp_secret:         str   = ""
        lot_multiplier:      float = 1.0
        trading_mode:        str   = "paper"
        product_type:        str   = "MIS"    # "MIS" intraday | "NRML" carry-forward
        assigned_strategy:   str   = ""
        assigned_instrument: str   = "NIFTY"
        source_ip:           str   = ""        # bind API egress to this local IP (static-IP brokers)

    class _BrokerModeSchema(_PydanticBase):
        mode: str  # "paper" | "live"

    class _BindingIPSchema(_PydanticBase):
        source_ip:    str = ""   # LOCAL/private IP the bot binds order egress to
        whitelist_ip: str = ""   # PUBLIC IP the client whitelists in their broker

    class _OIWindowSchema(_PydanticBase):
        n: int = 0   # strikes each side of ATM for the OI panel (0 = all pool strikes)

    class _RunSchema(_PydanticBase):
        running: bool = False   # per-strategy Start/Stop toggle

    class _FyersAuthCodeSchema(_PydanticBase):
        auth_code: str

    class _SetIndexSchema(_PydanticBase):
        index: str  # NIFTY / BANKNIFTY / FINNIFTY

    class _StrategySelectionItem(_PydanticBase):
        strategy: str   # sell_straddle | iron_condor | trap_trading
        instrument: str # NIFTY | BANKNIFTY | FINNIFTY | SENSEX | MIDCPNIFTY

    class _StrategySelectionsSchema(_PydanticBase):
        selections: list  # List[_StrategySelectionItem]

    class _DeploymentSchema(_PydanticBase):
        binding_id:     str
        strategy_name:  str
        underlying:     str   = "NIFTY"
        lot_multiplier: float = 1.0
        max_profit_rs:  float = 0.0
        max_sl_rs:      float = 0.0
        squareoff_time: str   = "15:15"

    class _TrapConfigUpdateSchema(_PydanticBase):
        HTF_MINUTES:         Optional[int]   = None
        MTF_MINUTES:         Optional[int]   = None
        LTF_MINUTES:         Optional[int]   = None
        RETEST_ZONE_PERCENT: Optional[float] = None
        SLIPPAGE_BUFFER:     Optional[float] = None
        bars_lookback_days:  Optional[int]   = None
        SL_MODE:             Optional[str]   = None  # "dynamic" | "structural"
        SL_PCT:              Optional[float] = None  # % below entry (dynamic mode)
        SL_BUFFER_PCT:       Optional[float] = None  # % buffer below structural SL level
        ENTRY_CUTOFF_TIME:   Optional[str]   = None  # HH:MM — no new entries after this time

    class _TrapInstrumentsSchema(_PydanticBase):
        instruments: List[str]

    class _TrapReplaySchema(_PydanticBase):
        symbol:     str
        start_date: str   # ISO date string: "2026-05-01"
        end_date:   str   # ISO date string: "2026-05-31"

    class _TrapHistoricalReplaySchema(_PydanticBase):
        script:        str            # "NIFTY" | "BANKNIFTY"
        provider:      str = "upstox"
        backtest_date: str            # ISO date: "2026-05-29"
        capital:       float = 500_000.0
        lookback_days: int   = 2      # trading days before backtest_date to include

    class _AmoTestSchema(_PydanticBase):
        client_id:    str
        binding_id:   str
        underlying:   str = "NIFTY"
        expiry_pref:  str = "current_week"   # current_week | next_week | monthly
        strike:       Optional[int] = None   # None = ATM
        opt_type:     str = "CE"
        qty:          int = 1                # in lots (1 lot = exchange lot_size)

except ImportError:
    _HAS_FASTAPI = False

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_MONITOR_HTML = os.path.join(_TEMPLATE_DIR, "monitor.html")

# ─────────────────────────────────────────────────────────────────────────────
# Historical replay helpers  (pure functions — no I/O, no imports at module level)
# ─────────────────────────────────────────────────────────────────────────────

# NSE weekly expiry weekday per underlying (0=Mon … 6=Sun)
_WEEKLY_EXPIRY_WEEKDAY: Dict[str, int] = {
    "NIFTY":       1,   # Tuesday  (moved from Thursday Feb 2025)
    "BANKNIFTY":   2,   # Wednesday
    "FINNIFTY":    1,   # Tuesday
    "MIDCPNIFTY":  0,   # Monday
    "SENSEX":      1,   # Tuesday
}

_STRIKE_STEPS: Dict[str, int] = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "SENSEX": 100, "MIDCPNIFTY": 50,
}

_LOT_SIZES_MAP: Dict[str, int] = {
    "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60,
    "SENSEX": 20, "MIDCPNIFTY": 120,
}

_UPSTOX_INDEX_KEY: Dict[str, str] = {
    "NIFTY":       "NSE_INDEX|Nifty 50",
    "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
    "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY":  "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX":      "BSE_INDEX|SENSEX",
}


def _next_weekly_expiry(from_date, underlying: str):
    """
    Return the nearest active expiry on or after from_date.
    Always from InstrumentRegistry (real Upstox contract dates). Never calculated.
    Caller must ensure registry is loaded before calling this.
    """
    from data_layer.instrument_registry import REGISTRY as _REG
    exp = _REG.get_active_expiry(underlying, from_date)
    if exp:
        return exp
    raise RuntimeError(
        f"No active expiry found in registry for {underlying} from {from_date}. "
        "Load the registry first (call _REG.load_sync) before resolving expiry."
    )


def _prior_trading_day(d):
    """Return the last Mon–Fri before d (skips weekends)."""
    from datetime import timedelta
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:   # Saturday=5, Sunday=6
        prev -= timedelta(days=1)
    return prev


def _dte_itm_offset(dte: int, step: int) -> int:
    """
    Days-to-expiry → ITM strike offset (in index points).
    step is the strike step (50 for NIFTY, 100 for BANKNIFTY).
    """
    # Matrix: dte → offset in multiples of step
    if dte <= 0:
        return 2 * step    # 0 DTE  → 100 pts for NIFTY (2 × 50)
    elif dte == 1:
        return 4 * step    # 1 DTE  → 200 pts for NIFTY (4 × 50)
    elif dte == 2:
        return 6 * step    # 2 DTE  → 300 pts for NIFTY
    elif dte == 3:
        return 8 * step    # 3 DTE  → 400 pts for NIFTY
    else:
        return 10 * step   # 4+ DTE → 500 pts for NIFTY


_UPSTOX_MONTH_CODE: Dict[int, str] = {
    1:"1", 2:"2", 3:"3", 4:"4", 5:"5", 6:"6",
    7:"7", 8:"8", 9:"9", 10:"O", 11:"N", 12:"D",
}

# ── Instrument master cache (keyed by calendar date string, refreshed daily) ─
_UPSTOX_MASTER_CACHE: dict = {}   # date_str -> list[dict]


def _upstox_download_master_sync(date_str: str) -> list:
    """
    Download and cache the Upstox NSE instrument master JSON.
    File is ~10 MB gzipped; cached in memory for the day to avoid re-downloading.
    date_str is used only as a cache key (e.g. "2026-05-31").
    """
    import gzip, json
    from urllib.request import urlopen

    if date_str in _UPSTOX_MASTER_CACHE:
        return _UPSTOX_MASTER_CACHE[date_str]

    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    logger.info("Downloading Upstox NSE instrument master from %s ...", url)
    with urlopen(url, timeout=60) as resp:
        data = gzip.decompress(resp.read())
    instruments = json.loads(data)
    _UPSTOX_MASTER_CACHE[date_str] = instruments
    logger.info("Upstox master loaded: %d instruments", len(instruments))
    return instruments


def _find_upstox_instrument_key(
    instruments: list,
    underlying: str,
    expiry_date,
    strike: int,
    opt_type: str,
) -> str:
    """
    Scan the instrument master for the exact instrument_key matching
    underlying / expiry / strike / CE|PE.
    Returns empty string if not found.
    """
    target_expiry = expiry_date.isoformat() if hasattr(expiry_date, "isoformat") else str(expiry_date)
    for inst in instruments:
        if inst.get("segment") != "NSE_FO":
            continue
        if inst.get("instrument_type") != "OPT":
            continue
        if inst.get("underlying_symbol") != underlying:
            continue
        # Expiry may be "2026-06-02" or "2026-06-02T00:00:00+05:30"
        if str(inst.get("expiry", ""))[:10] != target_expiry:
            continue
        if abs(float(inst.get("strike_price", -1)) - strike) > 0.01:
            continue
        # Option type is embedded in trading_symbol (ends with CE or PE)
        ts = inst.get("trading_symbol", "")
        if ts.endswith(opt_type):
            return inst.get("instrument_key", "")
    return ""


def _upstox_option_key(underlying: str, expiry, strike: int, opt_type: str) -> str:
    """
    Upstox instrument key: NSE_FO|NIFTY{YY}{M}{DD}{strike}{CE/PE}
    YY = 2-digit year, M = single-char month code (1-9, O, N, D), DD = 2-digit day.
    Example: NIFTY Jun-25-2025 22000CE → NSE_FO|NIFTY2562522000CE
    """
    segment = "BSE_FO" if underlying == "SENSEX" else "NSE_FO"
    yy = expiry.strftime("%y")
    mc = _UPSTOX_MONTH_CODE[expiry.month]
    dd = expiry.strftime("%d")
    return f"{segment}|{underlying}{yy}{mc}{dd}{strike}{opt_type}"


def _fetch_upstox_candles_sync(access_token: str, instrument_key: str,
                               from_date_str: str, to_date_str: str) -> list:
    """
    Fetch 1-minute candles from Upstox historical API (synchronous).
    Returns list of dicts {timestamp, open, high, low, close, volume}.
    Raises upstox_client.rest.ApiException on broker error.
    """
    import upstox_client
    cfg = upstox_client.Configuration()
    cfg.access_token = access_token
    client = upstox_client.ApiClient(cfg)
    api = upstox_client.HistoryApi(client)
    resp = api.get_historical_candle_data1(
        instrument_key=instrument_key,
        interval="1minute",
        to_date=to_date_str,
        from_date=from_date_str,
        api_version="2.0",
    )
    candles = getattr(getattr(resp, "data", None), "candles", None) or []
    result = []
    for c in candles:
        # c = [timestamp_str, open, high, low, close, volume, oi]
        result.append({
            "timestamp": str(c[0]),
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": int(c[5] or 0),
        })
    return result


class DashboardServer:
    """
    FastAPI-based async admin/client dashboard with JWT RBAC.

    Two roles:
      admin  — full system view + all management actions
      client — context-locked to own sub-portfolio; can register new brokers live

    Usage:
        dashboard = DashboardServer(bus, cfg, registry,
                                    router=router, rebalancer=rebalancer,
                                    feeder=feeder)
        task = asyncio.create_task(dashboard.serve(host="0.0.0.0", port=5000))
        ...
        dashboard.stop()
        await task
    """

    def __init__(
        self,
        bus: EventBus,
        cfg,              # GlobalConfig
        registry,         # ClientRegistry
        router=None,      # ExecutionRouter
        rebalancer=None,  # StrikeRebalancer
        feeder=None,      # GlobalFeeder — optional, for admin feeder management
        trap_engine=None, # TrapTradingEngine — optional, for strategy telemetry
        risk_manager=None, # RiskManager — optional, for firm risk summary + kill-all
        iron_condors=None,  # List[IronCondorStrategy]
        sell_straddles=None, # List[SellStraddleStrategy] (legacy per-index; optional)
        straddle_manager=None, # StraddleBookManager — per-binding books (live list + find)
        straddle_bridge=None, # StraddleExecutionBridge — for per-broker square-off on Trade/Terminal OFF
        trap_scanner_manager=None,  # TrapBookManager — per-binding trap scanner books
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._registry = registry
        self._router = router
        self._rebalancer = rebalancer
        self._feeder = feeder
        self._trap_engine = trap_engine
        self._risk_manager = risk_manager
        self._iron_condors: list = iron_condors or []
        self._straddle_manager = straddle_manager
        self._sell_straddles_static: list = sell_straddles or []
        self._straddle_bridge = straddle_bridge
        self._trap_scanner_manager = trap_scanner_manager
        self._ws_bridge = WsBridge(bus, cfg=cfg)
        self._uvicorn_server = None

        from data_layer.client_db import ClientDB
        self._client_db = ClientDB()
        self._auth_alerts: dict = {"feeder": ""}
        # Tracks the most recent pending OAuth request per provider.
        # Used for brokers that don't return a state param (Zerodha).
        self._pending_auth: dict = {}

        # Register heartbeat providers
        self._ws_bridge.register_stats_provider("clients", self._client_summary)
        # PCR + max-OI over the subscribed pool strikes (computed from the WS option cache)
        self._ws_bridge.register_stats_provider("oi", self._ws_bridge.oi_summary)
        if router is not None:
            self._ws_bridge.register_stats_provider(
                "workers",
                lambda: self._router.worker_stats() if self._router else [],
            )
            self._ws_bridge.register_stats_provider("brokers", self._broker_summary)

        self._app = self._build_app()

        if not hasattr(cfg, "rms"):
            cfg.rms = {
                "max_drawdown_pct":       5.0,
                "order_throttle_per_sec": 5,
                "squareoff_time":         "15:15",
                "distance_filter_pct":    5.0,
            }

    @property
    def ws_bridge(self) -> WsBridge:
        return self._ws_bridge

    # ── FastAPI application ───────────────────────────────────────────────────

    def _build_app(self):
        if not _HAS_FASTAPI:
            raise ImportError(
                "FastAPI not installed. Run: pip install fastapi uvicorn[standard]"
            )

        app = FastAPI(
            title="TERMINUS — OptionChain AlgoTrader",
            description="Role-based real-time multi-tenant options trading monitor",
            version="2.0.0",
            docs_url="/api/docs",
        )
        _srv   = self
        bridge = self._ws_bridge

        def _refresh_live_pnl() -> None:
            """Refresh each client's _daily_pnl in REAL RUPEES (booked from History + running
            unrealized × lot). Delegates to the single source of truth so the header, admin
            dashboard and position panel all agree with the History tab."""
            _srv._compute_live_pnls()

        # ── Auth helpers ──────────────────────────────────────────────────────

        _bearer = HTTPBearer(auto_error=False)

        async def _current_user(
            creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
        ) -> dict:
            if not creds:
                raise HTTPException(status_code=401, detail="Authorization required.")
            try:
                return verify_token(creds.credentials)
            except ValueError as exc:
                raise HTTPException(status_code=401, detail=str(exc))

        async def _require_admin(user: dict = Depends(_current_user)) -> dict:
            if user.get("role") != "admin":
                raise HTTPException(status_code=403, detail="Admin access required.")
            return user

        async def _require_client(user: dict = Depends(_current_user)) -> dict:
            if user.get("role") != "client":
                raise HTTPException(status_code=403, detail="Client portal access required.")
            return user

        # ── Serve dashboard HTML ──────────────────────────────────────────────

        @app.get("/", include_in_schema=False)
        async def index():
            if not os.path.exists(_MONITOR_HTML):
                return HTMLResponse(
                    "<h1>monitor.html not found</h1>"
                    "<p>Expected at: " + _MONITOR_HTML + "</p>",
                    status_code=503,
                )
            return FileResponse(_MONITOR_HTML, media_type="text/html")

        # ── PUBLIC — Authentication ───────────────────────────────────────────

        @app.post("/api/auth/login", tags=["Auth"])
        async def login(request: Request):
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid JSON body.")
            auth_cfg = _srv._cfg.auth
            role     = str(raw.get("role")     or "").strip()
            username = str(raw.get("username") or "").strip()
            password = str(raw.get("password") or "")

            if not role or not username or not password:
                raise HTTPException(
                    status_code=400,
                    detail="username and password are required.",
                )

            if role == "admin":
                if not hmac.compare_digest(username, auth_cfg.admin_username):
                    raise HTTPException(status_code=401, detail="Invalid admin credentials.")
                # DB-stored hash takes precedence over env var (allows change-password without restart)
                stored_hash = _srv._client_db.get_admin_password_hash_sync() if _srv._client_db else ""
                if stored_hash:
                    from data_layer.client_db import verify_password as _vp
                    if not _vp(password, stored_hash):
                        raise HTTPException(status_code=401, detail="Invalid admin credentials.")
                else:
                    if not hmac.compare_digest(password, auth_cfg.admin_password):
                        raise HTTPException(status_code=401, detail="Invalid admin credentials.")
                token = create_token(username, "admin")
                return {"access_token": token, "token_type": "bearer", "role": "admin"}

            elif role == "client":
                # Try registry first (approved, active clients)
                client = _srv._registry.get(username) if _srv._registry else None
                if client is not None and client.active:
                    # Legacy clients use PIN; DB clients use password
                    pw_ok = _srv._client_db.verify_client_password(username, password)
                    if not pw_ok:
                        # Fall back to legacy PIN
                        expected = auth_cfg.client_pin(username)
                        if not hmac.compare_digest(password, expected):
                            raise HTTPException(status_code=401, detail="Invalid client credentials.")
                    token = create_token(username, "client", username)
                    return {
                        "access_token": token,
                        "token_type":   "bearer",
                        "role":         "client",
                        "client_id":    username,
                        "client_name":  getattr(client, "name", username),
                    }
                # Not in registry — check ClientDB (pending / newly registered)
                db_client = _srv._client_db.get_client_sync(username)
                if db_client is None:
                    raise HTTPException(status_code=401, detail="Client not found.")
                if not _srv._client_db.verify_client_password(username, password):
                    raise HTTPException(status_code=401, detail="Invalid client credentials.")
                token = create_token(username, "client", username)
                return {
                    "access_token": token,
                    "token_type":   "bearer",
                    "role":         "client",
                    "client_id":    username,
                    "client_name":  db_client.get("name", username),
                }

            raise HTTPException(status_code=400, detail="role must be 'admin' or 'client'.")

        @app.post("/api/admin/change-password", tags=["Admin"])
        async def admin_change_password(request: Request, body: _ChangeAdminPasswordSchema):
            _require_admin(request)
            current = body.current_password
            new_pwd = body.new_password
            if not current or not new_pwd:
                raise HTTPException(status_code=400, detail="current_password and new_password required.")
            if len(new_pwd) < 8:
                raise HTTPException(status_code=400, detail="new_password must be at least 8 characters.")
            # Verify current password (DB hash first, then env var)
            auth_cfg = _srv._cfg.auth
            stored_hash = _srv._client_db.get_admin_password_hash_sync() if _srv._client_db else ""
            from data_layer.client_db import verify_password as _vp, hash_password as _hp
            if stored_hash:
                ok = _vp(current, stored_hash)
            else:
                ok = hmac.compare_digest(current, auth_cfg.admin_password)
            if not ok:
                raise HTTPException(status_code=401, detail="Current password is incorrect.")
            await _srv._client_db.set_admin_password_hash(_hp(new_pwd))
            return {"ok": True, "message": "Admin password updated."}

        @app.post("/api/admin/system-settings", tags=["Admin"])
        async def set_system_setting(body: _SystemSettingSchema, _: dict = Depends(_require_admin)):
            if not _srv._client_db:
                raise HTTPException(status_code=503, detail="DB not available.")
            await _srv._client_db.set_setting(body.key, body.value)
            return {"ok": True}

        @app.post("/api/admin/client/{client_id}/reset-token", tags=["Admin"])
        async def generate_client_reset_token(client_id: str, request: Request):
            _require_admin(request)
            if not _srv._client_db:
                raise HTTPException(status_code=503, detail="DB not available.")
            token = await _srv._client_db.create_reset_token("client", client_id)
            return {"ok": True, "token": token, "expires_in": "24 hours",
                    "note": "Show this token to the client once. It cannot be retrieved again."}

        @app.post("/api/auth/reset-password", tags=["Auth"])
        async def reset_password(body: _ResetPasswordSchema):
            token   = body.token.strip()
            new_pwd = body.new_password
            if not token or not new_pwd:
                raise HTTPException(status_code=400, detail="token and new_password are required.")
            if len(new_pwd) < 8:
                raise HTTPException(status_code=400, detail="new_password must be at least 8 characters.")
            result = _srv._client_db.consume_reset_token_sync(token)
            if result is None:
                raise HTTPException(status_code=400, detail="Invalid or expired reset token.")
            target_role, target_id = result
            from data_layer.client_db import hash_password as _hp
            if target_role == "admin":
                await _srv._client_db.set_admin_password_hash(_hp(new_pwd))
            else:
                await _srv._client_db.set_client_password(target_id, _hp(new_pwd))
            return {"ok": True, "message": "Password updated. Please log in with your new password."}

        # ── ADMIN — read-only ─────────────────────────────────────────────────

        @app.get("/api/status", tags=["Admin"])
        async def api_status(_: dict = Depends(_require_admin)):
            return {"ts": datetime.now(IST).isoformat(), "clients": _srv._client_summary()}

        @app.get("/api/workers", tags=["Admin"])
        async def api_workers(_: dict = Depends(_require_admin)):
            stats = _srv._router.worker_stats() if _srv._router else []
            return {"workers": stats}

        @app.get("/api/broker_status", tags=["Admin"])
        async def api_broker_status(_: dict = Depends(_require_admin)):
            return {"ts": datetime.now(IST).isoformat(), "brokers": _srv._broker_summary()}

        # ── ADMIN — client lifecycle management ───────────────────────────────

        @app.post("/api/halt/{client_id}", tags=["Admin"])
        async def api_halt(client_id: str, _: dict = Depends(_require_admin)):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.halt()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_HALTED", "client_id": client_id, "reason": "web_dashboard",
            })
            logger.info("Dashboard: halted client %s.", client_id)
            return {"ok": True, "message": f"Client {client_id} halted."}

        @app.post("/api/resume/{client_id}", tags=["Admin"])
        async def api_resume(client_id: str, _: dict = Depends(_require_admin)):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.resume()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_RESUMED", "client_id": client_id,
            })
            return {"ok": True, "message": f"Client {client_id} resumed."}

        @app.post("/api/halt_all", tags=["Admin"])
        async def api_halt_all(_: dict = Depends(_require_admin)):
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {"event": "ALL_HALTED"})
            return {"ok": True, "message": "All clients halted."}

        @app.post("/api/kill_switch", tags=["Admin"])
        async def api_kill_switch(_: dict = Depends(_require_admin)):
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":   SysEvent.KILL_SWITCH,
                "message": "Emergency kill switch activated via web dashboard.",
                "ts":      datetime.now(IST).isoformat(),
            })
            logger.critical("Dashboard: KILL SWITCH activated.")
            return {"ok": True, "message": "KILL SWITCH activated — all clients halted immediately."}

        @app.post("/api/rebalance/{underlying}", tags=["Admin"])
        async def api_rebalance(underlying: str, _: dict = Depends(_require_admin)):
            underlying = underlying.upper()
            if _srv._rebalancer is None:
                raise HTTPException(503, "StrikeRebalancer not wired to dashboard.")
            state = _srv._rebalancer._state.get(underlying)
            if state is None:
                raise HTTPException(404, f"Unknown underlying: {underlying!r}")
            state.current_atm = None
            state.open_atm = None
            logger.info("Dashboard: rebalance triggered for %s.", underlying)
            return {"ok": True, "message": f"Rebalance triggered for {underlying}."}

        @app.post("/api/set_lots/{client_id}/{binding_id}/{multiplier}", tags=["Admin"])
        async def api_set_lots(
            client_id: str, binding_id: str, multiplier: float,
            _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            for b in client.broker_bindings:
                if b.binding_id == binding_id:
                    b.lot_multiplier = multiplier
                    return {"ok": True, "message": f"{client_id}/{binding_id} lot_multiplier → {multiplier}"}
            raise HTTPException(404, f"Binding {binding_id!r} not found for {client_id!r}.")

        @app.post("/api/client/update_token", tags=["Admin"])
        async def api_update_token(
            body: _TokenUpdateSchema, _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(body.client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {body.client_id!r} not found.")
            binding = next(
                (b for b in client.broker_bindings if b.binding_id == body.binding_id), None
            )
            if binding is None:
                raise HTTPException(404, f"Binding {body.binding_id!r} not found.")
            creds = {
                k: v for k, v in body.model_dump().items()
                if k not in ("client_id", "binding_id") and v is not None
            }
            if creds and _srv._registry:
                _srv._registry.inject_credentials(body.client_id, body.binding_id, **creds)
            broker = None
            if _srv._router:
                broker = (_srv._router._brokers.get(body.client_id) or {}).get(body.binding_id)
            if broker is None:
                raise HTTPException(503, "Broker worker not found — router not started.")
            ok = await broker.authenticate()
            event = "AUTH_SUCCESS" if ok else "AUTH_FAILED"
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": event, "client_id": body.client_id, "binding_id": body.binding_id,
            })
            if ok:
                logger.info("Dashboard: re-authenticated %s/%s.", body.client_id, body.binding_id)
                return {"ok": True, "message": f"Authenticated {body.client_id}/{body.binding_id}."}
            logger.warning("Dashboard: auth FAILED for %s/%s.", body.client_id, body.binding_id)
            raise HTTPException(502, f"Authentication failed for {body.client_id}/{body.binding_id}.")

        # ── ADMIN — feeder management ─────────────────────────────────────────

        @app.get("/api/admin/feeder/status", tags=["Admin"])
        async def api_feeder_status(_: dict = Depends(_require_admin)):
            from broker_auth.headless_auth import _token_is_fresh
            feeder   = _srv._feeder
            provider = (
                feeder.active_provider
                if feeder and hasattr(feeder, "active_provider")
                else _srv._cfg.primary_feeder_provider
            )
            connected = feeder.is_running if feeder else False
            dual_lat  = feeder.dual_latency if feeder and hasattr(feeder, "dual_latency") else {}
            dual_conn = (feeder._dual_feeder.provider_connected
                         if feeder and getattr(feeder, "_dual_feeder", None) else {})

            _ALL_PROVIDERS = ["upstox", "fyers", "zerodha", "dhan", "angelone", "aliceblue"]
            providers_status: dict = {}
            for p in _ALL_PROVIDERS:
                row = _srv._client_db.get_feeder_creds_sync(p) or {}
                creds_present = bool(row.get("client_id") or row.get("api_key") or row.get("access_token"))
                token_fresh   = _token_is_fresh(
                    row.get("token_generated_at", ""), row.get("token_expiry_at", "")
                ) if creds_present else False
                providers_status[p] = {
                    "creds_present":   creds_present,
                    "token_fresh":     token_fresh,
                    "latency_ms":      round(dual_lat.get(p, 0.0), 3),
                    "feed_connected":  dual_conn.get(p, False),
                }

            return {
                "ts":        datetime.now(IST).isoformat(),
                "provider":  provider,
                "connected": connected,
                # flat legacy keys for Upstox/Fyers (UI still reads these)
                "upstox_latency_ms":    providers_status["upstox"]["latency_ms"],
                "fyers_latency_ms":     providers_status["fyers"]["latency_ms"],
                "upstox_creds_present": providers_status["upstox"]["creds_present"],
                "fyers_creds_present":  providers_status["fyers"]["creds_present"],
                "upstox_token_fresh":   providers_status["upstox"]["token_fresh"],
                "fyers_token_fresh":    providers_status["fyers"]["token_fresh"],
                "upstox_connected":     providers_status["upstox"]["feed_connected"],
                "fyers_connected":      providers_status["fyers"]["feed_connected"],
                # full per-provider map
                "providers": providers_status,
            }

        @app.get("/api/admin/settings", tags=["Admin"])
        async def api_get_settings(_: dict = Depends(_require_admin)):
            """Return persisted system settings (currently only GLOBAL_REDIRECT_BASE)."""
            redirect_base = await asyncio.to_thread(
                _srv._client_db.get_setting_sync, "GLOBAL_REDIRECT_BASE", ""
            )
            return {"ok": True, "GLOBAL_REDIRECT_BASE": redirect_base}

        @app.post("/api/admin/settings", tags=["Admin"])
        async def api_save_settings(
            request: Request, _: dict = Depends(_require_admin),
        ):
            """Save system settings. Accepts JSON body with GLOBAL_REDIRECT_BASE."""
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            redirect_base = str(body.get("GLOBAL_REDIRECT_BASE", "")).strip().rstrip("/")
            await _srv._client_db.set_setting("GLOBAL_REDIRECT_BASE", redirect_base)
            logger.info("[Settings] GLOBAL_REDIRECT_BASE updated to: %s", redirect_base)
            return {
                "ok": True,
                "message": "Settings saved.",
                "GLOBAL_REDIRECT_BASE": redirect_base,
            }

        @app.get("/api/admin/feeder/auth-url", tags=["Admin"])
        async def api_feeder_auth_url(
            provider: str,
            request:  Request,
            _: dict = Depends(_require_admin),
        ):
            """
            Generate the broker OAuth login URL for the admin data feeder.
            Admin clicks this, browser opens broker login page.
            After login, broker redirects to /callback/{provider} and token is stored automatically.
            Requires feeder credentials (api_key + secret) to be saved first via /save-creds.
            """
            import time as _time
            from broker_auth.oauth_manager import generate_auth_url, build_state
            t0 = _time.monotonic()

            db_row   = _srv._client_db.get_feeder_creds_sync(provider) or {}
            api_key  = db_row.get("api_key", "")
            secret   = db_row.get("secret", "")
            user_id  = db_row.get("client_id", "")

            if not api_key:
                return {
                    "ok": False,
                    "error": (
                        f"No API key saved for '{provider}' feeder. "
                        "Save credentials via /api/admin/feeder/save-creds first."
                    ),
                }

            base_url     = _redirect_base(request, _srv._client_db)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("admin", "feeder", provider)

            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, provider, api_key, secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.error("[Feeder] Auth URL generation failed for %s in %.1fms: %s",
                             provider, elapsed, auth_url)
                return {"ok": False, "error": auth_url}

            # Store pending auth for providers that don't return state (e.g. Zerodha)
            _srv._pending_auth[provider] = {
                "role": "admin", "client_id": "feeder", "binding_id": provider,
                "api_key": api_key, "api_secret": secret,
            }
            logger.info("[Feeder] %s auth URL ready in %.1fms", provider.upper(), elapsed)
            return {"ok": True, "provider": provider, "auth_url": auth_url}

        @app.post("/api/admin/feeder/save-creds", tags=["Admin"])
        async def api_feeder_save_creds(
            body: _SaveFeederCredsSchema, _: dict = Depends(_require_admin),
        ):
            """
            Save admin feeder credentials (client_id, api_key, secret only).
            No passwords, PINs, or TOTP secrets — authentication happens via broker portal.
            """
            import time as _time
            t0 = _time.monotonic()
            if not body.provider:
                raise HTTPException(400, "provider is required.")
            await _srv._client_db.upsert_feeder_creds(
                provider=body.provider,
                client_id=body.client_id,
                api_key=body.api_key,
                secret=body.secret,
            )
            elapsed = (_time.monotonic() - t0) * 1000
            logger.info("[Feeder] Credentials saved for %s in %.1fms", body.provider.upper(), elapsed)
            return {
                "ok": True,
                "message": (
                    f"Credentials saved for {body.provider.upper()} feeder. "
                    "Use the auth-url endpoint to complete OAuth login."
                ),
            }

        @app.post("/api/admin/feeder/{provider}/connect", tags=["Admin"])
        async def api_feeder_provider_connect(
            provider: str,
            request:  Request,
            _: dict = Depends(_require_admin),
        ):
            """
            Toggle ON for admin feeder (Upstox or Fyers).
            Step 1: check cached token in DB → validate via API ping.
            Step 2: if invalid/missing → generate OAuth URL for browser redirect.
            """
            import time as _time
            from broker_auth.headless_auth import _token_is_fresh
            from broker_auth.oauth_manager import generate_auth_url, build_state, validate_token

            t0 = _time.monotonic()
            p  = provider.lower()
            if p not in {"upstox", "fyers"}:
                return {
                    "ok":    False,
                    "error": f"Unsupported feeder provider '{provider}'. Allowed: upstox, fyers.",
                }

            db_row  = _srv._client_db.get_feeder_creds_sync(p) or {}
            api_key = db_row.get("api_key", "")
            secret  = db_row.get("secret", "")
            user_id = db_row.get("client_id", "")
            token   = db_row.get("access_token", "")
            gen_at  = db_row.get("token_generated_at", "")
            exp_at  = db_row.get("token_expiry_at", "")

            if not api_key:
                return {
                    "ok":    False,
                    "error": (
                        f"No API key saved for '{p}'. "
                        "Click ⚙ to enter credentials first."
                    ),
                }

            logger.info(
                "[Feeder/Toggle] [%s] connect — api_key_present=%s token_present=%s",
                p, bool(api_key), bool(token),
            )

            # Step 1: cached token check
            if token and _token_is_fresh(gen_at, exp_at):
                valid = await asyncio.to_thread(validate_token, p, api_key, token)
                elapsed = (_time.monotonic() - t0) * 1000
                if valid:
                    # Token valid — start the live feeder stream
                    await _start_feeder_stream(_srv._feeder, p, api_key, token, _srv._client_db)
                    logger.info(
                        "[Feeder/Toggle] [%s] cached token valid → feeder started in %.1fms", p, elapsed,
                    )
                    return {
                        "ok":        True,
                        "connected": True,
                        "flow":      "cached",
                        "message":   f"{p.upper()} feeder connected and streaming.",
                    }
                logger.info(
                    "[Feeder/Toggle] [%s] cached token rejected in %.1fms", p, elapsed,
                )

            # Step 2: generate OAuth URL
            base_url     = _redirect_base(request, _srv._client_db)
            callback_url = f"{base_url}/callback/{p}"
            state        = build_state("admin", "feeder", p)

            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, p, api_key, secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.error(
                    "[Feeder/Toggle] [%s] auth URL failed in %.1fms: %s", p, elapsed, auth_url,
                )
                return {"ok": False, "error": auth_url}

            # Store pending auth for Zerodha (doesn't return state param)
            _srv._pending_auth[p] = {
                "role": "admin", "client_id": "feeder", "binding_id": p,
                "api_key": api_key, "api_secret": secret,
            }
            logger.info(
                "[Feeder/Toggle] [%s] OAuth URL ready in %.1fms → awaiting login", p, elapsed,
            )
            return {
                "ok":        False,
                "connected": False,
                "flow":      "oauth",
                "auth_url":  auth_url,
                "message":   f"Open the {p.upper()} login page to authenticate.",
            }

        @app.post("/api/admin/feeder/{provider}/disconnect", tags=["Admin"])
        async def api_feeder_provider_disconnect(
            provider: str,
            _: dict = Depends(_require_admin),
        ):
            """Toggle OFF for admin feeder — stops the active feeder for this provider."""
            p = provider.lower()
            if p not in {"upstox", "fyers"}:
                return {"ok": False, "error": f"Unsupported feeder provider '{p}'."}

            feeder = _srv._feeder
            if feeder is not None:
                try:
                    active = getattr(feeder, "active_provider", None)
                    if active in (p, "dual", None):
                        await feeder.stop()
                        logger.info("[Feeder/Toggle] [%s] feeder stopped (was: %s).", p, active)
                except Exception as exc:
                    logger.warning("[Feeder/Toggle] [%s] feeder stop raised: %s", p, exc)

            logger.info("[Feeder/Toggle] [%s] disconnect complete.", p)
            return {"ok": True, "message": f"{p.upper()} feeder disconnected."}

        @app.post("/api/admin/feeder/connect", tags=["Admin"])
        async def api_feeder_connect(
            body: _FeederConnectSchema, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")

            # Validate provider is known
            from data_layer.global_feeder import _FEEDER_REGISTRY
            if body.provider not in _FEEDER_REGISTRY:
                raise HTTPException(
                    400,
                    f"Unknown provider '{body.provider}'. Available: {list(_FEEDER_REGISTRY)}",
                )

            logger.info("Dashboard: feeder reconnect requested → provider=%s.", body.provider)
            old_provider = _srv._cfg.primary_feeder_provider

            # Stop current feeder, update provider, restart
            try:
                await feeder.stop()
            except Exception as exc:
                logger.warning("Dashboard: feeder stop raised: %s", exc)

            _srv._cfg.primary_feeder_provider = body.provider  # type: ignore[misc]

            # Inject optional credentials into the feeder registry or cfg before start
            # (Real broker feeders read these from their BrokerBinding or cfg at connect time)
            if body.user_id:
                _srv._cfg.__dict__.setdefault("_feeder_creds", {})
                _srv._cfg.__dict__["_feeder_creds"]["user_id"] = body.user_id
            if body.api_key:
                _srv._cfg.__dict__.setdefault("_feeder_creds", {})
                _srv._cfg.__dict__["_feeder_creds"]["api_key"] = body.api_key

            try:
                await feeder.start()
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event":   SysEvent.FEEDER_RESTORED,
                    "message": f"Feeder switched to '{body.provider}' via dashboard.",
                })
                logger.info("Dashboard: feeder started with provider=%s.", body.provider)
                return {"ok": True, "message": f"Feeder connected via '{body.provider}'."}
            except Exception as exc:
                logger.error("Dashboard: feeder connect failed: %s", exc)
                _srv._cfg.primary_feeder_provider = old_provider  # type: ignore[misc]
                raise HTTPException(502, f"Feeder connect failed: {exc}")

        # ── CLIENT — own portfolio ────────────────────────────────────────────

        @app.get("/api/client/me", tags=["Client"])
        async def api_client_me(user: dict = Depends(_require_client)):
            _refresh_live_pnl()
            cid = user.get("client_id", "")
            client = _srv._registry.get(cid) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {cid!r} not found.")
            brokers_status = [
                b for b in _srv._broker_summary() if b["client_id"] == cid
            ]
            return {
                "ts":        datetime.now(IST).isoformat(),
                "client":    _build_client_dict(client),
                "brokers":   brokers_status,
            }

        @app.get("/api/client/brokers", tags=["Client"])
        async def api_client_brokers(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            return {
                "ts":      datetime.now(IST).isoformat(),
                "brokers": [b for b in _srv._broker_summary() if b["client_id"] == cid],
            }

        @app.get("/api/client/broker/{binding_id}/account", tags=["Client"])
        async def api_client_broker_account(binding_id: str, user: dict = Depends(_require_client)):
            """Profile + funds + leverage from the LIVE broker (whatever the broker exposes).
            Generic across brokers — calls get_profile()/get_funds()/get_leverage() if present.
            Populated on Terminal-connect so the client sees their broker-side name, available funds
            and (crypto) leverage. Returns {} fields the broker doesn't support."""
            cid = user.get("client_id", "")
            broker = ((getattr(_srv._router, "_brokers", None) or {}).get(cid, {}) or {}).get(binding_id)
            if broker is None:
                return {"ok": False, "error": "Broker not connected — turn Terminal ON first."}
            out: dict = {"ok": True, "binding_id": binding_id,
                         "provider": getattr(getattr(broker, "_b", None), "provider", "")
                                     or getattr(broker, "provider", "")}
            try:
                if hasattr(broker, "get_profile"):
                    p = await broker.get_profile()
                    out["profile"] = {"name": p.get("name") or p.get("email") or p.get("user_name", ""),
                                      "email": p.get("email", ""), "id": p.get("id", "")}
            except Exception as exc:
                out["profile_error"] = str(exc)
            try:
                if hasattr(broker, "get_funds"):
                    out["funds"] = await broker.get_funds()
            except Exception as exc:
                out["funds_error"] = str(exc)
            try:
                if hasattr(broker, "get_leverage"):
                    # Leverage is per-product on Delta; report the current default if the broker
                    # tracks one, else leave for the per-product set call.
                    out["leverage"] = getattr(broker, "_leverage", None)
                    out["leverage_supported"] = True
            except Exception:
                out["leverage_supported"] = False
            return out

        @app.post("/api/client/broker/{binding_id}/leverage", tags=["Client"])
        async def api_client_broker_set_leverage(
            binding_id: str, body: dict, user: dict = Depends(_require_client),
        ):
            """Set leverage on a broker that supports it (Delta). body: {product_id, leverage}."""
            cid = user.get("client_id", "")
            broker = ((getattr(_srv._router, "_brokers", None) or {}).get(cid, {}) or {}).get(binding_id)
            if broker is None or not hasattr(broker, "set_leverage"):
                return {"ok": False, "error": "Broker does not support leverage."}
            try:
                ok = await broker.set_leverage(int(body.get("product_id")), float(body.get("leverage")))
                return {"ok": bool(ok), "leverage": body.get("leverage")}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        # ── CLIENT — live broker provisioning ────────────────────────────────

        @app.post("/api/client/register_broker", tags=["Client"])
        async def client_register_broker(
            payload: _BrokerProvisionSchema,
            user: dict = Depends(_require_client),
        ):
            from config.client_profiles import BrokerBinding
            from execution_bridge.base_broker import create_broker

            cid = user.get("client_id", "")
            client = _srv._registry.get(cid) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {cid!r} not found.")

            # 1. Build the binding
            binding = BrokerBinding(
                binding_id=payload.binding_id,
                provider=payload.provider,      # type: ignore[arg-type]
                label=payload.label,
                user_id=payload.user_id,
                password=payload.password,
                api_key=payload.api_key,
                api_secret=payload.api_secret,
                totp_secret=payload.totp_secret,
                vendor_code=payload.vendor_code,
                imei=payload.imei,
                client_code=payload.client_code,
                access_token=payload.access_token,
                lot_multiplier=payload.lot_multiplier,
            )

            # 2. Register binding in registry (raises ValueError if duplicate)
            try:
                _srv._registry.add_broker_binding(cid, binding)
            except ValueError as exc:
                raise HTTPException(409, str(exc))

            # 3. Create broker instance and authenticate
            try:
                broker = create_broker(binding, cid)
            except ValueError as exc:
                raise HTTPException(400, str(exc))

            ok = await broker.authenticate()
            if not ok:
                # Remove the binding if auth fails so state stays consistent
                client.broker_bindings = [
                    b for b in client.broker_bindings if b.binding_id != payload.binding_id
                ]
                raise HTTPException(
                    502,
                    f"Authentication failed for provider '{payload.provider}'. "
                    "Check credentials and try again.",
                )

            # 4. Inject into router broker map
            if _srv._router:
                _srv._router._brokers.setdefault(cid, {})[payload.binding_id] = broker

            # 5. Inject into existing worker OR spawn a new isolated worker
            if _srv._router:
                updated = _srv._router._pool.add_broker_to_worker(
                    cid, payload.binding_id, broker, payload.provider
                )
                if not updated:
                    # First broker for this client — create a dedicated worker task
                    from execution_bridge.parallel_worker_pool import ClientExecutionWorker
                    worker = ClientExecutionWorker(
                        client=client,
                        brokers=_srv._router._brokers[cid],
                        bus=_srv._bus,
                        cfg=_srv._cfg,
                    )
                    await _srv._router._pool.add_worker(worker)
                    logger.info("Dashboard: spawned new execution worker for client %s.", cid)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":      "BROKER_PROVISIONED",
                "client_id":  cid,
                "binding_id": payload.binding_id,
                "provider":   payload.provider,
            })
            logger.info("Dashboard: provisioned broker %s/%s (%s).",
                        cid, payload.binding_id, payload.provider)
            return {
                "ok":      True,
                "message": (
                    f"Broker '{payload.binding_id}' ({payload.provider}) "
                    f"authenticated and registered for client {cid}."
                ),
            }

        # ── PUBLIC — self-registration ───────────────────────────────────────

        @app.post("/api/auth/register", tags=["Auth"])
        async def api_self_register(body: _ClientSelfRegisterSchema):
            existing = _srv._client_db.get_client_sync(body.client_id)
            if existing is not None:
                raise HTTPException(409, f"Client ID '{body.client_id}' already exists.")
            if len(body.password) < 6:
                raise HTTPException(400, "Password must be at least 6 characters.")
            await _srv._client_db.register_client(
                client_id=body.client_id,
                name=body.name,
                email=body.email,
                password=body.password,
                capital=body.capital,
                max_risk_pct=body.max_risk_pct,
                max_daily_loss_pct=body.max_daily_loss_pct,
            )
            logger.info("Dashboard: self-registered client %s (pending approval).", body.client_id)
            return {"ok": True, "message": "Registration submitted. Awaiting admin approval."}

        # ── CLIENT — lifecycle status ─────────────────────────────────────────

        @app.get("/api/client/status", tags=["Client"])
        async def api_client_status(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            # Check registry first
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                # Refresh live P&L here too — the client's own header polls this endpoint,
                # not the firm dashboard, so without this _daily_pnl stays stale at 0 even
                # while a position runs (booked + unrealized).
                _refresh_live_pnl()
                bindings = _srv._client_db.get_bindings_safe_sync(cid)
                return {
                    "phase":               "active",
                    "client_id":           cid,
                    "name":                getattr(reg_client, "name", cid),
                    "is_admin_approved":   True,
                    "is_client_bot_active": getattr(reg_client, "is_client_bot_active", False),
                    "target_index":        getattr(reg_client, "target_index", "NIFTY"),
                    "capital":             float(reg_client.risk.capital),
                    "daily_pnl":           round(float(getattr(reg_client, "_daily_pnl", 0.0)), 2),
                    "tradeable":           reg_client.is_tradeable(),
                    "bindings":            bindings,
                }
            # Not in registry — check DB
            db_client = _srv._client_db.get_client_sync(cid)
            if db_client is None:
                raise HTTPException(404, "Client not found.")
            bindings = _srv._client_db.get_bindings_safe_sync(cid)
            phase = "pending" if bindings else "onboarding"
            if db_client.get("is_admin_approved"):
                phase = "active"
            return {
                "phase":               phase,
                "client_id":           cid,
                "name":                db_client.get("name", cid),
                "is_admin_approved":   bool(db_client.get("is_admin_approved", 0)),
                "is_client_bot_active": bool(db_client.get("is_client_bot_active", 0)),
                "target_index":        db_client.get("target_index", "NIFTY"),
                "capital":             float(db_client.get("capital", 500_000.0)),
                "daily_pnl":           0.0,
                "tradeable":           False,
                "bindings":            bindings,
            }

        # ── CLIENT — add broker binding (portal onboarding) ───────────────────

        @app.post("/api/client/add_broker", tags=["Client"])
        async def api_client_add_broker(
            body: _AddPortalBrokerSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            if not body.binding_id.strip():
                raise HTTPException(400, "binding_id is required.")

            # Guard: binding_id already taken by a *different* provider → reject
            existing_bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_safe_sync, cid
            )
            clash = next(
                (b for b in existing_bindings
                 if b["binding_id"] == body.binding_id
                 and b["provider"] != body.provider),
                None,
            )
            if clash:
                raise HTTPException(
                    400,
                    f"Binding ID '{body.binding_id}' is already used by "
                    f"{clash['provider'].upper()}. Choose a different Binding ID "
                    f"(e.g. '{body.provider}_main').",
                )

            # Is this a brand-new binding or an edit of an existing one?
            _is_new_binding = not any(
                b["binding_id"] == body.binding_id for b in existing_bindings
            )
            try:
                await _srv._client_db.upsert_binding(
                    client_id=cid,
                    binding_id=body.binding_id,
                    provider=body.provider,
                    label=body.label,
                    user_id=body.user_id,
                    api_key=body.api_key,
                    api_secret=body.api_secret,
                    password=body.password,
                    totp_secret=body.totp_secret,
                    access_token="",
                    lot_multiplier=body.lot_multiplier,
                    trading_mode=body.trading_mode,
                    product_type=body.product_type,
                    assigned_strategy=body.assigned_strategy,
                    assigned_instrument=body.assigned_instrument,
                    source_ip=body.source_ip,
                )
                # Only a BRAND-NEW broker starts with trade OFF. Editing an
                # existing binding (e.g. changing product to NRML) must NOT
                # silently disable trade/engine — upsert already preserves
                # engine_active/terminal_connected.
                if _is_new_binding:
                    await _srv._client_db.set_trade_enabled(cid, body.binding_id, False)
            except HTTPException:
                raise
            except Exception as exc:
                logger.error("Dashboard: add_broker DB error for %s: %s", cid, exc)
                raise HTTPException(500, f"Failed to save broker credentials: {exc}")

            logger.info("Dashboard: client %s added broker binding %s (%s).", cid, body.binding_id, body.provider)
            return {
                "ok": True,
                "message": (
                    f"Broker '{body.binding_id}' saved. "
                    "Click the Terminal toggle to authenticate via your broker's login page."
                ),
            }

        # ── CLIENT — start bot (pre-flight validation) ────────────────────────

        @app.post("/api/client/start_bot", tags=["Client"])
        async def api_client_start_bot(user: dict = Depends(_require_client)):
            from broker_auth.headless_auth import headless_engine as _he

            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is None:
                raise HTTPException(403, "Client not yet approved or not active.")
            if not reg_client.is_admin_approved:
                raise HTTPException(403, "Account not yet approved by admin.")

            # Pre-flight: headless authenticate all trade-enabled bindings
            bindings_db = _srv._client_db.get_bindings_sync(cid)
            failed_ids: List[str] = []
            for b in bindings_db:
                if not b.get("is_trade_enabled", 1):
                    continue
                try:
                    ok, msg, token = await _he.authenticate_binding(b, cid, _srv._client_db)
                    if ok and token:
                        # Mirror fresh token into in-memory binding
                        bb = next(
                            (x for x in reg_client.broker_bindings if x.binding_id == b["binding_id"]),
                            None,
                        )
                        if bb is not None:
                            bb.access_token = token
                    if not ok:
                        logger.warning(
                            "Dashboard: pre-flight auth failed [%s/%s]: %s",
                            cid, b["binding_id"], msg,
                        )
                        failed_ids.append(b["binding_id"])
                except Exception as exc:
                    logger.warning("Dashboard: pre-flight auth error for %s/%s: %s", cid, b["binding_id"], exc)
                    failed_ids.append(b["binding_id"])

            if failed_ids:
                raise HTTPException(
                    401,
                    f"Authentication failed for: {', '.join(failed_ids)}. "
                    "Token expired or invalid. Please update your access token and try again.",
                )

            # Activate bot flag
            reg_client.is_client_bot_active = True
            await _srv._client_db.upsert_client(cid, is_client_bot_active=1)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_BOT_STARTED", "client_id": cid,
            })
            logger.info("Dashboard: client %s started bot.", cid)
            return {"ok": True, "message": "Bot activated. Pre-flight authentication passed."}

        # ── CLIENT — stop bot ─────────────────────────────────────────────────

        @app.post("/api/client/stop_bot", tags=["Client"])
        async def api_client_stop_bot(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                reg_client.is_client_bot_active = False
                reg_client.halt("CLIENT_STOPPED_BOT")
            await _srv._client_db.upsert_client(cid, is_client_bot_active=0)
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_BOT_STOPPED", "client_id": cid,
            })
            logger.info("Dashboard: client %s stopped bot.", cid)
            return {"ok": True, "message": "Bot deactivated."}

        # ── CLIENT — per-broker trade toggle ──────────────────────────────────

        @app.post("/api/client/set_trade/{binding_id}", tags=["Client"])
        async def api_client_set_trade(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            # Read current state and toggle
            bindings = _srv._client_db.get_bindings_safe_sync(cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            new_state = not bool(b.get("is_trade_enabled", 1))
            # Trade ON requires the broker terminal to be connected first.
            if new_state and not b.get("terminal_connected"):
                return {"ok": False, "binding_id": binding_id,
                        "error": "Terminal not connected. Switch Terminal ON first."}

            await _srv._client_db.set_trade_enabled(cid, binding_id, new_state)
            # Trade toggle ALSO drives the execution engine: ON → engine_active so
            # orders actually route to this broker (the bridge gates on engine_active);
            # OFF → engine_active off. This matches the user model: Trade = "ready to
            # trade, orders can be sent here". Deploy only selects strategies.
            await _srv._client_db.set_engine_active(cid, binding_id, new_state)

            if not new_state and _srv._straddle_bridge is not None:
                try:
                    n = await _srv._straddle_bridge.square_off_binding(cid, binding_id, _srv._sell_straddles)
                    logger.info("Dashboard: Trade OFF %s/%s — squared off %d leg(s).", cid, binding_id, n)
                except Exception as exc:
                    logger.error("Dashboard: square-off on Trade OFF failed for %s/%s: %s", cid, binding_id, exc)

            if new_state:
                # Hot-apply every saved deployment for this binding so lot/squareoff
                # are live immediately when the engine comes up.
                try:
                    from data_layer.deployment_store import (
                        load_deployment_json, apply_deployment_to_runtime_config,
                    )
                    deployments = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
                    for dep in deployments:
                        if dep.get("binding_id") != binding_id:
                            continue
                        did = f"{cid}_{binding_id}_{dep.get('strategy_name')}_{dep.get('underlying')}"
                        dj = load_deployment_json(did)
                        if dj:
                            apply_deployment_to_runtime_config(dj)
                except Exception as exc:
                    logger.warning("set_trade: deployment apply failed for %s/%s: %s", cid, binding_id, exc)

            # Mirror in in-memory profile
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client:
                for rb in reg_client.broker_bindings:
                    if rb.binding_id == binding_id:
                        rb.is_trade_enabled = new_state
            logger.info("Dashboard: Trade toggle %s/%s → %s (engine_active=%s)",
                        cid, binding_id, "ON" if new_state else "OFF", new_state)
            return {"ok": True, "binding_id": binding_id, "is_trade_enabled": new_state,
                    "engine_active": new_state}

        # ── CLIENT — per-strategy Start/Stop toggle (the new control surface) ──
        @app.post("/api/client/deployment/{deploy_id}/run", tags=["Client"])
        async def api_client_deployment_run(
            deploy_id: str, body: _RunSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            logger.info("[StrategyRun] request cid=%s deploy_id=%s running=%s", cid, deploy_id, body.running)
            deps = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
            dep = next((d for d in deps if d.get("deploy_id") == deploy_id), None)
            if dep is None:
                logger.warning("[StrategyRun] deploy_id NOT FOUND for cid=%s — available=%s",
                               cid, [d.get("deploy_id") for d in deps])
                raise HTTPException(404, f"Deployment '{deploy_id}' not found.")
            bid   = dep.get("binding_id", "")
            strat = dep.get("strategy_name", "")
            und   = str(dep.get("underlying", "") or "NIFTY")
            running = bool(body.running)
            # Start requires the broker terminal to be connected (app authenticated to broker).
            if running:
                binds = _srv._client_db.get_bindings_safe_sync(cid)
                b = next((x for x in binds if x["binding_id"] == bid), None)
                if not b or not b.get("terminal_connected"):
                    return {"ok": False, "deploy_id": deploy_id,
                            "error": "Terminal not connected. Switch the broker's Terminal ON first."}
                # Hot-apply this deployment's saved config so lot/squareoff are live immediately.
                try:
                    from data_layer.deployment_store import load_deployment_json, apply_deployment_to_runtime_config
                    dj = load_deployment_json(deploy_id)
                    if dj:
                        apply_deployment_to_runtime_config(dj)
                except Exception as exc:
                    logger.warning("deployment_run apply failed for %s: %s", deploy_id, exc)
            # ORDER MATTERS: square off FIRST (while book still alive), THEN set is_running=False.
            # Flipping is_running first lets StraddleBookManager's 5s reconcile tear the book down
            # before square_off_binding finds it → exchange legs left open, nothing in history.
            squared = 0
            if not running and strat == "sell_straddle" and _srv._straddle_bridge is not None:
                try:
                    squared = await _srv._straddle_bridge.square_off_binding(
                        cid, bid, _srv._sell_straddles, underlying=und)
                except Exception as exc:
                    logger.error("deployment_run square-off failed for %s: %s", deploy_id, exc)
            await _srv._client_db.set_deployment_running(deploy_id, cid, running)
            logger.info("Dashboard: strategy RUN %s → %s (squared=%d)",
                        deploy_id, "ON" if running else "OFF", squared)
            return {"ok": True, "deploy_id": deploy_id, "running": running, "squared_off": squared}

        # ── CLIENT — global STOP & SQUARE-OFF (flatten the whole client) ──────
        @app.post("/api/client/stop_squareoff", tags=["Client"])
        async def api_client_stop_squareoff(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            # Flatten every binding's legs FIRST, THEN stop the deployments — if we toggle the run
            # flags off first, StraddleBookManager's reconcile can remove the books before we square
            # them off and we close NOTHING on the exchange (orphaning real open legs).
            squared = 0
            if _srv._straddle_bridge is not None:
                try:
                    for b in _srv._client_db.get_bindings_safe_sync(cid):
                        squared += await _srv._straddle_bridge.square_off_binding(
                            cid, b.get("binding_id", ""), _srv._sell_straddles)
                except Exception as exc:
                    logger.error("stop_squareoff failed for %s: %s", cid, exc)
            await _srv._client_db.stop_all_deployments(cid)          # all run toggles OFF
            logger.info("Dashboard: STOP & SQUARE-OFF %s — squared %d leg(s) across all brokers.",
                        cid, squared)
            return {"ok": True, "squared_off": squared}

        # ── ADMIN — toggle per-client granular tick-by-tick exit audit ────────
        @app.post("/api/admin/client/{client_id}/binding/{binding_id}/granular_ticks",
                  tags=["Admin"])
        async def api_admin_set_granular_ticks(
            client_id: str, binding_id: str, _: dict = Depends(_require_admin),
        ):
            bindings = _srv._client_db.get_bindings_safe_sync(client_id)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            new_state = not bool(b.get("show_granular_ticks", 0))
            await _srv._client_db.set_show_granular_ticks(client_id, binding_id, new_state)
            logger.info("Dashboard: granular-ticks toggle %s/%s → %s",
                        client_id, binding_id, "ON" if new_state else "OFF")
            return {"ok": True, "client_id": client_id, "binding_id": binding_id,
                    "show_granular_ticks": new_state}

        @app.post("/api/admin/client/{client_id}/binding/{binding_id}/source_ip",
                  tags=["Admin"])
        async def api_admin_set_binding_ips(
            client_id: str, binding_id: str, body: _BindingIPSchema,
            _: dict = Depends(_require_admin),
        ):
            """Admin assigns a dedicated egress IP to a client's broker binding.
            source_ip = LOCAL/private interface IP the bot binds orders to;
            whitelist_ip = PUBLIC IP shown to the client to add to their broker whitelist."""
            bindings = _srv._client_db.get_bindings_safe_sync(client_id)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            await _srv._client_db.set_binding_ips(
                client_id, binding_id, body.source_ip, body.whitelist_ip)
            # Hot-swap the live broker's egress binding if it's already authenticated.
            try:
                brk = (_srv._router._brokers.get(client_id) or {}).get(binding_id) if _srv._router else None
                if brk is not None and hasattr(brk, "_binding"):
                    brk._binding.source_ip = body.source_ip.strip()
            except Exception:
                pass
            logger.info("Dashboard: set binding IPs %s/%s source=%s whitelist=%s",
                        client_id, binding_id, body.source_ip, body.whitelist_ip)
            return {"ok": True, "client_id": client_id, "binding_id": binding_id,
                    "source_ip": body.source_ip.strip(), "whitelist_ip": body.whitelist_ip.strip()}

        @app.post("/api/admin/oi_window", tags=["Admin"])
        async def api_admin_set_oi_window(body: _OIWindowSchema, _: dict = Depends(_require_admin)):
            """Set the OI panel window = strikes each side of ATM (0 = all pool strikes)."""
            _srv._ws_bridge.set_oi_window(body.n)
            logger.info("Dashboard: OI window set to ±%d strikes", int(body.n))
            return {"ok": True, "window": int(max(0, body.n))}

        # ── CLIENT — 1-min combined-premium chart series (VWAP/RSI/SLOPE) ─────
        @app.get("/api/client/strategy/{deploy_id}/premium_series", tags=["Client"])
        async def api_client_premium_series(
            deploy_id: str, _: dict = Depends(_require_client),
        ):
            """1-min combined CE+PE premium series with broker-VWAP/RSI/SLOPE overlays for
            the client straddle chart. deploy_id = {client}_{binding}_{strategy}_{underlying};
            the underlying (last token) selects the per-underlying strategy instance."""
            underlying = deploy_id.rsplit("_", 1)[-1].upper()
            strat = next((s for s in (getattr(_srv, "_sell_straddles", []) or [])
                          if str(getattr(s, "_underlying", "")).upper() == underlying), None)
            if strat is None or not hasattr(strat, "get_premium_series"):
                return {"ok": True, "deploy_id": deploy_id, "underlying": underlying, "series": []}
            return {"ok": True, "deploy_id": deploy_id, "underlying": underlying,
                    "series": strat.get_premium_series()}

        # ── CLIENT — set target index ─────────────────────────────────────────

        @app.post("/api/client/set_index", tags=["Client"])
        async def api_client_set_index(
            body: _SetIndexSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "CRUDEOIL", "BTC", "ETH"}
            idx = body.index.upper()
            if idx not in allowed:
                raise HTTPException(400, f"Invalid index. Allowed: {sorted(allowed)}")
            await _srv._client_db.upsert_client(cid, target_index=idx)
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                reg_client.target_index = idx
            return {"ok": True, "target_index": idx}

        # ── CLIENT — strategy selections ─────────────────────────────────────

        @app.get("/api/client/strategy_selections", tags=["Client"])
        async def api_client_get_selections(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            db_client = _srv._client_db.get_client_sync(cid)
            if db_client is None:
                raise HTTPException(404, "Client not found.")
            import json as _json
            raw = db_client.get("strategy_selections", "[]") or "[]"
            try:
                selections = _json.loads(raw)
            except Exception:
                selections = []
            return {"ok": True, "selections": selections}

        @app.post("/api/client/strategy_selections", tags=["Client"])
        async def api_client_save_selections(
            body: _StrategySelectionsSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            allowed_strategies = {"sell_straddle", "iron_condor", "trap_trading"}
            allowed_instruments = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "CRUDEOIL"}
            import json as _json
            validated = []
            for item in (body.selections or []):
                s = item.get("strategy", "") if isinstance(item, dict) else getattr(item, "strategy", "")
                ins = item.get("instrument", "") if isinstance(item, dict) else getattr(item, "instrument", "")
                if s in allowed_strategies and ins.upper() in allowed_instruments:
                    validated.append({"strategy": s, "instrument": ins.upper()})
            await _srv._client_db.upsert_client(cid, strategy_selections=_json.dumps(validated))
            return {"ok": True, "saved": len(validated)}

        # ── CLIENT — positions (live open legs) ───────────────────────────────

        @app.get("/api/client/positions", tags=["Client"])
        async def api_client_positions(user: dict = Depends(_require_client)):
            """
            Live open legs per (binding, strategy), read from the GLOBAL strategy
            instances (_iron_condors / _sell_straddles / _trap_engine). Positions
            are engine-level (per underlying); we surface them under each of the
            client's deployments so the portal shows the strikes being traded.
            """
            cid = user.get("client_id", "")
            try:
                deployments = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
            except Exception:
                deployments = []

            def _ic_legs(pos, product="NRML"):
                out = []
                for leg in (pos.short_ce, pos.short_pe, pos.long_ce, pos.long_pe):
                    strike = int(getattr(leg, "strike", 0))
                    if strike <= 0:
                        continue
                    side = getattr(leg, "side", "")
                    ot   = getattr(leg, "option_type", "")
                    ep   = float(getattr(leg, "entry_price", 0.0) or 0.0)
                    ltp  = float(getattr(leg, "ltp", ep) or ep)
                    ls   = int(getattr(pos, "lot_size", 0) or 0)
                    qty  = ls * (-1 if side == "sell" else 1)
                    # sell profits when price falls; buy profits when price rises
                    pnl = round((ep - ltp) * abs(qty), 2) if side == "sell" else round((ltp - ep) * abs(qty), 2)
                    out.append({"symbol": f"{pos.underlying} {strike}{ot} {side.upper()}",
                                "instrument": f"{pos.underlying} {strike} {ot}",
                                "type": product, "side": side.upper(),
                                "qty": qty, "lot_size": ls, "lots": 1,
                                "entry_price": round(ep, 2),
                                "sell_avg": round(ep, 2) if side == "sell" else 0.0,
                                "buy_avg":  round(ep, 2) if side != "sell" else 0.0,
                                "ltp": round(ltp, 2), "pnl": pnl, "mtm": pnl})
                return out

            def _ccy_cv(underlying):
                """(currency_symbol, contract_value) per exchange. Crypto (Delta) P&L is in USD and
                each contract is a fraction of a coin (BTC 0.001, ETH 0.01), so the premium-points
                P&L must be scaled by the contract value to match the Delta app. NSE/MCX = ₹, ×1."""
                u = str(underlying).upper()
                if u == "BTC":
                    return ("$", 0.001)
                if u == "ETH":
                    return ("$", 0.01)
                return ("₹", 1.0)

            def _ss_legs(pos, product="MIS"):
                out = []
                ccy, cv = _ccy_cv(pos.underlying)
                _crypto = str(pos.underlying).upper() in ("BTC", "ETH")
                for leg in (pos.ce_leg, pos.pe_leg):
                    strike = int(getattr(leg, "strike", 0))
                    if strike <= 0:
                        continue
                    ot = getattr(leg, "option_type", "")
                    ep = float(getattr(leg, "entry_price", 0.0) or 0.0)
                    ltp = float(getattr(leg, "ltp", ep) or ep)
                    # Crypto P&L is valued at the MARK (fair value), not the noisy last-trade LTP, so it
                    # matches the Delta app (a stale wide LTP can flip a leg's sign vs reality).
                    _mark = float(getattr(leg, "mark", 0.0) or 0.0)
                    _val = _mark if (_crypto and _mark > 0) else ltp
                    ls = int(getattr(pos, "lot_size", 0) or 0)
                    qty = -ls  # straddle is short both
                    _pnl = round((ep - _val) * abs(qty) * cv, 2)
                    # Extract expiry from leg.symbol (Delta: C-BTC-64000-140626 → 14JUN26)
                    _sym = str(getattr(leg, "symbol", "") or "")
                    _exp_lbl = ""
                    _parts = _sym.split("-")
                    if len(_parts) == 4 and len(_parts[3]) == 6 and _parts[3].isdigit():
                        _dd, _mm, _yy = _parts[3][:2], _parts[3][2:4], _parts[3][4:]
                        _mons = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
                        try:
                            _exp_lbl = f"{int(_dd)}{_mons[int(_mm)-1]}{_yy}"
                        except Exception:
                            pass
                    _instr = f"{pos.underlying} {strike} {ot}" + (f" {_exp_lbl}" if _exp_lbl else "")
                    out.append({"symbol": f"{pos.underlying} {strike}{ot} SELL",
                                "instrument": _instr,
                                "type": product, "side": "SELL", "ccy": ccy,
                                "qty": qty, "lot_size": ls, "lots": 1,
                                "entry_price": round(ep, 2),
                                "sell_avg": round(ep, 2), "buy_avg": 0.0,
                                "ltp": round(_val, 2), "pnl": _pnl, "mtm": _pnl})
                return out

            def _find(strategies, underlying):
                for s in strategies or []:
                    if getattr(s, "_underlying", None) == underlying:
                        return s
                return None

            by_broker: dict = {}
            for dep in deployments:
                bid = dep.get("binding_id", "")
                sname = dep.get("strategy_name", "")
                underlying = dep.get("underlying") or dep.get("assigned_instrument") or "NIFTY"
                by_broker.setdefault(bid, {})
                legs = []
                tracking = None
                straddle_info = None   # sell_straddle: total sold, exit-basis, LTP/Theta triplets
                booked = 0.0   # session realized P&L (₹) — straddle re-entries/rolls booked today
                try:
                    if sname == "iron_condor":
                        strat = _find(getattr(_srv, "_iron_condors", []), underlying)
                        pos = getattr(strat, "_position", None) if strat else None
                        if pos and getattr(pos, "status", "open") == "open":
                            try:
                                from data_layer.runtime_config import RuntimeConfig as _RC2
                                _icp = str(_RC2.index_section(underlying, "iron_condor").get("product_type", "NRML")).upper()
                            except Exception:
                                _icp = "NRML"
                            legs = _ic_legs(pos, product=_icp if _icp in ("MIS", "NRML") else "NRML")
                    elif sname == "sell_straddle":
                        strat = _srv._find_ss_book(cid, bid, underlying)
                        pos = getattr(strat, "_position", None) if strat else None
                        # Booked = sum of TODAY's closed-trade P&L from the History ledger (the
                        # source of truth shown in the History tab; survives restarts). Falls back
                        # to the in-memory session counter only if History is unavailable.
                        _ls = int(_srv._cfg.exchange.lot_sizes.get(underlying, 0) or 0)
                        try:
                            from data_layer import trade_history as _th
                            _today = datetime.now(IST).date().isoformat()
                            _recs = _th.load(cid, 500)
                            booked = round(sum(
                                float(r.get("pnl", 0) or 0) for r in _recs
                                if str(r.get("ts", ""))[:10] == _today
                                and r.get("strategy") == "sell_straddle"
                                and str(r.get("instrument", "")).upper() == str(underlying).upper()
                                and str(r.get("binding_id", "")) == bid
                            ), 2)
                        except Exception:
                            booked = round(float(getattr(strat, "_session_realized_pnl_pts", 0.0) or 0.0) * _ls, 2)
                        if pos and getattr(pos, "status", "open") == "open":
                            try:
                                from data_layer.runtime_config import RuntimeConfig as _RC2
                                _ssp = str(_RC2.index_section(underlying, "sell_straddle").get("product_type", "MIS")).upper()
                            except Exception:
                                _ssp = "MIS"
                            legs = _ss_legs(pos, product=_ssp if _ssp in ("MIS", "NRML") else "MIS")
                        # Always surface today's exit basis; add LTP/Theta triplets when open.
                        if strat is not None:
                            _basis = str(getattr(strat, "_day_exit_basis", "ltp")).lower()
                            _dpt = float(getattr(strat, "_day_profit_target_pct", 0.0) or 0.0)
                            _dsl = float(getattr(strat, "_day_loss_sl_pct", 0.0) or 0.0)
                            straddle_info = {"exit_basis": _basis, "day_target_pct": _dpt,
                                             "day_sl_pct": _dsl, "total_value_sold": 0.0,
                                             "ltp": None, "theta": None, "cooldown": None}
                            # Re-entry cooldown after a full exit: surface the lift time + seconds
                            # left so the client can see entry is paused while data keeps flowing.
                            try:
                                _cd = getattr(strat, "_sl_cooldown_until", None)
                                if _cd is not None:
                                    from config.global_config import IST as _IST
                                    _nowi = datetime.now(_IST)
                                    if _cd > _nowi:
                                        _strikes = len(getattr(getattr(strat, "_pool_engine", None), "_closes", {}) or {})
                                        straddle_info["cooldown"] = {
                                            "until": _cd.strftime("%H:%M:%S"),
                                            "secs_left": int((_cd - _nowi).total_seconds()),
                                            "strikes_tracked": _strikes,
                                        }
                            except Exception:
                                pass
                            if pos and getattr(pos, "status", "open") == "open":
                                _spot   = float(getattr(strat, "_spot", 0.0) or 0.0)
                                _entryC = float(pos.ce_leg.entry_price + pos.pe_leg.entry_price)
                                _curC   = float((getattr(pos.ce_leg, "ltp", 0) or 0) +
                                                (getattr(pos.pe_leg, "ltp", 0) or 0))
                                _credit = float(getattr(strat, "_initial_net_credit", 0.0) or pos.net_credit or 0.0)
                                _run    = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
                                _real   = float(getattr(strat, "_session_realized_pnl_pts", 0.0) or 0.0)
                                _ltp_pct = ((_real + _run) / _credit * 100.0) if _credit else 0.0
                                try:
                                    # Theta basis (user spec): baseline = TOTAL THETA RECEIVED at
                                    # entry (entry_time_value; == premium for an ATM straddle).
                                    # Fixed thresholds = entry_theta × day%. Track premium decay.
                                    _eTV = float(getattr(pos, "entry_time_value", 0.0) or 0.0) or float(pos.net_credit or 0.0)
                                    _cTV = float(pos.current_value)              # full LTP premium (for display)
                                    _cTV_theta = float(pos.current_time_value(_spot))  # time-value only (for theta decay)
                                    _tPct = float(pos.premium_decay_pct())
                                except Exception:
                                    _eTV = _cTV = _cTV_theta = _tPct = 0.0
                                # Use _initial_entry_time_value (max across re-entries) as denominator
                                _init_etv = float(getattr(strat, "_initial_entry_time_value", 0.0) or 0.0) or _eTV
                                _lot_sz  = int(getattr(strat, "_lot_size", 1) or 1)
                                _lot_mul = int(getattr(strat, "_lot_multiplier", 1) or 1)
                                _qty     = _lot_sz * _lot_mul
                                # Contract value: BTC=0.001, ETH=0.01, NSE=1.0
                                _und_u = str(pos.underlying).upper()
                                _cv = 0.001 if _und_u == "BTC" else (0.01 if _und_u == "ETH" else 1.0)
                                _qty_cv = _qty * _cv   # actual currency units per premium pt
                                straddle_info["total_value_sold"] = round(_entryC, 2)
                                straddle_info["ltp"] = {"total_sold": round(_entryC, 2),
                                                        "current": round(_curC, 2), "pct": round(_ltp_pct, 2)}
                                # Fixed exit levels known AT ENTRY: total premium × day% (user spec).
                                _tgt_amt     = round(_init_etv * _dpt / 100.0, 2)
                                _sl_amt      = round(_init_etv * _dsl / 100.0, 2)
                                _decayed_pts = round(_init_etv - _cTV_theta, 2)  # time-value decay, positive = profit
                                _remain_pts  = round(_tgt_amt - _decayed_pts, 2)
                                straddle_info["theta"] = {
                                    "entry": round(_init_etv, 2),
                                    "entry_rs": round(_init_etv * _qty_cv, 4),
                                    "current": round(_cTV, 2),
                                    "pct": round(_tPct, 2),
                                    "target_amt": _tgt_amt,
                                    "target_rs": round(_tgt_amt * _qty_cv, 4),
                                    "sl_amt": _sl_amt,
                                    "sl_rs": round(_sl_amt * _qty_cv, 4),
                                    "decayed_pts": _decayed_pts,
                                    "decayed_rs": round(_decayed_pts * _qty_cv, 4),
                                    "remaining_pts": _remain_pts,
                                    "remaining_rs": round(_remain_pts * _qty_cv, 4),
                                    "exit_profit_at": round(_init_etv - _tgt_amt, 2),
                                    "exit_loss_at":   round(_init_etv + _sl_amt, 2),
                                }
                                # Scalable TSL live state
                                _tsl_on = bool(getattr(strat, "_tsl_enabled", False))
                                try:
                                    if _tsl_on:
                                        _tsl_basis   = str(getattr(strat, "_tsl_basis", "ltp")).lower()
                                        _bp = float(getattr(strat, "_tsl_base_profit_rs", 0)) * _lot_mul * _cv
                                        _bl = float(getattr(strat, "_tsl_base_lock_rs",   0)) * _lot_mul * _cv
                                        _sp = float(getattr(strat, "_tsl_step_profit_rs", 0)) * _lot_mul * _cv
                                        _cur_lock = float(getattr(pos, "tsl_high_lock_rs", 0.0) or 0.0)
                                        if _tsl_basis == "theta":
                                            _tsl_pnl_pts = _eTV - float(pos.current_time_value(_spot))
                                        else:
                                            _tsl_pnl_pts = float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
                                        _cur_profit_rs = round(_tsl_pnl_pts * _qty_cv, 4)
                                        if _cur_profit_rs < _bp:
                                            _next_rs = _bp
                                        elif _sp > 0:
                                            _steps = int((_cur_profit_rs - _bp) // _sp)
                                            _next_rs = _bp + (_steps + 1) * _sp
                                        else:
                                            _next_rs = None
                                        straddle_info["tsl"] = {
                                            "enabled": True, "basis": _tsl_basis,
                                            "base_profit_rs": round(_bp, 4), "base_lock_rs": round(_bl, 4),
                                            "current_profit_rs": _cur_profit_rs,
                                            "locked": _cur_lock > 0,
                                            "lock_rs": round(_cur_lock, 4),
                                            "next_step_rs": round(_next_rs, 4) if _next_rs is not None else None,
                                        }
                                    else:
                                        straddle_info["tsl"] = {"enabled": False}
                                except Exception:
                                    straddle_info["tsl"] = {"enabled": False}
                                # Exit eval cache — built every 3s by _check_exits
                                straddle_info["exit_eval"] = getattr(strat, "_last_exit_eval", None)
                    elif sname == "trap_trading":
                        eng = getattr(_srv, "_trap_engine", None)
                        op = getattr(eng, "_open_positions", {}) if eng else {}
                        prem = getattr(eng, "_prem_cache", {}) if eng else {}
                        # Day-locked tracked strikes (prev-day ATM + DTE), shown even
                        # when there is no open position so the UI reflects scanning.
                        _legp = getattr(eng, "_leg_prem", {}) if eng else {}
                        _htf_det = getattr(eng, "_htf_det", {}) if eng else {}
                        _mtf_det = getattr(eng, "_mtf_det", {}) if eng else {}
                        _ds = getattr(eng, "_day_strikes", {}).get(underlying) if eng else None
                        if _ds is not None:
                            def _legview(strike, opt):
                                lk = f"{underlying}:{int(strike)}:{opt}"
                                h = _htf_det.get(lk)
                                m = _mtf_det.get(lk)
                                hlv = getattr(h, "active_level", None)
                                return {
                                    "strike": int(strike),
                                    "ltp": round(float(_legp.get((underlying, int(strike), opt), 0.0) or 0.0), 2),
                                    "htf_state": getattr(getattr(h, "state", None), "name", "WATCH") if h else "—",
                                    "mtf_state": getattr(getattr(m, "state", None), "name", "WATCH") if m else "—",
                                    "level_l": round(float(getattr(hlv, "entry_l", 0.0) or 0.0), 2) if hlv else 0.0,
                                    "level_h": round(float(getattr(hlv, "sl_h", 0.0) or 0.0), 2) if hlv else 0.0,
                                    "traps": len(getattr(h, "_levels", []) or []) if h else 0,
                                }
                            _ce = _legview(_ds.ce_strike, "CE")
                            _pe = _legview(_ds.pe_strike, "PE")
                            tracking = {
                                "atm": _ds.atm, "dte": _ds.dte, "offset": _ds.offset_pts,
                                "ce_strike": _ds.ce_strike, "pe_strike": _ds.pe_strike,
                                "ce_ltp": _ce["ltp"], "pe_ltp": _pe["ltp"],
                                "ce": _ce, "pe": _pe,
                                # back-compat single fields (CE leg)
                                "phase": _ce["htf_state"], "entry_line": 0.0,
                            }
                        for _tid, tup in op.items():
                            try:
                                _t, opt_sym, entry_px, qty = tup
                            except Exception:
                                continue
                            ltp = float(prem.get(opt_sym, entry_px) or entry_px)
                            _ls = int(_srv._cfg.exchange.lot_sizes.get(underlying, 0) or 0)
                            _lots = (abs(int(qty)) // _ls) if _ls else 0
                            legs.append({"symbol": opt_sym, "qty": int(qty),
                                         "lot_size": _ls, "lots": _lots,
                                         "entry_price": round(float(entry_px), 2),
                                         "ltp": round(ltp, 2),
                                         "pnl": round((float(entry_px) - ltp) * abs(int(qty)), 2)})
                except Exception as exc:
                    logger.debug("client/positions: %s/%s build error: %s", sname, underlying, exc)
                by_broker[bid][sname] = {"legs": legs, "pnl": round(sum(l["pnl"] for l in legs), 2),
                                          "booked": booked, "tracking": tracking,
                                          "straddle": straddle_info,
                                          "ccy": (legs[0].get("ccy", "₹") if legs else
                                                  ("$" if str(underlying).upper() in ("BTC", "ETH") else "₹"))}
            return {"ok": True, "by_broker": by_broker}

        # ── CLIENT — history ──────────────────────────────────────────────────

        @app.get("/api/client/history", tags=["Client"])
        async def api_client_history(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            # Persistent per-client closed-trade history (recorded by the bridges on
            # every EXIT). Survives restarts.
            from data_layer import trade_history as _th
            rows = await asyncio.to_thread(_th.load, cid, 200)
            trades = [{
                "date":        r.get("ts", ""),
                "strategy":    r.get("strategy", "—"),
                "instrument":  r.get("instrument", "—"),
                "entry_price": r.get("entry_price", "—"),
                "exit_price":  r.get("exit_price", "—"),
                "exit_reason": r.get("exit_reason", "—"),
                "pnl":         float(r.get("pnl", 0)),
                "legs":        r.get("legs"),   # per-leg detail (side/strike/entry/exit/pnl) if recorded
            } for r in rows]
            return {"ok": True, "trades": trades}

        # ── CLIENT — kill broker ──────────────────────────────────────────────

        @app.post("/api/client/kill_broker/{binding_id}", tags=["Client"])
        async def api_client_kill_broker(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is None:
                raise HTTPException(404, "Client worker not running.")
            worker = getattr(reg_client, "workers", {}).get(binding_id)
            if worker is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            # Kill = square off ALL app-pushed trades on this broker FIRST (close-own-legs only),
            # stop its strategies, THEN halt the worker / disconnect.
            # ORDER MATTERS (same race as the per-broker square-off): flatten the legs BEFORE setting
            # is_running=False, else StraddleBookManager's reconcile can remove the book first and we
            # close NOTHING on the exchange.
            squared = 0
            if _srv._straddle_bridge is not None:
                try:
                    squared = await _srv._straddle_bridge.square_off_binding(
                        cid, binding_id, _srv._sell_straddles)
                except Exception as exc:
                    logger.error("kill_broker square-off failed for %s/%s: %s", cid, binding_id, exc)
            for d in _srv._client_db.get_deployments_sync(cid):
                if d.get("binding_id") == binding_id:
                    await _srv._client_db.set_deployment_running(d.get("deploy_id", ""), cid, False)
            if hasattr(worker, "halt"):
                await worker.halt()
            elif hasattr(worker, "stop"):
                await worker.stop()
            await _srv._client_db.upsert_client(cid, **{f"trade_enabled_{binding_id}": False})
            return {"ok": True, "squared": squared,
                    "message": f"Broker '{binding_id}' killed — squared off {squared} leg(s) & halted."}

        # ── CLIENT — delete broker binding ───────────────────────────────────

        @app.delete("/api/client/broker/{binding_id}", tags=["Client"])
        async def api_client_delete_broker(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_safe_sync, cid
            )
            match = next((b for b in bindings if b["binding_id"] == binding_id), None)
            if match is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            if match.get("is_trade_enabled"):
                raise HTTPException(
                    400,
                    f"Cannot delete '{binding_id}' while trading is ON. "
                    "Disable trading first.",
                )
            await _srv._client_db.delete_binding(cid, binding_id)
            logger.info("Dashboard: deleted binding %s for client %s", binding_id, cid)
            return {"ok": True, "message": f"Broker '{binding_id}' deleted."}

        # Fyers manual auth endpoints removed — use unified /connect → /callback/fyers flow

        # ── CLIENT — per-broker start / stop / mode ──────────────────────────

        @app.post("/api/client/broker/{binding_id}/start", tags=["Client"])
        async def api_client_broker_start(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """
            Enable trading for a specific broker binding.
            Automatically checks token validity; runs headless auth if token
            is missing or expired before enabling trading.
            """
            from broker_auth.headless_auth import headless_engine as _he
            cid = user.get("client_id", "")
            bindings_db = _srv._client_db.get_bindings_sync(cid)
            b_row = next((b for b in bindings_db if b["binding_id"] == binding_id), None)
            if b_row is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")

            provider = (b_row.get("provider") or "mock").lower()
            try:
                ok, auth_msg, token = await _he.authenticate_binding(b_row, cid, _srv._client_db)
            except Exception as exc:
                return {"ok": False, "message": f"Auth error: {exc}", "token_ok": False, "trading_enabled": False}

            if not ok:
                # Auth failed — do NOT enable trading
                return {
                    "ok": False,
                    "message": f"Could not connect {provider.title()}: {auth_msg}",
                    "token_ok": False,
                    "trading_enabled": False,
                }

            # Auth succeeded — enable trading for this binding
            await _srv._client_db.set_trade_enabled(cid, binding_id, True)
            logger.info("Dashboard: broker %s/%s started (token_ok=%s).", cid, binding_id, bool(token))
            return {
                "ok": True,
                "message": f"Connected & trading enabled. {auth_msg}",
                "token_ok": True,
                "trading_enabled": True,
            }

        @app.post("/api/client/broker/{binding_id}/stop", tags=["Client"])
        async def api_client_broker_stop(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            await _srv._client_db.set_trade_enabled(cid, binding_id, False)
            logger.info("Dashboard: broker %s/%s stopped.", cid, binding_id)
            return {"ok": True, "message": f"Broker '{binding_id}' stopped — trading disabled."}

        @app.post("/api/client/broker/{binding_id}/squareoff", tags=["Client"])
        async def api_client_broker_squareoff(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """BROKER-SPECIFIC square-off: flatten ONLY this broker's app-pushed legs (close-own-legs)
            and stop its strategies — terminal stays connected. Distinct from the global
            STOP & SQUARE-OFF (/api/client/stop_squareoff) which flattens every broker."""
            cid = user.get("client_id", "")
            # ORDER MATTERS: flatten the open legs FIRST (places the broker BUY-to-close), THEN stop the
            # deployments. If we set is_running=False first, StraddleBookManager's 5s reconcile can tear
            # the book down before square_off_binding runs → it finds no book, closes NOTHING in the
            # exchange, yet the position is discarded → real legs left open on Delta and the card vanishes.
            squared = 0
            if _srv._straddle_bridge is not None:
                try:
                    squared = await _srv._straddle_bridge.square_off_binding(
                        cid, binding_id, _srv._sell_straddles)
                except Exception as exc:
                    logger.error("broker squareoff failed for %s/%s: %s", cid, binding_id, exc)
            for d in _srv._client_db.get_deployments_sync(cid):
                if d.get("binding_id") == binding_id:
                    await _srv._client_db.set_deployment_running(d.get("deploy_id", ""), cid, False)
            logger.info("Dashboard: broker square-off %s/%s — %d leg(s).", cid, binding_id, squared)
            return {"ok": True, "squared": squared,
                    "message": f"Squared off {squared} leg(s) on '{binding_id}'."}

        @app.post("/api/client/broker/{binding_id}/mode", tags=["Client"])
        async def api_client_broker_mode(
            binding_id: str, body: _BrokerModeSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            if body.mode not in ("paper", "live"):
                raise HTTPException(400, "mode must be 'paper' or 'live'.")
            await _srv._client_db.set_trading_mode(cid, binding_id, body.mode)
            # Hot-swap the LIVE broker's in-memory mode so the change takes effect WITHOUT a
            # terminal restart (order routing reads DB trading_mode per-order, but keep the broker
            # object consistent for any broker-internal mode/product logic).
            try:
                broker = ((getattr(_srv._router, "_brokers", None) or {}).get(cid, {}) or {}).get(binding_id)
                if broker is not None:
                    broker._trading_mode_raw = "live" if body.mode == "live" else "paper"
                    _bind = getattr(broker, "_binding", None) or getattr(broker, "_b", None)
                    if _bind is not None:
                        try:
                            _bind.trading_mode = body.mode
                        except Exception:
                            pass
            except Exception as exc:
                logger.debug("mode hot-swap for %s/%s: %s", cid, binding_id, exc)
            return {"ok": True, "mode": body.mode,
                    "message": f"Mode set to {body.mode.upper()} (live — no restart needed)."}

        # ── CLIENT — Terminal toggle: Interactive OAuth Flow ─────────────────────

        @app.post("/api/client/broker/{binding_id}/connect", tags=["Client"])
        async def api_client_broker_connect(
            binding_id: str,
            request:    Request,
            user:       dict = Depends(_require_client),
        ):
            """
            Terminal toggle ON — Interactive OAuth handshake for all 6 providers:
              1. Check DB for cached access_token generated today → API ping
              2a. Token valid  → connect instantly (terminal_connected=1)
              2b. Token missing/expired → return broker OAuth login URL
                  Fyers/Upstox/Zerodha : standard OAuth2 redirect
                  Dhan                 : 3-step consent (server pre-generates consentAppId)
                  AngelOne             : implicit redirect (state in callback path)
                  AliceBlue            : OAuth2 + SHA256 exchange
            """
            import time as _time
            from broker_auth.headless_auth import headless_engine as _he
            from broker_auth.oauth_manager import generate_auth_url, build_state

            t0 = _time.monotonic()
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                return {"ok": False, "error": f"Broker '{binding_id}' not found."}

            provider   = (b.get("provider") or "mock").lower()
            api_key    = b.get("api_key", "")
            api_secret = b.get("api_secret", "")
            user_id    = b.get("user_id", "")

            logger.info(
                "[Terminal] [%s/%s] %s — CONNECT request (api_key_present=%s user_id_present=%s) (%.1fms)",
                cid, binding_id, provider.upper(), bool(api_key), bool(user_id), (_time.monotonic()-t0)*1000,
            )

            # Delta (crypto) authenticates DIRECTLY with the API key/secret — NO OAuth URL. Build the
            # broker, authenticate() (signed /v2/wallet/balances from the box's whitelisted IP), and
            # on success mark terminal connected + register the execution broker.
            if provider == "delta":
                try:
                    from config.client_profiles import BrokerBinding as _BB
                    from execution_bridge.base_broker import create_broker
                    _bb = _BB(
                        binding_id=binding_id, provider="delta", label=b.get("label", ""),
                        user_id=user_id, api_key=api_key, api_secret=api_secret, access_token="",
                        is_trade_enabled=bool(b.get("is_trade_enabled", 1)),
                        lot_multiplier=float(b.get("lot_multiplier", 1.0) or 1.0),
                        product_type=(b.get("product_type", "") or "MIS"),
                        trading_mode=(b.get("trading_mode", "paper") or "paper"),
                        source_ip=(b.get("source_ip", "") or ""),
                    )
                    _nb = create_broker(_bb, cid)
                    if await _nb.authenticate():
                        await _srv._client_db.set_terminal_connected(cid, binding_id, True)
                        if _srv._router is not None:
                            _srv._router._brokers.setdefault(cid, {})[binding_id] = _nb
                            try:
                                _srv._router._pool.add_broker_to_worker(cid, binding_id, _nb, "delta")
                            except Exception:
                                pass
                        logger.info("[Terminal] [%s/%s] DELTA connected (api-key auth).", cid, binding_id)
                        return {"ok": True, "connected": True, "message": "Delta Exchange connected.",
                                "flow": "apikey"}
                    return {"ok": False, "error": "Delta auth failed — check the API key/secret and that "
                            "this server's IP is whitelisted on the Delta API key."}
                except Exception as exc:
                    logger.error("[Terminal] [%s/%s] Delta connect error: %s", cid, binding_id, exc)
                    return {"ok": False, "error": f"Delta connect error: {exc}"}

            ok, msg, token = await _he.authenticate_binding(b, cid, _srv._client_db)

            if ok:
                await _srv._client_db.set_terminal_connected(cid, binding_id, True)
                # CRITICAL: refresh the cached ORDER-ROUTING broker with the just-refreshed
                # token. The execution broker in _router._brokers is authenticated ONCE at
                # ExecutionRouter.start(); if the process booted before today's token was
                # generated, it holds a stale/expired token and live orders fail with
                # "Incorrect api_key or access_token" even though the terminal shows CONNECTED
                # with a fresh "Token OK". Re-create + re-auth it here so order routing uses
                # the new token without needing a full bot restart.
                try:
                    _fresh = await asyncio.to_thread(_srv._client_db.get_bindings_sync, cid)
                    _row = next((x for x in _fresh if x.get("binding_id") == binding_id), None)
                    if _row is not None and _srv._router is not None:
                        from config.client_profiles import BrokerBinding as _BB
                        from execution_bridge.base_broker import create_broker
                        _bb = _BB(
                            binding_id=binding_id, provider=provider,
                            label=_row.get("label", ""), user_id=_row.get("user_id", ""),
                            api_key=_row.get("api_key", ""), api_secret=_row.get("api_secret", ""),
                            access_token=_row.get("access_token", ""),
                            is_trade_enabled=bool(_row.get("is_trade_enabled", 1)),
                            lot_multiplier=float(_row.get("lot_multiplier", 1.0) or 1.0),
                            product_type=(_row.get("product_type", "") or "MIS"),
                            trading_mode=(_row.get("trading_mode", "paper") or "paper"),
                            source_ip=(_row.get("source_ip", "") or ""),
                        )
                        _nb = create_broker(_bb, cid)
                        if await _nb.authenticate():
                            _srv._router._brokers.setdefault(cid, {})[binding_id] = _nb
                            try:
                                _srv._router._pool.add_broker_to_worker(cid, binding_id, _nb, provider)
                            except Exception:
                                pass
                            logger.info("[Terminal] [%s/%s] execution broker refreshed with new token.",
                                        cid, binding_id)
                        else:
                            logger.warning("[Terminal] [%s/%s] execution broker re-auth returned False "
                                           "(orders may still fail — restart the bot).", cid, binding_id)
                except Exception as _re:
                    logger.warning("[Terminal] [%s/%s] execution broker refresh error: %s",
                                   cid, binding_id, _re)
                logger.info(
                    "[Terminal] [%s/%s] CONNECTED instantly — cached token valid (%.1fms)",
                    cid, binding_id, (_time.monotonic()-t0)*1000,
                )
                return {"ok": True, "connected": True, "message": msg, "flow": "cached"}

            # Token missing/expired — generate broker OAuth login URL
            logger.info(
                "[Terminal] [%s/%s] no valid token (reason=%s) — generating OAuth URL (%.1fms)",
                cid, binding_id, msg, (_time.monotonic()-t0)*1000,
            )
            base_url     = _redirect_base(request, _srv._client_db)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("client", cid, binding_id)
            logger.debug("[Terminal] [%s/%s] callback_url=%s state=%s", cid, binding_id, callback_url, state[:20])

            # generate_auth_url may make an HTTP call (Dhan), so run in thread
            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, provider, api_key, api_secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.warning(
                    "[Terminal] [%s/%s] auth URL generation failed (%.1fms): %s",
                    cid, binding_id, elapsed, auth_url,
                )
                return {"ok": False, "connected": False, "flow": "error", "error": auth_url}

            # Store pending auth for providers that don't return state (e.g. Zerodha)
            _srv._pending_auth[provider] = {
                "role": "client", "client_id": cid, "binding_id": binding_id,
                "api_key": api_key, "api_secret": api_secret,
            }
            logger.info(
                "[Terminal] [%s/%s] OAuth URL generated in %.1fms — provider=%s callback=%s",
                cid, binding_id, elapsed, provider.upper(), callback_url,
            )
            return {
                "ok":       False,
                "connected": False,
                "flow":     "oauth",
                "auth_url": auth_url,
                "message":  f"Open the broker login page to authenticate {provider.upper()}.",
            }

        @app.post("/api/client/broker/{binding_id}/disconnect", tags=["Client"])
        async def api_client_broker_disconnect(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """Terminal toggle OFF — stops engine and clears trade toggle atomically."""
            cid = user.get("client_id", "")
            await _srv._client_db.set_terminal_connected(cid, binding_id, False)
            await _srv._client_db.set_engine_active(cid, binding_id, False)
            await _srv._client_db.set_trade_enabled(cid, binding_id, False)
            # ORDER MATTERS: square off open legs FIRST (places BUY-to-close on exchange), THEN
            # set run toggles OFF. If we flip is_running first, StraddleBookManager's 5s reconcile
            # tears the book down before square_off_binding runs → nothing closes on exchange.
            if _srv._straddle_bridge is not None:
                try:
                    n = await _srv._straddle_bridge.square_off_binding(cid, binding_id, _srv._sell_straddles)
                    logger.info("Dashboard: Terminal OFF %s/%s — squared off %d leg(s).", cid, binding_id, n)
                except Exception as exc:
                    logger.error("Dashboard: square-off on Terminal OFF failed for %s/%s: %s", cid, binding_id, exc)
            try:
                for d in _srv._client_db.get_deployments_sync(cid):
                    if d.get("binding_id") == binding_id:
                        await _srv._client_db.set_deployment_running(d.get("deploy_id", ""), cid, False)
            except Exception:
                pass
            logger.info(
                "Terminal disconnect: [%s/%s] disconnected — terminal + engine + trade cleared.",
                cid, binding_id,
            )
            return {"ok": True, "message": "Terminal disconnected. Engine and Trade stopped."}

        # ── CLIENT — Engine toggle (Step 2: hot-reload strategy execution) ─────

        @app.post("/api/client/broker/{binding_id}/engine-start", tags=["Client"])
        async def api_client_engine_start(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """
            Trading Engine toggle ON.
            Requires terminal_connected=1 first.
            Scans strategy assignments and hot-attaches broker to the running engine.
            """
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_safe_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                return {"ok": False, "error": f"Broker '{binding_id}' not found."}
            if not b.get("terminal_connected"):
                return {
                    "ok": False,
                    "error": "Terminal not connected. Switch Terminal ON first.",
                }

            strategy  = b.get("assigned_strategy", "") or ""
            underlying = b.get("assigned_instrument", "NIFTY") or "NIFTY"

            await _srv._client_db.set_engine_active(cid, binding_id, True)

            # Apply any saved deployment config to RuntimeConfig immediately
            deploy_id = f"{cid}_{binding_id}_{strategy}_{underlying}"
            try:
                from data_layer.deployment_store import load_deployment_json, apply_deployment_to_runtime_config
                deploy = load_deployment_json(deploy_id)
                if deploy:
                    apply_deployment_to_runtime_config(deploy)
                    logger.info(
                        "Engine start: [%s/%s] applied deployment config %s",
                        cid, binding_id, deploy_id,
                    )
            except Exception as exc:
                logger.warning("Engine start: deployment config apply failed: %s", exc)

            logger.info(
                "Engine start: [%s/%s] ACTIVE — strategy=%s underlying=%s mode=%s",
                cid, binding_id, strategy or "(none)", underlying,
                b.get("trading_mode", "paper"),
            )
            return {
                "ok": True,
                "engine_active": True,
                "strategy": strategy,
                "underlying": underlying,
                "message": (
                    f"Trading Engine active — {strategy or 'no strategy'} "
                    f"on {underlying} [{b.get('trading_mode','paper').upper()}]"
                ),
            }

        @app.post("/api/client/broker/{binding_id}/engine-stop", tags=["Client"])
        async def api_client_engine_stop(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """Trading Engine toggle OFF — stops execution routing for this broker."""
            cid = user.get("client_id", "")
            await _srv._client_db.set_engine_active(cid, binding_id, False)
            logger.info("Engine stop: [%s/%s] engine deactivated.", cid, binding_id)
            return {"ok": True, "engine_active": False, "message": "Trading Engine stopped."}

        # ── CLIENT — Strategy deployment ──────────────────────────────────────

        @app.post("/api/client/strategy/deploy", tags=["Client"])
        async def api_client_strategy_deploy(
            body: _DeploymentSchema, user: dict = Depends(_require_client),
        ):
            """
            Save a strategy deployment config.
            Persists to SQLite + JSON file at data/deployments/{deploy_id}.json
            Immediately applies to RuntimeConfig if engine is active.
            """
            cid = user.get("client_id", "")

            # Validate squareoff time
            sq = (body.squareoff_time or "15:15").strip()
            try:
                h, m = sq.split(":")
                assert 0 <= int(h) <= 23 and 0 <= int(m) <= 59
            except Exception:
                return {"ok": False, "error": f"Invalid squareoff_time '{sq}'. Use HH:MM format."}

            allowed_strategies = {"sell_straddle", "iron_condor", "trap_trading", "trap_scanner"}
            if body.strategy_name not in allowed_strategies:
                return {"ok": False, "error": f"Unknown strategy '{body.strategy_name}'."}

            deploy_id = await _srv._client_db.save_deployment(
                client_id      = cid,
                binding_id     = body.binding_id,
                strategy_name  = body.strategy_name,
                underlying     = body.underlying,
                lot_multiplier = body.lot_multiplier,
                max_profit_rs  = body.max_profit_rs,
                max_sl_rs      = body.max_sl_rs,
                squareoff_time = sq,
            )

            from data_layer.deployment_store import save_deployment_json, apply_deployment_to_runtime_config
            save_deployment_json(
                deploy_id      = deploy_id,
                client_id      = cid,
                binding_id     = body.binding_id,
                strategy_name  = body.strategy_name,
                underlying     = body.underlying,
                lot_multiplier = body.lot_multiplier,
                max_profit_rs  = body.max_profit_rs,
                max_sl_rs      = body.max_sl_rs,
                squareoff_time = sq,
            )

            # If engine is already active for this broker, hot-apply immediately
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_safe_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == body.binding_id), None)
            hot_applied = False
            if b and b.get("engine_active"):
                try:
                    apply_deployment_to_runtime_config({
                        "strategy_name": body.strategy_name, "underlying": body.underlying,
                        "lot_multiplier": body.lot_multiplier, "max_profit_rs": body.max_profit_rs,
                        "max_sl_rs": body.max_sl_rs, "squareoff_time": sq,
                    })
                    hot_applied = True
                except Exception as exc:
                    logger.warning("Deploy hot-apply failed: %s", exc)

            logger.info(
                "Deploy saved: %s [lots=%.1f profit=%.0f sl=%.0f sq=%s hot=%s]",
                deploy_id, body.lot_multiplier, body.max_profit_rs,
                body.max_sl_rs, sq, hot_applied,
            )
            return {
                "ok": True,
                "deploy_id": deploy_id,
                "hot_applied": hot_applied,
                "message": f"Deployment saved{' and hot-applied' if hot_applied else ''}.",
            }

        @app.get("/api/client/strategy/deployments", tags=["Client"])
        async def api_client_get_deployments(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            rows = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
            return {"ok": True, "deployments": rows}

        @app.delete("/api/client/strategy/deploy/{deploy_id}", tags=["Client"])
        async def api_client_delete_deployment(
            deploy_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            await _srv._client_db.delete_deployment(deploy_id, cid)
            from data_layer.deployment_store import delete_deployment_json
            delete_deployment_json(deploy_id)
            logger.info("Deployment deleted: %s for client %s", deploy_id, cid)
            return {"ok": True, "message": "Deployment removed."}

        @app.post("/api/client/strategy/deploy/{deploy_id}/remove", tags=["Client"])
        async def api_client_remove_deployment(
            deploy_id: str, user: dict = Depends(_require_client),
        ):
            """POST-based alternative to DELETE for proxy-safe removal."""
            cid = user.get("client_id", "")
            await _srv._client_db.delete_deployment(deploy_id, cid)
            from data_layer.deployment_store import delete_deployment_json
            delete_deployment_json(deploy_id)
            logger.info("Deployment removed (POST): %s for client %s", deploy_id, cid)
            return {"ok": True, "message": "Deployment removed."}

        # ── UNIFIED OAuth Callback Handler ────────────────────────────────────
        # Receives redirects from ALL 6 broker login portals.
        # Routing strategy:
        #   Fyers/Upstox/Zerodha/AngelOne: state in ?state= query param
        #   Dhan: no state; consume-consent returns dhanClientId → DB lookup
        #   AliceBlue: state in ?state= query param

        async def _handle_oauth_callback(
            broker_name: str,
            request:     Request,
            path_state:  str = "",   # AngelOne: state from URL path
        ):
            import time as _t
            from broker_auth.oauth_manager import parse_state, exchange_code, consume_dhan_consent
            t0       = _t.monotonic()
            provider = broker_name.lower()

            # Log every callback entry — full query param keys (not values, for security)
            qp_keys = list(request.query_params.keys())
            logger.info(
                "[Callback] ENTRY provider=%s path_state=%s query_keys=%s client_ip=%s",
                provider, bool(path_state), qp_keys,
                request.headers.get("x-forwarded-for") or request.client.host if request.client else "unknown",
            )

            error = request.query_params.get("error", "")
            if error:
                logger.warning("[Callback] %s — broker returned error: %s", provider.upper(), error)
                return HTMLResponse(_callback_page("error", provider, f"Login failed: {error}"))

            # ── Extract the auth code from broker-specific query param ────────
            actual_code = ""
            extra: dict = {}

            if provider == "dhan":
                actual_code = request.query_params.get("tokenId", "")
            elif provider == "angelone":
                actual_code = request.query_params.get("auth_token", "") or request.query_params.get("authToken", "")
            elif provider == "aliceblue":
                actual_code = request.query_params.get("authCode", "")
                extra["user_id"] = request.query_params.get("userId", "")
            elif provider == "fyers":
                actual_code = request.query_params.get("auth_code", "") or request.query_params.get("code", "")
            elif provider == "zerodha":
                actual_code = request.query_params.get("request_token", "") or request.query_params.get("code", "")
            else:
                actual_code = request.query_params.get("code", "") or request.query_params.get("auth_code", "")

            logger.info(
                "[Callback] %s received — code=%s path_state=%s error=%s",
                provider.upper(), bool(actual_code), bool(path_state), error or "none",
            )

            if not actual_code:
                logger.error("[Callback] %s — no auth code in callback. query_keys=%s", provider.upper(), qp_keys)
                return HTMLResponse(_callback_page("error", provider, "No auth code received from broker."))

            # ── Identify admin vs client (routing) ────────────────────────────

            if provider == "dhan":
                # Dhan: call consume-consent → get dhanClientId → DB lookup
                platform_creds = await asyncio.to_thread(
                    _srv._client_db.get_platform_credentials_sync, "dhan"
                )
                if not platform_creds:
                    logger.error("[Callback] Dhan — no platform credentials found in DB (feeder_creds + bindings both empty)")
                    return HTMLResponse(_callback_page("error", provider, "Dhan app credentials not configured."))

                dhan_ok, dhan_msg, access_token, dhan_client_id = await asyncio.to_thread(
                    consume_dhan_consent,
                    platform_creds["api_key"],
                    platform_creds["api_secret"],
                    actual_code,
                )
                elapsed = (_t.monotonic() - t0) * 1000
                if not dhan_ok:
                    logger.error("[Callback] Dhan consume-consent failed in %.1fms: %s", elapsed, dhan_msg)
                    return HTMLResponse(_callback_page("error", provider, dhan_msg))

                # Look up which binding/feeder this dhanClientId belongs to
                match = await asyncio.to_thread(
                    _srv._client_db.find_by_broker_user_id_sync, "dhan", dhan_client_id
                )
                if not match:
                    logger.error("[Callback] Dhan — no binding found for dhanClientId=%s", dhan_client_id)
                    return HTMLResponse(_callback_page("error", provider,
                        f"No binding found for Dhan client {dhan_client_id}. "
                        "Ensure the Client ID is saved in your broker binding."))

                if match["scope"] == "feeder":
                    await _srv._client_db.update_feeder_token(
                        provider, access_token,
                        generated_at=datetime.now(IST).isoformat(), expiry_at=_ist_eod(),
                    )
                    await _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                    logger.info("[Callback] Dhan feeder token stored in %.1fms", elapsed)
                    return HTMLResponse(_callback_page("success", provider, "Dhan data feeder connected!"))
                else:
                    cid = match["client_id"]
                    bid = match["binding_id"]
                    await _srv._client_db.update_access_token(cid, bid, access_token, datetime.now(IST).isoformat(), _ist_eod())
                    await _srv._client_db.set_terminal_connected(cid, bid, True)
                    await _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": cid, "binding_id": bid, "provider": provider, "ok": True})
                    logger.info("[Callback] Dhan [%s/%s] token stored, terminal=ON in %.1fms", cid, bid, elapsed)
                    return HTMLResponse(_callback_page("success", provider, f"Dhan broker connected!"))

            elif provider == "aliceblue":
                # AliceBlue: userId in callback → DB lookup, then exchange
                alice_user_id = extra.get("user_id", "")
                if not alice_user_id:
                    logger.error("[Callback] AliceBlue — userId missing from callback. query_keys=%s", qp_keys)
                    return HTMLResponse(_callback_page("error", provider, "AliceBlue userId missing from callback."))

                match = await asyncio.to_thread(
                    _srv._client_db.find_by_broker_user_id_sync, "aliceblue", alice_user_id
                )
                if not match:
                    logger.error("[Callback] AliceBlue — no binding found for userId=%s", alice_user_id)
                    return HTMLResponse(_callback_page("error", provider,
                        f"No binding found for AliceBlue userId {alice_user_id}. "
                        "Ensure the Client ID (userId) is saved in your broker binding."))

                api_key    = match["api_key"]
                api_secret = match["api_secret"]
                base_url   = _redirect_base(request, _srv._client_db)
                callback_url = f"{base_url}/callback/{provider}"

                ok, msg, token = await asyncio.to_thread(
                    exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                )
                elapsed = (_t.monotonic() - t0) * 1000

                if not ok:
                    return HTMLResponse(_callback_page("error", provider, msg))

                if match["scope"] == "feeder":
                    await _srv._client_db.update_feeder_token(provider, token, datetime.now(IST).isoformat(), _ist_eod())
                    await _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                    return HTMLResponse(_callback_page("success", provider, "AliceBlue feeder connected!"))
                else:
                    cid, bid = match["client_id"], match["binding_id"]
                    await _srv._client_db.update_access_token(cid, bid, token, datetime.now(IST).isoformat(), _ist_eod())
                    await _srv._client_db.set_terminal_connected(cid, bid, True)
                    await _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": cid, "binding_id": bid, "provider": provider, "ok": True})
                    logger.info("[Callback] AliceBlue [%s/%s] token stored in %.1fms", cid, bid, elapsed)
                    return HTMLResponse(_callback_page("success", provider, "AliceBlue broker connected!"))

            else:
                # Standard state-based routing: Fyers, Upstox, Zerodha, AngelOne
                state_str = path_state or request.query_params.get("state", "")
                parsed    = parse_state(state_str) if state_str else {}
                role       = parsed.get("role", "")
                client_id  = parsed.get("client_id", "")
                binding_id = parsed.get("binding_id", "")

                # Zerodha does NOT return the state param — fall back to pending auth store
                if not role and provider == "zerodha":
                    pending = _srv._pending_auth.pop("zerodha", None)
                    if pending:
                        role       = pending["role"]
                        client_id  = pending["client_id"]
                        binding_id = pending["binding_id"]
                        logger.info(
                            "[Callback] Zerodha — no state param, using pending auth: role=%s client=%s binding=%s",
                            role, client_id, binding_id,
                        )
                    else:
                        logger.error("[Callback] Zerodha — no state param and no pending auth found")
                        return HTMLResponse(_callback_page("error", provider,
                            "Zerodha callback received but no pending auth found. "
                            "Please try clicking the toggle again."))
                base_url   = _redirect_base(request, _srv._client_db)
                callback_url = f"{base_url}/callback/{provider}"

                if role == "admin":
                    db_row     = _srv._client_db.get_feeder_creds_sync(provider) or {}
                    api_key    = db_row.get("api_key", "")
                    api_secret = db_row.get("secret", "")
                    ok, msg, token = await asyncio.to_thread(
                        exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                    )
                    elapsed = (_t.monotonic() - t0) * 1000
                    if ok and token:
                        await _srv._client_db.update_feeder_token(provider, token, datetime.now(IST).isoformat(), _ist_eod())
                        # Start the live feed stream immediately after token is stored
                        api_key = db_row.get("api_key", "")
                        await _start_feeder_stream(_srv._feeder, provider, api_key, token)
                        await _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                        logger.info("[Callback] Admin %s token stored + stream started in %.1fms", provider.upper(), elapsed)
                        return HTMLResponse(_callback_page("success", provider, "Data feeder connected and streaming!"))
                    logger.error("[Callback] Admin %s exchange failed: %s", provider, msg)
                    return HTMLResponse(_callback_page("error", provider, msg))

                elif role == "client" and client_id and binding_id:
                    bindings = await asyncio.to_thread(_srv._client_db.get_bindings_sync, client_id)
                    b = next((x for x in bindings if x["binding_id"] == binding_id), None)
                    if b is None:
                        return HTMLResponse(_callback_page("error", provider, "Binding not found."))
                    api_key    = b.get("api_key", "")
                    api_secret = b.get("api_secret", "")
                    ok, msg, token = await asyncio.to_thread(
                        exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                    )
                    elapsed = (_t.monotonic() - t0) * 1000
                    if ok and token:
                        # Strip "Bearer " prefix some brokers include in the token value
                        clean_token = token[7:] if token.startswith("Bearer ") else token
                        now = datetime.now(IST).isoformat()
                        await _srv._client_db.update_access_token(client_id, binding_id, clean_token, now, _ist_eod())
                        await _srv._client_db.set_terminal_connected(client_id, binding_id, True)
                        # Refresh the cached order-routing broker with the fresh OAuth token
                        # (same stale-token issue as the terminal-connect path).
                        try:
                            if _srv._router is not None:
                                from config.client_profiles import BrokerBinding as _BB
                                from execution_bridge.base_broker import create_broker
                                _bb = _BB(
                                    binding_id=binding_id, provider=provider,
                                    label=b.get("label", ""), user_id=b.get("user_id", ""),
                                    api_key=api_key, api_secret=api_secret, access_token=clean_token,
                                    is_trade_enabled=bool(b.get("is_trade_enabled", 1)),
                                    lot_multiplier=float(b.get("lot_multiplier", 1.0) or 1.0),
                                    product_type=(b.get("product_type", "") or "MIS"),
                                    trading_mode=(b.get("trading_mode", "paper") or "paper"),
                                    source_ip=(b.get("source_ip", "") or ""),
                                )
                                _nb = create_broker(_bb, client_id)
                                if await _nb.authenticate():
                                    _srv._router._brokers.setdefault(client_id, {})[binding_id] = _nb
                                    try:
                                        _srv._router._pool.add_broker_to_worker(client_id, binding_id, _nb, provider)
                                    except Exception:
                                        pass
                                    logger.info("[Callback] [%s/%s] execution broker refreshed with new token.",
                                                client_id, binding_id)
                        except Exception as _re:
                            logger.warning("[Callback] [%s/%s] execution broker refresh error: %s",
                                           client_id, binding_id, _re)
                        await _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": client_id, "binding_id": binding_id, "provider": provider, "ok": True})
                        logger.info("[Callback] Client [%s/%s] %s token stored, terminal=ON in %.1fms",
                                    client_id, binding_id, provider.upper(), elapsed)
                        return HTMLResponse(_callback_page("success", provider,
                            f"Broker {provider.upper()} connected! You can close this tab."))
                    logger.error("[Callback] Client [%s/%s] %s exchange failed: %s",
                                 client_id, binding_id, provider, msg)
                    return HTMLResponse(_callback_page("error", provider, msg))
                else:
                    logger.error(
                        "[Callback] %s — invalid/missing state. state_str=%s parsed=%s",
                        provider.upper(), state_str[:40] if state_str else "(empty)", parsed,
                    )
                    return HTMLResponse(_callback_page("error", provider, "Invalid or missing state parameter."))

        @app.get("/callback/{broker_name}", tags=["Auth"], include_in_schema=False)
        async def oauth_callback(broker_name: str, request: Request):
            return await _handle_oauth_callback(broker_name, request, path_state="")

        @app.get("/callback/{broker_name}/{path_state}", tags=["Auth"], include_in_schema=False)
        async def oauth_callback_with_state(broker_name: str, path_state: str, request: Request):
            """AngelOne embeds our state token in the redirect_url path."""
            return await _handle_oauth_callback(broker_name, request, path_state=path_state)

        # ── ADMIN — pending clients ───────────────────────────────────────────

        @app.get("/api/admin/clients/pending", tags=["Admin"])
        async def api_pending_clients(_: dict = Depends(_require_admin)):
            rows = _srv._client_db.get_pending_clients_sync()
            return {"pending": rows, "ts": datetime.now(IST).isoformat()}

        @app.get("/api/admin/auth_alerts", tags=["Admin"])
        async def api_auth_alerts(_: dict = Depends(_require_admin)):
            return {
                "feeder": _srv._auth_alerts.get("feeder", ""),
                "ts":     datetime.now(IST).isoformat(),
            }

        # ── ADMIN — strategy telemetry (TrapTradingEngine live state) ────────

        @app.get("/api/admin/strategy/telemetry", tags=["Admin"])
        async def api_strategy_telemetry(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            eng = _srv._trap_engine
            if eng is None:
                return {"ts": now_ist, "trap_engine": {}, "active": False}
            try:
                snapshot = eng.telemetry_snapshot()
                return {"ts": now_ist, "trap_engine": snapshot, "active": True}
            except Exception as exc:
                logger.warning("Dashboard: strategy telemetry read failed: %s", exc)
                return {"ts": now_ist, "trap_engine": {}, "active": False, "error": str(exc)}

        # ── ADMIN — premium-selling strategy registry ────────────────────

        @app.get("/api/admin/strategies", tags=["Admin"])
        async def api_strategies(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            try:
                out = []
                for ic in _srv._iron_condors:
                    pos = ic.position
                    entry = None
                    if pos:
                        entry = {
                            "short_ce": pos.short_ce.strike,
                            "short_pe": pos.short_pe.strike,
                            "long_ce":  pos.long_ce.strike,
                            "long_pe":  pos.long_pe.strike,
                            "net_credit":    round(pos.net_credit, 2),
                            "profit_target": round(pos.profit_target_rs, 2),
                            "stop_loss":     round(pos.sl_rs, 2),
                            "unrealized_pnl": round(pos.total_pnl_pts * pos.lot_size, 2),
                            "open_time": pos.open_time.isoformat() if pos.open_time else None,
                        }
                    out.append({
                        "type":         "iron_condor",
                        "underlying":   ic._underlying,
                        "running":      ic._running,
                        "has_position": ic.has_open_position,
                        "spot":         round(ic._spot, 2),
                        "entry_allowed": getattr(ic, "entry_allowed", True),
                        "position":     entry,
                    })
                for ss in _srv._sell_straddles:
                    pos = ss.position
                    entry = None
                    if pos:
                        entry = {
                            "atm":       pos.atm_at_entry,
                            "ce_strike": pos.ce_leg.strike,
                            "pe_strike": pos.pe_leg.strike,
                            "net_credit":    round(pos.net_credit, 2),
                            "unrealized_pnl": round(pos.unrealized_pnl, 2),
                            "peak_profit":   round(pos.peak_profit, 2),
                            "trailing_active": pos.trailing_active,
                            "open_time": pos.open_time.isoformat() if pos.open_time else None,
                        }
                    out.append({
                        "type":         "sell_straddle",
                        "underlying":   ss._underlying,
                        "running":      ss._running,
                        "has_position": ss.has_open_position,
                        "trades_today": ss.trades_today,
                        "booked_pnl":   round(getattr(ss, "_session_realized_pnl_pts", 0.0), 2),
                        "spot":         round(ss._spot, 2),
                        "rsi":          round(ss._ind.get("rsi", 0.0), 1),
                        "adx":          round(ss._ind.get("adx", 0.0), 1),
                        "entry_allowed": getattr(ss, "entry_allowed", True),
                        "position":     entry,
                    })
                return {"strategies": out, "ts": now_ist}
            except Exception as exc:
                logger.warning("Dashboard: /api/admin/strategies failed: %s", exc)
                return {"ok": False, "error": str(exc), "strategies": [], "ts": now_ist}

        # ── ADMIN — runtime strategy configuration ───────────────────────

        @app.get("/api/admin/strategy/config", tags=["Admin"])
        async def api_strategy_config_get(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            cfg = RuntimeConfig.get()
            ic  = cfg.get("iron_condor", {})
            ss  = cfg.get("sell_straddle", {})
            tt  = cfg.get("trap_trading", {})
            ic_idx = ic.get("per_index", {})
            return {
                "ic_squareoff_time":  ic.get("squareoff_time", "15:15"),
                "ic_rsi_min":         ic.get("rsi_min",  40.0),
                "ic_rsi_max":         ic.get("rsi_max",  60.0),
                "ic_adx_max":         ic.get("adx_max",  25.0),
                "ic_profit_pct":      ic.get("profit_pct", 50.0),
                "ic_sl_pct":          ic.get("sl_pct",   200.0),
                "ic_nifty_otm":       ic_idx.get("NIFTY",      {}).get("short_otm_pts", 200.0),
                "ic_nifty_wing":      ic_idx.get("NIFTY",      {}).get("wing_width_pts",200.0),
                "ic_banknifty_otm":   ic_idx.get("BANKNIFTY",  {}).get("short_otm_pts", 400.0),
                "ic_banknifty_wing":  ic_idx.get("BANKNIFTY",  {}).get("wing_width_pts",500.0),
                "ic_finnifty_otm":    ic_idx.get("FINNIFTY",   {}).get("short_otm_pts", 200.0),
                "ic_finnifty_wing":   ic_idx.get("FINNIFTY",   {}).get("wing_width_pts",200.0),
                "ic_sensex_otm":      ic_idx.get("SENSEX",     {}).get("short_otm_pts", 500.0),
                "ic_sensex_wing":     ic_idx.get("SENSEX",     {}).get("wing_width_pts",500.0),
                "ic_midcp_otm":       ic_idx.get("MIDCPNIFTY", {}).get("short_otm_pts", 150.0),
                "ic_midcp_wing":      ic_idx.get("MIDCPNIFTY", {}).get("wing_width_pts",200.0),
                "ss_entry_start":     ss.get("entry_start",    "09:20"),
                "ss_entry_end":       ss.get("entry_end",      "12:00"),
                "ss_squareoff_time":  ss.get("squareoff_time", "15:15"),
                "ss_max_trades":      ss.get("max_trades",      1),
                "tt_htf_minutes":     tt.get("htf_minutes",      75),
                "tt_ltf_minutes":     tt.get("ltf_minutes",       5),
                "tt_sl_mode":         tt.get("sl_mode",           "dynamic"),
                "tt_sl_pct":          tt.get("sl_pct",            2.0),
            }

        @app.post("/api/admin/strategy/config/update", tags=["Admin"])
        async def api_strategy_config_update(
            body: _StrategyConfigSchema,
            _: dict = Depends(_require_admin),
        ):
            from data_layer.runtime_config import RuntimeConfig
            patch = {
                "iron_condor": {
                    "squareoff_time": body.ic_squareoff_time,
                    "rsi_min":        body.ic_rsi_min,
                    "rsi_max":        body.ic_rsi_max,
                    "adx_max":        body.ic_adx_max,
                    "profit_pct":     body.ic_profit_pct,
                    "sl_pct":         body.ic_sl_pct,
                    "per_index": {
                        "NIFTY":      {"short_otm_pts": body.ic_nifty_otm,     "wing_width_pts": body.ic_nifty_wing},
                        "BANKNIFTY":  {"short_otm_pts": body.ic_banknifty_otm, "wing_width_pts": body.ic_banknifty_wing},
                        "FINNIFTY":   {"short_otm_pts": body.ic_finnifty_otm,  "wing_width_pts": body.ic_finnifty_wing},
                        "SENSEX":     {"short_otm_pts": body.ic_sensex_otm,    "wing_width_pts": body.ic_sensex_wing},
                        "MIDCPNIFTY": {"short_otm_pts": body.ic_midcp_otm,     "wing_width_pts": body.ic_midcp_wing},
                    },
                },
                "sell_straddle": {
                    "entry_start":    body.ss_entry_start,
                    "entry_end":      body.ss_entry_end,
                    "squareoff_time": body.ss_squareoff_time,
                    "max_trades":     body.ss_max_trades,
                },
                "trap_trading": {
                    "htf_minutes": body.tt_htf_minutes,
                    "ltf_minutes": body.tt_ltf_minutes,
                    "sl_mode":     body.tt_sl_mode,
                    "sl_pct":      body.tt_sl_pct,
                },
            }
            RuntimeConfig.update(patch)

            # Live-inject into running strategy instances
            reconfigure_errors = []
            for ic in (_srv._iron_condors or []):
                try:
                    ic.reconfigure()
                except Exception as e:
                    reconfigure_errors.append(f"iron_condor[{ic._underlying}]: {e}")
            for ss in (_srv._sell_straddles or []):
                try:
                    ss.reconfigure()
                except Exception as e:
                    reconfigure_errors.append(f"sell_straddle[{ss._underlying}]: {e}")
            if _srv._trap_engine is not None:
                try:
                    _srv._cfg.trap_engine.reconfigure(
                        SL_MODE=body.tt_sl_mode,
                        SL_PCT=body.tt_sl_pct,
                    )
                except Exception as e:
                    reconfigure_errors.append(f"trap_engine: {e}")

            if reconfigure_errors:
                logger.warning("strategy/config/update: partial reconfigure errors: %s", reconfigure_errors)
                return {"ok": True, "message": "Config saved. Some live-inject errors.", "errors": reconfigure_errors}

            logger.info("strategy/config/update: all strategies reconfigured live.")
            return {"ok": True, "message": "Runtime configuration live-deployed to all strategies."}

        # ── ADMIN — per-index strategy config (rule builder) ─────────────────

        @app.get("/api/admin/strategy/config/{index}", tags=["Admin"])
        async def api_index_config_get(index: str, _: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            idx = index.upper()
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "CRUDEOIL", "BTC", "ETH"}
            if idx not in allowed:
                raise HTTPException(400, f"Unknown index '{idx}'. Allowed: {sorted(allowed)}")
            return {
                "index": idx,
                "sell_straddle": RuntimeConfig.index_section(idx, "sell_straddle"),
                "iron_condor":   RuntimeConfig.index_section(idx, "iron_condor"),
                "trap_trading":  RuntimeConfig.index_section(idx, "trap_trading"),
            }

        @app.post("/api/admin/strategy/config/{index}", tags=["Admin"])
        async def api_index_config_save(
            index: str, request: Request, _: dict = Depends(_require_admin),
        ):
            from data_layer.runtime_config import RuntimeConfig
            idx = index.upper()
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY", "CRUDEOIL", "BTC", "ETH"}
            if idx not in allowed:
                raise HTTPException(400, f"Unknown index '{idx}'. Allowed: {sorted(allowed)}")
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")

            # Validate and persist each strategy section if present
            if "sell_straddle" in body:
                RuntimeConfig.set_index_section(idx, "sell_straddle", body["sell_straddle"])
                for ss in (_srv._sell_straddles or []):
                    if getattr(ss, "_underlying", None) == idx:
                        try:
                            ss.reconfigure()
                        except Exception as exc:
                            logger.warning("reconfigure SS[%s]: %s", idx, exc)

            if "iron_condor" in body:
                RuntimeConfig.set_index_section(idx, "iron_condor", body["iron_condor"])
                for ic in (_srv._iron_condors or []):
                    if getattr(ic, "_underlying", None) == idx:
                        try:
                            ic.reconfigure()
                        except Exception as exc:
                            logger.warning("reconfigure IC[%s]: %s", idx, exc)

            if "trap_trading" in body:
                # Trap engine reads settings live via RuntimeConfig.index_section, so
                # persisting is sufficient; it applies on the next day-strike lock.
                RuntimeConfig.set_index_section(idx, "trap_trading", body["trap_trading"])

            logger.info("Dashboard: per-index config saved for %s.", idx)
            return {"ok": True, "message": f"Config for {idx} saved and injected into running strategies."}

        @app.get("/api/admin/strategy/config/all/indices", tags=["Admin"])
        async def api_all_indices_config(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            return RuntimeConfig.get_all_indices()

        @app.get("/api/admin/strategy/trap-config", tags=["Admin"])
        async def api_trap_config_get(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            cfg = RuntimeConfig.get().get("trap_trading") or {}
            return {"ok": True, "config": cfg}

        @app.post("/api/admin/strategy/trap-config", tags=["Admin"])
        async def api_trap_config_save(
            request: Request, _: dict = Depends(_require_admin),
        ):
            # Note: TrapEngineConfig (HTF_MINUTES, MTF_MINUTES, LTF_MINUTES,
            # RETEST_ZONE_PERCENT, SLIPPAGE_BUFFER, bars_lookback_days) is now
            # managed at POST /api/admin/trap/config. This endpoint manages
            # RuntimeConfig overrides only (adx_threshold, volume_spike_multiplier,
            # swing_lookback, zone_tolerance_pct, void_atr_mult). Callers should
            # migrate timeframe parameters to /api/admin/trap/config.
            from data_layer.runtime_config import RuntimeConfig
            try:
                body = await request.json()
            except Exception:
                return {"ok": False, "error": "Invalid JSON body."}
            allowed = {"htf_minutes", "ltf_minutes", "retest_zone_pct",
                       "slippage_buffer", "sl_mode", "sl_pct",
                       "sl_buffer_pct", "entry_cutoff_time"}
            patch = {k: v for k, v in body.items() if k in allowed}
            if not patch:
                return {"ok": False, "error": "No valid fields provided."}
            RuntimeConfig.update({"trap_trading": patch})
            # Live-inject into TrapEngineConfig
            engine_cfg = getattr(getattr(_srv, "_cfg", None), "trap_engine", None)
            if engine_cfg is not None:
                try:
                    live_updates = {}
                    if "sl_mode" in patch:
                        live_updates["SL_MODE"] = patch["sl_mode"]
                    if "sl_pct" in patch:
                        live_updates["SL_PCT"] = patch["sl_pct"]
                    if "retest_zone_pct" in patch:
                        live_updates["RETEST_ZONE_PERCENT"] = patch["retest_zone_pct"]
                    if "slippage_buffer" in patch:
                        live_updates["SLIPPAGE_BUFFER"] = patch["slippage_buffer"]
                    if "sl_buffer_pct" in patch:
                        live_updates["SL_BUFFER_PCT"] = patch["sl_buffer_pct"]
                    if "entry_cutoff_time" in patch:
                        live_updates["ENTRY_CUTOFF_TIME"] = patch["entry_cutoff_time"]
                    if live_updates:
                        engine_cfg.reconfigure(**live_updates)
                except Exception as exc:
                    logger.warning("Trap config live-inject failed: %s", exc)
            logger.info("Trap Trading config saved: %s", patch)
            return {"ok": True, "message": "Trap Trading config saved."}

        # ── ADMIN — TrapEngineConfig REST endpoints ───────────────────────────

        @app.get("/api/admin/trap/config", tags=["Admin"])
        async def api_trap_engine_config_get(_: dict = Depends(_require_admin)):
            """Return current TrapEngineConfig values."""
            engine_cfg = getattr(_srv._cfg, "trap_engine", None)
            if engine_cfg is None:
                return {"ok": False, "error": "TrapEngineConfig not available"}
            return {"ok": True, "config": engine_cfg.snapshot()}

        @app.post("/api/admin/trap/config", tags=["Admin"])
        async def api_trap_engine_config_set(
            payload: _TrapConfigUpdateSchema,
            _: dict = Depends(_require_admin),
        ):
            """Update one or more TrapEngineConfig fields atomically."""
            engine_cfg = getattr(_srv._cfg, "trap_engine", None)
            if engine_cfg is None:
                return {"ok": False, "error": "TrapEngineConfig not available"}
            try:
                updates = {k: v for k, v in payload.model_dump().items() if v is not None}
                if not updates:
                    return {"ok": False, "error": "No fields provided."}
                updated = engine_cfg.reconfigure(**updates)
                return {"ok": True, "updated": updated, "config": engine_cfg.snapshot()}
            except (ValueError, AttributeError) as exc:
                return {"ok": False, "error": str(exc)}

        @app.get("/api/admin/trap/client/{client_id}/instruments", tags=["Admin"])
        async def api_trap_client_instruments_get(
            client_id: str, _: dict = Depends(_require_admin),
        ):
            """Get the TrapTrading instrument list for a client."""
            try:
                instruments = await _srv._client_db.get_trap_instruments(client_id)
                return {"ok": True, "client_id": client_id, "instruments": instruments}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        @app.post("/api/admin/trap/client/{client_id}/instruments", tags=["Admin"])
        async def api_trap_client_instruments_set(
            client_id: str,
            payload: _TrapInstrumentsSchema,
            _: dict = Depends(_require_admin),
        ):
            """Set the TrapTrading instrument list for a client."""
            try:
                await _srv._client_db.set_trap_instruments(client_id, payload.instruments)
                return {"ok": True, "client_id": client_id, "instruments": payload.instruments}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        @app.get("/api/admin/trap/clients/instruments", tags=["Admin"])
        async def api_trap_all_clients_instruments(_: dict = Depends(_require_admin)):
            """Return trap_instruments for all clients in one call (avoids N+1 per-client fetches)."""
            try:
                clients = await asyncio.to_thread(_srv._client_db.get_all_clients_sync)
                result = {}
                for c in clients:
                    cid = c.get("client_id") or c.get("id")
                    if not cid:
                        continue
                    result[cid] = await _srv._client_db.get_trap_instruments(cid)
                return {"ok": True, "instruments": result}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        # ── ADMIN — TrapTrading paper backtest replay ─────────────────────────

        @app.post("/api/strategies/trap_trading/replay", tags=["Admin"])
        async def api_trap_replay(
            payload: _TrapReplaySchema,
            _: dict = Depends(_require_admin),
        ):
            """
            Replay saved 1-min bars through the TrapTradingEngine in backtest mode.

            Creates an isolated engine instance (is_backtest=True, no broker orders),
            resamples bars to HTF/MTF, replays each bar through the state machine,
            and returns the full trade log plus final phase per symbol.
            """
            import pandas as pd
            from datetime import timedelta
            from strategies.trap_trading_engine import TrapTradingEngine, _Phase
            from data_layer.base_feeder import CandleEvent, EventBus as _EB

            if _srv._client_db is None:
                return {"ok": False, "error": "ClientDB not available."}

            symbol = payload.symbol.upper()
            try:
                since = datetime.fromisoformat(payload.start_date).replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=IST
                )
                until = datetime.fromisoformat(payload.end_date).replace(
                    hour=23, minute=59, second=59, microsecond=0, tzinfo=IST
                )
            except ValueError as exc:
                return {"ok": False, "error": f"Invalid date format: {exc}"}

            rows = await asyncio.to_thread(
                _srv._client_db.get_1m_bars_sync, symbol, since, until
            )
            if not rows:
                return {
                    "ok": False,
                    "error": f"No 1-min bars found for {symbol} in [{payload.start_date}, {payload.end_date}].",
                }

            # Build isolated engine — no bus publishing, no DB client → pure sandbox
            sandbox_bus = _EB()
            sandbox_eng = TrapTradingEngine(sandbox_bus, _srv._cfg, client_db=None)

            # Force every state to backtest mode as states are created
            _original_get_state = sandbox_eng._get_state

            def _bt_get_state(sym):
                st = _original_get_state(sym)
                st.is_backtest = True
                return st

            sandbox_eng._get_state = _bt_get_state  # monkey-patch for isolation

            # Resample bars
            tc = _srv._cfg.trap_engine
            df = pd.DataFrame(rows)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.set_index("timestamp").sort_index()
            agg = {"open": "first", "high": "max", "low": "min",
                   "close": "last", "volume": "sum"}

            _htf_origin = df.index[0].normalize().replace(hour=9, minute=15, second=0)
            htf_df = df.resample(
                f"{tc.HTF_MINUTES}min", closed="left", label="right", origin=_htf_origin
            ).agg(agg).dropna()

            mtf_df = df.resample(
                f"{tc.MTF_MINUTES}min", closed="left", label="right"
            ).agg(agg).dropna()

            def _make_candle(sym, tf, ts, row):
                return CandleEvent(
                    symbol=sym, timeframe=tf,
                    timestamp=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                    open=float(row["open"]), high=float(row["high"]),
                    low=float(row["low"]),   close=float(row["close"]),
                    volume=int(row["volume"]),
                )

            # Interleave HTF and MTF replay in chronological order
            htf_events = [
                (ts, "htf", row) for ts, row in htf_df.iterrows()
            ]
            mtf_events = [
                (ts, "mtf", row) for ts, row in mtf_df.iterrows()
            ]
            all_events = sorted(htf_events + mtf_events, key=lambda x: x[0])

            for ts, kind, row in all_events:
                tf = tc.HTF_MINUTES if kind == "htf" else tc.MTF_MINUTES
                candle = _make_candle(symbol, tf, ts, row)
                if kind == "htf":
                    sandbox_eng._process_htf(candle)
                else:
                    sandbox_eng._process_mtf(candle)

            # Collect results
            trade_log = sandbox_eng.backtest_log()
            phase_map  = {
                sym: st.phase.name
                for sym, st in sandbox_eng._states.items()
            }

            return {
                "ok":          True,
                "symbol":      symbol,
                "start_date":  payload.start_date,
                "end_date":    payload.end_date,
                "bars_total":  len(rows),
                "htf_bars":    len(htf_df),
                "mtf_bars":    len(mtf_df),
                "trades":      trade_log,
                "trade_count": len(trade_log),
                "final_phase": phase_map,
                "signal_count": sandbox_eng.signal_count(),
            }

        # ── ADMIN — Historical API replay (Upstox live data, no local DB) ────

        @app.post("/api/strategies/trap_trading/historical_replay", tags=["Admin"])
        async def api_trap_historical_replay(
            payload: _TrapHistoricalReplaySchema,
            _: dict = Depends(_require_admin),
        ):
            """
            Pull real 1-min bars from Upstox historical API, run DTE ITM
            strike selection, and replay through the TrapTradingEngine sandbox.

            No local DB data required — works on weekends or when paper mode
            has not accumulated bars yet.
            """
            import math
            import pandas as pd
            from datetime import date as _date, timedelta
            from strategies.trap_trading_engine import TrapTradingEngine
            from data_layer.base_feeder import CandleEvent, EventBus as _EB

            script = payload.script.upper()
            provider = payload.provider.lower()

            # 1. Parse backtest_date
            try:
                bd = _date.fromisoformat(payload.backtest_date)
            except ValueError as exc:
                return {"ok": False, "error": f"Invalid backtest_date: {exc}"}

            # 2. Provider + access token
            if provider != "upstox":
                return {"ok": False, "error": f"Provider '{provider}' not yet supported. Use 'upstox'."}
            if _srv._client_db is None:
                return {"ok": False, "error": "ClientDB not available."}

            creds = await asyncio.to_thread(_srv._client_db.get_feeder_creds_sync, "upstox")
            if not creds or not creds.get("access_token"):
                return {
                    "ok": False,
                    "error": "No Upstox access token in DB. Authenticate via Admin > Feeder > Upstox first.",
                }
            access_token = creds["access_token"]

            # 3. Load registry first — expiry must come from real contract dates, not weekday math
            from data_layer.instrument_registry import REGISTRY as _REG
            if not _REG.is_loaded(script):
                try:
                    await asyncio.to_thread(_REG.load_sync, script, access_token)
                except Exception as exc:
                    return {"ok": False, "error": f"Failed to load instrument registry [{script}]: {exc}"}

            # 4. Active weekly expiry and DTE — always from registry
            expiry = _next_weekly_expiry(bd, script)
            dte    = (expiry - bd).days
            step   = _STRIKE_STEPS.get(script, 50)
            lot    = _LOT_SIZES_MAP.get(script, 75)

            # 5. Prior trading day → underlying open/close → base_strike
            prior_day  = _prior_trading_day(bd)
            index_key  = _UPSTOX_INDEX_KEY.get(script, f"NSE_INDEX|{script}")
            prior_str  = prior_day.isoformat()
            bd_str     = bd.isoformat()

            try:
                prior_bars = await asyncio.to_thread(
                    _fetch_upstox_candles_sync, access_token, index_key, prior_str, prior_str,
                )
            except Exception as exc:
                return {"ok": False, "error": f"Failed to fetch prior-day bars ({prior_str}): {exc}"}

            if not prior_bars:
                return {"ok": False, "error": f"No bars for {script} on {prior_str} (market holiday?). Try another date."}

            prior_open  = prior_bars[0]["open"]
            prior_close = prior_bars[-1]["close"]
            prior_high  = max(b["high"] for b in prior_bars)
            prior_low   = min(b["low"]  for b in prior_bars)
            base_strike = int(round(((prior_high + prior_low) / 2) / step) * step)

            # 6. DTE ITM matrix
            offset    = _dte_itm_offset(dte, step)
            ce_strike = int(round((base_strike - offset) / step) * step)
            pe_strike = int(round((base_strike + offset) / step) * step)

            # 6b. Instrument keys from already-loaded registry
            ce_key = _REG.get_upstox_key(script, expiry, ce_strike, "CE")
            pe_key = _REG.get_upstox_key(script, expiry, pe_strike, "PE")

            # Fallback: show constructed key if not found in registry (for display only)
            ce_key_display = ce_key or _upstox_option_key(script, expiry, ce_strike, "CE")
            pe_key_display = pe_key or _upstox_option_key(script, expiry, pe_strike, "PE")

            if not ce_key and not pe_key:
                diag_lines = _REG.get_diagnostics(script)
                return {
                    "ok": False,
                    "error": (
                        f"Contracts not found in Upstox registry for "
                        f"{script} expiry={expiry.isoformat()} CE={ce_strike} PE={pe_strike}."
                    ),
                    "registry_diagnostics": diag_lines,
                    "debug_hint": (
                        f"Registry loaded {len(_REG._upstox_keys.get(script, {}))} contracts. "
                        f"Known expiries: {[e.isoformat() for e in _REG.all_expiries(script)]}. "
                        f"Check registry_diagnostics for full step-by-step log."
                    ),
                }

            # 6. Fetch option premium bars — 2 prior trading days + backtest_date
            #    The engine runs on OPTION PREMIUM bars (not spot).
            #    Spot was only used to calculate which strike to select.
            lookback_days = max(1, int(payload.lookback_days))
            fetch_from = bd
            for _ in range(lookback_days):
                fetch_from = _prior_trading_day(fetch_from)
            fetch_from_str = fetch_from.isoformat()

            ce_bars: list = []
            pe_bars: list = []
            ce_fetch_error: str = ""
            pe_fetch_error: str = ""

            try:
                ce_bars = await asyncio.to_thread(
                    _fetch_upstox_candles_sync, access_token, ce_key, fetch_from_str, bd_str,
                )
            except Exception as exc:
                ce_fetch_error = str(exc)
                logger.warning("historical_replay CE fetch failed [%s]: %s", ce_key, exc)

            try:
                pe_bars = await asyncio.to_thread(
                    _fetch_upstox_candles_sync, access_token, pe_key, fetch_from_str, bd_str,
                )
            except Exception as exc:
                pe_fetch_error = str(exc)
                logger.warning("historical_replay PE fetch failed [%s]: %s", pe_key, exc)

            if not ce_bars and not pe_bars:
                err = f"CE error: {ce_fetch_error}" if ce_fetch_error else ""
                err += f" PE error: {pe_fetch_error}" if pe_fetch_error else ""
                return {"ok": False, "error": f"No option premium bars fetched. {err}".strip()}

            # 7. Helper: resample + run engine on one option's premium bars
            tc  = _srv._cfg.trap_engine
            agg = {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
            from zoneinfo import ZoneInfo as _ZI
            _IST = _ZI("Asia/Kolkata")

            def _to_df(bars):
                df = pd.DataFrame(bars)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Kolkata")
                return df.set_index("timestamp").sort_index()

            def _mk_candle(sym, tf, ts, row):
                ts_dt = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                if getattr(ts_dt, "tzinfo", None) is None:
                    ts_dt = ts_dt.replace(tzinfo=_IST)
                return CandleEvent(
                    symbol=sym, timeframe=tf, timestamp=ts_dt,
                    open=float(row["open"]),  high=float(row["high"]),
                    low=float(row["low"]),    close=float(row["close"]),
                    volume=int(row.get("volume", 0)),
                )

            from strategies.trap_trading_engine import _Phase as _TPhase

            def _run_engine_on_bars(bars: list, sym_label: str):
                """
                Full 5-stage backtest on option premium 1m bars.

                Stage 1+2: HTF (75m) candle processing
                Stage 3:   1m bars used as synthetic ticks — check if premium
                           retraces to entry_origin ± RETEST_ZONE_PERCENT
                Stage 4:   MTF (5m) candle processing (only when in RETEST_ALERT)
                Stage 5:   1m bars as ticks — check if premium touches ltf_entry_line
                """
                df = _to_df(bars)
                _origin = df.index[0].normalize().replace(hour=9, minute=15, second=0)
                htf = df.resample(f"{tc.HTF_MINUTES}min", closed="left", label="right", origin=_origin).agg(agg).dropna()
                mtf = df.resample(f"{tc.MTF_MINUTES}min", closed="left", label="right").agg(agg).dropna()

                eng = TrapTradingEngine(_EB(), _srv._cfg, client_db=None)
                _orig = eng._get_state
                def _bt(s):
                    st = _orig(s)
                    st.is_backtest = True
                    return st
                eng._get_state = _bt

                transitions: list = []
                htf_table:   list = []   # every HTF bar with remarks
                mtf_table:   list = []   # every MTF bar with remarks
                prev_phase = "IDLE"
                retest_pct = tc.RETEST_ZONE_PERCENT / 100.0
                slippage   = tc.SLIPPAGE_BUFFER

                # Build chronological event list: HTF, MTF, and 1m ticks
                htf_events = [(ts, "htf", row) for ts, row in htf.iterrows()]
                mtf_events = [(ts, "mtf", row) for ts, row in mtf.iterrows()]
                one_m_events = [(ts, "1m",  row) for ts, row in df.iterrows()]

                all_events = sorted(htf_events + mtf_events + one_m_events, key=lambda x: x[0])

                for ts, kind, row in all_events:
                    st = eng._states.get(sym_label)
                    prem = float(row["close"])

                    if kind == "htf":
                        tf = tc.HTF_MINUTES
                        candle = _mk_candle(sym_label, tf, ts, row)
                        eng._process_htf(candle)
                        st = eng._states.get(sym_label)

                        # Determine remark for HTF table
                        new_phase = st.phase.name if st else "IDLE"
                        remark = ""
                        if prev_phase == "IDLE" and new_phase == "TRAP_LOCKED":
                            lvs = st.trap_levels
                            lv0 = lvs[0] if lvs else None
                            remark = (
                                f"STAGE 1+2 SINGLE-CANDLE TRAP — swept prev high, bearish close. "
                                + (f"entry_origin={lv0.entry_origin:.2f} target={lv0.target_high:.2f}" if lv0 else "")
                            )
                        elif prev_phase == "IDLE" and new_phase == "HTF_BEARISH":
                            remark = f"STAGE 1 — Bearish candle noted (H:{st.htf_bearish_high:.2f} L:{float(row['low']):.2f}). Waiting for sweep above H:{st.htf_bearish_high:.2f} to lock trap."
                        elif prev_phase == "HTF_BEARISH" and new_phase == "HTF_BEARISH":
                            remark = f"Stage 1 additional level added ({len(st.pending_levels)} pending, sweep needed above {st.htf_bearish_high:.2f})"
                        elif prev_phase == "HTF_BEARISH" and new_phase == "TRAP_LOCKED":
                            lvs = st.trap_levels
                            levels_str = " | ".join(f"{lv.entry_origin:.0f}" for lv in lvs)
                            remark = (
                                f"STAGE 2 — SWEEP CONFIRMED. {len(lvs)} level(s) activated: [{levels_str}]  "
                                f"target={st.target_high:.2f}"
                            )
                        elif prev_phase == "HTF_BEARISH" and new_phase == "IDLE":
                            remark = "Bullish — no sweep. Reset to IDLE."
                        elif new_phase == "HTF_BEARISH":
                            remark = "Stage 1 candidate updated (still bearish, no sweep yet)."
                        elif prev_phase == "TRAP_LOCKED" and new_phase not in ("TRAP_LOCKED", "RETEST_ALERT", "MTF_BEARISH", "MTF_LOCKED", "ARMED", "LIVE"):
                            remark = "HTF bar after trap locked (monitoring for retest)."
                        elif new_phase in ("RETEST_ALERT",):
                            remark = f"STAGE 3 — Retest zone entered. premium={prem:.2f}"
                        elif new_phase == "ARMED":
                            remark = (
                                f"STAGE 4 — MTF nested trap confirmed. "
                                f"ltf_entry={st.ltf_entry_line:.2f}  ltf_sl={st.ltf_sl_line:.2f}"
                            )
                        elif new_phase == "LIVE":
                            remark = f"STAGE 5 — ENTRY TRIGGERED. entry_price={st.entry_price:.2f}"

                        htf_table.append({
                            "ts":     str(ts)[:19],
                            "open":   round(float(row["open"]),  2),
                            "high":   round(float(row["high"]),  2),
                            "low":    round(float(row["low"]),   2),
                            "close":  round(float(row["close"]), 2),
                            "phase":  new_phase,
                            "remark": remark,
                        })

                    elif kind == "mtf":
                        tf = tc.MTF_MINUTES
                        candle = _mk_candle(sym_label, tf, ts, row)
                        phase_before = eng._states.get(sym_label)
                        phase_before_name = phase_before.phase.name if phase_before else "IDLE"
                        eng._process_mtf(candle)
                        st = eng._states.get(sym_label)
                        new_phase_mtf = st.phase.name if st else "IDLE"
                        mtf_remark = ""
                        if phase_before_name == "RETEST_ALERT" and new_phase_mtf == "ARMED":
                            mtf_remark = f"STAGE 4 — Bearish 5m candle → ARMED immediately. ltf_entry={st.ltf_entry_line:.2f}"
                        elif phase_before_name == "MTF_BEARISH" and new_phase_mtf == "ARMED":
                            mtf_remark = f"STAGE 4 — ARMED. ltf_entry={st.ltf_entry_line:.2f}"
                        elif new_phase_mtf == "MTF_BEARISH":
                            mtf_remark = "Stage 4 — legacy MTF_BEARISH (updating candidate)."
                        mtf_table.append({
                            "ts":     str(ts)[:19],
                            "open":   round(float(row["open"]),  2),
                            "high":   round(float(row["high"]),  2),
                            "low":    round(float(row["low"]),   2),
                            "close":  round(float(row["close"]), 2),
                            "phase":  new_phase_mtf,
                            "remark": mtf_remark,
                        })

                    elif kind == "1m" and st is not None:
                        ts_time = ts.time() if hasattr(ts, "time") else None

                        # Stage 3: scan all active trap levels highest-first
                        if st.phase == _TPhase.TRAP_LOCKED:
                            for lv in st.trap_levels:
                                if not lv.active:
                                    continue
                                lo = lv.entry_origin * (1.0 - retest_pct)
                                hi = lv.entry_origin * (1.0 + retest_pct)
                                if lo <= prem <= hi:
                                    st.active_level = lv
                                    st.entry_origin = lv.entry_origin
                                    st.target_high  = lv.target_high
                                    st.phase = _TPhase.RETEST_ALERT
                                    break

                        # Stage 5: synthetic tick check — ARMED → LIVE (backtest entry)
                        elif st.phase == _TPhase.ARMED and st.ltf_entry_line > 0:
                            if prem <= st.ltf_entry_line + slippage:
                                eng._record_backtest_entry(sym_label, sym_label, prem, st)

                        # LTF exit guard — LIVE: check SL, target, EOD
                        elif st.phase == _TPhase.LIVE and st.trade_id:
                            exit_reason = None
                            if st.ltf_sl_line > 0 and prem < st.ltf_sl_line:
                                exit_reason = "SL_HIT"
                            elif st.target_high > 0 and prem >= st.target_high:
                                exit_reason = "TARGET_HIT"
                            elif ts_time and ts_time.hour == 15 and ts_time.minute >= 25:
                                exit_reason = "EOD_1530"
                            if exit_reason:
                                # Record exit into the backtest log entry
                                for rec in eng._backtest_log:
                                    if rec.get("trade_id") == st.trade_id:
                                        entry_px = float(rec.get("entry_price", 0))
                                        pnl_pts  = prem - entry_px
                                        rec["exit_price"]  = round(prem, 2)
                                        rec["exit_time"]   = str(ts)[:19]
                                        rec["exit_reason"] = exit_reason
                                        rec["pnl_pts"]     = round(pnl_pts, 2)
                                        rec["pnl_rs"]      = round(pnl_pts * lot, 2)
                                        break
                                rb          = st.rolling_base
                                trap_levels = st.trap_levels
                                act_level   = st.active_level
                                eng._reset_state(sym_label)
                                eng._states[sym_label].rolling_base = rb
                                eng._states[sym_label].trap_levels  = trap_levels
                                eng._states[sym_label].active_level = act_level
                                if exit_reason == "SL_HIT":
                                    eng._reset_to_next_level(sym_label)

                    # Track phase transitions
                    st_now = eng._states.get(sym_label)
                    new_phase = st_now.phase.name if st_now else "IDLE"
                    if new_phase != prev_phase:
                        transitions.append({
                            "ts":         str(ts)[:19],
                            "timeframe":  kind,
                            "from_phase": prev_phase,
                            "to_phase":   new_phase,
                            "bar": {
                                "open":  round(float(row["open"]),  2),
                                "high":  round(float(row["high"]),  2),
                                "low":   round(float(row["low"]),   2),
                                "close": round(float(row["close"]), 2),
                            },
                        })
                        prev_phase = new_phase

                final = eng._states.get(sym_label)
                trades_raw = eng.backtest_log()
                return {
                    "htf_bars":          len(htf),
                    "mtf_bars":          len(mtf),
                    "htf_table":         htf_table,
                    "mtf_table":         mtf_table,
                    "phase_transitions": [t for t in transitions if t["timeframe"] in ("htf","mtf")],
                    "all_transitions":   transitions,
                    "final_phase":       final.phase.name if final else "IDLE",
                    "signal_count":      eng.signal_count(),
                    "trades_raw":        trades_raw,
                    "entry_origin":      round(final.entry_origin,   2) if final else 0,
                    "target_high":       round(final.target_high,    2) if final else 0,
                    "trap_levels":       [
                        {"entry_origin": lv.entry_origin, "target": lv.target_high,
                         "active": lv.active}
                        for lv in (final.trap_levels if final else [])
                    ],
                    "ltf_entry_line":    round(final.ltf_entry_line, 2) if final else 0,
                    "ltf_sl_line":       round(final.ltf_sl_line,    2) if final else 0,
                    "rolling_base":      round(final.rolling_base,   2) if final else 0,
                }

            # 8. Run engine on CE premium bars and PE premium bars separately
            ce_result = _run_engine_on_bars(ce_bars, "CE") if ce_bars else None
            pe_result = _run_engine_on_bars(pe_bars, "PE") if pe_bars else None

            # 9. Build trade logs with position sizing + exit summary
            def _build_trade_logs(trades_raw, opt_type):
                logs = []
                for t in trades_raw:
                    ep   = float(t.get("entry_price", 0))
                    qty  = max(math.floor(payload.capital / (ep * lot)) * lot if ep > 0 else lot, lot)
                    lots = qty // lot
                    xp   = t.get("exit_price")
                    pnl_pts = t.get("pnl_pts")
                    pnl_rs  = round(float(pnl_pts) * lots, 2) if pnl_pts is not None else None
                    logs.append({
                        "timestamp":         t.get("timestamp", "")[:19],
                        "type":              opt_type,
                        "entry_price":       round(ep, 2),
                        "quantity":          qty,
                        "lots":              lots,
                        "ltf_sl_line":       round(float(t.get("ltf_sl",       0)), 2),
                        "macro_high_target": round(float(t.get("target_high",  0)), 2),
                        "entry_origin":      round(float(t.get("entry_origin", 0)), 2),
                        "margin_est":        round(ep * qty, 2),
                        "exit_price":        round(xp, 2) if xp is not None else None,
                        "exit_time":         t.get("exit_time", ""),
                        "exit_reason":       t.get("exit_reason", "OPEN"),
                        "pnl_pts":           round(float(pnl_pts), 2) if pnl_pts is not None else None,
                        "pnl_rs":            pnl_rs,
                    })
                return logs

            ce_trade_logs = _build_trade_logs(ce_result["trades_raw"], "CE") if ce_result else []
            pe_trade_logs = _build_trade_logs(pe_result["trades_raw"], "PE") if pe_result else []
            all_trade_logs = ce_trade_logs + pe_trade_logs

            def _prem_summary(bars, key):
                if not bars:
                    return {"instrument_key": key, "bars": 0, "open": 0, "close": 0, "day_high": 0, "day_low": 0}
                return {
                    "instrument_key": key,
                    "bars":      len(bars),
                    "open":      round(bars[0]["open"],              2),
                    "close":     round(bars[-1]["close"],            2),
                    "day_high":  round(max(b["high"] for b in bars), 2),
                    "day_low":   round(min(b["low"]  for b in bars), 2),
                    "fetch_from": fetch_from_str,
                    "fetch_to":   bd_str,
                }

            return {
                "ok": True,
                "contract_info": {
                    "script":                 script,
                    "backtest_date":          bd_str,
                    "fetch_from":             fetch_from_str,
                    "prior_day":              prior_str,
                    "prior_day_open":         round(prior_open,  2),
                    "prior_day_close":        round(prior_close, 2),
                    "prior_day_high":         round(prior_high,  2),
                    "prior_day_low":          round(prior_low,   2),
                    "calculated_base_strike": base_strike,
                    "days_to_expiry":         dte,
                    "itm_offset_pts":         offset,
                    "target_expiry":          expiry.isoformat(),
                    "ce_strike":              ce_strike,
                    "pe_strike":              pe_strike,
                    "selected_ce_symbol":     ce_key_display,
                    "selected_pe_symbol":     pe_key_display,
                    "lot_size":               lot,
                    "capital":                payload.capital,
                    "engine_note": (
                        "Engine runs on OPTION PREMIUM bars (CE and PE separately). "
                        f"Bars fetched from {fetch_from_str} to {bd_str} ({lookback_days+1} trading days)."
                    ),
                },
                "data_summary": {
                    "ce_premium":     _prem_summary(ce_bars, ce_key_display),
                    "pe_premium":     _prem_summary(pe_bars, pe_key_display),
                    "ce_fetch_error": ce_fetch_error,
                    "pe_fetch_error": pe_fetch_error,
                },
                "ce_engine": ce_result,
                "pe_engine": pe_result,
                # Flattened for UI compatibility
                "phase_transitions": (ce_result or {}).get("phase_transitions", []),
                "trade_logs":        all_trade_logs,
                "trade_count":       len(all_trade_logs),
                "final_phase": (
                    f"CE:{(ce_result or {}).get('final_phase','N/A')} "
                    f"PE:{(pe_result or {}).get('final_phase','N/A')}"
                ),
                "signal_count": (
                    ((ce_result or {}).get("signal_count", 0)) +
                    ((pe_result or {}).get("signal_count", 0))
                ),
                "option_data_note": "",
            }

        # ── ADMIN — AMO connectivity test ─────────────────────────────────────

        @app.post("/api/admin/amo_test", tags=["Admin"])
        async def api_amo_test(
            payload: _AmoTestSchema,
            _: dict = Depends(_require_admin),
        ):
            """
            Place a real AMO (After Market Order) for 1 lot at market price to verify
            broker connectivity, symbol resolution, and order routing end-to-end.

            The order is placed as AMO=True so it is queued for next-session open —
            safe to use as a connectivity test outside market hours.
            Cancel the order immediately from the broker terminal after verifying.
            """
            from datetime import timedelta
            from data_layer.instrument_registry import REGISTRY as _REG, next_expiry as _nexp
            from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType, create_broker
            from config.client_profiles import BrokerBinding

            if _srv._client_db is None:
                return {"ok": False, "error": "ClientDB not available."}

            underlying = payload.underlying.upper()
            lot_size   = _srv._cfg.exchange.lot_sizes.get(underlying, 75)
            qty        = payload.qty * lot_size

            # Resolve expiry — always from registry (real contract dates), never calculated
            today = datetime.now(IST).date()
            expiry_pref = payload.expiry_pref.lower()

            # Load registry first so we have real expiry dates.
            # Try feeder creds first, then fall back to any Upstox broker binding token.
            _upstox_creds_pre = await asyncio.to_thread(
                _srv._client_db.get_feeder_creds_sync, "upstox"
            )
            _upstox_token_pre = (_upstox_creds_pre or {}).get("access_token", "")
            if not _upstox_token_pre:
                # Feeder has no token — try any Upstox broker binding
                _all_bindings = await asyncio.to_thread(
                    _srv._client_db.get_bindings_sync, payload.client_id
                )
                for _b in _all_bindings:
                    if _b.get("provider") == "upstox" and _b.get("access_token"):
                        _upstox_token_pre = _b["access_token"]
                        break
            if _upstox_token_pre and not _REG.is_loaded(underlying):
                try:
                    await asyncio.to_thread(_REG.load_sync, underlying, _upstox_token_pre)
                except Exception as _le:
                    logger.warning("AMO test: registry pre-load failed: %s", _le)

            real_expiries = _REG.all_expiries(underlying)  # sorted list from broker API
            future_expiries = [e for e in real_expiries if e >= today]

            if future_expiries:
                if expiry_pref == "next_week":
                    expiry = future_expiries[1] if len(future_expiries) > 1 else future_expiries[0]
                elif expiry_pref == "monthly":
                    # Last expiry in list = farthest (usually monthly)
                    expiry = future_expiries[-1]
                else:
                    expiry = future_expiries[0]  # nearest upcoming
            else:
                # Registry loaded but returned no future expiries — cannot proceed
                return {
                    "ok": False,
                    "error": (
                        f"No active expiries found in registry for {underlying}. "
                        "Re-authenticate Upstox so the registry can fetch live contract dates."
                    ),
                }

            # Minimum valid strikes per underlying (sanity guard)
            _MIN_STRIKE = {"NIFTY": 5000, "BANKNIFTY": 10000, "FINNIFTY": 5000,
                           "SENSEX": 20000, "MIDCPNIFTY": 5000}

            # Resolve strike (ATM if not specified)
            if payload.strike:
                strike = int(payload.strike)
                min_valid = _MIN_STRIKE.get(underlying, 100)
                if strike < min_valid:
                    return {
                        "ok": False,
                        "error": (
                            f"Strike {strike} looks wrong for {underlying} "
                            f"(minimum valid: {min_valid}). "
                            f"Enter the full ATM strike, e.g. 24500 for NIFTY."
                        ),
                    }
            else:
                # Try spot from trap engine's spot cache (populated from live index ticks)
                spot = 0.0
                if _srv._trap_engine:
                    try:
                        spot = _srv._trap_engine._spot_cache.get(underlying, 0.0)
                    except Exception:
                        pass
                if spot <= 0:
                    step = _STRIKE_STEPS.get(underlying, 50)
                    examples = {"NIFTY": "24500", "BANKNIFTY": "52000", "FINNIFTY": "23000"}
                    hint = examples.get(underlying, f"nearest multiple of {step}")
                    return {
                        "ok": False,
                        "error": (
                            f"Market not live — spot price unavailable for {underlying}. "
                            f"Enter a strike manually (e.g. {hint})."
                        ),
                    }
                step   = _STRIKE_STEPS.get(underlying, 50)
                strike = int(round(spot / step) * step)

            # Fetch client binding (do this before registry load to fail fast)
            db_client = _srv._client_db.get_client_sync(payload.client_id)
            if db_client is None:
                return {"ok": False, "error": f"Client '{payload.client_id}' not found."}

            bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_sync, payload.client_id
            )
            binding_row = next((b for b in bindings if b["binding_id"] == payload.binding_id), None)
            if binding_row is None:
                return {"ok": False, "error": f"Binding '{payload.binding_id}' not found."}

            provider = binding_row.get("provider", "mock")

            # Registry already loaded above (expiry resolution step)

            # Build broker-specific symbol
            try:
                broker_symbol = _REG.get_broker_symbol(
                    underlying, expiry, strike, payload.opt_type, provider
                )
            except Exception as _e:
                broker_symbol = None
                logger.warning("AMO test: get_broker_symbol failed: %s", _e)

            if not broker_symbol:
                return {
                    "ok": False,
                    "error": (
                        f"Symbol not found in instrument registry for "
                        f"{underlying} {expiry} {strike}{payload.opt_type} ({provider}). "
                        f"Ensure Upstox token is valid so registry can load."
                    ),
                    "underlying":        underlying,
                    "expiry":            expiry.isoformat(),
                    "strike":            strike,
                    "opt_type":          payload.opt_type,
                    "provider":          provider,
                    "available_expiries": [e.isoformat() for e in future_expiries[:6]],
                }

            # Build broker instance
            from config.client_profiles import BrokerBinding as _BB
            bb = _BB(
                binding_id=binding_row["binding_id"],
                provider=provider,
                label=binding_row.get("label", ""),
                user_id=binding_row.get("user_id", ""),
                api_key=binding_row.get("api_key", ""),
                api_secret=binding_row.get("api_secret", ""),
                access_token=binding_row.get("access_token", ""),
                lot_multiplier=float(binding_row.get("lot_multiplier", 1.0)),
                is_trade_enabled=bool(binding_row.get("is_trade_enabled", 1)),
            )

            try:
                broker = create_broker(bb, payload.client_id)
            except Exception as _e:
                return {"ok": False, "error": f"Broker init failed: {_e}", "provider": provider}

            try:
                auth_ok = await broker.authenticate()
            except Exception as _e:
                return {"ok": False, "error": f"Broker auth error: {_e}", "provider": provider}
            if not auth_ok:
                # Show credential state to help diagnose — no secrets exposed
                _cred_debug = {
                    "has_access_token": bool(binding_row.get("access_token")),
                    "token_len":        len(binding_row.get("access_token") or ""),
                    "has_user_id":      bool(binding_row.get("user_id")),
                    "has_api_key":      bool(binding_row.get("api_key")),
                    "has_api_secret":   bool(binding_row.get("api_secret")),
                }
                _auth_hint = {
                    "angelone": " Token found but rejected — may be expired. Re-do OAuth login from client portal.",
                    "zerodha":  " Access token expires daily. Re-authenticate from client portal.",
                    "upstox":   " Access token expires daily. Re-authenticate from client portal.",
                    "fyers":    " Access token may be expired. Re-authenticate from client portal.",
                    "dhan":     " Access token may be expired. Re-authenticate from client portal.",
                }.get(provider, "")
                return {"ok": False, "error": f"Broker auth failed for {provider}/{payload.binding_id}.{_auth_hint}",
                        "provider": provider, "credential_state": _cred_debug}

            req = OrderRequest(
                broker_symbol=broker_symbol,
                exchange="NFO" if underlying != "SENSEX" else "BFO",
                side=OrderSide.BUY,
                qty=qty,
                order_type=OrderType.MARKET,
                price=0.0,
                tag=f"AMO_TEST_{underlying}_{payload.opt_type}",
                client_id=payload.client_id,
            )

            # Enable AMO on the broker if supported
            # Upstox discontinued API AMO (UDAPI1162) — skip AMO flag, use regular order
            # which will be rejected outside hours but proves connectivity
            _UPSTOX_NO_AMO = provider == "upstox"
            if hasattr(broker, "_is_amo") and not _UPSTOX_NO_AMO:
                broker._is_amo = True

            try:
                order_id = await broker.place_order(req)
                try:
                    await broker.logout()
                except Exception:
                    pass
                return {
                    "ok":            True,
                    "order_id":      order_id,
                    "provider":      provider,
                    "broker_symbol": broker_symbol,
                    "underlying":    underlying,
                    "expiry":        expiry.isoformat(),
                    "strike":        strike,
                    "opt_type":      payload.opt_type,
                    "qty":           qty,
                    "lots":          payload.qty,
                    "lot_size":      lot_size,
                    "message":       "AMO placed. Cancel from broker terminal immediately after verifying.",
                }
            except Exception as exc:
                try:
                    await broker.logout()
                except Exception:
                    pass
                err_str = str(exc)
                # Annotate known broker-specific errors with actionable hints
                hint = ""
                if "UDAPI1162" in err_str:
                    hint = " [Upstox has discontinued API AMO — use Upstox app/web for AMO. API connectivity confirmed OK.]"
                elif "Algo orders are not allowed" in err_str or "-50" in err_str:
                    hint = " [Fyers: enable API/Algo trading in your Fyers account settings → My Profile → API Access.]"
                elif "not authorized" in err_str.lower() or "unauthorized" in err_str.lower():
                    hint = " [Token may be expired — re-authenticate from the client portal.]"
                return {
                    "ok":            False,
                    "error":         err_str + hint,
                    "broker_symbol": broker_symbol,
                    "provider":      provider,
                    "underlying":    underlying,
                    "expiry":        expiry.isoformat(),
                    "strike":        strike,
                    "opt_type":      payload.opt_type,
                }

        # ── ADMIN — Registry debug + reload ──────────────────────────────────

        @app.get("/api/admin/registry/status", tags=["Admin"])
        async def api_registry_status(_: dict = Depends(_require_admin)):
            """Show what's loaded in InstrumentRegistry per underlying."""
            from data_layer.instrument_registry import REGISTRY as _R
            result = {}
            for idx in _srv._cfg.monitored_indices:
                keys = _R._upstox_keys.get(idx, {})
                sample = [
                    {"key": f"{e}/{s}/{o}", "instrument_key": k}
                    for (e, s, o), k in list(keys.items())[:5]
                ]
                result[idx] = {
                    "loaded":         _R.is_loaded(idx),
                    "contract_count": len(keys),
                    "expiries":       [d.isoformat() for d in _R.all_expiries(idx)],
                    "sample_keys":    sample,
                    "diagnostics":    _R.get_diagnostics(idx),
                }
            return {"ok": True, "registry": result}

        @app.post("/api/admin/registry/reload", tags=["Admin"])
        async def api_registry_reload(_: dict = Depends(_require_admin)):
            """Force reload InstrumentRegistry from Upstox for all monitored indices."""
            from data_layer.instrument_registry import REGISTRY as _R

            if _srv._client_db is None:
                return {"ok": False, "error": "ClientDB not available."}

            creds = await asyncio.to_thread(_srv._client_db.get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return {"ok": False, "error": "No Upstox access token. Authenticate feeder first."}

            results = {}
            for idx in _srv._cfg.monitored_indices:
                try:
                    await asyncio.to_thread(_R.load_sync, idx, token)
                    results[idx] = {
                        "ok":       True,
                        "contracts": len(_R._upstox_keys.get(idx, {})),
                        "expiries":  [d.isoformat() for d in _R.all_expiries(idx)],
                    }
                except Exception as exc:
                    results[idx] = {"ok": False, "error": str(exc)}

            return {"ok": True, "results": results}

        # ── ADMIN — portfolio risk command center ────────────────────────────

        @app.get("/api/admin/risk/summary", tags=["Admin"])
        async def api_risk_summary(_: dict = Depends(_require_admin)):
            _refresh_live_pnl()
            rm = _srv._risk_manager
            if rm is None:
                # Fallback: build summary purely from registry state
                clients = _srv._registry.all_active() if _srv._registry else []
                total_capital  = sum(c.risk.capital    for c in clients)
                total_net_mtm  = sum(getattr(c, "_daily_pnl", 0.0) for c in clients)
                total_open_lots = 0
                return {
                    "ts":               datetime.now(IST).isoformat(),
                    "total_capital":    round(total_capital,  2),
                    "total_net_mtm":    round(total_net_mtm,  2),
                    "total_open_lots":  total_open_lots,
                    "avg_slippage_pts": 0.0,
                    "client_count":     len(clients),
                    "clients":          [_build_client_dict(c) for c in clients],
                    "clients_at_risk":  [],
                    "risk_manager_active": False,
                }
            try:
                summary = rm.risk_summary()
                summary["risk_manager_active"] = True
                # Overlay live firm MTM from open strategy positions — the risk
                # manager's own states only move on broker fills (none in dry run).
                try:
                    _cl = _srv._registry.all_active() if _srv._registry else []
                    summary["total_net_mtm"] = round(sum(getattr(c, "_daily_pnl", 0.0) for c in _cl), 2)
                except Exception:
                    pass
                return summary
            except Exception as exc:
                logger.warning("Dashboard: risk summary read failed: %s", exc)
                return {
                    "ts":               datetime.now(IST).isoformat(),
                    "risk_manager_active": False,
                    "error":            str(exc),
                }

        @app.post("/api/admin/risk/kill_all", tags=["Admin"])
        async def api_risk_kill_all(
            body: _KillAllConfirmSchema,
            _: dict = Depends(_require_admin),
        ):
            if not body.confirm:
                raise HTTPException(400, "confirm must be true to execute firm-wide kill-all.")
            rm = _srv._risk_manager
            if rm is not None:
                result = await rm.kill_all()
            else:
                # Fallback: halt all via registry
                if _srv._registry:
                    _srv._registry.halt_all()
                actioned = [c.client_id for c in (_srv._registry.all_active() if _srv._registry else [])]
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event":     "KILL_SWITCH",
                    "scope":     "FIRM_WIDE",
                    "clients":   actioned,
                    "timestamp": datetime.now(IST).isoformat(),
                })
                result = {"status": "ok", "halted": actioned, "count": len(actioned),
                          "ts": datetime.now(IST).isoformat()}
            logger.warning("Dashboard: FIRM-WIDE KILL-ALL executed by admin. Result: %s", result)
            return result

        # ── ADMIN — approve client + assign strategies ────────────────────────

        @app.get("/api/admin/clients/{client_id}/bindings", tags=["Admin"])
        async def api_admin_client_bindings(
            client_id: str, _: dict = Depends(_require_admin)
        ):
            """Return all broker bindings for a client — reads DB directly, not registry."""
            bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_safe_sync, client_id
            )
            return {"ok": True, "client_id": client_id, "bindings": bindings}

        @app.post("/api/admin/clients/{client_id}/approve", tags=["Admin"])
        async def api_approve_client(
            client_id: str,
            body: _ApproveClientSchema,
            _: dict = Depends(_require_admin),
        ):
            from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding

            db_client = _srv._client_db.get_client_sync(client_id)
            if db_client is None:
                raise HTTPException(404, f"Client '{client_id}' not found in DB.")

            # Write strategy assignments and set approved flag
            for binding_id, strategy in body.strategy_assignments.items():
                await _srv._client_db.set_assigned_strategy(client_id, binding_id, strategy)
            await _srv._client_db.upsert_client(client_id, is_admin_approved=1)

            # Build ClientProfile and register in registry
            if _srv._registry is not None and _srv._registry.get(client_id) is None:
                strategies_raw = db_client.get("enabled_strategies") or "A,B,C"
                strategies = [s.strip() for s in strategies_raw.split(",") if s.strip()]
                risk = RiskProfile(
                    capital=float(db_client.get("capital", 500_000)),
                    max_risk_per_trade_pct=float(db_client.get("max_risk_pct", 1.0)),
                    max_daily_loss_pct=float(db_client.get("max_daily_loss_pct", 3.0)),
                )
                profile = ClientProfile(
                    client_id=client_id,
                    name=db_client.get("name", ""),
                    email=db_client.get("email", ""),
                    risk=risk,
                    enabled_strategies=strategies,
                    is_admin_approved=True,
                    target_index=db_client.get("target_index", "NIFTY"),
                )
                for b in _srv._client_db.get_bindings_sync(client_id):
                    strategy_for_b = body.strategy_assignments.get(b["binding_id"], b.get("assigned_strategy", ""))
                    profile.broker_bindings.append(BrokerBinding(
                        binding_id=b["binding_id"],
                        provider=b["provider"],      # type: ignore[arg-type]
                        label=b.get("label", ""),
                        user_id=b.get("user_id", ""),
                        api_key=b.get("api_key", ""),
                        api_secret=b.get("api_secret", ""),
                        totp_secret=b.get("totp_secret", ""),
                        access_token=b.get("access_token", ""),
                        lot_multiplier=float(b.get("lot_multiplier", 1.0)),
                        enabled=bool(b.get("enabled", 1)),
                        assigned_strategy=strategy_for_b,
                        is_trade_enabled=bool(b.get("is_trade_enabled", 1)),
                    ))
                try:
                    _srv._registry.register(profile)
                except ValueError:
                    pass  # already registered from a previous call

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_APPROVED", "client_id": client_id,
            })
            logger.info("Dashboard: approved client %s with strategies %s.", client_id, body.strategy_assignments)
            return {"ok": True, "message": f"Client '{client_id}' approved and activated."}

        # ── ADMIN — single-provider feeder connects ──────────────────────────

        @app.post("/api/admin/feeder/connect/upstox", tags=["Admin"])
        async def api_feeder_connect_upstox(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raw = {}
            # Strip all whitespace — hidden spaces in copy-pasted keys cause UDAPI100060.
            import re as _re
            _mobile_raw = str(raw.get("client_id", "") or "").strip()
            # Keep only digits; if 12-digit with leading 91 (country code) drop the prefix
            _mobile_digits = _re.sub(r"\D", "", _mobile_raw)
            if len(_mobile_digits) == 12 and _mobile_digits.startswith("91"):
                _mobile_digits = _mobile_digits[2:]
            form = {
                "client_id":   _mobile_digits,
                "api_key":     str(raw.get("api_key",     "") or "").strip(),
                "secret":      str(raw.get("secret",      "") or "").strip(),
                "password":    str(raw.get("password",    "") or "").strip(),
                "totp_secret": str(raw.get("totp_secret", "") or raw.get("totp", "") or "").strip(),
            }
            # Merge from DB if form fields are incomplete
            db_row = _srv._client_db.get_feeder_creds_sync("upstox") or {}
            creds = {
                "client_id":          form["client_id"]   or str(db_row.get("client_id",   "") or "").strip(),
                "api_key":            form["api_key"]     or str(db_row.get("api_key",     "") or "").strip(),
                "secret":             form["secret"]      or str(db_row.get("secret",      "") or "").strip(),
                "password":           form["password"]    or str(db_row.get("password",    "") or "").strip(),
                "totp_secret":        form["totp_secret"] or str(db_row.get("totp_secret", "") or "").strip(),
                "access_token":       db_row.get("access_token",       ""),
                "token_generated_at": db_row.get("token_generated_at", ""),
                "token_expiry_at":    db_row.get("token_expiry_at",    ""),
            }
            if not creds["client_id"] and not creds["api_key"]:
                raise HTTPException(400, "Upstox credentials are required. Enter them in the form or they must be cached in the database.")
            # Persist any newly-entered credentials to DB
            if form["client_id"] or form["api_key"]:
                await _srv._client_db.upsert_feeder_creds(
                    "upstox",
                    client_id=form["client_id"],
                    api_key=form["api_key"],
                    secret=form["secret"],
                    password=form["password"],
                    totp_secret=form["totp_secret"],
                )
            # Auto-generate access token via headless TOTP auth
            try:
                from broker_auth.headless_auth import headless_engine as _he
                ok, msg, token = await _he.validate_feeder_creds("upstox", creds)
                if ok and token:
                    creds["access_token"] = token
                    _srv._auth_alerts["feeder"] = ""
                    from broker_auth.headless_auth import _ist_eod
                    await _srv._client_db.update_feeder_token(
                        "upstox", token,
                        generated_at=datetime.now(IST).isoformat(),
                        expiry_at=_ist_eod(),
                    )
                else:
                    _srv._auth_alerts["feeder"] = f"Upstox auth failed: {msg}"
                    logger.error("Dashboard: Upstox headless auth failed: %s", msg)
                    await _srv._client_db.update_feeder_token(
                        "upstox", "", generated_at="", expiry_at="",
                    )
                    clean_error = _upstox_translate_error(msg)
                    return JSONResponse(status_code=502, content={"ok": False, "error": clean_error})
            except Exception as exc:
                _srv._auth_alerts["feeder"] = f"Upstox auth error: {exc}"
                logger.error("Dashboard: Upstox auth error: %s", exc)
                await _srv._client_db.update_feeder_token(
                    "upstox", "", generated_at="", expiry_at="",
                )
                clean_error = _upstox_translate_error(str(exc))
                return JSONResponse(status_code=502, content={"ok": False, "error": clean_error})
            try:
                await feeder.start_single("upstox", creds)
            except Exception as exc:
                logger.error("Dashboard: upstox connect failed: %s", exc)
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Upstox connect failed: {exc}"},
                )
            return {"ok": True, "message": "Upstox feed stream initialized.", "provider": "upstox", "token_fresh": True}

        @app.post("/api/admin/feeder/connect/fyers", tags=["Admin"])
        async def api_feeder_connect_fyers(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raw = {}
            # Fyers uses app_key / pin but we store as api_key / password internally
            form = {
                "client_id":   raw.get("client_id", "") or raw.get("fy_id", ""),
                "api_key":     raw.get("app_key", ""),
                "secret":      raw.get("secret", ""),
                "password":    raw.get("pin", ""),
                "totp_secret": raw.get("totp_secret", "") or raw.get("totp", ""),
            }
            # Merge from DB if form fields are incomplete
            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            creds = {
                "client_id":         form["client_id"]   or db_row.get("client_id",   ""),
                "app_key":           form["api_key"]     or db_row.get("api_key",     ""),
                "secret":            form["secret"]      or db_row.get("secret",      ""),
                "pin":               form["password"]    or db_row.get("password",    ""),
                "totp_secret":       form["totp_secret"] or db_row.get("totp_secret", ""),
                "access_token":      db_row.get("access_token",      ""),
                "token_generated_at": db_row.get("token_generated_at", ""),
                "token_expiry_at":   db_row.get("token_expiry_at",   ""),
            }
            if not creds["client_id"] and not creds["app_key"]:
                raise HTTPException(400, "Fyers credentials are required. Enter them in the form or they must be cached in the database.")
            # Persist any newly-entered credentials to DB (api_key = app_key, password = pin)
            if form["client_id"] or form["api_key"]:
                await _srv._client_db.upsert_feeder_creds(
                    "fyers",
                    client_id=form["client_id"],
                    api_key=form["api_key"],
                    secret=form["secret"],
                    password=form["password"],
                    totp_secret=form["totp_secret"],
                )
            # Auto-generate access token via headless TOTP auth
            try:
                from broker_auth.headless_auth import headless_engine as _he
                ok, msg, token = await _he.validate_feeder_creds("fyers", creds)
                if ok and token:
                    creds["access_token"] = token
                    _srv._auth_alerts["feeder"] = ""
                    from broker_auth.headless_auth import _ist_eod
                    await _srv._client_db.update_feeder_token(
                        "fyers", token,
                        generated_at=datetime.now(IST).isoformat(),
                        expiry_at=_ist_eod(),
                    )
                else:
                    _srv._auth_alerts["feeder"] = f"Fyers auth failed: {msg}"
                    logger.error("Dashboard: Fyers headless auth failed: %s", msg)
                    # Invalidate cached token without touching credential fields
                    await _srv._client_db.update_feeder_token(
                        "fyers", "", generated_at="", expiry_at="",
                    )
                    return JSONResponse(
                        status_code=502,
                        content={"ok": False, "error": f"Fyers auth failed: {msg}"},
                    )
            except Exception as exc:
                _srv._auth_alerts["feeder"] = f"Fyers auth error: {exc}"
                logger.error("Dashboard: Fyers auth error: %s", exc)
                await _srv._client_db.update_feeder_token(
                    "fyers", "", generated_at="", expiry_at="",
                )
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Fyers auth error: {exc}"},
                )
            try:
                await feeder.start_single("fyers", creds)
            except Exception as exc:
                logger.error("Dashboard: fyers connect failed: %s", exc)
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Fyers connect failed: {exc}"},
                )
            return {"ok": True, "message": "Fyers feed stream initialized.", "provider": "fyers", "token_fresh": True}

        # ── ADMIN — Universal feeder OAuth flow (all providers) ──────────────

        @app.get("/api/admin/feeder/{provider_name}/auth-url", tags=["Admin"])
        async def api_admin_feeder_auth_url(
            provider_name: str,
            request: Request,
            _: dict = Depends(_require_admin),
        ):
            """Generate OAuth authorization URL for the admin data feeder."""
            from broker_auth.oauth_manager import generate_auth_url, build_state
            provider = provider_name.lower()
            db_row = _srv._client_db.get_feeder_creds_sync(provider) or {}
            api_key    = db_row.get("api_key", "")
            api_secret = db_row.get("secret", "")
            if not api_key:
                return {"ok": False, "error": f"{provider.upper()} App ID/Key not saved. Configure credentials first."}

            base_url     = _redirect_base(request, _srv._client_db)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("admin", "admin", provider)

            ok, url = generate_auth_url(provider, api_key, api_secret, callback_url, state)
            if ok:
                logger.info("[Admin] %s feeder OAuth URL generated → %s", provider.upper(), callback_url)
                return {"ok": True, "url": url, "callback_url": callback_url}
            return {"ok": False, "error": url}  # url contains instructions for manual providers

        # Keep legacy Fyers-specific URL for backward compat
        @app.get("/api/admin/feeder/fyers/auth-url", tags=["Admin"])
        async def api_fyers_auth_url(request: Request, _: dict = Depends(_require_admin)):
            from broker_auth.oauth_manager import generate_auth_url, build_state
            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            api_key    = db_row.get("api_key", "")
            api_secret = db_row.get("secret", "")
            if not api_key:
                return {"ok": False, "error": "Fyers App ID not saved."}
            base_url     = _redirect_base(request, _srv._client_db)
            callback_url = f"{base_url}/callback/fyers"
            state        = build_state("admin", "admin", "fyers")
            ok, url = generate_auth_url("fyers", api_key, api_secret, callback_url, state)
            return {"ok": ok, "url": url}

        @app.post("/api/admin/feeder/fyers/exchange-code", tags=["Admin"])
        async def api_fyers_exchange_code(
            request: Request, _: dict = Depends(_require_admin),
        ):
            """Step 2 of the Fyers OAuth flow: exchange auth_code for access token."""
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raw = {}

            # Accept either a full redirect URL or just the raw auth_code
            redirect_url = raw.get("redirect_url", "")
            auth_code    = raw.get("auth_code", "")
            if redirect_url and not auth_code:
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(redirect_url).query)
                auth_code = (qs.get("auth_code") or qs.get("code") or [""])[0]
            if not auth_code:
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "auth_code not found. Paste the full redirect URL or just the auth_code value."},
                )

            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            app_id     = db_row.get("api_key", "")
            secret_key = db_row.get("secret",  "")
            if not app_id or not secret_key:
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "Fyers App ID and Secret not in DB. Save credentials first."},
                )

            try:
                import hashlib, requests as _rq
                app_id_hash = hashlib.sha256(f"{app_id}:{secret_key}".encode()).hexdigest()
                resp = await asyncio.to_thread(
                    lambda: _rq.post(
                        "https://api-t1.fyers.in/api/v3/validate-authcode",
                        json={
                            "grant_type": "authorization_code",
                            "appIdHash":  app_id_hash,
                            "code":       auth_code,
                        },
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                        timeout=15,
                    ).json()
                )

                access_token = resp.get("access_token") or (resp.get("data") or {}).get("access_token")
                if not access_token:
                    msg = resp.get("message") or str(resp)
                    _srv._auth_alerts["feeder"] = f"Fyers OAuth failed: {msg}"
                    return JSONResponse(status_code=502, content={"ok": False, "error": f"Fyers: {msg}"})

                from broker_auth.headless_auth import _ist_eod
                await _srv._client_db.update_feeder_token(
                    "fyers", access_token,
                    generated_at=datetime.now(IST).isoformat(),
                    expiry_at=_ist_eod(),
                )
                _srv._auth_alerts["feeder"] = ""
                logger.info("Dashboard: Fyers OAuth token saved successfully.")

                # Rebuild creds dict for feeder start
                creds = {
                    "client_id":          db_row.get("client_id", ""),
                    "app_key":            app_id,
                    "secret":             secret_key,
                    "pin":                db_row.get("password", ""),
                    "totp_secret":        db_row.get("totp_secret", ""),
                    "access_token":       access_token,
                    "token_generated_at": datetime.now(IST).isoformat(),
                    "token_expiry_at":    _ist_eod(),
                }
                try:
                    await feeder.start_single("fyers", creds)
                except Exception as exc:
                    logger.warning("Dashboard: Fyers feeder start after OAuth: %s", exc)
                    return {"ok": True, "message": "Token saved. Feeder start failed — restart manually.", "token_fresh": True}

                return {"ok": True, "message": "Fyers connected via OAuth — token refreshed.", "provider": "fyers", "token_fresh": True}

            except Exception as exc:
                logger.error("Dashboard: Fyers exchange-code error: %s", exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

        # ── ADMIN — dual active-active feeder ────────────────────────────────

        @app.post("/api/admin/feeder/connect_dual", tags=["Admin"])
        async def api_feeder_connect_dual(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raw = {}

            # Merge form fields with DB fallback for each provider
            from broker_auth.headless_auth import headless_engine as _he, _ist_eod
            u_db = _srv._client_db.get_feeder_creds_sync("upstox") or {}
            f_db = _srv._client_db.get_feeder_creds_sync("fyers")  or {}

            u_form = {
                "client_id":   raw.get("upstox_client_id", ""),
                "api_key":     raw.get("upstox_api_key",   ""),
                "secret":      raw.get("upstox_secret",    ""),
                "password":    raw.get("upstox_password",  ""),
                "totp_secret": raw.get("upstox_totp",      ""),
            }
            f_form = {
                "client_id":   raw.get("fyers_client_id", ""),
                "api_key":     raw.get("fyers_app_key",   ""),
                "secret":      raw.get("fyers_secret",    ""),
                "password":    raw.get("fyers_pin",       ""),
                "totp_secret": raw.get("fyers_totp",      ""),
            }
            upstox_creds = {
                "client_id":   u_form["client_id"]   or u_db.get("client_id",   ""),
                "api_key":     u_form["api_key"]     or u_db.get("api_key",     ""),
                "secret":      u_form["secret"]      or u_db.get("secret",      ""),
                "password":    u_form["password"]    or u_db.get("password",    ""),
                "totp_secret": u_form["totp_secret"] or u_db.get("totp_secret", ""),
                "access_token":       u_db.get("access_token",      ""),
                "token_generated_at": u_db.get("token_generated_at",""),
                "token_expiry_at":    u_db.get("token_expiry_at",   ""),
            }
            fyers_creds = {
                "client_id":   f_form["client_id"]   or f_db.get("client_id",   ""),
                "app_key":     f_form["api_key"]     or f_db.get("api_key",     ""),
                "secret":      f_form["secret"]      or f_db.get("secret",      ""),
                "pin":         f_form["password"]    or f_db.get("password",    ""),
                "totp_secret": f_form["totp_secret"] or f_db.get("totp_secret", ""),
                "access_token":       f_db.get("access_token",      ""),
                "token_generated_at": f_db.get("token_generated_at",""),
                "token_expiry_at":    f_db.get("token_expiry_at",   ""),
            }
            # Persist any newly-entered form credentials to DB
            if u_form["client_id"] or u_form["api_key"]:
                await _srv._client_db.upsert_feeder_creds("upstox", **u_form)
            if f_form["client_id"] or f_form["api_key"]:
                await _srv._client_db.upsert_feeder_creds("fyers",  **f_form)

            # Auto-generate tokens for both providers via headless auth
            now_ist = datetime.now(IST).isoformat()
            eod     = _ist_eod()
            failures = []

            ok_u, msg_u, tok_u = await _he.validate_feeder_creds("upstox", upstox_creds)
            if ok_u and tok_u:
                upstox_creds["access_token"] = tok_u
                await _srv._client_db.update_feeder_token("upstox", tok_u, now_ist, eod)
            else:
                failures.append(f"Upstox: {msg_u}")
                upstox_creds = {}   # don't try to connect with a bad/missing token

            ok_f, msg_f, tok_f = await _he.validate_feeder_creds("fyers", fyers_creds)
            if ok_f and tok_f:
                fyers_creds["access_token"] = tok_f
                await _srv._client_db.update_feeder_token("fyers", tok_f, now_ist, eod)
            else:
                failures.append(f"Fyers: {msg_f}")
                fyers_creds = {}    # don't try to connect with a bad/missing token

            # Abort only if BOTH providers failed — partial success still starts the feeder
            if not ok_u and not ok_f:
                alert = "CRITICAL: " + " | ".join(failures)
                _srv._auth_alerts["feeder"] = alert
                logger.error("Dashboard: dual feeder auth failed (both providers): %s", alert)
                raise HTTPException(502, f"Both providers failed: {'; '.join(failures)}")

            if failures:
                _srv._auth_alerts["feeder"] = "WARNING: " + " | ".join(failures)
                logger.warning("Dashboard: dual feeder partial auth — %s", failures)
            else:
                _srv._auth_alerts["feeder"] = ""

            try:
                await feeder.start_dual(upstox_creds, fyers_creds)
            except Exception as exc:
                logger.error("Dashboard: start_dual failed: %s", exc)
                raise HTTPException(502, f"Dual feeder connect failed: {exc}")

            active = [p for p, ok in [("upstox", ok_u), ("fyers", ok_f)] if ok]
            warn   = f" | WARNING: {'; '.join(failures)}" if failures else ""
            return {
                "ok": True,
                "message": f"Dual feed active: {', '.join(active)}.{warn}",
                "provider": "dual",
                "active_providers": active,
            }

        # ── ADMIN — save-only credential endpoints ───────────────────────────

        @app.post("/api/admin/feeder/creds/upstox", tags=["Admin"])
        async def api_save_upstox_creds(
            body: _SaveUpstoxCredsSchema, _: dict = Depends(_require_admin),
        ):
            db = _srv._client_db
            if db is None:
                return JSONResponse(status_code=503, content={"ok": False, "error": "Database not available."})
            # Sanitise — Pydantic guarantees str but strip whitespace and guard None
            client_id   = str(body.client_id   or "").strip()
            api_key     = str(body.api_key     or "").strip()
            secret      = str(body.secret      or "").strip()
            password    = str(body.password    or "").strip()
            totp_secret = str(body.totp_secret or "").strip()
            if not any([client_id, api_key, secret, password, totp_secret]):
                return JSONResponse(status_code=400, content={"ok": False, "error": "At least one credential field must be provided."})
            try:
                await db.upsert_feeder_creds(
                    provider="upstox",
                    client_id=client_id,
                    api_key=api_key,
                    secret=secret,
                    password=password,
                    totp_secret=totp_secret,
                )
            except Exception as exc:
                logger.error("Dashboard: Upstox credential save failed: %s", exc, exc_info=exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": f"Database write failed: {exc}"})
            logger.info("Dashboard: Upstox feeder credentials saved to DB.")
            return {"ok": True, "message": "Upstox credentials saved."}

        @app.post("/api/admin/feeder/creds/fyers", tags=["Admin"])
        async def api_save_fyers_creds(
            body: _SaveFyersCredsSchema, _: dict = Depends(_require_admin),
        ):
            db = _srv._client_db
            if db is None:
                return JSONResponse(status_code=503, content={"ok": False, "error": "Database not available."})
            client_id   = str(body.client_id   or "").strip()
            api_key     = str(body.app_key     or "").strip()
            secret      = str(body.secret      or "").strip()
            password    = str(body.pin         or "").strip()
            totp_secret = str(body.totp_secret or "").strip()
            if not any([client_id, api_key, secret, password, totp_secret]):
                return JSONResponse(status_code=400, content={"ok": False, "error": "At least one credential field must be provided."})
            try:
                await db.upsert_feeder_creds(
                    provider="fyers",
                    client_id=client_id,
                    api_key=api_key,
                    secret=secret,
                    password=password,
                    totp_secret=totp_secret,
                )
            except Exception as exc:
                logger.error("Dashboard: Fyers credential save failed: %s", exc, exc_info=exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": f"Database write failed: {exc}"})
            logger.info("Dashboard: Fyers feeder credentials saved to DB.")
            return {"ok": True, "message": "Fyers credentials saved."}

        # ── ADMIN — RMS config ────────────────────────────────────────────────

        @app.get("/api/admin/rms", tags=["Admin"])
        async def api_rms_get(_: dict = Depends(_require_admin)):
            defaults = {
                "max_drawdown_pct":       5.0,
                "order_throttle_per_sec": 5,
                "squareoff_time":         "15:15",
                "distance_filter_pct":    5.0,
            }
            rms = getattr(_srv._cfg, "rms", {})
            return {**defaults, **rms}

        @app.post("/api/admin/rms", tags=["Admin"])
        async def api_rms_post(request: Request, _: dict = Depends(_require_admin)):
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            if not hasattr(_srv._cfg, "rms") or not isinstance(_srv._cfg.rms, dict):
                _srv._cfg.rms = {}
            _srv._cfg.rms.update(raw)
            if "squareoff_time" in raw and hasattr(_srv._cfg, "squareoff_time"):
                _srv._cfg.squareoff_time = raw["squareoff_time"]
            logger.info("Dashboard: RMS config updated: %s", raw)
            return {"ok": True}

        # ── ADMIN — force liquidate all positions ─────────────────────────────

        @app.post("/api/admin/force_liquidate", tags=["Admin"])
        async def api_force_liquidate(_: dict = Depends(_require_admin)):
            n = 0
            if _srv._registry:
                for client in _srv._registry.all_active():
                    try:
                        client.halt()
                        n += 1
                    except Exception as exc:
                        logger.warning(
                            "Dashboard: force_liquidate: halt(%s) raised: %s",
                            getattr(client, "client_id", "?"), exc,
                        )
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":   SysEvent.KILL_SWITCH,
                "message": "Force liquidation triggered via dashboard.",
                "ts":      datetime.now(IST).isoformat(),
            })
            logger.critical("Dashboard: FORCE LIQUIDATE — halted %d clients.", n)
            return {"ok": True, "halted": n}

        # ── ADMIN — live broker positions ────────────────────────────────────

        @app.get("/api/admin/positions", tags=["Admin"])
        async def api_positions(_: dict = Depends(_require_admin)):
            """Fetch open positions from all authenticated broker bindings."""
            router = _srv._router
            if router is None:
                return {"ok": True, "positions": []}
            results = []
            for client_id, bindings in router._brokers.items():
                for binding_id, broker in bindings.items():
                    try:
                        positions = await broker.get_positions()
                        for pos in positions:
                            if pos.qty == 0:
                                continue
                            results.append({
                                "client_id":  client_id,
                                "binding_id": binding_id,
                                "symbol":     pos.symbol,
                                "qty":        pos.qty,
                                "avg_price":  round(pos.avg_price, 2),
                                "pnl":        round(pos.pnl, 2),
                                "product":    pos.product,
                            })
                    except Exception as exc:
                        logger.warning("Dashboard: positions fetch failed %s/%s: %s",
                                       client_id, binding_id, exc)
            return {"ok": True, "positions": results}

        # ── ADMIN — checkpoint ────────────────────────────────────────────────

        @app.post("/api/admin/checkpoint", tags=["Admin"])
        async def api_checkpoint(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CHECKPOINT_REQUESTED",
                "ts":    now_ist,
            })
            logger.info("Dashboard: checkpoint requested at %s.", now_ist)
            return {"ok": True, "ts": now_ist}

        # ── ADMIN — warm boot ─────────────────────────────────────────────────

        @app.post("/api/admin/warm_boot", tags=["Admin"])
        async def api_warm_boot(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "WARM_BOOT_REQUESTED",
                "ts":    now_ist,
            })
            logger.info("Dashboard: warm boot requested at %s.", now_ist)
            return {"ok": True, "ts": now_ist}

        # ── ADMIN — client profile management ────────────────────────────────

        @app.get("/api/admin/clients", tags=["Admin"])
        async def api_clients_list(_: dict = Depends(_require_admin)):
            _refresh_live_pnl()
            import json as _json
            clients = []

            # Primary source: in-memory registry (has live runtime state)
            registry_ids: set = set()
            if _srv._registry is not None:
                for c in _srv._registry._clients.values():
                    d = _build_client_dict(c)
                    d["halted"]          = bool(getattr(c, "_halted", False))
                    d["lot_multiplier"]  = float(c.risk.size_multiplier)
                    d["broker_bindings"] = [
                        {"binding_id": b.binding_id, "provider": b.provider, "enabled": b.enabled}
                        for b in c.broker_bindings
                    ]
                    db_row = _srv._client_db.get_client_sync(c.client_id) or {}
                    raw_sel = db_row.get("strategy_selections", "[]") or "[]"
                    try:
                        d["strategy_selections"] = _json.loads(raw_sel)
                    except Exception:
                        d["strategy_selections"] = []
                    clients.append(d)
                    registry_ids.add(c.client_id)

            # Fallback / supplement: DB rows not yet in registry (unapproved or demo mode)
            if _srv._client_db is not None:
                db_rows = await asyncio.to_thread(_srv._client_db.get_all_clients_sync)
                for row in db_rows:
                    cid = row.get("client_id")
                    if not cid or cid in registry_ids:
                        continue  # already included from registry
                    raw_sel = row.get("strategy_selections", "[]") or "[]"
                    try:
                        strategy_selections = _json.loads(raw_sel)
                    except Exception:
                        strategy_selections = []
                    clients.append({
                        "client_id":          cid,
                        "name":               row.get("name", ""),
                        "email":              row.get("email", ""),
                        "capital":            float(row.get("capital", 0)),
                        "lot_multiplier":     float(row.get("lot_multiplier", 1.0)),
                        "is_admin_approved":  bool(row.get("is_admin_approved", 0)),
                        "is_active":          bool(row.get("is_active", 1)),
                        "halted":             False,
                        "broker_bindings":    [],
                        "strategy_selections": strategy_selections,
                    })

            return {"clients": clients, "ts": datetime.now(IST).isoformat()}

        @app.post("/api/admin/clients/register", tags=["Admin"])
        async def api_clients_register(
            body: _ClientRegisterSchema, _: dict = Depends(_require_admin),
        ):
            from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding

            if _srv._registry is None:
                raise HTTPException(503, "ClientRegistry not available.")
            risk = RiskProfile(
                capital=body.capital,
                max_risk_per_trade_pct=body.max_risk_pct,
                max_daily_loss_pct=body.max_daily_loss_pct,
                size_multiplier=body.lot_multiplier,
            )
            try:
                risk.validate()
            except AssertionError as exc:
                raise HTTPException(400, str(exc))

            profile = ClientProfile(
                client_id=body.client_id,
                name=body.name,
                risk=risk,
                enabled_strategies=body.strategies or ["A", "B", "C"],
            )
            try:
                _srv._registry.register(profile)
            except ValueError as exc:
                raise HTTPException(409, str(exc))

            if body.binding_id:
                binding = BrokerBinding(
                    binding_id=body.binding_id,
                    provider=body.provider or "mock",
                    lot_multiplier=body.lot_multiplier,
                )
                profile.broker_bindings.append(binding)
                if _srv._router:
                    try:
                        from execution_bridge.base_broker import create_broker
                        from execution_bridge.parallel_worker_pool import ClientExecutionWorker
                        broker = create_broker(binding, body.client_id)
                        _srv._router._brokers.setdefault(body.client_id, {})[body.binding_id] = broker
                        worker = ClientExecutionWorker(
                            client=profile,
                            brokers=_srv._router._brokers[body.client_id],
                            bus=_srv._bus,
                            cfg=_srv._cfg,
                        )
                        await _srv._router._pool.add_worker(worker)
                        logger.info("Dashboard: spawned worker for new client %s.", body.client_id)
                    except Exception as exc:
                        logger.warning("Dashboard: worker spawn for %s failed: %s", body.client_id, exc)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":     "CLIENT_REGISTERED",
                "client_id": body.client_id,
            })
            logger.info("Dashboard: registered client %s.", body.client_id)
            return {"ok": True, "message": f"Client '{body.client_id}' registered and live."}

        @app.post("/api/admin/clients/{client_id}/reauth", tags=["Admin"])
        async def api_clients_reauth(
            client_id: str, _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            if _srv._router is None:
                raise HTTPException(503, "ExecutionRouter not wired to dashboard.")
            client_brokers = _srv._router._brokers.get(client_id) or {}
            if not client_brokers:
                raise HTTPException(404, f"No broker workers found for client {client_id!r}.")
            results = []
            for binding_id, broker in client_brokers.items():
                ok = await broker.authenticate()
                results.append({"binding_id": binding_id, "ok": ok})
                event = "AUTH_SUCCESS" if ok else "AUTH_FAILED"
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event": event, "client_id": client_id, "binding_id": binding_id,
                })
            all_ok = all(r["ok"] for r in results)
            logger.info("Dashboard: reauth %s — %s.", client_id, "OK" if all_ok else "PARTIAL_FAIL")
            return {"ok": all_ok, "results": results}

        # ── ADMIN — IV matrix ─────────────────────────────────────────────────

        @app.get("/api/admin/iv_matrix", tags=["Admin"])
        async def api_iv_matrix(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            option_cache = (
                getattr(_srv._ws_bridge, "_option_cache", None)
                or getattr(_srv._ws_bridge, "option_snapshot", None)
            )
            if option_cache:
                return {"strikes": list(option_cache.values()), "ts": now_ist}
            return {"strikes": [], "ts": now_ist}

        # ── ADMIN — audit log (JSON) ──────────────────────────────────────────

        @app.get("/api/admin/audit_log", tags=["Admin"])
        async def api_audit_log(_: dict = Depends(_require_admin)):
            import glob as _glob
            now_ist = datetime.now(IST).isoformat()
            log_dir = getattr(getattr(_srv._cfg, "storage", None), "log_dir", None) or "logs"
            try:
                files = sorted(_glob.glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime)
                if not files:
                    files = sorted(_glob.glob(os.path.join(log_dir, "*.txt")), key=os.path.getmtime)
                if not files:
                    return {"lines": [], "ts": now_ist}
                with open(files[-1], "r", encoding="utf-8", errors="replace") as fh:
                    lines = [ln.rstrip("\n") for ln in fh.readlines()[-200:]]
                return {"lines": lines, "ts": now_ist}
            except Exception as exc:
                logger.warning("Dashboard: audit_log read failed: %s", exc)
                return {"lines": [], "ts": now_ist, "error": str(exc)}

        # ── ADMIN — audit log (CSV download) ─────────────────────────────────

        @app.get("/api/admin/audit_log/csv", tags=["Admin"])
        async def api_audit_log_csv(
            token: str = Query(default=""),
            _: dict = Depends(_require_admin),
        ):
            import glob as _glob
            log_dir = getattr(getattr(_srv._cfg, "storage", None), "log_dir", None) or "logs"
            try:
                files = sorted(_glob.glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime)
                if not files:
                    files = sorted(_glob.glob(os.path.join(log_dir, "*.txt")), key=os.path.getmtime)
                if not files:
                    raise HTTPException(404, "No log files found.")
                return FileResponse(files[-1], media_type="text/csv", filename="audit_log.csv")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(500, f"Log file read failed: {exc}")

        # ── ANY AUTH — telemetry ──────────────────────────────────────────────

        @app.get("/api/admin/telemetry", tags=["Admin"])
        async def api_telemetry(_: dict = Depends(_current_user)):
            feeder   = _srv._feeder
            dual_lat = feeder.dual_latency if feeder and hasattr(feeder, "dual_latency") else {}
            return {
                "upstox_latency_ms": round(dual_lat.get("upstox", 0.0), 3),
                "fyers_latency_ms":  round(dual_lat.get("fyers",  0.0), 3),
                "ws_clients":        len(bridge._connections),
            }

        # ── WebSocket endpoint ────────────────────────────────────────────────

        @app.websocket("/ws")
        async def websocket_endpoint(
            websocket: WebSocket,
            token: str = Query(default=""),
        ):
            if not token:
                await websocket.close(code=1008, reason="auth required")
                return
            try:
                verify_token(token)
            except ValueError as exc:
                await websocket.close(code=1008, reason=str(exc))
                return

            await websocket.accept()
            bridge.add_connection(websocket)
            try:
                while True:
                    await websocket.receive_text()
            except (WebSocketDisconnect, Exception):
                pass
            finally:
                bridge.remove_connection(websocket)

        # ── ADMIN — Trap Scanner config ───────────────────────────────────────

        @app.get("/api/admin/trap_scanner/settings", tags=["Admin"])
        async def api_trap_scanner_settings_get(_: dict = Depends(_require_admin)):
            """Return the current trap_scanner global admin settings (stored in system_settings)."""
            import json
            raw = await asyncio.to_thread(
                _srv._client_db.get_setting_sync, "trap_scanner", ""
            )
            cfg = {}
            if raw:
                try:
                    cfg = json.loads(raw)
                except Exception:
                    pass
            return {"ok": True, "settings": cfg}

        @app.post("/api/admin/trap_scanner/settings", tags=["Admin"])
        async def api_trap_scanner_settings_save(
            request: Request, _: dict = Depends(_require_admin),
        ):
            """Persist trap_scanner admin settings (htf_minutes, ltf_minutes, per_index config etc.)."""
            import json
            try:
                body = await request.json()
            except Exception:
                return {"ok": False, "error": "Invalid JSON body."}
            # Whitelist top-level keys
            allowed_top = {"htf_minutes", "ltf_minutes", "gap_threshold_pct", "per_index"}
            filtered = {k: v for k, v in body.items() if k in allowed_top}
            if not filtered:
                return {"ok": False, "error": "No valid fields provided."}
            # Load existing and merge
            raw = await asyncio.to_thread(_srv._client_db.get_setting_sync, "trap_scanner", "{}")
            try:
                existing = json.loads(raw)
            except Exception:
                existing = {}
            existing.update(filtered)
            await _srv._client_db.set_setting("trap_scanner", json.dumps(existing))
            logger.info("TrapScanner admin settings saved: %s", filtered)
            return {"ok": True, "message": "Trap scanner settings saved."}

        @app.get("/api/admin/trap_scanner/status", tags=["Admin"])
        async def api_trap_scanner_status(_: dict = Depends(_require_admin)):
            """Return live telemetry for all running trap scanner books."""
            mgr = getattr(_srv, "_trap_scanner_manager", None)
            if mgr is None:
                return {"ok": True, "books": []}
            return {"ok": True, "books": mgr.telemetry_all()}

        return app

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def serve(self, host: str = "0.0.0.0", port: int = 5000) -> None:
        try:
            import uvicorn
        except ImportError:
            logger.error(
                "uvicorn not installed — dashboard will not start. "
                "Install with: pip install uvicorn[standard]"
            )
            return

        # Pre-flight port check — avoids confusing raw uvicorn error on Windows
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
            _s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                _s.bind((host if host != "0.0.0.0" else "127.0.0.1", port))
            except OSError:
                logger.error(
                    "Dashboard: port %d is already in use — "
                    "stop the previous instance or choose a different --port.  "
                    "System continues without dashboard.",
                    port,
                )
                return

        await self._client_db.initialise()

        config = uvicorn.Config(
            app=self._app,
            host=host,
            port=port,
            log_level="warning",
            loop="none",
            lifespan="off",
        )
        self._uvicorn_server = uvicorn.Server(config)
        self._uvicorn_server.install_signal_handlers = lambda: None

        logger.info("Dashboard: http://%s:%d  (WebSocket: ws://%s:%d/ws)", host, port, host, port)

        # Kick off background boot-time feeder auto-connect after a short settle delay
        boot_task = asyncio.create_task(
            self._boot_feeder_auto_connect(), name="boot_feeder_auto_connect"
        )

        try:
            await asyncio.gather(
                self._uvicorn_server.serve(),
                self._ws_bridge.run(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            if not boot_task.done():
                boot_task.cancel()

    async def _boot_feeder_auto_connect(self) -> None:
        """
        On server startup, check DB for cached feeder credentials and
        auto-reconnect if found.  Runs after a 4-second settle delay so the
        feeder object is fully initialised before we touch it.
        """
        await asyncio.sleep(4.0)
        try:
            feeder = self._feeder
            if feeder is None:
                return

            from broker_auth.headless_auth import headless_engine as _he, _token_is_fresh, _ist_eod
            u_row = self._client_db.get_feeder_creds_sync("upstox") or {}
            f_row = self._client_db.get_feeder_creds_sync("fyers")  or {}

            has_upstox = bool(u_row.get("client_id") or u_row.get("api_key"))
            has_fyers  = bool(f_row.get("client_id") or f_row.get("api_key"))

            if not has_upstox and not has_fyers:
                logger.info("DashboardServer: No cached feeder credentials — skipping auto-connect.")
                return

            logger.info(
                "DashboardServer: Boot-time auto-connect (upstox=%s, fyers=%s).",
                has_upstox, has_fyers,
            )
            now_ist = datetime.now(IST).isoformat()
            eod     = _ist_eod()

            upstox_creds: dict = {}
            fyers_creds:  dict = {}

            if has_upstox:
                upstox_creds = {
                    "client_id":          u_row.get("client_id", ""),
                    "api_key":            u_row.get("api_key", ""),
                    "access_token":       u_row.get("access_token", ""),
                    "token_generated_at": u_row.get("token_generated_at", ""),
                    "token_expiry_at":    u_row.get("token_expiry_at", ""),
                }
                if not _token_is_fresh(u_row.get("token_generated_at",""), u_row.get("token_expiry_at","")):
                    logger.info("DashboardServer: Upstox token stale — will attempt stream without fresh token.")
                    if not upstox_creds["access_token"]:
                        has_upstox = False

            if has_fyers:
                fyers_creds = {
                    "client_id":          f_row.get("client_id", ""),
                    "api_key":            f_row.get("api_key", ""),
                    "access_token":       f_row.get("access_token", ""),
                    "token_generated_at": f_row.get("token_generated_at", ""),
                    "token_expiry_at":    f_row.get("token_expiry_at", ""),
                }
                if not _token_is_fresh(f_row.get("token_generated_at",""), f_row.get("token_expiry_at","")):
                    logger.info("DashboardServer: Fyers token stale — will attempt stream without fresh token.")
                    if not fyers_creds["access_token"]:
                        has_fyers = False

            # Connect the feeder with whatever credentials are ready
            try:
                if has_upstox and has_fyers:
                    await feeder.start_dual(upstox_creds, fyers_creds)
                    logger.info("DashboardServer: Boot auto-connect — dual feed active.")
                elif has_upstox:
                    await feeder.start_single("upstox", upstox_creds)
                    logger.info("DashboardServer: Boot auto-connect — Upstox feed active.")
                elif has_fyers:
                    await feeder.start_single("fyers", fyers_creds)
                    logger.info("DashboardServer: Boot auto-connect — Fyers feed active.")
            except Exception as exc:
                logger.error("DashboardServer: Boot auto-connect feeder start failed: %s", exc)
        except Exception as exc:
            logger.warning("DashboardServer: _boot_feeder_auto_connect error: %s", exc)

    @property
    def _sell_straddles(self) -> list:
        """Live list of SellStraddle books — per-binding from the manager, else legacy static."""
        if self._straddle_manager is not None:
            return self._straddle_manager.books
        return self._sell_straddles_static

    def _find_ss_book(self, client_id: str, binding_id: str, underlying: str):
        """Locate the per-binding book for this deployment; fall back to per-underlying match."""
        if self._straddle_manager is not None:
            b = self._straddle_manager.find(client_id, binding_id, underlying)
            if b is not None:
                return b
        u = str(underlying).upper()
        for s in self._sell_straddles:
            if getattr(s, "_underlying", None) == u and (
                not getattr(s, "_client_id", "") or
                (s._client_id == client_id and s._binding_id == binding_id)
            ):
                return s
        return None

    def stop(self) -> None:
        self._ws_bridge.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _compute_live_pnls(self) -> None:
        """Set each active client's _daily_pnl in REAL RUPEES = booked (today's closed trades
        from the History ledger) + running (unrealized of open positions × lot size). This is
        the single source of truth so the header, admin dashboard and position panel all agree
        with the History tab (no more points-vs-rupees mismatch)."""
        try:
            clients = self._registry.all_active() if self._registry else []
            sslist  = self._sell_straddles or []
            iclist  = self._iron_condors or []
            trap    = self._trap_engine
            from data_layer import trade_history as _th
            _today = datetime.now(IST).date().isoformat()

            def _find(lst, u):
                for s in lst:
                    if getattr(s, "_underlying", None) == u:
                        return s
                return None

            def _lot(u):
                return int(self._cfg.exchange.lot_sizes.get(u, 0) or 0) if self._cfg else 0

            for c in clients:
                # Booked (₹) — today's closed trades from History (the source of truth).
                try:
                    _recs = _th.load(c.client_id, 500)
                    booked = sum(float(r.get("pnl", 0) or 0) for r in _recs
                                 if str(r.get("ts", ""))[:10] == _today)
                except Exception:
                    booked = 0.0
                # Running (₹) — unrealized of open positions across this client's deployments.
                try:
                    deps = self._client_db.get_deployments_sync(c.client_id)
                except Exception:
                    deps = []
                running = 0.0
                for d in deps:
                    sname = d.get("strategy_name", "")
                    u = (d.get("underlying") or d.get("assigned_instrument") or "").upper()
                    if sname == "sell_straddle":
                        s = self._find_ss_book(c.client_id, d.get("binding_id", ""), u)
                        p = getattr(s, "_position", None) if s else None
                        if p and getattr(p, "status", "open") == "open":
                            running += float(getattr(p, "unrealized_pnl", 0.0) or 0.0) * _lot(u)
                    elif sname == "iron_condor":
                        s = _find(iclist, u)
                        p = getattr(s, "_position", None) if s else None
                        if p and getattr(p, "status", "open") == "open":
                            running += float(getattr(p, "total_pnl_pts", 0.0) or 0.0) * int(getattr(p, "lot_size", 0) or 0)
                    elif sname == "trap_trading" and trap is not None:
                        op   = getattr(trap, "_open_positions", {}) or {}
                        prem = getattr(trap, "_prem_cache", {}) or {}
                        for _tid, tup in op.items():
                            try:
                                _t, osym, ep, q = tup
                            except Exception:
                                continue
                            ltp = float(prem.get(osym, ep) or ep)
                            running += (float(ep) - ltp) * abs(int(q))
                try:
                    c._daily_pnl = round(booked + running, 2)
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("_compute_live_pnls failed: %s", exc)

    def _client_summary(self) -> List[dict]:
        if self._registry is None:
            return []
        self._compute_live_pnls()   # fresh client-wise P&L for the admin dashboard
        import json as _json
        result = []
        for c in self._registry.all_active():
            d = _build_client_dict(c)
            # The admin Client Portfolio reads strategy_selections (not enabled_strategies),
            # so include it from the DB — otherwise it always renders "None selected".
            try:
                row = self._client_db.get_client_sync(c.client_id)
                raw = (row or {}).get("strategy_selections", "[]") or "[]"
                d["strategy_selections"] = _json.loads(raw)
            except Exception:
                d["strategy_selections"] = []
            result.append(d)
        return result

    def _broker_summary(self) -> List[dict]:
        if self._registry is None:
            return []
        result = []
        for c in self._registry.all_active():
            cid = c.client_id
            for binding in c.broker_bindings:
                broker = (
                    (self._router._brokers.get(cid) or {}).get(binding.binding_id)
                    if self._router else None
                )
                result.append({
                    "client_id":        cid,
                    "client_name":      getattr(c, "name", cid),
                    "binding_id":       binding.binding_id,
                    "provider":         binding.provider,
                    "label":            binding.label,
                    "api_key_set":      bool(binding.api_key),
                    "is_authenticated": broker.is_authenticated if broker else False,
                    "enabled":          binding.enabled,
                })
        return result


# ── Module-level helper (used inside route closures) ─────────────────────────

def _build_client_dict(c) -> dict:
    return {
        "client_id":      c.client_id,
        "name":           getattr(c, "name", c.client_id),
        "tradeable":      c.is_tradeable(),
        "daily_pnl":      round(float(getattr(c, "_daily_pnl", 0.0)), 2),
        "capital":        float(c.risk.capital),
        "max_risk_pct":   float(c.risk.max_risk_per_trade_pct),
        "daily_loss_pct": float(c.risk.max_daily_loss_pct),
        "strategies":     list(c.enabled_strategies),
        "brokers":        [b.provider for b in c.broker_bindings if b.enabled],
    }
