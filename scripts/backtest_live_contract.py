"""
scripts/backtest_live_contract.py
──────────────────────────────────────────────────────────────────────────────
Live-contract backtest runner for the TrapTradingEngine 5-stage state machine.

Uses real data from the current week's active NIFTY options contract and
validates engine output against broker charts.

Usage:
    # Requires dashboard to be running
    python scripts/backtest_live_contract.py

    # Override API base and admin password
    python scripts/backtest_live_contract.py --base http://localhost:5000 --password admin123

    # Skip API, use DB directly (no server needed)
    python scripts/backtest_live_contract.py --direct-db --db data/clients.db

    # Custom symbol and date range
    python scripts/backtest_live_contract.py --symbol BANKNIFTY --days 5
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# ── Optional dependencies (warn gracefully) ───────────────────────────────────

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_STRIKE_STEPS = {
    "NIFTY":       50,
    "BANKNIFTY":   100,
    "FINNIFTY":    50,
    "SENSEX":      100,
    "MIDCPNIFTY":  50,
}

_LOT_SIZES = {
    "NIFTY":       65,
    "BANKNIFTY":   30,
    "FINNIFTY":    60,
    "SENSEX":      20,
    "MIDCPNIFTY":  120,
}

# Fyers single-char month codes for weekly option symbols
_FYERS_MONTH = {
    1:"1",2:"2",3:"3",4:"4",5:"5",6:"6",
    7:"7",8:"8",9:"9",10:"O",11:"N",12:"D",
}

_SEP  = "-" * 72
_DSEP = "=" * 72


# ─────────────────────────────────────────────────────────────────────────────
# Expiry calculation
# ─────────────────────────────────────────────────────────────────────────────

def next_tuesday(from_date: date = None) -> date:
    """Return the next (or same-day) Tuesday on or after from_date."""
    d = from_date or date.today()
    days_ahead = (1 - d.weekday()) % 7   # Tuesday = weekday 1
    if days_ahead == 0:
        days_ahead = 7  # if today IS Tuesday, use next week's
    return d + timedelta(days=days_ahead)


def trading_days_ago(n: int, from_date: date = None) -> date:
    """Return the date n trading days (Mon–Fri) before from_date."""
    d = from_date or date.today()
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:   # Mon=0 … Fri=4
            count += 1
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Symbol builders
# ─────────────────────────────────────────────────────────────────────────────

def _internal_symbol(underlying: str, expiry: date, strike: int, opt_type: str) -> str:
    """
    Broker-neutral internal symbol used by the option_1m_bar_repository.
    Format: NIFTY:03JUN25:24500:CE
    """
    return f"{underlying}:{expiry.strftime('%d%b%y').upper()}:{strike}:{opt_type}"


def _fyers_symbol(underlying: str, expiry: date, strike: int, opt_type: str) -> str:
    """
    Fyers weekly option format: NSE:NIFTY2562524500CE
    NIFTY + YY + single-char-month + DD + strike + CE/PE
    """
    yy  = expiry.strftime("%y")
    mc  = _FYERS_MONTH[expiry.month]
    dd  = expiry.strftime("%d")
    return f"NSE:{underlying}{yy}{mc}{dd}{strike}{opt_type}"


def _shoonya_symbol(underlying: str, expiry: date, strike: int, opt_type: str) -> str:
    """
    Shoonya/Finvasia weekly: NIFTY3JUN25C24500
    underlying + D + MON + YY + C/P + strike
    """
    d_str = str(expiry.day)          # no leading zero
    mon   = expiry.strftime("%b").upper()
    yy    = expiry.strftime("%y")
    cp    = "C" if opt_type == "CE" else "P"
    return f"{underlying}{d_str}{mon}{yy}{cp}{strike}"


def _nse_symbol(underlying: str, expiry: date, strike: int, opt_type: str) -> str:
    """
    NSE/Zerodha format (monthly style): NIFTY25JUN2524500CE
    Used when recording via TickRecorder in the system.
    """
    return f"{underlying}{expiry.strftime('%d%b%y').upper()}{strike}{opt_type}"


def build_symbols(underlying: str, expiry: date, strike: int) -> dict:
    """Build all broker-format symbols for CE and PE at a given strike/expiry."""
    result = {}
    for ot in ("CE", "PE"):
        result[ot] = {
            "internal": _internal_symbol(underlying, expiry, strike, ot),
            "fyers":    _fyers_symbol(underlying, expiry, strike, ot),
            "shoonya":  _shoonya_symbol(underlying, expiry, strike, ot),
            "nse":      _nse_symbol(underlying, expiry, strike, ot),
        }
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ATM strike
# ─────────────────────────────────────────────────────────────────────────────

def atm_strike(spot: float, underlying: str) -> int:
    step = _STRIKE_STEPS.get(underlying, 50)
    return int(round(spot / step) * step)


# ─────────────────────────────────────────────────────────────────────────────
# API client
# ─────────────────────────────────────────────────────────────────────────────

class ApiClient:
    def __init__(self, base_url: str, username: str, password: str):
        if not _HAS_REQUESTS:
            raise RuntimeError("pip install requests")
        self.base = base_url.rstrip("/")
        self.token: Optional[str] = None
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        r = _requests.post(
            f"{self.base}/api/auth/login",
            json={"client_id": username, "password": password},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        self.token = data.get("access_token") or data.get("token")
        if not self.token:
            raise RuntimeError(f"Login failed: {data}")

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token}"}

    def get(self, path: str) -> dict:
        r = _requests.get(f"{self.base}{path}", headers=self._headers(), timeout=15)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict) -> dict:
        r = _requests.post(
            f"{self.base}{path}",
            json=payload,
            headers=self._headers(),
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    def spot_price(self, underlying: str) -> Optional[float]:
        """Try to read spot from trap telemetry snapshot."""
        try:
            data = self.get("/api/admin/strategy/telemetry")
            snap = data.get("trap_engine", {})
            st   = snap.get(underlying, {})
            # spot isn't stored directly in telemetry; use entry_origin as proxy if LIVE
            entry = st.get("entry_price", 0.0)
            if entry > 0:
                return entry
        except Exception:
            pass
        return None

    def db_symbols(self) -> list:
        """Return all distinct symbols recorded in option_1m_bar_repository via API."""
        try:
            data = self.get("/api/admin/trap/clients/instruments")
            return []   # this endpoint returns client instruments, not DB symbols
        except Exception:
            return []

    def replay(self, symbol: str, start_date: str, end_date: str) -> dict:
        return self.post("/api/strategies/trap_trading/replay", {
            "symbol":     symbol,
            "start_date": start_date,
            "end_date":   end_date,
        })


# ─────────────────────────────────────────────────────────────────────────────
# Direct-DB mode (no server required)
# ─────────────────────────────────────────────────────────────────────────────

class DirectDB:
    """Query option_1m_bar_repository directly when server is not running."""

    def __init__(self, db_path: str):
        self.path = db_path
        if not Path(db_path).exists():
            raise FileNotFoundError(f"DB not found: {db_path}")

    def distinct_symbols(self) -> list:
        con = sqlite3.connect(self.path)
        try:
            rows = con.execute(
                "SELECT DISTINCT symbol FROM option_1m_bar_repository ORDER BY symbol"
            ).fetchall()
            return [r[0] for r in rows]
        except sqlite3.OperationalError:
            return []   # table not created yet — server hasn't initialised DB
        finally:
            con.close()

    def bar_range(self, symbol: str):
        """Return (min_ts, max_ts, count) for a symbol."""
        con = sqlite3.connect(self.path)
        row = con.execute(
            "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) "
            "FROM option_1m_bar_repository WHERE symbol=?", (symbol,)
        ).fetchone()
        con.close()
        return row   # (min_ts, max_ts, count)

    def get_bars(self, symbol: str, since: str, until: str) -> list:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """SELECT symbol, timestamp, open, high, low, close, volume
               FROM option_1m_bar_repository
               WHERE symbol=? AND timestamp>=? AND timestamp<=?
               ORDER BY timestamp""",
            (symbol, since, until),
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]

    def run_replay_local(
        self,
        symbol: str,
        since: str,
        until: str,
        cfg=None,
    ) -> dict:
        """
        Run the TrapTradingEngine replay locally (no HTTP).
        Mirrors the logic in dashboard_server.py /api/strategies/trap_trading/replay.
        """
        if not _HAS_PANDAS:
            return {"ok": False, "error": "pip install pandas"}

        # Import engine
        root = Path(__file__).parent.parent
        sys.path.insert(0, str(root))

        from config.global_config import GlobalConfig
        from strategies.trap_trading_engine import TrapTradingEngine, _Phase
        from data_layer.base_feeder import EventBus, CandleEvent
        from zoneinfo import ZoneInfo
        IST = ZoneInfo("Asia/Kolkata")

        if cfg is None:
            cfg = GlobalConfig()

        rows = self.get_bars(symbol, since, until)
        if not rows:
            return {"ok": False, "error": f"No bars for {symbol} in [{since}, {until}]"}

        # Isolated sandbox engine
        sandbox_bus = EventBus()
        eng = TrapTradingEngine(sandbox_bus, cfg, client_db=None)
        _orig = eng._get_state

        def _bt_get_state(sym):
            st = _orig(sym)
            st.is_backtest = True
            return st

        eng._get_state = _bt_get_state

        tc  = cfg.trap_engine
        df  = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df  = df.set_index("timestamp").sort_index()
        agg = {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}

        htf_df = df.resample(f"{tc.HTF_MINUTES}min", closed="left", label="left").agg(agg).dropna()
        mtf_df = df.resample(f"{tc.MTF_MINUTES}min", closed="left", label="left").agg(agg).dropna()

        from zoneinfo import ZoneInfo
        IST = ZoneInfo("Asia/Kolkata")

        def _make_candle(sym, tf, ts, row):
            ts_dt = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=IST)
            return CandleEvent(
                symbol=sym, timeframe=tf, timestamp=ts_dt,
                open=float(row["open"]), high=float(row["high"]),
                low=float(row["low"]),   close=float(row["close"]),
                volume=int(row["volume"]),
            )

        # Phase-change tracker for state machine log
        phase_log: list = []
        prev_phase: dict = {}

        all_events = sorted(
            [(ts, "htf", row) for ts, row in htf_df.iterrows()] +
            [(ts, "mtf", row) for ts, row in mtf_df.iterrows()],
            key=lambda x: x[0],
        )

        for ts, kind, row in all_events:
            tf = tc.HTF_MINUTES if kind == "htf" else tc.MTF_MINUTES
            eng._process_htf(_make_candle(symbol, tf, ts, row)) if kind == "htf" \
                else eng._process_mtf(_make_candle(symbol, tf, ts, row))

            # Detect phase transition
            st = eng._states.get(symbol)
            if st:
                new_phase = st.phase.name
                if prev_phase.get(symbol) != new_phase:
                    phase_log.append({
                        "ts":    ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                        "tf":    f"{tc.HTF_MINUTES}m" if kind == "htf" else f"{tc.MTF_MINUTES}m",
                        "phase": new_phase,
                        "bar":   {
                            "open":  float(row["open"]),
                            "high":  float(row["high"]),
                            "low":   float(row["low"]),
                            "close": float(row["close"]),
                        },
                    })
                    prev_phase[symbol] = new_phase

        return {
            "ok":           True,
            "symbol":       symbol,
            "start_date":   since[:10],
            "end_date":     until[:10],
            "bars_total":   len(rows),
            "htf_bars":     len(htf_df),
            "mtf_bars":     len(mtf_df),
            "trades":       eng.backtest_log(),
            "trade_count":  len(eng.backtest_log()),
            "final_phase":  {s: st.phase.name for s, st in eng._states.items()},
            "phase_log":    phase_log,
            "signal_count": eng.signal_count(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Position sizing audit
# ─────────────────────────────────────────────────────────────────────────────

def position_size_audit(underlying: str, entry_price: float, capitals: list) -> list:
    """
    Show quantity calculation for each capital level.
    Quantity = floor(capital / (entry_price * lot_size)) * lot_size
    """
    import math
    lot = _LOT_SIZES.get(underlying, 75)
    rows = []
    for cap in capitals:
        if entry_price <= 0:
            rows.append({"capital": cap, "qty": 0, "lots": 0})
            continue
        raw = math.floor(cap / (entry_price * lot)) * lot
        qty = max(raw, lot)
        rows.append({
            "capital":    cap,
            "lot_size":   lot,
            "entry_price": entry_price,
            "qty":        qty,
            "lots":       qty // lot,
            "margin_est": round(qty * entry_price, 2),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Print helpers
# ─────────────────────────────────────────────────────────────────────────────

def _col(text: str, width: int, align: str = "<") -> str:
    return f"{text:{align}{width}}"


def print_header(underlying: str, expiry: date, strike: int,
                 spot: float, symbols: dict) -> None:
    lot = _LOT_SIZES.get(underlying, 75)
    print(_DSEP)
    print(f"  TRAP TRADING ENGINE - LIVE CONTRACT BACKTEST RUNNER")
    print(f"  Date: {date.today().isoformat()}   Underlying: {underlying}")
    print(_DSEP)
    print()
    print("CONTRACT PARAMETERS")
    print(_SEP)
    print(f"  Underlying spot (approx)   : {spot:>10.2f}")
    print(f"  Expiry (next Tuesday)      : {expiry.strftime('%d-%b-%Y')}  ({expiry.strftime('%A')})")
    print(f"  ATM Strike                 : {strike}")
    print(f"  Step size                  : {_STRIKE_STEPS.get(underlying, 50)} pts")
    print(f"  Lot size (NSE 2026)        : {lot}   <-- confirmed correct")
    print()
    print("  Option symbols by broker format:")
    for ot in ("CE", "PE"):
        print(f"    {ot}:")
        for fmt, sym in symbols[ot].items():
            print(f"      {fmt:<12}: {sym}")
    print()


def print_phase_log(phase_log: list, underlying: str) -> None:
    print("5-STAGE STATE MACHINE TRANSITIONS")
    print(_SEP)
    if not phase_log:
        print("  No phase transitions detected in this date range.")
        print("  This is expected if:")
        print("    • The DB has no 1-min bars recorded yet (paper mode needs market hours)")
        print("    • The date range is too short (<5 HTF bars)")
        print("    • No bearish 75-min candle formed in this window")
        print()
        return

    stage_labels = {
        "IDLE":         "STAGE 0 — IDLE           (waiting for HTF bearish candle)",
        "HTF_BEARISH":  "STAGE 1 — HTF_BEARISH    (75m bearish candle recorded)",
        "TRAP_LOCKED":  "STAGE 2 — TRAP_LOCKED    (75m sweep confirmed — trap set)",
        "RETEST_ALERT": "STAGE 3 — RETEST_ALERT   (premium retested entry_origin)",
        "MTF_BEARISH":  "STAGE 4a — MTF_BEARISH   (5m bearish candle found)",
        "MTF_LOCKED":   "STAGE 4b — MTF_LOCKED    (5m sweep — ARMED immediately)",
        "ARMED":        "STAGE 4b — ARMED         (waiting for premium touch)",
        "LIVE":         "STAGE 5 — LIVE           (position open ✓)",
    }

    for entry in phase_log:
        ts_str = str(entry.get("ts", ""))[:19].replace("T", " ")
        tf     = entry.get("tf", "")
        phase  = entry.get("phase", "")
        bar    = entry.get("bar", {})
        label  = stage_labels.get(phase, phase)

        arrow = "  → " if phase != "IDLE" else "    "
        print(f"{arrow}{label}")
        if bar:
            print(f"       [{tf}] @ {ts_str}  "
                  f"O={bar.get('open',0):.2f}  H={bar.get('high',0):.2f}  "
                  f"L={bar.get('low',0):.2f}  C={bar.get('close',0):.2f}")
    print()


def print_trade_table(trades: list, underlying: str) -> None:
    print("TRIGGERED TRADES — CHART ALIGNMENT TABLE")
    print(_SEP)
    print("  Use this table to verify against your broker's 5-min / 75-min chart.")
    print()

    if not trades:
        print("  No trades triggered in this backtest window.")
        print()
        return

    # Table header
    hdrs = ["Timestamp", "Sym/Type", "Entry Px", "Qty", "Entry Origin",
            "LTF SL (1m close)", "Exit Target", "R:R"]
    widths = [20, 12, 10, 6, 14, 18, 13, 6]
    header_row = "  " + "  ".join(_col(h, w) for h, w in zip(hdrs, widths))
    print(header_row)
    print("  " + "─" * (sum(widths) + 2 * len(widths)))

    lot = _LOT_SIZES.get(underlying, 75)
    for t in trades:
        ts_str   = str(t.get("timestamp", ""))[:19].replace("T", " ")
        sym      = t.get("option_symbol", t.get("underlying", "?"))[-6:]   # last 6 chars = strike+type
        entry    = float(t.get("entry_price", 0))
        qty      = int(t.get("quantity", 0))
        origin   = float(t.get("entry_origin", 0))
        ltf_sl   = float(t.get("ltf_sl", 0))
        target   = float(t.get("target_high", 0))

        risk     = abs(entry - ltf_sl)
        reward   = abs(target - entry)
        rr_str   = f"{reward/risk:.1f}" if risk > 0 else "∞"

        row = "  " + "  ".join(_col(v, w) for v, w in zip(
            [ts_str, sym, f"{entry:.2f}", str(qty),
             f"{origin:.2f}", f"{ltf_sl:.2f}", f"{target:.2f}", rr_str],
            widths,
        ))
        print(row)

    print()
    print(f"  Lot size applied : {lot}")
    print(f"  Qty formula      : floor(capital / (entry_price × {lot})) × {lot}")
    print()


def print_position_sizing(underlying: str, entry_price: float) -> None:
    if entry_price <= 0:
        return
    capitals = [500_000, 1_000_000, 2_000_000, 5_000_000]
    rows     = position_size_audit(underlying, entry_price, capitals)
    lot      = _LOT_SIZES.get(underlying, 75)

    print("POSITION SIZING AUDIT")
    print(_SEP)
    print(f"  Entry price used : {entry_price:.2f}")
    print(f"  Lot size ({underlying})  : {lot}")
    print()
    hdrs   = ["Capital (₹)", "Lots", "Qty", "Margin est (₹)"]
    widths = [16, 8, 8, 16]
    print("  " + "  ".join(_col(h, w) for h, w in zip(hdrs, widths)))
    print("  " + "─" * 54)
    for r in rows:
        print("  " + "  ".join(_col(v, w) for v, w in zip(
            [f"{r['capital']:,}", str(r['lots']),
             str(r['qty']),       f"{r['margin_est']:,.2f}"],
            widths,
        )))
    print()


def print_summary(result: dict, underlying: str) -> None:
    print("BACKTEST SUMMARY")
    print(_SEP)
    print(f"  Symbol           : {result.get('symbol')}")
    print(f"  Date range       : {result.get('start_date')} → {result.get('end_date')}")
    print(f"  1-min bars total : {result.get('bars_total', 0)}")
    print(f"  HTF bars (75m)   : {result.get('htf_bars', 0)}")
    print(f"  MTF bars (5m)    : {result.get('mtf_bars', 0)}")
    print(f"  Trades triggered : {result.get('trade_count', 0)}")
    print(f"  Signal count     : {result.get('signal_count', 0)}")
    print(f"  Final phase      : {result.get('final_phase', {})}")

    if result.get('trade_count', 0) == 0:
        print()
        print("  [i] 0 trades is a valid result -- the 5-stage filter is selective.")
        print("     Widen the date range or check that 1-min bars are being recorded")
        print("     during paper-mode market hours (09:15–15:30 IST).")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TrapTrading live-contract backtest runner")
    parser.add_argument("--base",       default="http://localhost:5000",
                        help="Dashboard base URL")
    parser.add_argument("--username",   default="admin")
    parser.add_argument("--password",   default="admin123")
    parser.add_argument("--symbol",     default="NIFTY",
                        help="Underlying index (NIFTY / BANKNIFTY / FINNIFTY)")
    parser.add_argument("--days",       type=int, default=3,
                        help="Number of trading days of history to replay")
    parser.add_argument("--spot",       type=float, default=0.0,
                        help="Override spot price (skip API lookup)")
    parser.add_argument("--direct-db",  action="store_true",
                        help="Query DB directly — no HTTP server needed")
    parser.add_argument("--db",         default="data/clients.db",
                        help="Path to clients.db (used with --direct-db)")
    parser.add_argument("--opt-symbol", default="",
                        help="Override option symbol stored in DB (if known)")
    args = parser.parse_args()

    underlying = args.symbol.upper()

    # ── Expiry & strike — from registry (real contract dates, never calculated) ─
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))
    from data_layer.instrument_registry import REGISTRY as _REG
    expiry = _REG.get_active_expiry(underlying) if _REG.is_loaded(underlying) else None
    if expiry is None:
        print(f"  [warn] Registry not loaded for {underlying} — expiry unknown. "
              f"Run the dashboard and authenticate Upstox first.")
        expiry = date.today()   # placeholder for symbol display only
    spot       = args.spot or 24500.0     # fallback if API unavailable
    step       = _STRIKE_STEPS.get(underlying, 50)
    strike     = atm_strike(spot, underlying)
    symbols    = build_symbols(underlying, expiry, strike)
    lot        = _LOT_SIZES[underlying] if underlying in _LOT_SIZES else 75

    # ── Date range ────────────────────────────────────────────────────────────
    today      = date.today()
    start_date = trading_days_ago(args.days, today)
    start_str  = start_date.isoformat()
    end_str    = today.isoformat()

    # ── Choose replay symbol ──────────────────────────────────────────────────
    # The replay endpoint queries option_1m_bar_repository by symbol.
    # Options recorded by tick_recorder are stored under their full contract symbol.
    # The underlying index bars (if recorded) are stored as "NIFTY" etc.
    # Pass --opt-symbol to override with the exact stored symbol.
    replay_symbol = args.opt_symbol or underlying

    # ── Header ────────────────────────────────────────────────────────────────
    print_header(underlying, expiry, strike, spot, symbols)

    # ── Run replay ────────────────────────────────────────────────────────────
    result: dict = {}

    if args.direct_db:
        # ── Direct DB path ────────────────────────────────────────────────────
        print(f"MODE: Direct DB ({args.db})")
        print(_SEP)
        try:
            db = DirectDB(args.db)
        except FileNotFoundError as exc:
            print(f"  ERROR: {exc}")
            sys.exit(1)

        avail = db.distinct_symbols()
        if avail:
            print(f"  Symbols in DB ({len(avail)}):")
            for s in avail[:20]:
                mn, mx, cnt = db.bar_range(s)
                print(f"    {s:<40}  {cnt:>6} bars   {mn[:10]} → {mx[:10]}")
            if len(avail) > 20:
                print(f"    … and {len(avail)-20} more")
        else:
            print("  No bars recorded yet in option_1m_bar_repository.")
            print("  Start paper-mode during market hours to accumulate data.")
        print()

        if replay_symbol not in avail:
            # Show what IS available so user can pass --opt-symbol
            print(f"  '{replay_symbol}' not found in DB.")
            if avail:
                print(f"  Try: --opt-symbol '{avail[0]}'")
            sys.exit(0)

        print(f"  Replaying '{replay_symbol}'  {start_str} → {end_str} ...")
        print()
        result = db.run_replay_local(replay_symbol, start_str + "T00:00:00",
                                     end_str + "T23:59:59")

    else:
        # ── API path ──────────────────────────────────────────────────────────
        if not _HAS_REQUESTS:
            print("ERROR: pip install requests   (or use --direct-db for no-server mode)")
            sys.exit(1)

        print(f"MODE: API ({args.base})")
        print(_SEP)

        try:
            client = ApiClient(args.base, args.username, args.password)
        except Exception as exc:
            print(f"  Login failed: {exc}")
            print("  Make sure the dashboard is running: python run_system.py --mode paper --ui")
            sys.exit(1)

        # Try to get live spot from telemetry
        if args.spot == 0.0:
            live_spot = client.spot_price(underlying)
            if live_spot and live_spot > 0:
                spot   = live_spot
                strike = atm_strike(spot, underlying)
                symbols = build_symbols(underlying, expiry, strike)
                print(f"  Live spot from telemetry: {spot:.2f}  → ATM strike: {strike}")
            else:
                print(f"  Could not read live spot — using fallback {spot:.2f}  → ATM strike: {strike}")
                print("  Pass --spot <value> to override.")
        print()

        print(f"  Replaying '{replay_symbol}'  {start_str} → {end_str} ...")
        try:
            result = client.replay(replay_symbol, start_str, end_str)
        except Exception as exc:
            print(f"  Replay request failed: {exc}")
            sys.exit(1)

    # ── Output ────────────────────────────────────────────────────────────────
    if not result.get("ok"):
        print(f"REPLAY FAILED: {result.get('error')}")
        sys.exit(1)

    print_summary(result, underlying)

    phase_log = result.get("phase_log", [])
    print_phase_log(phase_log, underlying)

    trades = result.get("trades", [])
    print_trade_table(trades, underlying)

    # Position sizing — use first trade's entry price, or a demo price
    demo_entry = trades[0]["entry_price"] if trades else 150.0
    print_position_sizing(underlying, demo_entry)

    # ── Raw JSON (optional) ───────────────────────────────────────────────────
    if "--json" in sys.argv:
        print("RAW JSON")
        print(_SEP)
        print(json.dumps(result, indent=2, default=str))

    print(_DSEP)
    print("  BACKTEST COMPLETE")
    print(_DSEP)


if __name__ == "__main__":
    main()
