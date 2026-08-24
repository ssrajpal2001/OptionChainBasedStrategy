"""
strategies/oi_orb_screener/screener.py -- pure/synchronous screener logic.

Faithful port of colab/oi_orb_screener/screener_nse_direct.py's pipeline
(NSE session, F&O universe fetch, OI-spurt fetch, NIFTY regime, shortlist
build, ORB bar accumulation, breakout signal evaluation) -- confirmed
working against real NSE endpoints on 2026-08-24 (NextApi/apiClient/
marketWatchApi for the F&O universe, live-analysis-oi-spurts-underlyings
for OI spurts, allIndices for NIFTY regime).

This module is deliberately kept SYNCHRONOUS (same `requests`-based shape
as the Colab original) so it stays a simple, directly-testable port with
zero asyncio coupling. strategies/oi_orb_screener/engine.py wraps every
call into this module with asyncio.to_thread() at the call site, per this
codebase's own blocking-I/O rule (CLAUDE.md "Development Notes").

CONFIG default REGIME_FILTER_ENABLED=True here (the real spec, unlike the
one-off comparison run done directly in Colab on 2026-08-24) -- the live
book always runs with the regime table enforced unless a deployment's own
strategy_params explicitly override it.

Deliberately excluded from this port (out of scope for the "does live
order placement + LTP tracking work" pass, see engine.py's own docstring):
_notify_browser (Colab/IPython-only), wait_until_actionable's blocking
sleep-loop (engine.py's own async loop handles daily gating instead),
run_screener_and_monitor's full orchestration loop (engine.py owns that,
adapted to asyncio).
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import requests

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")


CONFIG = {
    "OI_SPURT_MIN_PCT": 7.0,
    "PRICE_MOVE_MIN_PCT": 2.0,
    "STOCK_MOVE_ABORT_PCT": 4.0,
    "NIFTY_BULLISH_PCT": 0.3,
    "NIFTY_BEARISH_PCT": -0.3,
    "TOP_N_PER_SIDE": 5,
    "ORB_START": "09:15",
    "ORB_END": "09:30",
    "ENTRY_WINDOW_START": "09:30",
    "ENTRY_WINDOW_END": "10:30",
    "SCORE_WEIGHTS": {"price": 0.25, "oi_spurt": 0.25, "rel_strength": 0.25, "volume": 0.25},
    "POLL_SECONDS": 20,
    "MAX_MONITOR_MINUTES": 90,
    # Live default (unlike the 2026-08-24 one-off Colab comparison run, which
    # temporarily set this False for calibration only) -- the real regime
    # table stays enforced. strategy_params can still override per-deployment.
    "REGIME_FILTER_ENABLED": True,
    # 2026-08-24, direct user request -- TEMPORARY connectivity-test toggle,
    # NOT a spec change: default False (real ORB_START/ORB_END/ENTRY_WINDOW_
    # START/END timing always applies). Set True only to verify end-to-end
    # order placement + LTP subscription outside the real 09:30-10:30 IST
    # entry window (e.g. testing mid-afternoon right after a deploy) -- ORB
    # freezes immediately off whatever bars exist (real Yahoo backfill still
    # covers the real elapsed 09:15-09:30 session regardless of what time the
    # book actually starts), and the entry-window check is skipped entirely.
    # Turn this back OFF once connectivity is confirmed -- see
    # strategies/oi_orb_screener/engine.py's own docstring.
    "IGNORE_TIME_WINDOWS": False,
}

NSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
}

FNO_UNIVERSE_URL = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi"
FNO_UNIVERSE_PARAMS = {"functionName": "getIndicesData", "symbol": "SECURITIES IN F&O"}
ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
OI_SPURT_URL = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"


class NSESession:
    """Cookie warm-up + retry -- same pattern as the Colab script's own
    NSESession, confirmed working live 2026-08-24."""

    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers.update(NSE_HEADERS)
        self._warm()

    def _warm(self) -> None:
        try:
            self.s.get("https://www.nseindia.com", timeout=10)
            self.s.get("https://www.nseindia.com/market-data/oi-spurts", timeout=10)
            self.s.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
        except Exception:
            pass  # best-effort -- retried on first real 401/403 in get_json()

    def get_json(self, url, params=None, retries: int = 3):
        for attempt in range(retries):
            try:
                r = self.s.get(url, params=params, timeout=10)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (401, 403):
                    self._warm()
            except Exception:
                pass
            time.sleep(1.5 * (attempt + 1))
        return None


def fetch_fno_price_universe(nse: "NSESession") -> pd.DataFrame:
    """Response shape: {"data": {"aduCount": {...}, "data": [...rows...]}} --
    confirmed against a real captured request/response pair, 2026-08-24."""
    payload = nse.get_json(FNO_UNIVERSE_URL, params=FNO_UNIVERSE_PARAMS)
    outer = (payload or {}).get("data")
    rows = (outer or {}).get("data") if isinstance(outer, dict) else None
    if payload is None or outer is None or rows is None:
        raise RuntimeError(
            "Could not fetch the F&O price universe from NSE (NextApi/marketWatchApi/"
            "getIndicesData) -- unexpected response shape or blocked/rate-limited IP. "
            f"Raw payload top-level keys: "
            f"{list(payload.keys()) if isinstance(payload, dict) else type(payload)}."
        )
    if not rows:
        raise RuntimeError(
            "NSE returned an empty F&O universe list (0 rows) even though the request "
            f"itself succeeded. symbol value '{FNO_UNIVERSE_PARAMS['symbol']}' may have "
            "changed again on NSE's side."
        )
    df = pd.DataFrame(rows)
    wanted = ["symbol", "lastPrice", "pChange", "open", "dayHigh", "dayLow",
              "previousClose", "totalTradedVolume"]
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        wanted = [c for c in wanted if c in df.columns]
    return df[wanted].copy()


def fetch_nifty_pchange(nse: "NSESession") -> float:
    payload = nse.get_json(ALL_INDICES_URL)
    if not payload or "data" not in payload:
        raise RuntimeError("Could not fetch NIFTY 50 data from NSE (allIndices).")
    for row in payload["data"]:
        name = str(row.get("index", row.get("indexName", ""))).strip().upper()
        if name == "NIFTY 50":
            for key in ("percentChange", "pChange", "perChange"):
                if key in row:
                    return float(row[key])
    raise RuntimeError("NIFTY 50 not found in allIndices response.")


def _find_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "records", "OISpurts", "oi_spurts", "resultData"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
    return None


def _find_column(columns, candidates):
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def fetch_oi_spurts_nse(nse: "NSESession") -> pd.DataFrame:
    """symbol/avgInOI field names confirmed against a real live response,
    2026-08-23 (avgInOI verified arithmetically against a real row)."""
    payload = nse.get_json(OI_SPURT_URL)
    if payload is None:
        raise RuntimeError("NSE OI-Spurts endpoint returned nothing after retries.")
    records = _find_records(payload)
    if not records:
        raise RuntimeError("Could not locate the OI-Spurts record list in the NSE response.")
    df = pd.DataFrame(records)
    sym_col = _find_column(df.columns, ["symbol", "underlying", "underlyingValue"])
    pct_col = _find_column(df.columns, [
        "avgInOI", "percentageChangeInOI", "percentChangeOI", "pctChangeInOI",
        "changeInOIPercentage", "perChangeOI", "pChangeOI", "% chng in OI",
    ])
    if sym_col is None or pct_col is None:
        raise RuntimeError(f"Could not auto-detect symbol/%-change-in-OI columns. Columns: {list(df.columns)}.")
    out = pd.DataFrame({
        "symbol": df[sym_col].astype(str).str.upper().str.strip(),
        "oi_spurt_pct": pd.to_numeric(df[pct_col], errors="coerce"),
    }).dropna()
    return out


def build_shortlist(nse: "NSESession", cfg=CONFIG):
    universe = fetch_fno_price_universe(nse)
    nifty_pchange = fetch_nifty_pchange(nse)
    oi_spurts = fetch_oi_spurts_nse(nse)

    merged = universe.merge(oi_spurts, on="symbol", how="inner")
    merged = merged[merged["oi_spurt_pct"] >= cfg["OI_SPURT_MIN_PCT"]]
    merged = merged[merged["pChange"].abs() >= cfg["PRICE_MOVE_MIN_PCT"]]

    if merged.empty:
        return pd.DataFrame(), nifty_pchange

    merged["rel_strength"] = merged["pChange"] - nifty_pchange

    def _minmax(s: pd.Series) -> pd.Series:
        lo, hi = s.min(), s.max()
        if hi - lo < 1e-9:
            return pd.Series(0.5, index=s.index)
        return (s - lo) / (hi - lo)

    w = cfg["SCORE_WEIGHTS"]
    vol_col = "totalTradedVolume" if "totalTradedVolume" in merged.columns else None
    merged["score"] = (
        w["price"] * _minmax(merged["pChange"].abs())
        + w["oi_spurt"] * _minmax(merged["oi_spurt_pct"])
        + w["rel_strength"] * _minmax(merged["rel_strength"].abs())
        + (w["volume"] * _minmax(merged[vol_col]) if vol_col else 0.0)
    )

    bullish = merged[merged["pChange"] >= cfg["PRICE_MOVE_MIN_PCT"]].sort_values(
        "score", ascending=False).head(cfg["TOP_N_PER_SIDE"])
    bearish = merged[merged["pChange"] <= -cfg["PRICE_MOVE_MIN_PCT"]].sort_values(
        "score", ascending=False).head(cfg["TOP_N_PER_SIDE"])

    shortlist = pd.concat([bullish, bearish], ignore_index=True)
    return shortlist, nifty_pchange


class MinuteBars:
    """Buckets polled (symbol, price, timestamp) into 1-min OHLC bars in-process."""

    def __init__(self) -> None:
        self.bars: dict = defaultdict(dict)

    def on_quote(self, symbol: str, price: float, ts: datetime) -> None:
        if price <= 0:
            return
        key = ts.strftime("%H:%M")
        sym_bars = self.bars[symbol]
        b = sym_bars.get(key)
        if b is None:
            sym_bars[key] = {"o": price, "h": price, "l": price, "c": price}
        else:
            b["h"] = max(b["h"], price)
            b["l"] = min(b["l"], price)
            b["c"] = price

    def orb(self, symbol: str, start: str, end: str):
        keys = sorted(k for k in self.bars.get(symbol, {}) if start <= k < end)
        if not keys:
            return None, None
        highs = [self.bars[symbol][k]["h"] for k in keys]
        lows = [self.bars[symbol][k]["l"] for k in keys]
        return max(highs), min(lows)


@dataclass
class Signal:
    symbol: str
    side: str            # "CALL" | "PUT"
    reason: str           # "orb_high_breakout" | "orb_low_breakdown"
    trigger_price: float
    orb_high: float
    orb_low: float
    ts: str


def classify_nifty_regime(nifty_orb_pchange: float, cfg=CONFIG) -> str:
    if nifty_orb_pchange >= cfg["NIFTY_BULLISH_PCT"]:
        return "bullish"
    if nifty_orb_pchange <= cfg["NIFTY_BEARISH_PCT"]:
        return "bearish"
    return "neutral"


def evaluate_breakout(symbol, ltp, prev_close, orb_high, orb_low, regime,
                       already_fired: set, cfg=CONFIG, now: datetime = None) -> "Signal | None":
    """Same regime table as the Colab original: Bullish day -> CALL on ORB-high,
    PUT on ORB-low. Bearish day -> ORB-high breakout ignored, PUT on ORB-low.
    Neutral day -> no trade (unless REGIME_FILTER_ENABLED is explicitly off).

    Note: unlike the Colab original, this does NOT compute a strike/expiry --
    that belongs to stock_resolve.py (real Upstox-registry-resolved values),
    not a price-band/calendar heuristic embedded in the pure signal function."""
    regime_filter_on = cfg.get("REGIME_FILTER_ENABLED", True)
    if regime_filter_on and regime == "neutral":
        return None
    stock_move_pct = (ltp - prev_close) / prev_close * 100.0 if prev_close else 0.0
    if abs(stock_move_pct) >= cfg["STOCK_MOVE_ABORT_PCT"]:
        return None

    now = now or datetime.now(IST)

    if ltp > orb_high and (symbol, "CALL") not in already_fired:
        if not regime_filter_on or regime == "bullish":
            already_fired.add((symbol, "CALL"))
            return Signal(symbol, "CALL", "orb_high_breakout", ltp, orb_high, orb_low,
                          now.strftime("%H:%M:%S"))
        already_fired.add((symbol, "CALL"))
        return None

    if ltp < orb_low and (symbol, "PUT") not in already_fired:
        already_fired.add((symbol, "PUT"))
        return Signal(symbol, "PUT", "orb_low_breakdown", ltp, orb_high, orb_low,
                      now.strftime("%H:%M:%S"))

    return None


def backfill_orb_from_yahoo(bars: MinuteBars, symbols, cfg=CONFIG) -> None:
    """Best-effort only, mirrors the Colab original -- never required."""
    try:
        import yfinance as yf
    except ImportError:
        return
    try:
        tickers = [s + ".NS" for s in symbols]
        df = yf.download(tickers, period="1d", interval="1m", progress=False, group_by="ticker")
        for sym, ticker in zip(symbols, tickers):
            try:
                sub = df[ticker] if len(tickers) > 1 else df
            except Exception:
                continue
            for ts, row in sub.iterrows():
                ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
                key = ts_ist.strftime("%H:%M")
                if key >= cfg["ORB_END"]:
                    continue
                if key in bars.bars[sym]:
                    continue
                if pd.isna(row.get("High")) or pd.isna(row.get("Low")):
                    continue
                bars.bars[sym][key] = {
                    "o": float(row["Open"]), "h": float(row["High"]),
                    "l": float(row["Low"]), "c": float(row["Close"]),
                }
    except Exception:
        pass
