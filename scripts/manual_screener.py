"""
Standalone intraday F&O screener — manual chart-review shortlist.

Not part of any live strategy (SellStraddle / D1 Trap / FVG). Pure price-action /
market-breadth / OI filter, no indicators. Run manually during market hours:

    python scripts/manual_screener.py

Pipeline:
  1. NSE F&O securities universe (symbol, LTP, %chg)          -> api/equity-stockIndices
  2. Market breadth (advances vs declines) from Moneycontrol  -> bias BULLISH/BEARISH
  3. Filter universe by |%chg| >= 2.0%, direction per bias
  4. Cross-reference NSE OI Spurts table, OI %chg >= 7.0%, same direction (buildup)
  5. Print final 1-2 candidate table
"""

from __future__ import annotations

import random
import re
import sys
import time
from dataclasses import dataclass

import pandas as pd
from curl_cffi import requests as curl_requests
from tabulate import tabulate

NSE_BASE = "https://www.nseindia.com"
NSE_FO_UNIVERSE_URL = f"{NSE_BASE}/api/equity-stockIndices?index=SECURITIES%20IN%20FO"
NSE_OI_SPURTS_URL = f"{NSE_BASE}/api/live-analysis-oi-spurts"
MC_BREADTH_URL = "https://www.moneycontrol.com/stocks/marketstats/nsemktstat/index.php"

PRICE_MOVE_THRESHOLD_PCT = 2.0
OI_CHANGE_THRESHOLD_PCT = 7.0

MAX_RETRIES = 4
RETRY_BACKOFF_SEC = 2.0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


def _random_headers(referer: str) -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Connection": "keep-alive",
    }


# Cookie-minting path: hit these real listing pages (not just "/") so Akamai issues
# cookies that the JSON API endpoints will actually accept.
_COOKIE_MINT_PATHS = (
    "/",
    "/market-data/live-equity-market",
    "/option-chain",
)


def _mint_cookies(session: curl_requests.Session) -> None:
    for path in _COOKIE_MINT_PATHS:
        try:
            resp = session.get(
                f"{NSE_BASE}{path}",
                headers=_random_headers(NSE_BASE),
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"[Session] mint page {path} returned {resp.status_code} (continuing)")
        except Exception as exc:  # noqa: BLE001
            print(f"[Session] mint page {path} failed: {exc} (continuing)")
        time.sleep(0.5)


def _new_nse_session() -> curl_requests.Session:
    """NSE's Akamai WAF blocks plain `requests` outright (even on the homepage) on
    many networks — curl_cffi's Chrome TLS fingerprint impersonation is required, the
    same technique already used for Upstox auth in broker_auth/headless_auth.py."""
    session = curl_requests.Session(impersonate="chrome131")
    _mint_cookies(session)
    return session


def _get_json_with_retries(
    session: curl_requests.Session, url: str, referer: str
) -> dict:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=_random_headers(referer), timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (401, 403, 429):
                # Likely a stale/blocked session — refresh cookies and retry.
                session.cookies.clear()
                _mint_cookies(session)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001 - deliberately broad for retry loop
            last_exc = exc
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts") from last_exc


@dataclass
class FOStock:
    symbol: str
    ltp: float
    pchange: float


def fetch_fo_universe(session: curl_requests.Session) -> list[FOStock]:
    data = _get_json_with_retries(
        session, NSE_FO_UNIVERSE_URL, referer=f"{NSE_BASE}/market-data/live-equity-market"
    )
    rows = data.get("data", [])
    universe: list[FOStock] = []
    for row in rows:
        symbol = row.get("symbol")
        ltp = row.get("lastPrice")
        pchange = row.get("pChange")
        if symbol is None or ltp is None or pchange is None:
            continue
        universe.append(FOStock(symbol=symbol, ltp=float(ltp), pchange=float(pchange)))
    return universe


def fetch_market_bias_moneycontrol() -> str | None:
    """Scrape Moneycontrol's NSE market-stats page for advances/declines and derive
    BULLISH/BEARISH bias. Returns None (rather than raising) if the page layout can't
    be parsed or the site is unreachable — caller falls back to the NSE-internal
    breadth derived from the F&O universe itself."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = curl_requests.get(
                MC_BREADTH_URL,
                headers=_random_headers("https://www.moneycontrol.com/"),
                impersonate="chrome131",
                timeout=10,
            )
            resp.raise_for_status()
            html = resp.text

            adv_match = re.search(r"Advances?\D{0,20}?(\d{1,4})", html, re.IGNORECASE)
            dec_match = re.search(r"Declin(?:e|es)?\D{0,20}?(\d{1,4})", html, re.IGNORECASE)
            if not (adv_match and dec_match):
                raise ValueError("Could not locate advance/decline counts on page")

            advances = int(adv_match.group(1))
            declines = int(dec_match.group(1))
            print(f"[Breadth] Moneycontrol Advances={advances}  Declines={declines}")
            return "BULLISH" if advances > declines else "BEARISH"
        except Exception as exc:  # noqa: BLE001
            print(f"[Breadth] Moneycontrol attempt {attempt} failed: {exc}")
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    print("[Breadth] Moneycontrol unreachable after retries — falling back to F&O-internal breadth")
    return None


def fno_internal_bias(universe: list[FOStock]) -> str:
    """Fallback breadth signal: count advances/declines directly within the F&O
    universe already fetched in Step 1, instead of failing the whole run when
    Moneycontrol's page layout changes or blocks the request."""
    advances = sum(1 for s in universe if s.pchange > 0)
    declines = sum(1 for s in universe if s.pchange < 0)
    print(f"[Breadth] F&O-internal Advances={advances}  Declines={declines}")
    return "BULLISH" if advances >= declines else "BEARISH"


def fetch_market_bias(universe: list[FOStock]) -> str:
    bias = fetch_market_bias_moneycontrol()
    if bias is not None:
        return bias
    return fno_internal_bias(universe)


def filter_by_momentum(universe: list[FOStock], bias: str) -> list[FOStock]:
    if bias == "BULLISH":
        return [s for s in universe if s.pchange >= PRICE_MOVE_THRESHOLD_PCT]
    return [s for s in universe if s.pchange <= -PRICE_MOVE_THRESHOLD_PCT]


def fetch_oi_spurts(session: curl_requests.Session) -> dict[str, float]:
    """Returns {symbol: oi_pchange} for the NSE OI Spurts table."""
    data = _get_json_with_retries(
        session, NSE_OI_SPURTS_URL, referer=f"{NSE_BASE}/market-data/oi-spurts"
    )
    rows = data.get("data", []) or data.get("OISpurts", [])
    oi_map: dict[str, float] = {}
    for row in rows:
        symbol = row.get("symbol") or row.get("underlying")
        oi_pchange = row.get("oiChange") or row.get("perOIChange") or row.get("oiPercentChange")
        if symbol is None or oi_pchange is None:
            continue
        try:
            oi_map[symbol] = float(oi_pchange)
        except (TypeError, ValueError):
            continue
    return oi_map


def cross_reference(
    momentum_stocks: list[FOStock], oi_map: dict[str, float], bias: str
) -> pd.DataFrame:
    buildup_label = "Long Buildup" if bias == "BULLISH" else "Short Buildup"
    rows = []
    for stock in momentum_stocks:
        oi_pchange = oi_map.get(stock.symbol)
        if oi_pchange is None or oi_pchange < OI_CHANGE_THRESHOLD_PCT:
            continue
        rows.append(
            {
                "Symbol": stock.symbol,
                "LTP": stock.ltp,
                "Price % Change": round(stock.pchange, 2),
                "OI % Change": round(oi_pchange, 2),
                "Buildup Type": buildup_label,
                "Final Status": "PASSED ALL FILTERS",
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    print("=" * 70)
    print("Manual Intraday F&O Screener — chart-review shortlist (no indicators)")
    print("=" * 70)

    print("\n[Step 1] Fetching NSE F&O securities universe...")
    session = _new_nse_session()
    universe = fetch_fo_universe(session)
    print(f"  -> {len(universe)} F&O symbols loaded")

    print("\n[Step 2] Checking market breadth (Moneycontrol, with F&O-internal fallback)...")
    bias = fetch_market_bias(universe)
    print(f"  -> Market Bias: {bias}")

    print(f"\n[Step 3] Filtering for |Price % Change| >= {PRICE_MOVE_THRESHOLD_PCT}% "
          f"({'up' if bias == 'BULLISH' else 'down'} moves only)...")
    momentum_stocks = filter_by_momentum(universe, bias)
    print(f"  -> {len(momentum_stocks)} stocks passed the momentum filter")

    print("\n[Step 4] Cross-referencing NSE OI Spurts (OI % Change >= "
          f"{OI_CHANGE_THRESHOLD_PCT}%)...")
    oi_map = fetch_oi_spurts(session)
    result_df = cross_reference(momentum_stocks, oi_map, bias)

    print("\n[Step 5] Final Result")
    print("-" * 70)
    if result_df.empty:
        print("No stocks matched the 2% Price + 7% OI Spurt filter today.")
        return 0

    shortlist = result_df.sort_values("OI % Change", ascending=False).head(2)
    print(tabulate(shortlist, headers="keys", tablefmt="grid", showindex=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
