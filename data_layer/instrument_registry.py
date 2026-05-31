"""
data_layer/instrument_registry.py — Centralized instrument key resolver.

Single source of truth for mapping (underlying, expiry, strike, CE/PE)
to the exact broker-specific symbol or key required by:
  • Upstox WebSocket subscription & order placement  (instrument_key)
  • Fyers  WebSocket subscription & order placement  (trading_symbol)
  • Shoonya / AngelOne / Dhan                        (derived from SymbolTranslator)
  • StrikeRebalancer (subscription tokens)
  • TrapTradingEngine (LTP tracking)
  • HistoricalReplay (Upstox historical API)

Data source: Upstox get_option_contracts REST API.
  Called once per underlying at startup, then cached for the session.
  Upstox response contains instrument_key + trading_symbol for every
  active option contract — no 10 MB master JSON download needed.

Fyers / Shoonya / AngelOne symbols are derived deterministically via
SymbolTranslator — no API call needed for those brokers.

All I/O is synchronous — call via asyncio.to_thread() from async code.
No time.sleep. No module-level side effects.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Module-level cache: date_str -> list of all NSE instrument dicts (downloaded once/day)
_MASTER_CACHE: Dict[str, list] = {}

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Weekly expiry weekday per underlying (0=Mon … 6=Sun)
_EXPIRY_WEEKDAY: Dict[str, int] = {
    "NIFTY":       1,   # Tuesday
    "BANKNIFTY":   2,   # Wednesday
    "FINNIFTY":    1,   # Tuesday
    "MIDCPNIFTY":  0,   # Monday
    "SENSEX":      1,   # Tuesday
}

# Upstox underlying instrument key (used for get_option_contracts call)
_UPSTOX_UNDERLYING_KEY: Dict[str, str] = {
    "NIFTY":       "NSE_INDEX|Nifty 50",
    "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
    "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY":  "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX":      "BSE_INDEX|SENSEX",
}


# ─────────────────────────────────────────────────────────────────────────────
# InstrumentRegistry
# ─────────────────────────────────────────────────────────────────────────────

class InstrumentRegistry:
    """
    Centralized broker-key resolver for all active option contracts.

    Usage:
        registry = InstrumentRegistry()
        await asyncio.to_thread(registry.load_sync, "NIFTY", access_token)

        key = registry.get_upstox_key("NIFTY", date(2026,6,2), 24500, "CE")
        sym = registry.get_fyers_symbol("NIFTY", date(2026,6,2), 24500, "CE")
        tokens = registry.get_subscription_tokens("NIFTY", date(2026,6,2),
                                                   [24400,24450,24500,24550,24600],
                                                   provider="upstox")
    """

    def __init__(self) -> None:
        # {underlying: {(expiry_str, strike_int, opt_type): upstox_instrument_key}}
        self._upstox_keys: Dict[str, Dict[Tuple[str, int, str], str]] = {}
        # {underlying: sorted list of active expiry dates}
        self._expiries: Dict[str, List[date]] = {}
        # track which underlyings have been loaded
        self._loaded: Set[str] = set()

    # ── Loading ───────────────────────────────────────────────────────────────

    def load_sync(self, underlying: str, access_token: str = "") -> None:
        """
        Load active option contracts from Upstox static instrument master JSON.

        Downloads https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
        once per session (cached by date string), then filters to:
          segment=NSE_FO, instrument_type=OPT, underlying_symbol=underlying, expiry>=today

        Works 24/7 including weekends — static file, no live market required.
        Must be called via asyncio.to_thread() from async code.
        """
        import gzip
        import json
        from urllib.request import urlopen, Request

        today = date.today()
        cache_key = today.isoformat()

        # Download + cache the master JSON (10MB compressed, ~40MB parsed)
        # Cache is module-level to survive multiple load_sync calls in same process
        raw_instruments = _MASTER_CACHE.get(cache_key)
        if raw_instruments is None:
            url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
            logger.info("InstrumentRegistry: downloading Upstox NSE master from %s ...", url)
            try:
                req = Request(url, headers={"Accept-Encoding": "gzip"})
                with urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                # Handle both pre-decompressed and gzipped responses
                try:
                    raw_instruments = json.loads(gzip.decompress(raw))
                except Exception:
                    raw_instruments = json.loads(raw)
                _MASTER_CACHE[cache_key] = raw_instruments
                logger.info("InstrumentRegistry: master loaded — %d total instruments", len(raw_instruments))
            except Exception as exc:
                logger.error("InstrumentRegistry: master download failed: %s", exc)
                self._loaded.add(underlying)   # mark as attempted to avoid infinite retry
                return

        # Filter to this underlying's active options
        keys: Dict[Tuple[str, int, str], str] = {}
        expiry_set: Set[date] = set()

        for inst in raw_instruments:
            if isinstance(inst, dict):
                seg  = inst.get("segment", "")
                itype = inst.get("instrument_type", "")
                usym = inst.get("underlying_symbol", "")
                ikey = inst.get("instrument_key", "")
                ts   = inst.get("trading_symbol", "")
                exp_raw = inst.get("expiry", "")
                strike_raw = inst.get("strike_price", 0)
            else:
                seg  = getattr(inst, "segment", "")
                itype = getattr(inst, "instrument_type", "")
                usym = getattr(inst, "underlying_symbol", "")
                ikey = getattr(inst, "instrument_key", "")
                ts   = getattr(inst, "trading_symbol", "")
                exp_raw = getattr(inst, "expiry", "")
                strike_raw = getattr(inst, "strike_price", 0)

            if seg != "NSE_FO" or itype != "OPT" or usym != underlying:
                continue
            if not ikey:
                continue

            # Parse expiry
            try:
                if isinstance(exp_raw, (date, datetime)):
                    expiry_date = exp_raw.date() if isinstance(exp_raw, datetime) else exp_raw
                else:
                    expiry_date = date.fromisoformat(str(exp_raw)[:10])
            except (ValueError, TypeError):
                continue

            if expiry_date < today:
                continue

            # Determine option type from trading_symbol or instrument_key suffix
            if ts.endswith("CE") or ikey.endswith("CE"):
                opt_type = "CE"
            elif ts.endswith("PE") or ikey.endswith("PE"):
                opt_type = "PE"
            else:
                continue

            strike = int(round(float(strike_raw or 0)))
            if strike <= 0:
                continue

            keys[(expiry_date.isoformat(), strike, opt_type)] = ikey
            expiry_set.add(expiry_date)

        self._upstox_keys[underlying] = keys
        self._expiries[underlying] = sorted(expiry_set)
        self._loaded.add(underlying)

        logger.info(
            "InstrumentRegistry [%s]: %d contracts across expiries: %s",
            underlying, len(keys),
            ", ".join(e.isoformat() for e in sorted(expiry_set)[:6]),
        )

    def is_loaded(self, underlying: str) -> bool:
        return underlying in self._loaded

    # ── Expiry helpers ────────────────────────────────────────────────────────

    def get_active_expiry(self, underlying: str, from_date: date = None) -> Optional[date]:
        """
        Return the nearest active expiry on or after from_date.
        Falls back to mathematical calculation if registry not loaded.
        """
        from_date = from_date or date.today()
        expiries = self._expiries.get(underlying, [])
        for exp in expiries:
            if exp >= from_date:
                return exp
        # Fallback: mathematical next-expiry
        return _calc_next_expiry(underlying, from_date)

    def all_expiries(self, underlying: str) -> List[date]:
        """Return all loaded active expiry dates for an underlying."""
        return list(self._expiries.get(underlying, []))

    # ── Upstox ───────────────────────────────────────────────────────────────

    def get_upstox_key(
        self,
        underlying: str,
        expiry: date,
        strike: int,
        opt_type: str,
    ) -> str:
        """
        Return the Upstox instrument_key for order placement and historical API.
        Returns empty string if not found (contract not loaded or expired).
        """
        keys = self._upstox_keys.get(underlying, {})
        return keys.get((expiry.isoformat(), strike, opt_type), "")

    def get_upstox_index_key(self, underlying: str) -> str:
        """Return the Upstox instrument_key for the underlying spot index."""
        return _UPSTOX_UNDERLYING_KEY.get(underlying, f"NSE_INDEX|{underlying}")

    # ── Multi-broker symbol resolution ────────────────────────────────────────

    def get_broker_symbol(
        self,
        underlying: str,
        expiry: date,
        strike: int,
        opt_type: str,
        provider: str,
    ) -> str:
        """
        Return the correct symbol/key for a given broker provider.

        Upstox → instrument_key from registry (required for API)
        Fyers  → NSE:NIFTY2660224500CE (derived via SymbolTranslator)
        Shoonya → NIFTY2JUN26C24500   (derived via SymbolTranslator)
        AngelOne → NIFTY02JUN2624500CE (derived via SymbolTranslator)
        Dhan   → internal canonical str (token lookup done by broker)
        """
        from data_layer.symbol_translator import InternalSymbol, SymbolTranslator

        internal = InternalSymbol(
            underlying=underlying,
            strike=float(strike),
            option_type=opt_type,
            expiry=expiry,
        )

        p = provider.lower()
        if p == "upstox":
            key = self.get_upstox_key(underlying, expiry, strike, opt_type)
            if key:
                return key
            # Fallback to constructed format (may be rejected by API — log warning)
            logger.warning(
                "InstrumentRegistry: Upstox key not found for %s %s %d%s — "
                "using constructed fallback. Call load_sync() first.",
                underlying, expiry, strike, opt_type,
            )
            return SymbolTranslator.to_upstox(internal)

        elif p == "fyers":
            return SymbolTranslator.to_fyers(internal, is_monthly=is_monthly_expiry(expiry, underlying))

        elif p == "angelone":
            return SymbolTranslator.to_angelone(internal)

        elif p == "dhan":
            return SymbolTranslator.to_dhan_lookup_key(internal)

        elif p == "zerodha":
            return SymbolTranslator.to_angelone(internal)

        else:
            return str(internal)

    def get_subscription_tokens(
        self,
        underlying: str,
        expiry: date,
        strikes: List[int],
        provider: str,
        opt_types: List[str] = None,
    ) -> List[str]:
        """
        Build the list of subscription tokens for a given set of strikes.
        Used by StrikeRebalancer when calling feeder.subscribe_tokens().
        """
        if opt_types is None:
            opt_types = ["CE", "PE"]
        tokens = []
        for strike in strikes:
            for ot in opt_types:
                sym = self.get_broker_symbol(underlying, expiry, strike, ot, provider)
                if sym:
                    tokens.append(sym)
        return tokens

    def build_instrument_map(self, underlying: str) -> Dict[str, str]:
        """
        Build the {canonical_str: upstox_instrument_key} dict for
        UpstoxBroker.inject_instrument_map().

        canonical_str is the InternalSymbol.__str__ format:
          NIFTY:02JUN26:24500:CE
        """
        from data_layer.symbol_translator import InternalSymbol

        result: Dict[str, str] = {}
        keys = self._upstox_keys.get(underlying, {})
        for (expiry_str, strike, opt_type), inst_key in keys.items():
            try:
                expiry = date.fromisoformat(expiry_str)
                internal = InternalSymbol(
                    underlying=underlying,
                    strike=float(strike),
                    option_type=opt_type,
                    expiry=expiry,
                )
                result[str(internal)] = inst_key
            except Exception:
                continue
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton (shared across all subsystems)
# ─────────────────────────────────────────────────────────────────────────────

REGISTRY = InstrumentRegistry()


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ─────────────────────────────────────────────────────────────────────────────

def _calc_next_expiry(underlying: str, from_date: date) -> date:
    """Mathematical fallback: next weekly expiry on or after from_date."""
    target_wd = _EXPIRY_WEEKDAY.get(underlying, 1)
    days_ahead = (target_wd - from_date.weekday()) % 7
    return from_date + timedelta(days=days_ahead)


def is_monthly_expiry(expiry: date, underlying: str) -> bool:
    """
    True if this expiry is the monthly (last weekly expiry of the month).
    Monthly = the last occurrence of the weekly expiry weekday in the month.
    """
    wd = _EXPIRY_WEEKDAY.get(underlying, 1)
    # Check if adding 7 days crosses into the next month
    return (expiry + timedelta(days=7)).month != expiry.month


def next_expiry(underlying: str, from_date: date = None) -> date:
    """Public helper — uses REGISTRY if loaded, else mathematical fallback."""
    from_date = from_date or date.today()
    return REGISTRY.get_active_expiry(underlying, from_date) or _calc_next_expiry(underlying, from_date)
