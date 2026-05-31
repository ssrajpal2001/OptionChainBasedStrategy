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

    def load_sync(self, underlying: str, access_token: str, weeks_ahead: int = 8) -> None:
        """
        Load active option contracts for an underlying from Upstox API.

        Uses get_put_call_option_chain(instrument_key, expiry_date) for each
        of the next `weeks_ahead` weekly expiries. Each call returns OptionStrikeData
        objects with call_options.instrument_key and put_options.instrument_key.

        Must be called via asyncio.to_thread() from async code.
        Safe to call multiple times — subsequent calls refresh the cache.
        """
        try:
            import upstox_client
        except ImportError:
            logger.warning("InstrumentRegistry: upstox_client not installed — keys unavailable.")
            return

        underlying_key = _UPSTOX_UNDERLYING_KEY.get(underlying)
        if not underlying_key:
            logger.warning("InstrumentRegistry: no Upstox key for underlying '%s'", underlying)
            return

        cfg = upstox_client.Configuration()
        cfg.access_token = access_token
        api_client = upstox_client.ApiClient(cfg)
        api = upstox_client.OptionsApi(api_client)

        # Calculate next N weekly expiry dates for this underlying
        today = date.today()
        expiry_dates: List[date] = []
        d = today
        for _ in range(weeks_ahead):
            d = _calc_next_expiry(underlying, d)
            expiry_dates.append(d)
            d = d + timedelta(days=1)

        keys: Dict[Tuple[str, int, str], str] = {}
        expiry_set: Set[date] = set()

        for expiry_date in expiry_dates:
            expiry_str = expiry_date.isoformat()
            try:
                resp = api.get_put_call_option_chain(
                    instrument_key=underlying_key,
                    expiry_date=expiry_str,
                    api_version="2.0",
                )
            except Exception as exc:
                logger.debug(
                    "InstrumentRegistry [%s] expiry=%s: chain fetch failed: %s",
                    underlying, expiry_str, exc,
                )
                continue

            data = getattr(resp, "data", None)
            if not data:
                logger.debug("InstrumentRegistry [%s] expiry=%s: empty chain", underlying, expiry_str)
                continue

            items = data if isinstance(data, list) else list(data)
            count_before = len(keys)

            for strike_data in items:
                # strike_data is OptionStrikeData:
                #   strike_price: float
                #   expiry: datetime
                #   call_options: PutCallOptionChainData  (has .instrument_key)
                #   put_options:  PutCallOptionChainData  (has .instrument_key)
                strike_raw = (
                    strike_data.get("strike_price", 0)
                    if isinstance(strike_data, dict)
                    else getattr(strike_data, "strike_price", 0)
                )
                strike = int(round(float(strike_raw or 0)))
                if strike <= 0:
                    continue

                for opt_type, attr in (("CE", "call_options"), ("PE", "put_options")):
                    if isinstance(strike_data, dict):
                        opt_data = strike_data.get(attr, {})
                        inst_key = (opt_data or {}).get("instrument_key", "") if isinstance(opt_data, dict) else ""
                    else:
                        opt_data = getattr(strike_data, attr, None)
                        inst_key = getattr(opt_data, "instrument_key", "") if opt_data else ""

                    if inst_key:
                        keys[(expiry_str, strike, opt_type)] = inst_key
                        expiry_set.add(expiry_date)

            added = len(keys) - count_before
            logger.debug(
                "InstrumentRegistry [%s] expiry=%s: +%d contracts", underlying, expiry_str, added
            )

        self._upstox_keys[underlying] = keys
        self._expiries[underlying] = sorted(expiry_set)
        self._loaded.add(underlying)

        logger.info(
            "InstrumentRegistry [%s]: loaded %d contracts across %d expiries (%s).",
            underlying, len(keys), len(expiry_set),
            ", ".join(e.isoformat() for e in sorted(expiry_set)),
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
