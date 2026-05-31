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
        # {underlying: list of diagnostic strings from last load attempt}
        self._diag: Dict[str, List[str]] = {}

    # ── Loading ───────────────────────────────────────────────────────────────

    def load_sync(self, underlying: str, access_token: str = "", weeks_ahead: int = 8) -> None:
        """
        Load active option contracts via Upstox get_option_contracts API.
        Falls back to static NSE master JSON if API returns 0 contracts.
        All steps logged into self._diag[underlying] for UI diagnostics.
        """
        diag: List[str] = []
        self._diag[underlying] = diag

        try:
            import upstox_client
        except ImportError:
            diag.append("ERROR: upstox_client not installed — pip install upstox-python-sdk")
            logger.warning(diag[-1])
            return

        today = date.today()
        underlying_key = _UPSTOX_UNDERLYING_KEY.get(underlying)
        if not underlying_key:
            diag.append(f"ERROR: no Upstox underlying key mapping for '{underlying}'")
            return

        diag.append(f"underlying_key = {underlying_key}")
        diag.append(f"access_token present = {bool(access_token)} (len={len(access_token)})")

        # ── Primary: get_option_contracts API (lightweight, targeted) ─────────
        if access_token:
            cfg = upstox_client.Configuration()
            cfg.access_token = access_token
            api_client_obj = upstox_client.ApiClient(cfg)
            opt_api = upstox_client.OptionsApi(api_client_obj)

            # Calculate next N weekly expiry dates
            expiry_dates: List[date] = []
            d = today
            for _ in range(weeks_ahead):
                d = _calc_next_expiry(underlying, d)
                expiry_dates.append(d)
                d = d + timedelta(days=1)

            diag.append(f"expiries to query: {[e.isoformat() for e in expiry_dates[:4]]} ...")

            keys: Dict[Tuple[str, int, str], str] = {}
            expiry_set: Set[date] = set()

            for expiry_date in expiry_dates:
                expiry_str = expiry_date.isoformat()
                try:
                    resp = opt_api.get_option_contracts(
                        instrument_key=underlying_key,
                        expiry_date=expiry_str,
                    )
                    resp_type = type(resp).__name__
                    # SDK returns GetOptionContractResponse wrapper (not raw list)
                    # .data contains the list[InstrumentData]
                    if isinstance(resp, list):
                        items = resp
                    elif hasattr(resp, "data") and resp.data is not None:
                        items = resp.data if isinstance(resp.data, list) else list(resp.data)
                    else:
                        items = []
                    diag.append(f"  {expiry_str}: resp type={resp_type} items={len(items)}")
                except Exception as exc:
                    diag.append(f"  {expiry_str}: API EXCEPTION — {exc}")
                    logger.warning("InstrumentRegistry [%s] expiry=%s: %s", underlying, expiry_str, exc)
                    items = []

                count_before = len(keys)
                # Log first item's actual fields for debugging
                if items and len(keys) == 0 and expiry_str == expiry_dates[0].isoformat():
                    first = items[0]
                    fi_key, fi_ts, fi_strike, fi_exp = self._parse_instrument(first)
                    itype_f = first.get("instrument_type","?") if isinstance(first,dict) else getattr(first,"instrument_type","?")
                    diag.append(f"  first item: ikey={fi_key!r} ts={fi_ts!r} strike={fi_strike} expiry={fi_exp!r} instrument_type={itype_f!r}")

                for inst in items:
                    ikey, ts, strike_raw, exp_raw = self._parse_instrument(inst)
                    if not ikey:
                        continue
                    opt_type = self._detect_opt_type(ts, ikey, inst)
                    if not opt_type:
                        continue
                    strike = int(round(float(strike_raw or 0)))
                    if strike <= 0:
                        continue
                    keys[(expiry_str, strike, opt_type)] = ikey
                    expiry_set.add(expiry_date)

                added = len(keys) - count_before
                if added > 0:
                    sample = next((v for (e,s,o),v in keys.items() if e == expiry_str), "")
                    diag.append(f"    → {added} contracts parsed. Sample: {sample}")

            diag.append(f"API total: {len(keys)} contracts across {len(expiry_set)} expiries")

            if keys:
                self._upstox_keys[underlying] = keys
                self._expiries[underlying] = sorted(expiry_set)
                self._loaded.add(underlying)
                logger.info("InstrumentRegistry [%s]: %d contracts via API", underlying, len(keys))
                return

            diag.append("API returned 0 contracts — falling back to master JSON (works 24/7)")
            logger.warning("InstrumentRegistry [%s]: 0 from API, trying master JSON", underlying)
        else:
            diag.append("No access_token — skipping API, going straight to master JSON fallback")

        # ── Fallback: static NSE master JSON (works 24/7, cached per session) ──
        self._load_from_master_json(underlying, today, diag)

    def get_diagnostics(self, underlying: str) -> List[str]:
        """Return the diagnostic log from the last load_sync call for this underlying."""
        return list(self._diag.get(underlying, ["No load attempted yet."]))

    def _load_from_master_json(self, underlying: str, today: date, diag: List[str] = None) -> None:
        """Download and parse the Upstox NSE instrument master JSON (cached per session)."""
        if diag is None:
            diag = self._diag.setdefault(underlying, [])
        import gzip
        import json
        from urllib.request import urlopen, Request

        cache_key = today.isoformat()
        raw_instruments = _MASTER_CACHE.get(cache_key)

        if raw_instruments is None:
            url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
            diag.append(f"Downloading master JSON: {url}")
            logger.info("InstrumentRegistry: downloading NSE master JSON ...")
            try:
                req = Request(url, headers={"Accept-Encoding": "gzip"})
                with urlopen(req, timeout=60) as r:
                    raw = r.read()
                try:
                    raw_instruments = json.loads(gzip.decompress(raw))
                except Exception:
                    raw_instruments = json.loads(raw)
                _MASTER_CACHE[cache_key] = raw_instruments
                diag.append(f"Master JSON downloaded: {len(raw_instruments)} total instruments")
                logger.info("InstrumentRegistry: master JSON loaded — %d instruments", len(raw_instruments))
            except Exception as exc:
                diag.append(f"Master JSON DOWNLOAD FAILED: {exc}")
                logger.error("InstrumentRegistry: master JSON download failed: %s", exc)
                self._loaded.add(underlying)
                return
        else:
            diag.append(f"Master JSON served from session cache: {len(raw_instruments)} instruments")

        keys: Dict[Tuple[str, int, str], str] = {}
        expiry_set: Set[date] = set()

        # Segment prefix for this underlying (BSE for SENSEX, NSE for rest)
        seg_prefix = "BSE_FO|" if underlying == "SENSEX" else "NSE_FO|"
        diag.append(f"Filtering master JSON: ikey startswith '{seg_prefix}' AND ts startswith '{underlying}'")

        # Log first 3 overall samples + first matching NSE_FO sample
        for i, si in enumerate(raw_instruments[:3]):
            ik, ts_i, _, _ = self._parse_instrument(si)
            diag.append(f"  sample[{i}]: ikey={ik!r} ts={ts_i!r}")
        # Find first NSE_FO instrument to show actual NIFTY format
        for si in raw_instruments:
            ik, ts_i, _, _ = self._parse_instrument(si)
            if ik.startswith(seg_prefix):
                diag.append(f"  first NSE_FO sample: ikey={ik!r} ts={ts_i!r}")
                break

        for inst in raw_instruments:
            ikey, ts, strike_raw, exp_raw = self._parse_instrument(inst)

            # Filter by instrument_key prefix (reliable, works regardless of field naming)
            if not ikey or not ikey.startswith(seg_prefix):
                continue

            # Filter by trading_symbol starting with the underlying name
            # Handles both weekly (NIFTY2660224500CE) and monthly (NIFTY26JUN24500CE)
            if not ts.startswith(underlying):
                continue

            try:
                expiry_date = (
                    exp_raw.date() if isinstance(exp_raw, datetime)
                    else exp_raw if isinstance(exp_raw, date)
                    else date.fromisoformat(str(exp_raw)[:10])
                )
            except (ValueError, TypeError):
                continue

            if expiry_date < today:
                continue

            opt_type = self._detect_opt_type(ts, ikey, inst)
            if not opt_type:
                continue

            strike = int(round(float(strike_raw or 0)))
            if strike <= 0:
                continue

            keys[(expiry_date.isoformat(), strike, opt_type)] = ikey
            expiry_set.add(expiry_date)

        self._upstox_keys[underlying] = keys
        self._expiries[underlying] = sorted(expiry_set)
        self._loaded.add(underlying)
        summary = (
            f"Master JSON result: {len(keys)} contracts across expiries: "
            + ", ".join(e.isoformat() for e in sorted(expiry_set)[:6])
        )
        diag.append(summary)
        logger.info("InstrumentRegistry [%s]: %s", underlying, summary)

    @staticmethod
    def _parse_instrument(inst) -> tuple:
        """Extract (instrument_key, trading_symbol, strike_price, expiry_raw) from dict or object."""
        if isinstance(inst, dict):
            return (
                inst.get("instrument_key", ""),
                inst.get("trading_symbol", ""),
                inst.get("strike_price", 0),
                inst.get("expiry", ""),
            )
        return (
            getattr(inst, "instrument_key", ""),
            getattr(inst, "trading_symbol", ""),
            getattr(inst, "strike_price", 0),
            getattr(inst, "expiry", ""),
        )

    @staticmethod
    def _detect_opt_type(ts: str, ikey: str, inst) -> Optional[str]:
        """
        Detect CE/PE from trading_symbol or instrument_key.

        Upstox trading_symbol formats observed:
          Compact : NIFTY2660224500CE        (endswith CE/PE)
          Spaced  : NIFTY 24500 CE 02 JUN 26 (CE/PE in middle, space-delimited)

        Also checks instrument_type field directly (may be 'CE' or 'PE').
        """
        # Direct suffix (compact format)
        if ts.endswith("CE") or ikey.endswith("CE"):
            return "CE"
        if ts.endswith("PE") or ikey.endswith("PE"):
            return "PE"

        # Space-delimited format: " CE " or " PE " anywhere in trading_symbol
        ts_upper = ts.upper()
        if " CE " in ts_upper or ts_upper.endswith(" CE"):
            return "CE"
        if " PE " in ts_upper or ts_upper.endswith(" PE"):
            return "PE"

        # Fall back to instrument_type field
        itype = inst.get("instrument_type", "") if isinstance(inst, dict) else getattr(inst, "instrument_type", "")
        if str(itype).upper() in ("CE", "CALL"):
            return "CE"
        if str(itype).upper() in ("PE", "PUT"):
            return "PE"

        return None

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
            return SymbolTranslator.to_zerodha(internal, is_monthly=is_monthly_expiry(expiry, underlying))

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
