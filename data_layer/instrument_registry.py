"""
data_layer/instrument_registry.py — Centralized instrument key resolver.

Single source of truth for mapping (underlying, expiry, strike, CE/PE)
to the exact broker-specific symbol or key required by:
  • Upstox WebSocket subscription & order placement  (instrument_key)
  • Fyers  WebSocket subscription & order placement  (trading_symbol)
  • Shoonya / AngelOne / Dhan                        (derived from SymbolTranslator)
  • StrikeRebalancer (subscription tokens)
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
    "SENSEX":      4,   # Friday (BSE Sensex weekly options)
}

# Upstox underlying instrument key (used for get_option_contracts call)
_UPSTOX_UNDERLYING_KEY: Dict[str, str] = {
    "NIFTY":       "NSE_INDEX|Nifty 50",
    "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
    "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY":  "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX":      "BSE_INDEX|SENSEX",
}

# MCX commodities — loaded from the MCX master JSON (futures-driven ATM).
_MCX_UNDERLYINGS: Set[str] = {"CRUDEOIL", "CRUDEOILM", "NATURALGAS", "GOLD", "GOLDM", "SILVER"}
_MCX_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"
_MONTH_ABBR_UP = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                  "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


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
        # MCX commodities: near-month FUTURES symbols (the ATM source)
        self._futures_upstox: Dict[str, str] = {}   # underlying -> "MCX_FO|499095"
        self._futures_fyers: Dict[str, str] = {}     # underlying -> "MCX:CRUDEOIL26JUNFUT"
        self._futures_expiry: Dict[str, date] = {}

    # ── Loading ───────────────────────────────────────────────────────────────

    def load_sync(self, underlying: str, access_token: str = "", weeks_ahead: int = 8) -> None:
        """
        Load active option contracts via Upstox get_option_contracts API.
        Falls back to static NSE master JSON if API returns 0 contracts.
        All steps logged into self._diag[underlying] for UI diagnostics.
        """
        diag: List[str] = []
        self._diag[underlying] = diag

        today = date.today()

        # MCX commodities load from the MCX master JSON (no Upstox SDK needed).
        if underlying.upper() in _MCX_UNDERLYINGS:
            self._load_mcx(underlying.upper(), today, diag)
            return

        # BSE indices (SENSEX, BANKEX) — load from BSE master JSON directly.
        # The API's weekday-based expiry math is unreliable for BSE weekly options,
        # so we use the actual exchange master register instead of calendar days.
        if underlying.upper() in ("SENSEX", "BANKEX"):
            self._load_from_master_json(underlying, today, diag)
            return

        underlying_key = _UPSTOX_UNDERLYING_KEY.get(underlying)
        if not underlying_key:
            # 2026-08-09 fix: individual FnO stocks (RELIANCE, HDFCBANK, ...) were
            # never in _UPSTOX_UNDERLYING_KEY (that dict only has indices/MCX), so
            # this used to just log an error and return -- self._expiries[underlying]
            # was NEVER populated for any FnO stock, meaning get_active_expiry()
            # always returned None and _get_expiry() (strategies/d1_trap_option/
            # book.py, used by BOTH d1_trap_fno and the new d1_trap_fno_sr) could
            # never resolve an expiry -- every entry attempt silently failed with
            # "no active expiry -- cannot enter", for the WATCHLIST mechanism too,
            # not just new code. _load_from_master_json already works correctly for
            # arbitrary NSE F&O underlyings (confirmed: RELIANCE resolves real
            # 2026-08-25/09-29/10-27 monthly expiries) -- it just was never called
            # here. Route non-index underlyings through it instead of erroring out.
            diag.append(f"'{underlying}' not in _UPSTOX_UNDERLYING_KEY (index/MCX only) "
                        f"-- falling back to master JSON for FnO stock expiry/contract data")
            self._load_from_master_json(underlying, today, diag)
            return

        try:
            import upstox_client
        except ImportError:
            diag.append("ERROR: upstox_client not installed — pip install upstox-python-sdk")
            logger.warning(diag[-1])
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
                # The options API doesn't expose futures — resolve the near-month
                # futures key separately from the (cached) master JSON.
                self._resolve_futures_key(underlying, today, diag)
                return

            diag.append("API returned 0 contracts — falling back to master JSON (works 24/7)")
            logger.warning("InstrumentRegistry [%s]: 0 from API, trying master JSON", underlying)
        else:
            diag.append("No access_token — skipping API, going straight to master JSON fallback")

        # ── Fallback: static NSE master JSON (works 24/7, cached per session) ──
        self._load_from_master_json(underlying, today, diag)

    def _resolve_futures_key(self, underlying: str, today: date, diag: List[str]) -> None:
        """Populate self._futures_upstox[underlying] (AND, 2026-08-26,
        self._futures_fyers[underlying]) with the near-month index futures
        contract, read from the (session-cached) exchange master JSON. Index
        underlyings only load OPTIONS via the fast options API, which has no
        futures endpoint — this is a cheap supplementary lookup against the
        same master JSON _load_from_master_json already knows how to
        download/cache, without redoing the (already-succeeded) option parse.

        2026-08-26 fix (real incident, confirmed live): this used to populate
        ONLY self._futures_upstox, never self._futures_fyers, for ANY index
        underlying (_load_mcx, the commodity sibling of this method, derives
        both). get_futures_fyers() always returned "" for NIFTY as a result --
        not a timing race that would self-correct, a PERMANENT gap. Confirmed
        live: UpstoxFeeder correctly got the futures tick (NSE_FO|... key,
        ltp diverging ~177pts from real spot as expected for cost-of-carry),
        while FyersFeeder fell back to real spot every single connect, forever
        -- a standing ~177pt mismatch between the primary and standby feed
        that would have made self._spot jump instantly on any Fyers failover.
        Now derives the Fyers symbol the same way _load_mcx already does for
        commodities (yy + 3-letter month + "FUT"), with the correct NSE/BSE
        exchange prefix."""
        if underlying.upper() in _MCX_UNDERLYINGS or underlying in self._futures_upstox:
            return
        import gzip, json
        from urllib.request import urlopen, Request

        _is_bse = underlying in ("SENSEX", "BANKEX")
        _exch = "BSE" if _is_bse else "NSE"
        cache_key = f"{_exch}:{today.isoformat()}"
        raw_instruments = _MASTER_CACHE.get(cache_key)
        if raw_instruments is None:
            url = f"https://assets.upstox.com/market-quote/instruments/exchange/{_exch}.json.gz"
            try:
                import ssl as _ssl
                _ctx = _ssl.create_default_context()
                _ctx.check_hostname = False
                _ctx.verify_mode = _ssl.CERT_NONE
                req = Request(url, headers={"Accept-Encoding": "gzip"})
                with urlopen(req, timeout=60, context=_ctx) as r:
                    raw = r.read()
                try:
                    raw_instruments = json.loads(gzip.decompress(raw))
                except Exception:
                    raw_instruments = json.loads(raw)
                _MASTER_CACHE[cache_key] = raw_instruments
            except Exception as exc:
                diag.append(f"futures key lookup: master JSON download failed: {exc}")
                return

        seg_prefix = "BSE_FO|" if _is_bse else "NSE_FO|"
        fut_candidates: List[Tuple[date, str]] = []
        for inst in raw_instruments:
            ikey, ts, _strike, exp_raw = self._parse_instrument(inst)
            if not ikey or not ikey.startswith(seg_prefix):
                continue
            _uns = ((inst.get("underlying_symbol") or inst.get("name") or "")
                    if isinstance(inst, dict)
                    else (getattr(inst, "underlying_symbol", "") or getattr(inst, "name", "")))
            if _uns:
                if str(_uns).upper() != underlying.upper():
                    continue
            elif not ts.startswith(underlying):
                continue
            _itype = (inst.get("instrument_type", "") if isinstance(inst, dict)
                      else getattr(inst, "instrument_type", ""))
            if not (str(_itype).upper().startswith("FUT") or ts.upper().endswith("FUT")):
                continue
            try:
                if isinstance(exp_raw, datetime):
                    expiry_date = exp_raw.date()
                elif isinstance(exp_raw, date):
                    expiry_date = exp_raw
                elif isinstance(exp_raw, (int, float)) or (isinstance(exp_raw, str) and str(exp_raw).strip().isdigit()):
                    _epoch = int(exp_raw)
                    if _epoch > 10_000_000_000:
                        _epoch //= 1000
                    from config.global_config import IST as _IST
                    expiry_date = datetime.fromtimestamp(_epoch, _IST).date()
                else:
                    expiry_date = date.fromisoformat(str(exp_raw)[:10])
            except (ValueError, TypeError, OSError, OverflowError):
                continue
            if expiry_date < today:
                continue
            fut_candidates.append((expiry_date, ikey))

        if fut_candidates:
            fut_candidates.sort(key=lambda x: x[0])
            f_exp, f_ikey = fut_candidates[0]
            self._futures_upstox[underlying] = f_ikey
            self._futures_expiry[underlying] = f_exp
            # 2026-08-26 fix: derive the Fyers symbol too (same yy+mon3+"FUT"
            # convention _load_mcx already uses for commodities) -- previously
            # never set for index underlyings at all, see this method's own
            # updated docstring for the real incident this caused.
            _yy = f_exp.strftime("%y")
            _mon3 = _MONTH_ABBR_UP[f_exp.month - 1]
            self._futures_fyers[underlying] = f"{_exch}:{underlying}{_yy}{_mon3}FUT"
            diag.append(
                f"futures (near-month): upstox={f_ikey} fyers={self._futures_fyers[underlying]} "
                f"expiry={f_exp}"
            )
        else:
            diag.append("futures key lookup: no FUT instrument matched in master JSON")

    def _load_mcx(self, underlying: str, today: date, diag: List[str]) -> None:
        """
        Load an MCX commodity (e.g. CRUDEOIL) option chain + near-month futures
        from the Upstox MCX master JSON. Stores option instrument_keys, expiries,
        and the near FUTURES key (Upstox) + derived Fyers futures symbol (the ATM
        source). trading_symbol format is spaced, e.g.:
          option : 'CRUDEOIL 8500 CE 16 JUN 26'   ikey 'MCX_FO|565901'
          futures: 'CRUDEOIL FUT 18 JUN 26'        ikey 'MCX_FO|499095'
        """
        import gzip, json
        from urllib.request import urlopen, Request

        cache_key = "MCX:" + today.isoformat()
        data = _MASTER_CACHE.get(cache_key)
        if data is None:
            try:
                import ssl as _ssl
                ctx = _ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = _ssl.CERT_NONE
                req = Request(_MCX_MASTER_URL, headers={"Accept-Encoding": "gzip"})
                with urlopen(req, timeout=60, context=ctx) as r:
                    raw = r.read()
                try:
                    data = json.loads(gzip.decompress(raw))
                except Exception:
                    data = json.loads(raw)
                _MASTER_CACHE[cache_key] = data
                diag.append(f"MCX master loaded: {len(data)} instruments")
            except Exception as exc:
                diag.append(f"MCX master download failed: {exc}")
                logger.error("InstrumentRegistry MCX[%s]: download failed: %s", underlying, exc)
                self._loaded.add(underlying)
                return

        def _parse_ts_date(parts: List[str]):
            # parts end with [DD, MON, YY]; returns a date or None
            try:
                dd = int(parts[-3]); mon = _MONTH_ABBR_UP.index(parts[-2].upper()) + 1
                yy = 2000 + int(parts[-1])
                return date(yy, mon, dd)
            except Exception:
                return None

        keys: Dict[Tuple[str, int, str], str] = {}
        expiry_set: Set[date] = set()
        fut_candidates: List[Tuple[date, str]] = []   # (expiry, ikey)

        for inst in data:
            ikey, ts, strike_raw, _exp = self._parse_instrument(inst)
            if not ikey or not ikey.startswith("MCX_FO|") or not ts:
                continue
            parts = ts.split()
            if not parts or parts[0].upper() != underlying:   # EXACT underlying (CRUDEOIL != CRUDEOILM)
                continue
            # Futures: 'CRUDEOIL FUT 18 JUN 26'
            if len(parts) >= 2 and parts[1].upper() == "FUT":
                exp = _parse_ts_date(parts)
                if exp and exp >= today:
                    fut_candidates.append((exp, ikey))
                continue
            # Options: 'CRUDEOIL 8500 CE 16 JUN 26'
            opt_type = "CE" if " CE " in f" {ts} " else ("PE" if " PE " in f" {ts} " else None)
            if not opt_type:
                continue
            exp = _parse_ts_date(parts)
            if not exp or exp < today:
                continue
            try:
                strike = int(round(float(strike_raw or 0)))
                if strike <= 0:
                    # fall back to the numeric token in ts (parts[1])
                    strike = int(round(float(parts[1])))
            except (ValueError, TypeError):
                continue
            if strike <= 0:
                continue
            keys[(exp.isoformat(), strike, opt_type)] = ikey
            expiry_set.add(exp)

        self._upstox_keys[underlying] = keys
        self._expiries[underlying] = sorted(expiry_set)
        self._loaded.add(underlying)

        # Near-month futures = nearest expiry on/after today → ATM source.
        if fut_candidates:
            fut_candidates.sort(key=lambda x: x[0])
            f_exp, f_ikey = fut_candidates[0]
            self._futures_upstox[underlying] = f_ikey
            self._futures_expiry[underlying] = f_exp
            yy = f_exp.strftime("%y"); mon = _MONTH_ABBR_UP[f_exp.month - 1]
            self._futures_fyers[underlying] = f"MCX:{underlying}{yy}{mon}FUT"
            diag.append(f"futures: upstox={f_ikey} fyers={self._futures_fyers[underlying]} expiry={f_exp}")

        diag.append(f"MCX[{underlying}] result: {len(keys)} options across "
                    f"{len(expiry_set)} expiries: {', '.join(e.isoformat() for e in sorted(expiry_set)[:4])}")
        logger.info("InstrumentRegistry MCX[%s]: %d options, futures=%s",
                    underlying, len(keys), self._futures_fyers.get(underlying, "?"))

    # ── MCX futures accessors (ATM source for commodities) ─────────────────────

    def get_futures_fyers(self, underlying: str) -> str:
        return self._futures_fyers.get(underlying.upper(), "")

    def get_futures_upstox(self, underlying: str) -> str:
        return self._futures_upstox.get(underlying.upper(), "")

    def load_futures_only_sync(self, underlying: str, today: date = None) -> None:
        """Resolve just the near-month futures instrument_key for an arbitrary
        NSE F&O underlying via the master JSON. load_sync's options-API path
        only works for underlyings in _UPSTOX_UNDERLYING_KEY (indices/MCX) and
        returns early for anything else -- this bypasses that restriction for
        callers that only need the futures key (e.g. OI-buildup checks on
        individual F&O stocks), not the full option-contract map. Cheap to call
        repeatedly: _load_from_master_json's download is cached per (exchange,
        day) in _MASTER_CACHE."""
        underlying = underlying.upper()
        if underlying in self._futures_upstox:
            return
        self._load_from_master_json(underlying, today or date.today())

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

        # SENSEX (and other BSE indices) options live in Upstox's BSE master, NOT the
        # NSE master — using NSE here is why SENSEX loaded 0 BSE_FO contracts.
        _is_bse = underlying in ("SENSEX", "BANKEX")
        _exch = "BSE" if _is_bse else "NSE"
        cache_key = f"{_exch}:{today.isoformat()}"
        raw_instruments = _MASTER_CACHE.get(cache_key)

        if raw_instruments is None:
            url = f"https://assets.upstox.com/market-quote/instruments/exchange/{_exch}.json.gz"
            diag.append(f"Downloading master JSON: {url}")
            logger.info("InstrumentRegistry: downloading %s master JSON ...", _exch)
            try:
                import ssl as _ssl
                _ctx = _ssl.create_default_context()
                _ctx.check_hostname = False
                _ctx.verify_mode = _ssl.CERT_NONE
                req = Request(url, headers={"Accept-Encoding": "gzip"})
                with urlopen(req, timeout=60, context=_ctx) as r:
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
        fut_candidates: List[Tuple[date, str]] = []   # (expiry, ikey) — near-month futures

        # Segment prefix for this underlying (BSE for SENSEX/BANKEX, NSE for rest)
        seg_prefix = "BSE_FO|" if underlying in ("SENSEX", "BANKEX") else "NSE_FO|"
        diag.append(f"Filtering master JSON: ikey startswith '{seg_prefix}' AND ts startswith '{underlying}'")

        # Log first 3 overall samples + first matching {seg_prefix} sample
        for i, si in enumerate(raw_instruments[:3]):
            ik, ts_i, _, _ = self._parse_instrument(si)
            diag.append(f"  sample[{i}]: ikey={ik!r} ts={ts_i!r}")
        # Find first {seg_prefix} instrument to show actual format
        for si in raw_instruments:
            ik, ts_i, _, _ = self._parse_instrument(si)
            if ik.startswith(seg_prefix):
                diag.append(f"  first {seg_prefix} sample: ikey={ik!r} ts={ts_i!r}")
                break

        for inst in raw_instruments:
            ikey, ts, strike_raw, exp_raw = self._parse_instrument(inst)

            # Filter by instrument_key prefix (reliable, works regardless of field naming)
            if not ikey or not ikey.startswith(seg_prefix):
                continue

            # Exact underlying match — prefer the underlying_symbol/name field so
            # 'SENSEX' does NOT also match 'SENSEX50'. Fall back to ts prefix.
            _uns = ((inst.get("underlying_symbol") or inst.get("name") or "")
                    if isinstance(inst, dict)
                    else (getattr(inst, "underlying_symbol", "") or getattr(inst, "name", "")))
            if _uns:
                if str(_uns).upper() != underlying.upper():
                    continue
            elif not ts.startswith(underlying):
                continue

            try:
                if isinstance(exp_raw, datetime):
                    expiry_date = exp_raw.date()
                elif isinstance(exp_raw, date):
                    expiry_date = exp_raw
                elif isinstance(exp_raw, (int, float)) or (isinstance(exp_raw, str) and str(exp_raw).strip().isdigit()):
                    # epoch (BSE master uses milliseconds, e.g. 1781807399000)
                    _epoch = int(exp_raw)
                    if _epoch > 10_000_000_000:   # milliseconds → seconds
                        _epoch //= 1000
                    from config.global_config import IST as _IST
                    expiry_date = datetime.fromtimestamp(_epoch, _IST).date()
                else:
                    expiry_date = date.fromisoformat(str(exp_raw)[:10])
            except (ValueError, TypeError, OSError, OverflowError):
                continue

            if expiry_date < today:
                continue

            # Index futures: instrument_type FUT(IDX) or trading_symbol ending "FUT",
            # no strike. Track the near-month contract as the futures ATM/historical source.
            _itype = (inst.get("instrument_type", "") if isinstance(inst, dict)
                      else getattr(inst, "instrument_type", ""))
            if str(_itype).upper().startswith("FUT") or ts.upper().endswith("FUT"):
                fut_candidates.append((expiry_date, ikey))
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

        if fut_candidates:
            fut_candidates.sort(key=lambda x: x[0])
            f_exp, f_ikey = fut_candidates[0]
            self._futures_upstox[underlying] = f_ikey
            self._futures_expiry[underlying] = f_exp
            diag.append(f"futures (near-month): upstox={f_ikey} expiry={f_exp}")

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
        Returns None if registry not yet loaded — never calculates from weekday math.
        """
        from_date = from_date or date.today()
        expiries = self._expiries.get(underlying, [])
        for exp in expiries:
            if exp >= from_date:
                return exp
        return None

    def get_active_expiry_strict(
        self, underlying: str, from_date: date, max_days_out: int = 35,
    ) -> Optional[date]:
        """Like get_active_expiry, but returns None instead of silently substituting
        a far-month contract when the TRUE front-month contract for a historical
        from_date has already expired and been delisted from the currently-loaded
        master (get_active_expiry has no record of it, so it returns the next
        one it still has -- which can be a whole cycle away).

        2026-08-09: this exact bug silently fed pre-rollover BANKNIFTY backtest
        dates (in a window that crossed the July->August monthly expiry) real
        historical prices for the AUGUST contract instead of the JULY one that
        was actually front-month on those dates -- valid-looking data, wrong
        contract. Monthly cycles run ~28-31 days apart, so a resolved expiry
        more than max_days_out days past from_date is a strong signal the real
        contract for that date is gone, not that this IS the real contract.
        Any backtest walking multiple historical days for a monthly-expiry
        underlying should use this, not get_active_expiry, and skip the day
        entirely on None rather than falling back to the loose version."""
        exp = self.get_active_expiry(underlying, from_date)
        if exp is None:
            return None
        if (exp - from_date).days > max_days_out:
            return None
        return exp

    def all_expiries(self, underlying: str) -> List[date]:
        """Return all loaded active expiry dates for an underlying."""
        return list(self._expiries.get(underlying, []))

    def get_available_strikes(self, underlying: str, expiry: date, opt_type: str = "") -> List[int]:
        """2026-08-27, real incident: GVT&D PE entry failed with "no upstox_key
        resolved for GVT&D PE4350" -- stock_resolve.py's price-band strike-step
        heuristic (no real chain grid available to it) assumed a flat 50pt grid
        for anything under Rs5000, but GVT&D's REAL listed grid switches to
        100pt around that price level (4300/4400 are real, 4350 was never
        listed at all). Confirmed against the real Upstox master JSON directly
        (scripts/check_stock_expiries_raw.py): underlying_symbol matched
        perfectly, real contracts existed at 2026-09-29/10-27/11-23 -- the
        heuristic's guessed strike was simply never a real one.

        This exposes the ACTUAL listed strikes already loaded in
        self._upstox_keys (keyed (expiry_iso, strike, opt_type)) so a caller
        can snap to the nearest REAL strike instead of guessing a step from a
        price band. Returns [] if this underlying/expiry has no loaded
        contracts (caller must fall back to the old heuristic, never crash)."""
        keys = self._upstox_keys.get(underlying.upper(), {})
        exp_iso = expiry.isoformat()
        strikes = {
            k[1] for k in keys
            if k[0] == exp_iso and (not opt_type or k[2] == opt_type.upper())
        }
        return sorted(strikes)

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

    def historical_instrument_key(self, underlying: str) -> str:
        """Upstox instrument_key to use for the underlying's HISTORICAL candles.
        MCX commodities → the loaded near-month FUTURES key (the ATM source);
        index underlyings → the static index key. Empty if not resolvable."""
        u = underlying.upper()
        if u in self._futures_upstox:
            return self._futures_upstox[u]
        return _UPSTOX_UNDERLYING_KEY.get(u, "")

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

        Upstox  → instrument_key from registry (required for API)
        Fyers   → NSE:NIFTY2660224500CE (derived via SymbolTranslator)
        Zerodha → NIFTY2662524500CE     (derived via SymbolTranslator)
        AngelOne → NIFTY02JUN2624500CE  (derived via SymbolTranslator)
        Dhan    → internal canonical str (token lookup done by broker)
        """
        from data_layer.symbol_translator import InternalSymbol, SymbolTranslator

        internal = InternalSymbol(
            underlying=underlying,
            strike=float(strike),
            option_type=opt_type,
            expiry=expiry,
        )

        p = provider.lower()

        # ── MCX commodities (CRUDEOIL etc.) — monthly format, exchange MCX ──────
        if underlying.upper() in _MCX_UNDERLYINGS:
            yy = expiry.strftime("%y")
            mon = _MONTH_ABBR_UP[expiry.month - 1]
            core = f"{underlying.upper()}{yy}{mon}{int(strike)}{opt_type}"  # CRUDEOIL26JUN8500CE
            if p == "fyers":
                return f"MCX:{core}"
            if p == "zerodha":
                return core
            if p == "upstox":
                return self.get_upstox_key(underlying.upper(), expiry, int(strike), opt_type)
            return core

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
    True if this expiry is the monthly (last expiry of the month).

    Uses the registry's sorted expiry list: if the NEXT expiry after this one
    falls in a different calendar month, this expiry is the monthly one.
    Falls back to the +7-day heuristic only if the registry has no data.
    """
    all_exp = REGISTRY.all_expiries(underlying)
    if all_exp:
        try:
            idx = all_exp.index(expiry)
            next_exp = all_exp[idx + 1] if idx + 1 < len(all_exp) else None
            if next_exp is not None:
                return next_exp.month != expiry.month
            # expiry is the last known — treat as monthly
            return True
        except ValueError:
            pass  # expiry not in list — fall through
    # Registry not loaded: compare months via +7-day offset (no weekday math)
    return (expiry + timedelta(days=7)).month != expiry.month


def next_expiry(underlying: str, from_date: date = None) -> Optional[date]:
    """
    Public helper — always from REGISTRY (real Upstox contract dates).
    Returns None if registry is not yet loaded. Never falls back to weekday math.
    Callers must load the registry before calling this.
    """
    from_date = from_date or date.today()
    return REGISTRY.get_active_expiry(underlying, from_date)
