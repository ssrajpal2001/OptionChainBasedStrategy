"""
strategies/trap_scanner/data.py — Upstox REST helpers and instrument-key builders.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Intraday warm cache: (provider_symbol, date) -> (bars, monotonic_ts)
# Shared across all trap-scanner engine instances to honor
# "historical REST API called only once per CE1/CE2/PE1/PE2".
_INTRADAY_CACHE: Dict[Tuple[str, date], Tuple[List[dict], float]] = {}
_INTRADAY_CACHE_TTL_SECONDS = 300.0

from strategies.trap_scanner.config import _SPOT_KEYS

logger = logging.getLogger(__name__)


class DataMixin:
    """Data fetching, instrument-key construction and subscription helpers."""

    # ── Token / broker helpers ────────────────────────────────────────────────

    def _get_upstox_token(self) -> Optional[str]:
        # All underlyings including MCX CrudeOil use the primary upstox account.
        # Upstox WebSocket handles both NSE/BSE and MCX on the same connection.
        creds = self._db.get_feeder_creds_sync("upstox")
        return (creds or {}).get("access_token") or ""

    def _get_fyers_token(self) -> Tuple[Optional[str], Optional[str]]:
        """Return (client_id, access_token) from feeder creds, or (None, None)."""
        try:
            creds = self._db.get_feeder_creds_sync("fyers") or {}
            return creds.get("api_key"), creds.get("access_token")
        except Exception as exc:
            self._log.debug("_get_fyers_token: %s", exc)
            return None, None

    async def _ensure_broker(self):
        if self._broker and self._broker.is_authenticated:
            return self._broker
        try:
            bindings = self._db.get_bindings_sync(self._cid)
            row = next((b for b in bindings if b.get("binding_id") == self._bid), None)
            if not row:
                return None
            from config.client_profiles import BrokerBinding
            b = BrokerBinding(**{k: v for k, v in row.items()
                                 if k in BrokerBinding.__dataclass_fields__})
            from execution_bridge.base_broker import create_broker
            broker = create_broker(b, self._cid)
            if not await broker.authenticate():
                return None
            self._broker = broker
            return broker
        except Exception as exc:
            self._log.error("_ensure_broker: %s", exc)
            return None

    # ── Historical / intraday data ────────────────────────────────────────────

    async def _fetch_prev_day_ohlc(self) -> Optional[Dict]:
        try:
            token = self._get_upstox_token()
            if not token:
                self._log.warning("_fetch_prev_day_ohlc: no Upstox token")
                return None
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key and self._htf_source == "futures":
                # Futures-mode (CrudeOil, BTC, ETH): use REGISTRY futures key for daily OHLC
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    spot_key = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    pass
            if not spot_key:
                self._log.warning("_fetch_prev_day_ohlc: no spot key for %s", self._und)
                return None
            import aiohttp
            from urllib.parse import quote as _quote
            today   = date.today()
            fr_date = today - timedelta(days=10)
            encoded_key = _quote(spot_key, safe="")
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{encoded_key}/day/{today}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        body = await r.text()
                        self._log.warning(
                            "_fetch_prev_day_ohlc: HTTP %d for %s — %s",
                            r.status, spot_key, body[:200],
                        )
                        return None
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            if not candles:
                self._log.warning(
                    "_fetch_prev_day_ohlc: no candles returned for %s", spot_key,
                )
                return None
            # Upstox returns newest-first. During market hours today's daily candle
            # is NOT included → candles[0] = yesterday. After market close candles[0]
            # = today (partial/closed) → candles[1] = yesterday.
            # Detect by comparing candle date to today.
            first_date = str(candles[0][0])[:10]
            if first_date == str(today):
                if len(candles) < 2:
                    self._log.warning("_fetch_prev_day_ohlc: only today's candle for %s", spot_key)
                    return None
                prev = candles[1]
            else:
                prev = candles[0]
            self._log.info("_fetch_prev_day_ohlc(%s): prev=%s H=%.2f L=%.2f C=%.2f",
                           spot_key, str(prev[0])[:10], float(prev[2]), float(prev[3]), float(prev[4]))
            return {"open": float(prev[1]), "high": float(prev[2]),
                    "low":  float(prev[3]), "close": float(prev[4])}
        except Exception as exc:
            self._log.warning("_fetch_prev_day_ohlc: %s", exc)
            return None

    async def _fetch_today_open(self) -> float:
        """Return today's OPENING price (first bar of the session), NOT the current live price."""
        try:
            token = self._get_upstox_token()
            if not token:
                return 0.0
            spot_key = _SPOT_KEYS.get(self._und)
            if not spot_key and self._htf_source == "futures":
                # Futures-mode (CrudeOil, BTC, ETH): use REGISTRY futures key
                try:
                    from data_layer.instrument_registry import REGISTRY as _REG
                    spot_key = _REG.historical_instrument_key(self._und) if _REG.is_loaded(self._und) else ""
                except Exception:
                    pass
            if not spot_key:
                return 0.0
            import aiohttp
            from urllib.parse import quote
            encoded_key = quote(spot_key, safe="")
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{encoded_key}/1minute"
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        return 0.0
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            if not candles:
                return 0.0
            # Upstox returns candles newest-first; candles[-1] = oldest = first bar of the day
            today_open = float(candles[-1][1])
            self._log.info("_fetch_today_open(%s): first_bar_open=%.2f (from %d candles)",
                           spot_key, today_open, len(candles))
            return today_open
        except Exception as exc:
            self._log.warning("_fetch_today_open: %s", exc)
            return 0.0

    async def _fetch_prev_close_and_today_open_from_1m(self, fut_key: str) -> Tuple[float, float]:
        """
        For futures-mode (CrudeOil/BTC/ETH): return (prev_close, today_open).

        prev_close: historical 1m endpoint (ends yesterday) — same active contract,
          avoids daily-candle API returning an EXPIRED contract's close (e.g. June
          close when July contract is active → false 5.9% gap).
        today_open: intraday endpoint via _fetch_today_open() — MCX historical
          endpoint excludes today's session, intraday endpoint has today's bars.

        Returns (prev_close, today_open) — (0.0, 0.0) on failure.
        """
        try:
            # Historical bars → last bar of yesterday = prev_close (same active contract)
            # MCX historical endpoint excludes today's session — that's expected and correct
            bars = await self._fetch_1m_history(fut_key)
            if not bars:
                return 0.0, 0.0
            today_str  = date.today().isoformat()
            prev_bars  = [b for b in bars if b["datetime"][:10] < today_str]
            prev_close = float(prev_bars[-1]["close"]) if prev_bars else 0.0

            # Intraday endpoint → first bar of today's session = today_open
            # _fetch_today_open() uses REGISTRY for CrudeOil/BTC/ETH (no _SPOT_KEYS entry)
            # and returns candles[-1][1] = oldest (9:00 AM) bar's open = true market open
            today_open = await self._fetch_today_open()

            return prev_close, today_open
        except Exception as exc:
            self._log.warning("_fetch_prev_close_and_today_open_from_1m: %s", exc)
            return 0.0, 0.0

    async def _fetch_intraday_bars(self, instrument_key: str) -> List[dict]:
        """Fetch today's intraday 1-min bars from Upstox intraday endpoint."""
        if not instrument_key:
            return []
        try:
            token = self._get_upstox_token()
            if not token:
                return []
            import aiohttp
            url = (f"https://api.upstox.com/v2/historical-candle/intraday/"
                   f"{instrument_key}/1minute")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            bars = [
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
                for c in reversed(candles)
            ]
            self._log.info("_fetch_intraday_bars(%s): %d bars today", instrument_key, len(bars))
            return bars
        except Exception as exc:
            self._log.warning("_fetch_intraday_bars(%s) error: %s", instrument_key, exc)
            return []

    def _build_fyers_symbol(self, strike: int, opt_type: str) -> str:
        """Build Fyers symbol for an option strike using current expiry."""
        try:
            from data_layer.symbol_translator import InternalSymbol, SymbolTranslator
            from data_layer.instrument_registry import REGISTRY
            sym = InternalSymbol(
                underlying=self._und,
                strike=float(strike),
                option_type=opt_type,
                expiry=self._expiry_date or date.today(),
            )
            # Monthly detection: expiry is the last expiry of its month in REGISTRY.
            is_monthly = False
            if self._expiry_date and REGISTRY.is_loaded(self._und):
                all_exp = REGISTRY.all_expiries(self._und)
                month_exps = [e for e in all_exp if e.month == self._expiry_date.month]
                is_monthly = self._expiry_date == max(month_exps) if month_exps else False
            return SymbolTranslator.to_fyers(sym, is_monthly=is_monthly)
        except Exception as exc:
            self._log.warning("_build_fyers_symbol %s %s%s: %s", self._und, strike, opt_type, exc)
            return ""

    def _build_fyers_spot_symbol(self) -> str:
        """Build Fyers spot/index symbol (e.g. NSE:NIFTY50-INDEX)."""
        exchange = "BSE" if self._und == "SENSEX" else "NSE"
        name = {
            "NIFTY": "NIFTY50",
            "BANKNIFTY": "NIFTYBANK",
            "FINNIFTY": "FINNIFTY",
            "MIDCPNIFTY": "MIDCPNIFTY",
            "SENSEX": "SENSEX",
        }.get(self._und, self._und)
        return f"{exchange}:{name}-INDEX"

    async def _fetch_fyers_intraday_bars(self, symbol: str) -> List[dict]:
        """Fetch today's intraday 1-min bars from Fyers data/history endpoint."""
        if not symbol:
            return []
        client_id, token = self._get_fyers_token()
        if not client_id or not token:
            return []
        try:
            from data_layer.historical_candles import fetch_fyers_intraday_1m
            bars = await fetch_fyers_intraday_1m(symbol, client_id, token)
            self._log.info("_fetch_fyers_intraday_bars(%s): %d bars today", symbol, len(bars))
            return [{"datetime": b["ts"], "open": b["open"], "high": b["high"],
                     "low": b["low"], "close": b["close"], "volume": b["volume"]} for b in bars]
        except Exception as exc:
            self._log.warning("_fetch_fyers_intraday_bars(%s) error: %s", symbol, exc)
            return []

    async def _fetch_intraday_bars_with_fallback(
        self, upstox_key: str, strike: Optional[int] = None, opt_type: Optional[str] = None
    ) -> List[dict]:
        """Fetch today's 1-min bars: Upstox primary, Fyers fallback, with TTL cache.

        Args:
            upstox_key: Upstox instrument key for the instrument.
            strike: Option strike (required for Fyers fallback on options).
            opt_type: "CE" or "PE" (required for Fyers fallback on options).
        """
        if not upstox_key:
            return []
        cache_key = (upstox_key, date.today())
        cached, cached_at = _INTRADAY_CACHE.get(cache_key, (None, 0.0))
        if cached is not None and (time.monotonic() - cached_at) < _INTRADAY_CACHE_TTL_SECONDS:
            self._log.debug("_fetch_intraday_bars_with_fallback cache hit: %s", upstox_key)
            return cached

        # Primary: Upstox
        bars = await self._fetch_intraday_bars(upstox_key)
        source = "upstox"

        # Fallback: Fyers (only for option strikes with known strike/type)
        if not bars and strike and opt_type:
            fyers_symbol = self._build_fyers_symbol(int(strike), opt_type)
            if fyers_symbol:
                bars = await self._fetch_fyers_intraday_bars(fyers_symbol)
                if bars:
                    source = "fyers"

        self._log.info(
            "_fetch_intraday_bars_with_fallback(%s): %d bars from %s",
            upstox_key, len(bars), source,
        )
        if bars:
            _INTRADAY_CACHE[cache_key] = (bars, time.monotonic())
        return bars

    @staticmethod
    def _merge_bars(existing: List[dict], new_bars: List[dict]) -> List[dict]:
        """Merge two oldest-first bar lists, deduping by datetime."""
        if not new_bars:
            return existing
        if not existing:
            return list(new_bars)
        seen = {b["datetime"] for b in existing}
        merged = existing + [b for b in new_bars if b["datetime"] not in seen]
        merged.sort(key=lambda b: b["datetime"])
        return merged

    async def _fetch_1m_history(self, instrument_key: str) -> List[dict]:
        if not instrument_key:
            return []
        try:
            token = self._get_upstox_token()
            if not token:
                return []
            import aiohttp
            today   = date.today()
            to_date = today + timedelta(days=1)   # include today (Upstox excludes to_date)
            fr_date = today - timedelta(days=14)   # full prev week + current week for HTF pattern seed
            url = (f"https://api.upstox.com/v2/historical-candle/"
                   f"{instrument_key}/1minute/{to_date}/{fr_date}")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        body = await r.text()
                        self._log.warning(
                            "_fetch_1m_history(%s): HTTP %d — %s", instrument_key, r.status, body[:200]
                        )
                        return []
                    data = await r.json()
            candles = data.get("data", {}).get("candles", [])
            bars = [
                {"datetime": c[0], "open": float(c[1]), "high": float(c[2]),
                 "low": float(c[3]), "close": float(c[4]), "volume": int(c[5])}
                for c in reversed(candles)  # oldest first
            ]
            self._log.info("_fetch_1m_history(%s): %d bars (%s → %s)",
                           instrument_key, len(bars),
                           bars[0]["datetime"][:10] if bars else "—",
                           bars[-1]["datetime"][:10] if bars else "—")
            return bars
        except Exception as exc:
            self._log.warning("_fetch_1m_history(%s): %s", instrument_key, exc)
            return []

    async def _pick_liquid_strike(
        self,
        primary_strike: int, primary_key: str,
        atm_strike: int,    atm_key: str,
        opt_type: str, max_spread_pct: float
    ) -> Tuple[int, str]:
        """
        Check bid-ask spread on primary (scan) strike; fall back to ATM if too wide.
        Returns (strike, upstox_key) for the chosen exec strike.

        For futures-mode (CrudeOil): primary = scan strike (S1 CE / R1 PE).
        For option-mode (Sensex/Nifty): primary = 1-ITM option.
        """
        async def _spread_pct(key: str) -> float:
            try:
                import aiohttp
                token = self._get_upstox_token()
                if not token:
                    return 0.0
                url = (f"https://api.upstox.com/v2/market-quote/quotes"
                       f"?instrument_key={key.replace('|', '%7C')}")
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url, headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=3)) as r:
                        if r.status != 200:
                            return 0.0
                        data = await r.json()
                quotes = data.get("data", {})
                quote = list(quotes.values())[0] if quotes else {}
                depth = quote.get("depth", {})
                bid = (depth.get("buy") or [{}])[0].get("price", 0)
                ask = (depth.get("sell") or [{}])[0].get("price", 0)
                if not bid or not ask:
                    return 0.0
                mid = (bid + ask) / 2
                return round((ask - bid) / mid * 100, 2) if mid > 0 else 0.0
            except Exception as exc:
                self._log.warning("spread check %s: %s", key, exc)
                return 0.0

        sp = await _spread_pct(primary_key)
        if sp == 0.0 or sp <= max_spread_pct:
            if sp > 0:
                self._log.info("spread OK: %s%s spread=%.1f%%", primary_strike, opt_type, sp)
            return primary_strike, primary_key

        self._log.warning(
            "spread too wide: %s%s spread=%.1f%% > %.1f%% — trying ATM %s",
            primary_strike, opt_type, sp, max_spread_pct, atm_strike,
        )
        sp_atm = await _spread_pct(atm_key)
        if sp_atm == 0.0 or sp_atm <= max_spread_pct:
            self._log.info("ATM fallback: %s%s spread=%.1f%%", atm_strike, opt_type, sp_atm)
            return atm_strike, atm_key

        self._log.warning(
            "ATM also too wide: %s%s spread=%.1f%% — using scan strike anyway",
            atm_strike, opt_type, sp_atm,
        )
        return primary_strike, primary_key  # last resort: place anyway

    # ── Instrument key builders ───────────────────────────────────────────────

    def _build_upstox_key(self, strike: Optional[int], opt_type: str) -> str:
        if not strike:
            return ""
        exp = self._expiry_str or ""
        # Try global REGISTRY first — BSE_FO requires a numeric token (not symbol format).
        # REGISTRY is pre-loaded by the rebalancer at startup; if loaded it has correct keys.
        try:
            from data_layer.instrument_registry import REGISTRY
            reg_loaded = REGISTRY.is_loaded(self._und)
            if self._expiry_date is not None and reg_loaded:
                key = REGISTRY.get_upstox_key(self._und, self._expiry_date, int(strike), opt_type)
                if key:
                    self._log.debug(
                        "_build_upstox_key %s %s%s → REGISTRY: %s", self._und, strike, opt_type, key
                    )
                    return key
                self._log.warning(
                    "_build_upstox_key %s %s%s → REGISTRY loaded but strike NOT found (expiry=%s)",
                    self._und, strike, opt_type, self._expiry_date,
                )
            elif not reg_loaded:
                self._log.warning(
                    "_build_upstox_key %s %s%s → REGISTRY NOT loaded for %s; using fallback key",
                    self._und, strike, opt_type, self._und,
                )
        except Exception as exc:
            self._log.warning("_build_upstox_key REGISTRY lookup failed: %s", exc)
        # Fallback: constructed symbol (works for NSE_FO; BSE_FO may return empty from REST)
        _PFX = {
            "NIFTY": "NSE_FO|", "BANKNIFTY": "NSE_FO|",
            "FINNIFTY": "NSE_FO|", "SENSEX": "BSE_FO|", "MIDCPNIFTY": "NSE_FO|",
            "CRUDEOIL": "MCX_FO|", "GOLDM": "MCX_FO|",
        }
        pfx = _PFX.get(self._und, "NSE_FO|")
        fallback = f"{pfx}{self._und}{exp}{strike}{opt_type}"
        self._log.warning("_build_upstox_key %s %s%s → FALLBACK key: %s", self._und, strike, opt_type, fallback)
        return fallback

    def _build_broker_symbol(self, strike: Optional[int], opt_type: str) -> str:
        exp = self._expiry_str or ""
        return f"{self._und}{exp}{strike}{opt_type}"

    # ── Expiry resolution ─────────────────────────────────────────────────────

    async def _get_expiry(self) -> Tuple[Optional[str], Optional[date]]:
        """Returns (expiry_str, expiry_date). expiry_str = e.g. '18JUN26', expiry_date = date obj.

        Primary: asks REGISTRY for the nearest loaded expiry — this is always correct because
        it comes from the actual Upstox master JSON (BSE_FO stores epochs, not symbol strings,
        so hardcoded weekday math was getting the right calendar date but the REGISTRY had a
        different date key due to timezone offset in the BSE epoch).

        Fallback: weekday math, used only if REGISTRY is not yet loaded.
        """
        try:
            from data_layer.instrument_registry import REGISTRY
            if REGISTRY.is_loaded(self._und):
                all_exp   = sorted(REGISTRY.all_expiries(self._und))
                today     = date.today()
                future    = [e for e in all_exp if e >= today]
                mode      = self._expiry_mode

                if mode == "monthly" or self._monthly_exp:
                    # Last expiry of the current calendar month from REGISTRY
                    exp_date = None
                    for i, exp in enumerate(future):
                        if i + 1 >= len(future) or future[i + 1].month != exp.month:
                            exp_date = exp
                            break
                    if exp_date is not None:
                        exp_str = exp_date.strftime("%d%b%y").upper()
                        self._log.info("_get_expiry %s → MONTHLY: %s (%s)", self._und, exp_str, exp_date)
                        return exp_str, exp_date
                    self._log.warning("_get_expiry %s → MONTHLY but no monthly expiry in REGISTRY", self._und)

                elif mode == "next_week" or self._next_week_exp:
                    # Skip the nearest expiry — get the one after it
                    if len(future) >= 2:
                        exp_date = future[1]
                        exp_str  = exp_date.strftime("%d%b%y").upper()
                        self._log.info("_get_expiry %s → NEXT WEEK: %s (%s)", self._und, exp_str, exp_date)
                        return exp_str, exp_date

                elif len(mode) == 10 and mode[4] == "-":
                    # Specific date: YYYY-MM-DD
                    import re as _re
                    if _re.match(r"^\d{4}-\d{2}-\d{2}$", mode):
                        from datetime import date as _date
                        picked = _date.fromisoformat(mode)
                        # Snap to nearest REGISTRY expiry on or after picked date
                        snapped = next((e for e in future if e >= picked), None)
                        if snapped is not None:
                            exp_str = snapped.strftime("%d%b%y").upper()
                            self._log.info("_get_expiry %s → FIXED %s → snapped: %s (%s)",
                                           self._und, mode, exp_str, snapped)
                            return exp_str, snapped

                # Default: nearest (current week) expiry
                exp_date = REGISTRY.get_active_expiry(self._und)
                if exp_date is not None:
                    exp_str = exp_date.strftime("%d%b%y").upper()
                    self._log.info("_get_expiry %s → CURRENT: %s (%s)", self._und, exp_str, exp_date)
                    return exp_str, exp_date
                self._log.warning("_get_expiry %s → REGISTRY loaded but no active expiry found", self._und)
        except Exception as exc:
            self._log.warning("_get_expiry REGISTRY lookup failed: %s", exc)
        # Fallback: weekday math (works for NSE; BSE/MCX may differ — prefer REGISTRY)
        _EXPIRY_DOW = {
            "NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1,
            "SENSEX": 3, "MIDCPNIFTY": 1,  # SENSEX = Thursday (verified from BSE master 2026-06-17)
        }
        if self._und == "CRUDEOIL":
            # MCX CrudeOil options expire on 20th (or nearest preceding Monday if 20th is weekend)
            today = date.today()
            d = date(today.year, today.month, 20)
            while d.weekday() > 4:   # weekend → back to Friday (but MCX uses Monday; keep simple)
                d -= timedelta(days=1)
            if d < today:   # 20th already passed this month → next month
                import calendar as _cal
                nm = today.month + 1 if today.month < 12 else 1
                ny = today.year if today.month < 12 else today.year + 1
                d = date(ny, nm, 20)
                while d.weekday() > 4:
                    d -= timedelta(days=1)
            self._log.warning("_get_expiry CRUDEOIL → REGISTRY unavailable; date-20 fallback: %s", d)
            return d.strftime("%d%b%y").upper(), d
        # Calendar fallback — REGISTRY not loaded yet.
        # next_week_expiry CANNOT be resolved without registry contract list;
        # log an error and fall back to current-week expiry until REGISTRY loads.
        if self._next_week_exp:
            self._log.error(
                "_get_expiry %s → next_week_expiry=True but REGISTRY not loaded; "
                "falling back to nearest expiry from calendar — RESTART after REGISTRY loads",
                self._und,
            )
        weekday = _EXPIRY_DOW.get(self._und, 3)
        d = date.today()
        for _ in range(7):
            if d.weekday() == weekday:
                self._log.warning("_get_expiry %s → REGISTRY unavailable; weekday fallback: %s", self._und, d)
                return d.strftime("%d%b%y").upper(), d
            d += timedelta(days=1)
        return None, None

    # ── Subscriptions ─────────────────────────────────────────────────────────

    async def _subscribe_instruments(self) -> None:
        """Pin + force-subscribe all tracked option keys so ticks arrive regardless of ATM window."""
        # Step 1: pin strikes so rebalancer never unsubscribes them on ATM drift
        if self._rebalancer is not None:
            for strike in [self._ce1_strike, self._ce2_strike, self._pe1_strike, self._pe2_strike]:
                if strike:
                    try:
                        self._rebalancer.pin_strike(self._und, float(strike))
                    except Exception as exc:
                        self._log.warning("pin_strike %s %s: %s", self._und, strike, exc)
        else:
            self._log.warning("No rebalancer set — falling back to direct feeder subscription only")

        # Step 2: force-subscribe the specific instrument keys directly via feeder.
        # pin_strike alone only prevents UNsubscription; it does NOT subscribe a key that
        # was never in the ATM window.  Deep-ITM / OTM legs used by TrapScanner are often
        # outside the ±N-strike window, so they never receive ticks without this call.
        # For futures-mode (CrudeOil) also subscribe the futures key so INDEX_TICK arrives.
        keys = [k for k in [self._fut_key,
                             self._ce1_key, self._ce2_key,
                             self._pe1_key, self._pe2_key] if k]
        if keys:
            feeder = (self._mcx_feeder if self._mcx_feeder is not None
                      else getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None)
            if feeder and hasattr(feeder, "subscribe_tokens"):
                try:
                    await feeder.subscribe_tokens(keys)
                    self._log.info("force-subscribed %d option keys via %s: %s",
                                   len(keys), type(feeder).__name__, keys)
                except Exception as exc:
                    self._log.warning("feeder.subscribe_tokens failed: %s", exc)
            else:
                self._log.warning("No feeder accessible — option ticks depend on ATM window")

        self._log.info(
            "pinned CE1=%s(%s) CE2=%s(%s) PE1=%s(%s) PE2=%s(%s) for %s",
            self._ce1_strike, self._ce1_key,
            self._ce2_strike, self._ce2_key,
            self._pe1_strike, self._pe1_key,
            self._pe2_strike, self._pe2_key,
            self._und,
        )
