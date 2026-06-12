"""
data_layer/universal_option_mapper.py — exchange-agnostic option symbol + lifecycle mapping.

STAGE 1 of the Delta-Exchange (crypto) plug-and-play integration. PURE + timezone-aware + fully
unit-tested. No I/O, no network, no broker SDK imports — the strategy layer stays market-agnostic;
only feeder/broker adapters call these to translate the neutral request into an exchange string.

It builds on the existing broker-neutral `InternalSymbol` (data_layer/symbol_translator.py):
    InternalSymbol(underlying, strike, option_type="CE"|"PE", expiry: date)

Two markets, fundamentally different lifecycles:
  • NSE/BSE : weekly/monthly expiries, suffix CE/PE, contract expires 15:30 IST on expiry day.
  • DELTA   : DAILY crypto options 24/7/365, suffix C/P, every contract expires 17:30 IST
              (12:00 UTC). At 17:30 IST the front-day contract dies and the next day's mints.

Delta symbol format (verified shape):  {UNDERLYING}-{DDMONYY}-{STRIKE}-{C|P}   e.g. BTC-12JUN26-70000-C
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Optional, Tuple

try:                                            # py3.9+ stdlib
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
    UTC = ZoneInfo("UTC")
except Exception:                               # pragma: no cover - fallback for minimal envs
    from datetime import timezone as _tz
    IST = _tz(timedelta(hours=5, minutes=30))
    UTC = _tz.utc

from data_layer.symbol_translator import InternalSymbol

# Crypto daily contracts expire at 17:30 IST == 12:00 UTC, every calendar day.
DELTA_DAILY_EXPIRY_IST = time(17, 30, 0)

_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
_MONTH_IDX = {m: i + 1 for i, m in enumerate(_MONTHS)}


class UniversalOptionMapper:
    """Static, pure helpers. The strategy passes abstract intent
    (underlying / CALL|PUT / strike / expiry) and never concatenates exchange strings itself."""

    # ── Option-type normalization ─────────────────────────────────────────────
    @staticmethod
    def to_short_type(option_type: str) -> str:
        """CE/CALL/C → 'C'; PE/PUT/P → 'P' (Delta style)."""
        t = str(option_type).strip().upper()
        if t in ("CE", "CALL", "C"):
            return "C"
        if t in ("PE", "PUT", "P"):
            return "P"
        raise ValueError(f"Unknown option type: {option_type!r}")

    @staticmethod
    def to_internal_type(option_type: str) -> str:
        """CE/CALL/C → 'CE'; PE/PUT/P → 'PE' (NSE/internal style)."""
        return "CE" if UniversalOptionMapper.to_short_type(option_type) == "C" else "PE"

    # ── Delta symbol <-> InternalSymbol ───────────────────────────────────────
    @staticmethod
    def to_delta_symbol(internal: InternalSymbol) -> str:
        """InternalSymbol → 'BTC-12JUN26-70000-C'."""
        return (f"{internal.underlying.upper()}-{internal.expiry.strftime('%d%b%y').upper()}"
                f"-{internal.strike_int}-{UniversalOptionMapper.to_short_type(internal.option_type)}")

    @staticmethod
    def parse_delta_symbol(symbol: str) -> InternalSymbol:
        """'BTC-12JUN26-70000-C' → InternalSymbol(underlying, strike, 'CE'/'PE', expiry)."""
        parts = str(symbol).strip().upper().split("-")
        if len(parts) != 4:
            raise ValueError(f"Not a Delta option symbol: {symbol!r}")
        und, ddmonyy, strike_s, ctype = parts
        dd, mon, yy = ddmonyy[:2], ddmonyy[2:5], ddmonyy[5:7]
        if mon not in _MONTH_IDX:
            raise ValueError(f"Bad month in Delta symbol: {symbol!r}")
        exp = date(2000 + int(yy), _MONTH_IDX[mon], int(dd))
        return InternalSymbol(
            underlying=und, strike=float(strike_s),
            option_type=UniversalOptionMapper.to_internal_type(ctype), expiry=exp,
        )

    # ── Daily-expiry lifecycle (the 17:30 IST rollover engine) ─────────────────
    @staticmethod
    def active_daily_expiry(now: Optional[datetime] = None) -> date:
        """The expiry DATE of the currently-active Delta daily contract.
        Before 17:30 IST → today; at/after 17:30 IST → tomorrow (the just-minted front-day)."""
        n = (now or datetime.now(IST)).astimezone(IST)
        return n.date() + timedelta(days=1) if n.time() >= DELTA_DAILY_EXPIRY_IST else n.date()

    @staticmethod
    def next_rollover_at(now: Optional[datetime] = None) -> datetime:
        """The next 17:30-IST boundary (timezone-aware) at which the active daily contract rolls."""
        n = (now or datetime.now(IST)).astimezone(IST)
        today_cutoff = datetime.combine(n.date(), DELTA_DAILY_EXPIRY_IST, tzinfo=IST)
        return today_cutoff if n < today_cutoff else today_cutoff + timedelta(days=1)

    @staticmethod
    def seconds_to_next_rollover(now: Optional[datetime] = None) -> float:
        n = (now or datetime.now(IST)).astimezone(IST)
        return (UniversalOptionMapper.next_rollover_at(n) - n).total_seconds()

    @staticmethod
    def build_internal(underlying: str, option_type: str, strike: float,
                       exchange: str = "DELTA", expiry: Optional[date] = None,
                       now: Optional[datetime] = None) -> InternalSymbol:
        """Abstract intent → InternalSymbol. For DELTA with no explicit expiry, resolves the active
        daily expiry (honouring the 17:30 IST rollover). NSE callers pass an explicit weekly/monthly
        expiry (the existing registry/translator owns NSE expiry math)."""
        if expiry is None:
            if str(exchange).upper() == "DELTA":
                expiry = UniversalOptionMapper.active_daily_expiry(now)
            else:
                raise ValueError("NSE/BSE requires an explicit expiry date.")
        return InternalSymbol(
            underlying=str(underlying).upper(), strike=float(strike),
            option_type=UniversalOptionMapper.to_internal_type(option_type), expiry=expiry,
        )
