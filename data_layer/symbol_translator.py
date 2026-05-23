"""
data_layer/symbol_translator.py — Unified internal symbol ↔ broker format mapper.

Each broker uses its own exotic symbol format. All internal strategy
logic uses the neutral InternalSymbol struct. Only the execution_bridge
and the feeder adapters call these translation methods.

Format references (verified as of 2025):
  Shoonya  : NIFTY28MAY26C22000        (underlying + DDMONYY + C/P + strike)
  Fyers    : NSE:NIFTY2652822000CE     (exchange:underlying + YY + expiry_code + strike + CE/PE)
  AngelOne : NIFTY28MAY2422000CE       (underlying + DDMON + YY + strike + CE/PE)
  Dhan     : uses numeric security_id from instrument master (token-based)
  Upstox   : NSE_FO|NIFTY2562522000CE  (segment|underlying + YYDDMM + strike + CE/PE)
             instrument_key lookup required for API calls
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Internal Symbol (broker-neutral)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class InternalSymbol:
    underlying: str              # e.g. "NIFTY"
    strike: float
    option_type: str             # "CE" or "PE"
    expiry: date

    @property
    def strike_int(self) -> int:
        return int(self.strike)

    def __str__(self) -> str:
        return (
            f"{self.underlying}:{self.expiry.strftime('%d%b%y').upper()}"
            f":{self.strike_int}:{self.option_type}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Month code tables
# ─────────────────────────────────────────────────────────────────────────────

_MONTH_3 = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

# Fyers uses single-char month codes for weekly expiries
_FYERS_MONTH_CODE = {
    1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
    7: "7", 8: "8", 9: "9", 10: "O", 11: "N", 12: "D",
}


# ─────────────────────────────────────────────────────────────────────────────
# Translator
# ─────────────────────────────────────────────────────────────────────────────

class SymbolTranslator:
    """
    Static factory methods for converting InternalSymbol → broker symbol string.

    All methods are pure functions (no I/O, no state).
    """

    # ── Shoonya / Finvasia ────────────────────────────────────────────────────

    @staticmethod
    def to_shoonya(sym: InternalSymbol) -> str:
        """
        Format: NIFTY28MAY26C22000
        Underlying + DD + MON + YY + C/P + strike (no decimal, no space)
        """
        dd = sym.expiry.strftime("%d").lstrip("0") or "0"
        mon = _MONTH_3[sym.expiry.month - 1]
        yy = sym.expiry.strftime("%y")
        cp = "C" if sym.option_type == "CE" else "P"
        return f"{sym.underlying}{dd}{mon}{yy}{cp}{sym.strike_int}"

    @staticmethod
    def from_shoonya(raw: str) -> Optional[InternalSymbol]:
        """Parse a Shoonya symbol back to InternalSymbol."""
        pattern = r"^([A-Z]+)(\d{1,2})([A-Z]{3})(\d{2})([CP])(\d+)$"
        m = re.match(pattern, raw)
        if not m:
            return None
        underlying, dd, mon, yy, cp, strike_str = m.groups()
        month = _MONTH_3.index(mon) + 1
        year = 2000 + int(yy)
        expiry = date(year, month, int(dd))
        return InternalSymbol(
            underlying=underlying,
            strike=float(strike_str),
            option_type="CE" if cp == "C" else "PE",
            expiry=expiry,
        )

    # ── Fyers ─────────────────────────────────────────────────────────────────

    @staticmethod
    def to_fyers(sym: InternalSymbol) -> str:
        """
        Weekly: NSE:NIFTY2652822000CE
        Format: NSE:{underlying}{YY}{M_CODE}{DD}{strike}{CE/PE}
        Monthly expiries use full month number (2 digits).
        """
        yy = sym.expiry.strftime("%y")
        m_code = _FYERS_MONTH_CODE[sym.expiry.month]
        dd = sym.expiry.strftime("%d")
        exchange = "BSE" if sym.underlying == "SENSEX" else "NSE"
        return f"{exchange}:{sym.underlying}{yy}{m_code}{dd}{sym.strike_int}{sym.option_type}"

    @staticmethod
    def from_fyers(raw: str) -> Optional[InternalSymbol]:
        """Parse a Fyers symbol back to InternalSymbol."""
        pattern = r"^(?:NSE|BSE):([A-Z]+)(\d{2})([0-9ON D])(\d{2})(\d+)(CE|PE)$"
        m = re.match(pattern, raw)
        if not m:
            return None
        underlying, yy, m_code, dd, strike_str, opt_type = m.groups()
        rev_map = {v: k for k, v in _FYERS_MONTH_CODE.items()}
        month = rev_map.get(m_code, 0)
        if month == 0:
            return None
        year = 2000 + int(yy)
        expiry = date(year, month, int(dd))
        return InternalSymbol(
            underlying=underlying,
            strike=float(strike_str),
            option_type=opt_type,
            expiry=expiry,
        )

    # ── Angel One ─────────────────────────────────────────────────────────────

    @staticmethod
    def to_angelone(sym: InternalSymbol) -> str:
        """
        Format: NIFTY28MAY2422000CE
        Underlying + DD + MON + YY + strike + CE/PE
        """
        dd = sym.expiry.strftime("%d")
        mon = _MONTH_3[sym.expiry.month - 1]
        yy = sym.expiry.strftime("%y")
        return f"{sym.underlying}{dd}{mon}{yy}{sym.strike_int}{sym.option_type}"

    @staticmethod
    def from_angelone(raw: str) -> Optional[InternalSymbol]:
        pattern = r"^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$"
        m = re.match(pattern, raw)
        if not m:
            return None
        underlying, dd, mon, yy, strike_str, opt_type = m.groups()
        month = _MONTH_3.index(mon) + 1
        year = 2000 + int(yy)
        expiry = date(year, month, int(dd))
        return InternalSymbol(
            underlying=underlying,
            strike=float(strike_str),
            option_type=opt_type,
            expiry=expiry,
        )

    # ── Dhan ──────────────────────────────────────────────────────────────────

    @staticmethod
    def to_dhan_lookup_key(sym: InternalSymbol) -> str:
        """
        Dhan is token-based. Return a lookup key to find the numeric
        security_id from the instrument master loaded at startup.
        Format: underlying:DDMONYY:strike:opt_type  (internal canonical)
        """
        return str(sym)

    # ── Upstox ────────────────────────────────────────────────────────────────

    @staticmethod
    def to_upstox(sym: InternalSymbol) -> str:
        """
        Upstox instrument_key format for NFO options:
          NSE_FO|NIFTY2562522000CE
          Segment (NSE_FO / BSE_FO) + | + underlying + YY + DD + MM + strike + CE/PE

        This string is a LOOKUP KEY used to resolve the actual numeric
        instrument_token from the Upstox instrument master JSON.
        Download from: https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
        and call UpstoxBroker.inject_instrument_map() at startup.
        """
        segment = "BSE_FO" if sym.underlying == "SENSEX" else "NSE_FO"
        yy = sym.expiry.strftime("%y")
        dd = sym.expiry.strftime("%d")
        mm = sym.expiry.strftime("%m")
        return f"{segment}|{sym.underlying}{yy}{dd}{mm}{sym.strike_int}{sym.option_type}"

    @staticmethod
    def to_upstox_index(underlying: str) -> str:
        """Return the Upstox instrument key for the underlying index."""
        _map = {
            "NIFTY":     "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "FINNIFTY":  "NSE_INDEX|Nifty Fin Service",
            "MIDCPNIFTY":"NSE_INDEX|NIFTY MID SELECT",
            "SENSEX":    "BSE_INDEX|SENSEX",
        }
        return _map.get(underlying, f"NSE_INDEX|{underlying}")

    # ── Generic index token ───────────────────────────────────────────────────

    @staticmethod
    def spot_symbol(underlying: str, exchange: str = "NSE") -> str:
        """Return the spot index symbol for a given broker exchange."""
        return f"{exchange}:{underlying}-INDEX"

    # ── Canonical round-trip test ─────────────────────────────────────────────

    @classmethod
    def roundtrip_check(cls, sym: InternalSymbol) -> Dict[str, bool]:
        """Verify to/from translation is lossless for each broker. Dev/test only."""
        from typing import Dict
        results: Dict[str, bool] = {}

        sh = cls.to_shoonya(sym)
        results["shoonya"] = cls.from_shoonya(sh) == sym

        fy = cls.to_fyers(sym)
        results["fyers"] = cls.from_fyers(fy) == sym

        ao = cls.to_angelone(sym)
        results["angelone"] = cls.from_angelone(ao) == sym

        return results
