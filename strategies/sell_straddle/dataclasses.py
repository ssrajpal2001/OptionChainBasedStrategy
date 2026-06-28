"""
strategies/sell_straddle/dataclasses.py — StraddleLeg + StraddlePosition.

Pure data containers for a sold ATM straddle.  Kept intentionally free of
strategy/feed dependencies so they can be imported and tested in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class StraddleLeg:
    option_type: str
    strike: float
    entry_price: float
    ltp: float = 0.0
    mark: float = 0.0          # broker mark/ATP (fair value) — used for crypto P&L display (LTP is noisy)
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    open_reason: str = ""
    symbol: str = ""            # full broker symbol e.g. C-BTC-64000-140626 or NIFTY24600CE


@dataclass
class StraddlePosition:
    underlying: str
    atm_at_entry: float
    entry_spot: float
    ce_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("CE", 0, 0))
    pe_leg: StraddleLeg = field(default_factory=lambda: StraddleLeg("PE", 0, 0))

    net_credit: float = 0.0       # CE_entry + PE_entry at open
    tsl_high_lock_rs: float = 0.0  # Highest scalable TSL lock reached in ₹
    peak_profit: float = 0.0       # Highest unrealized P&L seen (for trailing SL)
    trailing_active: bool = False  # True once profit crossed trail_lock threshold

    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    close_reason: str = ""
    realized_pnl: float = 0.0
    status: str = "open"           # "open" | "closed"

    entry_indicators: Dict[str, float] = field(default_factory=dict)

    # Session VWAP tracking for VWAP Rise SL
    session_min_vwap: float = float("inf")

    # Trailing-SL (lock-%/floor-%) peak profit % since entry (basis ltp or theta). Highest
    # profit% seen; once it crosses lock%, exit when profit drops floor% below this peak.
    trail_peak_pct: float = 0.0

    # Last ACCEPTED combined VWAP (dropout filter for vwap_rise): a sudden crater vs this (one
    # leg's ATP dropping out) is rejected so it can't poison session_min_vwap → false vwap_rise.
    vwap_last_good: float = 0.0

    # Day-wise THETA exit: combined option TIME VALUE (extrinsic) captured at entry. The
    # theta-based day exit measures how far the live combined time value has decayed from this.
    entry_time_value: float = 0.0

    # Total contracts per leg (lot_size × lot_multiplier) — used by the dashboard
    # to render qty and rupee P&L. Without it the UI shows qty=0 → P&L always 0.
    lot_size: int = 0

    def to_dict(self) -> dict:
        """JSON-serialisable snapshot for PositionStore."""
        def _leg(l: StraddleLeg) -> dict:
            return {"option_type": l.option_type, "strike": l.strike,
                    "entry_price": l.entry_price, "ltp": l.ltp,
                    "open_time": l.open_time.isoformat() if l.open_time else None,
                    "close_time": l.close_time.isoformat() if l.close_time else None,
                    "open_reason": l.open_reason}
        return {
            "underlying": self.underlying, "atm_at_entry": self.atm_at_entry,
            "entry_spot": self.entry_spot,
            "ce_leg": _leg(self.ce_leg), "pe_leg": _leg(self.pe_leg),
            "net_credit": self.net_credit, "tsl_high_lock_rs": self.tsl_high_lock_rs,
            "peak_profit": self.peak_profit, "trailing_active": self.trailing_active,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "realized_pnl": self.realized_pnl, "status": self.status,
            "entry_indicators": dict(self.entry_indicators),
            "lot_size": self.lot_size,
            "entry_time_value": self.entry_time_value,
            "session_min_vwap": self.session_min_vwap,
            "vwap_last_good": self.vwap_last_good,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StraddlePosition":
        from datetime import datetime as _dt
        def _leg(x: dict) -> StraddleLeg:
            return StraddleLeg(option_type=x["option_type"], strike=x["strike"],
                               entry_price=x["entry_price"], ltp=x.get("ltp", 0.0),
                               open_time=_dt.fromisoformat(x["open_time"]) if x.get("open_time") else None,
                               close_time=_dt.fromisoformat(x["close_time"]) if x.get("close_time") else None,
                               open_reason=x.get("open_reason", ""))
        return cls(
            underlying=d["underlying"], atm_at_entry=d.get("atm_at_entry", 0.0),
            entry_spot=d.get("entry_spot", 0.0),
            ce_leg=_leg(d["ce_leg"]), pe_leg=_leg(d["pe_leg"]),
            net_credit=d.get("net_credit", 0.0), tsl_high_lock_rs=d.get("tsl_high_lock_rs", 0.0),
            peak_profit=d.get("peak_profit", 0.0), trailing_active=d.get("trailing_active", False),
            open_time=_dt.fromisoformat(d["open_time"]) if d.get("open_time") else None,
            realized_pnl=d.get("realized_pnl", 0.0), status=d.get("status", "open"),
            entry_indicators=dict(d.get("entry_indicators", {})),
            lot_size=d.get("lot_size", 0),
            entry_time_value=d.get("entry_time_value", 0.0),
            session_min_vwap=d.get("session_min_vwap", float("inf")),
            vwap_last_good=d.get("vwap_last_good", 0.0),
        )

    @property
    def current_value(self) -> float:
        return self.ce_leg.ltp + self.pe_leg.ltp

    @property
    def unrealized_pnl(self) -> float:
        return self.net_credit - self.current_value

    def current_time_value(self, spot: float) -> float:
        """Live combined option time value (extrinsic) at the given spot — for theta-based exit."""
        from strategies.theta_calc import combined_time_value
        return combined_time_value(self.ce_leg.strike, self.pe_leg.strike, spot,
                                   self.ce_leg.ltp, self.pe_leg.ltp)

    def theta_decay_pct(self, spot: float) -> float:
        """Signed % the combined time value has decayed since entry (positive = profit)."""
        from strategies.theta_calc import theta_decay_pct as _tdp
        return _tdp(self.entry_time_value, self.current_time_value(spot))

    def premium_decay_pct(self) -> float:
        """CLEAN theta% (user spec 2026-06-10): the decay tracked against the TOTAL THETA
        RECEIVED AT ENTRY. = (entry premium − current premium) / entry_time_value × 100, where
        entry_time_value is the combined TIME VALUE captured at entry ('total theta received';
        for an ATM straddle it equals the entry premium). The numerator is the premium decay
        (= running P&L in pts). Because the denominator is fixed at entry, the absolute profit/SL
        thresholds (entry_theta × day%) are known at the start. Positive = decayed = profit; it
        tracks P&L and rises cleanly (no spot-driven oscillation)."""
        base = float(getattr(self, "entry_time_value", 0.0) or 0.0) or float(self.net_credit or 0.0)
        if base <= 0:
            return 0.0
        return (self.net_credit - self.current_value) / base * 100.0


def format_exit_eval(underlying: str, pnl_pts: float, credit: float, criteria) -> str:
    """One EXIT-EVAL log line showing every exit criterion checked on the max-TF close.
    `criteria`: list of (name, detail, hit:bool). Shows current-vs-threshold + ✓/✗ per
    criterion and the overall HOLD/EXIT outcome — mirrors the entry EVAL line."""
    parts, fired = [], []
    for name, detail, hit in criteria:
        parts.append(f"{name}({detail})={'✓HIT' if hit else '✗'}")
        if hit:
            fired.append(name)
    pct = (pnl_pts / credit * 100.0) if credit else 0.0
    outcome = ("EXIT:" + ",".join(fired)) if fired else "HOLD"
    return (f"EXIT-EVAL {underlying} pnl={pnl_pts:.2f} ({pct:.1f}% of credit) | "
            + " | ".join(parts) + f" → {outcome}")
