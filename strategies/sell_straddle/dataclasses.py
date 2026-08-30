"""
strategies/sell_straddle/dataclasses.py — StraddleLeg + StraddlePosition.

Pure data containers for a sold ATM straddle.  Kept intentionally free of
strategy/feed dependencies so they can be imported and tested in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
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
    expiry_date: Optional[date] = None

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

    # EOD hedge-and-carry (2026-08-20, user spec): when both sold legs are running in
    # loss at close-of-day (and it isn't T-1 from expiry), each sold leg gets a bought
    # protective leg on the SAME side, far enough OTM that its LTP is <=50% of the sold
    # leg's running LTP. The position then carries forward as a positional (NRML) trade
    # instead of the normal EOD square-off. `is_hedged_positional` is the flag that
    # tells the EOD/exit machinery "this position is deliberately not being flattened
    # today" -- persisted via to_dict()/from_dict() same as everything else so a
    # restart the next trading day recognizes and keeps managing it, not treats it as
    # stale. The hedge legs are NEVER touched by the normal sold-leg rollover/exit
    # logic -- they only ever get closed together with the sold legs, either by the
    # T-1-from-expiry forced closure or the same-strike-collision guard (see
    # exits.py/rolling.py).
    hedge_ce_leg: Optional[StraddleLeg] = None
    hedge_pe_leg: Optional[StraddleLeg] = None
    is_hedged_positional: bool = False

    # Post-15:00 per-leg R1 exit (2026-08-28, direct user spec): once a leg is
    # closed independently via this mechanic, the OTHER leg keeps running solo
    # -- these flags are how the rest of the position machinery knows one side
    # is gone. current_value/unrealized_pnl below exclude a closed leg's ltp
    # entirely rather than leaving it frozen-but-counted (which would silently
    # double the closed leg's own P&L into every downstream sum once its
    # realized P&L is also booked into the session total by _close_leg).
    ce_leg_closed: bool = False
    pe_leg_closed: bool = False

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
            "entry_spot": self.entry_spot, "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "ce_leg": _leg(self.ce_leg), "pe_leg": _leg(self.pe_leg),
            "net_credit": self.net_credit, "tsl_high_lock_rs": self.tsl_high_lock_rs,
            "peak_profit": self.peak_profit, "trailing_active": self.trailing_active,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "entry_time": self.open_time.isoformat() if self.open_time else None,
            "realized_pnl": self.realized_pnl, "status": self.status,
            "entry_indicators": dict(self.entry_indicators),
            "lot_size": self.lot_size,
            "entry_time_value": self.entry_time_value,
            "session_min_vwap": self.session_min_vwap,
            "vwap_last_good": self.vwap_last_good,
            # 2026-08-27, direct user-driven audit ("check every part which is used
            # for application work should be stored not in memory"): this field was
            # a dataclass attribute updated live by the trailing-SL mechanic but was
            # never actually included here -- a restart silently reset the trailing
            # floor's own peak-profit% tracking back to 0.0 even with a real,
            # already-progressed trailing stop in flight.
            "trail_peak_pct": self.trail_peak_pct,
            "hedge_ce_leg": _leg(self.hedge_ce_leg) if self.hedge_ce_leg else None,
            "hedge_pe_leg": _leg(self.hedge_pe_leg) if self.hedge_pe_leg else None,
            "is_hedged_positional": self.is_hedged_positional,
            "ce_leg_closed": self.ce_leg_closed,
            "pe_leg_closed": self.pe_leg_closed,
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
            expiry_date=_dt.fromisoformat(d["expiry_date"]).date() if d.get("expiry_date") else None,
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
            trail_peak_pct=d.get("trail_peak_pct", 0.0),
            hedge_ce_leg=_leg(d["hedge_ce_leg"]) if d.get("hedge_ce_leg") else None,
            hedge_pe_leg=_leg(d["hedge_pe_leg"]) if d.get("hedge_pe_leg") else None,
            is_hedged_positional=bool(d.get("is_hedged_positional", False)),
            ce_leg_closed=bool(d.get("ce_leg_closed", False)),
            pe_leg_closed=bool(d.get("pe_leg_closed", False)),
        )

    @property
    def current_value(self) -> float:
        v = 0.0
        if not self.ce_leg_closed:
            v += self.ce_leg.ltp
        if not self.pe_leg_closed:
            v += self.pe_leg.ltp
        return v

    @property
    def unrealized_pnl(self) -> float:
        pnl = 0.0
        if not self.ce_leg_closed:
            pnl += self.ce_leg.entry_price - self.ce_leg.ltp
        if not self.pe_leg_closed:
            pnl += self.pe_leg.entry_price - self.pe_leg.ltp
        return pnl

    @property
    def hedge_unrealized_pnl(self) -> float:
        """Running P&L on the BOUGHT hedge legs (long -- opposite sign
        convention from the sold legs' unrealized_pnl, which is a short
        position). 0.0 for any leg not currently built."""
        pnl = 0.0
        if self.hedge_ce_leg is not None:
            pnl += self.hedge_ce_leg.ltp - self.hedge_ce_leg.entry_price
        if self.hedge_pe_leg is not None:
            pnl += self.hedge_pe_leg.ltp - self.hedge_pe_leg.entry_price
        return pnl

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
