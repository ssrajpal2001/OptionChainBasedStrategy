"""
strategies/trap_scanner/config.py — per-index + admin config loading.
"""
from __future__ import annotations

from typing import Dict

# ── Per-index config ──────────────────────────────────────────────────────────
_INDEX_CFG: Dict[str, dict] = {
    # htf_source="option": HTF and LTF both scan OPTION premium bars (same units → scan_ltf works)
    # Reference: NiftyTrapScanner phase2/ltf-entry-engine CLAUDE.md Section 2
    "NIFTY":      {"step": 100, "lot": 65,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 10.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "BANKNIFTY":  {"step": 100, "lot": 30,  "gap_near": 400, "gap_far": 800,
                   "sl_buf": 4.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "FINNIFTY":   {"step": 50,  "lot": 40,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 2.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "SENSEX":     {"step": 100, "lot": 20,  "gap_near": 300, "gap_far": 600,
                   "sl_buf": 2.0, "cutoff": "15:20", "sq_off": "15:25",
                   "window": None, "exchange": "BFO", "htf_source": "option"},
    "MIDCPNIFTY": {"step": 25,  "lot": 75,  "gap_near": 100, "gap_far": 200,
                   "sl_buf": 1.0, "cutoff": "15:10", "sq_off": "15:20",
                   "window": None, "exchange": "NFO", "htf_source": "option"},
    "CRUDEOIL":   {"step": 100, "lot": 100, "gap_near": 200, "gap_far": 500,
                   "sl_buf": 20.0, "cutoff": "22:45", "sq_off": "23:00",
                   "window": [[14, 30], [22, 45]], "exchange": "MCX",
                   "htf_source": "futures", "htf_min_override": 30},
    "BTC":        {"step": 1000, "lot": 1,  "gap_near": 2000, "gap_far": 4000,
                   "sl_buf": 50.0, "cutoff": "23:00", "sq_off": "23:15",
                   "window": None, "exchange": "DELTA", "htf_source": "futures"},
    "ETH":        {"step": 100, "lot": 1,  "gap_near": 200, "gap_far": 400,
                   "sl_buf": 5.0, "cutoff": "23:00", "sq_off": "23:15",
                   "window": None, "exchange": "DELTA", "htf_source": "futures"},
}

# Upstox REST instrument keys for spot / futures data
_SPOT_KEYS: Dict[str, str] = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "CRUDEOIL":   "MCX_FO|499095",   # CRUDEOIL near-month futures (dynamic in production)
}


def _pivot_levels(H: float, L: float, C: float) -> Dict[str, float]:
    P = (H + L + C) / 3
    return {
        "pivot": P,
        "r1": 2 * P - L, "r2": P + (H - L),
        "s1": 2 * P - H, "s2": P - (H - L),
    }


def _round_strike(price: float, step: int) -> int:
    return int(round(price / step) * step)


class ConfigMixin:
    """Load per-index defaults overlaid with admin per-index overrides."""

    def _load_index_config(self, und: str, ts_admin_cfg: dict) -> None:
        _def = _INDEX_CFG.get(und, _INDEX_CFG["NIFTY"])
        _adm = ts_admin_cfg.get("per_index", {}).get(und, {})

        self._step       = int(_def["step"])
        self._lot_size   = int(_adm.get("lot_size",     _def["lot"]))
        self._sl_buf     = float(_adm.get("sl_buffer",  _def["sl_buf"]))
        self._gap_near   = int(_adm.get("gap_itm_near", _def["gap_near"]))
        self._gap_far    = int(_adm.get("gap_itm_far",  _def["gap_far"]))
        self._cutoff_str = _adm.get("entry_cutoff",     _def["cutoff"])
        self._sq_off_str = _adm.get("sq_off_time",      _def["sq_off"])
        self._entry_win    = _adm.get("entry_window",    _def["window"])
        # Profit floor: lock ₹N once total P&L (T1+remainder) hits it.
        # If P&L drops back below floor → exit immediately at that tick. 0 = disabled.
        self._profit_floor  = float(_adm.get("profit_floor", 0.0))
        # Legacy admin toggles — kept for backward-compat but _expiry_mode takes priority
        self._next_week_exp  = bool(_adm.get("next_week_expiry", False))
        self._monthly_exp    = bool(_adm.get("monthly_expiry", False))
        # No-Target-TSL mode: skip T1 half-exit and TSL; floor locks from total P&L directly.
        # Exit only on: SL, OPP_SIGNAL (opposite side), Floor breach, EOD.
        self._no_target_tsl = bool(_adm.get("no_target_tsl", False))
        # Scale-in mode: split entry into 1 lot probe + 3 lot add + rest on 1m breach.
        # Default False — keeps original retest logic until explicitly enabled per index.
        self._scale_in_enabled = bool(_adm.get("scale_in_enabled", False))
        self._exchange   = _def["exchange"]
        self._htf_source = _def["htf_source"]   # "spot" or "futures"
        self._gap_thresh  = float(ts_admin_cfg.get("gap_threshold_pct", 0.5))
        self._admin_cfg   = ts_admin_cfg
        # CrudeOil HTF = 30-min (frozen per spec); all others = admin-configurable (default 75)
        _htf_override     = _def.get("htf_min_override")
        self._htf_min     = _htf_override if _htf_override else int(ts_admin_cfg.get("htf_minutes", 75))
        self._ltf_min     = int(ts_admin_cfg.get("ltf_minutes", 5))
