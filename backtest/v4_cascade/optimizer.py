"""backtest/v4_cascade/optimizer.py -- grid-search over V4 Cascade's exit
parameters (sl_buffer, target_floor_multiple, TSL lookback_bases/tf_minutes),
re-running run_backtest.run_backtest per cell against the SAME already-built
5m bars. Entry timing/price is NOT fully independent of exit parameters here
(an earlier/later exit changes when the engine is next free to scan for a
new entry on that side), so each grid cell gets its own full, independent
replay -- correct over clever, and still cheap since there are no REST calls
inside the loop (all bars are already fetched once, up front)."""
from __future__ import annotations

import itertools
from typing import Dict, List, Optional

from strategies.v4_cascade.config import V4CascadeConfig

from backtest.v4_cascade.run_backtest import run_backtest

DEFAULT_GRID: Dict[str, list] = {
    "sl_buffer": [5.0, 10.0, 15.0, 20.0],
    "target_floor_multiple": [1.0, 1.5, 2.0, 2.5],
    "t2_trail_lookback_bases": [2, 4, 6],
    # NOTE: no t2_trail_tf_minutes grid dimension -- TrailingBaseTracker's
    # internal scanner has a hardcoded ladder=[5] (see exits.py); there is
    # no real mechanism today for a 15m-timeframe TSL, so grid-searching it
    # would test something that doesn't actually exist in production.
}


def compute_metrics(legs: List[dict]) -> dict:
    """Rupee P&L (pnl_points * qty), events in chronological close order.
    profit_factor = gross_profit / gross_loss (inf if there are wins and no
    losses at all, 0.0 if there are no wins). max_drawdown is the largest
    peak-to-trough dip in the cumulative equity curve (negative or zero)."""
    events = sorted(
        (leg for leg in legs if leg.get("pnl_points") is not None and leg.get("qty")),
        key=lambda l: l["close_ts"] or l["entry_ts"],
    )
    if not events:
        return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "profit_factor": 0.0, "max_drawdown": 0.0, "net_pnl": 0.0}
    pnls_rupees = [leg["pnl_points"] * leg["qty"] for leg in events]
    gross_profit = sum(p for p in pnls_rupees if p > 0)
    gross_loss = -sum(p for p in pnls_rupees if p < 0)
    wins = sum(1 for p in pnls_rupees if p > 0)
    losses = sum(1 for p in pnls_rupees if p < 0)
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = float("inf") if gross_profit > 0 else 0.0
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls_rupees:
        equity += p
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return {
        "trades": len(pnls_rupees), "wins": wins, "losses": losses,
        "win_rate": round(100.0 * wins / len(pnls_rupees), 2),
        "profit_factor": round(profit_factor, 3) if profit_factor != float("inf") else profit_factor,
        "max_drawdown": round(max_dd, 2),
        "net_pnl": round(sum(pnls_rupees), 2),
    }


def grid_search(bars_5m, underlying: str = "NIFTY", lot_size: int = 65, lot_multiplier: int = 2,
                 grid: Optional[Dict[str, list]] = None) -> List[dict]:
    """Returns every combo's {params, metrics, legs}, ranked best-first:
    higher profit_factor wins; ties (including two 'inf' cells, i.e. wins
    with zero losses) broken by the LESS-negative max_drawdown."""
    grid = grid or DEFAULT_GRID
    keys = list(grid.keys())
    results = []
    for combo in itertools.product(*(grid[k] for k in keys)):
        params = dict(zip(keys, combo))
        cfg = V4CascadeConfig(
            underlying=underlying, lot_size=lot_size, lot_multiplier=lot_multiplier,
            sl_buffer=params["sl_buffer"], target_floor_multiple=params["target_floor_multiple"],
            t2_trail_lookback_bases=params["t2_trail_lookback_bases"],
        )
        legs = run_backtest(cfg, bars_5m)
        results.append({"params": params, "metrics": compute_metrics(legs), "legs": legs})

    def rank_key(r: dict):
        pf = r["metrics"]["profit_factor"]
        pf_key = pf if pf != float("inf") else 1e9
        return (-pf_key, -r["metrics"]["max_drawdown"])

    results.sort(key=rank_key)
    return results
