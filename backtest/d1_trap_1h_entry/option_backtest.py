"""
backtest/d1_trap_1h_entry/option_backtest.py
============================================
Option P&L backtest for C2 + TWEAK strategy — July 2026.

When the spot strategy fires a trade:
  LONG  -> Buy NIFTY weekly CE, strike = ATM - 100  (2 ITM below ATM)
  SHORT -> Buy NIFTY weekly PE, strike = ATM + 100  (2 ITM above ATM)

Pricing: Black-Scholes with 20-day historical volatility estimated from NIFTY D1 bars.
Rationale: Upstox feeder token does not support the option/contract info API,
and all July weekly contracts (Tue Jul 7/14/21/28) have already expired and
are removed from the instrument master. Real-data fetch is therefore impossible
for historical July trades. B-S gives a theoretically sound approximation.

ATM   = round(spot / 50) * 50
Expiry = nearest upcoming TUESDAY (NIFTY weekly expiry day as of 2025+)
Lot   = 75, Qty = 1 lot

Usage:
  python backtest/d1_trap_1h_entry/option_backtest.py
"""
from __future__ import annotations

import asyncio
import csv
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.global_config import IST
from backtest.d1_trap_1h_entry.backtest import (
    _Bar, Trade,
    fetch_bars, resample_to_60m, resample_to_5m,
    run_backtest_combined,
)

# ── Constants ─────────────────────────────────────────────────────────────────
NIFTY_KEY   = "NSE_INDEX|Nifty 50"
STRIKE_STEP = 50        # NIFTY strike interval
ITM_STRIKES = 2         # buy 2-ITM
LOT_SIZE    = 75        # NIFTY lot size 2025+
QTY         = 1
RISK_FREE   = 0.07      # India risk-free rate (≈ RBI repo)
HIST_VOL_WINDOW = 20    # D1 bars for realised-vol estimate

JULY_START = date(2026, 7, 1)
JULY_END   = date(2026, 7, 28)

_RESULTS_DIR = str(Path(__file__).parent / "results")

# NIFTY expiry weekday: Tuesday (weekday index 1; confirmed by InstrumentRegistry)
_NIFTY_EXPIRY_WEEKDAY = 1   # 0=Mon, 1=Tue


# ── Math helpers ──────────────────────────────────────────────────────────────
def _ncdf(x: float) -> float:
    """Standard normal CDF via math.erf (no scipy needed)."""
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


def black_scholes(S: float, K: float, T: float, sigma: float,
                  r: float = RISK_FREE, opt_type: str = "CE") -> float:
    """European B-S price. T in years. Returns 0 when T<=0 and option is OTM."""
    if T <= 0:
        return max(0.0, S - K) if opt_type == "CE" else max(0.0, K - S)
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    if opt_type == "CE":
        return S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
    else:
        return K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)


def hist_vol(d1_bars: List[_Bar], at_date: date, window: int = HIST_VOL_WINDOW) -> float:
    """Annualised realised vol from the D1 bars up to and including at_date."""
    bars = [b for b in d1_bars if b.timestamp.date() <= at_date]
    bars = bars[-window - 1:]          # need window+1 closes for window returns
    if len(bars) < 5:
        return 0.14                    # fallback if data sparse
    closes = [b.close for b in bars]
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    mean = sum(rets) / len(rets)
    var  = sum((r - mean) ** 2 for r in rets) / max(len(rets) - 1, 1)
    return math.sqrt(var * 252)


def nifty_atm(spot: float) -> int:
    return round(spot / STRIKE_STEP) * STRIKE_STEP


def nearest_nifty_expiry(d: date) -> date:
    """Nearest Tuesday >= d. If d is Tuesday, skip to next Tuesday (same-day illiquid)."""
    days_ahead = (_NIFTY_EXPIRY_WEEKDAY - d.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return d + timedelta(days=days_ahead)


def T_years(from_dt: datetime, expiry: date) -> float:
    """Calendar-day fraction of a year remaining to expiry (floor 0)."""
    exp_dt = datetime(expiry.year, expiry.month, expiry.day, 15, 30, tzinfo=IST)
    remaining = (exp_dt - from_dt).total_seconds()
    return max(0.0, remaining / (365.25 * 86400))


# ── Option trade record ───────────────────────────────────────────────────────
@dataclass
class OptionTrade:
    spot_trade: Trade
    opt_type:   str       # "CE" | "PE"
    strike:     int
    expiry:     date
    sigma:      float     # realised vol used for pricing
    opt_entry:  float     # B-S premium at entry
    opt_exit:   float     # B-S premium at exit

    @property
    def opt_pnl_pts(self) -> float:
        return self.opt_exit - self.opt_entry   # long option: profit when premium rises

    @property
    def opt_pnl_rs(self) -> float:
        return self.opt_pnl_pts * LOT_SIZE * QTY

    @property
    def spot_pnl_pts(self) -> Optional[float]:
        return self.spot_trade.pnl_pts

    @property
    def spot_pnl_rs(self) -> float:
        if self.spot_trade.pnl_pts is None:
            return 0.0
        return self.spot_trade.pnl_pts * LOT_SIZE * QTY   # use LOT_SIZE=75


# ── Main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    # ── Token ────────────────────────────────────────────────────────────────
    token = os.environ.get("UPSTOX_TOKEN", "")
    if not token:
        try:
            from data_layer.client_db import ClientDB
            creds = ClientDB().get_feeder_creds_sync("upstox") or {}
            token = creds.get("access_token", "")
        except Exception:
            pass
    if not token:
        print("ERROR: set UPSTOX_TOKEN or configure Upstox creds in DB"); sys.exit(1)

    # ── Spot data — full 6 months for zone detection ─────────────────────────
    full_start = date(2026, 2, 1)
    end        = JULY_END

    print(f"\nFetching D1 bars {full_start} -> {end} ...")
    d1_bars = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "day", full_start, end, token)
    print(f"  -> {len(d1_bars)} D1 bars")

    print(f"Fetching 30m bars {full_start} -> {end} ...")
    bars_30m = await asyncio.to_thread(fetch_bars, NIFTY_KEY, "30minute", full_start, end, token)
    h1_bars  = resample_to_60m(bars_30m)
    print(f"  -> {len(h1_bars)} 60m bars")

    print(f"Fetching 1m bars {full_start} -> {end} ...")
    bars_1m = await asyncio.to_thread(
        fetch_bars, NIFTY_KEY, "1minute", full_start, end, token, chunk_days=30
    )
    m5_bars = resample_to_5m(bars_1m)
    print(f"  -> {len(m5_bars)} 5m bars")

    if not d1_bars or not h1_bars or not m5_bars:
        print("ERROR: spot data unavailable"); sys.exit(1)

    # ── C2 + TWEAK backtest ───────────────────────────────────────────────────
    print("\nRunning C2 + TWEAK (6 months, single position) ...")
    all_trades = run_backtest_combined(
        d1_bars, h1_bars, m5_bars,
        exit_mode="tsl_1h", use_flip=False, use_tweak=True,
    )
    july_trades = [
        t for t in all_trades
        if t.entry_ts is not None and JULY_START <= t.entry_ts.date() <= JULY_END
        and t.pnl_pts is not None
    ]
    print(f"  -> {len(all_trades)} total;  {len(july_trades)} closed July trades")

    if not july_trades:
        print("No July trades."); return

    # ── Option P&L via Black-Scholes ──────────────────────────────────────────
    print(f"\nPricing options (Black-Scholes, realised-vol={HIST_VOL_WINDOW}d)...")
    print(f"  Note: Upstox feeder token lacks option/contract API scope; all July")
    print(f"  NIFTY weekly contracts (Tue Jul 7/14/21/28) are expired. B-S gives")
    print(f"  a theoretically sound approximation using historical realised vol.\n")

    opt_trades: List[OptionTrade] = []
    for t in july_trades:
        entry_dt = t.entry_ts
        exit_dt  = t.exit_ts
        atm      = nifty_atm(t.entry)
        expiry   = nearest_nifty_expiry(entry_dt.date())

        if t.direction == "LONG":
            strike   = atm - ITM_STRIKES * STRIKE_STEP   # 2 ITM CE
            opt_type = "CE"
        else:
            strike   = atm + ITM_STRIKES * STRIKE_STEP   # 2 ITM PE
            opt_type = "PE"

        sigma = hist_vol(d1_bars, entry_dt.date())

        # Entry premium (spot = trade entry price)
        T_in  = T_years(entry_dt, expiry)
        entry_prem = black_scholes(t.entry, strike, T_in, sigma, opt_type=opt_type)

        # Exit premium (spot = trade exit price at exit timestamp)
        exit_spot = t.exit_price or t.entry
        T_out = T_years(exit_dt, expiry) if exit_dt else 0.0
        exit_prem = black_scholes(exit_spot, strike, T_out, sigma, opt_type=opt_type)

        ot = OptionTrade(
            spot_trade=t, opt_type=opt_type, strike=strike, expiry=expiry,
            sigma=sigma, opt_entry=entry_prem, opt_exit=exit_prem,
        )
        opt_trades.append(ot)

    # ── Summary ───────────────────────────────────────────────────────────────
    sep = "=" * 100
    print(f"\n{sep}")
    print(f"  OPTION BACKTEST — July 2026  (C2+TWEAK, 2-ITM NIFTY weekly, 1×75 lot, B-S pricing)")
    print(sep)
    print(f"  Trades: {len(opt_trades)}     Lot size: {LOT_SIZE}    IV: realised {HIST_VOL_WINDOW}d")

    win_opt  = [ot for ot in opt_trades if ot.opt_pnl_rs > 0]
    loss_opt = [ot for ot in opt_trades if ot.opt_pnl_rs <= 0]
    gp  = sum(ot.opt_pnl_rs for ot in win_opt)
    gl  = abs(sum(ot.opt_pnl_rs for ot in loss_opt))
    pf  = gp / gl if gl else float("inf")

    total_opt_pnl  = sum(ot.opt_pnl_rs   for ot in opt_trades)
    total_spot_pnl = sum(ot.spot_pnl_rs  for ot in opt_trades)
    avg_sigma = sum(ot.sigma for ot in opt_trades) / len(opt_trades)

    print(f"\n  Black-Scholes summary:")
    print(f"    Mean realised IV used : {avg_sigma*100:.1f}%")
    print(f"    Option winners        : {len(win_opt)} / {len(opt_trades)}  ({100*len(win_opt)//len(opt_trades)}%)")
    print(f"    Profit Factor (opt)   : {pf:.2f}")
    print(f"    Net Option P&L        : Rs {total_opt_pnl:>10,.0f}")
    print(f"    Net Spot P&L          : Rs {total_spot_pnl:>10,.0f}  (same trades × 75 lots)")
    mult = total_opt_pnl / total_spot_pnl if total_spot_pnl else 0
    print(f"    Option / Spot ratio   : {mult:.2f}×  (leverage from ITM option)")

    # ── Trade table ───────────────────────────────────────────────────────────
    fmt_dt = lambda dt: dt.strftime("%m/%d %H:%M") if dt else ""
    print(f"\n  {'Entry':12}  {'Exit':12}  {'Dir':5}  {'Strike':8}  {'Exp':8}  "
          f"{'IV%':5}  {'OptIn':7}  {'OptOut':7}  "
          f"{'OptChg':>7}  {'Opt Rs':>8}  {'Spot Rs':>8}  {'Outcome':9}  {'Method'}")
    print("  " + "-" * 108)
    for ot in opt_trades:
        st = ot.spot_trade
        opt_chg = ot.opt_exit - ot.opt_entry
        print(
            f"  {fmt_dt(st.entry_ts):12}  {fmt_dt(st.exit_ts):12}  "
            f"{st.direction:5}  "
            f"{ot.strike:>5}{ot.opt_type}   "
            f"{ot.expiry.strftime('%d-%b'):8}  "
            f"{ot.sigma*100:5.1f}%  "
            f"{ot.opt_entry:7.1f}  "
            f"{ot.opt_exit:7.1f}  "
            f"{opt_chg:>+7.1f}  "
            f"{ot.opt_pnl_rs:>8.0f}  "
            f"{ot.spot_pnl_rs:>8.0f}  "
            f"{st.outcome:9}  "
            f"{st.method}"
        )

    # ── Expiry-week breakdown ─────────────────────────────────────────────────
    print(f"\n  By expiry week:")
    by_exp: dict = {}
    for ot in opt_trades:
        by_exp.setdefault(ot.expiry, []).append(ot)
    for exp in sorted(by_exp):
        grp = by_exp[exp]
        net = sum(ot.opt_pnl_rs for ot in grp)
        wins = sum(1 for ot in grp if ot.opt_pnl_rs > 0)
        print(f"    {exp}  {len(grp):2} trades  {wins}/{len(grp)} win  Rs {net:>8,.0f}")

    # ── Save CSV ──────────────────────────────────────────────────────────────
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    csv_path = os.path.join(_RESULTS_DIR, "option_july.csv")
    fields = ["Direction", "Entry_TS", "Exit_TS", "Spot_Entry", "Spot_Exit",
              "ATM", "Strike", "Opt_Type", "Expiry", "IV_pct",
              "BS_Entry", "BS_Exit", "Opt_PnL_pts", "Opt_PnL_Rs",
              "Spot_PnL_pts", "Spot_PnL_Rs", "Method", "Outcome"]

    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for ot in opt_trades:
            st = ot.spot_trade
            fmt = lambda dt: dt.strftime("%Y-%m-%d %H:%M") if dt else ""
            w.writerow({
                "Direction":    st.direction,
                "Entry_TS":     fmt(st.entry_ts),
                "Exit_TS":      fmt(st.exit_ts),
                "Spot_Entry":   f"{st.entry:.2f}",
                "Spot_Exit":    f"{st.exit_price:.2f}" if st.exit_price else "",
                "ATM":          str(nifty_atm(st.entry)),
                "Strike":       str(ot.strike),
                "Opt_Type":     ot.opt_type,
                "Expiry":       str(ot.expiry),
                "IV_pct":       f"{ot.sigma*100:.1f}",
                "BS_Entry":     f"{ot.opt_entry:.2f}",
                "BS_Exit":      f"{ot.opt_exit:.2f}",
                "Opt_PnL_pts":  f"{ot.opt_pnl_pts:.2f}",
                "Opt_PnL_Rs":   f"{ot.opt_pnl_rs:.0f}",
                "Spot_PnL_pts": f"{st.pnl_pts:.2f}" if st.pnl_pts else "",
                "Spot_PnL_Rs":  f"{ot.spot_pnl_rs:.0f}",
                "Method":       st.method,
                "Outcome":      st.outcome,
            })
    print(f"\n  CSV -> {csv_path}")
    print(sep)


if __name__ == "__main__":
    asyncio.run(main())
