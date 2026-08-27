"""
scripts/nifty_1500_sr_breakout_backtest.py — one-off backtest (2026-08-27, direct
user spec, real NIFTY spot + real option premium via Upstox).

Mechanic (verbatim from the user): "at 15:00 hours system will check the nifty
price get the ATM price. Get the call and put ATM check the rates. After that.
We have an inbuilt logic of support and resistance. Check one minute candle
closes above R1 on both call and put, which ever side candle close happens on
r1 above that side will initiate a buy Trade and candle closes below S1 will
be our stop loss with trailing stop loss as S1 itself, the trade with close at
1535, same day."

Concretely, per trading day:
  1. Take the NIFTY spot 1-min candle at/after 15:00 IST; ATM = round(close/50)*50.
  2. Resolve NIFTY's active weekly expiry for that day (REGISTRY.get_active_expiry_strict
     -- returns None rather than silently substituting today's live contract when
     the true historical expiry has since rolled off Upstox's instrument master;
     that day is skipped, not faked).
  3. Fetch the WHOLE day's real 1-min premium candles for the ATM CE and the ATM
     PE. Both sides are fed, bar-by-bar in timestamp order, from market open
     (09:15) into the REAL, reused strategies/d1_trap_option/support_resistance.py
     SupportResistanceCalculator (one instance, two independent inst_keys "CE"/
     "PE" -- same one-calculator/two-logical-instrument pattern this module's own
     PositionalSRTracker already uses for LONG/SHORT zone pools). Feeding from
     market open (not from 15:00) is a deliberate choice so R1/S1 are already
     *established* structure by 15:00, not a state machine starting from
     scratch at the exact moment we start checking it -- an S&R engine with no
     prior candles has nothing to trail yet (see support_resistance.py's own
     INITIAL_TREND_ESTABLISHMENT phase).
  4. Only START checking the entry condition on bars closing >= 15:00. The
     FIRST side (CE or PE, in time order) whose 1-min bar CLOSES above its own
     currently-established R1 high fires a BUY on that side, at that bar's
     close (this backtest's own entry-price assumption -- the spec doesn't
     specify tick-level slippage, and every other backtest script in this repo
     enters at the triggering bar's own close/high, e.g. SRPingPongTracker's
     R1-breach entry).
  5. Once in a trade, only that side is tracked (the spec says "a buy trade",
     singular -- one trade per day). SL = the CURRENT established S1 low,
     RE-READ after every subsequent bar close, so it moves as the live S&R
     engine's own S1 promotes upward -- this literally implements "trailing
     stop loss as S1 itself" (no separate ratchet-only clamp is added on top;
     if the S&R engine's own S1 print were ever to have moved down, the SL
     would follow it down too, since the user asked for the SL to BE S1, not a
     one-way trail of it).
  6. A bar closing below the current S1 exits at that close (reason
     "sl_s1@<level>"). Otherwise the trade is force-closed at the first bar
     timestamped >= 15:35 IST, same day (reason "eod_1535").

Known, honestly-flagged limitations (same category as this repo's other
"cannot be backtested"/"structural limitation" callouts):
  - InstrumentRegistry can only resolve instrument_keys for expiries still
    present in Upstox's CURRENT live instrument-master JSON -- there is no way
    to reconstruct instrument_keys for contracts that have since expired off
    it. This backtest can therefore only run over recent trading days whose
    NIFTY weekly was still live at the time this script runs, not an arbitrary
    historical range. Days that fail to resolve are skipped and printed, not
    silently dropped.
  - SL/target are option-PREMIUM levels here (unlike Liquidity Sweep/Liquidity
    Trap's deliberate spot-based SL/target) -- this is what the user's own
    spec describes (S&R computed directly on "the call and put" rates, not on
    spot), so no separate translation layer is needed or added.
  - No commission/slippage model -- pnl is raw premium-point difference,
    reported in both points and rupees (NIFTY lot_size=75) for readability
    only; no live/paper wiring, this is backtest-only exactly like every
    other scripts/*_backtest.py in this repo.

Usage:
    python scripts/nifty_1500_sr_breakout_backtest.py <upstox_token> [--days N]
    (N = number of NIFTY trading days to look back over; default 10)
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dtime
from typing import Dict, List, Optional
from urllib.parse import quote as _q

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m, _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
SPOT_KEY = "NSE_INDEX|Nifty 50"
STRIKE_STEP = 50
LOT_SIZE = 75
ENTRY_CHECK_START = dtime(15, 0)
FORCE_EXIT_TIME = dtime(15, 35)


@dataclass
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass
class Trade:
    day: date
    side: str
    strike: int
    entry_ts: datetime
    entry_price: float
    exit_ts: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""

    @property
    def pnl_pts(self) -> float:
        return (self.exit_price - self.entry_price) if self.exit_price is not None else 0.0


def _rows_to_bars(rows: List[dict]) -> List[Bar]:
    out = []
    for r in rows:
        ts = datetime.fromisoformat(r["ts"])
        out.append(Bar(ts=ts, open=r["open"], high=r["high"], low=r["low"], close=r["close"]))
    out.sort(key=lambda b: b.ts)
    return out


def by_day(bars: List[Bar]) -> Dict[date, List[Bar]]:
    days: Dict[date, List[Bar]] = {}
    for b in bars:
        days.setdefault(b.ts.date(), []).append(b)
    return days


def fetch_option_day(strike: int, side: str, expiry, day: date, token: str) -> List[Bar]:
    key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
    if not key:
        return []
    url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/"
           f"{day.isoformat()}/{day.isoformat()}")
    rows = _parse_candles(_http_get_json(url, token))
    return _rows_to_bars(rows)


def run_day(day: date, strike: int, ce_bars: List[Bar], pe_bars: List[Bar]) -> Optional[Trade]:
    """Feed both sides into one SupportResistanceCalculator (2 logical
    inst_keys), from market open, then look for the first >=15:00 bar close
    that breaches its own established R1. See module docstring for the full
    mechanic."""
    calc = SupportResistanceCalculator()
    tagged = [("CE", b) for b in ce_bars] + [("PE", b) for b in pe_bars]
    tagged.sort(key=lambda t: t[1].ts)

    trade: Optional[Trade] = None
    for side, bar in tagged:
        candle = {"timestamp": bar.ts, "high": bar.high, "low": bar.low, "duration": 1}
        calc.process_straddle_candle(side, candle, silent=True)
        state = calc.get_calculated_sr_state(side)

        if trade is None:
            if bar.ts.time() < ENTRY_CHECK_START:
                continue
            if state["r1_established"]:
                r1_high = state["sr_levels"]["R1"]["high"]
                if bar.close > r1_high:
                    trade = Trade(day=day, side=side, strike=strike,
                                  entry_ts=bar.ts, entry_price=bar.close)
            continue

        if side != trade.side:
            continue
        if bar.ts.time() >= FORCE_EXIT_TIME:
            trade.exit_ts, trade.exit_price, trade.exit_reason = bar.ts, bar.close, "eod_1535"
            break
        if state["s1_established"]:
            s1_low = state["sr_levels"]["S1"]["low"]
            if bar.close < s1_low:
                trade.exit_ts, trade.exit_price = bar.ts, bar.close
                trade.exit_reason = f"sl_s1@{s1_low:.2f}"
                break

    if trade is not None and trade.exit_ts is None:
        last_bar = (ce_bars if trade.side == "CE" else pe_bars)[-1]
        trade.exit_ts, trade.exit_price, trade.exit_reason = last_bar.ts, last_bar.close, "data_end"
    return trade


def report(trades: List[Trade]) -> None:
    if not trades:
        print("\n=== RESULTS ===  no trades fired over the tested window.")
        return
    n = len(trades)
    wins = [t for t in trades if t.pnl_pts > 0]
    losses = [t for t in trades if t.pnl_pts <= 0]
    gross_win = sum(t.pnl_pts for t in wins)
    gross_loss = -sum(t.pnl_pts for t in losses)
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    net_pts = sum(t.pnl_pts for t in trades)
    print(f"\n=== RESULTS ===  n={n}  win%={100.0 * len(wins) / n:.1f}  PF={pf:.2f}  "
          f"net={net_pts:+.2f} pts (₹{net_pts * LOT_SIZE:+.0f} @ lot={LOT_SIZE})")


async def main() -> None:
    if not TOKEN:
        print("Usage: python scripts/nifty_1500_sr_breakout_backtest.py <upstox_token> [--days N]")
        return
    days_back = 10
    if "--days" in sys.argv:
        days_back = int(sys.argv[sys.argv.index("--days") + 1])

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days_back * 2 + 5)  # buffer for weekends/holidays

    print(f"Fetching NIFTY spot 1-min candles {start} .. {end} ...")
    spot_bars = _rows_to_bars(await fetch_upstox_range_1m(SPOT_KEY, TOKEN, start, end))
    spot_by_day = by_day(spot_bars)
    if not spot_by_day:
        print("No spot data returned -- check token / date range.")
        return

    REGISTRY.load_sync("NIFTY", TOKEN)

    trading_days = sorted(spot_by_day.keys())[-days_back:]
    trades: List[Trade] = []
    for day in trading_days:
        day_spot = spot_by_day[day]
        bar_1500 = next((b for b in day_spot if b.ts.time() >= ENTRY_CHECK_START), None)
        if bar_1500 is None:
            print(f"{day}: no 15:00 spot bar -- skip")
            continue
        atm = round(bar_1500.close / STRIKE_STEP) * STRIKE_STEP

        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=day)
        if expiry is None:
            print(f"{day}: spot={bar_1500.close:.1f} ATM={atm} -- no active expiry resolvable "
                  f"(contract likely rolled off Upstox's live instrument master) -- skip")
            continue

        ce_bars = await asyncio.to_thread(fetch_option_day, atm, "CE", expiry, day, TOKEN)
        pe_bars = await asyncio.to_thread(fetch_option_day, atm, "PE", expiry, day, TOKEN)
        if not ce_bars or not pe_bars:
            print(f"{day}: ATM={atm} expiry={expiry} -- missing CE/PE premium data -- skip")
            continue

        trade = run_day(day, atm, ce_bars, pe_bars)
        if trade is None:
            print(f"{day}: ATM={atm} expiry={expiry} -- no R1 breakout entry")
            continue
        trades.append(trade)
        print(f"{day}: ATM={atm} expiry={expiry} {trade.side}{trade.strike} "
              f"entry {trade.entry_ts.strftime('%H:%M')}@{trade.entry_price:.2f} -> "
              f"exit {trade.exit_ts.strftime('%H:%M')}@{trade.exit_price:.2f} "
              f"({trade.exit_reason}) pnl={trade.pnl_pts:+.2f}pts")

    report(trades)


if __name__ == "__main__":
    asyncio.run(main())
