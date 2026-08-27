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
  3. Fetch the ATM CE/PE 1-min premium candles for that day, then keep ONLY
     the [15:00, 15:35] window (_window_bars() -- see the EIGHTH correction
     below, which SUPERSEDES this step's original "feed from market open"
     design). Both sides are fed, bar-by-bar in timestamp order, into the
     REAL, reused strategies/d1_trap_option/support_resistance.py
     SupportResistanceCalculator (one instance, two independent inst_keys
     "CE"/"PE" -- same one-calculator/two-logical-instrument pattern this
     module's own PositionalSRTracker already uses for LONG/SHORT zone
     pools). The 15:00 bar is the calculator's genuine very first candle
     (Phase 0 init) -- no pre-15:00 history is used at all.
  4. Only START checking the entry condition on bars closing in [15:00, 15:35].
     R1/S1 are read as the CALCULATOR'S OWN LEVELS AS OF BEFORE the bar being
     checked is fed in (never the level that bar itself just extended -- see
     the 2026-08-27 correction below). The FIRST side (CE or PE, in time
     order) whose 1-min bar CLOSES above that pre-bar R1 fires a BUY on that
     side, at that bar's close.
  5. Once in a trade, only that side is tracked (the spec says "a buy trade",
     singular -- one trade per day). SL = the pre-bar S1 low, re-read fresh
     every subsequent bar, so it moves as the live S&R engine's own S1
     evolves -- this implements "trailing stop loss as S1 itself".
  6. A bar closing below the pre-bar S1 exits at that close (reason
     "sl_s1@<level>"). Otherwise the trade is force-closed at the first bar
     timestamped >= 15:35 IST, same day (reason "eod_1535").

2026-08-27 CORRECTION (real user-caught bug, confirmed against an annotated
real NIFTY 24150 PE chart showing a plain base-range breakout at R1=70.60
with S1=65.00 that this script had reported as "no entry"): two bugs, now
fixed.
  (a) Look-ahead ordering bug -- the original code read R1/S1 via
      get_calculated_sr_state() AFTER already feeding the current bar into
      the calculator, so a bar that itself just pushed R1 to a new high was
      being compared against ITS OWN just-updated R1 (self-referential --
      close <= high always, so this could never legitimately fire off that
      bar, and left R1 as printed massively lagging what the real chart
      showed at the time). Fixed to snapshot R1/S1 BEFORE feeding each bar
      (matches the existing "st_before"/"phase_before" pattern already used
      by strategies/d1_trap_option/support_resistance.py's own
      SRPingPongTracker for exactly this reason), then feed the bar, so the
      comparison is always against the level that existed BEFORE this bar
      closed -- a real, non-look-ahead breakout check.
  (b) Over-strict "established" gate -- the original code additionally
      required is_established=True (the ping-pong state machine's own
      pullback-confirmation flag, which only flips true once a LATER bar
      prints a lower-high+lower-low candle after the peak). The user's own
      chart shows a plain base/consolidation R1 (set from the pre-breakout
      sideways range, no multi-candle pullback confirmation needed) getting
      breached and closed above immediately -- exactly Phase 0's/R1_TRACKING's
      raw, continuously-updated R1['high'] field (which the calculator
      already ratchets to every new high seen, independent of is_established;
      is_established is a separate confirmation flag the calculator uses for
      its own internal phase transitions, not a precondition this backtest's
      entry rule needs). Dropped the is_established gate entirely -- entry
      now fires on ANY pre-bar R1 breach, established or not, matching the
      user's own simpler mental model and the real chart evidence.

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
  - 2026-08-27 fix (real user-caught bug: today's ATM printed as 24300 when
    the real 15:00 spot gave 24200): TODAY's data (both spot and CE/PE
    premium) is fetched via Upstox's separate INTRADAY endpoint
    (fetch_upstox_intraday_1m), never the dated historical-candle range used
    for every prior day -- the dated endpoint does not reliably serve the
    still-in-progress trading day. Every log line now also prints the exact
    15:00 spot bar (timestamp + close) that produced each day's ATM, so a
    wrong ATM is auditable directly from the script's own output.

2026-08-27 THIRD FIX -- both sides' diagnostics (R1/S1/bars_checked/trace)
are now computed FULLY INDEPENDENTLY for the whole day (see _scan_side's own
docstring). The prior single merged-loop stopped updating a side's own
diagnostics the instant the OTHER side's trade fired first (a real,
user-caught bug: CE and PE both had a bar at 15:00, CE happened to be
processed first, so PE printed "not yet initialized" despite having a real
15:00 bar of its own the whole time). Each side is now scanned start-to-
finish on its own; only afterward is the globally-earliest confirmed signal
(across both sides) picked as the actual trade, and that side alone is
re-walked to simulate the SL/exit going forward.

2026-08-27 SECOND CORRECTION (direct user spec refinement, PARTIALLY
SUPERSEDED by the SIXTH correction below -- kept for history) -- entry/exit
are a TWO-CANDLE confirmation, not a single close-beyond-level check: a bar
closing beyond R1/S1 becomes a "signal" (its own high/low recorded as the
trigger level); a later bar breaching that level fills it. Originally this
only checked the SINGLE very-next bar (see the SIXTH correction for why that
was wrong -- it's a standing order, not next-bar-only).

2026-08-27 FOURTH CORRECTION -- SUPERSEDES the ENTRY half of the SECOND
correction above (the EXIT/SL half is unchanged). The ENTRY signal is now
armed by the strict ping-pong "R1 is breached" event -- a phase transition
from S2_TRACKING/R2_TRACKING back into R1_TRACKING, the exact condition
support_resistance.py's own already-validated SRPingPongTracker uses for its
entry -- NOT a plain "close above R1" check, and explicitly NOT gated on
is_established (is_established legitimately reads False right after this
exact promotion; that is normal internal state-machine bookkeeping, not
evidence the breach didn't happen -- this was a real bug the user caught:
"system see that r1 is not established so trade did not happen"). The order
is placed "on the high" -- the breaching bar's own high (which the state
machine's own promotion rule makes identical to the new R1 immediately
after) -- confirmed by the user's own worked example (PE breach bar's own
high prints as R1 on the very next row; the next bar's own high exceeding it
is the confirmation). Once in a trade, R1/R2 are no longer watched at all --
only S1/S2 for the trailing SL, per the user's own explicit "when breached
we are not going to check for R1 and R2 instead only check for S1 and S2".

2026-08-27 FIFTH CORRECTION -- no longer one trade per day. If a trade's
exit is an SL (not the 15:35 EOD close or running out of data), scanning
resumes on BOTH sides again "from that time onwards" (user's own words,
inclusive of the exit's own minute -- see the seventh fix below for why
inclusive matters) for the next-earliest confirmed signal, and that trade is
taken too -- repeating until an EOD/data_end close, or no further candidate
signal exists before 15:35. run_day() therefore returns a LIST of Trade
objects per day (possibly empty, one, or several), not a single
Optional[Trade].

2026-08-27 SEVENTH FIX -- the re-entry cursor above used a STRICT `>` when
filtering candidates after an SL exit, so a genuinely valid confirmation on
the OTHER side landing in the exact same minute as the exit (e.g. CE stops
out at 15:06 while PE's own confirmation also lands at 15:06) was silently
skipped, and the scan wrongly jumped to that side's own next LATER signal
instead of switching to the side that actually confirmed next (a real,
user-caught bug: two CE trades fired the same day while PE's clearly-earlier
15:06 confirmation was ignored). Changed to `>=` -- inclusive of the exit
minute itself.

2026-08-27 SIXTH CORRECTION, direct user clarification -- both the entry and
SL "signal" are STANDING orders, not a single-shot next-bar-only check:
"any candle that breached this value, entry happened, not next candle."
Once armed (by an R1-breach phase event for entry, or a close-below-S1 for
SL), the order stays live across as many subsequent bars as it takes -- the
FIRST later bar (any bar, not just the very next one) whose high/low
breaches it fills the order. It is only cleared on an actual fill, never
merely for going unconfirmed on a given bar; a fresh breach event still
replaces an unfilled order with the newer level.

Usage:
    python scripts/nifty_1500_sr_breakout_backtest.py <upstox_token> [--days N] [--trace-day YYYY-MM-DD]
    (N = number of NIFTY trading days to look back over; default 10. Every
    1-min CE/PE bar in the 15:00-15:35 window -- high/low/close/R1/S1, tagged
    with "R1 BREACHED" / "CONFIRMED ENTRY" -- is printed for EVERY day
    by default, per the user's own 2026-08-27 request to verify the S&R
    engine's behavior candle-by-candle rather than take a summary on faith.
    --trace-day restricts that verbose dump to just one date, for less
    output across a longer --days run.)
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dtime
from typing import Dict, List, Optional
from urllib.parse import quote as _q

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m, fetch_upstox_intraday_1m, _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.d1_trap_option.support_resistance import SupportResistanceCalculator

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
SPOT_KEY = "NSE_INDEX|Nifty 50"
STRIKE_STEP = 50
LOT_SIZE = 75
ENTRY_CHECK_START = dtime(15, 0)
FORCE_EXIT_TIME = dtime(15, 35)
# 2026-08-27 NINTH CORRECTION, direct user spec change: trade the strike
# whose 15:00 premium is closest to Rs100 -- NOT plain ATM. CE and PE are
# searched independently (their premium curves differ), so the traded CE
# strike and PE strike can legitimately differ from each other and from ATM.
TARGET_PREMIUM_RS = 100.0
STRIKE_SEARCH_STEPS = 10   # scans ATM +/- this many STRIKE_STEP increments


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


async def fetch_option_day(strike: int, side: str, expiry, day: date, token: str) -> List[Bar]:
    """2026-08-27 fix (real user-caught bug, e.g. today's ATM computed as 24300
    when the real 15:00 spot was 24200): Upstox's DATED historical-candle
    endpoint (/v2/historical-candle/{key}/1minute/{from}/{to}) does not
    reliably serve the still-in-progress trading day -- per
    data_layer/historical_candles.py's own module docstring, TODAY's bars
    require the separate intraday endpoint (fetch_upstox_intraday_1m). Asking
    the dated endpoint for `day == date.today()` silently returned wrong/empty
    data, which fed a wrong or stale 15:00 bar into the ATM calc. Every day
    strictly before today still uses the dated endpoint (that data is
    finalized and the dated endpoint is the correct/only source for it)."""
    key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, side)
    if not key:
        return []
    if day == date.today():
        rows = await fetch_upstox_intraday_1m(key, token)
    else:
        url = (f"https://api.upstox.com/v2/historical-candle/{_q(key, safe='')}/1minute/"
               f"{day.isoformat()}/{day.isoformat()}")
        rows = _parse_candles(_http_get_json(url, token))
    return _rows_to_bars(rows)


async def find_closest_premium_strike(atm: int, side: str, expiry, day: date, token: str,
                                       target: float = TARGET_PREMIUM_RS,
                                       steps: int = STRIKE_SEARCH_STEPS) -> tuple[Optional[int], List[Bar]]:
    """2026-08-27, direct user spec change: trade the strike whose 15:00
    premium is closest to Rs100, not plain ATM. Scans candidate strikes
    ATM-steps*STRIKE_STEP .. ATM+steps*STRIKE_STEP (fetched in parallel via
    asyncio.gather -- this multiplies the REST call count considerably vs the
    old fixed-ATM design, since every candidate strike needs its own
    historical-candle fetch; Upstox has no bulk historical option-chain
    endpoint), and returns whichever candidate's 15:00 close is nearest
    `target`, along with that strike's already-fetched full-window bars (no
    second fetch needed for the winner). Returns (None, []) if nothing valid
    was found (e.g. every candidate strike failed to resolve/fetch)."""
    candidates = [atm + k * STRIKE_STEP for k in range(-steps, steps + 1)]

    async def _probe(strike: int) -> tuple[int, List[Bar], Optional[float]]:
        bars = await fetch_option_day(strike, side, expiry, day, token)
        bar_1500 = next((b for b in bars if b.ts.time() >= ENTRY_CHECK_START), None)
        return strike, bars, (bar_1500.close if bar_1500 else None)

    results = await asyncio.gather(*[_probe(s) for s in candidates])
    valid = [(s, bars, p) for s, bars, p in results if p is not None and bars]
    if not valid:
        return None, []
    best_strike, best_bars, _ = min(valid, key=lambda r: abs(r[2] - target))
    return best_strike, best_bars


def _new_diag() -> dict:
    return {"r1": None, "s1": None, "max_close": None, "min_close": None, "bars_checked": 0, "trace": []}


def _window_bars(bars: List[Bar]) -> List[Bar]:
    """2026-08-27 EIGHTH CORRECTION, direct user clarification: the S&R
    calculator must start COMPLETELY FRESH at 15:00 -- the 15:00 bar IS the
    very first candle (Phase 0 init: R1=that bar's high, S1=that bar's low),
    not a state machine that's already cycled through several ping-pong
    phases from market-open history. Feeding market-open-to-15:00 history in
    first (the ORIGINAL design in this script, since superseded) let leftover
    pre-15:00 structure make a bar AT 15:00 already read as mid-ping-pong
    (e.g. phase_before=R2_TRACKING on the very first window bar) -- which the
    user confirmed is impossible for a genuine R2-breaches-R1 event to have
    happened for real that early ("that did not happen at 15:01 as it is 2nd
    candle"). Only bars within [15:00, 15:35] are ever fed to the calculator
    now; nothing before 15:00 is used for phase/R1/S1 purposes at all."""
    return [b for b in bars if ENTRY_CHECK_START <= b.ts.time() <= FORCE_EXIT_TIME]


def _scan_side(bars: List[Bar]) -> tuple[dict, List[dict]]:
    """Run ONE side's whole 15:00-15:35 window, independently of the other
    side and of whether a trade ends up being taken at all. This
    independence is the 2026-08-27 fix for a real bug: the old single
    merged-loop stopped updating a side's diagnostics (bars_checked/R1/S1/
    trace) the instant the OTHER side's confirmation fired first (e.g. CE
    and PE both had a 15:00 bar; CE happened to be processed first in the
    merged/sorted loop, so PE's own R1/S1/trace was silently never recorded
    at all, printing as "not yet initialized" even though PE had real data
    the whole time). `bars` must already be window-filtered via
    _window_bars() -- the 15:00 bar is treated as the calculator's very
    first candle (see _window_bars' own docstring). Returns (diag,
    confirmed_events) where confirmed_events is every signal-then-later-bar-
    breach event that fired during the window, in chronological order (there
    can be more than one per side across the window; run_day below acts on
    the globally-earliest one not yet consumed by a prior trade)."""
    calc = SupportResistanceCalculator()
    d = _new_diag()
    pending_signal: Optional[dict] = None
    confirmed: List[dict] = []
    for bar in bars:
        # Snapshot R1/S1/phase as they stood BEFORE this bar -- never compare
        # a bar against a level that bar itself just extended (2026-08-27
        # fix #1).
        st_before = calc.get_calculated_sr_state("OPT")
        levels_before = st_before.get("sr_levels") or {}
        r1_before = (levels_before.get("R1") or {}).get("high")
        s1_before = (levels_before.get("S1") or {}).get("low")
        phase_before = st_before.get("current_phase", "UNKNOWN")

        candle = {"timestamp": bar.ts, "high": bar.high, "low": bar.low, "duration": 1}
        calc.process_straddle_candle("OPT", candle, silent=True)
        phase_after = calc.get_calculated_sr_state("OPT").get("current_phase", "UNKNOWN")
        t = bar.ts.time()
        if not (ENTRY_CHECK_START <= t <= FORCE_EXIT_TIME):
            continue

        d["bars_checked"] += 1
        if r1_before is not None:
            d["r1"] = r1_before
        if s1_before is not None:
            d["s1"] = s1_before
        d["max_close"] = bar.close if d["max_close"] is None else max(d["max_close"], bar.close)
        d["min_close"] = bar.close if d["min_close"] is None else min(d["min_close"], bar.close)

        # 2026-08-27 FOURTH CORRECTION, direct user clarification: the ENTRY
        # signal is armed by the strict ping-pong "R1 is breached" event
        # itself -- a phase transition from S2_TRACKING/R2_TRACKING back into
        # R1_TRACKING (the same condition strategies/d1_trap_option/
        # support_resistance.py's own already-validated SRPingPongTracker
        # uses for its entry) -- NOT by a plain "close above R1" check (that
        # was the prior, now-superseded rule), and NOT gated on is_established
        # (is_established legitimately reads False right after this exact
        # promotion -- that is normal internal bookkeeping, not evidence the
        # breach didn't happen; user's own words: "system see that r1 is not
        # established so trade did not happen" was the bug). The order is
        # placed "on the high" -- the BREACHING bar's own high (which, by the
        # state machine's own promotion rule, becomes the new R1 immediately
        # after this bar). Once in a trade, R1/R2 are no longer watched at
        # all -- only S1/S2 (see _simulate_exit) for the trailing SL, per the
        # user's own explicit "when breached we are not going to check for
        # R1 and R2 instead only check for S1 and S2".
        #
        # 2026-08-27 SIXTH CORRECTION, direct user clarification -- this is a
        # STANDING order, not a single-shot next-bar-only check: "any candle
        # that breached this value, entry happened, not next candle." Once
        # armed, the order stays live across as many subsequent bars as it
        # takes; the FIRST later bar (any bar, not just the very next one)
        # whose HIGH exceeds it fills the order. It is only cleared on an
        # actual fill, never merely for going unconfirmed on a given bar. A
        # fresh r1_breach_event still replaces it with the newer level.
        r1_breach_event = phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == "R1_TRACKING"
        s1_breach_event = phase_before in ("S2_TRACKING", "R2_TRACKING") and phase_after == "S1_TRACKING"

        confirmed_now = pending_signal is not None and bar.high > pending_signal["level"]
        if confirmed_now:
            confirmed.append({"ts": bar.ts, "price": pending_signal["level"]})
            pending_signal = None   # order filled -- done
        if r1_breach_event:
            pending_signal = {"level": bar.high, "ts": bar.ts}   # arm (or replace) the standing order

        d["trace"].append({
            "ts": bar.ts, "high": bar.high, "low": bar.low, "close": bar.close,
            "r1": r1_before, "s1": s1_before, "confirmed": confirmed_now,
            "phase_before": phase_before, "phase_after": phase_after,
            "r1_breach_event": r1_breach_event, "s1_breach_event": s1_breach_event,
        })
    return d, confirmed


def _simulate_exit(bars: List[Bar], side: str, strike: int, day: date,
                    entry_ts: datetime, entry_price: float) -> Trade:
    """Replay ONE side's bars from scratch (cheap -- a plain state machine
    over one day) to rebuild correct S&R state up to entry_ts, then manage
    the SL mirror of the entry confirmation (signal bar closes below S1;
    only the very next bar's LOW-breach of that signal bar's own low exits)
    from entry_ts onward, or force-close at 15:35, or run out of data."""
    calc = SupportResistanceCalculator()
    trade = Trade(day=day, side=side, strike=strike, entry_ts=entry_ts, entry_price=entry_price)
    pending_sl_signal: Optional[dict] = None
    for bar in bars:
        levels_before = (calc.get_calculated_sr_state("OPT").get("sr_levels") or {})
        s1_before = (levels_before.get("S1") or {}).get("low")
        candle = {"timestamp": bar.ts, "high": bar.high, "low": bar.low, "duration": 1}
        calc.process_straddle_candle("OPT", candle, silent=True)

        if bar.ts <= entry_ts:
            continue   # still rebuilding pre-entry state, not yet managing the trade

        t = bar.ts.time()
        if t >= FORCE_EXIT_TIME:
            trade.exit_ts, trade.exit_price, trade.exit_reason = bar.ts, bar.close, "eod_1535"
            return trade
        # 2026-08-27 SIXTH CORRECTION (mirrors the entry-side fix in
        # _scan_side): a standing order, not single-shot next-bar-only --
        # stays live until an actual fill (any later bar's LOW breaching it),
        # never cleared merely for going unconfirmed on a given bar.
        if pending_sl_signal is not None and bar.low < pending_sl_signal["low"]:
            trade.exit_ts, trade.exit_price = bar.ts, pending_sl_signal["low"]
            trade.exit_reason = f"sl_s1_breach@{pending_sl_signal['low']:.2f}"
            return trade
        if s1_before is not None and bar.close < s1_before:
            pending_sl_signal = {"low": bar.low, "ts": bar.ts}   # arm (or replace) the standing order

    last_bar = bars[-1]
    trade.exit_ts, trade.exit_price, trade.exit_reason = last_bar.ts, last_bar.close, "data_end"
    return trade


def run_day(day: date, ce_strike: int, pe_strike: int, ce_bars: List[Bar],
            pe_bars: List[Bar]) -> tuple[List[Trade], dict]:
    """Scan CE and PE fully and independently (see _scan_side's own
    docstring for why that independence matters). Pick the globally-earliest
    confirmed entry across both sides (CE wins an exact-timestamp tie, since
    it's evaluated first below -- an arbitrary but deterministic tie-break;
    genuine same-minute CE/PE ties are rare and not otherwise specified by
    the user), simulate that side's SL/exit. ce_strike/pe_strike are
    separate (2026-08-27 NINTH correction: each side trades its own
    ~Rs100-premium strike, found independently via
    find_closest_premium_strike -- not necessarily the same as each other or
    as plain ATM).

    2026-08-27 FIFTH CORRECTION, direct user spec: this is no longer a
    single trade per day. If a trade's exit is an SL (not EOD/data_end), we
    resume watching BOTH sides again "from that time onwards" (user's own
    words) for the next-earliest confirmed signal strictly after this exit,
    and take that trade too -- repeating until an EOD/data_end close or no
    further candidates remain before 15:35."""
    ce_window = _window_bars(ce_bars)
    pe_window = _window_bars(pe_bars)
    ce_diag, ce_confirmed = _scan_side(ce_window)
    pe_diag, pe_confirmed = _scan_side(pe_window)
    diag = {"CE": ce_diag, "PE": pe_diag}

    all_candidates = sorted(
        [("CE", e) for e in ce_confirmed] + [("PE", e) for e in pe_confirmed],
        key=lambda c: c[1]["ts"])

    trades: List[Trade] = []
    cursor_ts: Optional[datetime] = None
    while True:
        # 2026-08-27, real user-caught bug: this used a STRICT `>` here, so a
        # different side's genuinely valid confirmed entry landing on the
        # EXACT SAME MINUTE as the just-closed trade's exit (e.g. CE stops
        # out at 15:06 while PE's own confirmation also lands at 15:06) was
        # silently skipped -- the scan jumped straight to that side's OWN
        # next later signal instead, wrongly re-entering the same side twice
        # instead of switching to the side that actually confirmed next.
        # `>=` includes same-minute candidates -- "from that time onwards"
        # (the user's own words) is inclusive of the exit minute itself.
        candidates = [c for c in all_candidates if cursor_ts is None or c[1]["ts"] >= cursor_ts]
        if not candidates:
            break
        winner_side, winner = candidates[0]
        bars = ce_window if winner_side == "CE" else pe_window
        winner_strike = ce_strike if winner_side == "CE" else pe_strike
        trade = _simulate_exit(bars, winner_side, winner_strike, day, winner["ts"], winner["price"])
        trades.append(trade)
        if trade.exit_reason in ("eod_1535", "data_end"):
            break
        cursor_ts = trade.exit_ts   # SL exit -- resume scanning from right after it
    return trades, diag


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
    days_back = 7   # 2026-08-27, direct user request: default to a 7-day run
    if "--days" in sys.argv:
        days_back = int(sys.argv[sys.argv.index("--days") + 1])
    trace_day: Optional[date] = None
    if "--trace-day" in sys.argv:
        trace_day = date.fromisoformat(sys.argv[sys.argv.index("--trace-day") + 1])

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days_back * 2 + 5)  # buffer for weekends/holidays

    print(f"Fetching NIFTY spot 1-min candles {start} .. {end} (historical) ...")
    spot_bars = _rows_to_bars(await fetch_upstox_range_1m(SPOT_KEY, TOKEN, start, end))
    spot_by_day = by_day(spot_bars)

    # 2026-08-27 fix (real user-caught bug): TODAY must come from the intraday
    # endpoint, never the dated historical-candle range above -- see
    # fetch_option_day's own docstring for why. Fetched separately and merged
    # in so today gets exactly the same "spot 15:00 bar -> ATM" treatment as
    # every past day, just sourced correctly.
    today = date.today()
    today_rows = await fetch_upstox_intraday_1m(SPOT_KEY, TOKEN)
    if today_rows:
        spot_by_day[today] = _rows_to_bars(today_rows)
        print(f"{today}: {len(spot_by_day[today])} intraday spot bars fetched "
              f"({spot_by_day[today][0].ts.strftime('%H:%M')} .. {spot_by_day[today][-1].ts.strftime('%H:%M')})")
    else:
        print(f"{today}: intraday spot fetch returned nothing (market closed / no token / holiday)")

    if not spot_by_day:
        print("No spot data returned -- check token / date range.")
        return

    REGISTRY.load_sync("NIFTY", TOKEN)

    past_days = sorted(d for d in spot_by_day if d < today)[-days_back:]
    trading_days = past_days + ([today] if today in spot_by_day else [])
    trades: List[Trade] = []
    for day in trading_days:
        day_spot = spot_by_day[day]
        bar_1500 = next((b for b in day_spot if b.ts.time() >= ENTRY_CHECK_START), None)
        if bar_1500 is None:
            print(f"{day}: no 15:00 spot bar -- skip")
            continue
        atm = round(bar_1500.close / STRIKE_STEP) * STRIKE_STEP
        print(f"{day}: 15:00 spot bar @ {bar_1500.ts.strftime('%H:%M:%S')} close={bar_1500.close:.2f} -> ATM={atm}")

        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=day)
        if expiry is None:
            print(f"{day}: spot={bar_1500.close:.1f} ATM={atm} -- no active expiry resolvable "
                  f"(contract likely rolled off Upstox's live instrument master) -- skip")
            continue

        # 2026-08-27 NINTH CORRECTION, direct user spec change: trade the
        # ~Rs100-premium strike (found independently per side), not plain
        # ATM. This means many more REST calls per day (up to 2*STRIKE_
        # SEARCH_STEPS+1 candidates per side) -- see find_closest_premium_
        # strike's own docstring.
        ce_strike, ce_bars = await find_closest_premium_strike(atm, "CE", expiry, day, TOKEN)
        pe_strike, pe_bars = await find_closest_premium_strike(atm, "PE", expiry, day, TOKEN)
        if ce_strike is None or pe_strike is None or not ce_bars or not pe_bars:
            print(f"{day}: ATM={atm} expiry={expiry} -- could not find a ~Rs{TARGET_PREMIUM_RS:.0f} "
                  f"CE/PE strike -- skip")
            continue
        print(f"{day}: selected CE{ce_strike} / PE{pe_strike} (closest to Rs{TARGET_PREMIUM_RS:.0f} "
              f"at 15:00, ATM was {atm})")

        day_trades, diag = run_day(day, ce_strike, pe_strike, ce_bars, pe_bars)
        for side in ("CE", "PE"):
            d = diag[side]
            side_strike = ce_strike if side == "CE" else pe_strike
            r1_str = f"{d['r1']:.2f}" if d["r1"] is not None else "n/a (not yet initialized)"
            s1_str = f"{d['s1']:.2f}" if d["s1"] is not None else "n/a (not yet initialized)"
            max_str = f"{d['max_close']:.2f}" if d["max_close"] is not None else "n/a"
            min_str = f"{d['min_close']:.2f}" if d["min_close"] is not None else "n/a"
            print(f"    {side}{side_strike}: R1={r1_str} S1={s1_str} "
                  f"window[15:00-15:35] close range=[{min_str}..{max_str}] "
                  f"bars_checked={d['bars_checked']}")
        # Full per-minute S&R trace for BOTH sides, every day, unless
        # --trace-day restricts it to one specific date (direct user
        # request: "provide each minute data and S&R data" to verify the
        # engine's behavior candle-by-candle, not just a final summary).
        if trace_day is None or day == trace_day:
            print(f"    --- minute-by-minute trace for {day} (compare against the real "
                  f"CE{ce_strike}/PE{pe_strike} chart) ---")
            merged_trace = sorted(
                ({**row, "side": s} for s in ("CE", "PE") for row in diag[s]["trace"]),
                key=lambda r: r["ts"])
            for row in merged_trace:
                r1_s = f"{row['r1']:.2f}" if row["r1"] is not None else "-"
                s1_s = f"{row['s1']:.2f}" if row["s1"] is not None else "-"
                tags = []
                if row["r1_breach_event"]:
                    tags.append("R1 BREACHED -- order ready @ %.2f (next candle must breach this)" % row["high"])
                if row["s1_breach_event"]:
                    tags.append("S1 BREACHED (phase %s->%s)" % (row["phase_before"], row["phase_after"]))
                if row["confirmed"]:
                    tags.append("CONFIRMED ENTRY")
                tag = "  <== " + " | ".join(tags) if tags else ""
                print(f"    {row['ts'].strftime('%H:%M')} {row['side']} "
                      f"H={row['high']:.2f} L={row['low']:.2f} C={row['close']:.2f} "
                      f"R1={r1_s} S1={s1_s} phase={row['phase_before']}->{row['phase_after']}{tag}")
        if not day_trades:
            print(f"{day}: ATM={atm} expiry={expiry} -- no R1/S1 breakout entry")
            continue
        trades.extend(day_trades)
        for trade in day_trades:
            print(f"{day}: ATM={atm} expiry={expiry} {trade.side}{trade.strike} "
                  f"entry {trade.entry_ts.strftime('%H:%M')}@{trade.entry_price:.2f} -> "
                  f"exit {trade.exit_ts.strftime('%H:%M')}@{trade.exit_price:.2f} "
                  f"({trade.exit_reason}) pnl={trade.pnl_pts:+.2f}pts"
                  + ("  [re-entry after SL]" if trade is not day_trades[0] else ""))

    report(trades)


if __name__ == "__main__":
    asyncio.run(main())
