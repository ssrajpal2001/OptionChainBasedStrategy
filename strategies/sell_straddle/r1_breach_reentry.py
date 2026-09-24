"""
strategies/sell_straddle/r1_breach_reentry.py -- R1-breach exit / immediate
re-entry mechanic for rolled-in legs (2026-09-22, direct user spec; Part 2
REDESIGNED 2026-09-24, direct user spec, several rounds of clarification).

Scope: ONLY legs opened via a single-side roll (leg.open_reason starts with
"single_side_roll") are ever watched by this mechanic -- never the original
"beginning"/"re_entry" legs. Real incident that prompted this: a rolled-in
leg with no protection kept bleeding alongside a never-rolled leg that also
had no protection; this gives the ROLLED-IN leg specifically an early,
structure-based exit instead of waiting on the generic exit ladder.

Mechanic, in the user's own step numbering (Steps 1-3 are the ordinary
SellStraddle beginning-entry + main-rollover flow, unrelated to this file):

STEP 3 -- TICK-BY-TICK: the INSTANT a leg is taken via a single-side roll, arm
a fresh SupportResistanceCalculator for that leg's own strike -- seeded (not
blank) from REAL intraday history (09:15-to-now, REST-fetched as 1-min bars,
aggregated into 5-min MARKET-ANCHORED bars -- 2026-09-23 correction, see
_R1S1_BAR_MINUTES's own comment) so R1 reflects the strike's genuine 5-min
structure since market open. 2026-09-24 CORRECTION, direct user instruction
("when new leg is taken immediately check for r1 breach irrespective that leg
is in profit or in loss"): arming is NO LONGER gated on the leg's running P&L
being negative -- a freshly rolled-in leg is watched from the very next tick
regardless of whether it happens to be sitting in profit or loss at that
moment. Once armed, R1 keeps advancing every completed 5-min bar
(_r1_feed_bar) and every live tick is checked against it (_level_breached) --
this runs continuously for as long as the leg stays open, on EVERY roll that
produces a new leg, every time.

STEP 4 -- R1 BREACH (checked every tick against the seeded/updated state) =
TRUE when EITHER: R1 is not yet established, OR the current phase is
literally R1_TRACKING (a fresh "R2 breaches R1" transition always lands the
phase there). The instant true, close that leg alone
(exit_reason="r1_breach_post_roll") -- the kept leg keeps running untouched.
Then, every _R1_CANDIDATE_RETRY_SECONDS, search BOTH sides (ITM and OTM) of
the just-closed strike in expanding _R1_REENTRY_GAP_PTS rings
(select_partner_for(anchor_strike=...) -- the SAME both-directions ring
search the ITM-roll-protection pool search already uses, NOT the main
rollover's single-fixed-direction-then-fallback search) for a strike whose
LTP is STRICTLY LESS than the just-closed leg's own LTP at the moment of
breach (inverted from the main rollover's "must be richer" rule -- see
_evaluate_roll_candidate's max_ltp_exclusive mode in selection.py) and that
still passes entry_rules_reentry. The instant one passes, re-enter it
IMMEDIATELY (entry_reason="r1_pair_reentry_post_breach") -- no further
waiting on any breach/trigger for the new leg itself.

STEP 5 -- GIVE UP: if _R1_GIVEUP_SECONDS of continuous retrying never
produces a single passing candidate, close the remaining kept leg
(reason="r1_reentry_giveup_no_pair") and let ordinary fresh BEGINNING entry
logic re-fire on the next cycle, same as any other flat/day-start state.

STEP 6 -- LTP FLOOR: if a candidate DOES pass the ring search but its LTP is
below the same ltp_target floor fresh BEGINNING/re-entry already enforces,
do NOT enter it -- close the remaining kept leg
(reason="r1_reentry_ltp_below_threshold") and shift to next week's expiry
(self._shift_to_next_week_expiry, the existing low-anchor-LTP safety net
from entries.py), then let fresh BEGINNING logic re-fire on the new expiry.

SR calculator instances (used only by STEP 3/4's own R1-breach detection on
the rolled-in leg, no longer by the re-entry search) are intentionally NOT
persisted across a restart (same choice already made for _post1500_calc) --
they safely re-seed from real REST history the next time this mechanic arms.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from config.global_config import IST

logger = logging.getLogger(__name__)

# Throttle for re-deriving/re-checking a waiting-for-S1-breach candidate --
# avoids hammering the partner-selection search every single tick.
_R1_CANDIDATE_RETRY_SECONDS = 15


def _level_breached(sr_state: dict, level_key: str, tracking_phase: str,
                     ltp: float = None) -> bool:
    """Direct user spec: <level> breach is true when it is not established,
    OR the calculator is currently in that level's own TRACKING phase (a
    genuine re-breach transition -- e.g. "R2 breaches R1" -- always lands
    the phase there, so that case is already covered by this check).

    2026-09-24 CORRECTION, direct user instruction ("IF LTP GOES ABOVE R1
    THAT MEANS BREACH. REST ALL BREACH ARE ALSO VALID"): the phase/
    established check above is entirely BAR-CLOSE-based -- the calculator
    only updates its phase/established flags when a completed 5-min candle
    is fed in (_r1_feed_bar), so a live tick that has already ticked past
    the level's own numeric value sits unreported until the current bucket
    finishes and closes, which can lag the real breach by several minutes.
    Direct correction: the live LTP crossing the level's number IS the
    breach, immediately, every tick -- not just when the calculator's own
    bar-close-driven state machine catches up. Added as an ADDITIONAL
    ("or") condition, not a replacement -- the existing phase/established
    check still independently covers bar-close-confirmed transitions (e.g.
    an R2->R1 promotion) that don't necessarily show up as a simple
    LTP > level comparison. `ltp=None` (the default) preserves the exact
    original bar-close-only behavior for any caller that doesn't pass it."""
    sr = sr_state.get("sr_levels", {}) or {}
    lvl = sr.get(level_key)
    established = bool(lvl.get("is_established")) if lvl else False
    phase_breach = (not established) or (sr_state.get("current_phase") == tracking_phase)
    if ltp is not None and lvl:
        if level_key == "R1":
            level_value = float(lvl.get("high", 0.0) or 0.0)
            if level_value > 0 and ltp > level_value:
                return True
        elif level_key == "S1":
            level_value = float(lvl.get("low", 0.0) or 0.0)
            if level_value > 0 and ltp < level_value:
                return True
    return phase_breach


class R1BreachReentryMixin:
    """Per-leg R1-breach exit + S1-breach re-entry, scoped to rolled-in legs."""

    def _r1_init_state(self) -> None:
        if not hasattr(self, "_r1_watch") or not isinstance(getattr(self, "_r1_watch", None), dict):
            # side -> {"calc": SupportResistanceCalculator, "inst_key": str,
            #          "bar_acc": {"minute":, "h":, "l":} | None, "strike": int}
            self._r1_watch = {}
        if not hasattr(self, "_r1_pending") or self._r1_pending is None:
            # None, or {"side":, "candidate_strike":, "calc":, "inst_key":,
            #           "bar_acc":, "last_check": datetime}
            self._r1_pending = None
        if not hasattr(self, "_r1_closing") or not isinstance(getattr(self, "_r1_closing", None), dict):
            # 2026-09-23 CRITICAL FIX, real live incident: side -> bool, "a close
            # for this side is currently in flight". _tick_loop and
            # _eod_backstop_loop are two independent asyncio tasks that can both
            # reach _check_exits() -> _check_r1_breach_and_reentry() for the same
            # position around the same real moment (same documented race already
            # fixed once for _post1500_closing -- see config.py's own comment on
            # that flag, "confirmed live: two ticks 138ms apart ... both fired a
            # real close -> 2 broker orders for the same leg"). This mechanic
            # copied post1500's _close_leg() call but not its per-leg concurrency
            # guard, so the same race reproduced here. Set True BEFORE the
            # `await self._close_leg(...)` (not after) so a concurrent re-entry
            # sees the leg as already being closed and skips.
            self._r1_closing = {"CE": False, "PE": False}

    # 2026-09-23, direct user spec (real live incident review, "PE side breached
    # R1 -- check using token that the 5 min R1 high was genuinely breached"):
    # this mechanic was built and originally documented as 1-min bars (see the
    # module's own opening docstring, pre-this-fix) -- the user's actual intent
    # was 5-min R1/S1, confirmed directly. Both the REST seed and the live tick
    # accumulator below now bucket into 5-min MARKET-ANCHORED bars (09:15, 09:20,
    # ... -- not midnight-aligned, same convention as OI-ORB's own
    # to_n_min_bars_market_anchored, chosen for the same reason: a midnight-
    # aligned first bucket would only hold a partial ~5min of real data before
    # market open, distorting the earliest read).
    _R1S1_BAR_MINUTES = 5

    # 2026-09-24, direct user spec: the post-R1-breach re-entry search uses a
    # 50pt ring gap (NOT the main rollover's 100pt _ROLLOVER_MIN_GAP_PTS) --
    # deliberately its own constant so a future change to the main rollover's
    # gap can never silently drag this one along with it.
    _R1_REENTRY_GAP_PTS = 50.0

    # 2026-09-24, direct user spec: after 60s of continuous retrying with no
    # passing candidate, give up -- close the remaining kept leg and reset to
    # fresh BEGINNING entry rather than leave a single naked leg running
    # indefinitely.
    _R1_GIVEUP_SECONDS = 60.0

    async def _seed_r1s1_calc(self, strike: int, side: str, inst_key: str):
        """REST-fetch today's 1-min history for (strike, side), aggregate into
        5-min market-anchored bars, and replay into a fresh
        SupportResistanceCalculator -- see this module's own docstring for why
        (real structure since 09:15, not a blank start)."""
        from strategies.core.support_resistance import SupportResistanceCalculator
        calc = SupportResistanceCalculator()
        try:
            if getattr(self, "_is_crypto", False):
                return calc
            from data_layer.historical_candles import fetch_upstox_intraday_1m
            from data_layer.instrument_registry import REGISTRY
            from data_layer.client_db import ClientDB
            from strategies.core.trap_zone_utils import Bar as _Bar
            from strategies.core.candle_indicators import to_n_min_bars_market_anchored
            import asyncio as _aio
            creds = await _aio.to_thread(ClientDB().get_feeder_creds_sync, "upstox")
            token = (creds or {}).get("access_token", "")
            if not token:
                return calc
            pos = self._position
            exp = pos.expiry_date if (pos and pos.expiry_date) else \
                REGISTRY.get_active_expiry(self._underlying, datetime.now(IST).date())
            ikey = REGISTRY.get_broker_symbol(self._underlying, exp, int(strike), side, "upstox")
            if not ikey:
                return calc
            bars_1m = await fetch_upstox_intraday_1m(ikey, token)
            _bars = [
                _Bar(ts=datetime.fromisoformat(b["ts"]), open=float(b.get("open", b["high"])),
                     high=float(b["high"]), low=float(b["low"]), close=float(b.get("close", b["high"])))
                for b in bars_1m
            ] if bars_1m else []
            _bars_5m = to_n_min_bars_market_anchored(_bars, self._R1S1_BAR_MINUTES) if _bars else []
            # 2026-09-24 CRITICAL FIX, real live incident (Gurmeet's NIFTY book):
            # to_n_min_bars_market_anchored has no concept of "still forming" --
            # if the REST fetch (arm time) lands mid-bucket, its own LAST bar is
            # only a PARTIAL bucket (e.g. arming at 09:43:45 mid-way through the
            # [09:40,09:45) window only has ~4 of 5 real minutes), not a genuinely
            # closed candle. Seeding it anyway sets the calculator's internal
            # `last_candle.timestamp` to that same bucket boundary. The live feed
            # (_r1_feed_bar) then starts its OWN fresh accumulator for that exact
            # same bucket from arm-time onward, and once IT later finalizes and
            # feeds the (genuinely complete) candle,
            # SupportResistanceCalculator.process_straddle_candle's own duplicate-
            # candle guard (`ts == last_candle['timestamp'] and duration <=
            # last_candle['duration']`) silently DROPS it -- real new price action
            # discarded, and every subsequent R1/S1 transition pushed one full
            # bucket late from then on. Confirmed live: R1 stayed frozen at its
            # seed-time value (189) through several real bucket closes that should
            # have moved it to 172.3, then 173.7 (a genuine breach) -- reproduced
            # exactly via a full _check_exits() integration test driving the real
            # seed + live-feed path together, not just each in isolation. Fix: the
            # seed must never include the currently-forming bucket -- only bars
            # already fully closed by wall-clock time. The live feed then owns
            # that bucket exclusively, starting its own accumulator from whatever
            # real data has already ticked in (matching how it already behaves for
            # every bucket after the first).
            _now_ist = datetime.now(IST)
            _bars_5m = [
                b for b in _bars_5m
                if (b.ts + timedelta(minutes=self._R1S1_BAR_MINUTES)) <= _now_ist
            ]
            # 2026-09-24, direct user spec ("you should have found same day if
            # available or prev day R1 in 5 min and then check for breach"),
            # widened after a real backtest replay showed the original
            # zero-bars-only fallback still left R1 unestablished with just 1
            # same-day bar (nowhere near enough for the state machine to move
            # out of INITIAL_TREND_ESTABLISHMENT): the PREVIOUS trading day's
            # real closed 5-min bars are now ALWAYS prepended before today's
            # own bars, not just as a last-resort fallback when today has
            # none at all. This gives the calculator genuine multi-day
            # continuity from the very first tick of the trading day (same
            # "prev-day seed + holiday step-back" discipline already
            # established elsewhere in this codebase for the pool engine's
            # own RSI/ROC warm-up -- fetch_upstox_1m already implements the
            # day-by-day holiday/empty step-back itself). A previous day's
            # own bars are all genuinely closed already -- no "still forming"
            # filter needed there (unlike today's own fetch, where the arm
            # moment can land mid-bucket).
            from data_layer.historical_candles import fetch_upstox_1m
            _prev_bars_1m = await fetch_upstox_1m(ikey, token)
            _prev_bars_5m = []
            if _prev_bars_1m:
                _prev_bars = [
                    _Bar(ts=datetime.fromisoformat(b["ts"]), open=float(b.get("open", b["high"])),
                         high=float(b["high"]), low=float(b["low"]), close=float(b.get("close", b["high"])))
                    for b in _prev_bars_1m
                ]
                _prev_bars_5m = to_n_min_bars_market_anchored(_prev_bars, self._R1S1_BAR_MINUTES)
            _bars_5m = _prev_bars_5m + _bars_5m
            bars_1m = _prev_bars_1m + bars_1m
            if not _bars_5m:
                return calc
            candles = [
                {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": self._R1S1_BAR_MINUTES}
                for b in _bars_5m
            ]
            calc.reset_and_process_sequence(inst_key, candles)
            _st = calc.get_calculated_sr_state(inst_key)
            self._clog.info(
                "SellStraddle[%s]: R1/S1 SEEDED %s%d from %d real 1-min bars "
                "(%d real %d-min bars: %d prev-day + %d today) -- phase=%s r1_established=%s s1_established=%s",
                self._underlying, side, int(strike), len(bars_1m), len(candles),
                self._R1S1_BAR_MINUTES, len(_prev_bars_5m), len(candles) - len(_prev_bars_5m),
                _st.get("current_phase"), _st.get("r1_established"), _st.get("s1_established"),
            )
        except Exception as exc:
            self._clog.warning("SellStraddle[%s]: R1/S1 seed failed for %s%d: %s",
                                self._underlying, side, int(strike), exc)
        return calc

    def _r1_bucket_start(self, now: datetime) -> datetime:
        """Current 5-min market-anchored bucket start for `now` (09:15, 09:20,
        ... boundaries) -- same anchoring as to_n_min_bars_market_anchored, kept
        as a live running-tick equivalent since we bucket incrementally here
        rather than resampling a whole stored series."""
        anchor_mins = 9 * 60 + 15
        mins = now.hour * 60 + now.minute
        bucket_idx = (mins - anchor_mins) // self._R1S1_BAR_MINUTES
        start_mins = anchor_mins + bucket_idx * self._R1S1_BAR_MINUTES
        return now.replace(hour=start_mins // 60, minute=start_mins % 60, second=0, microsecond=0)

    def _r1_feed_bar(self, entry: dict, ltp: float, now: datetime) -> None:
        """Feed a live tick into a 5-min (market-anchored) bar accumulator;
        process a completed candle into the tracked SupportResistanceCalculator
        on bar close."""
        if ltp <= 0:
            return
        bucket = self._r1_bucket_start(now)
        acc = entry.get("bar_acc")
        if acc is None:
            entry["bar_acc"] = {"minute": bucket, "h": ltp, "l": ltp}
        elif bucket != acc["minute"]:
            entry["calc"].process_straddle_candle(
                entry["inst_key"],
                {"timestamp": acc["minute"], "high": acc["h"], "low": acc["l"],
                 "duration": self._R1S1_BAR_MINUTES},
            )
            entry["bar_acc"] = {"minute": bucket, "h": ltp, "l": ltp}
        else:
            acc["h"] = max(acc["h"], ltp)
            acc["l"] = min(acc["l"], ltp)

    async def _check_r1_breach_and_reentry(self, now: datetime) -> None:
        self._r1_init_state()
        pos = self._position
        if not pos or pos.status != "open":
            return

        # ── Part 1: watch rolled-in legs for an R1 breach ──────────────
        for side in ("CE", "PE"):
            leg_closed_attr = f"{side.lower()}_leg_closed"
            if getattr(pos, leg_closed_attr):
                self._r1_watch.pop(side, None)
                continue
            leg = pos.ce_leg if side == "CE" else pos.pe_leg
            open_reason = str(getattr(leg, "open_reason", "") or "")
            if not open_reason.startswith("single_side_roll"):
                # Not a rolled-in leg (beginning/re_entry/etc.) -- never watched.
                self._r1_watch.pop(side, None)
                continue
            strike = int(leg.strike)
            watch = self._r1_watch.get(side)
            if watch is not None and watch.get("strike") != strike:
                # This side rolled again onto a different strike since we last
                # armed -- drop stale tracker, a fresh arm will pick up the new
                # leg immediately below (irrespective of its P&L).
                self._r1_watch.pop(side, None)
                watch = None

            ltp = float(getattr(leg, "ltp", 0.0) or 0.0)
            running_pnl = float(getattr(leg, "entry_price", 0.0) or 0.0) - ltp  # short leg

            if watch is None:
                # 2026-09-24 CORRECTION, direct user instruction: arm the INSTANT
                # this leg is seen as rolled-in, irrespective of profit/loss --
                # no longer gated on running_pnl < 0. The old profit-gate meant a
                # rolled-in leg sitting flat/in-profit was invisible to this
                # mechanic until it first dipped negative, silently deferring the
                # R1 check by however long that took.
                inst_key = f"{self._underlying}_{side}_{strike}_R1S1_ROLL"
                calc = await self._seed_r1s1_calc(strike, side, inst_key)
                self._r1_watch[side] = watch = {
                    "calc": calc, "inst_key": inst_key, "bar_acc": None, "strike": strike,
                }
                self._clog.info(
                    "SellStraddle[%s]: R1-WATCH ARMED on rolled-in %s%d (running P&L=%.2f pts) "
                    "-- watching for R1 breach.",
                    self._underlying, side, strike, running_pnl,
                )

            self._r1_feed_bar(watch, ltp, now)
            sr_state = watch["calc"].get_calculated_sr_state(watch["inst_key"])
            if not _level_breached(sr_state, "R1", "R1_TRACKING", ltp=ltp):
                continue

            # 2026-09-23 CRITICAL FIX, real live incident: a concurrent
            # _check_exits() call (tick loop vs EOD backstop loop -- see
            # _r1_closing's own init comment above) could both observe the
            # breach true and both call _close_leg() before either finished,
            # producing a burst of duplicate real closes/orders for the same
            # leg within the same second. Claim this side synchronously
            # before the first await, mirroring _post1500_closing.
            if self._r1_closing.get(side):
                continue
            self._r1_closing[side] = True

            r1 = (sr_state.get("sr_levels") or {}).get("R1") or {}
            # Stashed for _close_remark (exits.py) to build the real History
            # remark string from -- see the "r1_breach_post_roll" case there.
            self._r1_last_breach_info = {
                "strike": strike, "phase": sr_state.get("current_phase"),
                "r1_established": sr_state.get("r1_established"),
                "r1": float(r1.get("high", 0.0) or 0.0), "ltp": ltp,
            }
            self._clog.info(
                "SellStraddle[%s]: R1 BREACH on rolled-in %s%d -- phase=%s r1_established=%s "
                "R1=%.2f ltp=%.2f -> closing this leg, will scan for an S1-breach replacement.",
                self._underlying, side, strike, sr_state.get("current_phase"),
                sr_state.get("r1_established"), float(r1.get("high", 0.0) or 0.0), ltp,
            )
            close_ev = await self._close_leg(side, "r1_breach_post_roll", now)
            self._r1_watch.pop(side, None)
            if getattr(close_ev, "close_aborted", False):
                # Close not confirmed -- leave the leg as-is, try again next tick
                # (same retry discipline as every other exit path in this book).
                self._r1_closing[side] = False
                continue
            # 2026-09-23 CRITICAL FIX, real live incident: this was never set on
            # a successful close, so current_value/unrealized_pnl kept counting
            # the "closed" leg and, worse, the very next tick's top-of-loop
            # `if getattr(pos, leg_closed_attr): continue` guard never tripped --
            # the loop re-armed a FRESH watch on the SAME still-"open" leg
            # (still in loss, same stale R1) and re-closed it again, forever,
            # each time booking another real leg_pnl into session_realized_pnl_pts
            # and (live) sending another real broker exit order. This is what
            # produced the observed runaway Booked P&L (₹-197828) and dozens of
            # duplicate History rows for the same strike within the same minute.
            setattr(pos, leg_closed_attr, True)
            self._r1_pending = {
                "side": side, "candidate_strike": None, "last_check": None,
                "_last_closed_strike": strike, "closing_ltp": ltp, "armed_at": now,
            }
            self._persist()
            pos = self._position
            if not pos or pos.status != "open":
                return

        # 2026-09-24 CRITICAL FIX, real live incident (Gurmeet's NIFTY book),
        # direct user spec ("when we restart it should call rest api
        # historical data and warm up the r1 and s1 as it does for other
        # indicators"): _r1_pending is never persisted across a restart (by
        # design -- see this module's own docstring, same graceful-
        # degradation choice already made for _post1500_calc). Part 1 above
        # is the ONLY place that creates it, and it requires a FRESH breach
        # on a currently-OPEN rolled-in leg -- which can never happen again
        # once that leg has no open position at all. Confirmed live: CE23150
        # closed via r1_breach_post_roll at 10:55am, a restart at some point
        # after that wiped _r1_pending, and the S1-breach candidate search
        # for the empty CE side never resumed for the rest of the session --
        # not because the search logic is broken (it isn't), but because
        # nothing ever called it again. Bootstrap it back from the LIVE
        # position on the first call after a restart, exactly like every
        # other indicator in this codebase re-warms via REST rather than
        # being persisted: if exactly one side is closed and the position is
        # still open, resume the search -- UNLESS post-15:00's own mechanic
        # was what closed it (_post1500_leg_closed is THAT mechanic's own
        # separate bookkeeping, distinct from pos.ce_leg_closed/pe_leg_closed
        # -- its design deliberately wants no re-entry once single-leg,
        # "R1 logic will survive and EOD" only, never this mechanic's own
        # replacement search).
        if self._r1_pending is None:
            _p1500_closed = getattr(self, "_post1500_leg_closed", None) or {}
            for _side in ("CE", "PE"):
                _closed_attr = f"{_side.lower()}_leg_closed"
                if getattr(pos, _closed_attr, False) and not _p1500_closed.get(_side, False):
                    # The closed leg's own strike field survives a close (never
                    # cleared -- see _close_leg), so it's still a reliable anchor
                    # for the ring search below even after a restart.
                    # closing_ltp is intentionally left unset (unknown at restart
                    # -- the leg's own ltp field keeps ticking with live data even
                    # after close, so it no longer reflects the real price AT the
                    # moment of the original breach) -- Part 2 treats a missing
                    # closing_ltp as "no premium cap for this restored search"
                    # rather than risk anchoring to a stale number. armed_at is
                    # stamped to NOW so the give-up timer starts fresh from the
                    # restart, not from the (unknown) original breach time.
                    _closed_leg = pos.ce_leg if _side == "CE" else pos.pe_leg
                    self._r1_pending = {
                        "side": _side, "candidate_strike": None, "last_check": None,
                        "_last_closed_strike": int(_closed_leg.strike),
                        "closing_ltp": None, "armed_at": now,
                    }
                    self._clog.info(
                        "SellStraddle[%s]: R1-REENTRY — resuming search for %s side "
                        "(found already-closed on restart, not via post-1500), anchored "
                        "on its last strike %d.",
                        self._underlying, _side, int(_closed_leg.strike),
                    )
                    break

        # ── Part 2: search for an immediate re-entry pair after an R1 breach ──
        # 2026-09-24 REDESIGN, direct user spec (several rounds of clarification,
        # confirmed step by step): the S1-breach-wait mechanic above (seed a
        # candidate's own SR calculator, wait for ITS S1 to breach before
        # entering) is GONE. The new mechanic is a direct mirror of the main
        # rollover (Step 2 in the user's own numbering), just with inverted
        # economics for the post-breach context:
        #   - search BOTH sides (ITM and OTM) of the closed strike, ring by
        #     ring, 50pts apart (not 100 -- the main rollover's gap) --
        #     select_partner_for(anchor_strike=...) already does exactly this,
        #     unlike select_rollover_partner_directional's single-fixed-
        #     direction-then-fallback search used by the main rollover.
        #   - premium condition is INVERTED: the new leg's LTP must be
        #     STRICTLY LESS than the R1-breached leg's own LTP at the moment
        #     it closed (risk-reducing replacement), not greater (the main
        #     rollover's "must be a richer leg" rule) -- see
        #     _evaluate_roll_candidate's new max_ltp_exclusive mode
        #     (selection.py).
        #   - the moment a candidate passes (gap + inverted premium gate +
        #     the existing entry_rules_reentry rule set), enter it
        #     IMMEDIATELY -- no waiting on any further breach/trigger for the
        #     new leg itself, same immediacy as the main rollover.
        #   - Step 6: if the passing candidate's LTP is below the SAME
        #     ltp_target floor fresh BEGINNING/re-entry already enforces, do
        #     NOT enter it -- instead close the remaining kept leg and shift
        #     to next week's expiry (self._shift_to_next_week_expiry, the
        #     existing low-anchor-LTP safety net), then let fresh BEGINNING
        #     logic re-fire on the new expiry.
        #   - Step 5: if 60s of continuous retrying (_R1_GIVEUP_SECONDS) never
        #     produces a single passing candidate, close the remaining kept
        #     leg and finalize -- the position goes fully flat and ordinary
        #     BEGINNING entry logic re-fires fresh, same as any other day-start.
        pending = self._r1_pending
        if not pending:
            return
        pos = self._position
        if not pos or pos.status != "open":
            return
        side = pending["side"]

        _last = pending.get("last_check")
        if _last and (now - _last).total_seconds() < _R1_CANDIDATE_RETRY_SECONDS:
            return
        pending["last_check"] = now

        from strategies.sell_straddle.selection import select_partner_for
        from data_layer.runtime_config import RuntimeConfig
        from strategies.core.rule_evaluator import eval_rules as _eval_rules

        keep_side = "PE" if side == "CE" else "CE"
        keep_leg = pos.pe_leg if keep_side == "PE" else pos.ce_leg
        keep_strike = int(keep_leg.strike)
        keep_ltp = float(getattr(keep_leg, "ltp", 0.0) or getattr(keep_leg, "entry_price", 0.0) or 0.0)
        anchor_strike = int(pending.get("_last_closed_strike") or keep_strike)
        closing_ltp = pending.get("closing_ltp")
        max_ltp_exclusive = float(closing_ltp) if closing_ltp and float(closing_ltp) > 0 else None

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        offset = int(max(int(ss.get("pool_otm_depth", 0) or 0), int(ss.get("pool_itm_depth", 0) or 0)) or 4)
        max_itm = int(ss.get("roll_max_itm_steps", 5))

        def _rule_pass(ce_s: int, pe_s: int):
            ind = self._ind_by_tf(ce_s, pe_s, rules)
            passed, reason = _eval_rules(rules, ind)
            return passed, reason, ind

        # Same trace-capture discipline as the main rollover's own search (see
        # rolling.py) -- the dashboard reads pending["last_search_summary"] to
        # show live checked/reject_counts even while nothing has passed yet.
        _trace: list = []
        partner = select_partner_for(
            self._strike_prem, roll_side=side, kept_strike=keep_strike, kept_ltp=keep_ltp,
            spot=(self._atm_ref if self._atm_ref > 0 else self._spot),
            step=self._R1_REENTRY_GAP_PTS, offset=offset, ltp_target=0.0, rule_pass=_rule_pass,
            max_itm_steps=max_itm, trace=_trace, ltp_le_kept=False,
            anchor_strike=anchor_strike, max_ltp_exclusive=max_ltp_exclusive,
        )
        _end_evt = next((e for e in reversed(_trace) if e.get("event") == "select_partner_for_end"), None)
        pending["last_search_summary"] = {
            "checked": (_end_evt or {}).get("candidates_total"),
            "reject_counts": (_end_evt or {}).get("reject_counts"),
            "kept_side": keep_side, "kept_strike": keep_strike, "kept_ltp": round(keep_ltp, 2),
            "anchor_strike": anchor_strike, "gap_pts": self._R1_REENTRY_GAP_PTS,
        }

        if not partner:
            _armed_at = pending.get("armed_at") or now
            _elapsed = (now - _armed_at).total_seconds()
            if _elapsed >= self._R1_GIVEUP_SECONDS:
                self._clog.info(
                    "SellStraddle[%s]: R1-REENTRY GIVE UP (%s side) -- no partner passed within "
                    "%.0fs of retrying (checked=%s reject_counts=%s); closing remaining %s%d and "
                    "resetting to fresh BEGINNING entry.",
                    self._underlying, side, self._R1_GIVEUP_SECONDS,
                    pending["last_search_summary"]["checked"], pending["last_search_summary"]["reject_counts"],
                    keep_side, keep_strike,
                )
                await self._close_position("r1_reentry_giveup_no_pair")
                self._r1_pending = None
                self._persist()
                return
            self._clog.info(
                "SellStraddle[%s]: R1-REENTRY WAIT (%s side empty) -- no partner currently "
                "passes (checked=%s reject_counts=%s); still watching (%.0fs/%.0fs before give-up).",
                self._underlying, side,
                pending["last_search_summary"]["checked"], pending["last_search_summary"]["reject_counts"],
                _elapsed, self._R1_GIVEUP_SECONDS,
            )
            return

        new_strike, new_ltp = partner
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        if float(new_ltp) < ltp_target:
            self._clog.info(
                "SellStraddle[%s]: R1-REENTRY candidate %s%d @%.2f is below the ltp_target floor "
                "(%.2f) -- closing remaining %s%d and shifting to next week's expiry.",
                self._underlying, side, int(new_strike), new_ltp, ltp_target, keep_side, keep_strike,
            )
            await self._close_position("r1_reentry_ltp_below_threshold")
            await self._shift_to_next_week_expiry(
                f"R1-reentry candidate {side}{int(new_strike)} ltp={new_ltp:.2f} below "
                f"ltp_target={ltp_target:.2f}",
                f"R1-reentry candidate ltp={new_ltp:.2f} < target={ltp_target:.2f}",
            )
            self._r1_pending = None
            self._persist()
            return

        self._clog.info(
            "SellStraddle[%s]: R1-REENTRY — candidate %s%d @%.2f passes (anchor=%d gap=%d closing_ltp=%s) "
            "-> re-entering immediately against kept %s%d.",
            self._underlying, side, int(new_strike), new_ltp, anchor_strike, self._R1_REENTRY_GAP_PTS,
            f"{closing_ltp:.2f}" if closing_ltp else "unknown", keep_side, keep_strike,
        )
        await self._open_leg(side, int(new_strike), float(new_ltp), now, "r1_pair_reentry_post_breach")
        self._r1_pending = None
        self._persist()
