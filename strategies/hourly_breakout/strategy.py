"""
strategies/hourly_breakout/strategy.py — HourlyBreakoutStrategy.

A pure, stateful strategy class that detects 1-hour trapped-liquidity setups on
Spot/Futures and executes retest-trigger entries on the corresponding 5-minute
option chart. Designed to be called by a live book wrapper or a backtest loop.

Public interface (required by the master application):
    on_1h_candle_close(spot_df, ce_df, pe_df) -> None
    on_5m_candle_close(spot_df, ce_df, pe_df) -> None
    on_tick(current_tick_data) -> None
    get_active_orders_and_signals() -> List[HourlyBreakoutSignal]

State machine:
    IDLE                -> scanning 1H chart for a trap setup
    TRAP_QUALIFIED     -> 1H trap found; waiting for 5M retest
    WAITING_FOR_5M_TRIGGER -> retest 5M candle identified; waiting for breach
    IN_POSITION        -> long CE or PE position active
    COOLDOWN           -> flat after a close; no new setups for 15 minutes
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import pandas as pd


class State(Enum):
    IDLE = auto()
    TRAP_QUALIFIED = auto()
    WAITING_FOR_5M_TRIGGER = auto()
    IN_POSITION = auto()
    COOLDOWN = auto()


class Side(Enum):
    CE = "CE"
    PE = "PE"


@dataclass
class HourlyBreakoutSignal:
    """Immutable signal emitted by the strategy."""
    side: Side                      # CE or PE
    action: str                     # "ENTRY" | "EXIT"
    entry_price: float
    sl_price: float
    target_price: float
    trigger_timestamp: datetime
    reason: str
    confidence: float = 1.0
    rr_ratio: float = 0.0
    option_symbol: str = ""
    exit_price: float = 0.0


@dataclass
class _OpenPosition:
    side: Side
    entry_price: float
    sl_price: float
    target_price: float
    entry_time: datetime
    bars_held: int = 0
    max_profit_reached: float = 0.0  # highest (CE) / lowest (PE) favourable distance
    highest_option_price: float = 0.0
    lowest_option_price: float = float("inf")
    reason: str = ""


class HourlyBreakoutStrategy:
    """
    1-hour trapped-liquidity breakout strategy.

    Parameters:
        underlying: underlying name (e.g. "NIFTY").
        lot_size: trading lot size for P&L estimation.
        lot_multiplier: number of lots.
        sl_buffer_pts: stop-loss buffer in option premium points.
        min_rr: minimum reward/risk ratio to allow an entry.
        max_spread_pct: block market entry if bid-ask spread > this % of price.
        stagnation_bars: exit after this many 5M bars if no 1R profit / new swing.
        cooldown_bars: bars to wait after a close before scanning new setups.
        tick_size: option tick size for trigger placement.
    """

    def __init__(
        self,
        underlying: str = "NIFTY",
        lot_size: int = 65,
        lot_multiplier: int = 1,
        sl_buffer_pts: float = 2.0,
        min_rr: float = 1.5,
        max_spread_pct: float = 1.5,
        stagnation_bars: int = 6,
        cooldown_bars: int = 3,
        tick_size: float = 0.05,
    ) -> None:
        self._underlying = underlying
        self._lot_size = lot_size
        self._lot_multiplier = lot_multiplier
        self._sl_buffer_pts = sl_buffer_pts
        self._min_rr = min_rr
        self._max_spread_pct = max_spread_pct / 100.0
        self._stagnation_bars = stagnation_bars
        self._cooldown_bars = cooldown_bars
        self._tick_size = tick_size

        self._state = State.IDLE
        self._position: Optional[_OpenPosition] = None
        self._cooldown_counter: int = 0

        # 1H trap setup storage (reset on each new qualified trap)
        self._trap_type: Optional[Side] = None          # CE or PE is the primary side
        self._break_level: Optional[float] = None       # breakout_level (CE) / breakdown_level (PE)
        self._trap_reference_ts: Optional[datetime] = None  # timestamp of the HTF reference candle
        self._trap_confirmation_time: Optional[datetime] = None
        self._option_swing_extreme: Optional[float] = None  # swing high (CE) / swing low (PE)

        # 5M trigger state
        self._trigger_candle: Optional[pd.Series] = None
        self._trigger_price: Optional[float] = None
        self._pending_signal: Optional[HourlyBreakoutSignal] = None
        self._signals: List[HourlyBreakoutSignal] = []

        # Last observed spread guard data
        self._last_spread_pct: float = 0.0

    # ── public state accessors ───────────────────────────────────────────────
    @property
    def state(self) -> str:
        return self._state.name

    @property
    def is_in_position(self) -> bool:
        return self._state == State.IN_POSITION and self._position is not None

    @property
    def current_side(self) -> Optional[str]:
        return self._position.side.value if self._position else None

    @property
    def position_summary(self) -> Optional[Dict]:
        pos = self._position
        if pos is None:
            return None
        return {
            "side": pos.side.value,
            "entry_price": pos.entry_price,
            "sl_price": pos.sl_price,
            "target_price": pos.target_price,
            "entry_time": pos.entry_time.isoformat(),
            "bars_held": pos.bars_held,
            "max_profit_reached": pos.max_profit_reached,
        }

    # ── main public callbacks ────────────────────────────────────────────────
    def on_1h_candle_close(
        self,
        spot_df: pd.DataFrame,
        ce_df: pd.DataFrame,
        pe_df: pd.DataFrame,
    ) -> None:
        """Called on every 1-hour candle close. Scans for trap setups."""
        if spot_df.empty:
            return

        # Reset 1H scan if we are flat/cooldown/qualified — but preserve an
        # active IN_POSITION state; it manages exits on the 5M clock.
        if self._state in (State.IDLE, State.COOLDOWN):
            self._reset_scan()
            self._evaluate_1h_trap(spot_df, ce_df, pe_df)
        elif self._state == State.TRAP_QUALIFIED and self._trap_confirmation_time is not None:
            # If no retest happened within a reasonable window, clear stale trap.
            # We use a 3-bar (3H) window as a defensive timeout.
            last_ts = spot_df.index[-1]
            if last_ts - self._trap_confirmation_time > timedelta(hours=3):
                self._reset_scan()
                self._set_state(State.IDLE)

    def on_5m_candle_close(
        self,
        spot_df: pd.DataFrame,
        ce_df: pd.DataFrame,
        pe_df: pd.DataFrame,
    ) -> None:
        """Called on every 5-minute candle close. Handles retest, entry, exits."""
        if spot_df.empty or ce_df.empty or pe_df.empty:
            return

        last_5m = spot_df.iloc[-1]
        last_ce = ce_df.iloc[-1]
        last_pe = pe_df.iloc[-1]
        last_ts = spot_df.index[-1]

        # Manage cooldown countdown.
        if self._state == State.COOLDOWN:
            self._cooldown_counter += 1
            if self._cooldown_counter >= self._cooldown_bars:
                self._cooldown_counter = 0
                self._set_state(State.IDLE)
                self._reset_scan()
            return

        # No new action while already in a position — only exit management.
        if self._state == State.IN_POSITION:
            self._manage_position(last_ce, last_pe, last_ts)
            return

        # Need a qualified trap to look for 5M retest triggers.
        if self._state not in (State.TRAP_QUALIFIED, State.WAITING_FOR_5M_TRIGGER):
            return

        # Identify primary option chart and opposite chart.
        primary_df, opposite_df = (ce_df, pe_df) if self._trap_type == Side.CE else (pe_df, ce_df)
        primary_last = primary_df.iloc[-1]

        # ── WAITING_FOR_5M_TRIGGER: check breach of trigger candle high ─────────
        if self._state == State.WAITING_FOR_5M_TRIGGER and self._trigger_candle is not None:
            trigger_ts = self._trigger_candle.name if hasattr(self._trigger_candle, "name") else last_ts
            if last_ts > trigger_ts:
                if self._trap_type == Side.CE and last_ce["high"] >= self._trigger_price:
                    self._maybe_emit_entry(Side.CE, self._trigger_price, self._trigger_candle["low"], last_ts)
                elif self._trap_type == Side.PE and last_pe["low"] <= self._trigger_price:
                    self._maybe_emit_entry(Side.PE, self._trigger_price, self._trigger_candle["high"], last_ts)

            # Failure invalidation: primary option closes back through break level.
            if self._trap_type == Side.CE and last_ce["close"] < self._break_level:
                self._pivot_to_opposite(Side.PE, pe_df, last_ts)
            elif self._trap_type == Side.PE and last_pe["close"] > self._break_level:
                self._pivot_to_opposite(Side.CE, ce_df, last_ts)
            return

        # ── TRAP_QUALIFIED: look for a retest candle touching break level ──────
        if self._state == State.TRAP_QUALIFIED:
            spot_touched_level = (
                (self._trap_type == Side.CE and last_5m["low"] <= self._break_level)
                or (self._trap_type == Side.PE and last_5m["high"] >= self._break_level)
            )
            if not spot_touched_level:
                return

            # Confirm the primary option candle is the retest candle.
            trigger = primary_last
            if self._spread_guard_violated(trigger["close"]):
                return

            if self._trap_type == Side.CE:
                trigger_high = trigger["high"]
                provisional_entry = trigger_high + 0.5 * self._tick_size
                sl = trigger["low"] - self._sl_buffer_pts
                rr = self._rr_ratio(provisional_entry, sl, self._option_swing_extreme)
                if rr >= self._min_rr:
                    self._trigger_candle = trigger
                    self._trigger_price = provisional_entry
                    self._set_state(State.WAITING_FOR_5M_TRIGGER)
            else:
                trigger_low = trigger["low"]
                provisional_entry = trigger_low - 0.5 * self._tick_size
                sl = trigger["high"] + self._sl_buffer_pts
                rr = self._rr_ratio(provisional_entry, sl, self._option_swing_extreme)
                if rr >= self._min_rr:
                    self._trigger_candle = trigger
                    self._trigger_price = provisional_entry
                    self._set_state(State.WAITING_FOR_5M_TRIGGER)

    def on_tick(self, current_tick_data: Dict) -> None:
        """Called on every live tick. Guards entry via bid-ask spread."""
        bid = float(current_tick_data.get("bid", 0) or 0)
        ask = float(current_tick_data.get("ask", 0) or 0)
        ltp = float(current_tick_data.get("ltp", 0) or 0)
        if bid > 0 and ask > 0 and ltp > 0:
            self._last_spread_pct = (ask - bid) / ltp

    def get_active_orders_and_signals(self) -> List[HourlyBreakoutSignal]:
        """Return signals generated since the last call and clear the buffer."""
        out = list(self._signals)
        self._signals.clear()
        return out

    # ── 1H trap detection ──────────────────────────────────────────────────────
    def _evaluate_1h_trap(
        self,
        spot_df: pd.DataFrame,
        ce_df: pd.DataFrame,
        pe_df: pd.DataFrame,
    ) -> None:
        if len(spot_df) < 3:
            return

        # Bullish trap: bearish candle B, later price breaks below B.Low,
        # then current/recent candle closes above B.High.
        for i in range(len(spot_df) - 2, 0, -1):
            candle_b = spot_df.iloc[i]
            if candle_b["close"] >= candle_b["open"]:
                continue  # need bearish candle B

            b_low = candle_b["low"]
            b_high = candle_b["high"]

            # Subsequent price must trade below B.Low somewhere after candle B.
            subsequent_below = (spot_df.iloc[i + 1 :]["low"] < b_low).any()
            if not subsequent_below:
                continue

            # Current/recent candle closes above B.High.
            current = spot_df.iloc[-1]
            if current["close"] > b_high:
                self._trap_type = Side.CE
                self._break_level = b_high
                self._trap_reference_ts = candle_b.name
                self._trap_confirmation_time = current.name
                self._option_swing_extreme = self._find_swing_high(ce_df, current.name)
                self._set_state(State.TRAP_QUALIFIED)
                return

        # Bearish trap: bullish candle A, later price breaks above A.High,
        # then current/recent candle closes below A.Low.
        for i in range(len(spot_df) - 2, 0, -1):
            candle_a = spot_df.iloc[i]
            if candle_a["close"] <= candle_a["open"]:
                continue  # need bullish candle A

            a_low = candle_a["low"]
            a_high = candle_a["high"]

            subsequent_above = (spot_df.iloc[i + 1 :]["high"] > a_high).any()
            if not subsequent_above:
                continue

            current = spot_df.iloc[-1]
            if current["close"] < a_low:
                self._trap_type = Side.PE
                self._break_level = a_low
                self._trap_reference_ts = candle_a.name
                self._trap_confirmation_time = current.name
                self._option_swing_extreme = self._find_swing_low(pe_df, current.name)
                self._set_state(State.TRAP_QUALIFIED)
                return

    def _find_swing_high(
        self,
        option_df: pd.DataFrame,
        before_ts: datetime,
        window: int = 2,
    ) -> Optional[float]:
        """Most recent 1H swing high in ``option_df`` strictly before ``before_ts``."""
        if option_df.empty:
            return None
        h1 = option_df[option_df.index < before_ts].resample("1h").agg({"high": "max"}).dropna()
        if h1.empty:
            return None
        if len(h1) < 2 * window + 1:
            return float(h1["high"].max())
        highs = h1["high"].tolist()
        swing_highs = []
        for i in range(window, len(highs) - window):
            if highs[i] > max(highs[i - window:i]) and highs[i] > max(highs[i + 1:i + window + 1]):
                swing_highs.append(highs[i])
        return float(swing_highs[-1]) if swing_highs else float(h1["high"].max())

    def _find_swing_low(
        self,
        option_df: pd.DataFrame,
        before_ts: datetime,
        window: int = 2,
    ) -> Optional[float]:
        """Most recent 1H swing low in ``option_df`` strictly before ``before_ts``."""
        if option_df.empty:
            return None
        l1 = option_df[option_df.index < before_ts].resample("1h").agg({"low": "min"}).dropna()
        if l1.empty:
            return None
        if len(l1) < 2 * window + 1:
            return float(l1["low"].min())
        lows = l1["low"].tolist()
        swing_lows = []
        for i in range(window, len(lows) - window):
            if lows[i] < min(lows[i - window:i]) and lows[i] < min(lows[i + 1:i + window + 1]):
                swing_lows.append(lows[i])
        return float(swing_lows[-1]) if swing_lows else float(l1["low"].min())

    # ── 5M execution helpers ───────────────────────────────────────────────────
    def _maybe_emit_entry(
        self,
        side: Side,
        entry_price: float,
        trigger_extreme: float,
        ts: datetime,
    ) -> None:
        if side == Side.CE:
            sl = trigger_extreme - self._sl_buffer_pts
            target = self._option_swing_extreme
        else:
            sl = trigger_extreme + self._sl_buffer_pts
            target = self._option_swing_extreme

        if target is None or entry_price == sl:
            return

        rr = self._rr_ratio(entry_price, sl, target)
        if rr < self._min_rr:
            self._reset_scan()
            self._set_state(State.IDLE)
            return

        self._position = _OpenPosition(
            side=side,
            entry_price=entry_price,
            sl_price=sl,
            target_price=target,
            entry_time=ts,
            reason="primary_continuation",
        )
        self._set_state(State.IN_POSITION)
        self._pending_signal = None

        signal = HourlyBreakoutSignal(
            side=side,
            action="ENTRY",
            entry_price=entry_price,
            sl_price=sl,
            target_price=target,
            trigger_timestamp=ts,
            reason="primary_continuation",
            rr_ratio=rr,
        )
        self._signals.append(signal)

    def _pivot_to_opposite(
        self,
        opposite_side: Side,
        opposite_df: pd.DataFrame,
        ts: datetime,
    ) -> None:
        """Scenario B: primary setup failed — immediately pivot to opposite side."""
        if opposite_df.empty:
            return
        last = opposite_df.iloc[-1]
        if self._spread_guard_violated(last["close"]):
            return

        if opposite_side == Side.CE:
            entry = last["high"] + 0.5 * self._tick_size
            sl = last["low"] - self._sl_buffer_pts
            target = float(opposite_df["high"].max())
        else:
            entry = last["low"] - 0.5 * self._tick_size
            sl = last["high"] + self._sl_buffer_pts
            target = float(opposite_df["low"].min())

        if target is None or entry == sl:
            return

        rr = self._rr_ratio(entry, sl, target)
        if rr < self._min_rr:
            self._reset_scan()
            self._set_state(State.IDLE)
            return

        self._position = _OpenPosition(
            side=opposite_side,
            entry_price=entry,
            sl_price=sl,
            target_price=target,
            entry_time=ts,
            reason="failure_pivot",
        )
        self._set_state(State.IN_POSITION)

        signal = HourlyBreakoutSignal(
            side=opposite_side,
            action="ENTRY",
            entry_price=entry,
            sl_price=sl,
            target_price=target,
            trigger_timestamp=ts,
            reason="failure_pivot",
            rr_ratio=rr,
        )
        self._signals.append(signal)

    # ── position management ──────────────────────────────────────────────────
    def _manage_position(self, ce_bar: pd.Series, pe_bar: pd.Series, ts: datetime) -> None:
        pos = self._position
        if pos is None:
            return

        bar = ce_bar if pos.side == Side.CE else pe_bar
        pos.bars_held += 1

        # Check for a new swing extreme *before* updating the running tracker.
        new_extreme = (
            (pos.side == Side.CE and bar["high"] > pos.highest_option_price)
            or (pos.side == Side.PE and bar["low"] < pos.lowest_option_price)
        )
        pos.highest_option_price = max(pos.highest_option_price, bar["high"])
        pos.lowest_option_price = min(pos.lowest_option_price, bar["low"])

        if pos.side == Side.CE:
            # SL hit
            if bar["low"] <= pos.sl_price:
                self._close_position(pos.sl_price, ts, "sl_hit")
                return
            # Target hit
            if bar["high"] >= pos.target_price:
                self._close_position(pos.target_price, ts, "target_hit")
                return
            # Track max favourable excursion for stagnation rule
            current_run = bar["high"] - pos.entry_price
            pos.max_profit_reached = max(pos.max_profit_reached, current_run)
        else:
            if bar["high"] >= pos.sl_price:
                self._close_position(pos.sl_price, ts, "sl_hit")
                return
            if bar["low"] <= pos.target_price:
                self._close_position(pos.target_price, ts, "target_hit")
                return
            current_run = pos.entry_price - bar["low"]
            pos.max_profit_reached = max(pos.max_profit_reached, current_run)

        # Stagnation rule: 30 minutes (6 bars) without 1R profit or new swing extreme.
        risk = abs(pos.entry_price - pos.sl_price)
        if pos.bars_held >= self._stagnation_bars:
            one_r_reached = pos.max_profit_reached >= risk
            if not (one_r_reached or new_extreme):
                self._close_position(bar["close"], ts, "stagnation_exit")
                return

    def _close_position(self, exit_price: float, ts: datetime, reason: str) -> None:
        pos = self._position
        if pos is None:
            return

        signal = HourlyBreakoutSignal(
            side=pos.side,
            action="EXIT",
            entry_price=pos.entry_price,
            sl_price=pos.sl_price,
            target_price=pos.target_price,
            trigger_timestamp=ts,
            reason=reason,
            exit_price=exit_price,
        )
        self._signals.append(signal)

        self._position = None
        self._reset_scan()
        self._cooldown_counter = 0
        self._set_state(State.COOLDOWN)

    # ── helpers ────────────────────────────────────────────────────────────────
    def _rr_ratio(self, entry: float, sl: float, target: Optional[float]) -> float:
        if target is None:
            return 0.0
        risk = abs(entry - sl)
        if risk == 0:
            return 0.0
        # Target must be on the profitable side of entry (opposite side from SL).
        long_ok = sl < entry < target
        short_ok = target < entry < sl
        if not (long_ok or short_ok):
            return 0.0
        return abs(target - entry) / risk

    def _spread_guard_violated(self, price: float) -> bool:
        if price <= 0:
            return True
        return self._last_spread_pct > self._max_spread_pct

    def _reset_scan(self) -> None:
        self._trap_type = None
        self._break_level = None
        self._trap_reference_ts = None
        self._trap_confirmation_time = None
        self._option_swing_extreme = None
        self._trigger_candle = None
        self._trigger_price = None
        self._pending_signal = None

    def _set_state(self, state: State) -> None:
        self._state = state
