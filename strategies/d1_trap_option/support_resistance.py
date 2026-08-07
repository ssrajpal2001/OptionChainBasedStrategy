"""
strategies/d1_trap_option/support_resistance.py — ported from the user's
"ping-pong" Support & Resistance state machine (2026-08-07), for evaluation
as a replacement entry/SL mechanic inside BearTrap's existing HTF zone
(see scripts/d1trap_sr_zone_backtest.py -- this module is NOT wired into the
live D1TrapBearOnlyBook, evaluation only).

Core logic (process_straddle_candle, get_calculated_sr_state,
_initialize_instrument) is UNCHANGED from the source the user provided --
only the logging import was swapped (this repo has no utils.logger module)
and the pandas-specific `get_sr_status_shared` replay wrapper was dropped
since the backtest script drives process_straddle_candle directly per-bar
itself (same pattern every other backtest script in this repo uses), rather
than needing that helper's own OHLC-DataFrame-indexed replay logic.

Mechanic summary (see module docstring in the backtest script for the full
write-up): starts at a "base range" (Phase 0), and once price makes a clean
directional break (higher high AND higher low, or the mirror), tracks that
break's high as resistance (R1) while the prior candle's low locks in as
support (S1). A full reversal through the established level triggers a
"directional flip" (level resets, tracking swaps sides); a partial pullback
that itself reverses gets "promoted" to become the new S1/R1, letting the
ladder walk up (uptrend) or down (downtrend) as confirmed swing points form.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def log_sr_details(msg: str) -> None:
    """Verbose per-candle trace, split from the main logger since it's noisy
    -- mirrors the source's separate log_sr_details channel."""
    logger.debug(msg)


class SupportResistanceCalculator:
    def __init__(self):
        # self.states stores the S&R state for each instrument
        # Format: { inst_key: { 'current_phase': str, 'sr_levels': { 'S1': {...}, 'R1': {...}, ... } } }
        self.states = {}

    def get_calculated_sr_state(self, inst_key):
        state = self.states.get(inst_key, {'current_phase': 'UNKNOWN', 'sr_levels': {}})
        sr_levels = state.get('sr_levels', {})
        s1 = sr_levels.get('S1')
        r1 = sr_levels.get('R1')

        state['s1_established'] = s1.get('is_established', False) if s1 else False
        state['r1_established'] = r1.get('is_established', False) if r1 else False
        return state

    def reset_and_process_sequence(self, inst_key, candles):
        """Clears existing state for an instrument and replays a sequence of candles."""
        if inst_key in self.states:
            del self.states[inst_key]

        for candle in candles:
            self.process_straddle_candle(inst_key, candle)

        return self.get_calculated_sr_state(inst_key)

    def process_straddle_candle(self, inst_key, candle_data, silent=False):
        """
        Support & Resistance logic using a granular state machine.
        Follows Ping-Pong logic with Rule #1 as anchor.
        """
        if inst_key not in self.states:
            self._initialize_instrument(inst_key, candle_data)
            return

        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        state = self.states[inst_key]
        last_candle = state['last_candle']

        # Skip if older
        if ts < last_candle['timestamp']:
            return
        # Skip if same time and NOT a longer duration (prevents double processing in history replay)
        if ts == last_candle['timestamp'] and duration <= last_candle.get('duration', 1):
            return

        phase = state['current_phase']
        sr_levels = state['sr_levels']
        s1 = sr_levels['S1']
        r1 = sr_levels['R1']

        # --- Phase 0: Base Range Breakout ---
        prev_high = last_candle['high']
        prev_low = last_candle['low']

        if phase == 'INITIAL_TREND_ESTABLISHMENT':
            # 1. OUTSIDE CANDLE: EXPAND BASE
            if high > prev_high and low < prev_low:
                s1['low'] = low
                r1['high'] = high
                s1['timestamp'] = ts
                r1['timestamp'] = ts
                if not silent:
                    logger.debug(f"S&R: Phase 0 Base Expansion (Outside Candle) | Strike: {inst_key} | at {ts} | Range: {low:.2f} - {high:.2f}")

            # 2. BREAKOUT HIGH (BOUNCE): S1 Established at PREVIOUS Low
            elif high > prev_high and low > prev_low:
                s1['low'] = prev_low
                s1['is_established'] = True
                s1['timestamp'] = last_candle['timestamp']

                r1['high'] = high
                r1['breakout_level'] = low
                r1['is_established'] = False
                r1['timestamp'] = ts

                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Phase 0 -> R1_TRACKING (High Breach at {ts}). S1 established at {s1['low']:.2f} | Hurdle: {prev_high:.2f}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            # 4. BREAKOUT LOW (PULLBACK): R1 Established at PREVIOUS High
            elif low < prev_low and high < prev_high:
                r1['high'] = prev_high
                r1['is_established'] = True
                r1['timestamp'] = last_candle['timestamp']

                s1['low'] = low
                s1['breakout_level'] = high
                s1['is_established'] = False
                s1['timestamp'] = ts

                state['current_phase'] = 'S1_TRACKING'
                msg = f"S&R: Phase 0 -> S1_TRACKING (Low Breach at {ts}). R1 established at {r1['high']:.2f} | Hurdle: {prev_low:.2f}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
            return

        # --- Phase 1: Primary Trend Tracking ---
        if phase == 'R1_TRACKING':
            if low < s1['low']:
                s1_base = s1['low']
                s1['low'] = low
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                s1['is_established'] = False
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if high > r1['high']:
                r1['high'] = high
                r1['timestamp'] = ts
                r1['is_established'] = False

            if high < prev_high and low < prev_low:
                r1['is_established'] = True
                state['current_phase'] = 'S2_TRACKING'
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: R1 Established at {r1['high']:.2f} | Confirmed at {ts} (Phase: S2_TRACKING)"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        elif phase == 'S1_TRACKING':
            if high > r1['high']:
                r1_base = r1['high']
                r1['high'] = high
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Directional Flip -> R1_TRACKING (R1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1['low'] = low
                s1['timestamp'] = ts
                s1['is_established'] = False

            if low > prev_low and high > prev_high:
                s1['is_established'] = True
                state['current_phase'] = 'R2_TRACKING'
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: S1 Established at {s1['low']:.2f} | Confirmed at {ts} (Phase: R2_TRACKING)"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        # --- Phase 2: Secondary Tracking (Ping-Pong / Promotion) ---
        elif phase == 'S2_TRACKING':
            s2 = sr_levels['S2']

            if high > r1['high']:
                r1_base = r1['high']
                if sr_levels.get('S2'):
                    old_s1 = s1['low']
                    sr_levels['S1'] = sr_levels['S2'].copy()
                    sr_levels['S1']['is_established'] = True
                    s1 = sr_levels['S1']
                    msg = f"S&R: Scenario A (R1 Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Resetting R1"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts}"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)

                r1['high'] = high
                r1['breakout_level'] = low
                r1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'R1_TRACKING'
                r1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1_base = s1['low']
                s1['low'] = low
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif low < s2['low']:
                s2['low'] = low
                s2['timestamp'] = ts

            elif low > prev_low and high > prev_high:
                old_s1 = s1['low']
                sr_levels['S1'] = sr_levels['S2'].copy()
                sr_levels['S1']['is_established'] = True
                s1 = sr_levels['S1']
                sr_levels['S2'] = None

                state['current_phase'] = 'R2_TRACKING'
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: Scenario B (Bounce No Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Tracking R2"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)

        elif phase == 'R2_TRACKING':
            r2 = sr_levels['R2']

            if high > r1['high']:
                r1_base = r1['high']
                sr_levels['R1'] = sr_levels['R2'].copy()
                r1 = sr_levels['R1']
                r1['high'] = high
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts} | R1 took R2 values"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                s1_base = s1['low']
                if sr_levels.get('R2'):
                    old_r1 = r1['high']
                    sr_levels['R1'] = sr_levels['R2'].copy()
                    sr_levels['R1']['is_established'] = True
                    r1 = sr_levels['R1']
                    msg = f"S&R: Directional Flip (S1 Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Resetting S1"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts}"
                    if not silent:
                        logger.debug(msg)
                        log_sr_details(msg)

                s1['low'] = low
                s1['breakout_level'] = high
                s1['timestamp'] = ts
                sr_levels['R2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif high > r2['high']:
                r2['high'] = high
                r2['timestamp'] = ts

            elif high < prev_high and low < prev_low:
                old_r1 = r1['high']
                sr_levels['R1'] = sr_levels['R2'].copy()
                sr_levels['R1']['is_established'] = True
                r1 = sr_levels['R1']
                sr_levels['R2'] = None

                state['current_phase'] = 'S2_TRACKING'
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"S&R: Scenario B (Pullback No Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Tracking S2"
                if not silent:
                    logger.debug(msg)
                    log_sr_details(msg)

        state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}

    def _initialize_instrument(self, inst_key, candle_data):
        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        self.states[inst_key] = {
            'current_phase': 'INITIAL_TREND_ESTABLISHMENT',
            'last_candle': {'high': high, 'low': low, 'timestamp': ts, 'duration': duration},
            'sr_levels': {
                'S1': {'low': low, 'high': high, 'timestamp': ts, 'is_established': False},
                'R1': {'high': high, 'low': low, 'timestamp': ts, 'is_established': False},
                'S2': None,
                'R2': None
            }
        }
