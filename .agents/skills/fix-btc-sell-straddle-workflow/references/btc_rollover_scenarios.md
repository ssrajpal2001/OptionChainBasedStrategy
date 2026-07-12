# BTC Sell-Straddle Rollover Scenarios

## Scenario A — Squareoff at 15:24 instead of 16:30

### Symptom

Log shows squareoff around 15:24 even though the desired BTC window is 18:30→16:30.

### Root cause

`data/strategy_config.json` on the runtime machine had:

```json
"BTC": {
  "sell_straddle": {
    "squareoff_time": "15:20"
  }
}
```

The code read this value and squared off at the configured time. Additionally, saved deployments used to override this value via `deployment_store.py`, making the mismatch harder to spot.

### Fix

1. Update `data/strategy_config.json`:
   ```json
   "BTC": {
     "sell_straddle": {
       "entry_start": "18:30",
       "entry_end": "16:30",
       "squareoff_time": "16:30"
     }
   }
   ```
2. Ensure `data_layer/deployment_store.py` ignores timing fields from deployments.
3. Restart and confirm startup banner shows `SquareOff:16:30`.

---

## Scenario B — VWAP-rise rollover at 15:10 closes full position with `no_partner`

### Symptom

Log:

```text
SellStraddle[BTC]: ROLLOVER vwap_rise_roll — no valid partner for running CE63800 @168.00
(CE pnl=-99.50 PE pnl=25.00); closing position.
```

### Position state

- Open: CE63800 @~168, PE63800 @~48
- Spot: ~63,950
- `theta_target`: 15

### Why no partner

`_single_side_roll` keeps the bleeding leg (`CE63800`) and searches for a new PE with:

1. premium ≤ 168 (closest to 168 from below)
2. time value ≥ `theta_target` (15)
3. within `roll_max_itm_steps` of ATM
4. passing re-entry rules

With spot at 63,950, the PE strikes near 168 premium are deep ITM (e.g., PE64400). Their time value is negative because intrinsic value exceeds premium, so they fail the theta floor. OTM PEs pass theta but have premium far below 168, so the closest-to-168 filter picks nothing useful.

### Diagnostic after fix

The improved log prints a summary:

```text
reason: checked 13 candidates; all blocked (dual_floor_fail=7, ltp_above_kept=3, no_quote_in_pool=2, rule_fail=1)
```

If `dual_floor_fail` dominates, the theta floor is the blocker.

### Options

- Lower `theta_target` if you want to allow deep-ITM rolls.
- Increase `pool_otm_depth` / `pool_itm_depth` if candidates lack quotes.
- Accept the full close and let re-entry logic start fresh.

---

## Scenario C — Repeated VWAP-rise rollovers

### Symptom

Position rolls, then almost immediately triggers another VWAP-rise exit.

### Root cause (historical)

Before the reset fix, `_single_side_roll` did not reset `session_min_vwap`, `peak_profit`, `tsl_high_lock_rs`, or `vwap_last_good` after a roll. The new pair was compared against the old pair's min VWAP, causing false triggers.

### Current behavior

After a successful roll, `rolling.py` resets:

```python
self._position.session_min_vwap = float("inf")
self._position.peak_profit = 0.0
self._position.tsl_high_lock_rs = 0.0
self._position.trailing_active = False
self._position.trail_peak_pct = 0.0
self._position.vwap_last_good = 0.0
self._position.entry_time_value = self._position.current_time_value(self._spot)
```

The log now confirms:

```text
SellStraddle[BTC]: ROLL complete — fresh pair CE63800/PE63800.
Exit conditions reset: min_vwap=inf, peak_profit=0, tsl_lock=0, entry_tv=...
Day% guardrail continues on cumulative realized=...
```

So the second VWAP-rise is genuine, not stale data.

### If repeated rolls still happen

- BTC is trending strongly against the straddle.
- Consider widening `vwap_rise_sl_threshold_pct` or disabling `vwap_rise_sl_enabled`.

---

## Scenario D — All Delta orders rejected for `insufficient_margin`

### Symptom

Every order returns `insufficient_margin`; balance is only a few dollars while margin required is ~$120.

### Root cause

This is expected in dry-run / low-balance mode on Delta Exchange India. The strategy logic is working, but orders cannot fill because the account lacks funds.

### Fix

Not a code fix. Either:

- Fund the Delta account, or
- Continue dry-run testing and ignore the rejections.

Do not change margin logic for this symptom alone.
