---
name: fix-btc-sell-straddle-workflow
description: Diagnose and fix BTC sell-straddle rollover, squareoff-timing, and repeated-roll issues on Delta Exchange India in the OptionChainBasedStrategy project. Use when logs show unexpected BTC sell_straddle squareoffs, rollovers closing the full position, missing rollover partners, VWAP-rise loops, or timing mismatches between config and actual behavior.
---

# Fix BTC Sell-Straddle Workflow

## Quick diagnostic checklist

1. **Confirm the runtime config** — read `data/strategy_config.json` (NOT the gitignored template) and check the `BTC.sell_straddle` block:
   - `entry_start` should be `18:30`
   - `entry_end` should be `16:30`
   - `squareoff_time` should be `16:30`
2. **Confirm the deployment is not overriding timing** — `data_layer/deployment_store.py` must ignore `squareoff_time`, `entry_start`, and `entry_end` from saved deployments.
3. **Read the exact log line** for the rollover / squareoff event.
   - If it says `single_side_roll_*_no_partner`, inspect the `reason=` summary.
   - If it says `SQUAREOFF`, note the triggered time and compare with `squareoff_time`.
4. **Check `DeltaChainManager` window** — in `run_system.py` the chain window must be at least `max(pool_itm_depth, pool_otm_depth)` for BTC.
5. **Run tests**: `pytest tests/strategies/ -q`

## Key files and what to verify

| File | What to check |
|------|---------------|
| `data/strategy_config.json` | `BTC.sell_straddle.entry_start`, `entry_end`, `squareoff_time` |
| `data/strategy_config_for_ec2.json` | Template should match the desired production timing |
| `data_layer/deployment_store.py` | `apply_deployment_to_runtime_config()` must NOT patch timing fields |
| `strategies/sell_straddle/rolling.py` | `_single_side_roll()` logs `reason=` summary; resets exit state after roll |
| `strategies/sell_straddle/selection.py` | `select_partner_for()` uses variable-strike scan for BTC; `theta_target` floor |
| `run_system.py` | `DeltaChainManager(..., window=...)` uses largest crypto sell_straddle pool depth |

## Common fixes

### Squareoff happens too early

- Edit `data/strategy_config.json` → set `squareoff_time` to `16:30`.
- Verify deployment store does not override it.
- Restart the bot and confirm the startup banner shows `SquareOff:16:30`.

### Rollover closes full position with `no_partner`

- Read the new `reason=` summary in the log.
- If `dual_floor_fail` dominates, the `theta_target` floor is rejecting deep-ITM candidates. Decide whether to lower `theta_target` or accept the close.
- If `ltp_above_kept` dominates, no candidate has premium ≤ the bleeding leg; the market has moved too far.
- If `no_quote_in_pool` dominates, increase `DeltaChainManager` window or check feed subscription.

### Repeated VWAP-rise rollovers

- After a successful roll, `rolling.py` resets `session_min_vwap`, `peak_profit`, and TSL state so the next exit is evaluated on the fresh pair.
- If rolls still repeat, the pair is genuinely trending; review the VWAP-rise threshold or disable `vwap_rise_sl_enabled`.

## Tests

```bash
pytest tests/strategies/ -q
```

Expected: 154 passing.

## Detailed scenarios

See [references/btc_rollover_scenarios.md](references/btc_rollover_scenarios.md) for step-by-step root-cause analysis of real production incidents.
