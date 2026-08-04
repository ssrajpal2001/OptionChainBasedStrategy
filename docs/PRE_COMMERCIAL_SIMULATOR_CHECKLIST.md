# Pre-Commercial Simulator Checklist

Run in `--mode paper` / `--mode demo` (see CLAUDE.md "Launch Commands") against
every strategy before any new client goes live with real capital. Each item
must be exercised at least once and its outcome recorded (pass/fail + log
excerpt) before sign-off.

## 1. Broker-unavailable fail-loud (Tasks 1-7: IMPLEMENTED)

**SellStraddle and V4 Cascade: Full state-safe fix — confirm-then-finalize + optimistic-close-with-revert.**
- [ ] Run `scripts/fault_injection_broker_dropout.py` to exercise the two proven failure scenarios:
      1. Broker dropout mid-ENTRY (strategy believes it entered, broker saw nothing)
      2. Broker dropout mid-EXIT (strategy begins exit, broker unreachable partway through)
      
      Both scenarios verify that:
      - On entry: no fabricated `[PAPER]` fill logged, `BROKER_UNAVAILABLE` SYSTEM_EVENT fires, position remains IDLE
      - On exit: the confirm-then-finalize loop detects the broker failure, reverts internal state, position remains open and unmanaged-but-unclosed
      - Restarting the process restores the position from persistence exactly as it was (strikes, entry prices, TSL/peak state)
      - `pm2 logs terminus` / the client's log file shows the CRITICAL broker-unavailable line clearly, within one trading session

**D1Trap BearOnly, FVG, FnO Positional: Bridge-level fix only — no fabricated fill on broker resolution failure.**
- [ ] Force `ExecutionRouter._brokers[client][binding]` to `None`/missing while an ENTRY is pending for each strategy.
      Confirm: no `[PAPER]` fill is logged, a `BROKER_UNAVAILABLE` SYSTEM_EVENT fires. Note: D1Trap and FVG strategy engines 
      do NOT yet have a fill-confirmation feedback loop (a known gap; see "Follow-up Work" section), so the strategy's 
      internal state may advance without confirmation — this is flagged as a follow-up, not silently "fixed" by omission.
- [ ] Same, but for an EXIT with an already-open position. Confirm no fabricated fill is logged and a `BROKER_UNAVAILABLE` 
      event fires. Acknowledge that D1Trap/FVG/FnO do not yet have exit-state reversion (another follow-up item) — confirm 
      this gap is still explicitly documented, not hidden.

## 2. Feed dropout mid-position
- [ ] Kill the feed (Upstox/Fyers) mid-open-position in paper mode for each
      strategy. Confirm: `SysEvent.FEEDER_DOWN` fires, no exit misfires on
      stale/zero ticks, and the GlobalFeeder heartbeat provider-switch (see
      CLAUDE.md "Development Notes") does not duplicate an order on
      reconnect.

## 3. Restart mid-position (position_store round-trip)
- [ ] For every strategy that persists to `data/positions/*.json`
      (sell_straddle, d1_trap_bear_only, v4_cascade), open a paper position,
      kill and restart the process, confirm the restored position matches
      exactly (strikes, entry prices, TSL/peak state) — this is the exact
      class of corruption the 2026-08-04 gurmeet reconciliation script
      (`scripts/fix_gurmeet_straddle_reconcile.py`) had to repair by hand.

## 4. Duplicate-entry regression (known open item)
- [ ] Confirm the BearTrap NIFTY 10:04:00 double-entry pattern (two duplicate
      "no running trap deployment" BUY rejections, flagged but not yet
      root-caused per prior session notes) is reproduced or ruled out in
      paper mode before commercial launch — this predates and is independent
      of the broker-dropout fix in this plan.

## 5. Rate-limit / order-rejection handling
- [ ] Force a broker rejection (e.g. bad symbol, insufficient margin in a
      paper/no-fund account) mid-ENTRY and mid-EXIT for each strategy.
      Confirm the strategy's abort path (SellStraddle/V4 Cascade's `entry_aborted`/
      `exit_aborted`, or the equivalent existing ENTRY-only path for
      D1Trap/FVG) fires the same way it does for a missing broker — a
      rejection and a missing-broker should both count as "did not fill,"
      never silently treated as a fill.

## 6. Multi-tenant isolation
- [ ] With two paper clients running the same strategy/underlying
      simultaneously, force a broker dropout on ONLY one client's binding.
      Confirm the other client's orders are unaffected — this plan's
      `resolve_broker_or_alert` is per (client_id, binding_id), so this
      should hold, but must be verified end-to-end, not just at the unit
      level.
