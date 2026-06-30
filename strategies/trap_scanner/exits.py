"""
strategies/trap_scanner/exits.py — SL, T1, trailing SL, sweep re-entry and EOD square-off.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config.global_config import IST
from strategies.trap_scanner import scanner
from strategies.trap_scanner.zones import _bars_to_df, _resample_htf

logger = logging.getLogger(__name__)


class ExitMixin:
    """Exit management, liquidity-sweep re-entry and EOD cleanup."""

    async def _check_sweep_reentry(self, ltp: float) -> None:
        """
        After a plain SL hit, watch for liquidity sweep: price recovers above sl_level
        within 2 ticks/candles → re-enter same zone (bears swept longs, now reversing).
        """
        sw = self._sweep_watch
        if not sw or self._position:
            return
        if ltp > sw["sl_level"]:
            self._log.info(
                "SWEEP REENTRY: ltp=%.2f recovered above sl=%.2f → re-entering %s",
                ltp, sw["sl_level"], sw["leg"],
            )
            self._sweep_watch = None
            # Re-fire entry signal using stored zone — notified_uid already consumed,
            # so create a synthetic entry dict at current ltp.
            zone = sw.get("entry_zone", {})
            synth_entry = {
                "entry": ltp, "zone_low": zone.get("zone_low", ltp - 5),
                "zone_high": zone.get("zone_high", ltp + 5),
                "status": "SWEEP", "closed_on": None, "trapped_on": None,
            }
            await self._on_entry_signal(sw["leg"], sw["opt_type"], synth_entry, zone)
        else:
            sw["candles_left"] -= 1
            if sw["candles_left"] <= 0:
                self._log.info("Sweep watch expired — no recovery above %.2f", sw["sl_level"])
                self._sweep_watch = None

    async def _check_option_t1(self, opt_ltp: float, ts: Optional[datetime] = None) -> None:
        """Futures-mode T1 check against option ltp (CE1/PE1). SL stays in futures domain."""
        pos = self._position
        if not pos or pos.get("t1_hit") or not pos.get("t1_price", 0):
            return
        if opt_ltp >= pos["t1_price"]:
            pos["t1_hit"] = True
            pos["remaining_qty"] -= pos["t1_qty"]
            # CTC trail_sl = spot_at_entry (futures domain, not option fill price)
            pos["trail_sl"] = pos.get("spot_at_entry", pos["sl_price"])
            self._log.info("T1 HIT (option) opt_ltp=%.2f t1=%.2f qty=%d → trail_sl=%.2f (CTC futures)",
                           opt_ltp, pos["t1_price"], pos["t1_qty"], pos["trail_sl"])
            oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
            pos["order_id_t1"] = oid
            self._record_closed_trade(pos, exit_price=opt_ltp, exit_reason="T1", qty_override=pos["t1_qty"])
            self._persist_position()

    async def _check_tick_exit(self, ltp: float, ts: Optional[datetime] = None) -> None:
        pos = self._position
        if not pos:
            await self._check_sweep_reentry(ltp)
            return

        if self._no_target_tsl:
            # ── No-Target-TSL mode ───────────────────────────────────────────
            # Skip T1 half-exit and TSL entirely.
            # Floor locks directly from full-position P&L (no T1 prerequisite).
            if self._profit_floor > 0:
                total_qty   = pos.get("total_qty", pos.get("remaining_qty", 0))
                entry_px    = pos.get("entry_price", 0.0)
                current_pnl = (ltp - entry_px) * total_qty
                if not pos.get("floor_locked") and current_pnl >= self._profit_floor:
                    pos["floor_locked"] = True
                    self._log.info("FLOOR LOCKED ₹%.0f  ltp=%.2f  pnl=%.0f",
                                   self._profit_floor, ltp, current_pnl)
                if pos.get("floor_locked") and current_pnl < self._profit_floor:
                    self._log.info("FLOOR_SL  ltp=%.2f  pnl=%.0f < floor=%.0f → exit",
                                   ltp, current_pnl, self._profit_floor)
                    remaining = pos["remaining_qty"]
                    await self._place_exit(remaining, ltp, "FLOOR_SL")
                    self._record_closed_trade(pos, exit_price=ltp, exit_reason="FLOOR_SL")
                    self._position = None
                    self._clear_persisted_position()
                    return
        else:
            # ── Standard mode: T1 half-exit + TSL ───────────────────────────
            # T1: 50% at HTF target (option-mode only; futures-mode T1 in _check_option_t1)
            if not pos["t1_hit"] and ltp >= pos["t1_price"] and self._htf_source != "futures":
                pos["t1_hit"] = True
                pos["remaining_qty"] -= pos["t1_qty"]
                pos["trail_sl"] = pos.get("spot_at_entry", pos["entry_price"])
                pos["t1_realised_pnl"] = (ltp - pos["entry_price"]) * pos["t1_qty"]
                self._log.info("T1 HIT ltp=%.2f t1=%.2f qty=%d → trail_sl=%.2f  t1_pnl=%.0f",
                               ltp, pos["t1_price"], pos["t1_qty"], pos["entry_price"],
                               pos["t1_realised_pnl"])
                oid = await self._place_exit(pos["t1_qty"], pos["t1_price"], "T1")
                pos["order_id_t1"] = oid
                self._record_closed_trade(pos, exit_price=ltp, exit_reason="T1", qty_override=pos["t1_qty"])
                self._persist_position()

            # T2: runner exit — close remaining qty at HTF (180m) target
            if pos["t1_hit"] and not pos.get("t2_hit") and pos.get("t2_price", 0) > 0:
                if ltp >= pos["t2_price"]:
                    pos["t2_hit"] = True
                    remaining = pos["remaining_qty"]
                    self._log.info("T2 HIT ltp=%.2f t2=%.2f qty=%d → closing all",
                                   ltp, pos["t2_price"], remaining)
                    await self._place_exit(remaining, pos["t2_price"], "T2")
                    self._record_closed_trade(pos, exit_price=ltp, exit_reason="T2",
                                              qty_override=remaining)
                    self._position = None
                    self._clear_persisted_position()
                    return

            # Advance 5m trail SL using OPTION bar lows (only after T1)
            if pos["t1_hit"] and ts is not None:
                self._update_trail_sl(pos, ts)

            # Profit floor: locks after T1; exits if P&L drops below floor
            if pos["t1_hit"] and self._profit_floor > 0:
                t1_pnl      = pos.get("t1_realised_pnl", 0.0)
                rem_qty     = pos.get("remaining_qty", 0)
                entry_px    = pos.get("entry_price", 0.0)
                running_rem = (ltp - entry_px) * rem_qty if rem_qty > 0 else 0.0
                current_pnl = t1_pnl + running_rem
                if not pos.get("floor_locked") and current_pnl >= self._profit_floor:
                    pos["floor_locked"] = True
                    self._log.info("PROFIT FLOOR LOCKED ₹%.0f  (t1=%.0f + rem=%.0f)",
                                   self._profit_floor, t1_pnl, running_rem)
                if pos.get("floor_locked") and current_pnl < self._profit_floor:
                    self._log.info("FLOOR_SL  ltp=%.2f  pnl=%.0f < floor=%.0f → exit",
                                   ltp, current_pnl, self._profit_floor)
                    remaining = pos["remaining_qty"]
                    await self._place_exit(remaining, ltp, "FLOOR_SL")
                    self._record_closed_trade(pos, exit_price=ltp, exit_reason="FLOOR_SL")
                    self._position = None
                    self._clear_persisted_position()
                    return

        # SL check (active in both modes; no TSL in no_target_tsl mode)
        active_sl = pos["sl_price"] if self._no_target_tsl else (
            pos["trail_sl"] if pos["t1_hit"] else pos["sl_price"])
        is_pe_fut = (self._htf_source == "futures" and pos.get("opt_type") == "PE")
        sl_hit = (ltp >= active_sl) if is_pe_fut else (ltp <= active_sl)
        if sl_hit:
            remaining = pos["remaining_qty"]
            reason = "TRAIL_SL" if pos["t1_hit"] else "SL"
            self._log.info("%s ltp=%.2f sl=%.2f qty=%d", reason, ltp, active_sl, remaining)
            await self._place_exit(remaining, active_sl, reason)
            self._record_closed_trade(pos, exit_price=ltp, exit_reason=reason)
            # Liquidity sweep watch: only on plain SL (not trail), not after T1.
            # If price recovers above SL within 2 candles → sweep → re-enter same zone.
            if not pos["t1_hit"]:
                self._sweep_watch = {
                    "opt_type":   pos.get("opt_type", "CE"),
                    "leg":        pos["leg"],
                    "sl_level":   active_sl,
                    "entry_zone": pos.get("htf_zone", {}),
                    "qty":        remaining,
                    "candles_left": 2,
                    "orig_entry": pos["entry_price"],
                }
                self._log.info("SL hit — watching for liquidity sweep re-entry above %.2f", active_sl)
            self._position = None
            self._clear_persisted_position()

    def _update_trail_sl(self, pos: dict, ts: datetime) -> None:
        """
        Trap-based trail SL on SCAN-STRIKE option bars. Only active after T1.

        Logic (per new 5-min bar close):
          1. Scan latest option bars for bear traps that formed ABOVE our entry
          2. Register new traps as WATCHING
          3. State machine per trap:
             WATCHING    → hi >= zone_high (bears' SL hit = bears squeezed)     → SQUEEZED
             SQUEEZED    → lo <= zone_trigger (price pulls back to bears' entry) → PULLED_BACK
             PULLED_BACK → hi >= zone_high again (confirmed support held)        → CONFIRMED
             CONFIRMED   → trail_sl steps up to zone_trigger − sl_buf (bears' entry − buffer)
        """
        bar_5m = ts.replace(second=0, microsecond=0)
        bar_5m = bar_5m.replace(minute=(bar_5m.minute // 5) * 5)
        last = pos.get("last_5m_ts")
        if last is not None and bar_5m <= last:
            return
        pos["last_5m_ts"] = bar_5m

        leg_bars_map = {
            "CE1": self._bars_ce1, "CE2": self._bars_ce2,
            "PE1": self._bars_pe1, "PE2": self._bars_pe2,
            "FUT": self._bars_fut,
        }
        bars = leg_bars_map.get(pos["leg"], [])
        if not bars:
            return

        # Build 5-min bars from raw 1-min bars for the scan
        df1 = pd.DataFrame(bars)
        if df1.empty or "datetime" not in df1.columns:
            return
        df1["datetime"] = pd.to_datetime(df1["datetime"])
        df1 = df1.set_index("datetime")
        df5 = df1.resample("5min").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna().reset_index()

        if len(df5) < 3:
            return

        entry_price = pos["entry_price"]
        entry_ts    = datetime.fromisoformat(pos["entry_ts"])
        trail_traps = pos.setdefault("trail_traps", [])

        # Scan 5-min option bars for new bear traps above entry
        from strategies.trap_scanner import scanner as _sc
        try:
            _, all_traps = _sc.scan_ltf(df5, df5["high"].max(), df5["low"].min())
        except Exception:
            return

        # Register new traps formed after entry and above entry price
        known_keys = {t["key"] for t in trail_traps}
        for trap in all_traps:
            if trap.get("status") != "TRAPPED":
                continue
            trap_ts = pd.to_datetime(trap.get("trapped_on") or trap.get("ref_ts"))
            if trap_ts is None:
                continue
            trap_ts_dt = trap_ts.to_pydatetime().replace(tzinfo=None)
            if trap_ts_dt <= entry_ts:
                continue
            zt = trap.get("zone_trigger", 0)
            zh = trap.get("zone_high", 0)
            if zt <= entry_price or zh <= zt:
                continue
            key = trap_ts_dt.strftime("%H%M")
            if key not in known_keys:
                trail_traps.append({"key": key, "zone_trigger": zt,
                                    "zone_high": zh, "state": "WATCHING"})
                self._log.debug("TRAIL trap registered key=%s zt=%.2f zh=%.2f", key, zt, zh)

        # Get current bar hi/lo for state advances
        prev_start = bar_5m - timedelta(minutes=5)
        bucket = [b for b in bars[-15:]
                  if prev_start <= pd.to_datetime(b["datetime"]) < bar_5m]
        if not bucket:
            return
        bar_hi = max(b["high"] for b in bucket)
        bar_lo = min(b["low"]  for b in bucket)

        changed = False
        for trap in trail_traps:
            if trap["state"] == "CONFIRMED":
                continue
            zh, zt = trap["zone_high"], trap["zone_trigger"]
            if trap["state"] == "WATCHING":
                if bar_hi >= zh:
                    trap["state"] = "SQUEEZED"
                    self._log.info("TRAIL SQUEEZED zt=%.2f zh=%.2f bar_hi=%.2f",
                                   zt, zh, bar_hi)
            elif trap["state"] == "SQUEEZED":
                if bar_lo <= zt:
                    trap["state"] = "PULLED_BACK"
                    self._log.info("TRAIL PULLED_BACK zt=%.2f bar_lo=%.2f", zt, bar_lo)
            elif trap["state"] == "PULLED_BACK":
                if bar_hi >= zh:
                    trap["state"] = "CONFIRMED"
                    # CE (futures / option-mode): sl is floor → step UP
                    # PE (futures): sl is ceiling → step DOWN (zone_trigger + buf moves ceiling down)
                    is_pe_fut = (self._htf_source == "futures"
                                 and pos.get("opt_type") == "PE")
                    if is_pe_fut:
                        new_sl = round(zt + self._sl_buf, 2)
                        better = new_sl < pos["trail_sl"]
                        direction = "DOWN"
                    else:
                        new_sl = round(zt - self._sl_buf, 2)
                        better = new_sl > pos["trail_sl"]
                        direction = "UP"
                    if better:
                        old = pos["trail_sl"]
                        pos["trail_sl"] = new_sl
                        changed = True
                        self._log.info("TRAIL_SL STEP %s %.2f -> %.2f "
                                       "(zt=%.2f confirmed, buf=%.2f, zh=%.2f)",
                                       direction, old, new_sl, zt, self._sl_buf, zh)

        if changed:
            self._persist_position()

    async def _place_exit(self, qty: int, price: float, reason: str) -> Optional[str]:
        if qty <= 0 or not self._position:
            return None
        broker = await self._ensure_broker()
        if not broker:
            return None
        pos = self._position
        broker_sym = self._build_broker_symbol(pos["strike"], pos["side"])
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        # DELTA perpetuals: close is opposite of entry (LONG → SELL to close; SHORT → BUY to cover)
        # All other exchanges: always SELL (selling back the option we bought)
        perp_side = pos.get("perp_side")
        if perp_side == "buy":
            close_side = OrderSide.SELL   # close LONG position
        elif perp_side == "sell":
            close_side = OrderSide.BUY    # close SHORT position (cover)
        else:
            close_side = OrderSide.SELL   # standard option exit
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=close_side,
            qty=qty,
            order_type=OrderType.MARKET,
            price=price,
            tag=f"TRAP_EXIT_{reason}",
            client_id=self._cid,
        )
        try:
            oid = await broker.place_order(req)
            self._log.info("EXIT %s qty=%d order=%s", reason, qty, oid)
            return oid
        except Exception as exc:
            self._log.error("Exit order failed (%s): %s", reason, exc)
            return None

    async def _eod_square_off(self) -> None:
        pos = self._position
        if pos and pos["remaining_qty"] > 0:
            self._log.info("EOD square-off: %d units", pos["remaining_qty"])
            eod_ltp = self._ltp_cache.get(pos["leg"], 0.0)
            await self._place_exit(pos["remaining_qty"], 0.0, "EOD")
            self._record_closed_trade(pos, exit_price=eod_ltp, exit_reason="EOD")
        self._position = None
        self._clear_persisted_position()
        # Unsubscribe all option keys from the live feeder after market close so
        # those WS slots are freed up (important when running NIFTY + SENSEX + CrudeOil
        # on a single account — NSE/BSE slots released before CrudeOil session starts).
        await self._unsubscribe_all_legs()

    async def _unsubscribe_all_legs(self) -> None:
        """Release all pinned/subscribed option keys from the feeder after EOD."""
        keys = [k for k in [self._ce1_key, self._ce2_key,
                             self._pe1_key, self._pe2_key] if k]
        if not keys:
            return
        feeder = getattr(self._rebalancer, "_feeder", None) if self._rebalancer else None
        if feeder and hasattr(feeder, "unsubscribe_tokens"):
            try:
                await feeder.unsubscribe_tokens(keys)
                self._log.info("EOD: unsubscribed %d option keys for %s: %s",
                               len(keys), self._und, keys)
            except Exception as exc:
                self._log.warning("EOD unsubscribe failed: %s", exc)
        # Also unpin strikes so rebalancer doesn't re-subscribe them tomorrow at wrong prices
        if self._rebalancer is not None:
            for strike in [self._ce1_strike, self._ce2_strike,
                           self._pe1_strike, self._pe2_strike]:
                if strike:
                    try:
                        self._rebalancer.unpin_strike(self._und, float(strike))
                    except Exception:
                        pass

    def _update_futures_tsl(self, ts: datetime) -> None:
        """
        Futures-mode TSL: on each 5-min FUT candle close while in a CE/PE position,
        scan for new TRAPPED bear zones ABOVE (CE) or below (PE) the entry spot.

        TSL step 1: first new zone above entry → trail_sl = spot_at_entry (CTC)
        TSL step 2: each subsequent zone → trail_sl = new zone_low - sl_buf (trails up)

        Only advances trail_sl — never moves it back down.
        Only runs before T1 is hit (after T1, trail is handled by _update_trail_sl).
        """
        pos = self._position
        if not pos or pos.get("t1_hit"):
            return

        today = datetime.now(IST).date()
        today_bars = [b for b in self._bars_fut
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return

        df  = _bars_to_df(today_bars[-100:])
        opt_type      = pos.get("opt_type", "CE")
        spot_at_entry = pos.get("spot_at_entry", pos.get("ep", 0))
        current_sl    = pos.get("trail_sl", pos.get("sl_price", 0))
        orig_sl       = pos.get("sl_price", 0)

        _, entries = scanner.scan_htf(df)
        trapped = [e for e in entries if e["status"] == "TRAPPED"]

        if opt_type == "CE":
            # New bear traps ABOVE entry spot confirm upward momentum
            above = [e for e in trapped if e.get("zone_high", 0) > spot_at_entry]
            if not above:
                return
            # Best zone = highest zone_low above entry (most conservative trail)
            best = max(above, key=lambda e: e.get("zone_low", 0))
            new_sl = round(best["zone_low"] - self._sl_buf, 2)

            if current_sl < spot_at_entry:
                # Step 1: advance to CTC (break-even) first
                new_sl = max(new_sl, spot_at_entry)
            # Only advance, never retreat
            if new_sl > current_sl:
                self._log.info(
                    "TSL advance (CE): trail_sl %.2f → %.2f (zone_low=%.2f above entry=%.2f)",
                    current_sl, new_sl, best["zone_low"], spot_at_entry,
                )
                pos["trail_sl"] = new_sl
                self._persist_position()
        else:
            # PE: new bull traps BELOW entry spot
            below = [e for e in trapped if e.get("zone_low", 0) < spot_at_entry]
            if not below:
                return
            best  = min(below, key=lambda e: e.get("zone_high", float("inf")))
            new_sl = round(best["zone_high"] + self._sl_buf, 2)
            if current_sl > spot_at_entry:
                new_sl = min(new_sl, spot_at_entry)
            if new_sl < current_sl:
                self._log.info(
                    "TSL advance (PE): trail_sl %.2f → %.2f (zone_high=%.2f below entry=%.2f)",
                    current_sl, new_sl, best["zone_high"], spot_at_entry,
                )
                pos["trail_sl"] = new_sl
                self._persist_position()
