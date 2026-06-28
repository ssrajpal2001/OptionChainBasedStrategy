"""
strategies/trap_scanner/entries.py — entry signal handling and order placement.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from typing import Any, Dict, Optional

import pandas as pd

from config.global_config import IST
from strategies.trap_scanner.config import _round_strike
from strategies.trap_scanner.zones import _bars_to_df, _resample_htf, _zone_uid
from strategies.trap_scanner import scanner

logger = logging.getLogger(__name__)


class EntryMixin:
    """Strike selection, liquidity check and entry order placement."""

    async def _on_entry_signal(self, leg: str, opt_type: str,
                                entry: dict, htf_zone: dict) -> None:
        if self._no_margin_today:
            self._log.debug("Entry blocked — no margin today (add funds)")
            return
        if self._position:
            return
        # Terminal + Trade gate: never fire if THIS binding's broker terminal is
        # disconnected or the Trade toggle is OFF (fixes trade firing with terminal/trade OFF).
        if not self._can_trade():
            self._log.info("Entry blocked — terminal/trade OFF for %s/%s (%s %s)",
                           self._cid, self._bid, leg, opt_type)
            return
        now = datetime.now(IST)

        # Cutoff gate
        ch, cm = map(int, self._cutoff_str.split(":"))
        if now.time() >= time(ch, cm):
            return

        # Entry window gate (e.g. CrudeOil W2: 18:45–19:15)
        if self._entry_win:
            wh, wm = self._entry_win[0]; eh, em = self._entry_win[1]
            if not (time(wh, wm) <= now.time() <= time(eh, em)):
                return

        uid = _zone_uid(htf_zone)
        if uid in self._notified_uids:
            return
        self._notified_uids.add(uid)
        self._zone_ltf_status[uid] = "entered"

        # Scan strike (S1 CE / R1 PE) is naturally ITM relative to futures LTP
        scan_strike_map = {
            "CE1": self._ce1_strike, "CE2": self._ce2_strike,
            "PE1": self._pe1_strike, "PE2": self._pe2_strike,
            "FUT": self._ce1_strike if opt_type == "CE" else self._pe1_strike,
        }
        scan_strike = scan_strike_map.get(leg) or 0

        spot = self._spot_cache or self._spot_open
        atm  = _round_strike(spot, self._step)

        if self._htf_source == "futures":
            # CrudeOil/BTC/ETH: order goes to scan strike (S1 CE / R1 PE).
            # S1/R1 pivot strikes are naturally ITM — no separate 1-ITM computation needed.
            # Spread check: if scan strike too wide, fall back to ATM.
            primary_strike = scan_strike
            primary_key    = self._build_upstox_key(primary_strike, opt_type)
            atm_key        = self._build_upstox_key(atm, opt_type)
            max_spread_pct = float(self._admin_cfg.get("max_spread_pct", 3.0))
            strike, exec_key = await self._pick_liquid_strike(
                primary_strike, primary_key, atm, atm_key, opt_type, max_spread_pct
            )
        else:
            # Sensex/Nifty: 1-ITM option is primary; ATM as fallback if spread too wide.
            # tracked_sym (scan_key) = SCAN STRIKE option key — never changes, even if
            # exec_key falls back to ATM. SL/T1 are always on the scan strike option LTP.
            if opt_type == "CE":
                primary_1itm = atm - self._step
            elif opt_type == "PE":
                primary_1itm = atm + self._step
            else:
                primary_1itm = scan_strike
            primary_key    = self._build_upstox_key(primary_1itm, opt_type)
            atm_key        = self._build_upstox_key(atm, opt_type)
            max_spread_pct = float(self._admin_cfg.get("max_spread_pct", 3.0))
            strike, exec_key = await self._pick_liquid_strike(
                primary_1itm, primary_key, atm, atm_key, opt_type, max_spread_pct
            )

        # Entry reference = zone_high (sellers' entry level = C1.LOW).
        # We entered when premium re-tested this level after TRAPPED.
        ep       = round(entry.get("zone_high", entry.get("zone_trigger", 0)), 2)
        total_qty = self._lot_size * self._lot_mul
        t1_qty    = total_qty // 2

        # Price domain for SL/T1/monitoring depends on htf_source:
        #
        # futures-mode (CrudeOil/BTC/ETH):
        #   Signal from FUTURES bars → SL/T1 also in FUTURES ₹ (same chart).
        #   pos["leg"]="FUT" → _idx_tick_loop drives _check_tick_exit with futures LTP.
        #   Order close goes to exec_key (scan strike option or ATM fallback) via _place_exit.
        #   scan_key = futures key (tracked_sym fixed; never the option key).
        #
        # option-mode (Sensex/Nifty):
        #   Signal from SCAN STRIKE option bars → SL/T1 in OPTION ₹.
        #   pos["leg"]=CE1/PE1 → _opt_tick_loop drives _check_tick_exit with option LTP.
        #   scan_key = scan strike option key (tracked_sym fixed; NOT the exec/1-ITM key).
        if self._htf_source == "futures":
            tracking_leg = "FUT"
            # CE (bear trap): SL = floor below zone → exit if FUT drops to sl (ltp <= sl)
            # PE (bull trap): SL = ceiling above zone → exit if FUT rises to sl (ltp >= sl)
            if opt_type == "CE":
                sl_price = round(entry["zone_low"]  - self._sl_buf, 2)
            else:
                sl_price = round(entry["zone_high"] + self._sl_buf, 2)
            # T1 = option chart HTF: latest TRAPPED bear zone sl on CE1/PE1 bars.
            # Bears shorted the option → their SL (ref bar HIGH on option chart) = our T1.
            # Checked against option ltp (not futures) in _opt_tick_loop.
            opt_bars = self._bars_ce1 if opt_type == "CE" else self._bars_pe1
            t1_price     = self._compute_option_t1(opt_bars)
            t1_price_fut = round(htf_zone.get("sl", 0), 2)  # kept for logging/UI reference
        else:
            tracking_leg = leg   # CE1 or PE1 — scan strike option bars
            sl_price     = round(entry["zone_low"] - self._sl_buf, 2)  # option zone_low (option ₹)
            t1_price     = round(htf_zone.get("sl", 0), 2)             # HTF ref bar HIGH (bears' SL)
            t1_price_fut = None

        self._log.info(
            "ENTRY %s scan_strike=%d order_strike=%d%s spot=%.2f atm=%d "
            "ep=%.2f sl=%.2f t1=%.2f qty=%d tracking=%s exec_key=%s",
            self._und, scan_strike, strike, opt_type, spot, atm,
            ep, sl_price, t1_price, total_qty, tracking_leg, exec_key,
        )

        if self._rebalancer is not None:
            try:
                self._rebalancer.pin_strike(self._und, float(strike))
            except Exception:
                pass

        broker = await self._ensure_broker()
        if not broker:
            self._log.error("No broker — entry aborted")
            return

        broker_sym = self._build_broker_symbol(strike, opt_type)
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=OrderSide.BUY,
            qty=total_qty,
            order_type=OrderType.MARKET,
            price=ep,
            tag=f"TRAP_{self._und}_{opt_type}",
            client_id=self._cid,
        )
        from execution_bridge.base_broker import OrderStatus
        import asyncio as _asyncio

        async def _wait_fill(oid: str, label: str):
            """Poll up to 6s for a terminal order status (COMPLETE or CANCELLED)."""
            for attempt in range(6):
                fl = await broker.get_order_status(oid)
                if fl.status in (OrderStatus.COMPLETE, OrderStatus.CANCELLED):
                    return fl
                self._log.info("Entry %s %s: status=%s avg=%.2f — waiting...",
                               label, oid, fl.status, fl.avg_price or 0)
                await _asyncio.sleep(1)
            return fl

        async def _place_and_fill(sym, ltp_hint, label):
            """Place a MARKET order and wait for fill."""
            r = req.__class__(
                broker_symbol=sym,
                exchange=self._exchange,
                side=req.side,
                qty=total_qty,
                order_type=req.order_type,
                price=ltp_hint,
                tag=req.tag,
                client_id=self._cid,
            )
            oid = await broker.place_order(r)
            fl  = await _wait_fill(oid, label)
            return oid, fl

        try:
            opt_leg_key = "CE1" if opt_type == "CE" else "PE1"

            # Build 1-ITM, ATM, 1-OTM strikes for futures-mode
            if self._htf_source == "futures":
                itm1_strike = strike          # scan strike (naturally ITM)
                atm_strike  = atm
                if opt_type == "CE":
                    otm1_strike = atm + self._step   # 1-OTM CE = above ATM
                else:
                    otm1_strike = atm - self._step   # 1-OTM PE = below ATM
            else:
                itm1_strike = strike
                atm_strike  = atm
                if opt_type == "CE":
                    otm1_strike = atm + self._step
                else:
                    otm1_strike = atm - self._step

            candidates = [
                (itm1_strike, self._build_broker_symbol(itm1_strike, opt_type), "1-ITM"),
                (atm_strike,  self._build_broker_symbol(atm_strike,  opt_type), "ATM"),
                (otm1_strike, self._build_broker_symbol(otm1_strike, opt_type), "1-OTM"),
            ]

            order_id = None
            fill = None
            for cand_strike, cand_sym, cand_label in candidates:
                cand_ltp = self._ltp_cache.get(opt_leg_key, 0) or 0
                self._log.info("Entry attempt %s: %d%s ltp=%.2f", cand_label, cand_strike, opt_type, cand_ltp)
                order_id, fill = await _place_and_fill(cand_sym, cand_ltp, f"{cand_strike}{opt_type}{cand_label}")
                if fill.status == OrderStatus.COMPLETE and fill.avg_price > 0:
                    strike   = cand_strike
                    exec_key = self._build_upstox_key(cand_strike, opt_type)
                    self._log.info("Entry FILLED at %s: %d%s avg=%.2f", cand_label, cand_strike, opt_type, fill.avg_price)
                    break
                self._log.warning(
                    "Entry %s %d%s REJECTED (status=%s) — trying next",
                    cand_label, cand_strike, opt_type, fill.status if fill else "none"
                )

            if fill is None or fill.status != OrderStatus.COMPLETE or fill.avg_price <= 0:
                self._log.error(
                    "Entry aborted — all 3 strikes (1-ITM/ATM/1-OTM) rejected for %s%s. "
                    "Add funds to trade CrudeOil options.",
                    strike, opt_type
                )
                self._no_margin_today = True   # flag to skip further entries today
                return

            avg = fill.avg_price

        except Exception as exc:
            self._log.error("Entry order failed: %s", exc)
            return

        # scan_key = the key used for SL/T1 monitoring ticks.
        # futures-mode: tracking_leg="FUT" → futures key (SL/T1 in futures ₹).
        # option-mode:  tracking_leg=CE1/PE1 → that option's Upstox key.
        scan_key = {
            "CE1": self._ce1_key, "CE2": self._ce2_key,
            "PE1": self._pe1_key, "PE2": self._pe2_key,
            "FUT": self._fut_key,
        }.get(tracking_leg, self._fut_key if self._htf_source == "futures" else "")
        self._position = {
            "leg":            tracking_leg,  # FUT for futures-mode (futures ticks drive SL/T1)
            "signal_leg":     leg,           # original detection leg (FUT for CrudeOil)
            "side":           opt_type,
            "strike":         strike,        # exec strike (scan strike or ATM fallback)
            "scan_strike":    scan_strike,   # pivot strike (S1/R1) used for zone detection
            "spot_at_entry":  round(spot, 2),
            "exec_key":       exec_key,      # Upstox key for 1-ITM contract
            "scan_key":       scan_key,      # Upstox key for tracking leg (SL monitoring)
            "entry_price":    round(avg, 2),   # option fill premium
            "fut_entry_ref":  ep if self._htf_source == "futures" else None,
            "sl_price":       sl_price,
            "trail_sl":       sl_price,      # steps up via trap-based trail after T1
            "last_5m_ts":     None,
            # Trap-based trail state: bears trapped above entry → squeezed → pullback → confirmed
            # Each entry: {zone_trigger, zone_high, state: WATCHING|SQUEEZED|PULLED_BACK|CONFIRMED}
            "trail_traps":    [],
            "t1_price":       t1_price,      # futures ₹ for futures-mode; option ₹ for option-mode
            "t1_price_fut":   t1_price_fut,  # unused (kept for backward compat with persisted state)
            "total_qty":      total_qty,
            "t1_qty":         t1_qty,
            "remaining_qty":  total_qty,
            "t1_hit":         False,
            "entry_ts":       now.isoformat(),
            "signal_source":  f"HTF zone {_zone_uid(htf_zone)} → LTF {leg}",
            "order_id_entry": order_id,
            "order_id_t1":    None,
            "htf_zone":       htf_zone,
            "opt_type":       opt_type,
        }
        self._persist_position()
        self._log.info(
            "ENTRY PLACED scan=%d exec=%d%s spot=%.2f fill=%.2f sl=%.2f t1=%.2f order=%s",
            scan_strike, strike, opt_type, spot, avg, sl_price, t1_price, order_id,
        )

    def _compute_option_t1(self, opt_bars: list) -> float:
        """T1 = latest TRAPPED bear zone sl on the option chart (ref bar HIGH = bears' SL)."""
        try:
            if not opt_bars or len(opt_bars) < 3:
                return 0.0
            df  = _bars_to_df(opt_bars[-200:])
            htf = _resample_htf(df, self._htf_min)
            if len(htf) < 2:
                return 0.0
            _, entries = scanner.scan_htf(htf)
            trapped = [e for e in entries if e["status"] == "TRAPPED"]
            if not trapped:
                return 0.0
            return round(trapped[-1]["sl"], 2)   # most recent trapped zone's ref bar HIGH
        except Exception as exc:
            self._log.warning("_compute_option_t1 failed: %s", exc)
            return 0.0
