"""
strategies/trap_scanner/entries.py — entry signal handling and order placement.
"""
from __future__ import annotations

import asyncio
import logging
import time as _time_mod
from datetime import datetime, date, time
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
                                entry: dict, htf_zone: dict,
                                qty_override: Optional[int] = None,
                                stage: Optional[str] = None,
                                mtf_zone: Optional[dict] = None) -> None:
        if self._no_margin_today:
            self._log.debug("Entry blocked — no margin today (add funds)")
            return
        is_probe = stage == "probe"
        # Scale-in adds are handled by _add_to_position, not here.
        if self._position and not is_probe:
            return
        if self._position and is_probe:
            self._log.debug("Probe entry skipped — position already exists")
            return
        # Guard: prevent concurrent entry tasks from all firing while the first
        # awaits a fill. Without this, every option tick while in an HTF zone
        # creates a new task via create_task, all seeing self._position=None,
        # causing dozens of BUY orders before the first fill sets _position.
        if not is_probe and self._entry_in_progress:
            return
        if not is_probe:
            self._entry_in_progress = True
        try:
            await self._on_entry_signal_inner(leg, opt_type, entry, htf_zone,
                                              qty_override, stage, mtf_zone, is_probe)
        finally:
            if not is_probe:
                self._entry_in_progress = False

    async def _on_entry_signal_inner(self, leg: str, opt_type: str,
                                     entry: dict, htf_zone: dict,
                                     qty_override, stage, mtf_zone, is_probe) -> None:
        # Terminal + Trade gate: never fire if THIS binding's broker terminal is
        # disconnected or the Trade toggle is OFF (fixes trade firing with terminal/trade OFF).
        if not self._can_trade():
            self._log.info("Entry blocked — terminal/trade OFF for %s/%s (%s %s)",
                           self._cid, self._bid, leg, opt_type)
            return
        now = datetime.now(IST)

        # Cutoff gate — skip for 24/7 crypto (cutoff_str=None)
        if self._cutoff_str:
            ch, cm = map(int, self._cutoff_str.split(":"))
            if now.time() >= time(ch, cm):
                return

        # DTE minimum filter: block new entries when too close to expiry (near-expiry noise).
        # BANKNIFTY: backtest shows GapWR=0% and elevated false-SLs when DTE<=10.
        if self._dte_min > 0 and self._expiry_date is not None:
            from datetime import date as _date
            _dte = (self._expiry_date - _date.today()).days
            if _dte <= self._dte_min:
                self._log.info("Entry blocked — DTE=%d <= min_filter=%d (%s %s)",
                               _dte, self._dte_min, leg, opt_type)
                return

        # Entry window gate (e.g. CrudeOil W2: 18:45–19:15)
        if self._entry_win:
            wh, wm = self._entry_win[0]; eh, em = self._entry_win[1]
            if not (time(wh, wm) <= now.time() <= time(eh, em)):
                return

        uid = _zone_uid(htf_zone)
        if uid in self._notified_uids:
            return
        # Do NOT consume uid yet — only add to _notified_uids after a confirmed fill.
        # Consuming before placement means a broker rejection silently kills the zone
        # and the second zone (CE2/PE2) never gets a chance.
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

        if self._exchange == "DELTA":
            # BTC/ETH: trade perpetual futures (BTCUSD/ETHUSD) — no option strike needed.
            # CE trap = bear sellers trapped → price squeezes UP → go LONG perpetual.
            # PE trap = bull buyers trapped → price squeezes DOWN → go SHORT perpetual.
            perp_sym = "BTCUSD" if self._und == "BTC" else "ETHUSD"
            strike, exec_key = 0, perp_sym
        elif self._htf_source == "futures":
            # CrudeOil: order goes to scan strike (S1 CE / R1 PE).
            # S1/R1 pivot strikes are naturally ITM — no separate 1-ITM computation needed.
            # Spread check: if scan strike too wide, fall back to ATM.
            primary_strike = scan_strike
            primary_key    = self._build_upstox_key(primary_strike, opt_type)
            atm_key        = self._build_upstox_key(atm, opt_type)
            max_spread_pct = float(self._admin_cfg.get("max_spread_pct", 3.0))
            strike, exec_key = await self._pick_liquid_strike(
                primary_strike, primary_key, atm, atm_key, opt_type, max_spread_pct
            )
        elif self._htf_source == "spot":
            # NIFTY spot-mode: signal is on the spot chart, execution is on the scan-strike option.
            # Do NOT buy 1-ITM; keep CE1/PE1 as selected at day-open. Fall back to ATM only if
            # the scan-strike feed is not yet alive.
            primary_strike = scan_strike
            primary_key    = self._build_upstox_key(primary_strike, opt_type)
            scan_ltp       = self._ltp_cache.get(leg, 0) or 0
            if scan_ltp > 0:
                strike, exec_key = primary_strike, primary_key
            else:
                strike, exec_key = atm, self._build_upstox_key(atm, opt_type)
                self._log.info(
                    "Spot-mode entry: scan strike %d%s has no LTP yet → falling back to ATM %d%s",
                    primary_strike, opt_type, strike, opt_type,
                )
        else:
            # Sensex/Nifty option-mode: 1-ITM option is primary; ATM as fallback if spread too wide.
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
        if qty_override is not None and qty_override > 0:
            total_qty = int(qty_override)
        else:
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
        elif self._htf_source == "spot":
            tracking_leg = leg   # CE1 or PE1 — scan-strike option for execution
            # SL is in SPOT units because the signal/zone is on the spot chart.
            if opt_type == "CE":
                sl_price = round(entry["zone_low"] - self._sl_buf, 2)
            else:
                sl_price = round(entry["zone_high"] + self._sl_buf, 2)
            # T1 = MTF target; T2 = HTF runner target.
            htf_sl = round(htf_zone.get("sl", 0), 2)
            mtf_sl = round(mtf_zone.get("sl", 0), 2) if mtf_zone else 0.0
            if mtf_sl > 0 and htf_sl > 0 and mtf_sl < htf_sl:
                t1_price = mtf_sl
                t2_price = htf_sl
            else:
                t1_price = htf_sl
                t2_price = 0.0
            t1_price_fut = None
        else:
            tracking_leg = leg   # CE1 or PE1 — scan strike option bars
            sl_price     = round(entry["zone_low"] - self._sl_buf, 2)  # option zone_low (option ₹)
            # T1 = MTF (15m) zone sl = sellers' stop on 15m chart (reachable intraday).
            # T2 = HTF (180m) zone sl = sellers' stop on 180m chart (runner target).
            # If no MTF zone passed (cascade/sweep paths), T1 = HTF sl (legacy behavior).
            htf_sl   = round(htf_zone.get("sl", 0), 2)
            mtf_sl   = round(mtf_zone.get("sl", 0), 2) if mtf_zone else 0.0
            if mtf_sl > 0 and htf_sl > 0 and mtf_sl < htf_sl:
                t1_price = mtf_sl   # 15m ref bar HIGH (first, closer target)
                t2_price = htf_sl   # 180m ref bar HIGH (runner target)
            else:
                t1_price = htf_sl   # fallback: no distinct MTF level
                t2_price = 0.0
            t1_price_fut = None

        self._log.info(
            "ENTRY %s scan_strike=%d order_strike=%d%s spot=%.2f atm=%d "
            "ep=%.2f sl=%.2f t1=%.2f t2=%.2f qty=%d tracking=%s exec_key=%s",
            self._und, scan_strike, strike, opt_type, spot, atm,
            ep, sl_price, t1_price, t2_price if self._htf_source != "futures" else 0.0,
            total_qty, tracking_leg, exec_key,
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
        # DELTA perpetuals: CE trap → LONG (BUY); PE trap → SHORT (SELL)
        # All other exchanges: always BUY (buying an option)
        if self._exchange == "DELTA":
            entry_side = OrderSide.BUY if opt_type == "CE" else OrderSide.SELL
        else:
            entry_side = OrderSide.BUY
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=entry_side,
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

            # Build order candidate(s)
            if self._htf_source == "spot":
                # Spot-mode: single MARKET order at the selected scan-strike (or ATM fallback).
                candidates = [
                    (strike, self._build_broker_symbol(strike, opt_type), "SCAN"),
                ]
            elif self._htf_source == "futures":
                itm1_strike = strike          # scan strike (naturally ITM)
                atm_strike  = atm
                if opt_type == "CE":
                    otm1_strike = atm + self._step   # 1-OTM CE = above ATM
                else:
                    otm1_strike = atm - self._step   # 1-OTM PE = below ATM
                candidates = [
                    (itm1_strike, self._build_broker_symbol(itm1_strike, opt_type), "1-ITM"),
                    (atm_strike,  self._build_broker_symbol(atm_strike,  opt_type), "ATM"),
                    (otm1_strike, self._build_broker_symbol(otm1_strike, opt_type), "1-OTM"),
                ]
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
                sim_ltp = self._ltp_cache.get(opt_leg_key, 0) or ep
                self._log.warning(
                    "Entry order rejected for %s%s — recording PAPER position at ltp=%.2f",
                    strike, opt_type, sim_ltp,
                )
                avg = sim_ltp
                order_id = order_id or "PAPER"
            else:
                # Confirmed fill — NOW consume the zone uid so same zone never fires again.
                if not is_probe:
                    self._notified_uids.add(uid)
                avg = fill.avg_price

        except Exception as exc:
            self._log.error("Entry order failed: %s — recording PAPER position", exc)
            sim_ltp = self._ltp_cache.get("CE1" if opt_type == "CE" else "PE1", 0) or ep
            avg = sim_ltp
            order_id = "PAPER"

        self._build_position_from_fill(
            leg=leg, opt_type=opt_type, strike=strike, scan_strike=scan_strike,
            spot=spot, atm=atm, ep=ep, sl_price=sl_price, t1_price=t1_price,
            t2_price=t2_price, t1_price_fut=t1_price_fut, total_qty=total_qty,
            tracking_leg=tracking_leg, exec_key=exec_key, avg=avg, order_id=order_id,
            uid=uid, htf_zone=htf_zone, stage=stage,
            signal_source=f"HTF zone {_zone_uid(htf_zone)} → LTF {leg}",
        )

    def _build_position_from_fill(self, *, leg: str, opt_type: str, strike: int,
                                  scan_strike: int, spot: float, atm: int,
                                  ep: float, sl_price: float, t1_price: float,
                                  t2_price: float, t1_price_fut: Optional[float],
                                  total_qty: int, tracking_leg: str,
                                  exec_key: str, avg: float, order_id: str,
                                  uid: str, htf_zone: dict,
                                  stage: Optional[str], signal_source: str) -> None:
        """Create and persist the position dict from a completed entry fill."""
        t1_qty = total_qty // 2
        now = datetime.now(IST)
        scan_key = {
            "CE1": self._ce1_key, "CE2": self._ce2_key,
            "PE1": self._pe1_key, "PE2": self._pe2_key,
            "FUT": self._fut_key,
        }.get(tracking_leg, self._fut_key if self._htf_source == "futures" else "")
        self._position = {
            "leg":            tracking_leg,
            "signal_leg":     leg,
            "side":           opt_type,
            "strike":         strike,
            "scan_strike":    scan_strike,
            "spot_at_entry":  round(spot, 2),
            "exec_key":       exec_key,
            "scan_key":       scan_key,
            "entry_price":    round(avg, 2),
            "fut_entry_ref":  ep if self._htf_source in ("futures", "spot") else None,
            "sl_price":       sl_price,
            "trail_sl":       sl_price,
            "last_5m_ts":     None,
            "trail_traps":    [],
            "t1_price":       t1_price,
            "t2_price":       t2_price if self._htf_source != "futures" else 0.0,
            "t1_price_fut":   t1_price_fut,
            "total_qty":      total_qty,
            "t1_qty":         t1_qty,
            "remaining_qty":  total_qty,
            "t1_hit":         False,
            "t2_hit":         False,
            "entry_ts":       now.isoformat(),
            "signal_source":  signal_source,
            "order_id_entry": order_id,
            "order_id_t1":    None,
            "perp_side":      ("buy" if opt_type == "CE" else "sell") if self._exchange == "DELTA" else None,
            "htf_zone":       htf_zone,
            "opt_type":       opt_type,
            "scale_stage":    stage or "full",
            "scale_5m_added": False,
            "scale_1m_added": False,
            "htf_zone_uid":   uid,
            "scale_fills":    [],
        }
        self._persist_position()
        self._log.info(
            "ENTRY PLACED scan=%d exec=%d%s spot=%.2f fill=%.2f sl=%.2f t1=%.2f order=%s",
            scan_strike, strike, opt_type, spot, avg, sl_price, t1_price, order_id,
        )
        asyncio.ensure_future(self._place_exchange_sl(sl_price))

    async def _place_htf_direct_limit(self, leg: str, opt_type: str, zone: dict,
                                      entry_price: float, sl_price: float,
                                      qty: int, timeout_min: int) -> None:
        """Place a LIMIT order at HTF zone trigger and manage fill/timeout.

        This is the fast-entry path: no MTF/LTF confirmation. The order is cancelled
        if it does not fill within ``timeout_min`` minutes or if the zone is broken.
        """
        uid = _zone_uid(zone)
        if uid in self._notified_uids:
            self._htf_direct_pending.pop(uid, None)
            return

        # Basic gates (re-checked in case state changed between scheduling and execution)
        if not self._can_trade():
            self._log.info("HTF DIRECT [%s] uid=%s: entry blocked — terminal/trade OFF", leg, uid)
            self._htf_direct_pending.pop(uid, None)
            return
        now = datetime.now(IST)
        if self._cutoff_str:
            ch, cm = map(int, self._cutoff_str.split(":"))
            if now.time() >= time(ch, cm):
                self._log.info("HTF DIRECT [%s] uid=%s: entry blocked — after cutoff", leg, uid)
                self._htf_direct_pending.pop(uid, None)
                return
        if self._dte_min > 0 and self._expiry_date is not None:
            _dte = (self._expiry_date - date.today()).days
            if _dte <= self._dte_min:
                self._log.info("HTF DIRECT [%s] uid=%s: entry blocked — DTE", leg, uid)
                self._htf_direct_pending.pop(uid, None)
                return

        if self._htf_source != "option":
            self._log.info("HTF DIRECT [%s] uid=%s: skipped — only option-mode is supported", leg, uid)
            self._htf_direct_pending.pop(uid, None)
            return

        broker = await self._ensure_broker()
        if not broker:
            self._log.error("HTF DIRECT [%s] uid=%s: no broker", leg, uid)
            self._htf_direct_pending.pop(uid, None)
            return

        spot = self._spot_cache or self._spot_open
        atm = _round_strike(spot, self._step)
        scan_strike_map = {
            "CE1": self._ce1_strike, "CE2": self._ce2_strike,
            "PE1": self._pe1_strike, "PE2": self._pe2_strike,
        }
        scan_strike = scan_strike_map.get(leg) or 0

        # Option-mode strike selection: 1-ITM → ATM → 1-OTM
        if opt_type == "CE":
            itm1_strike = atm - self._step
            otm1_strike = atm + self._step
        else:
            itm1_strike = atm + self._step
            otm1_strike = atm - self._step
        candidates = [
            (itm1_strike, self._build_broker_symbol(itm1_strike, opt_type), "1-ITM"),
            (atm,         self._build_broker_symbol(atm, opt_type),         "ATM"),
            (otm1_strike, self._build_broker_symbol(otm1_strike, opt_type), "1-OTM"),
        ]

        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType, OrderStatus
        entry_price = round(max(entry_price, 0.05) / 0.05) * 0.05
        order_id = None
        fill = None
        exec_strike = 0
        exec_key = ""

        for cand_strike, cand_sym, cand_label in candidates:
            try:
                self._log.info(
                    "HTF DIRECT [%s] uid=%s: placing LIMIT %d%s @%.2f (%s)",
                    leg, uid, cand_strike, opt_type, entry_price, cand_label,
                )
                req = OrderRequest(
                    broker_symbol=cand_sym,
                    exchange=self._exchange,
                    side=OrderSide.BUY,
                    qty=qty,
                    order_type=OrderType.LIMIT,
                    price=entry_price,
                    tag=f"TRAP_HTFDIRECT_{self._und}_{opt_type}",
                    client_id=self._cid,
                )
                order_id = await broker.place_order(req)
                exec_strike = cand_strike
                exec_key = self._build_upstox_key(cand_strike, opt_type)

                # Poll until filled, cancelled, rejected, or timeout
                deadline = _time_mod.monotonic() + timeout_min * 60
                poll_interval = 5.0
                while _time_mod.monotonic() < deadline:
                    await asyncio.sleep(poll_interval)
                    fl = await broker.get_order_status(order_id)
                    if not fl:
                        continue
                    if fl.status == OrderStatus.COMPLETE and fl.avg_price > 0:
                        fill = fl
                        break
                    if fl.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                        self._log.warning(
                            "HTF DIRECT [%s] uid=%s: order %s %s",
                            leg, uid, order_id, fl.status.name,
                        )
                        fill = fl
                        break
                    # Zone invalidation: for a BUY limit, if premium drops below SL,
                    # the trap idea is broken → cancel remaining order.
                    ltp = self._ltp_cache.get(leg, 0) or 0
                    if ltp > 0 and ltp < sl_price:
                        self._log.info(
                            "HTF DIRECT [%s] uid=%s: zone broken (ltp=%.2f < sl=%.2f) → cancelling",
                            leg, uid, ltp, sl_price,
                        )
                        await broker.cancel_order(order_id)
                        fill = fl
                        break

                if fill and fill.status == OrderStatus.COMPLETE:
                    break

                # Candidate did not fill — try next strike if still within timeout
                if _time_mod.monotonic() >= deadline:
                    self._log.info(
                        "HTF DIRECT [%s] uid=%s: timeout waiting for %s — cancelling",
                        leg, uid, cand_label,
                    )
                    await broker.cancel_order(order_id)
                    fill = None
                    break
                fill = None
            except Exception as exc:
                self._log.error("HTF DIRECT [%s] uid=%s: candidate %s failed: %s",
                                leg, uid, cand_label, exc)
                fill = None

        if fill and fill.status == OrderStatus.COMPLETE and fill.avg_price > 0:
            self._notified_uids.add(uid)
            self._htf_direct_pending.pop(uid, None)
            self._zone_ltf_status[uid] = "direct_limit_filled"
            t1_price = round(zone.get("sl", 0.0), 2)
            self._build_position_from_fill(
                leg=leg, opt_type=opt_type, strike=exec_strike,
                scan_strike=scan_strike, spot=spot, atm=atm,
                ep=entry_price, sl_price=sl_price, t1_price=t1_price,
                t2_price=0.0, t1_price_fut=None, total_qty=qty,
                tracking_leg=leg, exec_key=exec_key, avg=fill.avg_price,
                order_id=order_id or "PAPER", uid=uid, htf_zone=zone,
                stage="htf_direct",
                signal_source=f"HTF DIRECT zone {uid}",
            )
            return

        # Did not fill — consume uid so the normal cascade does not chase a stale move
        self._notified_uids.add(uid)
        self._htf_direct_pending.pop(uid, None)
        self._zone_ltf_status[uid] = "direct_limit_cancelled"
        self._log.info("HTF DIRECT [%s] uid=%s: no fill — uid consumed", leg, uid)

    async def _add_to_position(self, qty: int, reason: str, entry: dict) -> bool:
        """Add `qty` lots to an existing scaled-in position and recompute average entry.

        Returns True if the add-order filled completely, False otherwise.
        Does NOT advance scale_stage — caller must do that after confirming True.
        """
        pos = self._position
        if not pos:
            return False
        if pos.get("t1_hit"):
            self._log.info("Scale-in %s blocked — T1 already hit", reason)
            return False
        if not self._can_trade():
            self._log.info("Scale-in %s blocked — terminal/trade OFF for %s/%s",
                           reason, self._cid, self._bid)
            return False
        now = datetime.now(IST)
        if self._cutoff_str:
            ch, cm = map(int, self._cutoff_str.split(":"))
            if now.time() >= time(ch, cm):
                self._log.info("Scale-in %s blocked — after cutoff", reason)
                return False

        strike = pos["strike"]
        opt_type = pos["side"]
        broker = await self._ensure_broker()
        if not broker:
            self._log.error("Scale-in %s aborted — no broker", reason)
            return False

        broker_sym = self._build_broker_symbol(strike, opt_type)
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType, OrderStatus
        ltp_hint = self._ltp_cache.get(pos.get("leg"), 0) or 0
        req = OrderRequest(
            broker_symbol=broker_sym,
            exchange=self._exchange,
            side=OrderSide.BUY,
            qty=qty,
            order_type=OrderType.MARKET,
            price=ltp_hint,
            tag=f"TRAP_SCALE_{self._und}_{opt_type}_{reason}",
            client_id=self._cid,
        )
        try:
            oid = await broker.place_order(req)
            fl = None
            for attempt in range(6):
                fl = await broker.get_order_status(oid)
                if fl and fl.status in (OrderStatus.COMPLETE, OrderStatus.CANCELLED):
                    break
                await asyncio.sleep(1)
            if not fl or fl.status != OrderStatus.COMPLETE or fl.avg_price <= 0:
                self._log.warning("Scale-in %s rejected: status=%s", reason,
                                  fl.status if fl else "none")
                return False

            fill_price = fl.avg_price
            old_total = pos["total_qty"]
            old_avg = pos["entry_price"]
            new_total = old_total + qty
            new_avg = round((old_avg * old_total + fill_price * qty) / new_total, 2)
            pos["total_qty"] = new_total
            pos["remaining_qty"] += qty
            pos["entry_price"] = new_avg
            pos["t1_qty"] = new_total // 2
            pos.setdefault("scale_fills", []).append({
                "reason": reason,
                "qty": qty,
                "price": fill_price,
                "ts": now.isoformat(),
                "order_id": oid,
            })
            self._persist_position()
            self._log.info(
                "SCALE-IN %s: +%d qty @%.2f | new avg=%.2f total=%d remaining=%d",
                reason, qty, fill_price, new_avg, new_total, pos["remaining_qty"],
            )
            return True
        except Exception as exc:
            self._log.error("Scale-in %s failed: %s", reason, exc)
            return False

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
