"""
strategies/trap_scanner/zones.py — HTF scan, LTF scan, cascade, zone reachability.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import pandas as pd

from config.global_config import IST
from strategies.trap_scanner import scanner

logger = logging.getLogger(__name__)


def _bars_to_df(bars: List[dict]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _resample_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    dfc = df.set_index("datetime")
    htf = dfc.resample(f"{minutes}min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return htf


def _zone_uid(e: dict) -> str:
    """Stable unique ID for a zone so notified_uids deduplicate across candle refreshes."""
    return f"{e.get('ref_ts','')}_{e.get('zone_high',0):.2f}_{e.get('kind','BEAR')}"


class ZonesMixin:
    """HTF/LTF scanning, cascade fallback and zone-reachability logic."""

    # ── Candle bucketing ──────────────────────────────────────────────────────

    def _update_bucket(self, bkey: str, ltp: float, ts: datetime) -> Optional[dict]:
        bucket_ts = ts.replace(second=0, microsecond=0)
        ltp = float(ltp)
        if bkey not in self._buckets:
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp, "volume": 0}
            return None
        b = self._buckets[bkey]
        if b["ts"] != bucket_ts:
            completed = {"datetime": b["ts"].isoformat(),
                         "open": b["open"], "high": b["high"],
                         "low": b["low"],   "close": b["close"], "volume": b["volume"]}
            self._buckets[bkey] = {"ts": bucket_ts, "open": ltp, "high": ltp,
                                   "low": ltp, "close": ltp, "volume": 0}
            return completed
        b["high"]  = max(b["high"], ltp)
        b["low"]   = min(b["low"],  ltp)
        b["close"] = ltp
        b["volume"] += 1
        return None

    # ── HTF ATR / reachability ────────────────────────────────────────────────

    def _compute_htf_atr(self) -> float:
        """14-bar ATR on HTF bars. Used for zone-reachability distance check."""
        if self._htf_source == "futures":
            bars = self._bars_fut
        elif self._htf_source == "spot":
            bars = self._bars_ce1  # option bars — ATR must match option zone scale
        else:  # "option": use CE1 bars (representative; same scale as zones)
            bars = self._bars_ce1
        df = _bars_to_df(bars)
        if df.empty:
            return 0.0
        htf = _resample_htf(df, self._htf_min)
        if len(htf) < 2:
            return 0.0
        trs = []
        for i in range(1, len(htf)):
            h, l, pc = htf.iloc[i]["high"], htf.iloc[i]["low"], htf.iloc[i - 1]["close"]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return round(sum(trs[-14:]) / min(len(trs), 14), 2) if trs else 0.0

    def _check_zone_reachability(self) -> None:
        """
        Per-leg cascade for htf_source=option: CE and PE evaluated independently.
        CE cascades if no bear zones OR nearest bear zone > 1.5×ATR from CE LTP.
        PE cascades if no bull zones OR nearest bull zone > 1.5×ATR from PE LTP.
        For futures/spot: single shared _intraday_mode (unchanged).
        """
        threshold = 2.0 * self._htf_atr_val if self._htf_atr_val > 0 else None

        if self._htf_source == "option":
            ltp_ce = self._ltp_cache.get("CE1") or self._ltp_cache.get("CE2") or 0.0
            ltp_pe = self._ltp_cache.get("PE1") or self._ltp_cache.get("PE2") or 0.0

            def _regime_ok(zone: dict, ltp: float) -> bool:
                """Drop zones from a different premium regime (theta-decayed or expired)."""
                if ltp <= 0:
                    return True
                zh = zone.get("zone_high", 0.0)
                zl = zone.get("zone_low", 0.0)
                return zh > ltp * 0.3 and zl < ltp * 3.0

            # --- CE leg ---
            bear_trapped = [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"]
            bear_trapped = [z for z in bear_trapped if _regime_ok(z, ltp_ce)]
            if not bear_trapped or not ltp_ce or threshold is None:
                new_ce = True
                reason_ce = "No bear zones" if not bear_trapped else "No CE LTP/ATR"
            else:
                dist_ce = min(abs(ltp_ce - z.get("zone_trigger", ltp_ce)) for z in bear_trapped)
                new_ce = dist_ce > threshold
                reason_ce = f"dist={dist_ce:.1f} {'>' if new_ce else '<='} {threshold:.1f}"
            if new_ce != self._intraday_mode_ce:
                self._intraday_mode_ce = new_ce
                self._log.info("CE cascade=%s (%s) ltp_ce=%.1f", new_ce, reason_ce, ltp_ce)

            # --- PE leg ---
            bull_trapped = [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"]
            bull_trapped = [z for z in bull_trapped if _regime_ok(z, ltp_pe)]
            if not bull_trapped or not ltp_pe or threshold is None:
                new_pe = True
                reason_pe = "No bull zones" if not bull_trapped else "No PE LTP/ATR"
            else:
                dist_pe = min(abs(ltp_pe - z.get("zone_trigger", ltp_pe)) for z in bull_trapped)
                new_pe = dist_pe > threshold
                reason_pe = f"dist={dist_pe:.1f} {'>' if new_pe else '<='} {threshold:.1f}"
            if new_pe != self._intraday_mode_pe:
                self._intraday_mode_pe = new_pe
                self._log.info("PE cascade=%s (%s) ltp_pe=%.1f", new_pe, reason_pe, ltp_pe)

            # Legacy single flag = True if either leg is cascading
            self._intraday_mode = self._intraday_mode_ce or self._intraday_mode_pe
            return

        # --- futures / spot: single shared flag ---
        ltp = self._spot_cache or self._spot_open
        if not ltp or threshold is None:
            if self._trapped_zone_count() == 0:
                if not self._intraday_mode:
                    self._intraday_mode = True
                    self._log.info("No TRAPPED zones → cascade")
            else:
                if self._intraday_mode:
                    self._intraday_mode = False
            return

        trapped = (
            [e for e in self._htf_bear_zones if e["status"] == "TRAPPED"] +
            [e for e in self._htf_bull_zones if e["status"] == "TRAPPED"] +
            [e for e in self._htf_fut_zones  if e["status"] == "TRAPPED"]
        )
        if not trapped:
            if not self._intraday_mode:
                self._intraday_mode = True
                self._log.info("No TRAPPED zones → cascade")
            return

        nearest_dist = min(
            abs(ltp - z.get("zone_trigger", z.get("entry", ltp))) for z in trapped
        )
        if nearest_dist > threshold:
            if not self._intraday_mode:
                self._intraday_mode = True
                self._log.info(
                    "Nearest zone too far: dist=%.2f > 1.5*ATR=%.2f ltp=%.2f → cascade",
                    nearest_dist, threshold, ltp,
                )
        else:
            if self._intraday_mode:
                self._intraday_mode = False
                self._log.info(
                    "Zone reachable: dist=%.2f <= 1.5*ATR=%.2f → normal mode",
                    nearest_dist, threshold,
                )

    # ── HTF scan ──────────────────────────────────────────────────────────────

    def _run_htf_scan(self, bars_override: Optional[List[dict]] = None,
                      minutes_override: Optional[int] = None) -> None:
        """
        Run HTF scan on option premium bars (NSE/BSE) or futures bars (MCX).
        htf_source="option": scan_htf on CE1 bars for BEAR zones; scan_htf on PE1 for BULL zones
          → zone H/L in option premium units; scan_ltf with same bars is consistent (no mismatch)
        htf_source="spot":   scan_htf_spot on SPOT bars (legacy path)
        htf_source="futures": scan_htf on futures bars
        """
        minutes = minutes_override or self._htf_min
        if self._htf_source == "futures":
            bars = bars_override or self._bars_fut
            df = _bars_to_df(bars)
            if df.empty or len(df) < 2:
                return
            htf = _resample_htf(df, minutes)
            if len(htf) < 2:
                return
            # scan_htf_spot returns bear (→CE) + bull (→PE) zones with kind/direction fields
            _, entries = scanner.scan_htf_spot(htf)
            self._htf_fut_zones = entries
        elif self._htf_source == "option":
            # Bear zones from CE1 bars: seller traps on CE premium → buy CE
            df_ce = _bars_to_df(bars_override or self._bars_ce1)
            if not df_ce.empty and len(df_ce) >= 2:
                htf_ce = _resample_htf(df_ce, minutes)
                if len(htf_ce) >= 2:
                    _, bear_entries = scanner.scan_htf(htf_ce)
                    self._htf_bear_zones = bear_entries
            # Seller traps in CE2 bars → buy CE2
            df_ce2 = _bars_to_df(self._bars_ce2)
            if not df_ce2.empty and len(df_ce2) >= 2:
                htf_ce2 = _resample_htf(df_ce2, minutes)
                if len(htf_ce2) >= 2:
                    _, bear_entries_2 = scanner.scan_htf(htf_ce2)
                    self._htf_bear_zones_2 = bear_entries_2
            # Seller traps in PE1 bars → buy PE1
            df_pe = _bars_to_df(self._bars_pe1)
            if not df_pe.empty and len(df_pe) >= 2:
                htf_pe = _resample_htf(df_pe, minutes)
                if len(htf_pe) >= 2:
                    _, bull_entries = scanner.scan_htf(htf_pe)
                    self._htf_bull_zones = bull_entries
            # Seller traps in PE2 bars → buy PE2
            df_pe2 = _bars_to_df(self._bars_pe2)
            if not df_pe2.empty and len(df_pe2) >= 2:
                htf_pe2 = _resample_htf(df_pe2, minutes)
                if len(htf_pe2) >= 2:
                    _, bull_entries_2 = scanner.scan_htf(htf_pe2)
                    self._htf_bull_zones_2 = bull_entries_2

            # Intraday cascade fallback: if no CLOSE HTF zone exists for a leg,
            # scan today's 15-min bars to find intraday seller traps
            INTRADAY_MIN = 15
            atr = self._htf_atr_val or 100
            close_thresh = 2.0 * atr

            def _nearest_dist(zones, ltp):
                if not zones or ltp <= 0:
                    return float('inf')
                trapped = [z for z in zones if z["status"] == "TRAPPED"]
                if not trapped:
                    return float('inf')
                return min(abs(z.get("zone_trigger", ltp) - ltp) for z in trapped)

            def _intraday_zones(bars, ltp, existing):
                """Scan 15-min intraday bars; return new zones not already close to existing ones."""
                df = _bars_to_df(bars)
                if df.empty or len(df) < 2:
                    return []
                intra = _resample_htf(df, INTRADAY_MIN)
                if len(intra) < 2:
                    return []
                _, zones = scanner.scan_htf(intra)
                # Tag as intraday and deduplicate by zone_low
                existing_lows = {round(z.get("zone_low", 0), 1) for z in existing}
                new = []
                for z in zones:
                    z = dict(z, htf_label="15-min intraday")
                    if round(z.get("zone_low", 0), 1) not in existing_lows:
                        new.append(z)
                return new

            ltp_ce1 = self._ltp_cache.get("CE1") or 0
            if _nearest_dist(self._htf_bear_zones, ltp_ce1) > close_thresh:
                self._htf_bear_zones += _intraday_zones(self._bars_ce1, ltp_ce1, self._htf_bear_zones)

            ltp_ce2 = self._ltp_cache.get("CE2") or 0
            if _nearest_dist(self._htf_bear_zones_2, ltp_ce2) > close_thresh:
                self._htf_bear_zones_2 += _intraday_zones(self._bars_ce2, ltp_ce2, self._htf_bear_zones_2)

            ltp_pe1 = self._ltp_cache.get("PE1") or 0
            if _nearest_dist(self._htf_bull_zones, ltp_pe1) > close_thresh:
                self._htf_bull_zones += _intraday_zones(self._bars_pe1, ltp_pe1, self._htf_bull_zones)

            ltp_pe2 = self._ltp_cache.get("PE2") or 0
            if _nearest_dist(self._htf_bull_zones_2, ltp_pe2) > close_thresh:
                self._htf_bull_zones_2 += _intraday_zones(self._bars_pe2, ltp_pe2, self._htf_bull_zones_2)
        else:  # "spot": scan spot for direction, option bars for entry zones
            bars = bars_override or self._bars_spot
            df = _bars_to_df(bars)
            if df.empty or len(df) < 2:
                return
            htf = _resample_htf(df, minutes)
            if len(htf) < 2:
                return
            _, all_entries = scanner.scan_htf_spot(htf)
            self._htf_bear_zones = [e for e in all_entries if e.get("kind") == "BEAR"]
            self._htf_bull_zones = [e for e in all_entries if e.get("kind") == "BULL"]

            # Now scan option bars for option-premium-level zones (used for LTF entry)
            # Direction (CE vs PE) is gated by spot bear/bull zones above
            df_ce = _bars_to_df(self._bars_ce1)
            if not df_ce.empty and len(df_ce) >= 2:
                htf_ce = _resample_htf(df_ce, minutes)
                if len(htf_ce) >= 2:
                    _, bear_opt = scanner.scan_htf(htf_ce)
                    self._opt_bear_zones = bear_opt
            df_pe = _bars_to_df(self._bars_pe1)
            if not df_pe.empty and len(df_pe) >= 2:
                htf_pe = _resample_htf(df_pe, minutes)
                if len(htf_pe) >= 2:
                    _, bull_opt = scanner.scan_htf(htf_pe)
                    self._opt_bull_zones = bull_opt

    def _trapped_zone_count(self) -> int:
        if self._htf_source == "futures":
            return sum(1 for e in self._htf_fut_zones if e["status"] == "TRAPPED")
        return (sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED") +
                sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED"))

    # ── Candle close dispatch ─────────────────────────────────────────────────

    def _ltp_in_any_htf_zone(self, leg: str) -> bool:
        """
        Return True if the current live LTP for this leg is inside any TRAPPED HTF zone.
        Used to bypass the 5-min LTF gate so a fast opening move that sweeps both
        HTF and LTF bears simultaneously is caught on the very next 1-min close.
        """
        if self._position:
            return False   # already in trade — no need for fast scan
        ltp = self._ltp_cache.get(leg, 0)
        if ltp <= 0:
            return False
        if self._htf_source == "option":
            all_zones = (
                [z for z in self._htf_bear_zones if z["status"] == "TRAPPED"]
                if leg in ("CE1", "CE2") else
                [z for z in self._htf_bull_zones if z["status"] == "TRAPPED"]
            )
        elif self._htf_source == "futures":
            all_zones = [z for z in self._htf_fut_zones if z["status"] == "TRAPPED"]
        else:
            all_zones = []
        # Include a 2% buffer above zone_high: price may be in "waiting_retest" state
        # (trapped above zone_high, coming back). Keep the 1-min gate alive until retest.
        return any(z["zone_low"] <= ltp <= z["zone_high"] * 1.02 for z in all_zones)

    def _on_candle_close(self, leg: str, ts: datetime) -> None:
        # Futures-mode TSL: on every FUT candle close while in position,
        # check for new bear traps ABOVE entry → advance trail_sl
        if self._position and self._htf_source == "futures" and leg == "FUT":
            self._update_futures_tsl(ts)
            return
        if self._position:
            # Scale-in watcher: if we have a probe/add position, check for next stage.
            # Only for option/spot mode (position leg is CE1/CE2/PE1/PE2), not futures-mode.
            if (self._scale_in_enabled
                    and self._position.get("scale_stage") in ("probe", "added_5m")
                    and self._position.get("leg") in ("CE1", "CE2", "PE1", "PE2")):
                asyncio.get_event_loop().create_task(self._maybe_scale_in(self._position["leg"], ts))
            return

        # Refresh HTF scan on HTF boundary
        # "option": trigger on CE1 bar close (scans both CE1 and PE1)
        # "spot":   trigger on SPOT bar close
        # "futures": trigger on FUT bar close
        is_htf_boundary = ts.minute % self._htf_min == 0
        if is_htf_boundary:
            htf_trigger = (
                (self._htf_source == "option"  and leg == "CE1") or
                (self._htf_source == "spot"    and leg == "SPOT") or
                (self._htf_source == "futures" and leg == "FUT")
            )
            if htf_trigger:
                self._run_htf_scan()
                self._htf_atr_val = self._compute_htf_atr()
                prev_mode = self._intraday_mode
                self._check_zone_reachability()
                self._log.info(
                    "HTF scan: bear=%d bull=%d fut=%d ATR=%.2f intraday_mode=%s position=%s",
                    sum(1 for e in self._htf_bear_zones if e["status"] == "TRAPPED"),
                    sum(1 for e in self._htf_bull_zones if e["status"] == "TRAPPED"),
                    sum(1 for e in self._htf_fut_zones  if e["status"] == "TRAPPED"),
                    self._htf_atr_val, self._intraday_mode,
                    self._position["side"] if self._position else "none",
                )

        # On every LTF boundary — scan option premium bars inside HTF zones.
        # FAST-OPEN exception: if current LTP is already inside a TRAPPED HTF zone
        # (seeded from prev-day bars), scan on every 1-min close so we catch the
        # opening sweep that removes both HTF and LTF bears simultaneously.
        at_ltf_boundary = (ts.minute % self._ltf_min == 0)
        if not at_ltf_boundary and not self._ltp_in_any_htf_zone(leg):
            return

        if self._htf_source == "option":
            ce_leg = leg in ("CE1", "CE2")
            pe_leg = leg in ("PE1", "PE2")
            do_cascade_ce = ce_leg and self._intraday_mode_ce
            do_cascade_pe = pe_leg and self._intraday_mode_pe
            # Always run HTF normal scan first (uses 75-min near zones)
            self._ltf_scan_normal(leg, ts)
            # Also always dispatch cascade as fallback: if HTF zones exist but price never
            # enters them (different premium regime intraday), cascade finds today's zones.
            # _on_entry_signal's _notified_uids + self._position guard prevent double entry.
            asyncio.get_event_loop().create_task(
                self._cascade_scan(ts, cascade_ce=ce_leg, cascade_pe=pe_leg)
            )
        else:
            if self._intraday_mode:
                asyncio.get_event_loop().create_task(self._cascade_scan(ts))
            else:
                self._ltf_scan_normal(leg, ts)

    # ── LTF normal scan ───────────────────────────────────────────────────────

    def _ltf_scan_normal(self, leg: str, ts: datetime) -> None:
        """
        Normal mode: 5-min LTF scan on OPTION premium bars inside HTF spot zones.
        BEAR spot zones → scan CE1/CE2 option bars
        BULL spot zones → scan PE1/PE2 option bars
        FUTURES mode    → LTF also on FUTURES bars (institutions move via futures, not options).
                          Option bars are illiquid/gappy — unsuitable for trap detection.
                          Only ORDER and post-entry exits use options.
        """
        if self._htf_source == "futures":
            # Futures zones = BIAS only. LTF detection on OPTION bars (CE1/PE1).
            # Bear futures zone → CE bias → scan CE1 option bars for bear traps → buy CE.
            # Bull futures zone → PE bias → scan PE1 option bars for bear traps → buy PE.
            # FUT candle close does not trigger LTF — option ticks do.
            if leg == "FUT":
                return
            spot = self._spot_cache or 0.0
            fut_bear = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"
                        and e.get("kind", "BEAR") == "BEAR"
                        and e.get("zone_low", 0) <= spot <= e.get("zone_high", 0)]
            fut_bull = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"
                        and e.get("kind", "BEAR") == "BULL"
                        and spot >= e.get("zone_low", 0)]
            if leg == "CE1" and fut_bear:
                self._run_ltf_on("CE1", self._bars_ce1, fut_bear, "CE",
                                 require_closed=True, price_override=spot, all_cleared_entry=True)
            elif leg == "PE1" and fut_bull:
                self._run_ltf_on("PE1", self._bars_pe1, fut_bull, "PE",
                                 require_closed=True, price_override=spot, all_cleared_entry=True)
            return

        # BEAR zones → buy CE
        # htf_source="spot": direction gate = spot bear zones; entry zones = option premium zones
        spot_has_bear = bool([e for e in self._htf_bear_zones if e["status"] == "TRAPPED"])
        spot_has_bull = bool([e for e in self._htf_bull_zones if e["status"] == "TRAPPED"])

        if self._htf_source == "spot":
            bear_zones = [e for e in self._opt_bear_zones if e["status"] == "TRAPPED"] if spot_has_bear else []
            bull_zones = [e for e in self._opt_bull_zones if e["status"] == "TRAPPED"] if spot_has_bull else []
        bear_zones_2: list = []
        bull_zones_2: list = []
        if self._htf_source == "spot":
            bear_zones = [e for e in self._opt_bear_zones if e["status"] == "TRAPPED"] if spot_has_bear else []
            bull_zones = [e for e in self._opt_bull_zones if e["status"] == "TRAPPED"] if spot_has_bull else []
        else:
            ltp_ce = self._ltp_cache.get("CE1") or self._ltp_cache.get("CE2") or 0.0
            ltp_pe = self._ltp_cache.get("PE1") or self._ltp_cache.get("PE2") or 0.0
            def _alive_ce(z): return ltp_ce <= 0 or ltp_ce >= z.get("zone_low", 0)
            def _alive_pe(z): return ltp_pe <= 0 or ltp_pe >= z.get("zone_low", 0)
            bear_zones   = [e for e in self._htf_bear_zones   if e["status"] == "TRAPPED" and _alive_ce(e)]
            bear_zones_2 = [e for e in self._htf_bear_zones_2 if e["status"] == "TRAPPED" and _alive_ce(e)]
            bull_zones   = [e for e in self._htf_bull_zones   if e["status"] == "TRAPPED" and _alive_pe(e)]
            bull_zones_2 = [e for e in self._htf_bull_zones_2 if e["status"] == "TRAPPED" and _alive_pe(e)]

        _rc = self._htf_source not in ("option", "spot")

        # Gap bias (option-mode only, always ON): UP gap → CE trades only; DOWN gap → PE trades only.
        # Counter-trend leg is skipped — backtest shows 84.8% win vs 71.4% without bias.
        _bias_ce_only = self._gap_fired and self._gap_direction == "UP"
        _bias_pe_only = self._gap_fired and self._gap_direction == "DOWN"

        if leg == "CE1" and bear_zones and not _bias_pe_only:
            self._run_ltf_on("CE1", self._bars_ce1, bear_zones, "CE", require_closed=_rc)
        elif leg == "CE2" and bear_zones_2 and not _bias_pe_only:
            self._run_ltf_on("CE2", self._bars_ce2, bear_zones_2, "CE", require_closed=_rc)
        elif leg == "PE1" and bull_zones and not _bias_ce_only:
            self._run_ltf_on("PE1", self._bars_pe1, bull_zones, "PE", require_closed=_rc)
        elif leg == "PE2" and bull_zones_2 and not _bias_ce_only:
            self._run_ltf_on("PE2", self._bars_pe2, bull_zones_2, "PE", require_closed=_rc)

    def _run_ltf_on(self, leg_key: str, bars: List[dict],
                    htf_zones: List[dict], opt_type: str,
                    require_closed: bool = True,
                    price_override: float = 0.0,
                    all_cleared_entry: bool = False) -> None:
        """
        require_closed=True    : entry when LTF zone is CLOSED (normal mode)
        require_closed=False   : entry on TRAPPED (cascade mode)
        all_cleared_entry=True : entry when ALL LTF bear traps in option bars are CLOSED
                                 (futures-mode: sellers exhausted = enter immediately, no retest)
        """
        if not htf_zones or len(bars) < 3:
            return
        # Bug C fix: LTF scan today-only — historical seeded bars must not produce stale zones
        today = datetime.now(IST).date()
        today_bars = [b for b in bars
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return
        df = _bars_to_df(today_bars[-200:])
        current_price = price_override if price_override > 0 else (self._ltp_cache.get(leg_key, 0) or self._ltp_cache.get("SPOT", 0))

        # Futures-mode: ALL-CLEARED entry — when every LTF bear trap in option bars
        # is CLOSED (all option sellers have covered), sellers are exhausted → enter now.
        if all_cleared_entry:
            uid = _zone_uid(htf_zones[0]) if htf_zones else "fut_mode"
            if uid in self._notified_uids:
                return
            _, ltf_entries = scanner.scan_ltf(
                df,
                htf_zone_high=9999999,  # no zone bounds — scan full today's bars
                htf_zone_low=0,
                htf_ref_bar="",
                htf_trap_bar="",
                htf_target=0.0,
            )
            trapped_now  = [e for e in ltf_entries if e["status"] == "TRAPPED"]
            closed_today = [e for e in ltf_entries if e["status"] == "CLOSED"]
            self._log.info(
                "_run_ltf_on [%s] all_cleared: trapped=%d closed=%d",
                leg_key, len(trapped_now), len(closed_today),
            )
            if closed_today and not trapped_now:
                # All sellers out — pick the most recent closed zone as reference
                best = max(closed_today, key=lambda e: str(e.get("closed_on", "")))
                self._zone_ltf_status[uid] = "ltf_signal_all_cleared"
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg_key, opt_type, best, htf_zones[0])
                )
            return

        for zone in htf_zones:
            uid = _zone_uid(zone)
            if uid in self._notified_uids:
                continue
            if uid not in self._zone_ltf_status:
                self._zone_ltf_status[uid] = "watching"

            # Gate: price must be anywhere inside [zone_low, zone_high] — full zone valid.
            # No 1/3 restriction: LTF traps (existing or new) anywhere in the zone are entry candidates.
            z_low  = zone["zone_low"]
            z_high = zone["zone_high"]
            if current_price > 0 and (current_price < z_low or current_price > z_high):
                continue

            # 15m intermediate gate — applies to BOTH normal and cascade paths.
            # A 15m zone must be CLOSED (SL hit + price returned to zone_high) before
            # the 5m LTF scan fires.
            # Normal (require_closed=True): 15m zone must be inside HTF zone bounds.
            # Cascade (require_closed=False): no HTF zone bound — any CLOSED 15m zone today.
            if require_closed:
                mtf = self._find_closed_15m_zone(today_bars, zone)
            else:
                mtf = self._find_closed_15m_zone(today_bars, None)

            if mtf is None:
                status_key = "waiting_15m"
                if self._zone_ltf_status.get(uid) != status_key:
                    self._zone_ltf_status[uid] = status_key
                    self._log.info(
                        "_run_ltf_on [%s] uid=%s: waiting for 15m CLOSED zone "
                        "(mode=%s zone_high=%.1f zone_low=%.1f)",
                        leg_key, uid,
                        "normal" if require_closed else "cascade",
                        z_high, z_low,
                    )
                continue

            ltf_zone_high = mtf["zone_high"]
            ltf_zone_low  = mtf["zone_low"]
            self._log.info(
                "_run_ltf_on [%s] uid=%s: 15m gate PASSED (mode=%s) "
                "(15m zone_high=%.1f zone_low=%.1f sl=%.1f closed_on=%s)",
                leg_key, uid,
                "normal" if require_closed else "cascade",
                ltf_zone_high, ltf_zone_low,
                mtf.get("sl", 0), str(mtf.get("closed_on", ""))[:16],
            )

            # Scale-in probe: enter 1 lot immediately on 15m zone_high touch.
            # The full position is built later via _maybe_scale_in on lower-TF confirmation.
            if self._scale_in_enabled and not self._position:
                if self._zone_scale_state.get(uid) != "probe":
                    self._zone_scale_state[uid] = "probe"
                    self._zone_ltf_status[uid] = "probe_signal"
                    probe_qty = self._lot_size * 1
                    asyncio.get_event_loop().create_task(
                        self._on_entry_signal(leg_key, opt_type, mtf, zone,
                                              qty_override=probe_qty, stage="probe")
                    )
                    self._log.info(
                        "SCALE-IN PROBE triggered [%s] uid=%s: %d lots @%.1f",
                        leg_key, uid, probe_qty, ltf_zone_high,
                    )
                    return

            _, ltf_entries = scanner.scan_ltf(
                df,
                htf_zone_high=ltf_zone_high,
                htf_zone_low=ltf_zone_low,
                htf_ref_bar=str(zone.get("ref_ts", "")),
                htf_trap_bar=str(zone.get("trapped_on", zone.get("closed_on", ""))),
                htf_target=zone.get("sl", 0.0),
            )
            best = scanner.select_best_ltf_entry(ltf_entries)  # CLOSED only (both modes)
            if best:
                # Entry is at zone_high = LTF sellers' entry level (C1.LOW).
                # After TRAPPED (price shot above C1.HIGH), we wait for price to pull
                # back to zone_high — that re-test of sellers' entry IS our entry signal.
                # Both CE and PE use same check: option premium must retrace to zone_high.
                entry_level = best["zone_high"]
                tol = entry_level * 0.005  # 0.5% tolerance for tick noise
                if current_price > entry_level + tol:
                    # Price still above sellers' entry — trap fired but retest not yet.
                    self._zone_ltf_status[uid] = "waiting_retest"
                    self._log.debug(
                        "_run_ltf_on [%s] WAITING RETEST: ltp=%.2f > zone_high=%.2f",
                        leg_key, current_price, entry_level,
                    )
                    continue
                self._zone_ltf_status[uid] = "ltf_signal"
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg_key, opt_type, best, zone)
                )
                return

    def _find_closed_15m_zone(self, today_bars: List[dict],
                               htf_zone: Optional[dict]) -> Optional[dict]:
        """
        Find the most-recently CLOSED 15m seller-trap zone.
        CLOSED = SL hit (TRAPPED) + price subsequently returned to zone_high.
        htf_zone: if provided, zone_high must be within [htf_zone_low, htf_zone_high].
                  if None (cascade mode), any CLOSED 15m zone qualifies.
        """
        if len(today_bars) < 2:
            return None
        df = _bars_to_df(today_bars)
        mtf = _resample_htf(df, 15)
        if len(mtf) < 2:
            return None
        _, entries = scanner.scan_htf(mtf)
        if htf_zone is not None:
            closed = [
                e for e in entries
                if e["status"] == "CLOSED"
                and e["zone_high"] >= htf_zone["zone_low"]
                and e["zone_high"] <= htf_zone["zone_high"]
            ]
        else:
            closed = [e for e in entries if e["status"] == "CLOSED"]
        if not closed:
            return None
        return max(closed, key=lambda e: str(e.get("closed_on", "")))

    async def _maybe_scale_in(self, leg_key: str, ts: datetime) -> None:
        """Add to an existing probe position on 5m closed zone (3 lots) and 1m breach (rest)."""
        pos = self._position
        if not pos:
            return
        if pos.get("leg") != leg_key:
            return
        stage = pos.get("scale_stage")
        if stage not in ("probe", "added_5m"):
            return
        if pos.get("t1_hit"):
            return

        htf_zone = pos.get("htf_zone")
        if not htf_zone:
            return
        z_low = htf_zone.get("zone_low", 0)
        z_high = htf_zone.get("zone_high", 0)
        current_price = self._ltp_cache.get(leg_key, 0)
        if current_price <= 0 or current_price < z_low or current_price > z_high:
            return

        opt_type = pos["side"]
        bars = {
            "CE1": self._bars_ce1, "CE2": self._bars_ce2,
            "PE1": self._bars_pe1, "PE2": self._bars_pe2,
        }.get(leg_key, [])
        if len(bars) < 3:
            return

        today = datetime.now(IST).date()
        today_bars = [b for b in bars
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return

        # Stage 2: add 3 lots when a CLOSED 5m zone exists inside the 15m context.
        if stage == "probe" and not pos.get("scale_5m_added"):
            mtf = self._find_closed_15m_zone(today_bars, None)
            if mtf:
                df = _bars_to_df(today_bars[-200:])
                _, ltf_entries = scanner.scan_ltf(
                    df,
                    htf_zone_high=mtf["zone_high"],
                    htf_zone_low=mtf["zone_low"],
                    htf_ref_bar=str(htf_zone.get("ref_ts", "")),
                    htf_trap_bar=str(htf_zone.get("trapped_on", htf_zone.get("closed_on", ""))),
                    htf_target=htf_zone.get("sl", 0.0),
                )
                best = scanner.select_best_ltf_entry(ltf_entries)
                if best:
                    add_qty = self._lot_size * 3
                    ok = await self._add_to_position(add_qty, "SCALE_5M", best)
                    if ok:
                        pos["scale_stage"] = "added_5m"
                        pos["scale_5m_added"] = True
                        self._persist_position()

        # Stage 3: add remaining lots on 1m zone-high breach.
        if pos.get("scale_stage") == "added_5m" and not pos.get("scale_1m_added"):
            if self._is_1m_zone_breached(today_bars, htf_zone, opt_type, current_price):
                target_total = self._lot_size * self._lot_mul
                add_qty = max(0, target_total - pos["total_qty"])
                if add_qty > 0:
                    ok = await self._add_to_position(add_qty, "SCALE_1M", htf_zone)
                    if ok:
                        pos["scale_stage"] = "full"
                        pos["scale_1m_added"] = True
                        uid = pos.get("htf_zone_uid")
                        if uid:
                            self._notified_uids.add(uid)
                        self._persist_position()
                        self._log.info(
                            "SCALE-IN COMPLETE [%s] uid=%s: total_qty=%d avg=%.2f",
                            leg_key, uid, pos["total_qty"], pos["entry_price"],
                        )

    def _is_1m_zone_breached(self, today_bars: List[dict], htf_zone: dict,
                             opt_type: str, current_price: float) -> bool:
        """Return True if the most recent trapped 1m zone high is touched/breached."""
        if len(today_bars) < 3 or current_price <= 0:
            return False
        df = _bars_to_df(today_bars[-60:])
        if len(df) < 2:
            return False
        _, entries = scanner.scan_htf(df)
        if not entries:
            return False
        # Use the most recent trapped zone inside today's 1m bars.
        trapped = [e for e in entries if e["status"] == "TRAPPED"]
        if not trapped:
            return False
        zone = max(trapped, key=lambda e: str(e.get("trapped_on", "")))
        z_high = zone.get("zone_high", 0)
        if z_high <= 0:
            return False
        # Require price to have reached the 1m sellers' entry level (zone_high).
        tol = z_high * 0.005
        return current_price >= z_high - tol

    def _run_ltf_futures_mode(self, leg_key: str, bars: List[dict],
                               htf_zones: List[dict], opt_type: str) -> None:
        """
        Futures-mode LTF: scan FUTURES bars for 5-min traps (same price domain as HTF).
        CrudeOil institutions move via futures; option bars are illiquid/gappy.

        Both HTF and LTF use futures bars — zone prices are consistent.
        Normal scan_ltf zone-filter works because bars and zones share futures price units.

        Compared to option-mode _run_ltf_on:
          - No dual-confirm needed (LTF bars ARE the futures signal)
          - opt_type is always "CE" (bear trap → buy CE); PE logic TBD
          - tracking_leg in position is CE1/PE1 (option ticks for SL/T1/trail)
        """
        if not htf_zones or len(bars) < 3:
            return
        today = datetime.now(IST).date()
        today_bars = [b for b in bars
                      if pd.to_datetime(b.get("datetime", "")).date() == today]
        if len(today_bars) < 3:
            return
        df = _bars_to_df(today_bars[-200:])
        current_spot = self._ltp_cache.get("FUT", 0) or self._ltp_cache.get("SPOT", 0)
        # Recent 5-min highs/lows from today's bars (last 6 bars = 30 min of context)
        recent_highs = df["high"].iloc[-6:].tolist() if len(df) >= 6 else df["high"].tolist()
        recent_lows  = df["low"].iloc[-6:].tolist()  if len(df) >= 6 else df["low"].tolist()

        for zone in htf_zones:
            uid = _zone_uid(zone)
            if uid in self._notified_uids:
                continue
            if uid not in self._zone_ltf_status:
                self._zone_ltf_status[uid] = "watching"

            z_low  = zone["zone_low"]
            z_high = zone["zone_high"]
            width  = z_high - z_low

            # Zone invalidation: if price already broke through the zone in the wrong direction
            # today, the trapped traders were actually RIGHT — skip this zone.
            # CE (bear zone): bears were right if price broke below zone_low today
            # PE (bull zone): bulls were right if price broke above zone_high today
            if opt_type == "CE" and recent_lows and min(recent_lows) < z_low:
                self._log.debug(
                    "zone %s invalidated: price broke below zone_low=%.1f (bears were right)",
                    uid, z_low)
                continue
            if opt_type == "PE" and recent_highs and max(recent_highs) > z_high:
                self._log.debug(
                    "zone %s invalidated: price broke above zone_high=%.1f (bulls were right)",
                    uid, z_high)
                continue

            # Proximity gate: full zone (removed 2/3 restriction).
            # Direction-of-approach + "clean old zones" logic handles quality filtering.
            # CE: price must be inside [zone_low, zone_high] approaching from above
            # PE: price must be inside [zone_low, zone_high] approaching from below
            if current_spot > 0 and (current_spot < z_low or current_spot > z_high):
                continue

            # Direction-of-approach gate:
            # CE (bear trap): price must have come FROM ABOVE zone_high recently
            # PE (bull trap): price must have come FROM BELOW zone_low recently
            if opt_type == "CE":
                if recent_highs and max(recent_highs) <= z_high:
                    self._log.debug(
                        "zone %s skipped: CE wrong direction — never above zone_high=%.1f",
                        uid, z_high)
                    continue
            else:  # PE
                if recent_lows and min(recent_lows) >= z_low:
                    self._log.debug(
                        "zone %s skipped: PE wrong direction — never below zone_low=%.1f",
                        uid, z_low)
                    continue

            # Scan 5-min bars inside the HTF zone
            scan_fn = scanner.scan_ltf_bull if opt_type == "PE" else scanner.scan_ltf
            _, ltf_entries = scan_fn(
                df,
                htf_zone_high=zone["zone_high"],
                htf_zone_low=zone["zone_low"],
                htf_ref_bar=str(zone.get("ref_ts", "")),
                htf_trap_bar=str(zone.get("trapped_on", zone.get("closed_on", ""))),
                htf_target=zone.get("sl", 0.0),
            )
            # Pick the FIRST trap market reaches as it enters the HTF zone:
            # CE (market coming DOWN): highest zone_high first — bears who entered highest
            #   are the first ones market returns to; their covering = reversal UP = buy CE.
            # PE (market coming UP): lowest zone_low first — bulls who entered lowest
            #   are the first ones market returns to; their covering = reversal DOWN = buy PE.
            trapped_ltf = [e for e in ltf_entries if e.get("status") == "TRAPPED"]
            if trapped_ltf:
                if opt_type == "CE":
                    trapped_ltf = [max(trapped_ltf, key=lambda e: e.get("zone_high", 0))]
                else:
                    trapped_ltf = [min(trapped_ltf, key=lambda e: e.get("zone_low", 0))]
            best = scanner.select_fresh_ltf_entry(trapped_ltf, opt_type=opt_type)
            if best:
                # Retest gate: wait for futures price to pull back to sellers'/buyers' entry.
                # CE (bear zone): sellers entered at zone_high (C1.LOW). After TRAPPED
                #   (price > C1.HIGH), wait for price to come back DOWN to zone_high.
                # PE (bull zone): buyers entered at zone_low (C1.HIGH of bull zone). After
                #   TRAPPED (price < C1.LOW), wait for price to come back UP to zone_low.
                tol = 0.005  # 0.5%
                if opt_type == "CE":
                    entry_level = best["zone_high"]
                    retest_ok = current_spot <= entry_level * (1 + tol)
                else:
                    entry_level = best["zone_low"]
                    retest_ok = current_spot >= entry_level * (1 - tol)
                if not retest_ok:
                    self._zone_ltf_status[uid] = "waiting_retest"
                    self._log.debug(
                        "_run_ltf_futures_mode [%s/%s] WAITING RETEST: spot=%.1f entry_level=%.1f",
                        leg_key, opt_type, current_spot, entry_level,
                    )
                    continue
                self._zone_ltf_status[uid] = "ltf_signal"
                asyncio.get_event_loop().create_task(
                    self._on_entry_signal(leg_key, opt_type, best, zone)
                )
                return

    # ── Intraday cascade ──────────────────────────────────────────────────────

    async def _cascade_scan(self, ts: datetime,
                             cascade_ce: bool = True, cascade_pe: bool = True) -> None:
        """
        Intraday cascade: no 75-min zone TRAPPED (or zone too far).
        Per-leg for htf_source=option: cascade_ce/cascade_pe control which leg scans.
        1. Resample today's completed 1m bars to 15-min (drop current incomplete bar)
        2. scan_htf on those completed 15-min bars
        3. If a 15-min zone TRAPPED → scan_ltf on completed 5-min option bars
        4. Entry fires on TRAPPED status (not CLOSED — cascade rule)
        """
        today = datetime.now(IST).date()
        # Current 15-min bucket start — bars in this bucket are still forming
        cur_15m_start = ts.replace(second=0, microsecond=0)
        cur_15m_start = cur_15m_start.replace(minute=(cur_15m_start.minute // self._cascade_min) * self._cascade_min)

        def _complete_today(src: List[dict]) -> List[dict]:
            """Return today's bars that belong to a COMPLETED 15-min bucket."""
            return [
                b for b in src
                if (pd.to_datetime(b["datetime"]).date() == today and
                    pd.to_datetime(b["datetime"]) < cur_15m_start)
            ]

        if self._htf_source == "futures":
            today_bars = _complete_today(self._bars_fut)
            if len(today_bars) < 4:
                return
            self._run_htf_scan(bars_override=today_bars, minutes_override=self._cascade_min)
            zones_15m = [e for e in self._htf_fut_zones if e["status"] == "TRAPPED"]
            self._run_ltf_on("FUT", self._bars_fut, zones_15m, "CE", require_closed=False)
        elif self._htf_source == "option":
            # Per-leg: only scan the leg(s) that are in cascade mode
            bear_15: list = []
            bull_15: list = []
            if cascade_ce:
                today_ce = _complete_today(self._bars_ce1)
                self._log.info("CE cascade scan: today_bars=%d", len(today_ce))
                if len(today_ce) >= 4:
                    df_ce = _bars_to_df(today_ce)
                    htf_ce = _resample_htf(df_ce, self._cascade_min)
                    if len(htf_ce) >= 2:
                        _, be = scanner.scan_htf(htf_ce)
                        bear_15 = [e for e in be if e["status"] == "TRAPPED"]
                        self._log.info("CE cascade: %d/%d zones TRAPPED from %d 15m candles",
                                       len(bear_15), len(be), len(htf_ce))
            if cascade_pe:
                today_pe = _complete_today(self._bars_pe1)
                self._log.info("PE cascade scan: today_bars=%d", len(today_pe))
                if len(today_pe) >= 4:
                    df_pe = _bars_to_df(today_pe)
                    htf_pe = _resample_htf(df_pe, self._cascade_min)
                    if len(htf_pe) >= 2:
                        _, bu = scanner.scan_htf(htf_pe)
                        bull_15 = [e for e in bu if e["status"] == "TRAPPED"]
                        self._log.info("PE cascade: %d/%d zones TRAPPED from %d 15m candles",
                                       len(bull_15), len(bu), len(htf_pe))
            if bear_15:
                bear_15 = sorted(bear_15, key=lambda z: z.get("zone_high", 0), reverse=True)
                self._run_ltf_on("CE1", self._bars_ce1, bear_15, "CE", require_closed=False)
                self._run_ltf_on("CE2", self._bars_ce2, bear_15, "CE", require_closed=False)
            if bull_15:
                bull_15 = sorted(bull_15, key=lambda z: z.get("zone_high", 0), reverse=True)
                self._run_ltf_on("PE1", self._bars_pe1, bull_15, "PE", require_closed=False)
                self._run_ltf_on("PE2", self._bars_pe2, bull_15, "PE", require_closed=False)
        else:  # "spot" cascade: spot 15-min for direction, option 15-min for entry zones
            today_spot = _complete_today(self._bars_spot)
            if len(today_spot) < 4:
                return
            df_spot = _bars_to_df(today_spot)
            htf_spot_15 = _resample_htf(df_spot, self._cascade_min)
            if len(htf_spot_15) < 2:
                return
            _, all_15 = scanner.scan_htf_spot(htf_spot_15)
            spot_bear = [e for e in all_15 if e.get("kind") == "BEAR" and e["status"] == "TRAPPED"]
            spot_bull = [e for e in all_15 if e.get("kind") == "BULL" and e["status"] == "TRAPPED"]
            self._log.info("spot cascade: bear=%d bull=%d from %d 15m candles",
                           len(spot_bear), len(spot_bull), len(htf_spot_15))

            # CE side: spot bear trap (bears squeezed → market up → CE rises → BUY CE)
            if spot_bear:
                today_ce = _complete_today(self._bars_ce1)
                if len(today_ce) >= 4:
                    df_ce = _bars_to_df(today_ce)
                    htf_ce_15 = _resample_htf(df_ce, self._cascade_min)
                    if len(htf_ce_15) >= 2:
                        _, ce_ents = scanner.scan_htf(htf_ce_15)
                        bear_opt_15 = [e for e in ce_ents if e["status"] == "TRAPPED"]
                        self._log.info("CE opt cascade: %d/%d zones TRAPPED from %d 15m candles ltp=%.1f",
                                       len(bear_opt_15), len(ce_ents), len(htf_ce_15),
                                       self._ltp_cache.get("CE1") or 0)
                        # Update live display so LEG shows today's zones not stale historical
                        if bear_opt_15:
                            self._opt_bear_zones = ce_ents  # all (incl non-trapped) for zone table
                            self._run_ltf_on("CE1", self._bars_ce1, bear_opt_15, "CE", require_closed=False)
                            self._run_ltf_on("CE2", self._bars_ce2, bear_opt_15, "CE", require_closed=False)
                        else:
                            self._log.info("CE opt cascade: no trapped zones — ltp=%.1f zones=%s",
                                           self._ltp_cache.get("CE1") or 0,
                                           [(round(e.get("zone_low",0),1), round(e.get("zone_high",0),1),
                                             e["status"]) for e in ce_ents[-5:]])

            # PE side: spot bull trap (bulls squeezed → market down → PE rises → BUY PE)
            if spot_bull:
                today_pe = _complete_today(self._bars_pe1)
                if len(today_pe) >= 4:
                    df_pe = _bars_to_df(today_pe)
                    htf_pe_15 = _resample_htf(df_pe, self._cascade_min)
                    if len(htf_pe_15) >= 2:
                        _, pe_ents = scanner.scan_htf(htf_pe_15)
                        bull_opt_15 = [e for e in pe_ents if e["status"] == "TRAPPED"]
                        self._log.info("PE opt cascade: %d/%d zones TRAPPED from %d 15m candles ltp=%.1f",
                                       len(bull_opt_15), len(pe_ents), len(htf_pe_15),
                                       self._ltp_cache.get("PE1") or 0)
                        if bull_opt_15:
                            self._opt_bull_zones = pe_ents  # update display
                            self._run_ltf_on("PE1", self._bars_pe1, bull_opt_15, "PE", require_closed=False)
                            self._run_ltf_on("PE2", self._bars_pe2, bull_opt_15, "PE", require_closed=False)
                        else:
                            self._log.info("PE opt cascade: no trapped zones — ltp=%.1f zones=%s",
                                           self._ltp_cache.get("PE1") or 0,
                                           [(round(e.get("zone_low",0),1), round(e.get("zone_high",0),1),
                                             e["status"]) for e in pe_ents[-5:]])
