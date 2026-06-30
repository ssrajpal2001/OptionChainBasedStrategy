"""
scripts/btc_live_trader.py
==========================
BTC Perpetual Futures Live Trader — Delta Exchange India

Runs the same 4h/30m trap-scanner strategy validated in backtest:
  HTF=4h · Sub=30m · SL=$500pts · Floor=$200pts · Cap=$1000pts · 20 lots

- NO day-session concept: BTC is 24/7 positional, trade closes in profit or loss,
  fresh trade taken thereafter. No force-close at any hour.
- 1 trade at a time. New signal is IGNORED while a position is open.
- Zone cooldown: zone that caused a losing SL is skipped for 1 day.
- Exit monitoring: polls Delta REST every 10s for SL / profit-floor / profit-cap.
- Logs to logs/btc_live_YYYY-MM-DD.log

Launch via PM2:
    pm2 start scripts/btc_live_trader.py --name btc-trader --interpreter python3

Config stored in data/clients.db table btc_live_config (run setup_btc_live.py first).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional

import pandas as pd
import requests

# ── path so we can import project modules ──────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from strategies.trap_scanner import scanner
from scripts.btc_backtest import (
    _resample, _zone_trigger_price, _init_sl,
    CONTRACT_SIZE, MAX_PER_REQ,
)

# ── constants ──────────────────────────────────────────────────────────────────
DELTA_BASE   = "https://api.india.delta.exchange"
SYMBOL       = "BTCUSD"
DB_PATH      = os.path.join(_ROOT, "data", "clients.db")
LOG_DIR      = os.path.join(_ROOT, "logs")


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _setup_log() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"btc_live_{date.today()}.log")
    fmt = "%(asctime)s %(levelname)-8s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt,
                        handlers=[
                            logging.StreamHandler(sys.stdout),
                            RotatingFileHandler(log_file, maxBytes=20*1024*1024, backupCount=3),
                        ])
    return logging.getLogger("btc_live")

log = _setup_log()


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT key, value FROM btc_live_config").fetchall()
    conn.close()
    return {k: json.loads(v) for k, v in rows}


# ─────────────────────────────────────────────────────────────────────────────
# Delta Exchange REST helpers
# ─────────────────────────────────────────────────────────────────────────────

class DeltaClient:
    def __init__(self, api_key: str, api_secret: str):
        self._key    = api_key
        self._secret = api_secret
        self._pid: Optional[int] = None   # BTCUSD product_id (cached)

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        ts  = str(int(time.time()))
        msg = f"{method.upper()}{ts}{path}{body}"
        sig = hmac.new(self._secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
        return {
            "api-key"      : self._key,
            "timestamp"    : ts,
            "signature"    : sig,
            "Content-Type" : "application/json",
            "User-Agent"   : "btc-live-trader",
        }

    def get(self, path: str, params: dict = None) -> dict:
        r = requests.get(DELTA_BASE + path, params=params,
                         headers=self._sign("GET", path), timeout=10)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload)
        r = requests.post(DELTA_BASE + path, data=body,
                          headers=self._sign("POST", path, body), timeout=10)
        r.raise_for_status()
        return r.json()

    def delete(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload)
        r = requests.delete(DELTA_BASE + path, data=body,
                            headers=self._sign("DELETE", path, body), timeout=10)
        r.raise_for_status()
        return r.json()

    def product_id(self) -> int:
        if self._pid:
            return self._pid
        resp = self.get("/v2/products", {"contract_type": "perpetual_futures"})
        for p in (resp.get("result") or []):
            if p.get("symbol") == SYMBOL:
                self._pid = int(p["id"])
                log.info("BTCUSD product_id = %d", self._pid)
                return self._pid
        raise RuntimeError("BTCUSD perpetual not found in Delta products")

    def mark_price(self) -> float:
        resp = self.get(f"/v2/tickers/{SYMBOL}")
        return float((resp.get("result") or {}).get("mark_price", 0))

    def place_market(self, side: str, size: int, paper: bool = False) -> Optional[str]:
        """side='buy'|'sell', size=lots. Returns order_id or None (paper)."""
        if paper:
            log.info("[PAPER] %s %d lots BTCUSD @ market", side.upper(), size)
            return f"PAPER-{int(time.time())}"
        pid = self.product_id()
        resp = self.post("/v2/orders", {
            "product_id"    : pid,
            "product_symbol": SYMBOL,
            "side"          : side,
            "order_type"    : "market_order",
            "size"          : size,
            "time_in_force" : "ioc",
        })
        result = resp.get("result") or {}
        oid = str(result.get("id", ""))
        log.info("ORDER %s %d lots → id=%s state=%s",
                 side.upper(), size, oid, result.get("state", "?"))
        return oid or None

    def get_position(self) -> dict:
        """Returns {"size": <int>, "entry_price": <float>, "side": "buy"|"sell"|None}"""
        try:
            resp = self.get("/v2/positions/margined")
            for p in (resp.get("result") or []):
                if p.get("product", {}).get("symbol") == SYMBOL:
                    size = int(p.get("size", 0) or 0)
                    ep   = float(p.get("entry_price", 0) or 0)
                    side = "buy" if size > 0 else ("sell" if size < 0 else None)
                    return {"size": abs(size), "entry_price": ep, "side": side}
        except Exception as e:
            log.warning("get_position error: %s", e)
        return {"size": 0, "entry_price": 0.0, "side": None}

    def close_position(self, size: int, current_side: str, paper: bool = False) -> None:
        close_side = "sell" if current_side == "buy" else "buy"
        self.place_market(close_side, size, paper=paper)


# ─────────────────────────────────────────────────────────────────────────────
# Candle data helpers
# ─────────────────────────────────────────────────────────────────────────────

def fetch_recent_1m(n_candles: int) -> pd.DataFrame:
    """Fetch the latest n_candles 1m bars from Delta Exchange."""
    end_ts   = int(time.time())
    start_ts = end_ts - n_candles * 60
    all_c: list = []
    current_end = end_ts
    while current_end > start_ts and len(all_c) < n_candles:
        try:
            r = requests.get(DELTA_BASE + "/v2/history/candles",
                             params={"symbol": SYMBOL, "resolution": "1m",
                                     "start": start_ts, "end": current_end},
                             timeout=15)
            r.raise_for_status()
            candles = r.json().get("result", [])
        except Exception as e:
            log.warning("Candle fetch error: %s", e)
            break
        if not candles:
            break
        all_c.extend(candles)
        oldest = min(c["time"] for c in candles)
        if oldest <= start_ts:
            break
        current_end = oldest - 60
        time.sleep(0.15)

    if not all_c:
        return pd.DataFrame()
    df = pd.DataFrame(all_c)
    df["datetime"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Zone detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_signal(df_1m: pd.DataFrame, htf_min: int, sub_min: int) -> Optional[dict]:
    """
    Run HTF + sub-zone detection on the provided 1m bars.
    Returns a signal dict or None.
    """
    if len(df_1m) < htf_min + sub_min:
        return None

    htf_bars = _resample(df_1m, htf_min)
    _, htf_entries = scanner.scan_htf_spot(htf_bars) if len(htf_bars) >= 3 else (None, [])
    htf_zones = [e for e in (htf_entries or []) if e.get("status") == "CLOSED"]
    if not htf_zones:
        return None

    sub_bars = _resample(df_1m, sub_min)
    _, sub_ents = scanner.scan_htf_spot(sub_bars) if len(sub_bars) >= 3 else (None, [])
    sub_zones = [e for e in (sub_ents or []) if e.get("status") == "CLOSED"]

    for htf_z in htf_zones:
        zh = float(htf_z["zone_high"])
        zl = float(htf_z["zone_low"])
        kind = htf_z.get("kind", "BEAR")
        is_long = (kind == "BEAR")
        zone_key = f"{zl:.0f}-{zh:.0f}"

        trigger = _zone_trigger_price(htf_z)
        t1      = float(htf_z.get("sl", 0))
        if t1 <= 0:
            continue
        if is_long and t1 <= trigger:
            continue
        if not is_long and t1 >= trigger:
            continue

        if sub_zones:
            sub_in = [s for s in sub_zones
                      if s.get("kind") == kind
                      and float(s.get("zone_high", 0)) <= zh + (zh - zl) * 0.1
                      and float(s.get("zone_low",  0)) >= zl - (zh - zl) * 0.1]
            if not sub_in:
                continue

        return {
            "zone_key" : zone_key,
            "kind"     : kind,
            "is_long"  : is_long,
            "trigger"  : trigger,
            "t1"       : t1,
            "sl_init"  : _init_sl(htf_z, 0),
            "zh"       : zh,
            "zl"       : zl,
        }
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main trader loop
# ─────────────────────────────────────────────────────────────────────────────

class BtcLiveTrader:
    def __init__(self, cfg: dict):
        self.cfg           = cfg
        self.paper         = bool(cfg.get("paper_mode", True))
        self.lots          = int(cfg["lots"])
        self.htf_min       = int(cfg["htf_min"])
        self.sub_min       = int(cfg["sub_min"])
        self.sl_buf        = float(cfg["sl_buf"])
        self.floor_pts     = float(cfg["profit_floor_pts"])
        self.cap_pts       = float(cfg["profit_cap_pts"])
        self.lookback_days = int(cfg["lookback_days"])
        self.cooldown_days = int(cfg.get("cooldown_days", 1))
        self.max_day_loss  = float(cfg.get("max_daily_loss_usdt", 300))

        self.client        = DeltaClient(cfg["api_key"], cfg["api_secret"])

        # Open position state
        self._pos: Optional[dict] = None   # {side, entry_price, sl, t1, cap, floor_active, zone_key}

        # Zone cooldown {zone_key: date_str of last SL}
        self._sl_zones: dict = {}

        # Daily P&L guard
        self._day_str    = date.today().isoformat()
        self._day_pnl    = 0.0

        # Signal scan interval: rescan after every sub_min candle close
        self._last_scan_ts = 0

        log.info("BtcLiveTrader started | paper=%s lots=%d HTF=%dm sub=%dm "
                 "SL=$%d floor=$%d cap=$%d",
                 self.paper, self.lots, self.htf_min, self.sub_min,
                 self.sl_buf, self.floor_pts, self.cap_pts)
        log.info("Max daily loss cap: $%.2f USDT", self.max_day_loss)

    # ── position entry ────────────────────────────────────────────────────────

    def _enter(self, signal: dict, price: float) -> None:
        is_long   = signal["is_long"]
        entry     = price
        sl        = (entry - self.sl_buf) if is_long else (entry + self.sl_buf)
        t1        = signal["t1"]
        cap_price = (entry + self.cap_pts) if is_long else (entry - self.cap_pts)

        side = "buy" if is_long else "sell"
        oid  = self.client.place_market(side, self.lots, paper=self.paper)
        if oid is None:
            log.error("Order placement failed — skipping entry")
            return

        self._pos = {
            "side"         : side,
            "is_long"      : is_long,
            "entry_price"  : entry,
            "sl"           : sl,
            "t1"           : t1,
            "cap_price"    : cap_price,
            "floor_active" : False,
            "zone_key"     : signal["zone_key"],
            "order_id"     : oid,
            "entry_time"   : datetime.now(timezone.utc).isoformat(),
        }
        log.info("ENTERED %s zone=%s entry=%.0f sl=%.0f t1=%.0f cap=%.0f",
                 side.upper(), signal["zone_key"], entry, sl, t1, cap_price)

    # ── position exit ─────────────────────────────────────────────────────────

    def _exit(self, reason: str, price: float) -> None:
        if not self._pos:
            return
        pos      = self._pos
        is_long  = pos["is_long"]
        pnl_pts  = (price - pos["entry_price"]) if is_long else (pos["entry_price"] - price)
        pnl_usdt = round(pnl_pts * CONTRACT_SIZE * self.lots, 4)

        close_side = "sell" if is_long else "buy"
        self.client.place_market(close_side, self.lots, paper=self.paper)

        self._day_pnl += pnl_usdt
        log.info("EXIT [%s] zone=%s entry=%.0f exit=%.0f pts=%+.0f P&L=$%.2f USDT | day_pnl=$%.2f",
                 reason, pos["zone_key"], pos["entry_price"], price,
                 pnl_pts, pnl_usdt, self._day_pnl)

        if reason == "SL" and pnl_usdt < 0:
            self._sl_zones[pos["zone_key"]] = date.today().isoformat()

        self._pos = None

    # ── SL / exit check ───────────────────────────────────────────────────────

    def _check_exit(self, price: float) -> None:
        if not self._pos:
            return
        p = self._pos
        is_long = p["is_long"]

        running_pts = (price - p["entry_price"]) if is_long else (p["entry_price"] - price)

        # Break-even floor: lock SL at entry once price moves floor_pts in favour
        if self.floor_pts > 0 and not p["floor_active"] and running_pts >= self.floor_pts:
            p["sl"]           = p["entry_price"]
            p["floor_active"] = True
            log.info("BREAK-EVEN locked at %.0f (moved %.0f pts)", p["entry_price"], running_pts)

        # Trailing SL: trail behind price by sl_buf
        if is_long:
            new_trail = price - self.sl_buf
            if new_trail > p["sl"]:
                p["sl"] = new_trail
        else:
            new_trail = price + self.sl_buf
            if new_trail < p["sl"]:
                p["sl"] = new_trail

        # Profit cap
        if self.cap_pts > 0 and running_pts >= self.cap_pts:
            self._exit("TARGET", price)
            return

        # T1
        if is_long and price >= p["t1"]:
            self._exit("T1", p["t1"])
            return
        if not is_long and price <= p["t1"]:
            self._exit("T1", p["t1"])
            return

        # SL
        if is_long and price <= p["sl"]:
            self._exit("SL", p["sl"])
            return
        if not is_long and price >= p["sl"]:
            self._exit("SL", p["sl"])
            return

    # ── zone cooldown check ───────────────────────────────────────────────────

    def _on_cooldown(self, zone_key: str) -> bool:
        if zone_key not in self._sl_zones:
            return False
        days = (date.today() - date.fromisoformat(self._sl_zones[zone_key])).days
        return days <= self.cooldown_days

    # ── daily reset ───────────────────────────────────────────────────────────

    def _daily_reset(self) -> None:
        global log
        today = date.today().isoformat()
        if today != self._day_str:
            log.info("New day %s — prior day P&L=$%.2f", today, self._day_pnl)
            self._day_str = today
            self._day_pnl = 0.0
            log = _setup_log()

    # ── main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        log.info("=== BTC Live Trader running (Ctrl+C to stop) ===")
        n_candles = (self.htf_min + self.lookback_days * 1440 + 120)

        while True:
            try:
                self._daily_reset()

                price = self.client.mark_price()
                if price <= 0:
                    log.warning("mark_price=0 — skipping tick")
                    time.sleep(10)
                    continue

                # Daily loss halt
                if self._day_pnl <= -abs(self.max_day_loss):
                    log.warning("Daily loss cap reached ($%.2f) — no new trades today",
                                self._day_pnl)
                    if self._pos:
                        self._exit("DAY_LOSS_HALT", price)
                    time.sleep(60)
                    continue

                # Monitor open position every 10s
                if self._pos:
                    self._check_exit(price)
                    time.sleep(10)
                    continue

                # Scan for new signal every sub_min minutes
                now = time.time()
                scan_interval = self.sub_min * 60
                if now - self._last_scan_ts < scan_interval:
                    time.sleep(10)
                    continue

                self._last_scan_ts = now
                log.info("Scanning zones... BTC mark=%.0f", price)

                df_1m = fetch_recent_1m(n_candles)
                if df_1m.empty or len(df_1m) < self.htf_min:
                    log.warning("Insufficient candle data (%d bars)", len(df_1m))
                    time.sleep(30)
                    continue

                signal = detect_signal(df_1m, self.htf_min, self.sub_min)
                if signal is None:
                    log.info("No zone signal")
                    time.sleep(10)
                    continue

                if self._on_cooldown(signal["zone_key"]):
                    log.info("Zone %s on cooldown — skipping", signal["zone_key"])
                    time.sleep(10)
                    continue

                # Enter only when price is INSIDE the zone (within 150 pts of trigger).
                # Bug was: (price <= trigger) passed for any price 2000+ pts below zone.
                trig    = signal["trigger"]
                is_long = signal["is_long"]
                zh      = signal["zh"]
                zl      = signal["zl"]
                tol     = 150   # pts: how close to trigger we must be to enter
                if is_long:
                    # BEAR zone → LONG: price must be near trigger from below, or inside zone
                    in_range = (zl - tol <= price <= trig + tol)
                else:
                    # BULL zone → SHORT: price must be near trigger from above, or inside zone
                    in_range = (trig - tol <= price <= zh + tol)

                if in_range:
                    log.info("Signal: %s zone=%s [%.0f-%.0f] trigger=%.0f price=%.0f t1=%.0f",
                             "LONG" if is_long else "SHORT",
                             signal["zone_key"], zl, zh, trig, price, signal["t1"])
                    self._enter(signal, price)
                else:
                    log.info("Signal zone=%s [%.0f-%.0f] trigger=%.0f — price=%.0f not in range, waiting",
                             signal["zone_key"], zl, zh, trig, price)

                time.sleep(10)

            except KeyboardInterrupt:
                log.info("Stopped by user.")
                if self._pos:
                    log.warning("Open position exists — closing before exit")
                    price = self.client.mark_price()
                    self._exit("MANUAL_STOP", price)
                break
            except Exception as e:
                log.error("Loop error: %s", e, exc_info=True)
                time.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────

def main():
    try:
        cfg = load_config()
    except Exception as e:
        print(f"ERROR: Could not load config from DB: {e}")
        print("Run: python3 scripts/setup_btc_live.py first")
        sys.exit(1)

    if cfg.get("api_key", "").startswith("PASTE"):
        print("ERROR: Real API credentials not set. Edit setup_btc_live.py and re-run it.")
        sys.exit(1)

    trader = BtcLiveTrader(cfg)
    trader.run()


if __name__ == "__main__":
    main()
