#!/usr/bin/env python3
"""
FnO Intraday Monitor — Phase 2
--------------------------------
Loads last night's FnO scan JSON, then polls Upstox quote API every 60s
during market hours. Fires alerts when a stock hits its entry zone,
SL, or T1 target.

Run alongside the main system (pm2 or separate terminal):
    python3 scripts/fno_intraday_monitor.py

Alerts are printed to terminal + logged to logs/fno_monitor_YYYY-MM-DD.log
If the dashboard is running, alerts are also posted via REST API.

Alert types:
  ENTRY  — price touched the entry zone boundary → trade now
  SL_HIT — price hit stop loss (if you entered earlier)
  T1_HIT — price hit target (if you entered earlier)
  NEAR   — price within 0.3% of entry (heads-up, 1 alert per zone)
"""
import os, sys, json, sqlite3, time, logging
from datetime import date, datetime, timedelta
from typing import Optional
import pytz

_IST = pytz.timezone("Asia/Kolkata")
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import requests

# ── Config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC  = 60       # check prices every 60 seconds
NEAR_THRESHOLD_PCT = 0.3      # % from entry to fire NEAR alert
DASHBOARD_URL      = "http://localhost:5000"   # set "" to skip
LOG_DIR            = os.path.join(_ROOT, "logs")
DB_PATH            = os.path.join(_ROOT, "data", "clients.db")
UPSTOX_BASE        = "https://api.upstox.com/v2"

# FnO instrument keys (symbol → Upstox key mapping, same as fno_stocks.csv)
SYMBOL_TO_KEY = {
    "RELIANCE":     "NSE_EQ|INE002A01018",
    "HDFCBANK":     "NSE_EQ|INE040A01034",
    "SBIN":         "NSE_EQ|INE062A01020",
    "AXISBANK":     "NSE_EQ|INE238A01034",
    "TCS":          "NSE_EQ|INE467B01029",
    "INFY":         "NSE_EQ|INE009A01021",
    "WIPRO":        "NSE_EQ|INE075A01022",
    "BHARTIARTL":   "NSE_EQ|INE397D01024",
    "ITC":          "NSE_EQ|INE154A01025",
    "TATAMOTORS":   "NSE_EQ|INE155L01010",
    "KOTAKBANK":    "NSE_EQ|INE237A01028",
    "BAJFINANCE":   "NSE_EQ|INE296A01024",
    "LT":           "NSE_EQ|INE018A01030",
    "ICICIBANK":    "NSE_EQ|INE090A01021",
    "NESTLEIND":    "NSE_EQ|INE239A01016",
    "SUNPHARMA":    "NSE_EQ|INE044A01036",
    "MARUTI":       "NSE_EQ|INE585B01010",
    "TITAN":        "NSE_EQ|INE280A01028",
    "ONGC":         "NSE_EQ|INE213A01029",
    "NTPC":         "NSE_EQ|INE733E01010",
    "COALINDIA":    "NSE_EQ|INE522F01014",
    "DRREDDY":      "NSE_EQ|INE089A01023",
    "ADANIENT":     "NSE_EQ|INE423A01024",
    "ADANIPORTS":   "NSE_EQ|INE742F01042",
    "TATASTEEL":    "NSE_EQ|INE081A01020",
    "HINDALCO":     "NSE_EQ|INE038A01020",
    "GRASIM":       "NSE_EQ|INE047A01021",
    "JSWSTEEL":     "NSE_EQ|INE019A01038",
    "TECHM":        "NSE_EQ|INE669C01036",
    "HCLTECH":      "NSE_EQ|INE860A01027",
    "CIPLA":        "NSE_EQ|INE059A01026",
    "BAJAJFINSV":   "NSE_EQ|INE918I01026",
    "DIVISLAB":     "NSE_EQ|INE361B01024",
    "APOLLOHOSP":   "NSE_EQ|INE437A01024",
    "EICHERMOT":    "NSE_EQ|INE066A01021",
    "IOC":          "NSE_EQ|INE242A01010",
    "BRITANNIA":    "NSE_EQ|INE216A01030",
    "HEROMOTOCO":   "NSE_EQ|INE158A01026",
    "BPCL":         "NSE_EQ|INE029A01011",
    "PIDILITIND":   "NSE_EQ|INE318A01026",
    "CANBK":        "NSE_EQ|INE476A01022",
    "TATACONSUM":   "NSE_EQ|INE192A01025",
    "INDUSINDBK":   "NSE_EQ|INE095A01012",
    "M&M":          "NSE_EQ|INE101A01026",
    "PFC":          "NSE_EQ|INE134E01011",
    "SIEMENS":      "NSE_EQ|INE003A01024",
    "RECLTD":       "NSE_EQ|INE020B01018",
    "BANKBARODA":   "NSE_EQ|INE028A01039",
    "ABB":          "NSE_EQ|INE117A01022",
    "UNIONBANK":    "NSE_EQ|INE692A01016",
    "INDIGO":       "NSE_EQ|INE646L01027",
    "PNB":          "NSE_EQ|INE160A01022",
    "SBILIFE":      "NSE_EQ|INE123W01016",
    "ZOMATO":       "NSE_EQ|INE758T01015",
    "IRCTC":        "NSE_EQ|INE335Y01020",
    "ZEEL":         "NSE_EQ|INE256A01028",
    "ICICIGI":      "NSE_EQ|INE765G01017",
    "ICICIPRULI":   "NSE_EQ|INE726G01019",
    "HDFCLIFE":     "NSE_EQ|INE795G01014",
    "HDFCAMC":      "NSE_EQ|INE127D01025",
    "ABCAPITAL":    "NSE_EQ|INE674K01013",
    "SBICARD":      "NSE_EQ|INE018E01016",
    "IDFCFIRSTB":   "NSE_EQ|INE818H01020",
    "MFSL":         "NSE_EQ|INE538L01028",
    "MANAPPURAM":   "NSE_EQ|INE522D01027",
    "CHOLAFIN":     "NSE_EQ|INE121A01024",
    "RBLBANK":      "NSE_EQ|INE976G01028",
    "FEDERALBNK":   "NSE_EQ|INE171A01029",
    "BAJAJ-AUTO":   "NSE_EQ|INE917I01010",
    "BANDHANBNK":   "NSE_EQ|INE545U01014",
    "YESBANK":      "NSE_EQ|INE528G01035",
    "SHRIRAMFIN":   "NSE_EQ|INE721A01047",
    "LICHSGFIN":    "NSE_EQ|INE115A01026",
    "AUBANK":       "NSE_EQ|INE949L01017",
    "MUTHOOTFIN":   "NSE_EQ|INE414G01012",
    "POONAWALLA":   "NSE_EQ|INE511C01022",
    "SAIL":         "NSE_EQ|INE114A01011",
    "BHEL":         "NSE_EQ|INE257A01026",
    "GAIL":         "NSE_EQ|INE129A01019",
    "PETRONET":     "NSE_EQ|INE347G01014",
    "HINDPETRO":    "NSE_EQ|INE094A01015",
    "TORNTPOWER":   "NSE_EQ|INE813H01021",
    "MGL":          "NSE_EQ|INE752E01010",
    "TATAPOWER":    "NSE_EQ|INE245A01021",
    "IGL":          "NSE_EQ|INE203G01027",
    "NMDC":         "NSE_EQ|INE584A01023",
    "VEDL":         "NSE_EQ|INE205A01025",
    "JINDALSTEL":   "NSE_EQ|INE749A01030",
    "VOLTAS":       "NSE_EQ|INE226A01021",
    "HAVELLS":      "NSE_EQ|INE176B01034",
    "POLYCAB":      "NSE_EQ|INE455K01017",
    "APLAPOLLO":    "NSE_EQ|INE702C01027",
    "NATIONALUM":   "NSE_EQ|INE139A01034",
    "CROMPTON":     "NSE_EQ|INE299U01018",
    "DIXON":        "NSE_EQ|INE935N01020",
    "CUMMINSIND":   "NSE_EQ|INE298A01020",
    "ESCORTS":      "NSE_EQ|INE042A01014",
    "BOSCHLTD":     "NSE_EQ|INE323A01026",
    "EXIDEIND":     "NSE_EQ|INE302A01020",
    "SUNDARMFIN":   "NSE_EQ|INE660A01013",
    "MOTHERSON":    "NSE_EQ|INE775A01035",
    "BALKRISIND":   "NSE_EQ|INE261B01015",
    "BHARATFORG":   "NSE_EQ|INE465A01025",
    "ASHOKLEY":     "NSE_EQ|INE208A01029",
    "TVSMOTOR":     "NSE_EQ|INE494B01023",
    "MRF":          "NSE_EQ|INE883A01011",
    "APOLLOTYRE":   "NSE_EQ|INE438A01022",
    "TORNTPHARM":   "NSE_EQ|INE685A01028",
    "LUPIN":        "NSE_EQ|INE326A01037",
    "BIOCON":       "NSE_EQ|INE376G01013",
    "AUROPHARMA":   "NSE_EQ|INE406A01037",
    "ALKEM":        "NSE_EQ|INE540L01014",
    "GLENMARK":     "NSE_EQ|INE935A01035",
    "GRANULES":     "NSE_EQ|INE101D01020",
    "GODREJCP":     "NSE_EQ|INE102D01028",
    "DABUR":        "NSE_EQ|INE016A01026",
    "MCDOWELL-N":   "NSE_EQ|INE854D01024",
    "MARICO":       "NSE_EQ|INE196A01026",
    "COLPAL":       "NSE_EQ|INE259A01022",
    "EMAMILTD":     "NSE_EQ|INE548C01032",
    "UBL":          "NSE_EQ|INE686F01025",
    "JUBLFOOD":     "NSE_EQ|INE797F01020",
    "DEEPAKNTR":    "NSE_EQ|INE288B01029",
    "COROMANDEL":   "NSE_EQ|INE169A01031",
    "NAVINFLUOR":   "NSE_EQ|INE048G01026",
    "SRF":          "NSE_EQ|INE647A01010",
    "TATACHEM":     "NSE_EQ|INE092A01019",
    "CONCOR":       "NSE_EQ|INE111A01025",
    "SHREECEM":     "NSE_EQ|INE070A01015",
    "AMBUJACEMENT": "NSE_EQ|INE079A01024",
    "ACC":          "NSE_EQ|INE012A01025",
    "COFORGE":      "NSE_EQ|INE591G01017",
    "PERSISTENT":   "NSE_EQ|INE262H01013",
    "OFSS":         "NSE_EQ|INE881D01027",
    "MPHASIS":      "NSE_EQ|INE356A01018",
    "LTIM":         "NSE_EQ|INE214T01019",
    "NAUKRI":       "NSE_EQ|INE663F01024",
    "HAL":          "NSE_EQ|INE066F01020",
    "IEX":          "NSE_EQ|INE022Q01020",
    "TRENT":        "NSE_EQ|INE849A01020",
    "ZYDUSLIFE":    "NSE_EQ|INE010B01027",
    "BEL":          "NSE_EQ|INE263A01024",
    "RVNL":         "NSE_EQ|INE415G01027",
    "IRFC":         "NSE_EQ|INE053F01010",
    "DLF":          "NSE_EQ|INE271C01023",
    "GODREJPROP":   "NSE_EQ|INE484J01027",
    "PRESTIGE":     "NSE_EQ|INE811K01011",
    "SOBHA":        "NSE_EQ|INE671H01015",
    "INDUSTOWER":   "NSE_EQ|INE121J01017",
    "MAXHEALTH":    "NSE_EQ|INE027H01010",
    "FORTIS":       "NSE_EQ|INE061F01013",
    "TRIDENT":      "NSE_EQ|INE064C01022",
    "TATACOMM":     "NSE_EQ|INE151A01013",
    "KALYANKJIL":   "NSE_EQ|INE303R01014",
    "LICI":         "NSE_EQ|INE0J1Y01017",
}


# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
log_path = os.path.join(LOG_DIR, f"fno_monitor_{date.today()}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path),
    ]
)
log = logging.getLogger("fno_monitor")


# ── Token ─────────────────────────────────────────────────────────────────────
def get_token() -> str:
    try:
        conn = sqlite3.connect(DB_PATH)
        row  = conn.execute(
            "SELECT access_token FROM system_feeder_creds WHERE provider='upstox' LIMIT 1"
        ).fetchone()
        conn.close()
        return (row[0] or "") if row else ""
    except Exception:
        return ""


# ── Market hours check ────────────────────────────────────────────────────────
def is_market_open() -> bool:
    now = datetime.now(_IST)
    if now.weekday() >= 5:   # Saturday/Sunday
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(9, 15) <= t <= dtime(15, 30)


# ── Load today's scan ─────────────────────────────────────────────────────────
def load_scan(scan_date: date) -> list:
    path = os.path.join(_ROOT, "data", f"fno_scan_{scan_date}.json")
    if not os.path.exists(path):
        # Try yesterday's scan (for early morning before today's is run)
        yesterday = scan_date - timedelta(days=1)
        path = os.path.join(_ROOT, "data", f"fno_scan_{yesterday}.json")
        if not os.path.exists(path):
            log.error("No scan file found for %s or %s", scan_date, yesterday)
            return []
        log.info("Using yesterday's scan: %s", path)
    data   = json.load(open(path))
    stocks = data.get("stocks", [])
    log.info("Loaded %d signals from %s  (NIFTY=%s bias=%s)",
             len(stocks), os.path.basename(path),
             data.get("nifty_close", "?"), data.get("nifty_bias", "?"))
    return stocks


# ── Fetch LTP for a batch of stocks ──────────────────────────────────────────
def fetch_ltp_batch(symbols: list, token: str) -> dict:
    """Returns {symbol: ltp} for all symbols in one API call."""
    keys = []
    sym_to_key = {}
    for sym in symbols:
        key = SYMBOL_TO_KEY.get(sym)
        if key:
            keys.append(key)
            sym_to_key[key] = sym

    if not keys:
        return {}

    # Upstox allows comma-separated instrument_key in market-quote
    instrument_keys = ",".join(keys)
    from urllib.parse import quote as q
    url = f"{UPSTOX_BASE}/market-quote/ltp?instrument_key={q(instrument_keys, safe=',|')}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}",
                                       "Accept": "application/json"}, timeout=10)
        if r.status_code != 200:
            log.warning("LTP API error %s", r.status_code)
            return {}
        raw = r.json().get("data", {})
        result = {}
        for key_resp, v in raw.items():
            # key_resp looks like "NSE_EQ:RELIANCE" or "NSE_EQ|RELIANCE"
            # match back to symbol
            for orig_key, sym in sym_to_key.items():
                # Upstox returns key with : instead of |
                normalized = orig_key.replace("|", ":")
                if key_resp == normalized or key_resp == orig_key:
                    ltp = v.get("last_price") or v.get("ltp") or 0
                    if ltp:
                        result[sym] = float(ltp)
                    break
        return result
    except Exception as e:
        log.warning("LTP fetch error: %s", e)
        return {}


# ── Alert state tracker ───────────────────────────────────────────────────────
class ZoneTracker:
    """Tracks alert state for one stock zone."""

    def __init__(self, signal: dict):
        self.sym       = signal["symbol"]
        self.direction = signal["direction"]
        self.entry     = float(signal["entry_plan_price"])
        self.sl        = float(signal["stock_sl"])
        self.t1        = float(signal["stock_t1"])
        self.rr        = float(signal["rr_ratio"])
        self.zone_low  = float(signal["zone_low"])
        self.zone_high = float(signal["zone_high"])

        # Alert state flags
        self.near_alerted   = False
        self.entry_alerted  = False
        self.entered        = False   # True once ENTRY fired
        self.sl_alerted     = False
        self.t1_alerted     = False
        self.done           = False   # True once SL or T1 hit

    def check(self, ltp: float) -> Optional[dict]:
        """
        Check LTP against levels. Returns alert dict if something fired, else None.
        """
        if self.done:
            return None

        # ── Pre-entry: watching for price to approach / touch entry ──────────
        if not self.entry_alerted:
            # NEAR alert: within NEAR_THRESHOLD_PCT of entry
            if self.direction == "CE":
                dist_pct = (ltp - self.entry) / self.entry * 100
            else:
                dist_pct = (self.entry - ltp) / self.entry * 100

            if 0 < dist_pct <= NEAR_THRESHOLD_PCT and not self.near_alerted:
                self.near_alerted = True
                return self._alert("NEAR", ltp,
                    f"{self.sym} {self.direction} approaching entry {self.entry:.2f}  "
                    f"[LTP={ltp:.2f}, {dist_pct:.2f}% away]  R:R={self.rr:.1f}x")

            # ENTRY alert: price touched/crossed entry level
            entry_hit = (self.direction == "CE" and ltp <= self.entry) or \
                        (self.direction == "PE" and ltp >= self.entry)
            if entry_hit:
                self.entry_alerted = True
                self.entered       = True
                action = "BUY CE" if self.direction == "CE" else "BUY PE"
                return self._alert("ENTRY", ltp,
                    f"🔔 {self.sym} {self.direction} ENTRY TRIGGERED @ {ltp:.2f}  "
                    f"→ {action} now!  SL={self.sl:.2f}  T1={self.t1:.2f}  R:R={self.rr:.1f}x")

        # ── Post-entry: watching SL and T1 ───────────────────────────────────
        if self.entered:
            sl_hit = (self.direction == "CE" and ltp <= self.sl) or \
                     (self.direction == "PE" and ltp >= self.sl)
            t1_hit = (self.direction == "CE" and ltp >= self.t1) or \
                     (self.direction == "PE" and ltp <= self.t1)

            if sl_hit and not self.sl_alerted:
                self.sl_alerted = True
                self.done       = True
                return self._alert("SL_HIT", ltp,
                    f"🔴 {self.sym} {self.direction} SL HIT @ {ltp:.2f}  "
                    f"(SL was {self.sl:.2f})  EXIT now.")

            if t1_hit and not self.t1_alerted:
                self.t1_alerted = True
                self.done       = True
                return self._alert("T1_HIT", ltp,
                    f"🟢 {self.sym} {self.direction} TARGET HIT @ {ltp:.2f}  "
                    f"(T1={self.t1:.2f})  BOOK PROFIT.")

        return None

    def _alert(self, kind: str, ltp: float, msg: str) -> dict:
        return {
            "kind":      kind,
            "symbol":    self.sym,
            "direction": self.direction,
            "ltp":       ltp,
            "entry":     self.entry,
            "sl":        self.sl,
            "t1":        self.t1,
            "rr":        self.rr,
            "message":   msg,
            "ts":        datetime.now(_IST).strftime("%H:%M:%S"),
        }


# ── Post alert to dashboard ───────────────────────────────────────────────────
def post_to_dashboard(alert: dict):
    if not DASHBOARD_URL:
        return
    try:
        requests.post(
            f"{DASHBOARD_URL}/api/scanner/alert",
            json=alert,
            timeout=3,
        )
    except Exception:
        pass   # dashboard may not be running — silent fail


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  FnO Intraday Monitor — Phase 2")
    log.info("  Log: %s", log_path)
    log.info("=" * 60)

    token = get_token()
    if not token:
        log.error("No Upstox token in DB — exiting"); return

    # Load signals
    signals = load_scan(date.today())
    if not signals:
        log.error("No signals to monitor — run fno_stock_scanner.py first"); return

    # Build trackers
    trackers = {}
    log.info("\nMonitoring %d stocks:", len(signals))
    for s in signals:
        sym = s["symbol"]
        t   = ZoneTracker(s)
        trackers[sym] = t
        action = "Pullback→CE" if s["direction"] == "CE" else "Rally→PE"
        log.info("  %-14s %s  entry=%-9.2f SL=%-9.2f T1=%-9.2f R:R=%.1fx  [%s]",
                 sym, s["direction"], t.entry, t.sl, t.t1, t.rr, action)

    log.info("\nWaiting for market open (9:15 IST)...")
    alerts_fired = []

    while True:
        now_ist = datetime.now(_IST)

        # Wait for market open
        if not is_market_open():
            if now_ist.hour >= 15 and now_ist.minute >= 35:
                log.info("Market closed (15:30 IST). Today's alerts: %d", len(alerts_fired))
                log.info("Summary:")
                for a in alerts_fired:
                    log.info("  %s  %s %s  %s @ %.2f", a["ts"], a["symbol"], a["direction"], a["kind"], a["ltp"])
                log.info("Monitor stopped. Re-run tomorrow after nightly scan.")
                break
            time.sleep(30)
            continue

        # Fetch LTP for all tracked symbols
        active_syms = [sym for sym, t in trackers.items() if not t.done]
        if not active_syms:
            log.info("All zones resolved (SL/T1 hit). Monitor done for today.")
            break

        ltp_map = fetch_ltp_batch(active_syms, token)
        if not ltp_map:
            log.warning("Empty LTP response — retrying in 30s")
            time.sleep(30)
            continue

        ts = now_ist.strftime("%H:%M:%S")

        # Status line every poll
        status_parts = []
        for sym in active_syms:
            ltp = ltp_map.get(sym)
            if ltp:
                t = trackers[sym]
                if t.direction == "CE":
                    dist = (ltp - t.entry) / t.entry * 100
                    arrow = "↓" if dist > 0 else "✓"
                else:
                    dist = (t.entry - ltp) / t.entry * 100
                    arrow = "↑" if dist > 0 else "✓"
                status_parts.append(f"{sym}={ltp:.1f}({dist:+.1f}%{arrow})")

        if status_parts:
            log.info("[%s] %s", ts, "  ".join(status_parts))

        # Check each tracker
        for sym, tracker in list(trackers.items()):
            if tracker.done:
                continue
            ltp = ltp_map.get(sym)
            if not ltp:
                continue
            alert = tracker.check(ltp)
            if alert:
                log.info("")
                log.info("  " + "!" * 60)
                log.info("  %s", alert["message"])
                log.info("  " + "!" * 60)
                log.info("")
                alerts_fired.append(alert)
                post_to_dashboard(alert)

        # Sleep until next poll
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
