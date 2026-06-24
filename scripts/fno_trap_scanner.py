#!/usr/bin/env python3
"""
fno_trap_scanner.py — NSE F&O Trap Zone Scanner (75-min HTF)

Scans 90 liquid NSE F&O stocks for BEAR/BULL trap zones.
CNX (NIFTY) alignment flags high-confidence signals.
Telegram alert on new zone entries. Refreshes every 5 min during market hours.

Trap semantics:
  BEAR trap → bears squeezed (price broke UP past their SL)
              → pullback into zone = BUY CE opportunity
  BULL trap → bulls squeezed (price broke DOWN past their SL)
              → bounce into zone  = BUY PE opportunity

  CNX CONFIRMED = NIFTY and stock are in same-direction trap (higher conviction)

Usage:
  python scripts/fno_trap_scanner.py
  python scripts/fno_trap_scanner.py --token JWT --tg_token BOT --tg_chat CHAT_ID
  python scripts/fno_trap_scanner.py --once          # one-time scan, no loop
  python scripts/fno_trap_scanner.py --htf 60        # 60-min HTF (default 75)
  python scripts/fno_trap_scanner.py --lookback 5    # 5 prev days for zones (default 3)

Telegram setup:
  1. @BotFather on Telegram → /newbot → copy bot_token
  2. Start chat with your bot, then visit:
     https://api.telegram.org/bot<TOKEN>/getUpdates
  3. Find "chat":{"id": ...} — that is your chat_id
  4. Pass --tg_token <bot_token> --tg_chat <chat_id>
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner.scanner import scan_htf_spot

# ── TOKEN — paste your daily Upstox JWT here to avoid passing --token each run ─
# Get it from the UI (Feeder section) or from run_system.py startup logs.
# Leave blank ("") to read from data/clients.db automatically.
UPSTOX_TOKEN = ""

# ── Defaults (overridden by CLI args) ─────────────────────────────────────────
HTF_MINUTES        = 75
LOOKBACK_DAYS      = 3       # prev trading days of bars for zone detection
APPROACH_PCT       = 0.005   # alert when price is within 0.5% of zone edge
ALERT_COOLDOWN_MIN = 30      # suppress re-alerts for same zone (min)
SCAN_INTERVAL_MIN  = 5       # refresh every N minutes
MKT_OPEN           = "09:15"
MKT_CLOSE          = "15:30"

BASE_URL       = "https://api.upstox.com/v2"
NSE_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
NSE_CACHE_FILE = "data/_nse_key_cache.json"
NSE_CACHE_DATE = "data/_nse_key_cache_date.txt"
NIFTY_KEY      = "NSE_INDEX|Nifty 50"
HEADERS: dict  = {}

# ── 90 liquid NSE F&O stocks with liquid options ──────────────────────────────
FNO_STOCKS = [
    # NIFTY 50 — largest options OI
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "KOTAKBANK",
    "HINDUNILVR", "AXISBANK", "BAJFINANCE", "WIPRO", "LT", "SBIN",
    "MARUTI", "TITAN", "SUNPHARMA", "HCLTECH", "ULTRACEMCO", "ASIANPAINT",
    "ITC", "BHARTIARTL", "ADANIPORTS", "NESTLEIND", "TECHM", "M&M",
    "POWERGRID", "NTPC", "BPCL", "COALINDIA", "INDUSINDBK", "TATASTEEL",
    "TATACONSUM", "GRASIM", "JSWSTEEL", "HINDALCO", "DRREDDY", "CIPLA",
    "DIVISLAB", "EICHERMOT", "ONGC", "IOC", "TMCV", "BAJAJ-AUTO",
    "HEROMOTOCO", "APOLLOHOSP", "PIDILITIND", "BRITANNIA", "SHREECEM",
    "BAJAJFINSV", "ADANIENT", "SBILIFE", "LTTS",
    # Additional high-OI F&O names with liquid options
    "BANKBARODA", "CANBK", "FEDERALBNK", "IDFCFIRSTB", "PNB",
    "BANDHANBNK", "IDEA", "TATAPOWER", "ADANIGREEN", "ETERNAL",
    "IRCTC", "JUBLFOOD", "BIOCON", "LUPIN", "TORNTPHARM",
    "AUROPHARMA", "DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE",
    "INDUSTOWER", "JSWENERGY", "MOTHERSON", "ASHOKLEY", "HAL",
    "BEL", "BHEL", "SAIL", "NMDC", "VEDL", "APOLLOTYRE", "MRF",
    "DMART", "NYKAA", "PAYTM", "JIOFIN", "MFSL", "BALKRISIND",
    "ABCAPITAL", "TORNTPOWER",
]


# ── API helpers ────────────────────────────────────────────────────────────────
def _get(url: str, params: dict = None) -> dict:
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(3)
                continue
            return r.json() if r.status_code == 200 else {}
        except Exception:
            time.sleep(1)
    return {}


def fetch_1m(key: str, dt_str: str) -> pd.DataFrame:
    enc  = key.replace("|", "%7C").replace(" ", "%20")
    url  = f"{BASE_URL}/historical-candle/{enc}/1minute/{dt_str}/{dt_str}"
    data = _get(url)
    cands = (data.get("data") or {}).get("candles", [])
    if not cands:
        return pd.DataFrame()
    df = pd.DataFrame(cands, columns=["datetime", "open", "high", "low", "close", "volume", "oi"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime").reset_index(drop=True)


def fetch_1m_multi(key: str, lookback: int) -> pd.DataFrame:
    """Concatenate 1m bars for the last N trading days."""
    frames = []
    d = date.today()
    for _ in range(lookback):
        d = prev_trading_day(d)
        bars = fetch_1m(key, d.isoformat())
        if not bars.empty:
            frames.append(bars)
        time.sleep(0.3)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames).sort_values("datetime").reset_index(drop=True)


def fetch_ltp_batch(keys: list) -> dict:
    """Batch LTP for up to 500 instrument keys (one REST call)."""
    if not keys:
        return {}
    try:
        r = requests.get(
            f"{BASE_URL}/market-quote/ltp",
            headers=HEADERS,
            params={"instrument_key": ",".join(keys)},
            timeout=20,
        )
        if r.status_code == 200:
            raw = r.json().get("data", {})
            # normalise keys — some API versions URL-encode the pipe
            return {k.replace("%7C", "|"): v for k, v in raw.items()}
    except Exception:
        pass
    return {}


def resample_htf(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    return (
        df.set_index("datetime")
        .resample(f"{minutes}min", label="right", closed="right")
        .agg({"open": "first", "high": "max", "low": "min",
              "close": "last", "volume": "sum"})
        .dropna(subset=["open"])
        .reset_index()
    )


def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while p.weekday() >= 5:
        p -= timedelta(days=1)
    return p


# ── NSE master JSON ────────────────────────────────────────────────────────────
def load_nse_key_map() -> dict:
    """Return {tradingsymbol: instrument_key} — downloaded once per calendar day."""
    today_str = date.today().isoformat()
    if os.path.exists(NSE_CACHE_FILE) and os.path.exists(NSE_CACHE_DATE):
        with open(NSE_CACHE_DATE) as f:
            if f.read().strip() == today_str:
                with open(NSE_CACHE_FILE) as f2:
                    return json.load(f2)

    print("  Downloading NSE master JSON from Upstox (once per day)...", flush=True)
    r = requests.get(NSE_MASTER_URL, timeout=60)
    instruments = json.loads(gzip.decompress(r.content))

    key_map: dict = {}
    for inst in instruments:
        key = inst.get("instrument_key", "")
        if not key.startswith("NSE_EQ|"):
            continue
        # Upstox uses "trading_symbol" (underscore), equity symbols may have "-EQ"/"-BE" suffix
        raw_sym = inst.get("trading_symbol", "") or inst.get("tradingsymbol", "")
        sym = raw_sym.replace("-EQ", "").replace("-BE", "").upper()
        if sym and sym not in key_map:
            key_map[sym] = key

    os.makedirs("data", exist_ok=True)
    with open(NSE_CACHE_FILE, "w") as f:
        json.dump(key_map, f)
    with open(NSE_CACHE_DATE, "w") as f:
        f.write(today_str)

    print(f"  NSE master: {len(key_map)} equity instruments cached")
    return key_map


# ── Zone detection ─────────────────────────────────────────────────────────────
def get_trapped_zones(key: str, lookback: int) -> list:
    """Fetch prev-N-days bars → resample to HTF → return TRAPPED zones only."""
    bars = fetch_1m_multi(key, lookback)
    if bars.empty:
        return []
    htf = resample_htf(bars, HTF_MINUTES)
    if len(htf) < 3:
        return []
    _, entries = scan_htf_spot(htf)
    return [e for e in entries if e["status"] == "TRAPPED"]


def zone_proximity(ltp: float, zone: dict) -> tuple:
    """
    Direction-aware proximity check.

    BEAR trap (bullish): price should pull back DOWN into zone from above.
      → alert when ltp > zone_high but within APPROACH_PCT, or IN_ZONE.
      → skip if ltp < zone_low (zone already failed downward).

    BULL trap (bearish): price should bounce UP into zone from below.
      → alert when ltp < zone_low but within APPROACH_PCT, or IN_ZONE.
      → skip if ltp > zone_high (zone already failed upward).

    Returns (status, dist_pct) where status ∈ {"IN_ZONE","NEAR","FAR"}.
    """
    zh   = zone["zone_high"]
    zl   = zone["zone_low"]
    kind = zone["kind"]

    if zl <= ltp <= zh:
        return "IN_ZONE", 0.0

    if kind == "BEAR":
        if ltp > zh:
            d = (ltp - zh) / zh
            return ("NEAR", round(d * 100, 2)) if d <= APPROACH_PCT else ("FAR", round(d * 100, 2))
        return "FAR", 0.0  # ltp < zl → zone failed

    else:  # BULL
        if ltp < zl:
            d = (zl - ltp) / zl
            return ("NEAR", round(d * 100, 2)) if d <= APPROACH_PCT else ("FAR", round(d * 100, 2))
        return "FAR", 0.0  # ltp > zh → zone failed


# ── Telegram ───────────────────────────────────────────────────────────────────
def send_telegram(bot_token: str, chat_id: str, text: str) -> None:
    if not bot_token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


def _build_tg_msg(r: dict, nifty_ltp: float) -> str:
    emoji = "🟢" if r["kind"] == "BEAR" else "🔴"
    setup = "BULL SETUP → BUY CE" if r["kind"] == "BEAR" else "BEAR SETUP → BUY PE"
    cnx   = "\n⚡ <b>CNX CONFIRMED</b> (NIFTY same direction)" if r["cnx_confirmed"] else ""
    tag   = "✅ IN ZONE" if r["zone_status"] == "IN_ZONE" else f"⏳ NEAR (+{r['dist_pct']:.2f}%)"
    return (
        f"{emoji} <b>{r['symbol']}</b> — {setup}{cnx}\n"
        f"LTP: <b>{r['ltp']:.2f}</b>  |  Zone: {r['zone_low']:.2f} – {r['zone_high']:.2f}\n"
        f"T1 (trap level): {r['trap_sl']:.2f}  |  {tag}\n"
        f"NIFTY: {nifty_ltp:.0f}  |  {datetime.now().strftime('%H:%M')}"
    )


# ── Single scan pass ───────────────────────────────────────────────────────────
def run_scan(
    zone_map: dict,
    key_map: dict,
    tg_token: str,
    tg_chat: str,
    alerted: dict,
    nifty_direction: str,
) -> None:
    now = datetime.now()

    # One batch call for all stocks + NIFTY LTP
    ltp_data = fetch_ltp_batch(list(key_map.values()) + [NIFTY_KEY])
    nifty_raw = ltp_data.get(NIFTY_KEY, {})
    nifty_ltp = float(nifty_raw.get("last_price", 0)) if isinstance(nifty_raw, dict) else 0.0

    bull_setups: list = []   # BEAR trap  → bullish, buy CE
    bear_setups: list = []   # BULL trap  → bearish, buy PE

    for sym in FNO_STOCKS:
        key   = key_map.get(sym)
        zones = zone_map.get(sym, [])
        if not key or not zones:
            continue

        raw = ltp_data.get(key, {})
        ltp = float(raw.get("last_price", 0)) if isinstance(raw, dict) else 0.0
        if ltp <= 0:
            continue

        for z in zones:
            status, dist_pct = zone_proximity(ltp, z)
            if status == "FAR":
                continue

            kind     = z["kind"]
            zone_key = f"{sym}_{kind}_{z['zone_low']:.2f}"
            cnx_ok   = bool(nifty_direction) and kind == nifty_direction

            rec = {
                "symbol": sym, "kind": kind, "ltp": ltp,
                "zone_high": z["zone_high"], "zone_low": z["zone_low"],
                "trap_sl": z["sl"],
                "dist_pct": dist_pct,
                "zone_status": status,
                "cnx_confirmed": cnx_ok,
                "zone_key": zone_key,
            }
            (bull_setups if kind == "BEAR" else bear_setups).append(rec)

            # Alert logic: new zone, NEAR→IN_ZONE upgrade, or cooldown expired
            last = alerted.get(zone_key, {})
            upgrade      = status == "IN_ZONE" and last.get("status") == "NEAR"
            cooldown_ok  = (
                (now - last["at"]).total_seconds() >= ALERT_COOLDOWN_MIN * 60
                if last.get("at") else True
            )
            if not last or upgrade or cooldown_ok:
                alerted[zone_key] = {"at": now, "status": status}
                send_telegram(tg_token, tg_chat, _build_tg_msg(rec, nifty_ltp))

    # ── Terminal output ────────────────────────────────────────────────────────
    div = "=" * 68
    print(f"\n{div}")
    print(f"  FNO TRAP SCANNER  [{now.strftime('%H:%M:%S')}]"
          f"    NIFTY {nifty_ltp:.0f}"
          f"    CNX: {nifty_direction or 'NEUTRAL'}")
    print(div)

    def _show(records: list, label: str, emoji: str) -> None:
        if not records:
            return
        by_conf = lambda r: (not r["cnx_confirmed"], r["dist_pct"])
        for r in sorted(records, key=by_conf):
            cnx_tag = " ⚡CNX" if r["cnx_confirmed"] else "     "
            prox    = "✅ IN ZONE" if r["zone_status"] == "IN_ZONE" else f"⏳ NEAR +{r['dist_pct']:.2f}%"
            print(f"  {emoji}{cnx_tag}  {r['symbol']:<14}"
                  f"  LTP {r['ltp']:>9.2f}"
                  f"  zone {r['zone_low']:.2f}–{r['zone_high']:.2f}"
                  f"  T1 {r['trap_sl']:.2f}"
                  f"  {prox}")

    total = len(bull_setups) + len(bear_setups)
    if total == 0:
        print("\n  No stocks in or approaching a trapped zone.")
    else:
        if bull_setups:
            print(f"\n  🟢 BULL SETUP — BUY CE  ({len(bull_setups)} stock{'s' if len(bull_setups)>1 else ''})")
            _show(bull_setups, "BULL", "🟢")
        if bear_setups:
            print(f"\n  🔴 BEAR SETUP — BUY PE  ({len(bear_setups)} stock{'s' if len(bear_setups)>1 else ''})")
            _show(bear_setups, "BEAR", "🔴")

    print(f"\n  {total} signal(s) | next scan in {SCAN_INTERVAL_MIN} min\n")


# ── Startup: load zones ────────────────────────────────────────────────────────
def build_zone_map(stock_keys: dict, lookback: int) -> tuple:
    """Fetch bars + detect zones for all stocks and NIFTY. Returns (zone_map, nifty_direction)."""
    total = len(stock_keys)
    print(f"\n[2/3] Fetching {lookback}-day bars & detecting {HTF_MINUTES}-min zones ...")
    eta = total * lookback * 0.3
    print(f"  ETA ≈ {eta:.0f}s — please wait\n")

    # NIFTY
    print("  NIFTY ...", end="", flush=True)
    nifty_zones = get_trapped_zones(NIFTY_KEY, lookback)
    nifty_direction = nifty_zones[-1]["kind"] if nifty_zones else ""
    print(f" {len(nifty_zones)} trapped zones | CNX={nifty_direction or 'NEUTRAL'}")

    zone_map: dict = {}
    for i, (sym, key) in enumerate(stock_keys.items(), 1):
        print(f"  [{i:>2}/{total}] {sym:<14}", end="", flush=True)
        zones = get_trapped_zones(key, lookback)
        if zones:
            zone_map[sym] = zones
            kinds = ", ".join(sorted({z["kind"] for z in zones}))
            print(f" {len(zones)} zones ({kinds})")
        else:
            print(" —")

    hits = len(zone_map)
    print(f"\n  {hits}/{total} stocks have TRAPPED zones")
    return zone_map, nifty_direction


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    global HTF_MINUTES, LOOKBACK_DAYS  # set from CLI args below

    ap = argparse.ArgumentParser(
        description="NSE F&O Trap Zone Scanner — 75-min HTF, Telegram alerts"
    )
    ap.add_argument("--token",    help="Upstox JWT (omit to read from DB)")
    ap.add_argument("--tg_token", default="", help="Telegram bot token")
    ap.add_argument("--tg_chat",  default="", help="Telegram chat/group ID")
    ap.add_argument("--once",     action="store_true", help="Single scan then exit")
    ap.add_argument("--htf",      type=int, default=HTF_MINUTES,
                    help=f"HTF candle minutes (default {HTF_MINUTES})")
    ap.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                    help=f"Prev trading days for zone detection (default {LOOKBACK_DAYS})")
    args = ap.parse_args()

    HTF_MINUTES   = args.htf
    LOOKBACK_DAYS = args.lookback

    # ── Token — priority: CLI arg > UPSTOX_TOKEN constant > DB ───────────────
    token = args.token or UPSTOX_TOKEN.strip()
    if not token:
        try:
            from data_layer.client_db import ClientDB
            creds = ClientDB("data/clients.db").get_feeder_creds_sync("upstox")
            token = (creds or {}).get("access_token", "")
        except Exception:
            pass
    if not token:
        print("ERROR: No Upstox token found.")
        print("  Option 1: Set UPSTOX_TOKEN at top of this script")
        print("  Option 2: Pass --token YOUR_JWT")
        print("  Option 3: Configure Upstox feeder in the UI (reads from DB)")
        sys.exit(1)
    HEADERS["Authorization"] = f"Bearer {token}"

    print("\n" + "=" * 68)
    print("  NSE F&O Trap Zone Scanner")
    print(f"  HTF={HTF_MINUTES}min  Lookback={LOOKBACK_DAYS}d  Stocks={len(FNO_STOCKS)}")
    print("=" * 68)

    # ── NSE key map ────────────────────────────────────────────────────────────
    print("\n[1/3] Resolving NSE instrument keys...")
    nse_map    = load_nse_key_map()
    stock_keys = {sym: nse_map[sym] for sym in FNO_STOCKS if sym in nse_map}
    missing    = [s for s in FNO_STOCKS if s not in nse_map]
    if missing:
        print(f"  WARNING — not in NSE master: {missing}")
    print(f"  {len(stock_keys)}/{len(FNO_STOCKS)} stocks resolved")

    # ── Zone map ───────────────────────────────────────────────────────────────
    zone_map, nifty_direction = build_zone_map(stock_keys, LOOKBACK_DAYS)

    # ── Scan loop ──────────────────────────────────────────────────────────────
    alerted: dict = {}
    print(f"\n[3/3] Starting scan loop — every {SCAN_INTERVAL_MIN} min ({MKT_OPEN}–{MKT_CLOSE} IST)")
    if args.tg_token:
        print(f"  Telegram → chat {args.tg_chat}")
    else:
        print("  Telegram: disabled (pass --tg_token and --tg_chat to enable)")

    if args.once:
        run_scan(zone_map, stock_keys, args.tg_token, args.tg_chat, alerted, nifty_direction)
        return

    while True:
        now_str = datetime.now().strftime("%H:%M")
        if MKT_OPEN <= now_str <= MKT_CLOSE:
            run_scan(zone_map, stock_keys, args.tg_token, args.tg_chat, alerted, nifty_direction)
            time.sleep(SCAN_INTERVAL_MIN * 60)
        elif now_str < MKT_OPEN:
            opens_at = datetime.strptime(MKT_OPEN, "%H:%M")
            now_dt   = datetime.now().replace(second=0, microsecond=0)
            wait_sec = max(int((opens_at - now_dt.replace(hour=opens_at.hour, minute=opens_at.minute)).total_seconds()), 30)
            # simpler: just wait 60s chunks until open
            print(f"[{now_str}] Market not open yet (opens {MKT_OPEN}). Waiting...")
            time.sleep(60)
        else:
            print(f"[{now_str}] Market closed.")
            break


if __name__ == "__main__":
    main()
