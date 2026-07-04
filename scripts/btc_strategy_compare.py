"""
scripts/btc_strategy_compare.py
Compare two BTC entry strategies on historical 1m data.
  A) Full cascade:  HTF(5m) -> MTF(3m) -> OB+CHoCH -> LTF 1m candle break
  B) OB direct:     HTF(5m) -> OB+CHoCH -> entry at spot (SL = $200)

Usage: python scripts/btc_strategy_compare.py
"""
import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd
from strategies.trap_scanner import scanner as sc

# ── config ────────────────────────────────────────────────────────────────────
HTF_MIN  = 5
MTF_MIN  = 3
SL_BUF   = 100.0    # cascade SL buffer pts
OB_SL    = 200.0    # OB-direct fixed SL pts
ZIGZAG   = 9
OB_WIN   = 500      # bars of history for OB scan (same as live engine)
HTF_WIN  = 60       # HTF candles window for zone scan
MTF_WIN  = 100      # MTF candles window for zone scan
# Only run OB check every N minutes (same as live engine: once/sec ~ every tick, but
# for backtest we check per 1m candle close — that's sufficient)

# ── helpers ───────────────────────────────────────────────────────────────────
def zone_kind(z):
    return "BEAR" if z.get("sl", 0) > z.get("zone_high", 0) else "BULL"

def zone_uid(z):
    return (round(z.get("zone_low", 0), 1), round(z.get("zone_high", 0), 1), str(z.get("ref_ts", "")))

def check_ob_choch(bars_1m, spot, side, zigzag):
    """Run OB+CHoCH on most recent OB_WIN 1m bars. Return True if gate clears."""
    if len(bars_1m) < max(zigzag * 3 + 14, 30):
        return False
    try:
        df = pd.DataFrame(bars_1m[-OB_WIN:])
        df["datetime"] = pd.to_datetime(df["datetime"])
        bull_obs, bear_obs = sc.active_order_blocks(df, zigzag_len=zigzag)
        signals  = sc.detect_choch_bos(df, zigzag_len=zigzag)
        choch    = sc.last_choch_direction(signals)
    except Exception:
        return False
    if side == "CE":    # BEAR trap: expect reversal UP
        return choch == "UP" and bool(sc.price_in_order_block(spot, bull_obs))
    else:               # BULL trap: expect reversal DOWN
        return choch == "DOWN" and bool(sc.price_in_order_block(spot, bear_obs))

# ── load + prep data ──────────────────────────────────────────────────────────
print("Loading BTC 1m cache...")
df1m = pd.read_parquet(os.path.join(ROOT, "data", "btc_1m_cache.parquet"))
df1m["datetime"] = pd.to_datetime(df1m["datetime"])
df1m = df1m.set_index("datetime").sort_index()
if df1m.index.tzinfo is not None:
    df1m.index = df1m.index.tz_localize(None)
print(f"  {len(df1m)} bars  {df1m.index[0]}  to  {df1m.index[-1]}")

# Pre-compute HTF + MTF bars once
print("Pre-computing HTF/MTF bars...")
htf_df = df1m[["open","high","low","close","volume"]].resample(f"{HTF_MIN}min").agg(
    {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
).dropna(subset=["close"])
mtf_df = df1m[["open","high","low","close","volume"]].resample(f"{MTF_MIN}min").agg(
    {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
).dropna(subset=["close"])
htf_df = htf_df.reset_index()
mtf_df = mtf_df.reset_index()
print(f"  HTF bars: {len(htf_df)}  MTF bars: {len(mtf_df)}")

# Build 1m bar list for OB scanning
bars_raw = df1m.reset_index().rename(columns={"index":"datetime"})
bars_list = bars_raw.to_dict("records")
bars_1m_ts = bars_raw["datetime"].tolist()

# ── Strategy A: Full Cascade ──────────────────────────────────────────────────
print("\n--- Strategy A: Full Cascade (HTF->MTF->OB->LTF) ---")

entries_a, exits_a = [], []
htf_zone = htf_side = htf_ref_ts = mtf_zone = None
ob_armed = False
ltf_hi = ltf_lo = 0.0
ltf_armed = False
position = None
notified_a = set()

# Index pointers into htf/mtf frames for O(1) lookup
htf_ptr = 0
mtf_ptr = 0

last_htf_ts = None   # last HTF candle close we processed zones at

for i, (ts, row) in enumerate(df1m.iterrows()):
    spot = float(row.close)

    # ---- SL check -----------------------------------------------------------
    if position:
        side = position["side"]
        if (side == "PE" and spot < position["sl"]) or (side == "CE" and spot > position["sl"]):
            pnl = (spot - position["entry"]) * (1 if side == "PE" else -1)
            exits_a.append({"ts": ts, "side": side, "entry": position["entry"],
                            "exit": spot, "pnl": round(pnl, 1)})
            position = None
            htf_zone = htf_side = htf_ref_ts = mtf_zone = None
            ob_armed = False; ltf_armed = False
        continue

    # ---- Stage 3: LTF candle break ------------------------------------------
    if ltf_armed and mtf_zone and ob_armed:
        if htf_side == "PE" and spot > ltf_hi:
            position = {"side": "PE", "entry": spot, "sl": spot - SL_BUF}
            entries_a.append({"ts": ts, "side": "PE", "entry": spot})
            ltf_armed = False
            continue
        if htf_side == "CE" and spot < ltf_lo:
            position = {"side": "CE", "entry": spot, "sl": spot + SL_BUF}
            entries_a.append({"ts": ts, "side": "CE", "entry": spot})
            ltf_armed = False
            continue
        # Update LTF candle reference each bar
        ltf_hi = float(row.high); ltf_lo = float(row.low)

    # ---- Advance HTF pointer ------------------------------------------------
    # Find latest HTF candle whose time <= current 1m bar ts
    while htf_ptr + 1 < len(htf_df) and htf_df.iloc[htf_ptr + 1]["datetime"] <= ts:
        htf_ptr += 1

    # ---- Stage 1: HTF zone scan (only on new HTF candle close) ---------------
    cur_htf_ts = htf_df.iloc[htf_ptr]["datetime"] if htf_ptr < len(htf_df) else None
    if not htf_zone and cur_htf_ts != last_htf_ts and htf_ptr >= 6:
        last_htf_ts = cur_htf_ts
        htf_window = htf_df.iloc[max(0, htf_ptr - HTF_WIN): htf_ptr + 1].copy()
        try:
            _, htf_zones = sc.scan_htf(htf_window)
        except Exception:
            htf_zones = []
        for z in htf_zones:
            if z.get("status") != "TRAPPED":
                continue
            uid = zone_uid(z)
            if uid in notified_a:
                continue
            zl, zh = z.get("zone_low", 0), z.get("zone_high", 0)
            if zl <= spot <= zh:
                htf_zone = z
                htf_side = "CE" if zone_kind(z) == "BEAR" else "PE"
                htf_ref_ts = z.get("ref_ts")
                break

    if not htf_zone:
        continue

    # ---- HTF zone exit -------------------------------------------------------
    zl, zh = htf_zone.get("zone_low", 0), htf_zone.get("zone_high", 0)
    if (htf_side == "CE" and spot > zh) or (htf_side == "PE" and spot < zl):
        notified_a.add(zone_uid(htf_zone))
        htf_zone = htf_side = htf_ref_ts = mtf_zone = None
        ob_armed = False; ltf_armed = False
        continue

    # ---- Advance MTF pointer -------------------------------------------------
    while mtf_ptr + 1 < len(mtf_df) and mtf_df.iloc[mtf_ptr + 1]["datetime"] <= ts:
        mtf_ptr += 1

    # ---- Stage 2: MTF zone scan ---------------------------------------------
    if not mtf_zone and mtf_ptr >= 3:
        mtf_window = mtf_df.iloc[max(0, mtf_ptr - MTF_WIN): mtf_ptr + 1].copy()
        try:
            _, mtf_zones = sc.scan_htf(mtf_window)
        except Exception:
            mtf_zones = []
        kind_filter = "BEAR" if htf_side == "CE" else "BULL"
        htf_ts_pd = pd.Timestamp(htf_ref_ts) if htf_ref_ts else None
        for z in mtf_zones:
            if z.get("status") != "TRAPPED" or zone_kind(z) != kind_filter:
                continue
            z_ts = pd.Timestamp(z.get("ref_ts")) if z.get("ref_ts") else None
            if htf_ts_pd and z_ts and z_ts < htf_ts_pd:
                continue
            mzl, mzh = z.get("zone_low", 0), z.get("zone_high", 0)
            if mzl <= spot <= mzh:
                mtf_zone = z
                break

    # ---- Stage 2.5: OB + CHoCH gate -----------------------------------------
    if mtf_zone and not ob_armed:
        ob_armed = check_ob_choch(bars_list[:i+1], spot, htf_side, ZIGZAG)

    # ---- Arm LTF candle (first 1m bar after OB gate clears) -----------------
    if mtf_zone and ob_armed and not ltf_armed:
        ltf_hi = float(row.high); ltf_lo = float(row.low)
        ltf_armed = True

# ---- Summary A ---------------------------------------------------------------
wins_a   = [e for e in exits_a if e["pnl"] > 0]
losses_a = [e for e in exits_a if e["pnl"] <= 0]
print(f"Entries={len(entries_a)}  Exits={len(exits_a)}  Open={len(entries_a)-len(exits_a)}")
if exits_a:
    avg_w = sum(e["pnl"] for e in wins_a) / max(len(wins_a), 1)
    avg_l = sum(e["pnl"] for e in losses_a) / max(len(losses_a), 1)
    print(f"Wins={len(wins_a)} avg=+{avg_w:.1f}  Losses={len(losses_a)} avg={avg_l:.1f}")
    print(f"Net P&L={sum(e['pnl'] for e in exits_a):.1f} pts  "
          f"WinRate={len(wins_a)/len(exits_a)*100:.1f}%")
else:
    print("No completed trades.")

# ── Strategy B: OB Direct ─────────────────────────────────────────────────────
print("\n--- Strategy B: OB Direct (HTF->OB+CHoCH->entry@spot SL=$200) ---")

entries_b, exits_b = [], []
htf_zone = htf_side = htf_ref_ts = None
position = None
notified_b = set()
htf_ptr = 0
last_htf_ts = None

for i, (ts, row) in enumerate(df1m.iterrows()):
    spot = float(row.close)

    if position:
        side = position["side"]
        if (side == "PE" and spot < position["sl"]) or (side == "CE" and spot > position["sl"]):
            pnl = (spot - position["entry"]) * (1 if side == "PE" else -1)
            exits_b.append({"ts": ts, "side": side, "entry": position["entry"],
                            "exit": spot, "pnl": round(pnl, 1)})
            position = None
            htf_zone = htf_side = htf_ref_ts = None
        continue

    while htf_ptr + 1 < len(htf_df) and htf_df.iloc[htf_ptr + 1]["datetime"] <= ts:
        htf_ptr += 1

    cur_htf_ts = htf_df.iloc[htf_ptr]["datetime"] if htf_ptr < len(htf_df) else None
    if not htf_zone and cur_htf_ts != last_htf_ts and htf_ptr >= 6:
        last_htf_ts = cur_htf_ts
        htf_window = htf_df.iloc[max(0, htf_ptr - HTF_WIN): htf_ptr + 1].copy()
        try:
            _, htf_zones = sc.scan_htf(htf_window)
        except Exception:
            htf_zones = []
        for z in htf_zones:
            if z.get("status") != "TRAPPED":
                continue
            uid = zone_uid(z)
            if uid in notified_b:
                continue
            zl, zh = z.get("zone_low", 0), z.get("zone_high", 0)
            if zl <= spot <= zh:
                htf_zone = z
                htf_side = "CE" if zone_kind(z) == "BEAR" else "PE"
                htf_ref_ts = z.get("ref_ts")
                break

    if not htf_zone:
        continue

    zl, zh = htf_zone.get("zone_low", 0), htf_zone.get("zone_high", 0)
    if (htf_side == "CE" and spot > zh) or (htf_side == "PE" and spot < zl):
        notified_b.add(zone_uid(htf_zone))
        htf_zone = htf_side = htf_ref_ts = None
        continue

    # OB + CHoCH -> immediate entry
    if check_ob_choch(bars_list[:i+1], spot, htf_side, ZIGZAG):
        sl = spot - OB_SL if htf_side == "PE" else spot + OB_SL
        position = {"side": htf_side, "entry": spot, "sl": sl}
        entries_b.append({"ts": ts, "side": htf_side, "entry": spot})
        notified_b.add(zone_uid(htf_zone))
        htf_zone = htf_side = htf_ref_ts = None

wins_b   = [e for e in exits_b if e["pnl"] > 0]
losses_b = [e for e in exits_b if e["pnl"] <= 0]
print(f"Entries={len(entries_b)}  Exits={len(exits_b)}  Open={len(entries_b)-len(exits_b)}")
if exits_b:
    avg_w = sum(e["pnl"] for e in wins_b) / max(len(wins_b), 1)
    avg_l = sum(e["pnl"] for e in losses_b) / max(len(losses_b), 1)
    print(f"Wins={len(wins_b)} avg=+{avg_w:.1f}  Losses={len(losses_b)} avg={avg_l:.1f}")
    print(f"Net P&L={sum(e['pnl'] for e in exits_b):.1f} pts  "
          f"WinRate={len(wins_b)/len(exits_b)*100:.1f}%")
else:
    print("No completed trades.")

# ── Summary table ─────────────────────────────────────────────────────────────
net_a = sum(e["pnl"] for e in exits_a)
net_b = sum(e["pnl"] for e in exits_b)
wr_a  = len(wins_a) / len(exits_a) * 100 if exits_a else 0
wr_b  = len(wins_b) / len(exits_b) * 100 if exits_b else 0
avg_a = net_a / len(exits_a) if exits_a else 0
avg_b = net_b / len(exits_b) if exits_b else 0

print("\n" + "=" * 55)
print(f"  {'':20} {'CASCADE':>15} {'OB DIRECT':>15}")
print("=" * 55)
print(f"  {'Trades (closed)':20} {len(exits_a):>15} {len(exits_b):>15}")
print(f"  {'Win rate':20} {wr_a:>14.1f}% {wr_b:>14.1f}%")
print(f"  {'Net P&L (pts)':20} {net_a:>15.1f} {net_b:>15.1f}")
print(f"  {'Avg per trade':20} {avg_a:>15.1f} {avg_b:>15.1f}")
print("=" * 55)

# Print individual trades for analysis
if exits_a:
    print("\nCASCADE trades:")
    for e in exits_a:
        print(f"  {e['ts']} {e['side']} entry={e['entry']:.1f} exit={e['exit']:.1f} pnl={e['pnl']:+.1f}")
if exits_b:
    print("\nOB DIRECT trades:")
    for e in exits_b:
        print(f"  {e['ts']} {e['side']} entry={e['entry']:.1f} exit={e['exit']:.1f} pnl={e['pnl']:+.1f}")
