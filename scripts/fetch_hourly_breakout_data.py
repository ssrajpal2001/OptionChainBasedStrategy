"""Fetch July-expiry NIFTY spot + ATM CE/PE 1m history from Upstox and save CSV."""
from __future__ import annotations
import argparse, asyncio, logging, os, sys
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
OUT_DIR = ROOT / "data" / "hourly_breakout"
SPOT_CSV = OUT_DIR / "spot_NIFTY_1m.csv"
UNDERLYING, STRIKE_STEP = "NIFTY", 50.0

def _trading_days(start, end):
    d, days = start, []
    while d <= end:
        if d.weekday() < 5: days.append(d)
        d += timedelta(days=1)
    return days

def _load_spot(start, end):
    cands = sorted((ROOT/"data"/"nse_option_cache").glob("spot_NIFTY_1m_*.parquet"))
    if cands:
        df = pd.concat([pd.read_parquet(p) for p in cands[-3:]], ignore_index=True)
        df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        df["datetime"] = pd.to_datetime(df["datetime"])
        mind, maxd = df["datetime"].min().date(), df["datetime"].max().date()
        if mind <= start and maxd >= end:
            return df
    return pd.DataFrame()

def _daily_strikes(spot_df, start, end):
    spot = spot_df.copy()
    col = "ts" if "ts" in spot.columns else "datetime"
    spot[col] = pd.to_datetime(spot[col])
    spot["date"] = spot[col].dt.date
    rows = []
    for d in _trading_days(start, end):
        day = spot[spot["date"] == d]
        if day.empty: continue
        o = float(day.iloc[0]["open"])
        rows.append((d, int(round(o/STRIKE_STEP)*STRIKE_STEP), o))
    return rows

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--token", default=os.environ.get("UPSTOX_ACCESS_TOKEN", ""))
    p.add_argument("--start", default="2026-07-01")
    p.add_argument("--end", default="2026-07-30")
    p.add_argument("--strike-range", type=int, default=2)
    a = p.parse_args()
    if not a.token:
        logger.error("No token"); return 1
    start = datetime.strptime(a.start, "%Y-%m-%d").date()
    end = datetime.strptime(a.end, "%Y-%m-%d").date()
    await asyncio.to_thread(REGISTRY.load_sync, UNDERLYING, a.token, 8)
    exp = next((e for e in REGISTRY.all_expiries(UNDERLYING) if e.month==7 and e.year==2026), None)
    if not exp:
        logger.error("No July expiry"); return 1
    logger.info("July expiry: %s", exp)
    spot = _load_spot(start, end)
    if spot.empty:
        spot = pd.DataFrame(await fetch_upstox_range_1m(REGISTRY.get_upstox_index_key(UNDERLYING), a.token, start, end))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spot.to_csv(SPOT_CSV, index=False)
    logger.info("Spot saved: %s rows", len(spot))
    days = _daily_strikes(spot, start, end)
    logger.info("Daily strikes: %s", [(d,s) for d,s,_ in days])
    sem = asyncio.Semaphore(3)
    fetched = {}
    async def fetch_one(side, strike, day):
        key = REGISTRY.get_upstox_key(UNDERLYING, exp, strike, side)
        if not key: return
        async with sem:
            rows = await fetch_upstox_range_1m(key, a.token, day, day)
            if rows:
                fetched.setdefault((side, strike), []).extend(rows)
    await asyncio.gather(*[fetch_one(side, strike, day) for day, strike, _ in days for side in ("CE","PE")], return_exceptions=True)
    for (side, strike), rows in fetched.items():
        path = OUT_DIR / f"opt_NIFTY{side}{strike}_1m.csv"
        df = pd.DataFrame(rows).sort_values("ts").drop_duplicates("ts")
        df.to_csv(path, index=False)
        logger.info("Saved %s: %s rows", path.name, len(df))
    pd.DataFrame([{"date":d,"spot_open":o,"atm_strike":s} for d,s,o in days]).to_csv(OUT_DIR/"strike_manifest.csv", index=False)
    return 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
