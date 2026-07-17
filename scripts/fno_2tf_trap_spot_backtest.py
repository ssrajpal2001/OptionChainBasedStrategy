"""
scripts/fno_2tf_trap_spot_backtest.py
=======================================
2-timeframe cascading trap backtest on FNO stocks using spot data.

HTF detects structural bull/bear traps; LTF provides the execution entry.
Entry is a full breach of the LTF trap candle (not the 1/3 retracement used in V4).
Stop loss = opposite LTF candle extreme +/- SL_BUFFER.
Target = the HTF structural reference level (prev high for bear trap, prev low for bull trap).

Optional LTF filters: 500-period VWAP, 20-period ADX < max, 14-period RSI directional.
Lot size is read from data/fno_stocks.csv per symbol.

Usage:
  python scripts/fno_2tf_trap_spot_backtest.py --symbols RELIANCE,TCS,HDFCBANK --vwap
  python scripts/fno_2tf_trap_spot_backtest.py --symbols ALL --no-vwap --adx 20 --rsi-long 40
  python scripts/fno_2tf_trap_spot_backtest.py --symbols ALL --vwap --start 2026-06-01 --end 2026-07-14
"""
from __future__ import annotations

import argparse
import glob
import os
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "data", "nse_option_cache")
OUTPUT_DIR = os.path.join(ROOT, "data")
STOCK_CSV = os.path.join(ROOT, "data", "fno_stocks.csv")

SL_BUFFER = 10.0
ENTRY_START = time(9, 15)
ENTRY_END = time(15, 20)
INTRADAY_EXIT = time(15, 30)

HTF_MIN = 75
LTF_MIN = 5

VWAP_PERIOD = 500
ADX_PERIOD = 20
RSI_PERIOD = 14

DEFAULT_MAX_ADX = 20.0
DEFAULT_RSI_LONG_MIN = 40.0
DEFAULT_RSI_SHORT_MAX = 60.0

# ──────────────────────────────────────────────────────────────────────────────
# Indicators (no lookahead)
# ──────────────────────────────────────────────────────────────────────────────


def compute_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    up_move = high - prev_high
    down_move = prev_low - low

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / period, adjust=False).mean() / atr

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1.0 / period, adjust=False).mean()
    return adx


def compute_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> pd.Series:
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_rolling_vwap(df: pd.DataFrame, period: int = VWAP_PERIOD) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].fillna(0)
    weight = vol if vol.sum() > 0 else pd.Series(1, index=df.index)
    pv = typical * weight
    return pv.rolling(window=period, min_periods=period).sum() / weight.rolling(window=period, min_periods=period).sum()


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────


def load_stock_universe() -> pd.DataFrame:
    df = pd.read_csv(STOCK_CSV)
    df = df.dropna(subset=["symbol", "lot_size"])
    df["lot_size"] = df["lot_size"].astype(int)
    return df[["symbol", "lot_size"]]


def load_1m_spot(symbol: str) -> pd.DataFrame:
    pattern = os.path.join(CACHE_DIR, f"spot_{symbol}_1m_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise RuntimeError(f"No 1m spot parquet found for {symbol} in {CACHE_DIR}")

    frames = []
    for f in files:
        df = pd.read_parquet(f)
        df["datetime"] = pd.to_datetime(df["datetime"])
        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata")
        else:
            df["datetime"] = df["datetime"].dt.tz_localize("Asia/Kolkata")
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)
    df = df[(df["datetime"].dt.time >= ENTRY_START) & (df["datetime"].dt.time <= INTRADAY_EXIT)]
    return df


def resample_per_day(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    frames = []
    for day, g in df_1m.groupby(df_1m["datetime"].dt.date):
        g = g.set_index("datetime").sort_index()
        origin = pd.Timestamp(f"{day} 09:15:00", tz="Asia/Kolkata")
        g = g[g.index >= origin] if g.index.min() < origin else g
        r = g.resample(f"{minutes}min", origin=origin).agg(
            {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        ).dropna().reset_index()
        frames.append(r)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def prepare_ltf_with_indicators(df_1m: pd.DataFrame, ltf_min: int) -> pd.DataFrame:
    df = resample_per_day(df_1m, ltf_min)
    df = df.sort_values("datetime").reset_index(drop=True)
    df["vwap"] = compute_rolling_vwap(df, VWAP_PERIOD)
    df["adx"] = compute_adx(df, ADX_PERIOD)
    df["rsi"] = compute_rsi(df, RSI_PERIOD)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Trap detection
# ──────────────────────────────────────────────────────────────────────────────


def find_htf_traps(df_htf: pd.DataFrame) -> List[Dict]:
    """Detect structural traps on the HTF timeframe."""
    traps = []
    for i in range(1, len(df_htf)):
        prev = df_htf.iloc[i - 1]
        curr = df_htf.iloc[i]

        if curr["low"] < prev["low"]:
            traps.append({
                "kind": "BEAR",
                "direction": "LONG",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["high"]),
                "htf_entry_level": float(prev["low"]),
                "zone_high": float(prev["low"]),
                "zone_low": float(curr["low"]),
            })
        if curr["high"] > prev["high"]:
            traps.append({
                "kind": "BULL",
                "direction": "SHORT",
                "breach_ts": curr["datetime"],
                "breach_end": curr["datetime"] + timedelta(minutes=HTF_MIN),
                "target": float(prev["low"]),
                "htf_entry_level": float(prev["high"]),
                "zone_high": float(curr["high"]),
                "zone_low": float(prev["high"]),
            })
    return traps


def find_ltf_traps(df_ltf: pd.DataFrame, htf_trap: Dict, entry_mode: str = "full") -> List[Dict]:
    """Find LTF trap candles inside the HTF breach window."""
    bars = df_ltf[
        (df_ltf["datetime"] >= htf_trap["breach_ts"]) &
        (df_ltf["datetime"] < htf_trap["breach_end"])
    ].copy()
    if len(bars) < 2:
        return []
    bars = bars.sort_values("datetime").reset_index(drop=True)

    traps = []
    for i in range(1, len(bars)):
        prev = bars.iloc[i - 1]
        curr = bars.iloc[i]
        if htf_trap["kind"] == "BEAR":
            if curr["low"] < prev["low"]:
                zone_high = float(prev["low"])
                zone_low = float(curr["low"])
                if entry_mode == "third":
                    trigger = zone_low + (zone_high - zone_low) / 3.0
                else:
                    trigger = float(curr["high"])
                sl = zone_low - SL_BUFFER
                traps.append({
                    "kind": "BEAR",
                    "direction": "LONG",
                    "setup_ts": curr["datetime"],
                    "zone_high": zone_high,
                    "zone_low": zone_low,
                    "trigger": round(trigger, 2),
                    "sl": round(sl, 2),
                    "target": htf_trap["target"],
                    "htf_entry_level": htf_trap["htf_entry_level"],
                })
        else:
            if curr["high"] > prev["high"]:
                zone_high = float(curr["high"])
                zone_low = float(prev["high"])
                if entry_mode == "third":
                    trigger = zone_high - (zone_high - zone_low) / 3.0
                else:
                    trigger = float(curr["low"])
                sl = zone_high + SL_BUFFER
                traps.append({
                    "kind": "BULL",
                    "direction": "SHORT",
                    "setup_ts": curr["datetime"],
                    "zone_high": zone_high,
                    "zone_low": zone_low,
                    "trigger": round(trigger, 2),
                    "sl": round(sl, 2),
                    "target": htf_trap["target"],
                    "htf_entry_level": htf_trap["htf_entry_level"],
                })
    return traps


# ──────────────────────────────────────────────────────────────────────────────
# Filters & execution
# ──────────────────────────────────────────────────────────────────────────────


def check_ltf_filters(
    ltf_trap: Dict,
    df_ltf_full: pd.DataFrame,
    max_adx: float,
    rsi_long_min: float,
    rsi_short_max: float,
    use_vwap: bool,
    use_adx: bool,
    use_rsi: bool,
) -> bool:
    row = df_ltf_full[df_ltf_full["datetime"] == ltf_trap["setup_ts"]]
    if row.empty:
        return False
    row = row.iloc[0]
    vwap, adx, rsi, close = row["vwap"], row["adx"], row["rsi"], row["close"]

    if use_vwap and not pd.isna(vwap):
        if ltf_trap["kind"] == "BEAR" and close <= vwap:
            return False
        if ltf_trap["kind"] == "BULL" and close >= vwap:
            return False

    if use_adx and not pd.isna(adx):
        if adx >= max_adx:
            return False

    if use_rsi and not pd.isna(rsi):
        if ltf_trap["kind"] == "BEAR" and rsi <= rsi_long_min:
            return False
        if ltf_trap["kind"] == "BULL" and rsi >= rsi_short_max:
            return False

    return True


def simulate_trade(
    ltf_trap: Dict,
    df_1m_day: pd.DataFrame,
    eod: datetime,
) -> Optional[Dict]:
    trigger, sl, target = ltf_trap["trigger"], ltf_trap["sl"], ltf_trap["target"]
    kind = ltf_trap["kind"]

    future = df_1m_day[df_1m_day["datetime"] > ltf_trap["setup_ts"]].copy()
    if future.empty:
        return None

    entry_ts = None
    for _, row in future.iterrows():
        if row["datetime"] > eod:
            return None
        if kind == "BEAR" and row["high"] >= trigger:
            entry_ts = row["datetime"]
            break
        if kind == "BULL" and row["low"] <= trigger:
            entry_ts = row["datetime"]
            break

    if entry_ts is None:
        return None

    after = df_1m_day[df_1m_day["datetime"] >= entry_ts].copy()
    exit_ts = None
    exit_spot = None
    exit_reason = "OPEN"

    for _, row in after.iterrows():
        bar_end = row["datetime"] + timedelta(minutes=1)

        if kind == "BEAR":
            if row["low"] <= sl:
                exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                break
            if row["high"] >= target:
                exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                break
        else:
            if row["high"] >= sl:
                exit_spot, exit_reason, exit_ts = sl, "SL", min(bar_end, eod)
                break
            if row["low"] <= target:
                exit_spot, exit_reason, exit_ts = target, "TARGET", min(bar_end, eod)
                break

        if bar_end >= eod:
            exit_spot, exit_reason, exit_ts = float(row["close"]), "EOD", eod
            break

    if exit_ts is None or exit_spot is None:
        return None

    pts = exit_spot - trigger if kind == "BEAR" else trigger - exit_spot
    return {
        "kind": kind,
        "direction": ltf_trap["direction"],
        "setup_ts": ltf_trap["setup_ts"],
        "entry_ts": entry_ts,
        "entry_price": trigger,
        "sl": sl,
        "target": target,
        "exit_ts": exit_ts,
        "exit_price": exit_spot,
        "exit_reason": exit_reason,
        "pts": round(pts, 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Backtest orchestration
# ──────────────────────────────────────────────────────────────────────────────


def run_backtest(
    df_1m: pd.DataFrame,
    lot_size: int,
    max_adx: float = DEFAULT_MAX_ADX,
    rsi_long_min: float = DEFAULT_RSI_LONG_MIN,
    rsi_short_max: float = DEFAULT_RSI_SHORT_MAX,
    use_vwap: bool = True,
    use_adx: bool = True,
    use_rsi: bool = True,
    entry_mode: str = "full",
) -> pd.DataFrame:
    df_ltf_full = prepare_ltf_with_indicators(df_1m, LTF_MIN)
    df_htf_full = resample_per_day(df_1m, HTF_MIN)

    all_trades: List[Dict] = []
    all_dates = sorted(df_1m["datetime"].dt.date.unique())

    for day in all_dates:
        day_1m = df_1m[df_1m["datetime"].dt.date == day]
        day_ltf = df_ltf_full[df_ltf_full["datetime"].dt.date == day]
        day_htf = df_htf_full[df_htf_full["datetime"].dt.date == day]

        if day_1m.empty or day_ltf.empty or day_htf.empty:
            continue

        eod = IST.localize(datetime.combine(day, INTRADAY_EXIT))
        in_position = False

        for htf in find_htf_traps(day_htf):
            if in_position:
                break
            for ltf in find_ltf_traps(day_ltf, htf, entry_mode=entry_mode):
                if in_position:
                    break
                if ltf["setup_ts"].time() > ENTRY_END:
                    continue
                if not check_ltf_filters(
                    ltf, df_ltf_full, max_adx, rsi_long_min, rsi_short_max,
                    use_vwap, use_adx, use_rsi,
                ):
                    continue
                trade = simulate_trade(ltf, day_1m, eod)
                if trade:
                    trade["pnl_rs"] = round(trade["pts"] * lot_size, 2)
                    all_trades.append(trade)
                    in_position = True

    return pd.DataFrame(all_trades)


def summarize(trades: pd.DataFrame) -> Dict:
    if trades.empty:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "gross_profit": 0.0, "gross_loss": 0.0, "net_pnl": 0.0,
            "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "rr": 0.0,
            "max_dd": 0.0,
        }

    pnls = trades["pnl_rs"].values
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    gp = wins.sum() if len(wins) else 0.0
    gl = abs(losses.sum()) if len(losses) else 0.0
    net = pnls.sum()
    pf = gp / gl if gl > 0 else float("inf")
    avg_win = wins.mean() if len(wins) else 0.0
    avg_loss = abs(losses.mean()) if len(losses) else 0.0
    rr = avg_win / avg_loss if avg_loss > 0 else 0.0

    cum = pnls.cumsum()
    cummax = np.maximum.accumulate(cum)
    max_dd = (cummax - cum).max()

    return {
        "total": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": 100 * len(wins) / len(trades), "gross_profit": gp,
        "gross_loss": gl, "net_pnl": net, "profit_factor": pf,
        "avg_win": avg_win, "avg_loss": avg_loss, "rr": rr, "max_dd": max_dd,
    }


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────


def available_symbols() -> List[str]:
    files = glob.glob(os.path.join(CACHE_DIR, "spot_*_1m_*.parquet"))
    symbols = set()
    for f in files:
        name = os.path.basename(f)
        # spot_SYMBOL_1m_...
        parts = name.split("_")
        if len(parts) >= 3 and parts[0] == "spot" and parts[2] == "1m":
            sym = parts[1]
            if sym != "NIFTY":  # exclude indices; keep FNO stocks only
                symbols.add(sym)
    return sorted(symbols)


def parse_symbols(arg: str, universe: pd.DataFrame) -> List[str]:
    if arg == "ALL":
        return available_symbols()
    symbols = [s.strip().upper() for s in arg.split(",")]
    return [s for s in symbols if s in universe["symbol"].values]


def main():
    parser = argparse.ArgumentParser(description="FNO 2-TF Spot Trap Backtest")
    parser.add_argument("--symbols", default="ALL", help="Comma-separated symbols or ALL")
    parser.add_argument("--start", type=date.fromisoformat, default=None)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--vwap", action="store_true", help="Enable VWAP filter")
    parser.add_argument("--no-vwap", action="store_true", help="Disable VWAP filter")
    parser.add_argument("--adx", type=float, default=DEFAULT_MAX_ADX)
    parser.add_argument("--rsi-long", type=float, default=DEFAULT_RSI_LONG_MIN)
    parser.add_argument("--rsi-short", type=float, default=DEFAULT_RSI_SHORT_MAX)
    parser.add_argument("--entry-mode", choices=["full", "third"], default="full",
                        help="Entry trigger mode: full LTF candle breach or 1/3 retracement")
    parser.add_argument("--out", default=None, help="Output CSV path")
    args = parser.parse_args()

    use_vwap = args.vwap if args.vwap else not args.no_vwap

    universe = load_stock_universe()
    symbols = parse_symbols(args.symbols, universe)
    if not symbols:
        print("No valid symbols selected.")
        return

    start = args.start or date(2026, 6, 1)
    end = args.end or date(2026, 7, 14)

    print(f"Running 2-TF backtest | symbols={len(symbols)} | VWAP={use_vwap} | ADX<{args.adx} | "
          f"RSI long>{args.rsi_long} short<{args.rsi_short} | entry={args.entry_mode} | {start} to {end}")
    print("-" * 100)

    rows = []
    all_trades = []
    for symbol in symbols:
        try:
            df_1m = load_1m_spot(symbol)
            df_1m = df_1m[(df_1m["datetime"].dt.date >= start) & (df_1m["datetime"].dt.date <= end)]
            if df_1m.empty:
                print(f"{symbol:15} no data in range")
                continue

            lot_size = int(universe[universe["symbol"] == symbol]["lot_size"].iloc[0])
            trades = run_backtest(
                df_1m,
                lot_size=lot_size,
                max_adx=args.adx,
                rsi_long_min=args.rsi_long,
                rsi_short_max=args.rsi_short,
                use_vwap=use_vwap,
                entry_mode=args.entry_mode,
            )
            s = summarize(trades)
            rows.append({
                "symbol": symbol,
                "lot_size": lot_size,
                "trades": s["total"],
                "wins": s["wins"],
                "losses": s["losses"],
                "win_rate": s["win_rate"],
                "net_pnl": s["net_pnl"],
                "profit_factor": s["profit_factor"],
                "avg_win": s["avg_win"],
                "avg_loss": s["avg_loss"],
                "rr": s["rr"],
                "max_dd": s["max_dd"],
            })
            if not trades.empty:
                trades["symbol"] = symbol
                trades["lot_size"] = lot_size
                all_trades.append(trades)

            print(f"{symbol:15} lot={lot_size:5} trades={s['total']:3} wins={s['wins']:3} ({s['win_rate']:5.1f}%) "
                  f"PF={s['profit_factor']:5.2f} net=Rs.{s['net_pnl']:>10,.2f} DD=Rs.{s['max_dd']:>10,.2f}")
        except Exception as e:
            print(f"{symbol:15} ERROR: {e}")

    if not rows:
        print("No results produced.")
        return

    summary_df = pd.DataFrame(rows).sort_values("net_pnl", ascending=False)
    print("-" * 100)
    print(summary_df.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    total_net = summary_df["net_pnl"].sum()
    total_trades = summary_df["trades"].sum()
    print("-" * 100)
    print(f"Aggregate: {total_trades} trades across {len(rows)} symbols | Total Net P&L = Rs.{total_net:,.2f}")

    if all_trades:
        combined = pd.concat(all_trades, ignore_index=True)
        out = args.out or os.path.join(
            OUTPUT_DIR,
            f"fno_2tf_trap_spot_{'vwap' if use_vwap else 'novwap'}_{args.entry_mode}_{start}_{end}.csv",
        )
        combined.to_csv(out, index=False)
        print(f"Saved {len(combined)} trade records to {out}")


if __name__ == "__main__":
    main()
