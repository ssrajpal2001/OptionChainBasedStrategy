"""
btc_cascade_replay.py — replay BTC 1m cache through the 3-stage cascade.
Mirrors engine._check_mtf_zone / _check_ob_gate / _check_futures_arm_entry exactly.

HTF=3m  MTF=2m  LTF=1m  OB=ON  SL=100pts
"""
import sys, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd
from strategies.trap_scanner import scanner as sc

HTF_MIN  = 3
MTF_MIN  = 2
SL_BUF   = 100.0
ZIGZAG   = 9
LOOKBACK = 500   # 1m bars of context fed into each check

def resample(df1m_ctx, minutes):
    r = df1m_ctx[["open","high","low","close","volume"]].resample(f"{minutes}min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"]).reset_index()
    return r

def _uid(z):
    return (round(z.get("zone_low",0),1), round(z.get("zone_high",0),1), str(z.get("ref_ts","")))

def run():
    print("Loading BTC 1m cache...")
    df1m = pd.read_parquet(os.path.join(ROOT,"data","btc_1m_cache.parquet"))
    df1m["datetime"] = pd.to_datetime(df1m["datetime"])
    df1m = df1m.set_index("datetime").sort_index()
    df1m = df1m.tail(5 * 1440)   # last 5 days
    print(f"  {len(df1m)} bars  {df1m.index[0]} → {df1m.index[-1]}\n")

    entries, exits = [], []

    # ── cascade state ──
    htf_zone      = None
    htf_side      = None
    htf_ref_ts    = None
    mtf_zone      = None
    ob_gate_armed = False
    ltf_candle    = None    # {"high","low"} — current 1m bar waiting for break
    position      = None    # {"side","entry","sl"}
    notified      = set()

    bars = []

    for ts, row in df1m.iterrows():
        bars.append({
            "datetime": ts, "open": row.open, "high": row.high,
            "low": row.low, "close": row.close, "volume": row.volume
        })
        if len(bars) < LOOKBACK:
            continue

        ctx = pd.DataFrame(bars[-LOOKBACK:]).set_index("datetime")
        spot = float(row.close)

        # ── SL check ────────────────────────────────────────────────────────
        if position:
            side = position["side"]
            hit_sl = (side == "PE" and spot < position["sl"]) or \
                     (side == "CE" and spot > position["sl"])
            if hit_sl:
                pnl = (spot - position["entry"]) * (1 if side=="PE" else -1)
                exits.append({"ts":ts,"side":side,"reason":"SL",
                               "entry":position["entry"],"exit":spot,"pnl":round(pnl,1)})
                print(f"  EXIT  SL  [{ts}] {side} entry={position['entry']:.1f} "
                      f"sl={position['sl']:.1f} exit={spot:.1f}  P&L={pnl:+.1f}")
                position = None
                htf_zone = None; htf_side = None; htf_ref_ts = None
                mtf_zone = None; ob_gate_armed = False; ltf_candle = None
            continue   # no new entries while in position

        # ── Stage 3: entry on 1m candle break ───────────────────────────────
        if mtf_zone and ob_gate_armed and ltf_candle:
            if htf_side == "PE" and spot > ltf_candle["high"]:
                sl = spot - SL_BUF
                position = {"side":"PE","entry":spot,"sl":sl}
                entries.append({"ts":ts,"side":"PE","entry":spot,"sl":sl})
                print(f"ENTRY PE [{ts}] spot={spot:.1f}  sl={sl:.1f}")
                ltf_candle = None
                continue
            if htf_side == "CE" and spot < ltf_candle["low"]:
                sl = spot + SL_BUF
                position = {"side":"CE","entry":spot,"sl":sl}
                entries.append({"ts":ts,"side":"CE","entry":spot,"sl":sl})
                print(f"ENTRY CE [{ts}] spot={spot:.1f}  sl={sl:.1f}")
                ltf_candle = None
                continue
            # update 1m candle each bar
            ltf_candle = {"high": float(row.high), "low": float(row.low)}

        # ── Stage 1: HTF zone entry ─────────────────────────────────────────
        htf_df = resample(ctx, HTF_MIN)
        if len(htf_df) < 6:
            continue
        htf_zones = sc.scan_htf(htf_df)

        if not htf_zone:
            for z in htf_zones:
                if z.get("status") != "TRAPPED":
                    continue
                uid = _uid(z)
                if uid in notified:
                    continue
                zl, zh = z.get("zone_low",0), z.get("zone_high",0)
                if zl <= spot <= zh:
                    htf_zone  = z
                    htf_side  = "CE" if z.get("kind","BEAR")=="BEAR" else "PE"
                    htf_ref_ts = z.get("ref_ts")
                    print(f"S1 HTF  [{ts}] {z.get('kind')} [{zl:.1f}–{zh:.1f}]"
                          f" formed@{str(htf_ref_ts)[11:16]}  spot={spot:.1f}  side={htf_side}")
                    break
            continue

        # ── HTF exit ────────────────────────────────────────────────────────
        zl, zh = htf_zone.get("zone_low",0), htf_zone.get("zone_high",0)
        if (htf_side == "CE" and spot < zl) or (htf_side == "PE" and spot > zh):
            print(f"  HTF EXIT [{ts}] spot={spot:.1f} left zone [{zl:.1f}–{zh:.1f}]")
            notified.add(_uid(htf_zone))
            htf_zone = None; htf_side = None; htf_ref_ts = None
            mtf_zone = None; ob_gate_armed = False; ltf_candle = None
            continue

        # ── Stage 2: MTF zone ───────────────────────────────────────────────
        if not mtf_zone:
            mtf_df = resample(ctx, MTF_MIN)
            if len(mtf_df) >= 3:
                mtf_zones = sc.scan_htf(mtf_df)
                kind_filter = "BEAR" if htf_side=="CE" else "BULL"
                htf_ts_pd = pd.Timestamp(htf_ref_ts) if htf_ref_ts else None
                for z in mtf_zones:
                    if z.get("status") != "TRAPPED" or z.get("kind","BEAR") != kind_filter:
                        continue
                    z_ts = pd.Timestamp(z.get("ref_ts")) if z.get("ref_ts") else None
                    if htf_ts_pd and z_ts and z_ts < htf_ts_pd:
                        continue
                    zml, zmh = z.get("zone_low",0), z.get("zone_high",0)
                    if zml <= spot <= zmh:
                        mtf_zone = z
                        print(f"  S2 MTF [{ts}] {kind_filter} [{zml:.1f}–{zmh:.1f}]"
                              f" formed@{str(z.get('ref_ts',''))[11:16]}  spot={spot:.1f}")
                        break

        # ── Stage 2.5: OB + CHoCH gate ──────────────────────────────────────
        if mtf_zone and not ob_gate_armed:
            try:
                df_ob = ctx.reset_index()
                bull_obs, bear_obs = sc.active_order_blocks(df_ob, zigzag_len=ZIGZAG)
                signals   = sc.detect_choch_bos(df_ob, zigzag_len=ZIGZAG)
                choch_dir = sc.last_choch_direction(signals)
            except Exception as exc:
                choch_dir = None
                bull_obs = bear_obs = []
            if htf_side == "PE" and choch_dir == "DOWN":
                ob = sc.price_in_order_block(spot, bear_obs)
                if ob:
                    ob_gate_armed = True
                    print(f"  S2.5 OB [{ts}] CHoCH=DOWN bear [{ob['zone_low']:.1f}–{ob['zone_high']:.1f}]  spot={spot:.1f}")
            elif htf_side == "CE" and choch_dir == "UP":
                ob = sc.price_in_order_block(spot, bull_obs)
                if ob:
                    ob_gate_armed = True
                    print(f"  S2.5 OB [{ts}] CHoCH=UP   bull [{ob['zone_low']:.1f}–{ob['zone_high']:.1f}]  spot={spot:.1f}")

        # ── Arm LTF candle ───────────────────────────────────────────────────
        if mtf_zone and ob_gate_armed and ltf_candle is None:
            ltf_candle = {"high": float(row.high), "low": float(row.low)}
            print(f"  S3 LTF [{ts}] candle armed H={ltf_candle['high']:.1f} L={ltf_candle['low']:.1f}")

    # ── summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"REPLAY  HTF={HTF_MIN}m  MTF={MTF_MIN}m  LTF=1m  OB=ON  SL={SL_BUF}pts")
    print(f"  Entries={len(entries)}  Exits={len(exits)}  Open={len(entries)-len(exits)}")
    wins   = [e for e in exits if e["pnl"]>0]
    losses = [e for e in exits if e["pnl"]<=0]
    if exits:
        avg_win  = sum(e["pnl"] for e in wins)/len(wins)   if wins   else 0
        avg_loss = sum(e["pnl"] for e in losses)/len(losses) if losses else 0
        print(f"  Wins={len(wins)} avg={avg_win:.1f}  Losses={len(losses)} avg={avg_loss:.1f}")
        print(f"  Net P&L={sum(e['pnl'] for e in exits):.1f} pts")

if __name__ == "__main__":
    run()
