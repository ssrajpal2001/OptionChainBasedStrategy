"""
backtest_crude_compare.py — CrudeOil Two-Approach Comparison Backtest
Approach A: 15-min FUT bias + 5-min OPT LTF + 1-ITM strike (intraday)
Approach B: 30-min FUT bias + 5-min OPT LTF + 2-ITM strike (HTF)
Usage: python3 scripts/backtest_crude_compare.py --token YOUR_TOKEN --days 7 --lots 2
"""
from __future__ import annotations
import argparse, gzip, json, sys, os, time
from datetime import date, timedelta
import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from strategies.trap_scanner import scanner

CRUDE_STEP=100; CRUDE_LOT=100; SL_BUF=20.0
ENTRY_OPEN="09:30"; SQ_OFF="23:00"; MKT_OPEN="09:00"; MKT_CLOSE="23:30"
HTF_A=15; HTF_B=30; LTF_MIN=5; ITM_STEPS_A=1; ITM_STEPS_B=2
HEADERS: dict = {}
_MCX_MASTER: list = []

def _get(url, retries=3):
    for _ in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 429: time.sleep(2); continue
            return r.json() if r.status_code == 200 else {}
        except: time.sleep(1)
    return {}

def fetch_1m(key, dt):
    enc=key.replace("|","%7C")
    data=_get(f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/{dt}/{dt}")
    cands=data.get("data",{}).get("candles",[])
    if not cands: return pd.DataFrame()
    df=pd.DataFrame(cands,columns=["datetime","open","high","low","close","volume","oi"])
    df["datetime"]=pd.to_datetime(df["datetime"])
    df=df.sort_values("datetime").reset_index(drop=True)
    df=df[(df["datetime"].dt.strftime("%H:%M")>=MKT_OPEN)&(df["datetime"].dt.strftime("%H:%M")<=MKT_CLOSE)]
    return df

def resample(df, minutes):
    if df.empty: return df
    r=df.set_index("datetime").resample(f"{minutes}min",label="right",closed="right").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna().reset_index()
    return r

def load_mcx_master():
    global _MCX_MASTER
    if _MCX_MASTER: return
    try:
        r=requests.get("https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz",timeout=30)
        _MCX_MASTER=json.loads(gzip.decompress(r.content))
        print(f"  [master] MCX: {len(_MCX_MASTER)} instruments")
    except Exception as e: print(f"  [master] failed: {e}")

def find_crude_option(strike, otype, min_expiry):
    load_mcx_master()
    ot=otype.upper(); candidates=[]
    for row in _MCX_MASTER:
        itype=str(row.get("instrument_type","")).upper()
        row_otype=itype if itype in ("CE","PE") else str(row.get("option_type","")).upper()
        if row_otype!=ot: continue
        if abs(float(row.get("strike",0) or 0)-strike)>0.5: continue
        sym=str(row.get("tradingsymbol","") or row.get("name","")).upper()
        und=str(row.get("underlying_symbol","") or "").upper()
        if "CRUDE" not in sym and "CRUDE" not in und: continue
        exp_str=str(row.get("expiry","") or "")
        try: exp_dt=date.fromisoformat(exp_str[:10])
        except: continue
        if exp_dt<min_expiry: continue
        key=str(row.get("instrument_key",""))
        if key: candidates.append((exp_dt,key))
    if not candidates: return ""
    candidates.sort(key=lambda x:x[0])
    return candidates[0][1]

def get_atm(fut_df, step):
    if fut_df.empty: return 0
    return int(round(float(fut_df.iloc[0]["open"])/step)*step)

def get_trading_days(n):
    days=[]; d=date.today()
    while len(days)<n:
        d-=timedelta(days=1)
        if d.weekday()<5: days.append(d.isoformat())
    return list(reversed(days))

def all_sellers_cleared(opt_5m):
    if opt_5m.empty or len(opt_5m)<3: return False
    _,entries=scanner.scan_htf(opt_5m)
    trapped=[e for e in entries if e["status"]=="TRAPPED"]
    closed=[e for e in entries if e["status"]=="CLOSED"]
    return len(closed)>0 and len(trapped)==0

def ts_fmt(ts):
    return ts.strftime("%H:%M") if hasattr(ts,"strftime") else str(ts)[:16]

def record_exit(trade, exit_price, exit_ts, reason, out):
    pnl=round(exit_price-trade["entry_price"],2)
    out.append({"approach":trade["approach"],"opt_type":trade["opt_type"],
        "strike":trade["strike"],"entry_ts":ts_fmt(trade["entry_ts"]),"exit_ts":ts_fmt(exit_ts),
        "entry":round(trade["entry_price"],2),"exit":round(exit_price,2),
        "sl":round(trade["sl_price"],2),"t1":round(trade["t1_price"],2),
        "t1_hit":trade["t1_hit"],"result":reason,"pnl_pts":pnl,
        "fut_entry":round(trade.get("fut_entry",0),2),
        "bias_zone":trade.get("bias_zone","")})

def run_day(trade_date, fut_df, ce_df, pe_df, ce_strike, pe_strike, htf_min, approach):
    sq_time=pd.Timestamp(f"{trade_date} {SQ_OFF}")
    entry_open=pd.Timestamp(f"{trade_date} {ENTRY_OPEN}")
    trades=[]

    def _run_side(opt_type, opt_df, strike):
        if fut_df.empty or opt_df.empty: return
        in_trade=None; notified=set()
        fut_htf=resample(fut_df,htf_min)
        for _,row in fut_htf.iterrows():
            bar_ts=row["datetime"]; cur_fut=float(row["close"])
            if bar_ts<entry_open: continue
            if bar_ts>=sq_time:
                if in_trade:
                    ob=opt_df[opt_df["datetime"]<=bar_ts]
                    ep=float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry_price"]
                    record_exit(in_trade,ep,bar_ts,"EOD",trades); in_trade=None
                break
            if in_trade:
                fut_fwd=fut_df[(fut_df["datetime"]>in_trade["entry_ts"])&(fut_df["datetime"]<=bar_ts)]
                result=None
                for _,fb in fut_fwd.iterrows():
                    flo,fhi,fts=float(fb["low"]),float(fb["high"]),fb["datetime"]
                    if fts>=sq_time:
                        ob=opt_df[opt_df["datetime"]<=fts]
                        ep=float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry_price"]
                        result=("EOD",ep,fts); break
                    if not in_trade["t1_hit"]:
                        t1h=(fhi>=in_trade["t1_price"]) if opt_type=="CE" else (flo<=in_trade["t1_price"])
                        if t1h:
                            in_trade["t1_hit"]=True
                            ob=opt_df[opt_df["datetime"]<=fts]
                            ep=float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry_price"]
                            result=("T1",ep,fts); break
                    slh=(flo<=in_trade["sl_price"]) if opt_type=="CE" else (fhi>=in_trade["sl_price"])
                    if slh:
                        ob=opt_df[opt_df["datetime"]<=fts]
                        ep=float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry_price"]
                        result=("SL",ep,fts); break
                if result:
                    record_exit(in_trade,result[1],result[2],result[0],trades); in_trade=None
                continue
            hist_fut=fut_df[fut_df["datetime"]<=bar_ts]
            htf=resample(hist_fut,htf_min)
            if len(htf)<2: continue
            _,all_zones=scanner.scan_htf_spot(htf)
            trapped=[e for e in all_zones if e["status"]=="TRAPPED"]
            kind_want="BEAR" if opt_type=="CE" else "BULL"
            bias_zones=[e for e in trapped if e.get("kind")==kind_want and cur_fut>=e.get("zone_low",0)]
            if opt_type=="CE":
                bias_zones=[e for e in bias_zones if cur_fut<=e.get("zone_high",0)]
            if not bias_zones: continue
            zone=min(bias_zones,key=lambda e:abs(cur_fut-e.get("zone_low",cur_fut)))
            uid=f"{zone.get('ref_ts','')}_{zone.get('zone_high',0):.1f}_{kind_want}"
            if uid in notified: continue
            opt_today=opt_df[(opt_df["datetime"].dt.date==pd.Timestamp(trade_date).date())&(opt_df["datetime"]<=bar_ts)]
            opt_5m=resample(opt_today,LTF_MIN)
            if not all_sellers_cleared(opt_5m): continue
            ob=opt_df[opt_df["datetime"]<=bar_ts]
            if ob.empty: continue
            entry_opt_p=float(ob.iloc[-1]["close"])
            if entry_opt_p<=0: continue
            sl_p=round(zone["zone_low"]-SL_BUF,1) if opt_type=="CE" else round(zone["zone_high"]+SL_BUF,1)
            t1_p=zone.get("sl",zone["zone_high"]+50) if opt_type=="CE" else zone.get("sl",zone["zone_low"]-50)
            notified.add(uid)
            in_trade={"approach":approach,"opt_type":opt_type,"strike":strike,
                "entry_price":entry_opt_p,"sl_price":sl_p,"t1_price":t1_p,
                "t1_hit":False,"entry_ts":bar_ts,"fut_entry":cur_fut,
                "bias_zone":f"{zone.get('zone_low',0):.0f}→{zone.get('zone_high',0):.0f}"}
        if in_trade and not fut_df.empty:
            ob=opt_df[opt_df["datetime"]<=sq_time]
            ep=float(ob.iloc[-1]["close"]) if not ob.empty else in_trade["entry_price"]
            record_exit(in_trade,ep,sq_time,"EOD",trades)

    _run_side("CE",ce_df,ce_strike)
    _run_side("PE",pe_df,pe_strike)
    return trades

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--token",required=True)
    parser.add_argument("--days",type=int,default=7)
    parser.add_argument("--lots",type=int,default=2)
    parser.add_argument("--fut-key",default="MCX_FO|520702")
    args=parser.parse_args()
    global HEADERS
    HEADERS={"Authorization":f"Bearer {args.token}","Accept":"application/json"}
    trading_days=get_trading_days(args.days)
    lot_size=CRUDE_LOT*args.lots
    print(f"\nCrudeOil Comparison Backtest  {trading_days[0]} → {trading_days[-1]}")
    print(f"Approach A: 15-min FUT + 5-min OPT + 1-ITM   Approach B: 30-min FUT + 5-min OPT + 2-ITM\n")
    all_results=[]
    for trade_date in trading_days:
        print(f"\n{'─'*55}  {trade_date}")
        print(f"  Fetching futures... ",end="",flush=True)
        fut_df=fetch_1m(args.fut_key,trade_date); print(f"{len(fut_df)} bars"); time.sleep(0.5)
        if fut_df.empty: print("  No data — skip"); continue
        atm=get_atm(fut_df,CRUDE_STEP)
        if not atm: print("  No ATM — skip"); continue
        ce_a=atm-ITM_STEPS_A*CRUDE_STEP; pe_a=atm+ITM_STEPS_A*CRUDE_STEP
        ce_b=atm-ITM_STEPS_B*CRUDE_STEP; pe_b=atm+ITM_STEPS_B*CRUDE_STEP
        print(f"  ATM={atm}  A:CE={ce_a}/PE={pe_a}  B:CE={ce_b}/PE={pe_b}")
        dt_obj=date.fromisoformat(trade_date)
        print(f"  Finding option keys... ",end="",flush=True)
        ce_key_a=find_crude_option(ce_a,"CE",dt_obj); pe_key_a=find_crude_option(pe_a,"PE",dt_obj)
        ce_key_b=find_crude_option(ce_b,"CE",dt_obj); pe_key_b=find_crude_option(pe_b,"PE",dt_obj)
        print(f"CE_A={'OK' if ce_key_a else 'X'} PE_A={'OK' if pe_key_a else 'X'} CE_B={'OK' if ce_key_b else 'X'} PE_B={'OK' if pe_key_b else 'X'}")
        def _fo(key,lbl):
            if not key: return pd.DataFrame()
            print(f"  Fetching {lbl}... ",end="",flush=True)
            df=fetch_1m(key,trade_date); print(f"{len(df)} bars"); time.sleep(0.4); return df
        ce_df_a=_fo(ce_key_a,f"CE{ce_a}(A)"); pe_df_a=_fo(pe_key_a,f"PE{pe_a}(A)")
        ce_df_b=_fo(ce_key_b,f"CE{ce_b}(B)") if ce_b!=ce_a else ce_df_a
        pe_df_b=_fo(pe_key_b,f"PE{pe_b}(B)") if pe_b!=pe_a else pe_df_a
        trades_a=run_day(trade_date,fut_df,ce_df_a,pe_df_a,ce_a,pe_a,HTF_A,"A-15m")
        trades_b=run_day(trade_date,fut_df,ce_df_b,pe_df_b,ce_b,pe_b,HTF_B,"B-30m")
        for lbl,trades in [("A (15-min)",trades_a),("B (30-min)",trades_b)]:
            day_pnl=sum(t["pnl_pts"]*lot_size for t in trades)
            print(f"\n  Approach {lbl}:")
            if not trades: print("    No trades")
            for t in trades:
                win="WIN " if t["pnl_pts"]>0 else "LOSS"
                print(f"    {t['entry_ts']}→{t['exit_ts']}  {t['opt_type']} {t['strike']}"
                      f"  opt:{t['entry']:.1f}→{t['exit']:.1f}"
                      f"  fut@entry={t['fut_entry']:.0f} zone={t['bias_zone']}"
                      f"  {t['result']:<6} {t['pnl_pts']:+.1f}pts"
                      f"  Rs{t['pnl_pts']*lot_size:+.0f}  {win}")
            print(f"    Day P&L: Rs{day_pnl:+.0f}")
            for t in trades: t["date"]=trade_date
            all_results.extend(trades)
    if not all_results: print("\nNo trades."); return
    df_res=pd.DataFrame(all_results)
    print(f"\n{'='*60}\n  SUMMARY\n{'='*60}")
    for ap,lbl in [("A-15m","A 15-min intraday"),("B-30m","B 30-min HTF")]:
        sub=df_res[df_res["approach"]==ap]
        if sub.empty: print(f"\n  {lbl}: No trades"); continue
        wins=sub[sub["pnl_pts"]>0]; losses=sub[sub["pnl_pts"]<=0]
        total=sub["pnl_pts"].sum()*lot_size
        print(f"\n  Approach {lbl}")
        print(f"    Trades={len(sub)} W={len(wins)} L={len(losses)} Win%={len(wins)/len(sub)*100:.0f}%")
        print(f"    Total P&L=Rs{total:+,.0f}  AvgWin=Rs{wins['pnl_pts'].mean()*lot_size if len(wins) else 0:+,.0f}  AvgLoss=Rs{losses['pnl_pts'].mean()*lot_size if len(losses) else 0:+,.0f}")
        for dt,grp in sub.groupby("date"):
            print(f"      {dt}  {len(grp)} trades  Rs{grp['pnl_pts'].sum()*lot_size:+,.0f}")

if __name__=="__main__":
    main()
