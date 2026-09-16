"""
scripts/oi_orb_multiday_full_stock_explanation.py

Direct user spec: "check each stock and check how far they went before
closing, and also check why some stocks did not touch the vwap, next
check why stoploss was hit -- i want each stock explanation one by one
in html and in ur terminal as well."

Covers all 35 real (date,symbol) entries from the just-completed 7-day
frozen-mechanic backtest that reached at least Step 2 (OI confirm):
  - 19 that TRADED: shows the full real option-premium path from entry
    to exit (max favorable / max adverse excursion + when they
    happened), not just the entry/exit snapshot -- "how far they went
    before closing."
  - 16 that stopped at no_vwap_retest: full real ARM/EXPIRE event
    history (RollingVwapRetestTracker's own internal events, normally
    discarded) from OI-confirm onward, plus the final price-vs-VWAP gap
    at EOD -- "why some stocks did not touch the VWAP."
  - SWIGGY (2026-09-02, the one real SL hit): additionally prints the
    exact 20-min HA bucket that satisfied _ha_vwap_close_sl_adverse
    (HA open/high/low/close, session VWAP as-of that bucket, shape+gap
    check) -- "why stoploss was hit."

Emits both a terminal report and an HTML file (written locally; publish
separately). Real data only, same real functions reused throughout
(RollingVwapRetestTracker, VwapState, OiOrbScreenerStrategy._ha_vwap_
close_sl_adverse, to_heikin_ashi, to_n_min_bars_market_anchored) --
consistent with every other 2026-09-16 script.

MUST run on EC2 (real Upstox account access tokens + real historical
range data).

Usage: python scripts/oi_orb_multiday_full_stock_explanation.py
"""
from __future__ import annotations

import asyncio
import html
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars_market_anchored
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy, _VWAP_SL_TF_MIN
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

EOD_TIME = "15:15"
VWAP_WINDOW_MIN = 15.0

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"

# (trade_date, symbol, side, oi_confirm_hhmm) -- real values from the
# just-completed 7-day backtest.
TRADED = [
    ("2026-09-01", "LTF", "PUT", "13:52", "14:33", "11.7", "15:15", "11.65", "eod_squareoff"),
    ("2026-09-01", "POLYCAB", "PUT", "10:44", "11:29", "242.45", "15:15", "377.0", "eod_squareoff"),
    ("2026-09-02", "BSE", "PUT", "11:13", "14:25", "117.4", "15:15", "115.25", "eod_squareoff"),
    ("2026-09-02", "EICHERMOT", "PUT", "11:29", "13:27", "148.0", "15:15", "121.5", "eod_squareoff"),
    ("2026-09-02", "HEROMOTOCO", "PUT", "11:22", "14:28", "107.25", "15:15", "103.0", "eod_squareoff"),
    ("2026-09-02", "SWIGGY", "PUT", "12:10", "12:24", "9.35", "14:55", "8.05", "vwap_close_sl"),
    ("2026-09-03", "APLAPOLLO", "PUT", "11:17", "14:24", "53.4", "15:15", "57.25", "eod_squareoff"),
    ("2026-09-03", "GODREJCP", "PUT", "09:17", "09:17", "18.8", "15:15", "17.4", "eod_squareoff"),
    ("2026-09-03", "SOLARINDS", "CALL", "11:07", "12:27", "800.05", "15:15", "870.35", "eod_squareoff"),
    ("2026-09-04", "ATHERENERG", "PUT", "11:10", "14:54", "61.3", "15:15", "67.75", "eod_squareoff"),
    ("2026-09-04", "HAVELLS", "PUT", "09:17", "09:31", "28.0", "15:15", "34.5", "eod_squareoff"),
    ("2026-09-04", "KEI", "PUT", "09:16", "09:27", "192.55", "15:15", "244.55", "eod_squareoff"),
    ("2026-09-04", "POLYCAB", "PUT", "09:16", "09:17", "218.7", "15:15", "225.75", "eod_squareoff"),
    ("2026-09-07", "MANAPPURAM", "PUT", "12:51", "14:22", "8.3", "15:15", "8.15", "eod_squareoff"),
    ("2026-09-07", "WIPRO", "PUT", "13:42", "15:11", "5.58", "15:15", "5.87", "eod_squareoff"),
    ("2026-09-08", "GVT&D", "CALL", "09:18", "09:27", "190.85", "15:15", "252.4", "eod_squareoff"),
    ("2026-09-08", "POWERINDIA", "PUT", "09:55", "10:13", "989.8", "15:15", "905.0", "eod_squareoff"),
    ("2026-09-09", "COFORGE", "PUT", "09:19", "09:23", "52.1", "15:15", "38.4", "eod_squareoff"),
    ("2026-09-09", "MUTHOOTFIN", "PUT", "11:31", "12:02", "57.05", "15:15", "57.0", "eod_squareoff"),
]

NO_VWAP_RETEST = [
    ("2026-09-01", "ASHOKLEY", "PUT", "14:13"),
    ("2026-09-01", "HEROMOTOCO", "CALL", "14:00"),
    ("2026-09-01", "KALYANKJIL", "CALL", "15:12"),
    ("2026-09-01", "KEI", "PUT", "09:57"),
    ("2026-09-01", "MARUTI", "PUT", "12:30"),
    ("2026-09-02", "VOLTAS", "PUT", "11:52"),
    ("2026-09-03", "KAYNES", "PUT", "12:35"),
    ("2026-09-03", "RBLBANK", "CALL", "11:23"),
    ("2026-09-04", "MOTILALOFS", "CALL", "14:16"),
    ("2026-09-07", "BOSCHLTD", "CALL", "11:46"),
    ("2026-09-07", "ICICIPRULI", "PUT", "11:00"),
    ("2026-09-07", "VMM", "PUT", "10:52"),
    ("2026-09-09", "INFY", "PUT", "11:56"),
    ("2026-09-09", "PERSISTENT", "PUT", "15:11"),
    ("2026-09-09", "TCS", "PUT", "15:09"),
    ("2026-09-09", "TECHM", "PUT", "13:34"),
]


def _access_tokens():
    db = ClientDB()
    tokens = []
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            tokens.append(creds["access_token"])
    if not tokens:
        raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")
    return tokens


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _explain_traded(tokens, book, trade_date_str, symbol, side, oi_confirm_hhmm,
                           entry_hhmm, entry_price, exit_hhmm, exit_price, exit_reason):
    trade_date = date.fromisoformat(trade_date_str)
    entry_ts = datetime.combine(trade_date, datetime.strptime(entry_hhmm, "%H:%M").time(), tzinfo=IST)
    exit_ts = datetime.combine(trade_date, datetime.strptime(exit_hhmm, "%H:%M").time(), tzinfo=IST)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    out = {"symbol": symbol, "date": trade_date_str, "side": side, "kind": "TRADED",
           "entry_ts": entry_hhmm, "entry_price": entry_price, "exit_ts": exit_hhmm,
           "exit_price": exit_price, "exit_reason": exit_reason,
           "pnl": round(float(exit_price) - float(entry_price), 2)}
    if not eq_key:
        out["error"] = "NO_EQ_KEY"
        return out
    entry_spot_rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    entry_bars = _to_bars(entry_spot_rows)
    entry_spot_candidates = [b for b in entry_bars if b.ts <= entry_ts]
    entry_spot = entry_spot_candidates[-1].close if entry_spot_candidates else None
    opt_type = "CE" if side == "CALL" else "PE"
    contract = None
    if entry_spot is not None:
        contract = await stock_resolve.resolve_contract_async(symbol, entry_spot, opt_type)
    if contract is None:
        out["error"] = "NO_CONTRACT"
        return out
    opt_rows = await hc.fetch_upstox_range_1m_multi_account(contract.upstox_key, tokens, trade_date, trade_date)
    opt_bars = _to_bars(opt_rows)
    between = [b for b in opt_bars if entry_ts <= b.ts <= exit_ts]
    if not between:
        out["error"] = "NO_OPTION_BARS_BETWEEN_ENTRY_EXIT"
        return out
    max_bar = max(between, key=lambda b: b.high)
    min_bar = min(between, key=lambda b: b.low)
    out["max_premium"] = max_bar.high
    out["max_premium_ts"] = max_bar.ts.strftime("%H:%M")
    out["min_premium"] = min_bar.low
    out["min_premium_ts"] = min_bar.ts.strftime("%H:%M")
    ep = float(entry_price)
    out["max_favorable_pts"] = round(max_bar.high - ep, 2)
    out["max_adverse_pts"] = round(ep - min_bar.low, 2)

    if symbol == "SWIGGY":
        # Real SL breakdown: walk 20-min HA buckets from entry, find the
        # exact one that satisfied _ha_vwap_close_sl_adverse.
        ha_1m = to_heikin_ashi(entry_bars)
        ha_tf = to_n_min_bars_market_anchored(ha_1m, _VWAP_SL_TF_MIN)
        vwap_state = VwapState()
        vwap_at_minute = {}
        for b in entry_bars:
            typical = (b.high + b.low + b.close) / 3.0
            vwap_state.update(symbol, typical, 1.0)
            v = vwap_state.current(symbol)
            if v is not None:
                vwap_at_minute[b.ts.replace(second=0, microsecond=0)] = v
        sorted_minutes = sorted(vwap_at_minute.keys())

        def _vwap_as_of(bucket_end):
            eligible = [ts for ts in sorted_minutes if ts < bucket_end]
            return vwap_at_minute[eligible[-1]] if eligible else None

        sl_detail = None
        entry_floor = entry_ts.replace(second=0, microsecond=0)
        for hb in ha_tf:
            bucket_end = hb.ts + timedelta(minutes=_VWAP_SL_TF_MIN)
            if bucket_end <= entry_floor or bucket_end > exit_ts + timedelta(minutes=1):
                continue
            vwap_now = _vwap_as_of(bucket_end)
            if vwap_now is None or vwap_now <= 0:
                continue
            adverse = OiOrbScreenerStrategy._ha_vwap_close_sl_adverse(hb, vwap_now, side)
            gap_pct = (vwap_now - hb.close) / vwap_now * 100.0 if side == "CALL" else \
                      (hb.close - vwap_now) / vwap_now * 100.0
            shape_ok = (hb.high == hb.open) if side == "CALL" else (hb.low == hb.open)
            if adverse:
                sl_detail = {
                    "bucket_start": hb.ts.strftime("%H:%M"), "bucket_end": bucket_end.strftime("%H:%M"),
                    "ha_open": round(hb.open, 2), "ha_high": round(hb.high, 2), "ha_low": round(hb.low, 2),
                    "ha_close": round(hb.close, 2), "vwap": round(vwap_now, 2), "gap_pct": round(gap_pct, 3),
                    "shape_ok": shape_ok,
                }
                break
        out["sl_detail"] = sl_detail
    return out


async def _explain_no_retest(tokens, symbol, side, trade_date_str, confirm_hhmm):
    trade_date = date.fromisoformat(trade_date_str)
    confirm_ts = datetime.combine(trade_date, datetime.strptime(confirm_hhmm, "%H:%M").time(), tzinfo=IST)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    out = {"symbol": symbol, "date": trade_date_str, "side": side, "kind": "NO_VWAP_RETEST",
           "confirm_ts": confirm_hhmm}
    if not eq_key:
        out["error"] = "NO_EQ_KEY"
        return out
    rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    bars = _to_bars(rows)
    if not bars:
        out["error"] = "NO_BARS"
        return out

    tracker = RollingVwapRetestTracker(window_min=VWAP_WINDOW_MIN)
    vwap_state = VwapState()
    events = []
    for b in bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(symbol, typical, 1.0)
        vwap = vwap_state.current(symbol)
        if vwap is None:
            continue
        bar_ts = b.ts.replace(second=0, microsecond=0)
        if bar_ts < confirm_ts.replace(second=0, microsecond=0):
            continue
        armed_before = tracker._armed and tracker._armed_side == side
        fired = tracker.check(side, bar_ts, b.close, vwap)
        armed_after = tracker._armed and tracker._armed_side == side
        if not armed_before and armed_after:
            events.append((bar_ts, "ARM", b.close, vwap))
        elif armed_before and not armed_after and not fired:
            events.append((bar_ts, "EXPIRE", b.close, vwap))
        if fired:
            events.append((bar_ts, "FIRE", b.close, vwap))
    out["events"] = [(ts.strftime("%H:%M"), kind, round(price, 2), round(vwap, 2))
                      for ts, kind, price, vwap in events]
    post_confirm_bars = [b for b in bars if b.ts >= confirm_ts]
    if post_confirm_bars:
        last = post_confirm_bars[-1]
        last_vwap = vwap_state.current(symbol)
        out["last_ts"] = last.ts.strftime("%H:%M")
        out["last_price"] = round(last.close, 2)
        out["last_vwap"] = round(last_vwap, 2) if last_vwap else None
        if last_vwap:
            out["final_gap_pct"] = round((last.close - last_vwap) / last_vwap * 100.0, 3)
    return out


def _print_traded(o):
    print(f"\n{'-'*130}\n{o['symbol']} ({o['date']}, {o['side']}) -- TRADED\n{'-'*130}")
    if "error" in o:
        print(f"  Could not compute full excursion: {o['error']} (already-known entry/exit still real)")
    print(f"  Entry: {o['entry_ts']} @ {o['entry_price']}   Exit: {o['exit_ts']} @ {o['exit_price']} "
          f"[{o['exit_reason']}]   PNL={o['pnl']:+.2f}")
    if "max_premium" in o:
        print(f"  Real path between entry and exit: HIGH={o['max_premium']} @ {o['max_premium_ts']} "
              f"(max favorable {o['max_favorable_pts']:+.2f} pts)   "
              f"LOW={o['min_premium']} @ {o['min_premium_ts']} (max adverse -{o['max_adverse_pts']:.2f} pts)")
    if o.get("sl_detail"):
        d = o["sl_detail"]
        print(f"  SL FIRED in 20-min bucket [{d['bucket_start']}-{d['bucket_end']}]: "
              f"HA(open={d['ha_open']} high={d['ha_high']} low={d['ha_low']} close={d['ha_close']}) "
              f"vs VWAP={d['vwap']} gap={d['gap_pct']:+.3f}% shape_ok={d['shape_ok']}")


def _print_no_retest(o):
    print(f"\n{'-'*130}\n{o['symbol']} ({o['date']}, {o['side']}) -- NO VWAP RETEST (never fired)\n{'-'*130}")
    if "error" in o:
        print(f"  Could not fetch real data: {o['error']}")
        return
    print(f"  OI confirmed at {o['confirm_ts']}, real events from there onward:")
    if not o["events"]:
        print(f"    Price never even armed to the {o['side']}-side of VWAP after OI confirm.")
    for ts, kind, price, vwap in o["events"]:
        print(f"    {ts}  {kind:8s} price={price} vwap={vwap}")
    if "last_price" in o:
        print(f"  Last real reading of the day: {o['last_ts']} price={o['last_price']} vwap={o['last_vwap']} "
              f"(gap={o.get('final_gap_pct', '?'):+.3f}%)")


async def main():
    tokens = _access_tokens()
    bus_book_dummy = None

    class _NullBus:
        def subscribe(self, topic): return None
        def unsubscribe(self, topic, q): pass
        async def publish(self, topic, event): pass

    book = OiOrbScreenerStrategy(
        _NullBus(), cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )

    print("=" * 130)
    print("OI-ORB Screener -- FULL per-stock explanation, real 7-day backtest (19 traded + 16 no-VWAP-retest)")
    print("=" * 130)

    traded_results = []
    for row in TRADED:
        trade_date_str, symbol, side, oi_confirm_hhmm, entry_hhmm, entry_price, exit_hhmm, exit_price, exit_reason = row
        r = await _explain_traded(tokens, book, trade_date_str, symbol, side, oi_confirm_hhmm,
                                   entry_hhmm, entry_price, exit_hhmm, exit_price, exit_reason)
        traded_results.append(r)
        _print_traded(r)

    no_retest_results = []
    for trade_date_str, symbol, side, confirm_hhmm in NO_VWAP_RETEST:
        r = await _explain_no_retest(tokens, symbol, side, trade_date_str, confirm_hhmm)
        no_retest_results.append(r)
        _print_no_retest(r)

    print("\n" + "=" * 130)
    print("Writing HTML report...")

    def esc(x):
        return html.escape(str(x))

    rows_html = []
    for o in traded_results:
        excursion = ""
        if "max_premium" in o:
            excursion = (f"High {esc(o['max_premium'])}@{esc(o['max_premium_ts'])} "
                         f"(+{esc(o['max_favorable_pts'])}) / Low {esc(o['min_premium'])}@{esc(o['min_premium_ts'])} "
                         f"(-{esc(o['max_adverse_pts'])})")
        sl_html = ""
        if o.get("sl_detail"):
            d = o["sl_detail"]
            sl_html = (f"<br><span class='sl-note'>SL bucket [{esc(d['bucket_start'])}-{esc(d['bucket_end'])}]: "
                       f"HA O={esc(d['ha_open'])} H={esc(d['ha_high'])} L={esc(d['ha_low'])} C={esc(d['ha_close'])} "
                       f"vs VWAP={esc(d['vwap'])} gap={esc(d['gap_pct'])}% shape_ok={esc(d['shape_ok'])}</span>")
        pnl_cls = "pos" if o["pnl"] >= 0 else "neg"
        rows_html.append(f"""
<div class="card">
  <div class="head"><span class="sym">{esc(o['symbol'])}</span> <span class="side">{esc(o['side'])}</span>
    <span class="date">{esc(o['date'])}</span><span class="pnl {pnl_cls}">{o['pnl']:+.2f} pts</span></div>
  <div class="body">Entry {esc(o['entry_ts'])} @ {esc(o['entry_price'])} &rarr; Exit {esc(o['exit_ts'])} @
    {esc(o['exit_price'])} [{esc(o['exit_reason'])}]<br>
    Path: {excursion}{sl_html}</div>
</div>""")

    for o in no_retest_results:
        events_html = "".join(
            f"<div class='ev'>{esc(ts)} <b>{esc(kind)}</b> price={esc(price)} vwap={esc(vwap)}</div>"
            for ts, kind, price, vwap in o.get("events", [])
        ) or "<div class='ev'>Never armed after OI confirm.</div>"
        last_html = ""
        if "last_price" in o:
            last_html = (f"<div class='last'>Last reading {esc(o['last_ts'])}: price={esc(o['last_price'])} "
                         f"vwap={esc(o['last_vwap'])} gap={esc(o.get('final_gap_pct'))}%</div>")
        rows_html.append(f"""
<div class="card">
  <div class="head"><span class="sym">{esc(o['symbol'])}</span> <span class="side">{esc(o['side'])}</span>
    <span class="date">{esc(o['date'])}</span><span class="tag">no retest</span></div>
  <div class="body">OI confirmed {esc(o['confirm_ts'])}<br>{events_html}{last_html}</div>
</div>""")

    html_doc = f"""<title>OI-ORB Multi-Day Stock Explanations</title>
<style>
body{{font-family:-apple-system,Segoe UI,sans-serif;background:#faf8f5;color:#1c1a17;margin:0;padding:24px}}
.card{{background:#fff;border:1px solid #e5ddd0;border-radius:10px;padding:14px 16px;margin-bottom:12px}}
.head{{display:flex;gap:10px;align-items:baseline;margin-bottom:6px}}
.sym{{font-weight:700;font-family:monospace}}
.side{{color:#a5502b}}
.date{{color:#6b6357;font-size:.85rem}}
.pnl{{margin-left:auto;font-family:monospace;font-weight:700}}
.pnl.pos{{color:#1c6e4a}}
.pnl.neg{{color:#a3312a}}
.tag{{margin-left:auto;background:#eee;padding:2px 8px;border-radius:99px;font-size:.75rem}}
.body{{font-size:.86rem;line-height:1.6;font-family:monospace}}
.ev{{padding:2px 0}}
.sl-note{{color:#a3312a}}
.last{{margin-top:4px;color:#6b6357}}
h2{{margin-top:32px}}
</style>
<h1>OI-ORB Screener &mdash; Full Per-Stock Explanation (7-day real backtest)</h1>
<h2>Traded (19)</h2>
{''.join(rows_html[:len(traded_results)])}
<h2>No VWAP retest (16)</h2>
{''.join(rows_html[len(traded_results):])}
"""
    out_path = "scratch_oi_orb_stock_explanations.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_doc)
    print(f"HTML written to {out_path} -- copy it back for publishing.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
