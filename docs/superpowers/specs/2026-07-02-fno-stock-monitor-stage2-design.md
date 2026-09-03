# FnO Stock Monitor — Stage 2 Design
**Date:** 2026-07-02
**Status:** Approved
**Phase:** 2 — Intraday MTF+LTF cascade alert (no auto-trade)

---

## Goal

Watch the stocks shortlisted by the Stage 1 nightly scan during market hours.
Build 15m (MTF) and 5m (LTF) candles from live Upstox spot ticks. When MTF
confirms a trap zone in the same direction as the D1 zone, arm LTF. When LTF
also shows a trapped zone, fire a dashboard alert with full trade details so
the user can decide to enter manually.

---

## Trigger & Scope

- Starts automatically when `run_system.py` launches, **if** today's scan file
  exists (`data/fno_scan_YYYY-MM-DD.json`).
- Active window: **9:15 AM – 3:15 PM IST** only.
- Covers every stock in the scan file (both CE and PE entries).
- No order execution. Alert only.

---

## Data Flow

```
Upstox WS → raw spot tick for EICHERMOT / BAJAJFINSV / …
     ↓  (published as Topic.INDEX_TICK by existing feeder)
FnoStockMonitor._on_index_tick()
     → ignored if symbol not in _watched (scan file stocks only)
     ↓
_update_bucket("15m", symbol, ltp, ts)
     → on bucket close → _on_mtf_candle(symbol, bar)
          → scan_htf_spot(accumulated 15m bars)
          → if new TRAPPED zone matches D1 direction → arm LTF for symbol

_update_bucket("5m", symbol, ltp, ts)
     → on bucket close → _on_ltf_candle(symbol, bar)
          → if LTF armed for symbol:
               scan_htf_spot(accumulated 5m bars)
               → if new TRAPPED zone → publish Topic.FNO_STOCK_ALERT
     ↓
ws_bridge → WebSocket → monitor.html Stocks tab → alert card + sound
```

---

## Detection Logic

Stage 1 already confirmed the D1 zone. Stage 2 adds two more confirmation
layers using the **same** `scanner.scan_htf_spot()` function on shorter bars.

### MTF (15m) — confirmation
- Accumulate intraday 15m spot bars for each watched stock.
- On every 15m close, call `scan_htf_spot(15m_bars)`.
- Look for any zone with `status == "TRAPPED"` and `kind` matching the D1
  direction (`BEAR` → CE, `BULL` → PE) formed **today**.
- If found: mark `_ltf_armed[symbol] = True`. Log MTF trap price.

### LTF (5m) — entry signal
- Accumulate intraday 5m spot bars.
- On every 5m close, **only if** `_ltf_armed[symbol]`:
  - Call `scan_htf_spot(5m_bars)`.
  - Look for any zone with `status == "TRAPPED"` and `kind` matching D1
    direction, formed **today**.
  - If found: fire alert. Disarm LTF (`_ltf_armed[symbol] = False`) so the
    same zone doesn't re-alert. Use zone UID deduplication to prevent
    re-alerting on subsequent candles.

### D1 SL breach guard
- On every INDEX_TICK for a watched stock: if spot price crosses the D1 SL
  (`price < d1_sl` for CE, `price > d1_sl` for PE) → remove stock from
  `_watched` and disarm. Log silently.

---

## Alert Payload (`Topic.FNO_STOCK_ALERT`)

```python
@dataclass
class FnoStockAlert:
    symbol: str           # "EICHERMOT"
    direction: str        # "CE" | "PE"
    spot_price: float     # current spot at alert time
    d1_zone_low: float
    d1_zone_high: float
    d1_zone_date: str     # "Jun 30"
    strike: int           # suggested strike from Stage 1 scan
    lot_size: int
    sl: float             # D1 zone SL (spot price)
    t1: float             # D1 T1 (spot price)
    risk_pts: float
    reward_pts: float
    rr_ratio: float
    mtf_trap_price: float  # spot price where 15m zone trapped
    ltf_trap_price: float  # spot price where 5m zone trapped
    fired_at: datetime
```

---

## Dashboard Alert Card

Displayed on the Stocks tab. New alert card appears above the CE/PE scan
sections. Sound plays once on arrival (`/static/alert.mp3` — a short beep,
served by FastAPI static files).

```
⚡ EICHERMOT  ▼ PE SIGNAL            R:R 4.0×
─────────────────────────────────────────────
D1 Zone    : ₹7165 – 7178  (Jun 30)
Spot Now   : ₹7160
Strike     : 7200 PE  (lot 175)
SL (spot)  : ₹7192   risk  ₹48/share
Target T1  : ₹6950   reward ₹194/share
MTF 15m    : Trapped at ₹7168 ✓
LTF 5m     : Trapped at ₹7155 ✓
                              [Notified ✓]
```

- Alert cards are shown **newest-first**.
- Clicking "Notified ✓" marks the alert acknowledged (removes card, keeps in
  `_notified_uids` set so it cannot re-fire today).
- Unacknowledged alerts persist across dashboard page reloads
  (`GET /api/scanner/alerts` returns current active list).

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/scanner/alerts` | Returns list of active (un-notified) `FnoStockAlert` dicts |
| `POST` | `/api/scanner/alerts/{uid}/notified` | Mark alert as notified; removes from active list |

---

## Files Created / Modified

| File | Action |
|---|---|
| `strategies/fno_stock_monitor.py` | **New** — `FnoStockMonitor` class |
| `config/global_config.py` | Add `Topic.FNO_STOCK_ALERT` |
| `ui_layer/ws_bridge.py` | Subscribe `FNO_STOCK_ALERT`; forward to WebSocket as `{"type":"fno_alert", ...}` |
| `ui_layer/dashboard_server.py` | Instantiate + start monitor; add 2 alert endpoints; serve `alert.mp3` static file |
| `ui_layer/static/alert.mp3` | **New** — short 440 Hz beep, generated once via Python `wave` stdlib at startup if missing |
| `ui_layer/templates/monitor.html` | Alert cards section on Stocks tab; WebSocket handler for `fno_alert`; play sound |

---

## FnoStockMonitor — Internal State

```python
_scan_entries: List[dict]           # loaded from today's scan file
_watched: Dict[str, dict]           # symbol → scan entry (CE or PE)
_buckets_5m: Dict[str, dict]        # symbol → current open 5m candle
_buckets_15m: Dict[str, dict]       # symbol → current open 15m candle
_bars_5m: Dict[str, List[dict]]     # symbol → closed 5m bars today
_bars_15m: Dict[str, List[dict]]    # symbol → closed 15m bars today
_ltf_armed: Dict[str, bool]         # symbol → True after MTF confirms
_active_alerts: List[FnoStockAlert] # un-notified alerts (served via API)
_notified_uids: Set[str]            # zone UIDs that already fired today
_feeder: Optional[BaseFeeder]       # to subscribe spot instruments
```

---

## Subscription

`FnoStockMonitor.set_feeder(feeder)` is called by `run_system.py` after feeder
starts. The monitor then calls `feeder.pin_instruments(instrument_keys)` for
each watched stock's Upstox spot key (read from the scan file entry —
`fno_stocks.csv` already has the key column).

Spot ticks arrive on `Topic.INDEX_TICK`. The monitor filters by
`tick.symbol in _watched`.

---

## Lifecycle

1. `run_system.py` creates `FnoStockMonitor(bus, cfg)` and calls `warm_start()`.
2. `warm_start()`: loads today's scan file. If missing or empty → logs and
   does nothing (no-op, no error).
3. `set_feeder(feeder)` → pins stock spot instrument keys.
4. `start()` → subscribes `Topic.INDEX_TICK`, launches async tasks.
5. At 3:15 PM IST → stop monitoring, log EOD summary.
6. At 3:30 PM IST → clear `_active_alerts` (any residual unnotified alerts gone).

---

## What This Does NOT Include

- Auto-trade execution — deferred to Phase 3
- Option premium monitoring — spot price drives all signals
- Intraday D1 zone re-scan — Stage 1 scan is nightly only
- Persistence of alerts across bot restarts — in-memory only
- Multiple simultaneous alerts per stock — first MTF+LTF signal per stock per day wins
