# Client & Operator User Guide

## What This System Does

This system monitors NSE/BSE option chains in real time, identifies high-probability
entry setups using three complementary strategies, and places orders on behalf of
registered clients through their configured broker accounts — all automatically,
concurrently, without manual intervention during market hours.

---

## Prerequisites

### Python & Dependencies

```
Python 3.11+

pip install numpy pyarrow zstandard pyotp

# For each broker you want to use (install only what you need):
pip install NorenRestApiPy       # Shoonya / Finvasia
pip install fyers-apiv3          # Fyers
pip install smartapi-python      # Angel One
pip install dhanhq               # Dhan HQ
pip install upstox-python-sdk    # Upstox
```

### Directory Bootstrap

Run once before the first session:

```
python -c "from config.global_config import GLOBAL_CFG"
```

This creates `data/`, `data/recorded/`, `data/backtest/`, and `logs/` automatically.

---

## Quick Start — Demo Mode (No Broker Account Needed)

Run the full system with synthetic data and a mock broker:

```
python main.py --mode demo --index NIFTY --capital 500000
```

The admin console will appear. Type `status` to see the demo client, `help` for all commands.
Press Ctrl+C or type `quit` to stop.

---

## Quick Start — Backtest

Replay stored tick data (or synthetic fallback) through the full strategy pipeline:

```
python main.py --mode backtest --index NIFTY --start 2024-01-15 --end 2024-02-14
```

Results are printed to the terminal and saved to `data/backtest/backtest_NIFTY_*.json`.

---

## Registering a Client

### Step 1 — Create a Profile File

Create or edit `config/client_profiles.json`:

```json
[
  {
    "client_id": "C001",
    "name": "Rajesh Kumar",
    "email": "rajesh@example.com",
    "phone": "9876543210",
    "risk": {
      "capital": 500000,
      "max_risk_per_trade_pct": 1.0,
      "max_daily_loss_pct": 3.0,
      "max_open_positions": 1,
      "max_daily_trades": 5,
      "min_risk_reward": 2.0,
      "margin_utilization_limit": 0.8,
      "size_multiplier": 1.0
    },
    "broker_bindings": [
      {
        "binding_id": "C001_shoonya",
        "provider": "shoonya",
        "label": "Shoonya Main Account",
        "lot_multiplier": 1.0,
        "enabled": true
      }
    ],
    "enabled_strategies": ["A", "B", "C"],
    "expiry_preference": "CURRENT_WEEK",
    "moneyness_execution": "ATM",
    "active": true,
    "notes": ""
  }
]
```

**Important**: Credentials (passwords, API keys, TOTP secrets) are NEVER stored in
this file. They are injected from environment variables at startup.

### Step 2 — Set Environment Variables

Set one environment variable per credential field per binding. Use the prefix
`{CLIENT_ID}_{PROVIDER}_`:

#### Shoonya Example
```
set C001_SHOONYA_USER_ID=SH12345
set C001_SHOONYA_PASSWORD=mypassword
set C001_SHOONYA_API_SECRET=abc123def456
set C001_SHOONYA_TOTP_SECRET=JBSWY3DPEHPK3PXP
set C001_SHOONYA_VENDOR_CODE=SH001
set C001_SHOONYA_IMEI=12345678-abcd-1234-efgh-1234567890ab
```

#### Fyers Example
```
set C001_FYERS_API_KEY=APPID12345-100
set C001_FYERS_ACCESS_TOKEN=eyJhbGciOi...
```

#### Angel One Example
```
set C001_ANGEL_API_KEY=XxYyZz123
set C001_ANGEL_CLIENT_CODE=A123456
set C001_ANGEL_PASSWORD=pin1234
set C001_ANGEL_TOTP_SECRET=BASE32SECRET
```

#### Dhan HQ Example
```
set C001_DHAN_CLIENT_CODE=1234567890
set C001_DHAN_ACCESS_TOKEN=eyJhbGciOi...
```

#### Upstox Example
```
set C001_UPSTOX_API_KEY=your_api_key
set C001_UPSTOX_API_SECRET=your_api_secret
set C001_UPSTOX_ACCESS_TOKEN=eyJhbGciOi...
```

### Step 3 — Wire Credentials in main.py

Edit `_setup_live_clients()` in `main.py` to call `inject_credentials()` after loading:

```python
def _setup_live_clients(registry: ClientRegistry) -> None:
    registry.load_non_sensitive()
    registry.inject_credentials("C001", "C001_shoonya",
        user_id=os.getenv("C001_SHOONYA_USER_ID", ""),
        password=os.getenv("C001_SHOONYA_PASSWORD", ""),
        api_secret=os.getenv("C001_SHOONYA_API_SECRET", ""),
        totp_secret=os.getenv("C001_SHOONYA_TOTP_SECRET", ""),
        vendor_code=os.getenv("C001_SHOONYA_VENDOR_CODE", ""),
        imei=os.getenv("C001_SHOONYA_IMEI", ""),
    )
```

### Step 4 — Start Live / Paper Mode

```
python main.py --mode live  --index NIFTY    # Real orders
python main.py --mode paper --index NIFTY    # Mock orders, real data
```

---

## Risk Settings Explained

| Setting                    | Default  | Description                                        |
|----------------------------|----------|----------------------------------------------------|
| `capital`                  | 500000   | INR capital allocated to this client               |
| `max_risk_per_trade_pct`   | 1.0      | Max capital risked per trade (% of capital)        |
| `max_daily_loss_pct`       | 3.0      | Auto-halt threshold (% of capital)                 |
| `max_open_positions`       | 1        | Max simultaneous open option positions             |
| `max_daily_trades`         | 5        | Max total trades per day                           |
| `min_risk_reward`          | 2.0      | Minimum R:R ratio for any signal to be traded      |
| `margin_utilization_limit` | 0.80     | Never use more than 80% of available margin        |
| `size_multiplier`          | 1.0      | Scale lot size (e.g. 2.0 = double standard lots)  |

**Auto-halt**: When `_daily_pnl` drops below `-capital * max_daily_loss_pct / 100`, the
client is automatically halted. No new orders will be placed until an operator runs
`resume C001` in the admin console.

---

## Admin Console Commands

Launch any mode and the console starts automatically. Type commands at the `admin> ` prompt.

### Client Management

| Command                         | Description                                          |
|---------------------------------|------------------------------------------------------|
| `help`                          | Show all commands                                    |
| `status`                        | Show all clients: P&L, tradeable status, brokers    |
| `halt C001`                     | Immediately stop new orders for client C001          |
| `resume C001`                   | Re-enable a halted client                            |
| `halt_all`                      | Stop all clients (emergency kill switch)             |
| `reset_daily`                   | Reset daily P&L counters (done auto at 09:15)        |
| `funds C001`                    | Fetch live margin balance from all C001 brokers      |
| `positions C001`                | Fetch live positions from all C001 brokers           |
| `set_lots C001 C001_shoonya 2`  | Change lot multiplier for a specific binding         |
| `add_client <json>`             | Register a new client profile at runtime             |

### Diagnostics

| Command          | Description                                                       |
|------------------|-------------------------------------------------------------------|
| `drop_counts`    | Show how many EventBus messages were dropped per topic            |
| `worker_stats`   | Show per-client execution worker queue depth and throughput       |

### State Monitoring (Mid-Day Reboot Recovery)

| Command          | Description                                                         |
|------------------|---------------------------------------------------------------------|
| `state_status`   | Show SQLite snapshot DB path, file size, and flush count            |
| `state_restore`  | Reload indicator + Strategy B state from the most recent snapshot   |

The system automatically snapshots RSI, VWAP, ADX, EMA, ATR values and Strategy B
rolling base / void phase to SQLite on every candle close. If the system is restarted
mid-day (e.g. after a crash), run `state_restore` to recover state without replaying
the day's ticks from scratch.

### Dynamic Strike Rebalancing

| Command               | Description                                                    |
|-----------------------|----------------------------------------------------------------|
| `rebalance NIFTY`     | Force immediate ATM rebalance for NIFTY (or any index)        |

The system automatically rebalances the option chain subscription when the underlying
spot price drifts 3+ strike intervals from the market-open ATM. Use `rebalance <index>`
to force an immediate rebalance if you suspect the subscription window is stale.

### Quit

| Command  | Description                  |
|----------|------------------------------|
| `quit`   | Graceful shutdown            |

---

## Mid-Day Reboot Procedure

If the system crashes or is manually restarted during market hours:

1. Start normally: `python main.py --mode live --index NIFTY`
2. Wait for the admin console prompt.
3. Run `state_restore` to reload indicator state and open positions from SQLite.
4. Run `status` to verify all clients are in the expected state.
5. Run `rebalance NIFTY` (and any other active index) to force a fresh ATM subscription.

The system will recover Strategy B rolling_base and void phase state from the last
snapshot — typically within 1 candle close of the crash time.

---

## Expiry Preferences

| Value           | Description                              |
|-----------------|------------------------------------------|
| `CURRENT_WEEK`  | Nearest Thursday expiry (default)        |
| `NEXT_WEEK`     | Following Thursday expiry                |
| `MONTHLY`       | Last Thursday of the current month       |

Set in the profile JSON: `"expiry_preference": "CURRENT_WEEK"`

---

## Moneyness Execution

| Value   | Description                              |
|---------|------------------------------------------|
| `ATM`   | Strike nearest to current spot (default) |
| `ITM_1` | One strike in-the-money                  |
| `OTM_1` | One strike out-of-the-money              |

Set in the profile JSON: `"moneyness_execution": "ATM"`

---

## Adding Multiple Clients (N clients x M brokers)

Each client can have multiple broker bindings. Example — one client trading on
both Shoonya and Fyers simultaneously:

```json
{
  "client_id": "C002",
  "broker_bindings": [
    {
      "binding_id": "C002_shoonya",
      "provider": "shoonya",
      "lot_multiplier": 0.5,
      "enabled": true
    },
    {
      "binding_id": "C002_fyers",
      "provider": "fyers",
      "lot_multiplier": 0.5,
      "enabled": true
    }
  ]
}
```

When a signal fires, each client's worker processes it independently. The system
uses a **per-client isolated queue** architecture — Client A's broker network latency
never delays Client B's order placement.

---

## Tick Recording

When running live or paper mode, all market ticks are automatically recorded to:

```
data/recorded/
  NIFTY/
    spot/
      2024-01-15.parquet
      2024-01-16.parquet
    options/
      2024-01-15.parquet
```

Files use Apache Parquet format with ZStandard compression (very compact).
Use these recordings for backtesting on real historical data.

**Read recorded data in Python:**

```python
import pyarrow.parquet as pq
table = pq.read_table("data/recorded/NIFTY/spot/2024-01-15.parquet")
df = table.to_pandas()
print(df.head())
```

---

## Running Backtest on Recorded Data

Once you have accumulated recorded Parquet files, backtest against them:

```
python main.py --mode backtest --index NIFTY --start 2024-01-15 --end 2024-02-14 --capital 500000
```

The backtester will prefer real recorded data over synthetic fallback.
Results include: win rate, profit factor, max drawdown, per-exit breakdown.

---

## Strategy Overview

### Strategy A — OI Zone Breakout / Rejection
Detects when price approaches a high-OI strike (max call OI = resistance,
max put OI = support) with directional momentum. Enters on breakout confirmation
with a strong-body candle and RSI alignment.

### Strategy B — Liquidity Trap (Rolling Base + Void/Lift)
Identifies institutional liquidity traps — high OI spikes that absorb buying/selling,
followed by a sharp reversal. The Rolling Base tracks the weakest recent low dynamically.

**Void Lift**: If price runs more than 2x ATR beyond the trap level, the setup enters
a VOID state. The void is lifted **only** when `candle.low ≤ htf_entry_level + 0.10%` —
the price must physically retest the original structural level. Until that retest
condition is met, the VOID state is considered invalid for trading.

### Strategy C — Panic Selling / Put Unwind
Catches exhaustion after 3+ consecutive bearish candles with volume spike, sharp PCR drop,
and heavy put OI unwinding. Enters long on the reversal signal when smart money exits
their puts (unwinding = bearish positions being covered = bullish flow).

### Confluence Gate
A signal is only dispatched if:
- Risk:Reward ratio >= 2.0 (configurable in `StrategyParams.min_risk_reward`)
- Confidence score >= 0.50 (from the emitting strategy)
- No directional conflict (LONG and SHORT cannot fire simultaneously)

---

## Logs

All logs are written to `logs/algo_YYYYMMDD_HHMMSS.log` and echoed to stdout.
Log level is configurable:

```
python main.py --mode paper --log-level DEBUG
```

Key log lines to monitor:
- `SIGNAL DISPATCHED` — a signal passed all confluence gates
- `Worker[C001]: FILLED` — order confirmed filled by broker
- `ClientManager: HALTED` — a client hit its daily loss limit
- `StrikeRebalancer: rebalancing` — ATM subscription window updated
- `StatePersistence: snapshot loop started` — state snapshots active
- `EventBus: topic '...' dropped N events` — consumer is too slow

---

## Common Issues

### "No client profiles found"
Create `config/client_profiles.json` or register a client via the admin console:
```
admin> add_client {"client_id":"C001","capital":500000,"broker":"mock","strategies":["A","B"]}
```

### "access_token not set for Fyers"
Fyers requires a pre-generated access token from their OAuth2 flow.
Generate it using the Fyers web login and set it as `C001_FYERS_ACCESS_TOKEN` before starting.

### "smartapi-python not installed"
```
pip install smartapi-python pyotp
```

### "upstox-python-sdk not installed"
```
pip install upstox-python-sdk
```

### Zero signals in backtest
- Increase date range (minimum 2-3 weeks recommended)
- Check log for indicator warm-up — strategies need at least 22 candles to start
- Try `--log-level DEBUG` to see per-strategy evaluation details

### Orders not placed in live mode
Run `funds C001` in the admin console to verify broker authentication succeeded.
Check `logs/algo_*.log` for authentication errors.

### State not recovered after restart
Run `state_status` to verify the SQLite DB exists and has recent flush activity.
If the DB is missing or empty (first day), state recovery is not possible — the
system will warm up naturally after the first few candle closes.
