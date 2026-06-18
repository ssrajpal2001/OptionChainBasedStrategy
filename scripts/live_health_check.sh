#!/bin/bash
# Live health check — run anytime during market hours
# Usage: bash scripts/live_health_check.sh
# Press Ctrl+C to stop

DATE=$(date +%Y%m%d)
LOG_DIR="logs"
CLIENT_LOG="$LOG_DIR/clients"
SYS_LOG="$LOG_DIR/system-${DATE}.log"

echo "================================================================"
echo "  ALGO HEALTH CHECK — $(date '+%Y-%m-%d %H:%M:%S IST')"
echo "================================================================"

# ── 1. PM2 PROCESS ───────────────────────────────────────────────────
echo ""
echo "[ 1 ] PM2 PROCESS"
pm2 list | grep algo

# ── 2. DATA FEED ─────────────────────────────────────────────────────
echo ""
echo "[ 2 ] DATA FEED (last Upstox + Fyers tick)"
grep -i "upstox.*INDEX\|fyers.*INDEX" "$SYS_LOG" 2>/dev/null | tail -4

# ── 3. BROKER AUTH ───────────────────────────────────────────────────
echo ""
echo "[ 3 ] BROKER AUTH"
grep -i "authenticated\|auth failed\|no token\|access denied" "$SYS_LOG" 2>/dev/null | tail -10

# ── 4. SELL STRADDLE — NIFTY ─────────────────────────────────────────
echo ""
echo "[ 4 ] SELL STRADDLE — NIFTY (last 5 lines)"
SS_LOG=$(ls "$CLIENT_LOG"/ss_NIFTY_ssrajpal2001_SA5770_${DATE}.log 2>/dev/null | head -1)
if [ -f "$SS_LOG" ]; then
    tail -5 "$SS_LOG"
else
    echo "  ⚠ Log not found: $CLIENT_LOG/ss_NIFTY_ssrajpal2001_SA5770_${DATE}.log"
fi

# ── 5. TRAP SCANNER — SENSEX ─────────────────────────────────────────
echo ""
echo "[ 5 ] TRAP SCANNER — SENSEX (last 5 lines)"
TS_SENSEX=$(ls "$CLIENT_LOG"/ts_SENSEX_ssrajpal2001_Angelone_sarabjeet_${DATE}.log 2>/dev/null | head -1)
if [ -f "$TS_SENSEX" ]; then
    tail -5 "$TS_SENSEX"
else
    echo "  ⚠ Log not found"
fi

# ── 6. TRAP SCANNER — CRUDEOIL ───────────────────────────────────────
echo ""
echo "[ 6 ] TRAP SCANNER — CRUDEOIL (last 5 lines)"
TS_CRUDE=$(ls "$CLIENT_LOG"/ts_CRUDEOIL_ssrajpal2001_Angelone_sarabjeet_${DATE}.log 2>/dev/null | head -1)
if [ -f "$TS_CRUDE" ]; then
    tail -5 "$TS_CRUDE"
else
    echo "  ⚠ Log not found"
fi

# ── 7. ANY ERRORS TODAY ───────────────────────────────────────────────
echo ""
echo "[ 7 ] ERRORS (system log today)"
grep -i "ERROR\|CRITICAL\|exception" "$SYS_LOG" 2>/dev/null | grep -v "rate\|phantom\|stale" | tail -10

# ── 8. TRADES PLACED TODAY ───────────────────────────────────────────
echo ""
echo "[ 8 ] TRADES PLACED TODAY"
grep -i "ORDER PLACED\|FILL\|ENTRY\|EXIT\|BUY\|SELL" "$CLIENT_LOG"/*${DATE}*.log 2>/dev/null | grep -v "BLOCK\|skip\|cand" | tail -15

# ── 9. OPEN POSITIONS ────────────────────────────────────────────────
echo ""
echo "[ 9 ] OPEN POSITIONS"
grep -i "position.*open\|entry confirmed\|IN TRADE\|remaining_qty" "$CLIENT_LOG"/*${DATE}*.log 2>/dev/null | tail -10

# ── 10. HEARTBEAT (ticks/min) ─────────────────────────────────────────
echo ""
echo "[ 10 ] HEARTBEAT (latest ticks/min for each engine)"
grep -i "ticks/min\|IDX_TICKS\|OPT_TICKS" "$CLIENT_LOG"/*${DATE}*.log 2>/dev/null | tail -8

echo ""
echo "================================================================"
echo "  Done. Run again anytime: bash scripts/live_health_check.sh"
echo "================================================================"
