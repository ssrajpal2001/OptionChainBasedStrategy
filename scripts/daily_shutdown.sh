#!/bin/bash
# Daily 15:50 IST auto-shutdown -- disables all broker trading + strategies,
# stops pm2. Manual restart required by design (run the Connect .bat tomorrow).
cd ~/OptionChainBasedStrategy || exit 1
LOG=logs/cron_shutdown.log
echo "$(date): daily auto-shutdown starting" >> "$LOG"

sqlite3 data/clients.db "UPDATE broker_bindings SET is_trade_enabled=0;" >> "$LOG" 2>&1
sqlite3 data/clients.db "UPDATE strategy_deployments SET is_running=0;" >> "$LOG" 2>&1
echo "$(date): brokers + strategies disabled in DB" >> "$LOG"

pm2 stop terminus >> "$LOG" 2>&1
echo "$(date): pm2 stopped -- shutdown complete" >> "$LOG"
