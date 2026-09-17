@echo off
title AlgoTrading EC2 - SSH
echo Connecting to AlgoTrading EC2 (13.200.171.160)...
echo.
echo NOTE: this does a FULL delete + fresh start of terminus (not "pm2 restart"),
echo because pm2 can silently keep reusing a STALE cached strategy list on a
echo plain restart even when you pass different args to a new "pm2 start" -- this
echo is exactly what dropped oi_orb_screener_top20 on 2026-09-09. Delete+fresh-start
echo is immune to that since it never depends on trusting pm2's cached state.
echo.
echo 2026-09-14: iron_fly added to --strategies (NIFTY Weekly Iron Condor -^>
echo Iron Fly, first paper_route deployment).
echo.
echo 2026-09-17: oi_orb_screener_top20 REMOVED from the codebase entirely --
echo it deviated from the validated backtest mechanic (no pChange direction
echo gate, no futures-OI breach confirmation -- confirmed live via a real
echo mistrade, POLICYBZR firing CALL on a -4.24%% pChange day). Strategy list
echo below swapped to plain "oi_orb_screener" (the validated variant). Also
echo note: run_system.py silently DROPS any --strategies name it doesn't
echo recognize (no error, no warning) -- if this list is ever out of sync
echo with strategies/registry.py again, a strategy can silently stop running
echo with zero indication in the log short of "grep enabled strategies" below
echo not showing the name you expected.
echo.
ssh -t -i "%USERPROFILE%\.ssh\algotrading_ec2" ec2-user@13.200.171.160 "cd ~/OptionChainBasedStrategy; git pull origin nifty-cascade-v4-indicators; pm2 delete terminus 2>/dev/null; pm2 start run_system.py --name terminus --interpreter python3 -- --mode live --ui --port 5000 --index NIFTY --strategies sell_straddle,oi_orb_screener,cag_straddle,iron_fly --futures-atm-underlyings NIFTY; pm2 save --force; sleep 8; echo; echo '=== VERIFY: actual running process args ==='; ps aux | grep run_system.py | grep -v grep; echo; echo '=== VERIFY: strategies the app itself parsed ==='; grep 'enabled strategies' ~/.pm2/logs/terminus-out.log | tail -1; echo; echo '=== VERIFY: any auth/startup failures? ==='; grep -iE 'auth failed|startup aborted' ~/.pm2/logs/terminus-out.log | tail -5; echo; echo '=== Recent log tail ==='; pm2 logs terminus --lines 40 --nostream; bash -l"
echo.
echo Connection closed. Press any key to close this window.
pause >nul
