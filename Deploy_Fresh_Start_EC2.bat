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
echo 2026-09-29, direct user decision: oi_orb_screener REMOVED from this list,
echo REPLACED by oi_bias_rsi_exit (OI-spurt selection + combined-OI bias +
echo StochRSI entry(3m)/exit(75m) -- validated against 20 real trading days,
echo see scripts/oi_bias_rsi_exit_backtest.py / _optimize.py). First live
echo paper-mode day -- watch logs closely.
echo.
echo 2026-10-05, direct user decision: oi_bias_rsi_exit, cag_straddle, and
echo iron_fly REMOVED from --strategies -- ONLY sell_straddle stays, so the
echo ONLY book that can spawn is whatever DB deployment rows have
echo is_running=1 under strategy_name IN ('sell_straddle',
echo 'sell_straddle_calc_vwap') -- currently just
echo ssrajpal2001/SA5770/NIFTY/sell_straddle_calc_vwap (vp_oi_enabled=true).
echo NOTE: 'sell_straddle_calc_vwap' is NOT a separate --strategies name --
echo it is a strategy_name variant handled entirely inside the sell_straddle
echo module's own StraddleBookManager (forces vwap_source=calculative), so
echo --strategies must still say "sell_straddle", never
echo "sell_straddle_calc_vwap" (an unrecognized name here would be silently
echo DROPPED with no error, per the warning above).
echo.
echo 2026-10-06: --futures-atm-underlyings NIFTY REMOVED. It forced self._spot
echo to be sourced from the near-month FUTURES contract for every NIFTY
echo binding in this process (incl. Gurmeet's live book) -- a side effect only
echo the old BEGINNING-anchor-side logic needed. That logic now resolves the
echo anchor side via a one-time REST monthly-contract theta fetch instead, so
echo the flag is no longer required by anything.
echo.
echo 2026-10-07: --futures-oi-underlyings NIFTY ADDED (separate from, and NOT
echo the same as, the removed --futures-atm-underlyings above). Subscribes
echo the near-month futures tick stream (price+OI) PURELY so vp_oi_regime's
echo Future-OI buildup/unwinding classification has real data -- does NOT
echo blend futures into self._spot/_atm_ref for anyone (that side effect
echo stays scoped to --futures-atm-underlyings, which is NOT re-added).
echo.
ssh -t -i "%USERPROFILE%\.ssh\algotrading_ec2" ec2-user@13.200.171.160 "cd ~/OptionChainBasedStrategy; git pull origin nifty-cascade-v4-indicators; pm2 delete terminus 2>/dev/null; pm2 start run_system.py --name terminus --interpreter python3 -- --mode live --ui --port 5000 --index NIFTY --strategies sell_straddle --futures-oi-underlyings NIFTY; pm2 save --force; sleep 8; echo; echo '=== VERIFY: actual running process args ==='; ps aux | grep run_system.py | grep -v grep; echo; echo '=== VERIFY: strategies the app itself parsed ==='; grep 'enabled strategies' ~/.pm2/logs/terminus-out.log | tail -1; echo; echo '=== VERIFY: any auth/startup failures? ==='; grep -iE 'auth failed|startup aborted' ~/.pm2/logs/terminus-out.log | tail -5; echo; echo '=== Recent log tail ==='; pm2 logs terminus --lines 40 --nostream; bash -l"
echo.
echo Connection closed. Press any key to close this window.
pause >nul
