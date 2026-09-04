# systemd units — automated daily morning-start / evening-stop

Part of the automated daily lifecycle (see
`docs/superpowers/specs/2026-09-03-fully-automated-daily-lifecycle-design.md`
and `docs/superpowers/plans/2026-09-03-automated-daily-lifecycle.md`).

## Before installing

Edit `WorkingDirectory` and `User` in both `.service` files to match the real
deployment path and OS user on your server — the checked-in files use
`/home/ubuntu/OptionChainBasedStrategy` / `ubuntu` as placeholders, not a
confirmed value.

Also confirm `_PM2_START_CMD` in `scripts/auto_morning_start.py` matches your
real `pm2 describe terminus` output before the first install (Task 6, Step 5
of the implementation plan) — the checked-in default mirrors this repo's own
`CLAUDE.md` "Launch Commands" section but hasn't been diffed against the
actual running process args.

## Install (run once on the server)

    sudo cp ops/systemd/auto-morning-start.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable auto-morning-start.service
    sudo systemctl enable --now auto-evening-stop.timer

## Verify

    systemctl status auto-morning-start.service
    systemctl list-timers auto-evening-stop.timer
    tail -f /var/log/auto_morning_start.log
    tail -f /var/log/auto_evening_stop.log

## Manual test run (before trusting it to boot/timer triggers)

    cd /path/to/OptionChainBasedStrategy
    python3 scripts/seed_headless_creds.py          # one-time, interactive — see its own docstring
    python3 scripts/auto_morning_start.py --dry-run # validates real logins, no pm2/DB side effects
    python3 scripts/auto_morning_start.py           # real run, only once you're ready to watch it
    python3 scripts/auto_evening_stop.py --dry-run

Or trigger the installed units directly without waiting for boot/15:58:

    sudo systemctl start auto-morning-start.service
    sudo systemctl start auto-evening-stop.service

## Uninstall / pause automation

    sudo systemctl disable auto-morning-start.service
    sudo systemctl disable --now auto-evening-stop.timer
