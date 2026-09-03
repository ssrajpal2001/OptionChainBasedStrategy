# ops/systemd/README.md

## Install (run once on EC2)

`pip install -r requirements.txt` installs the `playwright` Python package
but does NOT install the actual Chromium browser binary Playwright drives --
that's a required separate step, or the Fyers best-effort headless login
will fail on every single run with a non-obvious error:

    playwright install chromium

(Amazon Linux may also need system-level dependencies for headless Chromium
to launch -- if `playwright install chromium` alone isn't enough, see
Playwright's own `install-deps` command/docs for the OS package list.)

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

## Uninstall / pause automation

    sudo systemctl disable auto-morning-start.service
    sudo systemctl disable --now auto-evening-stop.timer
