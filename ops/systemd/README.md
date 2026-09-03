# ops/systemd/README.md

## Install (run once on EC2)

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
