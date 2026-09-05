# EC2 Automation — Handoff / Deployment Runbook

Everything code-side (Tasks 1-6, 8-11 of the implementation plan) is built,
tested, and committed on branch `feat/automated-daily-lifecycle`. This is
what's left, and it's all on your side of the fence — server access, real
credentials, real AWS. I intentionally did not touch any of this from here
(no SSH access, and broker passwords/TOTP secrets should never be typed into
a chat).

> **⚠️ 2026-09-06 update, confirmed via real live testing on the server:**
> Fyers headless login is **confirmed non-viable** — Cloudflare Turnstile is
> loaded on Fyers' login page and withholds its token from the automated
> browser session (verified directly: the field fills correctly, the tab
> switches correctly, the submit button still never enables). This is not a
> bug and not fixable by adjusting the Playwright script — Turnstile
> validates the browsing session/behavior, not the form. Per the original
> design spec's own pre-approved fallback, **Fyers stays manual-login only**
> going forward. Upstox and Zerodha headless login are NOT affected by this
> and remain worth pursuing — see `broker_auth/headless_totp_auth_fyers.py`'s
> own module docstring for the full evidence trail if this ever needs
> re-litigating.

## 1. Merge and pull

    git checkout nifty-cascade-v4-indicators   # or master, your call
    git merge feat/automated-daily-lifecycle
    git push
    # then on the server:
    git pull

## 2. Install the new dependency

    pip install -r requirements.txt
    playwright install chromium      # one-time, needed for the Fyers step

## 3. Seed real credentials (interactive, one-time, never logged)

    python scripts/seed_headless_creds.py

You'll be prompted for Upstox/Fyers API creds + password + TOTP base32
secret, and the Zerodha (SA5770) binding's password + TOTP secret, and
optionally a Gmail address + app-specific password for the alert emails.
Nothing typed is echoed or logged. Run this on the server, against the
real `data/clients.db` — not locally then copied over, unless you're
deliberately doing it that way.

## 4. pm2 launch command — DONE, confirmed 2026-09-06

`_start_pm2()` now prefers `pm2 restart terminus` — pm2's own remembered
definition from the last `pm2 save` (already run on the real box with the
real production args: `--mode live --ui --port 5000 --index NIFTY,SENSEX
--strategies sell_straddle,oi_orb_screener,cag_straddle
--futures-atm-underlyings NIFTY,SENSEX`). `_PM2_START_CMD` in
`scripts/auto_morning_start.py` now matches this exactly too, but only as a
fallback for a from-scratch box where no `pm2 save` exists yet — normal
operation never touches it. If production's launch flags ever change
again, just `pm2 save` after the manual restart and the automation stays
correct automatically, no code edit needed.

## 5. Dry-run validation (repeatable, safe, no side effects)

    python scripts/auto_morning_start.py --dry-run
    python scripts/auto_evening_stop.py --dry-run

Confirms the summary email arrives and Upstox/Zerodha both show `OK`,
with zero pm2/DB writes. Run this as many times as you want before
trusting a real run.

## 6. One real manual run, watched

    python scripts/auto_morning_start.py

Confirm: pm2 process up, dashboard reachable, Upstox feeder shows a fresh
token in the admin panel, Zerodha SA5770 binding shows
terminal_connected/trade_enabled true, any `is_running=1` deployment
starts evaluating within its normal reconcile window.

## 7. Install systemd units

See `ops/systemd/README.md` — **edit `WorkingDirectory`/`User` in both
`.service` files first**, they're placeholders (`/home/ubuntu/...` / `ubuntu`).

    sudo cp ops/systemd/auto-morning-start.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable auto-morning-start.service
    sudo systemctl enable --now auto-evening-stop.timer

## 8. Deploy the AWS EC2 start/stop schedule

See `ops/aws/README.md`. Needs the AWS CLI configured locally (wherever
you run this from — your own machine is fine, doesn't need to be the
EC2 box itself).

    aws cloudformation deploy \
      --template-file ops/aws/ec2_scheduler.yaml \
      --stack-name trading-ec2-scheduler \
      --parameter-overrides InstanceId=i-XXXXXXXXXXXXXXXXX \
      --capabilities CAPABILITY_IAM

Manually invoke the Lambda with `{"action":"start"}`/`{"action":"stop"}`
first and confirm the instance actually transitions state before trusting
the schedule.

## 9. Go-live day (watched, not the first unattended day)

Leave everything enabled, don't touch anything manually that morning.
Confirm: instance starts ~09:00 IST, summary email arrives ~09:16 IST,
dashboard shows fresh tokens + connected binding + active strategies,
evening summary email arrives ~15:58, pm2 shows stopped, instance
transitions to `stopped` shortly after 16:00. Only after this one clean
day should you stop watching — from here on, a failure surfaces via
email, not silence.

---

## About "test tomorrow" (Sat/Sun, market closed)

Since the market's closed this weekend, you can still exercise most of
this end-to-end **except the actual trading-relevant outcome**:

- Steps 1-6 above (seed creds, dry-run, one real run) work fine on a
  non-trading day — Upstox/Zerodha login endpoints don't care whether
  the market is open, so you can confirm the headless logins themselves
  succeed and the dashboard shows connected/trade-enabled.
- Step 7 (systemd) and step 8 (AWS start/stop) can also be installed and
  manually triggered (`sudo systemctl start ...` / Lambda manual invoke)
  regardless of market hours — that only proves the plumbing fires, not
  that a strategy actually traded.
- What you WON'T be able to confirm this weekend: whether a deployment
  left `is_running=1` actually starts evaluating real signals, since
  there's no real market data flowing on a non-trading day. That part
  only proves out on the next real trading morning.

So: yes, worth testing tomorrow — just go in expecting to validate
"did the login/connect/enable sequence fire correctly," not "did a
strategy actually trade," since the second one needs a real session.
