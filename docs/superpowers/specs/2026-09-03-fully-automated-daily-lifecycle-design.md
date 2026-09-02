# Fully Automated Daily EC2 + Trading Lifecycle — Design Spec

Date: 2026-09-03
Status: Approved for planning (brainstorming complete, not yet implemented)

## Problem

Every trading day today is 100% manual: the user starts the EC2 instance from
the AWS console, SSHes/opens the cloud terminal, runs `run_system.py` for all
strategies, opens the dashboard admin panel to authenticate each data feeder
(Upstox, Fyers) via OAuth, authenticates the `ssrajpal2001` / `SA5770` Zerodha
client broker binding, switches that broker's terminal + trade toggles on,
and confirms each strategy deployment is running. At end of day (~16:00 IST)
the reverse happens by hand, then the EC2 instance is stopped manually.

Goal: automate the entire sequence so the system comes up unattended at
09:00 IST and shuts down unattended at 16:00 IST, with best-effort execution
(a failed step doesn't block the rest) and an email summary so failures are
never silent.

## Non-goals

- No change to any strategy's own trading logic, exit mechanics, or EOD
  square-off behavior — those are trusted as-is (SellStraddle force-exits
  15:15, CAG 15:35, etc., all already well before 16:00).
- No new "verify flat before shutdown" safety check (considered, explicitly
  declined — shutdown just stops the app and trusts existing force-exits).
- No headless automation for brokers/feeders beyond Upstox, Zerodha
  (SA5770), and Fyers (best-effort) — AngelOne already has a working
  standalone script (`scripts/angel_refresh_token.py`) that this work does
  not need to touch, though it could later be folded into the same morning
  orchestrator as a fourth best-effort step if the user wants that later.
- No multi-region / multi-instance complexity — one EC2 instance, one
  schedule.

## Architecture — three independent pieces

### 1. EC2 power (AWS-side, outside this repo)

- One EventBridge Scheduler rule, cron `0 9 * * 1-5` in `Asia/Kolkata` (or
  UTC-converted, `0 3 * * 1-5`) → triggers a small Lambda that calls
  `ec2:StartInstances` for the known instance ID.
- A second rule, `0 16 * * 1-5` → Lambda calling `ec2:StopInstances`.
- Weekday-only (`1-5`); no market-holiday awareness in v1 — starting the
  instance on a holiday just means the morning script runs, finds no market
  activity, and the day is a no-op past feeder/broker login. Acceptable per
  user (not raised as a concern); can be revisited later if holiday noise
  becomes annoying.
- IAM role scoped to exactly `ec2:StartInstances`/`ec2:StopInstances` on
  that one instance ARN — least privilege.
- This piece has zero code in this repository. It is AWS console / IaC
  configuration only (CloudFormation/Terraform if the user wants it
  reproducible, or clicked together directly — decide at plan time).

### 2. Morning boot sequence (on EC2)

A `systemd` oneshot service, enabled but gated on network availability
(`After=network-online.target`, `Wants=network-online.target`), targeting
completion by ~09:14 IST (instance boot from a stopped state is typically
under a minute, so a short additional delay is built into the script itself
rather than relied upon at the systemd level — see below). Runs
`scripts/auto_morning_start.py`.

**Sequence (each step best-effort — logged + collected for the summary
email, failure of one step does not stop the next):**

1. Small startup delay (config, default 60s) to let networking settle.
2. Start `run_system.py` under pm2 with the user's existing production
   command (same args as run by hand today — confirm exact `--index`/
   `--strategies` flags at plan time from the current pm2 process list).
3. Poll the dashboard's health endpoint (`GET /` or a dedicated health
   route if one exists — check at plan time) until it responds, capped at
   a reasonable timeout (e.g. 90s), before touching auth.
4. Headless-authenticate Upstox (system feeder) → rebuilt curl_cffi flow
   (revived from git history pre-`8b03adf`, hardened) → on success, call
   the same `update_feeder_token` the OAuth callback route calls today.
5. Headless-authenticate Zerodha (`SA5770` binding) → rebuilt
   login→twofa→checksum-token-exchange flow (revived from the same
   pre-refactor commit) → on success: set `access_token` +
   `set_terminal_connected(True)` + `set_trade_enabled(True)` — the exact
   same two calls the dashboard's "connect" button makes.
6. Best-effort headless-authenticate Fyers via Playwright (new build, not
   a revival — the old vagator-API approach was already Cloudflare-blocked
   even before the refactor). Runs headless Chromium, fills the login form,
   solves TOTP, captures the resulting OAuth redirect/token. Explicitly
   allowed to fail; the summary email will say so plainly. If this proves
   completely unworkable during implementation/testing, falling back to
   "Fyers stays manual" is an acceptable outcome — flagged as the one step
   in this plan with real feasibility risk.
7. No explicit "start each strategy" call is needed: every strategy's book
   manager reconciles every 5s off `terminal_connected` + `trade_enabled` +
   that deployment's own persisted `is_running` flag. Once step 5 flips the
   Zerodha binding's terminal/trade flags true, any deployment already left
   `is_running=1` auto-spawns on its own. **Action item for the user before
   go-live: confirm which deployments should always be on, and leave them
   in that state — a deployment manually toggled off yesterday stays off
   today; this script does not turn strategies on that were left off.**
8. Compose and send one summary email (see Alerting below) — always sent,
   success or failure, so a fully-successful morning is confirmed and a
   partial one is visible immediately.

### 3. Afternoon shutdown (on EC2)

A `systemd` timer at 16:00 IST (or a cron entry — pick one consistently at
plan time) runs a short `scripts/auto_evening_stop.py`:
- `pm2 stop` the app process (not `delete` — keeps pm2's process
  definition so tomorrow's boot script can `pm2 restart`/`pm2 start`
  against the same definition without re-specifying flags).
- No square-off logic, no position check — trusts existing per-strategy
  force-exit times (all confirmed well before 16:00).
- Sends its own short confirmation email (or folds into the same
  alerting helper used by the morning script) so a shutdown that silently
  didn't run is also visible.

The EC2-stop Lambda (piece 1) fires independently ~a few minutes after
this, per its own schedule — no direct coordination needed since `pm2
stop` is fast and the natural gap between the two schedules is enough
buffer (confirm a safe gap, e.g. run the shutdown script at 15:58, EC2
stop at 16:00, at plan time).

## Credential storage

- **`system_feeder_creds`** (Upstox, Fyers): needs a migration adding
  `password_enc TEXT` and `totp_secret_enc TEXT` columns (the columns this
  table had before `8b03adf` dropped them), using the exact same
  `_encode_cred`/`_decode_cred` XOR+PBKDF2 obfuscation every other secret
  in `client_db.py` already uses. No new crypto — reuse what's there.
- **`broker_bindings`** (Zerodha / SA5770): already has `password_enc` and
  `totp_secret_enc` (currently only populated for AngelOne) — no schema
  change needed, just populating those two columns for this binding.
- **One-time seeding**, not part of the daily automation: a small
  interactive script, `scripts/seed_headless_creds.py`, prompts for each
  secret (Upstox password + TOTP base32 secret; Zerodha password + TOTP
  base32 secret; Fyers password/PIN + TOTP base32 secret), encodes and
  writes them via the existing `ClientDB` setters, and never logs or
  echoes the raw values. Run once by the user on EC2 (or locally against
  a copy of the DB, then deployed) before the first automated morning.
- Secrets are never placed in the repo, in `strategy_config.json`, in env
  vars, or in any script argument — DB-only, matching every existing
  credential in this system.

## Alerting (email, built from scratch)

- Simplest viable option for a single recipient: Gmail SMTP with an
  app-specific password (the user's own `ssrajpal2001@gmail.com`), sent via
  `smtplib` — no new AWS service, no new account needed. (AWS SES was
  considered but adds sandbox/verification overhead for a single-recipient
  use case; can be swapped in later without changing the calling code if
  volume/reliability ever demands it.)
- The Gmail app password is itself a secret — stored the same way as every
  other credential (DB, obfuscated), not in a script or config file.
- One small helper, `utils/email_alert.py` (or similar — exact location at
  plan time), used by both the morning and evening scripts: takes a
  subject + a list of `(step_name, ok, detail)` tuples, formats one plain
  summary, sends it. Failure to send the email itself is logged locally
  (can't email about the emailer being down) but never raises — must never
  be the reason the rest of the sequence aborts.
- Example morning summary shape:
  ```
  Subject: [AutoStart] 2026-09-04 — 2/3 OK, Fyers failed

  pm2 start ............. OK
  dashboard health ....... OK
  Upstox login ........... OK  (token generated 09:14:52 IST)
  Zerodha (SA5770) login .. OK  (terminal+trade enabled 09:15:10 IST)
  Fyers login ............. FAILED — Cloudflare challenge page (see log)

  Strategies auto-resumed via existing is_running flags: sell_straddle,
  oi_orb_screener (no explicit action taken — reconcile-driven).
  ```

## Error handling

- Every step wrapped individually — one broker's failure never blocks
  another broker or the strategy-resume step.
- No retries beyond the network layer's own natural timeout/retry (e.g. a
  couple of HTTP retries within a single login attempt) — a full step
  either succeeds or is reported failed for that day; no cross-step retry
  loop that could hammer a broker's login endpoint and risk a lockout.
- All secrets scrubbed from log lines (mask user IDs/emails, never log
  passwords/TOTP secrets/tokens in full — mirror the existing `_mask()`
  helper pattern already used in the pre-refactor Fyers code).
- If `run_system.py` itself fails to start (step 2), everything downstream
  is skipped (no feeder/broker login without the app running to receive
  the tokens) — this is the one hard dependency in the chain; every other
  step is independent of every other.

## Testing plan

- **Unit-testable in isolation** (no real network): TOTP code generation,
  Zerodha's checksum signature (`sha256(api_key+request_token+api_secret)`),
  response-parsing for each provider's login steps, using recorded/mocked
  HTTP responses — following the same test style already used elsewhere in
  this codebase for broker integrations.
- **Not unit-testable**: the actual login HTTP flows against Upstox/
  Zerodha/Fyers's real servers — these only prove out against the real
  services.
- Both morning and evening scripts get a `--dry-run` flag: runs the real
  login flows (so credentials/logic can be validated against production
  broker endpoints) but skips the pm2 start/stop and DB writes — safe to
  run by hand, repeatedly, during market hours, before ever trusting it to
  an unattended 09:14 cron/systemd trigger.
- Before enabling the EventBridge schedule: at least one full manual dry
  run, then one full manual live run (triggering the script by hand at the
  normal time, verifying the summary email and dashboard state match
  expectations), before flipping the AWS-side schedule on.

## Rollout order (high-level — detailed steps belong in the implementation plan)

1. DB migration for `system_feeder_creds` (password_enc/totp_secret_enc).
2. Revive + harden Upstox headless login from pre-`8b03adf` history.
3. Revive + harden Zerodha headless login from the same history.
4. Seed credentials via `scripts/seed_headless_creds.py`.
5. Build `utils/email_alert.py` + Gmail app-password setup.
6. Build `scripts/auto_morning_start.py` (steps 1-4 wired, Fyers stubbed
   as "not yet implemented, always reports failed") + `--dry-run`.
7. Manually validate Upstox + Zerodha end-to-end (dry-run, then live) on a
   real trading morning.
8. Build the Fyers Playwright attempt as its own add-on step — validate
   separately; if it doesn't work reliably within reasonable effort,
   ship without it (Fyers stays manual) rather than block the rest.
9. Build `scripts/auto_evening_stop.py`.
10. Wire `systemd` units for both scripts on EC2.
11. Set up the EventBridge + Lambda EC2 start/stop schedule.
12. One full end-to-end unattended day, watched closely, before trusting
    it to run with no one checking in.

## Open risks (explicitly accepted, not blockers)

- **Zerodha headless login is not an officially supported API path** —
  scripting the login page rather than using Kite Connect's sanctioned
  OAuth flow. Explicitly accepted by the user as the same risk any
  unofficial Kite-login automation carries.
- **Fyers headless login was already found Cloudflare-blocked** even in
  the pre-refactor code. Explicitly attempted anyway per user's choice,
  scoped as its own best-effort, separately-shippable step (#8 above) so
  it cannot hold up the rest of the automation.
- **Broker login flows are inherently fragile to upstream changes** — this
  is exactly why they were removed once already (per user's own recollection: "it broke / was unreliable"). The best-effort + email-alert design exists specifically so a future breakage degrades to "you get an email and log in by hand that day" rather than a silent trading-day loss — but the underlying fragility itself is not eliminated by this design, only made visible quickly when it happens.
