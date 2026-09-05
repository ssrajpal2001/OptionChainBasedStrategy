"""
broker_auth/headless_totp_auth_fyers.py -- best-effort Fyers headless login
via Playwright (real browser automation).

Fyers' own vagator API was found Cloudflare-blocked even in this repo's
pre-refactor code (see docs/superpowers/specs/2026-09-03-fully-automated-
daily-lifecycle-design.md, "Open risks") -- this module attempts a real
headless-browser login instead, per direct user decision to try anyway.
Explicitly allowed to fail; scripts/auto_morning_start.py treats this as a
best-effort step that never blocks Upstox/Zerodha or strategy resume.
"""
from __future__ import annotations

from playwright.sync_api import sync_playwright


class FyersHeadlessLoginError(Exception):
    """Fyers headless login failed at a specific step (often Cloudflare)."""


def fyers_totp_login(
    client_id: str, app_id: str, password: str, totp_secret: str, pin: str,
    redirect_uri: str = "",
) -> str:
    """redirect_uri (2026-09-06, same class of real incident as Upstox's own
    redirect_uri fix): the token-exchange call below used to hardcode
    redirect_uri="" on the fyersModel.SessionModel -- Fyers' token exchange
    validates this against what's actually registered for app_id, same as
    Upstox does. Callers must resolve the real one from
    system_settings.GLOBAL_REDIRECT_BASE (+ "/callback/fyers"), the same
    value the app's real interactive OAuth flow already uses -- no default
    is provided so a caller that forgets gets a clear error here instead of
    a confusing Fyers-side rejection."""
    import pyotp

    if not client_id:
        raise FyersHeadlessLoginError("Fyers: client_id is required.")
    if not totp_secret:
        raise FyersHeadlessLoginError("Fyers: totp_secret is required.")
    if not redirect_uri:
        raise FyersHeadlessLoginError(
            "Fyers: redirect_uri is required — pass the same value saved in "
            "system_settings.GLOBAL_REDIRECT_BASE (+ '/callback/fyers'), the "
            "one already used by the real interactive OAuth flow. Do not "
            "hardcode a guess."
        )

    try:
        totp_code = pyotp.TOTP(totp_secret.upper().replace(" ", "").replace("-", "")).now()
    except Exception as exc:
        raise FyersHeadlessLoginError(f"Fyers: invalid TOTP secret — {exc}")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = None
            try:
                # 2026-09-06 real incident: headless Chromium's default (small)
                # viewport tripped Fyers' responsive layout, which renders the
                # login form TWICE (desktop + mobile variants, one hidden via
                # CSS) -- Playwright's locator resolved 2 elements for
                # input[id='fy_client_id'] and picked the hidden one, timing
                # out on fill(). An explicit desktop-sized viewport avoids the
                # mobile breakpoint in the first place; the `:visible` filter
                # (a Playwright-specific selector extension) is kept as a
                # defense-in-depth second layer in case a duplicate persists
                # for some other reason.
                page = browser.new_page(viewport={"width": 1366, "height": 900})
                page.goto("https://login.fyers.in/")
                page.fill("input[id='fy_client_id']:visible", client_id)
                page.click("button[id='clientIdSubmit']:visible")
                page.fill("input[id='fy_totp']:visible", totp_code)
                page.click("button[id='totpSubmit']:visible")
                page.fill("input[id='fy_pin']:visible", pin)
                page.click("button[id='pinSubmit']:visible")
                page.wait_for_url("**/api-login/redirect-uri/**", timeout=20000)
                final_url = page.url
                from urllib.parse import parse_qs, urlparse
                qs = parse_qs(urlparse(final_url).query)
                auth_code = (qs.get("auth_code") or [""])[0]
                if not auth_code:
                    raise FyersHeadlessLoginError(f"Fyers: auth_code not found in redirect URL {final_url!r}")
                from fyers_apiv3 import fyersModel
                session = fyersModel.SessionModel(
                    client_id=app_id, secret_key="", redirect_uri=redirect_uri,
                    response_type="code", grant_type="authorization_code",
                )
                session.set_token(auth_code)
                resp = session.generate_token()
                access_token = resp.get("access_token", "")
                if not access_token:
                    raise FyersHeadlessLoginError(f"Fyers: token exchange failed — {resp}")
                return access_token
            except Exception as exc:
                # 2026-09-06 real incident: a bare "Timeout 30000ms exceeded"
                # tells us the fill never happened, but not WHY -- could be a
                # captcha/bot-check page (Fyers' vagator API was already known
                # Cloudflare-blocked before this Playwright approach was even
                # tried, per this module's own docstring), a changed form, or
                # something else entirely. Capture what's ACTUALLY on the page
                # at the moment of failure so the next attempt is diagnosable
                # from the log alone. Never lets a diagnostic failure mask the
                # real error.
                _diag = ""
                if page is not None:
                    try:
                        _title = page.title()
                        _url = page.url
                        _body = page.inner_text("body")[:300]
                        _diag = f" | page_title={_title!r} page_url={_url!r} body_snippet={_body!r}"
                    except Exception:
                        _diag = " | (could not capture page diagnostics)"
                if isinstance(exc, FyersHeadlessLoginError):
                    raise FyersHeadlessLoginError(str(exc) + _diag)
                raise FyersHeadlessLoginError(str(exc) + _diag)
            finally:
                browser.close()
    except FyersHeadlessLoginError:
        raise
    except Exception as exc:
        raise FyersHeadlessLoginError(str(exc))
