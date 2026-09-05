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


def _click_if_present(page, text: str, timeout: int = 3000) -> bool:
    """Best-effort click on visible text -- a no-op (returns False) if the
    element doesn't exist or isn't clickable, never raises. Used for the
    Mobile-number/Client-ID tab toggle, which may or may not be present
    depending on Fyers' current page state."""
    try:
        loc = page.get_by_text(text, exact=True)
        if loc.count() > 0:
            loc.first.click(timeout=timeout)
            return True
    except Exception:
        pass
    return False


def _fill_first_match(page, selectors: list, value: str, timeout: int = 10000) -> str:
    """Try each selector in order, filling the first one that resolves.
    Raises the LAST exception seen (most likely to be the most informative,
    since earlier candidates in the list are usually the primary/expected
    one) if none work -- keeps the real Fyers page's exact DOM structure
    from being a single point of failure for this whole login attempt."""
    last_exc = None
    for sel in selectors:
        try:
            page.fill(sel, value, timeout=timeout)
            return sel
        except Exception as exc:
            last_exc = exc
    raise last_exc


def _fill_and_enable(page, selectors: list, value: str, submit_selector: str,
                      fill_timeout: int = 10000, enable_timeout: int = 3000) -> str:
    """2026-09-06 real incident: .fill() alone left Fyers' own submit button
    permanently disabled -- the value visibly landed in the field (we got
    past the earlier fill-timeout error entirely) but the button's own
    client-side validation (a React-controlled-input pattern that validates
    on its real onChange/onBlur handlers) never fired, because .fill()'s
    synthetic value-set doesn't always trigger those on every framework.
    Sequence: fill -> Tab (blur, the most common validation trigger) -> if
    the submit button is still disabled, clear and retry with real
    keystroke-by-keystroke .type() (dispatches genuine per-character input
    events no framework can miss) -> Tab again."""
    matched_sel = _fill_first_match(page, selectors, value, fill_timeout)
    page.keyboard.press("Tab")
    try:
        page.wait_for_selector(f"{submit_selector}:not([disabled])", timeout=enable_timeout)
        return matched_sel
    except Exception:
        pass
    page.fill(matched_sel, "")
    page.type(matched_sel, value, delay=50)
    page.keyboard.press("Tab")
    page.wait_for_selector(f"{submit_selector}:not([disabled])", timeout=enable_timeout * 2)
    return matched_sel


def _dump_field_diagnostics(page) -> str:
    """2026-09-06 real incident: after two guess-and-check rounds (viewport,
    then tab-click + type()-fallback) still didn't enable Fyers' own submit
    button, guessing a THIRD time isn't a good use of another live
    round-trip -- get real ground truth instead. Reports, for every
    selector this module knows about, whether it exists, is visible, is
    disabled, and its current value -- so a mismatch (e.g. our fill landed
    in a hidden duplicate, or the real field has a different id entirely)
    is visible directly from the log instead of inferred from a timeout
    message alone. Every check is independently wrapped -- one failing
    lookup never blocks the others from reporting."""
    selectors = [
        "input[id='fy_client_id']", "input[name='fy_client_id']",
        "input[placeholder*='Client ID' i]", "button[id='clientIdSubmit']",
        "input[id='fy_totp']", "button[id='totpSubmit']",
        "input[id='fy_pin']", "button[id='pinSubmit']",
    ]
    lines = []
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = loc.count()
            if n == 0:
                lines.append(f"{sel}: count=0")
                continue
            # 2026-09-06 real incident: a prior version only ever inspected
            # .first -- when count>1 (confirmed real: fy_client_id matched 2
            # elements), the SECOND one could be the one the submit button's
            # own validation is actually wired to, silently unfilled, while
            # .first looked perfectly filled. Inspect every match, not just
            # the first, so a mismatch between duplicates is visible.
            per_element = []
            for i in range(n):
                el = loc.nth(i)
                try:
                    vis = el.is_visible()
                except Exception:
                    vis = "<n/a>"
                try:
                    val = el.input_value()
                except Exception:
                    val = "<n/a (not an input?)>"
                try:
                    disabled = el.is_disabled()
                except Exception:
                    disabled = "<n/a>"
                per_element.append(f"[{i}]visible={vis},disabled={disabled},value={val!r}")
            lines.append(f"{sel}: count={n} " + " ".join(per_element))
        except Exception as exc:
            lines.append(f"{sel}: <lookup error: {exc}>")
    # Radio state (2026-09-06): the form_html snapshot showed mobile_rb's
    # `checked=""` HTML ATTRIBUTE still present even after our "Client ID"
    # text click -- but that attribute reflects the ORIGINAL server-rendered
    # markup, not necessarily the live DOM/React state after a click. Query
    # the actual runtime .checked PROPERTY directly to know for certain
    # whether the tab switch really took effect.
    for radio_id in ("mobile_rb", "clientId_rb"):
        try:
            checked = page.eval_on_selector(f"#{radio_id}", "el => el.checked")
            lines.append(f"#{radio_id}.checked={checked}")
        except Exception as exc:
            lines.append(f"#{radio_id}: <lookup error: {exc}>")
    # 2026-09-06 real incident: the field is confirmed correctly filled (right
    # value, right visible element) via BOTH .fill() and real keystroke
    # .type(), yet clientIdSubmit stayed disabled either way -- ruling out a
    # simple "wrong duplicate" or "fill didn't fire events" explanation.
    # That combination is the classic signature of an invisible CAPTCHA
    # token gating the button rather than the field's own value -- check
    # directly for the usual suspects (reCAPTCHA/hCaptcha/Cloudflare
    # Turnstile) instead of guessing at more selectors.
    try:
        captcha_info = page.evaluate("""() => ({
            grecaptcha: typeof window.grecaptcha !== 'undefined',
            hcaptcha: typeof window.hcaptcha !== 'undefined',
            turnstile: typeof window.turnstile !== 'undefined',
            recaptcha_iframe: document.querySelectorAll("iframe[src*='recaptcha']").length,
            hcaptcha_iframe: document.querySelectorAll("iframe[src*='hcaptcha']").length,
            turnstile_iframe: document.querySelectorAll("iframe[src*='turnstile'], iframe[src*='challenges.cloudflare']").length,
            g_recaptcha_div: document.querySelectorAll(".g-recaptcha, [data-sitekey]").length,
        })""")
        lines.append(f"captcha_check={captcha_info}")
    except Exception as exc:
        lines.append(f"captcha_check: <lookup error: {exc}>")
    try:
        form_html = page.eval_on_selector("form", "el => el.outerHTML")
        # 2026-09-06: 1500 chars cut off before ever reaching the actual
        # Client ID input section (only the now-hidden mobile section was
        # captured) -- widened substantially so the real section renders.
        lines.append(f"form_html={form_html[:6000]!r}")
    except Exception as exc:
        lines.append(f"form_html: <no <form> found or eval failed: {exc}>")
    return " || ".join(lines)


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
                # 2026-09-06, second real incident (viewport fix alone wasn't
                # enough): captured page content showed the real login page
                # presents a "Mobile number" / "Client ID" TOGGLE with a "+91"
                # country-code prefix visible by default -- strongly implying
                # Mobile Number is the default-active tab and the Client ID
                # input only becomes visible after switching to that tab.
                # Click it first (best-effort -- a no-op if it's already the
                # active tab or this specific toggle doesn't exist), then try
                # a few candidate selectors for the input itself since the
                # exact id may also have changed since this was first written.
                _click_if_present(page, "Client ID")
                _fill_and_enable(page, [
                    "input[id='fy_client_id']:visible",
                    "input[name='fy_client_id']:visible",
                    "input[placeholder*='Client ID' i]:visible",
                ], client_id, "button[id='clientIdSubmit']")
                page.click("button[id='clientIdSubmit']:visible")
                _fill_and_enable(page, ["input[id='fy_totp']:visible"], totp_code, "button[id='totpSubmit']")
                page.click("button[id='totpSubmit']:visible")
                _fill_and_enable(page, ["input[id='fy_pin']:visible"], pin, "button[id='pinSubmit']")
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
                        _fields = _dump_field_diagnostics(page)
                        _diag = (
                            f" | page_title={_title!r} page_url={_url!r} "
                            f"body_snippet={_body!r} | fields: {_fields}"
                        )
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
