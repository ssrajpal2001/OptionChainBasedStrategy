# Interactive OAuth Refactor — Design Spec
**Date:** 2026-05-28  
**Status:** Approved  
**Scope:** Full removal of headless automation remnants; clean Interactive OAuth flow for both Admin (feeder) and Client (broker bindings) across 6 confirmed brokers.

---

## 1. Problem Statement

The authentication layer was previously refactored to OAuth-only in `headless_auth.py` and `oauth_manager.py`, but several remnants of the old headless approach remain:

- Pydantic request schemas still accept `password`, `totp_secret`, `pin` fields on broker-facing endpoints
- DB DDL (`broker_bindings`, `system_feeder_creds`) still declares `password_enc` / `totp_secret_enc` columns
- `upsert_feeder_creds()` still accepts and stores `password` / `totp_secret`
- `api_client_add_broker` has a leftover TOTP auto-auth code block
- No admin feeder OAuth URL endpoint exists — admin cannot trigger the OAuth redirect from the UI
- Two Fyers-specific manual auth endpoints exist alongside the unified flow, creating inconsistency
- Dhan, AngelOne, and AliceBlue were incorrectly classified as "manual token" providers — they all have full browser-redirect OAuth flows

---

## 2. Design Goals

1. Zero passwords, PINs, or TOTP secrets accepted, stored, or processed anywhere in the backend
2. Single unified Interactive OAuth flow for all 6 supported brokers
3. Admin feeder and client broker bindings use identical OAuth handshake patterns
4. All 6 brokers: toggle ON → check cached token → if expired, open broker login page → user authenticates on broker's portal → broker redirects to our callback → token stored → toggle flips green → tab closes
5. All authentication steps emit execution-time logs for latency monitoring

---

## 3. Supported Brokers & OAuth Flow Types

| Provider | Flow Type | Browser Login URL | Callback Token Param | Server Exchange |
|---|---|---|---|---|
| **Fyers** | OAuth2 code | `https://api-t1.fyers.in/api/v3/generate-authcode?client_id=&redirect_uri=&response_type=code&state=` | `auth_code` | POST `api-t1.fyers.in/api/v3/validate-authcode` with SHA256(appId:secret) |
| **Upstox** | OAuth2 code | `https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id=&redirect_uri=&state=` | `code` | POST `api.upstox.com/v2/login/authorization/token` |
| **Zerodha** | OAuth2 code | `https://kite.zerodha.com/connect/login?v=3&api_key=` | `request_token` | `KiteConnect.generate_session(request_token, api_secret)` |
| **Dhan** | 3-step consent | `https://auth.dhan.co/login/consentApp-login?consentAppId=` (consentAppId generated server-side first) | `tokenId` | POST `auth.dhan.co/app/consumeApp-consent?tokenId=` with app_id/app_secret headers |
| **AngelOne** | Implicit redirect | `https://smartapi.angelone.in/publisher-login/?api_key=&state=&redirect_url=` | `auth_token` | None — `auth_token` IS the access token |
| **AliceBlue** | OAuth2 + SHA256 | `https://ant.aliceblueonline.com/?appcode=` | `authCode` + `userId` | POST `a3.aliceblueonline.com/open-api/od/v1/vendor/getUserDetails` with SHA256(userId+authCode+apiSecret) → `userSession` |

**Out of scope this phase:** Shoonya (no OAuth redirect), Groww (redirect URL unconfirmed).

---

## 4. State Routing Strategy

OAuth2 providers (Fyers, Upstox, Zerodha) pass our `?state=` query parameter back in the callback — this is used to identify admin vs client and the specific binding.

Dhan and AngelOne do **not** reliably pass back a state query param. Solution: encode state in the **callback URL path**:

```
OAuth2 providers:  /callback/{broker}?code=XXX&state={encoded_state}
Dhan/AngelOne:     /callback/{broker}/{encoded_state}?tokenId=XXX   (state in path)
AliceBlue:         /callback/{broker}?authCode=XXX&userId=XXX&state={encoded_state}
```

State format: `base64url(role|client_id|binding_id|timestamp)`
- `role` = `"admin"` | `"client"`
- `client_id` = client ID for client flows; `"feeder"` for admin feeder flows
- `binding_id` = broker binding ID; provider name for admin feeder flows

Two FastAPI callback routes:
```python
GET /callback/{broker_name}               # Fyers, Upstox, Zerodha, AliceBlue
GET /callback/{broker_name}/{path_state}  # Dhan, AngelOne
```
Both delegate to the same internal handler.

---

## 5. Dhan Consent Pre-Generation

Dhan requires a server-side API call **before** the browser redirect. The `/connect` endpoint for Dhan:

1. POSTs to `https://auth.dhan.co/app/generate-consent?client_id={dhanClientId}` with headers `app_id` (api_key) and `app_secret`
2. Receives `consentAppId`
3. Returns `{"flow": "oauth", "auth_url": "https://auth.dhan.co/login/consentApp-login?consentAppId={consentAppId}"}`

Frontend opens `auth_url`. User logs in + completes 2FA. Dhan redirects to:
`http://<server>/callback/dhan/{state}?tokenId={tokenId}`

Callback handler POSTs to `https://auth.dhan.co/app/consumeApp-consent?tokenId={tokenId}` → receives `accessToken` + `expiryTime`.

---

## 6. DB Schema Migration

### Tables changed
- `broker_bindings`: drop `password_enc`, `totp_secret_enc`
- `system_feeder_creds`: drop `password_enc`, `totp_secret_enc`

### Migration strategy
SQLite rename-recreate-copy-drop pattern inside `ClientDB._create_tables()`:

```sql
BEGIN TRANSACTION;
ALTER TABLE broker_bindings RENAME TO broker_bindings_old;
CREATE TABLE broker_bindings (...);  -- new schema, no password/totp cols
INSERT INTO broker_bindings SELECT <kept cols> FROM broker_bindings_old;
DROP TABLE broker_bindings_old;
-- same for system_feeder_creds
COMMIT;
```

Migration is conditional: check `PRAGMA table_info(broker_bindings)` first; only run if `password_enc` column exists. Wrapped in transaction — atomic.

### API changes to ClientDB
- `upsert_feeder_creds(provider, client_id, api_key, secret)` — remove `password`, `totp_secret` params
- `get_feeder_creds_sync()` return dict — remove `password`, `totp_secret` keys
- `upsert_binding()` — already silently drops password/totp; no functional change needed

---

## 7. Pydantic Schema Cleanup

All changes in `ui_layer/dashboard_server.py`:

| Schema | Change |
|---|---|
| `_FeederConnectSchema` | Remove `password`, `totp_secret`; keep only `provider` |
| `_BrokerProvisionSchema` | Remove `password`, `totp_secret`, `vendor_code`, `imei`, `client_code` |
| `_AddPortalBrokerSchema` | Remove `password`, `totp_secret` |
| `_DualFeederSchema` | Delete entirely (no route uses it) |
| `_SaveUpstoxCredsSchema` | Remove `password`, `totp_secret` |
| `_SaveFyersCredsSchema` | Remove `pin`, `totp_secret` |
| `_TokenUpdateSchema` | Remove `password`, `totp_secret` |

New schema added:
```python
class _SaveFeederCredsSchema(_PydanticBase):
    provider:   str
    client_id:  str = ""
    api_key:    str = ""
    secret:     str = ""
```

`_ClientSelfRegisterSchema` keeps `password` — this is the system dashboard login password, not a broker credential.

---

## 8. Route Changes

### Removed (2 routes)
| Route | Reason |
|---|---|
| `GET /api/client/broker/{binding_id}/fyers-auth-url` | Replaced by unified `/connect` flow |
| `POST /api/client/broker/{binding_id}/fyers-exchange` | Replaced by unified `/callback/fyers` |

### Added (3 routes)
| Route | Purpose |
|---|---|
| `GET /api/admin/feeder/auth-url?provider={p}` | Reads api_key/secret from DB, builds state `admin\|feeder\|{provider}\|ts`, returns OAuth URL |
| `POST /api/admin/feeder/save-creds` | Saves feeder credentials (client_id, api_key, secret only) |
| `GET /callback/{broker_name}/{path_state}` | Second callback route for Dhan/AngelOne (state in path) |

### Changed (1 route)
| Route | Change |
|---|---|
| `POST /api/admin/feeder/connect` | `_FeederConnectSchema` loses password/totp; if no valid token, returns `{"oauth_required": true, "auth_url": "..."}` instead of attempting connect |

### Code removed (1 block)
`api_client_add_broker` lines ~942–960: TOTP auto-auth block deleted. After saving credentials, returns:
`{"ok": true, "message": "Broker saved. Click the Terminal toggle to authenticate."}`

---

## 9. oauth_manager.py Changes

### `_OAUTH_PROVIDERS` expansion
```python
_OAUTH_PROVIDERS = {"fyers", "upstox", "zerodha", "dhan", "angelone", "aliceblue"}
_MANUAL_TOKEN_PROVIDERS = {}  # empty — all supported brokers now use OAuth redirect
```

### `generate_auth_url()` — new implementations

**Dhan** (synchronous API call to generate consentAppId first):
```python
def _dhan_auth_url(api_key, api_secret, user_id, callback_url):
    r = requests.post(
        f"https://auth.dhan.co/app/generate-consent?client_id={user_id}",
        headers={"app_id": api_key, "app_secret": api_secret},
        timeout=10,
    )
    consent_id = r.json().get("consentAppId")
    return f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_id}"
```

**AngelOne**:
```python
def _angelone_auth_url(api_key, callback_url, state):
    return (
        f"https://smartapi.angelone.in/publisher-login/"
        f"?api_key={api_key}&state={state}&redirect_url={callback_url}"
    )
```

**AliceBlue**:
```python
def _aliceblue_auth_url(api_key, state):
    # api_key is the appcode for AliceBlue
    return f"https://ant.aliceblueonline.com/?appcode={api_key}&state={state}"
```

### `exchange_code()` — new implementations

**Dhan** (consume-consent):
```python
r = requests.post(
    f"https://auth.dhan.co/app/consumeApp-consent?tokenId={auth_code}",
    headers={"app_id": api_key, "app_secret": api_secret},
    timeout=10,
)
# returns accessToken + expiryTime
```

**AngelOne** (implicit — auth_token IS the token):
```python
# No exchange needed — return auth_code directly as access_token
return True, "AngelOne token obtained.", auth_code
```

**AliceBlue** (SHA256 checksum exchange):
```python
import hashlib
checksum = hashlib.sha256(f"{user_id}{auth_code}{api_secret}".encode()).hexdigest()
r = requests.post(
    "https://a3.aliceblueonline.com/open-api/od/v1/vendor/getUserDetails",
    json={"checkSum": checksum},
    timeout=10,
)
# returns userSession as access token
```

Note: AliceBlue callback returns both `authCode` and `userId`. The `userId` is needed for the checksum. The callback handler extracts both and passes `userId` via the state's `client_id` field or as a separate query param.

### `generate_auth_url()` signature update
Add `user_id: str = ""` parameter (needed for Dhan's `client_id` in generate-consent, and AliceBlue state enrichment).

---

## 10. Callback Handler Updates (`/callback/{broker_name}`)

Existing handler already covers Fyers, Upstox, Zerodha. Extensions needed:

- Accept `path_state` as optional path param (second route for Dhan/AngelOne)
- Extract token from `tokenId` param (Dhan), `auth_token` param (AngelOne), `authCode`+`userId` params (AliceBlue)
- For AliceBlue: pass `userId` into `exchange_code()` — extend function signature with `extra: dict = {}`
- All new providers: store token via `update_access_token()`, call `set_terminal_connected()`, push `terminal_connected` WS event
- Admin callback: store via `update_feeder_token()`, push `feeder_token_updated` WS event

---

## 11. End-to-End Flows

### Client Terminal toggle ON (all providers)
```
POST /api/client/broker/{binding_id}/connect
  → check DB for fresh access_token → API ping
  → if valid: terminal_connected=1 → return {connected:true, flow:"cached"}
  → if invalid:
      Dhan: server calls generate-consent → gets consentAppId
      Others: build auth URL directly
      return {flow:"oauth", auth_url:"..."}

Frontend opens auth_url in new tab
User authenticates on broker's official portal
Broker redirects to:
  Fyers/Upstox/Zerodha/AliceBlue: /callback/{broker}?code=XXX&state={encoded_state}
  Dhan/AngelOne:                   /callback/{broker}/{encoded_state}?tokenId=XXX

Backend:
  → decode state (from query param or path)
  → call exchange_code() (or direct for AngelOne)
  → update_access_token(client_id, binding_id, token)
  → set_terminal_connected(client_id, binding_id, True)
  → bus.publish("system_event", {type:"terminal_connected", ...})
  → return HTML page: "Connected! This tab will close in 3 seconds."

Frontend WS receives terminal_connected event → toggle flips green
```

### Admin Feeder connect
```
GET /api/admin/feeder/auth-url?provider=fyers
  → read api_key/secret from system_feeder_creds
  → state = base64url(admin|feeder|{provider}|ts)
  → return {ok:true, auth_url:"..."}

Frontend opens auth_url in new tab
Admin authenticates
Broker redirects to /callback/{provider}?code=XXX&state={encoded_state}

Backend:
  → decode state → role=admin
  → exchange_code()
  → update_feeder_token(provider, token)
  → bus.publish("system_event", {type:"feeder_token_updated", provider, ok:true})
  → return HTML "Data feeder connected!"

Admin can then call POST /api/admin/feeder/connect {provider:"fyers"} to activate the live stream
```

### Callback URL format (register in broker developer console)
```
Fyers/Upstox/Zerodha/AliceBlue:  http://<server>:<port>/callback/{broker}
Dhan:                             http://<server>:<port>/callback/dhan/{state}  ← state varies per session
AngelOne:                         http://<server>:<port>/callback/angelone/{state}  ← state varies per session
```

> **Note for Dhan/AngelOne:** Because state is in the path, the registered redirect URL in the broker developer console must use a **wildcard or prefix match** (e.g. `http://<server>/callback/dhan/*`), OR configure the fixed base URL and rely on Dhan/AngelOne appending query params only. Verify in your broker developer console whether wildcard redirect URIs are supported.

---

## 12. Files Changed

| File | Changes |
|---|---|
| `data_layer/client_db.py` | DDL schema; DB migration in `_create_tables()`; `upsert_feeder_creds()` signature; `get_feeder_creds_sync()` return dict |
| `broker_auth/oauth_manager.py` | Expand `_OAUTH_PROVIDERS`; add `generate_auth_url()` + `exchange_code()` for Dhan, AngelOne, AliceBlue; add `user_id` param; add `validate_token()` for AngelOne, AliceBlue |
| `broker_auth/headless_auth.py` | Remove `requires_manual_token()` call path; all 6 providers now route to `oauth_required` |
| `ui_layer/dashboard_server.py` | Schema cleanup; remove 2 Fyers manual routes; add 3 new routes; add path-state callback route; remove TOTP block from `api_client_add_broker`; update connect handler for Dhan pre-consent call |

---

## 13. Security Invariants

- No `password`, `pin`, or `totp_secret` field accepted in any broker-facing API request body
- No `password_enc` or `totp_secret_enc` column exists in DB after migration
- Only persisted credentials: `user_id` (broker client ID), `api_key`, `api_secret`
- All identity verification (PIN, TOTP, 2FA) happens exclusively on the broker's official portal
- OAuth state contains timestamp; stale states (> 10 min) are rejected in callback handler

---

## 14. Out of Scope

- Groww (OAuth redirect URL unconfirmed — add in next phase once URL is verified)
- Shoonya/Finvasia (no OAuth redirect flow exists)
- UI/frontend changes to `monitor.html` (handled separately)
- HMAC signing of state parameter (timestamp-only is acceptable for on-premises deployment)
- Zerodha `KiteConnect.generate_session` — already implemented, no change
