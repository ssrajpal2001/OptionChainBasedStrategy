# Interactive OAuth Refactor — Design Spec
**Date:** 2026-05-28  
**Status:** Approved  
**Scope:** Full removal of headless automation remnants; clean Interactive OAuth flow for both Admin (feeder) and Client (broker bindings)

---

## 1. Problem Statement

The authentication layer was previously refactored to OAuth-only in `headless_auth.py` and `oauth_manager.py`, but several remnants of the old headless approach remain:

- Pydantic request schemas still accept `password`, `totp_secret`, `pin` fields on broker-facing endpoints
- DB DDL (`broker_bindings`, `system_feeder_creds`) still declares `password_enc` / `totp_secret_enc` columns
- `upsert_feeder_creds()` still accepts and stores `password` / `totp_secret`
- `api_client_add_broker` has a leftover TOTP auto-auth code block
- No admin feeder OAuth URL endpoint exists — admin cannot trigger the OAuth redirect from the UI
- Two Fyers-specific manual auth endpoints exist alongside the unified flow, creating inconsistency

---

## 2. Design Goals

1. Zero passwords, PINs, or TOTP secrets accepted, stored, or processed anywhere in the backend
2. Single unified Interactive OAuth flow for all OAuth-capable providers (Fyers, Upstox, Zerodha)
3. Admin feeder and client broker bindings use identical OAuth handshake patterns
4. Manual-token providers (Dhan, AngelOne, Shoonya) unchanged — return instructions, no credentials stored
5. All authentication steps emit execution-time logs for latency monitoring

---

## 3. DB Schema Migration (Approach B)

### Tables changed
- `broker_bindings`: drop `password_enc`, `totp_secret_enc`
- `system_feeder_creds`: drop `password_enc`, `totp_secret_enc`

### Migration strategy
SQLite rename-recreate-copy-drop pattern inside `ClientDB._create_tables()`:

```
BEGIN TRANSACTION;
ALTER TABLE broker_bindings RENAME TO broker_bindings_old;
CREATE TABLE broker_bindings (...);  -- new schema, no password/totp cols
INSERT INTO broker_bindings SELECT <kept cols> FROM broker_bindings_old;
DROP TABLE broker_bindings_old;
-- same for system_feeder_creds
COMMIT;
```

Migration is conditional: check `PRAGMA table_info(broker_bindings)` first; only run if `password_enc` column exists. Wrapped in a transaction — atomic.

### API changes to ClientDB
- `upsert_feeder_creds(provider, client_id, api_key, secret)` — remove `password`, `totp_secret` params
- `get_feeder_creds_sync()` return dict — remove `password`, `totp_secret` keys
- `upsert_binding()` — already silently drops password/totp; no change needed

---

## 4. Pydantic Schema Cleanup

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

## 5. Route Changes

### Removed
| Route | Reason |
|---|---|
| `GET /api/client/broker/{binding_id}/fyers-auth-url` | Replaced by unified `/connect` flow |
| `POST /api/client/broker/{binding_id}/fyers-exchange` | Replaced by unified `/callback/fyers` |

### Added
| Route | Purpose |
|---|---|
| `GET /api/admin/feeder/auth-url?provider={p}` | Returns OAuth URL for admin feeder connect; reads api_key/secret from DB |
| `POST /api/admin/feeder/save-creds` | Saves feeder credentials (client_id, api_key, secret only) |

### Changed
| Route | Change |
|---|---|
| `POST /api/admin/feeder/connect` | `_FeederConnectSchema` loses password/totp fields; if no valid token, returns `{"oauth_required": true, "auth_url": "..."}` |

### Code block removed
`api_client_add_broker` lines ~942–960: TOTP auto-auth block deleted. After saving credentials, returns `{"ok": true, "message": "Broker saved. Click the Terminal toggle to authenticate."}`.

---

## 6. End-to-End Flows

### Client Terminal toggle ON
```
Frontend: POST /api/client/broker/{binding_id}/connect
  → check DB for fresh access_token
  → if valid: API ping → terminal_connected=1 → return {connected:true, flow:"cached"}
  → if invalid/missing: generate auth_url with state=base64(client|cid|binding_id|ts)
                        return {flow:"oauth", auth_url:"..."}

Frontend opens auth_url in new tab
User logs in on broker portal
Broker redirects → GET /callback/{provider}?code=XXX&state=YYY

Backend:
  → decode state → role=client, client_id, binding_id
  → exchange_code(provider, api_key, secret, code, callback_url)
  → update_access_token(client_id, binding_id, token)
  → set_terminal_connected(client_id, binding_id, True)
  → bus.publish("system_event", {type:"terminal_connected", ...})

Frontend WS receives terminal_connected event → toggle flips green
```

### Admin Feeder connect
```
Frontend: GET /api/admin/feeder/auth-url?provider=fyers
  → read api_key/secret from system_feeder_creds
  → build state=base64(admin|feeder|{provider}|ts)
  → return {ok:true, auth_url:"..."}

Frontend opens auth_url in new tab
Admin logs in on broker portal
Broker redirects → GET /callback/fyers?code=XXX&state=YYY

Backend:
  → decode state → role=admin
  → read api_key/secret from system_feeder_creds
  → exchange_code(provider, api_key, secret, code, callback_url)
  → update_feeder_token(provider, token)
  → bus.publish("system_event", {type:"feeder_token_updated", provider, ok:true})

Admin WS receives feeder_token_updated → feeder toggle flips green
Then admin calls POST /api/admin/feeder/connect {provider:"fyers"} to activate the live feed
```

### Manual-token providers (Dhan, AngelOne, Shoonya)
```
POST /api/client/broker/{binding_id}/connect
  → no valid token → generate_auth_url returns (False, instructions_string)
  → return {flow:"manual_token", instructions:"..."}
Frontend shows instructions modal — no credentials accepted or stored
```

### Callback URL format (must be registered in broker developer console)
```
http://<server-host>:<port>/callback/fyers    (Fyers)
http://<server-host>:<port>/callback/upstox   (Upstox)
http://<server-host>:<port>/callback/zerodha  (Zerodha)
```
Callback URL is dynamically constructed from `request.url.scheme` + `request.url.netloc` at runtime.

---

## 7. Files Changed

| File | Changes |
|---|---|
| `data_layer/client_db.py` | `_DDL` schema update; DB migration in `_create_tables()`; `upsert_feeder_creds()` signature; `get_feeder_creds_sync()` return dict |
| `ui_layer/dashboard_server.py` | Schema cleanup; remove 2 Fyers manual routes; add 2 admin feeder routes; change feeder connect; remove TOTP auth block from `api_client_add_broker` |
| `broker_auth/headless_auth.py` | No changes needed — already clean |
| `broker_auth/oauth_manager.py` | No changes needed — already complete |

---

## 8. Security Invariants

- No `password`, `pin`, or `totp_secret` field accepted in any broker-facing API request body
- No `password_enc` or `totp_secret_enc` column exists in the DB after migration
- Only persisted credentials: `client_id` (broker user ID), `api_key`, `api_secret`
- All identity verification (PIN, TOTP) happens exclusively on the broker's official portal
- OAuth state parameter is base64-encoded with timestamp; parsed on callback to identify admin vs client

---

## 9. Out of Scope

- UI/frontend changes to `monitor.html` (handled separately)
- Zerodha KiteConnect session (`generate_session`) — already implemented in `oauth_manager.py`, no change
- CSRF hardening of state parameter (timestamp-only; no HMAC) — acceptable for on-premises deployment
