# Toggle Refactor — Design Spec
**Date:** 2026-05-30
**Status:** Approved
**Scope:** Fix Pydantic validation error, refactor Admin Feeder panel to toggle + gear pattern, enforce dual-toggle (Terminal + Trade) on Client cards, wire WebSocket auto-flip for all OAuth callbacks.

---

## 1. Problem Statement

Four distinct issues must be resolved in a single cohesive refactor:

1. **Pydantic validation error** on `/api/admin/feeder/connect` — local alias `FeederConnectSchema` is invisible to FastAPI under `from __future__ import annotations`, causing `{"loc":["query","body"],"msg":"Field required"}`.
2. **Admin Feeder panel** uses modals + "Connect Now" buttons instead of a clean toggle-per-provider pattern.
3. **Client broker cards** have Trade (`is_trade_enabled`) wired to a start/stop button pattern rather than a visual pill toggle clearly gated on Terminal state.
4. **WebSocket auto-flip** is missing — OAuth callback stores the token and sets `terminal_connected=True` in DB but the frontend never receives a push; it relies solely on polling.

---

## 2. Architecture — No New Pages, No New Tables

All changes are contained within:
- `ui_layer/dashboard_server.py` — bug fix + 2 new feeder endpoints
- `ui_layer/ws_bridge.py` — `_sys_loop` rich event forwarding
- `ui_layer/templates/monitor.html` — Admin panel rewrite, Client card toggle update, WS handler extension
- `ui_layer/dashboard_server.py` — fix 3 alias bugs; add 2 new feeder endpoints; `api_client_broker_disconnect` atomically clears trade toggle (calls existing `set_trade_enabled`)

No schema migrations. No new DB tables. No new Pydantic models.

---

## 3. Section 1 — Pydantic Bug Fix

**File:** `ui_layer/dashboard_server.py`

**Root cause:** Inside `_build_app()`, three local aliases are created:
```python
TokenUpdateSchema    = _TokenUpdateSchema
FeederConnectSchema  = _FeederConnectSchema
BrokerProvisionSchema = _BrokerProvisionSchema
```

Under `from __future__ import annotations`, FastAPI resolves type hint strings against module globals. These local aliases are not in module globals — FastAPI cannot find them and degrades the parameter to a query param named `body`.

**Fix:** Delete the three alias assignments. Update each endpoint to reference the module-level name directly:

| Endpoint | Before | After |
|---|---|---|
| `api_feeder_connect` | `body: FeederConnectSchema` | `body: _FeederConnectSchema` |
| `api_update_token` | `body: TokenUpdateSchema` | `body: _TokenUpdateSchema` |
| `client_register_broker` | `payload: BrokerProvisionSchema` | `payload: _BrokerProvisionSchema` |

---

## 4. Section 2 — Admin Feeder Panel Redesign

### 4.1 What Is Removed
- `showUpstoxModal`, `showFyersModal` state variables
- `openUpstoxModal()`, `closeUpstoxModal()`, `openFyersModal()`, `closeFyersModal()` functions
- `connectUpstox()`, `connectFyers()`, `connectDual()` functions
- `saveUpstoxCreds()`, `saveFyersCreds()` functions (replaced by unified `saveFeederCreds(provider)`)
- Both full modal HTML blocks (Upstox and Fyers modals)
- "Connect Now" button strips
- `connectDual()` button at bottom of feeder section

### 4.2 What Replaces It
Two compact toggle rows rendered from an array `['upstox','fyers']`.

**Visual layout per row:**
```
┌─ UPSTOX ──────────────────────────────────────────────────────┐
│  ● TOKEN FRESH — 2026-05-30      [ ⚙ Edit ]  [  ○──● ON  ]  │
│  ▼ Inline cred form (hidden by default, toggled by ⚙)         │
│  Client ID: [____________]  API Key: [____________]            │
│  Secret:    [____________]                       [ SAVE ]      │
└────────────────────────────────────────────────────────────────┘
```

**State variables added:**
```javascript
feederToggle:      { upstox: false, fyers: false },   // live ON/OFF state
feederToggleLoading: { upstox: false, fyers: false },
feederEditOpen:    { upstox: false, fyers: false },   // gear open/close
feederEditCreds:   {
    upstox: { client_id:'', api_key:'', secret:'' },
    fyers:  { client_id:'', api_key:'', secret:'' },
},
feederEditSaving:  { upstox: false, fyers: false },
feederEditMsg:     { upstox: '', fyers: '' },
feederMsg:         { upstox: '', fyers: '' },
feederConnected:   { upstox: false, fyers: false },   // updated by WS push
```

**State variables removed:** `showUpstoxModal`, `showFyersModal`, `upstoxMsg`, `upstoxOk`, `upstoxConnecting`, `fyersMsg`, `fyersOk`, `fyersConnecting`, `dualMsg`, `dualOk`, `dualConnecting`, `dualCreds`, `saveUpstoxMsg`, `saveUpstoxOk`, `saveFyersMsg`, `saveFyersOk`, `upstoxSecretVisible`, `fyersSecretVisible`, `fyersOauthStep`, `fyersAuthUrl`, `fyersRedirectUrl`, `fyersExchanging`.

### 4.3 Toggle ON Flow
```javascript
async toggleFeeder(provider) {
    // POST /api/admin/feeder/{provider}/connect
    // → {ok:true, connected:true, flow:"cached"}  → instant green
    // → {ok:false, flow:"oauth", auth_url:"..."}  → open tab + poll
}
```

### 4.4 Toggle OFF Flow
```javascript
async toggleFeederOff(provider) {
    // POST /api/admin/feeder/{provider}/disconnect
    // → stops feeder, marks disconnected
}
```

### 4.5 Gear / Credential Edit
```javascript
async saveFeederCreds(provider) {
    // POST /api/admin/feeder/save-creds (existing endpoint, unchanged)
    // On success: collapses form, shows [stored ✓] placeholders
}
```
Credentials are **never pre-populated in plain text** — fields show `[stored ✓]` placeholder when creds exist in DB; user must re-enter to change.

### 4.6 New Backend Endpoints (2)

**`POST /api/admin/feeder/{provider}/connect`**
```
1. get_feeder_creds_sync(provider) → api_key, secret, token, generated_at, expiry_at
2. _token_is_fresh(generated_at, expiry_at) → if True: validate_token(provider, api_key, token)
3. Token valid → {ok:true, connected:true, flow:"cached"}
4. Token invalid → generate_auth_url(provider, api_key, secret, callback_url, state, user_id)
5. → {ok:false, connected:false, flow:"oauth", auth_url:"..."}
```

**`POST /api/admin/feeder/{provider}/disconnect`**
```
1. feeder.stop() if feeder is running and active_provider == provider
2. Return {ok:true, message:"Feeder disconnected."}
```

Both endpoints require admin JWT. `provider` path param validated against `{"upstox","fyers"}`.

### 4.7 Feeder Status Polling After Toggle ON (OAuth path)
After opening the OAuth tab, frontend polls `loadFeederStatus()` every 3s (max 60 tries = 3 min) as fallback. WS push (Section 4) makes it instant when WS is healthy.

---

## 5. Section 3 — Client Broker Cards: Dual Toggle

### 5.1 What Changes
Trade (`is_trade_enabled`) becomes a visual pill-switch toggle identical in appearance to the Terminal toggle. It is **hidden/greyed** when `terminal_connected === false`.

### 5.2 Toggle State Rules
| Terminal | Trade | Effect |
|---|---|---|
| OFF | OFF | Broker disconnected, no orders |
| ON | OFF | Session live, no orders routed |
| ON | ON | Session live, **orders execute** |
| OFF | ON | Impossible — prevented by UI + backend |

**Enforcement:**
- UI: Trade toggle `disabled` and visually dimmed when `!b.terminal_connected`
- Backend: `api_client_broker_disconnect` (Terminal OFF) now also calls `set_trade_enabled(cid, bid, False)` atomically — one extra DB write, same endpoint

### 5.3 Visual Layout Per Card
```
┌─ UPSTOX_MAIN ─────────────────────────────────────────────────┐
│  Upstox · Paper mode                   [✎ Edit]  [🗑 Delete] │
│                                                                 │
│  [ ●──○ Terminal OFF ]   [ ●──○ Trade OFF ] ← greyed          │
│                                                                 │
│  ⚡ Deploy  (disabled until Terminal ON)                       │
└────────────────────────────────────────────────────────────────┘
```

### 5.4 Trade Toggle Function
```javascript
async toggleTrade(b) {
    // If terminal not connected → no-op (UI already prevents this)
    // POST /api/client/set_trade/{binding_id}  (existing endpoint, unchanged)
    // Reload clientStatus after response
}
```
No new backend endpoint needed — `set_trade_enabled` endpoint already exists at `POST /api/client/set_trade/{binding_id}`.

### 5.5 Terminal Toggle OFF — Atomic Trade Clear
`api_client_broker_disconnect` updated to call `set_trade_enabled(cid, binding_id, False)` after `set_terminal_connected(cid, binding_id, False)`. This prevents the impossible Trade-ON + Terminal-OFF state persisting in DB across sessions.

---

## 6. Section 4 — WebSocket Auto-flip

### 6.1 Backend: `ws_bridge.py` — `_sys_loop`

Extend `_sys_loop` to detect typed auth events and forward them verbatim:

```python
_AUTH_EVENT_TYPES = {"terminal_connected", "feeder_token_updated"}

async def _sys_loop(self):
    while self._running:
        evt = await asyncio.wait_for(self._sys_q.get(), timeout=1.0)
        if isinstance(evt, dict) and evt.get("type") in _AUTH_EVENT_TYPES:
            # Forward verbatim so frontend can auto-flip toggles
            await self.broadcast({
                **evt,
                "ts": datetime.now(IST).strftime("%H:%M:%S IST"),
            })
        else:
            # Existing generic sys event broadcast
            code = getattr(evt, "code", None) or (evt.get("event") if isinstance(evt, dict) else None) or ""
            msg  = getattr(evt, "message", "") or (evt.get("message", "") if isinstance(evt, dict) else "")
            await self.broadcast({"type":"sys","code":str(code),"msg":str(msg),"ts":datetime.now(IST).strftime("%H:%M:%S IST")})
```

### 6.2 Frontend: `_handle()` — Two New Cases

```javascript
} else if (msg.type === 'terminal_connected') {
    await this.loadClientStatus();
    if (this.csBrokerOp[msg.binding_id]) {
        this.csBrokerOp[msg.binding_id].ok  = true;
        this.csBrokerOp[msg.binding_id].msg = `✓ ${(msg.provider||'').toUpperCase()} connected via OAuth`;
    }

} else if (msg.type === 'feeder_token_updated') {
    await this.loadFeederStatus();
    this.feederToggle[msg.provider]    = true;
    this.feederConnected[msg.provider] = true;
    this.feederMsg[msg.provider]       = `✓ ${(msg.provider||'').toUpperCase()} connected`;
}
```

### 6.3 Polling Stays as Fallback
- Client broker cards: `_pollForToken` keeps running (60 × 3s = 3 min)
- Admin feeder: poll `loadFeederStatus()` every 3s after opening OAuth tab (same ceiling)
- WS push makes both instant when WS is healthy; polling catches the WS-down edge case

### 6.4 End-to-End Flow
```
User flips toggle ON
  → /connect → no cached token → {flow:"oauth", auth_url}
  → OAuth tab opens + polling starts (3s interval)
  → User authenticates on broker portal
  → /callback/{broker} → token stored → set_terminal_connected(True)
  → bus.publish({type:"terminal_connected", client_id, binding_id, provider})
  → _sys_loop detects auth type → broadcasts verbatim over WS
  → _handle() → loadClientStatus() + op.ok = true → toggle flips green ← instant
  → Polling detects terminal_connected=true → clears interval
```

---

## 7. Files Changed

| File | Change |
|---|---|
| `ui_layer/dashboard_server.py` | Fix 3 alias bugs; add 2 new feeder endpoints; disconnect atomically clears trade |
| `ui_layer/ws_bridge.py` | `_sys_loop` forwards auth events verbatim |
| `ui_layer/templates/monitor.html` | Admin panel rewrite (2 toggle rows + gear); client card trade toggle visual update; `_handle()` extended; old modal/button code removed |
| `data_layer/client_db.py` | No changes needed — all required methods exist |

---

## 8. Out of Scope
- No changes to execution routing logic
- No changes to DB schema or migrations
- No changes to strategy engine, risk manager, or feeders themselves
- Shoonya / Groww remain unsupported this phase
