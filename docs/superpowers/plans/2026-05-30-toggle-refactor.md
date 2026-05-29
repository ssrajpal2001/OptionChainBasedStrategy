# Toggle Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix Pydantic alias bug, replace Admin Feeder modals/buttons with per-provider toggles + inline gear edit, make Client Trade toggle a proper pill switch gated on Terminal state, and wire WebSocket push to auto-flip all toggles after OAuth callbacks.

**Architecture:** Four files change. Backend gets two new feeder endpoints and a disconnect trade-clear fix. WsBridge gets a one-branch addition to `_sys_loop`. Frontend drops ~200 lines of modal/button code and gains toggle rows + two new WS event handlers.

**Tech Stack:** FastAPI, Alpine.js v3 CDN, Tailwind CSS CDN, asyncio, SQLite, Python 3.9+

---

## File Map

| File | What changes |
|---|---|
| `ui_layer/dashboard_server.py` | Remove 3 local aliases → fix Pydantic bug; add `POST /api/admin/feeder/{provider}/connect`; add `POST /api/admin/feeder/{provider}/disconnect`; `api_client_broker_disconnect` atomically clears trade |
| `ui_layer/ws_bridge.py` | `_sys_loop`: branch on auth event types, forward verbatim |
| `ui_layer/templates/monitor.html` | Remove old state vars + modal JS functions + modal HTML; add feeder toggle state + `toggleFeeder` / `_pollForFeederToken` / `saveFeederCreds`; update `loadFeederStatus`; replace admin panel HTML; update `_handle()`; add Trade pill toggle to client cards; add `toggleTrade` + `tradeLoading` to `_initBrokerOp` |

---

## Task 1: Fix Pydantic Alias Bug

**Files:**
- Modify: `ui_layer/dashboard_server.py:403-405, 571, 723, 799`

**What to verify before starting:** Run the server and POST to `/api/admin/feeder/connect` with `{"provider":"mock"}`. You will see `{"detail":[{"type":"missing","loc":["query","body"]...}]}`.

- [ ] **Step 1: Remove the three local alias lines**

In `ui_layer/dashboard_server.py`, inside `_build_app()`, find and delete these 3 lines (currently at ~403-405):
```python
        TokenUpdateSchema    = _TokenUpdateSchema
        FeederConnectSchema  = _FeederConnectSchema
        BrokerProvisionSchema = _BrokerProvisionSchema
```

- [ ] **Step 2: Update the three endpoint annotations**

Find and replace each annotation:

`api_update_token` (~line 571):
```python
# Before:
        async def api_update_token(
            body: TokenUpdateSchema, _: dict = Depends(_require_admin),
        ):
# After:
        async def api_update_token(
            body: _TokenUpdateSchema, _: dict = Depends(_require_admin),
        ):
```

`api_feeder_connect` (~line 723):
```python
# Before:
        async def api_feeder_connect(
            body: FeederConnectSchema, _: dict = Depends(_require_admin),
        ):
# After:
        async def api_feeder_connect(
            body: _FeederConnectSchema, _: dict = Depends(_require_admin),
        ):
```

`client_register_broker` (~line 799):
```python
# Before:
        async def client_register_broker(
            payload: BrokerProvisionSchema,
# After:
        async def client_register_broker(
            payload: _BrokerProvisionSchema,
```

- [ ] **Step 3: Verify syntax**

```bash
python -c "import py_compile; py_compile.compile('ui_layer/dashboard_server.py', doraise=True); print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Verify the fix manually**

Start the server (`python run_system.py --mode demo --ui --port 5000`) and in another terminal:
```bash
curl -s -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"role":"admin","username":"admin","password":"admin"}' | python -m json.tool
# Copy the access_token, then:
curl -s -X POST http://localhost:5000/api/admin/feeder/connect \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <TOKEN>" \
  -d '{"provider":"mock"}' | python -m json.tool
```
Expected: `{"ok": true, "message": "Feeder connected via 'mock'."}` — NOT a validation error.

- [ ] **Step 5: Commit**

```bash
git add ui_layer/dashboard_server.py
git commit -m "Fix: Pydantic alias bug — use module-level schema names in 3 endpoint annotations"
```

---

## Task 2: WsBridge _sys_loop — Forward Auth Events Verbatim

**Files:**
- Modify: `ui_layer/ws_bridge.py:195-218`

- [ ] **Step 1: Add the auth-push type set and update `_sys_loop`**

In `ui_layer/ws_bridge.py`, add the constant after the logger declaration (after line ~34) and replace the `_sys_loop` method body:

Add constant after `logger = logging.getLogger(__name__)`:
```python
_AUTH_PUSH_TYPES = {"terminal_connected", "feeder_token_updated"}
```

Replace the `_sys_loop` method (currently lines 195-218) with:
```python
    async def _sys_loop(self) -> None:
        while self._running:
            try:
                evt = await asyncio.wait_for(self._sys_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                # Auth events forwarded verbatim so frontend auto-flips toggles
                if isinstance(evt, dict) and evt.get("type") in _AUTH_PUSH_TYPES:
                    await self.broadcast({
                        **evt,
                        "ts": datetime.now(IST).strftime("%H:%M:%S IST"),
                    })
                else:
                    code = (
                        getattr(evt, "code", None)
                        or (evt.get("event") if isinstance(evt, dict) else None)
                        or ""
                    )
                    msg = (
                        getattr(evt, "message", "")
                        or (evt.get("message", "") if isinstance(evt, dict) else "")
                    )
                    await self.broadcast({
                        "type": "sys",
                        "code": str(code),
                        "msg":  str(msg),
                        "ts":   datetime.now(IST).strftime("%H:%M:%S IST"),
                    })
            except Exception as exc:
                logger.debug("WsBridge._sys_loop: %s", exc)
```

- [ ] **Step 2: Verify syntax**

```bash
python -c "import py_compile; py_compile.compile('ui_layer/ws_bridge.py', doraise=True); print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add ui_layer/ws_bridge.py
git commit -m "Feat: WsBridge forwards terminal_connected + feeder_token_updated events verbatim for WS auto-flip"
```

---

## Task 3: New Feeder Endpoints + Disconnect Trade Clear

**Files:**
- Modify: `ui_layer/dashboard_server.py`

### 3A: Add `POST /api/admin/feeder/{provider}/connect`

- [ ] **Step 1: Add the endpoint**

In `ui_layer/dashboard_server.py`, find the existing `api_feeder_connect` endpoint (the one with `body: _FeederConnectSchema`). Insert the new endpoint **immediately before** it:

```python
        @app.post("/api/admin/feeder/{provider}/connect", tags=["Admin"])
        async def api_feeder_provider_connect(
            provider: str,
            request:  Request,
            _: dict = Depends(_require_admin),
        ):
            """
            Toggle ON for admin feeder.
            Step 1: check cached token in DB → validate via API ping.
            Step 2: if invalid/missing → generate OAuth URL for browser redirect.
            """
            import time as _time
            from broker_auth.headless_auth import _token_is_fresh
            from broker_auth.oauth_manager import (
                generate_auth_url, build_state, validate_token,
            )

            t0 = _time.monotonic()
            p  = provider.lower()
            if p not in {"upstox", "fyers"}:
                return {
                    "ok":    False,
                    "error": f"Unsupported feeder provider '{provider}'. Allowed: upstox, fyers.",
                }

            db_row  = _srv._client_db.get_feeder_creds_sync(p) or {}
            api_key = db_row.get("api_key", "")
            secret  = db_row.get("secret", "")
            user_id = db_row.get("client_id", "")
            token   = db_row.get("access_token", "")
            gen_at  = db_row.get("token_generated_at", "")
            exp_at  = db_row.get("token_expiry_at", "")

            if not api_key:
                return {
                    "ok":    False,
                    "error": (
                        f"No API key saved for '{p}'. "
                        "Click ⚙ to enter credentials first."
                    ),
                }

            logger.info(
                "[Feeder/Toggle] [%s] connect — api_key_present=%s token_present=%s",
                p, bool(api_key), bool(token),
            )

            # Step 1: cached token check
            if token and _token_is_fresh(gen_at, exp_at):
                valid = await asyncio.to_thread(validate_token, p, api_key, token)
                elapsed = (_time.monotonic() - t0) * 1000
                if valid:
                    logger.info(
                        "[Feeder/Toggle] [%s] cached token valid → instant ON in %.1fms", p, elapsed,
                    )
                    return {
                        "ok":       True,
                        "connected": True,
                        "flow":     "cached",
                        "message":  f"{p.upper()} feeder connected (cached token).",
                    }
                logger.info(
                    "[Feeder/Toggle] [%s] cached token rejected in %.1fms", p, elapsed,
                )

            # Step 2: generate OAuth URL
            base_url     = _base_url(request)
            callback_url = f"{base_url}/callback/{p}"
            state        = build_state("admin", "feeder", p)

            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, p, api_key, secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.error(
                    "[Feeder/Toggle] [%s] auth URL failed in %.1fms: %s", p, elapsed, auth_url,
                )
                return {"ok": False, "error": auth_url}

            logger.info(
                "[Feeder/Toggle] [%s] OAuth URL ready in %.1fms → awaiting login", p, elapsed,
            )
            return {
                "ok":        False,
                "connected": False,
                "flow":      "oauth",
                "auth_url":  auth_url,
                "message":   f"Open the {p.upper()} login page to authenticate.",
            }
```

### 3B: Add `POST /api/admin/feeder/{provider}/disconnect`

- [ ] **Step 2: Add the disconnect endpoint**

Insert immediately after the `api_feeder_provider_connect` endpoint above:

```python
        @app.post("/api/admin/feeder/{provider}/disconnect", tags=["Admin"])
        async def api_feeder_provider_disconnect(
            provider: str,
            _: dict = Depends(_require_admin),
        ):
            """Toggle OFF for admin feeder — stops active feeder for this provider."""
            p = provider.lower()
            if p not in {"upstox", "fyers"}:
                return {"ok": False, "error": f"Unsupported feeder provider '{p}'."}

            feeder = _srv._feeder
            if feeder is not None:
                try:
                    active = getattr(feeder, "active_provider", None)
                    if active == p or active is None:
                        await feeder.stop()
                        logger.info("[Feeder/Toggle] [%s] feeder stopped.", p)
                except Exception as exc:
                    logger.warning("[Feeder/Toggle] [%s] feeder stop raised: %s", p, exc)

            logger.info("[Feeder/Toggle] [%s] disconnect complete.", p)
            return {"ok": True, "message": f"{p.upper()} feeder disconnected."}
```

### 3C: Atomic trade clear on Terminal disconnect

- [ ] **Step 3: Update `api_client_broker_disconnect`**

Find `api_client_broker_disconnect` (~line 1391). Replace its body:

```python
        @app.post("/api/client/broker/{binding_id}/disconnect", tags=["Client"])
        async def api_client_broker_disconnect(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """Terminal toggle OFF — stops engine and clears trade toggle atomically."""
            cid = user.get("client_id", "")
            await _srv._client_db.set_terminal_connected(cid, binding_id, False)
            await _srv._client_db.set_engine_active(cid, binding_id, False)
            await _srv._client_db.set_trade_enabled(cid, binding_id, False)
            logger.info("Terminal disconnect: [%s/%s] disconnected — terminal + engine + trade cleared.", cid, binding_id)
            return {"ok": True, "message": "Terminal disconnected. Engine and Trade stopped."}
```

- [ ] **Step 4: Verify syntax**

```bash
python -c "import py_compile; py_compile.compile('ui_layer/dashboard_server.py', doraise=True); print('OK')"
```
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add ui_layer/dashboard_server.py
git commit -m "Feat: add feeder/{provider}/connect + disconnect endpoints; disconnect atomically clears trade toggle"
```

---

## Task 4: Admin Feeder Panel — Remove Old Code, Add Toggle Rows

**Files:**
- Modify: `ui_layer/templates/monitor.html`

This task has 5 atomic steps. Do them in order.

### 4A: Replace old state variables

- [ ] **Step 1: Replace old feeder state block**

Find and replace this block (lines ~496-515):
```javascript
    /* ── Credential config modals ───────────────────────────────────── */
    showUpstoxModal: false, showFyersModal: false,
    upstoxSecretVisible: false, fyersSecretVisible: false,
    saveUpstoxMsg: '', saveUpstoxOk: null, saveUpstoxLoading: false,
    saveFyersMsg:  '', saveFyersOk:  null, saveFyersLoading:  false,

    /* ── Dual feeder ────────────────────────────────────────────────── */
    dualCreds: {
      upstox_client_id:'', upstox_api_key:'', upstox_secret:'',
      fyers_client_id:'', fyers_app_key:'', fyers_secret:'',
    },
    dualMsg: '', dualOk: null, dualConnecting: false,
    showUpstoxCreds: false, showFyersCreds: false,
    upstoxMsg: '', upstoxOk: null, upstoxConnecting: false,
    fyersMsg:  '', fyersOk:  null, fyersConnecting:  false,
    fyersAuthUrl: '', fyersRedirectUrl: '', fyersOauthStep: 0, fyersExchanging: false,
```

Replace with:
```javascript
    /* ── Admin feeder toggles ───────────────────────────────────────── */
    feederToggle:        { upstox: false, fyers: false },
    feederToggleLoading: { upstox: false, fyers: false },
    feederEditOpen:      { upstox: false, fyers: false },
    feederEditCreds:     {
      upstox: { client_id:'', api_key:'', secret:'' },
      fyers:  { client_id:'', api_key:'', secret:'' },
    },
    feederEditSaving:    { upstox: false, fyers: false },
    feederEditMsg:       { upstox: '', fyers: '' },
    feederMsg:           { upstox: '', fyers: '' },
```

- [ ] **Step 2: Replace old generic feeder state block**

Find and replace this block (lines ~630-634):
```javascript
    /* ── Admin feeder panel ─────────────────────────────────────────── */
    feederProvider:  'mock',
    feederCreds:     { user_id:'', api_key:'', totp_secret:'' },
    feederMsg:       '',
    feederOk:        null,
    feederConnecting: false,
    supportedFeeders: ['mock','fyers','upstox','zerodha','dhan','angelone','aliceblue'],
```

Replace with:
```javascript
    supportedFeeders: ['mock','fyers','upstox','zerodha','dhan','angelone','aliceblue'],
```

### 4B: Remove old JS functions, add new ones

- [ ] **Step 3: Remove old feeder JS functions**

Find and delete all of the following function blocks (they are consecutive, lines ~994-1210):
- `async connectFeeder() { ... },`
- `async connectUpstox() { ... },`
- `async connectFyers() { ... },`
- `async connectDual() { ... },`
- `openUpstoxModal() { ... },`
- `closeUpstoxModal() { ... },`
- `openFyersModal() { ... },`
- `closeFyersModal() { ... },`
- `async getFyersAuthUrl() { ... },`  *(may already be removed)*
- `async saveUpstoxCreds() { ... },`
- `async saveFyersCreds() { ... },`

- [ ] **Step 4: Add new feeder JS functions**

Find the comment `/* ── Auth alerts ─────────────────────────── */` (around line 1044) and insert the following block **before** it:

```javascript
    /* ── Admin feeder toggles ───────────────────────────────────────── */
    async toggleFeeder(p) {
      if (this.feederToggleLoading[p]) return;
      if (this.feederToggle[p]) {
        // Toggle OFF
        this.feederToggleLoading[p] = true;
        this.feederMsg[p] = '';
        try {
          const r = await this._fetch(`/api/admin/feeder/${p}/disconnect`, { method: 'POST' });
          if (!r) return;
          const d = await r.json();
          this.feederToggle[p] = false;
          this.feederMsg[p] = d.message || 'Disconnected.';
          await this.loadFeederStatus();
        } catch { this.feederMsg[p] = 'Request failed.'; }
        finally { this.feederToggleLoading[p] = false; }
        return;
      }
      // Toggle ON
      this.feederToggleLoading[p] = true;
      this.feederMsg[p] = '';
      try {
        const r = await this._fetch(`/api/admin/feeder/${p}/connect`, { method: 'POST' });
        if (!r) return;
        const d = await r.json();
        if (d.ok && d.connected) {
          this.feederToggle[p] = true;
          this.feederMsg[p] = `✓ ${p.toUpperCase()} connected (cached token).`;
          await this.loadFeederStatus();
          return;
        }
        if (d.flow === 'oauth' && d.auth_url) {
          this.feederMsg[p] = `Opening ${p.toUpperCase()} login… Toggle flips automatically on success.`;
          window.open(d.auth_url, '_blank', 'width=900,height=700,menubar=no,toolbar=no');
          this._pollForFeederToken(p, 60);
          return;
        }
        this.feederMsg[p] = d.error || 'Authentication required. Save credentials first.';
      } catch (e) { this.feederMsg[p] = 'Request failed: ' + (e.message || e); }
      finally { this.feederToggleLoading[p] = false; }
    },

    _pollForFeederToken(p, maxTries) {
      let tries = 0;
      const iv = setInterval(async () => {
        tries++;
        await this.loadFeederStatus();
        if ((this.systemStatus.providers || {})[p]?.token_fresh) {
          clearInterval(iv);
          this.feederToggle[p] = true;
          this.feederMsg[p] = `✓ ${p.toUpperCase()} connected via OAuth!`;
        }
        if (tries >= maxTries) {
          clearInterval(iv);
          if (!this.feederToggle[p]) this.feederMsg[p] = 'Login timeout. Please try again.';
        }
      }, 3000);
    },

    async saveFeederCreds(p) {
      this.feederEditSaving[p] = true;
      this.feederEditMsg[p]    = '';
      try {
        const r = await this._fetch('/api/admin/feeder/save-creds', {
          json: {
            provider:  p,
            client_id: this.feederEditCreds[p].client_id,
            api_key:   this.feederEditCreds[p].api_key,
            secret:    this.feederEditCreds[p].secret,
          }
        });
        if (!r) return;
        const d = await r.json();
        this.feederEditMsg[p] = d.ok ? '✓ Credentials saved.' : (d.error || 'Save failed.');
        if (d.ok) {
          this.feederEditCreds[p] = { client_id:'', api_key:'', secret:'' };
          this.feederEditOpen[p]  = false;
          await this.loadFeederStatus();
        }
      } catch { this.feederEditMsg[p] = 'Request failed.'; }
      finally { this.feederEditSaving[p] = false; }
    },

```

### 4C: Update `loadFeederStatus` to sync toggle state

- [ ] **Step 5: Update `loadFeederStatus`**

Find the current `loadFeederStatus` function (line ~1261):
```javascript
    async loadFeederStatus() {
      try {
        const r = await this._fetch('/api/admin/feeder/status');
        if (r && r.ok) { const d = await r.json(); this.systemStatus = d; }
      } catch {}
    },
```

Replace with:
```javascript
    async loadFeederStatus() {
      try {
        const r = await this._fetch('/api/admin/feeder/status');
        if (r && r.ok) {
          const d = await r.json();
          this.systemStatus = d;
          // Sync toggle state from DB token freshness on every status load
          for (const p of ['upstox', 'fyers']) {
            const prov = (d.providers || {})[p] || {};
            if (prov.token_fresh) this.feederToggle[p] = true;
          }
        }
      } catch {}
    },
```

### 4D: Replace Admin Panel HTML

- [ ] **Step 6: Replace the two hardcoded feeder cards + connectDual button**

Find and replace this entire block (the two hardcoded cards + connectDual button, starting with the Upstox card `<div class="rounded p-3 flex flex-col gap-3"` through to `</div>` that closes the `connectDual` button's parent `<div class="flex items-center gap-3 mt-4">`):

The block to find starts with:
```html
      <!-- Upstox trigger card -->
      <div class="rounded p-3 flex flex-col gap-3" style="background:var(--t-surface);border:1px solid var(--t-border)">
        <div class="flex items-center justify-between">
          <div>
            <div class="font-ui font-bold text-sm tracking-widest" style="color:var(--t-cyan)">UPSTOX API v2</div>
```

And ends with:
```html
      <div x-show="dualMsg" class="font-mono text-xs px-3 py-2 rounded"
           :style="dualOk ? 'color:var(--t-green);background:rgba(var(--t-green-rgb),0.07);border:1px solid rgba(var(--t-green-rgb),0.25)'
                          : 'color:var(--t-red);background:rgba(var(--t-red-rgb),0.07);border:1px solid rgba(var(--t-red-rgb),0.25)'"
           x-text="dualMsg"></div>
    </div>
```

Replace the entire block with:

```html
      <!-- Feeder toggle rows — Upstox + Fyers -->
      <div class="space-y-3">
        <template x-for="p in ['upstox','fyers']" :key="p">
          <div class="rounded p-3 flex flex-col gap-2" style="background:var(--t-surface);border:1px solid var(--t-border)">

            <!-- Row: name + status + gear + toggle -->
            <div class="flex items-center gap-3">
              <div class="flex-1 min-w-0">
                <div class="font-ui font-bold text-sm tracking-widest"
                     :style="p==='upstox'?'color:var(--t-cyan)':'color:var(--t-amber)'"
                     x-text="p.toUpperCase()+' '+(p==='upstox'?'API v2':'API v3')"></div>
                <div class="font-mono text-xs mt-0.5">
                  <span x-show="(systemStatus.providers||{})[p]?.token_fresh"
                        style="color:var(--t-green)">&#x25CF; TOKEN FRESH</span>
                  <span x-show="(systemStatus.providers||{})[p]?.creds_present && !(systemStatus.providers||{})[p]?.token_fresh"
                        style="color:var(--t-amber)">&#x26A0; TOKEN EXPIRED</span>
                  <span x-show="!(systemStatus.providers||{})[p]?.creds_present"
                        style="color:var(--t-muted)">&#x25CB; No credentials</span>
                </div>
              </div>

              <!-- Gear: open/close inline cred form -->
              <button @click="feederEditOpen[p]=!feederEditOpen[p]"
                      class="text-sm px-2 py-1 rounded border font-mono transition-all"
                      style="border-color:var(--t-border);color:var(--t-muted)"
                      title="Edit credentials">&#x2699;</button>

              <!-- ON/OFF toggle -->
              <button @click="toggleFeeder(p)"
                      :disabled="feederToggleLoading[p]"
                      class="flex items-center gap-2 px-3 py-1.5 rounded-lg border font-mono text-sm font-bold transition-all"
                      :style="feederToggleLoading[p]
                        ? 'border-color:var(--t-border);color:var(--t-muted);cursor:wait'
                        : feederToggle[p]
                          ? 'background:rgba(var(--t-green-rgb),0.1);color:var(--t-green);border-color:rgba(var(--t-green-rgb),0.4)'
                          : 'background:transparent;color:var(--t-muted);border-color:var(--t-border)'">
                <template x-if="feederToggleLoading[p]">
                  <span style="display:inline-block;width:12px;height:12px;border:2px solid var(--t-muted);border-top-color:var(--t-green);border-radius:50%;animation:spin 0.7s linear infinite"></span>
                </template>
                <template x-if="!feederToggleLoading[p]">
                  <span class="w-8 h-4 rounded-full relative transition-all"
                        :style="feederToggle[p]?'background:var(--t-green)':'background:var(--t-dim)'">
                    <span class="absolute top-0.5 w-3 h-3 bg-white rounded-full shadow transition-all"
                          :style="feederToggle[p]?'left:18px':'left:2px'"></span>
                  </span>
                </template>
                <span x-text="feederToggleLoading[p]?'Connecting…':(feederToggle[p]?'ON':'OFF')"></span>
              </button>
            </div>

            <!-- Status message -->
            <div x-show="feederMsg[p]" class="font-mono text-xs px-2 py-1 rounded"
                 x-text="feederMsg[p]"
                 :style="feederToggle[p]
                   ?'color:var(--t-green);background:rgba(var(--t-green-rgb),0.07);border:1px solid rgba(var(--t-green-rgb),0.2)'
                   :'color:var(--t-amber);background:rgba(var(--t-amber-rgb),0.07);border:1px solid rgba(var(--t-amber-rgb),0.2)'"></div>

            <!-- Inline credential edit form -->
            <div x-show="feederEditOpen[p]" class="pt-2 space-y-2 border-t" style="border-color:var(--t-border)">
              <div class="font-mono text-xs" style="color:var(--t-muted)">Credentials encrypted at rest · Never logged</div>
              <div class="grid grid-cols-3 gap-2">
                <div>
                  <div class="sec-label mb-1">CLIENT ID</div>
                  <input class="t-input" type="text" x-model="feederEditCreds[p].client_id"
                         :placeholder="(systemStatus.providers||{})[p]?.creds_present?'[stored ✓]':'Broker client ID'"
                         autocomplete="off" spellcheck="false"/>
                </div>
                <div>
                  <div class="sec-label mb-1">API KEY</div>
                  <input class="t-input" type="text" x-model="feederEditCreds[p].api_key"
                         :placeholder="(systemStatus.providers||{})[p]?.creds_present?'[stored ✓]':'API key'"
                         autocomplete="off" spellcheck="false"/>
                </div>
                <div>
                  <div class="sec-label mb-1">SECRET</div>
                  <input class="t-input" type="password" x-model="feederEditCreds[p].secret"
                         :placeholder="(systemStatus.providers||{})[p]?.creds_present?'[stored ✓]':'App secret'"
                         autocomplete="off" spellcheck="false"/>
                </div>
              </div>
              <div class="flex items-center gap-3">
                <button @click="saveFeederCreds(p)" :disabled="feederEditSaving[p]"
                        class="font-ui font-bold text-xs px-4 py-1.5 rounded border"
                        style="border-color:rgba(var(--t-cyan-rgb),0.4);color:var(--t-cyan);background:rgba(var(--t-cyan-rgb),0.06)">
                  <span x-show="!feederEditSaving[p]">&#x1F4BE; SAVE</span>
                  <span x-show="feederEditSaving[p]">Saving…</span>
                </button>
                <div x-show="feederEditMsg[p]" class="font-mono text-xs"
                     :style="feederEditMsg[p].startsWith('✓')?'color:var(--t-green)':'color:var(--t-red)'"
                     x-text="feederEditMsg[p]"></div>
              </div>
            </div>

          </div>
        </template>
      </div>
```

### 4E: Remove both modal HTML blocks

- [ ] **Step 7: Delete the Upstox modal HTML block**

Find and delete everything from:
```
<!-- ═══════════════════════════════════════════════════════════════════════════
     UPSTOX API v2 CONFIGURATION MODAL
```
through to and including the closing `</div>` of that modal (line ~5806 before Fyers modal starts).

- [ ] **Step 8: Delete the Fyers modal HTML block**

Find and delete everything from:
```
<!-- ═══════════════════════════════════════════════════════════════════════════
     FYERS API v3 CONFIGURATION MODAL
```
through to and including its closing `</div>` (the last `</div>` before `</body>`).

- [ ] **Step 9: Verify the page still renders**

Start the server and open `http://localhost:5000` in a browser. Log in as admin. Confirm:
- Admin Workspace → Data Feeder section shows two toggle rows (UPSTOX and FYERS), each with ⚙ gear and ON/OFF toggle
- No modal popups appear anywhere
- No console errors about undefined variables

- [ ] **Step 10: Commit**

```bash
git add ui_layer/templates/monitor.html
git commit -m "Feat: Admin feeder panel — replace modals/buttons with per-provider toggle rows + inline gear edit"
```

---

## Task 5: Client Card — Trade Toggle as Pill Switch + WS Auto-flip

**Files:**
- Modify: `ui_layer/templates/monitor.html`

### 5A: Add `tradeLoading` and `toggleTrade`

- [ ] **Step 1: Update `_initBrokerOp` to include `tradeLoading`**

Find `_initBrokerOp` function:
```javascript
    _initBrokerOp(bid) {
      if (!this.csBrokerOp[bid]) this.csBrokerOp[bid] = {
        loading: false, termLoading: false, engLoading: false,
        termStep: '', msg: '', ok: null, step: '',
      };
    },
```

Replace with:
```javascript
    _initBrokerOp(bid) {
      if (!this.csBrokerOp[bid]) this.csBrokerOp[bid] = {
        loading: false, termLoading: false, engLoading: false, tradeLoading: false,
        termStep: '', msg: '', ok: null, step: '',
      };
    },
```

- [ ] **Step 2: Replace the existing bare `toggleTrade` function**

There is already a minimal `toggleTrade(bindingId)` at line ~1657 that has no loading state or feedback. Replace it entirely — find:
```javascript
    async toggleTrade(bindingId) {
      try {
        const r = await this._fetch(`/api/client/set_trade/${bindingId}`, { method: 'POST' });
        if (r && r.ok) await this.loadClientStatus();
      } catch {}
    },
```
Replace with:

```javascript
    /* ── Trade toggle ───────────────────────────────────────────────── */
    async toggleTrade(b) {
      if (!b.terminal_connected) return;
      const bid = b.binding_id;
      this._initBrokerOp(bid);
      const op = this.csBrokerOp[bid];
      op.tradeLoading = true;
      try {
        const r = await this._fetch(`/api/client/set_trade/${encodeURIComponent(bid)}`, { method: 'POST' });
        if (!r) return;
        const d = await r.json();
        op.ok  = d.ok;
        op.msg = d.ok
          ? (d.is_trade_enabled ? '✓ Trade enabled — orders will route here.' : '✓ Trade disabled.')
          : (d.error || 'Failed.');
        if (d.ok) await this.loadClientStatus();
      } catch (e) { op.ok = false; op.msg = 'Request failed.'; }
      finally { op.tradeLoading = false; }
    },
```

### 5B: Replace Trade toggle HTML in client broker cards

- [ ] **Step 3: Find the current Trade start/stop button and replace it**

In the client broker card section, find the button that currently controls `is_trade_enabled` — it is rendered as the "START / STOP" or `set_trade` button adjacent to the Terminal toggle. It looks like:

```html
                <!-- ── ENGINE TOGGLE ──
```

Immediately **after** the Terminal toggle `</button>` and before the Engine toggle, insert the Trade toggle:

```html
                <!-- ── TRADE TOGGLE ────────────────────────────────────────
                     Only enabled when Terminal is ON.
                     ON = orders routed here. OFF = connected but no orders. -->
                <button @click="toggleTrade(b)"
                        :disabled="!b.terminal_connected || (csBrokerOp[b.binding_id]||{}).tradeLoading"
                        class="flex items-center gap-2 px-4 py-2 rounded-lg border font-mono text-sm font-bold transition-all"
                        :title="!b.terminal_connected ? 'Connect Terminal first' : (b.is_trade_enabled ? 'Click to disable order routing' : 'Click to enable order routing')"
                        :style="!b.terminal_connected
                          ? 'opacity:0.35;cursor:not-allowed;border-color:var(--t-border);color:var(--t-muted)'
                          : (csBrokerOp[b.binding_id]||{}).tradeLoading
                            ? 'border-color:var(--t-border);color:var(--t-muted);cursor:wait'
                            : b.is_trade_enabled
                              ? \'background:rgba(var(--t-green-rgb),0.1);color:var(--t-green);border-color:rgba(var(--t-green-rgb),0.4)\'
                              : \'background:transparent;color:var(--t-muted);border-color:var(--t-border)\'">
                  <template x-if="(csBrokerOp[b.binding_id]||{}).tradeLoading">
                    <span style="display:inline-block;width:14px;height:14px;border:2px solid var(--t-muted);border-top-color:var(--t-green);border-radius:50%;animation:spin 0.7s linear infinite"></span>
                  </template>
                  <template x-if="!(csBrokerOp[b.binding_id]||{}).tradeLoading">
                    <span class="w-8 h-4 rounded-full relative transition-all"
                          :style="b.is_trade_enabled && b.terminal_connected?'background:var(--t-green)':'background:var(--t-dim)'">
                      <span class="absolute top-0.5 w-3 h-3 bg-white rounded-full shadow transition-all"
                            :style="b.is_trade_enabled && b.terminal_connected?'left:18px':'left:2px'"></span>
                    </span>
                  </template>
                  <span x-text="
                    (csBrokerOp[b.binding_id]||{}).tradeLoading ? 'Updating…'
                    : (b.is_trade_enabled && b.terminal_connected ? 'Trade ON' : 'Trade OFF')
                  "></span>
                </button>
```

**Note:** This is inserted between the Terminal toggle `</button>` and the Engine toggle `<button @click="toggleEngine(b)"`. Do not remove or replace the Engine toggle.

### 5C: Wire WS auto-flip in `_handle()`

- [ ] **Step 4: Add two new cases to `_handle()`**

Find the `_handle(msg)` function:
```javascript
    _handle(msg) {
      if (msg.type === 'tick') {
        ...
      } else if (msg.type === 'stats') {
        if (msg.name === 'clients') this.clients = msg.data || [];
        if (msg.name === 'workers') this.workers = msg.data || [];
        if (msg.name === 'brokers') this.brokers = msg.data || [];
      }
    },
```

Add two new `else if` branches at the end, just before the closing `}`:
```javascript
      } else if (msg.type === 'terminal_connected') {
        // OAuth callback fired → instantly refresh bindings + flip terminal toggle message
        this.loadClientStatus();
        if (msg.binding_id && this.csBrokerOp[msg.binding_id]) {
          this.csBrokerOp[msg.binding_id].ok  = true;
          this.csBrokerOp[msg.binding_id].msg =
            `✓ ${(msg.provider||'').toUpperCase()} connected via OAuth`;
        }
      } else if (msg.type === 'feeder_token_updated') {
        // OAuth callback fired for admin feeder → flip toggle + reload status
        this.loadFeederStatus();
        if (msg.provider && this.feederToggle[msg.provider] !== undefined) {
          this.feederToggle[msg.provider] = true;
          this.feederMsg[msg.provider]    =
            `✓ ${(msg.provider||'').toUpperCase()} connected via OAuth`;
        }
```

- [ ] **Step 5: Verify the page renders correctly**

Start the server and open `http://localhost:5000`. Log in as a client. Confirm:
- Each broker card shows **two toggles**: "Terminal OFF/ON" (amber when on) and "Trade OFF/ON" (green when on)
- Trade toggle is visually greyed and non-clickable when Terminal is OFF
- Flipping Terminal OFF → Trade also shows OFF (refreshed from DB)
- No console errors

- [ ] **Step 6: Commit**

```bash
git add ui_layer/templates/monitor.html
git commit -m "Feat: Client card Trade pill toggle + WS auto-flip for terminal_connected + feeder_token_updated"
```

---

## Task 6: End-to-End Verification + Push

- [ ] **Step 1: Full smoke test — Admin Feeder**

Start the server (`python run_system.py --mode demo --ui --port 5000`). Open `http://localhost:5000`. Log in as admin.

Check:
1. Admin Workspace → Data Feeder shows two toggle rows (UPSTOX, FYERS) — no modals
2. Click ⚙ on Upstox → inline credential form appears
3. Enter API Key + Secret → click SAVE → form collapses, "✓ Credentials saved." appears
4. Click the Upstox toggle → it shows "Connecting…" spinner briefly
5. If no valid token → browser opens Upstox OAuth tab → after login → toggle auto-flips to ON (green) within 3 seconds
6. Click the toggle again (to OFF) → toggle flips to OFF

- [ ] **Step 2: Full smoke test — Client Terminal + Trade toggles**

Log in as a client. Navigate to the broker cards.

Check:
1. Trade toggle is greyed and disabled when Terminal is OFF
2. Flip Terminal ON (for a mock provider) → Terminal toggle turns amber, Trade toggle becomes clickable
3. Flip Trade ON → Trade toggle turns green, status shows "✓ Trade enabled"
4. Flip Terminal OFF → both Terminal and Trade return to OFF (Trade cleared atomically by disconnect endpoint)
5. No "Field required" Pydantic validation errors in server logs

- [ ] **Step 3: Full smoke test — WS auto-flip**

With a real broker configured and the WS connection active:
1. Toggle ON a broker with an expired token → OAuth tab opens
2. Complete login on broker portal
3. Observe toggle flips green **immediately** without waiting for the 3-second poll cycle
4. Check server logs for `[Callback] ENTRY`, `[DB] update_access_token`, `[DB] set_terminal_connected → True`, and the WsBridge verbatim broadcast

- [ ] **Step 4: Push to origin**

```bash
git push origin master
```

Expected output includes `master -> master` with 5 new commits.

- [ ] **Step 5: Pull on EC2**

```bash
cd ~/OptionChainBasedStrategy && git pull origin master
# Then restart the server
```
