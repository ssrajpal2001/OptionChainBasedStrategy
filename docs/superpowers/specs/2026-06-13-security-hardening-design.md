# Security & Code Quality Hardening — Design Spec
**Date:** 2026-06-13  
**Branch:** feat/security-hardening (from feat/delta-crypto-integration)  
**Status:** Approved by user

---

## 1. Scope

Six work streams to make the application production-secure, non-redundant, performant, and meeting industrial standards before launch.

1. Redundancy cleanup
2. Performance fixes
3. Error handling hardening
4. Security (password enforcement + forgot-password flow)
5. Industrial standards (Pydantic models, test config, schema validation, data-driven config)

---

## 2. Redundancy Cleanup

### 2a — `strike_utils.py` (new file)
**File:** `strategies/strike_utils.py`

```python
def compute_atm(spot: float, step: float) -> float:
    return round(spot / step) * step
```

Replace all 3+ inline `round(spot / step) * step` occurrences:
- `strategies/iron_condor.py:457`
- `strategies/trap_strike_selection.py:49`
- Any other inline occurrence found via grep

### 2b — `_decode_cred` duplicate
**File:** `run_system.py:260-268`  
Delete local `_bdec` function. Import `_decode_cred` from `data_layer.client_db`.

### 2c — Logger factory
**File:** `utils/logging_utils.py` (new file)

```python
def make_rotating_logger(name: str, log_path: str, level=logging.INFO) -> logging.Logger:
    """Create a file+console logger with daily rotation. Idempotent."""
```

Replace duplicated logger setup in:
- `run_system.py:141-159` (`get_client_logger`)
- `strategies/sell_straddle.py:60-83` (`_make_strategy_logger`)
- `strategies/iron_condor.py:67-82` (`_make_ic_logger`)

### 2d — Dead code removal
In `strategies/sell_straddle.py`, delete:
- `_try_smart_roll()` method
- `classify_roll()` function/method

---

## 3. Performance Fixes

### 3a — Batch DB query in `straddle_book_manager`
**File:** `data_layer/client_db.py` — add:

```python
def get_running_straddle_deployments_sync(self) -> list[dict]:
    """Single JOIN query: all is_running=1 sell_straddle deployments across all clients."""
```

SQL:
```sql
SELECT c.client_id, d.*
FROM clients c
JOIN strategy_deployments d ON c.client_id = d.client_id
WHERE c.is_active = 1
  AND d.strategy_name = 'sell_straddle'
  AND d.is_running = 1
```

**File:** `strategies/straddle_book_manager.py:_wanted()` — replace N+1 loop with single call to `get_running_straddle_deployments_sync()`.

### 3b — `import time` at module level
**File:** `strategies/iron_condor.py`  
Move `import time` from inside function bodies (lines 417, 440) to module top.

---

## 4. Error Handling

### 4a — Broker auth failure is fatal
**File:** `execution_bridge/execution_router.py:start()`

Current: logs `logger.error(...)` and silently continues.  
New behaviour:
- Collect all failed `(client_id, binding_id)` pairs into a list
- After the auth loop: if any failures → `logger.critical(...)` with `exc_info=True` per failure
- Raise `RuntimeError(f"Broker auth failed for: {failed_list}. System cannot start.")` — caller in `run_system.py` catches and calls `sys.exit(1)` with a clear message.
- In `run_system.py` startup: catch `RuntimeError` from router start, print actionable message, exit.

### 4b — `exc_info=True` on swallowed exceptions
**File:** `strategies/straddle_book_manager.py:108-116`  
Add `exc_info=True` to all bare `except Exception as exc: logger.warning(...)` blocks so stack traces appear in logs.

### 4c — Missing strategy config warning
**File:** `strategies/sell_straddle.py:_load_thresholds()`  
At the top of the method, after loading `ss = ...`:
```python
if not ss:
    logger.warning("SellStraddle[%s]: 'sell_straddle' config section missing — using defaults.", self._underlying)
```

---

## 5. Security

### 5a — Startup password enforcement
**File:** `run_system.py` (live mode startup path)

```python
def _enforce_secrets(mode: str) -> None:
    if mode != "live":
        return
    pwd = os.getenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    if pwd in ("admin123", "", "changeme"):
        sys.exit(
            "FATAL: TERMINUS_ADMIN_PASSWORD is the default/empty value.\n"
            "Set a strong password: export TERMINUS_ADMIN_PASSWORD=<your-password>\n"
            "Then restart."
        )
    jwt = os.getenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    if "CHANGE-IN-PRODUCTION" in jwt or len(jwt) < 32:
        sys.exit(
            "FATAL: TERMINUS_JWT_SECRET is the dev default or too short (< 32 chars).\n"
            "Set it: export TERMINUS_JWT_SECRET=<random-32+-char-string>"
        )
```

Called at the very top of `main()` before any async work.

### 5b — DB-stored admin password (enables change-password without restart)
**Table:** `system_settings(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)`  
Created in `client_db.py:_init_db()`.

Key: `admin_password_hash` — value: output of `hash_password(new_password)`.

**Login check order** (`dashboard_server.py` login endpoint):
1. Check `system_settings.admin_password_hash` — if present, verify with `verify_password()`
2. Else fall back to `TERMINUS_ADMIN_PASSWORD` env var

**New endpoint:** `POST /api/admin/change-password`  
Request: `{current_password: str, new_password: str}`  
- Verify `current_password` against current credential (DB hash or env var)
- Validate `new_password` length ≥ 8
- Write `hash_password(new_password)` → `system_settings['admin_password_hash']`
- Returns `{"ok": true}`

### 5c — Client password stored in DB
**Column:** `clients.pin_hash TEXT` (nullable, added via `ALTER TABLE IF NOT EXISTS`).

**Login check order** for clients:
1. Check `clients.pin_hash` — if present, verify with `verify_password()`
2. Else fall back to `TERMINUS_CLIENT_PIN_<ID>` env var → `client_id` default

### 5d — Forgot password (Option B — Admin-generated one-time token)

**New DB table:**
```sql
CREATE TABLE IF NOT EXISTS password_resets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash  TEXT NOT NULL,
    target_role TEXT NOT NULL,  -- 'admin' | 'client'
    target_id   TEXT NOT NULL,  -- client_id or 'admin'
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    used        INTEGER NOT NULL DEFAULT 0
)
```

**Token generation:**
- `secrets.token_urlsafe(32)` → plaintext token (returned to admin, shown once)
- `hashlib.sha256(token.encode()).hexdigest()` stored in DB
- Expires in **24 hours**

**New admin endpoint:** `POST /api/admin/client/{client_id}/reset-token`  
- Generates token, stores hash, returns `{"ok": true, "token": "<plaintext>"}` (shown once in UI)
- Admin UI: "Generate Reset Token" button per client in client-profiles panel → copies token to clipboard with one click

**Public reset endpoint:** `POST /api/auth/reset-password`  
Request: `{token: str, new_password: str}`  
- Hash the incoming token, look up in `password_resets` where `used=0` and `expires_at > now`
- Verify found; write new `pin_hash` / `admin_password_hash`; mark `used=1`
- Returns `{"ok": true}` or `{"ok": false, "error": "Invalid or expired token"}`

**Admin forgot password:**  
No self-service flow (single admin, SSH required). Login page shows:  
*"Admin password reset: SSH to server and run `export TERMINUS_ADMIN_PASSWORD=<new>`  then `pm2 restart algo`."*

**UI changes (`monitor.html`):**
- Login form: add "Forgot password?" link (below the login button)
- Clicking it reveals a second form: `Reset Token` input + `New Password` + `Confirm Password` + Submit
- On success: show "Password updated — please log in." and flip back to login form

---

## 6. Industrial Standards

### 6a — Pydantic request models
**File:** `ui_layer/dashboard_server.py`  
Add `BaseModel` subclasses (at module level, not inside functions — required due to `from __future__ import annotations`):

```python
class LoginRequest(BaseModel):
    username: str
    password: str

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

class UpsertBindingRequest(BaseModel):
    # fields matching current dict payload
    ...

class CreateClientRequest(BaseModel):
    ...
```

Apply to: login, reset-password, change-password, create-client, upsert-binding endpoints.

### 6b — `pytest.ini`
**File:** `pytest.ini` (project root)

```ini
[pytest]
testpaths = tests
addopts = --tb=short -q --cov=strategies --cov=data_layer --cov=execution_bridge --cov=ui_layer --cov-report=term-missing --cov-fail-under=55
```

Also add `pytest-cov` to the requirements comment block in `CLAUDE.md`.

### 6c — Runtime config schema validation
**File:** `data_layer/runtime_config.py` — add:

```python
_REQUIRED_SS_KEYS = {"entry_rules_beginning", "exit_rules"}

def validate_index_section(index: str, section: str, raw: dict) -> None:
    """Log warnings for missing required keys in a strategy config section."""
    for key in _REQUIRED_SS_KEYS:
        if key not in raw:
            logger.warning("RuntimeConfig[%s/%s]: missing expected key '%s' — defaults will apply.", index, section, key)
```

Called from `SellStraddleStrategy._load_thresholds()` after loading `ss`.

### 6d — Data-driven underlying config
**Table:** `system_settings` (shared with 5b above, key-value store)

Keys: `strike_step_{UNDERLYING}`, `lot_size_{UNDERLYING}`  
Example: `strike_step_NIFTY=50`, `lot_size_NIFTY=75`

**File:** `config/global_config.py:ExchangeConfig`  
Add method:
```python
def load_from_db(self, db) -> None:
    """Override strike_steps and lot_sizes from system_settings table if present."""
```

Called once at startup from `run_system.py` after DB is initialised. Hardcoded dicts remain as fallback defaults — no code change needed to add a new underlying if it's in the DB.

**Admin endpoint:** `POST /api/admin/system-settings` with `{key, value}` — stores/updates a `system_settings` row.

---

## 7. Execution Order

```
Phase 1 (no-risk, new files):
  - strategies/strike_utils.py
  - utils/logging_utils.py
  - client_db.py: system_settings + password_resets tables + batch query + pin_hash column

Phase 2 (swap call sites — redundancy + performance):
  - Replace inline ATM calc everywhere → strike_utils.compute_atm
  - Delete _bdec from run_system.py, import _decode_cred
  - Replace logger factories → make_rotating_logger
  - Delete dead _try_smart_roll / classify_roll
  - straddle_book_manager._wanted() → batch query
  - iron_condor.py: move import time to top

Phase 3 (error handling):
  - execution_router.start() → fatal on auth failure
  - straddle_book_manager: exc_info=True
  - sell_straddle._load_thresholds(): missing section warning

Phase 4 (security):
  - run_system.py: _enforce_secrets()
  - dashboard_server.py: DB-first login for admin + client
  - New endpoints: change-password, reset-token (admin), reset-password (public)
  - monitor.html: forgot password UI

Phase 5 (industrial standards):
  - Pydantic request models on key endpoints
  - pytest.ini
  - Runtime config validation
  - ExchangeConfig.load_from_db() + admin system-settings endpoint

Phase 6 (tests):
  - tests/test_strike_utils.py
  - tests/test_password_reset.py
  - tests/test_startup_enforcement.py
  - Re-run full suite: 228+ tests pass
```

---

## 8. Non-Goals

- Email/SMTP reset flow (deferred — Option B chosen)
- Multi-admin support
- OAuth / SSO
- Full OpenAPI spec (only key endpoints get Pydantic models)
- CI/CD pipeline (pytest.ini only, no GitHub Actions)
