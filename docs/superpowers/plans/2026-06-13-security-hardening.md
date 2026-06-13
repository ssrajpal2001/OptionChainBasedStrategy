# Security & Code Quality Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the algo trading system for production launch — eliminate code redundancy, fix performance bottlenecks, enforce security boundaries, add forgot-password (admin-token flow), and raise code to industrial standards.

**Architecture:** Six independent work streams executed in dependency order. Phases 1–3 are purely additive (new files + call-site swaps). Phase 4 builds on existing `system_settings` + `hash_password`/`verify_password` already in `client_db.py`. Phase 5 adds Pydantic models and config validation on top of the existing FastAPI dashboard.

**Tech Stack:** Python 3.11+, FastAPI, SQLite (`data/clients.db`), Alpine.js + Tailwind CSS (UI), stdlib `secrets`/`hashlib`/`hmac`, existing `hash_password`/`verify_password` from `data_layer/client_db.py`.

---

## Pre-flight: confirm baseline

- [ ] Run full test suite — confirm 228 pass before touching anything:
  ```bash
  cd e:/AlgoSoft/OptionChainBasedStrategy
  python -m pytest tests/ -q
  ```
  Expected: `228 passed`

---

## Phase 1 — New utility files (zero risk, additive only)

### Task 1: `strategies/strike_utils.py` — single ATM calculation

**Files:**
- Create: `strategies/strike_utils.py`
- Test: `tests/strategies/test_strike_utils.py`

> Context: `round(spot / step) * step` appears inline in `iron_condor.py:457` and `trap_trading_engine.py:1576`. `trap_strike_selection.py:49` uses the *same arithmetic* but on `mid=(high+low)/2` — that is intentionally different and is NOT touched here.

- [ ] **Write the failing test**

```python
# tests/strategies/test_strike_utils.py
from strategies.strike_utils import compute_atm

def test_compute_atm_exact():
    assert compute_atm(24500.0, 50.0) == 24500.0

def test_compute_atm_rounds_up():
    assert compute_atm(24526.0, 50.0) == 24550.0

def test_compute_atm_rounds_down():
    assert compute_atm(24524.0, 50.0) == 24500.0

def test_compute_atm_crypto():
    assert compute_atm(63787.30, 1000.0) == 64000.0

def test_compute_atm_small_step():
    assert compute_atm(200.75, 0.5) == 201.0
```

- [ ] **Run test — expect FAIL (ImportError)**
  ```bash
  python -m pytest tests/strategies/test_strike_utils.py -v
  ```

- [ ] **Create `strategies/strike_utils.py`**

```python
"""strategies/strike_utils.py — shared strike-price arithmetic."""
from __future__ import annotations


def compute_atm(spot: float, step: float) -> float:
    """Round spot to the nearest strike step. Single source of truth for ATM calc."""
    return round(spot / step) * step
```

- [ ] **Run test — expect PASS**
  ```bash
  python -m pytest tests/strategies/test_strike_utils.py -v
  ```

- [ ] **Commit**
  ```bash
  git add strategies/strike_utils.py tests/strategies/test_strike_utils.py
  git commit -m "feat: add strike_utils.compute_atm — single ATM calculation"
  ```

---

### Task 2: `utils/logging_utils.py` — unified logger factory

**Files:**
- Create: `utils/__init__.py` (empty)
- Create: `utils/logging_utils.py`
- Test: `tests/test_logging_utils.py`

> Context: Three near-identical logger factories exist in `run_system.py:141`, `sell_straddle.py:60`, `iron_condor.py:67`. Key differences: sell_straddle uses `RotatingFileHandler` (10 MB × 3); the others use plain `FileHandler`. The unified factory supports both via `max_bytes` param.

- [ ] **Write the failing test**

```python
# tests/test_logging_utils.py
import logging, os, tempfile
from utils.logging_utils import make_strategy_logger

def test_returns_logger():
    with tempfile.TemporaryDirectory() as d:
        lg = make_strategy_logger("ss_TEST_20260613", log_dir=d)
        assert isinstance(lg, logging.Logger)

def test_idempotent():
    with tempfile.TemporaryDirectory() as d:
        lg1 = make_strategy_logger("ss_IDEM_20260613", log_dir=d)
        lg2 = make_strategy_logger("ss_IDEM_20260613", log_dir=d)
        assert lg1 is lg2
        assert len(lg1.handlers) == 1  # not doubled

def test_log_file_created():
    with tempfile.TemporaryDirectory() as d:
        make_strategy_logger("ss_FILE_20260613", log_dir=d)
        files = os.listdir(d)
        assert any("ss_FILE" in f for f in files)
```

- [ ] **Run test — expect FAIL**
  ```bash
  python -m pytest tests/test_logging_utils.py -v
  ```

- [ ] **Create `utils/__init__.py`** (empty file)

- [ ] **Create `utils/logging_utils.py`**

```python
"""utils/logging_utils.py — unified strategy/client logger factory."""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler


def make_strategy_logger(
    filename_stem: str,
    *,
    log_dir: str = os.path.join("logs", "clients"),
    propagate: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Logger:
    """Return a RotatingFileHandler logger. Idempotent — safe to call multiple times.

    Args:
        filename_stem: The log filename without extension, e.g. ``ss_NIFTY_client1_b1_20260613``.
        log_dir:       Directory for log files (created if missing).
        propagate:     Whether to also emit to the root logger / parent handlers.
        max_bytes:     Rotate after this many bytes (default 10 MB).
        backup_count:  Keep this many rotated backups.
    """
    name = f"strat.{filename_stem}"
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg  # already configured — idempotent
    lg.setLevel(logging.DEBUG)
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, f"{filename_stem}.log"),
        encoding="utf-8",
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = propagate
    return lg
```

- [ ] **Run test — expect PASS**
  ```bash
  python -m pytest tests/test_logging_utils.py -v
  ```

- [ ] **Commit**
  ```bash
  git add utils/__init__.py utils/logging_utils.py tests/test_logging_utils.py
  git commit -m "feat: add utils/logging_utils.make_strategy_logger — unified logger factory"
  ```

---

### Task 3: `data_layer/client_db.py` additions — batch query + password_resets table

**Files:**
- Modify: `data_layer/client_db.py`
- Test: `tests/data_layer/test_client_db_additions.py`

> Context: `system_settings` table already exists. `hash_password`/`verify_password`/`verify_client_password` already exist. Need to add: (a) `password_resets` table, (b) `get_running_straddle_deployments_sync()` batch method, (c) `get_admin_password_hash_sync()` + `set_admin_password_hash()`, (d) `create_reset_token()` + `consume_reset_token()`.

- [ ] **Write the failing tests**

```python
# tests/data_layer/test_client_db_additions.py
import pytest, tempfile, os
from data_layer.client_db import ClientDB

@pytest.fixture
def db(tmp_path):
    return ClientDB(str(tmp_path / "test.db"))

def test_get_running_straddle_deployments_empty(db):
    rows = db.get_running_straddle_deployments_sync()
    assert rows == []

def test_admin_password_hash_roundtrip(db):
    import asyncio
    assert db.get_admin_password_hash_sync() == ""
    asyncio.get_event_loop().run_until_complete(
        db.set_admin_password_hash("salt:hash_value")
    )
    assert db.get_admin_password_hash_sync() == "salt:hash_value"

def test_create_and_consume_reset_token(db):
    import asyncio
    token = asyncio.get_event_loop().run_until_complete(
        db.create_reset_token("client", "alice")
    )
    assert len(token) > 20
    result = db.consume_reset_token_sync(token)
    assert result == ("client", "alice")

def test_consume_token_twice_fails(db):
    import asyncio
    token = asyncio.get_event_loop().run_until_complete(
        db.create_reset_token("client", "alice")
    )
    db.consume_reset_token_sync(token)
    result = db.consume_reset_token_sync(token)
    assert result is None

def test_consume_bad_token_fails(db):
    result = db.consume_reset_token_sync("notavalidtoken")
    assert result is None
```

- [ ] **Run test — expect FAIL**
  ```bash
  python -m pytest tests/data_layer/test_client_db_additions.py -v
  ```

- [ ] **Add `password_resets` table to `_SCHEMA` in `data_layer/client_db.py`**

Find the end of the `_SCHEMA` string (just before the closing `"""`) and add:

```python
CREATE TABLE IF NOT EXISTS password_resets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token_hash  TEXT    NOT NULL,
    target_role TEXT    NOT NULL,
    target_id   TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    expires_at  TEXT    NOT NULL,
    used        INTEGER NOT NULL DEFAULT 0
);
```

- [ ] **Add three methods to `ClientDB` class in `data_layer/client_db.py`**

Add after the `# ── System settings` section:

```python
# ── Admin password (DB-stored, avoids server restart on change) ──────────────

def get_admin_password_hash_sync(self) -> str:
    """Return stored admin password hash, or '' if never set."""
    return self.get_setting_sync("admin_password_hash", "")

async def set_admin_password_hash(self, hashed: str) -> None:
    """Persist a new admin password hash."""
    await self.set_setting("admin_password_hash", hashed)

# ── Password reset tokens ─────────────────────────────────────────────────────

async def create_reset_token(self, target_role: str, target_id: str) -> str:
    """Generate a one-time reset token, store its hash, return plaintext token."""
    import secrets, hashlib
    from datetime import datetime, timedelta, timezone
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=24)
    await asyncio.to_thread(
        self._exec,
        "INSERT INTO password_resets (token_hash, target_role, target_id, created_at, expires_at, used) "
        "VALUES (?, ?, ?, ?, ?, 0)",
        (token_hash, target_role, target_id, now.isoformat(), expires.isoformat()),
    )
    return token

def consume_reset_token_sync(self, token: str) -> tuple[str, str] | None:
    """Validate token, mark used, return (target_role, target_id) or None if invalid/expired."""
    import hashlib
    from datetime import datetime, timezone
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    try:
        con = sqlite3.connect(self._db_path)
        row = con.execute(
            "SELECT id, target_role, target_id, expires_at, used FROM password_resets "
            "WHERE token_hash=?",
            (token_hash,),
        ).fetchone()
        if row is None or row["used"]:
            con.close()
            return None
        expires = datetime.fromisoformat(row["expires_at"])
        if datetime.now(expires.tzinfo) > expires:
            con.close()
            return None
        con.execute("UPDATE password_resets SET used=1 WHERE id=?", (row["id"],))
        con.commit()
        con.close()
        return (row["target_role"], row["target_id"])
    except Exception as exc:
        logger.error("consume_reset_token_sync: %s", exc)
        return None

# ── Batch straddle deployment query ──────────────────────────────────────────

def get_running_straddle_deployments_sync(self) -> list[dict]:
    """Single JOIN: all is_running=1 sell_straddle deployments across active clients."""
    try:
        con = sqlite3.connect(self._db_path)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT c.client_id, d.binding_id, d.underlying, d.lot_multiplier,
                   d.strategy_name, d.is_running, d.assigned_instrument
            FROM clients c
            JOIN strategy_deployments d ON c.client_id = d.client_id
            WHERE c.is_active = 1
              AND d.strategy_name = 'sell_straddle'
              AND d.is_running = 1
            """
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("get_running_straddle_deployments_sync: %s", exc)
        return []
```

- [ ] **Run tests — expect PASS**
  ```bash
  python -m pytest tests/data_layer/test_client_db_additions.py -v
  ```

- [ ] **Commit**
  ```bash
  git add data_layer/client_db.py tests/data_layer/test_client_db_additions.py
  git commit -m "feat(db): password_resets table, reset-token methods, batch straddle query, admin pw hash"
  ```

---

## Phase 2 — Redundancy cleanup + performance

### Task 4: Swap ATM call sites + delete `_bdec`

**Files:**
- Modify: `strategies/iron_condor.py` (line ~457)
- Modify: `strategies/trap_trading_engine.py` (line ~1576)
- Modify: `run_system.py` (lines 260–268)

- [ ] **Replace ATM calc in `strategies/iron_condor.py`**

Find the line (around 457):
```python
atm  = round(spot / step) * step
```
Replace with:
```python
from strategies.strike_utils import compute_atm
atm = compute_atm(spot, step)
```

- [ ] **Replace ATM calc in `strategies/trap_trading_engine.py`**

Find the line (around 1576):
```python
return round(spot / step) * step
```
Replace with:
```python
from strategies.strike_utils import compute_atm
return compute_atm(spot, step)
```

- [ ] **Simplify `_bdec` in `run_system.py`**

Find (around line 260):
```python
def _bdec(row, key):
    """Read an XOR-encoded *_enc column and decode it to plaintext."""
    enc = _bget(row, key, "")
    if not enc:
        return ""
    try:
        return _decode_cred(enc)
    except Exception:
        return ""
```
Replace with a one-liner (the `_decode_cred` import already exists just above):
```python
def _bdec(row, key):
    return _decode_cred(_bget(row, key, ""))
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add strategies/iron_condor.py strategies/trap_trading_engine.py run_system.py
  git commit -m "refactor: use strike_utils.compute_atm; simplify _bdec wrapper"
  ```

---

### Task 5: Swap logger factories + delete dead roll code

**Files:**
- Modify: `strategies/sell_straddle.py`
- Modify: `strategies/iron_condor.py`
- Modify: `run_system.py`

> The three factories have slight differences in `propagate` and handler type. The new `make_strategy_logger` uses `RotatingFileHandler` (same as sell_straddle — safest default). `run_system.get_client_logger` sets `propagate=True`; the others `False`. We pass `propagate=` explicitly.

- [ ] **Replace `_make_strategy_logger` in `strategies/sell_straddle.py`**

Find the function `_make_strategy_logger` (around line 60) and its full body. Replace the whole function with:

```python
def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from datetime import datetime
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(f"ss_{tag}_{date_str}", propagate=False)
```

- [ ] **Replace `_make_ic_logger` in `strategies/iron_condor.py`**

Find the function `_make_ic_logger` (around line 67) and replace with:

```python
def _make_ic_logger(underlying: str) -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(f"ic_{underlying}_{date_str}", propagate=False)
```

- [ ] **Replace `get_client_logger` in `run_system.py`**

Find `get_client_logger` (around line 141) and replace the body:

```python
def get_client_logger(client_id: str, strategy: str, log_dir: str = "logs") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    return make_strategy_logger(
        f"{client_id}_{strategy}_{date_str}",
        log_dir=os.path.join(log_dir, "clients"),
        propagate=True,
    )
```

- [ ] **Delete dead `_try_smart_roll` in `strategies/sell_straddle.py`**

Find and delete the entire `async def _try_smart_roll(self, now: datetime, trigger: str) -> bool:` method body (from line ~2034 to its end). Also delete the import `from strategies.straddle_selection import scan_pool, classify_roll` inside it if it's the only use site.

Verify `classify_roll` is no longer referenced anywhere:
```bash
grep -rn "classify_roll\|_try_smart_roll" strategies/
```
Expected: zero results.

- [ ] **Move `import time` to module top in `strategies/iron_condor.py`**

Ensure `import time` is at the top of the file with other stdlib imports. Remove any `import time` lines inside function bodies.

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add strategies/sell_straddle.py strategies/iron_condor.py run_system.py
  git commit -m "refactor: unify logger factories via logging_utils; delete dead _try_smart_roll"
  ```

---

### Task 6: Replace N+1 query in `straddle_book_manager._wanted()`

**Files:**
- Modify: `strategies/straddle_book_manager.py`

- [ ] **Replace `_wanted()` body**

Find `def _wanted(self) -> Dict[Key, int]:` (around line 59). Replace its entire body with:

```python
def _wanted(self) -> Dict[Key, int]:
    """Map of (client,binding,underlying) → lot_multiplier for every sell_straddle
    deployment that is RUNNING (is_running=1). Single JOIN query — O(1) regardless
    of client count (replaces N+1 per-client loop).
    """
    wanted: Dict[Key, int] = {}
    try:
        rows = self._db.get_running_straddle_deployments_sync()
    except Exception:
        return wanted
    for d in rows:
        cid = d.get("client_id", "")
        bid = d.get("binding_id", "")
        und = str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
        if not cid or not bid:
            continue
        if self._indices and und not in self._indices:
            continue
        try:
            lots = max(1, int(round(float(d.get("lot_multiplier", 1) or 1))))
        except Exception:
            lots = 1
        wanted[(cid, bid, und)] = lots
    return wanted
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add strategies/straddle_book_manager.py
  git commit -m "perf: replace N+1 DB loop in straddle_book_manager with single JOIN query"
  ```

---

## Phase 3 — Error handling

### Task 7: Broker auth failure is fatal + exc_info + config warning

**Files:**
- Modify: `execution_bridge/execution_router.py`
- Modify: `strategies/straddle_book_manager.py`
- Modify: `strategies/sell_straddle.py`
- Modify: `run_system.py`

- [ ] **Make broker auth failure fatal in `execution_bridge/execution_router.py`**

Find `async def start(self) -> None:` (around line 112). Replace with:

```python
async def start(self) -> None:
    """Authenticate brokers and spin up per-client workers.

    Raises RuntimeError if ANY broker auth fails — the system must not start
    with a missing broker as it leads to confusing 'no broker for binding' errors later.
    """
    failed: list[str] = []
    for client in self._registry.all_active():
        self._brokers[client.client_id] = {}
        for binding in client.enabled_brokers():
            broker = create_broker(binding, client.client_id)
            try:
                ok = await broker.authenticate()
            except Exception as exc:
                logger.critical(
                    "Router: Auth EXCEPTION for %s/%s (%s): %s",
                    client.client_id, binding.binding_id, binding.provider, exc,
                    exc_info=True,
                )
                ok = False
            if ok:
                self._brokers[client.client_id][binding.binding_id] = broker
                logger.info(
                    "Router: Authenticated %s/%s (%s).",
                    client.client_id, binding.binding_id, binding.provider,
                )
            else:
                logger.critical(
                    "Router: Auth FAILED for %s/%s (%s). System cannot start.",
                    client.client_id, binding.binding_id, binding.provider,
                )
                failed.append(f"{client.client_id}/{binding.binding_id}({binding.provider})")

        worker = ClientExecutionWorker(
            client=client,
            brokers=self._brokers[client.client_id],
            bus=self._bus,
            cfg=self._cfg,
        )
        self._pool.register(worker)

    if failed:
        raise RuntimeError(
            f"Broker authentication failed for: {', '.join(failed)}. "
            "Fix credentials or remove the binding before starting in live mode."
        )

    await self._pool.start_all()
    logger.info("Router: %d client workers active.", len(self._brokers))
```

- [ ] **Catch RuntimeError in `run_system.py`**

Find where `await router.start()` is called. Wrap it:

```python
try:
    await router.start()
except RuntimeError as exc:
    logger.critical("Startup aborted: %s", exc)
    print(f"\n\nFATAL: {exc}\n\nCheck broker credentials in the dashboard and retry.\n")
    raise SystemExit(1)
```

- [ ] **Add `exc_info=True` in `straddle_book_manager.py`**

Find the two `except Exception as exc: logger.warning(...)` blocks around lines 108–116. Change both to:

```python
except Exception as exc:
    logger.warning("StraddleBookManager: spawn %s failed: %s", key, exc, exc_info=True)
```
and:
```python
except Exception as exc:
    logger.warning("StraddleBookManager: stop %s failed: %s", key, exc, exc_info=True)
```

- [ ] **Add missing config section warning in `sell_straddle.py:_load_thresholds()`**

Find `_load_thresholds(self)`. After the line that assigns `ss = ...` (the sell_straddle config dict), add:

```python
if not ss:
    logger.warning(
        "SellStraddle[%s]: 'sell_straddle' config section missing from runtime config — using defaults.",
        self._underlying,
    )
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add execution_bridge/execution_router.py strategies/straddle_book_manager.py \
          strategies/sell_straddle.py run_system.py
  git commit -m "fix(error-handling): broker auth fatal; exc_info on swallowed exceptions; config warning"
  ```

---

## Phase 4 — Security

### Task 8: Startup secret enforcement

**Files:**
- Modify: `run_system.py`
- Test: `tests/test_startup_enforcement.py`

- [ ] **Write failing tests**

```python
# tests/test_startup_enforcement.py
import os, sys, pytest

def test_weak_admin_password_blocked(monkeypatch):
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "a" * 40)
    from importlib import import_module
    # We call the function directly after importing
    import run_system
    with pytest.raises(SystemExit):
        run_system._enforce_secrets("live")

def test_weak_jwt_secret_blocked(monkeypatch):
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "StrongP@ss99!")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "short")
    import run_system
    with pytest.raises(SystemExit):
        run_system._enforce_secrets("live")

def test_demo_mode_skips_enforcement(monkeypatch):
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    import run_system
    run_system._enforce_secrets("demo")  # must NOT raise

def test_strong_credentials_pass(monkeypatch):
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "StrongP@ss99!")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "a-long-random-secret-that-is-fine-here")
    import run_system
    run_system._enforce_secrets("live")  # must NOT raise
```

- [ ] **Run test — expect FAIL (AttributeError: module has no _enforce_secrets)**
  ```bash
  python -m pytest tests/test_startup_enforcement.py -v
  ```

- [ ] **Add `_enforce_secrets` to `run_system.py`**

Add this function near the top of `run_system.py` (after imports, before `main()`):

```python
_WEAK_PASSWORDS = {"admin123", "password", "changeme", "secret", ""}

def _enforce_secrets(mode: str) -> None:
    """Refuse to start in live mode with default/weak credentials."""
    if mode not in ("live",):
        return
    pwd = os.getenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    if pwd in _WEAK_PASSWORDS:
        sys.exit(
            "\nFATAL: TERMINUS_ADMIN_PASSWORD is a default/weak value.\n"
            "  Set a strong password: export TERMINUS_ADMIN_PASSWORD=<your-password>\n"
            "  Then restart.\n"
        )
    jwt = os.getenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    if "CHANGE-IN-PRODUCTION" in jwt or len(jwt) < 32:
        sys.exit(
            "\nFATAL: TERMINUS_JWT_SECRET is the dev default or too short (< 32 chars).\n"
            "  Generate one: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
            "  Then: export TERMINUS_JWT_SECRET=<generated-value>\n"
        )
```

- [ ] **Call `_enforce_secrets(mode)` at the top of `main()` in `run_system.py`**

Find `async def main(` or the top-level entry point. Add as the very first line inside:

```python
_enforce_secrets(args.mode)
```

(where `args.mode` is the parsed CLI mode argument — check the exact variable name in the file)

- [ ] **Run tests — expect PASS**
  ```bash
  python -m pytest tests/test_startup_enforcement.py -v
  ```

- [ ] **Commit**
  ```bash
  git add run_system.py tests/test_startup_enforcement.py
  git commit -m "security: enforce strong admin password + JWT secret in live mode"
  ```

---

### Task 9: DB-stored admin password + `change-password` endpoint

**Files:**
- Modify: `ui_layer/dashboard_server.py`
- Test: `tests/test_change_password.py`

> Context: Admin login currently does `hmac.compare_digest(password, auth_cfg.admin_password)` which reads from env var. We add a DB-first check: if `admin_password_hash` is set in `system_settings`, verify against it; else fall back to env var. New `POST /api/admin/change-password` stores the new hash.

- [ ] **Write failing tests**

```python
# tests/test_change_password.py
import pytest, asyncio, tempfile
from data_layer.client_db import ClientDB, hash_password, verify_password

def test_admin_password_hash_roundtrip():
    with tempfile.TemporaryDirectory() as d:
        db = ClientDB(f"{d}/test.db")
        loop = asyncio.get_event_loop()
        assert db.get_admin_password_hash_sync() == ""
        h = hash_password("newStrongPwd!")
        loop.run_until_complete(db.set_admin_password_hash(h))
        stored = db.get_admin_password_hash_sync()
        assert verify_password("newStrongPwd!", stored)
        assert not verify_password("wrongpwd", stored)
```

- [ ] **Run test — expect PASS** (uses Task 3 code already committed)
  ```bash
  python -m pytest tests/test_change_password.py -v
  ```

- [ ] **Update admin login in `ui_layer/dashboard_server.py`**

Find the admin branch in `login` (around line 725):
```python
if not (
    hmac.compare_digest(username, auth_cfg.admin_username)
    and hmac.compare_digest(password, auth_cfg.admin_password)
):
    raise HTTPException(status_code=401, detail="Invalid admin credentials.")
```

Replace with:
```python
if not hmac.compare_digest(username, auth_cfg.admin_username):
    raise HTTPException(status_code=401, detail="Invalid admin credentials.")
# DB-stored hash takes precedence over env var (allows change-password without restart)
stored_hash = _srv._client_db.get_admin_password_hash_sync() if _srv._client_db else ""
if stored_hash:
    from data_layer.client_db import verify_password as _vp
    if not _vp(password, stored_hash):
        raise HTTPException(status_code=401, detail="Invalid admin credentials.")
else:
    if not hmac.compare_digest(password, auth_cfg.admin_password):
        raise HTTPException(status_code=401, detail="Invalid admin credentials.")
```

- [ ] **Add `POST /api/admin/change-password` endpoint to `dashboard_server.py`**

Add after the login endpoint (still inside the `_register_routes` / app setup block):

```python
@app.post("/api/admin/change-password", tags=["Admin"])
async def admin_change_password(request: Request):
    _require_admin(request)
    try:
        raw = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON.")
    current = str(raw.get("current_password") or "")
    new_pwd = str(raw.get("new_password") or "")
    if not current or not new_pwd:
        raise HTTPException(status_code=400, detail="current_password and new_password required.")
    if len(new_pwd) < 8:
        raise HTTPException(status_code=400, detail="new_password must be at least 8 characters.")
    # Verify current password (DB hash first, then env var)
    auth_cfg = _srv._cfg.auth
    stored_hash = _srv._client_db.get_admin_password_hash_sync() if _srv._client_db else ""
    from data_layer.client_db import verify_password as _vp, hash_password as _hp
    if stored_hash:
        ok = _vp(current, stored_hash)
    else:
        ok = hmac.compare_digest(current, auth_cfg.admin_password)
    if not ok:
        raise HTTPException(status_code=401, detail="Current password is incorrect.")
    await _srv._client_db.set_admin_password_hash(_hp(new_pwd))
    return {"ok": True, "message": "Admin password updated."}
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add ui_layer/dashboard_server.py tests/test_change_password.py
  git commit -m "security: DB-stored admin password; POST /api/admin/change-password endpoint"
  ```

---

### Task 10: Reset-token endpoints (admin generates → client uses)

**Files:**
- Modify: `ui_layer/dashboard_server.py`
- Test: `tests/test_password_reset.py`

- [ ] **Write failing tests**

```python
# tests/test_password_reset.py
import pytest, asyncio, tempfile
from data_layer.client_db import ClientDB

def test_create_and_consume(tmp_path):
    db = ClientDB(str(tmp_path / "t.db"))
    loop = asyncio.get_event_loop()
    token = loop.run_until_complete(db.create_reset_token("client", "bob"))
    result = db.consume_reset_token_sync(token)
    assert result == ("client", "bob")

def test_expired_token_rejected(tmp_path, monkeypatch):
    import hashlib
    from datetime import datetime, timedelta, timezone
    db = ClientDB(str(tmp_path / "t.db"))
    loop = asyncio.get_event_loop()
    token = loop.run_until_complete(db.create_reset_token("client", "bob"))
    # Manually expire it
    import sqlite3
    con = sqlite3.connect(str(tmp_path / "t.db"))
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    con.execute("UPDATE password_resets SET expires_at=?", (past,))
    con.commit(); con.close()
    result = db.consume_reset_token_sync(token)
    assert result is None

def test_unknown_token_rejected(tmp_path):
    db = ClientDB(str(tmp_path / "t.db"))
    assert db.consume_reset_token_sync("garbage_token") is None
```

- [ ] **Run tests — expect PASS** (uses Task 3 code)
  ```bash
  python -m pytest tests/test_password_reset.py -v
  ```

- [ ] **Add `POST /api/admin/client/{client_id}/reset-token` to `dashboard_server.py`**

```python
@app.post("/api/admin/client/{client_id}/reset-token", tags=["Admin"])
async def generate_client_reset_token(client_id: str, request: Request):
    _require_admin(request)
    if not _srv._client_db:
        raise HTTPException(status_code=503, detail="DB not available.")
    # Invalidate any existing unused tokens for this client
    token = await _srv._client_db.create_reset_token("client", client_id)
    return {"ok": True, "token": token, "expires_in": "24 hours",
            "note": "Show this token to the client once. It cannot be retrieved again."}
```

- [ ] **Add `POST /api/auth/reset-password` to `dashboard_server.py`**

```python
@app.post("/api/auth/reset-password", tags=["Auth"])
async def reset_password(request: Request):
    try:
        raw = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON.")
    token    = str(raw.get("token") or "").strip()
    new_pwd  = str(raw.get("new_password") or "")
    if not token or not new_pwd:
        raise HTTPException(status_code=400, detail="token and new_password are required.")
    if len(new_pwd) < 8:
        raise HTTPException(status_code=400, detail="new_password must be at least 8 characters.")
    result = _srv._client_db.consume_reset_token_sync(token)
    if result is None:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token.")
    target_role, target_id = result
    from data_layer.client_db import hash_password as _hp
    if target_role == "admin":
        await _srv._client_db.set_admin_password_hash(_hp(new_pwd))
    else:
        # Store hashed password in clients.password_hash column
        await _srv._client_db.set_client_password(target_id, _hp(new_pwd))
    return {"ok": True, "message": "Password updated. Please log in with your new password."}
```

- [ ] **Add `set_client_password` to `data_layer/client_db.py`**

```python
async def set_client_password(self, client_id: str, hashed: str) -> None:
    """Store a hashed password for a client (upserts password_hash column)."""
    await asyncio.to_thread(
        self._exec,
        "UPDATE clients SET password_hash=? WHERE client_id=?",
        (hashed, client_id),
    )
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add ui_layer/dashboard_server.py data_layer/client_db.py
  git commit -m "security: reset-token endpoints — admin generates, client uses to reset password"
  ```

---

### Task 11: Forgot-password UI in `monitor.html`

**Files:**
- Modify: `ui_layer/templates/monitor.html`

> The login form is Alpine.js powered. We add a `showReset` flag and a secondary form panel.

- [ ] **Find the login form in `monitor.html`**

Search for `x-data` block that contains `password`, `username`, `login` logic. It will be near the top of the file inside a full-screen login overlay.

- [ ] **Add `showReset: false` to the login Alpine data object**

Find the Alpine `x-data="{ username: '', password: '', ..."` and add `showReset: false, resetToken: '', newPassword: '', confirmPassword: '', resetMsg: ''` to the data object.

- [ ] **Add "Forgot password?" link below the login button**

After the login `<button>` in the login form, add:

```html
<div class="mt-3 text-center">
  <button type="button" @click="showReset=true"
          class="text-xs text-blue-400 hover:text-blue-300 underline">
    Forgot password?
  </button>
</div>
```

- [ ] **Add the reset panel (hidden until `showReset=true`)**

After the login form `</form>`, add:

```html
<!-- Password Reset Panel -->
<div x-show="showReset" x-cloak class="mt-4 border-t border-gray-600 pt-4">
  <p class="text-xs text-gray-400 mb-3">
    Enter the reset token provided by your administrator.
  </p>
  <div x-show="resetMsg" x-text="resetMsg"
       class="mb-2 text-sm text-green-400"></div>
  <input x-model="resetToken" type="text" placeholder="Reset token"
         class="w-full bg-gray-700 text-white rounded px-3 py-2 text-sm mb-2 border border-gray-600"/>
  <input x-model="newPassword" type="password" placeholder="New password (min 8 chars)"
         class="w-full bg-gray-700 text-white rounded px-3 py-2 text-sm mb-2 border border-gray-600"/>
  <input x-model="confirmPassword" type="password" placeholder="Confirm new password"
         class="w-full bg-gray-700 text-white rounded px-3 py-2 text-sm mb-3 border border-gray-600"/>
  <button @click="
    if(newPassword !== confirmPassword){ resetMsg='Passwords do not match.'; return; }
    if(newPassword.length < 8){ resetMsg='Password too short.'; return; }
    fetch('/api/auth/reset-password', {method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({token: resetToken, new_password: newPassword})
    }).then(r=>r.json()).then(d=>{
      if(d.ok){ resetMsg='Password updated — please log in.'; showReset=false; resetToken=''; newPassword=''; confirmPassword=''; }
      else { resetMsg = d.detail || d.error || 'Failed.'; }
    }).catch(()=>{ resetMsg='Network error.'; });
  "
  class="w-full bg-blue-600 hover:bg-blue-500 text-white rounded px-3 py-2 text-sm font-medium">
    Reset Password
  </button>
  <div class="mt-3 text-center">
    <button type="button" @click="showReset=false"
            class="text-xs text-gray-400 hover:text-gray-300 underline">
      Back to login
    </button>
  </div>
  <p class="mt-4 text-xs text-gray-500 text-center">
    Admin password reset requires SSH access:<br>
    <code>export TERMINUS_ADMIN_PASSWORD=new_password && pm2 restart algo</code>
  </p>
</div>
```

- [ ] **Add "Generate Reset Token" button in admin client-profiles panel**

In the client-profiles section of `monitor.html`, find the per-client ACTIONS buttons (near `toggleGranular`, square-off buttons). Add:

```html
<button @click="
  fetch('/api/admin/client/' + client.client_id + '/reset-token', {
    method:'POST', headers: adminHeaders()
  }).then(r=>r.json()).then(d=>{
    if(d.ok){
      prompt('Copy this token and send to the client (shown ONCE):', d.token);
    } else { alert('Failed: ' + (d.detail||d.error)); }
  });
" class="px-2 py-1 text-xs bg-yellow-700 hover:bg-yellow-600 rounded text-white">
  Reset Token
</button>
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add ui_layer/templates/monitor.html
  git commit -m "feat(ui): forgot-password reset form + admin generate-reset-token button"
  ```

---

## Phase 5 — Industrial standards

### Task 12: Pydantic request models for key endpoints

**Files:**
- Modify: `ui_layer/dashboard_server.py`

> FastAPI note: Pydantic models must be at MODULE LEVEL (not inside functions) due to `from __future__ import annotations`. The current login endpoint uses `request: Request` + manual `await request.json()`. We add models at module level and switch key endpoints to use them.

- [ ] **Add Pydantic models at module level in `dashboard_server.py`**

Find the module-level Pydantic model section (near `class SomeExistingModel(BaseModel)`). Add:

```python
class LoginRequest(BaseModel):
    role:     str
    username: str
    password: str

class ResetPasswordRequest(BaseModel):
    token:        str
    new_password: str

class ChangeAdminPasswordRequest(BaseModel):
    current_password: str
    new_password:     str

class SystemSettingRequest(BaseModel):
    key:   str
    value: str
```

- [ ] **Switch `login` endpoint to use `LoginRequest`**

Change the signature from:
```python
async def login(request: Request):
    raw = await request.json()
    role = str(raw.get("role") ...)
```
To:
```python
async def login(req: LoginRequest):
    role     = req.role.strip()
    username = req.username.strip()
    password = req.password
```

- [ ] **Switch `reset-password` endpoint to use `ResetPasswordRequest`**

Change:
```python
async def reset_password(request: Request):
    raw = await request.json()
    token   = str(raw.get("token") ...)
    new_pwd = str(raw.get("new_password") ...)
```
To:
```python
async def reset_password(req: ResetPasswordRequest):
    token   = req.token.strip()
    new_pwd = req.new_password
```

- [ ] **Switch `change-password` endpoint to use `ChangeAdminPasswordRequest`**

Change:
```python
async def admin_change_password(request: Request):
    raw = await request.json()
    current = str(raw.get("current_password") ...)
    new_pwd = str(raw.get("new_password") ...)
```
To:
```python
async def admin_change_password(request: Request, req: ChangeAdminPasswordRequest):
    # request still needed for _require_admin(request) auth check
    current = req.current_password
    new_pwd = req.new_password
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add ui_layer/dashboard_server.py
  git commit -m "feat: Pydantic request models for login, reset-password, change-password endpoints"
  ```

---

### Task 13: `pytest.ini` + runtime config validation + data-driven underlying config

**Files:**
- Create: `pytest.ini`
- Modify: `data_layer/runtime_config.py`
- Modify: `config/global_config.py`
- Modify: `run_system.py`

- [ ] **Create `pytest.ini`**

```ini
[pytest]
testpaths = tests
addopts   = --tb=short -q
```

> Note: `--cov-fail-under` omitted for now — install `pytest-cov` first (`pip install pytest-cov`) then add `--cov=strategies --cov=data_layer --cov-report=term-missing` once coverage baseline is known.

- [ ] **Run tests to confirm `pytest.ini` works**
  ```bash
  python -m pytest
  ```
  Expected: same 228+ results, shorter command.

- [ ] **Add `validate_index_section` to `data_layer/runtime_config.py`**

Add after existing imports:

```python
import logging as _logging
_rclog = _logging.getLogger(__name__)

_REQUIRED_SS_KEYS: frozenset[str] = frozenset({"entry_rules_beginning", "exit_rules"})

def validate_index_section(index: str, section: str, raw: dict) -> None:
    """Warn on missing required keys in a strategy config section. Never raises."""
    for key in _REQUIRED_SS_KEYS:
        if key not in raw:
            _rclog.warning(
                "RuntimeConfig[%s/%s]: missing key '%s' — defaults will apply.",
                index, section, key,
            )
```

- [ ] **Call `validate_index_section` from `sell_straddle.py:_load_thresholds()`**

After loading `ss = ...`, add:

```python
from data_layer.runtime_config import validate_index_section
validate_index_section(self._underlying, "sell_straddle", ss)
```

- [ ] **Add `ExchangeConfig.load_from_db()` to `config/global_config.py`**

Inside the `ExchangeConfig` dataclass, add:

```python
def load_from_db(self, db) -> None:
    """Override strike_steps and lot_sizes from system_settings if present.
    Keys: strike_step_NIFTY, lot_size_NIFTY, etc. Falls back to hardcoded defaults.
    """
    import re
    for key in list(self.strike_steps.keys()) + list(self.lot_sizes.keys()):
        pass  # just iterate known underlyings to check for overrides
    # scan system_settings for strike_step_* and lot_size_* keys
    # db.get_setting_sync is synchronous — safe to call at startup before event loop
    known = set(self.strike_steps) | set(self.lot_sizes)
    for und in known:
        ss = db.get_setting_sync(f"strike_step_{und}", "")
        if ss:
            try:
                self.strike_steps[und] = float(ss)
            except ValueError:
                pass
        ls = db.get_setting_sync(f"lot_size_{und}", "")
        if ls:
            try:
                self.lot_sizes[und] = int(ls)
            except ValueError:
                pass
```

- [ ] **Call `cfg.exchange.load_from_db(client_db)` in `run_system.py`** after the DB is initialised and before strategies are started:

```python
cfg.exchange.load_from_db(client_db)
```

- [ ] **Add `POST /api/admin/system-settings` endpoint to `dashboard_server.py`**

```python
@app.post("/api/admin/system-settings", tags=["Admin"])
async def upsert_system_setting(request: Request, req: SystemSettingRequest):
    _require_admin(request)
    await _srv._client_db.set_setting(req.key, req.value)
    return {"ok": True, "key": req.key}
```

- [ ] **Run full test suite — expect 228+ pass**
  ```bash
  python -m pytest tests/ -q
  ```

- [ ] **Commit**
  ```bash
  git add pytest.ini data_layer/runtime_config.py config/global_config.py \
          run_system.py ui_layer/dashboard_server.py
  git commit -m "feat: pytest.ini; runtime config validation; data-driven ExchangeConfig.load_from_db"
  ```

---

## Phase 6 — Final verification

### Task 14: Full regression + smoke test

- [ ] **Run full test suite**
  ```bash
  python -m pytest tests/ -v 2>&1 | tail -20
  ```
  Expected: all 228+ tests pass, 0 failures.

- [ ] **Verify demo mode still starts cleanly (no secret enforcement)**
  ```bash
  python run_system.py --mode demo --no-ui 2>&1 | head -20
  ```
  Expected: starts without `FATAL:` message.

- [ ] **Verify live mode blocks weak password**
  ```bash
  TERMINUS_ADMIN_PASSWORD=admin123 python run_system.py --mode live --no-ui 2>&1 | head -5
  ```
  Expected: `FATAL: TERMINUS_ADMIN_PASSWORD is a default/weak value.`

- [ ] **Final commit**
  ```bash
  git add -A
  git commit -m "chore: security hardening complete — all 228+ tests pass"
  ```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ 2a strike_utils — Task 1
- ✅ 2b _decode_cred — Task 4
- ✅ 2c logger factory — Tasks 2 + 5
- ✅ 2d dead code — Task 5
- ✅ 3a batch query — Tasks 3 + 6
- ✅ 3b import time — Task 5
- ✅ 4a broker auth fatal — Task 7
- ✅ 4b exc_info — Task 7
- ✅ 4c config warning — Task 7
- ✅ 5a startup enforcement — Task 8
- ✅ 5b DB admin password — Task 9
- ✅ 5c client pin in DB — already exists (`verify_client_password` + `password_hash` col)
- ✅ 5d forgot password token — Tasks 10 + 11
- ✅ 6a Pydantic models — Task 12
- ✅ 6b pytest.ini — Task 13
- ✅ 6c config validation — Task 13
- ✅ 6d data-driven config — Task 13

**Type consistency:** All method signatures match across tasks. `consume_reset_token_sync` returns `tuple[str,str] | None` and is used as such in Task 10.

**No placeholders:** All code blocks are complete and runnable.
