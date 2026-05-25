"""
config/client_profiles.py — Multi-tenant client registry.

Defines:
  • BrokerBinding  — one broker account attached to one client
  • RiskProfile    — per-client capital, drawdown, and trade limits
  • ClientProfile  — the full client record (info + risk + N broker bindings)
  • ClientRegistry — in-memory store; persisted to JSON on disk

Design principle: the global data feeder is broker-agnostic from a
client perspective.  Execution only touches the broker layer through
the execution_bridge.  ClientManager validates risk before routing.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Dict, List, Literal, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Broker Binding
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BrokerBinding:
    """One broker account attached to one client."""
    binding_id: str                     # Unique within the client; e.g. "shoonya_main"
    provider: Literal["shoonya", "dhan", "fyers", "angelone", "upstox", "mock"]
    label: str = ""                     # Human-readable tag

    # Credentials — stored encrypted at rest in production
    user_id: str = ""
    password: str = ""
    api_key: str = ""
    api_secret: str = ""
    totp_secret: str = ""               # Base32 TOTP seed for 2FA
    vendor_code: str = ""               # Shoonya-specific
    imei: str = ""                      # Shoonya-specific
    client_code: str = ""               # Angel One-specific
    access_token: str = ""              # Override (pre-fetched)

    # Lifecycle / strategy assignment (DB-backed)
    assigned_strategy: str = ""          # Admin-pinned strategy: "A", "B", or "C"
    is_trade_enabled: bool = True        # Client per-broker trade switch
    token_generated_at: str = ""         # IST ISO timestamp of last token refresh
    token_expiry_at: str = ""            # IST ISO timestamp of token expiry

    # Execution params per binding
    lot_multiplier: float = 1.0         # Scale signal lots (e.g. 0.5 = half size)
    enabled: bool = True

    @classmethod
    def from_env(cls, binding_id: str, provider: str, prefix: str) -> "BrokerBinding":
        """Load from environment variables with a given prefix, e.g. CLIENT1_SHOONYA_."""
        return cls(
            binding_id=binding_id,
            provider=provider,                   # type: ignore[arg-type]
            user_id=os.getenv(f"{prefix}USER_ID", ""),
            password=os.getenv(f"{prefix}PASSWORD", ""),
            api_key=os.getenv(f"{prefix}API_KEY", ""),
            api_secret=os.getenv(f"{prefix}API_SECRET", ""),
            totp_secret=os.getenv(f"{prefix}TOTP_SECRET", ""),
            vendor_code=os.getenv(f"{prefix}VENDOR_CODE", ""),
            imei=os.getenv(f"{prefix}IMEI", ""),
            client_code=os.getenv(f"{prefix}CLIENT_CODE", ""),
        )

    def mask(self) -> Dict:
        """Return a copy safe for logging (credentials redacted)."""
        d = asdict(self)
        for k in ("password", "api_key", "api_secret", "totp_secret",
                  "vendor_code", "imei", "access_token"):
            if d.get(k):
                d[k] = "***"
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Risk Profile
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RiskProfile:
    capital: float = 500_000.0                  # INR capital allocation
    max_risk_per_trade_pct: float = 1.0          # % of capital risked per trade
    max_daily_loss_pct: float = 3.0              # Hard halt threshold
    max_open_positions: int = 1
    max_daily_trades: int = 5
    min_risk_reward: float = 2.0                 # Override global if needed
    margin_utilization_limit: float = 0.80       # 80% of available margin

    # Trade-size allocation (used by execution_router multiplier)
    size_multiplier: float = 1.0                 # E.g. 2.0 = double standard lots

    def max_risk_inr(self) -> float:
        return self.capital * self.max_risk_per_trade_pct / 100

    def max_daily_loss_inr(self) -> float:
        return self.capital * self.max_daily_loss_pct / 100

    def validate(self) -> None:
        assert 0 < self.max_risk_per_trade_pct <= 5, "max_risk_per_trade_pct must be 0–5%"
        assert 0 < self.max_daily_loss_pct <= 10, "max_daily_loss_pct must be 0–10%"
        assert self.min_risk_reward >= 1.0, "min_risk_reward must be ≥ 1.0"
        assert self.margin_utilization_limit <= 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Client Profile
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ClientProfile:
    """Full record for one managed trading account."""
    client_id: str                      # Unique identifier, e.g. "C001"
    name: str = ""
    email: str = ""
    phone: str = ""

    risk: RiskProfile = field(default_factory=RiskProfile)
    broker_bindings: List[BrokerBinding] = field(default_factory=list)

    # Enabled strategies (subset of ["A", "B", "C"])
    enabled_strategies: List[str] = field(default_factory=lambda: ["A", "B", "C"])

    # Active expiry preference per client
    expiry_preference: Literal["CURRENT_WEEK", "NEXT_WEEK", "MONTHLY"] = "CURRENT_WEEK"
    moneyness_execution: Literal["ATM", "ITM_1", "OTM_1"] = "ATM"

    active: bool = True
    created_date: str = field(default_factory=lambda: str(date.today()))
    notes: str = ""

    # Lifecycle flags (DB-backed, admin + client controlled)
    is_admin_approved: bool = False      # Set True by admin on approval
    is_client_bot_active: bool = False   # Set by client master toggle
    target_index: str = "NIFTY"         # Client-selected index (NIFTY / BANKNIFTY / FINNIFTY)

    # Runtime state — not persisted
    _daily_pnl: float = field(default=0.0, compare=False, repr=False)
    _daily_trades: int = field(default=0, compare=False, repr=False)
    _halted: bool = field(default=False, compare=False, repr=False)

    def add_broker(self, binding: BrokerBinding) -> None:
        if any(b.binding_id == binding.binding_id for b in self.broker_bindings):
            raise ValueError(f"Binding ID '{binding.binding_id}' already exists for {self.client_id}.")
        self.broker_bindings.append(binding)

    def remove_broker(self, binding_id: str) -> bool:
        before = len(self.broker_bindings)
        self.broker_bindings = [b for b in self.broker_bindings if b.binding_id != binding_id]
        return len(self.broker_bindings) < before

    def enabled_brokers(self) -> List[BrokerBinding]:
        return [b for b in self.broker_bindings if b.enabled]

    def is_tradeable(self) -> bool:
        """Return True if this client can receive new trade signals."""
        if not self.active or self._halted:
            return False
        if not self.is_admin_approved:
            return False
        if not self.is_client_bot_active:
            return False
        if self._daily_trades >= self.risk.max_daily_trades:
            return False
        if self._daily_pnl <= -self.risk.max_daily_loss_inr():
            return False
        return len(self.enabled_brokers()) > 0

    def record_trade(self, pnl: float) -> None:
        self._daily_trades += 1
        self._daily_pnl += pnl

    def halt(self, reason: str = "") -> None:
        self._halted = True

    def resume(self) -> None:
        self._halted = False

    def reset_daily(self) -> None:
        self._daily_pnl = 0.0
        self._daily_trades = 0
        self._halted = False

    def to_dict(self) -> Dict:
        return {
            "client_id": self.client_id,
            "name": self.name,
            "email": self.email,
            "phone": self.phone,
            "risk": asdict(self.risk),
            "broker_bindings": [b.mask() for b in self.broker_bindings],
            "enabled_strategies": self.enabled_strategies,
            "expiry_preference": self.expiry_preference,
            "moneyness_execution": self.moneyness_execution,
            "active": self.active,
            "created_date": self.created_date,
            "notes": self.notes,
            "is_admin_approved": self.is_admin_approved,
            "is_client_bot_active": self.is_client_bot_active,
            "target_index": self.target_index,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Client Registry
# ─────────────────────────────────────────────────────────────────────────────

class ClientRegistry:
    """
    In-memory store for all registered ClientProfiles.

    Persists to a JSON file at `profiles_path`. Credentials are NOT
    written to this file — they must be injected from environment
    variables or a secrets vault at startup.
    """

    DEFAULT_PATH = "config/client_profiles.json"

    def __init__(self, profiles_path: str = DEFAULT_PATH) -> None:
        self._path = profiles_path
        self._clients: Dict[str, ClientProfile] = {}

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def register(self, profile: ClientProfile) -> None:
        if profile.client_id in self._clients:
            raise ValueError(f"Client '{profile.client_id}' already registered.")
        profile.risk.validate()
        self._clients[profile.client_id] = profile

    def update(self, profile: ClientProfile) -> None:
        if profile.client_id not in self._clients:
            raise KeyError(f"Client '{profile.client_id}' not found.")
        profile.risk.validate()
        self._clients[profile.client_id] = profile

    def remove(self, client_id: str) -> bool:
        return self._clients.pop(client_id, None) is not None

    def get(self, client_id: str) -> Optional[ClientProfile]:
        return self._clients.get(client_id)

    def all_active(self) -> List[ClientProfile]:
        return [c for c in self._clients.values() if c.active]

    def tradeable_clients(self) -> List[ClientProfile]:
        return [c for c in self._clients.values() if c.is_tradeable()]

    def count(self) -> int:
        return len(self._clients)

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump([c.to_dict() for c in self._clients.values()], f, indent=2)

    def load_non_sensitive(self) -> None:
        """
        Load profile metadata from disk. Credentials are NOT stored in
        the JSON file — inject them via inject_credentials() after loading.
        """
        if not os.path.exists(self._path):
            return
        with open(self._path, encoding="utf-8") as f:
            records = json.load(f)
        for rec in records:
            risk = RiskProfile(**rec.get("risk", {}))
            profile = ClientProfile(
                client_id=rec["client_id"],
                name=rec.get("name", ""),
                email=rec.get("email", ""),
                phone=rec.get("phone", ""),
                risk=risk,
                enabled_strategies=rec.get("enabled_strategies", ["A", "B", "C"]),
                expiry_preference=rec.get("expiry_preference", "CURRENT_WEEK"),
                moneyness_execution=rec.get("moneyness_execution", "ATM"),
                active=rec.get("active", True),
                created_date=rec.get("created_date", str(date.today())),
                notes=rec.get("notes", ""),
                # Lifecycle flags — existing JSON profiles treated as pre-approved
                is_admin_approved=rec.get("is_admin_approved", True),
                is_client_bot_active=rec.get("is_client_bot_active", False),
                target_index=rec.get("target_index", "NIFTY"),
            )
            self._clients[profile.client_id] = profile

    def add_broker_binding(self, client_id: str, binding: "BrokerBinding") -> None:
        """Add a new BrokerBinding to a client at runtime (live provisioning)."""
        client = self._clients.get(client_id)
        if client is None:
            raise KeyError(f"Client '{client_id}' not found.")
        if any(b.binding_id == binding.binding_id for b in client.broker_bindings):
            raise ValueError(
                f"Binding '{binding.binding_id}' already exists on client '{client_id}'."
            )
        client.broker_bindings.append(binding)

    def inject_credentials(self, client_id: str, binding_id: str, **kwargs: str) -> None:
        """
        Inject live credentials into a binding after loading the profile.
        kwargs: user_id, password, api_key, api_secret, totp_secret, etc.
        """
        client = self._clients.get(client_id)
        if client is None:
            raise KeyError(f"Client '{client_id}' not found.")
        for b in client.broker_bindings:
            if b.binding_id == binding_id:
                for key, val in kwargs.items():
                    if hasattr(b, key):
                        setattr(b, key, val)
                return
        raise KeyError(f"Binding '{binding_id}' not found on client '{client_id}'.")

    def reset_all_daily(self) -> None:
        for c in self._clients.values():
            c.reset_daily()

    def halt_all(self) -> None:
        for c in self._clients.values():
            c.halt("SYSTEM_KILL_SWITCH")

    def summary(self) -> List[Dict]:
        return [c.to_dict() for c in self._clients.values()]


# ─────────────────────────────────────────────────────────────────────────────
# Default Registry — populated at startup by AdminConsole
# ─────────────────────────────────────────────────────────────────────────────

REGISTRY: ClientRegistry = ClientRegistry()
