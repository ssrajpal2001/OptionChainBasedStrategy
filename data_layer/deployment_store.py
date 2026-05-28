"""
data_layer/deployment_store.py — Strategy deployment persistence.

Dual-layer storage:
  1. SQLite (via ClientDB) — normalized, queryable, recoverable
  2. JSON files — human-readable, portable, editable for weekend backtests

File layout:
  data/deployments/{client_id}_{binding_id}_{strategy_name}.json

JSON structure matches the DB record exactly so backtester can load
deployments without touching the database at all.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.global_config import IST

logger = logging.getLogger(__name__)

_DEPLOY_DIR = Path("data/deployments")


def _ensure_dir() -> None:
    _DEPLOY_DIR.mkdir(parents=True, exist_ok=True)


def _json_path(deploy_id: str) -> Path:
    return _DEPLOY_DIR / f"{deploy_id}.json"


# ── Save ─────────────────────────────────────────────────────────────────────

def save_deployment_json(
    deploy_id:      str,
    client_id:      str,
    binding_id:     str,
    strategy_name:  str,
    underlying:     str,
    lot_multiplier: float,
    max_profit_rs:  float,
    max_sl_rs:      float,
    squareoff_time: str,
) -> None:
    """Write deployment config to JSON file alongside the SQLite record."""
    _ensure_dir()
    payload = {
        "deploy_id":      deploy_id,
        "client_id":      client_id,
        "binding_id":     binding_id,
        "strategy_name":  strategy_name,
        "underlying":     underlying,
        "lot_multiplier": lot_multiplier,
        "max_profit_rs":  max_profit_rs,
        "max_sl_rs":      max_sl_rs,
        "squareoff_time": squareoff_time,
        "is_active":      True,
        "saved_at":       datetime.now(IST).isoformat(),
    }
    path = _json_path(deploy_id)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "DeploymentStore: saved JSON → %s  [%s/%s %s lots=%.1f P=%.0f SL=%.0f sq=%s]",
        path.name, client_id, binding_id, strategy_name,
        lot_multiplier, max_profit_rs, max_sl_rs, squareoff_time,
    )


def load_deployment_json(deploy_id: str) -> Optional[Dict[str, Any]]:
    """Load a single deployment config from JSON."""
    path = _json_path(deploy_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("DeploymentStore: failed to read %s — %s", path, exc)
        return None


def delete_deployment_json(deploy_id: str) -> None:
    """Soft-delete: mark is_active=false in JSON but keep file for audit."""
    path = _json_path(deploy_id)
    if not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data["is_active"]   = False
        data["deleted_at"]  = datetime.now(IST).isoformat()
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("DeploymentStore: soft-deleted %s", deploy_id)
    except Exception as exc:
        logger.error("DeploymentStore: delete failed for %s — %s", deploy_id, exc)


def list_deployments_json(client_id: str) -> List[Dict[str, Any]]:
    """Return all active JSON deployments for a client (used by backtester)."""
    _ensure_dir()
    results = []
    for path in _DEPLOY_DIR.glob(f"{client_id}_*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("is_active", True):
                results.append(data)
        except Exception:
            pass
    return sorted(results, key=lambda x: x.get("saved_at", ""))


def apply_deployment_to_runtime_config(deploy: Dict[str, Any]) -> None:
    """
    Push deployment thresholds into RuntimeConfig so running strategy
    immediately picks up the new lot size, profit target and SL.
    Called by the engine hot-reload on engine-start.
    """
    from data_layer.runtime_config import RuntimeConfig

    strategy = deploy.get("strategy_name", "sell_straddle")
    underlying = deploy.get("underlying", "NIFTY")
    patch: Dict[str, Any] = {}

    if deploy.get("lot_multiplier"):
        patch["lot_multiplier"] = float(deploy["lot_multiplier"])
    if deploy.get("squareoff_time"):
        patch["squareoff_time"] = str(deploy["squareoff_time"])
    if deploy.get("max_profit_rs") and float(deploy["max_profit_rs"]) > 0:
        patch["capital_deployed_inr"] = float(deploy["max_profit_rs"]) * 100 / 30  # rough
    if deploy.get("max_sl_rs") and float(deploy["max_sl_rs"]) > 0:
        patch["max_sl_rs"] = float(deploy["max_sl_rs"])

    if patch:
        RuntimeConfig.update({"indices": {underlying: {strategy: patch}}})
        logger.info(
            "DeploymentStore: applied to RuntimeConfig — %s/%s %s",
            underlying, strategy, patch,
        )
