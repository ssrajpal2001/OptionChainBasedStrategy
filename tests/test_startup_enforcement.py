"""Tests for startup security enforcement (_enforce_secrets)."""

import os
import sys
import pytest


def test_weak_admin_password_blocked(monkeypatch):
    """Weak admin password should block startup in live mode."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "a" * 40)
    # Force reimport to pick up new env vars
    import importlib
    import run_system
    importlib.reload(run_system)
    with pytest.raises(SystemExit):
        run_system._enforce_secrets("live")


def test_weak_jwt_secret_blocked(monkeypatch):
    """Short JWT secret should block startup in live mode."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "StrongP@ss99!")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "short")
    import importlib
    import run_system
    importlib.reload(run_system)
    with pytest.raises(SystemExit):
        run_system._enforce_secrets("live")


def test_dev_default_jwt_secret_blocked(monkeypatch):
    """Dev default JWT secret containing CHANGE-IN-PRODUCTION should block live mode."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "StrongP@ss99!")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    import importlib
    import run_system
    importlib.reload(run_system)
    with pytest.raises(SystemExit):
        run_system._enforce_secrets("live")


def test_demo_mode_skips_enforcement(monkeypatch):
    """Demo mode should skip enforcement and not raise."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION")
    import importlib
    import run_system
    importlib.reload(run_system)
    run_system._enforce_secrets("demo")  # must NOT raise


def test_paper_mode_skips_enforcement(monkeypatch):
    """Paper mode should skip enforcement and not raise."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "admin123")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "short")
    import importlib
    import run_system
    importlib.reload(run_system)
    run_system._enforce_secrets("paper")  # must NOT raise


def test_strong_credentials_pass(monkeypatch):
    """Strong credentials should pass enforcement in live mode."""
    monkeypatch.setenv("TERMINUS_ADMIN_PASSWORD", "StrongP@ss99!")
    monkeypatch.setenv("TERMINUS_JWT_SECRET", "a-long-random-secret-that-is-fine-here-32chars-ok")
    import importlib
    import run_system
    importlib.reload(run_system)
    run_system._enforce_secrets("live")  # must NOT raise
