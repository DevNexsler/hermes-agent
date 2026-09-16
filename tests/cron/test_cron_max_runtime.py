"""Tests for the cron agent-run wall-clock cap (HERMES_CRON_MAX_RUNTIME /
cron.max_runtime_seconds).

The inactivity watchdog (HERMES_CRON_TIMEOUT) cannot end a run that keeps
retrying failed provider calls, because every retry counts as activity. The
wall-clock cap is the hard bound that does.
"""

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from cron import scheduler  # noqa: E402


class TestResolveMaxRuntime:
    def test_default_is_two_hours(self, monkeypatch):
        monkeypatch.delenv("HERMES_CRON_MAX_RUNTIME", raising=False)
        with patch.object(scheduler, "load_config", return_value={}):
            assert scheduler._resolve_cron_max_runtime_seconds() == 7200.0

    def test_env_overrides_config(self, monkeypatch):
        monkeypatch.setenv("HERMES_CRON_MAX_RUNTIME", "90")
        with patch.object(scheduler, "load_config",
                          return_value={"cron": {"max_runtime_seconds": 30}}):
            assert scheduler._resolve_cron_max_runtime_seconds() == 90.0

    def test_config_value_used(self, monkeypatch):
        monkeypatch.delenv("HERMES_CRON_MAX_RUNTIME", raising=False)
        with patch.object(scheduler, "load_config",
                          return_value={"cron": {"max_runtime_seconds": 300}}):
            assert scheduler._resolve_cron_max_runtime_seconds() == 300.0

    def test_zero_means_unlimited(self, monkeypatch):
        monkeypatch.setenv("HERMES_CRON_MAX_RUNTIME", "0")
        assert scheduler._resolve_cron_max_runtime_seconds() is None
        monkeypatch.delenv("HERMES_CRON_MAX_RUNTIME", raising=False)
        with patch.object(scheduler, "load_config",
                          return_value={"cron": {"max_runtime_seconds": 0}}):
            assert scheduler._resolve_cron_max_runtime_seconds() is None

    def test_invalid_env_falls_through(self, monkeypatch):
        monkeypatch.setenv("HERMES_CRON_MAX_RUNTIME", "soon")
        with patch.object(scheduler, "load_config",
                          return_value={"cron": {"max_runtime_seconds": 45}}):
            assert scheduler._resolve_cron_max_runtime_seconds() == 45.0

    def test_negative_config_falls_to_default(self, monkeypatch):
        monkeypatch.delenv("HERMES_CRON_MAX_RUNTIME", raising=False)
        with patch.object(scheduler, "load_config",
                          return_value={"cron": {"max_runtime_seconds": -5}}):
            assert scheduler._resolve_cron_max_runtime_seconds() == 7200.0
