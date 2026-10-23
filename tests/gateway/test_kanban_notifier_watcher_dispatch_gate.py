"""Notifier polling has an independent gateway config gate."""

import asyncio
from unittest.mock import MagicMock, patch

from gateway.config import Platform
from gateway.run import GatewayRunner


def _make_runner(with_adapter=False):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: MagicMock()} if with_adapter else {}
    runner._kanban_sub_fail_counts = {}
    return runner


def test_notifier_watcher_skips_when_notifications_disabled():
    runner = _make_runner(with_adapter=True)

    with patch(
        "hermes_cli.config.load_config",
        return_value={"kanban": {"notify_in_gateway": False}},
    ):
        with patch("hermes_cli.kanban_db.list_boards") as list_boards:
            asyncio.run(runner._kanban_notifier_watcher())

    list_boards.assert_not_called()


def test_notifier_watcher_polls_without_dispatch_ownership(monkeypatch):
    """A profile gateway still polls its profile-owned subscriptions."""
    runner = _make_runner(with_adapter=True)
    past_gate = []
    import hermes_cli.kanban_db as _kb

    with patch(
        "hermes_cli.config.load_config",
        return_value={
            "kanban": {
                "dispatch_in_gateway": False,
                "notify_in_gateway": True,
            }
        },
    ):
        with patch.object(
            _kb, "list_boards",
            side_effect=lambda *a, **kw: past_gate.append(True) or [],
        ):
            from tests.gateway.restart_test_helpers import run_one_notifier_tick
            asyncio.run(run_one_notifier_tick(runner, monkeypatch))

    assert past_gate, (
        "gateways without the dispatch lock must still poll owned subscriptions"
    )
