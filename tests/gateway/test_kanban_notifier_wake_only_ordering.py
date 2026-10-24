"""Wake-only (delivery_mode='wake') push-adapter delivery ordering.

For a push-adapter subscription in wake-only mode the visible text ping is
intentionally skipped (the ``send_passive`` gate), so the wake injection IS
the sole delivery. The cursor must therefore only advance AFTER the wake
succeeds: advancing first and running the wake best-effort afterwards let a
failed wake permanently lose the event — the exact bug class the non-push
(api_server) self-post branch already guards against with rewind/retry.

Residual insight extracted from closed PR #84191 (@MaximCrabbe).
"""

import asyncio

from gateway.config import Platform
from gateway.run import GatewayRunner
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_notify as kbn


class RecordingAdapter:
    """Push-capable adapter (no supports_async_delivery attr => push)."""

    from gateway.platforms.base import BasePlatformAdapter
    extract_local_files = staticmethod(BasePlatformAdapter.extract_local_files)

    def __init__(self):
        self.sent = []
        self.handled = []

    async def send(self, chat_id, text, metadata=None):
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})

    async def handle_message(self, event):
        self.handled.append(event)
        event._gateway_accepted = True


class FailingWakeAdapter(RecordingAdapter):
    """Push adapter whose wake injection (handle_message) always fails."""

    async def handle_message(self, event):
        self.handled.append(event)
        raise RuntimeError("simulated wake failure")


async def _run_one_notifier_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


def _make_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    from tests.gateway.restart_test_helpers import install_native_admission
    install_native_admission(runner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._kanban_sub_fail_counts = {}
    runner._kanban_dispatcher_lock_handle = object()
    return runner


def _make_completed_task(delivery_mode):
    conn = kbc.connect()
    try:
        tid = kb.create_task(
            conn,
            title="wake ordering task",
            assignee="worker",
            session_id="agent:main:telegram:dm:chat-1",
        )
        kbn.add_notify_sub(
            conn,
            task_id=tid,
            platform="telegram",
            chat_id="chat-1",
            chat_type="dm",
            delivery_mode=delivery_mode,
        )
        kb.complete_task(conn, tid, summary="done")
        return tid
    finally:
        conn.close()


def _unseen_terminal_events(tid):
    conn = kbc.connect()
    try:
        _, events = kbn.unseen_events_for_sub(
            conn,
            task_id=tid,
            platform="telegram",
            chat_id="chat-1",
            kinds=["completed", "blocked", "gave_up", "crashed", "timed_out"],
        )
        return events
    finally:
        conn.close()


def _subs(tid):
    conn = kbc.connect()
    try:
        return kbn.list_notify_subs(conn, tid)
    finally:
        conn.close()


def test_wake_only_success_advances_cursor_single_wake(tmp_path, monkeypatch):
    """Wake succeeds: exactly one wake, no text ping, cursor advanced."""
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "wake-ok.db"))
    kb.init_db()
    tid = _make_completed_task("wake")

    adapter = RecordingAdapter()
    runner = _make_runner(adapter)
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert adapter.sent == [], "wake-only mode must not send a text ping"
    assert len(adapter.handled) == 1, "wake injection is the sole delivery"
    assert _unseen_terminal_events(tid) == [], (
        "cursor must advance after a successful wake-only delivery"
    )
    assert runner._kanban_sub_fail_counts == {}


def test_generic_wake_failure_retains_unknown_without_retry(tmp_path, monkeypatch):
    """Semantic change: generic effects are UNKNOWN, never failure-budget retries."""
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "wake-fail.db"))
    kb.init_db()
    tid = _make_completed_task("wake")
    adapter = FailingWakeAdapter()
    runner = _make_runner(adapter)
    for _ in range(13):
        runner._running = True
        asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
    assert len(adapter.handled) == 1
    assert len(_subs(tid)) == 1
    assert 'notification_attention_v1' in _subs(tid)[0]['delivery_metadata']
    assert runner._kanban_sub_fail_counts == {}


def test_notify_wake_generic_failure_retains_ping_and_attention(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "notify-wake.db"))
    kb.init_db()
    tid = _make_completed_task("notify+wake")
    adapter = FailingWakeAdapter()
    runner = _make_runner(adapter)
    for _ in range(2):
        runner._running = True
        asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
    assert len(adapter.sent) == len(adapter.handled) == 1
    sub = _subs(tid)[0]
    assert sub['last_ping_event_id'] > 0 and 'notification_attention_v1' in sub['delivery_metadata']
    assert runner._kanban_sub_fail_counts == {}
    assert 'notification_attention_v1' not in adapter.sent[0]['metadata']


def test_unknown_never_drops_subscription_at_old_failure_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "wake-cap.db"))
    kb.init_db()
    tid = _make_completed_task("wake")
    adapter = FailingWakeAdapter()
    runner = _make_runner(adapter)
    runner._kanban_sub_fail_counts = {(tid, "telegram", "chat-1", ""): 11}
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
    assert len(adapter.handled) == 1 and len(_subs(tid)) == 1
    assert list(runner._kanban_sub_fail_counts.values()) == [11]
    assert 'notification_attention_v1' in _subs(tid)[0]['delivery_metadata']
