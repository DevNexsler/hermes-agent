"""Trusted wakes cross the real adapter busy boundary, not external-user auth."""
import asyncio

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from gateway.wake import WakeNotAccepted, deliver_wake
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc


class WakeAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)
        self.sent = []

    @property
    def name(self):
        return "telegram"

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(success=True)

    async def get_chat_info(self, chat_id):
        return {"id": chat_id, "type": "private"}


async def unused_handler(event):
    raise AssertionError("busy event must not start a concurrent turn")


@pytest.mark.asyncio
@pytest.mark.parametrize("human_pending", [False, True])
async def test_completed_board_wake_is_deferred_without_changing_durable_result_or_human_input(tmp_path, monkeypatch, human_pending):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "board.db"))
    kb.init_db()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="wake receipt", assignee="worker")
        kb.complete_task(conn, tid, summary="completed result")
        task = kb.get_task(conn, tid)
    assert task is not None and task.status == "done"
    text = f"[Kanban task completed] {task.result}"
    adapter, runner, source, key = busy_gateway()
    human = MessageEvent(text="human follow-up", message_type=MessageType.TEXT, source=source)
    if human_pending:
        adapter._pending_messages[key] = human
    assert runner._is_user_authorized(source) is False
    with pytest.raises(WakeNotAccepted):
        await deliver_wake(adapter, source=source, text=text)
    if human_pending:
        assert adapter._pending_messages[key] is human
        assert human.text == "human follow-up"
    else:
        assert key not in adapter._pending_messages
    assert not runner._overflow_queue(key)
    # Explicitly unaccepted admission leaves the existing durable producer's result intact.
    with kbc.connect() as conn:
        retained = kb.get_task(conn, tid)
    assert retained.status == "done" and retained.result == task.result
    assert not adapter.sent
    assert not adapter._active_sessions[key].is_set()


def busy_gateway():
    adapter = WakeAdapter()
    runner = object.__new__(GatewayRunner)
    from tests.gateway.restart_test_helpers import install_native_admission
    install_native_admission(runner)
    adapter.gateway_runner = runner
    runner.config = GatewayConfig()
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._draining = False
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_type="dm")
    key = adapter._event_session_key(MessageEvent(text="", message_type=MessageType.TEXT, source=source))
    adapter.set_message_handler(unused_handler)
    adapter.set_busy_session_handler(runner._handle_active_session_busy_message)
    adapter._active_sessions[key] = asyncio.Event()
    return adapter, runner, source, key


@pytest.mark.asyncio
async def test_identityless_external_input_is_still_rejected():
    adapter, runner, source, key = busy_gateway()
    for _ in range(2):
        await adapter.handle_message(MessageEvent(text="external", message_type=MessageType.TEXT, source=source))
    assert key not in adapter._pending_messages
    assert not runner._overflow_queue(key)
    assert not adapter.sent


@pytest.mark.asyncio
async def test_busy_board_notification_retains_exact_durable_obligation(tmp_path, monkeypatch):
    from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
    from hermes_cli import kanban_db_notify as kbn
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "board.db"))
    adapter, runner, source, key = busy_gateway()
    runner._delivery_adapter_for = lambda source: adapter
    runner._kanban_dispatcher_lock_handle = object()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title="owned durable wake", assignee="worker", session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="123",
                          chat_type="dm", delivery_mode="wake")
        kb.complete_task(conn, tid, summary="exact completed result")
        original = kb.get_task(conn, tid)
        cursor, events = kbn.unseen_events_for_sub(conn, task_id=tid, platform="telegram", chat_id="123")
        event_ids = [event.id for event in events]
    human = MessageEvent(text="human follow-up", message_type=MessageType.TEXT, source=source)
    adapter._pending_messages[key] = human
    failures = {}
    for _ in range(2):
        deliveries = await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=False, gc_retention_days=30)
        assert len(deliveries) == 1
        await _KanbanNotification(runner, deliveries[0], platform_cls=Platform,
                                  sub_fail_counts=failures).deliver()
        with kbc.connect() as conn:
            retained = kb.get_task(conn, tid)
            after_cursor, retained_events = kbn.unseen_events_for_sub(
                conn, task_id=tid, platform="telegram", chat_id="123")
            assert retained == original
            assert after_cursor == cursor and [event.id for event in retained_events] == event_ids
            assert len(kbn.list_notify_subs(conn, tid)) == 1
        assert adapter._pending_messages[key] is human
        assert not runner._overflow_queue(key) and not adapter.sent and not failures
