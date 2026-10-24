"""Related nested effect history and atomic archived-notification ownership."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from gateway.admission import (GatewayDraining, adopt_ingress_task, gateway_ingress,
                               native_producer_ingress, refuse_queued_ingress)
from gateway.config import Platform
from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from tests.gateway.test_kanban_wake_acceptance import setup_route


class SyntheticBaseException(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['nested', 'adopted_child'])
@pytest.mark.parametrize('outcome', ['False', 'generic', 'typed', 'base_exception', 'cancel'])
async def test_related_nested_effect_never_restores_parent_no_effect_proof(path, outcome):
    runner, _, _, _ = setup_route()
    effects = []
    def invoked(text):
        effects.append(text)
        if outcome == 'False':
            return False
        if outcome == 'generic':
            raise RuntimeError('synthetic partial callback failure')
        if outcome == 'typed':
            raise GatewayDraining('synthetic callee proof', pre_effect=True, reason='busy_queue_refused')
        if outcome == 'base_exception':
            raise SyntheticBaseException()
        raise asyncio.CancelledError()
    agent = SimpleNamespace(steer=invoked)
    release = asyncio.Event()
    async def invoke():
        if path == 'adopted_child':
            await release.wait()
        return runner._steer_running_agent(agent, 'synthetic attempted effect')
    with gateway_ingress(runner):
        try:
            with gateway_ingress(runner):
                if path == 'adopted_child':
                    task = adopt_ingress_task(runner, asyncio.create_task(invoke()))
                else:
                    await invoke()
            if path == 'adopted_child':
                # Inner scope exited; adopted task still owns same ingress history.
                release.set()
                await task
        except BaseException as exc:
            expected = {'generic': RuntimeError, 'typed': RuntimeError,
                        'base_exception': SyntheticBaseException, 'cancel': asyncio.CancelledError}
            assert outcome in expected and isinstance(exc, expected[outcome])
        else:
            assert outcome == 'False'
        with pytest.raises(GatewayDraining) as refusal:
            refuse_queued_ingress(runner)
        assert refusal.value.pre_effect is False
    assert effects == ['synthetic attempted effect']


@pytest.mark.asyncio
async def test_independent_native_leg_does_not_borrow_parent_effect_or_permission():
    runner, _, _, _ = setup_route()
    from gateway.admission import mark_ingress_effect
    with gateway_ingress(runner):
        mark_ingress_effect(runner)
        with native_producer_ingress(runner):
            with pytest.raises(GatewayDraining) as independent:
                refuse_queued_ingress(runner)
            assert independent.value.pre_effect is True
        with pytest.raises(GatewayDraining) as parent:
            refuse_queued_ingress(runner)
        assert parent.value.pre_effect is False


@pytest.mark.asyncio
@pytest.mark.parametrize('replacement', ['foreign', 'malformed', 'new_cursor'])
async def test_archived_notification_cannot_delete_new_owner_after_settlement(tmp_path, monkeypatch, replacement):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='exact archived obligation', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify')
        kb.complete_task(conn, tid, summary='retained original result')
        kb.archive_task(conn, tid)
    rows = await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=False, gc_retention_days=30)
    row, = rows
    saved_attention = json.loads(row['sub']['delivery_metadata']['notification_attention_v1'])
    original = kbn.checkpoint_notify_delivery
    foreign = None
    def after_settle(conn, **kwargs):
        nonlocal foreign
        original(conn, **kwargs)
        if kwargs['outcome'] != 'accepted':
            return
        # Real second SQLite connection commits AFTER owned settlement/deletion.
        with kbc.connect() as other:
            if not kbn.list_notify_subs(other, tid):
                kbn.add_notify_sub(other, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify')
            record = dict(saved_attention, attempt_id='new-owner-attempt', owner='foreign-owner')
            foreign = None if replacement == 'malformed' else json.dumps(record, sort_keys=True)
            other.execute('UPDATE kanban_notify_subs SET delivery_metadata=?, last_event_id=? WHERE task_id=?',
                (json.dumps({'notification_attention_v1': foreign}),
                 saved_attention['claimed_cursor'] + (1 if replacement == 'new_cursor' else 0), tid))
            other.commit()
    monkeypatch.setattr(kbn, 'checkpoint_notify_delivery', after_settle)
    try:
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        with kbc.connect() as conn:
            retained = kbn.list_notify_subs(conn, tid)
            assert len(retained) == 1
            assert retained[0]['delivery_metadata']['notification_attention_v1'] == foreign
        assert not await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=True, gc_retention_days=1)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_clarification_registered_after_preflight_cannot_be_false_accepted(monkeypatch):
    from tools import clarify_gateway as cm
    from gateway.platforms.event import MessageEvent
    runner, adapter, source, key = setup_route()
    adapter._active_sessions[key] = asyncio.Event()
    original = cm.get_pending_for_session
    checks, reached = [], []
    entry = None
    def registered_between_lookups(session_key, **kwargs):
        nonlocal entry
        pending = original(session_key, **kwargs)
        checks.append(session_key)
        if len(checks) == 1:
            entry = cm.register('late-owned-control', key, 'Human choice', ['A', 'B'])
        return pending
    async def actual_core_control(event):
        reached.append(event)
        return await runner._hm_clarify_reply(event, source, key)
    adapter.set_message_handler(actual_core_control)
    monkeypatch.setattr(cm, 'get_pending_for_session', registered_between_lookups)
    try:
        with pytest.raises(GatewayDraining):
            with gateway_ingress(runner):
                await adapter.handle_message(MessageEvent(text='99', source=source))
        assert len(checks) >= 2 and not entry.event.is_set()
        assert not adapter._pending_messages and not runner._queued_events
    finally:
        cm.resolve_gateway_clarify('late-owned-control', '')
        cm.wait_for_response('late-owned-control', 0.1)


@pytest.mark.asyncio
async def test_delivery_uses_claimed_task_snapshot_after_foreign_task_change(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    original_file, foreign_file = tmp_path / 'original.txt', tmp_path / 'foreign.txt'
    original_file.write_text('owned')
    foreign_file.write_text('foreign')
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='ORIGINAL TITLE', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify')
        kb.complete_task(conn, tid, summary='original result ' + str(original_file))
    original = kbn.claim_notify_delivery
    def after_commit(conn, **kwargs):
        result = original(conn, **kwargs)
        with kbc.connect() as other:
            other.execute('UPDATE tasks SET title=?, result=?, session_id=? WHERE id=?',
                ('FOREIGN TITLE', 'foreign result ' + str(foreign_file), 'foreign-session', tid))
            other.commit()
        return result
    monkeypatch.setattr(kbn, 'claim_notify_delivery', after_commit)
    artifacts = []
    async def upload(**kwargs):
        from gateway.platforms.base import SendResult
        artifacts.append(kwargs['file_path'])
        return SendResult(success=True)
    adapter.send_document = upload
    try:
        row, = await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=False, gc_retention_days=30)
        retained = json.loads(row['sub']['delivery_metadata']['notification_attention_v1'])
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        assert retained['task']['title'] == 'ORIGINAL TITLE'
        assert len(adapter.wire) == 1 and 'ORIGINAL TITLE' in adapter.wire[0]
        assert 'FOREIGN TITLE' not in adapter.wire[0] and str(foreign_file) not in artifacts
        with kbc.connect() as conn:
            assert kb.get_task(conn, tid).title == 'FOREIGN TITLE'  # Foreign task edit preserved.
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_repeated_dispatcher_cancellation_holds_real_lock_until_physical_exit(tmp_path, monkeypatch):
    import threading
    import gateway.kanban_watchers as watchers
    from gateway.kanban_watchers_common import _acquire_singleton_lock, _release_singleton_lock
    from hermes_cli import kanban_db_dispatch as kbd
    runner, _, _, _ = setup_route()
    path = tmp_path / 'dispatcher.lock'
    handle, state = _acquire_singleton_lock(path)
    assert state == 'held'
    runner._kanban_dispatcher_lock_handle = handle
    entered, release = threading.Event(), threading.Event()
    def physical():
        entered.set()
        assert release.wait(5)
        return []
    runner._kanban_dispatcher_boot = lambda: (lambda: {'kanban': {'auto_decompose': False}}, kb, {})
    monkeypatch.setattr(watchers, '_resolve_dispatcher_settings', lambda *_: SimpleNamespace(interval=1))
    monkeypatch.setattr(watchers, '_KanbanDispatcher', lambda *_: SimpleNamespace(tick_once=physical, ready_nonempty=lambda: False))
    monkeypatch.setattr(watchers, '_kanban_dispatch_allowed', lambda: True)
    monkeypatch.setattr(kbd, 'reap_worker_zombies', lambda: [])
    real_sleep = asyncio.sleep
    async def initial_delay(_):
        await real_sleep(0)
    monkeypatch.setattr(watchers.asyncio, 'sleep', initial_delay)
    task = asyncio.create_task(runner._kanban_dispatcher_watcher())
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await real_sleep(0)
        task.cancel()
        await real_sleep(0)
        probe, status = _acquire_singleton_lock(path)
        _release_singleton_lock(probe)
        assert status == 'contended'
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        probe, status = _acquire_singleton_lock(path)
        try:
            assert status == 'held'
        finally:
            _release_singleton_lock(probe)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        runner._release_kanban_dispatcher_lock()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['persist', 'readback'])
async def test_archive_deletion_fault_retains_owned_attention_and_blocks_readiness(tmp_path, monkeypatch, failure):
    import sqlite3
    from gateway.admission import gateway_admission_status
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='archive persistence fault', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify')
        kb.complete_task(conn, tid, summary='exact original result')
        kb.archive_task(conn, tid)
    row, = await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=False, gc_retention_days=30)
    before = row['sub']['delivery_metadata']['notification_attention_v1']
    with kbc.connect() as conn:
        if failure == 'persist':
            conn.execute("CREATE TRIGGER fault BEFORE DELETE ON kanban_notify_subs BEGIN SELECT RAISE(ABORT, 'synthetic deletion failure'); END")
        else:
            conn.execute("CREATE TRIGGER fault AFTER DELETE ON kanban_notify_subs BEGIN INSERT INTO kanban_notify_subs(task_id,platform,chat_id,thread_id,created_at,last_event_id,delivery_metadata) VALUES(OLD.task_id,OLD.platform,OLD.chat_id,OLD.thread_id,OLD.created_at,OLD.last_event_id,'{}'); END")
        conn.commit()
    try:
        with pytest.raises((sqlite3.DatabaseError, RuntimeError)):
            await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        with kbc.connect() as conn:
            retained = kbn.list_notify_subs(conn, tid)
            assert len(retained) == 1
            assert retained[0]['delivery_metadata']['notification_attention_v1'] == before
        assert gateway_admission_status(runner)['known'] is False
    finally:
        await adapter.disconnect()
