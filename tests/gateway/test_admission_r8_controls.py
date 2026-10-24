"""Real approval waiters, archived workers and SQLite subscription generations."""
import asyncio
from contextlib import ExitStack, contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
import subprocess
import sys
import threading

import pytest

from gateway.admission import GatewayDraining, gateway_admission_status, gateway_ingress
from gateway.kanban_watchers_notifier import _notifier_collect
from gateway.platforms.event import MessageEvent
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from tests.gateway.test_kanban_wake_acceptance import setup_route


@contextmanager
def pending_approval(key):
    from tools import approval
    from tools.approval_gateway_wait import _await_gateway_decision
    ready = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        waiter = executor.submit(_await_gateway_decision, key, lambda _: ready.set(),
                                 {'command': 'synthetic inert command', 'pattern_key': 'r8'})
        try:
            assert ready.wait(3)
            yield waiter
        finally:
            approval.unregister_gateway_notify(key)
            waiter.result(timeout=3)


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['base', 'late_base', 'busy', 'approve', 'deny'])
@pytest.mark.parametrize('answer', ['yes', 'no'])
async def test_leased_approval_cannot_resolve_waiting_execution(monkeypatch, path, answer):
    from tools import approval
    runner, adapter, source, key = setup_route()
    adapter._active_sessions[key] = asyncio.Event()
    runner._is_user_authorized_for_source = lambda _: True
    runner._admit_bot_message_for_source = lambda _: True
    runner._effective_busy_input_mode = lambda _: 'steer'
    runner._pending_approvals = {}
    adapter.set_message_handler(runner._handle_message)
    adapter.set_busy_session_handler(runner._handle_active_session_busy_message)
    event = MessageEvent(text=answer, source=source)
    with ExitStack() as cleanup:
        waiter = None
        if path == 'late_base':
            import gateway.admission as admission
            original = admission.prepare_ingress_dispatch
            async def late(*args):
                nonlocal waiter
                dispatched = await original(*args)
                # Real thread registers immediately after Base preflight.
                waiter = cleanup.enter_context(pending_approval(key))
                return dispatched
            monkeypatch.setattr(admission, 'prepare_ingress_dispatch', late)
        else:
            waiter = cleanup.enter_context(pending_approval(key))
        with pytest.raises(GatewayDraining) as refusal:
            with gateway_ingress(runner):
                if path in {'base', 'late_base'}:
                    await adapter.handle_message(event)
                elif path == 'busy':
                    await runner._route_plaintext_approval_while_busy(event, key)
                else:
                    event.text = '/' + path
                    await getattr(runner, '_handle_' + path + '_command')(event)
        assert refusal.value.pre_effect is True
        if path not in {'approve', 'deny'}:
            assert event.text == answer
        assert not waiter.done() and approval.list_gateway_approvals(key)
        assert not adapter._pending_messages and not runner._queued_events
        # Ordinary unleased human reply still resolves actual waiting thread.
        event.text = answer
        assert await runner._route_plaintext_approval_while_busy(event, key) is True
        decision = await asyncio.to_thread(waiter.result, 3)
        assert decision['choice'] == ('once' if answer == 'yes' else 'deny')


@pytest.mark.asyncio
async def test_leased_approval_lookup_error_propagates_unknown(monkeypatch):
    from tools import approval
    runner, _, source, key = setup_route()
    def unreadable(_):
        raise RuntimeError('synthetic approval registry failure')
    monkeypatch.setattr(approval, 'has_blocking_approval', unreadable)
    with gateway_ingress(runner):
        with pytest.raises(RuntimeError, match='registry failure'):
            await runner._route_plaintext_approval_while_busy(MessageEvent(text='yes', source=source), key)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['running', 'terminal_live', 'orphan_claim', 'unreadable', 'empty'])
async def test_supported_board_archive_keeps_physical_work_in_strict_inventory(tmp_path, monkeypatch, state):
    from hermes_cli import kanban_db_dispatch as kbd
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    kb.create_board('retained')
    worker = None
    try:
        with kbc.connect(board='retained') as conn:
            if state not in {'empty', 'unreadable'}:
                tid = kb.create_task(conn, title='archived exact worker', assignee='owner')
                kb.claim_task(conn, tid)
                if state != 'orphan_claim':
                    worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
                    kbd._set_worker_pid(conn, tid, worker.pid)
                if state == 'terminal_live':
                    kb.complete_task(conn, tid, summary='result settled, process still owns execution',
                                     expected_run_id=kb.get_task(conn, tid).current_run_id)
        archived = kb.remove_board('retained', archive=True)
        if state == 'unreadable':
            from pathlib import Path
            (Path(archived['new_path']) / 'kanban.db').write_bytes(b'malformed retained board')
        runner, _, _, _ = setup_route()
        runner._ingress_barrier.close()
        status = gateway_admission_status(runner)
        if state == 'unreadable':
            assert status['known'] is False and status['quiescent'] is None
        else:
            assert status['known'] is True
            assert status['quiescent'] is (state == 'empty')
            if state != 'empty':
                assert status['active_work'] >= 1
    finally:
        if worker is not None:
            worker.stdin.write(b'x')
            worker.stdin.close()
            assert worker.wait(timeout=5) == 0
            if state == 'terminal_live':
                settled = gateway_admission_status(runner)
                assert settled['known'] and settled['quiescent']


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ['user_id', 'notifier_profile'])
async def test_subscription_self_heal_cannot_claim_stale_attention_owner(tmp_path, monkeypatch, field):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    runner._profile_adapters = {'new-owner': {adapter.platform: adapter}}
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='exact subscription generation', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify')
        kb.complete_task(conn, tid, summary='exact result')
        before = kbn.list_notify_subs(conn, tid)[0]
    original = kbn.claim_notify_delivery
    def concurrent_self_heal(conn, **kwargs):
        with kbc.connect() as other:
            kbn.add_notify_sub(other, task_id=tid, platform='telegram', chat_id='42',
                               **{field: 'new-owner'})
        return original(conn, **kwargs)
    monkeypatch.setattr(kbn, 'claim_notify_delivery', concurrent_self_heal)
    try:
        rows = await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=False, gc_retention_days=30)
        assert rows == []
        with kbc.connect() as conn:
            after = kbn.list_notify_subs(conn, tid)[0]
        assert after[field] == 'new-owner' and after['last_event_id'] == before['last_event_id']
        assert 'notification_attention_v1' not in after['delivery_metadata']
        monkeypatch.setattr(kbn, 'claim_notify_delivery', original)
        row, = await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=False, gc_retention_days=30)
        record = json.loads(row['sub']['delivery_metadata']['notification_attention_v1'])
        profile = 'new-owner' if field == 'notifier_profile' else 'default'
        subscriber = 'new-owner' if field == 'user_id' else '42'
        assert record['owner'] == f'profile:{profile}/telegram:subscriber:{subscriber}'
        assert record['profile'] == profile
        assert record['subscription'][field] == 'new-owner'
        from gateway.config import Platform
        from gateway.session import SessionSource
        home = runner._resolve_profile_home_for_source(SessionSource(platform=Platform.TELEGRAM,
            chat_id='42', profile=after.get('notifier_profile'))).resolve()
        assert record['profile_home'] == str(home)
    finally:
        await adapter.disconnect()
