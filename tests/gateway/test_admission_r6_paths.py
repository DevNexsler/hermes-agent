"""Real native/SQLite/HTTP regressions from R6; synthetic effects, temp homes only."""
import asyncio
import json
import threading
from types import SimpleNamespace

from aiohttp import web
import pytest

from gateway.admission import GatewayDraining, gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
from gateway.platforms.base import SendResult
from gateway.platforms.event import MessageEvent
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from tests.gateway.test_kanban_wake_acceptance import setup_route


def claim(runner):
    return _notifier_collect(runner, kb, notifier_profile=None, gc_due=False, gc_retention_days=30)


def seed(key, *, mode='wake', mixed=False, artifact=None):
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='R6 exact obligation', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', user_id='42',
                          chat_type='dm', delivery_mode=mode)
        if mixed:
            kb.block_task(conn, tid, reason='synthetic diagnostic', kind='transient')
        kb.complete_task(conn, tid, summary='retained result')
        if artifact:
            conn.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='completed'",
                         (json.dumps({'artifacts': [str(artifact)]}), tid))
            conn.commit()
    return tid


def sub(tid):
    with kbc.connect() as conn:
        return kbn.list_notify_subs(conn, tid)[0]


@pytest.mark.asyncio
@pytest.mark.parametrize('answer', ['1', '99', 'ordinary prose'])
@pytest.mark.parametrize('entry_path', ['base', 'core'])
@pytest.mark.parametrize('busy', [False, True])
async def test_leased_busy_clarification_cannot_consume_or_ack_wake(answer, entry_path, busy):
    from tools import clarify_gateway as cm
    runner, adapter, source, key = setup_route()
    if busy:
        adapter._active_sessions[key] = asyncio.Event()
    received = []
    async def intercept(event):
        received.append(event)
        return await runner._hm_clarify_reply(event, source, key)
    adapter.set_message_handler(intercept)
    entry = cm.register('r6-clarify', key, 'Human choice', ['A', 'B'])
    try:
        with pytest.raises(GatewayDraining) as refusal:
            with gateway_ingress(runner):
                if entry_path == 'base':
                    await adapter.handle_message(MessageEvent(text=answer, source=source))
                else:
                    await runner._hm_clarify_reply(MessageEvent(text=answer, source=source), source, key)
        assert refusal.value.pre_effect is True
        assert cm.get_pending_for_session(key, include_choice_prompts=True) is entry
        assert not entry.event.is_set() and not received
        # Ordinary human still reaches the existing resolver.
        await adapter.handle_message(MessageEvent(text='1', source=source))
        await asyncio.gather(*list(adapter._background_tasks))
        assert entry.event.is_set() and len(received) == 1
    finally:
        cm.resolve_gateway_clarify('r6-clarify', '')
        cm.wait_for_response('r6-clarify', 0.1)


@pytest.mark.asyncio
async def test_partial_wake_batch_refusal_keeps_original_attention(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr('gateway.warning_notifications.warning_notifications_enabled', lambda *_: False)
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    tid = seed(key, mixed=True)
    row, = await asyncio.to_thread(claim, runner)
    before = sub(tid)
    calls = []
    async def accepted_then_refused(event):
        calls.append(event)
        if len(calls) == 2:
            raise GatewayDraining('known pre-effect busy', pre_effect=True, reason='busy_backlog')
        event._gateway_accepted = True
    adapter.handle_message = accepted_then_refused
    try:
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        assert len(calls) == 2
        assert sub(tid)['delivery_metadata']['notification_attention_v1'] == before['delivery_metadata']['notification_attention_v1']
        assert sub(tid)['last_event_id'] == before['last_event_id']
        assert not await asyncio.to_thread(claim, runner)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['raise', 'negative', 'cancel'])
async def test_actual_artifact_failure_never_settles_attention(tmp_path, monkeypatch, failure):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    artifact = tmp_path / 'artifact.txt'
    artifact.write_text('synthetic')
    await adapter.connect()
    tid = seed(key, mode='notify', artifact=artifact)
    row, = await asyncio.to_thread(claim, runner)
    before = sub(tid)
    effects = []
    async def uncertain(**kwargs):
        effects.append(kwargs['file_path'])
        if failure == 'cancel':
            raise asyncio.CancelledError()
        if failure == 'raise':
            raise RuntimeError('synthetic artifact outcome unknown')
        return SendResult(success=False, error='synthetic uncertain upload')
    adapter.send_document = uncertain
    try:
        delivery = _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={})
        if failure == 'cancel':
            with pytest.raises(asyncio.CancelledError):
                await delivery.deliver()
        else:
            await delivery.deliver()
        assert effects == [str(artifact)] and len(adapter.wire) == 1
        assert sub(tid)['delivery_metadata']['notification_attention_v1'] == before['delivery_metadata']['notification_attention_v1']
        assert not await asyncio.to_thread(claim, runner)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['lost_response', '500'])
async def test_owned_notification_http_unknown_posts_once_and_retains_restart_intent(tmp_path, monkeypatch, outcome):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr('gateway.wake._RETRY_DELAYS_SECONDS', (0, 0, 0))
    runner, adapter, _, key = setup_route()
    adapter.supports_async_delivery = False
    adapter._host, adapter._api_key = '127.0.0.1', 'isolated-key'
    posts = []
    async def effect_then_fail(request):
        posts.append(await request.json())
        if outcome == 'lost_response':
            request.transport.close()
        return web.Response(status=500)
    app = web.Application()
    app.router.add_post('/v1/chat/completions', effect_then_fail)
    server = web.AppRunner(app)
    await server.setup()
    site = web.TCPSite(server, '127.0.0.1', 0)
    await site.start()
    adapter._port = site._server.sockets[0].getsockname()[1]
    await adapter.connect()
    tid = seed(key)
    row, = await asyncio.to_thread(claim, runner)
    before = sub(tid)
    try:
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        assert len(posts) == 1
        assert sub(tid)['delivery_metadata']['notification_attention_v1'] == before['delivery_metadata']['notification_attention_v1']
        restarted, reopened, _, _ = setup_route()
        await reopened.connect()
        try:
            assert not await asyncio.to_thread(claim, restarted)
        finally:
            await reopened.disconnect()
        assert len(posts) == 1
    finally:
        await adapter.disconnect()
        await server.cleanup()


@pytest.mark.asyncio
async def test_foreign_attention_replacement_after_claim_never_adopted(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    tid = seed(key, mode='notify')
    original = kbn.claim_notify_delivery
    foreign = None
    def swapped(conn, **kwargs):
        nonlocal foreign
        result = original(conn, **kwargs)
        row = sub(tid)
        record = json.loads(row['delivery_metadata']['notification_attention_v1'])
        record['owner'], record['attempt_id'] = 'foreign', 'foreign-attempt'
        foreign = json.dumps(record, sort_keys=True)
        with kbc.connect() as other:
            other.execute('UPDATE kanban_notify_subs SET delivery_metadata=? WHERE task_id=?',
                          (json.dumps({'notification_attention_v1': foreign}), tid))
            other.commit()
        return result
    monkeypatch.setattr(kbn, 'claim_notify_delivery', swapped)
    try:
        rows = await asyncio.to_thread(claim, runner)
        if rows:
            with pytest.raises(RuntimeError):
                await _KanbanNotification(runner, rows[0], platform_cls=Platform, sub_fail_counts={}).deliver()
        assert not adapter.wire
        assert sub(tid)['delivery_metadata']['notification_attention_v1'] == foreign
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('closed', [True, False])
async def test_actual_dispatcher_fences_before_tick_and_retains_cancelled_executor(monkeypatch, closed):
    import gateway.kanban_watchers as watchers
    from hermes_cli import kanban_db_dispatch as kbd
    runner, _, _, _ = setup_route()
    runner._kanban_dispatcher_lock_handle = None
    entered, finished = threading.Event(), threading.Event()
    release = threading.Event()
    effects = []
    def dispatch():
        effects.append('synthetic spawn boundary')
        entered.set()
        try:
            assert release.wait(5)
        finally:
            finished.set()
        return []
    runner._kanban_dispatcher_boot = lambda: (lambda: {'kanban': {'auto_decompose': False}}, kb, {})
    monkeypatch.setattr(watchers, '_resolve_dispatcher_settings', lambda *_: SimpleNamespace(interval=1))
    monkeypatch.setattr(watchers, '_KanbanDispatcher', lambda *_: SimpleNamespace(tick_once=dispatch, ready_nonempty=lambda: False))
    monkeypatch.setattr(watchers, '_kanban_dispatch_allowed', lambda: True)
    monkeypatch.setattr(kbd, 'reap_worker_zombies', lambda: [])
    real_sleep = asyncio.sleep
    async def initial_sleep(delay):
        await real_sleep(0)
    monkeypatch.setattr(watchers.asyncio, 'sleep', initial_sleep)
    async def end_tick(_):
        runner._running = False
    runner._sleep_between_ticks = end_tick
    if closed:
        runner._ingress_barrier.close()
        release.set()
    task = asyncio.create_task(runner._kanban_dispatcher_watcher())
    try:
        if closed:
            await asyncio.wait_for(task, 5)
            assert not effects
        else:
            assert await asyncio.to_thread(entered.wait, 3)
            runner._ingress_barrier.close()
            task.cancel()
            await real_sleep(0)
            status = gateway_admission_status(runner)
            assert status['known'] and not status['quiescent']
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert await asyncio.to_thread(finished.wait, 3)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['running', 'terminal_live', 'orphan_claim', 'unreadable'])
async def test_real_kanban_worker_store_prevents_false_quiescence(tmp_path, monkeypatch, state):
    import subprocess
    import sys
    from hermes_cli import kanban_db_dispatch as kbd
    db = tmp_path / 'board.db'
    monkeypatch.setenv('HERMES_KANBAN_DB', str(db))
    runner, _, _, _ = setup_route()
    runner._ingress_barrier.close()
    worker = None
    try:
        if state == 'unreadable':
            db.write_bytes(b'synthetic malformed board')
        else:
            with kbc.connect() as conn:
                tid = kb.create_task(conn, title='tracked subprocess', assignee='owner')
                kb.claim_task(conn, tid)
                if state != 'orphan_claim':
                    worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
                    kbd._set_worker_pid(conn, tid, worker.pid)
                if state == 'terminal_live':
                    kb.complete_task(conn, tid, summary='result settled; physical process still live',
                                     expected_run_id=kb.get_task(conn, tid).current_run_id)
        status = gateway_admission_status(runner)
        if state == 'unreadable':
            assert status['known'] is False and status['quiescent'] is None
        else:
            assert status['known'] and not status['quiescent'] and status['active_work'] >= 1
    finally:
        if worker is not None:
            worker.stdin.write(b'x')
            worker.stdin.close()
            assert worker.wait(timeout=5) == 0


@pytest.mark.asyncio
async def test_partial_image_delivery_keeps_aggregate_attention(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    files = [tmp_path / 'one.png', tmp_path / 'two.png']
    for path in files:
        path.write_bytes(b'synthetic inert image')
    await adapter.connect()
    tid = seed(key, mode='notify')
    with kbc.connect() as conn:
        conn.execute("UPDATE task_events SET payload=? WHERE task_id=? AND kind='completed'",
                     (json.dumps({'artifacts': [str(path) for path in files]}), tid))
        conn.commit()
    row, = await asyncio.to_thread(claim, runner)
    before = sub(tid)
    effects = []
    async def image_boundary(**kwargs):
        effects.append(kwargs['image_path'])
        return SendResult(success=len(effects) == 1, error='synthetic partial image failure')
    adapter.send_image_file = image_boundary
    try:
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        assert effects == [str(path) for path in files]
        assert sub(tid)['delivery_metadata']['notification_attention_v1'] == before['delivery_metadata']['notification_attention_v1']
        assert not await asyncio.to_thread(claim, runner)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['update', 'confirm'])
@pytest.mark.parametrize('entry_path', ['base', 'core'])
async def test_other_pending_controls_refuse_lease_before_resolution(kind, entry_path):
    from tools import slash_confirm
    runner, adapter, source, key = setup_route()
    actions = []
    async def unexpected(event):
        actions.append(event)
    adapter.set_message_handler(unexpected)
    state = runner._session_state(key)
    if kind == 'update':
        state.persistent.update_prompt_pending = True
    else:
        slash_confirm.register(key, 'owned-confirm', 'synthetic', unexpected)
    event = MessageEvent(text='yes', source=source)
    try:
        with pytest.raises(GatewayDraining) as refusal:
            with gateway_ingress(runner):
                if entry_path == 'base':
                    await adapter.handle_message(event)
                elif kind == 'update':
                    runner._hm_update_prompt_reply(event, key)
                else:
                    await runner._hm_slash_confirm_reply(event, key)
        assert refusal.value.pre_effect is True
        assert not actions and not adapter._background_tasks
        if kind == 'update':
            assert state.persistent.update_prompt_pending is True
        else:
            assert slash_confirm.get_pending(key)['confirm_id'] == 'owned-confirm'
    finally:
        slash_confirm.clear(key)
