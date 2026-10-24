"""Real capacity registry, goal shutdown and notification checkpoint failures."""
import asyncio
import json

import pytest

from gateway.admission import GatewayDraining, gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from hermes_cli import active_sessions as active
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from hermes_cli.goals import GoalManager
from tests.gateway.test_admission_native_work import native  # noqa: F401
from tests.gateway.test_kanban_wake_acceptance import setup_route


@pytest.mark.asyncio
@pytest.mark.parametrize('stale_guard', [False, True])
async def test_leased_capacity_refuses_before_base_ack_or_core_effect(native, stale_guard):
    runner, adapter = native.runner, native.adapter
    runner.config.max_concurrent_sessions = 1
    other, refusal = active.try_acquire_active_session(session_id='other-owner', surface='fixture',
        config=runner.config, registry_home=native.home, track_liveness=True)
    assert other is not None and refusal is None
    event = MessageEvent(text='capacity wake', source=SessionSource(platform=Platform.TELEGRAM,
        chat_id='123456', user_id='u1', chat_type='dm'))
    if stale_guard:
        key = runner._session_key_for_source(event.source)
        done = asyncio.create_task(asyncio.sleep(0))
        await done
        adapter._active_sessions[key] = asyncio.Event()
        adapter._session_tasks[key] = done
    try:
        with pytest.raises(GatewayDraining) as rejected:
            with gateway_ingress(runner):
                await adapter.handle_message(event)
        assert rejected.value.pre_effect is True
        assert event._gateway_accepted is False and not native.effects
        assert not adapter._pending_messages and not runner._queued_events
    finally:
        other.release()


@pytest.mark.asyncio
async def test_unreadable_capacity_registry_blocks_readiness(native):
    registry = native.home / 'runtime' / 'active_sessions.json'
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text('malformed synthetic ownership')
    event = MessageEvent(text='unreadable capacity', source=SessionSource(platform=Platform.TELEGRAM,
        chat_id='123456', user_id='u1', chat_type='dm'))
    with pytest.raises(GatewayDraining) as refusal:
        with gateway_ingress(native.runner):
            await native.adapter.handle_message(event)
    assert refusal.value.pre_effect is False
    assert gateway_admission_status(native.runner)['known'] is False
    assert not native.effects


@pytest.mark.asyncio
async def test_accepted_goal_capacity_refusal_preserves_durable_maintenance_handoff(native, monkeypatch):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('retained goal capacity race')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='exact prior result')
    key = runner._session_key_for_source(source)
    event = adapter._pending_messages.pop(key)
    runner.config.max_concurrent_sessions = 1
    other, refusal = active.try_acquire_active_session(session_id='other-owner', surface='fixture',
        config=runner.config, registry_home=native.home, track_liveness=True)
    assert other is not None and refusal is None
    try:
        adapter._spawn_drain_task(event, key)
        await asyncio.gather(*adapter._background_tasks)
        assert GoalManager(entry.session_id).state.status == 'paused'
        row, = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
        record = json.loads(row[1])
        assert record['continuation_prompt'] == event.text and record['final_response'] == 'exact prior result'
        assert not native.effects
    finally:
        other.release()


@pytest.mark.asyncio
async def test_leased_capacity_owned_before_background_core_and_released_on_cancel(native, monkeypatch):
    runner, adapter = native.runner, native.adapter
    runner.config.max_concurrent_sessions = 1
    entered, release = asyncio.Event(), asyncio.Event()
    original = adapter._run_processing_hook
    async def held(label, *args):
        if label == 'on_processing_start':
            entered.set()
            await release.wait()
        return await original(label, *args)
    monkeypatch.setattr(adapter, '_run_processing_hook', held)
    async def attempt(chat):
        event = MessageEvent(text=chat, source=SessionSource(platform=Platform.TELEGRAM,
            chat_id=chat, user_id='u1', chat_type='dm'))
        try:
            with gateway_ingress(runner):
                await adapter.handle_message(event)
            return event._gateway_accepted
        except GatewayDraining:
            return False
    try:
        outcomes = await asyncio.gather(attempt('123456'), attempt('654321'))
        assert outcomes.count(True) == 1 and outcomes.count(False) == 1
        await asyncio.wait_for(entered.wait(), 3)
        assert len(active.active_session_registry_snapshot(native.home, strict=True)) == 1
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        tasks = list(adapter._background_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)
        assert active.active_session_registry_snapshot(native.home, strict=True) == []
    finally:
        release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize('location', ['slot', 'overflow'])
@pytest.mark.parametrize('result', [None, {'final_response': ''}, {'final_response': 'muted result'}])
async def test_shutdown_preserves_accepted_goal_before_destructive_fifo_drain(native, monkeypatch, location, result):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('retained shutdown goal')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='prior checkpoint')
    key = runner._session_key_for_source(source)
    event = adapter._pending_messages[key]
    event.message_id = 'exact-goal-input'
    if location == 'overflow':
        adapter._pending_messages[key] = MessageEvent(text='ordinary human', source=source)
        runner._session_state(key).conversation.queued_events.append(event)
    runner._draining, runner._running = True, False
    await runner._run_agent_drain_pending(result, adapter, source, key)
    assert GoalManager(entry.session_id).state.status == 'paused'
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    record = json.loads(rows[0][1])
    assert record['continuation_prompt'] == event.text and record['final_response'] == 'prior checkpoint'
    assert record['input_context']['message_id'] == 'exact-goal-input'
    from hermes_state import SessionDB
    reopened = SessionDB(db_path=native.home / 'state.db')
    try:
        assert json.loads(reopened.get_meta(rows[0][0])) == record
        assert json.loads(reopened.get_meta('goal:' + entry.session_id))['status'] == 'paused'
    finally:
        reopened.close()
    assert not native.effects


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['ignore', 'restore', 'foreign'])
async def test_ping_checkpoint_readback_failure_retains_unknown_and_never_rewinds(tmp_path, monkeypatch, failure):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, key = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='exact ping checkpoint', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='notify+wake')
        kb.complete_task(conn, tid, summary='owned result')
    row, = await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=False, gc_retention_days=30)
    attention = row['sub']['delivery_metadata']['notification_attention_v1']
    with kbc.connect() as conn:
        if failure == 'ignore':
            conn.execute("CREATE TRIGGER ping_fault BEFORE UPDATE OF last_ping_event_id ON kanban_notify_subs BEGIN SELECT RAISE(IGNORE); END")
        elif failure == 'restore':
            conn.execute("CREATE TRIGGER ping_fault AFTER UPDATE OF last_ping_event_id ON kanban_notify_subs BEGIN UPDATE kanban_notify_subs SET last_ping_event_id=OLD.last_ping_event_id WHERE task_id=OLD.task_id; END")
        else:
            conn.execute("CREATE TRIGGER ping_fault AFTER UPDATE OF last_ping_event_id ON kanban_notify_subs BEGIN UPDATE kanban_notify_subs SET delivery_metadata='{\"notification_attention_v1\":\"foreign\"}' WHERE task_id=OLD.task_id; END")
        conn.commit()
    try:
        await _KanbanNotification(runner, row, platform_cls=Platform, sub_fail_counts={}).deliver()
        with kbc.connect() as conn:
            retained = kbn.list_notify_subs(conn, tid)[0]
        assert retained['delivery_metadata']['notification_attention_v1'] == attention
        assert retained['last_event_id'] == row['cursor'] and retained['last_ping_event_id'] == 0
        assert gateway_admission_status(runner)['known'] is False
        assert len(adapter.wire) == 1
        assert not await asyncio.to_thread(_notifier_collect, runner, kb,
            notifier_profile=None, gc_due=True, gc_retention_days=1)
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['task_abort', 'task_ignore', 'run_ignore', 'event_ignore'])
async def test_post_spawn_pid_write_failure_never_refunds_live_worker_or_reports_zero(tmp_path, monkeypatch, failure):
    import subprocess
    import sys
    from hermes_cli import kanban_db_dispatch as kbd
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    runner, _, _, _ = setup_route()
    workers = []
    def spawn(*_args, **_kwargs):
        worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
        workers.append(worker)
        return worker.pid
    try:
        with kbc.connect() as conn:
            tid = kb.create_task(conn, title='exact unresolved spawned worker', assignee='owner',
                workspace_kind='dir', workspace_path=str(tmp_path))
            if failure == 'task_abort':
                conn.execute("CREATE TRIGGER pid_fault BEFORE UPDATE OF worker_pid ON tasks WHEN NEW.worker_pid IS NOT NULL BEGIN SELECT RAISE(ABORT, 'synthetic PID persistence failure'); END")
            elif failure in {'task_ignore', 'run_ignore'}:
                table = 'tasks' if failure == 'task_ignore' else 'task_runs'
                conn.execute(f'CREATE TRIGGER pid_fault BEFORE UPDATE OF worker_pid ON {table} BEGIN SELECT RAISE(IGNORE); END')
            else:
                conn.execute("CREATE TRIGGER pid_fault BEFORE INSERT ON task_events WHEN NEW.kind='spawned' BEGIN SELECT RAISE(IGNORE); END")
            conn.commit()
            with pytest.raises(RuntimeError, match='ownership'):
                kbd.dispatch_once(conn, spawn_fn=spawn)
            task = kb.get_task(conn, tid)
            assert task.status == 'running' and task.claim_lock and task.worker_pid is None
            assert len(workers) == 1 and workers[0].poll() is None
            assert conn.execute('SELECT outcome FROM task_runs WHERE id=?', (task.current_run_id,)).fetchone()[0] is None
            assert conn.execute("SELECT COUNT(*) FROM task_events WHERE task_id=? AND kind='spawn_failed'", (tid,)).fetchone()[0] == 0
            evidence = json.loads(conn.execute("SELECT payload FROM task_events WHERE task_id=? AND kind='worker_spawn_unknown'", (tid,)).fetchone()[0])
            assert evidence['pid'] == workers[0].pid and evidence['owner'] == 'profile:owner/kanban-dispatcher'
            runner._ingress_barrier.close()
            assert gateway_admission_status(runner)['known'] is False
            # New connection simulates restart; TTL expiry cannot grant another spawn.
            conn.execute('DROP TRIGGER pid_fault')
            conn.execute('UPDATE tasks SET claim_expires=1 WHERE id=?', (tid,))
            conn.commit()
        with kbc.connect() as reopened:
            assert kb.release_stale_claims(reopened) == 0
            assert not kbd.dispatch_once(reopened, spawn_fn=spawn).spawned
            assert len(workers) == 1
    finally:
        for worker in workers:
            worker.stdin.write(b'x')
            worker.stdin.close()
            assert worker.wait(timeout=5) == 0


@pytest.mark.asyncio
async def test_cancel_before_first_step_releases_only_owned_untransferred_capacity(native):
    runner, adapter = native.runner, native.adapter
    runner.config.max_concurrent_sessions = 1
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    with gateway_ingress(runner):
        await adapter.handle_message(MessageEvent(text='unstarted', source=source))
        assert len(active.active_session_registry_snapshot(native.home, strict=True)) == 1
        task, = adapter._background_tasks
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)
    assert active.active_session_registry_snapshot(native.home, strict=True) == []
    assert not native.effects


@pytest.mark.asyncio
@pytest.mark.parametrize('exception_type', [TypeError, ValueError])
async def test_unknown_spawn_callback_runs_once_and_never_refunds(tmp_path, monkeypatch, exception_type):
    from hermes_cli import kanban_db_dispatch as kbd
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    invocations = []
    def partial(*args, **kwargs):
        invocations.append('synthetic physical launch boundary')
        raise exception_type('outcome unknown after effect')
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='retain uncertain launch', assignee='owner',
                             workspace_kind='dir', workspace_path=str(tmp_path))
        errors = []
        try:
            kbd.dispatch_once(conn, spawn_fn=partial)
        except RuntimeError as exc:
            errors.append(str(exc))
        assert len(invocations) == 1
        assert len(errors) == 1 and 'ownership UNKNOWN' in errors[0]
        retained = kb.get_task(conn, tid)
        assert retained.status == 'running' and retained.claim_lock
        assert kbd.dispatch_once(conn, spawn_fn=partial).spawned == []
        assert len(invocations) == 1


@pytest.mark.asyncio
async def test_capacity_registry_actual_home_scope_a_b_a(native):
    from gateway.admission_capacity import reserve_ingress_capacity
    b = native.home / 'profiles' / 'b'
    b.mkdir(parents=True)
    (b / 'config.yaml').write_text('{}\n')
    native.runner.config.max_concurrent_sessions = 1
    for profile, home in ((None, native.home), ('b', b), (None, native.home)):
        source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1',
                               chat_type='dm', profile=profile)
        event = MessageEvent(text='exact routed capacity', source=source)
        assert native.runner._resolve_profile_home_for_source(source).resolve() == home.resolve()
        key = native.runner._session_key_for_source(source)
        with gateway_ingress(native.runner):
            reserve_ingress_capacity(native.runner, event, key)
            assert len(active.active_session_registry_snapshot(home, strict=True)) == 1
            other = b if home == native.home else native.home
            assert active.active_session_registry_snapshot(other, strict=True) == []
        assert active.active_session_registry_snapshot(home, strict=True) == []


@pytest.mark.asyncio
async def test_native_proven_missing_executable_keeps_prelaunch_retry_contract(tmp_path, monkeypatch):
    from hermes_cli import kanban_db_dispatch as kbd
    from hermes_cli.kanban_db_spawn import pending_worker_launch
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    monkeypatch.setattr(kbd, '_worker_argv', lambda *args: [str(tmp_path / 'missing-native-worker-executable')])
    monkeypatch.setattr(kbd, '_restart_safe_worker_argv', lambda task, argv: argv)
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='known native prelaunch refusal', assignee='owner',
                             workspace_kind='dir', workspace_path=str(tmp_path))
        assert kbd.dispatch_once(conn).spawned == []
        task = kb.get_task(conn, tid)
        assert task.status == 'ready' and task.claim_lock is None
        assert task.consecutive_failures == 1
        assert pending_worker_launch(conn, tid) is None
