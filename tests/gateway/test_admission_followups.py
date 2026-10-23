"""Unrelated late arrivals and native standing goals never borrow a previous HTTP lease."""
import asyncio
import threading

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress, ingress_admitted
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.run_turn_runner import TurnRunner
from gateway.session import SessionSource
from hermes_cli.goals import GoalManager
from tests.gateway.test_admission_core_executor import executor_futures, settled  # noqa: F401 — real futures
from tests.gateway.test_admission_native_work import native  # noqa: F401 — real native fixture


@pytest.mark.asyncio
async def test_late_human_queue_reaches_real_worker_without_stale_lease_and_retains_cancelled_work(native, monkeypatch, executor_futures):
    runner, adapter = native.runner, native.adapter
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    key = runner._session_key_for_source(source)
    deliver, queued, physical, exited = (asyncio.Event() for _ in range(4))
    release = threading.Event()
    loop = asyncio.get_running_loop()
    effects = []
    def inference(turn):
        text = turn._ctx.message
        effects.append(text)
        if text == 'late independent human':
            try:
                loop.call_soon_threadsafe(physical.set)
                assert release.wait(10)
            finally:
                loop.call_soon_threadsafe(exited.set)
        result = {'final_response': 'synthetic reply', 'messages': [], 'api_calls': 0, 'completed': True}
        turn._ctx.result_holder[0] = result
        return result
    monkeypatch.setattr(TurnRunner, 'run_sync', inference)
    original = adapter._fire_post_delivery_callback
    callbacks = []
    async def before_cleanup(*args):
        if not callbacks:
            callbacks.append(True)
            deliver.set()
            await queued.wait()
        await original(*args)
    monkeypatch.setattr(adapter, '_fire_post_delivery_callback', before_cleanup)
    async def independent_arrival():
        await deliver.wait()
        assert not ingress_admitted(runner)
        await adapter.handle_message(MessageEvent(text='late independent human', source=source))
        queued.set()
    arrival = asyncio.create_task(independent_arrival())  # Created before authenticated lease.
    try:
        with gateway_ingress(runner):
            await adapter.handle_message(MessageEvent(text='first admitted', source=source))
        await asyncio.wait_for(physical.wait(), 8)
        await asyncio.wait_for(arrival, 5)
        owner = adapter._session_tasks.get(key)
        assert owner is not None and not owner.done(), 'late worker lost its Base/core owner'
        runner._enter_external_drain()
        await adapter.cancel_session_processing(key)
        status = gateway_admission_status(runner)
        assert status['known'] and not status['quiescent'], 'physical late worker hidden by cancellation'
    finally:
        release.set()
        arrival.cancel()
        await asyncio.gather(arrival, return_exceptions=True)
        if physical.is_set():
            await asyncio.wait_for(exited.wait(), 5)
        await settled(executor_futures)
    assert effects == ['first admitted', 'late independent human']
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
async def test_leased_post_turn_goal_preserves_real_continuation(native, monkeypatch):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('finish synthetic standing goal')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more work', False, None, False))
    with gateway_ingress(native.runner):
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
    key = native.runner._session_key_for_source(source)
    pending = native.adapter._pending_messages.get(key)
    assert pending is not None and 'standing goal' in pending.text
    assert GoalManager(entry.session_id).state.turns_used == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['before_judge', 'during_judge'])
async def test_maintenance_goal_pause_is_durable_owned_and_keeps_continuation_history(native, monkeypatch, phase):
    import json
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    native.db.append_message(entry.session_id, 'user', 'synthetic retained history')
    history = native.db.get_messages(entry.session_id)
    GoalManager(entry.session_id).set('finish synthetic standing goal')
    expected_prompt = GoalManager(entry.session_id).next_continuation_prompt()
    loop = asyncio.get_running_loop()
    def close():
        native.runner._ingress_barrier.hold = {'known': True, 'active': True,
            'owner': 'synthetic-maintenance-owner', 'hold_id': 'synthetic-hold'}
        native.runner._enter_external_drain()
    def judge(*a, **k):
        if phase == 'during_judge':
            loop.call_soon_threadsafe(close)
        return ('continue', 'more work', False, None, False)
    monkeypatch.setattr('hermes_cli.goals.judge_goal', judge)
    with gateway_ingress(native.runner):
        if phase == 'before_judge':
            close()
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source,
                                                        final_response='exact synthetic checkpoint')
    paused = GoalManager(entry.session_id).state
    assert paused.status == 'paused' and 'maintenance' in paused.paused_reason
    assert paused.turns_used == (0 if phase == 'before_judge' else 1)
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    assert len(rows) == 1, (str(native.db.db_path), native.db.list_meta_prefix('goal'))
    record = json.loads(rows[0][1])
    assert record['owner'] == 'synthetic-maintenance-owner' and record['hold_id'] == 'synthetic-hold'
    assert record['session_id'] == entry.session_id and record['checkpoint']['goal'] == paused.goal
    assert record['continuation_prompt'] == expected_prompt
    assert record['final_response'] == 'exact synthetic checkpoint'
    assert '/goal resume' in paused.paused_reason
    assert not native.adapter._pending_messages and not native.effects
    assert native.db.get_messages(entry.session_id) == history
    # A new SQLite connection reads the exact persisted checkpoint and receipt after restart.
    from hermes_state import SessionDB
    reopened = SessionDB(db_path=native.home / 'state.db')
    try:
        assert json.loads(reopened.get_meta('goal:' + entry.session_id))['status'] == 'paused'
        assert json.loads(reopened.get_meta(rows[0][0])) == record
        assert reopened.get_messages(entry.session_id) == history
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('foreign', ['user-paused', 'maintenance-deferred; owner=other; /goal resume'])
async def test_maintenance_pause_cas_preserves_changed_goal_and_fails_quiescence(native, monkeypatch, foreign):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic concurrent owner')
    mgr = GoalManager(entry.session_id)
    GoalManager(entry.session_id).pause(reason=foreign)
    raw = native.db.get_meta('goal:' + entry.session_id)
    native.runner._enter_external_drain()
    with pytest.raises(RuntimeError, match='existing owner preserved'):
        await native.runner._defer_goal_for_maintenance(mgr, source, mgr.next_continuation_prompt(), 'checkpoint')
    assert native.db.get_meta('goal:' + entry.session_id) == raw
    assert not native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    assert gateway_admission_status(native.runner)['quiescent'] is None
    native.runner._exit_external_drain()
    assert gateway_admission_status(native.runner)['quiescent'] is None


@pytest.mark.asyncio
async def test_unverified_maintenance_write_blocks_quiescence(native, monkeypatch):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic persistence failure')
    native.runner._enter_external_drain()
    def fail(*a, **k):
        raise OSError('synthetic offline write failure')
    monkeypatch.setattr(native.db, '_execute_write', fail)
    with pytest.raises(OSError, match='synthetic offline write failure'):
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
    assert gateway_admission_status(native.runner)['known'] is False
    assert not native.adapter._pending_messages and not native.effects


@pytest.mark.asyncio
async def test_leased_goal_resume_command_enqueues_independent_native_turn(native):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    mgr = GoalManager(entry.session_id)
    mgr.set('synthetic explicit goal resume')
    mgr.pause()
    with gateway_ingress(native.runner):
        await native.runner._handle_goal_command(MessageEvent(text='/goal resume', source=source))
    key = native.runner._session_key_for_source(source)
    assert native.adapter._pending_messages[key].text == GoalManager(entry.session_id).next_continuation_prompt()


@pytest.mark.asyncio
async def test_failed_inherited_adoption_cancels_and_tracks_child_before_first_step(native):
    from gateway.admission import GatewayDraining, adopt_ingress_task
    effects, children = [], []
    async def work():
        effects.append('must not start')
    async def unrelated():
        child = asyncio.create_task(work())
        children.append(child)
        with pytest.raises(GatewayDraining):
            adopt_ingress_task(native.runner, child)
        assert child in native.runner._deferred_agent_cleanup_tasks
        await asyncio.gather(child, return_exceptions=True)
    with gateway_ingress(native.runner):
        task = asyncio.create_task(unrelated())  # Ambient context, deliberately not an owner.
    native.runner._enter_external_drain()
    await asyncio.wait_for(task, 5)
    assert children[0].cancelled() and not effects
    assert gateway_admission_status(native.runner)['quiescent']


@pytest.mark.asyncio
async def test_fence_after_goal_enqueue_defers_before_actual_core_entry(native, monkeypatch):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic queued goal fence race')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(native.runner):
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
    key = native.runner._session_key_for_source(source)
    event = native.adapter._pending_messages.pop(key)
    native.runner._enter_external_drain()
    native.adapter._spawn_drain_task(event, key)
    await asyncio.wait_for(asyncio.gather(*native.adapter._background_tasks), 5)
    assert GoalManager(entry.session_id).state.status == 'paused'
    assert len(native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')) == 1
    assert not native.effects


@pytest.mark.asyncio
async def test_cancel_unstarted_goal_after_fence_retains_durable_handoff(native, monkeypatch):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic unstarted queued goal')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(native.runner):
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
    key = native.runner._session_key_for_source(source)
    pending = native.adapter._pending_messages.pop(key)
    native.runner._enter_external_drain()
    native.adapter._spawn_drain_task(pending, key)
    task = native.adapter._session_tasks[key]
    task.cancel()  # Before coroutine's first step; its finally cannot run.
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.wait_for(asyncio.gather(*getattr(native.runner, '_deferred_agent_cleanup_tasks', ())), 5)
    assert GoalManager(entry.session_id).state.status == 'paused'
    assert len(native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')) == 1
    assert not native.effects


@pytest.mark.asyncio
async def test_maintenance_checkpoint_real_multiplex_home_scope_a_b_a(native):
    import json
    from agent.secret_scope import set_multiplex_active
    from gateway.run import _profile_runtime_scope
    from hermes_state import SessionDB
    a, b = native.home, native.home.parent / 'B'
    b.mkdir()
    (b / 'config.yaml').write_text('security: {tirith_enabled: false}\n')
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    set_multiplex_active(True)
    try:
        for i, home in enumerate((a, b, a)):
            with _profile_runtime_scope(home):
                db = native.runner._session_db._db
                sid = f'synthetic-goal-scope-{i}'
                db.create_session(sid, 'test')
                db.append_message(sid, 'user', f'owned history {i}')
                mgr = GoalManager(sid)
                mgr.set(f'owned goal {i}')
                with gateway_ingress(native.runner):
                    native.runner._enter_external_drain()
                    assert await native.runner._defer_goal_for_maintenance(
                        mgr, source, mgr.next_continuation_prompt(), f'owned result {i}')
                rows = db.list_meta_prefix(f'goal_maintenance_pause:{sid}:')
                record = json.loads(rows[0][1])
                assert record['profile_home'] == str(home.resolve())
                assert native.runner._ingress_barrier.home == a.resolve()
                native.runner._exit_external_drain()
        for home, expected in ((a, {0, 2}), (b, {1})):
            db = SessionDB(db_path=home / 'state.db')
            try:
                records = [json.loads(raw) for _, raw in db.list_meta_prefix('goal_maintenance_pause:')]
                assert {int(r['session_id'].rsplit('-', 1)[1]) for r in records} == expected
            finally:
                db.close()
    finally:
        set_multiplex_active(False)


@pytest.mark.asyncio
async def test_queued_goal_binds_route_before_checkpoint_lookup(native, monkeypatch):
    from gateway.run import _profile_runtime_scope
    from agent.secret_scope import set_multiplex_active
    b = native.home / 'profiles' / 'b'
    b.mkdir(parents=True)
    (b / 'config.yaml').write_text('{}\n')
    native.runner.config.multiplex_profiles = True
    sid = 'synthetic-shared-id'
    with _profile_runtime_scope(native.home):
        GoalManager(sid).set('A goal must remain active')
    with _profile_runtime_scope(b):
        GoalManager(sid).set('B routed goal must pause')
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', profile='b')
    event = MessageEvent(text='exact B continuation', source=source)
    event._goal_admission_checkpoint = {'session_id': sid, 'final_response': 'B prior result'}
    assert native.runner._resolve_profile_home_for_source(source).resolve() == b.resolve()
    set_multiplex_active(True)
    try:
        native.runner._enter_external_drain()
        assert await native.runner._defer_queued_goal_for_maintenance(event)
        with _profile_runtime_scope(native.home):
            assert GoalManager(sid).state.status == 'active'
        with _profile_runtime_scope(b):
            assert GoalManager(sid).state.status == 'paused'
    finally:
        set_multiplex_active(False)


@pytest.mark.asyncio
async def test_fence_after_queued_goal_preflight_cannot_drop_continuation(native, monkeypatch):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic continuation settlement')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(native.runner):
        await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
    key = native.runner._session_key_for_source(source)
    event = native.adapter._pending_messages.pop(key)
    original = native.adapter._run_processing_hook
    async def close_after_preflight(label, *args):
        if label == 'on_processing_start':
            native.runner._enter_external_drain()
        return await original(label, *args)
    monkeypatch.setattr(native.adapter, '_run_processing_hook', close_after_preflight)
    native.finish.set()
    native.adapter._spawn_drain_task(event, key)
    await asyncio.wait_for(asyncio.gather(*native.adapter._background_tasks), 5)
    assert native.effects == [event.text] or GoalManager(entry.session_id).state.status == 'paused'


@pytest.mark.asyncio
async def test_native_startup_goal_continuation_owns_fresh_event_while_http_unready(native, monkeypatch):
    from gateway.admission import GatewayDraining, startup_resume_ingress
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('synthetic startup standing goal')
    key = native.runner._session_key_for_source(source)
    owner = MessageEvent(text='', source=source, internal=True)
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    native.runner._startup_restore_in_progress = True
    try:
        with startup_resume_ingress(native.runner, owner, key):
            await native.runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='partial')
        event = native.adapter._pending_messages.pop(key)
        with pytest.raises(GatewayDraining):
            with gateway_ingress(native.runner):
                raise AssertionError('HTTP borrowed native startup permission')
        native.finish.set()
        native.adapter._spawn_drain_task(event, key)
        await asyncio.wait_for(asyncio.gather(*native.adapter._background_tasks), 5)
        assert native.effects == [event.text]
        assert not native.runner._ingress_barrier._uncertain_work
    finally:
        native.runner._startup_restore_in_progress = False
