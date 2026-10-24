"""A missing core entry never refunds a heartbeat after an attempted hook effect."""
import asyncio
import threading

import pytest

from gateway.admission import gateway_admission_status
from gateway.hooks import HookRegistry
from hermes_cli import heartbeat
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_loop_storage import loop_entry, settle
from tests.gateway.test_admission_loop_termination import install_real_turn

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('physical', [False, True])
@pytest.mark.parametrize('outcome', ['stop', 'error', 'complete'])
async def test_actual_hook_effect_before_core_retains_charged_heartbeat(native, monkeypatch, outcome, physical):
    source, entry = loop_entry(native, fired=False)
    sid, key = entry.session_id, entry.session_key
    heartbeat.save_heartbeat(sid, heartbeat.HeartbeatState('EXACT hook heartbeat', 60, created_at=1))
    turn = install_real_turn(native, monkeypatch, 'hook')
    registry = HookRegistry()
    entered, release, effects = asyncio.Event(), asyncio.Event(), []
    physical_release = threading.Event()
    loop = asyncio.get_running_loop()

    def work():
        loop.call_soon_threadsafe(entered.set)
        assert physical_release.wait(5)
        effects.append('EXACT physical hook settled')

    async def hook(event, context):
        assert event == 'agent:start' and context['session_id'] == sid
        effects.append('EXACT inert hook effect before await')
        if physical:
            await native.runner._run_in_executor_with_context(work)
        else:
            entered.set()
            await release.wait()
        if outcome == 'error':
            raise asyncio.CancelledError('EXACT hook cancellation after effect')

    registry._handlers['agent:start'] = [hook]  # Real registry/dispatch, inert authenticated hook.
    monkeypatch.setattr(native.runner, 'hooks', registry)
    watch = {key: (source, sid)}
    try:
        await native.runner._heartbeat_poll_once(watch)
        await asyncio.wait_for(entered.wait(), 5)
        charged = native.db.get_meta('heartbeat:' + sid)
        assert heartbeat.load_heartbeat(sid).fire_count == 1
        task = native.adapter._session_tasks[key]
        native.runner._ingress_barrier.close()
        assert not gateway_admission_status(native.runner)['quiescent']
        if outcome == 'stop':
            await native.adapter.handle_message(native.runner._synthetic_prompt_event(source, '/stop'))
            await asyncio.gather(task, return_exceptions=True)
            assert task.cancelled()
            if physical:
                assert not gateway_admission_status(native.runner)['quiescent']
                assert effects == ['EXACT inert hook effect before await']
        release.set()
        physical_release.set()
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)
        expected = ['EXACT inert hook effect before await'] + (['EXACT physical hook settled'] if physical else [])
        assert effects == expected
        assert native.db.get_meta('heartbeat:' + sid) == charged
        status = gateway_admission_status(native.runner)
        assert status['known'] is (outcome == 'complete')
        assert bool(status['quiescent']) is (outcome == 'complete')
        assert turn.effects == (['EXACT inert core entry'] if outcome == 'complete' else [])
        reopened = SessionDB(db_path=native.db.db_path)
        try:
            assert reopened.get_meta('heartbeat:' + sid) == charged
            assert reopened.get_messages(sid)[0]['content'] == 'EXACT loop continuation inputs'
        finally:
            reopened.close()
        native.runner._ingress_barrier.reopen()
        await native.runner._heartbeat_poll_once(watch)
        await settle(native)
        assert effects == expected
    finally:
        release.set()
        physical_release.set()
        turn.proceed.set()
        turn.physical.set()
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('refusal', ['busy', 'cancel_before_effect'])
async def test_real_base_no_effect_heartbeat_refund_remains_supported(native, monkeypatch, refusal):
    source, entry = loop_entry(native, fired=False)
    sid, key = entry.session_id, entry.session_key
    heartbeat.save_heartbeat(sid, heartbeat.HeartbeatState('EXACT proven unexecuted heartbeat', 60, created_at=1.0))
    before = native.db.get_meta('heartbeat:' + sid)
    if refusal == 'busy':
        monkeypatch.setattr(native.runner, '_active_session_limit_message', lambda key: 'EXACT pre-effect busy')
    else:
        gate = asyncio.Event()
        original = native.adapter._process_message_background
        async def before_effect(*args):
            await gate.wait()
            return await original(*args)
        monkeypatch.setattr(native.adapter, '_process_message_background', before_effect)
    await native.runner._heartbeat_poll_once({key: (source, sid)})
    if refusal == 'cancel_before_effect':
        task = native.adapter._session_tasks[key]
        await native.adapter.handle_message(native.runner._synthetic_prompt_event(source, '/stop'))
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
    await settle(native)
    native.runner._ingress_barrier.close()
    status = gateway_admission_status(native.runner)
    assert status['known'] is True and status['quiescent']
    assert native.db.get_meta('heartbeat:' + sid) == before
    assert not native.effects and not native.adapter._pending_messages


@pytest.mark.asyncio
async def test_cancelled_before_start_heartbeat_monitor_retains_charge_unknown(native, monkeypatch):
    source, entry = loop_entry(native, fired=False)
    sid, key = entry.session_id, entry.session_key
    heartbeat.save_heartbeat(sid, heartbeat.HeartbeatState('EXACT cancelled observer', 60, created_at=1.0))
    gate = asyncio.Event()
    original_process = native.adapter._process_message_background
    async def before_effect(*args):
        await gate.wait()
        return await original_process(*args)
    monkeypatch.setattr(native.adapter, '_process_message_background', before_effect)
    original_track, cancelled = native.runner._track_deferred_agent_worker, []
    def cancel_monitor(future, agent):
        if isinstance(future, asyncio.Task):
            cancelled.append(future)
            future.cancel()
        return original_track(future, agent)
    monkeypatch.setattr(native.runner, '_track_deferred_agent_worker', cancel_monitor)
    await native.runner._heartbeat_poll_once({key: (source, sid)})
    assert len(cancelled) == 1
    charged = native.db.get_meta('heartbeat:' + sid)
    task = native.adapter._session_tasks[key]
    native.runner._ingress_barrier.close()
    await native.adapter.handle_message(native.runner._synthetic_prompt_event(source, '/stop'))
    await asyncio.gather(task, return_exceptions=True)
    await settle(native)
    assert not gateway_admission_status(native.runner)['known']
    assert native.db.get_meta('heartbeat:' + sid) == charged
    assert not native.effects
