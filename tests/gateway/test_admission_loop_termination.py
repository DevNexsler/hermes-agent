"""Actual Base /stop cannot strand a charged loop behind known quiescence."""
import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from gateway.admission import gateway_admission_status
from gateway.run import GatewayRunner
from hermes_cli import loops
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_loop_storage import loop_entry, settle

native = test_admission_native_work.native


def install_real_turn(native, monkeypatch, phase):
    runner = native.runner
    monkeypatch.setattr(runner, '_handle_message_with_agent',
                        GatewayRunner._handle_message_with_agent.__get__(runner))
    entered, proceed, physical = asyncio.Event(), asyncio.Event(), threading.Event()
    event_loop, effects = asyncio.get_running_loop(), []
    async def prepare(event, source, entry, *args):
        if phase == 'preflight':
            entered.set()
            await proceed.wait()
            return 'EXACT retained preflight refusal', []
        return runner._PreparedTurn([], '', event.text, event.text, None, None, entry.session_id), []
    async def emit(name, *args):
        if name == 'agent:start' and phase in ('hook', 'hook_error'):
            entered.set()
            await proceed.wait()
            if phase == 'hook_error':
                effects.append('EXACT inert hook effect')
                raise RuntimeError('EXACT hook outcome ambiguous')
    def work(result):
        event_loop.call_soon_threadsafe(entered.set)
        assert physical.wait(5)
        effects.append('EXACT physical inert worker settled')
        return result
    async def model(**kwargs):
        effects.append('EXACT inert core entry')
        result = {'final_response': 'EXACT inert loop response', 'messages': [], 'completed': True,
                  'api_calls': 1, 'session_id': kwargs['session_id']}
        if phase in ('model_error', 'model_failed'):
            entered.set()
            await proceed.wait()
            effects.append('EXACT inert partial core effect')
            if phase == 'model_error':
                raise RuntimeError('EXACT partial core outcome ambiguous')
            result['failed'] = True
        if phase == 'worker':
            return await runner._run_in_executor_with_context(work, result)
        return result
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, 'hooks', SimpleNamespace(emit=emit))
    monkeypatch.setattr(runner, '_run_agent', model)
    return SimpleNamespace(entered=entered, proceed=proceed, physical=physical, effects=effects)


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['hook', 'worker', 'preflight', 'hook_error', 'model_error', 'model_failed'])
@pytest.mark.parametrize('outcome', ['stop', 'complete'])
async def test_actual_base_stop_retains_loop_attempt_until_accounted(native, monkeypatch, phase, outcome):
    source, entry = loop_entry(native, fired=False)
    key, metadata = entry.session_key, 'loop:' + entry.session_id
    turn = install_real_turn(native, monkeypatch, phase)
    try:
        await native.runner._loop_wakeup_fire_one(entry.session_id, loops.load_loop(entry.session_id), 10**12, set())
        await asyncio.wait_for(turn.entered.wait(), 5)
        charged = native.db.get_meta(metadata)
        assert json.loads(charged)['awaiting_response'] is True
        task = native.adapter._session_tasks[key]
        native.runner._ingress_barrier.close()
        if outcome == 'stop':
            await native.adapter.handle_message(native.runner._synthetic_prompt_event(source, '/stop'))
            await asyncio.gather(task, return_exceptions=True)
            assert task.cancelled()  # Actual Base command cancellation, not merely an interrupt flag.
            if phase == 'worker':
                assert not gateway_admission_status(native.runner)['quiescent']
                assert turn.effects == ['EXACT inert core entry']
        turn.proceed.set()
        turn.physical.set()
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)
        status = gateway_admission_status(native.runner)
        if outcome == 'stop' or phase not in ('hook', 'worker'):
            assert status['known'] is False and not status['quiescent']
            assert native.db.get_meta(metadata) == charged
            assert not native.adapter._pending_messages
        else:
            assert status['known'] is True and status['quiescent']
            assert not loops.load_loop(entry.session_id).awaiting_response
        completed = {'hook': ['EXACT inert core entry'],
                     'worker': ['EXACT inert core entry', 'EXACT physical inert worker settled'],
                     'preflight': [], 'hook_error': ['EXACT inert hook effect'],
                     'model_error': ['EXACT inert core entry', 'EXACT inert partial core effect'],
                     'model_failed': ['EXACT inert core entry', 'EXACT inert partial core effect']}
        cancelled = {**completed, 'hook': [], 'hook_error': [],
                     'model_error': ['EXACT inert core entry'], 'model_failed': ['EXACT inert core entry']}
        assert turn.effects == (cancelled if outcome == 'stop' else completed)[phase]
        reopened = SessionDB(db_path=native.db.db_path)
        try:
            assert reopened.get_meta(metadata) == native.db.get_meta(metadata)
            assert reopened.get_messages(entry.session_id)[0]['content'] == 'EXACT loop continuation inputs'
        finally:
            reopened.close()
    finally:
        turn.proceed.set()
        turn.physical.set()
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('owner', ['base', 'observer'])
async def test_loop_cancellation_before_first_owner_step_is_unknown(native, monkeypatch, owner):
    _source, entry = loop_entry(native, fired=False)
    cancelled = []
    if owner == 'base':
        original = native.adapter._track_session_task
        def track(key, task):
            result = original(key, task)
            task.cancel()
            cancelled.append(task)
            return result
        monkeypatch.setattr(native.adapter, '_track_session_task', track)
    else:
        original = native.runner._track_deferred_agent_worker
        def track(future, agent):
            original(future, agent)
            if isinstance(future, asyncio.Task):
                future.cancel()
                cancelled.append(future)
        monkeypatch.setattr(native.runner, '_track_deferred_agent_worker', track)
    try:
        await native.runner._loop_wakeup_fire_one(entry.session_id, loops.load_loop(entry.session_id), 10**12, set())
        await asyncio.sleep(0)
        native.runner._ingress_barrier.close()
        assert cancelled
        assert not gateway_admission_status(native.runner)['known']
    finally:
        native.finish.set()
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('refusal', ['typed', 'generic', 'partial_typed', 'missing_receipt'])
async def test_loop_refund_requires_positive_pre_effect_refusal(native, monkeypatch, refusal):
    from gateway.admission import GatewayDraining, mark_ingress_effect
    _source, entry = loop_entry(native, fired=False)
    effects = []
    async def admission(event):
        if refusal == 'partial_typed':
            mark_ingress_effect(native.runner)
            effects.append('EXACT inert partial admission effect')
        if refusal in ('typed', 'partial_typed'):
            raise GatewayDraining('EXACT synthetic refusal', pre_effect=True, reason='busy_backlog')
        if refusal == 'generic':
            raise RuntimeError('EXACT ambiguous dispatch')
        event._gateway_accepted = False  # Explicit missing-receipt test boundary.
    monkeypatch.setattr(native.adapter, 'handle_message', admission)
    if refusal == 'typed':
        await native.runner._loop_wakeup_fire_one(entry.session_id, loops.load_loop(entry.session_id), 10**12, set())
    else:
        with pytest.raises(RuntimeError) as result:
            await native.runner._loop_wakeup_fire_one(entry.session_id, loops.load_loop(entry.session_id), 10**12, set())
        assert not isinstance(result.value, GatewayDraining)
    await settle(native)
    native.runner._ingress_barrier.close()
    state = loops.load_loop(entry.session_id)
    assert gateway_admission_status(native.runner)['known'] is (refusal == 'typed')
    assert state.awaiting_response is (refusal != 'typed')
    assert state.ticks_fired == int(refusal != 'typed')
    assert effects == (['EXACT inert partial admission effect'] if refusal == 'partial_typed' else [])
