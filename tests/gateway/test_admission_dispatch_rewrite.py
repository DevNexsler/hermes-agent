"""Supported text rewrites retain exact admission and native execution ownership."""
import asyncio

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress, startup_resume_ingress, startup_resume_admitted
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from hermes_cli import active_sessions as active
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
async def test_real_base_hook_rewrite_enters_core_once_with_reserved_capacity(native, monkeypatch):
    runner, adapter = native.runner, native.adapter
    runner.config.max_concurrent_sessions = 1
    original = MessageEvent(text='original transport input', source=SessionSource(
        platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'))
    async def rewrite(name, **kwargs):
        assert name == 'pre_gateway_dispatch' and kwargs['event'] is original
        return [{'action': 'rewrite', 'text': 'rewritten model input'}]
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', rewrite)
    with gateway_ingress(runner):
        await adapter.handle_message(original)
    assert original._gateway_accepted is True
    try:
        await asyncio.wait_for(native.entered.wait(), 2)
        assert native.effects == ['rewritten model input']
        assert original.text == 'original transport input'
        assert len(active.active_session_registry_snapshot(native.home, strict=True)) == 1
        runner._enter_external_drain()
        assert not gateway_admission_status(runner)['quiescent']
    finally:
        native.finish.set()
        await asyncio.gather(*adapter._background_tasks)
    assert active.active_session_registry_snapshot(native.home, strict=True) == []
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
async def test_hook_rewrite_retains_process_local_checkpoint_and_exact_startup_owner(native, monkeypatch):
    runner = native.runner
    event = MessageEvent(text='original', source=SessionSource(
        platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'))
    checkpoint = {'session_id': 'synthetic-goal', 'goal_state': 'exact goal identity'}
    event._goal_admission_checkpoint = checkpoint
    event._heartbeat_session_id = 'synthetic-owner'
    event._notification_reply_muted = True
    event._gateway_accepted = True
    async def rewrite(*args, **kwargs):
        return [{'action': 'rewrite', 'text': 'rewritten'}]
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', rewrite)
    key = runner._session_key_for_source(event.source)
    runner._startup_restore_in_progress = True
    try:
        with startup_resume_ingress(runner, event, key):
            rewritten = await runner._hm_pre_gateway_dispatch_hook(event, event.source)
            assert rewritten is not event and event.text == 'original'
            assert rewritten._goal_admission_checkpoint is checkpoint
            assert rewritten._heartbeat_session_id == 'synthetic-owner'
            assert rewritten._notification_reply_muted is True and rewritten._gateway_accepted is True
            assert startup_resume_admitted(runner, rewritten, key)
            assert not startup_resume_admitted(runner, event, key)
    finally:
        runner._startup_restore_in_progress = False


@pytest.mark.asyncio
@pytest.mark.parametrize('leased', [True, False])
async def test_rewritten_actual_turn_marks_original_execution_before_cancel(native, monkeypatch, leased):
    from contextlib import nullcontext
    from gateway.run import GatewayRunner
    runner, adapter = native.runner, native.adapter
    event = MessageEvent(text='original', source=SessionSource(
        platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'))
    async def rewrite(*args, **kwargs):
        return [{'action': 'rewrite', 'text': 'rewritten'}]
    async def prepare(*args):
        return runner._PreparedTurn([], '', 'rewritten', 'rewritten', None, None), {}
    async def inference(**kwargs):
        native.effects.append(kwargs['message'])
        native.entered.set()
        await native.finish.wait()
        raise asyncio.CancelledError  # After physical entry; avoid unrelated response rendering.
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', rewrite)
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', inference)
    with gateway_ingress(runner) if leased else nullcontext():
        await adapter.handle_message(event)
    try:
        await asyncio.wait_for(native.entered.wait(), 2)
        assert event._heartbeat_execution_started is True
        assert event.text == 'original' and native.effects == ['rewritten']
    finally:
        native.finish.set()
        await asyncio.gather(*adapter._background_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_unowned_hook_cannot_transfer_another_live_ingress_reservation(native, monkeypatch):
    from gateway.admission_capacity import reserve_ingress_capacity, take_ingress_capacity
    runner = native.runner
    event = MessageEvent(text='owner', source=SessionSource(
        platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'))
    key = runner._session_key_for_source(event.source)
    async def rewrite(*args, **kwargs):
        return [{'action': 'rewrite', 'text': 'unowned clone'}]
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', rewrite)
    with gateway_ingress(runner):
        reserve_ingress_capacity(runner, event, key)
        async def unadopted():
            cloned = await runner._hm_pre_gateway_dispatch_hook(event, event.source)
            assert take_ingress_capacity(runner, cloned, key)[0] is None
        await asyncio.create_task(unadopted())
        unrelated = MessageEvent(text='unrelated', source=event.source)
        assert take_ingress_capacity(runner, unrelated, key)[0] is None
        slot, refusal = take_ingress_capacity(runner, event, key)
        assert slot is not None and refusal is None
        slot.release()
    assert active.active_session_registry_snapshot(native.home, strict=True) == []


@pytest.mark.asyncio
async def test_rewritten_goal_cancel_after_real_core_entry_never_pauses_executing_goal(native, monkeypatch):
    from gateway.run import GatewayRunner
    from hermes_cli.goals import GoalManager
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    GoalManager(entry.session_id).set('exact native goal')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='exact prior result')
    key = runner._session_key_for_source(source)
    event = adapter._pending_messages.pop(key)
    before = native.db.get_meta('goal:' + entry.session_id)
    async def rewrite(*args, **kwargs):
        return [{'action': 'rewrite', 'text': 'rewritten goal'}]
    async def prepare(*args):
        return runner._PreparedTurn([], '', 'rewritten goal', 'rewritten goal', None, None), {}
    async def inference(**kwargs):
        native.effects.append(kwargs['message'])
        native.entered.set()
        await native.finish.wait()
        raise asyncio.CancelledError
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', rewrite)
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', inference)
    adapter._spawn_drain_task(event, key)
    try:
        await asyncio.wait_for(native.entered.wait(), 2)
        assert event._heartbeat_execution_started is True
        runner._enter_external_drain()
        await adapter.cancel_session_processing(key)
        await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
        assert native.db.get_meta('goal:' + entry.session_id) == before
        assert not native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
        assert native.effects == ['rewritten goal']
    finally:
        native.finish.set()
        await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
