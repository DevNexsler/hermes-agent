"""Restore remains native-owned; leased arrivals cannot displace previously accepted backlog."""
import asyncio
from datetime import datetime

import pytest

from gateway.admission import GatewayDraining, gateway_ingress, initialize_gateway_admission
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import _AGENT_PENDING_SENTINEL
from gateway.session import SessionEntry
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


@pytest.mark.asyncio
async def test_real_startup_schedule_dispatch_finish_allowed_but_external_ingress_stays_closed(tmp_path):
    runner, adapter = make_restart_runner(admission_home=tmp_path)
    adapter.gateway_runner = runner
    adapter.set_message_handler(runner._handle_message)
    runner._startup_restore_in_progress = True
    source = make_restart_source()
    key = runner._session_key_for_source(source)
    entry = SessionEntry(session_key=key, session_id='offline-resume', created_at=datetime.now(),
        updated_at=datetime.now(), origin=source, platform=source.platform, resume_pending=True,
        resume_reason='restart_timeout', last_resume_marked_at=datetime.now())
    runner.session_store._entries = {key: entry}
    effects = []
    async def inert_agent(event, *_args):
        effects.append(event.internal)
    runner._handle_message_with_agent = inert_agent
    with pytest.raises(GatewayDraining):
        with gateway_ingress(runner):
            pytest.fail('HTTP became ready during restore')
    assert runner._schedule_resume_pending_sessions() == 1
    await asyncio.gather(*runner._startup_restore_tasks)
    assert effects == [True]
    assert not runner._is_session_running(key)
    await runner._finish_startup_restore()
    assert not runner._startup_restore_in_progress
    with gateway_ingress(runner):
        pass
    # Same lifecycle path remains refused once external closure is engaged.
    runner._startup_restore_in_progress = True
    runner._enter_external_drain()
    assert runner._schedule_resume_pending_sessions() == 1
    await asyncio.gather(*runner._startup_restore_tasks)
    assert effects == [True] and not runner._is_session_running(key)
    assert entry.resume_pending


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sentinel', 'photo', 'orphan'])
async def test_leased_base_refuses_priority_or_orphan_backlog_before_ack_and_preserves_it(tmp_path, kind):
    runner, adapter = make_restart_runner(admission_home=tmp_path)
    adapter.gateway_runner = runner
    adapter.set_message_handler(runner._handle_message)
    source = make_restart_source()
    key = runner._session_key_for_source(source)
    old = MessageEvent(text='accepted-before', source=source)
    if kind == 'orphan':
        runner._session_state(key).conversation.queued_events.append(old)
    else:
        runner._session_state(key).turn.agent = _AGENT_PENDING_SENTINEL
    event = MessageEvent(text='retry', source=source,
                         message_type=MessageType.PHOTO if kind == 'photo' else MessageType.TEXT)
    with gateway_ingress(runner):
        with pytest.raises(GatewayDraining):
            await adapter.handle_message(event)
    assert not event._gateway_accepted and not adapter._background_tasks
    assert not adapter._pending_messages
    if kind == 'orphan':
        assert runner._overflow_queue(key) == [old]
    else:
        assert runner._peek_session_state(key).turn.agent is _AGENT_PENDING_SENTINEL


@pytest.mark.asyncio
@pytest.mark.parametrize('photo', [False, True])
async def test_real_core_priority_merge_propagates_typed_refusal(tmp_path, photo):
    runner, adapter = make_restart_runner(admission_home=tmp_path)
    source = make_restart_source()
    key = runner._session_key_for_source(source)
    runner._session_state(key).turn.agent = _AGENT_PENDING_SENTINEL
    event = MessageEvent(text='retry', source=source,
                         message_type=MessageType.PHOTO if photo else MessageType.TEXT)
    with gateway_ingress(runner):
        with pytest.raises(GatewayDraining):
            await runner._handle_message(event)
    assert not adapter._pending_messages
    assert runner._peek_session_state(key).turn.agent is _AGENT_PENDING_SENTINEL
