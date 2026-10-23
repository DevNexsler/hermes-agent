"""Native producers refund refused ingress, and strict API readback never hides an error."""

import asyncio

import pytest

from gateway.admission import (
    GatewayDraining, gateway_admission_status, gateway_ingress, initialize_gateway_admission,
)
from gateway.config import Platform
from gateway.platforms.api_server import APIServerAdapter
from gateway.platforms.event import MessageEvent
from gateway.wake import WakeNotAccepted, admit_internal_event
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


@pytest.mark.asyncio
@pytest.mark.parametrize('runner_busy_handler', [False, True])
async def test_native_internal_refund_and_leased_busy_refusal_never_queue(tmp_path, runner_busy_handler):
    runner, adapter = make_restart_runner(admission_home=tmp_path)
    adapter.gateway_runner = runner
    adapter.set_message_handler(runner._handle_message)
    if not runner_busy_handler:
        adapter.set_busy_session_handler(None)
    started, finish = asyncio.Event(), asyncio.Event()
    effects = []

    async def inert_agent(event, *_args):
        effects.append(event.text)
        started.set()
        await finish.wait()
        return None

    runner._handle_message_with_agent = inert_agent
    first = MessageEvent(text='first', source=make_restart_source(), internal=True)
    try:
        await admit_internal_event(adapter, first)
        await asyncio.wait_for(started.wait(), 5)
        with gateway_ingress(runner):
            with pytest.raises(GatewayDraining):
                await adapter.handle_message(MessageEvent(text='busy', source=make_restart_source()))
        assert not adapter._pending_messages and not runner._queued_events
        assert effects == ['first']
        runner._enter_external_drain()
        refused = MessageEvent(text='refunded', source=make_restart_source(), internal=True)
        with pytest.raises(WakeNotAccepted):
            await admit_internal_event(adapter, refused)
        assert not refused._gateway_accepted and not adapter._pending_messages
    finally:
        finish.set()
        await asyncio.gather(*adapter._background_tasks)
    assert gateway_admission_status(runner)['quiescent'] is True
    assert effects == ['first']


def test_present_api_adapter_with_unreadable_or_missing_counter_is_unknown(tmp_path):
    runner, _adapter = make_restart_runner(admission_home=tmp_path)
    runner._enter_external_drain()
    api = object.__new__(APIServerAdapter)
    api._pending_agent_requests = 1
    api._inflight_agent_runs = 0

    class UnreadableTasks:
        def values(self):
            raise RuntimeError('offline unreadable task registry')

    api._active_run_tasks = UnreadableTasks()
    runner.adapters[Platform.API_SERVER] = api
    assert api.active_agent_work_count() == 0  # legacy best-effort shutdown behavior
    with pytest.raises(RuntimeError):
        api.active_agent_work_count(strict=True)
    assert gateway_admission_status(runner)['quiescent'] is None
    api._active_run_tasks = {}
    api._session_tasks = {}
    status = gateway_admission_status(runner)
    assert status['known'] and status['active_work'] >= 1 and not status['quiescent']
    api._pending_agent_requests = 0
    assert gateway_admission_status(runner)['quiescent'] is True
    runner.adapters[Platform.API_SERVER] = object()
    assert gateway_admission_status(runner)['quiescent'] is None


@pytest.mark.parametrize('reported', [
    {'known': False}, {}, {'known': True, 'active_tasks': -1, 'active_threads': 0},
    {'known': True, 'active_tasks': False, 'active_threads': 0},
])
def test_declared_observer_contract_never_converts_unknown_to_idle(tmp_path, reported):
    runner, adapter = make_restart_runner(admission_home=tmp_path)
    runner._enter_external_drain()
    assert gateway_admission_status(runner)['quiescent'] is True
    adapter.admission_observer_status = lambda: reported
    assert gateway_admission_status(runner)['quiescent'] is None
    adapter.admission_observer_status = lambda: {'known': True, 'active_tasks': 0, 'active_threads': 1}
    assert gateway_admission_status(runner)['observers'] == 1
    assert not gateway_admission_status(runner)['quiescent']
    adapter.admission_observer_status = lambda: {'known': True, 'active_tasks': 0, 'active_threads': 0}
    assert gateway_admission_status(runner)['quiescent'] is True


def test_corrupt_lazy_deferred_registry_is_unknown(tmp_path):
    runner, _adapter = make_restart_runner(admission_home=tmp_path)
    runner._enter_external_drain()
    assert gateway_admission_status(runner)['quiescent'] is True
    runner._deferred_agent_workers = None
    assert gateway_admission_status(runner)['quiescent'] is None
