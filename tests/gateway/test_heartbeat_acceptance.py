"""An adapter slot is not proof a heartbeat reached execution."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.admission import initialize_gateway_admission
from gateway.run import GatewayRunner
from gateway.session import SessionSource, build_session_key
from hermes_cli.heartbeat import HeartbeatManager, HeartbeatState, save_heartbeat
from evals.heartbeat_idle_wire import WireAdapter


async def settle_storage(runner):
    for _ in range(200):
        workers = list(getattr(runner, '_deferred_agent_workers', {}))
        if not workers:
            return
        await asyncio.gather(*workers, return_exceptions=True)
        await asyncio.sleep(0)
    raise AssertionError('heartbeat persistence did not settle')


@pytest.mark.asyncio
async def test_cancelled_admission_is_refunded_but_started_execution_is_not():
    runner = object.__new__(GatewayRunner)
    runner._running, runner._draining = True, False
    initialize_gateway_admission(runner)
    runner._running_agents = {}
    runner._run_in_executor_with_context = asyncio.to_thread
    adapter = WireAdapter(PlatformConfig(enabled=True, typing_indicator=False), Platform.TELEGRAM)
    adapter.wire = []
    adapter.gateway_runner = runner
    runner._delivery_adapter_for = lambda source: adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='42', user_id='42')
    key = build_session_key(source)
    watch = {key: (source, 'cancel-test')}
    started = asyncio.Event()

    async def model(**kwargs):
        started.set()
        await asyncio.Event().wait()

    runner._run_agent = model
    runner.hooks = SimpleNamespace(emit=AsyncMock())
    runner._hmwa_resolve_session = AsyncMock(return_value=(source, SimpleNamespace(session_id="cancel-test"), key))
    runner._hmwa_prepare_turn = AsyncMock(return_value=(
        runner._PreparedTurn([], "", "check", "check", None, None), {}))
    runner._clear_session_env = lambda tokens: None

    async def handler(event):
        return await runner._handle_message_with_agent(event, source, key, 1)

    adapter.set_message_handler(handler)
    await runner._warm_goals_session_db('test')
    execute = asyncio.Event()
    original_process = adapter._process_message_background
    async def gated_process(*args):
        await execute.wait()
        return await original_process(*args)
    adapter._process_message_background = gated_process
    for cancel_before_start in (True, False):
        if not cancel_before_start:
            execute.set()
        save_heartbeat('cancel-test', HeartbeatState(prompt='check', interval_seconds=60, created_at=1))
        await runner._heartbeat_poll_once(watch)
        task = adapter._session_tasks[key]
        if not cancel_before_start:
            await asyncio.wait_for(started.wait(), 2)
        await adapter.cancel_session_processing(key)
        await asyncio.gather(task, return_exceptions=True)
        await settle_storage(runner)
        assert HeartbeatManager('cancel-test').state.fire_count == int(not cancel_before_start)


@pytest.mark.asyncio
@pytest.mark.parametrize('replacement', [False, True])
async def test_missing_entry_retains_tick_unknown_without_overwriting_replacement(replacement):
    runner = object.__new__(GatewayRunner)
    runner._running, runner._draining = True, False
    initialize_gateway_admission(runner)
    runner._running_agents = {}
    runner._run_in_executor_with_context = asyncio.to_thread
    adapter = WireAdapter(PlatformConfig(enabled=True, typing_indicator=False), Platform.TELEGRAM)
    adapter.wire = []
    adapter.gateway_runner = runner
    runner._delivery_adapter_for = lambda source: adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='42', user_id='42')
    key = build_session_key(source)
    watch = {key: (source, 'rejected-test')}
    await runner._warm_goals_session_db('test')

    async def denied(event):
        if replacement:
            HeartbeatManager('rejected-test').set('replacement', 120)
        return 'not authorized'

    adapter.set_message_handler(denied)
    save_heartbeat('rejected-test', HeartbeatState(prompt='check', interval_seconds=60, created_at=1))
    await runner._heartbeat_poll_once(watch)
    await asyncio.gather(*adapter._background_tasks)
    await settle_storage(runner)
    state = HeartbeatManager('rejected-test').state
    assert state.prompt == ('replacement' if replacement else 'check')
    assert state.fire_count == (0 if replacement else 1)
    assert runner._ingress_barrier._uncertain_work
