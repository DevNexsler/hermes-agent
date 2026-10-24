"""Cancelling real Base/core dispatch cannot hide its still-running executor worker."""
import asyncio
import threading

import pytest
import pytest_asyncio

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.turn_context import TurnContext
from tests.gateway.test_admission_native_work import native  # noqa: F401 — explicit real-runner fixture


@pytest_asyncio.fixture
async def executor_futures(monkeypatch):
    futures = []
    loop = asyncio.get_running_loop()
    original = loop.run_in_executor
    def capture(*args, **kwargs):
        future = original(*args, **kwargs)
        futures.append(future)
        return future
    monkeypatch.setattr(loop, 'run_in_executor', capture)
    return futures


async def settled(futures):
    await asyncio.wait_for(asyncio.gather(*(asyncio.shield(f) for f in futures), return_exceptions=True), 5)
    await settled_status()


async def settled_status():
    # Drain entry publishes asynchronously; terminal quiescence includes that real generation.
    from gateway.status import flush_runtime_status_async
    assert await flush_runtime_status_async(timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize('method', ['_hm_skill_slash_rewrite', '_is_telegram_topic_root_lobby'])
async def test_real_core_executor_stays_counted_after_base_task_cancel(native, monkeypatch, method, executor_futures):
    entered, exited, release = asyncio.Event(), asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original = getattr(native.runner, method)
    def pending(*args):
        try:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10)
            return original(*args)
        finally:
            loop.call_soon_threadsafe(exited.set)
    monkeypatch.setattr(native.runner, method, pending)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    event = MessageEvent(text='inert owned core work', source=source)
    key = native.runner._session_key_for_source(source)
    with gateway_ingress(native.runner):
        await native.adapter.handle_message(event)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._enter_external_drain()
        await native.adapter.cancel_session_processing(key)
        status = gateway_admission_status(native.runner)
        assert not status['quiescent'] and status['adopted'] >= 1
        assert not native.effects and not exited.is_set()
    finally:
        release.set()
        await asyncio.wait_for(exited.wait(), 5)
        await settled(executor_futures)
    assert not native.effects and gateway_admission_status(native.runner)['quiescent']


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['store', 'db'])
@pytest.mark.parametrize('leased', [True, False])
async def test_real_sqlite_facade_worker_survives_cancel(native, monkeypatch, boundary, executor_futures, leased):
    entered, exited, release = asyncio.Event(), asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    if boundary == 'store':
        target, method = native.runner.session_store, 'get_or_create_session'
        facade = native.runner.async_session_store
        args = (SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'),)
    else:
        target, method = native.db, 'create_session'
        facade = native.runner._session_db
        args = ('synthetic-physical-write', 'test')
    original = getattr(target, method)
    def pending(*args, **kwargs):
        try:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10)
            return original(*args, **kwargs)
        finally:
            loop.call_soon_threadsafe(exited.set)
    monkeypatch.setattr(target, method, pending)
    async def request():
        from contextlib import nullcontext
        with gateway_ingress(native.runner) if leased else nullcontext():
            await getattr(facade, method)(*args)
    task = asyncio.create_task(request())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._enter_external_drain()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        status = gateway_admission_status(native.runner)
        assert not status['quiescent']
        assert status['adopted' if leased else 'active_work'] >= 1
    finally:
        release.set()
        await asyncio.wait_for(exited.wait(), 5)
        await settled(executor_futures)
    assert gateway_admission_status(native.runner)['quiescent']
    if boundary == 'db':
        assert native.db.get_session('synthetic-physical-write')
    else:
        assert native.runner.session_store.get_or_create_session(*args).session_id


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['base', 'runner'])
async def test_orphan_accepted_backlog_prevents_quiescence_without_consuming(native, boundary):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    key = native.runner._session_key_for_source(source)
    accepted = MessageEvent(text='accepted synthetic backlog', source=source)
    if boundary == 'base':
        native.adapter._pending_messages[key] = accepted
    else:
        native.runner._session_state(key).conversation.queued_events.append(accepted)
    native.runner._enter_external_drain()
    status = gateway_admission_status(native.runner)
    assert status['known'] and not status['quiescent'] and status['active_work'] > 0
    if boundary == 'base':
        assert native.adapter._pending_messages[key] is accepted
    else:
        assert native.runner._overflow_queue(key) == [accepted]
    assert not native.effects


@pytest.mark.asyncio
async def test_native_turn_worker_handoff_keeps_physical_owner_after_cancel(native, monkeypatch, executor_futures):
    monkeypatch.setenv('HERMES_AGENT_TIMEOUT', '0')
    entered, exited, release = asyncio.Event(), asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    def inference():
        try:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10)
        finally:
            loop.call_soon_threadsafe(exited.set)
    context = TurnContext(session_id='synthetic-worker')
    with gateway_ingress(native.runner):
        worker = native.runner._run_agent_start_turn_worker(context, inference)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._enter_external_drain()
        worker.executor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker.executor_task
        status = gateway_admission_status(native.runner)
        assert status['adopted'] >= 1 and not status['quiescent']
    finally:
        release.set()
        await asyncio.wait_for(exited.wait(), 5)
        await settled(executor_futures)
    assert gateway_admission_status(native.runner)['quiescent']


@pytest.mark.asyncio
async def test_unleased_pre_session_base_thread_remains_owned_after_cancel(native, monkeypatch, executor_futures):
    entered, exited, release = asyncio.Event(), asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    native.adapter._topic_recovery_fn = lambda *args: None
    original = native.adapter._apply_topic_recovery
    def pending(event):
        try:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10)
            return original(event)
        finally:
            loop.call_soon_threadsafe(exited.set)
    monkeypatch.setattr(native.adapter, '_apply_topic_recovery', pending)
    event = MessageEvent(text='unleased pre-session thread', source=SessionSource(
        platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm'))
    task = asyncio.create_task(native.adapter.handle_message(event))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._enter_external_drain()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        status = gateway_admission_status(native.runner)
        assert not status['quiescent'] and status['active_work'] >= 1
        assert not native.effects
    finally:
        release.set()
        await asyncio.wait_for(exited.wait(), 5)
        await settled(executor_futures)
    assert gateway_admission_status(native.runner)['quiescent']
