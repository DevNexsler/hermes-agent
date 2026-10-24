"""Actual drain publication counts pending/writing generations, not idle writer thread."""
import asyncio
import threading

import pytest

from gateway import status
from gateway.admission import gateway_admission_status
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('pending', [False, True])
@pytest.mark.parametrize('cancel', [False, True])
async def test_native_drain_counts_real_status_writer(native, monkeypatch, pending, cancel):
    runner, loop = native.runner, asyncio.get_running_loop()
    writer = status._get_runtime_status_writer()
    assert await asyncio.to_thread(writer.flush, 5)
    entered, release = asyncio.Event(), threading.Event()
    writes = []

    def write(path, payload):
        assert path.resolve().is_relative_to(native.home.resolve())
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        status._write_json_file(path, payload)  # Actual atomic file write, synthetic home only.
        writes.append(payload)

    monkeypatch.setattr(writer, '_write_fn', write)
    runner._enter_external_drain()
    waiter = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if pending:
            runner._update_runtime_status(gateway_state='draining')
        waiter = asyncio.create_task(status.flush_runtime_status_async(timeout=1))
        await asyncio.sleep(0)
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent']
        release.set()
        assert await asyncio.to_thread(writer.flush, 5)
    finally:
        release.set()
        assert await asyncio.to_thread(writer.flush, 5)
        if waiter is not None:
            await asyncio.gather(waiter, return_exceptions=True)
    assert writes and not native.effects
    assert writer._thread.is_alive()  # Idle permanent thread alone is not in-flight work.
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
async def test_actual_failed_persist_stays_unknown_until_successful_generation(native, monkeypatch):
    writer = status._get_runtime_status_writer()
    assert await asyncio.to_thread(writer.flush, 5)
    original = writer._write_fn

    def fail(_path, _payload):
        raise OSError('synthetic persistence refusal')

    monkeypatch.setattr(writer, '_write_fn', fail)
    try:
        native.runner._enter_external_drain()
        assert not await asyncio.to_thread(writer.flush, 5)
        snapshot = gateway_admission_status(native.runner)
        assert snapshot['known'] is False and snapshot['quiescent'] is None
    finally:
        monkeypatch.setattr(writer, '_write_fn', original)
        assert await asyncio.to_thread(status.write_runtime_status, gateway_state='draining', wait_timeout=5)
    assert gateway_admission_status(native.runner)['quiescent'] is True


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [('_pending', 'unreadable'), ('_writing_generation', True),
                                        ('_completed_generation', 7), ('_submitted_generation', 1)])
async def test_invalid_or_lost_status_generation_never_reports_idle(native, monkeypatch, field, value):
    writer = type(status._get_runtime_status_writer())()  # Actual factory class; no thread/file writes.
    setattr(writer, field, value)
    monkeypatch.setattr(status, '_runtime_status_writer', writer)
    native.runner._ingress_barrier.close()
    snapshot = gateway_admission_status(native.runner)
    assert snapshot['known'] is False and snapshot['quiescent'] is None
