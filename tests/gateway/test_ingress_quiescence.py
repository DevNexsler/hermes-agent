"""HTTP admission stays visible before a session exists and through background handoff."""

import asyncio
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web
import pytest

from gateway.admission import (
    GatewayDraining, gateway_admission_status, gateway_ingress, initialize_gateway_admission,
)
from gateway.platforms.event import MessageEvent
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


@asynccontextmanager
async def _server(handler):
    app = web.Application()
    app.router.add_post('/wake', handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    try:
        async with aiohttp.ClientSession() as client:
            yield client, f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/wake'
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_http_prefence_work_survives_drain_and_handoff_without_false_ack(tmp_path):
    runner, adapter = make_restart_runner()
    initialize_gateway_admission(runner, home=tmp_path)
    adapter.gateway_runner = runner
    entered, release, background, finish = (asyncio.Event() for _ in range(4))
    effects = []

    async def process(event, session_key):
        from gateway.admission import ingress_admitted
        assert ingress_admitted(runner)
        # Use the real core pipeline through its drain gate; inference itself stays inert.
        runner._handle_message_with_agent = inert_agent
        result = await runner._handle_message(event)
        assert result == 'offline result'
        background.set()
        await finish.wait()

    async def inert_agent(*args):
        return 'offline result'

    adapter._process_message_background = process
    adapter.set_message_handler(runner._handle_message)

    async def handler(request):
        try:
            with gateway_ingress(runner):
                entered.set()
                await release.wait()  # body/SQLite/journal wait before handle_message
                await request.json()
                event = MessageEvent(text='offline fixture', source=make_restart_source())
                await adapter.handle_message(event)
                assert event._gateway_accepted
                effects.append('accepted')
                return web.json_response({'status': 'accepted'}, status=202)
        except GatewayDraining as exc:
            return web.json_response({'status': 'unavailable'}, status=exc.status, headers=exc.headers)

    async with _server(handler) as (client, url):
        first = asyncio.create_task(client.post(url, json={'delivery_id': 'same-id'}))
        await asyncio.wait_for(entered.wait(), 5)
        assert not adapter._active_sessions
        assert runner._active_work_count() > 0
        runner._enter_external_drain()
        status = gateway_admission_status(runner)
        assert status['closed'] and status['ingress'] == 1 and not status['quiescent']
        async with client.post(url, json={'delivery_id': 'same-id'}) as response:
            assert response.status == 503 and response.headers['Retry-After']
        assert effects == []
        release.set()
        response = await asyncio.wait_for(first, 5)
        assert response.status == 202
        response.release()
        await asyncio.wait_for(background.wait(), 5)
        status = gateway_admission_status(runner)
        assert status['ingress'] == 0 and status['adopted'] == 1 and not status['quiescent']
        finish.set()
        await asyncio.gather(*adapter._background_tasks)
        await asyncio.sleep(0)  # run task completion callbacks
        assert gateway_admission_status(runner)['quiescent'] is True
        runner._exit_external_drain()
        async with client.post(url, json={'delivery_id': 'same-id'}) as response:
            assert response.status == 202
        await asyncio.gather(*adapter._background_tasks)
    assert effects == ['accepted', 'accepted']


@pytest.mark.asyncio
async def test_cancelled_handler_and_unready_capability_fail_closed(tmp_path):
    runner, _adapter = make_restart_runner()
    with pytest.raises(GatewayDraining):
        with gateway_ingress(runner):
            pytest.fail('missing native capability accepted work')
    assert gateway_admission_status(runner)['quiescent'] is None
    initialize_gateway_admission(runner, home=tmp_path)
    entered, never = asyncio.Event(), asyncio.Event()

    async def pending():
        with gateway_ingress(runner):
            entered.set()
            await never.wait()

    task = asyncio.create_task(pending())
    await asyncio.wait_for(entered.wait(), 5)
    runner._enter_external_drain()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert gateway_admission_status(runner)['quiescent'] is True
    runner._running = False
    runner._exit_external_drain()
    with pytest.raises(GatewayDraining):
        with gateway_ingress(runner):
            pytest.fail('stopping runtime accepted work')
