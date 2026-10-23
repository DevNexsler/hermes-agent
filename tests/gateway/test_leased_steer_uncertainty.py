"""Leased steer failures never become a pre-effect refusal after possible effects."""
from types import SimpleNamespace

import pytest

from gateway.admission import GatewayDraining, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from tests.gateway.restart_test_helpers import install_native_admission


@pytest.mark.asyncio
@pytest.mark.parametrize('error_type', ['generic', 'typed'])
@pytest.mark.parametrize('route', ['command', 'busy', 'priority'])
@pytest.mark.parametrize('target', ['parent', 'child'])
async def test_leased_partial_steer_failure_propagates_unknown(tmp_path, route, target, error_type):
    runner = object.__new__(GatewayRunner)
    install_native_admission(runner, home=tmp_path)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='synthetic', user_id='synthetic', chat_type='dm')
    event = MessageEvent(text='/steer synthetic' if route == 'command' else 'synthetic', source=source)
    effects = []
    def partial(text):
        effects.append(text)
        if error_type == 'typed':
            raise GatewayDraining('unknown after synthetic effect marker', pre_effect=True)
        raise RuntimeError('unknown after synthetic effect marker')
    agent = SimpleNamespace(steer=partial if target == 'parent' else lambda text: False)
    if target == 'child':
        agent._active_children = [SimpleNamespace(steer=partial)]
    runner._session_state('synthetic').turn.agent = agent
    with gateway_ingress(runner):
        with pytest.raises(RuntimeError, match='unknown after synthetic effect marker'):
            if route == 'command':
                await runner._busy_steer_command(event, 'synthetic', source)
            elif route == 'priority':
                runner._hm_busy_steer(event, agent, 'synthetic')
            else:
                await runner._resolve_busy_steer_or_redirect(event, 'synthetic', 'steer', agent)
    assert len(effects) == 1
    assert not runner._queued_events


@pytest.mark.asyncio
async def test_unleased_partial_child_error_keeps_ordinary_human_contract():
    runner = object.__new__(GatewayRunner)
    effects = []
    def partial(text):
        effects.append(text)
        raise RuntimeError('legacy human steer error')
    agent = SimpleNamespace(steer=lambda text: False, _active_children=[SimpleNamespace(steer=partial)])
    assert runner._steer_running_agent(agent, 'human') is False
    assert effects == ['human']


@pytest.mark.asyncio
async def test_refusal_proof_only_before_effects(tmp_path):
    from gateway.admission import GatewayDraining, mark_ingress_effect, refuse_queued_ingress
    runner = object.__new__(GatewayRunner)
    install_native_admission(runner, home=tmp_path)
    with gateway_ingress(runner):
        with pytest.raises(GatewayDraining) as before:
            refuse_queued_ingress(runner)
        assert before.value.pre_effect is True
        mark_ingress_effect(runner)
        with pytest.raises(GatewayDraining) as after:
            refuse_queued_ingress(runner)
        assert after.value.pre_effect is False
    runner._ingress_barrier.close()
    with pytest.raises(GatewayDraining) as closed:
        with gateway_ingress(runner):
            raise AssertionError('closed admission cannot enter')
    assert closed.value.pre_effect is True and closed.value.reason == 'closed_or_unready'


@pytest.mark.asyncio
async def test_actual_busy_notice_prevents_later_positive_refusal(tmp_path):
    from gateway.admission import refuse_queued_ingress
    from tests.gateway.test_busy_wake_admission import WakeAdapter
    runner = object.__new__(GatewayRunner)
    install_native_admission(runner, home=tmp_path)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='synthetic', user_id='synthetic', chat_type='dm')
    event = MessageEvent(text='synthetic', source=source)
    adapter = WakeAdapter()
    with gateway_ingress(runner):
        await runner._send_busy_reply(event, adapter, 'synthetic notice')
        with pytest.raises(GatewayDraining) as refusal:
            refuse_queued_ingress(runner)
        assert refusal.value.pre_effect is False
    assert adapter.sent == ['synthetic notice']


@pytest.mark.asyncio
async def test_effect_mark_precedes_call_and_forged_nested_proof_is_downgraded(tmp_path):
    from gateway.admission import mark_ingress_effect, refuse_queued_ingress
    runner = object.__new__(GatewayRunner)
    install_native_admission(runner, home=tmp_path)
    def decline(text):
        # Native mark must precede even a declined callback; no inferred no-effect.
        with pytest.raises(GatewayDraining) as refusal:
            refuse_queued_ingress(runner)
        assert refusal.value.pre_effect is False
        return False
    with pytest.raises(GatewayDraining) as forged:
        with gateway_ingress(runner):
            assert runner._steer_running_agent(SimpleNamespace(steer=decline), 'synthetic') is False
            raise GatewayDraining('callee forged safe-looking proof', pre_effect=True)
    assert forged.value.pre_effect is False
    with gateway_ingress(runner):
        mark_ingress_effect(runner)
        runner._ingress_barrier.close()
        with pytest.raises(GatewayDraining) as nested:
            with gateway_ingress(runner):
                raise AssertionError('closed nested lease entered')
        assert nested.value.pre_effect is False


@pytest.mark.asyncio
async def test_cancellation_at_effect_boundary_never_restores_positive_proof(tmp_path):
    import asyncio
    from gateway.admission import refuse_queued_ingress
    runner = object.__new__(GatewayRunner)
    install_native_admission(runner, home=tmp_path)
    def cancelled(text):
        raise asyncio.CancelledError()
    with gateway_ingress(runner):
        with pytest.raises(asyncio.CancelledError):
            runner._steer_running_agent(SimpleNamespace(steer=cancelled), 'synthetic')
        with pytest.raises(GatewayDraining) as refusal:
            refuse_queued_ingress(runner)
        assert refusal.value.pre_effect is False
