"""Durable UNKNOWN notification ownership survives effect errors, cancellation and restart."""
import asyncio
import json
import hashlib
import subprocess
import sys

import pytest

from gateway.config import Platform
from gateway.kanban_watchers_notifier import _KanbanNotification, _notifier_collect
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as kbn
from tests.gateway.test_kanban_wake_acceptance import setup_route


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['ping', 'wake', 'cancel', 'cancel_before', 'claim_only'])
async def test_unknown_notification_keeps_exact_obligation_without_retry_or_gc(tmp_path, monkeypatch, phase):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, source, key = setup_route()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='synthetic attention', assignee='owner', session_id=key)
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', user_id='42',
                          chat_type='dm', delivery_mode='notify' if phase == 'ping' else 'wake')
        kb.complete_task(conn, tid, summary='exact retained completed result')
        task_before = kb.get_task(conn, tid)
        _, original_events = kbn.unseen_events_for_sub(conn, task_id=tid, platform='telegram', chat_id='42')
    effects, entered, release = [], asyncio.Event(), asyncio.Event()
    async def uncertain(*args, **kwargs):
        effects.append('synthetic physical boundary')
        entered.set()
        if phase == 'cancel':
            await release.wait()
        raise RuntimeError('unknown after synthetic marker')
    adapter.send = uncertain
    adapter._active_sessions[key] = asyncio.Event()
    adapter.set_message_handler(uncertain)
    adapter.set_busy_session_handler(uncertain)
    def collect():
        return _notifier_collect(runner, kb, notifier_profile=None, gc_due=True, gc_retention_days=1)
    await adapter.connect()
    rows = await asyncio.to_thread(collect)
    assert len(rows) == 1
    with kbc.connect() as conn:
        sub = kbn.list_notify_subs(conn, tid)[0]
        receipt = json.loads(sub['delivery_metadata']['notification_attention_v1'])
        assert receipt['version'] == 1 and receipt['outcome'] == 'UNKNOWN'
        assert receipt['task']['result'] == task_before.result
        assert [event['id'] for event in receipt['events']] == [event.id for event in original_events]
        assert receipt['owner'] and receipt['required_action'] and receipt['attempt_id']
        raw_receipt = sub['delivery_metadata']['notification_attention_v1']
    # Independent interpreter opens committed SQLite read-only: no inherited cache/connection.
    digest = subprocess.check_output([sys.executable, '-c',
        "import sqlite3,hashlib,sys,json; c=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True); r=c.execute('SELECT delivery_metadata FROM kanban_notify_subs WHERE task_id=?',(sys.argv[2],)).fetchone(); print(hashlib.sha256(json.loads(r[0])['notification_attention_v1'].encode()).hexdigest())",
        str(tmp_path / 'board.db'), tid], text=True).strip()
    assert digest == hashlib.sha256(raw_receipt.encode()).hexdigest()
    if phase != 'claim_only':
        notification = _KanbanNotification(runner, rows[0], platform_cls=Platform, sub_fail_counts={})
        task = asyncio.create_task(notification.deliver())
        if phase in ('cancel', 'cancel_before'):
            if phase == 'cancel':
                await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
    for _ in range(13):  # Fresh collector/SQLite connection; never retry or drop after12.
        assert not await asyncio.to_thread(collect)
    with kbc.connect() as conn:
        conn.execute('UPDATE task_events SET created_at = 1 WHERE task_id = ?', (tid,))
        conn.commit()
        assert kbn.purge_stale_done_notify_subs(conn, max_age_days=1) == 0
        sub_after = kbn.list_notify_subs(conn, tid)[0]
        assert sub_after['delivery_metadata']['notification_attention_v1'] == raw_receipt
        assert sub_after['last_event_id'] == receipt['claimed_cursor']
        assert kb.get_task(conn, tid) == task_before
    assert len(effects) == (0 if phase in ('claim_only', 'cancel_before') else 1)
    release.set()
    await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('foreign', [None, {'owner': 'foreign'}, 'malformed', '{"version":99,"owner":"foreign"}'])
async def test_any_foreign_or_malformed_attention_blocks_claim_and_gc(tmp_path, monkeypatch, foreign):
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, _ = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='retained foreign attention', assignee='owner')
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='wake')
        kb.complete_task(conn, tid, summary='unchanged')
        original_cursor = kbn.list_notify_subs(conn, tid)[0]['last_event_id']
        raw = json.dumps({'notification_attention_v1': foreign})
        conn.execute('UPDATE kanban_notify_subs SET delivery_metadata = ? WHERE task_id = ?', (raw, tid))
        conn.execute('UPDATE task_events SET created_at = 1 WHERE task_id = ?', (tid,))
        conn.commit()
    assert not await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=True, gc_retention_days=1)
    with kbc.connect() as conn:
        row = conn.execute('SELECT * FROM kanban_notify_subs WHERE task_id = ?', (tid,)).fetchone()
        assert row['delivery_metadata'] == raw and row['last_event_id'] == original_cursor
        assert 'notification_attention_v1' in kbn.list_notify_subs(conn, tid)[0]['delivery_metadata']
    await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['foreign_attention', 'foreign_cursor'])
async def test_attention_settlement_cas_preserves_foreign_changes(tmp_path, monkeypatch, mutation):
    from gateway.admission import gateway_admission_status
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, _ = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='owned CAS', assignee='owner')
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='wake')
        kb.complete_task(conn, tid, summary='exact result')
    rows = await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=False, gc_retention_days=30)
    notification = _KanbanNotification(runner, rows[0], platform_cls=Platform, sub_fail_counts={})
    with kbc.connect() as conn:
        if mutation == 'foreign_cursor':
            conn.execute('UPDATE kanban_notify_subs SET last_event_id = last_event_id + 100 WHERE task_id = ?', (tid,))
        else:
            conn.execute('UPDATE kanban_notify_subs SET delivery_metadata = ? WHERE task_id = ?',
                         (json.dumps({'notification_attention_v1': 'foreign'}), tid))
        conn.commit()
        before = dict(conn.execute('SELECT * FROM kanban_notify_subs WHERE task_id = ?', (tid,)).fetchone())
    with pytest.raises(RuntimeError):
        await notification.rewind()
    with kbc.connect() as conn:
        assert dict(conn.execute('SELECT * FROM kanban_notify_subs WHERE task_id = ?', (tid,)).fetchone()) == before
    assert gateway_admission_status(runner)['known'] is False
    await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['persist', 'readback'])
async def test_claim_transaction_failure_rolls_back_and_blocks_readiness(tmp_path, monkeypatch, failure):
    from gateway.admission import GatewayDraining, gateway_admission_status
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    runner, adapter, _, _ = setup_route()
    await adapter.connect()
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='transaction fault', assignee='owner')
        kbn.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='42', delivery_mode='wake')
        kb.complete_task(conn, tid, summary='retained exact payload')
        before = dict(conn.execute('SELECT * FROM kanban_notify_subs WHERE task_id = ?', (tid,)).fetchone())
        if failure == 'persist':
            conn.execute("CREATE TRIGGER fault BEFORE UPDATE OF delivery_metadata ON kanban_notify_subs BEGIN SELECT RAISE(ABORT, 'synthetic persistence fault'); END")
        else:
            conn.execute("CREATE TRIGGER fault AFTER UPDATE OF last_event_id ON kanban_notify_subs BEGIN UPDATE kanban_notify_subs SET delivery_metadata='{}'; END")
        conn.commit()
    assert not await asyncio.to_thread(_notifier_collect, runner, kb,
        notifier_profile=None, gc_due=False, gc_retention_days=30)
    with kbc.connect() as conn:
        assert dict(conn.execute('SELECT * FROM kanban_notify_subs WHERE task_id = ?', (tid,)).fetchone()) == before
    assert gateway_admission_status(runner)['known'] is False
    with pytest.raises(GatewayDraining):
        await runner._kanban_notifier_tick(kb, Platform, {},
            notifier_profile=None, gc_due=False, gc_retention_days=30)
    assert adapter.wire == []
    await adapter.disconnect()
