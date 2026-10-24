"""Goal/heartbeat continuation, post-turn hooks and loop-wakeup watcher methods for GatewayRunner.

Split out of ``gateway/run.py``; bound onto ``GatewayRunner`` via the MRO.
``gateway.run`` internals are imported lazily inside method bodies (import cycle),
so ``patch("gateway.run.X")`` keeps intercepting them at call time.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

from gateway.platforms.event import MessageEvent, MessageType
from gateway.admission import admit_gateway_work

if TYPE_CHECKING:  # string annotations only; never imported at runtime (cycle)
    from gateway.run import GatewayRunner  # noqa: F401
    from gateway.run_turn_runner import TurnRunner  # noqa: F401

# Log-record parity with the origin module.
logger = logging.getLogger("gateway.run")


class GatewayGoalsMixin:
    """Goal/heartbeat continuation, post-turn hooks and loop-wakeup watcher methods for GatewayRunner."""

    # ── /goal — persistent cross-turn goals (Ralph-style loop) ──────────
    def _goal_max_turns_from_config(self) -> int:
        """Configured /goal turn budget. GatewayRunner.config is a GatewayConfig dataclass, so the
        top-level ``goals`` block is only reachable via hermes_cli.config.load_config()."""
        try:
            goals_cfg = (
                (self.config or {}).get("goals", {})
                if isinstance(self.config, dict)
                else getattr(self.config, "goals", {}) or {}
            )
            if not goals_cfg:
                from hermes_cli.config import load_config

                goals_cfg = (load_config() or {}).get("goals") or {}
            return int(goals_cfg.get("max_turns", 20) or 20)
        except Exception:
            return 20

    async def _warm_goals_session_db(self, label: str) -> None:
        """Warm the goals SessionDB cache off-loop (best-effort): a cold cache runs the state.db
        init on the loop thread and freezes the loop. The executor hop keeps the profile home
        override alive under multiplex; a failed warm-up is a bounded stall, never a crash."""
        try:
            from hermes_cli.goals import _get_session_db as _warm_goals_db

            await self._run_in_executor_with_context(_warm_goals_db)
        except Exception as exc:
            logger.warning("%s: session DB warm-up failed: %s", label, exc)

    async def _session_entry_for_manager(self, event: "MessageEvent", label: str):
        """Session entry for a /goal or /heartbeat manager, or None when lookup fails. Warms the
        SessionDB cache first (a cold cache drops the first write while the reply claims it was
        set). Internal events never touch activity (idle/daily reset clock)."""
        await self._warm_goals_session_db(label)
        try:
            session_entry = await self.async_session_store.get_or_create_session(
                event.source, touch_activity=not bool(getattr(event, "internal", False)),
            )
        except Exception as exc:
            logger.debug("%s: session lookup failed: %s", label, exc)
            return None
        return session_entry if getattr(session_entry, "session_id", None) else None

    async def _manager_for_event(self, event: "MessageEvent", kind: str, load):
        """``(manager, session_entry)`` for *kind* ("goal"/"heartbeat"), or ``(None, None)``.
        ``load()`` imports the manager class and returns a ``session_id -> manager`` factory."""
        try:
            factory = load()
        except Exception as exc:
            logger.debug("%s manager unavailable: %s", kind, exc)
            return None, None
        session_entry = await self._session_entry_for_manager(event, f"{kind} manager")
        if session_entry is None:
            return None, None
        return factory(session_entry.session_id), session_entry

    async def _get_goal_manager_for_event(self, event: "MessageEvent"):
        """Return ``(GoalManager, session_entry)`` for this event, or ``(None, None)``."""
        def _load():
            from hermes_cli.goals import GoalManager
            max_turns = self._goal_max_turns_from_config()
            return lambda sid: GoalManager(session_id=sid, default_max_turns=max_turns)
        return await self._manager_for_event(event, "goal", _load)

    async def _get_heartbeat_manager_for_event(self, event: "MessageEvent"):
        """Return ``(HeartbeatManager, session_entry)`` for this event, or ``(None, None)``."""
        def _load():
            from hermes_cli.heartbeat import HeartbeatManager
            return lambda sid: HeartbeatManager(session_id=sid)
        return await self._manager_for_event(event, "heartbeat", _load)

    @staticmethod
    def _synthetic_prompt_event(source: Any, text: str, *, internal: bool = False) -> MessageEvent:
        """Build the TEXT event used to inject a goal/heartbeat/loop prompt into a session.

        The stored source's ``message_id`` is the message that registered the watch; a synthetic
        prompt is not a reply to it, so it is dropped or every progress bubble and final reply
        would quote that stale message (Telegram DM topics route anchorless via the topic id).
        """
        source = dataclasses.replace(source, message_id=None) if getattr(source, "message_id", None) else source
        return MessageEvent(text=text, message_type=MessageType.TEXT, source=source, internal=internal)

    def _register_heartbeat_watch(self, quick_key: str, source: Any, session_id: str) -> None:
        """Track the canonical route and start the restart-recoverable poller."""
        watch = getattr(self, "_heartbeat_watch", None)
        if watch is None:
            watch = self._heartbeat_watch = {}
        watch[quick_key] = (source, session_id)
        self._start_heartbeat_poller()

    def _unregister_heartbeat_watch(self, quick_key: str) -> None:
        (getattr(self, "_heartbeat_watch", None) or {}).pop(quick_key, None)

    async def _heartbeat_poll_once(self, watch: dict) -> None:
        """Wake each idle watched session once; leave busy sessions' ticks unclaimed."""
        for quick_key, (source, session_id) in list(watch.items()):
            try:
                with self._profile_scope_for_source(source):
                    await self._heartbeat_poll_watch(watch, quick_key, source, session_id)
            except Exception as exc:
                logger.debug("heartbeat poll for %s failed: %s", quick_key, exc)

    @admit_gateway_work
    async def _heartbeat_poll_watch(self, watch, quick_key, source, session_id):
        await self._warm_goals_session_db("heartbeat poll")
        store = getattr(self, "session_store", None)
        if store is not None:
            current = store.peek_session_id(quick_key)
            if not current:
                watch.pop(quick_key, None)
                return
            session_id = current
            watch[quick_key] = (source, session_id)
        adapter = self._delivery_adapter_for(source)
        if adapter is None or not adapter._message_handler:
            return
        if (
            self._is_session_running(quick_key)
            or quick_key in adapter._active_sessions
            or self._queue_depth(quick_key, adapter=adapter) > 0
        ):
            return  # keep missed intervals due until user work has drained
        from hermes_cli.heartbeat import HeartbeatManager

        mgr = await self._run_in_executor_with_context(lambda: HeartbeatManager(session_id=session_id, strict=True))
        if not mgr.has_heartbeat():
            watch.pop(quick_key, None)
            return
        prompt = await self._run_in_executor_with_context(mgr.due_prompt)
        if not prompt:
            return
        if (self._is_session_running(quick_key) or quick_key in adapter._active_sessions
                or self._queue_depth(quick_key, adapter=adapter) > 0):
            await self._run_in_executor_with_context(mgr.abandon_fire)
            return
        event = self._synthetic_prompt_event(source, prompt)
        event.metadata["gateway_session_key"] = quick_key
        event._heartbeat_execution_started = False
        # Provenance read by display_kind_for_event / the turn's quiet surfaces; the event stays
        # non-internal so authorization and the emergency stop still apply.
        event._heartbeat_session_id = session_id
        # Recheck above after the off-thread durable claim; a human keeps the idle boundary.
        try:
            await adapter.handle_message(event)
        finally:
            task = getattr(adapter, "_session_tasks", {}).get(quick_key)
            if task is not None:
                from gateway.run_heartbeat_acceptance import settle_heartbeat_attempt
                task.add_done_callback(lambda done: settle_heartbeat_attempt(self, event, mgr))
            elif quick_key not in adapter._active_sessions:
                await self._run_in_executor_with_context(mgr.abandon_fire)

    def _start_heartbeat_poller(self) -> None:
        """Start the single gateway-wide heartbeat poll task (idempotent)."""
        existing = getattr(self, "_heartbeat_poll_task", None)
        if existing is not None and not existing.done():
            return

        from hermes_cli.heartbeat import POLL_SECONDS

        async def _poll_loop():
            while True:
                await asyncio.sleep(POLL_SECONDS)
                from gateway.run_heartbeat_restore import restore_heartbeat_watches
                await restore_heartbeat_watches(self)
                watch = getattr(self, "_heartbeat_watch", None)
                if watch:
                    await self._heartbeat_poll_once(watch)

        try:
            task = self._heartbeat_poll_task = asyncio.create_task(_poll_loop())
            # PERMANENT once started (infinite loop) — tag it like a _spawn_supervised watcher so
            # _scale_to_zero_has_live_background_work() doesn't treat the gateway as busy forever.
            task._hermes_supervised_watcher = True  # type: ignore[attr-defined]
            _bg = getattr(self, "_background_tasks", None)
            if _bg is not None:
                _bg.add(task)
                task.add_done_callback(_bg.discard)
        except Exception:
            logger.debug("Failed to start heartbeat poller", exc_info=True)

    def _goal_notice_adapter(self, source: Any):
        adapter = self._delivery_adapter_for(source)
        if not adapter:
            logger.debug("goal continuation: no adapter for %s", getattr(source, "platform", None))
        return adapter

    async def _send_goal_status_notice(self, source: Any, message: str) -> None:
        """Send a /goal judge status line back to the originating chat/thread."""
        adapter = self._goal_notice_adapter(source)
        if not adapter:
            return
        metadata = None
        with suppress(Exception):
            metadata = self._thread_metadata_for_source(source)
        result = await adapter.send(source.chat_id, message, metadata=metadata)
        if result is not None and not getattr(result, "success", True):
            logger.warning(
                "goal continuation: status send failed: %s", getattr(result, "error", "unknown error"),
            )

    async def _defer_goal_status_notice_after_delivery(self, source: Any, message: str) -> None:
        """Send a /goal status line after the main response is delivered.

        The adapter sends the agent response after this caller returns, so for reading order use
        its one-shot post-delivery callback when available, else deliver directly (never drop).
        """
        adapter = self._goal_notice_adapter(source)
        if not adapter:
            return

        async def _deliver() -> None:
            try:
                await self._send_goal_status_notice(source, message)
            except Exception as exc:
                logger.warning("goal continuation: status send failed: %s", exc, exc_info=True)

        session_key = None
        with suppress(Exception):
            session_key = self._session_key_for_source(source)
        if session_key and hasattr(adapter, "register_post_delivery_callback"):
            try:
                active = getattr(adapter, "_active_sessions", {}).get(session_key)
                generation = getattr(active, "_hermes_run_generation", None) if active is not None else None
                adapter.register_post_delivery_callback(session_key, _deliver, generation=generation)
                return
            except Exception as exc:
                logger.debug("goal continuation: post-delivery callback registration failed: %s", exc)
        await _deliver()

    async def _post_turn_manager(self, session_entry: Any, label: str, module: str, load):
        """Shared head of the post-turn hooks: ``load()`` imports the manager module and returns a
        ``session_id -> manager`` factory; None when unavailable / no session id. Warms the
        SessionDB cache first — a cold cache at the turn boundary drops the read/write."""
        try:
            factory = load()
        except Exception as exc:
            logger.debug("%s: %s module unavailable: %s", label, module, exc)
            return None
        sid = getattr(session_entry, "session_id", None) or ""
        if not sid:
            return None
        await self._warm_goals_session_db(label)
        return await self._run_in_executor_with_context(factory, sid)

    async def _post_turn_goal_continuation(
        self, *, session_entry: Any, source: Any, final_response: str,
    ) -> None:
        """Retain evaluation and durable handoff even if its delivered-turn waiter cancels."""
        from gateway.admission import adopt_ingress_task
        task = adopt_ingress_task(self, asyncio.create_task(self._post_turn_goal_work(
            session_entry=session_entry, source=source, final_response=final_response)))
        self._track_deferred_agent_worker(task, None)
        await asyncio.shield(task)

    async def _post_turn_goal_work(
        self, *, session_entry: Any, source: Any, final_response: str,
    ) -> None:
        """Run the goal judge after a gateway turn (AFTER delivery) and, if still active, enqueue a
        continuation through the adapter FIFO so a simultaneous real user message takes priority."""
        def _load():
            from hermes_cli.goals import GoalManager
            max_turns = self._goal_max_turns_from_config()
            return lambda sid: GoalManager(session_id=sid, default_max_turns=max_turns, strict=True)

        try:
            mgr = await self._post_turn_manager(session_entry, "goal continuation", "goals", _load)
            if mgr is None:
                raise RuntimeError('automatic goal lookup unavailable')
        except Exception:
            self._ingress_barrier.fail_quiescence()
            logger.error('Goal lookup unconfirmed for session %s; inspect retained goal/history before release',
                         getattr(session_entry, 'session_id', None), exc_info=True)
            raise
        if not mgr.is_active():
            return
        if await self._defer_goal_for_maintenance(mgr, source, mgr.next_continuation_prompt(), final_response):
            return

        _bg_procs, _active_deleg = None, 0
        with suppress(Exception):
            from hermes_cli.goals import count_active_delegations, gather_background_processes as _gather_bg
            # Only THIS session's processes (gateway turns register under turn_ctx.session_id):
            # subagents' pollers must not park the parent's goal.
            _bg_procs = _gather_bg(owner_task_id=getattr(session_entry, "session_id", None) or None)
            _active_deleg = count_active_delegations(getattr(session_entry, "session_id", None))

        # judge_goal() is a synchronous aux-LLM HTTP call (10-40 s; would block Discord heartbeats).
        # _run_in_executor_with_context carries the profile secret scope / aux runtime contextvars
        # without which aux credential resolution fails under multiplexing.
        try:
            decision = await self._run_in_executor_with_context(
                lambda: mgr.evaluate_after_turn(
                    final_response or "", user_initiated=True, background_processes=_bg_procs,
                    active_delegations=_active_deleg,
                ),
            )
        except Exception:
            self._ingress_barrier.fail_quiescence()
            raise
        if decision.get('goal_state_conflict'):
            retained = await self._defer_goal_for_maintenance(mgr, source, decision['interrupted_continuation_prompt'],
                final_response, expected_goal=decision['goal_evaluation_origin'],
                ownership_conflict=decision['goal_state_conflict'],
                input_context={'evaluated_goal_state': decision['evaluated_goal_state']})
            if not retained:
                self._ingress_barrier.fail_quiescence()
                raise RuntimeError('goal conflict handoff unconfirmed')
            return
        if await self._defer_goal_for_maintenance(
                mgr, source, decision.get('continuation_prompt'), final_response):
            return
        msg = decision.get("message") or ""
        # Deferred until the visible final response is delivered, else "✓ Goal achieved" precedes it.
        if msg and source is not None:
            await self._defer_goal_status_notice_after_delivery(source, msg)
        prompt = decision.get("continuation_prompt") or ""
        if not decision.get("should_continue") or not prompt or source is None:
            return
        # Enqueue via the adapter's FIFO so a user message already in flight preempts naturally.
        try:
            adapter = self._delivery_adapter_for(source)
            _quick_key = self._session_key_for_source(source)
            if adapter and _quick_key:
                from gateway.admission import native_producer_ingress, startup_session_admitted, without_gateway_ingress
                continuation = self._synthetic_prompt_event(source, prompt)
                continuation._goal_admission_checkpoint = {'session_id': mgr.session_id,
                                                            'goal_state': mgr.state.to_json(),
                                                            'final_response': final_response,
                                                            'startup_restore': startup_session_admitted(self, _quick_key)}
                with native_producer_ingress(self), without_gateway_ingress():
                    self._enqueue_fifo(_quick_key, continuation, adapter)
        except Exception as exc:
            from gateway.admission import GatewayDraining
            if isinstance(exc, GatewayDraining):
                if await self._defer_goal_for_maintenance(mgr, source, prompt, final_response):
                    return
                raise
            logger.debug("goal continuation: enqueue failed: %s", exc)

    @staticmethod
    def _goal_input_snapshot(event):
        """Freeze the same JSON-safe inputs the private maintenance receipt persists."""
        import json
        return json.loads(json.dumps({
            'message_id': event.message_id, 'channel_prompt': event.channel_prompt,
            'channel_context': event.channel_context, 'raw_message': event.raw_message}, default=str))

    def _queued_goal_owner(self, event, checkpoint, resolved_session_id):
        from hermes_cli.goals import GoalManager
        origin = checkpoint['session_id']
        key = self._session_key_for_source(event.source)
        conflict = {'origin_session_id': origin, 'resolved_session_id': resolved_session_id, 'session_key': key}
        try:
            current = self.session_store.lookup_by_session_key(key)
            current_id = current.session_id if current is not None else None
            conflict.update(current_session_id=current_id, suspended=bool(current is not None and current.suspended))
            tip = self.session_store._db_for_key(key).get_compression_tip(origin)
        except Exception as exc:
            conflict['reason'] = f'conversation lineage unreadable: {type(exc).__name__}'
            return GoalManager(origin), conflict
        if (current is None or current.suspended or current_id != tip
                or (resolved_session_id is not None and current_id != resolved_session_id)):
            conflict['reason'] = 'originating conversation replaced or suspended before execution'
            return GoalManager(origin), conflict
        return GoalManager(current_id), None

    def _claim_goal_entry(self, event, prepared_session_id):
        """No await/retry: serialize the final identity check and entry with route/goal writers."""
        checkpoint = getattr(event, '_goal_admission_checkpoint', None)
        if checkpoint is None:
            return True
        import sqlite3
        from contextlib import closing
        from pathlib import Path
        from gateway.run import _profile_runtime_scope
        from gateway.admission_capacity import mark_event_execution_started
        from hermes_cli.goals import _confirmed_goal_state
        from hermes_cli.sqlite_safe_read import connect_tracked
        store = self.session_store
        key = self._session_key_for_source(event.source)
        conflict = {'origin_session_id': checkpoint['session_id'], 'prepared_session_id': prepared_session_id,
                    'session_key': key, 'reason': 'final goal/route ownership unavailable or changed'}
        acquired = store._lock.acquire(blocking=False)
        try:
            if acquired:
                home = self._resolve_profile_home_for_source(event.source)
                with _profile_runtime_scope(home, {}):
                    db = store._db_for_key(key)
                    uri = Path(db.db_path).resolve().as_uri() + '?mode=rw'
                    with closing(connect_tracked(uri, uri=True, timeout=0, isolation_level=None)) as conn:
                        conn.row_factory = sqlite3.Row
                        conn.execute('BEGIN IMMEDIATE')
                        try:
                            current = store._entry_locked(key)
                            row = conn.execute('SELECT value FROM state_meta WHERE key=?',
                                               ('goal:' + prepared_session_id,)).fetchone()
                            tip = db.get_compression_tip(checkpoint['session_id'], conn=conn)
                            if (current is not None and not current.suspended
                                    and current.session_id == prepared_session_id == tip and row is not None
                                    and _confirmed_goal_state(row[0]).to_json() == checkpoint['goal_state']):
                                mark_event_execution_started(event)
                                return True
                            conflict.update(current_session_id=current.session_id if current else None)
                        finally:
                            conn.rollback()
        except Exception as exc:
            conflict['reason'] = f'final ownership unreadable/contended: {type(exc).__name__}'
        finally:
            if acquired:
                store._lock.release()
        event._goal_entry_conflict = conflict
        event._goal_ownership_refused = True
        self._ingress_barrier.fail_quiescence()
        return False

    async def _defer_queued_goal_for_maintenance(self, event, *, resolved_session_id=None):
        checkpoint = getattr(event, '_goal_admission_checkpoint', None)
        if checkpoint is None:
            return False
        from gateway.run import _async_profile_runtime_scope
        home = self._resolve_profile_home_for_source(event.source)
        async with _async_profile_runtime_scope(home, offload=self._run_in_executor_with_context):
            resolved = resolved_session_id or getattr(event, '_goal_resolved_session_id', None)
            mgr, conflict = await self._run_in_executor_with_context(
                lambda: self._queued_goal_owner(event, checkpoint, resolved))
            conflict = getattr(event, '_goal_entry_conflict', None) or conflict
            expected = checkpoint.get('goal_state')
            if not isinstance(expected, str):
                self._ingress_barrier.fail_quiescence()
                raise RuntimeError('queued goal originating identity unavailable')
            if conflict or mgr.state is None or mgr.state.to_json() != expected:
                event._goal_ownership_refused = True
                self._ingress_barrier.fail_quiescence()
            elif resolved_session_id is not None:
                event._goal_resolved_session_id = resolved_session_id
            inputs = dict(checkpoint.get('input_context') or self._goal_input_snapshot(event))
            inputs['origin_session_id'] = checkpoint['session_id']
            return await self._defer_goal_for_maintenance(
                mgr, event.source, event.text, checkpoint['final_response'], expected_goal=expected,
                input_context=inputs, ownership_conflict=conflict)

    async def _run_goal_fifo_drain(self, adapter, event, session_key):
        """Fresh goal permission spans preflight through core entry; closed work becomes durable pause."""
        from gateway.admission import GatewayDraining, native_producer_ingress, startup_resume_ingress
        startup = (getattr(self, '_startup_restore_in_progress', False)
                   and event._goal_admission_checkpoint.get('startup_restore') is True)
        scope = startup_resume_ingress(self, event, session_key) if startup else native_producer_ingress(self)
        started = False
        guard = adapter._active_sessions.get(session_key)
        try:
            with scope:
                from gateway.admission_capacity import reserve_ingress_capacity
                reserve_ingress_capacity(self, event, session_key)
                started = True
                await adapter._process_message_background(event, session_key)
        except GatewayDraining:
            if not started:
                if await self._defer_queued_goal_for_maintenance(event):
                    return
                # Accepted FIFO cannot disappear on a later capacity refusal. Retain
                # exact checkpoint as maintenance, never auto-spin/resend or borrow a slot.
                self._ingress_barrier.fail_quiescence()
                if await self._defer_queued_goal_for_maintenance(event):
                    return
            self._ingress_barrier.fail_quiescence()
            raise RuntimeError('goal continuation ownership unconfirmed')
        finally:
            if not started and adapter._session_tasks.get(session_key) is asyncio.current_task():
                adapter._cleanup_finished_session_task(session_key, guard)

    def _retain_goal_fifo_followup(self, adapter, event, session_key):
        """Keep an already accepted goal in existing FIFO; its fresh Base task owns admission."""
        if session_key not in adapter._pending_messages:
            adapter._pending_messages[session_key] = event
        else:
            self._session_state(session_key).conversation.queued_events.append(event)

    async def _preserve_maintenance_goal_fifo(self, adapter, session_key):
        """Checkpoint accepted continuations before any shutdown queue consumption."""
        if not adapter or not session_key:
            return
        barrier = getattr(self, '_ingress_barrier', None)
        if not (self._draining or self._external_drain_active or (barrier is not None and barrier._closed)):
            return
        pending = adapter._pending_messages
        entries = [pending.get(session_key), *list(self._overflow_queue(session_key) or ())]
        for event in entries:
            if getattr(event, '_goal_admission_checkpoint', None) is None:
                continue
            if not await self._defer_queued_goal_for_maintenance(event):
                if barrier is not None:
                    barrier.fail_quiescence()
                raise RuntimeError('accepted goal maintenance checkpoint unconfirmed')
            # An await may have installed another event. Remove only this exact obligation.
            if pending.get(session_key) is event:
                del pending[session_key]
            overflow = self._overflow_queue(session_key)
            if overflow is not None:
                overflow[:] = [queued for queued in overflow if queued is not event]

    def _track_goal_drain_checkpoint(self, adapter, event, session_key, task):
        """A queued goal cancelled before its first step still gets a durable maintenance handoff."""
        guard = adapter._active_sessions.get(session_key)
        def finished(done):
            barrier = self._ingress_barrier
            closed = barrier._closed or self._draining or self._external_drain_active
            if not done.cancelled() or getattr(event, '_heartbeat_execution_started', False) or not closed:
                return
            from gateway.admission import without_gateway_ingress
            with without_gateway_ingress():
                cleanup = asyncio.create_task(self._defer_queued_goal_for_maintenance(event))
            tasks = self.__dict__.setdefault('_deferred_agent_cleanup_tasks', set())
            def verified(result):
                if adapter._session_tasks.get(session_key) is done:
                    adapter._cleanup_finished_session_task(session_key, guard)
                if result.cancelled() or result.exception() is not None:
                    barrier.fail_quiescence()
            cleanup.add_done_callback(verified)
            self._track_task_in(tasks, cleanup)
        task.add_done_callback(finished)

    async def _defer_goal_for_maintenance(self, mgr, source, prompt, final_response, *, input_context=None, expected_goal=None, ownership_conflict=None):
        """Retain a not-yet-started continuation as an owned, actionable maintenance checkpoint."""
        barrier = getattr(self, '_ingress_barrier', None)
        closed = (getattr(self, '_draining', False) or getattr(self, '_external_drain_active', False)
                  or (barrier is not None and barrier._closed))
        if ((not closed and not ownership_conflict) or not prompt or source is None
                or (not mgr.is_active() and expected_goal is None)):
            return False
        hold = barrier.hold if barrier is not None else {}
        owner = hold.get('owner') or 'gateway-maintenance-operator'
        def pause():
            try:
                return mgr.pause_for_maintenance(
                    owner=owner, hold_id=hold.get('hold_id'), continuation_prompt=prompt,
                    final_response=final_response, source=dataclasses.asdict(source), input_context=input_context,
                    expected_goal=expected_goal, ownership_conflict=ownership_conflict)
            except Exception:
                if barrier is not None:
                    barrier.fail_quiescence()
                logger.error('Goal maintenance checkpoint unconfirmed for %s; quiescence blocked',
                             mgr.session_id, exc_info=True)
                raise
        receipt = await self._run_in_executor_with_context(pause)
        if not receipt['pause_applied']:
            if barrier is not None:
                barrier.fail_quiescence()
            logger.error('Old goal continuation retained UNKNOWN for %s; replacement untouched; owner=%s', mgr.session_id, owner)
            return True
        await self._defer_goal_status_notice_after_delivery(
            source, f'Goal paused for gateway maintenance; owner={owner}. Use /goal resume after handoff.')
        return True

    async def _run_post_turn_hooks(
        self, *, agent_result: Any, source: Any, is_internal: bool, event: Any = None,
    ) -> None:
        """Retain routing through bookkeeping when the delivered-turn waiter is cancelled."""
        if getattr(event, '_goal_ownership_refused', False):
            return  # No turn ran; post-turn bookkeeping must not reset a suspended route.
        from gateway.admission import adopt_ingress_task
        task = adopt_ingress_task(self, asyncio.create_task(self._post_turn_bookkeeping(
            agent_result=agent_result, source=source, is_internal=is_internal, event=event)))
        self._track_deferred_agent_worker(task, None)
        await asyncio.shield(task)

    async def _post_turn_bookkeeping(
        self, *, agent_result: Any, source: Any, is_internal: bool, event: Any = None,
    ) -> None:
        """Run goal and loop bookkeeping after an agent turn returns."""
        final_text = self._final_text_for_post_turn_hooks(agent_result, event)
        try:
            session_entry = await self.async_session_store.get_or_create_session(
                source, touch_activity=not is_internal,
            )
        except Exception:
            self._ingress_barrier.fail_quiescence()
            logger.error('Post-turn route unresolved for %s/%s; inspect retained goal/loop obligations before release',
                         source.platform, source.chat_id, exc_info=True)
            return
        # Empty interrupted/errored responses must not drive /goal, but an in-flight /loop tick
        # still needs to be released and rescheduled.
        hooks = [("loop completion", self._post_turn_loop_completion)]
        if final_text.strip():
            hooks.insert(0, ("goal continuation", self._post_turn_goal_continuation))
        for label, hook in hooks:
            try:
                await hook(session_entry=session_entry, source=source, final_response=final_text)
            except Exception as exc:
                logger.debug("%s hook failed: %s", label, exc)

    @staticmethod
    def _final_text_for_post_turn_hooks(agent_result, event=None) -> str:
        """Text for /goal and /loop after a gateway turn. Streamed turns return None from
        _handle_message_with_agent (already_sent); the delivered reply is stashed on the event."""
        text = ""
        if isinstance(agent_result, dict):
            text = str(agent_result.get("final_response") or "")
        elif isinstance(agent_result, str):
            text = agent_result
        if text.strip():
            return text
        streamed = getattr(event, "_streamed_final_response", None)
        return streamed if isinstance(streamed, str) and streamed.strip() else text

    async def _post_turn_loop_completion(
        self, *, session_entry: Any, source: Any, final_response: str,
    ) -> None:
        """Complete a /loop wakeup tick after a gateway turn. No-op unless a tick is in flight
        (``awaiting_response``, set when the wakeup was injected); applies the LOOP_COMPLETE marker
        / --until judge / caps and schedules the next tick for the idle wakeup watcher."""
        def _load():
            from hermes_cli.loops import LoopManager
            return lambda sid: LoopManager(session_id=sid, strict=True)

        try:
            mgr = await self._post_turn_manager(session_entry, "loop completion", "loops", _load)
            if mgr is None:
                raise RuntimeError('automatic loop lookup unavailable')
        except Exception:
            self._ingress_barrier.fail_quiescence()
            logger.error('Loop lookup unconfirmed for session %s; inspect retained loop/history before release',
                         getattr(session_entry, 'session_id', None), exc_info=True)
            raise
        state = mgr.state
        if state is None or not state.awaiting_response:
            return
        # The --until judge is a sync aux-LLM call — keep it off the event loop, but carry the
        # contextvars: a bare executor hop drops the profile HERMES_HOME override and secret scope,
        # so a served secondary's tick would be written into the DEFAULT profile's state.db.
        decision = await self._run_in_executor_with_context(mgr.complete_tick, final_response or "")
        msg = decision.get("message") or ""
        if msg and source is not None:
            await self._defer_goal_status_notice_after_delivery(source, msg)

    @admit_gateway_work
    async def _loop_wakeup_fire_one(
        self, sid: str, state: Any, now: float, warned_no_route: set, profile: Optional[str] = None,
    ) -> None:
        """Inject one due /loop wakeup into its session, applying every deferral rule. ``profile`` is
        the store being scanned (None = default); a ``profile`` persisted in the route wins."""
        from hermes_cli.loops import LoopManager, goal_blocks_loop_tick
        from gateway.wake import WakeNotAccepted, admit_internal_event

        if state.awaiting_response or now < state.next_due_at:
            return
        route = state.route or {}
        platform_name = route.get("platform", "")
        chat_id = route.get("chat_id", "")
        if not platform_name or not chat_id:
            return  # CLI / TUI-owned loop — their own schedulers drive it.
        profile = route.get("profile") or profile
        # The loop's OWN profile's adapter map, fail closed: ``self.adapters`` is the default profile's,
        # so a secondary session's wakeup would inject via the default bot on a bare chat_id (a
        # Telegram DM lands in the user's chat with the other bot).
        adapters = self._adapters_for_profile(profile)
        adapter = next((a for p, a in adapters.items() if p.value == platform_name), None)
        if adapter is None:
            if sid not in warned_no_route:
                warned_no_route.add(sid)
                logger.debug(
                    "loop wakeup: no adapter for platform %r (session %s, profile %s)", platform_name, sid, profile,
                )
            return

        source = self._build_process_event_source({
            "session_key": "",
            "platform": platform_name,
            "chat_id": chat_id,
            **{k: route.get(k, "") for k in ("chat_type", "thread_id", "user_id", "user_name")},
        })
        if source is None:
            return
        if profile and not getattr(source, "profile", None):
            source.profile = profile  # session key + runtime scope of the injected turn
        session_key = None
        with suppress(Exception):
            session_key = self._session_key_for_source(source)
        if session_key and session_key in self._running_agents:
            return  # busy — stays due, next scan retries
        if await self._run_in_executor_with_context(lambda: goal_blocks_loop_tick(sid, strict=True)):
            return

        mgr = await self._run_in_executor_with_context(lambda: LoopManager(session_id=sid, strict=True))
        if not mgr.is_due(now):
            return
        # fire_tick()/complete_tick() are writes (BEGIN IMMEDIATE) taking the SessionDB writer lock; a slow
        # writer elsewhere holding it while the loop thread blocked froze the gateway until the watchdog
        # fired. The context-preserving executor keeps the profile HERMES_HOME override under multiplex.
        wakeup = await self._run_in_executor_with_context(mgr.fire_tick)
        if not wakeup:
            return
        # #85957: after the parent turn's event.complete the CLIENT owns the next turn on this stateless
        # surface. Persist the completion as a durable delivery row — never self-post it as a new role=user
        # prompt.
        # #85957: same client-owns-the-turn rule as the raw-key branch above — persist the completion as a
        # delivery row, never self-post it as a new role=user prompt.
        try:
            logger.info(
                "loop wakeup #%s — injecting for %s chat=%s thread=%s",
                mgr.state.ticks_fired if mgr.state else "?",
                platform_name, source.chat_id, source.thread_id,
            )
            await admit_internal_event(adapter, self._synthetic_prompt_event(source, wakeup, internal=True))
            # Slash-command loops dispatch through the command path and never hit the post-turn
            # completion hook — complete the tick immediately (caps + scheduling).
            if wakeup.lstrip().startswith("/"):
                await self._run_in_executor_with_context(mgr.complete_tick, "")
        except WakeNotAccepted:
            await self._run_in_executor_with_context(mgr.abandon_tick)
        except Exception as exc:
            logger.warning("loop wakeup outcome unknown for %s: %s", sid, exc)

    async def _loop_wakeup_watcher(self, interval: float = 15.0) -> None:
        """Fire due /loop wakeups for idle gateway sessions: a coarse ticker scans persisted loops
        (SessionDB ``loop:*`` rows) and injects each due prompt via the synthetic-message path.
        Deferrals: session running a turn (FIFO would race the live turn); active non-parked /goal
        (goal owns the idle boundary); no routing metadata (one-time warning).

        Multiplex: one gateway-wide task, so ``list_active_loops`` alone reads only the launch home's
        store — a ``/loop`` set from a secondary profile's chat would never fire. Every served
        profile's store is scanned under its own runtime scope (same shape as ``_handoff_watcher``),
        and each hit is fired against that profile's adapters."""
        from gateway.run import _async_profile_runtime_scope, _handoff_watch_scopes
        from gateway.run_idle_gates import profile_has_active_loop
        await asyncio.sleep(5)  # let platforms finish connecting
        warned_no_route: set = set()

        def _scope(profile_home):
            # profile_home None = the launch profile's own store; once the process multiplexes it
            # binds its own scope instead of running on ambient env (see _scope_or_null).
            if profile_home is not None:
                return _async_profile_runtime_scope(profile_home)
            from tui_gateway.launch_profile_policy import async_launch_profile_scope_if_multiplexed
            return async_launch_profile_scope_if_multiplexed()

        async def _scan_one_store(profile_name: Optional[str]) -> None:
            from hermes_cli.loops import list_active_loops

            # Warm once per scan: the scan reads every persisted loop and a cold cache would
            # run the state.db init on the loop thread before the first read.
            await self._warm_goals_session_db("loop wakeup")
            # Off-loop too: the read is lock-free under WAL but convoys on the writer lock without it.
            active_loops = await self._run_in_executor_with_context(lambda: list_active_loops(strict=True))
            now = time.time()
            for sid, state in active_loops:
                await self._loop_wakeup_fire_one(sid, state, now, warned_no_route, profile_name)

        while self._running:
            try:
                for profile_name, profile_home in _handoff_watch_scopes(self):
                    # Idle gate (run_idle_gates): skip the scope entry when the profile's store holds
                    # no active loop. The root scan (None) is unscoped and stays cheap.
                    if profile_home is not None and not await self._run_in_executor_with_context(
                            profile_has_active_loop, profile_home):
                        continue
                    async with _scope(profile_home):
                        await _scan_one_store(profile_name)
            except Exception as exc:
                logger.debug("loop wakeup watcher error: %s", exc)
            await asyncio.sleep(interval)
