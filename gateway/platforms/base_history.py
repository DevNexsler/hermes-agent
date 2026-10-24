"""Bounded delivery lookup; detached physical reads remain owned after timeout/cancel."""
import asyncio
import contextlib
from contextvars import copy_context
import threading


async def bounded_history_media_paths(adapter, session_key):
    from gateway.platforms import base
    def fail_open(reason, *, exc_info=False):
        base.logger.warning('[%s] ' + reason + ' %s; delivering bare local file path(s) without history dedup',
                            adapter.name, session_key, exc_info=exc_info)
    admission = base._HISTORY_MEDIA_LOOKUP_ADMISSION
    if not admission.acquire(blocking=False):
        fail_open('Media-delivery history lookup capacity exhausted for')
        return None
    loop = asyncio.get_running_loop()
    result_future = loop.create_future()
    def publish(result=None, error=None):
        if not result_future.done():
            # The loop callback can beat Thread teardown; strict ownership includes that tail.
            if thread.is_alive():
                loop.call_later(.001, publish, result, error)
                return
            result_future.set_exception(error) if error is not None else result_future.set_result(result)
    def worker():
        result, error = None, None
        try:
            result = adapter._history_media_paths_for_session(session_key)
        except BaseException as exc:
            error = exc
        finally:
            admission.release()
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(publish, result, error)
    try:
        runner = adapter.gateway_runner
        if runner is not None:
            runner._track_deferred_agent_worker(result_future, None)
        else:
            result_future.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        thread = threading.Thread(target=copy_context().run, args=(worker,),
                                  name='media-history-lookup', daemon=True)
        thread.start()
    except Exception:
        result_future.cancel()  # No worker started; release both owners.
        admission.release()
        fail_open('Could not start media-delivery history lookup worker for', exc_info=True)
        return None
    try:
        return await asyncio.wait_for(asyncio.shield(result_future),
                                      timeout=base._HISTORY_MEDIA_LOOKUP_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        fail_open('Timed out loading media-delivery history for')
        return None
    except Exception:
        fail_open('Media-delivery history lookup failed for', exc_info=True)
        return None
