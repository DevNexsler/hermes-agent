"""A retired MCP session must not hold discovery of its replacement hostage."""

import asyncio
from types import SimpleNamespace

from tools.mcp_tool import MCPServerTask


def test_reconnect_discovers_new_session_while_old_call_holds_rpc_lock(monkeypatch):
    """A dropped server may leave its old tools/call waiting until the outer timeout."""
    acquired = asyncio.Event()
    release = asyncio.Event()
    attempts = []

    class Session:
        async def list_tools(self):
            return SimpleNamespace(tools=[], nextCursor=None)

    class Server(MCPServerTask):
        async def _run_http(self, config):
            attempts.append(len(attempts) + 1)
            self.session = Session()
            self._ready.set()
            if len(attempts) == 1:
                async def old_call():
                    async with self._rpc_lock:
                        acquired.set()
                        await release.wait()

                holder = asyncio.create_task(old_call())
                await acquired.wait()
                self._old_holder = holder
                return "reconnect"

            await asyncio.wait_for(self._discover_tools(), timeout=0.5)
            self._shutdown_event.set()
            return "shutdown"

    monkeypatch.setattr(Server, "_register_discovered_tools_if_needed", lambda self: None)
    server = Server("retired-lock")

    async def exercise():
        try:
            await asyncio.wait_for(server.run({"url": "http://example.test/mcp", "skip_preflight": True}), timeout=2)
        finally:
            release.set()
            holder = getattr(server, "_old_holder", None)
            if holder is not None:
                await holder

    asyncio.run(exercise())
    assert attempts == [1, 2]
