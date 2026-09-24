"""Brave Search (free-tier Data-for-Search API) — search only, 2,000 queries/month.

Config: ``web.search_backend`` / ``web.backend: "brave-free"`` (hyphen form kept for
existing user configs). Env: ``BRAVE_SEARCH_API_KEY``. Pair with Firecrawl/Tavily/Exa
for ``web_extract``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import httpx

from plugins.web._common import BaseWebSearchProvider, provider_env, search_fail, search_ok, setup_schema, titled_rows
from plugins.web.brave_free.request_queue import BraveQueueBusy, queued_request

logger = logging.getLogger(__name__)

_BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


class BraveFreeWebSearchProvider(BaseWebSearchProvider):
    """Search-only Brave provider using the free-tier Data-for-Search API."""

    NAME = "brave-free"
    DISPLAY_NAME = "Brave Search (Free)"
    KEY_ENV = "BRAVE_SEARCH_API_KEY"

    def search(self, query: str, limit: int = 5) -> Dict[str, Any]:
        api_key = provider_env("BRAVE_SEARCH_API_KEY")
        if not api_key:
            return search_fail("BRAVE_SEARCH_API_KEY is not set")
        from tools.web_tools import _load_web_config
        web_config = _load_web_config()
        try:
            response = queued_request(
                api_key,
                lambda: httpx.get(
                    _BRAVE_ENDPOINT,
                    params={"q": query, "count": max(1, min(int(limit), 20))},
                    headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
                    timeout=15,
                ),
                interval=float(web_config.get("brave_min_interval_seconds", 2.0)),
                max_wait=float(web_config.get("brave_queue_max_wait_seconds", 30.0)),
                cooldown=float(web_config.get("brave_429_cooldown_seconds", 60.0)),
            )
            response.raise_for_status()
            data = response.json()
        except BraveQueueBusy as exc:
            return search_fail(str(exc))
        except httpx.HTTPStatusError as exc:
            logger.warning("Brave Search HTTP error: %s", exc)
            return search_fail(
                f"Brave Search returned HTTP {exc.response.status_code}; "
                "use the web-usage Chrome browser if search cannot wait"
            )
        except httpx.RequestError as exc:
            logger.warning("Brave Search request error: %s", exc)
            return search_fail(f"Could not reach Brave Search: {exc}")
        except (TypeError, ValueError) as exc:
            logger.warning("Brave Search response/config error: %s", exc)
            return search_fail(f"Could not parse Brave Search response: {exc}")
        raw_results = (data.get("web") or {}).get("results", []) or []
        web_results = titled_rows(raw_results[:limit], "description")
        logger.info("Brave Search '%s': %d results (from %d raw, limit %d)", query, len(web_results), len(raw_results), limit)
        return search_ok(web_results)

    def get_setup_schema(self) -> Dict[str, Any]:
        return setup_schema(
            "Brave Search (Free)", "free", "Free-tier API key — 2k queries/mo, search only.",
            "BRAVE_SEARCH_API_KEY", "Brave Search API key (free tier)", "https://brave.com/search/api/",
        )


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
import os  # noqa: F401,E402


_PLUGIN_COMPAT_LAZY = {
    'WebSearchProvider': ('agent.web_search_provider', 'WebSearchProvider'),
}


def __getattr__(name):  # PEP 562 — lazy so no import cycles
    target = _PLUGIN_COMPAT_LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    from hermes_cli.plugin_compat import warn_once
    warn_once(__name__, name, *target)
    return getattr(importlib.import_module(target[0]), target[1])
# ---- END PLUGIN-COMPAT ----
