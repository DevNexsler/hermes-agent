---
sidebar_position: 7
title: "Gateway Internals"
description: "How the messaging gateway boots, authorizes users, routes sessions, and delivers messages"
---

# Gateway Internals

The messaging gateway is the long-running process that connects Hermes to 20+ external messaging platforms through a unified architecture.

## Key Files

| File | Purpose |
|------|---------|
| `gateway/run.py` | `GatewayRunner` facade — composes the `gateway/run_*.py` sibling mixins (startup, adapters, inbound, turn, busy, goals, notifications, shutdown, …) and `gateway/slash_commands_*.py` handlers |
| `gateway/session.py` | `SessionStore` — conversation persistence and session key construction |
| `gateway/delivery.py` | Outbound message delivery to target platforms/channels |
| `gateway/pairing.py` | DM pairing flow for user authorization |
| `gateway/channel_directory.py` | Maps chat IDs to human-readable names for cron delivery |
| `gateway/hooks.py` | Hook discovery, loading, and lifecycle event dispatch |
| `gateway/mirror.py` | Cross-session message mirroring for `send_message` |
| `gateway/status.py` | Token lock management for profile-scoped gateway instances |
| `gateway/builtin_hooks/` | Extension point for always-registered hooks (none shipped) |
| `gateway/platform_registry.py` | Adapter registry, factories, and deferred (lazy) loaders for bundled platform plugins |
| `plugins/platforms/<name>/` | Bundled messaging adapters (most platforms: `adapter.py` + `plugin.yaml`) |
| `gateway/platforms/` | Shared `base.py` plus legacy/direct adapters (Signal, API server, webhooks, …) |

## Architecture Overview

```text
┌─────────────────────────────────────────────────┐
│                  GatewayRunner                  │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Telegram │  │ Discord  │  │  Slack   │       │
│  │ Adapter  │  │ Adapter  │  │ Adapter  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │             │             │             │
│       └─────────────┼─────────────┘             │
│                     ▼                           │
│              _handle_message()                  │
│                     │                           │
│         ┌───────────┼───────────┐               │
│         ▼           ▼           ▼               │
│  Slash command   AIAgent    Queue/BG            │
│    dispatch      creation   sessions            │
│                     │                           │
│                     ▼                           │
│                 SessionStore                    │
│              (SQLite persistence)               │
└───────┴─────────────┴─────────────┴─────────────┘
```

## Message Flow

When a message arrives from any platform:

1. **Platform adapter** receives raw event, normalizes it into a `MessageEvent`
2. **Base adapter** checks active session guard:
   - If agent is running for this session → queue message, set interrupt event
   - If `/approve`, `/deny`, `/stop` → bypass guard (dispatched inline)
3. **GatewayRunner._handle_message()** receives the event:
   - Resolve session key via `_session_key_for_source()` (format: `agent:{namespace}:{platform}:{chat_type}:{chat_id}`; the namespace is `main` for the default profile, `<profile>` under multiplexing — see [Multiplexed profiles](#multiplexed-profiles))
   - Check authorization (see Authorization below)
   - Check if it's a slash command → dispatch to command handler
   - Check if agent is already running → intercept commands like `/stop`, `/status`
   - Otherwise → create `AIAgent` instance and run conversation
4. **Response** is sent back through the platform adapter

### Session Key Format

Session keys encode the full routing context:

```
agent:{namespace}:{platform}:{chat_type}:{chat_id}
```

For example: `agent:main:telegram:private:123456789` for the default profile, or
`agent:work:telegram:private:123456789` when the multiplexer routes that chat to profile `work`
(`gateway/session.py::_session_key_namespace`; a profile literally named `main` is marked `main~`).

Thread-aware platforms (Telegram forum topics, Discord threads, Slack threads) may include thread IDs in the chat_id portion. **Never construct session keys manually** — always use `build_session_key()` from `gateway/session.py`.

### Two-Level Message Guard

When an agent is actively running, incoming messages pass through two sequential guards:

1. **Level 1 — Base adapter** (`gateway/platforms/base.py`): Checks `_active_sessions`. If the session is active, queues the message in `_pending_messages` and sets an interrupt event. This catches messages *before* they reach the gateway runner.

2. **Level 2 — Gateway runner** (`gateway/run_inbound.py`): Checks `_running_agents`. Intercepts specific commands (`/stop`, `/new`, `/queue`, `/status`, `/approve`, `/deny`) and routes them appropriately. Everything else triggers `running_agent.interrupt()`.

Commands that must reach the runner while the agent is blocked (like `/approve`) are dispatched **inline** via `await self._message_handler(event)` — they bypass the background task system to avoid race conditions.

## Authorization

The gateway uses a multi-layer authorization check, evaluated in order:

1. **Per-platform allow-all flag** (e.g., `TELEGRAM_ALLOW_ALL_USERS`) — if set, all users on that platform are authorized
2. **Platform allowlist** (e.g., `TELEGRAM_ALLOWED_USERS`) — comma-separated user IDs
3. **DM pairing** — authenticated users can pair new users via a pairing code
4. **Global allow-all** (`GATEWAY_ALLOW_ALL_USERS`, or `gateway.allow_all_users` in `config.yaml`, bridged to the env var by `gateway/config_loader.py::bridge_core_env_settings`) — if set, all users across all platforms are authorized
5. **Default: deny** — unauthorized users are rejected

### DM Pairing Flow

```text
Admin: /pair
Gateway: "Pairing code: ABC123. Share with the user."
New user: ABC123
Gateway: "Paired! You're now authorized."
```

Pairing state is persisted in `gateway/pairing.py` and survives restarts.

## Slash Command Dispatch

All slash commands in the gateway flow through the same resolution pipeline:

1. `resolve_command()` from `hermes_cli/commands.py` maps input to canonical name (handles aliases, prefix matching)
2. The canonical name is checked against `GATEWAY_KNOWN_COMMANDS`
3. `_handle_message()` (`gateway/run_inbound.py`) looks the handler up by name — `_handle_<name>_command` on the `gateway/slash_commands_*.py` mixins — via `_command_handler_table` over `_IDLE_COMMANDS` / `_PLAIN_COMMANDS` in `gateway/run_busy.py`; there is no `if canonical == ...` chain
4. Some commands are gated on config (`gateway_config_gate` on `CommandDef`)

### Running-Agent Guard

Commands that must NOT execute while the agent is processing are rejected early:

While `_quick_key in self._running_agents`, `_dispatch_busy_slash_command()` in `gateway/run_busy.py` routes each recognized command by its `CommandDef.busy_policy` / `busy_handler`: a mid-run variant (`_busy_<key>_command`) if one exists, otherwise the normal handler when `busy_policy` allows it, otherwise a reject message ("⏳ Agent is running — `/model` can't run mid-turn…").

Bypass commands (`/stop`, `/new`, `/approve`, `/deny`, `/queue`, `/status`) have mid-run handlers and are dispatched inline.

## Config Sources

The gateway reads configuration from multiple sources:

| Source | What it provides |
|--------|-----------------|
| `~/.hermes/.env` | API keys, bot tokens, platform credentials |
| `~/.hermes/config.yaml` | Model settings, tool configuration, display options |
| Environment variables | Override any of the above |

Unlike the CLI (which uses `load_cli_config()` with hardcoded defaults), the gateway reads `config.yaml` directly via YAML loader. This means config keys that exist in the CLI's defaults dict but not in the user's config file may behave differently between CLI and gateway.

## Platform Adapters

Most messaging platforms ship as plugin adapters under `plugins/platforms/<name>/adapter.py`; a few legacy adapters still live directly in `gateway/platforms/`. All extend `BasePlatformAdapter` from `gateway/platforms/base.py`:

```text
plugins/platforms/                  # plugin-packaged adapters (one dir each)
├── telegram/adapter.py     # Telegram Bot API (long polling or webhook)
├── discord/adapter.py      # Discord bot via discord.py
├── slack/adapter.py        # Slack Socket Mode
├── whatsapp/adapter.py     # WhatsApp Business Cloud API
├── matrix/adapter.py       # Matrix via mautrix (optional E2EE)
├── mattermost/adapter.py   # Mattermost WebSocket API
├── email/adapter.py        # Email via IMAP/SMTP
├── sms/adapter.py          # SMS via Twilio
├── dingtalk/adapter.py     # DingTalk WebSocket
├── feishu/adapter.py       # Feishu/Lark WebSocket or webhook
├── wecom/adapter.py        # WeCom (WeChat Work) callback
├── line/adapter.py         # LINE Messaging API
├── teams/adapter.py        # Microsoft Teams
├── irc/adapter.py          # IRC (canonical scoped-lock example)
├── homeassistant/adapter.py # Home Assistant conversation integration
└── …                       # google_chat, ntfy, photon, raft, simplex, …

gateway/platforms/                  # core base + legacy direct adapters
├── base.py              # BasePlatformAdapter — shared logic for all platforms
├── signal.py            # Signal via signal-cli REST API
├── weixin.py            # Weixin (personal WeChat) via iLink Bot API
├── bluebubbles.py       # Apple iMessage via BlueBubbles macOS server
├── qqbot/               # QQ Bot (Tencent QQ) via Official API v2 (sub-package)
├── yuanbao.py           # Yuanbao (Tencent) DM/group adapter
├── msgraph_webhook.py   # Microsoft Graph change-notification webhook (Teams, Outlook, etc.)
├── webhook.py           # Inbound/outbound webhook adapter
└── api_server.py        # REST API server adapter
```

**Deferred loading:** Bundled `kind: platform` plugins register cheap `register_deferred` loaders in `gateway/platform_registry.py` (via `hermes_cli/plugins.py`) so platform SDKs import only when the gateway starts, delivers, or runs setup/status — not on plain `hermes chat`. Resolution loads one adapter on lookup; full enumeration runs pending loaders only on paths that need every platform.

Experimental connector-backed platforms use the generic relay adapter in `gateway/relay/` instead of a direct platform module. When `GATEWAY_RELAY_URL` or `gateway.relay_url` is configured, the gateway registers the `relay` platform, dials the connector over an outbound WebSocket, and receives `descriptor`, `inbound`, and `interrupt_inbound` frames on that same socket. The connector advertises a `CapabilityDescriptor`; Hermes can send normal outbound replies, token-less `follow_up` operations, and interrupt frames back through the relay. The source-grounded wire contract lives in [Relay ↔ Connector contract](relay-connector-contract.md).

Adapters implement a common interface:
- `connect()` / `disconnect()` — lifecycle management
- `send()` — outbound message delivery
- inbound events are normalized into a `MessageEvent` and forwarded via `handle_message()`

Internal push wakes use `gateway.wake.admit_internal_event`: the public
`handle_message()` still returns `None`, but the event's process-local
`_gateway_accepted` receipt is set only after scheduling or queue insertion.
A missing handler, mismatched explicit session key, or queue-cap drop is not
acceptance. Custom adapters overriding ingress should delegate internal events to
`BasePlatformAdapter.handle_message()` (or explicitly record actual admission),
not equate a consumed/dropped callback with acceptance. This receipt is separate
from heartbeat execution accounting and does not bypass authorization, emergency
stop, or later turn-preparation gates.

### Token Locks

Adapters that connect with unique credentials call `acquire_scoped_lock()` in `connect()` and `release_scoped_lock()` in `disconnect()`. This prevents two profiles from using the same bot token simultaneously.

A lock conflict is emitted as `{scope}_lock` with `retryable=True` so a **mid-run** reconnect can recover once the other holder exits. At **startup**, though, a live foreign holder is a configuration conflict: `gateway/restart.py::is_global_startup_conflict()` recognizes the `*_lock` / `lock_conflict` code families and the startup router parks the platform `fatal` instead of retry-queueing it. With nothing else connected the gateway exits `78` (`EX_CONFIG`, `gateway_state=startup_failed`) so the supervisor stops restarting it: systemd via `RestartPreventExitStatus=78`, s6 via finish→125, launchd via `KeepAlive.SuccessfulExit=false` after the stderr wrapper maps 78→0. Alongside a genuinely transient peer failure the gateway stays alive and only the peer retries.

## Delivery Path

Outgoing deliveries (`gateway/delivery.py`) handle:

- **Direct reply** — send response back to the originating chat
- **Home channel delivery** — route cron job outputs and background results to a configured home channel
- **Explicit target delivery** — the send engine specifying `telegram:-1001234567890`, exposed via the [`hermes send` CLI](../guides/pipe-script-output.md) for shell scripts and via cron `deliver:` targets
- **Cross-platform delivery** — deliver to a different platform than the originating message

Cron job deliveries are NOT mirrored into gateway session history — they live in their own cron session only. This is a deliberate design choice to avoid message alternation violations.

## Hooks

Gateway hooks are Python modules that respond to lifecycle events:

### Gateway Hook Events

| Event | When fired |
|-------|-----------|
| `gateway:startup` | Gateway process starts |
| `session:start` | New conversation session begins |
| `session:end` | Session completes or times out |
| `session:reset` | User resets session with `/new` |
| `agent:start` | Agent begins processing a message |
| `agent:step` | Agent completes one tool-calling iteration |
| `agent:end` | Agent finishes and returns response |
| `command:*` | Any slash command is executed |

Hooks are discovered from `gateway/builtin_hooks/` (an extension point — currently empty in the shipped distribution; `_register_builtin_hooks()` is a no-op stub) and `<profile home>/hooks/` (user-installed; `~/.hermes/hooks/` for the default profile, one directory per served profile under multiplexing — paths resolve at call time, never at import). Each hook is a directory with a `HOOK.yaml` manifest and `handler.py`.

## Memory Provider Integration

When a memory provider plugin (e.g., Honcho) is enabled:

1. Gateway creates an `AIAgent` per message with the session ID
2. The `MemoryManager` initializes the provider with the session context
3. Provider tools (e.g., `honcho_profile`, `viking_search`) are routed through:

```text
AIAgent._invoke_tool()
  → self._memory_manager.handle_tool_call(name, args)
    → provider.handle_tool_call(name, args)
```

4. On session end/reset, `on_session_end()` fires for cleanup and final data flush

### Memory Flush Lifecycle

Explicit conversation boundaries (such as `/new`, `/reset`, or `/resume`) flush and finalize the outgoing session. Idle time and daily boundaries never finalize it.

Resource-only TTL, LRU, and memory-pressure eviction commits the cached transcript to configured memory providers before releasing the agent's clients. It does not close the durable conversation: the next turn reloads the same transcript and identity.

## Background Maintenance

The gateway runs periodic maintenance alongside message handling:

- **Cron ticking** — checks job schedules and fires due jobs
- **Session housekeeping** — reclaims cached resources without ending transcripts
- **Memory flush** — commits memory before soft cache eviction
- **Cache refresh** — refreshes model lists and provider status

## Process Management

The gateway runs as a long-lived process, managed via:

- `hermes gateway start` / `hermes gateway stop` — manual control
- `systemctl` (Linux) or `launchctl` (macOS) — service management
- PID file at `~/.hermes/gateway.pid` — profile-scoped process tracking

**Profile-scoped vs global**: `start_gateway()` uses profile-scoped PID files. Standalone (one gateway per profile), `hermes -p x gateway stop` stops only that profile's gateway. Under multiplexing there is ONE gateway process per host, owned by whichever profile launched it (`gateway/host_rendezvous.py` publishes its PID, home and served set; `gateway/host_attach.py` is the attach/rescan/refuse decision every lifecycle verb goes through): `hermes gateway stop` on the owner takes every served profile down, and `hermes -p x gateway stop` for a served secondary refuses with exit 78 (it has no gateway of its own). A second `gateway run` for a served profile attaches and exits 0 — under a service supervisor it exits 75 (EX_TEMPFAIL) instead, so the redundant unit is RETRIED rather than parked: "someone else serves me right now" is a runtime observation that ends when that process does, and 78 (which systemd, s6 and launchd all treat as permanent) would strand the profile. ATTACH requires a live `identify` answer from the owner; a rendezvous record with nothing answering behind it proves an owner exists but never that it serves you, so it yields a transient refusal (exit 75), never an attach. An owner that answers `multiplex: False` to the rescan is another profile's *standalone* gateway, not a multiplexer that excluded you: the verb starts this profile's own gateway beside it (the one-process-per-profile topology), it does not refuse — refusing there exited 78 and parked every launchd unit but the first to claim the host lock. `hermes gateway stop --all` uses global `ps aux` scanning to kill all gateway processes (used during updates). Liveness is decided by `gateway.status.live_gateway_pid_for_home` (PID + start-time fingerprint), never bare PID existence.

## Multiplexed profiles

With `gateway.multiplex_profiles: true` one process serves the default profile plus every live directory under `profiles/` (`hermes_cli/profiles.py::profiles_to_serve(multiplex=True)`). `os.environ` and module globals hold the **launch** profile's values, so every activity for a secondary binds its scope explicitly — a profile is home + secret scope + terminal scope together:

| Activity | Binding |
|---|---|
| Routed turn | `gateway/run.py::_profile_runtime_scope(home)` via `run_turn.py::_profile_scope_for_source` |
| Agent release / eviction (TTL, LRU, memory pressure) | `gateway/run_agent_cache.py::_run_release_in_profile_scope` |
| Shutdown | `gateway/run_shutdown.py::_finalize_session` |
| Post-turn media delivery | `gateway/platforms/base.py::_media_delivery_scope` |
| Cron tick | `cron/scheduler_provider.py::_profile_cron_scope(home)` (one ticker, profiles in sequence) |
| Child processes (`hermes -p X` workers, relay turns, browser drivers) | `tools/environments/local.py::served_profile_child_env` |
| Background threads | `agent/memory_provider.py::spawn_context_thread` |

Secret reads fail closed (`agent.secret_scope.get_secret` raises `UnscopedSecretError`) only after `set_multiplex_active(True)`, which the gateway, cron, `gateway migrate` and the Desktop/dashboard `serve` backend set. Adapter YAML never reaches `os.environ` under multiplex: `gateway/platforms/_shared.py::apply_yaml_bridge` seeds `PlatformConfig.extra` and skips the environ write under a secondary's scope; gates read through `platform_gate_env`. Shared-ingress platforms (WhatsApp bridge, Relay) run on the default profile only; a secondary that enables one is logged once and stamped into runtime status (`run_adapters.py::_note_unserved_secondary_platform`). Per-profile isolation as the user sees it: [Multi-profile gateways § What is isolated per profile](../user-guide/multi-profile-gateways.md#what-is-isolated-per-profile).

## Mid-run plugin loading

Plugins that load after the adapters connected (install/enable from the CLI, Desktop, dashboard or
`plugins.manage`; a tool-triggered force re-discovery) re-wire their platform handlers without a restart
(#87770). The pieces, all in `gateway/run_plugin_rewire.py`:

- **Discovery listener** — `_start_recover_previous_run` subscribes `PluginManager.on_plugin_loaded` for the
  launch profile and `_load_secondary_profile_config` does so per served profile. The event fires from inside
  `discover_and_load` (never from an RPC) for the newly loaded plugins; the callback hops onto the gateway
  loop with `call_soon_threadsafe`.
- **Idempotent re-wire** — `BasePlatformAdapter.rewire_plugin_handlers()` re-reads
  `get_platform_handler_factories(platform)` and runs only factories not yet wired on the live native
  client (keyed `(plugin, qualname)` because a force reload hands back new function objects). Telegram
  hoists the added handlers ahead of core's catch-alls; Slack also re-registers missing
  `register_slack_action_handler` callbacks once per `AsyncApp`.
- **`reload-plugins` control verb** — other processes (`hermes plugins install`, `hermes serve`) ask the
  running gateway to force-rescan the requested (served) home; the answer carries `plugins`, per-plugin
  `activations` and `adapters_rewired`, so the caller can say "active now" truthfully.
- **Scope limit** — handlers only. Tools and system-prompt sections of a late plugin wait for the next
  session (prompt-cache invariant); portable MCP servers wait for `mcp.reload`. Nothing un-wires on disable.

## Related Docs

- [Session Storage](./session-storage.md)
- [Cron Internals](./cron-internals.md)
- [ACP Internals](./acp-internals.md)
- [Agent Loop Internals](./agent-loop.md)
- [Messaging Gateway (User Guide)](../user-guide/messaging/index.md)

## Retryable extension admission and maintenance holds

`gateway.admission` supplies a gateway-owned reservation for HTTP extensions which acknowledge
work before a core session exists. After synchronous authentication, enter
`with gateway_ingress(adapter.gateway_runner):` **before the first await or mutation**, including
body parsing, deduplication, SQLite and journal setup. Keep the scope through response creation.
Answer `GatewayDraining` with JSON HTTP 503 and `exc.headers` (`Retry-After: 1`). The exception
alone grants no retry authority: positive pre-effect proof follows the narrower contract below.
A closed-fence refusal happens before journals, dedup claims, delivery receipts
or core dispatch. A missing or unready native capability also raises this exception. Test doubles must install an explicit
admission fixture; a missing runner never means acceptance.

Reservations acquired before fence closure finish naturally. The base adapter adopts the task it
spawns before that task can run; its completion callback releases the reservation even if cancelled
before its first step. Only the reserving handler and explicitly adopted task identities can pass
the core external-drain gate. An unrelated task inheriting context cannot borrow admission.
Extension-created tasks use `adopt_ingress_task(runner, task)` before the handler exits.

Retryable ingress supports direct starts and actual steering. In-memory queue/FIFO/debounce
fallback raises `GatewayDraining` before storage. Callers retain the original durable item;
only positively proven pre-effect refusal permits their defined retry/refund policy. Cancellation
or a generic error retains UNKNOWN. Native internal producers use the same reservation and require
an explicit adapter admission receipt; ambiguous outcomes preserve durable ownership.
Ordinary unleased human queues keep their
existing behavior. Startup resume reserves its own event and preclaimed session through the native
lifecycle path; it can finish restoration while HTTP readiness stays closed, but cannot bypass a
maintenance hold or external drain. Leased arrivals with existing backlog refuse before Base
acknowledges or mutates that backlog. A consumer must never record delivery after a refused steer
or queue fallback.

Native loop ticks, heartbeat decisions and CLI handoff claims reserve before their first durable
mutation. Their lifecycle coroutine stays adopted if its watcher is cancelled; only a proven
unaccepted tick may refund. Unknown outcomes retain durable ownership. During shutdown, an idle
pre-fence reservation still enters and settles its core turn; a busy shutdown handoff refuses
before acknowledgment or queue mutation.

Use `await ingress_to_thread(fn, *args, **kwargs)` for work under this reservation. Cancellation
raises `asyncio.CancelledError` promptly while the shielded executor future remains counted until
the actual worker exits. This helper requires a live authenticated reservation and otherwise raises
`GatewayDraining`. Background observers/startup housekeeping outside ingress keep their own tracked
execution path; do not fabricate ingress or silently fall back from this helper. Admission is a
transport receipt, not authorization, model completion or proof of an outbound business effect.

Native thread boundaries use `native_to_thread`; session database/routing facades accept its
offload policy without depending on the gateway package. Turn and housekeeping pools keep their
existing separation. With a live lease, actual executor futures remain adopted across caller
cancellation; outside ingress, native observers retain their existing execution policy. This
internal policy does not weaken the strict `ingress_to_thread` extension contract.
Accepted Base pending/debounce buffers and runner FIFO/command backlog also prevent strict
quiescence, even when their original guard is orphaned. Inspection never consumes or refunds them.

Independent human FIFO handoffs clear ambient ingress context. Failed Task adoption cancels and
tracks the scheduled child before its first step. Ordinary native executor work also stays in the
existing deferred-worker registry through physical completion after caller cancellation. Busy slash
queue/rejected-steer refusal propagates to HTTP 503; an unexpected steering exception propagates as
an error, with no accepted receipt. Such an error has an unknown effect outcome, never retry authority.

Standing-goal continuation and kickoff/resume are separate native producers. Their enqueue
reservation linearizes with closure and clears parent permission; transport fallback remains
refused. If maintenance closes before continuation entry, including after FIFO enqueue or cancellation
before its first step, `GoalManager.pause_for_maintenance` atomically compares unchanged active state
and stores a maintenance pause plus a private `state_meta` receipt under
`goal_maintenance_pause:<session_id>:<pause_id>`. Receipt retains owner/hold identity, exact goal
checkpoint, continuation prompt and inputs, prior result, profile path and durable history identity.
Existing user/foreign changes are preserved. Failed durable write/readback latches strict quiescence
unknown; marker removal cannot clear that process-local uncertainty.

Every accepted goal drain settles before its producer lease exits, including normal hook skip,
authorization/preflight refusal, error and cancellation. No core entry means no goal post-turn
evaluation; refusal text cannot spend a goal turn or replace its original checkpoint. A shared
private witness records confirmed maintenance retention across supported event rewrites. Terminal
callbacks cover cancellation before the first step and remain in the existing deferred registry
until cleanup is registered or confirmed. Failed/cancelled settlement closes UNKNOWN and never
starts another settlement attempt. An unexecuted accepted obligation refused while open also
closes UNKNOWN before preserving its exact maintenance handoff; it is never silently replayed.
Ordinary unleased human hook skips retain their existing behavior. Physical receipt writers stay
owned after waiter cancellation. These controls provide no restart or business-completion proof.

Every pre-entry and terminal preservation path shares one attempt witness on the accepted goal
envelope, set before physical receipt storage starts. Cancellation cannot authorize a second
writer, including cancellation of Base's first check, the post-`agent:start` check, or a later
capacity refusal. Confirmed retention is idempotent; an unconfirmed started attempt stays UNKNOWN.
Automatic loop/goal mixing checks inspect satisfied waits without clearing or saving cached goal
state. User pause/clear remains authoritative; normal goal evaluation keeps its existing CAS.
Native worker liveness keeps a positive PID-existence result when a secondary macOS `ps` query
fails. A failed or empty query cannot authorize terminal-worker cleanup; a confirmed zombie or
observed exit keeps the existing cleanup contract. Native macOS fault-path qualification remains
required separately from Linux tests.

These pauses never resume automatically. Before fence release, the maintenance owner must inventory
every profile's receipts and leave a named, actionable `/goal resume` handoff. A rollout must not
resume user/foreign/changed pauses. No automatic maintenance-resume API is supplied here; source
publication does not authorize goal commands. Existing history stays in the same profile database.

`gateway_admission_status(runner)` returns `known`, `closed`, `ingress`, `adopted`, `active_work`,
`quiescent`, `observers`, plus public `hold` identity when known. `adopted` includes background tasks and executor
futures. Counters overlap intentionally: only zero matters. Unknown capability, hold or work counters
return `known=False` and `quiescent=None`; never convert that to zero. Read this synchronously on the
gateway loop. Before treating quiescence as an activation fence, hold the existing cron tick lock
for **every** home returned by `_cron_tick_profile_homes`, verify no home was added, and separately
exclude manual/external producers. Tick locks cover scan/advance/register/submit, not job lifetime:
hold them before checking running-job counters. Snapshot zero or producer shutdown alone is insufficient.

Extensions with work outside authenticated handlers implement optional synchronous
`admission_observer_status() -> {known: True, active_tasks: int, active_threads: int}`. These
nonnegative integer counts participate in strict readiness. Missing hooks mean no declared extra
work; a present hook that raises, returns unknown, or supplies invalid counts means unknown
readiness. The hook reads counters on the gateway loop without filesystem/secret/config I/O.
Track actual executor completion through observer cancellation; a cancelled asyncio task alone
does not prove its thread stopped. Lifecycle observers do not acquire HTTP admission permission.

`gateway.admission_hold` supplies separate persistent maintenance control:

- `acquire_admission_hold(owner=..., home=..., token=...)` returns `hold_id` and `token`. Persist a
  random `secrets.token_urlsafe(32)` token privately **before** acquisition; retries with that token
  and owner recover a lost response. A different acquisition raises `AdmissionHoldConflict`.
- `inspect_admission_hold(home=...)` returns `known`, `active`, `hold_id`, `owner`; it exposes no token
  or digest. Malformed/unreadable state means active with unknown identity.
- `release_admission_hold(token=..., home=...)` compares under a shared cross-process lock, returns
  false if already absent, and raises `AdmissionHoldConflict` for foreign/unknown state. Keep the
  serialization lock inode; never unlink it or log tokens.

Holds never expire because of age, owner death or machine epoch. NAS `.drain_request.json` remains a
separate expiring control. New-source startup loads the owner hold before listeners, cron or restore
and waits until explicit release; abort/cancellation leaves it intact. The owner must restore matched
assets before release after a failed deployment. This feature cannot fence an older running binary:
qualifying the initial upgrade and rollback to source without this capability requires a separate
old-process quiescence/bootstrap proof. Source installation or a passing test is not that proof.


Positive refusal is narrower than HTTP503 or the exception type. `GatewayDraining` exposes
`pre_effect` (strict bool, default False) and stable `reason`. Admission/preflight/queue refusal
before any invocation can attest True. The lease marks effects **before** core, notice and every
parent/child steer invocation; mark never resets, including cancellation or a returned False.
Nested/callee typed exceptions cannot upgrade proof after this mark. Attempted steering that
fails or declines remains UNKNOWN; no inferred no-effect or automatic retry. Ordinary human
fanout keeps its existing behavior. Busy leased control/media/interrupt fallbacks refuse before
invocation; direct confirmed steer or fresh start remain the acceptance paths.
Related nested reservations share one monotonic effect history, including adopted children that
run after the nested scope exits. False/error/BaseException/cancellation never restores parent
no-effect proof. Explicit independent human/goal/producer scopes retain separate admission and
history; sharing effect history grants no additional permission.
Preflight refuses pending clarify/update/slash-confirm/tool-approval prompts before Base can acknowledge
background dispatch; core resolver guards cover direct callers too. Unleased human replies retain
their existing control behavior.
Idle leased dispatch also acquires the existing cross-process active-session registry lease before
Base acknowledgement, pinned to the routed profile home. Exact event/ingress ownership transfers
that lease to core; competing ingress cannot reacquire its writer. Last owner releases any
untransferred slot, including cancellation before task start. Unreadable ownership blocks readiness.
Core/hook entry marks effect history before invocation; later refusal cannot regain positive proof.

An extension may offer version1 refusal receipts only with explicit True and an allowlisted
reason, synchronously authenticated request version/DD headers, and exact body/DD binding when
body was read. Generic503/500, malformed or missing proof, timeout and wrong identity are UNKNOWN.
The external consumer owns its wire schema and durable intent; native source supplies no resend
policy. Missing admission capability at first ingress refuses before body work.

Kanban notification claims atomically store a scalar JSON string in existing subscription
`delivery_metadata.notification_attention_v1` with schema version1, outcome UNKNOWN, exact
attempt id, original/claimed cursor, complete task/result/event snapshots, original subscription,
owner principal, profile/home/board/database realpath, time/reason and required operator action.
Record and claim commit together before any ping/artifact/wake. Any present attention, including
malformed/foreign state, blocks collector effects and stale-subscription GC. Generic errors,
crash and cancellation never rewind, automatically retry or drop after a failure count. Existing
`hermes kanban notify-list [task] --json` exposes the complete record; text listing marks UNKNOWN
and shows owner/attempt/cursors. Private attention never travels in platform send metadata.
Owner identifies the exact profile/subscriber principal; rollout must map this to a named
accountable operator and preserve actionable handoff, not claim a human assignment was automatic.
Claim compares the complete subscription generation under its writer lock. Concurrent ownership
or routing changes defer the claim until a later collection resolves the new profile/home.

Only an unchanged owned attention+cursor can settle successful delivery. Proven pre-effect
refusal clears its exact attempt and rewinds only its owned cursor; independently committed ping
receipts survive. Wake reserves a separate native leg after ping checkpoints. SQL/write/readback
or ownership failures block strict readiness. Attention never expires or resolves automatically;
no general manual resolution/resend command is introduced. Incapable older source can ignore
these fields, so rollback safety still requires independent closure and retention proof.
Ping checkpoints verify exact attention/cursor ownership and saved ping cursor in the same writer
transaction. Ignored/restored updates or concurrent marker changes roll back and retain UNKNOWN.

Owned notifier HTTP wake uses one POST attempt, including lost-response/timeout failures; generic
errors and HTTP status alone never authorize replay. If an earlier diagnostic/result wake group
was accepted, a later pre-effect refusal retains aggregate UNKNOWN rather than rewinding the
whole cursor. Artifact exceptions and negative delivery results retain attention too. Native image
batch success means at least one image landed, so owned notifications submit individual image
uploads to verify every artifact before aggregate settlement.

`claim_notify_delivery` returns cursors/events, subscription/attention and Task captured inside
its SQLite claim transaction. Delivery never rereads a later task for text, destination or
artifacts. A later foreign replacement cannot become the caller's ownership token. Existing
cursor-only consumers keep `claim_unseen_events_for_sub` and its three-value result. Successful
archive settlement checks exact owned attention/cursor and current archive status, then deletes
in that same transaction. No postsettlement unconditional unsubscribe; a foreign or malformed
attention installed after commit survives.

Embedded dispatcher reserves its complete decomposition/claim/spawn tick before mutations.
Repeated cancellation waits for admitted physical tick completion before releasing singleton lock.
Strict status also reads machine-shared board claims and task/run worker records without schema
initialization, reconciliation or signals: running/orphan claims, live terminal workers and
unverifiable foreign-host workers prevent zero; unreadable stores mean unknown. Retained DBs under
the supported `boards/_archived/` location stay in this read-only inventory after board removal.
This inventory
does not fence independent dispatcher processes. Coordinator must verify their exclusion and
actual machine-shared dispatcher-lock ownership alongside every served home's cron tick lock.
Existing task events retain a version1 `worker_spawn_pending` intent before physical launch:
exact task/run/claim, profile owner principal, input/history identity, workspace/database and
required operator action. Only matching verified `spawned` or proven native pre-Popen
`spawn_failed` receipt resolves that launch. PID handoff verifies actual task/run readback;
post-spawn failure retains claim and, when writable, appends `worker_spawn_unknown` with PID.
Unresolved intents block strict readiness, automatic dispatch and native reclaim across restart.
Generic callback exceptions remain UNKNOWN; signature inspection never retries an invoked callback.
Owner principal still needs named-human handoff. No queue/schema/automatic resolution introduced;
incapable rollback remains separately held.

Queued goal checkpoints bind their owning profile before lookup/pause. Fresh reservation spans
capacity acquisition through core entry. Shutdown preserves all checkpointed goal entries
in existing slot/overflow as durable owned maintenance pauses before destructive dequeue, including
empty or muted final responses. A later capacity refusal preserves accepted goal checkpoint as
maintenance handoff rather than dropping or automatically spinning the FIFO.
Both enqueue sites pin the originating goal state, including a new generation on set/resume.
Before pause and immediately before agent entry, the queued obligation must still match that state.
A replacement (even identical text at the same clock time), user pause, cleared or malformed state
is never adopted. The existing receipt instead retains the original checkpoint and inputs with
`pause_applied=false`, `outcome="UNKNOWN"`, `observed_goal`/`observed_goal_raw`, and `required_action`;
the successor stays untouched and strict readiness latches unknown. A successful owned pause has
`pause_applied=true`, `outcome="PAUSED"`, and its verified `paused_goal_sha256`. Both outcomes require
explicit operator accounting, never automatic resume/refund/resend. The history remains in the same
profile's existing database; no additional queue is created.

Post-turn judge and quality-gate evaluation also pin the pre-evaluation goal generation and exact
stored JSON. Automatic evaluation uses an isolated snapshot, then compares and reads back every
save inside the existing SQLite writer transaction. A user pause, clear, replacement or resume
committed while the judge waits wins; evaluation cannot resurrect its cached goal. A conflict
retains the original goal, continuation prompt, delivered response and attempted evaluation in the
same UNKNOWN receipt even with admission open. No continuation or misleading pause notice runs.
Persistence/readback failure blocks readiness. The gateway adopts and tracks the complete
evaluation-to-handoff task, so cancellation of its delivered-turn waiter cannot discard the
physical executor result or durable handoff. Explicit user controls keep their existing contract;
no writer lock spans a judge, gate or other await.

The initial automatic goal lookup is strict too: confirmed missing row or inactive state can end
the hook, while unavailable storage, failed reads, present SQL NULL, malformed/nonfinite state or
unknown fields/status block readiness before its tracked task settles. This path preserves the
stored goal and conversation history; an unreadable store cannot attest a completed durable
handoff. The operator must inspect that session before proceeding. No automatic retry or resume
resolves this uncertainty. Ordinary interactive goal reads retain their existing best-effort behavior.

Strict decoding validates original nested gate/contract/subgoal containers and fields before the
permissive legacy parser. Every supplied value must survive decoding; missing known legacy fields
may default and equivalent JSON numbers compare by value, while booleans remain distinct. Discarded
constraints, unknown nested fields or changed scalar values block readiness and preserve the raw row.
Duplicate JSON keys at any depth are ambiguous and rejected before decoding.
The gateway also adopts the whole post-turn task before its earlier routing lookup. Failure of both
SQLite routing persistence and its JSON fallback latches UNKNOWN before that task settles; cancellation
cannot abandon later goal/loop accounting. A surviving authoritative routing path keeps normal behavior.

Automatic loop completion, tick claims and heartbeat claims/refunds also use confirmed metadata
reads and exact-row compare-and-swap with writer-transaction readback. Failed reads, malformed
state, rejected writes or unconfirmed readback close readiness as UNKNOWN; cached changes never
attest persisted completion. A concurrent user pause or replacement keeps ownership. Heartbeat
refund runs off-loop; its task and physical executor remain tracked. A refund cancelled before
its first step closes UNKNOWN before registry removal. Strict readiness includes registered
completed futures until their accounting callbacks finish. Interactive managers retain their
existing best-effort defaults.

Rotating compression passes its actual session database to validated goal, loop and heartbeat
transfer. Each transfer copies the exact original JSON to the child and marks the parent cleared
in one checked SQLite writer transaction. Unknown fields, duplicate keys, lossy normalization,
foreign child state or storage/readback failure cannot silently discard the parent's checkpoint.
The existing native execution context closes UNKNOWN before ownership settles, even when
compression catches the transfer error. The child session may already exist; this does not
attest rollback of the whole compression. Raw state and ancestor history remain inspectable.
This uncertainty latch is process-local and supplies no restart or OLD-source bootstrap proof.

Loop admission retains the exact raw claim and owning database through the actual Base task's
termination. An adopted native monitor preserves its existing lease and executor accounting;
it does not block scans of other loops. Normal post-turn completion must match that claim (or
its verified compression child), enter core, and finish confirmed loop accounting. Preflight
refusal, caught turn failure, generic error, or cancellation cannot silently reschedule the
charged attempt. Original checkpoint/history remain actionable with readiness UNKNOWN; physical
workers remain counted until actual settlement. A cancelled-before-start monitor also closes
UNKNOWN before removal. Slash-command loops complete after actual command dispatch and Base
termination, rather than merely receiving an admission boolean.

Only a positively typed pre-effect admission refusal, with unchanged no-effect history and no
accepted/spawned task, may rewind the exact owned claim. Missing admission receipt or an exception
after effects is ambiguous; neither refunds the tick nor authorizes automatic replay. A misleading
typed refusal after effects propagates generic uncertainty. This deliberately tightens the former
untyped `WakeNotAccepted` refund. Compression transfer reads back both final rows after both writes,
so a parent-clear trigger cannot invalidate an earlier child readback unnoticed.

Heartbeat settlement likewise owns the actual Base task through termination. Missing core entry
does not prove no effects: processing hooks, turn preparation or `agent:start` hooks may already
have run. Their cancellation or failure retains the charged heartbeat and closes readiness UNKNOWN;
reopening cannot replay that attempt. This intentionally replaces the old denial-text refund.
Only a positively typed pre-effect refusal or a Base task cancelled with unchanged no-effect
history refunds the exact claim. A confirmed no-effect refund's strict physical writer stays owned
after observer cancellation and marks storage failures UNKNOWN. Cancelling the observer before
that writer starts closes UNKNOWN. These are process-local controls, not restart/bootstrap proof.

Worker PID publication verifies final task/run rows after the resolving `spawned` receipt write,
inside the same SQLite transaction. A receipt trigger cannot erase a fast-completed live child's
run PID and commit a false resolution; failed readback rolls back receipt and PID writes, leaving
the original pending launch, result and history retained as UNKNOWN.

Queued goals also bind the originating conversation to the resolved/current route before preparation
and again before actual entry. Only a verified compression tip with the identical migrated goal may
continue along that lineage; reset, switch, suspension or unreadable lineage retains the old obligation
as UNKNOWN with `ownership_conflict` (origin/resolved/current IDs, route, suspension and reason).
No successor is paused or invoked, and rejected work runs no post-turn bookkeeping that could reset
a suspended route. Kickoff/resume freeze original command message ID, channel prompt/context and raw
payload as the same JSON-safe private snapshot for immediate, FIFO, cancellation and capacity handoff.
These inputs remain preservation metadata; the synthetic execution envelope is unchanged.

Unresolved physical launch evidence also protects the complete task history, worker logs and managed
workspace, including ancestor handoff inputs. Event/log GC and workspace cleanup check this ownership
under the existing SQLite writer transaction, serialized with launch intent commit. Archive/delete
refuse unresolved task/ancestor ownership; completion may settle its result while preserving inputs.
Verified matching launch settlement restores ordinary cleanup eligibility. No absence-of-PID or
elapsed-time inference clears the launch, and no new settlement/resend interface is added.

The final queued-goal check runs after the last await: a nonwaiting route lock and SQLite writer
snapshot verify the prepared session, original goal generation and verified compression tip together,
then mark execution entry before releasing either lock. Preparation-time compression can advance the
prepared identity only along that verified lineage with identical goal state. Contention or unreadable
ownership retains the exact original obligation as UNKNOWN; no lock spans an await.

Unlink cannot remove an unresolved obligation's ancestor protection. Log retention covers both the current
`<task>.log` and supported numbered rotations. Whole-board hard deletion checks the database inside
the directory being removed, independently of a database override, and refuses active/unverifiable
workers, unresolved launches or any notification attention. Before unlinking files, it commits and
reads back a version1 `board_removal_pending` event in the existing task-events store, with empty
task ID, operation identity, owner principal, database path and required operator action. Native writer
transactions reject any present removal event. This also closes preopened WAL connections, which
otherwise can keep claiming work after filesystem deletion. Failed/interrupted deletion leaves the
surviving store closed and strict inventory UNKNOWN across restart; no automatic clearing/reopen.
The matching private version1 record at `<kanban-home>/kanban/board-removals/<slug>.json` survives
outside the deletion set. Stable nonwaiting file ownership serializes removal, while full byte CAS
preserves any foreign/malformed replacement. Connect/create/writer checks refuse retained external
ownership before automatic initialization. Only successful owned removal with absent target clears
that exact outside record; failure after deleting the database or entire directory stays UNKNOWN.
Our database descriptor closes after checked intent commit and before filesystem unlink; old WAL
connections still refuse through their durable event. No lock inode is unlinked. Aliased directory/DB
removal is refused because its physical ownership path cannot be retained by this contract.
Explicit recreation after successful removal creates an independent database, while old connections remain closed. Existing
archive behavior retains its database for inventory. These guards require the new source; old writers
and incapable rollback still need separate exclusion. Owner principal does not assign a human.

Inventory enumerates default/named/archived database realpaths plus any configured database override;
an override cannot hide another board. Task deletion and archived-task purge retain running claims,
live or unverifiable task/run PIDs, unresolved launch/notification attention and original ancestors.
The same retained set protects events, current/rotated logs, attachment removal and workspace cleanup,
including ancestor handoffs and the completion-before-physical-exit window. Unreadable process
fingerprints retain ownership without authorizing signals or automatic reclaim. Log GC refuses a
database override that differs from the log board, and never creates a missing ownership database.
Both persisted and current composed worker fingerprints require complete boot-ID/PID1/start witnesses;
partial epochs remain unknown. Log GC pins the effective board before selecting logs and database.
Attachment reference checks and unlink share the writer transaction, including legacy path aliases;
a registration queued behind deletion refuses a missing blob before recording metadata or history.
Failed-upload, size-rejection and staged-copy cleanup use the same serialized reference check;
unreadable ownership retains the file and original failure. Referenced workspace inputs defer removal.
Shared retained workspace realpaths also defer enclosing, nested and alias cleanup, even without
attachment rows; direct, parent and CLI cleanup hold the writer boundary through removal.
The shared blob-unlink seam also preserves an input inside a retained workspace after its last
attachment row is removed or upload registration fails. Nested writer savepoints validate board
closure before yielding, leaving their caller's outer transaction owned by that caller.
Attachment identity, input containment and retained workspace overlap share the existing NFC
comparison policy after realpath resolution. Destructive I/O keeps the original stored path spelling;
on normalization-sensitive filesystems equivalent comparison keys conservatively retain inputs.
Resolved original paths stay separate from comparison keys; keys never become lookup paths.
Actual filesystem identity and ancestor checks also retain case aliases. Unreadable identity
refuses destructive cleanup; absent names alone do not imply business completion or retry authority.
Native log rotation runs before claim under that same board's writer boundary. Prior physical,
launch, notification or ancestor ownership preserves every generation, including zero-backup
configuration. An injected database cannot authorize canonical log rotation; append-only spawn
remains available. Read-only strict inventory uses tracked connections so concurrent header probes
cannot discard SQLite snapshot locks.
Rotation pins the effective board and log target before validation; concurrent board selection
changes cannot redirect deletion into another board's retained generations.
Legacy scalar process witnesses must be finite and positive; malformed witnesses retain ownership.
Bounded media-history delivery may time out, but its deferred physical read remains registered until
the actual daemon thread exits, including cancellation and post-read thread teardown.
Any notification attention (including malformed/foreign)
also keeps strict readiness UNKNOWN across restart. Normal completion/archive semantics remain;
ordinary settled deletion/GC becomes eligible after physical ownership and attention both settle.

Supported pre-dispatch text rewrites clone the event while retaining private native checkpoints.
Only the current live reservation's exact event may transfer its reserved capacity/startup ownership
to that clone; matching session keys or inherited context alone grant no permission. Original
transport text stays unchanged. Real agent entry marks the original event too, so a cancellation
callback cannot mistake an executing rewritten goal/heartbeat for work cancelled before entry.

Already accepted in-core goal followups return to the existing Base
FIFO for a separately reserved task. Native boot continuation retains verified same-session boot
provenance in a fresh event/session reservation; HTTP cannot borrow it. Session facade callbacks
retain unleased physical futures too. Base carries runner I/O identity independently of ingress
permission; clearing human FIFO ingress never removes physical-worker ownership.
