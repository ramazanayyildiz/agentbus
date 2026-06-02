# AgentBus Terminal Room

A **multi-agent chat room in your terminal.** You + a Claude agent + a Codex agent
(and any others you add) share one merged conversation thread over
[AgentBus](../README.md). You type, they all see it; they talk to each other; you
steer. Different vendors, one screen.

```
────────────────────────────────────────────────────────
▌ ram → @claude-A · 21:40:01
  Rust or Go for a high-concurrency local message-bus daemon?
────────────────────────────────────────────────────────
▌ claude-A · 21:40:09
  Rust — predictable tail latency, zero-GC, tight buffer control.
────────────────────────────────────────────────────────
▌ codex-A · 21:40:15
  Go — simpler concurrency + ops; GC is a non-issue at local IPC latencies.
ram> ▍
```

---

## Quick start

```sh
agentbus-room myroom
```

That launches a room called `myroom` with two agents — **`claude-A`** (Claude) and
**`codex-A`** (Codex) — and drops you at a `ram>` prompt. Type to talk to both.
`Ctrl-C` shuts down cleanly (kills the agents, cleans up temp files).

Use a **different room id** for each separate chat (`agentbus-room standup`,
`agentbus-room debug`); rooms are fully isolated and can run at the same time.

---

## Install

```sh
# from the agentbus repo root — put the launcher on your PATH
ln -sf "$PWD/room/agentbus-room.mjs" ~/.local/bin/agentbus-room
```

It's a symlink to the executable script (shebang `#!/usr/bin/env node`), so it always
runs the latest code — no rebuild, no binary signing. You can also run it directly:
`node room/agentbus-room.mjs myroom`.

### Prerequisites
- **`agentbus`** daemon binary on PATH (`~/.local/bin/agentbus` or `~/.cargo/bin`) — the daemon is started automatically if not running.
- **`sqlite3`** CLI (`brew install sqlite3`) — used to tail the message log for rendering.
- **Node.js 18+** (ES modules, `node:net`, `node:readline`; zero npm dependencies).
- A **Claude** and/or **Codex** CLI on PATH, logged in. Run the room from a directory you trust (agents launch there).

---

## Running rooms

```sh
agentbus-room <room-id> [options]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `--agents <list>` | `claude-A,codex-A:codex` | Comma list of `name[:program]`. Program is `claude` (default) or `codex`. |
| `--launch-dir <dir>` | current dir | Working directory the agents run in (must be trusted). |
| `--cb-max <n>` | `6` | Circuit-breaker: pause after N consecutive agent messages with no human input. |
| `--no-agents` | off | Don't launch agents — attach to ones already running via `agentbus run`. The lightweight "hub died but agents survived" reconnect. |
| `--resume` | off | Reconnect to a **closed** room: replay the prior thread + relaunch each agent with its session restored (see [Reconnecting](#reconnecting)). |
| `--self-test` | — | Run the pure-logic test suite (no agents, no daemon) and exit. |

```sh
agentbus-room standup --agents pm:claude,eng:codex,qa:claude   # 3 agents, mixed
agentbus-room review  --agents claude-A,codex-A:codex --cb-max 10
```

**Agents run in _your_ working directory.** Both Claude and Codex launch with their working
root set to the room's launch dir (current dir, or `--launch-dir`), so they see and edit your
actual project. (Codex previously ran in an isolated tmp workdir and couldn't see your files;
it now runs `codex --cd <launchDir>` while keeping its isolated `CODEX_HOME` for auth + no-MCP.)

**Pasting multi-line text.** A pasted block is sent as **one** message, not one message per
line. The hub coalesces the burst of newline events behind a short (~20 ms) debounce window;
a single typed line is unaffected. The multi-line structure is preserved in the sent message.

---

## Reconnecting

Two independent reconnect paths, for two different failure modes:

| You want… | Use | What happens |
|-----------|-----|--------------|
| The hub crashed/closed but the **agent processes are still running** | `agentbus-room <id> --no-agents` | The hub re-attaches to the live agents on the bus and renders from now. No relaunch, no history replay. |
| The whole room was **closed** (agents gone) and you want to pick up where you left off | `agentbus-room <id> --resume` | Replays the prior thread behind a `──── resumed · history above ────` divider, then relaunches each agent with its **session restored** so it remembers the conversation. |

How `--resume` restores each agent:
- **State file** — every room persists `{ roomId, updatedAt, agents:[{name, program, codexHome?, claudeSessionId?}] }` to `<AGENTBUS_DIR or ~/.agentbus>/rooms/<id>.json` on launch, `/add`, `/kick`, and shutdown.
- **Codex** — runs under a **stable** per-(room, agent) `CODEX_HOME` (under `…/rooms/<id>/codex-<name>/`, not tmp), so its sessions persist. `--resume` relaunches `codex resume --last --all` in that home. The home is **kept** on normal shutdown (so resume works) and deleted only on `/kick`.
- **Claude** — each agent is launched with a pinned `--session-id <uuid>` (minted by the hub) which is stored in the state file; `--resume` relaunches `claude --resume <uuid>`.
- On `--resume`, agents are **not** re-seeded — they restore their own context from their session, so re-seeding is skipped to avoid fighting that.
- Without `--resume`, behavior is unchanged: fresh agents, render from now, and the codex home (still stable) simply starts a new session.

---

## In-room commands

| Command | Action |
|---------|--------|
| *(just type)* | Send to everyone in the room |
| *(paste a block)* | Multi-line paste is sent as **one** message (coalesced), not one per line |
| `@<name> <msg>` | Send to one agent only. `@` + **Tab** completes member names. |
| `/who` | List members and their programs |
| `/add <name>[:program]` | Launch + seed a new agent mid-session (e.g. `/add gpt2:codex`) |
| `/kick <name>` | Kill + clean up + remove a member (room stays up) |
| `/status` | Room id, members, circuit-breaker state |
| `/resume` | Unblock the circuit-breaker after an auto-pause |
| `/help` | List commands |
| `/quit` `/exit` (or `Ctrl-C`) | Graceful shutdown — kills agents, cleans temp files |

---

## Features

- **Cross-vendor** — Claude and Codex (and more) collaborate in one thread. Add/remove agents live with `/add` / `/kick`.
- **Clean transcript** — each message is a "bubble": a separator rule + colored `▌ author · time` bar + indented body, so multi-line replies never blur together.
- **Isolated input** — incoming messages print *above* your prompt; what you're typing is never split by an agent that speaks mid-keystroke.
- **"Working…" indicator** — `[room] <agent> is working…` shows while an agent is busy on a task, so a long file search doesn't look like a hang.
- **Concurrent rooms** — every room namespaces its bus identity (`room-<id>`, agents `<id>-<name>`), so you can run several rooms at once without them clobbering each other.
- **Circuit-breaker** — if agents talk among themselves for N turns with no human input, fan-out pauses and hands the turn back to you (`/resume` or just type to continue).
- **Restart-resilient** — if the AgentBus daemon restarts under a live room, the hub reconnects, re-registers, and resumes the conversation.
- **Reconnectable** — `--resume` reopens a closed room with its history replayed and each agent's session restored; `--no-agents` re-attaches to agents that outlived the hub. See [Reconnecting](#reconnecting).
- **Works in your repo** — agents run with your launch dir as their working root (Codex via `--cd`), so they operate on the real project, not a sandbox.
- **Paste-aware input** — a pasted multi-line block becomes one message, not one per line.

---

## How it works

AgentBus is a **unicast** message bus (point-to-point, by recipient name). The room
hub is the **broadcast/relay layer** AgentBus doesn't provide — an IRC-server pattern:

```
          ┌──────────────── one terminal screen ─────────────────┐
          │  agentbus-room <id>   (registered as 'room-<id>')    │
          │   • consume-loop: socket Read --wait as room-<id>    │
          │   • re-fan each msg to other members (sender excl.)  │
          │   • DB-tail render (rowid cursor; strips <id>- pfx)  │
          │   • human input line → relay to all (or @mention)    │
          │   • circuit-breaker · working-indicator · reconnect  │
          └───────▲──────────────────▲────────────────▲──────────┘
                  │ --to room-<id>    │ --to room-<id> │ human input
      ┌───────────┴────┐  ┌───────────┴─────┐   ┌──────┴───┐
      │ <id>-claude-A  │  │ <id>-codex-A    │   │   ram    │
      │ (PTY, claude)  │  │ (PTY, codex)    │   │ (human)  │
      └────────────────┘  └─────────────────┘   └──────────┘
```

**Key design choices:**

- **Namespaced identities.** The hub registers as `room-<id>`; each agent as `<id>-<name>` on the bus, while the *display* name stays `<name>`. Display→busId conversion happens only at outgoing bus boundaries; the `<id>-` prefix is stripped back at incoming/render boundaries. This is why concurrent rooms don't collide.
- **Everyone addresses the room.** Agents always `send --to room-<id>`. The hub consumes those, then re-fans `--from room-<id> --to <member>` with the real author moved into the body (`"claude-A: …"`) — so each agent naturally replies to the room and no message is missed.
- **Render from the DB tail**, not in-memory state (`WHERE thread_id=<id> AND from_agent != room-<id> ORDER BY rowid`). Off-protocol messages still show; relay copies (`from_agent = room-<id>`) are filtered to avoid double-rendering. The human's own line is echoed locally (the render filter suppresses `from=room`).
- **Socket for reads** (structured, no envelope loss); **shell-out for sends** (avoids evicting the consume socket's registration).
- **Trust establishment** — agents would otherwise refuse bus-injected instructions as prompt-injection. Each agent is launched with a system prompt declaring the room channel legitimate (Claude: `--append-system-prompt-file`; Codex: `AGENTS.md` in its `CODEX_HOME`, read as global instructions), while keeping normal judgment on destructive actions.

### Agent launch recipes
- **Claude** — `agentbus run --name <id>-<name> -- claude --dangerously-skip-permissions --strict-mcp-config --mcp-config <empty> --append-system-prompt-file <prompt> --session-id <uuid>`. MCP is disabled for a fast, clean boot. The pinned `--session-id` lets `--resume` relaunch `claude --resume <uuid>`.
- **Codex** — `agentbus run --name <id>-<name> -- codex --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --cd <launchDir>` under a **stable isolated `CODEX_HOME`** (under `…/rooms/<id>/codex-<name>/`; auth symlinked from `~/.codex`; a minimal `config.toml` pre-trusts **your launch dir** and omits MCP servers so there's no folder-trust modal and no heavy MCP boot). The room prompt is delivered via `<CODEX_HOME>/AGENTS.md` (global instructions) so the user's repo is never polluted. `--resume` relaunches `codex resume --last --all` in that same home.

Add a program by writing a `launch<X>Agent` and extending `parseAgentSpec`'s `VALID_PROGRAMS`.

---

## I/O contract

The hub speaks the AgentBus socket protocol directly. Four delivery shapes arrive; all handled:

| Path | Shape | When |
|------|-------|------|
| A — queued batch | `{"type":"Ok","data":[...]}` | Messages already in DB when Read arrives |
| B — live push | `{"type":"Message","message":{...}}` | Pushed while the hub was waiting |
| C — empty | `{"type":"Ok","data":[]}` | No messages, `wait=false` |
| D — timeout | `{"type":"Error","message":"No messages (timeout)"}` | `wait=true`, 30s elapsed (normal — re-issue) |

---

## Self-test

```sh
agentbus-room --self-test     # exit 0 = pass, 1 = fail; no daemon/agents needed (CI-safe)
```

Covers the pure logic: `parseReadLine` (all I/O paths), `computeFanout` (sender
exclusion, body prefixing, `@mention` narrowing), `CircuitBreaker`, `LineReader`
buffer splitting, `agentBusId`/`displayName` namespacing, `completeMention`
(`@`+Tab), `parseAgentSpec` (`/add` parsing), `coalesceLines` (paste coalescing),
`buildCodexConfigToml` (launch-dir trust), and the resume helpers
(`stateFilePathFor`, `codexHomeFor`, state serialize/deserialize round-trip,
`buildReplaySql`).

Runtime behaviors that need a **live** room to verify (not unit-tested): paste
coalescing timing, codex `--cd <launchDir>`, codex `AGENTS.md`-from-`CODEX_HOME`,
`codex resume --last --all`, and claude `--session-id` capture + `--resume <uuid>`.

---

## Troubleshooting

- **Agent exits immediately at launch (code 1).** Usually a bad MCP config or an untrusted launch dir. Claude needs the empty MCP config as `{"mcpServers":{}}` (a bare `{}` fails `--strict-mcp-config`); Codex needs your launch dir pre-trusted (the hub's isolated `CODEX_HOME` `config.toml` handles this).
- **Two rooms "fighting" (EPIPE / reconnect storm).** Pre-namespacing behavior — fixed: ensure you're on the current version where each room is `room-<id>`. Distinct room ids never collide.
- **Codex slow to boot / spawns apps.** Codex loads `config.toml` MCP servers (some launch desktop apps). The room runs Codex under an isolated `CODEX_HOME` with no MCP to avoid this; if you customize, keep MCP out of that config.
- **No "working…" but nothing appears.** The agent is likely doing real work (file search, reading). Give it time; the bubble appears when it replies.

---

## Files written

| Path | Description |
|------|-------------|
| `room-<id>.log` | Appended plain-text conversation log (cwd) |
| `<tmp>/room-sp-<name>-<id>.txt` | Claude system-prompt file (cleaned on exit / `/kick`) |
| `<tmp>/empty-mcp-<id>.json` | Empty MCP config for Claude agents (per room) |
| `<AGENTBUS_DIR or ~/.agentbus>/rooms/<id>.json` | Room state file for `--resume` (roster + each agent's session handle) |
| `<AGENTBUS_DIR or ~/.agentbus>/rooms/<id>/codex-<name>/` | **Stable** isolated `CODEX_HOME` (config + symlinked auth + `AGENTS.md`). Kept on shutdown so `--resume` works; deleted only on `/kick`. |

---

## Roadmap / not yet

- **Model-A "council" mode** — a driven turn-loop (rounds, critic, synthesis) as an alternative to the emergent room. `CircuitBreaker` + `computeFanout` primitives are reusable.
- **Web/Tauri surface** — swap the `readline` input for a WebSocket bridge; relay + render logic unchanged.
- **Native subcommand** — fold the hub into the Rust binary as `agentbus room <id>`.

---

*The room hub is a zero-dependency Node ES module (`agentbus-room.mjs`). Run `agentbus-room --self-test` to verify after any change.*
