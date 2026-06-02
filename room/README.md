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
| `--no-agents` | off | Don't launch agents — attach to ones already running via `agentbus run`. |
| `--self-test` | — | Run the pure-logic test suite (no agents, no daemon) and exit. |

```sh
agentbus-room standup --agents pm:claude,eng:codex,qa:claude   # 3 agents, mixed
agentbus-room review  --agents claude-A,codex-A:codex --cb-max 10
```

---

## In-room commands

| Command | Action |
|---------|--------|
| *(just type)* | Send to everyone in the room |
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
- **Trust establishment** — agents would otherwise refuse bus-injected instructions as prompt-injection. Each agent is launched with a system prompt declaring the room channel legitimate (Claude: `--append-system-prompt-file`; Codex: `AGENTS.md` in its workdir), while keeping normal judgment on destructive actions.

### Agent launch recipes
- **Claude** — `agentbus run --name <id>-<name> -- claude --dangerously-skip-permissions --strict-mcp-config --mcp-config <empty> --append-system-prompt-file <prompt>`. MCP is disabled for a fast, clean boot.
- **Codex** — `agentbus run --name <id>-<name> -- codex --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --cd <workdir>` under an **isolated `CODEX_HOME`** (auth symlinked from `~/.codex`; a minimal `config.toml` pre-trusts the workdir and omits MCP servers so there's no folder-trust modal and no heavy MCP boot). The room prompt is delivered via `<workdir>/AGENTS.md`.

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
(`@`+Tab), and `parseAgentSpec` (`/add` parsing).

---

## Troubleshooting

- **Agent exits immediately at launch (code 1).** Usually a bad MCP config or an untrusted launch dir. Claude needs the empty MCP config as `{"mcpServers":{}}` (a bare `{}` fails `--strict-mcp-config`); Codex needs its workdir pre-trusted (the hub's isolated `CODEX_HOME` handles this).
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
| `<tmp>/codex-home-<id>-<name>/` | Isolated `CODEX_HOME` (config + symlinked auth) |
| `<tmp>/codex-work-<id>-<name>/` | Codex workdir + `AGENTS.md` trust file |

---

## Roadmap / not yet

- **Model-A "council" mode** — a driven turn-loop (rounds, critic, synthesis) as an alternative to the emergent room. `CircuitBreaker` + `computeFanout` primitives are reusable.
- **Web/Tauri surface** — swap the `readline` input for a WebSocket bridge; relay + render logic unchanged.
- **Native subcommand** — fold the hub into the Rust binary as `agentbus room <id>`.

---

*The room hub is a zero-dependency Node ES module (`agentbus-room.mjs`). Run `agentbus-room --self-test` to verify after any change.*
