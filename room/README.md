# AgentBus Terminal Room HUB

The **room hub** (`agentbus-room.mjs`) is the broadcast / relay layer that
AgentBus itself does not provide. AgentBus is unicast-only; the hub is the
IRC-server that makes multi-party rooms work.

## Architecture

```
          ┌──────────────── one terminal screen ─────────────────┐
          │  agentbus-room <room-id>   (registered as 'room')    │
          │   • consume-loop: socket Read --wait as room          │
          │   • re-fan each msg to other members (sender-excl.)  │
          │   • DB-tail render (rowid cursor, from_agent != room)│
          │   • human input line → relay to all (or @mention)   │
          │   • circuit-breaker (max consecutive agent turns)     │
          └───────▲──────────────────▲────────────────▲──────────┘
                  │ --to room         │ --to room      │ human input
      ┌───────────┴───┐   ┌───────────┴──────┐   ┌────┴────┐
      │ claude-A (PTY)│   │ codex-A (PTY)    │   │  ram    │
      │ agentbus run  │   │ agentbus run     │   │ (human) │
      └───────────────┘   └──────────────────┘   └─────────┘
```

**Key design choices:**

- Agents always send `--to room`. The hub re-fans as `--from room --to <member>`,
  with the real author in the body (`"claude-A: <text>"`). This ensures agents
  always reply `--to room` naturally and the hub never misses a reply.

- Render source is the **DB tail** (`WHERE thread_id=room AND from_agent != room ORDER BY rowid`),
  not the in-memory relay state. Off-protocol / misaddressed messages still appear.
  Relay copies (`from_agent = room`) are filtered out to prevent double-rendering.

- Human input is **echoed locally** (not via DB tail) because the hub relays it
  as `from=room`, which the render filter suppresses.

- **Socket for reads** (structured, no envelope loss). **Shell-out for sends**
  (avoids second-Register eviction on the consume socket).

## Prerequisites

- `agentbus` daemon binary at `~/.local/bin/agentbus`
- `sqlite3` CLI available (`brew install sqlite` or system package)
- Node.js 18+ (uses ES modules, `node:net`, `node:readline`)
- A trusted working directory for agent launch (avoids Claude's folder-trust modal)

## Usage

```sh
# Standard: launch hub with two agents, room ID "main"
node agentbus-room.mjs main

# Custom agents — each spec is `name[:program]` (program = claude | codex)
node agentbus-room.mjs main --agents claude-A,claude-B

# Mixed Claude + Codex room (bare name defaults to claude)
node agentbus-room.mjs main --agents claude-A,codex-A:codex

# Custom circuit-breaker threshold (default: 6 consecutive agent msgs)
node agentbus-room.mjs main --cb-max 4

# Launch agents in a specific trusted directory
node agentbus-room.mjs main --launch-dir /Users/you/CODE/myproject

# Hub only (no agent launch — agents already running via agentbus run)
node agentbus-room.mjs main --no-agents

# Self-test (verifies relay logic with no real agents)
node agentbus-room.mjs --self-test
```

## In-room commands

| Command       | Action                                        |
|---------------|-----------------------------------------------|
| `/status`     | Show room ID, members, circuit-breaker state  |
| `/resume`     | Unblock circuit-breaker after auto-pause      |
| `/help`       | List commands                                 |
| `/quit`       | Graceful shutdown, kills agent processes      |
| `@claude-A …` | Narrow input to a single agent               |

## Self-test (no real agents spawned)

```sh
node agentbus-room.mjs --self-test
```

Exercises all pure relay logic with fixture data:

- `parseReadLine` — all four I/O contract paths (batch, push, empty, timeout, error)
- `computeFanout` — sender exclusion, body prefixing, `@mention` narrowing
- `CircuitBreaker` — trip at N, reset on human input, re-trip after reset
- `LineReader` — chunked delivery buffer splitting

Exit code 0 = all pass. Exit code 1 = failures (safe to run in CI with no daemon).

## I/O Contract

The hub speaks the AgentBus socket protocol directly. Two distinct delivery shapes
arrive on the socket; both are handled:

| Path | Shape | When |
|------|-------|------|
| A — queued batch | `{"type":"Ok","data":[...]}` | Messages already in DB when Read arrives |
| B — live push | `{"type":"Message","message":{...}}` | Message pushed while hub was waiting |
| C — empty | `{"type":"Ok","data":[]}` | No messages, wait=false |
| D — timeout | `{"type":"Error","message":"No messages (timeout)"}` | wait=true, 30s elapsed |

Timeout (path D) is the normal case. The hub silently re-issues the Read request.

## Circuit-Breaker

If N consecutive agent messages arrive with no human input (default N=6), fan-out
is paused and the hub prints a prompt to the human. Use `/resume` to unblock or
just type any message.

## Render vs Relay: a note on two different axes

The **consume-loop** filters by *recipient* (`to_agent = room`).
The **DB-tail render** filters by *thread_id* (`thread_id = room AND from_agent != room`).
These are different views:

- Agent sends `--to room --thread-id X` (X ≠ roomId): relayed but not rendered.
- Agent sends `--to other-agent --thread-id roomId`: rendered but not relayed.

Both are acceptable for MVP. The render catches divergence for the human to see.

## Files written

| File | Description |
|------|-------------|
| `agentbus-room.mjs` | Hub implementation (ES module, zero npm deps) |
| `room-system-prompt.template.txt` | Per-agent system-prompt template |
| `room-<id>.log` | Appended conversation log (created at runtime) |
| `/tmp/room-sp-<agent>-<room>.txt` | Temp system-prompt files (cleaned on exit) |
| `/tmp/room-transcript-<agent>-<room>.txt` | PTY transcript files |

## Extending

**Per-program launch (Claude + Codex, implemented):**
`launchAgent(name, program, ...)` dispatches to `launchClaudeAgent` (unchanged: spawns
`claude --append-system-prompt-file …` with an empty strict MCP config) or
`launchCodexAgent` (spawns `codex --dangerously-bypass-approvals-and-sandbox
--dangerously-bypass-hook-trust --cd <workdir>` under an isolated per-agent
`CODEX_HOME`). Codex auth is symlinked from `~/.codex` (auth.json, accounts,
version.json, models_cache.json); `config.toml` pre-trusts the workdir and omits MCP
servers; the room system prompt is delivered via `<workdir>/AGENTS.md` (same text the
Claude path puts in its `--append-system-prompt-file`). Select per agent with
`--agents name:program`. To add another program, add a `launchXAgent` and extend
`parseAgentSpec`'s `VALID_PROGRAMS`.

**Model A council mode:**
Swap the emergent consume-loop for a driven turn-loop. The `CircuitBreaker` and
`computeFanout` primitives remain unchanged; only the orchestration changes.

**Web/Tauri surface:**
Replace the `readline` human input with a WebSocket bridge. The hub's relay and
render logic stays identical.
