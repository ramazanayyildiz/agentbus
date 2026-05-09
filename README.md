# AgentBus

A purpose-built message bus for AI coding agents — Claude Code, Codex, Aider,
opencode — that lets them talk to each other automatically. Without tmux.

Think of it as `tmux send-keys` for any terminal-based agent, with a SQLite-
backed message queue, registration, and routing on top.

## What it does

```
┌──────────────────┐                        ┌──────────────────┐
│ Terminal A       │                        │ Terminal B       │
│ (Warp, iTerm...) │                        │ (Warp, iTerm...) │
│                  │                        │                  │
│ $ agentbus run   │                        │ $ agentbus send  │
│   --name codex   │                        │   --from atlas   │
│   -- codex --yolo│   ◄── inject(PTY) ─◄  │   --to codex     │
│                  │                        │   "scrape this"  │
│  [codex inside,  │                        │                  │
│   sees pasted    │                        │                  │
│   message]       │                        │                  │
└──────────────────┘                        └──────────────────┘
         │                                           │
         │            ┌──────────────┐               │
         └────► sock ►│  agentbusd   │ ◄ sock ◄──────┘
                      │  (daemon)    │
                      │  + SQLite    │
                      │  (~/.agentbus/)│
                      └──────────────┘
```

The daemon owns the routing + persistence. The `agentbus run` wrapper owns
the PTY of the agent it wraps and bridges bus messages into the agent's
terminal as if they were typed.

## Architecture

Four crates in a workspace:

| Crate           | Role                                                            |
|-----------------|-----------------------------------------------------------------|
| `agentbus-core` | Shared types, SQLite schema, request/response protocol          |
| `agentbusd`     | Async tokio daemon, listens on `~/.agentbus/agentbus.sock`     |
| `agentbus-cli`  | `agentbus` CLI binary (start, register, send, read, run, ...)   |
| `agentbus-pty`  | PtyRunner — wraps an agent process, bridges PTY ↔ bus           |

Plus an end-to-end integration test crate at `tests/`.

## Quick start

```bash
# 1. Build
cd ~/CODE/Ram/agentbus
cargo build --release

# 2. Start the daemon (idempotent — checks PID file)
./target/release/agentbus start

# 3. Wrap your first agent in Terminal A:
./target/release/agentbus run \
  --name codex \
  --program codex \
  --model gpt-5.5 \
  --project myproj \
  --transcript /tmp/codex.log \
  -- codex resume <session-id> --yolo

# 4. Wrap a second agent in Terminal B:
./target/release/agentbus run \
  --name claude2 \
  --program claude \
  --model opus \
  --project myproj \
  -- claude --dangerously-skip-permissions

# 5. From a third terminal, send messages between them:
./target/release/agentbus send \
  --from atlas \
  --to codex \
  --msg-type request \
  "Hey codex, what are you working on?"

# Codex receives the message inside its terminal as a pasted line:
#   [agentbus from=atlas type=request] Hey codex, what are you working on?
# It can reply via:
#   $ agentbus send --from codex --to atlas --msg-type response "..."
```

## Subcommands

```
agentbus start              # start daemon (idempotent)
agentbus status             # list registered agents
agentbus list               # list registered agents (alias)
agentbus register --name <N> --program <P> [--model <M>] [--project <Pr>]
agentbus send --from <F> --to <T> --msg-type <T> [--thread-id <I>] <BODY>
agentbus read   --name <N> [--wait] [--timeout <SECS>]
agentbus close  --name <N>
agentbus run    --name <N> [--program <P>] [--transcript <PATH>] [--restart] -- <CMD...>
```

## How `agentbus run` works under the hood

When you run `agentbus run --name foo -- some-cmd`, the wrapper:

1. **Connects to the daemon** via Unix socket and sends `Register{name=foo, ...}`.
2. **Spawns `some-cmd`** inside a `portable-pty` PTY — owns the master,
   the child sees a real TTY.
3. **Puts your local stdin into raw mode** (no-op if stdin isn't a TTY).
4. **Runs four concurrent tasks**:
   - **A**: local stdin → PTY writer mpsc (your typing goes through)
   - **B**: bus `Read{wait:true}` loop → PTY writer mpsc (incoming
     messages get formatted by an adapter and queued)
   - **C**: PTY master → local stdout (transparent passthrough; also
     stamps "last output time" for idle detection and writes to the
     transcript file if configured)
   - **D**: single PTY writer task drains the mpsc — serializes writes
     so a long bus message never interleaves with your keystrokes
5. **Waits for the child to exit** and propagates the exit code.

### Adapter selection

`agentbus-pty` ships four built-in adapters, picked by case-insensitive
substring match against `--program`:

| Adapter   | Match     | Format                                  | Idle gate |
|-----------|-----------|-----------------------------------------|-----------|
| `claude`  | "claude"  | `\x1b[200~ envelope \x1b[201~\r` (paste) | 750ms    |
| `codex`   | "codex"   | `\x1b[200~ envelope \x1b[201~\r` (paste) | 750ms    |
| `aider`   | "aider"   | `envelope\r` (plain, readline-friendly)  | 500ms    |
| `generic` | (default) | `envelope\r`                              | 0ms      |

The "envelope" is a single line:
```
[agentbus from=<from> type=<type>[ thread=<id>]] <body>
```

### Idle-time gating

If the wrapped agent is mid-response (streaming output), the runner waits
until the PTY has been quiet for `idle_ms_before_inject()` ms before
delivering the next bus message. Capped at 30s — a wedged agent can't
permanently block the bus.

### Sanitization

Before any body hits the PTY, control characters are stripped:
- Drop: 0x00..=0x1F (except tab), 0x7f (DEL), 0x80..=0x9f (C1)
- Keep: tab, printable ASCII, all Unicode ≥ 0xa0 (UTF-8 safe)

So a malicious or buggy `agentbus send` can't smuggle Ctrl-C or terminal
manipulation codes through the bus.

## Database layout (`~/.agentbus/bus.db`)

```sql
agents(id PK, name UNIQUE, program, model, project, state, pid, registered_at)
messages(id PK, from_agent FK, to_agent FK, thread_id, msg_type, body,
         metadata, read_at, claimed_at, created_at)
```

`messages` uses `claimed_at` to give at-least-once delivery without
duplicates: the daemon claims a message in the same transaction it returns
it, only marks `read_at` after the socket write succeeds, and releases the
claim if the write fails (so the next reader picks it up).

WAL mode + `synchronous=NORMAL` for concurrent readers.

## Build phases (history)

This codebase was built in clean phases — see git log for the breadcrumbs.

| Phase | What                                                      |
|-------|-----------------------------------------------------------|
| 0     | PTY smoke test — bracketed paste + byte transparency      |
| 1     | `agentbus run` — PTY wrap + 4-task bridge                 |
| 2     | Adapter trait — per-agent injection profiles              |
| 3     | Idle-time gating — wait for PTY quiet before injecting    |
| 4     | Transcript file capture (lite — file-backed)              |
| 5     | Restart on non-zero exit, with bounded backoff            |

## Known limitations / TODO

From an external review of the daemon code:

- **HIGH**: any connection can `Unregister` any agent; daemon doesn't
  enforce that only the owning session can close itself.
- **HIGH**: `unregister_agent` deletes from `agents` while `messages` has
  FKs on `agents(name)` with no `ON DELETE` behavior — once an agent has
  message history, unregister can fail or leave inconsistent state.
- **MEDIUM**: daemon startup blindly removes the socket path before bind.
  A second daemon could break the first daemon's socket — should use a
  lock file plus a live-socket probe.
- **MEDIUM**: pushed messages and request responses share one stream with
  no request IDs — fine for the current CLI, fragile for long-lived
  PTY-runner clients issuing requests while also receiving pushed messages.
- **MEDIUM**: no `0700` permission hardening on `~/.agentbus` or the
  socket — anyone on the machine can connect.

Future phases:

- Real session/transcript tables in SQLite (Phase 4 currently uses files).
- Adapter `is_prompt_ready(tail)` for regex-based prompt detection on top
  of the idle gate.
- Bracketed-paste fallback to per-character typing for adapters whose
  inner agent doesn't recognize the markers.
- MCP server wrapper so Claude Code (and any other MCP-aware agent) can
  send/read on the bus via tools instead of CLI.
- Hook integration so a Claude Code session auto-registers + auto-reads
  on session start without needing `agentbus run` wrapping it.

## Demo: two mock agents talking

The `smoke/` directory has a self-contained PoC:

```bash
# From repo root, with daemon running:
./target/release/agentbus run --name worker --program test \
  -- ./smoke/target/release/mock-agent &

# In another terminal:
./target/release/agentbus register --name atlas --program test
./target/release/agentbus send --from atlas --to worker \
  --msg-type request "hello worker, please reply OK"

# The mock-agent (a raw-mode stdin reader that prints every byte it sees)
# receives the full envelope byte-for-byte, plus a CR that triggers
# [SUBMIT received].
```

`smoke/mock_agent.rs` is the easiest way to reason about what the inner
agent is actually receiving — it ignores nothing and prints everything.

## License

MIT

## Related

- Built as a replacement for [`smux`](https://github.com/ShawnPana/smux) / tmux-bridge
- Inspired in part by [Augment Intent's](https://www.augmentcode.com/product/intent) coordinator/implementor/verifier pattern
