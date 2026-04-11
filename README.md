# agentbus

> Rust daemon + CLI for bidirectional agent-to-agent communication across any terminal.

`agentbus` lets multiple AI coding agents (Claude Code, Codex, Qwen, Gemini — anything that runs in a terminal) send structured messages to each other through a background daemon. It replaces `tmux-bridge` / `smux` with a proper message bus: no polling, no fragile text parsing, no race conditions, and it works across any terminal emulator — not just tmux panes.

## Why

When you orchestrate multiple AI agents on a task today, coordination is done either through tmux pane text-pasting (unreliable), or one agent calls another as a subprocess (no bidirectional flow). Neither scales past a toy demo.

`agentbus` gives you:

- **Structured messaging** — typed requests/responses/status/errors, not raw terminal text
- **Instant push delivery** — a blocking `read --wait` receives messages the moment they arrive, no `sleep`-based polling
- **Any terminal** — Warp, iTerm, VS Code, SSH sessions. Agents just need to run `bash`
- **At-least-once delivery** — messages survive agent crashes via a SQLite claim protocol
- **Crash-safe** — killed agents release their claims on reconnect; daemon cleans up orphans

## How it works

```
Terminal 1              Terminal 2              Terminal 3
┌────────────┐         ┌────────────┐         ┌────────────┐
│ Claude     │         │ Codex      │         │ Qwen       │
│ $ agentbus │         │ $ agentbus │         │ $ agentbus │
│   send     │         │   read     │         │   register │
│   ...      │         │   --wait   │         │            │
└─────┬──────┘         └─────┬──────┘         └─────┬──────┘
      │                      │                      │
      └──────────────────────┼──────────────────────┘
                             │ Unix socket
                    ┌────────▼────────┐
                    │   agentbusd     │
                    │   (daemon)      │
                    │                 │
                    │ SQLite bus.db   │
                    │ mpsc push chan  │
                    │ tokio::select!  │
                    └─────────────────┘
```

A background daemon (`agentbusd`) listens on a Unix socket at `~/.agentbus/agentbus.sock`. Agents connect via the `agentbus` CLI, register themselves, and send/receive messages. Each connection holds an open socket; when another agent sends to you, the daemon pushes the message through your existing connection via `tokio::select!` — no polling.

All messages persist to SQLite at `~/.agentbus/bus.db`. If a delivery fails (socket closed mid-write), the claim is released and the next connection picks the message up — that's the at-least-once guarantee.

## Usage

```bash
# Terminal 1 — start the daemon (once per machine)
agentbusd

# Terminal 2 — alice registers and waits for messages
agentbus register --name alice --program claude-code
agentbus read --name alice --wait --timeout 60

# Terminal 3 — bob sends alice a message
agentbus register --name bob --program codex
agentbus send --from bob --to alice --msg-type request "Review src/auth.ts"
# → alice's terminal immediately prints: "Message from bob: Review src/auth.ts"
```

### Commands

| Command | Description |
|---------|-------------|
| `agentbus register --name X --program Y` | Register an agent by name |
| `agentbus list` | List all registered agents |
| `agentbus send --from X --to Y "body"` | Send a message |
| `agentbus read` | Read queued messages (non-blocking) |
| `agentbus read --wait --timeout 60` | Block until a message arrives or timeout |
| `agentbus close --name X` | Unregister an agent |
| `agentbus status` | Show daemon status |

### Message types

`request` · `response` · `done` · `question` · `error` · `status`

## Build

Requires Rust 1.94+.

```bash
git clone https://github.com/ramazanayyildiz/agentbus
cd agentbus
cargo build --release
# Binaries: target/release/agentbusd + target/release/agentbus
```

## Test

```bash
cargo test --workspace
```

70 tests covering unit, database, integration, concurrency, and end-to-end CLI flows. All pass.

## Architecture

Workspace with 3 crates:

| Crate | Purpose |
|-------|---------|
| `agentbus-core` | Types, protocol, SQLite database layer |
| `agentbusd` | Daemon binary — listens on Unix socket, brokers messages |
| `agentbus-cli` | CLI client — `agentbus` binary users invoke |

### Design principles

- **No MCP dependency.** Uses structured JSON-over-Unix-socket. Any agent that can run bash can participate.
- **At-least-once, not at-most-once.** Claim-then-write-then-ack pattern with transactional claims (`BEGIN IMMEDIATE`).
- **Instant push.** `tokio::select!` concurrently polls the socket and the per-agent mpsc receiver, so pushed messages arrive in ~20ms regardless of whether the client is in `read --wait`.
- **No shared runtime stalls.** All SQLite operations go through `tokio::task::spawn_blocking` so async tasks never block on disk I/O.
- **Bounded backpressure.** Per-agent push channel is capped at 1000 messages; overflow releases the claim so the message stays redeliverable.

## Status

**Phase 1** — working prototype. Daemon + CLI functional, 70 tests passing. Tested on macOS. Linux support is intended but untested.

Planned:
- Layer 2: Tauri desktop app with embedded terminals and agent chat UI
- Thread-scoped conversations (same-screen mode)
- Agent capability discovery for automatic routing
- Distributed (multi-machine) coordination

## License

MIT

## Related

- Built as a replacement for [`smux`](https://github.com/ShawnPana/smux) / tmux-bridge
- Inspired in part by [Augment Intent's](https://www.augmentcode.com/product/intent) coordinator/implementor/verifier pattern
