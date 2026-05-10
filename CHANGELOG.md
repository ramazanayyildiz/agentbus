# Changelog

All notable changes to agentbus will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-10

First public release. Verified end-to-end with real Claude Code, Codex, aider,
and opencode sessions. 87 tests passing, all HIGH and MEDIUM bugs from external
review fixed.

### Added

- **Core daemon** (`agentbusd`) — Tokio async server on a Unix socket, SQLite
  persistence, at-least-once delivery via claim/release pattern.
- **CLI client** (`agentbus`) — `start`, `register`, `list`, `send`, `read`,
  `close`, `status`, and `run` subcommands.
- **PTY runner** (`agentbus run`) — wraps any terminal-based agent process
  in a pseudo-terminal, registers it on the bus, and bridges incoming bus
  messages into the agent's input as if pasted by a user. Four concurrent
  tasks: stdin → PTY, PTY → stdout, bus → PTY (via mpsc serializer),
  reconnect-aware bus listener.
- **MCP server** (`agentbus-mcp`) — Model Context Protocol bridge so
  MCP-aware hosts (Claude Code, Codex, opencode) can expose `agentbus_send`,
  `agentbus_inbox`, `agentbus_list`, `agentbus_register`, `agentbus_whoami`
  as tools to their LLMs. Two-connection architecture (RPC + Poll) avoids
  Read-wait deadlocks.
- **Adapter trait** with four built-in profiles:
  - `ClaudeAdapter` — bracketed paste + 750 ms idle gate
  - `CodexAdapter` — bracketed paste + 750 ms idle gate
  - `OpencodeAdapter` — bracketed paste + 750 ms idle gate
  - `AiderAdapter` — plain text + 500 ms idle gate
  - `GenericAdapter` — fallback, plain text, no idle gate
- **Auto-name** for `agentbus run`: `--name` is optional; the basename of
  the wrapped command is used, with a numeric suffix on collision.
- **Auto-reconnect** in the PTY runner with exponential backoff (250 ms →
  8 s) when the bus daemon disconnects.
- **Dead-connection sweep** on daemon startup: rows previously in `active`
  state get flipped to `disconnected` so `agentbus status` reflects reality.
- **Soft-delete** on Unregister: state flips to `unregistered` instead of
  DELETE, preserving message-history FKs.
- **Permission hardening**: `~/.agentbus/` chmod 0700, socket chmod 0600.
- **Socket race protection**: daemon refuses to start if another daemon's
  PID is alive (PID-file lock check).
- **PTY auto-size** + SIGWINCH propagation so the wrapped agent's TUI
  renders at the same dimensions as the user's local terminal.
- **Transcript file** capture via `--transcript <path>`.
- **Restart on failure** via `--restart` + `--max-restarts <N>`.
- **Cross-platform releases** via `cargo-dist`: macOS arm64, macOS x64,
  Linux arm64, Linux x64. Single tarball per platform with all three
  binaries. One-line shell installer + Homebrew formula auto-generated.

### Verified

- Real-world bidirectional roundtrip: Claude Opus → Codex GPT-5.5 → Claude
  Opus, no tmux, no manual copy/paste.
- 10-agent load test: 220 messages, 220 delivered, 0 unclaimed,
  ~232 msg/s (CLI overhead bottleneck).
- aider 0.86.2 + opencode TUI both render injected envelopes correctly.

### Security

- HIGH: only the connection that registered an agent name can `Unregister`
  it (ownership check in daemon).
- HIGH: `unregister_agent` is now a soft-delete (state → `unregistered`)
  so message FKs to `agents(name)` don't break once history exists.
- MEDIUM: daemon refuses to start if another daemon's PID is alive
  (PID-file probe via `kill 0`).
- MEDIUM: 0700 on `~/.agentbus/`, 0600 on `~/.agentbus/agentbus.sock`.

### Not yet supported

- Native Windows. The daemon uses Unix sockets and the PTY runner uses
  Unix-only ioctls/termios. Use WSL.
- Sessions / transcript SQLite tables. The current `--transcript <path>`
  writes to a file; per-session DB rows are deferred.
- Server → client MCP notifications. Hosts currently poll
  `agentbus_inbox`.
- Adapter `is_prompt_ready(tail)` regex hooks. Idle-time gating covers
  the practical cases.
- Bracketed-paste fallback to per-character typing for adapters whose
  inner agent doesn't recognize the markers.
