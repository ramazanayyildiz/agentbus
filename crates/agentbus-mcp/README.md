# agentbus-mcp

A Model Context Protocol server that bridges any MCP-aware host (Claude
Code, Codex, opencode...) to the agentbus daemon. Exposes the bus as a
small set of JSON-RPC tools that the host can call directly — no PTY
wrapping required.

## Why this exists alongside `agentbus run`

| | `agentbus run` (PTY) | `agentbus-mcp` (MCP) |
|---|---|---|
| Universal | ✅ any TUI | ❌ MCP-aware hosts only |
| No session restart | ❌ wraps from process start | ✅ host launches it |
| Visual envelope in input field | ✅ user sees `[agentbus from=...]` | ❌ tool calls are invisible |
| Programmatic control | ❌ user-prompt-driven | ✅ host can call tools mid-task |
| Aider, opencode (no MCP) | ✅ | ❌ |
| Claude Code, Codex (MCP) | ✅ via wrapper | ✅ native, cleaner |

Use both. PTY runner is the universal fallback for non-MCP agents. MCP
server is the cleaner integration when the host supports it.

## Tools exposed

| Tool | Purpose |
|---|---|
| `agentbus_send` | Publish a message to another agent (with type, thread_id) |
| `agentbus_inbox` | Drain the buffered queue of pushed messages |
| `agentbus_list` | List all agents registered on the bus |
| `agentbus_register` | Re-register this MCP server under a different name |
| `agentbus_whoami` | Returns the name this server is currently registered as |

`agentbus_inbox` is the polling counterpart to PTY-runner's automatic
injection. The MCP host calls it whenever it wants to check for new
messages — e.g. before responding to the user, or after a tool call.

## Architecture

The server opens **two** independent Unix-socket connections to
`agentbusd`:

  - **RPC connection** — used by tool dispatches (Send, List). Short
    request/response, never blocks.
  - **Poll connection** — registered on the bus; the agent's push channel
    lives here; sits in `Read{wait:true,timeout=300}` waiting for pushed
    messages, which are buffered into an inbox the `agentbus_inbox` tool
    drains.

Two connections are required: a single shared connection deadlocks
because the daemon won't process a Send while a Read is blocked, and
Read{wait:true} can block for 5 minutes.

```
MCP host (Claude Code, Codex, ...)
         │ JSON-RPC over stdio (newline-delimited)
         ▼
   agentbus-mcp ───── RPC conn ────► agentbusd
         │                            │
         └────── Poll conn ───────────┘
                  (registered, holds push channel)
```

## Usage

### Claude Code config

Add to `~/.claude/.mcp.json` (or whatever your MCP config path is):

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "/Users/you/CODE/Ram/agentbus/target/release/agentbus-mcp",
      "args": ["--name", "claude-main", "--program", "claude", "--model", "opus"]
    }
  }
}
```

On next session start, Claude Code launches `agentbus-mcp` as a child
process. It auto-registers as `claude-main` on the bus. Claude can now
call tools:

```
> Use the agentbus_list tool to see who's online.
> Use agentbus_send to ping the codex agent: "Are you done with the auth task?"
> Use agentbus_inbox to drain new messages.
```

### Standalone test

```bash
# Start daemon if not running
agentbus start

# Pipe JSON-RPC requests in, see responses out
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | agentbus-mcp --name test
```

## CLI flags

```
agentbus-mcp [--name <NAME>] [--program <P>] [--model <M>] [--project <Pr>] [--no-auto-register]
```

  - `--name`              — agent name to register (default: `mcp-<pid>`)
  - `--program`           — program label (default: `mcp`)
  - `--model`             — model label (default: `unknown`)
  - `--project`           — project label (default: `default`)
  - `--no-auto-register`  — skip startup registration; client must call
                            `agentbus_register` explicitly

## Limitations / TODO

  - No server→client notifications. Claude Code's MCP support handles
    `notifications/message` from servers, but most hosts only poll. We
    only expose `agentbus_inbox` for now.
  - `agentbus_register` is destructive — it Unregisters the current name
    on the poll connection then Registers the new name. Failure modes
    (e.g. new name taken on another connection) leave the server in an
    unregistered state.
  - No automatic reconnection if the daemon dies and restarts. The MCP
    server logs the disconnect and will return errors on subsequent
    tool calls.
  - No bracketed-paste or formatting on the bus side — that's handled
    by the recipient's adapter (PTY runner injects it as paste; MCP
    recipients just see the JSON via `agentbus_inbox`).
