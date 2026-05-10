//! agentbus-mcp — Model Context Protocol server bridging Claude Code,
//! Codex, opencode (any MCP-aware host) to the agentbus daemon.
//!
//! Why this exists alongside `agentbus run`:
//!
//!   - `agentbus run` wraps an agent's PTY and injects bus messages as
//!     pasted text. Universal — works for any TUI — but requires running
//!     the agent through a wrapper, which can cost the existing session
//!     when started inside a fresh shell.
//!
//!   - `agentbus-mcp` runs as an MCP child process the host launches at
//!     session start. The host calls our tools (agentbus_send,
//!     agentbus_inbox, agentbus_list) directly via JSON-RPC. No PTY
//!     wrapping, no session restart, no pasting hacks. Cleaner integration
//!     for any host that speaks MCP.
//!
//! Trade-off: only MCP-aware hosts get the cleaner UX. PTY runner stays as
//! the universal fallback for non-MCP agents (aider, opencode if it
//! doesn't add MCP, future CLIs).
//!
//! Protocol: JSON-RPC 2.0 over stdio (newline-delimited). The MCP spec
//! handshake is `initialize` -> `initialized` notification -> `tools/list`
//! and `tools/call` from then on. We hand-roll it (no rmcp dependency) so
//! the surface stays small and easy to read.

use std::io::Write as _;
use std::sync::Arc;

use agentbus_core::{socket_path, BusRequest, BusResponse, Message};
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "agentbus-mcp")]
#[command(
    about = "MCP server bridge for agentbus — exposes the bus as JSON-RPC tools to MCP-aware agents."
)]
struct Cli {
    /// Agent name to register on the bus. Defaults to `mcp-<pid>` if not
    /// provided. Set this to a stable value (e.g. `claude-code-1`) so
    /// other agents can address you.
    #[arg(long)]
    name: Option<String>,

    /// Program label (e.g. claude, codex). Informational metadata.
    #[arg(long, default_value = "mcp")]
    program: String,

    /// Model label (informational metadata).
    #[arg(long, default_value = "unknown")]
    model: String,

    /// Project label.
    #[arg(long, default_value = "default")]
    project: String,

    /// Don't auto-register on startup; client must call agentbus_register
    /// explicitly. Useful when the host (Claude Code) wants to choose the
    /// name dynamically.
    #[arg(long)]
    no_auto_register: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr — stdout is reserved for the JSON-RPC stream the
    // host is parsing.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let agent_name = cli
        .name
        .clone()
        .unwrap_or_else(|| format!("mcp-{}", std::process::id()));

    info!(
        "agentbus-mcp starting: name={} program={} model={}",
        agent_name, cli.program, cli.model
    );

    // ---- Connect to bus daemon -------------------------------------------
    //
    // We open TWO independent connections to agentbusd:
    //   - `rpc_*`  — used by tool dispatches (Send, List, Register, etc.).
    //                Short-lived request/response, never blocks.
    //   - `poll_*` — used by the background inbox poller, which sits in a
    //                long Read{wait:true} call. Sharing this connection
    //                with RPC would deadlock: while the poll is blocked,
    //                the daemon can't process a Send on the same socket.
    let sock = socket_path()?;

    let rpc_stream = UnixStream::connect(&sock)
        .await
        .with_context(|| format!("connect (rpc) to agentbus daemon at {:?}", sock))?;
    let (rpc_read_half, rpc_write_half) = rpc_stream.into_split();
    let bus_read = Arc::new(Mutex::new(TokioBufReader::new(rpc_read_half)));
    let bus_write = Arc::new(Mutex::new(rpc_write_half));

    let poll_stream = UnixStream::connect(&sock)
        .await
        .with_context(|| format!("connect (poll) to agentbus daemon at {:?}", sock))?;
    let (poll_read_half, poll_write_half) = poll_stream.into_split();
    let poll_read = Arc::new(Mutex::new(TokioBufReader::new(poll_read_half)));
    let poll_write = Arc::new(Mutex::new(poll_write_half));

    // Auto-register on the POLL connection — the daemon's clients map
    // associates the push channel with the connection that registered, so
    // pushed messages flow into the connection we're reading on. We also
    // need to register on the RPC connection? No: registration is
    // associated with one push channel per agent (the Register handler
    // refuses duplicate names with "already connected"). So only the poll
    // connection registers.
    if !cli.no_auto_register {
        let req = BusRequest::Register {
            name: agent_name.clone(),
            program: cli.program.clone(),
            model: cli.model.clone(),
            project: cli.project.clone(),
        };
        send_bus_req(&poll_write, &req).await?;
        let resp = recv_bus_resp(&poll_read).await?;
        match resp {
            BusResponse::Ok { .. } => info!("auto-registered as '{}'", agent_name),
            BusResponse::Error { message } => {
                return Err(anyhow!("auto-register failed: {}", message));
            }
            BusResponse::Message { .. } => {
                return Err(anyhow!(
                    "unexpected message response during auto-register"
                ));
            }
        }
    }

    // ---- Inbox: poll the bus and buffer pushed messages -------------------
    //
    // The MCP host doesn't get push notifications from us in this first
    // version — it polls via the agentbus_inbox tool. We background-poll
    // the bus with Read{wait:true,timeout=300} and push results into a
    // Vec that agentbus_inbox drains.
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<Message>(1024);
    let bus_read_for_poll = Arc::clone(&poll_read);
    let bus_write_for_poll = Arc::clone(&poll_write);
    let agent_name_for_poll = agent_name.clone();
    let _poll_task = tokio::spawn(async move {
        loop {
            let req = BusRequest::Read {
                wait: Some(true),
                timeout_secs: Some(300),
            };
            if let Err(e) = send_bus_req(&bus_write_for_poll, &req).await {
                warn!("bus send (poll) failed: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
            match recv_bus_resp(&bus_read_for_poll).await {
                Ok(BusResponse::Message { message }) => {
                    if inbox_tx.send(message).await.is_err() {
                        break;
                    }
                }
                Ok(BusResponse::Ok { data }) => {
                    if let Some(arr) = data.as_array() {
                        for v in arr {
                            if let Ok(m) = serde_json::from_value::<Message>(v.clone()) {
                                if inbox_tx.send(m).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
                Ok(BusResponse::Error { message }) => {
                    if !message.contains("timeout") {
                        warn!("bus error (poll) for {}: {}", agent_name_for_poll, message);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
                Err(e) => {
                    warn!("bus recv (poll) failed: {}", e);
                    break;
                }
            }
        }
    });

    // The inbox buffer the agentbus_inbox tool drains.
    let inbox: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let inbox_for_drain = Arc::clone(&inbox);
    tokio::spawn(async move {
        while let Some(msg) = inbox_rx.recv().await {
            inbox_for_drain.lock().await.push(msg);
        }
    });

    // ---- JSON-RPC main loop ----------------------------------------------
    //
    // Read newline-delimited JSON-RPC requests from stdin, dispatch, write
    // responses to stdout. Notifications (no `id` field) get no response.
    let stdin = tokio::io::stdin();
    let mut reader = TokioBufReader::new(stdin);
    let mut line = String::new();
    let stdout = std::io::stdout();
    let stdout = Arc::new(std::sync::Mutex::new(stdout));

    let ctx = Arc::new(McpContext {
        agent_name: Mutex::new(agent_name),
        bus_read,
        bus_write,
        poll_read,
        poll_write,
        inbox,
    });

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            info!("stdin EOF; exiting");
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: serde_json::Result<JsonRpcRequest> = serde_json::from_str(trimmed);
        match parsed {
            Ok(req) => {
                let resp = handle_request(&ctx, req).await;
                if let Some(resp) = resp {
                    let mut buf = serde_json::to_vec(&resp)?;
                    buf.push(b'\n');
                    let mut s = stdout.lock().unwrap();
                    s.write_all(&buf)?;
                    s.flush()?;
                }
            }
            Err(e) => {
                error!("malformed JSON-RPC line: {} -- {:?}", e, trimmed);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// JSON-RPC types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    method: String,
    #[serde(default)]
    params: Value,
    /// Present for requests, absent for notifications.
    id: Option<Value>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum JsonRpcResponse {
    Result {
        jsonrpc: &'static str,
        id: Value,
        result: Value,
    },
    Error {
        jsonrpc: &'static str,
        id: Value,
        error: JsonRpcError,
    },
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

fn ok(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse::Result {
        jsonrpc: "2.0",
        id,
        result,
    }
}

fn err(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse::Error {
        jsonrpc: "2.0",
        id,
        error: JsonRpcError { code, message },
    }
}

// ---------------------------------------------------------------------------
// Server context + dispatch
// ---------------------------------------------------------------------------

struct McpContext {
    agent_name: Mutex<String>,
    /// RPC connection used by tool dispatches (Send, List). Never registered
    /// on the bus — it just makes ad-hoc requests with `from: Some(name)`.
    bus_read: Arc<Mutex<TokioBufReader<tokio::net::unix::OwnedReadHalf>>>,
    bus_write: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    /// Poll connection used by the inbox poller. Registered on the bus so
    /// pushed messages flow here. Tool re-register goes here too.
    poll_read: Arc<Mutex<TokioBufReader<tokio::net::unix::OwnedReadHalf>>>,
    poll_write: Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    inbox: Arc<Mutex<Vec<Message>>>,
}

async fn handle_request(ctx: &McpContext, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
    debug!("rpc: method={} id={:?}", req.method, req.id);
    let id = req.id.clone();

    // Notifications (no id) return None.
    let resp_id = match id.clone() {
        Some(v) => v,
        None => {
            // Process notifications (currently we don't act on any).
            return None;
        }
    };

    match req.method.as_str() {
        "initialize" => Some(ok(
            resp_id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "agentbus-mcp",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )),

        "tools/list" => Some(ok(resp_id, json!({"tools": tool_definitions()}))),

        "tools/call" => match dispatch_tool(ctx, &req.params).await {
            Ok(v) => Some(ok(resp_id, v)),
            Err(e) => Some(err(resp_id, -32000, e.to_string())),
        },

        // ping is part of MCP keepalive; respond empty.
        "ping" => Some(ok(resp_id, json!({}))),

        other => Some(err(
            resp_id,
            -32601,
            format!("method not found: {}", other),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tool definitions + dispatch
// ---------------------------------------------------------------------------

fn tool_definitions() -> Value {
    json!([
        {
            "name": "agentbus_send",
            "description": "Send a structured message on the agentbus to another agent. The message is delivered immediately if the recipient is connected, or queued in SQLite for delivery on next read.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "to": { "type": "string", "description": "Recipient agent name." },
                    "body": { "type": "string", "description": "Message body (will be sanitized; control characters stripped)." },
                    "msg_type": {
                        "type": "string",
                        "enum": ["request", "response", "done", "question", "error", "status"],
                        "description": "Semantic type of the message.",
                        "default": "request"
                    },
                    "thread_id": { "type": "string", "description": "Optional thread identifier for grouping messages." }
                },
                "required": ["to", "body"]
            }
        },
        {
            "name": "agentbus_inbox",
            "description": "Drain pushed-but-not-yet-returned messages addressed to this MCP agent. Returns an array of messages that have arrived since the last call.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "max": { "type": "integer", "default": 100, "description": "Max messages to return." }
                }
            }
        },
        {
            "name": "agentbus_list",
            "description": "List all agents currently registered on the bus.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "agentbus_register",
            "description": "Re-register or change this MCP server's agent name on the bus.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "program": { "type": "string", "default": "mcp" },
                    "model": { "type": "string", "default": "unknown" },
                    "project": { "type": "string", "default": "default" }
                },
                "required": ["name"]
            }
        },
        {
            "name": "agentbus_whoami",
            "description": "Returns the name this MCP server is currently registered as.",
            "inputSchema": { "type": "object", "properties": {} }
        }
    ])
}

async fn dispatch_tool(ctx: &McpContext, params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("tools/call missing 'name'"))?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));

    match name {
        "agentbus_send" => tool_send(ctx, args).await,
        "agentbus_inbox" => tool_inbox(ctx, args).await,
        "agentbus_list" => tool_list(ctx).await,
        "agentbus_register" => tool_register(ctx, args).await,
        "agentbus_whoami" => {
            let n = ctx.agent_name.lock().await.clone();
            Ok(content_text(&format!("Registered as: {}", n)))
        }
        other => Err(anyhow!("unknown tool: {}", other)),
    }
}

async fn tool_send(ctx: &McpContext, args: Value) -> Result<Value> {
    let to = args
        .get("to")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'to' is required"))?
        .to_string();
    let body = args
        .get("body")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'body' is required"))?
        .to_string();
    let msg_type = args
        .get("msg_type")
        .and_then(|v| v.as_str())
        .unwrap_or("request")
        .to_string();
    let thread_id = args
        .get("thread_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let from = ctx.agent_name.lock().await.clone();
    let req = BusRequest::Send {
        from: Some(from),
        to,
        thread_id,
        msg_type,
        body,
    };
    send_bus_req(&ctx.bus_write, &req).await?;
    let resp = recv_bus_resp(&ctx.bus_read).await?;
    match resp {
        BusResponse::Ok { data } => Ok(content_text(&format!(
            "sent: {}",
            data.get("id").and_then(|v| v.as_str()).unwrap_or("?")
        ))),
        BusResponse::Error { message } => Err(anyhow!("send failed: {}", message)),
        BusResponse::Message { .. } => Err(anyhow!("unexpected Message response on send")),
    }
}

async fn tool_inbox(ctx: &McpContext, args: Value) -> Result<Value> {
    let max = args
        .get("max")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;
    let mut inbox = ctx.inbox.lock().await;
    let take = inbox.len().min(max);
    let drained: Vec<Message> = inbox.drain(..take).collect();
    let body = if drained.is_empty() {
        json!({"messages": [], "count": 0})
    } else {
        json!({
            "messages": drained,
            "count": drained.len(),
        })
    };
    Ok(content_json(body))
}

async fn tool_list(ctx: &McpContext) -> Result<Value> {
    let req = BusRequest::List;
    send_bus_req(&ctx.bus_write, &req).await?;
    let resp = recv_bus_resp(&ctx.bus_read).await?;
    match resp {
        BusResponse::Ok { data } => Ok(content_json(data)),
        BusResponse::Error { message } => Err(anyhow!("list failed: {}", message)),
        BusResponse::Message { .. } => Err(anyhow!("unexpected Message response on list")),
    }
}

async fn tool_register(ctx: &McpContext, args: Value) -> Result<Value> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("'name' is required"))?
        .to_string();
    let program = args
        .get("program")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp")
        .to_string();
    let model = args
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let project = args
        .get("project")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    // Re-register on the POLL connection so the push channel reattaches to
    // the new name. We unregister the current name first (HIGH-1 fix means
    // only we — as the owning connection — can do this), then Register the
    // new name. Note: the daemon's "already connected" check is on the
    // clients map keyed by name; since we're using the same connection,
    // we Unregister first to free the slot.
    let current = ctx.agent_name.lock().await.clone();
    if !current.is_empty() && current != name {
        let unreg = BusRequest::Unregister {
            name: current.clone(),
        };
        send_bus_req(&ctx.poll_write, &unreg).await?;
        // Drain the response — ignore failures; the next Register will
        // surface any real problem.
        let _ = recv_bus_resp(&ctx.poll_read).await;
    }

    let req = BusRequest::Register {
        name: name.clone(),
        program,
        model,
        project,
    };
    send_bus_req(&ctx.poll_write, &req).await?;
    let resp = recv_bus_resp(&ctx.poll_read).await?;
    match resp {
        BusResponse::Ok { .. } => {
            *ctx.agent_name.lock().await = name.clone();
            Ok(content_text(&format!("registered as: {}", name)))
        }
        BusResponse::Error { message } => Err(anyhow!("register failed: {}", message)),
        BusResponse::Message { .. } => Err(anyhow!("unexpected Message response on register")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wrap a string into the MCP "content" envelope expected for tool results.
fn content_text(s: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": s }]
    })
}

/// Wrap arbitrary JSON into a single text content block (pretty-printed)
/// for tool results that carry structured data.
fn content_json(v: Value) -> Value {
    let pretty = serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string());
    json!({
        "content": [{ "type": "text", "text": pretty }]
    })
}

async fn send_bus_req(
    write: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    req: &BusRequest,
) -> Result<()> {
    let mut buf = serde_json::to_vec(req)?;
    buf.push(b'\n');
    let mut g = write.lock().await;
    g.write_all(&buf).await?;
    g.flush().await?;
    Ok(())
}

async fn recv_bus_resp(
    read: &Arc<Mutex<TokioBufReader<tokio::net::unix::OwnedReadHalf>>>,
) -> Result<BusResponse> {
    let mut line = String::new();
    let mut g = read.lock().await;
    let n = g.read_line(&mut line).await?;
    if n == 0 {
        return Err(anyhow!("agentbusd closed connection"));
    }
    let resp: BusResponse = serde_json::from_str(line.trim())
        .with_context(|| format!("parse bus response: {:?}", line))?;
    Ok(resp)
}
