use agentbus_core::{
    ensure_agentbus_dir, pid_file_path, socket_path, BusRequest, BusResponse, Database,
};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};

/// Bounded push channel depth. Chosen high enough that a slow client can
/// backlog transient bursts but low enough that a wedged client can't pin
/// unlimited memory (Issue 6).
const PUSH_CHANNEL_CAPACITY: usize = 1000;

type ClientMap = Arc<Mutex<HashMap<String, mpsc::Sender<agentbus_core::Message>>>>;
type DbHandle = Arc<Mutex<Database>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // MED-3 fix (external review): ensure ~/.agentbus exists with 0700 perms
    // before doing anything else. The daemon writes the DB and binds the
    // socket inside this dir, so locking down access at the dir level is
    // the simplest defense against other users on the machine.
    ensure_agentbus_dir()?;

    // MED-1 fix: don't blindly clobber a running daemon's socket. If a
    // PID file exists and that PID is alive, refuse to start. We only
    // remove the socket after we've established no other daemon owns it.
    let pid_path = pid_file_path()?;
    if pid_path.exists() {
        if let Ok(prev) = fs::read_to_string(&pid_path) {
            if let Ok(prev_pid) = prev.trim().parse::<u32>() {
                if pid_alive(prev_pid) {
                    eprintln!(
                        "agentbusd: another daemon is already running (PID {}). Refusing to start.",
                        prev_pid
                    );
                    std::process::exit(1);
                } else {
                    info!(
                        "stale PID file found for PID {} (process not alive); reclaiming",
                        prev_pid
                    );
                }
            }
        }
    }

    // Initialize database (after dir + lock so we don't create files in a
    // contested location).
    let db = Arc::new(Mutex::new(Database::init()?));
    info!("Database initialized");

    // Sweep stale "active" agents on startup. Anyone previously connected
    // is by definition disconnected now (their bus socket went away when
    // we restarted). Without this `agentbus status` shows zombies.
    {
        let guard = db.lock().await;
        match guard.mark_all_active_as_disconnected() {
            Ok(n) if n > 0 => info!("marked {} stale agents as disconnected on startup", n),
            Ok(_) => {}
            Err(e) => warn!("startup disconnected-sweep failed: {}", e),
        }
    }

    // Write our PID file (claims the bus_dir).
    let pid = std::process::id();
    fs::write(&pid_path, pid.to_string())?;
    info!("Daemon PID {} written to {:?}", pid, pid_path);

    // Remove old socket if it exists. Safe now because we've verified no
    // other live daemon owns this dir.
    let sock_path = socket_path()?;
    if sock_path.exists() {
        fs::remove_file(&sock_path)?;
    }

    // Listen on Unix socket
    let listener = UnixListener::bind(&sock_path)?;
    // Lock down the socket itself to 0600 (owner-only) to match the dir.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&sock_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&sock_path, perms)?;
    }
    info!("Listening on {:?}", sock_path);

    // Map of connected agents: agent_name → message channel
    let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));

    // Handle SIGTERM/SIGINT for graceful shutdown
    let sock_path_clone = sock_path.clone();
    let pid_path_clone = pid_path.clone();
    tokio::spawn(async move {
        let sigterm = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to install SIGTERM handler: {}", e);
                        return None;
                    }
                };
                sigterm.recv().await
            }
            #[cfg(not(unix))]
            {
                std::future::pending::<Option<()>>().await
            }
        };

        let sigint = async { tokio::signal::ctrl_c().await.ok() };

        tokio::select! {
            _ = sigterm => info!("Received SIGTERM"),
            _ = sigint => info!("Received SIGINT"),
        }

        // Cleanup
        if sock_path_clone.exists() {
            let _ = fs::remove_file(&sock_path_clone);
        }
        if pid_path_clone.exists() {
            let _ = fs::remove_file(&pid_path_clone);
        }
        info!("Cleanup done, exiting");
        std::process::exit(0);
    });

    // Accept connections
    loop {
        let (socket, _) = listener.accept().await?;
        let db = Arc::clone(&db);
        let clients = Arc::clone(&clients);

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, db, clients).await {
                error!("Client error: {}", e);
            }
        });
    }
}

/// Wrap a blocking DB call so it runs on the blocking thread pool and does
/// not stall other tasks on the async runtime (Issue 7).
async fn db_call<F, T>(db: &DbHandle, f: F) -> anyhow::Result<T>
where
    F: for<'a> FnOnce(&'a mut Database) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let db = Arc::clone(db);
    tokio::task::spawn_blocking(move || {
        // `blocking_lock` is the documented escape hatch for acquiring a
        // tokio Mutex from a blocking context. Safe here because we're on
        // a dedicated blocking thread.
        let mut guard = db.blocking_lock();
        f(&mut guard)
    })
    .await?
}

/// Abstraction over the socket write path so unit tests can inject a
/// writer that fails on demand. This is the seam for the F-011 / J-006
/// write-failure tests: a test implementation can be swapped in without
/// touching the rest of the daemon. `async fn in trait` (stable since
/// Rust 1.75) keeps us free of the `async-trait` crate.
trait MessageWriter: Send + Unpin {
    async fn write_response(&mut self, resp: &BusResponse) -> anyhow::Result<()>;
}

/// Real production impl: frame the response with a trailing newline and
/// write it to the underlying socket half.
impl MessageWriter for tokio::net::unix::OwnedWriteHalf {
    async fn write_response(&mut self, resp: &BusResponse) -> anyhow::Result<()> {
        let mut buf = serde_json::to_vec(resp)?;
        buf.push(b'\n');
        self.write_all(&buf).await?;
        self.flush().await?;
        Ok(())
    }
}

/// Outcome of one iteration of the `handle_client` loop. The inner function
/// uses a custom enum so a `select!` branch can tell us to exit cleanly vs.
/// keep looping without abusing `?` / sentinel errors.
#[derive(Debug)]
enum LoopOutcome {
    Continue,
    Exit,
}

/// Drive a single iteration of the client loop: either read and dispatch one
/// request from the socket, or deliver one pushed message. Extracted from
/// `handle_client` to drop its complexity score from ~14 to ~8 and to make
/// the logic reachable from unit tests with a custom writer.
///
/// Generic over the reader and writer so tests can inject `FailingWriter`
/// without wiring up a real Unix socket writer.
#[allow(clippy::too_many_arguments)]
async fn handle_one_iteration<R, W>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    db: &DbHandle,
    clients: &ClientMap,
    agent_name: &mut Option<String>,
    rx: &mut Option<mpsc::Receiver<agentbus_core::Message>>,
    line_buf: &mut String,
) -> anyhow::Result<LoopOutcome>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    W: MessageWriter,
{
    line_buf.clear();

    // Prepare a future for pushed messages. When we're not registered,
    // this future is `pending` forever so the select! still works.
    let pushed = async {
        match rx.as_mut() {
            Some(rx) => rx.recv().await,
            None => std::future::pending::<Option<agentbus_core::Message>>().await,
        }
    };

    tokio::select! {
        // === Branch 1: incoming request from client ===
        read_res = reader.read_line(line_buf) => {
            let n = read_res?;
            if n == 0 {
                // Client disconnected — release any claims, drop the push
                // channel slot, and mark the agent as Disconnected so
                // `agentbus status` reflects reality.
                //
                // Eviction race: a fresh connection with the same name may
                // have already taken our slot in `clients` (Register
                // handler reattach path drops our tx). If that happened,
                // our local rx has all-senders-dropped → `is_closed()`
                // returns true. In that case the slot in the map is no
                // longer ours; we must NOT remove it (would clobber the
                // fresh connection) and we must NOT mark_disconnected
                // (would lie about the agent — the fresh connection is
                // alive and healthy).
                let we_were_evicted =
                    rx.as_ref().map(|r| r.is_closed()).unwrap_or(false);
                if let Some(ref name) = agent_name {
                    if !we_were_evicted {
                        {
                            let mut map = clients.lock().await;
                            map.remove(name);
                        }
                        let name_release = name.clone();
                        let _ =
                            db_call(db, move |d| d.release_all_claims_for(&name_release)).await;
                        let name_disc = name.clone();
                        let _ = db_call(db, move |d| d.mark_disconnected(&name_disc)).await;
                        info!("Agent {} disconnected", name);
                    } else {
                        info!(
                            "Agent {} reachability/short connection ended (slot owned by another connection — leaving map untouched)",
                            name
                        );
                    }
                }
                return Ok(LoopOutcome::Exit);
            }

            let trimmed = line_buf.trim();
            if trimmed.is_empty() {
                return Ok(LoopOutcome::Continue);
            }

            // Parse request
            let request: BusRequest = match serde_json::from_str(trimmed) {
                Ok(req) => req,
                Err(e) => {
                    let resp = BusResponse::Error {
                        message: format!("Parse error: {}", e),
                    };
                    writer.write_response(&resp).await?;
                    return Ok(LoopOutcome::Continue);
                }
            };

            let response = dispatch_request(
                request,
                db,
                clients,
                agent_name,
                rx,
                writer,
            )
            .await?;

            if let Some(resp) = response {
                writer.write_response(&resp).await?;
            }
            Ok(LoopOutcome::Continue)
        }

        // === Branch 2: pushed message from another agent ===
        maybe_msg = pushed => {
            let Some(msg) = maybe_msg else {
                // Sender dropped — other end of the channel is gone.
                // Clear our receiver so the branch becomes pending again.
                *rx = None;
                return Ok(LoopOutcome::Continue);
            };

            // Deliver-then-mark-read (Issue 1). If the write fails, we
            // release the claim so the next connection redelivers.
            let resp = BusResponse::Message { message: msg.clone() };
            match writer.write_response(&resp).await {
                Ok(()) => {
                    let msg_id = msg.id.clone();
                    if let Err(e) = db_call(db, move |d| d.mark_read(&msg_id)).await {
                        warn!("mark_read failed for pushed msg: {}", e);
                    }
                    Ok(LoopOutcome::Continue)
                }
                Err(e) => {
                    warn!("Push write failed ({}); releasing claim {}", e, msg.id);
                    let msg_id = msg.id.clone();
                    let _ = db_call(db, move |d| d.release_claim(&msg_id)).await;
                    // Client socket is broken — propagate the error so
                    // the task ends and cleanup runs.
                    Err(e)
                }
            }
        }
    }
}

async fn handle_client(
    socket: UnixStream,
    db: DbHandle,
    clients: ClientMap,
) -> anyhow::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut agent_name: Option<String> = None;
    let mut rx: Option<mpsc::Receiver<agentbus_core::Message>> = None;

    // Run the per-iteration loop and capture any error; ALWAYS run the
    // cleanup block below so DB state and the clients map don't go stale
    // when an iteration fails (write to dead socket, parse error chain,
    // any future I/O fault). Without this the agent would stay 'active'
    // in `agentbus status` until something else evicts it — sometimes
    // never. Found the gap when wrapper SIGTERM left the agent stuck
    // 'active' for >35s post-Read{wait}-timeout.
    let result: anyhow::Result<()> = async {
        loop {
            match handle_one_iteration(
                &mut reader,
                &mut writer,
                &db,
                &clients,
                &mut agent_name,
                &mut rx,
                &mut line,
            )
            .await?
            {
                LoopOutcome::Continue => continue,
                LoopOutcome::Exit => return Ok(()),
            }
        }
    }
    .await;

    // Cleanup. Runs whether the loop returned Ok(Exit) (clean EOF —
    // handle_one_iteration already did its half) or Err (timeout-write-
    // fail or other I/O fault — the in-loop path never ran). The
    // eviction-aware check below is the same one used in the in-loop
    // EOF path; see the long-form comment there.
    let we_were_evicted = rx.as_ref().map(|r| r.is_closed()).unwrap_or(false);
    if let Some(name) = agent_name.as_ref() {
        if !we_were_evicted {
            {
                let mut map = clients.lock().await;
                map.remove(name);
            }
            let name_clone = name.clone();
            let _ = db_call(&db, move |d| d.release_all_claims_for(&name_clone)).await;
            let name_disc = name.clone();
            let _ = db_call(&db, move |d| d.mark_disconnected(&name_disc)).await;
            info!("Agent {} cleanup complete (handle_client exit)", name);
        } else {
            info!(
                "Agent {} handle_client exit while evicted — slot left intact",
                name
            );
        }
    }

    result
}

/// Dispatch a parsed `BusRequest`. Returns `Ok(Some(resp))` if a normal
/// response should be written back, or `Ok(None)` if the handler already
/// wrote its own response (e.g. the Read/wait branch).
#[allow(clippy::too_many_arguments)]
async fn dispatch_request<W: MessageWriter>(
    request: BusRequest,
    db: &DbHandle,
    clients: &ClientMap,
    agent_name: &mut Option<String>,
    rx: &mut Option<mpsc::Receiver<agentbus_core::Message>>,
    writer: &mut W,
) -> anyhow::Result<Option<BusResponse>> {
    let response = match request {
        BusRequest::Register {
            name,
            program,
            model,
            project,
        } => {
            // Reattach-on-Register: if the name is already in the clients
            // map, the previous connection is either dead (waiting for
            // the dispatch-side socket-EOF detection to fire, up to 30s
            // post-disconnect with the current Read{wait} timeout) or the
            // user is reattaching from a fresh wrapper after Ctrl+C.
            //
            // Either way, evict the old slot and let the new connection
            // take over. The old connection's push channel closes; its
            // next try_send fails; daemon cleans up that side too. mosh
            // / screen reattach style.
            //
            // This relaxes the original "Issue 4" rule (refuse duplicate
            // Register). The motivation back then was "don't let a
            // second client steal the push channel from a live first
            // client." In single-user local IPC this is a worse UX than
            // it's worth — the more common case is the user Ctrl+C-d
            // their wrapper and immediately relaunched with the same
            // name, hitting "already connected" because the daemon
            // hadn't noticed the old socket was dead yet.

            let name_c = name.clone();
            let program_c = program.clone();
            let model_c = model.clone();
            let project_c = project.clone();
            let agent_result = db_call(db, move |d| {
                d.register_agent(&name_c, &program_c, &model_c, &project_c)
            })
            .await;

            match agent_result {
                Ok(agent) => {
                    let (tx, rx_new) = mpsc::channel(PUSH_CHANNEL_CAPACITY);
                    let mut map = clients.lock().await;
                    if let Some(old_tx) = map.remove(&name) {
                        // Drop the old sender; the previous connection's
                        // receiver wakes up with None and the runner /
                        // bus_to_pty task treats it as connection lost.
                        drop(old_tx);
                        info!("evicted previous connection for '{}' (reattach)", name);
                    }
                    map.insert(name.clone(), tx);
                    drop(map);

                    *agent_name = Some(name.clone());
                    *rx = Some(rx_new);
                    info!("Agent {} claimed by connection", name);

                    BusResponse::Ok {
                        data: serde_json::to_value(&agent)?,
                    }
                }
                Err(e) => {
                    warn!("Registration failed: {}", e);
                    BusResponse::Error {
                        message: format!("Registration failed: {}", e),
                    }
                }
            }
        }

        BusRequest::Unregister { name } => {
            // HIGH-1 fix (external review): only the connection that owns the
            // agent's name (i.e. the one that successfully Registered it) can
            // Unregister it. Without this check any client can boot any other
            // agent off the bus by sending an Unregister request.
            //
            // Two failure modes are rejected here:
            //   - this connection isn't registered at all (agent_name == None)
            //   - this connection registered as someone else (mismatch)
            match agent_name.as_deref() {
                None => {
                    return Ok(Some(BusResponse::Error {
                        message: format!(
                            "not authorized to unregister '{}': this connection is not registered",
                            name
                        ),
                    }));
                }
                Some(owner) if owner != name => {
                    return Ok(Some(BusResponse::Error {
                        message: format!(
                            "not authorized to unregister '{}': this connection registered as '{}'",
                            name, owner
                        ),
                    }));
                }
                _ => {}
            }

            let name_c = name.clone();
            match db_call(db, move |d| d.unregister_agent(&name_c)).await {
                Ok(()) => {
                    let mut map = clients.lock().await;
                    map.remove(&name);
                    info!("Agent {} unregistered", name);
                    BusResponse::Ok {
                        data: serde_json::json!({"status": "unregistered"}),
                    }
                }
                Err(e) => BusResponse::Error {
                    message: format!("Unregister failed: {}", e),
                },
            }
        }

        BusRequest::List => match db_call(db, |d| d.list_agents()).await {
            Ok(agents) => BusResponse::Ok {
                data: serde_json::to_value(&agents)?,
            },
            Err(e) => BusResponse::Error {
                message: format!("List failed: {}", e),
            },
        },

        BusRequest::Send {
            from,
            to,
            thread_id,
            msg_type,
            body,
        } => {
            // Fail loudly on unknown types — no silent downgrade to Request
            // (Issue 9).
            let msg_type_enum = match agentbus_core::MessageType::parse(&msg_type) {
                Ok(t) => t,
                Err(e) => {
                    return Ok(Some(BusResponse::Error {
                        message: format!("Send failed: {}", e),
                    }));
                }
            };

            let sender = from
                .clone()
                .or_else(|| agent_name.clone())
                .unwrap_or_else(|| "unknown".to_string());
            let to_c = to.clone();
            let thread_c = thread_id.clone();
            let body_c = body.clone();

            match db_call(db, move |d| {
                d.send_message(&sender, &to_c, thread_c.as_deref(), msg_type_enum, &body_c)
            })
            .await
            {
                Ok(msg) => {
                    // Try to push immediately to the connected recipient.
                    // Atomically claim in the DB first so the recipient's
                    // next fetch_and_claim can't also pick this up. If the
                    // claim fails (another path already took it) or the
                    // push channel is full/closed, release the claim so
                    // the message becomes redeliverable (Issues 1–3).
                    let recipient_connected = {
                        let map = clients.lock().await;
                        map.contains_key(&to)
                    };

                    if recipient_connected {
                        let msg_id = msg.id.clone();
                        let claimed = db_call(db, move |d| d.claim_message(&msg_id)).await;
                        match claimed {
                            Ok(true) => {
                                let map = clients.lock().await;
                                let push_result = if let Some(tx) = map.get(&to) {
                                    tx.try_send(msg.clone()).map_err(|e| e.to_string())
                                } else {
                                    Err("recipient disconnected mid-push".to_string())
                                };
                                drop(map);

                                if let Err(e) = push_result {
                                    warn!(
                                        "Push to {} failed ({}); releasing claim {}",
                                        to, e, msg.id
                                    );
                                    let mid = msg.id.clone();
                                    let _ =
                                        db_call(db, move |d| d.release_claim(&mid)).await;
                                }
                            }
                            Ok(false) => { /* someone else claimed — fine */ }
                            Err(e) => warn!("claim failed: {}", e),
                        }
                    }

                    BusResponse::Ok {
                        data: serde_json::to_value(&msg)?,
                    }
                }
                Err(e) => BusResponse::Error {
                    message: format!("Send failed: {}", e),
                },
            }
        }

        BusRequest::Read { wait, timeout_secs } => {
            let Some(name) = agent_name.clone() else {
                return Ok(Some(BusResponse::Error {
                    message: "Not registered".to_string(),
                }));
            };

            // Phase A: claim any already-pending messages atomically.
            let name_c = name.clone();
            let claimed = db_call(db, move |d| d.fetch_and_claim_messages(&name_c)).await;

            match claimed {
                Ok(messages) => {
                    if !messages.is_empty() {
                        // Phase B: write → flush → mark_read (Issue 1).
                        let resp = BusResponse::Ok {
                            data: serde_json::to_value(&messages)?,
                        };
                        match writer.write_response(&resp).await {
                            Ok(()) => {
                                for msg in &messages {
                                    let mid = msg.id.clone();
                                    if let Err(e) =
                                        db_call(db, move |d| d.mark_read(&mid)).await
                                    {
                                        warn!("mark_read failed: {}", e);
                                    }
                                }
                                // Already wrote our response — tell caller
                                // not to write again.
                                return Ok(None);
                            }
                            Err(e) => {
                                warn!("Read write failed ({}); releasing claims", e);
                                for msg in &messages {
                                    let mid = msg.id.clone();
                                    let _ =
                                        db_call(db, move |d| d.release_claim(&mid)).await;
                                }
                                return Err(e);
                            }
                        }
                    }

                    // Empty queue — decide whether to wait.
                    if !wait.unwrap_or(false) {
                        return Ok(Some(BusResponse::Ok {
                            data: serde_json::json!([]),
                        }));
                    }

                    let Some(ref mut receiver) = rx else {
                        return Ok(Some(BusResponse::Error {
                            message: "Not registered".to_string(),
                        }));
                    };

                    let result = if let Some(d) = timeout_secs.map(std::time::Duration::from_secs)
                    {
                        match tokio::time::timeout(d, receiver.recv()).await {
                            Ok(Some(m)) => Some(m),
                            _ => None,
                        }
                    } else {
                        receiver.recv().await
                    };

                    match result {
                        Some(msg) => {
                            // Deliver-then-mark-read for the wait path too.
                            let resp = BusResponse::Message { message: msg.clone() };
                            match writer.write_response(&resp).await {
                                Ok(()) => {
                                    let mid = msg.id.clone();
                                    if let Err(e) =
                                        db_call(db, move |d| d.mark_read(&mid)).await
                                    {
                                        warn!("mark_read failed: {}", e);
                                    }
                                    return Ok(None);
                                }
                                Err(e) => {
                                    let mid = msg.id.clone();
                                    let _ =
                                        db_call(db, move |d| d.release_claim(&mid)).await;
                                    return Err(e);
                                }
                            }
                        }
                        None => BusResponse::Error {
                            message: "No messages (timeout)".to_string(),
                        },
                    }
                }
                Err(e) => BusResponse::Error {
                    message: format!("Read failed: {}", e),
                },
            }
        }

        BusRequest::Close => {
            if let Some(ref name) = agent_name {
                let name_c = name.clone();
                let _ = db_call(db, move |d| d.unregister_agent(&name_c)).await;
                let mut map = clients.lock().await;
                map.remove(name);
                info!("Agent {} closed connection", name);
            }
            BusResponse::Ok {
                data: serde_json::json!({"status": "closed"}),
            }
        }

        BusRequest::Status => match db_call(db, |d| d.list_agents()).await {
            Ok(agents) => BusResponse::Ok {
                data: serde_json::to_value(serde_json::json!({
                    "agent_count": agents.len(),
                    "agents": agents
                }))?,
            },
            Err(e) => BusResponse::Error {
                message: format!("Status failed: {}", e),
            },
        },
    };

    Ok(Some(response))
}

// ===========================================================================
// Unit tests — exercise the previously-untestable write-failure paths
// (F-011 claim released after Read-response write failure, J-006 pushed-branch
// write failure releases claim) by driving `handle_one_iteration` with a
// `FailingWriter` that simulates a socket error on the first response write.
//
// These tests live inside `main.rs` (not integration tests) because the
// `MessageWriter` trait and `handle_one_iteration` are crate-private to the
// binary, and integration tests compile as separate binaries that can't see
// private items of a binary crate. Putting them here is the path the Risk
// Profile Refactor 2 / 3 guidance explicitly calls out.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use agentbus_core::{Message, MessageType};
    use serial_test::serial;
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use tokio::net::UnixStream;

    /// `MessageWriter` implementation that fails on the first write and
    /// records any successful writes into an inner Vec for inspection.
    /// Using a plain `usize` counter (not AtomicUsize) because `write_response`
    /// takes `&mut self` — exclusive access makes atomics pointless.
    struct FailingWriter {
        remaining: StdMutex<usize>,
        writes: StdMutex<Vec<BusResponse>>,
    }

    impl FailingWriter {
        fn new(fail_after: usize) -> Self {
            Self {
                remaining: StdMutex::new(fail_after),
                writes: StdMutex::new(Vec::new()),
            }
        }

        fn take_writes(&self) -> Vec<BusResponse> {
            std::mem::take(&mut *self.writes.lock().unwrap())
        }
    }

    impl MessageWriter for FailingWriter {
        async fn write_response(&mut self, resp: &BusResponse) -> anyhow::Result<()> {
            let mut rem = self.remaining.lock().unwrap();
            if *rem == 0 {
                return Err(anyhow::anyhow!("simulated write failure"));
            }
            *rem -= 1;
            self.writes.lock().unwrap().push(resp.clone());
            Ok(())
        }
    }

    /// Create an isolated DB rooted at `<tmp>/.agentbus`, like the Tier 2
    /// database tests. Returns (db_handle, tmp). Caller must keep the tmp
    /// alive for the duration of the test.
    fn fresh_db_handle() -> (DbHandle, TempDir) {
        let tmp = tempfile::TempDir::new_in("/tmp").expect("tempdir");
        std::env::set_var("AGENTBUS_DIR", tmp.path().join(".agentbus"));
        let _ = std::fs::remove_dir_all(tmp.path().join(".agentbus"));
        let db = Database::init().expect("Database::init");
        (Arc::new(Mutex::new(db)), tmp)
    }

    /// Look up `claimed_at` for a given message id via a fresh raw connection.
    fn claimed_at_for(tmp: &TempDir, msg_id: &str) -> Option<String> {
        use rusqlite::Connection;
        let path = tmp.path().join(".agentbus").join("bus.db");
        let conn = Connection::open(path).expect("raw conn");
        conn.query_row(
            "SELECT claimed_at FROM messages WHERE id = ?",
            rusqlite::params![msg_id],
            |r| r.get::<_, Option<String>>(0),
        )
        .expect("query claimed_at")
    }

    // -------------------------------------------------------------------
    // J-006 — pushed-branch write failure releases claim
    //
    // Setup: bob is registered and has a push channel. A message for bob is
    // inserted into the DB and claimed (mirroring what the Send path does
    // right before it calls try_send on the push tx). We push the message
    // into the mpsc, then drive one iteration with a FailingWriter. The
    // pushed branch should win the select!, attempt to write, fail, release
    // the claim, and return Err.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn j006_push_branch_write_failure_releases_claim() {
        let (db, tmp) = fresh_db_handle();

        // Seed agents + message (borrow the mutex just for the setup).
        let msg = {
            let guard = db.lock().await;
            guard.register_agent("alice", "p", "m", "proj").unwrap();
            guard.register_agent("bob", "p", "m", "proj").unwrap();
            guard
                .send_message("alice", "bob", None, MessageType::Request, "hi")
                .unwrap()
        };

        // Simulate the daemon Send path: claim the message before pushing.
        {
            let guard = db.lock().await;
            assert!(guard.claim_message(&msg.id).unwrap());
        }
        assert!(claimed_at_for(&tmp, &msg.id).is_some(), "setup: should be claimed");

        // Build the iteration harness state. The reader half of a UnixStream
        // pair that nobody writes to yields a pending read_line — so the
        // pushed branch reliably wins the select!.
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel::<Message>(PUSH_CHANNEL_CAPACITY);
        let mut rx_opt: Option<mpsc::Receiver<Message>> = Some(rx);

        // Push the message so it's waiting for the next select! poll.
        tx.send(msg.clone()).await.expect("push");

        // Register bob in the map just to match production state (not
        // strictly required for the pushed branch to work, but keeps the
        // shape of the test realistic).
        {
            let mut map = clients.lock().await;
            map.insert("bob".to_string(), tx);
        }
        let mut agent_name: Option<String> = Some("bob".to_string());

        // Reader that pends forever.
        let (_peer, ours) = UnixStream::pair().expect("unix pair");
        let (read_half, _write_half) = ours.into_split();
        let mut reader = BufReader::new(read_half);

        let mut writer = FailingWriter::new(0); // first write fails
        let mut line = String::new();

        let result = handle_one_iteration(
            &mut reader,
            &mut writer,
            &db,
            &clients,
            &mut agent_name,
            &mut rx_opt,
            &mut line,
        )
        .await;

        assert!(
            result.is_err(),
            "expected Err from failed pushed write, got {result:?}"
        );
        // Give the db_call spawn_blocking a beat to commit the release.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            claimed_at_for(&tmp, &msg.id).is_none(),
            "claim should be released after pushed-branch write failure"
        );
        // FailingWriter never succeeded.
        assert!(writer.take_writes().is_empty());
    }

    // -------------------------------------------------------------------
    // F-011 — Read-path write failure releases all claims
    //
    // Setup: bob has three unread messages in the DB. A Read(wait=false)
    // request is queued in the socket-pair read half. We drive one iteration
    // with a FailingWriter. handle_one_iteration should:
    //   - parse the Read request,
    //   - dispatch it (Phase A: fetch_and_claim_messages claims all three),
    //   - attempt the response write (Phase B), which fails,
    //   - release the three claims,
    //   - propagate the Err.
    // -------------------------------------------------------------------
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn f011_claim_released_after_write_failure() {
        let (db, tmp) = fresh_db_handle();

        let msg_ids: Vec<String> = {
            let guard = db.lock().await;
            guard.register_agent("alice", "p", "m", "proj").unwrap();
            guard.register_agent("bob", "p", "m", "proj").unwrap();
            (0..3)
                .map(|i| {
                    guard
                        .send_message(
                            "alice",
                            "bob",
                            None,
                            MessageType::Request,
                            &format!("m{i}"),
                        )
                        .unwrap()
                        .id
                })
                .collect()
        };

        // Client map must contain bob so agent_name resolves the receiver
        // for the Read branch. We don't actually use the push rx here — the
        // Read request's non-wait path takes the direct DB fetch branch.
        let clients: ClientMap = Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel::<Message>(PUSH_CHANNEL_CAPACITY);
        let mut rx_opt: Option<mpsc::Receiver<Message>> = Some(rx);
        {
            let mut map = clients.lock().await;
            map.insert("bob".to_string(), tx);
        }
        let mut agent_name: Option<String> = Some("bob".to_string());

        // Queue a Read request in the socket pair. Use a real pair so that
        // the read_line future makes progress before the pushed branch
        // becomes runnable (the pushed branch has no sender, so it pends).
        let (peer, ours) = UnixStream::pair().expect("unix pair");
        let read_req = BusRequest::Read {
            wait: Some(false),
            timeout_secs: None,
        };
        let line = serde_json::to_string(&read_req).unwrap() + "\n";
        // Write from the peer side so our reader sees it.
        {
            let (_pr, mut pw) = peer.into_split();
            pw.write_all(line.as_bytes()).await.unwrap();
            pw.flush().await.unwrap();
        }

        let (read_half, _write_half) = ours.into_split();
        let mut reader = BufReader::new(read_half);

        let mut writer = FailingWriter::new(0); // first write fails
        let mut line_buf = String::new();

        let result = handle_one_iteration(
            &mut reader,
            &mut writer,
            &db,
            &clients,
            &mut agent_name,
            &mut rx_opt,
            &mut line_buf,
        )
        .await;

        assert!(
            result.is_err(),
            "expected Err from failed Read-path write, got {result:?}"
        );
        // Release claims run through db_call → spawn_blocking; let them
        // commit before we inspect the DB.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        for id in &msg_ids {
            assert!(
                claimed_at_for(&tmp, id).is_none(),
                "claim for {id} should have been released after write failure"
            );
        }
        assert!(writer.take_writes().is_empty());
    }
}

/// Best-effort liveness check for a PID. Used by daemon startup to decide
/// whether a stale PID file represents a real process or just a leftover
/// from a previous unclean shutdown. On Unix we send signal 0 — kill(2)
/// returns 0 if the process exists and we're allowed to signal it,
/// ESRCH if it doesn't exist.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    let res = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if res == 0 {
        return true;
    }
    // EPERM means the process exists but we're not allowed to signal it.
    // Still counts as "alive" — refuse to start, don't clobber its socket.
    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(0);
    errno == libc::EPERM
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    // Conservative: if we can't probe, assume it's alive — better to
    // refuse to start than to clobber another daemon's socket.
    true
}
