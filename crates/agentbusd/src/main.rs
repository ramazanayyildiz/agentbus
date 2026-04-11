use agentbus_core::{agentbus_dir, pid_file_path, socket_path, BusRequest, BusResponse, Database};
use std::collections::hash_map::Entry;
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

    // Initialize database
    let db = Arc::new(Mutex::new(Database::init()?));
    info!("Database initialized");

    // Create .agentbus directory if needed
    let bus_dir = agentbus_dir()?;
    fs::create_dir_all(&bus_dir)?;

    // Write PID file
    let pid = std::process::id();
    let pid_path = pid_file_path()?;
    fs::write(&pid_path, pid.to_string())?;
    info!("Daemon PID {} written to {:?}", pid, pid_path);

    // Remove old socket if it exists
    let sock_path = socket_path()?;
    if sock_path.exists() {
        fs::remove_file(&sock_path)?;
    }

    // Listen on Unix socket
    let listener = UnixListener::bind(&sock_path)?;
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

/// Write a response to the client, framed with a trailing newline.
async fn write_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    resp: &BusResponse,
) -> anyhow::Result<()> {
    let mut buf = serde_json::to_vec(resp)?;
    buf.push(b'\n');
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Outcome of one iteration of the `handle_client` loop. The inner function
/// uses a custom enum so a `select!` branch can tell us to exit cleanly vs.
/// keep looping without abusing `?` / sentinel errors.
enum LoopOutcome {
    Continue,
    Exit,
}

/// Drive a single iteration of the client loop: either read and dispatch one
/// request from the socket, or deliver one pushed message. Extracted from
/// `handle_client` to drop its complexity score from ~14 to ~8 and to make
/// the logic reachable from unit tests with a custom writer.
#[allow(clippy::too_many_arguments)]
async fn handle_one_iteration(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    db: &DbHandle,
    clients: &ClientMap,
    agent_name: &mut Option<String>,
    rx: &mut Option<mpsc::Receiver<agentbus_core::Message>>,
    line_buf: &mut String,
) -> anyhow::Result<LoopOutcome> {
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
                // Client disconnected — release any claims and the push
                // channel slot.
                if let Some(ref name) = agent_name {
                    {
                        let mut map = clients.lock().await;
                        map.remove(name);
                    }
                    let name_clone = name.clone();
                    let _ = db_call(db, move |d| d.release_all_claims_for(&name_clone)).await;
                    info!("Agent {} disconnected", name);
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
                    write_response(writer, &resp).await?;
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
                write_response(writer, &resp).await?;
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
            match write_response(writer, &resp).await {
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

/// Dispatch a parsed `BusRequest`. Returns `Ok(Some(resp))` if a normal
/// response should be written back, or `Ok(None)` if the handler already
/// wrote its own response (e.g. the Read/wait branch).
#[allow(clippy::too_many_arguments)]
async fn dispatch_request(
    request: BusRequest,
    db: &DbHandle,
    clients: &ClientMap,
    agent_name: &mut Option<String>,
    rx: &mut Option<mpsc::Receiver<agentbus_core::Message>>,
    writer: &mut (impl AsyncWriteExt + Unpin),
) -> anyhow::Result<Option<BusResponse>> {
    let response = match request {
        BusRequest::Register {
            name,
            program,
            model,
            project,
        } => {
            // Reject duplicate registrations so a second connection can't
            // steal the push channel from the first (Issue 4).
            {
                let map = clients.lock().await;
                if map.contains_key(&name) {
                    return Ok(Some(BusResponse::Error {
                        message: format!("agent '{}' already connected", name),
                    }));
                }
            }

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
                    // Insert push channel via `entry()` so we still refuse
                    // if a race sneaked another connection in between the
                    // check above and now.
                    let (tx, rx_new) = mpsc::channel(PUSH_CHANNEL_CAPACITY);
                    let mut map = clients.lock().await;
                    match map.entry(name.clone()) {
                        Entry::Occupied(_) => {
                            return Ok(Some(BusResponse::Error {
                                message: format!("agent '{}' already connected", name),
                            }));
                        }
                        Entry::Vacant(slot) => {
                            slot.insert(tx);
                        }
                    }
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
                        match write_response(writer, &resp).await {
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
                            match write_response(writer, &resp).await {
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
