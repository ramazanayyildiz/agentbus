use agentbus_core::{socket_path, BusRequest, BusResponse};
use clap::{Parser, Subcommand};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "agentbus")]
#[command(version, about = "Agent message bus CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the daemon
    Start,

    /// Register this agent
    Register {
        /// Agent name
        #[arg(long)]
        name: String,

        /// Program name
        #[arg(long)]
        program: String,

        /// Model name (optional)
        #[arg(long, default_value = "unknown")]
        model: String,

        /// Project name (optional)
        #[arg(long, default_value = "default")]
        project: String,
    },

    /// List all agents
    List,

    /// Send a message
    Send {
        /// Sending agent name
        #[arg(long)]
        from: String,

        /// Target agent name
        #[arg(long)]
        to: String,

        /// Message type (request, response, done, question, error, status)
        #[arg(long, default_value = "request")]
        msg_type: String,

        /// Thread ID (optional)
        #[arg(long)]
        thread_id: Option<String>,

        /// Message body
        body: String,
    },

    /// Read messages
    Read {
        /// Agent name (will register if not already)
        #[arg(long)]
        name: Option<String>,

        /// Wait for a message (blocking)
        #[arg(long)]
        wait: bool,

        /// Timeout in seconds
        #[arg(long)]
        timeout: Option<u64>,
    },

    /// Close/unregister
    Close {
        /// Agent name to close
        #[arg(long)]
        name: String,
    },

    /// Show daemon status
    Status,

    /// Prune dead agents (and their messages) from the bus database.
    /// By default removes agents in `disconnected` and `unregistered`
    /// state older than 24 hours. Use --dry-run to preview.
    Prune {
        /// Comma-separated states to prune. Default: disconnected,unregistered
        #[arg(long, default_value = "disconnected,unregistered")]
        state: String,

        /// Delete agents whose registered_at is older than this duration.
        /// Accepts: 30s, 5m, 2h, 7d. Default: 24h.
        #[arg(long, default_value = "24h")]
        older_than: String,

        /// Show what would be deleted without actually deleting.
        #[arg(long)]
        dry_run: bool,

        /// Confirm without prompting (skip interactive y/n).
        #[arg(long, short)]
        yes: bool,
    },

    /// Wrap a command in a PTY, register it on the bus, and bridge bus
    /// messages into the wrapped process. Use `--` to separate flags from
    /// the target command, e.g.:
    ///   agentbus run -- claude --dangerously-skip-permissions
    ///   agentbus run --name codex -- codex resume <id> --yolo
    ///
    /// If --name is omitted, it's auto-derived from the basename of the
    /// wrapped command (e.g. `agentbus run -- claude ...` registers as
    /// "claude"). If that name is already connected on the bus, a numeric
    /// suffix is appended ("claude-2", "claude-3", ...).
    Run {
        /// Agent name to register on the bus. Auto-derived from argv[0]
        /// basename when omitted.
        #[arg(long)]
        name: Option<String>,

        /// Program type (used for adapter selection in Phase 2; informational
        /// in Phase 1). Defaults to argv[0] of the wrapped command.
        #[arg(long)]
        program: Option<String>,

        /// Model name (informational metadata)
        #[arg(long, default_value = "unknown")]
        model: String,

        /// Project name (informational metadata)
        #[arg(long, default_value = "default")]
        project: String,

        /// PTY rows
        #[arg(long, default_value = "40")]
        rows: u16,

        /// PTY columns
        #[arg(long, default_value = "120")]
        cols: u16,

        /// If set, every byte produced by the wrapped agent is also appended
        /// to this file. Useful for replay or post-hoc inspection.
        #[arg(long)]
        transcript: Option<std::path::PathBuf>,

        /// Restart the wrapped command if it exits non-zero. The bus
        /// registration is preserved across restarts; messages queued
        /// during the gap are picked up on the next read.
        #[arg(long)]
        restart: bool,

        /// Maximum number of restarts before giving up. Default: 5.
        #[arg(long, default_value = "5")]
        max_restarts: u32,

        /// Command and args to execute inside the PTY. Everything after `--`.
        #[arg(last = true, required = true)]
        argv: Vec<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start => cmd_start()?,
        Commands::Register {
            name,
            program,
            model,
            project,
        } => cmd_register(&name, &program, &model, &project)?,
        Commands::List => cmd_list()?,
        Commands::Send {
            from,
            to,
            msg_type,
            thread_id,
            body,
        } => cmd_send(&from, &to, &msg_type, thread_id.as_deref(), &body)?,
        Commands::Read { name, wait, timeout } => cmd_read(name.as_deref(), wait, timeout)?,
        Commands::Close { name } => cmd_close(&name)?,
        Commands::Status => cmd_status()?,
        Commands::Prune {
            state,
            older_than,
            dry_run,
            yes,
        } => cmd_prune(&state, &older_than, dry_run, yes)?,
        Commands::Run {
            name,
            program,
            model,
            project,
            rows,
            cols,
            transcript,
            restart,
            max_restarts,
            argv,
        } => cmd_run(
            name,
            program,
            model,
            project,
            rows,
            cols,
            transcript,
            restart,
            max_restarts,
            argv,
        )?,
    }

    Ok(())
}

/// Run a target command wrapped in a PTY, bridged to the bus.
///
/// We need a tokio runtime here (the rest of the CLI is sync), so we build
/// a multi-thread runtime locally rather than #[tokio::main]ing the whole
/// binary. Keeps the other subcommands' startup latency unchanged.
fn cmd_run(
    name: Option<String>,
    program: Option<String>,
    model: String,
    project: String,
    rows: u16,
    cols: u16,
    transcript: Option<std::path::PathBuf>,
    restart: bool,
    max_restarts: u32,
    argv: Vec<String>,
) -> anyhow::Result<()> {
    // Tracing for the runner. Quiet by default; logs go to stderr so they
    // don't fight with the inner agent's output on stdout.
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    if argv.is_empty() {
        return Err(anyhow::anyhow!("`run` requires a command after --"));
    }
    let basename = std::path::Path::new(&argv[0])
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    let program = program.unwrap_or_else(|| basename.clone());

    // Auto-name when --name was omitted: use the basename, but if that's
    // already a live (connected) agent on the bus, append a numeric suffix.
    // Preserves the "no two agents with the same name connected at once"
    // invariant the daemon enforces, without making the user think up
    // distinct names every time they wrap a session.
    let name = match name {
        Some(n) => n,
        None => pick_unique_name(&basename)?,
    };
    eprintln!("agentbus: registering as '{}'", name);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    // Restart loop. With --restart we keep the bus registration alive across
    // child restarts: each iteration builds a fresh PtyRunnerConfig (cheap),
    // calls into the runner, and decides whether to retry based on exit
    // code + restart budget. Successful exits (code 0) always return — we
    // only retry on failure.
    let mut attempts: u32 = 0;
    loop {
        let cfg = agentbus_pty::runner::PtyRunnerConfig {
            agent_name: name.clone(),
            program: program.clone(),
            model: model.clone(),
            project: project.clone(),
            argv: argv.clone(),
            rows,
            cols,
            transcript_path: transcript.clone(),
        };
        let code = rt.block_on(agentbus_pty::PtyRunner::run(cfg))?;

        if code == 0 || !restart {
            std::process::exit(code);
        }

        attempts += 1;
        if attempts > max_restarts {
            eprintln!(
                "agentbus run: exceeded max-restarts ({}), giving up. Last exit code: {}",
                max_restarts, code
            );
            std::process::exit(code);
        }

        // Backoff: doubles each retry, capped at 30 seconds.
        let backoff = std::cmp::min(30, 1u64 << attempts.min(5));
        eprintln!(
            "agentbus run: child exited with code {}; restarting in {}s (attempt {}/{})",
            code, backoff, attempts, max_restarts
        );
        std::thread::sleep(std::time::Duration::from_secs(backoff));
    }
}

fn cmd_start() -> anyhow::Result<()> {
    let bus_dir = agentbus_core::agentbus_dir()?;
    fs::create_dir_all(&bus_dir)?;

    // Check if daemon is already running
    let pid_path = agentbus_core::pid_file_path()?;
    if pid_path.exists() {
        let pid_str = fs::read_to_string(&pid_path)?;
        if let Ok(pid) = pid_str.trim().parse::<u32>() {
            // Check if process exists
            #[cfg(unix)]
            {
                if nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    None,
                )
                .is_ok()
                {
                    println!("Daemon already running (PID: {})", pid);
                    return Ok(());
                }
            }
        }
    }

    // Start daemon in background
    let exe = std::env::current_exe()?;
    let daemon_path = exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Cannot find daemon"))?
        .join("agentbusd");

    if !daemon_path.exists() {
        return Err(anyhow::anyhow!(
            "Daemon binary not found at {:?}",
            daemon_path
        ));
    }

    let child = std::process::Command::new(&daemon_path)
        .spawn();

    match child {
        Ok(_) => {
            println!("Daemon started");
            // Give it a moment to initialize
            std::thread::sleep(Duration::from_millis(100));
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("Failed to start daemon: {}", e)),
    }
}

fn connect() -> anyhow::Result<UnixStream> {
    let sock_path = socket_path()?;
    let stream = UnixStream::connect(&sock_path)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    Ok(stream)
}

/// Read one newline-delimited JSON response from the daemon. Streams are
/// not message-aligned, so a fixed-buffer `read` can split a large response
/// or return partial bytes — `read_line` handles the framing correctly
/// (Issue 5).
fn read_response(stream: &UnixStream) -> anyhow::Result<BusResponse> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(anyhow::anyhow!("daemon closed connection"));
    }
    let response: BusResponse = serde_json::from_str(line.trim())?;
    Ok(response)
}

fn send_request(req: &BusRequest) -> anyhow::Result<BusResponse> {
    let mut stream = connect()?;

    // Send request
    let req_str = serde_json::to_string(req)?;
    stream.write_all(format!("{}\n", req_str).as_bytes())?;

    read_response(&stream)
}

fn cmd_register(name: &str, program: &str, model: &str, project: &str) -> anyhow::Result<()> {
    let req = BusRequest::Register {
        name: name.to_string(),
        program: program.to_string(),
        model: model.to_string(),
        project: project.to_string(),
    };

    let resp = send_request(&req)?;
    match resp {
        BusResponse::Ok { data } => {
            println!("Registered: {}", serde_json::to_string_pretty(&data)?);
            Ok(())
        }
        BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
        _ => Err(anyhow::anyhow!("Unexpected response")),
    }
}

fn cmd_list() -> anyhow::Result<()> {
    let req = BusRequest::List;
    let resp = send_request(&req)?;

    match resp {
        BusResponse::Ok { data } => {
            println!("{}", serde_json::to_string_pretty(&data)?);
            Ok(())
        }
        BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
        _ => Err(anyhow::anyhow!("Unexpected response")),
    }
}

fn cmd_send(
    from: &str,
    to: &str,
    msg_type: &str,
    thread_id: Option<&str>,
    body: &str,
) -> anyhow::Result<()> {
    let mut stream = connect()?;

    // Send the message (from field is required)
    let req = BusRequest::Send {
        from: Some(from.to_string()),
        to: to.to_string(),
        thread_id: thread_id.map(|s| s.to_string()),
        msg_type: msg_type.to_string(),
        body: body.to_string(),
    };

    let req_str = serde_json::to_string(&req)?;
    stream.write_all(format!("{}\n", req_str).as_bytes())?;

    let response = read_response(&stream)?;
    match response {
        BusResponse::Ok { data } => {
            println!("Sent: {}", serde_json::to_string_pretty(&data)?);
            Ok(())
        }
        BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
        BusResponse::Message { .. } => Err(anyhow::anyhow!("Unexpected message response")),
    }
}

fn cmd_read(agent_name: Option<&str>, wait: bool, timeout: Option<u64>) -> anyhow::Result<()> {
    let mut stream = connect()?;
    // Persistent reader so framing state survives across both the Register
    // response and the Read response (Issue 5).
    let mut reader = BufReader::new(stream.try_clone()?);

    // Register the agent if name provided
    if let Some(name) = agent_name {
        let reg_req = BusRequest::Register {
            name: name.to_string(),
            program: "cli".to_string(),
            model: "user".to_string(),
            project: "default".to_string(),
        };
        let reg_str = serde_json::to_string(&reg_req)?;
        stream.write_all(format!("{}\n", reg_str).as_bytes())?;

        // Drain registration response (one line).
        let mut line = String::new();
        reader.read_line(&mut line)?;
    }

    let req = BusRequest::Read {
        wait: Some(wait),
        timeout_secs: timeout,
    };

    // Set read timeout (add a little slack so the daemon has time to reply
    // before the socket itself times out).
    if let Some(secs) = timeout {
        stream.set_read_timeout(Some(Duration::from_secs(secs + 5)))?;
    }

    // Send request
    let req_str = serde_json::to_string(&req)?;
    stream.write_all(format!("{}\n", req_str).as_bytes())?;

    // Read response (newline-framed).
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => {
            println!("No messages");
            Ok(())
        }
        Ok(_) => {
            let response: BusResponse = serde_json::from_str(line.trim())?;
            match response {
                BusResponse::Ok { data } => {
                    println!("{}", serde_json::to_string_pretty(&data)?);
                    Ok(())
                }
                BusResponse::Message { message } => {
                    println!("Message from {}: {}", message.from, message.body);
                    Ok(())
                }
                BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
            }
        }
        Err(e) => Err(anyhow::anyhow!("Read error: {}", e)),
    }
}

fn cmd_close(name: &str) -> anyhow::Result<()> {
    let req = BusRequest::Unregister {
        name: name.to_string(),
    };

    let resp = send_request(&req)?;
    match resp {
        BusResponse::Ok { data } => {
            println!("Closed: {}", serde_json::to_string_pretty(&data)?);
            Ok(())
        }
        BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
        _ => Err(anyhow::anyhow!("Unexpected response")),
    }
}

/// Pick a name that's not currently live (state='active') on the bus.
/// Tries the basename first, then basename-2, basename-3, ... up to 100.
/// Falls back to basename-<pid> if we hit the cap or the daemon is
/// unreachable — better to register with a probably-unique name than to
/// fail outright.
fn pick_unique_name(basename: &str) -> anyhow::Result<String> {
    // Get the current agent list. If the daemon isn't running we can't
    // probe — return the basename and let the runner's startup phase
    // surface the real error.
    let req = BusRequest::List;
    let resp = match send_request(&req) {
        Ok(r) => r,
        Err(_) => return Ok(basename.to_string()),
    };
    let agents: Vec<serde_json::Value> = match resp {
        BusResponse::Ok { data } => serde_json::from_value(data).unwrap_or_default(),
        _ => return Ok(basename.to_string()),
    };

    // Build a set of "currently active" names — only those count as
    // collisions because the daemon allows reusing names that are
    // disconnected/unregistered (re-register flips state back to Active).
    let live: std::collections::HashSet<String> = agents
        .iter()
        .filter(|v| v.get("state").and_then(|s| s.as_str()) == Some("active"))
        .filter_map(|v| {
            v.get("name")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    if !live.contains(basename) {
        return Ok(basename.to_string());
    }
    for n in 2..=100 {
        let candidate = format!("{}-{}", basename, n);
        if !live.contains(&candidate) {
            return Ok(candidate);
        }
    }
    // Cap reached — fall back to PID suffix. Extremely unlikely, but
    // better than hanging or erroring.
    Ok(format!("{}-{}", basename, std::process::id()))
}

/// Parse a short human duration like "30s", "5m", "2h", "7d" into seconds.
/// Bare digits are treated as seconds. Used by `agentbus prune --older-than`.
fn parse_duration(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow::anyhow!("empty duration"));
    }
    let (num_str, unit_secs): (&str, u64) = if let Some(stripped) = s.strip_suffix('d') {
        (stripped, 86_400)
    } else if let Some(stripped) = s.strip_suffix('h') {
        (stripped, 3_600)
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, 60)
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, 1)
    } else {
        (s, 1)
    };
    let n: u64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("bad duration `{}`: expected forms 30s/5m/2h/7d", s))?;
    Ok(n.saturating_mul(unit_secs))
}

/// Prune dead agents from the DB by talking to the daemon's underlying
/// SQLite directly. We DON'T do this through the daemon protocol because
/// (a) there's no `Prune` request, and (b) it's an admin operation, not
/// a bus operation. The daemon's reads are unaffected; the WAL mode keeps
/// concurrent access safe.
fn cmd_prune(state: &str, older_than: &str, dry_run: bool, yes: bool) -> anyhow::Result<()> {
    use agentbus_core::AgentState;
    let states: Vec<AgentState> = state
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(AgentState::parse)
        .collect::<Result<_, _>>()?;
    if states.is_empty() {
        return Err(anyhow::anyhow!(
            "--state must list at least one state (e.g. disconnected,unregistered)"
        ));
    }
    let secs = parse_duration(older_than)?;
    let cutoff = chrono::Utc::now() - chrono::Duration::seconds(secs as i64);
    let cutoff_str = cutoff.to_rfc3339();

    // Open DB directly. WAL means the daemon's reads stay consistent.
    let mut db = agentbus_core::Database::init()?;

    if dry_run {
        // Dry-run: list what would be pruned without touching anything.
        // Done via a fresh connection so we don't accidentally start a
        // transaction here.
        let agents = db.list_agents()?;
        let candidates: Vec<_> = agents
            .iter()
            .filter(|a| {
                states.iter().any(|s| s == &a.state) && a.registered_at.as_str() < cutoff_str.as_str()
            })
            .collect();
        if candidates.is_empty() {
            println!("No agents match the prune filter.");
            return Ok(());
        }
        println!("Would delete {} agents (and their messages):", candidates.len());
        for a in candidates {
            println!(
                "  {:14}  state={:13}  registered_at={}",
                a.name,
                a.state.as_str(),
                a.registered_at
            );
        }
        println!("\nRe-run without --dry-run to apply.");
        return Ok(());
    }

    // Confirmation prompt unless --yes
    if !yes {
        eprint!(
            "Prune agents in state [{}] older than {} (cutoff {}). Continue? [y/N] ",
            state, older_than, cutoff_str
        );
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim().to_lowercase().as_str(), "y" | "yes") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let (agents, messages) = db.prune_inactive_agents(&states, &cutoff_str)?;
    println!(
        "Pruned: {} agent(s), {} message(s) deleted.",
        agents, messages
    );
    Ok(())
}

fn cmd_status() -> anyhow::Result<()> {
    let req = BusRequest::Status;
    let resp = send_request(&req)?;

    match resp {
        BusResponse::Ok { data } => {
            println!("{}", serde_json::to_string_pretty(&data)?);
            Ok(())
        }
        BusResponse::Error { message } => Err(anyhow::anyhow!(message)),
        _ => Err(anyhow::anyhow!("Unexpected response")),
    }
}
