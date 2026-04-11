use agentbus_core::{socket_path, BusRequest, BusResponse};
use clap::{Parser, Subcommand};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "agentbus")]
#[command(about = "Agent message bus CLI", long_about = None)]
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
    }

    Ok(())
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
