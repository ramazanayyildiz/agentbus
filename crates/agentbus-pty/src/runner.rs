//! Core PTY runner.
//!
//! Orchestrates four streams of bytes:
//!
//!   Local stdin ──┐
//!                 ├─► PTY writer (serializer task) ──► PTY master
//!   Bus message ──┘
//!
//!   PTY master ──► Local stdout
//!
//! The serializer is essential: without it, a long bus message could
//! interleave with the user's keystrokes mid-write and corrupt the input
//! line. With the mpsc, every write is atomic from the inner agent's
//! perspective.

use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agentbus_core::{socket_path, BusRequest, BusResponse};
use anyhow::{anyhow, Context, Result};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::adapter;

/// What the user is asking for. Kept small so the CLI layer stays thin.
pub struct PtyRunnerConfig {
    pub agent_name: String,
    pub program: String,
    pub model: String,
    pub project: String,
    pub argv: Vec<String>,
    pub rows: u16,
    pub cols: u16,
    /// If set, every byte read from the PTY master is appended to this
    /// path verbatim. Useful for replay, debugging, and (eventually) the
    /// Phase 4 transcript_chunks table once we centralize storage.
    pub transcript_path: Option<std::path::PathBuf>,
}

/// Top-level entry point.
pub struct PtyRunner;

/// Bytes destined for the PTY master, with a tag so the serializer can log
/// what it's writing. Using an enum (not just `Vec<u8>`) lets us add per-
/// source policy later (e.g. rate limit bus injections).
enum PtyWrite {
    UserStdin(Vec<u8>),
    BusMessage(Vec<u8>),
}

impl PtyRunner {
    /// Run until the child exits or stdin closes. Restores terminal state on
    /// every exit path including panics (best-effort — termios is restored
    /// in the `_TermiosGuard` drop impl).
    pub async fn run(cfg: PtyRunnerConfig) -> Result<i32> {
        if cfg.argv.is_empty() {
            return Err(anyhow!("argv is empty — nothing to run"));
        }

        // Pick adapter from program name. The CLI sets cfg.program from
        // either an explicit --program flag or the basename of argv[0].
        let adapter_box = adapter::pick(&cfg.program);
        info!(
            "using '{}' adapter for program '{}'",
            adapter_box.name(),
            cfg.program
        );
        let adapter_arc: Arc<dyn adapter::Adapter> = Arc::from(adapter_box);

        // ---- 1. Connect to bus + register ------------------------------------
        let sock = socket_path()?;
        let bus = UnixStream::connect(&sock)
            .await
            .with_context(|| format!("connect to agentbus daemon at {:?}", sock))?;
        let (bus_read, bus_write) = bus.into_split();
        let bus_read = Arc::new(Mutex::new(BufReader::new(bus_read)));
        let bus_write = Arc::new(Mutex::new(bus_write));

        let register = BusRequest::Register {
            name: cfg.agent_name.clone(),
            program: cfg.program.clone(),
            model: cfg.model.clone(),
            project: cfg.project.clone(),
        };
        send_request(&bus_write, &register).await?;
        let resp = recv_response(&bus_read).await?;
        match &resp {
            BusResponse::Ok { .. } => info!("registered '{}' on bus", cfg.agent_name),
            BusResponse::Error { message } => {
                return Err(anyhow!("bus registration failed: {}", message));
            }
            BusResponse::Message { .. } => {
                return Err(anyhow!("unexpected message response during register"));
            }
        }

        // ---- 2. Spawn PTY child ---------------------------------------------
        // Prefer the local terminal's actual size if stdout is a TTY — that
        // way the wrapped agent's TUI renders at the right dimensions
        // instead of our 40x120 default. Falls back to cfg values when we
        // can't query (e.g. piped or background runs).
        let (rows, cols) = detect_local_size().unwrap_or((cfg.rows, cfg.cols));
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("openpty")?;
        info!("PTY initial size: {}x{} (rows x cols)", rows, cols);

        let mut cmd = CommandBuilder::new(&cfg.argv[0]);
        for a in &cfg.argv[1..] {
            cmd.arg(a);
        }
        cmd.cwd(std::env::current_dir()?);
        // Inherit a useful environment. CommandBuilder defaults are minimal.
        for (k, v) in std::env::vars() {
            cmd.env(&k, &v);
        }

        let child = pair.slave.spawn_command(cmd).context("spawn child")?;
        // Drop the slave handle now that the child has it; we only need master.
        drop(pair.slave);

        let pty_reader = pair.master.try_clone_reader().context("clone pty reader")?;
        let pty_writer = pair.master.take_writer().context("take pty writer")?;
        // Keep the master so we can call resize() on SIGWINCH. Wrapped in a
        // mutex because the resize task and any future master-using code
        // share it.
        let pty_master = Arc::new(Mutex::new(pair.master));

        // ---- 3. Put local stdin into raw mode -------------------------------
        let _termios_guard = TermiosGuard::install()?;

        // ---- 4. Channels ----------------------------------------------------
        let (write_tx, mut write_rx) = mpsc::channel::<PtyWrite>(256);

        // Shared "last PTY output time" — updated by Task C every time bytes
        // arrive from the wrapped agent, read by Task B before injecting a
        // bus message. We store millis-since-process-start in an AtomicI64
        // so the readers don't need a Mutex on the hot path.
        let process_start = Instant::now();
        let last_output_ms = Arc::new(AtomicI64::new(0));

        // ---- 5. Spawn worker tasks -----------------------------------------
        // (a) PTY writer serializer — only ever one writer to the PTY master
        let mut pty_writer_task = pty_writer;
        let writer_join = tokio::task::spawn_blocking(move || -> Result<()> {
            // We drive this with blocking_recv on a tokio mpsc.
            // spawn_blocking gives us a real OS thread which is appropriate
            // because the underlying PTY writer is a synchronous file handle.
            while let Some(item) = write_rx.blocking_recv() {
                let bytes = match &item {
                    PtyWrite::UserStdin(b) => b.as_slice(),
                    PtyWrite::BusMessage(b) => {
                        debug!("injecting {} bytes from bus into PTY", b.len());
                        b.as_slice()
                    }
                };
                if let Err(e) = pty_writer_task.write_all(bytes) {
                    warn!("pty write failed: {}", e);
                    break;
                }
                if let Err(e) = pty_writer_task.flush() {
                    warn!("pty flush failed: {}", e);
                    break;
                }
            }
            Ok(())
        });

        // (b) PTY reader -> local stdout passthrough.
        //
        // Side effects:
        //   - stamps `last_output_ms` on every read so the bus injection task
        //     can detect idle periods (Phase 3)
        //   - if a transcript path was configured, append every byte to it
        //     for replay / debugging (Phase 4 lite — file-backed, no DB)
        let last_output_for_reader = Arc::clone(&last_output_ms);
        let transcript_path = cfg.transcript_path.clone();
        let pty_to_stdout = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut reader = pty_reader;
            let mut buf = [0u8; 4096];
            let mut stdout = std::io::stdout();
            // Open the transcript file once; tolerate failure by leaving it
            // None and just not writing.
            let mut transcript = transcript_path.as_ref().and_then(|p| {
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)
                {
                    Ok(f) => Some(f),
                    Err(e) => {
                        warn!("transcript file open failed for {:?}: {}", p, e);
                        None
                    }
                }
            });
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let now_ms = process_start.elapsed().as_millis() as i64;
                        last_output_for_reader.store(now_ms, Ordering::Relaxed);
                        if stdout.write_all(&buf[..n]).is_err() {
                            break;
                        }
                        let _ = stdout.flush();
                        if let Some(f) = transcript.as_mut() {
                            let _ = f.write_all(&buf[..n]);
                            // No flush — the OS will batch writes; on exit
                            // the file is closed so anything in-flight lands.
                        }
                    }
                    Err(_) => break,
                }
            }
            Ok(())
        });

        // (c) Local stdin -> PTY writer channel
        let stdin_tx = write_tx.clone();
        let stdin_to_pty = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut stdin = std::io::stdin();
            let mut buf = [0u8; 4096];
            loop {
                match stdin.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if stdin_tx
                            .blocking_send(PtyWrite::UserStdin(buf[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            Ok(())
        });

        // (d) Bus reader -> PTY writer channel.
        //
        // Long-lived loop: send Read{wait:true, timeout=300s}, await response,
        // forward Message responses to the PTY. On daemon disconnect we exit
        // with an error.
        let bus_tx = write_tx.clone();
        let bus_read_clone = Arc::clone(&bus_read);
        let bus_write_clone = Arc::clone(&bus_write);
        let adapter_for_bus = Arc::clone(&adapter_arc);
        let last_output_for_bus = Arc::clone(&last_output_ms);
        let idle_threshold = adapter_arc.idle_ms_before_inject();
        let bus_to_pty = tokio::spawn(async move {
            loop {
                let req = BusRequest::Read {
                    wait: Some(true),
                    timeout_secs: Some(300),
                };
                if let Err(e) = send_request(&bus_write_clone, &req).await {
                    warn!("bus send failed: {}", e);
                    break;
                }
                match recv_response(&bus_read_clone).await {
                    Ok(BusResponse::Message { message }) => {
                        // Phase 3: idle gating. If the adapter wants idle
                        // detection, poll last_output_ms until the gap from
                        // the most recent PTY output exceeds the threshold.
                        // We cap the wait at 30s so a stuck agent can't
                        // permanently block bus messages — if no idle window
                        // appears, the message goes through anyway and the
                        // user sees it interleave with running output.
                        if idle_threshold > 0 {
                            let max_wait = Duration::from_secs(30);
                            let started = Instant::now();
                            loop {
                                let now_ms = process_start.elapsed().as_millis() as i64;
                                let last_ms =
                                    last_output_for_bus.load(Ordering::Relaxed);
                                let idle_for = now_ms.saturating_sub(last_ms) as u64;
                                if idle_for >= idle_threshold {
                                    break;
                                }
                                if started.elapsed() >= max_wait {
                                    debug!(
                                        "idle wait exceeded {:?}; injecting anyway",
                                        max_wait
                                    );
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }

                        let bytes = adapter_for_bus.format_message(&message);
                        if bus_tx
                            .send(PtyWrite::BusMessage(bytes))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(BusResponse::Ok { data }) => {
                        // wait=true returns Ok([]) only on daemon timeout
                        // expiry — keep polling.
                        if data.is_array()
                            && data.as_array().map(|a| a.is_empty()).unwrap_or(false)
                        {
                            continue;
                        }
                        // Empty/unexpected Ok — keep going.
                        debug!("unexpected Ok response in bus loop: {:?}", data);
                    }
                    Ok(BusResponse::Error { message }) => {
                        // "No messages (timeout)" is normal under wait — loop.
                        if message.contains("timeout") {
                            continue;
                        }
                        warn!("bus error: {}", message);
                        // Brief backoff to avoid tight error loops.
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        warn!("bus recv failed: {}", e);
                        break;
                    }
                }
            }
        });

        // (e) SIGWINCH propagation — when the user resizes their local
        // terminal we need to resize the PTY too, otherwise the inner
        // agent's TUI keeps rendering at the original size.
        let pty_master_for_winch = Arc::clone(&pty_master);
        let winch_task = tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut winch = match signal(SignalKind::window_change()) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("SIGWINCH handler install failed: {}", e);
                        return;
                    }
                };
                while winch.recv().await.is_some() {
                    if let Some((rows, cols)) = detect_local_size() {
                        let m = pty_master_for_winch.lock().await;
                        if let Err(e) = m.resize(PtySize {
                            rows,
                            cols,
                            pixel_width: 0,
                            pixel_height: 0,
                        }) {
                            warn!("PTY resize failed: {}", e);
                        } else {
                            debug!("resized PTY to {}x{}", rows, cols);
                        }
                    }
                }
            }
        });

        // ---- 6. Wait for child exit -----------------------------------------
        // portable-pty's Child is sync; poll it from a blocking task so we
        // notice the exit without busy-looping the runtime.
        let mut child_box = child;
        let exit_code = tokio::task::spawn_blocking(move || -> Result<i32> {
            loop {
                match child_box.try_wait()? {
                    Some(status) => {
                        let code = status.exit_code() as i32;
                        return Ok(code);
                    }
                    None => std::thread::sleep(Duration::from_millis(100)),
                }
            }
        })
        .await??;

        info!("child exited with code {}", exit_code);

        // ---- 7. Cleanup -----------------------------------------------------
        // Closing the write_tx ends the writer task.
        drop(write_tx);
        // Best-effort: drop other tasks, restore termios via guard.
        bus_to_pty.abort();
        stdin_to_pty.abort();
        pty_to_stdout.abort();
        winch_task.abort();
        let _ = writer_join.await;

        // Send Close to bus daemon. Best effort.
        let close = BusRequest::Close;
        let _ = send_request(&bus_write, &close).await;

        Ok(exit_code)
    }
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

async fn send_request(
    write: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    req: &BusRequest,
) -> Result<()> {
    let mut buf = serde_json::to_vec(req)?;
    buf.push(b'\n');
    let mut guard = write.lock().await;
    guard.write_all(&buf).await?;
    guard.flush().await?;
    Ok(())
}

async fn recv_response(
    read: &Arc<Mutex<BufReader<tokio::net::unix::OwnedReadHalf>>>,
) -> Result<BusResponse> {
    let mut line = String::new();
    let mut guard = read.lock().await;
    let n = guard.read_line(&mut line).await?;
    if n == 0 {
        return Err(anyhow!("daemon closed connection"));
    }
    let resp: BusResponse = serde_json::from_str(line.trim())
        .with_context(|| format!("parse bus response: {:?}", line))?;
    Ok(resp)
}

/// Restore termios on drop. Installed once at start of `run`.
///
/// If stdin is not a TTY (piped input, /dev/null, automated testing), we
/// skip raw-mode setup entirely — there's no terminal to put into raw mode
/// and no risk of canonical buffering since stdin will just deliver whatever
/// bytes the upstream produces. The PTY layer still works; only the user
/// keystroke -> PTY bridge degrades to "whatever stdin gives us, send it."
struct TermiosGuard {
    fd: i32,
    saved: Option<libc::termios>,
}

impl TermiosGuard {
    fn install() -> Result<Self> {
        let fd = std::io::stdin().as_raw_fd();
        let is_tty = unsafe { libc::isatty(fd) } == 1;
        if !is_tty {
            tracing::info!("stdin is not a TTY; skipping raw-mode setup");
            return Ok(Self { fd, saved: None });
        }

        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut t) } != 0 {
            return Err(anyhow!(
                "tcgetattr failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let saved = t;
        // Raw-ish mode: keep ISIG off so Ctrl-C goes to the inner agent,
        // not us. Disable echo and canonical buffering. We turn off ICRNL
        // so the inner agent sees a real \r when the user hits Enter.
        t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
        t.c_iflag &= !(libc::IXON | libc::ICRNL | libc::INPCK | libc::ISTRIP | libc::BRKINT);
        t.c_cc[libc::VMIN] = 1;
        t.c_cc[libc::VTIME] = 0;
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &t) } != 0 {
            return Err(anyhow!(
                "tcsetattr failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(Self {
            fd,
            saved: Some(saved),
        })
    }
}

impl Drop for TermiosGuard {
    fn drop(&mut self) {
        if let Some(saved) = self.saved {
            unsafe {
                libc::tcsetattr(self.fd, libc::TCSANOW, &saved);
            }
        }
    }
}

/// Query the local controlling terminal for its current (rows, cols).
/// Returns None if stdout isn't a TTY or the ioctl fails — caller falls
/// back to a configured default.
fn detect_local_size() -> Option<(u16, u16)> {
    // We probe stdout because in `agentbus run` stdin gets put into raw
    // mode and may be redirected; stdout is the natural reference for
    // "what terminal is the user looking at."
    let fd = std::io::stdout().as_raw_fd();
    if unsafe { libc::isatty(fd) } != 1 {
        return None;
    }
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    // SAFETY: TIOCGWINSZ is defined for ttys; we just verified isatty.
    let res = unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut ws) };
    if res != 0 {
        return None;
    }
    if ws.ws_row == 0 || ws.ws_col == 0 {
        return None;
    }
    Some((ws.ws_row, ws.ws_col))
}
