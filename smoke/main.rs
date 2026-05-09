// Phase 0 smoke test: does bracketed paste injection work?
//
// Spawns a target command in a PTY, waits for it to be ready, then injects
// a bracketed-paste payload followed by a carriage return. We then capture
// output for a few seconds and print it so we can see whether the target
// (a) treated it as paste, (b) treated it as keystrokes, (c) submitted it.
//
// Usage:
//   pty-smoke -- bash -i
//   pty-smoke -- claude --dangerously-skip-permissions
//   pty-smoke -- codex resume <id> --yolo
//
// What "works" means:
//   - The agent receives the text as a single block (not character-by-character)
//   - The trailing \r submits it (we see the agent process the prompt)
//   - No control chars from the body get interpreted

use anyhow::Result;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let sep = args.iter().position(|a| a == "--").unwrap_or(0);
    if sep == 0 || sep + 1 >= args.len() {
        eprintln!("usage: pty-smoke -- <command> [args...]");
        std::process::exit(2);
    }
    let cmd_args = &args[sep + 1..];
    eprintln!("[smoke] spawning: {:?}", cmd_args);

    // Build the command for the PTY
    let mut cmd = CommandBuilder::new(&cmd_args[0]);
    for a in &cmd_args[1..] {
        cmd.arg(a);
    }
    cmd.env("TERM", "xterm-256color");
    if let Ok(home) = std::env::var("HOME") {
        cmd.env("HOME", home);
    }
    if let Ok(path) = std::env::var("PATH") {
        cmd.env("PATH", path);
    }
    cmd.cwd(std::env::current_dir()?);

    // Open PTY
    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize {
        rows: 40,
        cols: 120,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut child = pair.slave.spawn_command(cmd)?;
    drop(pair.slave); // close child side; only master needed now

    let mut reader = pair.master.try_clone_reader()?;
    let mut writer = pair.master.take_writer()?;

    // Capture output in background, print everything we see
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_w = Arc::clone(&captured);
    let reader_handle = thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let bytes = &buf[..n];
                    captured_w.lock().unwrap().extend_from_slice(bytes);
                    // Live-print to stderr so we see it scroll
                    let _ = std::io::stderr().write_all(bytes);
                    let _ = std::io::stderr().flush();
                }
                Err(_) => break,
            }
        }
    });

    // Give the target time to come up and render its prompt
    eprintln!("\n[smoke] waiting 4s for target to settle...");
    thread::sleep(Duration::from_secs(4));

    // Build payload: bracketed paste + body + close + CR
    let body = "AGENTBUS-SMOKE-PAYLOAD: please reply with the literal string OK_PASTED";
    let payload = format!("\x1b[200~{}\x1b[201~\r", body);

    eprintln!("\n[smoke] injecting bracketed-paste payload (len={})", payload.len());
    writer.write_all(payload.as_bytes())?;
    writer.flush()?;

    // Capture output for a while so we can see what happens
    let watch = Duration::from_secs(15);
    let start = Instant::now();
    while start.elapsed() < watch {
        if let Some(status) = child.try_wait()? {
            eprintln!("\n[smoke] child exited early: {:?}", status);
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    // Kill child cleanly
    eprintln!("\n[smoke] sending kill...");
    let _ = child.kill();
    let _ = child.wait();

    drop(writer);
    let _ = reader_handle.join();

    // Verdict
    let captured_bytes = captured.lock().unwrap().clone();
    let captured_str = String::from_utf8_lossy(&captured_bytes);
    eprintln!("\n[smoke] === verdict ===");
    eprintln!("[smoke] captured {} bytes total", captured_bytes.len());
    let body_visible = captured_str.contains(body);
    let pasted_marker = captured_str.contains("OK_PASTED");
    eprintln!("[smoke] body text visible in output: {}", body_visible);
    eprintln!("[smoke] OK_PASTED appeared in response: {}", pasted_marker);

    Ok(())
}
