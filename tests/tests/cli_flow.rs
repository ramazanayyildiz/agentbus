//! Tier 5 — End-to-end CLI tests.
//!
//! Covers Test Plan cases UF-001 and UF-002.
//!
//! Each test:
//!   1. Locates `agentbusd` and `agentbus` binaries via the workspace target
//!      directory (discovered relative to CARGO_MANIFEST_DIR).
//!   2. Creates a per-test tempdir under /tmp and uses it as HOME.
//!   3. Spawns the daemon as a child process.
//!   4. Runs the `agentbus` CLI as a child process with the same HOME.
//!   5. Asserts on stdout / exit codes.
//!
//! Harness: the e2e crate (`tests/`) lives inside the agentbus workspace, so
//! `CARGO_MANIFEST_DIR` for this test is `<repo>/tests`. Walking up one
//! directory lands us in the workspace root; `target/debug/agentbusd` and
//! `target/debug/agentbus` live there.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn workspace_target_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR for the e2e crate is `<workspace>/tests`.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .expect("e2e crate must live under a workspace")
        .join("target")
        .join("debug")
}

fn daemon_exe() -> PathBuf {
    workspace_target_dir().join("agentbusd")
}

fn cli_exe() -> PathBuf {
    workspace_target_dir().join("agentbus")
}

/// Ensure both binaries are built before running e2e tests. We rely on the
/// fact that `cargo test --workspace` builds the full workspace, but running
/// just this test crate individually won't. To stay robust, invoke cargo
/// build explicitly once at startup.
fn ensure_binaries_built() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let status = Command::new(env!("CARGO"))
            .args(["build", "--workspace"])
            .status()
            .expect("cargo build");
        assert!(status.success(), "cargo build failed");
    });
    assert!(daemon_exe().exists(), "agentbusd binary missing: {:?}", daemon_exe());
    assert!(cli_exe().exists(), "agentbus binary missing: {:?}", cli_exe());
}

/// Spawn the daemon with a fresh tempdir HOME. Returns the handle and the
/// tempdir (hold both until test ends).
struct DaemonHandle {
    child: Child,
    tmp: TempDir,
}

impl DaemonHandle {
    fn start() -> Self {
        ensure_binaries_built();
        let tmp = tempfile::TempDir::new_in("/tmp").expect("tempdir");
        let home = tmp.path().to_path_buf();
        let child = Command::new(daemon_exe())
            .env("HOME", &home)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn daemon");
        // Wait for socket file
        let socket_path = home.join(".agentbus").join("agentbus.sock");
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if socket_path.exists() {
                std::thread::sleep(Duration::from_millis(50));
                return DaemonHandle { child, tmp };
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let mut c = child;
        let _ = c.kill();
        panic!("daemon socket never appeared at {:?}", socket_path);
    }

    fn home(&self) -> &Path {
        self.tmp.path()
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn cli(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(cli_exe())
        .env("HOME", home)
        .args(args)
        .output()
        .expect("run agentbus cli")
}

// ===========================================================================
// UF-001 — Full demo flow: alice ↔ bob roundtrip via CLI
// ===========================================================================
#[test]
fn uf001_full_demo_flow_alice_bob_roundtrip() {
    let daemon = DaemonHandle::start();

    // register alice
    let out = cli(
        daemon.home(),
        &["register", "--name", "alice", "--program", "test"],
    );
    assert!(
        out.status.success(),
        "register alice failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // register bob
    let out = cli(
        daemon.home(),
        &["register", "--name", "bob", "--program", "test"],
    );
    assert!(out.status.success(), "register bob failed");

    // alice sends "hello"
    let out = cli(
        daemon.home(),
        &[
            "send",
            "--from",
            "alice",
            "--to",
            "bob",
            "--msg-type",
            "request",
            "hello",
        ],
    );
    assert!(
        out.status.success(),
        "send failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // bob reads (non-wait)
    let out = cli(daemon.home(), &["read", "--name", "bob"]);
    assert!(
        out.status.success(),
        "read failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("hello"),
        "expected 'hello' in read output, got: {stdout}"
    );
    assert!(
        stdout.contains("alice"),
        "expected 'alice' in read output, got: {stdout}"
    );
}

// ===========================================================================
// UF-002 — Blocking read: bob waits, alice sends, bob receives
// ===========================================================================
#[test]
fn uf002_blocking_read_bob_waits_alice_sends() {
    let daemon = DaemonHandle::start();

    // Pre-register both agents
    let out = cli(
        daemon.home(),
        &["register", "--name", "alice", "--program", "test"],
    );
    assert!(out.status.success());
    let out = cli(
        daemon.home(),
        &["register", "--name", "bob", "--program", "test"],
    );
    assert!(out.status.success());

    let started = Instant::now();

    // Spawn bob in the background with --wait --timeout 10
    let bob = Command::new(cli_exe())
        .env("HOME", daemon.home())
        .args(["read", "--name", "bob", "--wait", "--timeout", "10"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bob read");

    // Give bob a moment to enter the wait state
    std::thread::sleep(Duration::from_millis(500));

    // alice sends
    let out = cli(
        daemon.home(),
        &[
            "send",
            "--from",
            "alice",
            "--to",
            "bob",
            "--msg-type",
            "request",
            "hello",
        ],
    );
    assert!(out.status.success(), "alice send failed");

    // Wait for bob to exit
    let bob_out = bob.wait_with_output().expect("wait bob");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "bob took too long to exit: {elapsed:?}"
    );
    assert!(
        bob_out.status.success(),
        "bob exited non-zero: stdout={} stderr={}",
        String::from_utf8_lossy(&bob_out.stdout),
        String::from_utf8_lossy(&bob_out.stderr)
    );
    let stdout = String::from_utf8_lossy(&bob_out.stdout);
    assert!(
        stdout.contains("hello"),
        "bob did not receive 'hello': {stdout}"
    );
    assert!(
        stdout.contains("alice"),
        "bob output missing 'alice': {stdout}"
    );
}
