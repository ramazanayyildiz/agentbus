//! Tier 3 — Integration tests for agentbusd protocol.
//!
//! Covers Test Plan cases F-001 .. F-014.
//!
//! Each test spawns a real `agentbusd` child process with `HOME` pointed at a
//! unique tempdir under /tmp. This gives TRUE process isolation — concurrent
//! tests cannot race on `dirs::home_dir()`.
//!
//! ## TestDaemon harness
//! - `TestDaemon::start()` spawns the daemon, waits for the unix socket to
//!   appear on disk, and returns a handle.
//! - `TestDaemon::connect()` opens a fresh `UnixStream` to the socket.
//! - `TestDaemon::drop()` kills the child.

#![allow(dead_code)]

use agentbus_core::{BusRequest, BusResponse};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Spawned daemon child + its isolated HOME. Kills the child on drop.
struct TestDaemon {
    child: Child,
    tmp: TempDir,
    socket_path: PathBuf,
}

impl TestDaemon {
    /// Start a fresh daemon in an isolated tempdir. Waits up to 5 seconds for
    /// the unix socket to appear.
    fn start() -> Self {
        let tmp = tempfile::TempDir::new_in("/tmp").expect("create tempdir");
        let home = tmp.path().to_path_buf();
        let socket_path = home.join(".agentbus").join("agentbus.sock");

        // `CARGO_BIN_EXE_agentbusd` is set by Cargo for integration tests in
        // the same crate as the binary.
        let exe = env!("CARGO_BIN_EXE_agentbusd");

        let child = Command::new(exe)
            .env("HOME", &home)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn agentbusd");

        // Wait for socket file to appear
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if socket_path.exists() {
                // Give the listener a beat to actually start accept()ing.
                std::thread::sleep(Duration::from_millis(50));
                return TestDaemon {
                    child,
                    tmp,
                    socket_path,
                };
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        // Failed — kill the child before panicking.
        let mut child = child;
        let _ = child.kill();
        panic!("daemon did not create socket within 5s at {:?}", socket_path);
    }

    async fn connect(&self) -> UnixStream {
        UnixStream::connect(&self.socket_path)
            .await
            .expect("connect to socket")
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Convenience: send one JSON request, await one JSON response line.
async fn send_recv(stream: &mut UnixStream, req: &BusRequest) -> BusResponse {
    let line = serde_json::to_string(req).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut resp_line = String::new();
    let n = reader.read_line(&mut resp_line).await.unwrap();
    assert!(n > 0, "daemon closed socket without responding");
    serde_json::from_str(&resp_line).expect("parse BusResponse")
}

/// Send only (no read). Used when the caller wants to drive the reader
/// themselves (e.g. for Read wait flows).
async fn send_line(stream: &mut UnixStream, req: &BusRequest) {
    let line = serde_json::to_string(req).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
}

async fn read_one_response(reader: &mut BufReader<&mut UnixStream>) -> BusResponse {
    let mut resp_line = String::new();
    let n = reader
        .read_line(&mut resp_line)
        .await
        .expect("read response");
    assert!(n > 0, "socket closed");
    serde_json::from_str(&resp_line).unwrap_or_else(|e| {
        panic!("parse response failed: {e}; line: {resp_line:?}")
    })
}

fn register_req(name: &str) -> BusRequest {
    BusRequest::Register {
        name: name.to_string(),
        program: "test".to_string(),
        model: "unknown".to_string(),
        project: "default".to_string(),
    }
}

fn send_req(from: &str, to: &str, body: &str) -> BusRequest {
    BusRequest::Send {
        from: Some(from.to_string()),
        to: to.to_string(),
        thread_id: None,
        msg_type: "request".to_string(),
        body: body.to_string(),
    }
}

// ===========================================================================
// F-001 — Client connects, registers, receives Ok
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f001_register_happy_path() {
    let daemon = TestDaemon::start();
    let mut s = daemon.connect().await;
    let resp = send_recv(&mut s, &register_req("alice")).await;
    assert!(
        matches!(resp, BusResponse::Ok { .. }),
        "expected Ok, got {resp:?}"
    );
}

// ===========================================================================
// F-002 — List returns registered agents
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f002_list_returns_registered_agents() {
    let daemon = TestDaemon::start();
    let mut s1 = daemon.connect().await;
    let _ = send_recv(&mut s1, &register_req("alice")).await;

    let mut s2 = daemon.connect().await;
    let _ = send_recv(&mut s2, &register_req("bob")).await;

    // Third connection just to call List
    let mut s3 = daemon.connect().await;
    let _ = send_recv(&mut s3, &register_req("carol")).await;
    let resp = send_recv(&mut s3, &BusRequest::List).await;
    match resp {
        BusResponse::Ok { data } => {
            // data is a JSON array of Agent
            let arr = data.as_array().expect("data should be array");
            assert!(arr.len() >= 3);
            let names: Vec<String> = arr
                .iter()
                .map(|a| a["name"].as_str().unwrap_or("").to_string())
                .collect();
            assert!(names.contains(&"alice".to_string()));
            assert!(names.contains(&"bob".to_string()));
            assert!(names.contains(&"carol".to_string()));
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

// ===========================================================================
// F-003 — Push branch delivers to a client in Read(wait=true)
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f003_push_while_in_read_wait_delivers() {
    let daemon = TestDaemon::start();

    // Bob connects, registers, and starts a blocking Read.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    // Alice connects and registers in a separate task so she doesn't block bob.
    let sock = daemon.socket_path.clone();
    let sender = tokio::spawn(async move {
        // Small delay so bob enters Read first
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut alice = UnixStream::connect(&sock).await.unwrap();
        let _ = send_recv(&mut alice, &register_req("alice")).await;
        let _ = send_recv(&mut alice, &send_req("alice", "bob", "hello")).await;
    });

    // Bob issues Read with wait=true
    send_line(
        &mut bob,
        &BusRequest::Read {
            wait: Some(true),
            timeout_secs: Some(5),
        },
    )
    .await;
    let mut reader = BufReader::new(&mut bob);
    let resp = read_one_response(&mut reader).await;
    match resp {
        BusResponse::Message { message } => {
            assert_eq!(message.from, "alice");
            assert_eq!(message.body, "hello");
        }
        BusResponse::Ok { data } => {
            // If a non-push fetch path fires first, data should contain a list;
            // still acceptable as long as alice's message is present.
            let arr = data.as_array().expect("data should be array");
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0]["body"], "hello");
        }
        other => panic!("unexpected response: {other:?}"),
    }

    sender.await.unwrap();
}

// ===========================================================================
// F-004 — Push branch delivers while client is idle (no active Read)
// The daemon buffers into the push mpsc; the next Read should see the message.
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f004_push_while_idle_deliverable_on_next_read() {
    let daemon = TestDaemon::start();

    // Bob registers but does not Read yet.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    // Alice registers and sends on a separate connection.
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;
    let _ = send_recv(&mut alice, &send_req("alice", "bob", "idle-push")).await;

    // Give the daemon a moment to attempt the push
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now Bob reads. The message should come back either via the pushed
    // channel's buffered mpsc (delivered as Message) or via a DB fetch on
    // Read (delivered as an Ok list). Either is acceptable — what must NOT
    // happen is loss.
    send_line(
        &mut bob,
        &BusRequest::Read {
            wait: Some(false),
            timeout_secs: None,
        },
    )
    .await;
    let mut reader = BufReader::new(&mut bob);
    let resp = read_one_response(&mut reader).await;
    match resp {
        BusResponse::Message { message } => {
            assert_eq!(message.body, "idle-push");
        }
        BusResponse::Ok { data } => {
            let arr = data.as_array().expect("data should be array");
            assert!(!arr.is_empty(), "message must not be lost");
            assert_eq!(arr[0]["body"], "idle-push");
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

// ===========================================================================
// F-005 — Non-wait Read on an empty queue returns Ok with empty list
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f005_read_nowait_empty_queue_returns_empty_ok() {
    let daemon = TestDaemon::start();
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;
    let resp = send_recv(
        &mut bob,
        &BusRequest::Read {
            wait: Some(false),
            timeout_secs: None,
        },
    )
    .await;
    match resp {
        BusResponse::Ok { data } => {
            // An empty list is the expected shape
            let arr = data.as_array().unwrap_or(&Vec::new()).clone();
            assert!(arr.is_empty(), "expected empty list, got {data:?}");
        }
        BusResponse::Message { .. } => panic!("unexpected Message on empty queue"),
        BusResponse::Error { message } => panic!("unexpected error: {message}"),
    }
}

// ===========================================================================
// F-006 — Full send/read flow
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f006_send_and_read_full_flow() {
    let daemon = TestDaemon::start();
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;

    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let _ = send_recv(&mut alice, &send_req("alice", "bob", "hi bob")).await;

    // Give the push channel a brief moment
    tokio::time::sleep(Duration::from_millis(150)).await;

    let resp = send_recv(
        &mut bob,
        &BusRequest::Read {
            wait: Some(false),
            timeout_secs: None,
        },
    )
    .await;
    let body = match resp {
        BusResponse::Message { message } => message.body,
        BusResponse::Ok { data } => {
            let arr = data.as_array().expect("array");
            assert_eq!(arr.len(), 1);
            arr[0]["body"].as_str().unwrap().to_string()
        }
        other => panic!("unexpected: {other:?}"),
    };
    assert_eq!(body, "hi bob");
}

// ===========================================================================
// F-007 — Duplicate Register is refused (Issue 4 regression)
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f007_duplicate_register_refused() {
    let daemon = TestDaemon::start();
    let mut conn1 = daemon.connect().await;
    let resp1 = send_recv(&mut conn1, &register_req("alice")).await;
    assert!(matches!(resp1, BusResponse::Ok { .. }));

    let mut conn2 = daemon.connect().await;
    let resp2 = send_recv(&mut conn2, &register_req("alice")).await;
    match resp2 {
        BusResponse::Error { message } => {
            assert!(
                message.contains("already connected"),
                "unexpected error message: {message}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ===========================================================================
// F-008 — Send to non-existent agent surfaces FK violation (Issue 8)
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f008_send_to_nonexistent_agent_is_error() {
    let daemon = TestDaemon::start();
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;

    let resp = send_recv(&mut alice, &send_req("alice", "ghost", "x")).await;
    match resp {
        BusResponse::Error { message } => {
            // Don't hardcode the exact SQLite error text — just confirm it's an
            // error path, not a silent success.
            assert!(
                message.to_lowercase().contains("send failed")
                    || message.to_lowercase().contains("foreign")
                    || message.to_lowercase().contains("constraint"),
                "unexpected error text: {message}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ===========================================================================
// F-009 — Unknown msg_type in Send fails loudly (Issue 9 regression)
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f009_unknown_msg_type_in_send_fails_loudly() {
    let daemon = TestDaemon::start();
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let req = BusRequest::Send {
        from: Some("alice".to_string()),
        to: "bob".to_string(),
        thread_id: None,
        msg_type: "invalid".to_string(),
        body: "x".to_string(),
    };
    let resp = send_recv(&mut alice, &req).await;
    match resp {
        BusResponse::Error { message } => {
            assert!(
                message.to_lowercase().contains("invalid message type"),
                "unexpected error text: {message}"
            );
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ===========================================================================
// F-010 — Read(wait=true, timeout_secs=1) times out with no messages
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f010_read_wait_with_timeout_times_out() {
    let daemon = TestDaemon::start();
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let started = Instant::now();
    let resp = send_recv(
        &mut bob,
        &BusRequest::Read {
            wait: Some(true),
            timeout_secs: Some(1),
        },
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "timeout took too long: {elapsed:?}"
    );
    match resp {
        BusResponse::Error { message } => {
            assert!(
                message.to_lowercase().contains("timeout")
                    || message.to_lowercase().contains("no messages"),
                "unexpected: {message}"
            );
        }
        BusResponse::Ok { data } => {
            // Accept an empty-list Ok as an equally valid "nothing happened"
            // representation, as long as elapsed is bounded.
            let arr = data.as_array().cloned().unwrap_or_default();
            assert!(arr.is_empty());
        }
        BusResponse::Message { .. } => panic!("unexpected Message on empty queue"),
    }
}

// ===========================================================================
// F-011 — Offline recipient: message persists in DB until bob comes online
// (weaker proof of at-least-once than the test plan's "mid-flight" scenario,
// but actually provable against the real daemon).
//
// The test plan's original scenario — "disconnect BEFORE mark_read" — cannot
// be exercised end-to-end without a write-failure injection harness because
// the daemon uses deliver-then-mark-read at the kernel layer: a successful
// `write_all()` into a Unix socket is treated as delivery, even if the
// client drops the connection immediately after. That path is intrinsic to
// the current daemon design and is covered at the DB layer by the
// `release_all_claims_for` unit tests in `agentbus-core/tests/database.rs`.
// See the Risk Profile's "Refactor Recommendation 2: MessageWriter trait"
// for the injection point needed to fully test B5 in-process.
//
// Here we verify the weaker property: if a recipient is registered but not
// currently connected at the moment Send runs, the message must still be
// delivered the next time they Read. This exercises the "recipient_connected
// = false" branch in dispatch_request::Send → no push attempted → row sits
// in DB unclaimed.
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f011_offline_recipient_reads_queued_message() {
    let daemon = TestDaemon::start();

    // Pre-register bob via a throwaway connection, then drop it so bob is
    // not in the clients map when alice sends.
    {
        let mut bob_register = daemon.connect().await;
        let _ = send_recv(&mut bob_register, &register_req("bob")).await;
        drop(bob_register);
    }
    // Wait for the disconnect cleanup to run.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Alice registers and sends to bob (who is not currently connected).
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;
    let resp = send_recv(&mut alice, &send_req("alice", "bob", "offline-msg")).await;
    assert!(
        matches!(resp, BusResponse::Ok { .. }),
        "send failed: {resp:?}"
    );

    // Bob reconnects as a fresh client. Message must be waiting in DB.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;
    let resp = send_recv(
        &mut bob,
        &BusRequest::Read {
            wait: Some(true),
            timeout_secs: Some(3),
        },
    )
    .await;
    let got_body = match resp {
        BusResponse::Message { message } => message.body,
        BusResponse::Ok { data } => {
            let arr = data.as_array().expect("array");
            assert!(!arr.is_empty(), "message lost — at-least-once broken");
            arr[0]["body"].as_str().unwrap().to_string()
        }
        BusResponse::Error { message } => {
            panic!("reconnected bob did not see the queued message: {message}")
        }
    };
    assert_eq!(got_body, "offline-msg");
}

// ===========================================================================
// F-012 — Malformed JSON returns Error, connection stays open
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f012_malformed_json_returns_error_connection_stays_open() {
    let daemon = TestDaemon::start();
    let mut s = daemon.connect().await;
    s.write_all(b"not valid json\n").await.unwrap();
    s.flush().await.unwrap();

    let mut reader = BufReader::new(&mut s);
    let resp = read_one_response(&mut reader).await;
    assert!(matches!(resp, BusResponse::Error { .. }));
    drop(reader);

    // Connection should still be usable — register succeeds after.
    let resp2 = send_recv(&mut s, &register_req("alice")).await;
    assert!(
        matches!(resp2, BusResponse::Ok { .. }),
        "connection was killed by parse error: {resp2:?}"
    );
}

// ===========================================================================
// F-013 — Empty line is ignored
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f013_empty_line_ignored_no_response() {
    let daemon = TestDaemon::start();
    let mut s = daemon.connect().await;
    s.write_all(b"\n").await.unwrap();
    s.flush().await.unwrap();

    // Wait briefly; daemon should NOT produce output. Then send a real
    // request and ensure it still works.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let resp = send_recv(&mut s, &register_req("alice")).await;
    assert!(matches!(resp, BusResponse::Ok { .. }));
}

// ===========================================================================
// F-014 — Oversized payload does not panic the daemon
// ===========================================================================
#[tokio::test(flavor = "multi_thread")]
async fn f014_oversized_payload_does_not_panic_daemon() {
    let daemon = TestDaemon::start();
    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    // 1 MiB body
    let big = "x".repeat(1024 * 1024);
    let req = send_req("alice", "bob", &big);
    let resp = send_recv(&mut alice, &req).await;
    // Either Ok or Error is fine — we just want to prove the daemon is alive.
    match resp {
        BusResponse::Ok { .. } | BusResponse::Error { .. } => {}
        BusResponse::Message { .. } => panic!("unexpected Message response"),
    }
    // Confirm liveness with a cheap follow-up.
    let ping = send_recv(&mut alice, &BusRequest::Status).await;
    match ping {
        BusResponse::Ok { .. } | BusResponse::Error { .. } => {}
        BusResponse::Message { .. } => panic!("unexpected Message on Status"),
    }
}
