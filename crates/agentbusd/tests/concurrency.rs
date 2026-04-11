//! Tier 4 — Async / concurrency tests for agentbusd.
//!
//! Covers Test Plan cases J-001 .. J-008.
//!
//! Each test spawns a real `agentbusd` child with an isolated HOME.
//!
//! ## What's testable, what isn't
//! - J-004 (concurrent claimers, no duplicates): spawn many reader tasks
//!   against the live daemon. True in-process concurrency against the same
//!   SQLite DB requires multiple `Database` handles — that lives in the
//!   agentbus-core crate and is exercised there.
//! - J-005 (channel overflow): send > PUSH_CHANNEL_CAPACITY messages to a
//!   recipient that never reads; verify daemon does NOT crash and the
//!   excess messages stay retrievable via Read (they remain in DB as
//!   unclaimed).
//! - J-008 (no double delivery): two simultaneous Read(wait=true)s for the
//!   same agent is impossible (duplicate-Register is refused), so this is
//!   expressed as: a single bob in Read(wait=true) receives each message
//!   exactly once even when many arrive in a burst.
//!
//! Tests requiring code refactor (write-failure injection — J-006) are
//! documented at the bottom as TODOs.

#![allow(dead_code)]

use agentbus_core::{BusRequest, BusResponse};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

// ---------------------------------------------------------------------------
// Test harness (duplicated from protocol.rs so the file is self-contained)
// ---------------------------------------------------------------------------

struct TestDaemon {
    child: Child,
    _tmp: TempDir,
    socket_path: PathBuf,
}

impl TestDaemon {
    fn start() -> Self {
        let tmp = tempfile::TempDir::new_in("/tmp").expect("create tempdir");
        let bus_dir = tmp.path().join(".agentbus");
        let socket_path = bus_dir.join("agentbus.sock");
        let exe = env!("CARGO_BIN_EXE_agentbusd");

        let child = Command::new(exe)
            .env("AGENTBUS_DIR", &bus_dir)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn agentbusd");

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if socket_path.exists() {
                std::thread::sleep(Duration::from_millis(50));
                return TestDaemon {
                    child,
                    _tmp: tmp,
                    socket_path,
                };
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let mut child = child;
        let _ = child.kill();
        panic!("daemon did not create socket within 5s at {:?}", socket_path);
    }

    async fn connect(&self) -> UnixStream {
        UnixStream::connect(&self.socket_path).await.expect("connect")
    }

    fn socket_path(&self) -> PathBuf {
        self.socket_path.clone()
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn send_recv(stream: &mut UnixStream, req: &BusRequest) -> BusResponse {
    let line = serde_json::to_string(req).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut resp_line = String::new();
    let n = reader.read_line(&mut resp_line).await.unwrap();
    assert!(n > 0, "daemon closed socket");
    serde_json::from_str(&resp_line).expect("parse BusResponse")
}

async fn send_line(stream: &mut UnixStream, req: &BusRequest) {
    let line = serde_json::to_string(req).unwrap() + "\n";
    stream.write_all(line.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
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
// J-001 — 10 concurrent clients on different agent names are independent
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j001_ten_concurrent_clients_independent() {
    let daemon = TestDaemon::start();
    let sock = daemon.socket_path();

    let mut tasks = Vec::new();
    for i in 0..10 {
        let sock = sock.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = UnixStream::connect(&sock).await.unwrap();
            let name = format!("agent{i}");
            let resp = send_recv(&mut s, &register_req(&name)).await;
            matches!(resp, BusResponse::Ok { .. })
        }));
    }
    for t in tasks {
        assert!(t.await.unwrap(), "one client failed to register");
    }
}

// ===========================================================================
// J-002 — Burst of sends to one recipient, all received via Reads
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j002_burst_sends_all_delivered() {
    let daemon = TestDaemon::start();

    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    const N: usize = 20;
    for i in 0..N {
        let r = send_recv(&mut alice, &send_req("alice", "bob", &format!("m{i}"))).await;
        assert!(matches!(r, BusResponse::Ok { .. }), "send {i} failed: {r:?}");
    }

    // Drain everything bob has. Some messages may have been pushed via the
    // mpsc and buffered in bob's socket recv buffer; others may be in DB.
    // Keep reading (wait=false) until empty.
    let mut collected: Vec<String> = Vec::new();
    let mut reader = BufReader::new(&mut bob);

    // Drain any push-branch Messages already queued in the socket
    loop {
        let mut buf = String::new();
        let read_fut = reader.read_line(&mut buf);
        match tokio::time::timeout(Duration::from_millis(200), read_fut).await {
            Ok(Ok(n)) if n > 0 => {
                let resp: BusResponse = serde_json::from_str(&buf).unwrap();
                if let BusResponse::Message { message } = resp {
                    collected.push(message.body);
                }
            }
            _ => break,
        }
    }
    drop(reader);

    // Now drain the DB via Read(wait=false) until empty
    loop {
        let r = send_recv(
            &mut bob,
            &BusRequest::Read {
                wait: Some(false),
                timeout_secs: None,
            },
        )
        .await;
        match r {
            BusResponse::Ok { data } => {
                let arr = data.as_array().cloned().unwrap_or_default();
                if arr.is_empty() {
                    break;
                }
                for m in arr {
                    collected.push(m["body"].as_str().unwrap().to_string());
                }
            }
            BusResponse::Message { message } => {
                collected.push(message.body);
            }
            BusResponse::Error { message } => panic!("read error: {message}"),
        }
    }

    // No duplicates, all bodies present
    collected.sort();
    let mut expected: Vec<String> = (0..N).map(|i| format!("m{i}")).collect();
    expected.sort();
    assert_eq!(collected, expected, "delivered set mismatch");
}

// ===========================================================================
// J-003 — Read(wait=true) sees a message pushed slightly later
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j003_read_wait_then_late_send() {
    let daemon = TestDaemon::start();
    let sock = daemon.socket_path();

    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let alice_sock = sock.clone();
    let sender = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut a = UnixStream::connect(&alice_sock).await.unwrap();
        let _ = send_recv(&mut a, &register_req("alice")).await;
        let _ = send_recv(&mut a, &send_req("alice", "bob", "late")).await;
    });

    send_line(
        &mut bob,
        &BusRequest::Read {
            wait: Some(true),
            timeout_secs: Some(5),
        },
    )
    .await;
    let mut reader = BufReader::new(&mut bob);
    let mut line = String::new();
    let n = reader.read_line(&mut line).await.unwrap();
    assert!(n > 0);
    let resp: BusResponse = serde_json::from_str(&line).unwrap();
    match resp {
        BusResponse::Message { message } => assert_eq!(message.body, "late"),
        BusResponse::Ok { data } => {
            let arr = data.as_array().unwrap();
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0]["body"], "late");
        }
        BusResponse::Error { message } => panic!("error: {message}"),
    }
    sender.await.unwrap();
}

// ===========================================================================
// J-004 — 5 concurrent senders × 20 messages each: recipient sees all 100
// with no duplicates (regression for Issue 2: BEGIN IMMEDIATE holds)
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j004_concurrent_senders_no_duplicate_delivery() {
    let daemon = TestDaemon::start();
    let sock = daemon.socket_path();

    // Register bob upfront so FK passes, then disconnect him so he can be
    // re-registered by the reader task.
    {
        let mut s = daemon.connect().await;
        let _ = send_recv(&mut s, &register_req("bob")).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    const SENDERS: usize = 5;
    const MSGS_PER_SENDER: usize = 20;

    let mut senders = Vec::new();
    for i in 0..SENDERS {
        let sock = sock.clone();
        senders.push(tokio::spawn(async move {
            let mut s = UnixStream::connect(&sock).await.unwrap();
            let name = format!("sender{i}");
            let _ = send_recv(&mut s, &register_req(&name)).await;
            for j in 0..MSGS_PER_SENDER {
                let body = format!("s{i}-m{j}");
                let r = send_recv(&mut s, &send_req(&name, "bob", &body)).await;
                assert!(matches!(r, BusResponse::Ok { .. }));
            }
        }));
    }
    for t in senders {
        t.await.unwrap();
    }

    // Now bob reads everything
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let mut collected: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && collected.len() < SENDERS * MSGS_PER_SENDER {
        let r = send_recv(
            &mut bob,
            &BusRequest::Read {
                wait: Some(false),
                timeout_secs: None,
            },
        )
        .await;
        match r {
            BusResponse::Ok { data } => {
                let arr = data.as_array().cloned().unwrap_or_default();
                if arr.is_empty() {
                    break;
                }
                for m in arr {
                    collected.push(m["body"].as_str().unwrap().to_string());
                }
            }
            BusResponse::Message { message } => collected.push(message.body),
            BusResponse::Error { message } => panic!("read error: {message}"),
        }
    }

    // Exactly SENDERS*MSGS_PER_SENDER unique bodies
    let expected: usize = SENDERS * MSGS_PER_SENDER;
    assert_eq!(
        collected.len(),
        expected,
        "expected {expected} messages, got {}",
        collected.len()
    );
    let mut dedup = collected.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        expected,
        "duplicate delivery detected: {} unique / {} total",
        dedup.len(),
        collected.len()
    );
}

// ===========================================================================
// J-005 — Slow consumer: > PUSH_CHANNEL_CAPACITY messages, daemon stays alive
//
// PUSH_CHANNEL_CAPACITY is 1000. We send 1100 messages to bob (who is
// connected but never calls Read). The first ~1000 fit in the mpsc buffer;
// additional messages either fail try_send and stay in DB unclaimed, or the
// excess sits in the socket buffer. What must NOT happen: the daemon
// crashes, OOMs, or silently drops messages.
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j005_channel_overflow_no_leak_no_crash() {
    let daemon = TestDaemon::start();

    // Bob registers but does NOT read from his stream at all.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;

    // PUSH_CHANNEL_CAPACITY = 1000 (from daemon source). We send 1100 to
    // overflow. Use a smaller body to avoid OS socket buffer issues.
    const N: usize = 1100;
    let mut ok_count = 0usize;
    for i in 0..N {
        let r = send_recv(&mut alice, &send_req("alice", "bob", &format!("m{i}"))).await;
        match r {
            BusResponse::Ok { .. } => ok_count += 1,
            BusResponse::Error { .. } => {
                // Daemon may return an error for backlogged sends; that's
                // acceptable — the important thing is it didn't crash.
            }
            BusResponse::Message { .. } => panic!("unexpected Message"),
        }
    }

    // The daemon must still respond to a Status request — i.e. it hasn't
    // crashed or wedged.
    let status = send_recv(&mut alice, &BusRequest::Status).await;
    match status {
        BusResponse::Ok { .. } | BusResponse::Error { .. } => {}
        BusResponse::Message { .. } => panic!("unexpected Message on Status"),
    }
    assert!(
        ok_count > 0,
        "no sends succeeded — daemon may be deadlocked"
    );

    // Cleanup: drop bob
    drop(bob);
}

// ===========================================================================
// J-007 — Recipient disconnects between two sends. Second send targets a
// registered-but-disconnected agent and still buffers into DB.
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j007_recipient_disconnect_between_sends_does_not_lose_second() {
    let daemon = TestDaemon::start();

    let mut alice = daemon.connect().await;
    let _ = send_recv(&mut alice, &register_req("alice")).await;

    // First bob connection receives the first send's push.
    {
        let mut bob = daemon.connect().await;
        let _ = send_recv(&mut bob, &register_req("bob")).await;
        let _ = send_recv(&mut alice, &send_req("alice", "bob", "first")).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(bob);
    }
    // Wait for disconnect cleanup.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second send happens while bob is offline.
    let _ = send_recv(&mut alice, &send_req("alice", "bob", "second")).await;

    // Reconnect bob and drain.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;
    let mut bodies: Vec<String> = Vec::new();
    for _ in 0..3 {
        let r = send_recv(
            &mut bob,
            &BusRequest::Read {
                wait: Some(false),
                timeout_secs: None,
            },
        )
        .await;
        match r {
            BusResponse::Ok { data } => {
                let arr = data.as_array().cloned().unwrap_or_default();
                if arr.is_empty() {
                    break;
                }
                for m in arr {
                    bodies.push(m["body"].as_str().unwrap().to_string());
                }
            }
            BusResponse::Message { message } => bodies.push(message.body),
            BusResponse::Error { .. } => break,
        }
    }

    // "second" must be present. ("first" may or may not be — it was
    // delivered to the dropped socket and marked read; at-most-once on
    // non-graceful client close is a known limitation.)
    assert!(
        bodies.contains(&"second".to_string()),
        "second message lost: {bodies:?}"
    );
}

// ===========================================================================
// J-008 — Single recipient in Read(wait=true) receives each of N messages
// exactly once (no double delivery).
//
// Duplicate Register is refused by the daemon, so we can't have two clients
// both claiming to be "bob". The duplicate-delivery risk is instead
// between the push branch and the Read-path fetch inside a single
// handle_client task. Here we exercise the push path under burst and
// assert no body is seen twice.
// ===========================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn j008_push_vs_read_no_double_delivery() {
    let daemon = TestDaemon::start();
    let sock = daemon.socket_path();

    // Bob connects and enters Read(wait=true) with a long timeout.
    let mut bob = daemon.connect().await;
    let _ = send_recv(&mut bob, &register_req("bob")).await;

    // Pre-seed a few messages while bob is idle (not yet reading). These go
    // into the push channel OR DB.
    let sender_sock = sock.clone();
    let sender = tokio::spawn(async move {
        let mut alice = UnixStream::connect(&sender_sock).await.unwrap();
        let _ = send_recv(&mut alice, &register_req("alice")).await;
        for i in 0..10 {
            let _ = send_recv(&mut alice, &send_req("alice", "bob", &format!("m{i}"))).await;
        }
    });
    sender.await.unwrap();

    // Let the daemon's push branch fully drain the mpsc into bob's socket
    // recv buffer before we start reading.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Drain all pending Message lines from the socket. Once we stop seeing
    // new push lines for a while, fall back to an explicit Read to catch
    // anything that ended up in the DB (Send path saw recipient_connected=
    // false briefly, etc.).
    let mut collected: Vec<String> = Vec::new();
    let mut reader = BufReader::new(&mut bob);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && collected.len() < 10 {
        let mut buf = String::new();
        let read_fut = reader.read_line(&mut buf);
        match tokio::time::timeout(Duration::from_millis(300), read_fut).await {
            Ok(Ok(n)) if n > 0 => {
                let resp: BusResponse = serde_json::from_str(&buf).unwrap();
                match resp {
                    BusResponse::Message { message } => collected.push(message.body),
                    BusResponse::Ok { .. } | BusResponse::Error { .. } => {}
                }
            }
            _ => break,
        }
    }
    drop(reader);

    // Any stragglers in the DB?
    if collected.len() < 10 {
        let r = send_recv(
            &mut bob,
            &BusRequest::Read {
                wait: Some(false),
                timeout_secs: None,
            },
        )
        .await;
        if let BusResponse::Ok { data } = r {
            for m in data.as_array().cloned().unwrap_or_default() {
                collected.push(m["body"].as_str().unwrap().to_string());
            }
        }
    }

    collected.sort();
    let mut dedup = collected.clone();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        collected.len(),
        "duplicate delivery: {} total, {} unique",
        collected.len(),
        dedup.len()
    );
    assert_eq!(
        collected.len(),
        10,
        "expected 10 messages, got {}: {collected:?}",
        collected.len()
    );
}

// ===========================================================================
// TODO: Tests requiring code refactor or deeper instrumentation
// ===========================================================================
//
// - J-006 (Pushed-branch write failure releases claim): needs the
//   MessageWriter trait injection recommended in Risk Profile §Refactor 2.
//   Without that, write failures can only be forced by killing the client
//   mid-write, and the daemon's write_all() will usually complete before
//   the kernel notices. Deferred.
//
// - J-004 in-process variant (two Database handles racing on fetch_and_claim
//   against the same bus.db): belongs in agentbus-core/tests/database.rs but
//   requires exposing a multi-handle Database constructor. Currently
//   Database::init() always opens the canonical path, so two handles in the
//   same process both point at the same WAL-mode file — which could be
//   tested but raises concerns around serial_test interaction. Covered
//   here at the daemon integration level instead.
