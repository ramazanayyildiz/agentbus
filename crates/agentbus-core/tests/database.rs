//! Tier 2 — Database layer tests for agentbus-core.
//!
//! Covers Test Plan cases M-001 .. M-012.
//!
//! ## Isolation
//!
//! `Database::init()` now honours the `AGENTBUS_DIR` env var, which points
//! directly at the `.agentbus` directory to use. Tests still mutate a
//! process-global env var, so we keep `serial_test::serial` on every test
//! that touches it. Each test creates its own `tempfile::TempDir` and points
//! `AGENTBUS_DIR` at `<tmp>/.agentbus` for the duration of the test.
//!
//! Each test also opens a separate raw `rusqlite::Connection` against the
//! created `<tmp>/.agentbus/bus.db` for read-only assertions (checking
//! `read_at`, `claimed_at`, etc. directly).

use agentbus_core::{AgentState, Database, MessageType};
use rusqlite::{Connection, OpenFlags};
use serial_test::serial;
use std::path::PathBuf;
use tempfile::TempDir;

/// Fresh isolated Database rooted at a tempdir. Returns (db, tempdir) — hold
/// the tempdir for the lifetime of the test so it isn't cleaned up early.
fn fresh_db() -> (Database, TempDir) {
    // Force tempdir into /tmp so we're on a real local filesystem (iCloud-
    // backed directories can't host SQLite databases reliably).
    let tmp = tempfile::TempDir::new_in("/tmp").expect("create tempdir in /tmp");
    // Point the core `agentbus_dir()` helper at this test's tempdir via
    // `AGENTBUS_DIR`. Still process-global, hence `#[serial]` on tests.
    std::env::set_var("AGENTBUS_DIR", tmp.path().join(".agentbus"));
    // Defensive cleanup in case a prior test leaked into this dir.
    let _ = std::fs::remove_dir_all(tmp.path().join(".agentbus"));
    let db = Database::init().expect("Database::init");
    (db, tmp)
}

/// Direct read-only connection to the db file for whitebox assertions.
/// The real daemon stores data in `~/.agentbus/bus.db` (NB: `bus.db`, not
/// `agentbus.db`).
fn raw_conn(tmp: &TempDir) -> Connection {
    let path: PathBuf = tmp.path().join(".agentbus").join("bus.db");
    Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)
        .expect("open raw conn")
}

// ---------------------------------------------------------------------------
// M-001 — Database::init creates schema, pragmas, indices
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m001_init_creates_schema_pragmas_indices() {
    let (_db, tmp) = fresh_db();
    let conn = raw_conn(&tmp);

    // Tables exist
    let agents_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='agents'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(agents_exists, 1, "agents table missing");

    let messages_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='messages'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(messages_exists, 1, "messages table missing");

    // claimed_at column exists on messages
    let has_claimed_at: bool = conn
        .prepare("SELECT 1 FROM pragma_table_info('messages') WHERE name = 'claimed_at'")
        .unwrap()
        .exists([])
        .unwrap();
    assert!(has_claimed_at, "claimed_at column missing");

    // PRAGMA foreign_keys = 1
    let fk: i64 = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert_eq!(fk, 1, "foreign_keys not enabled");

    // PRAGMA journal_mode = wal (may come back as "wal")
    let jm: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(jm.to_lowercase(), "wal", "journal_mode should be WAL");

    // Index exists
    let idx_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_messages_to_agent'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(idx_exists, 1, "idx_messages_to_agent missing");
}

// ---------------------------------------------------------------------------
// M-002 — register_agent inserts new agent and returns a canonical row
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m002_register_agent_inserts_canonical_row() {
    let (db, _tmp) = fresh_db();
    let agent = db
        .register_agent("alice", "claude-code", "opus", "agentbus")
        .expect("register");

    assert_eq!(agent.name, "alice");
    assert_eq!(agent.program, "claude-code");
    assert_eq!(agent.model, "opus");
    assert_eq!(agent.project, "agentbus");
    assert_eq!(agent.state, AgentState::Active);
    // UUID-formatted id (36 chars with 4 dashes)
    assert_eq!(agent.id.len(), 36, "id should be a UUID");
    assert_eq!(agent.id.matches('-').count(), 4);
    // registered_at is an RFC3339 timestamp
    assert!(
        chrono::DateTime::parse_from_rfc3339(&agent.registered_at).is_ok(),
        "registered_at not RFC3339: {}",
        agent.registered_at
    );
}

// ---------------------------------------------------------------------------
// M-003 — duplicate register updates metadata, keeps id and registered_at
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m003_duplicate_register_preserves_id_and_registered_at() {
    let (db, _tmp) = fresh_db();
    let a1 = db.register_agent("alice", "p1", "m1", "proj1").unwrap();

    // Second call with different program/model/project
    let a2 = db.register_agent("alice", "p2", "m2", "proj2").unwrap();

    assert_eq!(a1.id, a2.id, "id should be preserved on re-register");
    assert_eq!(
        a1.registered_at, a2.registered_at,
        "registered_at should be preserved"
    );
    // Metadata is updated
    assert_eq!(a2.program, "p2");
    assert_eq!(a2.model, "m2");
    assert_eq!(a2.project, "proj2");
}

// ---------------------------------------------------------------------------
// M-004 — send_message inserts a row and returns struct with new UUID
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m004_send_message_inserts_and_returns_struct() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    let msg = db
        .send_message("alice", "bob", None, MessageType::Request, "hi")
        .expect("send");
    assert_eq!(msg.from, "alice");
    assert_eq!(msg.to, "bob");
    assert_eq!(msg.msg_type, MessageType::Request);
    assert_eq!(msg.body, "hi");
    assert!(msg.read_at.is_none());
    assert_eq!(msg.id.len(), 36);
    assert!(chrono::DateTime::parse_from_rfc3339(&msg.created_at).is_ok());
}

// ---------------------------------------------------------------------------
// M-005 — fetch_and_claim_messages returns unread + stamps claimed_at (atomic)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m005_fetch_and_claim_stamps_claimed_at_and_is_idempotent() {
    let (mut db, tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    for i in 0..3 {
        db.send_message("alice", "bob", None, MessageType::Request, &format!("m{i}"))
            .unwrap();
    }

    let first = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(first.len(), 3, "should return all 3 pending");

    // Second call sees no unclaimed rows — atomic claim
    let second = db.fetch_and_claim_messages("bob").unwrap();
    assert!(second.is_empty(), "second fetch should be empty");

    // Whitebox: all 3 rows have claimed_at set
    let conn = raw_conn(&tmp);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE to_agent='bob' AND claimed_at IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 3);
}

// ---------------------------------------------------------------------------
// M-006 — mark_read sets read_at
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m006_mark_read_sets_read_at() {
    let (mut db, tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();
    let _claimed = db.fetch_and_claim_messages("bob").unwrap();

    db.mark_read(&m.id).expect("mark_read");

    let conn = raw_conn(&tmp);
    let read_at: Option<String> = conn
        .query_row(
            "SELECT read_at FROM messages WHERE id = ?",
            rusqlite::params![&m.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(read_at.is_some(), "read_at should be set");
}

// ---------------------------------------------------------------------------
// M-007 — release_claim un-claims an unread message
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m007_release_claim_unclaims_unread_message() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();

    let first = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(first.len(), 1);
    // Second fetch is empty because it's claimed
    assert!(db.fetch_and_claim_messages("bob").unwrap().is_empty());

    db.release_claim(&m.id).expect("release_claim");

    // Now the message comes back
    let again = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].id, m.id);
}

// ---------------------------------------------------------------------------
// M-007b — release_claim does NOT re-open an already-delivered message
// (the AND read_at IS NULL guard)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m007b_release_claim_does_not_unread_delivered_message() {
    let (mut db, tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();
    let _ = db.fetch_and_claim_messages("bob").unwrap();
    db.mark_read(&m.id).unwrap();

    // This should be a no-op because read_at IS NOT NULL
    db.release_claim(&m.id).unwrap();

    let conn = raw_conn(&tmp);
    let (read_at, claimed_at): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT read_at, claimed_at FROM messages WHERE id = ?",
            rusqlite::params![&m.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(read_at.is_some(), "read_at should remain set");
    assert!(
        claimed_at.is_some(),
        "claimed_at should NOT be cleared on a delivered message"
    );
}

// ---------------------------------------------------------------------------
// M-008 — release_all_claims_for un-claims all un-delivered claims
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m008_release_all_claims_for_releases_unread() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    for i in 0..3 {
        db.send_message("alice", "bob", None, MessageType::Request, &format!("m{i}"))
            .unwrap();
    }
    let claimed = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(claimed.len(), 3);

    db.release_all_claims_for("bob").unwrap();

    let again = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(again.len(), 3, "all 3 should be re-delivered");
}

// ---------------------------------------------------------------------------
// M-008b — release_all_claims_for leaves other agents' messages alone
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m008b_release_all_claims_for_isolates_by_agent() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    db.register_agent("carol", "p", "m", "proj").unwrap();

    db.send_message("alice", "bob", None, MessageType::Request, "for bob")
        .unwrap();
    db.send_message("alice", "carol", None, MessageType::Request, "for carol")
        .unwrap();

    // Claim for both recipients
    let _b = db.fetch_and_claim_messages("bob").unwrap();
    let _c = db.fetch_and_claim_messages("carol").unwrap();

    // Release only bob
    db.release_all_claims_for("bob").unwrap();

    // bob sees his message again
    let bob_again = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(bob_again.len(), 1);
    // carol still has the claim in place
    let carol_again = db.fetch_and_claim_messages("carol").unwrap();
    assert_eq!(carol_again.len(), 0);
}

// ---------------------------------------------------------------------------
// M-008c — release_all_claims_for does not unmark-read a delivered message
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m008c_release_all_claims_for_does_not_affect_read() {
    let (mut db, tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();
    let _ = db.fetch_and_claim_messages("bob").unwrap();
    db.mark_read(&m.id).unwrap();

    db.release_all_claims_for("bob").unwrap();

    let conn = raw_conn(&tmp);
    let read_at: Option<String> = conn
        .query_row(
            "SELECT read_at FROM messages WHERE id = ?",
            rusqlite::params![&m.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(read_at.is_some(), "read_at must remain set");
}

// ---------------------------------------------------------------------------
// M-009 — list_agents returns all, ordered by name
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m009_list_agents_returns_alphabetical() {
    let (db, _tmp) = fresh_db();
    db.register_agent("charlie", "p", "m", "proj").unwrap();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    let agents = db.list_agents().unwrap();
    let names: Vec<&str> = agents.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["alice", "bob", "charlie"]);
}

// ---------------------------------------------------------------------------
// M-009b — list_agents on empty DB returns empty vec
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m009b_list_agents_empty_returns_empty_vec() {
    let (db, _tmp) = fresh_db();
    let agents = db.list_agents().unwrap();
    assert!(agents.is_empty());
}

// ---------------------------------------------------------------------------
// M-010 — unregister_agent soft-deletes (state='unregistered'), preserves FKs
//
// Updated from the original "deletes row" semantics: HIGH-2 fix from the
// external review. Hard DELETE broke FKs once the agent had any message
// history. Soft-delete keeps the row, flips state to Unregistered, and the
// next register_agent flips it back to Active via ON CONFLICT.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m010_unregister_agent_soft_deletes() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    assert!(db.agent_exists("alice").unwrap());

    db.unregister_agent("alice").unwrap();

    // Row still present, but in 'unregistered' state.
    assert!(db.agent_exists("alice").unwrap());
    let agent = db.get_agent("alice").unwrap().expect("row preserved");
    assert_eq!(agent.state, agentbus_core::AgentState::Unregistered);

    // Re-register flips state back to Active without losing the row.
    db.register_agent("alice", "p2", "m2", "proj2").unwrap();
    let agent = db.get_agent("alice").unwrap().unwrap();
    assert_eq!(agent.state, agentbus_core::AgentState::Active);
    assert_eq!(agent.program, "p2");
}

// ---------------------------------------------------------------------------
// M-010c — mark_disconnected vs mark_all_active_as_disconnected
//
// Verifies the dead-connection-sweep behavior. After daemon restart, all
// "live" rows should flip to Disconnected without disturbing any rows
// that were explicitly Unregistered (which means the agent asked to be
// removed, not that they crashed).
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m010c_disconnected_sweep_skips_unregistered_rows() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    db.register_agent("carol", "p", "m", "proj").unwrap();

    // Carol explicitly Unregistered — sweep must NOT resurrect her.
    db.unregister_agent("carol").unwrap();

    let n = db.mark_all_active_as_disconnected().unwrap();
    assert_eq!(n, 2, "alice + bob should be swept; carol left alone");

    let alice = db.get_agent("alice").unwrap().unwrap();
    let bob = db.get_agent("bob").unwrap().unwrap();
    let carol = db.get_agent("carol").unwrap().unwrap();
    assert_eq!(alice.state, agentbus_core::AgentState::Disconnected);
    assert_eq!(bob.state, agentbus_core::AgentState::Disconnected);
    assert_eq!(carol.state, agentbus_core::AgentState::Unregistered);

    // Re-register flips Disconnected back to Active.
    db.register_agent("alice", "p", "m", "proj").unwrap();
    let alice = db.get_agent("alice").unwrap().unwrap();
    assert_eq!(alice.state, agentbus_core::AgentState::Active);
}

// ---------------------------------------------------------------------------
// M-010b — unregister doesn't break FKs even after sending messages
//
// HIGH-2 regression guard: if we ever go back to DELETE, this test fails
// because messages.from_agent FK to agents(name) blocks the delete.
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m010b_unregister_works_with_message_history() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    db.send_message(
        "alice",
        "bob",
        None,
        agentbus_core::MessageType::Request,
        "hi",
    )
    .unwrap();

    // Soft-delete must succeed even though messages reference 'alice'.
    db.unregister_agent("alice").unwrap();
    assert!(db.agent_exists("alice").unwrap());

    // Bob can still claim the message — FK on from_agent still resolves.
    let msgs = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].from, "alice");
}

// ---------------------------------------------------------------------------
// M-011 — fetch_and_claim ordering by created_at ASC (FIFO)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m011_fetch_and_claim_preserves_fifo_order() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    let bodies = ["first", "second", "third", "fourth", "fifth"];
    let mut inserted_ids = Vec::new();
    for body in bodies {
        let msg = db
            .send_message("alice", "bob", None, MessageType::Request, body)
            .unwrap();
        inserted_ids.push(msg.id);
        // Ensure created_at timestamps differ. RFC3339 with microsecond
        // resolution usually suffices but a small sleep guarantees ordering.
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let fetched = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(fetched.len(), 5);
    let fetched_ids: Vec<String> = fetched.iter().map(|m| m.id.clone()).collect();
    assert_eq!(
        fetched_ids, inserted_ids,
        "messages should be returned in insertion order"
    );
    let fetched_bodies: Vec<&str> = fetched.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(fetched_bodies, bodies);
}

// ---------------------------------------------------------------------------
// M-012 — claim_message returns true once, false the second time
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m012_claim_message_is_atomic_single_row() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();

    let first = db.claim_message(&m.id).unwrap();
    assert!(first, "first claim should succeed");
    let second = db.claim_message(&m.id).unwrap();
    assert!(!second, "second claim should fail");
}

// ---------------------------------------------------------------------------
// M-013 — mark_read on non-existent id is a no-op (no error)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m013_mark_read_noop_on_missing_id() {
    let (db, _tmp) = fresh_db();
    db.mark_read("nonexistent-id").expect("should be no-op");
}

// ---------------------------------------------------------------------------
// M-014 — Send to non-existent agent: FK constraint rejects insert
// (Issue 8 regression — the daemon relies on this from the DB layer)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m014_send_message_fk_violation_on_unknown_recipient() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    // "ghost" is not registered
    let res = db.send_message("alice", "ghost", None, MessageType::Request, "x");
    assert!(res.is_err(), "expected FK violation, got Ok");
}

// ---------------------------------------------------------------------------
// M-015 — Released claim comes back to next fetch (redelivery path)
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m015_released_claim_redelivered_to_next_fetch() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "payload")
        .unwrap();

    let claimed = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(claimed.len(), 1);

    // Simulated delivery failure
    db.release_claim(&m.id).unwrap();

    let again = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].id, m.id);
    assert_eq!(again[0].body, "payload");
}

// ---------------------------------------------------------------------------
// M-016 — mark_read after claim prevents redelivery
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m016_mark_read_prevents_redelivery() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let m = db
        .send_message("alice", "bob", None, MessageType::Request, "x")
        .unwrap();
    let _ = db.fetch_and_claim_messages("bob").unwrap();
    db.mark_read(&m.id).unwrap();

    // Even after release_all_claims_for, a read message is not redelivered
    db.release_all_claims_for("bob").unwrap();
    let again = db.fetch_and_claim_messages("bob").unwrap();
    assert!(again.is_empty(), "read message should not be redelivered");
}

// ---------------------------------------------------------------------------
// M-017 — Mixed claimed/unclaimed/read: only unclaimed+unread returned
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m017_fetch_only_returns_unclaimed_and_unread() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();

    let m1 = db
        .send_message("alice", "bob", None, MessageType::Request, "m1")
        .unwrap();
    let _m2 = db
        .send_message("alice", "bob", None, MessageType::Request, "m2")
        .unwrap();
    let m3 = db
        .send_message("alice", "bob", None, MessageType::Request, "m3")
        .unwrap();

    // Manually claim m1 only (one-shot claim)
    assert!(db.claim_message(&m1.id).unwrap());
    // Claim+mark_read m3
    assert!(db.claim_message(&m3.id).unwrap());
    db.mark_read(&m3.id).unwrap();

    // fetch_and_claim should only return m2
    let fetched = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].body, "m2");
}

// ---------------------------------------------------------------------------
// M-018 — get_agent returns Some for existing, None for missing
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m018_get_agent_existing_and_missing() {
    let (db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    let got = db.get_agent("alice").unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().name, "alice");

    let none = db.get_agent("nobody").unwrap();
    assert!(none.is_none());
}

// ---------------------------------------------------------------------------
// M-019 — agent_exists boolean
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m019_agent_exists_boolean() {
    let (db, _tmp) = fresh_db();
    assert!(!db.agent_exists("alice").unwrap());
    db.register_agent("alice", "p", "m", "proj").unwrap();
    assert!(db.agent_exists("alice").unwrap());
}

// ---------------------------------------------------------------------------
// M-020 — thread_id is persisted round-trip through send/fetch
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m020_send_and_fetch_preserves_thread_id() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let _m = db
        .send_message(
            "alice",
            "bob",
            Some("thread-42"),
            MessageType::Question,
            "q?",
        )
        .unwrap();

    let fetched = db.fetch_and_claim_messages("bob").unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].thread_id.as_deref(), Some("thread-42"));
    assert_eq!(fetched[0].msg_type, MessageType::Question);
}

// ---------------------------------------------------------------------------
// M-021 — Empty queue returns empty vec
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m021_fetch_empty_queue_returns_empty_vec() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("bob", "p", "m", "proj").unwrap();
    let fetched = db.fetch_and_claim_messages("bob").unwrap();
    assert!(fetched.is_empty());
}

// ---------------------------------------------------------------------------
// M-022 — Sending from one agent to self is allowed
// ---------------------------------------------------------------------------
#[test]
#[serial]
fn m022_send_to_self_allowed() {
    let (mut db, _tmp) = fresh_db();
    db.register_agent("alice", "p", "m", "proj").unwrap();
    let _ = db
        .send_message("alice", "alice", None, MessageType::Status, "note")
        .unwrap();
    let fetched = db.fetch_and_claim_messages("alice").unwrap();
    assert_eq!(fetched.len(), 1);
}

// ===========================================================================
// TODO: Tests requiring code refactor or more complex harness
// ===========================================================================
// - M-011 concurrent-claimers variant (two &mut Database pointing at the same
//   file): belongs in concurrency.rs and uses two processes or two connections
//   — skipped in this file because Database::init() always opens the canonical
//   ~/.agentbus/agentbus.db and we'd need to expose a multi-handle constructor.
//   Covered by J-004 in the concurrency suite (separate handles via
//   rusqlite::Connection::open on the same path).
// - Corrupt metadata JSON / corrupt msg_type: requires writing raw SQL behind
//   Database, then calling fetch. Can be added via raw_conn INSERT but is
//   covered conceptually by the U-* parse error tests.
