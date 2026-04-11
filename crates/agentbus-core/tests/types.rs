//! Tier 1 — Unit tests for agentbus-core types.
//!
//! Covers Test Plan cases U-001 .. U-018 and V-001 .. V-003.
//!
//! Pure tests, no DB, no FS.
//!
//! Notes on comparison:
//! - `BusRequest`, `BusResponse`, `Message`, and `BusError` do NOT derive
//!   `PartialEq` (they transitively wrap `rusqlite::Error` / `serde_json::Value`).
//!   Round-trip tests serialize both the original and the deserialized value
//!   to `serde_json::Value` and compare those.
//! - `AgentState` and `MessageType` DO derive `PartialEq`, so they compare
//!   directly.

use agentbus_core::{
    agentbus_dir, pid_file_path, socket_path, AgentState, BusError, BusRequest, BusResponse,
    Message, MessageType,
};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// U-001 — AgentState::parse — all valid states round-trip
// ---------------------------------------------------------------------------
#[test]
fn u001_agent_state_parse_all_valid_states_round_trip() {
    let cases = [
        ("active", AgentState::Active),
        ("working", AgentState::Working),
        ("waiting", AgentState::Waiting),
        ("done", AgentState::Done),
        ("error", AgentState::Error),
    ];
    for (s, expected) in cases {
        let parsed = AgentState::parse(s).unwrap_or_else(|e| panic!("parse({s}) failed: {e}"));
        assert_eq!(parsed, expected, "variant mismatch for {s}");
        assert_eq!(parsed.as_str(), s, "as_str round-trip mismatch for {s}");
    }
}

// ---------------------------------------------------------------------------
// U-002 — MessageType::parse — all valid types round-trip
// ---------------------------------------------------------------------------
#[test]
fn u002_message_type_parse_all_valid_types_round_trip() {
    let cases = [
        ("request", MessageType::Request),
        ("response", MessageType::Response),
        ("done", MessageType::Done),
        ("question", MessageType::Question),
        ("error", MessageType::Error),
        ("status", MessageType::Status),
    ];
    for (s, expected) in cases {
        let parsed = MessageType::parse(s).unwrap_or_else(|e| panic!("parse({s}) failed: {e}"));
        assert_eq!(parsed, expected);
        assert_eq!(parsed.as_str(), s);
    }
}

// ---------------------------------------------------------------------------
// U-003 — BusRequest::Register round-trip with type tag
// ---------------------------------------------------------------------------
#[test]
fn u003_bus_request_register_serde_round_trip() {
    let req = BusRequest::Register {
        name: "alice".to_string(),
        program: "claude-code".to_string(),
        model: "opus".to_string(),
        project: "agentbus".to_string(),
    };
    let json_str = serde_json::to_string(&req).expect("serialize");
    let json_val: Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(json_val["type"], "Register");
    assert_eq!(json_val["name"], "alice");
    assert_eq!(json_val["program"], "claude-code");
    assert_eq!(json_val["model"], "opus");
    assert_eq!(json_val["project"], "agentbus");

    let back: BusRequest = serde_json::from_str(&json_str).expect("deserialize");
    let back_val = serde_json::to_value(&back).unwrap();
    assert_eq!(back_val, json_val, "round-trip JSON mismatch");
}

// ---------------------------------------------------------------------------
// U-004 — BusRequest::Send with optional fields as None
// ---------------------------------------------------------------------------
#[test]
fn u004_bus_request_send_optional_fields_none() {
    let req = BusRequest::Send {
        from: None,
        to: "bob".to_string(),
        thread_id: None,
        msg_type: "request".to_string(),
        body: "hi".to_string(),
    };
    let s = serde_json::to_string(&req).unwrap();
    let v: Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["type"], "Send");
    assert_eq!(v["to"], "bob");
    assert_eq!(v["msg_type"], "request");
    assert_eq!(v["body"], "hi");
    // from and thread_id are serialized as null (Option without skip_serializing_if)
    assert!(v["from"].is_null(), "from should be null, got {}", v["from"]);
    assert!(
        v["thread_id"].is_null(),
        "thread_id should be null, got {}",
        v["thread_id"]
    );

    // Round-trip
    let back: BusRequest = serde_json::from_str(&s).unwrap();
    match back {
        BusRequest::Send {
            from,
            to,
            thread_id,
            msg_type,
            body,
        } => {
            assert!(from.is_none());
            assert!(thread_id.is_none());
            assert_eq!(to, "bob");
            assert_eq!(msg_type, "request");
            assert_eq!(body, "hi");
        }
        _ => panic!("expected Send variant after round-trip"),
    }
}

// ---------------------------------------------------------------------------
// U-005 — BusResponse::Message wraps a Message struct
// ---------------------------------------------------------------------------
#[test]
fn u005_bus_response_message_preserves_inner_message() {
    let msg = Message {
        id: "00000000-0000-0000-0000-000000000001".to_string(),
        from: "alice".to_string(),
        to: "bob".to_string(),
        thread_id: Some("t1".to_string()),
        msg_type: MessageType::Request,
        body: "hello".to_string(),
        metadata: Some(json!({"files": ["a.rs"]})),
        read_at: None,
        created_at: "2026-04-11T12:00:00+00:00".to_string(),
    };
    let resp = BusResponse::Message { message: msg };
    let s = serde_json::to_string(&resp).unwrap();
    let v: Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["type"], "Message");
    assert_eq!(v["message"]["from"], "alice");
    assert_eq!(v["message"]["to"], "bob");
    assert_eq!(v["message"]["body"], "hello");
    assert_eq!(v["message"]["msg_type"], "request");
    assert_eq!(v["message"]["metadata"]["files"][0], "a.rs");

    let back: BusResponse = serde_json::from_str(&s).unwrap();
    assert_eq!(serde_json::to_value(&back).unwrap(), v);
}

// ---------------------------------------------------------------------------
// U-006 — AgentState::parse returns typed error on unknown
// ---------------------------------------------------------------------------
#[test]
fn u006_agent_state_parse_unknown_returns_typed_error() {
    let err = AgentState::parse("zombie").unwrap_err();
    assert!(
        matches!(err, BusError::InvalidAgentState(ref s) if s == "zombie"),
        "expected InvalidAgentState(\"zombie\"), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// U-007 — MessageType::parse returns typed error on unknown
// ---------------------------------------------------------------------------
#[test]
fn u007_message_type_parse_unknown_returns_typed_error() {
    let err = MessageType::parse("gossip").unwrap_err();
    assert!(
        matches!(err, BusError::InvalidMessageType(ref s) if s == "gossip"),
        "expected InvalidMessageType(\"gossip\"), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// U-008 — AgentState::parse on empty string
// ---------------------------------------------------------------------------
#[test]
fn u008_agent_state_parse_empty_string() {
    let err = AgentState::parse("").unwrap_err();
    assert!(matches!(err, BusError::InvalidAgentState(ref s) if s.is_empty()));
}

// ---------------------------------------------------------------------------
// U-009 — MessageType::parse case sensitivity
// ---------------------------------------------------------------------------
#[test]
fn u009_message_type_parse_is_case_sensitive() {
    assert!(MessageType::parse("Request").is_err());
    assert!(MessageType::parse("REQUEST").is_err());
    // And the lowercase version still works
    assert!(MessageType::parse("request").is_ok());
}

// ---------------------------------------------------------------------------
// U-010 — BusRequest deserialization — unknown variant tag
// ---------------------------------------------------------------------------
#[test]
fn u010_bus_request_unknown_variant_is_parse_error() {
    let s = r#"{"type": "Explode"}"#;
    let res: Result<BusRequest, _> = serde_json::from_str(s);
    assert!(res.is_err(), "unknown variant should fail to deserialize");
}

// ---------------------------------------------------------------------------
// U-011 — BusRequest::Send with empty body round-trips as empty string
// ---------------------------------------------------------------------------
#[test]
fn u011_bus_request_send_empty_body_round_trips() {
    let req = BusRequest::Send {
        from: Some("alice".to_string()),
        to: "bob".to_string(),
        thread_id: None,
        msg_type: "request".to_string(),
        body: String::new(),
    };
    let s = serde_json::to_string(&req).unwrap();
    let back: BusRequest = serde_json::from_str(&s).unwrap();
    match back {
        BusRequest::Send { body, .. } => assert_eq!(body, ""),
        _ => panic!("expected Send"),
    }
}

// ---------------------------------------------------------------------------
// U-012 — BusRequest::Read with both wait and timeout_secs absent
// ---------------------------------------------------------------------------
#[test]
fn u012_bus_request_read_both_fields_absent() {
    let s = r#"{"type": "Read"}"#;
    let back: BusRequest = serde_json::from_str(s).expect("should deserialize");
    match back {
        BusRequest::Read { wait, timeout_secs } => {
            assert!(wait.is_none());
            assert!(timeout_secs.is_none());
        }
        _ => panic!("expected Read variant"),
    }
}

// ---------------------------------------------------------------------------
// U-013 — Message metadata JSON preserved
// ---------------------------------------------------------------------------
#[test]
fn u013_message_metadata_preserved_round_trip() {
    let msg = Message {
        id: "id-1".to_string(),
        from: "a".to_string(),
        to: "b".to_string(),
        thread_id: None,
        msg_type: MessageType::Status,
        body: "x".to_string(),
        metadata: Some(json!({"files": ["a.rs", "b.rs"], "n": 42})),
        read_at: None,
        created_at: "2026-01-01T00:00:00Z".to_string(),
    };
    let s = serde_json::to_string(&msg).unwrap();
    let back: Message = serde_json::from_str(&s).unwrap();
    assert_eq!(back.metadata.as_ref().unwrap()["n"], 42);
    assert_eq!(back.metadata.as_ref().unwrap()["files"][1], "b.rs");
}

// ---------------------------------------------------------------------------
// U-014 — socket_path ends with .agentbus/agentbus.sock
// ---------------------------------------------------------------------------
#[test]
fn u014_socket_path_ends_with_dot_agentbus_sock() {
    let p = socket_path().expect("socket_path");
    let s = p.to_string_lossy();
    assert!(
        s.ends_with(".agentbus/agentbus.sock"),
        "unexpected socket path: {s}"
    );
}

// ---------------------------------------------------------------------------
// U-015 — pid_file_path ends with .agentbus/agentbusd.pid
// ---------------------------------------------------------------------------
#[test]
fn u015_pid_file_path_ends_with_agentbusd_pid() {
    let p = pid_file_path().expect("pid_file_path");
    let s = p.to_string_lossy();
    assert!(s.ends_with(".agentbus/agentbusd.pid"), "unexpected pid path: {s}");
}

// ---------------------------------------------------------------------------
// U-016 — agentbus_dir does not require the path to exist
// ---------------------------------------------------------------------------
#[test]
fn u016_agentbus_dir_returns_path_without_io() {
    let p = agentbus_dir().expect("agentbus_dir");
    let s = p.to_string_lossy();
    assert!(s.ends_with(".agentbus"), "unexpected dir: {s}");
    // Existence is NOT required — helper is pure.
}

// ---------------------------------------------------------------------------
// U-017 — BusError Display contains the expected human-readable text
// ---------------------------------------------------------------------------
#[test]
fn u017_bus_error_display_agent_state() {
    let e = BusError::InvalidAgentState("x".to_string());
    let s = format!("{e}");
    assert!(s.contains("invalid agent state"));
    assert!(s.contains("x"));

    let e2 = BusError::InvalidMessageType("gossip".to_string());
    let s2 = format!("{e2}");
    assert!(s2.contains("invalid message type"));
    assert!(s2.contains("gossip"));
}

// ---------------------------------------------------------------------------
// U-018 — BusError::Db and BusError::Json From impls exist (compile check)
// ---------------------------------------------------------------------------
#[test]
fn u018_bus_error_from_impls_wrap_upstream() {
    // rusqlite::Error -> BusError::Db via `?`
    fn _wrap_db(e: rusqlite::Error) -> BusError {
        e.into()
    }
    // serde_json::Error -> BusError::Json via `?`
    fn _wrap_json(e: serde_json::Error) -> BusError {
        e.into()
    }
    let json_err = serde_json::from_str::<Value>("not json").unwrap_err();
    let wrapped: BusError = json_err.into();
    assert!(matches!(wrapped, BusError::Json(_)));
}

// ---------------------------------------------------------------------------
// V-001 — BusRequest::Send missing required `to`
// ---------------------------------------------------------------------------
#[test]
fn v001_bus_request_send_missing_to_is_error() {
    let s = r#"{"type": "Send", "body": "x", "msg_type": "request"}"#;
    let res: Result<BusRequest, _> = serde_json::from_str(s);
    assert!(res.is_err(), "missing `to` should fail");
    let msg = res.unwrap_err().to_string();
    assert!(
        msg.contains("to") || msg.contains("missing"),
        "unexpected error message: {msg}"
    );
}

// ---------------------------------------------------------------------------
// V-002 — BusRequest::Register missing required `name`
// ---------------------------------------------------------------------------
#[test]
fn v002_bus_request_register_missing_name_is_error() {
    let s = r#"{"type": "Register", "program": "x", "model": "y", "project": "z"}"#;
    let res: Result<BusRequest, _> = serde_json::from_str(s);
    assert!(res.is_err());
}

// ---------------------------------------------------------------------------
// V-003 — BusResponse with unknown tag is a parse error
// ---------------------------------------------------------------------------
#[test]
fn v003_bus_response_unknown_tag_is_parse_error() {
    let s = r#"{"type": "Hallo", "data": {}}"#;
    let res: Result<BusResponse, _> = serde_json::from_str(s);
    assert!(res.is_err());
}
