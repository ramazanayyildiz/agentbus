use chrono::Utc;
use rusqlite::{Connection, Result as SqliteResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

/// Errors returned by the agentbus core.
#[derive(Debug, Error)]
pub enum BusError {
    #[error("invalid agent state: {0}")]
    InvalidAgentState(String),
    #[error("invalid message type: {0}")]
    InvalidMessageType(String),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Agent state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentState {
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "working")]
    Working,
    #[serde(rename = "waiting")]
    Waiting,
    #[serde(rename = "done")]
    Done,
    #[serde(rename = "error")]
    Error,
}

impl AgentState {
    pub fn as_str(&self) -> &str {
        match self {
            AgentState::Active => "active",
            AgentState::Working => "working",
            AgentState::Waiting => "waiting",
            AgentState::Done => "done",
            AgentState::Error => "error",
        }
    }

    /// Parse from the canonical string representation.
    /// Returns `BusError::InvalidAgentState` on unknown input — callers should
    /// surface the error instead of silently defaulting so that protocol drift
    /// and DB corruption are caught early (Issue 9).
    pub fn parse(s: &str) -> Result<Self, BusError> {
        match s {
            "active" => Ok(AgentState::Active),
            "working" => Ok(AgentState::Working),
            "waiting" => Ok(AgentState::Waiting),
            "done" => Ok(AgentState::Done),
            "error" => Ok(AgentState::Error),
            other => Err(BusError::InvalidAgentState(other.to_string())),
        }
    }
}

/// Message type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    #[serde(rename = "request")]
    Request,
    #[serde(rename = "response")]
    Response,
    #[serde(rename = "done")]
    Done,
    #[serde(rename = "question")]
    Question,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "status")]
    Status,
}

impl MessageType {
    pub fn as_str(&self) -> &str {
        match self {
            MessageType::Request => "request",
            MessageType::Response => "response",
            MessageType::Done => "done",
            MessageType::Question => "question",
            MessageType::Error => "error",
            MessageType::Status => "status",
        }
    }

    /// Parse from the canonical string representation.
    /// Returns `BusError::InvalidMessageType` on unknown input (Issue 9).
    pub fn parse(s: &str) -> Result<Self, BusError> {
        match s {
            "request" => Ok(MessageType::Request),
            "response" => Ok(MessageType::Response),
            "done" => Ok(MessageType::Done),
            "question" => Ok(MessageType::Question),
            "error" => Ok(MessageType::Error),
            "status" => Ok(MessageType::Status),
            other => Err(BusError::InvalidMessageType(other.to_string())),
        }
    }
}

/// Agent record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub program: String,
    pub model: String,
    pub project: String,
    pub state: AgentState,
    pub pid: Option<i32>,
    pub registered_at: String,
}

/// Message record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub from: String,
    pub to: String,
    pub thread_id: Option<String>,
    pub msg_type: MessageType,
    pub body: String,
    pub metadata: Option<serde_json::Value>,
    pub read_at: Option<String>,
    pub created_at: String,
}

/// Client → Daemon protocol
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BusRequest {
    Register {
        name: String,
        program: String,
        model: String,
        project: String,
    },
    Unregister {
        name: String,
    },
    List,
    Send {
        from: Option<String>,
        to: String,
        thread_id: Option<String>,
        msg_type: String,
        body: String,
    },
    Read {
        wait: Option<bool>,
        timeout_secs: Option<u64>,
    },
    Close,
    Status,
}

/// Daemon → Client protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BusResponse {
    Ok {
        data: serde_json::Value,
    },
    Error {
        message: String,
    },
    Message {
        message: Message,
    },
}

/// Database layer for SQLite
pub struct Database {
    conn: Connection,
}

impl Database {
    /// Initialize database, create ~/.agentbus directory and db file.
    ///
    /// Honours the `AGENTBUS_DIR` env var if set (used by tests for full
    /// per-process isolation); otherwise falls back to `~/.agentbus`.
    pub fn init() -> anyhow::Result<Self> {
        let db_dir = agentbus_dir()?;

        fs::create_dir_all(&db_dir)?;

        let db_path = db_dir.join("bus.db");
        let conn = Connection::open(&db_path)?;

        // Enable foreign keys (declared but not enforced by default — Issue 8)
        // and turn on WAL for concurrent readers + a writer. synchronous=NORMAL
        // is the recommended pairing for WAL.
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = NORMAL;",
        )?;

        let db = Database { conn };
        db.create_tables()?;
        db.migrate()?;
        Ok(db)
    }

    fn create_tables(&self) -> SqliteResult<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                program TEXT NOT NULL,
                model TEXT NOT NULL,
                project TEXT NOT NULL,
                state TEXT NOT NULL,
                pid INTEGER,
                registered_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                from_agent TEXT NOT NULL,
                to_agent TEXT NOT NULL,
                thread_id TEXT,
                msg_type TEXT NOT NULL,
                body TEXT NOT NULL,
                metadata TEXT,
                read_at TEXT,
                claimed_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(from_agent) REFERENCES agents(name),
                FOREIGN KEY(to_agent) REFERENCES agents(name)
            );

            CREATE INDEX IF NOT EXISTS idx_messages_to_agent
                ON messages(to_agent, read_at, claimed_at);
            "#,
        )?;
        Ok(())
    }

    /// Add columns introduced after v0 schema. `ALTER TABLE ADD COLUMN` with
    /// a guard query is the simplest forward-compatible migration path.
    fn migrate(&self) -> SqliteResult<()> {
        let has_claimed_at: bool = self
            .conn
            .prepare("SELECT 1 FROM pragma_table_info('messages') WHERE name = 'claimed_at'")?
            .exists([])?;
        if !has_claimed_at {
            self.conn
                .execute_batch("ALTER TABLE messages ADD COLUMN claimed_at TEXT;")?;
        }
        Ok(())
    }

    /// Register (or re-register) an agent. Uses `INSERT ... ON CONFLICT` so a
    /// second Register for the same name refreshes metadata and returns the
    /// real persisted row — no fabricated records (Issue 4, core half).
    pub fn register_agent(
        &self,
        name: &str,
        program: &str,
        model: &str,
        project: &str,
    ) -> anyhow::Result<Agent> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let state = AgentState::Active;

        self.conn.execute(
            "INSERT INTO agents (id, name, program, model, project, state, registered_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(name) DO UPDATE SET
                 program = excluded.program,
                 model = excluded.model,
                 project = excluded.project,
                 state = excluded.state",
            rusqlite::params![&id, name, program, model, project, state.as_str(), &now],
        )?;

        // Read back the canonical row (id/registered_at may be the original
        // values if the row already existed).
        self.get_agent(name)?
            .ok_or_else(|| anyhow::anyhow!(BusError::AgentNotFound(name.to_string())))
    }

    /// Fetch an agent by name, returning `None` if absent.
    pub fn get_agent(&self, name: &str) -> anyhow::Result<Option<Agent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, program, model, project, state, pid, registered_at
             FROM agents WHERE name = ? LIMIT 1",
        )?;
        let mut rows = stmt.query(rusqlite::params![name])?;
        if let Some(row) = rows.next()? {
            let state_str: String = row.get(5)?;
            let state = AgentState::parse(&state_str)
                .map_err(|e| anyhow::anyhow!(e))?;
            Ok(Some(Agent {
                id: row.get(0)?,
                name: row.get(1)?,
                program: row.get(2)?,
                model: row.get(3)?,
                project: row.get(4)?,
                state,
                pid: row.get(6)?,
                registered_at: row.get(7)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn unregister_agent(&self, name: &str) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM agents WHERE name = ?", rusqlite::params![name])?;
        Ok(())
    }

    pub fn list_agents(&self) -> anyhow::Result<Vec<Agent>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, program, model, project, state, pid, registered_at
             FROM agents ORDER BY name",
        )?;

        let rows = stmt.query_map([], |row| {
            // Collect raw fields; state parsing happens outside so we can
            // surface a typed error instead of swallowing it (Issue 9).
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let program: String = row.get(2)?;
            let model: String = row.get(3)?;
            let project: String = row.get(4)?;
            let state_str: String = row.get(5)?;
            let pid: Option<i32> = row.get(6)?;
            let registered_at: String = row.get(7)?;
            Ok((
                id,
                name,
                program,
                model,
                project,
                state_str,
                pid,
                registered_at,
            ))
        })?;

        let mut agents = Vec::new();
        for row in rows {
            let (id, name, program, model, project, state_str, pid, registered_at) = row?;
            let state = AgentState::parse(&state_str).map_err(|e| anyhow::anyhow!(e))?;
            agents.push(Agent {
                id,
                name,
                program,
                model,
                project,
                state,
                pid,
                registered_at,
            });
        }
        Ok(agents)
    }

    pub fn send_message(
        &self,
        from: &str,
        to: &str,
        thread_id: Option<&str>,
        msg_type: MessageType,
        body: &str,
    ) -> anyhow::Result<Message> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        self.conn.execute(
            "INSERT INTO messages (id, from_agent, to_agent, thread_id, msg_type, body, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            rusqlite::params![
                &id,
                from,
                to,
                thread_id,
                msg_type.as_str(),
                body,
                &now
            ],
        )?;

        Ok(Message {
            id,
            from: from.to_string(),
            to: to.to_string(),
            thread_id: thread_id.map(|s| s.to_string()),
            msg_type,
            body: body.to_string(),
            metadata: None,
            read_at: None,
            created_at: now,
        })
    }

    /// Atomically claim all unread, unclaimed messages for `to_agent`.
    ///
    /// Uses a single `BEGIN IMMEDIATE` transaction to SELECT the pending
    /// messages, stamp them with `claimed_at = now`, and return them. Two
    /// concurrent callers cannot pick up the same row (Issue 2).
    ///
    /// Claimed messages are NOT yet considered delivered. The caller MUST
    /// call either `mark_read` (after a successful socket write) or
    /// `release_claim` (if delivery failed) — this is what gives the bus
    /// at-least-once semantics without duplicates under normal operation.
    pub fn fetch_and_claim_messages(&mut self, to_agent: &str) -> anyhow::Result<Vec<Message>> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let now = Utc::now().to_rfc3339();

        // Collect the candidate rows first so we can release the statement
        // before running UPDATE in the same transaction.
        let mut messages: Vec<Message> = {
            let mut stmt = tx.prepare(
                "SELECT id, from_agent, to_agent, thread_id, msg_type, body, metadata, read_at, created_at
                 FROM messages
                 WHERE to_agent = ?1 AND read_at IS NULL AND claimed_at IS NULL
                 ORDER BY created_at ASC",
            )?;
            let rows = stmt.query_map(rusqlite::params![to_agent], |row| {
                let id: String = row.get(0)?;
                let from: String = row.get(1)?;
                let to: String = row.get(2)?;
                let thread_id: Option<String> = row.get(3)?;
                let msg_type_str: String = row.get(4)?;
                let body: String = row.get(5)?;
                let metadata_raw: Option<String> = row.get(6)?;
                let read_at: Option<String> = row.get(7)?;
                let created_at: String = row.get(8)?;
                Ok((
                    id,
                    from,
                    to,
                    thread_id,
                    msg_type_str,
                    body,
                    metadata_raw,
                    read_at,
                    created_at,
                ))
            })?;

            let mut out = Vec::new();
            for row in rows {
                let (id, from, to, thread_id, msg_type_str, body, metadata_raw, read_at, created_at) =
                    row?;
                let msg_type =
                    MessageType::parse(&msg_type_str).map_err(|e| anyhow::anyhow!(e))?;
                let metadata = match metadata_raw {
                    Some(s) => Some(serde_json::from_str(&s)?),
                    None => None,
                };
                out.push(Message {
                    id,
                    from,
                    to,
                    thread_id,
                    msg_type,
                    body,
                    metadata,
                    read_at,
                    created_at,
                });
            }
            out
        };

        // Stamp claimed_at on each selected row. Scoping in its own block
        // so `stmt` drops before we commit.
        {
            let mut upd = tx.prepare(
                "UPDATE messages SET claimed_at = ?1 WHERE id = ?2 AND claimed_at IS NULL",
            )?;
            let mut kept = Vec::with_capacity(messages.len());
            for msg in messages.drain(..) {
                let affected = upd.execute(rusqlite::params![&now, &msg.id])?;
                if affected == 1 {
                    kept.push(msg);
                }
                // If affected == 0 another transaction beat us to it — skip.
            }
            messages = kept;
        }

        tx.commit()?;
        Ok(messages)
    }

    /// Attempt to atomically claim a single message by id. Returns true if
    /// the row was unclaimed and is now marked `claimed_at = now`. Used by
    /// the daemon's Send path to hand a freshly-inserted message to the
    /// recipient's push channel without letting a concurrent `Read` also
    /// pick it up from the DB (Issue 2 / Issue 3).
    pub fn claim_message(&self, message_id: &str) -> anyhow::Result<bool> {
        let now = Utc::now().to_rfc3339();
        let affected = self.conn.execute(
            "UPDATE messages SET claimed_at = ?1
             WHERE id = ?2 AND read_at IS NULL AND claimed_at IS NULL",
            rusqlite::params![&now, message_id],
        )?;
        Ok(affected == 1)
    }

    /// Mark a message as delivered. Called ONLY after `write_all()` to the
    /// target socket has succeeded (Issue 1).
    pub fn mark_read(&self, message_id: &str) -> anyhow::Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "UPDATE messages SET read_at = ? WHERE id = ?",
            rusqlite::params![&now, message_id],
        )?;
        Ok(())
    }

    /// Release a claim so another delivery attempt can pick the message up.
    /// Called when a claimed message could not be written to the client — e.g.
    /// the socket closed mid-flight. This is the redelivery half of
    /// at-least-once (Issue 1 / Issue 2).
    pub fn release_claim(&self, message_id: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE messages SET claimed_at = NULL WHERE id = ? AND read_at IS NULL",
            rusqlite::params![message_id],
        )?;
        Ok(())
    }

    /// Release every claim for the given agent — used on client disconnect
    /// so any mid-flight claimed-but-unread messages become redeliverable.
    pub fn release_all_claims_for(&self, to_agent: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE messages SET claimed_at = NULL
             WHERE to_agent = ? AND read_at IS NULL AND claimed_at IS NOT NULL",
            rusqlite::params![to_agent],
        )?;
        Ok(())
    }

    pub fn agent_exists(&self, name: &str) -> anyhow::Result<bool> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM agents WHERE name = ? LIMIT 1")?;
        let exists = stmt.exists(rusqlite::params![name])?;
        Ok(exists)
    }
}

/// Helper to get agentbus directory path.
///
/// Respects `AGENTBUS_DIR` if set so integration and unit tests can point each
/// spawned daemon or in-process `Database` at an isolated tempdir without
/// mutating process-global `HOME`. Falls back to `~/.agentbus`.
pub fn agentbus_dir() -> anyhow::Result<PathBuf> {
    if let Ok(override_dir) = std::env::var("AGENTBUS_DIR") {
        if !override_dir.is_empty() {
            return Ok(PathBuf::from(override_dir));
        }
    }
    let dir = dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?
        .join(".agentbus");
    Ok(dir)
}

/// Get socket path
pub fn socket_path() -> anyhow::Result<PathBuf> {
    Ok(agentbus_dir()?.join("agentbus.sock"))
}

/// Get PID file path
pub fn pid_file_path() -> anyhow::Result<PathBuf> {
    Ok(agentbus_dir()?.join("agentbusd.pid"))
}
