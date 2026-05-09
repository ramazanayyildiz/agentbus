//! Per-agent adapter profiles.
//!
//! Different agent CLIs have slightly different conventions for how to
//! deliver input. This module abstracts those differences behind a small
//! trait so the runner core stays generic.
//!
//! Each adapter answers three questions:
//!   1. **Match**: does this adapter handle a given program name?
//!   2. **Format**: how should an incoming bus message be turned into bytes
//!      to inject into the PTY?
//!   3. **Submit**: what bytes signal "send this input" to the agent?
//!
//! Phase 2 ships four adapters: Claude Code, Codex, Aider, and a Generic
//! fallback. Phase 3 will extend the trait with `is_prompt_ready(tail)` so
//! the runner can queue messages until the agent is back at its prompt.

use agentbus_core::Message;

use crate::inject;

/// Trait for per-agent injection behavior.
///
/// Trait objects (`Box<dyn Adapter>`) are used by the runner so the choice
/// can be made at runtime from the program name.
pub trait Adapter: Send + Sync {
    /// Stable name for logs and `--program` matching.
    fn name(&self) -> &'static str;

    /// Build the bytes to write into the PTY for `msg`. Implementations
    /// should always sanitize body content via `inject::sanitize` before
    /// embedding it.
    fn format_message(&self, msg: &Message) -> Vec<u8>;

    /// Minimum idle time (ms) the PTY output stream must show before this
    /// adapter is willing to inject a message. Phase 3 uses this as a
    /// universal "the agent isn't actively producing output right now"
    /// heuristic, in lieu of fragile prompt-regex detection.
    ///
    /// Returning 0 disables idle gating — the message is injected as soon
    /// as it arrives. Phase 1/2 behavior.
    fn idle_ms_before_inject(&self) -> u64 {
        0
    }
}

// --------------------------------------------------------------------------
// Adapters
// --------------------------------------------------------------------------

/// Generic fallback. Single-line envelope + CR. Phase 1 default.
pub struct GenericAdapter;

impl Adapter for GenericAdapter {
    fn name(&self) -> &'static str {
        "generic"
    }

    fn format_message(&self, msg: &Message) -> Vec<u8> {
        inject::format_for_injection(msg)
    }
}

/// Claude Code adapter.
///
/// Claude's TUI accepts pasted text well and submits on Enter. We use
/// bracketed paste so multi-word envelopes are visually grouped in the
/// input field rather than auto-completed against the slash-command list.
///
/// The bracketed-paste markers are inert if Claude doesn't recognize them
/// (the Phase 0 mock-agent test confirmed they pass through the PTY
/// transparently — worst case the body looks slightly noisier).
pub struct ClaudeAdapter;

impl Adapter for ClaudeAdapter {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn format_message(&self, msg: &Message) -> Vec<u8> {
        bracketed_paste_envelope(msg)
    }

    /// Claude Code streams output during a response. 750ms idle is enough
    /// to confirm the streaming has settled at the prompt without making
    /// the bus feel laggy.
    fn idle_ms_before_inject(&self) -> u64 {
        750
    }
}

/// Codex adapter. Behavior identical to Claude's for now (same input model:
/// single-line typed text, Enter to submit). Kept as a separate type so we
/// can diverge later — e.g. if Codex grows a `/paste` command.
pub struct CodexAdapter;

impl Adapter for CodexAdapter {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn format_message(&self, msg: &Message) -> Vec<u8> {
        bracketed_paste_envelope(msg)
    }

    fn idle_ms_before_inject(&self) -> u64 {
        750
    }
}

/// Aider adapter. Aider has a readline-style prompt so plain envelope + CR
/// is the cleanest delivery — bracketed paste would render as visible
/// markers in the buffer.
pub struct AiderAdapter;

impl Adapter for AiderAdapter {
    fn name(&self) -> &'static str {
        "aider"
    }

    fn format_message(&self, msg: &Message) -> Vec<u8> {
        inject::format_for_injection(msg)
    }

    fn idle_ms_before_inject(&self) -> u64 {
        500
    }
}

// --------------------------------------------------------------------------
// Selection
// --------------------------------------------------------------------------

/// Pick an adapter for a given program name. Falls back to `GenericAdapter`
/// when no built-in match applies.
///
/// Matching is case-insensitive and substring-based against the basename of
/// the program path so things like `/usr/local/bin/claude` and
/// `claude --dangerously-skip-permissions` both match the Claude adapter.
pub fn pick(program: &str) -> Box<dyn Adapter> {
    let p = program.to_ascii_lowercase();
    if p.contains("claude") {
        Box::new(ClaudeAdapter)
    } else if p.contains("codex") {
        Box::new(CodexAdapter)
    } else if p.contains("aider") {
        Box::new(AiderAdapter)
    } else {
        Box::new(GenericAdapter)
    }
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// Build a bracketed-paste-wrapped envelope. Only the body is wrapped — the
/// `[agentbus from=... ]` prefix sits inside the paste so the recipient sees
/// it as one block of pasted text.
fn bracketed_paste_envelope(msg: &Message) -> Vec<u8> {
    let safe_body = inject::sanitize(&msg.body);
    let safe_from = inject::sanitize(&msg.from);
    let thread = msg
        .thread_id
        .as_deref()
        .map(|t| format!(" thread={}", inject::sanitize(t)))
        .unwrap_or_default();
    let line = format!(
        "[agentbus from={} type={}{}] {}",
        safe_from,
        msg.msg_type.as_str(),
        thread,
        safe_body
    );
    let mut out = Vec::with_capacity(line.len() + 8);
    out.extend_from_slice(b"\x1b[200~");
    out.extend_from_slice(line.as_bytes());
    out.extend_from_slice(b"\x1b[201~");
    out.push(b'\r');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentbus_core::MessageType;

    fn msg(body: &str) -> Message {
        Message {
            id: "id".into(),
            from: "alice".into(),
            to: "bob".into(),
            thread_id: None,
            msg_type: MessageType::Request,
            body: body.into(),
            metadata: None,
            read_at: None,
            created_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn pick_resolves_known_programs() {
        assert_eq!(pick("claude").name(), "claude");
        assert_eq!(pick("/usr/local/bin/claude").name(), "claude");
        assert_eq!(pick("codex resume xyz --yolo").name(), "codex");
        assert_eq!(pick("aider").name(), "aider");
        assert_eq!(pick("vim").name(), "generic");
    }

    #[test]
    fn claude_adapter_uses_bracketed_paste() {
        let bytes = ClaudeAdapter.format_message(&msg("hello"));
        assert!(bytes.starts_with(b"\x1b[200~"));
        assert!(bytes.windows(6).any(|w| w == b"\x1b[201~"));
        assert_eq!(bytes.last(), Some(&b'\r'));
    }

    #[test]
    fn aider_adapter_does_not_use_bracketed_paste() {
        let bytes = AiderAdapter.format_message(&msg("hello"));
        assert!(!bytes.starts_with(b"\x1b[200~"));
        assert_eq!(bytes.last(), Some(&b'\r'));
    }

    #[test]
    fn generic_adapter_matches_default_format() {
        let bytes = GenericAdapter.format_message(&msg("hello"));
        let expected = inject::format_for_injection(&msg("hello"));
        assert_eq!(bytes, expected);
    }

    #[test]
    fn case_insensitive_matching() {
        assert_eq!(pick("CLAUDE").name(), "claude");
        assert_eq!(pick("Codex").name(), "codex");
    }

    #[test]
    fn idle_thresholds_match_per_adapter_expectations() {
        // Streaming TUIs need a beat to settle.
        assert_eq!(ClaudeAdapter.idle_ms_before_inject(), 750);
        assert_eq!(CodexAdapter.idle_ms_before_inject(), 750);
        // Aider's prompt is calmer.
        assert_eq!(AiderAdapter.idle_ms_before_inject(), 500);
        // Generic fallback: no gating, behave like Phase 1/2.
        assert_eq!(GenericAdapter.idle_ms_before_inject(), 0);
    }
}
