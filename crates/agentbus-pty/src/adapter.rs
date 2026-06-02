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
use crate::strip;

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

    /// Is the agent's input prompt visible / ready at the bottom of the
    /// screen?
    ///
    /// `screen_tail` is the last ~N lines of the rendered PTY output with ANSI
    /// escapes stripped but box-drawing / prompt glyphs preserved (see
    /// `crate::strip::strip_ansi_preserve_box`). This is a layered improvement
    /// on top of the idle gate: the runner injects only when BOTH the idle
    /// gate is satisfied AND this returns true (with a hard 30s safety cap so a
    /// never-ready agent still eventually injects — see `runner.rs`).
    ///
    /// Conservative default: `true`. An adapter with no reliable prompt
    /// pattern falls back to the pure idle gate, exactly matching pre-Phase-3
    /// behavior. Ported per-agent from coder/agentapi's
    /// `lib/msgfmt/agent_readiness.go` + `lib/msgfmt/message_box.go`.
    fn is_prompt_ready(&self, _screen_tail: &str) -> bool {
        true
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

    /// Claude shows a `>` / `❯` prompt (optionally inside a `─────` box) at the
    /// bottom once it's ready for input. Ported from agentapi's
    /// `findGreaterThanMessageBox` + `findGenericSlimMessageBox`
    /// (lib/msgfmt/message_box.go), used for AgentTypeClaude in
    /// agent_readiness.go.
    fn is_prompt_ready(&self, screen_tail: &str) -> bool {
        strip::has_greater_than_or_slim_box(screen_tail)
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

    /// Codex shows a `›` prompt marker near the bottom when ready. Ported from
    /// agentapi's `removeCodexMessageBox` (lib/msgfmt/message_box.go), used for
    /// AgentTypeCodex in agent_readiness.go.
    fn is_prompt_ready(&self, screen_tail: &str) -> bool {
        strip::has_codex_box(screen_tail)
    }
}

/// opencode adapter. Modern Bun TUI; verified in real-world test that
/// envelope renders inside its input field. We use bracketed paste so the
/// envelope is grouped as one block instead of being interpreted as
/// individual keystrokes (which would trip the slash-command autocomplete).
pub struct OpencodeAdapter;

impl Adapter for OpencodeAdapter {
    fn name(&self) -> &'static str {
        "opencode"
    }

    fn format_message(&self, msg: &Message) -> Vec<u8> {
        bracketed_paste_envelope(msg)
    }

    fn idle_ms_before_inject(&self) -> u64 {
        750
    }

    /// opencode renders a `╹▀▀▀▀…` footer (and a `❯` prompt) once its TUI is
    /// interactive. Ported from agentapi's `removeOpencodeMessageBox`
    /// (lib/msgfmt/message_box.go), used for AgentTypeOpencode in
    /// agent_readiness.go.
    fn is_prompt_ready(&self, screen_tail: &str) -> bool {
        strip::has_opencode_box(screen_tail)
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
    } else if p.contains("opencode") {
        // Order matters: must check "opencode" before any partial match
        // for "code" (none today, but defensive). Check before generic.
        Box::new(OpencodeAdapter)
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
        assert_eq!(pick("opencode").name(), "opencode");
        assert_eq!(pick("/usr/local/bin/opencode --tui").name(), "opencode");
        assert_eq!(pick("vim").name(), "generic");
    }

    #[test]
    fn opencode_uses_bracketed_paste_and_idle_gating() {
        let bytes = OpencodeAdapter.format_message(&msg("hi"));
        assert!(bytes.starts_with(b"\x1b[200~"));
        assert!(bytes.windows(6).any(|w| w == b"\x1b[201~"));
        assert_eq!(bytes.last(), Some(&b'\r'));
        assert_eq!(OpencodeAdapter.idle_ms_before_inject(), 750);
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

    // --- is_prompt_ready (Phase 3 readiness detection) -------------------

    /// Realistic Claude input-box screen: a settled prompt at the bottom.
    const CLAUDE_READY: &str = "\
● I've finished the refactor.

────────────────────────────────────────
 >
────────────────────────────────────────
  ? for shortcuts";

    /// Realistic Codex screen with its `›` prompt marker near the bottom.
    const CODEX_READY: &str = "\
codex ran tests, all green.

›
  send a message";

    /// Mid-spinner "thinking" screen — agent is busy, NOT ready.
    const THINKING: &str = "\
✻ Crunching the request…
  streaming tokens here
  still working on it";

    #[test]
    fn claude_prompt_ready_detects_input_box() {
        assert!(ClaudeAdapter.is_prompt_ready(CLAUDE_READY));
    }

    #[test]
    fn claude_prompt_not_ready_mid_spinner() {
        assert!(!ClaudeAdapter.is_prompt_ready(THINKING));
    }

    #[test]
    fn claude_not_ready_on_streamed_redirect() {
        // A `>` inside streamed output (shell redirect) must NOT be read as a
        // ready prompt — the hardened, recurring-gate behavior.
        let screen = "running shell command\n  $ cat > /tmp/out.txt\nwriting output";
        assert!(!ClaudeAdapter.is_prompt_ready(screen));
    }

    #[test]
    fn codex_prompt_ready_detects_box() {
        assert!(CodexAdapter.is_prompt_ready(CODEX_READY));
    }

    #[test]
    fn codex_prompt_not_ready_mid_spinner() {
        assert!(!CodexAdapter.is_prompt_ready(THINKING));
    }

    #[test]
    fn opencode_prompt_ready_detects_footer() {
        let screen = "┃  Build  Anthropic Claude Sonnet 4\n\
            ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n\
            tab switch agent  ctrl+p commands";
        assert!(OpencodeAdapter.is_prompt_ready(screen));
    }

    #[test]
    fn opencode_prompt_not_ready_mid_spinner() {
        assert!(!OpencodeAdapter.is_prompt_ready(THINKING));
    }

    #[test]
    fn generic_and_aider_default_ready_true() {
        // Conservative default: no pattern -> always ready -> pure idle gate.
        assert!(GenericAdapter.is_prompt_ready(THINKING));
        assert!(AiderAdapter.is_prompt_ready(THINKING));
        assert!(GenericAdapter.is_prompt_ready(""));
    }
}
