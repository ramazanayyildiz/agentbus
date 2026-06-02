//! Terminal-chrome stripping helpers.
//!
//! Two distinct, *opposite* operations live near each other in this crate and
//! must not be confused (a lesson from porting agentapi):
//!
//!   - `strip_ansi_preserve_box` (this module) — removes ANSI/VT escape
//!     sequences but **preserves** box-drawing characters. Used as the input
//!     to readiness detection (`Adapter::is_prompt_ready`), where the box and
//!     prompt glyphs (`─ │ ❯ > ›`) are the *signal* we look for.
//!
//!   - `strip_tui_chrome` (this module) — removes ANSI escapes **and** the
//!     box-drawing chrome, spinner frames, and input-box markers, yielding
//!     clean human-readable message text. Used by the room hub / renderer to
//!     turn raw PTY/transcript bytes into a message body.
//!
//! Both are pure functions with no agent-specific state so they're trivially
//! testable.
//!
//! Ported from coder/agentapi:
//!   - box-drawing / spinner / input-box markers: lib/msgfmt/message_box.go
//!     (`containsHorizontalBorder`, `removeOpencodeMessageBox`,
//!     `removeAmpMessageBox`) and the spinner glyphs documented in
//!     agentapi's screen tracker. The chrome set below is a superset that
//!     covers claude/codex/amp/opencode/gemini/cursor box styles.

use std::sync::OnceLock;

use regex::Regex;

/// Box-drawing and TUI-frame glyphs that agentapi's message-box detection
/// keys on. Kept as one set so both readiness (which *looks* for these) and
/// chrome stripping (which *removes* these) agree on what counts as chrome.
///
/// Source: the literals in lib/msgfmt/message_box.go (`─ ╌ │ ❯ ╭ ╮ ╰ ╯ ┃ ╹ ▀`)
/// plus the common box-drawing block so claude/codex/amp/opencode/gemini/cursor
/// frames are all covered.
const BOX_DRAWING_CHARS: &[char] = &[
    '─', '│', '╭', '╮', '╰', '╯', '┌', '┐', '└', '┘', '├', '┤', '┬', '┴', '┼', '━', '┃', '┏', '┓',
    '┗', '┛', '┣', '┫', '┳', '┻', '╋', '═', '║', '╔', '╗', '╚', '╝', '╠', '╣', '╦', '╩', '╬', '╌',
    '╍', '╎', '╏', '▀', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█', '╹', '╻', '╺', '╸',
];

/// Spinner / "thinking" animation frames used by agent TUIs (claude, codex).
/// Not part of agentapi's readiness check — included here only for chrome
/// stripping so a captured mid-spinner frame doesn't leak a stray glyph into
/// rendered message text.
const SPINNER_CHARS: &[char] = &[
    '✻', '✽', '✶', '✳', '✢', '·', '⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏', '◐', '◓', '◑',
    '◒',
];

/// Compiled once: matches CSI / OSC / two-char ESC sequences and the bracketed
/// paste markers. We deliberately do NOT strip box-drawing chars here — that's
/// the whole point of "preserve box".
fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // - \x1b\[ ... <final byte>   CSI (colors, cursor moves, \x1b[200~ etc.)
        // - \x1b\] ... (BEL | ST)     OSC (window title, etc.)
        // - \x1b followed by a single non-[ char   two-byte escapes
        Regex::new(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]")
            .expect("static ANSI regex is valid")
    })
}

/// Remove ANSI/VT escape sequences and the lone DEL/C0 noise but **preserve**
/// box-drawing and prompt glyphs. This is the input that
/// `Adapter::is_prompt_ready` consumes — the box/prompt characters are the
/// readiness signal, so stripping them here would defeat detection.
///
/// Faithful-but-pragmatic note: agentapi feeds its readiness check a screen
/// rendered by a full VT emulator. AgentBus has only the raw byte stream, so
/// this regex-based cleanup is an *approximation* of that rendered screen. It
/// is acceptable because the readiness gate is conservative (it only ever
/// makes injection wait, never forces it) and the runner's 30s safety cap
/// guarantees forward progress even if detection is imperfect.
pub fn strip_ansi_preserve_box(raw: &str) -> String {
    let no_ansi = ansi_re().replace_all(raw, "");
    // Drop residual C0 controls except \t \n \r (those structure the screen).
    no_ansi
        .chars()
        .filter(|c| {
            let cp = *c as u32;
            *c == '\t' || *c == '\n' || *c == '\r' || cp >= 0x20 && cp != 0x7f
        })
        .collect()
}

/// Turn raw PTY/transcript bytes into clean, human-readable message text.
///
/// Pipeline (generic; covers claude/codex/amp/opencode/gemini/cursor):
///   1. strip ANSI/VT escapes (reusing the readiness ANSI regex)
///   2. drop box-drawing chrome chars and spinner frames
///   3. drop input-box marker lines (`❯`, leading `>` / `›`, and dashed
///      separators like `------` or `───────`)
///   4. collapse the now-blank lines and trim leading/trailing empties
///      (ports `trimEmptyLines` from lib/msgfmt/msgfmt.go)
///
/// Box-drawing literals and the dashed-separator idea are ported from
/// `containsHorizontalBorder` in lib/msgfmt/message_box.go; `trimEmptyLines`
/// from lib/msgfmt/msgfmt.go.
pub fn strip_tui_chrome(raw: &str) -> String {
    let no_ansi = ansi_re().replace_all(raw, "");

    let mut cleaned_lines: Vec<String> = Vec::new();
    for line in no_ansi.split('\n') {
        // Remove box-drawing + spinner glyphs from within the line.
        let mut s: String = line
            .chars()
            .filter(|c| !BOX_DRAWING_CHARS.contains(c) && !SPINNER_CHARS.contains(c))
            .collect();

        let trimmed = s.trim();

        // Drop pure input-box marker / separator lines entirely.
        if is_input_box_marker_line(trimmed) {
            s = String::new();
        } else {
            // Keep content but normalize: strip a leading prompt marker
            // (`>` / `›` / `❯`) that agents echo in front of the input line.
            s = strip_leading_prompt_marker(trimmed).to_string();
        }

        cleaned_lines.push(s);
    }

    trim_empty_lines(&cleaned_lines)
}

/// A line that is *only* TUI chrome with no message content: a dashed
/// separator, a bare prompt marker, or an empty line. Ported from the
/// `containsHorizontalBorder` heuristic (lib/msgfmt/message_box.go) generalized
/// to ASCII `-` separators as well.
fn is_input_box_marker_line(trimmed: &str) -> bool {
    if trimmed.is_empty() {
        return true;
    }
    // Bare prompt markers.
    if trimmed == ">" || trimmed == "›" || trimmed == "❯" || trimmed == "|" || trimmed == "│" {
        return true;
    }
    // Dashed/box separators: a run of `-` (>= 5) or box-drawing chars only.
    let dash_only = trimmed.len() >= 5 && trimmed.chars().all(|c| c == '-');
    if dash_only {
        return true;
    }
    true_if_all_box(trimmed)
}

fn true_if_all_box(trimmed: &str) -> bool {
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|c| c == ' ' || BOX_DRAWING_CHARS.contains(&c))
}

/// Strip a single leading prompt marker (`>`, `›`, `❯`) plus following space.
fn strip_leading_prompt_marker(s: &str) -> &str {
    for m in ['>', '›', '❯'] {
        if let Some(rest) = s.strip_prefix(m) {
            return rest.trim_start();
        }
    }
    s
}

/// Trim leading and trailing all-whitespace lines, preserving interior blank
/// lines. Ported from `trimEmptyLines` in lib/msgfmt/msgfmt.go.
fn trim_empty_lines(lines: &[String]) -> String {
    let first = lines
        .iter()
        .position(|l| !l.trim().is_empty())
        .unwrap_or(lines.len());
    let last = lines
        .iter()
        .rposition(|l| !l.trim().is_empty())
        .map(|i| i + 1)
        .unwrap_or(0);
    if first >= last {
        return String::new();
    }
    lines[first..last].join("\n")
}

// --------------------------------------------------------------------------
// Readiness helpers shared by adapters.
// --------------------------------------------------------------------------

/// Detect a Claude/Goose/Aider-style "greater-than" or slim input box near the
/// bottom of `screen_tail`. Ported from `findGreaterThanMessageBox` and
/// `findGenericSlimMessageBox` in lib/msgfmt/message_box.go.
///
/// agentapi scans the last 6 lines for a `>` (optionally fronted by a
/// horizontal border) or a `border / | / border` sandwich. We replicate that
/// against the last ~8 lines of the (ANSI-stripped) tail.
///
/// HARDENING vs agentapi: agentapi uses `strings.Contains(">")` because it
/// runs readiness *once* before the initial prompt. AgentBus uses this as a
/// *recurring per-message* gate, so a bare `strings.Contains(">")` would fire
/// on the agent's own streamed output (markdown quotes, `cat > file`, git
/// diffs, npm `> pkg@1.0.0`, boot banners) and inject mid-response. We instead
/// require the prompt marker to be the FIRST meaningful glyph on the line
/// (after optional leading border chars / spaces) — that's what a real input
/// prompt looks like, and it still matches ` >`, `> placeholder`, and the
/// bordered `│ > │` prompt.
pub(crate) fn has_greater_than_or_slim_box(screen_tail: &str) -> bool {
    let lines: Vec<&str> = screen_tail.lines().collect();
    let n = lines.len();
    if n == 0 {
        return false;
    }
    let start = n.saturating_sub(8);

    // findGreaterThanMessageBox (hardened): a line whose first meaningful glyph
    // is a `>` / `❯` prompt marker means the input prompt is showing.
    for line in &lines[start..] {
        if line_starts_with_prompt_marker(line) {
            return true;
        }
    }

    // findGenericSlimMessageBox: border / (| or │ or ❯) / border sandwich.
    if n >= 3 {
        for i in start..n.saturating_sub(2) {
            let mid = lines[i + 1];
            if contains_horizontal_border(lines[i])
                && (mid.contains('|') || mid.contains('│') || mid.contains('❯'))
                && contains_horizontal_border(lines[i + 2])
            {
                return true;
            }
        }
    }
    false
}

/// Detect a Codex-style input box: a `›` prompt marker near the bottom.
/// Ported from `removeCodexMessageBox` in lib/msgfmt/message_box.go, which
/// keys on `›` at the 3rd-from-last line. We relax to "any of the last ~5
/// lines" so a settled Codex prompt is detected even if the bottom chrome
/// shifts by a row.
pub(crate) fn has_codex_box(screen_tail: &str) -> bool {
    let lines: Vec<&str> = screen_tail.lines().collect();
    let n = lines.len();
    if n == 0 {
        return false;
    }
    let start = n.saturating_sub(5);
    lines[start..].iter().any(|l| l.contains('›'))
}

/// Detect the opencode footer line `╹▀▀▀▀…` near the bottom, which only
/// renders once the TUI is interactive. Ported from `removeOpencodeMessageBox`
/// in lib/msgfmt/message_box.go.
pub(crate) fn has_opencode_box(screen_tail: &str) -> bool {
    screen_tail
        .lines()
        .rev()
        .take(8)
        .any(|l| l.trim_start().starts_with("╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀"))
        // opencode also shows a `❯`/`>` prompt; accept that too for robustness.
        || screen_tail
            .lines()
            .rev()
            .take(5)
            .any(|l| l.contains('❯'))
}

/// True if the line's first meaningful glyph (after skipping leading spaces
/// and box-drawing border chars like `│ ╎ ─`) is a `>` or `❯` prompt marker.
/// This distinguishes a real input prompt from a `>` that merely appears
/// inside streamed agent output. See `has_greater_than_or_slim_box`.
fn line_starts_with_prompt_marker(line: &str) -> bool {
    let rest = line.trim_start_matches(|c: char| {
        c == ' ' || c == '\t' || BOX_DRAWING_CHARS.contains(&c)
    });
    rest.starts_with('>') || rest.starts_with('❯')
}

/// True if the line contains a horizontal border made of box-drawing chars.
/// Ported from `containsHorizontalBorder` in lib/msgfmt/message_box.go.
fn contains_horizontal_border(line: &str) -> bool {
    line.contains("───────────────") || line.contains("╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ansi_stripped_but_box_preserved() {
        let raw = "\x1b[2J\x1b[1;32m╭──────╮\x1b[0m\n\x1b[200~hi\x1b[201~";
        let out = strip_ansi_preserve_box(raw);
        assert!(out.contains('╭'), "box char must survive: {out:?}");
        assert!(out.contains('─'), "border char must survive: {out:?}");
        assert!(!out.contains('\x1b'), "no escape bytes: {out:?}");
        assert!(out.contains("hi"));
    }

    #[test]
    fn strip_chrome_yields_clean_text() {
        // A chrome-laden Claude-style frame around a two-line message.
        let raw = "\x1b[38;5;240m╭───────────────────────────╮\n\
            │ Here is the actual answer. │\n\
            │ Second line of content.    │\n\
            ╰───────────────────────────╯\n\
            ───────────────────────────\n\
            > \n\
            ───────────────────────────\x1b[0m";
        let clean = strip_tui_chrome(raw);
        assert!(clean.contains("Here is the actual answer."), "{clean:?}");
        assert!(clean.contains("Second line of content."), "{clean:?}");
        assert!(!clean.contains('╭'), "box chars removed: {clean:?}");
        assert!(!clean.contains('─'), "borders removed: {clean:?}");
        assert!(!clean.contains('\x1b'), "no escapes: {clean:?}");
        // No bare prompt line.
        assert!(!clean.lines().any(|l| l.trim() == ">"), "{clean:?}");
    }

    #[test]
    fn strip_chrome_removes_spinner_frames() {
        let raw = "✻ Thinking…\n· · ·\nDone.";
        let clean = strip_tui_chrome(raw);
        assert!(clean.contains("Thinking"), "{clean:?}");
        assert!(clean.contains("Done."), "{clean:?}");
        assert!(!clean.contains('✻'), "{clean:?}");
    }

    #[test]
    fn trim_empty_lines_keeps_interior() {
        let lines = vec![
            "".to_string(),
            "a".to_string(),
            "".to_string(),
            "b".to_string(),
            "".to_string(),
        ];
        assert_eq!(trim_empty_lines(&lines), "a\n\nb");
    }

    #[test]
    fn greater_than_box_detected() {
        let screen = "some output\nmore output\n──────────────────\n> \n──────────────────";
        assert!(has_greater_than_or_slim_box(screen));
    }

    #[test]
    fn slim_box_detected() {
        let screen = "work\n───────────────\n│\n───────────────";
        assert!(has_greater_than_or_slim_box(screen));
    }

    #[test]
    fn greater_than_in_streamed_output_is_not_a_prompt() {
        // A `>` mid-line (shell redirect, markdown quote, npm output) must NOT
        // be mistaken for the input prompt — this is the boot-churn /
        // mid-response false-positive the hardening guards against.
        let shell = "running shell command\n  $ cat > /tmp/out.txt\nwriting output";
        assert!(!has_greater_than_or_slim_box(shell), "shell redirect");
        let quote = "the doc says:\n  some text > other text in a sentence";
        assert!(!has_greater_than_or_slim_box(quote), "mid-sentence >");
        // NOTE: a line like `> my-pkg@1.0.0 build` (npm lifecycle output) is
        // surface-indistinguishable from a prompt-with-placeholder `> type
        // here`, so it is NOT filtered. Residual false-positive documented in
        // the report; the 750ms idle gate makes it rare in practice since such
        // lines appear mid-stream, not after output settles.
    }

    #[test]
    fn bordered_prompt_marker_is_detected() {
        // `│ > │` style prompt: marker is first meaningful glyph after border.
        let screen = "answer text\n│ > type here │";
        assert!(has_greater_than_or_slim_box(screen));
    }

    #[test]
    fn no_box_when_mid_spinner() {
        let screen = "✻ Crunching the request…\nstreaming tokens here\nstill working";
        assert!(!has_greater_than_or_slim_box(screen));
        assert!(!has_codex_box(screen));
        assert!(!has_opencode_box(screen));
    }

    #[test]
    fn codex_box_detected() {
        let screen = "ran a command\n\n›\n";
        assert!(has_codex_box(screen));
    }

    #[test]
    fn opencode_box_detected() {
        let screen =
            "┃\n┃  Build  Anthropic Claude Sonnet 4\n╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n  tab switch";
        assert!(has_opencode_box(screen));
    }
}
