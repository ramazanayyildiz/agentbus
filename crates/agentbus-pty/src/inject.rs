//! Message-injection helpers.
//!
//! Phase 1 MVP policy:
//!   - Strip C0 control characters (0x00-0x1F except \t \n) and the DEL byte
//!     so a malicious or buggy sender can't smuggle Ctrl-C, escape sequences,
//!     or terminal-manipulation codes through the bus.
//!   - Wrap the body with a small envelope so the inner agent (and the user
//!     watching the terminal) can see who sent the message and on what
//!     thread.
//!   - Append \r as the submit signal. Verified universal in the Phase 0
//!     smoke test — every TUI we care about treats CR as Enter.
//!
//! Phase 2 will move bracketed-paste wrapping and submit-key selection
//! behind an `Adapter` trait so different agent CLIs can opt in to richer
//! injection patterns.

use agentbus_core::Message;

/// Build the bytes to write into the PTY for an incoming bus message.
pub fn format_for_injection(msg: &Message) -> Vec<u8> {
    let safe_body = sanitize(&msg.body);
    let thread = msg
        .thread_id
        .as_deref()
        .map(|t| format!(" thread={}", t))
        .unwrap_or_default();
    // Single-line envelope. We want it to look like one line of typed input
    // so the inner TUI submits it as one message, not multiple.
    let line = format!(
        "[agentbus from={} type={}{}] {}",
        sanitize(&msg.from),
        msg.msg_type.as_str(),
        thread,
        safe_body
    );
    let mut out = line.into_bytes();
    out.push(b'\r');
    out
}

/// Strip control characters that could be hostile to the inner TTY.
///
/// Operates on chars (Unicode scalars), not raw bytes — that way multi-byte
/// UTF-8 sequences for non-ASCII chars survive intact.
///
/// Keep:
///   - HTAB (0x09)
///   - printable ASCII (0x20..=0x7e)
///   - any non-ASCII char (codepoint >= 0x80)
///
/// Drop:
///   - 0x00..=0x1F except \t (no LF, no CR — those are added by the caller)
///   - 0x7f (DEL)
///   - C1 control codes (0x80..=0x9f) — these are sometimes used as ANSI
///     control bytes; chars in this range are extremely rare in real text
///     and not worth the risk
pub fn sanitize(s: &str) -> String {
    s.chars()
        .filter(|c| {
            let cp = *c as u32;
            cp == 0x09
                || (0x20..=0x7e).contains(&cp)
                || cp >= 0xa0 // skip C1 controls 0x80..=0x9f
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentbus_core::MessageType;

    fn msg(body: &str) -> Message {
        Message {
            id: "test-id".into(),
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
    fn strips_control_characters() {
        let out = sanitize("hello\x03\x07\x1b[2Jworld");
        assert_eq!(out, "hello[2Jworld");
    }

    #[test]
    fn keeps_tab_and_drops_lf_cr() {
        let out = sanitize("a\tb\nc\rd");
        assert_eq!(out, "a\tbcd");
    }

    #[test]
    fn keeps_unicode() {
        let out = sanitize("café 日本語");
        assert_eq!(out, "café 日本語");
    }

    #[test]
    fn injection_format_ends_with_cr() {
        let bytes = format_for_injection(&msg("hi"));
        assert_eq!(bytes.last(), Some(&b'\r'));
    }

    #[test]
    fn injection_format_has_envelope() {
        let bytes = format_for_injection(&msg("hi"));
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.contains("[agentbus from=alice"));
        assert!(s.contains("type=request"));
        assert!(s.contains("hi"));
    }

    #[test]
    fn injection_drops_dangerous_chars() {
        // Body containing Ctrl-C (0x03) and an escape sequence
        let bytes = format_for_injection(&msg("evil\x03\x1b[1;31mtext"));
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(!s.contains('\x03'));
        assert!(!s.contains('\x1b'));
        // The visible part survives
        assert!(s.contains("eviltext") || s.contains("evil[1;31mtext"));
    }
}
