// Mock agent — a stand-in for claude/codex that gives us perfect visibility
// into exactly what bytes the inner process receives from the PTY master.
//
// Behavior:
//   - Prints a banner and a prompt
//   - Reads from stdin in raw mode (no canonical line buffering)
//   - For every byte received, prints a line showing:
//       byte_index | hex | char | description
//   - When it sees \r or \n, prints a "[SUBMIT received]" marker
//   - Exits on Ctrl-D (0x04) or Ctrl-C (0x03)
//
// This lets us answer: "does claude/codex actually see the bracketed-paste
// escape codes, or does the TTY layer translate them?"
//
// Usage in smoke test:
//   pty-smoke -- ./mock-agent

use std::io::{self, Read, Write};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

fn main() -> io::Result<()> {
    println!("MOCK_AGENT_READY (PID={})\r", std::process::id());
    println!("Watching stdin in raw mode. Ctrl-C or Ctrl-D to exit.\r");
    print!("mock> ");
    io::stdout().flush()?;

    // Put stdin into raw mode so we see every byte as-is, including escape
    // sequences and the carriage return / line feed exactly as sent.
    #[cfg(unix)]
    let saved_termios = enable_raw_mode()?;

    let mut stdin = io::stdin();
    let mut buf = [0u8; 1];
    let mut idx: usize = 0;
    let mut last_was_cr = false;

    loop {
        match stdin.read(&mut buf) {
            Ok(0) => {
                eprintln!("[mock] stdin closed\r");
                break;
            }
            Ok(_) => {
                let b = buf[0];
                let desc = describe(b);
                let printable = if (0x20..=0x7e).contains(&b) {
                    format!("{}", b as char)
                } else {
                    ".".to_string()
                };
                // \r at the end so the line wraps correctly under raw mode.
                println!("[{:04}] 0x{:02x}  {:<3} {}\r", idx, b, printable, desc);
                io::stdout().flush()?;
                idx += 1;

                if b == 0x04 {
                    eprintln!("[mock] EOT (Ctrl-D), exiting\r");
                    break;
                }
                if b == 0x03 {
                    eprintln!("[mock] ETX (Ctrl-C), exiting\r");
                    break;
                }
                if b == b'\r' || b == b'\n' {
                    // Treat CR-LF as one submission; only fire on the first.
                    if !(b == b'\n' && last_was_cr) {
                        println!("[SUBMIT received]\r");
                        io::stdout().flush()?;
                    }
                }
                last_was_cr = b == b'\r';
            }
            Err(e) => {
                eprintln!("[mock] read error: {}\r", e);
                break;
            }
        }
    }

    #[cfg(unix)]
    restore_termios(saved_termios)?;

    Ok(())
}

#[cfg(unix)]
fn enable_raw_mode() -> io::Result<libc::termios> {
    let fd = io::stdin().as_raw_fd();
    let mut t: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(fd, &mut t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let saved = t;

    // Disable canonical mode + echo + signal generation
    t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG | libc::IEXTEN);
    // Disable input processing that would translate or strip bytes
    t.c_iflag &= !(libc::IXON | libc::ICRNL | libc::INPCK | libc::ISTRIP | libc::BRKINT);
    // Read returns as soon as 1 byte is available, no timeout
    t.c_cc[libc::VMIN] = 1;
    t.c_cc[libc::VTIME] = 0;

    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(saved)
}

#[cfg(unix)]
fn restore_termios(t: libc::termios) -> io::Result<()> {
    let fd = io::stdin().as_raw_fd();
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn describe(b: u8) -> &'static str {
    match b {
        0x00 => "NUL",
        0x03 => "ETX (Ctrl-C)",
        0x04 => "EOT (Ctrl-D)",
        0x07 => "BEL",
        0x08 => "BS",
        0x09 => "TAB",
        0x0a => "LF \\n",
        0x0d => "CR \\r",
        0x1b => "ESC",
        0x7f => "DEL",
        b if (0x20..=0x7e).contains(&b) => "printable",
        b if b < 0x20 => "C0 control",
        _ => "non-ASCII",
    }
}
