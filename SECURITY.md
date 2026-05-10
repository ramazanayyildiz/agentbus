# Security policy

agentbus is a local-only IPC bus. The daemon listens on a Unix domain
socket inside `~/.agentbus/` (chmod 0700), the socket itself is chmod 0600,
and there is no network listener. Threat model is limited accordingly:
attackers with access to the user's account can already do anything they
want; the goal is to keep agentbus from making things worse.

## What's in scope

- Privilege escalation — a malicious bus client manipulating the daemon
  to do something the user didn't intend
- Cross-tenant attacks on a shared machine — one Unix user reading or
  injecting messages addressed to a different user's bus
- Input handling — a malicious `agentbus send` payload causing a wrapped
  agent's terminal to be hijacked by escape sequences
- Filesystem permissions — the `~/.agentbus/` directory and its contents
- CI / release artifact integrity — the binaries we publish under the
  GitHub Releases page

## What's out of scope

- Hostile users on the same machine with access to your home directory.
  If they can read `~/.agentbus/bus.db`, they can read your message
  history. The 0700 perm makes this a deliberate violation of POSIX
  permissions, not an oversight.
- Compromised LLM providers. Whatever the wrapped Claude / Codex agent
  decides to do is on it; agentbus only delivers messages.
- Network attacks. There is no network listener.

## Reporting a vulnerability

**Don't open a public GitHub issue for security bugs.**

Email: `software-dev@unusualgrowth.com`

Include:

  - A description of the vulnerability and its impact
  - Steps to reproduce
  - The version of agentbus you tested (`agentbus --version`)
  - Any proof-of-concept code or commands

You'll get a response within 7 days. If the bug is confirmed, we'll work
on a fix in a private branch, coordinate a disclosure timeline (typically
30–90 days), and credit you in the CHANGELOG unless you prefer otherwise.

## Past advisories

None yet — agentbus is too young.

## Hardening choices already in place

- **Daemon owns the socket bind.** The PID file is checked at startup;
  if another live agentbusd holds it, we refuse to start (instead of
  clobbering its socket).
- **Permissions.** `~/.agentbus/` is created with mode 0700, the socket
  with mode 0600.
- **Unregister authorization.** Only the connection that registered an
  agent name can `Unregister` it.
- **Soft-delete.** Removing an agent is a state flip, not a row delete,
  so message history stays referentially intact.
- **Body sanitization.** Message bodies are stripped of C0 controls
  (0x00–0x1F except `\t`), DEL (0x7F), and C1 controls (0x80–0x9F)
  before injection. A malicious sender can't smuggle Ctrl-C, escape
  sequences, or terminal manipulation codes through to a wrapped agent.
- **Bounded backpressure.** The daemon's per-agent push channel is
  capped (1000 messages); overflow releases the claim so the message
  stays redeliverable.
- **At-least-once with explicit ack.** A message is marked `read` only
  after the recipient's socket write succeeded, not before.

## What we still want to do

- A CSPRNG for IDs instead of `Uuid::v4()` (already cryptographically
  random in practice, but documenting it explicitly is useful).
- Optional message-body encryption at rest in `bus.db` (low-priority —
  the threat model doesn't really need it).
- Audit logging of `Unregister` and `Send` from non-owner senders, so
  admins can spot misbehaving clients.
