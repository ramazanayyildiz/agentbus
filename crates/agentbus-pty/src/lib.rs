//! AgentBus PTY runner.
//!
//! Spawns a target command in a pseudo-terminal, registers an agent on the
//! bus, and bridges the two:
//!
//!   - bytes from the user's local stdin -> PTY stdin
//!   - bytes from the PTY's stdout -> user's local stdout (transparent)
//!   - messages received from the bus -> injected into the PTY as if pasted
//!
//! See `crate::runner::PtyRunner::run` for the orchestration. The PTY layer
//! is byte-transparent (verified in the `smoke/` Phase 0 crate), so all this
//! module does is shovel bytes between the four streams without translation.

pub mod inject;
pub mod runner;

pub use runner::PtyRunner;
