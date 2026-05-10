# Contributing to agentbus

Thanks for your interest. This is a small project; the bar for contributions
is correctness, clarity, and tests. Code review is welcomed and PRs are read.

## Quick start

```sh
git clone https://github.com/ramazanayyildiz/agentbus
cd agentbus
cargo build --release
cargo test --release
```

The workspace has three crates and a tests harness:

  - `crates/agentbus-core` — types, SQLite, protocol
  - `crates/agentbus-pty`  — `PtyRunner`, adapters, byte injection
  - `crates/agentbus-bin`  — three binaries: `agentbus`, `agentbusd`,
    `agentbus-mcp`
  - `tests/`               — end-to-end CLI flows

## Running the daemon during development

```sh
cargo build --release
./target/release/agentbus start            # spawns agentbusd
./target/release/agentbus status           # check
```

The daemon writes to `~/.agentbus/` (db, socket, pid file). For an isolated
test environment, set `AGENTBUS_DIR=/tmp/abx-test agentbusd` so tests don't
collide with your real bus.

## What kind of contribution fits

**Yes:**
- Bug fixes with a regression test
- New adapter for an agent we don't ship a profile for (open a draft PR
  and discuss the prompt patterns + idle threshold before adding)
- Documentation improvements
- Test coverage gaps
- Platform support: Linux x86 / arm64 fixes (macOS is the primary dev
  target, regressions there happen)

**Maybe — discuss first:**
- New top-level subcommands (we want the CLI surface to stay small)
- Schema changes (each one is a migration risk)
- Major architecture changes

**No:**
- Native Windows support without a clear plan for Unix sockets, ConPTY,
  and signal handling. WSL works fine.
- Cosmetic-only refactors that touch many files

## How a change should look

1. **One concept per PR.** Refactors and behavior changes go in separate PRs.
2. **Tests follow the convention.** Existing tests live in:
   - `crates/agentbus-core/tests/` — unit + DB layer
   - `crates/agentbus-bin/tests/` — daemon integration
   - `tests/tests/cli_flow.rs` — end-to-end
   Match the prefix scheme (`f001_`, `m010_`, `j006_`, etc.) so the test
   IDs map to behavior categories.
3. **Commit messages** use the project format:
   `[area] short summary` followed by a body explaining *why*. The git log
   doubles as a build journal — see existing commits for the style.
4. **No emojis in code or commits.** Keep them in the README.
5. **Conservative defaults.** A new flag or env var is fine; changing an
   existing default is a breaking change and needs a real reason.

## Reporting bugs

Open an issue with:

  - Operating system + version (`uname -a` is enough)
  - Output of `agentbus status` (or the error you hit instead)
  - Minimum repro: the exact `agentbus run …` line and the message you
    sent that didn't behave
  - If the daemon crashed, the last 50 lines of its stderr (it logs to the
    terminal where `agentbus start` ran, or to `journald` / `launchd` if
    you started it elsewhere)

## Reporting security issues

Don't open a public issue. See [SECURITY.md](SECURITY.md).

## Adding a new adapter

The adapter trait lives in `crates/agentbus-pty/src/adapter.rs`. To add a
new agent profile:

1. Add a struct + `impl Adapter for YourAgentAdapter`.
2. Decide between `inject::format_for_injection` (plain envelope + CR)
   and `bracketed_paste_envelope` (paste-aware TUIs).
3. Pick an `idle_ms_before_inject()` based on how the agent streams output.
   Aider-style readline prompts settle around 500 ms; streaming TUIs like
   Claude Code/Codex/opencode want 750 ms or higher.
4. Wire the new adapter into `pick()` with a substring match. Order
   matters when one substring is contained in another.
5. Add a unit test in `mod tests` confirming the format and idle threshold.
6. Add a real-world verification entry to the CHANGELOG.

## Style

- Rust 2021 edition. Follow `rustfmt` defaults; CI runs `cargo fmt --check`
  on PRs eventually but for now the convention is what's already in tree.
- Comments explain *why*, not *what*. The code is readable; reviewer
  questions about reasoning are usually answered by an inline comment.
- No `unwrap()` outside tests. Use `?`, `Result`, or `Option::ok_or_else`.

## License

By contributing you agree that your changes are licensed under the
project's [MIT License](LICENSE).
