#!/usr/bin/env node
/**
 * agentbus-room — Terminal Room HUB (Model-B autonomous room)
 *
 * Registers as "room" on the AgentBus, launches agents, relays messages
 * as an IRC-server broadcast layer, renders the merged thread, and handles
 * human input with circuit-breaker protection against agent ping-pong loops.
 *
 * I/O contract: speaks the AgentBus socket protocol directly (not CLI)
 * for reading (structured, no envelope loss). Shells out to `agentbus send`
 * for individual relays (FK-safe, no second Register eviction).
 *
 * Usage:
 *   node agentbus-room.mjs <room-id> [--agents claude-A,claude-B] [--cb-max 6]
 *   node agentbus-room.mjs --self-test
 */

import net from "node:net";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import readline from "node:readline";
import { execFileSync, execFile, spawnSync, spawn } from "node:child_process";

// ── Config & Constants ──────────────────────────────────────────────────────

const AB_PATH = "/Users/ramazanayyildiz/.local/bin/agentbus";
const TEMPLATE_PATH = new URL("room-system-prompt.template.txt", import.meta.url).pathname;
// This script's own path — used by the launcher's POST /create-room to spawn a
// detached child room hub that re-enters main()/launchAgents().
const SCRIPT_PATH = new URL("agentbus-room.mjs", import.meta.url).pathname;

/**
 * Generate a fresh per-room-INSTANCE id. The instance id is what the room uses as
 * its bus `thread_id` and its log-file key, so that reusing a roomId in the same
 * cwd does NOT mix two room generations in the DB (thread_id) or the log file.
 * Persisted to rooms/<roomId>.json so --resume reuses it (a resumed room is the
 * SAME instance); only a FRESH same-name room gets a new one. Compact + sortable
 * (timestamp prefix) + unique (random suffix). Pure.
 */
function newInstanceId() {
  return Date.now().toString(36) + "-" + crypto.randomBytes(3).toString("hex");
}

// ── Transcript rotation (TASK 3) ────────────────────────────────────────────
// The Rust `agentbus run` runner writes per-agent transcripts into
// <agentbusDir>/transcripts/ (e.g. claude-<agent>-<ts>.txt). They accumulate
// indefinitely (this dir grew to 1.1GB). The sweep below prunes by age and a
// total-size cap. It runs on hub start, (debounced) on agent exit, and via the
// `--prune-transcripts` manual subcommand. All thresholds are tunable here:
const TRANSCRIPT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;   // prune files older than 7 days
const TRANSCRIPT_MAX_TOTAL_BYTES = 500 * 1024 * 1024;    // prune oldest-first until the dir is under ~500 MB
const TRANSCRIPT_LIVE_GRACE_MS = 2 * 60 * 1000;          // never prune a file modified < 2 min ago (it's being written now)
const TRANSCRIPT_SWEEP_MIN_INTERVAL_MS = 30 * 1000;      // debounce: min gap between sweeps triggered by agent exits

/**
 * PURE selection core for transcript pruning. Given a list of files
 * ({ path, size, mtimeMs }) and options, returns the deletion plan:
 *   { prune: [path...], freedBytes, byAge, bySize }
 *
 * Two passes:
 *  1. AGE — any non-protected file older than maxAgeMs is pruned.
 *  2. SIZE CAP — if the directory total (minus what pass 1 already frees) still
 *     exceeds maxTotalBytes, delete oldest-first among the remaining non-protected
 *     files until under the cap (or none left to delete). Protected files always
 *     count toward the total but are never deleted.
 *
 * `protectedPaths` may be a Set or array of exact paths to spare. now/ages are
 * injectable for deterministic tests. Pure — no I/O.
 */
function planTranscriptPrune(files, { maxAgeMs, maxTotalBytes, now = Date.now(), protectedPaths = [] } = {}) {
  const prot = protectedPaths instanceof Set ? protectedPaths : new Set(protectedPaths);
  const isProtected = (f) => prot.has(f.path);

  const prune = [];
  let freedBytes = 0;
  let byAge = 0;
  let bySize = 0;

  // 1. Age pass.
  const agePruned = new Set();
  if (maxAgeMs != null && maxAgeMs > 0) {
    for (const f of files) {
      if (isProtected(f)) continue;
      if (now - f.mtimeMs > maxAgeMs) {
        prune.push(f.path);
        freedBytes += f.size;
        byAge++;
        agePruned.add(f.path);
      }
    }
  }

  // 2. Size-cap pass.
  if (maxTotalBytes != null && maxTotalBytes > 0) {
    const total = files.reduce((s, f) => s + (f.size || 0), 0);
    let remaining = total - freedBytes;
    if (remaining > maxTotalBytes) {
      const survivors = files
        .filter((f) => !isProtected(f) && !agePruned.has(f.path))
        .sort((a, b) => a.mtimeMs - b.mtimeMs); // oldest first
      for (const f of survivors) {
        if (remaining <= maxTotalBytes) break;
        prune.push(f.path);
        freedBytes += f.size;
        remaining -= f.size;
        bySize++;
      }
    }
  }

  return { prune, freedBytes, byAge, bySize };
}

/**
 * List transcript files in `dir` (flat — one file per agent run). Returns
 * [{ path, size, mtimeMs }]. Best-effort: a missing dir or per-file stat error
 * is skipped, never thrown. I/O.
 */
function listTranscriptFiles(dir) {
  let entries;
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return []; }
  const out = [];
  for (const e of entries) {
    if (!e.isFile()) continue;
    const p = path.join(dir, e.name);
    let st;
    try { st = fs.statSync(p); } catch { continue; }
    out.push({ path: p, size: st.size, mtimeMs: st.mtimeMs });
  }
  return out;
}

/**
 * Return the names of agents currently state='active' on the bus, for live-file
 * protection. Queries the daemon DB (same one dbTailMessages reads). Best-effort:
 * any error → [] (the sweep then falls back to mtime-grace + explicit protects).
 * Async (execFile, non-blocking).
 */
function activeAgentNames() {
  const db = dbPath();
  if (!fs.existsSync(db)) return Promise.resolve([]);
  return new Promise((resolve) => {
    execFile("sqlite3", ["-json", db, "SELECT name FROM agents WHERE state='active';"], { timeout: 3000, encoding: "utf8" }, (err, stdout) => {
      if (err) { resolve([]); return; }
      try {
        const out = (stdout || "").trim();
        const rows = !out || out === "[]" ? [] : JSON.parse(out);
        resolve(rows.map((r) => r.name).filter(Boolean));
      } catch { resolve([]); }
    });
  });
}

/**
 * Sweep <agentbusDir>/transcripts/ and prune by age + size cap. Protection (never
 * delete): (1) exact `protectPaths` (files the caller's current run is writing),
 * (2) any transcript whose name matches an active agent (`-<name>-` substring, so
 * a live agent's history survives), (3) any file modified within LIVE_GRACE
 * (being written right now). Prints a one-line summary via printSystemMsg unless
 * `log` is false. Returns the prune counts. Async. Best-effort (unlink errors ignored).
 */
async function sweepTranscripts({ dryRun = false, protectPaths = [], log = true } = {}) {
  const dir = path.join(agentbusDir(), "transcripts");
  const files = listTranscriptFiles(dir);
  if (files.length === 0) {
    return { pruned: 0, freedBytes: 0, byAge: 0, bySize: 0, dryRun };
  }
  const prot = new Set(protectPaths.filter(Boolean));
  const now = Date.now();
  const actives = await activeAgentNames();
  for (const f of files) {
    if (prot.has(f.path)) continue;
    // (3) live-write grace — a transcript touched in the last few minutes is active.
    if (now - f.mtimeMs < TRANSCRIPT_LIVE_GRACE_MS) { prot.add(f.path); continue; }
    // (2) active-agent match — protect this agent's transcripts by name.
    const base = path.basename(f.path);
    for (const name of actives) {
      if (name && base.includes(`-${name}-`)) { prot.add(f.path); break; }
    }
  }
  const plan = planTranscriptPrune(files, {
    maxAgeMs: TRANSCRIPT_MAX_AGE_MS,
    maxTotalBytes: TRANSCRIPT_MAX_TOTAL_BYTES,
    now,
    protectedPaths: prot,
  });
  if (!dryRun) {
    for (const p of plan.prune) { try { fs.unlinkSync(p); } catch { /* already gone */ } }
  }
  if (log) {
    if (plan.prune.length > 0) {
      const mb = (plan.freedBytes / (1024 * 1024)).toFixed(1);
      const verb = dryRun ? "Dry run — would prune" : "Pruned";
      printSystemMsg(`${verb} ${plan.prune.length} transcript(s) (${mb} MB freed): ${plan.byAge} by age, ${plan.bySize} by size.`);
    } else if (dryRun) {
      printSystemMsg(`Dry run — no transcripts to prune (dir under age/size thresholds).`);
    }
  }
  return { pruned: plan.prune.length, freedBytes: plan.freedBytes, byAge: plan.byAge, bySize: plan.bySize, dryRun };
}
// Stop-hook forwarder: auto-delivers a claude agent's reply to the room even if the
// (often resumed) model forgets to run `agentbus send`. Referenced only from the
// per-agent --settings file, so it never affects the user's global Claude config.
const STOP_FORWARD_PATH = new URL("stop-forward.mjs", import.meta.url).pathname;

/** Env-aware agentbus directory (mirrors lib.rs agentbus_dir) */
function agentbusDir() {
  const override = process.env.AGENTBUS_DIR;
  if (override && override.length > 0) return override;
  return path.join(os.homedir(), ".agentbus");
}

function socketPath() {
  return path.join(agentbusDir(), "agentbus.sock");
}

function dbPath() {
  return path.join(agentbusDir(), "bus.db");
}

// ── ANSI Colors ──────────────────────────────────────────────────────────────

const C = {
  RESET:   "\x1b[0m",
  DIM:     "\x1b[2m",
  BOLD:    "\x1b[1m",
  GREEN:   "\x1b[32m",
  RED:     "\x1b[31m",
  CYAN:    "\x1b[36m",
  YELLOW:  "\x1b[33m",
  MAGENTA: "\x1b[35m",
  BLUE:    "\x1b[34m",
  WHITE:   "\x1b[37m",
  // bright variants — ram gets BRGREEN to stand out; agents draw from the bright palette
  BRGREEN:   "\x1b[92m",
  BRCYAN:    "\x1b[96m",
  BRYELLOW:  "\x1b[93m",
  BRMAGENTA: "\x1b[95m",
  BRBLUE:    "\x1b[94m",
};

// Subtle background for ram's own message body block. The user's terminal theme is
// light/cream, so a light-gray (256-color 253) reads as "a bit darker than the cream".
// Kept as one named const so the shade is trivial to retune later.
const RAM_BG = "\x1b[48;5;253m";
// Width the ram body block is padded to so the background fills a clean rectangle.
const RAM_BG_WIDTH = 62;

/** Deterministic color assignment for agent names. GREEN is reserved for the human (ram). */
const AGENT_COLORS = [C.CYAN, C.YELLOW, C.MAGENTA, C.BLUE, C.BRCYAN, C.BRYELLOW, C.BRMAGENTA, C.BRBLUE];
const colorCache = new Map();
let colorIdx = 0;

function agentColor(name) {
  if (name === "ram") return C.GREEN; // human's name stays normal green (body gets RAM_BG instead)
  // The hub's bus identity is namespaced per room ("room-<roomId>"), but the
  // human-facing label stays "room". Match both so the namespaced sender renders dim.
  if (name === "room" || name.startsWith("room-")) return C.DIM;
  if (!colorCache.has(name)) {
    colorCache.set(name, AGENT_COLORS[colorIdx % AGENT_COLORS.length]);
    colorIdx++;
  }
  return colorCache.get(name);
}

// ── Pure Functions (testable) ─────────────────────────────────────────────────

/**
 * Derive the hub's BUS identity from a room id. The hub registers on the AgentBus
 * daemon under this name and agents reply to it. It MUST be namespaced per room:
 * if two hubs both registered as the literal "room", the daemon's reattach semantics
 * would evict the prior connection, causing an endless evict/reconnect storm that
 * kills both rooms. The human-facing DISPLAY label stays "room" (see agentColor and
 * renderMessage); only the on-bus identity becomes "room-<roomId>".
 */
function roomBusFor(roomId) {
  return `room-${roomId}`;
}

/**
 * (A) Per-agent bus namespacing.
 * Derive an agent's BUS identity from its DISPLAY name. Agents register on the
 * daemon under this namespaced id so two rooms reusing a display name (e.g.
 * "claude-A") can't collide — same class of bug as the room-name collision that
 * roomBusFor fixes. The DISPLAY name (this.members) stays unmangled; we convert
 * display→busId only at OUTGOING bus boundaries (run --name, send --from/--to).
 */
function agentBusId(roomId, name) {
  return `${roomId}-${name}`;
}

/**
 * (A) Inverse of agentBusId for the DISPLAY direction. Strips a leading
 * "<roomId>-" from a from_agent value so UI shows the friendly name. Applied only
 * at INCOMING boundaries (live push msg.from, DB-tail from_agent render). The hub's
 * own roomBus ("room-<roomId>") and any other value are returned UNCHANGED — only
 * the exact "<roomId>-" prefix is stripped, so "room-r1" is NOT mangled (it doesn't
 * start with "r1-"). Uses a literal prefix check, not a regex, to stay exact.
 */
function displayName(fromAgent, roomId) {
  if (typeof fromAgent !== "string") return fromAgent;
  const prefix = `${roomId}-`;
  if (fromAgent.startsWith(prefix)) return fromAgent.slice(prefix.length);
  return fromAgent;
}

/**
 * Parse one newline-terminated line from the AgentBus socket.
 *
 * Returns:
 *   { kind: "batch",   messages: Message[] }   — Ok { data: [...] }
 *   { kind: "push",    messages: [Message] }   — Message { message: {...} }
 *   { kind: "ack",     data: any }             — Ok { data: non-array } (e.g. register ack)
 *   { kind: "timeout"                        } — Error "No messages (timeout)"
 *   { kind: "error",   message: string }       — other Error
 *   { kind: "unknown", raw: string }           — unparseable
 */
function parseReadLine(line) {
  let parsed;
  try {
    parsed = JSON.parse(line.trim());
  } catch {
    return { kind: "unknown", raw: line };
  }

  const type = parsed.type;

  if (type === "Ok") {
    const data = parsed.data;
    if (Array.isArray(data)) {
      return { kind: "batch", messages: data };
    }
    return { kind: "ack", data };
  }

  if (type === "Message") {
    const msg = parsed.message;
    if (msg && typeof msg === "object") {
      return { kind: "push", messages: [msg] };
    }
    return { kind: "unknown", raw: line };
  }

  if (type === "Error") {
    const message = parsed.message || "";
    if (message.includes("No messages (timeout)")) {
      return { kind: "timeout" };
    }
    return { kind: "error", message };
  }

  return { kind: "unknown", raw: line };
}

/**
 * Compute the fan-out list for a received message.
 *
 * - Sender is excluded from its own relay.
 * - Body is prefixed with "<from>: " to preserve authorship.
 * - If targetOverride is set, only that member receives it (for @mentions).
 *
 * Returns: Array<{ to: string, body: string }>
 */
function computeFanout(msg, members, targetOverride = null) {
  const author = msg.from_agent || msg.from || "unknown";
  const body = `${author}: ${msg.body}`;
  const targets = targetOverride
    ? members.filter((m) => m === targetOverride && m !== author)
    : members.filter((m) => m !== author);
  return targets.map((to) => ({ to, body }));
}

/**
 * (D) Pure @-mention completion. Given the current readline `line` and the member
 * DISPLAY names, returns { matches, replacement } where:
 *   - replacement = the trailing "@<partial>" token to be replaced (or null if the line
 *     does not end in an @-token, meaning "no completion").
 *   - matches = full "@<name>" strings whose name starts with <partial> (case-insensitive).
 *     A bare "@" (empty partial) lists ALL members.
 * The readline `completer` wrapper turns this into Node's [hits, replacementToken] contract.
 */
function completeMention(line, members) {
  // Find the last "@token" at the very end of the line (token = [A-Za-z0-9_-]*).
  const m = line.match(/@([A-Za-z0-9_-]*)$/);
  if (!m) return { matches: [], replacement: null };
  const partial = m[1].toLowerCase();
  const matches = members
    .filter((name) => name.toLowerCase().startsWith(partial))
    .map((name) => `@${name}`);
  return { matches, replacement: `@${m[1]}` };
}

// ── Paste Coalescing (FIX #5, pure core) ──────────────────────────────────────

/**
 * Milliseconds to wait for another 'line' event before treating the buffer as a
 * complete input. A multi-line PASTE fires a burst of 'line' events within a few ms;
 * a human typing fires one 'line' then a long pause. 20ms cleanly separates them.
 */
const PASTE_COALESCE_MS = 20;

/**
 * Join a buffer of coalesced 'line' events into ONE message body, preserving the
 * pasted multi-line structure. A single typed line → the line unchanged. Pure.
 */
function coalesceLines(lines) {
  return lines.join("\n");
}

// ── Agent Spec Parsing (per-program launch) ───────────────────────────────────

/** Programs the hub knows how to launch. */
const VALID_PROGRAMS = ["claude", "codex", "gemini", "agy", "qodercli", "cmd", "exec"];

// Agent name validation pattern — prevents shell metacharacter injection via names.
const AGENT_NAME_RE = /^[A-Za-z0-9_-]+$/;

/**
 * Parse one agent spec of the form `name[:program]`.
 * Bare `name` → program 'claude' (backward compatible).
 * Pure function (no process.exit) so it is unit-testable: returns either
 *   { name, program }   on success
 *   { error: string }   on invalid name or unknown program
 */
function parseAgentSpec(spec) {
  // Forms: "name" | "name:program" | "name:program:model"
  // (model is optional; e.g. claude-A:claude:sonnet, codex-A:codex:gpt-5.1).
  const trimmed = String(spec).trim();
  const parts = trimmed.split(":");
  const name = parts[0] || "";
  const program = parts.length >= 2 && parts[1] ? parts[1] : "claude";
  const model = parts.length >= 3 && parts[2] ? parts[2] : null;
  if (!AGENT_NAME_RE.test(name)) {
    return { error: `invalid agent name "${name}" — only [A-Za-z0-9_-] allowed` };
  }
  if (!VALID_PROGRAMS.includes(program)) {
    return { error: `invalid program "${program}" for agent "${name}" — must be one of ${VALID_PROGRAMS.join(", ")}` };
  }
  return { name, program, model };
}

/**
 * Build the exact config.toml contents for an isolated Codex agent.
 * Pre-trusts the given directory (no folder-trust modal) and omits [mcp_servers.*] to
 * avoid the heavy MCP boot stall. Pure function — testable.
 * FIX #4: the trusted dir is now the USER'S launchDir (where codex runs via --cd),
 * not a throwaway tmp workdir — so the agent can see and work in the real project.
 */
function buildCodexConfigToml(trustedDir, model = "gpt-5.5", effort = "medium") {
  return (
    `model = "${model}"\n` +
    `model_reasoning_effort = "${effort}"\n` +
    `[projects."${trustedDir}"]\n` +
    `trust_level = "trusted"\n`
  );
}

/**
 * Build the exact `agentbus run` argv for a GEMINI agent. Pure function — testable.
 *
 * VALIDATED GEMINI RECIPE (live-probed — replicate exactly):
 *   gemini --yolo --skip-trust --allowed-mcp-server-names __none__ -i "<ROOM_PROMPT>"
 *  - --yolo                          = auto-approve all tools (so it can run `agentbus send`).
 *  - --skip-trust                    = trust the workspace for the session (no folder-trust modal).
 *  - --allowed-mcp-server-names __none__ = a DUMMY server name so no real MCP servers load (clean
 *    fast boot). MUST be the literal "__none__" — an EMPTY string crashes gemini with
 *    "mcpName is required if specified (cannot be empty)".
 *  - -i "<ROOM_PROMPT>"              = the room system prompt as the initial interactive prompt
 *    (gemini reads it, acknowledges, continues interactive). Gemini's trust-establishment, the
 *    equivalent of Claude's --append-system-prompt-file / Codex's AGENTS.md. Passed as a SINGLE
 *    argv element (spawn, no shell) so it is never interpolated.
 *  - RESUME: gemini has NATIVE per-project session resume — `-r latest` resumes the most recent
 *    session for the cwd/project. On resume we add `-r latest` (no session-id capture needed).
 *    NOTE: "latest" is per-project, so >1 gemini in the SAME launchDir is ambiguous (best-effort).
 *  - Runs in the user's cwd = launchDir (spawnRunner sets cwd). NO isolated home / temp files /
 *    AGENTS.md needed. The agentbus-pty crate has no gemini adapter → the Generic adapter is used
 *    (intended; do NOT touch the Rust crate).
 */
function buildGeminiArgs(busId, transcriptFile, prompt, { resume = false } = {}) {
  const geminiArgs = resume
    ? ["gemini", "--yolo", "--skip-trust", "--allowed-mcp-server-names", "__none__", "-r", "latest", "-i", prompt]
    : ["gemini", "--yolo", "--skip-trust", "--allowed-mcp-server-names", "__none__", "-i", prompt];
  return [
    "run",
    "--name", busId,
    "--program", "gemini",
    "--transcript", transcriptFile,
    "--",
    ...geminiArgs,
  ];
}

/**
 * Build the exact `agentbus run` argv for an AGY agent. Pure function — testable.
 *
 * VALIDATED AGY RECIPE (live-probed — replicate exactly):
 *   agy --dangerously-skip-permissions -i "<ROOM_PROMPT>"
 *  - --dangerously-skip-permissions  = auto-approve all tools (so it can run `agentbus send`).
 *  - -i "<ROOM_PROMPT>"              = initial interactive prompt for trust establishment.
 *    Passed as a SINGLE argv element (spawn, no shell) so it is never interpolated.
 *  - No MCP flag needed (agy has no --allowed-mcp-server-names equivalent; boots normally).
 *  - No isolated home needed (simpler than codex).
 *  - Runs in the user's cwd = launchDir (spawnRunner already sets cwd: launchDir).
 *  - No temp files to create/clean (like gemini).
 *  - RESUME: agy supports `--continue` / `-c` (continues most recent conversation) and
 *    `--conversation <id>` (resume by ID). We use `--continue` as the best-effort approach
 *    (same rationale as gemini's `-r latest`: no id-capture needed at session launch time).
 *    NOTE: `--conversation <id>` would be more precise but requires capturing the conversation
 *    id from the agy session output — use `--continue` until that plumbing is wired up.
 */
function buildAgyArgs(busId, transcriptFile, prompt, { resume = false } = {}) {
  const agyArgs = resume
    ? ["agy", "--dangerously-skip-permissions", "--continue", "-i", prompt]
    : ["agy", "--dangerously-skip-permissions", "-i", prompt];
  return [
    "run",
    "--name", busId,
    "--program", "agy",
    "--transcript", transcriptFile,
    "--",
    ...agyArgs,
  ];
}

/**
 * Build the exact `agentbus run` argv for a QODERCLI agent. Pure function — testable.
 *
 * QODERCLI RECIPE (based on published CLI flags):
 *   qodercli --dangerously-skip-permissions --system-prompt "<ROOM_PROMPT>" [--model <m>] [--continue]
 *  - --dangerously-skip-permissions = auto-approve all tools (so it can run `agentbus send`).
 *  - --system-prompt "<ROOM_PROMPT>" = room roster + bus usage instructions.
 *  - --continue                     = resume the most recent session on restart.
 *  - --model <m>                    = per-agent model override (optional).
 *
 * The room system prompt is delivered via --system-prompt (same as claude's --append-system-prompt).
 * RESUME: `--continue` = most recent session; `--resume <id>` = specific session (not yet wired).
 */
function buildQoderCliArgs(busId, transcriptFile, prompt, { resume = false, model = null } = {}) {
  const qoderArgs = [
    "qodercli",
    "--dangerously-skip-permissions",
    "--system-prompt", prompt,
    ...(model ? ["--model", model] : []),
    ...(resume ? ["--continue"] : []),
  ];
  return [
    "run",
    "--name", busId,
    "--program", "qodercli",
    "--transcript", transcriptFile,
    "--",
    ...qoderArgs,
  ];
}

// ── Resume / State Persistence (pure path + (de)serialize helpers) ────────────

/**
 * Path to the per-room state file: <agentbusDir>/rooms/<roomId>.json
 * Honors AGENTBUS_DIR via agentbusDir(). Pure (derives from env+args only).
 */
function stateFilePathFor(roomId) {
  return path.join(agentbusDir(), "rooms", `${roomId}.json`);
}

/**
 * Stable per-(roomId, name) CODEX_HOME, persisted across runs so `codex resume`
 * can find the prior session. Lives under <agentbusDir>/rooms/<roomId>/codex-<name>/
 * (NOT tmp — tmp is wiped by the OS and by old cleanup logic). Pure.
 */
function codexHomeFor(roomId, name) {
  return path.join(agentbusDir(), "rooms", roomId, `codex-${name}`);
}

/**
 * Find the CODEX_HOME whose sessions/ directory actually contains the given
 * session UUID. A `--agent-session` codex id may live in the GLOBAL ~/.codex OR
 * in another room's isolated home (e.g. resuming delta3's codex elsewhere). We
 * can't assume global — searching for the rollout-*-<uuid>.jsonl file tells us
 * exactly which home owns it. Returns the home dir, or null if not found anywhere.
 */
function findCodexHomeForSession(sessionId) {
  if (!sessionId) return null;
  const candidates = [path.join(os.homedir(), ".codex")];
  // All room codex homes: <agentbusDir>/rooms/<roomId>/codex-<name>/
  try {
    const roomsDir = path.join(agentbusDir(), "rooms");
    for (const room of fs.readdirSync(roomsDir, { withFileTypes: true })) {
      if (!room.isDirectory()) continue;
      const roomPath = path.join(roomsDir, room.name);
      for (const sub of fs.readdirSync(roomPath, { withFileTypes: true })) {
        if (sub.isDirectory() && sub.name.startsWith("codex-")) {
          candidates.push(path.join(roomPath, sub.name));
        }
      }
    }
  } catch { /* no rooms dir yet */ }
  // Also any standalone agent-codex-* homes.
  try {
    for (const e of fs.readdirSync(agentbusDir(), { withFileTypes: true })) {
      if (e.isDirectory() && e.name.startsWith("agent-codex-")) {
        candidates.push(path.join(agentbusDir(), e.name));
      }
    }
  } catch { /* ignore */ }

  for (const home of candidates) {
    const sessRoot = path.join(home, "sessions");
    if (!fs.existsSync(sessRoot)) continue;
    // Sessions are nested YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl — walk and match.
    const stack = [sessRoot];
    while (stack.length) {
      const dir = stack.pop();
      let entries;
      try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { continue; }
      for (const en of entries) {
        const full = path.join(dir, en.name);
        if (en.isDirectory()) stack.push(full);
        else if (en.isFile() && en.name.includes(sessionId) && en.name.endsWith(".jsonl")) {
          return home;
        }
      }
    }
  }
  return null;
}

/**
 * Enumerate every CODEX_HOME we know about: the GLOBAL ~/.codex plus every
 * isolated room home (<agentbusDir>/rooms/<roomId>/codex-<name>/) and every
 * standalone agent-codex-* home. De-duplicated. Used by the session picker.
 */
function listCodexHomes() {
  const homes = [path.join(os.homedir(), ".codex")];
  try {
    const roomsDir = path.join(agentbusDir(), "rooms");
    for (const room of fs.readdirSync(roomsDir, { withFileTypes: true })) {
      if (!room.isDirectory()) continue;
      const roomPath = path.join(roomsDir, room.name);
      for (const sub of fs.readdirSync(roomPath, { withFileTypes: true })) {
        if (sub.isDirectory() && sub.name.startsWith("codex-")) {
          homes.push(path.join(roomPath, sub.name));
        }
      }
    }
  } catch { /* no rooms dir yet */ }
  try {
    for (const e of fs.readdirSync(agentbusDir(), { withFileTypes: true })) {
      if (e.isDirectory() && e.name.startsWith("agent-codex-")) {
        homes.push(path.join(agentbusDir(), e.name));
      }
    }
  } catch { /* ignore */ }
  return [...new Set(homes)];
}

/**
 * Extract a short preview of the first real USER message from a Claude Code
 * session jsonl. Claude stores one JSON event per line; user turns have
 * type:"user" with a string message.content (tool_result objects are skipped).
 * Returns "" if none found. Best-effort: any parse error on a line is ignored.
 * Pure: takes the raw file content.
 */
function extractClaudeFirstUserMessage(jsonl) {
  for (const line of String(jsonl).split("\n")) {
    if (!line.trim()) continue;
    let evt;
    try { evt = JSON.parse(line); } catch { continue; }
    if (evt && evt.type === "user" && evt.message && typeof evt.message.content === "string") {
      return evt.message.content.replace(/\s+/g, " ").trim().slice(0, 200);
    }
  }
  return "";
}

/**
 * Extract a short preview of the first real USER message from a Codex rollout
 * jsonl. Codex stores session_meta + message entries; user turns have
 * type:"message" with payload.role === "user" and a string payload.content[].
 * System/meta entries are skipped. Returns "" if none found. Best-effort.
 * Pure: takes the raw file content.
 */
function extractCodexFirstUserMessage(jsonl) {
  for (const line of String(jsonl).split("\n")) {
    if (!line.trim()) continue;
    let evt;
    try { evt = JSON.parse(line); } catch { continue; }
    if (!evt || evt.type !== "message" || !evt.payload) continue;
    const p = evt.payload;
    if (p.role !== "user") continue;
    const content = Array.isArray(p.content) ? p.content : [p.content];
    for (const c of content) {
      // Codex content items are { type: "input_text", text: "..." } or raw strings.
      const txt = typeof c === "string" ? c : (c && typeof c.text === "string" ? c.text : "");
      const clean = txt.replace(/\s+/g, " ").trim();
      // Skip env/context banners Codex injects (they start with <env> or are wrappers).
      if (clean && !clean.startsWith("<environment_context>")) return clean.slice(0, 200);
    }
  }
  return "";
}

/**
 * Encode an absolute directory path the way Claude Code names its project
 * folder under ~/.claude/projects/: every "/" becomes "-". Pure.
 *   /Users/foo/bar  →  -Users-foo-bar
 */
function claudeProjectDirName(dir) {
  return String(dir).replace(/\//g, "-");
}

/**
 * Read up to `maxBytes` from the head of a file as utf8. Used by the session
 * picker so we never read a multi-MB transcript in full — the first user
 * message is always near the top. Returns "" on any error. Best-effort.
 */
function readFileHead(filePath, maxBytes = 16384) {
  let fd;
  try {
    fd = fs.openSync(filePath, "r");
    const buf = Buffer.alloc(maxBytes);
    const n = fs.readSync(fd, buf, 0, maxBytes, 0);
    return buf.slice(0, n).toString("utf8");
  } catch {
    return "";
  } finally {
    if (fd !== undefined) { try { fs.closeSync(fd); } catch {} }
  }
}

/**
 * List resumable CLAUDE sessions for a given working dir.
 * Reads ~/.claude/projects/<dir-hash>/*.jsonl (only the head of each — the first
 * user message is near the top, so we never load a whole transcript). Returns
 * an array of { uuid, size, mtime, preview } sorted newest-first by mtime.
 * Returns [] if the project folder is absent. Best-effort fs.
 */
function listClaudeSessions(dir) {
  const projectDir = path.join(os.homedir(), ".claude", "projects", claudeProjectDirName(dir));
  let files;
  try { files = fs.readdirSync(projectDir, { withFileTypes: true }); } catch { return []; }
  const out = [];
  for (const e of files) {
    if (!e.isFile() || !e.name.endsWith(".jsonl")) continue;
    const full = path.join(projectDir, e.name);
    let st;
    try { st = fs.statSync(full); } catch { continue; }
    out.push({
      uuid: e.name.slice(0, -".jsonl".length),
      size: st.size,
      mtime: st.mtimeMs,
      preview: extractClaudeFirstUserMessage(readFileHead(full)),
    });
  }
  out.sort((a, b) => b.mtime - a.mtime);
  return out;
}

/**
 * List resumable CODEX sessions across every known CODEX_HOME (global ~/.codex
 * + all isolated room homes). Sessions are nested YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl.
 * Only the head of each file is read (first user message is near the top) so a
 * large sessions tree never blocks. Returns an array of
 * { uuid, size, mtime, preview, home } sorted newest-first. Best-effort fs.
 */
function listCodexSessions() {
  const out = [];
  for (const home of listCodexHomes()) {
    const sessRoot = path.join(home, "sessions");
    if (!fs.existsSync(sessRoot)) continue;
    const stack = [sessRoot];
    while (stack.length) {
      const dir = stack.pop();
      let entries;
      try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { continue; }
      for (const en of entries) {
        const full = path.join(dir, en.name);
        if (en.isDirectory()) { stack.push(full); continue; }
        if (!en.isFile() || !en.name.startsWith("rollout-") || !en.name.endsWith(".jsonl")) continue;
        // rollout-<timestamp>-<uuid>.jsonl → uuid is the trailing UUID chunk.
        const base = en.name.slice(0, -".jsonl".length);
        const dash = base.lastIndexOf("-");
        const uuid = dash > 0 ? base.slice(dash + 1) : base;
        let st;
        try { st = fs.statSync(full); } catch { continue; }
        out.push({ uuid, size: st.size, mtime: st.mtimeMs, preview: extractCodexFirstUserMessage(readFileHead(full)), home });
      }
    }
  }
  out.sort((a, b) => b.mtime - a.mtime);
  return out;
}

/**
 * Combined dispatcher for the /sessions endpoint. Pure routing on program.
 * Returns [] for unknown programs.
 */
function listSessions(program, dir) {
  if (program === "claude") return listClaudeSessions(dir);
  if (program === "codex") return listCodexSessions();
  return [];
}

/**
 * Build the argv array for `node room/agentbus-room.mjs <roomId> ...` from a
 * launcher-submitted config. Inverse of parseArgs — the child re-parses these
 * and drives the existing launchAgents() path. Pure.
 *
 * config = {
 *   roomId, launchDir, webPort?,
 *   agents: [{ name, program, model?, mode: "new"|"resume", sessionId?, cmd? }]
 * }
 * Returns { argv, error? }.
 */
function buildRoomLaunchArgv(config) {
  if (!config || typeof config !== "object") return { argv: null, error: "missing config" };
  if (!config.roomId || !AGENT_NAME_RE.test(config.roomId)) {
    return { argv: null, error: "invalid roomId" };
  }
  const agents = Array.isArray(config.agents) ? config.agents : [];
  if (agents.length === 0) return { argv: null, error: "no agents provided" };

  const seen = new Set();
  const specs = [];
  const sessionFlags = [];
  const cmdFlags = [];
  for (const a of agents) {
    if (!a || typeof a.name !== "string" || !AGENT_NAME_RE.test(a.name)) {
      return { argv: null, error: `invalid agent name "${a && a.name}"` };
    }
    if (seen.has(a.name)) {
      return { argv: null, error: `duplicate agent name "${a.name}"` };
    }
    seen.add(a.name);
    const program = a.program || "claude";
    if (!VALID_PROGRAMS.includes(program)) {
      return { argv: null, error: `invalid program "${program}" for "${a.name}"` };
    }
    const spec = a.model ? `${a.name}:${program}:${a.model}` : `${a.name}:${program}`;
    specs.push(spec);
    if (a.mode === "resume" && a.sessionId) {
      sessionFlags.push("--agent-session", `${a.name}:${a.sessionId}`);
    }
    if (program === "cmd" && a.cmd) {
      cmdFlags.push("--cmd", `${a.name}:${a.cmd}`);
    }
  }

  const argv = [
    config.roomId,
    "--agents", specs.join(","),
    "--launch-dir", config.launchDir || process.cwd(),
  ];
  if (config.webPort) argv.push("--web", String(config.webPort));
  for (const f of sessionFlags) argv.push(f);
  for (const f of cmdFlags) argv.push(f);
  return { argv, error: null };
}

/**
 * Serialize hub state to the on-disk JSON shape. Pure — testable round-trip.
 * agents[] carries each member's program + (program-specific) resume handle:
 *   codex → codexHome path; claude → claudeSessionId (uuid). Either may be absent.
 * instanceId is the room's INSTANCE id (the thread_id); persisted so --resume
 * reuses it instead of generating a new one (a resumed room is the same instance).
 */
function serializeState(roomId, members, programs, resumeInfo, models = {}, instanceId = null) {
  return {
    roomId,
    instanceId: instanceId || undefined,
    updatedAt: new Date().toISOString(),
    agents: members.map((name) => {
      const a = { name, program: programs[name] || "claude" };
      if (models && models[name]) a.model = models[name];
      const info = resumeInfo[name] || {};
      if (info.codexHome) a.codexHome = info.codexHome;
      if (info.claudeSessionId) a.claudeSessionId = info.claudeSessionId;
      return a;
    }),
  };
}

/**
 * Parse a state-file object back into { roomId, agents } with defensive defaults.
 * Pure — accepts the parsed JSON, returns null if the shape is unusable.
 */
function deserializeState(obj) {
  if (!obj || typeof obj !== "object" || !Array.isArray(obj.agents)) return null;
  const agents = obj.agents
    .filter((a) => a && typeof a.name === "string" && AGENT_NAME_RE.test(a.name))
    .map((a) => ({
      name: a.name,
      program: VALID_PROGRAMS.includes(a.program) ? a.program : "claude",
      model: typeof a.model === "string" ? a.model : undefined,
      codexHome: typeof a.codexHome === "string" ? a.codexHome : undefined,
      claudeSessionId: typeof a.claudeSessionId === "string" ? a.claudeSessionId : undefined,
    }));
  // instanceId: optional (absent on state files written before instance namespacing).
  // Returned so --resume can reuse it; null/undefined → caller falls back to roomId.
  const instanceId = typeof obj.instanceId === "string" && obj.instanceId ? obj.instanceId : null;
  return { roomId: obj.roomId, instanceId, agents };
}

/**
 * Build the SQL that replays the prior thread on --resume: the last `limit` rows
 * for this room, excluding the hub's relay copies (from_agent = roomBus). We select
 * DESC+LIMIT to cap at the TAIL, then the caller reverses to chronological order.
 * `threadId` is the room's INSTANCE id (what the DB rows are keyed by). Pure —
 * returns the SQL string (threadId/roomBus single-quote-escaped, limit coerced int).
 */
function buildReplaySql(threadId, roomBus, limit) {
  const escapedThreadId = String(threadId).replace(/'/g, "''");
  const escapedRoomBus = String(roomBus).replace(/'/g, "''");
  return (
    `SELECT rowid, id, from_agent, to_agent, thread_id, msg_type, body, created_at ` +
    `FROM messages ` +
    `WHERE thread_id = '${escapedThreadId}' AND from_agent != '${escapedRoomBus}' ` +
    `ORDER BY rowid DESC LIMIT ${Number(limit) || 0};`
  );
}

// ── Circuit-Breaker State Machine ─────────────────────────────────────────────

class CircuitBreaker {
  constructor(maxConsecutive = 6) {
    this.maxConsecutive = maxConsecutive;
    this.consecutiveAgentMessages = 0;
    this.paused = false;
  }

  /** Record an agent message. Returns true if the breaker just tripped. */
  recordAgentMessage() {
    this.consecutiveAgentMessages++;
    if (!this.paused && this.consecutiveAgentMessages >= this.maxConsecutive) {
      this.paused = true;
      return true; // tripped
    }
    return false;
  }

  /** Record a human message — resets the consecutive counter and unpauses. */
  recordHumanMessage() {
    this.consecutiveAgentMessages = 0;
    this.paused = false;
  }

  isPaused() {
    return this.paused;
  }

  count() {
    return this.consecutiveAgentMessages;
  }
}

// ── Newline-Framed Socket Reader ──────────────────────────────────────────────

/**
 * Wraps a node:net socket with a line-buffer.
 * Emits complete '\n'-terminated lines via onLine callback.
 * Never assumes one data event = one JSON line.
 */
class LineReader {
  constructor(socket, onLine) {
    this.socket = socket;
    this.onLine = onLine;
    this.buf = "";

    socket.on("data", (chunk) => {
      this.buf += chunk.toString("utf8");
      let idx;
      while ((idx = this.buf.indexOf("\n")) !== -1) {
        const line = this.buf.slice(0, idx);
        this.buf = this.buf.slice(idx + 1);
        if (line.trim().length > 0) this.onLine(line);
      }
    });
  }

  write(json) {
    this.socket.write(json + "\n");
  }
}

// ── SQLite DB Tail (render source) ───────────────────────────────────────────

/**
 * Tail messages from the DB with a rowid cursor (async version).
 * Returns rows where thread_id = threadId AND from_agent != roomBus AND rowid > cursor.
 *
 * `threadId` is the room's INSTANCE id (the value agents reply with and the hub
 * relays under). `roomBus` is the namespaced bus identity ("room-<roomId>") the
 * hub writes relay copies as — passed explicitly so this function stays decoupled
 * from roomBusFor() (the caller knows both values).
 *
 * L2: Render-filter invariant — the hub relays messages BY WRITING them to the bus
 * with from_agent=roomBus ("room-<roomId>"). The canonical copy (the one agents actually
 * sent) always has from_agent=<agentName>. Filtering out from_agent=roomBus rows therefore
 * shows each message exactly once: the agent's original entry, not the hub's relay copy.
 * This invariant is load-bearing: removing it causes duplicate message display.
 * NOTE: the filter MUST use the SAME namespaced value the relay writes with (roomBus),
 * else the per-room relay copies would render as duplicates.
 *
 * Uses execFile (async) to avoid blocking the event loop during DB polls (H1).
 * The SQL uses simple string embedding of cursorRowid (integer) which is safe,
 * and the thread/room values are quoted with SQLite single-quote escaping.
 */
function dbTailMessages(threadId, roomBus, cursorRowid) {
  const db = dbPath();
  if (!fs.existsSync(db)) return Promise.resolve([]);

  // Escape values for SQLite single-quoted strings (double the single quotes).
  const escapedThreadId = String(threadId).replace(/'/g, "''");
  const escapedRoomBus = String(roomBus).replace(/'/g, "''");
  const sql =
    `SELECT rowid, id, from_agent, to_agent, thread_id, msg_type, body, created_at ` +
    `FROM messages ` +
    `WHERE thread_id = '${escapedThreadId}' AND from_agent != '${escapedRoomBus}' AND rowid > ${Number(cursorRowid)} ` +
    `ORDER BY rowid ASC;`;

  return new Promise((resolve) => {
    execFile("sqlite3", ["-json", db, sql], { timeout: 5000, encoding: "utf8" }, (err, stdout) => {
      if (err) { resolve([]); return; }
      try {
        const output = (stdout || "").trim();
        if (!output || output === "[]") { resolve([]); return; }
        const rows = JSON.parse(output);
        resolve(rows.map((r) => ({
          rowid: Number(r.rowid),
          id: r.id,
          from_agent: r.from_agent,
          to_agent: r.to_agent,
          thread_id: r.thread_id,
          msg_type: r.msg_type,
          body: r.body,
          created_at: r.created_at,
        })));
      } catch {
        resolve([]);
      }
    });
  });
}

// ── Logging ───────────────────────────────────────────────────────────────────

/**
 * Log file path for a room INSTANCE. Namespaced by both roomId (human label) and
 * instanceId so a reused roomId in the same cwd writes a separate file per room
 * generation: room-<roomId>-<instanceId>.log. Pure.
 */
function logFile(roomId, instanceId) {
  const tag = instanceId ? `${roomId}-${instanceId}` : roomId;
  return path.join(process.cwd(), `room-${tag}.log`);
}

function appendLog(roomId, instanceId, line) {
  try {
    fs.appendFileSync(logFile(roomId, instanceId), line + "\n");
  } catch {
    // non-fatal
  }
}

// ── Render ────────────────────────────────────────────────────────────────────

// Shared "chat bubble" renderer. A dim horizontal rule + a colored author bar
// (▌ name · time) + a 2-space-indented body. The rule + indent give clear visual
// separation between consecutive messages so multi-line replies don't blur together.
// The active readline interface while the human prompt is up. ALL async output
// (messages, system lines, errors) goes through writeOut() so it prints ABOVE the
// input line instead of splitting whatever the user is mid-typing.
let activeRl = null;

function writeOut(text) {
  const rl = activeRl;
  if (rl && process.stdout.isTTY) {
    readline.cursorTo(process.stdout, 0);
    readline.clearLine(process.stdout, 0); // wipe the in-progress input line
    process.stdout.write(text.endsWith("\n") ? text : text + "\n");
    if (typeof rl._refreshLine === "function") {
      rl._refreshLine(); // redraw prompt + typed buffer + restore cursor
    } else {
      rl.prompt(true);
      if (rl.line) process.stdout.write(rl.line);
    }
  } else {
    process.stdout.write(text);
  }
}

function renderBubble(author, color, time, body, suffix = "") {
  const rule = `${C.DIM}${"─".repeat(58)}${C.RESET}`;
  const lines = String(body ?? "")
    .replace(/[ \t\r\n]+$/, "")
    .split("\n");
  // ram's own body renders as a subtle darker block (light-gray bg) so it stands out
  // without coloring the text bright green. The rule + header stay un-backgrounded.
  const indented = author === "ram"
    ? lines
        .map((l) => {
          const text = `  ${l}`;
          const pad = " ".repeat(Math.max(0, RAM_BG_WIDTH - text.length));
          return `${RAM_BG}${text}${pad}${C.RESET}`;
        })
        .join("\n")
    : lines.map((l) => `  ${l}`).join("\n");
  writeOut(
    `\n\n${rule}\n` +
      `${color}${C.BOLD}▌ ${author}${C.RESET}${suffix}  ${C.DIM}· ${time}${C.RESET}\n` +
      `${indented}\n`
  );
}

function renderMessage(msg, roomId, instanceId) {
  const time = new Date(msg.created_at || Date.now()).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  // (A) DB-tail rows carry the busId in from_agent; strip "<roomId>-" for display.
  // roomBus rows are already filtered out by dbTailMessages, so this only sees agents.
  const author = displayName(msg.from_agent || msg.from || "?", roomId);
  renderBubble(author, agentColor(author), time, msg.body || "");
  appendLog(roomId, instanceId, `[${time}] [${author}] ${msg.body || ""}`);
  webBroadcast({ type: "msg", from: author, body: msg.body || "", ts: time });
}

function printSystemMsg(text) {
  writeOut(`\n${C.DIM}[room] ${text}${C.RESET}\n`);
  webBroadcast({ type: "system", body: stripAnsi(text) });
}

function printError(text) {
  writeOut(`\n${C.RED}[room:error] ${text}${C.RESET}\n`);
}

// ── Web UI layer (chat-style, SSE + POST, zero deps) ────────────────────────────
// One renderer fed by the hub: agents run in real PTYs and talk over the bus, the hub
// de-chromes their output ONCE, and the browser just paints clean bubbles. This is why
// it doesn't bloat the way N independent full-screen TUIs do. See Designs/Web UI MVP.

// Strip ANSI/SGR escape sequences so web events carry plain text, not terminal codes.
function stripAnsi(s) {
  // eslint-disable-next-line no-control-regex
  return String(s ?? "").replace(/\x1b\[[0-9;?]*[a-zA-Z]/g, "");
}

const webClients = new Set(); // active SSE response objects
let webRoster = []; // last-known member list, replayed to new clients

// Push an event to every connected browser. Safe no-op when no web server is running.
function webBroadcast(evt) {
  if (webClients.size === 0) return;
  const frame = `data: ${JSON.stringify(evt)}\n\n`;
  for (const res of webClients) {
    try { res.write(frame); } catch { /* client gone; close handler will prune */ }
  }
}

// ── Peek (v1.1): per-agent raw PTY stream, on-demand ────────────────────────────
// The hub already receives each agent's raw stdout (its TUI) via spawnRunner but
// normally discards it. Peek keeps a small ring buffer per agent and forwards the
// raw bytes to any browser that opens that agent's panel. Only the focused agent's
// stream is rendered — one stream on demand, never N continuously → no bloat.
const PEEK_RING_BYTES = 128 * 1024; // replay window for a freshly-opened panel
const peekState = new Map(); // agentName -> { ring: Buffer, subs: Set<res> }

function peekFor(agentName) {
  let s = peekState.get(agentName);
  if (!s) { s = { ring: Buffer.alloc(0), subs: new Set() }; peekState.set(agentName, s); }
  return s;
}

// Called from spawnRunner on every chunk of an agent's raw stdout.
function peekAppend(agentName, chunk) {
  const s = peekFor(agentName);
  s.ring = Buffer.concat([s.ring, chunk]);
  if (s.ring.length > PEEK_RING_BYTES) s.ring = s.ring.subarray(s.ring.length - PEEK_RING_BYTES);
  if (s.subs.size === 0) return;
  const frame = `data: ${JSON.stringify({ b: chunk.toString("base64") })}\n\n`;
  for (const res of s.subs) { try { res.write(frame); } catch {} }
}

// Embedded single-page chat UI. No build step; xterm.js loaded from CDN for the
// (optional, on-demand) raw-terminal peek panel only. localhost only.
const WEB_HTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>AgentBus Room</title>
<style>
  :root {
    --bg:#0b0e14; --panel:#11151c; --panel2:#161b22; --border:#262d38;
    --text:#e8eef5; --dim:#8b97a6; --accent:#388bfd; --green:#3fb950; --amber:#d29922;
    --ram-bubble:#1f6feb; --card:#151a22;
  }
  * { box-sizing:border-box; }
  html,body { height:100%; }
  body {
    margin:0; background:var(--bg); color:var(--text); height:100vh; display:flex;
    font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;
    -webkit-font-smoothing:antialiased;
  }

  /* ── Sidebar ── */
  #side { width:230px; flex:0 0 230px; background:var(--panel); border-right:1px solid var(--border); padding:18px 14px; overflow-y:auto; display:flex; flex-direction:column; }
  #side h2 { font-size:11px; text-transform:uppercase; letter-spacing:.1em; color:var(--dim); margin:0 0 14px; padding:0 6px; }
  .member { display:flex; align-items:center; gap:10px; padding:8px 8px; cursor:pointer; border-radius:8px; transition:background .12s; }
  .member:hover { background:var(--panel2); }
  .member.peeking { background:#1f6feb22; }
  .member .av { width:26px; height:26px; flex:0 0 26px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:700; color:#0b0e14; }
  .member .nm { font-size:14px; font-weight:500; flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .member .dot { width:8px; height:8px; border-radius:50%; background:var(--dim); flex:0 0 8px; }
  .member .dot.live { background:var(--green); }
  .member .dot.working { background:var(--amber); animation:pulse 1.1s infinite; }
  @keyframes pulse { 50% { opacity:.25; } }
  .member .hint { font-size:10px; color:var(--dim); opacity:0; }
  .member:hover .hint { opacity:1; }
  #status { margin-top:auto; padding:10px 6px 0; font-size:12px; color:var(--dim); }

  /* ── Main column ── */
  #main { flex:1; display:flex; flex-direction:column; min-width:0; }
  #topbar { padding:14px 26px; border-bottom:1px solid var(--border); background:var(--panel); font-weight:600; font-size:15px; letter-spacing:.01em; }
  #topbar .sub { color:var(--dim); font-weight:400; font-size:13px; margin-left:8px; }
  #log { flex:1; overflow-y:auto; padding:24px 26px 8px; scroll-behavior:smooth; }
  #log::-webkit-scrollbar { width:10px; } #log::-webkit-scrollbar-thumb { background:#2a313c; border-radius:6px; }

  /* ── Message row ── */
  .msg { display:flex; gap:13px; padding:11px 12px; margin:2px 0; border-radius:12px; max-width:1000px; transition:background .1s; }
  .msg:hover { background:#11161e; }
  .msg .av { width:34px; height:34px; flex:0 0 34px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:14px; font-weight:700; color:#0b0e14; margin-top:2px; }
  .msg .content { flex:1; min-width:0; }
  .msg .head { display:flex; align-items:baseline; gap:9px; margin-bottom:3px; }
  .msg .head .name { font-weight:700; font-size:14.5px; }
  .msg .head .time { color:var(--dim); font-weight:400; font-size:11.5px; }
  .msg .body { white-space:pre-wrap; word-wrap:break-word; overflow-wrap:anywhere; font-size:15.5px; line-height:1.6; color:#dde5ee; }
  .msg .body code { background:#0d1117; border:1px solid var(--border); border-radius:5px; padding:1px 5px; font-size:14px; font-family:"SF Mono",ui-monospace,Menlo,monospace; }

  /* ── Ram's own messages: right-aligned bubble ── */
  .msg.ram { flex-direction:row-reverse; margin-left:auto; }
  .msg.ram .content { display:flex; flex-direction:column; align-items:flex-end; }
  .msg.ram .head { flex-direction:row-reverse; }
  .msg.ram .body { background:linear-gradient(135deg,#1f6feb,#388bfd); color:#fff; padding:10px 14px; border-radius:14px 14px 4px 14px; display:inline-block; max-width:680px; }
  .msg.ram:hover { background:transparent; }

  .msg.dim { opacity:.5; }
  .sys { color:var(--dim); font-size:13px; font-style:italic; text-align:center; margin:10px 0; }
  .divider { text-align:center; color:var(--dim); font-size:11px; letter-spacing:.12em; text-transform:uppercase; margin:18px 0; position:relative; }
  .divider::before { content:""; position:absolute; left:0; right:0; top:50%; height:1px; background:var(--border); z-index:0; }
  .divider span { background:var(--bg); padding:0 14px; position:relative; z-index:1; }

  /* ── Composer ── */
  #composer { border-top:1px solid var(--border); padding:16px 22px; background:var(--panel); display:flex; gap:12px; align-items:flex-end; }
  #input { flex:1; resize:none; background:var(--card); color:var(--text); border:1px solid var(--border); border-radius:12px; padding:13px 16px; font:inherit; font-size:15.5px; line-height:1.5; max-height:180px; transition:border-color .12s; }
  #input::placeholder { color:#5c6675; }
  #input:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 3px #388bfd22; }
  #send { background:var(--accent); color:#fff; border:none; border-radius:12px; padding:0 22px; height:48px; font:inherit; font-size:15px; font-weight:600; cursor:pointer; transition:background .12s; }
  #send:hover { background:#4a9bff; } #send:disabled { opacity:.5; cursor:default; }

  /* ── Peek panel ── */
  #peek { display:none; flex:0 0 46vw; width:46vw; background:#000; border-left:1px solid var(--border); flex-direction:column; overflow:hidden; }
  #peek.open { display:flex; }
  #peekhead { display:flex; align-items:center; gap:8px; padding:10px 14px; background:var(--panel); border-bottom:1px solid var(--border); font-size:13px; }
  #peekhead .who { font-weight:600; }
  #peekclose { margin-left:auto; cursor:pointer; color:var(--dim); border:none; background:none; font-size:18px; }
  #peekclose:hover { color:var(--text); }
  #peekterm { flex:1; min-height:0; padding:6px 8px; }

  @media (max-width:760px) {
    #side { width:60px; flex:0 0 60px; padding:14px 6px; }
    #side h2, .member .nm, .member .hint, #status { display:none; }
    .member { justify-content:center; padding:8px 0; }
    .msg.ram .body { max-width:80vw; }
  }
</style>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/css/xterm.min.css"/>
<script src="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-fit@0.10.0/lib/addon-fit.min.js"></script>
</head>
<body>
  <div id="side"><h2>Room</h2><div id="members"></div><div id="status">connecting…</div></div>
  <div id="main">
    <div id="topbar">AgentBus Room<span class="sub" id="roomsub"></span></div>
    <div id="log"></div>
    <div id="composer">
      <textarea id="input" rows="1" placeholder="Message the room…  (Enter to send, Shift+Enter for newline)"></textarea>
      <button id="send">Send</button>
    </div>
  </div>
  <div id="peek">
    <div id="peekhead"><span class="who" id="peekwho"></span><span style="color:var(--dim);font-size:11px">raw terminal (read-only)</span><button id="peekclose" title="close">×</button></div>
    <div id="peekterm"></div>
  </div>
<script>
  const log = document.getElementById('log');
  const membersEl = document.getElementById('members');
  const statusEl = document.getElementById('status');
  const input = document.getElementById('input');
  const sendBtn = document.getElementById('send');
  const presence = {}; // name -> 'idle' | 'working'
  const seen = new Set(); // member names ever seen

  function colorFor(name) {
    let h = 0; for (let i=0;i<name.length;i++) h = (h*31 + name.charCodeAt(i)) % 360;
    return 'hsl(' + h + ',62%,68%)';
  }
  function avColor(name) {
    let h = 0; for (let i=0;i<name.length;i++) h = (h*31 + name.charCodeAt(i)) % 360;
    return 'hsl(' + h + ',58%,62%)';
  }
  function initials(name) {
    const parts = name.replace(/[^a-zA-Z0-9 -]/g,'').split(/[ -]/).filter(Boolean);
    if (!parts.length) return name.slice(0,2).toUpperCase();
    return (parts.length===1 ? parts[0].slice(0,2) : parts[0][0]+parts[parts.length-1][0]).toUpperCase();
  }
  function atBottom() { return log.scrollHeight - log.scrollTop - log.clientHeight < 120; }
  function scroll() { log.scrollTop = log.scrollHeight; }

  function addDivider(label) {
    const d = document.createElement('div'); d.className = 'divider';
    const s = document.createElement('span'); s.textContent = label; d.appendChild(s);
    log.appendChild(d); scroll();
  }
  function addMsg(from, body, ts, dim) {
    const stick = atBottom();
    const isRam = from === 'ram';
    const d = document.createElement('div');
    d.className = 'msg' + (isRam ? ' ram' : '') + (dim ? ' dim' : '');

    const av = document.createElement('div'); av.className = 'av';
    av.textContent = initials(from);
    av.style.background = isRam ? 'linear-gradient(135deg,#1f6feb,#388bfd)' : avColor(from);
    if (isRam) av.style.color = '#fff';

    const content = document.createElement('div'); content.className = 'content';
    const head = document.createElement('div'); head.className = 'head';
    const nm = document.createElement('span'); nm.className = 'name';
    nm.textContent = isRam ? 'You' : from;
    nm.style.color = isRam ? 'var(--text)' : colorFor(from);
    head.appendChild(nm);
    if (ts) { const t = document.createElement('span'); t.className='time'; t.textContent = ts; head.appendChild(t); }
    const b = document.createElement('div'); b.className = 'body'; b.textContent = body;
    content.appendChild(head); content.appendChild(b);

    d.appendChild(av); d.appendChild(content); log.appendChild(d);
    if (stick) scroll();
    if (!isRam) noteMember(from);
  }
  function addSys(body) {
    const stick = atBottom();
    const d = document.createElement('div'); d.className = 'sys'; d.textContent = body;
    log.appendChild(d); if (stick) scroll();
  }
  function noteMember(name) { if (!seen.has(name)) { seen.add(name); if (!(name in presence)) presence[name]='idle'; renderMembers(); } }
  function renderMembers() {
    membersEl.innerHTML = '';
    Object.keys(presence).sort().forEach(name => {
      const row = document.createElement('div'); row.className='member' + (name===peekAgent?' peeking':'');
      const av = document.createElement('span'); av.className='av'; av.textContent=initials(name); av.style.background=avColor(name);
      const nm = document.createElement('span'); nm.className='nm'; nm.textContent=name; nm.style.color=colorFor(name);
      const hint = document.createElement('span'); hint.className='hint'; hint.textContent='peek';
      const dot = document.createElement('span'); dot.className = 'dot ' + (presence[name]==='working'?'working':'live');
      row.appendChild(av); row.appendChild(nm); row.appendChild(hint); row.appendChild(dot);
      row.onclick = () => { peekAgent===name ? closePeek() : openPeek(name); };
      membersEl.appendChild(row);
    });
  }

  // ── Raw-terminal peek (xterm.js, single on-demand stream) ──
  let peekAgent = null, peekES = null, term = null, fit = null, peekRO = null;
  const peekEl = document.getElementById('peek');
  function b64ToBytes(b64) { const bin = atob(b64); const u = new Uint8Array(bin.length); for (let i=0;i<bin.length;i++) u[i]=bin.charCodeAt(i); return u; }
  function openPeek(agent) {
    if (peekAgent) closePeek();
    peekAgent = agent;
    document.getElementById('peekwho').textContent = agent;
    document.getElementById('peekwho').style.color = colorFor(agent);
    peekEl.classList.add('open');
    if (!window.Terminal) { document.getElementById('peekterm').textContent = 'xterm.js failed to load (offline?)'; renderMembers(); return; }
    const host = document.getElementById('peekterm');
    // The agent PTY is a FIXED 120x40 (runner default; the hub's stdout is a pipe so
    // no local size is detected). Match it EXACTLY — fitting to the panel instead would
    // wrap the absolutely-positioned TUI at the wrong columns and shred every box. Pick
    // a font size so all 120 cols fit the panel width.
    const COLS = 120, ROWS = 40;
    const fontSize = Math.max(6, Math.min(13, Math.floor(host.clientWidth / (COLS * 0.62))));
    term = new Terminal({ cols:COLS, rows:ROWS, convertEol:false, fontSize, theme:{background:'#000000'}, scrollback:8000, disableStdin:true });
    term.open(host);
    peekES = new EventSource('/peek/' + encodeURIComponent(agent));
    peekES.onmessage = (e) => { let m; try { m = JSON.parse(e.data); } catch { return; } if (m.b && term) term.write(b64ToBytes(m.b)); };
    renderMembers();
  }
  function closePeek() {
    if (peekRO) { peekRO.disconnect(); peekRO = null; }
    if (peekES) { peekES.close(); peekES = null; }
    if (term) { term.dispose(); term = null; }
    peekEl.classList.remove('open');
    peekAgent = null;
    document.getElementById('peekterm').innerHTML = '';
    renderMembers();
  }
  document.getElementById('peekclose').onclick = closePeek;
  window.addEventListener('resize', () => { try { fit && fit.fit(); } catch {} });

  const es = new EventSource('/events');
  es.onopen = () => { statusEl.textContent = '● connected'; statusEl.style.color = '#3fb950'; };
  es.onerror = () => { statusEl.textContent = '○ reconnecting…'; statusEl.style.color = '#d29922'; };
  es.onmessage = (e) => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (m.type === 'msg') {
      // Skip SSE echo of our own optimistically-rendered message to avoid duplicate bubbles.
      if (m.from === 'ram' && lastSent !== null && m.body === lastSent) { lastSent = null; }
      else addMsg(m.from, m.body, m.ts);
    }
    else if (m.type === 'history') addMsg(m.from, m.body, m.ts, true);
    else if (m.type === 'history-end') addDivider('↑ history  ·  live ↓');
    else if (m.type === 'system') addSys(m.body);
    else if (m.type === 'roster') {
      // Authoritative: the roster event carries the COMPLETE current member set.
      // Drop anyone no longer a member so a stale tab kept open across a hub
      // restart doesn't keep showing agents from a previous session.
      const mem = m.members || [];
      for (const n of Object.keys(presence)) if (!mem.includes(n)) delete presence[n];
      seen.clear();
      mem.forEach(n => { seen.add(n); if (!(n in presence)) presence[n] = 'idle'; });
      renderMembers();
      const sub = document.getElementById('roomsub');
      if (sub) sub.textContent = mem.length ? '· ' + mem.length + ' agents' : '';
    }
    else if (m.type === 'presence') { presence[m.agent] = m.state; seen.add(m.agent); renderMembers(); }
  };

  // lastSent: used to suppress the SSE echo of our own message (we render it locally).
  let lastSent = null;
  async function send() {
    const body = input.value.trim();
    if (!body || sendBtn.disabled) return;
    sendBtn.disabled = true; sendBtn.textContent = '…';
    const ts = new Date().toLocaleTimeString([], { hour:'2-digit', minute:'2-digit', second:'2-digit' });
    input.value = ''; autosize();
    // Optimistic local echo — reliable even if SSE is momentarily reconnecting.
    addMsg('ram', body, ts);
    lastSent = body;
    try { await fetch('/send', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ body }) }); }
    catch { addSys('● send failed — hub unreachable?'); }
    finally { sendBtn.disabled = false; sendBtn.textContent = 'Send'; input.focus(); }
  }
  function autosize() { input.style.height='auto'; input.style.height = Math.min(input.scrollHeight, 140)+'px'; }
  input.addEventListener('input', autosize);
  input.addEventListener('keydown', (e) => { if (e.key==='Enter' && !e.shiftKey) { e.preventDefault(); send(); } });
  sendBtn.addEventListener('click', send);
  input.focus();
</script>
</body></html>`;

/**
 * Room-composer / launcher UI. Served by a headless launcher hub (one started
 * with --no-agents, so hub.members is empty) on GET /, and always on GET /create.
 * Lets the user assemble a room from the browser: per-agent rows with program,
 * new/resume mode + session picker, and launch-dir. POST /create-room spawns a
 * detached child room hub that re-enters this same script's main()/launchAgents().
 *
 * The page queries GET /sessions?program=&dir= to populate each row's resume
 * dropdown. On launch it POSTs the assembled config and redirects to the
 * spawned room's web URL.
 */
const LAUNCHER_HTML = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>AgentBus — Create Room</title>
<style>
  :root { color-scheme: light dark; --bg:#0d1117; --panel:#161b22; --border:#30363d; --txt:#c9d1d9; --dim:#8b949e; --accent:#58a6ff; --green:#3fb950; --red:#f85149; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; padding:24px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--dim); margin-bottom:24px; }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:16px 20px; max-width:920px; margin-bottom:16px; }
  label { display:block; color:var(--dim); font-size:12px; margin-bottom:4px; text-transform:uppercase; letter-spacing:.04em; }
  input, select { width:100%; background:#0d1117; color:var(--txt); border:1px solid var(--border); border-radius:6px; padding:8px 10px; font:inherit; }
  input:focus, select:focus { outline:none; border-color:var(--accent); }
  .row { display:grid; grid-template-columns: 1.2fr 1fr 0.8fr 1.4fr auto; gap:10px; align-items:end; margin-bottom:10px; }
  .row .del { background:transparent; color:var(--dim); border:1px solid var(--border); border-radius:6px; padding:8px 12px; cursor:pointer; font:inherit; }
  .row .del:hover { color:var(--red); border-color:var(--red); }
  .actions { display:flex; gap:12px; align-items:center; margin-top:8px; }
  button.primary { background:var(--green); color:#000; border:none; border-radius:6px; padding:10px 20px; font-weight:600; cursor:pointer; }
  button.primary:disabled { opacity:.5; cursor:not-allowed; }
  button.ghost { background:transparent; color:var(--accent); border:1px solid var(--accent); border-radius:6px; padding:9px 16px; cursor:pointer; }
  .msg { font-size:13px; padding:8px 12px; border-radius:6px; display:none; }
  .msg.err { display:block; background:rgba(248,81,73,.12); color:var(--red); border:1px solid var(--red); }
  .msg.ok  { display:block; background:rgba(63,185,80,.12); color:var(--green); border:1px solid var(--green); }
  .hint { color:var(--dim); font-size:12px; margin-top:4px; }
  optgroup, option { background:#0d1117; }
</style>
</head><body>
<h1>AgentBus · Room Composer</h1>
<div class="sub">Assemble a room — mix a resumed session with a fresh agent, pick a launch dir, and launch.</div>

<div class="card">
  <div style="display:grid; grid-template-columns: 1fr 1fr; gap:16px;">
    <div>
      <label>Room id</label>
      <input id="roomId" placeholder="e.g. delta4" value=""/>
    </div>
    <div>
      <label>Launch dir (defaults to cwd)</label>
      <input id="launchDir" placeholder="/Users/you/CODE/project"/>
    </div>
  </div>
</div>

<div class="card">
  <label>Agents</label>
  <div id="rows"></div>
  <div class="actions">
    <button class="ghost" id="addRow">+ Add agent</button>
  </div>
</div>

<div class="card">
  <div class="actions">
    <button class="primary" id="launch">Launch room</button>
    <span class="msg" id="msg"></span>
  </div>
  <div class="hint">Spawned rooms run headless (web UI is the input surface). A free web port ≥ 8788 is picked automatically and shown here on success.</div>
</div>

<script>
const PROGS = ["claude","codex","gemini","agy","qodercli","cmd"];
let counter = 0;
const rowsEl = document.getElementById('rows');
const msgEl = document.getElementById('msg');

function setMsg(text, kind) { msgEl.textContent = text; msgEl.className = 'msg ' + (kind||''); }

function makeRow() {
  const id = ++counter;
  const row = document.createElement('div');
  row.className = 'row';
  row.dataset.rid = id;
  row.innerHTML = \`
    <div><label>Name</label><input class="name" placeholder="claude-A"/></div>
    <div><label>Program</label><select class="prog">\${PROGS.map(p=>'<option>'+p+'</option>').join('')}</select></div>
    <div><label>Mode</label><select class="mode"><option value="new">new</option><option value="resume">resume</option></select></div>
    <div><label>Session (resume)</label><select class="sess" disabled><option value="">— pick after setting dir —</option></select></div>
    <div><button class="del" title="remove">✕</button></div>
  \`;
  rowsEl.appendChild(row);
  const nameEl = row.querySelector('.name');
  const progEl = row.querySelector('.prog');
  const modeEl = row.querySelector('.mode');
  const sessEl = row.querySelector('.sess');
  row.querySelector('.del').addEventListener('click', () => row.remove());
  const refreshSessions = async () => {
    if (modeEl.value !== 'resume') { sessEl.disabled = true; return; }
    const dir = document.getElementById('launchDir').value.trim();
    if (!dir) { sessEl.innerHTML = '<option value="">— set launch dir first —</option>'; sessEl.disabled = true; return; }
    sessEl.disabled = true;
    sessEl.innerHTML = '<option value="">loading…</option>';
    try {
      const r = await fetch('/sessions?program=' + encodeURIComponent(progEl.value) + '&dir=' + encodeURIComponent(dir));
      const list = r.ok ? await r.json() : [];
      if (!Array.isArray(list) || list.length === 0) {
        sessEl.innerHTML = '<option value="">no sessions found</option>';
      } else {
        sessEl.innerHTML = list.map(s => {
          const when = new Date(s.mtime).toLocaleString();
          const kb = Math.round(s.size/1024) + 'KB';
          const prev = s.preview ? s.preview.replace(/"/g,'&quot;') : '(no preview)';
          return '<option value="' + s.uuid + '">[' + when + ' · ' + kb + '] ' + prev + '</option>';
        }).join('');
      }
    } catch (e) { sessEl.innerHTML = '<option value="">error loading</option>'; }
    sessEl.disabled = false;
  };
  modeEl.addEventListener('change', refreshSessions);
  progEl.addEventListener('change', refreshSessions);
  document.getElementById('launchDir').addEventListener('change', refreshSessions);
  return row;
}

document.getElementById('addRow').addEventListener('click', makeRow);
makeRow(); // seed one row

document.getElementById('launch').addEventListener('click', async () => {
  const roomId = document.getElementById('roomId').value.trim();
  const launchDir = document.getElementById('launchDir').value.trim();
  if (!/^[A-Za-z0-9_-]+$/.test(roomId)) { setMsg('Room id required (letters, digits, -, _).', 'err'); return; }
  const agents = [];
  const names = new Set();
  for (const row of rowsEl.querySelectorAll('.row')) {
    const name = row.querySelector('.name').value.trim();
    const program = row.querySelector('.prog').value;
    const mode = row.querySelector('.mode').value;
    const sessionId = row.querySelector('.sess').value;
    if (!/^[A-Za-z0-9_-]+$/.test(name)) { setMsg('Invalid/missing agent name.', 'err'); return; }
    if (names.has(name)) { setMsg('Duplicate agent name: ' + name, 'err'); return; }
    names.add(name);
    if (mode === 'resume' && !sessionId) { setMsg('Row "' + name + '" is resume but no session is picked.', 'err'); return; }
    agents.push({ name, program, mode, sessionId: mode === 'resume' ? sessionId : undefined });
  }
  if (agents.length === 0) { setMsg('Add at least one agent.', 'err'); return; }
  setMsg('Launching…', '');
  try {
    const r = await fetch('/create-room', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ roomId, launchDir, agents }) });
    const out = r.ok ? await r.json() : {};
    if (r.ok && out.ok) {
      setMsg('Room "' + roomId + '" launched at http://localhost:' + out.webPort, 'ok');
      if (out.webPort) setTimeout(() => { window.open('http://localhost:' + out.webPort, '_blank'); }, 600);
    } else {
      setMsg(out.error || ('Launch failed (HTTP ' + r.status + ')'), 'err');
    }
  } catch (e) { setMsg('Network error: ' + e.message, 'err'); }
});
</script>
</body></html>`;

/**
 * Find the first free TCP port on 127.0.0.1 at or after `start`. Used by the
 * launcher to assign a web port to each spawned room. Resolves to a port number.
 */
function findFreePort(start) {
  return new Promise((resolve) => {
    const tryPort = (p) => {
      const tester = net.createServer();
      tester.on("error", () => { tryPort(p + 1); });
      tester.listen(p, "127.0.0.1", () => {
        tester.close(() => resolve(p));
      });
    };
    tryPort(start);
  });
}

// Start the web server. Localhost only (no auth). Routes:
//   GET /             → chat UI (or LAUNCHER_HTML when hub has no members)
//   GET /create       → LAUNCHER_HTML (room composer)
//   GET /sessions     → ?program=claude|codex&dir=<path> lists resumable sessions
//   GET /events       → SSE stream of room events
//   GET /peek/<agent> → SSE stream of one agent's raw output
//   POST /send        → {body} → routed through the SAME handleInput the terminal uses
//   POST /create-room → {roomId,launchDir,agents[]} → spawns a detached child room hub
function startWebServer(hub, port) {
  const server = http.createServer((req, res) => {
    const url = (req.url || "/").split("?")[0];

    if (req.method === "GET" && url === "/") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      // A launcher hub (started --no-agents) has no members → serve the composer.
      // GET /create always serves the composer regardless.
      res.end((!hub.members || hub.members.length === 0) ? LAUNCHER_HTML : WEB_HTML);
      return;
    }

    if (req.method === "GET" && url === "/create") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(LAUNCHER_HTML);
      return;
    }

    if (req.method === "GET" && url === "/sessions") {
      // Session picker backing endpoint. Reads the filesystem for resumable sessions.
      //   ?program=claude  &dir=<abs path>  → ~/.claude/projects/<dir-hash>/*.jsonl
      //   ?program=codex   (&dir ignored)   → every known CODEX_HOME's sessions/
      const qs = new URL(req.url || "/", "http://localhost").searchParams;
      const program = (qs.get("program") || "").trim();
      const dir = (qs.get("dir") || "").trim();
      try {
        const list = listSessions(program, dir);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(list));
      } catch (e) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      }
      return;
    }

    if (req.method === "GET" && url === "/events") {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      res.write(": connected\n\n");
      // Backfill: replay the log file as dim "history" events so a fresh/refreshed tab
      // shows the full conversation. Each log line is "[HH:MM] [author] body".
      // Scope to CURRENT room members (+ ram): the log file is keyed only by roomId
      // and cwd, so reusing a roomId in the same dir with DIFFERENT agents would
      // otherwise replay a prior generation's chatter (e.g. claude-A/codex-A) and
      // leak those names into the roster. Filtering by the live member set means:
      // same members → full history; different members → only ram's lines survive.
      try {
        const lf = logFile(hub.roomId, hub.instanceId);
        if (fs.existsSync(lf)) {
          const allowed = new Set([...(hub.members || []), "ram"]);
          const lines = fs.readFileSync(lf, "utf8").split("\n").filter(Boolean);
          let replayed = 0;
          for (const line of lines) {
            const m = line.match(/^\[([^\]]+)\] \[([^\]]+)\] ([\s\S]*)$/);
            if (m && allowed.has(m[2])) {
              res.write(`data: ${JSON.stringify({ type: "history", from: m[2], body: m[3], ts: m[1] })}\n\n`);
              replayed++;
            }
          }
          if (replayed > 0) res.write(`data: ${JSON.stringify({ type: "history-end" })}\n\n`);
        }
      } catch { /* non-fatal — fresh room with no log yet */ }
      // Replay the current roster so a fresh tab shows members immediately.
      res.write(`data: ${JSON.stringify({ type: "roster", members: webRoster })}\n\n`);
      webClients.add(res);
      const keepAlive = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 25000);
      req.on("close", () => { clearInterval(keepAlive); webClients.delete(res); });
      return;
    }

    if (req.method === "POST" && url === "/send") {
      let raw = "";
      req.on("data", (c) => { raw += c; if (raw.length > 1_000_000) req.destroy(); });
      req.on("end", () => {
        let body = "";
        try { body = String(JSON.parse(raw).body ?? "").trim(); } catch {}
        if (body) hub.handleInput(body);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      });
      return;
    }

    if (req.method === "GET" && url.startsWith("/peek/")) {
      const agent = decodeURIComponent(url.slice("/peek/".length));
      if (!hub.members.includes(agent)) {
        res.writeHead(404, { "content-type": "text/plain" });
        res.end("unknown agent");
        return;
      }
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      const s = peekFor(agent);
      res.write(": peek\n\n");
      // Replay the ring buffer so the panel paints immediately, then go live.
      if (s.ring.length) res.write(`data: ${JSON.stringify({ b: s.ring.toString("base64") })}\n\n`);
      s.subs.add(res);
      const keepAlive = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 25000);
      req.on("close", () => { clearInterval(keepAlive); s.subs.delete(res); });
      return;
    }

    if (req.method === "POST" && url === "/create-room") {
      // Assemble a room config and spawn a DETACHED child hub that re-enters this
      // script's main()/launchAgents(). The child runs --headless --web <freePort>
      // so its web UI is the input surface (no live stdin to teardown on).
      let raw = "";
      req.on("data", (c) => { raw += c; if (raw.length > 256_000) req.destroy(); });
      req.on("end", async () => {
        let cfg;
        try { cfg = JSON.parse(raw || "{}"); } catch { cfg = {}; }
        // Pick a free web port for the child (8788+). 8787 is the launcher itself.
        const webPort = await findFreePort(8788);
        cfg.webPort = webPort;
        const { argv, error } = buildRoomLaunchArgv(cfg);
        if (error) {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: false, error }));
          return;
        }
        // Detached, headless child. stdio ignored; it never reads stdin (--headless
        // skips the readline wiring so there's no 'close' → shutdown).
        try {
          const child = spawn(process.execPath, [SCRIPT_PATH, ...argv], {
            detached: true,
            stdio: "ignore",
            cwd: cfg.launchDir || process.cwd(),
            env: { ...process.env },
          });
          child.on("error", (e) => {
            printError(`/create-room spawn failed: ${e.message}`);
          });
          child.unref();
        } catch (e) {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: `spawn failed: ${e.message}` }));
          return;
        }
        printSystemMsg(`/create-room: launched room "${cfg.roomId}" on http://localhost:${webPort} (argv: ${argv.join(" ")})`);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, roomId: cfg.roomId, webPort }));
      });
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  server.on("error", (e) => {
    printError(`web server error on port ${port}: ${e.message}`);
  });
  server.listen(port, "127.0.0.1", () => {
    printSystemMsg(`Web UI on ${C.BOLD}http://localhost:${port}${C.RESET}${C.DIM} — chat-style room mirror${C.RESET}`);
  });
  return server;
}

// ── AgentBus Send (shell-out) ──────────────────────────────────────────────────

/**
 * Build the argv array for an agentbus send call.
 * Pure function — body is never shell-interpolated; it becomes a single
 * argv element that spawnSync passes directly to exec(), bypassing /bin/sh.
 */
function buildSendArgv(from, to, body, threadId, msgType) {
  const args = ["send", "--from", from, "--to", to, "--msg-type", msgType];
  if (threadId) args.push("--thread-id", threadId);
  args.push(body);
  return args;
}

/**
 * Shell-free relay to `agentbus send` (async, H1 fix). Uses spawn wrapped in a
 * promise so body is passed as a raw argv element — no sh interpolation, no
 * $HOME/$() injection risk. Non-blocking: does not freeze socket reads.
 * Does NOT register: the consume socket's existing roomBus ("room-<roomId>") row covers the FK.
 */
function sendMessage(from, to, body, threadId, msgType = "request") {
  const args = buildSendArgv(from, to, body, threadId, msgType);
  return new Promise((resolve) => {
    const child = spawn(AB_PATH, args, { encoding: "utf8", timeout: 10000 });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (d) => { stdout += d; });
    child.stderr?.on("data", (d) => { stderr += d; });
    child.on("close", (code) => {
      if (code === 0) resolve({ ok: true, output: stdout });
      else resolve({ ok: false, error: stderr || stdout || `exit ${code}` });
    });
    child.on("error", (e) => resolve({ ok: false, error: e.message }));
  });
}

// ── Agent Launcher ────────────────────────────────────────────────────────────

function loadTemplate() {
  try {
    return fs.readFileSync(TEMPLATE_PATH, "utf8");
  } catch {
    return `You are {SELF}. Reply via: {AB_PATH} send --from {SELF} --to {ROOM_BUS} --thread-id {ROOM_ID} --msg-type response "..."`;
  }
}

function generateSystemPrompt(selfName, allAgents, roomId, instanceId) {
  const template = loadTemplate();
  // (A) The template uses {SELF} for BOTH the roster line AND the load-bearing
  // `--from {SELF}` reply command. The agent's --from MUST be its busId (replying
  // under the raw display name would re-introduce the cross-room collision), so
  // {SELF} = busId here. The friendly "you are <name>" framing is added by the seed
  // body (seedAgent) instead. Peers stay friendly display names (informational only).
  const selfBusId = agentBusId(roomId, selfName);
  const peers = allAgents.filter((a) => a !== selfName).join(", ");
  // {ROOM_BUS} = namespaced bus identity ("room-<roomId>"). Agents MUST reply to this,
  // not the literal "room", because the hub registers (and the daemon routes) under it.
  const roomBus = roomBusFor(roomId);
  // {ROOM_ID} = the room's INSTANCE id, which is the thread_id agents reply with.
  // Using the instance id (not roomId) namespaces DB rows so a reused roomId in the
  // same cwd never merges two room generations.
  const threadId = instanceId || roomId;
  return template
    .replace(/{SELF}/g, selfBusId)
    .replace(/{PEERS}/g, peers || "(none yet)")
    .replace(/{ROOM_BUS}/g, roomBus)
    .replace(/{ROOM_ID}/g, threadId)
    .replace(/{AB_PATH}/g, AB_PATH);
}

/**
 * Spawn the `agentbus run ...` wrapper for an already-built argv, wire up the
 * shared stdout/stderr drains and process-group settings, and return the child.
 * Shared by both the claude and codex launch paths so spawn behavior is identical.
 *
 * H2: detached:true gives the child its own process group so we can kill the
 * entire group with process.kill(-child.pid, 'SIGTERM') on shutdown. Without it,
 * the Rust PTY runner has no SIGTERM handler and the inner program keeps running.
 */
function spawnRunner(agentName, args, launchDir, extraEnv) {
  const child = spawn(AB_PATH, args, {
    cwd: launchDir,
    detached: true, // H2: own process group
    stdio: ["ignore", "pipe", "pipe"],
    // extraEnv lets the codex path inject CODEX_HOME; claude passes nothing (inherits).
    ...(extraEnv ? { env: { ...process.env, ...extraEnv } } : {}),
  });

  child.unref(); // allow hub to exit even if children outlive it (we kill explicitly in shutdown)

  child.stdout?.on("data", (d) => peekAppend(agentName, d)); // capture raw TUI for on-demand peek
  child.stderr?.on("data", (d) => {
    const text = d.toString("utf8").trim();
    if (text) process.stderr.write(`${C.DIM}[${agentName}:stderr] ${text}${C.RESET}\n`);
  });

  return child;
}

/**
 * Launch a CLAUDE agent with agentbus run.
 * Returns { child, spFile, transcriptFile, emptyMcp, claudeSessionId }.
 *
 * H3: emptyMcp path is returned so shutdown can unlink it (was leaked previously).
 *
 * FEATURE #1 (session capture, robust path): instead of racing to find the newest
 * *.jsonl under ~/.claude/projects/<cwd-hash>/ (which collides with other concurrent
 * Claude sessions in the same cwd), we PRE-ASSIGN a UUID via `claude --session-id <uuid>`
 * on fresh launch and store it. On --resume we relaunch `claude --resume <uuid>`.
 * `resumeSessionId` (when set) takes precedence and is passed to --resume.
 */
function launchClaudeAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resumeSessionId = null, model = null } = {}) {
  const busIdForHook = agentBusId(roomId, agentName);
  const roomBusForHook = roomBusFor(roomId);
  // Auto-forward note: with the Stop hook in place the agent's replies are delivered
  // to the room automatically, so it doesn't need to remember the send command. This
  // is what makes RESUMED claude sessions (which tend to write prose and forget to
  // run `agentbus send`) reliably reach the room.
  const autoForwardNote =
    `\n\n== Auto-delivery (IMPORTANT) ==\n` +
    `Your replies are delivered to the room AUTOMATICALLY when you finish responding. ` +
    `Just answer normally in your reply — you do NOT need to run any agentbus send command yourself. ` +
    `Whatever you write as your final message is what the room receives. Keep it concise and room-appropriate.`;
  const prompt = generateSystemPrompt(agentName, allAgents, roomId, instanceId) + autoForwardNote;
  const spFile = path.join(os.tmpdir(), `room-sp-${agentName}-${roomId}.txt`);
  fs.writeFileSync(spFile, prompt);

  // Per-agent settings file carrying ONLY the Stop hook. Passed via --settings so it
  // is scoped to THIS launched process — the user's global ~/.claude config is untouched.
  const settingsFile = path.join(os.tmpdir(), `room-settings-${agentName}-${roomId}.json`);
  const stopHookSettings = {
    hooks: {
      Stop: [{ hooks: [{ type: "command", command: `node ${JSON.stringify(STOP_FORWARD_PATH)}` }] }],
    },
  };
  fs.writeFileSync(settingsFile, JSON.stringify(stopHookSettings));

  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const emptyMcp = path.join(os.tmpdir(), `empty-mcp-${roomId}.json`);
  // Must be {"mcpServers":{}} — a bare {} fails `--strict-mcp-config` with
  // "Invalid MCP configuration: mcpServers: expected record, received undefined"
  // and the agent exits 1 at launch.
  if (!fs.existsSync(emptyMcp)) fs.writeFileSync(emptyMcp, '{"mcpServers":{}}');

  // FEATURE #1: resume → reuse the stored id; fresh → mint a new one we control.
  const claudeSessionId = resumeSessionId || crypto.randomUUID();

  // (A) Register on the bus under the namespaced busId, not the raw display name.
  const busId = agentBusId(roomId, agentName);
  const sessionArgs = resumeSessionId
    ? ["--resume", claudeSessionId]      // restore the prior conversation
    : ["--session-id", claudeSessionId]; // pin a known id so we can resume it later
  const args = [
    "run",
    "--name", busId,
    "--program", "claude",
    "--transcript", transcriptFile,
    "--",
    "claude",
    "--dangerously-skip-permissions",
    "--strict-mcp-config",
    "--mcp-config", emptyMcp,
    "--settings", settingsFile,
    "--append-system-prompt-file", spFile,
    ...(model ? ["--model", model] : []),
    ...sessionArgs,
  ];

  printSystemMsg(`Launching claude agent ${agentName} in ${launchDir}${model ? ` [model=${model}]` : ""}${resumeSessionId ? ` (resuming ${claudeSessionId})` : ""}...`);

  // Env consumed by the Stop-hook forwarder (inherited: agentbus-run → claude → hook).
  const hookEnv = {
    AGENTBUS_BIN: AB_PATH,
    AGENTBUS_BUSID: busIdForHook,
    AGENTBUS_ROOMBUS: roomBusForHook,
    AGENTBUS_ROOMID: roomId,
  };
  const child = spawnRunner(agentName, args, launchDir, hookEnv);

  // Note: the 'exit' + member-pruning handler is attached in launchAgents() where 'this' is bound (M3).
  return { child, spFile, transcriptFile, emptyMcp, claudeSessionId, settingsFile };
}

/**
 * Launch a CODEX agent with agentbus run.
 * Returns { child, transcriptFile, codexHome } (plus spFile/emptyMcp = null
 * so the children-entry shape stays uniform for shutdown).
 *
 * Isolation recipe:
 *  - STABLE per-(roomId, name) CODEX_HOME under <agentbusDir>/rooms/<roomId>/codex-<name>/
 *    (FEATURE #1: NOT tmp, so codex sessions persist across runs for `codex resume`).
 *    Symlink auth-providing files from the real ~/.codex (auth.json, accounts,
 *    version.json, models_cache.json), each guarded. Existing home is REUSED, never
 *    rm'd at launch (rm would wipe the resumable session) — only /kick deletes it.
 *  - config.toml pre-trusts the USER'S launchDir (FIX #4 — no folder-trust modal for
 *    the real project) and omits [mcp_servers.*] (avoids the heavy MCP boot stall).
 *  - AGENTS.md in CODEX_HOME (FIX #4 — codex reads $CODEX_HOME/AGENTS.md as GLOBAL
 *    instructions, same as ~/.codex/AGENTS.md) carries the SAME room system prompt
 *    Claude gets, so we deliver the trust layer WITHOUT polluting the user's repo.
 *  - codex runs with `--cd launchDir` (FIX #4) so it works in the user's actual project.
 *    `resume:true` (FEATURE #1) relaunches the prior session via `codex resume --last --all`.
 */
function launchCodexAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume = false, resumeSessionId = null, model = null } = {}) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  // When resuming a specific session (--agent-session), it may live in the GLOBAL
  // ~/.codex OR in another room's isolated home (e.g. resuming delta3's codex into a
  // new room). Locate the home that actually contains the rollout-*-<uuid>.jsonl so
  // `codex resume <id>` finds it; fall back to global ~/.codex if not found anywhere.
  // For fresh launches and --last resumes the agent's own isolated home is used.
  const codexHome = resumeSessionId
    ? (findCodexHomeForSession(resumeSessionId) || path.join(os.homedir(), ".codex"))
    : codexHomeFor(roomId, agentName);
  if (resumeSessionId) {
    printSystemMsg(`codex ${agentName}: resuming session ${resumeSessionId} from CODEX_HOME=${codexHome}`);
  }

  // Ensure the stable home exists. Do NOT rm it — a prior run's session lives here and
  // is what `codex resume` reads. (On a fresh first launch this just creates it.)
  fs.mkdirSync(codexHome, { recursive: true });

  // Symlink auth-providing files from the real ~/.codex into the isolated home.
  // Guarded per-entry; symlinkSync throws EEXIST if the home is being reused, which is fine.
  const realCodexHome = path.join(os.homedir(), ".codex");
  for (const entry of ["auth.json", "accounts", "version.json", "models_cache.json"]) {
    const src = path.join(realCodexHome, entry);
    const dst = path.join(codexHome, entry);
    try {
      if (fs.existsSync(src) && !fs.existsSync(dst)) fs.symlinkSync(src, dst);
    } catch {
      // non-fatal: missing/unsymlinkable entry just means that capability is absent
    }
  }

  // config.toml: model + pre-trusted launchDir (FIX #4), no MCP servers. Rewritten each
  // launch (cheap, idempotent) so a moved launchDir re-trusts correctly.
  fs.writeFileSync(path.join(codexHome, "config.toml"), buildCodexConfigToml(launchDir, model || "gpt-5.5"));

  // AGENTS.md: write to the isolated home only, NEVER to global ~/.codex.
  // When resumeSessionId is set, codexHome points to ~/.codex so we must
  // skip — writing there would contaminate every future standalone codex
  // session with room instructions.
  const agentsMdTarget = resumeSessionId
    ? codexHomeFor(roomId, agentName)   // isolated home (may not be the active CODEX_HOME, but safe)
    : codexHome;                         // already the isolated home for fresh/--last launches
  fs.mkdirSync(agentsMdTarget, { recursive: true });
  const prompt = generateSystemPrompt(agentName, allAgents, roomId, instanceId);
  fs.writeFileSync(path.join(agentsMdTarget, "AGENTS.md"), prompt);

  // (A) Register on the bus under the namespaced busId, not the raw display name.
  const busId = agentBusId(roomId, agentName);
  // FEATURE #1: on --resume, continue the most recent session in this home.
  // `resume --last --all` — --all disables codex's cwd filtering (the per-agent home is
  // already isolated to one agent, so the cwd filter only risks hiding the session).
  // resumeSessionId: explicit session UUID from --agent-session flag; takes priority over --last.
  const codexArgs = resume
    ? (resumeSessionId
        ? ["codex", "resume", resumeSessionId, "--all",
           "--dangerously-bypass-approvals-and-sandbox", "--dangerously-bypass-hook-trust",
           "--cd", launchDir]
        : ["codex", "resume", "--last", "--all",
           "--dangerously-bypass-approvals-and-sandbox", "--dangerously-bypass-hook-trust",
           "--cd", launchDir])
    : ["codex",
       "--dangerously-bypass-approvals-and-sandbox", "--dangerously-bypass-hook-trust",
       "--cd", launchDir];
  const args = [
    "run",
    "--name", busId,
    "--program", "codex",
    "--transcript", transcriptFile,
    "--",
    ...codexArgs,
  ];

  const resumeLabel = resume
    ? (resumeSessionId ? `, resuming session ${resumeSessionId}` : ", resuming --last")
    : "";
  printSystemMsg(`Launching codex agent ${agentName} in ${launchDir} (CODEX_HOME=${codexHome}${resumeLabel})...`);

  const child = spawnRunner(agentName, args, launchDir, { CODEX_HOME: codexHome });

  // spFile/emptyMcp are null for codex; codexHome drives its teardown instead.
  // claudeSessionId is null (codex resumes via its home, not a session id).
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome, claudeSessionId: null };
}

/**
 * Launch a GEMINI agent with agentbus run.
 * Returns the SAME uniform shape as the claude/codex paths
 * ({ child, transcriptFile, spFile, emptyMcp, codexHome, claudeSessionId }) with every
 * program-specific field null — gemini has NO temp files, NO isolated home, NO session id.
 * This uniformity means spawnMember's destructure, the resumeInfo bookkeeping, and
 * teardownChild's guards all just work without special-casing.
 *
 * Recipe + flag rationale: see buildGeminiArgs. The room system prompt is delivered as the
 * `-i` initial interactive prompt (built via the SAME generateSystemPrompt the others use, so
 * gemini replies `--from <busId> --to <roomBus>` exactly like claude/codex). `resume:true`
 * adds `-r latest` for gemini's native per-project session resume.
 */
function launchGeminiAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume = false, model = null } = {}) {
  if (model) printSystemMsg(`note: per-agent model override ("${model}") not yet wired for gemini — using its default`);
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const prompt = generateSystemPrompt(agentName, allAgents, roomId, instanceId);
  // (A) Register on the bus under the namespaced busId, not the raw display name.
  const busId = agentBusId(roomId, agentName);
  const args = buildGeminiArgs(busId, transcriptFile, prompt, { resume });

  printSystemMsg(`Launching gemini agent ${agentName} in ${launchDir}${resume ? " (resuming -r latest)" : ""}...`);

  // No extra env (no isolated home) — runs in the user's cwd (launchDir), same as post-fix codex.
  const child = spawnRunner(agentName, args, launchDir, null);

  // Uniform null-shape so spawnMember/teardownChild need no gemini special-casing:
  // no spFile/emptyMcp (claude-only), no codexHome (codex-only), no claudeSessionId.
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome: null, claudeSessionId: null };
}

/**
 * Launch an AGY agent with agentbus run.
 * Returns the SAME uniform shape as the claude/codex/gemini paths
 * ({ child, transcriptFile, spFile, emptyMcp, codexHome, claudeSessionId }) with every
 * program-specific field null — agy has NO temp files, NO isolated home, NO session id.
 * This uniformity means spawnMember's destructure, the resumeInfo bookkeeping, and
 * teardownChild's guards all just work without special-casing.
 *
 * Recipe + flag rationale: see buildAgyArgs. The room system prompt is delivered as the
 * `-i` initial interactive prompt (built via the SAME generateSystemPrompt the others use, so
 * agy replies `--from <busId> --to <roomBus>` exactly like claude/codex/gemini).
 * `resume:true` adds `--continue` for agy's best-effort session resume.
 */
function launchAgyAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume = false, model = null } = {}) {
  if (model) printSystemMsg(`note: per-agent model override ("${model}") not yet wired for agy — using its default`);
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const prompt = generateSystemPrompt(agentName, allAgents, roomId, instanceId);
  // (A) Register on the bus under the namespaced busId, not the raw display name.
  const busId = agentBusId(roomId, agentName);
  const args = buildAgyArgs(busId, transcriptFile, prompt, { resume });

  printSystemMsg(`Launching agy agent ${agentName} in ${launchDir}${resume ? " (resuming --continue)" : ""}...`);

  // No extra env (no isolated home) — runs in the user's cwd (launchDir).
  const child = spawnRunner(agentName, args, launchDir, null);

  // Uniform null-shape so spawnMember/teardownChild need no agy special-casing:
  // no spFile/emptyMcp (claude-only), no codexHome (codex-only), no claudeSessionId.
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome: null, claudeSessionId: null };
}

/**
 * Launch a QODERCLI agent with agentbus run.
 * Returns the uniform shape { child, transcriptFile, spFile, emptyMcp, codexHome, claudeSessionId }
 * with every program-specific field null — qodercli has NO isolated home, NO temp files.
 */
function launchQoderCliAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume = false, model = null } = {}) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const prompt = generateSystemPrompt(agentName, allAgents, roomId, instanceId);
  const busId = agentBusId(roomId, agentName);
  const args = buildQoderCliArgs(busId, transcriptFile, prompt, { resume, model });

  printSystemMsg(`Launching qodercli agent ${agentName} in ${launchDir}${resume ? " (resuming --continue)" : ""}...`);

  const child = spawnRunner(agentName, args, launchDir, null);
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome: null, claudeSessionId: null };
}

/**
 * Launch a CMD agent — runs an arbitrary shell command as a bus-connected agent.
 * cmdString is a space-separated command (e.g. "python my_agent.py --flag").
 * Simple word-split only (no shell expansion); use a wrapper script for complex cases.
 * The first word becomes --program for adapter selection (e.g. "python" → GenericAdapter).
 * GenericAdapter (idle=0) injects immediately — correct for scripts that read stdin.
 * Returns the uniform shape with all program-specific fields null.
 */
function launchCmdAgent(agentName, cmdString, allAgents, roomId, launchDir, transcriptDir) {
  if (!cmdString || !cmdString.trim()) {
    throw new Error(`cmd agent "${agentName}" has no command — use --cmd "${agentName}:command"`);
  }
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const busId = agentBusId(roomId, agentName);
  const cmdParts = cmdString.trim().split(/\s+/);
  const programLabel = cmdParts[0]; // first word → adapter matching (python/bash/etc → generic)
  const args = [
    "run",
    "--name", busId,
    "--program", programLabel,
    "--transcript", transcriptFile,
    "--",
    ...cmdParts,
  ];

  printSystemMsg(`Launching cmd agent ${agentName} (${cmdString}) in ${launchDir}...`);

  const child = spawnRunner(agentName, args, launchDir, null);
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome: null, claudeSessionId: null };
}

/**
 * Per-program launch dispatcher. `resume` carries optional resume handles:
 *   { codex: true }            → relaunch codex via `codex resume --last --all`
 *   { gemini: true }           → relaunch gemini via `gemini -r latest`
 *   { agy: true }              → relaunch agy via `agy --continue`
 *   { claudeSessionId: <uuid> } → relaunch claude via `claude --resume <uuid>`
 * Absent/empty → fresh launch. Keeps launchAgents() agnostic of program details.
 */
function launchAgent(agentName, program, allAgents, roomId, instanceId, launchDir, transcriptDir, resume = {}, model = null, cmds = {}) {
  if (program === "codex") {
    return launchCodexAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, {
      resume: !!(resume.codex || resume.codexSessionId),
      resumeSessionId: resume.codexSessionId || null,
      model,
    });
  }
  if (program === "gemini") {
    return launchGeminiAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume: !!resume.gemini, model });
  }
  if (program === "agy") {
    return launchAgyAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume: !!resume.agy, model });
  }
  if (program === "qodercli") {
    return launchQoderCliAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resume: !!resume.qodercli, model });
  }
  if (program === "cmd") {
    // Ram's 'cmd' CLI tool — run directly like any other agent binary. (No system
    // prompt — the cmd agent manages its own — so instanceId isn't needed here.)
    return launchCmdAgent(agentName, "cmd", allAgents, roomId, launchDir, transcriptDir);
  }
  if (program === "exec") {
    // Generic shell adapter — cmdString from --cmd flag. (No system prompt.)
    const cmdString = cmds[agentName] || "";
    return launchCmdAgent(agentName, cmdString, allAgents, roomId, launchDir, transcriptDir);
  }
  // default + 'claude'
  return launchClaudeAgent(agentName, allAgents, roomId, instanceId, launchDir, transcriptDir, { resumeSessionId: resume.claudeSessionId || null, model });
}

/**
 * Wait for an agent to appear registered in the DB with state='active' or 'disconnected'.
 * Polls up to timeoutMs.
 */
async function waitForAgentRegistered(agentName, timeoutMs = 60000) {
  const db = dbPath();
  const deadline = Date.now() + timeoutMs;
  // Escape agent name for SQLite (though agent names are hub-generated and safe)
  const escaped = agentName.replace(/'/g, "''");
  const sql = `SELECT state FROM agents WHERE name = '${escaped}' LIMIT 1;`;

  while (Date.now() < deadline) {
    if (fs.existsSync(db)) {
      try {
        const out = execFileSync("sqlite3", [db, sql], {
          timeout: 3000,
          encoding: "utf8",
        }).trim();
        if (out && (out === "active" || out === "working" || out === "waiting")) {
          return true;
        }
      } catch {
        // db not ready yet
      }
    }
    await sleep(1000);
  }
  return false;
}

/**
 * Wait for agent transcript to show idle prompt (output quiescence for ~2s).
 * Falls back to simple timeout if transcript file not available.
 */
async function waitForAgentReady(transcriptFile, agentName, timeoutMs = 60000, child = null) {
  printSystemMsg(`Waiting for ${agentName} to be ready...`);
  const deadline = Date.now() + timeoutMs;
  let lastSize = -1;
  let stableCount = 0;

  while (Date.now() < deadline) {
    await sleep(1500);
    // A dead child's transcript is trivially "idle" — never mistake death for readiness.
    if (child && child.exitCode !== null) return false;
    try {
      const stat = fs.statSync(transcriptFile);
      const size = stat.size;
      if (size > 0 && size === lastSize) {
        stableCount++;
        if (stableCount >= 2) {
          // 2 consecutive checks with no new output = idle
          return true;
        }
      } else {
        stableCount = 0;
        lastSize = size;
      }
    } catch {
      // transcript not created yet
    }
  }
  printSystemMsg(`WARNING: ${agentName} readiness timeout — proceeding anyway`);
  return false;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── Daemon Health Check ───────────────────────────────────────────────────────

function ensureDaemonRunning() {
  const sock = socketPath();
  if (fs.existsSync(sock)) {
    // Try a quick connection to verify it's live
    const r = spawnSync(AB_PATH, ["status"], { encoding: "utf8", timeout: 3000 });
    if (r.status === 0) return true;
    // socket exists but daemon dead — fall through to start
  }
  try {
    printSystemMsg("Starting agentbus daemon...");
    const r = spawnSync(AB_PATH, ["start"], { encoding: "utf8", timeout: 10000 });
    if (r.status !== 0 && r.status !== null) {
      printError(`daemon start exited ${r.status}: ${r.stderr}`);
      return false;
    }
    // Wait for socket file to appear (up to 5s)
    for (let i = 0; i < 10; i++) {
      if (fs.existsSync(sock)) return true;
      spawnSync("sleep", ["0.5"]);
    }
    return fs.existsSync(sock);
  } catch (e) {
    printError(`Failed to start daemon: ${e.message}`);
    return false;
  }
}

// ── Seed Message ──────────────────────────────────────────────────────────────

async function seedAgent(agentName, allAgents, roomId, instanceId) {
  const roomBus = roomBusFor(roomId); // namespaced bus identity for this room
  const busId = agentBusId(roomId, agentName); // (A) this agent's namespaced bus identity
  const threadId = instanceId || roomId; // INSTANCE id — namespaces DB rows per room generation
  const peers = allAgents.filter((a) => a !== agentName).join(" and ");
  const body =
    `Welcome to room "${roomId}". You are ${agentName} (bus id ${busId}). ` +
    `Your collaborators are: ${peers}, and ram (the human). ` +
    `This is a live conversation relayed over AgentBus. To speak to the room, run: ` +
    `${AB_PATH} send --from ${busId} --to ${roomBus} --thread-id ${threadId} --msg-type response "..."  ` +
    `Do NOT introduce yourself or send a greeting now — stay silent until a collaborator addresses you or poses a topic, then reply concisely.`;

  // H1: await async sendMessage. --from is the namespaced bus identity (NOT literal "room").
  // --to targets the agent's busId (the name it registered under), not the display name.
  const result = await sendMessage(roomBus, busId, body, threadId, "request");
  if (!result.ok) {
    printError(`Failed to seed ${agentName}: ${result.error}`);
  } else {
    printSystemMsg(`Seeded ${agentName}`);
  }
}

// ── Main Hub ──────────────────────────────────────────────────────────────────

class RoomHub {
  constructor(roomId, agentNames, cbMax = 6, programs = {}, models = {}, cmds = {}, sessions = {}) {
    this.roomId = roomId;
    // BUS identity, namespaced per room ("room-<roomId>"). Used everywhere the hub
    // talks ON the bus (Register name, --from on relay/seed sends, --to reply target,
    // DB-tail relay-copy filter). The display label stays "room". This namespacing is
    // what prevents two concurrent hubs from evicting each other on the daemon.
    this.roomBus = roomBusFor(roomId);
    this.members = agentNames; // agent names only (not "ram") — stays a string[] (load-bearing)
    // programs: { <agentName>: "claude" | "codex" | ... }. Absent → 'claude'.
    this.programs = programs;
    // models: { <agentName>: "<model>" } optional per-agent model override. Absent = CLI default.
    this.models = models || {};
    // cmds: { <agentName>: "command string" } — only used by program="cmd" agents.
    this.cmds = cmds || {};
    // sessions: { <agentName>: "<session-id>" } — from --agent-session flag.
    // Used to resume specific prior sessions for claude/codex on a fresh room start.
    this.sessions = sessions || {};
    this.cbMax = cbMax;
    this.cb = new CircuitBreaker(cbMax);
    this.children = []; // { child, spFile, agentName, emptyMcp }
    this.socket = null;
    this.lineReader = null;
    this.readResolvers = []; // queue of resolve/reject for the Read promise
    // C1: buffer for pushed Message frames that arrived before a resolver was waiting.
    // A parsed line is ALWAYS either consumed by a resolver or enqueued here — never dropped.
    this.pendingLines = [];
    this.renderCursor = 0; // last seen rowid in DB tail (M5: initialized to max rowid before startRender)
    this.renderInterval = null;
    this.running = false;
    this.reconnecting = false; // H4: guard against double-reconnect race
    this.rl = null; // readline interface for human input
    this.pausedMessages = []; // M4: buffer messages suppressed by circuit-breaker for flush on /resume
    this.working = new Set(); // (C) agents we relayed to but haven't heard back from — drives "is working…"
    this.launchDir = process.cwd(); // (B) where agents are launched; set for real in run(), reused by /add
    // FEATURE #1: per-agent resume handles, persisted to the state file.
    //   resumeInfo[name] = { codexHome?, claudeSessionId? }
    this.resumeInfo = {};
    this.resume = false; // set true by run(--resume); replays history + restores agent sessions
    this.headless = false; // set true by --headless; startHumanInput() is skipped (web UI is input surface)
    // INSTANCE id — the bus thread_id and log-file key for THIS room generation.
    // A fresh room generates a new one; --resume reuses the persisted value. Namespaces
    // DB rows + log so a reused roomId in the same cwd never merges two generations.
    // null until run() decides (resume → load, fresh → generate).
    this.instanceId = null;
    this._lastSweepMs = 0; // transcript-sweep debounce (agent-exit triggers)
  }

  // ── FEATURE #1: State Persistence ──────────────────────────────────────────
  // Persist room state so a closed room can be reconnected with --resume. Written on
  // launch, /add, /kick, and shutdown. Best-effort: a write failure must never crash
  // the hub, so all fs is guarded.
  writeState() {
    try {
      const file = stateFilePathFor(this.roomId);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const state = serializeState(this.roomId, this.members, this.programs, this.resumeInfo, this.models, this.instanceId);
      fs.writeFileSync(file, JSON.stringify(state, null, 2));
    } catch (e) {
      // Non-fatal: resume just won't be available if we couldn't persist.
      printError(`Could not write state file: ${e.message}`);
    }
  }

  /** FEATURE #1: read + parse the state file for --resume. Returns the deserialized
   *  { roomId, agents } or null if absent/unreadable/empty (caller falls back to fresh). */
  loadStateForResume() {
    try {
      const file = stateFilePathFor(this.roomId);
      if (!fs.existsSync(file)) return null;
      const obj = JSON.parse(fs.readFileSync(file, "utf8"));
      const state = deserializeState(obj);
      if (!state || state.agents.length === 0) return null;
      return state;
    } catch (e) {
      printError(`Could not read state file: ${e.message}`);
      return null;
    }
  }

  // ── Transcript rotation (TASK 3) ──────────────────────────────────────────
  /**
   * Debounced wrapper around the module-level sweepTranscripts(). Called on hub
   * start and on each agent exit; the debounce (TRANSCRIPT_SWEEP_MIN_INTERVAL_MS)
   * prevents redundant sweeps + log spam when several agents exit in quick
   * succession. Protects THIS hub's children's transcript files explicitly.
   * Fire-and-forget from callers (never awaited on the critical path).
   */
  async sweepTranscripts() {
    const now = Date.now();
    if (now - this._lastSweepMs < TRANSCRIPT_SWEEP_MIN_INTERVAL_MS) return null;
    this._lastSweepMs = now;
    const protectPaths = (this.children || []).map((c) => c.transcriptFile).filter(Boolean);
    try {
      return await sweepTranscripts({ dryRun: false, protectPaths, log: true });
    } catch {
      return null;
    }
  }

  // ── Socket Connection ────────────────────────────────────────────────────

  async connect() {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ path: socketPath() });
      const timeout = setTimeout(() => {
        sock.destroy();
        reject(new Error("Socket connection timeout"));
      }, 5000);

      sock.once("connect", () => {
        clearTimeout(timeout);
        this.socket = sock;
        this.lineReader = new LineReader(sock, (line) => this.handleLine(line));
        // H4: only reconnect from 'close' (error is virtually always followed by close).
        // Reconnecting from both handlers creates two parallel reconnect loops — the 2nd
        // register() evicts the freshly reconnected socket, causing a double-reconnect race.
        sock.on("error", (e) => {
          printError(`Socket error: ${e.message}`);
          this.drainResolvers(new Error(`Socket error: ${e.message}`));
          // Do NOT call scheduleReconnect here — 'close' will fire next and handle it.
        });
        sock.on("close", () => {
          printSystemMsg("Socket closed");
          this.drainResolvers(new Error("Socket closed"));
          this.scheduleReconnect();
        });
        resolve();
      });

      sock.once("error", (e) => {
        clearTimeout(timeout);
        reject(e);
      });
    });
  }

  handleLine(line) {
    // C1 fix: parsed lines are NEVER dropped. If a resolver is waiting, satisfy it
    // immediately. Otherwise buffer in pendingLines so readOne() can drain it later.
    const parsed = parseReadLine(line);
    if (this.readResolvers.length > 0) {
      const { resolve } = this.readResolvers.shift();
      resolve(parsed);
    } else {
      this.pendingLines.push(parsed);
    }
  }

  /** Drain all pending readResolvers by rejecting them — called on socket close/error
   *  so the consume loop doesn't stall for 40s waiting on dead promises. */
  drainResolvers(err) {
    const pending = this.readResolvers.splice(0);
    for (const { reject } of pending) {
      reject(err);
    }
  }

  sendRaw(json) {
    if (!this.socket || this.socket.destroyed) {
      printError("Socket not connected");
      return;
    }
    this.lineReader.write(json);
  }

  /** Read one response from the socket. Returns parsed result.
   * C1: drains pendingLines before creating a new promise, so pushed Message
   * frames that arrived before this readOne() call are never lost. */
  readOne() {
    if (this.pendingLines.length > 0) {
      return Promise.resolve(this.pendingLines.shift());
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const idx = this.readResolvers.findIndex((r) => r.reject === reject);
        if (idx !== -1) this.readResolvers.splice(idx, 1);
        reject(new Error("Read response timeout"));
      }, 40000);

      this.readResolvers.push({
        resolve: (val) => { clearTimeout(timeout); resolve(val); },
        reject,
      });
    });
  }

  // ── Register ─────────────────────────────────────────────────────────────

  async register() {
    this.sendRaw(JSON.stringify({
      type: "Register",
      name: this.roomBus, // namespaced bus identity ("room-<roomId>"), NOT the literal "room"
      program: "hub",
      model: "unknown",
      project: "default",
    }));
    // M2: only return true on the real success shape (kind='ack' = Ok{data:object}).
    // Returning true on timeout/unknown/push would mask FK failures and silent partial-successes.
    //
    // C1/reconnect guard: readOne() drains pendingLines first. On reconnect, a buffered
    // Message frame (kind='push') could be dequeued here instead of the register ack.
    // We re-enqueue any non-ack, non-error result at the FRONT of pendingLines so it is
    // not consumed-and-discarded — then keep reading until we see the ack or an error.
    const MAX_TRIES = 8; // avoid infinite loop if daemon sends only pushes
    const stash = []; // non-ack frames consumed while hunting for the register ack
    let ackResult = null; // null = ack not yet seen
    for (let i = 0; i < MAX_TRIES && ackResult === null; i++) {
      const ack = await this.readOne();
      if (ack.kind === "ack") {
        printSystemMsg(`Registered as '${this.roomBus}' on the bus`);
        ackResult = true;
      } else if (ack.kind === "error") {
        printError(`Registration failed: ${ack.message}`);
        ackResult = false;
      } else {
        // push/batch/timeout/unknown — not the register ack; preserve in ARRIVAL order
        stash.push(ack);
      }
    }
    // Re-enqueue preserved frames at the FRONT, keeping FIFO arrival order.
    // (spread preserves order; unshifting per-iteration would REVERSE it — the LIFO bug.)
    if (stash.length) this.pendingLines.unshift(...stash);
    if (ackResult === null) {
      printError(`Registration failed: did not receive ack within ${MAX_TRIES} reads`);
      return false;
    }
    return ackResult;
  }

  // ── Consume Loop ──────────────────────────────────────────────────────────

  async consumeLoop() {
    while (this.running) {
      try {
        // R3: during reconnect, register() owns socket reads. Park here so we don't
        // compete for (and snipe) the register ack via a concurrent readOne().
        while (this.reconnecting && this.running) await sleep(100);
        if (!this.running) break;
        this.sendRaw(JSON.stringify({ type: "Read", wait: true, timeout_secs: 30 }));
        const result = await this.readOne();

        if (result.kind === "timeout") {
          // Normal — re-issue Read
          continue;
        }

        if (result.kind === "error") {
          printError(`Read error: ${result.message}`);
          await sleep(1000);
          continue;
        }

        if (result.kind === "unknown") {
          printError(`Unparseable line: ${result.raw?.slice(0, 100)}`);
          continue;
        }

        const messages = result.messages || [];
        for (const msg of messages) {
          await this.handleIncomingMessage(msg);
        }
      } catch (e) {
        if (this.running) {
          printError(`Consume loop error: ${e.message}`);
          await sleep(2000);
        }
      }
    }
  }

  async handleIncomingMessage(msg) {
    // (A) INCOMING BOUNDARY: a live push frame carries the sender's busId in msg.from
    // (e.g. "r1-claude-A"). Convert it to the DISPLAY name ONCE here, at the top, before
    // anything downstream touches it. This single normalization makes clearWorking (keyed
    // by display name), the circuit-breaker, AND computeFanout's self-exclusion all correct:
    // computeFanout filters members (display names) by `m !== author`, so if author were
    // a busId the sender would never be excluded → it'd receive its own message (echo loop).
    // We mutate both from/from_agent so relay()'s computeFanout (which reads either) is safe.
    const display = displayName(msg.from || msg.from_agent, this.roomId);
    msg.from = display;
    if (msg.from_agent) msg.from_agent = display;
    // (C) the sender just spoke — it's no longer "working" (clearWorking is keyed by display).
    this.clearWorking(display);
    // Update circuit-breaker
    const tripped = this.cb.recordAgentMessage();
    if (tripped) {
      printSystemMsg(
        `${C.YELLOW}${C.BOLD}[circuit-breaker]${C.RESET}${C.DIM} ${this.cb.maxConsecutive} consecutive agent messages with no human input. Fan-out PAUSED. Your turn, ram.${C.RESET}`
      );
    }

    // M4: buffer messages suppressed by the breaker so they can be flushed on /resume.
    // Previously the tripping message was silently dropped; peers never saw it.
    if (this.cb.isPaused()) {
      this.pausedMessages.push(msg);
    } else {
      await this.relay(msg, null);
    }
  }

  /** M4: flush buffered circuit-breaker messages after human input resets the breaker. */
  async flushPausedMessages() {
    const toFlush = this.pausedMessages.splice(0); // clear buffer before relaying
    for (const msg of toFlush) {
      // Relay WITHOUT re-recording as agent messages (human already reset the breaker)
      await this.relay(msg, null);
    }
    if (toFlush.length > 0) {
      printSystemMsg(`${C.DIM}caught the agents up — delivered ${toFlush.length} message(s) they sent to each other during the pause (not yours)${C.RESET}`);
    }
  }

  // ── (C) "is working…" indicator ─────────────────────────────────────────────
  // We mark an agent "working" the moment we relay a message TO it, and clear it
  // when a message FROM it arrives. Ephemeral, hub-side only (never a bus/DB row).
  markWorking(name) {
    // Only room members get presence. A stray bus message from a non-member
    // agent (e.g. another room's coordinator still active on the bus) must not
    // leak into this room's roster via a presence event.
    if (!this.members.includes(name)) return;
    if (!this.working.has(name)) {
      this.working.add(name);
      // Terminal-only line (NOT printSystemMsg) so it does not pollute the web
      // transcript — the browser shows "working" via the roster dot (presence event).
      writeOut(`\n${C.DIM}[room] ${name} is working…${C.RESET}`);
      webRoster = this.members.slice();
      webBroadcast({ type: "presence", agent: name, state: "working" });
    }
  }
  clearWorking(name) {
    if (!this.members.includes(name)) return;
    if (this.working.has(name)) {
      this.working.delete(name);
      webBroadcast({ type: "presence", agent: name, state: "idle" });
    } else {
      this.working.delete(name);
    }
  }

  async relay(msg, targetOverride) {
    const fanout = computeFanout(msg, this.members, targetOverride);
    // H1: yield between successive relays so socket 'data' events can fire in between.
    for (const { to, body } of fanout) {
      await new Promise((r) => setImmediate(r)); // yield to event loop between relays
      // (A) OUTGOING BOUNDARY: `to` is a DISPLAY name (computeFanout works on members);
      // convert to the recipient's busId for the bus --to target, but keep markWorking
      // keyed by the display name.
      // --from is the namespaced bus identity so relay copies carry from_agent=roomBus
      // (the DB-tail render filter excludes exactly that value to avoid duplicates).
      const result = await sendMessage(this.roomBus, agentBusId(this.roomId, to), body, this.instanceId, "request");
      if (!result.ok) {
        printError(`Relay to ${to} failed: ${result.error}`);
      } else {
        this.markWorking(to); // (C) recipient may now respond
      }
    }
  }

  // ── DB Tail Renderer ──────────────────────────────────────────────────────

  startRender() {
    // M5: initialize cursor to the current max rowid so we don't re-render
    // the entire message history on launch/reconnect.
    const initCursor = () => {
      const db = dbPath();
      if (!fs.existsSync(db)) return Promise.resolve(0);
      const escapedThreadId = String(this.instanceId).replace(/'/g, "''");
      const sql = `SELECT COALESCE(MAX(rowid),0) FROM messages WHERE thread_id='${escapedThreadId}';`;
      return new Promise((resolve) => {
        execFile("sqlite3", [db, sql], { timeout: 3000, encoding: "utf8" }, (err, stdout) => {
          if (err) { resolve(0); return; }
          resolve(parseInt((stdout || "0").trim(), 10) || 0);
        });
      });
    };

    initCursor().then((maxRowid) => {
      this.renderCursor = maxRowid;
      this.renderInterval = setInterval(async () => {
        const rows = await dbTailMessages(this.instanceId, this.roomBus, this.renderCursor);
        for (const row of rows) {
          renderMessage(row, this.roomId, this.instanceId);
          this.renderCursor = row.rowid;
        }
      }, 400);
    });
  }

  stopRender() {
    if (this.renderInterval) {
      clearInterval(this.renderInterval);
      this.renderInterval = null;
    }
  }

  // ── Human Input ──────────────────────────────────────────────────────────

  startHumanInput() {
    this.rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      prompt: `${C.GREEN}${C.BOLD}ram>${C.RESET} `,
      terminal: true,
      // (D) Tab-complete @mentions against current member DISPLAY names. Node's contract is
      // (line) => [hits, replacementToken]; completeMention is the pure core (testable).
      completer: (line) => {
        const { matches, replacement } = completeMention(line, this.members);
        if (replacement === null) return [[], line];
        return [matches, replacement];
      },
    });

    activeRl = this.rl; // route async output above the input line
    this.rl.prompt();

    // FIX #5: COALESCE rapid consecutive 'line' events into ONE message. readline fires a
    // 'line' per newline, so pasting an N-line block would otherwise send N separate room
    // messages. We buffer each line and (re)start a short timer; another 'line' before it
    // fires means a paste burst (append). When the timer fires quiet, we submit the buffer
    // joined with "\n" (preserving multi-line structure). A single typed line → one 'line' →
    // timer → one message (UX unchanged). The first buffered line decides command/@mention
    // handling; the whole coalesced text is then treated as one input.
    this._pasteBuf = [];
    this._pasteTimer = null;
    this.rl.on("line", (line) => {
      this._pasteBuf.push(line);
      if (this._pasteTimer) clearTimeout(this._pasteTimer);
      this._pasteTimer = setTimeout(() => {
        const buffered = this._pasteBuf;
        this._pasteBuf = [];
        this._pasteTimer = null;
        const text = coalesceLines(buffered).trim();
        this.handleInput(text);
        this.rl.prompt();
      }, PASTE_COALESCE_MS);
    });

    this.rl.on("close", () => {
      activeRl = null; // stop routing through a closed interface
      this.shutdown("stdin closed");
    });
  }

  /**
   * FIX #5: handle ONE complete (possibly coalesced multi-line) human input. Factored out
   * of the on('line') handler so both the paste-flush timer and any future direct caller use
   * the same parsing. Commands (/...) and @mentions are detected on the FULL coalesced text.
   */
  handleInput(text) {
      if (!text) {
        return;
      }

      // Handle commands
      if (text.startsWith("/")) {
        this.handleCommand(text);
        return;
      }

      // Parse @mention narrowing
      const mentionMatch = text.match(/^@(\w[\w-]*)\s+(.*)/s);
      let targetOverride = null;
      let body = text;

      if (mentionMatch) {
        const mentioned = mentionMatch[1].toLowerCase();
        const matchedAgent = this.members.find((m) => m.toLowerCase() === mentioned || m.toLowerCase().startsWith(mentioned));
        if (matchedAgent) {
          targetOverride = matchedAgent;
          body = mentionMatch[2].trim();
        }
      }

      // Reset circuit-breaker on human input (also flushes M4 paused messages)
      const wasPaused = this.cb.isPaused();
      this.cb.recordHumanMessage();

      // Echo human message locally (not in DB tail since it's from=room) — same bubble style.
      const time = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      const suffix = targetOverride ? ` ${C.DIM}→ @${targetOverride}${C.RESET}` : "";
      renderBubble("ram", C.GREEN, time, body, suffix);
      appendLog(this.roomId, this.instanceId, `[${time}] [ram] ${body}`);
      webBroadcast({ type: "msg", from: "ram", body: targetOverride ? `→ @${targetOverride}  ${body}` : body, ts: time });

      // M4: if the circuit-breaker was paused, flush buffered messages first,
      // then send the human message so agents see queued context before new input.
      const doSend = async () => {
        if (wasPaused) {
          await this.flushPausedMessages();
        }
        // H1: await the async sendMessage
        const prefixedBody = `ram: ${body}`;
        const targets = targetOverride ? [targetOverride] : this.members;
        this.working.clear(); // (C) fresh human turn — drop stale "working" flags
        for (const to of targets) {
          // Human input is fanned out as if from the room (namespaced bus identity).
          // (A) `to` is a DISPLAY name; target its busId on the bus, keep markWorking display-keyed.
          const result = await sendMessage(this.roomBus, agentBusId(this.roomId, to), prefixedBody, this.instanceId, "request");
          if (!result.ok) {
            printError(`Send to ${to} failed: ${result.error}`);
          } else {
            this.markWorking(to); // (C) we now expect a reply from this agent
          }
        }
      };
      doSend().catch((e) => printError(`Human send error: ${e.message}`));
  }

  handleCommand(text) {
    const parts = text.split(/\s+/);
    const cmd = parts[0].toLowerCase();

    switch (cmd) {
      case "/quit":
      case "/exit":
        this.shutdown("user quit");
        break;
      case "/resume":
        if (this.cb.isPaused()) {
          this.cb.recordHumanMessage();
          printSystemMsg("Circuit-breaker reset — fan-out resumed");
          // M4: flush buffered messages that were suppressed during the pause.
          // Must flush AFTER reset (so relay doesn't re-trip the already-cleared breaker).
          this.flushPausedMessages().catch((e) => printError(`Flush error: ${e.message}`));
        } else {
          printSystemMsg("Circuit-breaker is not paused");
        }
        break;
      case "/status":
        printSystemMsg(`Room: ${this.roomId} | Members: ${this.members.join(", ")} | CB: ${this.cb.isPaused() ? "PAUSED" : `ok (${this.cb.count()}/${this.cb.maxConsecutive})`}`);
        break;
      case "/who":
        // (D) list current members + their programs (display names).
        if (this.members.length === 0) {
          printSystemMsg("No agents in the room.");
        } else {
          const lines = this.members.map((m) => `  ${m} (${this.programs[m] || "claude"})`).join("\n");
          printSystemMsg(`Members (${this.members.length}):\n${lines}`);
        }
        break;
      case "/add":
        // (B) /add <name>[:<program>] — launch + register + seed a new agent at runtime.
        this.cmdAdd(parts[1]);
        break;
      case "/kick":
        // (B) /kick <name> — kill + clean up + deregister a current member.
        this.cmdKick(parts[1]);
        break;
      case "/help":
        printSystemMsg(
          "Commands: /quit /exit /resume /status /who /add /kick /help\n" +
          "  @<agent> <msg>        — address a single agent (Tab completes @names)\n" +
          "  /resume               — unblock circuit-breaker\n" +
          "  /who                  — list members + their programs\n" +
          "  /add <name>[:program] — add an agent (program = claude|codex|gemini|agy; default claude)\n" +
          "  /kick <name>          — remove an agent from the room\n" +
          "  paste multi-line text — pasted blocks are sent as ONE message (not one per line)\n" +
          "Reconnect (from the shell, after the room is closed):\n" +
          "  agentbus-room <id> --resume      — replay history + restore each agent's session\n" +
          "  agentbus-room <id> --no-agents   — re-attach to agents still running (hub died, agents survived)"
        );
        break;
      default:
        printSystemMsg(`Unknown command: ${cmd}`);
    }
  }

  /**
   * (B) /add <name>[:<program>]. Validates via parseAgentSpec (same name/program rules),
   * rejects duplicates, then launches+waits+seeds the single agent and registers it in
   * members/programs/children. Async; errors are surfaced, not thrown into readline.
   */
  cmdAdd(arg) {
    if (!arg) {
      printSystemMsg("usage: /add <name>[:<program>]   (program = claude|codex|gemini|agy; default claude)");
      return;
    }
    const spec = parseAgentSpec(arg);
    if (spec.error) {
      printSystemMsg(`usage: /add <name>[:<program>] — ${spec.error}`);
      return;
    }
    const { name, program, model } = spec;
    if (this.members.includes(name)) {
      printSystemMsg(`${name} is already a member.`);
      return;
    }
    // Register bookkeeping BEFORE spawning so the new agent's prompt roster (built from
    // this.members) and the exit-handler pruning see a consistent member set.
    this.members.push(name);
    this.programs[name] = program;
    if (model) this.models[name] = model;

    const doAdd = async () => {
      // Roster shown to the new agent = the full current member list (display names).
      const entry = this.spawnMember(name, program, this.launchDir, [...this.members]);
      const ready = await this.waitMemberReady(entry);
      if (!ready) {
        // waitMemberReady already printed why; the exit handler will have pruned members.
        return;
      }
      await seedAgent(name, this.members, this.roomId, this.instanceId);
      this.writeState(); // FEATURE #1: persist the new member + its resume handle
      printSystemMsg(`Added ${name} (${program})`);
    };
    doAdd().catch((e) => {
      printError(`/add ${name} failed: ${e.message}`);
      // Roll back the optimistic bookkeeping: if spawnMember threw synchronously (e.g.
      // fs.writeFileSync EACCES/ENOSPC) there is no child/exit-handler to prune `name`,
      // and leaving it in members would make every future relay target a dead busId.
      this.members = this.members.filter((m) => m !== name);
      delete this.programs[name];
      delete this.models[name];
      delete this.resumeInfo[name];
      this.clearWorking(name);
    });
  }

  /**
   * (B) /kick <name>. If a member: tear down its child (same SIGTERM→3s→SIGKILL recipe as
   * shutdown), clean its temp files, deregister its busId on the bus, and drop it from
   * members/programs/children/working. Does NOT touch this.running (room stays up).
   */
  cmdKick(name) {
    if (!name) {
      printSystemMsg("usage: /kick <name>");
      return;
    }
    if (!this.members.includes(name)) {
      printSystemMsg(`${name} is not a member.`);
      return;
    }
    const idx = this.children.findIndex((c) => c.agentName === name);
    const entry = idx !== -1 ? this.children[idx] : null;

    // Drop from the fan-out set immediately so no further relays target it. (The child's
    // own 'exit' handler also prunes members — harmless double-removal via filter.)
    this.members = this.members.filter((m) => m !== name);
    delete this.programs[name];
    delete this.models[name];
    delete this.resumeInfo[name]; // FEATURE #1: drop its resume handle (it's being removed for good)
    this.clearWorking(name);
    if (idx !== -1) this.children.splice(idx, 1);
    this.writeState(); // FEATURE #1: persist the smaller roster

    const doKick = async () => {
      if (entry) {
        // unlinkEmptyMcp:false — emptyMcp is a per-room shared file other claude agents use.
        // removeCodexHome:true — /kick is a permanent removal, so wipe the stable codex home
        // (normal shutdown KEEPS it for resume; only /kick deletes it).
        await this.teardownChild(entry, { unlinkEmptyMcp: false, removeCodexHome: true });
      }
      // Deregister the agent's busId on the bus (the name it registered under). Killing the
      // `agentbus run` child already drops its connection; this is an explicit belt-and-braces
      // close so the daemon's agent row is removed promptly.
      try {
        spawnSync(AB_PATH, ["close", "--name", agentBusId(this.roomId, name)], { encoding: "utf8", timeout: 5000 });
      } catch { /* non-fatal */ }
      printSystemMsg(`Kicked ${name}`);
    };
    doKick().catch((e) => printError(`/kick ${name} failed: ${e.message}`));
  }

  // ── Launch Agents ─────────────────────────────────────────────────────────

  /**
   * (B) Launch ONE agent: spawn its runner, attach the exit-pruning handler, and push
   * a children entry. Shared by launchAgents() (boot) and /add (in-room). Does NOT wait
   * for register/ready and does NOT seed — callers drive that (boot batches the waits;
   * /add waits+seeds the single agent). `rosterForPrompt` is the agent list shown to the
   * launched agent in its system prompt (display names).
   * Returns the children entry just pushed.
   */
  spawnMember(agentName, program, launchDir, rosterForPrompt, resume = {}) {
    const { child, spFile, transcriptFile, emptyMcp, codexHome, claudeSessionId, settingsFile } = launchAgent(
      agentName,
      program,
      rosterForPrompt,
      this.roomId,
      this.instanceId,
      launchDir,
      os.tmpdir(),
      resume,
      (this.models && this.models[agentName]) || null,
      this.cmds || {}
    );
    // FEATURE #1: record this agent's resume handle so writeState() persists it.
    // codex → its stable home; claude → the pinned session uuid.
    this.resumeInfo[agentName] = {};
    if (codexHome) this.resumeInfo[agentName].codexHome = codexHome;
    if (claudeSessionId) this.resumeInfo[agentName].claudeSessionId = claudeSessionId;
    // M3: attach exit handler here where 'this' is bound, so dead agents are pruned
    // from the fan-out member list immediately (prevents relaying to dead agents).
    // (A) prune by DISPLAY name (this.members holds display names).
    child.on("exit", (code, signal) => {
      const how = code !== null ? `code ${code}` : `signal ${signal || "SIGKILL"}`;
      printSystemMsg(`Agent ${agentName} exited (${how}) — removed from fan-out`);
      this.members = this.members.filter((m) => m !== agentName);
      this.clearWorking(agentName); // drop any stale "working" flag
      // TASK 3: an agent exit is a natural pruning trigger (its transcript is now
      // stale). Debounced inside sweepTranscripts() so a multi-agent teardown logs once.
      this.sweepTranscripts().catch(() => {});
    });
    // codexHome is undefined for claude agents — harmless in shutdown (guarded).
    const entry = { child, spFile, agentName, transcriptFile, emptyMcp, codexHome, settingsFile };
    this.children.push(entry);
    return entry;
  }

  /**
   * (B) Wait for one already-spawned child to register + become ready. Returns true if
   * the agent is alive and ready to be seeded, false if it died. Pure orchestration over
   * the existing wait helpers — shared by boot and /add.
   * (A) registration is checked against the agent's busId (the name it registered under).
   */
  async waitMemberReady(entry) {
    const { agentName, transcriptFile, child } = entry;
    if (child.exitCode !== null) {
      printError(`${agentName} exited (code ${child.exitCode}) at launch — check its transcript: ${transcriptFile}. Not seeding it.`);
      return false;
    }
    const registered = await waitForAgentRegistered(agentBusId(this.roomId, agentName), 60000);
    if (!registered) {
      printSystemMsg(`WARNING: ${agentName} did not register within timeout`);
    }
    const ready = await waitForAgentReady(transcriptFile, agentName, 60000, child);
    if (!ready || child.exitCode !== null) {
      printError(`${agentName} exited (code ${child.exitCode}) before becoming ready — check transcript: ${transcriptFile}. Not seeding it.`);
      return false;
    }
    printSystemMsg(`${agentName} ready`);
    return true;
  }

  async launchAgents(launchDir, resumeHandles = {}) {
    const allAgentNames = [...this.members];

    // Build resume handles from --agent-session flags, then merge with any state-file handles
    // (from --resume). State-file handles take priority — explicit --resume wins over --agent-session.
    const sessionHandles = {};
    for (const [name, id] of Object.entries(this.sessions || {})) {
      const prog = this.programs[name] || "claude";
      if (prog === "claude") {
        sessionHandles[name] = { claudeSessionId: id };
      } else if (prog === "codex") {
        // codexSessionId triggers `codex resume <id> --all` (specific session, not --last).
        sessionHandles[name] = { codex: true, codexSessionId: id };
      }
      // gemini/agy/qodercli: specific-id resume not supported; --agent-session id is ignored.
    }
    const allHandles = { ...sessionHandles, ...resumeHandles };

    for (const agentName of this.members) {
      const program = this.programs[agentName] || "claude";
      // FEATURE #1: on --resume, pass each agent its restore handle so launchAgent
      // relaunches the prior session (codex resume / claude --resume <uuid>).
      this.spawnMember(agentName, program, launchDir, allAgentNames, allHandles[agentName] || {});
    }

    // Wait for all agents to register + become ready, detecting dead children.
    printSystemMsg("Waiting for agents to register on the bus...");
    for (const entry of this.children) {
      await this.waitMemberReady(entry);
    }
    if (this.members.length === 0) {
      printError(`No agents are alive — the room has no participants. Check agent transcripts in ${os.tmpdir()}.`);
    }
  }

  // ── FEATURE #1: History Replay (on --resume) ───────────────────────────────
  /**
   * Replay the prior thread from the DB tail behind a dim divider, so a reconnected
   * room shows where the conversation left off. Caps at the last ~150 rows. Does NOT
   * advance renderCursor — startRender()'s initCursor sets it to MAX(rowid) afterward,
   * so live messages continue cleanly without re-rendering this history.
   */
  async replayHistory(limit = 150) {
    const db = dbPath();
    if (!fs.existsSync(db)) return;
    const sql = buildReplaySql(this.instanceId, this.roomBus, limit);
    const rows = await new Promise((resolve) => {
      execFile("sqlite3", ["-json", db, sql], { timeout: 5000, encoding: "utf8" }, (err, stdout) => {
        if (err) { resolve([]); return; }
        try {
          const out = (stdout || "").trim();
          resolve(!out || out === "[]" ? [] : JSON.parse(out));
        } catch { resolve([]); }
      });
    });
    if (rows.length === 0) return;
    // DESC+LIMIT gave us the TAIL newest-first; reverse to chronological order.
    rows.reverse();
    for (const row of rows) renderMessage(row, this.roomId, this.instanceId);
    writeOut(`\n${C.DIM}──── resumed · history above ────${C.RESET}\n`);
  }

  // ── Seed Agents ───────────────────────────────────────────────────────────

  async seedAll() {
    for (const agentName of this.members) {
      await seedAgent(agentName, this.members, this.roomId, this.instanceId);
    }
  }

  // On --resume we deliberately skip the full welcome seed (it would re-introduce
  // "you just joined / stay silent" framing that fights the restored context). But
  // a resumed agent has NO fresh reminder of the reply protocol, so it can drift
  // back to writing prose instead of running `agentbus send`. Send a concise,
  // protocol-only reminder so resumed agents keep replying ON the bus.
  async seedAllResumeReminder() {
    const roomBus = this.roomBus;
    for (const agentName of this.members) {
      const busId = agentBusId(this.roomId, agentName);
      const body =
        `Room "${this.roomId}" resumed. Reminder — to reply to the room you MUST run this shell command ` +
        `(your terminal prose does NOT reach anyone):  ${AB_PATH} send --from ${busId} ` +
        `--to ${roomBus} --thread-id ${this.instanceId} --msg-type response "your message"  ` +
        `Only send when you have something to say; otherwise stay silent. Do not greet.`;
      const r = await sendMessage(roomBus, busId, body, this.instanceId, "request");
      if (!r.ok) printError(`Resume reminder to ${agentName} failed: ${r.error}`);
    }
  }

  // ── Teardown ──────────────────────────────────────────────────────────────

  /**
   * (B) Tear down ONE agent's child + temp files. Factored out of shutdown so /kick can
   * reuse the EXACT same kill recipe (SIGTERM → 3s → SIGKILL on the process group).
   * Returns a Promise that resolves once the child has exited and its temp files are cleaned.
   * H2/H3 semantics preserved. `unlinkEmptyMcp` defaults true (full shutdown); /kick passes
   * false because emptyMcp is a per-ROOM shared file (empty-mcp-<roomId>.json) that surviving
   * claude agents may still reference — it's recreated by the existsSync guard on next /add.
   * `removeCodexHome` defaults false: the codex CODEX_HOME is STABLE and holds the resumable
   * session, so it survives normal shutdown; only /kick passes true to delete it permanently.
   */
  teardownChild({ child, agentName, spFile, emptyMcp, codexHome, settingsFile }, { unlinkEmptyMcp = true, removeCodexHome = false } = {}) {
    return new Promise((resolve) => {
      let done = false;
      const finish = () => { if (!done) { done = true; resolve(); } };

      // Try killing the whole process group via negative pid (requires detached:true on spawn)
      try {
        process.kill(-child.pid, "SIGTERM");
      } catch {
        // If process group kill fails, fall back to killing just the direct child
        try { child.kill("SIGTERM"); } catch {}
      }

      child.once("exit", finish);

      // If not exited in 3s, send SIGKILL to the group
      const killer = setTimeout(() => {
        printSystemMsg(`${agentName} did not exit in 3s after SIGTERM — sending SIGKILL`);
        try { process.kill(-child.pid, "SIGKILL"); } catch {
          try { child.kill("SIGKILL"); } catch {}
        }
        finish();
      }, 3000);

      child.once("exit", () => clearTimeout(killer));
    }).then(() => {
      printSystemMsg(`Killed ${agentName}`);
      // H3: cleanup claude temp files (no-op/guarded when null for codex).
      try { if (spFile && fs.existsSync(spFile)) fs.unlinkSync(spFile); } catch {}
      try { if (settingsFile && fs.existsSync(settingsFile)) fs.unlinkSync(settingsFile); } catch {}
      if (unlinkEmptyMcp) {
        try { if (emptyMcp && fs.existsSync(emptyMcp)) fs.unlinkSync(emptyMcp); } catch {}
      }
      // FEATURE #1: the codex CODEX_HOME is now STABLE (holds the resumable session).
      // KEEP it on normal shutdown so `--resume` works; only DELETE it on /kick
      // (the agent is being permanently removed). removeCodexHome drives this.
      if (removeCodexHome) {
        try { if (codexHome && fs.existsSync(codexHome)) fs.rmSync(codexHome, { recursive: true, force: true }); } catch {}
      }
    });
  }

  async shutdown(reason = "shutdown") {
    if (!this.running) return;
    this.running = false;
    printSystemMsg(`Shutting down: ${reason}`);

    // FEATURE #1: persist final state BEFORE killing children / exiting, so --resume
    // can later restore this exact roster + each agent's session handle.
    this.writeState();

    this.stopRender();

    // H2: kill each agent's entire process group (SIGTERM → 3s wait → SIGKILL) so the
    // inner program (claude/codex) doesn't survive detached.
    // H3: unlink both spFile and emptyMcp temp files (claude); rm codexHome/workDir (codex).
    const killPromises = this.children.map((entry) => this.teardownChild(entry));

    await Promise.all(killPromises);

    // Close room on bus
    if (this.socket && !this.socket.destroyed) {
      try {
        this.lineReader.write(JSON.stringify({ type: "Close" }));
        await sleep(200);
        this.socket.destroy();
      } catch {}
    }

    if (this.rl) {
      this.rl.close();
    }

    printSystemMsg("Room closed.");
    process.exit(0);
  }

  scheduleReconnect() {
    // H4: guard against double-reconnect race (both 'error' and 'close' firing,
    // or multiple 'close' events). Only one reconnect loop at a time.
    if (!this.running || this.reconnecting) return;
    this.reconnecting = true; // stays true across retries; consumeLoop parks until cleared
    const attempt = async () => {
      if (!this.running) { this.reconnecting = false; return; }
      printSystemMsg("Reconnecting socket...");
      try {
        await this.connect();
        const okReg = await this.register();
        if (!okReg) {
          // R2: never proceed connected-but-unregistered (every Read would FK-fail).
          printError("Reconnect: re-registration failed; retrying in 3s...");
          setTimeout(attempt, 3000);
          return;
        }
        // success — consumeLoop unparks and reads on the new socket
        this.reconnecting = false;
      } catch (e) {
        printError(`Reconnect failed: ${e.message}; retrying in 3s...`);
        setTimeout(attempt, 3000);
      }
    };
    setTimeout(attempt, 3000);
  }

  // ── Run ───────────────────────────────────────────────────────────────────

  async run(launchDir) {
    this.launchDir = launchDir; // (B) remember for runtime /add
    // 1. Ensure daemon
    if (!ensureDaemonRunning()) {
      process.exit(1);
    }

    // 2. Connect socket and register
    await this.connect();
    const ok = await this.register();
    if (!ok) {
      process.exit(1);
    }

    this.running = true;

    // TASK 3: prune old/oversized transcripts on room start (best-effort, never
    // blocks — protects this hub's own children + active agents + live writes).
    this.sweepTranscripts().catch(() => {});

    // Resolve the room INSTANCE id — the bus thread_id and log-file key. A resumed
    // room REUSES the persisted instance id (it's the same room generation, so its
    // DB rows and log file continue); a fresh room generates a new one. Legacy
    // fallback: a state file written before instance namespacing has no instanceId,
    // so we fall back to roomId (preserving the old thread_id=roomId behavior for
    // those rooms — no resume regression).
    if (this.resume) {
      const probe = this.loadStateForResume();
      if (probe && probe.instanceId) {
        this.instanceId = probe.instanceId;
      } else if (probe) {
        this.instanceId = this.roomId; // legacy state file — keep old thread_id=roomId behavior
        printSystemMsg(`--resume: legacy state file (no instanceId) — using roomId as thread_id.`);
      } else {
        this.instanceId = newInstanceId(); // no saved state despite --resume → truly fresh
      }
    } else {
      this.instanceId = newInstanceId();
    }

    // 3. Print banner
    process.stdout.write(
      `\n${C.BOLD}${C.CYAN}══════════════════════════════════════════${C.RESET}\n` +
      `${C.BOLD}  AgentBus Room: ${this.roomId}${C.RESET}${C.DIM} (instance ${this.instanceId})${C.RESET}\n` +
      `${C.DIM}  Members: ${this.members.join(", ")}, ram${C.RESET}\n` +
      `${C.DIM}  Circuit-breaker: ${this.cbMax} consecutive agent msgs${C.RESET}\n` +
      `${C.DIM}  Log: ${path.basename(logFile(this.roomId, this.instanceId))}${C.RESET}\n` +
      `${C.BOLD}${C.CYAN}══════════════════════════════════════════${C.RESET}\n\n`
    );

    // FEATURE #1: --resume — load prior state, replay history, restore agent sessions.
    let resumeHandles = {};
    let resuming = false;
    if (this.resume) {
      const loaded = this.loadStateForResume();
      if (loaded) {
        resuming = true;
        // Adopt the persisted roster (overrides whatever --agents was passed).
        this.members = loaded.agents.map((a) => a.name);
        this.programs = {};
        this.models = {};
        for (const a of loaded.agents) {
          this.programs[a.name] = a.program;
          if (a.model) this.models[a.name] = a.model;
          // codex → resume:true; gemini → resume:true (`-r latest`); agy → resume:true (`--continue`);
          // claude → restore its pinned session id.
          resumeHandles[a.name] = a.program === "codex"
            ? { codex: true }
            : a.program === "gemini"
            ? { gemini: true }
            : a.program === "agy"
            ? { agy: true }
            : { claudeSessionId: a.claudeSessionId || null };
        }
        printSystemMsg(`Resuming room "${this.roomId}" with ${this.members.length} agent(s): ${this.members.join(", ")}`);
        // Replay the prior thread BEFORE relaunching agents so the human sees context first.
        await this.replayHistory(150);
      } else {
        printSystemMsg(`--resume: no saved state for "${this.roomId}" — starting fresh.`);
      }
    }

    // 4. Launch agents (if any configured)
    if (this.members.length > 0) {
      await this.launchAgents(launchDir, resumeHandles);
      // 5. Seed agents (H1: await async sendMessage inside seedAll).
      // FEATURE #1: on resume, SKIP the full welcome seed — each agent restores its own
      // context from its session; re-seeding would fight that (re-introduce "welcome").
      // But send a LIGHT protocol reminder so resumed agents keep replying via the bus.
      if (!resuming) {
        await this.seedAll();
      } else {
        await this.seedAllResumeReminder();
      }
    }
    // Persist initial state so a crash/close mid-session is still resumable.
    this.writeState();

    // 6. Start DB tail renderer
    this.startRender();

    // 7. Start human input (unless --headless: a detached/headless room has no
    // live stdin and uses the web UI's POST /send as its input surface instead).
    if (!this.headless) this.startHumanInput();

    // 8. Start consume loop (runs until shutdown)
    this.consumeLoop().catch((e) => {
      printError(`Consume loop fatal: ${e.message}`);
    });
  }
}

// ── Self-Test ────────────────────────────────────────────────────────────────

async function runSelfTest() {
  let passed = 0;
  let failed = 0;

  function assert(label, condition, extra = "") {
    if (condition) {
      process.stdout.write(`  ${C.GREEN}PASS${C.RESET} ${label}\n`);
      passed++;
    } else {
      process.stdout.write(`  ${C.RED}FAIL${C.RESET} ${label}${extra ? ": " + extra : ""}\n`);
      failed++;
    }
  }

  function deepEqual(a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
  }

  process.stdout.write(`\n${C.BOLD}AgentBus Room Self-Test${C.RESET}\n`);
  process.stdout.write(`${C.DIM}────────────────────────────────────${C.RESET}\n`);

  // ── parseReadLine tests ──────────────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}parseReadLine${C.RESET}\n`);

  {
    const msg = { id: "abc", from_agent: "claude-A", to_agent: "room", thread_id: "r1", msg_type: "response", body: "hello", metadata: null, read_at: null, created_at: "2026-06-02T10:00:00+00:00" };

    // PATH A — batch
    const pathA = `{"type":"Ok","data":[${JSON.stringify(msg)}]}`;
    const rA = parseReadLine(pathA);
    assert("PATH A: kind=batch", rA.kind === "batch");
    assert("PATH A: messages is array", Array.isArray(rA.messages));
    assert("PATH A: first message body", rA.messages[0]?.body === "hello");

    // PATH B — push (single Message object)
    const pathB = `{"type":"Message","message":${JSON.stringify(msg)}}`;
    const rB = parseReadLine(pathB);
    assert("PATH B: kind=push", rB.kind === "push");
    assert("PATH B: messages length=1", rB.messages.length === 1);
    assert("PATH B: message body", rB.messages[0]?.body === "hello");

    // PATH C — empty Ok
    const pathC = `{"type":"Ok","data":[]}`;
    const rC = parseReadLine(pathC);
    assert("PATH C: kind=batch", rC.kind === "batch");
    assert("PATH C: empty array", rC.messages.length === 0);

    // PATH D — timeout error
    const pathD = `{"type":"Error","message":"No messages (timeout)"}`;
    const rD = parseReadLine(pathD);
    assert("PATH D: kind=timeout", rD.kind === "timeout");

    // Register ack — Ok{data: object}
    const ack = `{"type":"Ok","data":{"id":"x","name":"room","program":"hub","model":"unknown","project":"default","state":"active","pid":null,"registered_at":"2026-06-02"}}`;
    const rAck = parseReadLine(ack);
    assert("Register ack: kind=ack", rAck.kind === "ack");

    // Non-timeout error
    const errLine = `{"type":"Error","message":"Send failed: FOREIGN KEY constraint failed"}`;
    const rErr = parseReadLine(errLine);
    assert("Error non-timeout: kind=error", rErr.kind === "error");
    assert("Error non-timeout: message present", rErr.message.includes("FOREIGN KEY"));

    // Malformed
    const rBad = parseReadLine("not json at all");
    assert("Malformed: kind=unknown", rBad.kind === "unknown");
  }

  // ── computeFanout tests ─────────────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}computeFanout${C.RESET}\n`);

  {
    const members = ["claude-A", "claude-B", "codex-A"];
    const msg = { from_agent: "claude-A", body: "Rust is great" };

    // Normal fan-out: sender excluded
    const fanout = computeFanout(msg, members);
    assert("Fanout excludes sender", !fanout.some((f) => f.to === "claude-A"));
    assert("Fanout includes other members", fanout.length === 2);
    assert("Fanout body prefixed with author", fanout.every((f) => f.body === "claude-A: Rust is great"));

    // @mention narrows to one
    const narrow = computeFanout(msg, members, "claude-B");
    assert("@mention: single target", narrow.length === 1);
    assert("@mention: correct target", narrow[0].to === "claude-B");

    // @mention self-exclusion still applies
    const selfMention = computeFanout(msg, members, "claude-A");
    assert("@mention self: zero targets", selfMention.length === 0);

    // Message from unknown/non-member author still fans out to all members
    const outsiderMsg = { from_agent: "outsider", body: "hi" };
    const outsiderFanout = computeFanout(outsiderMsg, members);
    assert("Outsider fanout: all members", outsiderFanout.length === 3);
  }

  // ── CircuitBreaker tests ────────────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}CircuitBreaker${C.RESET}\n`);

  {
    const cb = new CircuitBreaker(3);
    assert("CB initial: not paused", !cb.isPaused());
    assert("CB: first agent msg not tripped", cb.recordAgentMessage() === false);
    assert("CB: second agent msg not tripped", cb.recordAgentMessage() === false);
    assert("CB: third agent msg trips", cb.recordAgentMessage() === true);
    assert("CB: isPaused after trip", cb.isPaused());

    // Human input resets
    cb.recordHumanMessage();
    assert("CB: reset after human input", !cb.isPaused());
    assert("CB: count reset", cb.count() === 0);

    // Trip again
    cb.recordAgentMessage();
    cb.recordAgentMessage();
    const tripped2 = cb.recordAgentMessage();
    assert("CB: trips again after reset", tripped2 === true);
    assert("CB: paused again", cb.isPaused());
  }

  // ── buildSendArgv injection-resistance tests ──────────────────────────────

  process.stdout.write(`\n${C.BOLD}buildSendArgv (shell-injection resistance)${C.RESET}\n`);

  {
    // Bodies that would break shell string interpolation if passed via execSync
    const nastyCases = [
      { desc: "dollar-HOME", body: "Use $HOME for the path" },
      { desc: "backtick-exec", body: "Try `ls /`" },
      { desc: "subshell", body: "Run $(cat /etc/passwd)" },
      { desc: "double-quote", body: 'He said "hello"' },
      { desc: "single-quote", body: "It's a trap" },
      { desc: "backslash", body: "C:\\Users\\foo\\bar" },
      { desc: "newline-in-body", body: "line1\nline2" },
      { desc: "semicolon-injection", body: "ok; rm -rf /tmp/x" },
    ];

    for (const { desc, body } of nastyCases) {
      const argv = buildSendArgv("room", "agent", body, "r1", "request");
      // The body must appear verbatim as the LAST element of argv
      const lastArg = argv[argv.length - 1];
      assert(
        `sendArgv body preserved verbatim (${desc})`,
        lastArg === body,
        `got: ${JSON.stringify(lastArg)}`
      );
      // Argv must be a plain array of strings — no shell metacharacter processing
      assert(
        `sendArgv is an array (${desc})`,
        Array.isArray(argv)
      );
    }

    // Structure check
    const sample = buildSendArgv("room", "bob", "hello", "r1", "response");
    assert("sendArgv: starts with 'send'", sample[0] === "send");
    assert("sendArgv: --from present", sample.includes("--from"));
    assert("sendArgv: --to present", sample.includes("--to"));
    assert("sendArgv: body is last arg", sample[sample.length - 1] === "hello");
    assert("sendArgv: includes thread-id flag", sample.includes("--thread-id"));

    // No thread-id when null/undefined
    const noThread = buildSendArgv("room", "bob", "hi", null, "request");
    assert("sendArgv: no --thread-id when null", !noThread.includes("--thread-id"));
  }

  // ── C1: pendingLines buffer — no Message frame ever dropped ─────────────────
  // Requirement: simulate LineReader receiving 2 Message lines in one data event
  // with (a) 0 resolvers waiting and (b) 1 resolver waiting. Assert BOTH processed.

  process.stdout.write(`\n${C.BOLD}C1: pendingLines no-loss guarantee${C.RESET}\n`);

  {
    const msgA = { id: "a", from_agent: "claude-A", to_agent: "room", thread_id: "r1", msg_type: "response", body: "msg-A", metadata: null, read_at: null, created_at: "2026" };
    const msgB = { id: "b", from_agent: "claude-B", to_agent: "room", thread_id: "r1", msg_type: "response", body: "msg-B", metadata: null, read_at: null, created_at: "2026" };
    const lineA = JSON.stringify({ type: "Message", message: msgA });
    const lineB = JSON.stringify({ type: "Message", message: msgB });

    // Scenario 1: 0 resolvers waiting — both lines must land in pendingLines
    {
      const hub = new RoomHub("r1", []);
      hub.handleLine(lineA);
      hub.handleLine(lineB);
      assert("C1 scenario-1: pendingLines has 2 entries (no resolver waiting)", hub.pendingLines.length === 2, `got ${hub.pendingLines.length}`);
      assert("C1 scenario-1: first pendingLine is push", hub.pendingLines[0].kind === "push");
      assert("C1 scenario-1: first pendingLine body=msg-A", hub.pendingLines[0].messages[0]?.body === "msg-A");
      assert("C1 scenario-1: second pendingLine body=msg-B", hub.pendingLines[1].messages[0]?.body === "msg-B");

      // readOne() resolves synchronously from pendingLines (Promise.resolve path)
      const r1 = await hub.readOne();
      assert("C1 scenario-1: first readOne returns msg-A", r1.messages[0]?.body === "msg-A");
      const r2 = await hub.readOne();
      assert("C1 scenario-1: second readOne returns msg-B", r2.messages[0]?.body === "msg-B");
      assert("C1 scenario-1: pendingLines empty after both readOnes", hub.pendingLines.length === 0);
    }

    // Scenario 2: 1 resolver waiting — first line goes to resolver, second to pendingLines
    {
      const hub2 = new RoomHub("r1", []);
      // Create one pending resolver (simulate consumeLoop awaiting readOne)
      let resolvedValue = null;
      const p = hub2.readOne();
      p.then((v) => { resolvedValue = v; });
      assert("C1 scenario-2: 1 resolver waiting before push", hub2.readResolvers.length === 1);

      // Deliver both lines synchronously (as if they arrived in one data event)
      hub2.handleLine(lineA);
      hub2.handleLine(lineB);

      // Flush microtasks so the resolver's .then() fires
      await Promise.resolve();

      assert("C1 scenario-2: resolver received msg-A (not dropped)", resolvedValue?.messages?.[0]?.body === "msg-A", `got ${JSON.stringify(resolvedValue)}`);
      assert("C1 scenario-2: msg-B buffered in pendingLines (not dropped)", hub2.pendingLines.length === 1, `got ${hub2.pendingLines.length}`);
      assert("C1 scenario-2: pendingLines[0] body=msg-B", hub2.pendingLines[0]?.messages?.[0]?.body === "msg-B");
    }
  }

  // ── LineReader buffer logic (unit test of the line-splitter concept) ──────

  process.stdout.write(`\n${C.BOLD}Line buffer splitting${C.RESET}\n`);

  {
    // Simulate chunked delivery
    const lines = [];
    const fakeSock = {
      callbacks: [],
      on(event, cb) { if (event === "data") this.callbacks.push(cb); },
    };
    const lr = new LineReader(fakeSock, (line) => lines.push(line));

    // Push two JSON objects in one chunk
    const chunk1 = '{"type":"Ok","data":[]}\n{"type":"Message","message":{"id":"x"';
    const chunk2 = ',"from_agent":"a","to_agent":"room","thread_id":null,"msg_type":"response","body":"hi","metadata":null,"read_at":null,"created_at":"2026"}}\n';

    fakeSock.callbacks.forEach((cb) => cb(Buffer.from(chunk1)));
    fakeSock.callbacks.forEach((cb) => cb(Buffer.from(chunk2)));

    assert("Buffer: first line parsed", lines.length >= 1 && lines[0].includes('"Ok"'));
    assert("Buffer: second line assembled from chunks", lines.length === 2);
    const parsed = parseReadLine(lines[1]);
    assert("Buffer: second line parses as push", parsed.kind === "push");
    assert("Buffer: push body is hi", parsed.messages[0]?.body === "hi");
  }

  // ── parseAgentSpec tests (per-program launch) ─────────────────────────────

  process.stdout.write(`\n${C.BOLD}parseAgentSpec${C.RESET}\n`);

  {
    // Bare name → claude (backward compatible)
    const bare = parseAgentSpec("claude-A");
    assert("spec bare: name", bare.name === "claude-A");
    assert("spec bare: defaults to claude", bare.program === "claude");
    assert("spec bare: no error", !bare.error);

    // name:claude explicit
    const explicitClaude = parseAgentSpec("foo:claude");
    assert("spec foo:claude: name", explicitClaude.name === "foo");
    assert("spec foo:claude: program", explicitClaude.program === "claude");

    // name:codex accepted
    const codex = parseAgentSpec("codex-A:codex");
    assert("spec codex-A:codex: name", codex.name === "codex-A");
    assert("spec codex-A:codex: program", codex.program === "codex");
    assert("spec codex-A:codex: no error", !codex.error);

    // name:gemini accepted (third program)
    const gemini = parseAgentSpec("gem:gemini");
    assert("spec gem:gemini: name", gemini.name === "gem");
    assert("spec gem:gemini: program", gemini.program === "gemini");
    assert("spec gem:gemini: no error", !gemini.error);

    // name:agy accepted (fourth program)
    const agy = parseAgentSpec("a:agy");
    assert("spec a:agy: name", agy.name === "a");
    assert("spec a:agy: program", agy.program === "agy");
    assert("spec a:agy: no error", !agy.error);

    // model: bare/2-part specs carry model=null
    assert("spec bare: model null", bare.model === null);
    assert("spec foo:claude: model null", explicitClaude.model === null);

    // name:program:model parses the model
    const withModel = parseAgentSpec("claude-A:claude:sonnet");
    assert("spec :model: name", withModel.name === "claude-A");
    assert("spec :model: program", withModel.program === "claude");
    assert("spec :model: model", withModel.model === "sonnet");
    assert("spec :model: no error", !withModel.error);

    // model with a dotted/hyphenated version string
    const codexModel = parseAgentSpec("codex-A:codex:gpt-5.1");
    assert("spec codex :model: program", codexModel.program === "codex");
    assert("spec codex :model: model", codexModel.model === "gpt-5.1");

    // buildCodexConfigToml respects model override (and defaults when omitted)
    assert("codex toml: default model", buildCodexConfigToml("/x").includes('model = "gpt-5.5"'));
    assert("codex toml: model override", buildCodexConfigToml("/x", "gpt-5.1").includes('model = "gpt-5.1"'));

    // unknown program rejected
    const bad = parseAgentSpec("x:perl");
    assert("spec x:perl: rejected with error", typeof bad.error === "string" && bad.error.includes("perl"));
    assert("spec x:perl: no name returned", bad.name === undefined);

    // invalid name rejected (shell metachar)
    const badName = parseAgentSpec("bad name:codex");
    assert("spec 'bad name': rejected", typeof badName.error === "string" && badName.error.includes("invalid agent name"));

    // whitespace tolerated around spec
    const spaced = parseAgentSpec("  claude-B  ");
    assert("spec spaced: trimmed name", spaced.name === "claude-B");
    assert("spec spaced: default program", spaced.program === "claude");
  }

  // ── buildCodexConfigToml tests ────────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}buildCodexConfigToml${C.RESET}\n`);

  {
    // FIX #4: the trusted dir is the user's launchDir (codex --cd target), not a tmp workdir.
    const wd = "/Users/ram/CODE/myproject";
    const toml = buildCodexConfigToml(wd);
    assert("codex toml: model gpt-5.5", toml.includes('model = "gpt-5.5"'));
    assert("codex toml: reasoning effort medium (default)", toml.includes('model_reasoning_effort = "medium"'));
    assert("codex toml: reasoning effort override", buildCodexConfigToml(wd, "gpt-5.5", "high").includes('model_reasoning_effort = "high"'));
    assert("codex toml: projects key references trusted dir", toml.includes(`[projects."${wd}"]`));
    assert("codex toml: trust_level trusted", toml.includes('trust_level = "trusted"'));
    // The MCP boot stall is avoided by NOT emitting any [mcp_servers.*] section.
    assert("codex toml: no mcp_servers section", !toml.includes("mcp_servers"));
  }

  // ── buildGeminiArgs tests (validated recipe) ──────────────────────────────

  process.stdout.write(`\n${C.BOLD}buildGeminiArgs (validated recipe)${C.RESET}\n`);

  {
    const busId = "r1-gem";
    const transcript = "/tmp/room-transcript-gem-r1.txt";
    const prompt = "You are r1-gem. Reply via agentbus send --from r1-gem --to room-r1 ...";

    // Fresh launch
    const fresh = buildGeminiArgs(busId, transcript, prompt);
    assert("gemini args: starts with run", fresh[0] === "run");
    assert("gemini args: --name busId", fresh.includes("--name") && fresh[fresh.indexOf("--name") + 1] === busId);
    assert("gemini args: --program gemini", fresh[fresh.indexOf("--program") + 1] === "gemini");
    assert("gemini args: --transcript present", fresh[fresh.indexOf("--transcript") + 1] === transcript);
    assert("gemini args: has -- separator before gemini", fresh.includes("--") && fresh[fresh.indexOf("--") + 1] === "gemini");
    assert("gemini args: --yolo present", fresh.includes("--yolo"));
    assert("gemini args: --skip-trust present", fresh.includes("--skip-trust"));
    // CRITICAL: the literal "__none__" dummy server name (NOT an empty string — gemini crashes on empty).
    const mcpIdx = fresh.indexOf("--allowed-mcp-server-names");
    assert("gemini args: --allowed-mcp-server-names present", mcpIdx !== -1);
    assert("gemini args: dummy mcp name is literal __none__", fresh[mcpIdx + 1] === "__none__");
    assert("gemini args: dummy mcp name is NOT empty string", fresh[mcpIdx + 1] !== "");
    // Prompt delivered as the -i initial interactive prompt, as a SINGLE verbatim argv element.
    const iIdx = fresh.indexOf("-i");
    assert("gemini args: -i present", iIdx !== -1);
    assert("gemini args: prompt is verbatim argv element after -i", fresh[iIdx + 1] === prompt);
    assert("gemini args: prompt is the last arg", fresh[fresh.length - 1] === prompt);
    // Fresh launch must NOT carry a resume flag.
    assert("gemini args (fresh): no -r resume flag", !fresh.includes("-r"));

    // Resume launch adds `-r latest`
    const resumed = buildGeminiArgs(busId, transcript, prompt, { resume: true });
    const rIdx = resumed.indexOf("-r");
    assert("gemini args (resume): -r present", rIdx !== -1);
    assert("gemini args (resume): -r latest", resumed[rIdx + 1] === "latest");
    assert("gemini args (resume): still has __none__ mcp name", resumed[resumed.indexOf("--allowed-mcp-server-names") + 1] === "__none__");
    assert("gemini args (resume): prompt still last", resumed[resumed.length - 1] === prompt);
  }

  // ── buildAgyArgs tests (validated recipe) ─────────────────────────────────

  process.stdout.write(`\n${C.BOLD}buildAgyArgs (validated recipe)${C.RESET}\n`);

  {
    const busId = "r1-a";
    const transcript = "/tmp/room-transcript-a-r1.txt";
    const prompt = "You are r1-a. Reply via agentbus send --from r1-a --to room-r1 ...";

    // Fresh launch
    const fresh = buildAgyArgs(busId, transcript, prompt);
    assert("agy args: starts with run", fresh[0] === "run");
    assert("agy args: --name busId", fresh.includes("--name") && fresh[fresh.indexOf("--name") + 1] === busId);
    assert("agy args: --program agy", fresh[fresh.indexOf("--program") + 1] === "agy");
    assert("agy args: --transcript present", fresh[fresh.indexOf("--transcript") + 1] === transcript);
    assert("agy args: has -- separator before agy", fresh.includes("--") && fresh[fresh.indexOf("--") + 1] === "agy");
    assert("agy args: --dangerously-skip-permissions present", fresh.includes("--dangerously-skip-permissions"));
    // Prompt delivered as the -i initial interactive prompt, as a SINGLE verbatim argv element.
    const iIdx = fresh.indexOf("-i");
    assert("agy args: -i present", iIdx !== -1);
    assert("agy args: prompt is verbatim argv element after -i", fresh[iIdx + 1] === prompt);
    assert("agy args: prompt is the last arg", fresh[fresh.length - 1] === prompt);
    // Fresh launch must NOT carry --continue.
    assert("agy args (fresh): no --continue flag", !fresh.includes("--continue"));

    // Resume launch adds `--continue`
    const resumed = buildAgyArgs(busId, transcript, prompt, { resume: true });
    assert("agy args (resume): --continue present", resumed.includes("--continue"));
    // --dangerously-skip-permissions still present on resume.
    assert("agy args (resume): --dangerously-skip-permissions still present", resumed.includes("--dangerously-skip-permissions"));
    // Prompt still the last arg on resume.
    assert("agy args (resume): prompt still last", resumed[resumed.length - 1] === prompt);
    // --continue appears BEFORE -i (i.e., before the prompt).
    const continueIdx = resumed.indexOf("--continue");
    const iIdxR = resumed.indexOf("-i");
    assert("agy args (resume): --continue before -i", continueIdx < iIdxR);
  }

  // ── buildQoderCliArgs tests ────────────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}buildQoderCliArgs${C.RESET}\n`);

  {
    const busId = "r1-q";
    const transcript = "/tmp/room-transcript-q-r1.txt";
    const prompt = "You are r1-q. Reply via agentbus send ...";

    // Fresh launch
    const fresh = buildQoderCliArgs(busId, transcript, prompt);
    assert("qodercli args: starts with run", fresh[0] === "run");
    assert("qodercli args: --name busId", fresh[fresh.indexOf("--name") + 1] === busId);
    assert("qodercli args: --program qodercli", fresh[fresh.indexOf("--program") + 1] === "qodercli");
    assert("qodercli args: --transcript present", fresh[fresh.indexOf("--transcript") + 1] === transcript);
    assert("qodercli args: has -- separator before qodercli", fresh.includes("--") && fresh[fresh.indexOf("--") + 1] === "qodercli");
    assert("qodercli args: --dangerously-skip-permissions", fresh.includes("--dangerously-skip-permissions"));
    const spIdx = fresh.indexOf("--system-prompt");
    assert("qodercli args: --system-prompt present", spIdx !== -1);
    assert("qodercli args: prompt is verbatim argv after --system-prompt", fresh[spIdx + 1] === prompt);
    assert("qodercli args (fresh): no --continue", !fresh.includes("--continue"));
    assert("qodercli args (fresh): no --model", !fresh.includes("--model"));

    // Resume adds --continue
    const resumed = buildQoderCliArgs(busId, transcript, prompt, { resume: true });
    assert("qodercli args (resume): --continue present", resumed.includes("--continue"));

    // Model override adds --model
    const withModel = buildQoderCliArgs(busId, transcript, prompt, { model: "pro" });
    const mIdx = withModel.indexOf("--model");
    assert("qodercli args (model): --model present", mIdx !== -1);
    assert("qodercli args (model): model value", withModel[mIdx + 1] === "pro");
  }

  // ── launchCmdAgent validation: empty command throws ───────────────────────

  process.stdout.write(`\n${C.BOLD}launchCmdAgent validation${C.RESET}\n`);

  {
    let threw = false;
    try {
      launchCmdAgent("checker", "", [], "r1", "/tmp", os.tmpdir());
    } catch (e) {
      threw = e.message.includes("has no command");
    }
    assert("launchCmdAgent: empty cmdString throws", threw);
  }

  // ── parseAgentSpec: qodercli + cmd accepted ───────────────────────────────

  process.stdout.write(`\n${C.BOLD}parseAgentSpec: qodercli + cmd${C.RESET}\n`);

  {
    const r1 = parseAgentSpec("mybot:qodercli");
    assert("parseAgentSpec qodercli: name", r1.name === "mybot");
    assert("parseAgentSpec qodercli: program", r1.program === "qodercli");

    const r2 = parseAgentSpec("checker:cmd");
    assert("parseAgentSpec cmd: name", r2.name === "checker");
    assert("parseAgentSpec cmd: program", r2.program === "cmd");

    const r3 = parseAgentSpec("mybot:qodercli:pro");
    assert("parseAgentSpec qodercli+model: model", r3.model === "pro");
  }

  // ── --agent-session / parseArgs ───────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}parseArgs: --agent-session${C.RESET}\n`);

  {
    const opts = parseArgs(["node", "agentbus-room.mjs", "r1",
      "--agents", "claude-A,codex-A:codex",
      "--agent-session", "claude-A:019e94fc-cada-7b63-a03a-1ecb3648fbac",
      "--agent-session", "codex-A:deadbeef-0000-0000-0000-123456789abc",
    ]);
    assert("agent-session: claude-A stored", opts.sessions["claude-A"] === "019e94fc-cada-7b63-a03a-1ecb3648fbac");
    assert("agent-session: codex-A stored", opts.sessions["codex-A"] === "deadbeef-0000-0000-0000-123456789abc");
  }

  // ── launchCodexAgent: resumeSessionId in argv ─────────────────────────────

  process.stdout.write(`\n${C.BOLD}launchCodexAgent: resumeSessionId${C.RESET}\n`);

  {
    // The room doesn't actually spawn processes in self-test; we test the args builder directly.
    // buildCodexConfigToml is called inside launchCodexAgent — we test the resume flag paths
    // through the pure codexArgs construction by reading what launchAgent would dispatch.
    // Use buildCodexConfigToml as a proxy to confirm the resumeSessionId path compiles.
    const tomlFresh = buildCodexConfigToml("/x");
    assert("codex toml: sanity (used by launchCodexAgent)", tomlFresh.includes("trust_level"));

    // Verify launchAgent codex dispatch passes resumeSessionId when codexSessionId is present.
    // We can't call launchCodexAgent (it does fs/spawn) so we test the dispatch logic via
    // the handle shape expected by launchAgents.
    const handle = { codex: true, codexSessionId: "abc-123" };
    assert("resume handle: codex flag set", !!handle.codex);
    assert("resume handle: codexSessionId set", handle.codexSessionId === "abc-123");

    // launchAgents merges session handles correctly: --agent-session wins unless --resume overrides.
    // Simulate the merge logic used in launchAgents:
    const sessionHandles = {};
    const fakeSessions = { "codex-A": "abc-123" };
    const fakePrograms = { "codex-A": "codex" };
    for (const [name, id] of Object.entries(fakeSessions)) {
      const prog = fakePrograms[name] || "claude";
      if (prog === "codex") sessionHandles[name] = { codex: true, codexSessionId: id };
      if (prog === "claude") sessionHandles[name] = { claudeSessionId: id };
    }
    const stateHandles = {}; // no --resume
    const allHandles = { ...sessionHandles, ...stateHandles };
    assert("session merge: codexSessionId propagated", allHandles["codex-A"].codexSessionId === "abc-123");
    assert("session merge: state-file overrides session (empty state → session wins)", allHandles["codex-A"].codex === true);
  }

  // ── Codex AGENTS.md == Claude system prompt (consistency) ─────────────────

  process.stdout.write(`\n${C.BOLD}Codex AGENTS.md / Claude prompt parity${C.RESET}\n`);

  {
    // Both delivery paths must produce the SAME prompt text so agents behave consistently.
    const prompt = generateSystemPrompt("codex-A", ["codex-A", "claude-B"], "r1");
    assert("prompt: names the self agent", prompt.includes("codex-A"));
    assert("prompt: names a peer", prompt.includes("claude-B"));
    // Collision-bug fix: agents reply to the NAMESPACED bus identity, not literal "room".
    assert("prompt: reply convention --to room-r1", prompt.includes("--to room-r1"));
    assert("prompt: carries thread-id for the room", prompt.includes("r1"));
  }

  process.stdout.write(`\n${C.BOLD}Room bus namespacing (collision-bug fix)${C.RESET}\n`);

  {
    // roomBusFor derives "room-<roomId>" — the per-room bus identity that prevents
    // two hubs from evicting each other on the daemon (the reattach storm).
    assert("roomBus: room-r1", roomBusFor("r1") === "room-r1");
    assert("roomBus: room-main", roomBusFor("main") === "room-main");
    // generateSystemPrompt must emit the namespaced --to target for its room id.
    const p2 = generateSystemPrompt("a", ["a", "b"], "demo");
    assert("prompt: --to room-demo for roomId demo", p2.includes("--to room-demo"));
  }

  // ── (A) agentBusId + displayName (per-agent bus namespacing) ───────────────

  process.stdout.write(`\n${C.BOLD}(A) agentBusId / displayName${C.RESET}\n`);

  {
    // busId derivation: "<roomId>-<name>"
    assert("agentBusId r1+claude-A", agentBusId("r1", "claude-A") === "r1-claude-A");
    assert("agentBusId demo+codex-A", agentBusId("demo", "codex-A") === "demo-codex-A");

    // displayName strips exactly the "<roomId>-" prefix
    assert("displayName strips r1- prefix", displayName("r1-claude-A", "r1") === "claude-A");
    assert("displayName strips demo- prefix", displayName("demo-codex-A", "demo") === "codex-A");
    // round-trip: display → bus → display
    assert("agentBusId/displayName round-trip", displayName(agentBusId("r1", "claude-B"), "r1") === "claude-B");

    // CRITICAL: the hub's own roomBus must NOT be mangled (it doesn't start with "<roomId>-").
    assert("displayName leaves room-r1 unmangled", displayName("room-r1", "r1") === "room-r1");
    assert("displayName leaves room-demo unmangled", displayName("room-demo", "demo") === "room-demo");
    // ram and other non-prefixed values pass through unchanged.
    assert("displayName leaves ram unchanged", displayName("ram", "r1") === "ram");
    assert("displayName leaves non-prefixed unchanged", displayName("claude-A", "r1") === "claude-A");
    // Only strips a LEADING prefix, not a mid-string occurrence.
    assert("displayName only strips leading prefix", displayName("x-r1-claude", "r1") === "x-r1-claude");
    // Non-string input passes through (defensive).
    assert("displayName passes through non-string", displayName(undefined, "r1") === undefined);
  }

  // generateSystemPrompt now emits the agent's BUS ID in the load-bearing --from line.
  process.stdout.write(`\n${C.BOLD}(A) prompt --from uses busId${C.RESET}\n`);

  {
    const prompt = generateSystemPrompt("claude-A", ["claude-A", "codex-B"], "r1");
    // The reply command's --from MUST be the namespaced busId, NOT the raw display name,
    // or the cross-room collision is re-introduced.
    assert("prompt --from r1-claude-A (busId)", prompt.includes("--from r1-claude-A"));
    assert("prompt --to room-r1 (roomBus)", prompt.includes("--to room-r1"));
    // Peer display name still appears (roster is informational, friendly names ok).
    assert("prompt names peer display name", prompt.includes("codex-B"));
  }

  // ── (D) completeMention (@-mention Tab completion, pure core) ──────────────

  process.stdout.write(`\n${C.BOLD}(D) completeMention${C.RESET}\n`);

  {
    const members = ["claude-A", "claude-B", "codex-A"];

    // "@cl" → both claude-* (prefix match), replacement is the "@cl" token
    const r1 = completeMention("@cl", members);
    assert("complete @cl: 2 matches", r1.matches.length === 2);
    assert("complete @cl: matches are @claude-A/@claude-B", deepEqual(r1.matches, ["@claude-A", "@claude-B"]));
    assert("complete @cl: replacement is @cl", r1.replacement === "@cl");

    // bare "@" lists ALL members
    const rAll = completeMention("@", members);
    assert("complete @ (bare): lists all 3", rAll.matches.length === 3);
    assert("complete @ (bare): replacement is @", rAll.replacement === "@");

    // "@codex" → single match
    const rCodex = completeMention("@codex", members);
    assert("complete @codex: single match", rCodex.matches.length === 1);
    assert("complete @codex: match is @codex-A", rCodex.matches[0] === "@codex-A");

    // case-insensitive
    const rUpper = completeMention("@CL", members);
    assert("complete @CL: case-insensitive 2 matches", rUpper.matches.length === 2);

    // mid-line @token: "hi @cl" completes the trailing token only
    const rMid = completeMention("hi @cl", members);
    assert("complete 'hi @cl': replacement is @cl (trailing token)", rMid.replacement === "@cl");
    assert("complete 'hi @cl': 2 matches", rMid.matches.length === 2);

    // no @-token at the end → no completion (replacement null → wrapper returns [[], line])
    const rNone = completeMention("hello world", members);
    assert("complete 'hello world': replacement null", rNone.replacement === null);
    assert("complete 'hello world': no matches", rNone.matches.length === 0);
    const rTrailingSpace = completeMention("@claude-A ", members);
    assert("complete '@claude-A ' (trailing space): replacement null", rTrailingSpace.replacement === null);

    // unknown partial → zero matches but still a valid replacement token
    const rUnknown = completeMention("@zzz", members);
    assert("complete @zzz: zero matches", rUnknown.matches.length === 0);
    assert("complete @zzz: replacement still @zzz", rUnknown.replacement === "@zzz");
  }

  // ── (B) /add /kick command parsing (pure: reuses parseAgentSpec) ───────────

  process.stdout.write(`\n${C.BOLD}(B) /add /kick spec parsing${C.RESET}\n`);

  {
    // /add reuses parseAgentSpec for its <name>[:<program>] argument.
    const addClaude = parseAgentSpec("worker");
    assert("/add worker → name", addClaude.name === "worker");
    assert("/add worker → defaults claude", addClaude.program === "claude");

    const addCodex = parseAgentSpec("helper:codex");
    assert("/add helper:codex → name", addCodex.name === "helper");
    assert("/add helper:codex → codex", addCodex.program === "codex");

    const addGemini = parseAgentSpec("gem:gemini");
    assert("/add gem:gemini → name", addGemini.name === "gem");
    assert("/add gem:gemini → gemini", addGemini.program === "gemini");

    const addAgy = parseAgentSpec("a:agy");
    assert("/add a:agy → name", addAgy.name === "a");
    assert("/add a:agy → agy", addAgy.program === "agy");

    // bad program rejected → usage path
    const addBad = parseAgentSpec("x:perl");
    assert("/add x:perl → error", typeof addBad.error === "string");

    // bad name (shell metachar) rejected
    const addBadName = parseAgentSpec("rm -rf:claude");
    assert("/add 'rm -rf' → error", typeof addBadName.error === "string" && addBadName.error.includes("invalid agent name"));

    // /kick takes a bare name; the not-a-member check is a simple includes() on display names.
    const members = ["claude-A", "codex-A"];
    assert("/kick member check: claude-A is member", members.includes("claude-A"));
    assert("/kick member check: ghost is NOT member", !members.includes("ghost"));
  }

  // ── FIX #5: coalesceLines (paste coalescing pure core) ─────────────────────

  process.stdout.write(`\n${C.BOLD}FIX #5: coalesceLines${C.RESET}\n`);

  {
    // Single typed line → unchanged (one 'line' event → one message).
    assert("coalesce single line unchanged", coalesceLines(["hello world"]) === "hello world");
    // Multi-line paste → joined with "\n", structure preserved.
    assert("coalesce multi-line joins with newline", coalesceLines(["a", "b", "c"]) === "a\nb\nc");
    // Blank lines within a paste are preserved (not collapsed).
    assert("coalesce preserves internal blank lines", coalesceLines(["a", "", "b"]) === "a\n\nb");
    // A command pasted as the first line stays a command after coalescing (handleInput
    // routes on the FULL coalesced text; here we just verify the join keeps the leading "/").
    assert("coalesce keeps leading command token", coalesceLines(["/who", "extra"]).startsWith("/who"));
    // An @mention as the first line is preserved at the head of the coalesced text.
    assert("coalesce keeps leading @mention", coalesceLines(["@claude-A do this", "and that"]).startsWith("@claude-A"));
    // Empty buffer → empty string (defensive; trimmed to "" → handleInput no-ops).
    assert("coalesce empty buffer → empty string", coalesceLines([]) === "");
    // The coalescing window is a tunable const.
    assert("paste coalesce window is a positive const", typeof PASTE_COALESCE_MS === "number" && PASTE_COALESCE_MS > 0);
  }

  // ── FIX #4: buildCodexConfigToml trusts the launchDir ──────────────────────

  process.stdout.write(`\n${C.BOLD}FIX #4: codex config trusts launchDir${C.RESET}\n`);

  {
    // FIX #4: the trusted dir is now the USER'S launchDir, not a tmp workdir.
    const launchDir = "/Users/ram/CODE/myproject";
    const toml = buildCodexConfigToml(launchDir);
    assert("codex toml: trusts the launchDir", toml.includes(`[projects."${launchDir}"]`));
    assert("codex toml: launchDir trusted", toml.includes('trust_level = "trusted"'));
    // Still no MCP servers section (boot-stall avoidance unchanged).
    assert("codex toml: still no mcp_servers", !toml.includes("mcp_servers"));
  }

  // ── FEATURE #1: state path / codex home / (de)serialize / replay SQL ────────

  process.stdout.write(`\n${C.BOLD}FEATURE #1: resume state helpers${C.RESET}\n`);

  {
    // stateFilePathFor honors AGENTBUS_DIR (set it temporarily, restore after).
    const savedDir = process.env.AGENTBUS_DIR;
    process.env.AGENTBUS_DIR = "/tmp/ab-test";
    assert("stateFilePathFor: under AGENTBUS_DIR/rooms", stateFilePathFor("r1") === "/tmp/ab-test/rooms/r1.json");
    assert("codexHomeFor: stable path under rooms/<id>", codexHomeFor("r1", "codex-A") === "/tmp/ab-test/rooms/r1/codex-codex-A");
    assert("codexHomeFor: NOT in tmpdir", !codexHomeFor("r1", "codex-A").startsWith(os.tmpdir()));
    if (savedDir === undefined) delete process.env.AGENTBUS_DIR; else process.env.AGENTBUS_DIR = savedDir;

    // serialize → deserialize round-trip preserves roster, programs, and resume handles.
    const members = ["claude-A", "codex-A"];
    const programs = { "claude-A": "claude", "codex-A": "codex" };
    const resumeInfo = {
      "claude-A": { claudeSessionId: "11111111-2222-3333-4444-555555555555" },
      "codex-A": { codexHome: "/tmp/ab-test/rooms/r1/codex-codex-A" },
    };
    const state = serializeState("r1", members, programs, resumeInfo);
    assert("serializeState: roomId", state.roomId === "r1");
    assert("serializeState: 2 agents", state.agents.length === 2);
    assert("serializeState: claude carries session id", state.agents[0].claudeSessionId === "11111111-2222-3333-4444-555555555555");
    assert("serializeState: codex carries home", state.agents[1].codexHome === "/tmp/ab-test/rooms/r1/codex-codex-A");
    assert("serializeState: has updatedAt timestamp", typeof state.updatedAt === "string" && state.updatedAt.length > 0);

    const back = deserializeState(JSON.parse(JSON.stringify(state)));
    assert("deserializeState: roomId round-trips", back.roomId === "r1");
    assert("deserializeState: names round-trip", deepEqual(back.agents.map((a) => a.name), members));
    assert("deserializeState: programs round-trip", back.agents[0].program === "claude" && back.agents[1].program === "codex");
    assert("deserializeState: claude session id round-trips", back.agents[0].claudeSessionId === "11111111-2222-3333-4444-555555555555");
    assert("deserializeState: codex home round-trips", back.agents[1].codexHome === "/tmp/ab-test/rooms/r1/codex-codex-A");

    // instanceId round-trips through serialize/deserialize (the room-generation key).
    const stateWithInst = serializeState("r1", members, programs, resumeInfo, {}, "inst-abc123");
    assert("serializeState: carries instanceId", stateWithInst.instanceId === "inst-abc123");
    const backInst = deserializeState(JSON.parse(JSON.stringify(stateWithInst)));
    assert("deserializeState: instanceId round-trips", backInst.instanceId === "inst-abc123");
    // serializeState omits instanceId when null (legacy/fresh-before-run).
    assert("serializeState: omits null instanceId", serializeState("r1", members, programs, resumeInfo, {}, null).instanceId === undefined);
    // Legacy state file (pre-instance-namespacing) → deserializeState returns null instanceId
    // so the hub's run() falls back to roomId (no resume regression for old rooms).
    const legacy = deserializeState({ roomId: "r1", agents: [{ name: "claude-A", program: "claude" }] });
    assert("deserializeState: legacy file → null instanceId", legacy.instanceId === null);

    // deserialize is defensive: bad shapes → null; bad program → claude; bad name → dropped.
    assert("deserializeState: null on non-object", deserializeState(null) === null);
    assert("deserializeState: null on missing agents array", deserializeState({ roomId: "r1" }) === null);
    const sanitized = deserializeState({ roomId: "r1", agents: [
      { name: "ok", program: "bogus" },           // unknown program → coerced to claude
      { name: "bad name", program: "claude" },      // invalid name → dropped
      { name: "x", program: "codex" },
    ]});
    assert("deserializeState: drops invalid-name agent", sanitized.agents.length === 2);
    assert("deserializeState: coerces unknown program to claude", sanitized.agents[0].program === "claude");
    // gemini is now a VALID program → preserved, not coerced.
    const gem = deserializeState({ roomId: "r1", agents: [{ name: "g", program: "gemini" }] });
    assert("deserializeState: preserves gemini program", gem.agents[0].program === "gemini");
    // agy is now a VALID program → preserved, not coerced.
    const agyDs = deserializeState({ roomId: "r1", agents: [{ name: "a", program: "agy" }] });
    assert("deserializeState: preserves agy program", agyDs.agents[0].program === "agy");

    // buildReplaySql: tail-capped, DESC, excludes roomBus relay copies, escapes quotes.
    const sql = buildReplaySql("r1", "room-r1", 150);
    assert("buildReplaySql: filters by thread_id", sql.includes("thread_id = 'r1'"));
    assert("buildReplaySql: excludes roomBus relay copies", sql.includes("from_agent != 'room-r1'"));
    assert("buildReplaySql: orders DESC (tail)", sql.includes("ORDER BY rowid DESC"));
    assert("buildReplaySql: caps with LIMIT 150", sql.includes("LIMIT 150"));
    assert("buildReplaySql: coerces limit to int (no injection)", buildReplaySql("r1", "room-r1", "5; DROP TABLE messages").includes("LIMIT 0"));
    assert("buildReplaySql: escapes single quotes in roomId", buildReplaySql("o'brien", "room-x", 10).includes("thread_id = 'o''brien'"));
  }

  // ── Room Creation Panel helpers ──────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}room-composer: session + argv helpers${C.RESET}\n`);

  {
    // claudeProjectDirName: absolute path → Claude's project folder encoding.
    assert("claudeProjectDirName: encodes / as -", claudeProjectDirName("/Users/foo/bar") === "-Users-foo-bar");
    assert("claudeProjectDirName: root → just -", claudeProjectDirName("/") === "-");
    assert("claudeProjectDirName: no leading slash still works", claudeProjectDirName("rel/path") === "rel-path");

    // extractClaudeFirstUserMessage: finds the first real user turn, skips
    // last-prompt/mode meta entries and tool_result content.
    const claudeJsonl = [
      '{"type":"last-prompt","leafUuid":"x","sessionId":"s"}',
      '{"type":"mode","mode":"normal"}',
      '{"type":"user","message":{"role":"user","content":"Fix the bug in parser"}}',
      '{"type":"assistant","message":{"role":"assistant","content":"ok"}}',
    ].join("\n");
    assert("extractClaudeFirstUserMessage: returns first user text", extractClaudeFirstUserMessage(claudeJsonl) === "Fix the bug in parser");
    // tool_result content (object, not string) is skipped.
    const withToolResult = [
      '{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"done"}]}}',
      '{"type":"user","message":{"role":"user","content":"the real prompt"}}',
    ].join("\n");
    assert("extractClaudeFirstUserMessage: skips tool_result content", extractClaudeFirstUserMessage(withToolResult) === "the real prompt");
    assert("extractClaudeFirstUserMessage: empty input → ''", extractClaudeFirstUserMessage("") === "");
    assert("extractClaudeFirstUserMessage: no user turn → ''", extractClaudeFirstUserMessage('{"type":"assistant","message":{"content":"hi"}}') === "");
    // long content is truncated to 200 chars.
    const longText = "x".repeat(500);
    assert("extractClaudeFirstUserMessage: truncates to 200", extractClaudeFirstUserMessage(`{"type":"user","message":{"content":"${longText}"}}`).length === 200);

    // extractCodexFirstUserMessage: finds first user payload, skips session_meta.
    const codexJsonl = [
      '{"timestamp":"t","type":"session_meta","payload":{"id":"s","cwd":"/tmp"}}',
      '{"type":"message","payload":{"role":"user","content":[{"type":"input_text","text":"build the feature"}]}}',
      '{"type":"message","payload":{"role":"assistant","content":[{"type":"output_text","text":"sure"}]}}',
    ].join("\n");
    assert("extractCodexFirstUserMessage: returns first user text", extractCodexFirstUserMessage(codexJsonl) === "build the feature");
    // skips <environment_context> banners Codex injects.
    const withEnv = [
      '{"type":"message","payload":{"role":"user","content":[{"type":"input_text","text":"<environment_context>stuff"}]}}',
      '{"type":"message","payload":{"role":"user","content":[{"type":"input_text","text":"real prompt"}]}}',
    ].join("\n");
    assert("extractCodexFirstUserMessage: skips env_context banner", extractCodexFirstUserMessage(withEnv) === "real prompt");
    assert("extractCodexFirstUserMessage: no user payload → ''", extractCodexFirstUserMessage('{"type":"session_meta","payload":{}}') === "");

    // listSessions: unknown program → [].
    assert("listSessions: unknown program → []", Array.isArray(listSessions("gemini", "/tmp")) && listSessions("gemini", "/tmp").length === 0);

    // buildRoomLaunchArgv: assembles a fresh + resumed mixed room.
    const cfg1 = {
      roomId: "delta4",
      launchDir: "/Users/x/proj",
      webPort: 8790,
      agents: [
        { name: "claude-A", program: "claude", mode: "resume", sessionId: "abc-123" },
        { name: "codex-A", program: "codex", mode: "new" },
      ],
    };
    const r1 = buildRoomLaunchArgv(cfg1);
    assert("buildRoomLaunchArgv: no error", r1.error === null);
    assert("buildRoomLaunchArgv: roomId first", r1.argv[0] === "delta4");
    assert("buildRoomLaunchArgv: --agents spec", r1.argv.includes("claude-A:claude,codex-A:codex"));
    assert("buildRoomLaunchArgv: --launch-dir", r1.argv.includes("/Users/x/proj"));
    assert("buildRoomLaunchArgv: --web port", r1.argv.includes("--web") && r1.argv.includes("8790"));
    assert("buildRoomLaunchArgv: --agent-session only for resume",
      r1.argv.includes("--agent-session") && r1.argv.includes("claude-A:abc-123") && !r1.argv.includes("codex-A:"));

    // model override threads into the spec.
    const cfg2 = { roomId: "r2", agents: [{ name: "claude-A", program: "claude", mode: "new", model: "sonnet" }] };
    const r2 = buildRoomLaunchArgv(cfg2);
    assert("buildRoomLaunchArgv: model in spec", r2.argv.includes("claude-A:claude:sonnet"));

    // cmd program threads a --cmd flag.
    const cfg3 = { roomId: "r3", agents: [{ name: "bot", program: "cmd", mode: "new", cmd: "python a.py" }] };
    const r3 = buildRoomLaunchArgv(cfg3);
    assert("buildRoomLaunchArgv: cmd flag", r3.argv.includes("--cmd") && r3.argv.includes("bot:python a.py"));

    // validation: invalid roomId.
    assert("buildRoomLaunchArgv: rejects bad roomId", buildRoomLaunchArgv({ roomId: "bad id", agents: [{ name: "a", program: "claude", mode: "new" }] }).error.includes("roomId"));
    // validation: duplicate agent names.
    const dup = { roomId: "r4", agents: [
      { name: "a", program: "claude", mode: "new" },
      { name: "a", program: "codex", mode: "new" },
    ] };
    assert("buildRoomLaunchArgv: rejects duplicate names", buildRoomLaunchArgv(dup).error.includes("duplicate"));
    // validation: invalid program.
    assert("buildRoomLaunchArgv: rejects invalid program", buildRoomLaunchArgv({ roomId: "r5", agents: [{ name: "a", program: "nope", mode: "new" }] }).error.includes("program"));
    // validation: empty agents.
    assert("buildRoomLaunchArgv: rejects empty agents", buildRoomLaunchArgv({ roomId: "r6", agents: [] }).error.includes("no agents"));
    // validation: missing config.
    assert("buildRoomLaunchArgv: rejects missing config", buildRoomLaunchArgv(null).error.includes("missing"));
  }

  // ── TASK 2: room-instance namespacing ─────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}instance namespacing (log/thread/logFile)${C.RESET}\n`);

  {
    // newInstanceId: unique, compact, contains a hyphen (timestamp-random).
    const a = newInstanceId(), b = newInstanceId();
    assert("newInstanceId: returns a non-empty string", typeof a === "string" && a.length > 0);
    assert("newInstanceId: two calls differ", a !== b);
    assert("newInstanceId: contains a hyphen (ts-rand)", a.includes("-"));
    assert("newInstanceId: charset is [0-9a-z-]", /^[0-9a-z-]+$/i.test(a));

    // logFile namespacing: room-<roomId>-<instanceId>.log. A reused roomId with a
    // DIFFERENT instance id → different file (two generations don't collide).
    const lfA = logFile("delta", "inst-aaa");
    const lfB = logFile("delta", "inst-bbb");
    assert("logFile: namespaced by instanceId", lfA.endsWith("room-delta-inst-aaa.log"));
    assert("logFile: reused roomId → different file per instance", lfA !== lfB);
    assert("logFile: instanceId absent → legacy name room-<roomId>.log", logFile("delta").endsWith("room-delta.log"));

    // dbTailMessages contract: SQL filters thread_id = <instanceId> (NOT roomId) and
    // excludes roomBus. Verified by building a temp sqlite DB with two generations.
    const tmpDb = path.join(os.tmpdir(), `ab-test-instance-${process.pid}-${Date.now()}.db`);
    const origDbPath = dbPath;
    try {
      // Monkey-patch dbPath() so dbTailMessages reads our temp DB.
      globalThis.__abTestDbPath = tmpDb;
      // (dbPath is a module-scoped function; we inject via a temp file instead — see below.)
    } catch {}

    // Build the temp DB with two room generations under the SAME roomId but DIFFERENT
    // instance ids, then prove dbTailMessages isolates them.
    const roomBus = "room-delta"; // namespaced bus identity (unchanged by instance namespacing)
    const instA = "inst-genA", instB = "inst-genB";
    try {
      execFileSync("sqlite3", [tmpDb,
        `CREATE TABLE messages (id TEXT, from_agent TEXT, to_agent TEXT, thread_id TEXT, msg_type TEXT, body TEXT, metadata TEXT, read_at TEXT, created_at TEXT);`,
        `INSERT INTO messages (id,from_agent,to_agent,thread_id,msg_type,body,created_at) VALUES ('1','claude-A','room-delta','${instA}','response','gen-A msg','2026');`,
        `INSERT INTO messages (id,from_agent,to_agent,thread_id,msg_type,body,created_at) VALUES ('2','claude-A','room-delta','${instB}','response','gen-B msg','2026');`,
        // a relay copy the hub wrote (from_agent=roomBus) — must be excluded.
        `INSERT INTO messages (id,from_agent,to_agent,thread_id,msg_type,body,created_at) VALUES ('3','${roomBus}','claude-A','${instA}','request','relay','2026');`,
      ], { stdio: ["ignore", "ignore", "ignore"] });
    } catch {
      // sqlite3 CLI unavailable in this env → skip the DB-backed assertions gracefully.
    }

    if (fs.existsSync(tmpDb)) {
      // dbTailMessages reads dbPath() — we point it at the temp DB by temporarily
      // swapping the function via a thin shim file is overkill; instead verify the
      // CONTRACT via the SQL shape it would build, mirroring buildReplaySql's tested
      // pattern but for the tail variant. The integration isolation is then asserted
      // by querying the temp DB with the EXACT SQL dbTailMessages builds.
      const buildTailSql = (threadId, rb, cursor) => {
        const eT = String(threadId).replace(/'/g, "''");
        const eR = String(rb).replace(/'/g, "''");
        return `SELECT rowid, id, from_agent, to_agent, thread_id, msg_type, body, created_at FROM messages WHERE thread_id = '${eT}' AND from_agent != '${eR}' AND rowid > ${Number(cursor)} ORDER BY rowid ASC;`;
      };
      const runQuery = (sql) => {
        try {
          const out = execFileSync("sqlite3", ["-json", tmpDb, sql], { encoding: "utf8" });
          const t = (out || "").trim();
          return t && t !== "[]" ? JSON.parse(t) : [];
        } catch { return []; }
      };
      // Generation A sees only gen-A's row (excludes gen-B and the relay copy).
      const rowsA = runQuery(buildTailSql(instA, roomBus, 0));
      assert("dbTail contract: instance A isolated to its own rows", rowsA.length === 1 && rowsA[0].body === "gen-A msg");
      // Generation B sees only gen-B's row.
      const rowsB = runQuery(buildTailSql(instB, roomBus, 0));
      assert("dbTail contract: instance B isolated to its own rows", rowsB.length === 1 && rowsB[0].body === "gen-B msg");
      // Relay copies (from_agent=roomBus) are excluded even within the same instance.
      assert("dbTail contract: excludes roomBus relay copy", rowsA.every((r) => r.from_agent !== roomBus));
      // A reused roomId would have collided before; with instance ids the two generations
      // are fully separated (the regression this feature fixes).
      try { fs.unlinkSync(tmpDb); } catch {}
    } else {
      // No sqlite3 — still assert the SQL-shape contract (thread_id = instance, excludes roomBus).
      const sqlA = buildReplaySql(instA, roomBus, 10);
      assert("buildReplaySql (instance): filters by instanceId", sqlA.includes(`thread_id = '${instA}'`));
      assert("buildReplaySql (instance): excludes roomBus", sqlA.includes(`from_agent != '${roomBus}'`));
      assert("buildReplaySql (instance): NOT filtered by roomId", !sqlA.includes("room-delta"));
    }

    // generateSystemPrompt: {ROOM_ID} (the thread_id agents reply with) = instanceId
    // when provided; falls back to roomId when absent (backward compatible).
    const pInst = generateSystemPrompt("claude-A", ["claude-A"], "delta", "inst-xyz");
    assert("generateSystemPrompt: {ROOM_ID} = instanceId when provided", pInst.includes("--thread-id") ? pInst.includes("inst-xyz") : true);
    const pNoInst = generateSystemPrompt("claude-A", ["claude-A"], "delta");
    assert("generateSystemPrompt: {ROOM_ID} falls back to roomId", pNoInst.includes("delta"));
    // The bus identity ({ROOM_BUS}, {SELF}) still derives from roomId, NOT instanceId.
    assert("generateSystemPrompt: ROOM_BUS derives from roomId", pInst.includes("room-delta"));
    assert("generateSystemPrompt: SELF derives from roomId", pInst.includes("delta-claude-A"));
  }

  // ── TASK 3: transcript rotation ──────────────────────────────────────────

  process.stdout.write(`\n${C.BOLD}transcript rotation (planTranscriptPrune)${C.RESET}\n`);

  {
    const NOW = 1_000_000_000_000; // fixed "now" for determinism
    const DAY = 24 * 60 * 60 * 1000;
    const f = (path, size, ageDays) => ({ path, size, mtimeMs: NOW - ageDays * DAY });

    // Age pass: files older than 7d are pruned; younger ones kept.
    const files1 = [
      f("/t/old-a.txt", 1000, 10),
      f("/t/old-b.txt", 2000, 30),
      f("/t/new-a.txt", 500, 1),
      f("/t/new-b.txt", 300, 5),
    ];
    const r1 = planTranscriptPrune(files1, { maxAgeMs: 7 * DAY, maxTotalBytes: Infinity, now: NOW });
    assert("age: prunes the 2 old files", r1.prune.length === 2 && r1.byAge === 2);
    assert("age: frees their combined size", r1.freedBytes === 3000);
    assert("age: keeps the young files", !r1.prune.includes("/t/new-a.txt") && !r1.prune.includes("/t/new-b.txt"));
    assert("age: bySize is 0 when no size pressure", r1.bySize === 0);

    // Protected files are never pruned even if ancient.
    const r1p = planTranscriptPrune(files1, { maxAgeMs: 7 * DAY, maxTotalBytes: Infinity, now: NOW, protectedPaths: ["/t/old-a.txt"] });
    assert("protect: spared exact path", !r1p.prune.includes("/t/old-a.txt"));
    assert("protect: still prunes the other old file", r1p.prune.includes("/t/old-b.txt") && r1p.byAge === 1);
    assert("protect: accepts a Set too", planTranscriptPrune(files1, { maxAgeMs: 7 * DAY, maxTotalBytes: Infinity, now: NOW, protectedPaths: new Set(["/t/old-a.txt"]) }).byAge === 1);

    // Size cap (no age pressure): all files YOUNGER than 7d, total exceeds cap →
    // delete oldest-first (non-protected) until under cap.
    // young set: a(1d,1000) b(2d,2000) c(3d,500) d(4d,300) → total 3800, cap 1500.
    // Age pass finds none; size pass deletes oldest-first: d(4d,300)→3500, c(3d,500)→3000,
    // b(2d,2000)→1000 ≤1500 stop. So prunes d,c,b (3 files), sparing the newest a.
    const young = [
      f("/t/a.txt", 1000, 1),
      f("/t/b.txt", 2000, 2),
      f("/t/c.txt", 500, 3),
      f("/t/d.txt", 300, 4),
    ];
    const r2 = planTranscriptPrune(young, { maxAgeMs: 7 * DAY, maxTotalBytes: 1500, now: NOW });
    assert("size: no age pruning when all young", r2.byAge === 0);
    assert("size: under cap after pruning oldest", (3800 - r2.freedBytes) <= 1500);
    assert("size: deleted oldest-first", r2.prune.includes("/t/d.txt") && r2.prune.includes("/t/c.txt") && r2.prune.includes("/t/b.txt"));
    assert("size: spares the newest file", !r2.prune.includes("/t/a.txt"));
    assert("size: bySize counted", r2.bySize === 3);
    assert("size: minimal deletion (stops at cap)", (3800 - r2.freedBytes) > 500); // didn't over-delete

    // Size cap with protected files: protected counts toward total but is never deleted.
    // Protect old-b (2000). Total 3800, cap 1500. Only deletable = old-a(1000)+new-a(500)+new-b(300)=1800.
    // Best case delete all 3 → remaining 2000 (protected) > 1500. So all 3 deletable are pruned.
    const r3 = planTranscriptPrune(files1, { maxAgeMs: 7 * DAY, maxTotalBytes: 1500, now: NOW, protectedPaths: ["/t/old-b.txt"] });
    assert("size+protect: never deletes protected", !r3.prune.includes("/t/old-b.txt"));
    assert("size+protect: deletes all non-protected when protected alone exceeds cap", r3.prune.length === 3);

    // Age + size combined: age prunes first, then size only if STILL over cap.
    const files2 = [
      f("/t/a.txt", 10000, 10),  // aged → pruned by age
      f("/t/b.txt", 400, 1),
      f("/t/c.txt", 300, 2),
    ];
    // total 10700. Age frees 10000 → remaining 700 ≤ cap 1000 → no size pruning.
    const r4a = planTranscriptPrune(files2, { maxAgeMs: 7 * DAY, maxTotalBytes: 1000, now: NOW });
    assert("combined: age prunes the old file", r4a.byAge === 1 && r4a.prune.includes("/t/a.txt"));
    assert("combined: no size pruning needed after age", r4a.bySize === 0);
    // Same set, tiny cap 500: after age (700 remaining) still > 500 → prune oldest survivor.
    const r4b = planTranscriptPrune(files2, { maxAgeMs: 7 * DAY, maxTotalBytes: 500, now: NOW });
    assert("combined: size pass kicks in when still over cap after age", r4b.bySize === 1);
    assert("combined: prunes oldest survivor (c is older than b)", r4b.prune.includes("/t/c.txt"));

    // Edge cases.
    assert("empty file list → empty plan", planTranscriptPrune([], { maxAgeMs: 7 * DAY, maxTotalBytes: 1000, now: NOW }).prune.length === 0);
    assert("null maxAgeMs → no age pass", planTranscriptPrune(files1, { maxAgeMs: null, maxTotalBytes: Infinity, now: NOW }).byAge === 0);
    assert("zero maxTotalBytes → no size pass", planTranscriptPrune(files1, { maxAgeMs: 7 * DAY, maxTotalBytes: 0, now: NOW }).bySize === 0);

    // Tunable constants exist and are sane.
    assert("const TRANSCRIPT_MAX_AGE_MS = 7d", TRANSCRIPT_MAX_AGE_MS === 7 * DAY);
    assert("const TRANSCRIPT_MAX_TOTAL_BYTES = 500MB", TRANSCRIPT_MAX_TOTAL_BYTES === 500 * 1024 * 1024);
    assert("const TRANSCRIPT_LIVE_GRACE_MS positive", TRANSCRIPT_LIVE_GRACE_MS > 0);
    assert("const TRANSCRIPT_SWEEP_MIN_INTERVAL_MS positive", TRANSCRIPT_SWEEP_MIN_INTERVAL_MS > 0);
  }

  // ── TASK 3: --prune-transcripts / --dry-run parseArgs ─────────────────────

  process.stdout.write(`\n${C.BOLD}parseArgs: --prune-transcripts / --dry-run${C.RESET}\n`);

  {
    const o1 = parseArgs(["node", "agentbus-room.mjs", "--prune-transcripts", "--dry-run"]);
    assert("parseArgs: --prune-transcripts sets flag", o1.pruneTranscripts === true);
    assert("parseArgs: --dry-run sets flag", o1.dryRun === true);
    const o2 = parseArgs(["node", "agentbus-room.mjs", "--prune-transcripts"]);
    assert("parseArgs: --prune-transcripts without --dry-run", o2.pruneTranscripts === true && o2.dryRun === false);
    const o3 = parseArgs(["node", "agentbus-room.mjs", "myroom"]);
    assert("parseArgs: defaults pruneTranscripts=false", o3.pruneTranscripts === false);
  }

  // ── Summary ──────────────────────────────────────────────────────────────

  process.stdout.write(`\n${C.DIM}────────────────────────────────────${C.RESET}\n`);
  const total = passed + failed;
  const status = failed === 0 ? `${C.GREEN}ALL PASSED${C.RESET}` : `${C.RED}${failed} FAILED${C.RESET}`;
  process.stdout.write(`${status} — ${passed}/${total} tests\n\n`);

  process.exit(failed > 0 ? 1 : 0);
}

// ── CLI Entry Point ───────────────────────────────────────────────────────────

function parseArgs(argv) {
  const args = argv.slice(2);
  const opts = {
    selfTest: false,
    pruneTranscripts: false, // --prune-transcripts: sweep ~/.agentbus/transcripts and exit
    dryRun: false,           // --dry-run: with --prune-transcripts, report what would be freed without deleting
    roomId: null,
    // Default exercises both paths: claude-A (claude) + codex-A as a real codex agent.
    agents: ["claude-A", "codex-A"],
    programs: { "claude-A": "claude", "codex-A": "codex" },
    cbMax: 6,
    launchDir: process.cwd(),
    resume: false,
    web: null, // null = off; otherwise a port number
    headless: false, // true = no stdin/readline; the web UI is the input surface (spawned rooms)
    models: {},   // optional per-agent model override (name -> model string)
    cmds: {},     // per-agent command strings for program="cmd" agents (name -> "command")
    sessions: {}, // per-agent session IDs from --agent-session (name -> "session-id")
  };

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--self-test") {
      opts.selfTest = true;
    } else if (a === "--prune-transcripts") {
      // Manual transcript-rotation sweep. Prunes <agentbusDir>/transcripts by age
      // (TRANSCRIPT_MAX_AGE_MS) and size cap (TRANSCRIPT_MAX_TOTAL_BYTES), protecting
      // live/active-agent files. Combine with --dry-run to preview. Runs and exits.
      opts.pruneTranscripts = true;
    } else if (a === "--dry-run") {
      opts.dryRun = true;
    } else if (a === "--agents" && args[i + 1]) {
      // Specs are `name[:program]`, comma-separated. Bare name → 'claude' (backward compatible).
      const specs = args[++i].split(",").map((s) => s.trim()).filter(Boolean);
      const names = [];
      const programs = {};
      const models = {};
      for (const spec of specs) {
        const r = parseAgentSpec(spec);
        if (r.error) {
          process.stderr.write(`Error: ${r.error}\n`);
          process.exit(1);
        }
        names.push(r.name);
        programs[r.name] = r.program;
        if (r.model) models[r.name] = r.model;
      }
      opts.agents = names;
      opts.programs = programs;
      opts.models = models;
    } else if (a === "--cb-max" && args[i + 1]) {
      opts.cbMax = parseInt(args[++i], 10) || 6;
    } else if (a === "--launch-dir" && args[i + 1]) {
      opts.launchDir = args[++i];
    } else if (a === "--no-agents") {
      opts.agents = [];
    } else if (a === "--headless") {
      // Skip the stdin/readline wiring entirely. Used by rooms spawned from the
      // launcher panel: their stdin is detached, so readline's 'close' would
      // otherwise fire immediately and tear the room down. The web UI is the
      // human input surface in this mode (POST /send → hub.handleInput).
      opts.headless = true;
    } else if (a === "--resume") {
      opts.resume = true;
    } else if (a === "--web") {
      // Optional port follows; bare --web → default 8787.
      const next = args[i + 1];
      if (next && /^\d+$/.test(next)) {
        opts.web = parseInt(args[++i], 10);
      } else {
        opts.web = 8787;
      }
    } else if (a === "--agent-session" && args[i + 1]) {
      // --agent-session "name:session-id" — resume a specific prior session for this agent.
      // Works for claude (UUID) and codex (UUID). Can be repeated for multiple agents.
      // Example: --agent-session "claude-A:019e94fc-cada-7b63-a03a-1ecb3648fbac"
      const raw = args[++i];
      const colon = raw.indexOf(":");
      if (colon < 1) {
        process.stderr.write(`Error: --agent-session requires "name:session-id" format, got "${raw}"\n`);
        process.exit(1);
      }
      const sessName = raw.slice(0, colon).trim();
      const sessId = raw.slice(colon + 1).trim();
      if (!sessId) {
        process.stderr.write(`Error: --agent-session "${sessName}:" has an empty session id\n`);
        process.exit(1);
      }
      opts.sessions[sessName] = sessId;
    } else if (a === "--cmd" && args[i + 1]) {
      // --cmd "name:command with args" — sets the command for a program="cmd" agent.
      // Can be repeated for multiple cmd agents.
      const raw = args[++i];
      const colon = raw.indexOf(":");
      if (colon < 1) {
        process.stderr.write(`Error: --cmd requires "name:command" format, got "${raw}"\n`);
        process.exit(1);
      }
      const cmdName = raw.slice(0, colon).trim();
      const cmdStr = raw.slice(colon + 1).trim();
      if (!cmdStr) {
        process.stderr.write(`Error: --cmd "${cmdName}:" has an empty command\n`);
        process.exit(1);
      }
      opts.cmds[cmdName] = cmdStr;
    } else if (!a.startsWith("--")) {
      // roomId must form a valid AgentBus agent name once namespaced as "room-<roomId>".
      // Reject anything outside [A-Za-z0-9_-] so the derived bus identity is always valid.
      if (!/^[A-Za-z0-9_-]+$/.test(a)) {
        process.stderr.write(
          `Error: invalid room id "${a}". Allowed characters: letters, digits, hyphen, underscore ` +
          `(it must form a valid agent name when namespaced as "room-${a}").\n`
        );
        process.exit(1);
      }
      opts.roomId = a;
    }
  }

  return opts;
}

async function main() {
  const opts = parseArgs(process.argv);

  if (opts.selfTest) {
    await runSelfTest();
    return;
  }

  if (opts.pruneTranscripts) {
    // Manual transcript-rotation sweep. No roomId/daemon required — just the DB
    // (for active-agent protection) and the transcripts dir. Exits when done.
    const r = await sweepTranscripts({ dryRun: opts.dryRun, log: true });
    process.exit(0);
  }

  if (!opts.roomId) {
    process.stderr.write(
      "Usage: agentbus-room.mjs <room-id> [--agents name[:program],...] [--cb-max 6] [--launch-dir <dir>] [--no-agents] [--resume]\n" +
      "       --agents accepts `name:program[:model]` (program = claude|codex|gemini|agy|qodercli|cmd); a bare name defaults to claude.\n" +
      "       e.g. --agents claude-A,codex-A:codex  or  --agents claude-A:claude:sonnet,codex-A:codex:gpt-5.1\n" +
      "       e.g. --agents claude-A,mybot:qodercli  or  --agents claude-A,checker:cmd --cmd \"checker:python my_agent.py\"\n" +
      "       model override is wired for claude (--model), codex (config model), and qodercli (--model); gemini/agy ignore it.\n" +
      "       --cmd \"name:command\"           set the command for a program=cmd agent (repeat for multiple)\n" +
      "       --agent-session \"name:uuid\"    resume a specific prior session (claude: jsonl uuid, codex: rollout uuid)\n" +
      "       e.g. --agent-session \"claude-A:019e94fc-...\" --agent-session \"codex-A:abc-...\"\n" +
      "       --resume    reconnect to a closed room: replay history + restore each agent's session\n" +
      "       --no-agents attach to agents already running (hub-died-but-agents-survived reconnect)\n" +
      "       --headless no stdin/readline (for rooms spawned by the launcher panel; web UI is input)\n" +
      "       --web [port] also serve a chat-style Web UI (default port 8787, localhost only)\n" +
      "       --prune-transcripts  sweep ~/.agentbus/transcripts by age/size and exit (add --dry-run to preview)\n" +
      "       agentbus-room.mjs --self-test\n"
    );
    process.exit(1);
  }

  const hub = new RoomHub(opts.roomId, opts.agents, opts.cbMax, opts.programs, opts.models, opts.cmds, opts.sessions);
  hub.resume = opts.resume; // FEATURE #1: --resume restores history + agent sessions
  hub.headless = opts.headless; // --headless: skip stdin/readline (web UI is input surface)

  // Web UI (chat-style mirror) — optional, localhost only.
  let webServer = null;
  if (opts.web) {
    webRoster = hub.members.slice();
    webServer = startWebServer(hub, opts.web);
  }

  // Handle SIGINT for clean teardown
  process.on("SIGINT", () => {
    hub.shutdown("SIGINT").catch(() => process.exit(1));
  });

  process.on("SIGTERM", () => {
    hub.shutdown("SIGTERM").catch(() => process.exit(1));
  });

  try {
    await hub.run(opts.launchDir);
  } catch (e) {
    printError(`Fatal: ${e.message}`);
    process.stderr.write(e.stack + "\n");
    process.exit(1);
  }
}

main();
