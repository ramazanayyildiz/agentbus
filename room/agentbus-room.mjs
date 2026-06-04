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
const VALID_PROGRAMS = ["claude", "codex", "gemini", "agy"];

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
  const trimmed = String(spec).trim();
  const colon = trimmed.indexOf(":");
  const name = colon === -1 ? trimmed : trimmed.slice(0, colon);
  const program = colon === -1 ? "claude" : trimmed.slice(colon + 1);
  if (!AGENT_NAME_RE.test(name)) {
    return { error: `invalid agent name "${name}" — only [A-Za-z0-9_-] allowed` };
  }
  if (!VALID_PROGRAMS.includes(program)) {
    return { error: `invalid program "${program}" for agent "${name}" — must be one of ${VALID_PROGRAMS.join(", ")}` };
  }
  return { name, program };
}

/**
 * Build the exact config.toml contents for an isolated Codex agent.
 * Pre-trusts the given directory (no folder-trust modal) and omits [mcp_servers.*] to
 * avoid the heavy MCP boot stall. Pure function — testable.
 * FIX #4: the trusted dir is now the USER'S launchDir (where codex runs via --cd),
 * not a throwaway tmp workdir — so the agent can see and work in the real project.
 */
function buildCodexConfigToml(trustedDir) {
  return (
    `model = "gpt-5.5"\n` +
    `model_reasoning_effort = "low"\n` +
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
 * Serialize hub state to the on-disk JSON shape. Pure — testable round-trip.
 * agents[] carries each member's program + (program-specific) resume handle:
 *   codex → codexHome path; claude → claudeSessionId (uuid). Either may be absent.
 */
function serializeState(roomId, members, programs, resumeInfo) {
  return {
    roomId,
    updatedAt: new Date().toISOString(),
    agents: members.map((name) => {
      const a = { name, program: programs[name] || "claude" };
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
      codexHome: typeof a.codexHome === "string" ? a.codexHome : undefined,
      claudeSessionId: typeof a.claudeSessionId === "string" ? a.claudeSessionId : undefined,
    }));
  return { roomId: obj.roomId, agents };
}

/**
 * Build the SQL that replays the prior thread on --resume: the last `limit` rows
 * for this room, excluding the hub's relay copies (from_agent = roomBus). We select
 * DESC+LIMIT to cap at the TAIL, then the caller reverses to chronological order.
 * Pure — returns the SQL string (roomId/roomBus single-quote-escaped, limit coerced int).
 */
function buildReplaySql(roomId, roomBus, limit) {
  const escapedRoomId = String(roomId).replace(/'/g, "''");
  const escapedRoomBus = String(roomBus).replace(/'/g, "''");
  return (
    `SELECT rowid, id, from_agent, to_agent, thread_id, msg_type, body, created_at ` +
    `FROM messages ` +
    `WHERE thread_id = '${escapedRoomId}' AND from_agent != '${escapedRoomBus}' ` +
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
 * Returns rows where thread_id = roomId AND from_agent != roomBus AND rowid > cursor.
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
 * and the room ID is quoted with SQLite single-quote escaping for the WHERE clause.
 */
function dbTailMessages(roomId, cursorRowid) {
  const db = dbPath();
  if (!fs.existsSync(db)) return Promise.resolve([]);

  // Escape roomId for SQLite single-quoted string (double the single quotes)
  const escapedRoomId = roomId.replace(/'/g, "''");
  // Relay copies are written with from_agent=roomBus ("room-<roomId>"); filter by that
  // SAME namespaced value so they don't render as duplicates of the agent's original.
  const escapedRoomBus = roomBusFor(roomId).replace(/'/g, "''");
  const sql =
    `SELECT rowid, id, from_agent, to_agent, thread_id, msg_type, body, created_at ` +
    `FROM messages ` +
    `WHERE thread_id = '${escapedRoomId}' AND from_agent != '${escapedRoomBus}' AND rowid > ${Number(cursorRowid)} ` +
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

function logFile(roomId) {
  return path.join(process.cwd(), `room-${roomId}.log`);
}

function appendLog(roomId, line) {
  try {
    fs.appendFileSync(logFile(roomId), line + "\n");
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

function renderMessage(msg, roomId) {
  const time = new Date(msg.created_at || Date.now()).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  // (A) DB-tail rows carry the busId in from_agent; strip "<roomId>-" for display.
  // roomBus rows are already filtered out by dbTailMessages, so this only sees agents.
  const author = displayName(msg.from_agent || msg.from || "?", roomId);
  renderBubble(author, agentColor(author), time, msg.body || "");
  appendLog(roomId, `[${time}] [${author}] ${msg.body || ""}`);
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

// Embedded single-page chat UI. No build step, no external assets, localhost only.
const WEB_HTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>AgentBus Room</title>
<style>
  :root { --bg:#0d1117; --panel:#161b22; --border:#30363d; --text:#e6edf3; --dim:#8b949e; --ram:#21262d; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--text); height:100vh; display:flex; }
  #side { width:200px; flex:0 0 200px; background:var(--panel); border-right:1px solid var(--border); padding:14px; overflow-y:auto; }
  #side h2 { font-size:11px; text-transform:uppercase; letter-spacing:.08em; color:var(--dim); margin:0 0 10px; }
  .member { display:flex; align-items:center; gap:8px; padding:4px 0; }
  .dot { width:8px; height:8px; border-radius:50%; background:#3fb950; flex:0 0 8px; }
  .dot.working { background:#d29922; animation:pulse 1s infinite; }
  @keyframes pulse { 50% { opacity:.3; } }
  #main { flex:1; display:flex; flex-direction:column; min-width:0; }
  #log { flex:1; overflow-y:auto; padding:18px 22px; }
  .msg { margin:0 0 18px; max-width:820px; }
  .msg .head { font-weight:600; font-size:13px; margin-bottom:3px; }
  .msg .head .time { color:var(--dim); font-weight:400; font-size:11px; margin-left:8px; }
  .msg .body { white-space:pre-wrap; word-wrap:break-word; }
  .msg.ram .body { background:var(--ram); border-radius:6px; padding:8px 11px; }
  .sys { color:var(--dim); font-size:12px; font-style:italic; margin:8px 0; }
  #composer { border-top:1px solid var(--border); padding:12px 16px; background:var(--panel); display:flex; gap:10px; }
  #input { flex:1; resize:none; background:var(--bg); color:var(--text); border:1px solid var(--border); border-radius:8px; padding:9px 12px; font:inherit; max-height:140px; }
  #input:focus { outline:none; border-color:#388bfd; }
  #send { background:#238636; color:#fff; border:none; border-radius:8px; padding:0 18px; font:inherit; font-weight:600; cursor:pointer; }
  #send:hover { background:#2ea043; }
  #status { font-size:11px; color:var(--dim); margin-top:14px; }
</style></head>
<body>
  <div id="side"><h2>Room</h2><div id="members"></div><div id="status">connecting…</div></div>
  <div id="main">
    <div id="log"></div>
    <div id="composer">
      <textarea id="input" rows="1" placeholder="Message the room…  (Enter to send, Shift+Enter for newline)"></textarea>
      <button id="send">Send</button>
    </div>
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
  function atBottom() { return log.scrollHeight - log.scrollTop - log.clientHeight < 80; }
  function scroll() { log.scrollTop = log.scrollHeight; }

  function addMsg(from, body, ts) {
    const stick = atBottom();
    const d = document.createElement('div');
    d.className = 'msg' + (from === 'ram' ? ' ram' : '');
    const head = document.createElement('div'); head.className = 'head';
    head.style.color = from === 'ram' ? '#3fb950' : colorFor(from);
    head.textContent = from;
    if (ts) { const t = document.createElement('span'); t.className='time'; t.textContent = ts; head.appendChild(t); }
    const b = document.createElement('div'); b.className = 'body'; b.textContent = body;
    d.appendChild(head); d.appendChild(b); log.appendChild(d);
    if (stick) scroll();
    if (from !== 'ram') noteMember(from);
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
      const row = document.createElement('div'); row.className='member';
      const dot = document.createElement('span'); dot.className = 'dot' + (presence[name]==='working'?' working':'');
      const lbl = document.createElement('span'); lbl.textContent = name; lbl.style.color = colorFor(name);
      row.appendChild(dot); row.appendChild(lbl); membersEl.appendChild(row);
    });
  }

  const es = new EventSource('/events');
  es.onopen = () => { statusEl.textContent = '● connected'; statusEl.style.color = '#3fb950'; };
  es.onerror = () => { statusEl.textContent = '○ reconnecting…'; statusEl.style.color = '#d29922'; };
  es.onmessage = (e) => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (m.type === 'msg') addMsg(m.from, m.body, m.ts);
    else if (m.type === 'system') addSys(m.body);
    else if (m.type === 'roster') { (m.members||[]).forEach(n => { seen.add(n); if(!(n in presence)) presence[n]='idle'; }); renderMembers(); }
    else if (m.type === 'presence') { presence[m.agent] = m.state; seen.add(m.agent); renderMembers(); }
  };

  async function send() {
    const body = input.value.trim();
    if (!body) return;
    input.value = ''; autosize();
    try { await fetch('/send', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ body }) }); }
    catch { addSys('send failed — is the hub still up?'); }
  }
  function autosize() { input.style.height='auto'; input.style.height = Math.min(input.scrollHeight, 140)+'px'; }
  input.addEventListener('input', autosize);
  input.addEventListener('keydown', (e) => { if (e.key==='Enter' && !e.shiftKey) { e.preventDefault(); send(); } });
  sendBtn.addEventListener('click', send);
  input.focus();
</script>
</body></html>`;

// Start the chat-UI web server. Localhost only (no auth). Routes:
//   GET /        → embedded chat UI
//   GET /events  → SSE stream of room events
//   POST /send   → {body} → routed through the SAME handleInput the terminal uses
function startWebServer(hub, port) {
  const server = http.createServer((req, res) => {
    const url = (req.url || "/").split("?")[0];

    if (req.method === "GET" && url === "/") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(WEB_HTML);
      return;
    }

    if (req.method === "GET" && url === "/events") {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      res.write(": connected\n\n");
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

function generateSystemPrompt(selfName, allAgents, roomId) {
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
  return template
    .replace(/{SELF}/g, selfBusId)
    .replace(/{PEERS}/g, peers || "(none yet)")
    .replace(/{ROOM_BUS}/g, roomBus)
    .replace(/{ROOM_ID}/g, roomId)
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

  child.stdout?.on("data", () => {}); // drain stdout
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
function launchClaudeAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resumeSessionId = null } = {}) {
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
  const spFile = path.join(os.tmpdir(), `room-sp-${agentName}-${roomId}.txt`);
  fs.writeFileSync(spFile, prompt);

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
    "--append-system-prompt-file", spFile,
    ...sessionArgs,
  ];

  printSystemMsg(`Launching claude agent ${agentName} in ${launchDir}${resumeSessionId ? ` (resuming ${claudeSessionId})` : ""}...`);

  const child = spawnRunner(agentName, args, launchDir, null);

  // Note: the 'exit' + member-pruning handler is attached in launchAgents() where 'this' is bound (M3).
  return { child, spFile, transcriptFile, emptyMcp, claudeSessionId };
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
function launchCodexAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume = false } = {}) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const codexHome = codexHomeFor(roomId, agentName);

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
  fs.writeFileSync(path.join(codexHome, "config.toml"), buildCodexConfigToml(launchDir));

  // AGENTS.md in the HOME (FIX #4): global instructions reused on every codex invocation,
  // including resume. Same room system prompt Claude gets — consistent roster/trust framing.
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
  fs.writeFileSync(path.join(codexHome, "AGENTS.md"), prompt);

  // (A) Register on the bus under the namespaced busId, not the raw display name.
  const busId = agentBusId(roomId, agentName);
  // FEATURE #1: on --resume, continue the most recent session in this home.
  // `resume --last --all` — --all disables codex's cwd filtering (the per-agent home is
  // already isolated to one agent, so the cwd filter only risks hiding the session).
  const codexArgs = resume
    ? ["codex", "resume", "--last", "--all",
       "--dangerously-bypass-approvals-and-sandbox", "--dangerously-bypass-hook-trust",
       "--cd", launchDir]
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

  printSystemMsg(`Launching codex agent ${agentName} in ${launchDir} (CODEX_HOME=${codexHome}${resume ? ", resuming" : ""})...`);

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
function launchGeminiAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume = false } = {}) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
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
function launchAgyAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume = false } = {}) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
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
 * Per-program launch dispatcher. `resume` carries optional resume handles:
 *   { codex: true }            → relaunch codex via `codex resume --last --all`
 *   { gemini: true }           → relaunch gemini via `gemini -r latest`
 *   { agy: true }              → relaunch agy via `agy --continue`
 *   { claudeSessionId: <uuid> } → relaunch claude via `claude --resume <uuid>`
 * Absent/empty → fresh launch. Keeps launchAgents() agnostic of program details.
 */
function launchAgent(agentName, program, allAgents, roomId, launchDir, transcriptDir, resume = {}) {
  if (program === "codex") {
    return launchCodexAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume: !!resume.codex });
  }
  if (program === "gemini") {
    return launchGeminiAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume: !!resume.gemini });
  }
  if (program === "agy") {
    return launchAgyAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resume: !!resume.agy });
  }
  // default + 'claude'
  return launchClaudeAgent(agentName, allAgents, roomId, launchDir, transcriptDir, { resumeSessionId: resume.claudeSessionId || null });
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

async function seedAgent(agentName, allAgents, roomId) {
  const roomBus = roomBusFor(roomId); // namespaced bus identity for this room
  const busId = agentBusId(roomId, agentName); // (A) this agent's namespaced bus identity
  const peers = allAgents.filter((a) => a !== agentName).join(" and ");
  const body =
    `Welcome to room "${roomId}". You are ${agentName} (bus id ${busId}). ` +
    `Your collaborators are: ${peers}, and ram (the human). ` +
    `This is a live conversation relayed over AgentBus. To speak to the room, run: ` +
    `${AB_PATH} send --from ${busId} --to ${roomBus} --thread-id ${roomId} --msg-type response "..."  ` +
    `Do NOT introduce yourself or send a greeting now — stay silent until a collaborator addresses you or poses a topic, then reply concisely.`;

  // H1: await async sendMessage. --from is the namespaced bus identity (NOT literal "room").
  // --to targets the agent's busId (the name it registered under), not the display name.
  const result = await sendMessage(roomBus, busId, body, roomId, "request");
  if (!result.ok) {
    printError(`Failed to seed ${agentName}: ${result.error}`);
  } else {
    printSystemMsg(`Seeded ${agentName}`);
  }
}

// ── Main Hub ──────────────────────────────────────────────────────────────────

class RoomHub {
  constructor(roomId, agentNames, cbMax = 6, programs = {}) {
    this.roomId = roomId;
    // BUS identity, namespaced per room ("room-<roomId>"). Used everywhere the hub
    // talks ON the bus (Register name, --from on relay/seed sends, --to reply target,
    // DB-tail relay-copy filter). The display label stays "room". This namespacing is
    // what prevents two concurrent hubs from evicting each other on the daemon.
    this.roomBus = roomBusFor(roomId);
    this.members = agentNames; // agent names only (not "ram") — stays a string[] (load-bearing)
    // programs: { <agentName>: "claude" | "codex" }. Names absent here default to 'claude'.
    this.programs = programs;
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
  }

  // ── FEATURE #1: State Persistence ──────────────────────────────────────────
  // Persist room state so a closed room can be reconnected with --resume. Written on
  // launch, /add, /kick, and shutdown. Best-effort: a write failure must never crash
  // the hub, so all fs is guarded.
  writeState() {
    try {
      const file = stateFilePathFor(this.roomId);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const state = serializeState(this.roomId, this.members, this.programs, this.resumeInfo);
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
    if (!this.working.has(name)) {
      this.working.add(name);
      printSystemMsg(`${C.DIM}${name} is working…${C.RESET}`);
      webRoster = this.members.slice();
      webBroadcast({ type: "presence", agent: name, state: "working" });
    }
  }
  clearWorking(name) {
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
      const result = await sendMessage(this.roomBus, agentBusId(this.roomId, to), body, this.roomId, "request");
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
      const escapedRoomId = this.roomId.replace(/'/g, "''");
      const sql = `SELECT COALESCE(MAX(rowid),0) FROM messages WHERE thread_id='${escapedRoomId}';`;
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
        const rows = await dbTailMessages(this.roomId, this.renderCursor);
        for (const row of rows) {
          renderMessage(row, this.roomId);
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
      appendLog(this.roomId, `[${time}] [ram] ${body}`);
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
          const result = await sendMessage(this.roomBus, agentBusId(this.roomId, to), prefixedBody, this.roomId, "request");
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
    const { name, program } = spec;
    if (this.members.includes(name)) {
      printSystemMsg(`${name} is already a member.`);
      return;
    }
    // Register bookkeeping BEFORE spawning so the new agent's prompt roster (built from
    // this.members) and the exit-handler pruning see a consistent member set.
    this.members.push(name);
    this.programs[name] = program;

    const doAdd = async () => {
      // Roster shown to the new agent = the full current member list (display names).
      const entry = this.spawnMember(name, program, this.launchDir, [...this.members]);
      const ready = await this.waitMemberReady(entry);
      if (!ready) {
        // waitMemberReady already printed why; the exit handler will have pruned members.
        return;
      }
      await seedAgent(name, this.members, this.roomId);
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
    const { child, spFile, transcriptFile, emptyMcp, codexHome, claudeSessionId } = launchAgent(
      agentName,
      program,
      rosterForPrompt,
      this.roomId,
      launchDir,
      os.tmpdir(),
      resume
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
    });
    // codexHome is undefined for claude agents — harmless in shutdown (guarded).
    const entry = { child, spFile, agentName, transcriptFile, emptyMcp, codexHome };
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

    for (const agentName of this.members) {
      const program = this.programs[agentName] || "claude";
      // FEATURE #1: on --resume, pass each agent its restore handle so launchAgent
      // relaunches the prior session (codex resume / claude --resume <uuid>).
      this.spawnMember(agentName, program, launchDir, allAgentNames, resumeHandles[agentName] || {});
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
    const sql = buildReplaySql(this.roomId, this.roomBus, limit);
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
    for (const row of rows) renderMessage(row, this.roomId);
    writeOut(`\n${C.DIM}──── resumed · history above ────${C.RESET}\n`);
  }

  // ── Seed Agents ───────────────────────────────────────────────────────────

  async seedAll() {
    for (const agentName of this.members) {
      await seedAgent(agentName, this.members, this.roomId);
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
  teardownChild({ child, agentName, spFile, emptyMcp, codexHome }, { unlinkEmptyMcp = true, removeCodexHome = false } = {}) {
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

    // 3. Print banner
    process.stdout.write(
      `\n${C.BOLD}${C.CYAN}══════════════════════════════════════════${C.RESET}\n` +
      `${C.BOLD}  AgentBus Room: ${this.roomId}${C.RESET}\n` +
      `${C.DIM}  Members: ${this.members.join(", ")}, ram${C.RESET}\n` +
      `${C.DIM}  Circuit-breaker: ${this.cbMax} consecutive agent msgs${C.RESET}\n` +
      `${C.DIM}  Log: room-${this.roomId}.log${C.RESET}\n` +
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
        for (const a of loaded.agents) {
          this.programs[a.name] = a.program;
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
      // FEATURE #1: on resume, SKIP seeding — each agent restores its own context from its
      // session; re-seeding would fight that (it'd re-introduce the "welcome" framing).
      if (!resuming) {
        await this.seedAll();
      }
    }
    // Persist initial state so a crash/close mid-session is still resumable.
    this.writeState();

    // 6. Start DB tail renderer
    this.startRender();

    // 7. Start human input
    this.startHumanInput();

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
    assert("codex toml: reasoning effort low", toml.includes('model_reasoning_effort = "low"'));
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
    roomId: null,
    // Default exercises both paths: claude-A (claude) + codex-A as a real codex agent.
    agents: ["claude-A", "codex-A"],
    programs: { "claude-A": "claude", "codex-A": "codex" },
    cbMax: 6,
    launchDir: process.cwd(),
    resume: false,
    web: null, // null = off; otherwise a port number
  };

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--self-test") {
      opts.selfTest = true;
    } else if (a === "--agents" && args[i + 1]) {
      // Specs are `name[:program]`, comma-separated. Bare name → 'claude' (backward compatible).
      const specs = args[++i].split(",").map((s) => s.trim()).filter(Boolean);
      const names = [];
      const programs = {};
      for (const spec of specs) {
        const r = parseAgentSpec(spec);
        if (r.error) {
          process.stderr.write(`Error: ${r.error}\n`);
          process.exit(1);
        }
        names.push(r.name);
        programs[r.name] = r.program;
      }
      opts.agents = names;
      opts.programs = programs;
    } else if (a === "--cb-max" && args[i + 1]) {
      opts.cbMax = parseInt(args[++i], 10) || 6;
    } else if (a === "--launch-dir" && args[i + 1]) {
      opts.launchDir = args[++i];
    } else if (a === "--no-agents") {
      opts.agents = [];
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

  if (!opts.roomId) {
    process.stderr.write(
      "Usage: agentbus-room.mjs <room-id> [--agents name[:program],...] [--cb-max 6] [--launch-dir <dir>] [--no-agents] [--resume]\n" +
      "       --agents accepts `name:program` (program = claude|codex|gemini|agy); a bare name defaults to claude.\n" +
      "       e.g. --agents claude-A,codex-A:codex  or  --agents claude-A,gem:gemini,a:agy\n" +
      "       --resume    reconnect to a closed room: replay history + restore each agent's session\n" +
      "       --no-agents attach to agents already running (hub-died-but-agents-survived reconnect)\n" +
      "       --web [port] also serve a chat-style Web UI (default port 8787, localhost only)\n" +
      "       agentbus-room.mjs --self-test\n"
    );
    process.exit(1);
  }

  const hub = new RoomHub(opts.roomId, opts.agents, opts.cbMax, opts.programs);
  hub.resume = opts.resume; // FEATURE #1: --resume restores history + agent sessions

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
