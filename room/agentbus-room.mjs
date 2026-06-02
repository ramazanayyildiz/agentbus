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
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
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
};

/** Deterministic color assignment for agent names. */
const AGENT_COLORS = [C.CYAN, C.YELLOW, C.MAGENTA, C.GREEN, C.BLUE];
const colorCache = new Map();
let colorIdx = 0;

function agentColor(name) {
  if (name === "ram") return C.GREEN;
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

// ── Agent Spec Parsing (per-program launch) ───────────────────────────────────

/** Programs the hub knows how to launch. */
const VALID_PROGRAMS = ["claude", "codex"];

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
 * Pre-trusts WORKDIR (no folder-trust modal) and omits [mcp_servers.*] to
 * avoid the heavy MCP boot stall. Pure function — testable.
 */
function buildCodexConfigToml(workDir) {
  return (
    `model = "gpt-5.5"\n` +
    `model_reasoning_effort = "low"\n` +
    `[projects."${workDir}"]\n` +
    `trust_level = "trusted"\n`
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

function renderMessage(msg, roomId) {
  const time = new Date(msg.created_at || Date.now()).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const author = msg.from_agent || msg.from || "?";
  const color = agentColor(author);
  const tag = `${C.DIM}${time}${C.RESET} ${color}${C.BOLD}[${author}]${C.RESET}`;
  const body = msg.body || "";

  // Print with blank line separator
  process.stdout.write(`\n${tag}\n${body}\n`);

  // Log to file
  appendLog(roomId, `[${time}] [${author}] ${body}`);
}

function printSystemMsg(text) {
  process.stdout.write(`\n${C.DIM}[room] ${text}${C.RESET}\n`);
}

function printError(text) {
  process.stdout.write(`\n${C.RED}[room:error] ${text}${C.RESET}\n`);
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
  const peers = allAgents.filter((a) => a !== selfName).join(", ");
  // {ROOM_BUS} = namespaced bus identity ("room-<roomId>"). Agents MUST reply to this,
  // not the literal "room", because the hub registers (and the daemon routes) under it.
  const roomBus = roomBusFor(roomId);
  return template
    .replace(/{SELF}/g, selfName)
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
 * Launch a CLAUDE agent with agentbus run. (Behavior unchanged from the original
 * single-program launcher — kept byte-for-byte.)
 * Returns { child, spFile, transcriptFile, emptyMcp }.
 *
 * H3: emptyMcp path is returned so shutdown can unlink it (was leaked previously).
 */
function launchClaudeAgent(agentName, allAgents, roomId, launchDir, transcriptDir) {
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
  const spFile = path.join(os.tmpdir(), `room-sp-${agentName}-${roomId}.txt`);
  fs.writeFileSync(spFile, prompt);

  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const emptyMcp = path.join(os.tmpdir(), `empty-mcp-${roomId}.json`);
  // Must be {"mcpServers":{}} — a bare {} fails `--strict-mcp-config` with
  // "Invalid MCP configuration: mcpServers: expected record, received undefined"
  // and the agent exits 1 at launch.
  if (!fs.existsSync(emptyMcp)) fs.writeFileSync(emptyMcp, '{"mcpServers":{}}');

  const args = [
    "run",
    "--name", agentName,
    "--program", "claude",
    "--transcript", transcriptFile,
    "--",
    "claude",
    "--dangerously-skip-permissions",
    "--strict-mcp-config",
    "--mcp-config", emptyMcp,
    "--append-system-prompt-file", spFile,
  ];

  printSystemMsg(`Launching claude agent ${agentName} in ${launchDir}...`);

  const child = spawnRunner(agentName, args, launchDir, null);

  // Note: the 'exit' + member-pruning handler is attached in launchAgents() where 'this' is bound (M3).
  return { child, spFile, transcriptFile, emptyMcp };
}

/**
 * Launch a CODEX agent with agentbus run.
 * Returns { child, transcriptFile, codexHome, workDir } (plus spFile/emptyMcp = null
 * so the children-entry shape stays uniform for shutdown).
 *
 * Isolation recipe (validated live):
 *  - Per-agent CODEX_HOME in tmpdir. Symlink auth-providing files from the real
 *    ~/.codex (auth.json, accounts, version.json, models_cache.json), each guarded.
 *  - config.toml pre-trusts a per-agent WORKDIR (no folder-trust modal) and omits
 *    [mcp_servers.*] (avoids the heavy MCP boot stall).
 *  - AGENTS.md in WORKDIR carries the SAME room system prompt Claude gets — the
 *    trust layer — just delivered as a file instead of --append-system-prompt-file.
 */
function launchCodexAgent(agentName, allAgents, roomId, launchDir, transcriptDir) {
  const transcriptFile = path.join(transcriptDir, `room-transcript-${agentName}-${roomId}.txt`);
  const codexHome = path.join(os.tmpdir(), `codex-home-${roomId}-${agentName}`);
  const workDir = path.join(os.tmpdir(), `codex-work-${roomId}-${agentName}`);

  // Stale dirs from a crashed prior run would make symlinkSync throw EEXIST — start clean.
  try { fs.rmSync(codexHome, { recursive: true, force: true }); } catch {}
  fs.mkdirSync(codexHome, { recursive: true });
  fs.mkdirSync(workDir, { recursive: true });

  // Symlink auth-providing files from the real ~/.codex into the isolated home.
  const realCodexHome = path.join(os.homedir(), ".codex");
  for (const entry of ["auth.json", "accounts", "version.json", "models_cache.json"]) {
    const src = path.join(realCodexHome, entry);
    const dst = path.join(codexHome, entry);
    try {
      if (fs.existsSync(src)) fs.symlinkSync(src, dst);
    } catch {
      // non-fatal: missing/unsymlinkable entry just means that capability is absent
    }
  }

  // config.toml: model + pre-trusted workdir, no MCP servers.
  fs.writeFileSync(path.join(codexHome, "config.toml"), buildCodexConfigToml(workDir));

  // AGENTS.md: reuse the EXACT same room system prompt Claude receives, so both
  // agents behave consistently (same roster/trust framing/--to room reply convention).
  const prompt = generateSystemPrompt(agentName, allAgents, roomId);
  fs.writeFileSync(path.join(workDir, "AGENTS.md"), prompt);

  const args = [
    "run",
    "--name", agentName,
    "--program", "codex",
    "--transcript", transcriptFile,
    "--",
    "codex",
    "--dangerously-bypass-approvals-and-sandbox",
    "--dangerously-bypass-hook-trust",
    "--cd", workDir,
  ];

  printSystemMsg(`Launching codex agent ${agentName} in ${workDir} (CODEX_HOME=${codexHome})...`);

  const child = spawnRunner(agentName, args, launchDir, { CODEX_HOME: codexHome });

  // spFile/emptyMcp are null for codex; codexHome/workDir drive its teardown instead.
  return { child, transcriptFile, spFile: null, emptyMcp: null, codexHome, workDir };
}

/**
 * Per-program launch dispatcher. Claude path is unchanged; codex path is new.
 * Keeps launchAgents() agnostic of program-specific details.
 */
function launchAgent(agentName, program, allAgents, roomId, launchDir, transcriptDir) {
  if (program === "codex") {
    return launchCodexAgent(agentName, allAgents, roomId, launchDir, transcriptDir);
  }
  // default + 'claude'
  return launchClaudeAgent(agentName, allAgents, roomId, launchDir, transcriptDir);
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
  const peers = allAgents.filter((a) => a !== agentName).join(" and ");
  const body =
    `Welcome to room "${roomId}". Your collaborators are: ${peers}, and ram (the human). ` +
    `This is a live conversation relayed over AgentBus. To speak to the room, run: ` +
    `${AB_PATH} send --from ${agentName} --to ${roomBus} --thread-id ${roomId} --msg-type response "..."  ` +
    `Do NOT introduce yourself or send a greeting now — stay silent until a collaborator addresses you or poses a topic, then reply concisely.`;

  // H1: await async sendMessage. --from is the namespaced bus identity (NOT literal "room").
  const result = await sendMessage(roomBus, agentName, body, roomId, "request");
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
    // (C) the sender just spoke — it's no longer "working".
    this.clearWorking(msg.from);
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
      printSystemMsg(`Flushed ${toFlush.length} buffered message(s) that were held by the circuit-breaker`);
    }
  }

  // ── (C) "is working…" indicator ─────────────────────────────────────────────
  // We mark an agent "working" the moment we relay a message TO it, and clear it
  // when a message FROM it arrives. Ephemeral, hub-side only (never a bus/DB row).
  markWorking(name) {
    if (!this.working.has(name)) {
      this.working.add(name);
      printSystemMsg(`${C.DIM}${name} is working…${C.RESET}`);
    }
  }
  clearWorking(name) { this.working.delete(name); }

  async relay(msg, targetOverride) {
    const fanout = computeFanout(msg, this.members, targetOverride);
    // H1: yield between successive relays so socket 'data' events can fire in between.
    for (const { to, body } of fanout) {
      await new Promise((r) => setImmediate(r)); // yield to event loop between relays
      // --from is the namespaced bus identity so relay copies carry from_agent=roomBus
      // (the DB-tail render filter excludes exactly that value to avoid duplicates).
      const result = await sendMessage(this.roomBus, to, body, this.roomId, "request");
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
    });

    this.rl.prompt();

    this.rl.on("line", (line) => {
      const text = line.trim();
      if (!text) {
        this.rl.prompt();
        return;
      }

      // Handle commands
      if (text.startsWith("/")) {
        this.handleCommand(text);
        this.rl.prompt();
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

      // Echo human message locally (not in DB tail since it's from=room)
      const time = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
      process.stdout.write(`\n${C.DIM}${time}${C.RESET} ${C.GREEN}${C.BOLD}[ram]${C.RESET}`);
      if (targetOverride) {
        process.stdout.write(`${C.DIM} → @${targetOverride}${C.RESET}`);
      }
      process.stdout.write(`\n${body}\n`);
      appendLog(this.roomId, `[${time}] [ram] ${body}`);

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
          const result = await sendMessage(this.roomBus, to, prefixedBody, this.roomId, "request");
          if (!result.ok) {
            printError(`Send to ${to} failed: ${result.error}`);
          } else {
            this.markWorking(to); // (C) we now expect a reply from this agent
          }
        }
      };
      doSend().catch((e) => printError(`Human send error: ${e.message}`));

      this.rl.prompt();
    });

    this.rl.on("close", () => {
      this.shutdown("stdin closed");
    });
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
      case "/help":
        printSystemMsg(
          "Commands: /quit /exit /resume /status /help\n" +
          "  @<agent> <msg>  — address a single agent\n" +
          "  /resume         — unblock circuit-breaker"
        );
        break;
      default:
        printSystemMsg(`Unknown command: ${cmd}`);
    }
  }

  // ── Launch Agents ─────────────────────────────────────────────────────────

  async launchAgents(launchDir) {
    const allAgentNames = [...this.members];

    for (const agentName of this.members) {
      const program = this.programs[agentName] || "claude";
      const { child, spFile, transcriptFile, emptyMcp, codexHome, workDir } = launchAgent(
        agentName,
        program,
        allAgentNames,
        this.roomId,
        launchDir,
        os.tmpdir()
      );
      // M3: attach exit handler here where 'this' is bound, so dead agents are pruned
      // from the fan-out member list immediately (prevents relaying to dead agents).
      child.on("exit", (code, signal) => {
        const how = code !== null ? `code ${code}` : `signal ${signal || "SIGKILL"}`;
        printSystemMsg(`Agent ${agentName} exited (${how}) — removed from fan-out`);
        this.members = this.members.filter((m) => m !== agentName);
      });
      // codexHome/workDir are undefined for claude agents — harmless in shutdown (guarded).
      this.children.push({ child, spFile, agentName, transcriptFile, emptyMcp, codexHome, workDir });
    }

    // Wait for all agents to register + become ready, detecting dead children.
    printSystemMsg("Waiting for agents to register on the bus...");
    for (const { agentName, transcriptFile, child } of this.children) {
      if (child.exitCode !== null) {
        printError(`${agentName} exited (code ${child.exitCode}) at launch — check its transcript: ${transcriptFile}. Not seeding it.`);
        continue;
      }
      const registered = await waitForAgentRegistered(agentName, 60000);
      if (!registered) {
        printSystemMsg(`WARNING: ${agentName} did not register within timeout`);
      }
      const ready = await waitForAgentReady(transcriptFile, agentName, 60000, child);
      if (!ready || child.exitCode !== null) {
        printError(`${agentName} exited (code ${child.exitCode}) before becoming ready — check transcript: ${transcriptFile}. Not seeding it.`);
        continue;
      }
      printSystemMsg(`${agentName} ready`);
    }
    if (this.members.length === 0) {
      printError(`No agents are alive — the room has no participants. Check agent transcripts in ${os.tmpdir()}.`);
    }
  }

  // ── Seed Agents ───────────────────────────────────────────────────────────

  async seedAll() {
    for (const agentName of this.members) {
      await seedAgent(agentName, this.members, this.roomId);
    }
  }

  // ── Teardown ──────────────────────────────────────────────────────────────

  async shutdown(reason = "shutdown") {
    if (!this.running) return;
    this.running = false;
    printSystemMsg(`Shutting down: ${reason}`);

    this.stopRender();

    // H2: kill each agent's entire process group (SIGTERM → 3s wait → SIGKILL) so the
    // inner program (claude/codex) doesn't survive detached.
    // H3: unlink both spFile and emptyMcp temp files (claude); rm codexHome/workDir (codex).
    const killPromises = this.children.map(({ child, agentName, spFile, emptyMcp, codexHome, workDir }) => {
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
        try { if (emptyMcp && fs.existsSync(emptyMcp)) fs.unlinkSync(emptyMcp); } catch {}
        // Codex teardown: remove the per-agent isolated CODEX_HOME and WORKDIR.
        try { if (codexHome && fs.existsSync(codexHome)) fs.rmSync(codexHome, { recursive: true, force: true }); } catch {}
        try { if (workDir && fs.existsSync(workDir)) fs.rmSync(workDir, { recursive: true, force: true }); } catch {}
      });
    });

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

    // 4. Launch agents (if any configured)
    if (this.members.length > 0) {
      await this.launchAgents(launchDir);
      // 5. Seed agents (H1: await async sendMessage inside seedAll)
      await this.seedAll();
    }

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

    // unknown program rejected
    const bad = parseAgentSpec("x:gemini");
    assert("spec x:gemini: rejected with error", typeof bad.error === "string" && bad.error.includes("gemini"));
    assert("spec x:gemini: no name returned", bad.name === undefined);

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
    const wd = "/tmp/codex-work-r1-codex-A";
    const toml = buildCodexConfigToml(wd);
    assert("codex toml: model gpt-5.5", toml.includes('model = "gpt-5.5"'));
    assert("codex toml: reasoning effort low", toml.includes('model_reasoning_effort = "low"'));
    assert("codex toml: projects key references workdir", toml.includes(`[projects."${wd}"]`));
    assert("codex toml: trust_level trusted", toml.includes('trust_level = "trusted"'));
    // The MCP boot stall is avoided by NOT emitting any [mcp_servers.*] section.
    assert("codex toml: no mcp_servers section", !toml.includes("mcp_servers"));
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
      "Usage: agentbus-room.mjs <room-id> [--agents name[:program],...] [--cb-max 6] [--launch-dir <dir>] [--no-agents]\n" +
      "       --agents accepts `name:program` (program = claude|codex); a bare name defaults to claude.\n" +
      "       e.g. --agents claude-A,codex-A:codex\n" +
      "       agentbus-room.mjs --self-test\n"
    );
    process.exit(1);
  }

  const hub = new RoomHub(opts.roomId, opts.agents, opts.cbMax, opts.programs);

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
