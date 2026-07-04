#!/usr/bin/env node
// stop-forward.mjs — Claude Code Stop-hook forwarder for AgentBus rooms.
//
// Purpose: a RESUMED Claude session in a room tends to write its reply as terminal
// prose and forget to run `agentbus send`, so its replies never reach the room.
// This hook fires every time the agent finishes a turn, extracts its last assistant
// text message from the session transcript, and forwards it to the room bus — so the
// reply lands regardless of whether the model remembered to send it itself.
//
// SCOPE: this script is referenced ONLY from the per-agent --settings file the room
// writes for its launched claude agents. It never touches the user's global Claude
// Code config, so normal `claude` usage is unaffected.
//
// Config comes from env (set by the room on the agentbus-run process, inherited by
// claude and thus by this hook):
//   AGENTBUS_BIN      path to the agentbus binary
//   AGENTBUS_BUSID    this agent's namespaced bus id (e.g. delta3-claude-delta)
//   AGENTBUS_ROOMBUS  the room's bus identity (e.g. room-delta3)
//   AGENTBUS_ROOMID   the room/thread id (e.g. delta3)
//
// Stop-hook stdin JSON: { session_id, transcript_path, stop_hook_active, cwd, ... }

import fs from "node:fs";
import { execFileSync } from "node:child_process";

function readStdin() {
  try { return fs.readFileSync(0, "utf8"); } catch { return ""; }
}

let input = {};
try { input = JSON.parse(readStdin() || "{}"); } catch { process.exit(0); }

// Loop guard: when a Stop hook itself continues the agent, Claude sets this true on
// the re-entry. Never forward in that case.
if (input.stop_hook_active) process.exit(0);

const AB = process.env.AGENTBUS_BIN;
const BUSID = process.env.AGENTBUS_BUSID;
const ROOMBUS = process.env.AGENTBUS_ROOMBUS;
const ROOMID = process.env.AGENTBUS_ROOMID;
const tp = input.transcript_path;
if (!AB || !BUSID || !ROOMBUS || !ROOMID || !tp) process.exit(0);

// Extract the LAST assistant text message from the transcript JSONL.
let text = "";
try {
  const lines = fs.readFileSync(tp, "utf8").split("\n").filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i--) {
    let o; try { o = JSON.parse(lines[i]); } catch { continue; }
    if (o.type !== "assistant" || !o.message) continue;
    const c = o.message.content;
    if (Array.isArray(c)) {
      text = c.filter((x) => x && x.type === "text").map((x) => x.text).join("\n").trim();
    } else if (typeof c === "string") {
      text = c.trim();
    }
    if (text) break;
  }
} catch { process.exit(0); }
if (!text) process.exit(0);

// Dedup: if the agent already sent this exact text as its most recent response
// (it ran `agentbus send` itself), don't double-post.
try {
  const db = `${process.env.HOME}/.agentbus/bus.db`;
  const esc = BUSID.replace(/'/g, "''");
  const sql = `SELECT body FROM messages WHERE from_agent='${esc}' AND msg_type='response' ORDER BY created_at DESC LIMIT 1;`;
  const last = execFileSync("sqlite3", [db, sql], { encoding: "utf8" }).trim();
  if (last && last === text) process.exit(0);
} catch { /* no db / sqlite missing — fall through and send */ }

// Forward to the room. argv form (no shell) so the body can't be interpolated.
try {
  execFileSync(AB, ["send", "--from", BUSID, "--to", ROOMBUS, "--thread-id", ROOMID, "--msg-type", "response", text], { timeout: 10000 });
} catch { /* best-effort */ }
process.exit(0);
