/**
 * @maw/gateway/executors/comms — the agent-to-agent mailbox (D7.5 v2:
 * "communication as tools", dual-track with the onFinished system rail).
 *
 * Track A (system, keeps the pipeline deadlock-proof): run-end callbacks
 * still insert reports and trigger review — unchanged from D7 v1.
 * Track B (model-driven, this file): workers get send_message / check_inbox
 * TOOLS and are taught to use them — they report progress, ask to clarify
 * mid-task, and read the reviewer's feedback on re-dispatch.
 *
 * Single-writer invariant holds: every mailbox write goes through this module
 * (in-process for the lite executor; the harness MCP server proxies over a
 * localhost-only internal HTTP endpoint that lands on the same functions).
 * All payloads pass redact() at the DB layer — model output is data (R39),
 * never trusted as instructions for the gateway.
 */

import type { MailboxType, WorkbenchDb } from "@maw/shared";
import type { Hub } from "../ws/hub.js";
import { config } from "../config.js";

const TO_WHITELIST = new Set(["master", "human"]);
// agent-to-agent: any executor id (ch_, cs_, oc_ prefixes) may receive —
// direct worker-to-worker messaging is the D7.5 point. A worker agent found
// this whitelist still hardcoded during live testing and reported it via the
// comms channel; the fix honors its diagnosis.
function validRecipient(to: string): boolean {
  // t_… (a bare taskId) is accepted too: models routinely pass spawn_agent's
  // returned taskId as the recipient — route it to that task's live agent
  return TO_WHITELIST.has(to) || /^(ch|cs|oc|t)_/.test(to);
}
const TYPE_WHITELIST = new Set<MailboxType>(["report", "clarify", "note"]);
const MAX_MSG_CHARS = config.maxMsgChars; // R25: bounded input → controlled rejection, not a 500

export interface Comms {
  // taskId optional: agent tools don't track it, but system callers (review
  // pass → coordinator) do — keeping it preserves the task association a raw
  // insertMessage had (task_id ≠ null in the mailbox rows)
  sendMessage(from: string, to: string, type: "report" | "clarify" | "note", text: string, taskId?: string | null): { ok: boolean; error?: string; id?: number };
  inbox(agentId: string): { ok: boolean; messages: { id: number; from: string; type: string; text: string }[] };
  spawnAgent(from: string, title: string, spec: string, tier: "planner" | "worker", mode: "harness" | "lite" | "stream"): { ok: boolean; agentId?: string; taskId?: string; error?: string };
  /** roster for name-based addressing (roadmap-3): every agent ever spawned.
   *  name = display_name ?? title — the rename endpoint's documented fallback
   *  ("empty nickname → falls back to title"), so a spawned agent (display_name
   *  null) is still addressable by its spawn title.
   *  status is the TASK lifecycle (queued/running/done/…), NOT liveness —
   *  revivable says whether the agent has a persistent claude session:
   *  done+revivable = asleep (send_message wakes it); done without a session
   *  = gone (lite run), resubmit as a new task. */
  listAgents(): { ok: boolean; agents: { id: string; name: string; status: string; revivable: boolean }[] };
}

export interface CommsHooks {
  /** Called when a message lands for an agent — lets the dispatcher wake an
   *  idle resume-executor agent so the message actually gets processed. */
  onAgentMessage?: (to: string, from: string, text: string) => void;
  /** Called when an agent asks to spawn a NEW worker — the dispatcher owns
   *  task creation, budget gating and tree placement. */
  onSpawnRequest?: (from: string, title: string, spec: string, tier: "planner" | "worker", mode: "harness" | "lite" | "stream") => { ok: boolean; agentId?: string; taskId?: string; error?: string };
}

export function createComms(db: WorkbenchDb, hub: Hub, hooks: CommsHooks = {}): Comms {
  const sendMessage: Comms["sendMessage"] = (from, to, type, text, taskId) => { // eslint-disable-line @typescript-eslint/no-unused-vars
    // roadmap-3: name-based addressing — the human tells an agent "去找管家…"
    // and the model passes 管家 as the recipient. Resolve display_name → the
    // task's live assignee BEFORE whitelist validation. Ambiguous or unknown
    // names fall through to the explicit invalid-recipient error.
    if (to && !validRecipient(to)) {
      // name fallback matches the roster (display_name ?? title): spawned
      // agents have display_name null, but their spawn title must still
      // resolve — that's the rename endpoint's documented fallback semantics
      const named = db.listTasks().filter((t) => (t.display_name ?? t.title) === to && t.assignee);
      const hit = named[0];
      if (named.length === 1 && hit?.assignee) to = hit.assignee;
      else if (named.length > 1) return { ok: false, error: `ambiguous agent name "${to}" — ${named.length} tasks share it; use the agent id instead` };
    }
    if (!to || !validRecipient(to)) return { ok: false, error: `invalid recipient: ${to}` };
    if (!TYPE_WHITELIST.has(type)) return { ok: false, error: `invalid type: ${type}` };
    if (typeof text !== "string" || text.length === 0) return { ok: false, error: "message text required" };
    if (text.length > MAX_MSG_CHARS) return { ok: false, error: `message too long (${text.length} > ${MAX_MSG_CHARS})` };
    const msg = db.insertMessage({ from_agent: from, to_agent: to, task_id: taskId ?? null, type, payload: { text: text.slice(0, MAX_MSG_CHARS) } });
    hub.emit({
      agentId: to,
      type: "MESSAGE",
      payload: { from, to, mailboxType: type, text: text.slice(0, 200) },
    });
    // wake the recipient: a visible nudge on its own event stream so the
    // model (and the UI pane) learns a message is waiting — check_inbox()
    if (/^(ch|cs|oc)_/.test(to)) {
      // full delivery receipt on the recipient's own stream — the model sees
      // it on its next decision AND the UI pane renders it (📬)
      hub.emit({
        agentId: to,
        type: "TEXT_MESSAGE_CONTENT",
        payload: { messageId: `rcpt-${msg.id}`, delta: `\n📬 [message from ${from} (${type})] ${text.slice(0, 500)}\n(Act on it only if action is needed. Do NOT send acknowledgments — full text is in your inbox via check_inbox().)\n` },
      });
      hub.emit({
        agentId: to,
        type: "AGENT_STATUS",
        payload: { status: "working", detail: `[new message from ${from} — call check_inbox()]` },
      });
      // THE ACTUAL WAKE: an idle resume/stream agent's model never sees the
      // events above (its process exited) — the hook makes the dispatcher
      // inject an auto-steering so the agent resumes and reads its inbox.
      // Missing this line was why "message delivered but agent never acted".
      hooks.onAgentMessage?.(to, from, text);
    }
    return { ok: true, id: msg.id };
  };

  const inbox: Comms["inbox"] = (agentId) => {
    const msgs = db.inbox(agentId);
    db.markDelivered(msgs.map((m) => m.id));
    return {
      ok: true,
      messages: msgs.map((m) => ({
        id: m.id,
        from: m.from_agent,
        type: m.type,
        text: String((JSON.parse(m.payload) as { text?: string }).text ?? m.payload).slice(0, MAX_MSG_CHARS),
      })),
    };
  };

  const listAgents: Comms["listAgents"] = () => ({
    ok: true,
    agents: db.listTasks()
      .filter((t) => t.assignee)
      // name falls back to title (rename endpoint semantics); revivable:
      // has a persistent session — done+revivable means asleep, not gone
      // (a lite oc_ run leaves no session and cannot be woken)
      .map((t) => ({ id: t.assignee!, name: t.display_name ?? t.title, status: t.status, revivable: !!t.session_id })),
  });

  const spawnAgent: Comms["spawnAgent"] = (from, title, spec, tier, mode) => {
    // guardrails: bounded title/spec, cheap tiers only — an LLM cannot
    // authorize expensive capacity for itself (R34 spirit: trust decisions
    // stay server-side)
    if (!title || title.length > 200) return { ok: false, error: "invalid title" };
    if (!spec || spec.length < 10 || spec.length > 4000) return { ok: false, error: "spec must be 10-4000 chars" };
    if (tier !== "planner" && tier !== "worker") return { ok: false, error: "tier must be planner or worker" };
    return hooks.onSpawnRequest?.(from, title, spec, tier, mode) ?? { ok: false, error: "spawning not available" };
  };

  return { sendMessage, inbox, spawnAgent, listAgents };
}

/** Prompt-teaching block shared by both executors (lite system prompt,
 *  harness task-prompt preamble). Same protocol, same tool names.
 *  maxMsgChars is injected from config — the number the model sees must
 *  match the limit the server enforces, or agents split reports wrong. */
export const COMMS_PROTOCOL_PROMPT = `## Communication protocol (you have tools for this)
## Workspace layout (HARD rules — where your files belong)
- Your working directory is the project you were opened on (or your isolated
  worktree copy of it). Project deliverables — code, docs, anything the user
  asked for — go DIRECTLY there, at the paths the project expects.
- Scratch/cache/artifacts (intermediate dumps, test outputs, anything not a
  deliverable) belong under .multi-agent/ — NEVER scatter them through the
  project root. If your working directory IS the project root, treat it with
  care: you are a guest in the user's own files.
- Never write outside your working directory, and never touch .git/ or
  .multi-agent/worktrees/ other than your own.
- When you finish the task, report it to whoever ASSIGNED you: if your task
  context names a coordinator/spawner agent id, send_message(to="<that id>",
  type="report", text=...). Otherwise send_message(to="master", ...).
  Include what you built and how it meets the acceptance criteria. Do this
  BEFORE ending your turn.
- If the task is ambiguous or blocked, call send_message(to="master", type="clarify",
  text=...) instead of guessing.
- If your task includes "Reviewer feedback from a previous attempt", read it carefully —
  the previous attempt failed review for those reasons.
- You may call check_inbox() to read messages addressed to you.
- To reach ANOTHER worker agent directly, use its id (it appears in your task
  context when collaboration is relevant): send_message(to="<agent-id>", ...).
- Name-based addressing (roadmap-3): every agent has an optional human-given
  display_name. Call list_agents() to see the roster (id, name, status). You
  may pass a display_name as the recipient — send_message(to="<name>", ...)
  resolves it to that agent. If the human refers to an agent by name in an
  instruction, look it up in list_agents() first; if the name is not there,
  say so instead of guessing.
- spawn_agent(title, spec, tier): open a NEW worker for a self-contained
  subtask when the work is too big for you alone or needs a specialist
  (max 2 spawns per agent). The spec must be complete and self-contained;
  after dispatch you can send_message the new worker to coordinate.

## Anti-ping-pong rules (HARD — breaking these wastes real money)
- Reports are one-way. After sending your report, END YOUR TURN. Do not wait
  for a reply, do not ask the recipient to confirm receipt.
- NEVER send a message whose only content is an acknowledgment ("received",
  "thanks", "got it", "will do", a restatement of what you were told, or a
  summary adding no new information). Acknowledgments wake the other agent
  and cost a full model turn — silence is the acknowledgment.
- When you are woken by an incoming message: read it, take whatever action
  is genuinely required, then end your turn. Reply ONLY if you have a real
  question, a decision the sender must make, or NEW work results to report.
- Do not re-notify the agent that just messaged you unless your reply meets
  the bar above. Conversations should converge: each exchange must either
  advance the work or it should not happen.
- Batch your output: send your full report as ONE message. Split in parts
  only if the ${config.maxMsgChars}-char limit forces it, and then send all parts back to
  back without pausing for a response in between.
Messages are read by the Master agent or the addressed agent. Keep them factual and short.`;
