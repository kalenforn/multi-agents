/**
 * @maw/gateway — process entry: assembles DB + Hub + ExecutorManager behind
 * one HTTP/WS listener (architecture invariant 1: single process).
 *
 * D2 scope: SUBMIT_GOAL records the root task and — under MAW_FAKE_DECOMPOSE=1 —
 * dispatches it to the fake executor, so the wire acceptance can run without
 * D3–D6. Control messages route to live handles via the manager; unknown
 * agents get an explicit RUN_ERROR, never silence.
 *
 * Env: PORT (8787) · MAW_DB_PATH (default data/workbench.db) ·
 *      MAW_FAKE_DECOMPOSE=1 (dev-only, delete after D6)
 */

import http from "node:http";
import { execFile, execFileSync } from "node:child_process";
import { mkdirSync, existsSync, readFileSync, writeFileSync, copyFileSync, statSync, readdirSync } from "node:fs";
import path from "node:path";
import { WebSocketServer, type WebSocket } from "ws";
import { isClientMessage, newTaskId, openDb, type WorkbenchDb } from "@maw/shared";
import { loadDotEnv } from "./env.js";
import { config, setCurrentProjectPointer } from "./config.js";
import { listProjects, registerProject } from "./projects.js";
import { ClaudeHarnessAdapter } from "./executors/claude-harness.js";
import { ClaudeStreamAdapter } from "./executors/claude-stream.js";
import { FakeExecutorAdapter } from "./executors/fake.js";
import { ExecutorManager } from "./executors/manager.js";
import { OpenAICompatAdapter } from "./executors/openai-compat.js";
import { createWorktree, diffWorktree, mergeWorktree, removeWorktree } from "./executors/worktree.js";
import { Hub, type HubClient } from "./ws/hub.js";
import { decomposeGoal, plantTree, projectContext } from "./orchestrator/master.js";
import { resolveExecutorForTask, applyBudget, type TaskProfile } from "./orchestrator/router.js";
import { enforceBudget } from "./orchestrator/ledger.js";
import { MAX_REDISPATCH, reviewWork } from "./orchestrator/review.js";
import { createComms } from "./executors/comms.js";
import { notifyCoordinatorPassed } from "./orchestrator/coordinator-notify.js";

loadDotEnv();

const PORT = config.port;
const DB_PATH = config.dbPath;
const FAKE_DECOMPOSE = config.fakeDecompose;
const DEV_EXECUTOR_RAW = config.devExecutor;
/** Executor names accepted per-request (and as the MAW_DEV_EXECUTOR default). */
const EXECUTOR_KINDS = new Set(["planner", "worker", "claude-planner", "claude-worker", "stream-planner", "stream-worker"]);
type DevExecutor = "planner" | "worker" | "claude-planner" | "claude-worker" | "stream-planner" | "stream-worker" | null;
const DEV_EXECUTOR: DevExecutor =
  DEV_EXECUTOR_RAW && EXECUTOR_KINDS.has(DEV_EXECUTOR_RAW)
    ? (DEV_EXECUTOR_RAW as Exclude<DevExecutor, null>)
    : null;

mkdirSync(path.dirname(DB_PATH), { recursive: true });

/** Prepare a project's .multi-agent/ workspace: mkdir, gitignore append, and
 *  (gateway-tree era only) one-time legacy DB migration. Runs on boot AND on
 *  every Open-Project hot swap — a project first opened mid-session deserves
 *  the same conveniences as one opened at startup. */
function bootstrapWorkspace(): void {
  try {
    const dir = config.workspaceDir;
    mkdirSync(dir, { recursive: true });
    const giPath = path.join(config.projectDir, ".gitignore");
    const gi = existsSync(giPath) ? readFileSync(giPath, "utf8") : "";
    if (!gi.split("\n").some((l) => l.trim() === ".multi-agent/" || l.trim() === ".multi-agent")) {
      writeFileSync(giPath, (gi.length && !gi.endsWith("\n") ? gi + "\n" : gi) + ".multi-agent/\n");
      console.log(`[gateway] appended .multi-agent/ to ${giPath}`);
    }
    const legacyDb = path.resolve(import.meta.dirname ?? ".", "data", "workbench.db");
    if (!existsSync(config.dbPath) && existsSync(legacyDb)) {
      copyFileSync(legacyDb, config.dbPath);
      console.log(`[gateway] migrated legacy DB ${legacyDb} → ${config.dbPath}`);
    }
  } catch (e) {
    console.warn(`[gateway] workspace bootstrap (gitignore/migrate) skipped:`, e instanceof Error ? e.message : e);
  }
}
bootstrapWorkspace();
registerProject(config.projectDir); // the boot project belongs on the sidebar too
const db: WorkbenchDb = openDb(DB_PATH);
const hub = new Hub(db);
const manager = new ExecutorManager(hub);
const SPAWN_CAP = config.spawnCap; // per-agent spawn limit

const comms = createComms(db, hub, {
  onSpawnRequest: (from, title, spec, tier, mode) => {
    // the spawning agent's own task roots the new subtask (tree stays
    // traceable in the UI: who opened whom)
    const spawnerTask = db.listTasks().find((t) => t.assignee === from);
    const rootId = spawnerTask?.parent_id ?? spawnerTask?.id ?? null;
    // reproduction cap by spawned-task messages
    const already = db.messagesFrom(from).filter((m) => m.payload.includes("spawned worker task")).length;
    if (already >= SPAWN_CAP) return { ok: false, error: `spawn cap reached (${SPAWN_CAP})` };
    const spawnId = newTaskId();
    // inject the spawner's identity into the spec — the comms protocol says
    // "report to the coordinator named in your task context", but the raw
    // spec never named one, so every spawned worker defaulted to master and
    // the coordinator never heard back directly (live p2p gap)
    const specWithCoordinator = `${spec}\n\n# Coordinator\n你由 agent ${from} 派出。完成后用 send_message(to="${from}", type="report") 直接向它汇报结论,不要发给 master。`;
    db.insertTask({ id: spawnId, parent_id: rootId, title, spec: specWithCoordinator, status: "queued", model_hint: tier });
    db.insertMessage({ from_agent: from, to_agent: "master", task_id: spawnId, type: "note", payload: { text: `spawned worker task ${spawnId} (${title})` } });
    hub.emit({ agentId: "master", taskId: spawnId, type: "TASK_TREE_UPDATED", payload: { reason: "decomposed", taskId: spawnId, planted: 1, spawnedBy: from } });
    console.log(`[comms] agent ${from} spawned worker task ${spawnId} (tier=${tier}, mode=${mode})`);
    void dispatchReady(rootId ?? spawnId).catch(() => undefined);
    // return the new agent's id too — the spawner addresses it via send_message
    const hint = mode === "stream" ? "cs_" : mode === "harness" ? "ch_" : "oc_";
    return { ok: true, taskId: spawnId, agentId: `${hint}${spawnId}` };
  },
  onAgentMessage: (to, from, text) => {
    // resolve bare taskIds (models pass spawn_agent's taskId as recipient)
    // to that task's CURRENT assignee agent id
    let recipient = to;
    if (/^t_/.test(to)) {
      try {
        const t = db.getTask(to);
        recipient = t.assignee ?? to;
      } catch { /* unknown task — fall through, will fail validation */ }
    }
    // wake an idle agent to process its mail — its model never saw the 📬
    // event (process exited); this steering resumes the session.
    // Anti-ping-pong: "act on it" must NOT offer a reply option — offering
    // one made every handshake echo back and forth, each echo a full paid turn.
    const handled = manager.inject(
      recipient,
      `[auto-steering] You have a new message from ${from} in your inbox. Call check_inbox() NOW and read it. Then: if it requires work, do the work; if it is a report or acknowledgment, you have received it and there is nothing to do — end your turn WITHOUT sending any reply. Send a message back ONLY if you have a genuine question, a decision the sender must make, or new work results.`
    );
    console.log(`[comms] auto-woke ${recipient}: handled=${handled} for message from ${from}`);
    if (!handled) {
      // the recipient is gone (lite executors die at run end; harness agents
      // past their idle window). Rather than a dead letter, wake a successor
      // that inherits the conversation context and the task.
      // Agent ids encode their task (ch_/cs_/oc_ + taskId [+ _w suffix]) —
      // resolve by TASK, not by assignee: a previous rescue already moved the
      // assignee, so matching on assignee would drop every later message.
      const taskId = to.replace(/^(ch|cs|oc)_/, "").replace(/_w\d*$/, "");
      let task: ReturnType<WorkbenchDb["getTask"]> | undefined;
      try { task = db.getTask(taskId); } catch (e) { console.log(`[comms] rescue lookup failed for ${to} → ${taskId}:`, e instanceof Error ? e.message : e); task = undefined; }
      if (task) {
        if (task.status === "cancelled") {
          console.log(`[comms] message for cancelled task ${task.id} left in inbox (audit trail)`);
          return;
        }
        // a DONE task is not a closed conversation: peer messages arriving
        // after completion are NEW work on the same worktree — spawn a
        // successor (never drop). This is what makes multi-turn agent-to-
        // agent talk survive executor death between turns.
        const spec = `${task.spec}\n\n# New message from peer ${from}\n${text}\n\nProcess the message above now (reply via send_message).`;
        const worktreeDir = task.worktree_path ?? undefined;
        // rescue runs spend tokens too — record usage (one cost_ledger row per
        // run, same as /api/agents/revive), then close the task
        const onRunComplete = (_o: unknown, handle: unknown) => {
          const h = handle as { id: string; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } };
          const usage = h.finalUsage;
          if (usage) {
            db.insertCost({
              agent_id: h.id,
              task_id: task.id,
              model: usage.model,
              input_tokens: usage.input,
              output_tokens: usage.output,
              cache_read: usage.cacheRead ?? 0,
              cache_write: usage.cacheWrite ?? 0,
              est_cost_usd: 0,
            });
          }
          db.setStatus(task.id, "done");
        };
        if (task.session_id) {
          // roadmap-1a semantics (same as /api/agents/revive): SAME agent id —
          // swapping in a _w-suffix successor breaks the UI panes/subscriptions
          // keyed on the old id (the _r-suffix scheme broke exactly there).
          // Resume the persisted claude session (the "inherits the context"
          // this path always promised) and persist the NEW session id so the
          // NEXT rescue can resume too — without onSessionId the id was never
          // written back and every later rescue started cold.
          const prev = task.assignee ?? to;
          const agentId = /^(ch|cs)_/.test(prev) ? prev : `ch_${task.id}`;
          const adapter = agentId.startsWith("cs_")
            ? new ClaudeStreamAdapter(tierOf(task.model_hint))
            : new ClaudeHarnessAdapter(tierOf(task.model_hint));
          manager.spawn(adapter, { title: task.title, spec }, {
            taskId: task.id,
            agentId, // SAME id — in-place wake, no suffix swap
            worktreeDir,
            resumeSessionId: task.session_id,
            onSessionId: (sid) => { try { db.setSessionId(task.id, sid); } catch { /* task row gone — non-fatal */ } },
            onRunComplete,
          });
          db.setStatus(task.id, "running", { assignee: agentId });
          hub.emit({ agentId: "master", taskId: task.id, type: "TASK_TREE_UPDATED", payload: { reason: "dispatch", taskId: task.id, agentId } });
          console.log(`[comms] dead-letter rescue: revived ${agentId} for ${task.id} (session resumed)`);
        } else {
          // no persistent session (oc_ lite or a pre-roadmap-1a task): a fresh
          // harness agent under a NEW _w-suffixed id. The suffix must be
          // monotonically increasing — the old endsWith ladder generated _w2
          // for an assignee already on _w3 and could collide with a live _w2.
          const m = /_w(\d+)$/.exec(task.assignee ?? "");
          const successor = `ch_${task.id}${m ? `_w${Number(m[1]) + 1}` : "_w"}`;
          const adapter = new ClaudeHarnessAdapter(tierOf(task.model_hint));
          manager.spawn(adapter, { title: task.title, spec }, {
            taskId: task.id,
            agentId: successor,
            worktreeDir,
            // persist the fresh session id so a LATER rescue can resume it
            // instead of cold-starting again (parity with the resumed branch)
            onSessionId: (sid) => { try { db.setSessionId(task.id, sid); } catch { /* task row gone — non-fatal */ } },
            onRunComplete,
          });
          db.setStatus(task.id, "running", { assignee: successor });
          hub.emit({ agentId: "master", taskId: task.id, type: "TASK_TREE_UPDATED", payload: { reason: "dispatch", taskId: task.id, agentId: successor } });
          console.log(`[comms] dead-letter rescue: spun ${successor} for ${task.id} (no session — cold start)`);
        }
      }
    }
  },
});

/** Model hint → executor tier for the rescue path. */
function tierOf(hint: string | null): "planner" | "worker" {
  return hint === "worker" ? "worker" : "planner";
}
const ORCHESTRATE = config.orchestrate;
const fake = new FakeExecutorAdapter();
const MAX_PARALLEL_WORKERS = config.maxParallelWorkers; // 3 harness workers + test harness ≈ memory ceiling on a 16GB machine
let fakeCounter = 0;
const EXEC_TIMEOUT_MS = config.execTimeoutMs; // execution watchdog
console.log(`[gateway] db ready at ${DB_PATH} (lastSeq=${db.lastSeq()})`);

// ── D12 crash recovery ─────────────────────────────────────────────────────
// On boot, tasks stuck in 'running' have no live executor (the previous
// process died with them). Requeue them — the worktree survives (files on
// disk), dispatchReady re-dispatches with the same task id so the recovery
// is a replay, not a repair. Blocked deps stay correct because depsDone
// reads the DB, not memory.
{
  const orphans = db.listTasks().filter((t) => t.status === "running");
  for (const t of orphans) {
    db.setStatus(t.id, "queued", { assignee: null });
    db.insertMessage({ from_agent: "master", to_agent: "human", task_id: t.id, type: "note", payload: { text: `crash recovery: requeued ${t.title}` } });
    console.log(`[recovery] requeued orphan task ${t.id} (assignee was ${t.assignee})`);
  }
  if (orphans.length > 0) {
    const roots = [...new Set(orphans.map((t) => t.parent_id).filter((x): x is string => !!x))];
    for (const r of roots) void dispatchReady(r);
  }
}

function resolveExecutor(requested?: string): string | null {
  const name = requested ?? DEV_EXECUTOR;
  return name && EXECUTOR_KINDS.has(name) ? name : null;
}

async function submitGoal(goal: string, requestedExecutor?: string): Promise<{ taskId: string; agentId?: string }> {
  const task = db.insertTask({ id: newTaskId(), title: goal.slice(0, 120), spec: goal });
  hub.emit({ agentId: "master", taskId: task.id, type: "TASK_TREE_UPDATED", payload: { reason: "human", taskId: task.id } });

  // D6: orchestrated mode — Master decomposes, Router assigns, workers run.
  // An explicit executor= still runs the single-task dev path.
  if (ORCHESTRATE && !requestedExecutor) {
    void orchestrate(goal, task.id);
    return { taskId: task.id };
  }

  const DEV_EXECUTOR_NOW = resolveExecutor(requestedExecutor);
  if (DEV_EXECUTOR_NOW) {
    // dev path: dispatch the whole goal to one real worker executor in a
    // worktree. The D6 Master loop (decomposition) replaces this for
    // executor-less submissions.
    const isStream = DEV_EXECUTOR_NOW.startsWith("stream-");
    const isHarness = DEV_EXECUTOR_NOW.startsWith("claude-");
    const tier = (isStream ? DEV_EXECUTOR_NOW.slice("stream-".length) : isHarness ? DEV_EXECUTOR_NOW.slice("claude-".length) : DEV_EXECUTOR_NOW) as "planner" | "worker";
    const preassigned = `${isStream ? "cs" : isHarness ? "ch" : "oc"}_${task.id}`;
    let worktreeDir: string | undefined;
    try {
      worktreeDir = (await createWorktree(task.id)).dir;
    } catch (err) {
      // no git repo / worktree failure → SANDBOX inside the project, never
      // undefined: undefined let the executor default its cwd to the GATEWAY's
      // own checkout and agents wrote into the wrong repo (the snake-game bug).
      // No branch isolation — review/merge impossible — but files land in the
      // opened project, bounded to .multi-agent/sandboxes/.
      worktreeDir = config.projectDir; // no git → work IN the project root: the semantics of opening claude in this directory (user feedback: NOT a .multi-agent/ cache path)
      console.error(`[gateway] worktree unavailable (${err instanceof Error ? err.message : err}) — working in project root ${worktreeDir}`);
    }
    hub.emit({
      agentId: "master",
      taskId: task.id,
      type: "TASK_TREE_UPDATED",
      payload: { reason: "dispatch", taskId: task.id, agentId: preassigned },
    });
    const adapter = isStream
      ? new ClaudeStreamAdapter(tier)
      : isHarness
        ? new ClaudeHarnessAdapter(tier)
        : new OpenAICompatAdapter(tier, worktreeDir, comms);
    const handle = manager.spawn(adapter, { title: task.title, spec: task.spec }, {
      taskId: task.id,
      agentId: preassigned,
      worktreeDir,
      // persist the claude session id as soon as the init frame reports it —
      // revival (roadmap-1a) needs it to outlive the process
      onSessionId: (sid) => { try { db.setSessionId(task.id, sid); } catch { /* task row gone — non-fatal */ } },
      // cost at RUN completion (same rationale as dispatchOne): waiting for
      // handle death would stall the ledger behind idle windows / stream
      // lifetimes. Per-run rows keep multi-turn conversations auditable.
      onRunComplete: (_outcome, h) => {
        const usage = (h as { id: string; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } }).finalUsage;
        if (usage) {
          db.insertCost({
            agent_id: (h as { id: string }).id,
            task_id: task.id,
            model: usage.model,
            input_tokens: usage.input,
            output_tokens: usage.output,
            cache_read: usage.cacheRead ?? 0,
            cache_write: usage.cacheWrite ?? 0,
            est_cost_usd: 0, // internal gateway — no per-token price published; ledger keeps tokens
          });
        }
        if (db.getTask(task.id).status !== "cancelled") db.setStatus(task.id, "done"); // killed tasks stay cancelled
      },
    });
    db.setStatus(task.id, "running", { assignee: handle.id, worktree_path: worktreeDir });
    return { taskId: task.id, agentId: handle.id };
  }
  if (FAKE_DECOMPOSE) {
    // dev-only stand-in for the D6 Master loop: dispatch straight to the fake.
    // Announcement goes out BEFORE spawn so clients learn the agentId before
    // its first event; clients then SUBSCRIBE + REPLAY sinceSeq (seq dedup
    // gives exactly-once at the app layer).
    fakeCounter += 1;
    const preassigned = `fake_${fakeCounter}`;
    hub.emit({
      agentId: "master",
      taskId: task.id,
      type: "TASK_TREE_UPDATED",
      payload: { reason: "dispatch", taskId: task.id, agentId: preassigned },
    });
    const handle = manager.spawn(fake, { title: task.title, spec: task.spec }, {
      taskId: task.id,
      agentId: preassigned,
      onFinished: () => db.setStatus(task.id, "done"), // D7 review loop replaces this
    });
    db.setStatus(task.id, "running", { assignee: handle.id });
    return { taskId: task.id, agentId: handle.id };
  }
  // production path waits for D6 decomposition
  return { taskId: task.id };
}

function handleClientMessage(client: HubClient, raw: unknown): void {
  if (!isClientMessage(raw)) {
    hub.emit({ agentId: "master", type: "RUN_ERROR", payload: { message: "malformed client message" } });
    return;
  }
  switch (raw.type) {
    case "SUBSCRIBE":
      client.subscriptions.add(raw.agentId);
      break;
    case "UNSUBSCRIBE":
      client.subscriptions.delete(raw.agentId);
      break;
    case "REPLAY":
      hub.replay(client, raw.sinceSeq);
      break;
    case "SUBMIT_GOAL":
      void submitGoal(raw.goal, raw.executor);
      break;
    case "INJECT": {
      const ok = manager.inject(raw.agentId, raw.prompt);
      if (!ok) {
        const why = manager.get(raw.agentId)
          ? "agent is not accepting input"
          : `agent ${raw.agentId} is not running (idle window closed or never existed) — resubmit as a new task instead`;
        // both channels: the pane of the agent you typed into AND the master
        // stream — errors must never be silent (learned the hard way today)
        hub.emit({ agentId: raw.agentId, type: "RUN_ERROR", payload: { message: `INJECT failed: ${why}` } });
        hub.emit({ agentId: "master", type: "RUN_ERROR", payload: { message: `INJECT → ${raw.agentId} failed: ${why}` } });
      }
      break;
    }
    case "INTERRUPT":
      if (!manager.interrupt(raw.agentId)) {
        hub.emit({ agentId: raw.agentId, type: "RUN_ERROR", payload: { message: `INTERRUPT failed: ${raw.agentId} not running` } });
        hub.emit({ agentId: "master", type: "RUN_ERROR", payload: { message: `INTERRUPT → ${raw.agentId} failed` } });
      }
      break;
    case "KILL": {
      const task = db.listTasks().find((t) => t.assignee === raw.agentId);
      const killed = manager.interrupt(raw.agentId) || !!task; // interrupt sends SIGINT; kill() below escalates
      if (!killed) {
        hub.emit({ agentId: raw.agentId, type: "RUN_ERROR", payload: { message: `unknown agent: ${raw.agentId}` } });
        break;
      }
      if (task) db.setStatus(task.id, "cancelled"); // set BEFORE kill so onFinished won't mark done
      const handle = manager.get(raw.agentId);
      void handle?.kill(); // SIGKILL escalation + stream teardown
      if (task) {
        hub.emit({ agentId: "master", taskId: task.id, type: "TASK_TREE_UPDATED", payload: { reason: "human", taskId: task.id } });
        if (task.worktree_path) {
          void removeWorktree(task.id).catch((e) => console.error("[gateway] worktree cleanup:", e));
        }
      }
      break;
    }
    case "APPROVE":
      if (!manager.approve(raw.agentId, raw.toolCallId, raw.decision, raw.patch)) {
        hub.emit({ agentId: raw.agentId, type: "RUN_ERROR", payload: { message: `unknown agent: ${raw.agentId}` } });
      }
      break;
  }
}

function json(res: http.ServerResponse, code: number, body: unknown): void {
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    // single-user local tool: the web dev server (3000) and gateway (8787) are
    // different origins — POST cross-origin needs these, or the browser
    // blocks the fetch (learned live during D5 UI testing)
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "content-type",
  });
  res.end(JSON.stringify(body));
}

// hoisted: the /api/project/open handler (registered on the server below) needs
// to tear both down for a clean project switch
let wss: import("ws").WebSocketServer;
const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", `http://localhost:${PORT}`);
  try {
    if (req.method === "OPTIONS") {
      // CORS preflight — same headers as above, 204 no content
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, POST, OPTIONS",
        "access-control-allow-headers": "content-type",
      });
      res.end();
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/internal/mailbox") {
      // internal endpoint for the harness MCP server (comms.mjs) — never
      // exposed beyond localhost (server binds 127.0.0.1); bounded fields,
      // whitelisted ops (R13/R14/R25)
      let body = "";
      req.on("data", (c) => { body += c; if (body.length > 20_000) req.destroy(); });
      req.on("end", async () => {
        try {
          const parsed = JSON.parse(body) as { agent?: string; op?: string; to?: string; mailboxType?: string; text?: string; title?: string; spec?: string; tier?: string };
          const agent = typeof parsed.agent === "string" && parsed.agent.length <= 120 ? parsed.agent : null;
          if (!agent) { json(res, 400, { ok: false, error: "agent required" }); return; }
          if (parsed.op === "send") {
            const r = comms.sendMessage(agent, String(parsed.to), String(parsed.mailboxType) as "report" | "clarify" | "note", String(parsed.text ?? ""));
            json(res, r.ok ? 200 : 400, r);
            return;
          }
          if (parsed.op === "spawn") {
            const r = comms.spawnAgent(
              agent,
              typeof parsed.title === "string" ? parsed.title.slice(0, 200) : "",
              typeof parsed.spec === "string" ? parsed.spec.slice(0, 4000) : "",
              parsed.tier === "worker" ? "worker" : "planner",
              "harness",
            );
            json(res, r.ok ? 200 : 400, r);
            return;
          }
          if (parsed.op === "inbox") {
            json(res, 200, comms.inbox(agent));
            return;
          }
          if (parsed.op === "listAgents") {
            json(res, 200, comms.listAgents());
            return;
          }
          json(res, 400, { ok: false, error: "invalid op" });
        } catch {
          json(res, 400, { ok: false, error: "invalid JSON body" });
        }
      });
    } else if (req.method === "GET" && url.pathname === "/health") {
      json(res, 200, { ok: true, lastSeq: db.lastSeq(), clients: hub.clientCount(), liveAgents: manager.liveCount(), liveAgentIds: manager.liveIds(), roster: manager.roster() });
    } else if (req.method === "GET" && url.pathname === "/api/tree") {
      json(res, 200, { ...db.treeSnapshot(), projectDir: config.projectDir });
    } else if (req.method === "GET" && url.pathname === "/api/tiers") {
      // what models actually serve each tier — read from env, never from code.
      // label: an explicit <TIER>_LABEL wins, else the model id (self-explanatory).
      const tiersOf = (t: "planner" | "worker") => {
        const prefixes = t === "planner" ? ["PLANNER", "GLM"] : ["WORKER", "DSV4"];
        const pick = (n: string) => prefixes.map((p) => process.env[`${p}_${n}`]).find((v) => v && v.trim())?.trim();
        const model = pick("MODEL");
        const label = pick("LABEL") ?? model ?? (t === "planner" ? "Planner" : "Worker");
        return { id: t, label, model: model ?? null };
      };
      json(res, 200, { tiers: [tiersOf("planner"), tiersOf("worker")] });
    } else if (req.method === "GET" && url.pathname === "/api/projects") {
      // the sidebar registry: every project ever opened, newest first.
      // currentProject lets the UI highlight without a second round trip.
      json(res, 200, { projects: listProjects(), currentProject: config.projectDir });
    } else if (req.method === "GET" && url.pathname === "/api/cost") {
      json(res, 200, { totals: db.costByTask() });
    } else if (req.method === "POST" && url.pathname === "/api/project/pick") {
      // the native folder picker behind the UI's 📂 button — macOS dev shape
      // of what the packaged app will do with its own dialog. The command is
      // a fixed literal (no shell, no user input reaches the command line —
      // R17/R25: the picked path is validated by the /open endpoint, not here).
      if (process.platform !== "darwin") { json(res, 501, { error: "native picker only on macOS — type the path instead" }); return; }
      execFile("osascript", ["-e", 'POSIX path of (choose folder with prompt "选择要打开的项目目录")'], { timeout: 300_000 }, (err, stdout) => {
        const code = (err as { code?: number } | null)?.code;
        if (code === -128 || (err && !stdout)) { json(res, 200, { cancelled: true }); return; } // user hit cancel
        if (err) { json(res, 500, { error: `osascript failed: ${err.message}` }); return; }
        const dir = String(stdout).trim().replace(/\/+$/, "");
        json(res, 200, { dir });
      });
    } else if (req.method === "POST" && url.pathname === "/api/project/open") {
      // roadmap-4b "Open Project", HOT version: rebind the SAME process —
      // no restart, no WS drop, imperceptible. Order matters:
      //   1. stop old agents FIRST (their death events belong to the OLD
      //      project's event history)
      //   2. switch config + rebind the shared WorkbenchDb (identity stable,
      //      every holder just works) + hub follows + workspace bootstrap
      //   3. broadcast the flip on the NEW db; every connected page's
      //      loadTree() sees the projectDir change, wipes local pane state,
      //      and REPLAY rebuilds the new project
      // Old project's DB/worktrees stay on disk untouched (stop, not delete).
      let body = "";
      req.on("data", (c) => { body += c; if (body.length > 10_000) req.destroy(); });
      req.on("end", () => {
        let dir = "";
        let init = false;
        try {
          const parsed = JSON.parse(body) as { dir?: unknown; init?: unknown };
          dir = typeof parsed.dir === "string" ? parsed.dir.trim() : "";
          init = parsed.init === true;
        } catch { /* fallthrough to validation error */ }
        if (!dir) { json(res, 400, { error: "dir required (absolute path to the project)" }); return; }
        const abs = path.resolve(dir.replace(/^~(?=\/|$)/, process.env.HOME ?? "~"));
        let stat: { isDirectory: () => boolean } | undefined;
        try { stat = statSync(abs); } catch { json(res, 404, { error: `目录不存在: ${abs}` }); return; }
        if (!stat.isDirectory()) { json(res, 400, { error: "不是目录" }); return; }
        if (init) {
          // "新建项目": a fresh dir needs git or executors lose worktree
          // isolation — init an empty non-git dir instead of failing later.
          // Only ever ADDS .git to an empty directory the operator just picked.
          if (!existsSync(path.join(abs, ".git")) && readdirSync(abs).length === 0) {
            execFileSync("git", ["init", "-q", abs]);
            console.log(`[gateway] new project: git init ${abs}`);
          }
        }
        registerProject(abs); // sidebar registry (current project included on re-open)
        if (abs === config.projectDir) { json(res, 200, { ok: true, projectDir: abs, already: true }); return; }
        const reaped = manager.killAll(); // deaths recorded in the OLD db
        if (reaped > 0) console.log(`[gateway] project switch → ${abs}: stopped ${reaped} agent(s)`);
        const notGit = !existsSync(path.join(abs, ".git"));
        config.switchProject(abs);
        setCurrentProjectPointer(abs); // crash/dev-restart recovery returns to the same project
        bootstrapWorkspace();
        db.rebind(config.dbPath);
        hub.rebind(db);
        console.log(`[gateway] project REBOUND (hot): ${abs} — db ${config.dbPath} (lastSeq=${db.lastSeq()})`);
        // every connected UI page: reload the tree, notice the projectDir flip,
        // wipe old panes, REPLAY the new project's history
        hub.emit({ agentId: "master", type: "TASK_TREE_UPDATED", payload: { reason: "project-switch", projectDir: abs, gitWarning: notGit ? "目标不是 git 仓库——agent 无 worktree 隔离，只能只读/对话" : undefined } });
        json(res, 200, { ok: true, projectDir: abs });
      });
    } else if (req.method === "POST" && url.pathname === "/api/agents/revive") {
      // roadmap-1a: in-place revival. Same agent id (panes/subscriptions keep
      // working — the _r-suffix scheme of the first attempt broke exactly
      // there), same worktree (recreated if the ENOENT lesson struck), the
      // claude session restored via --resume. Lite (oc_) agents have no
      // persistent session — rejected with a clear error, never silence.
      let body = "";
      req.on("data", (c) => { body += c; if (body.length > 10_000) req.destroy(); });
      req.on("end", async () => {
        const fail = (msg: string) => {
          // both channels, like INJECT failures — errors must never be silent
          hub.emit({ agentId: "master", type: "RUN_ERROR", payload: { message: `唤醒失败: ${msg}` } });
          json(res, 409, { error: msg });
        };
        try {
          const parsed = JSON.parse(body) as { agentId?: unknown };
          const agentId = typeof parsed.agentId === "string" ? parsed.agentId : "";
          if (!/^(ch|cs)_/.test(agentId)) { fail(agentId.startsWith("oc_") ? "lite agent 没有持久会话，无法唤醒（请提交新任务）" : "agentId 必须是 ch_/cs_ 前缀"); return; }
          const task = db.listTasks().find((t) => t.assignee === agentId);
          if (!task) { fail(`找不到 agent ${agentId} 对应的任务`); return; }
          if (manager.get(agentId)) { fail(`agent ${agentId} 还活着（roster 里），不用唤醒`); return; }
          if (!task.session_id) { fail(`任务 ${task.id} 没有记录 session id（老任务或 lite 运行）——无法唤醒，请提交新任务`); return; }
          const isStream = agentId.startsWith("cs_");
          const tier = tierOf(task.model_hint);
          // ENOENT lesson: spawn CWD must exist — recreate the worktree if
          // it was reaped by a previous KILL
          let worktreeDir: string | undefined;
          if (task.worktree_path && !existsSync(task.worktree_path)) {
            try {
              const wt = await createWorktree(task.id);
              worktreeDir = wt.dir;
              console.log(`[revive] worktree recreated for ${agentId}: ${worktreeDir}`);
            } catch (e) {
              console.error(`[revive] worktree recreate failed:`, e instanceof Error ? e.message : e);
              worktreeDir = undefined;
            }
          } else {
            worktreeDir = task.worktree_path ?? undefined;
          }
          const adapter = isStream
            ? new ClaudeStreamAdapter(tier)
            : new ClaudeHarnessAdapter(tier);
          const handle = manager.spawn(adapter, { title: task.title, spec: task.spec }, {
            taskId: task.id,
            agentId, // SAME id — in-place revival, no _r suffix
            worktreeDir,
            resumeSessionId: task.session_id,
            onSessionId: (sid) => { try { db.setSessionId(task.id, sid); } catch { /* non-fatal */ } },
            onRunComplete: (_o, h) => {
              const usage = (h as { id: string; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } }).finalUsage;
              if (usage) {
                db.insertCost({
                  agent_id: (h as { id: string }).id,
                  task_id: task.id,
                  model: usage.model,
                  input_tokens: usage.input,
                  output_tokens: usage.output,
                  cache_read: usage.cacheRead ?? 0,
                  cache_write: usage.cacheWrite ?? 0,
                  est_cost_usd: 0,
                });
              }
              // a cancelled task being revived is un-cancelled by this run
              db.setStatus(task.id, "done");
            },
          });
          db.setStatus(task.id, "running", { assignee: agentId, worktree_path: worktreeDir });
          hub.emit({ agentId: "master", taskId: task.id, type: "TASK_TREE_UPDATED", payload: { reason: "revived", taskId: task.id, agentId: handle.id } });
          json(res, 200, { ok: true, agentId: handle.id });
        } catch {
          json(res, 400, { error: "invalid JSON body" });
        }
      });
    } else if (req.method === "POST" && url.pathname === "/api/tasks/name") {
      // roadmap-2: rename an agent. Bounded (R25), empty string clears the
      // nickname → falls back to title. IDOR note: single-user workbench, no
      // cross-tenant resource — task existence is the only check.
      let body = "";
      req.on("data", (c) => { body += c; if (body.length > 10_000) req.destroy(); });
      req.on("end", () => {
        try {
          const parsed = JSON.parse(body) as { taskId?: unknown; name?: unknown };
          if (typeof parsed.taskId !== "string" || !/^t_/.test(parsed.taskId)) { json(res, 400, { error: "taskId required" }); return; }
          const name = typeof parsed.name === "string" ? parsed.name.trim() : "";
          if (name.length > 60) { json(res, 400, { error: "name too long (max 60)" }); return; }
          const t = db.listTasks().find((x) => x.id === parsed.taskId);
          if (!t) { json(res, 404, { error: "task not found" }); return; }
          db.setTaskName(t.id, name.length === 0 ? null : name);
          hub.emit({ agentId: "master", taskId: t.id, type: "TASK_TREE_UPDATED", payload: { reason: "rename", taskId: t.id } });
          json(res, 200, { ok: true, displayName: name.length === 0 ? null : name });
        } catch {
          json(res, 400, { error: "invalid JSON body" });
        }
      });
    } else if (req.method === "POST" && url.pathname === "/api/tasks") {
      let body = "";
      req.on("data", (chunk) => {
        body += chunk;
        if (body.length > 1_000_000) req.destroy(); // bounded input → controlled 4xx, not a 500
      });
      req.on("end", async () => {
        try {
          const parsed = JSON.parse(body) as { goal?: unknown; executor?: unknown };
          if (typeof parsed.goal !== "string" || parsed.goal.trim().length === 0) {
            json(res, 400, { error: "goal must be a non-empty string" });
            return;
          }
          const r = await submitGoal(parsed.goal, typeof parsed.executor === "string" ? parsed.executor : undefined);
          json(res, 202, { taskId: r.taskId, agentId: r.agentId ?? null });
        } catch {
          json(res, 400, { error: "invalid JSON body" });
        }
      });
    } else {
      json(res, 404, { error: "not found" });
    }
  } catch (err) {
    console.error("[gateway] request error:", err);
    json(res, 500, { error: "internal error" }); // sanitized — no internals to the wire
  }
});

wss = new WebSocketServer({ server });
wss.on("connection", (ws: WebSocket) => {
  const client = hub.attach(ws);
  ws.on("message", (data) => {
    try {
      handleClientMessage(client, JSON.parse(String(data)));
    } catch {
      handleClientMessage(client, undefined);
    }
  });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`[gateway] listening on http://localhost:${PORT} (ws on the same port)`);
  console.log(`[gateway] project dir: ${config.projectDir} — all artifacts in ${config.workspaceDir}`);
});

for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.on(sig, () => {
    const reaped = manager.killAll(); // reap executors first — orphaned claude processes keep billing
    console.log(`[gateway] ${sig} — reaped ${reaped} executor(s), closing`);
    wss.close();
    server.close(() => db.close());
    process.exit(0);
  });
}

// ── crash-containment (user-flagged severity): if the gateway DIES — kill -9,
// OOM, an uncaught throw — its spawned claude agents must not survive it as
// orphans billing tokens in the background. Two walls:
//
//  1. uncaught exception/rejection → reap everything, THEN exit. A crash now
//     takes its agents with it instead of leaking them.
//  2. boot-time orphan sweep: on every startup, scan for claude/comms
//     processes that carry OUR mcp-comms fingerprint but whose parent is init
//     (ppid 1) — i.e. survivors of a previous gateway that died hard — and
//     kill them. Bounded blast radius: the fingerprint (comms.mjs path +
//     --input-format stream-json) only matches processes WE spawned.
process.on("uncaughtException", (err) => {
  console.error("[gateway] UNCAUGHT EXCEPTION — reaping all agents before dying:", err);
  const reaped = manager.killAll();
  console.error(`[gateway] reaped ${reaped} executor(s) on crash exit`);
  process.exit(1);
});
process.on("unhandledRejection", (reason) => {
  console.error("[gateway] UNHANDLED REJECTION — reaping all agents before dying:", reason);
  const reaped = manager.killAll();
  console.error(`[gateway] reaped ${reaped} executor(s) on crash exit`);
  process.exit(1);
});

function sweepOrphanAgents(): void {
  try {
    const out = execFileSync("/bin/ps", ["-eo", "pid=,ppid=,command="], { encoding: "utf8" });
    // pid -> {ppid, cmd}; walk every fingerprinted process's ancestry: if the
    // chain never reaches THIS process, it belongs to a dead (or zombie-test)
    // gateway and gets killed. ppid==1 alone was insufficient — a leftover
    // `pnpm dev:gateway` in an agent worktree keeps whole tsx→node→claude
    // trees alive with no terminal, billing nothing forever (live finding).
    const procs = new Map<number, { ppid: number; cmd: string }>();
    for (const line of out.split("\n")) {
      const m = /^\s*(\d+)\s+(\d+)\s+(.*)$/.exec(line);
      if (!m) continue;
      procs.set(Number(m[1]), { ppid: Number(m[2]), cmd: m[3] ?? "" });
    }
    const isDescendant = (pid: number): boolean => {
      let cur = pid;
      const seen = new Set<number>();
      while (procs.has(cur) && !seen.has(cur)) {
        if (cur === process.pid) return true;
        seen.add(cur);
        cur = procs.get(cur)!.ppid;
      }
      return false;
    };
    let n = 0;
    for (const [pid, { cmd }] of procs) {
      if (pid === process.pid) continue;
      const ours = cmd.includes("maw-comms") || cmd.includes("comms.mjs");
      const claudeish = cmd.includes("stream-json") || cmd.includes("claude -p") || cmd.includes("fake-claude");
      if (!ours || !claudeish) continue;
      if (isDescendant(pid)) continue; // our own live agent — the current gateway owns it
      // kill the whole stray tree from its highest non-descendant ancestor:
      // killing only the leaf leaves tsx/node/bash midlayers spinning
      try {
        process.kill(pid, "SIGKILL");
        n++;
      } catch { /* already gone */ }
    }
    if (n > 0) console.log(`[gateway] orphan sweep: killed ${n} leftover maw agent process(es) from dead/stray gateways`);
  } catch (e) {
    console.warn(`[gateway] orphan sweep failed:`, e instanceof Error ? e.message : e);
  }
}
sweepOrphanAgents();

// ---------- D6: orchestrated master loop ----------

async function orchestrate(goal: string, rootTaskId: string): Promise<void> {
  hub.emit({ agentId: "master", taskId: rootTaskId, type: "AGENT_STATUS", payload: { status: "thinking", detail: "decomposing goal" } });
  const r = await decomposeGoal(db, hub, goal, rootTaskId, {});
  if (!r.ok || !r.tree) {
    console.error("[master] decomposition failed:", r.error);
    db.setStatus(rootTaskId, "awaiting_approval");
    hub.emit({ agentId: "master", taskId: rootTaskId, type: "RUN_ERROR", payload: { message: r.error ?? "decomposition failed" } });
    hub.emit({ agentId: "master", taskId: rootTaskId, type: "TASK_TREE_UPDATED", payload: { reason: "decomposed", taskId: rootTaskId } });
    return;
  }
  const ctx = await projectContext();
  const { planted } = plantTree(db, hub, rootTaskId, r.tree, ctx);
  db.setStatus(rootTaskId, "done", { assignee: "master" }); // root = the plan itself
  hub.emit({ agentId: "master", taskId: rootTaskId, type: "TEXT_MESSAGE_END", payload: { finalAnswer: `计划完成：${planted} 个子任务已入树，开始派发。` } });
  hub.emit({ agentId: "master", taskId: rootTaskId, type: "AGENT_STATUS", payload: { status: "idle" } });
  await dispatchReady(rootTaskId);
}

/** Dispatch queued tasks whose dependencies are done, up to MAX_PARALLEL. */
async function dispatchReady(rootTaskId: string): Promise<void> {
  const children = db.children(rootTaskId);
  const running = children.filter((c) => c.status === "running").length;
  const ready = children.filter((c) => c.status === "queued" && depsDone(db, c.id));
  let slots = MAX_PARALLEL_WORKERS - running;
  for (const child of ready) {
    if (slots <= 0) break;
    slots--;
    await dispatchOne(child.id);
  }
}

function depsDone(db: WorkbenchDb, taskId: string): boolean {
  // deps column (JSON array of task ids) — planted by the Master, resolved
  // at plant time. A dangling ref must not abort dispatchReady: skip it
  // (treat as satisfied) rather than throw mid-filter.
  let deps: string[] = [];
  try { deps = JSON.parse(db.getTask(taskId).deps || "[]") as string[]; } catch { return true; }
  for (const d of deps) {
    try {
      const s = db.getTask(d).status;
      // done = satisfied; awaiting_approval = the human is judging the UPSTREAM
      // work, but blocking every downstream task on that is cascade-freeze —
      // a downstream task can start and be superseded at merge time
      if (s !== "done" && s !== "awaiting_approval") return false;
    } catch {
      continue; // dangling dep id from bad model output — ignore, don't deadlock
    }
  }
  return true;
}

async function dispatchOne(taskId: string): Promise<void> {
  const task = db.getTask(taskId);
  // D11 budget gate: real ledger spend decides tier/block BEFORE spawn
  const budget = enforceBudget(db, taskId);
  const routed = applyBudget(
    { modelHint: task.model_hint ?? "planner", specChars: task.spec.length, parallelizable: true },
    budget,
  );
  if (!routed.profile) {
    db.setStatus(taskId, "blocked");
    hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "budget-block", detail: routed.reason } });
    db.insertMessage({ from_agent: "master", to_agent: "human", task_id: taskId, type: "broadcast", payload: { note: `blocked: ${routed.reason}` } });
    console.log(`[router] blocked ${taskId}: ${routed.reason}`);
    return;
  }
  if (routed.reason) console.log(`[router] downgraded ${taskId}: ${routed.reason}`);
  const executor = resolveExecutorForTask(routed.profile);
  const preassigned = `${executor.startsWith("stream-") ? "cs" : executor.startsWith("claude-") ? "ch" : "oc"}_${taskId}`;
  // re-dispatch: the previous attempt left a worktree/branch behind — clean it
  // so the worker starts fresh with the feedback-augmented spec
  await removeWorktree(taskId).catch(() => undefined);
  let worktreeDir: string | undefined;
  try {
    worktreeDir = (await createWorktree(taskId)).dir;
  } catch (err) {
    worktreeDir = config.projectDir; // same semantics as submitGoal's fallback: the project root, never the gateway repo, never a hidden cache dir
    console.error(`[gateway] worktree unavailable (${err instanceof Error ? err.message : err}) — working in project root ${worktreeDir}`);
  }
  hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "dispatch", taskId, agentId: preassigned } });
  const isStream = executor.startsWith("stream-");
  const isHarness = executor.startsWith("claude-");
  const tier = (isStream ? executor.slice("stream-".length) : isHarness ? executor.slice("claude-".length) : executor) as "planner" | "worker";
  const adapter = isStream
    ? new ClaudeStreamAdapter(tier)
    : isHarness
      ? new ClaudeHarnessAdapter(tier)
      : new OpenAICompatAdapter(tier, worktreeDir, comms);
  let reviewed = false;
  manager.spawn(adapter, { title: task.title, spec: task.spec }, {
    taskId,
    agentId: preassigned,
    worktreeDir,
    // task status + review key off RUN completion — never the handle's death,
    // which for harness executors sits behind the 30-min idle window
    onRunComplete: (outcome, h) => {
      console.log(`[cost] onRunComplete ${taskId} outcome=${outcome} finalUsage=${JSON.stringify((h as { finalUsage?: unknown }).finalUsage ?? null)}`);
      // cost accounting at RUN completion — waiting for handle death would
      // stall the ledger by the idle window (harness: 30min) or forever
      // (stream agents live until KILL). One row per run keeps multi-turn
      // conversations auditable turn by turn.
      const usage = (h as { id: string; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } }).finalUsage;
      if (usage) {
        db.insertCost({ agent_id: (h as { id: string }).id, task_id: taskId, model: usage.model, input_tokens: usage.input, output_tokens: usage.output, cache_read: usage.cacheRead ?? 0, cache_write: usage.cacheWrite ?? 0, est_cost_usd: 0 });
      }
      if (reviewed) return; // review once per dispatch (re-dispatch resets)
      reviewed = true;
      if (outcome === "success") {
        void reviewAndAdvance(h as { id: string; finalUsage?: { input: number; output: number; model: string } }, taskId);
      } else if (outcome === "interrupt") {
        redispatchOrEscalate(taskId, "run interrupted by human");
      } else {
        db.setStatus(taskId, "failed");
        hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "error" } });
      }
    },
  });
  db.setStatus(taskId, "running", { assignee: preassigned, worktree_path: worktreeDir });
  // execution watchdog: a hung executor (gateway congestion, stream stall)
  // must not hold a task in 'running' forever — force-fail after the cap
  const watchdog = setTimeout(() => {
    const t = db.getTask(taskId);
    if (t.status === "running") {
      db.setStatus(taskId, "failed");
      hub.emit({ agentId: "master", taskId, type: "RUN_ERROR", payload: { message: `execution watchdog: ${EXEC_TIMEOUT_MS / 60000}min cap exceeded` } });
      console.log(`[watchdog] task ${taskId} force-failed after ${EXEC_TIMEOUT_MS / 60000}min`);
    }
  }, EXEC_TIMEOUT_MS);
}

// ---------- D7: review loop ----------

/** Extract the worker's own summary. v1: generic — the full event text
 *  would be better evidence for the reviewer; tracked for D9 polish. */
function workerReport(_handle: unknown): string {
  return "worker run completed";
}

async function reviewAndAdvance(handle: { id: string; events?: unknown; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } }, taskId: string): Promise<void> {
  try {
    await reviewAndAdvanceInner(handle, taskId);
  } catch (err) {
    // a crashed review must not strand the task in 'running' forever —
    // escalate so a human sees it (found via D8 round-3: silent void-reject)
    console.error(`[review] reviewAndAdvance crashed for ${taskId}:`, err);
    db.setStatus(taskId, "awaiting_approval");
    hub.emit({ agentId: "master", taskId, type: "RUN_ERROR", payload: { message: `review pipeline crashed: ${err instanceof Error ? err.message : String(err)}` } });
  }
}

async function reviewAndAdvanceInner(handle: { id: string; events?: unknown; finalUsage?: { input: number; output: number; cacheRead?: number; cacheWrite?: number; model: string } }, taskId: string): Promise<void> {
  const task = db.getTask(taskId);
  if (task.status === "cancelled") return; // KILL'd mid-run — stay cancelled

  // NOTE: no insertCost here — dispatchOne's onRunComplete already writes one
  // cost_ledger row per run (including the first); duplicating it here double-
  // counted every successful first run.

  // worker reports via the mailbox (agent-to-agent, zero human relay)
  db.insertMessage({ from_agent: handle.id, to_agent: "master", task_id: taskId, type: "report", payload: { agent: handle.id, worktree: task.worktree_path } });

  // interrupted/failed runs skip review — straight to re-dispatch budget
  const interrupted = false; // v1: interrupted runs close the generator before we can see outcome here; D9 tightens
  if (interrupted) {
    redispatchOrEscalate(taskId, "run interrupted");
    return;
  }

  hub.emit({ agentId: "master", taskId, type: "AGENT_STATUS", payload: { status: "thinking", detail: "reviewing worker report" } });
  const diff = await diffWorktree(taskId, 6000, task.worktree_path);
  const workerMsgs = db.messagesFrom(handle.id)
    .filter((m) => m.type === "report" || m.type === "clarify" || m.type === "note")
    .map((m) => ({ type: m.type, text: String((JSON.parse(m.payload) as { text?: string }).text ?? m.payload) }));
  const review = await reviewWork(db, task, { summary: workerReport(handle), diff, messages: workerMsgs });

  if (!review.ok) {
    // review infrastructure failed — do NOT guess; escalate to the human
    db.setStatus(taskId, "awaiting_approval");
    hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "escalate", detail: review.error } });
    hub.emit({ agentId: "master", taskId, type: "RUN_ERROR", payload: { message: `review failed: ${review.error}` } });
    return;
  }

  if (review.verdict === "pass") {
    db.setStatus(taskId, "done");
    // self-implementation closeout: accepted work merges to the trunk
    const merge = await mergeWorktree(taskId).catch((e) => ({ ok: false, detail: String(e) }));
    if (!merge.ok) {
      db.setStatus(taskId, "awaiting_approval");
      db.insertMessage({ from_agent: "master", to_agent: "human", task_id: taskId, type: "broadcast", payload: { note: `PASSED review but merge FAILED: ${merge.detail} — resolve manually` } });
      hub.emit({ agentId: "master", taskId, type: "RUN_ERROR", payload: { message: `merge failed: ${merge.detail}` } });
      return;
    }
    db.insertMessage({ from_agent: "master", to_agent: "human", task_id: taskId, type: "broadcast", payload: { note: `task passed review: ${task.title} (${merge.detail})` } });
    // notify the COORDINATOR that spawned this worker — its report went to
    // master for review, but the reviewer's verdict never flowed back, so
    // the coordinator sat waiting for a result that HAD arrived (live p2p gap).
    // MUST ride comms.sendMessage: a direct db.insertMessage + hub.emit
    // bypasses hooks.onAgentMessage, so the coordinator never wakes.
    const notified = notifyCoordinatorPassed(comms, db, task, workerMsgs);
    if (notified.sent) console.log(`[review] notified coordinator ${notified.coordinator}: task ${taskId} passed`);
    else if (notified.error) console.error(`[review] coordinator notify failed for ${taskId} → ${notified.coordinator}: ${notified.error}`);
    hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "pass", merged: merge.detail } });
    // dynamic staffing: the reviewer may have requested one more worker
    if (review.spawn && task.parent_id) {
      const spawnId = newTaskId();
      db.insertTask({
        id: spawnId,
        parent_id: task.parent_id,
        title: review.spawn.title,
        spec: review.spawn.spec,
        status: "queued",
        deps: review.spawn.dependsOnReviewed ? [taskId] : [],
      });
      hub.emit({
        agentId: "master",
        taskId: spawnId,
        type: "TASK_TREE_UPDATED",
        payload: { reason: "decomposed", taskId: spawnId, planted: 1, spawnedByReview: true, parentTask: taskId },
      });
      console.log(`[master] review spawned a new worker: ${review.spawn.title}`);
    }
    await dispatchReady(task.parent_id!);
    return;
  }

  // fail → feedback into the task spec (visible to the next worker run),
  // budget-checked re-dispatch
  redispatchOrEscalate(taskId, review.feedback ?? "review verdict: fail (no feedback given)");
}

async function redispatchOrEscalate(taskId: string, feedback: string): Promise<void> {
  const task = db.getTask(taskId);
  if (task.attempts >= MAX_REDISPATCH) {
    db.setStatus(taskId, "awaiting_approval");
    db.insertMessage({ from_agent: "master", to_agent: "human", task_id: taskId, type: "broadcast", payload: { note: `escalated after ${task.attempts} re-dispatch(es): ${task.title}`, feedback } });
    hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "escalate" } });
    return;
  }
  const attempts = task.attempts + 1;
  // feedback rides the spec so the next worker run sees it (worktrees are fresh)
  const specWithFeedback = `${task.spec}\n\n# Reviewer feedback from attempt ${attempts} — fix these issues:\n${feedback}`;
  db.updateTaskSpec(taskId, specWithFeedback);
  db.setStatus(taskId, "queued", { attempts });
  db.insertMessage({ from_agent: "master", to_agent: task.assignee ?? "unknown", task_id: taskId, type: "re-dispatch", payload: { feedback, attempt: attempts } });
  hub.emit({ agentId: "master", taskId, type: "TASK_TREE_UPDATED", payload: { reason: "review", taskId, verdict: "fail", attempt: attempts } });
  await dispatchReady(task.parent_id!);
}
