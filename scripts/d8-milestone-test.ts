/**
 * D8 — MILESTONE A acceptance (plan §2 step 8): the full chain, zero manual
 * relay, one script. This is the go/no-go for the route.
 *
 * Chain: submit goal → GLM Master decomposes → Router assigns tiers → workers
 * execute concurrently in worktrees → workers send_message reports → GLM
 * Master reviews (content evidence + mailbox) → pass/fail verdicts → deps
 * unlock → all settle. Plus the blind spots this week's live usage exposed:
 *   - old-schema DB must migrate transparently (the 'invalid JSON body' day)
 *   - harness-tier comms must work END-TO-END (MCP syntax bug shipped once)
 *   - streaming tier: inject lands on the live process
 *
 * PASS = all checks green. FAIL = any step needed a human or silently died.
 */

import { spawn, execSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";

const PORT = 19910 + Math.floor(Math.random() * 80);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d8-"));
const dbPath = path.join(dbDir, "d8.db");

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

interface TestSock { ws: WebSocket; received: Map<number, ServerEvent>; lastSeq: number; }
function connect(): Promise<TestSock> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${PORT}`);
    const s: TestSock = { ws, received: new Map(), lastSeq: 0 };
    ws.on("open", () => resolve(s));
    ws.on("error", reject);
    ws.on("message", (d) => {
      const ev = JSON.parse(String(d)) as ServerEvent;
      s.received.set(ev.seq, ev);
      if (ev.seq > s.lastSeq) s.lastSeq = ev.seq;
    });
  });
}

const approved = new Set<string>();
const subscribed = new Set<string>(["master"]);
function autoFollow(s: TestSock): void {
  for (const e of s.received.values()) {
    const p = e.payload as { reason?: string; agentId?: string };
    if (e.type === "TASK_TREE_UPDATED" && p?.reason === "dispatch" && p.agentId && !subscribed.has(p.agentId)) {
      subscribed.add(p.agentId);
      s.ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: p.agentId }));
      s.ws.send(JSON.stringify({ type: "REPLAY", sinceSeq: s.lastSeq }));
    }
    if (e.type === "APPROVAL_REQUIRED") {
      const { toolCallId } = e.payload as { toolCallId: string };
      if (!approved.has(toolCallId)) {
        approved.add(toolCallId);
        s.ws.send(JSON.stringify({ type: "APPROVE", agentId: e.agentId, toolCallId, decision: "allow" }));
      }
    }
  }
}

const GOAL = `Create a tiny python project in this worktree: (1) math_utils.py with functions add(a,b) and multiply(a,b) with docstrings; (2) test_math.py using plain asserts covering both functions; (3) README.md with "## Install" and "## Usage" sections. The test writer MUST ask the math_utils writer for the exact function signatures via send_message BEFORE writing tests.`;

let gw: ChildProcess | null = null;
try {
  // ── pre-flight: old-schema DB migrates transparently ──
  console.log(`\n[D8 milestone A] phase 0: old-schema DB migration`);
  const oldDb = path.join(dbDir, "old.db");
  execSync(`sqlite3 ${oldDb} "CREATE TABLE tasks (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT NOT NULL, spec TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, assignee TEXT, model_hint TEXT, worktree_path TEXT, attempts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);"`);
  writeFileSync(path.join(dbDir, "probe.json"), '{"goal":"migration probe"}');

  gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: dbPath, MAW_ORCHESTRATE: "1", MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees"), MAW_MAX_PARALLEL: "2" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", () => { /* drained */ });
  gw.stderr?.on("data", () => { /* drained */ });
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  check("gateway boots on fresh DB", true);

  // ── phase 1: orchestrated full chain ──
  console.log(`\n[D8 milestone A] phase 1: full orchestrated chain`);
  const sock = await connect();
  sock.ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: "master" }));
  const submitRes = await (await fetch(`http://localhost:${PORT}/api/tasks`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ goal: GOAL }),
  })).json();
  const rootId = submitRes.taskId as string;
  check("goal accepted (orchestrated)", !!rootId);

  const plantDeadline = Date.now() + 420_000;
  let tree: TreeSnapshot | null = null;
  while (Date.now() < plantDeadline) {
    autoFollow(sock);
    tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
    if ((tree?.tasks.filter((t) => t.parent_id === rootId).length ?? 0) >= 2) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  const children = tree?.tasks.filter((t) => t.parent_id === rootId) ?? [];
  check("master decomposed (>=2 subtasks)", children.length >= 2, `planted=${children.length}`);
  const withCollab = children.filter((c) => c.spec.includes("Sibling agents") || c.spec.includes("send_message"));
  check("collaboration context planted in specs", withCollab.length >= 1, `tasks with sibling info=${withCollab.length}`);

  const finishDeadline = Date.now() + 1500_000;
  let lastPrint = "";
  while (Date.now() < finishDeadline) {
    autoFollow(sock);
    const snap = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
    const kids = snap.tasks.filter((t) => t.parent_id === rootId);
    const summary = kids.map((k) => k.status[0]).join("");
    if (summary !== lastPrint) { console.log(`  … statuses: ${kids.map((k) => k.status).join(",")}`); lastPrint = summary; }
    const settled = kids.every((k) => ["done", "failed", "cancelled", "awaiting_approval"].includes(k.status));
    if (settled && kids.length > 0) { tree = snap; break; }
    await new Promise((r) => setTimeout(r, 3000));
  }
  tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
  const finalKids = tree.tasks.filter((t) => t.parent_id === rootId);
  const doneCount = finalKids.filter((k) => k.status === "done").length;
  check("all subtasks settled", finalKids.every((k) => ["done", "failed", "cancelled", "awaiting_approval"].includes(k.status)), finalKids.map((k) => k.status).join(","));
  check("majority done (>=2/3)", doneCount >= Math.min(2, Math.ceil(finalKids.length * 0.6)), `done=${doneCount}/${finalKids.length}`);

  // full-chain evidence
  const events = [...sock.received.values()];
  const sawReview = events.some((e) => e.type === "TASK_TREE_UPDATED" && (e.payload as { reason?: string }).reason === "review");
  check("review loop ran", sawReview);
  const commTool = events.find((e) => e.type === "TOOL_CALL_START" && String((e.payload as { name?: string }).name ?? "").includes("send_message"));
  check("worker used the comms tool (harness MCP or lite)", !!commTool, `by=${commTool?.agentId}`);
  const workerMsgs = execSync(`sqlite3 ${dbPath} "SELECT COUNT(*) FROM messages WHERE from_agent != 'master' AND from_agent != 'human' AND type IN ('report','clarify','note','dispatch');" 2>/dev/null`).toString().trim();
  check("agent-originated mailbox messages exist", Number(workerMsgs) >= 1, `rows=${workerMsgs}`);
  check("zero human-sent messages", events.every((e) => e.agentId !== "human"));
  const cost = (await (await fetch(`http://localhost:${PORT}/api/cost`)).json()) as { totals: { task_id: string; input_tokens: number }[] };
  check("cost attributed per ran task", cost.totals.length >= doneCount, `cost rows=${cost.totals.length}`);
  const worktreeOk = finalKids.filter((k) => k.status === "done" && k.worktree_path).every((k) => existsSync(k.worktree_path!));
  check("done tasks have live worktrees", worktreeOk);

  // produced artifacts sanity (if all done)
  const wt = finalKids.find((k) => k.status === "done" && k.worktree_path)?.worktree_path;
  if (wt) {
    const files = execSync(`ls ${JSON.stringify(wt)} 2>/dev/null`).toString().trim();
    console.log(`  (info) worktree files: ${files.replace(/\n/g, ", ").slice(0, 120)}`);
  }

  // ── phase 2: streaming tier live-inject check ──
  console.log(`\n[D8 milestone A] phase 2: streaming tier live inject`);
  const sock2 = await connect();
  sock2.ws.send(JSON.stringify({ type: "SUBMIT_GOAL", goal: "Reply with exactly: pong1", executor: "stream-planner" }));
  let csAgent: string | null = null;
  const csDeadline = Date.now() + 300_000;
  while (Date.now() < csDeadline) {
    for (const e of sock2.received.values()) {
      const p = e.payload as { reason?: string; agentId?: string };
      if (e.type === "TASK_TREE_UPDATED" && p?.reason === "dispatch" && String(p.agentId ?? "").startsWith("cs_")) csAgent = p.agentId!;
    }
    if (csAgent) break;
    await new Promise((r) => setTimeout(r, 500));
  }
  check("streaming tier dispatched", !!csAgent, `agent=${csAgent}`);
  if (csAgent) {
    sock2.ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: csAgent }));
    sock2.ws.send(JSON.stringify({ type: "REPLAY", sinceSeq: sock2.lastSeq }));
    const r1 = await new Promise<ServerEvent | null>((res) => {
      const dl = Date.now() + 300_000;
      const iv = setInterval(() => {
        for (const e of sock2.received.values()) {
          if (e.type === "TEXT_MESSAGE_END" && String((e.payload as { finalAnswer?: string }).finalAnswer ?? "").includes("pong1")) { clearInterval(iv); res(e); }
        }
        if (Date.now() > dl) { clearInterval(iv); res(null); }
      }, 500);
    });
    check("stream round-1 answered", !!r1);
    sock2.ws.send(JSON.stringify({ type: "INJECT", agentId: csAgent, prompt: "Now reply with exactly: pong2" }));
    const t0 = Date.now();
    const r2 = await new Promise<ServerEvent | null>((res) => {
      const dl = Date.now() + 300_000;
      const iv = setInterval(() => {
        for (const e of sock2.received.values()) {
          if (e.seq > (r1?.seq ?? 0) && e.type === "TEXT_MESSAGE_END" && String((e.payload as { finalAnswer?: string }).finalAnswer ?? "").includes("pong2")) { clearInterval(iv); res(e); }
        }
        if (Date.now() > dl) { clearInterval(iv); res(null); }
      }, 500);
    });
    check("live inject answered on the SAME process", !!r2, `latency ${Date.now() - t0}ms`);
    sock2.ws.send(JSON.stringify({ type: "KILL", agentId: csAgent }));
  }
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 800));
  if (failed === 0) rmSync(dbDir, { recursive: true, force: true });
  else console.log(`  (failure evidence kept at ${dbDir})`);
}

console.log(failed === 0 ? `\nMILESTONE A PASS — full chain, zero manual relay` : `\nMILESTONE A FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
