/**
 * D7 acceptance (plan §2 step 7): the review loop closes the orchestration
 * chain. One goal, workers run, the Master reviews each completion against
 * the task spec + worktree diff:
 *  - pass → done + dependents unlocked
 *  - fail → feedback re-dispatch (attempts < 2) → next attempt's worker sees
 *    the reviewer feedback in its spec
 *  - budget exhausted → awaiting_approval (human queue), never silent
 * PASS criteria: all settled, >=1 review happened (events prove it), zero
 * human-sent messages, mailbox reports exist for every worker completion.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { execSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";

const PORT = 19950 + Math.floor(Math.random() * 40);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d7-"));

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
function autoApprove(s: TestSock): void {
  for (const e of s.received.values()) {
    const dispatched = e.type === "TASK_TREE_UPDATED" ? (e.payload as { reason?: string; agentId?: string }) : null;
    if (dispatched?.reason === "dispatch" && dispatched.agentId && !subscribed.has(dispatched.agentId)) {
      subscribed.add(dispatched.agentId);
      s.ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: dispatched.agentId }));
      s.ws.send(JSON.stringify({ type: "REPLAY", sinceSeq: s.lastSeq }));
    }
    if (e.type === "APPROVAL_REQUIRED") {
      const { toolCallId } = e.payload as { toolCallId: string };
      if (!approved.has(toolCallId)) {
        approved.add(toolCallId);
        console.log(`  … auto-approving ${e.agentId} ${toolCallId}`);
        s.ws.send(JSON.stringify({ type: "APPROVE", agentId: e.agentId, toolCallId, decision: "allow" }));
      }
    }
  }
}

// 这个目标故意带一个"容易做不完整"的任务（README 要求两个章节），
// 给审查循环制造真实的 pass/fail 信号面
const GOAL = `Create a tiny python project: math_utils.py with add(a,b) and multiply(a,b); and a README.md that MUST contain two sections: "## Install" with pip instructions and "## Usage" with a code example.`;

let gw: ChildProcess | null = null;
try {
  gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: path.join(dbDir, "d7.db"), MAW_ORCHESTRATE: "1", MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees") },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", () => { /* drained */ });
  gw.stderr?.on("data", () => { /* drained */ });
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }

  const sock = await connect();
  sock.ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: "master" }));
  console.log(`\n[D7 acceptance] goal: "${GOAL.slice(0, 50)}…"`);

  const submitRes = await (await fetch(`http://localhost:${PORT}/api/tasks`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ goal: GOAL }),
  })).json();
  const rootId = submitRes.taskId as string;
  check("goal accepted (orchestrated)", !!rootId);

  // 拆解窗口
  const plantDeadline = Date.now() + 420_000;
  let tree: TreeSnapshot | null = null;
  while (Date.now() < plantDeadline) {
    autoApprove(sock);
    tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
    if ((tree?.tasks.filter((t) => t.parent_id === rootId).length ?? 0) >= 2) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  const children = tree?.tasks.filter((t) => t.parent_id === rootId) ?? [];
  check("master planted subtasks", children.length >= 2, `planted=${children.length}`);

  // settle 窗口（含审查轮时间：每任务 run + review + 可能 re-dispatch）
  const finishDeadline = Date.now() + 1500_000;
  let lastPrint = "";
  let sawReviewEvent = false;
  let sawRedispatch = false;
  while (Date.now() < finishDeadline) {
    autoApprove(sock);
    const snap = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
    const kids = snap.tasks.filter((t) => t.parent_id === rootId);
    const summary = kids.map((k) => k.status).join(",");
    if (summary !== lastPrint) { console.log(`  … subtask statuses: ${summary}`); lastPrint = summary; }
    for (const e of sock.received.values()) {
      const p = e.type === "TASK_TREE_UPDATED" ? (e.payload as { reason?: string; verdict?: string }) : null;
      if (p?.reason === "review") sawReviewEvent = true;
      if (p?.reason === "review" && p.verdict === "fail") sawRedispatch = true;
    }
    const settled = kids.every((k) => ["done", "failed", "cancelled", "awaiting_approval"].includes(k.status));
    if (settled && kids.length > 0) { tree = snap; break; }
    await new Promise((r) => setTimeout(r, 3000));
  }
  tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
  const finalKids = tree.tasks.filter((t) => t.parent_id === rootId);

  check("review events observed (review loop actually ran)", sawReviewEvent, `reviews seen via TASK_TREE_UPDATED(reason=review)`);
  check("all subtasks settled", finalKids.every((k) => ["done", "failed", "cancelled", "awaiting_approval"].includes(k.status)), finalKids.map((k) => k.status).join(","));
  const doneCount = finalKids.filter((k) => k.status === "done").length;
  check("majority done", doneCount >= Math.ceil(finalKids.length * 0.6), `done=${doneCount}/${finalKids.length}`);
  if (sawRedispatch) console.log(`  (info) reviewer issued at least one fail verdict → re-dispatch happened (review has teeth)`);
  check("zero human-sent messages", [...sock.received.values()].every((e) => e.agentId !== "human"));

  // mailbox 证据：每个完成的 worker 都有 report
  const reports = execSync(`sqlite3 ${path.join(dbDir, "d7.db")} "SELECT COUNT(*) FROM messages WHERE type='report';" 2>/dev/null`).toString().trim();
  check("mailbox reports exist for worker completions", Number(reports) >= doneCount, `report messages=${reports} (done tasks=${doneCount})`);

  // verdict 分布（awaiting_approval 出现也合法：那是 2 轮后的升级，不是失败）
  const escalated = finalKids.filter((k) => k.status === "awaiting_approval");
  if (escalated.length > 0) console.log(`  (info) ${escalated.length} task(s) escalated to human queue after re-dispatch budget — correct D7 behavior`);
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 500));
  if (failed === 0) rmSync(dbDir, { recursive: true, force: true });
  else console.log(`  (failure evidence kept at ${dbDir})`);
}

console.log(failed === 0 ? `\nD7 ACCEPTANCE PASS — review loop closes the chain` : `\nD7 ACCEPTANCE FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
