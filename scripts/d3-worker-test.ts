/**
 * D3 acceptance (plan §2 step 3) against a REAL model on the internal gateway:
 * submit "create hello.txt containing ok in the task worktree" to a
 * MAW_DEV_EXECUTOR=<tier> gateway; PASS when
 *  (a) the file exists inside the task's git worktree, and
 *  (b) exactly 1 cost row landed for the task (per-task attribution).
 * Also checks worktree isolation: the file must NOT appear in the repo root.
 * Usage: pnpm test:d3:planner | pnpm test:d3:worker
 */

import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";

const TIER = (process.argv[2] === "worker" ? "worker" : "planner") as "planner" | "worker";
const PORT = 19000 + Math.floor(Math.random() * 200);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d3-"));

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

interface TestSock { ws: WebSocket; received: Map<number, ServerEvent>; seqs: number[]; }

function connect(): Promise<TestSock> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${PORT}`);
    const s: TestSock = { ws, received: new Map(), seqs: [] };
    ws.on("open", () => resolve(s));
    ws.on("error", reject);
    ws.on("message", (data) => {
      const ev = JSON.parse(String(data)) as ServerEvent;
      if (!s.received.has(ev.seq)) { s.received.set(ev.seq, ev); s.seqs.push(ev.seq); }
    });
  });
}

async function waitFor(s: TestSock, pred: (e: ServerEvent) => boolean, ms: number, what: string): Promise<ServerEvent | null> {
  const deadline = Date.now() + ms;
  const approved = new Set<string>();
  while (Date.now() < deadline) {
    // the executor gates run_command on human approval — grant it (this also
    // exercises the APPROVE control path, part of the D5 criteria)
    for (const e of s.received.values()) {
      if (e.type === "APPROVAL_REQUIRED") {
        const { toolCallId } = e.payload as { toolCallId: string };
        if (!approved.has(toolCallId)) {
          approved.add(toolCallId);
          console.log(`  … auto-approving run_command (toolCallId=${toolCallId})`);
          s.ws.send(JSON.stringify({ type: "APPROVE", agentId: e.agentId, toolCallId, decision: "allow" }));
        }
      }
    }
    for (const e of [...s.received.values()].sort((a, b) => a.seq - b.seq)) if (pred(e)) return e;
    await new Promise((r) => setTimeout(r, 50));
  }
  failed++;
  console.error(`  TIMEOUT waiting for ${what}`);
  return null;
}

async function api(pathname: string): Promise<unknown> {
  const r = await fetch(`http://localhost:${PORT}${pathname}`);
  return r.json();
}

let gw: ChildProcess | null = null;
try {
  gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: path.join(dbDir, "d3.db"), MAW_DEV_EXECUTOR: TIER, MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees") },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", (d) => process.env.MAW_D3_VERBOSE && console.log(String(d).trim()));
  gw.stderr?.on("data", (d) => { /* keep drained */ });
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  console.log(`\n[D3 acceptance] tier=${TIER}`);

  const sock = await connect();
  send(sock, { type: "SUBMIT_GOAL", goal: "Create a file named hello.txt containing exactly the two characters: ok" });

  const dispatch = await waitFor(sock, (e) => e.type === "TASK_TREE_UPDATED" && (e.payload as { reason?: string })?.reason === "dispatch", 5000, "dispatch");
  const agentId = (dispatch?.payload as { agentId?: string })?.agentId;
  check("dispatched to real executor", !!agentId, `agent=${agentId}`);
  if (agentId) send(sock, { type: "SUBSCRIBE", agentId });
  send(sock, { type: "REPLAY", sinceSeq: 0 });

  const fin = await waitFor(sock, (e) => e.type === "RUN_FINISHED", 420_000, "RUN_FINISHED (real model; planner thinking is slow)");
  const outcome = (fin?.payload as { outcome?: string })?.outcome;
  check("worker finished", outcome === "success", `outcome=${outcome}`);

  const tree = (await api("/api/tree")) as TreeSnapshot;
  const task = tree.tasks.find((t) => t.assignee === agentId);
  check("task row exists with worktree", !!task?.worktree_path, task?.worktree_path ?? "(none)");
  check("task status is done", task?.status === "done", `status=${task?.status}`);

  if (task?.worktree_path) {
    const file = path.join(task.worktree_path, "hello.txt");
    check("hello.txt exists in worktree", existsSync(file));
    if (existsSync(file)) {
      const content = readFileSync(file, "utf-8").trim();
      check("content is 'ok'", content === "ok", `content=${JSON.stringify(content.slice(0, 40))}`);
    }
    check("isolation: not written into repo root", !existsSync(path.join(ROOT, "hello.txt")));
    check("isolation: worktree is under MAW_WORKTREE_ROOT", task.worktree_path.startsWith(path.join(dbDir, "worktrees")));
  }

  const cost = (await api("/api/cost")) as { totals: { task_id: string; model: string; input_tokens: number; output_tokens: number }[] };
  const row = cost.totals.find((c) => c.task_id === task?.id);
  check("exactly 1 cost row for the task", cost.totals.length === 1, `rows=${cost.totals.length}`);
  check("cost row has real token counts", !!row && row.input_tokens > 0 && row.output_tokens > 0, `in=${row?.input_tokens} out=${row?.output_tokens} model=${row?.model}`);

  const events = [...sock.received.values()];
  check("streamed text arrived live", events.some((e) => e.type === "TEXT_MESSAGE_CONTENT" && e.agentId === agentId));
  const approvals = events.filter((e) => e.type === "APPROVAL_REQUIRED");
  check("approval gate only fires for run_command",
    approvals.every((e) => (e.payload as { name?: string }).name === "run_command"),
    `approvals=${approvals.length}`);
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 300));
  rmSync(dbDir, { recursive: true, force: true });
}

function send(s: TestSock, msg: unknown): void {
  s.ws.send(JSON.stringify(msg));
}

console.log(failed === 0 ? `\nD3 ACCEPTANCE PASS (${TIER})` : `\nD3 ACCEPTANCE FAIL (${TIER}) — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
