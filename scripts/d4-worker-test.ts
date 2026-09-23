/**
 * D4 acceptance (plan §2 step 4, route B): the Claude Code HARNESS as worker
 * executor, model = Planner (or DSV4) via the internal gateway.
 *  run 1 — "create hello.txt containing ok": PASS when the file exists in the
 *          worktree, harness tool calls streamed as TOOL_CALL_* events, and
 *          1 cost row landed (modelUsage attribution).
 *  run 2 — interrupt: submit a slow goal, INTERRUPT after RUN_STARTED; the
 *          run must end with outcome='interrupt' within 5 s (SIGINT receipt).
 * Usage: pnpm test:d4:planner | pnpm test:d4:worker
 */

import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";

const TIER = (process.argv[2] === "worker" ? "worker" : "planner") as "planner" | "worker";
const PORT = 19200 + Math.floor(Math.random() * 200);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d4-"));

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
function send(s: TestSock, msg: unknown): void { s.ws.send(JSON.stringify(msg)); }

async function waitFor(s: TestSock, pred: (e: ServerEvent) => boolean, ms: number, what: string): Promise<ServerEvent | null> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    for (const e of [...s.received.values()].sort((a, b) => a.seq - b.seq)) if (pred(e)) return e;
    await new Promise((r) => setTimeout(r, 50));
  }
  failed++;
  console.error(`  TIMEOUT waiting for ${what}`);
  return null;
}

let gw: ChildProcess | null = null;
try {
  gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: path.join(dbDir, "d4.db"), MAW_DEV_EXECUTOR: `claude-${TIER}`, MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees") },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", () => { /* drained */ });
  gw.stderr?.on("data", () => { /* drained */ });
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  console.log(`\n[D4 acceptance] claude-harness + ${TIER} (harness tools, swapped model)`);

  // ---------- run 1: file task through the harness ----------
  const sock = await connect();
  send(sock, { type: "SUBMIT_GOAL", goal: "Create a file named hello.txt containing exactly the two characters: ok" });
  const d1 = await waitFor(sock, (e) => e.type === "TASK_TREE_UPDATED" && (e.payload as { reason?: string })?.reason === "dispatch", 5000, "dispatch");
  const agentId = (d1?.payload as { agentId?: string })?.agentId;
  check("dispatched to harness executor", !!agentId, `agent=${agentId}`);
  if (agentId) send(sock, { type: "SUBSCRIBE", agentId });
  send(sock, { type: "REPLAY", sinceSeq: 0 });

  const started = await waitFor(sock, (e) => e.type === "RUN_STARTED" && e.agentId === agentId, 30_000, "harness RUN_STARTED (init frame)");
  check("harness init arrived", !!started, `model=${(started?.payload as { model?: string })?.model}`);

  const fin = await waitFor(sock, (e) => e.type === "RUN_FINISHED" && e.agentId === agentId, 420_000, "RUN_FINISHED (harness run)");
  check("harness run finished", (fin?.payload as { outcome?: string })?.outcome === "success");

  const events = [...sock.received.values()].filter((e) => e.agentId === agentId);
  check("harness tool calls streamed", events.some((e) => e.type === "TOOL_CALL_START" && (e.payload as { name?: string }).name === "Write"));
  check("assistant text streamed", events.some((e) => e.type === "TEXT_MESSAGE_CONTENT"));

  const tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
  const task = tree.tasks.find((t) => t.assignee === agentId);
  check("task done with worktree", task?.status === "done" && !!task.worktree_path);
  if (task?.worktree_path) {
    const file = path.join(task.worktree_path, "hello.txt");
    check("hello.txt exists in worktree", existsSync(file));
    if (existsSync(file)) check("content is 'ok'", readFileSync(file, "utf-8").trim() === "ok");
    check("isolation: repo root clean", !existsSync(path.join(ROOT, "hello.txt")));
  }
  const cost = (await (await fetch(`http://localhost:${PORT}/api/cost`)).json()) as { totals: { input_tokens: number; output_tokens: number; model: string }[] };
  const row = cost.totals.find((c) => c.model.includes(TIER === "planner" ? "GLM" : "DeepSeek") || c.model !== "unknown");
  check("cost row attributed", !!row && row.input_tokens > 0, `in=${row?.input_tokens} out=${row?.output_tokens} model=${row?.model}`);

  // ---------- run 2: interrupt timeliness ----------
  send(sock, { type: "SUBMIT_GOAL", goal: "Count from 1 to 20000 slowly, writing each number on its own line into count.txt, then summarize." });
  const d2 = await waitFor(sock, (e) => e.type === "TASK_TREE_UPDATED" && (e.payload as { reason?: string })?.reason === "dispatch" && (e.payload as { agentId?: string }).agentId !== agentId, 5000, "run-2 dispatch");
  const agent2 = (d2?.payload as { agentId?: string })?.agentId;
  if (agent2) { send(sock, { type: "SUBSCRIBE", agentId: agent2 }); }
  const started2 = await waitFor(sock, (e) => e.type === "RUN_STARTED" && e.agentId === agent2, 30_000, "run-2 RUN_STARTED");
  check("run-2 started", !!started2);
  const t0 = Date.now();
  send(sock, { type: "INTERRUPT", agentId: agent2! });
  const fin2 = await waitFor(sock, (e) => e.type === "RUN_FINISHED" && e.agentId === agent2 && (e.payload as { outcome?: string })?.outcome === "interrupt", 10_000, "run-2 RUN_FINISHED interrupt");
  const dt = Date.now() - t0;
  check("SIGINT interrupt ends harness run within 5 s", !!fin2 && dt <= 5000, `${dt}ms`);
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 400));
  rmSync(dbDir, { recursive: true, force: true });
}

console.log(failed === 0 ? `\nD4 ACCEPTANCE PASS (claude-harness + ${TIER})` : `\nD4 ACCEPTANCE FAIL (${TIER}) — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
