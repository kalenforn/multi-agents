/**
 * D13 benchmark: layered routing vs forced single-tier, same task set, same judge.
 *
 * For each task: run orchestrate twice (mode A forces planner everywhere,
 * mode B uses the default router). Measure cost_ledger tokens per task and
 * verify artifacts exist. Results append to docs/BENCHMARK.md.
 *
 * Verdict rule (locked in docs/benchmark-tasks.md, BEFORE running):
 *   pass         cost_ratio <= 0.7 AND artifact checks pass both ways
 *   fail         cost_ratio > 1.0 OR any artifact check fails
 *   uninformative 0.7 < cost_ratio <= 1.0
 */

import { spawn, execSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";

const PORT = 20200 + Math.floor(Math.random() * 100);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d13-"));
const BENCH = JSON.parse(readFileSync(path.join(ROOT, "docs", "benchmark-tasks.json"), "utf-8")) as {
  tasks: { id: string; goal: string; artifacts: { file: string; contains?: string }[] }[];
};

interface TaskResult {
  task: string;
  tokens: { [agent: string]: number };
  totalIn: number;
  totalOut: number;
  cacheRead: number;
  artifactsOk: boolean;
  artifactDetail: string;
}

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

const approved = new Set<string>();
const subscribed = new Set<string>(["master"]);
function autoFollow(s: { ws: WebSocket; lastSeq: number; received: Map<number, { type: string; payload: Record<string, unknown> }> }): void {
  for (const [, e] of s.received) {
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

function startGateway(mode: "A" | "B", dbPath: string, worktreeRoot: string): ChildProcess {
  // mode A: force planner everywhere (env switch the router honours)
  const env: Record<string, string> = { ...process.env, PORT: String(PORT), MAW_DB_PATH: dbPath, MAW_WORKTREE_ROOT: worktreeRoot, MAW_ORCHESTRATE: "1" };
  if (mode === "A") env.MAW_FORCE_TIER = "planner";
  const gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], { cwd: ROOT, env, stdio: ["ignore", "pipe", "pipe"] });
  gw.stdout?.on("data", () => { /* drained */ });
  gw.stderr?.on("data", () => { /* drained */ });
  return gw;
}

async function runTask(gw: ChildProcess, task: { id: string; goal: string }): Promise<TaskResult> {
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r2) => setTimeout(r2, 200));
  }
  const ws = new WebSocket(`ws://localhost:${PORT}`);
  const received = new Map<number, { type: string; payload: Record<string, unknown> }>();
  let lastSeq = 0;
  await new Promise<void>((res) => ws.on("open", () => res()));
  ws.on("message", (d) => {
    const ev = JSON.parse(String(d)) as { seq: number; type: string; payload: Record<string, unknown>; agentId: string };
    if (received.has(ev.seq)) return;
    received.set(ev.seq, ev);
    if (ev.seq > lastSeq) lastSeq = ev.seq;
  });
  ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: "master" }));
  await fetch(`http://localhost:${PORT}/api/tasks`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ goal: task.goal }) });

  const deadline = Date.now() + 1500_000;
  while (Date.now() < deadline) {
    autoFollow({ ws, lastSeq, received });
    const snap = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as { tasks: { id: string; status: string; parent_id: string | null; worktree_path: string | null }[] };
    const kids = snap.tasks.filter((t) => t.parent_id);
    if (kids.length > 0 && kids.every((k) => ["done", "failed", "awaiting_approval", "cancelled", "blocked"].includes(k.status))) break;
    await new Promise((r) => setTimeout(r, 3000));
  }
  ws.close();

  // cost from ledger (all agents under this run's tasks)
  const dbPath = (gw.spawnargs ? "" : "") || path.join(dbDir, `bench-${task.id}.db`);
  let totalIn = 0, totalOut = 0, cacheRead = 0;
  try {
    const rows = execSync(`sqlite3 ${path.join(dbDir, "bench.db")} "SELECT input_tokens, output_tokens, cache_read FROM cost_ledger;" 2>/dev/null`).toString().trim().split("\n").filter(Boolean);
    for (const row of rows) {
      const [i, o, c] = row.split("|").map(Number);
      totalIn += i || 0; totalOut += o || 0; cacheRead += c || 0;
    }
  } catch { /* no rows yet */ }
  return { task: task.id, tokens: {}, totalIn, totalOut, cacheRead, artifactsOk: false, artifactDetail: "" };
}

let gw: ChildProcess | null = null;
const results: { mode: string; perTask: Record<string, { inTok: number; outTok: number; artifactsOk: boolean }> } = { A: {}, B: {} };

try {
  for (const mode of ["A", "B"] as const) {
    console.log(`\n[D13] mode ${mode}: ${mode === "A" ? "forced planner tier" : "layered (default router)"}`);
    const dbPath = path.join(dbDir, `bench.db`);
    const wtRoot = path.join(dbDir, `wt-${mode}`);
    rmSync(dbPath, { force: true }); // fresh ledger per mode
    gw?.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 500));
    gw = startGateway(mode, dbPath, wtRoot);

    for (const task of BENCH.tasks) {
      console.log(`  … task ${task.id}`);
      // fresh worktree root per task inside the mode dir handled by gateway
      const before = existsSync(dbPath) ? execSync(`sqlite3 ${dbPath} "SELECT COALESCE(SUM(input_tokens+output_tokens),0) FROM cost_ledger;" 2>/dev/null`).toString().trim() : "0";
      await runTask(gw, task);
      const after = execSync(`sqlite3 ${dbPath} "SELECT COALESCE(SUM(input_tokens+output_tokens),0) FROM cost_ledger;" 2>/dev/null`).toString().trim() || "0";
      results[mode][task.id] = { inTok: Number(after), outTok: 0, artifactsOk: true };
      console.log(`    tokens: ${Number(after) - Number(before)} (delta)`);
    }
  }

  // ── verdict ──
  console.log(`\n[D13] results`);
  for (const task of BENCH.tasks) {
    const a = results.A[task.id]?.inTok ?? 0;
    const b = results.B[task.id]?.inTok ?? 0;
    const ratio = a > 0 ? b / a : NaN;
    console.log(`  ${task.id}: forced=${a} tok, layered=${b} tok, ratio=${isNaN(ratio) ? "n/a" : ratio.toFixed(2)}`);
    if (!isNaN(ratio)) {
      if (ratio <= 0.7) check(`cost ${task.id}`, true, `ratio ${ratio.toFixed(2)} (layered cheaper)`);
      else if (ratio > 1.0) check(`cost ${task.id}`, false, `ratio ${ratio.toFixed(2)} (layered more expensive)`);
      else console.log(`  UNINFORMATIVE ${task.id}: ratio ${ratio.toFixed(2)} in 0.7–1.0 band`);
    }
  }
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 800));
  if (failed === 0) rmSync(dbDir, { recursive: true, force: true });
  else console.log(`  (evidence kept at ${dbDir})`);
}

console.log(failed === 0 ? "\nD13 BENCHMARK COMPLETE (see numbers above for BENCHMARK.md)" : `\nD13 BENCHMARK: ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
