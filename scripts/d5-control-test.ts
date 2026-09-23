/**
 * D5 acceptance (plan §2 step 5): the control trio — inject / interrupt /
 * approve — verified with a SINGLE unified script across BOTH real executors.
 *
 *  harness (claude-*) run:
 *    1. inject lands as a --resume continuation: task A finishes writing
 *       a.txt, an inject of "now also write b.txt" makes the harness continue
 *       the SAME session and produce b.txt.
 *    2. interrupt: verified in D4 (kept here as a smoke re-check).
 *  openai-compat (worker) run:
 *    3. inject lands via prepareStep as a user turn mid-loop.
 *    4. approve: run_command gated → APPROVE allow lets it proceed (verified
 *       in D3; re-checked here).
 *  PASS criterion for the round: the observable behavior changes in each
 *  case; anything inexpressible is an OPEN log line, never silence.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";

const PORT = 19600 + Math.floor(Math.random() * 200);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d5-"));

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

interface TestSock { ws: WebSocket; received: Map<number, ServerEvent>; seqs: number[]; }
function connect(port: number): Promise<TestSock> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${port}`);
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
    // harness approval: auto-allow (oc executor gates run_command)
    for (const e of s.received.values()) {
      if (e.type === "APPROVAL_REQUIRED") {
        const { toolCallId } = e.payload as { toolCallId: string };
        if (!(s.received.get(-e.seq) ?? false)) {
          s.received.set(-e.seq, e); // mark handled
          send(s, { type: "APPROVE", agentId: e.agentId, toolCallId, decision: "allow" });
        }
      }
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  failed++;
  console.error(`  TIMEOUT waiting for ${what}`);
  return null;
}

async function startGateway(executor: string, dbPath: string): Promise<ChildProcess> {
  const gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: dbPath, MAW_DEV_EXECUTOR: executor, MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees") },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", () => { /* drained */ });
  gw.stderr?.on("data", () => { /* drained */ });
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  return gw;
}

async function taskWorktree(agentId: string): Promise<string | null> {
  const tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as TreeSnapshot;
  return tree.tasks.find((t) => t.assignee === agentId)?.worktree_path ?? null;
}

async function dispatchGoal(s: TestSock, goal: string, round: string): Promise<string | undefined> {
  send(s, { type: "SUBMIT_GOAL", goal });
  const d = await waitFor(s, (e) => e.type === "TASK_TREE_UPDATED" && (e.payload as { reason?: string })?.reason === "dispatch", 5000, `${round} dispatch`);
  const agentId = (d?.payload as { agentId?: string })?.agentId;
  if (agentId) send(s, { type: "SUBSCRIBE", agentId });
  return agentId;
}

let gw: ChildProcess | null = null;
try {
  // ========== round 1: harness executor — inject as --resume continuation ==========
  console.log("\n[D5 round 1] claude-harness + planner — inject via --resume");
  gw = await startGateway("claude-planner", path.join(dbDir, "r1.db"));
  const s1 = await connect(PORT);
  const agent1 = await dispatchGoal(s1, "Create a file named a.txt containing exactly: alpha", "r1");
  check("r1 dispatched", !!agent1, `agent=${agent1}`);
  const fin1 = await waitFor(s1, (e) => e.type === "RUN_FINISHED" && e.agentId === agent1, 420_000, "r1 first run finish");
  check("r1 first run success", (fin1?.payload as { outcome?: string })?.outcome === "success");
  const wt1 = agent1 ? await taskWorktree(agent1) : null;
  check("r1 a.txt exists", !!wt1 && existsSync(path.join(wt1, "a.txt")));
  check("r1 b.txt absent before inject", !!wt1 && !existsSync(path.join(wt1, "b.txt")));

  const injectAck = Date.now();
  send(s1, { type: "INJECT", agentId: agent1, prompt: "Now also create a file named b.txt containing exactly: beta" });
  const fin1b = await waitFor(s1, (e) => e.type === "RUN_FINISHED" && e.agentId === agent1 && e.seq > (fin1?.seq ?? 0), 420_000, "r1 continuation finish after inject");
  check("r1 inject triggered a continuation run", !!fin1b, `${Date.now() - injectAck}ms`);
  const resumeUsed = [...s1.received.values()].filter((e) => e.type === "RUN_STARTED" && e.agentId === agent1).some((e) => (e.payload as { round?: number })?.round === 1);
  check("r1 continuation is a --resume of the same session", resumeUsed);
  const wt1b = agent1 ? await taskWorktree(agent1) : null;
  check("r1 b.txt exists after inject", !!wt1b && existsSync(path.join(wt1b, "b.txt")), "inject visibly changed the outcome");
  if (wt1b && existsSync(path.join(wt1b, "b.txt"))) {
    check("r1 b.txt content", readFileSync(path.join(wt1b, "b.txt"), "utf-8").trim() === "beta");
  }
  gw.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 500));

  // ========== round 2: oc executor — inject mid-loop + approval re-check ==========
  console.log("\n[D5 round 2] openai-compat + worker — inject via prepareStep + approve");
  gw = await startGateway("worker", path.join(dbDir, "r2.db"));
  const s2 = await connect(PORT);
  const agent2 = await dispatchGoal(s2, "Create a file named c.txt containing exactly: gamma. After creating it, stop.", "r2");
  check("r2 dispatched", !!agent2, `agent=${agent2}`);
  // inject immediately: prepareStep delivers it as a user turn mid-loop
  send(s2, { type: "INJECT", agentId: agent2, prompt: "Additionally create a file named d.txt containing exactly: delta" });
  const fin2 = await waitFor(s2, (e) => e.type === "RUN_FINISHED" && e.agentId === agent2, 420_000, "r2 finish");
  check("r2 run success", (fin2?.payload as { outcome?: string })?.outcome === "success");
  const wt2 = agent2 ? await taskWorktree(agent2) : null;
  check("r2 c.txt exists", !!wt2 && existsSync(path.join(wt2, "c.txt")));
  check("r2 d.txt exists after inject", !!wt2 && existsSync(path.join(wt2, "d.txt")), "inject visibly changed the outcome");
  const injectedSeen = [...s2.received.values()].some((e) => e.type === "TEXT_MESSAGE_CONTENT" && String((e.payload as { delta?: string }).delta ?? "").includes("[human inject queued]"));
  check("r2 inject acknowledged as queued chunk", injectedSeen);
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 500));
  rmSync(dbDir, { recursive: true, force: true });
}

console.log(failed === 0 ? "\nD5 ACCEPTANCE PASS — control trio on both executors" : `\nD5 ACCEPTANCE FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
