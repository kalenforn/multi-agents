/**
 * D2 acceptance (plan §2 step 2): 2 sockets against a real gateway process.
 *  - round 1: both sockets subscribe (SUBSCRIBE + REPLAY sinceSeq, the
 *    exactly-once app-layer pattern) and receive the fake run's events
 *  - round 2: socket B is killed; the new run's events reach A only
 *  - B reconnects with REPLAY sinceSeq → receives exactly the missed events,
 *    zero duplicates (counted, not eyeballed)
 *  - round 3: INJECT lands a human-steered chunk; INTERRUPT ends the run
 *    within 2 s with outcome='interrupt'
 * Exit 0 = pass.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { WebSocket } from "ws";
import type { ServerEvent } from "@maw/shared";

const PORT = 18800 + Math.floor(Math.random() * 200);
const ROOT = path.resolve(import.meta.dirname, "..");
const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d2-"));
const dbPath = path.join(dbDir, "d2.db");

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

interface TestSock {
  ws: WebSocket;
  /** App-layer view: seq → event. Live + REPLAY overlap is deduped here. */
  received: Map<number, ServerEvent>;
  dupCount: number;
  seqs: number[];
}

function connect(): Promise<TestSock> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${PORT}`);
    const s: TestSock = { ws, received: new Map(), dupCount: 0, seqs: [] };
    ws.on("open", () => resolve(s));
    ws.on("error", reject);
    ws.on("message", (data) => {
      const ev = JSON.parse(String(data)) as ServerEvent;
      if (s.received.has(ev.seq)) s.dupCount += 1;
      else { s.received.set(ev.seq, ev); s.seqs.push(ev.seq); }
    });
  });
}

function send(s: TestSock, msg: unknown): void {
  s.ws.send(JSON.stringify(msg));
}

function close(s: TestSock): Promise<void> {
  return new Promise((res) => { s.ws.on("close", () => res()); s.ws.close(); });
}

async function waitFor(s: TestSock, pred: (e: ServerEvent) => boolean, timeoutMs: number, what: string): Promise<ServerEvent | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    for (const e of [...s.received.values()].sort((a, b) => a.seq - b.seq)) if (pred(e)) return e;
    await new Promise((r) => setTimeout(r, 25));
  }
  failed++;
  console.error(`  TIMEOUT waiting for ${what}`);
  return null;
}

interface DispatchInfo { agentId: string; seq: number; }

async function waitDispatch(s: TestSock, afterSeq: number, round: string): Promise<DispatchInfo | null> {
  const ev = await waitFor(
    s,
    (e) =>
      e.type === "TASK_TREE_UPDATED" &&
      e.seq > afterSeq &&
      (e.payload as { reason?: string })?.reason === "dispatch",
    5000,
    `${round} dispatch announcement`
  );
  if (!ev) return null;
  return { agentId: (ev.payload as { agentId: string }).agentId, seq: ev.seq };
}

/** The frontend exactly-once pattern: subscribe, then replay the gap. */
function subscribeTo(s: TestSock, agentId: string): void {
  send(s, { type: "SUBSCRIBE", agentId });
  send(s, { type: "REPLAY", sinceSeq: lastSeqOf(s) });
}

function lastSeqOf(s: TestSock): number {
  return s.seqs.length ? Math.max(...s.seqs) : 0;
}

async function healthUp(): Promise<boolean> {
  for (let i = 0; i < 60; i++) {
    try {
      const r = await fetch(`http://localhost:${PORT}/health`);
      if (r.ok) return true;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

function assertContiguous(s: TestSock, label: string): void {
  const sorted = [...s.seqs].sort((x, y) => x - y);
  const noGap = sorted.every((v, i) => i === 0 || v === sorted[i - 1]! + 1);
  check(`${label} stream contiguous from seq 1, no gap`,
    sorted.length > 0 && sorted[0] === 1 && noGap,
    `seqs 1..${sorted[sorted.length - 1] ?? "?"} (${sorted.length} events)`);
}

let gw: ChildProcess | null = null;
try {
  gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: dbPath, MAW_FAKE_DECOMPOSE: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  gw.stdout?.on("data", () => { /* keep pipe drained */ });
  gw.stderr?.on("data", () => { /* keep pipe drained */ });

  check("gateway boots", await healthUp());

  // ---- round 1: both sockets follow one fake run ----
  const A = await connect();
  const B = await connect();
  send(A, { type: "SUBMIT_GOAL", goal: "round-1" });

  const d1a = await waitDispatch(A, 0, "round-1");
  check("dispatch announces agentId before first agent event", !!d1a, `agent=${d1a?.agentId}`);
  if (!d1a) throw new Error("cannot continue without dispatch");
  subscribeTo(A, d1a.agentId);
  subscribeTo(B, d1a.agentId);

  const fin1a = await waitFor(A, (e) => e.type === "RUN_FINISHED" && e.agentId === d1a.agentId, 5000, "round-1 RUN_FINISHED on A");
  const fin1b = await waitFor(B, (e) => e.type === "RUN_FINISHED" && e.agentId === d1a.agentId, 5000, "round-1 RUN_FINISHED on B");
  check("both sockets saw the run finish", !!fin1a && !!fin1b);
  const run1 = [...A.received.values()].filter((e) => e.agentId === d1a.agentId);
  const core1 = run1.filter((e) => e.type !== "AGENT_STATUS");
  const status1 = run1.filter((e) => e.type === "AGENT_STATUS");
  check("fake run emitted exactly 5 core events + 1 lifecycle status",
    core1.length === 5 && status1.length === 1,
    `core=${core1.length} (RUN_STARTED + 3 TEXT + RUN_FINISHED), status=${status1.length}`);
  assertContiguous(A, "A round-1");
  assertContiguous(B, "B round-1");

  const lastSeqB = lastSeqOf(B);
  const lastSeqA = lastSeqOf(A);
  check("A and B agree on lastSeq", lastSeqA === lastSeqB, `A=${lastSeqA} B=${lastSeqB}`);
  await close(B);

  // ---- round 2: B is gone; only A receives ----
  const baselineA = lastSeqA;
  send(A, { type: "SUBMIT_GOAL", goal: "round-2" });
  const d2 = await waitDispatch(A, baselineA, "round-2");
  check("round-2 dispatches a fresh agent", !!d2 && d2.agentId !== d1a.agentId, `agent=${d2?.agentId}`);
  if (d2) subscribeTo(A, d2.agentId);
  const fin2 = await waitFor(A, (e) => e.type === "RUN_FINISHED" && e.agentId === d2?.agentId, 5000, "round-2 RUN_FINISHED on A");
  check("A received round 2", !!fin2);

  // ---- B reconnects and replays the gap ----
  const B2 = await connect();
  send(B2, { type: "REPLAY", sinceSeq: lastSeqB });
  const fin2b = await waitFor(B2, (e) => e.type === "RUN_FINISHED" && e.seq > lastSeqB, 5000, "round-2 RUN_FINISHED on B via replay");
  check("replay delivers the missed round-2 run", !!fin2b);
  const missed = A.seqs.filter((s) => s > lastSeqB).sort((x, y) => x - y);
  const replayed = B2.seqs.filter((s) => s > lastSeqB).sort((x, y) => x - y);
  check(
    "replayed set == missed set, each exactly once",
    replayed.length === missed.length && replayed.every((v, i) => v === missed[i]) && B2.dupCount === 0,
    `missed=${missed.length} replayed=${replayed.length} dups=${B2.dupCount}`
  );

  // ---- round 3: control plane — inject + interrupt ----
  const baseline3 = lastSeqOf(A);
  send(A, { type: "SUBMIT_GOAL", goal: "round-3" });
  const d3 = await waitDispatch(A, baseline3, "round-3");
  check("round-3 dispatches", !!d3, `agent=${d3?.agentId}`);
  if (!d3) throw new Error("cannot continue");
  subscribeTo(A, d3.agentId);
  const started3 = await waitFor(A, (e) => e.type === "RUN_STARTED" && e.agentId === d3.agentId, 5000, "round-3 RUN_STARTED");
  check("round-3 run started", !!started3);

  const t0 = Date.now();
  send(A, { type: "INJECT", agentId: d3.agentId, prompt: "steer!" });
  send(A, { type: "INTERRUPT", agentId: d3.agentId });
  const fin3 = await waitFor(
    A,
    (e) => e.type === "RUN_FINISHED" && e.agentId === d3.agentId && (e.payload as { outcome?: string })?.outcome === "interrupt",
    5000,
    "round-3 RUN_FINISHED outcome=interrupt"
  );
  const dt = Date.now() - t0;
  check("interrupt ends run within 2 s", !!fin3 && dt <= 2000, `${dt}ms`);

  const injectSeen = [...A.received.values()].some(
    (e) =>
      e.agentId === d3.agentId &&
      e.type === "TEXT_MESSAGE_CONTENT" &&
      typeof (e.payload as { delta?: string })?.delta === "string" &&
      (e.payload as { delta: string }).delta.includes("[human inject]")
  );
  check("inject landed as a streamed chunk before interrupt", injectSeen);

  assertContiguous(A, "A final");
} finally {
  gw?.kill("SIGTERM");
  await new Promise((r) => setTimeout(r, 300));
  rmSync(dbDir, { recursive: true, force: true });
}

console.log(failed === 0 ? "\nD2 ACCEPTANCE PASS — replay + control plane verified" : `\nD2 ACCEPTANCE FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
