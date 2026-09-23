/**
 * Shared test helper (D14 hardening): spawn a gateway for tests with
 * guaranteed process-tree cleanup — the zombie-gateway incident (9 stray
 * processes fighting over test DBs) must never recur.
 */
import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";

const ROOT = path.resolve(import.meta.dirname, "..");

export interface TestGateway {
  port: number;
  proc: ChildProcess;
  kill: () => Promise<void>;
}

export async function startTestGateway(opts: {
  port: number;
  dbPath: string;
  env?: Record<string, string>;
  worktreeRoot?: string;
}): Promise<TestGateway> {
  const proc = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(opts.port), MAW_DB_PATH: opts.dbPath, ...(opts.worktreeRoot ? { MAW_WORKTREE_ROOT: opts.worktreeRoot } : {}), ...opts.env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  proc.stdout?.on("data", () => { /* drained */ });
  proc.stderr?.on("data", () => { /* drained */ });
  // health poll
  for (let i = 0; i < 60; i++) {
    try { const r = await fetch(`http://localhost:${opts.port}/health`); if (r.ok) break; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  // the kill that matters: SIGKILL the whole process group (tsx children die too)
  async function kill(): Promise<void> {
    try { process.kill(-proc.pid!, "SIGKILL"); } catch { /* already gone */ }
    try { proc.kill("SIGKILL"); } catch { /* already gone */ }
    await new Promise((r) => setTimeout(r, 300));
  }
  return { port: opts.port, proc, kill };
}
