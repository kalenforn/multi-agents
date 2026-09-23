/**
 * @maw/gateway/config — typed loader for config.json.
 *
 * Precedence: MAW_* env var > config.json > hardcoded default. Secrets never
 * live here (they stay in .env); this file holds the tunable operating
 * parameters only. Security whitelists/denylists (tool allowlists, cmd
 * denylists, recipient whitelist) are deliberately NOT configurable — they
 * are R-series hard constraints, not ops knobs.
 */

import { readFileSync, writeFileSync } from "node:fs";
import { execSync } from "node:child_process";
import path from "node:path";

interface ConfigFile {
  server: { port: number };
  paths: { projectDir: string | null; db: string; worktreeRoot: string };
  orchestrator: {
    maxParallelWorkers: number;
    maxRedispatch: number;
    execTimeoutMs: number;
    spawnCap: number;
    taskTokenCap: number;
    globalTokenCap: number;
  };
  comms: { maxMsgChars: number; maxNoteChars: number };
  executors: {
    claudeStream: { maxTurns: number };
    claudeHarness: { maxTurns: number; runTimeoutMs: number; idleWindowMs: number };
    lite: { maxSteps: number; cmdTimeoutMs: number; modelTimeoutMs: number };
  };
  dev: { orchestrate: boolean; fakeDecompose: boolean; devExecutor: string | null };
}

const num = (envVal: string | undefined, fileVal: number): number => {
  const n = Number(envVal);
  return envVal !== undefined && Number.isFinite(n) ? n : fileVal;
};
const bool = (envVal: string | undefined, fileVal: boolean): boolean =>
  envVal === undefined ? fileVal : envVal === "1";

function load(): ConfigFile {
  try {
    return JSON.parse(
      readFileSync(path.join(import.meta.dirname ?? ".", "..", "config.json"), "utf8"),
    ) as ConfigFile;
  } catch (e) {
    throw new Error(
      `[config] config.json unreadable: ${e instanceof Error ? e.message : e} — every gateway knob depends on it`,
    );
  }
}

const file = load();

/** The project this workbench instance serves — roadmap-4 isolation: one
 *  workbench per project directory, everything it produces lands under
 *  <projectDir>/.multi-agent/. Resolution: MAW_PROJECT_DIR env > the
 *  .current-project pointer (written by POST /api/project/open — the app's
 *  "Open Project" action) > the git repo root of the gateway's cwd.
 *  rev-parse is trusted here: it is the operator's own environment, not
 *  model/user input (R1 trust boundary). */
const POINTER_FILE = path.join(import.meta.dirname ?? ".", ".current-project");

function resolveProjectDir(): string {
  const fromEnv = process.env.MAW_PROJECT_DIR;
  if (fromEnv && fromEnv.trim()) return path.resolve(fromEnv.trim());
  try {
    const pointer = readFileSync(POINTER_FILE, "utf8").trim();
    if (pointer) return path.resolve(pointer);
  } catch { /* no pointer yet — fall through */ }
  if (file.paths.projectDir) return path.resolve(file.paths.projectDir);
  try {
    return execSync("git rev-parse --show-toplevel", { encoding: "utf8" }).trim();
  } catch {
    return process.cwd(); // not a repo — run in place, .multi-agent still scopes the artifacts
  }
}

/** The "Open Project" action persists its target here then restarts the
 *  gateway; the next boot resolves it as the active project. */
export function setCurrentProjectPointer(dir: string): void {
  writeFileSync(POINTER_FILE, dir + "\n");
}

const initialProjectDir = resolveProjectDir();

/** Mutable runtime project context (roadmap-4b hot swap): Open Project
 *  rebinds the SAME gateway process to another directory in ~0ms. Callers
 *  read through these getters — module-level `const X = config.x` snapshots
 *  would freeze the old project and are forbidden. */
const projectCtx = {
  dir: initialProjectDir,
  get projectDir() { return this.dir; },
  get workspaceDir() { return path.join(this.dir, ".multi-agent"); },
  get dbPath() {
    // legacy env pins win — a pinned DB never moves with the project
    return process.env.MAW_DB_PATH ?? path.resolve(this.dir, file.paths.db);
  },
  get worktreeRoot() {
    return process.env.MAW_WORKTREE_ROOT ?? path.resolve(this.dir, file.paths.worktreeRoot);
  },
  /** the one mutation path: /api/project/open (validated upstream) */
  rebind(dir: string): void { this.dir = dir; },
};

export const config = {
  get projectDir() { return projectCtx.projectDir; },
  get workspaceDir() { return projectCtx.workspaceDir; },
  get dbPath() { return projectCtx.dbPath; },
  get worktreeRoot() { return projectCtx.worktreeRoot; },
  /** the hot-swap entry point, exported for the /open handler */
  switchProject(dir: string): void { projectCtx.rebind(dir); },
  port: num(process.env.PORT, file.server.port),
  fakeDecompose: bool(process.env.MAW_FAKE_DECOMPOSE, file.dev.fakeDecompose),
  orchestrate: bool(process.env.MAW_ORCHESTRATE, file.dev.orchestrate),
  devExecutor: process.env.MAW_DEV_EXECUTOR ?? file.dev.devExecutor,
  maxParallelWorkers: num(process.env.MAW_MAX_PARALLEL, file.orchestrator.maxParallelWorkers),
  maxRedispatch: num(process.env.MAW_MAX_REDISPATCH, file.orchestrator.maxRedispatch),
  execTimeoutMs: num(process.env.MAW_EXEC_TIMEOUT_MS, file.orchestrator.execTimeoutMs),
  spawnCap: num(process.env.MAW_SPAWN_CAP, file.orchestrator.spawnCap),
  taskTokenCap: num(process.env.MAW_TASK_TOKEN_CAP, file.orchestrator.taskTokenCap),
  globalTokenCap: num(process.env.MAW_GLOBAL_TOKEN_CAP, file.orchestrator.globalTokenCap),
  maxMsgChars: num(process.env.MAW_MAX_MSG_CHARS, file.comms.maxMsgChars),
  maxNoteChars: num(process.env.MAW_MAX_NOTE_CHARS, file.comms.maxNoteChars),
  streamMaxTurns: num(process.env.MAW_STREAM_MAX_TURNS, file.executors.claudeStream.maxTurns),
  harnessMaxTurns: num(process.env.MAW_HARNESS_MAX_TURNS, file.executors.claudeHarness.maxTurns),
  harnessRunTimeoutMs: num(process.env.MAW_RUN_TIMEOUT_MS, file.executors.claudeHarness.runTimeoutMs),
  idleWindowMs: num(process.env.MAW_IDLE_WINDOW_MS, file.executors.claudeHarness.idleWindowMs),
  liteMaxSteps: num(process.env.MAW_LITE_MAX_STEPS, file.executors.lite.maxSteps),
  liteCmdTimeoutMs: num(process.env.MAW_LITE_CMD_TIMEOUT_MS, file.executors.lite.cmdTimeoutMs),
  modelTimeoutMs: num(process.env.MAW_MODEL_TIMEOUT_MS, file.executors.lite.modelTimeoutMs),
} as const;
