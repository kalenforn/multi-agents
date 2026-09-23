/**
 * @maw/gateway/executors/claude-harness — D4 (route B, verified live):
 * the Claude Code HARNESS as a worker execution environment, with the MODEL
 * swapped to Planner / DeepSeek V4-Pro via the internal gateway
 * (ANTHROPIC_BASE_URL/_AUTH_TOKEN/_MODEL — same trick as the user's `cc`
 * zshrc function). Zero Claude-model spend; what we buy is the harness:
 * built-in file tools, search, auto-compaction, and stream-json control.
 *
 * Grounded in the 2026-09-10 headless probe:
 *  - claude -p --output-format stream-json --verbose emits: system/init
 *    (with capabilities incl. interrupt_receipt_v1), assistant messages
 *    (text / tool_use blocks), user messages (tool_result), result (usage)
 *  - interrupt = SIGINT (receipt capability declared in init)
 *  - inject = queued, delivered as a --resume <session_id> continuation run
 *    (streaming-input mode is the v2 upgrade; one-shot resume is D4 scope)
 *  - approval: harness tools are whitelist-gated via --allowedTools; Bash is
 *    NOT whitelisted so the model cannot shell out (PreToolUse-hook approval
 *    is the D5+ upgrade path). File tools run inside the task worktree (cwd).
 *  - cost: parsed from result.usage / modelUsage → cost_ledger row
 */

import { spawn as nodeSpawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { createInterface } from "node:readline";
import type { ApproveDecision } from "@maw/shared";
import { COMMS_PROTOCOL_PROMPT } from "./comms.js";
import { config } from "../config.js";
import type { AgentEvent, AgentHandle, ExecutorAdapter, SpawnOpts, TaskSpecInput } from "./adapter.js";

const CLAUDE_BIN = process.env.CLAUDE_BIN ?? "claude";
const MAX_TURNS = config.harnessMaxTurns;
const RUN_TIMEOUT_MS = config.harnessRunTimeoutMs; // hard ceiling per continuation run
const ALLOWED_TOOLS = "Read Write Edit Glob Grep NotebookEdit"; // file ops only — no Bash by design
const IDLE_WINDOW_MS = config.idleWindowMs; // post-run window: the workbench is interactive by default — an agent stays injectable after finishing (resume), until the window lapses

export class ClaudeHarnessAdapter implements ExecutorAdapter {
  readonly kind = "claude-harness";

  constructor(private readonly tier: "planner" | "worker") {}

  spawn(spec: TaskSpecInput, opts: SpawnOpts): AgentHandle {
    return new ClaudeHarnessHandle(this.tier, spec, opts);
  }
}

interface HarnessEnv {
  ANTHROPIC_BASE_URL: string;
  ANTHROPIC_AUTH_TOKEN: string;
  ANTHROPIC_MODEL: string;
}

/** Resolve a tier's provider env. Neutral names (PLANNER_ and WORKER_ prefixed)
 *  win; legacy GLM_ and DSV4_ prefixed names stay as fallbacks so existing .env
 *  files keep working. Model identities live ONLY in the environment. */
function harnessEnv(tier: "planner" | "worker"): HarnessEnv {
  const prefixes = tier === "planner" ? ["PLANNER", "GLM"] : ["WORKER", "DSV4"];
  const pick = (name: string): string | undefined => {
    for (const p of prefixes) {
      const v = process.env[`${p}_${name}`];
      if (v && v.trim()) return v.trim();
    }
    return undefined;
  };
  const apiKey = pick("API_KEY");
  const baseUrl = pick("BASE_URL");
  const model = pick("MODEL");
  if (!apiKey || !baseUrl || !model) {
    throw new Error(`ModelUnsupported: set ${prefixes[0]}_{API_KEY,BASE_URL,MODEL} in .env for tier '${tier}' (${prefixes[1]}_* legacy names also work)`);
  }
  // openai-compatible base is https://host/v1 — the Anthropic protocol on the
  // same gateway lives at the bare host (verified: /v1/messages is appended)
  const anthropicBase = baseUrl.replace(/\/v1\/?$/, "");
  return { ANTHROPIC_BASE_URL: anthropicBase, ANTHROPIC_AUTH_TOKEN: apiKey, ANTHROPIC_MODEL: model };
}

type AgentStatusValue = "thinking" | "working" | "awaiting_approval" | "idle" | "error";

interface StreamJsonLine {
  type: string;
  subtype?: string;
  session_id?: string;
  model?: string;
  message?: { content?: Array<Record<string, unknown>>; model?: string };
  usage?: Record<string, unknown>;
  modelUsage?: Record<string, { inputTokens?: number; outputTokens?: number; cacheReadInputTokens?: number }>;
  total_cost_usd?: number;
  is_error?: boolean;
  result?: string;
  /** present only with --include-partial-messages: raw provider deltas */
  event?: {
    type: string;
    delta?: { type?: string; text?: string; thinking?: string };
    content_block?: { type?: string; text?: string };
  };
}

class ClaudeHarnessHandle implements AgentHandle {
  readonly id: string;
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private finished = false;
  private state: AgentStatusValue = "thinking";
  private proc: ChildProcess | null = null;
  private injected: string[] = [];
  private readonly taskId?: string;
  private readonly spec: TaskSpecInput;
  private onRunComplete?: (outcome: "success" | "interrupt" | "error", handle: unknown) => void;
  private turnUser = ""; // the prompt that started the current run (for TURN_COMPLETED)
  private sessionId: string | null = null;
  private started = false;
  private runTimer: NodeJS.Timeout | null = null;
  private streamSeq = 0;
  private runCount = 0;
  private idleResolve: (() => void) | null = null;
  private runOutcome: "success" | "error" | "unset" = "unset";
  private interruptRequested = false;
  /** Final aggregated usage — read by the dispatch layer for cost_ledger. */
  finalUsage?: { input: number; output: number; cacheRead: number; model: string };
  private readonly cwd: string;

  constructor(private readonly tier: "planner" | "worker", spec: TaskSpecInput, opts: SpawnOpts) {
    this.id = opts.agentId ?? `ch_${Date.now().toString(36)}`;
    this.taskId = opts.taskId;
    this.spec = spec;
    this.cwd = opts.worktreeDir ?? config.workspaceDir; // NEVER process.cwd() — the gateway's own repo is not the agent's workspace (snake-game bug)
    // revival (roadmap-1a): seed the session from the persisted id so the
    // first continuation run resumes the prior conversation; also report it
    // up so the gateway can keep tasks.session_id fresh
    this.sessionId = opts.resumeSessionId ?? null;
    this.opts = opts;
    // THE MISSING LINE: without this assignment onRunComplete stayed
    // undefined forever — every completed harness run silently skipped
    // cost accounting AND review, stranding tasks in 'running' (the whole
    // D8 r2–r4 stall, finally cornered)
    this.onRunComplete = opts.onRunComplete;
    void this.loop();
  }
  private readonly opts: SpawnOpts;

  private push(ev: AgentEvent): void {
    if (this.taskId) ev.taskId = this.taskId;
    const w = this.waiters.shift();
    if (w) w({ value: ev, done: false });
    else this.queue.push(ev);
  }

  private finish(outcome: "success" | "interrupt" | "error", detail?: string): void {
    if (this.finished) return;
    this.finished = true;
    this.idleResolve?.(); // wake the idle window so the loop can exit
    if (this.runTimer) clearTimeout(this.runTimer);
    this.proc?.kill("SIGTERM");
    // non-success terminations (timeout/spawn-fail/interrupt-escalation) must
    // still reach the dispatcher — success already fired onRunComplete in the
    // close handler; without this, error paths left tasks in 'running' forever
    // (D8 r5: run timeout → RUN_ERROR visible but task stuck running)
    if (outcome !== "success") this.onRunComplete?.(outcome, this);
    for (const w of this.waiters.splice(0)) w({ value: undefined, done: true });
  }

  status(): AgentStatusValue {
    return this.state;
  }

  readonly events: AsyncIterable<AgentEvent> = this.gen();

  private async *gen(): AsyncGenerator<AgentEvent> {
    while (true) {
      const next = this.queue.shift();
      if (next) { yield next; continue; }
      if (this.finished) return;
      const r = await new Promise<IteratorResult<AgentEvent>>((res) => this.waiters.push(res));
      if (r.done) return;
      yield r.value;
    }
  }

  async inject(prompt: string): Promise<void> {
    // delivered as a --resume continuation once the current run completes (or
    // immediately, if the handle is sitting in its idle window)
    this.injected.push(prompt);
    this.idleResolve?.(); // wake the idle window → resume run
    this.push({ type: "TEXT_MESSAGE_CONTENT", payload: { messageId: `${this.id}-inj`, delta: `[human inject queued → resume] ${prompt}` } });
  }

  async interrupt(): Promise<void> {
    if (this.finished || this.interruptRequested) return;
    this.interruptRequested = true;
    this.push({ type: "RUN_FINISHED", payload: { outcome: "interrupt", detail: "SIGINT by human" } });
    // headless claude exits gracefully on SIGINT (verified: close code=0
    // ~1 s after signal); the close handler then ends the handle. Escalation:
    this.proc?.kill("SIGINT");
    setTimeout(() => {
      if (!this.finished) this.finish("interrupt", "SIGINT escalation");
    }, 2_000);
  }

  async approve(): Promise<void> {
    // no pending-approval flow in D4 (tools are whitelist-gated, no Bash);
    // PreToolUse-hook approval is the D5+ upgrade path
  }

  private idleWait(ms: number): Promise<void> {
    return new Promise((res) => {
      const timer = setTimeout(() => { this.idleResolve = null; res(); }, ms);
      this.idleResolve = () => { clearTimeout(timer); this.idleResolve = null; res(); };
    });
  }

  async kill(): Promise<void> {
    // hard release: SIGKILL the child immediately — the human asked to free it,
    // not to wait for a graceful stop
    if (this.proc && !this.finished) {
      try { this.proc.kill("SIGKILL"); } catch { /* already dead */ }
    }
    this.push({ type: "RUN_FINISHED", payload: { outcome: "error", detail: "killed by human" } });
    this.finish("error", "killed by human");
  }

  // ---------- run loop (one -p invocation per continuation) ----------

  private async loop(): Promise<void> {
    if (this.started) return;
    this.started = true;
    let env: HarnessEnv;
    try {
      env = harnessEnv(this.tier);
    } catch (err) {
      this.push({ type: "RUN_ERROR", payload: { message: err instanceof Error ? err.message : String(err) } });
      this.finish("error", "missing provider config");
      return;
    }
    let round = 0;
    while (!this.finished) {
      this.runCount++;
      const nextInject = this.injected.shift();
      const prompt = round === 0 ? this.spec.spec : `[human steering] ${nextInject}`;
      this.turnUser = round === 0 ? this.spec.spec : nextInject ?? "(steering)";
      if (round > 0 && nextInject) {
        // visible user turn: without this the UI never shows what the human
        // steered — the resume run looked like it started for no reason
        this.push({ type: "TEXT_MESSAGE_CONTENT", payload: { messageId: `${this.id}-u${round}`, delta: `\n👤 [your prompt] ${nextInject}\n` } });
      }
      const ok = await this.runOnce(env, prompt, round);
      if (!ok) return; // finish() already emitted
      if (this.injected.length === 0) {
        // idle window: the process has exited (zero resources); the SESSION
        // stays resumable — an inject inside the window wakes a --resume run,
        // after the window the handle truly ends.
        this.state = "idle";
        console.log(`[harness ${this.id}] entering idle window (${IDLE_WINDOW_MS}ms), injected=${this.injected.length}`);
        this.push({ type: "AGENT_STATUS", payload: { status: "idle", detail: "idle window — injectable" } });
        await this.idleWait(IDLE_WINDOW_MS);
        console.log(`[harness ${this.id}] idle window over, injected=${this.injected.length}, finished=${this.finished}`);
        if (this.finished) return;
        if (this.injected.length === 0) {
          this.finish("success");
          return;
        }
      }
      round++;
    }
  }

  private runOnce(env: HarnessEnv, prompt: string, round: number): Promise<boolean> {
    return new Promise((resolveRun) => {
      this.runOutcome = "unset";
      const args = [
        "-p", `${COMMS_PROTOCOL_PROMPT}\n\n${prompt}`,
        "--output-format", "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--max-turns", String(MAX_TURNS),
        "--allowedTools", `${ALLOWED_TOOLS} mcp__maw-comms__send_message mcp__maw-comms__check_inbox mcp__maw-comms__spawn_agent mcp__maw-comms__list_agents`,
        "--disallowedTools", "SendMessage ListAgents Task Stop TaskOutput",
        "--mcp-config", JSON.stringify({
          mcpServers: {
            "maw-comms": {
              command: process.execPath,
              args: [path.join(import.meta.dirname ?? ".", "..", "..", "mcp", "comms.mjs")],
              env: { MAW_AGENT_ID: this.id, MAW_GATEWAY_URL: `http://127.0.0.1:${process.env.PORT ?? 8787}` },
            },
          },
        }),
      ];
      if (this.sessionId) args.push("--resume", this.sessionId);
      const proc = nodeSpawn(CLAUDE_BIN, args, {
        cwd: this.cwd,
        env: {
          ...process.env,
          ...env,
          CLAUDE_CODE_MAX_CONTEXT_TOKENS: String(1_000_000),
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
      this.proc = proc;
      if (round === 0) this.state = "working";

      this.runTimer = setTimeout(() => {
        if (!this.finished) {
          this.push({ type: "RUN_ERROR", payload: { message: `run timeout after ${RUN_TIMEOUT_MS / 60000} min` } });
          this.finish("error", "timeout");
          resolveRun(false);
        }
      }, RUN_TIMEOUT_MS);

      const rl = createInterface({ input: proc.stdout });
      rl.on("line", (line) => this.onLine(line));
      proc.stderr?.on("data", (d) => {
        const s = String(d);
        if (s.includes("unrecognized_model")) return; // known noise with swapped models
        if (s.trim()) console.error(`[claude-harness ${this.id}] ${s.trim().slice(0, 300)}`);
      });
      proc.on("error", (e) => {
        if (this.finished) { resolveRun(false); return; }
        this.push({ type: "RUN_ERROR", payload: { message: `spawn failed: ${e.message}` } });
        this.finish("error", "spawn failed");
        resolveRun(false);
      });
      proc.on("close", (code, signal) => {
        if (this.runTimer) clearTimeout(this.runTimer);
        this.runTimer = null;
        this.proc = null;
        if (this.finished) { resolveRun(false); return; }
        if (this.interruptRequested || signal === "SIGINT" || signal === "SIGTERM") {
          // interrupt() already pushed the run-level RUN_FINISHED; if the
          // signal path got here first (e.g. KILL escalation), emit it now
          if (!this.interruptRequested) {
            this.push({ type: "RUN_FINISHED", payload: { outcome: "interrupt", detail: `signal ${signal}` } });
          }
          this.finish("interrupt", signal ? `signal ${signal}` : "SIGINT graceful exit");
          resolveRun(false);
          return;
        }
        if (this.runOutcome === "unset" && code !== 0) {
          this.push({ type: "RUN_ERROR", payload: { message: `claude exited with code ${code}` } });
          this.push({ type: "RUN_FINISHED", payload: { outcome: "error", detail: `exit ${code}` } });
          this.finish("error", `exit ${code}`);
          resolveRun(false);
          return;
        }
        // run-level finish: the RUN completed; the handle may live on in its
        // idle window for inject→resume, so this is NOT the handle finish.
        // The dispatcher keys task status off this callback — waiting for the
        // handle's finish would stall the pipeline by the idle-window length.
        this.push({ type: "RUN_FINISHED", payload: { outcome: "success", detail: `run ${this.runCount} complete` } });
        this.onRunComplete?.("success", this);
        resolveRun(true);
      });
    });
  }

  private onLine(line: string): void {
    let ev: StreamJsonLine;
    try {
      ev = JSON.parse(line) as StreamJsonLine;
    } catch {
      return; // harness banner lines are not JSON
    }
    switch (ev.type) {
      case "system": {
        if (ev.subtype === "init") {
          if (ev.session_id) { this.sessionId = ev.session_id; this.opts.onSessionId?.(ev.session_id); }
          this.push({ type: "RUN_STARTED", payload: { model: ev.model ?? ev.message?.model ?? "unknown", tier: this.tier, round: this.runCount } });
        }
        // hook_started / hook_response / compact → ignored (noise for the UI)
        break;
      }
      case "stream_event": {
        const ev2 = ev.event;
        if (!ev2) break;
        if (ev2.type === "content_block_delta" && ev2.delta?.type === "text_delta" && ev2.delta.text) {
          this.push({ type: "TEXT_MESSAGE_CONTENT", payload: { messageId: `${this.id}-s${this.streamSeq++}`, delta: ev2.delta.text } });
        }
        // thinking_delta 留给 UI 的"思考中"指示器（v2 显示思考流）；不作为正文
        break;
      }
      case "assistant": {
        // with --include-partial-messages the text already streamed token-by-
        // token via stream_event; pushing the full assistant block AGAIN
        // tripled every reply in the UI. Only tool_use blocks are handled here.
        const blocks = ev.message?.content ?? [];
        for (const b of blocks) {
          if (b.type === "tool_use") {
            this.push({
              type: "TOOL_CALL_START",
              payload: { toolCallId: String(b.id), name: String(b.name), argsPreview: JSON.stringify(b.input ?? {}).slice(0, 200) },
            });
          }
        }
        break;
      }
      case "user": {
        // tool_result blocks come back as user-role messages in stream-json
        const blocks = ev.message?.content ?? [];
        if (!Array.isArray(blocks)) break;
        for (const b of blocks) {
          if (typeof b === "object" && b !== null && "tool_use_id" in b) {
            this.push({
              type: "TOOL_CALL_RESULT",
              payload: {
                toolCallId: String((b as { tool_use_id: string }).tool_use_id),
                ok: !(b as { is_error?: boolean }).is_error,
                resultPreview: (typeof (b as { content?: unknown }).content === "string" ? (b as { content: string }).content : JSON.stringify((b as { content?: unknown }).content ?? "")).slice(0, 200),
              },
            });
          }
        }
        break;
      }
      case "result": {
        // usage + model attribution from the final result frame
        const usage = (ev.usage ?? {}) as { input_tokens?: number; output_tokens?: number };
        const modelKey = Object.keys(ev.modelUsage ?? {})[0];
        const mu = modelKey ? ev.modelUsage?.[modelKey] : undefined;
        this.finalUsage = {
          input: mu?.inputTokens ?? usage.input_tokens ?? 0,
          output: mu?.outputTokens ?? usage.output_tokens ?? 0,
          cacheRead: mu?.cacheReadInputTokens ?? 0,
          model: modelKey ?? "unknown",
        };
        this.push({
          type: "STATE_SNAPSHOT",
          payload: {
            kind: "usage-final",
            input_tokens: this.finalUsage.input,
            output_tokens: this.finalUsage.output,
            model: this.finalUsage.model,
          },
        });
        // conversation turn: user prompt + the model's complete reply — the
        // UI renders turns from THIS event (harness tier previously never
        // emitted it, so prompts vanished from rescue/task agents)
        this.push({
          type: "TURN_COMPLETED",
          payload: { userText: this.turnUser, agentText: ev.result ?? "", round: this.runCount },
        });
        if (typeof ev.result === "string" && ev.result.length > 0) {
          this.push({ type: "TEXT_MESSAGE_END", payload: { finalAnswer: ev.result.slice(0, 2000) } });
        }
        if (this.interruptRequested) {
          // harness marks an interrupted run as is_error in its result frame;
          // the human asked for it — report 'interrupt', not 'error'
          this.runOutcome = "success";
        } else if (ev.is_error) {
          this.runOutcome = "error";
          this.push({ type: "RUN_ERROR", payload: { message: "harness reported an error result" } });
          this.push({ type: "RUN_FINISHED", payload: { outcome: "error", detail: "harness error result" } });
          this.finish("error", "harness error result");
        } else {
          this.runOutcome = "success";
        }
        break;
      }
      default:
        break;
    }
  }
}
