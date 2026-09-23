/**
 * @maw/gateway/executors/claude-stream — D8: streaming input mode harness.
 *
 * The Claude Code process stays ALIVE with stdin open; every INJECT goes
 * straight into the stream (millisecond response, real conversation) instead
 * of the resume mode's per-turn process respawn. This is the executor the
 * interactive workbench actually wants:
 *
 *   claude --input-format stream-json --output-format stream-json ...
 *     stdin  ← {"type":"user","message":{...}} frames (we send them live)
 *     stdout → same NDJSON event stream as the resume executor
 *
 * interrupt = SIGINT (graceful turn abort, then the process keeps serving);
 * kill = SIGKILL. Comms tools ride the same MCP server as resume mode.
 */

import { spawn as nodeSpawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import path from "node:path";
import type { ApproveDecision } from "@maw/shared";
import type { AgentEvent, AgentHandle, ExecutorAdapter, SpawnOpts, TaskSpecInput } from "./adapter.js";
import { COMMS_PROTOCOL_PROMPT } from "./comms.js";
import { config } from "../config.js";

const CLAUDE_BIN = process.env.CLAUDE_BIN ?? "claude";
const MAX_TURNS = config.streamMaxTurns;
const ALLOWED_TOOLS = "Read Write Edit Glob Grep NotebookEdit mcp__maw-comms__send_message mcp__maw-comms__check_inbox mcp__maw-comms__spawn_agent mcp__maw-comms__list_agents";

interface HarnessEnv {
  ANTHROPIC_BASE_URL: string;
  ANTHROPIC_AUTH_TOKEN: string;
  ANTHROPIC_MODEL: string;
}

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
    throw new Error(`ModelUnsupported: set ${prefixes[0]}_{API_KEY,BASE_URL,MODEL} in .env for tier '${tier}' (legacy ${prefixes[1]}_* names also work)`);
  }
  return { ANTHROPIC_BASE_URL: baseUrl.replace(/\/v1\/?$/, ""), ANTHROPIC_AUTH_TOKEN: apiKey, ANTHROPIC_MODEL: model };
}

type AgentStatusValue = "thinking" | "working" | "awaiting_approval" | "idle" | "error";

interface StreamJsonLine {
  type: string;
  subtype?: string;
  session_id?: string;
  model?: string;
  message?: { content?: Array<Record<string, unknown>>; model?: string };
  event?: { type: string; delta?: { type?: string; text?: string } };
  usage?: Record<string, unknown>;
  modelUsage?: Record<string, { inputTokens?: number; outputTokens?: number; cacheReadInputTokens?: number; cacheCreationInputTokens?: number }>;
  is_error?: boolean;
  result?: string;
}

export class ClaudeStreamAdapter implements ExecutorAdapter {
  readonly kind = "claude-stream";

  constructor(private readonly tier: "planner" | "worker") {}

  spawn(spec: TaskSpecInput, opts: SpawnOpts): AgentHandle {
    return new ClaudeStreamHandle(this.tier, spec, opts);
  }
}

class ClaudeStreamHandle implements AgentHandle {
  readonly id: string;
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private finished = false;
  private state: AgentStatusValue = "thinking";
  private proc: ChildProcess | null = null;
  private readonly taskId?: string;
  private readonly spec: TaskSpecInput;
  private readonly cwd: string;
  private onRunComplete?: (outcome: "success" | "interrupt" | "error", handle: unknown) => void;
  private started = false;
  private streamSeq = 0;
  private turnCount = 0;
  private turnUser = "";
  private turnAgent = "";
  private lastAssistantBlock = ""; // dedupe: stream deltas vs complete blocks
  private usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, model: "unknown" };
  /** Cumulative usage already reported via onRunComplete — the delta prevents double-counting cost rows. */
  private lastReported = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
  /** Final aggregated usage — read by the dispatch layer for cost_ledger. One onRunComplete = one row, so this is the UNREPORTED delta of the turn that just completed, never the session total. */
  finalUsage?: { input: number; output: number; cacheRead: number; cacheWrite: number; model: string };

  constructor(tier: "planner" | "worker", spec: TaskSpecInput, opts: SpawnOpts) {
    this.id = opts.agentId ?? `cs_${Date.now().toString(36)}`;
    this.taskId = opts.taskId;
    this.opts = opts;
    this.onRunComplete = opts.onRunComplete;
    this.spec = spec;
    this.cwd = opts.worktreeDir ?? config.workspaceDir; // NEVER process.cwd() — the gateway's own repo is not the agent's workspace (snake-game bug)
    void this.run(tier);
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
    this.proc?.kill("SIGTERM");
    // error/interrupt paths must not write an inflated row either: report the
    // unreported delta, not the cumulative session total (clamped ≥ 0 in case
    // counters reset on a fresh --resume session)
    this.finalUsage = this.unreportedDelta();
    this.push({ type: "RUN_FINISHED", payload: { outcome, detail } });
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

  /** INJECT = a user frame straight into the live stdin — no respawn. */
  async inject(prompt: string): Promise<void> {
    if (this.finished || !this.proc?.stdin) {
      this.push({ type: "RUN_ERROR", payload: { message: "stream is closed" } });
      return;
    }
    this.turnCount += 1;
    this.state = "working";
    this.turnUser = prompt; // user bubble comes from TURN_COMPLETED (no raw echo needed)
    this.proc.stdin.write(JSON.stringify({ type: "user", message: { role: "user", content: `[human steering] ${prompt}` } }) + "\n");
  }

  async interrupt(): Promise<void> {
    // graceful turn abort; the process stays alive to serve the next inject
    this.proc?.kill("SIGINT");
    this.push({ type: "AGENT_STATUS", payload: { status: "idle", detail: "turn interrupted by human — stream still live" } });
  }

  async approve(): Promise<void> {
    // streaming mode inherits harness whitelist gating (no Bash); the
    // PreToolUse-hook approval upgrade applies to both harness executors
  }

  async kill(): Promise<void> {
    if (this.proc && !this.finished) {
      try { this.proc.kill("SIGKILL"); } catch { /* already dead */ }
    }
    this.push({ type: "RUN_FINISHED", payload: { outcome: "error", detail: "killed by human" } });
    this.finish("error", "killed by human");
  }

  private async run(tier: "planner" | "worker"): Promise<void> {
    if (this.started) return;
    this.started = true;
    let env: HarnessEnv;
    try {
      env = harnessEnv(tier);
    } catch (err) {
      this.push({ type: "RUN_ERROR", payload: { message: err instanceof Error ? err.message : String(err) } });
      this.finish("error", "missing provider config");
      return;
    }
    const args = [
      "--input-format", "stream-json",
      "--output-format", "stream-json",
      "--verbose",
      "--include-partial-messages",
      "--max-turns", String(MAX_TURNS),
      ...(this.opts.resumeSessionId ? ["--resume", this.opts.resumeSessionId] : []),
      "--allowedTools", ALLOWED_TOOLS,
      "--disallowedTools", "SendMessage ListAgents Task Stop TaskOutput WebSearch WebFetch",
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
    const proc = nodeSpawn(CLAUDE_BIN, args, {
      cwd: this.cwd,
      env: { ...process.env, ...env, CLAUDE_CODE_MAX_CONTEXT_TOKENS: String(1_000_000) },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.proc = proc;
    this.state = "working";

    const rl = createInterface({ input: proc.stdout });
    rl.on("line", (line) => this.onLine(line));
    proc.stderr?.on("data", (d) => {
      const s = String(d);
      if (s.includes("unrecognized_model")) return;
      if (s.trim()) console.error(`[claude-stream ${this.id}] ${s.trim().slice(0, 200)}`);
    });
    proc.on("error", (e) => {
      if (!this.finished) {
        this.push({ type: "RUN_ERROR", payload: { message: `spawn failed: ${e.message}` } });
        this.finish("error", "spawn failed");
      }
    });
    proc.on("close", (code) => {
      if (!this.finished) {
        // stdin EOF + process exit = the conversation ended (resume-mode KILL
        // escalation lands here too)
        this.finish(code === 0 ? "success" : "error", `stream closed (exit ${code})`);
      }
    });

    // first user turn: the task itself
    this.turnCount += 1;
    this.turnUser = this.spec.spec;
    proc.stdin.write(
      JSON.stringify({
        type: "user",
        message: { role: "user", content: `${COMMS_PROTOCOL_PROMPT}\n\n# Task\n${this.spec.spec}` },
      }) + "\n"
    );
  }

  /** Usage since the last onRunComplete fire (clamped ≥ 0 — counters may reset on a fresh --resume session). */
  private unreportedDelta(): { input: number; output: number; cacheRead: number; cacheWrite: number; model: string } {
    return {
      input: Math.max(0, this.usage.input - this.lastReported.input),
      output: Math.max(0, this.usage.output - this.lastReported.output),
      cacheRead: Math.max(0, this.usage.cacheRead - this.lastReported.cacheRead),
      cacheWrite: Math.max(0, this.usage.cacheWrite - this.lastReported.cacheWrite),
      model: this.usage.model,
    };
  }

  private onLine(line: string): void {
    let ev: StreamJsonLine;
    try {
      ev = JSON.parse(line) as StreamJsonLine;
    } catch {
      return;
    }
    switch (ev.type) {
      case "system": {
        if (ev.subtype === "init") {
          const model = ev.model ?? "unknown";
          this.usage.model = model;
          if (ev.session_id) this.opts.onSessionId?.(ev.session_id);
          this.push({ type: "RUN_STARTED", payload: { model, tier: "stream", round: this.turnCount } });
        }
        break;
      }
      case "stream_event": {
        const d = ev.event;
        if (d?.type === "content_block_delta" && d.delta?.type === "text_delta" && d.delta.text) {
          this.push({ type: "TEXT_MESSAGE_CONTENT", payload: { messageId: `${this.id}-s${this.streamSeq++}`, delta: d.delta.text } });
        }
        break;
      }
      case "assistant": {
        // complete assistant blocks are the AUTHORITATIVE text (deduped vs the
        // partial-delta display stream); tool_use blocks emit tool events
        for (const b of ev.message?.content ?? []) {
          if (b.type === "text" && typeof b.text === "string" && b.text !== this.lastAssistantBlock) {
            this.lastAssistantBlock = b.text;
            this.turnAgent = b.text;
          } else if (b.type === "tool_use") {
            this.push({ type: "TOOL_CALL_START", payload: { toolCallId: String(b.id), name: String(b.name), argsPreview: JSON.stringify(b.input ?? {}).slice(0, 200) } });
          }
        }
        break;
      }
      case "user": {
        for (const b of (ev.message?.content ?? []) as Array<Record<string, unknown>>) {
          if ("tool_use_id" in b) {
            this.push({ type: "TOOL_CALL_RESULT", payload: { toolCallId: String(b.tool_use_id), ok: !b.is_error, resultPreview: String(b.content ?? "").slice(0, 200) } });
          }
        }
        break;
      }
      case "result": {
        // per-turn result in streaming mode: accumulate usage, show final text
        const mu = Object.entries(ev.modelUsage ?? {})[0]?.[1];
        this.usage.input += mu?.inputTokens ?? Number(ev.usage?.input_tokens ?? 0);
        this.usage.output += mu?.outputTokens ?? Number(ev.usage?.output_tokens ?? 0);
        this.usage.cacheRead = (this.usage.cacheRead ?? 0) + (mu?.cacheReadInputTokens ?? 0);
        this.usage.cacheWrite = (this.usage.cacheWrite ?? 0) + (mu?.cacheCreationInputTokens ?? 0);
        // each onRunComplete fires exactly one cost_ledger insert, so report the
        // per-turn delta — a cumulative finalUsage would double-count turn over turn
        const delta = this.unreportedDelta();
        this.lastReported = { input: this.usage.input, output: this.usage.output, cacheRead: this.usage.cacheRead, cacheWrite: this.usage.cacheWrite };
        this.finalUsage = delta;
        this.push({
          type: "STATE_SNAPSHOT",
          payload: { kind: "usage-final", input_tokens: delta.input, output_tokens: delta.output, cache_read: delta.cacheRead, cache_write: delta.cacheWrite, model: delta.model },
        });
        if (typeof ev.result === "string" && ev.result.length > 0) {
          this.push({ type: "TEXT_MESSAGE_END", payload: { finalAnswer: ev.result.slice(0, 2000) } });
        }
        this.state = "idle";
        this.push({
          type: "TURN_COMPLETED",
          payload: { userText: this.turnUser, agentText: this.turnAgent, round: this.turnCount },
        });
        this.turnUser = "";
        this.turnAgent = "";
        this.lastAssistantBlock = "";
        this.push({ type: "AGENT_STATUS", payload: { status: "idle", detail: `turn ${this.turnCount} complete — stream live, injectable` } });
        // fire on EVERY turn: the dispatch layer's onRunComplete callback is the
        // only place cost rows are written — first-turn-only would drop turns 2+
        // (interactive injects) from cost_ledger. Callers guard their own one-shot
        // side effects (dispatchOne's `reviewed` flag; setStatus is idempotent).
        this.onRunComplete?.(ev.is_error ? "error" : "success", this);
        break;
      }
      default:
        break;
    }
  }
}
