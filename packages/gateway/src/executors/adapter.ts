/**
 * @maw/gateway/executors/adapter — the ONLY extension point (architecture
 * invariant 4). Landing of PLAN §6 pseudocode. Claude (D4) and
 * OpenAI-compat (D3) adapters implement this interface; adding OpenCode /
 * Aider / a raw-PTY fallback later means adding a file, nothing else.
 */

import type { AgentStatus, ApproveDecision, ServerEventType } from "@maw/shared";

/** An event produced by an executor, before it gets a `seq` from the hub. */
export interface AgentEvent {
  type: ServerEventType;
  payload: unknown;
  taskId?: string;
}

export interface AgentHandle {
  id: string;
  readonly events: AsyncIterable<AgentEvent>;
  /** Queued follow-up message; must never reorder mid-tool-call. */
  inject(prompt: string): Promise<void>;
  /** Must resolve <2 s or the state machine marks 'error' (plan §2 step 4). */
  interrupt(): Promise<void>;
  approve(toolCallId: string, decision: ApproveDecision, patch?: string): Promise<void>;
  kill(): Promise<void>;
  status(): AgentStatus;
}

export interface SpawnOpts {
  taskId?: string;
  /** Pre-assigned agent id — the dispatcher announces it before spawn. */
  agentId?: string;
  /** Working directory for the run (the task's git worktree). */
  worktreeDir?: string;
  /**
   * Revival (roadmap-1a): spawn with `--resume <sessionId>` so the process
   * comes back with its full prior conversation. In-place revival — the
   * agent id is unchanged, so UI panes/subscriptions keep working.
   */
  resumeSessionId?: string;
  /** Fires when the executor's init frame reports its claude session id —
   *  the gateway persists it to tasks.session_id for a future revival. */
  onSessionId?: (sessionId: string) => void;
  /**
   * Run-level completion callback — fires when a RUN finishes (success),
   * NOT when the handle dies (which may be an idle-window later for harness
   * executors). Task status and review must key off THIS, or a 30-minute
   * idle window would stall the whole pipeline.
   */
  onRunComplete?: (outcome: "success" | "interrupt" | "error", handle: unknown) => void;
}

export interface TaskSpecInput {
  title: string;
  spec: string;
}

export interface ExecutorAdapter {
  readonly kind: string;
  /** Throws ModelUnsupported BEFORE any process starts (plan §6). */
  spawn(spec: TaskSpecInput, opts: SpawnOpts): AgentHandle;
}
