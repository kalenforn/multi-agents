/**
 * @maw/shared — domain types
 * Single source of truth for task/agent/mailbox/cost shapes.
 * Dependency direction: web → shared ← gateway. The two leaf packages never
 * import each other; they speak the WS/HTTP protocol defined in events.ts.
 */

export type TaskStatus =
  | "pending" // created, waiting for decomposition/dispatch
  | "queued" // ready to dispatch, waiting for a free executor
  | "running" // an executor is working on it
  | "blocked" // budget cap tripped or dependency not met
  | "awaiting_approval" // escalated to the human queue (2 failed review rounds, or manual)
  | "done" // review passed
  | "failed" // terminal failure
  | "cancelled";

export type AgentStatus =
  | "thinking"
  | "working"
  | "awaiting_approval"
  | "idle"
  | "error";

/** Model tiers the router may assign. `opus` and `claude` are the same family;
 * kept distinct so the ledger can distinguish master spend from worker spend. */
export type ModelTier = "planner" | "worker"; // ids are neutral; the actual model comes from .env

export interface TaskRow {
  id: string; // t_166… (kebab-safe id)
  parent_id: string | null; // tree edge
  title: string;
  /** human-given agent nickname (roadmap-2). null = fall back to title. */
  display_name: string | null;
  /** persisted claude session id — the key to in-place revival (roadmap-1a). */
  session_id: string | null;
  spec: string; // full instructions handed to the worker
  status: TaskStatus;
  assignee: string | null; // agent id once dispatched
  model_hint: ModelTier | null; // router decision
  worktree_path: string | null; // isolation dir
  attempts: number; // review re-dispatch counter (cap 2, plan §2 step 7)
  deps: string; // JSON array of upstream task ids
  created_at: number;
  updated_at: number;
}

/** What Master decomposition produces for each subtask (zod-validated at D3+). */
export interface TaskSpec {
  title: string;
  spec: string;
  dependencies: string[]; // ids of tasks that must be `done` first
  model_hint?: ModelTier;
}

export type MailboxType =
  | "dispatch" // master → worker: here is your task
  | "report" // worker → master: done, here is the result/diff
  | "clarify" // worker → master: question mid-task
  | "re-dispatch" // master → worker: failed review, try again with feedback
  | "control" // human → any agent (inject/interrupt ride this at DB level)
  | "note" // any agent → master/human: free-form remark via send_message
  | "broadcast"; // master → all: plan-level context

export interface MailboxMessage {
  id: number;
  from_agent: string; // 'master' | agent id | 'human'
  to_agent: string;
  task_id: string | null;
  type: MailboxType;
  payload: string; // JSON envelope, typed per MailboxType at the call site
  delivered: number;
  read: number;
  created_at: number;
}

export type ApproveDecision = "allow" | "deny" | "edit";

export interface CostRecord {
  id: number;
  agent_id: string;
  task_id: string;
  model: string; // concrete model string actually called
  input_tokens: number;
  output_tokens: number;
  cache_read: number;
  cache_write: number;
  est_cost_usd: number;
  created_at: number;
}

export interface CostTotals {
  task_id: string;
  model: string;
  input_tokens: number;
  output_tokens: number;
  /** cached-context tokens read by the run — a first-class column in
   *  cost_ledger since the usage-tracking fix; the aggregator forgot it and
   *  the UI read it via a lying `as` cast (always 0 on screen). */
  cache_read: number;
  cache_write: number;
  est_cost_usd: number;
}

/** GET /api/tree shape */
export interface TreeSnapshot {
  tasks: TaskRow[];
  totals: CostTotals[];
  generated_at: number;
  /** the project this gateway instance serves — the UI keys its local reset
   *  on this: when it changes across a reconnect, the whole pane state is
   *  stale (another project's agents) and must be dropped before REPLAY. */
  projectDir: string;
}
