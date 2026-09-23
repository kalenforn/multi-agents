/**
 * @maw/gateway/orchestrator/ledger — D11: budget guard + spend queries.
 *
 * The ledger is the router's teeth: enforceBudget() reads real per-task and
 * global spend from cost_ledger (NOT optimistic counters) and can force the
 * cheap tier or block new dispatch entirely. Rows land at RUN completion,
 * so mid-run spend of the CURRENT run is invisible by design (bounded by
 * the executor's own step limits) — the guard protects the NEXT dispatch.
 */

import type { WorkbenchDb } from "@maw/shared";
import { config } from "../config.js";

export interface BudgetConfig {
  perTaskTokenCap: number;   // tokens (in+out) per task across attempts
  globalTokenCap: number;    // tokens across the whole session
}

export const DEFAULT_BUDGET: BudgetConfig = {
  perTaskTokenCap: config.taskTokenCap,
  globalTokenCap: config.globalTokenCap,
};

export interface TaskSpend {
  tokens: number;
  attempts: number;
}

export function taskSpend(db: WorkbenchDb, taskId: string): TaskSpend {
  const rows = db.costByTask().filter((c) => c.task_id === taskId);
  const tokens = rows.reduce((s, c) => s + c.input_tokens + c.output_tokens, 0);
  return { tokens, attempts: 0 };
}

export function globalSpend(db: WorkbenchDb): number {
  return db.costByTask().reduce((s, c) => s + c.input_tokens + c.output_tokens, 0);
}

export type BudgetAction =
  | { kind: "ok" }
  | { kind: "downgrade"; reason: string }   // force the cheap tier
  | { kind: "block"; reason: string };      // no new dispatch at all

/**
 * Pure decision — dispatch layers call this before spawning a worker.
 * Order: global block > per-task block > per-task downgrade > ok.
 */
export function enforceBudget(
  db: WorkbenchDb,
  taskId: string,
  cfg: BudgetConfig = DEFAULT_BUDGET,
): BudgetAction {
  const global = globalSpend(db);
  if (global >= cfg.globalTokenCap) {
    return { kind: "block", reason: `global budget exhausted: ${global} ≥ ${cfg.globalTokenCap} tokens` };
  }
  const spend = taskSpend(db, taskId);
  if (spend.tokens >= cfg.perTaskTokenCap) {
    return { kind: "block", reason: `task budget exhausted: ${spend.tokens} ≥ ${cfg.perTaskTokenCap} tokens` };
  }
  if (spend.tokens >= cfg.perTaskTokenCap * 0.6) {
    return { kind: "downgrade", reason: `task spend ${spend.tokens} > 60% of cap — forcing cheap tier` };
  }
  return { kind: "ok" };
}
