/**
 * @maw/gateway/orchestrator/router — D6 v0 rules: task profile → executor tier.
 *
 * Three hard rules for now (plan §2 step 6); the D11 budget guard and
 * dashboard extend this, never bypass it. The router can only DOWNGRADE
 * cost (force cheap) or hold — it never upgrades past what the plan allows,
 * so a bad model_hint cannot burn budget by asking for the expensive tier.
 */

import type { ModelTier } from "@maw/shared";

export interface TaskProfile {
  modelHint?: string;
  specChars: number;
  parallelizable: boolean; // rough proxy: no dependencies
}

export function routerTier(hint?: string): ModelTier | null {
  if (hint === "planner" || hint === "worker") return hint;
  return null;
}

/** Master's suggestion → actual executor name for the executor registry. */
export function resolveExecutorForTask(profile: TaskProfile): string {
  const hint = profile.modelHint === "worker" ? "worker" : "planner";
  // rule 1: long specs/long-horizon work goes through the harness (better
  // context management); short parallelizable work through the lite loop
  const harness = profile.specChars > 800 || !profile.parallelizable;
  return harness ? `claude-${hint}` : hint;
}

/**
 * Router-tier downgrade given a budget action from the ledger (D11).
 * The router can only DOWNGRADE — a budget action never upgrades cost.
 */
export function applyBudget(profile: TaskProfile, action: { kind: "ok" | "downgrade" | "block"; reason?: string }): { profile: TaskProfile | null; reason?: string } {
  if (action.kind === "block") return { profile: null, reason: action.reason };
  if (action.kind === "downgrade") return { profile: { ...profile, modelHint: "worker" }, reason: action.reason };
  return { profile };
}
