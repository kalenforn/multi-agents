/**
 * @maw/gateway/orchestrator/review — D7: the review loop.
 *
 * Worker finishes → mailbox report → Master (planner tier) reviews the worktree diff
 * against the task spec → verdict:
 *   pass        → task done, dependents unlocked
 *   fail        → re-dispatch with feedback, attempts < MAX_REDISPATCH
 *   escalate    → awaiting_approval (human queue) after MAX_REDISPATCH or on
 *                 review-error (never silently drop a worker's failure)
 *
 * Invariant (plan §2): every task leaves 'running' via exactly one of
 * {done, queued(re-dispatch), failed, awaiting_approval}. The review itself
 * is one bounded planner call with a zod-validated verdict.
 */

import { z } from "zod";
import { generateText } from "ai";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { config } from "../config.js";
import type { TaskRow, WorkbenchDb } from "@maw/shared";

const MAX_REDISPATCH = config.maxRedispatch; // plan OPEN item 3, default 2

const VerdictSchema = z.object({
  verdict: z.enum(["pass", "fail"]),
  feedback: z.string().max(2000).default(""),
  /** dynamic staffing: the reviewer may ask for ONE more worker when the
   *  evidence shows work the current tree cannot cover. Planted under the
   *  same root, dispatched by the normal pipeline. */
  spawn: z.object({
    title: z.string().min(3).max(200),
    spec: z.string().min(10).max(4000),
    depends_on_reviewed_task: z.boolean().default(false),
  }).optional(),
});

const REVIEW_SYSTEM = `You are the Master reviewer of a multi-agent workbench. A worker agent
claims to have completed a task. Review the evidence and decide.
Return ONLY JSON: {"verdict": "pass"|"fail", "feedback": "...(MAX 200 chars — a short actionable hint, NOT a report)", "spawn": {"title": "...", "spec": "...", "depends_on_reviewed_task": true|false}}
KEEP feedback SHORT. Long feedback risks truncated JSON and a wasted review round.
- pass: the task's acceptance criteria are met by the evidence.
- fail: something concrete is missing or wrong — feedback says exactly what,
  so the worker can fix it in one more attempt.
- spawn (optional, rare): only if the evidence reveals necessary work that
  NO existing subtask covers — request ONE extra worker with a complete,
  self-contained spec. Do not use spawn to re-split the reviewed task itself.
Be strict but fair: do not fail for cosmetic preferences. The task spec is
the contract. No markdown fences — JSON only.`;

export interface ReviewSpawnRequest {
  title: string;
  spec: string;
  dependsOnReviewed: boolean;
}

export interface ReviewVerdict {
  ok: boolean;
  verdict?: "pass" | "fail";
  feedback?: string;
  spawn?: ReviewSpawnRequest;
  error?: string;
}

/** One bounded review call. Errors → 'escalate' upstream, never silent. */
export async function reviewWork(
  db: WorkbenchDb,
  task: TaskRow,
  report: { summary: string; diff: string; messages: { type: string; text: string }[] },
): Promise<ReviewVerdict> {
  const apiKey = process.env.PLANNER_API_KEY ?? process.env.GLM_API_KEY;
  const baseUrl = process.env.PLANNER_BASE_URL ?? process.env.GLM_BASE_URL;
  const model = process.env.PLANNER_MODEL ?? process.env.GLM_MODEL;
  if (!apiKey || !baseUrl || !model) {
    return { ok: false, error: "ModelUnsupported: PLANNER_* not set for the reviewer (GLM_* legacy names also work)" };
  }
  const provider = createOpenAICompatible({ name: "reviewer", baseURL: baseUrl, apiKey });

  const evidence = [
    `# Task spec\n${task.spec}`,
    `# Worker's own summary\n${report.summary}`,
    `# Worker's messages via send_message (in its own words)\n${report.messages.map((m) => `[${m.type}] ${m.text}`).join("\n") || "(none)"}`,
    `# Worktree changes\n${report.diff}`,
  ].join("\n\n");

  const rawAttempts: string[] = [];
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const abort = new AbortController();
      const timer = setTimeout(() => abort.abort(), 180_000);
      const sys = attempt === 0 ? REVIEW_SYSTEM : REVIEW_SYSTEM + "\nYour previous answer was not valid JSON. Return ONLY the corrected JSON object.";
      const result = await generateText({
        model: provider.chatModel(model),
        system: sys,
        prompt: evidence,
        abortSignal: abort.signal,
        maxOutputTokens: 16_000, // thinking + verdict; default caps truncate long verdicts mid-JSON
      });
      clearTimeout(timer);
      rawAttempts.push(result.text.slice(0, 500));
      const parsed = VerdictSchema.safeParse(stripFences(result.text));
      if (parsed.success) {
        return {
          ok: true,
          verdict: parsed.data.verdict,
          feedback: parsed.data.feedback,
          spawn: parsed.data.spawn
            ? { title: parsed.data.spawn.title, spec: parsed.data.spawn.spec, dependsOnReviewed: parsed.data.spawn.depends_on_reviewed_task }
            : undefined,
        };
      }
      // parse fail → one repair round; second failure escalates upstream
    } catch (err) {
      if (attempt === 0) continue; // network/timeout → retry once
      return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
  }
  // include the raw outputs — without them a failing review is undiagnosable
  return { ok: false, error: `review verdict invalid after repair retry; raw: ${rawAttempts.join(" || ")}` };
}

export { MAX_REDISPATCH };

function stripFences(s: string): unknown {
  const m = s.match(/```(?:json)?\s*([\s\S]*?)```/);
  const candidate = m ? m[1]! : s;
  try {
    return JSON.parse(candidate);
  } catch {
    // salvage ladder for truncated/malformed verdicts:
    // a) first balanced {...} block (prose-wrapped)
    const start = candidate.indexOf("{");
    const end = candidate.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try { return JSON.parse(candidate.slice(start, end + 1)); } catch { /* fall through */ }
    }
    // b) TRUNCATED json (max-token cut mid-feedback): the verdict field sits
    //    at the front and stays intact — extract it directly, drop feedback
    const vm = candidate.match(/"verdict"\s*:\s*"(pass|fail)"/);
    if (vm) return { verdict: vm[1]!, feedback: "(truncated — see raw)" };
    return null;
  }
}
