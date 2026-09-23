/**
 * D1 acceptance smoke (plan §2 step 1): create DB, insert 1 row per table,
 * read back, verify invariants, clean up. Exit 0 = pass.
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { newTaskId, openDb } from "@maw/shared";

const dir = mkdtempSync(path.join(tmpdir(), "maw-smoke-"));
const db = openDb(path.join(dir, "smoke.db"));
let failed = 0;

function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}

try {
  // tasks
  const rootId = newTaskId();
  db.insertTask({ id: rootId, title: "smoke root" });
  const childId = newTaskId();
  db.insertTask({ id: childId, parent_id: rootId, title: "smoke child", status: "queued", model_hint: "planner" });
  check("tasks insert+get", db.getTask(childId).title === "smoke child");
  check("tasks tree edge", db.children(rootId).length === 1);
  db.setStatus(childId, "running", { assignee: "agent_a" });
  check("tasks status patch", db.getTask(childId).status === "running" && db.getTask(childId).assignee === "agent_a");

  // messages (mailbox)
  const msg = db.insertMessage({ from_agent: "master", to_agent: "agent_a", task_id: childId, type: "dispatch", payload: { spec: "x" } });
  check("mailbox insert+inbox", db.inbox("agent_a").length === 1 && db.inbox("agent_a")[0]!.id === msg.id);
  db.markDelivered([msg.id]);
  check("mailbox delivered", db.inbox("agent_a").length === 0);

  // events: monotonic seq + replay
  const e1 = db.appendEvent({ agentId: "agent_a", taskId: childId, type: "TEXT_MESSAGE_CONTENT", payload: { delta: "hello" } });
  const e2 = db.appendEvent({ agentId: "master", type: "TASK_TREE_UPDATED", payload: { reason: "dispatch" } });
  check("events monotonic seq", e2.seq === e1.seq + 1, `seq ${e1.seq} → ${e2.seq}`);
  const replayed = db.eventsSince(e1.seq);
  check("events replay sinceSeq+1", replayed.length === 1 && replayed[0]!.seq === e2.seq);
  const agentReplay = db.eventsSince(0, "agent_a");
  check("events per-agent filter", agentReplay.length === 1 && agentReplay[0]!.type === "TEXT_MESSAGE_CONTENT");

  // cost ledger
  db.insertCost({ agent_id: "agent_a", task_id: childId, model: "test-model", input_tokens: 1000, output_tokens: 200, cache_read: 0, cache_write: 0, est_cost_usd: 0.0003 });
  const totals = db.costByTask();
  check("cost attribution", totals.length === 1 && totals[0]!.task_id === childId && totals[0]!.est_cost_usd === 0.0003);

  // secrets redaction guardrail
  const secretMsg = db.insertMessage({ from_agent: "human", to_agent: "master", type: "control", payload: { note: "key sk-abcdef1234567890 leaked?" } });
  check("redact sk- pattern", !db.getMessage(secretMsg.id).payload.includes("sk-abcdef"));

  // snapshot
  const snap = db.treeSnapshot();
  check("tree snapshot", snap.tasks.length === 2 && snap.totals.length === 1);
} finally {
  db.close();
  rmSync(dir, { recursive: true, force: true });
}

console.log(failed === 0 ? "\nSMOKE PASS — D1 acceptance met" : `\nSMOKE FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
