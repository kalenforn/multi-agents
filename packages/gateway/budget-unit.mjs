import { openDb } from "@maw/shared";
import { enforceBudget, DEFAULT_BUDGET } from "./src/orchestrator/ledger.ts";
import { applyBudget } from "./src/orchestrator/router.ts";
import { rmSync } from "node:fs";

rmSync("/tmp/maw-bunit.db", { force: true });
const db = openDb("/tmp/maw-bunit.db");
// 场景 1：无花费 → ok
let a = enforceBudget(db, "t_a", { perTaskTokenCap: 1000, globalTokenCap: 10000 });
console.log("1) no spend:", a.kind, a.kind === "ok" ? "✅" : "❌");
// 场景 2：任务花 700（>60%）→ downgrade
db.insertCost({ agent_id: "x", task_id: "t_b", model: "m", input_tokens: 600, output_tokens: 100, cache_read: 0, cache_write: 0, est_cost_usd: 0 });
a = enforceBudget(db, "t_b", { perTaskTokenCap: 1000, globalTokenCap: 100000 });
console.log("2) 60%+ spend:", a.kind, a.kind === "downgrade" ? "✅" : "❌");
const r = applyBudget({ modelHint: "planner", specChars: 100, parallelizable: true }, a);
console.log("   downgraded tier:", r.profile?.modelHint, r.profile?.modelHint === "worker" ? "✅" : "❌");
// 场景 3：任务超限 → block
db.insertCost({ agent_id: "x", task_id: "t_c", model: "m", input_tokens: 900, output_tokens: 200, cache_read: 0, cache_write: 0, est_cost_usd: 0 });
a = enforceBudget(db, "t_c", { perTaskTokenCap: 1000, globalTokenCap: 100000 });
console.log("3) task cap exceeded:", a.kind, a.kind === "block" ? "✅" : "❌");
// 场景 4：全局超限 → block
db.insertCost({ agent_id: "x", task_id: "t_d", model: "m", input_tokens: 20000, output_tokens: 5000, cache_read: 0, cache_write: 0, est_cost_usd: 0 });
a = enforceBudget(db, "t_e", { perTaskTokenCap: 1000, globalTokenCap: 1000 });
console.log("4) global cap exceeded:", a.kind, a.kind === "block" ? "✅" : "❌");
db.close();
