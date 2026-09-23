/**
 * D7.6 acceptance (message wake-chain fixes, two live p2p gaps):
 *  - Bug A: review-pass → coordinator notification must ride comms.sendMessage
 *    (hooks.onAgentMessage fires → auto-steering inject / dead-letter rescue),
 *    carry the worker's substantive report (truncated ≤ 4000), and keep the
 *    task association (task_id on the mailbox row).
 *  - Bug B: spawn injects the spawner's identity into the child spec, so the
 *    child reports to its coordinator instead of defaulting to master.
 * Part 1 exercises comms + notifyCoordinatorPassed in-process (no LLM); part 2
 * probes the live gateway's internal mailbox endpoint (no LLM either).
 */

import { spawn, execSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { openDb } from "@maw/shared";
import { Hub } from "../packages/gateway/src/ws/hub.js";
import { createComms } from "../packages/gateway/src/executors/comms.js";
import { notifyCoordinatorPassed } from "../packages/gateway/src/orchestrator/coordinator-notify.js";

const ROOT = path.resolve(import.meta.dirname, "..");

let failed = 0;
function check(name: string, cond: boolean, detail = ""): void {
  if (cond) console.log(`  PASS ${name}${detail ? ` — ${detail}` : ""}`);
  else { failed++; console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---------- part 1: comms unit (in-process, no gateway) ----------

console.log("\n[D7.6 part 1] comms.sendMessage wake hook + coordinator notify (unit)");
{
  const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d76-unit-"));
  try {
    const db = openDb(path.join(dbDir, "unit.db"));
    const hub = new Hub(db); // no attached clients — emit just persists
    const hookCalls: { to: string; from: string; text: string }[] = [];
    const comms = createComms(db, hub, {
      onAgentMessage: (to, from, text) => hookCalls.push({ to, from, text }),
      onSpawnRequest: () => ({ ok: false, error: "not under test" }),
    });

    // 判据 1:sendMessage 到 ch_/cs_/oc_ 收件人 → onAgentMessage 钩子触发
    for (const to of ["ch_t_a1", "cs_t_a2", "oc_t_a3"]) {
      const r = comms.sendMessage("master", to, "note", `wake ${to}`);
      check(`sendMessage → ${to} accepted`, r.ok === true);
    }
    check(
      "onAgentMessage hook fired for ch_/cs_/oc_ recipients",
      ["ch_t_a1", "cs_t_a2", "oc_t_a3"].every((a) => hookCalls.some((h) => h.to === a)),
      `hookCalls=${hookCalls.map((h) => h.to).join(",")}`,
    );
    check("hook carries from + full text", hookCalls[0]?.from === "master" && hookCalls[0]?.text === "wake ch_t_a1");

    // 判据 2:master/human 收件人不触发代理唤醒钩子(它们不是 executor)
    const before = hookCalls.length;
    comms.sendMessage("ch_t_a1", "master", "report", "to master only");
    comms.sendMessage("ch_t_a1", "human", "note", "to human only");
    check("hook NOT fired for master/human recipients", hookCalls.length === before);

    // 判据 3:taskId 参数保留任务关联(修复 sendMessage 写死 task_id:null)
    // (messages.task_id 有 FK → 先建任务行,生产路径传的也总是已存在的任务 id)
    db.insertTask({ id: "t_root_1", parent_id: null, title: "root task", spec: "root", status: "done" });
    const r3 = comms.sendMessage("master", "ch_t_a1", "note", "with task assoc", "t_root_1");
    const row3 = db.inbox("ch_t_a1").find((m) => (JSON.parse(m.payload) as { text?: string }).text === "with task assoc");
    check("taskId preserved on comms message row", r3.ok === true && row3?.task_id === "t_root_1", `task_id=${row3?.task_id}`);

    // 判据 4:消息可被收件人 inbox 读到(comms 通道完整落地)
    const ib = comms.inbox("ch_t_a1");
    check("message readable via inbox()", ib.ok === true && ib.messages.some((m) => m.text === "with task assoc" && m.from === "master"));

    // 判据 5:超长消息受控拒绝(R25 有界输入)
    check("over-length message rejected", comms.sendMessage("master", "ch_t_a1", "note", "x".repeat(4001)).ok === false);

    // 判据 6:review pass → coordinator 通知走 comms 通道(钩子触发 + inbox 可读)
    const taskId = "t_work_1";
    db.insertTask({ id: taskId, parent_id: null, title: "sub work", spec: "do the thing", status: "done" });
    db.insertMessage({ from_agent: "ch_coord_1", to_agent: "master", task_id: taskId, type: "note", payload: { text: `spawned worker task ${taskId} (sub work)` } });
    hookCalls.length = 0;
    const n1 = notifyCoordinatorPassed(comms, db, db.getTask(taskId), [{ type: "report", text: "实现了 A 并通过测试 B" }]);
    check("review-pass notify sent to spawner", n1.sent === true && n1.coordinator === "ch_coord_1");
    check("review-pass notify fired wake hook", hookCalls.some((h) => h.to === "ch_coord_1" && h.from === "master"));
    const coordRows = db.messagesTo("ch_coord_1");
    const noteRow = coordRows.find((m) => (JSON.parse(m.payload) as { text?: string }).text?.includes("实现了 A"));
    check("coordinator inbox carries worker's report text", !!noteRow, `rows=${coordRows.length}`);
    check("coordinator notify keeps task_id", noteRow?.task_id === taskId);
    check("notify text names the task", !!noteRow && (JSON.parse(noteRow.payload) as { text?: string }).text?.includes("sub work"));

    // 判据 7:超长 worker 报告安全截断到 comms 上限
    const longId = "t_work_2";
    db.insertTask({ id: longId, parent_id: null, title: "long work", spec: "do the long thing", status: "done" });
    db.insertMessage({ from_agent: "ch_coord_2", to_agent: "master", task_id: longId, type: "note", payload: { text: `spawned worker task ${longId} (long work)` } });
    const n2 = notifyCoordinatorPassed(comms, db, db.getTask(longId), [{ type: "report", text: "L".repeat(5000) }]);
    const longRow = db.messagesTo("ch_coord_2").find((m) => m.task_id === longId);
    const longText = longRow ? (JSON.parse(longRow.payload) as { text?: string }).text ?? "" : "";
    check("long worker report truncated ≤ 4000 and still delivered", n2.sent === true && longText.length > 0 && longText.length <= 4000, `len=${longText.length}`);

    // 判据 8:守卫——无派发记录 / human 派发 / 空报告
    const orphanId = "t_work_3";
    db.insertTask({ id: orphanId, parent_id: null, title: "orphan work", spec: "no spawner", status: "done" });
    check("no spawn receipt → not sent", notifyCoordinatorPassed(comms, db, db.getTask(orphanId), []).sent === false);
    const humanId = "t_work_4";
    db.insertTask({ id: humanId, parent_id: null, title: "human work", spec: "human spawned", status: "done" });
    db.insertMessage({ from_agent: "human", to_agent: "master", task_id: humanId, type: "note", payload: { text: `spawned worker task ${humanId} (human work)` } });
    check("human spawner → not sent", notifyCoordinatorPassed(comms, db, db.getTask(humanId), []).sent === false);
    const quietId = "t_work_5";
    db.insertTask({ id: quietId, parent_id: null, title: "quiet work", spec: "no messages", status: "done" });
    db.insertMessage({ from_agent: "ch_coord_3", to_agent: "master", task_id: quietId, type: "note", payload: { text: `spawned worker task ${quietId} (quiet work)` } });
    check("empty worker report still notifies (verdict alone is news)", notifyCoordinatorPassed(comms, db, db.getTask(quietId), []).sent === true);
  } finally {
    rmSync(dbDir, { recursive: true, force: true });
  }
}

// ---------- part 2: live gateway via the internal mailbox endpoint ----------

console.log("\n[D7.6 part 2] live gateway — spawn spec injection + agent-recipient wake (no LLM)");
{
  const PORT = 19970 + Math.floor(Math.random() * 20);
  const dbDir = mkdtempSync(path.join(tmpdir(), "maw-d76-live-"));
  let gw: ChildProcess | null = null;
  try {
    gw = spawn("pnpm", ["--filter", "@maw/gateway", "start"], {
      cwd: ROOT,
      env: { ...process.env, PORT: String(PORT), MAW_DB_PATH: path.join(dbDir, "d76.db"), MAW_WORKTREE_ROOT: path.join(dbDir, "worktrees") },
      stdio: ["ignore", "pipe", "pipe"],
      detached: true, // process-group kill below reaps tsx children too
    });
    let gwLog = "";
    gw.stdout?.on("data", (d) => { gwLog += String(d); });
    gw.stderr?.on("data", (d) => { gwLog += String(d); });
    for (let i = 0; i < 60; i++) {
      try { const r = await fetch(`http://localhost:${PORT}/health`); if (r.ok) break; } catch { /* retry */ }
      await sleep(200);
    }

    const mailbox = (body: Record<string, unknown>) =>
      fetch(`http://localhost:${PORT}/api/internal/mailbox`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }).then((r) => r.json());

    // 判据 9:spawn 注入派发者身份(Bug B)
    const spawnRes = (await mailbox({ agent: "ch_coord_live", op: "spawn", title: "d76 spawn probe", spec: "Write a one-line note file for the d76 wake test.", tier: "planner" })) as { ok?: boolean; taskId?: string; agentId?: string };
    check("internal spawn accepted", spawnRes.ok === true, `taskId=${spawnRes.taskId}`);
    const tree = (await (await fetch(`http://localhost:${PORT}/api/tree`)).json()) as { tasks: { id: string; spec: string }[] };
    const spawned = tree.tasks.find((t) => t.id === spawnRes.taskId);
    check(
      "spawned spec names the coordinator (report-to injection)",
      !!spawned && spawned.spec.includes("你由 agent ch_coord_live 派出") && spawned.spec.includes('send_message(to="ch_coord_live"'),
      spawned?.spec.slice(-160),
    );
    check("original spec preserved alongside injection", !!spawned && spawned.spec.includes("d76 wake test"));

    // 判据 10:send 到 ch_ 收件人 → onAgentMessage 钩子真跑(auto-woke 日志)+
    // 消息经 comms 通道落 inbox(收件人不存在 → rescue 查无此任务,不误开 worker)
    const sendRes = (await mailbox({ agent: "ch_worker_live", op: "send", to: "ch_ghost_t_nope", mailboxType: "note", text: "d76 wake probe" })) as { ok?: boolean };
    check("send to agent recipient accepted", sendRes.ok === true);
    for (let i = 0; i < 50 && !gwLog.includes("auto-woke ch_ghost_t_nope"); i++) await sleep(200);
    check("gateway wake hook ran (onAgentMessage fired)", gwLog.includes("auto-woke ch_ghost_t_nope"));
    check("dead-letter rescue correctly skipped for unknown task", gwLog.includes("rescue lookup failed for ch_ghost_t_nope"));
    const inboxRes = (await mailbox({ agent: "ch_ghost_t_nope", op: "inbox" })) as { ok?: boolean; messages?: { text: string }[] };
    check("message lands in recipient inbox via comms channel", inboxRes.ok === true && (inboxRes.messages ?? []).some((m) => m.text === "d76 wake probe"));

    // 判据 11:worker 报告消息在 DB 里可被 review 侧收集(messagesFrom)
    const reportRows = execSync(`sqlite3 ${path.join(dbDir, "d76.db")} "SELECT COUNT(*) FROM messages WHERE to_agent='ch_ghost_t_nope';" 2>/dev/null`).toString().trim();
    check("recipient message persisted in mailbox table", Number(reportRows) >= 1, `rows=${reportRows}`);
  } finally {
    try { if (gw?.pid) process.kill(-gw.pid, "SIGKILL"); } catch { /* already gone */ }
    try { gw?.kill("SIGKILL"); } catch { /* already gone */ }
    await sleep(300);
    if (failed === 0) rmSync(dbDir, { recursive: true, force: true });
    else console.log(`  (failure evidence kept at ${dbDir})`);
  }
}

console.log(failed === 0 ? "\nD7.6 ACCEPTANCE PASS — wake chain fixed (comms-routed notify + spawner injection)" : `\nD7.6 ACCEPTANCE FAIL — ${failed} check(s) failed`);
process.exit(failed === 0 ? 0 : 1);
