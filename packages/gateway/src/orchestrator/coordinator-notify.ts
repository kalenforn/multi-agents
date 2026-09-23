/**
 * @maw/gateway/orchestrator/coordinator-notify — review-pass → coordinator.
 *
 * When a spawned worker's task passes review, the coordinator that spawned it
 * must hear the verdict + the worker's substantive report. This MUST ride
 * comms.sendMessage: a direct db.insertMessage + hub.emit bypasses
 * hooks.onAgentMessage, so the coordinator never wakes (auto-steering inject
 * and dead-letter rescue both go through that hook — the live p2p gap where
 * the notification sat silently in the inbox).
 *
 * The worker's own messages are the payload, not just a worktree pointer —
 * a pure-message worker's worktree is clean, so "产出在 worktree 可 read_file
 * 查看" alone misleads the coordinator (found in live testing).
 */

import type { TaskRow, WorkbenchDb } from "@maw/shared";
import type { Comms } from "../executors/comms.js";
import { config } from "../config.js";

const MAX_NOTE_CHARS = config.maxNoteChars; // config.link with comms.maxMsgChars — sendMessage rejects longer

export interface CoordinatorNotifyResult {
  sent: boolean;
  coordinator?: string;
  error?: string;
}

/** Find the coordinator that spawned `task` (the spawn receipt it left in
 *  master's mailbox) and deliver the pass notification via comms. */
export function notifyCoordinatorPassed(
  comms: Comms,
  db: WorkbenchDb,
  task: TaskRow,
  workerMsgs: { type: string; text: string }[],
): CoordinatorNotifyResult {
  const spawnMsg = db.messagesTo("master").find((m) => m.payload.includes(task.id) && m.payload.includes("spawned worker task"));
  const coordinator = spawnMsg?.from_agent;
  if (!coordinator || coordinator === task.id || coordinator === "human") return { sent: false };
  const report = workerMsgs.map((m) => `[${m.type}] ${m.text}`).join("\n\n");
  const worktree = task.worktree_path
    ? `\n\n产出 worktree: ${task.worktree_path}(如有文件改动可 read_file 查看并整合)。`
    : "";
  const note = `你派出的子任务「${task.title}」(task ${task.id}) 已完成并通过审查。\n\n# Worker 报告\n${report}${worktree}`.slice(0, MAX_NOTE_CHARS);
  const r = comms.sendMessage("master", coordinator, "note", note, task.id);
  return { sent: r.ok, coordinator, error: r.error };
}
