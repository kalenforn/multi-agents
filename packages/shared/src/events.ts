/**
 * @maw/shared — event vocabulary + wire protocol
 *
 * AG-UI vocabulary implemented locally (architecture invariant 5): we keep the
 * event taxonomy (lifecycle / text-delta / tool-call / approval / sub-agent
 * attribution) but hold the TypeScript types ourselves and ship them over a
 * plain WebSocket. No dependency on any pre-1.0 runtime. If AG-UI drifts, the
 * adapter layer here is the only place that changes.
 *
 * Invariant: every server event is persisted to `events` (monotonic seq)
 * BEFORE broadcast; reconnects replay from sinceSeq+1 (plan §2 step 2).
 */

import type { AgentStatus, ApproveDecision } from "./types.js";

// ---------- server → client ----------

export type ServerEventType =
  | "TEXT_MESSAGE_START"
  | "TEXT_MESSAGE_CONTENT"
  | "TEXT_MESSAGE_END"
  | "RUN_STARTED"
  | "RUN_FINISHED"
  | "RUN_ERROR"
  | "TOOL_CALL_START"
  | "TOOL_CALL_RESULT"
  | "APPROVAL_REQUIRED"
  | "AGENT_STATUS"
  | "STATE_SNAPSHOT"
  | "MESSAGE"
  | "TURN_COMPLETED"
  | "TASK_TREE_UPDATED";

/** Envelope for everything the gateway pushes over WS. */
export interface ServerEvent {
  seq: number; // monotonic, from `events` table — replay anchor
  ts: number;
  agentId: string; // which agent this belongs to; "master" for plan-level events
  taskId?: string;
  type: ServerEventType;
  payload: unknown; // typed below per ServerEventType
}

export interface TextMessageStartPayload {
  messageId: string;
  role: "assistant" | "user";
}
export interface TextMessageContentPayload {
  messageId: string;
  delta: string;
}
export interface TextMessageEndPayload {
  messageId: string;
}
export interface RunFinishedPayload {
  outcome: "success" | "interrupt" | "error";
  summary?: string;
}
export interface RunErrorPayload {
  message: string; // sanitized: no stack traces, no secrets
}
export interface ToolCallStartPayload {
  toolCallId: string;
  name: string;
}
export interface ToolCallResultPayload {
  toolCallId: string;
  ok: boolean;
  resultPreview: string;
}
export interface ApprovalRequiredPayload {
  toolCallId: string;
  name: string;
  argsPreview: string;
}
export interface AgentStatusPayload {
  status: AgentStatus;
  detail?: string;
}
export interface TaskTreeUpdatedPayload {
  reason: "decomposed" | "dispatch" | "review" | "human" | "recovered";
}
export interface TurnCompletedPayload {
  userText: string; // the prompt that started this turn
  agentText: string; // the model's complete reply (deduped)
  round: number;
}
export interface MessagePayload {
  from: string;
  to: string;
  mailboxType: string; // MailboxType at the DB; kept loose for wire compat
  text: string; // redacted before persistence; bounded length
}

// ---------- client → server ----------

export type ClientMessage =
  | { type: "SUBSCRIBE"; agentId: string }
  | { type: "UNSUBSCRIBE"; agentId: string }
  | { type: "REPLAY"; sinceSeq: number }
  | { type: "SUBMIT_GOAL"; goal: string; executor?: string }
  | { type: "INJECT"; agentId: string; prompt: string }
  | { type: "INTERRUPT"; agentId: string }
  | { type: "KILL"; agentId: string }
  | {
      type: "APPROVE";
      agentId: string;
      toolCallId: string;
      decision: ApproveDecision;
      patch?: string;
    };

export function isClientMessage(v: unknown): v is ClientMessage {
  if (typeof v !== "object" || v === null) return false;
  const t = (v as { type?: unknown }).type;
  return (
    typeof t === "string" &&
    [
      "SUBSCRIBE",
      "UNSUBSCRIBE",
      "REPLAY",
      "SUBMIT_GOAL",
      "INJECT",
      "INTERRUPT",
      "KILL",
      "APPROVE",
    ].includes(t)
  );
}
