/**
 * @maw/gateway/executors/manager — process lifecycle for executors.
 *
 * Consumes each AgentHandle's event stream and pushes it through the hub
 * (persist-then-broadcast). Tracks live handles so the control plane
 * (INJECT / INTERRUPT / APPROVE) can route to them; unknown agents get an
 * explicit error, never silence (plan §2 step 5).
 */

import type { AgentStatus, ApproveDecision } from "@maw/shared";
import type { Hub } from "../ws/hub.js";
import type { AgentEvent, AgentHandle, ExecutorAdapter, SpawnOpts, TaskSpecInput } from "./adapter.js";

export interface SpawnExtras {
  taskId?: string;
  agentId?: string;
  worktreeDir?: string;
  resumeSessionId?: string;
  onSessionId?: (sessionId: string) => void;
  onFinished?: (handle: AgentHandle) => void;
  onRunComplete?: (outcome: "success" | "interrupt" | "error", handle: unknown) => void;
}

export class ExecutorManager {
  private readonly handles = new Map<string, AgentHandle>();

  constructor(private readonly hub: Hub) {}

  spawn(adapter: ExecutorAdapter, spec: TaskSpecInput, extras: SpawnExtras = {}): AgentHandle {
    const opts: SpawnOpts = { taskId: extras.taskId, agentId: extras.agentId, worktreeDir: extras.worktreeDir, resumeSessionId: extras.resumeSessionId, onSessionId: extras.onSessionId, onRunComplete: extras.onRunComplete };
    const handle = adapter.spawn(spec, opts);
    this.handles.set(handle.id, handle);
    void this.consume(handle, extras);
    return handle;
  }

  get(agentId: string): AgentHandle | undefined {
    return this.handles.get(agentId);
  }

  liveCount(): number {
    return this.handles.size;
  }

  /** Ids of live handles — the UI's authority for amber lights. */
  liveIds(): string[] {
    return [...this.handles.keys()];
  }

  /** Live roster WITH per-agent run state — the single source of truth for
   *  the UI's indicator lights. Agents not on this list are not alive. */
  roster(): { id: string; state: string }[] {
    return [...this.handles.values()].map((h) => ({ id: h.id, state: h.status() }));
  }

  /** Hard-kill every live executor (gateway shutdown — no orphan children). */
  killAll(): number {
    let n = 0;
    for (const h of this.handles.values()) {
      void h.kill();
      // broadcast the death — without this, agents killed by a gateway
      // restart stay "alive" on the UI (lights/panes) until a refresh
      this.hub.emit({
        agentId: h.id,
        type: "RUN_ERROR",
        payload: { message: "agent terminated: gateway is shutting down" },
      });
      n++;
    }
    this.handles.clear();
    return n;
  }

  /** Route a control-plane client message. Returns false for unknown agents. */
  inject(agentId: string, prompt: string): boolean {
    const h = this.handles.get(agentId);
    if (!h) return false;
    void h.inject(prompt);
    return true;
  }

  interrupt(agentId: string): boolean {
    const h = this.handles.get(agentId);
    if (!h) return false;
    void h.interrupt();
    return true;
  }

  approve(agentId: string, toolCallId: string, decision: ApproveDecision, patch?: string): boolean {
    const h = this.handles.get(agentId);
    if (!h) { console.log(`[approve] ${toolCallId}: agent ${agentId} NOT in handles (${this.handles.size} live)`); return false; }
    console.log(`[approve] ${toolCallId} → ${agentId}: ${decision}`);
    void h.approve(toolCallId, decision, patch);
    return true;
  }

  private async consume(handle: AgentHandle, extras: SpawnExtras): Promise<void> {
    try {
      for await (const ev of handle.events) {
        this.hub.emit({
          agentId: handle.id,
          taskId: (ev as AgentEvent).taskId ?? extras.taskId ?? null,
          type: ev.type,
          payload: ev.payload,
        });
      }
    } catch (err) {
      console.error(`[manager] executor ${handle.id} stream error:`, err);
    } finally {
      this.handles.delete(handle.id);
      this.hub.emit({
        agentId: handle.id,
        type: "AGENT_STATUS",
        payload: { status: "idle" satisfies AgentStatus, detail: "stream closed" },
      });
      extras.onFinished?.(handle);
    }
  }
}
