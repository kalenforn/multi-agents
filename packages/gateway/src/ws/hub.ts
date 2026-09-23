/**
 * @maw/gateway/ws/hub — WebSocket fan-out with replay (plan §2 step 2).
 *
 * Invariants (architecture invariant 3):
 *  - every server event is persisted to `events` (monotonic seq) BEFORE broadcast
 *  - reconnects replay from sinceSeq+1, each event exactly once
 *  - every client is implicitly subscribed to "master" (plan-level events)
 */

import type { ServerEvent, ServerEventType, WorkbenchDb } from "@maw/shared";
import type { WebSocket } from "ws";

export interface HubClient {
  ws: WebSocket;
  subscriptions: Set<string>; // agent ids; "master" always present
}

export interface EmitInput {
  agentId: string;
  taskId?: string | null;
  type: ServerEventType;
  payload: unknown;
}

export class Hub {
  private readonly clients = new Set<HubClient>();

  constructor(private db: WorkbenchDb) {}

  /** Open-Project hot swap: follow the (identity-stable) WorkbenchDb onto
   *  another project. Existing subscriptions keep their agent-id strings —
   *  the REPLAY the UI sends right after rebuilds onto the new DB. */
  rebind(db: WorkbenchDb): void {
    this.db = db;
  }

  attach(ws: WebSocket): HubClient {
    const client: HubClient = { ws, subscriptions: new Set(["master"]) };
    this.clients.add(client);
    ws.on("close", () => this.clients.delete(client));
    return client;
  }

  clientCount(): number {
    return this.clients.size;
  }

  /** Persist-then-broadcast: append to `events` first, then fan out. */
  emit(e: EmitInput): ServerEvent {
    const event = this.db.appendEvent(e);
    this.broadcast(event);
    return event;
  }

  /** Replay stored events with seq > sinceSeq to one client. Returns count. */
  replay(client: HubClient, sinceSeq: number): number {
    const events = this.db.eventsSince(sinceSeq);
    for (const e of events) this.send(client, e);
    return events.length;
  }

  private broadcast(event: ServerEvent): void {
    for (const c of this.clients) {
      if (c.subscriptions.has(event.agentId)) this.send(c, event);
    }
  }

  private send(c: HubClient, e: ServerEvent): void {
    if (c.ws.readyState === c.ws.OPEN) c.ws.send(JSON.stringify(e));
  }
}
