/**
 * @maw/gateway/executors/fake — deterministic fake executor for the D2 wire
 * acceptance (plan §2 step 2). Emits exactly 5 events per run:
 * RUN_STARTED, 3× TEXT_MESSAGE_CONTENT, RUN_FINISHED. Responds to inject /
 * interrupt so the control plane can be verified before real executors land.
 *
 * DEV-ONLY: delete after D3–D5 replace it with real adapters.
 */

import type { AgentStatus } from "@maw/shared";
import type {
  AgentEvent,
  AgentHandle,
  ExecutorAdapter,
  SpawnOpts,
  TaskSpecInput,
} from "./adapter.js";

const TEXT_INTERVAL_MS = 150;

export class FakeExecutorAdapter implements ExecutorAdapter {
  readonly kind = "fake";
  private counter = 0;

  spawn(spec: TaskSpecInput, opts: SpawnOpts): AgentHandle {
    this.counter += 1;
    return new FakeHandle(opts.agentId ?? `fake_${this.counter}`, spec, opts);
  }
}

class FakeHandle implements AgentHandle {
  readonly id: string;
  private readonly queue: AgentEvent[] = [];
  private readonly waiters: Array<(r: IteratorResult<AgentEvent>) => void> = [];
  private readonly timers: NodeJS.Timeout[] = [];
  private finished = false;
  private state: AgentStatus = "idle";
  private readonly taskId?: string;
  private readonly nText: number;

  constructor(id: string, spec: TaskSpecInput, opts: SpawnOpts) {
    this.id = id;
    this.taskId = opts.taskId;
    this.nText = 3; // RUN_STARTED + 3 TEXT + RUN_FINISHED = 5 events per run
    this.state = "thinking";
    this.push({ type: "RUN_STARTED", payload: { goal: spec.title } });
    this.state = "working";
    for (let i = 0; i < this.nText; i++) {
      this.timers.push(
        setTimeout(() => {
          this.push({
            type: "TEXT_MESSAGE_CONTENT",
            payload: { messageId: `${this.id}-m${i}`, delta: `chunk ${i} of ${spec.title}` },
          });
        }, TEXT_INTERVAL_MS * (i + 1))
      );
    }
    this.timers.push(
      setTimeout(() => this.finish("success"), TEXT_INTERVAL_MS * (this.nText + 1))
    );
  }

  private push(ev: AgentEvent): void {
    if (this.taskId) ev.taskId = this.taskId;
    const w = this.waiters.shift();
    if (w) w({ value: ev, done: false });
    else this.queue.push(ev);
  }

  private finish(outcome: "success" | "interrupt" | "error"): void {
    if (this.finished) return;
    this.finished = true;
    for (const t of this.timers) clearTimeout(t);
    this.timers.length = 0;
    // executors emit run events only; the manager emits lifecycle AGENT_STATUS
    this.push({ type: "RUN_FINISHED", payload: { outcome } });
    this.state = "idle";
    // wake the generator so it terminates
    for (const w of this.waiters.splice(0)) w({ value: undefined, done: true });
  }

  status(): AgentStatus {
    return this.state;
  }

  readonly events: AsyncIterable<AgentEvent> = this.gen();

  private async *gen(): AsyncGenerator<AgentEvent> {
    while (true) {
      const next = this.queue.shift();
      if (next) {
        yield next;
        continue;
      }
      if (this.finished) return;
      const r = await new Promise<IteratorResult<AgentEvent>>((res) => this.waiters.push(res));
      if (r.done) return;
      yield r.value;
    }
  }

  async inject(prompt: string): Promise<void> {
    // simulate the human steering mid-run: an extra streamed chunk
    this.push({
      type: "TEXT_MESSAGE_CONTENT",
      payload: { messageId: `${this.id}-inj`, delta: `[human inject] ${prompt}` },
    });
  }

  async interrupt(): Promise<void> {
    this.finish("interrupt");
  }

  async approve(): Promise<void> {
    this.push({ type: "TOOL_CALL_RESULT", payload: { toolCallId: "fake", ok: true, resultPreview: "approved (fake)" } });
  }

  async kill(): Promise<void> {
    this.finish("error");
  }
}
