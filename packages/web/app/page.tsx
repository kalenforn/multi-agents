"use client";

/**
 * Master view + inline worker panel (D5): task cards live-updating over WS,
 * each dispatched task expands into its agent panel with the control trio —
 * INJECT / INTERRUPT / APPROVE — plus the live stream.
 *
 * Event-sourcing contract with the gateway: on connect we REPLAY sinceSeq 0,
 * so a page refresh rebuilds the full history (chunks, models, statuses) —
 * "waiting for agent output" only means the agent truly hasn't streamed yet.
 * Executor choice is per-submission (the D6 router's precursor).
 */

import { useEffect, useRef, useState } from "react";
import type { ServerEvent, TreeSnapshot } from "@maw/shared";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

const GATEWAY = process.env.NEXT_PUBLIC_GATEWAY_URL || "http://localhost:8787";

const STATUS_COLOR: Record<string, string> = {
  pending: "#8fa1a7",
  queued: "#3f8ec4",
  running: "#0e7c86",
  blocked: "#c2701c",
  awaiting_approval: "#c2451c",
  done: "#2d8a4e",
  failed: "#c23b2d",
  cancelled: "#8fa1a7",
};

// two orthogonal dials instead of six combos: model × mode
// tier labels come from the GATEWAY (/api/tiers reads the operator's env) —
// the code ships model-agnostic; what you run is what you configured.
const MODELS: { id: string; label: string }[] = [
  { id: "planner", label: "Planner" },
  { id: "worker", label: "Worker" },
];
const MODES = [
  { id: "stream", label: "对话流（常驻）" },
  { id: "claude", label: "harness（任务）" },
  { id: "lite", label: "lite（轻量）" },
];
const executorOf = (model: string, mode: string): string =>
  mode === "lite" ? model : mode === "stream" ? `stream-${model}` : `claude-${model}`;

interface TurnMsg {
  role: "user" | "agent" | "system";
  text: string; // user: injected prompt; agent: that turn's streamed output; system: receipts
  ts: number;
  live?: boolean; // agent turn still streaming (deltas appending)
}
interface AgentPanelState {
  agentId: string;
  model?: string;
  live?: "running" | "idle" | "stopped";
  turns: TurnMsg[]; // user bubbles + system receipts — rendered SEPARATE from agent stream
  chunks: string[];
  pendingApproval: { toolCallId: string; name: string; argsPreview: string } | null;
  interrupted: boolean;
  finished: boolean;
}

export default function MasterView() {
  const [snapshot, setSnapshot] = useState<TreeSnapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const [lastSeq, setLastSeq] = useState(0);
  const [, setTick] = useState(0); // 30s tick: re-render so breath lights time out without new events
  useEffect(() => { const iv = setInterval(() => setTick((n) => n + 1), 30_000); return () => clearInterval(iv); }, []);
  const [openAgents, setOpenAgents] = useState<string[]>([]); // max 3 panes
  const [openAgentModal, setOpenAgentModal] = useState<string | null>(null); // half-screen modal
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [panels, setPanels] = useState<Map<string, AgentPanelState>>(new Map());
  const goalRef = useRef<HTMLTextAreaElement>(null); // uncontrolled — IME-safe
  const [model, setModel] = useState("planner");
  const [tierLabels, setTierLabels] = useState<Map<string, string>>(new Map());
  useEffect(() => {
    // one-shot: the gateway knows the real tier labels from the operator's env
    (async () => {
      try {
        const j = (await (await fetch(`${GATEWAY}/api/tiers`, { cache: "no-store" })).json()) as { tiers: { id: string; label: string }[] };
        if (Array.isArray(j.tiers) && j.tiers.length) setTierLabels(new Map(j.tiers.map((t) => [t.id, t.label])));
      } catch { /* gateway down — neutral defaults stand */ }
    })();
  }, []);
  const [mode, setMode] = useState("stream");
  // （newPanelOpen 已移除：composer 常驻）
  const injectRefs = useRef(new Map<string, HTMLTextAreaElement>()); // per-agent uncontrolled inputs (IME-safe)
  const streamRefs = useRef(new Map<string, HTMLDivElement>()); // live stream panes
  const lastHeartbeat = useRef(new Map<string, number>()); // agentId → last event ts (breath timeout)
  const [liveIds, setLiveIds] = useState<Set<string>>(new Set()); // legacy (compat)
  const [rosterLoaded, setRosterLoaded] = useState(false); // first roster pull done?
  // ── THE state authority for indicator lights ──
  // gateway roster (id → working|idle|...) polled every 3s. Task status,
  // panel live, heartbeats deliberately do NOT participate.
  const [roster, setRoster] = useState<Map<string, string>>(new Map());

  useEffect(() => {
    const pull = async () => {
      try {
        const j = (await (await fetch(`${GATEWAY}/health`)).json()) as { liveAgentIds?: string[]; roster?: { id: string; state: string }[] };
        if (j.roster) {
          setRoster(new Map(j.roster.map((r) => [r.id, r.state])));
          setRosterLoaded(true);
        }
        if (j.liveAgentIds) setLiveIds(new Set(j.liveAgentIds));
        // cost refresh rides the same poll: runs finish without emitting any
        // TASK_TREE_UPDATED, so a snapshot taken at page load shows only the
        // FIRST run's tokens forever (live bug: "usage tracking only tracks
        // round one"). Totals-only update — tasks keep their event-sourced
        // refresh path; cheap and always current.
        const t = (await (await fetch(`${GATEWAY}/api/cost`, { cache: "no-store" })).json()) as { totals: TreeSnapshot["totals"] };
        if (t.totals) setSnapshot((prev) => (prev ? { ...prev, totals: t.totals } : prev));
      } catch { /* gateway down — keep last known roster */ }
    };
    pull();
    const iv = setInterval(pull, 3_000);
    return () => clearInterval(iv);
  }, []);
  useEffect(() => {
    for (const el of streamRefs.current.values()) el.scrollTop = el.scrollHeight; // follow the tail on new output
  }, [panels]);

  /** roadmap-1a: wake a stopped agent back to life via its persisted claude
   *  session — in-place, same id, so panes and subscriptions keep working.
   *  Gateway failures land as master RUN_ERROR (global banner), not silence. */
  async function wakeAgent(agentId: string) {
    try {
      const res = await fetch(`${GATEWAY}/api/agents/revive`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agentId }),
      });
      if (!res.ok) {
        const j = (await res.json().catch(() => null)) as { error?: string } | null;
        window.alert(`唤醒失败: ${j?.error ?? res.status}`);
      }
      // success: RUN_STARTED/AGENT_STATUS events flip the light via roster
    } catch { /* gateway down — WS banner shows */ }
  }

  // the project the gateway currently serves — keying the local reset on it:
  // after an "Open Project" switch, every panel/openAgents entry belongs to
  // the OLD project and must be dropped before REPLAY rebuilds the new one.
  const lastProjectRef = useRef<string | null>(null);
  const [knownProjects, setKnownProjects] = useState<{ path: string; name: string; lastOpenedAt: number }[]>([]);

  // ── three-column resizable layout (roadmap-4d followup): draggable divider
  // lines with a sane default; drag a side panel to its minimum to collapse
  // it to a sliver, click the sliver to pop it back. Double-click a divider
  // to reset to default widths.
  const [leftW, setLeftW] = useState(220);
  const [rightW, setRightW] = useState(300);
  const [leftCollapsed, setLeftCollapsed] = useState(false);
  const [rightCollapsed, setRightCollapsed] = useState(false);
  // expanded-modal size (user request: manually resizable) — px, dragged by
  // the bottom-right grip; double-click resets to the default 50vw/85vh
  const [modalW, setModalW] = useState<number | null>(null);
  const [modalH, setModalH] = useState<number | null>(null);

  /** Resize drag for the expanded modal grip (bottom-right corner). */
  function startModalResize(evt: React.MouseEvent) {
    const startX = evt.clientX;
    const startY = evt.clientY;
    const startW = modalW ?? window.innerWidth * 0.5;
    const startH = modalH ?? window.innerHeight * 0.85;
    const move = (e: MouseEvent) => {
      setModalW(Math.max(520, Math.min(startW + (e.clientX - startX), window.innerWidth - 40)));
      setModalH(Math.max(320, Math.min(startH - (e.clientY - startY), window.innerHeight - 40)));
    };
    const cleanup = () => { window.removeEventListener("mousemove", move); window.removeEventListener("mouseup", cleanup); document.body.style.userSelect = ""; };
    document.body.style.userSelect = "none";
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", cleanup);
  }

  /** Start a column resize drag. which: "left"|"right", evt: mousedown on the
   *  grip line. Collapse when dragged below the minimum; the sliver is
   *  click-to-expand. Uses window listeners so the drag survives the grip. */
  function startColResize(which: "left" | "right", evt: React.MouseEvent) {
    const startX = evt.clientX;
    const startW = which === "left" ? leftW : rightW;
    const MIN = 150;
    const move = (e: MouseEvent) => {
      const delta = e.clientX - startX;
      const w = startW + (which === "left" ? delta : -delta);
      if (which === "left") {
        if (w < 120) { setLeftCollapsed(true); setLeftW(startW); cleanup(); return; }
        setLeftCollapsed(false); setLeftW(Math.min(w, window.innerWidth * 0.4));
      } else {
        if (w < 120) { setRightCollapsed(true); setRightW(startW); cleanup(); return; }
        setRightCollapsed(false); setRightW(Math.min(w, window.innerWidth * 0.4));
      }
    };
    const cleanup = () => { window.removeEventListener("mousemove", move); window.removeEventListener("mouseup", cleanup); document.body.style.userSelect = ""; };
    document.body.style.userSelect = "none"; // don't paint text selections while dragging
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", cleanup);
  }

  async function refreshProjects() {
    try {
      const res = await fetch(`${GATEWAY}/api/projects`, { cache: "no-store" });
      if (res.ok) setKnownProjects(((await res.json()) as { projects: { path: string; name: string; lastOpenedAt: number }[] }).projects);
    } catch { /* gateway down */ }
  }

  /** returns true when an Open-Project flip happened (the caller then skips
   *  its own REPLAY — the flip branch already dispatched one; two replays
   *  would double-append live turns) */
  async function loadTree(): Promise<boolean> {
    let flipped = false;
    try {
      const res = await fetch(`${GATEWAY}/api/tree`, { cache: "no-store" });
      if (!res.ok) return false;
      const snap = (await res.json()) as TreeSnapshot;
      if (lastProjectRef.current !== null && snap.projectDir && snap.projectDir !== lastProjectRef.current) {
        // Open-Project switch: every panel and open pane belongs to the OLD
        // project — drop them all, then REPLAY the new project's history over
        // the SAME live socket (hot swap: no disconnect, no reconnect — the
        // gateway rebound in place). React state updaters queue in order, so
        // the wipe below commits before any replayed event applies.
        setPanels(new Map());
        setOpenAgents([]);
        setGlobalError(null);
        lastHeartbeat.current.clear();
        try { localStorage.removeItem("maw-open-agents"); } catch { /* private mode */ }
        // ask the MAIN socket (the renderer) to replay the new project's
        // history — a window event, not a direct send: this socket is a local
        // of the WS effect. Dispatch is sync; React state updaters queue in
        // order, so the wipes above commit before any replayed event applies.
        window.dispatchEvent(new CustomEvent("maw-replay"));
        void refreshProjects(); // sidebar: new project moves to the top
        flipped = true;
      }
      lastProjectRef.current = snap.projectDir;
      setSnapshot(snap);
      return flipped;
    } catch { /* gateway down — WS banner shows */ return false; }
  }

  /** roadmap-4 "Open Project": rebind the whole workbench to a directory.
   *  Gateway stops every old agent (history kept in that project's DB),
   *  restarts onto the pointer, and our WS reconnect + REPLAY rebuilds.
   *  Picker: the gateway raises the NATIVE macOS folder dialog (the packaged
   *  app swaps in its own dialog); manual path input only as a fallback. */
  /** Hot-switch the gateway to a project directory (roadmap-4d sidebar core).
   *  dir given: direct switch (sidebar click). dir omitted: raise the native
   *  folder picker (新建/导入 share this). init: fresh empty dir gets git init
   *  so executors keep worktree isolation. */
  async function openProject(dir?: string, init = false) {
    if (!dir) {
      try {
        const res = await fetch(`${GATEWAY}/api/project/pick`, { method: "POST" });
        if (res.ok) {
          const j = (await res.json()) as { dir?: string; cancelled?: boolean };
          if (j.cancelled) return; // closed the dialog — no-op, not an error
          if (j.dir) dir = j.dir;
        }
      } catch { /* gateway down — fall through to manual */ }
    }
    if (!dir) {
      const typed = window.prompt("打开项目（输入项目目录的绝对路径）", snapshot?.projectDir ?? "");
      if (!typed) return;
      dir = typed.trim();
    }
    if (dir === snapshot?.projectDir) return;
    try {
      const res = await fetch(`${GATEWAY}/api/project/open`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dir, init }),
      });
      const j = (await res.json().catch(() => null)) as { error?: string } | null;
      if (!res.ok) window.alert(`打开失败: ${j?.error ?? res.status}`);
      // no loadTree here: the flip broadcast arrives on the WS and loadTree's
      // projectDir change branch owns the wipe + REPLAY
    } catch { /* gateway mid-restart — WS reconnect carries the recovery */ }
  }

  /** roadmap-2: rename an agent (display_name on its task). Empty string clears. */
  async function renameAgent(taskId: string) {
    const t = (snapshot?.tasks ?? []).find((x) => x.id === taskId);
    const current = t?.display_name ?? "";
    const name = window.prompt("Agent 名字（留空恢复默认标题）", current);
    if (name === null) return; // cancelled
    try {
      const res = await fetch(`${GATEWAY}/api/tasks/name`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ taskId, name }),
      });
      if (!res.ok) {
        const j = (await res.json().catch(() => null)) as { error?: string } | null;
        window.alert(`改名失败: ${j?.error ?? res.status}`);
      }
      // success path: TASK_TREE_UPDATED arrives on master → loadTree() refreshes
    } catch { /* gateway down — WS banner shows */ }
  }

  // persist open panes across refreshes (per-viewer, localStorage)
  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem("maw-open-agents") ?? "[]") as string[];
      if (Array.isArray(saved) && saved.length) setOpenAgents(saved.slice(0, 3));
    } catch { /* fresh start */ }
  }, []);
  useEffect(() => {
    try { localStorage.setItem("maw-open-agents", JSON.stringify(openAgents)); } catch { /* private mode */ }
  }, [openAgents]);

  useEffect(() => {
    loadTree();
    void refreshProjects();
    let ws: WebSocket;
    let retry = 0;
    let torn = false;
    const pageLoadedAt = Date.now();
    let subscribed = new Set<string>(openAgents); // restore pane subscriptions on reconnect too
    // loadTree's Open-Project flip wipes panes and then asks THIS socket to
    // REPLAY the new project's history — it cannot reach this local `ws`
    // directly, so the request travels as a window event. (Sending REPLAY over
    // the WsBridge's __mawWs instead was a black hole: the bridge has no
    // onmessage renderer.)
    const onReplayRequest = () => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "REPLAY", sinceSeq: 0 })); };
    window.addEventListener("maw-replay", onReplayRequest);

    // auto-reconnect: gateway restarts and network blips must not leave the
    // page dead ("WS down" forever). On reconnect the full state rebuilds
    // from REPLAY (event sourcing) — subscribe to everything we track.
    function upsertPanel(agentId: string, patch: (p: AgentPanelState) => AgentPanelState) {
      lastHeartbeat.current.set(agentId, Date.now()); // every event = a heartbeat
      setPanels((prev) => {
        const next = new Map(prev);
        const p = next.get(agentId) ?? { agentId, turns: [], chunks: [], pendingApproval: null, interrupted: false, finished: false };
        next.set(agentId, patch(p));
        return next;
      });
    }

    function connect() {
      ws = new WebSocket(GATEWAY.replace(/^http/, "ws"));
      ws.onopen = () => {
      retry = 0;
      setConnected(true);
      ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: "master" }));
      for (const a of subscribed) ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: a }));
      // REPLAY must wait for the tree fetch: after an Open-Project switch the
      // gateway on this socket belongs to the NEW project, and loadTree's
      // projectDir check drops the OLD project's pane state. If REPLAY were
      // sent first, the new project's events could land BEFORE the wipe and
      // be erased by it — fetch-then-replay guarantees the order.
      void loadTree().then((flipped) => { if (!flipped && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "REPLAY", sinceSeq: 0 })); });
    };
    ws.onclose = () => {
      setConnected(false);
      if (torn) return; // effect teardown — do not reconnect
      retry = Math.min(retry + 1, 6);
      setTimeout(connect, Math.min(0.3 * Math.pow(2, retry - 1), 16)); // 0.3s,0.6s,1.2s…16s — fast hops for short gateway restarts (project switch), capped backoff for real outages
    };
    ws.onmessage = (ev) => {
      const msg = JSON.parse(String(ev.data)) as ServerEvent;
      if (msg.type === "TASK_TREE_UPDATED") loadTree();
      if ((msg.payload as { agentId?: string })?.agentId && !subscribed.has((msg.payload as { agentId: string }).agentId)) {
        const agentId = (msg.payload as { agentId: string }).agentId;
        subscribed.add(agentId);
        ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId }));
      }
      // agent-level events arrive either live (SUBSCRIBE) or via REPLAY
      if (msg.agentId !== "master") {
        if (!subscribed.has(msg.agentId)) {
          subscribed.add(msg.agentId);
          ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: msg.agentId }));
        }
      }
      switch (msg.type) {
        case "RUN_ERROR": {
          const { message } = msg.payload as { message: string };
          if (msg.agentId === "master") {
            // global banner: master-stream errors belong to the whole UI
            setGlobalError(`${new Date().toLocaleTimeString()} — ${message}`);
          } else {
            upsertPanel(msg.agentId, (p) => ({ ...p, chunks: [...p.chunks, `❌ ${message}`].slice(-3000) }));
          }
          break;
        }
        case "TOOL_CALL_START": {
          const { name, argsPreview } = msg.payload as { name: string; argsPreview?: string };
          // every call gets its own fenced block WITH its arguments — silent
          // tool calls (run_command/search) were invisible to the operator
          const args = argsPreview ?? "";
          upsertPanel(msg.agentId, (p) => ({ ...p, chunks: [...p.chunks, `\n🔧 ${name}\n\`\`\`
${args}\n\`\`\`
`].slice(-3000) }));
          break;
        }
        case "TOOL_CALL_RESULT": {
          const { toolCallId, ok, resultPreview } = msg.payload as { toolCallId: string; ok?: boolean; resultPreview?: string };
          const mark = ok === false ? "❌" : "✔";
          upsertPanel(msg.agentId, (p) => ({ ...p, chunks: [...p.chunks, `${mark} 结果(${toolCallId.slice(-6)}): ${resultPreview ?? ""}\n`].slice(-3000) }));
          break;
        }
        case "MESSAGE": {
          const { from, mailboxType, text } = msg.payload as { from: string; mailboxType: string; text: string };
          // agent-to-agent traffic renders as SYSTEM bubbles on the same
          // timeline (yellow) — visible, but distinct from human/LLM turns
          upsertPanel(msg.agentId, (p) => ({ ...p, turns: [...p.turns, { role: "system" as const, text: `📩 [${mailboxType}] from ${from}: ${text}`, ts: msg.ts }].slice(-200) }));
          break;
        }
        case "AGENT_STATUS": {
          const detail = (msg.payload as { detail?: string }).detail;
          const st = (msg.payload as { status?: string }).status;
          if (st) {
            upsertPanel(msg.agentId, (p) => ({ ...p, live: st === "working" || st === "thinking" ? "running" : st === "idle" ? "idle" : p.live }));
          }
          if (detail && detail.includes("check_inbox")) {
            upsertPanel(msg.agentId, (p) => ({ ...p, chunks: [...p.chunks, `🔔 ${detail}`].slice(-3000) }));
          }
          break;
        }
        case "RUN_STARTED": {
          upsertPanel(msg.agentId, (p) => {
            // TURN_COMPLETED archives the turn; init frames just mark the
            // next one — clearing chunks here is safe now
            return { ...p, model: (msg.payload as { model?: string }).model, finished: false, live: "running" as const, chunks: [] };
          });
          break;
        }
        case "TEXT_MESSAGE_CONTENT": {
          // live output appends DIRECTLY to the current agent turn on the
          // conversation timeline — one copy, one place, typewriter for free
          const delta = (msg.payload as { delta?: string }).delta ?? "";
          if (!delta || msg.agentId === "master") break;
          upsertPanel(msg.agentId, (p) => {
            const turns = [...p.turns];
            const last = turns[turns.length - 1];
            if (last && last.role === "agent" && last.live) {
              turns[turns.length - 1] = { ...last, text: last.text + delta };
            } else {
              turns.push({ role: "agent" as const, text: delta, ts: msg.ts, live: true });
            }
            return { ...p, turns: turns.slice(-200) };
          });
          break;
        }
        case "TURN_COMPLETED": {
          const { userText, agentText } = msg.payload as { userText?: string; agentText?: string };
          upsertPanel(msg.agentId, (p) => {
            // the user bubble MUST survive every path — the finalize branch
            // (live turn streamed) previously dropped userText entirely, so
            // after a refresh the prompt vanished while the reply stayed
            const turns = [...p.turns];
            const last = turns[turns.length - 1];
            const userTurn: TurnMsg | null = userText
              ? { role: "user" as const, text: userText, ts: msg.ts - 1 }
              : null;
            if (last && last.role === "agent" && last.live) {
              // finalize the live turn; insert the user bubble BEFORE it if
              // this prompt isn't already on the timeline
              const already = userTurn && turns.some((m) => m.role === "user" && m.text === userText);
              let insert: TurnMsg[] = turns;
              if (userTurn && !already) {
                const idx = turns.length - 1; // before the live agent turn
                insert = [...turns.slice(0, idx), userTurn, ...turns.slice(idx)];
              }
              insert[insert.length - 1] = {
                ...insert[insert.length - 1],
                text: agentText && agentText.length > insert[insert.length - 1].text.length ? agentText : insert[insert.length - 1].text,
                live: false,
              };
              return { ...p, turns: insert };
            }
            if (agentText && p.turns.some((m) => m.role === "agent" && m.text === agentText)) return p; // REPLAY dedupe
            if (userTurn) turns.push(userTurn);
            if (agentText) turns.push({ role: "agent" as const, text: agentText, ts: msg.ts });
            return { ...p, turns };
          });
          break;
        }
        case "APPROVAL_REQUIRED": {
          // only prompt for approvals that happened after page load (historical
          // ones were already decided)
          if (msg.ts > pageLoadedAt) {
            const { toolCallId, name, argsPreview } = msg.payload as { toolCallId: string; name: string; argsPreview: string };
            upsertPanel(msg.agentId, (p) => ({ ...p, pendingApproval: { toolCallId, name, argsPreview } }));
          }
          break;
        }
        case "RUN_FINISHED": {
          const outcome = (msg.payload as { outcome?: string }).outcome;
          const dark = outcome === "error" || outcome === "interrupt";
          upsertPanel(msg.agentId, (p) => ({ ...p, finished: true, live: dark ? "stopped" : "idle", chunks: [], interrupted: outcome === "interrupt" || p.interrupted }));
          break;
        }
      }
      setLastSeq((n) => Math.max(n, msg.seq));
    };
    } // connect() — onmessage handler assigned above
    connect(); // the D14 reconnect refactor left the function defined but never
    // invoked — compiled clean, so the page sat WS-down silently (empty panes,
    // no live updates) while the WsBridge kept control sends working
    return () => { torn = true; window.removeEventListener("maw-replay", onReplayRequest); try { ws.close(); } catch { /* gone */ } };
  }, []);

  function killAgent(agentId: string) {
    // optimistic: light goes DARK immediately (the gateway KILL also emits
    // RUN_ERROR, but local state flips without waiting for the round trip)
    setPanels((prev) => {
      const next = new Map(prev);
      const p = next.get(agentId);
      if (p) next.set(agentId, { ...p, live: "stopped" as const, finished: true });
      return next;
    });
    (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "KILL", agentId }));
  }

  function sendInject(agentId: string, prompt: string) {
    // optimistic: the user bubble appears IMMEDIATELY (the turn-confirmed
    // version comes from TURN_COMPLETED; dedupe by text+approx time handles
    // the echo)
    (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "INJECT", agentId, prompt }));
    setPanels((prev) => {
      const next = new Map(prev);
      const p = next.get(agentId) ?? { agentId, turns: [], chunks: [], pendingApproval: null, interrupted: false, finished: false };
      next.set(agentId, { ...p, turns: [...p.turns, { role: "user" as const, text: prompt, ts: Date.now() }].slice(-100) });
      return next;
    });
  }

  async function submitGoal(e: React.FormEvent) {
    e.preventDefault();
    const typed = goalRef.current?.value ?? "";
    // ＋New with empty input still opens an agent: a held-open standby pane
    // the user steers later ("open first, think later")
    const text = typed.trim() || "You are a standby agent. Acknowledge briefly and wait for task instructions.";
    // orchestrate sends no executor — the gateway Master decomposes instead
    const executor = mode === "orchestrate" ? undefined : executorOf(model, mode);
    await fetch(`${GATEWAY}/api/tasks`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(executor ? { goal: text, executor } : { goal: text }),
    });
    if (goalRef.current) goalRef.current.value = "";
  }

  const tasks = snapshot?.tasks ?? [];
  const roots = tasks.filter((t) => t.parent_id === null);
  // depth: root=0, child=1 (orchestrate trees are one level today; deeper
  // nesting renders recursively when D11 adds it)
  const depthOf = (t: (typeof tasks)[number]): number => (t.parent_id === null ? 0 : 1);
  const MAX_OPEN_PANELS = 3;
  function toggleAgent(agentId: string | null) {
    if (!agentId) return;
    setOpenAgents((prev) => {
      if (prev.includes(agentId)) return prev.filter((a) => a !== agentId);
      const next = [...prev, agentId];
      return next.slice(Math.max(0, next.length - MAX_OPEN_PANELS)); // FIFO beyond 3
    });
  }

  // segment a raw stream into render blocks: separator lines → hr,
  // timestamped badge lines → text (pre-wrap), everything else → markdown
// ── THE single state machine (D14 rebuild) ──
// One function, one truth. Lights AND controls derive from this phase —
// never from two different signal sets again.
type Phase = "working" | "idle" | "awaiting_approval" | "stopped" | "queued";
function agentPhase(
  agentId: string | null | undefined,
  taskStatus: string,
  roster: Map<string, string>,
  rosterLoaded: boolean,
): Phase {
  // 1. THE ROSTER IS SUPREME: the gateway's handle table is ground truth for
  //    "is this agent alive". A stream agent stays alive long after its task
  //    completes (idle window) — task status MUST NOT kill the light.
  const r = agentId ? roster.get(agentId) : undefined;
  if (r === "working" || r === "thinking") return "working";
  if (r === "idle") return "idle";
  if (r === "awaiting_approval" || r === "error") return "awaiting_approval";
  if (r) return "idle"; // any other live state = waiting
  // 2. not on the roster (never dispatched, reaped, or gateway restarted):
  if (taskStatus === "awaiting_approval") return "awaiting_approval";
  if (["queued", "pending"].includes(taskStatus)) return "queued";
  if (!rosterLoaded) return "queued"; // unknown until first poll
  return "stopped"; // was dispatched, now gone → dead
}
const PHASE_LIGHT: Record<Phase, { bg: string; title: string; breathe?: boolean }> = {
  working: { bg: "#2d8a4e", title: "工作中 — LLM 正在输出", breathe: true },
  idle: { bg: "#d9a441", title: "等待输入（agent 活着）" },
  awaiting_approval: { bg: "#d9a441", title: "等待人工裁决（可注入）" },
  stopped: { bg: "#c0392b", title: "已停止" },
  queued: { bg: "transparent", title: "排队等待派发" },
};

  const SEG_BADGE = /^\s*(?:\[[^\]]*\]\s*)?(?:👤|📬|🔔|🔧|❌)/; // optional [timestamp] prefix
  type Seg = { kind: "hr" } | { kind: "text"; text: string } | { kind: "md"; text: string };
  const segmentStream = (raw: string): Seg[] => {
    const segs: Seg[] = [];
    for (const line of raw.split("\n")) {
      if (/^─+$/.test(line.trim())) { segs.push({ kind: "hr" }); continue; }
      const isBadge = SEG_BADGE.test(line);
      const last = segs[segs.length - 1];
      const kind: "text" | "md" = isBadge ? "text" : "md";
      if (last && last.kind === kind) (last as { text: string }).text += "\n" + line;
      else if (kind === "text") segs.push({ kind: "text", text: line });
      else segs.push({ kind: "md", text: line });
    }
    return segs;
  };
  const MD_COMPONENTS = {
    p: ({ children }: { children?: React.ReactNode }) => <p style={{ margin: "6px 0" }}>{children}</p>,
    code: ({ children }: { children?: React.ReactNode }) => <code style={{ background: "#e8eceb", padding: "1px 5px", borderRadius: 4, fontSize: 12 }}>{children}</code>,
    pre: ({ children }: { children?: React.ReactNode }) => <pre style={{ background: "#eef1f0", padding: 10, borderRadius: 6, overflowX: "auto", fontSize: 12 }}>{children}</pre>,
    ul: ({ children }: { children?: React.ReactNode }) => <ul style={{ margin: "6px 0 6px 20px" }}>{children}</ul>,
    ol: ({ children }: { children?: React.ReactNode }) => <ol style={{ margin: "6px 0 6px 20px" }}>{children}</ol>,
    table: ({ children }: { children?: React.ReactNode }) => <table style={{ borderCollapse: "collapse", margin: "8px 0", fontSize: 12.5 }}>{children}</table>,
    th: ({ children }: { children?: React.ReactNode }) => <th style={{ border: "1px solid #c9d3d0", padding: "4px 8px", background: "#eef1f0" }}>{children}</th>,
    td: ({ children }: { children?: React.ReactNode }) => <td style={{ border: "1px solid #c9d3d0", padding: "4px 8px" }}>{children}</td>,
    h1: ({ children }: { children?: React.ReactNode }) => <h1 style={{ fontSize: 16, margin: "10px 0 6px" }}>{children}</h1>,
    h2: ({ children }: { children?: React.ReactNode }) => <h2 style={{ fontSize: 15, margin: "8px 0 5px" }}>{children}</h2>,
    h3: ({ children }: { children?: React.ReactNode }) => <h3 style={{ fontSize: 14, margin: "6px 0 4px" }}>{children}</h3>,
  };
  // conversation-flow renderer: user bubble and its agent reply INTERLEAVED
  // in time order — each turn visibly paired with the response it triggered
  const TurnFlow = ({ turns }: { turns: TurnMsg[] }) => (
    <div>
      {turns.map((m, i) =>
        m.role === "user" ? (
          <div key={i} style={{ display: "flex", justifyContent: "flex-end", margin: "8px 0" }}>
            <div style={{ background: "#e7f0f4", border: "1px solid #bcd8e8", borderRadius: "10px 10px 2px 10px", padding: "8px 12px", maxWidth: "85%", fontSize: 12.5 }}>
              <span style={{ color: "#3f8ec4", fontWeight: 700, fontSize: 10.5, display: "block", marginBottom: 2 }}>👤 {new Date(m.ts).toLocaleTimeString()}</span>
              <span style={{ color: "#17232a", whiteSpace: "pre-wrap" }}>{m.text}</span>
            </div>
          </div>
        ) : m.role === "agent" ? (
          <div key={i} style={{ display: "flex", justifyContent: "flex-start", margin: "8px 0" }}>
            <div style={{ background: "#fff", border: "1px solid #e3e8e6", borderRadius: "2px 10px 10px 10px", padding: "10px 12px", maxWidth: "92%", fontSize: 12 }}>
              <span style={{ color: "#8fa1a7", fontSize: 10.5, display: "block", marginBottom: 4 }}>🤖 {new Date(m.ts).toLocaleTimeString()}</span>
              {renderSegments(segmentStream(m.text))}
            </div>
          </div>
        ) : m.role === "system" ? (
          <div key={i} style={{ display: "flex", justifyContent: "flex-start", margin: "6px 0" }}>
            <div style={{ fontSize: 11.5, color: "#8c6d1f", background: "#fdf6e8", border: "1px solid #f0e0b8", borderRadius: 6, padding: "5px 10px", maxWidth: "85%" }}>
              <span style={{ color: "#c2701c", fontSize: 10.5, display: "block", marginBottom: 2 }}>📮 agent 间消息 {new Date(m.ts).toLocaleTimeString()}</span>
              <span style={{ whiteSpace: "pre-wrap" }}>{m.text}</span>
            </div>
          </div>
        ) : (
          <div key={i} style={{ display: "flex", justifyContent: "flex-start", margin: "6px 0" }}>
            <div style={{ fontSize: 11.5, color: "#8c6d1f", background: "#fdf6e8", borderRadius: 6, padding: "5px 10px" }}>{m.text}</div>
          </div>
        )
      )}

    </div>
  );

  const renderSegments = (segs: Seg[]) => segs.map((seg, i) =>
    seg.kind === "hr" ? (
      <hr key={i} style={{ border: "none", borderTop: "1.5px solid #c9d3d0", margin: "10px 0" }} />
    ) : seg.kind === "text" ? (
      <span key={i} style={{ display: "block", whiteSpace: "pre-wrap", color: "#5a6b72" }}>{seg.text}</span>
    ) : (
      <ReactMarkdown key={i} remarkPlugins={[remarkGfm]} components={MD_COMPONENTS}>{seg.text}</ReactMarkdown>
    )
  );

  return (
    <>
    <style>{`
      @keyframes spin { to { transform: rotate(360deg); } }
      @keyframes breathe { 0%,100% { opacity: 1; box-shadow: 0 0 6px #2d8a4e; } 50% { opacity: 0.35; box-shadow: 0 0 1px #2d8a4e; } }
    `}</style>
    <main style={{ width: "100vw", minHeight: "100vh", padding: "16px 20px", boxSizing: "border-box", fontFamily: "system-ui, -apple-system, sans-serif", color: "#17232a" }}>
      <header style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 18 }}>
        <h1 style={{ fontSize: 21, margin: 0 }}>Multi-Agent Workbench</h1>
        <span title={snapshot?.projectDir ?? ""} style={{ font: "400 11px/1.5 monospace", color: "#8fa1a7", maxWidth: "40vw", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {snapshot?.projectDir ?? "…"}
        </span>
        <span style={{ font: "500 12px/1 monospace", padding: "4px 10px", borderRadius: 999, color: connected ? "#0e7c86" : "#c23b2d", border: `1px solid ${connected ? "#0e7c86" : "#c23b2d"}` }}>
          {connected ? "WS connected" : "WS down"} · seq {lastSeq}
        </span>
      </header>

      {globalError && (
        <div onClick={() => setGlobalError(null)} title="点击关闭"
             style={{ background: "#fdecea", border: "1px solid #c23b2d", borderRadius: 8, padding: "10px 14px", marginBottom: 14, fontSize: 13, color: "#8c2f22", cursor: "pointer" }}>
          ⚠ {globalError}
        </div>
      )}
      {openAgents.length > 0 && (
        <div style={{ fontSize: 12, color: "#5a6b72", marginBottom: 10 }}>
          已开 {openAgents.length}/{MAX_OPEN_PANELS} 窗格 — 点击其他任务卡并排打开（超出 3 个自动关最早的）
        </div>
      )}
      {(() => {
        // D9.3: central approval queue — every pane's pending approval, top of page
        const pending = [...panels.values()].filter((x) => x.pendingApproval);
        if (pending.length === 0) return null;
        return (
          <div style={{ display: "grid", gap: 8, marginBottom: 14 }}>
            {pending.map((x) => (
              <div key={x.agentId} style={{ background: "#fdf3e7", border: "1px solid #e0a458", borderRadius: 8, padding: "10px 14px", fontSize: 13 }}>
                <div style={{ fontWeight: 600, marginBottom: 4 }}>⚠ 审批请求 · {x.agentId} · {x.pendingApproval!.name}</div>
                <div style={{ fontFamily: "monospace", fontSize: 12, marginBottom: 8, wordBreak: "break-all" }}>{x.pendingApproval!.argsPreview}</div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button onClick={() => {
                    (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "APPROVE", agentId: x.agentId, toolCallId: x.pendingApproval!.toolCallId, decision: "allow" }));
                    setPanels((prev) => { const n = new Map(prev); const y = n.get(x.agentId); if (y) n.set(x.agentId, { ...y, pendingApproval: null }); return n; });
                  }} style={{ padding: "5px 12px", borderRadius: 6, border: "none", background: "#2d8a4e", color: "#fff", cursor: "pointer", fontWeight: 600 }}>允许</button>
                  <button onClick={() => {
                    (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "APPROVE", agentId: x.agentId, toolCallId: x.pendingApproval!.toolCallId, decision: "deny" }));
                    setPanels((prev) => { const n = new Map(prev); const y = n.get(x.agentId); if (y) n.set(x.agentId, { ...y, pendingApproval: null }); return n; });
                  }} style={{ padding: "5px 12px", borderRadius: 6, border: "1px solid #c23b2d", background: "#fff", color: "#c23b2d", cursor: "pointer", fontWeight: 600 }}>拒绝</button>
                </div>
              </div>
            ))}
          </div>
        );
      })()}

      <div style={{ display: "flex", gap: 0, alignItems: "stretch" }}>
      {/* ── 左栏: 项目栏 — full-height card, draggable grip on its right edge.
          Collapse it by dragging past the minimum (or it parks itself to a
          sliver); click the sliver to reopen. 新建/导入 live in the header of
          this column, where 「项目」 used to stand alone. ── */}
      {leftCollapsed ? (
        <div onClick={() => setLeftCollapsed(false)} title="展开项目栏"
             style={{ width: 26, flexShrink: 0, background: "#fff", border: "1px solid #d9e0de", borderRadius: 10, display: "flex", alignItems: "center", justifyContent: "center", cursor: "pointer", color: "#5a6b72", fontSize: 13, marginRight: 16 }}>
          »
        </div>
      ) : (
        <aside style={{ width: leftW, flexShrink: 0, position: "sticky", top: 20, height: "calc(100vh - 40px)", background: "#fff", border: "1px solid #d9e0de", borderRadius: 10, padding: 12, fontSize: 12.5, display: "flex", flexDirection: "column", overflow: "hidden" }}>
          <div style={{ fontWeight: 700, marginBottom: 8, color: "#17232a", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            项目
            <span title="收起项目栏" onClick={() => setLeftCollapsed(true)} style={{ cursor: "pointer", color: "#8fa1a7", fontWeight: 400 }}>«</span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginBottom: 10 }}>
            <button onClick={() => { void openProject(undefined, true); }} title="新建项目 — 选一个空目录，自动 git init 并切换"
                    style={{ padding: "6px 4px", borderRadius: 6, fontSize: 11.5, fontWeight: 600, color: "#fff", background: "#0e7c86", border: "none", cursor: "pointer", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>＋ 新建</button>
            <button onClick={() => { void openProject(); }} title="导入已有项目目录"
                    style={{ padding: "6px 4px", borderRadius: 6, fontSize: 11.5, fontWeight: 600, color: "#0e7c86", background: "#fff", border: "1px solid #0e7c86", cursor: "pointer", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>📂 导入</button>
          </div>
          <div style={{ display: "grid", gap: 4, overflowY: "auto", flex: 1, alignContent: "start" /* pills keep content height — default stretch hands the free space to the rows and blows the pill up to column height (live feedback) */ }}>
            {knownProjects.length === 0 && <div style={{ color: "#8fa1a7", fontSize: 11.5 }}>还没有项目 — 用上方按钮导入</div>}
            {knownProjects.map((p) => {
              const cur = p.path === snapshot?.projectDir;
              return (
                <div key={p.path} onClick={() => { if (!cur) void openProject(p.path); }}
                     title={p.path}
                     style={{ padding: "7px 9px", borderRadius: 7, cursor: cur ? "default" : "pointer", fontWeight: cur ? 700 : 500,
                              background: cur ? "#0e7c86" : "transparent", color: cur ? "#fff" : "#17232a",
                              border: cur ? "none" : "1px solid #e3e8e6", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {cur ? "● " : ""}{p.name}
                </div>
              );
            })}
          </div>
        </aside>
      )}
      {/* grip line: drag to resize, double-click to reset */}
      <div onMouseDown={(e) => startColResize("left", e)} onDoubleClick={() => { setLeftW(220); setLeftCollapsed(false); }}
           title="拖动调整栏宽 · 双击复位 · 拖到最左收起"
           style={{ width: 7, flexShrink: 0, cursor: "col-resize", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ width: 3, height: 64, borderRadius: 2, background: "#c9d3d0" }} />
      </div>
      {/* ── 中栏: 主体 ── */}
      <div style={{ flex: 1, minWidth: 0, marginRight: 16 }}>
      {/* ── 常驻 composer（agent 一侧）：空输入=待命 agent ── */}
      <form onSubmit={(e) => { submitGoal(e).then(() => { if (goalRef.current) goalRef.current.value = ""; }); }} style={{ display: "flex", gap: 8, alignItems: "stretch", background: "#fff", padding: 10, borderRadius: 10, border: "1px solid #c9d3d0", marginBottom: 14 }}>
        <textarea
          ref={goalRef}
          placeholder="输入目标（空=待命 agent）… Enter 提交，Shift+Enter 换行"
          style={{ flex: 1, padding: "10px 12px", borderRadius: 8, border: "1px solid #dfe5e3", fontSize: 14, minHeight: 44, resize: "vertical" }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey && !(e.nativeEvent as { isComposing?: boolean }).isComposing) {
              e.preventDefault();
              (e.currentTarget.form as HTMLFormElement | null)?.requestSubmit();
            }
          }}
        />
        <div style={{ display: "grid", gap: 6, alignContent: "stretch" }}>
          <select value={model} onChange={(e) => setModel(e.target.value)} title="模型" style={{ padding: "7px 10px", borderRadius: 8, border: "1px solid #c9d3d0", fontSize: 13, background: "#fff" }}>
            {MODELS.map((x) => (<option key={x.id} value={x.id}>{tierLabels.get(x.id) ?? x.label}</option>))}
          </select>
          <select value={mode} onChange={(e) => setMode(e.target.value)} title="执行模式" style={{ padding: "7px 10px", borderRadius: 8, border: "1px solid #c9d3d0", fontSize: 13, background: "#fff" }}>
            <option value="orchestrate">Master 编排</option>
            {MODES.map((x) => (<option key={x.id} value={x.id}>{x.label}</option>))}
          </select>
        </div>
        <button type="submit" style={{ padding: "10px 20px", borderRadius: 8, border: "none", background: "#0e7c86", color: "#fff", fontWeight: 700, cursor: "pointer", fontSize: 14 }}>＋ New</button>
      </form>
      {tasks.length === 0 && openAgents.length === 0 && <p style={{ color: "#5a6b72" }}>暂无任务——点 ＋ New 提交一个目标。</p>}

      <ul style={{ listStyle: "none", padding: 0, display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 10, alignItems: "start" }}>
        {roots.map((t) => {
          const childTasks = tasks.filter((c) => c.parent_id === t.id);
          const all = [t, ...childTasks];
          return all.map((task) => {
            const taskPanel = task.assignee ? panels.get(task.assignee) : undefined;
            const isOpen = !!task.assignee && openAgents.includes(task.assignee);
            return (
              <li key={task.id} style={{ background: "#fff", border: "1px solid #d9e0de", borderRadius: 10, overflow: "hidden", borderTop: depthOf(task) > 0 ? "3px solid #3f8ec4" : "none" }}>
                <div
                  onClick={() => task.assignee && toggleAgent(task.assignee)}
                  style={{ padding: "10px 16px", borderLeft: `4px solid ${PHASE_LIGHT[agentPhase(task.assignee, task.status, roster, rosterLoaded)].bg}`, cursor: task.assignee ? "pointer" : "default" }}
                >
                  {/* 行1: 灯 + 名字/标题 —— 改名按钮行尾右对齐。one thing per line
                      (live feedback: title-adjacent transparent button read as
                      a stray emoji; make it look like a button, pin it right) */}
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    {(() => {
                      const phase = agentPhase(task.assignee, task.status, roster, rosterLoaded);
                      const light = PHASE_LIGHT[phase];
                      return <span title={light.title} style={{ flexShrink: 0, width: 10, height: 10, borderRadius: "50%", background: light.bg, animation: light.breathe ? "breathe 1.6s ease-in-out infinite" : undefined, border: phase === "queued" ? "1.5px dashed #8fa1a7" : undefined, boxSizing: "border-box" }} />;
                    })()}
                    <span style={{ fontSize: 14, fontWeight: task.parent_id === null ? 600 : 400, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1, minWidth: 0 }}>
                      {task.parent_id ? "└ " : ""}{task.display_name ?? task.title}
                    </span>
                    <button
                      onClick={(e) => { e.stopPropagation(); void renameAgent(task.id); }}
                      title={task.display_name ? `改名（当前: ${task.display_name}）` : "给这个 agent 起个名字"}
                      style={{ flexShrink: 0, border: "1px solid #c9d3d0", background: "#fff", color: "#5a6b72", borderRadius: 6, padding: "1px 8px", cursor: "pointer", fontSize: 11, fontWeight: 600, lineHeight: 1.5 }}
                    >✏️ 改名</button>
                  </div>
                  {/* 行2: agent id — small monospace, never fights the title for width */}
                  {task.assignee && (
                    <div style={{ font: "400 11px/1.6 monospace", color: "#8fa1a7", marginTop: 2 }}>{task.assignee}</div>
                  )}
                  {/* 行3: model — its own line (live feedback: model and phase
                      badge crammed together read as one noisy token string) */}
                  <div title={`task.status: ${task.status}`} style={{ font: "400 11px/1.6 monospace", color: "#8fa1a7", marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {(() => ((task.assignee ? panels.get(task.assignee)?.model : undefined) ?? "…"))()}
                  </div>
                  {/* 行4: phase badge — from agentPhase, the same single state
                      machine that drives the light; task.status as hover tooltip */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 1 }}>
                    <span title={`task.status: ${task.status}`} style={{ font: "500 11.5px/1.6 monospace", color: "#5a6b72" }}>
                      {(() => {
                        const p = task.assignee ? agentPhase(task.assignee, task.status, roster, rosterLoaded) : "queued";
                        return p === "working" ? "🌀 执行中" : p === "idle" ? "💤 等待（可注入）" : p === "awaiting_approval" ? "⏸ 等待人工裁决" : p === "queued" ? "⏳ 排队中" : "⛔ 已停止";
                      })()}
                      {task.attempts > 0 && <span title="review re-dispatch count" style={{ marginLeft: 8, color: STATUS_COLOR[task.status] ?? "#8fa1a7" }}>↻{task.attempts}</span>}
                    </span>
                    {isOpen && task.assignee && (
                      <button onClick={(e) => { e.stopPropagation(); setOpenAgentModal(task.assignee); }} title="半屏大面板（表格/代码可读）"
                              style={{ border: "1px solid #0e7c86", background: "#fff", color: "#0e7c86", borderRadius: 6, padding: "2px 8px", cursor: "pointer", fontSize: 11, fontWeight: 600, flexShrink: 0 }}>
                        ⤢ 展开
                      </button>
                    )}
                  </div>
                </div>
                {isOpen && task.assignee && (() => {
                  const ap = panels.get(task.assignee);
                  const raw = ap?.chunks.length ? ap.chunks.join("") : "";
                  return (
                  <div style={{ borderTop: "1px solid #eef2f1", background: "#fbfcfb" }}>
                    <div ref={(el) => { if (el) streamRefs.current.set(task.assignee!, el); }} style={{ maxHeight: 240, overflowY: "auto", fontSize: 11.5, margin: "6px 14px", whiteSpace: "normal", background: "#f4f6f5", borderRadius: 8, padding: 8, cursor: "pointer" }}
                         onClick={() => setOpenAgentModal(task.assignee)}>
                      <TurnFlow turns={(ap?.turns ?? []).slice(-30)} />
                    </div>
                    <div style={{ display: "flex", gap: 6, padding: "6px 10px 10px" }}>
                      <textarea
                        key={`inj-${task.assignee}`}
                        ref={(el) => { if (el) injectRefs.current.set(task.assignee!, el); }}
                        disabled={agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped"}
                        placeholder={agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" ? "⚠ agent 已停止 — 要继续请提交新任务（顶部）" : agentPhase(task.assignee, task.status, roster, rosterLoaded) === "awaiting_approval" ? "⏸ 等待人工裁决" : "注入提示词（Enter 发送）"}
                        style={{ flex: 1, padding: "6px 8px", borderRadius: 6, border: "1px solid #c9d3d0", fontSize: 12, minHeight: 30, resize: "none" }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" && !e.shiftKey && !(e.nativeEvent as { isComposing?: boolean }).isComposing) {
                            e.preventDefault();
                            const el = injectRefs.current.get(task.assignee!);
                            const draft = el?.value ?? "";
                            if (draft.trim()) {
                              sendInject(task.assignee!, draft);
                              if (el) el.value = "";
                            }
                          }
                        }}
                      />
                      <button disabled={agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped"} onClick={() => { const el = injectRefs.current.get(task.assignee!); const draft = el?.value ?? ""; if (draft.trim()) { sendInject(task.assignee!, draft); if (el) el.value = ""; } }} style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #0e7c86", background: "#fff", color: agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" ? "#8fa1a7" : "#0e7c86", cursor: agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" ? "not-allowed" : "pointer", fontWeight: 600, fontSize: 11.5 }}>注入</button>
                      <button disabled={agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped"} onClick={() => (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "INTERRUPT", agentId: task.assignee }))} title="打断当前回合" style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #c23b2d", background: "#fff", color: agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" ? "#8fa1a7" : "#c23b2d", cursor: agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" ? "not-allowed" : "pointer", fontWeight: 600, fontSize: 11.5 }}>打断</button>
                      {agentPhase(task.assignee, task.status, roster, rosterLoaded) === "stopped" && !task.assignee.startsWith("oc_") && (
                        <button onClick={() => { void wakeAgent(task.assignee!); }} title="用原会话唤醒这个停止的 agent" style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #0e7c86", background: "#fff", color: "#0e7c86", cursor: "pointer", fontWeight: 600, fontSize: 11.5 }}>⚡ 唤醒</button>
                      )}
                      <button onClick={() => { killAgent(task.assignee!); setOpenAgents((prev) => prev.filter((a) => a !== task.assignee)); }} title="关闭并清理该 agent" style={{ padding: "6px 10px", borderRadius: 6, border: "1px solid #8fa1a7", background: "#fff", color: "#5a6b72", cursor: "pointer", fontWeight: 600, fontSize: 11.5 }}>关闭</button>
                    </div>
                  </div>
                  );
                })()}
              </li>
            );
          });
        })}
      </ul>

      </div>{/* 中栏 end */}
      {/* grip line (right panel) */}
      <div onMouseDown={(e) => startColResize("right", e)} onDoubleClick={() => { setRightW(300); setRightCollapsed(false); }}
           title="拖动调整栏宽 · 双击复位 · 拖到最右收起"
           style={{ width: 7, flexShrink: 0, cursor: "col-resize", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ width: 3, height: 64, borderRadius: 2, background: "#c9d3d0" }} />
      </div>
      {/* ── 右栏: 成本计价 — full-height, collapsible by drag like the left ── */}
      {rightCollapsed ? (
        <div onClick={() => setRightCollapsed(false)} title="展开成本面板"
             style={{ width: 26, flexShrink: 0, background: "#fff", border: "1px solid #d9e0de", borderRadius: 10, display: "flex", alignItems: "center", justifyContent: "center", cursor: "pointer", color: "#5a6b72", fontSize: 13 }}>
          «
        </div>
      ) : (
      <aside style={{ width: rightW, flexShrink: 0, position: "sticky", top: 20, height: "calc(100vh - 40px)", background: "#fff", border: "1px solid #d9e0de", borderRadius: 10, padding: 14, fontSize: 12, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ fontWeight: 700, marginBottom: 10, color: "#17232a", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          成本（tokens）
          <span title="收起成本面板" onClick={() => setRightCollapsed(true)} style={{ cursor: "pointer", color: "#8fa1a7", fontWeight: 400 }}>»</span>
        </div>
        <div style={{ overflowY: "auto", flex: 1 }}>
        {snapshot?.totals.length ? (() => {
          const totIn = snapshot!.totals.reduce((s, c) => s + c.input_tokens, 0);
          const totOut = snapshot!.totals.reduce((s, c) => s + c.output_tokens, 0);
          const totCache = snapshot!.totals.reduce((s, c) => s + (c.cache_read ?? 0), 0);
          const totCacheW = snapshot!.totals.reduce((s, c) => s + (c.cache_write ?? 0), 0);
          return (<>
            <div style={{ fontFamily: "monospace", marginBottom: 10, lineHeight: 1.9 }}>
              <div title="本轮新增的未缓存输入（完整上下文 = 新增输入 + 缓存命中）"><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#0e7c86", marginRight: 6 }} />新增输入 <b>{totIn.toLocaleString()}</b></div>
              <div><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#0d3b66", marginRight: 6 }} />输出 <b>{totOut.toLocaleString()}</b></div>
              <div title="历史上下文从缓存读取的部分（含前几轮的 input/output）"><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "repeating-linear-gradient(45deg, #7fb7c4 0 3px, #c9dfe4 3px 6px)", marginRight: 6 }} />缓存命中 <b>{totCache.toLocaleString()}</b>{totIn + totCache > 0 ? `（${Math.round((totCache / (totIn + totCache)) * 100)}%）` : ""}</div>
              <div title="本轮写入缓存供后续轮次读取的 token"><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "repeating-linear-gradient(135deg, #c2701c 0 3px, #f0d9c0 3px 6px)", marginRight: 6 }} />缓存写入 <b>{totCacheW.toLocaleString()}</b></div>
            </div>
            {/* 图例：三段染色各指什么（live feedback: 单色条看不出成分） */}
            <div style={{ display: "flex", gap: 12, fontSize: 10.5, color: "#5a6b72", marginBottom: 8 }}>
              <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 9, height: 9, borderRadius: 2, background: "#0e7c86" }} />输入</span>
              <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 9, height: 9, borderRadius: 2, background: "#0d3b66" }} />输出</span>
              <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 9, height: 9, borderRadius: 2, background: "repeating-linear-gradient(45deg, #7fb7c4 0 3px, #c9dfe4 3px 6px)" }} />缓存</span>
            </div>
            {snapshot!.totals.map((c, i) => {
              // stacked three-segment bar: input blue, output dark blue, cache
              // striped blue — normalized to the task with the largest TOTAL
              // (input+output+cache) so the composition is comparable across rows
              const cr = c.cache_read ?? 0;
              const cw = c.cache_write ?? 0;
              const total = c.input_tokens + c.output_tokens;
              const max = Math.max(...snapshot!.totals.map((x) => x.input_tokens + x.output_tokens + (x.cache_read ?? 0) + (x.cache_write ?? 0)), 1);
              const wIn = (c.input_tokens / max) * 100;
              const wOut = (c.output_tokens / max) * 100;
              const wCr = (cr / max) * 100;
              const wCw = (cw / max) * 100;
              return (
                <div key={i} style={{ marginBottom: 8 }}>
                  <div style={{ fontSize: 11, fontFamily: "monospace", color: "#5a6b72", display: "flex", justifyContent: "space-between" }}>
                    <span title={c.task_id}>{c.task_id.slice(-12)}</span>
                    <span title={`输入 ${c.input_tokens.toLocaleString()} · 输出 ${c.output_tokens.toLocaleString()} · 缓存 ${cr.toLocaleString()}`}>{total > 100000 ? "⚠️ " : ""}{total.toLocaleString()}{cr > 0 ? ` ⚡${cr.toLocaleString()}` : ""}</span>
                  </div>
                  <div style={{ background: "#e8eceb", borderRadius: 4, height: 8, marginTop: 2, display: "flex", overflow: "hidden" }}>
                    <div title={`输入 ${c.input_tokens.toLocaleString()}`} style={{ width: `${wIn}%`, height: "100%", background: "#0e7c86" }} />
                    <div title={`输出 ${c.output_tokens.toLocaleString()}`} style={{ width: `${wOut}%`, height: "100%", background: "#0d3b66" }} />
                    <div title={`缓存命中 ${cr.toLocaleString()}`} style={{ width: `${wCr}%`, height: "100%", background: "repeating-linear-gradient(45deg, #7fb7c4 0 3px, #c9dfe4 3px 6px)" }} />
                    <div title={`缓存写入 ${cw.toLocaleString()}`} style={{ width: `${wCw}%`, height: "100%", background: "repeating-linear-gradient(135deg, #c2701c 0 3px, #f0d9c0 3px 6px)" }} />
                  </div>
                </div>
              );
            })}
          </>);
        })() : <div style={{ color: "#8fa1a7" }}>暂无用量</div>}
        </div>
      </aside>
      )}
      </div>{/* three-column flex end */}

      {openAgentModal && (() => {
        const aid = openAgentModal;
        const ap = panels.get(aid);
        const task = tasks.find((x) => x.assignee === aid);
        const raw = ap?.chunks.length ? ap.chunks.join("") : "";
        return (
          <div onClick={() => setOpenAgentModal(null)} style={{ position: "fixed", inset: 0, background: "rgba(10,16,18,0.55)", zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <div onClick={(e) => e.stopPropagation()}
                 style={{ width: modalW ?? "50vw", minWidth: 520, height: modalH ?? "85vh", position: "relative", background: "#fff", borderRadius: 14, display: "flex", flexDirection: "column", overflow: "hidden", boxShadow: "0 24px 60px rgba(0,0,0,0.35)" }}>
              {/* resize grip: bottom-right corner, drag to resize (width from the
                  right edge, height anchored bottom), double-click resets */}
              <div onMouseDown={(e) => { e.stopPropagation(); startModalResize(e); }}
                   onDoubleClick={() => { setModalW(null); setModalH(null); }}
                   title="拖动调整窗口大小 · 双击复位"
                   style={{ position: "absolute", right: 0, bottom: 0, width: 22, height: 22, cursor: "nwse-resize", display: "flex", alignItems: "center", justifyContent: "center", color: "#8fa1a7", fontSize: 14, zIndex: 5 }}>
                ⤡
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", padding: "10px 16px", borderBottom: "1px solid #e3e8e6", background: "#f4f6f5" }}>
                {/* same stacked-line header as the task card (live feedback:
                    the one-line header crammed name/id/model/status together) */}
                <div>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 14, fontWeight: task?.parent_id === null ? 600 : 500, color: "#17232a" }}>
                    {(() => {
                      const phase = agentPhase(aid, task?.status ?? "pending", roster, rosterLoaded);
                      const light = PHASE_LIGHT[phase];
                      return <span title={light.title} style={{ flexShrink: 0, width: 10, height: 10, borderRadius: "50%", background: light.bg, animation: light.breathe ? "breathe 1.6s ease-in-out infinite" : undefined, border: phase === "queued" ? "1.5px dashed #8fa1a7" : undefined, boxSizing: "border-box" }} />;
                    })()}
                    {task?.display_name ?? task?.title ?? aid}
                  </div>
                  <div style={{ font: "400 11px/1.6 monospace", color: "#8fa1a7", marginTop: 2 }}>{aid}</div>
                  <div title={`task.status: ${task?.status ?? "?"}`} style={{ font: "400 11px/1.6 monospace", color: "#8fa1a7", marginTop: 1 }}>
                    {(task ? (panels.get(aid)?.model ?? "…") : undefined) ?? "…"}
                  </div>
                  <div style={{ font: "500 11.5px/1.6 monospace", color: "#5a6b72", marginTop: 1 }}>
                    {(() => {
                      const p = agentPhase(aid, task?.status ?? "pending", roster, rosterLoaded);
                      return p === "working" ? "🌀 执行中" : p === "idle" ? "💤 等待（可注入）" : p === "awaiting_approval" ? "⏸ 等待人工裁决" : p === "queued" ? "⏳ 排队中" : "⛔ 已停止";
                    })()}
                    {task && task.attempts > 0 ? <span title="review re-dispatch count" style={{ marginLeft: 8 }}>↻{task.attempts}</span> : null}
                  </div>
                </div>
                <button onClick={() => setOpenAgentModal(null)} style={{ border: "none", background: "none", fontSize: 18, cursor: "pointer", color: "#5a6b72" }}>✕</button>
              </div>
              <div ref={(el) => { if (el) streamRefs.current.set(aid, el); }} style={{ flex: 1, overflowY: "auto", padding: 16, fontSize: 13.5, background: "#fbfcfb" }}>
                <TurnFlow turns={ap?.turns ?? []} />
              </div>
              {ap?.pendingApproval && (
                <div style={{ background: "#fdf3e7", borderTop: "1px solid #e0a458", padding: 10, fontSize: 12.5 }}>
                  <b>⚠ {ap.pendingApproval.name}</b>：{ap.pendingApproval.argsPreview.slice(0, 200)}
                  <div style={{ display: "flex", gap: 6, marginTop: 6 }}>
                    <button onClick={() => { (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "APPROVE", agentId: aid, toolCallId: ap.pendingApproval!.toolCallId, decision: "allow" })); setPanels((prev) => { const n = new Map(prev); const y = n.get(aid); if (y) n.set(aid, { ...y, pendingApproval: null }); return n; }); }} style={{ padding: "5px 12px", borderRadius: 6, border: "none", background: "#2d8a4e", color: "#fff", cursor: "pointer", fontWeight: 600 }}>允许</button>
                    <button onClick={() => { (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "APPROVE", agentId: aid, toolCallId: ap.pendingApproval!.toolCallId, decision: "deny" })); setPanels((prev) => { const n = new Map(prev); const y = n.get(aid); if (y) n.set(aid, { ...y, pendingApproval: null }); return n; }); }} style={{ padding: "5px 12px", borderRadius: 6, border: "1px solid #c23b2d", background: "#fff", color: "#c23b2d", cursor: "pointer", fontWeight: 600 }}>拒绝</button>
                  </div>
                </div>
              )}
              <div style={{ display: "flex", gap: 6, padding: 10, borderTop: "1px solid #e3e8e6" }}>
                {(() => {
                  const stopped = agentPhase(aid, task?.status ?? "pending", roster, rosterLoaded) === "stopped";
                  return (<>
                  <textarea
                  key={`injm-${aid}`}
                  ref={(el) => { if (el) injectRefs.current.set(aid, el); }}
                  disabled={stopped}
                  placeholder={stopped ? "⚠ agent 已停止 — 要继续请提交新任务" : agentPhase(aid, task?.status ?? "pending", roster, rosterLoaded) === "awaiting_approval" ? "⏸ 等待人工裁决" : "注入提示词（Enter 发送，Shift+Enter 换行）"}
                  style={{ flex: 1, padding: "8px 10px", borderRadius: 6, border: "1px solid #c9d3d0", fontSize: 13, minHeight: 38, resize: "none", background: stopped ? "#f4f6f5" : "#fff" }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey && !(e.nativeEvent as { isComposing?: boolean }).isComposing) {
                      e.preventDefault();
                      if (stopped) return;
                      const el = injectRefs.current.get(aid);
                      const draft = el?.value ?? "";
                      if (draft.trim()) {
                        sendInject(aid, draft);
                        if (el) el.value = "";
                      }
                    }
                  }}
                />
                <button disabled={stopped} onClick={() => { if (stopped) return; const el = injectRefs.current.get(aid); const draft = el?.value ?? ""; if (draft.trim()) { sendInject(aid, draft); if (el) el.value = ""; } }} style={{ padding: "8px 12px", borderRadius: 6, border: "1px solid #0e7c86", background: "#fff", color: stopped ? "#8fa1a7" : "#0e7c86", cursor: stopped ? "not-allowed" : "pointer", fontWeight: 600, fontSize: 12.5 }}>注入</button>
                <button disabled={stopped} onClick={() => { if (stopped) return; (window as unknown as { __mawWs?: WebSocket }).__mawWs?.send(JSON.stringify({ type: "INTERRUPT", agentId: aid })); }} style={{ padding: "8px 12px", borderRadius: 6, border: "1px solid #c23b2d", background: "#fff", color: stopped ? "#8fa1a7" : "#c23b2d", cursor: stopped ? "not-allowed" : "pointer", fontWeight: 600, fontSize: 12.5 }}>打断</button>
                {stopped && !aid.startsWith("oc_") && (
                  <button onClick={() => { void wakeAgent(aid); }} title="用原会话唤醒这个停止的 agent" style={{ padding: "8px 12px", borderRadius: 6, border: "1px solid #0e7c86", background: "#fff", color: "#0e7c86", cursor: "pointer", fontWeight: 600, fontSize: 12.5 }}>⚡ 唤醒</button>
                )}
                  </>);
                })()}
                <button onClick={() => { killAgent(aid); setOpenAgentModal(null); }} title="关闭并清理该 agent" style={{ padding: "8px 12px", borderRadius: 6, border: "1px solid #8fa1a7", background: "#fff", color: "#5a6b72", cursor: "pointer", fontWeight: 600, fontSize: 12.5 }}>关闭</button>
              </div>
            </div>
          </div>
        );
      })()}
      <WsBridge />
    </main>
    </>
  );
}

/** Exposes the WS to window for the control buttons (context provider at D9). */
function WsBridge() {
  useEffect(() => {
    let ws: WebSocket;
    let retry = 0;
    let torn = false;
    function connect() {
      ws = new WebSocket(GATEWAY.replace(/^http/, "ws"));
      ws.onopen = () => {
        retry = 0;
        ws.send(JSON.stringify({ type: "SUBSCRIBE", agentId: "master" }));
      };
      ws.onclose = () => {
        if (torn) return;
        retry = Math.min(retry + 1, 6);
        setTimeout(connect, Math.min(0.3 * Math.pow(2, retry - 1), 16));
      };
      (window as unknown as { __mawWs?: WebSocket }).__mawWs = ws;
    }
    connect();
    return () => { torn = true; try { ws.close(); } catch { /* gone */ } };
  }, []);
  return null;
}
