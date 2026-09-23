/**
 * @maw/shared — SQLite schema + DAO
 *
 * Single-writer discipline (architecture invariant 2): all writes in the
 * gateway go through the main-thread write queue and call these synchronous
 * better-sqlite3 methods. WAL mode. Crash recovery = event replay from `seq`.
 *
 * Secrets rule: keys never enter any table here; the payload redaction helper
 * is the enforcement point for the sk-/Bearer patterns (TECH_STACK §3).
 */

import Database from "better-sqlite3";
import type {
  CostRecord,
  CostTotals,
  MailboxMessage,
  MailboxType,
  ModelTier,
  ServerEvent,
  ServerEventType,
  TaskRow,
  TaskStatus,
  TreeSnapshot,
} from "./index.js";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS tasks (
  id            TEXT PRIMARY KEY,
  parent_id     TEXT REFERENCES tasks(id),
  title         TEXT NOT NULL,
  display_name  TEXT,                 -- human-given agent nickname (roadmap-2)
  session_id    TEXT,                 -- persisted claude session id → in-place revival (roadmap-1a)
  spec          TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL CHECK(status IN
    ('pending','queued','running','blocked','awaiting_approval','done','failed','cancelled')),
  assignee      TEXT,
  model_hint    TEXT,                 -- tier hint (planner|worker); validation lives in the router, not the DB
  worktree_path TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  deps          TEXT NOT NULL DEFAULT '[]',  -- JSON array of task ids (D7 review loop)
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS messages (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  from_agent TEXT NOT NULL,
  to_agent   TEXT NOT NULL,
  task_id    TEXT REFERENCES tasks(id),
  type       TEXT NOT NULL CHECK(type IN
    ('dispatch','report','clarify','re-dispatch','control','note','broadcast')),
  payload    TEXT NOT NULL,
  delivered  INTEGER NOT NULL DEFAULT 0,
  read       INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_agent, delivered);
CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id);

CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id   TEXT NOT NULL,
  task_id    TEXT,
  event_type TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_agent_seq ON events(agent_id, seq);

CREATE TABLE IF NOT EXISTS cost_ledger (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  agent_id      TEXT NOT NULL,
  task_id       TEXT NOT NULL,
  model         TEXT NOT NULL,
  input_tokens  INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read    INTEGER NOT NULL DEFAULT 0,
  cache_write   INTEGER NOT NULL DEFAULT 0,
  est_cost_usd  REAL NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cost_task ON cost_ledger(task_id, model);
`;

/** Redact secret-shaped strings before anything is persisted or logged. */
export function redact(text: string): string {
  return text.replace(/sk-[A-Za-z0-9_\-]{8,}/g, "sk-***").replace(/Bearer\s+[A-Za-z0-9._\-]{8,}/g, "Bearer ***");
}

export interface NewTask {
  id: string;
  parent_id?: string | null;
  title: string;
  spec?: string;
  status?: TaskStatus;
  model_hint?: ModelTier | null;
  deps?: string[];
}

export class WorkbenchDb {
  // NOT readonly: rebind() swaps the underlying connection on an Open-Project
  // switch while every holder keeps the same object reference — that identity
  // stability is what lets hundreds of call sites survive a project switch
  // with zero changes (roadmap-4b hot swap).
  private db: Database.Database;

  constructor(path: string) {
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    this.db.exec(SCHEMA);
    migrate(this.db); // additive column migrations (CREATE IF NOT EXISTS never updates)
  }

  /** Open-Project hot swap: point this SAME object at another project's DB.
   *  Safe because no statement is prepared at construction — every DAO method
   *  prepares at call time, so nothing outlives the old connection. */
  rebind(path: string): void {
    const old = this.db;
    this.db = new Database(path);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    this.db.exec(SCHEMA);
    migrate(this.db);
    try { old.close(); } catch { /* already closed */ }
  }

  close(): void {
    this.db.close();
  }

  // ---------- tasks ----------

  insertTask(t: NewTask): TaskRow {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO tasks (id, parent_id, title, spec, status, model_hint, deps, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(t.id, t.parent_id ?? null, t.title, t.spec ?? "", t.status ?? "pending", t.model_hint ?? null, JSON.stringify(t.deps ?? []), now, now);
    return this.getTask(t.id);
  }

  getTask(id: string): TaskRow {
    const row = this.db.prepare(`SELECT * FROM tasks WHERE id = ?`).get(id) as TaskRow | undefined;
    if (!row) throw new Error(`task not found: ${id}`);
    return row;
  }

  listTasks(): TaskRow[] {
    return this.db.prepare(`SELECT * FROM tasks ORDER BY created_at`).all() as TaskRow[];
  }

  children(parentId: string): TaskRow[] {
    return this.db.prepare(`SELECT * FROM tasks WHERE parent_id = ? ORDER BY created_at`).all(parentId) as TaskRow[];
  }

  updateTaskSpec(id: string, spec: string): void {
    this.db.prepare(`UPDATE tasks SET spec = ?, updated_at = ? WHERE id = ?`).run(spec, Date.now(), id);
  }

  /** Rename an agent's display name (roadmap-2). null clears it → falls back to title. */
  setTaskName(id: string, name: string | null): void {
    this.db.prepare(`UPDATE tasks SET display_name = ?, updated_at = ? WHERE id = ?`).run(name, Date.now(), id);
  }

  /** Persist the agent's claude session id the moment the init frame reports
   *  it — in-place revival (roadmap-1a) needs it to outlive the process. */
  setSessionId(id: string, sessionId: string): void {
    this.db.prepare(`UPDATE tasks SET session_id = ?, updated_at = ? WHERE id = ?`).run(sessionId, Date.now(), id);
  }

  setStatus(id: string, status: TaskStatus, patch?: { assignee?: string | null; attempts?: number; worktree_path?: string }): void {
    const sets = [`status = ?`, `updated_at = ?`];
    const args: unknown[] = [status, Date.now()];
    if (patch?.assignee !== undefined) { sets.push(`assignee = ?`); args.push(patch.assignee); }
    if (patch?.attempts !== undefined) { sets.push(`attempts = ?`); args.push(patch.attempts); }
    if (patch?.worktree_path !== undefined) { sets.push(`worktree_path = ?`); args.push(patch.worktree_path); }
    args.push(id);
    this.db.prepare(`UPDATE tasks SET ${sets.join(", ")} WHERE id = ?`).run(...args);
  }

  // ---------- mailbox ----------

  insertMessage(m: {
    from_agent: string;
    to_agent: string;
    task_id?: string | null;
    type: MailboxType;
    payload: unknown;
  }): MailboxMessage {
    const now = Date.now();
    const info = this.db
      .prepare(
        `INSERT INTO messages (from_agent, to_agent, task_id, type, payload, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .run(m.from_agent, m.to_agent, m.task_id ?? null, m.type, redact(JSON.stringify(m.payload)), now);
    return this.getMessage(Number(info.lastInsertRowid));
  }

  getMessage(id: number): MailboxMessage {
    const row = this.db.prepare(`SELECT * FROM messages WHERE id = ?`).get(id) as MailboxMessage | undefined;
    if (!row) throw new Error(`message not found: ${id}`);
    return row;
  }

  /** Undelivered messages for one agent, oldest first (mailbox poll). */
  inbox(agentId: string): MailboxMessage[] {
    return this.db
      .prepare(`SELECT * FROM messages WHERE to_agent = ? AND delivered = 0 ORDER BY id`)
      .all(agentId) as MailboxMessage[];
  }

  /** All messages FROM one agent (review evidence; not marked delivered). */
  messagesFrom(fromAgent: string): MailboxMessage[] {
    return this.db
      .prepare(`SELECT * FROM messages WHERE from_agent = ? ORDER BY id`)
      .all(fromAgent) as MailboxMessage[];
  }

  /** All messages TO one agent (spawn provenance lookup). */
  messagesTo(toAgent: string): MailboxMessage[] {
    return this.db
      .prepare(`SELECT * FROM messages WHERE to_agent = ? ORDER BY id`)
      .all(toAgent) as MailboxMessage[];
  }

  markDelivered(ids: number[]): void {
    if (ids.length === 0) return;
    const stmt = this.db.prepare(`UPDATE messages SET delivered = 1, read = 1 WHERE id = ?`);
    const tx = this.db.transaction((list: number[]) => { for (const id of list) stmt.run(id); });
    tx(ids);
  }

  // ---------- events (append-only; seq is the replay anchor) ----------

  appendEvent(e: {
    agentId: string;
    taskId?: string | null;
    type: ServerEventType;
    payload: unknown;
  }): ServerEvent {
    const now = Date.now();
    const info = this.db
      .prepare(`INSERT INTO events (agent_id, task_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)`)
      .run(e.agentId, e.taskId ?? null, e.type, redact(JSON.stringify(e.payload ?? null)), now);
    const seq = Number(info.lastInsertRowid);
    return { seq, ts: now, agentId: e.agentId, taskId: e.taskId ?? undefined, type: e.type, payload: e.payload };
  }

  eventsSince(sinceSeq: number, agentId?: string): ServerEvent[] {
    const rows = agentId
      ? (this.db.prepare(`SELECT * FROM events WHERE seq > ? AND agent_id = ? ORDER BY seq`).all(sinceSeq, agentId) as EventRow[])
      : (this.db.prepare(`SELECT * FROM events WHERE seq > ? ORDER BY seq`).all(sinceSeq) as EventRow[]);
    return rows.map((r) => ({
      seq: r.seq,
      ts: r.created_at,
      agentId: r.agent_id,
      taskId: r.task_id ?? undefined,
      type: r.event_type as ServerEventType,
      payload: safeParse(r.payload),
    }));
  }

  lastSeq(): number {
    const row = this.db.prepare(`SELECT COALESCE(MAX(seq), 0) AS s FROM events`).get() as { s: number };
    return row.s;
  }

  // ---------- cost ledger ----------

  insertCost(c: Omit<CostRecord, "id" | "created_at">): void {
    this.db
      .prepare(
        `INSERT INTO cost_ledger
         (agent_id, task_id, model, input_tokens, output_tokens, cache_read, cache_write, est_cost_usd, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(c.agent_id, c.task_id, c.model, c.input_tokens, c.output_tokens, c.cache_read, c.cache_write, c.est_cost_usd, Date.now());
  }

  costByTask(): CostTotals[] {
    return this.db
      .prepare(
        `SELECT task_id, model,
                SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens,
                SUM(cache_read) AS cache_read, SUM(cache_write) AS cache_write,
                SUM(est_cost_usd) AS est_cost_usd
         FROM cost_ledger GROUP BY task_id, model ORDER BY task_id`
      )
      .all() as CostTotals[];
  }

  // ---------- snapshots ----------

  treeSnapshot(): TreeSnapshot {
    // projectDir: the DAO layer has no notion of the project — the gateway
    // spreads its own over this at the /api/tree call site. Placeholder
    // keeps the type whole; the response always carries the real value.
    return { tasks: this.listTasks(), totals: this.costByTask(), generated_at: Date.now(), projectDir: "" };
  }
}

interface EventRow {
  seq: number;
  agent_id: string;
  task_id: string | null;
  event_type: string;
  payload: string;
  created_at: number;
}

function safeParse(s: string): unknown {
  try { return JSON.parse(s); } catch { return s; }
}

/**
 * Additive schema migrations: columns added after a DB was first created.
 * CREATE TABLE IF NOT EXISTS does nothing for existing tables — without this,
 * an old dev DB crashes on every insert that uses a new column (learned live:
 * the UI's POST failed as 'invalid JSON body' while every test passed, because
 * tests always start with a fresh DB).
 */
function migrate(db: Database.Database): void {
  const has = (table: string, col: string) =>
    (db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[]).some((c) => c.name === col);
  if (!has("tasks", "deps")) db.exec("ALTER TABLE tasks ADD COLUMN deps TEXT NOT NULL DEFAULT '[]'");
  if (!has("tasks", "display_name")) db.exec("ALTER TABLE tasks ADD COLUMN display_name TEXT");
  if (!has("tasks", "session_id")) db.exec("ALTER TABLE tasks ADD COLUMN session_id TEXT");
  // de-branding migration (repo went public): old DBs constrain model_hint
  // with a CHECK naming internal model tiers ('opus','claude','glm53','dsv4').
  // Recreate without the CHECK and map legacy values to the neutral ids.
  const oldCheck = (db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks'`).get() as { sql?: string }).sql ?? "";
  if (/CHECK\(model_hint IN/i.test(oldCheck)) {
    console.log("[migrate] tasks.model_hint: dropping branded CHECK, mapping legacy tier values");
    db.exec("PRAGMA foreign_keys = OFF");
    const tx = db.transaction(() => {
      db.exec("ALTER TABLE tasks RENAME TO tasks_old");
      // hand-written DDL (regex-extracting it from SCHEMA truncated at the
      // first '; inside a column comment — live lesson); must mirror SCHEMA's
      // tasks table minus the branded CHECK. Fresh installs never reach here.
      db.exec(`CREATE TABLE tasks_new (
  id            TEXT PRIMARY KEY,
  parent_id     TEXT, -- self-FK dropped in the migration copy: SQLite's rename-fixup rewrites REFERENCES across renames; the app layer owns integrity here
  title         TEXT NOT NULL,
  display_name  TEXT,
  session_id    TEXT,
  spec          TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL CHECK(status IN
    ('pending','queued','running','blocked','awaiting_approval','done','failed','cancelled')),
  assignee      TEXT,
  model_hint    TEXT,
  worktree_path TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  deps          TEXT NOT NULL DEFAULT '[]',
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
)`);
      db.exec(`INSERT INTO tasks_new (id, parent_id, title, display_name, session_id, spec, status, assignee, model_hint, worktree_path, attempts, deps, created_at, updated_at)
               SELECT id, parent_id, title, display_name, session_id, spec, status, assignee,
                      CASE model_hint WHEN 'glm53' THEN 'planner' WHEN 'dsv4' THEN 'worker' WHEN 'opus' THEN 'planner' WHEN 'claude' THEN 'planner' ELSE model_hint END,
                      worktree_path, attempts, deps, created_at, updated_at FROM tasks_old`);
      db.exec("DROP TABLE tasks_old");
      db.exec("ALTER TABLE tasks_new RENAME TO tasks");
    });
    tx();
    db.exec("PRAGMA foreign_keys = ON");
    // indexes followed the rename chain; recreate by name (IF NOT EXISTS)
    db.exec("CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_id)");
    db.exec("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)");
  }
}

let counter = 0;
/** Collision-safe task id within this process (prefix + time + counter). */
export function newTaskId(): string {
  counter = (counter + 1) % 1000;
  return `t_${Date.now().toString(36)}_${counter.toString(36).padStart(3, "0")}`;
}

export function openDb(path: string): WorkbenchDb {
  return new WorkbenchDb(path);
}
