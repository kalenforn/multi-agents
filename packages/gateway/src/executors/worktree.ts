/**
 * @maw/gateway/executors/worktree — per-task git worktree isolation.
 * Vibe Kanban / Conductor validated pattern; executors only ever touch their
 * own worktree directory. Path traversal is rejected (security rule R34:
 * user input must never control paths outside the sandbox).
 */

import { mkdirSync } from "node:fs";
import path from "node:path";
import { simpleGit } from "simple-git";
import { config } from "../config.js";

/** All repo operations target the PROJECT's git (roadmap-4: the workbench
 *  serves the project directory, not the gateway's own checkout). */
const projectGit = () => simpleGit({ baseDir: config.projectDir });

// NOT a module-level const: an Open-Project hot swap changes the project
// (and with it the worktree root) at runtime — a frozen snapshot would keep
// writing into the PREVIOUS project's tree. Read through config on every call.
const WORKTREE_ROOT = () => config.worktreeRoot;

export interface WorktreeHandle {
  dir: string;
  branch: string;
}

export async function createWorktree(taskId: string): Promise<WorktreeHandle> {  const branch = `maw/${taskId}`;
  const dir = path.join(WORKTREE_ROOT(), taskId);
  mkdirSync(WORKTREE_ROOT(), { recursive: true });
  const git = projectGit();
  try {
    await git.revparse(["--git-dir"]); // throws outside a repo
  } catch {
    throw new Error("not a git repo — worktree isolation requires git init first");
  }
  // A branch survives its worktree being reaped (KILL cleanup, gateway crash,
  // the .multi-agent/ move) — reattaching to the EXISTING branch instead of
  // failing keeps revivals and re-dispatches alive across restarts.
  const branchExists = await git.branch().then((b) => b.all.includes(branch)).catch(() => false);
  if (branchExists) {
    await git.raw(["worktree", "add", dir, branch]);
  } else {
    await git.raw(["worktree", "add", "-b", branch, dir]);
  }
  return { dir, branch };
}

/** Resolve a model-supplied path inside a worktree; throw on traversal. */
export function safeJoin(worktreeDir: string, relPath: string): string {
  const resolved = path.resolve(worktreeDir, relPath);
  const root = path.resolve(worktreeDir);
  if (resolved !== root && !resolved.startsWith(root + path.sep)) {
    throw new Error(`path escapes worktree: ${relPath}`);
  }
  return resolved;
}

/** Review evidence from the task worktree: content-level — untracked/changed
 *  files in full (bounded), so the reviewer sees what was actually produced,
 *  not just a stat line ("worker run completed" proves nothing). */
export async function diffWorktree(taskId: string, maxChars = 6000, taskWorktreePath?: string | null): Promise<string> {
  try {
    // prefer the task's OWN recorded absolute path — the module-level
    // WORKTREE_ROOT only matches when the process cwd/env align, which is
    // not guaranteed for test/production layouts (D8 r3 silent-crash source)
    const dir = taskWorktreePath ?? path.join(WORKTREE_ROOT(), taskId);
    const git = simpleGit(dir);
    const status = await git.status();
    const changed = [...status.not_added, ...status.modified];
    if (changed.length === 0) return "(no changes in worktree)";
    const parts: string[] = [`# ${changed.length} changed/new file(s)`];
    for (const f of changed.slice(0, 8)) {
      try {
        const isNew = status.not_added.includes(f);
        if (isNew) {
          const content = await readFileUtf8(path.join(dir, f));
          parts.push(`\n## ${f} (new file, ${content.length} chars)\n\`\`\`\n${content.slice(0, maxChars / changed.length)}\n\`\`\``);
        } else {
          const diff = await git.diff(["--", f]);
          parts.push(`\n## ${f} (modified — real diff)\n\`\`\`diff\n${diff.slice(0, maxChars / changed.length)}\n\`\`\``);
        }
      } catch {
        parts.push(`\n## ${f} (binary or unreadable)`);
      }
    }
    return parts.join("\n").slice(0, maxChars);
  } catch {
    return "(worktree evidence unavailable)";
  }
}

async function readFileUtf8(p: string): Promise<string> {
  return await import("node:fs/promises").then((m) => m.readFile(p, "utf-8"));
}

/**
 * Merge a passed-review worktree's branch back into the current branch
 * (squash — one commit per accepted task). Conflict → caller decides.
 */
export async function mergeWorktree(taskId: string): Promise<{ ok: boolean; detail: string; filesChanged?: number }> {
  try {
    const git = projectGit();
    const branchDiff = await git.raw(["diff", "HEAD", `maw/${taskId}`, "--name-only"]).catch(() => "");
    const files = branchDiff.split("\n").filter((f) => f.trim().length > 0);
    if (files.length === 0) return { ok: true, detail: "nothing to merge", filesChanged: 0 };
    await git.raw(["merge", "--squash", `maw/${taskId}`]);
    await git.commit(`task ${taskId}: accepted by review (squash merge from worktree)`);
    return { ok: true, detail: `merged ${files.length} file(s)`, filesChanged: files.length };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

export async function removeWorktree(taskId: string): Promise<void> {
  try {
    await projectGit().raw(["worktree", "remove", "--force", path.join(WORKTREE_ROOT(), taskId)]);
    await projectGit().raw(["branch", "-D", `maw/${taskId}`]);
  } catch {
    // best-effort cleanup; a stale worktree is visible and removable by hand
  }
}
