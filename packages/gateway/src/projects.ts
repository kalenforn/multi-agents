/**
 * @maw/gateway/projects — the cross-project registry (roadmap-4d sidebar).
 *
 * Lives in the GATEWAY's own tree (not any project's .multi-agent/ — a project
 * registry must outlive and span projects). Bounded list of directories the
 * operator has opened; paths are validated at OPEN time, not trusted here
 * (stale entries — deleted dirs — are filtered on read, never crash a read).
 */

import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import path from "node:path";
import { config } from "./config.js";

const REGISTRY = path.join(import.meta.dirname ?? ".", ".known-projects.json");

export interface KnownProject {
  path: string;
  name: string;
  lastOpenedAt: number;
}

function load(): KnownProject[] {
  try {
    const raw = JSON.parse(readFileSync(REGISTRY, "utf8")) as KnownProject[];
    return Array.isArray(raw) ? raw : [];
  } catch {
    return []; // no registry yet — fresh install
  }
}

/** List known projects in their STABLE registration order (first-imported
 *  first), skipping dirs that vanished on disk. No recency sort — the
 *  sidebar's items must not move when you click between them (live
 *  feedback: only the highlight should change). */
export function listProjects(): KnownProject[] {
  return load().filter((p) => { try { return statSync(p.path).isDirectory(); } catch { return false; } });
}

/** Upsert an opened project — WITHOUT reordering the list: a re-open only
 *  touches its timestamp (position stays; the sidebar must not jump the
 *  current project to the top on every click — live feedback). First open
 *  appends at the front (where a fresh project belongs). */
export function registerProject(dir: string): void {
  const abs = path.resolve(dir);
  const prev = load();
  const existing = prev.find((p) => p.path === abs);
  const next = existing
    ? prev.map((p) => (p.path === abs ? { ...p, lastOpenedAt: Date.now() } : p))
    : [{ path: abs, name: path.basename(abs) || abs, lastOpenedAt: Date.now() } as KnownProject, ...prev];
  // bounded (R25): a registry is a menu, not an archive — 50 covers every
  // realistic machine while keeping the sidebar (and the file) tight
  try {
    writeFileSync(REGISTRY, JSON.stringify(next.slice(0, 50), null, 1));
  } catch (e) {
    console.warn(`[projects] registry write failed:`, e instanceof Error ? e.message : e);
  }
}
