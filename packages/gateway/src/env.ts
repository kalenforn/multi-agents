/**
 * Minimal .env loader (zero-dep). Loads the repo-root .env if present; never
 * overrides existing env vars. Keys stay in process.env only — they never
 * reach the DB, the event stream, or agent prompts (TECH_STACK §3).
 */

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

export function loadDotEnv(startDir: string = process.cwd()): void {
  // walk up from packages/gateway to the repo root looking for .env
  for (const dir of [startDir, path.resolve(startDir, ".."), path.resolve(startDir, "..", "..")]) {
    const file = path.join(dir, ".env");
    if (!existsSync(file)) continue;
    for (const line of readFileSync(file, "utf-8").split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq <= 0) continue;
      const key = trimmed.slice(0, eq).trim();
      let value = trimmed.slice(eq + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      if (!(key in process.env)) process.env[key] = value;
    }
    return;
  }
}
