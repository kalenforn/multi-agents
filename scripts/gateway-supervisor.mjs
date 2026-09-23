#!/usr/bin/env node
/**
 * gateway-supervisor — dev-mode stand-in for the packaged app's process
 * manager (roadmap-4 "Open Project"): POST /api/project/open exits the
 * gateway after persisting the project pointer, and this loop brings it
 * right back up onto the new project. Same restart path tsx watch uses for
 * file changes; combined, exits and edits both converge to a fresh boot.
 */
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const entry = path.join(here, "..", "packages", "gateway", "src", "index.ts");
const bin = path.join(here, "..", "node_modules", ".bin", "tsx");

// plain tsx, NOT watch: with watch, the open-handler's exit(0) only kills the
// tsx CHILD while the watcher idles waiting for file changes — the restart
// loop below never fires. Plain tsx exits all the way up; same shape the
// packaged app's supervisor will have. (Trade-off: no hot reload on gateway
// edits in dev — restart with ^C after touching gateway code.)
for (;;) {
  const code = await new Promise((resolve) => {
    const child = spawn(bin, [entry], { stdio: "inherit" });
    child.on("exit", resolve);
  });
  console.log(`[supervisor] gateway exited (${code}) — restarting in 0.25s (project pointer pick-up)`);
  await new Promise((r) => setTimeout(r, 250));
}
