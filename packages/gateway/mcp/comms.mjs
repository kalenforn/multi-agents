#!/usr/bin/env node
/**
 * comms.mjs — MCP stdio server exposing send_message / check_inbox to a
 * headless Claude Code worker (D7.5 v2, harness executor track).
 *
 * The MCP server process itself NEVER touches the SQLite DB (single-writer
 * invariant): every operation is proxied over HTTP to the gateway's
 * localhost-only internal endpoint, which routes through the same Comms
 * functions the lite executor uses in-process. Claude Code spawns this via
 * --mcp-config; it identifies itself via MAW_AGENT_ID and calls
 * MAW_GATEWAY_URL.
 *
 * JSON-RPC 2.0 over stdio, newline-delimited — the minimum surface Claude
 * Code's MCP client needs (initialize / tools/list / tools/call).
 */

import readline from "node:readline";

const GATEWAY = process.env.MAW_GATEWAY_URL || "http://127.0.0.1:8787";
const AGENT = process.env.MAW_AGENT_ID || "unknown-agent";

const TOOLS = [
  {
    name: "send_message",
    description: "Send a message to the Master agent (report/clarify), the human, or another worker agent (ch_*/oc_* id — direct worker-to-worker communication).",
    inputSchema: {
      type: "object",
      properties: {
        to: { type: "string", description: "'master', 'human', or another agent id (ch_*/oc_*) for direct worker-to-worker messaging" },
        type: { type: "string", enum: ["report", "clarify", "note"] },
        text: { type: "string", maxLength: 4000 },
      },
      required: ["to", "type", "text"],
    },
  },
  {
    name: "spawn_agent",
    description: "Open a NEW worker agent for a self-contained subtask (max 2 per agent). Returns {taskId, agentId} — message its agentId via send_message to coordinate.",
    inputSchema: {
      type: "object",
      properties: {
        title: { type: "string", minLength: 3, maxLength: 200 },
        spec: { type: "string", minLength: 10, maxLength: 4000 },
        tier: { type: "string", enum: ["planner", "worker"], default: "planner" },
      },
      required: ["title", "spec"],
    },
  },
  {
    name: "check_inbox",
    description: "Read messages addressed to you (e.g. reviewer feedback from a previous attempt).",
    inputSchema: { type: "object", properties: {}, required: [] },
  },
  {
    name: "list_agents",
    description: "List the agent roster (id, name, status, revivable). Use it to find an agent's id when the human refers to one by name. NOTE: status is the task lifecycle, NOT liveness — done+revivable = asleep, send_message wakes it; done without a session = gone (lite), resubmit as a new task.",
    inputSchema: { type: "object", properties: {}, required: [] },
  },
];

const rl = readline.createInterface({ input: process.stdin });

function send(msg) {
  process.stdout.write(JSON.stringify(msg) + "\n");
}

async function callGateway(body) {
  try {
    const res = await fetch(`${GATEWAY}/api/internal/mailbox`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ agent: AGENT, ...body }),
    });
    return await res.json();
  } catch (e) {
    return { ok: false, error: `gateway unreachable: ${e.message}` };
  }
}

rl.on("line", (line) => {
  let req;
  try { req = JSON.parse(line); } catch { return; }
  const { id, method, params } = req ?? {};
  if (method === "initialize") {
    send({
      jsonrpc: "2.0", id,
      result: {
        protocolVersion: params?.protocolVersion ?? "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "maw-comms", version: "0.1.0" },
      },
    });
  } else if (method === "tools/list") {
    send({ jsonrpc: "2.0", id, result: { tools: TOOLS } });
  } else if (method === "tools/call") {
    const name = params?.name;
    const args = params?.arguments ?? {};
    if (name === "send_message") {
      callGateway({ op: "send", to: args.to, mailboxType: args.type, text: args.text })
        .then((r) => send({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(r) }] },
        }));
    } else if (name === "spawn_agent") {
      callGateway({ op: "spawn", title: args.title, spec: args.spec, tier: args.tier ?? "planner" })
        .then((r) => send({ jsonrpc: "2.0", id, result: { content: [{ type: "text", text: JSON.stringify(r) }] } }));
    } else if (name === "check_inbox") {
      callGateway({ op: "inbox" })
        .then((r) => send({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(r) }] },
        }));
    } else if (name === "list_agents") {
      callGateway({ op: "listAgents" })
        .then((r) => send({
          jsonrpc: "2.0", id,
          result: { content: [{ type: "text", text: JSON.stringify(r) }] },
        }));
    } else {
      send({ jsonrpc: "2.0", id, error: { code: -32601, message: `unknown tool: ${name}` } });
    }
  } else if (String(method ?? "").startsWith("notifications/")) {
    // notifications get no response (JSON-RPC)
  } else if (id !== undefined) {
    send({ jsonrpc: "2.0", id, error: { code: -32601, message: `unknown method: ${method}` } });
  }
});
