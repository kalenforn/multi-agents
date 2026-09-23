# Multi-Agent Workbench — 技术栈定稿

> 配套 `plans/multi-agent-workbench-mvp/PLAN.md` 与 `docs/ARCHITECTURE.md`。
> 所有版本 2026-09-10 对 npm registry 实查；升级必须过 `pnpm build && pnpm smoke` 后再动 lockfile。

## 1 · 一览表

| 层 | 选型 | 版本 | 定稿理由 | 被否的备选 |
|---|---|---|---|---|
| 语言/运行时 | TypeScript + Node | Node 22.23.2（本机） | Claude Agent SDK 官方 TS；前后端一种语言；Node 22 LTS | Go+Eino（Go 原生但 TS/AG-UI 生态弱）；Python（Agent SDK 也有 py 版，但前端与 AI SDK 生态在 TS） |
| Monorepo | pnpm workspaces | pnpm 11.22.0（本机） | 本机已装；workspace 协议直连源码 | npm/turbo（MVP 规模不需要 turbo） |
| 前端框架 | Next.js (App Router) + React | next 16.3.4 / react 19.3.0 | 生态默认；客户端组件直接挂 WS hooks | Vite SPA（SSR 无所谓但 Next 16 更省配置）；Electron（MVP 不需要桌面壳） |
| 终端流渲染 | @xterm/xterm + @xterm/headless | 6.0.0 / 6.0.0 | agent 原始流的低成本忠实呈现（MAW 已验证） | 自绘虚拟滚动（重复造轮子） |
| 实时通道 | ws（原生 WebSocket） | 8.21.3 | 零框架依赖；hub 自己写才 200 行 | Socket.IO（多一层协议没必要）；SSE（打断/注入是双向，SSE 只单向） |
| 事件词汇 | **AG-UI 词汇表本地实现** | 无运行时依赖 | 协议语义现成（interrupts/steering/subagent 归属）；类型本地持有防 pre-1.0 绑定（不变量 5） | CopilotKit 运行时（绑 LangGraph 味道太重）；Vercel AI SDK UI 流（单会话视角，无任务树）；纯自发明（孤岛） |
| 数据库 | better-sqlite3 | 13.0.3 | 单文件、WAL、同步 API 天然单写者；崩溃恢复=重放（不变量 2/3） | Postgres+pgmq（v2 迁移路径，接口已预留）；Redis（引入运维面） |
| Claude harness 执行器 | headless CLI `claude -p --output-format stream-json`（D4 已验）；模型芯走网关换 GLM/DSV4，零 Claude 模型成本 | claude code 2.1.263（本机） | harness 全套工具+压缩+SIGINT 打断（966ms 实测）；Agent SDK 0.3.267 为后续升级路径 | Agent SDK 直连（升级档）；手写 loop（D3 已有） |
| GLM/DSV4 执行器 | ai (Vercel AI SDK) + @ai-sdk/openai-compatible | **ai 锁 6.0.280** / openai-compatible 3.0.47 | ToolLoopAgent+needsApproval 自带工具审批；**锁 6 不上 7**：调研基线（v6 特性）已验证，v7 未跑过基准 | LangChain JS（过重）；自写 agent loop（needsApproval/重试细节重复造） |
| Git/worktree | simple-git | 3.36.0 | worktree 隔离的最小依赖 | 手拼 git 命令（转义地狱） |
| 结构化校验 | zod | 4.6.1 | Master 拆解 JSON 与 worker report 的入端校验（D3 起用） | 手写守卫（坏 JSON 分支靠它兜） |
| 死信救援 | gateway 内置 | agent 收件人已死时自动生成继承者（_w/_w2 链）接管任务——消息永不静默丢失 | 丢弃/仅存档 |
| 开发工具 | tsx + tsc | tsx 4.23.13 / **typescript 7.0.2** | tsx 直跑 TS 源（dev）；tsc 严格类型检查（build=typecheck） | vitest（D5 再定，MVP 先脚本断言） |

## 2 · 关键取舍说明

**为什么 build = typecheck**：workspace 内 shared 以 TS 源被 gateway（tsx）与 web（Next transpilePackages）直接消费，无产物分发。所以 shared/gateway 的 `build` 是 `tsc --noEmit`，web 的 `build` 是真正的 `next build`。等出现外部包消费者再改成 emit dist。

**为什么 ai 锁 6**：7.0.97 已是 latest，但调研中对 `ToolLoopAgent`/`needsApproval` 的验证全部落在 v6 线（6.0.280）；D3 接通后如果想上 v7，先在分支跑 smoke 再动。

**HTTP 服务不引框架**：gateway 就 3 个 GET + 1 个 POST，原生 `node:http` 足够；引 Express/Fastify 只为路由是负资产。路由长出验证/中间件需求时再上 Hono（届时一行迁移）。

**错误面约定**：对外错误一律 `{ error: string }` + 恰当 4xx；DB/内部异常不透传（500 只带 request id）。

## 3 · 安全约束（硬红线）

| # | 约束 | 落点 |
|---|---|---|
| 1 | **仅 API-key 计费**，绝不碰 Claude 订阅 OAuth（ToS 红线，违规封号不可逆） | 配置只认 `*_API_KEY` env；无任何订阅 token 字段 |
| 2 | key 只走环境变量；`.env` 永不入库（.gitignore 已含，`.env.example` 提供模板） | 根 `.gitignore` + 启动时校验 |
| 3 | key 绝不写入 `messages`/`events` 表、不进 agent prompt、日志出现 `sk-` 模式即告警 | ✅ 已随 D1 落在 `packages/shared/src/db.ts` 的 `redact()`（smoke 已覆盖） |
| 4 | 任务文本一律当**数据**不当指令（R39）：worker 只从结构化字段拿 spec；破坏性工具走审批；run_command 有 deny-list | executor 工具层（D3） |
| 5 | run_command deny-list 至少含 `rm -rf`、`sudo`、写 `~/.ssh`、`~/.aws` | executor 工具层（D3） |

## 4 · 目录与包边界

```
packages/shared     领域类型 + AG-UI 事件词汇 + SQLite schema/DAO（无 Node API 依赖，web 也能 import）
packages/gateway    单进程：HTTP + WS hub + 编排 + 执行器（依赖 shared）
packages/web        Next.js 前端（依赖 shared，经 WS/HTTP 连 gateway，不 import gateway）
scripts/            smoke / e2e 脚本
docs/               ARCHITECTURE / TECH_STACK / BENCHMARK(待 D13)
plans/              双语实施计划
```

依赖方向单向：`web → shared ← gateway`；web 与 gateway 之间只经 WS/HTTP 协议（shared 里的类型）交互，永不直接 import。

## 5 · 环境变量（.env.example 模板）

| 变量 | 用途 | 必填 |
|---|---|---|
| `ANTHROPIC_API_KEY` | Opus 5（Master + claude worker） | 是（D4 起） |
| `GLM_API_KEY` / `GLM_BASE_URL` | GLM-5.3 OpenAI 兼容端点 | 是（D3 起） |
| `DSV4_API_KEY` / `DSV4_BASE_URL` | DeepSeek V4-Pro OpenAI 兼容端点 | 是（D3 起） |
| `PORT` | gateway 端口（默认 8787） | 否 |
| `NEXT_PUBLIC_GATEWAY_URL` | web 指向 gateway（默认 http://localhost:8787） | 否 |

> base URL 以你拿到的实际端点为准（BLOCKED-ON 项）；模板里填的是默认示例，不是承诺值。


## 6 · 四条 spawn 路径（编排自动化全景）

| # | 路径 | 触发 | 实现 |
|---|---|---|---|
| 1 | Master 拆解 | 提交目标（orchestrate 模式） | D6 decomposeGoal + plantTree |
| 2 | Master 审查增援 | 审查 verdict 带 spawn{title,spec} | D9p4 |
| 3 | Worker 点对点 | send_message 给已有 agent | D7.5 |
| 4 | **任意 agent 自主** | spawn_agent 工具（三档执行器都有） | 护栏：便宜档 only、每 agent ≤2（MAW_SPAWN_CAP）、过 D11 预算闸、树归属可追溯 |

## 7 · 投递语义（消息永不丢失）

收件人 running → 下一轮处理；idle 窗口内 → auto-wake 注入唤醒；已死 → 死信救援
（继承者 agent 接管，_w 链式）；任务 cancelled → 信箱存档。唯一被拒绝的：
预算 blocked 的新任务（可见 verdict 事件，绝不静默）。
