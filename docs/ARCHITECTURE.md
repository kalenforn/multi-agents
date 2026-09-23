# Multi-Agent Workbench — 架构图（v1，待评审定稿）

> 对应 `plans/multi-agent-workbench-mvp/PLAN.md`（路线 C）。本文件是架构的图示化说明，
> 与计划五节内容一一对应；评审通过后即为定稿基线，修改需回写计划并加 Revision。

## 0 · 设计不变量（看图前先看这 5 条）

1. **单 gateway 进程**：WS Hub / 编排 / 执行器管理 / 信箱全在一个 Node 进程内，MVP 不做分布式。
2. **单写者 DB**：所有写经主线程写队列（better-sqlite3 同步调用），任意时刻至多 1 个写者。
3. **事件溯源**：一切 agent 输出先落 `events` 表（单调 `seq`），WS 只是广播层；断线/崩溃都靠重放恢复。
4. **ExecutorAdapter 是唯一扩展点**：新增 OpenCode/Aider/PTY 兜底只加 adapter，不碰总线、UI、编排。
5. **AG-UI 只当词汇表**：TS 类型本地持有，不硬依赖其 pre-1.0 运行时；换协议只改 adapter 层。

## 1 · 图 A：系统组件图

```mermaid
flowchart TB
    subgraph WEB["packages/web · Next.js 前端"]
        MV["Master 全局视图<br/>任务树 · 成本条 · 审批队列"]
        TAB["Worker 页签 × N<br/>StreamView · InjectBox · InterruptBtn · ApprovalModal"]
        HK["useAgentStream · useControl"]
        MV --> HK
        TAB --> HK
    end

    subgraph GW["packages/gateway · 单 Node 进程"]
        HUB["ws/hub.ts<br/>WS Hub · 按 agentId 分发 · REPLAY"]
        ORCH["orchestrator/master.ts<br/>Opus 5 拆解与审查循环"]
        RT["orchestrator/router.ts<br/>模型档位规则 + 预算熔断"]
        LG["orchestrator/ledger.ts<br/>成本台账"]
        EM["ExecutorManager<br/>进程生命周期 · 五态状态机"]
        MB["bus/mailbox.ts<br/>信箱投递 · 队列扫描"]
        CE["executors/claude.ts<br/>Agent SDK"]
        OE["executors/openai-compat.ts<br/>ai@6 ToolLoopAgent"]
    end

    DB[("SQLite · WAL<br/>tasks · messages · events · cost_ledger")]
    WTS["git worktrees/<br/>每任务独立分支"]
    APIS["模型端点<br/>Anthropic API · Opus 5 ｜ GLM-5.3 ｜ DeepSeek V4-Pro"]

    HK <-->|"WS · AG-UI 事件词汇"| HUB
    HUB --> ORCH
    ORCH --> RT
    ORCH --> MB
    ORCH --> EM
    RT --> LG
    EM --> CE
    EM --> OE
    CE --> APIS
    OE --> APIS
    ORCH --> DB
    MB --> DB
    HUB --> DB
    LG --> DB
    CE --> WTS
    OE --> WTS
```

要点：前端只经 1 条 WS 连接拿全部事件（按 `agentId` 分流到页签）；gateway 内组件间不跨进程；
模型端点与 worktree 只被 executor 层触碰（编排层不直接碰文件系统）。

## 2 · 图 B：端到端任务生命周期

```mermaid
sequenceDiagram
    autonumber
    actor H as 人类
    participant UI as Web
    participant Hu as WS Hub
    participant M as Master / Opus 5
    participant R as Router
    participant W as Worker / GLM-5.3 或 DSV4
    participant DB as SQLite

    H->>UI: 提交自然语言目标
    UI->>M: POST /api/tasks
    M->>M: 拆解为任务树 JSON
    M->>R: 复核 model_hint
    R-->>M: 档位决策 glm53 / dsv4 / claude
    M->>DB: tasks 入库 status=queued
    loop 按依赖拓扑 dispatch
        M->>W: spawn (worktree + spec)
        W-->>Hu: 流式事件 (events 表 seq++)
        Hu-->>UI: 实时推送 (agentId 路由到页签)
        W->>DB: report 信箱消息
        M->>DB: 消费 report
        M->>M: Opus 审查 diff
        alt 审查通过
            M->>DB: status=done · 解锁下游
        else 审查不过 且 attempts 少于 2
            M->>W: re-dispatch + feedback
        else 2 轮失败
            M->>DB: status=awaiting_approval
            Hu-->>UI: 推入审批队列
            H->>UI: 人工裁决
        end
    end
    M-->>UI: 汇总 diff · 人工 review 后合并
```

要点：审查循环是唯一的任务状态出口（不变量：每个 running 任务必经
done / queued / failed / awaiting_approval 之一离场）；人不在循环里当搬运工，只在 3 个位置介入——
审批队列、页签干预、最终合并。

## 3 · 图 C：执行器控制面与五态状态机

```mermaid
stateDiagram-v2
    [*] --> idle : spawn
    idle --> thinking : 收到任务 spec
    thinking --> working : 开始工具调用
    working --> awaiting_approval : canUseTool 触发
    awaiting_approval --> working : approve allow
    awaiting_approval --> idle : deny
    working --> idle : 本轮完成
    working --> error : interrupt 超时 2s / 端点 4xx 5xx
    error --> [*] : kill
    idle --> [*] : kill
```

```mermaid
flowchart LR
    UI["Web 控件"] -->|"INJECT"| H["inject<br/>排队的后续消息"]
    UI -->|"INTERRUPT"| I["interrupt<br/>2s 未返回则标 error"]
    UI -->|"APPROVE"| A["canUseTool 决策<br/>allow / deny / edit"]
    H --> E["executor 进程"]
    I --> E
    A --> E
```

要点：两个执行器（claude.ts / openai-compat.ts）实现同一个 `AgentHandle` 接口，
控制三件套语义一致；坏 JSON 时带修复提示重试一次再失败即升级人工。

## 4 · 图 D：数据模型（ER）

```mermaid
erDiagram
    tasks ||--o{ tasks : "parent_id"
    tasks ||--o{ messages : "task_id"
    tasks ||--o{ events : "task_id"
    tasks ||--o{ cost_ledger : "task_id"
    tasks {
        string id PK
        string parent_id FK
        string title
        string spec
        string status
        string assignee
        string model_hint
        string worktree_path
        int attempts
        int created_at
        int updated_at
    }
    messages {
        int id PK
        string from_agent
        string to_agent
        string task_id FK
        string type
        string payload
        int delivered
        int read
        int created_at
    }
    events {
        int seq PK
        string agent_id
        string task_id
        string event_type
        string payload
        int created_at
    }
    cost_ledger {
        int id PK
        string agent_id
        string task_id
        string model
        int input_tokens
        int output_tokens
        int cache_read
        int cache_write
        float est_cost_usd
        int created_at
    }
```

要点：4 张表同库同事务；`events.seq` 单调递增是 WS 重放与崩溃恢复的锚点；
`cost_ledger` 的 per-task 归因直接支撑 D13 基准的成本对比。

## 5 · 图 E：崩溃恢复

```mermaid
flowchart TB
    K["gateway 进程被 kill -9"] --> S1["重启 · 读 tasks / messages / events"]
    S1 --> C{"executor 进程存活?"}
    C -->|"PID + worktree 检测通过"| RA["reattach · 事件续流"]
    C -->|"已死"| RQ["任务回队 queued · 重新 spawn"]
    RA --> V["从 sinceSeq+1 重放<br/>单写者不变量不被破坏"]
    RQ --> V
    V --> OK["判据：0 任务丢失（D12 脚本验证）"]
```

## 6 · 关键决策记录（评审时重点确认）

| # | 决策 | 理由 | 不同意的后果 |
|---|---|---|---|
| 1 | WS 单通道 + AG-UI 词汇表 | 避免自发明协议；类型本地持有防绑定 | 换协议需重写前端事件层 |
| 2 | 单写者 SQLite（WAL） | 14 天内可交付的崩溃恢复最简路径 | 上量后迁移 Postgres+pgmq（接口已预留） |
| 3 | Claude 走 Agent SDK、GLM/DSV4 走 ai@6 | 各自控制面五要素零成本 | Agent SDK 不可用时降级 headless CLI stream-json |
| 4 | MVP 不引入 CCR | 只有 2 条接入路径，自研 gateway 更透明 | 多 CLI 混编时再加 CCR adapter |
| 5 | 仅 API-key 计费 | Anthropic ToS 禁订阅 OAuth（2026-02 起） | 违规封号，不可逆 |
| 6 | 信箱模式 agent 间通信 | 抄 Claude Code mailbox 已验证结构；人机指令同表 | 引入独立 MQ 则 MVP 复杂度超标 |

## 6.5 · D11/D12 补充机制（实现后回写）

- **预算闸**（D11）：每次 dispatch 前 `enforceBudget` 读真实 cost_ledger——
  任务花费 >60% 上限 → 强制降 dsv4；超限/全局耗尽 → 任务置 blocked + 人工通知。
  上限：`MAW_TASK_TOKEN_CAP`（默认 500k）/ `MAW_GLOBAL_TOKEN_CAP`（默认 5M）。
- **崩溃恢复**（D12）：gateway 启动时扫 running 孤儿任务 → 回队 queued →
  dispatchReady 重派（worktree 文件存活复用）。实测 kill -9 → 0 丢失。
- **spawn_agent**（第四路径）：任意 agent 可开新 worker，护栏见 TECH_STACK §6。
- **模型调用超时**：lite 档 `MAW_MODEL_TIMEOUT_MS`（默认 300s）——网关拥塞时
  Worker 不再无限挂起。

## 7 · 评审清单

- [ ] 图 A 组件边界：gateway 单进程是否可接受（vs. 独立 worker 进程池）
- [ ] 图 B 审查循环：自动 re-dispatch 2 轮上限是否合适（OPEN 项 3）
- [ ] 图 C 控制三件套语义是否覆盖你的干预需求
- [ ] 图 D 4 表 schema 字段是否够用（缺什么现在加）
- [ ] 图 E 崩溃恢复判据（0 任务丢失）是否过严/过松
- [ ] OPEN 项 1–3 与 BLOCKED-ON（三家 API key）需你拍板

评审通过后：本文件与计划对升 Revision 2，架构即定稿。
