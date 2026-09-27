# go-slo-budget

可追加修正（append-only correction）的 SLO 错误预算服务，并用错误预算控制发布门禁。

开发环境：Go 1.23.0，无第三方依赖。

- 运行测试：`go test ./...`（带竞态检测：`go test -race ./...`）
- 启动服务：`go run ./cmd/server -addr :8080 -state ./data/state.json`

## 核心语义

### 1. 指标上报：精确、幂等、不可越界

- 指标按 **服务 + 固定窗口** 接收增量（`total_delta` / `failed_delta`），全部使用整数，数值精确。
- 窗口为 **左闭右开** 区间 `[window_start, window_end)`。
- 单事件约束：增量非负，且 `failed_delta <= total_delta`。
- 累计不变量：**任何时候累计失败数都不得超过累计总数**；经由修正产生的累计失败越界同样被拒绝（`metric_invariant_violation`）。
- 上报携带 **外部事件号 `event_id`**：
  - 同号同内容重复提交 → 幂等重放，返回已存储事件，不重复累加；
  - 同号异内容 → `idempotency_conflict`；
  - 上报事件与修正事件共享事件号命名空间，跨类型复用同号也算冲突。

### 2. 修正：只能追加，不能覆盖

- 修正（correction）本身是一条新的不可变事件，引用原上报事件号 `original_event`，以**增量**（可负）记录。
- 原上报永不被修改；当前值 = 原事件增量 + 全部修正增量的求和。
- 修正同样以 `event_id` 幂等；应用后若产生**负累计**或**失败数超过总数**，返回 `metric_invariant_violation`，事件不会写入。
- 服务内部以单一互斥锁串行化所有变更并整体原子落盘，因此上报与修正并发时**不会丢失更新、不会出现负累计**。

### 3. 门禁快照：创建即冻结

- 创建门禁时指定服务与窗口范围，并**冻结**一份预算快照（`BudgetSnapshot`）：
  - 只包含创建时刻**已经接收**（`received_at <= frozen_at`）且完全落在指定范围内的固定窗口；
  - 记录聚合后的 total / failed、错误率，以及纳入快照的**全部事件号清单**（决策依据留痕）。
- 每个快照都是不可变版本（`GateVersion`）。**之后到达的数据不会改写既有版本的决定**。
- 判定规则：错误率严格小于 `max_error_rate` 时 `allowed`，否则 `blocked`。

### 4. 重新评估：新版本取代旧版本

- 仅当策略 `allow_reevaluation=true` 时允许重新评估，否则返回 `policy_denied`。
- 重新评估会**追加**一个新版本并冻结当时的新快照；旧版本完整保留可审计，但不再是当前版本。
- **旧版本的批准不得用于当前发布**：用旧版本号校验决定时返回 `obsolete_decision`。
- 创建与重新评估都以 **请求号 `request_id`** 幂等：同号同内容重放返回同一门禁/版本，同号异内容冲突。并发创建同一请求号时，全局只存在一个当前门禁版本。

### 5. 决定校验

- `GET /v1/gates/{id}/decision`（可带 `?version=n`，缺省校验当前版本）：
  - 版本为当前版本且 `allowed` → `200`；
  - 版本为当前版本但 `blocked` → `403 release_blocked`；
  - 指定版本已被取代 → `409 obsolete_decision`（响应体仍携带该版本决策信息）；
  - 门禁/版本不存在 → `404 not_found`。

## HTTP 接口

所有时间字段使用 RFC3339（如 `2026-01-01T00:00:00Z`）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/metrics/reports` | 指标增量上报（事件号幂等） |
| POST | `/v1/metrics/corrections` | 对历史上报追加修正增量 |
| GET | `/v1/budget?service=&start=&end=` | 查询窗口范围内的当前预算 |
| POST | `/v1/gates` | 创建门禁并冻结首版本（请求号幂等） |
| POST | `/v1/gates/{gateID}/reevaluate` | 重新评估，生成新版本（请求号幂等） |
| GET | `/v1/gates/{gateID}` | 查询门禁及其全部版本 |
| GET | `/v1/gates/{gateID}/decision?version=n` | 校验发布决定 |

### 示例

```bash
# 上报：100 个请求中 1 个失败
curl -s -XPOST localhost:8080/v1/metrics/reports -d '{
  "service":"checkout", "event_id":"evt-1",
  "window_start":"2026-01-01T00:00:00Z", "window_end":"2026-01-01T01:00:00Z",
  "total_delta":100, "failed_delta":1
}'

# 修正：原事件多计了 20 个请求、1 个失败
curl -s -XPOST localhost:8080/v1/metrics/corrections -d '{
  "event_id":"cor-1", "original_event":"evt-1",
  "total_delta_adj":-20, "failed_delta_adj":-1, "reason":"double counted"
}'

# 查询预算
curl -s "localhost:8080/v1/budget?service=checkout&start=2026-01-01T00:00:00Z&end=2026-01-01T01:00:00Z"

# 创建门禁：错误率阈值 5%，允许重评估
curl -s -XPOST localhost:8080/v1/gates -d '{
  "request_id":"req-1", "service":"checkout",
  "window_start":"2026-01-01T00:00:00Z", "window_end":"2026-01-01T01:00:00Z",
  "policy":{"max_error_rate":0.05,"allow_reevaluation":true}
}'

# 重新评估（晚到数据已改变预算时产生新版本）
curl -s -XPOST localhost:8080/v1/gates/gate-1/reevaluate -d '{"request_id":"req-2"}'

# 校验当前版本能否发布
curl -i localhost:8080/v1/gates/gate-1/decision
```

## 错误类型

错误响应体形如 `{"error":{"kind":"...","op":"...","message":"..."}}`。

| kind | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `validation_error` | 400 | 入参不合法（空字段、非法窗口、负增量等） |
| `idempotency_conflict` | 409 | 事件号/请求号被复用于不同内容 |
| `not_found` | 404 | 事件、门禁或版本不存在 |
| `metric_invariant_violation` | 422 | 应用增量后出现负累计或失败数超过总数 |
| `policy_denied` | 403 | 策略不允许该操作（如禁止重评估） |
| `obsolete_decision` | 409 | 校验的版本已被新版本取代 |
| `release_blocked` | 403 | 当前版本决定为阻止发布 |

Go 代码中可用 `errors.Is(err, slobudget.ErrConflict)` 等哨兵按类型判断。

## 持久化与并发

- 状态整体序列化为 JSON 落盘，写入采用“临时文件 + 原子重命名”，崩溃不会留下半截文件；重启后事件、修正、门禁全版本与幂等记录均可恢复。
- 变更在服务内串行化，每次变更成功后整体持久化（事件溯源式追加 + 聚合读取）。
- `MemoryStore` 用于测试；`FileStore` 用于单机运行。需要多实例部署时可实现 `Store` 接口替换为带事务的外部存储。

## 代码结构

| 文件 | 内容 |
| --- | --- |
| `model.go` | 上报、修正、快照、门禁、版本等领域模型 |
| `errors.go` | 结构化错误类型与哨兵 |
| `store.go` | `Store` 接口、内存存储与原子文件存储 |
| `service.go` | 核心业务逻辑：幂等、不变量、聚合、快照冻结、门禁版本 |
| `http.go` | HTTP JSON 接口与错误码映射 |
| `cmd/server/main.go` | 服务入口 |
| `*_test.go` | 单元测试、并发测试（`-race`）、HTTP 接口测试与持久化恢复测试 |
