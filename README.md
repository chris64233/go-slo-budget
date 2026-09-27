# go-slo-budget

可追加修正（append-only correction）的 SLO 错误预算服务，并用错误预算控制发布门禁。
纯 Go 标准库实现，无第三方依赖。

开发环境：Go 1.23.0。

- [核心模型](#核心模型)
- [不变量与并发保证](#不变量与并发保证)
- [HTTP 接口](#http-接口)
- [错误类型](#错误类型)
- [持久化与崩溃恢复](#持久化与崩溃恢复)
- [快速开始](#快速开始)
- [测试](#测试)
- [代码结构](#代码结构)

## 核心模型

- **指标上报（Report）**：按服务与固定窗口接收 `total`（总请求增量）与 `failed`
  （失败请求增量）。计数使用 `int64`，累加时做溢出检查。
- **追加修正（Correction）**：历史事件不可变、不可覆盖。任何修正都产生一条新的
  `correction` 事件，以增量（delta，可为负）引用原上报事件号
  （`correctsEventId`），作用于原事件所在窗口的累计。
- **固定窗口**：每个事件携带 `[windowStart, windowEnd)`，语义为**左闭右开**，
  即边界时刻 `windowEnd` 属于下一个窗口。同一服务不同窗口的预算分别累计。
- **预算快照与门禁（Gate）**：创建门禁时冻结策略指定窗口范围内、当时已接收事件
  形成的预算快照（含每个窗口的累计、汇总值、已接收事件高水位 `lastEventSeq`），
  并按阈值签发决定：

  ```
  failed <= errorBudget  → allow（允许发布）
  failed >  errorBudget  → deny （阻止发布）
  ```

  快照与可变状态完全独立，**之后到达的数据不会改写已有版本的决定**。
- **门禁版本（GateVersion）**：策略允许重评估（`allowReevaluation: true`）时，
  可用最新预算重新评估并生成新版本；旧版本被标记 `superseded`。
  **旧版本的批准不能用于当前发布**——校验旧版本会返回 `version_superseded`。
- **幂等**：
  - 上报/修正使用外部事件号（`eventId`，按服务唯一）。同号同内容重放返回原事件
    （响应中 `replayed: true`），不重复计数；**同号异内容返回 `conflict`**。
  - 创建/重评估使用请求号（`requestId`）。同号重放返回同一版本，不产生新版本。

## 不变量与并发保证

服务在一把互斥锁内完成「校验 → 持久化 → 更新内存」，因此：

1. 任何时刻单窗口 `failed <= total`，且二者均非负；
   修正会破坏该不变量（负累计、失败超总数）时整体被拒绝，累计不变。
2. 并发上报不丢失更新；并发修正不会产生负累计；
3. 并发重评估只产生连续的新版本，任何时刻只有一个当前门禁版本；
4. 并发创建（含相同 `requestId`）只产生一个版本。

## HTTP 接口

时间参数统一为 RFC3339（如 `2026-09-01T00:00:00Z`）。

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/services/{service}/events` | 指标上报 |
| POST | `/services/{service}/corrections` | 追加修正 |
| GET  | `/services/{service}/budget?start=&end=` | 预算查询（返回各窗口及汇总） |
| GET  | `/services/{service}/events` | 事件审计列表（上报 + 修正） |
| POST | `/gates` | 创建门禁（冻结快照并签发 v1） |
| POST | `/gates/{gateId}/reevaluate` | 重评估，生成新版本 |
| GET  | `/gates/{gateId}` | 门禁版本历史 |
| GET  | `/gates/{gateId}/versions/{version}/check` | 决定校验 |

### 示例

上报指标：

```bash
curl -sS -XPOST localhost:8080/services/checkout/events \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"evt-001",
    "windowStart":"2026-09-01T00:00:00Z",
    "windowEnd":"2026-09-02T00:00:00Z",
    "total":1000,"failed":8
  }'
```

追加修正（把失败数调减 3，以新事件引用原事件，不覆盖历史）：

```bash
curl -sS -XPOST localhost:8080/services/checkout/corrections \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId":"corr-001",
    "correctsEventId":"evt-001",
    "totalDelta":0,"failedDelta":-3
  }'
```

查询预算：

```bash
curl -sS 'localhost:8080/services/checkout/budget?start=2026-09-01T00:00:00Z&end=2026-09-02T00:00:00Z'
# {"windows":[...],"total":1000,"failed":5}
```

创建门禁（阈值 10，允许重评估）：

```bash
curl -sS -XPOST localhost:8080/gates -H 'Content-Type: application/json' -d '{
  "gateId":"release-42",
  "requestId":"create-req-1",
  "policy":{
    "service":"checkout",
    "windowStart":"2026-09-01T00:00:00Z",
    "windowEnd":"2026-09-02T00:00:00Z",
    "errorBudget":10,
    "allowReevaluation":true
  }
}'
```

校验发布决定：

```bash
curl -sS localhost:8080/gates/release-42/versions/1/check
# 200 + 当前版本且 allow；否则 403 deny / 409 superseded / 404
```

预算恶化后重评估，再以**新版本**校验：

```bash
curl -sS -XPOST localhost:8080/gates/release-42/reevaluate \
  -H 'Content-Type: application/json' -d '{"requestId":"reeval-req-1"}'
```

## 错误类型

所有错误响应形如 `{"code":"...","message":"..."}`，可用 `errors.Is` 在 Go 代码中判别：

| code | HTTP 状态 | 含义 |
| ---- | --------- | ---- |
| `invalid_argument` | 400 | 参数缺失或非法 |
| `invalid_window` | 400 | 窗口不是 `start < end` 的左闭右开区间 |
| `invalid_increment` | 400 | 增量为负，或失败增量大于总增量 |
| `constraint_violated` | 422 | 修正会导致负累计或失败累计超过总累计 |
| `overflow` | 422 | 累计求和超过 int64 范围 |
| `conflict` | 409 | 幂等号已存在但内容不同 |
| `already_exists` | 409 | 门禁已存在，应走重评估而非重复创建 |
| `not_found` | 404 | 事件 / 门禁 / 版本不存在 |
| `reevaluation_not_allowed` | 409 | 策略未开启重评估 |
| `version_superseded` | 409 | 被校验版本已不是当前版本（旧批准不可用） |
| `decision_denied` | 403 | 当前版本决定为阻止发布 |

## 持久化与崩溃恢复

- `WALStore`（`storage.go`）采用只追加日志（`wal.log`）。每次上报、修正、版本签发
  都先写日志并 `fsync`，再更新内存状态。
- 记录帧：`type(1B) | payloadLen(4B) | JSON payload | CRC32(4B)`，
  文件以魔数与格式版本开头。
- 启动时顺序重放：重建事件、窗口累计、门禁版本链（旧版本重新标记为 superseded）
  以及事件号 / 请求号幂等映射。
- 日志尾部残帧（写到一半崩溃）自动忽略并截断；完整帧 CRC 不匹配则报错，
  不会静默接受损坏数据。
- 测试与不需要落盘的场景可使用 `MemoryStore`；也可自行实现 `Store`
  接口接入数据库。

## 快速开始

```bash
go run ./cmd/slobudgetd -addr :8080 -data ./data
```

作为库使用：

```go
store, _ := slobudget.NewWALStore("./data")
svc, _ := slobudget.NewService(context.Background(), store)

svc.Report(ctx, slobudget.ReportInput{
    Service: "checkout", EventID: "evt-1",
    WindowStart: t0, WindowEnd: t1,
    Increment:   slobudget.Increment{Total: 1000, Failed: 8},
})
v, err := svc.CreateGate(ctx, slobudget.CreateGateInput{
    GateID: "release-42", RequestID: "req-1",
    Policy: slobudget.Policy{
        Service: "checkout", WindowStart: t0, WindowEnd: t1,
        ErrorBudget: 10, AllowReevaluation: true,
    },
})
```

## 测试

```bash
go test -race ./...
```

覆盖范围：

- 窗口累计、左闭右开包含关系、非法增量拒绝；
- 事件号幂等重放与同号异内容冲突；
- 追加修正、负累计 / 失败超总数约束、修正幂等与冲突；
- 100 路并发上报不丢更新、80 路并发修正不产生负累计；
- 快照冻结后迟到数据不改决定、超预算阻止、门禁创建 / 重评估幂等、
  旧版本批准失效、30 路并发重评估只有连续的一个当前版本链；
- WAL 全量状态恢复（事件、累计、版本链、两类幂等映射）、
  尾部截断恢复、CRC 损坏检测；
- HTTP 端到端流程与全部错误状态码映射。

## 代码结构

| 文件 | 职责 |
| ---- | ---- |
| `types.go` | 领域类型：增量、事件、预算、策略、快照、门禁版本 |
| `errors.go` | 稳定错误码、统一 `Error` 类型与哨兵错误 |
| `service.go` | 核心服务：上报、修正、预算查询、门禁创建 / 重评估 / 校验 |
| `storage.go` | `Store` 接口、`MemoryStore`、CRC 保护的 `WALStore` |
| `http.go` | JSON HTTP 接口与错误码 → HTTP 状态映射 |
| `cmd/slobudgetd` | 可运行的服务入口（WAL + HTTP） |
