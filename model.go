package slobudget

import (
	"fmt"
	"time"
)

// MetricReport 是一次指标上报（按服务 + 固定窗口的增量）。
type MetricReport struct {
	Service     string    `json:"service"`      // 服务标识
	WindowStart time.Time `json:"window_start"` // 窗口起点（含）
	WindowEnd   time.Time `json:"window_end"`   // 窗口终点（不含），左闭右开
	TotalDelta  int64     `json:"total_delta"`  // 总请求数增量，必须 >= 0
	FailedDelta int64     `json:"failed_delta"` // 失败请求数增量，必须 >= 0，且不得使累计失败数超过累计总数
	EventID     string    `json:"event_id"`     // 外部事件号，用于幂等
	ReportedAt  time.Time `json:"reported_at"`  // 事件到达（被服务接收）的时间
}

// MetricCorrection 是对历史上报事件的修正。
// 修正本身也是一条不可变事件：不覆盖原事件，而是引用原事件并记录增量。
type MetricCorrection struct {
	EventID        string    `json:"event_id"`         // 本次修正自身的外部事件号
	OriginalEvent  string    `json:"original_event"`   // 被修正的原上报事件号
	TotalDeltaAdj  int64     `json:"total_delta_adj"`  // 总请求数的修正增量，可为负
	FailedDeltaAdj int64     `json:"failed_delta_adj"` // 失败请求数的修正增量，可为负
	Reason         string    `json:"reason"`           // 修正原因
	ReportedAt     time.Time `json:"reported_at"`      // 修正被接收的时间
}

// MetricSnapshot 是某个服务某固定窗口在某一时刻的累计指标。
type MetricSnapshot struct {
	Service        string    `json:"service"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	Total          int64     `json:"total"`
	Failed         int64     `json:"failed"`
	LastReceivedAt time.Time `json:"last_received_at"` // 已纳入快照的最晚事件接收时间
}

// ErrorRate 返回失败率（0~1）；无请求时为 0。
func (m MetricSnapshot) ErrorRate() float64 {
	if m.Total <= 0 {
		return 0
	}
	return float64(m.Failed) / float64(m.Total)
}

// BudgetSnapshot 是门禁创建/重评估时刻冻结的预算快照与决策依据。
// 它覆盖创建时指定的窗口范围，并只纳入当时已经接收的事件；
// 之后到达的任何数据都不会改写它。
type BudgetSnapshot struct {
	Service     string           `json:"service"`
	WindowStart time.Time        `json:"window_start"` // 范围起点（含）
	WindowEnd   time.Time        `json:"window_end"`   // 范围终点（不含）
	FrozenAt    time.Time        `json:"frozen_at"`    // 冻结时刻：仅纳入 ReceivedAt <= FrozenAt 的事件
	Windows     []MetricSnapshot `json:"windows"`
	Total       int64            `json:"total"`
	Failed      int64            `json:"failed"`
	// 纳入快照的所有事件（上报与修正）号，作为决策依据留痕。
	IncludedEvents []string `json:"included_events"`
}

// ErrorRate 返回快照聚合的失败率（0~1）；无请求时为 0。
func (b BudgetSnapshot) ErrorRate() float64 {
	if b.Total <= 0 {
		return 0
	}
	return float64(b.Failed) / float64(b.Total)
}

// Decision 是门禁对一次发布的决定。
type Decision string

const (
	// DecisionAllowed 错误预算仍有剩余，允许发布。
	DecisionAllowed Decision = "allowed"
	// DecisionBlocked 错误预算耗尽（错误率达到/超过阈值），阻止发布。
	DecisionBlocked Decision = "blocked"
)

// GatePolicy 定义门禁策略。
type GatePolicy struct {
	// MaxErrorRate 允许的最大错误率（0~1）。快照失败率严格小于它才允许发布。
	MaxErrorRate float64 `json:"max_error_rate"`
	// AllowReevaluation 是否允许在策略允许时重新评估并生成新版本。
	AllowReevaluation bool `json:"allow_reevaluation"`
}

// Gate 是一个发布门禁，包含若干不可变版本；同一时刻只有一个当前版本。
type Gate struct {
	ID       string         `json:"id"`
	Service  string         `json:"service"`
	Policy   GatePolicy     `json:"policy"`
	Versions []*GateVersion `json:"versions"` // 按版本号升序，全部保留作为历史决策依据
}

// CurrentVersion 返回当前（最新）门禁版本号；不存在时返回 0。
func (g *Gate) CurrentVersion() int {
	return len(g.Versions)
}

// GateVersion 是门禁在某一时刻冻结的不可变决策版本。
type GateVersion struct {
	Number    int            `json:"number"`
	Snapshot  BudgetSnapshot `json:"snapshot"`
	Decision  Decision       `json:"decision"`
	Reason    string         `json:"reason"`
	CreatedAt time.Time      `json:"created_at"`
	// RequestID 是创建/重评估该版本的请求号，用于幂等。
	RequestID string `json:"request_id"`
}

// DecisionInfo 是决定校验接口返回的结果。
type DecisionInfo struct {
	GateID        string
	VersionNumber int
	CurrentNumber int
	Decision      Decision
	Reason        string
	Snapshot      BudgetSnapshot
}

func (d DecisionInfo) String() string {
	return fmt.Sprintf("gate=%s version=%d/%d decision=%s", d.GateID, d.VersionNumber, d.CurrentNumber, d.Decision)
}
