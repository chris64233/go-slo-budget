package slobudget

import "time"

// Increment 是一次指标上报或修正携带的增量。
// 所有计数使用 int64，服务在累加时做溢出检查，避免静默回绕。
type Increment struct {
	// Total 是总请求数的增量，必须 >= 0。
	Total int64 `json:"total"`
	// Failed 是失败请求数的增量，必须 >= 0 且 <= Total。
	Failed int64 `json:"failed"`
}

func (i Increment) valid() error {
	if i.Total < 0 || i.Failed < 0 {
		return errf(ErrInvalidIncrement, "增量不能为负 (total=%d failed=%d)", i.Total, i.Failed)
	}
	if i.Failed > i.Total {
		return errf(ErrInvalidIncrement, "失败增量 %d 大于总增量 %d", i.Failed, i.Total)
	}
	return nil
}

// EventKind 区分原始上报与追加修正。
type EventKind string

const (
	// KindReport 原始指标上报。
	KindReport EventKind = "report"
	// KindCorrection 追加修正：以增量形式引用一条已存在的原始事件。
	KindCorrection EventKind = "correction"
)

// Event 是一条已持久化、不可变的指标事件。
type Event struct {
	// Service 是被观测的服务名。
	Service string `json:"service"`
	// EventID 是外部事件号，同一服务下唯一，用于幂等。
	EventID string    `json:"eventId"`
	Kind    EventKind `json:"kind"`
	// WindowStart / WindowEnd 定义固定窗口，语义为左闭右开 [Start, End)。
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
	Increment   Increment `json:"increment"`
	// CorrectsEventID 仅对修正事件有效，指向被修正的原始上报事件号。
	CorrectsEventID string `json:"correctsEventId,omitempty"`
	// Seq 是服务分配的全局单调序号，也是冻结快照时的高水位依据。
	Seq int64 `json:"seq"`
	// CreatedAt 是服务接收并写入该事件的时间。
	CreatedAt time.Time `json:"createdAt"`
}

// Budget 是某个服务在一个固定窗口上的预算累计。
type Budget struct {
	Service     string    `json:"service"`
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
	// Total / Failed 是包含所有原始上报与修正后的当前累计值。
	Total  int64 `json:"total"`
	Failed int64 `json:"failed"`
	// Version 是预算的内部版本号，每追加一条事件单调递增。
	Version int64 `json:"version"`
}

// Remaining 返回剩余错误预算；errorBudget 为允许的失败数上限。
func (b Budget) Remaining(errorBudget int64) int64 {
	return errorBudget - b.Failed
}

// Decision 是门禁对一次发布给出的决定。
type Decision string

const (
	// DecisionAllow 允许发布。
	DecisionAllow Decision = "allow"
	// DecisionDeny 阻止发布。
	DecisionDeny Decision = "deny"
)

// Policy 定义预算窗口范围与门禁阈值。
type Policy struct {
	// Service 是门禁针对的服务。
	Service string `json:"service"`
	// WindowStart / WindowEnd 冻结的评估窗口（左闭右开）。
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
	// ErrorBudget 是窗口内允许的失败请求数上限。
	// 当前失败数 <= ErrorBudget 时允许，否则阻止。
	ErrorBudget int64 `json:"errorBudget"`
	// AllowReevaluation 为 true 时允许在策略上重新评估生成新版本。
	AllowReevaluation bool `json:"allowReevaluation"`
}

// Snapshot 是创建门禁（或重评估）时刻冻结的预算依据，此后不可改变。
type Snapshot struct {
	// Budgets 是评估窗口内各固定窗口的预算快照。
	Budgets []Budget `json:"budgets"`
	// Total / Failed 是所有快照窗口的汇总值。
	Total  int64 `json:"total"`
	Failed int64 `json:"failed"`
	// LastEventSeq 是冻结时已接收事件的最大序号；
	// 之后到达的事件不会进入本快照。
	LastEventSeq int64 `json:"lastEventSeq"`
}

// GateVersion 是门禁的一个版本。每个版本持有自己的冻结快照与决定。
type GateVersion struct {
	GateID   string   `json:"gateId"`
	Version  int64    `json:"version"`
	Policy   Policy   `json:"policy"`
	Snapshot Snapshot `json:"snapshot"`
	Decision Decision `json:"decision"`
	// RequestID 是创建或重评估请求的幂等号。
	RequestID string    `json:"requestId"`
	CreatedAt time.Time `json:"createdAt"`
	// Superseded 表示该版本已被更新的版本取代。
	Superseded bool `json:"superseded"`
}

// Gate 汇总一个门禁的全部版本。
type Gate struct {
	GateID string `json:"gateId"`
	// CurrentVersion 是当前生效版本号（0 表示尚无版本）。
	CurrentVersion int64          `json:"currentVersion"`
	Versions       []*GateVersion `json:"versions"`
	CreatedAt      time.Time      `json:"createdAt"`
}

// Current 返回当前生效版本，不存在时返回 (nil, false)。
func (g *Gate) Current() (*GateVersion, bool) {
	if g.CurrentVersion <= 0 {
		return nil, false
	}
	return g.Versions[g.CurrentVersion-1], true
}

// ReportResult 是幂等上报/修正的返回结果。
type ReportResult struct {
	Event Event `json:"event"`
	// Replayed 为 true 表示该幂等号此前已处理，本次为重放（直接返回原事件）。
	Replayed bool `json:"replayed"`
}

func validateWindow(start, end time.Time) error {
	if !start.Before(end) {
		return errf(ErrInvalidWindow, "窗口必须是左闭右开区间，要求 start < end，得到 [%v, %v)", start, end)
	}
	return nil
}

func add64(a, b int64) (int64, error) {
	if b > 0 && a > (1<<63-1)-b {
		return 0, errf(ErrOverflow, "累计求和溢出: %d + %d", a, b)
	}
	if b < 0 && a < -(1<<63)-b {
		return 0, errf(ErrOverflow, "累计求和溢出: %d + %d", a, b)
	}
	return a + b, nil
}
