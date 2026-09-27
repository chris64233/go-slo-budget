package slobudget

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Service 是 SLO 错误预算与发布门禁服务。
//
// 并发模型：服务内部以单一互斥锁串行化所有变更，状态整体落盘，
// 因此“上报与修正并发”“创建与重评估并发”都不会丢失更新或产生负累计；
// 同一时刻每个门禁只可能存在一个当前版本。
type Service struct {
	store Store
	now   func() time.Time

	mu    sync.Mutex
	state *State
	// loaded 标记状态是否已从 Store 载入。
	loaded bool
}

// Option 配置 Service。
type Option func(*Service)

// WithClock 注入时间源（主要用于测试冻结时刻）。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// NewService 创建服务。
func NewService(store Store, opts ...Option) *Service {
	s := &Service{store: store, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Service) loadLocked(ctx context.Context) error {
	if s.loaded {
		return nil
	}
	st, err := s.store.Load(ctx)
	if err != nil {
		return err
	}
	s.state = st
	s.loaded = true
	return nil
}

// persistLocked 在持锁状态下落盘。
func (s *Service) persistLocked(ctx context.Context) error {
	if err := s.store.Save(ctx, s.state); err != nil {
		return err
	}
	return nil
}

// ---------------- 指标上报 ----------------

// ReportMetric 接收一次指标增量上报，以外部事件号幂等。
// 同一事件号重复提交且内容一致时按重放处理；内容不一致返回 KindConflict。
func (s *Service) ReportMetric(ctx context.Context, r MetricReport) (MetricReport, error) {
	if r.Service == "" {
		return MetricReport{}, validationErr("ReportMetric", "service is required")
	}
	if r.EventID == "" {
		return MetricReport{}, validationErr("ReportMetric", "event id is required")
	}
	if !r.WindowStart.Before(r.WindowEnd) {
		return MetricReport{}, validationErr("ReportMetric", "window must satisfy start < end (half-open [start, end))")
	}
	if r.TotalDelta < 0 || r.FailedDelta < 0 {
		return MetricReport{}, validationErr("ReportMetric", "deltas must be non-negative")
	}
	if r.FailedDelta > r.TotalDelta {
		return MetricReport{}, validationErr("ReportMetric", "failed delta %d exceeds total delta %d", r.FailedDelta, r.TotalDelta)
	}
	if r.ReportedAt.IsZero() {
		r.ReportedAt = s.now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return MetricReport{}, err
	}

	// 幂等 / 冲突检测（上报与修正共用外部事件号命名空间）。
	if existing, ok := findReport(s.state, r.EventID); ok {
		if !sameReport(existing, r) {
			return MetricReport{}, conflictErr("ReportMetric", "event id %q already used with different content", r.EventID)
		}
		return existing, nil
	}
	if _, ok := findCorrection(s.state, r.EventID); ok {
		return MetricReport{}, conflictErr("ReportMetric", "event id %q already used by a correction", r.EventID)
	}

	// 应用前不变量校验：任何时候累计失败数不得超过累计总数。
	cur := s.windowAggregateLocked(r.Service, r.WindowStart, r.WindowEnd, s.now())
	newTotal := cur.Total + r.TotalDelta
	newFailed := cur.Failed + r.FailedDelta
	if newFailed > newTotal {
		return MetricReport{}, invariantErr("ReportMetric",
			"after report %q: cumulative failed %d would exceed cumulative total %d in window",
			r.EventID, newFailed, newTotal)
	}

	s.state.Reports = append(s.state.Reports, r)
	if err := s.persistLocked(ctx); err != nil {
		s.state.Reports = s.state.Reports[:len(s.state.Reports)-1]
		return MetricReport{}, err
	}
	return r, nil
}

// CorrectMetric 对历史上报做修正。修正不覆盖原事件，而是引用原事件号、
// 以增量形式追加为一条新的不可变事件。校验规则同上报：
// 同事件号幂等、同号异内容冲突；应用后不得出现负累计或失败数超过总数。
func (s *Service) CorrectMetric(ctx context.Context, c MetricCorrection) (MetricCorrection, error) {
	if c.EventID == "" {
		return MetricCorrection{}, validationErr("CorrectMetric", "correction event id is required")
	}
	if c.OriginalEvent == "" {
		return MetricCorrection{}, validationErr("CorrectMetric", "original event id is required")
	}
	if c.EventID == c.OriginalEvent {
		return MetricCorrection{}, validationErr("CorrectMetric", "correction event id must differ from original event id")
	}
	if c.ReportedAt.IsZero() {
		c.ReportedAt = s.now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return MetricCorrection{}, err
	}

	if existing, ok := findCorrection(s.state, c.EventID); ok {
		if !sameCorrection(existing, c) {
			return MetricCorrection{}, conflictErr("CorrectMetric", "event id %q already used with different content", c.EventID)
		}
		return existing, nil
	}
	if _, ok := findReport(s.state, c.EventID); ok {
		return MetricCorrection{}, conflictErr("CorrectMetric", "event id %q already used by a report", c.EventID)
	}

	orig, ok := findReport(s.state, c.OriginalEvent)
	if !ok {
		return MetricCorrection{}, notFoundErr("CorrectMetric", "original report event %q not found", c.OriginalEvent)
	}

	cur := s.windowAggregateLocked(orig.Service, orig.WindowStart, orig.WindowEnd, s.now())
	newTotal := cur.Total + c.TotalDeltaAdj
	newFailed := cur.Failed + c.FailedDeltaAdj
	if newTotal < 0 || newFailed < 0 {
		return MetricCorrection{}, invariantErr("CorrectMetric",
			"correction %q would produce negative cumulative (total=%d failed=%d)",
			c.EventID, newTotal, newFailed)
	}
	if newFailed > newTotal {
		return MetricCorrection{}, invariantErr("CorrectMetric",
			"after correction %q: cumulative failed %d would exceed cumulative total %d",
			c.EventID, newFailed, newTotal)
	}

	s.state.Corrections = append(s.state.Corrections, c)
	if err := s.persistLocked(ctx); err != nil {
		s.state.Corrections = s.state.Corrections[:len(s.state.Corrections)-1]
		return MetricCorrection{}, err
	}
	return c, nil
}

// QueryBudget 查询某服务在窗口范围 [start, end) 内、截至当前已接收全部事件的预算。
func (s *Service) QueryBudget(ctx context.Context, service string, start, end time.Time) (BudgetSnapshot, error) {
	if service == "" {
		return BudgetSnapshot{}, validationErr("QueryBudget", "service is required")
	}
	if !start.Before(end) {
		return BudgetSnapshot{}, validationErr("QueryBudget", "range must satisfy start < end")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return BudgetSnapshot{}, err
	}
	return s.budgetLocked(service, start, end, s.now()), nil
}

// ---------------- 门禁 ----------------

// CreateGateInput 是创建门禁的请求。
type CreateGateInput struct {
	RequestID   string     // 请求号，幂等键（必填）
	Service     string     // 目标服务（必填）
	WindowStart time.Time  // 冻结窗口范围起点（含）
	WindowEnd   time.Time  // 冻结窗口范围终点（不含）
	Policy      GatePolicy // 门禁策略
}

// CreateGate 创建门禁并冻结首版本：快照只包含创建时刻已接收的事件，
// 之后到达的数据不会影响该版本决定。以请求号幂等。
func (s *Service) CreateGate(ctx context.Context, in CreateGateInput) (*Gate, *GateVersion, error) {
	if err := validateGateInput("CreateGate", in); err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return nil, nil, err
	}

	hash := gateInputHash("create", in)
	if rec, ok := s.state.GateRequests[in.RequestID]; ok {
		if rec.Kind != "create" || rec.InputHash != hash {
			return nil, nil, conflictErr("CreateGate", "request id %q reused with different content", in.RequestID)
		}
		g := s.state.Gates[rec.GateID]
		return copyGate(g), copyVersion(g.Versions[rec.Version-1]), nil
	}

	frozenAt := s.now()
	snap := s.budgetLocked(in.Service, in.WindowStart, in.WindowEnd, frozenAt)
	decision, reason := evaluate(snap, in.Policy)

	s.state.GateSeq++
	gate := &Gate{
		ID:      gateID(s.state.GateSeq),
		Service: in.Service,
		Policy:  in.Policy,
	}
	v := &GateVersion{
		Number:    1,
		Snapshot:  snap,
		Decision:  decision,
		Reason:    reason,
		CreatedAt: frozenAt,
		RequestID: in.RequestID,
	}
	gate.Versions = append(gate.Versions, v)
	s.state.Gates[gate.ID] = gate
	s.state.GateRequests[in.RequestID] = GateRequestRecord{
		Kind: "create", GateID: gate.ID, Version: 1, InputHash: hash,
	}
	if err := s.persistLocked(ctx); err != nil {
		s.rollbackNewGateLocked(gate.ID, in.RequestID)
		return nil, nil, err
	}
	return copyGate(gate), copyVersion(v), nil
}

// Reevaluate 在策略允许时对既有门禁重新评估，追加一个新版本并冻结新快照。
// 旧版本完整保留但不再是当前版本，其批准不得用于当前发布。以请求号幂等。
func (s *Service) Reevaluate(ctx context.Context, gateID, requestID string) (*GateVersion, error) {
	if gateID == "" || requestID == "" {
		return nil, validationErr("Reevaluate", "gate id and request id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return nil, err
	}

	gate, ok := s.state.Gates[gateID]
	if !ok {
		return nil, notFoundErr("Reevaluate", "gate %q not found", gateID)
	}

	hash := gateInputHash("reevaluate", CreateGateInput{
		Service: gate.Service, Policy: gate.Policy, RequestID: requestID,
		WindowStart: gate.Versions[0].Snapshot.WindowStart,
		WindowEnd:   gate.Versions[0].Snapshot.WindowEnd,
	})
	if rec, ok := s.state.GateRequests[requestID]; ok {
		if rec.GateID != gateID || rec.Kind != "reevaluate" || rec.InputHash != hash {
			return nil, conflictErr("Reevaluate", "request id %q reused with different content", requestID)
		}
		return copyVersion(gate.Versions[rec.Version-1]), nil
	}

	if !gate.Policy.AllowReevaluation {
		return nil, &Error{Kind: KindPolicyDenied, Op: "Reevaluate",
			Msg: "gate policy does not allow reevaluation"}
	}

	frozenAt := s.now()
	first := gate.Versions[0].Snapshot
	snap := s.budgetLocked(gate.Service, first.WindowStart, first.WindowEnd, frozenAt)
	decision, reason := evaluate(snap, gate.Policy)
	v := &GateVersion{
		Number:    len(gate.Versions) + 1,
		Snapshot:  snap,
		Decision:  decision,
		Reason:    reason,
		CreatedAt: frozenAt,
		RequestID: requestID,
	}
	gate.Versions = append(gate.Versions, v)
	s.state.GateRequests[requestID] = GateRequestRecord{
		Kind: "reevaluate", GateID: gateID, Version: v.Number, InputHash: hash,
	}
	if err := s.persistLocked(ctx); err != nil {
		gate.Versions = gate.Versions[:len(gate.Versions)-1]
		delete(s.state.GateRequests, requestID)
		return nil, err
	}
	return copyVersion(v), nil
}

// GetGate 返回门禁（含全部版本）。
func (s *Service) GetGate(ctx context.Context, id string) (*Gate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return nil, err
	}
	gate, ok := s.state.Gates[id]
	if !ok {
		return nil, notFoundErr("GetGate", "gate %q not found", id)
	}
	return copyGate(gate), nil
}

// ValidateDecision 校验某门禁版本能否用于当前发布：
//   - versionNumber <= 0 时校验当前版本；
//   - 指定的版本不是当前版本时返回 KindObsolete（旧版本批准不得用于当前发布）；
//   - 当前版本决定为 blocked 时返回 KindReleaseBlocked；
//   - 决定为 allowed 时返回完整决策信息。
func (s *Service) ValidateDecision(ctx context.Context, gateID string, versionNumber int) (DecisionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(ctx); err != nil {
		return DecisionInfo{}, err
	}
	gate, ok := s.state.Gates[gateID]
	if !ok {
		return DecisionInfo{}, notFoundErr("ValidateDecision", "gate %q not found", gateID)
	}
	current := gate.CurrentVersion()
	if versionNumber <= 0 {
		versionNumber = current
	}
	if versionNumber < 1 || versionNumber > current {
		return DecisionInfo{}, notFoundErr("ValidateDecision", "gate %q has no version %d", gateID, versionNumber)
	}
	v := gate.Versions[versionNumber-1]
	info := DecisionInfo{
		GateID: gateID, VersionNumber: versionNumber, CurrentNumber: current,
		Decision: v.Decision, Reason: v.Reason, Snapshot: v.Snapshot,
	}
	if versionNumber != current {
		return info, &Error{
			Kind: KindObsolete, Op: "ValidateDecision",
			Msg: "version " + strconv.Itoa(versionNumber) + " is obsolete; current version is " + strconv.Itoa(current),
		}
	}
	if v.Decision == DecisionBlocked {
		return info, &Error{
			Kind: KindReleaseBlocked, Op: "ValidateDecision",
			Msg: v.Reason,
		}
	}
	return info, nil
}

// ---------------- 内部：聚合与快照 ----------------

// windowAggregateLocked 聚合单个固定窗口内截至 asOf 已接收的全部事件。
func (s *Service) windowAggregateLocked(service string, start, end, asOf time.Time) MetricSnapshot {
	m := MetricSnapshot{Service: service, WindowStart: start, WindowEnd: end}
	for i := range s.state.Reports {
		r := &s.state.Reports[i]
		if r.Service != service || !r.WindowStart.Equal(start) || !r.WindowEnd.Equal(end) {
			continue
		}
		if r.ReportedAt.After(asOf) {
			continue
		}
		m.Total += r.TotalDelta
		m.Failed += r.FailedDelta
		if r.ReportedAt.After(m.LastReceivedAt) {
			m.LastReceivedAt = r.ReportedAt
		}
	}
	for i := range s.state.Corrections {
		c := &s.state.Corrections[i]
		if c.ReportedAt.After(asOf) {
			continue
		}
		if orig, ok := findReport(s.state, c.OriginalEvent); ok {
			if orig.Service != service || !orig.WindowStart.Equal(start) || !orig.WindowEnd.Equal(end) {
				continue
			}
			m.Total += c.TotalDeltaAdj
			m.Failed += c.FailedDeltaAdj
			if c.ReportedAt.After(m.LastReceivedAt) {
				m.LastReceivedAt = c.ReportedAt
			}
		}
	}
	return m
}

// budgetLocked 冻结某服务窗口范围 [start, end) 内截至 asOf 的预算快照。
// 只包含完全落在范围内的固定窗口；左闭右开。
func (s *Service) budgetLocked(service string, start, end, asOf time.Time) BudgetSnapshot {
	type winKey struct {
		start time.Time
		end   time.Time
	}
	seen := map[winKey]bool{}
	var keys []winKey

	// 以已接收的原始上报确定窗口集合。
	for i := range s.state.Reports {
		r := &s.state.Reports[i]
		if r.Service != service || r.ReportedAt.After(asOf) {
			continue
		}
		if !windowInRange(r.WindowStart, r.WindowEnd, start, end) {
			continue
		}
		k := winKey{r.WindowStart, r.WindowEnd}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].start.Before(keys[j].start) })

	b := BudgetSnapshot{Service: service, WindowStart: start, WindowEnd: end, FrozenAt: asOf}
	eventSet := map[string]bool{}
	for _, k := range keys {
		w := s.windowAggregateLocked(service, k.start, k.end, asOf)
		b.Windows = append(b.Windows, w)
		b.Total += w.Total
		b.Failed += w.Failed
	}
	// 留痕：纳入快照的全部事件号（上报与修正）。
	for i := range s.state.Reports {
		r := &s.state.Reports[i]
		if r.Service != service || r.ReportedAt.After(asOf) {
			continue
		}
		if windowInRange(r.WindowStart, r.WindowEnd, start, end) {
			eventSet[r.EventID] = true
		}
	}
	for i := range s.state.Corrections {
		c := &s.state.Corrections[i]
		if c.ReportedAt.After(asOf) {
			continue
		}
		if orig, ok := findReport(s.state, c.OriginalEvent); ok &&
			orig.Service == service && windowInRange(orig.WindowStart, orig.WindowEnd, start, end) {
			eventSet[c.EventID] = true
		}
	}
	for id := range eventSet {
		b.IncludedEvents = append(b.IncludedEvents, id)
	}
	sort.Strings(b.IncludedEvents)
	return b
}

// windowInRange 判断固定窗口 [wStart, wEnd) 是否完全包含在范围 [start, end) 内。
func windowInRange(wStart, wEnd, start, end time.Time) bool {
	return !wStart.Before(start) && !wEnd.After(end)
}

func evaluate(b BudgetSnapshot, p GatePolicy) (Decision, string) {
	rate := b.ErrorRate()
	if rate < p.MaxErrorRate {
		return DecisionAllowed, "error rate " + formatRate(rate) + " is below threshold " + formatRate(p.MaxErrorRate)
	}
	return DecisionBlocked, "error rate " + formatRate(rate) + " meets or exceeds threshold " + formatRate(p.MaxErrorRate)
}

func validateGateInput(op string, in CreateGateInput) error {
	switch {
	case in.RequestID == "":
		return validationErr(op, "request id is required")
	case in.Service == "":
		return validationErr(op, "service is required")
	case !in.WindowStart.Before(in.WindowEnd):
		return validationErr(op, "window range must satisfy start < end")
	case in.Policy.MaxErrorRate < 0 || in.Policy.MaxErrorRate > 1:
		return validationErr(op, "max error rate must be within [0, 1]")
	}
	return nil
}

func (s *Service) rollbackNewGateLocked(gateID, requestID string) {
	delete(s.state.Gates, gateID)
	delete(s.state.GateRequests, requestID)
	if s.state.GateSeq > 0 {
		s.state.GateSeq--
	}
}

// ---------------- 小工具 ----------------

func findReport(st *State, id string) (MetricReport, bool) {
	for i := range st.Reports {
		if st.Reports[i].EventID == id {
			return st.Reports[i], true
		}
	}
	return MetricReport{}, false
}

func findCorrection(st *State, id string) (MetricCorrection, bool) {
	for i := range st.Corrections {
		if st.Corrections[i].EventID == id {
			return st.Corrections[i], true
		}
	}
	return MetricCorrection{}, false
}

func sameReport(a, b MetricReport) bool {
	return a.Service == b.Service &&
		a.WindowStart.Equal(b.WindowStart) && a.WindowEnd.Equal(b.WindowEnd) &&
		a.TotalDelta == b.TotalDelta && a.FailedDelta == b.FailedDelta
}

func sameCorrection(a, b MetricCorrection) bool {
	return a.OriginalEvent == b.OriginalEvent &&
		a.TotalDeltaAdj == b.TotalDeltaAdj && a.FailedDeltaAdj == b.FailedDeltaAdj &&
		a.Reason == b.Reason
}

func gateID(n int) string {
	return "gate-" + strconv.Itoa(n)
}

func formatRate(r float64) string {
	return strconv.FormatFloat(r, 'f', 4, 64)
}

// gateInputHash 对请求关键字段做稳定指纹，用于同请求号异内容冲突检测。
func gateInputHash(kind string, in CreateGateInput) string {
	payload := struct {
		Kind         string    `json:"kind"`
		RequestID    string    `json:"request_id"`
		Service      string    `json:"service"`
		WindowStart  time.Time `json:"window_start"`
		WindowEnd    time.Time `json:"window_end"`
		MaxErrorRate float64   `json:"max_error_rate"`
		AllowReeval  bool      `json:"allow_reevaluation"`
	}{
		Kind: kind, RequestID: in.RequestID, Service: in.Service,
		WindowStart: in.WindowStart.UTC(), WindowEnd: in.WindowEnd.UTC(),
		MaxErrorRate: in.Policy.MaxErrorRate, AllowReeval: in.Policy.AllowReevaluation,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func copyGate(g *Gate) *Gate {
	if g == nil {
		return nil
	}
	cp := *g
	cp.Versions = make([]*GateVersion, len(g.Versions))
	for i, v := range g.Versions {
		cp.Versions[i] = copyVersion(v)
	}
	return &cp
}

func copyVersion(v *GateVersion) *GateVersion {
	if v == nil {
		return nil
	}
	cp := *v
	cp.Snapshot.Windows = append([]MetricSnapshot(nil), v.Snapshot.Windows...)
	cp.Snapshot.IncludedEvents = append([]string(nil), v.Snapshot.IncludedEvents...)
	return &cp
}
