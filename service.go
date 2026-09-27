package slobudget

import (
	"context"
	"sort"
	"sync"
	"time"
)

// budgetKey 定位某个服务的一个固定窗口预算。
type budgetKey struct {
	service string
	start   time.Time
	end     time.Time
}

// Service 是 SLO 错误预算与发布门禁的核心服务。
//
// 并发模型：服务内部持有一把互斥锁，所有状态变更（上报、修正、创建、
// 重评估）都在锁内完成“校验 + 落盘 + 改内存”，因此并发请求不会丢失
// 更新、不会产生负累计，也不会出现一个门禁同时存在两个当前版本。
// Store 接口负责在持锁期间把数据同步持久化。
type Service struct {
	store Store
	now   func() time.Time

	mu sync.Mutex

	// events 按接收顺序保存全部不可变事件。
	events []Event
	// eventIndex 按 (service, eventID) 索引事件，支持幂等与修正引用。
	eventIndex map[string]map[string]*Event
	// budgets 是按固定窗口维护的当前累计。
	budgets map[budgetKey]*Budget
	// gates 保存全部门禁及其版本历史。
	gates map[string]*Gate
	// gateRequests 按 (gateID, requestID) 记录已处理的创建/重评估请求。
	gateRequests map[string]map[string]*GateVersion

	lastSeq int64
	started bool
}

// NewService 创建服务并从 store 恢复历史状态。
func NewService(ctx context.Context, store Store) (*Service, error) {
	s := &Service{
		store:        store,
		now:          time.Now,
		eventIndex:   map[string]map[string]*Event{},
		budgets:      map[budgetKey]*Budget{},
		gates:        map[string]*Gate{},
		gateRequests: map[string]map[string]*GateVersion{},
	}
	st, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.restore(st); err != nil {
		return nil, err
	}
	s.started = true
	return s, nil
}

func (s *Service) restore(st *persistedState) error {
	for i := range st.Events {
		ev := st.Events[i]
		s.events = append(s.events, ev)
		idx, ok := s.eventIndex[ev.Service]
		if !ok {
			idx = map[string]*Event{}
			s.eventIndex[ev.Service] = idx
		}
		idx[ev.EventID] = &s.events[len(s.events)-1]
		if ev.Seq > s.lastSeq {
			s.lastSeq = ev.Seq
		}

		// 重放事件以重建预算累计。
		key := budgetKey{ev.Service, ev.WindowStart, ev.WindowEnd}
		b := s.budgets[key]
		if b == nil {
			b = &Budget{Service: ev.Service, WindowStart: ev.WindowStart, WindowEnd: ev.WindowEnd}
			s.budgets[key] = b
		}
		total, err := add64(b.Total, ev.Increment.Total)
		if err != nil {
			return err
		}
		failed, err := add64(b.Failed, ev.Increment.Failed)
		if err != nil {
			return err
		}
		b.Total, b.Failed = total, failed
		b.Version++
	}
	for id, g := range st.Gates {
		s.gates[id] = g
		reqs := map[string]*GateVersion{}
		for _, v := range g.Versions {
			reqs[v.RequestID] = v
		}
		s.gateRequests[id] = reqs
	}
	return nil
}

// ReportInput 是一次原始指标上报的参数。
type ReportInput struct {
	Service     string
	EventID     string
	WindowStart time.Time
	WindowEnd   time.Time
	Increment   Increment
}

// Report 接收一次原始指标增量。
// EventID 是外部事件号：同号同内容重放返回原事件（Replayed=true），
// 同号异内容返回 CodeConflict。失败增量永远不能超过总增量。
func (s *Service) Report(ctx context.Context, in ReportInput) (*ReportResult, error) {
	if in.Service == "" {
		return nil, errf(ErrInvalidArgument, "service 不能为空")
	}
	if in.EventID == "" {
		return nil, errf(ErrInvalidArgument, "eventID 不能为空")
	}
	if err := validateWindow(in.WindowStart, in.WindowEnd); err != nil {
		return nil, err
	}
	if err := in.Increment.valid(); err != nil {
		return nil, err
	}

	ev := Event{
		Service:     in.Service,
		EventID:     in.EventID,
		Kind:        KindReport,
		WindowStart: in.WindowStart,
		WindowEnd:   in.WindowEnd,
		Increment:   in.Increment,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing := s.lookupEvent(in.Service, in.EventID); existing != nil {
		if sameEventContent(existing, &ev) {
			return &ReportResult{Event: *existing, Replayed: true}, nil
		}
		return nil, errf(ErrConflict, "事件号 %q 已存在但内容不同", in.EventID)
	}

	ev.Seq = s.lastSeq + 1
	ev.CreatedAt = s.now()

	// 预算累计更新（原始增量非负，只需检查溢出与失败<=总数，
	// 后者已由 Increment.valid 保证逐次成立、累加后仍成立）。
	key := budgetKey{in.Service, in.WindowStart, in.WindowEnd}
	b := s.budgets[key]
	if b == nil {
		b = &Budget{Service: in.Service, WindowStart: in.WindowStart, WindowEnd: in.WindowEnd}
		s.budgets[key] = b
	}
	total, err := add64(b.Total, in.Increment.Total)
	if err != nil {
		return nil, err
	}
	failed, err := add64(b.Failed, in.Increment.Failed)
	if err != nil {
		return nil, err
	}

	if err := s.store.AppendEvent(ctx, ev); err != nil {
		return nil, err
	}

	s.events = append(s.events, ev)
	s.eventIndexFor(in.Service)[in.EventID] = &s.events[len(s.events)-1]
	b.Total, b.Failed, b.Version = total, failed, b.Version+1
	s.lastSeq = ev.Seq

	return &ReportResult{Event: ev}, nil
}

// CorrectionInput 是一次追加修正的参数。
type CorrectionInput struct {
	Service string
	// EventID 是本次修正自身的幂等事件号（不能与任何既有事件重复）。
	EventID string
	// CorrectsEventID 引用被修正的原始上报事件号。
	CorrectsEventID string
	// TotalDelta / FailedDelta 是相对原始累计的增量，允许为负，
	// 但修正后窗口累计必须满足 total>=0、failed>=0、failed<=total。
	TotalDelta  int64
	FailedDelta int64
}

// Correct 以追加增量的方式修正一条原始上报。
// 修正绝不覆盖历史：它产生一条 KindCorrection 新事件并作用在
// 原事件所在的固定窗口上。
func (s *Service) Correct(ctx context.Context, in CorrectionInput) (*ReportResult, error) {
	if in.Service == "" {
		return nil, errf(ErrInvalidArgument, "service 不能为空")
	}
	if in.EventID == "" {
		return nil, errf(ErrInvalidArgument, "修正事件号不能为空")
	}
	if in.CorrectsEventID == "" {
		return nil, errf(ErrInvalidArgument, "必须引用被修正的原事件号")
	}
	if in.EventID == in.CorrectsEventID {
		return nil, errf(ErrInvalidArgument, "修正事件号不能与被修正事件号相同")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target := s.lookupEvent(in.Service, in.CorrectsEventID)
	if target == nil {
		return nil, errf(ErrNotFound, "被修正的原始事件 %q 不存在", in.CorrectsEventID)
	}
	if target.Kind != KindReport {
		return nil, errf(ErrInvalidArgument, "被引用事件 %q 不是原始上报，修正只能引用原始事件", in.CorrectsEventID)
	}
	if existing := s.lookupEvent(in.Service, in.EventID); existing != nil {
		candidate := Event{
			Service:         in.Service,
			EventID:         in.EventID,
			Kind:            KindCorrection,
			WindowStart:     target.WindowStart,
			WindowEnd:       target.WindowEnd,
			CorrectsEventID: in.CorrectsEventID,
			Increment:       Increment{Total: in.TotalDelta, Failed: in.FailedDelta},
		}
		if sameEventContent(existing, &candidate) {
			return &ReportResult{Event: *existing, Replayed: true}, nil
		}
		return nil, errf(ErrConflict, "事件号 %q 已存在但内容不同", in.EventID)
	}

	key := budgetKey{in.Service, target.WindowStart, target.WindowEnd}
	b := s.budgets[key] // 原始事件存在，预算必然存在
	newTotal, err := add64(b.Total, in.TotalDelta)
	if err != nil {
		return nil, err
	}
	newFailed, err := add64(b.Failed, in.FailedDelta)
	if err != nil {
		return nil, err
	}
	if err := validateResultingTotals(newTotal, newFailed); err != nil {
		return nil, err
	}

	ev := Event{
		Service:         in.Service,
		EventID:         in.EventID,
		Kind:            KindCorrection,
		WindowStart:     target.WindowStart,
		WindowEnd:       target.WindowEnd,
		Increment:       Increment{Total: in.TotalDelta, Failed: in.FailedDelta},
		CorrectsEventID: in.CorrectsEventID,
		Seq:             s.lastSeq + 1,
		CreatedAt:       s.now(),
	}

	if err := s.store.AppendEvent(ctx, ev); err != nil {
		return nil, err
	}

	s.events = append(s.events, ev)
	s.eventIndexFor(in.Service)[in.EventID] = &s.events[len(s.events)-1]
	b.Total, b.Failed, b.Version = newTotal, newFailed, b.Version+1
	s.lastSeq = ev.Seq

	return &ReportResult{Event: ev}, nil
}

// BudgetView 是预算查询结果。
type BudgetView struct {
	// Windows 是范围内按开始时间排序的各固定窗口预算。
	Windows []Budget `json:"windows"`
	// Total / Failed 是范围内所有窗口的汇总。
	Total  int64 `json:"total"`
	Failed int64 `json:"failed"`
}

// QueryBudget 查询某服务在 [start, end) 范围内的固定窗口预算及汇总。
// 只返回完全包含在该范围内的固定窗口；窗口语义为左闭右开。
func (s *Service) QueryBudget(start, end time.Time, service string) (*BudgetView, error) {
	if service == "" {
		return nil, errf(ErrInvalidArgument, "service 不能为空")
	}
	if err := validateWindow(start, end); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	view := &BudgetView{}
	for key, b := range s.budgets {
		if key.service != service {
			continue
		}
		if !key.start.Before(start) && !key.end.After(end) {
			view.Windows = append(view.Windows, *b)
		}
	}
	sort.Slice(view.Windows, func(i, j int) bool {
		return view.Windows[i].WindowStart.Before(view.Windows[j].WindowStart)
	})
	for _, b := range view.Windows {
		view.Total += b.Total
		view.Failed += b.Failed
	}
	return view, nil
}

// CreateGateInput 是创建发布门禁的参数。
type CreateGateInput struct {
	GateID    string
	RequestID string
	Policy    Policy
}

// CreateGate 冻结策略指定窗口范围、当时已接收事件形成的预算快照，
// 并按阈值签发第一个门禁版本。此后到达的数据不会改变该版本的决定。
//
// RequestID 用于幂等：同一 (GateID, RequestID) 重放返回同一版本；
// 不同 RequestID 重复创建同一 GateID 返回 CodeAlreadyExists。
func (s *Service) CreateGate(ctx context.Context, in CreateGateInput) (*GateVersion, error) {
	if in.GateID == "" {
		return nil, errf(ErrInvalidArgument, "gateID 不能为空")
	}
	if in.RequestID == "" {
		return nil, errf(ErrInvalidArgument, "requestID 不能为空")
	}
	if err := validatePolicy(in.Policy); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if reqs := s.gateRequests[in.GateID]; reqs != nil {
		if v := reqs[in.RequestID]; v != nil {
			return cloneGateVersion(v), nil
		}
	}
	if _, exists := s.gates[in.GateID]; exists {
		return nil, errf(ErrAlreadyExists, "门禁 %q 已存在，请使用重评估生成新版本", in.GateID)
	}

	snap := s.freezeSnapshot(in.Policy)
	v := &GateVersion{
		GateID:    in.GateID,
		Version:   1,
		Policy:    in.Policy,
		Snapshot:  snap,
		Decision:  decide(snap.Failed, in.Policy.ErrorBudget),
		RequestID: in.RequestID,
		CreatedAt: s.now(),
	}

	if err := s.store.AppendGateVersion(ctx, in.GateID, true, *v); err != nil {
		return nil, err
	}
	s.installVersion(in.GateID, v)
	return cloneGateVersion(v), nil
}

// ReevaluateGate 在策略允许时用最新预算重新评估，生成新的当前版本。
// 旧版本被标记为 Superseded，其批准不能再用于当前发布。
// RequestID 同样提供幂等；整个过程在锁内完成，并发重评估也只会有
// 一个新当前版本。
func (s *Service) ReevaluateGate(ctx context.Context, gateID, requestID string) (*GateVersion, error) {
	if gateID == "" {
		return nil, errf(ErrInvalidArgument, "gateID 不能为空")
	}
	if requestID == "" {
		return nil, errf(ErrInvalidArgument, "requestID 不能为空")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.gates[gateID]
	if !ok {
		return nil, errf(ErrNotFound, "门禁 %q 不存在", gateID)
	}
	if v := s.gateRequests[gateID][requestID]; v != nil {
		return cloneGateVersion(v), nil
	}
	if !g.Versions[g.CurrentVersion-1].Policy.AllowReevaluation {
		return nil, errf(ErrReevaluationNotAllowed, "门禁 %q 的策略不允许重新评估", gateID)
	}

	policy := g.Versions[g.CurrentVersion-1].Policy
	snap := s.freezeSnapshot(policy)
	v := &GateVersion{
		GateID:    gateID,
		Version:   g.CurrentVersion + 1,
		Policy:    policy,
		Snapshot:  snap,
		Decision:  decide(snap.Failed, policy.ErrorBudget),
		RequestID: requestID,
		CreatedAt: s.now(),
	}

	if err := s.store.AppendGateVersion(ctx, gateID, false, *v); err != nil {
		return nil, err
	}
	s.installVersion(gateID, v)
	return cloneGateVersion(v), nil
}

// CheckDecision 校验某门禁某版本当前是否可用于发布：
//   - 门禁或版本不存在：CodeNotFound
//   - 版本已被新版本取代：CodeVersionSuperseded（旧批准不得用于当前发布）
//   - 当前版本决定为阻止：CodeDecisionDenied
//   - 当前版本决定为允许：返回该版本（含冻结的决策依据快照）
func (s *Service) CheckDecision(gateID string, version int64) (*GateVersion, error) {
	if gateID == "" {
		return nil, errf(ErrInvalidArgument, "gateID 不能为空")
	}
	if version <= 0 {
		return nil, errf(ErrInvalidArgument, "版本号必须为正数，得到 %d", version)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.gates[gateID]
	if !ok {
		return nil, errf(ErrNotFound, "门禁 %q 不存在", gateID)
	}
	if version > g.CurrentVersion {
		return nil, errf(ErrNotFound, "门禁 %q 不存在版本 %d", gateID, version)
	}
	v := g.Versions[version-1]
	if version != g.CurrentVersion {
		return nil, errf(ErrVersionSuperseded, "版本 %d 已被当前版本 %d 取代，旧批准不可用", version, g.CurrentVersion)
	}
	if v.Decision != DecisionAllow {
		return nil, errf(ErrDecisionDenied, "门禁 %q 当前版本 %d 决定为阻止（失败 %d > 预算 %d）",
			gateID, version, v.Snapshot.Failed, v.Policy.ErrorBudget)
	}
	return cloneGateVersion(v), nil
}

// GetEvent 返回一条已接收事件（决策审计用），包含其全部修正链信息。
func (s *Service) GetEvent(service, eventID string) (*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := s.lookupEvent(service, eventID)
	if ev == nil {
		return nil, errf(ErrNotFound, "事件 %q 不存在", eventID)
	}
	cp := *ev
	return &cp, nil
}

// ListEvents 返回某服务按接收顺序排列的全部事件（原始上报与修正），
// 用于审计累计与决策依据。
func (s *Service) ListEvents(service string) ([]Event, error) {
	if service == "" {
		return nil, errf(ErrInvalidArgument, "service 不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, 0, len(s.events))
	for _, ev := range s.events {
		if ev.Service == service {
			out = append(out, ev)
		}
	}
	return out, nil
}

// GetGate 返回门禁的完整版本历史（只读副本）。
func (s *Service) GetGate(gateID string) (*Gate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.gates[gateID]
	if !ok {
		return nil, errf(ErrNotFound, "门禁 %q 不存在", gateID)
	}
	return cloneGate(g), nil
}

// ---- 内部辅助（调用方须持有 s.mu）---------------------------------------

func (s *Service) eventIndexFor(service string) map[string]*Event {
	idx, ok := s.eventIndex[service]
	if !ok {
		idx = map[string]*Event{}
		s.eventIndex[service] = idx
	}
	return idx
}

func (s *Service) lookupEvent(service, eventID string) *Event {
	if idx, ok := s.eventIndex[service]; ok {
		return idx[eventID]
	}
	return nil
}

func (s *Service) installVersion(gateID string, v *GateVersion) {
	g, ok := s.gates[gateID]
	if !ok {
		g = &Gate{GateID: gateID, CreatedAt: v.CreatedAt}
		s.gates[gateID] = g
		s.gateRequests[gateID] = map[string]*GateVersion{}
	}
	for _, old := range g.Versions {
		old.Superseded = true
	}
	stored := cloneGateVersion(v)
	g.Versions = append(g.Versions, stored)
	g.CurrentVersion = v.Version
	s.gateRequests[gateID][v.RequestID] = stored
}

// freezeSnapshot 冻结当前已接收事件在策略窗口范围内形成的预算。
// 返回的快照与可变状态完全独立，后续事件无法改写它。
func (s *Service) freezeSnapshot(p Policy) Snapshot {
	snap := Snapshot{LastEventSeq: s.lastSeq}
	for key, b := range s.budgets {
		if key.service != p.Service {
			continue
		}
		if !key.start.Before(p.WindowStart) && !key.end.After(p.WindowEnd) {
			snap.Budgets = append(snap.Budgets, *b)
		}
	}
	sort.Slice(snap.Budgets, func(i, j int) bool {
		return snap.Budgets[i].WindowStart.Before(snap.Budgets[j].WindowStart)
	})
	for _, b := range snap.Budgets {
		snap.Total += b.Total
		snap.Failed += b.Failed
	}
	return snap
}

func validateResultingTotals(total, failed int64) error {
	if total < 0 {
		return errf(ErrConstraintViolated, "修正后总请求累计为负: %d", total)
	}
	if failed < 0 {
		return errf(ErrConstraintViolated, "修正后失败累计为负: %d", failed)
	}
	if failed > total {
		return errf(ErrConstraintViolated, "修正后失败累计 %d 超过总累计 %d", failed, total)
	}
	return nil
}

func validatePolicy(p Policy) error {
	if p.Service == "" {
		return errf(ErrInvalidArgument, "策略缺少 service")
	}
	if err := validateWindow(p.WindowStart, p.WindowEnd); err != nil {
		return err
	}
	if p.ErrorBudget < 0 {
		return errf(ErrInvalidArgument, "错误预算阈值不能为负: %d", p.ErrorBudget)
	}
	return nil
}

func decide(failed, errorBudget int64) Decision {
	if failed <= errorBudget {
		return DecisionAllow
	}
	return DecisionDeny
}

// sameEventContent 判断同号事件的内容是否一致（幂等重放 vs 冲突）。
// Seq 与 CreatedAt 由服务分配，不参与外部内容比较。
func sameEventContent(a, b *Event) bool {
	return a.Service == b.Service &&
		a.EventID == b.EventID &&
		a.Kind == b.Kind &&
		a.WindowStart.Equal(b.WindowStart) &&
		a.WindowEnd.Equal(b.WindowEnd) &&
		a.Increment == b.Increment &&
		a.CorrectsEventID == b.CorrectsEventID
}
