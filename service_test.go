package slobudget

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---- 测试辅助 ----

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }
func (c *clock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

func newTestService() (*Service, *clock) {
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return NewService(NewMemoryStore(), WithClock(c.now)), c
}

var (
	t1 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 = t1.Add(time.Hour)
	t3 = t2.Add(time.Hour)
)

func mustReport(t *testing.T, svc *Service, r MetricReport) MetricReport {
	t.Helper()
	out, err := svc.ReportMetric(context.Background(), r)
	if err != nil {
		t.Fatalf("ReportMetric(%s): %v", r.EventID, err)
	}
	return out
}

func mustCorrect(t *testing.T, svc *Service, c MetricCorrection) {
	t.Helper()
	if _, err := svc.CorrectMetric(context.Background(), c); err != nil {
		t.Fatalf("CorrectMetric(%s): %v", c.EventID, err)
	}
}

// ---- 指标上报与查询 ----

func TestReportAndQuery(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "svc-a", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 5, EventID: "e1"})
	mustReport(t, svc, MetricReport{Service: "svc-a", WindowStart: t1, WindowEnd: t2, TotalDelta: 50, FailedDelta: 2, EventID: "e2"})

	b, err := svc.QueryBudget(context.Background(), "svc-a", t1, t2)
	if err != nil {
		t.Fatal(err)
	}
	if b.Total != 150 || b.Failed != 7 {
		t.Fatalf("aggregate = total %d failed %d, want 150/7", b.Total, b.Failed)
	}
	if got := b.ErrorRate(); got != float64(7)/150.0 {
		t.Fatalf("error rate = %v", got)
	}
	if len(b.Windows) != 1 {
		t.Fatalf("windows = %d, want 1", len(b.Windows))
	}
	if len(b.IncludedEvents) != 2 {
		t.Fatalf("included events = %v", b.IncludedEvents)
	}
}

func TestReportValidation(t *testing.T) {
	svc, _ := newTestService()
	base := MetricReport{Service: "svc-a", WindowStart: t1, WindowEnd: t2, TotalDelta: 10, FailedDelta: 1, EventID: "e1"}

	cases := []struct {
		name string
		mod  func(*MetricReport)
	}{
		{"empty service", func(r *MetricReport) { r.Service = "" }},
		{"empty event id", func(r *MetricReport) { r.EventID = "" }},
		{"inverted window", func(r *MetricReport) { r.WindowEnd = r.WindowStart }},
		{"negative delta", func(r *MetricReport) { r.TotalDelta = -1 }},
		{"failed > total in single event", func(r *MetricReport) { r.FailedDelta = 11 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.mod(&r)
			_, err := svc.ReportMetric(context.Background(), r)
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("want validation error, got %v", err)
			}
		})
	}
}

// 单事件校验：失败增量不得超过总增量，增量不得为负。
func TestReportSingleEventInvariant(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 10, FailedDelta: 8, EventID: "e1"})
	// 0 总请求 / 3 失败：单事件即非法。
	_, err := svc.ReportMetric(context.Background(), MetricReport{
		Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 0, FailedDelta: 3, EventID: "e2",
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("single-event failed>total want validation, got %v", err)
	}
	// 说明：纯上报路径中每条事件 failed<=total，求和后必然 failed<=total；
	// “累计失败超过总数 / 负累计”只会经由修正（负向调整）出现，见 TestCorrectionInvariants。
}

// ---- 幂等与冲突 ----

func TestReportIdempotencyAndConflict(t *testing.T) {
	svc, _ := newTestService()
	r := MetricReport{Service: "svc-a", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 5, EventID: "dup"}
	stored := mustReport(t, svc, r)

	// 同号同内容：重放，不重复累加（reported_at 不同也不影响幂等）。
	replay := r
	replay.ReportedAt = time.Now().Add(time.Hour)
	out, err := svc.ReportMetric(context.Background(), replay)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !out.ReportedAt.Equal(stored.ReportedAt) {
		t.Fatalf("replay should return stored event")
	}
	b, _ := svc.QueryBudget(context.Background(), "svc-a", t1, t2)
	if b.Total != 100 || b.Failed != 5 {
		t.Fatalf("replay double counted: %+v", b)
	}

	// 同号异内容：冲突。
	r.TotalDelta = 200
	_, err = svc.ReportMetric(context.Background(), r)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestEventIDNamespaceShared(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 10, FailedDelta: 1, EventID: "base"})
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 10, FailedDelta: 1, EventID: "x"})
	// 修正复用了已被上报占用的事件号 "x"（引用另一个合法原事件）。
	_, err := svc.CorrectMetric(context.Background(), MetricCorrection{
		EventID: "x", OriginalEvent: "base", TotalDeltaAdj: 0,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("reuse event id across kinds: want conflict, got %v", err)
	}
}

// ---- 修正 ----

func TestCorrectionAppendsNotOverwrites(t *testing.T) {
	svc, _ := newTestService()
	orig := MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 10, EventID: "e1"}
	mustReport(t, svc, orig)
	mustCorrect(t, svc, MetricCorrection{
		EventID: "c1", OriginalEvent: "e1",
		TotalDeltaAdj: -20, FailedDeltaAdj: -5, Reason: "double counted",
	})

	// 原事件保持不变。
	if got, ok := findReport(svc.state, "e1"); !ok || got.TotalDelta != 100 || got.FailedDelta != 10 {
		t.Fatalf("original report mutated: %+v ok=%v", got, ok)
	}
	// 累计按增量修正。
	b, _ := svc.QueryBudget(context.Background(), "s", t1, t2)
	if b.Total != 80 || b.Failed != 5 {
		t.Fatalf("after correction aggregate = %d/%d, want 80/5", b.Total, b.Failed)
	}
	// 修正事件也纳入快照留痕。
	if len(b.IncludedEvents) != 2 {
		t.Fatalf("included = %v", b.IncludedEvents)
	}
}

func TestCorrectionIdempotency(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 10, EventID: "e1"})
	c := MetricCorrection{EventID: "c1", OriginalEvent: "e1", TotalDeltaAdj: -10, FailedDeltaAdj: 0}
	mustCorrect(t, svc, c)
	mustCorrect(t, svc, c) // 重放
	b, _ := svc.QueryBudget(context.Background(), "s", t1, t2)
	if b.Total != 90 {
		t.Fatalf("correction double applied: total=%d", b.Total)
	}

	// 同号异内容冲突。
	c.TotalDeltaAdj = -20
	_, err := svc.CorrectMetric(context.Background(), c)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestCorrectionInvariants(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 10, EventID: "e1"})

	// 负累计：total 100 - 120。
	_, err := svc.CorrectMetric(context.Background(), MetricCorrection{
		EventID: "c1", OriginalEvent: "e1", TotalDeltaAdj: -120,
	})
	if !errors.Is(err, ErrInvariant) {
		t.Fatalf("negative total: want invariant, got %v", err)
	}

	// 单事件合法、但修正后累计失败超过总数：100/10 + (+0,+100)。
	_, err = svc.CorrectMetric(context.Background(), MetricCorrection{
		EventID: "c2", OriginalEvent: "e1", FailedDeltaAdj: 100,
	})
	if !errors.Is(err, ErrInvariant) {
		t.Fatalf("failed>total: want invariant, got %v", err)
	}

	// 引用不存在的原事件。
	_, err = svc.CorrectMetric(context.Background(), MetricCorrection{
		EventID: "c3", OriginalEvent: "ghost",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing original: want not found, got %v", err)
	}
}

// ---- 并发：不丢失更新、不出现负累计 ----

func TestConcurrentReportsAndCorrections(t *testing.T) {
	svc, _ := newTestService()
	const n = 200

	// 第一批：并发上报。
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mustReport(t, svc, MetricReport{
				Service: "s", WindowStart: t1, WindowEnd: t2,
				TotalDelta: 10, FailedDelta: 1, EventID: fmt.Sprintf("e-%d", i),
			})
		}(i)
	}
	wg.Wait()

	// 第二批：修正与“另一窗口的上报”并发，确保更新互不丢失、无负累计。
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			mustCorrect(t, svc, MetricCorrection{
				EventID: fmt.Sprintf("c-%d", i), OriginalEvent: fmt.Sprintf("e-%d", i),
				TotalDeltaAdj: -9,
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			mustReport(t, svc, MetricReport{
				Service: "s", WindowStart: t2, WindowEnd: t3,
				TotalDelta: 2, FailedDelta: 0, EventID: fmt.Sprintf("e2-%d", i),
			})
		}(i)
	}
	wg.Wait()

	b, err := svc.QueryBudget(context.Background(), "s", t1, t2)
	if err != nil {
		t.Fatal(err)
	}
	// 每事件净 total = 10-9 = 1，failed = 1。
	if b.Total != n || b.Failed != n {
		t.Fatalf("lost update: total=%d failed=%d want %d/%d", b.Total, b.Failed, n, n)
	}
	if b.Failed > b.Total {
		t.Fatalf("invariant violated: failed %d > total %d", b.Failed, b.Total)
	}

	b2, _ := svc.QueryBudget(context.Background(), "s", t2, t3)
	if b2.Total != 2*n {
		t.Fatalf("second window total=%d, want %d", b2.Total, 2*n)
	}
}

// ---- 窗口语义 ----

func TestHalfOpenWindows(t *testing.T) {
	svc, _ := newTestService()
	// 两个相邻固定窗口各自独立；查询范围左闭右开。
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 10, EventID: "a"})
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t2, WindowEnd: t3, TotalDelta: 20, EventID: "b"})

	// [t1,t2) 只含第一个窗口。
	b, _ := svc.QueryBudget(context.Background(), "s", t1, t2)
	if b.Total != 10 || len(b.Windows) != 1 {
		t.Fatalf("[t1,t2) = %+v", b)
	}
	// [t1,t3) 聚合两个窗口。
	b, _ = svc.QueryBudget(context.Background(), "s", t1, t3)
	if b.Total != 30 || len(b.Windows) != 2 {
		t.Fatalf("[t1,t3) = total %d windows %d", b.Total, len(b.Windows))
	}
	// 部分重叠（范围截断窗口中部）的窗口不纳入。
	b, _ = svc.QueryBudget(context.Background(), "s", t1, t2.Add(-time.Minute))
	if b.Total != 0 || len(b.Windows) != 0 {
		t.Fatalf("partial overlap should be excluded: %+v", b)
	}
}

// ---- 门禁快照冻结 ----

func TestGateFreezeAndReevaluate(t *testing.T) {
	svc, clk := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 1, EventID: "e1"})

	g, v1, err := svc.CreateGate(context.Background(), CreateGateInput{
		RequestID: "req-1", Service: "s", WindowStart: t1, WindowEnd: t2,
		Policy: GatePolicy{MaxErrorRate: 0.05, AllowReevaluation: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if v1.Decision != DecisionAllowed {
		t.Fatalf("v1 decision = %s (%s)", v1.Decision, v1.Reason)
	}

	// 冻结之后到达的大量失败数据。
	clk.advance(time.Minute)
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 50, EventID: "e2"})

	// v1 决定与快照不被改写。
	g2, _ := svc.GetGate(context.Background(), g.ID)
	stored := g2.Versions[0]
	if stored.Snapshot.Total != 100 || stored.Snapshot.Failed != 1 || stored.Decision != DecisionAllowed {
		t.Fatalf("v1 mutated by late data: %+v", stored.Snapshot)
	}

	// 当前版本仍是 v1：校验旧版本号 1 正常通过。
	info, err := svc.ValidateDecision(context.Background(), g.ID, 1)
	if err != nil {
		t.Fatalf("v1 should be current: %v", err)
	}
	if info.Decision != DecisionAllowed {
		t.Fatalf("info = %+v", info)
	}

	// 重新评估生成 v2，旧批准作废。
	v2, err := svc.Reevaluate(context.Background(), g.ID, "req-2")
	if err != nil {
		t.Fatal(err)
	}
	if v2.Number != 2 || v2.Decision != DecisionBlocked {
		t.Fatalf("v2 = %+v", v2)
	}
	if v2.Snapshot.Total != 200 || v2.Snapshot.Failed != 51 {
		t.Fatalf("v2 snapshot = %d/%d", v2.Snapshot.Total, v2.Snapshot.Failed)
	}

	// 用旧版本号校验 -> obsolete。
	_, err = svc.ValidateDecision(context.Background(), g.ID, 1)
	if !errors.Is(err, ErrObsolete) {
		t.Fatalf("old approval: want obsolete, got %v", err)
	}
	// 当前版本 blocked -> release blocked。
	_, err = svc.ValidateDecision(context.Background(), g.ID, 0)
	if !errors.Is(err, ErrReleaseBlocked) {
		t.Fatalf("blocked current: want release_blocked, got %v", err)
	}
}

func TestGateCreateIdempotency(t *testing.T) {
	svc, _ := newTestService()
	in := CreateGateInput{RequestID: "req-x", Service: "s", WindowStart: t1, WindowEnd: t2,
		Policy: GatePolicy{MaxErrorRate: 0.5}}
	g1, v1a, err := svc.CreateGate(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	g1b, v1b, err := svc.CreateGate(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if g1.ID != g1b.ID || v1a.Number != v1b.Number {
		t.Fatalf("idempotent create returned different gate: %s vs %s", g1.ID, g1b.ID)
	}
	if len(svc.state.Gates) != 1 {
		t.Fatalf("duplicate gate created: %d", len(svc.state.Gates))
	}

	// 同请求号异内容 -> 冲突。
	in.Service = "other"
	_, _, err = svc.CreateGate(context.Background(), in)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestConcurrentCreateSameRequest(t *testing.T) {
	svc, _ := newTestService()
	const n = 50
	var wg sync.WaitGroup
	ids := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, _, err := svc.CreateGate(context.Background(), CreateGateInput{
				RequestID: "only", Service: "s", WindowStart: t1, WindowEnd: t2,
				Policy: GatePolicy{MaxErrorRate: 0.5},
			})
			if err != nil {
				errs <- err
				return
			}
			ids <- g.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent create: %v", err)
	}
	first := ""
	count := 0
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("multiple gates for one request: %s vs %s", first, id)
		}
		count++
	}
	if count != n {
		t.Fatalf("responses = %d, want %d", count, n)
	}
	if len(svc.state.Gates) != 1 {
		t.Fatalf("gates = %d, want 1", len(svc.state.Gates))
	}
}

func TestReevaluatePolicyAndIdempotency(t *testing.T) {
	svc, _ := newTestService()
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, EventID: "e1"})
	g, _, _ := svc.CreateGate(context.Background(), CreateGateInput{
		RequestID: "r1", Service: "s", WindowStart: t1, WindowEnd: t2,
		Policy: GatePolicy{MaxErrorRate: 0.5, AllowReevaluation: false},
	})

	_, err := svc.Reevaluate(context.Background(), g.ID, "r2")
	if !errors.Is(err, ErrPolicyDenied) {
		t.Fatalf("want policy denied, got %v", err)
	}

	// 打开重评估开关后，同请求号重放只产生一个新版本。
	g.Policy.AllowReevaluation = true
	svc.state.Gates[g.ID].Policy.AllowReevaluation = true
	v1, err := svc.Reevaluate(context.Background(), g.ID, "r2")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := svc.Reevaluate(context.Background(), g.ID, "r2")
	if err != nil {
		t.Fatal(err)
	}
	if v1.Number != v2.Number {
		t.Fatalf("reevaluate not idempotent: %d vs %d", v1.Number, v2.Number)
	}
	if got := svc.state.Gates[g.ID].CurrentVersion(); got != 2 {
		t.Fatalf("current version = %d, want 2", got)
	}
}

// ---- 持久化：重启后状态可恢复，旧门禁决定仍可校验 ----

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"

	clk := &clock{t: t1}
	store1 := NewFileStore(path)
	svc1 := NewService(store1, WithClock(clk.now))
	mustReport(t, svc1, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 1, EventID: "e1"})
	g, v, _ := svc1.CreateGate(context.Background(), CreateGateInput{
		RequestID: "r1", Service: "s", WindowStart: t1, WindowEnd: t2,
		Policy: GatePolicy{MaxErrorRate: 0.05, AllowReevaluation: true},
	})
	if v.Decision != DecisionAllowed {
		t.Fatalf("v1 = %s", v.Decision)
	}

	// 用全新的服务实例从同一文件恢复。
	svc2 := NewService(NewFileStore(path), WithClock(clk.now))
	got, err := svc2.GetGate(context.Background(), g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentVersion() != 1 || got.Versions[0].Snapshot.Total != 100 {
		t.Fatalf("reloaded gate = %+v", got)
	}
	// 事件幂等记录也恢复：同号异内容仍冲突。
	_, err = svc2.ReportMetric(context.Background(), MetricReport{
		Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 999, EventID: "e1",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict after reload, got %v", err)
	}
}
