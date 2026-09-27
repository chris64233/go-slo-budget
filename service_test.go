package slobudget

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad time %q: %v", s, err)
	}
	return tm
}

func testWindow(t *testing.T) (time.Time, time.Time) {
	t.Helper()
	return mustTime(t, "2026-09-01T00:00:00Z"), mustTime(t, "2026-09-02T00:00:00Z")
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(context.Background(), NewMemoryStore())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func reportN(t *testing.T, svc *Service, service, eventID string, start, end time.Time, total, failed int64) {
	t.Helper()
	_, err := svc.Report(context.Background(), ReportInput{
		Service: service, EventID: eventID,
		WindowStart: start, WindowEnd: end,
		Increment: Increment{Total: total, Failed: failed},
	})
	if err != nil {
		t.Fatalf("Report %s: %v", eventID, err)
	}
}

func TestReportAccumulatesByWindow(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)

	reportN(t, svc, "svc-a", "e1", start, end, 100, 2)
	reportN(t, svc, "svc-a", "e2", start, end, 50, 1)

	view, err := svc.QueryBudget(start, end, "svc-a")
	if err != nil {
		t.Fatal(err)
	}
	if view.Total != 150 || view.Failed != 3 {
		t.Fatalf("got total=%d failed=%d, want 150/3", view.Total, view.Failed)
	}
	if len(view.Windows) != 1 {
		t.Fatalf("got %d windows, want 1", len(view.Windows))
	}
}

func TestReportWindowHalfOpenContainment(t *testing.T) {
	svc := newTestService(t)
	d0 := mustTime(t, "2026-09-01T00:00:00Z")
	d1 := mustTime(t, "2026-09-02T00:00:00Z")
	d2 := mustTime(t, "2026-09-03T00:00:00Z")

	reportN(t, svc, "s", "e1", d0, d1, 10, 1)
	reportN(t, svc, "s", "e2", d1, d2, 20, 2)

	// 查询范围 [d0, d1) 只包含第一个窗口：左闭右开，相邻窗口不重叠。
	view, err := svc.QueryBudget(d0, d1, "s")
	if err != nil {
		t.Fatal(err)
	}
	if view.Total != 10 || view.Failed != 1 {
		t.Fatalf("got %d/%d, want 10/1", view.Total, view.Failed)
	}

	// 非法窗口 start == end / start > end。
	if _, err := svc.QueryBudget(d1, d1, "s"); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("start==end want invalid_window, got %v", err)
	}
	if _, err := svc.QueryBudget(d2, d0, "s"); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("start>end want invalid_window, got %v", err)
	}
}

func TestInvalidIncrementRejected(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)

	cases := []struct {
		total, failed int64
		want          error
	}{
		{-1, 0, ErrInvalidIncrement},
		{0, -1, ErrInvalidIncrement},
		{5, 6, ErrInvalidIncrement},
	}
	for i, c := range cases {
		_, err := svc.Report(context.Background(), ReportInput{
			Service: "s", EventID: fmt.Sprintf("e%d", i),
			WindowStart: start, WindowEnd: end,
			Increment: Increment{Total: c.total, Failed: c.failed},
		})
		if !errors.Is(err, c.want) {
			t.Fatalf("case %d: got %v, want %v", i, err, c.want)
		}
	}
}

func TestReportIdempotencyAndConflict(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	in := ReportInput{
		Service: "s", EventID: "dup",
		WindowStart: start, WindowEnd: end,
		Increment: Increment{Total: 10, Failed: 1},
	}
	r1, err := svc.Report(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Replayed {
		t.Fatal("first report must not be a replay")
	}
	r2, err := svc.Report(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Replayed || r2.Event.Seq != r1.Event.Seq {
		t.Fatalf("same-event replay mismatch: %+v vs %+v", r1.Event, r2.Event)
	}

	// 同号异内容 -> 冲突，且不得重复计入。
	conflict := in
	conflict.Increment = Increment{Total: 10, Failed: 2}
	_, err = svc.Report(context.Background(), conflict)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want conflict", err)
	}
	// 同号异窗口也算异内容。
	conflict2 := in
	conflict2.WindowStart = mustTime(t, "2026-08-31T00:00:00Z")
	_, err = svc.Report(context.Background(), conflict2)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want conflict", err)
	}

	view, _ := svc.QueryBudget(start, end, "s")
	if view.Total != 10 || view.Failed != 1 {
		t.Fatalf("conflict report was counted: %d/%d", view.Total, view.Failed)
	}
}

func TestCorrectionAppendsAndValidates(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "orig", start, end, 100, 10)

	// 修正为追加事件：总数 -20，失败 -8 => 80/2。
	res, err := svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "corr-1", CorrectsEventID: "orig",
		TotalDelta: -20, FailedDelta: -8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Event.Kind != KindCorrection || res.Event.CorrectsEventID != "orig" {
		t.Fatalf("correction event wrong: %+v", res.Event)
	}
	events, _ := svc.ListEvents("s")
	if len(events) != 2 || events[0].Kind != KindReport {
		t.Fatalf("history must keep both events, got %+v", events)
	}

	view, _ := svc.QueryBudget(start, end, "s")
	if view.Total != 80 || view.Failed != 2 {
		t.Fatalf("got %d/%d, want 80/2", view.Total, view.Failed)
	}

	// 不能把失败改成超过总数。
	_, err = svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "corr-2", CorrectsEventID: "orig",
		TotalDelta: 0, FailedDelta: 79,
	})
	if !errors.Is(err, ErrConstraintViolated) {
		t.Fatalf("failed>total: got %v", err)
	}

	// 不能产生负累计。
	_, err = svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "corr-3", CorrectsEventID: "orig",
		TotalDelta: -81, FailedDelta: 0,
	})
	if !errors.Is(err, ErrConstraintViolated) {
		t.Fatalf("negative total: got %v", err)
	}
	_, err = svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "corr-4", CorrectsEventID: "orig",
		TotalDelta: 0, FailedDelta: -3,
	})
	if !errors.Is(err, ErrConstraintViolated) {
		t.Fatalf("negative failed: got %v", err)
	}

	// 引用不存在的事件。
	_, err = svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "corr-5", CorrectsEventID: "ghost",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing original: got %v", err)
	}

	// 累计未发生任何变化。
	view, _ = svc.QueryBudget(start, end, "s")
	if view.Total != 80 || view.Failed != 2 {
		t.Fatalf("rejected corrections changed totals: %d/%d", view.Total, view.Failed)
	}
}

func TestCorrectionIdempotencyAndConflict(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "orig", start, end, 100, 10)

	in := CorrectionInput{Service: "s", EventID: "corr", CorrectsEventID: "orig", TotalDelta: -10, FailedDelta: -1}
	r1, err := svc.Correct(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := svc.Correct(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Replayed || r2.Event.Seq != r1.Event.Seq {
		t.Fatal("correction replay must return same event")
	}

	// 同号异内容（不同增量）冲突。
	bad := in
	bad.FailedDelta = -2
	_, err = svc.Correct(context.Background(), bad)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want conflict", err)
	}

	view, _ := svc.QueryBudget(start, end, "s")
	if view.Total != 90 || view.Failed != 9 {
		t.Fatalf("got %d/%d, want 90/9", view.Total, view.Failed)
	}
}

func TestConcurrentReportsNoLostUpdate(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reportN(t, svc, "s", fmt.Sprintf("e-%d", i), start, end, 1, 1)
		}(i)
	}
	wg.Wait()

	view, _ := svc.QueryBudget(start, end, "s")
	if view.Total != n || view.Failed != n {
		t.Fatalf("got %d/%d, want %d/%d (lost update)", view.Total, view.Failed, n, n)
	}
	events, _ := svc.ListEvents("s")
	if len(events) != n {
		t.Fatalf("got %d events, want %d", len(events), n)
	}
}

func TestConcurrentCorrectionsNoNegative(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "orig", start, end, 50, 50)

	// 50 个 -1 失败修正合法；额外的修正必须因负累计被拒绝，
	// 任何调度顺序下都不能出现 failed < 0。
	const attempts = 80
	var wg sync.WaitGroup
	var allowed int64
	var mu sync.Mutex
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Correct(context.Background(), CorrectionInput{
				Service: "s", EventID: fmt.Sprintf("c-%d", i),
				CorrectsEventID: "orig", FailedDelta: -1,
			})
			if err == nil {
				mu.Lock()
				allowed++
				mu.Unlock()
			} else if !errors.Is(err, ErrConstraintViolated) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	view, _ := svc.QueryBudget(start, end, "s")
	if view.Failed != 0 {
		t.Fatalf("failed = %d, want exactly 0", view.Failed)
	}
	if allowed != 50 {
		t.Fatalf("allowed = %d, want 50", allowed)
	}
}

func gatePolicy(start, end time.Time, budget int64, allowReeval bool) Policy {
	return Policy{
		Service:           "s",
		WindowStart:       start,
		WindowEnd:         end,
		ErrorBudget:       budget,
		AllowReevaluation: allowReeval,
	}
}

func TestGateSnapshotFrozenAndDecided(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 100, 5)

	policy := gatePolicy(start, end, 10, false)
	v1, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g1", RequestID: "req-1", Policy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	if v1.Version != 1 || v1.Decision != DecisionAllow {
		t.Fatalf("v1 = v%d/%s, want v1/allow", v1.Version, v1.Decision)
	}
	if v1.Snapshot.Failed != 5 || v1.Snapshot.Total != 100 || v1.Snapshot.LastEventSeq != 1 {
		t.Fatalf("unexpected snapshot: %+v", v1.Snapshot)
	}

	// 门禁创建后到达的数据不能改写已有决定。
	reportN(t, svc, "s", "e2", start, end, 100, 50)

	got, err := svc.CheckDecision("g1", 1)
	if err != nil {
		t.Fatalf("frozen allow must still validate: %v", err)
	}
	if got.Snapshot.Failed != 5 {
		t.Fatalf("snapshot mutated after late data: %+v", got.Snapshot)
	}

	g, _ := svc.GetGate("g1")
	if g.CurrentVersion != 1 || len(g.Versions) != 1 {
		t.Fatalf("late data must not create a version: %+v", g)
	}

	// 预算查询反映新数据，但门禁快照不变。
	view, _ := svc.QueryBudget(start, end, "s")
	if view.Failed != 55 {
		t.Fatalf("live budget = %d, want 55", view.Failed)
	}
}

func TestGateDenyOverBudget(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 100, 11)

	v, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "r", Policy: gatePolicy(start, end, 10, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v.Decision != DecisionDeny {
		t.Fatalf("got %s, want deny", v.Decision)
	}
	if _, err := svc.CheckDecision("g", 1); !errors.Is(err, ErrDecisionDenied) {
		t.Fatalf("got %v, want decision_denied", err)
	}
}

func TestGateCreateIdempotentAndDuplicate(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 10, 0)
	in := CreateGateInput{GateID: "g", RequestID: "req", Policy: gatePolicy(start, end, 1, false)}

	v1, err := svc.CreateGate(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := svc.CreateGate(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != v1.Version {
		t.Fatal("same requestID must return same version")
	}
	g, _ := svc.GetGate("g")
	if len(g.Versions) != 1 {
		t.Fatalf("idempotent create produced %d versions", len(g.Versions))
	}

	// 同门禁不同请求号 -> already_exists。
	_, err = svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "other", Policy: gatePolicy(start, end, 1, false),
	})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("got %v, want already_exists", err)
	}
}

func TestReevaluationVersions(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 100, 5)

	v1, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "c", Policy: gatePolicy(start, end, 10, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v1.Decision != DecisionAllow {
		t.Fatalf("v1 = %s", v1.Decision)
	}

	// 预算恶化后重评估 -> v2 阻止。
	reportN(t, svc, "s", "e2", start, end, 100, 20)
	v2, err := svc.ReevaluateGate(context.Background(), "g", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != 2 || v2.Decision != DecisionDeny || v2.Snapshot.Failed != 25 {
		t.Fatalf("v2 = %+v", v2)
	}

	// 旧版本批准不得用于当前发布。
	if _, err := svc.CheckDecision("g", 1); !errors.Is(err, ErrVersionSuperseded) {
		t.Fatalf("old version check: got %v, want version_superseded", err)
	}
	if _, err := svc.CheckDecision("g", 2); !errors.Is(err, ErrDecisionDenied) {
		t.Fatalf("current denied: got %v", err)
	}

	// 重评估请求号幂等。
	v2again, err := svc.ReevaluateGate(context.Background(), "g", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if v2again.Version != 2 {
		t.Fatalf("idempotent reeval produced v%d", v2again.Version)
	}

	// 预算恢复后再次重评估 -> v3 允许，此时只有 v3 是当前版本。
	_, err = svc.Correct(context.Background(), CorrectionInput{
		Service: "s", EventID: "fix", CorrectsEventID: "e2",
		TotalDelta: 0, FailedDelta: -20,
	})
	if err != nil {
		t.Fatal(err)
	}
	v3, err := svc.ReevaluateGate(context.Background(), "g", "r2")
	if err != nil {
		t.Fatal(err)
	}
	if v3.Version != 3 || v3.Decision != DecisionAllow {
		t.Fatalf("v3 = v%d/%s", v3.Version, v3.Decision)
	}
	current, err := svc.CheckDecision("g", 3)
	if err != nil || current.Snapshot.Failed != 5 {
		t.Fatalf("current check: %v %+v", err, current)
	}
	g, _ := svc.GetGate("g")
	if g.CurrentVersion != 3 {
		t.Fatalf("current = %d, want 3", g.CurrentVersion)
	}
	for i, vv := range g.Versions[:2] {
		if !vv.Superseded {
			t.Errorf("version %d should be superseded", i+1)
		}
	}
}

func TestReevaluationNotAllowedByPolicy(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 10, 0)
	_, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "c", Policy: gatePolicy(start, end, 5, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.ReevaluateGate(context.Background(), "g", "r")
	if !errors.Is(err, ErrReevaluationNotAllowed) {
		t.Fatalf("got %v, want reevaluation_not_allowed", err)
	}
}

func TestConcurrentReevaluationSingleCurrentVersion(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 100, 5)
	_, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "create", Policy: gatePolicy(start, end, 100, true),
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 30
	var wg sync.WaitGroup
	versions := make(chan int64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := svc.ReevaluateGate(context.Background(), "g", fmt.Sprintf("r-%d", i))
			if err != nil {
				t.Errorf("reeval %d: %v", i, err)
				return
			}
			versions <- v.Version
		}(i)
	}
	wg.Wait()
	close(versions)

	seen := map[int64]bool{}
	for v := range versions {
		if seen[v] {
			t.Fatalf("version %d issued twice", v)
		}
		seen[v] = true
	}
	g, _ := svc.GetGate("g")
	if g.CurrentVersion != int64(n)+1 {
		t.Fatalf("current = %d, want %d", g.CurrentVersion, n+1)
	}
	if len(g.Versions) != n+1 {
		t.Fatalf("versions = %d, want %d", len(g.Versions), n+1)
	}
	// 只有当前版本能通过校验。
	current := g.CurrentVersion
	if _, err := svc.CheckDecision("g", current); err != nil {
		t.Fatalf("current version must validate: %v", err)
	}
	if current > 1 {
		if _, err := svc.CheckDecision("g", current-1); !errors.Is(err, ErrVersionSuperseded) {
			t.Fatalf("got %v", err)
		}
	}
}

func TestConcurrentCreateSameRequestSingleVersion(t *testing.T) {
	svc := newTestService(t)
	start, end := testWindow(t)
	reportN(t, svc, "s", "e1", start, end, 10, 0)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.CreateGate(context.Background(), CreateGateInput{
				GateID: "g", RequestID: "same-req", Policy: gatePolicy(start, end, 5, true),
			})
			if err != nil {
				t.Errorf("concurrent create: %v", err)
			}
		}()
	}
	wg.Wait()
	g, _ := svc.GetGate("g")
	if len(g.Versions) != 1 || g.CurrentVersion != 1 {
		t.Fatalf("got %d versions/current=%d, want exactly 1", len(g.Versions), g.CurrentVersion)
	}
}

func TestCheckDecisionNotFound(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.CheckDecision("ghost", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}

	start, end := testWindow(t)
	reportN(t, svc, "s", "e", start, end, 10, 0)
	_, _ = svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "c", Policy: gatePolicy(start, end, 5, true),
	})
	if _, err := svc.CheckDecision("g", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("future version: got %v", err)
	}
	if _, err := svc.CheckDecision("g", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("version 0: got %v", err)
	}
}

func TestSnapshotOnlyFreezesInRangeWindows(t *testing.T) {
	svc := newTestService(t)
	d0 := mustTime(t, "2026-09-01T00:00:00Z")
	d1 := mustTime(t, "2026-09-02T00:00:00Z")
	d2 := mustTime(t, "2026-09-03T00:00:00Z")
	d3 := mustTime(t, "2026-09-04T00:00:00Z")

	reportN(t, svc, "s", "in", d0, d1, 100, 3)
	reportN(t, svc, "s", "out", d1, d2, 100, 99)

	v, err := svc.CreateGate(context.Background(), CreateGateInput{
		GateID: "g", RequestID: "c", Policy: gatePolicy(d0, d1, 10, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if v.Snapshot.Total != 100 || v.Snapshot.Failed != 3 {
		t.Fatalf("snapshot leaked out-of-range windows: %+v", v.Snapshot)
	}

	// 查询更大范围 [d0,d3) 时两个窗口都在。
	view, _ := svc.QueryBudget(d0, d3, "s")
	if view.Total != 200 || view.Failed != 102 {
		t.Fatalf("range query = %d/%d", view.Total, view.Failed)
	}
}
