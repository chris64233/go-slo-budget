package slobudget

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWALRoundTripRestoresState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	start, end := testWindow(t)

	store1, err := NewWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc1, err := NewService(ctx, store1)
	if err != nil {
		t.Fatal(err)
	}
	reportN(t, svc1, "s", "e1", start, end, 100, 8)
	_, err = svc1.Correct(ctx, CorrectionInput{
		Service: "s", EventID: "c1", CorrectsEventID: "e1",
		TotalDelta: -10, FailedDelta: -3,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc1.CreateGate(ctx, CreateGateInput{
		GateID: "g", RequestID: "req-1", Policy: gatePolicy(start, end, 10, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 预算恶化，重评估出 v2 deny。
	reportN(t, svc1, "s", "e2", start, end, 100, 20)
	v2, err := svc1.ReevaluateGate(ctx, "g", "req-2")
	if err != nil {
		t.Fatal(err)
	}
	if v2.Decision != DecisionDeny {
		t.Fatalf("v2 = %s, want deny", v2.Decision)
	}
	if err := store1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：重放 WAL，全部状态与决策依据应恢复。
	store2, err := NewWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc2, err := NewService(ctx, store2)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	view, err := svc2.QueryBudget(start, end, "s")
	if err != nil {
		t.Fatal(err)
	}
	if view.Total != 190 || view.Failed != 25 {
		t.Fatalf("restored budget = %d/%d, want 190/25", view.Total, view.Failed)
	}
	events, err := svc2.ListEvents("s")
	if err != nil || len(events) != 3 {
		t.Fatalf("restored events = %d, err=%v", len(events), err)
	}

	g, err := svc2.GetGate("g")
	if err != nil {
		t.Fatal(err)
	}
	if g.CurrentVersion != 2 || len(g.Versions) != 2 {
		t.Fatalf("restored gate = %+v", g)
	}
	if !g.Versions[0].Superseded {
		t.Fatal("v1 should be superseded after replay")
	}

	// 旧批准仍不可用，当前阻止决定也恢复。
	if _, err := svc2.CheckDecision("g", 1); !errors.Is(err, ErrVersionSuperseded) {
		t.Fatalf("old version after restore: %v", err)
	}
	if _, err := svc2.CheckDecision("g", 2); !errors.Is(err, ErrDecisionDenied) {
		t.Fatalf("denied version after restore: %v", err)
	}

	// 幂等映射也应恢复：同请求号不产生新版本。
	v2b, err := svc2.ReevaluateGate(ctx, "g", "req-2")
	if err != nil {
		t.Fatal(err)
	}
	if v2b.Version != 2 {
		t.Fatalf("request idempotency lost across restart: v%d", v2b.Version)
	}
	// 同事件号重放恢复。
	r, err := svc2.Report(ctx, ReportInput{
		Service: "s", EventID: "e1", WindowStart: start, WindowEnd: end,
		Increment: Increment{Total: 100, Failed: 8},
	})
	if err != nil || !r.Replayed {
		t.Fatalf("event idempotency lost across restart: %+v %v", r, err)
	}
}

func TestWALTruncatedTailIsIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	start, end := testWindow(t)

	store, err := NewWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	reportN(t, svc, "s", "e1", start, end, 10, 1)
	reportN(t, svc, "s", "e2", start, end, 5, 2)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// 人为截断日志尾部若干字节（落在第二条记录的 CRC/帧体内），
	// 模拟写到一半崩溃：第一条完整记录必须保留，残帧被丢弃。
	path := filepath.Join(dir, "wal.log")
	info, _ := os.Stat(path)
	for _, cut := range []int64{1, 4, 9} {
		if err := os.Truncate(path, info.Size()-cut); err != nil {
			t.Fatal(err)
		}
		st2, err := NewWALStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		svc2, err := NewService(ctx, st2)
		if err != nil {
			t.Fatalf("truncated WAL must replay: %v", err)
		}
		view, _ := svc2.QueryBudget(start, end, "s")
		if view.Total != 10 || view.Failed != 1 {
			t.Fatalf("cut=%d restored = %d/%d, want 10/1", cut, view.Total, view.Failed)
		}

		// 残帧已被截断清理，新追加依然可用。
		reportN(t, svc2, "s", "e2", start, end, 5, 2)
		if err := st2.Close(); err != nil {
			t.Fatal(err)
		}
		st3, err := NewWALStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		svc3, err := NewService(ctx, st3)
		if err != nil {
			t.Fatal(err)
		}
		view, _ = svc3.QueryBudget(start, end, "s")
		if view.Total != 15 || view.Failed != 3 {
			t.Fatalf("post-truncation append = %d/%d, want 15/3", view.Total, view.Failed)
		}
		st3.Close()

		// 重建“含两条完整记录”的初始状态供下一轮截断。
		_ = os.RemoveAll(dir)
		_ = os.MkdirAll(dir, 0o755)
		s0, err := NewWALStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		svc0, err := NewService(ctx, s0)
		if err != nil {
			t.Fatal(err)
		}
		reportN(t, svc0, "s", "e1", start, end, 10, 1)
		reportN(t, svc0, "s", "e2", start, end, 5, 2)
		if err := s0.Close(); err != nil {
			t.Fatal(err)
		}
		info, _ = os.Stat(path)
	}
}

func TestWALCRCError(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	start, end := testWindow(t)

	store, err := NewWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	reportN(t, svc, "s", "e1", start, end, 10, 1)
	reportN(t, svc, "s", "e2", start, end, 10, 1)
	store.Close()

	// 翻转第二条记录 payload 中的一个字节（头部 14B + 帧头 5B 之后）。
	path := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 第一条事件记录大约从偏移 19 开始；定位第二条记录起点更稳妥的方式：
	// 直接破坏第一条记录 payload 的一字节，期望 CRC 报错而不是静默接受。
	idx := int64(14 + 5 + 2)
	data[idx] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	st2, err := NewWALStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(ctx, st2); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("corrupted WAL: got %v, want error", err)
	}
	st2.Close()
}
