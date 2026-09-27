package slobudget

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type apiClient struct {
	t   *testing.T
	srv *httptest.Server
}

func newAPIClient(t *testing.T) (*apiClient, *Service, *clock) {
	t.Helper()
	svc, clk := newTestService()
	srv := httptest.NewServer(NewServer(svc))
	t.Cleanup(srv.Close)
	return &apiClient{t: t, srv: srv}, svc, clk
}

func (c *apiClient) do(method, path string, body any) (int, map[string]any) {
	c.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		rdr = bytes.NewReader(data)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.srv.URL+path, rdr)
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatalf("decode %s %s: %v", method, path, err)
	}
	return resp.StatusCode, out
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func TestAPIReportCorrectionBudget(t *testing.T) {
	c, _, _ := newAPIClient(t)

	// 上报。
	status, out := c.do("POST", "/v1/metrics/reports", map[string]any{
		"service": "svc-a", "window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"total_delta": 100, "failed_delta": 10, "event_id": "e1",
	})
	if status != http.StatusCreated {
		t.Fatalf("report status = %d body=%v", status, out)
	}

	// 幂等重放：201，内容一致。
	status, out2 := c.do("POST", "/v1/metrics/reports", map[string]any{
		"service": "svc-a", "window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"total_delta": 100, "failed_delta": 10, "event_id": "e1",
	})
	if status != http.StatusCreated || out2["report"] == nil {
		t.Fatalf("replay status=%d body=%v", status, out2)
	}

	// 同号异内容：409 + 明确错误类型。
	status, out = c.do("POST", "/v1/metrics/reports", map[string]any{
		"service": "svc-a", "window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"total_delta": 200, "failed_delta": 10, "event_id": "e1",
	})
	if status != http.StatusConflict || errKind(out) != string(KindConflict) {
		t.Fatalf("conflict status=%d body=%v", status, out)
	}

	// 参数非法：400。
	status, out = c.do("POST", "/v1/metrics/reports", map[string]any{
		"service": "svc-a", "window_start": rfc3339(t2), "window_end": rfc3339(t1),
		"total_delta": 1, "failed_delta": 0, "event_id": "bad-window",
	})
	if status != http.StatusBadRequest || errKind(out) != string(KindValidation) {
		t.Fatalf("validation status=%d body=%v", status, out)
	}

	// 修正。
	status, out = c.do("POST", "/v1/metrics/corrections", map[string]any{
		"event_id": "c1", "original_event": "e1",
		"total_delta_adj": -20, "failed_delta_adj": -10, "reason": "fix",
	})
	if status != http.StatusCreated {
		t.Fatalf("correction status=%d body=%v", status, out)
	}

	// 修正导致不变量破坏：422。
	status, out = c.do("POST", "/v1/metrics/corrections", map[string]any{
		"event_id": "c2", "original_event": "e1",
		"total_delta_adj": -1000,
	})
	if status != http.StatusUnprocessableEntity || errKind(out) != string(KindInvariant) {
		t.Fatalf("invariant status=%d body=%v", status, out)
	}

	// 查询：100-20=80 total, 10-10=0 failed。
	status, out = c.do("GET", "/v1/budget?service=svc-a&start="+rfc3339(t1)+"&end="+rfc3339(t2), nil)
	if status != http.StatusOK {
		t.Fatalf("budget status=%d body=%v", status, out)
	}
	budget := out["budget"].(map[string]any)
	if budget["total"].(float64) != 80 || budget["failed"].(float64) != 0 {
		t.Fatalf("budget = %v", budget)
	}
	events := budget["included_events"].([]any)
	if len(events) != 2 {
		t.Fatalf("included events = %v", events)
	}
}

func TestAPIGateLifecycle(t *testing.T) {
	c, svc, clk := newAPIClient(t)

	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 1, EventID: "e1"})

	// 创建门禁，首版本允许。
	status, out := c.do("POST", "/v1/gates", map[string]any{
		"request_id": "g1", "service": "s",
		"window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"policy": map[string]any{"max_error_rate": 0.05, "allow_reevaluation": true},
	})
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", status, out)
	}
	gateID := out["gate"].(map[string]any)["id"].(string)
	if versionOf(out).Decision != "allowed" {
		t.Fatalf("v1 = %v", out["version"])
	}

	// 幂等重放创建：同一个 gate。
	status, out2 := c.do("POST", "/v1/gates", map[string]any{
		"request_id": "g1", "service": "s",
		"window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"policy": map[string]any{"max_error_rate": 0.05, "allow_reevaluation": true},
	})
	if status != http.StatusCreated || out2["gate"].(map[string]any)["id"] != gateID {
		t.Fatalf("idempotent create mismatch: %d %v", status, out2)
	}

	// 当前版本允许发布：200。
	status, out = c.do("GET", "/v1/gates/"+gateID+"/decision", nil)
	if status != http.StatusOK || out["decision"].(map[string]any)["decision"] != "allowed" {
		t.Fatalf("validate allowed: %d %v", status, out)
	}

	// 冻结后晚到的数据。
	clk.advance(time.Minute)
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, FailedDelta: 50, EventID: "e2"})

	// 重评估生成 v2，决定阻止。
	status, out = c.do("POST", "/v1/gates/"+gateID+"/reevaluate", map[string]any{"request_id": "g2"})
	if status != http.StatusCreated || versionOf(out).Decision != "blocked" {
		t.Fatalf("reevaluate: %d %v", status, out)
	}

	// 旧版本批准作废：409 obsolete，仍返回决策信息。
	status, out = c.do("GET", "/v1/gates/"+gateID+"/decision?version=1", nil)
	if status != http.StatusConflict || errKind(out) != string(KindObsolete) {
		t.Fatalf("obsolete: %d %v", status, out)
	}
	if out["decision"].(map[string]any)["version"].(float64) != 1 {
		t.Fatalf("obsolete body missing decision info: %v", out)
	}

	// 当前版本阻止发布：403。
	status, out = c.do("GET", "/v1/gates/"+gateID+"/decision", nil)
	if status != http.StatusForbidden || errKind(out) != string(KindReleaseBlocked) {
		t.Fatalf("blocked: %d %v", status, out)
	}

	// 门禁不存在：404。
	status, out = c.do("GET", "/v1/gates/gate-ghost/decision", nil)
	if status != http.StatusNotFound || errKind(out) != string(KindNotFound) {
		t.Fatalf("not found: %d %v", status, out)
	}
}

func TestAPIReevaluatePolicyDenied(t *testing.T) {
	c, svc, _ := newAPIClient(t)
	mustReport(t, svc, MetricReport{Service: "s", WindowStart: t1, WindowEnd: t2, TotalDelta: 100, EventID: "e1"})
	_, out := c.do("POST", "/v1/gates", map[string]any{
		"request_id": "g1", "service": "s",
		"window_start": rfc3339(t1), "window_end": rfc3339(t2),
		"policy": map[string]any{"max_error_rate": 0.5, "allow_reevaluation": false},
	})
	gateID := out["gate"].(map[string]any)["id"].(string)

	status, out := c.do("POST", "/v1/gates/"+gateID+"/reevaluate", map[string]any{"request_id": "g2"})
	if status != http.StatusForbidden || errKind(out) != string(KindPolicyDenied) {
		t.Fatalf("policy denied: %d %v", status, out)
	}
}

type versionJSON struct {
	Decision string
}

func versionOf(out map[string]any) versionJSON {
	v := out["version"].(map[string]any)
	return versionJSON{Decision: v["decision"].(string)}
}

func errKind(out map[string]any) string {
	e, ok := out["error"].(map[string]any)
	if !ok {
		return ""
	}
	return e["kind"].(string)
}
