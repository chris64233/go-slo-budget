package slobudget

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) (*httptest.Server, *Service) {
	t.Helper()
	svc := newTestService(t)
	srv := httptest.NewServer(NewHTTPHandler(svc))
	t.Cleanup(srv.Close)
	return srv, svc
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return resp.StatusCode, out
}

func TestHTTPEndToEnd(t *testing.T) {
	srv, _ := newTestServer(t)
	ctx := context.Background()
	_ = ctx

	start := "2026-09-01T00:00:00Z"
	end := "2026-09-02T00:00:00Z"

	// 上报。
	status, body := doJSON(t, http.MethodPost, srv.URL+"/services/checkout/events", map[string]any{
		"eventId": "e1", "windowStart": start, "windowEnd": end,
		"total": 100, "failed": 5,
	})
	if status != http.StatusOK {
		t.Fatalf("report status=%d body=%v", status, body)
	}

	// 同号异内容冲突 -> 409。
	status, body = doJSON(t, http.MethodPost, srv.URL+"/services/checkout/events", map[string]any{
		"eventId": "e1", "windowStart": start, "windowEnd": end,
		"total": 100, "failed": 9,
	})
	if status != http.StatusConflict || body["code"] != string(CodeConflict) {
		t.Fatalf("conflict: status=%d body=%v", status, body)
	}

	// 失败 > 总数 -> 400。
	status, body = doJSON(t, http.MethodPost, srv.URL+"/services/checkout/events", map[string]any{
		"eventId": "bad", "windowStart": start, "windowEnd": end,
		"total": 1, "failed": 2,
	})
	if status != http.StatusBadRequest || body["code"] != string(CodeInvalidIncrement) {
		t.Fatalf("invalid increment: status=%d body=%v", status, body)
	}

	// 修正：failed -2 => 3。
	status, body = doJSON(t, http.MethodPost, srv.URL+"/services/checkout/corrections", map[string]any{
		"eventId": "c1", "correctsEventId": "e1",
		"totalDelta": 0, "failedDelta": -2,
	})
	if status != http.StatusOK {
		t.Fatalf("correct status=%d body=%v", status, body)
	}

	// 预算查询。
	status, body = doJSON(t, http.MethodGet, srv.URL+"/services/checkout/budget?start="+start+"&end="+end, nil)
	if status != http.StatusOK {
		t.Fatalf("budget status=%d", status)
	}
	if body["total"].(float64) != 100 || body["failed"].(float64) != 3 {
		t.Fatalf("budget body=%v", body)
	}

	// 创建门禁（预算 3 <= 4 允许）。
	status, body = doJSON(t, http.MethodPost, srv.URL+"/gates", map[string]any{
		"gateId": "release-42", "requestId": "req-create",
		"policy": map[string]any{
			"service": "checkout", "windowStart": start, "windowEnd": end,
			"errorBudget": 4, "allowReevaluation": true,
		},
	})
	if status != http.StatusCreated || body["decision"] != string(DecisionAllow) {
		t.Fatalf("create gate: status=%d body=%v", status, body)
	}

	// 校验 v1 通过。
	status, body = doJSON(t, http.MethodGet, srv.URL+"/gates/release-42/versions/1/check", nil)
	if status != http.StatusOK || body["decision"] != string(DecisionAllow) {
		t.Fatalf("check v1: status=%d body=%v", status, body)
	}

	// 预算恶化后重评估 -> v2 deny。
	doJSON(t, http.MethodPost, srv.URL+"/services/checkout/events", map[string]any{
		"eventId": "e2", "windowStart": start, "windowEnd": end,
		"total": 100, "failed": 10,
	})
	status, body = doJSON(t, http.MethodPost, srv.URL+"/gates/release-42/reevaluate", map[string]any{
		"requestId": "req-r2",
	})
	if status != http.StatusCreated || body["decision"] != string(DecisionDeny) {
		t.Fatalf("reeval: status=%d body=%v", status, body)
	}

	// 旧版本 -> 409 version_superseded。
	status, body = doJSON(t, http.MethodGet, srv.URL+"/gates/release-42/versions/1/check", nil)
	if status != http.StatusConflict || body["code"] != string(CodeVersionSuperseded) {
		t.Fatalf("old version: status=%d body=%v", status, body)
	}
	// 当前版本 -> 403 decision_denied。
	status, body = doJSON(t, http.MethodGet, srv.URL+"/gates/release-42/versions/2/check", nil)
	if status != http.StatusForbidden || body["code"] != string(CodeDecisionDenied) {
		t.Fatalf("denied: status=%d body=%v", status, body)
	}

	// 重评估幂等：同请求号返回同一版本，不产生 v3。
	status, body = doJSON(t, http.MethodPost, srv.URL+"/gates/release-42/reevaluate", map[string]any{
		"requestId": "req-r2",
	})
	if status != http.StatusCreated || body["version"].(float64) != 2 {
		t.Fatalf("idempotent reeval: status=%d body=%v", status, body)
	}

	// 门禁历史。
	status, body = doJSON(t, http.MethodGet, srv.URL+"/gates/release-42", nil)
	if status != http.StatusOK || body["currentVersion"].(float64) != 2 {
		t.Fatalf("gate history: %v", body)
	}
}

func TestHTTPReevaluationNotAllowed(t *testing.T) {
	srv, _ := newTestServer(t)
	start := "2026-09-01T00:00:00Z"
	end := "2026-09-02T00:00:00Z"
	doJSON(t, http.MethodPost, srv.URL+"/services/s/events", map[string]any{
		"eventId": "e", "windowStart": start, "windowEnd": end,
		"total": 10, "failed": 0,
	})
	status, body := doJSON(t, http.MethodPost, srv.URL+"/gates", map[string]any{
		"gateId": "g", "requestId": "c",
		"policy": map[string]any{
			"service": "s", "windowStart": start, "windowEnd": end,
			"errorBudget": 1, "allowReevaluation": false,
		},
	})
	if status != http.StatusCreated {
		t.Fatalf("create: %d %v", status, body)
	}
	status, body = doJSON(t, http.MethodPost, srv.URL+"/gates/g/reevaluate", map[string]any{"requestId": "r"})
	if status != http.StatusConflict || body["code"] != string(CodeReevaluationNotAllowed) {
		t.Fatalf("got %d %v", status, body)
	}
}

func TestHTTPErrorsAndAudit(t *testing.T) {
	srv, _ := newTestServer(t)

	// 非法 JSON -> 400。
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/services/s/events", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 未知门禁 -> 404。
	status, _ := doJSON(t, http.MethodGet, srv.URL+"/gates/nope/versions/1/check", nil)
	if status != http.StatusNotFound {
		t.Fatalf("got %d", status)
	}

	// 事件审计列表（空）。
	status, body := doJSON(t, http.MethodGet, srv.URL+"/services/s/events", nil)
	if status != http.StatusOK {
		t.Fatalf("events status=%d", status)
	}
	arr, ok := body["events"].([]any)
	if !ok || len(arr) != 0 {
		t.Fatalf("events body=%v", body)
	}

	// 预算查询缺少时间参数 -> 400。
	status, _ = doJSON(t, http.MethodGet, srv.URL+"/services/s/budget", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("got %d", status)
	}
}
