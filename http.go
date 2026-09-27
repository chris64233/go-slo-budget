package slobudget

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// Server 把 Service 暴露为 HTTP JSON 接口。
type Server struct {
	svc *Service
	mux *http.ServeMux
}

// NewServer 创建 HTTP 服务。
func NewServer(svc *Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP 实现 http.Handler。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Handler 返回底层 handler，便于挂载。
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/metrics/reports", s.handleReport)
	s.mux.HandleFunc("POST /v1/metrics/corrections", s.handleCorrect)
	s.mux.HandleFunc("GET /v1/budget", s.handleQueryBudget)
	s.mux.HandleFunc("POST /v1/gates", s.handleCreateGate)
	s.mux.HandleFunc("POST /v1/gates/{gateID}/reevaluate", s.handleReevaluate)
	s.mux.HandleFunc("GET /v1/gates/{gateID}", s.handleGetGate)
	s.mux.HandleFunc("GET /v1/gates/{gateID}/decision", s.handleValidate)
}

// ---- 请求 / 响应结构 ----

type reportRequest struct {
	Service     string `json:"service"`
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
	TotalDelta  int64  `json:"total_delta"`
	FailedDelta int64  `json:"failed_delta"`
	EventID     string `json:"event_id"`
	ReportedAt  string `json:"reported_at,omitempty"`
}

type correctionRequest struct {
	EventID        string `json:"event_id"`
	OriginalEvent  string `json:"original_event"`
	TotalDeltaAdj  int64  `json:"total_delta_adj"`
	FailedDeltaAdj int64  `json:"failed_delta_adj"`
	Reason         string `json:"reason"`
	ReportedAt     string `json:"reported_at,omitempty"`
}

type createGateRequest struct {
	RequestID   string     `json:"request_id"`
	Service     string     `json:"service"`
	WindowStart string     `json:"window_start"`
	WindowEnd   string     `json:"window_end"`
	Policy      GatePolicy `json:"policy"`
}

// ---- handler ----

func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	var req reportRequest
	if !decode(w, r, &req) {
		return
	}
	start, end, ok := parseWindow(w, req.WindowStart, req.WindowEnd)
	if !ok {
		return
	}
	var reportedAt time.Time
	if req.ReportedAt != "" {
		t, err := time.Parse(time.RFC3339, req.ReportedAt)
		if err != nil {
			writeError(w, validationErr("handleReport", "invalid reported_at: %v", err))
			return
		}
		reportedAt = t
	}
	out, err := s.svc.ReportMetric(r.Context(), MetricReport{
		Service:     req.Service,
		WindowStart: start,
		WindowEnd:   end,
		TotalDelta:  req.TotalDelta,
		FailedDelta: req.FailedDelta,
		EventID:     req.EventID,
		ReportedAt:  reportedAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"report": reportToJSON(out)})
}

func (s *Server) handleCorrect(w http.ResponseWriter, r *http.Request) {
	var req correctionRequest
	if !decode(w, r, &req) {
		return
	}
	var reportedAt time.Time
	if req.ReportedAt != "" {
		t, err := time.Parse(time.RFC3339, req.ReportedAt)
		if err != nil {
			writeError(w, validationErr("handleCorrect", "invalid reported_at: %v", err))
			return
		}
		reportedAt = t
	}
	out, err := s.svc.CorrectMetric(r.Context(), MetricCorrection{
		EventID:        req.EventID,
		OriginalEvent:  req.OriginalEvent,
		TotalDeltaAdj:  req.TotalDeltaAdj,
		FailedDeltaAdj: req.FailedDeltaAdj,
		Reason:         req.Reason,
		ReportedAt:     reportedAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"correction": correctionToJSON(out)})
}

func (s *Server) handleQueryBudget(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	service := q.Get("service")
	start, end, ok := parseWindow(w, q.Get("start"), q.Get("end"))
	if !ok {
		return
	}
	b, err := s.svc.QueryBudget(r.Context(), service, start, end)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"budget": budgetToJSON(b)})
}

func (s *Server) handleCreateGate(w http.ResponseWriter, r *http.Request) {
	var req createGateRequest
	if !decode(w, r, &req) {
		return
	}
	start, end, ok := parseWindow(w, req.WindowStart, req.WindowEnd)
	if !ok {
		return
	}
	g, v, err := s.svc.CreateGate(r.Context(), CreateGateInput{
		RequestID:   req.RequestID,
		Service:     req.Service,
		WindowStart: start,
		WindowEnd:   end,
		Policy:      req.Policy,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"gate": gateToJSON(g), "version": versionToJSON(v)})
}

func (s *Server) handleReevaluate(w http.ResponseWriter, r *http.Request) {
	gateID := r.PathValue("gateID")
	var body struct {
		RequestID string `json:"request_id"`
	}
	if !decode(w, r, &body) {
		return
	}
	v, err := s.svc.Reevaluate(r.Context(), gateID, body.RequestID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"version": versionToJSON(v)})
}

func (s *Server) handleGetGate(w http.ResponseWriter, r *http.Request) {
	g, err := s.svc.GetGate(r.Context(), r.PathValue("gateID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gate": gateToJSON(g)})
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	version := 0
	if v := r.URL.Query().Get("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, validationErr("handleValidate", "invalid version: %v", err))
			return
		}
		version = n
	}
	info, err := s.svc.ValidateDecision(r.Context(), r.PathValue("gateID"), version)
	// obsolete / blocked 也携带完整决策信息返回，仅状态码不同。
	status := http.StatusOK
	if err != nil {
		var se *Error
		if !errors.As(err, &se) {
			writeError(w, err)
			return
		}
		switch se.Kind {
		case KindObsolete:
			status = http.StatusConflict
		case KindReleaseBlocked:
			status = http.StatusForbidden
		default:
			writeError(w, err)
			return
		}
	}
	writeJSON(w, status, map[string]any{"decision": decisionToJSON(info), "error": errorBody(err)})
}

// ---- 编解码辅助 ----

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		writeError(w, validationErr("decode", "invalid JSON body: %v", err))
		return false
	}
	return true
}

func parseWindow(w http.ResponseWriter, startS, endS string) (time.Time, time.Time, bool) {
	start, err := time.Parse(time.RFC3339, startS)
	if err != nil {
		writeError(w, validationErr("parseWindow", "invalid start time, want RFC3339: %v", err))
		return time.Time{}, time.Time{}, false
	}
	end, err := time.Parse(time.RFC3339, endS)
	if err != nil {
		writeError(w, validationErr("parseWindow", "invalid end time, want RFC3339: %v", err))
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

type errResponse struct {
	Kind string `json:"kind"`
	Op   string `json:"op,omitempty"`
	Msg  string `json:"message"`
}

func errorBody(err error) *errResponse {
	if err == nil {
		return nil
	}
	var se *Error
	if errors.As(err, &se) {
		return &errResponse{Kind: string(se.Kind), Op: se.Op, Msg: se.Msg}
	}
	return &errResponse{Kind: "internal_error", Msg: err.Error()}
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var se *Error
	if errors.As(err, &se) {
		switch se.Kind {
		case KindValidation:
			status = http.StatusBadRequest
		case KindConflict:
			status = http.StatusConflict
		case KindNotFound:
			status = http.StatusNotFound
		case KindInvariant:
			status = http.StatusUnprocessableEntity
		case KindPolicyDenied, KindReleaseBlocked:
			status = http.StatusForbidden
		case KindObsolete:
			status = http.StatusConflict
		}
	}
	writeJSON(w, status, map[string]any{"error": errorBody(err)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---- 输出序列化 ----

func reportToJSON(r MetricReport) map[string]any {
	return map[string]any{
		"service":      r.Service,
		"window_start": r.WindowStart.UTC().Format(time.RFC3339),
		"window_end":   r.WindowEnd.UTC().Format(time.RFC3339),
		"total_delta":  r.TotalDelta,
		"failed_delta": r.FailedDelta,
		"event_id":     r.EventID,
		"reported_at":  r.ReportedAt.UTC().Format(time.RFC3339),
	}
}

func correctionToJSON(c MetricCorrection) map[string]any {
	return map[string]any{
		"event_id":         c.EventID,
		"original_event":   c.OriginalEvent,
		"total_delta_adj":  c.TotalDeltaAdj,
		"failed_delta_adj": c.FailedDeltaAdj,
		"reason":           c.Reason,
		"reported_at":      c.ReportedAt.UTC().Format(time.RFC3339),
	}
}

func metricToJSON(m MetricSnapshot) map[string]any {
	return map[string]any{
		"service":          m.Service,
		"window_start":     m.WindowStart.UTC().Format(time.RFC3339),
		"window_end":       m.WindowEnd.UTC().Format(time.RFC3339),
		"total":            m.Total,
		"failed":           m.Failed,
		"error_rate":       m.ErrorRate(),
		"last_received_at": m.LastReceivedAt.UTC().Format(time.RFC3339),
	}
}

func budgetToJSON(b BudgetSnapshot) map[string]any {
	wins := make([]map[string]any, 0, len(b.Windows))
	for _, w := range b.Windows {
		wins = append(wins, metricToJSON(w))
	}
	return map[string]any{
		"service":         b.Service,
		"window_start":    b.WindowStart.UTC().Format(time.RFC3339),
		"window_end":      b.WindowEnd.UTC().Format(time.RFC3339),
		"frozen_at":       b.FrozenAt.UTC().Format(time.RFC3339),
		"windows":         wins,
		"total":           b.Total,
		"failed":          b.Failed,
		"error_rate":      b.ErrorRate(),
		"included_events": b.IncludedEvents,
	}
}

func versionToJSON(v *GateVersion) map[string]any {
	return map[string]any{
		"number":     v.Number,
		"snapshot":   budgetToJSON(v.Snapshot),
		"decision":   string(v.Decision),
		"reason":     v.Reason,
		"created_at": v.CreatedAt.UTC().Format(time.RFC3339),
		"request_id": v.RequestID,
	}
}

func gateToJSON(g *Gate) map[string]any {
	versions := make([]map[string]any, 0, len(g.Versions))
	for _, v := range g.Versions {
		versions = append(versions, versionToJSON(v))
	}
	return map[string]any{
		"id":              g.ID,
		"service":         g.Service,
		"policy":          g.Policy,
		"current_version": g.CurrentVersion(),
		"versions":        versions,
	}
}

func decisionToJSON(d DecisionInfo) map[string]any {
	return map[string]any{
		"gate_id":         d.GateID,
		"version":         d.VersionNumber,
		"current_version": d.CurrentNumber,
		"decision":        string(d.Decision),
		"reason":          d.Reason,
		"snapshot":        budgetToJSON(d.Snapshot),
	}
}
