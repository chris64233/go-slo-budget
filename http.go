package slobudget

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// HTTPHandler 把服务能力暴露为 JSON HTTP 接口：
//
//	POST   /services/{service}/events                指标上报
//	POST   /services/{service}/corrections           追加修正
//	GET    /services/{service}/budget?start=&end=    预算查询
//	GET    /services/{service}/events                事件审计列表
//	POST   /gates                                    创建门禁
//	POST   /gates/{gateID}/reevaluate                重评估
//	GET    /gates/{gateID}                           门禁版本历史
//	GET    /gates/{gateID}/versions/{version}/check  决定校验
//
// 时间参数统一使用 RFC3339（如 2026-09-01T00:00:00Z）。
type HTTPHandler struct {
	svc *Service
	mux *http.ServeMux
}

// NewHTTPHandler 构建挂载好全部路由的 handler。
func NewHTTPHandler(svc *Service) *HTTPHandler {
	h := &HTTPHandler{svc: svc, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /services/{service}/events", h.report)
	h.mux.HandleFunc("POST /services/{service}/corrections", h.correct)
	h.mux.HandleFunc("GET /services/{service}/budget", h.queryBudget)
	h.mux.HandleFunc("GET /services/{service}/events", h.listEvents)
	h.mux.HandleFunc("POST /gates", h.createGate)
	h.mux.HandleFunc("POST /gates/{gateID}/reevaluate", h.reevaluate)
	h.mux.HandleFunc("GET /gates/{gateID}", h.getGate)
	h.mux.HandleFunc("GET /gates/{gateID}/versions/{version}/check", h.check)
	return h
}

// ServeHTTP 实现 http.Handler。
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type reportRequest struct {
	EventID     string `json:"eventId"`
	WindowStart string `json:"windowStart"`
	WindowEnd   string `json:"windowEnd"`
	Total       int64  `json:"total"`
	Failed      int64  `json:"failed"`
}

type correctionRequest struct {
	EventID         string `json:"eventId"`
	CorrectsEventID string `json:"correctsEventId"`
	TotalDelta      int64  `json:"totalDelta"`
	FailedDelta     int64  `json:"failedDelta"`
}

type policyDTO struct {
	Service           string `json:"service"`
	WindowStart       string `json:"windowStart"`
	WindowEnd         string `json:"windowEnd"`
	ErrorBudget       int64  `json:"errorBudget"`
	AllowReevaluation bool   `json:"allowReevaluation"`
}

type createGateRequest struct {
	GateID    string    `json:"gateId"`
	RequestID string    `json:"requestId"`
	Policy    policyDTO `json:"policy"`
}

type reevaluateRequest struct {
	RequestID string `json:"requestId"`
}

type errorResponse struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (h *HTTPHandler) report(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	var req reportRequest
	if !decode(w, r, &req) {
		return
	}
	start, end, ok := parseWindow(w, req.WindowStart, req.WindowEnd)
	if !ok {
		return
	}
	res, err := h.svc.Report(r.Context(), ReportInput{
		Service:     service,
		EventID:     req.EventID,
		WindowStart: start,
		WindowEnd:   end,
		Increment:   Increment{Total: req.Total, Failed: req.Failed},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) correct(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	var req correctionRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := h.svc.Correct(r.Context(), CorrectionInput{
		Service:         service,
		EventID:         req.EventID,
		CorrectsEventID: req.CorrectsEventID,
		TotalDelta:      req.TotalDelta,
		FailedDelta:     req.FailedDelta,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (h *HTTPHandler) queryBudget(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	start, end, ok := parseWindow(w, r.URL.Query().Get("start"), r.URL.Query().Get("end"))
	if !ok {
		return
	}
	view, err := h.svc.QueryBudget(start, end, service)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *HTTPHandler) listEvents(w http.ResponseWriter, r *http.Request) {
	service := r.PathValue("service")
	events, err := h.svc.ListEvents(service)
	if err != nil {
		writeError(w, err)
		return
	}
	if events == nil {
		events = []Event{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (h *HTTPHandler) createGate(w http.ResponseWriter, r *http.Request) {
	var req createGateRequest
	if !decode(w, r, &req) {
		return
	}
	p := req.Policy
	start, end, ok := parseWindow(w, p.WindowStart, p.WindowEnd)
	if !ok {
		return
	}
	v, err := h.svc.CreateGate(r.Context(), CreateGateInput{
		GateID:    req.GateID,
		RequestID: req.RequestID,
		Policy: Policy{
			Service:           p.Service,
			WindowStart:       start,
			WindowEnd:         end,
			ErrorBudget:       p.ErrorBudget,
			AllowReevaluation: p.AllowReevaluation,
		},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *HTTPHandler) reevaluate(w http.ResponseWriter, r *http.Request) {
	gateID := r.PathValue("gateID")
	var req reevaluateRequest
	if !decode(w, r, &req) {
		return
	}
	v, err := h.svc.ReevaluateGate(r.Context(), gateID, req.RequestID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *HTTPHandler) getGate(w http.ResponseWriter, r *http.Request) {
	g, err := h.svc.GetGate(r.PathValue("gateID"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (h *HTTPHandler) check(w http.ResponseWriter, r *http.Request) {
	gateID := r.PathValue("gateID")
	version, err := strconv.ParseInt(r.PathValue("version"), 10, 64)
	if err != nil {
		writeError(w, errf(ErrInvalidArgument, "版本号不是合法整数: %q", r.PathValue("version")))
		return
	}
	v, err := h.svc.CheckDecision(gateID, version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// ---- HTTP 辅助 ----------------------------------------------------------

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, errf(ErrInvalidArgument, "请求体不是合法 JSON: %v", err))
		return false
	}
	return true
}

func parseWindow(w http.ResponseWriter, startStr, endStr string) (time.Time, time.Time, bool) {
	if startStr == "" || endStr == "" {
		writeError(w, errf(ErrInvalidArgument, "缺少窗口时间参数 start/end（RFC3339）"))
		return time.Time{}, time.Time{}, false
	}
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		writeError(w, errf(ErrInvalidArgument, "start 不是合法 RFC3339 时间: %v", err))
		return time.Time{}, time.Time{}, false
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		writeError(w, errf(ErrInvalidArgument, "end 不是合法 RFC3339 时间: %v", err))
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// statusForError 把领域错误码映射为合适的 HTTP 状态码。
func statusForError(code ErrorCode) int {
	switch code {
	case CodeInvalidArgument, CodeInvalidWindow, CodeInvalidIncrement:
		return http.StatusBadRequest
	case CodeConstraintViolated, CodeOverflow:
		return http.StatusUnprocessableEntity
	case CodeConflict:
		return http.StatusConflict
	case CodeNotFound:
		return http.StatusNotFound
	case CodeAlreadyExists:
		return http.StatusConflict
	case CodeReevaluationNotAllowed, CodeVersionSuperseded:
		return http.StatusConflict
	case CodeDecisionDenied:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, err error) {
	var se *Error
	if errors.As(err, &se) {
		writeJSON(w, statusForError(se.Code), errorResponse{Code: se.Code, Message: se.Error()})
		return
	}
	writeJSON(w, http.StatusInternalServerError, errorResponse{
		Code:    ErrorCode("internal_error"),
		Message: err.Error(),
	})
}
