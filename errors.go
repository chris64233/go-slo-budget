package slobudget

import "fmt"

// ErrorKind 是服务对外暴露的明确错误类型。
type ErrorKind string

const (
	// KindValidation 入参不合法（空字段、非法窗口、非法增量等）。
	KindValidation ErrorKind = "validation_error"
	// KindConflict 幂等号（外部事件号 / 请求号）被复用于不同内容。
	KindConflict ErrorKind = "idempotency_conflict"
	// KindNotFound 被引用的对象（事件、门禁、版本）不存在。
	KindNotFound ErrorKind = "not_found"
	// KindInvariant 应用增量后违反指标不变量（负累计或失败数超过总数）。
	KindInvariant ErrorKind = "metric_invariant_violation"
	// KindPolicyDenied 当前策略不允许该操作（如门禁禁止重新评估）。
	KindPolicyDenied ErrorKind = "policy_denied"
	// KindObsolete 校验的门禁版本已被新版本取代，旧版本决定不可再用。
	KindObsolete ErrorKind = "obsolete_decision"
	// KindReleaseBlocked 当前门禁版本的决定是阻止发布。
	KindReleaseBlocked ErrorKind = "release_blocked"
)

// Error 是 slobudget 包返回的结构化错误，可通过 Kind 程序化区分处理。
type Error struct {
	Kind ErrorKind
	Op   string
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("%s: %s", e.Op, e.Msg)
	}
	return e.Msg
}

// Unwrap 支持内部错误链。
func (e *Error) Unwrap() error { return e.Err }

// Is 使 errors.Is 可以按错误类型（Kind）匹配哨兵错误。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Kind == e.Kind
}

// 各错误类型的哨兵，调用方推荐使用 errors.Is(err, slobudget.ErrXxx) 判断。
var (
	ErrValidation     = &Error{Kind: KindValidation}
	ErrConflict       = &Error{Kind: KindConflict}
	ErrNotFound       = &Error{Kind: KindNotFound}
	ErrInvariant      = &Error{Kind: KindInvariant}
	ErrPolicyDenied   = &Error{Kind: KindPolicyDenied}
	ErrObsolete       = &Error{Kind: KindObsolete}
	ErrReleaseBlocked = &Error{Kind: KindReleaseBlocked}
)

func newError(op string, kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Op: op, Msg: fmt.Sprintf(format, args...)}
}

func validationErr(op, format string, args ...any) *Error {
	return newError(op, KindValidation, format, args...)
}

func conflictErr(op, format string, args ...any) *Error {
	return newError(op, KindConflict, format, args...)
}

func notFoundErr(op, format string, args ...any) *Error {
	return newError(op, KindNotFound, format, args...)
}

func invariantErr(op, format string, args ...any) *Error {
	return newError(op, KindInvariant, format, args...)
}
