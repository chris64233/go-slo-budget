package slobudget

import "fmt"

// ErrorCode 是服务对外暴露的稳定错误类别，便于调用方按类型处理。
type ErrorCode string

const (
	// CodeInvalidArgument 请求参数本身不合法（字段缺失、数值非法等）。
	CodeInvalidArgument ErrorCode = "invalid_argument"
	// CodeInvalidWindow 窗口不是合法的左闭右开区间（start >= end）。
	CodeInvalidWindow ErrorCode = "invalid_window"
	// CodeInvalidIncrement 增量非法：负数，或失败增量大于总增量。
	CodeInvalidIncrement ErrorCode = "invalid_increment"
	// CodeConstraintViolated 修正会破坏不变量：累计为负，或失败累计超过总累计。
	CodeConstraintViolated ErrorCode = "constraint_violated"
	// CodeConflict 幂等号已存在但携带了不同内容。
	CodeConflict ErrorCode = "conflict"
	// CodeNotFound 引用的对象不存在（事件、门禁、版本等）。
	CodeNotFound ErrorCode = "not_found"
	// CodeAlreadyExists 门禁已存在，不能重复创建。
	CodeAlreadyExists ErrorCode = "already_exists"
	// CodeReevaluationNotAllowed 当前策略不允许重新评估。
	CodeReevaluationNotAllowed ErrorCode = "reevaluation_not_allowed"
	// CodeVersionSuperseded 被校验的版本不是当前版本，旧批准不能用于当前发布。
	CodeVersionSuperseded ErrorCode = "version_superseded"
	// CodeDecisionDenied 当前版本的决定是阻止发布。
	CodeDecisionDenied ErrorCode = "decision_denied"
	// CodeOverflow 累计求和超出 int64 范围。
	CodeOverflow ErrorCode = "overflow"
)

// Error 是 slobudget 包返回的统一错误类型。
// 通过 errors.Is(err, ErrConflict) 等方式按错误码判别。
type Error struct {
	Code   ErrorCode
	Detail string
	Err    error
}

func (e *Error) Error() string {
	s := string(e.Code)
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Is 按错误码匹配哨兵错误。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

func (e *Error) Unwrap() error { return e.Err }

// 哨兵错误，调用方使用 errors.Is 判别。
var (
	ErrInvalidArgument        = &Error{Code: CodeInvalidArgument}
	ErrInvalidWindow          = &Error{Code: CodeInvalidWindow}
	ErrInvalidIncrement       = &Error{Code: CodeInvalidIncrement}
	ErrConstraintViolated     = &Error{Code: CodeConstraintViolated}
	ErrConflict               = &Error{Code: CodeConflict}
	ErrNotFound               = &Error{Code: CodeNotFound}
	ErrAlreadyExists          = &Error{Code: CodeAlreadyExists}
	ErrReevaluationNotAllowed = &Error{Code: CodeReevaluationNotAllowed}
	ErrVersionSuperseded      = &Error{Code: CodeVersionSuperseded}
	ErrDecisionDenied         = &Error{Code: CodeDecisionDenied}
	ErrOverflow               = &Error{Code: CodeOverflow}
)

func errf(sentinel *Error, format string, args ...any) error {
	return &Error{Code: sentinel.Code, Detail: fmt.Sprintf(format, args...)}
}

func wrapErr(sentinel *Error, cause error, format string, args ...any) error {
	return &Error{Code: sentinel.Code, Detail: fmt.Sprintf(format, args...), Err: cause}
}
