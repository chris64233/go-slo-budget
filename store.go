package slobudget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// State 是持久化的全部数据：不可变事件（上报、修正）与门禁（含全部版本）。
// 事件一旦写入只能追加；修正以新事件的形式引用原事件，绝不覆盖。
type State struct {
	Reports     []MetricReport     `json:"reports"`
	Corrections []MetricCorrection `json:"corrections"`
	Gates       map[string]*Gate   `json:"gates"`
	// GateRequests 以请求号记录创建/重评估的幂等结果。
	GateRequests map[string]GateRequestRecord `json:"gate_requests"`
	// GateSeq 是自动生成门禁 ID 的计数器。
	GateSeq int `json:"gate_seq"`
}

// GateRequestRecord 记录某个请求号对应的门禁操作结果，用于幂等重放与冲突检测。
type GateRequestRecord struct {
	Kind    string `json:"kind"` // "create" 或 "reevaluate"
	GateID  string `json:"gate_id"`
	Version int    `json:"version"`
	// InputHash 是请求关键字段的指纹；同请求号不同内容据此判定冲突。
	InputHash string `json:"input_hash"`
}

// Store 是状态持久化接口。Save 必须保证整体原子落盘。
type Store interface {
	Load(ctx context.Context) (*State, error)
	Save(ctx context.Context, state *State) error
}

// MemoryStore 把状态保存在内存中，主要用于测试。
type MemoryStore struct {
	mu    sync.Mutex
	state *State
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (m *MemoryStore) Load(_ context.Context) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return newState(), nil
	}
	return cloneState(m.state)
}

func (m *MemoryStore) Save(_ context.Context, state *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, err := cloneState(state)
	if err != nil {
		return err
	}
	m.state = cp
	return nil
}

// FileStore 把整体状态以 JSON 落盘，写入采用“临时文件 + 原子重命名”，
// 避免进程崩溃留下半截文件。
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore 创建指向 path 的文件存储（文件可尚不存在）。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (f *FileStore) Load(_ context.Context) (*State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	state.ensure()
	return &state, nil
}

func (f *FileStore) Save(_ context.Context, state *State) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("commit state file: %w", err)
	}
	return nil
}

func newState() *State {
	s := &State{
		Reports:      []MetricReport{},
		Corrections:  []MetricCorrection{},
		Gates:        map[string]*Gate{},
		GateRequests: map[string]GateRequestRecord{},
	}
	return s
}

func (s *State) ensure() {
	if s.Reports == nil {
		s.Reports = []MetricReport{}
	}
	if s.Corrections == nil {
		s.Corrections = []MetricCorrection{}
	}
	if s.Gates == nil {
		s.Gates = map[string]*Gate{}
	}
	if s.GateRequests == nil {
		s.GateRequests = map[string]GateRequestRecord{}
	}
}

// cloneState 通过 JSON 往返生成深拷贝，隔离调用方与已持久化的状态。
func cloneState(s *State) (*State, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var cp State
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	cp.ensure()
	return &cp, nil
}
