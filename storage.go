package slobudget

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Store 是服务的持久化抽象。所有方法都在服务级互斥锁内被调用，
// 实现可以不做自身并发控制，但必须保证数据落盘以满足崩溃恢复。
type Store interface {
	// Load 在服务启动时加载全部持久化状态。
	Load(ctx context.Context) (*persistedState, error)
	// AppendEvent 原子地追加一条指标事件（原始上报或修正）。
	AppendEvent(ctx context.Context, ev Event) error
	// AppendGateVersion 原子地追加一个新版本，并把旧版本标记为被取代、
	// 记录门禁的当前版本指针。create 为 true 时这是门禁的第一个版本。
	AppendGateVersion(ctx context.Context, gateID string, create bool, v GateVersion) error
}

// persistedState 是启动时从持久层还原的全部状态。
type persistedState struct {
	Events       []Event
	Gates        map[string]*Gate
	VersionsByID map[string]map[int64]*GateVersion
	LastEventSeq int64
}

// MemoryStore 是纯内存存储，主要用于测试；进程退出数据即丢失。
// 生产场景应使用 WALStore 或实现 Store 的数据库适配器。
type MemoryStore struct {
	mu           sync.Mutex
	Events       []Event
	Gates        map[string]*Gate
	VersionsByID map[string]map[int64]*GateVersion
	LastEventSeq int64
}

// NewMemoryStore 创建一个空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		Gates:        map[string]*Gate{},
		VersionsByID: map[string]map[int64]*GateVersion{},
	}
}

func (m *MemoryStore) Load(context.Context) (*persistedState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshot(), nil
}

func (m *MemoryStore) snapshot() *persistedState {
	gates := make(map[string]*Gate, len(m.Gates))
	versions := make(map[string]map[int64]*GateVersion, len(m.VersionsByID))
	for id, g := range m.Gates {
		gates[id] = cloneGate(g)
	}
	for id, vs := range m.VersionsByID {
		cp := make(map[int64]*GateVersion, len(vs))
		for n, v := range vs {
			cp[n] = cloneGateVersion(v)
		}
		versions[id] = cp
	}
	events := make([]Event, len(m.Events))
	copy(events, m.Events)
	return &persistedState{
		Events:       events,
		Gates:        gates,
		VersionsByID: versions,
		LastEventSeq: m.LastEventSeq,
	}
}

func (m *MemoryStore) AppendEvent(_ context.Context, ev Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events = append(m.Events, ev)
	m.LastEventSeq = ev.Seq
	return nil
}

func (m *MemoryStore) AppendGateVersion(_ context.Context, gateID string, create bool, v GateVersion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.Gates[gateID]
	if !ok {
		if !create {
			return errf(ErrNotFound, "门禁 %q 不存在", gateID)
		}
		g = &Gate{GateID: gateID, CreatedAt: v.CreatedAt}
		m.Gates[gateID] = g
		m.VersionsByID[gateID] = map[int64]*GateVersion{}
	}
	stored := cloneGateVersion(&v)
	for _, old := range g.Versions {
		old.Superseded = true
	}
	g.Versions = append(g.Versions, stored)
	m.VersionsByID[gateID][v.Version] = stored
	g.CurrentVersion = v.Version
	return nil
}

// ---- WAL 持久化 ---------------------------------------------------------

// 日志记录类型。
const (
	walMagic   = "SLOBUDGET-WAL"
	walVersion = uint16(1)

	recEvent       = byte(1)
	recGateVersion = byte(2)
)

// walRecord 是 WAL 中的一条记录。
type walRecord struct {
	Type    byte
	Event   *Event       `json:",omitempty"`
	GateID  string       `json:",omitempty"`
	Create  bool         `json:",omitempty"`
	Version *GateVersion `json:",omitempty"`
}

// WALStore 使用只追加（append-only）日志做持久化：
// 每次事件上报/修正、门禁版本签发都先同步落盘再更新内存状态，
// 启动时顺序重放日志即可恢复全部数据与决策依据。
//
// 记录帧格式（大端）：
//
//	magic(12B) | version(2B)，仅文件头
//	对每条记录：type(1B) | payloadLen(4B) | payload(json) | crc32(4B)
//
// crc32 覆盖 type + payloadLen + payload，用于检测日志截断/损坏。
type WALStore struct {
	mu  sync.Mutex
	f   *os.File
	dir string
}

// NewWALStore 打开（不存在则创建）dir 下的 wal.log，
// 但不立即重放——重放由 Service 启动时调用 Load 完成。
func NewWALStore(dir string) (*WALStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "创建 WAL 目录 %q", dir)
	}
	path := dir + string(os.PathSeparator) + "wal.log"
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "打开 WAL 文件 %q", path)
	}
	s := &WALStore{f: f, dir: dir}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, wrapErr(ErrInvalidArgument, err, "读取 WAL 文件信息")
	}
	if info.Size() == 0 {
		hdr := make([]byte, len(walMagic)+2)
		copy(hdr, walMagic)
		binary.BigEndian.PutUint16(hdr[len(walMagic):], walVersion)
		if _, err := f.Write(hdr); err != nil {
			_ = f.Close()
			return nil, wrapErr(ErrInvalidArgument, err, "写 WAL 文件头")
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, wrapErr(ErrInvalidArgument, err, "同步 WAL 文件头")
		}
	}
	return s, nil
}

// Close 关闭底层文件。
func (s *WALStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *WALStore) appendRecord(rec walRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return wrapErr(ErrInvalidArgument, err, "编码 WAL 记录")
	}
	frame := make([]byte, 0, 5+len(payload)+4)
	frame = append(frame, rec.Type)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)

	h := crc32.NewIEEE()
	_, _ = h.Write(frame)
	frame = binary.BigEndian.AppendUint32(frame, h.Sum32())

	if _, err := s.f.Write(frame); err != nil {
		return wrapErr(ErrInvalidArgument, err, "写 WAL 记录")
	}
	if err := s.f.Sync(); err != nil {
		return wrapErr(ErrInvalidArgument, err, "同步 WAL 记录")
	}
	return nil
}

// AppendEvent 落盘一条事件记录。
func (s *WALStore) AppendEvent(_ context.Context, ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendRecord(walRecord{Type: recEvent, Event: &ev})
}

// AppendGateVersion 落盘一条门禁版本记录。
func (s *WALStore) AppendGateVersion(_ context.Context, gateID string, create bool, v GateVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendRecord(walRecord{Type: recGateVersion, GateID: gateID, Create: create, Version: &v})
}

// Load 顺序重放整个 WAL，还原状态。尾部不完整/CRC 失败的记录
// 被视为截断日志而停止重放（此前完整记录均有效）。
func (s *WALStore) Load(context.Context) (*persistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "定位 WAL 文件头")
	}
	hdr := make([]byte, len(walMagic)+2)
	if _, err := io.ReadFull(s.f, hdr); err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "读 WAL 文件头")
	}
	if string(hdr[:len(walMagic)]) != walMagic {
		return nil, errf(ErrInvalidArgument, "WAL 文件魔数不匹配")
	}

	st := &persistedState{
		Gates:        map[string]*Gate{},
		VersionsByID: map[string]map[int64]*GateVersion{},
	}

	var validOffset int64 = int64(len(hdr))
	r := s.f
	for {
		frameHead := make([]byte, 5)
		n, err := io.ReadFull(r, frameHead)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// 读到干净的 EOF（0 字节）正常结束；读到半截帧头说明尾部截断。
			if n > 0 {
				break // 丢弃截断尾部
			}
			break
		}
		if err != nil {
			return nil, wrapErr(ErrInvalidArgument, err, "读 WAL 帧头")
		}
		recType := frameHead[0]
		payloadLen := binary.BigEndian.Uint32(frameHead[1:5])
		if payloadLen > 64*1024*1024 {
			return nil, errf(ErrInvalidArgument, "WAL 记录长度异常: %d", payloadLen)
		}
		body := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(r, body); err != nil {
			// 帧体截断：停止重放，保留此前完整记录。
			break
		}
		payload := body[:payloadLen]
		gotCRC := binary.BigEndian.Uint32(body[payloadLen:])
		h := crc32.NewIEEE()
		_, _ = h.Write(frameHead)
		_, _ = h.Write(payload)
		if gotCRC != h.Sum32() {
			return nil, errf(ErrInvalidArgument, "WAL CRC 校验失败，偏移 %d", validOffset)
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return nil, wrapErr(ErrInvalidArgument, err, "解码 WAL 记录")
		}
		rec.Type = recType // 帧字节是记录类型的权威来源
		if err := st.apply(rec); err != nil {
			return nil, err
		}
		// 记录 5 + payloadLen + 4 字节，记录新的有效偏移。
		validOffset += int64(5 + payloadLen + 4)
	}

	// 截断尾部写偏移，后续追加从最后一条完整记录之后开始。
	if _, err := s.f.Seek(validOffset, io.SeekStart); err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "定位 WAL 写偏移")
	}
	if err := s.f.Truncate(validOffset); err != nil {
		return nil, wrapErr(ErrInvalidArgument, err, "截断 WAL 尾部")
	}

	return st, nil
}

func (st *persistedState) apply(rec walRecord) error {
	switch rec.Type {
	case recEvent:
		if rec.Event == nil {
			return errf(ErrInvalidArgument, "WAL 事件记录缺少内容")
		}
		st.Events = append(st.Events, *rec.Event)
		if rec.Event.Seq > st.LastEventSeq {
			st.LastEventSeq = rec.Event.Seq
		}
	case recGateVersion:
		if rec.Version == nil {
			return errf(ErrInvalidArgument, "WAL 门禁记录缺少内容")
		}
		g, ok := st.Gates[rec.GateID]
		if !ok {
			g = &Gate{GateID: rec.GateID, CreatedAt: rec.Version.CreatedAt}
			st.Gates[rec.GateID] = g
			st.VersionsByID[rec.GateID] = map[int64]*GateVersion{}
		}
		for _, old := range g.Versions {
			old.Superseded = true
		}
		g.Versions = append(g.Versions, rec.Version)
		g.CurrentVersion = rec.Version.Version
		st.VersionsByID[rec.GateID][rec.Version.Version] = rec.Version
	default:
		return errf(ErrInvalidArgument, "未知的 WAL 记录类型 %d", rec.Type)
	}
	return nil
}

// ---- 深拷贝辅助 ---------------------------------------------------------

func cloneGate(g *Gate) *Gate {
	cp := &Gate{
		GateID:         g.GateID,
		CurrentVersion: g.CurrentVersion,
		CreatedAt:      g.CreatedAt,
		Versions:       make([]*GateVersion, len(g.Versions)),
	}
	for i, v := range g.Versions {
		cp.Versions[i] = cloneGateVersion(v)
	}
	return cp
}

func cloneGateVersion(v *GateVersion) *GateVersion {
	cp := *v
	cp.Snapshot.Budgets = append([]Budget(nil), v.Snapshot.Budgets...)
	return &cp
}
