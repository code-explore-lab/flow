package hls

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// blobStore 保存 init / part / segment 的字节。
//
// Track 的所有读写都在自己的互斥锁内完成，因此实现不需要自带锁。
// 内存 store 保持 P-2 的原有行为；磁盘 store 是 O-6 DVR 的后端，
// 让 5 分钟的时移窗口不必常驻内存。
type blobStore interface {
	Put(uri string, data []byte) error
	Get(uri string) ([]byte, bool)
	Size(uri string) int64
	Delete(uri string)
	Bytes() int64
	Close()
}

// ---- 内存 ----

type memStore struct {
	m map[string][]byte
	n int64
}

func newMemStore() *memStore {
	return &memStore{m: make(map[string][]byte)}
}

func (s *memStore) Put(uri string, data []byte) error {
	if old, ok := s.m[uri]; ok {
		s.n -= int64(len(old))
	}
	s.m[uri] = data
	s.n += int64(len(data))
	return nil
}

func (s *memStore) Get(uri string) ([]byte, bool) {
	b, ok := s.m[uri]
	return b, ok
}

func (s *memStore) Size(uri string) int64 {
	if b, ok := s.m[uri]; ok {
		return int64(len(b))
	}
	return 0
}

func (s *memStore) Delete(uri string) {
	if b, ok := s.m[uri]; ok {
		s.n -= int64(len(b))
		delete(s.m, uri)
	}
}

func (s *memStore) Bytes() int64 { return s.n }

// Close 是空操作：非 DVR 模式下 Track 关闭后仍然允许读已生成的段。
func (s *memStore) Close() {}

// ---- 磁盘 ----

// diskStore 把一条轨的 blob 写到 DVR 目录。URI 形如 "video/seg-3.m4s"，
// 直接映射成 root/video/seg-3.m4s；root 是整个流的 DVR 目录，两条轨共用。
type diskStore struct {
	root string // 流级目录，由 Packager 拥有
	kind string // "video" 或 "audio"
	m    map[string]int64
	n    int64
}

func newDiskStore(root, kind string) (*diskStore, error) {
	if root == "" {
		return nil, errors.New("hls: empty dvr root")
	}
	if err := os.MkdirAll(filepath.Join(root, kind), 0o755); err != nil {
		return nil, err
	}
	return &diskStore{root: root, kind: kind, m: make(map[string]int64)}, nil
}

func (s *diskStore) path(uri string) (string, error) {
	if uri == "" || strings.Contains(uri, `\`) || strings.Contains(uri, "..") ||
		filepath.IsAbs(uri) || !strings.HasPrefix(uri, s.kind+"/") {
		return "", fmt.Errorf("hls: invalid dvr uri %q", uri)
	}
	return filepath.Join(s.root, filepath.FromSlash(uri)), nil
}

func (s *diskStore) Put(uri string, data []byte) error {
	p, err := s.path(uri)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	// 段只有完整生成后才会进 playlist，因此不需要 tmp+rename。
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return err
	}
	if old, ok := s.m[uri]; ok {
		s.n -= old
	}
	s.m[uri] = int64(len(data))
	s.n += int64(len(data))
	return nil
}

func (s *diskStore) Get(uri string) ([]byte, bool) {
	if _, ok := s.m[uri]; !ok {
		return nil, false
	}
	p, err := s.path(uri)
	if err != nil {
		return nil, false
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, false
	}
	return b, true
}

func (s *diskStore) Size(uri string) int64 { return s.m[uri] }

func (s *diskStore) Delete(uri string) {
	size, ok := s.m[uri]
	if !ok {
		return
	}
	if p, err := s.path(uri); err == nil {
		_ = os.Remove(p)
	}
	delete(s.m, uri)
	s.n -= size
}

func (s *diskStore) Bytes() int64 { return s.n }

// Close 只删除本轨的子目录；整路流的目录由 Packager.Close 清理，
// 避免视频轨先结束时把音频轨的段一起删掉。
func (s *diskStore) Close() {
	_ = os.RemoveAll(filepath.Join(s.root, s.kind))
}

// ---- DVR 目录 ----

var dvrSeq atomic.Uint64

// newDVDRoot 为一次 Packager 实例分配唯一的目录。
//
// 唯一性是必要的：同名流被替换时，旧 packager 的 Close 与新 packager 的
// 初始化可能并发，共用目录会互相删除。进程启动时 PrepareDVR 会清空
// 上次运行留下的目录。
func newDVDRoot(opts Options, name string) string {
	if opts.DVRWindow <= 0 || strings.TrimSpace(opts.DVRDir) == "" {
		return ""
	}
	token := fmt.Sprintf("%d-%d", os.Getpid(), dvrSeq.Add(1))
	return filepath.Join(opts.DVRDir, sanitizeDVRName(name)+"-"+token)
}

// PrepareDVR 清空 DVR 根目录并重建。DVR 是环形时移缓冲而不是归档，
// 进程重启后旧内容没有意义；调用方应在启动时执行一次。
func PrepareDVR(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("hls: empty dvr dir")
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

func sanitizeDVRName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 48 {
			break
		}
	}
	if b.Len() == 0 {
		b.WriteString("stream")
	}
	sum := sha256.Sum256([]byte(name))
	return b.String() + "-" + hex.EncodeToString(sum[:4])
}

// newTrackStore 按配置选择磁盘或内存后端；磁盘不可用时回退内存，
// 保证 DVR 配置错误不会让 HLS/DASH 整体不可用。
func newTrackStore(opts Options, root, kind, name string) blobStore {
	if opts.DVRWindow > 0 && root != "" {
		s, err := newDiskStore(root, kind)
		if err == nil {
			return s
		}
		opts.logf("hls[%s/%s]: dvr store: %v (falling back to memory)", name, kind, err)
	}
	return newMemStore()
}
