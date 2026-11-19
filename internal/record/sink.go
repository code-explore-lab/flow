package record

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"flow/internal/stream"
)

// segmentWriter 一个容器写出的公共接口。Write 收到的样本已经聚合成 AU，
// 时间轴由轨道 goroutine 修补过；writer 只负责换算成本容器的时间基和轮转。
type segmentWriter interface {
	Write(s sample) error
	Close() error
}

// baseClock 一个文件内一条轨的时间轴。
//
// fresh：第一个样本的 DTS 映射到 0；
// resume：第一个样本映射到 .state 里记录的 next，续在已落盘内容之后。
type baseClock struct {
	have bool
	base int64
	next int64
}

// peek 只计算不推进，用于“这个关键帧会不会触发轮转”之类的判断。
func (c *baseClock) peek(raw int64) int64 {
	if !c.have {
		return c.next
	}
	d := raw - c.base
	if d < c.next {
		d = c.next
	}
	return d
}

// normalize 计算本样本在文件里的 DTS，并推进 next。
func (c *baseClock) normalize(raw int64) int64 {
	if !c.have {
		c.have = true
		c.base = raw - c.next
	}
	d := raw - c.base
	if d < c.next {
		d = c.next
	}
	c.next = d
	return d
}

// advance 把 next 推进到样本结束位置。
func (c *baseClock) advance(d int64, dur uint32) {
	if end := d + int64(dur); end > c.next {
		c.next = end
	}
}

// writerBase 与容器无关的文件、状态、配额和轮转逻辑。
// 子类负责在 header 里写自己的容器头/初始化段。
type writerBase struct {
	opts   Options
	name   string
	prefix string

	statePath string
	quota     *quota

	file     *os.File
	path     string
	size     int64
	initHash string
	fileSeq  int

	hasVideo bool
	hasAudio bool
	video    *stream.VideoCodec
	audio    *stream.AudioCodec

	vclock baseClock
	aclock baseClock

	nextFrag  uint32
	lastState time.Time
}

// newSegmentWriter 按容器格式创建写出器。
func newSegmentWriter(opts Options, name string, video *stream.VideoCodec, audio *stream.AudioCodec, hasVideo, hasAudio bool) (segmentWriter, error) {
	switch opts.Format {
	case FormatMP4:
		return newMP4Writer(opts, name, video, audio, hasVideo, hasAudio)
	case FormatFLV:
		return newFLVWriter(opts, name, video, audio, hasVideo, hasAudio)
	case FormatTS:
		return newTSWriter(opts, name, video, audio, hasVideo, hasAudio)
	}
	return nil, fmt.Errorf("record: unsupported format %q", opts.Format)
}

func newWriterBase(opts Options, name string, video *stream.VideoCodec, audio *stream.AudioCodec, hasVideo, hasAudio bool) writerBase {
	prefix := sanitizePrefix(name)
	return writerBase{
		opts:      opts,
		name:      name,
		prefix:    prefix,
		statePath: filepath.Join(opts.Dir, prefix+".state"),
		quota: &quota{
			dir:       opts.Dir,
			prefix:    prefix,
			max:       opts.Quota,
			retention: opts.Retention,
			now:       opts.Now,
			logf:      opts.Logf,
		},
		initHash: hashParams(hasVideo, video, hasAudio, audio),
		hasVideo: hasVideo,
		hasAudio: hasAudio,
		video:    video,
		audio:    audio,
		nextFrag: 1,
	}
}

// open 打开第一个文件：能续录就追加，否则新建。header 是新文件的容器头/init。
func (b *writerBase) open(header []byte) error {
	if b.opts.Resume {
		ok, err := b.openResume(int64(len(header)))
		if err != nil {
			return err
		}
		if ok {
			b.opts.logf("record[%s]: resumed %s at %d bytes", b.name, filepath.Base(b.path), b.size)
			return nil
		}
	}
	return b.openFresh(header)
}

func (b *writerBase) openFresh(header []byte) error {
	if b.file != nil {
		return errors.New("record: file already open")
	}
	if err := b.quota.enforce("", b.statePath); err != nil {
		return err
	}
	path, err := b.nextPath()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("record: create %s: %w", path, err)
	}
	if len(header) > 0 {
		if _, err := f.Write(header); err != nil {
			f.Close()
			os.Remove(path)
			return fmt.Errorf("record: write header %s: %w", path, err)
		}
	}
	b.file = f
	b.path = path
	b.size = int64(len(header))
	b.vclock = baseClock{}
	b.aclock = baseClock{}
	b.nextFrag = 1
	b.lastState = b.opts.Now()
	if err := b.writeState(); err != nil {
		b.closeFile(false)
		return err
	}
	// 第一个媒体包到达时立刻再写一次状态：崩溃后至少有文件头/第一个
	// fragment 可以续，而不是等满 1 秒的节流窗口。
	b.lastState = time.Time{}
	return nil
}

// openResume 尝试接着上一次崩溃留下的文件写。返回 false 表示没有可续的
// 状态（或参数集变化），调用方应新建文件。
func (b *writerBase) openResume(minSize int64) (bool, error) {
	st, err := readState(b.statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		// 状态文件损坏时不能盲目追加，删掉它，保留已录文件。
		b.opts.logf("record[%s]: bad state file %s: %v", b.name, b.statePath, err)
		os.Remove(b.statePath)
		return false, nil
	}
	if st.Format != string(b.opts.Format) || st.InitHash != b.initHash {
		return false, nil
	}
	f, err := os.OpenFile(st.Path, os.O_RDWR, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			os.Remove(b.statePath)
			return false, nil
		}
		return false, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return false, err
	}
	size := st.Size
	if size > info.Size() {
		size = info.Size()
	}
	if size < minSize {
		// 崩溃发生在写完容器头之前：这个文件没有价值，删掉重来。
		f.Close()
		os.Remove(st.Path)
		os.Remove(b.statePath)
		return false, nil
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return false, fmt.Errorf("record: truncate %s: %w", st.Path, err)
	}
	if _, err := f.Seek(size, io.SeekStart); err != nil {
		f.Close()
		return false, err
	}
	b.file = f
	b.path = st.Path
	b.size = size
	b.nextFrag = st.NextFragment
	if b.nextFrag == 0 {
		b.nextFrag = 1
	}
	// 崩溃点两条轨的落盘进度通常不一样（一般音频落后视频几百毫秒）。
	// 如果各自按自己的 next 续，新推流的音视频都从 0 开始，就会把这段
	// 差值变成整段录制里固定的音画偏移。统一取较晚的一条轨作为续录起点，
	// 另一条补一个（崩溃时本来就会丢的）间隙。
	audioNext, videoNext := st.AudioNext, st.VideoNext
	if b.hasVideo && b.hasAudio && b.audio != nil {
		rate := int64(audioClockRate(b.audio))
		if rate > 0 && audioNext*int64(videoTimescale) > videoNext*rate {
			videoNext = audioNext * int64(videoTimescale) / rate
		} else if rate > 0 {
			audioNext = videoNext * rate / int64(videoTimescale)
		}
	}
	b.vclock.next = videoNext
	b.aclock.next = audioNext
	b.lastState = b.opts.Now()
	return true, nil
}

// rotate 关闭当前文件并新建一个。子类在调用前把未写出的 fragment 冲刷掉。
func (b *writerBase) rotate(header []byte) error {
	if err := b.closeFile(true); err != nil {
		return err
	}
	return b.openFresh(header)
}

// closeFile 同步并关闭当前文件。clean=true 表示这段已经完整，删除 .state；
// clean=false 用于磁盘错误等场景，保留 .state 供下次续录。
func (b *writerBase) closeFile(clean bool) error {
	if b.file == nil {
		return nil
	}
	err := b.file.Sync()
	if cerr := b.file.Close(); err == nil {
		err = cerr
	}
	b.file = nil
	if clean {
		if rerr := os.Remove(b.statePath); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			b.opts.logf("record[%s]: remove state: %v", b.name, rerr)
		}
	}
	return err
}

func (b *writerBase) nextPath() (string, error) {
	stamp := b.opts.Now().Format("20060102-150405")
	for i := 0; i < 10000; i++ {
		b.fileSeq++
		path := filepath.Join(b.opts.Dir, fmt.Sprintf("%s-%s-%06d.%s", b.prefix, stamp, b.fileSeq, b.opts.Format))
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return path, nil
		}
	}
	return "", errors.New("record: cannot allocate a unique file name")
}

// fileState 是续录所需的全部信息。Size 只记录已完整落盘的字节数；
// 进程崩溃后从这里截断，丢掉半截 fragment。
type fileState struct {
	Version      int    `json:"version"`
	Format       string `json:"format"`
	Path         string `json:"path"`
	Size         int64  `json:"size"`
	InitHash     string `json:"init_hash"`
	NextFragment uint32 `json:"next_fragment"`
	VideoNext    int64  `json:"video_next"`
	AudioNext    int64  `json:"audio_next"`
	Updated      int64  `json:"updated"`
}

func (b *writerBase) writeState() error {
	return b.writeStateAt(b.opts.Now())
}

func (b *writerBase) writeStateAt(now time.Time) error {
	if b.file == nil {
		return nil
	}
	st := fileState{
		Version:      1,
		Format:       string(b.opts.Format),
		Path:         b.path,
		Size:         b.size,
		InitHash:     b.initHash,
		NextFragment: b.nextFrag,
		VideoNext:    b.vclock.next,
		AudioNext:    b.aclock.next,
		Updated:      now.UnixNano(),
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := b.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("record: write state: %w", err)
	}
	if err := os.Rename(tmp, b.statePath); err != nil {
		return fmt.Errorf("record: rename state: %w", err)
	}
	b.lastState = now
	return nil
}

// maybeState 限制状态文件写入频率；force=true 时立即写。
func (b *writerBase) maybeState(force bool) error {
	if !force && b.opts.Now().Sub(b.lastState) < time.Second {
		return nil
	}
	return b.writeState()
}

func readState(path string) (*fileState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var st fileState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Path == "" || st.Size <= 0 {
		return nil, errors.New("record: incomplete state")
	}
	return &st, nil
}

// quota 按目录 + 流名前缀统计录制文件，超限时删除最旧的文件。
// 只统计已完成/正在写的片段文件（<prefix>-*.ext），不会碰 .state。
type quota struct {
	dir       string
	prefix    string
	max       int64
	retention time.Duration
	now       func() time.Time
	logf      func(format string, args ...any)
}

func (q *quota) enforce(activePath, statePath string) error {
	if q.max <= 0 && q.retention <= 0 {
		return nil
	}
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return fmt.Errorf("record: read dir %s: %w", q.dir, err)
	}
	type file struct {
		path string
		size int64
		mod  time.Time
	}
	var files []file
	var total int64
	stateName := filepath.Base(statePath)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || name == stateName || !strings.HasPrefix(name, q.prefix+"-") {
			continue
		}
		path := filepath.Join(q.dir, name)
		if activePath != "" && path == activePath {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, file{path: path, size: info.Size(), mod: info.ModTime()})
		total += info.Size()
	}
	if q.retention > 0 {
		cutoff := q.now().Add(-q.retention)
		kept := files[:0]
		for _, f := range files {
			if f.mod.Before(cutoff) {
				if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("record: retention remove %s: %w", f.path, err)
				}
				total -= f.size
				q.logf("record: retention: removed %s (%d bytes)", filepath.Base(f.path), f.size)
				continue
			}
			kept = append(kept, f)
		}
		files = kept
	}
	if q.max <= 0 {
		return nil
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].mod.Equal(files[j].mod) {
			return files[i].path < files[j].path
		}
		return files[i].mod.Before(files[j].mod)
	})
	for total > q.max && len(files) > 0 {
		f := files[0]
		files = files[1:]
		if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("record: quota remove %s: %w", f.path, err)
		}
		total -= f.size
		q.logf("record: quota: removed %s (%d bytes)", filepath.Base(f.path), f.size)
	}
	if total > q.max {
		return fmt.Errorf("record: quota %d bytes exceeded (%d bytes in %s)", q.max, total, q.dir)
	}
	return nil
}

func hashParams(hasVideo bool, video *stream.VideoCodec, hasAudio bool, audio *stream.AudioCodec) string {
	h := sha256.New()
	writeField(h, []byte(fmt.Sprintf("video=%v", hasVideo)))
	if hasVideo && video != nil {
		writeField(h, []byte(video.Codec))
		writeField(h, video.VPS)
		writeField(h, video.SPS)
		writeField(h, video.PPS)
	}
	writeField(h, []byte(fmt.Sprintf("audio=%v", hasAudio)))
	if hasAudio && audio != nil {
		writeField(h, []byte(stream.NormalizeAudioCodec(audio.Codec)))
		writeField(h, audio.Config)
		writeUint32(h, audio.SampleRate)
		h.Write([]byte{byte(audio.Channels)})
		writeUint32(h, audio.ClockRate)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeField(w io.Writer, p []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(p)))
	w.Write(n[:])
	w.Write(p)
}

func writeUint32(w io.Writer, v uint32) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], v)
	w.Write(n[:])
}

func sanitizePrefix(name string) string {
	var b strings.Builder
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
		if b.Len() >= 80 {
			break
		}
	}
	out := strings.Trim(b.String(), "._-")
	if out == "" {
		out = "stream"
	}
	return out
}

func mkdirAll(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("record: create output dir %s: %w", dir, err)
	}
	return nil
}
