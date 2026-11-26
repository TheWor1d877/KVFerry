package telemetry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Fields 是结构化日志字段，事件公共字段（node_id、block_hash、segment_seq、layer、reason、seq）命名与 D47 一致；块字节与内容不进日志，只记块哈希。
// TraceID 关联同一次链路，请求级打点必带，过程级打点可空。Retry 记事件流重试次数（D47）。
type Fields struct {
	TraceID      string
	NodeID       string
	BlockHash    string
	SegmentSeq   int64
	DurationMs   int64
	Result       string
	Reason       string
	Layer        string
	Seq          int64
	Bytes        int64
	WatermarkPct int
	Retry        int64
	ShardIndex   int64
}

// NewTraceID 从链路要素生成请求级 trace_id：要素含块哈希与时间戳，经短哈希得到，
// 同一次调用内共用一个标识，不同调用互不相同。
func NewTraceID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(h[:8])
}

// Hook 是可注入的日志钩子，line 为格式化后的 JSON 行，测试用钩子捕获断言。
type Hook func(level, msg string, f Fields, line []byte)

// Logger 输出 stdout JSON 行，每条带 component="storage"。
type Logger struct {
	hook Hook
	log  *slog.Logger
}

// New 建立默认日志器，写 stdout JSON 行。
func New() *Logger {
	return &Logger{log: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
}

// NewWithHook 用注入钩子建立日志器，钩子与默认输出并行执行。
func NewWithHook(h Hook) *Logger {
	return &Logger{
		hook: h,
		log:  slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
}

// Info 打一条 info 日志。
func (l *Logger) Info(msg string, f Fields) {
	l.emit("info", msg, f)
}

// Warn 打一条 warn 日志。
func (l *Logger) Warn(msg string, f Fields) {
	l.emit("warn", msg, f)
}

// Error 打一条 error 日志。
func (l *Logger) Error(msg string, f Fields) {
	l.emit("error", msg, f)
}

func (l *Logger) attrs(f Fields) []slog.Attr {
	attrs := []slog.Attr{slog.String("component", "storage")}
	if f.TraceID != "" {
		attrs = append(attrs, slog.String("trace_id", f.TraceID))
	}
	if f.NodeID != "" {
		attrs = append(attrs, slog.String("node_id", f.NodeID))
	}
	if f.BlockHash != "" {
		attrs = append(attrs, slog.String("block_hash", f.BlockHash))
	}
	if f.SegmentSeq != 0 {
		attrs = append(attrs, slog.Int64("segment_seq", f.SegmentSeq))
	}
	if f.DurationMs != 0 {
		attrs = append(attrs, slog.Int64("duration_ms", f.DurationMs))
	}
	if f.Result != "" {
		attrs = append(attrs, slog.String("result", f.Result))
	}
	if f.Reason != "" {
		attrs = append(attrs, slog.String("reason", f.Reason))
	}
	if f.Layer != "" {
		attrs = append(attrs, slog.String("layer", f.Layer))
	}
	if f.Seq != 0 {
		attrs = append(attrs, slog.Int64("seq", f.Seq))
	}
	if f.Bytes != 0 {
		attrs = append(attrs, slog.Int64("bytes", f.Bytes))
	}
	if f.WatermarkPct != 0 {
		attrs = append(attrs, slog.Int("watermark_pct", f.WatermarkPct))
	}
	if f.Retry > 0 {
		attrs = append(attrs, slog.Int64("retry", f.Retry))
	}
	if f.ShardIndex != 0 {
		attrs = append(attrs, slog.Int64("shard_index", f.ShardIndex))
	}
	return attrs
}

func (l *Logger) emit(level, msg string, f Fields) {
	attrs := l.attrs(f)
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	switch level {
	case "warn":
		l.log.Warn(msg, args...)
	case "error":
		l.log.Error(msg, args...)
	default:
		l.log.Info(msg, args...)
	}
	if l.hook != nil {
		var buf bytes.Buffer
		h := slog.NewJSONHandler(&buf, nil)
		r := slog.NewRecord(time.Now(), slog.LevelInfo, msg, 0)
		r.AddAttrs(attrs...)
		_ = h.Handle(context.Background(), r)
		l.hook(level, msg, f, buf.Bytes())
	}
}
