package event

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// StreamOptions 是事件流客户端可注入参数（D41、D47）：断线重试间隔与发送轮询间隔，
// 测试注入短值替代真实等待。
type StreamOptions struct {
	Backoff time.Duration
	Tick    time.Duration
}

const (
	defaultBackoff = time.Second
	defaultTick    = 100 * time.Millisecond
)

// Stream 是 StreamEvents 客户端（D41、D47）：事件按 seq 顺序出网，决策回确认序号推进
// 续传位点，断线后从决策确认的 seq 之后续传；连接断开时把 offline 事件写进缓冲。
// 传输连接经 api.StorageReporterClient 注入，测试用 bufconn。
type Stream struct {
	nodeID  string
	buf     *Buffer
	client  api.StorageReporterClient
	log     *telemetry.Logger
	backoff time.Duration
	tick    time.Duration
	synced  atomic.Bool
}

// NewStream 建立事件流客户端（D41、D47）。
func NewStream(nodeID string, buf *Buffer, client api.StorageReporterClient, log *telemetry.Logger, opts StreamOptions) *Stream {
	if log == nil {
		log = telemetry.New()
	}
	if opts.Backoff <= 0 {
		opts.Backoff = defaultBackoff
	}
	if opts.Tick <= 0 {
		opts.Tick = defaultTick
	}
	return &Stream{
		nodeID:  nodeID,
		buf:     buf,
		client:  client,
		log:     log,
		backoff: opts.Backoff,
		tick:    opts.Tick,
	}
}

// Run 驱动事件流（D47）：断线后写 offline 事件、记日志并按 backoff 重连，重连先补传事件；
// ctx 取消即退出，退出不写 offline。
func (s *Stream) Run(ctx context.Context) {
	retry := int64(0)
	for {
		err := s.connectOnce(ctx, retry)
		if ctx.Err() != nil {
			return
		}
		retry++
		reason := "unknown"
		if err != nil {
			reason = err.Error()
		}
		s.buf.Offline(reason)
		s.log.Warn("event stream disconnected", telemetry.Fields{
			NodeID: s.nodeID,
			Seq:    s.buf.Acked(),
			Retry:  retry,
			Reason: reason,
			Result: "disconnected",
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.backoff):
		}
	}
}

// connectOnce 走一次连接生命周期（D41）：建立流后重置发送位点从确认位点补传，
// 发送与接收各占一个 goroutine，任一侧失败即断开返回。
func (s *Stream) connectOnce(ctx context.Context, retry int64) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.StreamEvents(streamCtx)
	if err != nil {
		return fmt.Errorf("open event stream: %w", err)
	}
	s.buf.ResetSent()
	// 首次补传未完成前心跳不报清单，保证重连后先补传事件再上报清单（D47）。
	s.synced.Store(false)
	s.log.Info("event stream connected", telemetry.Fields{
		NodeID: s.nodeID,
		Seq:    s.buf.Acked(),
		Retry:  retry,
		Result: "connected",
	})
	errCh := make(chan error, 2)
	go s.sendLoop(stream, streamCtx, errCh)
	go s.recvLoop(stream, errCh)
	select {
	case <-ctx.Done():
		_ = stream.CloseSend()
		return ctx.Err()
	case e := <-errCh:
		if e == nil {
			e = fmt.Errorf("event stream closed")
		}
		return e
	}
}

func (s *Stream) sendLoop(stream api.StorageReporter_StreamEventsClient, ctx context.Context, errCh chan<- error) {
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, resync := s.buf.Take()
			for _, ev := range pending {
				// Take 返回的指针对象由发送方独占，resync 标志按当前缓冲状态写入（D47）。
				ev.Resync = resync
				if err := stream.Send(ev); err != nil {
					errCh <- err
					return
				}
			}
			if first {
				// 当前连接首轮发送完成：确认位点之后的存量事件已全部出网（D47）。
				first = false
				s.synced.Store(true)
			}
		}
	}
}

// Synced 报告当前连接是否已完成首次补传（D47）：连接建立后为 false，首轮发送完成后为
// true，断线重连重新置 false；心跳在上报清单前读取，保证重连后先补传事件再上报清单。
func (s *Stream) Synced() bool { return s.synced.Load() }

func (s *Stream) recvLoop(stream api.StorageReporter_StreamEventsClient, errCh chan<- error) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		s.buf.Confirm(ack.GetLastConfirmedSeq())
	}
}
