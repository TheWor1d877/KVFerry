// Package node 提供跨模块装配的存储节点面（D40、D43、D49）：段层、BlockStore 服务
// 与清单上报在进程内组装，gRPC 经 bufconn 暴露。
package node

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/server"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// Options 是存储节点装配参数（D40、D42）。
type Options struct {
	DataDir string
	NodeID  string
	// Spec 是节点口径四项（D44）；指针传递避免按值拷贝 protobuf 消息。
	Spec          *api.KVSpec
	CapacityBytes int64
	CacheBytes    int64
	ShardBlocks   int
	// Reporter 是清单上报目标客户端，nil 时 ReportManifest 报错（D41）。
	Reporter api.StorageReporterClient
}

// Node 是装配完成的存储节点（D40、D43）：BlockStore 客户端与清单上报入口。
type Node struct {
	Client      api.BlockStoreClient
	lis         *bufconn.Listener
	gsrv        *grpc.Server
	store       *segment.Store
	conn        *grpc.ClientConn
	spec        spec.KVCacheSpec
	dataDir     string
	nodeID      string
	shardBlocks int
	reporter    api.StorageReporterClient
}

// Start 装配并启动存储节点组件（D40、D43）。
func Start(opts Options) (*Node, error) {
	kvSpec := spec.KVCacheSpec{
		BlockBytes:    opts.Spec.GetBlockBytes(),
		KVPrecision:   opts.Spec.GetKvPrecision(),
		BytesPerToken: opts.Spec.GetBytesPerToken(),
		AlignmentPad:  opts.Spec.GetAlignmentPad(),
	}
	if err := kvSpec.Validate(); err != nil {
		return nil, err
	}
	store, err := segment.Open(opts.DataDir, opts.NodeID, kvSpec, opts.CapacityBytes, segment.Options{Log: telemetry.New()})
	if err != nil {
		return nil, err
	}
	lis := bufconn.Listen(1 << 20)
	gsrv := grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxRecvMsgSize(10)))
	api.RegisterBlockStoreServer(gsrv, server.New(server.Deps{
		NodeID:          opts.NodeID,
		Store:           store,
		Cache:           cache.New(opts.CacheBytes),
		NodeSpec:        kvSpec,
		MaxMessageBytes: 10 << 20,
		Log:             telemetry.New(),
	}))
	go func() { _ = gsrv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		gsrv.Stop()
		store.Close()
		return nil, fmt.Errorf("dial node bufconn: %w", err)
	}
	return &Node{
		Client:      api.NewBlockStoreClient(conn),
		lis:         lis,
		gsrv:        gsrv,
		store:       store,
		conn:        conn,
		spec:        kvSpec,
		dataDir:     opts.DataDir,
		nodeID:      opts.NodeID,
		shardBlocks: opts.ShardBlocks,
		reporter:    opts.Reporter,
	}, nil
}

// DataDir 返回节点数据目录（D49）：快照与数据大文件所在目录。
func (n *Node) DataDir() string {
	return n.dataDir
}

// SegmentSize 返回段长（D43）。
func (n *Node) SegmentSize() int64 {
	return n.store.SegmentSize()
}

// SegmentOffset 返回段序号对应的数据偏移（D43、D48）。
func (n *Node) SegmentOffset(seq int64) int64 {
	return segment.FileHeaderSize + seq*n.SegmentSize()
}

// HighWater 返回位图高水位序列号（D49）。
func (n *Node) HighWater() int64 {
	return n.store.HighWater()
}

// ReportManifest 扫描段头生成清单条目并分片上报决策侧（D48、D49）。
func (n *Node) ReportManifest(ctx context.Context) error {
	if n.reporter == nil {
		return errors.New("manifest reporter not configured")
	}
	_, hashes, err := manifest.ScanSegmentHeads(n.dataDir, n.store.SegmentSize(), n.store.TotalSegments())
	if err != nil {
		return fmt.Errorf("scan segment heads: %w", err)
	}
	entries := manifest.BuildEntries(hashes, n.store.SegmentSize(), n.spec.BlockBytes)
	return manifest.NewReporter(n.nodeID, n.shardBlocks, n.reporter).Report(ctx, entries)
}

// Snapshot 写一次段清单快照（D49）：占用扫段头、高水位取位图。
func (n *Node) Snapshot() error {
	occupied, _, err := manifest.ScanSegmentHeads(n.dataDir, n.store.SegmentSize(), n.store.TotalSegments())
	if err != nil {
		return fmt.Errorf("scan segment heads: %w", err)
	}
	return manifest.Write(n.dataDir, occupied, n.store.HighWater())
}

// Close 停机并关闭数据文件。
func (n *Node) Close() error {
	n.conn.Close()
	n.gsrv.Stop()
	return n.store.Close()
}
