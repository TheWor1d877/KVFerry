package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TheWor1d877/KVFerry/storage/api"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/config"
	"github.com/TheWor1d877/KVFerry/storage/internal/event"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/server"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("kvferry-storage", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var configPath, specPath string
	fs.StringVar(&configPath, "config", "", "YAML 配置文件路径")
	fs.StringVar(&specPath, "spec-file", "", "引擎 KVCacheSpec JSON 文件路径")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if configPath == "" {
		fmt.Fprintln(os.Stderr, "--config is required")
		return 1
	}
	if specPath == "" {
		fmt.Fprintln(os.Stderr, "--spec-file is required")
		return 1
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	log := telemetry.New()

	// 事件环形缓冲（D47）：五类事件共用一套节点内 seq，emit 回调接入缓冲，
	// seq 高水位随段清单快照同频落盘。
	buf := event.NewBuffer(cfg.NodeID, event.DefaultCapacity, cfg.DataDir, log)
	defer buf.Close()
	emit := segment.EmitFunc(buf.Consume)

	// KVCacheSpec 读取失败时进程不得进入服务状态，不启动 gRPC（D44）。
	nodeSpec, err := readEngineSpec(specPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	store, err := segment.Open(cfg.DataDir, cfg.NodeID, nodeSpec, cfg.CapacityBytes, segment.Options{
		Emit: emit,
		Log:  log,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer store.Close()

	log.Info("startup spec verified", telemetry.Fields{
		NodeID: cfg.NodeID,
		Result: "spec_match",
		Reason: fmt.Sprintf("file header and engine spec consistent, segments=%d segment_size=%d", store.TotalSegments(), store.SegmentSize()),
	})

	// 段清单快照按心跳同频周期落盘（D49）；数据源扫段头取占用、从位图取高水位。
	snapshotSrc := manifest.SourceFunc(func() ([]int64, int64, error) {
		occupied, _, err := manifest.ScanSegmentHeads(cfg.DataDir, store.SegmentSize(), store.TotalSegments())
		if err != nil {
			return nil, 0, err
		}
		return occupied, store.HighWater(), nil
	})
	snapshotter := manifest.NewSnapshotter(cfg.DataDir, time.Duration(cfg.HeartbeatIntervalS)*time.Second, snapshotSrc, log)
	snapshotCtx, cancelSnapshot := context.WithCancel(context.Background())
	defer cancelSnapshot()
	go snapshotter.Run(snapshotCtx)

	// 事件 seq 高水位与段清单快照同频落盘（D47）。
	watermarkCtx, cancelWatermark := context.WithCancel(context.Background())
	defer cancelWatermark()
	go buf.Run(watermarkCtx, time.Duration(cfg.HeartbeatIntervalS)*time.Second)

	// 决策连接懒建立，决策不可达不阻塞节点服务（D51）。
	decisionConn, err := grpc.NewClient(cfg.DecisionEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("dial decision endpoint %s: %w", cfg.DecisionEndpoint, err))
		return 1
	}
	defer decisionConn.Close()
	reporterClient := api.NewStorageReporterClient(decisionConn)
	reporter := manifest.NewReporter(cfg.NodeID, cfg.ManifestShardBlocks, reporterClient)

	// 清单条目与容量水位从段头与段总数取，供事件流与心跳共用（D48）。
	scanEntries := func() ([]manifest.Entry, float64, error) {
		occupied, hashes, err := manifest.ScanSegmentHeads(cfg.DataDir, store.SegmentSize(), store.TotalSegments())
		if err != nil {
			return nil, 0, err
		}
		return manifest.BuildEntries(hashes, store.SegmentSize(), nodeSpec.BlockBytes),
			float64(len(occupied)) / float64(store.TotalSegments()), nil
	}
	reportAll := func(reason string) {
		entries, _, err := scanEntries()
		if err != nil {
			log.Warn("manifest scan failed", telemetry.Fields{NodeID: cfg.NodeID, Reason: err.Error(), Result: "scan_failed"})
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := reporter.Report(ctx, entries); err != nil {
			log.Warn("manifest report failed, keep serving", telemetry.Fields{NodeID: cfg.NodeID, Reason: err.Error(), Result: "report_failed"})
			return
		}
		_ = reason
	}

	go func() {
		reportAll("startup")
	}()

	// 事件流客户端（D41、D47）：断线续传与重连补传，决策不可达时循环重试不阻塞服务（D51）。
	stream := event.NewStream(cfg.NodeID, buf, reporterClient, log, event.StreamOptions{})
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	go stream.Run(streamCtx)

	// 心跳循环（D41、D48）：响应 pin 变更消费与全量重报触发，重报不动事件 seq（D47、D49）；
	// 事件流首次补传完成前跳过清单上报，保证重连后先补传事件再上报清单（D47）。
	hb := server.NewHeartbeat(server.HeartbeatDeps{
		NodeID:      cfg.NodeID,
		Interval:    time.Duration(cfg.HeartbeatIntervalS) * time.Second,
		Client:      reporterClient,
		ShardBlocks: cfg.ManifestShardBlocks,
		Synced:      stream.Synced,
		Source: func() ([]manifest.Entry, float64, error) {
			return scanEntries()
		},
		Pins: server.PinConsumerFunc(func(change *api.PinChange) {
			log.Info("pin change received", telemetry.Fields{
				NodeID:    cfg.NodeID,
				BlockHash: change.GetBlockHash(),
				Result:    "pin_received",
			})
		}),
		OnFullRereport: func() {
			reportAll("full_rereport")
			buf.ClearResync()
		},
		Log: log,
	})
	hbCtx, cancelHeartbeat := context.WithCancel(context.Background())
	defer cancelHeartbeat()
	go hb.Run(hbCtx)

	lru := cache.New(cfg.ReadCacheBytes)

	var ready atomic.Bool
	ready.Store(true)

	gsrv := grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxRecvMsgSize(int64(cfg.RPCMaxMessageMB))))
	api.RegisterBlockStoreServer(gsrv, server.New(server.Deps{
		NodeID:          cfg.NodeID,
		Store:           store,
		Cache:           lru,
		NodeSpec:        nodeSpec,
		MaxMessageBytes: int64(cfg.RPCMaxMessageMB) << 20,
		Ready:           ready.Load,
		Emit:            emit,
		Log:             log,
	}))

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("listen %s: %w", cfg.Listen, err))
		return 1
	}
	log.Info("storage node serving", telemetry.Fields{
		NodeID: cfg.NodeID,
		Result: cfg.Listen,
	})
	if err := gsrv.Serve(lis); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func readEngineSpec(path string) (spec.KVCacheSpec, error) {
	return spec.FileReader{Path: path}.ReadKVCacheSpec()
}
