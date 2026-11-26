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
	"github.com/TheWor1d877/KVFerry/storage/internal/audit"
	"github.com/TheWor1d877/KVFerry/storage/internal/cache"
	"github.com/TheWor1d877/KVFerry/storage/internal/config"
	"github.com/TheWor1d877/KVFerry/storage/internal/event"
	"github.com/TheWor1d877/KVFerry/storage/internal/manifest"
	"github.com/TheWor1d877/KVFerry/storage/internal/pin"
	"github.com/TheWor1d877/KVFerry/storage/internal/remote"
	"github.com/TheWor1d877/KVFerry/storage/internal/segment"
	"github.com/TheWor1d877/KVFerry/storage/internal/server"
	"github.com/TheWor1d877/KVFerry/storage/internal/spec"
	"github.com/TheWor1d877/KVFerry/storage/internal/startup"
	"github.com/TheWor1d877/KVFerry/storage/internal/telemetry"
)

// reasonSinkDemoted 是下沉成功后本机段回收的原因取值（D50）。
const reasonSinkDemoted = "sink_demoted"

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

	// pin 表构造与清单恢复（R34）：pin 清单恢复先于段头扫描，pin 的段在重建完成后
	// 直接进入不可驱逐集合；配额回收解除标记后释放对应段（R34）。
	var store *segment.Store
	pins := pin.New(pin.Options{
		DataDir:    cfg.DataDir,
		BlockBytes: nodeSpec.BlockBytes,
		QuotaBytes: cfg.PinQuotaBytes,
		DefaultTTL: time.Duration(cfg.PinTTLS) * time.Second,
		Log:        log,
		OnReclaim: func(blockHash string) {
			store.DropBlock(blockHash, segment.ReasonPinQuota)
		},
	})
	if err := pins.Load(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// 决策连接懒建立，决策不可达不阻塞启动口径编排与节点服务（D51）。
	decisionConn, err := grpc.NewClient(cfg.DecisionEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("dial decision endpoint %s: %w", cfg.DecisionEndpoint, err))
		return 1
	}
	defer decisionConn.Close()
	reporterClient := api.NewStorageReporterClient(decisionConn)
	reporter := manifest.NewReporter(cfg.NodeID, cfg.ManifestShardBlocks, reporterClient)

	// 启动口径校验与清空重建编排（D55）：口径不一致时清空数据大文件按新口径重建段区与位图；
	// 校验结论经 startup.Open 内的四处打点记录，重建后的启动清单上报即全量重报（D55）。
	store, _, err = startup.Open(cfg.DataDir, cfg.NodeID, nodeSpec, cfg.CapacityBytes, startup.Options{
		Emit:         emit,
		Log:          log,
		Pins:         pins,
		WatermarkPct: cfg.NVMEWatermarkPct,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer store.Close()

	// 清单条目与容量水位从段头与段总数取，供清单上报与心跳共用（D48）。
	scanEntries := func() ([]manifest.Entry, float64, error) {
		occupied, hashes, err := manifest.ScanSegmentHeads(cfg.DataDir, store.SegmentSize(), store.TotalSegments())
		if err != nil {
			return nil, 0, err
		}
		return manifest.BuildEntries(hashes, store.SegmentSize(), nodeSpec.BlockBytes),
			float64(len(occupied)) / float64(store.TotalSegments()), nil
	}
	rereport := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = startup.RereportRebuilt(ctx, cfg.DataDir, store, nodeSpec, cfg.NodeID, startup.RereporterFunc(reporter.Report), log)
	}

	// 启动即逐片上报一次全量清单；口径重建后的这次上报就是重建清单（D55）。
	go rereport()

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

	// 周期对账与数据抽读（D54）：audit_max_mbps 兆比特每秒换算字节每秒限速预算。
	aud := audit.New(audit.Options{
		NodeID:       cfg.NodeID,
		Store:        store,
		Emit:         emit,
		Log:          log,
		IntervalS:    cfg.AuditIntervalS,
		SamplePct:    cfg.AuditSamplePct,
		MaxBytesPerS: int64(cfg.AuditMaxMbps) * 1000 * 1000 / 8,
	})
	auditCtx, cancelAudit := context.WithCancel(context.Background())
	defer cancelAudit()
	go aud.Run(auditCtx)

	// 远端块服务客户端（D50、D51）：remote_nodes 为空时不启用，行为与单机一致（D42）。
	var remoteClient *remote.Client
	if len(cfg.RemoteNodes) > 0 {
		remoteClient = remote.New(remote.Options{
			NodeID:   cfg.NodeID,
			Nodes:    cfg.RemoteNodes,
			NodeSpec: nodeSpec,
			Emit:     emit,
			LocalPut: func(blockHash string, data []byte) error {
				_, _, err := store.Put(blockHash, data)
				return err
			},
			Log: log,
		})
		defer remoteClient.Close()
	}

	// 下沉触发来源（D50）：保留决策未实现前由 pin 变更触发——决策经心跳响应下发 pin，
	// 节点把本机持有且被 pin 的块按环推远端，下沉成功后本机段按普通驱逐流程回收。
	// 真实保留决策触发属决策服务实现，落地后替换本编排。
	sinkOnPin := func(change *api.PinChange) {
		if remoteClient == nil || !change.GetPinned() {
			return
		}
		hash := change.GetBlockHash()
		data, _, err := store.Get(hash)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		outcome, err := remoteClient.Sink(ctx, hash, data)
		if err != nil {
			return
		}
		if outcome == remote.SinkRemote {
			store.DropBlock(hash, reasonSinkDemoted)
		}
	}

	// 事件流客户端（D41、D47）：断线续传与重连补传，决策不可达时循环重试不阻塞服务（D51）。
	stream := event.NewStream(cfg.NodeID, buf, reporterClient, log, event.StreamOptions{})
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	go stream.Run(streamCtx)

	// 心跳循环（D41、D48）：响应 pin 变更写进 pin 表并落盘（R25、R34），
	// 全量重报请求触发重报且重报不动事件 seq（D47、D49）；
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
			var err error
			if change.GetPinned() {
				err = pins.Add(change.GetBlockHash(), time.Duration(change.GetPinTtlS())*time.Second)
			} else {
				err = pins.Remove(change.GetBlockHash())
			}
			if err != nil {
				log.Warn("pin change apply failed", telemetry.Fields{
					NodeID:    cfg.NodeID,
					BlockHash: change.GetBlockHash(),
					Result:    "pin_apply_failed",
					Reason:    err.Error(),
				})
			}
			sinkOnPin(change)
		}),
		OnFullRereport: func() {
			rereport()
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
		Pins:            pins,
		Remote:          remoteClient,
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
