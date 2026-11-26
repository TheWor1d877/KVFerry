package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/TheWor1d877/KVFerry/decision/internal/health"
	"github.com/TheWor1d877/KVFerry/decision/internal/index"
	"github.com/TheWor1d877/KVFerry/decision/internal/server"
	"github.com/TheWor1d877/KVFerry/decision/internal/telemetry"
	"github.com/TheWor1d877/KVFerry/storage/api"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("kvferry-decision", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":9200", "gRPC 监听地址")
	nodes := fs.String("nodes", "", "存储节点编号名单，逗号分隔，作为重建窗口的登记节点集合（D49）")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	log := telemetry.New()
	ix := index.New()

	// 事件应用器（D47）：事件按节点按 seq 单调推进，跳号置 resync 等该节点全量重报。
	applier := index.NewApplier(ix, log)

	// 决策进程重启后登记全部作废，开重建窗口等各节点逐片重报，窗口关闭前命中一律未命中
	// （D49、R32）。名单即登记节点集合；名单外节点经心跳 resync 分支 Enqueue 纳入接收集合（D47）。
	rb := index.StartRebuild(ix, parseNodes(*nodes), log)

	// 失联四态状态机（D51）：判死时长与滞回周期取部署缺省 30 秒（D42 的 down_after_s
	// 与 heartbeat_interval_s 缺省值），判定来源是 StorageReporter 服务端对流断开的观测。
	tracker := health.NewTracker(30*time.Second, 30*time.Second, log)

	rep := server.NewReporter(rb,
		server.WithIndex(ix),
		server.WithApplier(applier),
		server.WithHealth(tracker),
		server.WithLog(log),
	)

	gsrv := grpc.NewServer()
	api.RegisterStorageReporterServer(gsrv, rep)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("listen %s: %w", *listen, err))
		return 1
	}
	log.Info("decision serving", telemetry.Fields{
		NodeID: "decision",
		Result: *listen,
	})
	if err := gsrv.Serve(lis); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// parseNodes 把逗号分隔的节点名单拆成集合入参，空白段忽略。
func parseNodes(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, n := range strings.Split(s, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	return out
}
