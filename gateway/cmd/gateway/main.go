package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/TheWor1d877/KVFerry/internal/config"
	"github.com/TheWor1d877/KVFerry/internal/drill"
	"github.com/TheWor1d877/KVFerry/internal/health"
	"github.com/TheWor1d877/KVFerry/internal/probe"
	"github.com/TheWor1d877/KVFerry/internal/proxy"
	"github.com/TheWor1d877/KVFerry/internal/sticky"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	cfg, err := loadConfig(args)
	if err != nil {
		if errors.Is(err, errFlagParse) {
			return 2
		}
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	addr := cfg.Listen
	if addr == "" {
		addr = ":9000"
	}

	slog.Info("gateway started",
		"component", "gateway",
		"listen", addr,
		"upstreams", cfg.Upstreams,
	)

	table := sticky.NewTable()
	events := drill.NewBuffer(1024)
	probes := probe.NewManager(cfg, probe.Callbacks{
		OnRemove: func(node string) {
			table.DropNode(node)
			events.Record(drill.Event{TsMs: time.Now().UnixMilli(), Node: node, Action: drill.ActionRemove})
		},
		OnRecover: func(node string) {
			events.Record(drill.Event{TsMs: time.Now().UnixMilli(), Node: node, Action: drill.ActionRecover})
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go probes.Start(ctx)

	mux := http.NewServeMux()
	mux.Handle("/health", health.Handler())
	mux.Handle("/drill-events", drill.Handler(events))
	mux.Handle("/", proxy.New(cfg, table, probes, events))

	srv := &http.Server{Addr: addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("gateway stopped",
			"component", "gateway",
			"error", err.Error(),
		)
		return 1
	}
	return 0
}

var (
	// errFlagParse 标识 flag 解析失败；FlagSet 已把错误与 usage 输出到 stderr，run 据此返回 2。
	errFlagParse = errors.New("flag parse failed")
	// errMissingConfig 是缺 --config 的固定文案，run 输出后返回 1。
	errMissingConfig = errors.New("--config is required")
)

// loadConfig 解析命令行参数并加载 YAML 配置，是 run 在监听之前唯一执行的阶段（D16）。
func loadConfig(args []string) (config.Config, error) {
	configPath, overrides, err := parseArgs(args)
	if err != nil {
		return config.Config{}, err
	}
	return config.Load(configPath, overrides)
}

// parseArgs 解析 --config 与可重复的 --set；flag 错误交回调用方区分退出码（D16）。
func parseArgs(args []string) (string, []string, error) {
	var configPath string
	var overrides []string

	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&configPath, "config", "", "YAML 配置文件路径")
	fs.Func("set", "覆盖单项配置，形如 key=value，可重复", func(value string) error {
		overrides = append(overrides, value)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return "", nil, fmt.Errorf("%w: %w", errFlagParse, err)
	}
	if configPath == "" {
		return "", nil, errMissingConfig
	}
	return configPath, overrides, nil
}
