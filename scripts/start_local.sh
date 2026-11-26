#!/usr/bin/env bash
# KVFerry 单机启动脚本：拉起决策服务与存储节点两个进程（D55）。
# 用法：
#   bash scripts/start_local.sh init --dir <目录> [--data-dir <数据目录>]
#       生成最小示例配置 config.yaml 与引擎口径文件 spec.json
#   bash scripts/start_local.sh start --config <yaml> --spec-file <json>
#       [--decision-listen :9200] [--nodes node-1] [--block-bytes 1048576]
#       构建二进制并前台拉起两进程，Ctrl-C 一并退出
#   bash scripts/start_local.sh smoke
#       临时目录与随机端口拉起两进程，完成一次 PutBlock 与 GetBlock 验证后清理
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

usage() {
  sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
}

die() {
  echo "[start_local] $*" >&2
  exit 1
}

# pick_free_port 返回一个暂未被占用的本地端口；拉起前占用可能被抢，仅用于冒烟。
pick_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# wait_port 等待 TCP 端口可连，超时 15 秒。
wait_port() {
  local port="$1" name="$2" i
  for i in $(seq 1 75); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 3<&- || true
      echo "[start_local] $name 就绪：127.0.0.1:$port"
      return 0
    fi
    sleep 0.2
  done
  die "$name 端口 127.0.0.1:$port 等待超时"
}

# build_bins 构建 storage 与 decision 两个二进制到指定目录。
build_bins() {
  local out="$1"
  echo "[start_local] 构建二进制到 $out"
  (cd "$REPO_ROOT" && go build -o "$out/kvferry-decision" ./decision/cmd/kvferry-decision)
  (cd "$REPO_ROOT" && go build -o "$out/kvferry-storage" ./storage/cmd/kvferry-storage)
}

cmd_init() {
  local dir="" data_dir=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --dir) dir="$2"; shift 2 ;;
      --data-dir) data_dir="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "init 未知参数: $1" ;;
    esac
  done
  [ -n "$dir" ] || die "init 需要 --dir <目录>"
  data_dir="${data_dir:-$dir/data}"
  mkdir -p "$dir" "$data_dir"
  cat > "$dir/spec.json" <<'JSON'
{
  "block_bytes": 1048576,
  "kv_precision": "fp16",
  "bytes_per_token": 16384,
  "alignment_pad": 0
}
JSON
  cat > "$dir/config.yaml" <<EOF
# 存储节点最小示例配置（D42）：五项必填 + 常用默认。
node_id: node-1
decision_endpoint: 127.0.0.1:9200
data_dir: $data_dir
capacity_bytes: 10737418240
engine_endpoint: 127.0.0.1:0
listen: :9100
heartbeat_interval_s: 30
read_cache_bytes: 1073741824
nvme_watermark_pct: 90
EOF
  echo "[start_local] 示例配置已生成：$dir/config.yaml 与 $dir/spec.json"
  echo "[start_local] 拉起：bash scripts/start_local.sh start --config $dir/config.yaml --spec-file $dir/spec.json"
}

cmd_start() {
  local config="" spec_file="" decision_listen=":9200" nodes="node-1"
  while [ $# -gt 0 ]; do
    case "$1" in
      --config) config="$2"; shift 2 ;;
      --spec-file) spec_file="$2"; shift 2 ;;
      --decision-listen) decision_listen="$2"; shift 2 ;;
      --nodes) nodes="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "start 未知参数: $1" ;;
    esac
  done
  [ -n "$config" ] || die "start 需要 --config <yaml>"
  [ -n "$spec_file" ] || die "start 需要 --spec-file <json>"

  local tmp
  tmp="$(mktemp -d /tmp/kvferry-local.XXXXXX)"
  build_bins "$tmp"

  echo "[start_local] 拉起决策服务：$decision_listen"
  "$tmp/kvferry-decision" -listen "$decision_listen" -nodes "$nodes" &
  dec_pid=$!

  echo "[start_local] 拉起存储节点（配置：$config）"
  "$tmp/kvferry-storage" --config "$config" --spec-file "$spec_file" &
  sto_pid=$!

  trap 'kill "$dec_pid" "$sto_pid" 2>/dev/null || true; wait "$dec_pid" "$sto_pid" 2>/dev/null || true' EXIT INT TERM
  echo "[start_local] 两进程运行中，Ctrl-C 一并退出"
  wait "$sto_pid"
}

cmd_smoke() {
  # tmp、dec_pid、sto_pid 不用 local：EXIT trap 在函数返回后才触发，
  # 局部变量届时已销毁，清理函数拿不到 pid 与目录。
  tmp="$(mktemp -d /tmp/kvferry-smoke.XXXXXX)"
  dec_port="$(pick_free_port)"
  sto_port="$(pick_free_port)"

  cleanup() {
    for pid in "${dec_pid:-}" "${sto_pid:-}"; do
      [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait "${dec_pid:-}" "${sto_pid:-}" 2>/dev/null || true
    rm -rf "${tmp:-}"
  }
  trap cleanup EXIT

  local data_dir="$tmp/data"
  mkdir -p "$data_dir"
  cat > "$tmp/spec.json" <<'JSON'
{
  "block_bytes": 4096,
  "kv_precision": "fp16",
  "bytes_per_token": 64,
  "alignment_pad": 0
}
JSON
  cat > "$tmp/config.yaml" <<EOF
node_id: node-1
decision_endpoint: 127.0.0.1:$dec_port
data_dir: $data_dir
capacity_bytes: 10485760
engine_endpoint: 127.0.0.1:0
listen: 127.0.0.1:$sto_port
read_cache_bytes: 0
heartbeat_interval_s: 1
EOF

  build_bins "$tmp"

  echo "[start_local] 冒烟：决策服务 127.0.0.1:$dec_port，存储节点 127.0.0.1:$sto_port"
  "$tmp/kvferry-decision" -listen "127.0.0.1:$dec_port" -nodes node-1 > "$tmp/decision.log" 2>&1 &
  dec_pid=$!
  wait_port "$dec_port" "决策服务"

  "$tmp/kvferry-storage" --config "$tmp/config.yaml" --spec-file "$tmp/spec.json" > "$tmp/storage.log" 2>&1 &
  sto_pid=$!
  wait_port "$sto_port" "存储节点"

  # 等节点完成启动清单上报与事件流建立，再发起写读验证。
  sleep 2

  cat > "$tmp/smoke_client.go" <<'GO'
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/TheWor1d877/KVFerry/storage/api"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9100", "存储节点地址")
	flag.Parse()
	data := make([]byte, 4096)
	if _, err := crand.Read(data); err != nil {
		fmt.Fprintln(os.Stderr, "random bytes:", err)
		os.Exit(1)
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := api.NewBlockStoreClient(conn)
	kvSpec := &api.KVSpec{BlockBytes: 4096, KvPrecision: "fp16", BytesPerToken: 64}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.PutBlock(ctx, &api.PutBlockRequest{
		NodeId:    "node-1",
		BlockHash: "smoke-hash-0001",
		Spec:      kvSpec,
		BlockData: data,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "putblock:", err)
		os.Exit(1)
	}
	resp, err := client.GetBlock(ctx, &api.GetBlockRequest{
		BlockHash: "smoke-hash-0001",
		Spec:      kvSpec,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "getblock:", err)
		os.Exit(1)
	}
	if !bytes.Equal(resp.GetBlockData(), data) {
		fmt.Fprintln(os.Stderr, "getblock: readback bytes differ from written")
		os.Exit(1)
	}
	fmt.Println("putblock/getblock verified")
}
GO

  (cd "$REPO_ROOT/storage" && go run "$tmp/smoke_client.go" -addr "127.0.0.1:$sto_port")

  echo "[start_local] 冒烟通过：两进程就绪并完成一次 PutBlock 与 GetBlock"
}

case "${1:-}" in
  init) shift; cmd_init "$@" ;;
  start) shift; cmd_start "$@" ;;
  smoke) shift; cmd_smoke "$@" ;;
  -h|--help|"" ) usage ;;
  *) die "未知子命令: $1（可用：init、start、smoke）" ;;
esac