#!/usr/bin/env bash
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PY="${PYTHON:-.venv/bin/python}"
if [ ! -x "$PY" ]; then
  PY="python3"
fi

run() {
  echo "=== $1 ==="
  shift
  if ! "$@"; then
    echo "跳过：$* 执行失败" >&2
  fi
}

run "硬件采集" "$PY" survey/hardware/collect.py
run "拷贝重叠" "$PY" survey/overlap/bench_overlap.py --size-mib 256 --iters 20
run "代价表" "$PY" survey/cost/bench_cost.py --kv-gib 1 --block-mib 16 --iters 20
run "代价表汇总" "$PY" survey/cost/report.py --input survey/results/cost

echo "完成。结果在 survey/results/"