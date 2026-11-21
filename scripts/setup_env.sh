#!/usr/bin/env bash
# KVFerry 环境安装脚本
# 新建 Python 虚拟环境并安装 vLLM、PyTorch 及常用依赖。
# 用法：
#   bash scripts/setup_env.sh
#   bash scripts/setup_env.sh --python python3.12 --venv .venv
#   bash scripts/setup_env.sh --index-url https://pypi.tuna.tsinghua.edu.cn/simple
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3.12}"
VENV_DIR="${VENV_DIR:-$(pwd)/.venv}"
INDEX_URL="${INDEX_URL:-}"
VLLM_VERSION="${VLLM_VERSION:-0.34.0}"
TORCH_VERSION="${TORCH_VERSION:-2.14.0}"

usage() {
  sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --python) PYTHON_BIN="$2"; shift 2 ;;
    --venv) VENV_DIR="$2"; shift 2 ;;
    --index-url) INDEX_URL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "找不到 $PYTHON_BIN，请先安装 Python 3.11 及以上版本，或用 --python 指定解释器。" >&2
  exit 1
fi

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
  echo "$PYTHON_BIN 版本低于 3.11，vLLM $VLLM_VERSION 要求 3.11 及以上。" >&2
  exit 1
fi

echo "[1/4] 创建虚拟环境: $VENV_DIR (解释器: $("$PYTHON_BIN" -c 'import sys; print(sys.executable)'))"
if [ ! -x "$VENV_DIR/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

PIP="$VENV_DIR/bin/pip"
PIP_ARGS=()
if [ -n "$INDEX_URL" ]; then
  PIP_ARGS+=(--index-url "$INDEX_URL")
fi

echo "[2/4] 升级 pip"
"$PIP" install "${PIP_ARGS[@]}" --upgrade pip >/dev/null

REQ_FILE="$(cd "$(dirname "$0")" && pwd)/requirements.txt"

echo "[3/4] 安装依赖"
if [ -f "$REQ_FILE" ]; then
  "$PIP" install "${PIP_ARGS[@]}" -r "$REQ_FILE"
else
  "$PIP" install "${PIP_ARGS[@]}" "torch==$TORCH_VERSION" "vllm==$VLLM_VERSION" "numpy>=1.26"
fi

"$PIP" freeze > "$(cd "$(dirname "$0")" && pwd)/requirements.lock"

echo "[4/4] 验证"
"$VENV_DIR/bin/python" - <<'PY'
import torch
print(f"torch {torch.__version__}, cuda {torch.version.cuda}, 可用 {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"设备 {torch.cuda.get_device_name(0)}, 显存 {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f} GiB")
import vllm
print(f"vllm {vllm.__version__}")
PY

echo "完成。激活环境: source $VENV_DIR/bin/activate"