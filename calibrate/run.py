"""标定命令入口：调度标定链路、落盘与退出码（D31）。"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import config
import fetchbench
import interbench
import recompbench
import tables
import writebench

# D38：rpc_s 的探测请求为对取回组件端点的一次取回查询请求（单块查询体），
# HTTP POST、请求体含单块占位查询字段，发 100 次取中位数；取回组件落地后
# 改测真实块读请求（D38）。路径与查询体写为模块常量，由 main 装配替换
# interbench 的探测函数。
_RPC_PROBE_PATH = "/fetch/probe"
_RPC_PROBE_BLOCK_MIB = 64

_VLLM_COMPLETIONS_PATH = "/v1/completions"  # vLLM OpenAI 兼容 completions 端点
_VLLM_SLEEP_PATH = "/sleep"  # vLLM dev 管理端点：睡眠并丢弃 KV cache
_VLLM_WAKE_UP_PATH = "/wake_up"  # vLLM dev 管理端点：恢复推理服务
_HTTP_TIMEOUT_S = 600
_RPC_PROBE_TIMEOUT_S = 2
_PREFILL_TOKEN_ID_LOW = 1000  # 随机 token id 区间下限，避开特殊 token 区段
_PREFILL_TOKEN_ID_HIGH = 30000  # 区间上限取常见词表的安全范围

_PRECISION_ELEMENT_BYTES = {
    "fp16": 2,
    "float16": 2,
    "half": 2,
    "bf16": 2,
    "bfloat16": 2,
    "fp8": 1,
    "float8": 1,
    "fp8_e4m3": 1,
    "fp8_e4m3fn": 1,
    "fp8_e5m2": 1,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="run.py", description="代价表标定命令（D31）")
    parser.add_argument("--out", required=True, help="表库根目录（D31）")
    parser.add_argument("--engine", default=None, help="引擎端点")
    parser.add_argument("--fetch-endpoint", default=None, help="取回组件端点")
    parser.add_argument("--model-path", default=None, help="模型路径")
    parser.add_argument("--kv-precision", default="unknown", help="KV 精度口径")
    parser.add_argument(
        "--block-tokens", type=int, default=16, help="KV 块 token 数口径"
    )
    parser.add_argument(
        "--kv-bytes-per-token",
        type=float,
        default=None,
        help="每 token KV 字节数（可缺省，缺省按模型 config.json 推算，D37）",
    )
    parser.add_argument("--trace", default=None, help="定值扫描轨迹路径")
    parser.add_argument(
        "--nominal-file", default=None, help="路径到标称带宽的 JSON 映射"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    table_root = Path(args.out).expanduser().resolve()
    # D36：标定命令与探针、被动聚合器同经该键定位表库根，测量块文件落在其下
    os.environ[config.ENV_COST_TABLE_ROOT] = str(table_root)
    try:
        # D37：每 token KV 字节显式值直接使用，缺省按模型 config.json 推算，
        # 推算不适用（MLA）且未显式指定时报错退出
        kv_bytes_per_token = _resolve_kv_bytes_per_token(args)
        # 引擎客户端先装配：端点缺失时早失败，不空跑 T1
        engine = _build_engine_client(args.engine)
        nominal = _load_nominal(args.nominal_file)
        print("采口径字段", file=sys.stderr)
        provenance = config.collect_provenance(
            args.engine or "",
            args.model_path or "unknown",
            args.kv_precision,
            args.block_tokens,
        )
        print("T1 传输标定", file=sys.stderr)
        t1 = fetchbench.build_t1(nominal)
        print("存储写标定", file=sys.stderr)
        # D39：写列标定紧跟 T1 读标定，同窗口冷态，不依赖引擎写路径；
        # 写方向标称带宽经 --nominal-file 的 nvme-write 键可选提供
        write_fit = writebench.build_write_fit(
            nominal.get(writebench.WRITE_ROW) if nominal else None
        )
        print("T2 重算标定", file=sys.stderr)
        t2 = recompbench.build_t2(engine)
        print("T3 干扰对测", file=sys.stderr)
        interbench.set_engine(engine)
        # D38：rpc_s 探测请求按单块取回查询形态装配进 interbench
        interbench._probe_rpc_once = _probe_rpc_d38
        # D37：定值扫描重放的两笔账按显式或推算的每 token KV 字节换算
        interbench._REPLAY_KV_BYTES_PER_TOKEN = kv_bytes_per_token
        t3_forward, t3_reverse = interbench.build_t3("nvme-direct")
        cost_tables = tables.CostTables(
            version="",
            provenance=provenance,
            calibrated_at=datetime.now(UTC).isoformat(),
            t1=t1,
            t2=t2,
            t3_forward=t3_forward,
            t3_reverse=t3_reverse,
            t4={},
            tuned={},
            # D39：写上限落 T1 表文件的 t1_write 列组，取回与重算两笔账不用写方向数据
            t1_write={writebench.WRITE_ROW: write_fit},
        )
        print("T4 开销实测", file=sys.stderr)
        cost_tables.t4 = interbench.measure_decision_overhead(
            cost_tables, args.fetch_endpoint
        )
        print("定值扫描", file=sys.stderr)
        cost_tables.tuned = interbench.sweep_tuned(
            Path(args.trace) if args.trace else None, cost_tables
        )
        final_dir = tables.write_version(table_root, cost_tables)
        print(f"落盘版本目录: {final_dir}", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"标定失败，退出码 1，不落版本目录: {exc}", file=sys.stderr)
        return 1


class _VllmEngineClient:
    """vLLM OpenAI 兼容服务器的真实引擎客户端：端点交互全走 HTTP，不 import vllm 包。

    prefill_segment 与 decode_batch 经 /v1/completions 发请求；clear_kv_pool
    经 dev 管理端点 /sleep 与 /wake_up 清引擎 KV 池。
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = (endpoint or "").strip().rstrip("/")

    def prefill_segment(self, prompt_tokens: int) -> tuple[int, float]:
        # 取法依据：非流式响应计总耗时（请求发起到响应接收的壁钟时长）、
        # usage 字段取 token 数；prompt 为全新随机 token id 序列保证引擎
        # 零命中，零命中下 usage.prompt_tokens 即实际计算 token 数，命中段
        # 不计入分母（D27）；max_tokens=1 的一步 decode 相对 prefill 量级可忽略
        payload = {
            "prompt": _random_token_ids(int(prompt_tokens)),
            "max_tokens": 1,
            "temperature": 0.0,
            "stream": False,
        }
        start = time.perf_counter()
        usage = _post_json(self._completions_url(), payload, _HTTP_TIMEOUT_S)
        elapsed = time.perf_counter() - start
        computed = int(usage.get("prompt_tokens", 0))
        if computed <= 0:
            raise RuntimeError("completions 响应 usage.prompt_tokens 非正（D27）")
        return computed, elapsed

    def decode_batch(self, batch: int, prompt_tokens: int) -> float:
        # decode 步时间：batch 路并发 max_tokens=1 请求，窗口耗时从并发发出
        # 计到全部响应返回；prompt_tokens=0 时按最短单 token prompt 发起
        if batch <= 0:
            raise ValueError(f"decode 批必须为正数: {batch}")
        payload = {
            "prompt": _random_token_ids(max(int(prompt_tokens), 1)),
            "max_tokens": 1,
            "temperature": 0.0,
            "stream": False,
        }
        url = self._completions_url()
        errors: list[BaseException] = []

        def send_one() -> None:
            try:
                _post_json(url, payload, _HTTP_TIMEOUT_S)
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=send_one) for _ in range(batch)]
        start = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - start
        if errors:
            raise errors[0]
        return elapsed

    def clear_kv_pool(self) -> None:
        # 清引擎 KV 池走 /sleep 与 /wake_up 管理端点：两路由 vllm dev 路由提供，
        # 服务器须以环境变量 VLLM_SERVER_DEV_MODE=1 启动才挂载该组端点，且引擎
        # 须带 --enable-sleep-mode 启动；sleep level=1 丢弃 KV cache 并把权重
        # 挪到 CPU，wake_up 恢复推理服务
        _post_json(self._admin_url(_VLLM_SLEEP_PATH) + "?level=1", {}, _HTTP_TIMEOUT_S)
        _post_json(self._admin_url(_VLLM_WAKE_UP_PATH), {}, _HTTP_TIMEOUT_S)

    def _completions_url(self) -> str:
        return self._endpoint + _VLLM_COMPLETIONS_PATH

    def _admin_url(self, path: str) -> str:
        return self._endpoint + path


def _probe_rpc_d38(endpoint: str) -> float:
    # D38：对 rpc_endpoint 发一次单块取回查询请求（HTTP POST、请求体含单块
    # 占位查询字段），计请求往返耗时；取回组件落地后改测真实块读请求（D38）
    url = (endpoint or "").strip().rstrip("/") + _RPC_PROBE_PATH
    payload = {"block_mib": _RPC_PROBE_BLOCK_MIB, "query": "probe"}
    start = time.perf_counter()
    _post_json(url, payload, _RPC_PROBE_TIMEOUT_S)
    return time.perf_counter() - start


def _post_json(url: str, payload: dict, timeout_s: float) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return json.loads(response.read().decode("utf-8"))


def _random_token_ids(count: int) -> list[int]:
    return [
        random.randint(_PREFILL_TOKEN_ID_LOW, _PREFILL_TOKEN_ID_HIGH)
        for _ in range(count)
    ]


def _resolve_kv_bytes_per_token(args: argparse.Namespace) -> float:
    # D37：显式值直接使用不推算；未显式指定时按 --model-path 下 config.json
    # 推算；推算不适用且未显式指定时报错退出（由 main 以退出码 1 结束）
    if args.kv_bytes_per_token is not None:
        return float(args.kv_bytes_per_token)
    if not (args.model_path or "").strip():
        raise ValueError(
            "--kv-bytes-per-token 未指定且无 --model-path，"
            "每 token KV 字节数无从推算（D37）"
        )
    return _infer_kv_bytes_per_token(args.model_path)


def _infer_kv_bytes_per_token(model_path: str) -> float:
    # D37：每 token 字节数 = 2 × 层数 × KV 头数 × 每头维度 × 精度字节；
    # config 无 head_dim 时按 hidden_size 除以注意力头数；fp16 与 bf16 每元素
    # 2 字节、fp8 为 1 字节；含 kv_lora_rank（MLA 压缩特征）推算不适用
    payload = json.loads((Path(model_path) / "config.json").read_text(encoding="utf-8"))
    if "kv_lora_rank" in payload:
        raise ValueError(
            "模型 config.json 含 kv_lora_rank（MLA 压缩形态），"
            "每 token KV 字节数推算不适用，须显式指定（D37）"
        )
    layers = int(payload["num_hidden_layers"])
    kv_heads = int(payload.get("num_key_value_heads") or payload["num_attention_heads"])
    head_dim = payload.get("head_dim")
    if head_dim is None:
        head_dim = int(payload["hidden_size"]) // int(payload["num_attention_heads"])
    element_bytes = _precision_element_bytes(str(payload.get("torch_dtype", "")))
    return float(2 * layers * kv_heads * int(head_dim) * element_bytes)


def _precision_element_bytes(torch_dtype: str) -> int:
    key = torch_dtype.strip().lower()
    if key not in _PRECISION_ELEMENT_BYTES:
        raise ValueError(
            f"无法识别 KV 精度 {torch_dtype!r}，每 token KV 字节数推算不适用（D37）"
        )
    return _PRECISION_ELEMENT_BYTES[key]


def _load_nominal(path_text: str | None) -> dict[str, float] | None:
    if not path_text:
        return None
    payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"标称带宽映射须为 JSON 对象: {path_text}")
    return {str(key): float(value) for key, value in payload.items()}


def _build_engine_client(engine_endpoint: str | None) -> _VllmEngineClient:
    if not (engine_endpoint or "").strip():
        raise ValueError("未提供 --engine 端点，T2 起各链路无法执行（D31）")
    return _VllmEngineClient(engine_endpoint)


if __name__ == "__main__":
    sys.exit(main())
