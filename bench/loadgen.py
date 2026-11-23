"""负载台：轨迹回放、并发发请求、指标采集、每秒采样、轮次判定与引擎进程管理。"""

from __future__ import annotations

import asyncio
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import config
import record

TRACE_META_FIELDS = (
    "prefix_ratio",
    "length_tier",
    "load_tier",
    "model_tier",
    "synthetic_ratio",
    "n_requests",
    "seed",
)
TRACE_REQUEST_FIELDS = ("t_ms", "n_input", "session", "text")
WAITING_METRIC = "vllm:num_requests_waiting"
ENGINE_LOG_PREFIX = "engine-"
WARMUP_LOG_NAME = "warmup.log"
ROUND_LOG_NAME = "loadgen.log"


class GatewayError(RuntimeError):
    pass


class EngineError(RuntimeError):
    pass


class DependencyError(RuntimeError):
    pass


@dataclass
class RoundSpec:
    tier: str
    cell: config.Cell
    mode: str
    group: str
    rep: int
    kv_pool_gib: int
    replicas: int
    model_path: Path
    port: int
    gateway: str
    engine_ports: list[int]
    group_params: dict[str, Any] = field(default_factory=dict)
    max_output_tokens: int = config.DEFAULT_MAX_OUTPUT_TOKENS
    max_inflight: int = 0
    request_timeout_s: float = config.DEFAULT_REQUEST_TIMEOUT_S


def _iso(timestamp: float | None = None) -> str:
    return datetime.fromtimestamp(timestamp or time.time()).astimezone().isoformat(
        timespec="milliseconds"
    )


def check_gateway(endpoint: str | None) -> str:
    if not endpoint:
        raise GatewayError(
            f"未配置 {config.ENV_GATEWAY_ENDPOINT}；装置不降级为直连副本"
        )
    base = endpoint.rstrip("/")
    try:
        response = __import__("httpx").get(f"{base}/health", timeout=5.0)
    except Exception as exc:
        raise GatewayError(f"网关端点不可达: {base}（{type(exc).__name__}: {exc}）")
    if response.status_code >= 400:
        raise GatewayError(
            f"网关端点健康检查失败: {base}/health 返回 HTTP {response.status_code}"
        )
    return base


def load_trace(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not path.is_file():
        raise EngineError(f"轨迹文件不存在: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        lines = [line for line in handle if line.strip()]
    if not lines:
        raise EngineError(f"轨迹文件为空: {path}")
    try:
        meta = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise EngineError(f"轨迹元数据不是合法 JSON: {path}（{exc}）")
    missing = [name for name in TRACE_META_FIELDS if name not in meta]
    if missing:
        raise EngineError(f"轨迹元数据缺字段: {', '.join(missing)}")
    requests: list[dict[str, Any]] = []
    for index, line in enumerate(lines[1:]):
        try:
            request = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EngineError(f"轨迹第 {index + 2} 行不是合法 JSON: {exc}")
        missing = [name for name in TRACE_REQUEST_FIELDS if name not in request]
        if missing:
            raise EngineError(f"轨迹第 {index + 2} 行缺字段: {', '.join(missing)}")
        requests.append(request)
    if len(requests) != int(meta.get("n_requests", -1)):
        raise EngineError(
            f"轨迹请求数与元数据不一致: 实际 {len(requests)}，元数据 {meta.get('n_requests')}"
        )
    return meta, requests


def _vllm_entry() -> list[str]:
    venv_bin = config.REPO_ROOT / ".venv" / "bin" / "vllm"
    if venv_bin.is_file():
        return [str(venv_bin), "serve"]
    found = shutil.which("vllm")
    if found:
        return [found, "serve"]
    return [sys.executable, "-m", "vllm.entrypoints.openai.api_server"]


def build_vllm_command(
    tier: str, model_path: Path, port: int, kv_pool_gib: int, group: str
) -> list[str]:
    entry = _vllm_entry()
    context_limit = config.TOKEN_LIMIT_BY_TIER[tier]
    kv_bytes = int(kv_pool_gib * config.KV_BYTES_PER_GIB)
    if entry[-1] == "serve":
        command = entry + [str(model_path)]
    else:
        command = entry + ["--model", str(model_path)]
    command += [
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--max-model-len",
        str(context_limit),
        "--kv-cache-memory-bytes",
        str(kv_bytes),
    ]
    if group == "connector":
        budget = config.connector_budget_bytes()
        if budget is None:
            raise DependencyError(
                f"依赖组件未就绪：组 connector 需要 {config.ENV_CONNECTOR_BUDGET}（connector-budget，单位字节）"
            )
        transfer = {
            "kv_connector": "OffloadingConnector",
            "kv_role": "kv_both",
            "kv_connector_extra_config": {"cpu_bytes_to_use": budget},
        }
        command += [
            "--kv-transfer-config",
            json.dumps(transfer, ensure_ascii=False, separators=(",", ":")),
        ]
    return command


def _engine_env(tier: str, group: str) -> dict[str, str]:
    env = os.environ.copy()
    env["KVFERRY_BENCH_TIER"] = tier
    env["KVFERRY_BENCH_GROUP"] = group
    for key, value in config.group_launch_params(group).items():
        name = "KVFERRY_BENCH_" + key.upper().replace("-", "_")
        env[name] = "" if value is None else str(value)
    return env


def load_registry(tier: str) -> dict[str, Any] | None:
    path = config.procs_path(tier)
    if not path.is_file():
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_registry(tier: str, registry: dict[str, Any]) -> Path:
    path = config.procs_path(tier)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(registry, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(tmp, path)
    return path


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _spawn(command: list[str], log_path: Path, env: dict[str, str]) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as log_handle:
        return subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )


def _wait_health(port: int, pid: int, timeout: float) -> None:
    import httpx

    deadline = time.monotonic() + timeout
    last = "尚无响应"
    url = f"http://127.0.0.1:{port}/health"
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            raise EngineError(f"副本端口 {port} 进程提前退出（pid={pid}）")
        try:
            response = httpx.get(url, timeout=3.0)
            if response.status_code < 400:
                return
            last = f"HTTP {response.status_code}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(2.0)
    raise EngineError(f"副本端口 {port} 健康检查未通过: {last}")


def _stop_pids(procs: list[dict[str, Any]], grace_s: float = 20.0) -> None:
    pids = [int(proc["pid"]) for proc in procs if _pid_alive(int(proc["pid"]))]
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline and any(_pid_alive(pid) for pid in pids):
        time.sleep(0.5)
    for pid in pids:
        if _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def stop_engine(tier: str) -> None:
    registry = load_registry(tier)
    if registry is None:
        return
    _stop_pids(registry.get("procs", []))
    path = config.procs_path(tier)
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def start_engine(
    tier: str,
    model_path: Path,
    base_port: int,
    kv_pool_gib: int,
    replicas: int,
    group: str,
) -> dict[str, Any]:
    gateway = check_gateway(config.gateway_endpoint())
    if load_registry(tier) is not None:
        stop_engine(tier)
    if replicas <= 0:
        raise EngineError("replicas 必须为正整数")
    if kv_pool_gib <= 0:
        raise EngineError("kv-pool-gib 必须为正整数")
    env = _engine_env(tier, group)
    procs: list[dict[str, Any]] = []
    for index in range(replicas):
        port = base_port + index
        command = build_vllm_command(tier, model_path, port, kv_pool_gib, group)
        log_path = config.logs_dir(tier) / f"{ENGINE_LOG_PREFIX}{port}.log"
        process = _spawn(command, log_path, env)
        procs.append(
            {
                "port": port,
                "pid": process.pid,
                "log_path": str(log_path),
                "command": command,
            }
        )
    registry = {
        "tier": tier,
        "model_path": str(model_path),
        "kv_pool_gib": kv_pool_gib,
        "replicas": replicas,
        "base_port": base_port,
        "group": group,
        "gateway": gateway,
        "launch_params": config.group_launch_params(group),
        "procs": procs,
        "created_at": _iso(),
    }
    save_registry(tier, registry)
    try:
        for proc in procs:
            _wait_health(int(proc["port"]), int(proc["pid"]), config.DEFAULT_HEALTH_TIMEOUT_S)
    except Exception:
        _stop_pids(procs)
        try:
            config.procs_path(tier).unlink()
        except FileNotFoundError:
            pass
        raise
    return registry


def _clear_storage(group: str) -> None:
    if group == "connector":
        return
    if group in config.STORAGE_GROUPS:
        raise DependencyError(
            f"依赖组件未就绪：组 {group} 需要存储层复位接口（装置阶段未实现）"
        )


def reset_engine(tier: str, group: str) -> dict[str, Any]:
    registry = load_registry(tier)
    if registry is None:
        raise EngineError(
            f"{tier} 档没有副本登记: {config.procs_path(tier)}；先执行 up"
        )
    commands = [list(proc["command"]) for proc in registry["procs"]]
    _stop_pids(registry["procs"])
    env = _engine_env(tier, group)
    procs: list[dict[str, Any]] = []
    for command in commands:
        port = int(command[command.index("--port") + 1])
        log_path = config.logs_dir(tier) / f"{ENGINE_LOG_PREFIX}{port}.log"
        process = _spawn(command, log_path, env)
        procs.append(
            {
                "port": port,
                "pid": process.pid,
                "log_path": str(log_path),
                "command": command,
            }
        )
    registry["procs"] = procs
    registry["group"] = group
    registry["launch_params"] = config.group_launch_params(group)
    registry["created_at"] = _iso()
    save_registry(tier, registry)
    try:
        for proc in procs:
            _wait_health(int(proc["port"]), int(proc["pid"]), config.DEFAULT_HEALTH_TIMEOUT_S)
    except Exception:
        _stop_pids(procs)
        raise
    _clear_storage(group)
    return registry


class RoundLogger:
    def __init__(self, path: Path, identity: dict[str, Any]):
        self.path = path
        self.identity = identity
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a", encoding="utf-8", newline="\n", buffering=1)

    def emit(self, event: str, **fields: Any) -> None:
        payload = {"ts": _iso(), "event": event}
        payload.update(self.identity)
        payload.update(fields)
        self._handle.write(record.jsonl_line(payload))

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()


class Sampler:
    def __init__(self, ports: list[int], t0: float):
        import psutil

        self.ports = list(ports)
        self.t0 = t0
        self.process = psutil.Process()
        self.process.cpu_percent(interval=None)

    def sample(self) -> dict[str, Any]:
        queue_length = self._queue_length()
        cpu_percent = self.process.cpu_percent(interval=None)
        try:
            connections = len(self.process.net_connections(kind="inet"))
        except (OSError, RuntimeError):
            connections = None
        return {
            "t_rel_s": round(time.monotonic() - self.t0, 3),
            "queue_len": queue_length,
            "loadgen_cpu_pct": cpu_percent,
            "loadgen_conns": connections,
        }

    def _queue_length(self) -> float | None:
        import httpx

        total = 0.0
        seen = False
        for port in self.ports:
            try:
                response = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=2.0)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            for line in response.text.splitlines():
                if line.startswith(WAITING_METRIC):
                    parts = line.rsplit(None, 1)
                    if len(parts) == 2:
                        try:
                            total += float(parts[1])
                            seen = True
                        except ValueError:
                            continue
        return total if seen else None


async def _send_one(
    client,
    spec: RoundSpec,
    request: dict[str, Any],
    request_id: str,
    logger: RoundLogger,
    semaphore: asyncio.Semaphore | None,
) -> dict[str, Any]:
    if semaphore is not None:
        await semaphore.acquire()
    t_send = time.monotonic()
    result: dict[str, Any] = {
        "request_id": request_id,
        "session": request["session"],
        "n_input": request["n_input"],
        "ttft_ms": None,
        "output_tokens": 0,
        "cached_tokens": None,
        "completed": False,
        "error": None,
    }
    try:
        headers = {
            "X-Request-Id": request_id,
            "X-KVFerry-Session": str(request["session"]),
            "X-KVFerry-Group": spec.group,
        }
        policy = spec.group_params.get("fetch-policy")
        if policy is not None:
            headers["X-KVFerry-Fetch-Policy"] = str(policy)
        payload = {
            "model": config.MODEL_NAME_BY_TIER[spec.tier],
            "prompt": request["text"],
            "max_tokens": spec.max_output_tokens,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        url = spec.gateway.rstrip("/") + "/v1/completions"
        first = None
        chunks = 0
        usage = None
        async with client.stream(
            "POST", url, json=payload, headers=headers
        ) as response:
            if response.status_code >= 400:
                body = await response.aread()
                raise RuntimeError(
                    f"HTTP {response.status_code}: {body[:200]!r}"
                )
            async for raw_line in response.aiter_lines():
                if not raw_line or not raw_line.startswith("data:"):
                    continue
                data = raw_line[5:].strip()
                if not data or data == "[DONE]":
                    continue
                try:
                    chunk = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if isinstance(chunk.get("usage"), dict):
                    usage = chunk["usage"]
                choices = chunk.get("choices") or []
                for choice in choices:
                    piece = choice.get("text")
                    if piece is None:
                        piece = (choice.get("delta") or {}).get("content")
                    if piece:
                        chunks += 1
                        if first is None:
                            first = time.monotonic()
        result["output_tokens"] = chunks
        if first is not None:
            result["ttft_ms"] = round((first - t_send) * 1000.0, 3)
        if usage is not None:
            details = usage.get("prompt_tokens_details") or {}
            if "cached_tokens" in details and details["cached_tokens"] is not None:
                result["cached_tokens"] = int(details["cached_tokens"])
            if usage.get("completion_tokens") is not None:
                result["usage_completion_tokens"] = int(usage["completion_tokens"])
        result["completed"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if semaphore is not None:
            semaphore.release()
    logger.emit(
        "request_done",
        request_id=request_id,
        session=request["session"],
        finished_at=_iso(),
        ttft_ms=result["ttft_ms"],
    )
    return result


async def _sample_loop(
    sampler: Sampler,
    logger: RoundLogger,
    samples: list[dict[str, Any]],
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        sample = await asyncio.to_thread(sampler.sample)
        samples.append(sample)
        logger.emit("sample", **sample)
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.SAT_SAMPLE_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


async def _replay(
    spec: RoundSpec,
    requests: list[dict[str, Any]],
    logger: RoundLogger,
    sampler: Sampler,
    samples: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    t0 = sampler.t0
    stop = asyncio.Event()
    sample_task = asyncio.create_task(_sample_loop(sampler, logger, samples, stop))
    semaphore = (
        asyncio.Semaphore(spec.max_inflight) if spec.max_inflight > 0 else None
    )
    limits = __import__("httpx").Limits(
        max_connections=None, max_keepalive_connections=None
    )
    timeout = __import__("httpx").Timeout(spec.request_timeout_s, connect=10.0)
    results: list[dict[str, Any]] = []
    try:
        async with __import__("httpx").AsyncClient(
            timeout=timeout, limits=limits
        ) as client:
            tasks = []
            for index, request in enumerate(requests):
                delay = t0 + float(request["t_ms"]) / 1000.0 - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                request_id = f"{spec.mode}{spec.rep}-{index:06d}"
                tasks.append(
                    asyncio.create_task(
                        _send_one(client, spec, request, request_id, logger, semaphore)
                    )
                )
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            for item in gathered:
                if isinstance(item, Exception):
                    results.append(
                        {
                            "request_id": "unknown",
                            "session": "unknown",
                            "n_input": 0,
                            "ttft_ms": None,
                            "output_tokens": 0,
                            "cached_tokens": None,
                            "completed": False,
                            "error": f"{type(item).__name__}: {item}",
                        }
                    )
                else:
                    results.append(item)
    finally:
        stop.set()
        await sample_task
    return results


def _percentile(values: list[float], percent: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percent / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _summarize(
    meta: dict[str, Any],
    requests: list[dict[str, Any]],
    results: list[dict[str, Any]],
    group: str,
) -> dict[str, Any]:
    completed = [item for item in results if item.get("completed")]
    failed = [item for item in results if not item.get("completed")]
    ttft = [item["ttft_ms"] for item in completed if item.get("ttft_ms") is not None]
    input_tokens = sum(int(item["n_input"]) for item in completed)
    output_tokens = sum(
        int(
            item.get("usage_completion_tokens")
            if item.get("usage_completion_tokens") is not None
            else (item.get("output_tokens") or 0)
        )
        for item in completed
    )
    cached_values = [
        int(item["cached_tokens"])
        for item in completed
        if item.get("cached_tokens") is not None
    ]
    cached_tokens = sum(cached_values) if cached_values else None
    hit_rate = (
        cached_tokens / input_tokens
        if cached_tokens is not None and input_tokens > 0
        else None
    )
    fetch_bytes = None
    fetch_collected = False
    return {
        "n_planned": len(requests),
        "n_completed": len(completed),
        "n_failed": len(failed),
        "planned_input_tokens": sum(int(item["n_input"]) for item in requests),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "ttft_ms": ttft,
        "ttft_p50_ms": _percentile(ttft, 50),
        "ttft_p90_ms": _percentile(ttft, 90),
        "cached_tokens": cached_tokens,
        "prefix_hit_rate": hit_rate,
        "hit_collected": cached_tokens is not None,
        "fetch_bytes": fetch_bytes,
        "fetch_bytes_collected": fetch_collected,
        "fetch_bytes_note": "存储层未就绪，未采集" if group in config.STORAGE_GROUPS else "该组不计取回字节",
    }


def judge_pressure(
    samples: list[dict[str, Any]], load_tier: str
) -> dict[str, Any]:
    window = [
        sample
        for sample in samples
        if float(sample.get("t_rel_s", 0.0)) <= config.MEASURE_WINDOW_S
    ]
    if not window:
        return {"ok": False, "reason": "没有采样", "window_samples": 0}
    queues = [sample.get("queue_len") for sample in window]
    if any(value is None for value in queues):
        return {
            "ok": False,
            "reason": "队列长度未采集",
            "window_samples": len(window),
        }
    busy = [float(value) > 0 for value in queues]
    busy_ratio = sum(busy) / len(busy)
    max_backlog = _max_backlog_seconds(window)
    actual = float(window[-1]["t_rel_s"]) - float(window[0]["t_rel_s"])
    if load_tier == "high":
        ok = busy_ratio == 1.0 and max_backlog >= config.HIGH_LOAD_MIN_BACKLOG_S
        reason = None if ok else "高压档要求队列全程非空且连续非空不少于 30 秒"
    elif load_tier == "low":
        ok = busy_ratio < config.LOW_LOAD_MAX_BUSY_RATIO
        reason = None if ok else "低压档要求队列非空占比低于 10%"
    else:
        return {"ok": False, "reason": f"未知压力档: {load_tier}"}
    return {
        "ok": ok,
        "reason": reason,
        "load_tier": load_tier,
        "window_s": config.MEASURE_WINDOW_S,
        "window_actual_s": round(actual, 3),
        "window_samples": len(window),
        "busy_ratio": round(busy_ratio, 6),
        "max_backlog_s": round(max_backlog, 3),
    }


def _max_backlog_seconds(window: list[dict[str, Any]]) -> float:
    best = 0.0
    start = None
    for sample in window:
        moment = float(sample["t_rel_s"])
        queue = sample.get("queue_len")
        if queue is not None and float(queue) > 0:
            if start is None:
                start = moment
            best = max(best, moment - start)
        else:
            start = None
    return best


def judge_saturation(samples: list[dict[str, Any]]) -> dict[str, Any]:
    engine_start = None
    loadgen_start = None
    for sample in samples:
        moment = float(sample.get("t_rel_s", 0.0))
        queue = sample.get("queue_len")
        if engine_start is None and queue is not None and float(queue) > 0:
            engine_start = moment
        cpu = sample.get("loadgen_cpu_pct")
        connections = sample.get("loadgen_conns")
        cpu_sat = cpu is not None and float(cpu) >= config.LOADGEN_CPU_SAT_PCT
        conn_sat = (
            connections is not None
            and float(connections) >= config.LOADGEN_CONNECTION_SAT
        )
        if loadgen_start is None and (cpu_sat or conn_sat):
            loadgen_start = moment
    ok = True
    reason = None
    if loadgen_start is not None:
        if engine_start is None or loadgen_start < engine_start:
            ok = False
            reason = "负载台先于引擎饱和"
    return {
        "ok": ok,
        "reason": reason,
        "engine_saturation_start_s": engine_start,
        "loadgen_saturation_start_s": loadgen_start,
        "cpu_sat_pct": config.LOADGEN_CPU_SAT_PCT,
        "connection_sat": config.LOADGEN_CONNECTION_SAT,
    }


def judge_reconciliation(
    tier: str,
    group: str,
    cell_id: str,
    mode: str,
    rep: int,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    previous = record.find_previous_round(tier, group, cell_id, mode, rep)
    if previous is None:
        return {"ok": True, "reason": "无同格上一有效轮，跳过对账", "previous": None}
    old = previous.get("metrics") or {}
    checks: dict[str, Any] = {}
    ok = True
    for name in ("n_completed", "input_tokens", "output_tokens"):
        left = old.get(name)
        right = metrics.get(name)
        equal = left == right
        checks[name] = {"previous": left, "current": right, "equal": equal}
        ok = ok and equal
    old_p50 = old.get("ttft_p50_ms")
    new_p50 = metrics.get("ttft_p50_ms")
    if old_p50 is None or new_p50 is None:
        checks["ttft_p50"] = {"previous": old_p50, "current": new_p50, "ok": True}
    else:
        denominator = max(abs(float(old_p50)), abs(float(new_p50)), 1e-9)
        deviation = abs(float(old_p50) - float(new_p50)) / denominator
        within = deviation <= 0.10
        checks["ttft_p50"] = {
            "previous": old_p50,
            "current": new_p50,
            "deviation": round(deviation, 6),
            "limit": 0.10,
            "ok": within,
        }
        ok = ok and within
    return {
        "ok": ok,
        "reason": None if ok else "与同格上一轮对账不通过",
        "previous_round": previous.get("mode"),
        "checks": checks,
    }


def run_round(spec: RoundSpec) -> dict[str, Any]:
    gateway = check_gateway(spec.gateway)
    trace_file = config.trace_path(spec.tier, spec.cell.cell_id)
    meta, requests = load_trace(trace_file)
    if str(meta.get("model_tier")) != spec.tier:
        raise EngineError(
            f"轨迹模型档 {meta.get('model_tier')} 与本次 {spec.tier} 不一致，重新生成轨迹"
        )
    run_dir = config.round_dir(spec.tier, spec.group, spec.cell.cell_id, spec.mode, spec.rep)
    if run_dir.exists():
        raise EngineError(f"轮目录已存在，拒绝覆盖: {run_dir}")
    record.init_round_tables(run_dir)
    identity = {
        "round": f"{spec.mode}-{spec.rep}",
        "tier": spec.tier,
        "cell": spec.cell.cell_id,
        "mode": spec.mode,
        "group": spec.group,
        "rep": spec.rep,
    }
    logger = RoundLogger(run_dir / ROUND_LOG_NAME, identity)
    started_at = time.time()
    logger.emit("round_start", started_at=_iso(started_at), n_requests=len(requests))

    t0 = time.monotonic()
    sampler = Sampler(spec.engine_ports, t0)
    samples: list[dict[str, Any]] = []
    results = asyncio.run(_replay(spec, requests, logger, sampler, samples))
    finished_at = time.time()

    metrics = _summarize(meta, requests, results, spec.group)
    reconciliation = judge_reconciliation(
        spec.tier, spec.group, spec.cell.cell_id, spec.mode, spec.rep, metrics
    )
    saturation = judge_saturation(samples)
    pressure = judge_pressure(samples, spec.cell.load_tier)
    reasons: list[str] = []
    if not reconciliation["ok"]:
        reasons.append(reconciliation["reason"] or "对账不通过")
    if not saturation["ok"]:
        reasons.append(saturation["reason"] or "饱和判定不通过")
    if not pressure["ok"]:
        reasons.append(pressure["reason"] or "压力档不通过")
    valid = not reasons

    machine = config.machine_info()
    run_meta = {
        "schema_version": 1,
        "tier": spec.tier,
        "cell": spec.cell.cell_id,
        "mode": spec.mode,
        "group": spec.group,
        "rep": spec.rep,
        "kv_pool_gib": spec.kv_pool_gib,
        "replicas": spec.replicas,
        "engine_ports": spec.engine_ports,
        "trace_file": trace_file.name,
        "trace_sha256": config.sha256_file(trace_file),
        "trace_meta": meta,
        "seed": meta.get("seed"),
        "prefix_ratio": meta.get("prefix_ratio"),
        "synthetic_ratio": meta.get("synthetic_ratio"),
        "length_tier": meta.get("length_tier"),
        "load_tier": meta.get("load_tier"),
        "model_tier": meta.get("model_tier"),
        "group_params": spec.group_params,
        "cost_table_version": config.cost_table_version(),
        "bench_code_version": config.bench_code_version(),

        "machine": machine,
        "gateway": gateway,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "duration_s": round(finished_at - started_at, 3),
        "loadgen": {
            "pid": os.getpid(),
            "cpu_sat_pct": config.LOADGEN_CPU_SAT_PCT,
            "connection_sat": config.LOADGEN_CONNECTION_SAT,
        },
        "metrics": metrics,
        "validity": {
            "valid": valid,
            "reasons": reasons,
            "reconciliation": reconciliation,
            "saturation": saturation,
            "pressure": pressure,
        },
    }
    record.write_run_meta(run_dir, run_meta)
    logger.emit(
        "round_end",
        finished_at=_iso(finished_at),
        valid=valid,
        reasons=reasons,
        ttft_p50_ms=metrics["ttft_p50_ms"],
        n_completed=metrics["n_completed"],
    )
    logger.close()
    final_dir = run_dir
    if not valid:
        final_dir = record.move_to_quarantine(
            run_dir, spec.tier, spec.group, spec.cell.cell_id
        )
    return {
        "run_dir": str(final_dir),
        "valid": valid,
        "reasons": reasons,
        "metrics": metrics,
    }


def warmup_round(spec: RoundSpec) -> int:
    gateway = check_gateway(spec.gateway)
    trace_file = config.trace_path(spec.tier, spec.cell.cell_id)
    meta, requests = load_trace(trace_file)
    if str(meta.get("model_tier")) != spec.tier:
        raise EngineError(
            f"轨迹模型档 {meta.get('model_tier')} 与本次 {spec.tier} 不一致，重新生成轨迹"
        )
    log_path = config.logs_dir(spec.tier) / WARMUP_LOG_NAME
    identity = {
        "warmup": True,
        "tier": spec.tier,
        "cell": spec.cell.cell_id,
        "mode": spec.mode,
        "group": spec.group,
    }
    logger = RoundLogger(log_path, identity)
    logger.emit("warmup_start", started_at=_iso())
    t0 = time.monotonic()
    sampler = Sampler(spec.engine_ports, t0)
    samples: list[dict[str, Any]] = []
    results = asyncio.run(_replay(spec, requests, logger, sampler, samples))
    completed = sum(1 for item in results if item.get("completed"))
    logger.emit(
        "warmup_end", finished_at=_iso(), completed=completed, n_requests=len(requests)
    )
    logger.close()
    return completed