"""装置唯一命令入口：tracegen、up、run、reset、collect（D12）。"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import config
import record


def _print_error(message: str) -> None:
    print(f"错误: {message}", file=sys.stderr)


def _add_tier(parser: argparse.ArgumentParser, required: bool = True) -> None:
    parser.add_argument("--tier", required=required, help="模型档 S / M / L")


def _add_run_knobs(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--cell", default=config.DEFAULT_CELL, help="格 ID，或 all 表示全族串行"
    )
    parser.add_argument(
        "--mode", default=config.DEFAULT_MODE, help="cold / hot，或 all 表示两种各跑"
    )
    parser.add_argument(
        "--group", default=config.DEFAULT_GROUP, help="对照族组 ID：" + " / ".join(config.GROUPS)
    )
    parser.add_argument("--kv-pool-gib", type=int, default=None, help="KV 池容量（GiB），全族同值")
    parser.add_argument("--replicas", type=int, default=None, help="引擎副本数，全族同值")
    parser.add_argument(
        "--reps", type=int, default=config.DEFAULT_REPS, help="每格跑的轮数"
    )


def _add_model_runtime(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--model-path", default=None, help="模型本地路径")
    parser.add_argument("--port", type=int, default=None, help="vLLM 基准端口")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench/run.py",
        description="KVFerry 实验装置：轨迹生成、拉起、跑轮、复位与汇总。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    tracegen = subparsers.add_parser("tracegen", help="生成轨迹")
    _add_tier(tracegen)
    tracegen.add_argument("--cell", default=config.DEFAULT_CELL, help="格 ID，或 all")
    tracegen.add_argument("--base-trace", default=None, help="Mooncake 底稿 JSONL")
    tracegen.add_argument("--corpus", default=None, help="本地语料文本文件")
    tracegen.add_argument("--baseline-rate", type=float, default=None, help="基线完成速率（req/s）")
    tracegen.add_argument("--seed", type=int, default=config.DEFAULT_SEED, help="随机种子")
    tracegen.add_argument("--n-requests", type=int, default=None, help="每格请求数")
    _add_model_runtime(tracegen)
    tracegen.add_argument("--dry-run", action="store_true", help="只打印计划，不落盘")
    tracegen.set_defaults(handler=cmd_tracegen)

    up = subparsers.add_parser("up", help="拉起网关检查与引擎副本")
    _add_tier(up)
    _add_run_knobs(up)
    _add_model_runtime(up)
    up.add_argument("--dry-run", action="store_true", help="只打印计划，不启动进程")
    up.set_defaults(handler=cmd_up)

    run = subparsers.add_parser("run", help="跑一轮或全族")
    _add_tier(run)
    _add_run_knobs(run)
    _add_model_runtime(run)
    run.add_argument(
        "--max-output-tokens",
        type=int,
        default=config.DEFAULT_MAX_OUTPUT_TOKENS,
        help="每请求输出 token 上限",
    )
    run.add_argument("--max-inflight", type=int, default=0, help="并发上限，0 表示不限")
    run.add_argument(
        "--request-timeout-s",
        type=float,
        default=config.DEFAULT_REQUEST_TIMEOUT_S,
        help="单请求超时（秒）",
    )
    run.add_argument("--dry-run", action="store_true", help="只打印计划，不发请求")
    run.set_defaults(handler=cmd_run)

    reset = subparsers.add_parser("reset", help="手动重启副本并清空 KV 池与存储层")
    _add_tier(reset)
    reset.add_argument("--group", default=config.DEFAULT_GROUP, help="组 ID")
    reset.add_argument("--dry-run", action="store_true", help="只打印计划，不执行复位")
    reset.set_defaults(handler=cmd_reset)

    collect = subparsers.add_parser("collect", help="汇总每轮有效结果")
    _add_tier(collect)
    collect.add_argument("--group", default="all", help="组 ID，或 all")
    collect.set_defaults(handler=cmd_collect)

    return parser


def cmd_tracegen(args: argparse.Namespace) -> int:
    import tracegen

    tier = config.normalize_tier(args.tier)
    model_path = (
        Path(args.model_path).expanduser() if args.model_path else config.default_model_path(tier)
    )
    base_trace = Path(args.base_trace).expanduser() if args.base_trace else None
    corpus = Path(args.corpus).expanduser() if args.corpus else None
    return tracegen.run_tracegen(
        tier=tier,
        cell_arg=args.cell,
        base_trace_path=base_trace,
        corpus_path=corpus,
        model_path=model_path,
        baseline_rate=args.baseline_rate,
        seed=args.seed,
        n_requests=args.n_requests,
        dry_run=args.dry_run,
    )


def cmd_up(args: argparse.Namespace) -> int:
    import loadgen

    tier = config.normalize_tier(args.tier)
    group = config.resolve_group(args.group)
    if group == "all":
        raise ValueError("up 的 --group 只能取单个组")
    dependency_error = config.group_dependency_error(group)
    if dependency_error:
        _print_error(dependency_error)
        return 2
    model_path = (
        Path(args.model_path).expanduser() if args.model_path else config.default_model_path(tier)
    )
    port = args.port if args.port is not None else config.DEFAULT_PORT
    params = config.group_launch_params(group)
    if args.dry_run:
        print(f"up tier={tier} group={group} model_path={model_path} port={port}")
        print(
            f"kv_pool_gib={args.kv_pool_gib} replicas={args.replicas} params={params}"
        )
        print(f"gateway={config.gateway_endpoint() or '(未配置)'}")
        return 0
    if args.kv_pool_gib is None:
        raise ValueError("up 需要 --kv-pool-gib")
    if args.replicas is None:
        raise ValueError("up 需要 --replicas")
    registry = loadgen.start_engine(
        tier=tier,
        model_path=model_path,
        base_port=port,
        kv_pool_gib=args.kv_pool_gib,
        replicas=args.replicas,
        group=group,
    )
    ports = ", ".join(str(proc["port"]) for proc in registry["procs"])
    print(f"已拉起 {len(registry['procs'])} 个副本，端口 {ports}")
    print(f"进程登记: {config.procs_path(tier)}")
    return 0


def _registry_values(
    args: argparse.Namespace, registry: dict[str, Any], group: str
) -> tuple[int, int, list[int]]:
    registered_group = registry.get("group")
    if registered_group is not None and registered_group != group:
        raise ValueError(
            f"--group={group} 与 up 登记的组 {registered_group} 不一致，重新执行 up"
        )
    kv_pool_gib = int(registry["kv_pool_gib"])
    replicas = int(registry["replicas"])
    if args.kv_pool_gib is not None and args.kv_pool_gib != kv_pool_gib:
        raise ValueError(
            f"--kv-pool-gib={args.kv_pool_gib} 与 up 登记值 {kv_pool_gib} 不一致（R3：全族同值）"
        )
    if args.replicas is not None and args.replicas != replicas:
        raise ValueError(
            f"--replicas={args.replicas} 与 up 登记值 {replicas} 不一致（R3：全族同值）"
        )
    ports = [int(proc["port"]) for proc in registry["procs"]]
    if args.port is not None and args.port != int(registry["base_port"]):
        raise ValueError(
            f"--port={args.port} 与 up 登记基准端口 {registry['base_port']} 不一致"
        )
    return kv_pool_gib, replicas, ports


def cmd_run(args: argparse.Namespace) -> int:
    import loadgen

    tier = config.normalize_tier(args.tier)
    group = config.resolve_group(args.group)
    if group == "all":
        raise ValueError("run 的 --group 只能取单个组；六组请分别执行")
    dependency_error = config.group_dependency_error(group)
    if dependency_error:
        _print_error(dependency_error)
        return 2
    cells = config.resolve_cells(args.cell)
    modes = config.resolve_modes(args.mode)
    if args.reps <= 0:
        raise ValueError("--reps 必须为正整数")
    registry = loadgen.load_registry(tier)
    if args.dry_run:
        kv_pool_gib = args.kv_pool_gib or (registry["kv_pool_gib"] if registry else "未定")
        replicas = args.replicas or (registry["replicas"] if registry else "未定")
        print(
            f"run tier={tier} group={group} cells={len(cells)}"
            f" modes={','.join(modes)} reps={args.reps}"
            f" kv_pool_gib={kv_pool_gib} replicas={replicas}"
        )
        for cell in cells:
            for mode in modes:
                for rep in range(1, args.reps + 1):
                    print(f"{cell.cell_id}\t{mode}\t{rep}")
        return 0
    if registry is None:
        raise loadgen.EngineError(
            f"{tier} 档没有副本登记，先执行 up（{config.procs_path(tier)}）"
        )
    kv_pool_gib, replicas, ports = _registry_values(args, registry, group)
    gateway = config.gateway_endpoint()
    if not gateway:
        raise loadgen.GatewayError(
            f"未配置 {config.ENV_GATEWAY_ENDPOINT}；装置不降级为直连副本"
        )
    loadgen.check_gateway(gateway)
    model_path = (
        Path(args.model_path).expanduser() if args.model_path else Path(registry["model_path"])
    )
    group_params = config.group_launch_params(group)

    failures = 0
    for cell in cells:
        for mode in modes:
            for rep in range(1, args.reps + 1):
                spec = loadgen.RoundSpec(
                    tier=tier,
                    cell=cell,
                    mode=mode,
                    group=group,
                    rep=rep,
                    kv_pool_gib=kv_pool_gib,
                    replicas=replicas,
                    model_path=model_path,
                    port=int(registry["base_port"]),
                    gateway=gateway,
                    engine_ports=ports,
                    group_params=group_params,
                    max_output_tokens=args.max_output_tokens,
                    max_inflight=args.max_inflight,
                    request_timeout_s=args.request_timeout_s,
                )
                if mode == "cold":
                    loadgen.reset_engine(tier, group)
                else:
                    loadgen.warmup_round(spec)
                result = loadgen.run_round(spec)
                metrics = result["metrics"]
                ttft = metrics["ttft_p50_ms"]
                ttft_text = "未采集" if ttft is None else f"{ttft:.3f} ms"
                state = "有效" if result["valid"] else "作废"
                print(
                    f"{cell.cell_id}\t{mode}-{rep}\t{state}"
                    f"\tcompleted={metrics['n_completed']}/{metrics['n_planned']}"
                    f"\tttft_p50={ttft_text}\t{result['run_dir']}"
                )
                if not result["valid"]:
                    failures += 1
                    print("  作废原因: " + "；".join(result["reasons"]))
    if failures:
        print(f"有 {failures} 轮作废，已移进各格 quarantine/，详见 run_meta.json")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    import loadgen

    tier = config.normalize_tier(args.tier)
    group = config.resolve_group(args.group)
    if group == "all":
        raise ValueError("reset 的 --group 只能取单个组")
    dependency_error = config.group_dependency_error(group)
    if dependency_error:
        _print_error(dependency_error)
        return 2
    if args.dry_run:
        print(f"reset tier={tier} group={group}（重启副本，清 KV 池与存储层）")
        return 0
    loadgen.reset_engine(tier, group)
    print(f"已复位 {tier} 档副本（group={group}）")
    return 0


def _fmt(value: Any, digits: int = 6) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _group_index(name: str) -> int:
    try:
        return config.GROUPS.index(name)
    except ValueError:
        return len(config.GROUPS)


def _mode_index(name: str) -> int:
    return 0 if name == "cold" else 1


def aggregate(tier: str, group: str | None) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for _, meta in record.iter_round_metas(tier, group):
        validity = meta.get("validity") or {}
        if validity.get("valid") is not True:
            continue
        metrics = meta.get("metrics") or {}
        key = (
            str(meta.get("group")),
            str(meta.get("cell")),
            str(meta.get("mode")),
        )
        bucket = buckets.setdefault(
            key,
            {
                "reps": 0,
                "ttft": [],
                "hit_input": 0,
                "cached": 0,
                "hit_collected": False,
                "fetch": 0,
                "fetch_collected": False,
            },
        )
        bucket["reps"] += 1
        bucket["ttft"].extend(
            float(value)
            for value in (metrics.get("ttft_ms") or [])
            if value is not None
        )
        if metrics.get("hit_collected") and metrics.get("cached_tokens") is not None:
            bucket["hit_collected"] = True
            bucket["cached"] += int(metrics["cached_tokens"])
            bucket["hit_input"] += int(metrics.get("input_tokens") or 0)
        if metrics.get("fetch_bytes_collected") and metrics.get("fetch_bytes") is not None:
            bucket["fetch_collected"] = True
            bucket["fetch"] += int(metrics["fetch_bytes"])

    rows: list[dict[str, Any]] = []
    for (group_name, cell_id, mode), bucket in sorted(
        buckets.items(),
        key=lambda item: (
            _group_index(item[0][0]),
            item[0][1],
            _mode_index(item[0][2]),
        ),
    ):
        ttft = bucket["ttft"]
        hit_rate = (
            bucket["cached"] / bucket["hit_input"]
            if bucket["hit_collected"] and bucket["hit_input"] > 0
            else None
        )
        rows.append(
            {
                "tier": tier,
                "group": group_name,
                "cell": cell_id,
                "mode": mode,
                "valid_reps": bucket["reps"],
                "ttft_p50_ms": _percentile(ttft, 50),
                "ttft_p90_ms": _percentile(ttft, 90),
                "prefix_hit_rate": hit_rate,
                "prompt_tokens": bucket["hit_input"] if bucket["hit_collected"] else None,
                "cached_tokens": bucket["cached"] if bucket["hit_collected"] else None,
                "fetch_bytes": bucket["fetch"] if bucket["fetch_collected"] else None,
            }
        )
    return rows


def _percentile(values: list[float], percent: float) -> float | None:
    import math

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


def cmd_collect(args: argparse.Namespace) -> int:
    tier = config.normalize_tier(args.tier)
    group = config.resolve_group(args.group)
    rows = aggregate(tier, None if group == "all" else group)
    path = config.summary_path(tier)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(config.SUMMARY_COLUMNS)
        for row in rows:
            writer.writerow(
                [
                    row["tier"],
                    row["group"],
                    row["cell"],
                    row["mode"],
                    row["valid_reps"],
                    _fmt(row["ttft_p50_ms"], 3),
                    _fmt(row["ttft_p90_ms"], 3),
                    _fmt(row["prefix_hit_rate"], 6),
                    _fmt(row["prompt_tokens"]),
                    _fmt(row["cached_tokens"]),
                    _fmt(row["fetch_bytes"]),
                ]
            )
    print(f"写入 {len(rows)} 行: {path}")
    return 0


HANDLERS: dict[str, Callable[[argparse.Namespace], int]] = {
    "tracegen": cmd_tracegen,
    "up": cmd_up,
    "run": cmd_run,
    "reset": cmd_reset,
    "collect": cmd_collect,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return HANDLERS[args.command](args)
    except KeyboardInterrupt:
        _print_error("已中断")
        return 130
    except Exception as exc:
        _print_error(f"{type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())