"""重算微基准，产出 T2（D27、D31）。

EngineClient 是协议而非实现：标定与重标传真实客户端，单测传假客户端，
两边只认 prefill_segment 与 decode_batch 两个方法。真实客户端负责每次用
全新随机内容构造前缀保证引擎零命中、按引擎当前 chunk 配置切分持续发送，
并在 mixed 口径下维持 decode_batch(1, 0) 挂载的 1 路 decode 常驻流。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import tables

LENGTH_LADDER_TOKENS = (1024, 4096, 16384, 32768)
DIRECT_LENGTH_TOKENS = 65536
RECOMPUTE_MODES = ("prefill-only", "mixed")
MEASURE_REPS = 5

_LADDER_TOKENS = (*LENGTH_LADDER_TOKENS, DIRECT_LENGTH_TOKENS)
_MIXED_DECODE_BATCH = (
    1  # 混批负载固定 1 路，更大 decode 批的拖累由 T3 反向子表承担（D27）
)


class EngineClient(Protocol):
    def prefill_segment(self, prompt_tokens: int) -> tuple[int, float]:
        """返回实际计算的 token 数与 prefill 段耗时（秒）。"""

    def decode_batch(self, batch: int, prompt_tokens: int) -> float:
        """返回该步 decode 的步时间（秒）。"""


@dataclass(frozen=True)
class RecomputeSample:
    mode: str
    length_tokens: int
    per_token_s: tuple[float, ...]  # 每 token 时间序列，单位秒


def measure_recompute(
    engine: EngineClient, mode: str, length_tokens: int, reps: int = MEASURE_REPS
) -> RecomputeSample:
    if mode not in RECOMPUTE_MODES:
        raise ValueError(f"未知重算口径: {mode}（D27）")
    if reps <= 0:
        raise ValueError(f"reps 必须为正数: {reps}")
    samples: list[float] = []
    for _ in range(reps):
        if mode == "mixed":
            # 挂 1 路 decode 常驻（D27）：真实客户端在 decode_batch(1, 0) 上维持常驻流
            engine.decode_batch(_MIXED_DECODE_BATCH, 0)
        # 每 rep 独立发起新 prefill 请求，随机前缀由客户端构造保证零命中；
        # 分母取响应里的实际计算 token 数，命中段不计入分母（D27）
        computed, elapsed_s = engine.prefill_segment(length_tokens)
        if computed <= 0:
            raise RuntimeError(
                "prefill 实际计算 token 数非正，每 token 分母失真（D27）"
            )
        samples.append(elapsed_s / computed)
    return RecomputeSample(
        mode=mode, length_tokens=length_tokens, per_token_s=tuple(samples)
    )


def build_t2(engine: EngineClient) -> dict[str, dict[int, tables.RecomputeCell]]:
    result: dict[str, dict[int, tables.RecomputeCell]] = {}
    for mode in RECOMPUTE_MODES:
        cells: dict[int, tables.RecomputeCell] = {}
        for length in _LADDER_TOKENS:
            sample = measure_recompute(engine, mode, length)
            cells[length] = tables.RecomputeCell(
                per_token_s_median=_median(sample.per_token_s),
                per_token_s_p25=_p25(sample.per_token_s),
            )
        result[mode] = cells
    return result


def remeasure_length(
    engine: EngineClient, length_tokens: int
) -> dict[str, tables.RecomputeCell]:
    if length_tokens not in _LADDER_TOKENS:
        raise ValueError(f"长度档不在档位表内: {length_tokens}（D27）")
    # 单档重标两种口径都重测（D34）
    result: dict[str, tables.RecomputeCell] = {}
    for mode in RECOMPUTE_MODES:
        sample = measure_recompute(engine, mode, length_tokens)
        result[mode] = tables.RecomputeCell(
            per_token_s_median=_median(sample.per_token_s),
            per_token_s_p25=_p25(sample.per_token_s),
        )
    return result


def _median(values: tuple[float, ...]) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * 0.5)]


def _p25(values: tuple[float, ...]) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * 0.25)]
