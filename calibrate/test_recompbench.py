"""每 token 时间口径的单测（D27）：分母取实际 token 数、随机前缀零命中、
64k 直测档同构存储、混批 1 路 decode。"""

from __future__ import annotations

import pytest

import recompbench
import tables


class FakeEngine:
    """假客户端：prefill 段耗时按请求长度计，分母随命中比例变化可区分。"""

    def __init__(self, computed_ratio: float = 1.0):
        self.computed_ratio = computed_ratio
        self.prefill_prompts: list[int] = []
        self.decode_calls: list[tuple[int, int]] = []

    def prefill_segment(self, prompt_tokens: int) -> tuple[int, float]:
        self.prefill_prompts.append(prompt_tokens)
        computed = max(1, int(prompt_tokens * self.computed_ratio))
        return computed, prompt_tokens * 2e-6

    def decode_batch(self, batch: int, prompt_tokens: int) -> float:
        self.decode_calls.append((batch, prompt_tokens))
        return 0.02


class RampEngine(FakeEngine):
    """每 rep 每 token 时间递增，用于验证五次排序取第 3 与第 2 个的分位口径。"""

    def __init__(self):
        super().__init__()
        self._calls = 0

    def prefill_segment(self, prompt_tokens: int) -> tuple[int, float]:
        self._calls += 1
        per_token = 1e-6 + 1e-7 * self._calls
        return prompt_tokens, prompt_tokens * per_token


def test_denominator_is_computed_tokens():
    engine = FakeEngine(computed_ratio=0.5)  # 模拟一半前缀命中
    sample = recompbench.measure_recompute(engine, "prefill-only", 4096, reps=1)
    # 段耗时按请求 4096 token 计，实际计算 2048 token，分母必须取实际数（D27）
    assert sample.per_token_s[0] == pytest.approx(4096 * 2e-6 / 2048)
    assert sample.per_token_s[0] != pytest.approx(2e-6)


def test_fresh_prefix_each_rep_is_independent():
    engine = FakeEngine(computed_ratio=1.0)  # 每次全新随机内容，零命中
    sample = recompbench.measure_recompute(engine, "prefill-only", 4096, reps=5)
    # 每 rep 独立发起新 prefill 请求，样本数与请求数一致
    assert len(sample.per_token_s) == 5
    assert engine.prefill_prompts == [4096] * 5
    assert all(per_token == pytest.approx(2e-6) for per_token in sample.per_token_s)


def test_mixed_holds_one_decode_lane():
    engine = FakeEngine()
    recompbench.measure_recompute(engine, "mixed", 4096, reps=3)
    # 混批负载固定 1 路 decode 常驻（D27）
    assert engine.decode_calls == [(1, 0)] * 3


def test_prefill_only_has_no_decode():
    engine = FakeEngine()
    recompbench.measure_recompute(engine, "prefill-only", 4096, reps=2)
    assert engine.decode_calls == []


def test_build_t2_lattice_shape_and_direct_rung():
    engine = FakeEngine()
    t2 = recompbench.build_t2(engine)
    assert set(t2) == set(recompbench.RECOMPUTE_MODES)
    ladder = (*recompbench.LENGTH_LADDER_TOKENS, recompbench.DIRECT_LENGTH_TOKENS)
    for cells in t2.values():
        assert set(cells) == set(ladder)
        for cell in cells.values():
            assert cell.per_token_s_median == pytest.approx(2e-6)
            assert cell.per_token_s_p25 == pytest.approx(2e-6)
    # 64k 直测档与四档同构存储，同为 RecomputeCell 格值，不拟合（D27）
    direct = t2["mixed"][recompbench.DIRECT_LENGTH_TOKENS]
    assert isinstance(direct, tables.RecomputeCell)
    assert direct == tables.RecomputeCell(
        per_token_s_median=2e-6, per_token_s_p25=2e-6)


def test_quantiles_take_third_and_second_of_five():
    engine = RampEngine()
    cells = recompbench.remeasure_length(engine, 4096)
    # 每 reps 5 个递增样本：中位数取第 3 个、p25 取第 2 个
    assert cells["prefill-only"].per_token_s_median == pytest.approx(1.3e-6)
    assert cells["prefill-only"].per_token_s_p25 == pytest.approx(1.2e-6)
    assert cells["mixed"].per_token_s_median == pytest.approx(1.8e-6)
    assert cells["mixed"].per_token_s_p25 == pytest.approx(1.7e-6)


def test_remeasure_length_covers_both_modes():
    engine = FakeEngine()
    cells = recompbench.remeasure_length(engine, 4096)
    assert set(cells) == set(recompbench.RECOMPUTE_MODES)  # 两种口径都重测（D34）
    with pytest.raises(ValueError):
        recompbench.remeasure_length(engine, 2048)  # 不在档位表内


def test_measure_recompute_rejects_unknown_mode():
    engine = FakeEngine()
    with pytest.raises(ValueError):
        recompbench.measure_recompute(engine, "decode-heavy", 4096)