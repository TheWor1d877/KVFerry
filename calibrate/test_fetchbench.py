"""线性拟合与三重检查的覆盖：斜率倒数与截距、双分位拟合、最小档剔除、三重检查、加密挡位（D31、D27）。"""

from __future__ import annotations

import pytest

import fetchbench
import tables


def _linear(block_mib: int, ic: float = 0.01) -> float:
    return block_mib / 1024 + ic


def _seq(center: float, spread: float) -> tuple[float, ...]:
    return (center - spread, center - spread / 2, center,
            center + spread / 2, center + spread)


def _sample(path: str, block_mib: int, seq: tuple[float, ...]) -> fetchbench.BlockSample:
    return fetchbench.BlockSample(path=path, block_mib=block_mib, elapsed_s=seq)


def _linear_samples(path: str, ic: float = 0.01,
                    overrides: dict[int, float] | None = None) -> list:
    overrides = overrides or {}
    ladder = fetchbench.PATH_BLOCK_LADDERS[path]
    samples = []
    for block in ladder:
        center = overrides.get(block, _linear(block, ic))
        # 每档带微小极差，浮点拟合残差（1e-16 级）不误触残差违规
        samples.append(_sample(path, block, _seq(center, center * 1e-6)))
    return samples


def _make_fake(table: dict[tuple[str, int], tuple[float, ...]] | None = None):
    table = table or {}
    calls: list[tuple[str, int]] = []

    def fake(path: str, block_mib: int, reps: int = fetchbench.MEASURE_REPS):
        calls.append((path, block_mib))
        seq = table.get((path, block_mib))
        if seq is None:
            center = _linear(block_mib)
            seq = _seq(center, center * 0.004)
        return fetchbench.BlockSample(path=path, block_mib=block_mib,
                                      elapsed_s=seq[:reps])

    return fake, calls


def test_fit_bandwidth_slope_inverse_and_intercept():
    # 全同值序列使 median 与 p25 为同一套精确共线点，直接验证斜率倒数与截距
    samples = []
    for block in fetchbench.PATH_BLOCK_LADDERS["dram"]:
        center = _linear(block)
        samples.append(_sample("dram", block, (center,) * 5))
    fit = fetchbench.fit_bandwidth(samples)
    assert fit.bandwidth_gib_s_median == pytest.approx(1.0, rel=1e-9)
    assert fit.fixed_cost_s_median == pytest.approx(0.01, abs=1e-9)
    assert fit.bandwidth_gib_s_p25 == pytest.approx(1.0, rel=1e-9)
    assert fit.fixed_cost_s_p25 == pytest.approx(0.01, abs=1e-9)


def test_fit_bandwidth_dual_quantile_fits():
    med_ic, p25_ic = 0.01, 0.005
    ladder = fetchbench.PATH_BLOCK_LADDERS["dram"]
    samples = []
    for block in ladder:
        fast = _linear(block, p25_ic)
        slow = _linear(block, med_ic)
        samples.append(_sample("dram", block, (fast, fast, slow, slow, slow)))
    fit = fetchbench.fit_bandwidth(samples)
    assert fit.bandwidth_gib_s_median == pytest.approx(1.0, rel=1e-9)
    assert fit.fixed_cost_s_median == pytest.approx(med_ic, abs=1e-9)
    assert fit.bandwidth_gib_s_p25 == pytest.approx(1.0, rel=1e-9)
    assert fit.fixed_cost_s_p25 == pytest.approx(p25_ic, abs=1e-9)


def test_fit_bandwidth_excludes_min_block():
    ladder = fetchbench.PATH_BLOCK_LADDERS["dram"]
    samples = []
    for block in ladder:
        center = 100.0 if block == fetchbench.MIN_FIT_BLOCK_MIB else _linear(block)
        samples.append(_sample("dram", block, (center,) * 5))
    fit = fetchbench.fit_bandwidth(samples)
    assert fit.bandwidth_gib_s_median == pytest.approx(1.0, rel=1e-9)
    assert fit.fixed_cost_s_median == pytest.approx(0.01, abs=1e-9)
    assert fit.residuals_s[fetchbench.MIN_FIT_BLOCK_MIB] == pytest.approx(
        100.0 - _linear(8), abs=1e-9)


def test_check_fit_bandwidth_gap_violation():
    samples = _linear_samples("dram", overrides={64: 0.12})
    fit = fetchbench.fit_bandwidth(samples)
    violations = fetchbench.check_fit(samples, fit, None)
    assert any("相邻档 8-64" in item for item in violations)
    assert any("相邻档 64-256" in item for item in violations)


def test_check_fit_residual_violation():
    samples = _linear_samples("dram", overrides={4096: _linear(4096) + 0.001})
    fit = fetchbench.fit_bandwidth(samples)
    violations = fetchbench.check_fit(samples, fit, None)
    assert any("4096" in item and "残差" in item for item in violations)
    assert not any("相邻档" in item for item in violations)


def test_check_fit_nominal_bandwidth_violation():
    samples = _linear_samples("dram")
    fit = fetchbench.fit_bandwidth(samples)
    violations = fetchbench.check_fit(samples, fit, 10.0)
    assert len(violations) == 1
    assert "低于标称" in violations[0]


def test_check_fit_nominal_none_judges_intercept_only():
    samples = _linear_samples("dram")
    fit = fetchbench.fit_bandwidth(samples)
    assert fetchbench.check_fit(samples, fit, None) == []


def test_check_fit_intercept_violation():
    samples = _linear_samples("dram", ic=0.2)
    fit = fetchbench.fit_bandwidth(samples)
    violations = fetchbench.check_fit(samples, fit, None)
    assert len(violations) == 1
    assert "截距" in violations[0]


_REFINE_TABLE = {
    ("nvme-staged", 8): (0.019008, 0.021008, 0.023008, 0.025008, 0.027008),
    ("nvme-staged", 64): (0.0705, 0.0715, 0.0725, 0.0735, 0.0745),
    ("nvme-staged", 256): (0.258, 0.259, 0.26, 0.261, 0.262),
    ("nvme-staged", 32): (0.0422, 0.0432, 0.0442, 0.0452, 0.0462),
}


def test_build_t1_refine_round_success(monkeypatch):
    fake, calls = _make_fake(_REFINE_TABLE)
    monkeypatch.setattr(fetchbench, "measure_block", fake)
    t1 = fetchbench.build_t1(None)
    assert [b for p, b in calls if p == "nvme-staged"] == [8, 64, 256, 32]
    fit = t1["nvme-staged"]
    assert isinstance(fit, tables.PathFit)
    assert fit.min_block_mib == 8
    assert fit.min_block_s_median == pytest.approx(0.023008, abs=1e-9)
    assert fit.bandwidth_gib_s_median == pytest.approx(1.008, rel=0.02)


def test_build_t1_refine_rounds_exhausted(monkeypatch):
    fake, calls = _make_fake({
        ("nvme-staged", 8): (0.196, 0.198, 0.2, 0.202, 0.204),
        ("nvme-staged", 16): (0.0176, 0.0178, 0.018, 0.0182, 0.0184),
    })
    monkeypatch.setattr(fetchbench, "measure_block", fake)
    with pytest.raises(RuntimeError, match="三重检查"):
        fetchbench.build_t1(None)
    assert [b for p, b in calls if p == "nvme-staged"] == [8, 64, 256, 32, 16]


def test_build_t1_all_paths_and_min_block_direct(monkeypatch):
    fake, calls = _make_fake()
    monkeypatch.setattr(fetchbench, "measure_block", fake)
    t1 = fetchbench.build_t1(None)
    assert set(t1) == {"dram", "nvme-direct", "nvme-staged", "tcp-loopback"}
    fit = t1["dram"]
    assert fit.min_block_mib == 8
    assert fit.min_block_s_median == pytest.approx(_linear(8), rel=1e-9)
    assert fit.min_block_s_p25 == pytest.approx(_linear(8) - _linear(8) * 0.002, rel=1e-9)
    assert fit.bandwidth_gib_s_median == pytest.approx(1.0, rel=1e-6)
    assert fit.fixed_cost_s_median == pytest.approx(0.01, abs=1e-9)
    assert [b for p, b in calls if p == "dram"] == [8, 64, 256, 1024, 4096]


def test_remeasure_path_single_path(monkeypatch):
    fake, calls = _make_fake(_REFINE_TABLE)
    monkeypatch.setattr(fetchbench, "measure_block", fake)
    fit = fetchbench.remeasure_path("nvme-staged", None)
    assert [b for p, b in calls if p == "nvme-staged"] == [8, 64, 256, 32]
    assert fit.min_block_s_median == pytest.approx(0.023008, abs=1e-9)
    with pytest.raises(RuntimeError, match="未知传输路径"):
        fetchbench.remeasure_path("fc-direct", None)