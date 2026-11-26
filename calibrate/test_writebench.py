"""存储写基准与写列落表的单测（D39）：五档顺序调用、拟合斜率倒数与截距、
三重检查未过抛错、数据目录文件落点与复用、写列落盘加载 roundtrip；
测量实现不触真实 O_DIRECT 写盘，拟合与检查经注入的测量函数替换。"""

from __future__ import annotations

import json

import config
import fetchbench
import pytest
import tables
import writebench


def _linear(block_mib: int, ic: float = 0.02) -> float:
    return block_mib / 1024 + ic


def _seq(center: float, spread: float) -> tuple[float, ...]:
    return (
        center - spread,
        center - spread / 2,
        center,
        center + spread / 2,
        center + spread,
    )


def _write_sample(block_mib: int, seq: tuple[float, ...]) -> fetchbench.BlockSample:
    return fetchbench.BlockSample(
        path=writebench.WRITE_ROW, block_mib=block_mib, elapsed_s=seq
    )


def _make_fake(table: dict[int, tuple[float, ...]] | None = None):
    table = table or {}
    calls: list[int] = []

    def fake(block_mib: int, reps: int = writebench.MEASURE_REPS):
        calls.append(block_mib)
        seq = table.get(block_mib)
        if seq is None:
            center = _linear(block_mib)
            seq = _seq(center, center * 0.004)
        return _write_sample(block_mib, seq[:reps])

    return fake, calls


def test_build_write_fit_ladder_and_fit(monkeypatch):
    fake, calls = _make_fake()
    monkeypatch.setattr(writebench, "measure_write_block", fake)
    fit = writebench.build_write_fit(None)
    # 档位与读口径 nvme-direct 同五档（D39）
    assert calls == [8, 64, 256, 1024, 4096]
    assert fit.bandwidth_gib_s_median == pytest.approx(1.0, rel=1e-6)
    assert fit.fixed_cost_s_median == pytest.approx(0.02, abs=1e-9)
    assert fit.min_block_mib == 8
    assert fit.min_block_s_median == pytest.approx(_linear(8), rel=1e-9)
    assert fit.min_block_s_p25 == pytest.approx(
        _linear(8) - _linear(8) * 0.002, rel=1e-9
    )


def test_build_write_fit_check_fail_raises(monkeypatch):
    fake, _ = _make_fake({8: (0.196, 0.198, 0.2, 0.202, 0.204)})
    monkeypatch.setattr(writebench, "measure_write_block", fake)
    # 三重检查未过抛 RuntimeError，由标定命令按 D31 以退出码 1 结束
    with pytest.raises(RuntimeError, match="三重检查"):
        writebench.build_write_fit(None)


def test_write_target_reuse_under_table_root(tmp_path, monkeypatch):
    monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, str(tmp_path))
    target = writebench._write_target(8)
    # 写方向大文件落表库根下 .writebench 目录，与读探针块文件目录分开（D39）
    assert target.parent == tmp_path / ".writebench"
    assert target.stat().st_size == 8 * 1024**2
    inode = target.stat().st_ino
    # 目标已存在且大小一致：复用不重建
    assert writebench._write_target(8).stat().st_ino == inode


def _make_provenance() -> config.Provenance:
    return config.Provenance(
        gpu_name="NVIDIA A100",
        nic="eth0",
        pcie_topology="17:00.0",
        driver_version="550.54",
        engine_version="0.9.2",
        model="qwen3-4b",
        kv_precision="fp16",
        block_tokens=16,
    )


def _write_fit(bandwidth: float = 3.0) -> tables.PathFit:
    return tables.PathFit(
        bandwidth_gib_s_median=bandwidth,
        bandwidth_gib_s_p25=bandwidth * 0.9,
        fixed_cost_s_median=1e-4,
        fixed_cost_s_p25=1.1e-4,
        min_block_mib=8,
        min_block_s_median=3e-3,
        min_block_s_p25=3.3e-3,
    )


def _make_tables() -> tables.CostTables:
    read_fit = tables.PathFit(
        bandwidth_gib_s_median=7.0,
        bandwidth_gib_s_p25=6.5,
        fixed_cost_s_median=1e-4,
        fixed_cost_s_p25=1.2e-4,
        min_block_mib=8,
        min_block_s_median=2e-3,
        min_block_s_p25=2.2e-3,
    )
    return tables.CostTables(
        version="",
        provenance=_make_provenance(),
        calibrated_at="2026-01-01T00:00:00Z",
        t1={"nvme-direct": read_fit},
        t2={},
        t3_forward={},
        t3_reverse={},
        t4={"lookup_s": 1e-6, "rpc_s": 1e-4},
        tuned={"hysteresis": 0.8, "gate_pct": 5.0, "concurrency_cap": 4.0},
        t1_write={writebench.WRITE_ROW: _write_fit()},
    )


def test_write_column_roundtrip(tmp_path):
    root = tmp_path / "tables"
    source = _make_tables()
    final = tables.write_version(root, source)
    payload = json.loads((final / tables.T1_FILE).read_text(encoding="utf-8"))
    # 写方向列组与读列同在 T1 表文件内（D39）
    assert set(payload["t1_write"]) == {writebench.WRITE_ROW}
    loaded = tables.CostTables.load(root, final.name)
    assert loaded.t1_write == source.t1_write


def test_write_column_defaults_empty(tmp_path):
    # 未产写列的构造缺省空列组，落盘为空 t1_write，加载回空 dict（D39）
    root = tmp_path / "tables"
    source = _make_tables()
    source.t1_write = {}
    final = tables.write_version(root, source)
    payload = json.loads((final / tables.T1_FILE).read_text(encoding="utf-8"))
    assert payload["t1_write"] == {}
    loaded = tables.CostTables.load(root, final.name)
    assert loaded.t1_write == {}
