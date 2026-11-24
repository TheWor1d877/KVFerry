"""局部重标与新版本落盘的单测（D29、D35、R18）：成功落新版本且未重标文件
原样复制、version 统一为新目录版本号、三重检查未过返回 False 不落盘、事件三连字段。"""

from __future__ import annotations

import json

import pytest

import config
import fetchbench
import recalib
import recompbench
import tables


class _FakeEngine:
    def prefill_segment(self, prompt_tokens: int) -> tuple[int, float]:
        return prompt_tokens, 0.0

    def decode_batch(self, batch: int, prompt_tokens: int) -> float:
        return 0.0


def _prov() -> config.Provenance:
    return config.Provenance(
        gpu_name="NVIDIA A100", nic="eth0", pcie_topology="17:00.0",
        driver_version="550.54", engine_version="0.9.2",
        model="qwen3-4b", kv_precision="fp16", block_tokens=16)


def _make_fit(bandwidth: float = 2.0) -> tables.PathFit:
    return tables.PathFit(
        bandwidth_gib_s_median=bandwidth, bandwidth_gib_s_p25=bandwidth * 0.9,
        fixed_cost_s_median=0.0, fixed_cost_s_p25=0.0,
        min_block_mib=8, min_block_s_median=5e-4, min_block_s_p25=5.5e-4)


def _make_tables() -> tables.CostTables:
    cells = {tokens: tables.RecomputeCell(per_token_s_median=1e-5, per_token_s_p25=1e-5)
             for tokens in (1024, 4096, 16384, 32768, 65536)}
    return tables.CostTables(
        version="", provenance=_prov(), calibrated_at="2026-01-01T00:00:00Z",
        t1={path: _make_fit() for path in fetchbench.PATH_BLOCK_LADDERS},
        t2={mode: dict(cells) for mode in recompbench.RECOMPUTE_MODES},
        t3_forward={1: {0: 1.0, 512: 0.9}},
        t3_reverse={1: {1: 0.1}},
        t4={"lookup_s": 1e-6, "rpc_s": 1e-4},
        tuned={"hysteresis": 0.8, "gate_pct": 5.0, "concurrency_cap": 4.0},
    )


def _new_fit() -> tables.PathFit:
    return tables.PathFit(
        bandwidth_gib_s_median=25.0, bandwidth_gib_s_p25=24.0,
        fixed_cost_s_median=2e-4, fixed_cost_s_p25=2.2e-4,
        min_block_mib=8, min_block_s_median=4e-4, min_block_s_p25=4.4e-4)


def _events(capsys):
    captured = capsys.readouterr()
    return [json.loads(line) for line in captured.out.splitlines()]


def test_t1_recalib_writes_new_version_copies_rest(tmp_path, monkeypatch, capsys):
    root = tmp_path / "tables"
    baseline_dir = tables.write_version(root, _make_tables())
    baseline = tables.CostTables.load(root, baseline_dir.name)
    new_fit = _new_fit()
    monkeypatch.setattr(fetchbench, "remeasure_path",
                        lambda path, nominal: new_fit)
    assert recalib.recalibrate_t1_path(root, "dram", "probe", None) is True
    dirs = [p for p in root.iterdir() if p.is_dir()]
    assert len(dirs) == 2 and baseline_dir in dirs
    new_dir = (set(dirs) - {baseline_dir}).pop()
    loaded = tables.CostTables.load(root, new_dir.name)
    # 只替换受影响的路径行，未重标数据原样复制（D29、R18）
    assert loaded.t1["dram"] == new_fit
    assert loaded.t1["nvme-direct"] == baseline.t1["nvme-direct"]
    assert loaded.t2 == baseline.t2
    assert loaded.t3_forward == baseline.t3_forward
    assert loaded.t3_reverse == baseline.t3_reverse
    assert loaded.t4 == baseline.t4
    assert loaded.tuned == baseline.tuned
    assert loaded.provenance == baseline.provenance
    # 全部表文件 version 统一写为新目录版本号（D29）
    for name in tables.TABLE_FILES:
        payload = json.loads((new_dir / name).read_text(encoding="utf-8"))
        assert payload["version"] == new_dir.name
    tables.verify_dir(new_dir, new_dir.name)
    # 事件三连：recalibrate start、recalibrate end success、degrade end（D35）
    records = _events(capsys)
    assert [(r["event"], r.get("phase"), r.get("result")) for r in records] == [
        ("recalibrate", "start", None),
        ("recalibrate", "end", "success"),
        ("degrade", "end", None),
    ]
    for record in records:
        assert record["source"] == "probe"
        assert record["table"] == "t1"
        assert record["target"] == "dram"


def test_t2_recalib_updates_target_rung_both_modes(tmp_path, monkeypatch, capsys):
    root = tmp_path / "tables"
    baseline_dir = tables.write_version(root, _make_tables())
    baseline = tables.CostTables.load(root, baseline_dir.name)
    cells = {mode: tables.RecomputeCell(per_token_s_median=9e-6, per_token_s_p25=8e-6)
             for mode in recompbench.RECOMPUTE_MODES}
    monkeypatch.setattr(recompbench, "remeasure_length",
                        lambda engine, length: cells)
    assert recalib.recalibrate_t2_length(root, 4096, "passive", _FakeEngine()) is True
    dirs = [p for p in root.iterdir() if p.is_dir()]
    new_dir = (set(dirs) - {baseline_dir}).pop()
    loaded = tables.CostTables.load(root, new_dir.name)
    # 目标档两种口径都更新，其余档与表数据原样（D34、D29）
    assert loaded.t2["mixed"][4096] == cells["mixed"]
    assert loaded.t2["prefill-only"][4096] == cells["prefill-only"]
    assert loaded.t2["mixed"][1024] == baseline.t2["mixed"][1024]
    assert loaded.t1 == baseline.t1
    records = _events(capsys)
    assert records[0]["event"] == "recalibrate" and records[0]["phase"] == "start"
    assert records[-1]["event"] == "degrade" and records[-1]["phase"] == "end"
    for record in records:
        assert record["source"] == "passive"
        assert record["table"] == "t2"
        assert record["target"] == "4096"


def test_check_fail_returns_false_without_new_dir(tmp_path, monkeypatch, capsys):
    root = tmp_path / "tables"
    baseline_dir = tables.write_version(root, _make_tables())

    def raise_check_fail(path, nominal):
        raise RuntimeError("三重检查未过（D31）")

    monkeypatch.setattr(fetchbench, "remeasure_path", raise_check_fail)
    assert recalib.recalibrate_t1_path(root, "dram", "probe", None) is False
    # 不落新版本目录，旧版本目录保持（D29）
    assert [p for p in root.iterdir() if p.is_dir()] == [baseline_dir]
    # 失败打 recalibrate end（result=fail），不补 degrade end（D35）
    records = _events(capsys)
    assert [r["event"] for r in records] == ["recalibrate", "recalibrate"]
    assert records[0]["phase"] == "start"
    assert records[1]["phase"] == "end" and records[1]["result"] == "fail"


def test_write_failure_returns_false(tmp_path, monkeypatch, capsys):
    root = tmp_path / "tables"
    baseline_dir = tables.write_version(root, _make_tables())
    monkeypatch.setattr(fetchbench, "remeasure_path",
                        lambda path, nominal: _new_fit())

    def boom(root_, tables_):
        raise RuntimeError("落盘失败")

    monkeypatch.setattr(tables, "write_version", boom)
    assert recalib.recalibrate_t1_path(root, "dram", "probe", None) is False
    assert [p for p in root.iterdir() if p.is_dir()] == [baseline_dir]
    records = _events(capsys)
    assert records[-1]["result"] == "fail"


def test_t2_recalib_uses_module_lock_for_serialization(tmp_path, monkeypatch):
    root = tmp_path / "tables"
    tables.write_version(root, _make_tables())
    cells = {mode: tables.RecomputeCell(per_token_s_median=9e-6, per_token_s_p25=8e-6)
             for mode in recompbench.RECOMPUTE_MODES}
    monkeypatch.setattr(recompbench, "remeasure_length",
                        lambda engine, length: cells)
    assert recalib.recalibrate_t2_length(root, 4096, "passive", _FakeEngine()) is True
    # 模块级互斥锁存在，两次调用串行执行（R18）
    assert isinstance(recalib._RECALIB_LOCK, type(__import__("threading").Lock()))