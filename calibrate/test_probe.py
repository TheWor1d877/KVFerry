"""探针越阈计数与判定的单测（D33）：越阈计数、不越阈清零、连续三窗判定
过期、过期态跳过比对直接重试、成功后重载表。"""

from __future__ import annotations

import json

import pytest

import config
import fetchbench
import probe
import recompbench
import recalib
import tables

_PATH = "dram"
_OK_S = 0.03125      # 64 MiB / 2 GiB/s：与表值一致，不越阈
_OVER_S = 0.0390625  # 偏离表值 25%，超过默认阈值 20%


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
        t3_forward={}, t3_reverse={}, t4={}, tuned={})


def _make_cfg(tmp_path, **overrides) -> config.RuntimeConfig:
    values = dict(probe_interval_s=30, probe_block_mib=64, drift_threshold_pct=20.0,
                  drift_trigger_consecutive=3, drift_sample_min=100,
                  drift_window_min_s=60, drift_window_max_s=1800)
    values.update(overrides)
    return config.RuntimeConfig(table_root=tmp_path / "tables",
                                declared_version="unknown", **values)


def _patch_measure(monkeypatch, elapsed_s, calls):
    seq = iter(elapsed_s) if isinstance(elapsed_s, list) else None

    def fake_measure(path, block_mib, reps=fetchbench.MEASURE_REPS):
        calls.append(path)
        value = next(seq) if seq is not None else elapsed_s
        return fetchbench.BlockSample(path=path, block_mib=block_mib,
                                      elapsed_s=(value,) * reps)

    monkeypatch.setattr(fetchbench, "measure_block", fake_measure)


def _make_loop(tmp_path, monkeypatch, elapsed_s, measure_calls,
               recalib_results=None, recalib_calls=None, cfg_overrides=None):
    """落基线表、注入假测量与假重标，返回探针循环与基线目录。"""
    root = tmp_path / "tables"
    baseline = tables.write_version(root, _make_tables())
    _patch_measure(monkeypatch, elapsed_s, measure_calls)

    def fake_recalib(root_, path_, trigger, nominal):
        recalib_calls.append((path_, trigger, nominal))
        return recalib_results.pop(0) if recalib_results else True

    monkeypatch.setattr(recalib, "recalibrate_t1_path", fake_recalib)
    loop = probe.ProbeLoop(_make_cfg(tmp_path, **(cfg_overrides or {})))
    return loop, root, baseline


def _events(capsys):
    captured = capsys.readouterr()
    return [json.loads(line) for line in captured.out.splitlines()]


def test_three_over_threshold_windows_expire_path(tmp_path, monkeypatch, capsys):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    loop, root, baseline = _make_loop(tmp_path, monkeypatch, _OVER_S,
                                      measure_calls, [], recalib_calls)
    loop._check_path(_PATH)
    loop._check_path(_PATH)
    assert loop._consecutive[_PATH] == 2  # 越阈计数累加（D33）
    assert recalib_calls == []
    loop._check_path(_PATH)
    # 连续三窗越阈判定过期：同步重标被触发（D33）
    assert recalib_calls == [(_PATH, "probe", None)]
    records = _events(capsys)
    expire = [r for r in records if r["event"] == "expire"]
    degrade = [r for r in records if r["event"] == "degrade"]
    assert len(expire) == 1
    assert expire[0]["source"] == "probe"
    assert expire[0]["table"] == "t1"
    assert expire[0]["target"] == _PATH
    assert expire[0]["expected"] == pytest.approx(_OK_S)
    assert expire[0]["observed"] == pytest.approx(_OVER_S)
    assert expire[0]["deviation_pct"] == pytest.approx(25.0)
    assert len(degrade) == 1
    assert degrade[0]["phase"] == "start"
    assert degrade[0]["source"] == "probe"
    assert degrade[0]["table"] == "t1"
    assert degrade[0]["target"] == _PATH


def test_clear_window_resets_counter(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    sequence = [_OVER_S, _OVER_S, _OK_S, _OVER_S, _OVER_S]
    loop, _, _ = _make_loop(tmp_path, monkeypatch, sequence,
                            measure_calls, [], recalib_calls)
    for _ in range(5):
        loop._check_path(_PATH)
    # 第三窗不越阈把连续计数清零，后续两窗只攒回 2（D33）
    assert loop._consecutive[_PATH] == 2
    assert recalib_calls == []


def test_expired_state_skips_probe_and_retries(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    loop, _, _ = _make_loop(tmp_path, monkeypatch, _OVER_S, measure_calls,
                            [False, False, True], recalib_calls)
    for _ in range(3):
        loop._check_path(_PATH)  # 连续三窗越阈 → 过期，重标第一次失败
    assert len(measure_calls) == 3
    assert len(recalib_calls) == 1
    loop._check_path(_PATH)  # 过期态跳过常规比对直接重试
    loop._check_path(_PATH)  # 第三次重标成功
    assert len(measure_calls) == 3  # 过期态不再做探针测量
    assert len(recalib_calls) == 3
    assert _PATH not in loop._expired  # 重标成功解除过期态
    loop._check_path(_PATH)  # 恢复常规比对
    assert len(measure_calls) == 4


def test_tables_reload_after_successful_recalib(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    root = tmp_path / "tables"
    baseline = tables.write_version(root, _make_tables())
    _patch_measure(monkeypatch, _OVER_S, measure_calls)

    def fake_recalib(root_, path_, trigger, nominal):
        recalib_calls.append(path_)
        loaded = tables.CostTables.load(root_, "unknown")
        loaded.t1[_PATH] = _make_fit(bandwidth=3.0)
        tables.write_version(root_, loaded)
        return True

    monkeypatch.setattr(recalib, "recalibrate_t1_path", fake_recalib)
    loop = probe.ProbeLoop(_make_cfg(tmp_path))
    for _ in range(3):
        loop._check_path(_PATH)
    # 成功后重新加载表（D33）：新表带重标后的拟合参数与新版本号
    assert loop._tables.t1[_PATH].bandwidth_gib_s_median == 3.0
    assert loop._tables.version != baseline.name
    assert _PATH not in loop._expired


def test_probe_once_expected_uses_median_formula(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    loop, _, _ = _make_loop(tmp_path, monkeypatch, _OK_S, measure_calls)
    assert loop.probe_once(_PATH, loop._tables) is False
    expected, observed, deviation_pct = loop._last[_PATH]
    # 表值 = probe_block_mib 字节 ÷ median 带宽 + median 固定开销（D33、R16）
    assert expected == pytest.approx(_OK_S)
    assert observed == pytest.approx(_OK_S)
    assert deviation_pct == pytest.approx(0.0)


def test_probe_once_scales_min_block_below_ladder(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    loop, _, _ = _make_loop(tmp_path, monkeypatch, 3.125e-4, measure_calls,
                            [], recalib_calls, cfg_overrides={"probe_block_mib": 4})
    # 4 MiB 小于最小档 8 MiB：表值按最小档直测耗时折算（D33）
    assert loop.probe_once(_PATH, loop._tables) is True
    expected, observed, deviation_pct = loop._last[_PATH]
    assert expected == pytest.approx(5e-4 * (4 / 8))
    assert deviation_pct == pytest.approx(25.0)


def test_run_ticks_each_path_per_interval(tmp_path, monkeypatch):
    measure_calls: list[str] = []
    recalib_calls: list[tuple] = []
    loop, _, _ = _make_loop(tmp_path, monkeypatch, _OK_S, measure_calls,
                            [], recalib_calls)
    ticks: list[str] = []
    monkeypatch.setattr(loop, "_check_path", lambda path: ticks.append(path))
    sleeps: list[float] = []
    interval = loop._cfg.probe_interval_s

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise StopIteration

    monkeypatch.setattr(loop, "_sleep", fake_sleep)
    with pytest.raises(StopIteration):
        loop.run()
    # 每 probe_interval_s 对每条路径执行一轮 probe_once（D33）
    assert sleeps == [interval, interval]
    assert len(ticks) == len(fetchbench.PATH_BLOCK_LADDERS) * 2