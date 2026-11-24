"""被动窗口聚合与判定的单测（D34）：在飞块数非零样本丢弃、就近映射档位、
开满下限且凑样聚合、到上限不足样本不产出不清零不推进、连续三窗触发重标。"""

from __future__ import annotations

import json
import threading
import time

import pytest

import config
import recompbench
import recalib
import tables
import passive


class _Clock:
    """注入时钟：测试内显式推进，避免真实等待窗口时长。"""

    def __init__(self, start: float = 0.0):
        self.now = start

    def tick(self, delta: float) -> None:
        self.now += delta

    def __call__(self) -> float:
        return self.now


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


def _make_tables() -> tables.CostTables:
    cells = {tokens: tables.RecomputeCell(per_token_s_median=1e-5, per_token_s_p25=1e-5)
             for tokens in (1024, 4096, 16384, 32768, 65536)}
    return tables.CostTables(
        version="", provenance=_prov(), calibrated_at="2026-01-01T00:00:00Z",
        t1={}, t2={mode: dict(cells) for mode in recompbench.RECOMPUTE_MODES},
        t3_forward={}, t3_reverse={}, t4={}, tuned={})


def _make_cfg(tmp_path, **overrides) -> config.RuntimeConfig:
    values = dict(probe_interval_s=30, probe_block_mib=64, drift_threshold_pct=20.0,
                  drift_trigger_consecutive=3, drift_sample_min=2,
                  drift_window_min_s=60, drift_window_max_s=1800)
    values.update(overrides)
    return config.RuntimeConfig(table_root=tmp_path / "tables",
                                declared_version="unknown", **values)


def _make_agg(tmp_path, monkeypatch, clock, **overrides):
    root = tmp_path / "tables"
    baseline = tables.write_version(root, _make_tables())
    monkeypatch.setattr(passive, "_now", clock)
    cfg = _make_cfg(tmp_path, **overrides)
    agg = passive.PassiveAggregator(cfg, tables.CostTables.load(root, "unknown"),
                                    _FakeEngine())
    return agg, root, baseline


def _sample(tokens: int, per_token: float = 1e-5,
            infight: int = 0) -> passive.PrefillSample:
    return passive.PrefillSample(computed_tokens=tokens,
                                 segment_s=tokens * per_token,
                                 infight_blocks=infight)


def _wait(condition, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while not condition():
        if time.time() > deadline:
            raise AssertionError("等待超时")
        time.sleep(0.01)


def _events(capsys):
    captured = capsys.readouterr()
    return [json.loads(line) for line in captured.out.splitlines()]


def test_infight_samples_dropped(tmp_path, monkeypatch):
    clock = _Clock()
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    agg.record(_sample(1024, infight=1))
    agg.record(_sample(4096, infight=3))
    # 在飞块数非零的样本不计入任何窗口（D34 准入）
    assert agg._windows == {}


def test_length_mapping_nearest_rung(tmp_path, monkeypatch):
    clock = _Clock()
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    assert agg._map_length(1024) == 1024
    assert agg._map_length(2048) == 1024   # 2048 离 1024 更近
    assert agg._map_length(2560) == 4096   # 两档等距取更高档（D34）
    assert agg._map_length(3000) == 4096
    assert agg._map_length(70000) == 65536  # 超过 64k 映射到直测档
    assert agg._map_length(100) == 1024     # 低于最小档取最小档


def test_window_aggregates_after_min_duration_and_samples(tmp_path, monkeypatch):
    clock = _Clock()
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    agg.record(_sample(4096, per_token=1.25e-5))  # t=0 开窗，样本 1
    agg.record(_sample(4096, per_token=1.25e-5))  # 样本 2，时长 0 < 下限 60s
    assert 4096 in agg._windows  # 未开满下限不聚合
    clock.tick(70)
    agg.record(_sample(4096, per_token=1.25e-5))  # 开满 60s 且样本凑齐 → 聚合
    # 聚合值 1.25e-5 对表值 1e-5 偏离 25% > 20%：越阈计数 1（D34）
    assert agg._consecutive[4096] == 1
    assert 4096 not in agg._windows  # 关窗后样本不滚入下一窗口
    agg.record(_sample(4096))  # t=70 开新窗
    agg.record(_sample(4096))
    clock.tick(70)
    agg.record(_sample(4096))  # 聚合值等于表值：不越阈清零
    assert agg._consecutive[4096] == 0
    assert agg._thread is None


def test_window_max_discards_pool_without_progress(tmp_path, monkeypatch, capsys):
    clock = _Clock()
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock, drift_sample_min=3)
    # 先攒一个越阈窗，计数 1
    for _ in range(3):
        agg.record(_sample(4096, per_token=1.25e-5))
    clock.tick(70)
    agg.record(_sample(4096, per_token=1.25e-5))
    assert agg._consecutive[4096] == 1
    capsys.readouterr()
    # 不足样本窗：1 个样本跨过窗口上限，再来 1 个仍不足（样本下限 3）
    agg.record(_sample(4096, per_token=1.25e-5))
    clock.tick(1900)
    agg.record(_sample(4096, per_token=1.25e-5))
    # 到上限不足样本：不产出比对、整池丢弃、计数不清零也不推进（D34）
    assert 4096 not in agg._windows
    assert agg._consecutive[4096] == 1
    assert _events(capsys) == []
    # 随后重开新窗口从零计样本
    agg.record(_sample(4096))
    assert agg._windows[4096].start_s == pytest.approx(clock.now)
    assert len(agg._windows[4096].samples) == 1


def test_three_consecutive_windows_trigger_recalib(tmp_path, monkeypatch, capsys):
    clock = _Clock()
    calls: list[tuple[int, str]] = []

    def fake_recalib(root_, length, trigger, engine):
        calls.append((length, trigger))
        return True

    monkeypatch.setattr(recalib, "recalibrate_t2_length", fake_recalib)
    agg, root, baseline = _make_agg(tmp_path, monkeypatch, clock)
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(4096, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(4096, per_token=1.25e-5))  # 第三窗聚合后判过期
    # 连续三窗越阈判定该长度档过期并起后台重标（D34）
    assert calls == [(4096, "passive")]
    _wait(lambda: agg._thread is None)
    records = _events(capsys)
    expire = [r for r in records if r["event"] == "expire"]
    degrade = [r for r in records if r["event"] == "degrade"]
    assert len(expire) == 1
    assert expire[0]["source"] == "passive"
    assert expire[0]["table"] == "t2"
    assert expire[0]["target"] == "4096"
    assert expire[0]["expected"] == pytest.approx(1e-5)
    assert expire[0]["observed"] == pytest.approx(1.25e-5)
    assert expire[0]["deviation_pct"] == pytest.approx(25.0)
    assert len(degrade) == 1
    assert degrade[0]["phase"] == "start"


def test_same_rung_recalib_deduped_while_running(tmp_path, monkeypatch, capsys):
    clock = _Clock()
    release = threading.Event()
    calls: list[int] = []

    def blocking_recalib(root_, length, trigger, engine):
        calls.append(length)
        release.wait(timeout=5)
        return True

    monkeypatch.setattr(recalib, "recalibrate_t2_length", blocking_recalib)
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(4096, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(4096, per_token=1.25e-5))
    _wait(lambda: len(calls) == 1)  # 重标线程在跑且阻塞
    capsys.readouterr()
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(4096, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(4096, per_token=1.25e-5))  # 同档再次判过期
    # 同档重标线程在跑：后续同档过期判定只打事件不起线程（D34）
    assert calls == [4096]
    assert agg._pending == []
    records = _events(capsys)
    expire = [r for r in records if r["event"] == "expire"]
    degrade = [r for r in records if r["event"] == "degrade"]
    assert len(expire) == 3  # 三个越阈窗各是一次过期判定，都打事件
    assert len(degrade) == 3
    assert all(r["phase"] == "start" for r in degrade)
    release.set()
    _wait(lambda: agg._thread is None)


def test_cross_rung_recalib_serialized(tmp_path, monkeypatch):
    clock = _Clock()
    release = threading.Event()
    calls: list[int] = []

    def blocking_recalib(root_, length, trigger, engine):
        calls.append(length)
        release.wait(timeout=5)
        return True

    monkeypatch.setattr(recalib, "recalibrate_t2_length", blocking_recalib)
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(4096, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(4096, per_token=1.25e-5))
    _wait(lambda: len(calls) == 1)
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(1024, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(1024, per_token=1.25e-5))  # 跨档过期 → 排队
    assert agg._pending == [1024]
    release.set()
    _wait(lambda: agg._thread is None)
    # 进程内同一时刻至多一个重标线程：两档在单线程内先后串行（D34）
    assert calls == [4096, 1024]
    assert agg._pending == []


def test_close_drops_open_windows_and_stops_thread(tmp_path, monkeypatch):
    clock = _Clock()
    release = threading.Event()
    calls: list[int] = []

    def blocking_recalib(root_, length, trigger, engine):
        calls.append(length)
        release.wait(timeout=5)
        return True

    monkeypatch.setattr(recalib, "recalibrate_t2_length", blocking_recalib)
    agg, _, _ = _make_agg(tmp_path, monkeypatch, clock)
    for _ in range(3):
        for _ in range(2):
            agg.record(_sample(4096, per_token=1.25e-5))
        clock.tick(70)
        agg.record(_sample(4096, per_token=1.25e-5))
    _wait(lambda: len(calls) == 1)
    agg.record(_sample(1024))  # 未关窗样本池
    release.set()
    agg.close()
    # close 停掉后台线程并丢弃未关窗样本池（D34）
    assert agg._windows == {}
    assert agg._thread is None