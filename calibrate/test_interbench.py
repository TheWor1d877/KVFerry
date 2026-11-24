"""表结构定值与扫描的单测（D28、D31、R19）：正反子表行列与纯计算列、
T4 两键、迟滞五档扫描选档、并发上限拐点。"""

from __future__ import annotations

import json

import config
import interbench
import pytest
import recompbench
import tables


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


def _make_tables(t2=None, t3_forward=None, t3_reverse=None) -> tables.CostTables:
    if t2 is None:
        base = {
            tokens: tables.RecomputeCell(
                per_token_s_median=1.1e-5, per_token_s_p25=1.0e-5
            )
            for tokens in (1024, 4096, 16384, 32768, 65536)
        }
        t2 = {mode: dict(base) for mode in recompbench.RECOMPUTE_MODES}
    return tables.CostTables(
        version="20260101T000000Z",
        provenance=_make_provenance(),
        calibrated_at="2026-01-01T00:00:00Z",
        t1={
            "nvme-direct": tables.PathFit(
                bandwidth_gib_s_median=2.0,
                bandwidth_gib_s_p25=1.0,
                fixed_cost_s_median=0.0,
                fixed_cost_s_p25=0.0,
                min_block_mib=8,
                min_block_s_median=5e-3,
                min_block_s_p25=5e-3,
            )
        },
        t2=t2,
        t3_forward=t3_forward or {},
        t3_reverse=t3_reverse or {},
        t4={},
        tuned={},
    )


def _write_trace(path, n_inputs):
    meta = {
        "prefix_ratio": 0.5,
        "length_tier": "S",
        "load_tier": "low",
        "model_tier": "S",
        "synthetic_ratio": 0.0,
        "n_requests": len(n_inputs),
        "seed": 1,
    }
    lines = [json.dumps(meta)]
    for index, tokens in enumerate(n_inputs):
        lines.append(
            json.dumps(
                {
                    "t_ms": index * 10.0,
                    "n_input": tokens,
                    "session": f"s{index:06d}",
                    "text": "x",
                }
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_build_t3_shape_and_order(monkeypatch):
    calls = []
    cleared = []

    def fake_forward(path, decode_batch, prefill_tokens):
        calls.append(("f", decode_batch, prefill_tokens))
        return 1.0

    def fake_reverse(decode_batch, infight_blocks):
        calls.append(("r", decode_batch, infight_blocks))
        return 0.1

    monkeypatch.setattr(interbench, "measure_forward_discount", fake_forward)
    monkeypatch.setattr(interbench, "measure_reverse_amplification", fake_reverse)
    monkeypatch.setattr(interbench, "_clear_engine_kv_pool", lambda: cleared.append(1))
    forward, reverse = interbench.build_t3("nvme-direct")
    forward_columns = {*interbench.PREFILL_CHUNK_LADDER_TOKENS, 0}
    assert set(forward) == set(interbench.DECODE_BATCH_LADDER)
    for row in forward.values():
        assert set(row) == forward_columns  # 行是 decode 批，列含纯计算列 0（D28）
    assert set(reverse) == set(interbench.INFIGHT_BLOCKS_LADDER)
    for row in reverse.values():
        assert set(row) == set(interbench.DECODE_BATCH_LADDER)
    total_cells = len(interbench.DECODE_BATCH_LADDER) * len(forward_columns) + len(
        interbench.INFIGHT_BLOCKS_LADDER
    ) * len(interbench.DECODE_BATCH_LADDER)
    assert len(cleared) == total_cells  # 每格测前清一次引擎 KV 池
    forward_calls = [c for c in calls if c[0] == "f"]
    reverse_calls = [c for c in calls if c[0] == "r"]
    # 行优先固定顺序：列按 prefill 梯子扫完再收纯计算列
    assert forward_calls[0] == ("f", 1, 512)
    assert forward_calls[4] == ("f", 1, 0)
    assert forward_calls[5] == ("f", 2, 512)
    assert forward_calls[-1] == ("f", 32, 0)
    assert reverse_calls[0] == ("r", 1, 1)
    assert reverse_calls[-1] == ("r", 32, 16)


def test_build_t3_rejects_shape_drift(monkeypatch):
    monkeypatch.setattr(
        interbench, "measure_forward_discount", lambda path, batch, chunk: 1.0
    )
    monkeypatch.setattr(
        interbench, "measure_reverse_amplification", lambda batch, blocks: 0.1
    )
    monkeypatch.setattr(interbench, "_clear_engine_kv_pool", lambda: None)
    monkeypatch.setattr(interbench, "PREFILL_CHUNK_LADDER_TOKENS", (512, 999))
    with pytest.raises(RuntimeError, match="D28"):
        interbench.build_t3("nvme-direct")


def test_decision_overhead_keys(monkeypatch):
    monkeypatch.setattr(interbench, "_LOOKUP_LOOP_N", 200)
    probes = iter((index + 1) * 1e-6 for index in range(100))
    monkeypatch.setattr(interbench, "_probe_rpc_once", lambda endpoint: next(probes))
    result = interbench.measure_decision_overhead(_make_tables(), "http://127.0.0.1:1")
    assert set(result) == {"lookup_s", "rpc_s"}  # T4 两键（D28）
    assert result["lookup_s"] > 0
    # 100 次探测取中位数：递增序列中位为第 51 个值
    assert result["rpc_s"] == pytest.approx(51e-6)


def test_sweep_selects_min_flip_hysteresis(monkeypatch, tmp_path):
    # fetch(tokens) = tokens / 4096 秒（bw_p25=1.0、fixed_p25=0、换算 256 KiB/token）
    monkeypatch.setattr(interbench, "_REPLAY_KV_BYTES_PER_TOKEN", 256 * 1024)
    t2 = {
        1024: tables.RecomputeCell(per_token_s_median=1.1e-5, per_token_s_p25=1.0e-5),
        4096: tables.RecomputeCell(per_token_s_median=1.1e-5, per_token_s_p25=1.0e-5),
        16384: tables.RecomputeCell(
            per_token_s_median=2.6 / 16384, per_token_s_p25=2.6 / 16384
        ),
        32768: tables.RecomputeCell(per_token_s_median=1.1e-5, per_token_s_p25=1.0e-5),
        65536: tables.RecomputeCell(per_token_s_median=1.1e-5, per_token_s_p25=1.0e-5),
    }
    tables_obj = _make_tables(
        t2={mode: dict(t2) for mode in recompbench.RECOMPUTE_MODES},
        t3_forward=_full_grid(),
        t3_reverse=_full_reverse_grid(0.02),
    )
    trace = _write_trace(tmp_path / "trace.jsonl", [4096, 1024, 16384])
    result = interbench.sweep_tuned(trace, tables_obj)
    # 请求 A 翻向重算、请求 B 翻回取回；请求 C 两笔账比 0.65：
    # 迟滞 0.6 沿用上次决策（1 次翻转），其余档翻向重算（2 次翻转）
    assert result["hysteresis"] == 0.6


def test_sweep_tie_breaks_to_first_hysteresis(monkeypatch, tmp_path):
    monkeypatch.setattr(interbench, "_REPLAY_KV_BYTES_PER_TOKEN", 256 * 1024)
    tables_obj = _make_tables(
        t3_forward=_full_grid(), t3_reverse=_full_reverse_grid(0.02)
    )
    trace = _write_trace(tmp_path / "trace.jsonl", [4096, 1024])
    result = interbench.sweep_tuned(trace, tables_obj)
    # 两笔账比约 0.04，五档全部落在带外，翻转与首 token 时间并列时取首档
    assert result["hysteresis"] == 0.6
    assert result["gate_pct"] == interbench.GATE_PCT


def _full_grid(value_by_batch=None, value=1.0):
    batches = interbench.DECODE_BATCH_LADDER
    columns = (*interbench.PREFILL_CHUNK_LADDER_TOKENS, 0)
    forward = {}
    for batch in batches:
        row_value = value
        if value_by_batch is not None:
            row_value = value_by_batch.get(batch, value)
        forward[batch] = {chunk: row_value for chunk in columns}
    return forward


def _full_reverse_grid(value):
    return {
        blocks: {batch: value for batch in interbench.DECODE_BATCH_LADDER}
        for blocks in interbench.INFIGHT_BLOCKS_LADDER
    }


def test_concurrency_cap_limited_by_forward_grid(monkeypatch, tmp_path):
    # decode 批 16 与 32 下带宽折扣超出空载 90% 保底，正向拐点落在 decode 批 8；
    # 反向各行增幅都在闸门内，反向拐点为在飞块数 16，取较小值（D31、R19）
    forward = _full_grid(value_by_batch={16: 1.2, 32: 1.2})
    reverse = {
        blocks: {batch: 0.02 for batch in interbench.DECODE_BATCH_LADDER}
        for blocks in interbench.INFIGHT_BLOCKS_LADDER
    }
    tables_obj = _make_tables(t3_forward=forward, t3_reverse=reverse)
    trace = _write_trace(tmp_path / "trace.jsonl", [4096, 1024])
    result = interbench.sweep_tuned(trace, tables_obj)
    assert result["concurrency_cap"] == 8.0


def test_concurrency_cap_limited_by_reverse_grid(monkeypatch, tmp_path):
    # 正向折扣全部保住 90% 带宽，正向拐点为 decode 批 32；
    # 在飞块数 8 起增幅超过 5% 闸门，反向拐点为在飞块数 4，取较小值（D31）
    forward = _full_grid()
    reverse = {}
    for blocks in interbench.INFIGHT_BLOCKS_LADDER:
        value = 0.03 if blocks <= 4 else 0.08
        reverse[blocks] = {batch: value for batch in interbench.DECODE_BATCH_LADDER}
    tables_obj = _make_tables(t3_forward=forward, t3_reverse=reverse)
    trace = _write_trace(tmp_path / "trace.jsonl", [4096, 1024])
    result = interbench.sweep_tuned(trace, tables_obj)
    assert result["concurrency_cap"] == 4.0


def test_sweep_rejects_empty_trace(tmp_path):
    trace = _write_trace(tmp_path / "trace.jsonl", [])
    with pytest.raises(RuntimeError):
        interbench.sweep_tuned(trace, _make_tables())


def test_sweep_rejects_request_count_mismatch(tmp_path):
    trace = _write_trace(tmp_path / "trace.jsonl", [4096, 1024])
    lines = trace.read_text(encoding="utf-8").splitlines()
    lines[0] = json.dumps({**json.loads(lines[0]), "n_requests": 3})
    trace.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError):
        interbench.sweep_tuned(trace, _make_tables())


def test_reverse_amplification_requires_engine(monkeypatch):
    monkeypatch.setattr(interbench, "_engine", None)
    with pytest.raises(RuntimeError):
        interbench.measure_reverse_amplification(1, 1)
