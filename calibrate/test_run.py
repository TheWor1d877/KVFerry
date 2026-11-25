"""命令退出码与落盘的单测（D31）：任一链路失败退出码 1 且不落目录、三重
检查未过退出码 1、全部通过退出码 0 且五文件与 manifest 齐全——全部经注入
假客户端与假测量实现，不触真实引擎与硬件；D37 推算、D38 探测请求形态与
D39 写列装配一并覆盖。"""

from __future__ import annotations

import json
import urllib.request

import config
import fetchbench
import interbench
import recompbench
import run
import tables
import writebench

_WRITE_FIT = tables.PathFit(
    bandwidth_gib_s_median=3.0,
    bandwidth_gib_s_p25=2.8,
    fixed_cost_s_median=0.0,
    fixed_cost_s_p25=0.0,
    min_block_mib=8,
    min_block_s_median=3e-3,
    min_block_s_p25=3.2e-3,
)


def _prov() -> config.Provenance:
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


def _patch_chain(monkeypatch, fail: str | None = None) -> config.Provenance:
    """注入假口径采集与假标定链路：T1/T2/T3/T4 不触硬件与网络。"""
    provenance = _prov()
    monkeypatch.setattr(config, "collect_provenance", lambda *a, **k: provenance)

    def build_t1(nominal=None):
        if fail == "t1":
            raise RuntimeError("路径 dram 三重检查未过，加密 2 轮后仍有违规（D31）")
        fit = tables.PathFit(
            bandwidth_gib_s_median=2.0,
            bandwidth_gib_s_p25=1.0,
            fixed_cost_s_median=0.0,
            fixed_cost_s_p25=0.0,
            min_block_mib=8,
            min_block_s_median=5e-3,
            min_block_s_p25=5e-3,
        )
        return {"nvme-direct": fit, "dram": fit}

    monkeypatch.setattr(fetchbench, "build_t1", build_t1)
    # D39：写列标定经注入的假拟合替换，不触真实写盘
    monkeypatch.setattr(writebench, "build_write_fit", lambda nominal=None: _WRITE_FIT)

    def build_t2(engine):
        if fail == "t2":
            raise RuntimeError("引擎连接失败")
        cells = {
            tokens: tables.RecomputeCell(per_token_s_median=1e-5, per_token_s_p25=1e-5)
            for tokens in (
                *recompbench.LENGTH_LADDER_TOKENS,
                recompbench.DIRECT_LENGTH_TOKENS,
            )
        }
        return {mode: dict(cells) for mode in recompbench.RECOMPUTE_MODES}

    monkeypatch.setattr(recompbench, "build_t2", build_t2)

    def build_t3(path):
        if fail == "t3":
            raise RuntimeError("干扰对测失败")
        columns = (*interbench.PREFILL_CHUNK_LADDER_TOKENS, 0)
        forward = {
            batch: {chunk: 1.0 for chunk in columns}
            for batch in interbench.DECODE_BATCH_LADDER
        }
        reverse = {
            blocks: {batch: 0.02 for batch in interbench.DECODE_BATCH_LADDER}
            for blocks in interbench.INFIGHT_BLOCKS_LADDER
        }
        return forward, reverse

    monkeypatch.setattr(interbench, "build_t3", build_t3)
    monkeypatch.setattr(
        interbench,
        "measure_decision_overhead",
        lambda tables_, endpoint: {"lookup_s": 1e-6, "rpc_s": 2e-4},
    )
    return provenance


def _write_trace(path) -> None:
    meta = {
        "prefix_ratio": 0.5,
        "length_tier": "S",
        "load_tier": "low",
        "model_tier": "S",
        "synthetic_ratio": 0.0,
        "n_requests": 2,
        "seed": 1,
    }
    lines = [
        json.dumps(meta),
        json.dumps({"t_ms": 0.0, "n_input": 4096, "session": "s000000", "text": "x"}),
        json.dumps({"t_ms": 10.0, "n_input": 1024, "session": "s000001", "text": "x"}),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _make_args(root, trace, model_path=None, kv_bytes=131072):
    argv = [
        "--out",
        str(root),
        "--engine",
        "http://127.0.0.1:9",
        "--fetch-endpoint",
        "http://127.0.0.1:9",
        "--trace",
        str(trace),
    ]
    if model_path is not None:
        argv += ["--model-path", str(model_path)]
    if kv_bytes is not None:
        argv += ["--kv-bytes-per-token", str(kv_bytes)]
    return argv


def test_full_run_exit_0_writes_version_dir(tmp_path, monkeypatch):
    provenance = _patch_chain(monkeypatch)
    # 主链路会改 interbench 模块常量与探测函数，测试后恢复
    monkeypatch.setattr(
        interbench, "_REPLAY_KV_BYTES_PER_TOKEN", interbench._REPLAY_KV_BYTES_PER_TOKEN
    )
    monkeypatch.setattr(interbench, "_probe_rpc_once", interbench._probe_rpc_once)
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    code = run.main(_make_args(root, trace))
    assert code == 0
    dirs = [p for p in root.iterdir() if p.is_dir()]
    assert len(dirs) == 1
    final = dirs[0]
    assert sorted(p.name for p in final.iterdir()) == sorted(
        (*tables.TABLE_FILES, tables.MANIFEST_FILE)
    )
    tables.verify_dir(final, final.name)
    loaded = tables.CostTables.load(root, final.name)
    assert loaded.t4 == {"lookup_s": 1e-6, "rpc_s": 2e-4}
    # 写列装配进 t1_write，键为数据目录顺序写行（D39）
    assert loaded.t1_write == {"nvme-write": _WRITE_FIT}
    assert set(loaded.tuned) == {"hysteresis", "gate_pct", "concurrency_cap"}
    assert loaded.provenance == provenance
    # 装配生效：rpc_s 探测函数替换为 D38 形态，重放 KV 字节按 D37 取值
    assert interbench._probe_rpc_once is run._probe_rpc_d38
    assert interbench._REPLAY_KV_BYTES_PER_TOKEN == 131072.0


def test_chain_failure_exit_1_no_dir(tmp_path, monkeypatch):
    _patch_chain(monkeypatch, fail="t2")
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    assert run.main(_make_args(root, trace)) == 1
    # 任一链路失败：退出码 1 且不落版本目录（D31）
    assert not root.exists() or not list(root.iterdir())


def test_checkfit_failure_exit_1_no_dir(tmp_path, monkeypatch):
    _patch_chain(monkeypatch, fail="t1")
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    assert run.main(_make_args(root, trace)) == 1
    assert not root.exists() or not list(root.iterdir())


def test_kv_bytes_explicit_passthrough(tmp_path, monkeypatch):
    _patch_chain(monkeypatch)
    monkeypatch.setattr(
        interbench, "_REPLAY_KV_BYTES_PER_TOKEN", interbench._REPLAY_KV_BYTES_PER_TOKEN
    )
    monkeypatch.setattr(interbench, "_probe_rpc_once", interbench._probe_rpc_once)
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    # 显式给出 --kv-bytes-per-token 时直接使用不推算（D37）
    assert run.main(_make_args(root, trace, kv_bytes=4096)) == 0
    assert interbench._REPLAY_KV_BYTES_PER_TOKEN == 4096.0


def test_kv_bytes_inferred_from_config(tmp_path, monkeypatch):
    _patch_chain(monkeypatch)
    monkeypatch.setattr(
        interbench, "_REPLAY_KV_BYTES_PER_TOKEN", interbench._REPLAY_KV_BYTES_PER_TOKEN
    )
    monkeypatch.setattr(interbench, "_probe_rpc_once", interbench._probe_rpc_once)
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "config.json").write_text(
        json.dumps(
            {
                "num_hidden_layers": 32,
                "num_key_value_heads": 8,
                "head_dim": 128,
                "torch_dtype": "bfloat16",
            }
        ),
        encoding="utf-8",
    )
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    argv = _make_args(root, trace, model_path=model_dir, kv_bytes=None)
    assert run.main(argv) == 0
    # 每 token 字节数 = 2 × 层数 × KV 头数 × 每头维度 × 精度字节（D37）
    assert interbench._REPLAY_KV_BYTES_PER_TOKEN == 2 * 32 * 8 * 128 * 2


def test_kv_bytes_head_dim_fallback_and_fp8(tmp_path):
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "config.json").write_text(
        json.dumps(
            {
                "num_hidden_layers": 8,
                "num_key_value_heads": 8,
                "num_attention_heads": 32,
                "hidden_size": 4096,
                "torch_dtype": "fp8",
            }
        ),
        encoding="utf-8",
    )
    # 无 head_dim 按 hidden_size ÷ 注意力头数；fp8 每元素 1 字节（D37）
    assert run._infer_kv_bytes_per_token(str(model_dir)) == 2 * 8 * 8 * 128 * 1


def test_kv_bytes_mla_rejected_exit_1(tmp_path, monkeypatch):
    _patch_chain(monkeypatch)
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "config.json").write_text(
        json.dumps({"num_hidden_layers": 32, "kv_lora_rank": 512}), encoding="utf-8"
    )
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    argv = _make_args(root, trace, model_path=model_dir, kv_bytes=None)
    # MLA 压缩形态推算不适用且未显式指定：报错退出码 1（D37）
    assert run.main(argv) == 1
    assert not root.exists() or not list(root.iterdir())


def test_kv_bytes_missing_model_path_exit_1(tmp_path, monkeypatch):
    _patch_chain(monkeypatch)
    root = tmp_path / "tables"
    trace = tmp_path / "trace.jsonl"
    _write_trace(trace)
    argv = _make_args(root, trace, model_path=None, kv_bytes=None)
    assert run.main(argv) == 1


def test_rpc_probe_d38_request_shape(monkeypatch):
    captured: dict = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    elapsed = run._probe_rpc_d38("http://127.0.0.1:9/")
    assert captured["method"] == "POST"
    assert captured["url"] == "http://127.0.0.1:9" + run._RPC_PROBE_PATH
    # 请求体含单块占位查询字段（D38）
    assert captured["body"]["block_mib"] == run._RPC_PROBE_BLOCK_MIB
    assert captured["body"]["query"] == "probe"
    assert elapsed >= 0
