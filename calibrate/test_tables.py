"""表落盘与加载校验的单测（D26、D29、D30）。"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import pytest

import config
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


def _make_tables(version: str = "20260101T000000Z") -> tables.CostTables:
    return tables.CostTables(
        version=version,
        provenance=_make_provenance(),
        calibrated_at="2026-01-01T00:00:00Z",
        t1={"dram": tables.PathFit(
            bandwidth_gib_s_median=20.0,
            bandwidth_gib_s_p25=19.0,
            fixed_cost_s_median=1e-4,
            fixed_cost_s_p25=1.1e-4,
            min_block_mib=8,
            min_block_s_median=5e-4,
            min_block_s_p25=5.5e-4,
        )},
        t2={"mixed": {
            1024: tables.RecomputeCell(per_token_s_median=1e-5, per_token_s_p25=1.1e-5),
            4096: tables.RecomputeCell(per_token_s_median=2e-5, per_token_s_p25=2.1e-5),
            65536: tables.RecomputeCell(per_token_s_median=3e-5, per_token_s_p25=3.1e-5),
        }},
        t3_forward={1: {0: 1.0, 512: 0.9}},
        t3_reverse={1: {1: 0.1}},
        t4={"lookup_s": 1e-6, "rpc_s": 1e-4},
        tuned={"hysteresis": 0.8, "gate_pct": 5.0, "concurrency_cap": 4.0},
    )


_TOP_LEVEL_KEYS = {
    "version",
    "calibrated_at",
    "gpu_name",
    "nic",
    "pcie_topology",
    "driver_version",
    "engine_version",
    "model",
    "kv_precision",
    "block_tokens",
}


def test_write_version_layout(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    assert re.fullmatch(r"\d{8}T\d{6}Z", final.name)
    assert sorted(p.name for p in final.iterdir()) == sorted(
        (*tables.TABLE_FILES, tables.MANIFEST_FILE))
    for name in tables.TABLE_FILES:
        payload = json.loads((final / name).read_text(encoding="utf-8"))
        assert _TOP_LEVEL_KEYS <= set(payload)
        assert payload["version"] == final.name
        assert payload["calibrated_at"] == "2026-01-01T00:00:00Z"
    manifest = json.loads((final / tables.MANIFEST_FILE).read_text(encoding="utf-8"))
    assert manifest["version"] == final.name
    assert set(manifest["files"]) == set(tables.TABLE_FILES)


def test_load_roundtrip(tmp_path):
    root = tmp_path / "tables"
    source = _make_tables()
    final = tables.write_version(root, source)
    loaded = tables.CostTables.load(root, final.name)
    assert loaded.version == final.name
    assert loaded.provenance == _make_provenance()
    assert loaded.calibrated_at == "2026-01-01T00:00:00Z"
    assert loaded.t1["dram"] == source.t1["dram"]
    assert loaded.t2["mixed"][4096] == source.t2["mixed"][4096]
    assert loaded.t3_forward == source.t3_forward
    assert loaded.t3_reverse == source.t3_reverse
    assert loaded.t4 == source.t4
    assert loaded.tuned == source.tuned


def test_load_accepts_unknown_declaration(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    loaded = tables.CostTables.load(root, "unknown")
    assert loaded.version == final.name


def test_manifest_hash_reject(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    tampered = json.loads((final / tables.T4_FILE).read_text(encoding="utf-8"))
    tampered["t4"]["rpc_s"] = 9.9
    (final / tables.T4_FILE).write_text(
        json.dumps(tampered, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RuntimeError):
        tables.CostTables.load(root, final.name)
    with pytest.raises(RuntimeError):
        tables.verify_dir(final, final.name)


def test_manifest_dir_version_reject(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    manifest_path = final / tables.MANIFEST_FILE
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["version"] = "19991231T235959Z"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RuntimeError):
        tables.CostTables.load(root, final.name)
    with pytest.raises(RuntimeError):
        tables.verify_dir(final, final.name)


def test_declared_version_mismatch_rejected(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    tampered = json.loads((final / tables.T2_FILE).read_text(encoding="utf-8"))
    tampered["version"] = "20250101T000000Z"
    (final / tables.T2_FILE).write_text(
        json.dumps(tampered, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RuntimeError):
        tables.CostTables.load(root, final.name)
    with pytest.raises(RuntimeError):
        tables.CostTables.load(root, "unknown")


def test_verify_dir_accepts_valid(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    assert tables.verify_dir(final, final.name) is None
    assert tables.verify_dir(final, "unknown") is None


def test_verify_dir_rejects_bad_name_and_declaration(tmp_path):
    root = tmp_path / "tables"
    final = tables.write_version(root, _make_tables())
    bad = root / "not-a-version"
    bad.mkdir()
    with pytest.raises(RuntimeError):
        tables.verify_dir(bad, "unknown")
    with pytest.raises(RuntimeError):
        tables.verify_dir(final, "20250101T000000Z")


def test_write_version_collision_retries(tmp_path, monkeypatch):
    root = tmp_path / "tables"
    root.mkdir()
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    moments = [base, base.replace(second=1)]
    monkeypatch.setattr(tables, "_now_utc", lambda: moments.pop(0))
    sleeps: list[float] = []
    monkeypatch.setattr(tables.time, "sleep", lambda s: sleeps.append(s))
    (root / tables._format_timestamp(base)).mkdir()
    final = tables.write_version(root, _make_tables())
    assert final.name == tables._format_timestamp(base.replace(second=1))
    assert sleeps == [1]
    assert not any(p.name.startswith(".") for p in root.iterdir())


def test_write_version_collision_exhausted(tmp_path, monkeypatch):
    root = tmp_path / "tables"
    root.mkdir()
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    moments = [base.replace(second=s) for s in range(4)]
    monkeypatch.setattr(tables, "_now_utc", lambda: moments.pop(0))
    monkeypatch.setattr(tables.time, "sleep", lambda s: None)
    for s in range(4):
        (root / tables._format_timestamp(base.replace(second=s))).mkdir()
    with pytest.raises(RuntimeError):
        tables.write_version(root, _make_tables())
    assert not any(p.name.startswith(".") for p in root.iterdir())


def test_write_failure_leaves_no_staging(tmp_path, monkeypatch):
    root = tmp_path / "tables"
    original_write = tables._write_file

    def flaky(path, text):
        if path.name == tables.T2_FILE:
            raise OSError("disk full")
        return original_write(path, text)

    monkeypatch.setattr(tables, "_write_file", flaky)
    with pytest.raises(OSError):
        tables.write_version(root, _make_tables())
    assert list(root.iterdir()) == []


def test_lookup_fetch_uses_p25_and_min_block():
    source = _make_tables()
    gib = 1024 ** 3
    mib = 1024 ** 2
    assert source.lookup_fetch("dram", 64 * mib) == pytest.approx(
        64 * mib / (19.0 * gib) + 1.1e-4)
    assert source.lookup_fetch("dram", 4 * mib) == pytest.approx(5.5e-4 * 0.5)
    with pytest.raises(RuntimeError):
        source.lookup_fetch("nope", 1024)


def test_lookup_recompute_p25_ladder():
    source = _make_tables()
    per = 1.1e-5 + (2.1e-5 - 1.1e-5) * ((2048 - 1024) / (4096 - 1024))
    assert source.lookup_recompute("mixed", 2048) == pytest.approx(per * 2048)
    assert source.lookup_recompute("mixed", 1024) == pytest.approx(1.1e-5 * 1024)
    assert source.lookup_recompute("mixed", 512) == pytest.approx(1.1e-5 * 512)
    assert source.lookup_recompute("mixed", 70000) == pytest.approx(3.1e-5 * 70000)
    with pytest.raises(RuntimeError):
        source.lookup_recompute("nope", 1024)