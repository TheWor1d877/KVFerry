"""load_runtime_config 配置键与 collect_provenance 口径采集的单测。"""

from __future__ import annotations

import urllib.request
from pathlib import Path
from types import SimpleNamespace

import config
import pytest

_ENV_KEYS = (
    config.ENV_COST_TABLE_ROOT,
    config._ENV_COST_TABLE_VERSION,
    config.ENV_PROBE_INTERVAL_S,
    config.ENV_PROBE_BLOCK_MIB,
    config.ENV_DRIFT_THRESHOLD_PCT,
    config.ENV_DRIFT_TRIGGER_CONSECUTIVE,
    config.ENV_DRIFT_SAMPLE_MIN,
    config.ENV_DRIFT_WINDOW_MIN_S,
    config.ENV_DRIFT_WINDOW_MAX_S,
)


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_drift_defaults(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, str(tmp_path))
    cfg = config.load_runtime_config()
    assert cfg.table_root == tmp_path
    assert cfg.probe_interval_s == 30
    assert cfg.probe_block_mib == 64
    assert cfg.drift_threshold_pct == 20
    assert cfg.drift_trigger_consecutive == 3
    assert cfg.drift_sample_min == 100
    assert cfg.drift_window_min_s == 60
    assert cfg.drift_window_max_s == 1800


def test_drift_per_key_override(tmp_path, monkeypatch):
    overrides = {
        config.ENV_PROBE_INTERVAL_S: ("15", "probe_interval_s", 15),
        config.ENV_PROBE_BLOCK_MIB: ("128", "probe_block_mib", 128),
        config.ENV_DRIFT_THRESHOLD_PCT: ("12.5", "drift_threshold_pct", 12.5),
        config.ENV_DRIFT_TRIGGER_CONSECUTIVE: ("5", "drift_trigger_consecutive", 5),
        config.ENV_DRIFT_SAMPLE_MIN: ("200", "drift_sample_min", 200),
        config.ENV_DRIFT_WINDOW_MIN_S: ("90", "drift_window_min_s", 90),
        config.ENV_DRIFT_WINDOW_MAX_S: ("3600", "drift_window_max_s", 3600),
    }
    for env_name, (raw, field, want) in overrides.items():
        _clear_env(monkeypatch)
        monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, str(tmp_path))
        monkeypatch.setenv(env_name, raw)
        cfg = config.load_runtime_config()
        assert getattr(cfg, field) == want


def test_invalid_drift_values_raise(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, str(tmp_path))
    for env_name in (config.ENV_PROBE_INTERVAL_S, config.ENV_DRIFT_THRESHOLD_PCT):
        for raw in ("0", "-1", "abc"):
            monkeypatch.setenv(env_name, raw)
            with pytest.raises(ValueError):
                config.load_runtime_config()
            monkeypatch.delenv(env_name)


def test_missing_root_raises(monkeypatch):
    _clear_env(monkeypatch)
    with pytest.raises(ValueError):
        config.load_runtime_config()
    monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, "   ")
    with pytest.raises(ValueError):
        config.load_runtime_config()


def test_declared_version_defaults_to_unknown(tmp_path, monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv(config.ENV_COST_TABLE_ROOT, str(tmp_path))
    assert config.load_runtime_config().declared_version == "unknown"
    monkeypatch.setenv("KVFERRY_COST_TABLE_VERSION", "20260101T000000Z")
    assert config.load_runtime_config().declared_version == "20260101T000000Z"


def _fake_completed(stdout: str) -> SimpleNamespace:
    return SimpleNamespace(returncode=0, stdout=stdout, stderr="")


def test_provenance_fields_from_injected_commands(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[0] == "nvidia-smi" and "pci.bus_id" in cmd[1]:
            return _fake_completed("00000000:17:00.0\n")
        if cmd[0] == "nvidia-smi":
            return _fake_completed("NVIDIA A100, 550.54\n")
        if cmd[0] == "ethtool":
            return _fake_completed("Settings for eth0\n")
        if cmd[0] == "lspci":
            return _fake_completed(
                "17:00.0 VGA compatible controller: NVIDIA Corporation\n"
            )
        raise AssertionError(f"意外命令: {cmd}")

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"version": "0.9.2"}'

    monkeypatch.setattr(config.subprocess, "run", fake_run)
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda url, timeout=None: FakeResponse()
    )
    prov = config.collect_provenance("http://127.0.0.1:8000", "qwen3-4b", "fp16", 16)
    assert prov.gpu_name == "NVIDIA A100"
    assert prov.driver_version == "550.54"
    ifaces = sorted(p.name for p in Path("/sys/class/net").iterdir() if p.name != "lo")
    assert prov.nic == (ifaces[0] if ifaces else "unknown")
    assert prov.pcie_topology.startswith("17:00.0")
    assert prov.engine_version == "0.9.2"
    assert prov.model == "qwen3-4b"
    assert prov.kv_precision == "fp16"
    assert prov.block_tokens == 16


def test_provenance_unknown_on_command_failure(monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("command not found")

    monkeypatch.setattr(config.subprocess, "run", boom)
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    prov = config.collect_provenance("http://127.0.0.1:9", "qwen3-4b", "fp16", 16)
    assert prov.gpu_name == "unknown"
    assert prov.driver_version == "unknown"
    assert prov.nic == "unknown"
    assert prov.pcie_topology == "unknown"
    assert prov.engine_version == "unknown"
    assert prov.model == "qwen3-4b"
    assert prov.kv_precision == "fp16"
    assert prov.block_tokens == 16


def test_provenance_unknown_on_nonzero_exit(monkeypatch):
    def failing_run(*args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="error")

    class BrokenResponse:
        def __enter__(self):
            raise OSError("connection refused")

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(config.subprocess, "run", failing_run)
    monkeypatch.setattr(
        urllib.request, "urlopen", lambda url, timeout=None: BrokenResponse()
    )
    prov = config.collect_provenance("http://127.0.0.1:9", "qwen3-4b", "fp16", 16)
    assert prov.gpu_name == "unknown"
    assert prov.driver_version == "unknown"
    assert prov.nic == "unknown"
    assert prov.pcie_topology == "unknown"
    assert prov.engine_version == "unknown"
